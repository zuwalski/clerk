/* 
    Clerk application and storage engine.
    Copyright (C) 2008  Lars Szuwalski

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/
#include "cle_runtime.h"
#include "cle_stream.h"
#include "cle_object.h"
#include <stdio.h>

typedef double rt_number;

static const char number_format[] = "%.14g";

struct _rt_code
{
	struct _rt_code* next;
	char* code;
	st_ptr home;
	st_ptr strings;
	struct _body_ body;
};

// stack-element types
enum {
	STACK_NULL = 0,
	STACK_NUM,
	STACK_OBJ,
	STACK_OUTPUT,
	STACK_REF,
	STACK_CODE,
	STACK_PTR,
	// readonly ptr
	STACK_RO_PTR,
	STACK_PROP,
	STACK_COLLECTION,
	STACK_ITERATOR,
	STACK_ITERATOR_COL
};

struct _rt_stack
{
	union 
	{
		rt_number num;
		struct _rt_stack* var;
		struct
		{
			st_ptr single_ptr;
			st_ptr single_ptr_w;
		};
		struct
		{
			st_ptr obj;
			st_ptr ptr;
		};
		struct
		{
			st_ptr prop_obj;
			identity prop_id;
		};
		struct
		{
			st_ptr code_obj;
			struct _rt_code* code;
		};
		struct
		{
			cle_pipe* out;
			void* outdata;
			task* outtask;
		};
		it_ptr it;
	};
	uchar type;
	uchar flags;	// JIT-flags
};

struct _rt_callframe
{
	struct _rt_callframe* parent;
	struct _rt_code* code;
	struct page_wrap* pg;
	const char* pc;
	struct _rt_stack* vars;
	struct _rt_stack* sp;
	st_ptr object;
	char is_expr;
};

struct _rt_invocation
{
	struct _rt_callframe* top;
	struct _rt_code* code_cache;
	task* t;
	event_handler* hdl;
	ushort params_before_run;
	ushort response_started;
};

const static char exec_error = OP_ERROR;
const static char exec_end = OP_END;

static struct _rt_code _empty_method = 
	{0,(char*)&exec_end,{0,0,0},{0,0,0},
	{0,0,0,0,0,0}};

static uint _rt_error(struct _rt_invocation* inv, uint code)
{
	char buffer[64];
	int len = sprintf(buffer,"runtime:failed(%d)",code);
	cle_stream_fail(inv->hdl,buffer,len);
	inv->top->pc = &exec_error;
	return code;
}

static struct _rt_code* _rt_load_code(struct _rt_invocation* inv, st_ptr code)
{
	struct _rt_code* ret = inv->code_cache;
	st_ptr pt;
	struct _body_ body;

	// lookup in cache
	while(ret != 0)
	{
		if(ret->home.pg == code.pg && ret->home.key == code.key)
			return ret;

		ret = ret->next;
	}

	// not found -> load it and add it
	pt = code;
	if(st_move(inv->t,&pt,"B",2) != 0)
	{
		_rt_error(inv,__LINE__);
		return 0;
	}

	if(st_get(inv->t,&pt,(char*)&body,sizeof(struct _body_)) != -2)
	{
		_rt_error(inv,__LINE__);
		return 0;
	}

	ret = (struct _rt_code*)tk_alloc(inv->t,sizeof(struct _rt_code) + body.codesize - sizeof(struct _body_),0);

	// push
	ret->next = inv->code_cache;
	inv->code_cache = ret;

	ret->home = code;
	ret->body = body;

	ret->code = (char*)ret + sizeof(struct _rt_code);
	if(st_get(inv->t,&pt,ret->code,ret->body.codesize - sizeof(struct _body_)) != -1)
	{
		_rt_error(inv,__LINE__);
		return 0;
	}

	// get strings
	ret->strings = code;
	st_move(inv->t,&ret->strings,"S",2);
	return ret;
}

static struct _rt_callframe* _rt_newcall(struct _rt_invocation* inv, struct _rt_code* code, st_ptr* object, int is_expr)
{
	struct page_wrap* pw;
	struct _rt_callframe* cf = (struct _rt_callframe*)tk_alloc(inv->t,sizeof(struct _rt_callframe)
			+ (code->body.maxvars + code->body.maxstack) * sizeof(struct _rt_stack),&pw);

	cf->pg = pw;
	// push
	cf->parent = inv->top;
	inv->top = cf;

	cf->pc = code->code;
	cf->code = code;
	cf->object = *object;

	cf->vars = (struct _rt_stack*)((char*)cf + sizeof(struct _rt_callframe));

	// clear all vars
	memset(cf->vars,0,code->body.maxvars * sizeof(struct _rt_stack));
	cf->sp = cf->vars + code->body.maxvars + code->body.maxstack;

	cf->is_expr = is_expr;
	return cf;
}

static uint _rt_call(struct _rt_invocation* inv, struct _rt_stack* sp, int params)
{
	int i;
	if(sp[params].type != STACK_CODE)
		return _rt_error(inv,__LINE__);

	inv->top = _rt_newcall(inv,sp[params].code,&sp[params].code_obj,inv->top->is_expr);

	if(inv->top->code->body.maxparams < params)
		return _rt_error(inv,__LINE__);

	for(i = params - 1; i >= 0; i--)
		inv->top->vars[params - 1 - i] = sp[i];

	return 0;
}

static void _rt_get(struct _rt_invocation* inv, struct _rt_stack** sp)
{
	struct _rt_stack top = **sp;
	cle_typed_identity id;

	if(cle_probe_identity(inv->hdl->inst,&top.ptr,&id) != 0)
	{
		(*sp)->type = STACK_OBJ;	// already and always this val anyway ...
		return;
	}

	(*sp)->type = STACK_NULL;

	switch(id.type)
	{
	//case TYPE_STATE:	// validator-code
	case TYPE_HANDLER:
		// geting from within objectcontext or external?
		if(top.obj.pg != inv->top->object.pg || top.obj.key != inv->top->object.key)
			return;
	case TYPE_METHOD:
		if(cle_identity_value(inv->hdl->inst,id.id,top.obj,&top.ptr))
			return;

		(*sp)->code = _rt_load_code(inv,top.ptr);
		(*sp)->code_obj = (*sp)->obj;
		(*sp)->type = STACK_CODE;
		break;
	case TYPE_EXPR:
		if(cle_identity_value(inv->hdl->inst,id.id,top.obj,&top.ptr))
			return;

		inv->top = _rt_newcall(inv,_rt_load_code(inv,top.ptr),&(*sp)->obj,1);

		(*sp)->type = STACK_NULL;
		inv->top->sp--;
		inv->top->sp->type = STACK_REF;
		inv->top->sp->var = *sp;
		*sp = inv->top->sp;
		break;
	case TYPE_DEPENDENCY:
		// geting from within objectcontext or external?
		if(top.obj.pg != inv->top->object.pg || top.obj.key != inv->top->object.key)
			return;

		if(cle_identity_value(inv->hdl->inst,id.id,top.obj,&top.ptr))
			return;

		// TODO: try context-lookup

		// last try: is it a named object?
		if(cle_goto_object(inv->hdl->inst,top.ptr,&(*sp)->obj))
		{
			// name not bound exception
			_rt_error(inv,__LINE__);
			return;
		}

		(*sp)->ptr = (*sp)->obj;
		(*sp)->type = STACK_OBJ;
		break;
	case TYPE_ANY:	// property
		// geting from within objectcontext or external?
		if(top.obj.pg != inv->top->object.pg || top.obj.key != inv->top->object.key)
			return;
		(*sp)->prop_id = id.id;
		(*sp)->prop_obj = top.obj;
		(*sp)->type = STACK_PROP;
	}
}

static uint _rt_move(struct _rt_invocation* inv, struct _rt_stack** sp, cdat mv, uint length)
{
	switch((*sp)->type)
	{
	case STACK_OBJ:
		// object-root?
		if((*sp)->obj.pg == (*sp)->ptr.pg && (*sp)->obj.key == (*sp)->ptr.key)
		{
			if(cle_get_property_host(inv->hdl->inst,&(*sp)->obj,mv,length) < 0)
				return _rt_error(inv,__LINE__);
		}
		else if(st_move(inv->t,&(*sp)->ptr,mv,length) != 0)
			return _rt_error(inv,__LINE__);
		break;
	case STACK_PROP:
		if(cle_identity_value(inv->hdl->inst,(*sp)->prop_id,(*sp)->prop_obj,&(*sp)->ptr))
			return _rt_error(inv,__LINE__);

		if(st_move(inv->t,&(*sp)->ptr,mv,length) == 0)
			return 0;

		if(cle_get_property_ref_value(inv->hdl->inst,(*sp)->ptr,&(*sp)->obj))
			return _rt_error(inv,__LINE__);

		(*sp)->ptr = (*sp)->obj;
		if(cle_get_property_host(inv->hdl->inst,&(*sp)->ptr,mv,length) < 0)
			return _rt_error(inv,__LINE__);
		break;
	case STACK_PTR:
	case STACK_RO_PTR:
		return (st_move(inv->t,&(*sp)->single_ptr,mv,length))? _rt_error(inv,__LINE__) : 0;
	default:
		return _rt_error(inv,__LINE__);
	}
	_rt_get(inv,sp);
	return 0;
}

static uint _rt_move_st(struct _rt_invocation* inv, struct _rt_stack** sp, st_ptr* mv)
{
	switch((*sp)->type)
	{
	case STACK_OBJ:
		// object-root?
		if((*sp)->obj.pg == (*sp)->ptr.pg && (*sp)->obj.key == (*sp)->ptr.key)
		{
			if(cle_get_property_host_st(inv->hdl->inst,&(*sp)->obj,*mv) < 0)
				return _rt_error(inv,__LINE__);
		}
		else if(st_move_st(inv->t,&(*sp)->ptr,mv) != 0)
			return _rt_error(inv,__LINE__);
		break;
	case STACK_PROP:
		if(cle_identity_value(inv->hdl->inst,(*sp)->prop_id,(*sp)->prop_obj,&(*sp)->ptr))
			return _rt_error(inv,__LINE__);

		if(st_move_st(inv->t,&(*sp)->ptr,mv) == 0)
			return 0;

		if(cle_get_property_ref_value(inv->hdl->inst,(*sp)->ptr,&(*sp)->obj))
			return _rt_error(inv,__LINE__);

		(*sp)->ptr = (*sp)->obj;
		if(cle_get_property_host_st(inv->hdl->inst,&(*sp)->ptr,*mv) < 0)
			return _rt_error(inv,__LINE__);
		break;
	case STACK_PTR:
	case STACK_RO_PTR:
		return (st_move_st(inv->t,&(*sp)->single_ptr,mv) != 0)? _rt_error(inv,__LINE__) : 0;
	default:
		return _rt_error(inv,__LINE__);
	}
	_rt_get(inv,sp);
	return 0;
}

static struct _rt_stack* _rt_eval_expr(struct _rt_invocation* inv, struct _rt_stack* target, struct _rt_stack* expr)
{
//			if(cle_identity_value(inv->hdl->inst,F_TOSTRING,&value.obj,&value.ptr) == 0)
//			*sp = _rt_eval_expr(inv,to,&value);

	inv->top = _rt_newcall(inv,_rt_load_code(inv,expr->ptr),&expr->obj,1);
	inv->top->sp--;
	*inv->top->sp = *target;
	return inv->top->sp;
}

static uint _rt_string_out(struct _rt_invocation* inv, struct _rt_stack* to, struct _rt_stack* from)
{
	switch(to->type)
	{
	case STACK_OUTPUT:
		if(inv->response_started == 0)
		{
			to->out->start(to->outdata);
			inv->response_started = 1;
		}
		else inv->response_started = 2;
		return st_map(inv->t,&from->single_ptr,to->out->data,to->outdata);
	case STACK_PTR:
		return st_insert_st(inv->t,&to->single_ptr_w,&from->single_ptr);
	default:
		return _rt_error(inv,__LINE__);
	}
}

static void _rt_num_out(struct _rt_invocation* inv, struct _rt_stack* to, rt_number num)
{
	char buffer[32];
	int len = sprintf(buffer,number_format,num);
	switch(to->type)
	{
	case STACK_OUTPUT:
		if(inv->response_started == 0)
		{
			to->out->start(to->outdata);
			inv->response_started = 1;
		}
		else inv->response_started = 2;
		to->out->data(to->outdata,buffer,len);
		break;
	case STACK_PTR:
		st_insert(inv->t,&to->single_ptr_w,buffer,len);
		break;
	default:
		_rt_error(inv,__LINE__);
	}
}

static uint _rt_prop_out(struct _rt_invocation* inv, struct _rt_stack** sp, struct _rt_stack* to, struct _rt_stack* from)
{
	struct _rt_stack value;
	rt_number num;

	if(cle_identity_value(inv->hdl->inst,from->prop_id,from->prop_obj,&value.single_ptr))
		return _rt_error(inv,__LINE__);

	switch(cle_get_property_type_value(inv->hdl->inst,value.single_ptr))
	{
	case TYPE_ANY:
		return _rt_string_out(inv,to,&value);
	case TYPE_NUM:
		if(cle_get_property_num_value(inv->hdl->inst,value.single_ptr,&num))
			return _rt_error(inv,__LINE__);
		_rt_num_out(inv,to,num);
		break;
	case TYPE_REF:
	case TYPE_REF_MEM:
		if(cle_get_property_ref_value(inv->hdl->inst,value.single_ptr,&value.obj))
			return _rt_error(inv,__LINE__);
		if(cle_identity_value(inv->hdl->inst,F_TOSTRING,value.obj,&value.ptr) == 0)
			*sp = _rt_eval_expr(inv,to,&value);
		break;
	case TYPE_COLLECTION:
		// TODO: output collection
		break;
	}
	return 0;
}

static uint _rt_out(struct _rt_invocation* inv, struct _rt_stack** sp, struct _rt_stack* to, struct _rt_stack* from)
{
	switch(from->type)
	{
	case STACK_PROP:
		return _rt_prop_out(inv,sp,to,from);
	case STACK_RO_PTR:
	case STACK_PTR:
		return _rt_string_out(inv,to,from);
	case STACK_NUM:
		_rt_num_out(inv,to,from->num);
		break;
	case STACK_OBJ:
		if(cle_identity_value(inv->hdl->inst,F_TOSTRING,from->obj,&from->ptr) == 0)
			*sp = _rt_eval_expr(inv,to,from);
		break;	// no "tostring" expr? output nothing
	case STACK_CODE:
		// write out path/event to method/handler
		if(st_move(inv->t,&from->single_ptr,"p",1) == 0)
			return _rt_string_out(inv,to,from);
		break;
	case STACK_ITERATOR:
	case STACK_ITERATOR_COL:
		// output iterator
		break;
	}
	return 0;
}

static uint _rt_ref_out(struct _rt_invocation* inv, struct _rt_stack** sp, struct _rt_stack* to)
{
	st_ptr pt;
	st_empty(inv->t,&pt);

	if(to->var->type != STACK_NULL)
	{
		struct _rt_stack target;
		target.type = STACK_PTR;
		target.single_ptr_w = pt;

		if(_rt_out(inv,sp,&target,to->var))
			return 1;
	}

	to->var->single_ptr_w = to->var->single_ptr = pt;
	to->var->type = STACK_PTR;
	*to = *to->var;
	return 0;
}

static void _open_start(void* ipt) {}
static void _open_end(void* ipt, cdat c, uint u) {}
static void _open_submit(void* ipt, task* t, st_ptr* pt) {st_map_st(t,pt,cle_data,cle_push,cle_pop,ipt);}

static cle_pipe _rt_open_pipe = {_open_start,(void(*)(void*))cle_next,_open_end,(void(*)(void*))cle_pop,(void(*)(void*))cle_push,(uint(*)(void*,cdat,uint))cle_data,(void(*)(void*,task*,st_ptr*))_open_submit};

static uint _rt_do_open(struct _rt_invocation* inv, struct _rt_stack** sp)
{
	cle_pipe* response;
	void* resp_data;
	task* outtask;
	_ipt* ipt;
	
	switch((*sp)[1].type)
	{
	case STACK_OUTPUT:
		response  = (*sp)[1].out;
		resp_data = (*sp)[1].outdata;
		break;
	case STACK_PTR:
	case STACK_REF:
		// copy to ...
	default:
		return _rt_error(inv,__LINE__);
	}

	if((*sp)->type != STACK_RO_PTR && (*sp)->type != STACK_PTR)
		return _rt_error(inv,__LINE__);

	outtask = tk_clone_task(inv->t);

/*	FIXME
 * ipt = cle_start(
		outtask,
		inv->hdl->eventdata->config,
		(*sp)->single_ptr,
		inv->hdl->eventdata->userid,

		inv->hdl->eventdata->userroles,
		response,resp_data);
*/
	if(ipt == 0)
	{
		tk_drop_task(outtask);
		// TODO raise catchable exception
		return _rt_error(inv,__LINE__);
	}

	if(inv->response_started == 0)
		inv->response_started = 1;

	(*sp)->outdata = ipt;
	(*sp)->out     = &_rt_open_pipe;
	(*sp)->outtask = outtask;
	(*sp)->type    = STACK_OUTPUT;
	return 0;
}

// make sure its a number (load it)
static uint _rt_num(struct _rt_invocation* inv, struct _rt_stack* sp)
{
	if(sp->type == STACK_NUM)
		return 0;

	if(sp->type != STACK_PROP)
		return _rt_error(inv,__LINE__);

	// is there a number here?
	if(cle_get_property_num(inv->hdl->inst,sp->prop_obj,sp->prop_id,&sp->num))
		return _rt_error(inv,__LINE__);

	sp->type = STACK_NUM;
	return 0;
}

static int _rt_equal(struct _rt_invocation* inv, struct _rt_stack* op1, struct _rt_stack* op2)
{
	if(op1->type == STACK_NUM)
	{
		if(_rt_num(inv,op2))
			return 0;
	}
	else if(op2->type == STACK_NUM)
	{
		if(_rt_num(inv,op1))
			return 0;
	}
	// not a number
	else
	{
		return (op1->type == op2->type);
	}

	return op1->num == op2->num;
}

static int _rt_compare(struct _rt_invocation* inv, struct _rt_stack* op1, struct _rt_stack* op2)
{
	if(op1->type == STACK_NUM)
	{
		if(_rt_num(inv,op2))
			return 0;
	}
	else if(op2->type == STACK_NUM)
	{
		if(_rt_num(inv,op1))
			return 0;
	}
	// not a number
	else
	{
		op2->type = STACK_NULL;
		return 0;
	}

	return op1->num - op2->num;
}

static uint _rt_test(struct _rt_stack* sp)
{
	return ((sp->type != STACK_NULL) || (sp->type == STACK_NUM && sp->num != 0) ||
		((sp->type == STACK_PTR || sp->type == STACK_RO_PTR) && (st_is_empty(&sp->single_ptr) == 0)));
}

static void _rt_free(struct _rt_invocation* inv, struct _rt_stack* var)
{
	if(var->type == STACK_ITERATOR)
		it_dispose(inv->t,&var->it);
	var->type = STACK_NULL;
}

static uint _rt_new_obj(struct _rt_invocation* inv, struct _rt_stack* sp)
{
	cle_new_mem(inv->t,sp->obj,&sp->obj);

	if(cle_identity_value(inv->hdl->inst,F_INIT,sp->obj,&sp->ptr) == 0)
		sp->code = _rt_load_code(inv,sp->ptr);
	else
		sp->code = &_empty_method;

	sp->code_obj = sp->obj;
	sp->type = STACK_CODE;
	return 0;
}

static void _rt_type_id(struct _rt_invocation* inv, struct _rt_stack* sp)
{
	st_ptr result,result_w;
	st_empty(inv->t,&result);
	result_w = result;

	switch(sp->type)
	{
	case STACK_OBJ:
		{
			oid_str buffer;
			if(cle_get_oid_str(inv->hdl->inst,sp->obj,&buffer) == 0)
				st_insert(inv->t,&result_w,(cdat)&buffer,sizeof(buffer));
		}
		break;
	case STACK_PTR:
	case STACK_RO_PTR:
		st_insert(inv->t,&result_w,"[struct]",9);
		break;
	case STACK_NUM:
		st_insert(inv->t,&result_w,"[num]",6);
		break;
	case STACK_CODE:
		st_insert(inv->t,&result_w,"[code]",7);
		break;
	case STACK_NULL:
		st_insert(inv->t,&result_w,"[null]",7);
		break;
	case STACK_PROP:
		//cle_get_property_ref() and out id
		st_insert(inv->t,&result_w,"[property]",13);
	}
	sp->type = STACK_PTR;
}

static void cle_skip_header(cle_instance inst,st_ptr* pt){
	cle_panic(inst.t);
}

static void _rt_run(struct _rt_invocation* inv)
{
	struct _rt_stack* sp = inv->top->sp;
	while(1)
	{
		int tmp;
		switch(*inv->top->pc++)
		{
		case OP_NOOP:
			break;
		case OP_DEBUG:
			break;
		case OP_POP:
			sp++;
			break;
		case OP_ADD:
			if(_rt_num(inv,sp) || _rt_num(inv,sp + 1))
				break;
			sp[1].num += sp[0].num;
			sp++;
			break;
		case OP_SUB:
			if(_rt_num(inv,sp) || _rt_num(inv,sp + 1))
				break;
			sp[1].num -= sp[0].num;
			sp++;
			break;
		case OP_MUL:
			if(_rt_num(inv,sp) || _rt_num(inv,sp + 1))
				break;
			sp[1].num *= sp[0].num;
			sp++;
			break;
		case OP_DIV:
			if(_rt_num(inv,sp) || _rt_num(inv,sp + 1) || sp->num == 0)
				break;
			sp[1].num /= sp[0].num;
			sp++;
			break;
		case OP_NOT:
			sp->num = _rt_test(sp) == 0;
			sp->type = STACK_NUM;
			break;
		case OP_NEG:
			if(_rt_num(inv,sp))
				break;
			sp->num *= -1;
			break;

		case OP_EQ:
			sp[1].num = _rt_equal(inv,sp,sp + 1);
			sp[1].type = STACK_NUM;
			sp++;
			break;
		case OP_NE:
			sp[1].num = _rt_equal(inv,sp,sp + 1) == 0;
			sp[1].type = STACK_NUM;
			sp++;
			break;
		case OP_GE:
			sp[1].num = _rt_compare(inv,sp,sp + 1) >= 0;
			sp++;
			break;
		case OP_GT:
			sp[1].num = _rt_compare(inv,sp,sp + 1) > 0;
			sp++;
			break;
		case OP_LE:
			sp[1].num = _rt_compare(inv,sp,sp + 1) <= 0;
			sp++;
			break;
		case OP_LT:
			sp[1].num = _rt_compare(inv,sp,sp + 1) < 0;
			sp++;
			break;

		case OP_BNZ:
			// emit Is (branch forward conditional)
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			if(_rt_test(sp))
				inv->top->pc += tmp;
			sp++;
		case OP_BZ:
			// emit Is (branch forward conditional)
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			if(_rt_test(sp) == 0)
				inv->top->pc += tmp;
			sp++;
			break;
		case OP_BR:
			// emit Is (branch forward)
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += tmp + sizeof(ushort);
			break;

		case OP_LOOP:	// JIT-HOOK 
			// emit Is (branch back)
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc -= tmp - sizeof(ushort);
			break;
		case OP_ZLOOP:
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			if(_rt_test(sp) == 0)
				inv->top->pc -= tmp;
			sp++;
			break;
		case OP_NZLOOP:
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			if(_rt_test(sp))
				inv->top->pc -= tmp;
			sp++;
			break;

		case OP_FREE:
			// emit Ic2 (byte,byte)
			tmp = *inv->top->pc++;
			while(tmp-- > 0)
				_rt_free(inv,&inv->top->vars[tmp + *inv->top->pc]);
			inv->top->pc++;
			break;

		case OP_NULL:		// remove
			sp--;
			sp->type = STACK_NULL;
			break;
		case OP_IMM:
			// emit II (imm short)
			sp--;
			sp->type = STACK_NUM;
			sp->num = *((short*)inv->top->pc);
			inv->top->pc += sizeof(short);
			break;
		case OP_STR:
			// emit Is
			sp--;
			sp->type = STACK_RO_PTR;
			sp->single_ptr = inv->top->code->strings;
			st_move(inv->t,&sp->single_ptr,inv->top->pc,sizeof(ushort));
			inv->top->pc += sizeof(ushort);
			break;
		case OP_OBJ:
			sp--;
			sp->type = STACK_OBJ;
			sp->ptr = sp->obj = inv->top->object;
			cle_skip_header(inv->hdl->inst,&sp->ptr);
			break;

		case OP_NEW:
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			sp--;
//			if(cle_goto_object_cdat(inv->hdl->inst,inv->top->pc,tmp,&sp->obj))
				if (1)
				_rt_error(inv,__LINE__);
			else
			{
				inv->top->pc += tmp;
				if(_rt_new_obj(inv,sp))
					_rt_error(inv,__LINE__);
			}
			break;
		case OP_CLONE:
			if(sp->type != STACK_OBJ)
				_rt_error(inv,__LINE__);
			else if(_rt_new_obj(inv,sp))
				_rt_error(inv,__LINE__);
			break;
		case OP_ID:
			sp--;
			sp->type = STACK_OBJ;
			sp->obj = inv->top->object;
			_rt_type_id(inv,sp);
			break;
		case OP_IDO:
			_rt_type_id(inv,sp);
			break;
		case OP_FIND:
			inv->top->pc++;
			if(sp->type == STACK_PTR || sp->type == STACK_RO_PTR)
			{
				if(cle_goto_object(inv->hdl->inst,sp->single_ptr,&sp->obj))
					sp->type = STACK_NULL;
				else
				{
					sp->ptr = sp->obj;
					cle_skip_header(inv->hdl->inst,&sp->ptr);
					sp->type = STACK_OBJ;
				}
			}
			else
				_rt_error(inv,__LINE__);
			break;

		case OP_IT:
			inv->top->pc++;
			sp--;
			switch(sp[1].type)
			{
			case STACK_PROP:
				// try to start iterator on value
				break;
			case STACK_COLLECTION:
				it_create(inv->t,&sp->it,&sp[1].ptr);
				sp->type = STACK_ITERATOR_COL;
				break;
			case STACK_PTR:
			case STACK_RO_PTR:
				it_create(inv->t,&sp->it,&sp[1].single_ptr);
				sp->type = STACK_ITERATOR;
				break;
			default:
				_rt_error(inv,__LINE__);
			}
			break;
		case OP_INEXT:
			tmp = *inv->top->pc++;
			switch(sp->type)
			{
			case STACK_ITERATOR:
				sp->num = it_next(inv->t,0,&sp->it,0);
				break;
			case STACK_ITERATOR_COL:
				sp->num = it_next(inv->t,0,&sp->it,sizeof(oid));
				break;
			default:
				_rt_error(inv,__LINE__);
			}
			sp->type = STACK_NUM;
			break;
		case OP_IPREV:
			tmp = *inv->top->pc++;
			switch(sp->type)
			{
			case STACK_ITERATOR:
				sp->num = it_prev(inv->t,0,&sp->it,0);
				break;
			case STACK_ITERATOR_COL:
				sp->num = it_prev(inv->t,0,&sp->it,sizeof(oid));
				break;
			default:
				_rt_error(inv,__LINE__);
			}
			sp->type = STACK_NUM;
			break;
		case OP_IKEY:
			inv->top->pc++;
			if(sp->type != STACK_ITERATOR && sp->type != STACK_ITERATOR_COL)
				_rt_error(inv,__LINE__);
			else
			{
				st_ptr pt;
				st_empty(inv->t,&pt);
				st_append(inv->t,&pt,sp->it.kdata,sp->it.kused);
				sp->single_ptr = sp->single_ptr_w = pt;
				sp->type = STACK_PTR;
			}
			break;
		case OP_IVAL:
			inv->top->pc++;
			if(sp->type == STACK_ITERATOR)
			{
				st_ptr pt;
				if(it_current(inv->t,&sp->it,&pt) == 0)
				{
					sp->single_ptr = sp->single_ptr_w = pt;
					sp->type = STACK_PTR;
				}
				else
					sp->type = STACK_NULL;
			}
			else if(sp->type == STACK_ITERATOR_COL)
			{
				if(sp->it.kused == sizeof(oid))
				{
					// TODO: move to object.c
					st_ptr pt = inv->hdl->inst.root;
					st_move(inv->t,&pt,sp->it.kdata,sizeof(oid));
					sp->ptr = sp->obj = pt;
					cle_skip_header(inv->hdl->inst,&sp->ptr);
					sp->type = STACK_OBJ;
				}
				else
					sp->type = STACK_NULL;
			}
			else
				_rt_error(inv,__LINE__);
			break;

		case OP_OMV:
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			sp--;
			sp->obj = inv->top->object;
			if(cle_get_property_host(inv->hdl->inst,&sp->ptr,inv->top->pc,tmp) < 0)
			{
				sp->type = STACK_NULL;
				inv->top->pc += tmp;
			}
			else
			{
				inv->top->pc += tmp;
				sp->ptr = sp->obj;
				cle_skip_header(inv->hdl->inst,&sp->ptr);
				_rt_get(inv,&sp);
			}
			break;
		case OP_MV:
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			if(_rt_move(inv,&sp,inv->top->pc,tmp))
				sp->type = STACK_NULL;
			inv->top->pc += tmp;
			break;
		case OP_RIDX:
			if(sp->type == STACK_NUM)
			{
				char buffer[sizeof(rt_number) + HEAD_SIZE];
				buffer[0] = 0;
				buffer[1] = 'N';
				memcpy(buffer + 2,&sp->num,sizeof(rt_number));
				sp++;
				if(_rt_move(inv,&sp,buffer,sizeof(buffer)))
					sp->type = STACK_NULL;
			}
			else if(sp->type == STACK_PTR ||
				sp->type == STACK_RO_PTR)
			{
				st_ptr mv = sp->single_ptr;
				sp++;
				if(_rt_move_st(inv,&sp,&mv))
					sp->type = STACK_NULL;
			}
			break;
		case OP_LVAR:
			sp--;
			*sp = inv->top->vars[*inv->top->pc++];
			break;

		// writer
		case OP_POPW:
			if(sp->type == STACK_OUTPUT)
				sp->out->pop(sp->outdata);
			else
				sp++;
			break;
		case OP_DMVW:
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			switch(sp->type)
			{
			case STACK_REF:
				if(sp->var->type == STACK_NULL)
				{
					st_empty(inv->t,&sp->var->single_ptr_w);
					sp->var->single_ptr = sp->var->single_ptr_w;
				}
				else if(sp->var->type != STACK_PTR)
				{
					_rt_error(inv,__LINE__);
					return;
				}
				sp->single_ptr_w = sp->var->single_ptr;
				sp->single_ptr = sp->single_ptr_w;
				sp->type = STACK_PTR;
			case STACK_PTR:
				sp--;
				sp[0] = sp[1];
				st_insert(inv->t,&sp->single_ptr_w,inv->top->pc,tmp);
				break;
			case STACK_OUTPUT:
				if(inv->response_started == 0)
				{
					sp->out->start(sp->outdata);
					inv->response_started = 1;
				}
				else inv->response_started = 2;
				sp->out->push(sp->outdata);
				sp->out->data(sp->outdata,inv->top->pc,tmp);
			}
			inv->top->pc += tmp;
			break;
		case OP_MVW:
			tmp = *((ushort*)inv->top->pc);
			inv->top->pc += sizeof(ushort);
			switch(sp->type)
			{
			case STACK_PTR:
				st_insert(inv->t,&sp->single_ptr_w,inv->top->pc,tmp);
				break;
			case STACK_OUTPUT:
				if(inv->response_started == 0)
				{
					sp->out->start(sp->outdata);
					inv->response_started = 1;
				}
				else inv->response_started = 2;
				sp->out->data(sp->outdata,inv->top->pc,tmp);
			}
			inv->top->pc += tmp;
			break;
		case OP_WIDX:	// replace by OP_OUT ?
			_rt_out(inv,&sp,sp,sp + 1);
			sp++;
			break;

		case OP_OPEN:
			_rt_do_open(inv,&sp);
			break;
		case OP_OPEN_POP:
			// unfinished output
			if(inv->response_started == 2)
				sp->out->next(sp->outdata);
			sp->out->end(sp->outdata,0,0);
			inv->response_started = 1;
			tk_commit_task(sp->outtask);	// well, what if something went wrong??
			sp++;
			break;
		// receive input
		case OP_RECV:
			sp += *inv->top->pc++;
			inv->top->sp = sp;
			return;

		case OP_SET:
			if(inv->top->is_expr != 0)
				_rt_error(inv,__LINE__);
			else if(sp[1].type != STACK_PROP)
				_rt_error(inv,__LINE__);
			else
			{
				switch(sp->type)
				{
				case STACK_PROP:
					if(cle_identity_value(inv->hdl->inst,sp->prop_id,sp->prop_obj,&sp->ptr))
						_rt_error(inv,__LINE__);
					if(cle_set_property_ptr(inv->hdl->inst,sp[1].prop_obj,sp[1].prop_id,&sp[1].ptr))
						_rt_error(inv,__LINE__);
					// might not be a good idea if prop-val is a mem-ref
					st_copy_st(inv->t,&sp[1].ptr,&sp->ptr);
					break;
				case STACK_RO_PTR:
					// link
					if(cle_set_property_ptr(inv->hdl->inst,sp[1].prop_obj,sp[1].prop_id,&sp[1].ptr))
						_rt_error(inv,__LINE__);
					st_link(inv->t,&sp[1].ptr,&sp->single_ptr);
					break;
				case STACK_PTR:
					// copy 
					if(cle_set_property_ptr(inv->hdl->inst,sp[1].prop_obj,sp[1].prop_id,&sp[1].ptr))
						_rt_error(inv,__LINE__);
					st_copy_st(inv->t,&sp[1].ptr,&sp->single_ptr);
					break;
				case STACK_NUM:
					// bin-num
					if(cle_set_property_num(inv->hdl->inst,sp[1].prop_obj,sp[1].prop_id,sp->num))
						_rt_error(inv,__LINE__);
					break;
				case STACK_OBJ:
					// obj-ref
					if(cle_set_property_ref(inv->hdl->inst,sp[1].prop_obj,sp[1].prop_id,sp->obj))
						_rt_error(inv,__LINE__);
					break;
				case STACK_CODE:
					// write out path/event to method/handler
					if(st_move(inv->t,&sp->single_ptr,"p",1) == 0)
					{
						if(cle_set_property_ptr(inv->hdl->inst,sp[1].prop_obj,sp[1].prop_id,&sp[1].ptr))
							_rt_error(inv,__LINE__);
						st_copy_st(inv->t,&sp[1].ptr,&sp->single_ptr);
						break;
					}	// or null
				default:
					// empty / null
					if(cle_set_property_ptr(inv->hdl->inst,sp[1].prop_obj,sp[1].prop_id,&sp[1].ptr))
						_rt_error(inv,__LINE__);
				}
			}
			sp += 2;
			break;
		case OP_MERGE:
			switch(sp->type)
			{
			case STACK_PROP:
				if(inv->top->is_expr != 0)
					_rt_error(inv,__LINE__);
				else if(cle_set_property_ptr(inv->hdl->inst,sp->prop_obj,sp->prop_id,&sp->single_ptr))
					_rt_error(inv,__LINE__);
				else
				{
					sp->single_ptr_w = sp->single_ptr;
					sp->type = STACK_PTR;
				}
				break;
			case STACK_REF:
				if(sp->var->type == STACK_NULL)
				{
					st_empty(inv->t,&sp->var->single_ptr);
					sp->var->single_ptr_w = sp->var->single_ptr;
					sp->var->type = STACK_PTR;
				}
				else if(sp->var->type != STACK_PTR)
					_rt_error(inv,__LINE__);
			case STACK_PTR:
			case STACK_OUTPUT:
				break;
			default:
				_rt_error(inv,__LINE__);
			}
			break;
		case OP_2STR:
			tmp = *inv->top->pc++;	// vars
			if(tmp != 1)
			{
				_rt_error(inv,__LINE__);
				break;
			}
			// fall throu
		case OP_CAT:
			{
				struct _rt_stack to;
				st_empty(inv->t,&to.single_ptr);
				to.single_ptr_w = to.single_ptr;
				to.type = STACK_PTR;
				_rt_out(inv,&sp,&to,sp);
				*sp = to;
			}
			break;
		case OP_NEXT:	// non-string (concat) out-ing [OUT Last Tree]
			if(sp[1].type == STACK_REF)
			{
				if(sp[1].var->type == STACK_NULL)
					*sp[1].var = *sp;
				else _rt_ref_out(inv,&sp,sp + 1);
			}
			else
			{
				if(_rt_out(inv,&sp,sp + 1,sp) == 0)
				{
					sp++;
					if(inv->response_started == 2 && sp->type == STACK_OUTPUT)
					{
						inv->response_started = 1;
						sp->out->next(sp->outdata);
					}
				}
			}
			break;
		case OP_OUTL:
			// TODO: stream out structures
		case OP_OUT:	// stream out string
			if(sp[1].type == STACK_REF)
				_rt_ref_out(inv,&sp,sp + 1);
			else
				_rt_out(inv,&sp,sp + 1,sp);
			sp++;
			break;

		case OP_AVAR:
			inv->top->vars[*inv->top->pc++] = *sp;
			sp++;
			break;
		case OP_DEFP:
			// emit Is2 (branch forward)
			tmp = *inv->top->pc++;	// var
			if(inv->top->vars[tmp].type == STACK_NULL)
			{
				sp--;
				inv->top->vars[tmp].var = sp;
				inv->top->vars[tmp].type = STACK_REF;
				inv->top->pc += sizeof(ushort);
			}
			else
			{
				tmp = *((ushort*)inv->top->pc);
				inv->top->pc += tmp + sizeof(ushort);
			}
			break;
		case OP_END:
			tmp = inv->top->code->body.maxparams;
			while(tmp-- > 0)
				_rt_free(inv,&inv->top->vars[tmp]);

			if(inv->top->parent == 0)
			{
				// unfinished output? -> next
				if(inv->response_started == 2)
					inv->hdl->response->next(inv->hdl->respdata);

				cle_stream_end(inv->hdl);
				return;
			}
			else
			{
				struct _rt_callframe* cf = inv->top;
				inv->top = inv->top->parent;
				sp = inv->top->sp;

				// unref page of origin
				tk_unref(inv->t,cf->pg);
			}
			break;
		case OP_DOCALL:
			tmp = *inv->top->pc++;	// params
			if(_rt_call(inv,sp,tmp) == 0)
			{
				inv->top->parent->sp = sp + 1 + tmp;	// return-stack
				*(--inv->top->sp) = *(sp + 1 + tmp);	// copy output-target
				sp = inv->top->sp;		// set new stack
			}
			break;
		case OP_DOCALL_N:
			tmp = *inv->top->pc++;	// params
			if(_rt_call(inv,sp,tmp) == 0)
			{
				inv->top->parent->sp = sp + tmp;	// return-stack
				inv->top->sp--;
				inv->top->sp->type = STACK_REF;	// ref to sp-top
				inv->top->sp->var = sp + tmp;
				inv->top->sp->var->type = STACK_NULL;
				sp = inv->top->sp;		// set new stack
			}
			break;
		case OP_ERROR:	// system exception
			return;
		default:
			_rt_error(inv,__LINE__);
		}
	}
}

/*
	load method from "handler" and invoke on "object"
	if method takes any number of parameters -> collect parameters in next, before invoke
*/
static void _rt_start(event_handler* hdl)
{
	struct _rt_invocation* inv = (struct _rt_invocation*)tk_alloc(hdl->inst.t,sizeof(struct _rt_invocation),0);
	hdl->handler_data = inv;

	inv->t = hdl->inst.t;
	inv->response_started = 0;
	inv->code_cache = 0;
	inv->hdl = hdl;
	inv->top = 0;

	if(_rt_load_code(inv,hdl->handler) == 0)
		return;

	_rt_newcall(inv,inv->code_cache,&hdl->object,0);

	// push response-pipe
	inv->top->sp--;
	inv->top->sp->out = hdl->response;
	inv->top->sp->outdata = hdl->respdata;
	inv->top->sp->outtask = 0;
	inv->top->sp->type = STACK_OUTPUT;

	// get parameters before launch?
	inv->params_before_run = inv->code_cache->body.maxparams;

	if(inv->params_before_run == 0)
		_rt_run(inv);
}

static void _rt_next(event_handler* hdl)
{
	struct _rt_invocation* inv = (struct _rt_invocation*)hdl->handler_data;

	if(inv->params_before_run == 0)
	{
		inv->top->sp--;
		inv->top->sp->single_ptr = hdl->root;
		inv->top->sp->type = STACK_RO_PTR;

		_rt_run(inv);
	}
	else
	{
		struct _rt_stack* var = inv->top->vars + (inv->top->code->body.maxparams - inv->params_before_run);

		var->single_ptr = hdl->root;
		var->type = STACK_RO_PTR;

		if(--inv->params_before_run == 0)
			_rt_run(inv);
	}
	cle_standard_next_done(hdl);
}

static void _rt_end(event_handler* hdl, cdat code, uint length)
{
	struct _rt_invocation* inv = (struct _rt_invocation*)hdl->handler_data;
	
	if(hdl->error == 0 && length == 0)
	{
		// blocked on read() -> send null
		if(inv->params_before_run == 0)
		{
			inv->top->sp--;
			inv->top->sp->type = STACK_NULL;
		}

		_rt_run(inv);

		// retire 'read': then this is not needed // or loop and return null to task until it gives up
		if(hdl->error == 0)
			cle_stream_fail(hdl,"runtime:end",12);
	}
}

cle_syshandler _runtime_handler = {0,{_rt_start,_rt_next,_rt_end,cle_standard_pop,cle_standard_push,cle_standard_data,cle_standard_submit},0};

cle_syshandler _object_stream = {0,{_rt_start,_rt_next,_rt_end,cle_standard_pop,cle_standard_push,cle_standard_data,cle_standard_submit},0};


