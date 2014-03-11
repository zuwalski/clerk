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
#include "cle_compile.h"

#define BUFFER_GROW 256

#define ST_0 1
#define ST_ALPHA 2
#define ST_DOT 4
#define ST_STR 8
#define ST_BREAK 16
#define ST_VAR 32
#define ST_DEF 64
#define ST_NUM 128
#define ST_NUM_OP 256
//#define ST_IF 512
//#define ST_CALL 1024

#define PROC_EXPR 0
#define PURE_EXPR 1
#define NEST_EXPR 2

#define whitespace(c) (c == ' ' || c == '\t' || c == '\n' || c == '\r')
#define num(c) (c >= '0' && c <= '9')
#define minusnum(c) (c == '-' || num(c))
#define alpha(c) ((c != -1) && ((c & 0x80) || (c >= 'a' && c <= 'z')  || (c >= 'A' && c <= 'Z') || (c == '_')))
#define alphanum(c) (alpha(c) || num(c))

#define NUMBER_OF_SKIPS 32
struct _skips
{
	struct _skips* next;
	long skips[NUMBER_OF_SKIPS];
};

struct _skip_list
{
	struct _skips* top;
	uint glevel;
	uint home;
	uint index;
};

struct _cmp_op
{
	struct _cmp_op* prev;
	uchar opc;
	uchar prec;
};

struct _cmp_state
{
	cle_pipe* response;
	void* data;

	task* t;
	char* opbuf;
	char* code;
	char* lastop;
	struct _skips* freeskip;
	st_ptr* body;
	st_ptr root;
	st_ptr strs;
	st_ptr cur_string;

	uint err;

	uint bsize;
	uint top;

	// code-size
	uint code_size;
	uint code_next;

	// code-offset of first handler (if any)
	uint first_handler;

	// top-var
	uint top_var;

	// prg-stack
	uint s_top;
	uint s_max;

	// prg-stack
	uint v_top;
	uint v_max;

	// global level
	int glevel;

	// parameters
	uint params;

	int c;

	ushort stringidx;
};

struct _cmp_var
{
	uint prev;
	uint id;
	uint name;
	uint leng;
	uint level;
};
#define PEEK_VAR(v) ((struct _cmp_var*)(cst->opbuf + (v)))

static const char* keywords[] = {
	"do","end","if","elseif","else","while","repeat","until","open",
	"var","new","for","break","and","or","not",
	"handle","raise","switch","case","default","this","super","goto","next",0
};	// + true, false

#define KW_MAX_LEN 7

enum cmp_keywords {
	KW_DO = 1,
	KW_END,
	KW_IF,
	KW_ELSEIF,
	KW_ELSE,
	KW_WHILE,
	KW_REPEAT,
	KW_UNTIL,
	KW_OPEN,
	KW_VAR,
	KW_NEW,
	KW_FOR,
	KW_BREAK,
	KW_AND,
	KW_OR,
	KW_NOT,
	KW_HANDLE,
	KW_RAISE,
	KW_SWITCH,
	KW_CASE,
	KW_DEFAULT,
	KW_THIS,
	KW_SUPER,
	KW_GOTO,
	KW_NEXT
};

struct _cmp_buildin
{
	const char* id;
	uint opcode;
	uint opcode_obj;
	uint min_param;
	uint max_param;
};

static const struct _cmp_buildin buildins[] = {
	{"read",OP_RECV,0,0,1},				// * recieve data-structure from event-queue (timeout-param)
	{"list",OP_NULL,0,1,255},			// form a list from input-params
	{"void",OP_NULL,0,1,255},			// throw away values
	{"me",OP_NULL,0,0,0},
	{"parent",OP_NULL,0,0,0},
	{"user",OP_NULL,0,0,0},
	{"request",OP_NULL,0,0,0},
//	{"session",OP_NULL,0,0,0},

	{"id",OP_ID,OP_IDO,0,0},			// get objectid in stringid format
	{"object",OP_FIND,0,1,1},			// lookup object using name or id
	{"valid",OP_NULL,OP_NULL,0,1},		// is object valid (run validation)? (in state) (current or ref)
//	{"delete",0,OP_NULL,0,1},			// delete object or delete sub-tree

	{"add",0,OP_CADD,1,255},			// collection: add object(s) to collection (refs)
	{"remove",0,OP_CREMOVE,1,255},		// collection: remove object(s) from collection (ids or refs)
	{"in",0,OP_CIN,1,255},				// collection: test if object(s) are in collection
	{"gt",0,OP_NULL,1,1},				// collection: add greater-than criteria
	{"gte",0,OP_NULL,1,1},				// collection: add greater-or-equal criteria
	{"lt",0,OP_NULL,1,1},				// collection: add less-than criteria
	{"lte",0,OP_NULL,1,1},				// collection: add less-or-equal criteria
	{"eq",0,OP_NULL,1,1},				// collection: add equal criteria
	{"neq",0,OP_NULL,1,1},				// collection: add not-equal criteria
	{"filter",0,OP_NULL,1,1},			// collection: add filter-function (criteria)
	{"sort",0,OP_NULL,1,1},				// collection: add sort-function (criteria)

	{"map",0,OP_NULL,1,1},				// collection: map(k,v) function on keys,values => output
	{"foldl",0,OP_NULL,1,2},			// collection: foldl(k,v,r) function on keys,values => val(r)
	{"foldr",0,OP_NULL,1,2},			// collection: foldr(k,v,r) function on keys,values => val(r)

	// replace by language-construct
	{"iterate",0,OP_IT,0,0},
	{"key",0,OP_IKEY,0,0},
	{"value",0,OP_IVAL,0,0},
	{"next",0,OP_INEXT,0,0},
	{"prev",0,OP_IPREV,0,0},

	{"string",OP_2STR,0,1,255},
	{0,0,0,0,0}	// STOP
};

static char str_err[] = "[cmp]err ";

static void print_err(struct _cmp_state* cst, int line)
{
	char bf[6];
	int i;
	cst->response->data(cst->data,str_err,sizeof(str_err));

	for(i = 4; i != 0 && line > 0; i--)
	{
		bf[i] = '0' + (line % 10);
		line /= 10;
	}

	bf[5] = '\n';
	i++;
	cst->response->data(cst->data,bf + i,sizeof(bf) - i);
}

#define err(line) {cst->err++;print_err(cst,line);}

static int _cmp_expr(struct _cmp_state* cst, struct _skip_list* skips, uchar nest);

static struct _skips* _cmp_new_skips(struct _cmp_state* cst)
{
	struct _skips* skips = cst->freeskip;

	if(skips)
		cst->freeskip = skips->next;
	else
		skips = (struct _skips*)tk_alloc(cst->t,sizeof(struct _skips),0);

	skips->next = 0;
	return skips;
}

static void _cmp_new_skiplist(struct _cmp_state* cst, struct _skip_list* list)
{
	list->top = 0;
	list->glevel = cst->glevel;
	list->home = cst->code_next;
	list->index = 0;
}

static void _cmp_free_skiplist(struct _cmp_state* cst, struct _skip_list* list)
{
	uint jmpto = cst->code_next;
	while(list->top)
	{
		struct _skips* tmp;

		while(list->index-- > 0)
		{
			ushort* ptr = (ushort*)(cst->code + list->top->skips[list->index]);
			*ptr = (jmpto - list->top->skips[list->index] - sizeof(ushort));
		}

		tmp = list->top->next;
		list->top->next = cst->freeskip;
		cst->freeskip = list->top;
		list->top = tmp;
	}
}

static void _cmp_add_to_skiplist(struct _cmp_state* cst, struct _skip_list* list, long entry)
{
	if(list->top == 0)
	{
		list->top = _cmp_new_skips(cst);
	}
	else if(list->index == NUMBER_OF_SKIPS)
	{
		struct _skips* tmp = _cmp_new_skips(cst);
		tmp->next = list->top;

		list->top = tmp;
		list->index = 0;
	}

	list->top->skips[list->index++] = entry;
}

static int _cmp_nextc(struct _cmp_state* cst)
{
	if(cst->c >= 0)
		cst->c = st_scan(cst->t,cst->body);

	return cst->c;
}

static int _cmp_comment(struct _cmp_state* cst)
{
	int c;
	do {
		c = _cmp_nextc(cst);
	} while(c > 0 && c != '\n' && c != '\r');
	return c;
}

static void _cmp_check_buffer(struct _cmp_state* cst, uint top)
{
	if(top >= cst->bsize)
	{
		uint diff = top - cst->bsize;

		cst->bsize += BUFFER_GROW > diff? BUFFER_GROW : diff;
		cst->opbuf = (char*)tk_realloc(cst->t,cst->opbuf,cst->bsize);
	}
}

static uint _cmp_push_struct(struct _cmp_state* cst, uint size)
{
	uint begin = cst->top;
	if(begin == 0)
		begin = 4;
	else if(begin & 3)
		begin += 4 - (cst->top & 3);
	cst->top = begin + size;

	_cmp_check_buffer(cst,cst->top);
	return begin;
}

static void _cmp_check_code(struct _cmp_state* cst, uint top)
{
	if(top >= cst->code_size)
	{
		uint diff = top - cst->code_size;

		cst->code_size += BUFFER_GROW > diff? BUFFER_GROW : diff;
		cst->code = (char*)tk_realloc(cst->t,cst->code,cst->code_size);
	}
}

// TODO: replace by static-hash
static int _cmp_keyword(const char* buffer)
{
	int i;
	for(i = 0; keywords[i]; i++)
	{
		if(strcmp(buffer,keywords[i]) == 0)
		return i + 1;
	}

	return 0;
}

// TODO: replace by static-hash
static const struct _cmp_buildin* _cmp_buildins(const char* buffer)
{
	int i;
	for(i = 0; buildins[i].id; i++)
	{
		if(strcmp(buffer,buildins[i].id) == 0)
		return &buildins[i];
	}

	return 0;
}

static void _cmp_whitespace(struct _cmp_state* cst)
{
	do{
		_cmp_nextc(cst);
	}
	while(whitespace(cst->c));
}

static uint _cmp_name(struct _cmp_state* cst)
{
	uint op = cst->top;
	while(alphanum(cst->c))
	{
		_cmp_check_buffer(cst,op);
		cst->opbuf[op++] = cst->c;
		_cmp_nextc(cst);
	}

	if(op == cst->top) return 0;

	_cmp_check_buffer(cst,op);
	cst->opbuf[op++] = '\0';
	return op - cst->top;
}

static uint _cmp_typename(struct _cmp_state* cst)
{
	uint begin = cst->top,lenall = 0;

	while(1)
	{
		uint len;
		if(whitespace(cst->c)) _cmp_whitespace(cst);

		len = _cmp_name(cst);
		if(len == 0)
		{
			cst->top = begin;
			return 0;
		}

		cst->top += len;
		lenall += len;

		if(whitespace(cst->c)) _cmp_whitespace(cst);

		if(cst->c != '.')
			break;

		_cmp_nextc(cst);
	}

	cst->top = begin;
	return lenall;
}

static void _cmp_emit0(struct _cmp_state* cst, uchar opc)
{
	_cmp_check_code(cst,cst->code_next + 1);
	cst->lastop = cst->code + cst->code_next;
	cst->code[cst->code_next++] = opc;
}

static void _cmp_emitS(struct _cmp_state* cst, uchar opc, char* param, ushort len)
{
	_cmp_check_code(cst,cst->code_next + len + 1 + sizeof(ushort));
	cst->lastop = cst->code + cst->code_next;
	cst->code[cst->code_next++] = opc;
	memcpy(cst->code + cst->code_next,(char*)&len,sizeof(ushort));
	cst->code_next += sizeof(ushort);
	memcpy(cst->code + cst->code_next,param,len);
	cst->code_next += len;
}

static void _cmp_emitIc(struct _cmp_state* cst, uchar opc, uchar imm)
{
	_cmp_check_code(cst,cst->code_next + 2);
	cst->lastop = cst->code + cst->code_next;
	cst->code[cst->code_next++] = opc;
	cst->code[cst->code_next++] = imm;
}

static void _cmp_emitIc2(struct _cmp_state* cst, uchar opc, uchar imm1, uchar imm2)
{
	_cmp_check_code(cst,cst->code_next + 3);
	cst->lastop = cst->code + cst->code_next;
	cst->code[cst->code_next++] = opc;
	cst->code[cst->code_next++] = imm1;
	cst->code[cst->code_next++] = imm2;
}

static void _cmp_emitIs(struct _cmp_state* cst, uchar opc, ushort imm)
{
	_cmp_check_code(cst,cst->code_next + 1 + sizeof(ushort));
	cst->lastop = cst->code + cst->code_next;
	cst->code[cst->code_next++] = opc;
	memcpy(cst->code + cst->code_next,(char*)&imm,sizeof(ushort));
	cst->code_next += sizeof(ushort);
}

static void _cmp_emitIs2(struct _cmp_state* cst, uchar opc, uchar imm1, ushort imm2)
{
	_cmp_check_code(cst,cst->code_next + 2 + sizeof(ushort));
	cst->lastop = cst->code + cst->code_next;
	cst->code[cst->code_next++] = opc;
	cst->code[cst->code_next++] = imm1;
	memcpy(cst->code + cst->code_next,(char*)&imm2,sizeof(ushort));
	cst->code_next += sizeof(ushort);
}

static void _cmp_update_imm(struct _cmp_state* cst, uint offset, ushort imm)
{
	ushort* ptr = (ushort*)(cst->code + offset);
	*ptr = imm;
}

static void _cmp_stack(struct _cmp_state* cst, int diff)
{
	cst->s_top += diff;
	if(diff > 0 && cst->s_top > cst->s_max)
		cst->s_max = cst->s_top;
}

static struct _cmp_var* _cmp_find_var(struct _cmp_state* cst, uint length)
{
	struct _cmp_var* var = 0;
	uint nxtvar = cst->top_var;

	while(nxtvar)
	{
		var = PEEK_VAR(nxtvar);
		if(length == var->leng &&
			memcmp(cst->opbuf + cst->top,cst->opbuf + var->name,length) == 0)
			return var;

		nxtvar = var->prev;
	}

	return 0;
}

static struct _cmp_var* _cmp_new_var(struct _cmp_state* cst, uint len)
{
	struct _cmp_var* var;
	uint begin,top = cst->top;

	cst->top += len;	// res.name
	begin = _cmp_push_struct(cst,sizeof(struct _cmp_var));
	var = PEEK_VAR(begin);

	var->prev = cst->top_var;
	var->id   = cst->v_top;
	var->name = top;
	var->leng = len;
	var->level = cst->glevel;

	if(var->id > 0xFF) err(__LINE__)	// max 256 visible vars a any time
	cst->top_var = begin;

	cst->v_top++;
	if(cst->v_top > cst->v_max) cst->v_max = cst->v_top;
	return var;
}

static struct _cmp_var* _cmp_def_var(struct _cmp_state* cst)
{
	uint len;

	if(cst->c != ':') err(__LINE__)
	_cmp_nextc(cst);
	len = _cmp_name(cst);
	if(len == 0)
	{
		err(__LINE__)
		return 0;
	}

	if(_cmp_find_var(cst,len))
	{
		err(__LINE__)
		return 0;
	}

	return _cmp_new_var(cst,len);
}

static void _cmp_free_var(struct _cmp_state* cst)
{
	struct _cmp_var* var = 0;
	uint nxtvar = cst->top_var;
	uint from,count = 0;
	cst->glevel--;

	while(nxtvar)
	{
		var = PEEK_VAR(nxtvar);
		if(var->level <= cst->glevel) break;
		count++;
		from = var->id;
		nxtvar = var->prev;
	}

	if(count != 0)
	{
		if(cst->glevel != 0)	// free on glevel==0 done by OP_END
			_cmp_emitIc2(cst,OP_FREE,count,from);

		cst->v_top -= count;

		if(nxtvar)
		{
			cst->top_var = nxtvar;
			cst->top = nxtvar + sizeof(struct _cmp_var);
		}
		else
			cst->top_var = 0;
	}
}

static void _cmp_compute_free_var(struct _cmp_state* cst, uint to_glevel)
{
	struct _cmp_var* var = 0;
	uint nxtvar = cst->top_var;
	uint from,count = 0;

	while(nxtvar)
	{
		var = PEEK_VAR(nxtvar);
		if(var->level <= to_glevel) break;
		count++;
		from = var->id;
		nxtvar = var->prev;
	}

	if(count != 0 && cst->glevel != 0)
		_cmp_emitIc2(cst,OP_FREE,count,from);

	cst->v_top -= count;
}

static struct _cmp_var* _cmp_var(struct _cmp_state* cst)
{
	int len;
	_cmp_nextc(cst);
	len = _cmp_name(cst);
	if(len > 0)
	{
		struct _cmp_var* var = _cmp_find_var(cst,len);
		if(var)
			return var;
		else
			err(__LINE__)
	}
	else
		err(__LINE__)

	return 0;
}

/* "test ""test"" 'test'" | 'test ''test'' "test"' */
static void _cmp_string(struct _cmp_state* cst, st_ptr* out, int c)
{
	char buffer[BUFFER_GROW];
	int ic = 0,i = 0;

	while(1)
	{
		ic = _cmp_nextc(cst);
		if(ic == c)
		{
			ic = _cmp_nextc(cst);
			if(ic != c)
				break;
		}
		else if(ic <= 0)
		{
			err(__LINE__)
			return;
		}

		buffer[i++] = ic;
		if(i == BUFFER_GROW)
		{
			if(st_append(cst->t,out,buffer,i)) err(__LINE__)
			i = 0;
		}
	}

//	buffer[i++] = '\0';
	if(st_append(cst->t,out,buffer,i)) err(__LINE__)
}

static void _cmp_str(struct _cmp_state* cst, uint app)
{
	if(app == 0)
	{
		cst->cur_string = cst->strs;
		st_insert(cst->t,&cst->cur_string,(cdat)&cst->stringidx,sizeof(ushort));
		_cmp_emitIs(cst,OP_STR,cst->stringidx);
		_cmp_stack(cst,1);
		cst->stringidx++;
	}
	
	_cmp_string(cst,&cst->cur_string,cst->c);
}

// output all operators
static void _cmp_op_clear(struct _cmp_state* cst, struct _cmp_op** otop)
{
	struct _cmp_op* oper = *otop;
	*otop = 0;
	while(oper)
	{
		_cmp_emit0(cst,oper->opc);
		_cmp_stack(cst,-1);

		oper = oper->prev;
	}
}

// push new operator - release higher precedens first
static void _cmp_op_push(struct _cmp_state* cst, struct _cmp_op** otop, uchar opc, uchar prec)
{
	struct _cmp_op* oper = *otop;
	while(oper)
	{
		if(oper->prec >= prec && oper->opc != 0)
		{
			_cmp_emit0(cst,oper->opc);
			_cmp_stack(cst,-1);
			*otop = oper->prev;
		}
		else break;

		oper = oper->prev;
	}

	oper = (struct _cmp_op*)tk_alloc(cst->t,sizeof(struct _cmp_op),0);

	oper->opc = opc;
	oper->prec = prec;

	oper->prev = *otop;
	*otop = oper;
}

static void _cmp_op_pop(struct _cmp_state* cst, struct _cmp_op** otop)
{
	if(*otop != 0)
		*otop = (*otop)->prev;
}

static uint _cmp_call(struct _cmp_state* cst, uchar nest)
{
	uint term,pcount = 0;

	while(1)
	{
		pcount++;
		term = _cmp_expr(cst,0,NEST_EXPR);	// construct parameters
		if(term != ',')
			break;

		_cmp_nextc(cst);
	}
	if(term != ')') err(__LINE__)
	_cmp_nextc(cst);

	if(*cst->lastop == OP_NULL)
	{
		_cmp_stack(cst,-1);
		cst->code_next--;
		pcount--;
	}

	if(nest & NEST_EXPR)
	{
		_cmp_emitIc(cst,OP_DOCALL_N,pcount);
		_cmp_stack(cst,-pcount);
	}
	else
	{
		_cmp_emitIc(cst,OP_DOCALL,pcount);
		_cmp_stack(cst,-pcount - 1);
	}

	return pcount;
}

/*
	var :a,:b = 1,2;
	var :a = 1,:b = 2,:c = x;
*/
static uint _cmp_var_assign(struct _cmp_state* cst, const uint state)
{
	struct _cmp_var* vars[16];
	int i;

	for(i = 0; i < 16; i++)
	{
		vars[i] = (state == ST_DEF)? _cmp_def_var(cst) : _cmp_var(cst);

		if(whitespace(cst->c)) _cmp_whitespace(cst);

		if(cst->c == '=')
		{
			int exprs = 0;
			do
			{
				exprs++;
				_cmp_nextc(cst);
				if(_cmp_expr(cst,0,NEST_EXPR) != ',')
					break;
			}
			while(exprs <= i);

			while(exprs-- > 0)
			{
				_cmp_emitIc(cst,OP_AVAR,vars[exprs]? vars[exprs]->id : 0);
				_cmp_stack(cst,-1);
			}

			if(cst->c == ';')
				break;
			if(cst->c != ',')
			{
				err(__LINE__)
				break;
			}
			i = 0;
		}
		
		if(cst->c == ',')
		{
			_cmp_whitespace(cst);
			if(cst->c != ':')
			{
				err(__LINE__)
				return ST_0;
			}
		}
		else if(cst->c != ';')
		{
			if(i != 0) err(__LINE__)

			_cmp_emitIc(cst,OP_LVAR,vars[i]? vars[i]->id : 0);
			_cmp_stack(cst,1);
			return ST_VAR;
		}
		else break;
	}

	if(i == 16) err(__LINE__)
	else _cmp_nextc(cst);

	return ST_0;
}

#define chk_state(legal) if(((legal) & state) == 0) err(__LINE__)
/*
* path.:bindvar{path_must_exsist;other:endvar;} do -> exit
*/
static void _cmp_match_expr(struct _cmp_state* cst)
{
	uint state = ST_0;
	uint level = 0;

	while(1)
	{
		switch(cst->c)
		{
		case '{':
			chk_state(ST_0|ST_ALPHA|ST_VAR)
			level++;
			state = ST_0;
			break;
		case '}':
			chk_state(ST_0|ST_ALPHA|ST_VAR)
			if(level > 0) level--;
			else err(__LINE__)
			state = ST_0;
			break;
		case ';':
		case ',':
			chk_state(ST_ALPHA|ST_VAR)
			state = ST_0;
			break;
		case '.':
			chk_state(ST_ALPHA|ST_VAR)
			state = ST_DOT;
			break;
		case ':':
			chk_state(ST_0|ST_DOT|ST_ALPHA)
			{
				uint len;
				_cmp_nextc(cst);
				len = _cmp_name(cst);
				if(len > 0)
				{
					struct _cmp_var* var = _cmp_find_var(cst,len);
					if(var == 0)
						var = _cmp_new_var(cst,len);
				}
			}
			state = ST_VAR;
			continue;
		case '#':
			_cmp_comment(cst);
			break;
		case ' ':
		case '\t':
		case '\n':
		case '\r':
			break;
		default:
			if(alpha(cst->c))
			{
				uint len = _cmp_name(cst);
				if(len <= KW_MAX_LEN)
				{
					uint kw = _cmp_keyword(cst->opbuf + cst->top);
					if(kw != 0)
					{
						if(kw != KW_DO)
							err(__LINE__)
						else if(level != 0)
							err(__LINE__)
						else chk_state(ST_0|ST_ALPHA|ST_VAR)

						return;
					}
				}

				chk_state(ST_0|ST_DOT|ST_VAR)
				state = ST_ALPHA;
				continue;
			}
			else
			{
				err(__LINE__)
				return;
			}
		}
		_cmp_nextc(cst);
	}
}

static void _cmp_new(struct _cmp_state* cst)
{
	uint stack = cst->s_top;
	uint state = ST_0;
	uint level = 1;

	_cmp_emit0(cst,OP_MERGE);

	_cmp_nextc(cst);
	while(1)
	{
		switch(cst->c)
		{
		case '=':
			chk_state(ST_ALPHA)
			_cmp_nextc(cst);
			{
				uint term = _cmp_expr(cst,0,PURE_EXPR);
				//_cmp_emit0(cst,OP_POPW);
				// assign out stack +2
				_cmp_stack(cst,-1);
				state = ST_0;
				if(term != ';' && term != '}')
					err(__LINE__)
			}
			break;
		case '}':
			chk_state(ST_0|ST_ALPHA)
			if(cst->s_top != stack)
			{
				_cmp_emit0(cst,OP_POPW);
				_cmp_stack(cst,-1);
			}
			if(level > 0) level--; else err(__LINE__)
			if(level == 0) return;
			state = ST_0;
			break;
		case '{':
			chk_state(ST_0|ST_ALPHA)
			state = ST_0;
			level++;
			break;
		case '[':
			chk_state(ST_0|ST_ALPHA|ST_DOT)
			_cmp_nextc(cst);
			if(_cmp_expr(cst,0,NEST_EXPR) != ']') err(__LINE__);
			_cmp_emit0(cst,OP_WIDX);
			state = ST_ALPHA;
			break;
		case ',':
		case ';':
			chk_state(ST_ALPHA)
			_cmp_emit0(cst,OP_POPW);
			_cmp_stack(cst,-1);
			state = ST_0;
			break;
		case '.':							// ONLY Alpha -> build path incl dots
			chk_state(ST_ALPHA)
			state = ST_DOT;
			break;
		case '#':
			_cmp_comment(cst);
			break;
		case ' ':
		case '\t':
		case '\n':
		case '\r':
			break;
		default:
			if(alpha(cst->c))
			{
				uint len = _cmp_name(cst);
				if(len <= KW_MAX_LEN && _cmp_keyword(cst->opbuf + cst->top) != 0) err(__LINE__)
				switch(state)
				{
				case ST_0:
					_cmp_emitS(cst,OP_DMVW,cst->opbuf + cst->top,len);
					_cmp_stack(cst,1);
					break;
				case ST_DOT:
					_cmp_emitS(cst,OP_MVW,cst->opbuf + cst->top,len);
					break;
				default:
					err(__LINE__)
				}
				state = ST_ALPHA;
				continue;
			}
			else
			{
				err(__LINE__)
				return;
			}
		}
		_cmp_nextc(cst);
	}
}

// in nested exprs dont out the first element, but out all following (concating to the first)
//	if(state & (ST_ALPHA|ST_STR|ST_VAR|ST_NUM) && *cst->lastop != OP_DOCALL){
#define chk_out()\
	if(state & (ST_ALPHA|ST_STR|ST_VAR|ST_NUM)){\
		_cmp_op_clear(cst,&otop);\
		if(*cst->lastop != OP_OUT && *cst->lastop != OP_DOCALL){\
			if(nest == NEST_EXPR)\
				_cmp_emit0(cst,OP_CAT);\
			else\
			{_cmp_emit0(cst,OP_OUT);_cmp_stack(cst,-1);}}\
		_cmp_op_push(cst,&otop,OP_OUT,0);}

// direct call doesnt leave anything on the stack - force it
// if the next instr. needs the return-value
#define chk_call() if(*cst->lastop == OP_DOCALL)\
	{*cst->lastop = OP_DOCALL_N;_cmp_stack(cst,1);}

#define num_op(opc,prec) \
			chk_state(ST_ALPHA|ST_VAR|ST_NUM)\
			chk_call()\
			_cmp_op_push(cst,&otop,opc,prec);\
			state = ST_NUM_OP;\
			nest |= NEST_EXPR;\

static int _cmp_block_expr_nofree(struct _cmp_state* cst, struct _skip_list* skips, uchar nest)
{
	uint exittype,stack = cst->s_top;
	cst->glevel++;
	while(1)
	{
		exittype = _cmp_expr(cst,skips,nest);
		if((stack != cst->s_top) && (nest != NEST_EXPR))
		{
			_cmp_emit0(cst,OP_OUTL);
			_cmp_stack(cst,-1);
		}
		if(exittype == ';')
			_cmp_nextc(cst);
		else
			break;
	}
	return exittype;
}

static int _cmp_block_expr(struct _cmp_state* cst, struct _skip_list* skips, uchar nest)
{
	uint exittype = _cmp_block_expr_nofree(cst,skips,nest);
	_cmp_free_var(cst);
	return exittype;
}

static void _cmp_fwd_loop(struct _cmp_state* cst, uint loop_coff, uchar nest, uchar opc)
{
	struct _skip_list sl;
	uint coff = cst->code_next;
	_cmp_new_skiplist(cst,&sl);
	_cmp_emitIs(cst,opc,0);
	_cmp_stack(cst,-1);
	if(_cmp_block_expr_nofree(cst,&sl,nest) != 'e') err(__LINE__)
	_cmp_emitIs(cst,OP_LOOP,cst->code_next - loop_coff + 1 + sizeof(ushort));
	_cmp_update_imm(cst,coff + 1,cst->code_next - coff - 1 - sizeof(ushort));
	_cmp_free_skiplist(cst,&sl);
	_cmp_free_var(cst);
}

static int _cmp_expr(struct _cmp_state* cst, struct _skip_list* skips, uchar nest)
{
	struct _cmp_op* otop = 0;
	uint state = ST_0;
	uint stack = cst->s_top;

	while(1)
	{
		switch(cst->c)
		{
		case -1:
		case 0:
			if(cst->glevel != 1) err(__LINE__)
			chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM)
			_cmp_op_clear(cst,&otop);
			return -1;
		case '+':
			num_op(OP_ADD,4)
			break;
		case '-':
			if(state & (ST_0|ST_NUM_OP))
			{
				_cmp_op_push(cst,&otop,OP_NEG,16);
				_cmp_stack(cst,1);
				state = ST_NUM_OP;
			}
			else
			{
				num_op(OP_SUB,4)
			}
			break;
		case '*':
			num_op(OP_MUL,6)
			break;
		case '/':
			num_op(OP_DIV,5)
			break;
		case '<':
			chk_state(ST_ALPHA|ST_VAR|ST_NUM)
			chk_call()
			state = ST_NUM_OP;
			_cmp_nextc(cst);
			if(cst->c == '=')
			{
				_cmp_op_push(cst,&otop,OP_GE,1);
				break;
			}
			else if(cst->c == '>')
			{
				_cmp_op_push(cst,&otop,OP_NE,1);
				break;
			}
			_cmp_op_push(cst,&otop,OP_GT,1);
			continue;
		case '>':
			chk_state(ST_ALPHA|ST_VAR|ST_NUM)
			chk_call()
			state = ST_NUM_OP;
			_cmp_nextc(cst);
			if(cst->c == '=')
			{
				_cmp_op_push(cst,&otop,OP_LE,1);
				break;
			}
			_cmp_op_push(cst,&otop,OP_LT,1);
			continue;
		case '=':
			chk_call()
			if(nest)
			{
				chk_state(ST_ALPHA|ST_VAR|ST_NUM)
				_cmp_op_push(cst,&otop,OP_EQ,1);
				state = ST_NUM_OP;
			}
			else
			{
				chk_state(ST_ALPHA)
				_cmp_nextc(cst);
				if(_cmp_expr(cst,0,NEST_EXPR) != ';') err(__LINE__)
				_cmp_emit0(cst,OP_SET);
				_cmp_stack(cst,-2);
				state = ST_0;
			}
			break;
		case ':':	// var (all states ok)
			if(state == ST_0 && (nest & NEST_EXPR) == 0)
				state = _cmp_var_assign(cst,ST_0);
			else
			{
				struct _cmp_var* var;
				if(state == ST_DOT) err(__LINE__)
				chk_out()
				var = _cmp_var(cst);
				if(var)
				{
					_cmp_emitIc(cst,OP_LVAR,var->id);
					_cmp_stack(cst,1);
				}
				state = ST_VAR;
			}
			continue;
		case '\'':
		case '"':
			chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
			if(state != ST_STR)
				chk_out()
			_cmp_str(cst,state == ST_STR);
			state = ST_STR;
			continue;
		case '(':
			// st_alpha|st_var -> function-call
			_cmp_nextc(cst);
			if(state & (ST_ALPHA|ST_VAR))
			{
				chk_call()
				_cmp_call(cst,nest);
				state = ST_ALPHA;

				// dont out method 
				if(otop != 0 && otop->opc == OP_OUT)
					_cmp_op_pop(cst,&otop);
			}
			else
			{
				chk_state(ST_0|ST_STR|ST_NUM_OP)
				chk_out()
				if(_cmp_expr(cst,0,NEST_EXPR) != ')') err(__LINE__)
				if(*cst->lastop == OP_NULL) err(__LINE__)
				state = ST_ALPHA;
			}
			break;
		case '[':
			// TODO: build chain if [][][] = x
			chk_state(ST_ALPHA|ST_VAR)
			chk_call()
			_cmp_nextc(cst);
			if(_cmp_expr(cst,0,NEST_EXPR) != ']') err(__LINE__);
			if(*cst->lastop == OP_NULL) err(__LINE__)
			_cmp_emit0(cst,OP_RIDX);
			_cmp_stack(cst,-1);
			state = ST_ALPHA;
			break;
		case ']':
		case ')':
			chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM)
			_cmp_op_clear(cst,&otop);
			if((nest & NEST_EXPR) == 0) err(__LINE__)
			if(stack == cst->s_top) {_cmp_emit0(cst,OP_NULL);_cmp_stack(cst,1);}
			return cst->c;
		case '{':
			chk_state(ST_0|ST_ALPHA|ST_VAR)
			chk_call()
			if(stack == cst->s_top)
				_cmp_new(cst);
			else
			{
				_cmp_new(cst);
				_cmp_emit0(cst,OP_POPW);
				_cmp_stack(cst,-1);
			}
			state = ST_0;
			break;
		case '}':
		case ';':
		case ',':
			chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM)
			_cmp_op_clear(cst,&otop);
			if(stack == cst->s_top && nest == NEST_EXPR) {_cmp_emit0(cst,OP_NULL);_cmp_stack(cst,1);}
			return cst->c;
		case '.':
			chk_state(ST_ALPHA|ST_VAR)
			chk_call()
			state = ST_DOT;
			break;
		case '#':
			_cmp_comment(cst);
			break;
		case ' ':
		case '\t':
		case '\n':
		case '\r':
			break;
		default:
			if(num(cst->c))
			{
				short val = 0;
				chk_state(ST_0|ST_NUM_OP)
				do
				{
					val *= 10;
					val += cst->c - '0';
					_cmp_nextc(cst);
				}
				while(num(cst->c));

				_cmp_emitIs(cst,OP_IMM,val);
				_cmp_stack(cst,1);
				state = ST_NUM;
				continue;
			}
			else if(alpha(cst->c))
			{
				uint len;
				len = _cmp_name(cst);
				switch(len > KW_MAX_LEN? 0 :_cmp_keyword(cst->opbuf + cst->top))
				{
				case KW_DO:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM)
					if(nest & (NEST_EXPR))
					{
						_cmp_op_clear(cst,&otop);
						if(nest == NEST_EXPR && stack == cst->s_top) {_cmp_emit0(cst,OP_NULL);_cmp_stack(cst,1);}
						return 'd';
					}
					else
					{
						struct _skip_list sl;
						chk_out()
						_cmp_new_skiplist(cst,&sl);
						if(_cmp_block_expr(cst,&sl,nest) != 'e') err(__LINE__)
						_cmp_free_skiplist(cst,&sl);
					}
					state = nest? ST_ALPHA : ST_0;
					continue;
				case KW_END:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM|ST_BREAK)
					_cmp_op_clear(cst,&otop);
					if(nest == NEST_EXPR && stack == cst->s_top) {_cmp_emit0(cst,OP_NULL);_cmp_stack(cst,1);}
					return 'e';
				case KW_IF:	// if expr do bexpr [elseif expr do bexpr [else bexpr]] end
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM|ST_NUM_OP)
					chk_out()
					{
						struct _skip_list sl;
						uint need_else = nest & NEST_EXPR;
						_cmp_new_skiplist(cst,&sl);
						while(1)
						{
							uint term,coff;
							if(_cmp_expr(cst,0,NEST_EXPR) != 'd') err(__LINE__)
							coff = cst->code_next;
							_cmp_emitIs(cst,OP_BZ,0);
							_cmp_stack(cst,-1);
							term = _cmp_block_expr(cst,skips,nest);
							if(nest == NEST_EXPR) _cmp_stack(cst,-1);

							if(term == 'i' || term == 'l')
							{
								_cmp_add_to_skiplist(cst,&sl,cst->code_next + 1);
								_cmp_emitIs(cst,OP_BR,0);
								// update - we need the "skip-jump" before
								_cmp_update_imm(cst,coff + 1,cst->code_next - coff - 1 - sizeof(ushort));

								if(term == 'i') continue;
								else need_else = 0;

								term = _cmp_block_expr(cst,skips,nest);
							}
							else
								_cmp_update_imm(cst,coff + 1,cst->code_next - coff - 1 - sizeof(ushort));

							if(term != 'e') err(__LINE__)	// must end with "end"
							if(need_else) err(__LINE__)	// nested if without else
							break;
						}
						_cmp_free_skiplist(cst,&sl);
					}
					state = nest? ST_ALPHA : ST_0;
					continue;
				case KW_ELSEIF:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM|ST_BREAK)
					if(nest == NEST_EXPR && stack == cst->s_top) {_cmp_emit0(cst,OP_NULL);_cmp_stack(cst,1);}
					_cmp_op_clear(cst,&otop);
					return 'i';
				case KW_ELSE:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM|ST_BREAK)
					if(nest == NEST_EXPR && stack == cst->s_top) {_cmp_emit0(cst,OP_NULL);_cmp_stack(cst,1);}
					_cmp_op_clear(cst,&otop);
					return 'l';
				case KW_WHILE:	// while expr do bexpr end / do bexpr while expr end
				case KW_UNTIL:	// until expr do bexpr end / do bexpr until expr end
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					{
						uint until = *(cst->opbuf + cst->top) == 'u';
						uint loop_coff = cst->code_next;
						int ret = _cmp_expr(cst,0,NEST_EXPR);
						if(ret == 'd')	// while expr do ... end
							_cmp_fwd_loop(cst,loop_coff,nest,(until)? OP_BNZ : OP_BZ);
						else if(ret == 'e')	// do .. while expr end
						{
							if(skips == 0) err(__LINE__)
							else
							{
								// TODO: OP_NZLOOP / until = OP_ZLOOP
								_cmp_emitIs(cst,(until)? OP_ZLOOP : OP_NZLOOP,cst->code_next - skips->home + 1 + sizeof(ushort));
								_cmp_stack(cst,-1);
							}
							return 'e';
						}
						else err(__LINE__)
					}
					state = ST_0;
					continue;
				case KW_REPEAT:	// repeat loop/do..end
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					if(skips == 0) err(__LINE__)
					else
					{
						_cmp_compute_free_var(cst,skips->glevel + 1);
						_cmp_emitIs(cst,OP_LOOP,cst->code_next - skips->home + 1 + sizeof(ushort));
					}
					state = ST_BREAK;
					continue;
				case KW_BREAK:	// break loop/do..end
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					if(skips == 0) err(__LINE__)
					else
					{
						_cmp_compute_free_var(cst,skips->glevel + 1);
						_cmp_add_to_skiplist(cst,skips,cst->code_next + 1);
						_cmp_emitIs(cst,OP_BR,0);
					}
					state = ST_BREAK;
					continue;
				case KW_FOR:	// for var [,matcher] = expr do bexpr end
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					{
						uint loop_coff;
						_cmp_emit0(cst,OP_NULL);//OP_FOR);
						loop_coff = cst->code_next;
						if(whitespace(cst->c)) _cmp_whitespace(cst);
						if(_cmp_expr(cst,0,NEST_EXPR) != 'd') err(__LINE__)
						_cmp_fwd_loop(cst,loop_coff,nest,OP_NULL);//OP_FOR_START);
					}
					state = ST_0;
					continue;
/*					// [struct|collection].each it_expr [sort sort-rules] do bexpr end
					chk_state(ST_DOT)
					{
						uint loop_coff = cst->code_next;
						cst->glevel++;
						_cmp_match_expr(cst);
						_cmp_fwd_loop(cst,loop_coff,nest,OP_BZ);	// OP_EACH
						_cmp_free_var(cst);
					}
					state = ST_0; */
					continue;
				case KW_OPEN:	// open expr [do bexpr] end
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					if(nest) err(__LINE__)
					{
						int ret = _cmp_expr(cst,0,NEST_EXPR);
						_cmp_emit0(cst,OP_OPEN);
						//_cmp_stack(cst,1);
						if(ret == 'd')
						{
							// TODO: handle skips & raises ...
							if(_cmp_block_expr(cst,0,PROC_EXPR) != 'e') err(__LINE__)
						}
						else if(ret != 'e') err(__LINE__)
						_cmp_emit0(cst,OP_OPEN_POP);
						_cmp_stack(cst,-1);
					}
					state = ST_0;
					continue;
				case KW_VAR:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					if(nest & NEST_EXPR) err(__LINE__)
					if(whitespace(cst->c)) _cmp_whitespace(cst);
					state = _cmp_var_assign(cst,ST_DEF);
					continue;
				case KW_NEW:					// new typename(param_list) / [obj].new(param_list)
					if(state == ST_0)
					{
						uint len = _cmp_typename(cst);
						if(len == 0)
						{
							err(__LINE__)
							break;
						}

						_cmp_emitS(cst,OP_NEW,cst->opbuf + cst->top,len);
						_cmp_stack(cst,1);
					}
					else if(state == ST_DOT)
						_cmp_emit0(cst,OP_CLONE);
					else err(__LINE__)

					if(cst->c != '(') err(__LINE__)
					else
						_cmp_call(cst,nest);
					state = ST_ALPHA;
					break;
				case KW_AND:
					chk_state(ST_ALPHA|ST_STR|ST_VAR)
					{
						uint term,coff = cst->code_next;
						_cmp_emitIs(cst,OP_BZ,0);			// TODO: no-pop BZ
						_cmp_stack(cst,-1);
						term = _cmp_expr(cst,skips,nest);
						_cmp_update_imm(cst,coff + 1,cst->code_next - coff - 1 - sizeof(ushort));
						return term;
					}
				case KW_OR:
					chk_state(ST_ALPHA|ST_STR|ST_VAR)
					{
						uint term,coff = cst->code_next;
						_cmp_emitIs(cst,OP_BNZ,0);			// TODO: no-pop BNZ
						_cmp_stack(cst,-1);
						term = _cmp_expr(cst,skips,nest);
						_cmp_update_imm(cst,coff + 1,cst->code_next - coff - 1 - sizeof(ushort));
						return term;
					}
				case KW_NOT:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					{
						uint term;
						term = _cmp_expr(cst,skips,nest);
						_cmp_emit0(cst,OP_NOT);
						return term;
					}
				case KW_SUPER:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM_OP)
					chk_out()
					_cmp_emit0(cst,OP_SUPER);
					_cmp_stack(cst,1);
					state = ST_ALPHA;
					continue;
				case KW_THIS:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_NUM_OP)
					chk_out()
					_cmp_emit0(cst,OP_OBJ);
					_cmp_stack(cst,1);
					state = ST_ALPHA;
					continue;
				case KW_NEXT:
					chk_state(ST_ALPHA|ST_STR|ST_VAR|ST_NUM)
					if(otop != 0 && otop->opc == OP_OUT)
						_cmp_op_pop(cst,&otop);
					_cmp_op_clear(cst,&otop);
					_cmp_emit0(cst,OP_NEXT);
					_cmp_stack(cst,-1);
					state = ST_0;
					continue;
				case KW_GOTO:	// goto state-name [if expr]
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					if(cst->glevel != 1) err(__LINE__)
					return 'g';
				case KW_HANDLE:	// handle it_expr do bexpr end
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					if(nest) err(__LINE__)
					if(cst->glevel != 1) err(__LINE__)
					return 'h';
				case KW_RAISE:	// raise {new}
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					if(nest & NEST_EXPR) err(__LINE__)
					if(whitespace(cst->c)) _cmp_whitespace(cst);
					_cmp_emit0(cst,OP_NULL);	// OP_PRE_RAISE
					_cmp_stack(cst,1);
					if(cst->c != '{') err(__LINE__)
					else _cmp_new(cst);
					_cmp_emit0(cst,OP_NULL);	// OP_RAISE
					_cmp_stack(cst,-1);
					state = ST_BREAK;
					continue;
				case KW_SWITCH:		// switch expr case const do bexpr | default bexpr | end
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR)
					chk_out()
					{
						struct _skip_list sl;
						uint defh = 0;
						uint term = _cmp_expr(cst,0,NEST_EXPR);
						_cmp_new_skiplist(cst,&sl);

						// emit mapnr defaulthandler

						while(term != 'e')
						{
							switch(term)
							{
							case 'c':
								// todo: parse constant and add to map
								term = _cmp_block_expr(cst,&sl,nest);
								break;
							case 'f':
								if(defh != 0) err(__LINE__)
								defh = cst->code_next;
								term = _cmp_block_expr(cst,&sl,nest);
								break;
							case 'e':
								break;
							default:
								err(__LINE__)
								term = 'e';
							}
						}

						if(defh == 0 && nest & NEST_EXPR) err(__LINE__)	// nested switch needs default-branche
						_cmp_free_skiplist(cst,&sl);
					}
					state = nest? ST_ALPHA : ST_0;
					continue;
				case KW_CASE:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_BREAK)
					_cmp_op_clear(cst,&otop);
					if(nest == NEST_EXPR && stack == cst->s_top) {_cmp_emit0(cst,OP_NULL);_cmp_stack(cst,1);}
					return 'c';
				case KW_DEFAULT:
					chk_state(ST_0|ST_ALPHA|ST_STR|ST_VAR|ST_BREAK)
					_cmp_op_clear(cst,&otop);
					if(nest == NEST_EXPR && stack == cst->s_top) {_cmp_emit0(cst,OP_NULL);_cmp_stack(cst,1);}
					return 'f';
				default:
					chk_out()
					{
						// compile buildin-functions
						const struct _cmp_buildin* cmd = _cmp_buildins(cst->opbuf + cst->top);

						if(cmd != 0)
						{
							uint params;
							if(cst->c != '(') err(__LINE__);
							_cmp_nextc(cst);
							params = _cmp_call(cst,NEST_EXPR);
							if(params > cmd->max_param || params < cmd->min_param) err(__LINE__)

							*cst->lastop = (state & ST_DOT)? cmd->opcode_obj : cmd->opcode;
							if(*cst->lastop == 0) err(__LINE__)
							// for now: all buildins leave a value on the stack
							_cmp_stack(cst,1);
						}
						else if(state & ST_DOT)
							_cmp_emitS(cst,OP_MV,cst->opbuf + cst->top,len);
						else
						{
							_cmp_emitS(cst,OP_OMV,cst->opbuf + cst->top,len);
							_cmp_stack(cst,1);
						}
						state = ST_ALPHA;
					}
					continue;
				}
			}
			else
			{
				err(__LINE__)
				return 0;
			}
		}
		_cmp_nextc(cst);
	}
}

// setup call-site and funspace
static int _cmp_header(struct _cmp_state* cst)
{
	st_ptr params;

	// strings
	cst->strs = cst->root;
	st_insert(cst->t,&cst->strs,"S",2);

	params = cst->root;
	st_insert(cst->t,&params,"P",2);

	_cmp_whitespace(cst);
	if(cst->c == ':')
		while(1)
		{
			struct _cmp_var* var = _cmp_def_var(cst);
			uint coff = cst->code_next;

			_cmp_emitIs2(cst,OP_DEFP,cst->params++,0);

			if(whitespace(cst->c)) _cmp_whitespace(cst);

			// set a default-value
			if(cst->c == '=')
			{
				_cmp_stack(cst,1);
				_cmp_nextc(cst);
				_cmp_expr(cst,0,NEST_EXPR);
				// update branch-offset
				_cmp_update_imm(cst,coff + 2,cst->code_next - coff - 2 - sizeof(ushort));
				// mark optional
				st_append(cst->t,&params,"?",1);
			}
			else
				cst->code_next = coff;	// undo OP_DEFP

			if(var)	// append-param name
				st_append(cst->t,&params,cst->opbuf + var->name,var->leng);

			if(cst->c == ',')
			{
				_cmp_whitespace(cst);
				if(cst->c == ':') continue;
				else err(__LINE__)
			}
			break;
		}

	return (cst->c == ')'? 0 : __LINE__);
}

// setup _cmp_state
static void _cmp_init(struct _cmp_state* cst, task* t, st_ptr* body, st_ptr* ref, cle_pipe* response, void* data)
{
	memset(cst,0,sizeof(struct _cmp_state));	// clear struct. MOST vars have default 0 value
	// none-0 defaults:
	cst->body = body;
	cst->t = t;
	cst->root = *ref;
	cst->s_max = cst->s_top = 1;	// output-context

	cst->response = response;
	cst->data = data;

	// begin code
	_cmp_emit0(cst,OP_BODY);
	cst->code_next += sizeof(ushort)*2 + 3;
}

static void _cmp_end(struct _cmp_state* cst)
{
	tk_mfree(cst->t,cst->opbuf);

	// make body
	if(cst->err == 0)
	{
		ushort* ptr;
		_cmp_emit0(cst,OP_END);

		cst->code[1] = cst->params;	// max-params
		cst->code[2] = cst->v_max;	// max-vars
		cst->code[3] = cst->s_max;	// max-stack
		ptr = (ushort*)(cst->code + 4);
		*ptr = cst->code_next;		// codesize
		ptr = (ushort*)(cst->code + 6);
		*ptr = cst->first_handler;	// first handler

		st_insert(cst->t,&cst->root,"B",2);
		st_insert(cst->t,&cst->root,cst->code,cst->code_next);
	}

	tk_mfree(cst->t,cst->code);
}

int cmp_method(task* t, st_ptr* ref, st_ptr* body, cle_pipe* response, void* data, const uint is_handler)
{
	struct _cmp_state cst;
	int ret,codebegin;

	_cmp_init(&cst,t,body,ref,response,data);
	codebegin = cst.code_next;

	// create header:
	ret = _cmp_header(&cst);
	if(ret == 0)
	{
		uint br_prev_handler = 0;
		_cmp_nextc(&cst);
		// compile body
		ret = _cmp_block_expr(&cst,0,PROC_EXPR);
		while(ret == 'g')
		{
			uint len;
			if(is_handler == 0)
			{cst.err++; print_err(&cst,__LINE__);}

			len = _cmp_typename(&cst);
			if(len < 1) {cst.err++; print_err(&cst,__LINE__);}

			if(whitespace(cst.c)) _cmp_whitespace(&cst);

			// look for "if" or end-of-goto's
			len = _cmp_name(&cst);
			if(len > 0 && len <= KW_MAX_LEN)
			{
				ret = _cmp_keyword(cst.opbuf + cst.top);
				if(ret != KW_IF)
					break;
				ret = _cmp_expr(&cst,0,NEST_EXPR);
			}
			else
			{
				if(cst.c != -1) {cst.err++; print_err(&cst,__LINE__);}
				break;
			}
		}

		while(ret != -1 && ret != 'e')
		{
			if(ret == 'h')	// (exception) handle it-expr do bexpr end|handle... 
			{
				_cmp_emit0(&cst,OP_END);		// end func/expr here - begin handler-code
				if(cst.first_handler == 0) cst.first_handler = cst.code_next - codebegin;
				else
					_cmp_update_imm(&cst,br_prev_handler + 1,cst.code_next - br_prev_handler - 1 - sizeof(ushort));

				_cmp_match_expr(&cst);

				br_prev_handler = cst.code_next;
				_cmp_emitIs(&cst,OP_BR,0);
			}
			else
			{cst.err++; print_err(&cst,__LINE__);break;}

			ret = _cmp_block_expr(&cst,0,PROC_EXPR);
		}

		if(br_prev_handler != 0)
			_cmp_update_imm(&cst,br_prev_handler + 1,cst.code_next - br_prev_handler - 1 - sizeof(ushort));
	}
	else cst.err++;

	_cmp_end(&cst);

	return (cst.err > 0);
}

int cmp_expr(task* t, st_ptr* ref, st_ptr* body, cle_pipe* response, void* data)
{
	struct _cmp_state cst;
	uint ret;

	_cmp_init(&cst,t,body,ref,response,data);

	// strings
	cst.strs = cst.root;
	st_insert(t,&cst.strs,"S",2);

	_cmp_nextc(&cst);
	ret = _cmp_block_expr(&cst,0,PURE_EXPR);
	if(ret != -1 && ret != 'e') {cst.err++; print_err(&cst,__LINE__);}

	_cmp_end(&cst);

	return (cst.err > 0);
}
