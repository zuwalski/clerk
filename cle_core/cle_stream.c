/* 
 Clerk application and storage engine.
 Copyright (C) 2013  Lars Szuwalski

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
#include "cle_stream.h"

#include <string.h>
/*
 *	The main input-interface to the running system
 *	Events/messages are "pumped" in through the exported set of functions
 *
 *	TODO env passed to open.
 *	remove user-id - check for sa-role in user-roles
 */

struct _child_task {
	struct _child_task* next;
	cle_stream* task;
};

struct task_common {
	struct task_common* parent;
	struct _child_task* childs;
	cle_pipe_inst response;
	cle_instance inst;
	st_ptr event_name;
	st_ptr user_roles;
	st_ptr userid;
	st_ptr config;
	ptr_list* free;
	ptr_list* out;
	cle_stream* ipt;
	st_ptr top;
};

struct handler_node {
	struct handler_node* next;
	struct task_common* cmn;
	cle_pipe_inst handler;
	st_ptr event_rest;
	st_ptr handler_root;
	uint flags;
};

struct _syshandler {
	struct _syshandler* next_handler;
	const cle_pipe* handler;
	enum handler_type systype;
};

struct _scanner_ctx {
	cle_instance inst;
	struct handler_node* hdltypes[PIPELINE_RESPONSE + 1];
	const cle_pipe *obj_handler;
	st_ptr event_name_base;
	st_ptr event_name;
	st_ptr user_roles;
	st_ptr userid;
	st_ptr evt;
	st_ptr sys;
	uint allowed;
};

// fixed ids

static const uchar ID_ROLES[] = ":roles";
static const uchar ID_BODY[] = ":do";
static const uchar ID_REQ[] = ":req";
static const uchar ID_RESP[] = ":resp";

// forwards
static state _bh_next(void* v);

// ok node begin

static state _ok_start(void* v) {
	return OK;
}
static state _ok_next(void* v) {
	return OK;
}
static state _ok_end(void* v, cdat c, uint l) {
	return DONE;
}
static state _ok_pop(void* v) {
	return OK;
}
static state _ok_push(void* v) {
	return OK;
}
static state _ok_data(void* v, cdat c, uint l) {
	return OK;
}

static const cle_pipe _ok_node = { _ok_start, _ok_next, _ok_end, _ok_pop,
		_ok_push, _ok_data, 0 };

// copy node begin
static const cle_pipe _copy_node = { _ok_start, resp_next, _ok_end, resp_pop,
		resp_push, resp_data, 0 };

// response node begin
static state _rn_start(void* v) {
	struct handler_node* h = (struct handler_node*) v;
	return h->cmn->response.pipe->start(h->cmn->response.data);
}

static state _rn_next(void* v) {
	struct handler_node* h = (struct handler_node*) v;
	return h->cmn->response.pipe->next(h->cmn->response.data);
}

static state _rn_pop(void* v) {
	struct handler_node* h = (struct handler_node*) v;
	return h->cmn->response.pipe->pop(h->cmn->response.data);
}

static state _rn_push(void* v) {
	struct handler_node* h = (struct handler_node*) v;
	return h->cmn->response.pipe->push(h->cmn->response.data);
}

static state _rn_data(void* v, cdat c, uint l) {
	struct handler_node* h = (struct handler_node*) v;
	return h->cmn->response.pipe->data(h->cmn->response.data, c, l);
}

static state _rn_end(void* v, cdat c, uint l) {
	struct handler_node* h = (struct handler_node*) v;
	return h->cmn->response.pipe->end(h->cmn->response.data, c, l);
}

static const cle_pipe _response_node = { _rn_start, _rn_next, _rn_end, _rn_pop,
		_rn_push, _rn_data, 0 };

static struct handler_node* _hnode(struct _scanner_ctx* ctx,
		const cle_pipe* handler, st_ptr handler_root, enum handler_type type) {
	struct handler_node* hdl = 0;

	if (type == SYNC_REQUEST_HANDLER) {
		hdl = ctx->hdltypes[SYNC_REQUEST_HANDLER];
	}

	if (hdl == 0)
		hdl = (struct handler_node*) tk_alloc(ctx->inst.t,
				sizeof(struct handler_node), 0);

	hdl->next = ctx->hdltypes[type];
	ctx->hdltypes[type] = hdl;

	hdl->handler.pipe = handler;
	hdl->event_rest = ctx->event_name;
	hdl->handler_root = handler_root;
	return hdl;
}

static void _ready_node(struct handler_node* n, struct task_common* cmn) {
	n->handler.data = 0;
	n->flags = 0;
	n->cmn = cmn;
}

static void _reg_handlers(struct _scanner_ctx* ctx, st_ptr pt) {
	st_ptr tpt = pt;
	if (st_move(ctx->inst.t, &tpt, ID_BODY, sizeof(ID_BODY)) == 0) {
		_hnode(ctx, ctx->obj_handler, tpt, SYNC_REQUEST_HANDLER);
	}

	tpt = pt;
	if (st_move(ctx->inst.t, &tpt, ID_REQ, sizeof(ID_REQ)) == 0) {
		_hnode(ctx, ctx->obj_handler, tpt, PIPELINE_REQUEST);
	}

	tpt = pt;
	if (st_move(ctx->inst.t, &tpt, ID_RESP, sizeof(ID_RESP)) == 0) {
		_hnode(ctx, ctx->obj_handler, tpt, PIPELINE_RESPONSE);
	}
}

static void _reg_syshandlers(struct _scanner_ctx* ctx, st_ptr pt) {
	struct _syshandler* syshdl;

	if (pt.pg == 0 || st_move(0, &pt, HEAD_HANDLER, HEAD_SIZE))
		return;

	if (st_get(0, &pt, (char*) &syshdl, sizeof(struct _syshandler*)) != -1)
		cle_panic(ctx->inst.t);

	do {
		_hnode(ctx, syshdl->handler, ctx->evt, syshdl->systype);
		// next in list...
	} while ((syshdl = syshdl->next_handler));
}

static uint _check_access(task* t, st_ptr allow, st_ptr roles) {
	it_ptr aitr, ritr;
	uint ret;

	if (st_move(t, &allow, ID_ROLES, sizeof(ID_ROLES)))
		return 0;

	it_create(t, &aitr, &allow);
	it_create(t, &ritr, &roles);

	while (1) {
		ret = it_next_eq(t, 0, &aitr, 0);
		if (ret != 1)
			break;

		it_load(t, &ritr, aitr.kdata, aitr.kused);

		ret = it_next_eq(t, 0, &ritr, 0);
		if (ret != 1)
			break;

		it_load(t, &aitr, ritr.kdata, ritr.kused);
	}

	it_dispose(t, &aitr);
	it_dispose(t, &ritr);

	return (ret == 2);
}

static void _check_boundry(struct _scanner_ctx* ctx) {
	_reg_syshandlers(ctx, ctx->sys);

	if (ctx->evt.pg != 0) {
		_reg_handlers(ctx, ctx->evt);

		if (ctx->allowed == 0)
			ctx->allowed = _check_access(ctx->inst.t, ctx->evt,
					ctx->user_roles);
	}
}

/**
 * static ref path.path
 */
static int _scanner(void* p, uchar* buffer, uint len) {
	struct _scanner_ctx* ctx = (struct _scanner_ctx*) p;

	st_insert(ctx->inst.t, &ctx->event_name, buffer, len);

	if (ctx->evt.pg != 0 && st_move(ctx->inst.t, &ctx->evt, buffer, len)) {
		ctx->evt.pg = 0;

		// not found! scan end (or no possible grants)
		if (ctx->allowed == 0)
			return 1;
	}

	if (ctx->sys.pg != 0 && st_move(ctx->inst.t, &ctx->sys, buffer, len))
		ctx->sys.pg = 0;

	if (buffer[len - 1] == 0)
		_check_boundry(ctx);

	return 0;
}

static void _init_scanner(struct _scanner_ctx* ctx, task* parent, st_ptr config,
		st_ptr user_roles, st_ptr userid) {
	ctx->inst.t = tk_clone_task(parent);
	tk_root_ptr(ctx->inst.t, &ctx->inst.root);

	ctx->evt = ctx->inst.root;
//	if (st_move(ctx->inst.t, &ctx->evt, HEAD_NAMES, IHEAD_SIZE))
//		ctx->evt.pg = 0;

	ctx->sys = config;

	ctx->userid = userid;
	ctx->user_roles = user_roles;

	st_empty(ctx->inst.t, &ctx->event_name);
	ctx->event_name_base = ctx->event_name;

	memset(ctx->hdltypes, 0, sizeof(ctx->hdltypes));

	ctx->allowed = st_is_empty(ctx->inst.t, &userid);

	// TODO read from config
	ctx->obj_handler = &_copy_node;
}

static void _add_child(struct task_common* cmn, cle_stream* ipt) {
	struct _child_task* ct = (struct _child_task*) tk_alloc(cmn->inst.t,
			sizeof(struct _child_task), 0);
	ct->next = cmn->childs;
	cmn->childs = ct;
	ct->task = ipt;
}

static struct task_common* _create_task_common(struct _scanner_ctx* ctx,
		cle_pipe_inst response, st_ptr config) {
	struct task_common* cmn = (struct task_common*) tk_alloc(ctx->inst.t,
			sizeof(struct task_common), 0);
	cmn->response = response;
	cmn->inst = ctx->inst;

	cmn->config = config;
	cmn->event_name = ctx->event_name_base;
	cmn->user_roles = ctx->user_roles;
	cmn->userid = ctx->userid;

	st_empty(cmn->inst.t, &cmn->top);
	cmn->free = 0;
	cmn->out = (ptr_list*) tk_alloc(ctx->inst.t, sizeof(ptr_list), 0);
	cmn->out->link = 0;
	cmn->out->pt = cmn->top;

	cmn->parent = 0;
	cmn->childs = 0;
	_add_child(cmn, 0);
	_add_child(cmn, 0);
	_add_child(cmn, 0);
	_add_child(cmn, 0);
	return cmn;
}

static cle_stream* _setup_handlers(struct _scanner_ctx* ctx,
		cle_pipe_inst response, st_ptr config) {
	struct handler_node* hdl;
	struct task_common* cmn;
	cle_stream* ipt = ctx->hdltypes[SYNC_REQUEST_HANDLER];

	if (ctx->allowed == 0 || ipt == 0)
		return 0;

	cmn = _create_task_common(ctx, response, config);

	ipt->next = ctx->hdltypes[PIPELINE_RESPONSE];
	hdl = ipt;

	// setup response-handler chain
	// in correct order (most specific handler comes first)
	do {
		_ready_node(hdl, cmn);

		hdl = hdl->next;
	} while (hdl != 0);

	hdl = ctx->hdltypes[PIPELINE_REQUEST];
	// setup request-handler chain
	// reverse order (most general handlers comes first)
	while (hdl != 0) {
		struct handler_node* tmp;

		_ready_node(hdl, cmn);

		tmp = hdl->next;
		hdl->next = ipt;
		ipt = hdl;
		hdl = tmp;
	}

	cmn->ipt = ipt;
	return ipt;
}

// input interface
cle_stream* cle_open(task* parent, st_ptr config, st_ptr eventid, st_ptr userid,
		st_ptr user_roles, cle_pipe_inst response) {
	struct _scanner_ctx ctx;
	cle_stream* ipt = 0;

	_init_scanner(&ctx, parent, config, user_roles, userid);

	// before anything push response-node as last response-handler
	_hnode(&ctx, &_response_node, ctx.evt, PIPELINE_RESPONSE);

	_check_boundry(&ctx);

	if (cle_scan_validate(parent, &eventid, _scanner, &ctx) == 0)
		ipt = _setup_handlers(&ctx, response, config);

	if (ipt == 0)
		tk_drop_task(ctx.inst.t);

	return ipt;
}

/**
 * Must be called from the current thread of parent
 */
cle_stream* cle_open_child(void* parent, st_ptr eventid, cle_pipe_inst resp) {
	struct handler_node* prnt = (struct handler_node*) parent;
	struct task_common* pcmn = prnt->cmn;
	struct _child_task* ct;
	cle_stream* ipt = cle_open(pcmn->inst.t, pcmn->config, eventid,
			pcmn->userid, pcmn->user_roles, resp);
	if (ipt == 0)
		return 0;

	// attach child to parent
	ipt->cmn->parent = pcmn;
	ct = pcmn->childs;
	while (ct) {
		if (ct->task == 0) {
			ct->task = ipt;
			break;
		}
		ct = ct->next;
	}
	if (ct == 0)
		_add_child(pcmn, ipt);

	return ipt;
}

static state _need_start_call(struct handler_node* h) {
	state s = OK;
	if (h->flags & 1)
		return FAILED;

	if (h->flags == 0) {
		h->flags |= 2;
		s = h->handler.pipe->start(h);
	}
	return s;
}

static state _check_state(struct handler_node* h, state s, cdat msg,
		uint length) {
	struct _child_task* c;
	if (s == OK)
		return OK;

	if (s == DONE) {
		do {
			s = _need_start_call(h);

			// if its a basic handler -> send any last input
			if (s == OK && h->handler.pipe->next_ptr != 0
					&& (!st_is_empty(h->cmn->inst.t, &h->cmn->out->pt))) {
				if (_bh_next(h) > DONE) {
					s = FAILED;
					break;
				}
			}

			if (s != FAILED)
				s = h->handler.pipe->end(h, 0, 0);

			h->handler.pipe = &_ok_node;
			if (s > DONE)
				break;
			h = h->next;
		} while (h);
	} else if (s == LEAVE) {
		// must have started already
		s = h->handler.pipe->end(h, 0, 0);
		h->handler.pipe = &_copy_node;
	}

	if (s == DONE || s == OK)
		return DONE;

	// Failed
	h = h->cmn->ipt;
	c = h->cmn->childs;

	if (msg == 0 || length == 0) {
		msg = (cdat) "broken pipe";
		length = 12;
	}

	do {
		if ((h->flags & 1) == 0) {
			h->flags |= 1;
			h->handler.pipe->end(h, msg, length);
			h->handler.pipe = &_ok_node;
		}
		h = h->next;
	} while (h);

	// kill all child-tasks
	while (c) {
		// TODO must call through input-queue
		if (c->task) {
			cle_close(c->task, (cdat) "parent-failed", 14);
			c->task = 0;
		}
		c = c->next;
	}

	return FAILED;
}

static state _check_handler(struct handler_node* h, state (*handler)(void*)) {
	state s = _need_start_call(h);

	if (s == OK)
		s = handler(h);

	return _check_state(h, s, 0, 0);
}

state cle_close(cle_stream* ipt, cdat msg, uint len) {
	state s = (len == 0 && (ipt->flags & 1) == 0) ? DONE : FAILED;
	s = _check_state(ipt, s, msg, len);

	// detach from parent (if any)
	if (ipt->cmn->parent) {
		struct _child_task* c = ipt->cmn->parent->childs;
		while (c) {
			if (c->task == ipt) {
				c->task = 0;
				break;
			}
			c = c->next;
		}
	}

	// commit (trace and stream)
	if (s == DONE) {
		s = cmt_commit_task(ipt->cmn->inst.t) == 0 ? DONE : FAILED;
		//s = cle_commit_objects(ipt->cmn->inst, 0, 0) == 0 ? DONE : FAILED;
	} else {
		// drop local task
		tk_drop_task(ipt->cmn->inst.t);
	}

	return s;
}

state cle_next(cle_stream* ipt) {
	return _check_handler(ipt, ipt->handler.pipe->next);
}

state cle_pop(cle_stream* ipt) {
	return _check_handler(ipt, ipt->handler.pipe->pop);
}

state cle_push(cle_stream* ipt) {
	return _check_handler(ipt, ipt->handler.pipe->push);
}

state cle_data(cle_stream* ipt, cdat data, uint len) {
	state s = _need_start_call(ipt);
	if (s == OK)
		s = ipt->handler.pipe->data(ipt, data, len);

	return _check_state(ipt, s, 0, 0);
}

// response
void cle_handler_get_env(const void* p, struct handler_env* env) {
	struct handler_node* h = (struct handler_node*) p;

	env->handler_root = h->handler_root;
	env->event = h->cmn->event_name;
	env->event_rest = h->event_rest;
	env->roles = h->cmn->user_roles;
	env->data = h->handler.data;
	env->user = h->cmn->userid;
	env->inst = h->cmn->inst;
}

void cle_handler_set_data(void* p, void* data) {
	struct handler_node* h = (struct handler_node*) p;
	h->handler.data = data;
}
void* cle_handler_get_data(const void* p) {
	struct handler_node* h = (struct handler_node*) p;
	return h->handler.data;
}

state resp_data(void* p, cdat c, uint l) {
	struct handler_node* h = (struct handler_node*) p;
	state s = _need_start_call(h->next);
	if (s == OK)
		s = h->next->handler.pipe->data(h->next, c, l);

	return _check_state(h->next, s, 0, 0);
}
state resp_next(void* p) {
	struct handler_node* h = (struct handler_node*) p;
	return _check_handler(h->next, h->next->handler.pipe->next);
}
state resp_push(void* p) {
	struct handler_node* h = (struct handler_node*) p;
	return _check_handler(h->next, h->next->handler.pipe->push);
}
state resp_pop(void* p) {
	struct handler_node* h = (struct handler_node*) p;
	return _check_handler(h->next, h->next->handler.pipe->pop);
}
static state _data_serializer(void* p, cdat c, uint l, uint at) {
	struct handler_node* h = (struct handler_node*) p;
	return h->handler.pipe->data(p, c, l);
}
state resp_serialize(void* v, st_ptr pt) {
	struct handler_node* h = (struct handler_node*) v;
	state s = _need_start_call(h->next);
	if (s == OK)
		s = st_map_st(h->cmn->inst.t, &pt, _data_serializer,
				h->next->handler.pipe->push, h->next->handler.pipe->pop,
				h->next);

	return _check_state(h->next, s, 0, 0);
}

// add handler to config
uint cle_config_handler(task* t, st_ptr config, const cle_pipe* handler,
		enum handler_type type) {
	struct _syshandler *next_hdl = 0, *hdl;

	if (st_insert(t, &config, HEAD_HANDLER, HEAD_SIZE) == 0) {
		st_ptr tmp = config;
		if (st_get(t, &tmp, (char*) &next_hdl, sizeof(struct _syshandler*))
				!= -1)
			return 1;
	}

	hdl = (struct _syshandler*) tk_alloc(t, sizeof(struct _syshandler), 0);

	hdl->systype = type;
	hdl->handler = handler;
	hdl->next_handler = next_hdl;

	return st_update(t, &config, (cdat) &hdl, sizeof(struct _syshandler*));
}

// basic handler implementation

static state _bh_push(void* v) {
	struct handler_node* h = (struct handler_node*) v;
	struct task_common* cmn = h->cmn;
	ptr_list* l = cmn->free;
	if (l)
		cmn->free = l->link;
	else
		l = (ptr_list*) tk_alloc(cmn->inst.t, sizeof(ptr_list), 0);

	l->pt = cmn->out->pt;
	l->link = cmn->out;
	cmn->out = l;
	return OK;
}

static state _bh_pop(void* v) {
	struct handler_node* h = (struct handler_node*) v;
	ptr_list* tmp = h->cmn->out;
	if (tmp->link == 0)
		return FAILED;

	h->cmn->out = tmp->link;

	tmp->link = h->cmn->free;
	h->cmn->free = tmp;
	return OK;
}

static state _bh_data(void* v, cdat c, uint l) {
	struct handler_node* h = (struct handler_node*) v;
	st_insert(h->cmn->inst.t, &h->cmn->out->pt, c, l);
	return OK;
}

static state _bh_next(void* v) {
	struct handler_node* h = (struct handler_node*) v;

	state s = h->handler.pipe->next_ptr(v, h->cmn->top);

	st_empty(h->cmn->inst.t, &h->cmn->top);
	h->cmn->out->pt = h->cmn->top;
	return s;
}

cle_pipe cle_basic_handler(state (*start)(void*),
		state (*next)(void* p, st_ptr ptr),
		state (*end)(void* p, cdat msg, uint len)) {
	const cle_pipe p =
			{ start, _bh_next, end, _bh_pop, _bh_push, _bh_data, next };
	return p;
}

cle_pipe cle_basic_trigger_start(state (*start)(void* p)) {
	const cle_pipe p = { start, resp_next, _ok_end, resp_pop, resp_push,
			resp_data, 0 };
	return p;
}

cle_pipe cle_basic_trigger_end(state (*end)(void* p, cdat msg, uint len)) {
	const cle_pipe p = { _ok_start, resp_next, end, resp_pop, resp_push,
			resp_data, 0 };
	return p;
}
