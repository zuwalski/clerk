#include "cle_clerk.h"
#include "cle_stream.h"

typedef struct cle_env {
	struct cle_env* prev;
	st_ptr root;
	st_ptr vals;
	int flags;
} cle_env;

typedef struct _compile_ctx {
	task* t;
	st_ptr build_in;
	st_ptr funs;
	st_ptr empty;
} _compile_ctx;

static const uchar ID_IN[] = "in";
static const uchar ID_VAL[] = "val";
static const uchar ID_DEF[] = "def";
static const uchar ID_USE[] = "use";
static const uchar ID_LIB[] = ":lib";
static const uchar ID_PRIVATE[] = "private";

union option_fn {
	st_ptr fn;
	struct {
		void* not_error;
		int err_code;
	} err;
};

typedef uint (*_fn_eval)(_compile_ctx*, cle_env*, st_ptr);

/**
 * build-in:
 *
 * fn {$a,$b} = <expr> // lambda
 *
 * reduce{
 * 	[initial = val]
 * 	combine = (#1,#2) <expr>
 * }
 *
 * map {}
 *
 * flatmap {}
 * buffer, window
 */
// forward
static uint _eval(_compile_ctx* ctx, cle_env* env, st_ptr loc);
static union option_fn _def_fun(_compile_ctx* ctx, cle_env* env, st_ptr loc);
static uint _eval_call(_compile_ctx* ctx, cle_env* env, st_ptr loc);

/**
 * <pre>
 * let {
 * 		val name = <expr>,
 * 		def name = <expr>,
 * 		use {path.to.lib, path.to.lib ... },
 *
 * 		in <expr>
 * }
 * </pre>
 */
static uint _eval_let(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	cle_env e2 = { .prev = env, .root = loc, .vals = loc, .flags = (env != 0? env->flags : 0) };

	if (st_move(ctx->t, &loc, ID_IN, sizeof(ID_IN))) {
		return 1;
	}

	if (st_move(ctx->t, &e2.vals, ID_VAL, sizeof(ID_VAL))) {
		e2.vals = ctx->empty;
	}

	return _eval(ctx, &e2, loc);
}

/**
 * str {some.structure, but.no.eval}
 */
static uint _eval_str(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	printf("STR %p, %d, %d\n", loc.pg, loc.key, loc.offset);

	return 0;
}

/**
 * eval expr (producing the lamda-body)
 * equivalent of _def_fun at runtime -> return no-capture closure
 */
static uint _eval_eval(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	_eval(ctx, env, loc);
	printf("DO_EVAL\n");
	return 0;
}

/**
 * open expr (producing the pipe-name)
 */
static uint _eval_open(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	_eval(ctx, env, loc);
	printf("DO_OPEN\n");
	return 0;
}

/**
 * fun $a = expr
 * lamda / anonymous function
 */
static uint _eval_fun(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	union option_fn fun = _def_fun(ctx, env, loc);
	printf("LOAD_FUN %p, %d, %d\n", fun.fn.pg, fun.fn.key, fun.fn.offset);
	return fun.err.not_error != 0;
}

/**
 * obj {str.path = expr, ...}
 *
 */
static uint _eval_obj(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	printf("OBJ\n");
	return _eval_call(ctx, env, loc);
}

/**
 * fn{a = expr, b = expr ...}
 *
 * $c a = expr / $c{x = 1; y = 2}
 */
static uint _eval_call(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	it_ptr it;

	it_create(ctx->t, &it, &loc);

	while (it_next(ctx->t, &loc, &it, 0)) {
		printf("DATA %.*s\n", it.kused, it.kdata);
		if (!st_move(ctx->t, &loc, (cdat) "=", 2)) {
			printf("SET\n");
			_eval(ctx, env, loc);
		} else {
			_eval_call(ctx, env, loc);
		}
		printf("POP\n");
	}

	it_dispose(ctx->t, &it);
	return 0;
}

/**
 * def fn {$a; $b} = let { $x = $a; in $x }
 *
 */
static union option_fn _def_fun(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	// test cache
	union option_fn option;
	option.fn = ctx->funs;

	if (st_insert(ctx->t, &option.fn, (cdat) &loc, sizeof(st_ptr)) == 1) {
		cle_env e2 = { .prev = env, .root = ctx->empty, .vals = loc, .flags = 0 };

		printf("DEF_FUN %p, %d, %d\n", option.fn.pg, option.fn.key, option.fn.offset);

		if (st_move(ctx->t, &e2.vals, (cdat) "$", 1)) {
			e2.vals = ctx->empty;
		}

		if (st_move(ctx->t, &loc, (cdat) "=", 2)) {
			// body-not-found
			option.err.not_error = 0;
			option.err.err_code = 1;
		}
		// eval body
		else if (_eval(ctx, &e2, loc)) {
			option.err.not_error = 0;
			option.err.err_code = 2;
		}

		printf("DEF_END\n");
	}

	return option;
}

static union option_fn _search_imports(_compile_ctx* ctx, cle_env* search, cdat name,
		uint length) {
	union option_fn option = {.err.not_error = 0, .err.err_code = 0};
	st_ptr pt = search->root;
	if (!st_move(ctx->t, &pt, ID_USE, sizeof(ID_USE))) {
		it_ptr libs;
		it_create(ctx->t, &libs, &pt);

		while (it_next(ctx->t, 0, &libs, -1)) {
			tk_root_ptr(ctx->t, &pt);

			if (!st_move(ctx->t, &pt, libs.kdata, libs.kused)
					&& !st_move(ctx->t, &pt, ID_LIB, sizeof(ID_LIB))) {
				cle_env lib = { .prev = 0, .root = pt, .vals =
						ctx->empty, .flags = 0 };
				if (!st_move(ctx->t, &pt, ID_DEF, sizeof(ID_DEF))
						&& !st_move(ctx->t, &pt, name, length)) {
					cle_env lib_priv = lib;
					if (!st_move(ctx->t, &lib_priv.root, ID_PRIVATE,
							sizeof(ID_PRIVATE))) {
						lib.prev = &lib_priv;
						lib_priv.vals = ctx->empty;	// currently no var's in libs
					}
					printf("DEF_L %.*s\n", length, name);
					option = _def_fun(ctx, &lib, pt);
					break;
				}
			} // else: no-such-lib
		}

		it_dispose(ctx->t, &libs);
	}
	return option;
}

static union option_fn _find_fun(_compile_ctx* ctx, cle_env* env, cdat name,
		uint length) {
	union option_fn option = {.err.not_error = 0, .err.err_code = 0};
	cle_env* search;
	// search env-stack
	for (search = env; search && !option.err.not_error; search = search->prev) {
		st_ptr pt = search->root;

		// test def's
		if (st_move(ctx->t, &pt, ID_DEF, sizeof(ID_DEF)) == 0
				&& !st_move(ctx->t, &pt, name, length)) {
			printf("DEF %.*s\n", length, name);
			option = _def_fun(ctx, search, pt);

		} else {
			option = _search_imports(ctx, search, name, length);
		}
	}
	return option;
}

static uint _use_var(_compile_ctx* ctx, cle_env* env, st_ptr loc, cdat name, uint length) {
	uint ret;
	if (length == 5 && memcmp(name + 1, "this", 5) == 0) {
		// ref to "this"
		ret = 0;
	} else {
		cle_env* search;
		// search env-stack
		for (search = env; search; search = search->prev) {
			st_ptr pt = search->vals;

			// test vals
			if (!st_move(ctx->t, &pt, name, length)) {
				ret = 0;
				break;
			}
		}
	}

	if (ret == 0) {
		// read var
		printf("VAR %.*s\n", length, name);

		ret = _eval_call(ctx, env, loc);
	}

	return ret;
}

/**
 * stream => <lvl0> fn <param> {a = <lvl0>, ..}
 *
 * def fn {$a, $b} = let { val x = $a in $x }
 *
 * fn{a = 1, b = 2} | merge
 *
 * str {...} | $a (if $a is a fun it will curry with str-arg)
 */
static uint _eval(_compile_ctx* ctx, cle_env* env, st_ptr loc) {
	st_ptr pipe, seq;
	it_ptr it;
	uint ret = 3; // not found

	// if this is piped to a next stage find out now
	pipe = seq = loc;
	if (st_move(ctx->t, &pipe, (cdat) "|", 2)) {
		pipe.pg = 0;
	} else {
		printf("PIPE\n");
	}

	it_create(ctx->t, &it, &loc);

	while (1) {
		if (it_next(ctx->t, &loc, &it, 0) == 0) {
			ret = 0; // no op found
			break;
		}

		if (it.kdata[0] == '@' || it.kdata[0] == '|' || it.kdata[0] == ',') {
			// annotation pipe or seq - skip
			continue;
		} else if(ret == 0) {
			ret = 5; // more than one cmd
			break;
		}

		if (it.kdata[0] == '$') {
			// skip $
			ret = _use_var(ctx, env, loc, it.kdata + 1, it.kused - 1);
		} else if (it.kdata[0] >= '0' && it.kdata[0] <= '9') {
			// num-literal
			printf("NUM %.*s\n", it.kused, it.kdata);
			ret = 0;
		} else {
			// check build-ins
			st_ptr pt = ctx->build_in;
			if (!st_move(ctx->t, &pt, it.kdata, it.kused)) {
				_fn_eval fn;
				if (st_get(ctx->t, &pt, (char*) &fn, sizeof(fn)) != -1) {
					cle_panic(ctx->t);
				}

				ret = fn(ctx, env, loc);
			} else {
				union option_fn option = _find_fun(ctx, env, it.kdata, it.kused);

				if(option.err.not_error) {
					printf("LOAD_FUN %p, %d, %d\n", option.fn.pg, option.fn.key, option.fn.offset);
					ret = _eval_call(ctx, env, loc);
				}
			}
		}
	}

	it_dispose(ctx->t, &it);

	// if it contains a sequence find out now
	if (!st_move(ctx->t, &seq, (cdat) ",", 2)) {
		printf("NEXT\n");
		_eval(ctx, env, seq);
	}

	if(pipe.pg) {
		printf("PIPE_NEXT\n");
		_eval(ctx, env, pipe);
	}

	return ret;
}

static state eval_start(void* p) {
	struct handler_env env;
	cle_handler_get_env(p, &env);

	_def_fun(env.data, 0, env.handler_root);

	return OK;
}

static state eval_next(void* p, st_ptr pt) {
	struct handler_env env;
	cle_handler_get_env(p, &env);

	return OK;
}

static state eval_end(void* p, cdat d, uint l) {
	return l == 0 ? END : FAILED;
}

static void add_build_in(struct _compile_ctx* ctx, _fn_eval fn, char* id,
		int len) {
	st_ptr pt = ctx->build_in;
	st_insert(ctx->t, &pt, (cdat) id, len);
	st_update(ctx->t, &pt, (cdat) &fn, sizeof(fn));
}

cle_pipe_inst create_eval_handler(task* t) {
	static cle_pipe eval_pipe;
	struct _compile_ctx* ctx;
	cle_pipe_inst inst;

	eval_pipe = cle_basic_handler(eval_start, eval_next, eval_end);

	ctx = tk_alloc(t, sizeof(struct _compile_ctx), 0);

	ctx->t = t;
	st_empty(t, &ctx->build_in);
	st_empty(t, &ctx->funs);
	st_empty(t, &ctx->empty);

	add_build_in(ctx, _eval_let, "let", 4);
	add_build_in(ctx, _eval_str, "str", 4);

	inst.data = ctx;
	inst.pipe = &eval_pipe;
	return inst;
}

uint test_compile(task* t, st_ptr loc) {
	struct _compile_ctx ctx;

	ctx.t = t;
	st_empty(t, &ctx.build_in);
	st_empty(t, &ctx.funs);
	st_empty(t, &ctx.empty);

	add_build_in(&ctx, _eval_let, "let", 4);
	add_build_in(&ctx, _eval_str, "str", 4);
	add_build_in(&ctx, _eval_fun, "fun", 4);
	add_build_in(&ctx, _eval_obj, "obj", 4);
	add_build_in(&ctx, _eval_eval, "eval", 5);
	add_build_in(&ctx, _eval_open, "open", 5);

	printf("EVAL\n");

	return _eval(&ctx, 0, loc);
}
