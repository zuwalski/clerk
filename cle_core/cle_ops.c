#include <cle_clerk.h>

#include <stdio.h>

typedef struct cle_env {
	struct cle_env* prev;
	st_ptr root;
	st_ptr defs;
	uint next_var;
} cle_env;

static const uchar ID_BODY[] = "do";

static const char* BUILD_IN[][2] = {
		{ "+", "add" },
		{ "-", "sub" },
		{ "*", "mul" },
		{ "/", "div" },
		{ "%", "rem" }
};

uint cle_eval_lib(task* t, cle_env* parent, st_ptr lib, cdat fn, uint length);

typedef uint (*_fn_eval)(task*, cle_env*, st_ptr);

/**
 * path.path {*.$bindName}
 */
static uint _def_matcher(task* t, cle_env* env, st_ptr loc) {
	it_ptr it;
	st_ptr nxt;
	uint ret = 0;

	it_create(t, &it, &loc);

	while (!ret && it_next(t, &nxt, &it, 0)) {
		if (it.kused == 2 && it.kdata[0] == '*') {

		} else if (it.kdata[0] == '$') {
			if (it.kused < 3) {
				ret = 3;
				break;
			} else {

			}
		} else {

		}

		ret = _def_matcher(t, env, nxt);
	}

	it_dispose(t, &it);

	return ret;
}

static _fn_eval _fn_in_env(task* t, cle_env* env, cdat name, uint len) {

	return 0;
}

static uint _eval(task* t, cle_env* env, st_ptr body) {
	st_ptr nxt;
	it_ptr it;
	uint ret = 0;

	it_create(t, &it, &body);

	while (it_next(t, &nxt, &it, 0)) {
		_fn_eval eval = _fn_in_env(t, env, it.kdata, it.kused);
		if (eval == 0) {
			ret = 2;
			break;
		}

		if ((ret = eval(t, env, nxt)))
			break;
	}

	it_dispose(t, &it);

	return ret;
}

uint cle_eval_lib(task* t, cle_env* parent, st_ptr lib, cdat fn, uint length) {
	cle_env env = { .prev = parent, .root = lib };
	uint ret = 0;
	uchar match_id[3];

	// lookup fn in lib
	if (st_move(t, &lib, fn, length)) {
		return 1;
	}

	st_empty(t, &env.defs);
	env.next_var = 0;

	// setup matchers
	match_id[0] = '#';
	match_id[2] = 0;
	for (match_id[1] = 1; match_id[1] <= 0xFF; match_id[1]++) {
		st_ptr m = lib;

		if (st_move(t, &m, match_id, sizeof(match_id)))
			break;

		if ((ret = _def_matcher(t, &env, m)))
			return ret;
	}

	// lookup body
	if (st_move(t, &lib, ID_BODY, sizeof(ID_BODY))) {
		return 1;
	}

	// and eval
	return _eval(t, &env, lib);
}
