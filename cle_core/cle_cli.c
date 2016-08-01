#include "backends/cle_backends.h"
#include "cle_clerk.h"
#include "cle_source.h"
#include "cle_stream.h"
#include <stdio.h>
#include <stdlib.h>

void cle_panic(task* t) {
	fputs("PANIC", stderr);
	exit(-1);
}

#define TRACK_SIZE 2000

struct output_state {
	int tipp, ptip;
	ushort points[100];
	char data[TRACK_SIZE];
};

/*
 * P = pop, M = mark (push), . = sep.
 *
 * ccc.cccP -> {ccc.ccc}
 * ccMc.cccPa.ccc -> {ccc.ccc,cca.ccc}
 * ccMc.cMcP
 *
 */

static void _tracker_put_char(void* p, int chr) {
	struct output_state* out = (struct output_state*) p;

	if (out->tipp < TRACK_SIZE) {
		out->data[out->tipp++] = chr;
	}
}

static void _tracker_field_end(void* p) {
	struct output_state* out = (struct output_state*) p;
	int field_begin = out->ptip;
	int branch = 0;

	while (field_begin > 0 && (out->points[--field_begin] & 0xC000) == 0) {
		branch++;
	}

	if ((out->points[field_begin] & 0x4000) == 0) {
		if (branch || field_begin == 0) {
			out->points[field_begin] |= 0x4000;
			fputc('{', stdout);
		} else {
			fputc('.', stdout);
		}
	}

	field_begin = out->points[field_begin] & 0x3FFF;

	fprintf(stdout, "\"%.*s\"", out->tipp - field_begin,
			out->data + field_begin);

	out->points[out->ptip++] = 0x8000 | out->tipp;
}

static void _tracker_pop(void* p) {
	struct output_state* out = (struct output_state*) p;
	int field_sep = 0;

	// didn't call field-end on last piece?
	if (out->ptip > 0 && out->tipp > (out->points[out->ptip - 1] & 0x3FFF))
		_tracker_field_end(p);

	while (out->ptip > 0 && (out->points[--out->ptip] & 0x8000) != 0) {
		if (out->points[out->ptip] & 0x4000) {
			fputc('}', stdout);
		}

		field_sep |= out->points[out->ptip] & 0xC000;
	}

	if (field_sep == 0x8000) {
		if (out->ptip == 0)
			fprintf(stdout, "{}");
		else
			fputc(';', stdout);
	}

	out->tipp = out->points[out->ptip] & 0x3FFF;
}

static void _tracker_push(void* p) {
	struct output_state* out = (struct output_state*) p;

	out->points[out->ptip++] = out->tipp;
}

static void _tracker_reset(void* p) {
	struct output_state* out = (struct output_state*) p;

	while (out->ptip > 0)
		_tracker_pop(p);

	out->tipp = 0;
	out->ptip = 1;
	out->points[0] = 0x8000;
}

state start(void* p) {
	struct output_state* out = (struct output_state*) p;
	out->tipp = 0;
	out->ptip = 1;
	out->points[0] = 0x8000;
	return OK;
}
state next(void* p) {
	_tracker_reset(p);
	return OK;
}
state pop(void* p) {
	_tracker_pop(p);
	return OK;
}
state push(void* p) {
	_tracker_push(p);
	return OK;
}
state data(void* p, cdat d, uint l) {
	int i;
	for (i = 0; i < l; i++) {
		if (d[i])
			_tracker_put_char(p, d[i]);
		else
			_tracker_field_end(p);
	}
	return OK;
}
state end(void* p, cdat d, uint l) {
	if (l > 0) {
		fprintf(stderr, "ERROR: %.*s\n", l, d);
		return FAILED;
	}

	return END;
}

static const cle_pipe pout = { start, next, end, pop, push, data, 0 };

static void flush_line(FILE* f) {
	while (1) {
		switch (fgetc(f)) {
		case 0:
		case EOF:
		case '\n':
		case '\r':
			return;
		default:
			break;
		}
	}
}

static int read_event(task* t, st_ptr ptr, FILE* f) {
	int ok_is_empty = 0;
	while (1) {
		int c = fgetc(f);
		switch (c) {
		case 0:		// end-of-file / error => ok, if nothing read
		case EOF:
			return ok_is_empty ? -1 : 0;
		case '\n':	// white-space
		case '\r':
			if (ok_is_empty)
				return 1;
			/* no break */
		case '\t':
		case ' ':
			break;
		case '#':	// comment-line
			flush_line(f);
			break;
		case '}':
			return -1;	// error
		case '{':
			return ok_is_empty ? 2 : -1; // fire event -> continue reading
		case ';':
			return ok_is_empty ? 3 : -1; // fire event -> stop reading
		case '.':
		case '/':
			if (!ok_is_empty)
				return -1;
			c = 0;
			/* no break */
		default:
			st_append(t, &ptr, (cdat) &c, 1);
			ok_is_empty = c != 0;
		}
	}
}

static int read_string(FILE* f, cle_stream* strm, char quote) {
	while (1) {
		int c = fgetc(f);

		if (c <= 0)
			return c;

		if (c == quote) {
			c = fgetc(f);

			if (c != quote)
				return c;

			// escaped quote ""
		}

		cle_data(strm, (cdat) &c, 1);
	}
}

static int read_input(FILE* f, cle_stream* strm) {
	int ok_is_empty = 0;
	int level = 0;

	// continue reading even if stream is failed - to clear input structure
	int c = '{';

	while (1) {
		switch (c) {
		case 0:
		case EOF:
			return level >= 0;
		case '#':	// comment-line
			flush_line(f);
			break;
		case '"':
		case '\'':
			c = read_string(f, strm, c);
			continue;
		case '{':
			if (ok_is_empty) {
				c = 0;
				cle_data(strm, (cdat) &c, 1);
			}
			ok_is_empty = 0;
			cle_push(strm);
			level++;
			break;
		case ';':
		case '}':
			if (ok_is_empty) {
				int i = 0;
				cle_data(strm, (cdat) &i, 1);
			}
			cle_pop(strm);
			ok_is_empty = 0;
			if (c == '}' && --level <= 0) {
				if(cle_next(strm) == FAILED)
					return -1;
				else
					break;
			}
			cle_push(strm);
			break;
		case '/':	// slash-dot
		case '.':
			if(!ok_is_empty) {
				// signal input-error
				return -1;
			}
			/* no break */
		case '\n':	// white-space
		case '\r':
			if(level <= 0) {
				return 0;
			}
			/* no break */
		case '\t':
		case ' ':
			if(!ok_is_empty) { // don't repeat
				break;
			}
			c = 0;
			/* no break */
		default:
			cle_data(strm, (cdat) &c, 1);
			ok_is_empty = (c != 0);
		}

		c = fgetc(f);
	}
}

static cle_pipe_inst eval_handler;

static void read_cmds(task* parent, FILE* f, st_ptr config, st_ptr user_roles) {
	while (1) {
		int reader_state;
		task* t = tk_clone_task(parent);
		st_ptr event;
		st_empty(t, &event);

		printf("\n>> ");
		fflush(stdout);

		reader_state = read_event(t, event, f);

		if (reader_state > 0) {
			struct output_state out_state;
			cle_pipe_inst out = { &pout, &out_state };

			cle_stream* strm = cle_open(t, config, event, user_roles, out,
					eval_handler);
			if (strm) {
				if(reader_state == 2)
					read_input(f, strm);

				cle_close(strm, 0, 0);
			} else {
				fprintf(stderr, "open failed\n");
			}
		} else {
			fprintf(stderr, "read failed\n");
		}

		// clear input
		if (reader_state != 1) {
			flush_line(f);
		}

		tk_drop_task(t);
	}
}

static state echo_stream_start(void* p) {
	struct handler_env env;
	cle_handler_get_env(p, &env);
	st_ptr pt;
	st_empty(env.inst.t, &pt);
	cle_handler_set_data(p, st_merge_stream(env.inst.t, &pt));
	return OK;
}

static state echo_stream_end(void* p, cdat d, uint l) {
	struct st_stream* strm = (struct st_stream*) cle_handler_get_data(p);
	st_ptr pt;
	if(st_top_stream(strm, &pt) == 0) {
		resp_ptr_next(p, pt);
	}
	st_destroy_stream(strm);
	return l == 0 ? END : FAILED;
}

static state merge_stream_start(void* p) {
	struct handler_env env;
	cle_handler_get_env(p, &env);
	st_ptr pt = env.inst.root;
	if (st_move_st(env.inst.t, &pt, &env.event_rest))
		return FAILED;

	cle_handler_set_data(p, st_merge_stream(env.inst.t, &pt));
	return OK;
}

static state delete_stream_start(void* p) {
	struct handler_env env;
	cle_handler_get_env(p, &env);
	st_ptr pt = env.inst.root;
	if (st_move_st(env.inst.t, &pt, &env.event_rest))
		return FAILED;

	cle_handler_set_data(p, st_delete_stream(env.inst.t, &pt));
	return OK;
}

static state replace_stream_start(void* p) {
	struct handler_env env;
	cle_handler_get_env(p, &env);
	st_ptr pt = env.inst.root;
	if (st_move_st(env.inst.t, &pt, &env.event_rest))
		return FAILED;

	if (st_clear(env.inst.t, &pt))
		return FAILED;

	cle_handler_set_data(p, st_merge_stream(env.inst.t, &pt));
	return OK;
}

static state exist_stream_start(void* p) {
	struct handler_env env;
	cle_handler_get_env(p, &env);
	st_ptr pt = env.inst.root;
	if (st_move_st(env.inst.t, &pt, &env.event_rest))
		return FAILED;

	cle_handler_set_data(p, st_exist_stream(env.inst.t, &pt));
	return OK;
}

static state read_stream_start(void* p) {
	struct handler_env env;
	cle_handler_get_env(p, &env);

	st_ptr root = env.inst.root;

	if (st_move_st(env.inst.t, &root, &env.event_rest) == 0) {
		resp_ptr_next(p, root);
	}

	return OK;
}

static state quit_stream_start(void* p) {
	exit(0);
	return OK;
}

static state stream_next(void* p) {
	struct st_stream* strm = (struct st_stream*) cle_handler_get_data(p);
	// pop any remaining levels
	while (st_stream_pop(strm) == 0)
		;
	return OK;
}

static state stream_end(void* p, cdat d, uint l) {
	struct st_stream* strm = (struct st_stream*) cle_handler_get_data(p);
	st_destroy_stream(strm);
	return l == 0 ? END : FAILED;
}

static state stream_pop(void* p) {
	struct st_stream* strm = (struct st_stream*) cle_handler_get_data(p);
	return st_stream_pop(strm) ? FAILED : OK;
}

static state stream_push(void* p) {
	struct st_stream* strm = (struct st_stream*) cle_handler_get_data(p);
	return st_stream_push(strm) ? FAILED : OK;
}

static state stream_data(void* p, cdat c, uint l) {
	struct st_stream* strm = (struct st_stream*) cle_handler_get_data(p);
	return st_stream_data(strm, c, l, 0) ? FAILED : OK;
}

static const cle_pipe echo_handler = { echo_stream_start, stream_next,
		echo_stream_end, stream_pop, stream_push, stream_data, 0 };
static const cle_pipe merge_handler = { merge_stream_start, stream_next,
		stream_end, stream_pop, stream_push, stream_data, 0 };
static const cle_pipe delete_handler = { delete_stream_start, stream_next,
		stream_end, stream_pop, stream_push, stream_data, 0 };
static const cle_pipe replace_handler = { replace_stream_start, stream_next,
		stream_end, stream_pop, stream_push, stream_data, 0 };
static const cle_pipe check_handler = { exist_stream_start, stream_next,
		stream_end, stream_pop, stream_push, stream_data, 0 };

static state eval_start(void* p) {
	return OK;
}

static state eval_next(void* p, st_ptr pt) {
	struct handler_env env;
	cle_handler_get_env(p, &env);

	resp_ptr(p, env.handler_root);
	resp_data(p, (cdat) ":", 2);
	resp_ptr(p, pt);
	return resp_next(p);
}

static state eval_end(void* p, cdat d, uint l) {
	return l == 0 ? END : FAILED;
}

static state comp_next(void* p, st_ptr pt) {
	struct handler_env env;
	cle_handler_get_env(p, &env);

	test_compile(env.inst.t, pt);

	return OK;
}

int main(int argc, char* argv[]) {
	FILE* input = stdin;
	cle_pagesource* psource = &util_memory_pager;
	cle_psrc_data pdata = util_create_mempager();

	printf("%c%sClerk CLI%c%s\n", 0x1B, "[1;31m", 0x1B, "[0m");

	task* parent = tk_create_task(psource, pdata);

	st_ptr config, pt;

	st_empty(parent, &config);

	pt = config;
	st_insert(parent, &pt, (cdat) "sys\0merge", 10);
	cle_config_handler(parent, pt, &merge_handler, SYNC_REQUEST_HANDLER);

	pt = config;
	st_insert(parent, &pt, (cdat) "sys\0delete", 11);
	cle_config_handler(parent, pt, &delete_handler, SYNC_REQUEST_HANDLER);

	pt = config;
	st_insert(parent, &pt, (cdat) "sys\0replace", 12);
	cle_config_handler(parent, pt, &replace_handler, SYNC_REQUEST_HANDLER);

	pt = config;
	st_insert(parent, &pt, (cdat) "sys\0check", 10);
	cle_config_handler(parent, pt, &check_handler, SYNC_REQUEST_HANDLER);

	pt = config;
	st_insert(parent, &pt, (cdat) "echo", 5);
	cle_config_handler(parent, pt, &echo_handler, SYNC_REQUEST_HANDLER);

	cle_pipe read_handler = cle_basic_trigger_start(read_stream_start);
	pt = config;
	st_insert(parent, &pt, (cdat) "sys\0read", 9);
	cle_config_handler(parent, pt, &read_handler, SYNC_REQUEST_HANDLER);

	cle_pipe quit_handler = cle_basic_trigger_start(quit_stream_start);
	pt = config;
	st_insert(parent, &pt, (cdat) "quit", 5);
	cle_config_handler(parent, pt, &quit_handler, SYNC_REQUEST_HANDLER);
	// sys.dump{file=name}
	// sys.read
	// sys.source
	cle_pipe comp_handler = cle_basic_handler(eval_start, comp_next, eval_end);
	pt = config;
	st_insert(parent, &pt, (cdat) "comp", 5);
	cle_config_handler(parent, pt, &comp_handler, SYNC_REQUEST_HANDLER);

	cle_pipe _eval = cle_basic_handler(eval_start, eval_next, eval_end);
	eval_handler.pipe = &_eval;
	eval_handler.data = 0;

	read_cmds(parent, input, config, str(parent, "sa"));

	tk_drop_task(parent);

	return 0;
}
