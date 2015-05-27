#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "cle_clerk.h"
#include "cle_stream.h"
#include "backends/cle_backends.h"

void cle_panic(task* t) {
	fputs("PANIC", stderr);
	exit(-1);
}

state start(void* p) {
	return OK;
}
state next(void* p) {
	fputc('\n', stdout);
	return OK;
}
state pop(void* p) {
	fputc('}', stdout);
	return OK;
}
state push(void* p) {
	fputc('{', stdout);
	return OK;
}
state data(void* p, cdat d, uint l) {
	int i;
	for (i = 0; i < l && d[i] != 0; i++) {
		fputc(d[i], stdout);
	}
	return OK;
}
state end(void* p, cdat d, uint l) {
	data(p, d, l);
	return l == 0 ? DONE : FAILED;
}

static const cle_pipe pout = { start, next, end, pop, push, data, 0 };
static const cle_pipe_inst out = { &pout, 0 };

state test_start(void* p) {
	struct handler_env env;

	cle_handler_get_env(p, &env);

	resp_data(p, (cdat) "start: ", 7);
	resp_serialize(p, env.event);
	resp_data(p, (cdat) "\n", 1);

	return OK;
}

state test_next(void* p, st_ptr ptr) {
	resp_data(p, (cdat) "next: ", 6);
	resp_serialize(p, ptr);
	resp_data(p, (cdat) "\n", 1);
	return OK;
}

state test_end(void* p, cdat msg, uint len) {
	resp_data(p, (cdat) "end: ", 5);
	resp_data(p, msg, len);
	resp_data(p, (cdat) "\n", 1);
	return DONE;
}

static cle_pipe pipe_to_create;

static int read_event(task* t, st_ptr ptr, FILE* f) {
	int ok_is_empty = 0;
	int in_comment = 0;
	while (1) {
		int c = fgetc(f);
		switch (c) {
		case 0:		// end-of-file / error => ok, if nothing read
		case EOF:
			return ok_is_empty ? -1 : 0;
		case '\n':	// white-space
		case '\t':
			in_comment = 0;
		case ' ':
			break;
		case '#':	// comment-line
			in_comment = 1;
			break;
		case '}':
			if (in_comment)
				break;
			return -1;	// error
		case '{':
			if (in_comment)
				break;
			return ok_is_empty ? 2 : -1; // fire event -> continue reading
		case ';':
			if (in_comment)
				break;
			return ok_is_empty ? 1 : -1; // fire event -> stop reading
		case '.':
			if (in_comment)
				break;
			c = 0;
		default:
			if (!in_comment) {
				st_append(t, &ptr, (cdat) &c, 1);
				ok_is_empty = 1;
			}
		}
	}
}

static void read_input(FILE* f, cle_stream* strm) {
	int in_comment = 0;
	int level = 0;

	if (cle_push(strm) != OK)
		return;

	while (1) {
		int c = fgetc(f);
		switch (c) {
		case 0:
		case EOF:
			return;
		case '\n':	// new-line
		case '\t':
			in_comment = 0;
			break;
		case '#':	// comment-line
			in_comment = 1;
			break;
		case '{':
			if (!in_comment) {
				if (cle_push(strm) != OK)
					return;

				level++;
			}
			break;
		case '}':
			if (!in_comment) {
				if (cle_pop(strm) != OK)
					return;

				if (level-- <= 0)
					return;
			}
			break;
		case ';':
			if (!in_comment) {
				if (cle_pop(strm) != OK)
					return;
				if (cle_push(strm) != OK)
					return;
			}
			break;
		default:
			if (!in_comment && cle_data(strm, (cdat) &c, 1) != OK) {
				return;
			}
		}
	}
}

static void read_cmds(task* parent, FILE* f, st_ptr config, st_ptr userid,
		st_ptr user_roles) {
	int reader_state = 1;
	while (reader_state > 0) {
		task* t = tk_clone_task(parent);
		st_ptr event;
		st_empty(t, &event);

		reader_state = read_event(t, event, f);

		if (reader_state > 0) {
			cle_stream* strm = cle_open(t, config, event, userid, user_roles,
					out);
			if (strm) {
				if (reader_state == 2)
					read_input(f, strm);

				cle_close(strm, 0, 0);
			}
		}

		tk_drop_task(t);
	}
}

int main(int argc, char* argv[]) {
	FILE* input = stdin;
	cle_pagesource* psource = &util_memory_pager;
	cle_psrc_data pdata = util_create_mempager();

	task* parent = tk_create_task(psource, pdata);

	st_ptr config, userid, user_roles, eventid;

	st_empty(parent, &config);
	st_empty(parent, &user_roles);
	st_empty(parent, &userid);

	pipe_to_create = cle_basic_handler(test_start, test_next, test_end);

	cle_config_handler(parent, config, &pipe_to_create, SYNC_REQUEST_HANDLER);

	read_cmds(parent, input, config, userid, user_roles);

	tk_drop_task(parent);

	return 0;
}
