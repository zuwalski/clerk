#include <stdlib.h>
#include <stdio.h>
#include "cle_clerk.h"
#include "cle_stream.h"
#include "backends/cle_backends.h"

void cle_panic(task* t) {
	exit(-1);
}

state start(void* p) {
	return OK;
}
state next(void* p) {
	puts("\n");
	return OK;
}
state end(void* p, cdat d, uint l) {
	if (l > 0) {
		printf("error: %*s\n", l, d);
	}
	return OK;
}
state pop(void* p) {
	printf(" } ");
	return OK;
}
state push(void* p) {
	printf(" { ");
	return OK;
}
state data(void* p, cdat d, uint l) {
	if (l > 0) {
		printf("%*s\n", l, d);
	}
	return OK;
}

static const cle_pipe pout = { start, next, end, pop, push, data, 0 };
static const cle_pipe_inst out = { &pout, 0 };

state test_create(void* p) {
	puts("in test_create");
	return OK;
}
static cle_pipe pipe_to_create;

static st_ptr cmd_line_event(task* t, int argc, char* argv[]) {
	st_ptr tmp, rt;
	st_empty(t, &tmp);
	rt = tmp;
	if (argc > 1) {
		st_insert(t, &tmp, argv[1], strlen(argv[1]));
	}
	return rt;
}

int main(int argc, char* argv[]) {
	cle_stream* strm;
	cle_pagesource* psource = &util_memory_pager;
	cle_psrc_data pdata = util_create_mempager();

	task* t = tk_create_task(psource, pdata);

	st_ptr config, userid, user_roles, eventid;

	st_empty(t, &config);
	st_empty(t, &user_roles);
	st_empty(t, &userid);

	eventid = cmd_line_event(t, argc, argv);

	pipe_to_create = cle_basic_trigger_start(test_create);
	cle_config_handler(t, config, &pipe_to_create, SYNC_REQUEST_HANDLER);

	strm = cle_open(t, config, eventid, userid, user_roles, out);
	if (strm) {
		/* code */
		puts("running");

		cle_close(strm, 0, 0);
	}

	tk_drop_task(t);

	puts("clean exit .. bye");
}
