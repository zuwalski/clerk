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

#include "test.h"
#include "cle_stream.h"
#include "cle_object.h"
#include <stdio.h>
#include <time.h>

//////////////////////////////////

static uint _start(void* p) {
	//puts("_start");
	return 0;
}
static uint _next(void* p) {
	//puts("_next");
	return 0;
}
static uint _end(void* p, cdat msg, uint len) {
	//printf("_end: %*s\n", len, msg);
	return 1;
}
static uint _pop(void* p) {
	//puts("_pop");
	return 0;
}
static uint _push(void* p) {
	//puts("_push");
	return 0;
}
static uint _data(void* p, cdat data, uint len) {
	//printf("_data: %*s\n", len, data);
	return 0;
}

static const cle_pipe print_pipe = { _start, _next, _end, _pop, _push, _data, 0 };
static cle_pipe_inst pipe_inst = { &print_pipe, 0 };

static uint pt_start(void* p) {
	//puts("pt_start");
	return 0;
}
static uint pt_next(void* p) {
	//puts("pt_next");
	resp_next(p);
	return 0;
}
static uint pt_end(void* p, cdat msg, uint len) {
	//printf("pt_end: %*s\n", len, msg);
	return 1;
}
static uint pt_pop(void* p) {
	//puts("pt_pop");
	resp_pop(p);
	return 0;
}
static uint pt_push(void* p) {
	//puts("pt_push");
	resp_push(p);
	return 0;
}
static uint pt_data(void* p, cdat data, uint len) {
	//printf("pt_data: %*s\n", len, data);
	resp_data(p, data, len);
	return 0;
}

static const cle_pipe pt_pipe = { pt_start, pt_next, pt_end, pt_pop, pt_push, pt_data, 0 };

static uint bh_start(void* p) {
	//puts("bh_start");
	return 0;
}

static uint bh_next(void* p, st_ptr pt) {
	//puts("bh_next");
	resp_serialize(p, pt);
	resp_next(p);
	return 0;
}

static uint bh_end(void* p, cdat m, uint l) {
	//printf("bh_end: %*s\n", l, m);
	return 0;
}

static cle_pipe bh_pipe;

static const st_ptr empty = { 0, 0, 0 };

void test_stream_c2() {
	cle_psrc_data store = util_create_mempager();
	task* t = tk_create_task(&util_memory_pager, store);
	cle_instance inst = { t, root(t) };

	st_ptr eventid = str(t, "event");
	st_ptr config = str(t, "");
	st_ptr userid = str(t, "");
	st_ptr user_roles = str(t, "");
	int i;
	clock_t start,stop;

	cle_stream* ipt = cle_open(t, config, eventid, userid, user_roles, pipe_inst);

	ASSERT(ipt == 0);

	cle_new(inst, eventid, empty, 0);

	cmt_commit_task(t);

	t = tk_create_task(&util_memory_pager, store);

	puts("test 1:");
	ipt = cle_open(t, config, eventid, userid, user_roles, pipe_inst);

	ASSERT(ipt);

	cle_data(ipt, (cdat) "msg", 4);

	cle_close(ipt, 0, 0);

	cle_config_handler(t, config, &pt_pipe, PIPELINE_REQUEST);

	st_ptr tmp = config;
	st_insert(t, &tmp, (cdat) "event", 6);

	// SYNC_REQUEST_HANDLER = 0, PIPELINE_REQUEST, PIPELINE_RESPONSE
	cle_config_handler(t, tmp, &pt_pipe, PIPELINE_RESPONSE);

	puts("test 2:");
	ipt = cle_open(t, config, eventid, userid, user_roles, pipe_inst);

	ASSERT(ipt);

	cle_data(ipt, (cdat) "msg", 4);

	cle_close(ipt, 0, 0);

	bh_pipe = cle_basic_handler(bh_start, bh_next, bh_end);

	cle_config_handler(t, config, &bh_pipe, SYNC_REQUEST_HANDLER);

	puts("test 3:");

	start = clock();
	for (i = 0; i < 700000; i++) {
		ipt = cle_open(t, config, eventid, userid, user_roles, pipe_inst);

		cle_data(ipt, (cdat) "a", 2);
		cle_push(ipt);
		cle_data(ipt, (cdat) "1", 2);
		cle_push(ipt);
		cle_data(ipt, (cdat) "x", 2);

		cle_pop(ipt);
		cle_data(ipt, (cdat) "2", 2);
		cle_pop(ipt);
		cle_data(ipt, (cdat) "b", 2);

		cle_next(ipt);
		cle_close(ipt, 0, 0);
	}
	stop = clock();
	printf("time: %d\n", (stop - start));

	tk_drop_task(t);
}

