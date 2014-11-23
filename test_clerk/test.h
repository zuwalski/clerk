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

#ifndef __CLE_TEST_H__
#define __CLE_TEST_H__

#include "cle_struct.h"
#include "../cle_core/backends/cle_backends.h"
#include "../cle_core/cle_stream.h"
#include <assert.h>

#define HIGH_ITERATION_COUNT 1000000

//#define ASSERT(expr) if((expr) == 0) {printf("assert failed line %d in %s\n",__LINE__,__FILE__);return;}
#define ASSERT(e) assert(e)

void st_prt_page(st_ptr* pt);

void st_prt_page_showsub(st_ptr* pt, int showsub);

void st_prt_distribution(st_ptr* pt, task* tsk);

void _tk_print(page* pg);

void map_static_page(page* pgw);

int rt_do_read(task* t, st_ptr root);

void _rt_dump_function(task* t, st_ptr* root);

uint sim_new(uchar kdata[], uint ksize);

void test_stream_c2();

void test_compile_c();

void test_instance_c();

void test_copy(task* t, page* dst, st_ptr src);
void test_measure(task* t, st_ptr src);

extern cle_pipe _test_pipe_stdout;

st_ptr str(task* t, char* cs);

uint add(task* t, st_ptr p, char* cs);
uint rm(task* t, st_ptr p, char* cs);

st_ptr root(task* t);

#endif
