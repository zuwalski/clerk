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
#ifndef __CLE_CLERK_H__
#define __CLE_CLERK_H__

#include "cle_source.h"

typedef unsigned int uint;
typedef unsigned long ulong;
typedef unsigned short ushort;
typedef unsigned char uchar;
typedef const unsigned char* cdat;

typedef struct task task;

typedef struct st_ptr {
	struct page* pg;
	ushort key;
	ushort offset;
} st_ptr;

typedef struct ptr_list {
	struct ptr_list* link;
	st_ptr pt;
} ptr_list;

typedef struct it_ptr {
	struct page* pg;
	uchar* kdata;
	ushort key;
	ushort offset;
	ushort ksize;
	ushort kused;
} it_ptr;

typedef struct {
	cdat string;
	uint length;
} st_str;

struct st_stream;

/* generel functions */
// create empty node
// = 0 if ok - 1 if t is readonly
uint st_empty(task* t, st_ptr* pt);

// = 1 if pt points to empty node else = 0
uint st_is_empty(task* t, st_ptr* pt);

// test if path exsist from pt - else = 0
uint st_exist(task* t, st_ptr* pt, cdat path, uint length);

// make this ptr readonly
void st_readonly(st_ptr* pt);

// is this ptr readonly
uint st_is_readonly(st_ptr* pt);

// move ptr to path - else = 1
uint st_move(task* t, st_ptr* pt, cdat path, uint length);

// insert path - if already there = 1
uint st_insert(task* t, st_ptr* pt, cdat path, uint length);

uint st_update(task* t, st_ptr* pt, cdat path, uint length);

uint st_append(task* t, st_ptr* pt, cdat path, uint length);

uint st_delete(task* t, st_ptr* pt, cdat path, uint length);

uint st_clear(task* t, st_ptr* pt);

uint st_move_st(task* t, st_ptr* mv, st_ptr* str);

uint st_insert_st(task* t, st_ptr* to, st_ptr* from);

int st_exist_st(task* t, st_ptr* to, st_ptr* from);

uint st_delete_st(task* t, st_ptr* from, st_ptr* str);

int st_map(task* t, st_ptr* str, uint (*fun)(void*, cdat, uint, uint), void* ctx);

uint st_map_st(task* t, st_ptr* from, uint (*dat)(void*, cdat, uint, uint), uint (*push)(void*), uint (*pop)(void*), void* ctx);

//uint st_map_ptr(task* t, st_ptr* from, st_ptr* to, uint(*dat)(task*,st_ptr*,cdat,uint));

uint st_copy_st(task* t, st_ptr* to, st_ptr* from);

uint st_link(task* t, st_ptr* to, st_ptr* from);

uint st_dataupdate(task* t, st_ptr* pt, cdat path, uint length);

uint st_offset(task* t, st_ptr* pt, uint offset);
/*
 uint st_prepend(task* t, st_ptr* pt, cdat path, uint length, uint replace_length);
 */

int st_get(task* t, st_ptr* pt, char* buffer, uint buffer_length);

// read next char from pt and advance
int st_scan(task* t, st_ptr* pt);

// Convenience-function for static strings
st_ptr str(task* t, const char* cs);

//char* st_get_all(task* t, st_ptr* pt, uint* length);

/* iterator functions */
void it_create(task* t, it_ptr* it, st_ptr* pt);

void it_dispose(task* t, it_ptr* it);

void it_load(task* t, it_ptr* it, cdat path, uint length);

void it_reset(it_ptr* it);

uint it_new(task* t, it_ptr* it, st_ptr* pt);

uint it_next(task* t, st_ptr* pt, it_ptr* it, const int length);

uint it_next_eq(task* t, st_ptr* pt, it_ptr* it, const int length);

uint it_prev(task* t, st_ptr* pt, it_ptr* it, const int length);

uint it_prev_eq(task* t, st_ptr* pt, it_ptr* it, const int length);

uint it_current(task* t, it_ptr* it, st_ptr* pt);

/* Streaming functions */
struct st_stream* st_exist_stream(task* t, st_ptr* pt);
struct st_stream* st_merge_stream(task* t, st_ptr* pt);
struct st_stream* st_delete_stream(task* t, st_ptr* pt);
uint st_destroy_stream(struct st_stream* ctx);
uint st_stream_data(struct st_stream* ctx, cdat dat, uint length, uint at);
uint st_stream_push(struct st_stream* ctx);
uint st_stream_pop(struct st_stream* ctx);

/* Task functions */
task* tk_create_task(cle_pagesource* ps, cle_psrc_data psrc_data);

task* tk_clone_task(task* parent);

void tk_drop_task(task* t);
int cmt_commit_task(task* t);
int tk_delta(task* t, st_ptr* delete_tree, st_ptr* insert_tree);

// removing from h: internal use only!
void* tk_malloc(task* t, uint size);
void tk_mfree(task* t, void* mem);
// deprecated!
void* tk_realloc(task* t, void* mem, uint size);

void* tk_alloc(task* t, uint size, struct page** pgref);

void tk_root_ptr(task* t, st_ptr* pt);

ptr_list* ptr_list_reverse(ptr_list* e);
/* test */

void cle_panic(task* t);

#endif
