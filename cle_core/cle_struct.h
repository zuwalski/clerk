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
#ifndef __CLE_STRUCT_H__
#define __CLE_STRUCT_H__

#include "cle_clerk.h"

/* Config */

#define PAGE_SIZE (1024)

#define OVERFLOW_GROW (16*8)

#define IT_GROW_SIZE 32

#define PTR_ID 0xFFFF

/* Defs */

typedef struct key
{
	ushort offset;
	ushort length;
	ushort next;
	ushort sub;
} key;

typedef struct ptr
{
	ushort offset;
	ushort ptr_id;
	ushort next;
	ushort koffset;
	void*  pg;
} ptr;

typedef struct overflow {
	unsigned int used;
	unsigned int size;
} overflow;

typedef struct task_page {
	struct task_page* next;
	overflow*    ovf;
	unsigned long refcount;

	page pg;
} task_page;

struct task
{
	task_page*      stack;
	task_page*      wpages;
	cle_pagesource* ps;
	cle_psrc_data   psrc_data;
	segment         segment;
	st_ptr			root;
	st_ptr			pagemap;
};

#define TO_TASK_PAGE(pag) ((task_page*)((char*)(pag) - (unsigned long)(&((task_page*)0)->pg)))
#define GOKEY(pag,off) ((key*)((char*)(pag) + (off)))
#define GOPTR(pag,off) ((key*)(((char*)(TO_TASK_PAGE(pag))->ovf) + (((off) ^ 0x8000)<<4)))
#define GOOFF(pag,off) ((off & 0x8000)? GOPTR(pag,off):GOKEY(pag,off))
#define KDATA(k) ((unsigned char*)k + sizeof(key))
#define CEILBYTE(l)(((l) + 7) >> 3)
#define ISPTR(k) ((k)->length == PTR_ID)

key* _tk_get_ptr(task* t, page** pg, key* me);
ushort _tk_alloc_ptr(task* t, task_page* pg);
void _tk_stack_new(task* t);
void _tk_remove_tree(task* t, page* pg, ushort key);
page* _tk_write_copy(task* t, page* pg);
//void tk_unref(task* t, page_wrap* pg);
page* _tk_check_ptr(task* t, st_ptr* pt);
page* _tk_check_page(task* t, page* pw);

void tk_stats();

#endif
