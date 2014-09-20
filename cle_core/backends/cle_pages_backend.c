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
#include "cle_backends.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

/*
 
 simple mem pager
 
 */

struct _mem_psrc_data {
	page* root;
    page* free;
	int pagecount;
};

static struct {
	page pg;
	short s[6];
} _dummy_root = { { &_dummy_root.pg, MEM_PAGE_SIZE, sizeof(page) + 10, 0 }, { 0, 0, 0, 0, 0, 0 } };

static page* mem_new_page(cle_psrc_data pd) {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
	page* pg = md->free;
    
    if (pg == 0) {
        pg = malloc(MEM_PAGE_SIZE);
        md->pagecount++;
    } else {
        md->free = pg->id;
    }
    
	pg->id = pg;
    pg->waste = 0;
    pg->size = MEM_PAGE_SIZE;
    pg->used = sizeof(page);
    
	return pg;
}

static page* mem_read_page(cle_psrc_data pd, cle_pageid id) {
	return (page*) id;
}

static page* mem_root_page(cle_psrc_data pd) {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
	return md->root;
}

static void mem_write_page(cle_psrc_data pd, cle_pageid id, page* pg) {
    // dead
}

static void mem_remove_page(cle_psrc_data pd, cle_pageid id) {
    struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
    page* pg = (page*) id;
    
    if (pg != &_dummy_root.pg) {
        pg->id = md->free;
        md->free = pg;
    }
}

static int mem_commit(cle_psrc_data pd, page* pg) {
    struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
    md->root = pg;
    return 0;
}

static void mem_unref_page(cle_psrc_data pd, page* pg) {
}

static int mem_pager_simple(cle_psrc_data pd) {
	return 0;
}

static cle_psrc_data mem_pager_clone(cle_psrc_data dat) {
	return dat;
}

cle_pagesource util_memory_pager = { mem_new_page, mem_read_page, mem_root_page, mem_write_page, mem_remove_page,
    mem_unref_page, mem_pager_simple, mem_commit, mem_pager_simple, mem_pager_simple, mem_pager_clone };

cle_psrc_data util_create_mempager() {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) malloc(sizeof(struct _mem_psrc_data));
	md->root = (page*) &_dummy_root;
    md->free = 0;
	md->pagecount = 0;
	return (cle_psrc_data) md;
}

int mempager_get_pagecount(cle_psrc_data pd) {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
	return md->pagecount;
}

void mempager_destroy(cle_psrc_data pd) {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;

    free(md);
}

