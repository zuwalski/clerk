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

#define LOG_SIZE 1000000

/*
 
 simple mem pager

 */
struct _log {
    struct _log* prev;
    int size;
    int used;
};

struct _mem_psrc_data {
    struct _log* log;
	page* root;
	int pagecount;
    int log_count;
};

//page _dummy_root = {ROOT_ID,MEM_PAGE_SIZE,sizeof(page) + 10,0,0,0,1,0,0,0};
static struct {
	page pg;
	short s[6];
} _dummy_root = { { &_dummy_root, 0, MEM_PAGE_SIZE, sizeof(page) + 10, 0 }, { 0, 0, 0, 0, 0, 0 } };

static page* mem_new_page(cle_psrc_data pd) {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
	page* pg;

    if (md->log == 0 || md->log->size - md->log->used < MEM_PAGE_SIZE * 2) {
        struct _log* prev = md->log;
        
        md->log_count++;
        md->log = malloc(LOG_SIZE);
        
        md->log->prev = prev;
        md->log->size = LOG_SIZE;
        md->log->used = sizeof(struct _log);
    } else {
        pg = (page*) ((char*) md->log + md->log->used); // prev page
        
        md->log->used += pg->used;
        md->log->used += 16 - (md->log->used & 15);
    }
    
    assert(md->log->used <= md->log->size);

    pg = (page*) ((char*) md->log + md->log->used);
	pg->id = pg;
    pg->waste = 0;
	pg->parent = 0;
    pg->size = MEM_PAGE_SIZE;
    pg->used = sizeof(page);
    
	md->pagecount++;
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
    // write remove-log
}

static int mem_commit(cle_psrc_data pd, page* pg) {
    struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
    
    md->root = pg;
    
    /*
    if (((void*)pg > (void*)md->log) && ((char*)pg < ((char*)md->log + md->log->used))) {
        md->log->used = (int) ((char*) pg - (char*) md->log) + pg->used;
        
        assert(md->log->used <= md->log->size);
    } else
        md->log->used = sizeof(struct _log);
    */
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

cle_pagesource util_memory_log = { mem_new_page, mem_read_page, mem_root_page, mem_write_page, mem_remove_page,
		mem_unref_page, mem_pager_simple, mem_commit, mem_pager_simple, mem_pager_simple, mem_pager_clone };

cle_psrc_data util_create_memlog() {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) malloc(sizeof(struct _mem_psrc_data));
	md->root = (page*) &_dummy_root;
    md->log = 0;
	md->pagecount = 0;
    md->log_count = 0;
	return (cle_psrc_data) md;
}

int memlog_get_pagecount(cle_psrc_data pd) {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
	return md->pagecount;
}

int memlog_get_logcount(cle_psrc_data pd) {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
	return md->log_count;
}

void memlog_destroy(cle_psrc_data pd) {
	struct _mem_psrc_data* md = (struct _mem_psrc_data*) pd;
    while (md->log) {
        struct _log* t = md->log->prev;
        
        free(md->log);
        
        md->log = t;
    }
    
    free(md);
}

