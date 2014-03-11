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
#include <stdlib.h>
#include <string.h>

#include "cle_struct.h"
#include "../test_clerk/test.h"

/* mem-manager */
// TODO: call external allocator
// TODO: should not be used outside task.c -> make private
void* tk_malloc(task* t, uint size) {
	void* m = malloc(size);
	if (m == 0)
		cle_panic(t);

	return m;
}

// TODO: remove -> use tk_alloc
void* tk_realloc(task* t, void* mem, uint size) {
	void* m = realloc(mem, size);
	if (m == 0)
		cle_panic(t);

	return m;
}
// TODO: (see tk_malloc)
void tk_mfree(task* t, void* mem) {
	free(mem);
}

static task_page* _tk_alloc_page(task* t, uint page_size) {
	task_page* pg = (task_page*) tk_malloc(t, page_size + sizeof(task_page) - sizeof(page));

	pg->pg.id = 0;
	pg->pg.size = page_size;
	pg->pg.used = sizeof(page);
	pg->pg.waste = 0;
	pg->pg.parent = 0;

	pg->refcount = 1;
	pg->next = t->stack;
	pg->ovf = 0;
	return pg;
}

void _tk_stack_new(task* t) {
	t->stack = _tk_alloc_page(t, PAGE_SIZE);
}

void* tk_alloc(task* t, uint size, struct page** pgref) {
	task_page* pg = t->stack;
	uint offset;

	if (pg->pg.used + size + 7 > PAGE_SIZE) {
		if (size > PAGE_SIZE - sizeof(page)) {
			// big chunk - alloc specific
			task_page* tmp = _tk_alloc_page(t, size + sizeof(page));

			tmp->pg.used = tmp->pg.size;

			tmp->next = t->stack->next;
			t->stack->next = tmp;

			if (pgref != 0)
				*pgref = &tmp->pg;
			return (void*) ((char*) tmp + sizeof(task_page));
		}

		_tk_stack_new(t);
		pg = t->stack;
	}

	// align 8 (avoidable? dont use real pointers from here - eg. always copy?)
	if (pg->pg.used & 7)
		pg->pg.used += 8 - (pg->pg.used & 7);

	offset = pg->pg.used;
	pg->pg.used += size;

	t->stack->refcount++;

	if (pgref != 0)
		*pgref = &t->stack->pg;
	return (void*) ((char*) &pg->pg + offset);
}

static void _tk_release_page(task* t, task_page* wp) {

}

static page* _tk_load_page(task* t, cle_pageid pid, page* parent) {
	st_ptr root_ptr = t->pagemap;
	page* pw;

	// have a writable copy of the page?
	if (t->wpages == 0 || st_move(t, &root_ptr, (cdat) &pid, sizeof(pid))) {
		pw = (page*) pid;
	}
	// found: read address of page-copy
	else if (st_get(t, &root_ptr, (char*) &pw, sizeof(pw)) != -1)
		cle_panic(t); // map corrupted

	return pw;
}

page* _tk_check_page(task* t, page* pw) {
	if (pw->id == pw && t->wpages != 0) {
		st_ptr root_ptr = t->pagemap;

		// have a writable copy of the page?
		if (st_move(t, &root_ptr, (cdat) &pw->id, sizeof(cle_pageid)) == 0)
			if (st_get(t, &root_ptr, (char*) &pw, sizeof(pw)) != -1)
				cle_panic(t); // map corrupted
	}
	return pw;
}

page* _tk_check_ptr(task* t, st_ptr* pt) {
	pt->pg = _tk_check_page(t, pt->pg);
	return pt->pg;
}

/* copy to new (internal) page */
page* _tk_write_copy(task* t, page* pg) {
	st_ptr root_ptr;
	task_page* tpg;
	page* newpage;

	if (pg->id != pg)
		return pg;

	// add to map of written pages
	root_ptr = t->pagemap;

	if (st_insert(t, &root_ptr, (cdat) &pg->id, sizeof(cle_pageid)) == 0) {
        // already there
		if (st_get(t, &root_ptr, (char*) &pg, sizeof(pg)) != -1)
			cle_panic(t); // map corrupted
		return pg;
	}

    // copy-on-write: new page
	tpg = _tk_alloc_page(t, pg->size);
	newpage = &tpg->pg;

	memcpy(newpage, pg, pg->used);

	// pg in written pages list
	tpg->next = t->wpages;
	t->wpages = tpg;

	st_append(t, &root_ptr, (cdat) &newpage, sizeof(page*));

	return newpage;
}

key* _tk_get_ptr(task* t, page** pg, key* me) {
	ptr* pt = (ptr*) me;
	if (pt->koffset != 0) {
        
        // DEBUG
        if(pt->koffset == 1){
            st_ptr root;
            root.pg = *pg;
            root.key = sizeof(page);
            root.offset = 0;
            st_prt_page(&root);
        }
        
        
		*pg = (page*) pt->pg;
		me = GOKEY(*pg,pt->koffset); /* points to a key - not an ovf-ptr */
	} else {
		*pg = _tk_load_page(t, pt->pg, *pg);
		/* go to root-key */
		me = GOKEY(*pg,sizeof(page));
	}
	return me;
}

ushort _tk_alloc_ptr(task* t, task_page* pg) {
	ushort nkoff = pg->pg.used + (pg->pg.used & 1);
    
    // room on the page itself?
    if (nkoff + sizeof(ptr) <= pg->pg.size) {
        pg->pg.used = nkoff + sizeof(ptr);
    } else {
        overflow* ovf = pg->ovf;

        if (ovf == 0) /* alloc overflow-block */
        {
            ovf = (overflow*) tk_malloc(t, OVERFLOW_GROW);
            
            ovf->size = OVERFLOW_GROW;
            ovf->used = 16;
            
            pg->ovf = ovf;
        } else if (ovf->used == ovf->size) /* resize overflow-block */
        {
            ovf->size += OVERFLOW_GROW;
            
            ovf = (overflow*) tk_realloc(t, ovf, ovf->size);
            
            pg->ovf = ovf;
        }
        
        /* make pointer */
        nkoff = (ovf->used >> 4) | 0x8000;
        ovf->used += 16;
    }
	return nkoff;
}

void _tk_remove_tree(task* t, page* pg, ushort off) {

}

void tk_unref(task* t, struct page* pg) {
	if (pg->id == 0) {
		task_page* tp = TO_TASK_PAGE(pg);

		if (--tp->refcount == 0)
			_tk_release_page(t, tp);
	}
}

void tk_free_ptr(task* t, st_ptr* ptr) {
	tk_unref(t, ptr->pg);
}

void tk_ref_ptr(st_ptr* ptr) {
	if (ptr->pg->id == 0)
		TO_TASK_PAGE(ptr->pg) ->refcount++;
}

void tk_root_ptr(task* t, st_ptr* pt) {
	key* k;
	_tk_check_ptr(t, &t->root);

	k = GOKEY(t->root.pg,t->root.key);
	if (ISPTR(k)) {
		k = _tk_get_ptr(t, &t->root.pg, k);
		t->root.key = sizeof(page);
	}

	*pt = t->root;
}

void tk_dup_ptr(st_ptr* to, st_ptr* from) {
	*to = *from;
	tk_ref_ptr(to);
}

ushort tk_segment(task* t) {
	return t->segment;
}

segment tk_new_segment(task* t) {
	cle_panic(t); // not impl yet - will delegate to pagesource
	return 0;
}

task* tk_create_task(cle_pagesource* ps, cle_psrc_data psrc_data) {
	// initial alloc
	task* t = (task*) tk_malloc(0, sizeof(task));

	t->stack = 0;
	t->wpages = 0;
	t->segment = 1; // TODO get from pager
	t->ps = ps;
	t->psrc_data = psrc_data;

	_tk_stack_new(t);

	st_empty(t, &t->pagemap);

	if (ps) {
		t->root.pg = ps->root_page(psrc_data);
		t->root.key = sizeof(page);
		t->root.offset = 0;
	} else {
		st_empty(t, &t->root);
	}

	return t;
}

task* tk_clone_task(task* parent) {
	return tk_create_task(parent->ps, (parent->ps == 0) ? 0 : parent->ps->pager_clone(parent->psrc_data));
}

static void _tk_free_page_list(task_page* pw) {
	while (pw) {
		task_page* next = pw->next;

		tk_mfree(0, pw->ovf);
		tk_mfree(0, pw);

		pw = next;
	}
}

void tk_drop_task(task* t) {
	_tk_free_page_list(t->stack);

	_tk_free_page_list(t->wpages);

	// quit the pager here
	if (t->ps != 0)
		t->ps->pager_close(t->psrc_data);

	// last: free initial alloc
	tk_mfree(0, t);
}

/////////////////////////////// Sync v1 ///////////////////////////////////

struct trace_ptr {
	st_ptr* base;
	st_ptr ptr;
};

struct _tk_trace_page_hub {
	struct _tk_trace_page_hub* next;
	page* pgw;
	uint from, to;
};

struct _tk_trace_base {
	struct _tk_trace_page_hub* free;
	struct _tk_trace_page_hub* chain;
	ushort* kstack;
	task* t;
	uint ssize;
	uint sused;
};

#define _tk_load_orig(p) (((p)->id) ? (page*) (p)->id : (p))

static void _tk_base_reset(struct _tk_trace_base* base) {
	base->sused = 0;

	while (base->chain != 0) {
		struct _tk_trace_page_hub* n = base->chain->next;

		base->chain->next = base->free;
		base->free = base->chain;

		base->chain = n;
	}
}

static struct _tk_trace_page_hub* _tk_new_hub(struct _tk_trace_base* base, page* pgw, uint from, uint to) {
	struct _tk_trace_page_hub* r;
	if (base->free != 0) {
		r = base->free;
		base->free = r->next;
	} else {
		r = tk_alloc(base->t, sizeof(struct _tk_trace_page_hub), 0);
	}

	r->next = 0;
	r->pgw = pgw;
	r->from = from;
	r->to = to;

	return r;
}

static void _tk_push_key(struct _tk_trace_base* base, ushort k) {
	if (base->sused == base->ssize) {
		base->ssize += PAGE_SIZE;
		base->kstack = tk_realloc(base->t, base->kstack, base->ssize * sizeof(ushort));
	}

	base->kstack[base->sused++] = k;
}

static st_ptr _tk_trace_write(struct _tk_trace_base* base, page* pgw, st_ptr ptr, uint from, uint to, ushort offset) {
	if (from != to) {
		key* kp = GOOFF(pgw,base->kstack[from]);
		uchar* dat = KDATA(kp);
		uint i;

		for (i = from + 1; i < to; i++) {
			kp = GOOFF(pgw,base->kstack[i]);

			st_insert(base->t, &ptr, dat, kp->offset >> 3);

			dat = KDATA(kp);
		}

		st_insert(base->t, &ptr, dat, offset >> 3);
	}

	return ptr;
}

static st_ptr _tk_trace(struct _tk_trace_base* base, page* pgw, struct trace_ptr* pt, uint start_depth, ushort offset) {
	if (pt->ptr.pg == 0) {
		struct _tk_trace_page_hub* lead = base->chain;

		pt->ptr = *pt->base;

		while (lead != 0) {
			key* k = GOOFF(lead->pgw,base->kstack[lead->to]);

			pt->ptr = _tk_trace_write(base, lead->pgw, pt->ptr, lead->from, lead->to, k->offset);

			lead = lead->next;
		}
	}

	return _tk_trace_write(base, pgw, pt->ptr, start_depth, base->sused, offset);
}

static void _tk_insert_trace(struct _tk_trace_base* base, page* pgw, struct trace_ptr* insert_tree, uint start_depth,
		page* lpgw, ushort lkey, ushort offset) {
	// round up - make sure the last (diff'ed on) byte is there
	st_ptr root = _tk_trace(base, pgw, insert_tree, start_depth, offset + 7);

	ushort pt = _tk_alloc_ptr(base->t, TO_TASK_PAGE(root.pg) );

	key* kp = GOOFF(root.pg,root.key);

	ptr* ptrp = (ptr*) GOPTR(root.pg,pt);

	ptrp->pg = lpgw;
	ptrp->koffset = lkey;
	// the diff was on the last byte - adjust the offset
	ptrp->offset = root.offset - (8 - (offset & 7));

	ptrp->next = 0;
	ptrp->ptr_id = PTR_ID;

	if (kp->sub == 0) {
		kp->sub = pt;
	} else {
		kp = GOOFF(root.pg,kp->sub);

		while (kp->next != 0) {
			kp = GOOFF(root.pg,kp->next);
		}

		kp->next = pt;
	}
}

static void _tk_delete_trace(struct _tk_trace_base* base, page* pgw, struct trace_ptr* del_tree, uint start_depth, ushort found,
		ushort expect) {
	const page* orig = _tk_load_orig(pgw);
	const ushort lim = orig->used;
	// skip new keys
	while (found >= lim) {
		key* kp = GOOFF(pgw,found);

		found = kp->next;
	}

	while (found != expect) {
		// expect was deleted
		key* ok = (key*) ((char*) orig + expect);

		st_ptr root = _tk_trace(base, pgw, del_tree, start_depth, ok->offset);

		st_insert(base->t, &root, KDATA(ok), 1);

		expect = ok->next;
	}
}

static void _tk_trace_change(struct _tk_trace_base* base, page* pgw, struct trace_ptr* delete_tree,
		struct trace_ptr* insert_tree) {
	const page* orig = _tk_load_orig(pgw);
	const uint start_depth = base->sused, lim = orig->used;
	uint k = sizeof(page);

	while (1) {
		key* kp = GOOFF(pgw,k);

		// new key
		if (k >= lim) {
			if (ISPTR(kp)) {
				ptr* pt = (ptr*) kp;
				_tk_insert_trace(base, pgw, insert_tree, start_depth, pt->pg, pt->koffset, pt->offset);
			} else
				_tk_insert_trace(base, pgw, insert_tree, start_depth, pgw, k, kp->offset);
		} else {
			// old key - was it changed? (deletes only)
			key* ok = (key*) ((char*) orig + k);

			// changed next
			if (kp->next != ok->next)
				_tk_delete_trace(base, pgw, delete_tree, start_depth, kp->next, ok->next);

			if (ISPTR(kp) == 0) {
				// push content step
				_tk_push_key(base, k);

				// changed sub
				if (kp->sub != ok->sub)
					_tk_delete_trace(base, pgw, delete_tree, start_depth, kp->sub, ok->sub);

				// shortend key
				if (kp->length < ok->length) {
					st_ptr root = _tk_trace(base, pgw, delete_tree, start_depth, kp->length);

					st_insert(base->t, &root, KDATA(ok) + (kp->length >> 3), 1);
				}
				// appended to
				else if (kp->length > ok->length)
					_tk_trace(base, pgw, insert_tree, start_depth, kp->length + 7);

				if (kp->sub != 0) {
					k = kp->sub;
					// keep k on stack
					continue;
				}

				base->sused--;
			}
		}

		if (kp->next != 0)
			k = kp->next;
		else {
			// pop content step
			do {
				if (start_depth == base->sused)
					return;

				k = base->kstack[--base->sused];
				kp = GOOFF(pgw,k);
				k = kp->next;
			} while (k == 0);
		}
	}
}

static struct _tk_trace_page_hub* _tk_trace_page_ptr(struct _tk_trace_base* base, page* pgw, page* find) {
	const uint start_depth = base->sused;
	uint k = sizeof(page);

	while (1) {
		key* kp = GOOFF(pgw,k);

		if (ISPTR(kp)) {
			ptr* pt = (ptr*) kp;
			if (pt->koffset == 0 && pt->pg == find->id) {
				struct _tk_trace_page_hub* h;
				const uint to = base->sused + 1;

				_tk_push_key(base, k);

				if (pgw->parent != 0) {
					struct _tk_trace_page_hub* r = _tk_trace_page_ptr(base, pgw->parent, pgw);
					if (r == 0)
						return 0;

					h = _tk_new_hub(base, pgw, start_depth, to);
					r->next = h;
				} else {
					h = _tk_new_hub(base, pgw, start_depth, to);
					base->chain = h;
				}

				return h;
			}
		}

		if (ISPTR(kp) == 0 && kp->sub != 0) {
			// push content step
			_tk_push_key(base, k);
			k = kp->sub;
		} else if (kp->next != 0)
			k = kp->next;
		else {
			// pop content step
			do {
				if (start_depth == base->sused)
					return 0;

				k = base->kstack[--base->sused];
				kp = GOOFF(pgw,k);
				k = kp->next;
			} while (k == 0);
		}
	}
}

// returns res & 1 => deletes , res & 2 => inserts
int tk_delta(task* t, st_ptr* delete_tree, st_ptr* insert_tree) {
	struct _tk_trace_base base;
	task_page* pgw;
	int res = 0;

	base.sused = base.ssize = 0;
	base.chain = base.free = 0;
	base.kstack = 0;
	base.t = t;

	for (pgw = t->wpages; pgw != 0; pgw = pgw->next) {
		if (pgw->pg.parent == 0 || _tk_trace_page_ptr(&base, pgw->pg.parent, &pgw->pg)) {
			struct trace_ptr t_delete, t_insert;

			t_delete.base = delete_tree;
			t_insert.base = insert_tree;
			t_delete.ptr.pg = t_insert.ptr.pg = 0;

			_tk_trace_change(&base, &pgw->pg, &t_delete, &t_insert);
			res |= (t_delete.ptr.pg != 0) | (t_insert.ptr.pg != 0) << 1;
		}

		_tk_base_reset(&base);
	}

	tk_mfree(t, base.kstack);

	return res;
}
