/*
 Clerk application and storage engine.
 Copyright (C) 2014  Lars Szuwalski
 
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

#include "cle_struct.h"
#include <string.h>
#include <assert.h>

#include "../test_clerk/test.h"

struct _tk_setup {
	page* dest;
	task* t;
    
	uint size_byte;
	uint halfsize_byte;
    
	ushort o_pt;
};

static void _tk_compact_copy2(struct _tk_setup* setup, page* pg, key* parent, ushort next, int adjoffset) {
    while (next) {
        page* dest = setup->dest;
        key* k = GOOFF(pg, next);
        // trace to end-of-next's
        _tk_compact_copy2(setup, pg, parent, k->next, adjoffset);
        
        if (ISPTR(k)) // pointer
        {
            ptr* pt = (ptr*) k;
            if(setup->o_pt == 0) {
                setup->o_pt = next;
            }
            if (pt->koffset) {
                pg = (page*) pt->pg;
                k = GOKEY(pg,pt->koffset);
                k->offset = pt->offset;
                // continue processing target
            } else {
                ptr* newptr = (ptr*) GOKEY(dest, dest->used);
                newptr->pg = pt->pg;
                newptr->koffset = 0;
                newptr->ptr_id = PTR_ID;
                newptr->offset = pt->offset + adjoffset;
                newptr->next = parent->sub;
                parent->sub = dest->used;
                dest->used += sizeof(ptr);
                return;
            }
        }
        
        if (k->length == 0) {// empty key? (skip)
            adjoffset += k->offset;
        } else {
            if (setup->o_pt == 0 && (CEILBYTE(k->length) + sizeof(key) >= sizeof(ptr))) {
                setup->o_pt = next;
            }
            
            if (k->offset + adjoffset == parent->length) // append to parent key?
            {
                memcpy(KDATA(parent) + (parent->length >> 3), KDATA(k), CEILBYTE(k->length));
                
                adjoffset = parent->length & 0xFFF8; // 'my' subs are offset by floor(parent-length)
                
                parent->length = k->length + adjoffset;
                
                dest->used = ((char*)parent - (char*)dest) + sizeof(key) + CEILBYTE(parent->length);
                dest->used += dest->used & 1;
            }
            else // key w/data
            {
                key* np = GOKEY(dest, dest->used);
                np->offset = k->offset + adjoffset;
                np->length = k->length;
                np->sub = 0;
                np->next = parent->sub;
                parent->sub = dest->used;
                
                memcpy(KDATA(np), KDATA(k), CEILBYTE(k->length));
                dest->used += sizeof(key) + CEILBYTE(k->length);
                dest->used += dest->used & 1;
                adjoffset = 0;
                parent = np;
            }
        }
        
        next = k->sub;
    }
}

static void _tk_root_copy(struct _tk_setup* setup, page* pw, key* copy, ushort next, int cut_adj) {
    page* dest = setup->dest;
	key* root = GOKEY(dest, sizeof(page));

    assert(!ISPTR(copy));

	root->offset = root->next = root->sub = 0;
	root->length = copy->length - (cut_adj & 0xFFF8);
    
    memcpy(KDATA(root), KDATA(copy) + (cut_adj >> 3), CEILBYTE(root->length));
    
    dest->used = sizeof(page) + sizeof(key) + CEILBYTE(root->length);
    dest->used += dest->used & 1;
    
    _tk_compact_copy2(setup, pw, root, next, -(cut_adj & 0xFFF8));
    
    assert(dest->used <= dest->size);
}

// -----------

void test_copy(task* t, page* dst, st_ptr src) {
    /*
    key* copy = GOOFF(src.pg, src.key);
    
    _tk_root_copy(dst, src.pg, copy, copy->sub, src.offset);
    
    int i = sizeof(page);
    while (i < dst->used) {
        key* k = GOOFF(dst, i);
        
        printf("%d (o: %d, l: %d) (s: %d, n: %d)\n", i, k->offset, k->length, k->sub, k->next);
        
        if (ISPTR(k))
            i += sizeof(ptr);
        else
            i += sizeof(key) + CEILBYTE(k->length);
        
        i += i & 1;
    }
     */
}

void test_measure(task* t, st_ptr src) {
}

// --------

static ushort _tk_link_and_create_page(struct _tk_setup* setup, page* pw, int ptr_offset) {
    ptr* pt;
    
    assert(setup->o_pt);
    
	// no large enough existing key?
	if (setup->o_pt == 0) {
		//setup->o_pt = _tk_alloc_ptr(setup->t, TO_TASK_PAGE(pw) ); // might change ptr-address!
	}
    
	// create a link to new page
    pt = (ptr*) GOOFF(pw,setup->o_pt);
    pt->offset = ptr_offset;
    pt->next = 0;
	pt->ptr_id = PTR_ID;
	pt->koffset = 0;
    pt->pg = setup->dest->id;
    
	return setup->o_pt;
}

static uint _tk_cut(struct _tk_setup* setup, page* pg, key* copy, ushort* from, uint cut_at, uint size) {
    if (size < setup->halfsize_byte) {
        return size;
    }
    
    // get a fresh page
    setup->dest = setup->t->ps->new_page(setup->t->psrc_data);
    
    setup->o_pt = 0;
    
    // start compact-copy
    _tk_root_copy(setup, pg, copy, *from, cut_at);
    
    // cut-off 'copy'
    copy->length = cut_at;
    
    // link ext-pointer to new page
    *from = _tk_link_and_create_page(setup, pg, cut_at);

    return sizeof(ptr);
}

static uint _tk_measure(struct _tk_setup* setup, page* pg, key* parent, key* k) {
    uint next_size = k->next ? _tk_measure(setup, pg, parent, GOOFF(pg, k->next)) : 0;

    // reduce parent and set k->next
    next_size = _tk_cut(setup, pg, parent, &k->next, k->offset + 1, next_size);
    
    if (ISPTR(k)) {
        ptr* pt = (ptr*) k;
        
        return next_size + (pt->koffset ? _tk_measure(setup, pt->pg, parent, GOOFF(pt->pg, pt->koffset)) : sizeof(ptr));
    } else {
        uint k_size = k->sub ? _tk_measure(setup, pg, k, GOOFF(pg, k->sub)) : 0;

        k_size += sizeof(key) + CEILBYTE(k->length);
        
        return next_size + _tk_cut(setup, pg, k, &k->sub, 0, k_size);
    }
}

static ushort _cmt_find_ptr(page* cpg, page* find, ushort koff) {
    while (koff) {
        key* k = GOKEY(cpg, koff);
        
        if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
            if (pt->koffset == 0 && pt->pg == find->id)
                break;
        } else if((koff = _cmt_find_ptr(cpg, find, k->sub))) {
            break;
        }
        
        koff = k->next;
    }
    return koff;
}

static page* _cmt_mark_and_link(task* t) {
    task_page *end = 0;
    page* root = 0;
    
    while (t->wpages != end) {
        task_page* tp, *first = t->wpages;
        
        for (tp = first; tp != end; tp = tp->next) {
            page* find = &tp->pg;
            page* parent = _tk_parent(t, find->id);
            
            if (parent) {
                ushort pt_off = _cmt_find_ptr(parent, find, sizeof(page));
                if (pt_off) {
                    ptr* pt;
                    // not yet writable?
                    if (parent == parent->id)
                        parent = _tk_write_copy(t, parent);
                    
                    pt = (ptr*) GOOFF(parent, pt_off);
                    pt->koffset = sizeof(page);
                    pt->pg = find;
                } else {
                    // ptr not found in parent
                    cle_panic(t);
                }
            } else {
                root = find;
            }
            
            t->ps->remove_page(t->psrc_data, tp->pg.id);
        }
        
        end = first;
    }
    
    return root;
}

/**
 * Rebuild all changes into new root => create new db-version and switch to it.
 *
 * return 0 if ok - also if no changes was written.
 */
int cmt_commit_task(task* t) {
    int stat = 0;
	page* root = _cmt_mark_and_link(t);
    
	// rebuild from root => new root
	if (root) {
        struct _tk_setup setup;
        key* k;
        
        setup.t = t;
        setup.dest = 0;
        setup.halfsize_byte = root->size / 2;
        setup.size_byte = root->size - sizeof(page);
        
        k = GOKEY(root, sizeof(page));
        assert(!ISPTR(k));
        _tk_measure(&setup, root, k, GOOFF(root, k->sub));
        
        // reset and copy remaining rootpage
        setup.dest = t->ps->new_page(t->psrc_data);
        
        k = GOKEY(root, sizeof(page));
        _tk_root_copy(&setup, root, k, k->sub, 0);
        
        // avoid ptr-only-root-page problem
        k = GOKEY(setup.dest, sizeof(page));
        if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
            t->ps->remove_page(t->psrc_data, setup.dest);
            setup.dest = pt->pg;
        }

        // swap root
        stat = t->ps->pager_commit(t->psrc_data, setup.dest);
	}
    
	tk_drop_task(t);
    
	return stat;
}
