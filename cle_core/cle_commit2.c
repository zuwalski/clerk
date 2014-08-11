/*
 Clerk application and storage engine.
 Copyright (C) 2013  Lars Szuwalski
 
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

// -------------

struct _tk_setup {
	page* dest;
	task* t;
    
	uint halfsize;
	uint fullsize;
    
	ushort o_pt;
	ushort l_pt;
};

static uint _tk_measure2x(page* pw, ushort parent, ushort kptr) {
    uint size = 0;
    
    while (kptr) {
        key* k = GOOFF(pw,kptr);
        uint nsize = _tk_measure2x(pw, parent, k->next);
        
        printf("%d -> %d = %d (%d)\n", parent, kptr, nsize, size);
        
        size += nsize;
        
        if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
            if (pt->koffset > 1) {
                pw = pt->pg;
            } else {
                return size + sizeof(ptr);
            }
        } else if (k->length) {
            nsize = 1 + sizeof(key) + CEILBYTE(k->length);
            
            nsize += _tk_measure2x(pw, kptr, k->sub);
            
            printf("%d = %d (%d)\n", kptr, nsize, size);
            
            return size + nsize;
        }
        
        kptr = k->sub;
    }
    
    return size;
}

static void _tk_compact_copy(struct _tk_setup* setup, page* pw, key* parent, ushort* rsub, ushort next, int adjoffset) {
	while (next != 0) {
		key* k = GOOFF(pw,next);
		// trace to end-of-next's
		if (k->next != 0)
			_tk_compact_copy(setup, pw, parent, rsub, k->next, adjoffset);
        
		// trace a place for a pointer on this page (notice: PTR_ID == MAX-USHORT)
		if (setup->l_pt < k->length) {
			setup->l_pt = k->length;
			setup->o_pt = next;
		}
        
		while (ISPTR(k)) // pointer
		{
			ptr* pt = (ptr*) k;
			if (pt->koffset > 1) {
				pw = (page*) pt->pg;
				k = GOKEY(pw,pt->koffset);
			} else {
				ptr* newptr;
				setup->dest->used += setup->dest->used & 1;
				newptr = (ptr*) ((char*) setup->dest + setup->dest->used);
				newptr->koffset = 0;
				newptr->ptr_id = PTR_ID;
				newptr->offset = pt->offset + adjoffset;
				newptr->next = *rsub;
				newptr->pg = pt->pg;
				*rsub = setup->dest->used;
				setup->dest->used += sizeof(ptr);
				return;
			}
		}
        
		if (k->length == 0) // empty key? (skip)
			adjoffset += k->offset;
		else if ((parent != 0) && (k->offset + adjoffset == parent->length)) // append to parent key?
        {
			adjoffset = parent->length & 0xFFF8; // 'my' subs are offset by parent-length
            
			memcpy(KDATA(parent) + (parent->length >> 3), KDATA(k), CEILBYTE(k->length));
			parent->length = k->length + adjoffset;
			setup->dest->used += CEILBYTE(k->length);
		} else // key w/data
		{
			setup->dest->used += setup->dest->used & 1;
			parent = (key*) ((char*) setup->dest + setup->dest->used);
			parent->offset = k->offset + adjoffset;
			parent->length = k->length;
			parent->next = *rsub;
			parent->sub = 0;
			memcpy(KDATA(parent), KDATA(k), CEILBYTE(k->length));
			*rsub = setup->dest->used;
			rsub = &parent->sub;
			setup->dest->used += sizeof(key) + CEILBYTE(k->length);
			adjoffset = 0;
		}
		next = k->sub;
	}
}

static ushort _tk_link_and_create_page(struct _tk_setup* setup, page* pw, int ptr_offset) {
	// first: create a link to new page
	ptr* pt;
	ushort pt_offset;
    
	// use exsisting key?
	if (setup->l_pt + sizeof(key) * 8 >= sizeof(ptr) * 8) {
		pt_offset = setup->o_pt;
		pt = (ptr*) GOOFF(pw,pt_offset);
	}
	// room for ptr in page?
	else if (pw->used + sizeof(ptr) <= pw->size) {
		pt_offset = pw->used;
		pt = (ptr*) GOKEY(pw,pt_offset);
		pw->used += sizeof(ptr);
	} else {
		pt_offset = _tk_alloc_ptr(setup->t, TO_TASK_PAGE(pw) ); // might change ptr-address!
		pt = (ptr*) GOPTR(pw,pt_offset);
	}
    
	pt->offset = ptr_offset;
	pt->ptr_id = PTR_ID;
	pt->koffset = pt->next = 0;
    pt->pg = setup->dest->id;
	// then: create new page (copy dest) and put pageid in ptr
	//pt->pg = setup->t->ps->new_page(setup->t->psrc_data);
    //setup->dest->id = pt->pg;
    //memcpy(pt->pg, setup->dest, setup->dest->used);
    
	return pt_offset;
}

static int _tk_adjust_cut(page* pw, key* copy, key* prev, int cut_bid) {
	int limit = copy->length;
    
	if (prev != 0) {
		if (prev->next != 0)
			limit = GOOFF(pw,prev->next)->offset;
	} else if (copy->sub != 0)
		limit = (GOOFF(pw,copy->sub)->offset) & 0xFFF8;
    
	return (cut_bid > limit) ? limit : cut_bid;
}

static key* _tk_create_root_key(struct _tk_setup* setup, key* copy, int cut_adj) {
	// copy first/root key
	key* root = (key*) ((char*) setup->dest + sizeof(page));
    
	root->offset = root->next = root->sub = 0;
	root->length = copy->length - (cut_adj & 0xFFF8);
    
	memcpy(KDATA(root), KDATA(copy) + (cut_adj >> 3), CEILBYTE(root->length));
    
	setup->dest->used = sizeof(page) + sizeof(key) + CEILBYTE(root->length);
    
	return root;
}

static uint _tk_cut_key(struct _tk_setup* setup, page* pw, key* copy, key* prev, int cut_bid) {
    setup->dest = setup->t->ps->new_page(setup->t->psrc_data);
    
	int cut_adj = _tk_adjust_cut(pw, copy, prev, cut_bid);
	
    key* root = _tk_create_root_key(setup, copy, cut_adj);
    
	// cut-off 'copy'
	copy->length = cut_adj;
    
	// start compact-copy
	setup->o_pt = setup->l_pt = 0;
	_tk_compact_copy(setup, pw, root, &root->sub, (prev != 0) ? prev->next : copy->sub, -(cut_adj & 0xFFF8));
    
    assert(setup->dest->used <= setup->dest->size);
    
	// link ext-pointer to new page
	if (prev != 0)
		prev->next = _tk_link_and_create_page(setup, pw, cut_adj);
	else
		copy->sub = _tk_link_and_create_page(setup, pw, cut_adj);
    
	return (sizeof(ptr) * 8);
}

// ----------

static key EMPTY = { 0, 0, 0, 0 };

// BEGIN COPY
struct _cmt_cpy_ctx {
    char* dest_begin;
    char* dest;
    key* k;
    ptr* ptr_spot;
};

struct _cmt_cp {
    ushort* parent;
    ushort offset;
};

static void cp_dat(struct _cmt_cpy_ctx* ctx, cdat c, uint l) {
    ctx->k->length += l * 8;
    memcpy(ctx->dest, c, l);
    ctx->dest += l;
    
    if (ctx->ptr_spot == 0 && l >= sizeof(ptr))
        ctx->ptr_spot = (ptr*) c;
}

static void cp_pop(struct _cmt_cpy_ctx* ctx, struct _cmt_cp* lvl) {
    key* k;
    ctx->dest += (long long) ctx->dest & 1;
    ctx->k = k = (key*) ctx->dest;
    
    k->length = k->sub = 0;
    k->offset = lvl->offset;
    
    k->next = *lvl->parent;
    *lvl->parent = ctx->dest - ctx->dest_begin;
    
    ctx->dest += sizeof(key);
}

static struct _cmt_cp cp_push(struct _cmt_cpy_ctx* ctx, key* nxt) {
    struct _cmt_cp l = {
        &ctx->k->sub,
        ctx->k->length + (nxt->offset & 7)
    };
    
    if (ctx->ptr_spot == 0 && CEILBYTE(nxt->length) >= sizeof(ptr) - sizeof(key))
        ctx->ptr_spot = (ptr*) nxt;
        return l;
}

static key* cp_ptr(struct _cmt_cpy_ctx* ctx, page** pg, ptr* p) {
    key* k;
    
    if (p->koffset < 2) {
        ptr* np = (ptr*) ctx->k;
        ctx->dest += sizeof(ptr) - sizeof(key);
        
        np->ptr_id = PTR_ID;
        np->koffset = 1;
        np->pg = p->pg;
        
        k = &EMPTY;
    } else {
        *pg = p->pg;
        k = GOKEY(*pg, p->koffset);
    }
    return k;
}

static void _cmt_cp_worker(struct _cmt_cpy_ctx* ctx, page* pg, key* me, key* nxt, uint offset) {
    struct {
        page* pg;
        key* nxt;
        struct _cmt_cp lvl;
    } mx[32];
    uint idx = 0;
    
    while (1) {
        const uint klen = ((nxt != 0 ? nxt->offset : me->length) >> 3) - offset;
        if (klen != 0)
            cp_dat(ctx, KDATA(me) + offset, klen);
        
        if(nxt == 0) {
            if (idx-- == 0)
                return;
            
            nxt = mx[idx].nxt;
            pg = mx[idx].pg;
            
            cp_pop(ctx, &mx[idx].lvl);
        } else if (me->length != nxt->offset) {
            struct _cmt_cp lvl = cp_push(ctx, nxt);
            
            if ((idx & 0xE0) == 0) {
                mx[idx].nxt = nxt;
                mx[idx].lvl = lvl;
                mx[idx].pg = pg;
                ++idx;
                
                offset = nxt->offset >> 3;
                nxt = nxt->next? GOOFF(pg, nxt->next) : 0;
                continue;
            }
            
            _cmt_cp_worker(ctx, pg, me, nxt, offset);
            
            cp_pop(ctx, &lvl);
        } else if (ISPTR(nxt)) {
            // continue on pointer
        }
        
        me = nxt;
        while (ISPTR(me)) {
            me = cp_ptr(ctx, &pg, (ptr*) me);
        }
        nxt = me->sub? GOOFF(pg, me->sub) : 0;
        offset = 0;
    }
}

static void _cmt_cp(struct _cmt_cpy_ctx* ctx, page* pg, key* me, key* nxt, uint offset) {
    while (1) {
        if (me->length != nxt->offset) {
            ctx->k = 0;
        }
        
        while (ISPTR(nxt)) {
            ptr* p = (ptr*) nxt;
            pg = p->pg;
            nxt = GOKEY(pg, p->koffset);
        }
        
        const int klen = (nxt->length >> 3) - offset;
        if (klen > 0) {
            ctx->k->length += klen * 8;
            memcpy(ctx->dest, KDATA(me) + offset, klen);
            ctx->dest += klen;
        }
        
        if (nxt->next)
            _cmt_cp(ctx, pg, me, GOOFF(pg, nxt->next), offset);
        
        
        nxt = me->sub? GOOFF(pg, me->sub) : 0;
        offset = 0;
    }
}

static page* _cmt_copy_root(task* t, page* root) {
    struct _cmt_cpy_ctx work;
    struct _cmt_cp lvl;
    page* dst = t->ps->new_page(t->psrc_data);
    key* me = GOOFF(root, sizeof(page));
    key* nxt = me->sub ? GOOFF(root, me->sub) : 0;
    ushort dummy = 0;
    
    work.dest_begin = (char*) dst;
    work.dest = work.dest_begin + sizeof(page);
    work.ptr_spot = (ptr*) 1;
    lvl.parent = &dummy;
    lvl.offset = 0;
    
    cp_pop(&work, &lvl);
    
    _cmt_cp_worker(&work, root, me, nxt , 0);
    
    dst->used = work.dest - work.dest_begin;
    return dst;
}
// END COPY

void test_copy(task* t, page* dst, st_ptr src) {
    struct _cmt_cpy_ctx work;
    struct _cmt_cp lvl;
    
    key* me = GOOFF(src.pg, src.key);
    key* nxt = me->sub ? GOOFF(src.pg, me->sub) : 0;
    ushort dummy = 0;
    
    work.dest_begin = (char*) dst;
    work.dest = work.dest_begin + sizeof(page);
    lvl.parent = &dummy;
    lvl.offset = 0;
    
    cp_pop(&work, &lvl);
    
    _cmt_cp_worker(&work, src.pg, me, nxt , 0);
    
    dst->used = work.dest - work.dest_begin;
}

////
struct _cmt_cut {
    page* pg;
    key* me;
    key* nxt;
    ulong offset;
    ulong cut_size;
};

struct _cmt_meas_ctx {
    task* t;
    struct _cmt_cut cut;
    
    uint size;
    ulong target;
    uint pages;
};

struct _cpy_setup {
	page* dest;
    
	ushort o_pt;
	ushort l_pt;
};

static void _tk_compact_copy2(struct _cpy_setup* setup, page* pw, key* parent, ushort* rsub, ushort next, int adjoffset) {
	while (next != 0) {
		key* k = GOOFF(pw,next);
		// trace to end-of-next's
		if (k->next != 0)
			_tk_compact_copy2(setup, pw, parent, rsub, k->next, adjoffset);
        
		// trace a place for a pointer on this page (notice: PTR_ID == MAX-USHORT)
		if (setup->l_pt < k->length) {
			setup->l_pt = k->length;
			setup->o_pt = next;
		}
        
		while (ISPTR(k)) // pointer
		{
			ptr* pt = (ptr*) k;
			if (pt->koffset > 1) {
				pw = (page*) pt->pg;
				k = GOKEY(pw,pt->koffset);
			} else {
				ptr* newptr;
				setup->dest->used += setup->dest->used & 1;
				newptr = (ptr*) ((char*) setup->dest + setup->dest->used);
				newptr->koffset = pt->koffset;
				newptr->ptr_id = PTR_ID;
				newptr->offset = pt->offset + adjoffset;
				newptr->next = *rsub;
				newptr->pg = pt->pg;
				*rsub = setup->dest->used;
				setup->dest->used += sizeof(ptr);
				return;
			}
		}
        
		if (k->length == 0) // empty key? (skip)
			adjoffset += k->offset;
		else if ((parent != 0) && (k->offset + adjoffset == parent->length)) // append to parent key?
        {
			adjoffset = parent->length & 0xFFF8; // 'my' subs are offset by parent-length
            
			memcpy(KDATA(parent) + (parent->length >> 3), KDATA(k), CEILBYTE(k->length));
			parent->length = k->length + adjoffset;
			setup->dest->used += CEILBYTE(k->length);
		} else // key w/data
		{
			setup->dest->used += setup->dest->used & 1;
			parent = (key*) ((char*) setup->dest + setup->dest->used);
			parent->offset = k->offset + adjoffset;
			parent->length = k->length;
			parent->next = *rsub;
			parent->sub = 0;
			memcpy(KDATA(parent), KDATA(k), CEILBYTE(k->length));
			*rsub = setup->dest->used;
			rsub = &parent->sub;
			setup->dest->used += sizeof(key) + CEILBYTE(k->length);
			adjoffset = 0;
		}
		next = k->sub;
	}
}

static key* _tk_create_root_key2(struct _cpy_setup* setup, key* copy, ulong cut_adj) {
	// copy first/root key
	key* root = (key*) ((char*) setup->dest + sizeof(page));
    
	root->offset = root->next = root->sub = 0;
	root->length = copy->length - (cut_adj & 0xFFF8);
    
	memcpy(KDATA(root), KDATA(copy) + (cut_adj >> 3), CEILBYTE(root->length));
    
	setup->dest->used = sizeof(page) + sizeof(key) + CEILBYTE(root->length);
    
	return root;
}

static struct _cpy_setup _cmt_copy(task* t, page* pg, key* me, ushort nxt, int offset) {
    struct _cpy_setup setup;
    key* root;
    
	// start compact-copy
    setup.dest = t->ps->new_page(t->psrc_data);
	setup.o_pt = setup.l_pt = 0;
    
    root = _tk_create_root_key2(&setup, me, offset);
    
	_tk_compact_copy2(&setup, pg, root, &root->sub, nxt ? nxt : me->sub, -(offset & 0xFFF8));
    
    assert(setup.dest->used <= setup.dest->size);
    return setup;
}

static int _cmt_do_copy_page2(task* t, page* pg, key* me, key* nxt, int offset) {
    struct _cpy_setup setup;
    ptr* pt;
    
    ushort* link = (nxt) ? &nxt->next : &me->sub;
    
    setup = _cmt_copy(t, pg, me, nxt->next, offset);
    
    // cut-and-link ...
	me->length = offset;
    
    if (setup.o_pt == 0)
        setup.o_pt = _tk_alloc_ptr(t, TO_TASK_PAGE(pg));
    
    *link = setup.o_pt;
    
    pt = (ptr*) GOOFF(pg, setup.o_pt);
    pt->ptr_id = PTR_ID;
    pt->koffset = 1;	// magic marker: this links to a rebuilded page
    pt->offset = offset;
    pt->pg = setup.dest->id;    // the pointer (to be updated when copied)
    pt->next = 0;
    
    return setup.dest->used - sizeof(page);
}

static int ms_pop2(struct _cmt_meas_ctx* ctx, page* pg, key* me, key* nxt, uint size) {
    uint offset = nxt != 0? nxt->offset + 1 : 0;
    int did_cut = 0;
    
    //printf("pop: me %d over %d size %d\n", _key_ptr(pg, me), _key_ptr(pg,nxt), size);
    
    if (size > ctx->target) {
        const ulong rest = CEILBYTE(me->length - offset);
        const ulong overflow = size - ctx->target;
        
        // more data left than overflow?
        if (rest > overflow) {
            offset += overflow * 8; // reduce size
        } else {
            if(nxt) {
                nxt = GOOFF(pg, nxt->next);
                offset = nxt->offset;
            }
            else {
                nxt = GOOFF(pg, me->sub);
                offset = nxt->offset & 0xFFF8;
            }
            
            //offset = nxt->offset + 1;
        }
        
        size -= _cmt_do_copy_page2(ctx->t, pg, me, nxt, offset);
        
        ctx->size = size;
        ctx->pages++;
        did_cut = 1;
    }
    
    return did_cut;
}

static uint _tk_measure2(struct _cmt_meas_ctx* ctx, page* pw, key* parent, ushort kptr) {
	key* k = GOOFF(pw,kptr);
	uint size = (k->next == 0) ? 0 : _tk_measure2(ctx, pw, parent, k->next);
	k = GOOFF(pw,kptr); //if mem-ptr _ptr_alloc might have changed it
    
	// parent over k->offset
	if (parent != 0){
        if(ms_pop2(ctx, pw, parent, k, size + CEILBYTE(parent->length - k->offset) + sizeof(key) + 1))
            size = ctx->size;
    }
    
	if (ISPTR(k)) {
		ptr* pt = (ptr*) k;
		uint subsize;
		if (pt->koffset > 1)
			subsize = _tk_measure2(ctx, (page*) pt->pg, 0, pt->koffset);
		else
			subsize = sizeof(ptr) + 1;
        
		return size + subsize;
	} else // cut k below limit (length | sub->offset)
	{
		uint subsize = (k->sub == 0) ? 0 : _tk_measure2(ctx, pw, k, k->sub); // + size);
        
        if(ms_pop2(ctx, pw, k, 0, subsize + CEILBYTE(k->length)))
            subsize = ctx->size;
        
		size += subsize;
	}
    
	return size + CEILBYTE(k->length) + sizeof(key) + 1;
}

// BEGIN MEASURE


static int _key_ptr(page* pg, key* k){
    int offset = 0;
    if (k) {
        offset = (int) ((char*) k - (char*) pg);
        
        if (offset < 0 || offset > pg->size) {
            offset = (int)((char*) k - (char*) TO_TASK_PAGE(pg)->ovf);
            offset = 0x8000 | (offset >> 4);
        }
    }
    return offset;
}

static uint _cmt_do_copy_page(task* t, struct _cmt_cut* cut) {
    page* np = t->ps->new_page(t->psrc_data);
    ptr* pt;
    ushort* link;
    struct _cmt_cpy_ctx cpy;
    struct _cmt_cp lvl;
    ushort dummy = 0;
    
    cpy.dest_begin = (char*) np;
    cpy.dest = cpy.dest_begin + sizeof(page);
    cpy.ptr_spot = 0;
    
    link = (cut->nxt) ? &cut->nxt->next : &cut->me->sub;
    
    // create root-key
    lvl.parent = &dummy;
    lvl.offset = 0;
    
    cp_pop(&cpy, &lvl);
    // copy
    _cmt_cp_worker(&cpy, cut->pg, cut->me, (*link) ? GOOFF(cut->pg, *link) : 0, (uint)cut->offset >> 3);
    
    np->used = cpy.dest - cpy.dest_begin;
    assert(np->used <= np->size);
    
    // cut-and-link ...
	cut->me->length = cut->offset;
    
    if (cpy.ptr_spot == 0) {
        const ushort op = _tk_alloc_ptr(t, TO_TASK_PAGE(cut->pg));
        cpy.ptr_spot = (ptr*) GOOFF(cut->pg, op);
        *link = op;
    } else
        *link = _key_ptr(cut->pg, (key*) cpy.ptr_spot);
    
    pt = cpy.ptr_spot;
    pt->ptr_id = PTR_ID;
    pt->koffset = 1;	// magic marker: this links to a rebuilded page
    pt->offset = cut->offset;
    pt->pg = np->id;    // the pointer (to be updated when copied)
    pt->next = 0;
    
    return np->used - sizeof(page);
}

static key* ms_ptr(struct _cmt_meas_ctx* ctx, page** pg, ptr* p) {
    key* k;
    
    if (p->koffset < 2) {
        ctx->size += sizeof(ptr) + 1; //(ctx->size & 1);
        k = &EMPTY;
    } else {
        *pg = p->pg;
        k = GOKEY(*pg, p->koffset);
    }
    return k;
}

static void ms_pop(struct _cmt_meas_ctx* ctx, page* pg, key* me, key* nxt, struct _cmt_cut* cut, ulong size) {
    uint offset = nxt != 0? nxt->offset + 1 : 0;
    ulong subsize = ctx->size - size;
    
    if (subsize > ctx->target) {
        const ulong rest = CEILBYTE(me->length - offset);
        const ulong overflow = subsize - ctx->target;
        
        // more data left than overflow?
        if (rest > overflow) {
            offset += overflow * 8; // reduce size
            subsize -= overflow * 8;
        } else {
            _cmt_do_copy_page(ctx->t, cut);
            ctx->pages++;
            
            ctx->size -= cut->cut_size;
            subsize -= cut->cut_size;
        }
    }
    
    cut = &ctx->cut;
    cut->cut_size = subsize;
    cut->offset = offset;
    cut->nxt = nxt;
    cut->me = me;
    cut->pg = pg;
}

static void _cmt_ms_worker(struct _cmt_meas_ctx* ctx, page* pg, key* me, key* nxt, uint offset) {
    struct {
        page* pg;
        key* nxt;
        key* me;
        struct _cmt_cut cut;
        ulong size;
    } mx[32];
    uint idx = 0;
    
    while (1) {
        if ((idx & 0xE0) == 0) {
            ushort key_nxt;
            mx[idx].size = ctx->size;
            mx[idx].cut = ctx->cut;
            mx[idx].nxt = nxt;
            mx[idx].me = me;
            mx[idx].pg = pg;
            ++idx;
            
            if (nxt) {
                ctx->size += (nxt->offset >> 3) - offset;
                offset = nxt->offset >> 3;
                
                key_nxt = nxt->next;
            } else {
                key_nxt = me->sub;
                offset = 0;
            }
            
            if (key_nxt) {
                nxt = GOOFF(pg, key_nxt);
                continue;
            }
            
            ctx->size += (me->length >> 3) - offset;
        } else
            _cmt_ms_worker(ctx, pg, me, nxt, offset);
        
        do {
            struct _cmt_cut* cut;
            if (idx-- == 0)
                return;
            
            me = mx[idx].nxt;
            pg = mx[idx].pg;
            
            if (me && me->offset != mx[idx].me->length)
                ctx->size += sizeof(key) + 1; //(ctx->size & 1);
            
            // best cut of the 2 sub-branches
            //cut = (ctx->cut.cut_size < mx[idx].cut.cut_size)? &mx[idx].cut : &ctx->cut;
            cut = &ctx->cut;
            
            ms_pop(ctx, pg, mx[idx].me, me, cut, mx[idx].size);
        } while (me == 0);
        
        while (ISPTR(me)) {
            me = ms_ptr(ctx, &pg, (ptr*) me);
        }
        nxt = 0;
    }
}

// END MEASURE
void test_measure(task* t, st_ptr src) {
    struct _cmt_meas_ctx work;
    
    key* me = GOOFF(src.pg, src.key);
    
    work.target = 2000;
    work.size = 0; // sizeof(page) + sizeof(key);
    work.cut.cut_size = 0;
    
    _tk_measure2(&work, src.pg, 0, src.key);
}

static ptr* _cmt_find_ptr(page* cpg, page* find, ushort koff) {
    ushort stack[32];
    uint idx = 0;
    
    while (1) {
        key* k = GOKEY(cpg, koff);
        
        if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
            if (pt->koffset == 0 && pt->pg == find->id)
                return pt;
        } else if (k->sub) {
            if ((idx & 0xE0) == 0) {
                stack[idx++] = k->sub;
            } else {
                ptr* pt = _cmt_find_ptr(cpg, find, k->sub);
                if(pt)
                    return pt;
            }
        }
        
        if ((koff = k->next) == 0) {
            if (idx == 0)
                break;
            koff = stack[--idx];
        }
    }
    return 0;
}

static page* _cmt_mark_and_link(task* t) {
    task_page *end = 0;
    page* root = 0;
    
    while (t->wpages != end) {
        task_page* tp, *first = t->wpages;
        
        for (tp = first; tp != end; tp = tp->next) {
            page* parent = tp->pg.parent;
            page* find = &tp->pg;
            
            if (parent) {
                ptr* pt = _cmt_find_ptr(parent, find, sizeof(page));
                if (pt) {
                    // not yet writable?
                    if (parent == parent->id) {
                        parent = _tk_write_copy(t, parent);
                    }
                    pt->koffset = sizeof(page);
                    pt->pg = find;
                    
                    // fix offset like mem-ptr
                    GOKEY(find,sizeof(page))->offset = pt->offset;
                }
            } else
                root = find;
        }
        
        end = first;
    }
    
    return root;
}

static void _cmt_update_all_linked_pages(const page* pg) {
	uint i = sizeof(page);
	do {
		const key* k = GOKEY(pg, i);
		if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
			((page*) (pt->pg))->parent = pg->id;
			i += sizeof(ptr);
            if ( pt->koffset != 0) {
                pt->koffset = 0;
                // recurse onto rebuild page
                _cmt_update_all_linked_pages(pt->pg);
            }
		} else {
			i += sizeof(key) + CEILBYTE(k->length);
			i += i & 1;
		}
	} while (i < pg->used);
}

/**
 * Rebuild all changes into new root => create new db-version and switch to it.
 *
 * return 0 if ok - also if no changes was written.
 */
int cmt_commit_taskq(task* t) {
    int stat = 0;
	page* root = _cmt_mark_and_link(t);
    
	// rebuild from root => new root
	if (root) {
        page* new_root;
        struct _cmt_meas_ctx work;
        st_ptr ptr;
        
        work.target = root->size - sizeof(page) - sizeof(key);
        work.size = work.cut.cut_size = work.pages = 0;
        work.t = t;
        
		// start measure from root
        _tk_measure2(&work, root, 0, sizeof(page));
        //_cmt_ms_worker2(&work, root, GOOFF(root, sizeof(page)),0);
        //_cmt_ms_worker(&work, root, GOOFF(root, sizeof(page)),0 ,0);
        
        ptr.offset = 0;
        ptr.key = sizeof(page);
        ptr.pg = root;
        //st_prt_page(&ptr);
        
		// copy "rest" to new root
        new_root = _cmt_copy(t, root, GOKEY(root, sizeof(page)), 0, 0).dest;
        
        // link old pages to new root
        _cmt_update_all_linked_pages(new_root);
        
        stat = t->ps->pager_commit(t->psrc_data, new_root);
	}
    
	tk_drop_task(t);
    
	return stat;
}

/// OLD



/////////////////////////////// Commit v4 /////////////////////////////////

static uint _n_cut(struct _tk_setup* setup, page* pw, key* copy, key* prev, int cut_bid){
    struct _cmt_cut cut;
    cut.pg = pw;
    cut.me = copy;
    cut.nxt = prev;
    cut.offset = _tk_adjust_cut(pw, copy, prev, cut_bid);
    
    _cmt_do_copy_page(setup->t, &cut);
    
    return (sizeof(ptr) * 8);
}


static uint _tk_measure(struct _tk_setup* setup, page* pw, key* parent, ushort kptr) {
	key* k = GOOFF(pw,kptr);
	uint size = (k->next == 0) ? 0 : _tk_measure(setup, pw, parent, k->next);
	k = GOOFF(pw,kptr); //if mem-ptr _ptr_alloc might have changed it
    
	// parent over k->offset
	if (parent != 0)
		while (1) {
			int cut_offset = size + parent->length - k->offset + ((sizeof(key) + 1) * 8) - setup->halfsize;
			if (cut_offset <= 0) // upper-cut
				break;
            
			size = _tk_cut_key(setup, pw, parent, k, cut_offset + k->offset);
		}
    
	if (ISPTR(k)) {
		ptr* pt = (ptr*) k;
		uint subsize;
		if (pt->koffset != 0)
			subsize = _tk_measure(setup, (page*) pt->pg, 0, pt->koffset);
		else
			subsize = (sizeof(ptr) * 8);
        
		return size + subsize;
	} else // cut k below limit (length | sub->offset)
	{
		uint subsize = (k->sub == 0) ? 0 : _tk_measure(setup, pw, k, k->sub); // + size);
		const uint target_size = setup->halfsize; //(sizeof(key) * 8) - (k->length > setup->halfsize ? setup->fullsize : setup->halfsize);
        
		while (1) {
			int cut_offset = subsize + k->length - target_size;
			//			int cut_offset = subsize + k->length + (sizeof(key)*8) - setup->halfsize;
			if (cut_offset < 0)
				break;
            
			subsize = _tk_cut_key(setup, pw, k, 0, cut_offset); // + size;
		}
        
		size += subsize;
		//		size = subsize;
	}
    
	return size + k->length + ((sizeof(key) + 1) * 8);
}

void test_measures(task* t, st_ptr src) {
	struct _tk_setup setup;
    
    setup.halfsize = 2000;
    setup.fullsize = 4000;
    
    _tk_measure(&setup, src.pg, 0, src.key);
}

static uint _tk_to_mem_ptr(task* t, page* pw, page* to, ushort k) {
	while (k != 0) {
		key* kp = GOOFF(pw,k);
        
		if (ISPTR(kp)) {
			ptr* pt = (ptr*) kp;
            
			if (pt->koffset == 0 && pt->pg == to->id) {
				// is it writable? Make sure
				pw = _tk_write_copy(t, pw);
				pt = (ptr*) GOOFF(pw,k);
                
				pt->koffset = sizeof(page);
				pt->pg = to;
                
				// fix offset like mem-ptr
				GOKEY(to,sizeof(page)) ->offset = pt->offset;
                
				// force rebuild
				pw->waste = pw->size;
				return 1;
			}
		} else if (_tk_to_mem_ptr(t, pw, to, kp->sub))
			return 1;
        
		k = kp->next;
	}
    
	return 0;
}

static uint _tk_link_written_pages(task* t, task_page* pgw) {
	uint max_size = 0;
    
	while (pgw != 0) {
		page* pg = &pgw->pg;
		if (pg->size > max_size)
			max_size = pg->size;
        
		if (pg->parent != 0 && (pgw->ovf != 0 || pg->waste > pg->size / 2)) {
			page* parent = _tk_check_page(t, (page*) pg->parent->id);
            
			// make mem-ptr from parent -> pg (if not found - link was deleted, then just dont build it)
			_tk_to_mem_ptr(t, parent, pg, sizeof(page));
            
			// this page will be rebuild from parent (or dont need to after all)
			pgw->refcount = 0;
			//t->ps->remove_page(t->psrc_data, pg->id);
		}
        
		pgw = pgw->next;
	}
    
	return max_size;
}

uint just_write = 0;
uint rebuild = 0;

int cmt_commit_taskz(task* t) {
	struct _tk_setup setup;
	task_page* pgw;
	int ret = 0;
    
	uint max_size = _tk_link_written_pages(t, t->wpages);
    
	setup.t = t;
    
	for (pgw = t->wpages; pgw != 0 && ret == 0; pgw = pgw->next) {
		page* pg;
        
		if (pgw->refcount == 0)
			continue;
        
		/* overflowed or cluttered? */
		pg = &pgw->pg;
		if (pgw->ovf || pg->waste > pg->size / 2) {
			char bf[max_size];
			ushort sub = 0;
            
			//setup.dest = (page*) bf;
            
            
			setup.fullsize = (uint) (pg->size - sizeof(page)) << 3;
			setup.halfsize = setup.fullsize >> 1;
			setup.fullsize -= setup.halfsize >> 1;
            
			_tk_measure(&setup, pg, 0, sizeof(page));
            
			// reset and copy remaining rootpage
            setup.dest = (page*) bf;
            
			setup.dest->parent = 0;
			setup.dest->waste = 0;
			setup.dest->size = pg->size;
			setup.dest->used = sizeof(page);
			setup.dest->id = pg->id;
            
			_tk_compact_copy(&setup, pg, 0, &sub, sizeof(page), 0);
            
			t->ps->write_page(t->psrc_data, setup.dest->id, setup.dest);
			rebuild++;
		} else {
			t->ps->write_page(t->psrc_data, pg->id, pg);
			just_write++;
		}
        
		ret = t->ps->pager_error(t->psrc_data);
	}
    
    /*
     for (pgw = t->wpages; pgw != 0; pgw = pgw->next) {
     if (pgw->refcount == 0) {
     page* pg = &pgw->pg;
     t->ps->remove_page(t->psrc_data, pg->id);
     }
     }
     
     if (ret != 0)
     t->ps->pager_rollback(t->psrc_data);
     else
     ret = t->ps->pager_commit(t->psrc_data);
     */
	tk_drop_task(t);
    
	return ret;
}


