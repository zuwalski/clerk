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


int cont_fixed = 0;
int cont_notfixed = 0;
int max_notfixed = 0;
int copy_calls = 0;

struct _tk_setup {
	page* dest;
	task* t;
    
	uint halfsize;
    
	ushort o_pt;
};

static void _tk_copy_continue_key(page* dest, const key* parent, const key* k) {
    int curr_key = (int)((uchar*) parent - (uchar*) dest);
    int next_key = curr_key + sizeof(key) + CEILBYTE(parent->length);
    int offset = curr_key + sizeof(key) + CEILBYTE((parent->length & 0xFFF8) + k->length);
    next_key += next_key & 1;
    offset += offset & 1;
    offset -= next_key;
    
    // re-number kptr's from parent and on
    do {
        key* j = GOKEY(dest, curr_key);
        
        if (j->next) {
            j->next += offset;
        }
        
        if (ISPTR(j)) {
            curr_key += sizeof(ptr);
        } else {
            if (j->sub) {
                j->sub += offset;
            }
            curr_key += sizeof(key) + CEILBYTE(j->length);
            curr_key += curr_key & 1;
        }
    } while (curr_key < dest->used);
    
    // make room
    memmove(dest + next_key + offset, dest + next_key, dest->used - next_key);
    
    // extend parent
    memcpy(KDATA(parent) + (parent->length >> 3), KDATA(k), CEILBYTE(k->length));
    
    dest->used += offset;
}

static void _tk_compact_copy(page* dest, page* pw, key* parent, ushort* link, ushort next, int adjoffset) {
	while (next) {
		key* k = GOOFF(pw, next);
        
		if (ISPTR(k)) // pointer
		{
			ptr* pt = (ptr*) k;

			if (pt->koffset > 1) {
				pw = (page*) pt->pg;
				k = GOKEY(pw,pt->koffset);
                k->offset = pt->offset;
			} else {
				ptr* newptr = (ptr*) ((char*) dest + dest->used);
                *link = dest->used;
                link = &newptr->next;
				dest->used += sizeof(ptr);
                
                memcpy(newptr, pt, sizeof(ptr));
				newptr->offset += adjoffset;
				newptr->next = 0;

                next = k->next;
				continue;
			}
		}
        
		if (k->length == 0) { // empty key? (skip)
            if (k->sub) {
                key* s = GOOFF(pw, k->sub);
                s->offset = k->offset + adjoffset;
                s->next = k->next;
            }

            next = k->sub? k->sub : k->next;
        }
		else if (k->offset + adjoffset == parent->length) // append to parent key?
        {
			adjoffset = parent->length & 0xFFF8; // 'my' subs are offset by floor(parent-length)
            
            _tk_copy_continue_key(dest, parent, k);

            parent->length = adjoffset + k->length;
            
            // swith to sub's
            next = k->sub;
		}
        else // key w/data
		{
            key* target = (key*) ((char*) dest + dest->used);
            const ushort pl = parent->length;
            ushort t = *link = dest->used;
            
            link = &target->next;
			dest->used += sizeof(key) + CEILBYTE(k->length);
			dest->used += dest->used & 1;

			target->offset = k->offset + adjoffset;
			target->length = k->length;
			target->next = 0;
			target->sub = 0;
			memcpy(KDATA(target), KDATA(k), CEILBYTE(k->length));

            _tk_compact_copy(dest, pw, parent, link, k->next, adjoffset);
            
            // target might have moved. Re-position if parent-length changed
            if (parent->length != pl) {
                t += CEILBYTE(parent->length) - CEILBYTE(pl);
                t += t & 1;
                target = GOKEY(dest, t);
            }
            
            link = &target->sub;
            parent = target;
            adjoffset = 0;
            next = k->sub;
		}
	}
}

static void _tk_root_copy(page* dest, page* pw, key* copy, ushort next, int cut_adj) {
    // copy root-key
	key* root = (key*) ((char*) dest + sizeof(page));
    
	root->offset = root->next = root->sub = 0;
	root->length = copy->length - (cut_adj & 0xFFF8);
    
    memcpy(KDATA(root), KDATA(copy) + (cut_adj >> 3), CEILBYTE(root->length));
    
    dest->used = sizeof(page) + sizeof(key) + CEILBYTE(root->length);
    dest->used += dest->used & 1;
    
    _tk_compact_copy(dest, pw, root, &root->sub, next, -(cut_adj & 0xFFF8));
}

// ----------

struct copy_point {
    struct copy_point* ext;
    key* k;
    uint sub;
};

struct _trace {
    uint idx;
    struct copy_point cps[300];
};

static void _trace_page(struct _trace* t, page* cpg, ushort koff, ushort plength, struct copy_point** ext) {
    while (koff) {
        struct copy_point* cp = t->cps + t->idx++;
        key* k = cp->k = GOOFF(cpg, koff);
        
        cp->ext = 0;
        cp->sub = 0;
        koff = k->next;

        if (ISPTR(k)) {
            const ptr* pt = (ptr*) k;
            
            if (pt->koffset > 1) {
                cpg = pt->pg;
                koff = pt->koffset;
                
                k = cp->k = GOOFF(cpg, koff);
                k->offset = pt->offset;
                koff = k->next;
                //GOKEY(cpg, koff)->offset = pt->offset;
                
                //t->idx--;
            } else continue;
            
        }
        
        //else
        {
            if (k->offset == plength) {
                *ext = cp;
            }

            _trace_page(t, cpg, koff, plength, ext);

            if (k->sub) {
                cp->sub = t->idx;
            }
            
            plength = k->length;
            ext = &cp->ext;
            koff = k->sub;
        }
    }
}

static key* _trace_nxt_target(page* dest, ushort* const link) {
    key* target = GOKEY(dest, dest->used);

    dest->used = (target->length)? dest->waste : dest->used;
    
    *link = dest->used;
    
    return GOKEY(dest, dest->used);
}

static ushort* _trace_copy_ptr(page* dest, const ptr* p, ushort* link) {
    ptr* target = (ptr*) _trace_nxt_target(dest, link);
    
    dest->waste = dest->used + sizeof(ptr);
    
    target->ptr_id = PTR_ID;
    target->next = 0;
    target->koffset = p->koffset;
    target->offset = p->offset;
    target->pg = p->pg;
    
    return &target->next;
}

static ushort* _copy_key_from_trace(page* dest, struct copy_point* base, struct copy_point* cp, ushort* link) {
    key* target = _trace_nxt_target(dest, link);
    
    target->next = target->length = 0;
    target->offset = cp->k->offset;
    target->sub = cp->sub;

    do {
        struct copy_point* cx;
        const uint offset = target->length & 0xFFF8;
        
        memcpy(KDATA(target) + (target->length >> 3), KDATA(cp->k), CEILBYTE(cp->k->length));
        
        target->length = offset + cp->k->length;

        cp->k = 0;
        cx = base + cp->sub;
        cp = cp->ext;
        for (; cx < cp; cx++) {
//            cx->k->offset += offset;
        }
    } while (cp);
    
    dest->waste = dest->used + sizeof(key) + CEILBYTE(target->length);
    dest->waste += dest->waste & 1;
    
    if (target->length == 0) {
        *link = 0;
    } else {
        link = &target->next;
    }
    
    return link;
}

static void _trace_copy_list(page* dest, struct copy_point* base, struct copy_point* cp, ushort* link) {
    *link = 0;
    while (cp != base) {
        if (cp->k == 0) {
            cp = base + cp->sub;
        } else {
            const ushort next = cp->k->next;
            
            if (ISPTR(cp->k)) {
                link = _trace_copy_ptr(dest, cp->k, link);
            } else {
                link = _copy_key_from_trace(dest, base, cp, link);
            }
            
            if (next == 0) {
                break;
            }
            
            cp++;
        }
    }
}

static uint _call_trace_page(page* dest, page* cpg, key* root, ushort kptr, ushort cut_at) {
    ushort* link;
    struct _trace t;
    ushort tlen;
    t.idx = 1;
    t.cps->sub = kptr? 1 : 0;
    t.cps->ext = 0;
    t.cps->k = (key*) ((char*) root + (cut_at >> 3));
    tlen = t.cps->k->length;
    t.cps->k->length = root->length;
    
    _trace_page(&t, cpg, kptr, root->length, &t.cps->ext);
    
    memset((char*)dest + sizeof(page), 0, sizeof(key));
    // copy root
    dest->used = sizeof(page);
    _copy_key_from_trace(dest, t.cps, t.cps, &kptr);
    
    link = &kptr;
    *link = 0;
    
    for (struct copy_point* cp = t.cps; cp < t.cps + t.idx; cp++) {
        if (cp->k) {
            //            const ushort next = cp->k->next;
            
            if (ISPTR(cp->k)) {
                link = _trace_copy_ptr(dest, cp->k, link);
            } else {
                link = _copy_key_from_trace(dest, t.cps, cp, link);
            }
        }
    }
    
    /*
    kptr = sizeof(page);
    // copy rest
	do {
		key* target = GOKEY(dest, kptr);
        
		if (!ISPTR(target)) {
            _trace_copy_list(dest, t.cps, t.cps + target->sub, &target->sub);
            
			kptr += sizeof(key) + CEILBYTE(target->length);
            kptr += kptr & 1;
		} else {
			kptr += sizeof(ptr);
		}
	} while (kptr < dest->waste);
     */
    
    dest->used = dest->waste;
    t.cps->k = (key*) ((char*) root + (cut_at >> 3));
    t.cps->k->length = tlen;
    return t.idx;
}

// ---------------------------

struct _copy_redirect {
    int next;
    int o_ptr;
    page* pg[129];
    ushort k[129];
};

static key* _nxt_target(page* dest, ushort* const link) {
    dest->used += dest->used & 1;
    
    *link = dest->used;
    
    return GOKEY(dest, dest->used);
}

static ushort* _copy_ptr(page* dest, const ptr* p, ushort* link) {
    key* target = _nxt_target(dest, link);
    dest->used += sizeof(ptr);
    
    memcpy(target, p, sizeof(ptr));
    
    target->next = 0;
    return &target->next;
}

static ushort* _copy_key(struct _copy_redirect* redirect, page* dest, page* pg, key* k, ushort* const link, ushort kptr, ushort adjoff) {
    key* target = _nxt_target(dest, link);
    
    assert(!ISPTR(k));

    memset(target, 0, sizeof(key));

    target->offset = k->offset;
    
    if (kptr) {
        int rd = ++redirect->next;
        
        redirect->next &= 0x7F;
        redirect->k[rd] = kptr;
        redirect->pg[rd] = pg;
        
        target->sub = rd;
    }
    
    dest->used += sizeof(key);
    
    while(1) {
        const int len = k->length - adjoff;
        
        if (len > 0) {
            const int offset = target->length & 0xFFF8;

            if (target->length != offset) {
                target->length  = offset;
                dest->used--;
            }
            
            memcpy((char*) dest + dest->used, KDATA(k) + (adjoff >> 3), CEILBYTE(len));
            dest->used += CEILBYTE(len);
            target->length += len;

            if (kptr == 0) {
                break;
            }
            
            do {
                k = GOOFF(pg, kptr);
                k->offset += offset - adjoff;
            } while ((kptr = k->next));
            
            if (k->offset != target->length) {
                break;
            }
        } else if (kptr) {
            ushort parent_offset = k->offset;
            k = GOOFF(pg, kptr);
            k->offset = parent_offset;
        } else
            break;
        
        if (ISPTR(k)) {
            ptr* p = (ptr*) k;
            
            if (p->koffset < 2) {
                // end-of-key ptr
                if (target->length == 0) {
                    // "take-over" empty target
                    ptr* pt = (ptr*) target;
                    
                    pt->ptr_id = PTR_ID;
                    pt->koffset = p->koffset;
                    pt->pg = p->pg;
                    
                    // retire ptr
                    p->koffset = 0;
                    p->offset = PTR_ID;
                    
                    dest->used += sizeof(ptr) - sizeof(key);
                }
                break;
            }
            
            pg = p->pg;
            k = GOKEY(pg, p->koffset);
            k->offset = p->offset;
            p->offset = PTR_ID;
        } else {
            k->offset = PTR_ID;
        }
        
        kptr = k->sub;
        adjoff = 0;
    }
    
    // undo empty target
    if (target->length == 0) {
        dest->used -= sizeof(key);
        *link = 0;
        return link;
    }
    
    return &target->next;
}

static void _copy_key_list(struct _copy_redirect* redirect, page* dest, ushort* link, ushort kptr) {
    page* pg = redirect->pg[kptr];
    int toffset = -1;
    kptr = redirect->k[kptr];
    
    *link = 0;
    while (kptr) {
        key* k = GOOFF(pg, kptr);
        
        assert(k->offset > toffset);
        
        if (redirect->o_ptr == 0 && k->length >= sizeof(void*) * 8) {
            redirect->o_ptr = kptr;
        }
        
        if (k->offset != PTR_ID) {
            kptr = k->next;
            toffset = k->offset;
            
            if (!ISPTR(k)) {
                link = _copy_key(redirect, dest, pg, k, link, k->sub, 0);
            } else {
                ptr* p = (ptr*) k;
                
                if (p->koffset > 1) {
                    k = GOKEY(p->pg, p->koffset);
                    k->offset = p->offset;
                    link = _copy_key(redirect, dest, p->pg, k, link, k->sub, 0);
                } else {
                    link = _copy_ptr(dest, p, link);
                }
            }
        } else {
            // handle "skip-key"
            if (ISPTR(k)) {
                ptr* p = (ptr*) k;
                
                if (p->koffset == 0) {
                    break;
                }

                pg = p->pg;
                k = GOKEY(pg, p->koffset);
            }
            
            kptr = k->sub;
        }
    }
}

static ushort _copy_page_n(page* dest, page* pg, key* k, ushort nxt, ushort adjoff) {
    struct _copy_redirect redirect;
    key* target = GOKEY(dest, sizeof(page));
    ushort kptr;
    
    redirect.o_ptr = 0;
    redirect.next  = 0;
    redirect.pg[0] = 0;
    redirect.k[0]  = 0;
    
    //_call_trace_page(dest, pg, k, nxt, adjoff);
    
    //_tk_root_copy(dest, pg, k, nxt, adjoff);
    
    // copy root-key
    dest->used = sizeof(page);
    _copy_key(&redirect, dest, pg, k, &kptr, nxt, adjoff & 0xFFF8);

    target->offset = 0;
    
	do {
		target = GOKEY(dest, kptr);
        
		if (!ISPTR(target)) {
            _copy_key_list(&redirect, dest, &target->sub, target->sub);

			kptr += sizeof(key) + CEILBYTE(target->length);
            kptr += kptr & 1;
		} else {
			kptr += sizeof(ptr);
		}
	} while (kptr < dest->used);
    
    return redirect.o_ptr;
}


// -----------

void test_copy(task* t, page* dst, st_ptr src) {
    key* copy = GOOFF(src.pg, src.key);
    
    //_call_trace_page(dst, src.pg, copy, copy->sub, src.offset);
    //_copy_page_n(dst, src.pg, copy, copy->sub, src.offset);
    
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
}

void test_measure(task* t, st_ptr src) {
}

// --------

static ushort _tk_link_and_create_page(struct _tk_setup* setup, page* pw, int ptr_offset) {
	ptr* pt;
    
	// no large enough exsisting key?
	if (setup->o_pt == 0) {
		setup->o_pt = _tk_alloc_ptr(setup->t, TO_TASK_PAGE(pw) ); // might change ptr-address!
	}
    
	// create a link to new page
    pt = (ptr*) GOOFF(pw,setup->o_pt);
	pt->offset = ptr_offset;
	pt->ptr_id = PTR_ID;
	pt->koffset = 1; // magic marker
    pt->next = 0;
    pt->pg = setup->dest->id;
    
	return setup->o_pt;
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

static uint _tk_cut_key(struct _tk_setup* setup, page* pw, key* copy, key* prev, int cut_bid) {
    setup->dest = setup->t->ps->new_page(setup->t->psrc_data);

	int cut_adj = _tk_adjust_cut(pw, copy, prev, cut_bid);
	
    ushort* link = (prev != 0) ? &prev->next : &copy->sub;
    
	// start compact-copy
    _tk_root_copy(setup->dest, pw, copy, *link, cut_adj);
    setup->o_pt = 0; //_copy_page_n(setup->dest, pw, copy, *link, cut_adj);
    
    assert(setup->dest->used <= setup->dest->size);
    
	// cut-off 'copy'
	copy->length = cut_adj;
    
	// link ext-pointer to new page
    *link = _tk_link_and_create_page(setup, pw, cut_adj);
    
	return (sizeof(ptr) * 8);
}

#define KEYBITS ((sizeof(key) + 1) * 8)

static uint _tk_measure(struct _tk_setup* setup, page* pw, key* parent, ushort kptr) {
    if (kptr) {
        key* k = GOOFF(pw,kptr);
        uint size = _tk_measure(setup, pw, parent, k->next);
        k = GOOFF(pw,kptr); //if mem-ptr _ptr_alloc might have changed it
        
        // parent over k->offset
        if (parent != 0 && (size + (parent->length - k->offset) + KEYBITS > setup->halfsize)) {
            size = _tk_cut_key(setup, pw, parent, k, k->offset + 1);
        }
        
        if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
            const uint subsize = (pt->koffset > 1)? _tk_measure(setup, (page*) pt->pg, 0, pt->koffset) : (sizeof(ptr) * 8);
            
            return size + subsize;
        } else {
            // cut k below limit (length | sub->offset)
            uint subsize = _tk_measure(setup, pw, k, k->sub);
            
            if (subsize + k->length + KEYBITS > setup->halfsize) {
                subsize = _tk_cut_key(setup, pw, k, 0, 0);
            }
            
            return size + subsize + k->length + KEYBITS;
        }
    }
    return 0;
}

static ushort _cmt_find_ptr(page* cpg, page* find, ushort koff) {
    ushort stack[32];
    uint idx = 0;
    
    while (1) {
        key* k = GOKEY(cpg, koff);
        
        if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
            if (pt->koffset == 0 && pt->pg == find->id)
                return koff;
        } else if (k->sub) {
            if ((idx & 0xE0) == 0) {
                stack[idx++] = k->sub;
            } else {
                koff = _cmt_find_ptr(cpg, find, k->sub);
                if(koff)
                    return koff;
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
                ushort pt_off = _cmt_find_ptr(parent, find, sizeof(page));
                if (pt_off) {
                    ptr* pt;
                    // not yet writable?
                    if (parent == parent->id)
                        parent = _tk_write_copy(t, parent);
                    
                    pt = (ptr*) GOOFF(parent, pt_off);
                    pt->koffset = sizeof(page);
                    pt->pg = find;
                    
                    // fix offset like mem-ptr
                    //GOKEY(find,sizeof(page))->offset = pt->offset;
                }
            } else
                root = find;
        }
        
        end = first;
    }
    
    for (end = t->wpages; end; end = end->next) {
        t->ps->remove_page(t->psrc_data, end->pg.id);
    }
    
    return root;
}

static void _cmt_update_all_linked_pages(struct _tk_setup* setup, page* pg) {
	uint i = sizeof(page);
    
	do {
		const key* k = GOKEY(pg, i);
        
		if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
            if (pt->koffset == 1) {
                page* nxt_pg = pt->pg;
                
                pt->koffset = 0;
                nxt_pg->parent = pg;
                
                // recurse onto rebuild page
                _cmt_update_all_linked_pages(setup, nxt_pg);
            } else {
                ((page*) (pt->pg))->parent = pg->id;
            }

			i += sizeof(ptr);
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
int cmt_commit_task(task* t) {
    int stat = 0;
	page* root = _cmt_mark_and_link(t);
    
	// rebuild from root => new root
	if (root) {
        struct _tk_setup setup;
        key* k;
        
        setup.t = t;
        setup.dest = 0;
        setup.halfsize = (uint) (root->size - sizeof(page)) << 2;   // in bits
        
        _tk_measure(&setup, root, 0, sizeof(page));
        
        // reset and copy remaining rootpage
        setup.dest = t->ps->new_page(t->psrc_data);

        k = GOKEY(root, sizeof(page));
        _copy_page_n(setup.dest, root, k, k->sub, 0);
        
        // avoid ptr-only-root-page problem
        k = GOKEY(setup.dest, sizeof(page));
        if (ISPTR(k)) {
            ptr* pt = (ptr*) k;
            t->ps->remove_page(t->psrc_data, setup.dest);
            setup.dest = pt->pg;
        }

        // link old pages to new root
        _cmt_update_all_linked_pages(&setup, setup.dest);
        
        // swap root
        stat = t->ps->pager_commit(t->psrc_data, setup.dest);
	}
    
	tk_drop_task(t);
    
	return stat;
}
