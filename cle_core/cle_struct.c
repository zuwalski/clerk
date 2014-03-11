/* 
 Clerk application and storage engine.
 Copyright (C) 2008-2013  Lars Szuwalski

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
#include <string.h>

#include "cle_struct.h"

struct _st_lkup_res {
	task* t;
	page* pg;
	key* prev;
	key* sub;
	page* d_pg;
	key* d_prev;
	key* d_sub;
	cdat path;
	uint length;
	uint diff;
};

static uint _st_lookup(struct _st_lkup_res* rt) {
	key* me = rt->sub;
	cdat ckey = KDATA(me) + (rt->diff >> 3);
	uint max = me->length - rt->diff;

	while (1) {
		cdat to, curr = rt->path;
		uint i, d = 0;

		if (rt->length < max)
			max = rt->length;

		to = curr + ((max + 7) >> 3);

		while (curr < to && (d = *curr ^ *ckey++) == 0)
			curr++;

		i = (uint) ((curr - rt->path) << 3);
		if (i > max) {
			i -= 8;
			curr--;
		}
		rt->path = curr;

		// fold 1's after msb
		d |= (d >> 1);
		d |= (d >> 2);
		d |= (d >> 4);
		// lzc(a)
		d -= ((d >> 1) & 0x55);
		d = (((d >> 2) & 0x33) + (d & 0x33));
		d = (((d >> 4) + d) & 0x0f);

		d = i + 8 - d;

		rt->diff += d < max ? d : max;
		rt->sub = me;
		rt->prev = 0;
		rt->length -= i;

		if (rt->length == 0 || me->sub == 0)
			break;

		me = GOOFF(rt->pg,me->sub);
		while (me->offset < rt->diff) {
			rt->prev = me;

			if (me->next == 0)
				break;

			me = GOOFF(rt->pg,me->next);
		}

		if (me->offset != rt->diff)
			break;

		// for st_delete
		if (rt->sub->length != me->offset || (rt->d_sub == 0 && rt->sub->length != 0)) {
			rt->d_pg = rt->pg;
			rt->d_sub = rt->sub;
			rt->d_prev = rt->prev;
		}

		if (ISPTR(me))
			me = _tk_get_ptr(rt->t, &rt->pg, me);
		ckey = KDATA(me);
		max = me->length;
		rt->diff = 0;
	}
	return rt->length;
}

static uint _prev_offset(task_page* pg, key* prev) {
	return (pg->ovf != 0 && (char*) prev > (char*) pg->ovf && (char*) prev < (char*) pg->ovf + pg->ovf->size) ?
			(uint) ((char*) prev - (char*) pg->ovf) : 0;
}

static ptr* _st_page_overflow(struct _st_lkup_res* rt, uint size) {
	task_page* tp = TO_TASK_PAGE(rt->pg);
	ptr* pt;

	// rt->prev might move in _tk_alloc_ptr
	uint prev_offset = _prev_offset(tp, rt->prev);

	// allocate pointer
	ushort ptr_off = _tk_alloc_ptr(rt->t, tp);

	/* rebuild prev-pointer in (possibly) new ovf */
	if (prev_offset)
		rt->prev = (key*) ((char*) tp->ovf + prev_offset);

	// +1 make sure new data can be aligned as well
	if (size + rt->t->stack->pg.used + (rt->t->stack->pg.used & 1) > rt->t->stack->pg.size)
		_tk_stack_new(rt->t);

	// init mem-pointer
	pt = (ptr*) GOOFF(rt->pg,ptr_off);

	pt->pg = &rt->t->stack->pg;
	pt->koffset = rt->t->stack->pg.used + (rt->t->stack->pg.used & 1);
	pt->offset = rt->diff;
	pt->ptr_id = PTR_ID;

	if (rt->prev) {
		pt->next = rt->prev->next;
		rt->prev->next = ptr_off;
	} else /* sub MUST be there */
	{
		pt->next = rt->sub->sub;
		rt->sub->sub = ptr_off;
	}

	/* reset values */
	rt->pg = &rt->t->stack->pg;
	rt->prev = rt->sub = 0;

	return pt;
}

/* make a writable copy of external pages before write */
static void _st_make_writable(struct _st_lkup_res* rt) {
	page* old = rt->pg;

	rt->pg = _tk_write_copy(rt->t, rt->pg);
	if (rt->pg == old)
		return;

	/* fix pointers */
	if (rt->prev)
		rt->prev = GOKEY(rt->pg,(char*)rt->prev - (char*)old);

	if (rt->sub)
		rt->sub = GOKEY(rt->pg,(char*)rt->sub - (char*)old);
}

#define IS_LAST_KEY(k,pag) ((char*)(k) + ((k)->length >> 3) + sizeof(key) + 3 - (char*)(pag) > (pag)->used)

static void _st_write(struct _st_lkup_res* rt) {
	key* newkey;
	uint size = rt->length >> 3;

	_st_make_writable(rt);

	/* continue/append (last)key? */
	if ((rt->sub->length == 0 || rt->diff == rt->sub->length) && IS_LAST_KEY(rt->sub,rt->pg)) {
		uint length = (rt->pg->used + size > rt->pg->size) ? rt->pg->size - rt->pg->used : size;

		memcpy(KDATA(rt->sub) + (rt->diff >> 3), rt->path, length);
		rt->sub->length = ((rt->diff >> 3) + length) << 3;
		rt->pg->used = ((char*) KDATA(rt->sub) + ((uint) rt->sub->length >> 3)) - (char*) (rt->pg);

		rt->diff = rt->sub->length;
		size -= length;
		if (size == 0)	// no remaining data?
			return;

		rt->length = size << 3;
		rt->path += length;
	}

	do {
		// -1 make sure it can be aligned
		uint room = rt->pg->size - rt->pg->used - (rt->pg->used & 1);
		uint length = rt->length;
		uint pgsize = size;
		ushort nkoff;

		pgsize += sizeof(key);

		if (pgsize > room) /* page-overflow */
		{
			if (room > sizeof(key) + 2) /* room for any data at all? */
				pgsize = room; /* as much as we can */
			else {
				_st_page_overflow(rt, pgsize); /* we need a new page */

				room = rt->pg->size - rt->pg->used;
				pgsize = room > pgsize ? pgsize : room;
			}

			length = (uint) (pgsize - sizeof(key)) << 3;
			rt->length -= length;
		}

		nkoff = rt->pg->used;
		nkoff += nkoff & 1; /* short aligned */
		rt->pg->used = nkoff + pgsize;
		newkey = GOKEY(rt->pg,nkoff);

		newkey->offset = rt->diff;
		newkey->length = length;
		newkey->sub = newkey->next = 0;

		if (rt->prev) {
			newkey->next = rt->prev->next;
			rt->prev->next = nkoff;
		} else if (rt->sub) {
			newkey->next = rt->sub->sub;
			rt->sub->sub = nkoff;
		}

		memcpy(KDATA(newkey), rt->path, length >> 3);

		rt->prev = 0;
		rt->sub = newkey;
		rt->diff = length;
		rt->path += length >> 3;
		size -= pgsize - sizeof(key);
	} while (size);
}

static void _pt_move(st_ptr* pt, struct _st_lkup_res* rt) {
	pt->pg = rt->pg;
	pt->key = (char*) rt->sub - (char*) rt->pg;
	pt->offset = rt->diff;
}

static struct _st_lkup_res _init_res(task* t, st_ptr* pt, cdat path, uint length) {
	struct _st_lkup_res rt;
	rt.t = t;
	rt.path = path;
	rt.length = length << 3;
	rt.pg = _tk_check_ptr(t, pt);
	rt.sub = GOOFF(rt.pg,pt->key);
	rt.prev = 0;
	rt.diff = pt->offset;
	rt.d_sub = 0;
	return rt;
}

/* Interface-functions */

uint st_empty(task* t, st_ptr* pt) {
	key* nk = tk_alloc(t, sizeof(key) + 2, &pt->pg);	// dont use alloc (8-byte alignes) and waste 2 bytes here

	pt->key = (char*) nk - (char*) pt->pg;
	pt->offset = 0;

	memset(nk, 0, sizeof(key));
	return 0;
}

uint st_is_empty(task* t, st_ptr* pt) {
	key* k;
	ushort offset;
	if (pt == 0 || pt->pg == 0)
		return 1;
	_tk_check_ptr(t, pt);
	k = GOOFF(pt->pg,pt->key);
	offset = pt->offset;
	while (1) {
		if (offset < k->length)
			return 0;
		if (k->sub == 0)
			return 1;
		k = GOOFF(pt->pg,k->sub);
		offset = 0;
	}
}

uint st_exist(task* t, st_ptr* pt, cdat path, uint length) {
	struct _st_lkup_res rt = _init_res(t, pt, path, length);

	return !_st_lookup(&rt);
}

uint st_move(task* t, st_ptr* pt, cdat path, uint length) {
	struct _st_lkup_res rt = _init_res(t, pt, path, length);

	if (!_st_lookup(&rt))
		_pt_move(pt, &rt);

	return (rt.length != 0);
}

uint st_insert(task* t, st_ptr* pt, cdat path, uint length) {
	struct _st_lkup_res rt = _init_res(t, pt, path, length);

	if (_st_lookup(&rt))
		_st_write(&rt);
	else
		rt.path = 0;

	_pt_move(pt, &rt);
	return (rt.path != 0);
}

struct _prepare_update {
	ushort remove;
	ushort waste;
};

static struct _prepare_update _st_prepare_update(struct _st_lkup_res* rt, task* t, st_ptr* pt) {
	struct _prepare_update pu;
	pu.remove = 0;

	rt->pg = _tk_check_ptr(t, pt);
	rt->diff = pt->offset;
	rt->sub = GOOFF(rt->pg,pt->key);
	rt->prev = 0;
	rt->t = t;

	_st_make_writable(rt);

	if (rt->sub->sub) {
		key* nxt = GOOFF(rt->pg,rt->sub->sub);
		while (nxt->next && nxt->offset < pt->offset) {
			rt->prev = nxt;
			nxt = GOOFF(rt->pg,nxt->next);
		}

		if (rt->prev) {
			pu.remove = rt->prev->next;
			rt->prev->next = 0;
		} else {
			pu.remove = rt->sub->sub;
			rt->sub->sub = 0;
		}
	}

	pu.waste = rt->sub->length - rt->diff;
	rt->sub->length = rt->diff;

	return pu;
}

uint st_update(task* t, st_ptr* pt, cdat path, uint length) {
	struct _st_lkup_res rt;
	struct _prepare_update pu = _st_prepare_update(&rt, t, pt);

	if (length > 0) {
		length <<= 3;
		/* old key has room for this update (and its a local page) */
		if (pu.waste >= length && pt->pg->id == 0) {
			memcpy(KDATA(rt.sub)+(rt.diff >> 3), path, length >> 3);
			rt.diff = rt.sub->length = length + rt.diff;
			pu.waste -= length;
		} else {
			rt.path = path;
			rt.length = length;
			_st_write(&rt);
		}
	}

	if (rt.pg->id)	// should we care about cleanup?
	{
		rt.pg->waste += pu.waste >> 3;
		_tk_remove_tree(t, rt.pg, pu.remove);
	}

	_pt_move(pt, &rt);
	return 0;
}

static uint _st_do_delete(struct _st_lkup_res* rt) {
	if (rt->prev == 0 && rt->sub->sub) {
		key* k = GOOFF(rt->pg,rt->sub->sub);
		while (k->offset < rt->diff) {
			rt->prev = k;
			if (k->next == 0)
				break;
			k = GOOFF(rt->pg,k->next);
		}
	}

	if (rt->prev) {
		_st_make_writable(rt);
		//waste  = rt.sub->length - rt.prev->offset;
		//remove = rt.prev->next;
		//rm_pg  = rt.pg;

		rt->sub->length = rt->prev->offset;
		rt->prev->next = 0;
	} else if (rt->d_sub) {
		page* orig = rt->d_pg;
		rt->d_pg = _tk_write_copy(rt->t, rt->d_pg);

		if (rt->d_prev) {
			key* k;
			// fix pointer
			rt->d_prev = GOKEY(rt->d_pg,(char*)rt->d_prev - (char*)orig);

			k = GOOFF(rt->d_pg,rt->d_prev->next);
			rt->d_prev->next = k->next;

			if (k->offset == rt->d_sub->length)
				rt->d_sub->length = rt->d_prev->offset;
		} else {
			key* k;
			// fix pointer
			rt->d_sub = GOKEY(rt->d_pg,(char*)rt->d_sub - (char*)orig);

			k = GOOFF(rt->d_pg,rt->d_sub->sub);
			rt->d_sub->sub = k->next;

			if (k->offset == rt->d_sub->length)
				rt->d_sub->length = 0;
		}
	} else
		return 1;

	return 0;
}

uint st_delete(task* t, st_ptr* pt, cdat path, uint length) {
	struct _st_lkup_res rt = _init_res(t, pt, path, length);

	if (_st_lookup(&rt))
		return 1;

	if (_st_do_delete(&rt))
		_st_prepare_update(&rt, t, pt);
	return 0;
}

uint st_clear(task* t, st_ptr* pt) {
	struct _st_lkup_res rt;
	_st_prepare_update(&rt, t, pt);
	return 0;
}

uint st_dataupdate(task* t, st_ptr* pt, cdat path, uint length) {
	struct _st_lkup_res rt = _init_res(t, pt, 0, 0);

	if (length > 0)
		_st_make_writable(&rt);

	while (length > 0) {
		uint wlen;

		if (ISPTR(rt.sub)) {
			rt.sub = _tk_get_ptr(t, &rt.pg, rt.sub);
			_st_make_writable(&rt);
		}

		wlen = (rt.sub->length - rt.diff) >> 3;
		if (wlen != 0) {
			wlen = length > wlen ? wlen : length;
			memcpy(KDATA(rt.sub)+(rt.diff >> 3), path, wlen);
			length -= wlen;
			rt.diff = 0;
		}

		if (rt.sub->sub == 0)
			break;

		wlen = rt.sub->length;
		rt.sub = GOOFF(rt.pg,rt.sub->sub);
		while (rt.sub->offset != wlen) {
			if (rt.sub->next == 0)
				return (length > 0);

			rt.sub = GOOFF(rt.pg,rt.sub->next);
		}
	}

	return (length > 0);
}

uint st_link(task* t, st_ptr* to, st_ptr* from) {
	if (from->offset != 0) {
		return 1;
	} else {
		struct _st_lkup_res rt;
		struct _prepare_update pu = _st_prepare_update(&rt, t, to);
		ptr* pt = _st_page_overflow(&rt, 0);

		pt->pg = from->pg;
		pt->koffset = from->key;

		if (rt.pg->id)	// should we care about cleanup?
		{
			rt.pg->waste += pu.waste >> 3;
			_tk_remove_tree(t, rt.pg, pu.remove);
		}
	}
	return 0;
}

uint st_append(task* t, st_ptr* pt, cdat path, uint length) {
	struct _st_lkup_res rt = _init_res(t, pt, path, length);
	rt.diff = rt.sub->length;

	if (rt.sub->sub) {
		key* nxt = GOOFF(rt.pg,rt.sub->sub);
		while (nxt->offset < pt->offset) {
			rt.prev = nxt;

			if (nxt->next == 0)
				break;
			nxt = GOOFF(rt.pg,nxt->next);
		}

		if (nxt->offset == rt.sub->length) {
			while (1) {
				rt.sub = (ISPTR(nxt)) ? _tk_get_ptr(t, &rt.pg, nxt) : nxt;
				if (rt.sub->sub == 0) {
					rt.diff = rt.sub->length;
					rt.prev = 0;
					break;
				}

				nxt = GOOFF(rt.pg,rt.sub->sub);
				if (nxt->offset != rt.sub->length)
					return 1;
			}
		} else if (nxt->offset >= pt->offset)
			return 1;
	}

	_st_write(&rt);

	_pt_move(pt, &rt);
	return 0;
}

static key* _trace_nxt(st_ptr* pt) {
	key* nxt;
	key* me = GOOFF(pt->pg,pt->key);
	// deal with offset
	if (me->sub == 0)
		return 0;

	nxt = GOOFF(pt->pg,me->sub);
	while (nxt->offset < pt->offset) {
		if (nxt->next == 0)
			return 0;

		nxt = GOOFF(pt->pg,nxt->next);
	}
	return nxt;
}

// return read lenght. Or -1 => eof data, -2 more data, buffer full
int st_get(task* t, st_ptr* pt, char* buffer, uint length) {
	page* pg = _tk_check_ptr(t, pt);
	key* me = GOOFF(pg,pt->key);
	key* nxt;
	cdat ckey = KDATA(me) + (pt->offset >> 3);
	uint offset = pt->offset;
	uint klen;
	int read = 0;

	nxt = _trace_nxt(pt);

	klen = ((nxt) ? nxt->offset : me->length) - offset;

	length <<= 3;

	while (1) {
		uint max = 0;

		if (klen > 0) {
			max = (length > klen ? klen : length) >> 3;
			memcpy(buffer, ckey, max);
			buffer += max;
			read += max;
			length -= max << 3;
		}

		// move to next key for more data?
		if (length > 0) {
			// no next key! or trying to read past split?
			if (nxt == 0 || (nxt->offset < me->length && me->length != 0)) {
				pt->offset = max + (offset & 0xFFF8);
				break;
			}

			offset = 0;
			me = (ISPTR(nxt)) ? _tk_get_ptr(t, &pg, nxt) : nxt;
			ckey = KDATA(me);
			if (me->sub) {
				nxt = GOOFF(pg,me->sub);
				klen = nxt->offset;
			} else {
				klen = me->length;
				nxt = 0;
			}
		}
		// is there any more data?
		else {
			max <<= 3;

			if (max < klen)	// more on this key?
				pt->offset = max + (offset & 0xFFF8);
			else if (nxt && nxt->offset == me->length)	// continuing key?
					{
				pt->offset = 0;
				me = (ISPTR(nxt)) ? _tk_get_ptr(t, &pg, nxt) : nxt;
			} else {
				pt->offset = me->length;
				read = -1;	// no more!
				break;
			}

			// yes .. tell caller and move st_ptr
			read = -2;
			break;
		}
	}

	pt->key = (char*) me - (char*) pg;
	pt->pg = pg;
	return read;
}

uint st_offset(task* t, st_ptr* pt, uint offset) {
	page* pg = _tk_check_ptr(t, pt);
	key* me = GOOFF(pg,pt->key);
	key* nxt;
	uint klen;

	nxt = _trace_nxt(pt);

	klen = (nxt) ? nxt->offset : me->length;

	offset = (offset << 3) + pt->offset;

	while (1) {
		uint max = offset > klen ? klen : offset;
		offset -= max;

		// move to next key for more data?
		if (offset > 0 && nxt && nxt->offset == me->length) {
			me = (ISPTR(nxt)) ? _tk_get_ptr(t, &pg, nxt) : nxt;
			if (me->sub) {
				nxt = GOOFF(pg,me->sub);
				klen = nxt->offset;
			} else {
				klen = me->length;
				nxt = 0;
			}
		}
		// is there any more data?
		else {
			if (max <= klen)
				pt->offset = max;
			else if (nxt) {
				pt->offset = 0;
				me = (ISPTR(nxt)) ? _tk_get_ptr(t, &pg, nxt) : nxt;
			} else
				pt->offset = me->length;

			// move st_ptr
			pt->pg = pg;
			pt->key = (char*) me - (char*) pg;
			return (offset >> 3);
		}
	}
}

int st_scan(task* t, st_ptr* pt) {
	key* k = GOOFF(_tk_check_ptr(t, pt),pt->key);

	while (1) {
		uint tmp;

		if ((k->length - pt->offset) & 0xfff8) {
			tmp = pt->offset >> 3;
			pt->offset += 8;
			return *(KDATA(k) + tmp);
		}

		if (k->sub == 0)
			return -1;

		tmp = k->length;
		k = GOOFF(pt->pg,k->sub);

		while (k->offset != tmp) {
			if (k->next == 0)
				return -1;

			k = GOOFF(pt->pg,k->next);
		}

		if (ISPTR(k))
			k = _tk_get_ptr(t, &pt->pg, k);

		pt->offset = 0;
		pt->key = (char*) k - (char*) pt->pg;
	}
}

static uint _dont_use(void* ctx) {
	return -2;
}

int st_map(task* t, st_ptr* str, uint (*fun)(void*, cdat, uint, uint), void* ctx) {
	return st_map_st(t, str, fun, _dont_use, _dont_use, ctx);
}

static uint _mv_st(void* p, cdat txt, uint len, uint at) {
	struct _st_lkup_res* rt = (struct _st_lkup_res*) p;
	rt->path = txt;
	rt->length = len << 3;
	return _st_lookup(rt);
}

uint st_move_st(task* t, st_ptr* mv, st_ptr* str) {
	struct _st_lkup_res rt = _init_res(t, mv, 0, 0);
	uint ret;

	if ((ret = st_map_st(t, str, _mv_st, _dont_use, _dont_use, &rt)))
		return ret;

	_pt_move(mv, &rt);
	return 0;
}

// 0 = equal, -1 = pt1 < pt2, 1 = pt1 > pt2
uint st_compare(task* t, st_ptr* pt1, st_ptr* pt2) {
	struct _st_lkup_res rt = _init_res(t, pt1, 0, 0);

	if (st_map_st(t, pt2, _mv_st, _dont_use, _dont_use, &rt))
		return *rt.path & (1 << (rt.diff & 7)) ? 1 : -1;

	return 0;
}

struct _st_insert {
	struct _st_lkup_res rt;
	uint have_written;
};

static uint _ins_st(void* p, cdat txt, uint len, uint at) {
	struct _st_insert* sins = (struct _st_insert*) p;
	sins->rt.path = txt;
	sins->rt.length = len << 3;
	if (sins->have_written || _st_lookup(&sins->rt)) {
		sins->have_written = 1;
		_st_write(&sins->rt);
	}
	return 0;
}

uint st_insert_st(task* t, st_ptr* to, st_ptr* from) {
	struct _st_insert sins;
	uint ret;
	sins.rt.t = t;
	sins.rt.pg = _tk_check_ptr(t, to);
	sins.rt.sub = GOOFF(to->pg,to->key);
	sins.rt.diff = to->offset;
	sins.have_written = 0;

	if ((ret = st_map_st(t, from, _ins_st, _dont_use, _dont_use, &sins)))
		return ret;

	_pt_move(to, &sins.rt);
	return sins.have_written;
}

int st_exist_st(task* t, st_ptr* p1, st_ptr* p2) {
	struct st_stream* snd = st_exist_stream(t, p1);
    
	uint ret = st_map_st(t, p2, (uint(*)(void*, cdat, uint, uint))st_stream_data, (uint(*)(void*))st_stream_push, (uint(*)(void*))st_stream_pop, snd);
    
	st_destroy_stream(snd);
	return (ret == 0);
}

uint st_copy_st(task* t, st_ptr* to, st_ptr* from) {
	struct st_stream* snd = st_merge_stream(t, to);
    
	uint ret = st_map_st(t, from, (uint(*)(void*, cdat, uint, uint))st_stream_data, (uint(*)(void*))st_stream_push, (uint(*)(void*))st_stream_pop, snd);
    
	st_destroy_stream(snd);
	return ret;
}

uint st_delete_st(task* t, st_ptr* from, st_ptr* str) {
	struct _st_lkup_res rt = _init_res(t, from, 0, 0);
	uint ret = st_map_st(t, str, _mv_st, _dont_use, _dont_use, &rt);

	if (ret == 0 && _st_do_delete(&rt))
		_st_prepare_update(&rt, t, from);

	return ret;
}

// structure mapper

struct _st_map_worker_struct {
	uint (*dat)(void*, cdat, uint, uint);
	uint (*push)(void*);
	uint (*pop)(void*);
	void* ctx;
	task* t;
};

static uint _st_worker(struct _st_map_worker_struct* work, page* pg, key* me, key* nxt, uint offset, uint at) {
    struct {
        page* pg;
        key* nxt;
        uint at;
    } mx[16];
    uint ret = 0, idx = 0;
    
    while (1) {
        const uint klen = ((nxt != 0 ? nxt->offset : me->length) >> 3) - offset;
        if (klen != 0 && (ret = work->dat(work->ctx, KDATA(me) + offset, klen, at)))
            break;
        at += klen;
        
        if(nxt == 0) {
            if (idx-- == 0)
                break;
            
            pg = _tk_check_page(work->t, mx[idx].pg);
            nxt = mx[idx].nxt;
            at = mx[idx].at;
        } else if (me->length != nxt->offset) {
            if((ret = work->push(work->ctx))) break;
            
            if ((idx & 0xF0) == 0) {
                mx[idx].nxt = nxt;
                mx[idx].pg = pg;
                mx[idx].at = at;
                ++idx;
                
                offset = nxt->offset >> 3;
                nxt = nxt->next? GOOFF(pg, nxt->next) : 0;
                continue;
            }
            
            if((ret = _st_worker(work, pg, me, nxt, offset, at))) break;
        }
        
        if((ret = work->pop(work->ctx))) break;

        me = (ISPTR(nxt)) ? _tk_get_ptr(work->t, &pg, nxt) : nxt;
        nxt = me->sub? GOOFF(pg, me->sub) : 0;
        offset = 0;
    }
    return ret;
}

static uint _st_map_worker(struct _st_map_worker_struct* work, page* pg, key* me, key* nxt, uint offset, uint at) {
	struct {
		page* pg;
		key* me;
		key* nxt;
		uint at;
	} mx[16];
	uint ret = 0, idx = 0;
	while (1) {
		const uint klen = ((nxt != 0 ? nxt->offset : me->length) >> 3) - (offset >> 3);
		if (klen != 0) {
			cdat ckey = KDATA(me) + (offset >> 3);
			if ((ret = work->dat(work->ctx, ckey, klen, at)))
				break;
			at += klen;
		}

		if (nxt == 0) {
			_map_pop: if ((ret = work->pop(work->ctx)) || idx-- == 0)
				break;

			at = mx[idx].at;
			me = mx[idx].me;
			nxt = mx[idx].nxt;
			pg = _tk_check_page(work->t, mx[idx].pg);

			offset = nxt->offset;
			nxt = (nxt->next != 0) ? GOOFF(pg,nxt->next) : 0;
		} else {
			if (nxt->offset < me->length && me->length != 0) {
				if ((ret = work->push(work->ctx)))
					break;

				if (idx & 0xF0) {
					me = (ISPTR(nxt)) ? _tk_get_ptr(work->t, &pg, nxt) : nxt;
					nxt = (me->sub != 0) ? GOOFF(pg,me->sub) : 0;

					if ((ret = _st_map_worker(work, pg, me, nxt, 0, at)))
						break;
					goto _map_pop;
				}

				mx[idx].me = me;
				mx[idx].nxt = nxt;
				mx[idx].pg = pg;
				mx[idx].at = at;
				idx++;
			}

			me = (ISPTR(nxt)) ? _tk_get_ptr(work->t, &pg, nxt) : nxt;
			nxt = (me->sub != 0) ? GOOFF(pg,me->sub) : 0;
			offset = 0;
		}
	}

	return ret;
}

uint st_map_st(task* t, st_ptr* from, uint (*dat)(void*, cdat, uint, uint), uint (*push)(void*), uint (*pop)(void*), void* ctx) {
	struct _st_map_worker_struct work;
	work.ctx = ctx;
	work.dat = dat;
	work.pop = pop;
	work.push = push;
	work.t = t;

	_tk_check_ptr(t, from);

	return _st_map_worker(&work, from->pg, GOOFF(from->pg,from->key), _trace_nxt(from), from->offset, 0);
}

// sending functions
#define SEND_GROW 8

struct st_stream {
	struct _st_lkup_res* top;
	uint (*dat_fun)(struct _st_lkup_res*);
	uint (*pop_fun)(struct st_stream*);
	task* t;
	uint idx;
	uint max;
};

static struct st_stream* _st_create_stream(task* t, uint (*fun)(struct _st_lkup_res*), st_ptr* start) {
	struct st_stream* d = tk_malloc(t, sizeof(struct st_stream));

	d->t = t;
	d->idx = 0;
	d->max = SEND_GROW;
	d->dat_fun = fun;
	d->pop_fun = 0;
	d->top = tk_malloc(t, sizeof(struct _st_lkup_res) * d->max);

	d->top[0] = _init_res(t, start, 0, 0);
	return d;
}

struct st_stream* st_exist_stream(task* t, st_ptr* pt) {
	return _st_create_stream(t, _st_lookup, pt);
}

static uint _st_strm_ins(struct _st_lkup_res* rt) {
	if (_st_lookup(rt))
		_st_write(rt);
	return 0;
}

struct st_stream* st_merge_stream(task* t, st_ptr* pt) {
	return _st_create_stream(t, _st_strm_ins, pt);
}

static uint _st_strm_del_dat(struct _st_lkup_res* rt) {
	if (_st_lookup(rt)) {
		rt->path = 0;
	}
	return 0;
}

static uint _st_strm_del_pop(struct st_stream* ctx) {
	struct _st_lkup_res* rt = &ctx->top[ctx->idx];
	if (rt->path != 0 && _st_do_delete(rt)) {
		st_ptr pt;
		_pt_move(&pt, &ctx->top[0]);
		st_clear(rt->t, &pt);
	}
	return 0;
}

struct st_stream* st_delete_stream(task* t, st_ptr* pt) {
	struct st_stream* s = _st_create_stream(t, _st_strm_del_dat, pt);
	s->pop_fun = _st_strm_del_pop;
	st_stream_push(s);
	return s;
}

uint st_destroy_stream(struct st_stream* ctx) {
	uint ret = 0;
	if (ctx->pop_fun)
		ret = ctx->pop_fun(ctx);

	tk_mfree(ctx->t, ctx->top);
	tk_mfree(ctx->t, ctx);
	return ret;
}

uint st_stream_data(struct st_stream* ctx, cdat dat, uint length, uint at) {
	struct _st_lkup_res* rt = &ctx->top[ctx->idx];
	rt->path = dat;
	rt->length = length << 3;
	return ctx->dat_fun(rt);
}

uint st_stream_push(struct st_stream* ctx) {
	if (++ctx->idx == ctx->max) {
		ctx->max += SEND_GROW;
		ctx->top = tk_realloc(ctx->t, ctx->top, sizeof(struct _st_lkup_res) * ctx->max);
	}

	ctx->top[ctx->idx] = ctx->top[ctx->idx - 1];
	return 0;
}

uint st_stream_pop(struct st_stream* ctx) {
	uint ret = 0;
	if (ctx->idx == 0)
		return 1;

	if (ctx->pop_fun)
		ret = ctx->pop_fun(ctx);

	ctx->idx--;
	// signal that no data was send here yet
	ctx->top[ctx->idx].path = 0;
	return ret;
}

// ptr list

ptr_list* ptr_list_reverse(ptr_list* e) {
	ptr_list* link = 0;
	do {
		ptr_list* prev = e->link;
		e->link = link;
		link = e;
		e = prev;
	} while (e != 0);
	return link;
}
