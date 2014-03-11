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
#include <string.h>

#include "cle_struct.h"

/* ---------- iterator -------------- */

struct _st_lkup_it_res {
	task* t;
	page* pg;
	page* low_pg;
	page* high_pg;
	key* prev;
	key* sub;
	key* low;
	key* low_prev;
	key* high;
	key* high_prev;
	uchar* path;
	uchar* low_path;
	uchar* high_path;
	uint length;
	uint diff;
	uint low_diff;
	uint high_diff;
};

static void _it_lookup(struct _st_lkup_it_res* rt) {
	key* me = rt->sub;
	cdat ckey = KDATA(me) + (rt->diff >> 3);
	uchar* atsub = rt->path - (rt->diff >> 3);
	uint offset = rt->diff;
	uint max = me->length - rt->diff;

	rt->high = rt->high_prev = rt->low = rt->low_prev = 0;
	rt->high_path = rt->low_path = 0;
	rt->high_diff = rt->low_diff = 0;

	while (1) {
		uchar* to, *curr = rt->path;
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

		if (me->sub == 0)
			me = 0;
		else {
			ckey = KDATA(me);
			me = GOOFF(rt->pg,me->sub);

			while (me->offset < rt->diff) {
				rt->prev = me;

				if (me->offset >= offset) {
					if (*(ckey + (me->offset >> 3)) & (0x80 >> (me->offset & 7))) {
						rt->low = me;
						rt->low_prev = 0;
						rt->low_path = atsub + (me->offset >> 3);
						rt->low_pg = rt->pg;
						rt->low_diff = 0;
					} else {
						rt->high = me;
						rt->high_prev = 0;
						rt->high_path = atsub + (me->offset >> 3);
						rt->high_pg = rt->pg;
						rt->high_diff = 0;
					}
				}

				if (!me->next)
					break;

				me = GOOFF(rt->pg,me->next);
			}
		}

		if (rt->diff != rt->sub->length) {
			key* k = (me != 0 && me->offset >= offset) ? me : 0;

			if (*rt->path & (0x80 >> (rt->diff & 7))) {
				rt->low = rt->sub;
				rt->low_prev = k;
				rt->low_path = rt->path;
				rt->low_pg = rt->pg;
				rt->low_diff = (k == 0) ? rt->diff : k->offset;
			} else {
				rt->high = rt->sub;
				rt->high_prev = k;
				rt->high_path = rt->path;
				rt->high_pg = rt->pg;
				rt->high_diff = (k == 0) ? rt->diff : k->offset;
			}
		}

		if (rt->length == 0 || me == 0 || me->offset != rt->diff)
			break;

		// if this is a pointer - resolve it
		if (ISPTR(me))
			me = _tk_get_ptr(rt->t, &rt->pg, me);

		ckey = KDATA(me);
		atsub = rt->path;
		max = me->length;
		rt->diff = 0;
		offset = 0;
	}
}

static void _it_grow_kdata(it_ptr* it, struct _st_lkup_it_res* rt) {
	uchar* kdata = it->kdata;
	uint path_offset = (uint) ((char*) rt->path - (char*) it->kdata);
	it->ksize += IT_GROW_SIZE;
	it->kdata = tk_alloc(rt->t, it->ksize, 0);
	if (kdata != 0)
		memcpy(it->kdata, kdata, it->kused);
	rt->path = it->kdata + path_offset;
}

static void _it_get_prev(struct _st_lkup_it_res* rt) {
	if (rt->diff && rt->sub->sub) {
		key* nxt = GOOFF(rt->pg,rt->sub->sub);
		while (nxt->offset < rt->diff) {
			rt->prev = nxt;
			if (nxt->next == 0)
				break;
			nxt = GOOFF(rt->pg,nxt->next);
		}
	}
}

static void _it_next_prev(it_ptr* it, struct _st_lkup_it_res* rt, const uint is_next, const int length) {
	key* sub = rt->sub;
	key* prev = rt->prev;
	uint offset = rt->diff & 0xFFF8;

	it->kused = (uchar*) rt->path - (uchar*) it->kdata;

	if (length > 0 && it->ksize < length) {
		uchar* kdata = it->kdata;
		it->kdata = tk_alloc(rt->t, length, 0);
		it->ksize = length;
		rt->path = it->kdata + it->kused;
		if (kdata != 0)
			memcpy(it->kdata, kdata, it->kused);
	}

	do {
		cdat ckey;
		uint clen;

		if (ISPTR(sub))	// ptr-key?
			sub = _tk_get_ptr(rt->t, &rt->pg, sub);

		rt->sub = sub;
		ckey = KDATA(sub);

		if (prev)
			prev = (prev->next) ? GOOFF(rt->pg,prev->next) : 0;
		else if (sub->sub)
			prev = GOOFF(rt->pg,sub->sub);

		if (prev) {
			while (1) {
				if (prev->offset == sub->length)	// continue-key?
					break;
				else {
					// is prev higher/lower than sub?
					uchar is_low = *(ckey + (prev->offset >> 3)) & (0x80 >> (prev->offset & 7));
					if (is_next) {
						if (is_low)
							break;
					} else if (!is_low)
						break;
				}

				if (prev->next == 0) {
					prev = 0;
					break;
				}
				prev = GOOFF(rt->pg,prev->next);
			}
		}

		clen = (prev) ? prev->offset : sub->length;
		rt->diff = offset;
		if (offset) {
			clen -= offset;
			ckey += offset >> 3;
			offset = 0;
		}
		clen >>= 3;

		// copy bytes
		if (length == 0) {
			while (clen-- != 0) {
				it->kused++;
				if (it->kused > it->ksize)
					_it_grow_kdata(it, rt);

				rt->diff += 8;
				*rt->path++ = *ckey;
				if (*ckey == 0)
					return;
				ckey++;
			}
		} else if (length < 0) {
			it->kused += clen;

			if (it->kused > it->ksize)
				_it_grow_kdata(it, rt);

			memcpy(rt->path, ckey, clen);
			rt->diff += clen * 8;
			rt->path += clen;
		} else {
			if (it->kused + clen > length) {
				if (it->kused >= length)
					return;
				clen = length - it->kused;
			}

			memcpy(rt->path, ckey, clen);
			rt->diff += clen * 8;
			rt->path += clen;
			it->kused += clen;

			if (it->kused >= length)
				return;
		}

		sub = prev;	// next key
		prev = 0;
	} while (sub);
}

uint it_next(task* t, st_ptr* pt, it_ptr* it, const int length) {
	struct _st_lkup_it_res rt;
	rt.t = t;
	rt.path = it->kdata;
	rt.length = it->kused << 3;
	rt.pg = _tk_check_page(t, it->pg);
	rt.sub = GOOFF(rt.pg,it->key);
	rt.prev = 0;
	rt.diff = it->offset;

	if (rt.length > 0) {
		_it_lookup(&rt);

		if (rt.high == 0) {
			if (rt.length == 0)
				return 0;
		} else if (rt.length == 0) {
			rt.diff = rt.high_diff;	//rt.high_prev? rt.high_prev->offset : 0;
			rt.sub = rt.high;
			rt.prev = rt.high_prev;
			rt.path = rt.high_path;
			rt.pg = rt.high_pg;
		}
	} else
		_it_get_prev(&rt);

	_it_next_prev(it, &rt, 1, length);

	if (pt) {
		pt->pg = rt.pg;
		pt->key = (char*) rt.sub - (char*) rt.pg;
		pt->offset = rt.diff;
	}
	return (it->kused > 0);
}

uint it_next_eq(task* t, st_ptr* pt, it_ptr* it, const int length) {
	struct _st_lkup_it_res rt;
	rt.t = t;
	rt.path = it->kdata;
	rt.length = it->kused << 3;
	rt.pg = _tk_check_page(t, it->pg);
	rt.sub = GOOFF(rt.pg,it->key);
	rt.prev = 0;
	rt.diff = it->offset;

	if (rt.length > 0) {
		_it_lookup(&rt);

		if (rt.length == 0) {
			if (pt) {
				pt->pg = rt.pg;
				pt->key = (char*) rt.sub - (char*) rt.pg;
				pt->offset = rt.diff;
			}
			return 2;
		}

		if (rt.high == 0)
			return 0;

		rt.diff = rt.high_diff;
		rt.sub = rt.high;
		rt.prev = rt.high_prev;
		rt.path = rt.high_path;
		rt.pg = rt.high_pg;
	} else
		_it_get_prev(&rt);

	_it_next_prev(it, &rt, 1, length);

	if (pt) {
		pt->pg = rt.pg;
		pt->key = (char*) rt.sub - (char*) rt.pg;
		pt->offset = rt.diff;
	}
	return (it->kused > 0) ? 1 : 0;
}

uint it_prev(task* t, st_ptr* pt, it_ptr* it, const int length) {
	struct _st_lkup_it_res rt;
	rt.t = t;
	rt.path = it->kdata;
	rt.length = it->kused << 3;
	rt.pg = _tk_check_page(t, it->pg);
	rt.sub = GOOFF(rt.pg,it->key);
	rt.prev = 0;
	rt.diff = it->offset;

	if (rt.length > 0) {
		_it_lookup(&rt);

		if (rt.low == 0) {
			if (rt.length == 0)
				return 0;
		} else if (rt.length == 0) {
			rt.diff = rt.low_diff;
			rt.sub = rt.low;
			rt.prev = rt.low_prev;
			rt.path = rt.low_path;
			rt.pg = rt.low_pg;
		}
	} else
		_it_get_prev(&rt);

	_it_next_prev(it, &rt, 0, length);

	if (pt) {
		pt->pg = rt.pg;
		pt->key = (char*) rt.sub - (char*) rt.pg;
		pt->offset = rt.diff;
	}
	return (it->kused > 0);
}

uint it_prev_eq(task* t, st_ptr* pt, it_ptr* it, const int length) {
	struct _st_lkup_it_res rt;
	rt.t = t;
	rt.path = it->kdata;
	rt.length = it->kused << 3;
	rt.pg = _tk_check_page(t, it->pg);
	rt.sub = GOOFF(rt.pg,it->key);
	rt.prev = 0;
	rt.diff = it->offset;

	if (rt.length > 0) {
		_it_lookup(&rt);

		if (rt.length == 0) {
			if (pt) {
				pt->pg = rt.pg;
				pt->key = (char*) rt.sub - (char*) rt.pg;
				pt->offset = rt.diff;
			}
			return 2;
		}

		if (rt.low == 0)
			return 0;

		rt.diff = rt.low_diff;
		rt.sub = rt.low;
		rt.prev = rt.low_prev;
		rt.path = rt.low_path;
		rt.pg = rt.low_pg;
	} else
		_it_get_prev(&rt);

	_it_next_prev(it, &rt, 0, length);

	if (pt) {
		pt->pg = rt.pg;
		pt->key = (char*) rt.sub - (char*) rt.pg;
		pt->offset = rt.diff;
	}
	return (it->kused > 0) ? 1 : 0;
}

void it_load(task* t, it_ptr* it, cdat path, uint length) {
	if (it->ksize < length) {
		it->ksize = length + IT_GROW_SIZE;
		it->kdata = (uchar*) tk_realloc(t, it->kdata, it->ksize);
	}

	memcpy(it->kdata, path, length);

	it->kused = length;
}

void it_create(task* t, it_ptr* it, st_ptr* pt) {
	it->pg = pt->pg;
	it->key = pt->key;
	it->offset = pt->offset;

	it->kdata = 0;
	it->ksize = it->kused = 0;

	//it->pg->refcount++;
}

void it_dispose(task* t, it_ptr* it) {
	tk_unref(t, it->pg);
}

void it_reset(it_ptr* it) {
	it->kused = 0;
}

uint it_current(task* t, it_ptr* it, st_ptr* pt) {
	if (it->kused == 0)
		return 1;
	pt->pg = it->pg;
	pt->key = it->key;
	pt->offset = it->offset;
	return st_move(t, pt, it->kdata, it->kused);
}

/**
 * update/build increasing index
 */
uint it_new(task* t, it_ptr* it, st_ptr* pt) {
	struct _st_lkup_it_res rt;
	rt.t = t;
	rt.path = it->kdata;
	rt.length = 0;
	rt.pg = _tk_check_page(t, it->pg);
	rt.sub = GOOFF(rt.pg,it->key);
	rt.prev = 0;
	rt.diff = it->offset;
	it->kused = 0;

	_it_get_prev(&rt);

	_it_next_prev(it, &rt, 0, 0);	// get highest position

	if (it->kused == 0)	// init 1.index
			{
		if (it->ksize == 0)
			_it_grow_kdata(it, &rt);

		it->kdata[0] = it->kdata[1] = 1;
		it->kdata[2] = 0;
		it->kused = 3;
	} else if (it->kused < 3 || it->kused != it->kdata[0] + 2)
		return 1;	// can not be an index
	else {				// incr. index
		uint idx = it->kused - 2;

		while (idx != 0 && it->kdata[idx] == 0xFF) {
			it->kdata[idx] = 1;
			idx--;
		}

		if (idx == 0) {
			// index max-size!
			if (it->kdata[0] == 0xFF)
				return 1;

			if (it->kused == it->ksize)
				_it_grow_kdata(it, &rt);

			it->kdata[it->kused - 1] = 1;
			it->kdata[it->kused] = 0;
			it->kdata[0]++;
			it->kused++;
		} else
			it->kdata[idx]++;
	}

	pt->pg = it->pg;
	pt->key = it->key;
	pt->offset = it->offset;

	return (st_insert(t, pt, it->kdata, it->kused) == 0);
}
