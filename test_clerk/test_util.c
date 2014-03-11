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

/*
 TEST-SUITE RUNNER
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "test.h"
#include "../cle_core/cle_runtime.h"

static FILE* f;
static void print_struct(page* pg, const key* me, int ind, int meoff) {
	while (1) {
		int i;
        
		const char* path = KDATA(me);
		int l = me->length;
		int o = me->offset;
		//int meoff = (int)((char*)me - (char*)pg->pg);
        
		for (i = 0; i < ind; i++)
			fputs("..", f);
        
		if (ISPTR(me)) {
			ptr* pt = (ptr*) me;
            
			if (pt->koffset < sizeof(page)) {
				page* wrap;
				fprintf(f, "(%s%d)(EXT) page:%p (%d - n:%d) ", (*path & (0x80 >> (o & 7))) ? "+" : "-", pt->offset, pt->pg,
						meoff, pt->next);
                
                if (pt->koffset == 1){
					puts(">> XXXXXX");
                }
				else if (ind < 0) {
					wrap = pt->pg;
					printf(" (%d)>>\n", wrap->used);
					print_struct(wrap, GOKEY(wrap, sizeof(page)), ind + 2, sizeof(page));
				} else
					puts(">>");
			} else {
				fprintf(f, "(%s%d)(INT) page:%p + %d (%d - n:%d) >>\n", (*path & (0x80 >> (o & 7))) ? "+" : "-", pt->offset,
						pt->pg, pt->koffset, meoff, pt->next);
                
                if (ind < 4) {
                    print_struct((page*) pt->pg, GOKEY((page*) pt->pg,
                                                       pt->koffset), ind + 2, pt->koffset);
                }
			}
        } else {
                int i;
                
                fprintf(f, "(%s%d/%d) %s (%d - s:%d n:%d) [", (o < l && *path & (0x80 >> (o & 7))) ? "+" : "-", o, l, "" /*path*/,
                        meoff, me->sub, me->next);
                
                //printf("%s",path);
                for (i = 0; i < (l + 7) >> 3; i++) {
                    if (i > 3) {
                        printf("...");
                        break;
                    }
                    printf(" %x", path[i]);
                }
                
                if (me->sub) {
                    fputs("] ->\n", f);
                    print_struct(pg, GOOFF(pg, me->sub), ind + 1, me->sub);
                } else
                    fputs("]\n", f);
            }
        
		if (!me->next)
			break;
        
		//if(me == (key*)0x029b7edc)
		//{
		//	l = l;
		//}
        
		meoff = me->next;
		me = GOOFF(pg, me->next);
	}
}

void st_prt_page(st_ptr* pt) {
	f = stdout;
	fprintf(f, "%p(%d/%d)\n", pt->pg->id, pt->pg->used, pt->pg->waste);
	print_struct(pt->pg, GOOFF(pt->pg, pt->key), 0, pt->key);
}

int _tk_validate(page* pg, uint* kcount, ushort koff) {
	while (koff != 0) {
		key* k = (key*) ((char*) pg + koff);

		if (*kcount > (uint) (pg->used / 8)) {
			return 1;
		}
		*kcount += 1;

		if (ISPTR(k)) {
		} else if (_tk_validate(pg, kcount, k->sub) != 0)
			return 1;

		koff = k->next;
	}

	return 0;
}

void _tk_print(page* pg) {
	int koff = sizeof(page);

	printf("PAGE: %p/id:%p (%d,%d)\n", pg, pg->id, pg->used, pg->waste);

	while (koff < pg->used) {
		key* k = (key*) ((char*) pg + koff);

		if (ISPTR(k)) {
			ptr* pt = (ptr*) k;
			if (pt->koffset == 0)
				printf("%d Eptr n:%d >> %p\n", koff, pt->next, pt->pg);
			else
				printf("%d Iptr n:%d >> %p\n", koff, pt->next, pt->pg);

			koff += sizeof(ptr);
		} else {
			printf("%d key l:%d o:%d s:%d n:%d\n", koff, k->length, k->offset, k->sub, k->next);
			koff += sizeof(key) + ((k->length + 7) >> 3);
		}

		koff += koff & 1;
	}
}

static task* t;
static int levels[100];
static int filling[10];
static int empty_keys;
static int offset_zero;
static int key_count;
static int ptr_count;
static int mem_used;
static int mem_idle;

static void calc_dist(page* pg, key* me, key* parent, int level) {
	if (level >= 100)
		return;
    
	while (1) {
		key* pme;
		int offset = me->offset;

		if (offset == 0) {
			offset_zero++;
		}

		if (me->length == 0) {
			empty_keys++;
		}

		// as ptr has high lenght - these wont work with pointers
		if (ISPTR(me) == 0 && (((me->length + 7) >> 3) + sizeof(key) + (char*) me > (char*) pg + pg->used)) {
			printf("F0 ");
		}

		if (parent == 0 && me->offset != 0) {
			//printf("F1 ");
		}

		if (parent != 0 && ISPTR(parent) == 0 && me->offset > parent->length) {
			printf("F2 ");
		}

		if (ISPTR(me)) {
			//if(me->sub != 0)
			{
				ptr* pt = (ptr*) me;
				st_ptr tmp;
				page* pw = pg;
				key* root = _tk_get_ptr(t, &pw, me);
				//// keep page-wrapper (forever)
				//pw->refcount++;
				levels[level] += 1;
				ptr_count++;

				filling[(int) (((float) pw->used / (float) pw->size) * 8.0)]++;

				if (pw->used > pw->size) {
					printf("p overflow ");
				}

                mem_used += pg->used;
                mem_idle += pg->size - pg->used + pg->waste;

				//printf("%p\n",pw);

				//if(pw == (page_wrap*)0x003b4c88)
				//{
				//	st_ptr tmp;
				//	tmp.key = sizeof(page);
				//	tmp.offset = 0;
				//	tmp.pg = pw;
				//	st_prt_page(&tmp);
				//}

				//			tmp.key = (pt->koffset == 0)? sizeof(page) : pt->koffset;
				//			tmp.offset = 0;
				//			tmp.pg = pw;
				//			puts("\n");
				//			st_prt_page(&tmp);

				calc_dist(pw, root, 0, level + 1);
			}
		} else {
			key_count++;

			if (me->sub)
				calc_dist(pg, GOOFF(pg, me->sub), me, level);
		}

		if (!me->next)
			break;

		pme = me;

		me = GOOFF(pg, me->next);

		if (me->offset <= offset) {
			st_ptr tmp;
			tmp.key = sizeof(page);
			tmp.offset = 0;
			tmp.pg = pg;
			st_prt_page(&tmp);
			printf("F3 ");
		}
	}
}

void st_prt_distribution(st_ptr* pt, task* tsk) {
	task_page* pw;
	int i;

	t = tsk;

	for (i = 0; i < 100; i++)
		levels[i] = 0;

	for (i = 0; i < 8; i++)
		filling[i] = 0;

	empty_keys = 0;
	offset_zero = 0;
	key_count = 0;
	ptr_count = 0;
    mem_used = 0;
    mem_idle = 0;

	//puts("\n");
	//st_prt_page(pt);
	//puts("\n");

	tk_ref_ptr(pt);

	calc_dist(pt->pg, GOKEY(pt->pg, pt->key), 0, 0);

	for (i = 0; i < 100 && levels[i] != 0; i++)
		printf("L:%d -> %d\n", i + 1, levels[i]);

	for (i = 0; i < 8; i++)
		printf("fill %d%% => %d\n", (int) ((float) i * 100.0 / 8.0), filling[i]);

	printf("empty keys: %d\n", empty_keys);
	printf("zero offset: %d\n", offset_zero);
	printf("key count: %d\n", key_count);
	printf("ptr count: %d\n", ptr_count);

	{
		int pages_written = 0;
		int ovf_pages = 0;
		int ovf_used = 0;
		int ovf_free = 0;
		int stack_pages = 0;

		pw = tsk->stack;
		while (pw) {
			stack_pages++;
			if (pw->ovf != 0) {
				ovf_pages++;
				ovf_used += pw->ovf->used;
				ovf_free += pw->ovf->size - pw->ovf->used;
				if (pw->ovf->size > OVERFLOW_GROW) {
					//					printf("OVF size: %d used: %d\n",pw->ovf->size,pw->ovf->used);
				}
			}

			pw = pw->next;
		}

		pw = tsk->wpages;
		while (pw) {
			pages_written++;
			pw = pw->next;
		}

		printf("pages_written: %d\n", pages_written);
		printf("stack_pages: %d\n", stack_pages);
		printf("ovf_pages: %d\n", ovf_pages);
		printf("ovf_used: %d\n", ovf_used);
		printf("ovf_free: %d\n", ovf_free);
		printf("mem used: %d\n", mem_used);
		printf("mem idld: %d\n", mem_idle);
	}
}

uint sim_new(uchar kdata[], uint ksize) {
	if (kdata[0] == 0) // init 1.index
			{
		kdata[0] = kdata[1] = 1;
		kdata[2] = 0;
	} else { // incr. index
		uint idx = kdata[0];

		while (idx != 0 && kdata[idx] == 0xFF) {
			kdata[idx] = 1;
			idx--;
		}

		if (idx == 0) {
			// index max-size!
			if (kdata[0] == 0xFF || kdata[0] + 2 >= ksize)
				return 0;

			kdata[kdata[0] + 1] = 1;
			kdata[kdata[0] + 2] = 0;
			kdata[0]++;
		} else
			kdata[idx]++;
	}

	return kdata[0] + 2;
}

st_ptr str(task* t, char* cs) {
	st_ptr pt, org;

	st_empty(t, &pt);
	org = pt;

	st_insert(t, &pt, (cdat) cs, (uint)strlen(cs));

	return org;
}

uint add(task* t, st_ptr p, char* cs){
    return st_insert(t, &p, (cdat)cs, (uint)strlen(cs));
}

uint rm(task* t, st_ptr p, char* cs){
    return st_delete(t, &p, (cdat)cs, (uint)strlen(cs));
}

st_ptr root(task* t) {
	st_ptr rt;
	tk_root_ptr(t, &rt);
	return rt;
}

//////////////////////////////////////////////////////////////////////////
// DUMP CODE

/*
 stack-strukture:
 var-space
 stack-space

 runtime:
 [0] <- assign to: st_ptr OR output-identifier
 [...] <- values
 */
// code-header
static void _cle_indent(uint indent, const char* str, uint length) {
	while (indent-- > 0)
		printf("  ");
	printf("%.*s", length, str);
}

static void _cle_read(task* t, st_ptr* root, uint indent) {
	it_ptr it;
	st_ptr pt;
	uint elms = 0;

	it_create(t, &it, root);

	while (it_next(t, &pt, &it, 0)) {
		if (elms == 0) {
			puts("{");
			elms = 1;
		}
		_cle_indent(indent + 1, it.kdata, it.kused);
		_cle_read(t, &pt, indent + 1);
	}

	if (elms)
		_cle_indent(indent, "}\n", 2);
	else
		puts("");

	it_dispose(t, &it);
}

static void err(int line) {
	printf("DUMP err %d line\n", line);
}

static const char* _rt_opc_name(uint opc) {
	switch (opc) {
	case OP_NOOP:
		return "OP_NOOP";
	case OP_SETP:
		return "OP_SETP";
	case OP_DOCALL:
		return "OP_DOCALL";
	case OP_DOCALL_N:
		return "OP_DOCALL_N";
	case OP_WIDX:
		return "OP_WIDX";
	case OP_WVAR0:
		return "OP_WVAR0";
	case OP_WVAR:
		return "OP_WVAR";
	case OP_DMVW:
		return "OP_DMVW";
	case OP_MVW:
		return "OP_MVW";
	case OP_OUT:
		return "OP_OUT";
	case OP_RIDX:
		return "OP_RIDX";
	case OP_RVAR:
		return "OP_RVAR";
	case OP_LVAR:
		return "OP_LVAR";
	case OP_MV:
		return "OP_MV";
	case OP_END:
		return "OP_END";
	case OP_DEFP:
		return "OP_DEFP";
	case OP_BODY:
		return "OP_BODY";
	case OP_STR:
		return "OP_STR";
	case OP_CALL:
		return "OP_CALL";
	case OP_POP:
		return "OP_POP";
	case OP_POPW:
		return "OP_POPW";
	case OP_FREE:
		return "OP_FREE";
	case OP_AVARS:
		return "OP_AVARS";
	case OP_OVARS:
		return "OP_OVARS";
	case OP_ADD:
		return "OP_ADD";
	case OP_SUB:
		return "OP_SUB";
	case OP_MUL:
		return "OP_MUL";
	case OP_DIV:
		return "OP_DIV";
	case OP_REM:
		return "OP_REM";
	case OP_IMM:
		return "OP_IMM";
	case OP_BNZ:
		return "OP_BNZ";
	case OP_BZ:
		return "OP_BZ";
	case OP_BR:
		return "OP_BR";
	case OP_NE:
		return "OP_NE";
	case OP_GE:
		return "OP_GE";
	case OP_GT:
		return "OP_GT";
	case OP_LE:
		return "OP_LE";
	case OP_LT:
		return "OP_LT";
	case OP_EQ:
		return "OP_EQ";
	case OP_LOOP:
		return "OP_LOOP";
	case OP_ZLOOP:
		return "OP_ZLOOP";
	case OP_NZLOOP:
		return "OP_NZLOOP";
	case OP_CAV:
		return "OP_CAV";
	case OP_NULL:
		return "OP_NULL";
	case OP_SET:
		return "OP_SET";
	case OP_AVAR:
		return "OP_AVAR";
	case OP_ERROR:
		return "OP_ERROR";
	case OP_CAT:
		return "OP_CAT";
	case OP_NOT:
		return "OP_NOT";
	case OP_DEBUG:
		return "OP_DEBUG";
	case OP_NEW:
		return "OP_NEW";
	case OP_RECV:
		return "OP_RECV";
	case OP_OBJ:
		return "OP_OBJ";
	case OP_OMV:
		return "OP_OMV";
	case OP_MERGE:
		return "OP_MERGE";
	case OP_DOCALL_T:
		return "OP_DOCALL_T";
	case OP_2STR:
		return "OP_2STR";
	case OP_NEG:
		return "OP_NEG";
	case OP_NEXT:
		return "OP_NEXT";
	case OP_OPEN_POP:
		return "OP_OPEN_POP";
	case OP_OPEN:
		return "OP_OPEN";
	case OP_OUTL:
		return "OP_OUTL";
	case OP_CADD:
		return "OP_CADD";
	case OP_CIN:
		return "OP_CIN";
	case OP_CREMOVE:
		return "OP_CREMOVE";
	case OP_ID:
		return "OP_ID";
	case OP_IDO:
		return "OP_IDO";
	case OP_FIND:
		return "OP_FIND";
	case OP_CLONE:
		return "OP_CLONE";
	case OP_IT:
		return "OP_IT";
	case OP_IKEY:
		return "OP_IKEY";
	case OP_IVAL:
		return "OP_IVAL";
	case OP_INEXT:
		return "OP_INEXT";
	case OP_IPREV:
		return "OP_IPREV";

	default:
		return "OP_ILLEGAL";
	}
}

void _rt_dump_function(task* t, st_ptr* root) {
	st_ptr strings, tmpptr;
	char* bptr, *bptr2;
	int len, tmpint;
	uint opc = 0;
	ushort tmpushort;
	uchar tmpuchar;
	uchar tmpuchar2;

	tmpptr = *root;
	strings = *root;

	puts("BEGIN_FUNCTION/EXPRESSION\nAnnotations:");

	if (!st_move(t, &tmpptr, "A", 2)) // expr's dont have a.
		_cle_read(t, &tmpptr, 0);

	if (st_move(t, &strings, "S", 2)) {
		err(__LINE__);
		return;
	}

	tmpptr = strings;
	//	_cle_read(&tmpptr,0);

	tmpptr = *root;
	if (st_move(t, &tmpptr, "B", 2)) {
		err(__LINE__);
		return;
	} else {
		struct _body_ body;
		if (st_get(t, &tmpptr, (char*) &body, sizeof(struct _body_)) != -2) {
			err(__LINE__);
			return;
		}

		if (body.body != OP_BODY) {
			err(__LINE__);
			return;
		}

		bptr = bptr2 = (char*) tk_malloc(t, body.codesize - sizeof(struct _body_));
		if (st_get(t, &tmpptr, bptr, body.codesize - sizeof(struct _body_)) != -1) {
			tk_mfree(t, bptr);
			err(__LINE__);
			return;
		}

		len = body.codesize - sizeof(struct _body_);
		printf("\nCodesize %d, Params %d, Vars %d, Stacksize: %d, Firsthandler: %d\n", body.codesize, body.maxparams,
				body.maxvars, body.maxstack, body.firsthandler);
	}

	while (len > 0) {
		opc = *bptr;
		printf("%04d  ", (char*) bptr - (char*) bptr2);
		len--;
		bptr++;

		switch (opc) {
		case OP_NOOP:
		case OP_POP:
		case OP_POPW:
		case OP_WIDX:
		case OP_OUT:
		case OP_OUTL:
		case OP_RIDX:
		case OP_ADD:
		case OP_SUB:
		case OP_MUL:
		case OP_DIV:
		case OP_REM:
		case OP_NE:
		case OP_GE:
		case OP_GT:
		case OP_LE:
		case OP_LT:
		case OP_EQ:
		case OP_NULL:
		case OP_SET:
		case OP_CAT:
		case OP_NOT:
		case OP_END:
		case OP_CALL:
		case OP_OBJ:
		case OP_MERGE:
		case OP_NEG:
		case OP_NEXT:
		case OP_OPEN:
		case OP_OPEN_POP:
		case OP_CLONE:
			// emit0
			printf("%s\n", _rt_opc_name(opc));
			break;
			/*		case OP_END:
			 // emit0
			 puts("OP_END\nEND_OF_FUNCTION\n");
			 if(len != 0)
			 printf("!!! Remaining length: %d\n",len);
			 tk_mfree(bptr2);
			 return;
			 */
		case OP_DMVW:
		case OP_MVW:
		case OP_MV:
		case OP_OMV:
		case OP_NEW:
			// emit s
			tmpushort = *((ushort*) bptr);
			bptr += sizeof(ushort);
			printf("%-10s (%d) %s\n", _rt_opc_name(opc), tmpushort, bptr);
			bptr += tmpushort;
			len -= tmpushort + sizeof(ushort);
			break;

		case OP_RECV:
		case OP_SETP:
		case OP_WVAR0:
		case OP_WVAR:
		case OP_RVAR:
		case OP_LVAR:
		case OP_AVAR:
		case OP_DOCALL:
		case OP_DOCALL_N:
		case OP_DOCALL_T:
		case OP_2STR:
			// emit Ic
			tmpuchar = *bptr++;
			printf("%-10s %d\n", _rt_opc_name(opc), tmpuchar);
			len--;
			break;

		case OP_AVARS:
		case OP_OVARS:
		case OP_CADD:
		case OP_CIN:
		case OP_CREMOVE:
		case OP_ID:
		case OP_IDO:
		case OP_FIND:
			// emit Ic
			tmpuchar = *bptr++;
			printf("%-10s %d {", _rt_opc_name(opc), tmpuchar);
			while (tmpuchar-- > 0) {
				printf("%d ", *bptr++);
				len--;
			}
			puts("}");
			len--;
			break;

		case OP_STR:
			// emit Is
			tmpptr = strings;
			if (st_move(t, &tmpptr, bptr, sizeof(ushort)))
				err(__LINE__);
			else {
				char buffer[200];
				uint slen = st_get(t, &tmpptr, buffer, sizeof(buffer));
				printf("%-10s %.*s\n", _rt_opc_name(opc), slen, buffer);
			}
			bptr += sizeof(ushort);
			len -= sizeof(ushort);
			break;

		case OP_DEFP:
			// emit Is2 (branch forward)
			tmpuchar = *bptr++;
			tmpushort = *((ushort*) bptr);
			bptr += sizeof(ushort);
			len -= sizeof(ushort) + 1;
			printf("%-10s %d %04d\n", _rt_opc_name(opc), tmpuchar, tmpushort + (char*) bptr - (char*) bptr2);
			break;

		case OP_BNZ:
		case OP_BZ:
		case OP_BR:
		case OP_DEBUG:
			// emit Is (branch forward)
			tmpushort = *((ushort*) bptr);
			bptr += sizeof(ushort);
			len -= sizeof(ushort);
			printf("%-10s %04d\n", _rt_opc_name(opc), tmpushort + (char*) bptr - (char*) bptr2);
			break;

		case OP_LOOP:
		case OP_ZLOOP:
		case OP_NZLOOP:
		case OP_CAV:
			// emit Is (branch back)
			tmpushort = *((ushort*) bptr);
			bptr += sizeof(ushort);
			len -= sizeof(ushort);
			printf("%-10s %04d\n", _rt_opc_name(opc), (char*) bptr - (char*) bptr2 - tmpushort);
			break;

		case OP_FREE:
			// emit Ic2 (byte,byte)
			tmpuchar = *bptr++;
			tmpuchar2 = *bptr++;
			len -= 2;
			printf("%-10s %d %d\n", _rt_opc_name(opc), tmpuchar, tmpuchar2);
			break;

		case OP_IMM:
			// emit II (imm int)
			tmpint = *((short*) bptr);
			bptr += sizeof(short);
			len -= sizeof(short);
			printf("%-10s %d\n", _rt_opc_name(opc), tmpint);
			break;

		default:
			printf("ERR OPC(%d)\n", opc);
			//			err(__LINE__);
			//			tk_mfree(bptr2);
			//			return;
		}
	}

	tk_mfree(t, bptr2);
	puts("\nEND_OF_FUNCTION\n");
	if (opc != OP_END)
		err(__LINE__);
}

int rt_do_read(task* t, st_ptr root) {
	st_ptr rroot = root;
	char head[2];

	if (st_get(t, &root, head, sizeof(head)) <= 0 && head[0] == 0) {
		switch (head[1]) {
		case 'E':
		case 'M':
			_rt_dump_function(t, &root);
			break;
		case 'I': {
			int tmp;
			if (st_get(t, &root, (char*) &tmp, sizeof(int)) == 0) {
				printf("INT(%d)\n", tmp);
			} else
				printf("Illegal int\n");
		}
			break;
		case 'S': {
			char buffer[256];
			int len = st_get(t, &root, buffer, sizeof(buffer));
			if (len < 0)
				buffer[255] = 0;
			else
				buffer[255 - len] = '\0';
			printf("STR(%s%s)\n", buffer, (len < 0) ? "..." : "");
		}
			break;
		default:
			printf("Can't read this type - use functions. Type: %c\n", head[1]);
		}
	} else
		_cle_read(t, &rroot, 0);

	return 0;
}

// testhandler w any argument
static uint _start2(void* v) {
	printf(" + start2: ");
	return 0;
}
static uint _next2(void* v) {
	printf(" + next2: ");
	return 0;
}
static uint _end2(void* v, cdat c, uint u) {
	printf(" + end2: %s %d \n", c, u);
	return 0;
}
static uint _pop2(void* v) {
	printf(" + pop2: ");
	return 0;
}
static uint _push2(void* v) {
	printf(" + push2: ");
	return 0;
}
static uint _data2(void* v, cdat c, uint u) {
	printf("%.*s", u, c);
	return 0;
}

// defs
cle_pipe _test_pipe_stdout = { _start2, _next2, _end2, _pop2, _push2, _data2, 0 };

/*

 #include "cle_struct.h"
 static void print_struct(page_wrap* pg, const key* me, int ind)
 {
 while(1){
 int i;

 const char* path = KDATA(me);
 int l = me->length;
 int o = me->offset;
 int meoff = (int)((char*)me - (char*)pg->pg);

 for(i = 0; i < ind; i++)
 printf("..");

 if(l == 0)
 {
 ptr* pt = (ptr*)me;

 if(pt->koffset == 0)
 {
 printf("(%s%d)(EXT) page:%p (%d - n:%d) >>\n",(*path & (0x80 >> (o & 7)))?"+":"-",
 pt->offset,pt->pg,meoff,pt->next);
 }
 else
 {
 printf("(%s%d)(INT) page:%p + %d (%d - n:%d) >>\n",(*path & (0x80 >> (o & 7)))?"+":"-",
 pt->offset,pt->pg,pt->koffset,meoff,pt->next);
 }
 }
 else
 {
 int i;

 printf("(%s%d/%d) %s (%d - s:%d n:%d) [",
 (o < l && *path & (0x80 >> (o & 7)))?"+":"-",o,l,"" ,meoff,me->sub,me->next);

 //printf("%s",path);
 for(i = 0; i < (l + 7) >> 3; i++)
 {
 printf(" %x",path[i]);
 }


 if(me->sub){
 puts("] ->\n");
 print_struct(pg,GOOFF(pg,me->sub),ind+1);
 }
 else
 puts("]\n");
 }

 if(!me->next)
 break;

 //if(me == (key*)0x029b7edc)
 //{
 //	l = l;
 //}

 me = GOOFF(pg,me->next);
 }
 }

 */

void map_static_page(page* pgw) {
	int i;
	page* pg = pgw;

	for (i = 0; i < pg->used; i++) {
		uchar c = *((uchar*) pg + i);
		if (i != 0)
			printf(",");
		if ((i & 31) == 31)
			printf("\n");

		if (c >= 'a' && c <= 'z')
			printf("'%c'", c);
		else
			printf("0x%x", c);
	}

	puts("\n");
}
