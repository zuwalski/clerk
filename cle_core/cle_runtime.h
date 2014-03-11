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
#ifndef __CLE_RUNTIME_H__
#define __CLE_RUNTIME_H__

#include "cle_clerk.h"

#define ERROR_MAX 20

enum cle_opcode
{
	OP_ILLEGAL = 0,
	OP_NOOP,
	OP_FREE,
	OP_STR,
	OP_CALL,
	OP_SETP,
	OP_DOCALL,
	OP_DOCALL_N,
	OP_AVAR,
	OP_AVARS,
	OP_OVARS,
	OP_LVAR,
	OP_POP,
	OP_POPW,
	OP_WIDX,
	OP_WVAR,
	OP_WVAR0,
	OP_DMVW,
	OP_MVW,
	OP_OUT,
	OP_OUTL,
	OP_RIDX,
	OP_RVAR,
	OP_MV,
	OP_DEFP,
	OP_BODY,
	OP_END,
	OP_ADD,
	OP_SUB,
	OP_MUL,
	OP_DIV,
	OP_REM,
	OP_IMM,
	OP_BNZ,
	OP_BZ,
	OP_BR,
	OP_GE,
	OP_NE,
	OP_GT,
	OP_LE,
	OP_LT,
	OP_EQ,
	OP_LOOP,
	OP_ZLOOP,
	OP_NZLOOP,
	OP_CAV,
	OP_NULL,
	OP_SET,
	OP_ERROR,
	OP_CAT,
	OP_NOT,
	OP_DEBUG,
	OP_NEW,
	OP_CLONE,

	OP_RECV,
	OP_OBJ,
	OP_OMV,
//	OP_NUM,
	OP_MERGE,
	OP_DOCALL_T,
	OP_SUPER,
	OP_2STR,
	OP_NEG,
	OP_NEXT,
	OP_OPEN,
	OP_OPEN_POP,

	OP_CADD,
	OP_CIN,
	OP_CREMOVE,
	OP_ID,
	OP_IDO,
	OP_FIND,

	OP_IT,
	OP_IKEY,
	OP_IVAL,
	OP_INEXT,
	OP_IPREV,

	OP_OP_MAX
};

struct _body_
{
	char body;
	uchar maxparams;
	uchar maxvars;
	uchar maxstack;
	ushort codesize;
	ushort firsthandler;
};

#endif
