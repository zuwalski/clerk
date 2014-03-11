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
#ifndef __CLE_COMPILE_H__
#define __CLE_COMPILE_H__

#include "cle_clerk.h"
#include "cle_stream.h"

int cmp_method(task* t, st_ptr* dest, st_ptr* body, cle_pipe* response, void* data, const uint is_handler);

int cmp_expr(task* t, st_ptr* dest, st_ptr* body, cle_pipe* response, void* data);

#endif
