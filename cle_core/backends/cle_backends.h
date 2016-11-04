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

#ifndef __CLE_BACKENDS_H__
#define __CLE_BACKENDS_H__

#include "../cle_source.h"

#define MEM_PAGE_SIZE (1024*4)
#define PAGER_MAGIC 0x240673

extern cle_pagesource util_memory_pager;

cle_psrc_data util_create_mempager();

int mempager_get_pagecount(cle_psrc_data);

void mempager_destroy(cle_psrc_data);

// log-pager
extern cle_pagesource util_memory_log;

cle_psrc_data util_create_memlog();

int memlog_get_pagecount(cle_psrc_data pd);

int memlog_get_logcount(cle_psrc_data pd);

void memlog_destroy(cle_psrc_data pd);

#endif
