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
#ifndef __CLE_PAGESOURCE_H__
#define __CLE_PAGESOURCE_H__

typedef void* cle_pageid;
typedef void* cle_psrc_data;

typedef struct page {
	cle_pageid id;
	unsigned short size;
	unsigned short used;
	unsigned short waste;
	//short data[0];
} page;

typedef struct cle_pagesource {
	page* (*new_page)(cle_psrc_data);
	page* (*read_page)(cle_psrc_data, cle_pageid);
	page* (*root_page)(cle_psrc_data);
    
	void (*write_page)(cle_psrc_data, cle_pageid, page*);
	void (*remove_page)(cle_psrc_data, cle_pageid);
	void (*unref_page)(cle_psrc_data, page*);
	int (*pager_error)(cle_psrc_data);
	int (*pager_commit)(cle_psrc_data, page*);
	int (*pager_rollback)(cle_psrc_data);
	int (*pager_close)(cle_psrc_data);
	cle_psrc_data (*pager_clone)(cle_psrc_data);
} cle_pagesource;

#endif
