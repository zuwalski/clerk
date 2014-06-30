/*
 Clerk application and storage engine.
 Copyright (C) 2009  Lars Szuwalski

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
 Creating instances and managing content
 */

#ifndef __CLE_OBJECT_H__
#define __CLE_OBJECT_H__

#include "cle_clerk.h"

typedef struct {
	task* t;
	st_ptr root;
} cle_instance;

//root-headers \0\0[Identificer]
#define IHEAD_SIZE 3	//(sizeof(segment) + 1)
#define HEAD_NAMES ((cdat)"\0\0n")

#define HEAD_EVENT ((cdat)"\0\0e")

//object-headers have size 1 ([Identifier])
#define HEAD_SIZE 2
#define HEAD_OBJECTS ((cdat)"\0o")

#define HEAD_HANDLER ((cdat)"\0h")
#define HEAD_ROLES ((cdat)"\0r")
#define HEAD_LINK ((cdat)"\0l")

// properties
enum property_type {
	TYPE_ILLEGAL = 0,
	TYPE_STATE,
	TYPE_HANDLER,
	TYPE_METHOD,
	TYPE_EXPR,
	TYPE_ANY,
	TYPE_NUM,
	TYPE_REF,
	TYPE_REF_MEM,
	TYPE_COLLECTION,
	TYPE_DEPENDENCY
};

enum fixed_identities {
	SYS_NAMES = 0, SYS_DEV, SYS_STATE, F_INIT, F_TOSTRING, F_MSG_HANDLER
};

typedef ushort segment;

#define OID_HIGH_SIZE 4
typedef struct {
	segment _low;
	uchar _high[OID_HIGH_SIZE];
} oid;

extern const oid NOOID;

typedef struct {
	char chrs[sizeof(oid) * 2 + 1];
} oid_str;

typedef ulong identity;
// identity == 0 => not valid!

typedef struct {
	oid oid;
	identity handler;
	uchar type;
} cle_handler;

typedef struct {
	identity id;
	uchar type;
} cle_typed_identity;

typedef void* cle_obj_target_ctx;

typedef struct {
	int (*commit_begin)(cle_obj_target_ctx);
	int (*commit_done)(cle_obj_target_ctx);
	int (*commit_fail)(cle_obj_target_ctx);

	int (*commit_deletes)(cle_obj_target_ctx);
	int (*commit_inserts)(cle_obj_target_ctx);

	unsigned int (*commit_push)(cle_obj_target_ctx);
	unsigned int (*commit_pop)(cle_obj_target_ctx);
	unsigned int (*commit_data)(cle_obj_target_ctx, cdat, uint, uint);
} cle_object_target;

int cle_scan_validate(task* t, st_ptr* from, int (*fun)(void*, uchar*, uint), void* ctx);

int cle_new(cle_instance inst, st_ptr name, st_ptr extends, st_ptr* obj);

void cle_new_mem(task* app_instance, st_ptr extends, st_ptr* newobj);

int cle_goto_object(cle_instance inst, st_ptr name, st_ptr* obj);

int cle_goto_id(cle_instance inst, st_ptr* obj, oid id);

oid cle_oid_from_cdat(cle_instance inst, cdat name, uint length);

int cle_get_oid_str(cle_instance inst, st_ptr obj, oid_str* buffer);

oid cle_get_oid(cle_instance inst, st_ptr obj);

int cle_goto_parent(cle_instance inst, st_ptr* child);

int cle_is_related_to(cle_instance inst, st_ptr parent, st_ptr child);

int cle_delete_name(cle_instance inst, st_ptr name);

int cle_create_state(cle_instance inst, st_ptr obj, st_ptr newstate);

int cle_set_state(cle_instance inst, st_ptr obj, st_ptr state);

int cle_create_property(cle_instance inst, st_ptr obj, st_ptr propertyname, identity* id);

//int cle_create_expr(cle_instance inst, st_ptr obj, st_ptr path, st_ptr expr, cle_pipe* response, void* data);

int cle_get_handler(cle_instance inst, cle_handler href, st_ptr* obj, st_ptr* handler);

int cle_get_property_host(cle_instance inst, st_ptr* obj, cdat str, uint length);

int cle_get_property_host_st(cle_instance inst, st_ptr* obj, st_ptr propname);

int cle_probe_identity(cle_instance inst, st_ptr* reader, cle_typed_identity* id);

int cle_identity_value(cle_instance inst, identity id, st_ptr obj, st_ptr* value);

int cle_get_property_ref(cle_instance inst, st_ptr obj, identity id, st_ptr* ref);

int cle_get_property_ref_value(cle_instance inst, st_ptr prop, st_ptr* ref);

int cle_get_property_num(cle_instance inst, st_ptr obj, identity id, double* dbl);

int cle_get_property_num_value(cle_instance inst, st_ptr prop, double* dbl);

int cle_set_property_ref(cle_instance inst, st_ptr obj, identity id, st_ptr ref);

int cle_set_property_ptr(cle_instance inst, st_ptr obj, identity id, st_ptr* ptr);

int cle_set_property_num(cle_instance inst, st_ptr obj, identity id, double dbl);

enum property_type cle_get_property_type(cle_instance inst, st_ptr obj, identity id);

enum property_type cle_get_property_type_value(cle_instance inst, st_ptr prop);

int cle_commit_objects(cle_instance inst, cle_object_target* src, cle_obj_target_ctx sdat);

#endif
