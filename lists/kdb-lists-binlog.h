/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
              2011-2013 Vitaliy Valtman
*/

#ifndef __KDB_LISTS_BINLOG_H__
#define __KDB_LISTS_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	LISTS_SCHEMA_BASE
#define	LISTS_SCHEMA_BASE	0x6ef20000
#endif

#define	LISTS_SCHEMA_V1		0x6ef20101
#define	LISTS_SCHEMA_V2		0x6ef20201
#define	LISTS_SCHEMA_V3		0x6ef20301

/* sets list of friends of given user, len<256 or arbitrary */
#define	LEV_LI_SET_ENTRY		0x31af5f00
#define	LEV_LI_REPLACE_ENTRY		0x31af5e00
#define	LEV_LI_ADD_ENTRY		0x31af5d00
#define	LEV_LI_SET_ENTRY_EXT		0x7add2300
#define	LEV_LI_ADD_ENTRY_EXT		0x7add2100

#define	LEV_LI_SET_ENTRY_LONG		0x3a6edf00
#define	LEV_LI_REPLACE_ENTRY_LONG      	0x3a6ede00
#define	LEV_LI_ADD_ENTRY_LONG		0x3a6edd00

#define	LEV_LI_SET_ENTRY_TEXT		0x441ee900
#define	LEV_LI_SET_ENTRY_TEXT_LONG		0x5920f9da

#define	LEV_LI_SUBLIST_FLAGS	0x621173ec

#define	LEV_LI_SET_FLAGS		0x593e7000
#define	LEV_LI_INCR_FLAGS		0x3ac21200
#define	LEV_LI_DECR_FLAGS		0x2e179400

#define	LEV_LI_SET_FLAGS_LONG		0x242eb3c9
#define	LEV_LI_CHANGE_FLAGS_LONG	0x496431dd

#define	LEV_LI_SET_VALUE		0x10d3457f
#define	LEV_LI_SET_VALUE_LONG		0x1fbca34d
#define	LEV_LI_INCR_VALUE		0x64a13463
#define	LEV_LI_INCR_VALUE_TINY		0x6a49c700
#define	LEV_LI_INCR_VALUE_LONG		0x6f2e4cfd

#define	LEV_LI_DEL_LIST			0x742a62b5
#define	LEV_LI_DEL_OBJ			0x15aac63d
#define	LEV_LI_DEL_ENTRY		0x4d3afec0

#define	LEV_LI_DEL_SUBLIST		0x148f2de3

#ifdef	VALUES64
# define value_t	long long
#else
# define value_t	int
#endif

#define	VALUE_INTS	(sizeof (value_t) / 4)

#ifdef  LISTS_Z
typedef int *list_id_t;
typedef int *object_id_t;
typedef int alloc_list_id_t[0];
typedef int alloc_object_id_t[0];
typedef int array_object_id_t;
# define LIST_ID_INTS	list_id_ints
# define OBJECT_ID_INTS	object_id_ints
# define LISTS_SCHEMA_CUR		LISTS_SCHEMA_V3
# define MAX_LIST_ID_INTS	16
# define MAX_OBJECT_ID_INTS	16
typedef int var_list_id_t[MAX_LIST_ID_INTS];
typedef int var_object_id_t[MAX_OBJECT_ID_INTS];
#elif defined(LISTS64)
# define list_id_t	long long
# define object_id_t	long long
# define LIST_ID_INTS	2
# define OBJECT_ID_INTS	2
# define LISTS_SCHEMA_CUR		LISTS_SCHEMA_V2
# define alloc_list_id_t	list_id_t
# define alloc_object_id_t	object_id_t
# define array_object_id_t	object_id_t
# define MAX_LIST_ID_INTS	2
# define MAX_OBJECT_ID_INTS	2
# define var_list_id_t	list_id_t
# define var_object_id_t	object_id_t
#else
# define list_id_t	int
# define object_id_t	int
# define LIST_ID_INTS	1
# define OBJECT_ID_INTS	1
# define LISTS_SCHEMA_CUR		LISTS_SCHEMA_V1
# define alloc_list_id_t	list_id_t
# define alloc_object_id_t	object_id_t
# define array_object_id_t	object_id_t
# define MAX_LIST_ID_INTS	1
# define MAX_OBJECT_ID_INTS	1
# define var_list_id_t	list_id_t
# define var_object_id_t	object_id_t
#endif

struct lev_lists_start_ext {
  lev_type_t type;	// LISTS_SCHEMA_V3
  int schema_id;
  int extra_bytes;
  int split_mod;
  int split_min;
  int split_max;
  char kludge_magic;	// 1 if present
  char list_id_ints;
  char object_id_ints;
  char value_ints;	// 1 or 2
  unsigned short extra_mask;
  char str[4];		// extra_bytes, contains: [\x01 char field_bitmask] [<table_name> [\t <extra_schema_args>]] \0
};


struct lev_new_entry {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  int value;
};

struct lev_new_entry_long {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  long long value;
};

struct lev_new_entry_ext {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  int value;
  int extra[4];
};

struct lev_set_entry_text {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  char text[0];
};

struct lev_set_entry_text_long {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  int len;
  char text[0];
};

struct lev_set_flags {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
};

struct lev_set_value {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  int value;
};

struct lev_set_value_long {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  long long value;
};

struct lev_set_flags_long {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  int flags;
};

struct lev_change_flags_long {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
  int and_mask;
  int xor_mask;
};

struct lev_sublist_flags {
  lev_type_t type;
  alloc_list_id_t list_id;
  unsigned char xor_cond, and_cond, and_set, xor_set;
};

struct lev_del_list {
  lev_type_t type;
  alloc_list_id_t list_id;
};

struct lev_del_obj {
  lev_type_t type;
  alloc_object_id_t object_id;
};

struct lev_del_entry {
  lev_type_t type;
  alloc_list_id_t list_id;
  alloc_object_id_t object_id;
};

struct lev_del_sublist {
  lev_type_t type;
  alloc_list_id_t list_id;
  unsigned char xor_cond, and_cond, reserved[2];
};

struct lev_li_generic {
  lev_type_t type;
  int A[0];
};

#pragma	pack(pop)

#endif
