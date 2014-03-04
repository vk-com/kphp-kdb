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

#ifndef __LISTS_INDEX_LAYOUT_H__
#define	__LISTS_INDEX_LAYOUT_H__

#ifdef LISTS64
#define	LISTS_INDEX_MAGIC	0x11efabef
#define	LISTS_INDEX_MAGIC_NEW	0x11efabef
#define LISTS_INDEX_MAGIC_NO_REVLIST 0x11efabee
typedef struct {
  unsigned long long list_id;
  long long object_id;
} ltree_x_t;
#define var_ltree_x_t	ltree_x_t
#elif (defined (LISTS_Z))
#define	LISTS_INDEX_MAGIC	0x11effeba
#define	LISTS_INDEX_MAGIC_NEW	0x11efbefa
#define	LISTS_INDEX_MAGIC_NO_REVLIST	0x11efbefb
typedef int ltree_x_t[0];
typedef int var_ltree_x_t[MAX_OBJECT_ID_INTS + MAX_LIST_ID_INTS];
#else
#define	LISTS_INDEX_MAGIC	0x11effeba
#define	LISTS_INDEX_MAGIC_NEW	0x11efbefa
#define	LISTS_INDEX_MAGIC_NO_REVLIST	0x11efbefb
typedef long long ltree_x_t;
#define var_ltree_x_t	ltree_x_t
#endif

#define	MAX_SUBLISTS		8
#include "lists-data.h"

#pragma	pack(push,4)

struct lists_index_header {
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  unsigned long long slice_hash;
  int log_timestamp;
  int log_split_min, log_split_max, log_split_mod;
  long long list_index_offset;
  long long list_data_offset;
  long long revlist_data_offset;
  long long extra_data_offset;
  long long data_end_offset;
  int tot_lists;
  global_id_t last_global_id;
  unsigned log_pos0_crc32;
  unsigned log_pos1_crc32;
  int idx_mode;

  char kludge_magic;	// 1 if present
  char list_id_ints;
  char object_id_ints;
  char value_ints;	// 1 or 2
  unsigned params;
  int tot_revlist_metafiles;

  int reserved[29];
  unsigned revlist_arrays_crc32;
  unsigned filelist_crc32;
  unsigned header_crc32;
};

/* for idx_mode */
#define	LIDX_HAVE_BIGIDS	1
#define	LIDX_HAVE_BIGVALUES	2

/* for flags in file_list_header */
#define	LF_BIGFLAGS	1
#define	LF_HAVE_VALUES	2
#define	LF_HAVE_DATES	4
#define	LF_HAVE_TEXTS	8

#define PARAMS_HAS_REVLIST_OFFSETS 1
#define PARAMS_HAS_CRC32 2

/* kept in list_index_offset .. list_data_offset, at most tot_lists+1 entries */
struct file_list_index_entry {
  alloc_list_id_t list_id;
  int flags;
  long long list_file_offset;
};

#define FILE_LIST_MAGIC 0x11efc7c8

typedef struct file_list_header metafile_t;

/* kept in list_data_offset .. revlist_data_offset */
struct file_list_header {
  int magic;
  alloc_list_id_t list_id;
  int flags;
  union {
    int sublist_size_cum[9];			// sublist_size_cum[k] = size(sublist[0]) + ... + size(sublist[k-1])
    struct {
      int for_no_warnings[8];  
      int tot_entries;
    };
  };
  int data[0];
/* 
  object_id_t obj_id_list[tot_entries];		// 0. object_id[temp_id], 0<=temp_id<tot_entries, sorted
  int global_id_list[tot_entries];		// 1. global_id[temp_id]
  int sorted_global_id_list[tot_entries];	// 2. list of temp_id's ordered by corresp. global_id
  int sublist_temp_id_list[tot_entries];	// 3. temp_id's of each sublist, written one after another
  int sublist_temp_id_by_global[tot_entries];	// 4. same, but each sublist ordered by global_id
  int values[tot_entries];			// 5. if LF_HAVE_VALUES set
  long long values[tot_entries]			// 5b. if LF_HAVE_VALUES and LIDX_HAVE_BIGVALUES set
  int dates[tot_entries];			// 6. if LF_HAVE_DATES set
  int text_offset[tot_entries+1];		// 7. if LF_HAVE_TEXTS set
  unsigned char obj_flags[tot_entries];		// 8. if LF_BIGFLAGS not set
  int obj_flags[tot_entries];			// 8. if LF_BIGFLAGS set
*/
};

/* revlist_data_offset .. extra data offset contains at most last_global_id 8-byte entries */

#ifdef LISTS_Z
typedef int file_revlist_entry_t;
#else
typedef union file_revlist_entry {
  ltree_x_t pair;
  struct {
    unsigned list_id_t list_id;
    object_id_t object_id;
  };
} file_revlist_entry_t;
#endif

/* extra_data_offset .. data_end_offset contains last_global_id file_list_extras */
/* for now, extra_data_offset = data_end_offset */

struct file_list_extras {
  alloc_list_id_t list_id;
  unsigned ip;
  int port;
  unsigned front;
  unsigned long long ua_hash;
};

#pragma	pack(pop)

#endif
