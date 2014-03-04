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

    Copyright 2010-2013	Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/
#ifndef _BAYES_DATA_H_
#define _BAYES_DATA_H_

#include <aio.h>

#include "net-aio.h"
#include "net-connections.h"
#include "kdb-mf-binlog.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"

#include "utils.h"
#include "maccub.h"
#include "hash_table.h"

extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;

extern int index_mode, dump_mode, binlog_readed;
extern long max_memory;
extern int cur_users;
extern int index_users, header_size;
extern long long allocated_metafile_bytes;
extern int all_sugg_cnt;

extern int jump_log_ts;
extern long long jump_log_pos;
extern unsigned int jump_log_crc32;

extern char *newindexname, *suggname;

#define NOAIO 0

#define MEMORY_CHANGES_PERCENT (0.75)
#define MEMORY_USER_PERCENT (0.25)

#define MAX_CNT 1000

#define MAX_EXCEPTIONS 20000

#define MAX_SUGGESTIONS 400

int init_mf_data (int schema);

void init_all (char *indexname);
void free_all (void);

int add_common_friends (int uid, int add, int *a, int an);
int do_add_exception (int uid, int fid);
int do_del_exception (int uid, int fid);
int do_clear_exceptions (int uid);
int get_suggestions (int uid, int mx_cnt, int min_common, int *fr_buff);
void test_user_unload (int uid);
int user_LRU_unload (void);

long long get_del_by_LRU (void);

typedef struct userx user;

struct userx {
  char *metafile;
  int metafile_len;
  changes new_exceptions;

  struct aio_connection *aio;

  treap sugg;

  // LRU
  user *next_used, *prev_used;
};

#pragma pack(push,4)

typedef struct {
  int id;
  int size;
  long long shift;
} user_index_data;

typedef struct {
/* strange numbers */
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
  int last_global_id;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  int reserved[36];

  int user_cnt;
  user_index_data *user_index;
} index_header;

#pragma pack(pop)

void save_index (char *indexname);
int load_header (char *indexname);
void free_header (index_header *header);
void load_suggestions (char *suggname);

#endif
