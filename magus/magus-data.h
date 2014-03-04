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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#pragma once

#include "net-aio.h"
#include "net-connections.h"
#include "kfs.h"
#include "kdb-magus-binlog.h"

#include "magus-precalc.h"

#define MAGUS_INDEX_MAGIC 0x77d7eda3

#define MAX_MEMORY 2000000000
#define MEMORY_USER_PERCENT 0.75

extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;

extern int index_mode;
extern long long max_memory;
extern int cur_users;
extern int index_users, header_size;
extern int binlog_readed;

extern int types[256];
extern int dumps[256];

extern int jump_log_ts;
extern long long jump_log_pos;
extern unsigned int jump_log_crc32;

#define NOAIO 0


#define MIN_OBJS 5
#define MAX_OBJS 100
#define MAX_RES 400
#define MAX_FIDS FIDS_LIMIT
#define MAX_EXCEPTIONS 50000
#define MAX_OBJS_LEN 100000

#if MAX_RES + 1 > MAX_EXCEPTIONS || MAX_RES < 0
#  error Wrong MAX_RES chosen
#endif

int init_magus_data (int schema);

int init_all (kfs_file_handle_t Index);
void free_all (void);

#pragma pack(push,4)

typedef struct userx user;

struct userx {
  int id, local_id;

// Exceptions are stored here
  char *metafile;
  int metafile_len;

// There is no any precalculated suggestions it's too expensive
// int suggs_n;
// score *suggs;

// It seems that this is also too expensive and unuseful, not supported now
//  int objs_n;
//  score *objs;

  hset_int new_exceptions;

  struct aio_connection *aio;
  user *next_used, *prev_used;
};

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
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  int multiple_types;

  int reserved[38];

  int user_cnt;
  user_index_data *user_index;
} index_header;

#pragma pack(pop)

int save_index (void);

int do_add_exception (int uid, int type, char *fid);

int get_objs_hints (int user_id, int type, int fn, char *user_objs, char *res);
int get_objs (int user_id, int type, int cnt, char *res);

void load_similarity (int t);
void load_dump (int t);

#undef DL_HEADER
