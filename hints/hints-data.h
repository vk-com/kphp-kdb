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
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#pragma once

#include <aio.h>

#include "net-aio.h"
#include "net-connections.h"
#include "kfs.h"
#include "kdb-hints-binlog.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"

#include "hash_table.h"

#include "hints-tl.h"
#include "hints-interface-structures.h"

#ifdef HINTS
#include "perfect-hashing.h"
#endif

#define TYPE_ID(type,id) ((((long long)(type)) << 50) + (id))
#define TYPE(h) ((int)((h) >> 50))
#define ID(h) (int)(h)

#ifdef NOHINTS
extern int add_on_increment;
#endif
extern int immediate_mode;
extern int index_mode;
extern long max_memory;
extern long static_memory;
extern int cur_users;
extern int index_users, header_size, indexed_users;
extern int dump_mode, dump_type[256];
extern long long friend_changes;
extern long long total_cache_time;
extern int max_cache_time, min_cache_time;
extern int keep_not_alive;
extern int no_changes;

extern int estimate_users;
extern int MAX_CNT;

extern int max_cnt_type[256];

extern int jump_log_ts;
extern long long jump_log_pos;
extern unsigned int jump_log_crc32;
extern char *index_name, *new_binlog_name;

extern int MAX_RATING;

extern int rating_num;
extern int fading;

extern long long *objects_typeids_to_sort;

#ifdef HINTS
extern long long words_per_request[6];
#endif
extern long long bad_requests;

#define NOAIO 0
#define MAX_HISTORY 1000

extern int RATING_NORM;
#define MAX_RATING_NORM 4 * 7 * 24 * 12
#define MIN_RATING (0.15f)
#define MAX_RATING_NUM 8


int check_user_id (int user_id);
int check_type (int type);
int check_object_id (int object_id);
int check_rating (rating val);
int check_rating_num (int num);
int check_text_len (int text_len);


#ifdef HINTS
int do_add_user_object (int user_id, int object_type, long long object_id, int text_len, char *text);
#else
int do_add_user_object (int user_id, int object_type, long long object_id);
#endif
int do_set_user_object_type (int user_id, int object_type, long long object_id, int new_object_type);
#ifdef HINTS
int do_add_object_text (int object_type, long long object_id, int text_len, char *text);
#endif
int do_set_object_type (int object_type, long long object_id, int new_object_type);
int do_del_user_object (int user_id, int object_type, long long object_id);
int do_del_object_text (int object_type, long long object_id);
int do_set_user_info (int user_id, int info);
int do_set_user_object_rating (int user_id, int object_type, long long object_id, float new_rating, int num);
int do_increment_user_object_rating (int user_id, int object_type, long long object_id, int cnt, int num);
int do_nullify_user_rating (int user_id);
int do_set_user_rating_state (int user_id, int state);
int do_user_object_winner (int user_id, int object_type, int num, long long winner, int losers_cnt, int *losers);


int sort_user_objects (int user_id, int object_cnt, long long *obj, int max_cnt, int num, int need_rand);

#ifdef HINTS
//result will be in buf
int get_user_hints (int user_id, int max_buf_len, char *buf, int type, int max_cnt, int num, int need_rating, int need_text, int need_latin, int need_raw_format);
//result will be already outputed
int rpc_get_user_hints (int user_id, int query_len, char *query, int type, int max_cnt, int num, int need_rating, int need_text, int need_latin);
#else
//result will be in buf
int get_user_hints (int user_id, int max_buf_len, char *buf, int type, int max_cnt, int num, int need_rating, int need_rand, int need_raw_format);
//result will be already outputed
int rpc_get_user_hints (int user_id, int exceptions_cnt, long long *exceptions, int type, int max_cnt, int num, int need_rating, int need_rand);
#endif

#ifdef HINTS
int get_user_object_text (int user_id, int type, long long object_id, char **text);
#endif

int get_user_info (int user_id);

int init_hints_data (int schema);

int init_all (kfs_file_handle_t Index);
void free_all (void);

void update_user_info (void);

int get_new_users (void);
long get_changes_memory (void);
long long get_del_by_LRU (void);
int get_global_stats (char *buff);


void test_user_unload (int user_id);

int save_index (void);

//void test_mem (void);
