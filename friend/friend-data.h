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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
              2011-2013 Vitaliy Valtman
*/

#ifndef __FRIENDS_DATA_H__
#define __FRIENDS_DATA_H__

#include "kdb-data-common.h"
#include "kdb-friends-binlog.h"
#include "kfs.h"

#define	MAX_USERS	(1 << 21)
#define	MAX_FRIENDS	10000

#pragma	pack(push,4)
int init_friends_data (int schema);

extern int alloc_tree_nodes, free_tree_nodes;

typedef struct tree tree_t;

struct tree {
  tree_t *left, *right;
  int x, y;
  int cat, date;
};

extern int alloc_rev_friends_nodes;
typedef struct rev_friends rev_friends_t;

struct rev_friends {
  rev_friends_t *left, *right;
  int x1, x2, y;
  int date;
};

extern int privacy_nodes, tot_privacy_len;

typedef struct privacy privacy_t;

struct privacy {
  privacy_t *left, *right;
  privacy_key_t x;
  int y;
  int List[0];
};

extern int tot_users, max_uid;

typedef struct user user_t;

struct user {
  int user_id;
  int fr_cnt;
  tree_t *fr_tree, *req_tree, *req_time_tree;
  privacy_t *pr_tree;
  int req_cnt;
  int fr_last_date;
  int req_last_date;
  int cat_mask;
  int cat_ver[32];
};

typedef struct {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  int log_timestamp;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  int log_split_min;
  int log_split_max;
  int log_split_mod;
  int tot_users;
} index_header;
#define FRIEND_INDEX_MAGIC 0x67f8ab73
#define REVERSE_FRIEND_INDEX_MAGIC 0x523087f6

extern int ignored_delete_user_id;

int do_delete_user (int user_id);
int do_add_friend (int user_id, int friend_id, int cat_xor, int cat_and, int force);
int do_delete_friend (int user_id, int friend_id);
int do_set_category_friend_list (int user_id, int cat, int *List, int len);
int do_delete_friend_category (int user_id, int cat);

int do_add_friend_request (int user_id, int friend_id, int cat, int force);
int do_delete_friend_request (int user_id, int friend_id);
int do_delete_all_friend_requests (int user_id);

int do_set_privacy (int user_id, privacy_key_t privacy_key, const char *text, int len, int force);
int do_delete_privacy (int user_id, privacy_key_t privacy_key);


#define	MAX_RES	65536
extern int R[MAX_RES];
extern int *R_end;

int get_friend_request_cat (int user_id, int friend_id);
int get_friend_cat (int user_id, int friend_id);
void get_common_friends_num (int user_id, int user_num, const int *userlist, int *resultlist);
int get_common_friends (int user_id, int user_num, const int *userlist, int *resultlist, int max_result);

int prepare_friends (int user_id, int cat_mask, int mode);
int prepare_recent_friends (int user_id, int num);
int prepare_friend_requests (int user_id, int num);

int prepare_privacy_str (char *buff, int user_id, privacy_key_t privacy_key);

int check_privacy (int checker_id, int user_id, privacy_key_t privacy_key);


int conv_uid (int user_id);

int save_index (int writing_binlog);
int load_index (kfs_file_handle_t Index);

#pragma	pack(pop)
#endif
