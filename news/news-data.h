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
              2010-2011 Nikolai Durov
              2010-2011 Andrei Lopatin
              2011-2013 Vitaliy Valtman
              2011-2013 Anton Maydell
*/

#ifndef __NEWS_DATA_H__
#define __NEWS_DATA_H__

#include "kdb-data-common.h"
#include "kdb-news-binlog.h"
#include "kfs.h"

#define	MAX_USERS	(1 << 22)
//#define	PLACES_HASH	1719239
#define PLACES_HASH	10000019
#define RECOMMEND_PLACES_HASH 52561

int init_news_data (int schema);

extern int tot_users, tot_places, max_uid, ug_mode, items_kept, comments_kept;

extern int max_news_days, min_logevent_time;

#define	MAX_NEWS_DAYS	30
#define	MAX_USER_ITEMS	5000
#define	MAX_PLACE_COMMENTS	200

#define	USER_TYPES_MASK		0x000fffff
#define	GROUP_TYPES_MASK	0x001fffff
#define	COMMENT_TYPES_MASK	0x01f00000
#define NOTIFY_TYPES_MASK 0xffffffff
#define RECOMMEND_TYPES_MASK 0xffffffff

#define MAX_SMALL_BOOKMARK 4
#define DEFAULT_MAX_ALLOCATED_METAFILES_SIZE 500000000
#define MIN_MAX_ALLOCATED_METAFILES_SIZE     100000000
#define RECOMMEND_FIND_ITEM_DUPS_STEPS 1000

#define COMM_MODE (ug_mode == 1)
#define UG_MODE (ug_mode == 0 || ug_mode == -1)
#define NOTIFY_MODE (ug_mode == 2)
#define RECOMMEND_MODE (ug_mode == 3)

typedef struct news_item item_t;

struct news_item {
  item_t *next, *prev;
  int type;
  int date;
  int random_tag;
  int user;
  int group;
  int owner;
  int place;
  int item;
};

typedef struct user user_t;

struct user {
  item_t *first, *last;
  int user_id;
  int priv_mask;
  int tot_items;
};

typedef struct news_comment comment_t;

struct news_comment {
  comment_t *next, *prev;
  int date;
  int random_tag;
  int user;
  int group;
  int item;
};

typedef struct comment_place place_t;

struct comment_place {
  comment_t *first, *last;
  int type;
  int place;
  int tot_comments;
  int owner;
  place_t *hnext;
};

typedef struct news_notify notify_t;

struct news_notify {
  notify_t *next, *prev;
  int date;
  int random_tag;
  int item;
  int user;
};

typedef struct userplace userplace_t;
struct userplace {
  notify_t *first, *last;
  userplace_t *unext, *uprev;
  userplace_t *pnext, *pprev;
  userplace_t *hnext;
  int type;
  int owner;
  int place;
  int user_id;
  int total_items;
  int allocated_items;
};

typedef struct notify_user notify_user_t;
struct notify_user {
  userplace_t *first, *last;
  int user_id;
  int priv_mask;
  int total_items;
  int allocated_items;
};

typedef struct notify_place notify_place_t;
struct notify_place {
  userplace_t *first, *last;
  int type;
  int place;
  int total_items;
  int owner;
  place_t *hnext;
  int allocated_items;
};

typedef struct recommend_item recommend_item_t;
struct recommend_item {
  recommend_item_t *next, *prev;
  int owner;
  int place;
  //int item;
  int item_creation_time;
  int date;
  short type;
  short action;
};

typedef struct recommend_user recommend_user_t;
struct recommend_user {
  recommend_item_t *first, *last;
  int user_id;
  int total_items;
};

typedef struct recommend_place recommend_place_t;

struct recommend_place {
  double weight;
  recommend_place_t *hnext;
  int type;
  int place;
  int owner;
  int item_creation_time;
  int last_user, actions_bitset; /* used for removing duplicates (user, type, owner, place, actions) */
  int users;
};

int do_delete_user (int user_id);
int do_process_news_item (int user_id, int type, int user, int group, int owner, int place, int item);
int do_set_privacy (int user_id, int mask);

int do_delete_place (int type, int owner, int place);
int do_process_news_comment (int type, int user, int group, int owner, int place, int item);

int do_delete_comment (int type, int owner, int place, int item);
int do_undelete_comment (int type, int owner, int place, int item);

int do_delete_user_comment (int user_id, int type, int owner, int place, int item);
int do_undelete_user_comment (int user_id, int type, int owner, int place, int item);

int do_process_news_notify (int user_id, int type, int user, int owner, int place, int item);
int do_process_news_recommend (int user_id, int type, int owner, int place, int action, int item, int item_creation_time);
int do_set_recommend_rate (int type, int action, double rate);
int get_recommend_rate (int type, int action, double *rate);
#define MAX_GROUPS 65536
#define	MAX_RES	(MAX_GROUPS * 9)
extern int R[MAX_RES];
extern int *R_end;

void clear_result_buffer (void);
int prepare_raw_updates (int user_id, int mask, int start_time, int end_time);
int prepare_raw_comm_updates (int type, int owner, int place, int start_time, int end_time);
int prepare_raw_notify_updates (int user_id, int mask, int start_time, int end_time, int extra);
int recommend_prepare_raw_updates (int *Q, int QL, int mask, int st_time, int end_time, int excluded_user_id, int timestamp, int T);
int get_privacy_mask (int user_id);

extern long long garbage_objects_collected, garbage_users_collected, items_removed_in_process_new, items_removed_in_prepare_updates, dups_removed_in_process_raw_updates, dups_users_removed_from_urlist;
int news_collect_garbage (int steps);


#define NEWS_INDEX_MAGIC 0x9723fac3
#pragma	pack(push,4)

struct bookmark {
  long long value;
  int next;
  int y;
};

struct bookmark_user {
  int user_id;
  int offset;
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

  int ug_mode;
  int allowed_types_mask;
  int log_split_min;
  int log_split_max;
  int log_split_mod;
  int last_day;

  int small_users;
  int large_users;
  long long small_data_offset;
  long long large_data_offset;
} index_header;

struct metafile {
  struct aio_connection *aio;
  char *data;
  int next;
  int prev;
};

#pragma	pack(pop)

void update_offsets (int writing_binlog);
int save_index (int writing_binlog);
int load_index (kfs_file_handle_t Index);
int get_bookmarks (int user_id, int mask, int *Q, int max_res);
int do_add_del_bookmark (int user_id, int type, int owner, int place, int y);
int check_split (int n);
double new_users_perc (void);
int conv_uid (int user_id);

#endif
