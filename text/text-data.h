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
*/

#ifndef __TEXT_DATA_H__
#define __TEXT_DATA_H__

#include "kdb-data-common.h"
#include "kdb-text-binlog.h"
#include "text-index-layout.h"
#include "net-buffers.h"
#include "net-aio.h"
#include "kfs.h"

#define	METAFILE_ALLOC_THRESHOLD	(1 << 30)
#define	MAX_METAFILE_SIZE	(64 << 20)

#if	0
#define	MAX_USERS_NUM	(1 << 19)
#define	USERS_PRIME	1000003
#define MAX_USERS	USERS_PRIME
#else
#define	MAX_USERS	(1 << 21)
#define MAX_USERS_NUM	MAX_USERS
#endif

#define	NEGATIVE_USER_OFFSET	(1 << 20)
#define	MAX_RES		65536
#define	MAX_TMP_RES		(1 << 20)

#define	MAX_PERSISTENT_HISTORY_EVENTS	(MAX_TMP_RES / 4)

#define	MAX_INS_TAGS	8
#define	HISTORY_EVENTS	256

#define	CYCLIC_IP_BUFFER_SIZE	(1 << 20)

#define	MAX_USER_FRIENDS	MAX_RES
#define	DEFAULT_HOLD_ONLINE_TIME	900
#define	MIN_HOLD_ONLINE_TIME	20
#define	MAX_HOLD_ONLINE_TIME	3600

#define	MAX_PEER_ID	0x7fffffff

#define	MAX_USER_INCORE_MESSAGES	(1 << 16)

#define	MAX_SEARCH_UNPACKED_MESSAGES	5000
#define	MAX_SEARCH_SCANNED_INCORE_MESSAGES	5000

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern int idx_users, idx_bytes, idx_loaded_bytes, idx_last_global_id, idx_crc_enabled;

extern int text_shift;	/* use msg->text + text_shift for message_t *msg */
extern int read_extra_mask, index_extra_mask, write_extra_mask;

extern int search_enabled, use_stemmer, hashtags_enabled, searchtags_enabled, persistent_history_enabled;
extern int hold_online_time;

extern int use_aio;
extern long long aio_read_errors, aio_crc_errors;

int load_index (kfs_file_handle_t Index);
void output_large_metafiles (void);

int init_text_data (int schema);

extern int tree_nodes, online_tree_nodes, incore_messages;
extern int msg_search_nodes, edit_text_nodes;
extern long msg_search_nodes_bytes, edit_text_bytes;
extern int tot_users, last_global_id;
extern int history_enabled;
extern int alloc_history_strings;
extern long long alloc_history_strings_size;

extern int incore_persistent_history_lists, incore_persistent_history_events;
extern long long incore_persistent_history_bytes;

extern int cur_user_metafiles, tot_user_metafiles;
extern long long cur_user_metafile_bytes, tot_user_metafile_bytes, allocated_metafile_bytes, metafile_alloc_threshold;
extern int cur_search_metafiles, tot_search_metafiles;
extern long long cur_search_metafile_bytes, tot_search_metafile_bytes, allocated_search_metafile_bytes;
extern int cur_history_metafiles, tot_history_metafiles;
extern long long cur_history_metafile_bytes, tot_history_metafile_bytes, allocated_history_metafile_bytes;

//extern struct aio_connection *WaitAio, *WaitAio2, *WaitAio3;

typedef struct tree tree_t;
typedef struct ltree ltree_t;
typedef struct tree_ext tree_ext_t;
typedef struct tree_num tree_num_t;
typedef struct stree stree_t;
typedef struct user user_t;
typedef struct message message_t;
typedef struct edit_text edit_text_t;

#define	TF_PLUS		1	// new message, message_t *msg
#define	TF_ZERO		0	// old message, this node just contains new flags
#define	TF_MINUS	3	// old message deleted
#define	TF_REPLACED	2	// old message replaced with new, message_t *msg
#define	TF_ZERO_PRIME	4	// old message, this node contains a pointer to struct delayed_value_data with new flags and values

/* for peer ids tree & changed messages tree */
struct tree {
  tree_t *left, *right;
  int x;
  int y;
  union {
    void *data;
    struct value_data *value;
    message_t *msg;
    edit_text_t *edit_text;
    int flags;
    int z;
    struct { unsigned short set_mask, clear_mask; };
  };
};

struct ltree {
  ltree_t *left, *right;
  long long x;
  int y;
  int z;
};

#pragma pack(push,1)

/* void *data for delayed_values_tree points to struct value_data, its size is determined by fields_mask */
struct value_data {
  union {
    unsigned short zero_mask;
    unsigned short flags;
  };
  unsigned short fields_mask;
  union {
    int data[2];
    long long ldata[1];
  };
};

#pragma pack(pop)

/* for top msg tree */
struct tree_num {
  tree_num_t *left, *right;
  int x;
  int y;
  int N;
  int z;
};

#define	COMBINE_CLEAR_SET(_clear,_set)	( (((unsigned)(_clear)) << 16) | ((unsigned) (_set)) )

struct tree_ext {
  tree_ext_t *left, *right;
  int x;
  int y;
  int rpos;		/* # of (A_i >= x) in related list, i.e. A[N-i-1]<x<=A[N-i] */
  int delta;		/* sum of deltas for subtree, this node included */
};

typedef struct tree_and_list listree_t;

struct tree_and_list {
  tree_ext_t *root;
  int *A;		/* A[0] < A[1] < ... < A[N-1] <= last_A;
  			   A may be zero if the list is not in memory */
  int N;
  int last_A;
};

struct stree {
  stree_t *left, *right;
  int x;
  int y;
};

struct msg_search_node {
  struct msg_search_node *prev;
  int local_id;
  int words_num;
  hash_t words[0];
};

#define MIN_PERSISTENT_HISTORY_EVENTS	8

struct incore_persistent_history {
  int alloc_events;
  int cur_events;
  int history[0];
};

#define USER_CONN(U) ((struct conn_query *) U)
#define USER_PCONN(U) ((struct conn_query *) &U->last_local_id)

struct user {
  struct core_metafile *mf, *search_mf;
  int user_id;
  int user_flags;
  struct conn_query *first_q, *last_q;	/* must be here ! */
  struct file_user_list_entry *dir_entry;
  tree_t *msg_tree;
  tree_t *peer_tree;
  ltree_t *legacy_tree;
  tree_t *delayed_tree;
  tree_t *delayed_value_tree;
  tree_num_t *topmsg_tree;		/* x = max(local_id for fixed peer_id), flags = peer_id */
  stree_t *online_tree;
  tree_t *edit_text_tree;
  struct msg_search_node *last;
  int last_local_id;
  int history_ts;
  int *history;
  struct core_metafile *history_mf;
  struct conn_query *first_pq, *last_pq;  /* must be after two int's and two pointers */
  struct incore_persistent_history *persistent_history;
  int persistent_ts;
  int cur_insert_tags;
  int insert_tags[MAX_INS_TAGS][2];
  char secret[8];
//  int max_delayed_local_id;
  listree_t Sublists[0];
};

#define	MF_USER		0xe5072393
#define	MF_SEARCH	0x278a4bf4
#define	MF_HISTORY	0x53eebea4

typedef struct core_metafile core_mf_t;

struct core_metafile {
  core_mf_t *next, *prev;
  user_t *user;
  struct aio_connection *aio;
  int mf_type;
  int len;
  char data[0];
};

struct metafile_queue {
  core_mf_t *first, *last;
};



user_t *get_user (int user_id);
user_t *get_user_f (int user_id);

extern int extra_fields_num;
extern int extra_fields_mask, future_fields_mask;

struct message {
  int flags;
  int date;
  int user_id;
  int peer_id;
  int global_id;
  long long legacy_id;
  int peer_msg_id;
  int len;
  int kludges_size;
  int extra[0];		// extra_fields_num
  char text[1];
};

struct edit_text {
  struct msg_search_node *search_node;
  int len;
  int kludges_size;
  char text[1];
};

struct message_flags_and_extra {
  int flags;
  int extra[0];
};

extern int first_extra_global_id;

struct message_extras {
  int user_id;
  int local_id;
  int date;
  unsigned ip;
  int port;
  unsigned front;
  unsigned long long ua_hash;
};


/* memory allocation for user metafiles */


/* interface */

struct sublist_descr {
  int xor_mask, and_mask;
};

struct imessage {
  struct message *msg;		/* returned if data or data+text are here */
  struct file_message *fmsg;	/* returned if text or data+text are here */
  struct message_extras *m_extra;
  struct value_data *value_actions;
  struct edit_text *edit_text;
  int flags;
};

struct imessage_long {
  struct message *msg;
  struct file_message *fmsg;
  struct message_extras *m_extra;
  struct value_data *value_actions;
  struct edit_text *edit_text;
  struct message builtin_msg;
};

int do_store_new_message (struct lev_add_message *M, int random_tag, char *text, long long legacy_id);
int do_delete_message (int user_id, int local_id);
int do_delete_first_messages (int user_id, int first_local_id);
int do_decr_flags (int user_id, int local_id, int flags);
int do_incr_flags (int user_id, int local_id, int flags);
int do_set_flags (int user_id, int local_id, int flags);

int do_replace_message_text (int user_id, int local_id, char *text, int text_len);

// value_id: 0..7 (32-bit), 8..11 (64-bit)
int do_set_values (int user_id, int local_id, struct value_data *V);
int do_incr_value (int user_id, int local_id, int value_id, int incr);
int do_incr_value_long (int user_id, int local_id, int value_id, long long incr);

int do_change_mask (int new_mask);

extern int R[MAX_TMP_RES], R_cnt;

int get_local_id_by_random_tag (int user_id, int random_tag);
int get_local_id_by_legacy_id (int user_id, long long legacy_id);
int get_message_flags (int user_id, int local_id, int force);
int get_message_value (int user_id, int local_id, int value_id, int force, long long *result);
/* 0 = none, 1 = ok, -1 = error, -2 = need load */
int load_message (struct imessage *M, int user_id, int local_id, int force);
/* fetch_flags: 1 = unpack text, 2 = unpack kludges */
int load_message_long (struct imessage_long *M, int user_id, int local_id, int max_text_len, int fetch_flags);
int unpack_message_long (struct imessage_long *M, int max_text_len, int fetch_flags);
/* >= 0 - total # of results, -1 = error, -2 = need load */
int get_peer_msglist (int user_id, int peer_id, int from, int to);
/* >= 0 - total # of entries <= local_id, -1 = error, -2 = need load */
int get_peer_msglist_pos (int user_id, int peer_id, int local_id);
/* result is in R & R_cnt, return value is as above */
int get_join_peer_msglist (int user_id, int peer_list[], int peers, int limit);
int get_top_msglist (int user_id, int from, int to);
/* same, -3 = unsupported sublist type */
int get_msg_sublist (int user_id, int and_mask, int xor_mask, int from, int to);
int get_msg_sublist_ext (int user_id, int and_mask, int xor_mask, int mode, int from, int to);
int get_msg_sublist_pos (int user_id, int and_mask, int xor_mask, int local_id);

int get_timestamp (int user_id, int force);
int get_history (int user_id, int timestamp, int limit, int *R);

int get_persistent_timestamp (int user_id);
int get_persistent_history (int user_id, int timestamp, int limit, int *R);

struct sublist_descr *get_peer_sublist_type (void);
int get_sublist_types (struct sublist_descr *A);

int unload_generic_metafile (core_mf_t *M);

core_mf_t *load_user_metafile (long long user_id);
int unload_user_metafile (long long user_id);
int check_user_metafile (long long user_id, int *R);

extern conn_query_type_t aio_metafile_query_type;

int do_update_history (int user_id, int local_id, int flags, int op);
int do_update_history_extended (int user_id, const char *string, long len, int op);

char *get_user_secret (int user_id);
int set_user_secret (int user_id, const char *secret);

int user_friends_online (int user_id, int N, int *A);
int user_friends_offline (int user_id, int N, int *A);
int get_online_friends (int user_id, int mode);

void adjust_some_users (void);

/* >= 0 - total # of results, -1 = error, -2 = need load */
int get_search_results (int user_id, int peer_id, int and_mask, int xor_mask, int min_time, int max_time, int max_res, const char *query);

int conv_uid (int user_id);
char *check_kludges (char *text, int len);

#endif
