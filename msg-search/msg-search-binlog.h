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

    Copyright      2009 Vkontakte Ltd
              2008-2009 Nikolai Durov
              2008-2009 Andrei Lopatin
*/

#ifndef __MSG_SEARCH_BINLOG_H__
#define __MSG_SEARCH_BINLOG_H__

#include "msg-search-data.h"

#pragma	pack(push,4)

/* max dyndata users */
#define PRIME	1000003
#define ULOG_BUFFER_SIZE	(1L << 24)
#define DYNAMIC_DATA_BUFFER_SIZE	(1666 << 20)

/* log event header */

#define LE_DELMSG_MAGIC	0x21e3a456
#define LE_DELOLDMSG_MAGIC	0x71685356
#define LE_UNDELMSG_MAGIC	0x37cc2f26
#define	LE_NEWMSG_MAGIC	0x1a890000
#define	LE_NEWMSG_MAGIC_MAX	0x1a89ffff
#define	LE_PAD_MAGIC	0x45ba3de4
#define LE_TIMESTAMP_MAGIC	0x6ed931a8

struct log_event {
  int type;
  int user_id;
  int msg_id;
};

/* in-core: recent modifications data */

typedef struct user_mod_header user_mod_header_t;
typedef struct raw_message raw_message_t;
typedef struct hashed_message message_t;

struct raw_message {
  int user_id;
  int message_id;
  int no_reply;
  int len;
  char *data;
};


struct hashed_message {
  message_t *prev;
  int user_id;
  int message_id;
  int hc;
  hash_t hashes[1];
};

struct user_mod_header {
  int pos_to;
  int neg_to;
  int delmsg_cnt;
  int delmsg_max;
  int *delmsg_list;
  message_t *msgs;
};

extern char *binlogname;
extern long long binlog_size;
extern int binlog_fd;

/* dynamic data management */

extern int u_cnt, purged_msgs_cnt;
extern long long tot_kept_messages, tot_kept_msg_bytes;

extern char *dyn_first, *dyn_cur, *dyn_last;

extern long long log_pos, log_readto_pos, log_cutoff_pos;
extern int log_first_ts, log_read_until, log_last_ts;

extern user_mod_header_t *UserModHeaders[PRIME];
extern int Users[PRIME];

void init_dyn_data (void);

void *dyn_alloc (int size, int align);
user_mod_header_t *dyn_user_header (int user_id, int force);

hash_t *dyn_alloc_new_msg (int user_id, int message_id, int hcount);
void dyn_delete_msg (user_mod_header_t *H, int message_id);
void dyn_undelete_msg (user_mod_header_t *H, int message_id);
void dyn_delall_msg (user_mod_header_t *H, int message_id);

void dyn_purge_deleted_user_messages (user_mod_header_t *H);
void dyn_purge_all_deleted_messages (void);

void clear_log (void);
void set_log_data (int logfd, long long logsize);

int replay_logevent (struct log_event *E, int size);
void replay_log (int cutoff_time);
void flush_binlog (void);
void flush_binlog_ts (void);
int compute_uncommitted_log_bytes (void);

hash_t *alloc_log_event (int type, int user_id, int message_id, int extra);

#pragma	pack(pop)

#endif
