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

#ifndef _BAYES_DATA_H_
#define _BAYES_DATA_H_

#include "net-aio.h"
#include "kfs.h"
#include "kdb-bayes-binlog.h"

#include "hash_table.h"

#define SPAM 1
#define HAM 0

#define	BAYES_INDEX_MAGIC 0x4526fac4

#define BAYES_SPAM_LIMIT 0.9
#define BAYES_MAX_WORDS 10

#define MEMORY_USER_PERCENT 0.5

extern int index_mode;
extern long max_memory;
extern int cur_users;
extern int index_users, header_size;
extern long long teach_messages;
extern int binlog_readed;

extern int jump_log_ts;
extern long long jump_log_pos;
extern unsigned jump_log_crc32;

extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;

#define	BYS_MSG(c) ((message *) ((c)->custom_data + sizeof (struct mcs_data)))

#define NOAIO 0

typedef struct {
  char *text;
  int len, random_tag;
} message;

void msg_free (message *msg);
char *msg_get_buf (message *msg);
int msg_reinit (message *msg, int len, int random_tag);

double bayes_is_spam_prob (message *msg, int random_tag);
int bayes_is_spam (message *msg, int random_tag);
int bayes_is_spam_debug (message *msg, int random_tag, char *debug_s);

int do_bayes_set_spam (message *msg, int random_tag);
int do_bayes_set_ham (message *msg, int random_tag);
int do_bayes_unset_spam (message *msg, int random_tag);
int do_bayes_unset_ham (message *msg, int random_tag);
int do_bayes_reset_spam (message *msg, int random_tag);
int do_bayes_reset_ham (message *msg, int random_tag);

long long get_words_cnt ();

int init_bayes_data (int schema);

int init_all (kfs_file_handle_t Index);
void free_all (void);

#pragma pack(push,4)

typedef struct {
  int type;
  long long nbad, ngood;
  qhash_table cnt;
} bayes;

typedef struct userx user;

struct userx {
  char *metafile;
  int metafile_len;

  struct aio_connection *aio;

  bayes b;

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
  int black_list_size;
  long long teach_messages;
  int reserved[36];

  int user_cnt;
  user_index_data *user_index;
} index_header;

#pragma pack(pop)

int save_index ();

typedef struct black_listx black_list;

struct black_listx {
  int text_len;
  char *text;
  black_list *next;
};

int do_black_list_add (const char *s);
int do_black_list_delete (const char *s);
int black_list_get (char *buf, int buf_len);

#endif
