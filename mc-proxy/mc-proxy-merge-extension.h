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
              2011-2013 Vitaliy Valtman
*/

#ifndef __MC_PROXY_MERGE_EXTENSION_H__
#define	__MC_PROXY_MERGE_EXTENSION_H__

#include "mc-proxy.h"
int search_create (struct connection *c, char *key, int key_len);
int search_merge (struct connection *c, int data_len);
int search_stats (char *buff, int len);
int search_check (int type, const char *key, int key_len);
int search_store (struct connection *c, int op, const char *key, int key_len, int expire, int flags, int bytes);
int search_skip (struct connection *c, struct conn_query *q);

struct connection *get_target_conn (struct conn_target *S);

/* merge data structures */

struct gather_entry {
  int num;
  int res_bytes;
  int res_read;
  int *data;
};

#define GD_MAGIC 0x54780000
#define GD_MAGIC_MASK (-0x10000)

struct gather_data {
  struct connection *c;
  struct conn_query *q;
  double start_time;
  int magic;
  int tot_num;
  int wait_num;
  int ready_num;
  int error_num;
  int orig_key_len;
  int new_key_len;
  int sum;
  int size;
  char *orig_key;
  char *new_key;
  void *extra;
  struct gather_entry List[1];
};

/* end (merge structures) */

struct mc_proxy_merge_functions {
  int (*free_gather_extra)(void *E);
  int (*merge_end_query)(struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num);
  int (*generate_preget_query)(char *buff, const char *key, int len, void *E, int n);
  int (*generate_new_key)(char *buff, char *key, int len, void *E);
  void* (*store_gather_extra)(const char *key, int key_len);
  int (*check_query)(int type, const char *key, int key_len);
  int (*merge_store)(struct connection *c, int op, const char *key, int key_len, int expire, int flags, int bytes);
  void (*load_saved_data)(struct connection *c);
  int (*use_preget_query)(void *extra);
};

struct mc_proxy_merge_conf {
  int use_at;
  int use_preget_query;
};

int default_free_gather_extra (void *E);
int default_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num);
int default_generate_preget_query (char *buff, const char *key, int len, void *E, int n);
int default_generate_new_key (char *buff, char *key, int len, void *E);
void *default_store_gather_extra (const char *key, int key_len);
int default_check_query (int type, const char *key, int key_len);
int default_merge_store (struct connection *c, int op, const char *key, int key_len, int expire, int flags, int bytes);
void default_load_saved_data (struct connection *c);
int default_use_preget_query (void *extra);


int eat_at (const char *key, int key_len);
void* zzmalloc (int size);
void* zzmalloc0 (int size);
void zzfree (void *src, int size);
#endif
