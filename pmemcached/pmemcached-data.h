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
                   2010 Arseny Smirnov (Original memcached code)
                   2010 Aliaksei Levin (Original memcached code)
              2011-2013 Vitaliy Valtman
                   2011 Anton Maydell
*/

#ifndef __PMEMCACHED_DATA_H__
#define __PMEMCACHED_DATA_H__

#include "kdb-data-common.h"
#include "net-aio.h"
#include "net-events.h"
#include "net-connections.h"
#include "kdb-pmemcached-binlog.h"
#include "kfs.h"
#include "kdb-pmemcached-binlog.h"
#include "net-memcache-server.h"
#include "pmemcached-index-common.h"

//#define MAX_ZMALLOC_MEM 16384
#define DELAY_INFINITY (-1)
#define MAX_ZMALLOC_MEM 0

//the remaining memory is for entries
#define MAX_MEMORY 1200000000l
#define MEMORY_FOR_METAFILES 0.5
#define MEMORY_FOR_CACHE 0.1
#define MEMORY_FOR_WILDCARD_CACHE 0.1
#define MEMORY_RESERVED 0.1
#define MIN_MEMORY_FOR_METAFILES 10000000
#define MIN_MEMORY_FOR_CACHE 10000000
#define MIN_MEMORY_FOR_WILDCARD_CACHE 10000000
#define METAFILE_SIZE (1<<18)


#define HASH_TABLE_SIZE_EXP 23
#define HASH_TABLE_SIZE (1 << HASH_TABLE_SIZE_EXP)
#define MAX_HASH_TABLE_SIZE (1 << (HASH_TABLE_SIZE_EXP - 1))
#define HASH_TABLE_MASK (HASH_TABLE_SIZE - 1)

#define ENTRIES_TO_INDEX (1 << 24)

#define PMEMCACHED_TYPE_INDEX_DISK 0
#define PMEMCACHED_TYPE_INDEX_RAM 1

extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;

typedef struct tree tree_t;

typedef struct hash_entry hash_entry_t;

struct tree {
  tree_t *left, *right;
  hash_entry_t *x; 
  int y;
};


struct hash_entry
{
  char *key, *data;

  int key_len, data_len, exp_time;
  int timestamp;
  long long accum_value;


  //struct hash_entry *next_entry;
  //int next_time, prev_time;
  struct hash_entry *next_time, *prev_time;
  short flags;
  char need_merging;
};

struct wildcard_entry {
  char *key, *data;
  int key_len, data_len;
  struct wildcard_entry *next_use, *prev_use;
  int flags;
};

struct index_entry {
  short key_len;
  short flags;
  int data_len;
  int delay;
  char data[0];
};

struct data {
  int data_len;
  int delay;
  short flags;
  char *data;
};

struct entry {
  short key_len;
  char index_entry;
  char allocated_here;

  char *key;

  hash_entry_t* hash_entry;
  struct data data;
  int timestamp;
};



int get_entry_cnt (void);
long long get_memory_used(void);
int memory_full_warning (void);


int do_pmemcached_store (int op_add, const char *key, int key_len, int flags, int delay, const char* data, int data_len);
int do_pmemcached_incr (int op, const char *key, int key_len, long long arg);
struct data do_pmemcached_get (const char *key, int key_len);
int do_pmemcached_delete (const char *key, int key_len);
void do_pmemcached_merge (const char *key, int key_len);
int do_pmemcached_preload (const char *key, int key_len, int forceload);
int do_pmemcached_get_next_key (const char *key, int key_len, char **result_key, int *result_key_len);
int do_pmemcached_get_all_next_keys (const char *key, int key_len, int prefix_len, int strict);
void free_by_time (int mx);
int load_index (kfs_file_handle_t Index);
int save_index (int writing_binlog);
void init_hash_table (void);
void pmemcached_register_replay_logevent ();
void wildcard_add_value (const char *key, int key_len, const char *data, int data_len);
void wildcard_add_rpc_value (const char *key, int key_len, const char *data, int data_len);
struct data wildcard_get_value (const char *key, int key_len);
struct data wildcard_get_rpc_value (const char *key, int key_len);

double get_double_time_since_epoch(void);
void *zzmalloc (int size);
void zzfree (void *ptr, int size);

void debug_dump_key (const char *key, int key_len);
void data_prepare_stats (stats_buffer_t *sb);

void free_cache (void);
void init_memory_bounds (void);
void redistribute_memory (void);

extern int (*wildcard_engine_report) (const char *key, int key_len, struct data x);

int mystrcmp (const char *str1, int l1, const char *str2, int l2);
int mystrcmp2 (const char *str1, int l1, const char *str2, int l2, int x);
int dump_pointers (hash_entry_t **x, int p, int maxp);
#endif
