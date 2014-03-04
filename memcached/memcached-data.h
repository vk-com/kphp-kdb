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

    Copyright 2010-2012 Vkontakte Ltd
              2010-2012 Arseny Smirnov
              2010-2012 Aliaksei Levin
*/

#ifndef __MEMCACHED_DATA_H__
#define __MEMCACHED_DATA_H__

#include "kdb-data-common.h"
#include "net-connections.h"


#pragma pack(push, 4)

typedef struct hash_entry hash_entry_t;

struct hash_entry {
  char *key, *data;

  int flags, key_len, data_len, exp_time;
  long long key_hash;

  int next_entry;
  int next_used, prev_used;
  int next_time, prev_time;
};

#pragma pack(pop)

#define HASH_TABLE_SIZE_EXP 23
#define HASH_TABLE_SIZE (1 << HASH_TABLE_SIZE_EXP)
#define MAX_HASH_TABLE_SIZE (1 << (HASH_TABLE_SIZE_EXP - 1))
#define HASH_TABLE_MASK (HASH_TABLE_SIZE - 1)
#define GET_ENTRY_ID(x) ((unsigned int)(x) & HASH_TABLE_MASK)

#define TIME_TABLE_RATIO_EXP (4)
#define TIME_TABLE_SIZE_EXP (22 - TIME_TABLE_RATIO_EXP)
#define TIME_TABLE_SIZE (1 << TIME_TABLE_SIZE_EXP)
#define TIME_TABLE_MASK (TIME_TABLE_SIZE - 1)
#define GET_TIME_ID(x) (((unsigned int)(x) >> TIME_TABLE_RATIO_EXP) & TIME_TABLE_MASK)
#define MAX_TIME_GAP ((60 * 60) >> TIME_TABLE_RATIO_EXP)

//#define MAX_ZMALLOC_MEM 16384
#define MAX_ZMALLOC_MEM 0
#define MAX_MEMORY 1500000000l


long long get_hash (const char * s, int sn);

void init_hash_table (void);

void del_entry_used (int x);
void add_entry_used (int x);

void del_entry_time (int x);
void add_entry_time (int x);

void free_by_time (int mx);

void add_entry (int x);
void del_entry (int x);

int get_entry (const char *key, int key_len, long long hash);
int get_entry_no_check (long long hash);
hash_entry_t *get_entry_ptr (int x);

int get_new_entry (void);
int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags);

void *zzmalloc (int size);
void zzfree (void *ptr, int size);

int get_entry_cnt (void);
long get_memory_used (void);
long get_min_memory (void);
long get_min_memory_bytes (void);

long long get_del_by_LRU (void);
long long get_time_gap (void);

void write_stats (void);

#endif
