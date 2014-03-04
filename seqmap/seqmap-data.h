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

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman
*/

#ifndef __SEQMAP_DATA_H__
#define __SEQMAP_DATA_H__

#define MAX_ZMALLOC_MEM 0

#define MAX_MEMORY 1200000000l
#define METAFILE_SIZE (1 << 18)

int save_index (int writing_binlog);
void free_by_time (void);
void redistribute_memory (void);
int memory_full_warning (void);
void init_tree (void);
int load_index (kfs_file_handle_t Index);
long long get_memory_used(void);

void seqmap_register_replay_logevent ();

int do_store (int mode, int key_len, const int *key, int value_len, const int *value, int delay, int force);
int do_delete (int key_len, const int *key, int force);

struct item *get (int key_len, const int *key);
int get_range_count (int left_len, const int *left, int right_len, const int *right);
int get_range (int left_len, const int *left, int right_len, int *right, int limit, int *R, int size, int *cnt, int *total);


struct item {
  struct item *left, *right;
  struct item *prev_time, *next_time;
  int key_len;
  int value_len;
  int *key;
  int *value;
  int time;
  int type;
  int size;
  int delta;
  int minus_unsure;
  int plus_unsure;
  int y;
  int min_index_pos;
  int max_index_pos;
};

struct index_entry {
};

struct timeq {
  struct item *prev_time, *next_time;
};

#define TIME_MASK 0xffffff
#define MAX_TIME (TIME_MASK + 1)

#define NODE_TYPE(T) ((T)->type & 7)
#define NODE_TYPE_T(T) ((T)->type & 3)
#define NODE_TYPE_S(T) ((T)->type & 4)

#define NODE_TYPE_PLUS 1
#define NODE_TYPE_MINUS 2
#define NODE_TYPE_ZERO 3
#define NODE_TYPE_SURE 4
#define NODE_TYPE_UNSURE 0

#endif
