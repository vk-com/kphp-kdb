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
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#pragma once

#include "crc32.h"

#include "net-events.h"
#include "server-functions.h"

#include "vv-tl-parse.h"

#include "dl-utils.h"

#define MAX_PRIORITY 10
#define MAX_DELAY 3600
#define MAX_MEMORY 2000000000
#define DEFAULT_BUFFER_SIZE 1000000000ll
#define MAX_LETTER_SIZE 256000

#if MAX_PRIORITY > 10
#  error too large MAX_PRIORITY
#endif

#define LETTERS_INDEX_MAGIC 0x25612fa1

#define LETTER_BEGIN 0x5fade5912ca68231ll
#define LETTER_END 0x2dfeacb468123457ll
#define FILE_END 0x6b9132f0eadb2bacll


extern long max_memory;

extern int log_drive;

extern long long expired_letters;
extern long long letter_stat[MAX_PRIORITY][6]; //added, deleted, in_memory, read, write, sync
extern const char* letter_stat_name[6];

extern int engine_num;
extern int total_engines;


extern int task_deletes_begin;
extern int task_deletes_size;


int init_logs_data (int schema);

int init_all (char *index_name, long long size[MAX_PRIORITY]);
void free_all (void);

#pragma pack(push,4)

typedef struct {
  long long drive_l, drive_r;
  long long drive_old_mx, drive_mx;
  long long memory_l, memory_r;
  long long memory_buf_l, memory_buf_r;
  long long memory_buf_mx;

  long long reserved[10];
} one_header;

typedef struct {
  int magic;
  int created_at;

  int reserved[60];

  one_header data[MAX_PRIORITY];
} index_header;

#pragma pack(pop)

int add_letter (int delay, long long task_id, char *let);
int add_letter_priority (long long id, int priority, int delay, const char *error);
char *get_letters (int min_priority, int max_priority, int cnt, int immediate_delete);
int delete_letter (long long id);
void letter_delete_time (int gap, const char *error);
void process_delayed_letters (int all, int skip);
long long letters_clear (int priority);

int delete_letters_by_task_id (long long task_id);

long long get_drive_buffer_size (int priority);
long long get_drive_buffer_mx (int priority);

int get_sync_delay (void);

void flush_all (int force);
