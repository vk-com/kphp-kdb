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

#ifndef _LOGS_DATA_H_
#define _LOGS_DATA_H_

#include <math.h>

#include "net-connections.h"
#include "kfs.h"
#include "kdb-logs-binlog.h"

#include "vv-tl-parse.h"

#include "utils.h"

#define MAX_NAME_LEN 256
#define MAX_QUERY_LEN 100000
#define MAX_SELECT 100000
#define MAX_TYPE 32768
#define MAX_HISTORY 1000

#define LOGS_INDEX_MAGIC 0x72daf271


extern int std_t[FN + 4];
extern char *field_names[FN];


extern int index_mode;
extern int test_mode;
extern int dump_index_mode;
extern int my_verbosity;
extern long long max_memory;
extern long long query_memory;
extern int header_size;
extern int binlog_readed;
extern int write_only;
extern int dump_mode, dump_type, from_ts, to_ts;
extern long events_memory;
extern int eq_n, eq_total;
extern int mean_event_size;

extern int jump_log_ts;
extern long long jump_log_pos;
extern unsigned int jump_log_crc32;

extern char *dump_query;
extern char *stat_queries_file;


int init_logs_data (int schema);

int init_all (kfs_file_handle_t Index);
void free_all (void);

#define INDEX_TYPE 2
#define TIME_STEP 3600

#pragma pack(push,4)

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
  int log_timestamp0;
  int total_types;
  int types_data_len;
  int index_type;//0 - base index, 1 - with timetable, 2 - and with colors
  int time_table_size;

  int reserved[30];
} index_header;

#pragma pack(pop)

int local_uid (int user_id);

void load_dump (char *dump_name);
int save_index (void);

int get_color (int field_num, long long field_value);
int do_set_color (int field_num, long long field_value, int cnt, int and_mask, int xor_mask);

char *do_create_type (const char *s);
char *do_add_field (const char *s);
int do_add_event (char *type_s, int params[FN - 2], char *desc);
char *logs_select (char *s, int sn);
int is_name (char *s);

void print_stats (void);

int read_long (const char *s, long long *x, int *pos);

//statistics
char *get_type_size (int type);
char *get_types (void);
long get_colors_memory (void);
long get_q_st_memory (void);
int get_time (void);

#endif
