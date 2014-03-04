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
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
              2011-2013 Vitaliy Valtman
              2011-2013 Anton Maydell
*/

#ifndef __STATS_DATA_H__
#define __STATS_DATA_H__

#include "kdb-data-common.h"
#include "kdb-statsx-binlog.h"
#include "kfs.h"
#include "net-aio.h"

#pragma	pack(push,4)

extern int max_counters, counters_prime;

//#define MAX_INDEX_SIZE 30000000
//#define MAX_ELEM MAX_INDEX_SIZE
#define MAX_COUNTERS_ALLOCATED 10000000
#define MAX_SUBCNT 64
//#define MAX_USERS (1 << 21)
#define STATSX_INDEX_MAGIC_OLD 0x76ac7301
#define STATSX_INDEX_MAGIC_V1 0x927348af
#define STATSX_INDEX_MAGIC_V2 0xa978a06b
#define STATSX_INDEX_MAGIC_V3 0xf0a9faa8
#define STATSX_INDEX_MAGIC_V4 0x8890890a

#define USE_AIO 1
#define MAX_ZALLOC 0

#ifdef __LP64__
  #define MEMORY_TO_INDEX (0x80000000ll)
#else
  #define MEMORY_TO_INDEX (0x50000000ll)
#endif
 

#define FORCE_COUNTER_TYPE 1


#define MONTHLY_STAT_LEN 40

int init_stats_data (int schema);

int flush_view_counters (void);

extern int tot_counters;
extern long long tot_views;
extern int tot_counter_instances;
extern int tot_counters_allocated;
extern long long deleted_by_lru;
extern long long tot_memory_allocated;
extern double snapshot_loading_average_blocking_read_bytes;
extern int snapshot_loading_blocking_read_calls;

extern double max_counters_growth_percent;
extern int alloc_tree_nodes;//, free_tree_nodes;
extern int today_start;
extern long long tot_aio_loaded_bytes;
extern long long tot_aio_fails;
extern long long tot_aio_queries;

extern int tot_user_metafiles;
extern long long tot_user_metafile_bytes;

extern int mode_ignore_user_id;

typedef struct tree tree_t;

struct tree {
  tree_t *left, *right;
  int x, y;
};

#define MAX_SEX 2
#define MAX_AGE 8
#define MAX_MSTATUS 8
#define MAX_POLIT 8
#define MAX_SECTION 16
#define MAX_CITIES  63
#define MIN_CITIES  4
#define MAX_SEX_AGE (MAX_SEX*MAX_AGE)
#define MIN_COUNTRIES 2
#define MAX_COUNTRIES 63
#define MIN_GEOIP_COUNTRIES 2
#define MAX_GEOIP_COUNTRIES 63
#define MAX_TYPES 10
#define MAX_SOURCE 16

#define COUNTER_TYPE_DELETABLE (1 << 16)
#define COUNTER_TYPE_LAST (1 << 17)
#define COUNTER_TYPE_MONTH (1 << 18)

struct counter {
  long long counter_id;
  int type; // bits 0..15 for type. 
  int views;
  int unique_visitors;
  //int unique_visitors_month;
  int deletes; 
  int long_unique_visitors;
  int last_month_unique_visitors;
  int last_week_unique_visitors;
  tree_t *visitors;
  int views_uncommitted;
  struct counter *commit_next;
  struct counter *prev;
  int created_at;
  int valid_until;
  int visitors_sex[2];
  int *visitors_age;
  int *visitors_mstatus;
  int *visitors_polit;
  int *visitors_section;
  int *visitors_cities;
  int *visitors_sex_age;
  int *visitors_countries;
  int *visitors_geoip_countries;
  int *visitors_source;
  unsigned long long mask_subcnt;
  //int subcnt_number;
  int *subcnt;
  char timezone;
  struct counter *next_use;
  struct counter *prev_use;
  struct metafile *meta;
};

/*
struct cnt_type {
  int field_number;
  char **fields;
};
*/

struct index_header_v1 {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  int log_timestamp;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  
  int nrecords;
  long long shifts_offset;
};

struct index_header_v2 {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  int log_timestamp;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  
  int nrecords;
  long long shifts_offset;

  int total_elements;
  int last_incr_version;
  int last_index_day;
};

struct metafile {
  struct aio_connection *aio;
  long long size;
  char *data;
  long long cnt;
  int flags;
};


/*
extern int types_count;
extern struct cnt_type types [MAX_TYPES];
*/

void *zzmalloc (int l);
void *zzmalloc0 (int l);
void zzfree (void *p, int l);
void *zzrealloc0 (void *p, int old_len, int new_len);

int counter_incr (long long counter_id, int user_id, int replaying, int op, int subcnt);
int counter_incr_ext (long long counter_id, int user_id, int replaying, int op, int subcnt, int sex, int age, int m_status, int polit_views, int section, int city, int geoip_country,int country, int source);
int enable_counter (long long counter_id, int replay);
int disable_counter (long long counter_id, int replay);
int delete_counter (long long counter_id, int replay);
int get_counter_views (long long counter_id);
int get_counter_visitors (long long counter_id);
int get_counter_views_given_version (long long counter_id, int version);
int get_counter_visitors_given_version (long long counter_id, int version);
int get_counter_serialized (char *buffer, long long counter_id, int version);
int tl_get_counter_serialized (long long counter_id, int version, int mode);
int counter_serialize (struct counter *C, char *buffer);
int get_counter_versions (char *buffer, long long counter_id);
int tl_get_counter_versions (long long counter_id);
int get_monthly_visitors_serialized (char *buffer, long long counter_id);
int tl_get_monthly_visitors_serialized (long long counter_id);
int get_monthly_views_serialized (char *buffer, long long counter_id);
int tl_get_monthly_views_serialized (long long counter_id);
struct counter *get_counters_sum (long long counter_id, int start_version, int end_version);
int set_timezone (long long counter_id, int timezone, int replay);
int get_timezone (long long counter_id);
int save_index (void);
int load_index (kfs_file_handle_t Index);
int init (void);
int free_LRU (void);
void tl_serialize_counter (struct counter *C, int mode);

extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;


#pragma	pack(pop)

#endif
