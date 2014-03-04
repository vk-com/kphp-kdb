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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#ifndef __CACHE_DATA_H__
#define __CACHE_DATA_H__

#include "kfs.h"
#include "kdb-cache-binlog.h"
#include "cache-heap.h"

#define MAX_ACOUNTERS_INIT_STR_LEN 256
#define MAX_NODE_ID 16383
#define MAX_SERVER_ID 255
#define MAX_DISK_ID 255
#define MAX_YELLOW_LIGHT_DURATION 86400
#define MAX_URL_LENGTH 1024

#define CACHE_ERR_INVALID_NODE_ID (-1000)
#define CACHE_ERR_INVALID_SERVER_ID (-1001)
#define CACHE_ERR_INVALID_DISK_ID (-1002)
#define CACHE_ERR_UNKNOWN_T (-1003)
#define CACHE_ERR_LIMIT (-1004)
#define CACHE_ERR_NEW_INDEX (-1005)

#define CACHE_FEATURE_LONG_QUERIES 1
#define CACHE_FEATURE_REPLAY_DELETE 2
#define CACHE_FEATURE_DETAILED_SERVER_STATS 4
#define CACHE_FEATURE_FAST_BOTTOM_ACCESS 8
#define CACHE_FEATURE_ACCESS_QUERIES 16

/* uncomment next line to include monthly stats feature to the binary cache-engine */
//#define CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS 0x40000000

/* uncomment next line to include correlation stats feature to the binary cache-engine */
//#define CACHE_FEATURE_CORRELATION_STATS

#define CACHE_STAT_MAX_ACOUNTERS 4

#define CACHE_MAX_SIZE 0xffffffffffffLL

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern int cache_features_mask;

#define AMORTIZATION_TABLE_SQRT_SIZE_BITS 11
#define AMORTIZATION_TABLE_SQRT_SIZE (1<<AMORTIZATION_TABLE_SQRT_SIZE_BITS)
#define AMORTIZATION_TABLE_SQRT_SIZE_MASK (AMORTIZATION_TABLE_SQRT_SIZE-1)

struct time_amortization_table {
  double hi_exp[AMORTIZATION_TABLE_SQRT_SIZE], lo_exp[AMORTIZATION_TABLE_SQRT_SIZE];
  double c, T;
};
extern struct time_amortization_table *TAT;

#define CACHE_LOCAL_COPY_FLAG_LAST                   0x80000000
#define CACHE_LOCAL_COPY_FLAG_INT                    0x40000000
#define CACHE_LOCAL_COPY_FLAG_TEMPORARLY_UNAVAILABLE 0x20000000

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
#define CACHE_LOCAL_COPY_FLAG_MONTHLY_COUNTER        0x10000000
#endif

#define CACHE_LOCAL_COPY_FLAG_YELLOW_LIGHT_MASK      0x0001ffff

extern int cache_id, uri_hash_prime;
extern int amortization_counter_types, acounter_uncached_bucket_id, stats_counters;
extern char *acounters_init_string;

extern long long uries, deleted_uries, uri_bytes, access_short_logevents, access_long_logevents, skipped_access_logevents, local_copies_bytes, cached_uries;
extern long long cached_uries_knowns_size, sum_all_cached_files_sizes;
extern long long uri_cache_hits, get_uri_f_calls, uri2md5_extra_calls, uri_reallocs;

#pragma	pack(push,4)
struct amortization_counter {
  float value;
  int last_update_time;
};
#pragma	pack(pop)

struct amortization_counter_precise {
  double value;
  int last_update_time;
};

extern struct amortization_counter_precise *convert_success_counters, *convert_miss_counters, *access_success_counters, *access_miss_counters;

typedef union {
  unsigned char c[16];
  unsigned long long h[2];
} md5_t;

struct cache_uri {
  unsigned long long uri_md5_h0;
  struct cache_uri *hnext;
  char *local_copy;
  int last_access;
  int size;
  char data[2*sizeof(void*)]; /* prev, next, array of struct amortization_counters + uri (NUL terminated string) */
};

typedef struct {
  short node_id;
  unsigned char server_id;
  unsigned char disk_id;
} cache_local_copy_packed_location_t;

union cache_packed_local_copy_location {
  int i;
  cache_local_copy_packed_location_t p;
};

struct cache_local_copy {
  unsigned cached_at;
  int packed_location;
  unsigned flags;
  int yellow_light_start;
#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  float cached_counter_value[CACHE_STAT_MAX_ACOUNTERS];
#endif
  char location[256];
};

enum cache_sorted_order { cgsl_order_top = 0, cgsl_order_bottom = 1 };

typedef struct {
  int packed_location;
  int packed_location_len;
  int prefix_len;
  char packed_prefix[64];
} cache_disk_filter_t;

typedef struct {
  double real_min_rate;
  cache_heap_t *heap;
  cache_disk_filter_t *disk_filter;
  int cnt;
  int flags;
  int min_rate;
  int heap_acounter_off;
} cache_top_access_result_t;

typedef struct {
  char *buff;
  int pos;
  int size;
} cache_buffer_t;

typedef struct cache_stat_server {
  long double access_queries;
  long long files;
  long long files_bytes;
  struct cache_stat_server *hnext;
  int id;
} cache_stat_server_t;

void cache_bclear (cache_buffer_t *b, char *buff, int size);
void cache_bprintf (cache_buffer_t *b, const char *format, ...)  __attribute__ ((format (printf, 2, 3)));

double get_resource_usage_time (void);

int cache_unpack_local_copy (struct cache_uri *U, struct cache_local_copy *L, int olen, int *local_copy_len);
int cache_local_copy_get_flags (struct cache_local_copy *L, union cache_packed_local_copy_location *u);
/* warning return pointer in the global static array */
struct cache_local_copy *cache_uri_local_copy_find (struct cache_uri *U, cache_disk_filter_t *F);

struct cache_uri *get_uri_f (const char *const uri, int force);
int cache_set_amortization_tables_initialization_string (const char *const s);
int cache_le_start (struct lev_start *E);
void cache_incr (struct cache_uri *U, int t);
struct cache_uri *cache_get_uri_by_md5 (md5_t *uri_md5, int len);
void cache_set_size_short (struct lev_cache_set_size_short *E);
void cache_set_size_long (struct lev_cache_set_size_long *E);
int cache_get_priority_heaps (cache_heap_t *heap_cached, cache_heap_t *heap_uncached, int cached_limit, int uncached_limit, int *r1, int *r2);
char *const cache_get_uri_name (const struct cache_uri *const U);
struct amortization_counter *cache_uri_get_acounter (struct cache_uri *U, int id);
float cache_uri_get_acounter_value (struct cache_uri *U, int id);
long long cache_uri_get_size (const struct cache_uri *U);
void cache_all_uri_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order);
double cache_get_uri_heuristic (const struct cache_uri *U);

int cache_hashtable_init (int hash_size);
void cache_do_access (const char *const uri);
int cache_do_convert (const char *const uri, char *output, int olen);
/* should be enclosed in mark/release */
int cache_do_detailed_server_stats (cache_stat_server_t ***S, int flags);

int cache_do_memory_stats (char *output, int olen);
int cache_do_set_size (const char *const uri, long long size);
int cache_do_set_new_local_copy (const char *const global_uri, const char *const local_uri);
int cache_do_set_yellow_light_remaining (const char *const global_uri, const char *const local_uri, int duration);
int cache_do_delete_local_copy (const char *const global_uri, const char *const local_uri);
int cache_do_delete_remote_disk (int node_id, int server_id, int disk_id);
int cache_do_local_copies (const char *const uri, struct cache_local_copy **R);
int cache_do_change_disk_status (int node_id, int server_id, int disk_id, int enable);
int cache_get_file_size (const char *const uri, long long *size);
int cache_get_yellow_light_remaining (const char *const global_uri, const char *const local_uri, int *remaining_time, int *elapsed_time);
int cache_acounter (const char *const uri, int T, double *value);
void cache_clear_all_acounters (void);

void cache_local_copy_get_yellow_light_time (struct cache_local_copy *L, int *remaining_time, int *elapsed_time);

int cache_get_sorted_list (cache_top_access_result_t *R, int T, enum cache_sorted_order order, int limit, int flags, int min_rate);
int cache_get_bottom_disk (cache_top_access_result_t *R, int T, enum cache_sorted_order order, int limit, int node_id, int server_id, int disk_id, int flags);
int cache_get_top_stats (int T, enum cache_sorted_order order, int limit, char *output, int olen);

void cache_garbage_collector_init (void);
int cache_garbage_collector_step (int max_steps);
int cache_acounters_update_step (int max_steps);

int load_index (kfs_file_handle_t Index);
int save_index (int writing_binlog);
int cache_set_index_timestamp (int timestamp, int index_mode);
void cache_save_pseudo_index (void);
int cache_pseudo_index_check_mode (int index_mode);
int cache_pseudo_index_init (int index_mode, int timestamp, const char *const index_filter_server_list, int new_cache_id);

void cache_stats_counter_incr (struct amortization_counter_precise *C);
void cache_stats_relax ();
void cache_stats_perf (char *out, int olen, struct amortization_counter_precise *success, struct amortization_counter_precise *miss);

void make_empty_binlog (const char *binlog_name);

void debug_check_acounters_in_increasing_order (struct cache_uri *U);

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
int cache_monthly_stat_report (const char *const stat_filename);
#endif

#ifdef CACHE_FEATURE_CORRELATION_STATS
int cache_correlation_stat_report (const char *correlaction_stat_dir);
#endif

#endif
