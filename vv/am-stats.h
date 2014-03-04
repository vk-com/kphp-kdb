/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#ifndef __VV_AM_STATS_H__
#define __VV_AM_STATS_H__

#include <unistd.h>
#include "net-connections.h"

#define AM_GET_MEMORY_USAGE_SELF 1
#define AM_GET_MEMORY_USAGE_OVERALL 2

#define SB_PRINT_I64(x) sb_printf (&sb, "%s\t%lld\n", #x, x)
#define SB_PRINT_I32(x) sb_printf (&sb, "%s\t%d\n", #x, x)
#define SB_PRINT_QUERIES(x) sb_print_queries (&sb, #x, x)
#define SB_PRINT_TIME(x) sb_printf (&sb, "%s\t%.6lfs\n", #x, x)
#define SB_PRINT_PERCENT(x) sb_printf (&sb, "%s\t%.3lf%%\n", #x, x)

#define SB_BINLOG sb_printf (&sb, \
  "binlog_original_size\t%lld\n" \
  "binlog_loaded_bytes\t%lld\n" \
  "binlog_load_time\t%.6fs\n" \
  "current_binlog_size\t%lld\n" \
  "binlog_uncommitted_bytes\t%d\n" \
  "binlog_path\t%s\n" \
  "binlog_first_timestamp\t%d\n" \
  "binlog_read_timestamp\t%d\n" \
  "binlog_last_timestamp\t%d\n" \
  "max_binlog_size\t%lld\n" \
  "binlog_write_disabled\t%d\n", \
  log_readto_pos, \
  log_readto_pos - jump_log_pos, \
  binlog_load_time, \
  log_pos, \
  compute_uncommitted_log_bytes (), \
  binlogname ? (strlen(binlogname) < 250 ? binlogname : "(too long)") : "(none)", \
  log_first_ts, \
  log_read_until, \
  log_last_ts, \
  max_binlog_size, \
	binlog_disabled) \

#define SB_INDEX sb_printf (&sb, \
  "index_path\t%s\n" \
  "index_size\t%lld\n" \
  "index_load_time\t%.6fs\n", \
  engine_snapshot_name, engine_snapshot_size, index_load_time)

static inline double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

typedef struct {
  long long vm_size;
  long long vm_rss;
  long long vm_data;
  long long mem_free;
  long long swap_total;
  long long swap_free;
  long long swap_used;
} am_memory_stat_t;

int am_get_memory_usage (pid_t pid, long long *a, int m);
int am_get_memory_stats (am_memory_stat_t *S, int flags);

typedef struct {
  char *buff;
  int pos;
  int size;
} stats_buffer_t;
void sb_prepare (stats_buffer_t *sb, struct connection *c, char *buff, int size);
void sb_printf (stats_buffer_t *sb, const char *format, ...) __attribute__ ((format (printf, 2, 3)));
void sb_memory (stats_buffer_t *sb, int flags);
void sb_print_queries (stats_buffer_t *sb, const char *const desc, long long q);

int get_at_prefix_length (const char *key, int key_len);
#endif
