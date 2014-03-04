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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/resource.h>
#include <aio.h>

#include "kfs.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "cache-data.h"
#include "cache-heap.h"
#include "net-crypto-aes.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"cache-simulator-1.00-r11"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define TCP_PORT 11211

#define MAX_NET_RES	(1L << 16)
#define mytime() (get_utime(CLOCK_MONOTONIC))


/*
 *
 *		CACHE ENGINE
 *
 */

struct in_addr settings_addr;

/* stats counters */
static long long binlog_loaded_size;
static double binlog_load_time;

static int next_priority_lists_request_time = 0, next_download_file_time = 0;
static cache_heap_t heap_cached, heap_uncached;
static int heap_cached_files, heap_uncached_files, cached_ptr, uncached_ptr;
static long long cached_bytes = 0;
static int print_unknown_size_uries = 0;

struct {
  long long disk_size; //-D
  long long download_speed; //-S
  long long delay_between_priority_lists_requests; //-R
  long long default_file_size; //-F
  int init_using_greedy_strategy; //-g
  int optimization; //-O
  char *amortization_counters_initialization_string;
} simulation_params = {
  .disk_size = 1LL << 40,
  .download_speed = 10L << 20,
  .delay_between_priority_lists_requests = 86400,
  .default_file_size = 100 << 20,
  .init_using_greedy_strategy = 0,
  .optimization = 1,
  .amortization_counters_initialization_string = "hour,day,week,month"
};

struct {
  double max_priority_lists_request_time;
  double resource_usage_time;
  long long log_readto_pos;
  long long cache_init_files, cache_init_bytes;
  long long download_files, download_bytes;
  long long erased_files, erased_bytes;
  long long cache_hits_files, cache_hits_bytes;
  long long cache_misses_files, cache_misses_bytes;
  long long priority_lists_requests;
  long long priority_lists_requests_after_list_ending;
  long long required_files_with_unknown_size;
  long long with_known_size_files, with_known_size_bytes;
  long long max_known_size;
  int max_retrieved_files_between_two_priority_requests;
  int max_erased_files_between_two_priority_requests;
  int max_uries_in_one_bucket;
  int uries_last_hour_access, uries_last_day_access, uries_last_week_access, uries_last_month_access;
} simulation_stats;

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

static char *human_readable_size (long long size) {
  int i;
  static char buff[32];
  static const struct {
    int shift;
    char *suffix;
  } a[5] = {
    {.shift = 40, .suffix = "T"},
    {.shift = 30, .suffix = "G"},
    {.shift = 20, .suffix = "M"},
    {.shift = 10, .suffix = "K"},
    {.shift = 0, .suffix = ""},
  };
  buff[0] = 0;
  for (i = 0; i < 5; i++) {
    if (size >= 1LL << a[i].shift) {
      assert (snprintf (buff, sizeof (buff), "%.3lf%s", safe_div (size, 1LL << a[i].shift), a[i].suffix) < sizeof (buff));
      break;
    }
  }
  if (!buff[0]) {
    assert (snprintf (buff, sizeof (buff), "%lld", size) < sizeof (buff));
  }
  return buff;
}

static double get_rusage_time (void) {
  struct rusage r;
  if (getrusage (RUSAGE_SELF, &r)) { return 0.0; }
  return r.ru_utime.tv_sec + r.ru_stime.tv_sec + 1e-6 * (r.ru_utime.tv_usec + r.ru_stime.tv_usec);
}

#define PRINT_STAT_FILE(x) fprintf (stderr, "%s_files\t%lld\n%s_bytes\t%lld(%s)\n", \
  #x, simulation_stats.x##_files, #x, simulation_stats.x##_bytes, human_readable_size (simulation_stats.x##_bytes))

#define PRINT_STAT_I64(x) fprintf (stderr, "%s\t%lld\n", #x, simulation_stats.x)
#define PRINT_STAT_I32(x) fprintf (stderr, "%s\t%d\n", #x, simulation_stats.x)
#define PRINT_STAT_TIME(x) fprintf (stderr, "%s\t%.6lfs\n", #x, simulation_stats.x)
#define PRINT_PARAM_I64(x) fprintf (stderr, "%s\t%lld\n", #x, simulation_params.x)
#define PRINT_PARAM_I32(x) fprintf (stderr, "%s\t%d\n", #x, simulation_params.x)
#define PRINT_PARAM_STR(x) fprintf (stderr, "%s\t%s\n", #x, simulation_params.x)

static int uri_in_bucket;

void print_uri (struct cache_uri *U) {
  if (U->last_access > now) {
    vkprintf (1, "U->last_access: %d, now: %d\n", U->last_access, now);
  }
  if (U->last_access >= now - 3600) {
    simulation_stats.uries_last_hour_access++;
  }
  if (U->last_access >= now - 86400) {
    simulation_stats.uries_last_day_access++;
  }
  if (U->last_access >= now - 86400 * 7) {
    simulation_stats.uries_last_week_access++;
  }
  if (U->last_access >= now - 86400 * 30) {
    simulation_stats.uries_last_month_access++;
  }

  if (U->size == -2LL) {
    simulation_stats.required_files_with_unknown_size++;
    if (print_unknown_size_uries) {
      printf ("%s\n", cache_get_uri_name (U));
    }
  } else if (U->size >= 0) {
    simulation_stats.with_known_size_files++;
    simulation_stats.with_known_size_bytes += U->size;
    if (simulation_stats.max_known_size < U->size) {
      simulation_stats.max_known_size = U->size;
    }
  }
  uri_in_bucket++;
  if (U->hnext == NULL) {
    if (simulation_stats.max_uries_in_one_bucket < uri_in_bucket) {
      simulation_stats.max_uries_in_one_bucket = uri_in_bucket;
    }
    uri_in_bucket = 0;
  }
}

static void params (void) {
  fprintf (stderr, "disk_size\t%lld(%s)\n", simulation_params.disk_size, human_readable_size (simulation_params.disk_size));
  fprintf (stderr, "download_speed\t%lld(%s/sec)\n", simulation_params.download_speed, human_readable_size (simulation_params.download_speed));
  PRINT_PARAM_I64(delay_between_priority_lists_requests);
  fprintf (stderr, "default_file_size\t%lld(%s)\n", simulation_params.default_file_size, human_readable_size (simulation_params.default_file_size));
  PRINT_PARAM_I32(init_using_greedy_strategy);
  PRINT_PARAM_STR(amortization_counters_initialization_string);
  PRINT_PARAM_I32(optimization);
  fflush (stderr);
}

static void stats (void) {
  now = time (NULL);
  cache_all_uri_foreach (print_uri, cgsl_order_top);
  PRINT_STAT_FILE(cache_init);
  PRINT_STAT_FILE(download);
  PRINT_STAT_FILE(erased);
  PRINT_STAT_FILE(cache_hits);
  const long long difference_bytes = simulation_stats.cache_hits_bytes - simulation_stats.download_bytes;
  fprintf (stderr, "cache_hits_bytes-download_bytes\t%lld(%s)\n",
    difference_bytes, human_readable_size (difference_bytes));
  fprintf (stderr, "cache_hits_bytes/download_bytes\t%.6lf\n",
    safe_div (simulation_stats.cache_hits_bytes, simulation_stats.download_bytes));
  PRINT_STAT_FILE(cache_misses);
  PRINT_STAT_I64(priority_lists_requests);
  PRINT_STAT_I64(priority_lists_requests_after_list_ending);
  PRINT_STAT_I64(required_files_with_unknown_size);
  PRINT_STAT_I64(with_known_size_files);
  long long average_known_size = simulation_stats.with_known_size_files ? simulation_stats.with_known_size_bytes / simulation_stats.with_known_size_files : 0;
  fprintf (stderr, "average_known_size\t%s\n", human_readable_size (average_known_size));
  fprintf (stderr, "max_known_size\t%s\n", human_readable_size (simulation_stats.max_known_size));
  PRINT_STAT_I32(max_retrieved_files_between_two_priority_requests);
  PRINT_STAT_I32(max_erased_files_between_two_priority_requests);
  PRINT_STAT_TIME(max_priority_lists_request_time);
  simulation_stats.resource_usage_time = get_rusage_time ();
  PRINT_STAT_TIME(resource_usage_time);
  PRINT_STAT_I32(max_uries_in_one_bucket);
  PRINT_STAT_I32(uries_last_hour_access);
  PRINT_STAT_I32(uries_last_day_access);
  PRINT_STAT_I32(uries_last_week_access);
  PRINT_STAT_I32(uries_last_month_access);
  fflush (stderr);
}

//static int heap_cached_all_uries = 1, heap_uncached_all_uries = 1;

static void cache_priority_lists_request (void) {
  vkprintf (3, "<%d> cache_priority_list_request\n", next_priority_lists_request_time);
  double t = -get_rusage_time ();
  int cached_limit = 0, uncached_limit = 0;
  if (simulation_params.optimization) {
    cached_limit = 2 * simulation_stats.max_erased_files_between_two_priority_requests;
    if (cached_limit > CACHE_MAX_HEAP_SIZE) {
      cached_limit = CACHE_MAX_HEAP_SIZE;
    }
    uncached_limit = 2 * simulation_stats.max_retrieved_files_between_two_priority_requests;
    if (uncached_limit > CACHE_MAX_HEAP_SIZE) {
      uncached_limit = CACHE_MAX_HEAP_SIZE;
    }
  }

  if (!cached_limit) {
    cached_limit = CACHE_MAX_HEAP_SIZE;
  }
  if (!uncached_limit) {
    uncached_limit = CACHE_MAX_HEAP_SIZE;
  }

  cache_get_priority_heaps (&heap_cached, &heap_uncached, cached_limit, uncached_limit, &heap_cached_files, &heap_uncached_files);
  vkprintf (2, "heap_cached_files: %d, heap_uncached_files: %d\n", heap_cached_files, heap_uncached_files);

  //heap_cached_all_uries = heap_cached_files < cached_limit;
  //heap_uncached_all_uries = heap_uncached_files < uncached_limit;

  t += get_rusage_time ();
  if (simulation_stats.max_priority_lists_request_time < t) {
    simulation_stats.max_priority_lists_request_time = t;
  }
  cached_ptr = 1;
  uncached_ptr = 0;
  next_download_file_time = next_priority_lists_request_time;
  next_priority_lists_request_time += simulation_params.delay_between_priority_lists_requests;
  simulation_stats.priority_lists_requests++;
}

inline long long cache_get_uri_size (struct cache_uri *U, int required) {
  if (U->size < 0) {
    if (required) {
      U->size = -2LL;
    }
    return simulation_params.default_file_size;
  }
  return U->size;
}

void cache_add (struct cache_uri *U, int t) {
  const long long s = cache_get_uri_size (U, 1);
  vkprintf (2, "%d cache_add: %s (%lld bytes)\n", t, cache_get_uri_name (U), s);
  cached_bytes += s;
  assert (U->local_copy == NULL);
  U->local_copy = "cached";
  simulation_stats.download_files++;
  simulation_stats.download_bytes += s;
}

void cache_remove (struct cache_uri *U, int t) {
  const long long s = cache_get_uri_size (U, 1);
  vkprintf (2, "%d cache_remove: %s (%lld bytes)\n", t, cache_get_uri_name (U), s);
  cached_bytes -= s;
  assert (U->local_copy != NULL);
  U->local_copy = NULL;
  simulation_stats.erased_files++;
  simulation_stats.erased_bytes += s;
}

static void cache_download_next_file (void);

static void resend_priority_lists_request (int t) {
  vkprintf (2, "<%d> resend_priority_lists_request\n", t);
  next_priority_lists_request_time = t;
  simulation_stats.priority_lists_requests_after_list_ending++;
  cache_priority_lists_request ();
  cache_download_next_file ();
}

static void cache_download_next_file (void) {
  if (!simulation_stats.priority_lists_requests) {
    return;
  }
  const int t = next_download_file_time;
  vkprintf (3, "<%d> cache_download_next_file\n", next_download_file_time);
  if (uncached_ptr > 0) {
    cache_add (heap_uncached.H[uncached_ptr], t);
  }
  if (simulation_stats.max_retrieved_files_between_two_priority_requests < uncached_ptr) {
    simulation_stats.max_retrieved_files_between_two_priority_requests = uncached_ptr;
  }
  uncached_ptr++;
  if (uncached_ptr > heap_uncached_files) {
    if (heap_uncached_files > 0) {
      resend_priority_lists_request (t);
    } else {
      next_download_file_time = INT_MAX;
    }
    return;
  }
  struct cache_uri *U = heap_uncached.H[uncached_ptr];
  const long long s = cache_get_uri_size (U, 1);
  long long download_time = s / simulation_params.download_speed;
  if (s % simulation_params.download_speed) {
    download_time++;
  }
  assert (download_time + next_download_file_time <= INT_MAX);
  next_download_file_time += download_time;
  if (next_download_file_time >= next_priority_lists_request_time) {
    return;
  }

  long long min_cache_bytes = simulation_params.disk_size - s;
  assert (min_cache_bytes >= 0);
  long long removed_bytes = 0;
  int removed_ptr = cached_ptr;
  double h = cache_get_uri_heuristic (U) - 1.0;
  while (cached_bytes - removed_bytes > min_cache_bytes && removed_ptr <= heap_cached_files) {
    if (cache_get_uri_heuristic ((struct cache_uri *) heap_cached.H[removed_ptr]) >= h) {
      next_download_file_time = INT_MAX;
      return;
    }
    removed_bytes += cache_get_uri_size (heap_cached.H[removed_ptr], 1);
    removed_ptr++;
  }

  if (cached_bytes - removed_bytes > min_cache_bytes && removed_ptr > heap_cached_files) {
    resend_priority_lists_request (t);
    return;
  }

  while (cached_ptr < removed_ptr) {
    cache_remove (heap_cached.H[cached_ptr++], t);
  }

  if (simulation_stats.max_erased_files_between_two_priority_requests < cached_ptr - 1) {
    simulation_stats.max_erased_files_between_two_priority_requests = cached_ptr - 1;
  }
}

static void uri_access (struct cache_uri *U, int t) {
  if (U == NULL) {
    return;
  }
  U->last_access = now;
  cache_incr (U, t);
  if (U->local_copy) {
    const long long s = cache_get_uri_size (U, 1);
    simulation_stats.cache_hits_files += t;
    simulation_stats.cache_hits_bytes += t * s;
  } else {
    const long long s = cache_get_uri_size (U, 0);
    simulation_stats.cache_misses_files += t;
    simulation_stats.cache_misses_bytes += t * s;
  }
}

static void cache_access_short (struct lev_cache_access_short *E, int t) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 8);
  uri_access (U, t);
}

static void cache_access_long (struct lev_cache_access_long *E, int t) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 16);
  uri_access (U, t);
}

int cache_simulator_replay_logevent (struct lev_generic *E, int size) {
  int s;
  if (!next_priority_lists_request_time && now) {
    next_priority_lists_request_time = (now - (now % simulation_params.delay_between_priority_lists_requests)) + simulation_params.delay_between_priority_lists_requests;
    vkprintf (3, "next_priority_lists_request_time: %d, now: %d\n", next_priority_lists_request_time, now);
  }
  if (next_priority_lists_request_time) {
    if (now >= next_priority_lists_request_time) {
      cache_priority_lists_request ();
    }
    if (now >= next_download_file_time) {
      cache_download_next_file ();
    }
  }

  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return cache_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
      return default_replay_logevent (E, size);
    case LEV_CACHE_ACCESS_SHORT...LEV_CACHE_ACCESS_SHORT+0xff:
      s = sizeof (struct lev_cache_access_short);
      if (size < s) { return -2; }
      if (simulation_stats.log_readto_pos > log_cur_pos ()) {
        cache_access_short ((struct lev_cache_access_short *) E, E->type & 0xff);
      }
      return s;
    case LEV_CACHE_ACCESS_LONG...LEV_CACHE_ACCESS_LONG+0xff:
      s = sizeof (struct lev_cache_access_long);
      if (size < s) { return -2; }
      if (simulation_stats.log_readto_pos > log_cur_pos ()) {
        cache_access_long ((struct lev_cache_access_long *) E, E->type & 0xff);
      }
      return s;
    case LEV_CACHE_URI_ADD...LEV_CACHE_URI_ADD+0xff:
    case LEV_CACHE_URI_DELETE...LEV_CACHE_URI_DELETE+0xff:
      s = sizeof (struct lev_cache_uri) + (E->type & 0xff);
      if (size < s) { return -2; }
      //cache_uri_add ((struct lev_cache_uri *) E, E->type & 0xff);
      return s;
    case LEV_CACHE_SET_SIZE_SHORT:
      s = sizeof (struct lev_cache_set_size_short);
      if (size < s) { return -2; }
      if (simulation_stats.log_readto_pos > log_cur_pos ()) {
        cache_set_size_short ((struct lev_cache_set_size_short *) E);
      }
      return s;
    case LEV_CACHE_SET_SIZE_LONG:
      s = sizeof (struct lev_cache_set_size_long);
      if (size < s) { return -2; }
      if (simulation_stats.log_readto_pos > log_cur_pos ()) {
        cache_set_size_long ((struct lev_cache_set_size_long *) E);
      }
      return s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());

  return -3;
}

void greedy_init_uri_add (struct lev_cache_uri *E, int l) {
  char uri[256];
  memcpy (uri, E->data, l);
  uri[l] = 0;
  struct cache_uri *U = get_uri_f (uri, 0);
  assert (U);
  if (U->local_copy == NULL) {
    long long s = cache_get_uri_size (U, 1);
    if (cached_bytes + s <= simulation_params.disk_size) {
      U->local_copy = "cached";
      cached_bytes += s;
      simulation_stats.cache_init_files++;
      simulation_stats.cache_init_bytes += s;
    }
  }
}

int cache_simulator_greedy_init_replay_logevent (struct lev_generic *E, int size) {
  int s;

  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return cache_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
      return default_replay_logevent (E, size);
    case LEV_CACHE_ACCESS_SHORT...LEV_CACHE_ACCESS_SHORT+0xff:
      s = sizeof (struct lev_cache_access_short);
      if (size < s) { return -2; }
      return s;
    case LEV_CACHE_ACCESS_LONG...LEV_CACHE_ACCESS_LONG+0xff:
      s = sizeof (struct lev_cache_access_long);
      if (size < s) { return -2; }
      return s;
    case LEV_CACHE_URI_ADD...LEV_CACHE_URI_ADD+0xff:
      s = sizeof (struct lev_cache_uri) + (E->type & 0xff);
      if (size < s) { return -2; }
      if (simulation_stats.log_readto_pos > log_cur_pos ()) {
        greedy_init_uri_add ((struct lev_cache_uri *) E, E->type & 0xff);
      }
      return s;
    case LEV_CACHE_SET_SIZE_SHORT:
      s = sizeof (struct lev_cache_set_size_short);
      if (size < s) { return -2; }
      return s;
    case LEV_CACHE_SET_SIZE_LONG:
      s = sizeof (struct lev_cache_set_size_long);
      if (size < s) { return -2; }
      return s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());

  return -3;
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  fprintf (stderr, "%s\n", FullVersionStr);
  fprintf (stderr, "usage: cache-simulator [-v] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] <binlog> \n"
      "\tSimulate cache and prints cache stats.\n"
      "\t-v\toutput statistical and debug information into stderr\n"
      "\t-U\tprint to stdout unknown size URIes\n"
      "\t-g\tFill initial cache using Greedy strategy using additional binlog replaying step.\n"
      "\t\t\tThis option puts first accessed files till there is free space in the cache.\n"
      "\t[-D<disk_size>]\n"
      "\t[-S<download_speed]\tset file retrieving speed from central server to cache server\n"
      "\t[-R<delay_between_priority_lists_requests>]\n"
      "\t[-F<default_file_size>]\n"
      "\t[-T<amortization_counters_initialization_string>]\tcomma separated list of half-live pediods in seconds, also it is possible to use reserved words: hour, day, week and month.\n"
      "\t[-O<optimization_level>]\tdefault optimization_level is %d\n",
      simulation_params.optimization
      );
  exit (2);
}

char *ending (int x) {
  x %= 100;
  if (x / 10 == 1) {
    return "th";
  }
  if (x % 10 == 1) {
    return "st";
  }
  if (x % 10 == 2) {
    return "nd";
  }
  if (x % 10 == 3) {
    return "rd";
  }
  return "th";
}

void play_binlog (const char *const desc) {
  static int replaying_step = 0;
  int i;
  replaying_step++;
  vkprintf (1, "Start %d%s replaying binlog step (%s)\n", replaying_step, ending (replaying_step), desc);
  now = 0;
  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld) from position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);
  binlog_load_time = -mytime();

  clear_log ();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  vkprintf (1, "replay log events started\n");
  i = replay_log (0, 1);
  vkprintf (1, "replay log events finished\n");

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  clear_write_log ();
  close_binlog (Binlog, 1);

}

int main (int argc, char *argv[]) {
  int i;
  char c;
  long long x;

  set_debug_handlers ();
  binlog_disabled = 1;

  while ((i = getopt (argc, argv, "D:F:O:R:S:T:Ua:b:c:dghl:u:v")) != -1) {
    switch (i) {
     case 'D':
     case 'F':
     case 'S':
       c = 0;
       assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
       switch (c | 0x20) {
         case 'k':  x <<= 10; break;
         case 'm':  x <<= 20; break;
         case 'g':  x <<= 30; break;
         case 't':  x <<= 40; break;
         default: assert (c == 0x20);
       }
       assert (x >= 0);
       if (i == 'D') {
         simulation_params.disk_size = x;
       }
       if (i == 'F') {
         simulation_params.default_file_size = x;
       }
       if (i == 'S') {
         simulation_params.download_speed = x;
       }
       break;
    case 'O':
      simulation_params.optimization = atoi (optarg);
      break;
    case 'R':
      simulation_params.delay_between_priority_lists_requests = atoll (optarg);
      break;
    case 'T':
      simulation_params.amortization_counters_initialization_string = optarg;
      break;
    case 'U':
      print_unknown_size_uries = 1;
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'b':
      backlog = atoi (optarg);
      if (backlog <= 0) {
        backlog = BACKLOG;
      }
      break;
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
      	maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'g':
      simulation_params.init_using_greedy_strategy = 1;
      break;
    case 'h':
      usage ();
      return 2;
    case 'l':
      logname = optarg;
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    }
  }
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  aes_load_pwd_file (0);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_dyn_data ();
  cache_hashtable_init (2000000);

  if (cache_set_amortization_tables_initialization_string (simulation_params.amortization_counters_initialization_string) < 0) {
    kprintf ("cache_set_amortization_tables_initialization_string (\"%s\") failed.\n", simulation_params.amortization_counters_initialization_string);
    exit (1);
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

  cache_features_mask &= ~CACHE_FEATURE_REPLAY_DELETE;

  play_binlog ("Init URLs size field");
  simulation_stats.log_readto_pos = log_readto_pos;
  cache_clear_all_acounters ();

  if (simulation_params.init_using_greedy_strategy) {
    replay_logevent = cache_simulator_greedy_init_replay_logevent;
    play_binlog ("Init cache using greedy strategy");
  }

  replay_logevent = cache_simulator_replay_logevent;
  play_binlog ("Simulation");
  params ();
  stats ();
  return 0;
}

