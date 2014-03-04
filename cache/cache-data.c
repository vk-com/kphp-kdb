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

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <stddef.h>
#include <math.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/resource.h>
#include "net-events.h"
#include "crc32.h"
#include "md5.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "kdb-cache-binlog.h"
#include "cache-data.h"
#include "cache-heap.h"
#include "am-hash.h"

//#define PROFILE
//#define CACHE_TEST_MD5_COLLISION
#define URI_LIVING_TIME (86400 * 7)

int cache_id;
int uri_living_time = URI_LIVING_TIME;
int cache_features_mask = -1;
static int index_exists;
static int acounter_off, uri_off, acounters_size;
static int index_timestamp = 0;

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
static int monthly_acounter_off, monthly_acounter_id;
#endif

#define likely(x) __builtin_expect((x),1)
#define unlikely(x) __builtin_expect((x),0)

double get_resource_usage_time (void) {
  struct rusage r;
  if (getrusage (RUSAGE_SELF, &r)) { return 0.0; }
  return r.ru_utime.tv_sec + r.ru_stime.tv_sec + 1e-6 * (r.ru_utime.tv_usec + r.ru_stime.tv_usec);
}

/******************** profiling ********************/
#ifdef PROFILE
#define PROFILE_INCR(x) profile.x++;
#define PROFILE_CLEAR {memset(&profile,0,sizeof(profile)); profile.t = -get_resource_usage_time(); }
struct {
  long long amortization_counter_update_calls;
  double t;
} profile;
#define PROFILE_PRINT_I64(x) kprintf("%s\t%lld\n", #x, profile.x)
#define PROFILE_PRINT_ALL { \
  PROFILE_PRINT_I64(amortization_counter_update_calls);\
  kprintf("rusage_time\t%.6lfs\n",get_resource_usage_time()+profile.t);\
}
#else
#define PROFILE_INCR(x)
#define PROFILE_CLEAR
#define PROFILE_PRINT_ALL
#endif

/******************** amortization table functions ********************/
int amortization_counter_types, acounter_uncached_bucket_id;
static int amortization_counter_heuristic_id;

void time_amortization_table_init (struct time_amortization_table *self, double T) {
  int i;
  self->c = -(M_LN2 / T);
  self->T = T;
  const double hi_mult = exp (self->c * AMORTIZATION_TABLE_SQRT_SIZE), lo_mult = exp (self->c);
  self->hi_exp[0] = self->lo_exp[0] = 1.0;
  for (i = 1; i < AMORTIZATION_TABLE_SQRT_SIZE; i++) {
    self->hi_exp[i] = self->hi_exp[i-1] * hi_mult;
    self->lo_exp[i] = self->lo_exp[i-1] * lo_mult;
  }
}

inline double time_amortization_table_fast_exp (struct time_amortization_table *self, int dt) {
  return (dt < AMORTIZATION_TABLE_SQRT_SIZE * AMORTIZATION_TABLE_SQRT_SIZE) ?
          self->hi_exp[dt >> AMORTIZATION_TABLE_SQRT_SIZE_BITS] * self->lo_exp[dt & AMORTIZATION_TABLE_SQRT_SIZE_MASK] :
          exp (self->c * dt);
}

void amortization_counter_update (struct time_amortization_table *T, struct amortization_counter *C) {
  PROFILE_INCR(amortization_counter_update_calls)

  int dt = log_last_ts - C->last_update_time;
  if (!dt) {
    return;
  }
  if (likely(dt > 0)) {
    C->value *= time_amortization_table_fast_exp (T, dt);
    C->last_update_time = log_last_ts;
  }
}

void amortization_counter_increment (struct time_amortization_table *T, struct amortization_counter *C, int incr) {
  int dt = log_last_ts - C->last_update_time;
  if (!dt) {
    C->value += incr;
    return;
  }
  if (likely(dt > 0)) {
    C->value *= time_amortization_table_fast_exp (T, dt);
    C->last_update_time = log_last_ts;
    C->value += incr;
  } else {
    C->value += incr * time_amortization_table_fast_exp (T, -dt);
  }
}

void amortization_counter_precise_increment (struct time_amortization_table *T, struct amortization_counter_precise *C, int incr) {
  int dt = log_last_ts - C->last_update_time;
  if (!dt) {
    C->value += incr;
    return;
  }
  if (dt > 0) {
    C->value *= time_amortization_table_fast_exp (T, dt);
    C->last_update_time = log_last_ts;
    C->value += incr;
  } else {
    C->value += incr * time_amortization_table_fast_exp (T, -dt);
  }
}

struct time_amortization_table *TAT;
static struct amortization_counter_precise *cum_access_counters;
int stats_counters = 0;
struct amortization_counter_precise *convert_success_counters, *convert_miss_counters, *access_success_counters, *access_miss_counters;

void cache_stats_perf (char *out, int olen, struct amortization_counter_precise *success, struct amortization_counter_precise *miss) {
  int i;
  for (i = 0; i <= stats_counters; i++) {
    double x = success->value, y = x + miss->value;
    x = 100.0 * x;
    x = (y < 1e-6) ? 0.0 : x / y;
    int l = snprintf (out, olen, "%c%.6lf", i ? ',' : '\t', x);
    assert (l < olen);
    olen -= l;
    out += l;
    success++;
    miss++;
  }
}

void cache_stats_counter_incr (struct amortization_counter_precise *C) {
  int i;
  for (i = 0; i <= stats_counters; i++) {
    (C++)->value++;
  }
}

void cache_stats_counter_relax (struct amortization_counter_precise *C) {
  int i;
  for (i = 0; i < stats_counters; i++) {
    amortization_counter_precise_increment (&TAT[i], C++, 0);
  }
}

void cache_stats_relax (void) {
  cache_stats_counter_relax (convert_success_counters);
  cache_stats_counter_relax (convert_miss_counters);
  cache_stats_counter_relax (access_success_counters);
  cache_stats_counter_relax (access_miss_counters);
}

void time_amortization_tables_init (int n, double *T) {
  int i;
  amortization_counter_types = n;
  amortization_counter_heuristic_id = 0;
  TAT = zmalloc (n * sizeof (TAT[0]));
  for (i = 0; i < n; i++) {
    time_amortization_table_init (&TAT[i], T[i]);
    if (T[i] > T[amortization_counter_heuristic_id]) {
      amortization_counter_heuristic_id = i;
    }
  }
  acounter_uncached_bucket_id = n - 1;
  if (0) {
    for (i = n - 2; i >= 0; i--) {
      if (fabs (T[acounter_uncached_bucket_id] - 86400) > fabs (T[i] - 86400)) {
        acounter_uncached_bucket_id = i;
      }
    }
  }
  int next_off = 0;
  int prev_off = next_off;
  if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
    prev_off += sizeof (void *);
  }
  assert (next_off == 0 && (prev_off == 0 || prev_off == sizeof (void *)));

  acounter_off = prev_off;
  if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
    acounter_off += sizeof (void *);
  }

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  monthly_acounter_id = (amortization_counter_types - 1);
  monthly_acounter_off = acounter_off + sizeof (struct amortization_counter) * monthly_acounter_id;
  assert (amortization_counter_types <= CACHE_STAT_MAX_ACOUNTERS);
#endif

  acounters_size = (cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES) ? amortization_counter_types * sizeof (struct amortization_counter) : 0;
  uri_off = acounter_off + acounters_size;
  cum_access_counters = zmalloc0 (sizeof (cum_access_counters[0]) * n);

  stats_counters = n < 2 ? n : 2;
  convert_success_counters = zmalloc0 (sizeof (convert_success_counters[0]) * (stats_counters + 1));
  convert_miss_counters = zmalloc0 (sizeof (convert_miss_counters[0]) * (stats_counters + 1));
  access_success_counters = zmalloc0 (sizeof (access_success_counters[0]) * (stats_counters + 1));
  access_miss_counters = zmalloc0 (sizeof (access_miss_counters[0]) * (stats_counters + 1));

#ifdef CACHE_FEATURE_CORRELATION_STATS
  assert (amortization_counter_types <= CACHE_STAT_MAX_ACOUNTERS);
#endif


}

static int get_acounter_id_by_t (int T) {
  int i;
  for (i = 0; i < amortization_counter_types; i++) {
    if (fabs (TAT[i].T - T) < 0.5) {
      return i;
    }
  }
  return -1;
}

/******************** URI -> md5 hash table ********************/
int uri_hash_prime;
#define URI_CACHE_SIZE 0x100000
#define URI_CACHE_MASK 0x0fffff

long long uries, deleted_uries, uri_bytes, access_short_logevents, access_long_logevents, skipped_access_logevents;
long long local_copies_bytes, cached_uries;
long long uri_cache_hits, get_uri_f_calls, uri_reallocs;

static struct cache_uri *URI_CACHE[URI_CACHE_SIZE];
static struct cache_uri **H;

int cache_hashtable_init (int hash_size) {
  if (hash_size < 100000) {
    kprintf ("hash_size too low (minimal value is 100000).\n");
    return -1;
  }

  uri_hash_prime = am_choose_hash_prime (hash_size);
  H = zmalloc0 (sizeof (H[0]) * uri_hash_prime);
  return 0;
}

#define CACHE_URI_BUCKETS 16384
static struct cache_uri list_uncached[CACHE_URI_BUCKETS];
static struct cache_uri list_cached[CACHE_URI_BUCKETS];

#define PNEXT(p) (*((struct cache_uri **) p->data))
#define PPREV(p) (*((struct cache_uri **) (p->data + sizeof (void *))))

inline void cache_uri_list_remove (struct cache_uri *U) {
  assert (cache_features_mask & CACHE_FEATURE_LONG_QUERIES);
  struct cache_uri *u = PPREV(U), *v = PNEXT(U);
  if (u != NULL && v != NULL) {
    PNEXT(u) = v;
    PPREV(v) = u;
    PPREV(U) = PNEXT(U) = NULL;
  }
}

inline void cache_uri_list_insert (struct cache_uri *list, struct cache_uri *U) {
  assert (cache_features_mask & CACHE_FEATURE_LONG_QUERIES);
  struct cache_uri *v = PNEXT(list);
  PNEXT(list) = U; PPREV(U) = list;
  PPREV(v) = U; PNEXT(U) = v;
}

inline int get_bucket (struct cache_uri *U) {
  struct amortization_counter *C = ((struct amortization_counter *) &U->data[acounter_off]) + acounter_uncached_bucket_id;
  int bucket = (C->value < CACHE_URI_BUCKETS - 0.5) ? (int) C->value : CACHE_URI_BUCKETS - 1;
  assert (bucket >= 0 && bucket < CACHE_URI_BUCKETS);
  return bucket;
}

/* called in the case changing monthly acounter */
static void cache_uri_bucket_reuse (struct cache_uri *U) {
  if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
    if (U->local_copy == NULL) {
      cache_uri_list_remove (U);
      cache_uri_list_insert (list_uncached + get_bucket (U), U);
    } else if (!(cache_features_mask & CACHE_FEATURE_FAST_BOTTOM_ACCESS)) {
      cache_uri_list_remove (U);
      cache_uri_list_insert (list_cached + get_bucket (U), U);
    }
  }
}

char *const cache_get_uri_name (const struct cache_uri *const U) {
  return (char *const) (U->data + uri_off);
}

float cache_uri_get_acounter_value (struct cache_uri *U, int id) {
  assert (id >= 0 && id < amortization_counter_types);
  struct amortization_counter *C = (struct amortization_counter *) &U->data[acounter_off];
  C += id;
  amortization_counter_update (&TAT[id], C);
  return C->value;
}

static int cache_uri_get_size_additional_storage_bytes (const struct cache_uri *U) {
  if (U->size & 0x80000000) {
    return 0;
  }
  return (U->size >> 24);
}

static int cache_get_size_additional_storage_bytes (long long size) {
  if (size < 0x7fffffff) {
    return 0;
  }
  if (size <= 0xffffffffLL) {
    return 1;
  }
  if (size <= 0xffffffffffLL) {
    return 2;
  }
  if (size <= 0xffffffffffffLL) {
    return 3;
  }
  if (size <= 0xffffffffffffffLL) {
    return 4;
  }
  return 5;
}

long long cache_uri_get_size (const struct cache_uri *U) {
  if (U->size == -1) {
    return -1LL;
  }
  if (U->size & 0x80000000) {
    return U->size & 0x7fffffff;
  }
  long long res = U->size & 0xffffff;
  const int n = U->size >> 24;
  int i, s = 24;
  unsigned char *r = (unsigned char *) (U->data + (uri_off + strlen (U->data + uri_off) + 1));
  for (i = 0; i < n; i++) {
    const long long u = *r++;
    res |= u << s;
    s += 8;
  }
  return res;
}

static void uri2md5 (const char *const uri, md5_t *M) {
  md5 ((unsigned char *) uri, strlen (uri), M->c);
  #ifdef CACHE_TEST_MD5_COLLISION
  memset (M->c, 0, 8);
  #endif
}

long long uri2md5_extra_calls;

static int cache_uri_match_md5 (const struct cache_uri *U, const md5_t *uri_md5, int len) {
  if (U->uri_md5_h0 == uri_md5->h[0]) {
    if (len == 8) {
      return 1;
    }
    md5_t m;
    uri2md5 (U->data + uri_off, &m);
    uri2md5_extra_calls++;
    return (m.h[1] == uri_md5->h[1]);
  }
  return 0;
}

static int cache_uri_count_by_md5 (const md5_t *uri_md5, int len) {
  const int h = uri_md5->h[0] % uri_hash_prime;
  struct cache_uri *V;
  int cnt = 0;
  for (V = H[h]; V != NULL; V = V->hnext) {
    if (cache_uri_match_md5 (V, uri_md5, len)) {
      if (++cnt > 1) {
        return cnt;
      }
    }
  }
  return cnt;
}

struct cache_uri *cache_get_uri_by_md5 (md5_t *uri_md5, int len) {
  assert (len == 8 || len == 16);
  const int h = uri_md5->h[0] % uri_hash_prime;
  struct cache_uri **p = H + h, *V;
  while (*p) {
    V = *p;
    if (cache_uri_match_md5 (V, uri_md5, len)) {
      *p = V->hnext;
      V->hnext = H[h];
      H[h] = V;
      return V;
    }
    p = &V->hnext;
  }
  return NULL;
}

inline unsigned compute_uri_cache_slot (const char *const uri) {
  unsigned r = 0;
  const unsigned char *s;
  for (s = (const unsigned char *) uri; *s; s++) {
    r = r * 239U + (*s);
  }
  return r & URI_CACHE_MASK;
}

static void uri_cache_remove (const char *const uri) {
  const unsigned cache_slot = compute_uri_cache_slot (uri);
  if (URI_CACHE[cache_slot] && !strcmp (uri, &URI_CACHE[cache_slot]->data[uri_off])) {
    URI_CACHE[cache_slot] = NULL;
  }
}

struct {
  md5_t uri_md5;
  int computed;
} get_uri_f_last_md5;

static int sizeof_uri (const char *const uri) {
  return offsetof (struct cache_uri, data) + uri_off + strlen (uri) + 1;
}

static int cache_uri_sizeof (const struct cache_uri *U) {
  return (sizeof_uri (U->data + uri_off) + cache_uri_get_size_additional_storage_bytes (U) + 3) & -4;
}

static struct cache_uri *hlist_reverse (struct cache_uri *L) __attribute((warn_unused_result));
static struct cache_uri *hlist_reverse (struct cache_uri *L) {
  struct cache_uri *U, *V, *A = NULL;
  for (U = L; U != NULL; U = V) {
    V = U->hnext;
    U->hnext = A;
    A = U;
  }
  return A;
}

/* without unique checks and without inserting URI into CACHE array */
struct cache_uri *load_index_get_uri_f (const char *const uri) {
  uri2md5 (uri, &get_uri_f_last_md5.uri_md5);
  get_uri_f_last_md5.computed = 1;
  const int h = get_uri_f_last_md5.uri_md5.h[0] % uri_hash_prime;
  assert (h >= 0 && h < uri_hash_prime);
  uries++;
  const int sz = sizeof_uri (uri);
  uri_bytes += (sz + 3) & -4;
  struct cache_uri *V = zmalloc0 (sz);
  V->uri_md5_h0 = get_uri_f_last_md5.uri_md5.h[0];
  V->size = -1;
  strcpy (&V->data[uri_off], uri);
  V->hnext = H[h];
  return H[h] = V;
}

struct cache_uri *get_uri_f (const char *const uri, int force) {
  get_uri_f_calls++;
  get_uri_f_last_md5.computed = 0;
  const unsigned cache_slot = compute_uri_cache_slot (uri);
  if (URI_CACHE[cache_slot] && !strcmp (uri, &URI_CACHE[cache_slot]->data[uri_off])) {
    if (force >= 0) {
      uri_cache_hits++;
      return URI_CACHE[cache_slot];
    }
    URI_CACHE[cache_slot] = NULL;
  }

  uri2md5 (uri, &get_uri_f_last_md5.uri_md5);
  get_uri_f_last_md5.computed = 1;
  int h = get_uri_f_last_md5.uri_md5.h[0] % uri_hash_prime;
  assert (h >= 0 && h < uri_hash_prime);
  struct cache_uri **p = H + h, *V;
  while (*p) {
    V = *p;
    if (!strcmp (uri, &V->data[uri_off])) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = H[h];
        URI_CACHE[cache_slot] = H[h] = V;
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force > 0) {
    uries++;
    const int sz = sizeof_uri (uri);
    uri_bytes += (sz + 3) & -4;
    V = zmalloc0 (sz);
    V->uri_md5_h0 = get_uri_f_last_md5.uri_md5.h[0];
    V->size = -1;
    strcpy (&V->data[uri_off], uri);
    V->hnext = H[h];
    return URI_CACHE[cache_slot] = H[h] = V;
  }
  return NULL;
}

/********************* monthly stat *********************/
#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
#define MONTHLY_STAT_BUCKETS 100

struct {
  long long unreasonable_downloads[2];
  long long cum_unreasonable_downloads[2];
  long long deleted;
  long long total;
  long long cum_total;
} stat_monthly[CACHE_STAT_MAX_ACOUNTERS][MONTHLY_STAT_BUCKETS];

static void cumulative_stat_monthly (int id) {
  int i, j;
  for (i = MONTHLY_STAT_BUCKETS - 1; i >= 0; i--) {
    for (j = 0; j < 2; j++) {
      stat_monthly[id][i].cum_unreasonable_downloads[j] = stat_monthly[id][i].unreasonable_downloads[j];
    }
    stat_monthly[id][i].cum_total = stat_monthly[id][i].total;
    if (i < MONTHLY_STAT_BUCKETS - 1) {
      for (j = 0; j < 2; j++) {
        stat_monthly[id][i].cum_unreasonable_downloads[j] += stat_monthly[id][i+1].cum_unreasonable_downloads[j];
      }
      stat_monthly[id][i].cum_total += stat_monthly[id][i+1].cum_total;
    }
  }
}

static int reasonable_download (struct cache_uri *U, struct cache_local_copy *L) {
  if (!(L->flags & CACHE_LOCAL_COPY_FLAG_MONTHLY_COUNTER)) {
    return -1;
  }
  struct amortization_counter *C = (struct amortization_counter *) &U->data[monthly_acounter_off];
  amortization_counter_update (&TAT[monthly_acounter_id], C);
  struct amortization_counter D;
  D.value = L->cached_counter_value[monthly_acounter_id];
  D.last_update_time = L->cached_at;
  amortization_counter_update (&TAT[monthly_acounter_id], &D);
  if (D.value + 1.0 < C->value) {
    return 2;
  }
  return (D.value + 0.25 < C->value) ? 1 : 0;
}

static void update_monthly_stat (struct cache_uri *U, struct cache_local_copy *L, const int deleted) {
  const int r = reasonable_download (U, L);
  if (r < 0) {
    return;
  }

  const int old_file = L->cached_at + 7 * 86400 < log_last_ts;
  int i;
  for (i = 0; i < amortization_counter_types; i++) {
    int id = (int) L->cached_counter_value[i];
    if (id >= MONTHLY_STAT_BUCKETS) {
      id = MONTHLY_STAT_BUCKETS - 1;
    }
    stat_monthly[i][id].deleted += deleted;
    stat_monthly[i][id].total++;
    if (r < 2) {
      if (old_file || deleted) {
        stat_monthly[i][id].unreasonable_downloads[r]++;
      } else {
        stat_monthly[i][id].total--;
      }
    }
  }
}

static void cache_uri_incr_monthly_stats (struct cache_uri *U, struct cache_local_copy *L) {
  if (cache_features_mask & CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS) {
    int i;
    struct amortization_counter *C = ((struct amortization_counter *) &U->data[acounter_off]);
    for (i = 0; i < amortization_counter_types; i++) {
      amortization_counter_update (&TAT[i], C);
      L->cached_counter_value[i] = C->value;
      C++;
    }
    L->flags |= CACHE_LOCAL_COPY_FLAG_MONTHLY_COUNTER;
  }
}

static void cache_uri_decr_monthly_stats (struct cache_uri *U, struct cache_local_copy *L) {
  if ((cache_features_mask & CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS) && (L->flags & CACHE_LOCAL_COPY_FLAG_MONTHLY_COUNTER) ) {
    update_monthly_stat (U, L, 1);
  }
}
#endif

/********************* server stat *********************/
#define STAT_SERVER_HASH_PRIME 10007
int tot_servers;

static cache_stat_server_t *HSS[STAT_SERVER_HASH_PRIME];

cache_stat_server_t *get_stat_server_f (int id, int force) {
  int h = id % STAT_SERVER_HASH_PRIME;
  assert (h >= 0 && h < STAT_SERVER_HASH_PRIME);
  cache_stat_server_t **p = HSS + h, *V;
  while (*p) {
    V = *p;
    if (id == V->id) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = HSS[h];
        HSS[h] = V;
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force > 0) {
    tot_servers++;
    const int sz = sizeof (cache_stat_server_t);
    V = zmalloc0 (sz);
    V->id = id;
    V->hnext = HSS[h];
    return HSS[h] = V;
  }
  return NULL;
}

#ifdef CACHE_FEATURE_CORRELATION_STATS
#define CORRECLATION_STATS_BUCKETS 100
long long correlaction_stats[CACHE_STAT_MAX_ACOUNTERS][CACHE_STAT_MAX_ACOUNTERS][CORRECLATION_STATS_BUCKETS][CORRECLATION_STATS_BUCKETS];
#endif

void cache_incr (struct cache_uri *U, int t) {
  #ifdef CACHE_FEATURE_CORRELATION_STATS
  int id[CACHE_STAT_MAX_ACOUNTERS];
  #endif
  int i;
  struct amortization_counter *C = (struct amortization_counter *) &U->data[acounter_off];
  struct time_amortization_table *T = TAT;
  for (i = 0; i < amortization_counter_types; i++) {
#ifdef CACHE_FEATURE_CORRELATION_STATS
    id[i] = (int) C->value;
    if (id[i] >= CORRECLATION_STATS_BUCKETS) {
      id[i] = CORRECLATION_STATS_BUCKETS - 1;
    }
    assert (id[i] >= 0);
#endif
    amortization_counter_precise_increment (T, cum_access_counters + i, t);
    amortization_counter_increment (T++, C++, t);
  }
  cache_uri_bucket_reuse (U);

#ifdef CACHE_FEATURE_CORRELATION_STATS
  int j;
  for (j = 1; j < amortization_counter_types; j++) {
    for (i = 0; i < j; i++) {
      correlaction_stats[i][j][id[i]][id[j]] += t;
    }
  }
#endif
}

void cache_uri_free (struct cache_uri *U) {
  if (U == NULL) {
    return;
  }

  if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
    cache_uri_list_remove (U);
  }

  assert (U->local_copy == NULL);
  uries--;
  deleted_uries++;
  const int sz = cache_uri_sizeof (U);
  uri_bytes -= sz;
  zfree (U, sz);
}

long long cached_uries_knowns_size, sum_all_cached_files_sizes;

struct cache_uri *cache_set_size (struct cache_uri *U, long long new_size) __attribute((warn_unused_result));
struct cache_uri *cache_set_size (struct cache_uri *U, long long new_size) {
  assert (U);
  const long long old_size = cache_uri_get_size (U);
  if (old_size == new_size) {
    return U;
  }
  if (U->local_copy != NULL) {
    if (old_size >= 0) {
      cached_uries_knowns_size--;
      sum_all_cached_files_sizes -= old_size;
    }
    if (new_size >= 0) {
      cached_uries_knowns_size++;
      sum_all_cached_files_sizes += new_size;
    }
  }
  const int old_bytes = cache_get_size_additional_storage_bytes (old_size),
            new_bytes = cache_get_size_additional_storage_bytes (new_size),
            create_new_uri = old_bytes != new_bytes;
  struct cache_uri *V = U;
  if (create_new_uri) {
    uri_reallocs++;
    int sz = sizeof_uri (U->data + uri_off);
    V = zmalloc0 (sz + new_bytes);
    uries++;
    uri_bytes += (sz + new_bytes + 3) & -4;
    memcpy (V, U, sz);
    assert (U == get_uri_f (U->data + uri_off, -1));
    char *local_copy = U->local_copy;
    U->local_copy = NULL;
    cache_uri_free (U);
    U = NULL;
    assert (V->local_copy == local_copy);
    int h = V->uri_md5_h0 % uri_hash_prime;
    V->hnext = H[h];
    H[h] = V;
    if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
      if (PPREV(V)) {
        cache_uri_list_insert (PPREV(V), V);
      }
    }
  }
  if (new_size < 0x7fffffff) {
    V->size = new_size | 0x80000000;
  } else {
    int i;
    V->size = (new_size & 0xffffff) | (new_bytes << 24);
    new_size >>= 24;
    unsigned char *w = (unsigned char *) (V->data + (uri_off + strlen (V->data + uri_off) + 1));
    for (i = 0; i < new_bytes; i++) {
      *w++ = new_size & 0xff;
      new_size >>= 8;
    }
  }
  return V;
}

void cache_set_size_short (struct lev_cache_set_size_short *E) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 8);
  assert (U);
  U = cache_set_size (U, E->size);
}

void cache_set_size_long (struct lev_cache_set_size_long *E) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 16);
  assert (U);
  U = cache_set_size (U, E->size);
}

static void cache_access_update_stat_server (struct cache_uri *U);

static void kprintf_md5 (char *msg, md5_t *m, int len) {
  static char output[33], hcyf[16] = "0123456789abcdef";
  int i;
  for (i = 0; i < len; i++) {
    output[2*i] = hcyf[(m->c[i] >> 4)];
    output[2*i+1] = hcyf[m->c[i] & 15];
  }
  output[2*len] = 0;
  kprintf ("%s%s\n", msg, output);
}

static int cache_access_short (struct lev_cache_access_short *E, int t) {
  if (!(cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES)) {
    return -1;
  }
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 8);
  if (U == NULL) {
    if (verbosity >= 1) {
      kprintf_md5 ("wrong cache_access_short: ", (md5_t *) E->data, 8);
    }
    skipped_access_logevents++;
    return -1;
  }
  U->last_access = log_last_ts;
  cache_incr (U, t);
  access_short_logevents++;
  cache_access_update_stat_server (U);
  return 0;
}

static int cache_access_long (struct lev_cache_access_long *E, int t) {
  if (!(cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES)) {
    return -1;
  }
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 16);
  if (U == NULL) {
    if (verbosity >= 1) {
      kprintf_md5 ("wrong cache_access_long: ", (md5_t *) E->data, 16);
    }
    skipped_access_logevents++;
    return -1;
  }
  U->last_access = log_last_ts;
  cache_incr (U, t);
  access_long_logevents++;
  cache_access_update_stat_server (U);
  return 0;
}

static void compute_get_uri_f_last_md5 (const struct cache_uri *U) {
  if (!get_uri_f_last_md5.computed) {
    uri2md5 (U->data + uri_off, &get_uri_f_last_md5.uri_md5);
    uri2md5_extra_calls++;
    get_uri_f_last_md5.computed = 1;
    assert (U->uri_md5_h0 == get_uri_f_last_md5.uri_md5.h[0]);
  }
}

/* cache_get_unique_md5_bytes should be always called right after get_uri_f */
static int cache_get_unique_md5_bytes (const struct cache_uri *U) {
  if (!get_uri_f_last_md5.computed) {
    get_uri_f_last_md5.uri_md5.h[0] = U->uri_md5_h0;
  }
  int cnt = cache_uri_count_by_md5 (&get_uri_f_last_md5.uri_md5, 8);
  if (cnt == 1) {
    return 8;
  }
  if (!cnt) { return 0; }
  assert (cnt > 1);
  compute_get_uri_f_last_md5 (U);
  cnt = cache_uri_count_by_md5 (&get_uri_f_last_md5.uri_md5, 16);
  assert (cnt == 1);
  return 16;
}

int cache_get_file_size (const char *const uri, long long *size) {
  *size = -1;
  struct cache_uri *U = get_uri_f (uri, 0);
  if (U == NULL) {
    return -1;
  }
  *size = cache_uri_get_size (U);
  return (*size >= 0) ? 0 : -1;
}

int cache_uri_has_active_local_copy (struct cache_uri *U);

void cache_do_access (const char *const uri) {
  long long old_uries = uries;
  struct cache_uri *U = get_uri_f (uri, 1);

  cache_stats_counter_incr ((cache_uri_has_active_local_copy (U) == 1) ? access_success_counters : access_miss_counters);

  if (uries != old_uries) {
    int l = strlen (uri);
    assert (l < 256);
    struct lev_cache_uri *E = alloc_log_event (LEV_CACHE_URI_ADD + l, sizeof (struct lev_cache_uri) + l, 0);
    memcpy (E->data, uri, l);
  }
  const int bytes = cache_get_unique_md5_bytes (U);
  if (bytes == 8) {
    struct lev_cache_access_short *E = alloc_log_event (LEV_CACHE_ACCESS_SHORT + 1, sizeof (struct lev_cache_access_short), 0);
    memcpy (E->data, &U->uri_md5_h0, 8);
    cache_access_short (E, 1);
  } else {
    assert (bytes == 16);
    struct lev_cache_access_long *E = alloc_log_event (LEV_CACHE_ACCESS_LONG + 1, sizeof (struct lev_cache_access_long), 0);
    compute_get_uri_f_last_md5 (U);
    memcpy (E->data, get_uri_f_last_md5.uri_md5.c, 16);
    cache_access_long (E, 1);
  }
}

int cache_do_set_size (const char *const uri, long long size) {
  if (size < 0 || size > CACHE_MAX_SIZE) {
    vkprintf (2, "cache_do_set_size (%s, %lld), size is out of range\n", uri, size);
    return -1;
  }
  struct cache_uri *U = get_uri_f (uri, 0);
  if (U == NULL) {
    return -1;
  }
  if (U->size != size) {
    const int bytes = cache_get_unique_md5_bytes (U);
    assert (bytes != 0);
    if (bytes == 8 && size <= 0xffffffffLL) {
      struct lev_cache_set_size_short *E = alloc_log_event (LEV_CACHE_SET_SIZE_SHORT, sizeof (struct lev_cache_set_size_short), (unsigned) size);
      memcpy (E->data, &U->uri_md5_h0, 8);
    } else {
      struct lev_cache_set_size_long *E = alloc_log_event (LEV_CACHE_SET_SIZE_LONG, sizeof (struct lev_cache_set_size_long), 0);
      E->size = size;
      compute_get_uri_f_last_md5 (U);
      memcpy (E->data, get_uri_f_last_md5.uri_md5.c, 16);
    }
    U = cache_set_size (U, size);
  }
  return 0;
}

int cache_acounter (const char *const uri, int T, double *value) {
  const int id = get_acounter_id_by_t (T);
  if (id < 0) {
    return -1;
  }
  struct cache_uri *U = get_uri_f (uri, 0);
  if (U == NULL) {
    if (strcmp (uri, "__CUMULATIVE__")) {
      return -2;
    }
    struct amortization_counter_precise *C = cum_access_counters;
    C += id;
    amortization_counter_precise_increment (&TAT[id], C, 0);
    *value = C->value;
    return 0;
  } else {
    struct amortization_counter *C = (struct amortization_counter *) &U->data[acounter_off];
    C += id;
    amortization_counter_update (&TAT[id], C);
    *value = C->value;
    return 0;
  }
}

static void cache_uri_add (struct lev_cache_uri *E, int l) {
  char uri[256];
  memcpy (uri, E->data, l);
  uri[l] = 0;
  get_uri_f (uri, 1);
}

static void cache_uri_delete (struct lev_cache_uri *E, int l) {
  assert (log_last_ts > 0);
  if (cache_features_mask & CACHE_FEATURE_REPLAY_DELETE) {
    char uri[256];
    memcpy (uri, E->data, l);
    uri[l] = 0;
    struct cache_uri *U = get_uri_f (uri, 0);
    if (U == NULL) {
      vkprintf (2, "Delete not existing global uri - \"%s\".\n", uri);
      return;
    }
    if (U->last_access >= log_last_ts - uri_living_time + 7200) {
      vkprintf (2, "Skip deleting global uri, since it isn't too old - \"%s\", log_last_ts: %d, last_access: %d\n", uri, log_last_ts, U->last_access);
      return;
    }
    assert (U == get_uri_f (uri, -1));
    cache_uri_free (U);
  }
}

static int heap_acounter_id, heap_acounter_off;

static int cache_heap_cmp_bottom (const void *a, const void *b) {
  const struct cache_uri *U = (const struct cache_uri *) a;
  const struct cache_uri *V = (const struct cache_uri *) b;
  struct amortization_counter *C = ((struct amortization_counter *) &U->data[heap_acounter_off]);
  struct amortization_counter *D = ((struct amortization_counter *) &V->data[heap_acounter_off]);
  return C->value < D->value ? -1 : likely(C->value > D->value) ? 1 : strcmp (U->data + uri_off, V->data + uri_off);
}

static int cache_heap_cmp_top (const void *a, const void *b) {
  const struct cache_uri *U = (const struct cache_uri *) a;
  const struct cache_uri *V = (const struct cache_uri *) b;
  struct amortization_counter *C = ((struct amortization_counter *) &U->data[heap_acounter_off]);
  struct amortization_counter *D = ((struct amortization_counter *) &V->data[heap_acounter_off]);
  return C->value > D->value ? -1 : likely(C->value < D->value) ? 1 : -strcmp (U->data + uri_off, V->data + uri_off);
}

void cache_bclear (cache_buffer_t *b, char *buff, int size) {
  b->buff = buff;
  b->size = size;
  b->pos = 0;
}

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }


void cache_bprintf (cache_buffer_t *b, const char *format, ...) {
  if (b->pos >= b->size) { return; }
  va_list ap;
  va_start (ap, format);
  b->pos += vsnprintf (b->buff + b->pos, b->size - b->pos, format, ap);
  va_end (ap);
}

/*
void cache_uri_compute_heuristic (struct cache_uri *U) {
  struct amortization_counter *C = ((struct amortization_counter *) &U->data[acounter_off]) + amortization_counter_heuristic_id;
  amortization_counter_update (&TAT[amortization_counter_heuristic_id], C);
  U->h = C->value;
}
*/

double cache_get_uri_heuristic (const struct cache_uri *U) {
  int i;
  struct amortization_counter *C = (struct amortization_counter *) &U->data[acounter_off];
  double res = C->value;
  C++;
  for (i = 1; i < amortization_counter_types; i++) {
    if (res < C->value) {
      res = C->value;
    }
    C++;
  }
  return res;
}

static int uncached_heap_cmp (const void *a, const void *b) {
  const struct cache_uri *U = (const struct cache_uri *) a, *V = (const struct cache_uri *) b;
  const float x = cache_get_uri_heuristic (U), y = cache_get_uri_heuristic (V);
  if (x > y) {
    return -1;
  } else if (x < y) {
    return 1;
  } else {
    return strcmp (U->data + uri_off, V->data + uri_off);
  }
}

static int cached_heap_cmp (const void *a, const void *b) {
  return uncached_heap_cmp (b, a);
}

void cache_uri_acounters_clear (struct cache_uri *U) {
  memset (&U->data[acounter_off], 0, acounters_size);
}

void cache_clear_all_acounters (void) {
  cache_all_uri_foreach (cache_uri_acounters_clear, cgsl_order_top);
}

static int uncommited_delete_logevents_bytes;
static inline void cache_uri_do_delete (struct cache_uri *U) {
  vkprintf (3, "%s: %s\n", __func__, U->data + uri_off);
  const char *const uri = (char *const) (U->data + uri_off);
  int l = strlen (uri);
  assert (l < 256);
  int sz = sizeof (struct lev_cache_uri) + l;
  struct lev_cache_uri *E = alloc_log_event (LEV_CACHE_URI_DELETE + l, sz, 0);
  memcpy (E->data, uri, l);
  uncommited_delete_logevents_bytes += (sz + 3) & -4;
  if (uncommited_delete_logevents_bytes > (256 << 10)) {
    flush_binlog_forced (1);
    uncommited_delete_logevents_bytes = 0;
  }
  cache_uri_free (U);
}

struct {
  int cur_idx;
} garbage_collector, acounter_relax_collector;

void cache_garbage_collector_init (void) {
  garbage_collector.cur_idx = lrand48 () % uri_hash_prime;
}

int cache_acounters_update_step (int max_steps) {
  const int off = acounter_off + acounter_uncached_bucket_id * sizeof (struct amortization_counter);
  int i = acounter_relax_collector.cur_idx, steps = 0, r = 0;
  while (steps < max_steps && r < max_steps) {
    steps++;
    struct cache_uri *U;
    for (U = H[i]; U != NULL; U = U->hnext) {
      amortization_counter_update (TAT+acounter_uncached_bucket_id, (struct amortization_counter *) &U->data[off]);
      cache_uri_bucket_reuse (U);
      r++;
    }
    if (++i >= uri_hash_prime) {
      i -= uri_hash_prime;
    }
  }
  acounter_relax_collector.cur_idx = i;
  return r;
}

int cache_garbage_collector_step (int max_steps) {
  const int dead_time = log_last_ts - uri_living_time;
  vkprintf (3, "log_last_ts: %d, dead_time: %d\n", log_last_ts, dead_time);
  uncommited_delete_logevents_bytes = 0;
  int i = garbage_collector.cur_idx, steps = 0, r = 0;
  while (steps < max_steps && r < max_steps) {
    steps++;
    struct cache_uri **V = &(H[i]), *U;
    while ((U = *V) != NULL) {
      steps++;
      if (U->local_copy || U->last_access >= dead_time) {
        V = &(U->hnext);
      } else {
        *V = U->hnext; //remove from hash table
        uri_cache_remove (&(U->data[uri_off])); //without this statement, engine fails by assertion in md5_flush
        cache_uri_do_delete (U);
        r++;
      }
    }
    if (++i >= uri_hash_prime) {
      i -= uri_hash_prime;
    }
  }
  garbage_collector.cur_idx = i;
  return r;
}

/******************** foreach routines ********************/
static void cache_uri_lists_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order, struct cache_uri *lists) {
  assert (cache_features_mask & CACHE_FEATURE_LONG_QUERIES);
  int i;
  struct cache_uri *U, *B;
  if (order == cgsl_order_top) {
    for (i = CACHE_URI_BUCKETS - 1; i >= 0; i--) {
      B = &lists[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        func (U);
      }
    }
  } else {
    assert (order == cgsl_order_bottom);
    for (i = 0; i < CACHE_URI_BUCKETS; i++) {
      B = &lists[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        func (U);
      }
    }
  }
}

static void cache_uri_lists_nosize_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order, struct cache_uri *lists) {
  assert (cache_features_mask & CACHE_FEATURE_LONG_QUERIES);
  int i;
  struct cache_uri *U, *B;
  if (order == cgsl_order_top) {
    for (i = CACHE_URI_BUCKETS - 1; i >= 0; i--) {
      B = &lists[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        if (U->size == -1) {
          func (U);
        }
      }
    }
  } else {
    assert (order == cgsl_order_bottom);
    for (i = 0; i < CACHE_URI_BUCKETS; i++) {
      B = &lists[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        if (U->size == -1) {
          func (U);
        }
      }
    }
  }
}

static void cache_all_uri_uncached_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order) {
  cache_uri_lists_foreach (func, order, list_uncached);
}

static void cache_all_uri_uncached_nosize_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order) {
  cache_uri_lists_nosize_foreach (func, order, list_uncached);
}

static void cache_all_uri_cached_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order) {
  cache_uri_lists_foreach (func, order, list_cached);
}

static void cache_all_uri_cached_nosize_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order) {
  cache_uri_lists_nosize_foreach (func, order, list_cached);
}

void cache_all_uri_nosize_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order) {
  if (order == cgsl_order_top) {
    cache_all_uri_cached_nosize_foreach (func, order);
    cache_all_uri_uncached_nosize_foreach (func, order);
  } else {
    assert (order == cgsl_order_bottom);
    cache_all_uri_uncached_nosize_foreach (func, order);
    cache_all_uri_cached_nosize_foreach (func, order);
  }
}

void cache_all_uri_foreach (void (*func)(struct cache_uri *), enum cache_sorted_order order) {
  assert (cache_features_mask & CACHE_FEATURE_LONG_QUERIES);
  int i;
  struct cache_uri *U, *B;
  if (order == cgsl_order_top) {
    for (i = CACHE_URI_BUCKETS - 1; i >= 0; i--) {
      B = &list_cached[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        func (U);
      }
      B = &list_uncached[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        func (U);
      }
    }
  } else {
    assert (order == cgsl_order_bottom);
    for (i = 0; i < CACHE_URI_BUCKETS; i++) {
      B = &list_uncached[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        func (U);
      }
      B = &list_cached[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        func (U);
      }
    }
  }
}

/******************** local copy routines  ********************/

char *cache_uri_get_basename (struct cache_uri *U) {
  char *r = strrchr (U->data + uri_off, '/');
  if (r == NULL) {
    return (U->data + uri_off);
  }
  return r + 1;
}

int cache_local_copy_try_pack_location (struct cache_uri *U, struct cache_local_copy *L) {
  int node_id, server_id, disk_id;
  char basename[256], tmp[256+64];
  L->packed_location = 0;
  L->flags &= ~CACHE_LOCAL_COPY_FLAG_INT;
  if (sscanf (L->location, "cs%d_%d/d%d/%255s", &node_id, &server_id, &disk_id, basename) == 4 &&
     node_id >= 1 && node_id <= MAX_NODE_ID &&
     server_id >= 1 && server_id <= MAX_SERVER_ID &&
     disk_id >= 1 && disk_id <= MAX_DISK_ID &&
     !strcmp (cache_uri_get_basename (U), basename)) {
    /* fix for node_id, server_id with leading zeroes */
    assert (snprintf (tmp, sizeof (tmp), "cs%d_%d/d%d/%s", node_id, server_id, disk_id, basename) < sizeof (tmp));
    if (!strcmp (tmp, L->location)) {
      union cache_packed_local_copy_location u;
      u.p.node_id = node_id;
      u.p.server_id = server_id;
      u.p.disk_id = disk_id;
      L->packed_location = u.i;
      L->flags |= CACHE_LOCAL_COPY_FLAG_INT;
      return 0;
    }
  }
  return -1;
}

void cache_local_copy_unpack_location (struct cache_uri *U, struct cache_local_copy *L) {
  assert (L->packed_location);
  union cache_packed_local_copy_location u;
  u.i = L->packed_location;
  assert (snprintf (L->location, sizeof (L->location), "cs%d_%d/d%d/%s",
          (int) u.p.node_id,
          (int) u.p.server_id,
          (int) u.p.disk_id,
          cache_uri_get_basename (U)) < sizeof (L->location));
}

int cache_local_copy_pack (struct cache_local_copy *L, int ilen, char *output, int olen) {
  int i, o = 0;
  if (ilen <= 0) {
    return 0;
  }
  for (i = ilen - 2; i >= 0; i--) {
    L[i].flags &= ~CACHE_LOCAL_COPY_FLAG_LAST;
  }
  L[ilen-1].flags |= CACHE_LOCAL_COPY_FLAG_LAST;
  for (i = 0; i < ilen; i++) {
    L[i].flags &= ~CACHE_LOCAL_COPY_FLAG_INT;
    if (L[i].packed_location) {
      L[i].flags |= CACHE_LOCAL_COPY_FLAG_INT;
    }

    if (o + 4 > olen) {
      return -1;
    }
    memcpy (output + o, &L[i].flags, 4);
    o += 4;
    if (o + 4 > olen) {
      return -1;
    }
    memcpy (output + o, &L[i].cached_at, 4);
    o += 4;

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
    if (L[i].flags & CACHE_LOCAL_COPY_FLAG_MONTHLY_COUNTER) {
      if (o + 4 * amortization_counter_types > olen) {
        return -1;
      }
      memcpy (output + o, &L[i].cached_counter_value, 4 * amortization_counter_types);
      o += 4 * amortization_counter_types;
    }
#endif

    if (L[i].flags & CACHE_LOCAL_COPY_FLAG_YELLOW_LIGHT_MASK) {
      if (o + 4 > olen) {
        return -1;
      }
      memcpy (output + o, &L[i].yellow_light_start, 4);
      o += 4;
    }
    if (L[i].flags & CACHE_LOCAL_COPY_FLAG_INT) {
      if (o + 4 > olen) {
        return -1;
      }
      memcpy (output + o, &L[i].packed_location, 4);
      o += 4;
    } else {
      int l = strlen (L[i].location) + 1;
      if (o + l > olen) {
        return -1;
      }
      memcpy (output + o, L[i].location, l);
      o += l;
    }
  }
  return o;
}

int cache_local_copy_unpack (struct cache_uri *U, struct cache_local_copy *L, int olen, int unpack_location, int *local_copy_len) {
  int r = 0;
  if (U->local_copy == NULL) {
    if (local_copy_len) {
      *local_copy_len = 0;
    }
    return 0;
  }
  char *s = U->local_copy;
  do {
    if (r >= olen) {
      return -1;
    }
    memcpy (&L->flags, s, 4);
    s += 4;
    memcpy (&L->cached_at, s, 4);
    s += 4;

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
    if (L->flags & CACHE_LOCAL_COPY_FLAG_MONTHLY_COUNTER) {
      memcpy (L->cached_counter_value, s, 4 * amortization_counter_types);
      s += 4 * amortization_counter_types;
    }
#endif

    if (L->flags & CACHE_LOCAL_COPY_FLAG_YELLOW_LIGHT_MASK) {
      memcpy (&L->yellow_light_start, s, 4);
      s += 4;
    }
    if (L->flags & CACHE_LOCAL_COPY_FLAG_INT) {
      memcpy (&L->packed_location, s, 4);
      s += 4;
      if (unpack_location) {
        cache_local_copy_unpack_location (U, L);
      }
    } else {
      L->packed_location = 0;
      strcpy (L->location, s);
      s += strlen (s) + 1;
    }
    r++;
    if (L->flags & CACHE_LOCAL_COPY_FLAG_LAST) {
      break;
    }
    L++;
  } while (1);

  if (local_copy_len) {
    *local_copy_len = s - U->local_copy;
  }
  return r;
}

#define CACHE_MAX_LOCAL_COPY_BUFFSIZE 16384
static char local_copy_buff[CACHE_MAX_LOCAL_COPY_BUFFSIZE];
#define CACHE_MAX_LOCAL_COPIES 256
static struct cache_local_copy LC[CACHE_MAX_LOCAL_COPIES];

int cache_uri_has_active_local_copy (struct cache_uri *U) {
  int i, n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, NULL);
  if (n < 0) {
    return -1;
  }
  for (i = 0; i < n; i++) {
    union cache_packed_local_copy_location u;
    if (cache_local_copy_get_flags (LC + i, &u) == 1) {
      return 1;
    }
  }
  return 0;
}

static int get_bucket_by_packed_location (int packed_location) {
  union cache_packed_local_copy_location u;
  u.i = packed_location;
  return (u.p.server_id + (((int) u.p.node_id) << 8)) % CACHE_URI_BUCKETS;
}

static void cache_uri_bucket_insert_by_server (struct cache_uri *U, struct cache_local_copy *L, int n) {
  const int mask = (CACHE_FEATURE_LONG_QUERIES | CACHE_FEATURE_FAST_BOTTOM_ACCESS);
  assert ((cache_features_mask & mask) == mask);
  int bucket = 0;
  if (n == 1 && (L[0].flags & CACHE_LOCAL_COPY_FLAG_INT)) {
    bucket = get_bucket_by_packed_location (L[0].packed_location);
  }
  cache_uri_list_insert (list_cached + bucket, U);
}

int cache_uri_update_local_copy (struct cache_uri *U, struct cache_local_copy *L, int n, int old_len) {
  int o = cache_local_copy_pack (L, n, local_copy_buff, CACHE_MAX_LOCAL_COPY_BUFFSIZE);
  assert (o >= 0);
  if (o == old_len && U->local_copy) {
    memcpy (U->local_copy, local_copy_buff, o);
    return 0;
  }
  const int old_is_cached = U->local_copy ? 1 : 0;
  if (U->local_copy) {
    zfree (U->local_copy, old_len);
    local_copies_bytes -= (old_len + 3) & -4;
    U->local_copy = NULL;
    cached_uries--;
    const long long sz = cache_uri_get_size (U);
    if (sz >= 0) {
      cached_uries_knowns_size--;
      sum_all_cached_files_sizes -= sz;
    }
  }
  if (!o) {
    assert (U->local_copy == NULL);
  } else {
    U->local_copy = zmalloc (o);
    local_copies_bytes += (o + 3) & -4;
    memcpy (U->local_copy, local_copy_buff, o);
    cached_uries++;
    const long long sz = cache_uri_get_size (U);
    if (sz >= 0) {
      cached_uries_knowns_size++;
      sum_all_cached_files_sizes += sz;
    }
  }

  if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
    const int new_is_cached = U->local_copy ? 1 : 0;
    if (new_is_cached && (cache_features_mask & CACHE_FEATURE_FAST_BOTTOM_ACCESS)) {
      cache_uri_list_remove (U);
      cache_uri_bucket_insert_by_server (U, L, n);
    } else {
      if (old_is_cached != new_is_cached) {
        cache_uri_bucket_reuse (U);
      }
    }
  }
  return 0;
}

static void cache_local_copy_compute_packed_location (struct cache_local_copy *L, union cache_packed_local_copy_location *u);

static void cache_uri_incr_server_stats0 (const struct cache_uri *U, union cache_packed_local_copy_location u) {
  if (!(cache_features_mask & CACHE_FEATURE_DETAILED_SERVER_STATS)) {
    return;
  }
  const long long sz = cache_uri_get_size (U);
  if (sz >= 0) {
    u.p.disk_id = 0;
    cache_stat_server_t *S = get_stat_server_f (u.i, 1);
    S->files++;
    S->files_bytes += sz;
  }
}

static void cache_uri_incr_server_stats (const struct cache_uri *U, struct cache_local_copy *L) {
  if (!(cache_features_mask & CACHE_FEATURE_DETAILED_SERVER_STATS)) {
    return;
  }
  const long long sz = cache_uri_get_size (U);
  if (sz >= 0) {
    union cache_packed_local_copy_location u;
    cache_local_copy_compute_packed_location (L, &u);
    if (u.i) {
      u.p.disk_id = 0;
      cache_stat_server_t *S = get_stat_server_f (u.i, 1);
      S->files++;
      S->files_bytes += sz;
    }
  }
}

static void cache_uri_decr_server_stats (struct cache_uri *U, struct cache_local_copy *L) {
  if (!(cache_features_mask & CACHE_FEATURE_DETAILED_SERVER_STATS)) {
    return;
  }
  const long long sz = cache_uri_get_size (U);
  if (sz >= 0) {
    union cache_packed_local_copy_location u;
    cache_local_copy_compute_packed_location (L, &u);
    if (u.i) {
      u.p.disk_id = 0;
      cache_stat_server_t *S = get_stat_server_f (u.i, 0);
      assert (S);
      S->files--;
      S->files_bytes -= sz;
    }
  }
}

static void cache_local_copy_init (struct cache_local_copy *dest) {
  memset (dest, 0, 1 + offsetof (struct cache_local_copy, location));
  dest->cached_at = log_last_ts;
}

static void cache_local_copy_cpy (struct cache_local_copy *dest, struct cache_local_copy *src) {
  memcpy (dest, src, offsetof (struct cache_local_copy, location));
  strcpy (dest->location, src->location);
}

int cache_do_set_new_local_copy (const char *const global_uri, const char *const local_uri) {
  vkprintf (3, "cache_do_set_new_local_copy (%s, %s)\n", global_uri, local_uri);
  struct cache_uri *U = get_uri_f (global_uri, 0);
  if (U == NULL) {
    return -1;
  }
  LC[0].flags = 0;
  strcpy (LC[0].location, local_uri);
  union cache_packed_local_copy_location u;
  cache_local_copy_compute_packed_location (LC, &u);
  if (!u.i) {
    vkprintf (2, "Couldn't compute ${node_id},${server_id},${disk_id} for local uri: %s\n", local_uri);
    return -1;
  }
  int i, n, old_len;
  n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, &old_len);
  if (n < 0) {
    return -1;
  }
  for (i = 0; i < n; i++) {
    if (!strcmp (LC[i].location, local_uri)) {
      vkprintf (2, "cache_do_set_new_local_copy (global_uri: %s, local_uri: %s): ignore duplicate set.\n", global_uri, local_uri);
      return -1;
    }
  }
  struct cache_local_copy *L = &LC[n++];
  cache_local_copy_init (L);
  const int l = strlen (local_uri);
  assert (l < 256);
  strcpy (L->location, local_uri);

  const int bytes = cache_get_unique_md5_bytes (U);
  assert (bytes != 0);
  if (!cache_local_copy_try_pack_location (U, L) && bytes == 8) {
    struct lev_cache_set_new_local_copy_short *E = alloc_log_event (LEV_CACHE_SET_NEW_LOCAL_COPY_SHORT, sizeof (*E), L->packed_location);
    memcpy (E->data, &U->uri_md5_h0, 8);
  } else {
    struct lev_cache_set_new_local_copy_long *E = alloc_log_event (LEV_CACHE_SET_NEW_LOCAL_COPY_LONG + l, sizeof (*E) + l, 0);
    compute_get_uri_f_last_md5 (U);
    memcpy (E->md5, get_uri_f_last_md5.uri_md5.c, 16);
    memcpy (E->data, local_uri, l);
  }

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  cache_uri_incr_monthly_stats (U, L);
#endif
  cache_uri_incr_server_stats0 (U, u);
  cache_uri_update_local_copy (U, LC, n, old_len);
  return 0;
}

static void cache_local_copy_set_yellow_light (struct cache_local_copy *L, int duration) {
  vkprintf (4, "cache_local_copy_set_yellow_light (duration: %d)\n", duration);
  L->flags &= ~CACHE_LOCAL_COPY_FLAG_YELLOW_LIGHT_MASK;
  L->flags |= duration;
  L->yellow_light_start = log_last_ts;
}

static int cache_set_local_copy_yellow_light (struct cache_uri *U, const char *const local_uri, int duration) {
  int i, old_len;
  const int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, &old_len);
  if (n < 0) {
    return -1;
  }
  for (i = 0; i < n; i++) {
    if (!strcmp (LC[i].location, local_uri)) {
      break;
    }
  }

  if (i >= n) {
    return -1;
  }

  cache_local_copy_set_yellow_light (LC + i, duration);
  cache_uri_update_local_copy (U, LC, n, old_len);
  return 0;
}

int cache_do_set_yellow_light_remaining (const char *const global_uri, const char *const local_uri, int duration) {
  vkprintf (3, "cache_do_set_local_copy_yellow_light_remaining (\"%s\", \"%s\", %d)\n", global_uri, local_uri, duration);
  if (duration < 0 || duration > MAX_YELLOW_LIGHT_DURATION) {
    return -1;
  }
  struct cache_uri *U = get_uri_f (global_uri, 0);
  if (U == NULL) {
    return -1;
  }

  int i, n, old_len;
  n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, &old_len);
  if (n < 0) {
    return -1;
  }

  for (i = 0; i < n; i++) {
    if (!strcmp (LC[i].location, local_uri)) {
      break;
    }
  }

  if (i >= n) {
    return -1;
  }

  const int bytes = cache_get_unique_md5_bytes (U);
  assert (bytes != 0);
  if ((LC[i].flags & CACHE_LOCAL_COPY_FLAG_INT) && bytes == 8) {
    struct lev_cache_set_local_copy_yellow_light_short *E = alloc_log_event (LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_SHORT, sizeof (*E), duration);
    E->packed_location = LC[i].packed_location;
    memcpy (E->data, &U->uri_md5_h0, 8);
  } else {
    const int l = strlen (local_uri);
    assert (l < 256);
    struct lev_cache_set_local_copy_yellow_light_long *E = alloc_log_event (LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG + l, sizeof (*E) + l, duration);
    compute_get_uri_f_last_md5 (U);
    memcpy (E->md5, get_uri_f_last_md5.uri_md5.c, 16);
    memcpy (E->data, local_uri, l);
  }

  cache_local_copy_set_yellow_light (LC + i, duration);
  cache_uri_update_local_copy (U, LC, n, old_len);

  return 0;
}

void cache_local_copy_get_yellow_light_time (struct cache_local_copy *L, int *remaining_time, int *elapsed_time) {
  *remaining_time = *elapsed_time = 0;
  int duration = L->flags & CACHE_LOCAL_COPY_FLAG_YELLOW_LIGHT_MASK;
  if (!duration || (L->yellow_light_start + duration <= log_last_ts)) {
    return;
  }
  *elapsed_time = log_last_ts - L->yellow_light_start;
  *remaining_time = duration - (*elapsed_time);
}

int cache_get_yellow_light_remaining (const char *const global_uri, const char *const local_uri, int *remaining_time, int *elapsed_time) {
  vkprintf (3, "cache_get_local_copy_yellow_light_remaining (\"%s\", \"%s\")\n", global_uri, local_uri);
  struct cache_uri *U = get_uri_f (global_uri, 0);
  if (U == NULL) {
    return -1;
  }

  int i, n, old_len;
  n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, &old_len);
  if (n < 0) {
    return -1;
  }

  for (i = 0; i < n; i++) {
    if (!strcmp (LC[i].location, local_uri)) {
      break;
    }
  }

  if (i >= n) {
    return -1;
  }
  cache_local_copy_get_yellow_light_time (LC + i, remaining_time, elapsed_time);
  return 0;
}

int cache_do_delete_local_copy (const char *const global_uri, const char *const local_uri) {
  vkprintf (3, "cache_do_delete_local_copy (%s, %s)\n", global_uri, local_uri);
  struct cache_uri *U = get_uri_f (global_uri, 0);
  if (U == NULL) {
    return -1;
  }
  int i, n, old_len;
  n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, &old_len);
  if (n < 0) {
    return -1;
  }
  for (i = 0; i < n; i++) {
    if (!strcmp (LC[i].location, local_uri)) {
      break;
    }
  }
  if (i >= n) {
    return -1;
  }
  const int bytes = cache_get_unique_md5_bytes (U);
  assert (bytes != 0);
  if ((LC[i].flags & CACHE_LOCAL_COPY_FLAG_INT) && bytes == 8) {
    struct lev_cache_set_new_local_copy_short *E = alloc_log_event (LEV_CACHE_DELETE_LOCAL_COPY_SHORT, sizeof (*E), LC[i].packed_location);
    memcpy (E->data, &U->uri_md5_h0, 8);
  } else {
    const int l = strlen (local_uri);
    assert (l < 256);
    struct lev_cache_set_new_local_copy_long *E = alloc_log_event (LEV_CACHE_DELETE_LOCAL_COPY_LONG + l, sizeof (*E) + l, 0);
    compute_get_uri_f_last_md5 (U);
    memcpy (E->md5, get_uri_f_last_md5.uri_md5.c, 16);
    memcpy (E->data, local_uri, l);
  }

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  cache_uri_decr_monthly_stats (U, LC + i);
#endif
  cache_uri_decr_server_stats (U, LC + i);

  n--;
  if (i != n) {
    /* usually URI has only one local copy, so this assignement executes rarely */
    cache_local_copy_cpy (&LC[i], &LC[n]);
  }

  cache_uri_update_local_copy (U, LC, n, old_len);
  return 0;
}

static int cache_set_new_local_copy (struct cache_uri *U, struct cache_local_copy *L) {
  int n, old_len;
  n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, &old_len);
  if (n < 0 || n >= CACHE_MAX_LOCAL_COPIES) {
    return -1;
  }

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  cache_uri_incr_monthly_stats (U, L);
#endif
  cache_uri_incr_server_stats (U, L);
  cache_local_copy_cpy (&LC[n++], L);

  return cache_uri_update_local_copy (U, LC, n, old_len);
}

static int cache_set_new_local_copy_short (struct lev_cache_set_new_local_copy_short *E) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 8);
  assert (U);
  struct cache_local_copy L;
  cache_local_copy_init (&L);
  L.flags |= CACHE_LOCAL_COPY_FLAG_INT;
  L.packed_location = E->packed_location;
  return cache_set_new_local_copy (U, &L);
}

static int cache_set_new_local_copy_long (struct lev_cache_set_new_local_copy_long *E, int local_url_len) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->md5, 16);
  assert (U);
  struct cache_local_copy L;
  cache_local_copy_init (&L);
  assert (local_url_len < sizeof (L.location));
  memcpy (L.location, E->data, local_url_len);
  L.location[local_url_len] = 0;
  cache_local_copy_try_pack_location (U, &L);
  return cache_set_new_local_copy (U, &L);
}


static int cache_set_local_copy_yellow_light_short (struct lev_cache_set_local_copy_yellow_light_short *E) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 8);
  assert (U);
  struct cache_local_copy L;
  memset (&L, 0, sizeof (L));
  L.flags |= CACHE_LOCAL_COPY_FLAG_INT;
  L.packed_location = E->packed_location;
  cache_local_copy_unpack_location (U, &L);
  return cache_set_local_copy_yellow_light (U, L.location, E->yellow_light_duration);
}

static int cache_set_local_copy_yellow_light_long (struct lev_cache_set_local_copy_yellow_light_long *E, int local_url_len) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->md5, 16);
  assert (U);
  struct cache_local_copy L;
  memset (&L, 0, sizeof (L));
  assert (local_url_len < sizeof (L.location));
  memcpy (L.location, E->data, local_url_len);
  L.location[local_url_len] = 0;
  //cache_local_copy_try_pack_location (U, &L);
  return cache_set_local_copy_yellow_light (U, L.location, E->yellow_light_duration);
}

static int cache_delete_local_copy (struct cache_uri *U, struct cache_local_copy *L) {
  int i, n, old_len;
  n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, &old_len);
  if (n < 0 || n >= CACHE_MAX_LOCAL_COPIES) {
    return -1;
  }
  if (L->flags & CACHE_LOCAL_COPY_FLAG_INT) {
    cache_local_copy_unpack_location (U, L);
  }
  for (i = 0; i < n; i++) {
    if (!strcmp (LC[i].location, L->location)) {
      break;
    }
  }
  if (i >= n) {
    return -1;
  }

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  cache_uri_decr_monthly_stats (U, LC + i);
#endif
  cache_uri_decr_server_stats (U, LC + i);

  n--;
  if (i != n) {
    cache_local_copy_cpy (&LC[i], &LC[n]);
  }
  return cache_uri_update_local_copy (U, LC, n, old_len);
}

static int cache_delete_local_copy_short (struct lev_cache_set_new_local_copy_short *E) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->data, 8);
  assert (U);
  struct cache_local_copy L;
  memset (&L, 0, sizeof (L));
  L.flags |= CACHE_LOCAL_COPY_FLAG_INT;
  L.packed_location = E->packed_location;
  return cache_delete_local_copy (U, &L);
}

static int cache_delete_local_copy_long (struct lev_cache_set_new_local_copy_long *E, int local_url_len) {
  struct cache_uri *U = cache_get_uri_by_md5 ((md5_t *) E->md5, 16);
  assert (U);
  struct cache_local_copy L;
  memset (&L, 0, sizeof (L));
  assert (local_url_len < sizeof (L.location));
  memcpy (L.location, E->data, local_url_len);
  L.location[local_url_len] = 0;
  return cache_delete_local_copy (U, &L);
}

static void cache_access_update_stat_server (struct cache_uri *U) {
  if (cache_features_mask & CACHE_FEATURE_DETAILED_SERVER_STATS) {
    int old_len;
    const int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, &old_len);
    if (n > 0) {
      const double d = 1.0 / n;
      int i;
      for (i = 0; i < n; i++) {
        union cache_packed_local_copy_location u;
        cache_local_copy_compute_packed_location (LC+i, &u);
        u.p.disk_id = 0;
        if (u.i) {
          cache_stat_server_t *S = get_stat_server_f (u.i, 1);
          S->access_queries += d;
        }
      }
    }
  }
}

/******************** disabled disks routines ********************/
struct cache_local_copy_server {
  struct cache_local_copy_server *hnext;
  int id;
  unsigned bitset[8];
};

#define DISABLED_SERVER_HASH_PRIME 1103

static struct cache_local_copy_server *HDS[DISABLED_SERVER_HASH_PRIME];
int tot_disabled_servers;

struct cache_local_copy_server *get_disabled_server_f (int id, int force) {
  int h = id % DISABLED_SERVER_HASH_PRIME;
  assert (h >= 0 && h < DISABLED_SERVER_HASH_PRIME);
  struct cache_local_copy_server **p = HDS + h, *V;
  while (*p) {
    V = *p;
    if (id == V->id) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = HDS[h];
        HDS[h] = V;
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force > 0) {
    tot_disabled_servers++;
    const int sz = sizeof (struct cache_local_copy_server);
    V = zmalloc0 (sz);
    V->id = id;
    V->hnext = HDS[h];
    return HDS[h] = V;
  }
  return NULL;
}

static int cache_change_disk_status (struct lev_cache_change_disk_status *E, int enable) {
  union cache_packed_local_copy_location u;
  u.i = E->packed_location;
  vkprintf (3, "cache_change_disk_status (node_id:%d, server_id:%d, disk_id:%d, enable:%d)\n",
    (int) u.p.node_id, (int) u.p.server_id, (int) u.p.disk_id, enable);
  int id = u.p.node_id;
  id *= (MAX_SERVER_ID+1);
  id += u.p.server_id;
  if (enable) {
    struct cache_local_copy_server *S = get_disabled_server_f (id, 0);
    if (S != NULL) {
      int keep = 0;
      if (u.p.disk_id) {
        int i;
        S->bitset[u.p.disk_id >> 5] &= ~(1U << (u.p.disk_id & 31));
        for (i = 0; i < 8 && !(S->bitset[i]); i++);
        keep = i < 8;
      }
      if (!keep) {
        S = get_disabled_server_f (id, -1);
        zfree (S, sizeof (*S));
        tot_disabled_servers--;
      }
    }
  } else {
    struct cache_local_copy_server *S = get_disabled_server_f (id, 1);
    if (u.p.disk_id) {
      S->bitset[u.p.disk_id >> 5] |= (1U << (u.p.disk_id & 31));
    } else {
      memset (S->bitset, -1, sizeof (S->bitset));
    }
  }
  return 1;
}

static int cache_is_valid_disk (int node_id, int server_id, int disk_id) {
  if (!(node_id >= 1 && node_id <= MAX_NODE_ID)) {
    return CACHE_ERR_INVALID_NODE_ID;
  }
  if (!(server_id >= 1 && server_id <= MAX_SERVER_ID)) {
    return CACHE_ERR_INVALID_SERVER_ID;
  }
  if (!(disk_id >= 0 && disk_id <= MAX_DISK_ID)) {
    return CACHE_ERR_INVALID_DISK_ID;
  }
  return 0;
}

static int cache_get_disk_status (int node_id, int server_id, int disk_id) {
  struct cache_local_copy_server *S = get_disabled_server_f (node_id * (MAX_SERVER_ID + 1) + server_id, 0);
  if (S == NULL) {
    return 1;
  }
  return (S->bitset[disk_id >> 5] & (1U << (disk_id & 31))) ? 0 : 1;
}

int cache_do_change_disk_status (int node_id, int server_id, int disk_id, int enable) {
  assert (enable == 0 || enable == 1);
  int r = cache_is_valid_disk (node_id, server_id, disk_id);
  if (r < 0) {
    return r;
  }
  union cache_packed_local_copy_location u;
  u.p.node_id = node_id;
  u.p.server_id = server_id;
  u.p.disk_id = disk_id;
  struct lev_cache_change_disk_status *E = alloc_log_event (LEV_CACHE_CHANGE_DISK_STATUS + enable, sizeof (*E), u.i);
  return cache_change_disk_status (E, enable);
}

static void cache_local_copy_compute_packed_location (struct cache_local_copy *L, union cache_packed_local_copy_location *u) {
  if (L->flags & CACHE_LOCAL_COPY_FLAG_INT) {
    u->i = L->packed_location;
  } else {
    int node_id, server_id, disk_id;
    u->i = 0;
    if (sscanf (L->location, "cs%d_%d/d%d", &node_id, &server_id, &disk_id) == 3 &&
        node_id >= 1 && node_id <= MAX_NODE_ID &&
        server_id >= 1 && server_id <= MAX_SERVER_ID &&
        disk_id >= 1 && disk_id <= MAX_DISK_ID
      ) {
      u->p.node_id = node_id;
      u->p.server_id = server_id;
      u->p.disk_id = disk_id;
    }
  }
}

int cache_local_copy_get_flags (struct cache_local_copy *L, union cache_packed_local_copy_location *u) {
  if (L->flags & CACHE_LOCAL_COPY_FLAG_TEMPORARLY_UNAVAILABLE) {
    return 0;
  }
  cache_local_copy_compute_packed_location (L, u);
  int yellow_light_duration = L->flags & CACHE_LOCAL_COPY_FLAG_YELLOW_LIGHT_MASK;
  vkprintf (4, "cache_local_copy_get_flags: yellow_light_duration = %d\n", yellow_light_duration);
  if (yellow_light_duration && log_last_ts <= L->yellow_light_start + yellow_light_duration) {
    return 2;
  }
  return (u->i) ? cache_get_disk_status (u->p.node_id, u->p.server_id, u->p.disk_id) : 1;
}

cache_disk_filter_t disk_filter;

static int cache_uri_local_copy_disk_filter_match (struct cache_local_copy *L, cache_disk_filter_t *F) {
  if (L->flags & CACHE_LOCAL_COPY_FLAG_INT) {
    if (!memcmp (&L->packed_location, &F->packed_location, F->packed_location_len)) {
      return 1;
    }
  } else {
    if (!memcmp (L->location, F->packed_prefix, F->prefix_len)) {
      return 1;
    }
  }
  return 0;
}

struct cache_local_copy *cache_uri_local_copy_find (struct cache_uri *U, cache_disk_filter_t *F) {
  const int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, NULL);
  int i;
  for (i = 0; i < n; i++) {
    if (cache_uri_local_copy_disk_filter_match (LC + i, &disk_filter)) {
      return LC + i;
    }
  }
  return NULL;
}

int cache_uri_delete_remote_disk (struct cache_uri *U, cache_disk_filter_t *F) {
  vkprintf (4, "cache_uri_delete_remote_disk: (U:%p) starting\n", U);
  int i, n, old_len, r = 0;
  n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, &old_len);
  if (n < 0) {
    return -1;
  }
  for (i = 0; i < n; ) {
    if (cache_uri_local_copy_disk_filter_match (LC + i, F)) {
      r++;

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
      cache_uri_decr_monthly_stats (U, LC + i);
#endif
      cache_uri_decr_server_stats (U, LC + i);

      n--;
      if (i != n) {
        cache_local_copy_cpy (&LC[i], &LC[n]);
      }
    } else {
      i++;
    }
  }
  if (r) {
    cache_uri_update_local_copy (U, LC, n, old_len);
  }
  vkprintf (4, "cache_uri_delete_remote_disk: ending\n");
  return r;
}

static void cache_disk_filter_init (cache_disk_filter_t *F, int value) {
  union cache_packed_local_copy_location u;
  u.i = F->packed_location = value;
  F->packed_location_len = 4;
  if (!u.p.disk_id) {
    F->packed_location_len--;
    F->prefix_len = snprintf (F->packed_prefix, sizeof (F->packed_prefix), "cs%d_%d/d",
          (int) u.p.node_id, (int) u.p.server_id);
  } else {
    F->prefix_len = snprintf (F->packed_prefix, sizeof (F->packed_prefix), "cs%d_%d/d%d/",
          (int) u.p.node_id, (int) u.p.server_id, (int) u.p.disk_id);
  }
  assert (F->prefix_len < sizeof (F->packed_prefix));
}

static int cache_delete_remote_disk (struct lev_cache_change_disk_status *E) {
  vkprintf (2, "cache_delete_remote_disk: starting\n");
  cache_change_disk_status (E, 1);
  cache_disk_filter_init (&disk_filter, E->packed_location);
  long long r = 0;
  int i;
  struct cache_uri *U, *B, *W;
  const int bucket = get_bucket_by_packed_location (E->packed_location);
  if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
    for (i = CACHE_URI_BUCKETS - 1; i >= 0; i--) {
      if (cache_features_mask & CACHE_FEATURE_FAST_BOTTOM_ACCESS) {
        if (i != 0 && i != bucket) {
          continue;
        }
      }
      B = &list_cached[i];
      for (U = PNEXT(B); U != B; U = W) {
        W = PNEXT(U);
        /* cache_uri_delete_remote_disk changes U->pnext field,
           since it calls cache_uri_update_local_copy
           which calls cache_uri_bucket_insert,
           in the case when URI flips cached state
        */
        int o = cache_uri_delete_remote_disk (U, &disk_filter);
        if (o > 0) {
          r += o;
        }
      }
    }
  } else {
    for (i = 0; i < uri_hash_prime; i++) {
      for (U = H[i]; U != NULL; U = U->hnext) {
        if (U->local_copy != NULL) {
          int o = cache_uri_delete_remote_disk (U, &disk_filter);
          if (o > 0) {
            r += o;
          }
        }
      }
    }
  }
  vkprintf (2, "cache_delete_remote_disk: r = %lld\n", r);
  return (r < INT_MAX) ? r : INT_MAX;
}

int cache_do_delete_remote_disk (int node_id, int server_id, int disk_id) {
  int r = cache_is_valid_disk (node_id, server_id, disk_id);
  if (r < 0) {
    return r;
  }
  union cache_packed_local_copy_location u;
  u.p.node_id = node_id;
  u.p.server_id = server_id;
  u.p.disk_id = disk_id;
  struct lev_cache_change_disk_status *E = alloc_log_event (LEV_CACHE_DELETE_REMOTE_DISK, sizeof (*E), u.i);
  return cache_delete_remote_disk (E);
}

static int cache_convert (struct cache_uri *U, char *output, int olen) {
  int i, n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, NULL);
  if (n < 0) {
    vkprintf (1, "cache_convert: cache_local_copy_unpack failed.\n");
    return -1;
  }
  int m = 0;
  for (i = 0; i < n; i++) {
    union cache_packed_local_copy_location u;
    int flags = cache_local_copy_get_flags (LC + i, &u);
    vkprintf (4, "cache_local_copy_get_flags returns %d.\n", flags);
    if (flags != 1) {
      continue;
    }
    if (m != i) {
      LC[m] = LC[i];
    }
    m++;
  }
  if (!m) {
    return 0;
  }
  i = lrand48 () % m;
  if (LC[i].flags & CACHE_LOCAL_COPY_FLAG_INT) {
    cache_local_copy_unpack_location (U, LC + i);
  }
  int l = strlen (LC[i].location);
  if (olen < l + 1) {
    vkprintf (1, "cache_convert: output buffer is too small (%d bytes).\n", olen);
    return -1;
  }
  strcpy (output, LC[i].location);
  return 1;
}

int cache_do_convert (const char *const uri, char *output, int olen) {
  struct cache_uri *U = get_uri_f (uri, 0);
  if (U == NULL) {
    return -1;
  }
  return cache_convert (U, output, olen);
}

int cache_do_local_copies (const char *const uri, struct cache_local_copy **R) {
  struct cache_uri *U = get_uri_f (uri, 0);
  if (U == NULL) {
    return -1;
  }
  int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, NULL);
  if (n < 0) {
    vkprintf (1, "cache_do_local_copies (uri: %s): cache_local_copy_unpack failed.\n", uri);
    return -1;
  }
  *R = LC;
  return n;
}

/******************** heap routines  ********************/

static void cache_dump_priority_heap (const char *const heap_name, cache_heap_t *heap, int limit) {
  int j;
  kprintf ("%s:\n", heap_name);
  for (j = 1; j <= limit && j <= 10; j++) {
    struct cache_uri *U = heap->H[j];
    kprintf ("%d: %s %.6lg\n", j, cache_get_uri_name (U), (double) cache_get_uri_heuristic (U));
  }
}

int cache_get_priority_heaps (cache_heap_t *heap_cached, cache_heap_t *heap_uncached, int cached_limit, int uncached_limit, int *r1, int *r2) {
  int i;
  struct cache_uri *U;
  if (cached_limit == 0) {
    cached_limit = CACHE_MAX_HEAP_SIZE;
  }
  if (uncached_limit == 0) {
    uncached_limit = CACHE_MAX_HEAP_SIZE;
  }
  heap_cached->size = 0;
  heap_cached->max_size = (cached_limit < CACHE_MAX_HEAP_SIZE) ? cached_limit : CACHE_MAX_HEAP_SIZE;
  heap_cached->compare = cached_heap_cmp;
  heap_uncached->size = 0;
  heap_uncached->max_size = (uncached_limit < CACHE_MAX_HEAP_SIZE) ? uncached_limit : CACHE_MAX_HEAP_SIZE;
  heap_uncached->compare = uncached_heap_cmp;
  for (i = 0; i < uri_hash_prime; i++) {
    for (U = H[i]; U != NULL; U = U->hnext) {
      //cache_uri_compute_heuristic (U);
      cache_heap_insert (U->local_copy ? heap_cached : heap_uncached, U);
    }
  }
  *r1 = cache_heap_sort (heap_cached);
  if (verbosity >= 3) {
    cache_dump_priority_heap ("cached", heap_cached, *r1);
  }
  *r2 = cache_heap_sort (heap_uncached);
  if (verbosity >= 3) {
    cache_dump_priority_heap ("uncached", heap_uncached, *r2);
  }
  //TODO: protect useful files

  return 0;
}

static cache_heap_t *heap_foreach;
static struct time_amortization_table *tbl_foreach;

static void cache_heap_insert_uri_from_given_disk (struct cache_uri *U) {
  vkprintf (4, "cache_heap_insert_uri_from_given_disk (%s)\n", U->data + uri_off);
  int i;
  const int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, NULL);
  if (n < 0) {
    return;
  }
  for (i = 0; i < n; i++) {
    if (cache_uri_local_copy_disk_filter_match (LC + i, &disk_filter)) {
      struct amortization_counter *C = ((struct amortization_counter *) &U->data[heap_acounter_off]);
      amortization_counter_update (tbl_foreach, C);
      cache_heap_insert (heap_foreach, U);
      return;
    }
  }
}

static void cache_heap_insert_uri (struct cache_uri *U) {
  struct amortization_counter *C = ((struct amortization_counter *) &U->data[heap_acounter_off]);
  amortization_counter_update (tbl_foreach, C);
  cache_heap_insert (heap_foreach, U);
}

#define GET_HEAP_VALUE(k) (((struct amortization_counter *) &((struct cache_uri *) heap_foreach->H[k])->data[heap_acounter_off])->value)

static void cache_top_heap_insert_uri_optimized (struct cache_uri *U) {
  struct amortization_counter *C = ((struct amortization_counter *) &U->data[heap_acounter_off]);
  int i, j;
  if (unlikely(heap_foreach->size < heap_foreach->max_size)) {
    amortization_counter_update (tbl_foreach, C);
    i = ++(heap_foreach->size);
    while (i > 1) {
      j = i >> 1;
      if (GET_HEAP_VALUE(j) <= C->value) {
        break;
      }
      heap_foreach->H[i] = heap_foreach->H[j];
      i = j;
    }
    heap_foreach->H[i] = U;
  } else {
    if (unlikely(GET_HEAP_VALUE(1) < C->value)) {
      amortization_counter_update (tbl_foreach, C);
      if (GET_HEAP_VALUE(1) < C->value) {
        i = 1;
        while (1) {
          j = i << 1;
          if (j > heap_foreach->size) {
            break;
          }
          if (j < heap_foreach->size && GET_HEAP_VALUE(j) > GET_HEAP_VALUE(j+1)) {
            j++;
          }
          if (C->value <= GET_HEAP_VALUE(j)) {
            break;
          }
          heap_foreach->H[i] = heap_foreach->H[j];
          i = j;
        }
        heap_foreach->H[i] = U;
      }
    }
  }
}

static void cache_top_heap_insert_uri_optimized2 (struct cache_uri *U) {
  struct amortization_counter *C = ((struct amortization_counter *) &U->data[heap_acounter_off]);
  if (unlikely(GET_HEAP_VALUE(1) < C->value)) {
    amortization_counter_update (tbl_foreach, C);
    if (GET_HEAP_VALUE(1) < C->value) {
      int i = 1;
      while (1) {
        int j = i << 1;
        if (j > heap_foreach->size) {
          break;
        }
        if (j < heap_foreach->size && GET_HEAP_VALUE(j) > GET_HEAP_VALUE(j+1)) {
          j++;
        }
        if (C->value <= GET_HEAP_VALUE(j)) {
          break;
        }
        heap_foreach->H[i] = heap_foreach->H[j];
        i = j;
      }
      heap_foreach->H[i] = U;
    }
  }
}

static int cache_bottom_fill_heap_from_given_disk (cache_heap_t *heap, int T, int limit, int node_id, int server_id, int disk_id, enum cache_sorted_order order) {
  assert (order == cgsl_order_top || order == cgsl_order_bottom);
  int r = cache_is_valid_disk (node_id, server_id, disk_id);
  if (r < 0) {
    return r;
  }

  if (limit <= 0) {
    return CACHE_ERR_LIMIT;
  }

  heap_foreach = heap;
  heap_acounter_id = get_acounter_id_by_t (T);

  if (heap_acounter_id < 0) {
    return CACHE_ERR_UNKNOWN_T;
  }
  heap_acounter_off = acounter_off + heap_acounter_id * sizeof (struct amortization_counter);

  heap->size = 0;
  heap->max_size = (limit < CACHE_MAX_HEAP_SIZE) ? limit : CACHE_MAX_HEAP_SIZE;
  heap->compare = order == cgsl_order_top ? cache_heap_cmp_top : cache_heap_cmp_bottom;
  tbl_foreach = TAT + heap_acounter_id;
  union cache_packed_local_copy_location u;
  u.p.node_id = node_id;
  u.p.server_id = server_id;
  u.p.disk_id = disk_id;
  cache_disk_filter_init (&disk_filter, u.i);
  if (cache_features_mask & CACHE_FEATURE_FAST_BOTTOM_ACCESS) {
    int i;
    const int bucket = get_bucket_by_packed_location (u.i);
    /* zero bucket contains URIes with multiple locations */
    for (i = bucket; i >= 0; i -= bucket) {
      struct cache_uri *B = &list_cached[i], *U;
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        cache_heap_insert_uri_from_given_disk (U);
      }
    }
  } else {
    cache_all_uri_cached_foreach (cache_heap_insert_uri_from_given_disk, order);
  }
  return cache_heap_sort (heap);
}

static void cache_top_fill_heap_all (int min_rate) {
  if (min_rate >= 1000000) {
    min_rate = min_rate / 1000000 - 0;
  }
  if (min_rate >= CACHE_URI_BUCKETS) {
    min_rate = CACHE_URI_BUCKETS - 1; /* special bucket for counters in [CACHE_URI_BUCKETS - 1, +inf) */
  }

  assert (cache_features_mask & CACHE_FEATURE_LONG_QUERIES);
  int i;
  struct cache_uri *U, *B;
  for (i = CACHE_URI_BUCKETS - 1; i >= min_rate; i--) {
    B = &list_cached[i];
    for (U = PNEXT(B); U != B; U = PNEXT(U)) {
      cache_top_heap_insert_uri_optimized (U);
    }
    B = &list_uncached[i];
    for (U = PNEXT(B); U != B; U = PNEXT(U)) {
      cache_top_heap_insert_uri_optimized (U);
    }
    if (heap_acounter_id <= acounter_uncached_bucket_id && heap_foreach->size == heap_foreach->max_size && GET_HEAP_VALUE(1) > i) {
      vkprintf (2, "Skipping buckets from %d to 0.\n", i - 1);
      break;
    }
  }

  if (cache_features_mask & CACHE_FEATURE_FAST_BOTTOM_ACCESS) {
    while (i >= 0) {
      B = &list_cached[i];
      for (U = PNEXT(B); U != B; U = PNEXT(U)) {
        cache_top_heap_insert_uri_optimized (U);
      }
      i--;
    }
  }
}

static void cache_top_fill_heap_uncached (int min_rate) {
  static struct cache_uri *fake = NULL;
  int i;
  assert (cache_features_mask & CACHE_FEATURE_LONG_QUERIES);
  if (fake == NULL) {
    fake = zmalloc0 (sizeof_uri (""));
  }
  struct amortization_counter *C = ((struct amortization_counter *) &fake->data[heap_acounter_off]);

  if (min_rate >= 1000000) {
    C->value = min_rate * 1e-6; // - 0
    min_rate = min_rate / 1000000 - 0;
  } else {
    C->value = min_rate ? min_rate : -1.0;
  }

  if (min_rate >= CACHE_URI_BUCKETS) {
    min_rate = CACHE_URI_BUCKETS - 1;
  }

  for (i = 1; i <= heap_foreach->max_size; i++) {
    heap_foreach->H[i] = fake;
  }
  heap_foreach->size = heap_foreach->max_size;
  struct cache_uri *U, *B;
  for (i = CACHE_URI_BUCKETS - 1; i >= min_rate; i--) {
    B = &list_uncached[i];
    for (U = PNEXT(B); U != B; U = PNEXT(U)) {
      cache_top_heap_insert_uri_optimized2 (U);
    }
    if (heap_acounter_id <= acounter_uncached_bucket_id && GET_HEAP_VALUE(1) > i) {
      vkprintf (2, "Skipping uncached buckets from %d to 0.\n", i - 1);
      break;
    }
  }
}

int cache_top_fill_heap (cache_heap_t *heap, int T, enum cache_sorted_order order, int limit, int without_size, int uncached_only, int min_rate) {
  if (limit <= 0) {
    return CACHE_ERR_LIMIT;
  }
  heap_foreach = heap;
  heap_acounter_id = get_acounter_id_by_t (T);
  vkprintf (4, "heap_acounter_id: %d\n", heap_acounter_id);
  if (heap_acounter_id < 0) {
    return CACHE_ERR_UNKNOWN_T;
  }
  heap_acounter_off = acounter_off + heap_acounter_id * sizeof (struct amortization_counter);
  heap->size = 0;
  heap->max_size = (limit < CACHE_MAX_HEAP_SIZE) ? limit : CACHE_MAX_HEAP_SIZE;
  heap->compare = order == cgsl_order_top ? cache_heap_cmp_top : cache_heap_cmp_bottom;
  tbl_foreach = TAT + heap_acounter_id;

  PROFILE_CLEAR

  void (*func)(struct cache_uri *) = order == cgsl_order_top ? cache_top_heap_insert_uri_optimized : cache_heap_insert_uri;

  if (min_rate < 0) {
    min_rate = 0;
  }

/*
  if (min_rate >= CACHE_URI_BUCKETS) {
    min_rate = CACHE_URI_BUCKETS - 1;
  }
*/

  if (uncached_only) {
    if (without_size) {
      cache_all_uri_uncached_nosize_foreach (func, order);
    } else {
      if (order == cgsl_order_top) {
        cache_top_fill_heap_uncached (min_rate);
      } else {
        cache_all_uri_uncached_foreach (func, order);
      }
    }
  } else {
    if (without_size) {
      cache_all_uri_nosize_foreach (func, order);
    } else {
      if (order == cgsl_order_top) {
        cache_top_fill_heap_all (min_rate);
      } else {
        cache_all_uri_foreach (func, order);
      }
    }
  }

  PROFILE_PRINT_ALL

  return cache_heap_sort (heap);
}

struct cache_top_stats {
  int selection_size;
  int known_size_files;
  double sum_acounter;
  long long sum_filesize;
  double weighted_sum_filesize;
};

int cache_get_top_stats (int T, enum cache_sorted_order order, int limit, char *output, int olen) {
  cache_heap_t Heap;
  struct cache_top_stats S;
  memset (&S, 0, sizeof (S));
  int i;
  S.selection_size = cache_top_fill_heap (&Heap, T, order, limit, 0, 0, 0);
  if (S.selection_size < 0) {
    return -1;
  }
  for (i = 1; i <= S.selection_size; i++) {
    struct cache_uri *U = Heap.H[i];
    struct amortization_counter *C = ((struct amortization_counter *) &U->data[heap_acounter_off]);
    long long sz = cache_uri_get_size (U);
    if (sz >= 0) {
      S.known_size_files++;
      S.sum_filesize += sz;
      S.weighted_sum_filesize += ((double) C->value) * sz;
    }
    S.sum_acounter += C->value;
  }
  amortization_counter_precise_increment (&TAT[heap_acounter_id], cum_access_counters + heap_acounter_id, 0);

  cache_buffer_t b;
  cache_bclear (&b, output, olen);
  cache_bprintf (&b, "selection_size\t%d\n", S.selection_size);
  cache_bprintf (&b, "total_files\t%lld\n", uries);
  cache_bprintf (&b, "selection_relative_size\t%.10lf\n", safe_div (S.selection_size, uries));
  cache_bprintf (&b, "known_size_files\t%d\n", S.known_size_files);
  cache_bprintf (&b, "sum_acounter\t%.10lg\n", S.sum_acounter);
  cache_bprintf (&b, "relative_sum_acounter\t%.10lf\n", safe_div (S.sum_acounter, cum_access_counters[heap_acounter_id].value));
  cache_bprintf (&b, "sum_filesize\t%lld\n", S.sum_filesize);
  cache_bprintf (&b, "weighted_sum_filesize\t%.10lg\n", S.weighted_sum_filesize);
  if (b.pos >= olen) {
    return -1;
  }
  return b.pos;
}

static int cache_output_heap (cache_top_access_result_t *R, cache_heap_t *heap, int cnt, int flags, int min_rate) {
  int k;
  R->heap = heap;
  R->flags = flags;
  R->min_rate = min_rate;
  R->real_min_rate = min_rate >= 1000000 ? min_rate * 1e-6 : min_rate; // - 0.0
  R->disk_filter = (R->flags & 0x80000000) ? &disk_filter : NULL;
  R->heap_acounter_off = heap_acounter_off;
  int m = 0;
  for (k = 1; k <= cnt; k++) {
    struct cache_uri *U = heap->H[k];
    if (!U->data[uri_off]) {
      continue;
    }
    struct amortization_counter *C = ((struct amortization_counter *) &U->data[heap_acounter_off]);
    if (min_rate && C->value <= R->real_min_rate) {
      continue;
    }
    heap->H[++m] = heap->H[k];
  }
  R->cnt = m;
  return 0;
}

static int cmp_stat_server_id (const void *x, const void *y) {
  cache_stat_server_t *a = *(cache_stat_server_t **) x;
  cache_stat_server_t *b = *(cache_stat_server_t **) y;
  union cache_packed_local_copy_location c, d;
  c.i = a->id;
  d.i = b->id;
  if (c.p.node_id < d.p.node_id) {
    return -1;
  }
  if (c.p.node_id > d.p.node_id) {
    return 1;
  }
  if (c.p.server_id < d.p.server_id) {
    return -1;
  }
  if (c.p.server_id > d.p.server_id) {
    return 1;
  }
  return 0;
}

static int cmp_stat_server_access (const void *x, const void *y) {
  cache_stat_server_t *a = *(cache_stat_server_t **) x;
  cache_stat_server_t *b = *(cache_stat_server_t **) y;
  if (a->access_queries > b->access_queries) {
    return -1;
  }
  if (a->access_queries < b->access_queries) {
    return 1;
  }
  return cmp_stat_server_id (x, y);
}

static int cmp_stat_server_files (const void *x, const void *y) {
  cache_stat_server_t *a = *(cache_stat_server_t **) x;
  cache_stat_server_t *b = *(cache_stat_server_t **) y;
  if (a->files > b->files) {
    return -1;
  }
  if (a->files < b->files) {
    return 1;
  }
  return cmp_stat_server_id (x, y);
}

static int cmp_stat_server_files_bytes (const void *x, const void *y) {
  cache_stat_server_t *a = *(cache_stat_server_t **) x;
  cache_stat_server_t *b = *(cache_stat_server_t **) y;
  if (a->files_bytes > b->files_bytes) {
    return -1;
  }
  if (a->files_bytes < b->files_bytes) {
    return 1;
  }
  return cmp_stat_server_id (x, y);
}

int cache_do_memory_stats (char *output, int olen) {
  cache_buffer_t b;
  cache_bclear (&b, output, olen);
  int i;
  for (i = PTR_INTS; i <= MAX_RECORD_WORDS + 3; i++) {
    if (FreeCnt[i]) {
      cache_bprintf (&b, "FreeCnt[%d] = %d\n", i, (int) FreeCnt[i]);
    }
  }
  return b.pos;
}

int cache_do_detailed_server_stats (cache_stat_server_t ***R, int flags) {
  *R = NULL;
  if (!tot_servers) {
    return 0;
  }
  cache_stat_server_t **A = zmalloc0 (tot_servers * sizeof (cache_stat_server_t *)), *S;
  int i, n = 0;
  for (i = 0; i < STAT_SERVER_HASH_PRIME; i++) {
    for (S = HSS[i]; S != NULL; S = S->hnext) {
      assert (n < tot_servers);
      A[n++] = S;
    }
  }
  int(*compar)(const void *, const void *) = NULL;
  switch (flags & 3) {
    case 0:
      compar = cmp_stat_server_id;
      break;
    case 1:
      compar = cmp_stat_server_access;
      break;
    case 2:
      compar = cmp_stat_server_files_bytes;
      break;
    case 3:
      compar = cmp_stat_server_files;
      break;
  }
  vkprintf (4, "cache_do_detailed_server_stats: n = %d\n", n);
  qsort (A, n, sizeof (A[0]), compar);
  *R = A;
  return n;
}

#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS

static void func_monthly_stat (struct cache_uri *U) {
  int n, old_len;
  n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, &old_len);
  assert (n > 0);
  if (n > 1) {
    kprintf ("%s has %d local copies.\n", U->data + uri_off, n);
  }
  update_monthly_stat (U, &LC[0], 0);
}

int cache_monthly_stat_report (const char *const stat_filename) {
  int i, j;
  assert (cache_features_mask & CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS);
  cache_all_uri_cached_foreach (func_monthly_stat, cgsl_order_top);
  FILE *f = fopen (stat_filename, "w");
  if (f == NULL) {
    kprintf ("fopen (\"%s\", \"w\") fail. %m", stat_filename);
    return -1;
  }

  for (j = 0; j < amortization_counter_types; j++) {
    cumulative_stat_monthly (j);
    for (i = 0; i < MONTHLY_STAT_BUCKETS; i++) {
      fprintf (f, "%d\t%lld(%.6lf%%)\t%lld(%.6lf%%)\t%lld(%.6lf%%)\t%lld(%.6lf%%)\t%lld\t%lld\n", i,
        stat_monthly[j][i].unreasonable_downloads[0],
        safe_div (stat_monthly[j][i].unreasonable_downloads[0] * 100.0, stat_monthly[j][i].total),
        stat_monthly[j][i].unreasonable_downloads[1],
        safe_div (stat_monthly[j][i].unreasonable_downloads[1] * 100.0, stat_monthly[j][i].total),
        stat_monthly[j][i].cum_unreasonable_downloads[0],
        safe_div (stat_monthly[j][i].cum_unreasonable_downloads[0] * 100.0, stat_monthly[j][i].cum_total),
        stat_monthly[j][i].cum_unreasonable_downloads[1] ,
        safe_div (stat_monthly[j][i].cum_unreasonable_downloads[1] * 100.0, stat_monthly[j][i].cum_total),
        stat_monthly[j][i].total,
        stat_monthly[j][i].deleted);
    }
    fprintf (f, "\n");
  }
  fclose (f);
  return 0;
}

#endif

#ifdef CACHE_FEATURE_CORRELATION_STATS
int cache_correlation_stat_report (const char *correlaction_stat_dir) {
  int i, j, x, y;
  char path[PATH_MAX];
  for (i = 0; i < amortization_counter_types; i++) {
    for (j = i + 1; j < amortization_counter_types; j++) {
      assert (snprintf (path, sizeof (path), "%s/%d%d", correlaction_stat_dir, i, j) < sizeof (path));
      FILE *f = fopen (path, "w");
      if (f == NULL) {
        kprintf ("fopen (\"%s\", \"w\") fail. %m", path);
        return -1;
      }
      for (x = 0; x < CORRECLATION_STATS_BUCKETS; x++) {
        for (y = 0; y < CORRECLATION_STATS_BUCKETS; y++) {
          if (y) {
            fprintf (f, "\t");
          }
          fprintf (f, "%lld", correlaction_stats[i][j][x][y]);
        }
        fprintf (f, "\n");
      }
      fclose (f);
    }
  }
  return 0;
}
#endif

int cache_get_bottom_disk (cache_top_access_result_t *R, int T, enum cache_sorted_order order, int limit, int node_id, int server_id, int disk_id, int flags) {
  vkprintf (3, "%s (T:%d, limit:%d, node_id:%d, server_id:%d, disk_id:%d)\n", __func__, T, limit, node_id, server_id, disk_id);
  cache_heap_t Heap;
  int cnt = cache_bottom_fill_heap_from_given_disk (&Heap, T, limit, node_id, server_id, disk_id, order);
  if (cnt < 0) {
    vkprintf (2, "cache_bottom_fill_heap_from_given_disk (T:%d, limit:%d, node_id:%d, server_id:%d, disk_id:%d) return error code %d.\n", T, limit, node_id, server_id, disk_id, cnt);
    return cnt;
  }
  return cache_output_heap (R, &Heap, cnt, (flags & (1+2+4+32+64)) | 0x80000000, 0);
}

int cache_get_sorted_list (cache_top_access_result_t *R, int T, enum cache_sorted_order order, int limit, int flags, int min_rate) {
  cache_heap_t Heap;
  int cnt = cache_top_fill_heap (&Heap, T, order, limit, (flags & 8) ? 1 : 0, (flags & 16) ? 1 : 0, min_rate);
  if (cnt < 0) {
    return cnt;
  }

#ifdef CACHE_DEFEND_TOP_ACCESS_URIES_FROM_GC
  /* prevent top-priority uncached URIes returned by request from garbage collection */
  if ((flags & 16) && order == cgsl_order_top) {
    int i;
    for (i = 1; i <= cnt; i++) {
      struct cache_uri *U = Heap.H[i];
      U->last_access = log_last_ts;
    }
  }
#endif

  return cache_output_heap (R, &Heap, cnt, flags & (1 + 2 + 4), min_rate);
}

char *acounters_init_string = "hour,day,week,month";
static double tat_default_T[4] = {3600.0, 86400.0, 86400.0 * 7, 86400.0 * 30};

static struct {
  int n;
  double *T;
} tat_default = {
  .n = sizeof (tat_default_T) / sizeof (tat_default_T[0]),
  .T = tat_default_T
};

/******************** binlog ********************/

int cache_replay_logevent (struct lev_generic *E, int size);
int cache_replay_logevent_index_mode  (struct lev_generic *E, int size);

int cache_set_amortization_tables_initialization_string (const char *const s) {
  assert (strlen (s) <= MAX_ACOUNTERS_INIT_STR_LEN);
  char buf[MAX_ACOUNTERS_INIT_STR_LEN];
  int n = 0;
  char *r, *z = strdup (s);
  assert (z);
  for (r = z; *r; r++) {
    if (*r == ',') {
      n++;
    }
  }
  tat_default.n = n + 1;
  tat_default.T = zmalloc (tat_default.n * sizeof (tat_default.T[0]));
  n = 0;
  for (r = strtok (z, ","); r != NULL; r = strtok (NULL, ","), n++) {
    double x;
    char c = 0;
    if (!strcmp (r, "hour")) {
      x = 3600.0;
    } else if (!strcmp (r, "day")) {
      x = 3600.0 * 24.0;
    } else if (!strcmp (r, "week")) {
      x = 3600.0 * 24.0 * 7.0;
    } else if (!strcmp (r, "month")) {
      x = 3600.0 * 24.0 * 30.0;
    } else if (sscanf (r, "%lf%c", &x, &c) < 1) {
      kprintf ("Illegal half-life period: %s\n", r);
      free (z);
      return -1;
    } else {
      switch (c) {
        case 0:
        case 's':
          x *= 1.0;
          break;
        case 'h':
          x *= 3600.0;
          break;
        case 'd':
          x *= 3600.0 * 24.0;
          break;
        case 'w':
          x *= 3600.0 * 24.0 * 7.0;
          break;
        case 'm':
          x *= 3600.0 * 24.0 * 30.0;
          break;
        default:
          kprintf ("Illegal half-life period (unknown suffix): %s\n", r);
          free (z);
          return -1;
      }
    }
    if (x < 1e-9) {
      kprintf ("Half-life period too small: %s\n", r);
      free (z);
      return -1;
    }
    tat_default.T[n] = (int) (x + 0.5);
  }
  free (z);
  if (n != tat_default.n) {
    return -1;
  }

  int i, j;
  for (i = 0; i < n; i++) {
    for (j = i + 1; j < n; j++) {
      if (tat_default.T[i] > tat_default.T[j]) {
        double w = tat_default.T[i];
        tat_default.T[i] = tat_default.T[j];
        tat_default.T[j] = w;
      }
    }
  }

  cache_buffer_t b;
  cache_bclear (&b, buf, sizeof (buf));
  for (i = 0; i < n; i++) {
    if (i > 0) {
      cache_bprintf (&b, ",");
    }
    cache_bprintf (&b, "%d", (int) (tat_default.T[i] + 0.5));
  }
  assert (b.pos < sizeof (buf));
  acounters_init_string = zmalloc (b.pos + 1);
  memcpy (acounters_init_string, buf, b.pos);
  acounters_init_string[b.pos] = 0;
  return n;
}

int init_cache_data (int schema) {
  vkprintf (4, "%s (0x%x)\n", __func__, schema);
  assert (offsetof (cache_local_copy_packed_location_t, disk_id) == 3);
  time_amortization_tables_init (tat_default.n, tat_default.T);
  if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
    int i;
    for (i = 0; i < CACHE_URI_BUCKETS; i++) {
      struct cache_uri *U = &list_uncached[i];
      PPREV(U) = PNEXT(U) = U;
      U = &list_cached[i];
      PPREV(U) = PNEXT(U) = U;
    }
  }
  replay_logevent = !index_timestamp ? cache_replay_logevent : cache_replay_logevent_index_mode;
  vkprintf (4, "%s: sizeof (struct cache_uri): %d\n", __func__, (int) sizeof (struct cache_uri));
  vkprintf (4, "%s: acounter_off: %d\n", __func__, acounter_off);
  vkprintf (4, "%s: acounters_size: %d\n", __func__, acounters_size);
  vkprintf (4, "%s: uri_off: %d\n", __func__, uri_off);
  return 0;
}

int cache_le_start (struct lev_start *E) {
  if (E->schema_id != CACHE_SCHEMA_V1) {
    kprintf ("LEV_START schema_id isn't to CACHE_SCHEMA_V1.\n");
    return -1;
  }
  if (E->extra_bytes < 4) {
    kprintf ("LEV_START extra_bytes isn't equal to 4.\n");
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  memcpy (&cache_id, E->str, 4);

  return 0;
}

int cache_replay_logevent (struct lev_generic *E, int size) {
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
    case LEV_TAG:
      return default_replay_logevent (E, size);
    case LEV_CACHE_ACCESS_SHORT...LEV_CACHE_ACCESS_SHORT+0xff:
      s = sizeof (struct lev_cache_access_short);
      if (size < s) { return -2; }
      cache_access_short ((struct lev_cache_access_short *) E, E->type & 0xff);
      return s;
    case LEV_CACHE_ACCESS_LONG...LEV_CACHE_ACCESS_LONG+0xff:
      s = sizeof (struct lev_cache_access_long);
      if (size < s) { return -2; }
      cache_access_long ((struct lev_cache_access_long *) E, E->type & 0xff);
      return s;
    case LEV_CACHE_URI_ADD...LEV_CACHE_URI_ADD+0xff:
      s = sizeof (struct lev_cache_uri) + (E->type & 0xff);
      if (size < s) { return -2; }
      cache_uri_add ((struct lev_cache_uri *) E, E->type & 0xff);
      return s;
    case LEV_CACHE_URI_DELETE...LEV_CACHE_URI_DELETE+0xff:
      s = sizeof (struct lev_cache_uri) + (E->type & 0xff);
      if (size < s) { return -2; }
      cache_uri_delete ((struct lev_cache_uri *) E, E->type & 0xff);
      return s;
    case LEV_CACHE_SET_SIZE_SHORT:
      s = sizeof (struct lev_cache_set_size_short);
      if (size < s) { return -2; }
      cache_set_size_short ((struct lev_cache_set_size_short *) E);
      return s;
    case LEV_CACHE_SET_SIZE_LONG:
      s = sizeof (struct lev_cache_set_size_long);
      if (size < s) { return -2; }
      cache_set_size_long ((struct lev_cache_set_size_long *) E);
      return s;
    case LEV_CACHE_SET_NEW_LOCAL_COPY_SHORT:
      s = sizeof (struct lev_cache_set_new_local_copy_short);
      if (size < s) { return -2; }
      cache_set_new_local_copy_short ((struct lev_cache_set_new_local_copy_short *) E);
      return s;
    case LEV_CACHE_SET_NEW_LOCAL_COPY_LONG...LEV_CACHE_SET_NEW_LOCAL_COPY_LONG+0xff:
      s = sizeof (struct lev_cache_set_new_local_copy_long) + (E->type & 0xff);
      if (size < s) { return -2; }
      cache_set_new_local_copy_long ((struct lev_cache_set_new_local_copy_long *) E, E->type & 0xff);
      return s;
    case LEV_CACHE_DELETE_LOCAL_COPY_SHORT:
      s = sizeof (struct lev_cache_set_new_local_copy_short);
      if (size < s) { return -2; }
      cache_delete_local_copy_short ((struct lev_cache_set_new_local_copy_short *) E);
      return s;
    case LEV_CACHE_DELETE_LOCAL_COPY_LONG...LEV_CACHE_DELETE_LOCAL_COPY_LONG+0xff:
      s = sizeof (struct lev_cache_set_new_local_copy_long) + (E->type & 0xff);
      if (size < s) { return -2; }
      cache_delete_local_copy_long ((struct lev_cache_set_new_local_copy_long *) E, E->type & 0xff);
      return s;
    case LEV_CACHE_CHANGE_DISK_STATUS...LEV_CACHE_CHANGE_DISK_STATUS+1:
      s = sizeof (struct lev_cache_change_disk_status);
      if (size < s) { return -2; }
      cache_change_disk_status ((struct lev_cache_change_disk_status *) E, E->type & 1);
      return s;
    case LEV_CACHE_DELETE_REMOTE_DISK:
      s = sizeof (struct lev_cache_change_disk_status);
      if (size < s) { return -2; }
      cache_delete_remote_disk ((struct lev_cache_change_disk_status *) E);
      return s;
    case LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_SHORT:
      s = sizeof (struct lev_cache_set_local_copy_yellow_light_short);
      if (size < s) { return -2; }
      cache_set_local_copy_yellow_light_short ((struct lev_cache_set_local_copy_yellow_light_short *) E);
      return s;
    case LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG...LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG+0xff:
      s = sizeof (struct lev_cache_set_local_copy_yellow_light_long) + (E->type & 0xff);
      if (size < s) { return -2; }
      cache_set_local_copy_yellow_light_long ((struct lev_cache_set_local_copy_yellow_light_long *) E, E->type & 0xff);
      return s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());

  return -3;
}

static int *sorted_server_list;
static int cmp_int (const void *a, const void *b) {
  return *(const int *) a - *(const int *) b;
}

static void cache_uri_dump_local_uries (struct cache_uri *U) {
  int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, NULL);
  if (n < 0) {
    vkprintf (1, "cache_do_local_copies (uri: %s): cache_local_copy_unpack failed.\n", U->data + uri_off);
    return;
  }
  int i;
  for (i = 0; i < n; i++) {
    if (sorted_server_list) {
      union cache_packed_local_copy_location u;
      cache_local_copy_compute_packed_location (&LC[i], &u);
      u.p.disk_id = 0;
      if (bsearch (&u, &sorted_server_list[1], sorted_server_list[0], 4, cmp_int) == NULL) {
        continue;
      }
    }
    printf ("%s\t%s\n", LC[i].location, U->data + uri_off);
  }
}

static void cache_dump_local_uries_mapping (void) {
  cache_all_uri_cached_foreach (cache_uri_dump_local_uries, cgsl_order_top);
}

static void cache_uri_dump_uncached (struct cache_uri *U) {
  printf ("%s\t%d\n", U->data + uri_off, U->last_access);
}

static void cache_dump_uncached_uries (void) {
  cache_all_uri_uncached_foreach (cache_uri_dump_uncached, cgsl_order_top);
}

static int cache_index_mode = 0, index_new_cache_id = 0;

int cache_replay_logevent_index_mode  (struct lev_generic *E, int size) {
  if (log_last_ts >= index_timestamp) {
    cache_save_pseudo_index ();
    exit (0);
  }
  return cache_replay_logevent (E, size);
}

/**************************************** empty binlog *******************************************************/
struct lev_start *cache_lev_start_alloc (int cache_id, int *sz) {
  *sz = 28;
  struct lev_start *E = calloc (*sz, 1);
  E->type = LEV_START;
  E->schema_id = CACHE_SCHEMA_V1;
  E->extra_bytes = 4;
  E->split_mod = log_split_mod;
  E->split_min = log_split_min;
  E->split_max = log_split_min + 1;
  assert (E->split_mod > 0 && E->split_min >= 0 && E->split_min + 1 == E->split_max && E->split_max <= E->split_mod);

  memcpy (E->str, &cache_id, 4);
  return E;
}

void make_empty_binlog (const char *binlog_name) {
  char a[PATH_MAX];
  assert (snprintf (a, PATH_MAX, "%s.bin", binlog_name) < PATH_MAX);
  int sz;
  struct lev_start *E = cache_lev_start_alloc (cache_id, &sz);
  int fd = open (a, O_CREAT | O_WRONLY | O_EXCL, 0660);
  if (fd < 0) {
    kprintf ("open (%s, O_CREAT | O_WRONLY | O_EXCL, 0660) failed. %m\n", a);
    assert (fd >= 0);
  }
  assert (write (fd, E, sz) == sz);
  assert (fsync (fd) >= 0);
  assert (close (fd) >= 0);
}

/**************************************** DEBUG *******************************************************/

void debug_check_acounters_in_increasing_order (struct cache_uri *U) {
  int i;
  struct amortization_counter *D = NULL, *C = (struct amortization_counter *) &U->data[acounter_off];
  for (i = 0; i < amortization_counter_types; i++) {
    amortization_counter_update (&TAT[i], C);
    if (D) {
      assert (D->value <= C->value);
    }
    D = C;
    C++;
  }
}

/******************** index ********************/
#define CACHE_INDEX_MAGIC 0x416c4b63
#define CACHE_NO_COUNTERS_INDEX_MAGIC 0xbe93b49c
#pragma	pack(push,4)

typedef struct {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  int log_timestamp;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;

  int cache_id;
  int amortization_counter_types;
  int log_split_min;
  int log_split_max;
  int log_split_mod;

  int uri_living_time;

  long long uries, deleted_uries;
  /* uri_bytes isn't stored in index since it has different values for 32-bit & 64-bit arch */
  long long access_short_logevents, access_long_logevents;
  long long local_copies_bytes;
  long long cached_uries;
  int tot_disabled_servers;
  char acounters_init_string[MAX_ACOUNTERS_INIT_STR_LEN];
  unsigned long long body_crc64[8];
  unsigned long long header_crc64;
} cache_index_header_t;

struct cache_index_uri {
  long long size;
  int last_access;
  short local_copy_len, uri_len;
};

/*  Index structure:
    header
[0] cum_access_counters
[1] uries (cache_index_uri, acounters, local_copy, uri)
[2] disabled_servers
*/
#pragma	pack(pop)

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;
static int newidx_fd, idx_fd;

#define	BUFFSIZE	16777216

static char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff;
static long long bytes_read;

static void flushout (void) {
  int w, s;
  if (rptr < wptr) {
    s = wptr - rptr;
    w = write (newidx_fd, rptr, s);
    assert (w == s);
  }
  rptr = wptr = Buff;
}

static long long bytes_written;
static unsigned long long idx_crc64_complement;

static void clearin (void) {
  rptr = wptr = Buff + BUFFSIZE;
  bytes_read = 0;
  bytes_written = 0;
  idx_crc64_complement = -1LL;
}

static int writeout (const void *D, size_t len) {
  bytes_written += len;
  idx_crc64_complement = crc64_partial (D, len, idx_crc64_complement);
  const int res = len;
  const char *d = D;
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) {
      r = len;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout ();
    }
  }
  return res;
}

static void *readin (size_t len) {
  assert (len >= 0);
  if (rptr + len <= wptr) {
    return rptr;
  }
  if (wptr < Buff + BUFFSIZE) {
    return 0;
  }
  memcpy (Buff, rptr, wptr - rptr);
  wptr -= rptr - Buff;
  rptr = Buff;
  int r = read (idx_fd, wptr, Buff + BUFFSIZE - wptr);
  if (r < 0) {
    kprintf ("error reading file: %m\n");
  } else {
    wptr += r;
  }
  if (rptr + len <= wptr) {
    return rptr;
  } else {
    return 0;
  }
}

static void readadv (size_t len) {
  assert (len >= 0 && len <= wptr - rptr);
  rptr += len;
}

void bread (void *b, size_t len) {
  void *p = readin (len);
  assert (p != NULL);
  memcpy (b, p, len);
  idx_crc64_complement = crc64_partial (b, len, idx_crc64_complement);
  readadv (len);
  bytes_read += len;
}

int save_index (int writing_binlog) {
#ifdef CACHE_FEATURE_MONTHLY_COUNTER_PERF_STATS
  assert (0);
#endif
  int i;
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    kprintf ("cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (index_exists && log_cur_pos() == jump_log_pos) {
    kprintf ("skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
       newidxname, jump_log_pos);
    return 0;
  }

  vkprintf (1, "creating index %s at log position %lld\n", newidxname, log_cur_pos());

  newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    kprintf ("cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  cache_index_header_t header;
  memset (&header, 0, sizeof (header));

  header.magic = 0;
  header.created_at = time (NULL);
  header.log_pos1 = log_cur_pos ();
  header.log_timestamp = log_last_ts;
  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }
  header.log_pos1_crc32 = ~log_crc32_complement;

  header.cache_id = cache_id;
  header.amortization_counter_types = amortization_counter_types;
  header.uri_living_time = uri_living_time;

  header.uries = uries;
  header.deleted_uries = deleted_uries;
  //header.uri_bytes = uri_bytes;
  header.access_short_logevents = access_short_logevents;
  header.access_long_logevents = access_long_logevents;
  header.local_copies_bytes = local_copies_bytes;
  header.cached_uries = cached_uries;
  header.tot_disabled_servers = tot_disabled_servers;
  assert (strlen (acounters_init_string) < sizeof (header.acounters_init_string));
  strcpy (header.acounters_init_string, acounters_init_string);

  header.log_split_min = log_split_min;
  header.log_split_max = log_split_max;
  header.log_split_mod = log_split_mod;

  size_t off = sizeof (cache_index_header_t);
  assert (lseek (newidx_fd, off, SEEK_SET) == off);
  clearin ();
  int block = 0;
  assert (cum_access_counters);
  for (i = 0; i < amortization_counter_types; i++) {
    writeout (&cum_access_counters[i].value, sizeof (cum_access_counters[i].value));
    writeout (&cum_access_counters[i].last_update_time, 4);
  }
  flushout ();
  header.body_crc64[block++] = ~idx_crc64_complement;
  clearin ();

  long long _uries = 0, _uri_bytes = 0, _local_copies_bytes = 0, _cached_uries = 0;

  struct cache_uri *U;
  for (i = 0; i < uri_hash_prime; i++) {
    /* in load_index each URI inserted into head of hash list,
       so for saving list order, we need to reverse list before saving it
    */
    H[i] = hlist_reverse (H[i]);
    for (U = H[i]; U != NULL; U = U->hnext) {
      int l;
      struct cache_index_uri a;
      a.size = cache_uri_get_size (U);
      a.last_access = U->last_access;
      a.uri_len = strlen (U->data + uri_off);
      const int sz = cache_uri_sizeof (U);
      _uri_bytes += sz;
      assert (cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, &l) >= 0);
      assert (l >= 0 && l < 0x7fff);
      a.local_copy_len = l;
      _uries++;
      _local_copies_bytes += (l + 3) & -4;
      writeout (&a, sizeof (a));
      writeout (U->data + uri_off, a.uri_len);
      if (cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES) {
        writeout (U->data + acounter_off, acounters_size);
      }
      if (l > 0) {
        _cached_uries++;
        assert (U->local_copy != NULL);
        writeout (U->local_copy, l);
      }
      if (verbosity >= 3 && U->local_copy && U->size == -1) {
        kprintf ("cached, but no size: %s\n", U->data + uri_off);
      }
    }
  }
  assert (_uries == uries);
  assert (_local_copies_bytes == local_copies_bytes);
  assert (_uri_bytes == uri_bytes);
  assert (_cached_uries == cached_uries);
  if (bytes_written & 3) {
    int zero = 0;
    writeout (&zero, 4 - (bytes_written & 3));
  }
  flushout ();
  header.body_crc64[block++] = ~idx_crc64_complement;
  clearin ();

  int _tot_disabled_servers = 0;
  struct cache_local_copy_server *S;
  for (i = 0; i < DISABLED_SERVER_HASH_PRIME; i++) {
    for (S = HDS[i]; S != NULL; S = S->hnext) {
      _tot_disabled_servers++;
      writeout (&S->id, 4);
      writeout (&S->bitset, 32);
    }
  }
  assert (_tot_disabled_servers == tot_disabled_servers);
  flushout ();
  header.body_crc64[block++] = ~idx_crc64_complement;
  clearin ();

  header.magic = (cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES) ? CACHE_INDEX_MAGIC : CACHE_NO_COUNTERS_INDEX_MAGIC;
  header.header_crc64 = crc64 (&header, offsetof (cache_index_header_t, header_crc64));

  lseek (newidx_fd, 0, SEEK_SET);
  assert (write (newidx_fd, &header, sizeof (header)) == sizeof (header));
  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);

  vkprintf (3, "index written ok\n");

  if (rename_temporary_snapshot (newidxname)) {
    kprintf ("cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);

  return 0;
}

int load_index (kfs_file_handle_t Index) {
  int i;
  char buff[0x8001];
  if (Index == NULL) {
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    return 0;
  }
  idx_fd = Index->fd;
  cache_index_header_t header;
  size_t sz = sizeof (cache_index_header_t);
  if (read (idx_fd, &header, sz) != sz) {
    kprintf ("[%s] read index header failed. %m\n", Index->info->filename);
    return -1;
  }
  if (header.magic != CACHE_INDEX_MAGIC && header.magic != CACHE_NO_COUNTERS_INDEX_MAGIC) {
    kprintf ("[%s] index file is not for cache-engine\n", Index->info->filename);
    return -1;
  }
  if (crc64 (&header, offsetof (cache_index_header_t, header_crc64)) != header.header_crc64) {
    kprintf ("[%s] index header is broken (crc64 didn't match)\n", Index->info->filename);
    return -1;
  }
  const int has_counters = header.magic == CACHE_INDEX_MAGIC;
  vkprintf (3, "%s: has_counters = %d\n", __func__, has_counters);

  if (index_timestamp && header.log_timestamp > index_timestamp) {
    kprintf ("[%s] index is too new (header.log_timestamp: %d) for timestamp %d.\n", Index->info->filename, header.log_timestamp, index_timestamp);
    return CACHE_ERR_NEW_INDEX;
  }

  if (!has_counters && (cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES)) {
    kprintf ("[%s] index didn't contain amortization counter values, but cache-engine was run with access queries feature (%d).\n", Index->info->filename, CACHE_FEATURE_ACCESS_QUERIES);
    return -1;
  }

  if (strcmp (acounters_init_string, header.acounters_init_string)) {
    kprintf ("[%s] index acounters_init_string (\"%s\") isn't equal to cache-engine acounters_init_string (\"%s\"). Skipping index.\n", Index->info->filename, header.acounters_init_string, acounters_init_string);
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    return 0;
  }

  int n = cache_set_amortization_tables_initialization_string (header.acounters_init_string);
  if (n < 0) {
    kprintf ("[%s] cache_set_amortization_tables_initialization_string (\"%s\") fail.\n",
      Index->info->filename, header.acounters_init_string);
  }
  //amortization_counter_types = header.amortization_counter_types;
  if (n != header.amortization_counter_types) {
    kprintf ("[%s] header amortization_counter_types field is broken.\n", Index->info->filename);
    return -1;
  }
  init_cache_data (CACHE_SCHEMA_V1);

  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;

  cache_id = header.cache_id;

  uri_living_time = header.uri_living_time;
  assert (uri_living_time >= 3600);
  vkprintf (2, "uri_living_time: %ds\n", uri_living_time);
  deleted_uries = header.deleted_uries;
  access_short_logevents = header.access_short_logevents;
  access_long_logevents = header.access_long_logevents;
  uri_bytes = local_copies_bytes = cached_uries = tot_disabled_servers = 0;
  clearin ();
  for (i = 0; i < amortization_counter_types; i++) {
    bread (&cum_access_counters[i].value, sizeof (cum_access_counters[i].value));
    bread (&cum_access_counters[i].last_update_time, 4);
  }

  if (header.body_crc64[0] != ~idx_crc64_complement) {
    kprintf ("[%s] index is broken (crc64 didn't match for cum_access_counters chunk)\n", Index->info->filename);
    return -1;
  }
  idx_crc64_complement = -1LL;
  const int asz = header.amortization_counter_types * sizeof (struct amortization_counter);
  char *skip_buff = alloca (asz);
  long long ll;
  for (ll = 0; ll < header.uries; ll++) {
    int l;
    struct cache_index_uri a;
    bread (&a, sizeof (a));
    vkprintf (4, "%s: read %lldth URI (a.uri_len (%d), a.local_copy_len(%d)\n", __func__, ll + 1, a.uri_len, a.local_copy_len);
    assert (a.uri_len >= 0 && a.local_copy_len >= 0);
    l = a.uri_len;
    bread (buff, l);
    buff[l] = 0;
    struct cache_uri *U = load_index_get_uri_f (buff);
    U->last_access = a.last_access;
    if (a.last_access && a.last_access < 1342004209 - 86400 * 365) {
      kprintf ("Index with corrupted last_access field (%d).\n", a.last_access);
      return -1;
    }
    if (cache_features_mask & CACHE_FEATURE_ACCESS_QUERIES) {
      bread (U->data + acounter_off, acounters_size);
    } else if (has_counters) {
      bread (skip_buff, asz);
    }
    if (a.local_copy_len > 0) {
      int sz = a.local_copy_len;
      sz = (sz + 3) & -4;
      U->local_copy = zmalloc (sz);
      local_copies_bytes += sz;
      cached_uries++;
      bread (U->local_copy, a.local_copy_len);

      if (a.size >= 0 && (cache_features_mask & CACHE_FEATURE_DETAILED_SERVER_STATS)) {
        int old_len;
        const int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, &old_len);
        assert (old_len == a.local_copy_len);
        if (n > 0) {
          for (i = 0; i < n; i++) {
            union cache_packed_local_copy_location u;
            cache_local_copy_compute_packed_location (LC+i, &u);
            u.p.disk_id = 0;
            if (u.i) {
              cache_stat_server_t *S = get_stat_server_f (u.i, 1);
              S->files++;
              S->files_bytes += a.size;
            }
          }
        }
      }
    }

    if (cache_features_mask & CACHE_FEATURE_LONG_QUERIES) {
      if (U->local_copy == NULL) {
        cache_uri_list_insert (list_uncached + get_bucket (U), U);
      } else if (!(cache_features_mask & CACHE_FEATURE_FAST_BOTTOM_ACCESS)) {
        cache_uri_list_insert (list_cached + get_bucket (U), U);
      } else {
        int old_len;
        const int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 0, &old_len);
        assert (old_len == a.local_copy_len);
        cache_uri_bucket_insert_by_server (U, LC, n);
      }
    }

    U = cache_set_size (U, a.size); //should be called after U->local_copy initialization, for correct stat calculation
  }
  vkprintf (4, "tot_servers: %d\n", tot_servers);

  if (bytes_read & 3) {
    int zero = 0;
    bread (&zero, 4 - (bytes_read & 3));
    assert (zero == 0);
  }

  if (header.body_crc64[1] != ~idx_crc64_complement) {
    kprintf ("[%s] index is broken (crc64 didn't match for uries chunk)\n", Index->info->filename);
    return -1;
  }
  idx_crc64_complement = -1LL;
  assert (uries == header.uries);
  assert (local_copies_bytes == header.local_copies_bytes);
  assert (cached_uries == header.cached_uries);

  for (i = 0; i < header.tot_disabled_servers; i++) {
    int id;
    bread (&id, 4);
    struct cache_local_copy_server *S = get_disabled_server_f (id, 1);
    bread (S->bitset, 32);
  }

  if (header.body_crc64[2] != ~idx_crc64_complement) {
    kprintf ("[%s] index is broken (crc64 didn't match for disabled_servers chunk)\n", Index->info->filename);
    return -1;
  }
  idx_crc64_complement = -1LL;

  assert (tot_disabled_servers == header.tot_disabled_servers);

  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;
  jump_log_ts = header.log_timestamp;

  vkprintf (4, "jump_log_pos: %lld\n", jump_log_pos);
  vkprintf (4, "jump_log_ts: %d\n", jump_log_ts);
  //replay_logevent = cache_replay_logevent;
  index_exists = 1;
  return 0;
}

static int cache_read_local_server_list (const char *const filename, int arity) {
  FILE *f = fopen (filename, "r");
  if (f == NULL) {
    kprintf ("%s: fopen (\"%s\", \"r\") failed. %m\n", __func__, filename);
    return -1;
  }
  int *a = (int *) Buff, n = 0;
  int node_id = 0, server_id = 0;
  while (fscanf (f, " cs%d_%d", &node_id, &server_id) == 2) {
    if (n >= (sizeof (Buff) >> 2)) {
      kprintf ("%s: Too many servers in %s (%d).\n", __func__, filename, n);
      fclose (f);
      return -1;
    }
    if (!(node_id >= 1 && node_id <= MAX_NODE_ID && server_id >= 1 && server_id <= MAX_SERVER_ID)) {
      kprintf ("%s: Bad (out of range) server cs%d_%d\n", __func__, node_id, server_id);
      fclose (f);
      return -1;
    }
    union cache_packed_local_copy_location u;
    u.p.node_id = node_id;
    u.p.server_id = server_id;
    u.p.disk_id = 0;
    a[n++] = u.i;
  }
  char c;
  if (fscanf (f, " %c", &c) == 1) {
    kprintf ("c: %c\n", c);
    kprintf ("%s: expected eof (last parsed server cs%d_%d)\n", __func__, node_id, server_id);
    fclose (f);
    return -1;
  }
  fclose (f);
  if (n % arity) {
    kprintf ("%s: found %d servers - isn't multiple of %d\n", __func__, n, arity);
    return -1;
  }
  qsort (a, n / arity, 4 * arity, cmp_int);
  sorted_server_list = calloc (n + 1, 4);
  assert (sorted_server_list);
  sorted_server_list[0] = n / arity;
  memcpy (&sorted_server_list[1], a, 4 * n);
  int i, j, k = 1;
  for (i = 1; i <= sorted_server_list[0]; i++) {
    for (j = 0; j < arity; j++) {
      union cache_packed_local_copy_location u;
      u.i = sorted_server_list[k++];
      if (j) {
        printf (" -> ");
      }
      printf ("cs%d_%d", u.p.node_id, u.p.server_id);
    }
    printf ("\n");
  }
  return 0;
}

static struct {
  int b[1024];
  long long max_filesize;
  long long uries;
  long long local_uries;
  long long timestamps;
  long long access_logevents;
  long long set_size_logevents;
  int sz;
} le_buff;

static void *out_log_event_alloc (int type, int bytes, int arg1) {
  assert (!le_buff.sz);
  le_buff.sz = (bytes + 3) & -4;
  assert (le_buff.sz <= sizeof (le_buff.b));
  memset (le_buff.b, 0, le_buff.sz);
  le_buff.b[0] = type;
  le_buff.b[1] = arg1;
  return le_buff.b;
}

static void out_log_event_write (long long *events) {
  assert (le_buff.sz);
  assert (!(le_buff.sz & 3));
  writeout (le_buff.b, le_buff.sz);
  le_buff.sz = 0;
  (*events)++;
}

static void func_save_cached_uri (struct cache_uri *U) {
  int n = cache_local_copy_unpack (U, LC, CACHE_MAX_LOCAL_COPIES, 1, NULL);
  if (n < 0) {
    vkprintf (1, "%s (uri: %s): cache_local_copy_unpack failed.\n", __func__, U->data + uri_off);
    return;
  }
  int *dest = alloca (4 * n), m = 0;
  int *id = alloca (4 * n);
  int i;
  for (i = 0; i < n; i++) {
    int a[2];
    union cache_packed_local_copy_location u;
    cache_local_copy_compute_packed_location (&LC[i], &u);
    int disk_id = u.p.disk_id;
    u.p.disk_id = 0;
    a[0] = u.i;
    int *b = bsearch (a, &sorted_server_list[1], sorted_server_list[0], 8, cmp_int);
    if (b != NULL) {
      u.i = b[1];
      u.p.disk_id = disk_id;
      id[m] = i;
      dest[m] = u.i;
      m++;
    }
  }
  if (m > 0) {
    get_uri_f_last_md5.computed = 0;
    char *uri = U->data + uri_off;
    int l = strlen (uri);
    assert (l < 256);
    struct lev_cache_uri *E = out_log_event_alloc (LEV_CACHE_URI_ADD + l, sizeof (struct lev_cache_uri) + l, 0);
    memcpy (E->data, uri, l);
    out_log_event_write (&le_buff.uries);
    const int bytes = cache_get_unique_md5_bytes (U);
    const long long size = cache_uri_get_size (U);
    if (size >= 0) {
      if (le_buff.max_filesize < size) {
        le_buff.max_filesize = size;
      }
      if (bytes == 8 && size <= 0xffffffffLL) {
        struct lev_cache_set_size_short *E = out_log_event_alloc (LEV_CACHE_SET_SIZE_SHORT, sizeof (struct lev_cache_set_size_short), (unsigned) size);
        memcpy (E->data, &U->uri_md5_h0, 8);
      } else {
        struct lev_cache_set_size_long *E = out_log_event_alloc (LEV_CACHE_SET_SIZE_LONG, sizeof (struct lev_cache_set_size_long), 0);
        E->size = size;
        compute_get_uri_f_last_md5 (U);
        memcpy (E->data, get_uri_f_last_md5.uri_md5.c, 16);
      }
      out_log_event_write (&le_buff.set_size_logevents);
    }
    /*
    struct amortization_counter *C = (struct amortization_counter *) &U->data[acounter_off];
    int t_id = amortization_counter_types - 1;
    C += t_id;
    amortization_counter_update (&TAT[t_id], C);
    int access = (int) (C->value);
    */
    int access = 0;
    while (access > 0) {
      int l = access < 127 ? access : 127;
      access -= l;
      if (bytes == 8) {
        struct lev_cache_access_short *E = out_log_event_alloc (LEV_CACHE_ACCESS_SHORT + l, sizeof (struct lev_cache_access_short), 0);
        memcpy (E->data, &U->uri_md5_h0, 8);
      } else {
        assert (bytes == 16);
        struct lev_cache_access_long *E = out_log_event_alloc (LEV_CACHE_ACCESS_LONG + l, sizeof (struct lev_cache_access_long), 0);
        compute_get_uri_f_last_md5 (U);
        memcpy (E->data, get_uri_f_last_md5.uri_md5.c, 16);
      }
      out_log_event_write (&le_buff.access_logevents);
    }

    for (i = 0; i < m; i++) {
      if (bytes == 8 && (LC[id[i]].flags & CACHE_LOCAL_COPY_FLAG_INT)) {
        struct lev_cache_set_new_local_copy_short *E = out_log_event_alloc (LEV_CACHE_SET_NEW_LOCAL_COPY_SHORT, sizeof (*E), dest[i]);
        memcpy (E->data, &U->uri_md5_h0, 8);
      } else {
        //TODO:
        kprintf ("%s: Import of LEV_CACHE_SET_NET_LOCAL_COPY_LONG logevent isn't implemented\n", __func__);
        exit (1);
        /*
        struct lev_cache_set_new_local_copy_long *E = alloc_log_event (LEV_CACHE_SET_NEW_LOCAL_COPY_LONG + l, sizeof (*E) + l, 0);
        compute_get_uri_f_last_md5 (U);
        memcpy (E->md5, get_uri_f_last_md5.uri_md5.c, 16);
        memcpy (E->data, local_uri, l);
        */
      }
      out_log_event_write (&le_buff.local_uries);
    }
  }
}

static void cache_save_moved_server_binlog (void) {
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    kprintf ("%s: cannot write index: cannot compute its name\n", __func__);
    exit (1);
  }

  vkprintf (1, "%s: creating index %s at log position %lld\n", __func__, newidxname, log_cur_pos());

  newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    kprintf ("%s: cannot create new index file %s: %m\n", __func__, newidxname);
    exit (1);
  }

  int sz;
  clearin ();
  struct lev_start *E = cache_lev_start_alloc (index_new_cache_id, &sz);
  writeout (E, sz);

  out_log_event_alloc (LEV_TIMESTAMP, sizeof (struct lev_timestamp), time (NULL));
  out_log_event_write (&le_buff.timestamps);

  cache_all_uri_cached_foreach (func_save_cached_uri, cgsl_order_top);

  /*
  struct cache_local_copy_server *S;
  for (i = 0; i < DISABLED_SERVER_HASH_PRIME; i++) {
    for (S = HDS[i]; S != NULL; S = S->hnext) {
      //TODO:
      //_tot_disabled_servers++;
      //writeout (&S->id, 4);
      //writeout (&S->bitset, 32);
    }
  }
  */

  flushout ();

  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);

  vkprintf (3, "index written ok\n");

  if (rename_temporary_snapshot (newidxname)) {
    kprintf ("cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);
  printf ("uries\t%lld\n"
          "local_uries\t%lld\n"
          "access_logevents\t%lld\n"
          "set_size_logevents\t%lld\n"
          "max_filesize\t%lld\n",
     le_buff.uries, le_buff.local_uries,
     le_buff.access_logevents, le_buff.set_size_logevents,
     le_buff.max_filesize);
}

int cache_pseudo_index_check_mode (int index_mode) {
  return index_mode == 2 || index_mode == 3 || index_mode == 4;
}

int cache_pseudo_index_init (int index_mode, int timestamp, const char *const index_filter_server_list, int new_cache_id){
  assert (timestamp > 0);
  if (index_mode == 3) {
    if (!new_cache_id) {
      kprintf ("%s: new_cache_id wasn't given.\n", __func__);
      return -1;
    }
  }
  if (index_filter_server_list == NULL) {
    if (index_mode == 3) {
      kprintf ("%s: moved local server mapping filename wasn't given.\n", __func__);
      return -1;
    }
  } else {
    if (cache_read_local_server_list (index_filter_server_list, index_mode == 2 ? 1 : 2) < 0) {
      return -1;
    }
  }

  index_timestamp = timestamp;
  cache_index_mode = index_mode;

  index_new_cache_id = new_cache_id;
  return 0;
}


void cache_save_pseudo_index (void) {
  switch (cache_index_mode) {
    case 2: cache_dump_local_uries_mapping (); break;
    case 3: cache_save_moved_server_binlog (); break;
    case 4: cache_dump_uncached_uries (); break;
    default: kprintf ("%s: Unknown index mode %d\n", __func__, cache_index_mode); exit (1);
  }
}
