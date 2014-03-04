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

#define _FILE_OFFSET_BITS 64 

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <limits.h>
#include <errno.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kdb-statsx-binlog.h"
#include "statsx-data.h"
#include "server-functions.h"
#include "net-memcache-server.h"
#include "am-hash.h"

#include "vv-tl-parse.h"
#include "statsx-tl.h"
#include "vv-tl-aio.h"

#define LINE vkprintf (0, "At line %d\n", __LINE__)

extern int auto_create_new_versions;
extern int custom_version_names;
extern int incr_version;
int last_incr_version;
extern long long incr_counter_id;
extern int incr_version_read;
extern long long memory_to_index;
extern int index_fix_mode;

int index_version;

int tot_counters, tot_counter_instances, tot_counters_allocated;
long long tot_views;
long long tot_deletes;
long long deleted_by_lru;
long long tot_memory_allocated;
int newidx_fd;
long long tot_aio_loaded_bytes = 0;
long long tot_aio_fails;
long long tot_aio_queries;
int last_index_day;

//int tz_offset = 0;  // 3*3600;
int today_start;

int tot_user_metafiles = 0;
long long tot_user_metafile_bytes = 0;

int mode_ignore_user_id = 0;

int index_size;
int max_counters = 1000000, counters_prime;
double max_counters_growth_percent = 10.0;

long long *index_offset, *index_offset_head, *index_monthly_offset;
long long *index_cnt;
int *index_deleted;

int load_counter (long long cnt_id, int startup, int use_aio);
int index_get_idx (long long count_id);
static void do_use (struct counter *C);
static void copy_ancestor (struct counter *dst, struct counter *src);
static struct counter *malloc_counter (struct counter *D, long long cnt_id);
int free_LRU (void);
int check_version (long long counter_id, int version);

extern int create_day_start;

struct counter counter_wait = {
  .type = -2
};




void *zzmalloc (int l) {
  void *t;
  if (l < MAX_ZALLOC) {
    t = zmalloc (l);
  } else {
    t = malloc (l);
    assert (t);
  }
  if (t) {
    tot_memory_allocated += l;
  }
  return t;
}

void *zzmalloc0 (int l) {
  void *t;
  if (l < MAX_ZALLOC) {
    t = zmalloc0 (l);
  } else {
    t = malloc (l);
    assert (t);
    memset (t, 0, l);
  }
  if (t) {
    tot_memory_allocated += l;
  }
  return t;
}

void zzfree (void *p, int l) {
  if (l < MAX_ZALLOC) {
    zfree (p, l);
  } else {
    free (p);
  }
  tot_memory_allocated -= l;
}

void *zzrealloc0 (void *p, int old_len, int new_len) {
  void *tmp = zzmalloc0 (new_len);
  assert (tmp);
  int t = old_len;
  if (old_len > new_len) {
    t = old_len;
  }
  memcpy (tmp, p, t);
  if (p) {
    zzfree (p, old_len);
  }
  return tmp;  
}

/* --------- counters data ------------- */

struct counter *counters_commit_head;

struct counter **Counters;
//struct counter *Counters[COUNTERS_PRIME + 1];

int stats_replay_logevent (struct lev_generic *E, int size);
int stats_replay_logevent_reverse (struct lev_generic *E, int size);
extern int reverse_index_mode;
extern int monthly_stat;

int init_stats_data (int schema) {
  static int initialized = 0;
  if (initialized) {
    return 0;
  }
  initialized = 1;

  if (!reverse_index_mode) {
    replay_logevent = stats_replay_logevent;
  } else {
    replay_logevent = stats_replay_logevent_reverse;
  }

  counters_commit_head = 0;
  counters_prime = am_choose_hash_prime (max_counters * 1.5);
  vkprintf (1, "max_counters: %d, counters_prime: %d\n", max_counters, counters_prime);
  Counters = zzmalloc0 ((counters_prime + 1) * sizeof (struct counter *));
  struct counter *C;
  Counters[counters_prime] = C = zzmalloc0 (sizeof (struct counter));
  assert (C != NULL);
  C->next_use = C;
  C->prev_use = C;

  return 0;
}

inline int ipopcount(unsigned long long b) {
  int n;
  for(n = 0; b != 0; n++, b &= b - 1) {}
  return n;
}
/*
int packed_subcnt_get (struct counter *c, int idx ) {
  int i;
  unsigned long long u, w, m = 1;
  if (idx < 0 || idx >= MAX_SUBCNT) return 0;
  m <<= idx;
  if (m & c->mask_subcnt) {
    u = c->mask_subcnt;
    for (i=0;;i++) {
      w = u & (u - 1);
      if (m == (u ^ w) ) break;
      u = w;
    }
    return c->subcnt[i];
  }
  else {
    return 0;
  }
}
*/
int packed_subcnt_increment (struct counter *c, int idx, int delta ) {
  int i,j;
  unsigned long long u, w, m = 1;
  int *p;
  if (verbosity >= 4) {
    fprintf(stderr, "packed_subcnt_increment(c = %p, idx = %d, delta = %d)\n", c, idx, delta);
    fprintf(stderr, "c->mask_subcnt = %llx\n", c->mask_subcnt);
  }
  m <<= idx;
  if (m & c->mask_subcnt) {
    u = c->mask_subcnt;
    for (i=0;;i++) {
      w = u & (u - 1);
      if (m == (u ^ w) ) break;
      u = w;
    }
    return ( c->subcnt[i] += delta ) ;
  }
  else {
    u = c->mask_subcnt;
    j = -1;
    for (i=0;u != 0;i++) {
      w = u & (u - 1);
      if (m > (u ^ w)) j = i;
      u = w;
    }    
    p = (int*) zzmalloc(sizeof(int) * (i+1));
    p[j+1] = delta;
    if (i > 0) {
      memcpy(p, c->subcnt, sizeof(int) * (j+1));
      memcpy(p+(j+2), c->subcnt + (j+1), sizeof(int) * (i - (j+1)));
      zzfree(c->subcnt, sizeof(int) * i);
    }
    c->subcnt = p;
    c->mask_subcnt |= m;
    if (verbosity >= 4) {
      fprintf(stderr, "c->subcnt = ");
      for (j=0;j<=i;j++) fprintf (stderr, "%d ", c->subcnt[j]);
      fprintf(stderr, "\n");
    }
    return delta;
  }
}
/* ---------- tree functions ------------ */

int alloc_tree_nodes;

tree_t *new_tree_node (int x, int y) {
  tree_t *P;
  P = zzmalloc (sizeof (tree_t));
  assert (P);
  alloc_tree_nodes++;
  P->left = P->right = 0;
  P->x = x;
  P->y = y;
  return P;
}

tree_t *tree_lookup (tree_t *T, int x) {
  while (T && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

void tree_split (tree_t **L, tree_t **R, tree_t *T, int x) {
  if (!T) { *L = *R = 0; return; }
  if (x < T->x) {
    *R = T;
    tree_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_split (&T->right, R, T->right, x);
  }
}


int check_tree (tree_t *T) {
  if (!T) {
    return 1;
  }
  if (T->left) { assert (T->left->x < T->x); }
  if (T->right) { assert (T->right->x > T->x); }
  check_tree (T->left);
  check_tree (T->right);
  return 1;
}

tree_t *tree_insert (tree_t *T, int x, int y) {
  tree_t *P;
  if (!T) { return new_tree_node (x, y); }
  assert (x != T->x);
  if (T->y >= y) {
    if (x < T->x) {
      T->left = tree_insert (T->left, x, y);
    } else {
      T->right = tree_insert (T->right, x, y);
    }
    return T;
  }
  P = new_tree_node (x, y);
  tree_split (&P->left, &P->right, T, x);
  return P;
}

int tree_merge_array (tree_t *T, int *a1, int l1, int **a2, int *l2, int cur_day, int r) {
  if (!T) {
    return r;
  }
  if (T->left) { assert (T->left->x < T->x); }
  if (T->right) { assert (T->right->x > T->x); }
  assert (*l2 >= 0);
  r = tree_merge_array (T->left, a1, l1, a2, l2, cur_day, r);
  //fprintf (stderr, "T->x = %d\n", T->x);
  while (*l2 && (*a2)[0] < T->x) {
    assert (r < l1);
    a1[2 * r + 0] = (*a2)[0];
    a1[2 * r + 1] = (*a2)[1];
    (*l2)--;
    (*a2)+=2;
    r++;
  }
  assert (r < l1);
  a1[2 * r + 0] = T->x;
  a1[2 * r + 1] = cur_day;
  r++;
  if (*l2 && (*a2)[0] == T->x) {
    (*l2)--;
    (*a2)+=2;
  }
  assert (!(*l2) || (*a2)[0] > T->x);
  return tree_merge_array (T->right, a1, l1, a2, l2, cur_day, r);
}

int tree_merge_arr (tree_t *T, int *a1, int l1, int **a2, int *l2, int cur_day) {
  int r = tree_merge_array (T, a1, l1, a2, l2, cur_day, 0);
  while (*l2) {
    assert (r < l1);
    a1[2 * r + 0] = (*a2)[0];
    a1[2 * r + 1] = (*a2)[1];
    (*l2)--;
    (*a2)+=2;
    r++;
  }
  return r;
}

static void free_tree_node (tree_t *T) {
  zzfree (T, sizeof (tree_t));
}

void free_tree (tree_t *T) {
  if (T) {
    free_tree (T->left);
    free_tree (T->right);
    free_tree_node (T);
  }
}

static int stats_le_start (struct lev_start *E) {
  if (E->schema_id != STATS_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

static int set_incr_version (long long a, int b) {
  incr_counter_id = a;
  incr_version = b;
  incr_version_read = 1;
  return 0;
}

static int get_cnt_type (long long cnt_id) {
  if (cnt_id >= 0) { return 0;}
  else if (cnt_id < 0 && cnt_id >= -1000000000) { return 1;}
  else if (cnt_id < -1000000000) {return 2;}
  else { return 3;}
}

static inline int tz_offset (char timezone) {
  return ((timezone & 0x1F) - 12)*3600;
}

struct counter CounterDeleted;
static void free_counter (struct counter *C, int recursive);

int get_month (int created_at);
int get_year (int created_at);
static struct counter *get_counter_f (long long cnt_id, int force) {
  long long h1, h2;
  struct counter *C, *D;
  if (!cnt_id) { return 0; }
  h1 = h2 = cnt_id;
  if (h1 < 0) { h1 = 17-h1; }
  h1 = h1 % counters_prime;
  if (h2 < 0) { h2 = 17239-h2; }
  if (h1 < 0 || h2 < 0) { vkprintf (1, "h2 = %lld, cnt_id = %lld\n", h2, cnt_id); /*assert (0);*/ return 0; }
  h2 = 1 + (h2 % (counters_prime - 1));

  if (custom_version_names && force != 3 && force != 0) {
    //fprintf (stderr, "%d\n", cnt_id);
    if (index_version <= 1) {
      if (cnt_id != incr_counter_id) {
      	fprintf (stderr, "Error: cnt_id = %lld. incr_counter_id = %lld\n", cnt_id, incr_counter_id);
      	incr_version_read = 1;
      }
      //assert (cnt_id == incr_counter_id);
      if (!incr_version_read) {
        if (cnt_id == (int)cnt_id) {
          struct lev_stats_visitor_version *LV = 
            alloc_log_event (LEV_STATS_VISITOR_VERSION, sizeof (struct lev_stats_visitor_version), cnt_id);
          LV->version = incr_version;    
        } else {
          struct lev_stats_visitor_version64 *LV = 
            alloc_log_event (LEV_STATS_VISITOR_VERSION_64, sizeof (struct lev_stats_visitor_version64), cnt_id);
          LV->cnt_id = cnt_id;
          LV->version = incr_version;    
        }
      }
    } else {
      assert (index_version == 2 || index_version == 3 || index_version == 4);
      if (last_incr_version != incr_version && !incr_version_read) {
        if (cnt_id == (int)cnt_id) {
          struct lev_stats_visitor_version *LV = 
            alloc_log_event (LEV_STATS_VISITOR_VERSION, sizeof (struct lev_stats_visitor_version), cnt_id);
          LV->version = incr_version;    
          last_incr_version = incr_version;
        } else {
          struct lev_stats_visitor_version64 *LV = 
            alloc_log_event (LEV_STATS_VISITOR_VERSION_64, sizeof (struct lev_stats_visitor_version64), cnt_id);
          LV->cnt_id = cnt_id;
          LV->version = incr_version;    
          last_incr_version = incr_version;
        }
      }
    }
  }

  int first_free = -1;
  while ((D = Counters[h1]) != 0) {
    if (D == &CounterDeleted && first_free < 0) {
      first_free = h1;
    }
    if (D != &CounterDeleted && D->counter_id == cnt_id) {
      if (force < 0) {
        free_counter (D, 1);
        Counters[h1] = &CounterDeleted;
        tot_counters --;
        return 0;
      }
      if (force != 2 && (D->valid_until > now || !force)) {
        if (!custom_version_names || incr_version == D->created_at || incr_version == -1 || !force) {
          do_use (D);
          return D;
        } else if (incr_version < D->created_at) {
          return 0;
        } else {
          break;
        }
      } else {
        break;
      }
    }
    h1 += h2;
    if (h1 >= counters_prime) { h1 -= counters_prime; }
  }
  if (!force) { return 0; }

  if (tot_counters >= max_counters) {
    kprintf ("%s: tot_counters(%d) >= max_counters(%d)\n", __func__, tot_counters, max_counters);
    exit (1);
    return 0;
  }

  assert (!D || !custom_version_names || D->created_at < incr_version);

  C = malloc_counter (D, cnt_id);
  if (!C) { return C; }
  if (!D && first_free >= 0)  {
    Counters[first_free] = C;
  } else {
    Counters[h1] = C;
  }
  tot_counter_instances++;
  if (!D) { tot_counters++; }
  else { 
    if (!monthly_stat) {
      free_tree (D->visitors);  
      D->visitors = 0; 
    }
  }
  if (monthly_stat /*&& force != 3*/ && (D && get_month (D->created_at) != get_month (C->created_at))) {
    C->type |= COUNTER_TYPE_MONTH;
  }
  do_use (C);
  return C;
}


static struct counter *get_counter_old (long long cnt_id, int created_at, int use_aio) {
  if (verbosity >= 3) {
    fprintf (stderr, "get_counter_old (%lld, %d, %d)\n", cnt_id, created_at, use_aio);
  }

  if (use_aio > 0) {
    if (!check_version (cnt_id, created_at)) { return 0; }
    
    struct counter *D = get_counter_old (cnt_id, created_at, -1);
    if (D) { return D;}

  }


  struct counter *C = get_counter_f (cnt_id, 0);
  if (!C) { return C;}
  if (use_aio >= 0) {
    int res = load_counter (cnt_id, 0, use_aio);
    if (res == -1 && !C) {
      return 0;
    }
    if (res == -2) {
      return &counter_wait;
    }
  }
  if (!created_at) {
    do_use (C);
    return C;
  }


  while (C) {
    if (C->created_at < created_at) { 
      //if (use_aio) {
      //  assert (0);
      //}
      return 0; 
    }
    if (C->created_at == created_at) { break; }
    C = C->prev;
  }

  //if (use_aio == 1) {
  //  assert (C);
  //}

  if (C) { do_use (C); }
  return C;
}

static void copy_ancestor (struct counter *dst, struct counter *src) {
  dst->timezone = src->timezone;
  dst->meta = src->meta;
  src->meta = 0;
  //dst->type = src->type;
}
/* delete counter C from use (circular double linked list) */
static void del_use (struct counter *C) {
  if (C->prev_use && C->next_use) {
    C->next_use->prev_use = C->prev_use;
    C->prev_use->next_use = C->next_use;
    C->next_use = 0;
    C->prev_use = 0;
  }
}

static void add_use (struct counter *C) {
  C->next_use = Counters[counters_prime]->next_use;
  Counters[counters_prime]->next_use = C;
  C->prev_use = Counters[counters_prime];
  C->next_use->prev_use = C;
}

static void do_use (struct counter *C) {
  if (C->prev_use && C->next_use) {
    del_use (C);
    add_use (C);
  }
}

static void set_perm (struct counter *C) {
  //C->type &= (~COUNTER_TYPE_DELETABLE);
  del_use (C);
}


static void free_counter (struct counter *C, int recursive) {
  if (verbosity >= 4 && recursive) {
    fprintf (stderr, "free counter %p\n", C);
  }
  if (!C) { return; }
  del_use (C);
  if (recursive && C->prev) { free_counter (C->prev, recursive); }
  if (C->visitors_age) { zzfree (C->visitors_age,sizeof(int) * MAX_AGE); }
  if (C->visitors_mstatus) { zzfree (C->visitors_mstatus, sizeof(int) * MAX_MSTATUS); }
  if (C->visitors_polit) { zzfree (C->visitors_polit, sizeof(int) * MAX_POLIT); }
  if (C->visitors_section) { zzfree (C->visitors_section, sizeof(int) * MAX_SECTION); }
  if (C->visitors_cities) { zzfree (C->visitors_cities - 2, sizeof(int) * 2 * (C->visitors_cities[-2] + 1)); }
  if (C->visitors_sex_age) { zzfree (C->visitors_sex_age, sizeof(int) * MAX_SEX_AGE); }
  if (C->visitors_countries) { zzfree (C->visitors_countries - 2, sizeof(int) * 2 * (C->visitors_countries[-2] + 1)); }
  if (C->visitors_geoip_countries) { zzfree (C->visitors_geoip_countries - 2, sizeof(int) * 2 * (C->visitors_geoip_countries[-2] + 1)); }
  if (C->visitors_source) { zzfree (C->visitors_source, sizeof(int) * MAX_SOURCE); }
  //if (C->subcnt) { zzfree (C->subcnt, sizeof(int) * C->subcnt_number); }
  if (C->subcnt) { zzfree(C->subcnt, sizeof(int) * ipopcount(C->mask_subcnt)); }  
  free_tree (C->visitors);
  if (C->meta) { zzfree (C->meta, sizeof (struct metafile)); }
  zzfree (C, sizeof (struct counter));
  tot_counters_allocated--;
  if (verbosity >= 4 && recursive) {
    fprintf (stderr, "free counter done\n");
  }
}

extern int default_timezone;

static struct counter *malloc_counter (struct counter *D, long long cnt_id) {
  if (verbosity >= 4) {
    fprintf (stderr, "Allocating new counter\n");
  }
  struct counter *C = zzmalloc0 (sizeof (struct counter));
  if (!C) return C;
  tot_counters_allocated++;
  C->counter_id = cnt_id;
  C->prev = 0;
  C->timezone = default_timezone;
  if (D) {
    copy_ancestor (C, D);
  }
  if (create_day_start) {
    C->created_at = now - (now + tz_offset (C->timezone)) % 86400;
  } else {
    C->created_at = now;
  }
  C->last_month_unique_visitors = -1;
  C->long_unique_visitors = -1;
  C->last_week_unique_visitors = -1;
  if (auto_create_new_versions) {
    C->valid_until = now - (now + tz_offset (C->timezone)) % 86400 + 86400;
  } else {
    C->valid_until = 0x7fffffff;
  }
  if (custom_version_names) {
    C->created_at = incr_version;
  }
  C->prev = D;
  if (!D || FORCE_COUNTER_TYPE) {
    C->type = get_cnt_type (cnt_id);
  }
  if (verbosity >= 4) {
    fprintf (stderr, "New counter allocated\n");
  }
  return C;
}

/* replay log */

#define LID(E) (*(long long *)&E->a)

int stats_replay_logevent (struct lev_generic *E, int size) {
  int s;
  struct counter *C;
  if (tot_memory_allocated > memory_to_index) {
    fprintf (stderr, "not enough memory to start. creating index.\n");
    fprintf (stderr, "tot_memory_allocated = %lld, memory_to_index = %lld\n", tot_memory_allocated, (long long)memory_to_index);
    save_index ();
    exit (13);
  }
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    return stats_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
  case LEV_TAG:
  case LEV_CBINLOG_END:
    return default_replay_logevent (E, size);
  case LEV_STATS_VIEWS_EXT:
    if (size < 12) { return -2; }
    C = get_counter_f (E->a, 0);
    if (C) { C->views += E->b; }
    tot_views += E->b;
    return 12;
  case LEV_STATS_VIEWS_EXT_64:
    if (size < 16) { return -2; }
    C = get_counter_f (LID(E), 0);
    if (C) { C->views += E->c; }
    tot_views += E->c;
    return 16;
  case LEV_STATS_VIEWS ... LEV_STATS_VIEWS + 0xff:
    if (size < 8) { return -2; }
    C = get_counter_f (E->a, 0);
    if (C) { C->views += E->type & 0xff; }
    tot_views += E->type & 0xff;
    return 8;
  case LEV_STATS_VIEWS_64 ... LEV_STATS_VIEWS_64 + 0xff:
    if (size < 12) { return -2; }
    C = get_counter_f (LID(E), 0);
    if (C) { C->views += E->type & 0xff; }
    tot_views += E->type & 0xff;
    return 12;
  case LEV_STATS_VISITOR_OLD:
    if (size < 12) { return -2; }
    counter_incr (E->a, E->b, 1, 0, -1);
    return 12;
  case LEV_STATS_VISITOR_OLD_64:
    if (size < 16) { return -2; }
    counter_incr (LID (E), E->c, 1, 0, -1);
    return 16;
  case LEV_STATS_VISITOR ... LEV_STATS_VISITOR + 0xff:
    if (size < 12) { return -2; }
    counter_incr (E->a, E->b, 1, (E->type & 0x80) >> 7 , (E->type & 0x7f)-1);
    return 12;
  case LEV_STATS_VISITOR_64 ... LEV_STATS_VISITOR_64 + 0xff:
    if (size < 16) { return -2; }
    counter_incr (LID(E), E->c, 1, (E->type & 0x80) >> 7 , (E->type & 0x7f)-1);
    return 16;
  case LEV_STATS_VISITOR_OLD_EXT:
    if (size < 20) { return -2; }
    struct lev_stats_visitor_old_ext *LVold = (struct lev_stats_visitor_old_ext *) E;
    counter_incr_ext (LVold->cnt_id, LVold->user_id, 1, 0, -1, LVold->sex_age >> 4, 
      LVold->sex_age & 15, LVold->m_status, LVold->polit_views, LVold->section, LVold->city, 0, 0, 0);
    return 20;
  case LEV_STATS_VISITOR_OLD_EXT_64:
    if (size < 24) { return -2; }
    {
      struct lev_stats_visitor_old_ext64 *LVold = (struct lev_stats_visitor_old_ext64 *) E;
      counter_incr_ext (LVold->cnt_id, LVold->user_id, 1, 0, -1, LVold->sex_age >> 4, 
        LVold->sex_age & 15, LVold->m_status, LVold->polit_views, LVold->section, LVold->city, 0, 0, 0);
    }
    return 24;
  case LEV_STATS_VISITOR_EXT ... LEV_STATS_VISITOR_EXT + 0xff:
    if (size < sizeof (struct lev_stats_visitor_ext)) { return -2; }
    struct lev_stats_visitor_ext *LV = (struct lev_stats_visitor_ext *) E;
    counter_incr_ext (LV->cnt_id, LV->user_id, 1, (LV->type & 0x80) >> 7, (LV->type & 0x7f) - 1, LV->sex_age >> 4, 
      LV->sex_age & 15, LV->m_status, LV->polit_views, LV->section, LV->city, LV->geoip_country, LV->country, LV->source);
    return sizeof (struct lev_stats_visitor_ext);
  case LEV_STATS_VISITOR_EXT_64 ... LEV_STATS_VISITOR_EXT_64 + 0xff:
    if (size < sizeof (struct lev_stats_visitor_ext64)) { return -2; }    
    {
      struct lev_stats_visitor_ext64 *LV = (struct lev_stats_visitor_ext64 *) E;
      counter_incr_ext (LV->cnt_id, LV->user_id, 1, (LV->type & 0x80) >> 7, (LV->type & 0x7f) - 1, LV->sex_age >> 4, 
        LV->sex_age & 15, LV->m_status, LV->polit_views, LV->section, LV->city, LV->geoip_country, LV->country, LV->source);
    }
    return sizeof (struct lev_stats_visitor_ext64);
  case LEV_STATS_COUNTER_ON:
    if (size < 8) { return -2; }
    enable_counter (E->a, 1);
    return 8;
  case LEV_STATS_COUNTER_ON_64:
    if (size < 12) { return -2; }
    enable_counter (LID(E), 1);
    return 12;
  case LEV_STATS_COUNTER_OFF:
    if (size < 8) { return -2; }
    disable_counter (E->a, 1);
    return 8;
  case LEV_STATS_COUNTER_OFF_64:
    if (size < 12) { return -2; }
    disable_counter (LID(E), 1);
    return 12;
  case LEV_STATS_TIMEZONE ... LEV_STATS_TIMEZONE + 0xff:
    if (size < 8) { return -2; }
    set_timezone (E->a, E->type - LEV_STATS_TIMEZONE, 1);
    return 8;
  case LEV_STATS_TIMEZONE_64 ... LEV_STATS_TIMEZONE_64 + 0xff:
    if (size < 12) { return -2; }
    set_timezone (LID(E), E->type - LEV_STATS_TIMEZONE, 1);
    return 12;
  case LEV_STATS_VISITOR_VERSION:
    if (size < 12) { return -2; }
    set_incr_version (E->a, E->b);
    return 12;    
  case LEV_STATS_VISITOR_VERSION_64:
    if (size < 16) { return -2; }
    set_incr_version (LID(E), E->c);
    return 16;    
  case LEV_STATS_DELETE_COUNTER:
    if (size < sizeof (struct lev_delete_counter)) { return -2; }
    delete_counter (E->a, 1);
    return sizeof (struct lev_delete_counter);    
  case LEV_STATS_DELETE_COUNTER_64:
    if (size < sizeof (struct lev_delete_counter64)) { return -2; }
    delete_counter (LID(E), 1);
    return sizeof (struct lev_delete_counter64);    
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;

}

int timestamp_found, crc_found, timestamp_to_index;
int save_index_reverse (void);

int stats_replay_logevent_reverse (struct lev_generic *E, int size) {
  vkprintf (3, "E->type = %x\n", E->type);
  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -1; }
    return stats_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
  case LEV_TIMESTAMP:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_CRC32:
    if (size < sizeof (struct lev_crc32)) { return -2;}
    if (crc_found) {
      assert (timestamp_found);
      if (E->a >= timestamp_to_index && !custom_version_names) {
        save_index_reverse ();
        exit (0);
      }
    } else {
      vkprintf (0, "Custom crc32 event handler\n");
      crc_found = 1;
      if (!timestamp_found && !custom_version_names) {
        timestamp_found = 1;
        timestamp_to_index = (E->a / 86400 + 1) * 86400;
      }
      relax_log_crc32 (0);
      log_crc32_complement = ~(((struct lev_crc32 *)E)->crc32);
    }
    return default_replay_logevent (E, size);
  case LEV_STATS_VIEWS_EXT:
    return 12;
  case LEV_STATS_VIEWS ... LEV_STATS_VIEWS + 0xff:
    if (size < 8) { return -2; }
    return 8;
  case LEV_STATS_VISITOR_OLD:
    if (size < 12) { return -2; }
    return 12;
  case LEV_STATS_VISITOR ... LEV_STATS_VISITOR + 0xff:
    if (size < 12) { return -2; }
    return 12;
  case LEV_STATS_VISITOR_OLD_EXT:
    if (size < 20) { return -2; }
    return 20;
  case LEV_STATS_VISITOR_EXT ... LEV_STATS_VISITOR_EXT + 0xff:
    if (size < sizeof (struct lev_stats_visitor_ext)) { return -2; }
    return sizeof (struct lev_stats_visitor_ext);
  case LEV_STATS_COUNTER_ON:
    if (size < 8) { return -2; }
    return 8;
  case LEV_STATS_COUNTER_OFF:
    if (size < 8) { return -2; }
    return 8;
  case LEV_STATS_TIMEZONE ... LEV_STATS_TIMEZONE + 0xff:
    if (size < 8) { return -2; }
    return 8;
  case LEV_STATS_VISITOR_VERSION:
    if (size < 12) { return -2; }
    if (custom_version_names) {
      if (!timestamp_found) {
        timestamp_found = 1;
        timestamp_to_index = E->b + 1;
      } else {
        if (timestamp_to_index <= E->b) {
          assert (crc_found);
          save_index_reverse ();
          exit (0);
        }
      }
    }
    return 12;    
  case LEV_STATS_DELETE_COUNTER:
    if (size < sizeof (struct lev_delete_counter)) { return -2; }
    return sizeof (struct lev_delete_counter);    
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;

}

/* interface */

static inline int cnt_incr (int **p, int s, int t) {
  if (s > 0 && s <= t) {
    if (!*p) { *p = zzmalloc0 (t * sizeof(int)); }
    (*p)[s-1]++;
    return s;
  } else {
    return 0;
  }
}

//city, country, geoip country
static void add_list (int **p, int id, int MIN_LIST_SIZE, int MAX_LIST_SIZE) {
  int *q = *p;
  int i, c;
  if (!q) {
    *p = q = (int *) zzmalloc (MIN_LIST_SIZE * 2 * sizeof(int)) + 2;
    q[-2] = MIN_LIST_SIZE-1;
    q[-1] = 0;
  }
  for (i = 0; i < q[-1]; i++) {
    if (q[2*i] == id) {
      c = ++q[2*i+1];
      if (i && c > q[2*i-1]) {
        while (i && c > q[2*i-1]) {
          q[2*i] = q[2*i-2];
          q[2*i+1] = q[2*i-1];
          i--;
        }
        q[2*i] = id;
        q[2*i+1] = c;
      }
      return;
    }
  }
  if (i == q[-2]) {
    if (i < MAX_LIST_SIZE) {
      int *r = q;
      q = (int *) zzmalloc ((i + 1) * 4 * sizeof (int)) + 2;
      q[-2] = 2*i+1;
      memcpy (q, r, i * 2 * sizeof(int));
      zzfree (r - 2, (i + 1) * 2 * sizeof (int));
      *p = q;
    } else {
      return;
    }
  }
  assert (i < q[-2]);
  q[2*i] = id;
  q[2*i+1] = 1;
  q[-1] = i+1;
}

                                                       
int counter_incr (long long counter_id, int user_id, int replaying, int op, int subcnt) {
  int subcnt_value = 0;
  if (verbosity >= 4) {
    fprintf (stderr, "counter_incr (%lld, %d, %d, %d, %d)\n", counter_id, user_id, replaying, op, subcnt);
  }
  
  struct counter *C = get_counter_f (counter_id, replaying ? 3 : 1);
  if (verbosity >= 3) {
    fprintf (stderr, "Got counter\n");
  }

  if (!C) { return -1; }
  set_perm (C);


  if (verbosity >= 3) {
    fprintf (stderr, "Counter type is %d\n", C->type);
  }

  //if (-1 > subcnt || subcnt >= types[C->type].field_number) {
  if (-1 > subcnt || subcnt >= MAX_SUBCNT) {
    if (verbosity >= 1) {
      fprintf (stderr, "Unknown subcounter (%d). Skipping.\n", subcnt);
    }
    subcnt = -1;
  }
  
  if (op == 0) {  
    tot_views++;
  } else {
    tot_deletes++;
  }
  
  if (subcnt == -1) {
    if (op == 0) {
      C->views++;  
    } else {
      C->deletes++;
    }
  } else {
    assert (subcnt < MAX_SUBCNT);
    
    if (verbosity >= 4) {
      fprintf (stderr, "incrementing subcounter... ");
    }
    subcnt_value = packed_subcnt_increment(C, subcnt, (op == 0) ? 1 : (-1));
    
    if (verbosity >= 4) {
      fprintf (stderr, "done\n");
    }
  }

  if (subcnt != -1 || !tree_lookup (C->visitors, user_id)) {
    if (subcnt == -1) {
      if ((now >= today_start && !mode_ignore_user_id) || monthly_stat)  {
        //assert (!tree_lookup (C->visitors, user_id));
        //assert (check_tree (C->visitors));
        C->visitors = tree_insert (C->visitors, user_id, lrand48());
      }
      C->unique_visitors++;
    }

    if (!replaying) {
      if (verbosity >= 4) {
        fprintf (stderr, "creating logevent\n");
      }
      if (counter_id == (int)counter_id) {
        struct lev_stats_visitor *LV = 
          alloc_log_event (LEV_STATS_VISITOR + (op << 7) + subcnt + 1, sizeof (struct lev_stats_visitor), counter_id);
        LV->user_id = user_id;
      } else {
        struct lev_stats_visitor64 *LV = 
          alloc_log_event (LEV_STATS_VISITOR_64 + (op << 7) + subcnt + 1, sizeof (struct lev_stats_visitor64), counter_id);
        LV->cnt_id = counter_id;
        LV->user_id = user_id;
      }
      if (verbosity >= 4) {
        fprintf (stderr, "finished creating logevent\n");
      }
    }
  } else if (!replaying && !C->views_uncommitted++) {
    C->commit_next = counters_commit_head;
    counters_commit_head = C;
  }
  if (subcnt == -1) {
    return C->unique_visitors;
  } else {
    return subcnt_value;
  }
}

int counter_incr_ext (long long counter_id, int user_id, int replaying, int op, int subcnt, int sex, int age, int m_status, int polit_views, int section, int city, int geoip_country,int country, int source) {
  int subcnt_value = 0;
  if (verbosity >= 4) {
    fprintf (stderr, "counter_incr_ext (%lld, %d, %d, %d, %d)\n", counter_id, user_id, replaying, op, subcnt);
  }

  int sex_age = 16*sex + age;
  struct counter *C = get_counter_f (counter_id, replaying ? 3 : 1);
  if (!C) { return -1; }
  set_perm (C);

  if (-1 > subcnt || subcnt >= MAX_SUBCNT) {
    if (verbosity >= 1) {
      fprintf (stderr, "Unknown subcounter (%d). Skipping.\n", subcnt);
    }
    subcnt = -1;
  }

  if (op == 0) {
    tot_views++;
  } else {
    tot_deletes++;
  }
  
  if (subcnt == -1) {
    if (op == 0) {
      C->views++;  
    } else {
      C->deletes++;
    }
  } else {
    /*
    if (C->subcnt_number != types[C->type].field_number) {
      int *t = (int *)zzrealloc0 ((char *)C->subcnt, sizeof (int) * C->subcnt_number, sizeof (int) * types[C->type].field_number);
      if (!t) {
        if (verbosity >= 1) {
          fprintf (stderr, "Can not realloc data for subcounters: not enough mem\n");
        }
      } else {
        C->subcnt = t;
        C->subcnt_number = types[C->type].field_number;
      }
    } 
    if (subcnt >= C->subcnt_number) {
      return -1;
    }
    */
    subcnt_value = packed_subcnt_increment(C, subcnt, (op == 0) ? 1 : (-1));
  }

  if (verbosity >= 4) {
    fprintf (stderr, "Starting\n");
  }

  
  if (subcnt != -1 || !tree_lookup (C->visitors, user_id)) {
    if (subcnt == -1) {
      if ((now >= today_start && !mode_ignore_user_id) || monthly_stat) {
        //assert (!tree_lookup (C->visitors, user_id));
        //assert (check_tree (C->visitors));
        C->visitors = tree_insert (C->visitors, user_id, lrand48());
      }
      if (verbosity >= 4) {
        fprintf (stderr, "Inserted to tree\n");
      }
      C->unique_visitors++;
      if (sex > 0 && sex <= MAX_SEX) { C->visitors_sex[sex-1]++; } else { sex = 0; }
      age = cnt_incr (&C->visitors_age, age, MAX_AGE);
      m_status = cnt_incr (&C->visitors_mstatus, m_status, MAX_MSTATUS);
      polit_views = cnt_incr (&C->visitors_polit, polit_views, MAX_POLIT);
      section = cnt_incr (&C->visitors_section, section, MAX_SECTION);
      if (age > 0 && sex > 0) {
        cnt_incr (&C->visitors_sex_age, (sex-1)*MAX_AGE+age, MAX_SEX_AGE);
      }
      source = cnt_incr (&C->visitors_source, source, MAX_SOURCE);
      if (city > 0) {
        add_list (&C->visitors_cities, city, MIN_CITIES, MAX_CITIES);
      }
      if (country > 0) {
        add_list (&C->visitors_countries, country, MIN_COUNTRIES, MAX_COUNTRIES);
      }
      if (geoip_country > 0) {
        add_list (&C->visitors_geoip_countries, geoip_country, MIN_GEOIP_COUNTRIES, MAX_GEOIP_COUNTRIES);
      }
    }
    if (verbosity >= 4) {
      fprintf (stderr, "Creating logevent\n");
    }
    if (!replaying) {
      if (sex | age | m_status | polit_views | section | city | country | geoip_country | source) {
        if (counter_id == (int)counter_id) {
          struct lev_stats_visitor_ext *LV = 
            alloc_log_event (LEV_STATS_VISITOR_EXT + (op << 7) + subcnt + 1, sizeof (struct lev_stats_visitor_ext), counter_id);
          LV->user_id = user_id;
          LV->sex_age = sex_age;
          LV->m_status = m_status;
          LV->polit_views = polit_views;
          LV->section = section;
          LV->city = city;
          LV->country = country;
          LV->geoip_country = geoip_country;
          LV->source = source;
        } else {
          struct lev_stats_visitor_ext64 *LV = 
            alloc_log_event (LEV_STATS_VISITOR_EXT_64 + (op << 7) + subcnt + 1, sizeof (struct lev_stats_visitor_ext64), counter_id);
          LV->cnt_id = counter_id;
          LV->user_id = user_id;
          LV->sex_age = sex_age;
          LV->m_status = m_status;
          LV->polit_views = polit_views;
          LV->section = section;
          LV->city = city;
          LV->country = country;
          LV->geoip_country = geoip_country;
          LV->source = source;
        }
      } else {
        if (counter_id == (int)counter_id) {
          struct lev_stats_visitor *LV = 
            alloc_log_event (LEV_STATS_VISITOR + (op << 7) + subcnt + 1, sizeof (struct lev_stats_visitor), counter_id);
          LV->user_id = user_id;
        } else {
          struct lev_stats_visitor64 *LV = 
            alloc_log_event (LEV_STATS_VISITOR_64 + (op << 7) + subcnt + 1, sizeof (struct lev_stats_visitor64), counter_id);
          LV->cnt_id = counter_id;
          LV->user_id = user_id;
        }
      }
    }
  } else if (!replaying && !C->views_uncommitted++) {
    C->commit_next = counters_commit_head;
    counters_commit_head = C;
  }
  if (verbosity >= 4) {
    fprintf (stderr, "Incr done\n");
  }
  if (subcnt == -1) {
    return C->unique_visitors;
  } else {
    return subcnt_value;
  }
}

/* adding new log entries */

int flush_view_counters (void) {
  struct counter *C = counters_commit_head, *D;
  int cnt = 0;
  if (verbosity > 1) {
    fprintf (stderr, "flush_view_counters()\n");
  }
  counters_commit_head = 0;
  while (C) {
    assert (++cnt <= 1000000);
    D = C->commit_next;
    C->commit_next = 0;
    assert (C->views_uncommitted > 0);
    if (C->counter_id == (int)C->counter_id) {
      struct lev_stats_views_ext *LV;
      if (C->views_uncommitted < 256) {
        LV = alloc_log_event (LEV_STATS_VIEWS + C->views_uncommitted, 8, C->counter_id);
      } else {
        LV = alloc_log_event (LEV_STATS_VIEWS_EXT, 12, C->counter_id);
        LV->views = C->views_uncommitted;
      }
    } else {
      struct lev_stats_views_ext64 *LV;
      if (C->views_uncommitted < 256) {
        LV = alloc_log_event (LEV_STATS_VIEWS_64 + C->views_uncommitted, 12, C->counter_id);
      } else {
        LV = alloc_log_event (LEV_STATS_VIEWS_EXT_64, 16, C->counter_id);
        LV->views = C->views_uncommitted;
      }
      LV->cnt_id = C->counter_id;
    }
    C->views_uncommitted = 0;
    C = D;
  }
  return cnt;
}


int enable_counter (long long counter_id, int replay) {
  if (custom_version_names || create_day_start) {
    return 0;
  }  
  struct counter *C = get_counter_f (counter_id, 1);
  if (C && C->created_at == now && !replay) {
    if (counter_id == (int)counter_id) {
      alloc_log_event (LEV_STATS_COUNTER_ON, 8, counter_id);
    } else {
      struct lev_generic *E = alloc_log_event (LEV_STATS_COUNTER_ON_64, 12, counter_id);
      LID (E) = counter_id;
    }
  }
  return C ? 1 : 0;
}

int disable_counter (long long counter_id, int replay) {
  struct counter *C = get_counter_f (counter_id, 0);
  if (!C || now >= C->valid_until || custom_version_names || create_day_start) {
    if (verbosity) {
      fprintf (stderr, "disable failed: %p %d/%d\n", C, C->valid_until, now);
    }
    return 0;
  }
  C->valid_until = now;
  if (!replay) {
    if (counter_id == (int)counter_id) {
      alloc_log_event (LEV_STATS_COUNTER_OFF, 8, counter_id);
    } else {
      struct lev_generic *E = alloc_log_event (LEV_STATS_COUNTER_OFF_64, 12, counter_id);
      LID (E) = counter_id;
    }
  }
  return 1;
}

int get_counter_views (long long counter_id) {
  struct counter *C = get_counter_old (counter_id, 0, 1);
  if (!C) { return -1;}
  if (C->type == -2) { return -2;}
  return C->views;
}

int get_counter_visitors (long long counter_id) {
  struct counter *C = get_counter_old (counter_id, 0, 1);
  if (!C) { return -1;}
  if (C->type == -2) { return -2;}
  return C->unique_visitors;
}

int get_counter_views_given_version (long long counter_id, int version) {
  struct counter *C = get_counter_old (counter_id, version, 1);
  if (!C) { return -1;}
  if (C->type == -2) { return -2;}
  return C->views;
}

int get_counter_visitors_given_version (long long counter_id, int version) {
  struct counter *C = get_counter_old (counter_id, version, 1);
  if (!C) { return -1;}
  if (C->type == -2) { return -2;}
  return C->unique_visitors;
}

extern int Q_raw;

static char *serialize_list (char *ptr, const char *name, int *list, int num) {
  int i, cnt = 0;
  for (i = 0; i < num; i++) {
    if (list[i]) { cnt++; }
  }
  ptr += sprintf (ptr, "s:%ld:\"%s\";a:%d:{", (long) strlen(name), name, cnt);
  for (i = 0; i < num; i++) {
    if (list[i]) {
      ptr += sprintf (ptr, "i:%d;i:%d;", i+1, list[i]);
    }
  }
  *ptr++ = '}';
  return ptr;
}

static int *serialize_list_raw (int *ptr, int *list, int num) {
  int i;
  if (!list) {
    *(ptr++) = 0;
    return ptr;
  }
  *(ptr++) = num;
  for (i = 0; i < num; i++) {
    *(ptr++) = list[i];
  }
  return ptr;
}


static char *serialize_list2 (char *ptr, const char *name, int *list, int num) {
  ptr += sprintf (ptr, "s:%ld:\"%s\";a:%d:{", (long) strlen (name), name, num);
  int i;
  for (i = 0; i < 2*num; i++) {
    ptr += sprintf (ptr, "i:%d;", list[i]);
  }
  *ptr++ = '}';
  return ptr;
}

static int *serialize_list2_raw (int *ptr, int *list, int num) {
  int i;
  if (!list) {
    *(ptr++) = 0;
    return ptr;
  }
  *(ptr++) = num;
  for (i = 0; i < 2*num; i++) {
    *(ptr++) = list[i];
  }
  return ptr;
}

static char *serialize_list2a (char *ptr, const char *name, int *list, int num) {
  ptr += sprintf (ptr, "s:%ld:\"%s\";a:%d:{", (long) strlen (name), name, num);
  int i;
  for (i = 0; i < 2*num; i++) {
    static char t[4];
    t[0] = (char)((list[i]>>16) & 0xff);
    t[1] = (char)((list[i]>>8) & 0xff);
    t[2] = (char)(list[i] & 0xff);
    t[3] = 0;
    ptr += (i&1) ? sprintf (ptr, "i:%d;", list[i]) : sprintf(ptr, "s:%d:\"%s\";", (int)strlen (t), t);
  }
  *ptr++ = '}';
  return ptr;
}

static int *serialize_list2a_raw (int *ptr, int *list, int num) {
  int i;
  if (!list) {
    *(ptr++) = 0;
    return ptr;
  }
  *(ptr++) = num;
  for (i = 0; i < 2*num; i++) {
    *(ptr++) = list[i];
  }
  return ptr;
}

static char *serialize_subcnt_list (char *ptr, struct counter *C) {
  /* 
     don't output broken stats (dirty hack)
     php: mktime (12, 0, 0, 2, 2, 2011) == 1296637200
          Feb 02 2011, 12:00
  */
  if (C->valid_until < 1296637200) {
    ptr += sprintf (ptr, "s:5:\"extra\";a:0:{}");
    return ptr;
  }
  int num = ipopcount(C->mask_subcnt);
  unsigned long long u = 1;
  int i, j;
  ptr += sprintf (ptr, "s:5:\"extra\";a:%d:{", num);
  for (i = 0, j = 0; i < 64; i++, u <<= 1) 
    if (u & C->mask_subcnt) {
      ptr += sprintf (ptr, "i:%d;i:%d;", i, C->subcnt[j++]);
    }
  *ptr++ = '}';
  return ptr;
}

static int *serialize_subcnt_list_raw (int *ptr, struct counter *C) {
  /* 
     don't output broken stats (dirty hack)
     php: mktime (12, 0, 0, 2, 2, 2011) == 1296637200
          Feb 02 2011, 12:00
  */
  if (C->valid_until < 1296637200) {
    *(ptr++)  = 0;
    return ptr;
  }
  if (!C->subcnt) {
    *(ptr++) = 0;
    return ptr;
  }
  *(ptr++) = 64;
  unsigned long long u = 1;
  int i, j;
  for (i = 0, j = 0; i < 64; i++, u <<= 1) 
    if (u & C->mask_subcnt) {
      *(ptr++) = C->subcnt[j++];
    } else {
      *(ptr++) = 0;
    }
  return ptr;
}

static void list_add (int *a, int *b, int cnt) {
  if (!a || !b) { return; }
  int i;
  for (i = 0; i < cnt; i++) {
    a[i] += b[i];
  }
}

int counter_serialize_raw (struct counter *C, char *buffer) {
  if (!C) { return 0; }
  int *ptr = (void *)buffer;
  *(ptr++) = C->views;
  *(ptr++) = C->unique_visitors;
  *(ptr++) = C->deletes;
  *(ptr++) = C->created_at;
  *(ptr++) = C->valid_until;
  ptr = serialize_list_raw (ptr, C->visitors_sex, MAX_SEX); 
  ptr = serialize_list_raw (ptr, C->visitors_age, MAX_AGE); 
  ptr = serialize_list_raw (ptr, C->visitors_mstatus, MAX_MSTATUS); 
  ptr = serialize_list_raw (ptr, C->visitors_polit, MAX_POLIT); 
  ptr = serialize_list_raw (ptr, C->visitors_section, MAX_SECTION); 
  ptr = serialize_list_raw (ptr, C->visitors_sex_age, MAX_AGE * 2); 
  ptr = serialize_list2_raw (ptr, C->visitors_cities, C->visitors_cities ? C->visitors_cities[-1] : 0);
  ptr = serialize_list2a_raw (ptr, C->visitors_countries, C->visitors_countries ? C->visitors_countries[-1] : 0);
  ptr = serialize_list2a_raw (ptr, C->visitors_geoip_countries, C->visitors_geoip_countries ? C->visitors_geoip_countries[-1] : 0);
  ptr = serialize_list_raw (ptr, C->visitors_source, MAX_SOURCE);
  ptr = serialize_subcnt_list_raw (ptr, C);
  return (char *)ptr - buffer;
}

int counter_serialize (struct counter *C, char *buffer) {
  char *ptr = buffer;
  if (!C) { return 0; }
  if (C->type == -2) return -2;
  int cnt = 6 + (C->long_unique_visitors >= 0) + (C->last_week_unique_visitors >= 0);
  if (C->visitors_age) { cnt++; }
  if (C->visitors_mstatus) { cnt++; }
  if (C->visitors_polit) { cnt++; }
  if (C->visitors_section) { cnt++; }
  if (C->visitors_cities) { cnt++; }
  if (C->visitors_sex_age) { cnt++; }
  if (C->visitors_countries) { cnt++; }
  if (C->visitors_geoip_countries) { cnt++; }
  if (C->visitors_source) { cnt++; }
  if (C->mask_subcnt) { cnt++; }
  ptr += sprintf (ptr, "a:%d:{s:5:\"views\";i:%d;s:8:\"visitors\";i:%d;s:7:\"deletes\";i:%d;"
    "s:7:\"created\";i:%d;s:7:\"expires\";i:%d;"
    "s:3:\"sex\";a:2:{i:1;i:%d;i:2;i:%d;}", 
    cnt, C->views, C->unique_visitors, C->deletes, C->created_at, C->valid_until,
    C->visitors_sex[0], C->visitors_sex[1]);
  if (C->long_unique_visitors >= 0) {
    ptr += sprintf (ptr, "s:26:\"last_month_unique_visitors\";i:%d;", C->long_unique_visitors);
  }
  if (C->last_week_unique_visitors >= 0) {
    ptr += sprintf (ptr, "s:25:\"last_week_unique_visitors\";i:%d;", C->last_week_unique_visitors);
  }
  if (C->visitors_age) { 
    ptr = serialize_list (ptr, "age", C->visitors_age, MAX_AGE); 
  }
  if (C->visitors_mstatus) { 
    ptr = serialize_list (ptr, "marital_status", C->visitors_mstatus, MAX_MSTATUS); 
  }
  if (C->visitors_polit) { 
    ptr = serialize_list (ptr, "political_views", C->visitors_polit, MAX_POLIT); 
  }
  if (C->visitors_section) { 
    ptr = serialize_list (ptr, "section", C->visitors_section, MAX_SECTION); 
  }
  if (C->visitors_sex_age) { 
    ptr = serialize_list (ptr, "sex_age", C->visitors_sex_age, MAX_AGE * 2); 
  }
  if (C->visitors_cities) {
    ptr = serialize_list2 (ptr, "cities", C->visitors_cities, C->visitors_cities[-1]);
  }
  if (C->visitors_countries) {
    ptr = serialize_list2a (ptr, "countries", C->visitors_countries, C->visitors_countries[-1]);
  }
  if (C->visitors_geoip_countries) {
    ptr = serialize_list2a (ptr, "geoip_countries", C->visitors_geoip_countries, C->visitors_geoip_countries[-1]);
  }
  if (C->visitors_source) {
    ptr = serialize_list (ptr, "sources", C->visitors_source, MAX_SOURCE);
  }
  if (C->mask_subcnt) {
    ptr = serialize_subcnt_list (ptr, C);
  }
  *ptr++ = '}';
  *ptr = 0;
  if (verbosity >= 4) {
    fprintf (stderr, "%s\n", buffer);
  }
  return ptr - buffer;
}

void list_add_sub (int *a, int *b, unsigned long long mask) {
  int i;
  int j = 0;
  for (i = 0; i < 64; i++) if (mask & (1ull << i)) {
    a[i] += b[j ++];
  }
}

struct counter *get_counters_sum (long long counter_id, int start_version, int end_version) {
  static struct counter *C;  
  if (!C) {
    C = malloc_counter (0, counter_id);
    assert (C);
    C->visitors_age = zzmalloc (sizeof (int) * MAX_AGE);
    C->visitors_mstatus = zzmalloc (sizeof (int) * MAX_MSTATUS);
    C->visitors_polit = zzmalloc (sizeof (int) * MAX_POLIT);
    C->visitors_sex_age = zzmalloc (sizeof (int) * MAX_AGE * 2);
    C->visitors_section = zzmalloc (sizeof (int) * MAX_SECTION);
    C->visitors_source = zzmalloc (sizeof (int) * MAX_SOURCE);
    C->subcnt = zzmalloc (sizeof (int) * 64);
    C->mask_subcnt = (unsigned long long)-1;
  }
  C->views = 0;
  C->deletes = 0;  
  memset (C->visitors_sex, 0, MAX_SEX * 4);
  memset (C->visitors_age, 0, MAX_AGE * 4);
  memset (C->visitors_mstatus, 0, MAX_MSTATUS * 4);
  memset (C->visitors_polit, 0, MAX_POLIT * 4);
  memset (C->visitors_sex_age, 0, MAX_AGE * 8);
  memset (C->visitors_section, 0, MAX_SECTION * 4);
  memset (C->visitors_source, 0, MAX_SOURCE * 4);
  memset (C->subcnt, 0, sizeof (int) * 64);
  struct counter *D = get_counter_f (counter_id, 0);
  if (!D) { return 0; }
  while (1) {
    if (D->created_at < start_version) { return C; }
    if (D->created_at <= end_version) {
      C->views += D->views;
      C->deletes += D->deletes;
      list_add (C->visitors_sex, D->visitors_sex, MAX_SEX);
      list_add (C->visitors_age, D->visitors_age, MAX_AGE);
      list_add (C->visitors_mstatus, D->visitors_mstatus, MAX_MSTATUS);
      list_add (C->visitors_polit, D->visitors_polit, MAX_POLIT);
      list_add (C->visitors_sex_age, D->visitors_sex_age, MAX_AGE * 2);
      list_add (C->visitors_section, D->visitors_section, MAX_SECTION);
      list_add (C->visitors_source, D->visitors_source, MAX_SOURCE);
      list_add_sub (C->subcnt, D->subcnt, D->mask_subcnt);
    }
//    if (C->flags & 
    if (D->type & COUNTER_TYPE_LAST) { break; } 
    if (!D->prev) {
      if (load_counter (counter_id, 0, 1) <= 0) { 
        return (void *)-2l; 
      }
      D = D->prev;    
      if (!D) { return (void *)-2l; }
    } else {
      D = D->prev;
    }
  }
  return C;
}

int get_counter_serialized_raw (char *buffer, long long counter_id, int version) {
//  int *ptr = (int *)buffer;
  struct counter *C = get_counter_old (counter_id, version, 1);
  if (!C) { return 0; }
  if (C->type == -2) return -2;
  return counter_serialize_raw (C, buffer);
}

void tl_serialize_list (int *ptr, int size) {
  if (!ptr) {
    int i;
    for (i = 0; i < size; i++) {
      tl_store_int (0);
    }
  } else {
    tl_store_raw_data (ptr, 4 * size);
  }
} 

void tl_serialize_list_2 (int *ptr) {
  int e = ptr ? ptr[-1] : 0;
  tl_store_int (e);
  tl_store_raw_data (ptr, 8 * e);
} 

void tl_serialize_list_2a (int *ptr) {
  int e = ptr ? ptr[-1] : 0;
  tl_store_int (e);
  int i;
  for (i = 0; i < e; i++) {
    static char t[4];
    t[0] = (char)((ptr[2 * i]>>16) & 0xff);
    t[1] = (char)((ptr[2 * i]>>8) & 0xff);
    t[2] = (char)(ptr[2 * i] & 0xff);
    t[3] = 0;
    tl_store_string0 (t);
    tl_store_int (ptr[2 * i + 1]);
  }
}

void tl_serialize_subcnt_list (struct counter *C) {
  /* 
     don't output broken stats (dirty hack)
     php: mktime (12, 0, 0, 2, 2, 2011) == 1296637200
          Feb 02 2011, 12:00
  */
  if (C->valid_until < 1296637200) {
    tl_store_int (0);
    return;
  }
  int num = ipopcount(C->mask_subcnt);
  tl_store_int (num);
  unsigned long long u = 1;
  int i, j;
  for (i = 0, j = 0; i < 64; i++, u <<= 1) {
    if (u & C->mask_subcnt) {
      tl_store_int (i);
      tl_store_int (C->subcnt[j ++]);
    }
  }
}

void tl_serialize_counter (struct counter *C, int mode) {
  assert (C);
  if (mode & TL_STATSX_SEX) {
    tl_serialize_list (C->visitors_sex, MAX_SEX);
  }
  if (mode & TL_STATSX_AGE) {
    tl_serialize_list (C->visitors_age, MAX_AGE);
  }
  if (mode & TL_STATSX_MSTATUS) {
    tl_serialize_list (C->visitors_mstatus, MAX_MSTATUS);
  }
  if (mode & TL_STATSX_POLIT) {
    tl_serialize_list (C->visitors_polit, MAX_POLIT);
  }
  if (mode & TL_STATSX_SECTION) {
    tl_serialize_list (C->visitors_section, MAX_SECTION);
  }
  if (mode & TL_STATSX_CITY) {
    tl_serialize_list_2 (C->visitors_cities);
  }
  if (mode & TL_STATSX_GEOIP_COUNTRY) {
    tl_serialize_list_2a (C->visitors_geoip_countries);
  }
  if (mode & TL_STATSX_COUNTRY) {
    tl_serialize_list_2a (C->visitors_countries);
  }
  if (mode & TL_STATSX_SOURCE) {
    tl_serialize_list (C->visitors_source, MAX_SOURCE);
  }
  if (mode & TL_STATSX_VIEWS) {
    tl_store_int (C->views);
  }
  if (mode & TL_STATSX_VISITORS) {
    tl_store_int (C->unique_visitors);
  }
  if (mode & TL_STATSX_SEX_AGE) {
    tl_serialize_list (C->visitors_sex_age, MAX_SEX_AGE);
  }
  if (mode & TL_STATSX_MONTHLY) {
    tl_store_int (C->last_month_unique_visitors);
  }
  if (mode & TL_STATSX_WEEKLY) {
    tl_store_int (C->last_week_unique_visitors);
  }
  if (mode & TL_STATSX_DELETES) {
    tl_store_int (C->deletes);
  }
  if (mode & TL_STATSX_VERSION) {
    tl_store_int (C->created_at);
  }
  if (mode & TL_STATSX_EXPIRES) {
    tl_store_int (C->valid_until);
  }
  if (mode & TL_STATSX_EXTRA) {
    tl_serialize_subcnt_list (C);
  }
}

int tl_get_counter_serialized (long long counter_id, int version, int mode) {
  struct counter *C = get_counter_old (counter_id, version, 1);
  if (!C) { 
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }
  if (C->type == -2) { return -2; }
  tl_store_int (TL_MAYBE_TRUE);
  tl_serialize_counter (C, mode);
  return 0;
}

int get_counter_serialized (char *buffer, long long counter_id, int version) {
/*  if (verbosity) {
    int l = index_get_idx (counter_id);
    while (l != index_size && index_cnt[l] == counter_id) {
      fprintf (stderr, "%d ", index_cnt_ver[l]);
      l++;
    }
    fprintf (stderr, "\n");
    char* bf = zzmalloc0 (2000);
    get_counter_versions (bf, counter_id);
    fprintf (stderr, "%s\n", bf);
    zzfree (bf, 2000);
      
  }*/
  if (Q_raw) {
    return get_counter_serialized_raw (buffer, counter_id, version);
  }
  struct counter *C = get_counter_old (counter_id, version, 1);
  if (!C) { return 0; }
  if (C->type == -2) { return -2; }
  return counter_serialize (C, buffer);
}

int get_counter_versions_raw (char *buffer, long long counter_id) {
  int *ptr = (int *)buffer;
  struct counter *C = get_counter_f (counter_id, 0);
  while (C && ptr < (int *)(buffer + 100000) && C->prev >= 0) {
    *(ptr++) = C->created_at;
    C = C->prev;
  }
  if (ptr < (int *)(buffer + 100000)) {
    if (load_counter (counter_id, 0, 1) == -2) {
      return -2;
    }
    ptr = (int *)buffer;
    C = get_counter_f (counter_id, 0);
    while (C && ptr < (int *)(buffer + 100000) && C->prev >= 0) {
      *(ptr++) = C->created_at;
      C = C->prev;
    }
  }
  return ((char *)ptr) - buffer;
}

int get_counter_versions (char *buffer, long long counter_id) {
  if (Q_raw) {
    return get_counter_versions_raw (buffer, counter_id);
  }
  char *ptr = buffer;
  struct counter *C = get_counter_f (counter_id, 0);
  while (C && ptr < buffer + 100000 && C->prev >= 0) {
    if (ptr > buffer) { *ptr++ = ','; }
    ptr += sprintf (ptr, "%d", C->created_at);
    C = C->prev;
  }
  if (ptr < buffer + 100000) {
    if (load_counter (counter_id, 0, 1) == -2) {
      return -2;
    }
    ptr = buffer;
    C = get_counter_f (counter_id, 0);
    while (C && ptr < buffer + 100000 && C->prev >= 0) {
      if (ptr > buffer) { *ptr++ = ','; }
      ptr += sprintf (ptr, "%d", C->created_at);
      C = C->prev;
    }
  }
  return ptr - buffer;
}

int tl_get_counter_versions (long long counter_id) {
  if (load_counter (counter_id, 0, 1) == -2) {
    return -2;
  }
  struct counter *C = get_counter_f (counter_id, 0);
  if (!C) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }
  tl_store_int (TL_MAYBE_TRUE);
  int *count = tl_store_get_ptr (4);
  *count = 0;
  while (C) {
    tl_store_int (C->created_at);
    (*count) ++;
    C = C->prev;
  }
  return 0;
}

int get_monthly_visitors_serialized_raw (char *buffer, long long counter_id) {
  if (load_counter (counter_id, 0, 1) == -2) {
    return -2;
  }
  int *ptr = (int *)buffer;
  struct counter *C = get_counter_f (counter_id, 0);
  int first = 1;
  while (C) {
    //fprintf (stderr, ".%d %d\n", C->type & COUNTER_TYPE_MONTH, C->last_month_unique_visitors);
    if (first && !(C->type & COUNTER_TYPE_MONTH) && C->last_month_unique_visitors >= 0) {
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      m ++;
      *(ptr ++) = y * 100 + m;
      *(ptr ++) = C->last_month_unique_visitors;
      first = 0;
    }
    if ((C->type & COUNTER_TYPE_MONTH) && C->last_month_unique_visitors >= 0) {
      if (first) {
        int m = get_month (C->created_at);
        int y = get_year (C->created_at);
        m ++;
        *(ptr ++) = y * 100 + m;
        *(ptr ++) = 0;
        first = 0;
      }
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      //m--;
      if (!m) {
        m = 12;
        y--;
      }
      *(ptr ++) = y * 100 + m;
      *(ptr ++) = C->last_month_unique_visitors;
      //fprintf (stderr, "%d\n", C->created_at);
    }   
    //fprintf (stderr, "%d,%d,%d\n", C->created_at, C->last_month_unique_visitors, C->type & COUNTER_TYPE_MONTH8);
    C = C->prev;
  }
  return ((char *)ptr) - buffer;
}

int get_monthly_visitors_serialized (char *buffer, long long counter_id) {
  if (load_counter (counter_id, 0, 1) == -2) {
    return -2;
  }
  if (Q_raw) {
    return get_monthly_visitors_serialized_raw (buffer, counter_id);
  }
  char *ptr = buffer;
  struct counter *C = get_counter_f (counter_id, 0);
  int r = 0;
  int first = 1;
  while (C) {
    //fprintf (stderr, ".%d %d\n", C->type & COUNTER_TYPE_MONTH, C->last_month_unique_visitors);
    if (first && !(C->type & COUNTER_TYPE_MONTH) && C->last_month_unique_visitors >= 0) {
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      m ++;
      r = 1;
      ptr += sprintf (ptr, "%d,%d,%d", C->last_month_unique_visitors, m, y);
      first = 0;
    }
    if (verbosity >= 2) {
      if (C->type & COUNTER_TYPE_MONTH) {
        int m = get_month (C->created_at);
        int y = get_year (C->created_at);
        fprintf (stderr, "%d:%d:%d\n", m, y, C->created_at);
      }
    }
    if ((C->type & COUNTER_TYPE_MONTH) && C->last_month_unique_visitors >= 0) {
      if (first) {
        int m = get_month (C->created_at);
        int y = get_year (C->created_at);
        m ++;
        r = 1;
        ptr += sprintf (ptr, "%d,%d,%d", 0, m, y);
        first = 0;
      }
      if (r) {
        ptr += sprintf (ptr, ",");
      } else {
        r = 1;
      }
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      //m--;
      if (!m) {
        m = 12;
        y--;
      }
      ptr += sprintf (ptr, "%d,%d,%d", C->last_month_unique_visitors, m, y);
      //fprintf (stderr, "%d\n", C->created_at);
    }   
    //fprintf (stderr, "%d,%d,%d\n", C->created_at, C->last_month_unique_visitors, C->type & COUNTER_TYPE_MONTH8);
    C = C->prev;
  }
  return ptr - buffer;
}

int tl_get_monthly_visitors_serialized (long long counter_id) {
  if (load_counter (counter_id, 0, 1) == -2) {
    return -2;
  }
  struct counter *C = get_counter_f (counter_id, 0);
  if (!C) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  } 
  tl_store_int (TL_MAYBE_TRUE);
  int *count = tl_store_get_ptr (4);
  *count = 0;
  int first = 1;
  while (C) {
    if (first && !(C->type & COUNTER_TYPE_MONTH) && C->last_month_unique_visitors >= 0) {
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      m ++;
      tl_store_int (y);
      tl_store_int (m);
      tl_store_int (C->last_month_unique_visitors);
      (*count) ++;
      first = 0;
    }
    if ((C->type & COUNTER_TYPE_MONTH) && C->last_month_unique_visitors >= 0) {
      if (first) {
        int m = get_month (C->created_at);
        int y = get_year (C->created_at);
        m ++;
        tl_store_int (y);
        tl_store_int (m);
        tl_store_int (0);
        (*count) ++;
        first = 0;
      }
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      if (!m) {
        m = 12;
        y--;
      }
      tl_store_int (y);
      tl_store_int (m);
      tl_store_int (C->last_month_unique_visitors);
      (*count) ++;
    }   
    C = C->prev;
  }
  return 0;
}

int get_monthly_views_serialized_raw (char *buffer, long long counter_id) {
  if (load_counter (counter_id, 0, 1) == -2) {
    return -2;
  }
  int *ptr = (int *)buffer;
  struct counter *C = get_counter_f (counter_id, 0);
  int current_month_views = 0;
  while (C) {
    //fprintf (stderr, ".%d %d\n", C->type & COUNTER_TYPE_MONTH, C->last_month_unique_visitors);
    current_month_views += C->views;
    if (!C->prev || get_month (C->created_at) != get_month (C->prev->created_at) || get_year (C->created_at) != get_year (C->prev->created_at)) {
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      //m--;
      m ++;
      *(ptr ++) = y * 100 + m;
      *(ptr ++) = current_month_views;
      current_month_views = 0;
      //fprintf (stderr, "%d\n", C->created_at);
    }   
    //fprintf (stderr, "%d,%d,%d\n", C->created_at, C->last_month_unique_visitors, C->type & COUNTER_TYPE_MONTH8);
    C = C->prev;
  }
  return ((char *)ptr) - buffer;
}

int get_monthly_views_serialized (char *buffer, long long counter_id) {
  if (load_counter (counter_id, 0, 1) == -2) {
    return -2;
  }
  if (Q_raw) {
    return get_monthly_views_serialized_raw (buffer, counter_id);
  }
  char *ptr = buffer;
  struct counter *C = get_counter_f (counter_id, 0);
  int r = 0;
  int current_month_views = 0;
  while (C) {
    //fprintf (stderr, ".%d %d\n", C->type & COUNTER_TYPE_MONTH, C->last_month_unique_visitors);
    current_month_views += C->views;
    if (!C->prev || get_month (C->created_at) != get_month (C->prev->created_at) || get_year (C->created_at) != get_year (C->prev->created_at)) {
      if (r) {
        ptr += sprintf (ptr, ",");
      } else {
        r = 1;
      }
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      //m--;
      m ++;
      ptr += sprintf (ptr, "%d,%d,%d", current_month_views, m, y);
      current_month_views = 0;
      //fprintf (stderr, "%d\n", C->created_at);
    }   
    //fprintf (stderr, "%d,%d,%d\n", C->created_at, C->last_month_unique_visitors, C->type & COUNTER_TYPE_MONTH8);
    C = C->prev;
  }
  return ptr - buffer;
}

int tl_get_monthly_views_serialized (long long counter_id) {
  if (load_counter (counter_id, 0, 1) == -2) {
    return -2;
  }
  struct counter *C = get_counter_f (counter_id, 0);
  if (!C) {
    tl_store_int (TL_MAYBE_FALSE);
    return 0;
  }
  tl_store_int (TL_MAYBE_TRUE);
  int *count = tl_store_get_ptr (4);
  *count = 0;
  int current_month_views = 0;
  while (C) {
    //fprintf (stderr, ".%d %d\n", C->type & COUNTER_TYPE_MONTH, C->last_month_unique_visitors);
    current_month_views += C->views;
    if (!C->prev || get_month (C->created_at) != get_month (C->prev->created_at) || get_year (C->created_at) != get_year (C->prev->created_at)) {
      int m = get_month (C->created_at);
      int y = get_year (C->created_at);
      //m--;
      m ++;
      tl_store_int (y);
      tl_store_int (m);
      tl_store_int (current_month_views);
      current_month_views = 0;
      (*count) ++;
    }   
    C = C->prev;
  }
  return 0;
}

int check_version (long long counter_id, int version) {
  struct counter *C = get_counter_f (counter_id, 0);
  while (C) {
    if (C->created_at < version) { return 0; }
    if (C->created_at == version) { return 1; }
    C = C->prev;
  }
  return -1;
}

extern int ignore_set_timezone;
int set_timezone (long long counter_id, int timezone, int replay) {
  if (ignore_set_timezone) {
    return 0;
  }
  struct counter *C = get_counter_f (counter_id, 0);
  C->timezone = (char)timezone;  
  if (!replay) {
    if (counter_id == (int)counter_id) {
      struct lev_timezone *LV = alloc_log_event (LEV_STATS_TIMEZONE + (char)timezone, sizeof (struct lev_timezone), counter_id);
      LV->cnt_id = counter_id;  
    } else {
      struct lev_timezone64 *LV = alloc_log_event (LEV_STATS_TIMEZONE_64 + (char)timezone, sizeof (struct lev_timezone64), counter_id);
      LV->cnt_id = counter_id;  
    }
  }
  return timezone;
}                       

int get_timezone (long long counter_id) {
  struct counter *C = get_counter_f (counter_id, 0);
  if (!C) { return 0; }
  return C->timezone;
}                       

int delete_counter (long long counter_id, int replay) {
  if (!replay) {
    if (counter_id == (int)counter_id) {
      struct lev_delete_counter *LV = alloc_log_event (LEV_STATS_DELETE_COUNTER, sizeof (struct lev_delete_counter), counter_id);
      LV->cnt_id = counter_id;
    } else {
      struct lev_delete_counter64 *LV = alloc_log_event (LEV_STATS_DELETE_COUNTER_64, sizeof (struct lev_delete_counter64), counter_id);
      LV->cnt_id = counter_id;
    }
  }
  get_counter_f (counter_id, -1);
  int l = index_get_idx (counter_id);
  if (l < index_size) {
    index_deleted[l] = 1;
  }
  return 1;
}

/*
 *
 * GENERIC BUFFERED WRITE
 *
 */

#define BUFFSIZE  16777216

char Buff[BUFFSIZE], rBuff_static[BUFFSIZE], *rBuff = 0, *rptr = 0, *wptr = Buff;
long long write_pos;
int metafile_pos;
extern int idx_fd;
int rBuff_len = 0;

void flushout (void) {
  if (Buff < wptr) {
    int s = wptr - Buff;
    int w = write (newidx_fd, Buff, s);
    int e = errno;
    if (verbosity) {
      fprintf (stderr, "(s,w,BUFFSIZE) = (%d %d %d)\n", s, w, BUFFSIZE);
    }
    if (w == -1) {
      fprintf (stderr, "errno = %d (%s)\n", e, strerror (e));
    }
    assert (w == s);
  }
  wptr = Buff;
}

void clearin (void) {
  wptr = Buff + BUFFSIZE;
}

static inline void writeout (const void *D, size_t len) {
  const char *d = D;
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) { 
      memcpy (wptr, d, len);
      wptr += len;
      return;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout();
    }
  }                                
}

#define likely(x) __builtin_expect((x),1) 
#define unlikely(x) __builtin_expect((x),0)

static inline void writeout_long (long long value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 8)) {
    flushout();
  }
  *((long long *) wptr) = value;
  wptr += 8;
}

static inline void writeout_int (int value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 4)) {
    flushout();
  }
  *((int *) wptr) = value;
  wptr += 4;
}

static inline void writeout_short (int value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 2)) {
    flushout();
  }
  *((short *) wptr) = value;
  wptr += 2;
}

static inline void writeout_char (char value) {
  if (unlikely (wptr == Buff + BUFFSIZE)) {
    flushout();
  }
  *wptr++ = value;
}

void write_seek (long long new_pos) {
  flushout();
  assert (lseek (newidx_fd, new_pos, SEEK_SET) == new_pos);
  write_pos = new_pos;
}


/*
 *
 * GENERIC BUFFERED READ
 *
 */

long long readin_long () {
  rptr += 8;
  if (rptr - rBuff > rBuff_len) {
    if (verbosity >= 1) {
      fprintf (stderr, "Buffer size %d seems to be too small. Skipping read.\n", rBuff_len);
    }
  }
  return *(long long *)(rptr - 8);
}

int readin_int () {
  rptr += 4;
  if (rptr - rBuff > rBuff_len) {
    if (verbosity >= 1) {
      fprintf (stderr, "Buffer size %d seems to be too small. Skipping read.\n", rBuff_len);
    }
  }
  return *(int *)(rptr - 4);
}

int readin_char () {
  rptr ++;
  if (rptr - rBuff > rBuff_len) {
    if (verbosity >= 1) {
      fprintf (stderr, "Buffer size %d seems to be too small. Skipping read.\n", rBuff_len);
    }
  }
  return *(char *)(rptr - 1);
}

int readin (char *dst, int len) {
  if (rptr - rBuff > rBuff_len - len) {
    if (verbosity >= 1) {
      fprintf (stderr, "Buffer size %d seems to be too small. Skipping read.\n", rBuff_len);
    }
    return 0;    
  }
  memcpy (dst, rptr, len);
  rptr += len;
  return len;
}

int readin_skip (int l) {
  rptr += l;
  return l;
}

int readin_setpos (int p) {
  rptr = rBuff + p;
  return p;
}


int readin_reset (char *data, int data_len) {
  rBuff = data;
  rBuff_len = data_len;
  rptr = rBuff;
  return 0;
}




int write_tree (tree_t* v) {
  if (v == 0) {
    writeout_char (0);
    return 1;
  }
  int r = 0;
  writeout_char (1); r++;
  writeout_int (v->x); r += sizeof (int);
  writeout_int (v->y); r += sizeof (int);
  r += write_tree (v->left);
  r += write_tree (v->right);
  return r;
}

int write_list (int *a, int l) {
  writeout (a, sizeof (int) * l);
  return l * sizeof (int);
}

int write_list2 (int *a) {  
  writeout (a - 2, sizeof (int) * (a[-1] + 1) * 2);
  return sizeof (int) * (a[-1] + 1) * 2;
}

int write_counter (struct counter *C) {
  int i, j, r = 0;
  writeout_long (C->counter_id); r += 8;
  writeout_int (C->created_at); r += sizeof (int);
  writeout_int (C->type & ~COUNTER_TYPE_LAST); r += sizeof (int);
  writeout_int (C->views); r += sizeof (int);
  writeout_int (C->unique_visitors); r += sizeof (int);
  writeout_int (C->deletes); r += sizeof (int);
  writeout_int (C->created_at); r += sizeof (int);
  writeout_int (C->valid_until); r += sizeof (int);
  writeout_int (C->long_unique_visitors); r += sizeof (int);
  writeout_int (C->last_month_unique_visitors); r += sizeof (int);
  writeout_int (C->last_week_unique_visitors); r += sizeof (int);
  writeout (C->visitors_sex, sizeof (int) * 2); r += 2 * sizeof (int);
  int flag = 0;
  if (C->visitors_age) { flag |= 1 << 0; }
  if (C->visitors_mstatus) { flag |= 1 << 1; }
  if (C->visitors_polit) { flag |= 1 << 2; }
  if (C->visitors_section) { flag |= 1 << 3; }
  if (C->visitors_cities) { flag |= 1 << 4; }
  if (C->visitors_sex_age) { flag |= 1 << 5; }
  if (C->visitors_countries) { flag |= 1 << 6; }
  if (C->visitors_geoip_countries) { flag |= 1 << 7; }
  if (C->visitors_source) { flag |= 1 << 8; }
  writeout_int (flag); r += sizeof (int);
  if (C->visitors_age) { r += write_list (C->visitors_age, MAX_AGE); }
  if (C->visitors_mstatus) { r += write_list (C->visitors_mstatus, MAX_MSTATUS); }
  if (C->visitors_polit) { r += write_list (C->visitors_polit, MAX_POLIT); }
  if (C->visitors_section) { r += write_list (C->visitors_section, MAX_SECTION); }
  if (C->visitors_cities) { r += write_list2 (C->visitors_cities); }
  if (C->visitors_sex_age) { r += write_list (C->visitors_sex_age, MAX_SEX_AGE); }
  if (C->visitors_countries) { r += write_list2 (C->visitors_countries); }
  if (C->visitors_geoip_countries) { r += write_list2 (C->visitors_geoip_countries); }
  if (C->visitors_source) { r += write_list (C->visitors_source, MAX_SOURCE); }
  //writeout_int (C->subcnt_number); r += sizeof (int);
  //if (C->subcnt) { r += write_list (C->subcnt, C->subcnt_number); }
  writeout_long (C->mask_subcnt); r += sizeof (long long);
  j = ipopcount(C->mask_subcnt);
  for (i=0; i < j; i++) {
    writeout_int(C->subcnt[i]); r += sizeof(int);
  }
  writeout_char (C->timezone); r += sizeof (char);
  if (C->valid_until < now) {
    C->visitors = 0;
  }
  r += write_tree (C->visitors);
  return r;
}


tree_t* read_tree () {
  if (readin_char () == 0) {
    return 0;
  }
  int x = readin_int ();
  int y = readin_int ();
  tree_t *P = new_tree_node (x, y);
  P->left = read_tree ();
  P->right = read_tree ();
  if (P->left) { assert (P->left->x < P->x); }
  if (P->right) { assert (P->right->x > P->x); }
  return P;
}

void skip_tree (void) {
  if (readin_char () == 0) {
    return;
  }
  readin_int ();
  readin_int ();
  skip_tree ();
  skip_tree ();
}

int* read_list (int l) {
  int *a = zzmalloc0 (l * sizeof (int));
  assert (a);
  readin ((char *)a, l * sizeof (int));
  return a;
}

int* read_list2 () {
  int x = readin_int ();
  int y = readin_int ();
  assert (x >= y);
  int *a = zzmalloc0 (2 * (x + 1) * sizeof (int));
  assert (a);
  a[0] = x; a[1] = y; a += 2;
  readin ((char *)a, 2 * y * sizeof (int));
  return a;
}


int index_get_idx (long long count_id) {
  int l = -1;
  int r = index_size;
  while (r - l > 1) {
    int x = (r + l)>>1;
    if (index_cnt[x] < count_id) {
      l = x;
    } else {
      r = x;
    }
  }
  l++;
  if (l == index_size || index_cnt[l] != count_id || index_deleted[l]) {
    return index_size;
  } else {
    return l;
  }
}


struct counter* read_counter (int readtree) {
  int i,j;
  struct counter *C = malloc_counter (0, -1);
  assert (C->prev == 0);
  if (index_version >= 4) {
    C->counter_id = readin_long ();
  } else {
    C->counter_id = readin_int ();
  }
  C->created_at = readin_int ();
  C->type = readin_int ();
  if (FORCE_COUNTER_TYPE) {
    C->type = (C->type & 0xffff0000) | get_cnt_type (C->counter_id);
  }
  C->views = readin_int ();
  C->unique_visitors = readin_int ();
  C->deletes = readin_int ();
  C->created_at = readin_int ();
  C->valid_until = readin_int ();
  if (index_version >= 3) {
    C->long_unique_visitors = readin_int ();
    C->last_month_unique_visitors = readin_int ();
    C->last_week_unique_visitors = readin_int ();
  } else if (index_version >= 2) {
    C->long_unique_visitors = readin_int ();
    C->last_month_unique_visitors = readin_int ();
    C->last_week_unique_visitors = -1;
  } else {
    C->long_unique_visitors = -1;
    C->last_month_unique_visitors = -1;
    C->last_week_unique_visitors = readin_int ();
  }
  readin ((char *)C->visitors_sex, sizeof (int) * 2);
  int flag = readin_int ();
  if (flag & (1 << 0)) { C->visitors_age = read_list (MAX_AGE); }
  if (flag & (1 << 1)) { C->visitors_mstatus = read_list (MAX_MSTATUS); }
  if (flag & (1 << 2)) { C->visitors_polit = read_list (MAX_POLIT); }
  if (flag & (1 << 3)) { C->visitors_section = read_list (MAX_SECTION); }
  if (flag & (1 << 4)) { C->visitors_cities = read_list2 (); }
  if (flag & (1 << 5)) { C->visitors_sex_age = read_list (MAX_SEX_AGE); }
  if (flag & (1 << 6)) { C->visitors_countries = read_list2 (); }
  if (flag & (1 << 7)) { C->visitors_geoip_countries = read_list2 (); }
  if (flag & (1 << 8)) { C->visitors_source = read_list (MAX_SOURCE); }
  if (index_version == 0) {
    int t = readin_int ();
    if (t > 0) {
      fprintf (stderr, "Dropping old data about subcounters.\n");
    }
    readin_skip (t * sizeof (int));
    C->mask_subcnt = 0;
  } else {
    C->mask_subcnt = readin_long();
  }
  j = ipopcount(C->mask_subcnt);
  if (j > 0) {
    C->subcnt = zzmalloc(sizeof(int) * j);
    for(i=0; i < j; i++) {
      C->subcnt[i] = readin_int();
    }
  }

  //C->subcnt_number = readin_int ();
  //if (C->subcnt_number) { C->subcnt = read_list (C->subcnt_number); }
  C->timezone = readin_char ();
  if (readtree) { C->visitors = read_tree (); }
  else { skip_tree (); }
  assert (C->prev == 0);
  return C;
}


int cmp_cnt (const void *_a, const void *_b) { 
  int a = *(int *)_a;
  int b = *(int *)_b;
  if (Counters[a]->counter_id < Counters[b]->counter_id) return -1;
  if (Counters[a]->counter_id > Counters[b]->counter_id) return 1;
  if (Counters[a]->created_at < Counters[b]->created_at) return 1;
  if (Counters[a]->created_at > Counters[b]->created_at) return -1;
  return 0;
}


int free_LRU () {
  if (verbosity >= 3) { fprintf (stderr, "free_LRU\n"); }
  if (Counters[counters_prime] == 0) {
    return 0;
  }
  if (verbosity >= 3) { fprintf (stderr, "first and last are %p and %p\n", Counters[counters_prime]->prev_use, Counters[counters_prime]->next_use); }
  
  if (Counters[counters_prime]->prev_use == Counters[counters_prime]) {
    if (verbosity >= 3) {
      fprintf (stderr, "No elements can be deleted by LRU. Failed to free mem.\n");
    }
    return 0;
  } else {
    struct counter *C = Counters[counters_prime]->prev_use;
    if (C) {
      struct counter *D = get_counter_f (C->counter_id, 0);
      while (D && D->prev != C) {
        D = D->prev;
      }
      if (D) {
        assert (D->prev == C);
        D->prev = 0;
      }
      free_counter (C, 1);
      deleted_by_lru++;
    }
    return 1;
  }
}

int get_day (int created_at) {
  if (!custom_version_names) {
    if (create_day_start) {
      return (created_at + tz_offset (default_timezone) ) / 86400;
    } else {
      return (created_at) / 86400;
    }
  } else {
    long x = created_at;
    assert (sizeof (time_t) == sizeof (long));
    //struct tm *t = localtime ((time_t *)&x);
    struct tm *t = gmtime ((time_t *)&x);
    t->tm_sec = 0;
    t->tm_min = 0;
    t->tm_hour = 12;
    t->tm_mday = created_at % 100;
    t->tm_mon = created_at / 100 % 100 - 1;
    t->tm_year = created_at / 10000 - 1900;
    return mktime (t) / 86400;
  }
}

int get_month (int created_at) {
  if (!custom_version_names) {
    long x = created_at;
    if (create_day_start) {
      x += tz_offset (default_timezone);
    }
    //struct tm *t = localtime ((time_t *)&x);
    struct tm *t = gmtime ((time_t *)&x);
    return t->tm_mon;
  } else {
    return created_at / 100 % 100 - 1;
  }
}

int get_year (int created_at) {
  if (!custom_version_names) {
    long x = created_at;
    if (create_day_start) {
      x += tz_offset (default_timezone);
    }
    //struct tm *t = localtime ((time_t *)&x);
    struct tm *t = gmtime ((time_t *)&x);
    return t->tm_year + 1900;
  } else {
    return created_at / 10000;
  }
}

int obsolete_timestamp (int created_at, int max_timestamp) {
  return get_day (created_at) != get_day (max_timestamp);
}

int get_prev_month_end (int day) {
  long t1  = day * 86400  + 43200 ;
  assert (sizeof (time_t) == sizeof (long));
  //struct tm *t = localtime ((time_t *)&t1);
  struct tm *t = gmtime ((time_t *)&t1);
  return day - t->tm_mday;
}

int get_prev_month_start (int day) {
  return get_prev_month_end (get_prev_month_end (day)) + 1;
}

int get_cur_month_start (int day) {
  return get_prev_month_end (day) + 1;
}

int monthly_delete_old (int *a1, int l1, int *a2, int l2, int day) {
  int i;
  int r = 0;
  for (i = 0; i < l1; i++) if (a1[2 * i + 1] >= day) {
    assert (r < l2);
    a2[2 * r + 0] = a1[2 * i + 0];
    a2[2 * r + 1] = a1[2 * i + 1];
    r ++;
  }
  return r;
}

int get_visitors_in_range (int *a, int l, int min, int max) {
  int i, r = 0;
  for (i = 0; i < l; i++) {
    if (max + 1 < a[2 * i + 1]) {
      fprintf (stderr, "max = %d, cur = %d\n", max, a[2 * i + 1]);
    }
    assert (a[2 * i + 1] <= max + 1);
    if (a[2 * i + 1] >= min /*&& a[2 * i + 1] <= max */) {
      r ++;
    }
  }
  return r;
}

void update_monthly_stats (int **a1, int *l1, int **a2, int *l2, int *len, struct counter *C, int last_index_day) {
  if (!C) {
    return;
  }
  assert (*len <= *l1);
  update_monthly_stats (a1, l1, a2, l2, len, C->prev, last_index_day);
  int cur_day = get_day (C->created_at);
  vkprintf (1, "update_monthly_stats: counter_id = %lld, version = %d, cur_day = %d, last_index_day = %d\n", C->counter_id, C->created_at, cur_day, last_index_day);
  if (cur_day < last_index_day) {
    return;
  }
  vkprintf (1, "cur = %d, range: min = %d, max = %d, prev = %d, id = %lld, m1 = %d, m2 = %d, monthly = %d\n", cur_day, get_prev_month_start (cur_day), get_prev_month_end (cur_day), C->prev ? get_day (C->prev->created_at) : 0, C->counter_id, get_month (C->created_at),C->prev ? get_month (C->prev->created_at) : 0, C->type & COUNTER_TYPE_MONTH);
  if ((C->type & COUNTER_TYPE_MONTH) && (C->last_month_unique_visitors == -1)) {
    C->last_month_unique_visitors = get_visitors_in_range (*a1, *len, get_prev_month_start (cur_day), get_prev_month_end (cur_day));
    vkprintf (2, "cur = %d, range: min = %d, max = %d, result = %d\n", cur_day, get_prev_month_start (cur_day), get_prev_month_end (cur_day), C->last_month_unique_visitors);
  } else {
    if (!(C->type & COUNTER_TYPE_MONTH)) {
      C->last_month_unique_visitors = get_visitors_in_range (*a1, *len, get_cur_month_start (cur_day), cur_day);
    }
  }
  if (*len + C->unique_visitors > *l2) {
    zzfree (*a2, *l2 * 2 * sizeof (int));
    *l2 = (*len + C->unique_visitors);
    *a2 = zzmalloc (*l2 * 2 * sizeof (int));
  }
  int i;
  for (i = 0; i < (*len) - 1; i++) {
    assert ((*a1)[2 * i] < (*a1)[2 * i + 2]);
    assert ((*a1)[2 * i + 1] <= cur_day);
  }
  int x = *len;
  int *a1_save = *a1;
  *len = tree_merge_arr (C->visitors, *a2, *l2, &a1_save, &x, cur_day);
  for (i = 0; i < (*len) - 1; i++) {
    assert ((*a2)[2 * i] < (*a2)[2 * i + 2]);
    assert ((*a2)[2 * i + 1] <= cur_day);
  }
  if (*len > *l1) {
    zzfree (*a1, *l1 * 2 * sizeof (int));
    *l1 = *len;
    *a1 = zzmalloc (*l1 * 2 * sizeof (int));
  }
  *len = monthly_delete_old (*a2, *len, *a1, *l1, cur_day - MONTHLY_STAT_LEN);
  for (i = 0; i < (*len) - 1; i++) {
    assert ((*a1)[2 * i] < (*a1)[2 * i + 2]);
    assert ((*a1)[2 * i + 1] <= cur_day);
  }
  //C->long_unique_visitors = *len;
  C->long_unique_visitors = get_visitors_in_range (*a1, *len, cur_day - 29, cur_day);
  C->last_week_unique_visitors = get_visitors_in_range (*a1, *len, cur_day - 6, cur_day);
  vkprintf (2, "update_monthly_stats: counter_id = %lld, version = %d, cur_day = %d, value = %d, tree_size = %d, tree = %p\n", C->counter_id, C->created_at, cur_day, *len, C->unique_visitors, C->visitors);
}

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned int jump_log_crc32;
extern long long snapshot_size;

int save_index (void) {
  if (verbosity >= 4) {
    fprintf (stderr, "Yes, saving index.\n");
  }

  int i;
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (log_cur_pos() == jump_log_pos) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
       newidxname, jump_log_pos);
    return 0;
  } 

  if (verbosity > 0) {
    fprintf (stderr, "creating index %s at log position %lld\n", newidxname, log_cur_pos());
  }

  newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  struct index_header_v2 header;
  memset (&header, 0, sizeof (struct index_header_v2));

  header.magic = STATSX_INDEX_MAGIC_V4 + custom_version_names;
  header.created_at = time (NULL);
  header.log_pos1 = log_cur_pos ();
  header.log_timestamp = log_read_until;
  relax_log_crc32 (0);
  header.log_pos1_crc32 = ~log_crc32_complement;
  header.last_incr_version = last_incr_version;

  int count = 0;

  if (verbosity >= 2) {
    fprintf (stderr, "preparing index header\n");
  }
  const int plen = tot_counters * sizeof (int);
  int *p = zzmalloc (plen);
  for (i = 0; i < counters_prime; i++) {
    if (!Counters[i]) { continue; }
    assert (count < tot_counters);
    p[count++] = i;
  }
  long long *tmp_shifts = zzmalloc (sizeof (long long) * (count + 1));
  long long *tmp_shifts_head = zzmalloc (sizeof (long long) * (count + 1));
  long long *tmp_shifts_monthly = zzmalloc0 (sizeof (long long) * (count + 1));
  long long *tmp_cnt = zzmalloc (sizeof (long long) * (count + 1));
  header.nrecords = count;

  if (verbosity >= 2) {
    fprintf (stderr, "sorting Counters\n");
  }
  qsort (p, count, sizeof(int), cmp_cnt);

  if (verbosity >= 2) {
    fprintf (stderr, "sorting Counters done\n");
  }

  assert (lseek (newidx_fd, sizeof (struct index_header_v2), SEEK_SET) == sizeof (struct index_header_v2));
  long long shift = sizeof (struct index_header_v2);
  int max_timestamp = 0;
  int p1 = 0;
  while (p1 < count) {
    if (p1 == 0 || Counters[p[p1]]->counter_id != Counters[p[p1-1]]->counter_id) {
      struct counter *C = Counters[p[p1]];
      assert (C);
      if (C->created_at > max_timestamp) {
        max_timestamp = C->created_at;
      }
    } else {
      assert (0);
    }
    p1++;
  } 
  p1 = 0;
  if (monthly_stat) {
    int tmp_monthly_len = 1000000;
    int *tmp_monthly = zzmalloc (2 * sizeof (int) * tmp_monthly_len);
    int tmp_monthly_len2 = 1000000;
    int *tmp_monthly2 = zzmalloc (2 * sizeof (int) * tmp_monthly_len);
    int max_day = get_day (max_timestamp);
    int max_month = get_month (max_timestamp);
    p1 = 0;
    while (p1 < count) {
      if (p1 == 0 || Counters[p[p1]]->counter_id != Counters[p[p1-1]]->counter_id) {
     
        struct counter *C = Counters[p[p1]];
        assert (C);
        if (get_month (C->created_at) != max_month) {
          incr_version = max_timestamp;
          incr_version_read = 1;
          C = get_counter_f (C->counter_id, 1);
          assert (get_month (C->created_at) == max_month);
        }
        if (C->prev && get_month (C->created_at) != get_month (C->prev->created_at)) {
          C->type |=  COUNTER_TYPE_MONTH;
        }
        int i = index_get_idx (C->counter_id);
        int len;
        if (i == index_size) {
          len = 0;
        } else {
          len = (index_monthly_offset[i + 1] - index_monthly_offset[i]);
          assert (len >= 0);
          assert (len % (2 * sizeof (int)) == 0);
          len /= (2 * sizeof (int));
          //fprintf (stderr, "%d\n", len);
          if (len > tmp_monthly_len) {
            zzfree (tmp_monthly, tmp_monthly_len * 2 * sizeof (int));
            tmp_monthly_len = len;
            tmp_monthly = zzmalloc ( tmp_monthly_len * 2 * sizeof (int));
          }
          assert (lseek (idx_fd, index_monthly_offset[i], SEEK_SET) == index_monthly_offset[i]);
          assert (read (idx_fd, tmp_monthly, 2 * sizeof (int) * len) == 2 * sizeof (int) * len);
          int j;
          for (j = 0; j < len - 1; j++) {
            assert (tmp_monthly[2 * j] < tmp_monthly[2 * j + 2]);
          }
          if (index_fix_mode) {
            int x = 0;
            for (j = 0; j < len; j ++) if (tmp_monthly[2 * j + 1] <= max_day) {
              tmp_monthly[2 * x] = tmp_monthly[2 * j];
              tmp_monthly[2 * x + 1] = tmp_monthly[2 * j + 1];
              x ++;
            }
            len = x;
          }
        }
        //while (C && get_day (C->created_at) >= max_day) {
        //  C = C->prev;
        //}
        update_monthly_stats (&tmp_monthly, &tmp_monthly_len, &tmp_monthly2, &tmp_monthly_len2, &len, C, last_index_day);
        //fprintf (stderr, "\n");
        assert (write (newidx_fd, tmp_monthly, 2 * sizeof (int) * len) == 2 * sizeof (int) * len);
        int j;
        for (j = 0; j < len - 1; j++) {
          assert (tmp_monthly[2 * j] < tmp_monthly[2 * j + 2]);
        }
        tmp_shifts_monthly[p1] = shift;
        shift += 2 * sizeof (int) * len;
      }
      p1 ++;
    }
    last_index_day = max_day;
    tmp_shifts_monthly[p1 ++] = shift;
  }
  int total_elem = 0;
  int total = 0;
  p1 = 0;
  while (p1 < count) {
    if (p1 == 0 || Counters[p[p1]]->counter_id != Counters[p[p1-1]]->counter_id) {
      //struct counter *C = get_counter_old (Counters[p[p1]]->counter_id, 0, 0);
      struct counter *C = Counters[p[p1]];
      assert (C);
      tmp_shifts_head[total_elem] = shift;
      tmp_cnt[total_elem] = C->counter_id;
      total_elem++;
      if (obsolete_timestamp (C->created_at, max_timestamp)) {
        C->visitors = 0;
      }
      shift += write_counter (C);
      total ++;
    } 
    p1++;
  }
  tmp_shifts_head[total_elem++] = shift;
  p1 = 0;
  total_elem = 0;
  while (p1 < count) {
    if (p1 == 0 || Counters[p[p1]]->counter_id != Counters[p[p1-1]]->counter_id) {
      struct counter *C = get_counter_old (Counters[p[p1]]->counter_id, 0, 0);
      tmp_shifts[total_elem] = shift;
      total_elem++;
      C = C->prev;
      while (C) {
        assert (total_elem <= tot_counter_instances);
        //if (verbosity >= 2) {
        //  fprintf (stderr, "%d\t", C->created_at);
        //}
        shift += write_counter (C);
        C = C->prev;
        total ++;
      }
      if (verbosity >= 1 && !(total_elem & ((1 << 10) - 1))) {
        fprintf (stderr, "written %d counters (%lld memory)\n", total_elem, shift);
      }
    } 
    while (free_LRU()) {}
    p1++;
  }
  header.shifts_offset = shift;
  tmp_shifts[total_elem++] = shift;
  total_elem --;
  header.nrecords = total_elem;
  header.total_elements = total;
  header.last_index_day = last_index_day;

  if (verbosity >= 2) {
    fprintf (stderr, "writing offsets\n");
  }
  
  writeout (tmp_shifts, (total_elem + 1)* sizeof (long long));
  writeout (tmp_cnt, (total_elem + 1) * sizeof (long long));
  writeout (tmp_shifts_head, (total_elem + 1) * sizeof (long long));
  writeout (tmp_shifts_monthly, (total_elem + 1) * sizeof (long long));

  flushout ();
  zzfree (p, plen);

  if (verbosity >= 3) {
    fprintf (stderr, "writing header\n");
  }


  assert (lseek (newidx_fd, 0, SEEK_SET) == 0);
  writeout (&header, sizeof (struct index_header_v2));
  flushout ();

  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);

  if (verbosity >= 3) {
    fprintf (stderr, "writing index done\n");
  }

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);

  return 0;
}


int save_index_reverse (void) {
  assert (0);
/*  if (verbosity >= 4) {
    fprintf (stderr, "No, saving index in reverse mode.\n");
  }
  long long *tmp_shifts = zzmalloc (sizeof (long long) * (tot_counter_instances + 1));
  int *tmp_cnt = zzmalloc (sizeof (int) * (tot_counter_instances + 1));
  int *tmp_cnt_ver = zzmalloc (sizeof (int) * (tot_counter_instances + 1));

  int i;
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (log_cur_pos() == jump_log_pos) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
       newidxname, jump_log_pos);
    return 0;
  } 

  if (verbosity > 0) {
    fprintf (stderr, "creating index %s at log position %lld\n", newidxname, log_cur_pos());
  }

  newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  struct index_header_v1 header;
  memset (&header, 0, sizeof (struct index_header_v1));

  header.magic = STATSX_INDEX_MAGIC + custom_version_names;
  header.created_at = time (NULL);
  header.log_pos1 = log_cur_pos ();
  header.log_timestamp = log_read_until;
  relax_log_crc32 (0);
  header.log_pos1_crc32 = ~log_crc32_complement;

  int count = 0;

  if (verbosity >= 2) {
    fprintf (stderr, "preparing index header\n");
  }

  for(i = 0; i<COUNTERS_PRIME; i++) {
    if (!Counters[i]) { continue; }
    p[count++] = i;
  }

  if (verbosity >= 2) {
    fprintf (stderr, "sorting Counters\n");
  }
  qsort (p, count, sizeof(int), cmp_cnt);

  if (verbosity >= 2) {
    fprintf (stderr, "sorting Counters done\n");
  }

  assert (lseek (newidx_fd, sizeof (struct index_header), SEEK_SET) == sizeof (struct index_header));
  int p1 = 0;
  int total_elem = 0;
  long long shift = sizeof (struct index_header);
  while (p1 < count) {
    if (p1 == 0 || Counters[p[p1]]->counter_id != Counters[p[p1-1]]->counter_id) {
      struct counter *C = get_counter_old (Counters[p[p1]]->counter_id, 0, 0);
      while (C) {
        if (C->created_at < timestamp_to_index) {
          assert (total_elem <= tot_counter_instances);
          tmp_shifts[total_elem] = shift;
          tmp_cnt[total_elem] = C->counter_id;
          tmp_cnt_ver[total_elem] = C->created_at;
          //if (verbosity >= 2) {
          //  fprintf (stderr, "%d\t", C->created_at);
          //}
          total_elem++;
          shift += write_counter (C);
          if (verbosity >= 1 && (total_elem & ((1 << 17) - 1)) == 0) {
            fprintf (stderr, "written %d counters (%lld memory)\n", total_elem, shift);
          }
        }
        C = C->prev;
      }
    } 
    while (tot_counters_allocated >= MAX_COUNTERS_ALLOCATED && free_LRU()) {
    }
    p1++;
  }
  header.shifts_offset = shift;
  header.total_elements = total_elem;

  if (verbosity >= 2) {
    fprintf (stderr, "writing offsets\n");
  }
  
  tmp_shifts[total_elem++] = shift;
  writeout (tmp_shifts, total_elem * sizeof (long long));
  writeout (tmp_cnt, total_elem * sizeof (int));
  writeout (tmp_cnt_ver, total_elem * sizeof (int));

  flushout ();

  if (verbosity >= 3) {
    fprintf (stderr, "writing header\n");
  }


  assert (lseek (newidx_fd, 0, SEEK_SET) == 0);
  writeout (&header, sizeof (struct index_header) );;
  flushout ();

  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);

  if (verbosity >= 3) {
    fprintf (stderr, "writing index done\n");
  }

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);*/

  return 0;
}


static long long blocking_read_bytes = 0;
static int blocking_read_calls = 0;
double snapshot_loading_average_blocking_read_bytes;
int snapshot_loading_blocking_read_calls;

int get_index_version (int magic) {
  if (magic == STATSX_INDEX_MAGIC_OLD) {
    if (magic - STATSX_INDEX_MAGIC_OLD != custom_version_names) {
      vkprintf (0, "index file key [-x] is not as in index\n");
      return -1;
    }
    return index_version = 0;
  }
  if (magic == STATSX_INDEX_MAGIC_V1 || magic == STATSX_INDEX_MAGIC_V1 + 1) {
    if (magic - STATSX_INDEX_MAGIC_V1 != custom_version_names) {
      vkprintf (0, "index file key [-x] is not as in index\n");
      return -1;
    }
    return index_version = 1;
  }
  if (magic == STATSX_INDEX_MAGIC_V2 || magic == STATSX_INDEX_MAGIC_V2 + 1) {
    if (magic - STATSX_INDEX_MAGIC_V2 != custom_version_names) {
      vkprintf (0, "index file key [-x] is not as in index\n");
      return -1;
    }
    return index_version = 2;
  }
  if (magic == STATSX_INDEX_MAGIC_V3 || magic == STATSX_INDEX_MAGIC_V3 + 1) {
    if (magic - STATSX_INDEX_MAGIC_V3 != custom_version_names) {
      vkprintf (0, "index file key [-x] is not as in index\n");
      return -1;
    }
    return index_version = 3;
  }
  if (magic == STATSX_INDEX_MAGIC_V4 || magic == STATSX_INDEX_MAGIC_V4 + 1) {
    if (magic - STATSX_INDEX_MAGIC_V4 != custom_version_names) {
      vkprintf (0, "index file key [-x] is not as in index\n");
      return -1;
    }
    return index_version = 4;
  }
  vkprintf (0, "Unknown index magic %x\n", magic);
  return -1;
}

int check_index_type (int idx_fd) {
  lseek (idx_fd, 0, SEEK_SET);
  int magic;
  read (idx_fd, &magic, sizeof (int));
  if (get_index_version (magic) < 0) {
    return -1;
  }
  lseek (idx_fd, 0, SEEK_SET);
  return 0;
}

int header_size (void) {
  if (index_version == 1 || index_version == 0) {
    return sizeof (struct index_header_v1);
  } else if (index_version == 2) {
    return sizeof (struct index_header_v2);
  } else if (index_version == 3) {
    return sizeof (struct index_header_v2);
  } else if (index_version == 4) {
    return sizeof (struct index_header_v2);
  } else {
    assert (0);
    return 0;
  }
}


int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    index_size = 0;
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    snapshot_size = 0;
    return 0;
  }
  idx_fd = Index->fd;
  if (check_index_type (idx_fd) < 0) {
    return -1;
  }
  vkprintf (1, "index_version = %d\n", index_version);
  
  struct index_header_v2 header;
  read (idx_fd, &header, header_size ());

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;
  lseek (idx_fd, header.shifts_offset, SEEK_SET);

  index_size = header.nrecords;
 
  index_offset = zzmalloc (sizeof (long long) * (header.nrecords + 1));
  index_cnt = zzmalloc (sizeof (long long) * (header.nrecords + 1));
  //index_cnt_ver = zzmalloc (sizeof (int) * (header.nrecords + 1));
  index_deleted = zzmalloc0 (sizeof (int) * (header.nrecords + 1));
  index_monthly_offset = zzmalloc0 (sizeof (long long) * (header.nrecords + 1));

  assert (read (idx_fd, index_offset, sizeof (long long) * (header.nrecords + 1)) == sizeof (long long) * (header.nrecords + 1));
  if (index_version <= 3 || (index_fix_mode && index_version == 4)) {
    assert (read (idx_fd, index_cnt, sizeof (int) * (header.nrecords + 1)) == sizeof (int) * (header.nrecords + 1));
    int i;
    for (i = header.nrecords; i >= 0; i--) {
      index_cnt[i] = ((int *)index_cnt)[i];
    }
  } else {
    assert (read (idx_fd, index_cnt, sizeof (long long) * (header.nrecords + 1)) == sizeof (long long) * (header.nrecords + 1));
  }
  //read (idx_fd, index_cnt_ver, sizeof (int) * (header.nrecords + 1));
  if (index_version <= 1) {
    lseek (idx_fd, sizeof (int) * (header.nrecords + 1), SEEK_CUR);
  } else {
    index_offset_head = zzmalloc (sizeof (long long) * (header.nrecords + 1));
    assert (read (idx_fd, index_offset_head, sizeof (long long) * (header.nrecords + 1)) == sizeof (long long) * (header.nrecords + 1));
    assert (read  (idx_fd, index_monthly_offset, sizeof (long long) * (header.nrecords + 1)) == sizeof (long long) * (header.nrecords + 1));
  }
  snapshot_size = -1;
  int x = header.nrecords + header.nrecords * (max_counters_growth_percent * 0.01);
  if (max_counters < x) {
    max_counters = x;
  }
  init_stats_data (STATS_SCHEMA_V1);

  index_size = header.nrecords;
  if (verbosity >= 1) {
    fprintf (stderr, "%d records in index file\n", index_size);
  }
  if (index_version <= 1) {
    tot_counter_instances = index_size;
  } else {
    tot_counter_instances = header.total_elements;
  }

  blocking_read_bytes = 0;
  blocking_read_calls = 0;
  int i = 0;
  for (i = 0; i < index_size; i++) {
    if (i == 0 || index_cnt[i] != index_cnt[i-1]) {
      load_counter (index_cnt[i], -1, 0);
    } else {
      assert (index_version <= 1);
    }
  }

  if (blocking_read_calls) {
    snapshot_loading_average_blocking_read_bytes = blocking_read_bytes;
    snapshot_loading_average_blocking_read_bytes /= blocking_read_calls;
  } else {
    snapshot_loading_average_blocking_read_bytes = 0.0;
  }
  snapshot_loading_blocking_read_calls = blocking_read_calls;

  if (index_version >= 2) {
    last_index_day = header.last_index_day;
    last_incr_version = header.last_incr_version;
    set_incr_version (-1, last_incr_version);
  }
  return 0;
}


/*
 *
 *         AIO
 *
 */
//struct aio_connection *WaitAio;
int onload_metafile (struct connection *c, int read_bytes);

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_metafile
};

conn_query_type_t aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "statsx-aio-metafile-query",
  .wakeup = aio_query_timeout,
  .close = delete_aio_query,
  .complete = delete_aio_query
};

int onload_counter (char *data, int data_len, long long cnt, int startup) {
  vkprintf (2, "onload_counter: cnt = %lld, data_len = %d\n", cnt, data_len);
  readin_reset (data, data_len);
  if (startup) {
    struct counter *C = get_counter_f (cnt, 3);
    if (!C) {
      fprintf (stderr, "Not enough memory\n");
      return -1;
    }
    struct counter *D = read_counter(1);
    memcpy (C, D, sizeof (struct counter));
    zzfree (D, sizeof (struct counter));
    tot_counters_allocated--;
    return 1;
  }
  int l = index_get_idx (cnt);
  int r = l;
  if (l == index_size) return -1;
  while (r < index_size && index_cnt[r] == cnt) {
    r++;
  }
  struct counter *C = get_counter_f (cnt, 0);
  if (!C) {
    vkprintf (0, "Skipping result for aio query for deleted user.\n");
    return 1;
  }
  assert (C);
  while (C->prev) {
    C = C->prev;
  }
  int l0 = l;
  assert (C->prev == 0);
  while (rptr < rBuff + index_offset[r] - index_offset[l0]) {
    struct counter *D = read_counter (0);
    if (D->created_at < C->created_at) {
      assert (C->prev == 0);
      if (verbosity >= 4) {
        fprintf (stderr, "reading counter\n");
      }
      assert (C);
      C->prev = D;
      if (verbosity >= 5) {
        fprintf (stderr, "C = %p\n", C);
        fprintf (stderr, "C->prev = %p\n", C->prev);
        if (C->prev->prev != 0) {
          fprintf (stderr, "C->prev->prev = %p\n", C->prev->prev);
        }
      }
      assert (C->prev->prev == 0);
      if (verbosity >= 4) {
        fprintf (stderr, "read counter\n");
      }
      C = C->prev;
      C->type |= COUNTER_TYPE_DELETABLE;
      add_use (C);
      assert (rptr - rBuff <= index_offset[r] - index_offset[l0]);
    } else {
      free_counter (D, 0);
    }
    l++;
  }
  assert (rptr - rBuff == index_offset[r] - index_offset[l0]);
  C->type |= COUNTER_TYPE_LAST;
  return 1;
}


int load_counter (long long cnt, int startup, int use_aio) {
  if ((verbosity >= 2 && !startup) || verbosity >= 4) {
    fprintf (stderr, "load_counter (%lld %d %d)\n", cnt, startup, use_aio);
  }
  struct counter *C = get_counter_f (cnt, 0);
  while (C && C->prev) {
    C = C->prev;
  }
  if (C && (C->type & COUNTER_TYPE_LAST)) {
    return 1;
  }
  int l =  index_get_idx (cnt);
  if (l == index_size) return -1;
  int r = l;
  while (r < index_size && index_cnt[r] == cnt) {
    r++;
  }
  assert (r > l);
  if (verbosity >= 4) {
    fprintf (stderr, "found entries in index file.\n");
  }

  int aio = USE_AIO && !startup && use_aio; 


  if (aio) {
    WaitAioArrClear ();
    //WaitAio = NULL;

    C = get_counter_f (cnt, 0);
    assert (C);

    struct metafile *meta = C->meta;

    if (!meta) {
      meta = zzmalloc0 ( sizeof (struct metafile));
      C->meta = meta;
    }

    if (verbosity >= 3) {
      fprintf (stderr, "loading metafile %lld in aio mode\n", cnt);
    }

    if (meta->aio != NULL) {
      
      if (verbosity >= 2) {
        fprintf (stderr, "Dublicate AIO request");
      }

      check_aio_completion (meta->aio);

      if (meta->aio != NULL) {
        //WaitAio = meta->aio;
        WaitAioArrAdd (meta->aio);
        return -2;
      }
       
      assert (!meta->aio);
      assert (!meta->data);
    
      if (!(meta->flags & 1) ) {
        if (verbosity >= 2) {
          fprintf (stderr, "Previous AIO request was successfull");
        }
        return 1;
      } else {
        fprintf (stderr, "Previous AIO query failed at %p, scheduling a new one\n", meta);
      }
    } else {
      if (verbosity >= 4) {
        fprintf (stderr, "No previous aio found for this metafile\n");
      }
    }


    while (1) {
      meta->data = zzmalloc (index_offset[r] - index_offset[l]);
      if (meta->data) {
        meta->size = index_offset[r] - index_offset[l];
        break;
      }
      fprintf (stderr, "No space for allocating %lld data.\n", index_offset[r] - index_offset[l]);
      return -1;
    }
  
    if (verbosity >= 4) {
      fprintf (stderr, "AIO query creating...\n");
    }
    meta->cnt = cnt;
    if (verbosity >= 3) {
      fprintf (stderr, "Scheduled for reading %lld bytes.\n", meta->size);
    }
    meta->aio = create_aio_read_connection (idx_fd, meta->data, index_offset[l], meta->size, &ct_metafile_aio, meta);
    tot_aio_queries ++;
    if (verbosity >= 4) {
      fprintf (stderr, "AIO query created\n");
    }
    assert (meta->aio != NULL);
    //WaitAio = meta->aio;
    WaitAioArrAdd (meta->aio);

    return -2;
  } else {
    if (index_version <= 1 || startup != -1) {
      assert (lseek (idx_fd, index_offset[l], SEEK_SET) == index_offset[l]); 
    } else {
      assert (r == l + 1);
      assert (lseek (idx_fd, index_offset_head[l], SEEK_SET) == index_offset_head[l]); 
    }
    char *tmp = 0;
    long long size;
    if (index_version <= 1 || startup != -1) {
      size = index_offset[r] - index_offset[l];
    } else {
      size = index_offset_head[r] - index_offset_head[l];
    }
    if (size > BUFFSIZE) {
      tmp = zzmalloc (size);
      if (!tmp) {
        fprintf (stderr, "Not enough memory to allocate read buffer. Skipping request.\n");
        return -1;
      }
    } else {
      tmp = rBuff_static;
    }
    if ((!startup && verbosity >= 3) || verbosity >= 4) {
      fprintf (stderr, "reading data... offset = %lld, size = %lld, last = %lld\n", index_offset_head[l], size, index_offset_head[r]);
      fprintf (stderr, "l = %d, r = %d, index_size = %d\n", l, r, index_size);
    }
    assert (read (idx_fd, tmp, size) == size);    
    if (!startup && verbosity >= 3) {
      fprintf (stderr, "read data...!\n");
    }
    blocking_read_bytes += size;
    blocking_read_calls++;

    onload_counter (tmp, size, cnt, startup);

    if (!startup && verbosity >= 3) {
      fprintf (stderr, "Onload passed\n");
    }

    if (size > BUFFSIZE) {
      zzfree (tmp, size);
    }
    return 1; 
  }
}

int onload_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 2) {
    fprintf (stderr, "onload_metafile(%p,%d)\n", c, read_bytes);
  }

  struct aio_connection *a = (struct aio_connection *)c;
  struct metafile *meta = (struct metafile *) a->extra;

  assert (a->basic_type == ct_aio);
  assert (meta != NULL);          

  if (meta->aio != a) {
    fprintf (stderr, "assertion (meta->aio == a) will fail\n");
    fprintf (stderr, "%p != %p\n", meta->aio, a);
  }

  assert (meta->aio == a);

  if (read_bytes != meta->size) {
    if (verbosity >= 0) {
      fprintf (stderr, "ERROR reading metafile (counter %lld): read %d bytes out of %lld: %m\n", meta->cnt, read_bytes, meta->size);
    }
    zzfree (meta->data, meta->size);
    meta->data = 0;
    meta->aio = 0;
    tot_aio_fails ++;
    meta->flags |= 1;
    return 0;
  }
  assert(read_bytes == meta->size);

  tot_aio_loaded_bytes += read_bytes;



  if (verbosity > 2) {
    fprintf (stderr, "*** Read metafile: read %d bytes\n", read_bytes);
  }

  onload_counter (meta->data, meta->size, meta->cnt, 0);

  zzfree(meta->data, meta->size);
  meta->data = 0;

  meta->aio = NULL;
  meta->flags &= ~1;
  
  tot_user_metafile_bytes += meta->size;
  tot_user_metafiles++;
  
  return 1;
}
