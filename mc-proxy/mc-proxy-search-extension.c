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
              2009-2013 Nikolai Durov (original search-merge.c)
              2009-2013 Andrei Lopatin (original search-merge.c)
              2011-2013 Vitaliy Valtman
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>

#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "server-functions.h"
#include "estimate-split.h"
//#include "utils.h"
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "mc-proxy-search-extension.h"
#include "mc-proxy-merge-extension.h"
#include "kdb-data-common.h"

#define VERSION_STR "mc-proxy-search-0.1.01"

#define MAX_NET_RES (1L << 16)

//#define BUFF_SIZE 4096

//TODO: fix this constant
#define HINTS_MAX_CNT 1000

extern int verbosity;
extern int get_search_queries;
//int search_id = -239;
extern int get_targets;
extern struct conn_target *get_target[];
extern struct connection *get_connection[];

#define STATS_BUFF_SIZE (16 << 10)


#define GD_EXTRA  1
#define GD_DROP_EXTRA 2
#define GD_REVERSE  8
#define GD_RAW    4
#define GD_HASH 16
#define GD_ISSEARCH 32

/*
 * client
 */

/* -------- LIST GATHER/MERGE ------------- */

enum { g_id_asc = 1, g_id_desc = 9, g_rate_asc = 2, g_rate_desc = 10, g_desc = 8, g_double = 2, g_hash = 16 } GH_mode;

inline unsigned long long make_value64 (int value, int x) {
  unsigned int a = value; a -= (unsigned int) INT_MIN;
  unsigned int b = x; b -= (unsigned int) INT_MIN;
  return (((unsigned long long) a) << 32) | b;
}

typedef struct gather_heap_entry {
  unsigned long long value64;
  int *cur, *last;
  int remaining;
} gh_entry_t;

static gh_entry_t GH_E[MAX_CLUSTER_SERVERS];
static gh_entry_t *GH[MAX_CLUSTER_SERVERS+1];
static int GH_N, GH_total;

void clear_gather_heap (int mode) {
  if (mode < 0) { mode = 8 - mode; }
  GH_mode = mode;
  GH_N = 0;
  GH_total = 0;
}

static inline void load_heap_v (gh_entry_t *H) {
  int *data = H->cur;
  int x, value;
  if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
    x = (data[1] ? data[1] : data[0]);
    value = (GH_mode & g_double ? data[2] : x);
  } else if (TARG_EXTENSION) {
    x = data[0];
    value = (GH_mode & g_double ? data[1] : x);
  } else if (HINTS_MERGE_EXTENSION) {
    x = data[0];
    value = -data[2];
    //fprintf (stderr, "(%d ; %d)\n", value, x);
  } else {
    assert (0);
  }
  if (GH_mode & g_desc) {
    /* -(-2147483648) == -2147483648 */
    H->value64 = make_value64 (-(value+1),-(x+1));
  } else {
    H->value64 = make_value64 (value, x);
  }
}



static int gather_heap_insert (struct gather_entry *GE) {
  int cnt, cnt2, sz;
  gh_entry_t *H;
  assert (GH_N < MAX_CLUSTER_SERVERS);
  if (GE->num <= 0 || GE->res_read < 8 || !GE->data) {
    return 0;
  }
  cnt2 = cnt = (GE->res_read >> 2) - 1;
  if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
    sz = (GH_mode & g_double) ? 3 : 2;
  } else if (TARG_EXTENSION) {
    sz = (GH_mode & g_double) ? 2 : 1;
  } else if (HINTS_MERGE_EXTENSION) {
    sz = 3;
  } else {
    assert (0);
  }
  if (GH_mode & g_hash) { sz += 2; }
  cnt /= sz;
  cnt2 = cnt * sz;
  if (verbosity >= 3) {
    fprintf (stderr, "gather_heap_insert: %d elements (size %d)\n", cnt, (GE->res_read >> 2) - 1);
  }
  if (cnt <= 0) { 
    return 0;
  }
  GH_total += cnt;

  H = &GH_E[GH_N];
  H->remaining = cnt;
  H->cur = GE->data;
  H->last = GE->data + GE->res_bytes / 4 + 1;
  load_heap_v (H);

  int i = ++GH_N, j;
  unsigned long long value64 = H->value64;
  while (i > 1) {
    j = (i >> 1);
    if (GH[j]->value64 <= value64) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;

  return 1;
}

static int *get_gather_heap_head (void) {
	if (verbosity >= 4) {
		fprintf (stderr, "get_gather_heap_head: GH->last = %p (value64 = %016llx)\n", GH[1]->last, GH[1]->value64);
	}
  return GH_N ? GH[1]->cur : 0;
}

static void gather_heap_advance (void) {
  gh_entry_t *H;
  int sz;
  if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
    sz = (GH_mode & g_double) ? 3 : 2;
  } else if (TARG_EXTENSION) {
    sz = (GH_mode & g_double) ? 2 : 1;
  } else if (HINTS_MERGE_EXTENSION) {
    sz = 3;
  } else {
    assert (0);
  }
  if (GH_mode & g_hash) { sz += 2; }
  if (!GH_N) { return; }
  H = GH[1];
  H->cur += sz;
  if (!--H->remaining) {
    H = GH[GH_N--];
    if (!GH_N) { return; }
  } else {
    if (H->cur >= H->last) {
      assert (0);
    }
    load_heap_v (H);
  }
  int i = 1, j;
  unsigned long long value64 = H->value64;
  while (1) {
    j = i*2;
    if (j > GH_N) { break; }
    if (j < GH_N && GH[j+1]->value64 < GH[j]->value64) { j++; }
    if (value64 <= GH[j]->value64) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;
}

#define MAX_RES	10000
static int R[MAX_RES];
static int R_cnt;

static void gather_sum_res (struct gather_entry *GE, int limit) {
  int cnt = (GE->res_read >> 2) - 1;
  int *ptr = GE->data;
  int i;

  if (cnt > limit) { cnt = limit; }
  for (i = 0; i < cnt; i++) {
    R[i] += *ptr++;
  }
}


/* -------- END (LIST GATHER/MERGE) ------- */

/****************** hashset ***************************/

/* returns prime number which greater than 1.5n and not greater than 1.1 * 1.5 * n */
static int get_hashtable_size (int n) {
  static const int p[] = {1103,1217,1361,1499,1657,1823,2011,2213,2437,2683,2953,3251,3581,3943,4339,
  4783,5273,5801,6389,7039,7753,8537,9391,10331,11369,12511,13763,15149,16381};
  /*
  18341,20177,22229,
  24469,26921,29629,32603,35869,39461,43411,47777,52561,57829,63617,69991,76991,84691,93169,102497,
  112757,124067,136481,150131,165161,181693,199873,219871,241861,266051,292661,321947,354143, 389561, 428531,
  471389,518533,570389,627433,690187,759223,835207,918733,1010617,1111687,1222889,1345207,
  1479733,1627723,1790501,1969567,2166529,2383219,2621551,2883733,3172123,3489347,3838283,4222117,
  4644329,5108767,5619667,6181639,6799811,7479803,8227787,9050599,9955697,10951273,12046403,13251047,
  14576161,16033799,17637203,19400929,21341053,23475161,25822679,28404989,31245491,34370053,37807061,
  41587807,45746593,50321261,55353391,60888739,66977621,73675391,81042947,89147249,98061979,107868203,
  118655027,130520531,143572609,157929907,173722907,191095213,210204763,231225257,254347801,279782593,
  307760897,338536987,372390691,409629809,450592801,495652109,545217341,599739083,659713007,725684317,
  798252779,878078057,965885863,1062474559};
  */
  const int lp = sizeof (p) / sizeof (p[0]);
  int a = -1;
  int b = lp;
  n += n >> 1;
  while (b - a > 1) {
    int c = ((a + b) >> 1);
    if (p[c] <= n) { a = c; } else { b = c; }
  }
  if (a < 0) { a++; }
  assert (a < lp-1);
  return p[a];
}

static long long H[16384];
static int HSIZE;

static void hashset_init (int n) {
  HSIZE = get_hashtable_size (n);
  memset (H, 0, sizeof (H[0]) * HSIZE);
}

static int hashset_insert (long long id) {
  /* empty hash always stored */
  if (!id) { return 1; }
  int h1, h2;
  long long D;
  h1 = ((unsigned int) id) % HSIZE;
  h2 = 1 + ((unsigned int) (id >> 32)) % (HSIZE - 1);
  while ((D = H[h1]) != 0) {
    if (D == id) {
      return 0;
    }
    h1 += h2;
    if (h1 >= HSIZE) { h1 -= HSIZE; }
  }
  H[h1] = id;
  return 1;
}

/*********** end hashset *********/

#define SEARCH_MAGIC 0x12
#define SEARCHX_MAGIC 0x32
struct search_gather_extra {
  int magic;
  int flags;
  int slice_limit;
  int limit;
};

struct hints_gather_extra {
  int magic;
  int uid;
  int type;
  int num;
  int slice_limit;
  int limit;
  int need_rating;
  int need_latin;
};


#define MAX_RES 10000

static int Q_order, Q_limit, Q_slice_limit, Q_raw, is_search, targ_extra, Q_searchx_hash_off;
static const char *Qs, *Q_limit_ptr, *Q_limit_end;
static long double cum_hash_ratio = 0.0;
static long long tot_hash_ratio = 0;

static const char *parse_search_extras (const char *ptr);
static const char *searchx_parse_search_extras (const char *ptr);
static int generate_new_key (char *ptr, const char *key, int len, int pos);
static int prepare_search_query (const char *key, int len);

void *search_store_gather_extra (const char *key, int key_len) {
  int i = -1;

  Q_raw = 0, Q_limit = 0;

  i = prepare_search_query (key, key_len);
  if (i == -1) {
    return 0;
  }

  if (verbosity >= 2) {
    fprintf (stderr, "got: %s\n", key);
  }

  Qs = key + i; /* compile_query (key+7); */
  if (!Q_limit) {
    parse_search_extras (Qs);
  }


  Q_slice_limit = Q_limit;
  if (is_search) {
    if ((Q_order & 16) && tot_hash_ratio) {
      Q_slice_limit = estimate_split ((int) (Q_limit * (cum_hash_ratio / tot_hash_ratio)) , CC->tot_buckets);
      if (Q_slice_limit > Q_limit) { Q_slice_limit = Q_limit; }
    } else {
      Q_slice_limit = estimate_split (Q_limit, CC->tot_buckets);
    }
  }

  struct search_gather_extra *extra = zzmalloc (sizeof (struct search_gather_extra));
  extra->flags = (is_search ? GD_ISSEARCH : 0) + ((Q_order & 8) ? GD_EXTRA : 0) + ((Q_order & 4) ? GD_REVERSE : 0) + (Q_raw ? GD_RAW : 0) + ((Q_order & 2) ? GD_HASH : 0);
  extra->slice_limit = Q_slice_limit;
  extra->limit = Q_limit;
  extra->magic = SEARCH_MAGIC;
  return extra;
}

void *searchx_store_gather_extra (const char *key, int key_len) {
  Q_raw = 0, Q_limit = 0;

  if (verbosity >= 2) {
    fprintf (stderr, "got: %s\n", key);
  }

  if (key_len < 7 || (strncmp (key, "searchx", 7) && strncmp (key, "searchu", 7))) {
    return search_store_gather_extra (key, key_len);
  }

  searchx_parse_search_extras (key);

  if (Q_limit < 0) {
    return 0;
  }

  Q_slice_limit = Q_limit;
  if ((Q_order & 16) && tot_hash_ratio) {
    Q_slice_limit = estimate_split ((int) (Q_limit * (cum_hash_ratio / tot_hash_ratio)) , CC->tot_buckets);
    if (Q_slice_limit > Q_limit) { Q_slice_limit = Q_limit; }
  } else {
    Q_slice_limit = estimate_split (Q_limit, CC->tot_buckets);
  }

  struct search_gather_extra *extra = zzmalloc (sizeof (struct search_gather_extra));
  extra->flags = (is_search ? GD_ISSEARCH : 0) + ((Q_order & 8) ? GD_EXTRA : 0) + ((Q_order & 4) ? GD_REVERSE : 0) + (Q_raw ? GD_RAW : 0) + ((Q_order & 2) ? GD_HASH : 0);
  extra->slice_limit = Q_slice_limit;
  extra->limit = Q_limit;
  extra->magic = SEARCHX_MAGIC;
  return extra;
}

long long get_long (const char **s) {
  long long res = 0;
  while (**s <= '9' && **s >= '0') {
    res = res * 10 + **s - '0';
    (*s)++;
  }
  return res;
}

void *hints_merge_store_gather_extra (const char *key, int key_len) {
#define TRY(x) if (x) {return NULL;}
  //fprintf (stderr, "hints_merge_store_gather_extra %s\n", key);
  /*int l =  eat_at (key, key_len);
  key += l;
  key_len -= l;*/

  TRY (key[key_len - 1] != ')');
  const char *s = key + 12;

  int need_rating = 0;
  if (!strncmp (s, "_rating", 7)) {
    s += 7;
    need_rating = 1;
  }

  int need_latin = 0;
  if (!strncmp (s, "_latin", 6)) {
    s += 6;
    need_latin = 1;
  }

  int uid = get_long (&s);
  
  int res_cnt = HINTS_MAX_CNT;
  int res_type = -1;
  int res_num = 0;

  int was_type = 0, was_cnt = 0, was_num = 0;

  while (s[0] == '#' || s[0] == ',' || s[0] == '*') {
    if (s[0] == ',') {
      TRY (was_type);
      was_type = 1;
      s++;
      res_type = (int)get_long (&s);
    }
    if (s[0] == '#') {
      TRY (was_cnt);
      was_cnt = 1;
      s++;
      res_cnt = (int)get_long (&s);
    }
    if (s[0] == '*') {
      TRY (was_num);
      was_num = 1;
      s++;
      res_num = (int)get_long (&s);
    }
  }
  TRY (s[0] != '(');

  if (res_cnt < 0) {
    res_cnt = 0;
  }
  if (res_cnt > HINTS_MAX_CNT){
    res_cnt = HINTS_MAX_CNT;
  }

  struct hints_gather_extra *extra = zzmalloc (sizeof (struct hints_gather_extra));
  
  extra->uid = uid;
  extra->type = res_type;
  extra->num = res_num;
  extra->slice_limit = estimate_split (res_cnt, CC->tot_buckets);
  extra->limit = res_cnt;
  extra->need_rating = need_rating;
  extra->need_latin = need_latin;
  return extra;    
}


int search_free_gather_extra (void *E) {
  zzfree (E, sizeof (struct search_gather_extra));
  return 0;
}

int hints_merge_free_gather_extra (void *E) {
  zzfree (E, sizeof (struct hints_gather_extra));
  return 0;
}

int search_merge_generate_new_key (char *buff, char *key, int key_len, void *E) {
  int i = prepare_search_query (key, key_len);
  return generate_new_key (buff, key, key_len, i);
}

static int searchx_generate_new_key (char *buff, char *key, int key_len, void *E) {
  if (((struct search_gather_extra *)E)->magic == SEARCH_MAGIC) {
    return search_merge_generate_new_key (buff, key, key_len, E);
  }
  vkprintf (1, "searchx_generate_new_key\n");
  char *sbuff = buff;
  searchx_parse_search_extras (key);
  assert (Q_limit >= 0);  
  memcpy (buff, key, Q_limit_ptr - key);
  assert (buff[Q_searchx_hash_off] == '#');
  buff[Q_searchx_hash_off] = '%';
  buff += (Q_limit_ptr - key);
  buff += sprintf (buff, "%d", ((struct search_gather_extra *)E)->slice_limit);
  memcpy (buff, Q_limit_end, key_len - (Q_limit_end - key));
  buff += key_len - (Q_limit_end - key);

  *buff = 0;
  return buff - sbuff;
}

int hints_merge_generate_new_key (char *buff, char *key, int key_len, void *E) {
  assert (E != NULL);

  char *sbuff = buff;
  
  struct hints_gather_extra *ge = (struct hints_gather_extra *)E;
  
  buff += sprintf (buff, "gather_hints%s%d", ge->need_latin ? "_latin" : "", ge->uid);
  if (ge->type != -1) {
    buff += sprintf (buff, ",%d", ge->type);
  }
  if (ge->num != 0) {
    buff += sprintf (buff, "*%d", ge->num);
  }
  buff += sprintf (buff, "#%d", ge->slice_limit);
  
  while (*key != '(') {
    key++;
  }
  while (*key != ')') {
    *buff++ = *key++;
  }
  *buff++ = ')';

  *buff = 0;
  return buff - sbuff;
}

int search_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  struct search_gather_extra *extra = E;
  static char buff[2048];
  int w, i;
  int res;
  char *ptr, *size_ptr, *s;
  int len = key_len;

  Q_limit = extra->limit;
  Q_raw = ((extra->flags & GD_RAW) != 0);
  Q_order = (extra->flags & GD_EXTRA ? 2 : 1);

  if (extra->flags & GD_REVERSE) {
    Q_order += 8;
  }
  if (extra->flags & GD_DROP_EXTRA) {
    Q_order = 7;
  }
  
  if (extra->flags & GD_HASH) {
    Q_order |= 16;
  }

  if (verbosity >= 4) {
    fprintf (stderr, "Q_order = %d, Q_limit = %d\n", Q_order, Q_limit);
  }

  //q->extra = 0;
  //G->c = 0;
  //c->state &= ~C_INQUERY;
  //c->state |= C_WANTRW;
  int at_l = eat_at (key, len);

  int sum_mode = (key[at_l] == 'p' || !memcmp (key + at_l, "ad_ctr", 6) || !memcmp (key + at_l, "ad_sump", 7));
  if (verbosity >= 4) {
    fprintf (stderr, "sum_mode = %d\n", sum_mode);
  }
  if (sum_mode) {
    if (Q_limit <= 0) { Q_limit = 1000; }
    if (Q_limit > 10000) { Q_limit = 10000; } 
    memset (R, 0, Q_limit * sizeof(int));
    Q_order = 0;
    R_cnt = 0;
  } else {
    clear_gather_heap (Q_order);
  }

  res = 0;
  /* sum results */
  for (i = 0; i < tot_num; i++) if (data[i].num >= 0) {
    res += data[i].num;
    assert (data[i].num >= 0);
    if (verbosity >= 4) {
      fprintf (stderr, "Using result %d (num = %d)\n", i, data[i].num);
    }
    if (!sum_mode) {
      gather_heap_insert (&data[i]);
    } else {
      if (Q_limit > MAX_RES) { Q_limit = MAX_RES; }
      gather_sum_res (&data[i], Q_limit);
    }
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, data[i].num);
    }
  }

  if (verbosity > 0) {
    fprintf (stderr, "result = %d\n", res);
  }

  if (!Q_limit || !res) {
    w = sprintf (buff, "%d", res);
    return return_one_key (c, key, buff, w);
  }

  write_out (&c->Out, "VALUE ", 6);
  write_out (&c->Out, key, len);

  ptr = get_write_ptr (&c->Out, 512);
  if (!ptr) return -1;
  s = ptr + 448;

  memcpy (ptr, " 0 .........\r\n", 14);
  size_ptr = ptr + 3;
  ptr += 14;
  if (!Q_raw) {
    ptr += w = sprintf (ptr, "%d", res);
  } else {
    *((int *) ptr) = res;
    ptr += w = 4;
  }

  int have_extras = (((Q_order + 1) & 7) > 2);
  
  if (Q_order & 16) {
    hashset_init (Q_limit);
  }
  const int hash_offset = (GH_mode & g_double) ? 3 : 2;
  int tot_hash_dups = 0;

  char split;
  int id_bytes;
  if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
    split = '_';
    id_bytes = 8;
  } else if (TARG_EXTENSION) {
    split = ',';
    id_bytes = 4;
  } else {
    assert (0);
  }
  if (TARG_EXTENSION && key[at_l] == 'p') {
    while (Q_limit && !R[Q_limit-1]) { Q_limit--; }
  }
  for (i = 0; i < Q_limit; ) {
    int t, *Res = !sum_mode ? get_gather_heap_head () : R + i;
    if (!Res) { break; }
    long long hc; 
    if (Q_order & 16) {
      memcpy (&hc, &Res[hash_offset], 8);
      if (!hashset_insert (hc)) {
        /* skiping duplicate */
        tot_hash_dups++;
        gather_heap_advance ();
        continue;
      }
    }

    if (ptr >= s) {
      advance_write_ptr (&c->Out, ptr - (s - 448));
      ptr = get_write_ptr (&c->Out, 512);
      if (!ptr) return -1;
      s = ptr + 448;
    }
    
    if (!Q_raw) {
      *ptr++ = ',';  w++;
      if (!Res[1] || !(SEARCH_EXTENSION || SEARCHX_EXTENSION)) {
        w += t = sprintf (ptr, "%d", Res[0]);
      } else {
        w += t = sprintf (ptr, "%d%c%d", Res[0], split, Res[1]);
      }
      ptr += t;
      if (have_extras) {
        if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
          w += t = sprintf (ptr, ",%d", Res[2]);
        } else {
          assert (TARG_EXTENSION);
          w += t = sprintf (ptr, ",%d", Res[1]);
        }
        ptr += t;
      }
      if (Q_order & 16) {
        w += t = sprintf (ptr, ",%llx", hc);
        ptr += t;
      }
    } else {
      t = (have_extras ? id_bytes + 4 : id_bytes);
      if (Q_order & 16) { t += 8; }
      memcpy (ptr, Res, t);
      w += t;
      ptr += t;
    }
    gather_heap_advance ();
    i++;
  }
  
  if ((Q_order & 16) && i) {
    long double hash_ratio = ((long double) (i + tot_hash_dups)) / i;
    cum_hash_ratio += hash_ratio;
    tot_hash_ratio++;
  }
  

  size_ptr[sprintf (size_ptr, "% 9d", w)] = '\r';
  //memcpy (ptr, "\r\nEND\r\n", 7);
  //ptr += 7;
  memcpy (ptr, "\r\n", 2);
  ptr+=2;
  advance_write_ptr (&c->Out, ptr - (s - 448));

  return 0;
}

int hints_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  struct hints_gather_extra *extra = E;
  assert (E != 0);
  static char buff[2048];
  int w, i;
  int res = 0;
  char *ptr, *size_ptr, *s;
  int len = key_len;

  Q_limit = extra->limit;

  int at_l = eat_at (key, len);
  key += at_l;
  len -= at_l;

  clear_gather_heap (0);
  for (i = 0; i < tot_num; i++) if (data[i].num >= 0) {
    res += data[i].num;
    assert (data[i].num >= 0);
    if (verbosity >= 4) {
      fprintf (stderr, "Using result %d (num = %d)\n", i, data[i].num);
    }
    gather_heap_insert (&data[i]);
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, data[i].num);
    }
  }

  if (verbosity > 0) {
    fprintf (stderr, "result = %d\n", res);
  }

  if (!Q_limit || !res) {
    w = sprintf (buff, "%d", res);
    return return_one_key (c, key, buff, w);
  }

  write_out (&c->Out, "VALUE ", 6);
  write_out (&c->Out, key, len);

  ptr = get_write_ptr (&c->Out, 512);
  if (!ptr) return -1;
  s = ptr + 448;

  memcpy (ptr, " 0 .........\r\n", 14);
  size_ptr = ptr + 3;
  ptr += 14;
  ptr += w = sprintf (ptr, "%d", res);
  
  //char split;
  //int id_bytes;
  if (HINTS_MERGE_EXTENSION) {
  } else {
    assert (0);
  }

  for (i = 0; i < Q_limit; ) {
    int t, *Res = get_gather_heap_head ();
    if (!Res) { break; }
    
    if (ptr >= s) {
      advance_write_ptr (&c->Out, ptr - (s - 448));
      ptr = get_write_ptr (&c->Out, 512);
      if (!ptr) return -1;
      s = ptr + 448;
    }
    
    *ptr++ = ',';  w++;
    w += t = sprintf (ptr, "%d%c%d", Res[0], ',', Res[1]);
    ptr += t;
    if (extra->need_rating) {
      w += t = sprintf (ptr, "%c%.6lf", ',', (double)*(float *)&Res[2]);
      ptr += t;
    }

    gather_heap_advance ();
    i++;
  }
  
  size_ptr[sprintf (size_ptr, "% 9d", w)] = '\r';
  //memcpy (ptr, "\r\nEND\r\n", 7);
  //ptr += 7;
  memcpy (ptr, "\r\n", 2);
  ptr+=2;
  advance_write_ptr (&c->Out, ptr - (s - 448));

  return 0;
}

int search_check_query (int type, const char *key, int key_len) {
  if (type != mct_get) {
    return 0;
  }

  int l =  eat_at (key, key_len);
  key += l;
  key_len -= l;

  if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
    return (key_len >= 6 && !memcmp (key, "search", 6));
  } else {
    return (key_len >= 7 && !strncmp (key, "search(", 7)) || (key_len >= 6 && !strncmp (key, "target", 6))
          	|| (key_len >= 6 && !strncmp (key, "prices", 6)) ||  (key_len >= 3 && !strncmp (key, "ad_", 3));  }
}

int searchx_check_query (int type, const char *key, int key_len) {
  if (type != mct_get) {
    return 0;
  }

  int l =  eat_at (key, key_len);
  key += l;
  key_len -= l;

  assert (SEARCHX_EXTENSION);

  return (key_len >= 7 && !strncmp (key, "searchx", 7)) || (key_len >= 7 && !strncmp (key, "searchu", 7)) || search_check_query (type, key, key_len);
}

int hints_merge_check_query (int type, const char *key, int key_len) {
  if (type != mct_get) {
    return 0;
  }
  
  if (verbosity > 2) {
    fprintf (stderr, "hints_merge_check_query (key = %s)\n", key);
  }

  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  
  return (key_len > 12 && !strncmp (key, "gather_hints", 12));
}


static const char *search_parse_search_extras (const char *ptr) {
  Q_raw = 0;
  Q_order = 0;
  Q_limit = 0;
  Q_slice_limit = 0;

  Q_limit_ptr = Q_limit_end = 0;

  if (*ptr != '#') {
    return ptr;
  }
  ptr++;
  while (*ptr != '(' && *ptr != '[') {
    if (*ptr == '%') {
      Q_raw = 1;
    } else if (*ptr == 'X') { 
    } else if (*ptr == 'i') {
    	Q_order &= ~4; 
    } else if (*ptr == 'I') {
    	Q_order |= 4;
    } else if (*ptr == 'P' || *ptr == 'T') {
      Q_order |= 8;
    } else if (*ptr == 'U') {
      Q_order |= 2;
    } else if (*ptr == 'V') {
      //nothing to do
    } else if ('A' <= *ptr && *ptr <= 'Z') {
      Q_order |= 4;
      Q_order |= 8;
    } else if (('a' <= *ptr && *ptr <= 'z') || *ptr == '?') {
      Q_order &= ~4;
      Q_order |= 8;
    } else if ('0' <= *ptr && *ptr <= '9') {
      Q_limit_ptr = ptr;
      char *qtr;
      Q_limit = strtol (ptr, &qtr, 10);
      ptr = qtr;
      Q_limit_end = ptr;
      if (Q_limit < 0) { Q_limit = 0; }
      if (Q_limit > MAX_RES) { Q_limit = MAX_RES; }
      ptr--;
    } else {
      return ptr;
    }
    ptr++;
  }
  return ptr;
}

static const char *searchx_parse_search_extras (const char *key) {
  const char *ptr = key;
  Q_raw = 0;
  Q_order = 0;
  Q_limit = 0;
  Q_slice_limit = 0;
  Q_limit_ptr = Q_limit_end = 0;

  ptr += 6;
  assert (*ptr == 'x' || *ptr == 'u');
  if (*ptr == 'u') {
    Q_order |= 2;
    ptr++;
    if ('a' <= *ptr && *ptr <= 'z') {
      ptr++;
    }
  } else {
    ptr++;
  }

  if (*ptr != '#' && *ptr != '%') {
    Q_limit = -1;
    return 0;
  }

  Q_searchx_hash_off = ptr - key;

  if (*ptr == '%') {
    Q_raw = 1;
  }
  ptr ++;
  Q_order |= 8;
  Q_order |= 4;

  while (*ptr && *ptr != '(' && *ptr != '<' && *ptr != '[') {
    ptr ++;
  }
  if (!*ptr) {
    Q_limit = -1;
    return ptr;
  }
  ptr --;
  while (*ptr >= '0' && *ptr <='9') {
    ptr --;
  }
  ptr ++;
  char *qtr;
  Q_limit = strtol (ptr, &qtr, 10);
  Q_limit_ptr = ptr;
  Q_limit_end = qtr;
  if (Q_limit > MAX_RES) { Q_limit = MAX_RES; }
  return qtr;
}

static const char *targ_parse_search_extras (const char *ptr) {
  int l = 4;
  Q_raw = Q_limit = Q_order = 0;
  if (*ptr != '#') {
    return ptr;
  }
  ptr++;
  while (--l) {
    switch (*ptr) {
    case 'i':
      Q_order &= ~4;
      ptr++;
      break;
    case 'I':
      Q_order |= 4;
      ptr++;
      break;
    case 'r':
      Q_order &= ~4;
      Q_order |= 8;
      ptr++;
      break;
    case 'R':
      Q_order |= 4;
      Q_order |= 8;
      ptr++;
      break;
    case '0' ... '9':
      Q_limit_ptr = ptr;
      char *qtr;
      Q_limit = strtol (ptr, &qtr, 10);
      ptr = qtr;
      Q_limit_end = ptr;
      if (Q_limit < 0) { Q_limit = 0; }
      if (Q_limit > MAX_RES) { Q_limit = MAX_RES; }
      break;
    case '%':
      Q_raw = 1;
      ptr++;
      break;
    default:
      return ptr;
    }
  }
  if (verbosity >= 4) {
    fprintf (stderr, "targe_parse_search_extras: Q_limit=%d, Q_order=%d, Q_raw=%d\n", Q_limit, Q_order, Q_raw);
  }
  return ptr;
}

static const char *parse_search_extras (const char *ptr) {
  if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
    return search_parse_search_extras (ptr);
  } else if (TARG_EXTENSION) {
    return targ_parse_search_extras (ptr);
  } else {
    assert (0);
  }
}

static int search_generate_new_key (char *ptr, const char *key, int len, int pos) {
  int i = pos;
  int w;
  if (Q_slice_limit < Q_limit && key[i] == '#' && Q_limit > 0 && Q_slice_limit > 0 && Q_limit_ptr) {
    i = Q_limit_ptr - key;
    assert ((unsigned) i < 2048);
    memcpy (ptr, key, i);
    i += sprintf (ptr + i, "%d%%", Q_slice_limit);
    w = key + len - Q_limit_end;
    assert ((unsigned) w < 2048);
    memcpy (ptr + i, Q_limit_end, w);
    assert (i + w < len + 3);
    ptr[i+w] = 0;
    return i + w;
  } else if (Q_raw) {
    memcpy (ptr, key, len+1);
    return len;
  } else if (key[i] == '#') {
    memcpy (ptr, key, i+1);
    ptr[i+1] = '%';
    memcpy (ptr+i+2, key+i+1, len-i);
    return len + 1;
  } else {
    memcpy (ptr, key, i);
    ptr[i] = '#';
    ptr[i+1] = '%';
    memcpy (ptr+i+2, key+i, len-i+1);
    return len + 2;
  }
}

int targ_regenerate_search_extras (char *buffer) {
  char *ptr = buffer;
  *ptr++ = '#';
  if (Q_slice_limit > 0) {
    if (Q_order & 8) {
      if (Q_order & 4) {
        *ptr++ = 'R';
      } else {
        *ptr++ = 'r';
      }
    } else {
      if (Q_order & 4) {
        *ptr++ = 'I';
      } else {
        *ptr++ = 'i';
      }
    }
    ptr += sprintf (ptr, "%d", Q_slice_limit);
  }
  *ptr++ = '%';
  *ptr++ = ')';
  *ptr = 0;
  return ptr - buffer;
}

static int targ_generate_new_key (char *ptr, const char *key, int len, int pos) {
  int i = pos;
  if (is_search && Q_slice_limit != Q_limit && key[i] == '#') {
    memcpy (ptr, key, i);
    i += targ_regenerate_search_extras (ptr + i);
    assert (i < len + 3);
    return i;
  } else if (Q_raw) {
    memcpy (ptr, key, len+1);
    return len;
  } else if (targ_extra) {
    memcpy (ptr, key, len-1);
    if (key[i] == '#') {
      memcpy (ptr+len-1, "%)", 3);
      return len + 1;
    } else {
      memcpy (ptr+len-1, "#%)", 4);
      return len + 2;
    }
  } else {
    memcpy (ptr, key, len);
    memcpy (ptr+len, "#%", 3);
    return len + 2;
  }
}

static int generate_new_key (char *ptr, const char *key, int len, int pos) {
  if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
    return search_generate_new_key (ptr, key, len, pos);
  } else if (TARG_EXTENSION) {
    return targ_generate_new_key (ptr, key, len, pos);
  } else {
    assert (0);
  }
}

static int search_prepare_search_query (const char *key, int len) {
  if (!strncmp (key, "search", 6)) { 
    is_search = 1;
    return 6;
  } else { 
    return -1; 
  }
}

static int targ_prepare_search_query (const char *key, int len) {
  int i = -1, x = -1;
  targ_extra = 1;
  if (!strncmp (key, "search(", 7)) { 
    is_search = 1;
    i = 7;
  } else if (sscanf(key, "target%*u_%*d%n(", &i) >= 0 && i >= 7 && key[i] == '(') {
    i++;
  } else if (sscanf(key, "prices%*u%n(", &i) >= 0 && i >= 7 && key[i] == '(') {
    i++;
  } else if (!strncmp (key, "ad_", 3)) { 
    switch (key[3]) {
    case 'c':
      if (key[4] == 'l') {
        sscanf (key, "ad_clicks%*u%n", &x);
      } else if (len >= 7 && key[6] == 's') {
        sscanf (key, "ad_ctrsump%*u%n", &x);
        Q_limit = 13;
      } else {
        sscanf (key, "ad_ctr%*u%n", &x);
        Q_limit = 4;
      }
      break;
    case 'd':
      sscanf (key, "ad_disable%*u%n", &x);
      break;
    case 'e':
      sscanf (key, "ad_enable%*u%n", &x);
      if (x > 0 && key[x] == '_') {
        x = -1;
        sscanf (key, "ad_enable%*u_%*d%n", &x);
      }
      break;
    case 'l':
      sscanf (key, "ad_limited_views%*u,%*u%n", &x);
      break;
    case 'm':
      sscanf (key, "ad_money%*u%n", &x);
      break;
    case 's':
      if (!strncmp (key, "ad_sump", 7)) {
        sscanf (key, "ad_sump%*u%n", &x);
        Q_limit = 9;
      } else if (!strncmp (key, "ad_setctrsump", 13)) {
        sscanf (key, "ad_setctrsump%*u:%*d,%*d,%*d,%*d,%*d%n", &x);
      } else if (!strncmp (key, "ad_setsump", 10)) {
        sscanf (key, "ad_setsump%*u:%*d,%*d,%*d%n", &x);
      } else if (!strncmp (key, "ad_setctr", 9)) {
        sscanf (key, "ad_setctr%*u:%*d,%*d%n", &x);
      } else if (!strncmp (key, "ad_setaud", 9)) {
        sscanf (key, "ad_setaud%*u:%*d%n", &x);
      } else if (!strncmp (key, "ad_sites", 8)) {
        sscanf (key, "ad_sites%*u,%*u%n", &x);
      }
      break;
    case 'v':
      sscanf (key, "ad_views%*u%n", &x);
      break;
    }
    if (x <= 0 || (key[x] && key[x] != '#')) {
      return -1; 
    }
    i = 3;
  } else if (!strncmp (key, "deletegroup", 11)) {
    sscanf (key, "deletegroup%*u%n", &x);
    i = 3;
  } else { 
    return -1; 
  }

  if (x <= 0) {
    i = len - 2;
    while (i > 7 && ((key[i] >= '0' && key[i] <= '9') || key[i] == '%' || key[i] == 'i' || key[i] == 'I' || key[i] == 'r' || key[i] == 'R')) {
      i--;
    }
    if (key[i] != '#') {
      i = len - 1;
    }
  } else {
    i = x;
  }
  targ_extra = (x <= 0);
  return i;
}


static int prepare_search_query (const char *key, int len) {
  is_search = 0;
  if (SEARCH_EXTENSION || SEARCHX_EXTENSION) {
    return search_prepare_search_query (key, len);
  } else if (TARG_EXTENSION) {
    return targ_prepare_search_query (key, len);
  } else {
    assert (0);
  }
}

struct mc_proxy_merge_functions search_extension_functions = {
  .free_gather_extra = search_free_gather_extra,
  .merge_end_query = search_merge_end_query,
  .generate_preget_query = default_generate_preget_query,
  .generate_new_key = search_merge_generate_new_key,
  .store_gather_extra = search_store_gather_extra,
  .check_query = search_check_query,
  .merge_store = default_merge_store,
  .load_saved_data = default_load_saved_data
};

struct mc_proxy_merge_functions searchx_extension_functions = {
  .free_gather_extra = search_free_gather_extra,
  .merge_end_query = search_merge_end_query,
  .generate_preget_query = default_generate_preget_query,
  .generate_new_key = searchx_generate_new_key,
  .store_gather_extra = searchx_store_gather_extra,
  .check_query = searchx_check_query,
  .merge_store = default_merge_store,
  .load_saved_data = default_load_saved_data
};

/*struct mc_proxy_merge_functions targ_extension_functions = {
  .free_gather_extra = search_free_gather_extra,
  .merge_end_query = search_merge_end_query,
  .generate_preget_query = default_generate_preget_query,
  .generate_new_key = search_merge_generate_new_key,
  .store_gather_extra = search_store_gather_extra,
  .check_query = search_check_query,
  .merge_store = default_merge_store,
  .load_saved_data = default_load_saved_data
};*/

struct mc_proxy_merge_functions hints_merge_extension_functions = {
  .free_gather_extra = hints_merge_free_gather_extra,
  .merge_end_query = hints_merge_end_query,
  .generate_preget_query = default_generate_preget_query,
  .generate_new_key = hints_merge_generate_new_key,
  .store_gather_extra = hints_merge_store_gather_extra,
  .check_query = hints_merge_check_query,
  .merge_store = default_merge_store,
  .load_saved_data = default_load_saved_data
};

struct mc_proxy_merge_conf search_extension_conf = {
  .use_at = 1,
  .use_preget_query = 0
};

struct mc_proxy_merge_conf searchx_extension_conf = {
  .use_at = 1,
  .use_preget_query = 0
};

/*struct mc_proxy_merge_conf targ_extension_conf = {
  .use_at = 0,
  .use_preget_query = 0
};*/

struct mc_proxy_merge_conf hints_merge_extension_conf = {
  .use_at = 1,
  .use_preget_query = 0
};
