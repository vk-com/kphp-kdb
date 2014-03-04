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
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#include "vv-tl-parse.h"
#include "rpc-proxy/rpc-proxy.h"
#include "rpc-proxy/rpc-proxy-merge.h"
#include "rpc-proxy/rpc-proxy-merge-diagonal.h"
#include "net-rpc-targets.h"
#include "estimate-split.h"
#include "TL/constants.h"

struct targ_extra {
  int op;
  int mode;
  int limit;
  int ad_id;
};

#define MAX_RES 65536
int R[MAX_RES];

/* {{{ -------- LIST GATHER/MERGE ------------- */

static inline unsigned long long make_value64 (int value, int x) {
  unsigned int a = value; a -= (unsigned int) INT_MIN;
  unsigned int b = x; b -= (unsigned int) INT_MIN;
  return (((unsigned long long) a) << 32) | b;
}

typedef struct gather_heap_entry {
  int ad_id;
  int views;
  unsigned long long value64;
  int *cur;
  int remaining;
} gh_entry_t;

static gh_entry_t GH_E[MAX_CLUSTER_SERVERS];
static gh_entry_t *GH[MAX_CLUSTER_SERVERS + 1];
static int GH_N, GH_total, GH_mode, GH_n;

static void clear_gather_heap (int mode) {
  GH_N = 0;
  GH_total = 0;
  GH_mode = mode;
  GH_n = 0;
}

static inline void load_heap_v (gh_entry_t *H) {
  assert (H->remaining);
  H->ad_id = *(H->cur ++);
  if (GH_mode & 8) {
    H->views = H->ad_id;
  } else {
    H->views = *(H->cur ++);
  }
  if (!(GH_mode & 16)) {
    if (GH_mode & 4) {
      H->value64 = H->ad_id;
    } else {
      H->value64 = -H->ad_id;
    }
  } else {
    if (GH_mode & 4) {
      H->value64 = H->views;
    } else {
      H->value64 = -H->views;
    }
  }
  H->remaining --;
}



static int gather_heap_insert (int *data, int bytes) {
  if (bytes < 16) {
    vkprintf (2, "Bad result: bytes = %d\n", bytes);
    return -1;
  }
  if (*(data) == TL_MAYBE_FALSE) {
    return 0;
  }
  if (*(data) == TL_MAYBE_TRUE) {
    data ++;
    bytes -= 4;
  }
  if (*(data ++) != TL_VECTOR_TOTAL) {
    vkprintf (2, "Bad result: data = %d\n", *(data - 1));
    return -1;
  }
  gh_entry_t *H = &GH_E[GH_N];
  GH_total += *(data ++);
  H->remaining = *(data ++);
  int size = (GH_mode & 8) ? 1 : 2;
  if (H->remaining * size * 4 + 12 != bytes) {
    vkprintf (2, "Bad result: size = %d, H->remaining = %d, bytes = %d\n", size, H->remaining, bytes);
    return -1;
  }
  vkprintf (4, "gather_heap_insert: %d elements (size %d)\n", H->remaining, bytes - 12);
  if (!H->remaining) {
    return 0;
  }

  H->cur = data;
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

static gh_entry_t *get_gather_heap_head (void) {
  return GH_N ? GH[1] : 0;
}

static void gather_heap_advance (void) {
  gh_entry_t *H;
  if (!GH_N) { return; }
  H = GH[1];
  if (!H->remaining) {
    H = GH[GH_N--];
    if (!GH_N) { return; }
  } else {
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

/* -------- END (LIST GATHER/MERGE) ------- }}} */

int Q_userlist;
int Q_ul_size;
int Q[MAX_RES];
int QN[MAX_RES];
int Qf[MAX_RES];
int Qs[MAX_RES];
char QBefore[1000];
int QBeforeSize;

int work_userlist (void) {
  Q_userlist = 1;
  int n = tl_fetch_int ();
  Q_ul_size = n;
  int N = CC->tot_buckets;
  int i;
  for (i = 0; i < N; i++) {
    Qf[i] = -1;
    Qs[i] = 0;
  }
  if (n <= 0) {
    return 4;
  } else {
    tl_fetch_raw_data (Q, 4 * n);
    for (i = n - 1; i >= 0; i--) {
      int m = Q[i] % N;
      if (m < 0) { m = -m; }
      QN[i] = Qf[m];
      Qf[m] = i;
      Qs[m] ++;
    }
    return 4 + 4 * n;
  }
}

int userlist_on_send (struct gather *G, int n) {
  if (!Q_userlist) {
    return default_gather_on_send (G, n);
  } else {
    if (!Qs[n]) {
      return -1;
    } else {
      tl_store_raw_data (QBefore, QBeforeSize);
      int m = Qf[n];
      int cc = 0;
      tl_store_int (Qs[n]);
      while (m != -1) {
        tl_store_int (Q[m]);
        m = QN[m];
        cc ++;
      }
      assert (cc == Qs[n]);
      tl_copy_through (tl_fetch_unread (), 0);
      return 0;
    }
  }
}

void *sum_vector_on_start (void) {
  int op = tl_fetch_int ();
  int mode = 0;
  int limit = 0;
  int ad_id = 0;
  switch (op) {
  case TL_TARG_RECENT_VIEWS_STATS:
    ad_id = -1;
    mode = tl_fetch_int ();
    limit = tl_fetch_int ();
    break;
  case TL_TARG_RECENT_AD_VIEWERS:
    ad_id = tl_fetch_int ();
    mode = tl_fetch_int ();
    limit = tl_fetch_int ();
    break;
  default:
    fprintf (stderr, "op = 0x%08x\n", op);
    assert (0);
  }
  struct targ_extra *e = zmalloc (sizeof (*e));
  e->op = op;
  e->ad_id = ad_id;
  e->limit = limit;
  e->mode = mode;
  return e;
}

int sum_vector_on_send (struct gather *G, int num) {
  struct targ_extra *e = G->extra;
  switch (e->op) {
  case TL_TARG_RECENT_VIEWS_STATS:
    tl_store_int (e->op);
    tl_store_int (e->mode & (~1));
    tl_store_int (e->limit + 100);
    break;
  case TL_TARG_RECENT_AD_VIEWERS:
    tl_store_int (e->op);
    tl_store_int (e->ad_id);
    tl_store_int (e->mode & (~1));
    tl_store_int (e->limit);
    break;
  default:
    fprintf (stderr, "op = 0x%08x\n", e->op);
    assert (0);
  }
  return 0;
}

int cmp (const void *_a, const void *_b) {
  return *(int *)_a - *(int *)_b;
}

int cmpd (const void *_a, const void *_b) {
  return *(int *)_b - *(int *)_a;
}

void sum_vector_on_end (struct gather *G) {
  struct targ_extra *e = G->extra;
  if (merge_init_response (G) < 0) {
    zfree (e, sizeof (*e));
    merge_delete (G);
    return;
  }
//  int Q_limit = e->limit;
  int Q_order = e->mode & 7;
  clear_gather_heap (Q_order);
 
  int i;
  for (i = 0; i < G->tot_num; i++) if (G->List[i].bytes >= 0) {
    int res = gather_heap_insert (G->List[i].data, G->List[i].bytes);
    if (res < 0) {
      received_bad_answers ++;
    }
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, G->List[i].bytes);
    }
  }

  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (GH_total);
  int cur = -2;

  while (1) {
    gh_entry_t *H = get_gather_heap_head ();
    if (!H) { break; }

    if (cur >= 0 && H->ad_id == R[cur]) {
      R[cur + 1] += H->views;
    } else {
      cur += 2;
      R[cur] = H->ad_id;
      R[cur + 1] = H->views;
      if (cur >= MAX_RES) { break; }
    }
    gather_heap_advance ();
  }

  if (cur < 0) {
    tl_store_int (0);
  } else {
    cur += 2;
    if (e->mode & 1) {
      qsort (R, cur / 2, 8, (e->mode & 4) ? cmp : cmpd);
    }
    cur /= 2;
    if (cur > e->limit) {
      cur = e->limit;
    }
    tl_store_int (cur);
    tl_store_raw_data (R, cur * 8);
  }
  tl_store_end ();
  zfree (e, sizeof (*e));
  merge_delete (G);
  return;
}

void sum_vector_on_error (struct gather *G, int num) {
  int error_code = tl_fetch_lookup_int ();
  if (TL_IS_USER_ERROR (error_code)) {
    if (merge_init_response (G) >= 0) {
      tl_store_header (CQ->h);
      tl_copy_through (tl_fetch_unread (), 1);
      tl_store_end ();
    }
    struct targ_extra *e = G->extra;
    zfree (e, sizeof (*e));
    merge_terminate_gather (G);
  }
}

struct gather_methods sum_vector_gather_methods = {
  .on_start = sum_vector_on_start,
  .on_send = sum_vector_on_send,
  .on_error = sum_vector_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = sum_vector_on_end,
  .on_send_end = 0
};

void *sum_search_on_start (void) {
  tl_fetch_mark ();
  int op = tl_fetch_int ();
  int mode = tl_fetch_int ();
  int limit = tl_fetch_int ();
  if (mode & (1 << 19)) {
    tl_fetch_mark_restore ();
    QBeforeSize = 12;
    tl_fetch_raw_data (QBefore, QBeforeSize);
    work_userlist ();
  } else {
    Q_userlist = 0;
    tl_fetch_mark_restore ();
  }
  struct targ_extra *e = zmalloc (sizeof (*e));
  e->op = op;
  e->limit = limit;
  e->mode = mode;
  return e;
}

void sum_search_on_end (struct gather *G) {
  struct targ_extra *e = G->extra;
  if (merge_init_response (G) < 0) {
    zfree (e, sizeof (*e));
    merge_delete (G);
    return;
  }
//  int Q_limit = e->limit;
  int Q_order = (e->mode & 7) | 16;
  if (!(Q_order & 1)) {
    Q_order |= 8;
  }
  clear_gather_heap (Q_order);
 
  int i;
  for (i = 0; i < G->tot_num; i++) if (G->List[i].bytes >= 0) {
    int res = gather_heap_insert (G->List[i].data, G->List[i].bytes);
    if (res < 0) {
      received_bad_answers ++;
    }
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, G->List[i].bytes);
    }
  }

  tl_store_int (TL_VECTOR_TOTAL);
  tl_store_int (GH_total);
  int *cur = tl_store_get_ptr (4);
  *cur = 0;
  

  while (1) {
    if (*cur >= e->limit) { break; }
    gh_entry_t *H = get_gather_heap_head ();
    if (!H) { break; }
    
    tl_store_int (H->ad_id);
    if (!(Q_order & 8)) {
      tl_store_int (H->views);
    }
    (*cur) ++;
    gather_heap_advance ();
  }
  tl_store_end ();
  zfree (e, sizeof (*e));
  merge_delete (G);
  return;
}

struct gather_methods sum_search_gather_methods = {
  .on_start = sum_search_on_start,
  .on_send = userlist_on_send,
  .on_error = sum_vector_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = sum_search_on_end,
  .on_send_end = 0
};

void *sum_one_on_start (void) {
  return (void *)1;
}

void sum_one_on_answer (struct gather *G, int num) {
  if (tl_fetch_int () != TL_MAYBE_TRUE) {
    return;
  }
  int x = tl_fetch_int ();
  G->extra = (void *)(x + (long)G->extra);
}

void sum_one_on_end (struct gather *G) {
  if (merge_init_response (G) >= 0) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (((long)G->extra) - 1);
    tl_store_end ();
  }
  merge_delete (G);
}

void sum_one_on_error (struct gather *G, int num) {
  int error_code = tl_fetch_lookup_int ();
  if (TL_IS_USER_ERROR (error_code)) {
    if (merge_init_response (G) >= 0) {
      tl_store_header (CQ->h);
      tl_copy_through (tl_fetch_unread (), 1);
      tl_store_end ();
    }
    merge_terminate_gather (G);
  }
}

struct gather_methods sum_one_gather_methods = {
  .on_start = sum_one_on_start,
  .on_send = userlist_on_send,
  .on_error = sum_one_on_error,
  .on_answer = sum_one_on_answer,
  .on_timeout = 0,
  .on_end = sum_one_on_end,
  .on_send_end = 0
};

void *sum_tuple_on_start (void) {
  int op = tl_fetch_lookup_int ();
  int n = 0;
  int x;
  int rop;
  int t;
  int s = -1;
  switch (op) {
  case TL_TARG_AD_CTR:
    n = 4;
    rop = TL_MAYBE_TRUE;
    break;
  case TL_TARG_AD_SUMP:
    n = 9;
    rop = TL_MAYBE_TRUE;
    break;
  case TL_TARG_AD_CTR_SUMP:
    n = 13;
    rop = TL_MAYBE_TRUE;
    break;
  case TL_TARG_PRICES:
    tl_fetch_mark ();
    t = 0;
    tl_fetch_int (); // op
    x = tl_fetch_int (); // mode
    tl_fetch_int (); //place
    if (x & (1 << 17)) {
      tl_fetch_int (); //and
      tl_fetch_int (); //xor
      t += 8;
    }
    n = tl_fetch_int () + 1;
    if (x & (1 << 19)) {
      assert (t + 16 <= 1000);
      tl_fetch_mark_restore ();
      QBeforeSize = t + 16;
      tl_fetch_raw_data (QBefore, QBeforeSize);
      t += work_userlist ();
    } else {
      Q_userlist = 0;
      tl_fetch_mark_restore ();
    }
    rop = TL_TARG_PRICES_RESULT;
    break;
  case TL_TARG_AD_PRICING:
    tl_fetch_mark ();
    t = 0;
    tl_fetch_int (); // op
    x = tl_fetch_int (); // mode
    tl_fetch_int (); //ad
    tl_fetch_int (); //place
    if (x & (1 << 17)) {
      tl_fetch_int (); //and
      tl_fetch_int (); //xor
      t += 8;
    }
    if (x & (1 << 18)) {
      tl_fetch_int (); // max_users;
      t += 4;
    }
    n = tl_fetch_int () + 1;
    tl_fetch_mark_restore ();
    rop = TL_TARG_PRICES_RESULT;
    break;
  case TL_TARG_TARG_AUDIENCE:
    tl_fetch_mark ();
    t = 0;
    tl_fetch_int (); // op
    x = tl_fetch_int (); // mode
    tl_fetch_int (); //place
    tl_fetch_int (); //cpv
    if (x & (1 << 17)) {
      tl_fetch_int (); //and
      tl_fetch_int (); //xor
      t += 8;
    }
    if (x & (1 << 18)) {
      tl_fetch_int (); // max_users;
      t += 4;
    }
    if (x & (1 << 19)) {
      assert (t + 16 <= 1000);
      tl_fetch_mark_restore ();
      QBeforeSize = t + 16;
      tl_fetch_raw_data (QBefore, QBeforeSize);
      t += work_userlist ();
      n = tl_fetch_lookup_int ();
      s = n + 1;
      n = 3 * (n + 1);
    } else {
      n = tl_fetch_lookup_int ();
      s = n + 1;
      n = 3 * (n + 1);
      Q_userlist = 0;
      tl_fetch_mark_restore ();
    }
    rop = TL_VECTOR;
    break;
  case TL_TARG_AUDIENCE:
    tl_fetch_mark ();
    t = 0;
    tl_fetch_int (); // op
    x = tl_fetch_int (); // mode
    if (x & (1 << 19)) {
      assert (t + 8 <= 1000);
      tl_fetch_mark_restore ();
      QBeforeSize = t + 8;
      tl_fetch_raw_data (QBefore, QBeforeSize);
      t += work_userlist ();
      n = tl_fetch_lookup_int ();
      s = n + 1;
      n = (n + 1);
    } else {
      n = tl_fetch_lookup_int ();
      s = n + 1;
      n = (n + 1);
      Q_userlist = 0;
      tl_fetch_mark_restore ();
    }
    rop = TL_VECTOR;
    break;
  }
  if (!n) { return 0; }
  int *r = zmalloc0 (12 + n * 4);
  r[0] = n;
  r[1] = rop;
  r[2] = s;
  return r;
}

void sum_tuple_on_answer (struct gather *G, int num) {
  int *r = G->extra;
  if (tl_fetch_int () != r[1]) {
    return;
  } else {
    if (r[2] < 0 || r[2] == tl_fetch_int ()) {
      if (tl_fetch_unread () != r[0] * 4) { return; }
      int i;
      for (i = 0; i < r[0]; i++) {
        r[i + 3] += tl_fetch_int ();
      }
    }
  }
}

void sum_tuple_on_end (struct gather *G) {
  int *r = G->extra;
  if (merge_init_response (G) >= 0) {
    tl_store_int (r[1]);
    if (r[2] >= 0) {
      tl_store_int (r[2]);
    }
    tl_store_raw_data (r + 3, r[0] * 4);
    tl_store_end ();
  }
  zfree (r, 4 * r[0] + 12);
  merge_delete (G);
}

void sum_tuple_on_error (struct gather *G, int num) {
  int error_code = tl_fetch_lookup_int ();
  if (TL_IS_USER_ERROR (error_code)) {
    if (merge_init_response (G) >= 0) {
      tl_store_header (CQ->h);
      tl_copy_through (tl_fetch_unread (), 1);
      tl_store_end ();
    }
    int *r = G->extra;
    zfree (r, 4 * r[0] + 12);
    merge_terminate_gather (G);
  }
}
/*
int sum_tuple_userlist_on_send (struct gather *G, int n) {
  int x = tl_fetch_unread ();
  tl_fetch_int (); // op
  int mode = tl_fetch_int ();
  if (!(mode & (1 << 19))) {
    
  }
}*/

struct gather_methods sum_tuple_gather_methods = {
  .on_start = sum_tuple_on_start,
  .on_send = userlist_on_send,
  .on_error = sum_tuple_on_error,
  .on_answer = sum_tuple_on_answer,
  .on_timeout = 0,
  .on_end = sum_tuple_on_end,
  .on_send_end = 0
};
/*
struct gather_methods sum_tuple_userlist_gather_methods = {
  .on_start = sum_tuple_on_start,
  .on_send = sum_tuple_userlist_on_send,,
  .on_error = sum_tuple_on_error,
  .on_answer = sum_tuple_on_answer,
  .on_timeout = 0,
  .on_end = sum_tuple_on_end,
  .on_send_end = 0
};*/


int targ_forward (void) {
  Q_userlist = 0;
  int op = tl_fetch_lookup_int ();
  assert (op);
  //struct gather_methods m;
  switch (op) {
  case TL_TARG_AD_ENABLE:
  case TL_TARG_AD_ENABLE_PRICE:
  case TL_TARG_AD_DISABLE:
  case TL_TARG_AD_SET_CTR:
  case TL_TARG_AD_SET_SUMP:
  case TL_TARG_AD_SET_CTR_SUMP:
  case TL_TARG_AD_SET_AUD:
  case TL_TARG_AD_LIMITED_VIEWS:
  case TL_TARG_AD_VIEWS_RATE_LIMIT:
  case TL_TARG_AD_SITES:
  case TL_TARG_AD_SET_FACTOR:
  case TL_TARG_AD_SET_DOMAIN:
  case TL_TARG_AD_SET_CATEGORIES:
  case TL_TARG_AD_SET_GROUP:
  case TL_TARG_DELETE_GROUP:
    merge_forward (&diagonal_gather_methods);
    return 0;
  case TL_TARG_AD_CLICKS:
  case TL_TARG_AD_MONEY:
  case TL_TARG_AD_VIEWS:
  case TL_TARG_AD_RECENT_VIEWS:
  case TL_TARG_TARGET:
    merge_forward (&sum_one_gather_methods);
    return 0;
  case TL_TARG_AD_CTR:
  case TL_TARG_AD_SUMP:
  case TL_TARG_AD_CTR_SUMP:
    merge_forward (&sum_tuple_gather_methods);
    return 0;
  case TL_TARG_RECENT_VIEWS_STATS:
  case TL_TARG_RECENT_AD_VIEWERS:
    merge_forward (&sum_vector_gather_methods);
    return 0;
  case TL_TARG_SEARCH:
    merge_forward (&sum_search_gather_methods);
    return 0;
  case TL_TARG_AD_INFO:
    return default_random_forward ();
  case TL_TARG_PRICES:
  case TL_TARG_AD_PRICING:
  case TL_TARG_TARG_AUDIENCE:
  case TL_TARG_AUDIENCE:
    //merge_forward (&sum_tuple_userlist_gather_methods);
    merge_forward (&sum_tuple_gather_methods);
    return 0;
  default:
    return default_firstint_forward ();
  }
/*    m = sum_one_gather_methods;
    m.on_send = user_list_on_send;
    merge_forward (&m);*/
}

SCHEMA_ADD(targ) {
  if (C->methods.forward) {
    return -1;
  }
  C->methods.forward = targ_forward;
  return 0;
}
SCHEMA_REGISTER(targ,0)
