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

    Copyright      2013 Vkontakte Ltd
              2010-2011 Nikolai Durov (original news-merge.c)
              2010-2011 Andrei Lopatin (original news-merge.c)
              2011-2013 Vitaliy Valtman (original mc-proxy-news-recommend.c)
                   2013 Vitaliy Valtman
*/

#include "vv-tl-parse.h"
#include "rpc-proxy/rpc-proxy.h"
#include "rpc-proxy/rpc-proxy-merge.h"
#include "rpc-proxy/rpc-proxy-merge-diagonal.h"
#include "rpc-proxy-merge-news-r.h"
#include "../news/news-tl.h"
#include "TL/constants.h"

#define MAX_FRIENDS_NUM 100000
#define MAX_NEWS_BUCKETS 1000
#define MAX_RES	65536

int Q[MAX_FRIENDS_NUM];
int QN[MAX_FRIENDS_NUM];
int Rlen[MAX_NEWS_BUCKETS];
int Rfirst[MAX_NEWS_BUCKETS];
int Q_op;
int Q_size;
int Q_uid;
int R_common_len;
int R[MAX_FRIENDS_NUM];

/* merge data structures */

typedef struct news_item item_t;

#pragma pack(push,4)
struct news_id {
  int type;
  int owner;
  int place;
};

struct news_item {
  struct news_id id;
  int nusers;
  double weight;
};
#pragma pack(pop)

typedef struct gather_heap_entry {
  struct news_id id;
  int *cur, *last;
  int remaining;
} gh_entry_t;

static gh_entry_t GH_E[MAX_CLUSTER_SERVERS];
static gh_entry_t *GH[MAX_CLUSTER_SERVERS+1];
static int GH_N, GH_total;

static void clear_gather_heap (int mode) {
  GH_N = 0;
  GH_total = 0;
}

static inline void load_heap_v (gh_entry_t *H) {
  int *data = H->cur;
  H->id.type = data[0];
  H->id.owner = data[1];
  H->id.place = data[2];
}

static int news_id_compare (struct news_id id1, struct news_id id2) {
  if (id1.type != id2.type) { return id1.type - id2.type; }
  if (id1.owner != id2.owner) { return id1.owner - id2.owner; }
  return id1.place - id2.place;
}

static int gather_heap_insert (struct gather_entry *GE) {
  gh_entry_t *H;  
  assert (GH_N < MAX_CLUSTER_SERVERS);
  int sz = 6;
  if (GE->bytes < 8 || GE->data[0] != TL_VECTOR || GE->data[1] <= 0 || GE->bytes != 8 + 4 * sz * (GE->data[1])) {
    return 0;
  }
  if (verbosity >= 3) {
    fprintf (stderr, "gather_heap_insert: %d elements (size %d)\n", GE->data[1], GE->bytes);
  }
  int cnt = GE->data[1];
  GH_total += cnt;

  H = &GH_E[GH_N];
  H->remaining = cnt;
  H->cur = GE->data + 2;
  H->last = GE->data + 2 + cnt * sz;
  load_heap_v (H);

  int i = ++GH_N, j;
  struct news_id id = H->id;
  while (i > 1) {
    j = (i >> 1);
    if (news_id_compare (GH[j]->id, id) <= 0) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;

  return 1;
}

static int *get_gather_heap_head (void) {
  return GH_N ? GH[1]->cur : 0;
}

static void gather_heap_advance (void) {
  gh_entry_t *H;
  int sz = 6;
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
  struct news_id id = H->id;
  while (1) {
    j = i*2;
    if (j > GH_N) { break; }
    if (j < GH_N && news_id_compare (GH[j + 1]->id, GH[j]->id) < 0) {j ++; }
    if (news_id_compare (id, GH[j]->id) <= 0) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;
}
/* end (merge structures) */

struct union_heap_entry {
  struct news_item entry;
};

int UH_allocated;
int UH_limit;
int UH_size;
struct union_heap_entry *UH;
int UH_total;

static void union_heap_init (int size) {
  if (UH_allocated < size) {
    if (UH_allocated) {
      free (UH);
    }
    UH = malloc (sizeof (struct union_heap_entry) * (size + 1));
    assert (UH);
    UH_allocated = size;
  }
  assert (size <= UH_allocated);
  UH_limit = size;
  UH_size = 0;
  UH_total = 0;
}

static void union_heap_insert (struct news_item x) {
  assert (UH_limit >= UH_size);
  if (UH_limit == UH_size) {
    if (UH[1].entry.weight > x.weight) {
      return;
    }
    int i = 1, j;
    while (1) {
      j = i*2;
      if (j > UH_size) { break; }
      if (j < UH_size && UH[j + 1].entry.weight < UH[j].entry.weight) {j ++; }
      if (x.weight <= UH[j].entry.weight) { break; }
      UH[i] = UH[j];
      i = j;
    }
    UH[i].entry = x;
  } else {
    int i = ++UH_size, j;
    while (i > 1) {
      j = (i >> 1);
      if (UH[j].entry.weight < x.weight) { break; }
      UH[i] = UH[j];
      i = j;
    }
    UH[i].entry = x;
  }
}

static void union_heap_to_array (void) {  
  int p;
  UH_total = UH_size;
  for (p = UH_size; p >= 2; p--) {
    struct news_item x = UH[p].entry;
    assert (UH[1].entry.weight <= x.weight);
    UH[p].entry = UH[1].entry;
    UH_size --;
    UH_limit = UH_size;
    union_heap_insert (x);
  }
}
    
/* -------- LIST GATHER/MERGE ------------- */

int compare_weight (const void *a, const void *b) {
  const struct news_item *A = a;
  const struct news_item *B = b;
  if (A->weight < B->weight) { return 1; }
  if (A->weight > B->weight) { return -1; }
  return 0;
}

void rpc_proxy_rnews_on_end (struct gather *G) {
  struct rnews_gather_extra *extra = G->extra;
  int res = 0;

  clear_gather_heap (0);
  int i;
  for (i = 0; i < G->tot_num; i++) if (G->List[i].bytes > 0) {
  	//fprintf (stderr, "!!!");
    gather_heap_insert (&G->List[i]);
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, G->List[i].bytes);
    }
  }
  
  //static struct news_item R[PHP_MAX_RES * 10];
  union_heap_init (extra->limit);
  res = 0;
  struct news_item last_item = {.id.type = 0, .id.owner = 0, .id.owner = 0, .weight = 0};
  //long long last_res = 0;
  //while (res < PHP_MAX_RES * 10 - 1) {
  while (1) {
    struct news_item *t = (void *)get_gather_heap_head ();
    if (!t) {
      if (res && last_item.nusers >= extra->acting_users_limit) {
        union_heap_insert (last_item);
      }
      break;
    }
    vkprintf (4, "type = %d, owner = %d, place = %d\n", t->id.type, t->id.owner, t->id.place);
    if (res && !news_id_compare (last_item.id, t->id)) {
      assert (res > 0);
      last_item.weight += t->weight;
      last_item.nusers += t->nusers;
    } else {
      if (res > 0 && last_item.nusers >= extra->acting_users_limit) {
        union_heap_insert (last_item);
      }
      res ++; 
      last_item = *t;
    }
    gather_heap_advance ();
  }
  
  union_heap_to_array ();
  //qsort (R, res, sizeof (struct news_item), compare_weight);
  assert (res >= UH_total);
  res = UH_total;
  for (i = 0; i < res; i++) {
    vkprintf (4, "Item #%d: weight = %lf\n", i, UH[i + 1].entry.weight);
  }

  if (!extra->raw) {
    tl_store_int (TL_VECTOR);
    tl_store_int (res);
    int i;
    for (i = 0; i < res; i++) {
      tl_store_int (UH[i + 1].entry.id.type);
      tl_store_int (UH[i + 1].entry.id.owner);
      tl_store_int (UH[i + 1].entry.id.place);
    }
  } else {
    tl_store_int (TL_VECTOR);
    tl_store_int (res);
    int i;
    for (i = 0; i < res; i++) {
      tl_store_raw_data (&UH[i], sizeof (struct news_item));
    }
  }
  tl_store_end ();
  zfree (extra, sizeof (struct rnews_gather_extra));
  merge_delete (G);
}


/* -------- END (LIST GATHER/MERGE) ------- */

static void set_rlen (void) {
  int i, x;

  for (i = 0; i < CC->tot_buckets; i++) {
    Rlen[i] = 0;
    Rfirst[i] = -1;
  }
  
  int split_factor = CC->tot_buckets;
  R_common_len = 0;
  for (i = Q_size - 1; i >= 0; i--) {
    if (Q[2 * i + 1] < 0) {
      R[R_common_len++] = Q[2 * i];
      R[R_common_len++] = Q[2 * i + 1];
    } else {
      x = Q[2 * i];

      if (x < 0) { x = -x; }
      x %= split_factor;

      if (x < CC->tot_buckets) {
        QN[i] = Rfirst[x];
        Rfirst[x] = i;
        Rlen[x] ++;
      }
    }
  }
}

int rpc_proxy_rnews_on_send (struct gather *G, int num) {
  if (Rlen[num] + R_common_len <= 0 ) {
    return -1;
  }
  struct rnews_gather_extra *extra = G->extra;
  tl_store_int (TL_NEWS_GET_RAW_UPDATES);  
  tl_store_int (extra->type_mask);
  tl_store_int (extra->date);
  tl_store_int (extra->end_date);
  tl_store_int (extra->id);
  tl_store_int (extra->t);
  tl_store_int (extra->timestamp);
  tl_store_int (Rlen[num] + R_common_len);
  tl_store_raw_data (R, R_common_len * 4);
  int x = Rfirst[num];
  int i;
  for (i = 0; i < Rlen[num]; i++) {
    assert (x >= 0);
    tl_store_int (Q[2 * x + 0]);
    tl_store_int (Q[2 * x + 1]);
    x = QN[x];
  }
  assert (x == -1);
  return 0;
}

void *rpc_proxy_rnews_on_start (void) {
  Q_op = tl_fetch_int ();
  int type_mask = tl_fetch_int ();
  int date = tl_fetch_int ();
  int end_date = tl_fetch_int ();
  int id = tl_fetch_int ();
  int t = tl_fetch_int ();
  int timestamp = tl_fetch_int ();
  Q_size = tl_fetch_int ();
  if (Q_size < 0 || Q_size > MAX_FRIENDS_NUM / 2) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "max userlist size is %d, presented %d", MAX_FRIENDS_NUM, Q_size);
    return 0;
  }
  tl_fetch_string_data ((char *)Q, Q_size * 8);
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  set_rlen ();
  struct rnews_gather_extra *extra = zmalloc (sizeof (*extra));
  extra->type_mask = type_mask;
  extra->date = date;
  extra->timestamp = timestamp;
  extra->end_date = end_date;
  extra->acting_users_limit = 0;
  extra->limit = MAX_RES / 6;
  extra->t = t;
  extra->id = id;
  extra->raw = 0;
  return extra;
}

void *rpc_proxy_rnews_raw_on_start (void) {
  Q_op = tl_fetch_int ();
  int type_mask = tl_fetch_int ();
  int date = tl_fetch_int ();
  int end_date = tl_fetch_int ();
  int id = tl_fetch_int ();
  int t = tl_fetch_int ();
  int timestamp = tl_fetch_int ();
  int acting_users_limit = tl_fetch_int ();
  int limit = tl_fetch_int ();
  Q_size = tl_fetch_int ();
  if (Q_size < 0 || Q_size > MAX_FRIENDS_NUM / 2) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "max userlist size is %d, presented %d", MAX_FRIENDS_NUM, Q_size);
    return 0;
  }
  tl_fetch_string_data ((char *)Q, Q_size * 8);
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  set_rlen ();
  struct rnews_gather_extra *extra = zmalloc (sizeof (*extra));
  extra->type_mask = type_mask;
  extra->date = date;
  extra->timestamp = timestamp;
  extra->end_date = end_date;
  extra->acting_users_limit = acting_users_limit;
  extra->limit = limit > 0 ? limit : MAX_RES / 6;
  extra->t = t;
  extra->id = id;
  extra->raw = 0;
  return extra;
}

void rpc_proxy_rnews_on_error (struct gather *G, int num) {
  int error_code = tl_fetch_lookup_int ();
  if (TL_IS_USER_ERROR (error_code)) {
    if (merge_init_response (G) >= 0) {
      tl_store_header (CQ->h);
      tl_copy_through (tl_fetch_unread (), 1);
      tl_store_end ();
    }
    zfree (G->extra, sizeof (struct rnews_gather_extra));
    merge_terminate_gather (G);
  }
}


struct gather_methods rnews_gather_methods = {
  .on_start = rpc_proxy_rnews_on_start,
  .on_send = rpc_proxy_rnews_on_send,
  .on_error = rpc_proxy_rnews_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_rnews_on_end,
  .on_send_end = 0
};

struct gather_methods rnews_raw_gather_methods = {
  .on_start = rpc_proxy_rnews_raw_on_start,
  .on_send = rpc_proxy_rnews_on_send,
  .on_error = rpc_proxy_rnews_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_rnews_on_end,
  .on_send_end = 0
};
