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
              2010-2013 Vitaliy Valtman
*/

#include "vv-tl-parse.h"
#include "rpc-proxy/rpc-proxy.h"
#include "rpc-proxy/rpc-proxy-merge.h"
#include "net-rpc-targets.h"
#include "search-tl.h"
#include "estimate-split.h"

/* {{{ -------- LIST GATHER/MERGE ------------- */

inline unsigned long long make_value64 (int value, int x) {
  unsigned int a = value; a -= (unsigned int) INT_MIN;
  unsigned int b = x; b -= (unsigned int) INT_MIN;
  return (((unsigned long long) a) << 32) | b;
}

typedef struct gather_heap_entry {
  unsigned item_id;
  int owner_id;
  unsigned long long value64;
  long long hash;
  int *cur;
  int value;
  int remaining;
  int n;
} gh_entry_t;

static gh_entry_t GH_E[MAX_CLUSTER_SERVERS];
static gh_entry_t *GH[MAX_CLUSTER_SERVERS + 1];
static int GH_N, GH_total, GH_mode, GH_n;

void clear_gather_heap (int mode) {
  GH_N = 0;
  GH_total = 0;
  GH_mode = mode;
  GH_n = 0;
}

static inline void load_heap_v (gh_entry_t *H) {
  assert (H->remaining);
  if (H->n == 1) {
    H->owner_id = 0;
    H->item_id = *(H->cur ++);
  } else {
    H->owner_id = *(H->cur ++);
    H->item_id = *(H->cur ++);
  }
  if (GH_mode & FLAG_SORT) {
    H->value = *(H->cur ++);
    if (GH_mode & FLAG_SORT_DESC) {
      H->value64 = make_value64 (-(H->value+1),-(H->item_id+1));
    } else {
      H->value64 = make_value64 (H->value, H->item_id);
    }
  } else {
    H->value = H->item_id;
    H->value64 = 0;
  }
  if (GH_mode & FLAG_GROUP_HASH) {
    H->hash = *(long long *)(H->cur);
    H->cur += 2;
  }
  H->remaining --;
}



static int gather_heap_insert (int *data, int bytes) {
  if (bytes < 16) {
    vkprintf (2, "Bad result: bytes = %d\n", bytes);
    return -1;
  }
  if (*(data ++) != TL_SEARCH_RESULT) {
    vkprintf (2, "Bad result: data = %d\n", *(data - 1));
    return -1;
  }
  gh_entry_t *H = &GH_E[GH_N];
  H->n = *(data ++);
  if (H->n < 1 || H->n > 2) {
    vkprintf (2, "Bad result: H->n = %d\n", H->n);
    return -1;
  }
  GH_total += *(data ++);
  H->remaining = *(data ++);
  int size = H->n + ((GH_mode & FLAG_SORT) ? 1 : 0) + ((GH_mode & FLAG_GROUP_HASH) ? 2 : 0);
  if (H->remaining * size * 4 + 16 != bytes) {
    vkprintf (2, "Bad result: size = %d, H->remaining = %d, bytes = %d\n", size, H->remaining, bytes);
    return -1;
  }
  if (GH_n < H->n) {
    GH_n = H->n;
  }
  vkprintf (4, "gather_heap_insert: %d elements (size %d)\n", H->remaining, bytes - 16);
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

/****************** hashset ************************** {{{ */

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

/*********** end hashset ******** }}} */

struct search_extra {
  int mode;
  int limit;
  int sent_limit;
  int op;
  int attempt;
};

static long double cum_hash_ratio = 0.0;
static long long tot_hash_ratio = 0;

int resend_one (struct gather *G, int i) {
  CC = G->cluster;
  if (i >= CC->tot_buckets) { return 0; }

  long long new_qid = get_free_rpc_qid (G->qid);
  if (rpc_proxy_store_init (&CC->buckets[i], new_qid) <= 0) {
    return 0;
  }
/*  struct rpc_target *S = rpc_target_lookup_target (CC->buckets[i]);
  struct connection *c = rpc_target_choose_connection (S, 0);
  if (!c) { return 0; }*/

  //tl_store_init (c, new_qid);

  G->header->qid = new_qid;
  assert (G->header);
  tl_store_header (G->header);

  assert (G->saved_query);
  struct search_extra *extra = G->extra;
  tl_store_int (extra->op);
  tl_store_int (extra->mode);
  tl_store_int (extra->limit);
  tl_store_raw_data (G->saved_query, G->saved_query_len);


  tl_store_end_ext (RPC_INVOKE_REQ);
  if (G->List[i].bytes > 0) {
    free (G->List[i].data);
  }
  G->List[i].bytes = -2;

  struct rpc_query *q = create_rpc_query (new_qid, G->pid, G->qid, G->in_type, /*G->in,*/ merge_query_type, 0);
  q->extra = malloc (2 * sizeof (void *));
  ((void **)q->extra)[0] = G;
  ((void **)q->extra)[1] = (void *)(long)i;
  G->wait_num ++;
  return 1;
}

int check_resend (struct gather *G) {
  int i;
  struct search_extra *extra = G->extra;
  int total_sum = 0;
  int total_count = 0;
  for (i = 0; i < G->tot_num; i++) if (G->List[i].bytes >= 16 && G->List[i].data[0] == TL_SEARCH_RESULT) {
    int a = G->List[i].data[2];
    int b = G->List[i].data[3];
    if (b == extra->sent_limit && a > b) {
      total_count ++;
    }
    total_sum += b;
  }
  vkprintf (3, "total_sum = %d, limit = %d, count = %d, tot_num = %d\n", total_sum, extra->limit, total_count, G->tot_num);
  if (total_sum < 1.5 * extra->limit && total_count < 0.1 * G->tot_num) {
    int cc = 0;
    for (i = 0; i < G->tot_num; i++) if (G->List[i].bytes >= 16 && G->List[i].data[0] == TL_SEARCH_RESULT) {
      int a = G->List[i].data[2];
      int b = G->List[i].data[3];
      if (b == extra->sent_limit && a > b) {
        vkprintf (4, "Resending #%d\n", i);
        cc += resend_one (G, i);
      }
    }
    vkprintf (4, "result = %d\n", cc);
    return cc;
  } else {
    return 0;
  }
}

void rpc_proxy_search_on_end (struct gather *G) {
  struct search_extra *extra = G->extra;
  int Q_order = extra->mode;
  int Q_limit = extra->limit;
  clear_gather_heap (Q_order);
  int i;
  if  ((Q_order & FLAG_RETRY_SEARCH) && G->saved_query_len && extra->attempt == 0 && check_resend (G) > 0) {
    extra->attempt ++;
    return;
  }
  for (i = 0; i < G->tot_num; i++) if (G->List[i].bytes >= 0) {
    int res = gather_heap_insert (G->List[i].data, G->List[i].bytes);
    if (res < 0) {
      received_bad_answers ++;
    }
//    assert (r >= 0);
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "Dropping result %d (num = %d)\n", i, G->List[i].bytes);
    }
  }

  if (!merge_init_response (G)) {
    free (G->extra);
    merge_delete (G);
    return;
  }

  tl_store_int (TL_SEARCH_RESULT);
  tl_store_int (GH_n);
  tl_store_int (GH_total);
  int *x = tl_store_get_ptr (4);

  if (Q_order & FLAG_GROUP_HASH) {
    hashset_init (Q_limit);
  }
  int tot_hash_dups = 0;

  for (i = 0; i < Q_limit; ) {
    gh_entry_t *H = get_gather_heap_head ();
    if (!H) { break; }
    if (Q_order & FLAG_GROUP_HASH) {
      long long hc = H->hash;
      if (!hashset_insert (hc)) {
        /* skiping duplicate */
        tot_hash_dups++;
        gather_heap_advance ();
        continue;
      }
    }

    if (GH_n == 2) {
      tl_store_int (H->owner_id);
    }
    tl_store_int (H->item_id);
    if (Q_order & FLAG_SORT) {
      tl_store_int (H->value);
    }
    if (Q_order & FLAG_GROUP_HASH) {
      tl_store_long (H->hash);
    }
    gather_heap_advance ();
    i++;
  }

  if ((Q_order & FLAG_GROUP_HASH) && i) {
    long double hash_ratio = ((long double) (i + tot_hash_dups)) / i;
    cum_hash_ratio += hash_ratio;
    tot_hash_ratio++;
  }

  *x = i;
  tl_store_end ();
  free (G->extra);
  merge_delete (G);
  return;
}

void rpc_proxy_search_on_send_end (struct gather *G) {
  struct search_extra *extra = G->extra;
  if (extra->mode & FLAG_RETRY_SEARCH) {
    merge_save_query_remain (G);
    vkprintf (3, "saved_query_len = %d\n", G->saved_query_len);
  }
}

void *rpc_proxy_search_on_start (void) {
  struct search_extra *extra = malloc (sizeof (*extra));
  extra->op = tl_fetch_int ();
  assert (extra->op == TL_SEARCH_SEARCH);
  extra->mode = tl_fetch_int ();
  extra->limit = tl_fetch_int ();
  extra->attempt = 0;
  if ((extra->mode & FLAG_GROUP_HASH) && tot_hash_ratio) {
    extra->sent_limit = estimate_split ((int) (extra->limit * (cum_hash_ratio / tot_hash_ratio)), CC->tot_buckets);
    if (extra->sent_limit > extra->limit) { extra->sent_limit = extra->limit; }
  } else {
    extra->sent_limit = estimate_split (extra->limit, CC->tot_buckets);
  }
  vkprintf (3, "op = 0x%08x, mode = 0x%08x, limit = %d, slice_limit = %d\n", extra->op, extra->mode, extra->limit, extra->sent_limit);
  return extra;
}

int rpc_proxy_search_on_send (struct gather *G, int num) {
  struct search_extra *extra = G->extra;
  tl_store_int (extra->op);
  tl_store_int (extra->mode);
  tl_store_int (extra->sent_limit);
  vkprintf (4, "tl_fetch_unread () = %d\n", tl_fetch_unread ());
  tl_copy_through (tl_fetch_unread (), 0);
  return 0;
}

void rpc_proxy_search_on_error (struct gather *G, int num) {
  int error_code = tl_fetch_lookup_int ();
  if (TL_IS_USER_ERROR (error_code)) {
    if (merge_init_response (G) >= 0) {
      tl_store_header (CQ->h);
      tl_copy_through (tl_fetch_unread (), 1);
      tl_store_end ();
    }
    free (G->extra);
    merge_terminate_gather (G);
  }
}

struct gather_methods search_gather_methods = {
  .on_start = rpc_proxy_search_on_start,
  .on_send = rpc_proxy_search_on_send,
  .on_error = rpc_proxy_search_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_search_on_end,
  .on_send_end = rpc_proxy_search_on_send_end
};

int search_forward (void) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_SEARCH_SEARCH) {
    merge_forward (&search_gather_methods);
    return 0;
  } else if (op == TL_SEARCH_DELETE_WITH_HASH || op == TL_SEARCH_DELETE_WITH_HASHES || op == TL_SEARCH_INCR_RATE_BY_HASH || op == TL_SEARCH_CHANGE_RATES) {
    return default_query_diagonal_forward ();
  } else {
    return default_vector_forward ();
  }
}

SCHEMA_ADD(search) {
  if (C->methods.forward) {
    return -1;
  }
  C->methods.forward = search_forward;
  return 0;
}
SCHEMA_REGISTER(search,0)
