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

    Copyright 2013 Vkontakte Ltd
              2010-2011 Nikolai Durov (original news-merge.c)
              2010-2011 Andrei Lopatin (original news-merge.c)
              2011-2013 Vitaliy Valtman (original mc-proxy-news-recommend.c)
              2013 Vitaliy Valtman
*/

#include "vv-tl-parse.h"
#include "rpc-proxy/rpc-proxy.h"
#include "rpc-proxy/rpc-proxy-merge.h"
#include "rpc-proxy/rpc-proxy-merge-diagonal.h"
#include "rpc-proxy-merge-news.h"
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

typedef struct news_item item_t;

extern int G_NEWS_SCHEMA_NUM;

struct news_item {
  item_t *gnext;
  int g_flags;
  int day;
  int user_id;
  int date;
  int tag;
  int type;
  int user;
  int group;
  int owner;
  int place;
  int item;
};
    
/* -------- LIST GATHER/MERGE ------------- {{{ */

#define	MAX_ITEMS	1048576

static item_t *X[MAX_ITEMS];
static item_t X_src[MAX_ITEMS];
static int XN;

/*
+1  group by (type + day) + owner_id
+2  group by (type + day) + place 
+4  group by (type + day) + user_id
+8  group by (type + day) + item
+32 cancel enter+leave pairs (type+day+user_id+item)
+64 cancel multiple records (type+day+user_id, leave only the last one)
*/

static int UG_TypeReduce[] = {
0,1,2,3,4,5,6,8,
8,10,10,12,12,13,14,15,
16,17,18,19,20,21,22,23,
24,25,26,27,28,29,30,31
};

static int UG_TypeFlags[] = {
0,4,4,4,4,0,4,36,
36,36,36,40,40,4,4,4,
4,0,4,3,4,0,0,0,
0,0,0,0,0,0,0,0
};

static int Comm_TypeReduce[] = {
0,1,2,3,4,5,6,8,
8,10,10,12,12,13,14,15,
16,17,18,19,20,21,22,23,
24,25,26,27,28,29,30,31
};

static int Comm_TypeFlags[] = {
0,0,0,0,0,0,0,0,
0,0,0,0,0,0,0,0,
0,0,0,0,3,3,3,3,
3,0,0,0,0,0,0,0
};

static int *TypeReduce = UG_TypeReduce, *TypeFlags = UG_TypeFlags;

#define	HASH_SIZE	2390021
#define MAX_HASH_ITERATIONS	1000

static item_t *HX[HASH_SIZE];
static int HD[HASH_SIZE], Hs, Ht;

int ihash_lookup (item_t *A, int mode) {
  int t = A->type, tt = mode ? t : TypeReduce[t];
  item_t *B;
  unsigned h = tt * 17239;
  unsigned h2 = tt;

  if (A->day + Hs > Ht) {
    Ht = A->day + Hs;
  }

  if (mode) {
    if (TypeFlags[t] & 1) {
      h += 17 * A->owner;
      h2 += 239 * A->owner;
      mode |= 8;
    }
    if (TypeFlags[t] & 2) {
      h += 239 * A->place;
      h2 += 17 * A->place;
      mode |= 16;
    }
    if (TypeFlags[t] & 4) {
      h += A->user_id * 239;
      h2 += A->user_id * 10000;
      mode |= 2;
    }
    if (TypeFlags[t] & 8) {
      h += 666 * A->item;
      h2 += 13 * A->item;
      mode |= 4;
    }
  } else {
    h += A->user_id * 239;
    h2 += A->user_id * 10000;
    mode |= 2;

    if (mode & 32) {
      h += 666 * A->item;
      h2 += 13 * A->item;
      mode |= 4;
    }
  }

  h %= HASH_SIZE;
  h2 = (h2 % (HASH_SIZE - 1)) + 1;

  int hash_iterations = 0;

  while (1) {
    if (HD[h] != Ht || !HX[h]) {
      break;
    }
    B = HX[h];
    if (B->type == A->type || (!(mode & 1) && TypeReduce[B->type] == tt)) {
      if (!(mode & 2) || (A->user_id == B->user_id)) {
        if (!(mode & 4) || (A->item == B->item)) {
          if (!(mode & 8) || (A->owner == B->owner)) {
            if (!(mode & 16) || (A->place == B->place)) {
              return h;
            }
          }
        }
      }
    }
    h += h2;
    if (h >= HASH_SIZE) {
      h -= HASH_SIZE;
    }
    assert (++hash_iterations <= MAX_HASH_ITERATIONS);
  }

  HD[h] = Ht;
  HX[h] = 0;
  return h;
}



void isort (int a, int b) {
  int i, j, h;
  item_t *t;
  if (a >= b) { return; }
  i = a;  j = b;  h = X[(a+b)>>1]->date;
  do {
    while (X[i]->date > h) { i++; }
    while (X[j]->date < h) { j--; }
    if (i <= j) {
      t = X[i];  X[i++] = X[j];  X[j--] = t;
    }
  } while (i <= j);
  isort (a, j);
  isort (i, b);
}


int merge_items (struct news_gather_extra *extra, struct gather_entry *data, int tot_num) {
  int day = -1, i, j;
  int ug_mode = extra->ug_mode;
  int item_size = ug_mode <= 0 ? 36 : 32;
  item_t *A = 0, *B;
  int cur_num = -1;
  int remaining_items = 0;

  if (extra->date) {
    day = extra->date % 86400;
  }

  XN = 0;
  if (verbosity >= 4) {
    fprintf (stderr, "merge_items: tot_num = %d\n", tot_num);
    if (tot_num > 0) {
      fprintf (stderr, "merge_items: bytes[0] = %d\n", data[0].bytes);
    }
  }
  char *ptr = 0;
  while (1) {
    if (!remaining_items) {
      cur_num ++;
      while (cur_num < tot_num) {
        if (data[cur_num].bytes < 8 || data[cur_num].data[0] != TL_VECTOR || data[cur_num].data[1] <= 0) {
          cur_num ++;
          continue;
        }
        if (data[cur_num].data[1] * item_size + 8 != data[cur_num].bytes) {
          vkprintf (0, "Bad answer: data[1] = %d, size = %d\n", data[cur_num].data[1], data[cur_num].bytes);
          cur_num ++;
          continue;
        }
        break;
      }
      if (cur_num == tot_num) {
        break;
      }
      ptr = (char *)(data[cur_num].data + 2);      
      remaining_items = data[cur_num].data[1] - 1;
    } else {
      remaining_items--;
      ptr += item_size;
    }

    if (verbosity >= 4) {
      fprintf (stderr, "in merge: cur_num = %d, remaining = %d\n", cur_num, remaining_items);
    }

    if (ug_mode <= 0) {
      memcpy (&X_src[XN].user_id, ptr, item_size);
    } else {
      memcpy (&X_src[XN].date, ptr, item_size);
      X_src[XN].user_id = 0;
    }
    X[XN] = &X_src[XN];
    assert (X[XN]->type >= 0 && X[XN]->type < 32);
    X[XN]->g_flags = 0;
    X[XN]->gnext = 0;
    X[XN]->day = (day >= 0 ? (X[XN]->date - day) / 86400 : 0);
    XN++;
    if (XN == MAX_ITEMS) {
      break;
    }
  }

  isort (0, XN-1);

  if (!extra->grouping || extra->raw || !XN) {
    return XN;
  }

  if (Hs > 1000000000) {
    Hs = Ht = 0;
    memset (HD, 0, sizeof(HD));
  }

  Hs = ++Ht;
  j = 0;

  for (i = XN - 1; i >= 0; i--) {
    A = X[i];
    if (TypeFlags[A->type] & 96) {
      int h = ihash_lookup (A, 0);
      B = HX[h];
      if (B) {
        B->g_flags |= 2;
        j++;
      }
      HX[h] = A;
    }
  }

  Hs = ++Ht;
  for (i = XN - 1; i >= 0; i--) {
    A = X[i];
    if (!(A->g_flags) && (TypeFlags[A->type] & 15)) {
      int h = ihash_lookup (A, 1);
      B = HX[h];
      if (B) {
        B->g_flags |= 1;
        A->gnext = B;
        j++;
      }
      HX[h] = A;      
    }
  }

  if (j) {
    for (i = 0, j = 0; i < XN; i++) {
      A = X[i];
      if (!A->g_flags) {
        X[j++] = X[i];
      }
    }
    XN = j;
  }

  return XN;
}

static int serialize_one_item (item_t *A, int ug_mode) {
  if (ug_mode <= 0) {
    tl_store_raw_data (&A->user_id, 36);
  } else {
    tl_store_raw_data (&A->date, 32);
  }
  return 1;
}

static int serialize_item_group (item_t *A, int grouping, int ug_mode) {
  item_t *B;
  int t = A->type;
  assert (t >= 0 && t < 32);
  int i;

  if (!A->gnext) {
    int flags = TL_NEWS_FLAG_TYPE | ((ug_mode <= 0) ? TL_NEWS_FLAG_USER_ID : 0) | TL_NEWS_FLAG_DATE | TL_NEWS_FLAG_TAG | TL_NEWS_FLAG_USER | TL_NEWS_FLAG_GROUP | TL_NEWS_FLAG_OWNER | TL_NEWS_FLAG_PLACE | TL_NEWS_FLAG_ITEM;
    tl_store_int (flags);
    tl_store_int (1);
    tl_store_int (1);
    tl_store_int (A->type);
    if (ug_mode <= 0) {
      tl_store_int (A->user_id);
    }
    tl_store_int (A->date);
    tl_store_int (A->tag);
    tl_store_int (A->user);
    tl_store_int (A->group);
    tl_store_int (A->owner);
    tl_store_int (A->place);
    tl_store_int (A->item);
    return 1;
  }
  int n = 0;
  for (B = A; B; B = B->gnext) {
    n ++;
  }

  int *flags = tl_store_get_ptr (4);
  tl_store_int (n);
  if (n > grouping) { n = grouping; }
  tl_store_int (n);

  *flags = TL_NEWS_FLAG_TYPE | TL_NEWS_FLAG_DATE | TL_NEWS_FLAG_TAG;
  tl_store_int (A->type);
  if (ug_mode <= 0) {
    if (TypeFlags[t] & 4) {
      *flags |= TL_NEWS_FLAG_USER_ID;
      tl_store_int (A->user_id);
    } else {
      *flags |= 2 * TL_NEWS_FLAG_USER_ID;
      for (B = A, i = 0; i < n; B = B->gnext, i ++) {
        tl_store_int (B->user_id);
      }
    }
  }

  tl_store_int (A->date);
  tl_store_int (A->tag);

  *flags |= (2 * TL_NEWS_FLAG_USER);
  for (B = A, i = 0; i < n; B = B->gnext, i ++) {
    tl_store_int (B->user);
  }

  *flags |= (2 * TL_NEWS_FLAG_GROUP);
  for (B = A, i = 0; i < n; B = B->gnext, i ++) {
    tl_store_int (B->group);
  }

  if (TypeFlags[t] & 1) {
    *flags |= TL_NEWS_FLAG_OWNER;
    tl_store_int (A->owner);
  } else {
    *flags |= (2 * TL_NEWS_FLAG_OWNER);
    for (B = A, i = 0; i < n; B = B->gnext, i ++) {
      tl_store_int (B->owner);
    }
  }

  if (TypeFlags[t] & 2) {
    *flags |= TL_NEWS_FLAG_PLACE;
    tl_store_int (A->place);
  } else {
    *flags |= (2 * TL_NEWS_FLAG_PLACE);
    for (B = A, i = 0; i < n; B = B->gnext, i ++) {
      tl_store_int (B->place);
    }
  }

  if (TypeFlags[t] & 8) {
    *flags |= TL_NEWS_FLAG_ITEM;
    tl_store_int (A->item);
  } else {
    *flags |= (2 * TL_NEWS_FLAG_ITEM);
    for (B = A, i = 0; i < n; B = B->gnext, i ++) {
      tl_store_int (B->item);
    }
  }
  return 1;
}


/* -------- END (LIST GATHER/MERGE) ------- }}} */

void set_rlen_ug (int ug_mode) {
  assert (CC->tot_buckets <= MAX_NEWS_BUCKETS);
  int i;
  for (i = 0; i < CC->tot_buckets; i++) {
    Rlen[i] = 0;
    Rfirst[i] = -1;
  }

//  int f = (NEWS_UG_EXTENSION && CC->tot_buckets > split_factor);
  //int ug_mode = (CC->schema == SCHEMA_UGNEWS);
  int split_factor = (ug_mode != 0) ? CC->tot_buckets : CC->tot_buckets / 2;

  for (i = Q_size - 1; i >= 0; i--) {
    int x;
    if (ug_mode <= 0) {
      x = Q[i];
    } else {
      int t = (CC->cluster_mode & 7);
      switch (t) {
      case 2:
        x = Q[3 * i + 1];
        break;
      case 3:
        x = Q[3 * i + 2];
        break;
      default:
        x = Q[3 * i];
        break;
      }
    }
    if (ug_mode == 0) {
      if (x >= 0) {
        x %= split_factor;
      } else {
        x = (-x % split_factor) + split_factor;
      }
    } else {
      if (x < 0) { x = -x; }
      if (x < 0) { x = 0; }
      x %= split_factor;
    }
    assert (x >= 0 && x < CC->tot_buckets);
    QN[i] = Rfirst[x];
    Rfirst[x] = i;
    Rlen[x] ++;
  }
}

void *rpc_proxy_all_news_on_start (int ug_mode) {
  Q_op = tl_fetch_int ();
  int type_mask = -1;
  if (Q_op != TL_CNEWS_GET_GROUPED_UPDATES) {
    type_mask = tl_fetch_int ();
  }
  int date = tl_fetch_int ();
  int timestamp = tl_fetch_int ();
  int end_date = tl_fetch_int ();
  int grouping = tl_fetch_int ();
  int limit = tl_fetch_int ();
  Q_size = tl_fetch_int ();
  if (Q_op != TL_CNEWS_GET_GROUPED_USER_UPDATES) {
    if (Q_size < 0 || Q_size > ((ug_mode == 1) ? MAX_FRIENDS_NUM : (MAX_FRIENDS_NUM / 3))) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "max userlist size is %d, presented %d", MAX_FRIENDS_NUM, Q_size);
      return 0;
    }
    tl_fetch_string_data ((char *)Q, Q_size * (ug_mode == 1 ? 12 : 4));
  }
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  if (Q_op != TL_CNEWS_GET_GROUPED_USER_UPDATES) {
    set_rlen_ug (ug_mode);
  }
  struct news_gather_extra *extra = zmalloc (sizeof (*extra));
  extra->type_mask = type_mask;
  extra->date = date;
  extra->timestamp = timestamp;
  extra->end_date = end_date;
  extra->grouping = grouping;
  extra->limit = limit;
  extra->ug_mode = ug_mode;
  extra->raw = 0;
  return extra;
}

void *rpc_proxy_all_news_raw_on_start (int ug_mode) {
  Q_op = tl_fetch_int ();
  int type_mask = -1;
  if (Q_op != TL_CNEWS_GET_RAW_UPDATES) {
    type_mask = tl_fetch_int ();
  }
  int timestamp = tl_fetch_int ();
  int end_date = tl_fetch_int ();
  Q_size = tl_fetch_int ();
  if (Q_op != TL_CNEWS_GET_RAW_USER_UPDATES) {
    if (Q_size < 0 || Q_size > ((ug_mode == 1) ? MAX_FRIENDS_NUM : (MAX_FRIENDS_NUM / 3))) {
      tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "max userlist size is %d, presented %d", MAX_FRIENDS_NUM, Q_size);
      return 0;
    }
    tl_fetch_string_data ((char *)Q, Q_size * (ug_mode == 1 ? 12 : 4));
  }
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  if (Q_op != TL_CNEWS_GET_RAW_USER_UPDATES) {
    set_rlen_ug (ug_mode);
  }
  struct news_gather_extra *extra = zmalloc (sizeof (*extra));
  extra->type_mask = type_mask;
  extra->date = 0;
  extra->timestamp = timestamp;
  extra->end_date = end_date;
  extra->grouping = 0;
  extra->limit = 0;
  extra->ug_mode = ug_mode;
  extra->raw = 1;
  return extra;
}

void *rpc_proxy_ugnews_on_start (void) {
  return rpc_proxy_all_news_on_start (CC->schema_num == G_NEWS_SCHEMA_NUM ? -1 : 0);
}

void *rpc_proxy_ugnews_raw_on_start (void) {
  return rpc_proxy_all_news_raw_on_start (CC->schema_num == G_NEWS_SCHEMA_NUM ? -1 : 0);
}

int rpc_proxy_ugnews_on_send (struct gather *G, int num) {
  if (Rlen[num] <= 0) {
    return -1;
  }
  struct news_gather_extra *extra = G->extra;
  tl_store_int (TL_NEWS_GET_RAW_UPDATES);  
  tl_store_int (extra->type_mask);
  tl_store_int (extra->timestamp);
  tl_store_int (extra->end_date);
  tl_store_int (Rlen[num]);
  int x = Rfirst[num];
  int i;
  for (i = 0; i < Rlen[num]; i++) {
    assert (x >= 0);
    tl_store_int (Q[x]);
    x = QN[x];
  }
  assert (x == -1);
  return 0;
}

int rpc_proxy_cnews_on_send (struct gather *G, int num) {
  if (Rlen[num] <= 0) {
    return -1;
  }
  struct news_gather_extra *extra = G->extra;
  tl_store_int (TL_CNEWS_GET_RAW_UPDATES);  
  tl_store_int (extra->timestamp);
  tl_store_int (extra->end_date);
  tl_store_int (Rlen[num]);
  int x = Rfirst[num];
  int i;
  for (i = 0; i < Rlen[num]; i++) {
    assert (x >= 0);
    tl_store_int (Q[3 * x + 0]);
    tl_store_int (Q[3 * x + 1]);
    tl_store_int (Q[3 * x + 2]);
    x = QN[x];
  }
  assert (x == -1);
  return 0;
}

int rpc_proxy_cnews_user_on_send (struct gather *G, int num) {
  struct news_gather_extra *extra = G->extra;
  tl_store_int (TL_CNEWS_GET_RAW_USER_UPDATES);  
  tl_store_int (extra->type_mask);;
  tl_store_int (extra->timestamp);
  tl_store_int (extra->end_date);
  tl_store_int (Q_size);
  return 0;
}

void rpc_proxy_ugnews_on_error (struct gather *G, int num) {
  int error_code = tl_fetch_lookup_int ();
  if (TL_IS_USER_ERROR (error_code)) {
    if (merge_init_response (G) >= 0) {
      tl_store_header (CQ->h);
      tl_copy_through (tl_fetch_unread (), 1);
      tl_store_end ();
    }
    zfree (G->extra, sizeof (struct news_gather_extra));
    merge_terminate_gather (G);
  }
}

void rpc_proxy_ugnews_on_end (struct gather *G) {
  struct news_gather_extra *extra = G->extra;
  assert (extra);
  if (merge_init_response (G) < 0) {
    zfree (extra, sizeof (struct news_gather_extra));
    merge_delete (G);
    return;
  }
  switch (extra->ug_mode) {
  case 0:
  case -1:
    TypeFlags = UG_TypeFlags;
    TypeReduce = UG_TypeReduce;
    break;
  case 1:
    TypeFlags = Comm_TypeFlags;
    TypeReduce = Comm_TypeReduce;
    break;
  default:
    assert (0);
  }

  int res = merge_items (extra, G->List, G->tot_num);
  if (extra->limit && extra->limit < res) {
    res = extra->limit;
  }
  if (!extra->raw) {
    tl_store_int (TL_VECTOR);
    tl_store_int (res);
    int i;
    for (i = 0; i < res; i++) {
      serialize_item_group (X[i], extra->grouping, extra->ug_mode);
    }
  } else {
    tl_store_int (TL_VECTOR);
    tl_store_int (res);
    int i;
    for (i = 0; i < res; i++) {
      serialize_one_item (X[i], extra->ug_mode);
    }
  }
  tl_store_end ();
  zfree (extra, sizeof (struct news_gather_extra));
  merge_delete (G);
}

void *rpc_proxy_cnews_on_start (void) {
  return rpc_proxy_all_news_on_start (1);
}

void *rpc_proxy_cnews_raw_on_start (void) {
  return rpc_proxy_all_news_raw_on_start (1);
}

struct gather_methods ugnews_gather_methods = {
  .on_start = rpc_proxy_ugnews_on_start,
  .on_send = rpc_proxy_ugnews_on_send,
  .on_error = rpc_proxy_ugnews_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_ugnews_on_end,
  .on_send_end = 0
};

struct gather_methods ugnews_raw_gather_methods = {
  .on_start = rpc_proxy_ugnews_raw_on_start,
  .on_send = rpc_proxy_ugnews_on_send,
  .on_error = rpc_proxy_ugnews_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_ugnews_on_end,
  .on_send_end = 0
};

struct gather_methods cnews_gather_methods = {
  .on_start = rpc_proxy_cnews_on_start,
  .on_send = rpc_proxy_cnews_on_send,
  .on_error = rpc_proxy_ugnews_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_ugnews_on_end,
  .on_send_end = 0
};

struct gather_methods cnews_raw_gather_methods = {
  .on_start = rpc_proxy_cnews_raw_on_start,
  .on_send = rpc_proxy_cnews_on_send,
  .on_error = rpc_proxy_ugnews_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_ugnews_on_end,
  .on_send_end = 0
};

struct gather_methods cnews_user_gather_methods = {
  .on_start = rpc_proxy_cnews_on_start,
  .on_send = rpc_proxy_cnews_user_on_send,
  .on_error = rpc_proxy_ugnews_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_ugnews_on_end,
  .on_send_end = 0
};

struct gather_methods cnews_raw_user_gather_methods = {
  .on_start = rpc_proxy_cnews_raw_on_start,
  .on_send = rpc_proxy_cnews_user_on_send,
  .on_error = rpc_proxy_ugnews_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_ugnews_on_end,
  .on_send_end = 0
};


