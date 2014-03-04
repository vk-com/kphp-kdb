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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#include "vv-tl-parse.h"
#include "rpc-proxy/rpc-proxy.h"
#include "rpc-proxy/rpc-proxy-merge.h"
#include "net-rpc-targets.h"
#include "hints-tl.h"
#include "estimate-split.h"

#include <assert.h>


typedef struct gather_heap_entry {
  int type;
  int object_id;
  double rating;

  int *cur;
  int remaining;
} gh_entry_t;

static gh_entry_t GH_E[MAX_CLUSTER_SERVERS];
static gh_entry_t *GH[MAX_CLUSTER_SERVERS + 1];
static int GH_N, GH_total;

static inline void clear_gather_heap (void) {
  GH_N = 0;
  GH_total = 0;
}

static inline void load_heap_v (gh_entry_t *H) {
  assert (H->remaining);

  H->type = *H->cur++;
  H->object_id = *H->cur++;
  H->rating = *(double *)H->cur;
  H->cur += 2;
  H->remaining--;
}

static inline int gh_entry_less (gh_entry_t *lhs, gh_entry_t *rhs) {
  return (lhs->rating > rhs->rating + 1e-9 || (lhs->rating >= rhs->rating - 1e-9 && 
          (lhs->type  < rhs->type          || (lhs->type   == rhs->type && lhs->object_id < rhs->object_id))));
}

static int gather_heap_insert (int *data, int bytes) {
  if (bytes < 12) {
    vkprintf (2, "Bad result: bytes = %d\n", bytes);
    return -1;
  }
  if (*data++ != TL_VECTOR_TOTAL) {
    vkprintf (2, "Bad result: data = %08x\n", data[-1]);
    return -1;
  }
  gh_entry_t *H = &GH_E[GH_N];
  GH_total += *data++;
  H->remaining = *data++;
  if (H->remaining * 4 * 4 + 12 != bytes) {
    vkprintf (2, "Bad result: H->remaining = %d, bytes = %d\n", H->remaining, bytes);
    return -1;
  }
  vkprintf (4, "gather_heap_insert: %d elements (size %d)\n", H->remaining, bytes - 12);
  if (!H->remaining) {
    return 0;
  }

  H->cur = data;
  load_heap_v (H);

  int i = ++GH_N, j;
  while (i > 1) {
    j = (i >> 1);
    if (gh_entry_less (GH[j], H)) {
      break;
    }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;

  return 1;
}

static gh_entry_t *get_gather_heap_head (void) {
  return GH_N ? GH[1] : NULL;
}

static void gather_heap_advance (void) {
  if (!GH_N) {
    return;
  }

  gh_entry_t *H = GH[1];
  if (!H->remaining) {
    H = GH[GH_N--];
    if (!GH_N) {
      return;
    }
  } else {
    load_heap_v (H);
  }

  int i = 1;
  while (1) {
    int j = i * 2;
    if (j > GH_N) {
      break;
    }
    if (j < GH_N && gh_entry_less (GH[j + 1], GH[j])) {
      j++;
    }
    if (!gh_entry_less (GH[j], H)) {
      break;
    }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;
}


struct hints_extra {
  int op;
  int user_id;
  int type;
  int limit;

  int sent_op;
  int sent_limit;
};

void rpc_proxy_hints_on_end (struct gather *G) {
  struct hints_extra *extra = G->extra;
  clear_gather_heap ();
  int i;
  for (i = 0; i < G->tot_num; i++) {
    if (G->List[i].bytes >= 0) {
      int res = gather_heap_insert (G->List[i].data, G->List[i].bytes);
      if (res < 0) {
        received_bad_answers++;
      }
    } else {
      vkprintf (4, "Dropping result %d (num = %d)\n", i, G->List[i].bytes);
    }
  }

  if (merge_init_response (G)) {
    tl_store_int (TL_VECTOR_TOTAL);
    tl_store_int (GH_total);
    int *x = tl_store_get_ptr (4);

    for (i = 0; i < extra->limit; i++) {
      gh_entry_t *H = get_gather_heap_head ();
      if (H == NULL) {
        break;
      }

      tl_store_int (H->type);
      tl_store_int (H->object_id);
      if (extra->op == extra->sent_op) {
        tl_store_double (H->rating);
      }

      gather_heap_advance ();
    }

    *x = i;
    tl_store_end ();
  }

  free (G->extra);
  merge_delete (G);
  return;
}

void *rpc_proxy_hints_on_start (void) {
  struct hints_extra *extra = malloc (sizeof (*extra));
  extra->op = tl_fetch_int ();
  extra->user_id = tl_fetch_int ();
  extra->type = tl_fetch_int ();
  extra->limit = tl_fetch_int ();

  if (extra->limit > 10000) {
    extra->limit = 10000;
  } else if (extra->limit < 0) {
    extra->limit = 0;
  }

  switch (extra->op) {
    case TL_HINTS_GET_HINTS:
    case TL_HINTS_GET_HINTS_RATING:
      extra->sent_op = TL_HINTS_GET_HINTS_RATING;
      break;
    case TL_HINTS_GET_HINTS_LATIN:
    case TL_HINTS_GET_HINTS_LATIN_RATING:
      extra->sent_op = TL_HINTS_GET_HINTS_LATIN_RATING;
      break;
    case TL_RATING_GET_HINTS:
    case TL_RATING_GET_HINTS_RATING:
      extra->sent_op = TL_RATING_GET_HINTS_RATING;
      break;
    default:
      assert (0);
  }

  extra->sent_limit = estimate_split (extra->limit, CC->tot_buckets);
  vkprintf (3, "op = 0x%08x, sent_op = 0x%08x, limit = %d, sent_limit = %d\n", extra->op, extra->sent_op, extra->limit, extra->sent_limit);
  return extra;
}

int rpc_proxy_hints_on_send (struct gather *G, int num) {
  struct hints_extra *extra = G->extra;
  tl_store_int (extra->sent_op);
  tl_store_int (extra->user_id);
  tl_store_int (extra->type);
  tl_store_int (extra->sent_limit);
  vkprintf (4, "tl_fetch_unread () = %d\n", tl_fetch_unread ());
  tl_copy_through (tl_fetch_unread (), 0);
  return 0;
}

void rpc_proxy_hints_on_error (struct gather *G, int num) {
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

struct gather_methods hints_gather_methods = {
  .on_start = rpc_proxy_hints_on_start,
  .on_send = rpc_proxy_hints_on_send,
  .on_error = rpc_proxy_hints_on_error,
  .on_answer = NULL,
  .on_timeout = NULL,
  .on_end = rpc_proxy_hints_on_end,
  .on_send_end = NULL
};


int hints_merge_forward (void) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_HINTS_GET_HINTS ||
      op == TL_HINTS_GET_HINTS_RATING ||
      op == TL_HINTS_GET_HINTS_LATIN ||
      op == TL_HINTS_GET_HINTS_LATIN_RATING ||
      op == TL_RATING_GET_HINTS ||
      op == TL_RATING_GET_HINTS_RATING) {
    merge_forward (&hints_gather_methods);
    return 0;
  } else if (op == TL_HINTS_SET_TEXT_GLOBAL ||
             op == TL_HINTS_SET_TYPE_GLOBAL ||
             op == TL_HINTS_DELETE_OBJECT_GLOBAL ||
             op == TL_RATING_SET_TYPE_GLOBAL ||
             op == TL_RATING_DELETE_OBJECT_GLOBAL || 
             op == TL_HINTS_GET_HINTS_TEXT || 
             op == TL_HINTS_GET_HINTS_FULL || 
             op == TL_HINTS_GET_HINTS_LATIN_TEXT || 
             op == TL_HINTS_GET_HINTS_LATIN_FULL ||
             op == TL_RATING_GET_RANDOM_HINTS ||
             op == TL_RATING_GET_RANDOM_HINTS_RATING ||
             op == TL_HINTS_SORT ||
             op == TL_HINTS_GET_RANDOM ||
             op == TL_RATING_SORT ||
             op == TL_RATING_GET_RANDOM) {
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unsupported function %08x", op);
    return -1;
  } else {
    return default_firstint_forward ();
  }
}

SCHEMA_ADD(hints_merge) {
  if (C->methods.forward) {
    return -1;
  }
  C->methods.forward = hints_merge_forward;
  return 0;
}
SCHEMA_REGISTER(hints_merge,0)
