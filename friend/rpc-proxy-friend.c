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
              2013 Vitaliy Valtman
*/

#include "vv-tl-parse.h"
#include "rpc-proxy/rpc-proxy.h"
#include "rpc-proxy/rpc-proxy-merge.h"
#include "friends-tl.h"

#define MAX_COMMON_FRIENDS (1 << 20)
static int resultlist[MAX_COMMON_FRIENDS];

void rpc_proxy_common_friends_on_error (struct gather *G, int num) {
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

int compare (const void * a, const void * b) {
  return ( *(int*)a - *(int*)b );
}

void rpc_proxy_common_friends_on_end (struct gather *G) {
  int total = 0;
  int i;
  if (!merge_init_response (G)) {
    merge_delete (G);
    return;
  }
  for (i = 0; i < G->tot_num; i++) {
    if (G->List[i].bytes <= 8 || G->List[i].bytes != 8 + 4 * G->List[i].data[1]) {
      continue;
    }
    int new_total = total + G->List[i].data[1];
    if (new_total > MAX_COMMON_FRIENDS) {
      new_total = MAX_COMMON_FRIENDS;
    }
    memcpy (resultlist + total, G->List[i].data + 2, sizeof (int) * (new_total - total));
    total = new_total;
  }
  qsort (resultlist, total, sizeof (int), compare);
  tl_store_int (TL_VECTOR);
  tl_store_int (total);
  tl_store_string_data ((char *)resultlist, total * sizeof (int));
  tl_store_end ();
}

void rpc_proxy_common_friends_num_on_end (struct gather *G) {
  int total = 0;
  int i;
  if (!merge_init_response (G)) {
    merge_delete (G);
    return;
  }
  for (i = 0; i < G->tot_num; i++) {
    if (G->List[i].bytes <= 8 || G->List[i].bytes != 8 + 4 * G->List[i].data[1]) {
      continue;
    }
    int new_total = G->List[i].data[1];
    int j;
    if (new_total > total) {
      for (j = total; j < new_total; j++) {
        resultlist[j] = 0;
      }
      total = new_total;
    }
    for (j = 0; j < new_total; j++) {
      resultlist[j] += G->List[i].data[2 + j];
    }
  }
  tl_store_int (TL_VECTOR);
  tl_store_int (total);
  tl_store_string_data ((char *)resultlist, total * sizeof (int));
  tl_store_end ();
}

struct gather_methods common_friends_gather_methods = {
  .on_start = 0,
  .on_send = 0,
  .on_error = rpc_proxy_common_friends_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_common_friends_on_end,
  .on_send_end = 0
};

struct gather_methods common_friends_num_gather_methods = {
  .on_start = 0,
  .on_send = 0,
  .on_error = rpc_proxy_common_friends_on_error,
  .on_answer = 0,
  .on_timeout = 0,
  .on_end = rpc_proxy_common_friends_num_on_end,
  .on_send_end = 0
};


int friend_forward (void) {
  return default_firstint_forward ();
}

int common_friends_forward (void) {
  int op = tl_fetch_lookup_int (); // op
  if (op == TL_FRIEND_COMMON_FRIENDS) {
    merge_forward (&common_friends_gather_methods);
    return 0;
  } else if (op == TL_FRIEND_COMMON_FRIENDS_NUM) {
    merge_forward (&common_friends_num_gather_methods);
    return 0;
  } else {
    return default_firstint_forward ();
  }
}

SCHEMA_ADD(friends) {
  if (C->methods.forward) {
    return -1;
  }
  C->methods.forward = friend_forward;
  return 0;
}

SCHEMA_ADD(common_friends) {
  if (C->methods.forward) {
    return -1;
  }
  C->methods.forward = common_friends_forward;
  return 0;
}

SCHEMA_REGISTER(friends,0)
SCHEMA_REGISTER(common_friends,0)
