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

#include "rpc-proxy/rpc-proxy.h"
#include "net-rpc-targets.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "text-tl.h"

#include <assert.h>
#include "vv-tl-parse.h"
#include "rpc-proxy/rpc-proxy-merge.h"
#include "rpc-proxy/rpc-proxy-merge-diagonal.h"

#define MAX_FRIENDS_NUM 100000
int Q[MAX_FRIENDS_NUM];
int Q_op;
int Q_size;
int Q_uid;

void *rpc_proxy_text_online_on_start (void) {
  Q_op = tl_fetch_int ();
  Q_uid = tl_fetch_int ();
  Q_size = tl_fetch_int ();
  if (Q_size < 0 || Q_size > MAX_FRIENDS_NUM) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "online/offline support up to %d friends, presented %d", MAX_FRIENDS_NUM, Q_size);
    return 0;
  }
  tl_fetch_string_data ((char *)Q, Q_size * 4);
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    return 0;
  }
  int *extra = malloc (3 * sizeof (int));
  extra[0] = 0;
  extra[1] = 0;
  extra[2] = 0;
  return extra;
}

int rpc_proxy_text_online_on_send (struct gather *G, int num) {
  tl_store_int (Q_op);
  tl_store_int (Q_uid);
  int *size = tl_store_get_ptr (4);
  int i;
  int z = 0;
  for (i = 0; i < Q_size; i++) if (Q[i] % CC->tot_buckets == num) {
    tl_store_int (Q[i]);
    z ++;    
  }
  if (!z) {
    return -1;
  } else {
    *size = z;
    return 0;
  }
}

struct gather_methods text_online_gather_methods = {
  .on_start = rpc_proxy_text_online_on_start,
  .on_send = rpc_proxy_text_online_on_send,
  .on_error = rpc_proxy_diagonal_on_error,
  .on_answer = rpc_proxy_diagonal_on_answer,
  .on_timeout = 0,
  .on_end = rpc_proxy_diagonal_on_end,
  .on_send_end = 0
};

int text_forward (void) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_TEXT_SET_EXTRA_MASK) {
    return default_query_diagonal_forward ();
  } else if (op == TL_TEXT_ONLINE || op == TL_TEXT_OFFLINE) {
    merge_forward (&text_online_gather_methods);
    return 0;
  } else if (op == TL_TEXT_SUBLIST_TYPES || op == TL_TEXT_PEERMSG_TYPE || op == TL_TEXT_GET_EXTRA_MASK) {
    return default_random_forward ();
  } else {
    return default_firstint_forward ();
  }
}

SCHEMA_ADD(text) {
  if (C->methods.forward) {
    return -1;
  }
  C->methods.forward = text_forward;
  return 0;
}

SCHEMA_REGISTER(text,0)
