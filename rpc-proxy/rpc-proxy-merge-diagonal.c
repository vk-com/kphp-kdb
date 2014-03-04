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
              2012-2013 Vitaliy Valtman
*/

#include "vv-tl-parse.h"
#include "rpc-proxy.h"
#include "rpc-proxy-merge.h"

void *rpc_proxy_diagonal_on_start (void) {
  int *extra = malloc (3 * sizeof (int));
  extra[0] = 0;
  extra[1] = 0;
  extra[2] = 0;
  return extra;
}

void rpc_proxy_diagonal_on_answer (struct gather *G, int num) {
  int *extra = G->extra;
  if (tl_fetch_int () != TL_BOOL_STAT) {
    extra[2] ++;
    return;
  }
  if (tl_fetch_unread () != 12) {
    extra[2] ++;
    return;
  }
  extra[0] += tl_fetch_int ();
  extra[1] += tl_fetch_int ();
  extra[2] += tl_fetch_int ();
}

void rpc_proxy_diagonal_on_error (struct gather *G, int num) {
  int error_code = tl_fetch_lookup_int ();
  if (TL_IS_USER_ERROR (error_code)) {
    if (merge_init_response (G) >= 0) {
      tl_store_header (CQ->h);
      tl_copy_through (tl_fetch_unread (), 1);
      tl_store_end ();
    }
    free (G->extra);
    merge_terminate_gather (G);
  } else {
    int *extra = G->extra;
    extra[2] ++;
  }
}

void rpc_proxy_diagonal_on_end (struct gather *G) {
  int *extra = G->extra;
  if (merge_init_response (G) >= 0) {
    extra[2] += G->timeouted_num + G->not_sent_num;
    tl_store_int (TL_BOOL_STAT);
    tl_store_int (extra[0]);
    tl_store_int (extra[1]);
    tl_store_int (extra[2]);
    tl_store_end ();
  }
  free (extra);
  merge_delete (G);
}

struct gather_methods diagonal_gather_methods = {
  .on_start = rpc_proxy_diagonal_on_start,
  .on_send = 0,
  .on_error = rpc_proxy_diagonal_on_error,
  .on_answer = rpc_proxy_diagonal_on_answer,
  .on_timeout = 0,
  .on_end = rpc_proxy_diagonal_on_end,
  .on_send_end = 0
};
