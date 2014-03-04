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
              2011-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include "net-rpc-server.h"
#include "copyexec-rpc.h"
#include "server-functions.h"

struct connection *get_target_connection (struct conn_target *targ) {
  struct connection *c;
  if (!targ->outbound_connections) {
    return 0;
  }
  c = targ->first_conn;
  while (1) {
    if (server_check_ready (c) == cr_ok) {
      return c;
    }
    if (c == targ->last_conn) { break;}
    c = c->next;
  }
  return 0;
}

/********** rpc send/receive routines **********/

void *rpc_create_query (void *_R, int len, struct connection *c, int op) {
  len = (len + 3) & -4;
  vkprintf (4, "creating query... len = %d, op = %x\n", len, op);
  assert (len + 16 <= MAX_PACKET_LEN && len >= 0);
  if (!c || server_check_ready (c) != cr_ok) {
    vkprintf (4, "not_created: connection_failed\n");
    return 0;
  }
  int *R = _R;
  R[0] = len + 16;
  R[1] = RPCS_DATA(c)->out_packet_num++;
  R[2] = op;
  return R + 3;
}

int rpc_send_query (void *_R, struct connection *c) {
  int *R = _R;
  vkprintf (4, "sending query... len = %d, op = %x\n", R[0], R[2]);
  vkprintf (6, "c = %p, server_check_ready = %d (cr_ok = %d)\n", c, server_check_ready (c), cr_ok);
  assert (c && server_check_ready (c) == cr_ok);
  assert (R[0] <= MAX_PACKET_LEN && R[0] >= 16 && R[0] % 4 == 0);
  R[(R[0] >> 2) - 1] = compute_crc32 (R, R[0] - 4);
  write_out (&c->Out, R, R[0]);
  RPCS_FUNC(c)->flush_packet (c);
  vkprintf (4, "message_sent\n");
  return 0;
}

