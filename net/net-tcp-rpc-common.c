/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
                   2013 Vitaliy Valtman
*/

#include <sys/uio.h>
#include "net-msg.h"
#include "net-tcp-connections.h"
#include "net-tcp-rpc-common.h"

#include <stdio.h>
#include <assert.h>



// Flags:
//   Flag 1 - can not edit this message. Need to make copy.

void tcp_rpc_conn_send (struct connection *c, struct raw_message *raw, int flags) {
  vkprintf (3, "%s: sending message of size %d to conn fd=%d\n", __func__, raw->total_bytes, c->fd);
  assert (!(raw->total_bytes & 3));
  int Q[2];
  Q[0] = raw->total_bytes + 12;
  Q[1] = TCP_RPC_DATA(c)->out_packet_num ++;
  struct raw_message r;
  if (flags & 1) {
    rwm_clone (&r, raw);
  } else {
    r = *raw;
  }
  rwm_push_data_front (&r, Q, 8);
  unsigned crc32 = rwm_crc32 (&r, r.total_bytes);
  rwm_push_data (&r, &crc32, 4);
  rwm_union (&c->out, &r);
}

void tcp_rpc_conn_send_data (struct connection *c, int len, void *Q) {
  assert (!(len & 3));
  struct raw_message r;
  assert (rwm_create (&r, Q, len) == len);
  tcp_rpc_conn_send (c, &r, 0);
}
