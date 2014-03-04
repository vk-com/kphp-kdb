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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>

#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-rpc-common.h"
#include "rpc-const.h"

#ifdef AES
# include "net-crypto-aes.h"
#endif


/*
 *
 *		BASIC COMMON RPC SERVER & CLIENT CODE
 *
 */

extern int verbosity;
int rpc_disable_crc32_check;

void dump_rpc_packet (int *packet) {
  int len = (*packet + 3) >> 2, i;
  for (i = 0; i < len; i++) {
    fprintf (stderr, "%08x ", packet[i]);
    if ((i & 3) == 3) {
      fprintf (stderr, "\n");
    }
  }
  fprintf (stderr, "\n");
}

void dump_next_rpc_packet_limit (struct connection *c, int max_ints) {
  struct nb_iterator it;
  int i = 0, len = 4, x;
  nbit_set (&it, &c->In);
  while (i * 4 < len && i < max_ints) {
    assert (nbit_read_in (&it, &x, 4) == 4);
    if (!i) {
      len = x;
    }
    fprintf (stderr, "%08x ", x);
    if (!(++i & 7)) {
      fprintf (stderr, "\n");
    }
  }
  fprintf (stderr, "\n");
}

void dump_next_rpc_packet (struct connection *c) {
  dump_next_rpc_packet_limit (c, 1 << 29);
}



void tcp_rpc_conn_send_data (struct connection *c, int len, void *data) __attribute__ ((weak));
void tcp_rpc_conn_send_data (struct connection *c, int len, void *data) {
  assert (0);
}

void net_rpc_send_ping (struct connection *c, long long ping_id) {
  if (!(c->flags & C_RAWMSG)) {
    vkprintf (2, "Sending ping to fd=%d. ping_id = %lld\n", c->fd, ping_id);
    static int P[20];
    P[0] = 24;
    P[1] = ((struct rpcx_data *) ((c)->custom_data))->out_packet_num++;
    P[2] = RPC_PING;
    *(long long *)(P + 3) = ping_id;
    P[5] = compute_crc32 (P, 20);
    write_out (&c->Out, P, 24);
    flush_later (c);
  } else {
    static int P[20];
    P[0] = RPC_PING;
    *(long long *)(P + 1) = ping_id;
    tcp_rpc_conn_send_data (c, 12, P);
    flush_later (c);
  }
}

/*
 *
 *		END (BASIC COMMON RPC SERVER & CLIENT CODE)
 *
 */

