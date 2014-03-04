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
              2012-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <assert.h>
#include "net-rpc-server.h"
#include "storage-rpc.h"
#include "server-functions.h"

#define PACKET_BUFFER_SIZE (MAX_PACKET_LEN / 4)
static int packet_buffer[PACKET_BUFFER_SIZE+1], *packet_ptr, *packet_end;

void rpc_readin (struct connection *c, int len) {
  assert (read_in (&c->In, packet_buffer, len) == len);
  packet_ptr = packet_buffer;
  packet_end = packet_buffer + (len >> 2);
}

int rpc_fetch_ints (int *a, int len) {
  if (packet_ptr + len > packet_end) {
    return -1;
  }
  memcpy (a, packet_ptr, len * 4);
  packet_ptr += len;
  return 0;
}

int rpc_end_of_fetch (void) {
  return packet_ptr == packet_end - 1;
}

void rpc_clear_packet (int reserve_space_for_op) {
  packet_ptr = packet_buffer + 2;
  if (reserve_space_for_op) {
    packet_ptr++;
  }
}

void rpc_out_ints (int *what, int len) {
  assert (packet_ptr + len <= packet_buffer + PACKET_BUFFER_SIZE);
  memcpy (packet_ptr, what, len * 4);
  packet_ptr += len;
}

void rpc_out_int (int x) {
  vkprintf (4, "rpc_out_int (%d)\n", x);
  assert (packet_ptr + 1 <= packet_buffer + PACKET_BUFFER_SIZE);
  *packet_ptr++ = x;
}

void rpc_out_long (long long x) {
  vkprintf (4, "rpc_out_long (%lld)\n", x);
  assert (packet_ptr + 2 <= packet_buffer + PACKET_BUFFER_SIZE);
  *(long long *)packet_ptr = x;
  packet_ptr += 2;
}

void rpc_out_string (const char *str) {
  rpc_out_cstring (str, strlen (str));
}

void rpc_out_cstring (const char *str, long len) {
  vkprintf (4, "rpc_out_cstring (len: %lld)\n", (long long) len);
  assert (len >= 0 && len < (1 << 24));
  assert ((char *) packet_ptr + len + 8 < (char *) (packet_buffer + PACKET_BUFFER_SIZE));
  char *dest = (char *) packet_ptr;
  if (len < 254) {
    *dest++ = len;
  } else {
    *packet_ptr = (len << 8) + 0xfe;
    dest += 4;
  }
  memcpy (dest, str, len);
  dest += len;
  while ((long) dest & 3) {
    *dest++ = 0;
  }
  packet_ptr = (int *) dest;
}

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

int rpc_send_error (struct connection *c, long long req_id, int error_code, char *format, ...) {
  char error_message[1024];
  va_list ap;
  va_start (ap, format);
  assert (vsnprintf (error_message, sizeof (error_message), format, ap) < sizeof (error_message));
  va_end (ap);
  vkprintf (2, "%s: (req_id: %lld, code: %d, msg: \"%s\")\n", __func__, req_id, error_code, error_message);

  rpc_clear_packet (1);
  rpc_out_long (req_id);
  rpc_out_int (error_code);
  rpc_out_string (error_message);
  rpc_send_packet (c, RPC_REQ_ERROR);
  return 0;
}

/********** rpc send/receive routines **********/

/*
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
*/

int rpc_send_packet (struct connection *c, int op) {
  if (!c) {
    return 0;
  }
  int len = 4 * (packet_ptr - packet_buffer);
  int *R = packet_buffer;
  R[0] = len + 4;
  R[1] = RPCS_DATA(c)->out_packet_num++;
  if (op) {
    R[2] = op;
  }

  vkprintf (4, "sending query... len = %d, op = 0x%x\n", R[0], R[2]);
  vkprintf (6, "c = %p, server_check_ready = %d (cr_ok = %d)\n", c, server_check_ready (c), cr_ok);
  if (server_check_ready (c) != cr_ok) {
    vkprintf (2, "%s: not_created (connection %d failed).\n", __func__, c->fd);
    return 0;
  }
  assert (c && server_check_ready (c) == cr_ok);
  assert (R[0] <= MAX_PACKET_LEN && R[0] >= 16 && R[0] % 4 == 0);
  R[(R[0] >> 2) - 1] = compute_crc32 (R, R[0] - 4);
  write_out (&c->Out, R, R[0]);
  RPCS_FUNC(c)->flush_packet (c);
  if (verbosity >= 2) {
    kprintf ("%s: packet sent <req_id = %lld>, c->fd: %d, len: %d, op:0x%x, code: 0x%x.\n", __func__, *((long long *) (&R[3])), c->fd, R[0], R[2], R[5]);
    if (verbosity >= 4) {
      dump_rpc_packet (R);
    }
  }
  vkprintf (4, "message_sent\n");
  return 0;
}

