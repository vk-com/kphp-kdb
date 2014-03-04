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

#ifndef __NET_TCP_RPC_COMMON_H__
#define __NET_TCP_RPC_COMMON_H__

#include "pid.h"

struct tcp_message {
  struct connection *c;
  int op;
  int packet_num;
  struct raw_message raw;
};

#pragma pack(push,4)
struct tcp_rpc_nonce_packet {
  int type;
  int key_select;        /* least significant 32 bits of key to use */
  int crypto_schema;     /* 0 = NONE, 1 = AES */
  int crypto_ts;
  char crypto_nonce[16];
};

struct tcp_rpc_handshake_packet {
  int type;
  int flags;
  struct process_id sender_pid;
  struct process_id peer_pid;
  /* more ints? */
};

struct tcp_rpc_handshake_error_packet {
  int type;
  int error_code;
  struct process_id sender_pid;
};
#pragma pack(pop)

void tcp_rpc_conn_send (struct connection *c, struct raw_message *raw, int flags);
void tcp_rpc_conn_send_data (struct connection *c, int len, void *Q);

/* in conn->custom_data */
struct tcp_rpc_data {
  int packet_len;
  int packet_num;
  int packet_type;
  int packet_crc32;
  int flags;
  int in_packet_num;
  int out_packet_num;
  int crypto_flags;					/* 1 = allow unencrypted, 2 = allow encrypted, 4 = DELETE sent, waiting for NONCE/NOT_FOUND, 8 = encryption ON, 256 = packet numbers not sequential, 512 = allow quick ack packets, 1024 = compact mode off */
  struct process_id remote_pid;
  char nonce[16];
  int nonce_time;
  union {
    void *user_data;
    void *extra;
  };
  int extra_int;
  int extra_int2;
  int extra_int3;
  int extra_int4;
  double extra_double, extra_double2;
};
#define RPC_NONCE 0x7acb87aa
#define RPC_HANDSHAKE 0x7682eef5
#define RPC_HANDSHAKE_ERROR 0x6a27beda

#define RPC_DO		9
#define RPC_ANSWER	17

#define RPC_CRYPTO_NONE 0
#define RPC_CRYPTO_AES  1

#define	RPC_MF_COMPACT_ALLOW	1
#define	RPC_MF_COMPACT_FORCE	2

#define	TCP_RPC_DATA(c)	((struct tcp_rpc_data *) ((c)->custom_data))
#endif

