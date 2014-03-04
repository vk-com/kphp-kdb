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

#ifndef	__NET_RPC_COMMON__
#define	__NET_RPC_COMMON__

#include "net-connections.h"
#include "common/pid.h"

#pragma pack(push,4)

struct rpc_packet {
  int len;               /* total len in bytes, from len to crc32, inclusive, divisible by 4 */
                         /* len=4 is used for padding */
  int seq_num;           /* starts from zero, increments when each packet is sent, -1 = handshake packets */
  int type;              /* selects action */
  int data[0];
  /* ... int crc32 */    /* CRC32 of packet from len to the end of data */
};

struct rpc_nonce_packet {
  int len;
  int seq_num;           /* -1 */
  int type;              /* type = RPC_NONCE */
  int key_select;        /* least significant 32 bits of key to use */
  int crypto_schema;     /* 0 = NONE, 1 = AES */
  int crypto_ts;
  char crypto_nonce[16];
  int crc32;
};

struct rpc_handshake_packet {
  int len;
  int seq_num;
  int type;
  int flags;
  struct process_id sender_pid;
  struct process_id peer_pid;
  /* more ints? */
  int crc32;
};

struct rpc_handshake_error_packet {
  int len;
  int seq_num;
  int type;
  int error_code;
  struct process_id sender_pid;
  int crc32;
};

void dump_rpc_packet (int *packet);
void dump_next_rpc_packet (struct connection *c);
void dump_next_rpc_packet_limit (struct connection *c, int limit);

/* in conn->custom_data */
struct rpcx_data {
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

#define rpcs_data rpcx_data
#define rpcc_data rpcx_data

#define RPC_NONCE 0x7acb87aa
#define RPC_HANDSHAKE 0x7682eef5
#define RPC_HANDSHAKE_ERROR 0x6a27beda

#define RPC_DO		9
#define RPC_ANSWER	17

#define RPC_CRYPTO_NONE 0
#define RPC_CRYPTO_AES  1

#define	RPC_MF_COMPACT_ALLOW	1
#define	RPC_MF_COMPACT_FORCE	2

#define RPCX_DATA(c)    ((struct rpcx_data *) ((c)->custom_data))
#define RPCX_COMPACT(c)	(RPCX_DATA(c)->flags & 0x40000000)

void net_rpc_send_ping (struct connection *c, long long ping_id);
#pragma pack(pop)

#endif
