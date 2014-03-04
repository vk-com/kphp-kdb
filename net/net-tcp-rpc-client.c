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

#define        _FILE_OFFSET_BITS        64

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
#include "net-tcp-connections.h"
#include "net-tcp-rpc-common.h"
#include "net-tcp-rpc-client.h"

#include "rpc-const.h"
#include "net-crypto-aes.h"


/*
 *
 *                BASIC RPC CLIENT INTERFACE
 *
 */

extern int verbosity;
extern int rpc_disable_crc32_check;

int tcp_rpcc_parse_execute (struct connection *c);
int tcp_rpcc_compact_parse_execute (struct connection *c);
int tcp_rpcc_connected (struct connection *c);
int tcp_rpcc_connected_nohs (struct connection *c);
int tcp_rpcc_close_connection (struct connection *c, int who);
int tcp_rpcc_init_outbound (struct connection *c);
int tcp_rpc_client_check_ready (struct connection *c);
void tcp_rpcc_flush_crypto (struct connection *c);
int tcp_rpcc_flush (struct connection *c);
int tcp_rpcc_flush_packet (struct connection *c);
int tcp_rpcc_default_check_perm (struct connection *c);
int tcp_rpcc_init_crypto (struct connection *c);
int tcp_rpcc_start_crypto (struct connection *c, char *nonce, int key_select);


conn_type_t ct_tcp_rpc_client = {
  .magic = CONN_FUNC_MAGIC,
  .title = "rpc_client",
  .accept = server_failed,
  .init_accepted = server_failed,
  .run = server_read_write,
  .reader = tcp_server_reader,
  .writer = tcp_server_writer,
  .parse_execute = tcp_rpcc_parse_execute,
  .close = tcp_rpcc_close_connection,
  .free_buffers = tcp_free_connection_buffers,
  .init_outbound = tcp_rpcc_init_outbound,
  .connected = tcp_rpcc_connected,
  .wakeup = server_noop,
  .check_ready = tcp_rpc_client_check_ready,
  .flush = tcp_rpcc_flush,
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = tcp_aes_crypto_encrypt_output,
  .crypto_decrypt_input = tcp_aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = tcp_aes_crypto_needed_output_bytes,
  .flags = C_RAWMSG,
};

int tcp_rpcc_default_execute (struct connection *c, int op, struct raw_message *raw);

struct tcp_rpc_client_functions default_tcp_rpc_client = {
  .execute = tcp_rpcc_default_execute,
  .check_ready = server_check_ready,
  .flush_packet = tcp_rpcc_flush_packet,
  .rpc_check_perm = tcp_rpcc_default_check_perm,
  .rpc_init_crypto = tcp_rpcc_init_crypto,
  .rpc_start_crypto = tcp_rpcc_start_crypto,
  .rpc_ready = server_noop,
};

int tcp_rpcc_default_execute (struct connection *c, int op, struct raw_message *raw) {
  vkprintf (1, "rpcc_execute: fd=%d, op=%d, len=%d\n", c->fd, op, raw->total_bytes);
  if (op == RPC_PING && raw->total_bytes == 12) {
    c->last_response_time = precise_now;    
    static int Q[12];
    assert (rwm_fetch_data (raw, Q, 12) == 12);
    static int P[12];
    P[0] = RPC_PONG;    
    P[1] = Q[1];
    P[2] = Q[2];
    tcp_rpc_conn_send_data (c, 12, P);
    return 0;
  }
  return 0;
}

static int tcp_rpcc_process_nonce_packet (struct connection *c, struct raw_message *msg) {
  struct tcp_rpc_data *D = TCP_RPC_DATA(c);
  static struct tcp_rpc_nonce_packet P;
  int res;

  if (D->packet_num != -2 || D->packet_type != RPC_NONCE) {
    return -2;
  }
  if (D->packet_len != sizeof (struct tcp_rpc_nonce_packet)) {
    return -3;
  }

  assert (rwm_fetch_data (msg, &P, D->packet_len) == D->packet_len);

  vkprintf (2, "Processing nonce packet, crypto schema: %d, key select: %d\n", P.crypto_schema, P.key_select);

  switch (P.crypto_schema) {
  case RPC_CRYPTO_NONE:
    if (P.key_select) {
      return -3;
    }
    if (D->crypto_flags & 1) {
      if (D->crypto_flags & 2) {
        assert (!c->out_p.total_bytes);
      }
      D->crypto_flags = 1;
    } else {
      return -5;
    }
    break;
  case RPC_CRYPTO_AES:
    if (!P.key_select || P.key_select != get_crypto_key_id ()) {
      return -3;
    }
    if (!(D->crypto_flags & 2)) {
      return -5;
    }
    if (abs (P.crypto_ts - D->nonce_time) > 30) {
      return -6;        //less'om
    }
    res = TCP_RPCC_FUNC(c)->rpc_start_crypto (c, P.crypto_nonce, P.key_select);
    if (res < 0) {
      return -6;
    }
    break;
  default:
    return -4;
  }
  vkprintf (2, "Processed nonce packet, crypto flags = %d\n", D->crypto_flags);
  return 0;
}

static int tcp_rpcc_send_handshake_packet (struct connection *c) {
  struct tcp_rpc_data *D = TCP_RPC_DATA (c);
  static struct tcp_rpc_handshake_packet P;
  if (!PID.ip) {
    init_client_PID (c->our_ip);
    if (!PID.ip) {
      PID.ip = get_my_ipv4 ();
    }
  }
  memset (&P, 0, sizeof (P));
  P.type = RPC_HANDSHAKE;
  P.flags = 0;
  if (!D->remote_pid.port) {
    D->remote_pid.ip = (c->remote_ip == 0x7f000001 ? 0 : c->remote_ip);
    D->remote_pid.port = c->remote_port;
  }
  memcpy (&P.sender_pid, &PID, sizeof (struct process_id));
  memcpy (&P.peer_pid, &D->remote_pid, sizeof (struct process_id));
  
  tcp_rpc_conn_send_data (c, sizeof (P), &P);
  TCP_RPCC_FUNC(c)->flush_packet (c);

  return 0;
}

static int tcp_rpcc_send_handshake_error_packet (struct connection *c, int error_code) {
  static struct tcp_rpc_handshake_error_packet P;
  if (!PID.pid) {
    init_client_PID (c->our_ip);
  }
  memset (&P, 0, sizeof (P));
  P.type = RPC_HANDSHAKE_ERROR;
  P.error_code = error_code;
  memcpy (&P.sender_pid, &PID, sizeof (PID));
  tcp_rpc_conn_send_data (c, sizeof (P), &P);
  TCP_RPCC_FUNC(c)->flush_packet (c);

  return 0;
}

static int tcp_rpcc_process_handshake_packet (struct connection *c, struct raw_message *msg) {
  struct tcp_rpc_data *D = TCP_RPC_DATA(c);
  static struct tcp_rpc_handshake_packet P;
  if (D->packet_num != -1 || D->packet_type != RPC_HANDSHAKE) {
    return -2;
  }
  if (D->packet_len != sizeof (struct tcp_rpc_handshake_packet)) {
    tcp_rpcc_send_handshake_error_packet (c, -3);
    return -3;
  }
  assert (rwm_fetch_data (msg, &P, D->packet_len) == D->packet_len);  
  if (!matches_pid (&P.sender_pid, &D->remote_pid)) {
    tcp_rpcc_send_handshake_error_packet (c, -6);
    return -6;
  }
  if (!P.sender_pid.ip) {
    P.sender_pid.ip = D->remote_pid.ip;
  }
  memcpy (&D->remote_pid, &P.sender_pid, sizeof (struct process_id));
  if (!matches_pid (&PID, &P.peer_pid)) {
    tcp_rpcc_send_handshake_error_packet (c, -4);
    return -4;
  }
  return 0;
}

int tcp_rpcc_parse_execute (struct connection *c) {
  vkprintf (4, "%s. in_total_bytes = %d\n", __func__, c->in.total_bytes);  
  struct tcp_rpc_data *D = TCP_RPC_DATA(c);
  int len;

  while (1) {
    len = c->in.total_bytes; 
    if (len <= 0) {
      break;
    }
    if (!D->packet_len) {
      if (len < 4) {
        c->status = conn_reading_answer;
        return 4 - len;
      }
      assert (rwm_fetch_lookup (&c->in, &D->packet_len, 4) == 4);
      if (D->packet_len <= 0 || (D->packet_len & 3) || (D->packet_len > TCP_RPCC_FUNC(c)->max_packet_len && TCP_RPCC_FUNC(c)->max_packet_len > 0)) {
        vkprintf (1, "error while parsing packet: bad packet length %d\n", D->packet_len);
        c->status = conn_error;
        c->error = -1;
        return 0;
      }
    }
    if (D->packet_len == 4) {
      assert (rwm_fetch_data (&c->in, 0, 4) == 4);
      D->packet_len = 0;
      continue;
    }
    if (D->packet_len < 16) {
      vkprintf (1, "error while parsing packet: bad packet length %d\n", D->packet_len);
      c->status = conn_error;
      c->error = -1;
      return 0;
    }
    if (len < D->packet_len) {
      c->status = conn_reading_answer;
      return D->packet_len - len;
    }
    

    struct raw_message msg;
    if (c->in.total_bytes == D->packet_len) {
      msg = c->in;
      rwm_init (&c->in, 0);
    } else {
      rwm_split_head (&msg, &c->in, D->packet_len);
    }

    unsigned crc32;
    assert (rwm_fetch_data_back (&msg, &crc32, 4) == 4);
    if (!rpc_disable_crc32_check) {
      D->packet_crc32 = rwm_crc32 (&msg, D->packet_len - 4);
      if (crc32 != D->packet_crc32) {
        if (verbosity > 0) {
          fprintf (stderr, "error while parsing packet: crc32 = %08x != %08x\n", D->packet_crc32, crc32);
        }
        c->status = conn_error;
        c->error = -1;
        return 0;
      }
    }

    assert (rwm_fetch_data (&msg, 0, 4) == 4);
    assert (rwm_fetch_data (&msg, &D->packet_num, 4) == 4);
    assert (rwm_fetch_lookup (&msg, &D->packet_type, 4) == 4);
    D->packet_len -= 12;

    if (verbosity > 2) {
      fprintf (stderr, "received packet from connection %d\n", c->fd);
      rwm_dump (&msg);
    }


    int res = -1;

    if (D->packet_num != D->in_packet_num) {
      vkprintf (1, "error while parsing packet: got packet num %d, expected %d\n", D->packet_num, D->in_packet_num);
      c->status = conn_error;
      c->error = -1;
      return 0;
    } else if (D->packet_num < 0) {
      /* this is for us */
      if (D->packet_num == -2) {
        c->status = conn_running;
        res = tcp_rpcc_process_nonce_packet (c, &msg);
        if (res >= 0) {
          res = tcp_rpcc_send_handshake_packet (c);
          //fprintf (stderr, "send_handshake_packet returned %d\n", res);
        }
      } else if (D->packet_num == -1) {
        c->status = conn_running;
        res = tcp_rpcc_process_handshake_packet (c, &msg);
        if (res >= 0 && TCP_RPCC_FUNC(c)->rpc_ready) {
          res = TCP_RPCC_FUNC(c)->rpc_ready (c);
        }
      }
      rwm_free (&msg);
      if (res < 0) {
        c->status = conn_error;
        c->error = res;
        return 0;
      }
    } else {
      /* main case */
      c->status = conn_running;
      if (D->packet_type == RPC_PING) {
        res = tcp_rpcc_default_execute (c, D->packet_type, &msg);
      } else {
        res = TCP_RPCC_FUNC(c)->execute (c, D->packet_type, &msg);
      }
      if (res <= 0) {
        rwm_free (&msg);
      }
    }

    if (c->status == conn_error) {
      if (!c->error) {
        c->error = -2;
      }
      return 0;
    }
    
    D->in_packet_num++;
    D->packet_len = 0;
    if (c->status == conn_running) {
      c->status = conn_wait_answer;
    }
    if (c->status != conn_wait_answer) {
      break;
    }
  }
  return 0;
}



int tcp_rpcc_connected (struct connection *c) {
  TCP_RPC_DATA(c)->out_packet_num = -2;
  c->last_query_sent_time = precise_now;

  if (TCP_RPCC_FUNC(c)->rpc_check_perm) {
    int res = TCP_RPCC_FUNC(c)->rpc_check_perm (c);
    if (res < 0) {
      return res;
    }
    if (!(res &= 3)) {
      return -1;
    }
    TCP_RPC_DATA(c)->crypto_flags = res;
  } else {
    TCP_RPC_DATA(c)->crypto_flags = 3;
  }
  vkprintf (2, "RPC connection #%d: [%s]:%d -> [%s]:%d connected, crypto_flags = %d\n", c->fd, show_our_ip (c), c->our_port, show_remote_ip (c), c->remote_port, TCP_RPC_DATA(c)->crypto_flags);

  assert (TCP_RPCC_FUNC(c)->rpc_init_crypto);
  int res = TCP_RPCC_FUNC(c)->rpc_init_crypto (c);

  if (res > 0) {
    assert (TCP_RPC_DATA(c)->crypto_flags & 4);
  } else {
    return -1;
  }

  assert (TCP_RPCC_FUNC(c)->flush_packet);
  TCP_RPCC_FUNC(c)->flush_packet (c);

  return 0;
}


int tcp_rpcc_close_connection (struct connection *c, int who) {
  if (TCP_RPCC_FUNC(c)->rpc_close != NULL) {
    TCP_RPCC_FUNC(c)->rpc_close (c, who);
  }

  return client_close_connection (c, who);
}


int tcp_rpc_client_check_ready (struct connection *c) {
  return TCP_RPCC_FUNC(c)->check_ready (c);
}


int tcp_rpcc_init_fake_crypto (struct connection *c) {
  if (!(TCP_RPC_DATA(c)->crypto_flags & 1)) {
    return -1;
  }

  static struct tcp_rpc_nonce_packet buf;
  memset (&buf, 0, sizeof (buf));
  buf.type = RPC_NONCE;
  buf.crypto_schema = RPC_CRYPTO_NONE;

  tcp_rpc_conn_send_data (c, sizeof (buf), &buf);

  assert ((TCP_RPC_DATA(c)->crypto_flags & 14) == 0);
  TCP_RPC_DATA(c)->crypto_flags |= 4;
 
  return 1;
}


int tcp_rpcc_init_outbound (struct connection *c) {
  vkprintf (3, "rpcc_init_outbound (%d)\n", c->fd);
  struct tcp_rpc_data *D = TCP_RPC_DATA(c);
  c->last_query_sent_time = precise_now;

  if (TCP_RPCC_FUNC(c)->rpc_check_perm) {
    int res = TCP_RPCC_FUNC(c)->rpc_check_perm (c);
    if (res < 0) {
      return res;
    }
    if (!(res &= 3)) {
      return -1;
    }

    D->crypto_flags = res;
  } else {
    D->crypto_flags = 1;
  }

  D->in_packet_num = -2;

  return 0;
}

int aes_initialized;

int tcp_rpcc_default_check_perm (struct connection *c) {
  int ipxor = -1, mask = -1;
  if (c->flags & C_IPV6) {
    if (is_ipv6_localhost (c->our_ipv6) && is_ipv6_localhost (c->remote_ipv6)) {
      ipxor = 0;
    } else if (*((int *)(c->our_ipv6)) != *((int *)(c->remote_ipv6))) {
      return 0;
    } else {
      ipxor = ((int *)(c->our_ipv6))[1] ^ ((int *)(c->remote_ipv6))[1];
      mask = 0xffff;
    }
  } else {
    if ((c->remote_ip & 0xff000000) != 0x0a000000 && (c->remote_ip & 0xff000000) != 0x7f000000) {
      return 0;
    }
    ipxor = (c->our_ip ^ c->remote_ip);
    mask = 0xffff0000;
  }
  if (aes_initialized <= 0) {
    return (ipxor & mask) ? 0 : 1;
  }
  return (ipxor & mask) ? 2 : 3;
}

int tcp_rpcc_init_crypto (struct connection *c) {
  if (!(TCP_RPC_DATA(c)->crypto_flags & 2)) {
    return tcp_rpcc_init_fake_crypto (c);
  }

  TCP_RPC_DATA(c)->nonce_time = time (0);

  aes_generate_nonce (TCP_RPC_DATA(c)->nonce);

  static struct tcp_rpc_nonce_packet buf;
  memset (&buf, 0, sizeof (buf));
  memcpy (buf.crypto_nonce, TCP_RPC_DATA(c)->nonce, 16);
  buf.crypto_ts = TCP_RPC_DATA(c)->nonce_time;
  buf.type = RPC_NONCE;
  buf.key_select = get_crypto_key_id ();
  buf.crypto_schema = RPC_CRYPTO_AES;

  tcp_rpc_conn_send_data (c, sizeof (buf), &buf);

  assert ((TCP_RPC_DATA(c)->crypto_flags & 14) == 2);
  TCP_RPC_DATA(c)->crypto_flags |= 4;

  assert (!c->crypto);

/*  struct raw_message x;
  assert (!c->out_p.total_bytes);
  x = c->out_p;
  c->out_p = c->out;
  c->out = x;*/

  return 1;
}

int tcp_rpcc_start_crypto (struct connection *c, char *nonce, int key_select) {
  struct tcp_rpc_data *D = TCP_RPC_DATA(c);

  vkprintf (2, "rpcc_start_crypto: key_select = %d\n", key_select);

  if (c->crypto) {
    return -1;
  }

  if (c->in.total_bytes || c->out.total_bytes || !D->nonce_time) {
    return -1;
  }

  if (!key_select || key_select != get_crypto_key_id ()) {
    return -1;
  }

  struct aes_key_data aes_keys;

  if (aes_create_keys (&aes_keys, 1, nonce, D->nonce, D->nonce_time, c->remote_ip, c->remote_port, c->remote_ipv6, c->our_ip, c->our_port, c->our_ipv6) < 0) {
    return -1;
  }

  if (aes_crypto_init (c, &aes_keys, sizeof (aes_keys)) < 0) {
    return -1;
  }

  return 1;
}

void tcp_rpcc_flush_crypto (struct connection *c) {
  if (c->crypto) {
    int pad_bytes = c->type->crypto_needed_output_bytes (c);
    vkprintf (2, "rpcc_flush_packet: padding with %d bytes\n", pad_bytes);
    if (pad_bytes > 0) {
      assert (!(pad_bytes & 3));
      static int pad_str[3] = {4, 4, 4};
      assert (pad_bytes <= 12);
      assert (rwm_push_data (&c->out, pad_str, pad_bytes) == pad_bytes);
    }
  }
}

int tcp_rpcc_flush_packet (struct connection *c) {
  tcp_rpcc_flush_crypto (c);
  return flush_connection_output (c);
}

int tcp_rpcc_flush_packet_later (struct connection *c) {
  tcp_rpcc_flush_crypto (c);
  return flush_later (c);
}

int tcp_rpcc_flush (struct connection *c) {
  if (c->crypto) {
    int pad_bytes = c->type->crypto_needed_output_bytes (c);
    vkprintf (2, "rpcs_flush: padding with %d bytes\n", pad_bytes);
    if (pad_bytes > 0) {
      assert (!(pad_bytes & 3));
      static int pad_str[3] = {4, 4, 4};
      assert (pad_bytes <= 12);
      assert (rwm_push_data (&c->out, pad_str, pad_bytes) == pad_bytes);
    }
    return pad_bytes;
  }
  return 0;
}

/*
 *
 *                END (BASIC RPC CLIENT)
 *
 */


