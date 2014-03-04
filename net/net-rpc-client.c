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
#include "net-rpc-client.h"

#include "rpc-const.h"

#ifdef AES
# include "net-crypto-aes.h"
#endif


/*
 *
 *		BASIC RPC CLIENT INTERFACE
 *
 */

extern int verbosity;
extern int rpc_disable_crc32_check;

int rpcc_parse_execute (struct connection *c);
int rpcc_compact_parse_execute (struct connection *c);
int rpcc_connected (struct connection *c);
int rpcc_connected_nohs (struct connection *c);


conn_type_t ct_rpc_client = {
  .magic = CONN_FUNC_MAGIC,
  .title = "rpc_client",
  .accept = server_failed,
  .init_accepted = server_failed,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = rpcc_parse_execute,
  .close = rpcc_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = rpcc_init_outbound,
  .connected = rpcc_connected,
  .wakeup = server_noop,
  .check_ready = rpc_client_check_ready,
  .flush = rpcc_flush,
#ifdef AES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

conn_type_t ct_rpc_ext_client = {
  .magic = CONN_FUNC_MAGIC,
  .title = "ext_rpc_client",
  .accept = server_failed,
  .init_accepted = server_failed,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = rpcc_compact_parse_execute,
  .close = client_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = rpcc_init_outbound_nohs,
  .connected = rpcc_connected_nohs,
  .wakeup = server_noop,
  .check_ready = rpc_client_check_ready,
  .flush = rpcc_flush,
};

int rpcc_default_execute (struct connection *c, int op, int len);

struct rpc_client_functions default_rpc_client = {
  .execute = rpcc_default_execute,
  .check_ready = rpcc_default_check_ready,
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = server_noop,
};


int rpcc_default_execute (struct connection *c, int op, int len) {
  vkprintf (1, "rpcc_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);
  if (op == RPC_PING && len == 24) {
    c->last_response_time = precise_now;
    static int Q[12];
    assert (read_in (&c->In, Q, 24) == 24);
    static int P[12];
    P[0] = 24;
    P[1] = RPCC_DATA(c)->out_packet_num++;
    P[2] = RPC_PONG;    
    P[3] = Q[3];
    P[4] = Q[4];
    P[5] = compute_crc32 (P, 20);
    vkprintf (1, "Received ping from fd=%d. ping_id = %lld. Sending pong\n", c->fd, *(long long *)(Q + 3));
    write_out (&c->Out, P, 24);
    RPCC_FUNC(c)->flush_packet(c);
    return 24;
  }
  return SKIP_ALL_BYTES;
}

int rpcc_default_check_ready (struct connection *c) {
  if ((c->flags & C_FAILED) || c->status == conn_error) {
    return c->ready = cr_failed;
  }

  const double CONNECT_TIMEOUT = 3.0;
  if (c->status == conn_connecting || RPCC_DATA(c)->in_packet_num < 0) {
    if (RPCC_DATA(c)->in_packet_num == -1 && c->status == conn_running) {
      return c->ready = cr_ok;
    }

    assert (c->last_query_sent_time != 0);
    if (c->last_query_sent_time < precise_now - CONNECT_TIMEOUT) {
      fail_connection (c, -6);
      return c->ready = cr_failed;
    }
    return c->ready = cr_notyet;
  }
    
  if (c->status == conn_wait_answer || c->status == conn_reading_answer || c->status == conn_expect_query) {
    return c->ready = cr_ok;
  }

  return c->ready = cr_failed;
}

static int rpcc_process_nonce_packet (struct connection *c) {
  struct rpcc_data *D = RPCC_DATA(c);
  static struct rpc_nonce_packet P;
  int res;

  if (D->packet_num != -2 || D->packet_type != RPC_NONCE) {
    return -2;
  }
  if (D->packet_len != sizeof (struct rpc_nonce_packet)) {
    return -3;
  }
  assert (read_in (&c->In, &P, D->packet_len) == D->packet_len);

  vkprintf (2, "Processing nonce packet, crypto schema: %d, key select: %d\n", P.crypto_schema, P.key_select);

  switch (P.crypto_schema) {
  case RPC_CRYPTO_NONE:
    if (P.key_select) {
      return -3;
    }
    if (D->crypto_flags & 1) {
      #ifdef AES
      if (D->crypto_flags & 2) {
        release_all_unprocessed (&c->Out);
      }
      #endif
      D->crypto_flags = 1;
    } else {
      return -5;
    }
    break;
#ifdef AES
  case RPC_CRYPTO_AES:
    if (!P.key_select || P.key_select != get_crypto_key_id ()) {
      return -3;
    }
    if (!(D->crypto_flags & 2)) {
      return -5;
    }
    if (abs (P.crypto_ts - D->nonce_time) > 30) {
      return -6;	//less'om
    }
    res = RPCC_FUNC(c)->rpc_start_crypto (c, P.crypto_nonce, P.key_select);
    if (res < 0) {
      return -6;
    }
    break;
#endif
  default:
    return -4;
  }
  vkprintf (2, "Processed nonce packet, crypto flags = %d\n", D->crypto_flags);
  return 0;
}

static int rpcc_send_handshake_packet (struct connection *c) {
  struct rpcc_data *D = RPCC_DATA (c);
  static struct rpc_handshake_packet P;
  if (!PID.ip) {
    init_client_PID (c->our_ip);
    if (!PID.ip) {
      PID.ip = get_my_ipv4 ();
    }
  }
  memset (&P, 0, sizeof (P));
  P.len = sizeof (P);
  P.seq_num = -1;
  P.type = RPC_HANDSHAKE;
  P.flags = 0;
  if (!D->remote_pid.port) {
    D->remote_pid.ip = (c->remote_ip == 0x7f000001 ? 0 : c->remote_ip);
    D->remote_pid.port = c->remote_port;
  }
  memcpy (&P.sender_pid, &PID, sizeof (struct process_id));
  memcpy (&P.peer_pid, &D->remote_pid, sizeof (struct process_id));
  P.crc32 = compute_crc32 (&P, sizeof (P) - 4);
  write_out (&c->Out, &P, sizeof (P));
  RPCC_FUNC(c)->flush_packet (c);

  return 0;
}

static int rpcc_send_handshake_error_packet (struct connection *c, int error_code) {
  static struct rpc_handshake_error_packet P;
  if (!PID.pid) {
    init_client_PID (c->our_ip);
  }
  memset (&P, 0, sizeof (P));
  P.len = sizeof (P);
  P.seq_num = -1;
  P.type = RPC_HANDSHAKE_ERROR;
  P.error_code = error_code;
  memcpy (&P.sender_pid, &PID, sizeof (PID));
  P.crc32 = compute_crc32 (&P, sizeof (P) - 4);
  write_out (&c->Out, &P, sizeof (P));
  RPCC_FUNC(c)->flush_packet (c);

  return 0;
}

static int rpcc_process_handshake_packet (struct connection *c) {
  struct rpcc_data *D = RPCC_DATA(c);
  static struct rpc_handshake_packet P;
  if (D->packet_num != -1 || D->packet_type != RPC_HANDSHAKE) {
    return -2;
  }
  if (D->packet_len != sizeof (struct rpc_handshake_packet)) {
    rpcc_send_handshake_error_packet (c, -3);
    return -3;
  }
  assert (read_in (&c->In, &P, D->packet_len) == D->packet_len);
  if (!matches_pid (&P.sender_pid, &D->remote_pid)) {
    rpcc_send_handshake_error_packet (c, -6);
    return -6;
  }
  if (!P.sender_pid.ip) {
    P.sender_pid.ip = D->remote_pid.ip;
  }
  memcpy (&D->remote_pid, &P.sender_pid, sizeof (struct process_id));
  if (!matches_pid (&PID, &P.peer_pid)) {
    rpcc_send_handshake_error_packet (c, -4);
    return -4;
  }
  return 0;
}

int rpcc_parse_execute (struct connection *c) {
  struct rpcc_data *D = RPCC_DATA(c);
  int len;

  while (1) {
    len = nbit_total_ready_bytes (&c->Q);
    if (len <= 0) {
      break;
    }
    if (!D->packet_len) {
      if (len < 4) {
        c->status = conn_reading_answer;
        return 4 - len;
      }
      assert (nbit_read_in (&c->Q, &D->packet_len, 4) == 4);
      len -= 4;
      if (D->packet_len <= 0 || (D->packet_len & 3) || (D->packet_len > RPCC_FUNC(c)->max_packet_len && RPCC_FUNC(c)->max_packet_len > 0)) {
	vkprintf (1, "error while parsing packet: bad packet length %d\n", D->packet_len);
        c->status = conn_error;
        c->error = -1;
        return 0;
      }
    }
    if (D->packet_len == 4) {
      assert (advance_skip_read_ptr (&c->In, 4) == 4);
      D->packet_len = 0;
      nbit_set (&c->Q, &c->In);
      continue;
    }
    if (D->packet_len < 16) {
      vkprintf (1, "error while parsing packet: bad packet length %d\n", D->packet_len);
      c->status = conn_error;
      c->error = -1;
      return 0;
    }
    if (len + 4 < D->packet_len) {
      c->status = conn_reading_answer;
      return D->packet_len - len - 4;
    }
    assert (nbit_read_in (&c->Q, &D->packet_num, 8) == 8);    
    unsigned crc32 = !rpc_disable_crc32_check ? crc32_partial (&D->packet_len, 12, -1) : 0;
    len = D->packet_len - 16;
    while (len > 0) {
      int l = nbit_ready_bytes (&c->Q);
      char *ptr = nbit_get_ptr (&c->Q);
      if (l > len) {
        l = len;
      }
      assert (l > 0);
      if (!rpc_disable_crc32_check) {
        crc32 = crc32_partial (ptr, l, crc32);
      }
      len -= l;
      assert (nbit_advance (&c->Q, l) == l);
    }
    assert (!len);
    assert (nbit_read_in (&c->Q, &D->packet_crc32, 4) == 4);
    if (~crc32 != D->packet_crc32 && !rpc_disable_crc32_check) {
      vkprintf (1, "error while parsing packet: crc32 = %08x != %08x\n", ~crc32, D->packet_crc32);
      c->status = conn_error;
      c->error = -1;
      return 0;
    }

    if (verbosity > 2) {
      fprintf (stderr, "received packet from connection %d\n", c->fd);
      dump_next_rpc_packet (c);
    }

    int keep_total_bytes = c->In.total_bytes;
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
        res = rpcc_process_nonce_packet (c);
        if (res >= 0) {
          res = rpcc_send_handshake_packet (c);
          //fprintf (stderr, "send_handshake_packet returned %d\n", res);
        }
      } else if (D->packet_num == -1) {
        c->status = conn_running;
        res = rpcc_process_handshake_packet (c);
        if (res >= 0 && RPCC_FUNC(c)->rpc_ready) {
          res = RPCC_FUNC(c)->rpc_ready (c);
        }
      }
      if (res < 0) {
        c->status = conn_error;
        c->error = res;
        return 0;
      }
    } else {
      /* main case */
      c->status = conn_running;
      if (D->packet_type == RPC_PING) {
        res = rpcc_default_execute (c, D->packet_type, D->packet_len);
      } else {
        res = RPCC_FUNC(c)->execute (c, D->packet_type, D->packet_len);
      }
    }

    if (c->status == conn_error) {
      if (!c->error) {
        c->error = -2;
      }
      return 0;
    }

    if (res == SKIP_ALL_BYTES) {
      assert (keep_total_bytes == c->In.total_bytes);
      advance_skip_read_ptr (&c->In, D->packet_len);
      D->in_packet_num++;
    } else if (keep_total_bytes != c->In.total_bytes) {
      assert (keep_total_bytes == c->In.total_bytes + D->packet_len);
      D->in_packet_num++;
    }
    D->packet_len = 0;
    if (c->status == conn_running) {
      c->status = conn_wait_answer;
    }
    if (c->status != conn_wait_answer) {
      break;
    }
    nbit_set (&c->Q, &c->In);
  }
  return 0;
}

int rpcc_compact_parse_execute (struct connection *c) {
  struct rpcc_data *D = RPCC_DATA(c);
  int len;

  while (1) {
    len = nbit_total_ready_bytes (&c->Q);
    if (len <= 0) {
      break;
    }
    if (!D->packet_len) {
      if (len < 4) {
        c->status = conn_reading_answer;
        return 4 - len;
      }
      assert (nbit_read_in (&c->Q, &D->packet_len, 1) == 1);
      len--;
      if (D->packet_len & 0x80) {
	c->status = conn_error;
	c->error = -1;
	return 0;
      }
      if (D->packet_len == 0x7f) {
	assert (nbit_read_in (&c->Q, &D->packet_len, 3) == 3);
	if (D->packet_len < 0x7f) {
	  vkprintf (1, "error while parsing compact packet: got length %d in overlong encoding\n", D->packet_len);
	  c->status = conn_error;
	  c->error = -1;
	  return 0;
	}
	len -= 3;
      } else if (!D->packet_len) {
	vkprintf (1, "error while parsing compact packet: got zero packet length\n");
	c->status = conn_error;
	c->error = -1;
	return 0;
      }
      D->flags |= 0x40000000;
      D->packet_len <<= 2;
      if (D->packet_len > RPCC_FUNC(c)->max_packet_len && RPCC_FUNC(c)->max_packet_len > 0) {
	vkprintf (1, "error while parsing packet: bad packet length %d\n", D->packet_len);
	c->status = conn_error;
	c->error = -1;
	return 0;
      }
    }
    if (len < D->packet_len) {
      c->status = conn_reading_answer;
      return D->packet_len - len;
    }
    assert (nbit_read_in (&c->Q, &D->packet_type, 4) == 4);

    if (D->packet_len <= 0x7e * 4) {
      assert (advance_skip_read_ptr (&c->In, 1) == 1);
    } else {
      assert (advance_skip_read_ptr (&c->In, 4) == 4);
    }
    D->packet_num = D->in_packet_num;
    
    int keep_total_bytes = c->In.total_bytes;
    int res;

    /* main case */
    c->status = conn_running;
    res = RPCC_FUNC(c)->execute (c, D->packet_type, D->packet_len);

    if (c->status == conn_error) {
      if (!c->error) {
        c->error = -2;
      }
      return 0;
    }

    if (res == SKIP_ALL_BYTES) {
      assert (keep_total_bytes == c->In.total_bytes);
      advance_skip_read_ptr (&c->In, D->packet_len);
    } else {
      assert (keep_total_bytes == c->In.total_bytes + D->packet_len);
    }
    D->in_packet_num++;

    assert (!c->pending_queries && c->status == conn_running);
    c->status = conn_wait_answer;

    D->packet_len = 0;
    nbit_set (&c->Q, &c->In);
  }
  return 0;
}



int rpcc_connected (struct connection *c) {
  c->last_query_sent_time = precise_now;

  #ifdef AES
  if (RPCC_FUNC(c)->rpc_check_perm) {
    int res = RPCC_FUNC(c)->rpc_check_perm (c);
    if (res < 0) {
      return res;
    }
    if (!(res &= 3)) {
      return -1;
    }
    RPCC_DATA(c)->crypto_flags = res;
  } else {
    RPCC_DATA(c)->crypto_flags = 3;
  }
  #endif
  vkprintf (2, "RPC connection #%d: [%s]:%d -> [%s]:%d connected, crypto_flags = %d\n", c->fd, show_our_ip (c), c->our_port, show_remote_ip (c), c->remote_port, RPCC_DATA(c)->crypto_flags);

  assert (RPCC_FUNC(c)->rpc_init_crypto);
  int res = RPCC_FUNC(c)->rpc_init_crypto (c);

  if (res > 0) {
    assert (RPCC_DATA(c)->crypto_flags & 4);
  } else {
    return -1;
  }

  //write_out (&c->Out, "version\r\n", 9);

  assert (RPCC_FUNC(c)->flush_packet);
  RPCC_FUNC(c)->flush_packet (c);

  return 0;
}


int rpcc_close_connection (struct connection *c, int who) {
  if (RPCC_FUNC(c)->rpc_close != NULL) {
    RPCC_FUNC(c)->rpc_close (c, who);
  }

  return client_close_connection (c, who);
}


int rpc_client_check_ready (struct connection *c) {
  return RPCC_FUNC(c)->check_ready (c);
}


int rpcc_init_fake_crypto (struct connection *c) {
  if (!(RPCC_DATA(c)->crypto_flags & 1)) {
    return -1;
  }

  static struct rpc_nonce_packet buf;
  memset (&buf, 0, sizeof (buf));
  buf.len = sizeof (buf);
  buf.seq_num = -2;
  buf.type = RPC_NONCE;
  buf.crypto_schema = RPC_CRYPTO_NONE;
  buf.crc32 = compute_crc32 (&buf, sizeof (buf) - 4);

  write_out (&c->Out, &buf, sizeof (buf));

  assert ((RPCC_DATA(c)->crypto_flags & 14) == 0);
  RPCC_DATA(c)->crypto_flags |= 4;
 
  return 1;
}


#ifdef AES

int rpcc_init_outbound (struct connection *c) {
  vkprintf (3, "rpcc_init_outbound (%d)\n", c->fd);
  struct rpcc_data *D = RPCC_DATA(c);
  c->last_query_sent_time = precise_now;

  if (RPCC_FUNC(c)->rpc_check_perm) {
    int res = RPCC_FUNC(c)->rpc_check_perm (c);
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

int rpcc_default_check_perm (struct connection *c) {
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

int rpcc_init_crypto (struct connection *c) {
  if (!(RPCC_DATA(c)->crypto_flags & 2)) {
    return rpcc_init_fake_crypto (c);
  }

  RPCC_DATA(c)->nonce_time = time (0);

  aes_generate_nonce (RPCC_DATA(c)->nonce);

  static struct rpc_nonce_packet buf;
  memset (&buf, 0, sizeof (buf));
  memcpy (buf.crypto_nonce, RPCC_DATA(c)->nonce, 16);
  buf.crypto_ts = RPCC_DATA(c)->nonce_time;
  buf.len = sizeof (buf);
  buf.seq_num = -2;
  buf.type = RPC_NONCE;
  buf.key_select = get_crypto_key_id ();
  buf.crypto_schema = RPC_CRYPTO_AES;
  buf.crc32 = compute_crc32 (&buf, sizeof (buf) - 4);

  write_out (&c->Out, &buf, sizeof (buf));

  assert ((RPCC_DATA(c)->crypto_flags & 14) == 2);
  RPCC_DATA(c)->crypto_flags |= 4;
 
  mark_all_processed (&c->Out);

  return 1;
}

int rpcc_start_crypto (struct connection *c, char *nonce, int key_select) {
  struct rpcc_data *D = RPCC_DATA(c);

  vkprintf (2, "rpcc_start_crypto: key_select = %d\n", key_select);

  if (c->crypto) {
    return -1;
  }

  if (c->In.total_bytes || c->Out.total_bytes || !D->nonce_time) {
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

  mark_all_unprocessed (&c->In);

  return 1;
}

void rpcc_flush_crypto (struct connection *c) {
  if (c->crypto) {
    int pad_bytes = c->type->crypto_needed_output_bytes (c);
    vkprintf (2, "rpcc_flush_packet: padding with %d bytes\n", pad_bytes);
    if (pad_bytes > 0) {
      assert (!(pad_bytes & 3));
      static int pad_str[3] = {4, 4, 4};
      assert (pad_bytes <= 12);
      write_out (&c->Out, pad_str, pad_bytes);
    }
  }
}

int rpcc_flush_packet (struct connection *c) {
  rpcc_flush_crypto (c);
  return flush_connection_output (c);
}

int rpcc_flush_packet_later (struct connection *c) {
  rpcc_flush_crypto (c);
  return flush_later (c);
}

int rpcc_flush (struct connection *c) {
  if (c->crypto) {
    int pad_bytes = c->type->crypto_needed_output_bytes (c);
    vkprintf (2, "rpcc_flush: padding with %d bytes\n", pad_bytes);
    if (pad_bytes > 0) {
      assert (!(pad_bytes & 3));
      static int pad_str[3] = {4, 4, 4};
      assert (pad_bytes <= 12);
      assert (write_out (&c->Out, pad_str, pad_bytes) == pad_bytes);
    }
    return pad_bytes;
  }
  return 0;
}

#else

int rpcc_init_outbound (struct connection *c) {
  struct rpcc_data *D = RPCC_DATA(c);
  c->last_query_sent_time = precise_now;

  D->in_packet_num = -2;

  return 0;
}

int rpcc_default_check_perm (struct connection *c) {
  if ((c->remote_ip & 0xff000000) != 0x0a000000 && (c->remote_ip & 0xff000000) != 0x7f000000) {
    return 0;
  }
  if (!c->our_ip) {
    return 1;
  }
  return ((c->our_ip ^ c->remote_ip) & 0xffff0000) ? 0 : 1;
}

int rpcc_init_crypto (struct connection *c) {
  return rpcc_init_fake_crypto (c);
}

int rpcc_start_crypto (struct connection *c, char *key, int key_len) {
  return -1;
}

int rpcc_flush_packet (struct connection *c) {
  return flush_connection_output (c);
}

int rpcc_flush (struct connection *c) {
  return 0;
}

#endif

int rpcc_connected_nohs (struct connection *c) {
  c->last_query_sent_time = precise_now;

  c->status = conn_wait_answer;
  if (RPCC_FUNC(c)->rpc_ready) {
    RPCC_FUNC(c)->rpc_ready (c);
  }

  return 0;
}

int rpcc_init_outbound_nohs (struct connection *c) {
  struct rpcc_data *D = RPCC_DATA(c);
  c->last_query_sent_time = precise_now;

  D->in_packet_num = 0;
  D->crypto_flags = 1;

  return 0;
}

/*
 *
 *		END (BASIC RPC CLIENT)
 *
 */

