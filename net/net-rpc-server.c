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
#include "net-rpc-server.h"

#include "rpc-const.h"

#ifdef AES
# include "net-crypto-aes.h"
#endif

/*
 *
 *		BASIC RPC SERVER INTERFACE
 *
 */

extern int verbosity;

extern int rpc_disable_crc32_check;

#define	MAX_KEY_LEN	1000
#define	MAX_VALUE_LEN	1048576

static int rpcs_wakeup (struct connection *c);
int rpcs_parse_execute (struct connection *c);
int rpcs_compact_parse_execute (struct connection *c);
int rpcs_alarm (struct connection *c);
int rpcs_do_wakeup (struct connection *c);
int rpcs_init_accepted (struct connection *c);
int rpcs_close_connection (struct connection *c, int who);

conn_type_t ct_rpc_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "rpc_server",
  .accept = accept_new_connections,
  .init_accepted = rpcs_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = rpcs_parse_execute,
  .close = rpcs_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = rpcs_wakeup,
  .alarm = rpcs_alarm,
  .flush = rpcs_flush,
#ifdef AES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

conn_type_t ct_rpc_ext_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "ext_rpc_server",
  .accept = accept_new_connections,
  .init_accepted = rpcs_init_accepted_nohs,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = rpcs_compact_parse_execute,
  .close = rpcs_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = rpcs_wakeup,
  .alarm = rpcs_alarm,
};

int rpcs_default_execute (struct connection *c, int op, int len);
int rpcs_init_crypto (struct connection *c, struct rpc_nonce_packet *P);

struct rpc_server_functions default_rpc_server = {
  .execute = rpcs_default_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_wakeup = rpcs_do_wakeup,
  .rpc_alarm = rpcs_do_wakeup,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_ready = server_noop,
};

static void dump_next_packet (struct connection *c) {
  struct nb_iterator it;
  int i = 0, len = 4, x;
  nbit_set (&it, &c->In);
  while (i * 4 < len) {
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

int rpcs_default_execute (struct connection *c, int op, int len) {
  vkprintf (1, "rpcs_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);
  if (op == RPC_PING && len == 24) {
    c->last_response_time = precise_now;
    static int Q[12];
    assert (read_in (&c->In, Q, 24) == 24);
    static int P[12];
    P[0] = 24;
    P[1] = RPCS_DATA(c)->out_packet_num++;
    P[2] = RPC_PONG;    
    P[3] = Q[3];
    P[4] = Q[4];
    P[5] = compute_crc32 (P, 20);
    vkprintf (1, "Received ping from fd=%d. ping_id = %lld. Sending pong\n", c->fd, *(long long *)(Q + 3));
    write_out (&c->Out, P, 24);
    RPCS_FUNC(c)->flush_packet(c);
    return 24;
  }
  return SKIP_ALL_BYTES;

}

static int rpcs_process_nonce_packet (struct connection *c) {
  struct rpcs_data *D = RPCS_DATA(c);
  static struct rpc_nonce_packet P;
  int res;

  if (D->packet_num != -2 || D->packet_type != RPC_NONCE) {
    return -2;
  }
  if (D->packet_len != sizeof (struct rpc_nonce_packet)) {
    return -3;
  }
  assert (read_in (&c->In, &P, D->packet_len) == D->packet_len);
  switch (P.crypto_schema) {
  case RPC_CRYPTO_NONE:
    if (P.key_select) {
      return -3;
    }
    if (D->crypto_flags & 1) {
      D->crypto_flags = 1;
    } else {
      return -5;
    }
    break;
#ifdef AES
  case RPC_CRYPTO_AES:
    if (!P.key_select || P.key_select != get_crypto_key_id ()) {
      if (D->crypto_flags & 1) {
        D->crypto_flags = 1;
        break;
      }
      return -3;
    }
    if (!(D->crypto_flags & 2)) {
      if (D->crypto_flags & 1) {
        D->crypto_flags = 1;
        break;
      }
      return -5;
    }
    D->nonce_time = time (0);
    if (abs (P.crypto_ts - D->nonce_time) > 30) {
      return -6;	//less'om
    }
    D->crypto_flags &= -2;
    break;
#endif
  default:
    if (D->crypto_flags & 1) {
      D->crypto_flags = 1;
      break;
    }
    return -4;
  }
  res = RPCS_FUNC(c)->rpc_init_crypto (c, &P);
  if (res < 0) {
    return -6;
  }
  return 0;
}

static int rpcs_send_handshake_packet (struct connection *c) {
  struct rpcs_data *D = RPCS_DATA(c);
  static struct rpc_handshake_packet P;
  assert (PID.pid);
  memset (&P, 0, sizeof (P));
  P.len = sizeof (P);
  P.seq_num = -1;
  P.type = RPC_HANDSHAKE;
  P.flags = 0;
  memcpy (&P.sender_pid, &PID, sizeof (struct process_id));
  memcpy (&P.peer_pid, &D->remote_pid, sizeof (struct process_id));
  P.crc32 = compute_crc32 (&P, sizeof (P) - 4);
  write_out (&c->Out, &P, sizeof (P));
  RPCS_FUNC(c)->flush_packet (c);

  return 0;
}

static int rpcs_send_handshake_error_packet (struct connection *c, int error_code) {
  static struct rpc_handshake_error_packet P;
  assert (PID.pid);
  memset (&P, 0, sizeof (P));
  P.len = sizeof (P);
  P.seq_num = -1;
  P.type = RPC_HANDSHAKE_ERROR;
  P.error_code = error_code;
  memcpy (&P.sender_pid, &PID, sizeof (PID));
  P.crc32 = compute_crc32 (&P, sizeof (P) - 4);
  write_out (&c->Out, &P, sizeof (P));
  RPCS_FUNC(c)->flush_packet (c);

  return 0;
}

static int rpcs_process_handshake_packet (struct connection *c) {
  struct rpcs_data *D = RPCS_DATA(c);
  static struct rpc_handshake_packet P;
  if (!PID.ip) {
    init_server_PID (c->our_ip, c->our_port);
    if (!PID.ip) {
      PID.ip = get_my_ipv4 ();
    }
  }
  if (D->packet_num != -1 || D->packet_type != RPC_HANDSHAKE) {
    return -2;
  }
  if (D->packet_len != sizeof (struct rpc_handshake_packet)) {
    rpcs_send_handshake_error_packet (c, -3);
    return -3;
  }
  assert (read_in (&c->In, &P, D->packet_len) == D->packet_len);
  memcpy (&D->remote_pid, &P.sender_pid, sizeof (struct process_id));
  if (!matches_pid (&PID, &P.peer_pid)) {
    vkprintf (1, "PID mismatch during handshake: local %08x:%d:%d:%d, remote %08x:%d:%d:%d\n",
		 PID.ip, PID.port, PID.pid, PID.utime, P.peer_pid.ip, P.peer_pid.port, P.peer_pid.pid, P.peer_pid.utime);
    rpcs_send_handshake_error_packet (c, -4);
    return -4;
  }
  return 0;
}

int rpcs_parse_execute (struct connection *c) {
  struct rpcs_data *D = RPCS_DATA(c);
  int len;

  while (1) {
    len = nbit_total_ready_bytes (&c->Q);
    if (len <= 0) {
      break;
    }
    // fprintf (stderr, "in while : packet_len=%d, total_ready_bytes=%d; cptr=%p; c->status=%d\n", D->packet_len, len, c->Q.cptr, c->status);
    if (!D->packet_len) {
      if (len < 4) {
        c->status = conn_reading_query;
        return 4 - len;
      }
      assert (nbit_read_in (&c->Q, &D->packet_len, 4) == 4);
      // fprintf (stderr, "reading packet len [ = %d]\n", D->packet_len);
      len -= 4;
      if (D->crypto_flags & 512) {
	D->flags = (D->flags & 0x7fffffff) | (D->packet_len & 0x80000000);
	D->packet_len &= 0x7fffffff;
      }
      if (D->packet_len <= 0 || (D->packet_len & 0xc0000003)) {
        if (D->in_packet_num <= -2 && (D->packet_len == 0x656c6564 || D->packet_len == 0x74617473 || D->packet_len == 0x73726576 || D->packet_len == 0x20746567 || D->packet_len == 0x20746573 || D->packet_len == 0x20646461
                                                                   || D->packet_len == 0x6c706572 || D->packet_len == 0x72636e69 || D->packet_len == 0x72636564) && RPCS_FUNC(c)->memcache_fallback_type) {
	  vkprintf (1, "switching to memcache fallback for connection %d\n", c->fd);
          memset (c->custom_data, 0, sizeof (c->custom_data));
          c->type = RPCS_FUNC(c)->memcache_fallback_type;
          c->extra = RPCS_FUNC(c)->memcache_fallback_extra;
          if (c->type->init_accepted (c) < 0) {
	    vkprintf (1, "memcache init_accepted() returns error for connection %d\n", c->fd);
            c->status = conn_error;
            c->error = -33;
            return 0;
          }
          nbit_set (&c->Q, &c->In);
          return c->type->parse_execute (c);
        }
        if (D->in_packet_num <= -2 && (D->packet_len == 0x44414548 || D->packet_len == 0x54534f50 || D->packet_len == 0x20544547) && RPCS_FUNC(c)->http_fallback_type) {
	  vkprintf (1, "switching to http fallback for connection %d\n", c->fd);
          memset (c->custom_data, 0, sizeof (c->custom_data));
          c->type = RPCS_FUNC(c)->http_fallback_type;
          c->extra = RPCS_FUNC(c)->http_fallback_extra;
          if (c->type->init_accepted (c) < 0) {
	    vkprintf (1, "http init_accepted() returns error for connection %d\n", c->fd);
            c->status = conn_error;
            c->error = -33;
            return 0;
          }
          nbit_set (&c->Q, &c->In);
          return c->type->parse_execute (c);
        }
	vkprintf (1, "error while parsing packet: bad packet length %d\n", D->packet_len);
        c->status = conn_error;
        c->error = -1;
        return 0;
      }
      if (D->packet_len > RPCS_FUNC(c)->max_packet_len && RPCS_FUNC(c)->max_packet_len > 0) {
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
      //fprintf (stderr, "need %d bytes, only %d present; need %d more\n", D->packet_len, len + 4, D->packet_len - len - 4);
      c->status = conn_reading_query;
      return D->packet_len - len - 4;
    }
    assert (nbit_read_in (&c->Q, &D->packet_num, 8) == 8);
    unsigned crc32 = 0;
    if (!rpc_disable_crc32_check) {
      crc32 = crc32_partial (&D->packet_len, 12, -1);
      if ((D->flags < 0) && (D->crypto_flags & 512)) {
	crc32 ^= 0x1d80c620 ^ 0x7bd5c66f;
      }
    }

    //fprintf (stderr, "num = %d, len = %d\n", D->packet_num, D->packet_len);

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
    //fprintf (stderr, "check : crc32 = %08x ?= %08x\n", ~crc32, D->packet_crc32);
    if (~crc32 != D->packet_crc32 && !rpc_disable_crc32_check) {
      if (verbosity > 0) {
        dump_next_packet (c);
        fprintf (stderr, "error while parsing packet: crc32 = %08x != %08x\n", ~crc32, D->packet_crc32);
      }
      c->status = conn_error;
      c->error = -1;
      return 0;
    }

    if (verbosity > 2) {
      fprintf (stderr, "received packet from connection %d\n", c->fd);
      dump_next_packet (c);
    }

    int keep_total_bytes = c->In.total_bytes;
    int res = -1;

    if (D->in_packet_num == -3) {
      D->in_packet_num = 0;
    }

    if (!(D->crypto_flags & 256) && D->packet_num != D->in_packet_num) {
      vkprintf (1, "error while parsing packet: got packet num %d, expected %d\n", D->packet_num, D->in_packet_num);
      c->status = conn_error;
      c->error = -1;
      return 0;
    } else if (D->packet_num < 0) {
      /* this is for us */
      if (D->packet_num == -2) {
        c->status = conn_running;
        res = rpcs_process_nonce_packet (c);  // if res > 0, nonce packet sent in response
      } else if (D->packet_num == -1) {
        c->status = conn_running;
        res = rpcs_process_handshake_packet (c);
        if (res >= 0) {
          res = rpcs_send_handshake_packet (c);
          if (res >= 0 && RPCS_FUNC(c)->rpc_ready) {
            res = RPCS_FUNC(c)->rpc_ready (c);
          }
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
        res = rpcs_default_execute (c, D->packet_type, D->packet_len);
      } else {
        res = RPCS_FUNC(c)->execute (c, D->packet_type, D->packet_len);
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

    if (c->status == conn_running) {
      c->status = conn_expect_query;
    }

    //assert ((c->pending_queries && (c->status == conn_wait_net || c->status == conn_wait_aio)) || (!c->pending_queries && c->status == conn_expect_query));
    assert (c->status == conn_wait_net || (c->pending_queries && c->status == conn_wait_aio) || (!c->pending_queries && c->status == conn_expect_query));

    D->packet_len = 0;
    if (c->status != conn_expect_query) {
      break;
    }
    nbit_set (&c->Q, &c->In);
  }
  return 0;
}

int rpcs_compact_parse_execute (struct connection *c) {
  struct rpcs_data *D = RPCS_DATA(c);
  int len;

  if (D->crypto_flags & 1024) {
    return rpcs_parse_execute (c);
  }

  while (1) {
    len = nbit_total_ready_bytes (&c->Q);
    if (len <= 0) {
      break;
    }
    if (!D->packet_len) {
      if (len < 5) {
        c->status = conn_reading_query;
        return 5 - len;
      }
      assert (nbit_read_in (&c->Q, &D->packet_len, 1) == 1);
      len--;
      if (D->in_packet_num == -3) {
	if (D->packet_len != 0xef) {
	  D->crypto_flags |= 1024;
	  nbit_set (&c->Q, &c->In);
	  D->packet_len = 0;
	  return rpcs_parse_execute (c);
	}
	D->in_packet_num = 0;
	D->flags |= 0x40000000;
	assert (advance_skip_read_ptr (&c->In, 1) == 1);
	assert (nbit_read_in (&c->Q, &D->packet_len, 1) == 1);
	len--;
      }
      if (D->packet_len & 0x80) {
	D->flags |= 0x80000000;
	D->packet_len &= 0x7f;
      } else {
	D->flags &= 0x7fffffff;
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
      D->packet_len <<= 2;
      if (D->packet_len > RPCS_FUNC(c)->max_packet_len && RPCS_FUNC(c)->max_packet_len > 0) {
	vkprintf (1, "error while parsing packet: bad packet length %d\n", D->packet_len);
	c->status = conn_error;
	c->error = -1;
	return 0;
      }
    }
    if (len < D->packet_len) {
      c->status = conn_reading_query;
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
    res = RPCS_FUNC(c)->execute (c, D->packet_type, D->packet_len);

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
    c->status = conn_expect_query;

    D->packet_len = 0;
    nbit_set (&c->Q, &c->In);
  }
  return 0;
}


	  
static int rpcs_wakeup (struct connection *c) {
  if (c->status == conn_wait_net) {
    c->status = conn_expect_query;
    RPCS_FUNC(c)->rpc_wakeup (c);
  }
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}

int rpcs_alarm (struct connection *c) {
  RPCS_FUNC(c)->rpc_alarm (c);
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}

int rpcs_close_connection (struct connection *c, int who) {
  if (RPCS_FUNC(c)->rpc_close != NULL) {
    RPCS_FUNC(c)->rpc_close (c, who);
  } 

  return server_close_connection (c, who);
}


int rpcs_do_wakeup (struct connection *c) {
  return 0;
}


int rpcs_init_accepted (struct connection *c) {
  c->last_query_sent_time = precise_now;

  if (RPCS_FUNC(c)->rpc_check_perm) {
    int res = RPCS_FUNC(c)->rpc_check_perm (c);
    vkprintf (4, "rpcs_check_perm for connection %d: [%s]:%d -> [%s]:%d = %d\n", c->fd, show_remote_ip (c), c->remote_port, show_our_ip (c), c->our_port, res);
    if (res < 0) {
      return res;
    }
    if (!(res &= 3)) {
      return -1;
    }

    RPCS_DATA(c)->crypto_flags = res;
  } else {
    RPCS_DATA(c)->crypto_flags = 1;
  }

  RPCS_DATA(c)->in_packet_num = -2;
  
  return 0;
}

int rpcs_init_accepted_nohs (struct connection *c) {
  RPCS_DATA(c)->crypto_flags = 512 + 1;
  RPCS_DATA(c)->in_packet_num = -3;
  return RPCS_FUNC(c)->rpc_ready ? RPCS_FUNC(c)->rpc_ready (c) : 0;
}

int rpcs_init_fake_crypto (struct connection *c) {
  if (!(RPCS_DATA(c)->crypto_flags & 1)) {
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

  assert ((RPCS_DATA(c)->crypto_flags & 14) == 0);
  RPCS_DATA(c)->crypto_flags |= 4;
 
  return 1;
}


#ifdef AES

#include "net-crypto-aes.h"

int rpcs_default_check_perm (struct connection *c) {
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

int rpcs_init_crypto (struct connection *c, struct rpc_nonce_packet *P) {
//  fprintf (stderr, "mcs_init_crypto (%p [fd=%d], '%.*s')\n", c, c->fd, key_len, key);
  struct rpcs_data *D = RPCS_DATA(c);

  if (c->crypto) {
    return -1;
  }

  if ((D->crypto_flags & 3) == 1) {
    return rpcs_init_fake_crypto (c);
  }

  if ((D->crypto_flags & 3) != 2) {
    return -1;
  }

  if (c->In.total_bytes) {
    return -1;
  }

  aes_generate_nonce (D->nonce);

  struct aes_key_data aes_keys;

  if (aes_create_keys (&aes_keys, 0, D->nonce, P->crypto_nonce, P->crypto_ts, c->our_ip, c->our_port, c->our_ipv6, c->remote_ip, c->remote_port, c->remote_ipv6) < 0) {
    return -1;
  }

  if (aes_crypto_init (c, &aes_keys, sizeof (aes_keys)) < 0) {
    return -1;
  }

  static struct rpc_nonce_packet buf;
  memset (&buf, 0, sizeof (buf));
  memcpy (buf.crypto_nonce, D->nonce, 16);
  buf.crypto_ts = D->nonce_time;
  buf.len = sizeof (buf);
  buf.seq_num = -2;
  buf.type = RPC_NONCE;
  buf.key_select = get_crypto_key_id ();
  buf.crypto_schema = RPC_CRYPTO_AES;
  buf.crc32 = compute_crc32 (&buf, sizeof (buf) - 4);

  write_out (&c->Out, &buf, sizeof (buf));

  assert ((D->crypto_flags & 14) == 2);
  D->crypto_flags |= 4;

  mark_all_processed (&c->Out);
  mark_all_unprocessed (&c->In);

  return 1;
}

int rpcs_flush_packet (struct connection *c) {
  if (c->crypto) {
    int pad_bytes = c->type->crypto_needed_output_bytes (c);
    vkprintf (2, "rpcs_flush_packet: padding with %d bytes\n", pad_bytes);
    if (pad_bytes > 0) {
      assert (!(pad_bytes & 3));
      static int pad_str[3] = {4, 4, 4};
      assert (pad_bytes <= 12);
      write_out (&c->Out, pad_str, pad_bytes);
    }
  }
  return flush_connection_output (c);
}

int rpcs_flush (struct connection *c) {
  if (c->crypto) {
    int pad_bytes = c->type->crypto_needed_output_bytes (c);
    vkprintf (2, "rpcs_flush: padding with %d bytes\n", pad_bytes);
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
int rpcs_default_check_perm (struct connection *c) {
  if ((c->remote_ip & 0xff000000) != 0x0a000000 && (c->remote_ip & 0xff000000) != 0x7f000000) {
    return 0;
  }
  return ((c->our_ip ^ c->remote_ip) & 0xffff0000) ? 0 : 1;
}

int rpcs_init_crypto (struct connection *c, struct rpc_nonce_packet *P) {
  return rpcs_init_fake_crypto (c);
}

int rpcs_flush_packet (struct connection *c) {
  return flush_connection_output (c);
}

int rpcs_flush (struct connection *c) {
  return 0;
}
#endif


/*
 *
 *		END (BASIC RPC SERVER)
 *
 */

/* vim: set tabstop=8 expandtab: */
