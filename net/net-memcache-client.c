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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
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
#include "net-memcache-client.h"

#ifdef AES
# include "net-crypto-aes.h"
#endif


/*
 *
 *		MEMCACHED CLIENT INTERFACE
 *
 */

extern int verbosity;

#define	MAX_KEY_LEN	1000
#define	MAX_VALUE_LEN	1048576

static int mcc_parse_execute (struct connection *c);
static int mcc_connected (struct connection *c);

enum mc_query_parse_state {
  mrp_start,
  mrp_readcommand,
  mrp_skipspc,
  mrp_readkey,
  mrp_readint,
  mrp_readints,
  mrp_skiptoeoln,
  mrp_readtoeoln,
  mrp_eoln,
  mrp_wantlf,
  mrp_done
};

conn_type_t ct_memcache_client = {
  .magic = CONN_FUNC_MAGIC,
  .title = "memcache_client",
  .accept = server_failed,
  .init_accepted = server_failed,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = mcc_parse_execute,
  .close = client_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = mcc_init_outbound,
  .connected = mcc_connected,
  .wakeup = server_noop,
  .check_ready = mc_client_check_ready,
#ifdef AES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int mcc_execute (struct connection *c, int op);

struct memcache_client_functions default_memcache_client = {
  .execute = mcc_execute,
  .check_ready = server_check_ready,
  .flush_query = mcc_flush_query,
  .mc_check_perm = mcc_default_check_perm,
  .mc_init_crypto = mcc_init_crypto,
  .mc_start_crypto = mcc_start_crypto
};


int mcc_execute (struct connection *c, int op) {
  return -1;
}

static int mcc_parse_execute (struct connection *c) {
  struct mcc_data *D = MCC_DATA(c);
  char *ptr, *ptr_s, *ptr_e;
  int len;
  long long tt;

  while (1) {
    len = nbit_ready_bytes (&c->Q);
    ptr = ptr_s = nbit_get_ptr (&c->Q);
    ptr_e = ptr + len;
    if (len <= 0) {
      break;
    }

    while (ptr < ptr_e && c->parse_state != mrp_done) {

      switch (c->parse_state) {
      case mrp_start:
        D->clen = 0;
        D->response_flags = 0;
        D->response_type = mcrt_none;
        D->response_len = 0;
	D->key_offset = 0;
	D->key_len = 0;
	D->arg_num = 0;
        c->parse_state = mrp_readcommand;

      case mrp_readcommand:
        if (!D->clen && *ptr == '~') {
          ptr++;
          D->response_flags = 1;
        }
        while (ptr < ptr_e && D->clen < 15 && ((*ptr >= 'a' && *ptr <= 'z') || (*ptr >= 'A' && *ptr <= 'Z') || (*ptr == '_'))) {
          D->comm[D->clen++] = *ptr++;
        }
        if (ptr == ptr_e) {
          break;
        }
        
        if (D->clen == 0 && ((*ptr >= '0' && *ptr <= '9') || *ptr == '-')) {
          D->response_type = mcrt_NUMBER;
          c->parse_state = mrp_readints;
	  D->args[0] = 0;
          break;
        }

        if (D->clen == 15 || (*ptr != '\t' && *ptr != ' ' && *ptr != '\r' && *ptr != '\n')) {
          c->parse_state = mrp_skiptoeoln;
          break;
        }
        D->comm[D->clen] = 0;
        switch (D->comm[0]) {
        case 'V':
          if (!strcmp (D->comm, "VALUE")) {
            D->response_type = mcrt_VALUE;
          } else if (!strcmp (D->comm, "VERSION")) {
            D->response_type = mcrt_VERSION;
          }
          break;
        case 'S':
          if (!strcmp (D->comm, "STORED")) {
            D->response_type = mcrt_STORED;
          } else
          if (!strcmp (D->comm, "SERVER_ERROR")) {
            D->response_type = mcrt_SERVER_ERROR;
          }
          break;
        case 'N':
          if (!strcmp (D->comm, "NOT_STORED")) {
            D->response_type = mcrt_NOTSTORED;
          } else 
          if (!strcmp (D->comm, "NOT_FOUND")) {
            D->response_type = mcrt_NOTFOUND;
          }
          if (!strcmp (D->comm, "NONCE")) {
            D->response_type = mcrt_NONCE;
          }
          break;
        case 'C':
          if (!strcmp (D->comm, "CLIENT_ERROR")) {
            D->response_type = mcrt_CLIENT_ERROR;
          }
        case 'E':
          if (!strcmp (D->comm, "END")) {
            D->response_type = mcrt_END;
          } else
          if (!strcmp (D->comm, "ERROR")) {
            D->response_type = mcrt_ERROR;
          }
          break;
        case 'D':
          if (!strcmp (D->comm, "DELETED")) {
            D->response_type = mcrt_DELETED;
          }
          break;
        case 0:
          D->response_type = mcrt_empty;
          break;
        default:
          break;
        }

        if (D->response_type == mcrt_none) {
          c->parse_state = mrp_skiptoeoln;
          break;
        }
        

	D->key_offset = D->clen;
	c->parse_state = mrp_skipspc;

      case mrp_skipspc:

	while (ptr < ptr_e && (*ptr == ' ' || *ptr == '\t')) {
	  D->key_offset++;
          ptr++;
	}
	if (ptr == ptr_e) {
          break;
	}
	if (*ptr != '\r' && *ptr != '\n') {
	  if (D->response_type >= mcrt_STORED) {
	    c->parse_state = mrp_skiptoeoln;
	    break;
	  } else if (D->response_type >= mcrt_VERSION) {
	    c->parse_state = mrp_readtoeoln;
	    break;
	  } else {
	    c->parse_state = mrp_readkey;
	  }
        } else {
          c->parse_state = mrp_eoln;
          break;
        }
       
      case mrp_readkey:

	while (ptr < ptr_e && *ptr && *ptr != ' ' && *ptr != '\t' && *ptr != '\r' && *ptr != '\n') {
	  D->key_len++;
          ptr++;
	}
	if (ptr == ptr_e) {
          break;
	}
	D->arg_num = 0;

	if (D->response_type == mcrt_NONCE) {
	  c->parse_state = (*ptr == '\r' || *ptr == '\n') ? mrp_eoln : mrp_skiptoeoln;
	  break;
	}

	c->parse_state = mrp_readints;

      case mrp_readints:

	while (ptr < ptr_e && (*ptr == ' ' || *ptr == '\t')) {
          ptr++;
	}
	if (ptr == ptr_e) {
          break;
	}
	if (*ptr == '\r' || *ptr == '\n') {
	  c->parse_state = mrp_eoln;
	  break;
	}
	if (D->arg_num >= 4) {
	  c->parse_state = mrp_skiptoeoln;
	  break;
	}
	D->args[D->arg_num] = 0;
	if (*ptr == '-') {
	  ptr++;
	  D->response_flags |= 6;
	} else {
	  D->response_flags |= 4;
	}
	c->parse_state = mrp_readint;

      case mrp_readint:	

        tt = D->args[D->arg_num];
        while (ptr < ptr_e && *ptr >= '0' && *ptr <= '9') {
          if (tt > 0x7fffffffffffffffLL / 10 || (tt == 0x7fffffffffffffffLL / 10 && *ptr > '0' + (char) (0x7fffffffffffffffLL % 10) + ((D->response_flags & 2) != 0))) {
            fprintf (stderr, "number too large - already %lld\n", tt);
            c->parse_state = mrp_skiptoeoln;
            break;
          }
          tt = tt*10 + (*ptr - '0');
          D->response_flags &= ~4;
          ptr++;
        }
        D->args[D->arg_num] = tt;
        if (ptr == ptr_e) {
          break;
        }
        if (D->response_flags & 2) {
          D->args[D->arg_num] = -tt;
        }
        D->arg_num++;
        if ((D->response_flags & 4) || (*ptr != ' ' && *ptr != '\t' && *ptr != '\r' && *ptr != '\n')) {
	  c->parse_state = mrp_skiptoeoln;
	  break;
        }
        c->parse_state = mrp_readints;
        break;

      case mrp_skiptoeoln:

        fprintf (stderr, "mrp_skiptoeoln, response_type=%d, remainder=%.*s\n", D->response_type, ptr_e - ptr < 64 ? (int)(ptr_e - ptr) : 64, ptr);
        
        D->response_flags |= 16;

      case mrp_readtoeoln:

        while (ptr < ptr_e && (*ptr != '\r' && *ptr != '\n')) {
          ptr++;
        }
        if (ptr == ptr_e) {
          break;
        }
        c->parse_state = mrp_eoln;

      case mrp_eoln:

        assert (ptr < ptr_e && (*ptr == '\r' || *ptr == '\n'));

        if (*ptr == '\r') {
          ptr++;
        }
        c->parse_state = mrp_wantlf;

      case mrp_wantlf:

        if (ptr == ptr_e) {
          break;
        }
        if (*ptr == '\n') {
          ptr++;
        }
        c->parse_state = mrp_done;

      case mrp_done:
        break;

      default:
        assert (0);
      }
    }

    len = ptr - ptr_s;
    nbit_advance (&c->Q, len);
    D->response_len += len;

    if (c->parse_state == mrp_done) {
      if (D->key_len > MAX_KEY_LEN) {
        fprintf (stderr, "error: key len %d exceeds %d, response_type=%d\n", D->key_len, MAX_KEY_LEN, D->response_type);
        D->response_flags |= 16;
      }
      if (!(D->response_flags & 16)) {
        c->status = conn_running;

        if (!MCC_FUNC(c)->execute) {
          MCC_FUNC(c)->execute = mcc_execute;
        }

        int res = 0;

        if ((D->crypto_flags & 14) == 6) {
          switch (D->response_type) {
          case mcrt_NONCE:
            if (MCC_FUNC(c)->mc_start_crypto && D->key_len > 0 && D->key_len <= 255) {
              static char nonce_key[256];
              assert (advance_skip_read_ptr (&c->In, D->key_offset) == D->key_offset);
              assert (read_in (&c->In, nonce_key, D->key_len) == D->key_len);
              nonce_key[D->key_len] = 0;
              int t = D->response_len - D->key_offset - D->key_len;
              assert (advance_skip_read_ptr (&c->In, t) == t);
              if (t == 2) {
                res = MCC_FUNC(c)->mc_start_crypto (c, nonce_key, D->key_len);
                if (res > 0) {
                  D->crypto_flags = (D->crypto_flags & -16) | 10;
                  MCC_FUNC(c)->flush_query (c);
                  res = 0;
                  break;
                }
              }
            }
            c->status = conn_error;
            c->error = -1;
            return 0;
          case mcrt_NOTFOUND:
            if (D->crypto_flags & 1) {
              assert (advance_skip_read_ptr (&c->In, D->response_len) == D->response_len);
              release_all_unprocessed (&c->Out);
              D->crypto_flags = 1;
              break;
            }
          default:
            c->status = conn_error;
            c->error = -1;
            return 0;
          }
        } else if (D->response_type == mcrt_NONCE) {
            fprintf (stderr, "bad response\n");
            c->status = conn_error;
            c->error = -1;
            return 0;
        } else {

          res = MCC_FUNC(c)->execute (c, D->response_type);

        }

        if (res > 0) {
          c->status = conn_reading_answer;
          return res;	// need more bytes
        } else if (res < 0) {
          if (res == SKIP_ALL_BYTES) {
            assert (advance_skip_read_ptr (&c->In, D->response_len) == D->response_len);
          } else {
            assert (-res <= D->response_len);
            assert (advance_skip_read_ptr (&c->In, -res) == -res);
          }
        }
        if (c->status == conn_running) {
          c->status = conn_wait_answer;
        }
      } else {
        assert (advance_skip_read_ptr (&c->In, D->response_len) == D->response_len);
        c->status = conn_wait_answer;
      }
      if (D->response_flags & 48) {
        //write_out (&c->Out, "CLIENT_ERROR\r\n", 14);
        if (verbosity > 0) {
          fprintf (stderr, "bad response\n");
        }
        c->status = conn_error;
        c->error = -1;
	return 0;
      }
      if (c->status == conn_error && c->error < 0) {
        return 0;
      }
      c->parse_state = mrp_start;
      nbit_set (&c->Q, &c->In);
    }
  }
  if (c->status == conn_reading_answer) {
    return NEED_MORE_BYTES;
  }
  return 0;
}

int mcc_connected (struct connection *c) {
  c->last_query_sent_time = precise_now;

  #ifdef AES
  if (verbosity > 1) {
    fprintf (stderr, "connection #%d: connected, crypto_flags = %d\n", c->fd, MCC_DATA(c)->crypto_flags);
  }
  if (MCC_FUNC(c)->mc_check_perm) {
    int res = MCC_FUNC(c)->mc_check_perm (c);
    if (res < 0) {
      return res;
    }
    if (!(res &= 3)) {
      return -1;
    }
    MCC_DATA(c)->crypto_flags = res;
  } else {
    MCC_DATA(c)->crypto_flags = 3;
  }

  if ((MCC_DATA(c)->crypto_flags & 2) && MCC_FUNC(c)->mc_init_crypto) {
    int res = MCC_FUNC(c)->mc_init_crypto (c);

    if (res > 0) {
      assert (MCC_DATA(c)->crypto_flags & 4);
    } else if (!(MCC_DATA(c)->crypto_flags & 1)) {
      return -1;
    }
  }
  #endif

  write_out (&c->Out, "version\r\n", 9);

  //arseny30: added for php-engine:
  if (MCC_FUNC(c)->connected) {
    MCC_FUNC(c)->connected (c);
  }

  assert (MCC_FUNC(c)->flush_query);
  MCC_FUNC(c)->flush_query (c);

  return 0;
}


int mc_client_check_ready (struct connection *c) {
  return MCC_FUNC(c)->check_ready (c);
}


#ifdef AES

int mcc_init_outbound (struct connection *c) {
  c->last_query_sent_time = precise_now;

  if (MCC_FUNC(c)->mc_check_perm) {
    int res = MCC_FUNC(c)->mc_check_perm (c);
    if (res < 0) {
      return res;
    }
    if (!(res &= 3)) {
      return -1;
    }

    MCC_DATA(c)->crypto_flags = res;
  } else {
    MCC_DATA(c)->crypto_flags = 1;
  }

  return 0;
}

int aes_initialized;


int mcc_default_check_perm (struct connection *c) {
  int ipxor = -1, mask = -1;
  if (c->flags & C_IPV6) {
    if (is_ipv6_localhost (c->our_ipv6) && is_ipv6_localhost (c->remote_ipv6)) {
      ipxor = 0;
    } else if (*(int *)(c->our_ipv6) != *(int *)(c->remote_ipv6)) {
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

int mcc_init_crypto (struct connection *c) {
  MCC_DATA(c)->nonce_time = time (0);

  aes_generate_nonce (MCC_DATA(c)->nonce);

  static char buf[128];

  write_out (&c->Out, buf, sprintf (buf, "delete @#$AuTh$#@A:%08x:%016llx%016llx\r\n", 
    MCC_DATA(c)->nonce_time,
    *(long long *)MCC_DATA(c)->nonce,
    *(long long *)(MCC_DATA(c)->nonce + 8))
  );

  assert ((MCC_DATA(c)->crypto_flags & 14) == 2);
  MCC_DATA(c)->crypto_flags |= 4;
 
  mark_all_processed (&c->Out);

  return 1;
}

int mcc_start_crypto (struct connection *c, char *key, int key_len) {
  struct mcc_data *D = MCC_DATA(c);

  if (c->crypto) {
    return -1;
  }
  if (key_len != 32 || key[32]) {
    return -1;
  }

  if (c->In.total_bytes || c->Out.total_bytes || !D->nonce_time) {
    return -1;
  }

  static char nonce_in[16];
  char *tmp;

  *(long long *)(nonce_in + 8) = strtoull (key + 16, &tmp, 16);
  if (tmp != key + 32) {
    return -1;
  }

  key[16] = 0;
  *(long long *)nonce_in = strtoull (key, &tmp, 16);
  if (tmp != key + 16) {
    return -1;
  }

  struct aes_key_data aes_keys;

  if (aes_create_keys (&aes_keys, 1, nonce_in, D->nonce, D->nonce_time, c->remote_ip, c->remote_port, c->remote_ipv6, c->our_ip, c->our_port, c->our_ipv6) < 0) {
    return -1;
  }

  if (aes_crypto_init (c, &aes_keys, sizeof (aes_keys)) < 0) {
    return -1;
  }

  mark_all_unprocessed (&c->In);

  return 1;
}

int mcc_flush_query (struct connection *c) {
  if (c->crypto) {
    int pad_bytes = c->type->crypto_needed_output_bytes (c);
    if (verbosity > 1) {
      fprintf (stderr, "mcc_flush_query: padding with %d bytes\n", pad_bytes);
    }
    if (pad_bytes > 0) {
      static char pad_str[16] = "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n";
      assert (pad_bytes <= 15);
      write_out (&c->Out, pad_str, pad_bytes);
    }
  }
  return flush_connection_output (c);
}

#else

int mcc_init_outbound (struct connection *c) {
  c->last_query_sent_time = precise_now;

  return 0;
}

int mcc_default_check_perm (struct connection *c) {
  if ((c->remote_ip & 0xff000000) != 0x0a000000 && (c->remote_ip & 0xff000000) != 0x7f000000) {
    return 0;
  }
  if (!c->our_ip) {
    return 1;
  }
  return ((c->our_ip ^ c->remote_ip) & 0xffff0000) ? 0 : 1;
}

int mcc_init_crypto (struct connection *c) {
  return -1;
}

int mcc_start_crypto (struct connection *c, char *key, int key_len) {
  return -1;
}

int mcc_flush_query (struct connection *c) {
  return flush_connection_output (c);
}


#endif

/*
 *
 *		END (MEMCACHED CLIENT)
 *
 */

