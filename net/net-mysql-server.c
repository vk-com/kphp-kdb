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

    Copyright 2009-2011 Vkontakte Ltd
              2009-2011 Nikolai Durov
              2009-2011 Andrei Lopatin
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
#include <openssl/sha.h>

#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-mysql-server.h"
#include "net-crypto-aes.h"

/*
 *
 *		MySQL SERVER INTERFACE
 *
 */

extern int verbosity;

static int sqls_wakeup (struct connection *c);
static int sqls_alarm (struct connection *c);
static int sqls_ready_to_write (struct connection *c);

conn_type_t ct_mysql_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "mysql_server",
  .accept = accept_new_connections,
  .init_accepted = sqls_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = sqls_parse_execute,
  .close = server_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = sqls_wakeup,
  .alarm = sqls_alarm,
  .ready_to_write = sqls_ready_to_write,
#ifdef AES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};


int sqls_execute (struct connection *c, int op);
int sqls_password (struct connection *c, const char *user, char buffer[20]);

struct mysql_server_functions default_mysql_server = {
  .server_charset = cp1251_general_ci,
  .execute = sqls_execute,
  .sql_password = sqls_password,
  .sql_wakeup = sqls_do_wakeup,
  .sql_alarm = sqls_do_wakeup,
  .sql_flush_packet = sqls_flush_packet,
  .sql_check_perm = sqls_default_check_perm,
  .sql_init_crypto = sqls_init_crypto,
  .sql_start_crypto = sqls_start_crypto
};


char select_database_answer [] = {
1, 0, 0, 1, 1, 

32, 0, 0, 2, 
3, 'd', 'e', 'f', 0, 0, 0,
10, 'D', 'A', 'T', 'A', 'B', 'A', 'S', 'E', '(', ')', 0, 0x0c, 0x33, 0, 0x22,
0, 0, 0, 0xfd, 0, 0, 0x1f, 0, 0, 

5, 0, 0, 3, 0xfe, 0, 0, 2, 0, 

11, 0, 0, 4, 
10,
'b', 'o', 'x', 'e', 'd', '_', 'b', 'a', 's', 'e',

5, 0, 0, 5, 0xfe, 0, 0, 2, 0
};

char select_version_answer [] = {
1, 0, 0, 1, 1, 

39, 0, 0, 2, 
3, 'd', 'e', 'f', 0, 0, 0,
17, '@', '@', 'v', 'e', 'r', 's', 'i', 'o', 'n', '_', 'c',
'o', 'm', 'm', 'e', 'n', 't', 0, 0x0c, 0x33, 0, 0x22,
0, 0, 0, 0xfd, 1, 0, 0x1f, 0, 0, 

5, 0, 0, 3, 0xfe, 0, 0, 2, 0, 

9, 0, 0, 4, 
8,
'(', 'D', 'e', 'b', 'i', 'a', 'n', ')',

5, 0, 0, 5, 0xfe, 0, 0, 2, 0
};


enum sql_query_parse_state {
  sqp_start,
  sqp_done
};

#pragma pack(push,1)

struct init_packet {
  int len;
  char proto_ver;
  char server_ver[7+1];
  struct mysql_auth_packet_end e;
};

#ifdef AES

struct init_packet_aes {
  int len;
  char proto_ver;
  char server_ver[7];
  char server_nonce[1+2+8+1+32+1];
  struct mysql_auth_packet_end e;
};

#endif

#pragma pack(pop)

static struct init_packet P = {
  .len = sizeof (struct init_packet) - 4,
  .proto_ver = 10,
  .server_ver = "5.0.239",
  .e = {
    .server_capabilities = 0xa20c,
    .server_language = cp1251_general_ci,
    .server_status = 2
  }
};

#ifdef AES

static struct init_packet_aes P_AES = {
  .len = sizeof (struct init_packet_aes) - 4,
  .proto_ver = 10,
  .server_ver = "5.0.239",
  .server_nonce = "-A:",
  .e = {
    .server_capabilities = 0xa20c,
    .server_language = cp1251_general_ci,
    .server_status = 2
  }
};

#endif

int write_lcb (struct connection *c, unsigned long long l) {
  int res = 0;

  if (l <= 250) {
    res += write_out (&c->Out, (void *)&l, 1);
  } else 
  if (l <= 0xffff) {
    res += write_out (&c->Out, "\xfc", 1);
    res += write_out (&c->Out, (void *)&l, 2);
  } else
  if (l <= 0xffffff) {
    res += write_out (&c->Out, "\xfd", 1);
    res += write_out (&c->Out, (void *)&l, 3);
  } else {
    res += write_out (&c->Out, "\xfe", 1);
    res += write_out (&c->Out, (void *)&l, 8);
  }
  return res;
}


int sqls_flush_packet (struct connection *c, int packet_len) {
  int pad_bytes = 0;
  if (c->crypto) {
    pad_bytes = c->type->crypto_needed_output_bytes (c);
    if (packet_len >= 0) {
      int b = SQLS_DATA(c)->block_size;
      packet_len += 4;
      packet_len = b - packet_len % b;
      if (packet_len == b) {
        packet_len = 0;
      }
      assert (packet_len == pad_bytes);
    }
    if (verbosity > 1) {
      fprintf (stderr, "sqls_flush_query: padding with %d bytes\n", pad_bytes);
    }
    if (pad_bytes > 0) {
      static char pad_str[16];
      assert (pad_bytes <= 15);
      memset (pad_str, pad_bytes, pad_bytes);
      write_out (&c->Out, pad_str, pad_bytes);
    }
  }
  return pad_bytes;
}




int send_ok_packet (struct connection *c, unsigned long long affected_rows,
                    unsigned long long insert_id, int server_status, 
                    int warning_count, const char *message, int msg_len,
                    int sequence_number) {
  nbw_iterator_t it;

  int res = 0;

  nbit_setw (&it, &c->Out);
  sequence_number <<= 24;
  write_out (&c->Out, &sequence_number, 4);

  res += write_out (&c->Out, "", 1);
  res += write_lcb (c, affected_rows);
  res += write_lcb (c, insert_id);
  res += write_out (&c->Out, &server_status, 2);
  res += write_out (&c->Out, &warning_count, 2);
  res += write_out (&c->Out, message, msg_len);
  
  nbit_write_out (&it, &res, 3);

  sqls_flush_packet (c, res);
  c->flags |= C_WANTWR;
  
  return res + 4;
}


int send_error_packet (struct connection *c, int error_no,
                       int sql_state, const char *message, int msg_len,
                       int sequence_number) {
  nbw_iterator_t it;
  char buff[16];

  int res = 0;

  nbit_setw (&it, &c->Out);
  sequence_number <<= 24;
  write_out (&c->Out, &sequence_number, 4);

  res += write_out (&c->Out, "\xff", 1);
  res += write_out (&c->Out, &error_no, 2);
  sprintf (buff, "#%05d", sql_state);
  res += write_out (&c->Out, buff, 6);
  res += write_out (&c->Out, message, msg_len);
  
  nbit_write_out (&it, &res, 3);

  sqls_flush_packet (c, res);
  c->flags |= C_WANTWR;
  
  return res + 4;
}



int sqls_init_accepted (struct connection *c) {
  struct sqls_data *D = SQLS_DATA(c);
  struct mysql_auth_packet_end *e = &P.e;
  int i, res;

  if (SQLS_FUNC(c)->sql_check_perm) {
    res = SQLS_FUNC(c)->sql_check_perm(c);
    if (verbosity > 1) {
      fprintf (stderr, "sql_check_perm(%d) = %d\n", c->fd, res);
    }
    if (res < 0) {
      res = 0;
    }
    if (!(res &= 3)) {
      return -1;
    }
  } else {
    res = 1;
  }

  D->crypto_flags = res;

#ifdef AES
  if (res & 2) {
    res = SQLS_FUNC(c)->sql_init_crypto (c, P_AES.server_nonce, sizeof (P_AES.server_nonce));
    if (verbosity > 1) {
      fprintf (stderr, "sql_init_crypto(%d) = %d %s\n", c->fd, res, P_AES.server_nonce);
    }
    if (res == sizeof (P_AES.server_nonce)) {
      e = &P_AES.e;
      D->crypto_flags |= 4;
    } else {
      D->crypto_flags &= 1;
    }
  }
#else
  D->crypto_flags &= 1;
#endif

  if (!D->crypto_flags) {
    return -1;
  }

  for (i = 0; i < 20; i++) {
    D->scramble[i] = rand() % 94 + 33;
  }

  e->server_language = SQLS_FUNC(c)->server_charset;
  if (!e->server_language) {
    e->server_language = SQLS_FUNC(c)->server_charset = cp1251_general_ci;
  }
  memcpy (e->scramble1, D->scramble, 8);
  memcpy (e->scramble2, D->scramble+8, 13);
  e->thread_id = c->fd;

  D->auth_state = sql_auth_sent;

#ifdef AES
  if (e == &P_AES.e) {
    write_out (&c->Out, &P_AES, sizeof (struct init_packet_aes));
#else
  if (0) {
#endif
  } else {
    write_out (&c->Out, &P, sizeof (struct init_packet));
  }
  c->flags |= C_WANTRW;
  c->type->writer (c);

  return 0;
}


static int sqls_inner_authorise (struct connection *c) {
  //dump_connection_buffers (c);
  struct sqls_data *D = SQLS_DATA(c);
  int len = D->packet_len, ulen, user_scramble_len, database_name_len, i;
  char *p, *q, *username, *user_scramble, *database_name;
  char password_sha1[48], stage1_hash[20];

  if (verbosity > 1) {
    fprintf (stderr, "client_auth_packet received, len=%d\n", D->packet_len);
  }
  if (len >= 0x800) {
    if (verbosity > 0) {
      fprintf (stderr, "client_auth_packet too large\n");
    }
    return -1;
  }
  if (len < 34) {
    if (verbosity > 0) {
      fprintf (stderr, "client_auth_packet too small\n");
    }
    return -1;
  }
  assert (force_ready_bytes (&c->In, len+4) >= len+4);
  p = ((char *)get_read_ptr (&c->In)) + 4;
  D->client_flags = *(int *) (p);
  D->max_packet_size = *(int *) (p + 4);
  q = p + len;
  username = p += 32;

  if (verbosity > 1) {
    fprintf (stderr, "max packet size = %d, client flags = %d, username starts with %.10s\n", D->max_packet_size, D->client_flags, username);
  }

  while (p < q && *p) {
    p++;
  }
  if (p == q || p > username + 31) {
    return -1;
  }

  if (verbosity > 1) {
    fprintf (stderr, "user name = %s\n", username);
  }

  ulen = p - username;
  p++;
  if (p == q || (unsigned char) *p > 20 || p + (unsigned char) *p + 1 > q) {
    return -1;
  }
  user_scramble = p+1;
  user_scramble_len = (unsigned char) *p;
  
  if (user_scramble_len && user_scramble_len != 20) {
    return -1;
  }

  p += user_scramble_len + 1;

  database_name = 0;
  database_name_len = 0;
  if (p < q) {
    database_name = p;
    database_name_len = q - p - 1;
    if (q[-1] || database_name_len > 128 || strlen(p) != database_name_len) {
      return -1;
    }
  }

  if (verbosity > 1) {
    fprintf (stderr, "database name = %s\n", database_name);
  }

  memset (password_sha1+20, 0, 20);

  int res = SQLS_FUNC(c)->sql_password (c, username, password_sha1+20);

  if (!user_scramble_len) {
    for (i = 0; i < 20; i++)
      if (password_sha1[i+20]) {
        return 0;
      }
    return res;
  }

  if (res <= 0) {
    return res;
  }
  for (i = 0; i < 20; i++) {
    if (password_sha1[i+20]) {
      break;
    }
  }

  if (i == 20) {
    return res;
  }

  memcpy (password_sha1, D->scramble, 20);
  SHA1 ((unsigned char *)password_sha1, 40, (unsigned char *)stage1_hash);

  for (i = 0; i < 20; i++) {
    user_scramble[i] ^= stage1_hash[i];
  }

  SHA1 ((unsigned char *)user_scramble, 20, (unsigned char *)stage1_hash);

  if (memcmp (stage1_hash, password_sha1+20, 20)) {
    return 0;
  }

  return res;
}


int sqls_parse_execute (struct connection *c) {
  struct sqls_data *D = SQLS_DATA(c);
  int len = nbit_total_ready_bytes (&c->Q);
  static unsigned int psize;
  if (verbosity > 1) {
    fprintf (stderr, "sqls_parse_execute(%d), bytes=%d\n", c->fd, len);
  }

  while (len > 0 && !(c->flags & (C_FAILED | C_STOPREAD))) {
    c->status = conn_reading_query;
    if (D->packet_state == 0) {
      if (len < 4) {
        return 4 - len;
      }
      assert (nbit_read_in (&c->Q, &psize, 4) == 4);
      len -= 4;
      D->packet_state = 1;
      D->packet_len = psize & 0xffffff;
      D->output_packet_seq = D->packet_seq = (psize >> 24);
      if (D->block_size > 1) {
        D->packet_padding = D->block_size - (D->packet_len + 4) % D->block_size;
        if (D->packet_padding == D->block_size) {
          D->packet_padding = 0;
        }
      }
    }
    assert (D->packet_state == 1);
    if (len < D->packet_len + D->packet_padding) {
      return D->packet_len + D->packet_padding - len;
    }
    /* complete packet ready */
    if (verbosity > 1) {
      fprintf (stderr, "client packet ready: len=%d, padding=%d, seq_num=%d\n", D->packet_len, D->packet_padding, D->packet_seq);
    }
    if (D->auth_state == sql_auth_sent) {
      int res;
      if (D->packet_len == 16 && D->packet_seq == 0xcc && (D->crypto_flags & 14) == 6) {
        static char nonce_buff[20];
        assert (read_in (&c->In, nonce_buff, D->packet_len + 4) == 20);
        res = SQLS_FUNC(c)->sql_start_crypto (c, nonce_buff + 4, 16);
        if (res != 1) {
          res = -1;
        } else {
          D->crypto_flags = (D->crypto_flags & -16) | 10;
          assert (c->type->crypto_decrypt_input && c->crypto && c->In.pptr);
          c->type->crypto_decrypt_input (c);
          nbit_set (&c->Q, &c->In);
          len = nbit_total_ready_bytes (&c->Q);
          D->packet_state = 0;
          c->status = conn_expect_query;
          continue;
        }
      } else {
        if (D->crypto_flags & 4) {
          D->crypto_flags &= ~6;
          if (!(D->crypto_flags & 1)) {
            c->status = conn_error;
            c->error = -1;
            return 0;
          }
        }
        res = sqls_inner_authorise(c);
      }
      if (res < 0) {
        c->status = conn_error;
        c->error = -1;
        return 0;
      }
      if (res) {
        /* send ok packet */
        send_ok_packet (c, 0, 0, 2, 0, "Success", 7, 2);
        if (verbosity > 1) {
          fprintf (stderr, "authorized ok\n");
        }
        D->auth_state = sql_auth_ok;
        D->auth_user = res;
      } else {
        if (verbosity > 1) {
          fprintf (stderr, "authorization error\n");
        }
        send_error_packet (c, 1045, 28000, "Failed", 6, 2);
        /* send error packet */
      }
      advance_skip_read_ptr (&c->In, D->packet_len + D->packet_padding + 4);
    } else {
      int op = (D->packet_len > 0 ? *(char *) nbit_get_ptr (&c->Q) : -1);

      assert (D->auth_state == sql_auth_ok);

      if (verbosity > 1) {
        fprintf (stderr, "execute, op=%d\n", op);
      }

      int keep_total_bytes = c->In.total_bytes;

      /* execute */
      c->status = conn_running;
      int res = SQLS_FUNC(c)->execute (c, op);

      //dump_connection_buffers (c);

      if (res == SKIP_ALL_BYTES) {
//      assert (keep_total_bytes == c->In.total_bytes);  // this assert FAILS!
        if (keep_total_bytes != c->In.total_bytes) {
          fprintf (stderr, "error: in SKIP_ALL_BYTES for connection %d: keep_total=%d != total_bytes=%d, packet_len=%d, packet_padding=%d, packet_state=%d, packet_seq=%d, op=%d, status=%d\n", 
          		    c->fd, keep_total_bytes, c->In.total_bytes, D->packet_len, D->packet_padding, D->packet_state, D->packet_seq, op, c->status);
        }
        advance_skip_read_ptr (&c->In, D->packet_len + D->packet_padding + 4);
      } else if (keep_total_bytes != c->In.total_bytes) {
        assert (keep_total_bytes == c->In.total_bytes + D->packet_len + 4);
        advance_skip_read_ptr (&c->In, D->packet_padding);
      }
      if (c->status == conn_running) {
        c->status = conn_expect_query;
      }
    }
    D->packet_state = 0;
    if (c->status != conn_expect_query) {
      break;
    }
    nbit_set (&c->Q, &c->In);
    len = nbit_total_ready_bytes (&c->Q);
  }

  return 0;
}

//If you are using this function, base names other than boxed_base
//are not supported.
int sqls_builtin_execute (struct connection *c, int op) {
  struct sqls_data *D = SQLS_DATA(c);
  if (verbosity > 1) {
    fprintf (stderr, "in sqls_execute, conn=%d, packet_len=%d, op=%d\n", c->fd, D->packet_len, op);
  }
  if (op == MYSQL_COM_INIT_DB && D->packet_len == sizeof("boxed_base")) {
    static char buffer[256];
    read_in (&c->In, buffer, D->packet_len + 4);
    if (!memcmp (buffer+5, "boxed_base", sizeof("boxed_base")-1)) {
      D->custom = mrand48 ();
      send_ok_packet (c, 0, 0, 2, 0, 0, 0, D->packet_seq + 1);
    }
    return 0;
  }
  if (op == MYSQL_COM_PING) {
    assert (advance_skip_read_ptr (&c->In, D->packet_len + 4) == D->packet_len + 4);
    send_ok_packet (c, 0, 0, 2, 0, 0, 0, D->packet_seq + 1);
    return 0;
  }
  if (op == MYSQL_COM_QUERY) {
    static char buffer[256];

    nb_iterator_t it;
    nbit_set (&it, &c->In);
    int l = nbit_read_in (&it, buffer, 255);

    if (verbosity > 1) {
      fprintf (stderr, "received query: %.*s\n", l-5, buffer+5);
    }

    if (l == 22 && !memcmp (buffer + 5, "SELECT DATABASE()", 17)) {
      write_out (&c->Out, select_database_answer, sizeof (select_database_answer));
      advance_skip_read_ptr (&c->In, D->packet_len + 4);
      return 0;
    }
    if (l == sizeof ("select @@version_comment limit 1") + 4 &&
        !memcmp (buffer + 5, "select @@version_comment limit 1",
        sizeof ("select @@version_comment limit 1") - 1)) {
      write_out (&c->Out, select_version_answer, sizeof (select_version_answer));
      advance_skip_read_ptr (&c->In, D->packet_len + 4);
      return 0;
    }
  }
  return SKIP_ALL_BYTES;
}

int sqls_execute (struct connection *c, int op) {
  int res = sqls_builtin_execute (c, op);
  if (res == SKIP_ALL_BYTES) {
    send_error_packet (c, 1045, 28000, "Failed", 6, 1);
  }
  return res;
}


static int sqls_wakeup (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "sqls_wakeup for #%d: status=%d\n", c->fd, c->status);
  }
  if (c->status == conn_wait_net) {
    SQLS_FUNC(c)->sql_wakeup (c);
    if (c->status == conn_wait_net) {
      c->status = conn_expect_query;
    }
  }
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}

int sqls_password (struct connection *c, const char *user, char buffer[20]) {
  memset (buffer, 0, 20);

  if (!strcmp (user, "kitten")) {
    unsigned char buffer2[20];
    SHA1 ((unsigned char *)"test", 4, buffer2);
    SHA1 (buffer2, 20, (unsigned char *)buffer);
    return 2;
  }
  
  return 1;
}


static int sqls_alarm (struct connection *c) {
  SQLS_FUNC(c)->sql_alarm (c);
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}

int sqls_do_wakeup (struct connection *c) {
//  struct sqls_data *D = SQLS_DATA(c);
//  if (D->query_type == sqlt_SELECT) {
    /* return DROP DATABASE to client */
//  }

  return 0;
}

static int sqls_ready_to_write (struct connection *c) {
  if (SQLS_FUNC(c)->sql_ready_to_write) {
    SQLS_FUNC(c)->sql_ready_to_write (c);
  }
  return 0;
}


/*
 *
 *		CRYPTO
 *
 */

#ifdef AES


int sqls_default_check_perm (struct connection *c) {
  if ((c->remote_ip & 0xff000000) != 0x0a000000 && (c->remote_ip & 0xff000000) != 0x7f000000) {
    return 0;
  }
  if (!c->our_ip) {
    return 3;
  }
  if (aes_initialized <= 0) {
    return ((c->our_ip ^ c->remote_ip) & 0xffff0000) ? 0 : 1;
  }
  return ((c->our_ip ^ c->remote_ip) & 0xffff0000) ? 2 : 3;
}

int sqls_init_crypto (struct connection *c, char *buff, int len) {
  SQLS_DATA(c)->nonce_time = time (0);

  aes_generate_nonce (SQLS_DATA(c)->nonce);

  if (len != 45) {
    return -1;
  }

  int res = sprintf (buff, "-A:%08x:%016llx%016llx", 
    SQLS_DATA(c)->nonce_time,
    *(long long *)SQLS_DATA(c)->nonce,
    *(long long *)(SQLS_DATA(c)->nonce + 8));



  assert ((SQLS_DATA(c)->crypto_flags & 14) == 2);
  assert (res == 44);

  return res + 1;
}


int sqls_start_crypto (struct connection *c, char *key, int key_len) {
  struct sqls_data *D = SQLS_DATA(c);

  if (c->crypto) {
    return -1;
  }
  if (key_len != 16) {
    return -1;
  }

  if (c->Out.total_bytes || !D->nonce_time) {
    return -1;
  }

  struct aes_key_data aes_keys;

  if (aes_create_keys (&aes_keys, 0, D->nonce, key, D->nonce_time, c->our_ip, c->our_port, c->our_ipv6, c->remote_ip, c->remote_port, c->remote_ipv6) < 0) {
    return -1;
  }

  if (aes_crypto_init (c, &aes_keys, sizeof (aes_keys)) < 0) {
    return -1;
  }

  D->block_size = 16;

  mark_all_unprocessed (&c->In);
  mark_all_processed (&c->Out);

  return 1;
}

#else

int sqls_default_check_perm (struct connection *c) {
  if ((c->remote_ip & 0xff000000) != 0x0a000000 && (c->remote_ip & 0xff000000) != 0x7f000000) {
    return 0;
  }
  if (!c->our_ip) {
    return 1;
  }
  return ((c->our_ip ^ c->remote_ip) & 0xffff0000) ? 0 : 1;
}

int sqls_init_crypto (struct connection *c, char *init_buff, int init_len) {
  return -1;
}

int sqls_start_crypto (struct connection *c, char *key, int key_len) {
  return -1;
}

#endif



/*
 *
 *		END (MySQL SERVER)
 *
 */

