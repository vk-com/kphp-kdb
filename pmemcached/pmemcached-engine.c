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

    Copyright 2011-2013 Vkontakte Ltd
                   2010 Arseny Smirnov (Original memcached code)
                   2010 Aliaksei Levin (Original memcached code)
              2011-2013 Vitaliy Valtman
                   2011 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <signal.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "kdb-pmemcached-binlog.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "common-data.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#include "pmemcached-data.h"
#include "vv-tl-parse.h"
#include "vv-tl-aio.h"
#include "am-stats.h"

#include "TL/constants.h"

#include "pmemcached-interface-structures.h"

#define	MAX_KEY_LEN	1000
#define MAX_KEY_RETRIES	3
#define	MAX_VALUE_LEN	(1 << 20)

#define	VERSION "1.03"
#define	VERSION_STR "pmemcached "VERSION
#ifndef COMMIT
#define COMMIT "unknown"
#endif
const char FullVersionStr[] = "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
     "64-bit"
#else
     "32-bit"
#endif
      " after commit " COMMIT;
;

#define TCP_PORT 11211
#define UDP_PORT 11211

#define MAX_NET_RES	(1L << 16)

#define	PING_INTERVAL	10
#define	RESPONSE_FAIL_TIMEOUT	5

#define SEC_IN_MONTH (60 * 60 * 24 * 30)

#define mytime() get_utime (CLOCK_MONOTONIC)


#define MAX_WILDCARD_LEN (1 << 19)

/*
 *
 *		MEMCACHED PORT
 *
 */


int mcp_get_start (struct connection *c);
int mcp_get (struct connection *c, const char *key, int key_len);
int mcp_get_end (struct connection *c, int key_count);
int mcp_wakeup (struct connection *c);
int mcp_alarm (struct connection *c);
int mcp_stats (struct connection *c);
int mcp_check_ready (struct connection *c);

int pmemcached_engine_wakeup (struct connection *c);
int pmemcached_engine_alarm (struct connection *c);

conn_type_t ct_pmemcached_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "pmemcached_engine_server",
  .accept = accept_new_connections,
  .init_accepted = mcs_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = mcs_parse_execute,
  .close = server_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = pmemcached_engine_wakeup,
  .alarm = pmemcached_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

/*conn_type_t ct_pmemcached_rpc_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "pmemcached_engine_server",
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
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};*/




int backlog = BACKLOG, port = TCP_PORT, udp_port = UDP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
long long max_memory = MAX_MEMORY;

struct in_addr settings_addr;
int verbosity = 0, interactive = 0;
int return_false_if_not_found = 0;
int quit_steps;
double index_load_time, binlog_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos;
long long binlog_loaded_size;
long long binlog_readed;
int index_mode = 0;
long long snapshot_size;
long long index_size;
int index_type;
volatile int force_write_index;
volatile int force_check_aio;
int metafile_size = METAFILE_SIZE;
int reindex_on_low_memory;
int last_reindex_on_low_memory_time;
int disable_wildcard;
int protected_mode;
int udp_enabled;

char *aes_pwd_file;

int metafile_mode = 1;
char *progname = "pmemcached-1.02", *username, *logname, *binlogname;

int pack_mode;
int pack_fd;

int start_time;

long long cmd_get, cmd_set, get_hits, get_missed, cmd_incr, cmd_decr, cmd_delete, cmd_version, cmd_stats, curr_items, total_items;
long long tot_response_words, tot_response_bytes;

#define STATS_BUFF_SIZE	(1 << 21)
char stats_buff[STATS_BUFF_SIZE];

char key_buff[MAX_KEY_LEN+1];
char value_buff[MAX_VALUE_LEN+1];

//extern struct aio_connection *WaitAio;
extern conn_query_type_t aio_metafile_query_type;


int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);
int pmemcached_get_start (struct connection *c);
int pmemcached_get_end (struct connection *c, int key_count);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = pmemcached_get_start,
  .mc_get = memcache_get,
  .mc_get_end = pmemcached_get_end,
  .mc_incr = memcache_incr,
  .mc_delete = memcache_delete,
  .mc_version = memcache_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct rpc_server_functions memcache_rpc_server = {
  .execute = default_tl_rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_close = default_tl_close_conn,
  .memcache_fallback_type = &ct_pmemcached_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


static int mcs_crypted_check_perm (struct connection *c) {
  // copypasted from password-engine
  if (c->our_ip == 0x7f000001 && c->remote_ip == 0x7f000001) {
    return 3;
  }
  return mcs_default_check_perm (c) & -2;
}

static int rpcs_crypted_check_perm (struct connection *c) {
  if (c->our_ip == 0x7f000001 && c->remote_ip == 0x7f000001) {
    return 3;
  }
  return rpcs_default_check_perm(c) & -2;
}

int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags) {
  static char buff[65536];
  int l = sprintf (buff, "VALUE %s %d %d\r\n", key, flags, vlen);
  assert (l <= 65536);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return l;
}

int return_one_key_flags_len (struct connection *c, const char *key, int key_len, char *val, int vlen, int flags) {
  static char buff[65536];
  int l = sprintf (buff, "VALUE ");
  memcpy (buff + l, key, key_len);
  l += key_len;
  l += sprintf (buff + l, " %d %d\r\n", flags, vlen);
  assert (l <= 65536);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return l;
}

int memcache_wait (struct connection *c) {
  if (verbosity > 2) {
    fprintf (stderr, "wait for aio..\n");
  }
  if (c->flags & C_INTIMEOUT) {
    if (verbosity > 1) {
      fprintf (stderr, "memcache: IN TIMEOUT (%p)\n", c);
    }
    return 0;
  }
  if (c->Out.total_bytes > 8192) {
    c->flags |= C_WANTWR;
    c->type->writer (c);
  }
//    fprintf (stderr, "memcache_get_nonblock returns -2, WaitAio=%p\n", WaitAio);
  if (!WaitAioArrPos) {
    fprintf (stderr, "WaitAio=0 - no memory to load user metafile, query dropped.\n");
    return 0;
  }
  int i;
  for (i = 0; i < WaitAioArrPos; i++) {
    conn_schedule_aio (WaitAioArr[i], c, 0.7, &aio_metafile_query_type);
  }
  set_connection_timeout (c, 0.5);
  WaitAioArrClear ();
  return 1;
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  if (binlog_disabled == 1) {
    return -2;
  }
  if (protected_mode) {
    return -2;
  }
  c->flags &= ~C_INTIMEOUT;
  cmd_set++;

  if (delay == 0) {
    delay = DELAY_INFINITY;
  } else if (delay <= SEC_IN_MONTH) {
    delay += get_double_time_since_epoch();
  }

  if (verbosity >= 3) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d, flags = %d, exp_time = %d\n", key, key_len, size, flags, delay);
  }

  int force_create = 0;
  if (key_len >= 6 && !strncmp (key, "##incr##", 8)) {
    force_create = 1;
    key += 8;
    key_len -= 8;
  }

  if (size < MAX_VALUE_LEN) {
    int x = do_pmemcached_preload (key, key_len, op != mct_set || force_create);
    if (x == -2) {
      if (memcache_wait (c)) {
        return 0;
      } else {
        assert (read_in (&c->In, value_buff, size) == size);
        return 0;
      }
    } else {
      assert (read_in (&c->In, value_buff, size) == size);
      value_buff[size] = 0;
      int result;
      if (force_create) {
        if (x == 0) {
          result = do_pmemcached_store (op, key, key_len, flags, delay, value_buff, size);
        } else {
          long long arg = atoll (value_buff);
          assert (do_pmemcached_incr (0, key, key_len, arg) != -2);
          result = 1;
        }
      } else {
        result = do_pmemcached_store (op, key, key_len, flags, delay, value_buff, size);
      }
      assert (result != -2);
      return result;
    }
  }

  return -2;
}

int pmemcache_prepare_stats (struct connection *c);

int pmemcache_match_stats_key (const char *key, int key_len) {
  if (key_len < 5 || strncmp (key, "stats", 5)) { return 0; }
  if (key_len == 5) {
    return 1;
  }
  if (key_len >= 10 && key[5] == '#' && key[key_len - 1] == '$' && key[key_len - 2] == '#' && key[key_len - 3] == '@') {
    int i = 6, j;
    if (key[i] == '-') {
      i++;
    }
    for (j = i; isdigit (key[j]); j++) {}
    if (i < j && j == key_len - 3) { return 1; }
  }
  return 0;
}

#define MC_STORE_MAGIC 0x5489120f

#define MAX_TOTAL_DATA 10000000
struct keep_mc_store {
  int magic;
  int key_len;
  int prefix_len;
  int total_keys_sent;
  long long total_data_sent;
  char *ptr;
  char key[0];
};

char __buff[2048];
struct keep_mc_store *mc_store = (struct keep_mc_store *)__buff;;
int read_last_kept (struct connection *c, int force) {
  if (c->Tmp) {
    nb_iterator_t R;
    nbit_set (&R, c->Tmp);
    int l = nbit_read_in (&R, mc_store, sizeof (struct keep_mc_store));
    if (l > 0 && (force || mc_store->magic == MC_STORE_MAGIC)) {
      assert (l == sizeof (struct keep_mc_store));
      assert (mc_store->magic == MC_STORE_MAGIC);
      assert (advance_skip_read_ptr (c->Tmp, sizeof (struct keep_mc_store)) == sizeof (struct keep_mc_store));
      assert (read_in (c->Tmp, mc_store->key, mc_store->key_len) == mc_store->key_len);
      assert (!c->Tmp->total_bytes);
      //assert ((nbit_read_in (&R, mc_store->key, mc_store->key_len)) == mc_store->key_len);
      return 1;
    }
    return 0;
  }
  return 0;
}

int write_last_kept (struct connection *c, const char *key, int key_len, int prefix_len, long long total_data_sent, int total_keys_sent, char *ptr) {
  if (!c->Tmp) {
    c->Tmp = alloc_head_buffer();
    assert (c->Tmp);
  }
  assert (!c->Tmp->total_bytes);
  mc_store->key_len = key_len;
  mc_store->magic = MC_STORE_MAGIC;
  mc_store->prefix_len = prefix_len;
  mc_store->total_data_sent = total_data_sent;
  mc_store->total_keys_sent = total_keys_sent;
  mc_store->ptr = ptr;
  write_out (c->Tmp, mc_store, sizeof (struct keep_mc_store));
  write_out (c->Tmp, key, key_len);
  return 0;
}

int wildcard_arrays_allocated;
char wildcard_type;
char *wildcard_ptr;
int wildcard_total_data_sent;
int wildcard_total_keys_sent;
const char *wildcard_last_key;
int wildcard_last_key_len;
int wildcard_prefix_len;
struct connection *wildcard_connection;

int wildcard_engine_mc_report (const char *key, int key_len, struct data x) {
  //return;
  assert (key);
  assert (key_len <= 1024 && key_len >= 0);
  assert (x.data_len >= 0);
  assert (key_len >= wildcard_prefix_len);
  wildcard_last_key = key;
  wildcard_last_key_len = key_len;
  if (wildcard_type == '#') {
    if (wildcard_total_data_sent + key_len - wildcard_prefix_len + x.data_len + 1000 < MAX_WILDCARD_LEN) {
      if (metafile_mode) {
        //wildcard_ptr[wildcard_total_data_sent++] = 's';
        //wildcard_ptr[wildcard_total_data_sent++] = ':';
        //wildcard_total_data_sent += sprintf (wildcard_ptr + wildcard_total_data_sent, "%d", key_len - wildcard_prefix_len);
        //wildcard_ptr[wildcard_total_data_sent++] = ':';
        //wildcard_ptr[wildcard_total_data_sent++] = '"';
        wildcard_total_data_sent += sprintf (wildcard_ptr + wildcard_total_data_sent, "s:%d:\"", key_len - wildcard_prefix_len);
        memcpy (wildcard_ptr + wildcard_total_data_sent, key + wildcard_prefix_len, key_len - wildcard_prefix_len);
        wildcard_total_data_sent += key_len - wildcard_prefix_len;
        wildcard_total_data_sent += sprintf (wildcard_ptr + wildcard_total_data_sent, "\";");
        if (!(x.flags & 1)) {
          wildcard_total_data_sent += sprintf (wildcard_ptr + wildcard_total_data_sent, "s:%d:\"", x.data_len);
        }
        memcpy (wildcard_ptr + wildcard_total_data_sent, x.data, x.data_len);
        wildcard_total_data_sent += x.data_len;
        if (!(x.flags & 1)) {
          wildcard_total_data_sent += sprintf (wildcard_ptr + wildcard_total_data_sent, "\";");
        }
        wildcard_total_keys_sent ++;
      } else {
        static char s[2000];
        wildcard_total_data_sent += write_out (&wildcard_connection->Out, s, sprintf (s, "s:%d:\"", key_len - wildcard_prefix_len));
        wildcard_total_data_sent += write_out (&wildcard_connection->Out, key + wildcard_prefix_len, key_len - wildcard_prefix_len);
        if (!(x.flags & 1)) {
          wildcard_total_data_sent += write_out (&wildcard_connection->Out, s, sprintf (s, "\";s:%d:\"", x.data_len));
        } else {
          wildcard_total_data_sent += write_out (&wildcard_connection->Out,  "\";", 2);
        }
        wildcard_total_data_sent += write_out (&wildcard_connection->Out, x.data, x.data_len);
        if (!(x.flags & 1)) {
          wildcard_total_data_sent += write_out (&wildcard_connection->Out, "\";", 2);
        }
        wildcard_total_keys_sent ++;
      }
      return 1;
    } else {
      if (metafile_mode) {
        wildcard_total_data_sent += sprintf (wildcard_ptr + wildcard_total_data_sent, "s:11:\"###error###\";s:8:\"not_full\";");
        wildcard_total_keys_sent ++;
      } else {
        wildcard_total_data_sent += write_out (&wildcard_connection->Out, "s:11:\"###error###\";s:8:\"not_full\";", strlen ("s:11:\"###error###\";s:8:\"not_full\";"));
        wildcard_total_keys_sent ++;
      }
      return 0;
    }

  } else {
    wildcard_total_data_sent += return_one_key_flags_len (wildcard_connection, key + wildcard_prefix_len, key_len - wildcard_prefix_len, x.data, x.data_len, x.flags);
    wildcard_total_keys_sent ++;
    return 1;
  }
}

int wildcard_engine_rpc_report (const char *key, int key_len, struct data x) {
  //return;
  assert (key);
  assert (key_len <= 1024 && key_len >= 0);
  assert (x.data_len >= 0);
  assert (key_len >= wildcard_prefix_len);
  wildcard_last_key = key;
  wildcard_last_key_len = key_len;
  if (wildcard_total_data_sent + key_len - wildcard_prefix_len + x.data_len + 1000 < MAX_WILDCARD_LEN) {
    wildcard_total_data_sent += tl_write_string (key + wildcard_prefix_len, key_len - wildcard_prefix_len, wildcard_ptr + wildcard_total_data_sent, MAX_WILDCARD_LEN - wildcard_total_data_sent);    
    wildcard_total_data_sent += tl_write_string (x.data, x.data_len, wildcard_ptr + wildcard_total_data_sent, MAX_WILDCARD_LEN - wildcard_total_data_sent);
    wildcard_total_keys_sent ++;
    return 1;
  } else {
    wildcard_total_data_sent += tl_write_string ("###error###", 11, wildcard_ptr + wildcard_total_data_sent, MAX_WILDCARD_LEN - wildcard_total_data_sent);
    wildcard_total_data_sent += tl_write_string ("not_full", 8, wildcard_ptr + wildcard_total_data_sent, MAX_WILDCARD_LEN - wildcard_total_data_sent);
    wildcard_total_keys_sent ++;
    return 0;
  }
}

int (*wildcard_engine_report) (const char *key, int key_len, struct data x);

void wildcard_report_finish (const char *key, int key_len) {
  if (wildcard_type == '#') {
    if (metafile_mode) {
      memcpy (wildcard_ptr + wildcard_total_data_sent, "}", 1);
      wildcard_total_data_sent ++;
    } else {
      wildcard_total_data_sent ++;
      write_out (&wildcard_connection->Out, "}\r\n", 3);
    }
    if (metafile_mode) {
      static char s[12];
      sprintf (s, "%09d", wildcard_total_keys_sent);
      memcpy (wildcard_ptr + 2, s, 9);
      return_one_key_flags_len (wildcard_connection, key, key_len, wildcard_ptr, wildcard_total_data_sent, 1);
      wildcard_add_value (key, key_len - 1, wildcard_ptr, wildcard_total_data_sent);
      free (wildcard_ptr);
      wildcard_arrays_allocated --;
    } else {
      static char s[12];
      sprintf (s, "%09d", wildcard_total_data_sent);
      memcpy (wildcard_ptr, s, 9);
      sprintf (s, "%09d", wildcard_total_keys_sent);
      memcpy (wildcard_ptr + 13, s, 9);
    }
  }
}

void wildcard_rpc_report_finish (const char *key, int key_len) {
  *(int *)wildcard_ptr = wildcard_total_keys_sent;
  wildcard_add_rpc_value (key, key_len, wildcard_ptr, wildcard_total_data_sent);
  tl_store_raw_data (wildcard_ptr, wildcard_total_data_sent);
}

int memcache_get_wildcard (struct connection *c, const char *key, int key_len) {
  vkprintf (3, "memcache_get_wildcard. key = %s\n", key);
  wildcard_engine_report = wildcard_engine_mc_report;
  if (c->flags & C_INTIMEOUT) {
    return 0;
  }
  int r = read_last_kept (c, 1);
  vkprintf (3, "read_last_kept = %d\n", r);
  wildcard_type = key[key_len - 1];
  wildcard_ptr = 0;

  wildcard_last_key = key;;
  wildcard_last_key_len = key_len - 1;
  wildcard_connection = c;
  if (!r) {
    if (wildcard_type == '#') {
      struct data x = wildcard_get_value (key, key_len - 1);
      if (x.data_len != -1) {
        return_one_key_flags_len (c, key, key_len, x.data, x.data_len, x.flags);
        return 0;
      }
    }
    wildcard_total_data_sent = 0;
    wildcard_total_keys_sent = 0;
    if (wildcard_type == '#') {
      if (metafile_mode) {
        wildcard_ptr = malloc (MAX_WILDCARD_LEN);
        memcpy (wildcard_ptr, "a:000000000:{", 13);
        wildcard_total_data_sent = 13;
        wildcard_arrays_allocated ++;
      } else {
        static char buff[65536];
        int l = sprintf (buff, "VALUE ");
        memcpy (buff + l, key, key_len);
        l += key_len;
        l += sprintf (buff + l, " 1 ");
        write_out (&c->Out, buff, l);
        wildcard_ptr = get_write_ptr (&c->Out, 50);
        write_out (&c->Out, "000000000\r\na:000000000:{", 24);
        wildcard_total_data_sent = 13;
      }
    }
    wildcard_prefix_len = key_len - 1;
  } else {
    assert (metafile_mode);
    //fprintf (stderr, "kept key_len = %d, key = %.*s, prefix_len = %d\n", mc_store->key_len, mc_store->key_len, mc_store->key, mc_store->prefix_len);
    wildcard_last_key_len = mc_store->key_len;
    wildcard_last_key = mc_store->key;
    wildcard_prefix_len = mc_store->prefix_len;
    wildcard_total_data_sent = mc_store->total_data_sent;
    wildcard_total_keys_sent = mc_store->total_keys_sent;
    wildcard_ptr = mc_store->ptr;
  }


  int v = do_pmemcached_get_all_next_keys (wildcard_last_key, wildcard_last_key_len, wildcard_prefix_len, wildcard_total_keys_sent);
  vkprintf (3, "do_pmemcached_get_all_next_keys result: %d\n", v);
  if (v == -2) {
    assert (metafile_mode);
    write_last_kept (c, wildcard_last_key, wildcard_last_key_len, wildcard_prefix_len, wildcard_total_data_sent, wildcard_total_keys_sent, wildcard_ptr);
    memcache_wait (c);
    return 0;
  }
  wildcard_report_finish (key, key_len);
  return 0;
/*  int r = read_last_kept (c);
  int prefix_len;
  long long total_data_sent;
  int total_keys_sent;
  char type = key[key_len - 1];
  char *ptr = 0;
  if (!r) {
    key_len --;
    if (do_pmemcached_preload (key, key_len, 1) == -2) {
      memcache_wait (c);
      return 0;
    }

    struct data x = do_pmemcached_get (key, key_len);
    total_data_sent = 0;
    total_keys_sent = 0;
    if (type == '#') {
      ptr = malloc (MAX_WILDCARD_LEN);
      memcpy (ptr, "a:000000000:{", 13);
      total_data_sent = 13;
      wildcard_arrays_allocated ++;
    }
    prefix_len = key_len;
    if (x.data_len >= 0) {
      if (type == '*') {
        total_data_sent += return_one_key_flags_len (c, "", 0, x.data, x.data_len, x.flags);
        total_keys_sent ++;
      } else if (type == '#') {
        if (total_data_sent +  x.data_len + 20 < MAX_WILDCARD_LEN) {
          total_data_sent += sprintf (ptr + total_data_sent, "s:0:\"\";");
          if (!(x.flags & 1)) {
            total_data_sent += sprintf (ptr + total_data_sent, "s:%d:\"", x.data_len);
          }
          memcpy (ptr + total_data_sent, x.data, x.data_len);
          total_data_sent += x.data_len;
          if (!(x.flags & 1)) {
            total_data_sent += sprintf (ptr + total_data_sent, "\";");
          }
          total_keys_sent ++;
        }
      }
    } else {
      assert (x.data_len == -1);
    }

  } else {
    key_len = mc_store->key_len;
    key = mc_store->key;
    prefix_len = mc_store->prefix_len;
    total_data_sent = mc_store->total_data_sent;
    total_keys_sent = mc_store->total_keys_sent;
    ptr = mc_store->ptr;
  }
  while (1) {
    if (total_data_sent > MAX_TOTAL_DATA) {
      return_one_key_flags (c, "__error__", "ML", 2, 0);
      if (ptr) {
        free (ptr);
        wildcard_arrays_allocated --;
      }
      return 0;
    }
    char *next_key;
    int next_key_len;
    r = do_pmemcached_get_next_key (key, key_len, &next_key, &next_key_len);
    if (r == -1) {
      if (ptr) {
        memcpy (ptr + total_data_sent, "}", 1);
        total_data_sent ++;
        static char s[12];
        sprintf (s, "%09d", total_keys_sent);
        memcpy (ptr + 2, s, 9);
        return_one_key_flags (c, key, ptr, total_data_sent, 1);
        free (ptr);
        wildcard_arrays_allocated --;
      }
      return 0;
    }
    if (r == -2) {
      write_last_kept (c, key, key_len, prefix_len, total_data_sent, total_keys_sent, ptr);
      memcache_wait (c);
      return 0;
    }
    assert (next_key);
    assert (next_key_len >= 1);
    if (next_key_len < prefix_len || strncmp (key, next_key, prefix_len)) {
      if (ptr) {
        memcpy (ptr + total_data_sent, "}", 1);
        total_data_sent ++;
        static char s[12];
        sprintf (s, "%09d", total_keys_sent);
        memcpy (ptr + 2, s, 9);
        return_one_key_flags_len (c, key, prefix_len, ptr, total_data_sent, 1);
        free (ptr);
        wildcard_arrays_allocated --;
      }
      return 0;
    }
    //if (do_pmemcached_preload (next_key, next_key_len, 1) == -2) {
    //  write_last_kept (c, key, key_len, prefix_len, total_data_sent, total_keys_sent, ptr);
    //  memcache_wait (c);
    //  return 0;
    //}
    struct data x = do_pmemcached_get (next_key, next_key_len);

    if (x.data_len >= 0) {
      get_hits++;
      if (type == '*') {
        total_data_sent += return_one_key_flags_len (c, next_key + prefix_len, next_key_len - prefix_len, x.data, x.data_len, x.flags);
        total_keys_sent ++;
      } else if (type == '#') {
        if (total_data_sent + next_key_len - prefix_len + x.data_len + 20 < MAX_WILDCARD_LEN) {
          total_data_sent += sprintf (ptr + total_data_sent, "s:%d:\"", next_key_len - prefix_len);
          memcpy (ptr + total_data_sent, next_key + prefix_len, next_key_len - prefix_len);
          total_data_sent += next_key_len - prefix_len;
          total_data_sent += sprintf (ptr + total_data_sent, "\";");
          if (!(x.flags & 1)) {
            total_data_sent += sprintf (ptr + total_data_sent, "s:%d:\"", x.data_len);
          }
          memcpy (ptr + total_data_sent, x.data, x.data_len);
          total_data_sent += x.data_len;
          if (!(x.flags & 1)) {
            total_data_sent += sprintf (ptr + total_data_sent, "\";");
          }
          total_keys_sent ++;
        }
      }
    } else {
      assert (x.data_len == -1);
    }
    key = next_key;
    key_len = next_key_len;
  }*/
  return 0;

}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity >= 3) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }
  if (pmemcache_match_stats_key (key, key_len)) {
    int stats_len = pmemcache_prepare_stats (c);
    return_one_key (c, key, stats_buff, stats_len);
    return 0;
  }
  if (protected_mode) { return 0; }

  cmd_get++;
  if (!disable_wildcard && (key[key_len - 1] == '*' || key[key_len - 1] == '#')) {
    return memcache_get_wildcard (c, key, key_len);
  }

  if (do_pmemcached_preload (key, key_len, 1) == -2) {
    memcache_wait (c);
    return 0;
  }

  struct data x = do_pmemcached_get (key, key_len);

  if (x.data_len >= 0) {
    get_hits++;
    return_one_key_flags (c, key, x.data, x.data_len, x.flags);
  } else {
    if (x.data_len == -1 || x.data_len == -2) { // -2 can be returned if key is deleted by time and key was not in index
      if (return_false_if_not_found) {
        return_one_key_flags (c, key, "b:0;", 4, 1);
      }
      get_missed++;
    } else {
      assert (0);
    }
  }

  return 0;
}

int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg) {
  if (binlog_disabled == 1) {
    return 0;
  }
  if (protected_mode) {
    return 0;
  }
  c->flags &= ~C_INTIMEOUT;
  if (verbosity >= 3) {
    fprintf (stderr, "memcache_incr: op=%d, key='%s', val=%lld\n", op, key, arg);
  }
  if (op == 1) {
    cmd_decr++;
  } else {
    cmd_incr++;
  }

  int force_create = 0;
  if (key_len >= 3 && !strncmp (key, "###", 3)) {
    force_create = 1;
    key += 3;
    key_len -= 3;
  }
  int x = do_pmemcached_preload (key, key_len, 1);
  if (x == -2) {
    if (!memcache_wait (c)) {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }
    return 0;
  }

  if (x == 0 && force_create) {
    static char a[30];
    if (op) {
      arg = -arg;
    }
    sprintf (a, "%lld", arg);
    assert (do_pmemcached_store (mct_add, key, key_len, 0, DELAY_INFINITY, a, strlen (a)) != -2);
  } else {

    x = do_pmemcached_incr(op, key, key_len, arg);
    if (verbosity >= 4) { fprintf (stderr, "do_pmemcached_incr returns %d\n", x); }
    assert (x != -2);

    if (x == -1) {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
      return 0;
    }
  }

  struct data y = do_pmemcached_get (key, key_len);

  if (verbosity >= 4) { fprintf (stderr, "y.data_len = %d\n", y.data_len); }

  if (y.data_len == -2) {
    return memcache_wait (c);
  }

  if (y.data_len == -1) {
    write_out (&c->Out, "NOT_FOUND\r\n", 11);
  } else {
    write_out (&c->Out, y.data, y.data_len);
    write_out (&c->Out, "\r\n", 2);
  }

  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  if (binlog_disabled == 1) {
    return 0;
  }
  if (protected_mode) {
    return 0;
  }
  c->flags &= ~C_INTIMEOUT;

  if (verbosity >= 3) {
    fprintf (stderr, "memcache_delete: key='%s'\n", key);
  }
  cmd_delete++;

  if (do_pmemcached_preload (key, key_len, 1) == -2) {
    if (!memcache_wait (c)) {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }
    return 0;
  }
  int x = do_pmemcached_delete (key, key_len);
  assert (x != -2);

  if (x != -1) {
    write_out (&c->Out, "DELETED\r\n", 9);
    return 0;
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof(VERSION));
  return 0;
}

extern long long malloc_mem;
extern long long zalloc_mem;
extern long long wildcard_cache_memory;
extern int wildcard_cache_entries;
int pmemcache_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb,
    "malloc_mem\t%ld\n"
    "zalloc_mem\t%ld\n",
    (long) malloc_mem,
    (long) zalloc_mem);

  SB_BINLOG;
  sb_printf (&sb,
        "index_loaded_bytes\t%lld\n"
        "index_size\t%lld\n"
        "index_path\t%s\n"
        "index_load_time\t%.6lfs\n"
        "pid\t%d\n"
        "snapshot_size\t%d\n"
        "curr_items\t%d\n"
        "total_items\t%lld\n"
        "current_memory_used\t%lld\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "get_hits\t%lld\n"
        "get_misses\t%lld\n"
        "cmd_incr\t%lld\n"
        "cmd_decr\t%lld\n"
        "cmd_delete\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_stats\t%lld\n"
        "total_response_words\t%lld\n"
        "total_response_bytes\t%lld\n"
        "limit_max_dynamic_memory\t%lld\n"
        "reindex_on_low_memory\t%d\n"
        "reindex_on_low_memory_time_ago\t%d\n"
        "wildcard_arrays_allocated\t%d\n"
        "wildcard_cache_memory\t%lld\n"
        "wildcard_cache_entries\t%d\n",
        snapshot_size,
        index_size,
        engine_snapshot_name,
        index_load_time,
        getpid(),
        (int)(sizeof(int*)*8),
        get_entry_cnt(),
        total_items,
        get_memory_used(),
        cmd_get,
        cmd_set,
        get_hits,
        get_missed,
        cmd_incr,
        cmd_decr,
        cmd_delete,
        cmd_version,
        cmd_stats,
        tot_response_words,
        tot_response_bytes,
        max_memory,
        reindex_on_low_memory,
        reindex_on_low_memory ? now - last_reindex_on_low_memory_time: -1,
        wildcard_arrays_allocated,
        wildcard_cache_memory,
        wildcard_cache_entries);
  data_prepare_stats (&sb);
  custom_prepare_stats (&sb);
  sb_printf (&sb, "%s\n", FullVersionStr);
  return sb.pos;

}

int memcache_stats (struct connection *c) {
  cmd_stats++;
  int stats_len = pmemcache_prepare_stats (c);
  write_out (&c->Out, stats_buff, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


int pmemcached_engine_wakeup (struct connection *c) {
  // struct mcs_data *D = MCS_DATA(c);

  if (verbosity > 1) {
    fprintf (stderr, "pmemcached_engine_wakeup (%p)\n", c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_reading_query;

  //if (verbosity >= 4) { fprintf (stderr, "D->query_type = %d\n", D->query_type); }
  //assert (D->query_type == mct_get_resume || D->query_type == mct_incr);

  clear_connection_timeout (c);

  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  return 0;
}


int pmemcached_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "pmemcached_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  if (c->Tmp && !disable_wildcard && c->Tmp->total_bytes) {
    if (read_last_kept (c, 0)) {
      if (mc_store->ptr) {
        free (mc_store->ptr);
        wildcard_arrays_allocated --;
        //free_tmp_buffers (c);
      }
    }
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return pmemcached_engine_wakeup (c);
}


int pmemcached_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  vkprintf (1, "memcache_get_start\n");
  return 0;
}

int pmemcached_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  vkprintf (1, "memcache_get_end: %d keys requested\n", key_count);
  write_out (&c->Out, "END\r\n", 5);
  free_tmp_buffers (c);
  return 0;
}


TL_DO_FUN(pmemcached_get)
  if (do_pmemcached_preload (e->key, e->key_len, 1) == -2) {
    return -2;
  }

  struct data x = do_pmemcached_get (e->key, e->key_len);

  if (x.data_len >= 0) {
    tl_store_int (TL_MEMCACHE_STRVALUE);
    tl_store_string (x.data, x.data_len);
    tl_store_int (x.flags);
    get_hits ++;
  } else {
    tl_store_int (TL_MEMCACHE_NOT_FOUND);
    get_missed ++;
  }
TL_DO_FUN_END

TL_DO_FUN(pmemcached_delete)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled. Can not perform delete");
    return -1;
  }
  
  cmd_delete++;

  if (do_pmemcached_preload (e->key, e->key_len, 1) == -2) {
    return -2;
  }

  int x = do_pmemcached_delete (e->key, e->key_len);
  assert (x != -2);

  if (x != -1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(pmemcached_set)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled. Can not perform store");
    return -1;
  }
  int delay = e->delay;;
  if (delay == 0) {
    delay = DELAY_INFINITY;
  } else if (delay <= SEC_IN_MONTH) {
    delay += get_double_time_since_epoch();
  }

  if (do_pmemcached_preload (e->data, e->key_len, e->op != mct_set) == -2) {
    return -2;
  }

  int result = do_pmemcached_store (e->op, e->data, e->key_len, e->flags, delay, e->data + e->key_len + 1, e->value_len);
  assert (result != -2);
  if (result) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(pmemcached_incr)
  if (binlog_disabled == 1) {
    tl_fetch_set_error_format (TL_ERROR_BINLOG_DISABLED, "Binlog disabled. Can not perform store");
    return -1;
  }
  if (do_pmemcached_preload (e->key, e->key_len, 1) == -2) {
    return -2;
  }

  int x = do_pmemcached_incr (0, e->key, e->key_len, e->value);
  assert (x != -2);
  if (x == -1) {
    tl_store_int (TL_MEMCACHE_NOT_FOUND);
    return 0;
  }

  struct data y = do_pmemcached_get (e->key, e->key_len);
  assert (y.data_len !=-2);
  if (y.data_len == -1) {
    tl_store_int (TL_MEMCACHE_NOT_FOUND);
  } else {
    tl_store_int (TL_MEMCACHE_LONGVALUE);
    tl_store_long (atoll (y.data));
    tl_store_int (y.flags);
  }
TL_DO_FUN_END

TL_DO_FUN(pmemcached_get_wildcard)
  vkprintf (3, "memcache_get_wildcard. key = %s\n", e->key);
  wildcard_ptr = 0;
  wildcard_engine_report = wildcard_engine_rpc_report;
  
  if (e->last_key) {
    wildcard_last_key = e->last_key;
    wildcard_last_key_len = e->last_key_len;
  } else {
    wildcard_last_key = e->key;
    wildcard_last_key_len = e->key_len;
  }
  wildcard_prefix_len = e->key_len;
  
  if (!e->value_buf) {
    struct data x = wildcard_get_rpc_value (e->key, e->key_len);
    if (x.data_len != -1) {
      tl_store_raw_data (x.data, x.data_len);
      return 0;
    }
    wildcard_total_keys_sent = 0;
    wildcard_ptr = malloc (MAX_WILDCARD_LEN);
    *(int *)wildcard_ptr = 0;
    wildcard_total_data_sent = 4;
    wildcard_arrays_allocated ++;
    e->value_buf = wildcard_ptr;
  } else {
    assert (metafile_mode);
    //fprintf (stderr, "kept key_len = %d, key = %.*s, prefix_len = %d\n", mc_store->key_len, mc_store->key_len, mc_store->key, mc_store->prefix_len);
    wildcard_total_data_sent = e->data_sent;
    wildcard_total_keys_sent = e->keys_sent;
    wildcard_ptr = e->value_buf;
  }


  int v = do_pmemcached_get_all_next_keys (wildcard_last_key, wildcard_last_key_len, wildcard_prefix_len, wildcard_total_keys_sent);
  vkprintf (3, "do_pmemcached_get_all_next_keys result: %d\n", v);
  if (v == -2) {
    assert (metafile_mode);
    e->last_key_len = wildcard_last_key_len;
    if (e->last_key) {
      free (e->last_key);
    }
    e->last_key = malloc (e->last_key_len + 1);
    memcpy (e->last_key, wildcard_last_key, e->last_key_len);
    e->last_key[e->last_key_len] = 0;
    e->data_sent = wildcard_total_data_sent;
    e->keys_sent = wildcard_total_keys_sent;
    return -2;
  }
  wildcard_rpc_report_finish (e->key, e->key_len);
TL_DO_FUN_END

TL_PARSE_FUN(pmemcached_get,void)
  e->key_len = tl_fetch_string (e->key, MAX_KEY_LEN);
  if (e->key_len < 0) {
    return 0;
  }
  e->key[e->key_len] = 0;
  extra->size += e->key_len + 1;
TL_PARSE_FUN_END

TL_PARSE_FUN(pmemcached_delete,void)
  e->key_len = tl_fetch_string (e->key, MAX_KEY_LEN);
  if (e->key_len < 0) {
    return 0;
  }
  e->key[e->key_len] = 0;
  extra->size += e->key_len + 1;
TL_PARSE_FUN_END

TL_PARSE_FUN(pmemcached_set,int op)
  e->key_len = tl_fetch_string (e->data, MAX_KEY_LEN);
  if (e->key_len < 0) {
    return 0;
  }
  e->flags = tl_fetch_int ();
  e->delay = tl_fetch_int ();
  e->data[e->key_len] = 0;
  extra->size += e->key_len + 1;
  e->value_len = tl_fetch_string (e->data + e->key_len + 1, MAX_VALUE_LEN);
  if (e->value_len < 0) {
    return 0;
  }
  e->data[e->value_len + e->key_len + 1] = 0;
  extra->size += e->value_len + 1;
  e->op = op;
TL_PARSE_FUN_END

TL_PARSE_FUN(pmemcached_incr,int op)
  e->key_len = tl_fetch_string (e->key, MAX_KEY_LEN);
  if (e->key_len < 0) {
    return 0;
  }
  e->key[e->key_len] = 0;
  extra->size += e->key_len + 1;
  e->value = tl_fetch_long ();
  if (op == mct_decr) {
    e->value = -e->value;
  }
TL_PARSE_FUN_END

void tl_wildcard_free (struct tl_act_extra *extra) {
  struct tl_pmemcached_get_wildcard *e = (void *)extra->extra;
  if (e->value_buf) {
    free (e->value_buf);
    wildcard_arrays_allocated --;
  }
  if (e->last_key) {
    free (e->last_key);
  }

  tl_default_act_free (extra);
}

TL_PARSE_FUN(pmemcached_get_wildcard,void)
  e->value_buf = 0;
  e->data_sent = 0;
  e->keys_sent = 0;
  e->last_key = 0;
  e->key_len = tl_fetch_string0 (e->key, MAX_KEY_LEN);
  if (e->key_len < 0) {
    return 0;
  }
  extra->size += e->key_len + 1;
  e->key[e->key_len] = 0;
  extra->free = tl_wildcard_free;
TL_PARSE_FUN_END

struct tl_act_extra *pmemcached_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Pmemcached only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_MEMCACHE_GET:
    return tl_pmemcached_get ();
  case TL_MEMCACHE_DELETE:
    return tl_pmemcached_delete ();
  case TL_MEMCACHE_SET:
    return tl_pmemcached_set (mct_set);
  case TL_MEMCACHE_ADD:
    return tl_pmemcached_set (mct_add);
  case TL_MEMCACHE_REPLACE:
    return tl_pmemcached_set (mct_replace);
  case TL_MEMCACHE_INCR:
    return tl_pmemcached_incr (mct_incr);
  case TL_MEMCACHE_DECR:
    return tl_pmemcached_incr (mct_decr);
  case TL_MEMCACHE_GET_WILDCARD:
    return tl_pmemcached_get_wildcard ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}


/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

void reopen_logs (void) {
  int fd;
  fflush(stdout);
  fflush(stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}


static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  flush_binlog_last();
  sync_binlog (2);
  exit (EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  flush_binlog_last();
  sync_binlog (2);
  exit (EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  sync_binlog (2);
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  sync_binlog (2);
  reopen_logs ();
  signal (SIGUSR1, sigusr1_handler);
}

static void sigrtmax_handler (const int sig) {
  fprintf(stderr, "got SIGUSR3, write index.\n");
  force_write_index = 1;
}


int child_pid;

void check_child_status (void) {
  if (!child_pid) {
    return;
  }
  int status = 0;
  int res = waitpid (child_pid, &status, WNOHANG);
  if (res == child_pid) {
    if (WIFEXITED (status) || WIFSIGNALED (status)) {
      if (verbosity > 0) {
        fprintf (stderr, "child process %d terminated: exited = %d, signaled = %d, exit code = %d\n",
          child_pid, WIFEXITED (status) ? 1 : 0, WIFSIGNALED (status) ? 1 : 0, WEXITSTATUS (status));
      }
      child_pid = 0;
    }
  } else if (res == -1) {
    if (errno != EINTR) {
      fprintf (stderr, "waitpid (%d): %m\n", child_pid);
      child_pid = 0;
    }
  } else if (res) {
    fprintf (stderr, "waitpid (%d) returned %d???\n", child_pid, res);
  }
}

void fork_write_index (void) {
  if (child_pid) {
    if (verbosity > 0) {
      fprintf (stderr, "process with pid %d already generates index, skipping\n", child_pid);
    }
    return;
  }

  flush_binlog_ts ();

  int res = fork ();

  if (res < 0) {
    fprintf (stderr, "fork: %m\n");
  } else if (!res) {
    binlogname = 0;
    res = save_index (!binlog_disabled);
    exit (res);
  } else {
    if (verbosity > 0) {
      fprintf (stderr, "created child process pid = %d\n", res);
    }
    child_pid = res;
  }

  force_write_index = 0;
}

void cron (void) {
  create_all_outbound_connections ();
  flush_binlog ();
  free_by_time (137);
  redistribute_memory ();
  if (!reindex_on_low_memory && memory_full_warning ()) {
    force_write_index ++;
    reindex_on_low_memory ++;
    last_reindex_on_low_memory_time = now;
  }
  if (force_write_index) {
    fork_write_index ();
  }
  check_child_status();
}



int sfd;

void start_server (void) {
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();
  if (udp_enabled) {
    init_msg_buffers (0);
  }

  prev_time = 0;

  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  init_listening_connection (sfd, &ct_rpc_server, &memcache_rpc_server);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);
  signal (SIGRTMAX, sigrtmax_handler);

  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
        active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    fetch_binlog_data (binlog_disabled ? log_cur_pos() : log_write_pos ());
    epoll_work (17);

    check_all_aio_completions ();
    tl_restart_all_ready ();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    flush_binlog ();
    cstatus_binlog_pos (binlog_disabled ? log_cur_pos() : log_write_pos (), binlog_disabled);

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close (sfd);

  //  flush_binlog_ts();
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("%s\n",
          FullVersionStr
         );
  parse_usage ();
  exit (2);
}

int disable_cache;
char *pack_file = 0;

int f_parse_option (int val) {
  switch (val) {
  case 'f':
    return_false_if_not_found = 1;
    break;
  case 'i':
    index_mode = 1;
    break;
  case 'D':
    disable_cache ++;
    break;
  case 'm':
    max_memory = 1024 * 1024 * (long long) atoi (optarg);
    break;
  case 'P':
    pack_file = optarg;
    break;
  case 'M':
    metafile_size = 1024 * atoi (optarg);
    if (metafile_size > 1024 * 1024)
      metafile_size = 1024 * 1024;
    break;
  case 'S':
    memcache_methods.mc_check_perm = mcs_crypted_check_perm;
    memcache_rpc_server.rpc_check_perm = rpcs_crypted_check_perm;
    break;
  case 'R':
    memcache_methods.mc_check_perm = mcs_crypted_check_perm;
    memcache_rpc_server.rpc_check_perm = rpcs_crypted_check_perm;
    protected_mode ++;
    disable_wildcard ++;
    break;
  case 'w':
    disable_wildcard ++;
    break;
  case 'Q':
    tcp_maximize_buffers = 1;
    break;
  default:
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;
  binlog_readed = 0;

  signal (SIGRTMAX, sigrtmax_handler);
  set_debug_handlers ();

  progname = argv[0];

  parse_option ("return-false", no_argument, 0, 'f', "returns serialized false if not found");
  parse_option ("index", no_argument, 0, 'i', "starts in index mode");
  parse_option ("disable-cache", no_argument, 0, 'D', "disables cache of elements in engine");
  parse_option ("max-memory", required_argument, 0, 'm', "max memory (in MiB)");
  parse_option ("pack_file", required_argument, 0, 'P', "packs binlog+index in small binlog");
  parse_option ("metafile-size", required_argument, 0, 'M', "Size of metafile (in KiB)");
  parse_option ("secure", no_argument, 0, 'S', "Allow unecrypted connections only from localhost");
  parse_option ("restricted", no_argument, 0, 'R', "Allow unecrypted connections only from localhost, disables wildcard, disables memcache (aside from stats query)");
  parse_option ("disable-wildcard", no_argument, 0, 'w', "Disables wildcard queries");
  parse_option ("maximize-tcp-buffers", no_argument, 0, 'Q', "Tries to maximize tcp buffers");
  
  parse_engine_options_long (argc, argv, f_parse_option);

  if (argc != optind + 1) {
    usage ();
    return 2;
  }

  if (pack_file) {
    pack_fd = open (pack_file, O_TRUNC | O_WRONLY | O_EXCL | O_CREAT, 0600);
    pack_mode = 1;
    binlog_disabled = 1;
    assert (pack_fd >= 0);
  }

  if (strlen (argv[0]) >= 5 && memcmp ( argv[0] + strlen (argv[0]) - 5, "index" , 5) == 0) {
    index_mode = 1;
  }

  if (verbosity>=3) {
    fprintf (stderr, "Command line parsed\n");
  }

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (dynamic_data_buffer_size > max_memory) {
    dynamic_data_buffer_size = max_memory;
  }

  if (MAX_ZMALLOC_MEM == 0) {
    dynamic_data_buffer_size = 1 << 22;
  }

  if (raise_file_rlimit(maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  aes_load_pwd_file (aes_pwd_file);

  if (change_user(username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  //if (MAX_ZMALLOC_MEM > 0) {
  init_dyn_data();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }
  //}

  init_hash_table();

  if (!index_mode) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  if (verbosity>=2){
    fprintf (stderr, "starting reading binlog\n");
  }

  if (verbosity>=3){
    fprintf(stderr, "binlogname=%s\n",binlogname);
    fprintf(stderr, "argv[optind]=%s\n",argv[optind]);
  }

  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  if (verbosity>=3){
    fprintf (stderr, "engine_preload_filelist done\n");
  }


  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  index_load_time = -mytime();

  i = load_index (Snapshot);

  index_load_time += mytime();

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "load index: done, jump_log_pos=%lld, time %.06lfs\n",
       jump_log_pos, index_load_time);
  }

  if (index_type != PMEMCACHED_TYPE_INDEX_DISK) {
    close_snapshot (Snapshot, 1);
  }

  if (pack_mode) {
    int x = 0;
    static char buf[2000000];
    struct lev_start *a = (struct lev_start *)buf;
    a->type = LEV_START;
    a->schema_id = PMEMCACHED_SCHEMA_V1;
    a->split_mod = 1;
    a->split_min = 0;
    a->split_max = 1;
    write (pack_fd, a, sizeof (struct lev_start) - 4);
    while (1) {
      struct index_entry *entry = index_get_num (x ++, 0);
      if (entry->data_len < 0) {
        break;
      }
      struct lev_pmemcached_store *E = (struct lev_pmemcached_store *)buf;
      E->type = LEV_PMEMCACHED_STORE + 1;
      E->key_len = entry->key_len;
      E->data_len = entry->data_len;
      E->delay = entry->delay;
      E->flags = entry->flags;
      memcpy (E->data, entry->data, E->key_len + E->data_len);
      int t;
      for (t = 0; t <= 4; t++) {
        E->data[E->key_len + E->data_len + t] = 0;
      }
      int l = 1 + offsetof (struct lev_pmemcached_store, data) + E->key_len + E->data_len;
      int adj_bytes = -l & 3;
      l = (l + 3) & -4;
      if (adj_bytes) {
        memset (((char *)E) + l - adj_bytes, adj_bytes, adj_bytes);
      }

      write (pack_fd, E, l);
    }
    x--;
    vkprintf (1, "Written log events for %d index entries\n", x);
  }


  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  init_common_data (0, index_mode ? CD_INDEXER : CD_ENGINE);
  cstatus_binlog_name (engine_replica->replica_prefix);

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = -mytime();

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  if (verbosity) {
    fprintf (stderr, "replay log events started\n");
  }

  i = replay_log (0, 1);

  if (verbosity) {
    fprintf (stderr, "replay log events finished\n");
  }

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (!binlog_disabled) {
    clear_read_log();
  }

  if (i == -2) {
    long long true_readto_pos = log_readto_pos - Binlog->info->log_pos + Binlog->offset;
    fprintf (stderr, "REPAIR: truncating %s at log position %lld (file position %lld)\n", Binlog->info->filename, log_readto_pos, true_readto_pos);
    if (truncate (Binlog->info->filename, true_readto_pos) < 0) {
      perror ("truncate()");
      exit (2);
    }
  } else if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%lld, time %.06lfs\n",
             (long long) log_pos, get_memory_used (), binlog_load_time);
  }

  binlog_readed = 1;

  clear_write_log ();
  start_time = time (NULL);

  if (!index_mode) {
    tl_parse_function = pmemcached_parse_function;
    tl_aio_timeout = 0.5;
    start_server ();
  } else {
    save_index (!binlog_disabled);
  }

  return 0;
}

