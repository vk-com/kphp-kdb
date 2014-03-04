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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "random-data.h"
#include "kdb-data-common.h"
#include "am-stats.h"
#include "am-server-functions.h"

#include "TL/constants.h"
#include "random-interface-structures.h"
#include "vv-tl-parse.h"
#include "vv-tl-aio.h"

#define	VERSION_STR "random-engine-1.02"
static const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define PASSWORD_LENGTH 1024

#define mytime() get_utime (CLOCK_MONOTONIC)

/*
 *
 *		MEMCACHED PORT
 *
 */

int mcp_get (struct connection *c, const char *key, int key_len);
int mcp_wakeup (struct connection *c);
int mcp_alarm (struct connection *c);
int mcp_stats (struct connection *c);
int mcp_check_ready (struct connection *c);

conn_type_t ct_random_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "random_engine_server",
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
  .wakeup = 0,
  .alarm = 0,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

struct memcache_server_functions memcache_methods;


struct rpc_server_functions rpc_methods = {
  .execute = default_tl_rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_close = default_tl_close_conn,
  .memcache_fallback_type = &ct_memcache_server,
  .memcache_fallback_extra = &memcache_methods
};

static char *password_filename;

static long long get_queries;

#define VALUE_BUFFSIZE 0x100000
static char value_buff[VALUE_BUFFSIZE];
#define STATS_BUFF_SIZE	0x100000
static char stats_buff[STATS_BUFF_SIZE];

int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

int random_prepare_stats (struct connection *c);

int memcache_get (struct connection *c, const char *key, int key_len) {
  int n;
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;

  if (key_len >= 6 && !strncmp (key, "random", 6) && sscanf (key + 6, "%d", &n) == 1 && n > 0 && n <= VALUE_BUFFSIZE) {
    get_queries++;
    int r = random_get_bytes (value_buff, n);
    return_one_key (c, key - dog_len, value_buff, r);
    return 0;
  }

  if (key_len >= 10 && !strncmp (key, "hex_random", 10) && sscanf (key + 10, "%d", &n) == 1 && n > 0 && n <= VALUE_BUFFSIZE) {
    get_queries++;
    int r = random_get_hex_bytes (value_buff, n);
    return_one_key (c, key - dog_len, value_buff, r);
    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int stats_len = random_prepare_stats (c);
    return_one_key (c, key - dog_len, stats_buff, stats_len);
    return 0;
  }

  return 0;
}

int random_prepare_stats (struct connection *c) {
  dyn_update_stats ();
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb, "version\t%s\n", FullVersionStr);
  SB_PRINT_QUERIES(get_queries);
  return sb.pos;
}

int memcache_stats (struct connection *c) {
  int stats_len = random_prepare_stats (c);
  write_out (&c->Out, stats_buff, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

TL_DO_FUN(random_random)
  get_queries++;
  int r = random_get_bytes (value_buff, e->n < sizeof (value_buff) ? e->n : sizeof (value_buff));
  if (r <= 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_string (value_buff, r);
  }
TL_DO_FUN_END

TL_PARSE_FUN(random_random, void)
  e->n = tl_fetch_int ();
  if (e->n < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Negative value: n = %d", e->n);
    return 0;
  }
TL_PARSE_FUN_END

struct tl_act_extra *random_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Random only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_RANDOM_RANDOM:
    return tl_random_random ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}

void random_stats (void) {
  random_prepare_stats (0);
  tl_store_stats (stats_buff, 0);
}

void cron (void) {
  create_all_outbound_connections ();
}

static engine_t random_engine;
static server_functions_t random_functions = {
  .cron = cron,
};

void start_server (void) {
  int i;

  tl_parse_function = random_parse_function;
  tl_stat_function = random_stats;
  tl_aio_timeout = 2.0;

  server_init (&random_engine, &random_functions, &ct_rpc_server, &rpc_methods);

  int quit_steps = 0;

  for (i = 0; ; i++) {
    if (!(i & 255)) {
      vkprintf (1, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (7);
    random_work (80000 / (1000 / 11));
    if (!process_signals ()) {
      break;
    }
    if (quit_steps && !--quit_steps) break;
  }

  random_free (password_filename, PASSWORD_LENGTH);
  server_exit (&random_engine);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: random-engine [-h] [-v] [-N<key-len>] [-s<buffer-size>] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-l<logname>] <password-file> \n"
      "\t%s\n"
      "\tGenerates random bytes.\n"
      "\t-v\toutput statistical and debug information into stderr\n"
      "\t[-N<key-len>]\tset Blum-Blum-Shub key-len in bits (default is 2048 bits).\n"
      "\t[-s<buffer-size>]\tset random buffer size.\n",
      FullVersionStr);
  exit (2);
}

int main (int argc, char *argv[]) {
  char c;
  long long x;
  int i, key_len = 2048, buffer_size = 0;

  set_debug_handlers ();
  daemonize = 1;

  progname = argv[0];
  while ((i = getopt (argc, argv, "N:b:c:dhl:p:s:u:v")) != -1) {
    switch (i) {
    case 'N':
      key_len = atoi (optarg);
      break;
    case 'b':
      backlog = atoi (optarg);
      if (backlog <= 0) {
        backlog = BACKLOG;
      }
      break;
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
        maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'h':
      usage ();
      break;
    case 'l':
      logname = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 's':
       c = 0;
       assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
       switch (c | 0x20) {
         case 'k':  x <<= 10; break;
         case 'm':  x <<= 20; break;
         case 'g':  x <<= 30; break;
         case 't':  x <<= 40; break;
         default: assert (c == 0x20);
       }
       if (x >= 1024 && x < (1LL << 30)) {
         buffer_size = x;
       }
      break;
    case 'u':
      username = optarg;
      break;
    }
  }

  if (optind + 1 != argc) {
    kprintf ("<password-file> wasn't given\n");
    usage ();
  }

  password_filename = argv[optind];

  engine_init (&random_engine, 0, 0);

  start_time = time (0);

  random_init (2048, buffer_size, password_filename, PASSWORD_LENGTH);

  start_server ();
  return 0;
}

