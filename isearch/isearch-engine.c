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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <signal.h>

#include "kdb-data-common.h"
#include "kdb-isearch-binlog.h"
#include "server-functions.h"
#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#include "net-rpc-server.h"
#include "net-rpc-common.h"

#include "isearch-data.h"

#define MAX_VALUE_LEN (1 << 20)

#define VERSION "0.9"
#define VERSION_STR "isearch "VERSION

#define TCP_PORT 11211
#define UDP_PORT 11211

/*
 *
 *    MEMCACHED PORT
 *
 */

int port = TCP_PORT, udp_port = UDP_PORT;

struct in_addr settings_addr;
int interactive = 0;

volatile int sigpoll_cnt;

long long binlog_loaded_size;
double binlog_load_time, index_load_time;

long long cmd_get, cmd_set, cmd_stats, cmd_version;
double cmd_get_time = 0.0, cmd_set_time = 0.0;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0;

#define STATS_BUFF_SIZE (1 << 14)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
int isearch_prepare_stats (void);

conn_type_t ct_isearch_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "isearch_engine_server",
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
  .wakeup = server_failed,
  .alarm = server_failed,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};


int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = memcache_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct rpc_server_functions rpc_methods = {
  .execute = default_tl_rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_close = default_tl_close_conn,
  .memcache_fallback_type = &ct_isearch_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


char buf[MAX_VALUE_LEN];

static inline void safe_read_in (netbuffer_t *H, char *data, int len) {
  assert (read_in (H, data, len) == len);
  int i;
  for (i = 0; i < len; i++) {
    if (data[i] == 0) {
      data[i] = ' ';
    }
  }
}

static inline void eat_at (const char *key, int key_len, char **new_key, int *new_len) {
  if (*key == '^') {
    key++;
    key_len--;
  }

  *new_key = (char *)key;
  *new_len = key_len;

  if ((*key >= '0' && *key <= '9') || (*key == '-' && key[1] >= '0' && key[1] <= '9')) {
    key++;
    while (*key >= '0' && *key <= '9') {
      key++;
    }

    if (*key++ == '@') {
      if (*key == '^') {
        key++;
      }

      *new_len -= (key - *new_key);
      *new_key = (char *)key;
    }
  }
}

int memcache_store (struct connection *c, int op, const char *old_key, int old_key_len, int flags, int delay, int size) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d, \n", old_key, old_key_len, size);
  }

  if (size + 1 < MAX_VALUE_LEN) {
    char *key;
    int key_len;

    eat_at (old_key, old_key_len, &key, &key_len);

    //set ("stat{$uid},{$type}#{$cnt}", $text)
    if (key_len >= 4 && !strncmp (key, "stat", 4)) {
      int uid, type, cnt;
      if (sscanf (key, "stat%d,%d#%d", &uid, &type, &cnt) != 3 || cnt == 0) {
        RETURN(set, -2);
      }

      safe_read_in (&c->In, buf, size);
      buf[size] = 0;

      do_isearch_set_stat (uid, type, cnt, buf, size);

      RETURN(set, 1);
    }

    if (key_len >= 14 && !strncmp (key, "add_black_list", 14)) {
      safe_read_in (&c->In, buf, size);
      buf[size] = 0;

      if (verbosity > 1) {
        fprintf (stderr, "add_black_list %s\n", buf);
      }

      int result = do_black_list_add (buf, size);
      RETURN(set, result);
    }

    if (key_len >= 17 && !strncmp (key, "delete_black_list", 17)) {
      safe_read_in (&c->In, buf, size);
      buf[size] = 0;

      if (verbosity > 1) {
        fprintf (stderr, "delete_black_list %s\n", buf);
      }

      int result = do_black_list_delete (buf, size);
      RETURN(set, result);
    }
  }

  RETURN(set, -2);
}

int memcache_get (struct connection *c, const char *old_key, int old_key_len) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  int mode = 0, st = 0;

  if (key_len >= 5 && !strncmp (key, "hints", 5)) {
    mode = 1;
    st = 5;
  }

#ifdef TYPES
  if (key_len >= 5 && !strncmp (key, "types", 5)) {
    mode = 2;
    st = 5;
  }

  if (key_len >= 11 && !strncmp (key, "hints_debug", 11)) {
    mode = 3;
    st = 11;
  }
#endif

  if (mode) {
    if (key[st] != '(' || key[key_len - 1] != ')') {
      RETURN(get, 0);
    }
    int len = key_len - st - 2;
    memcpy (buf, key + st + 1, sizeof (char) * len);
    buf[len] = 0;

    if (verbosity >= 2) {
      fprintf (stderr, "run get_hints (%s, %d)\n", buf, mode);
    }

    get_hints (buf, mode, MAX_VALUE_LEN);

    return_one_key (c, old_key, buf, strlen (buf));

    if (verbosity > 0) {
      if (mytime() + cmd_time > 0.005) {
        fprintf (stderr, "Warning!!! Search query (%s) was %lf seconds.\n", key, mytime() + cmd_time);
      }
    }

    RETURN(get, 0);
  }

  if (key_len >= 10 && !strncmp (key, "suggestion", 10)) {
    if (key[10] != '(' || key[key_len - 1] != ')') {
      RETURN(get, 0);
    }

    int len = key_len - 12;
    memcpy (buf, key + 11, sizeof (char) * len);
    buf[len] = 0;

    if (verbosity >= 2) {
      fprintf (stderr, "run get_suggestion (%s)\n", buf);
    }

    get_suggestion (buf, MAX_VALUE_LEN);

    return_one_key (c, old_key, buf, strlen (buf));

    if (verbosity > 0) {
      if (mytime() + cmd_time > 0.005) {
        fprintf (stderr, "Warning!!! Search query (%s) was %lf seconds.\n", key, mytime() + cmd_time);
      }
    }

    RETURN(get, 0);
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = isearch_prepare_stats ();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, old_key, stats_buff, len + len2 - 1);

    return 0;
  }

  if (key_len >= 3 && !strncmp (key, "top", 3)) {
    int cnt;
    if (sscanf (key, "top%d", &cnt) == 1) {
      get_top (buf, cnt, MAX_VALUE_LEN);

      return_one_key (c, old_key, buf, strlen (buf));
    }
    RETURN(get, 0);
  }

  if (key_len >= 4 && !strncmp (key, "best", 4)) {
    int cnt;
    if (sscanf (key, "best%d", &cnt) == 1) {
      get_best (buf, cnt, MAX_VALUE_LEN);

      return_one_key (c, old_key, buf, strlen (buf));
    }
    RETURN(get, 0);
  }

  if (key_len >= 10 && !strncmp (key, "black_list", 10)) {
    if (key_len >= 16 && !strncmp (key, "black_list_force", 16)) {
      black_list_force();
    } else {
      char *s = black_list_get();
      int len = strlen (s);

      return_one_key (c, old_key, s, len);
    }

    RETURN(get, 0);
  }

  RETURN(get, 0);
}



int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

int isearch_prepare_stats (void) {
  cmd_stats++;
  int log_uncommitted = compute_uncommitted_log_bytes();

  return snprintf (stats_buff, STATS_BUFF_SIZE,
        "heap_used\t%ld\n"
        "heap_max\t%ld\n"
        "binlog_original_size\t%lld\n"
        "binlog_loaded_bytes\t%lld\n"
        "binlog_load_time\t%.6lfs\n"
        "current_binlog_size\t%lld\n"
        "binlog_uncommitted_bytes\t%d\n"
        "binlog_path\t%s\n"
        "binlog_first_timestamp\t%d\n"
        "binlog_read_timestamp\t%d\n"
        "binlog_last_timestamp\t%d\n"
        "max_binlog_size\t%lld\n"
        "index_loaded_bytes\t%d\n"
        "index_size\t%lld\n"
        "index_path\t%s\n"
        "index_load_time\t%.6lfs\n"
        "pid\t%d\n"
        "version\t%s\n"
        "pointer_size\t%d\n"
        "current_memory_used\t%lld\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "cmd_stats\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_get_time\t%.7lf\n"
        "cmd_set_time\t%.7lf\n"
        "max_cmd_get_time\t%.7lf\n"
        "max_cmd_set_time\t%.7lf\n"
        "lowest_rate\t"FD"\n"
        "limit_max_dynamic_memory\t%ld\n"
        "FADING\t%d\n"
        "SLOW\t%d\n"
        "TYPES\t%d\n"
        "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n",
        (long) (dyn_cur - dyn_first),
        (long) (dyn_last - dyn_first),
        log_readto_pos,
        log_readto_pos - jump_log_pos,
        binlog_load_time,
        log_pos,
        log_uncommitted,
        binlogname ? (strlen (binlogname) < 250 ? binlogname : "(too long)") : "(none)",
        log_first_ts,
        log_read_until,
        log_last_ts,
        max_binlog_size,
        header_size,
        engine_snapshot_size,
        engine_snapshot_name,
        index_load_time,
        getpid(),
        VERSION,
        (int)(sizeof (void *) * 8),
        dl_get_memory_used(),
        cmd_get,
        cmd_set,
        cmd_stats,
        cmd_version,
        cmd_get_time,
        cmd_set_time,
        max_cmd_get_time,
        max_cmd_set_time,
        lowest_rate,
        max_memory,
#ifdef FADING
        1,
#else
        0,
#endif
#ifdef SLOW
        1,
#else
        0,
#endif
#ifdef TYPES
        1
#else
        0
#endif
        );
}

int memcache_stats (struct connection *c) {
  int len = isearch_prepare_stats ();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


/*
 *
 *    RPC interface
 *
 */


static inline void tl_store_bool (int res) {
  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
}


TL_DO_FUN(isearch_add_search_query)
  int res = do_isearch_set_stat (e->user_id, e->type, e->count, e->query, e->query_len);

  tl_store_bool (res);
TL_DO_FUN_END

TL_DO_FUN(isearch_get_suggestion)
  assert (stats_buff + sizeof (int) == e->query);
  get_suggestion (e->query, STATS_BUFF_SIZE - sizeof (int));

  tl_store_int (TL_STRING);
  tl_store_string0 (e->query);
TL_DO_FUN_END

TL_DO_FUN(isearch_add_black_list)
  int res = do_black_list_add (e->query, e->query_len);

  tl_store_bool (res);
TL_DO_FUN_END

TL_DO_FUN(isearch_delete_black_list)
  int res = do_black_list_delete (e->query, e->query_len);

  tl_store_bool (res);
TL_DO_FUN_END

TL_DO_FUN(isearch_get_black_list)
  char *res = black_list_get();

  tl_store_int (TL_VECTOR);
  int *size = tl_store_get_ptr (4);
  int i = 0, cnt = 0;
  while (res[i]) {
    int j;
    for (j = i; res[j] != '\t' && res[j]; j++) {
    }
    tl_store_string (res + i, j - i);
    cnt++;
    if (!res[j]) {
      break;
    }
    i = j + 1;
  }
  *size = cnt;
TL_DO_FUN_END

TL_DO_FUN(isearch_force_update_black_list)
  black_list_force();

  tl_store_bool (1);
TL_DO_FUN_END


TL_PARSE_FUN(isearch_add_search_query, void)
  e->user_id = tl_fetch_int();
  e->type = tl_fetch_int();
  e->count = tl_fetch_int();

  e->query_len = tl_fetch_string0 (e->query, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->query_len + 1;
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Query is too long");
    return NULL;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(isearch_get_hints, void)
  e->query_len = tl_fetch_string0 (e->query, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->query_len + 1;
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Query is too long");
    return NULL;
  }
TL_PARSE_FUN_END

#ifdef TYPES
TL_PARSE_FUN(isearch_get_types, void)
  e->query_len = tl_fetch_string0 (e->query, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->query_len + 1;
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Query is too long");
    return NULL;
  }
TL_PARSE_FUN_END
#endif

TL_PARSE_FUN(isearch_get_suggestion, void)
  e->query_len = tl_fetch_string0 (e->query, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->query_len + 1;
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Query is too long");
    return NULL;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(isearch_get_top, void)
  e->limit = tl_fetch_int();
TL_PARSE_FUN_END

TL_PARSE_FUN(isearch_get_best, void)
  e->limit = tl_fetch_int();
TL_PARSE_FUN_END

TL_PARSE_FUN(isearch_add_black_list, void)
  e->query_len = tl_fetch_string0 (e->query, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->query_len + 1;
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Query is too long");
    return NULL;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(isearch_delete_black_list, void)
  e->query_len = tl_fetch_string0 (e->query, STATS_BUFF_SIZE - extra->size - 1);
  extra->size += e->query_len + 1;
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Query is too long");
    return NULL;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(isearch_get_black_list, void)
TL_PARSE_FUN_END

TL_PARSE_FUN(isearch_force_update_black_list, void)
TL_PARSE_FUN_END


struct tl_act_extra *isearch_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("iSearch only supports actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return NULL;
  }

  int op = tl_fetch_int();
  if (tl_fetch_error()) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Request is empty");
    return NULL;
  }

  switch (op) {
    case TL_ISEARCH_ADD_SEARCH_QUERY:
      return tl_isearch_add_search_query();
    case TL_ISEARCH_GET_HINTS:
      return tl_isearch_get_hints();
#ifdef TYPES
    case TL_ISEARCH_GET_TYPES:
      return tl_isearch_get_types();
#endif
    case TL_ISEARCH_GET_SUGGESTION:
      return tl_isearch_get_suggestion();
    case TL_ISEARCH_GET_TOP:
      return tl_isearch_get_top();
    case TL_ISEARCH_GET_BEST:
      return tl_isearch_get_best();
    case TL_ISEARCH_ADD_BLACK_LIST:
      return tl_isearch_add_black_list();
    case TL_ISEARCH_DELETE_BLACK_LIST:
      return tl_isearch_delete_black_list();
    case TL_ISEARCH_GET_BLACK_LIST:
      return tl_isearch_get_black_list();
    case TL_ISEARCH_FORCE_UPDATE_BLACK_LIST:
      return tl_isearch_force_update_black_list();
  }

  tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown function %08x", op);
  return NULL;
}


/*
 *
 *      SERVER
 *
 */


void reopen_logs (void) {
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2 (fd, 0);
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      assert (close (fd) >= 0);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      assert (close (fd) >= 0);
    }
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_immediate_handler (const int sig) {
  const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (1);
}

static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << sig);
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << sig);
  signal (sig, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

void cron (void) {
  flush_binlog ();
}

int sfd;

void start_server (void) {
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
    exit (3);
  }

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  tl_parse_function = isearch_parse_function;
  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
    reopen_logs();
  }

  if (verbosity) {
    fprintf (stderr, "Server started\n");
  }

  for (i = 0; !(pending_signals & ~((1ll << SIGUSR1) | (1ll << SIGHUP))); i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network bufers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    if (sigpoll_cnt > 0) {
      if (verbosity > 1) {
        fprintf (stderr, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      }
      sigpoll_cnt = 0;
    }

    if (pending_signals & (1ll << SIGHUP)) {
      pending_signals &= ~(1ll << SIGHUP);

      sync_binlog (2);
    }

    if (pending_signals & (1ll << SIGUSR1)) {
      pending_signals &= ~(1ll << SIGUSR1);

      reopen_logs();
      sync_binlog (2);
    }

    if (now != prev_time) {
      prev_time = now;
      cron ();
    }

    if (epoll_pre_event) {
      epoll_pre_event ();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (verbosity > 0 && pending_signals) {
    fprintf (stderr, "Quitting because of pending signals = %llx\n", pending_signals);
  }

  epoll_close (sfd);
  assert (close (sfd) >= 0);

  flush_binlog_last();
  sync_binlog (2);
}


/*
 *
 *    MAIN
 *
 */


void usage (void) {
  printf ("usage: %s [options] <index-file>\n"
    "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n"
    "\t%s\n",
    progname,
    index_mode ?
    "Generates new index of isearch using given old index": 
    "Performs retrieval of helpful information using given index");

  parse_usage();
  exit (2);
}

int isearch_parse_option (int val) {
  switch (val) {
    case 'm':
      max_memory = atoi (optarg);
      if (max_memory < 1) {
        max_memory = 1;
      }
      max_memory *= 1048576;
      break;
    case 'A':
      find_bad_requests = 1;
      break;
    case 'i':
      index_mode = 1;
      break;
    case 'k':
      if (mlockall (MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf (stderr, "error: fail to lock paged memory\n");
      }
      break;
    default:
      return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;

  dl_set_debug_handlers();
  progname = argv[0];
  now = time (NULL);

  index_mode = 0;
  if (strstr (progname, "isearch-index") != NULL) {
    index_mode = 1;
  }

  remove_parse_option (204);
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory not including zmemory in mebibytes");
  parse_option ("find-bad-requests", no_argument, NULL, 'A', "output bad requests to stdout");
  if (!index_mode) {
    parse_option ("index-mode", no_argument, NULL, 'i', "run in index mode");
  }
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");

  parse_engine_options_long (argc, argv, isearch_parse_option);
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  if (find_bad_requests && !index_mode) {
    fprintf (stderr, "[-A] key works only in index mode ([-i]). Now quitting.\n");
    exit (1);
  }

  if (verbosity > 0) {
    fprintf (stderr, "index_mode = %d\n", index_mode);
  }

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (!index_mode) {
    if (port < PRIVILEGED_TCP_PORTS) {
      sfd = server_socket (port, settings_addr, backlog, 0);
      if (sfd < 0) {
        fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
        exit (1);
      }
    }
  }

  aes_load_pwd_file (NULL);

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }


  dynamic_data_buffer_size = (1 << 16); //16 for rpc_targets

  init_dyn_data();


  log_ts_interval = 3;

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
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

  i = init_all (Snapshot);

  index_load_time += mytime();

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "load index: done, jump_log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
       jump_log_pos, (long) dl_get_memory_used (), index_load_time);
  }

  close_snapshot (Snapshot, 1);


  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = -mytime();

  clear_log();

/*  set_log_data (fd[2], fsize[2]); */
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
    fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
             (long long) log_pos, (long) dl_get_memory_used (), binlog_load_time);
  }

  binlog_readed = 1;

  clear_write_log();
  start_time = time (NULL);

  if (index_mode) {
    int result = save_index();

    if (verbosity) {
      int len = isearch_prepare_stats();
      stats_buff[len] = 0;
      fprintf (stderr, "%s\n", stats_buff);
    }

    free_all();
    return result;
  }

/*  if (!binlog_disabled) {
    clear_read_log();
    assert (close (fd[2]) >= 0);
    fd[2] = -1;
  }*/

  start_server();

  free_all();
  return 0;
}

