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

    Copyright 2010-2013	Vkontakte Ltd
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

#include "crc32.h"
#include "kdb-data-common.h"
#include "kdb-mf-binlog.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#include "net-rpc-server.h"
#include "net-rpc-common.h"

#include "mf-data.h"

#define MAX_VALUE_LEN (1 << 20)

#define VERSION "0.53"
#define VERSION_STR "mf "VERSION

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

char *indexname;
char binlog_fname_buff[256], newindex_fname_buff[256];

long long binlog_loaded_size;
double binlog_load_time, index_load_time;

long long cmd_get, cmd_set, cmd_delete, cmd_stats, cmd_version;
double cmd_get_time = 0.0, cmd_set_time = 0.0, cmd_delete_time = 0.0;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0, max_cmd_delete_time = 0.0;

#define STATS_BUFF_SIZE (1 << 14)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
int mf_prepare_stats (void);


int mf_engine_wakeup (struct connection *c);
int mf_engine_alarm (struct connection *c);

conn_type_t ct_mf_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "mf_engine_server",
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
  .wakeup = mf_engine_wakeup,
  .alarm = mf_engine_alarm,
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
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);
int memcache_get_start (struct connection *c);
int memcache_get_end (struct connection *c, int key_count);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = memcache_get_start,
  .mc_get = memcache_get,
  .mc_get_end = memcache_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = memcache_delete,
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
  .memcache_fallback_type = &ct_mf_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


char buf[MAX_VALUE_LEN];

#define mytime() get_utime (CLOCK_MONOTONIC)

int get_int (const char **s) {
  int res = 0;
  while (**s <= '9' && **s >= '0') {
    res = res * 10 + **s - '0';
    (*s)++;
  }
  return res;
}

#define MAX_FRIENDS 20000

int fr_buff[MAX_FRIENDS];

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

#define INIT double cmd_time = -mytime()
#define RETURN(x, y)                         \
  cmd_time += mytime() - 1e-6;               \
  if ((y) != -2) {                           \
    cmd_ ## x++;                             \
  }                                          \
  cmd_ ## x ## _time += cmd_time;            \
  if (cmd_time > max_cmd_ ## x ## _time) {   \
    max_cmd_ ## x ## _time = cmd_time;       \
  }                                          \
  return (y)

int memcache_store (struct connection *c, int op, const char *old_key, int old_key_len, int flags, int delay, int size) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d, \n", old_key, old_key_len, size);
  }

  if (size + 1 < MAX_VALUE_LEN) {
    char *key;
    int key_len;

    eat_at (old_key, old_key_len, &key, &key_len);

    //add("common_friends$id[,$add]", "$n,$f1,...,$fn")
    if (key_len >= 14 && !strncmp (key, "common_friends", 14)) {
      int uid;
      int add;
      int t = sscanf (key + 14, "%d,%d\n", &uid, &add);

      //TODO: comment for testing
      if (add != -1 && add != -4 && add != 1 && add != 4) {
        active_aio_queries |= (1 << 18);
        RETURN(set, -2);
      }

      if (t != 1 && t != 2) {
        RETURN(set, -2);
      }
      if (t == 1) {
        add = 1;
      }

      safe_read_in (&c->In, buf, size);
      buf[size] = 0;

      const char *s = buf;
      int n = get_int (&s), i;

      if (n >= MAX_FRIENDS - 1) {
        active_aio_queries |= (1 << 15);
        RETURN(set, 0);
      }

      if (n < 0) {
        active_aio_queries |= (1 << 16);
        RETURN(set, 0);
      }

      for (i = 0; i < n; i++)
      {
        if (*s == 0) {
          RETURN(set, 0);
        }
        s++;
        fr_buff[i] = get_int (&s);
        if (fr_buff[i] <= 0 || fr_buff[i] >= (1 << 28)) {
          active_aio_queries |= (1 << 17);
          RETURN(set, 0);
        }
      }

      int res = add_common_friends (uid, add, fr_buff, n);
      RETURN(set, res);
    }
  }

  RETURN(set, -2);
}


int memcache_try_get (struct connection *c, const char *old_key, int old_key_len) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 11 && !strncmp (key, "unload_user", 11)) {
    int uid;

    sscanf (key, "unload_user%d", &uid);
//    test_user_unload (uid);

    return_one_key (c, old_key, "0", 1);

    RETURN(get, 0);
  }

  if (key_len >= 10 && !strncmp (key, "unload_lru", 10)) {
//    user_LRU_unload();
    return_one_key (c, old_key, "0", 1);

    RETURN(get, 0);
  }


  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = mf_prepare_stats ();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    assert (len + len2 + 1 < STATS_BUFF_SIZE);
    return_one_key (c, old_key, stats_buff, len + len2 - 1);

    return 0;
  }


  if (key_len >= 9 && !strncmp (key, "exception", 9)) {
    int id, fid;
    if (sscanf (key, "exception%d_%d", &id, &fid) == 2) {
       do_add_exception (id, fid);
    }
    return_one_key (c, old_key, "0", 1);

    RETURN(get, 0);
  }

  if (key_len >= 16 && !strncmp (key, "clear_exceptions", 16)) {
    int id;
    if (sscanf (key, "clear_exceptions%d", &id) == 1) {
       int res = do_clear_exceptions (id);
       if (res < 0) {
         RETURN(get, res);
       }
    }
    return_one_key (c, old_key, "0", 1);

    RETURN(get, 0);
  }

  if (key_len >= 11 && !strncmp (key, "suggestions", 11)) {
    int uid, cnt, pos, min_common;
    int t = sscanf (key + 11, "%d#%d%n", &uid, &cnt, &pos);
    if (t != 1 && t != 2) {
      RETURN(get, 0);
    }
    if (t == 1) {
      cnt = MAX_CNT;
    } else {
      int t = sscanf (key + 11 + pos, ",%d", &min_common);
      if (t <= 0) {
        min_common = 1;
      }
    }

    if (2 * cnt + 5 > MAX_FRIENDS) {
      active_aio_queries |= (1 << 19);
      cnt = (MAX_FRIENDS - 5) / 2;
    }

    int res = get_suggestions (uid, cnt, min_common, fr_buff);
    if (res >= 0) {
      char *s = buf;
      int n = fr_buff[0], i;
      s += sprintf (s, "%d", n);
      for (i = 0; i < n; i++) {
        s += sprintf (s, ",%d,%d", fr_buff[i * 2 + 1], fr_buff[i * 2 + 2]);
      }
      return_one_key (c, old_key, buf, s - buf);
    }

    RETURN(get, res);
  }

  RETURN(get, 0);
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }

  WaitAio = 0;

  int res = memcache_try_get (c, key, key_len);

  if (res == -2) {
    if (verbosity > 2) {
      fprintf (stderr, "wait for aio..\n");
    }
    if (c->flags & C_INTIMEOUT) {
      if (verbosity > 1) {
        fprintf (stderr, "memcache_get: IN TIMEOUT (%p)\n", c);
      }
      return 0;
    }
    if (c->Out.total_bytes > 8192) {
      c->flags |= C_WANTWR;
      c->type->writer (c);
    }
//    fprintf (stderr, "memcache_get_nonblock returns -2, WaitAio=%p\n", WaitAio);
    if (!WaitAio) {
      fprintf (stderr, "WaitAio=0 - no memory to load user metafile, query dropped.\n");
      return 0;
    }
    conn_schedule_aio (WaitAio, c, 0.7, &aio_metafile_query_type);
    set_connection_timeout (c, 0.5);
    return 0;
  }

  return 0;
}

int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  c->query_start_time = mytime();
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  c->last_query_time = mytime() - c->query_start_time;
  write_out (&c->Out, "END\r\n", 5);
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get end: query time %.3fms\n", c->last_query_time * 1000);
  }
  return 0;
}

int memcache_delete (struct connection *c, const char *old_key, int old_key_len) {
  INIT;

  if (verbosity > 1) {
    fprintf (stderr, "memcache_delete: key='%s'\n", old_key);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  //delete("user_object{$uid},{$type}:{$obj_id}")
  if (key_len >= 9 && !strncmp (key, "exception", 9)) {
    int id, fid;
    if (sscanf (key, "exception%d_%d", &id, &fid) == 2 && do_del_exception (id, fid)) {
      write_out (&c->Out, "DELETED\r\n", 9);
      RETURN(delete, 0);
    }

    write_out (&c->Out, "NOT_FOUND\r\n", 11);
    RETURN(delete, 0);
  }

  RETURN(delete, 0);
}



int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

int mf_prepare_stats (void) {
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
        "index_loaded_bytes\t%d\n"
        "index_size\t%lld\n"
        "index_path\t%s\n"
        "index_load_time\t%.6lfs\n"
        "unloaded_users\t%lld\n"
        "pid\t%d\n"
        "version\t%s\n"
        "pointer_size\t%d\n"
        "allocated_metafile_bytes\t%lld\n"
        "loaded_suggestions\t%d\n"
        "current_memory_used\t%ld\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "cmd_delete\t%lld\n"
        "cmd_stats\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_get_time\t%.7lf\n"
        "cmd_set_time\t%.7lf\n"
        "cmd_delete_time\t%.7lf\n"
        "max_cmd_get_time\t%.7lf\n"
        "max_cmd_set_time\t%.7lf\n"
        "max_cmd_delete_time\t%.7lf\n"
        "tot_aio_queries\t%lld\n"
        "active_aio_queries\t%lld\n"
        "expired_aio_queries\t%lld\n"
        "avg_aio_query_time\t%.6f\n"
        "limit_max_dynamic_memory\t%ld\n"
        "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n",
        (long) (dyn_cur - dyn_first),
        (long) (dyn_last - dyn_first),
        binlog_loaded_size,
        log_readto_pos - jump_log_pos,
        binlog_load_time,
        log_pos,
        log_uncommitted,
        binlogname ? (strlen (binlogname) < 250 ? binlogname : "(too long)") : "(none)",
        log_first_ts,
        log_read_until,
        log_last_ts,
        header_size,
        fsize[0],
        fnames[0],
        index_load_time,
        get_del_by_LRU(),
        getpid(),
        VERSION,
        (int)(sizeof (void *) * 8),
        allocated_metafile_bytes,
        all_sugg_cnt,
        get_memory_used(),
        cmd_get,
        cmd_set,
        cmd_delete,
        cmd_stats,
        cmd_version,
        cmd_get_time,
        cmd_set_time,
        cmd_delete_time,
        max_cmd_get_time,
        max_cmd_set_time,
        max_cmd_delete_time,
        tot_aio_queries,
        active_aio_queries,
        expired_aio_queries,
        tot_aio_queries > 0 ? total_aio_time / tot_aio_queries : 0,
        max_memory);
}

int memcache_stats (struct connection *c) {
  int len = mf_prepare_stats ();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  assert (len + len2 + 1 < STATS_BUFF_SIZE);
  return 0;
}


int mf_engine_wakeup (struct connection *c) {
  struct mcs_data *D = MCS_DATA(c);

  if (verbosity > 1) {
    fprintf (stderr, "mf_engine_wakeup (%p)\n", c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_reading_query;
  assert (D->query_type == mct_get_resume);
  clear_connection_timeout (c);

  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  return 0;
}


int mf_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "mf_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return mf_engine_wakeup (c);
}



/*
 *
 *      END (PROXY MEMCACHE SERVER)
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

  if (daemonize) {
    setsid();
  }

  if (binlogname && !binlog_disabled) {
    open_file (2, binlogname, 1);
    assert (lock_whole_file (fd[2], F_WRLCK) > 0);
    set_log_data (fd[2], fsize[2]);
  }

  tl_aio_timeout = 0.7;
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

  for (i = 0; !(pending_signals & ~((1ll << SIGUSR1) | (1ll << SIGHUP))); i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
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

    if (!NOAIO) {
      check_all_aio_completions ();
    }
    tl_restart_all_ready();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
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
    "Generates new index of mutual friends using given old index": 
    "Performs suggetions retrieval queries using given index");

  parse_usage();
  exit (2);
}

int mf_parse_option (int val) {
  switch (val) {
    case 'm':
      max_memory = atoi (optarg);
      if (max_memory < 1) {
        max_memory = 1;
      }
      max_memory *= 1048576;
      break;
    case 's':
      suggname = optarg;
      break;
    case 'w':
      newindexname = optarg;
      break;
    case 'D':
      disable_crc32 = 3;
      break;
    case 'e':
      dump_mode = 1;
      index_mode = 1;
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

  set_debug_handlers ();
  progname = argv[0];
  now = time (NULL);

  index_mode = 0;
  if (strstr (progname, "mf-index") != NULL) {
    index_mode = 1;
  }
  binlog_readed = 0;

  remove_parse_option ('B');
  remove_parse_option (204);
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory not including zmemory in mebibytes");
  parse_option ("suggestions-file-name", required_argument, NULL, 's', "<suggestions-file-name> name of file with precalculated suggestions");
  parse_option ("new-index-name", required_argument, NULL, 'w', "<new-index-name> new name for index");
  parse_option ("disable-crc32", no_argument, NULL, 'D', "sets disable_crc32 to 3");
  parse_option ("generate-dump", no_argument, NULL, 'e', "generate dump to use in mf-merge-files");
  if (!index_mode) {
    parse_option ("index-mode", no_argument, NULL, 'i', "run in index mode");
  }
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");

  parse_engine_options_long (argc, argv, mf_parse_option);
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  init_files (5);

  if (verbosity > 0) {
    fprintf (stderr, "index_mode = %d\n", index_mode);
  }

  dynamic_data_buffer_size = (1 << 16);

  init_dyn_data();

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  indexname = argv[optind];

  index_load_time = -mytime();
  init_all (indexname);
  index_load_time += mytime();

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  if (!index_mode) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  aes_load_pwd_file (NULL);

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (!binlogname) {
    binlogname = ".bin";
  }

  if (!newindexname) {
    newindexname = ".tmp";
  }

  if (binlogname[0] == '.' && optind < argc && strlen (binlogname) + strlen (argv[optind]) < 250) {
    sprintf (binlog_fname_buff, "%s%s", argv[optind], binlogname);
    binlogname = binlog_fname_buff;
  }

  if (newindexname[0] == '.' && optind < argc && strlen (newindexname) + strlen (argv[optind]) < 250) {
    sprintf (newindex_fname_buff, "%s%s", argv[optind], newindexname);
    newindexname = newindex_fname_buff;
  }

  if (verbosity > 0) {
    fprintf (stderr, "opening binlog file %s\n", binlogname);
  }
  open_file (2, binlogname, -1);
  if (verbosity > 0) {
    fprintf (stderr, "binlog file %s opened %lld %d\n", binlogname, fsize[2], fd[2]);
  }

  log_ts_interval = 3;

  if (fsize[2] && fd[2] >= 0) {
    if (verbosity) {
      fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, fsize[2]);
    }
    binlog_load_time = mytime();

    clear_log();
    set_log_data (fd[2], fsize[2]);

    if (jump_log_pos) {
      if (verbosity) {
        fprintf (stderr, "log seek (jump_log_pos = %lld, jump_log_ts = %d, jump_log_crc32 = %u)\n",
                jump_log_pos, jump_log_ts, jump_log_crc32);
      }

      log_seek (jump_log_pos, jump_log_ts, jump_log_crc32);
    }

    if (verbosity) {
      fprintf (stderr, "replay log events started\n");
    }

    i = replay_log (0, 1);

    if (verbosity) {
      fprintf (stderr, "replay log events finished\n");
    }

    binlog_load_time = mytime() - binlog_load_time;
    binlog_loaded_size = fsize[2];

    if (i < 0) {
      fprintf (stderr, "fatal: error reading binlog\n");
      exit (1);
    }

    if (verbosity) {
      fprintf (stderr, "replay binlog file: done, pos=%lld, alloc_mem=%ld out of %ld, time %.06lfs\n",
         log_pos, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), binlog_load_time);
    }

    if (index_mode) {
      save_index (newindexname);

      if (verbosity) {
        int len = mf_prepare_stats();
        stats_buff[len] = 0;
        fprintf (stderr, "%s\n", stats_buff);
      }

      free_all();
      return 0;
    }

    if (!binlog_disabled) {
      clear_read_log();
      close (fd[2]);
      fd[2] = -1;
    }
  }

  binlog_readed = 1;


  clear_write_log();
  start_time = time (NULL);

  if (suggname != NULL) {
    load_suggestions (suggname);
  }

  start_server();

  free_all();
  return 0;
}
