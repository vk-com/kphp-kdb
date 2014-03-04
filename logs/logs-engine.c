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
#include <sys/wait.h>
#include <signal.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "kdb-logs-binlog.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#include "net-rpc-server.h"
#include "net-rpc-common.h"

#include "logs-data.h"
#include "dl-set-get.h"

#define MAX_VALUE_LEN (1 << 20)

#define VERSION "0.3"
#define VERSION_STR "logs "VERSION

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

char *dump_name;

long long binlog_loaded_size;
double binlog_load_time, index_load_time;

int child_pid;
int force_write_index;

long long cmd_get, cmd_set, cmd_stats, cmd_version;
double cmd_get_time = 0.0, cmd_set_time = 0.0;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0;

#define STATS_BUFF_SIZE (1 << 14)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
int logs_prepare_stats (void);

conn_type_t ct_logs_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "logs_engine_server",
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
  .memcache_fallback_type = &ct_logs_engine_server,
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
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d\n", old_key, old_key_len, size);
  }

  if (size + 1 < MAX_VALUE_LEN) {
    char *key;
    int key_len;

    eat_at (old_key, old_key_len, &key, &key_len);

    if (!write_only && key_len >= 6 && !strncmp (key, "select", 6) && size < MAX_QUERY_LEN) {
      int random_tag;
      if (sscanf (key, "select%d", &random_tag) != 1) {
        RETURN(set, -2);
      }

      if (verbosity > 1) {
        fprintf (stderr, "current_text %d\n", random_tag);
      }

      if (msg_reinit (MESSAGE(c), size, random_tag) < 0) {
        fprintf (stderr, "WARNING: not enough memory for message allocating\n");
        RETURN(set, -2); // not_enough memory
      }

      char *s = msg_get_buf (MESSAGE(c));
      assert (s);
      safe_read_in (&c->In, s, size);
      s[size] = 0;

      RETURN(set, 1);
    }

    if (key_len >= 9 && !strncmp (key, "add_event", 9) && key_len < 1000) {
      const char *s = key + 9;
      int sn = key_len - 9;
      if (s[0] != '(' || s[sn - 1] != ')')  {
        RETURN(set, -2);
      }
      s++, sn -= 2;
      int i;
      char *ts = stats_buff;

      if (verbosity > 2) {
        fprintf (stderr, "%d : %s\n", sn, s);
      }
      for (i = 0; i < sn && s[i] != ','; i++) {
        *ts++ = s[i];
      }
      *ts = 0;

      if (verbosity > 2 && i != sn) {
        fprintf (stderr, "key = %s | ts = %s, s = %s\n", key, stats_buff, s + i + 1);
      }

      int params[FN - 2];
      int j;
      for (j = 2; j < FN; j += 1 + std_t[j]) {
        int pos = -1;
        long long x;
        if (read_long (s + i + 1, &x, &pos) < 1 || (i + 1 + pos != sn && s[pos + i + 1] != ',')) {
          RETURN(set, -2);
        }

        i += pos + 1;
        if (std_t[j]) {
          *(long long *)&params[j - 2] = x;
        } else {
          params[j - 2] = (int)x;
        }
      }

      safe_read_in (&c->In, buf, size);
      buf[size] = 0;

      int result = do_add_event (stats_buff, params, buf);

      RETURN(set, result);
    }
  }

  RETURN(set, -2);
}

char *history_q[MAX_HISTORY + 1];
int history_l, history_r;

void history_q_add (char *s) {
  if (s == NULL) {
    return;
  }
  history_q[history_r++] = dl_strdup (s);
  if (history_r > MAX_HISTORY) {
    history_r = 0;
  }
  if (history_l >= history_r) {
    dl_strfree (history_q[history_l]);
    history_q[history_l++] = 0;
    if (history_l > MAX_HISTORY) {
      history_l = 0;
    }
  }
}

int memcache_get (struct connection *c, const char *old_key, int old_key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);
  }

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = logs_prepare_stats();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, old_key, stats_buff, len + len2 - 1);

    return 0;
  }

  GET_LOG;

  SET_LOG_VERBOSITY;

  INIT;

  if (!write_only && key_len >= 6 && !strncmp (key, "SELECT", 6) && key_len + 1 < STATS_BUFF_SIZE) {
    memcpy (stats_buff, key, key_len);
    stats_buff[key_len] = 0;
    int i;
    for (i = 0; i < key_len; i++) {
      if (stats_buff[i] == (char)0xa0) {
        stats_buff[i] = ' ';
      }
    }

    double tt = 0.0;
    if (test_mode) {
      tt = -cputime();
    }

    history_q_add (stats_buff);

    char *res = logs_select (stats_buff, key_len);

    return_one_key (c, old_key, res, strlen (res));

    if (test_mode) {
      tt += cputime();
      if (tt >= 0.01) {
        fprintf (stderr, "query %s total time : %.6lf\n\n", history_q[(history_r + MAX_HISTORY - 1) % MAX_HISTORY], tt);
      }
    }

    RETURN(get, 0);
  }

  if (!write_only && key_len >= 6 && !strncmp (key, "select", 6)) {
    int random_tag;
    if (sscanf (key, "select%d", &random_tag) != 1) {
      RETURN(get, 0);
    }
    message *msg = MESSAGE(c);

    if (msg_verify (msg, random_tag) < 0) {
      RETURN(get, 0);
    }

    double tt = 0.0;
    if (test_mode) {
      tt = -cputime();
    }

    history_q_add (msg->text);

    memcpy (buf, msg->text, msg->len + 1);

    char *res = logs_select (buf, msg->len);

    return_one_key (c, old_key, res, strlen (res));

    if (test_mode) {
      tt += cputime();
      if (tt >= 0.01) {
        fprintf (stderr, "query %s total time : %.6lf\n\n", stats_buff, tt);
      }
    }

    RETURN(get, 0);
  }

  if (key_len >= 11 && !strncmp (key, "create_type", 11)) {
    const char *s = key + 11;
    int sn = key_len - 11;
    if (sn >= 2 && s[0] == '(' && s[sn - 1] == ')') {
      memcpy (buf, s + 1, sn - 2);
      buf[sn - 2] = 0;

      char *res = do_create_type (buf);
      return_one_key (c, old_key, res, strlen (res));
    }

    RETURN(set, 0);
  }

  if (key_len >= 9 && !strncmp (key, "add_field", 9)) {
    const char *s = key + 9;
    int sn = key_len - 9;
    if (sn >= 2 && s[0] == '(' && s[sn - 1] == ')') {
      memcpy (buf, s + 1, sn - 2);
      buf[sn - 2] = 0;

      char *res = do_add_field (buf);
      return_one_key (c, old_key, res, strlen (res));
    }

    RETURN(set, 0);
  }

  if (!write_only && key_len >= 9 && !strncmp (key, "type_size", 9)) {
    int type;
    if (sscanf (key + 9, "%d", &type) != 1) {
      type = -1;
    } else if (type == -1) {
      return_one_key (c, old_key, "", 0);
      return 0;
    }

    char *res = get_type_size (type);

    return_one_key (c, old_key, res, strlen (res));
    return 0;
  }

  if (!write_only && key_len >= 5 && !strncmp (key, "color", 5)) {
    int field_num;
    long long field_value;
    int cur;
    if (sscanf (key + 5, "%d,%lld%n", &field_num, &field_value, &cur) != 2 || field_num < 0 || field_num >= FN || cur + 5 != key_len) {
      return 0;
    }

    char *res = dl_pstr ("%d", get_color (field_num, field_value));

    return_one_key (c, old_key, res, strlen (res));
    return 0;
  }

  if (key_len >= 12 && !strncmp (key, "change_color", 12)) {
    int field_num;
    long long field_value;
    int and_mask;
    int xor_mask;
    int cnt = 1;
    int cur;

    if (sscanf (key + 12, "%d,%lld,%d,%d%n#%d%n", &field_num, &field_value, &and_mask, &xor_mask, &cur, &cnt, &cur) < 4 || field_num < 0 || field_num >= FN || cur + 12 != key_len) {
      return 0;
    }

    char *res = do_set_color (field_num, field_value, cnt, and_mask, xor_mask) ? "OK" : "NOK";

    return_one_key (c, old_key, res, strlen (res));
    return 0;
  }

  if (!write_only && key_len == 5 && !strcmp (key, "types")) {
    char *res = get_types();

    return_one_key (c, old_key, res, strlen (res));
    return 0;
  }

  if (!write_only && key_len == 4 && !strcmp (key, "time")) {
    int c_time = get_time();
    char *res = dl_pstr ("%d", c_time);

    return_one_key (c, old_key, res, strlen (res));
    return 0;
  }

  if (!write_only && key_len >= 7 && !strncmp (key, "history", 7)) {
    int cnt;
    if (sscanf (key + 7, "%d", &cnt) != 1) {
      cnt = MAX_HISTORY;
    }

    char *res = buf;
    int cur = history_r;

    while (cnt-- && cur != history_l) {
      cur--;
      if (cur == -1) {
        cur += MAX_HISTORY + 1;
      }
      int l = strlen (history_q[cur]);
      if (res - buf + l + 2 >= MAX_VALUE_LEN) {
        break;
      }
      memcpy (res, history_q[cur], l);
      res += l;
      *res++ = '\n';
    }
    *res++ = 0;

    return_one_key (c, old_key, buf, strlen (buf));
    return 0;
  }

  return 0;
}



int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}

int logs_prepare_stats (void) {
  cmd_stats++;
  int log_uncommitted = compute_uncommitted_log_bytes();

  return snprintf (stats_buff, STATS_BUFF_SIZE,
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
        "events_memory_used\t%ld\n"
        "colors_memory_used\t%ld\n"
        "queue_begins_memory_used\t%ld\n"
        "cmd_get\t%lld\n"
        "cmd_set\t%lld\n"
        "cmd_stats\t%lld\n"
        "cmd_version\t%lld\n"
        "cmd_get_time\t%.7lf\n"
        "cmd_set_time\t%.7lf\n"
        "max_cmd_get_time\t%.7lf\n"
        "max_cmd_set_time\t%.7lf\n"
        "limit_max_dynamic_memory\t%lld\n"
        "limit_query_memory\t%lld\n"
        "events_in_memory\t%d\n"
        "events_total\t%d\n"
        "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n",
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
        events_memory,
        get_colors_memory(),
        get_q_st_memory(),
        cmd_get,
        cmd_set,
        cmd_stats,
        cmd_version,
        cmd_get_time,
        cmd_set_time,
        max_cmd_get_time,
        max_cmd_set_time,
        max_memory,
        query_memory,
        eq_n,
        eq_total);
}

int memcache_stats (struct connection *c) {
  int len = logs_prepare_stats ();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
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

static void sigrtmax_handler (const int sig) {
  const char message[] = "got SIGUSR3, write index.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  force_write_index = 1;
}

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

  flush_binlog_ts();

  int res = fork();

  if (res < 0) {
    fprintf (stderr, "fork: %m\n");
  } else if (!res) {
    binlogname = NULL;
    res = save_index();
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
  flush_binlog();
  if (force_write_index) {
    fork_write_index();
  }
  check_child_status();
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

  if (verbosity > 0) {
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

  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGRTMAX, sigrtmax_handler);
  signal (SIGPOLL, sigpoll_handler);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
    reopen_logs();
  }

  if (verbosity > 0 || test_mode) {
    fprintf (stderr, "Server started in %.6lfs\n", clock() * 1.0 / CLOCKS_PER_SEC);
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
    "Generates new index of logs using given old index": 
    "Performs retrieval of helpful information using given index");

  parse_usage();
  exit (2);
}

int logs_parse_option (int val) {
  switch (val) {
    case 'D':
      assert (!dump_mode);
      dump_mode = 1;
      dump_query = optarg;
      break;
    case 'e':
      mean_event_size = atoi (optarg);
      assert (mean_event_size > 50 && mean_event_size < 1000);
      break;
    case 'F':
      from_ts = atoi (optarg);
      if (from_ts < 0) {
        usage();
        return 2;
      }
      break;
    case 'm':
      max_memory = atol (optarg) * 1048576;
      break;
    case 'N': {
      int num, pos;
      assert (sscanf (optarg, "%d%n", &num, &pos) >= 1 && 0 <= num && num < FN && optarg[pos] == ',');
      if (!is_name (optarg + pos + 1)) {
        fprintf (stderr, "Not a valid name: \"%s\"\n", optarg + pos + 1);
        usage();
        return 2;
      }
      field_names[num] = optarg + pos + 1;
      break;
    }
    case 'q':
      query_memory = atol (optarg) * 1048576;
      break;
    case 'R':
      dump_name = optarg;
      break;
    case 's':
      assert (!dump_mode);
      dump_mode = 2;
      dump_query = "0";
      stat_queries_file = optarg;
      break;
    case 'T':
      to_ts = atoi (optarg);
      if (to_ts < 0) {
        usage();
        return 2;
      }
      break;
    case 'i':
      index_mode = 1;
      break;
    case 'I':
      dump_index_mode = 1;
      break;
    case 'k':
      if (mlockall (MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf (stderr, "error: fail to lock paged memory\n");
      }
      break;
    case 'n':
      errno = 0;
      nice (atoi (optarg));
      if (errno) {
        perror ("nice");
      }
      break;
    case 't':
      test_mode = 1;
      query_memory = 500000000;
      break;
    case 'V':
      my_verbosity++;
      break;
    case 'w':
      write_only = 1;
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
  if (strstr (progname, "logs-index") != NULL) {
    index_mode = 1;
  }

  remove_parse_option (204);
  parse_option ("generate-dump", required_argument, NULL, 'D', "<type[,query]> dump events of specified type into stdout or all events if type == 0, satisfying specified query, and exit");
  parse_option ("mean-event-size", required_argument, NULL, 'e', "<mean-event-size> sets estimated event size");
  parse_option ("dump-from", required_argument, NULL, 'F', "<from-timestamp> first timestamp for event dumping, default is 0");
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory in mebibytes");
  parse_option ("set-field-name", required_argument, NULL, 'N', "<field_number,field_name> specify custom field name for standard field with id field_number");
  parse_option ("query-memory-limit", required_argument, NULL, 'q', "<query-memory-limit> sets amount of memory reserved for queries (not events) in mebibytes");
  parse_option ("read-dump", required_argument, NULL, 'R', "<dump-name> file with dump from logs-engine to read from instead of binlog");
  parse_option (NULL, required_argument, NULL, 's', "<queries_file> answer to stat queries from specified file, answers will be printed to stdout, incompatible with -D");
  parse_option ("dump-to", required_argument, NULL, 'T', "<to-timestamp> last timestamp for event dumping, default is INT_MAX");
  if (!index_mode) {
    parse_option ("index-mode", no_argument, NULL, 'i', "run in index mode");
  }
  parse_option ("dump-time-table", no_argument, NULL, 'I', "dump time table from index in human readeable format and exit");
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");
  parse_option (NULL, no_argument, NULL, 'n', "sets niceness");
  parse_option ("test-mode", no_argument, NULL, 't', "output queries time to stderr");
  parse_option ("output-queries", no_argument, NULL, 'V', "output queries into stderr");
  if (!index_mode) {
    parse_option ("write-only", no_argument, NULL, 'w', "don't save changes in memory and don't answer queries");
  }

  parse_engine_options_long (argc, argv, logs_parse_option);
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  assert (0 < query_memory && query_memory + (1 << 26) < max_memory);

  if (verbosity > 0) {
    fprintf (stderr, "index_mode = %d\n", index_mode);
  }

  if (dump_mode) {
    index_mode = 1;

    if (from_ts > to_ts) {
      fprintf (stderr, "from_ts can't be greater than to_ts\n");
      usage();
      exit (2);
    }
  }

  if (index_mode) {
    binlog_disabled = 1;
    write_only = 1;
  }

  if (dump_name) {
    if (write_only) {
      fprintf (stderr, "can't use dump file in current mode\n");
      usage();
      exit (2);
    }
  }

  if (dump_name) {
    binlog_disabled = 1;
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

    if (verbosity > 0) {
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

  if (verbosity > 0) {
    fprintf (stderr, "load index: done, jump_log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
       jump_log_pos, (long) dl_get_memory_used(), index_load_time);
  }

  close_snapshot (Snapshot, 1);

  if (dump_name) {
    load_dump (dump_name);
  } else {
    //Binlog reading
    Binlog = open_binlog (engine_replica, jump_log_pos);
    if (!Binlog) {
      fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
      exit (1);
    }

    binlogname = Binlog->info->filename;

    if (verbosity > 0) {
      fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
    }
    binlog_load_time = -mytime();

    clear_log();
    init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

    if (verbosity > 0) {
      fprintf (stderr, "replay log events started\n");
    }

    i = replay_log (0, 1);

    if (verbosity > 0) {
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

    if (verbosity > 0) {
      fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
               (long long) log_pos, (long) dl_get_memory_used (), binlog_load_time);
    }

    clear_write_log();
  }
  binlog_readed = 1;

  start_time = time (NULL);

  if (dump_mode) {
    if (dump_mode == 2) {
      print_stats();
    }
    free_all();
    return 0;
  }

  if (index_mode) {
    int result = save_index();

    if (verbosity > 0) {
      int len = logs_prepare_stats();
      stats_buff[len] = 0;
      fprintf (stderr, "%s\n", stats_buff);
    }

    free_all();
    return result;
  }

  start_server();

  free_all();
  return 0;
}

