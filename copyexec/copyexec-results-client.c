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
              2011-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <signal.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>

#include "kfs.h"
#include "kdb-binlog-common.h"
#include "net-connections.h"
#include "net-rpc-client.h"
#include "copyexec-rpc.h"
#include "copyexec-data.h"
#include "copyexec-rpc.h"
#include "copyexec-rpc-layout.h"

static long long jump_log_pos = 0LL;
static int jump_log_ts;
static unsigned jump_log_crc32;
static char *remote_hostname;
static long long handshake_log_pos = 0;
static int handshake_sent_time = 0, data_sent_time = 0, ping_sent_time = 0;
static int ping_period = 30;

int rpc_send_handshake (struct connection *c);
int rpcc_execute (struct connection *c, int op, int len);

struct rpc_client_functions copyexec_rpc_client = {
  .execute = rpcc_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpc_send_handshake
};

struct conn_target default_ct = {
.min_connections = 1,
.max_connections = 1,
.type = &ct_rpc_client,
.extra = (void *)&copyexec_rpc_client,
.reconnect_timeout = 17
};

/**************************************** results queue  *******************************************************/
#define RESULTS_QUEUE_SIZE 0x1000
struct results_queue_entry {
  long long binlog_pos;
  int transaction_id;
  unsigned result;
} RQ[RESULTS_QUEUE_SIZE];

static int rq_left = 0, rq_right = 0, rq_size = 0;

struct results_queue_entry *results_queue_push (void) {
  if (rq_size >= RESULTS_QUEUE_SIZE) {
    return NULL;
  }
  struct results_queue_entry *R = &RQ[rq_right];
  if (++rq_right == RESULTS_QUEUE_SIZE) {
    rq_right = 0;
  }
  rq_size++;
  return R;
}

struct results_queue_entry *results_queue_front (void) {
  if (!rq_size) {
    return NULL;
  }
  return &RQ[rq_left];
}

struct results_queue_entry *results_queue_pop (void) {
  if (!rq_size) {
    return NULL;
  }
  struct results_queue_entry *R = &RQ[rq_left];
  if (++rq_left == RESULTS_QUEUE_SIZE) {
    rq_left = 0;
  }
  rq_size--;
  return R;
}

/**************************************** hostname *******************************************************/
static char *hostname;

int detect_hostname (void) {
  static char hostname_buffer[256];
  int r, i;
  if (!hostname || !*hostname) {
    hostname = getenv ("HOSTNAME");
    if (!hostname || !*hostname) {
      int fd = open ("/etc/hostname", O_RDONLY);
      if (fd < 0) {
        kprintf ("cannot read /etc/hostname: %m\n");
        exit (2);
      }
      r = read (fd, hostname_buffer, 256);
      if (r <= 0 || r >= 256) {
        kprintf ("cannot read hostname from /etc/hostname: %d bytes read\n", r);
        exit (2);
      }
      hostname_buffer[r] = 0;
      close (fd);
      hostname = hostname_buffer;
      while (*hostname == 9 || *hostname == 32) {
        hostname++;
      }
      i = 0;
      while (hostname[i] > 32) {
        i++;
      }
      hostname[i] = 0;
    }
  }
  if (!hostname || !*hostname) {
    kprintf ("fatal: cannot detect hostname\n");
    exit (2);
  }
  i = 0;
  while ((hostname[i] >= '0' && hostname[i] <= '9') || hostname[i] == '.' || hostname[i] == '-' || hostname[i] == '_' || (hostname[i] >= 'A' && hostname[i] <= 'Z') || (hostname[i] >= 'a' && hostname[i] <= 'z')) {
    i++;
  }
  if (hostname[i] || i >= 64) {
    kprintf ("fatal: bad hostname '%s'\n", hostname);
    exit (2);
  }
  vkprintf (1, "hostname is %s\n", hostname);
  return 0;
}

/**************************************** RPC functions *******************************************************/

enum client_state {
  st_startup = 0,
  st_handshake_sent = 1,
  st_handshake_received = 2,
  st_data_sent = 3
} cur_client_state = st_startup;

char *client_state_to_str (enum client_state s) {
  switch (s) {
    case st_startup: return "startup";
    case st_handshake_sent: return "handshake_sent";
    case st_handshake_received: return "handshake_received";
    case st_data_sent: return "data_sent";
    default: return "unknown";
  }
}

static int P[MAX_PACKET_LEN / 4], Q[MAX_PACKET_LEN / 4];

int rpc_send_handshake (struct connection *c) {
  vkprintf (3, "rpc_send_handshake (c: %p)\n", c);
  if (cur_client_state != st_startup) {
    vkprintf (2, "rpc_send_handshake: reconnection. cur_client_state %s\n", client_state_to_str (cur_client_state));
  }
  if (!main_volume_id) {
    kprintf ("rpc_send_handshake: main_volume_id isn't initialized.\n");
    exit (1);
  }
  if (!random_tag) {
    kprintf ("rpc_send_handshake: random_tag isn't initialized.\n");
    exit (1);
  }
  detect_hostname ();
  struct copyexec_rpc_handshake *E = rpc_create_query (Q, sizeof (*E) + strlen (hostname), c, COPYEXEC_RPC_TYPE_HANDSHAKE);
  if (E == NULL) {
    vkprintf (2, "rpc_send_handshake: rpc_create_query returns NULL.\n");
    return -__LINE__;
  }
  E->volume_id = main_volume_id;
  E->random_tag = random_tag;
  E->hostname_length = strlen (hostname);
  E->pid = getpid ();
  memcpy (E->hostname, hostname, E->hostname_length);
  cur_client_state = st_handshake_sent;
  handshake_sent_time = now;
  return rpc_send_query (Q, c);
}

int rpc_send_data (struct connection *c) {
  for (;;) {
    struct results_queue_entry *A = results_queue_front ();
    if (A == NULL) {
      vkprintf (3, "rpc_send_data: results_queue is empty.\n");
      return 0;
    }
    vkprintf (3, "rpc_send_data: A->pos: 0x%llx, A->transaction_id: %d, A->result:%x\n", A->binlog_pos, A->transaction_id, A->result);
    if (A->binlog_pos <= handshake_log_pos) {
      vkprintf (3, "rpc_send_data: Skipping sending A (handshake_log_pos: %lld)\n", handshake_log_pos);
      results_queue_pop ();
      continue;
    }
    struct copyexec_rpc_send_data *E = rpc_create_query (Q, sizeof (*E), c, COPYEXEC_RPC_TYPE_SEND_DATA);
    if (E == NULL) {
      vkprintf (2, "rpc_send_data: rpc_create_query returns NULL.\n");
      return -__LINE__;
    }
    E->binlog_pos = A->binlog_pos;
    E->transaction_id = A->transaction_id;
    E->result = A->result;
    vkprintf (3, "rpc_send_data: E->binlog_pos: 0x%llx, E->transaction_id: %d, E->result:%x\n", E->binlog_pos, E->transaction_id, E->result);
    cur_client_state = st_data_sent;
    data_sent_time = now;
    return rpc_send_query (Q, c);
  }
  return 0;
}

int rpc_send_ping (struct connection *c) {
  vkprintf (3, "rpc_send_ping (c: %p)\n", c);
  if (rpc_create_query (Q, 0, c, COPYEXEC_RPC_TYPE_PING) == NULL) {
    vkprintf (2, "rpc_send_ping: rpc_create_query returns NULL.\n");
    return -__LINE__;
  }
  ping_sent_time = now;
  return rpc_send_query (Q, c);
}

int rpc_execute_handshake_err (struct connection *c, struct copyexec_rpc_handshake_error *P, int len) {
  if (len != sizeof (struct copyexec_rpc_handshake_error)) {
    return -__LINE__;
  }
  kprintf ("rpc_execute_handshake_err (c: %p, P->error_code: %d). cur_client_state: %s\n", c, P->error_code, client_state_to_str (cur_client_state));
  handshake_sent_time = now + 10 * 60; /* send handshake packet again ~10 minutes latter */
  return 0;
}

int rpc_execute_value_pos (struct connection *c, struct copyexec_rpc_pos *P, int len) {
  if (len != sizeof (struct copyexec_rpc_pos)) {
    return -__LINE__;
  }
  vkprintf (3, "rpc_execute_value_pos (c: %p, P->binlog_pos: 0x%llx). cur_client_state: %s\n", c, P->binlog_pos, client_state_to_str (cur_client_state));
  struct results_queue_entry *A = results_queue_front ();

  switch (cur_client_state) {
    case st_handshake_sent:
      handshake_log_pos = P->binlog_pos;
      cur_client_state = st_handshake_received;
      break;
    case st_data_sent:
      A = results_queue_front ();
      if (A == NULL) {
        vkprintf (3, "rpc_execute_value_pos: unexpected VALUE_POS packet. Queue is empty.\n");
        return -__LINE__;
      }
      if (A->binlog_pos != P->binlog_pos) {
        vkprintf (3, "rpc_execute_value_pos: expected VALUE_POS (%lld), but received %lld.\n", A->binlog_pos, P->binlog_pos);
        return -__LINE__;
      }
      results_queue_pop ();
      cur_client_state = st_handshake_received;
      break;
    default:
      vkprintf (3, "rpc_execute_value_pos: unexpected cur_client_state (%s).\n", client_state_to_str (cur_client_state));
      return -__LINE__;
  }
  return 0;
}

int rpcc_execute (struct connection *c, int op, int len) {
  vkprintf (1, "rpcc_execute: fd=%d, op=%x, len=%d\n", c->fd, op, len);

  if (len > MAX_PACKET_LEN) {
    return SKIP_ALL_BYTES;
  }

  assert (read_in (&c->In, &P, len) == len);
  len -= 16;
  switch (op) {
    case COPYEXEC_RPC_TYPE_ERR_HANDSHAKE:
      return rpc_execute_handshake_err (c, (struct copyexec_rpc_handshake_error *) (P + 3), len);
    case COPYEXEC_RPC_TYPE_VALUE_POS:
      return rpc_execute_value_pos (c, (struct copyexec_rpc_pos *) (P + 3), len);
  }

  return -__LINE__;
}

/**************************************** main binlog  *******************************************************/

int copyexec_results_client_set_status (struct lev_copyexec_main_transaction_status *E) {
  struct results_queue_entry *A = results_queue_push ();
  if (A == NULL) {
    vkprintf (3, "Results queue full. Stop binlog replaying.\n");
    return -1;
  }
  A->binlog_pos = log_cur_pos ();
  A->result = (((unsigned)(E->type - LEV_COPYEXEC_MAIN_TRANSACTION_STATUS)) << 28) | (E->result & 0x0fffffff);
  A->transaction_id = E->transaction_id;

  return 0;
}

static int copyexec_results_client_replay_logevent (struct lev_generic *E, int size) {
  vkprintf (4, "LE (type=%x, offset=%lld)\n", E->type, log_cur_pos ());
  int s;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return copyexec_main_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
      return default_replay_logevent (E, size);
    case LEV_COPYEXEC_MAIN_TRANSACTION_STATUS ... LEV_COPYEXEC_MAIN_TRANSACTION_STATUS + ts_io_failed:
      s = sizeof (struct lev_copyexec_main_transaction_status);
      if (size < s) {
        return -2;
      }
      if (copyexec_results_client_set_status ((struct lev_copyexec_main_transaction_status *) E) < 0) {
        return -3;
      }
      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_BEGIN:
      s = sizeof (struct lev_copyexec_main_command_begin);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_command_begin *) E)->command_size;
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_END:
      s = sizeof (struct lev_copyexec_main_command_end);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_command_end *) E)->saved_stdout_size;
      s += ((struct lev_copyexec_main_command_end *) E)->saved_stderr_size;
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_TRANSACTION_ERROR:
      s = sizeof (struct lev_copyexec_main_transaction_err);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_transaction_err *) E)->error_msg_size;
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_WAIT:
      s = sizeof (struct lev_copyexec_main_command_wait);
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_KILL:
      s = sizeof (struct lev_copyexec_main_command_kill);
      if (size < s) {
        return -2;
      }

      return s;
    case LEV_COPYEXEC_MAIN_TRANSACTION_SKIP:
      s = sizeof (struct lev_copyexec_main_transaction_skip);
      if (size < s) {
        return -2;
      }
/*
      if (first_transaction_id < ((struct lev_copyexec_main_transaction_skip *) E)->first_transaction_id) {
        first_transaction_id = ((struct lev_copyexec_main_transaction_skip *) E)->first_transaction_id;
      }
*/
      return s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());
  exit (1);
}

static void copyexec_results_client_read_new_events (void) {
  vkprintf (4, "copyexec_results_client_read_new_events: binlog_fd = %d\n", binlog_fd);
  int res = replay_log (0, 1);
  if (res < 0) {
    vkprintf (3, "replay_log returns %d.\n", res);
  }
}

static void copyexec_results_client_cron (void) {
  create_all_outbound_connections ();
}

int parse_copyexec_results_addr (const char *s) {
  if (remote_hostname) {
    kprintf ("Command line switch -R is given more than once.\n");
    return -__LINE__;
  }

  const char *p = strchr (s, ':');
  if (p == NULL) {
    kprintf ("parse_copyexec_results_addr (%s): port isn't given.\n", s);
    return -__LINE__;
  }

  int l = p - s;
  if (sscanf (p+1, "%d", &default_ct.port) != 1) {
    kprintf ("parse_copyexec_results_addr (%s): port isn't integer.\n", s);
    return -__LINE__;
  }

  remote_hostname = malloc (l + 1);
  assert (remote_hostname != NULL);
  memcpy (remote_hostname, s, l);
  remote_hostname[l] = 0;
  return 0;
}

pid_t create_results_sending_process (void) {
  if (remote_hostname == NULL) {
    vkprintf (1, "hostname isn't given. Skipping results sending process creation.\n");
    return 0;
  }
  struct hostent *h;
  if (!(h = gethostbyname (remote_hostname)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    kprintf ("cannot resolve %s\n", remote_hostname);
    exit (2);
  }
  default_ct.target = *((struct in_addr *) h->h_addr);

  pid_t p = fork ();
  if (p < 0) {
    kprintf ("create_results_sending_process: fork failed. %m\n");
    exit (1);
  }

  if (p) {
    return p;
  }

  change_process_name ("copyexec-client");
  struct conn_target *targ = create_target (&default_ct, 0);

  binlog_disabled = 1;
  //disable_crc32 = 3;

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld) from position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);
  clear_log ();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);
  vkprintf (1, "replay log events started\n");

  replay_logevent = copyexec_results_client_replay_logevent;
  int i = replay_log (0, 1);
  vkprintf (1, "replay_log returns %d.\n", i);
  clear_write_log ();

  int prev_time = 0;

  init_epoll ();
  init_netbuffers ();

  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

  //init_listening_connection (sfd, &ct_mysql_server, &mysql_methods);
  create_all_outbound_connections ();

  //signal (SIGINT, sigint_handler);
  //signal (SIGTERM, sigterm_handler);
  //signal (SIGUSR1, sigusr1_handler);

  if (daemonize) {
    //signal (SIGHUP, sighup_handler);
    signal (SIGHUP, SIG_IGN);
  }

  for (i = 0; ; i++) {
    if (!(i & 255)) {
      vkprintf (2, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }

    const int get_process_creation_time_main_pid = get_process_creation_time (main_pid);

    if (main_process_creation_time != get_process_creation_time_main_pid) {
      vkprintf (0, "Main process was terminated. Stop copyexec-results-client. main_process_creation_time:%d, get_process_creation_time (%d):%d\n", main_process_creation_time, (int) main_pid, get_process_creation_time_main_pid);
      break;
    }

    int delay = 10 + (lrand48 () % 239);
    vkprintf (4, "epoll_work (%d)\n", delay);
    epoll_work (delay);
    copyexec_results_client_read_new_events ();

    if (cur_client_state == st_handshake_sent && now - 60 > handshake_sent_time) {
      struct connection *c = get_target_connection (targ);
      if (c == NULL) {
        vkprintf (1, "get_target_connection returns NULL (wants to resend handshake packet).\n");
      } else {
        rpc_send_handshake (c);
      }
    } else if (rq_size > 0 && (cur_client_state == st_handshake_received || (cur_client_state == st_data_sent && now - 60 > data_sent_time))) {
      struct connection *c = get_target_connection (targ);
      if (c == NULL) {
        vkprintf (1, "get_target_connection returns NULL (wants to send data packet).\n");
      } else {
        rpc_send_data (c);
      }
    } else if (cur_client_state == st_handshake_received && ping_sent_time < now - ping_period && data_sent_time < now - ping_period) {
      struct connection *c = get_target_connection (targ);
      if (c == NULL) {
        vkprintf (1, "get_target_connection returns NULL (wants to send ping packet).\n");
      } else {
        rpc_send_ping (c);
      }
    }

    if (now != prev_time) {
      prev_time = now;
      copyexec_results_client_cron ();
    }
    if (quit_steps && !--quit_steps) break;
  }

  exit (0);
  return 0;
}
