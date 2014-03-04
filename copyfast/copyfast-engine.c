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
              2011-2013 Vitaliy Valtman
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <aio.h>
#include <netdb.h>
#include <signal.h>

#include "md5.h"
#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "net-crypto-aes.h"

#include "copyfast-rpc-layout.h"
#include "copyfast-common.h"
#include "copyfast-engine-data.h"
#include "copyfast-engine.h"

#include "kdb-data-common.h"

#define VERSION_STR "copyfast-engine-0.5"

#define TCP_PORT 23918

#define MAX_NET_RES (1L << 16)
#define L vkprintf (6, "at line %d\n", __LINE__)

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#ifdef __LP64__ 
#define __M__ "64-bit"
#else
#define __M__ "32-bit"
#endif
#define FULL_VERSION      "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " " __M__  " after commit " COMMIT "\n"

#define PROTOCOL_VERSION 1
int verbosity, interactive, quit_steps, start_time, log_verbosity = 1;
int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;
int server_port = 23917;
unsigned host;

#define MAX_CLUSTERS 1000
struct cluster *Clusters[MAX_CLUSTERS];
struct cluster *CC;
int max_cluster;

// unsigned char is_letter[256];
char *progname = "copyfast-engine", *username, *binlogname, *logname;
char metaindex_fname_buff[256], binlog_fname_buff[256], hostname[256] = "127.0.0.1";

struct conn_target *main_targ;

#define STATS_BUFF_SIZE (1 << 20)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];

struct long_update_stats update_stats;

int rpcc_execute (struct connection *c, int op, int len);
int rpcs_execute (struct connection *c, int op, int len);

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

struct memcache_server_functions copyfast_engine_memcache_inbound = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_wakeup = mcs_do_wakeup,
  .mc_alarm = mcs_do_wakeup,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};
struct rpc_server_functions copyfast_rpc_server = {
  .execute = rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .memcache_fallback_type = &ct_memcache_server,
  .memcache_fallback_extra = &copyfast_engine_memcache_inbound
};

struct rpc_client_functions copyfast_rpc_client = {
  .execute = rpcc_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpc_send_join
};

struct rpc_client_functions copyfast_rpc_child = {
  .execute = rpcs_execute,
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
  .extra = (void *)&copyfast_rpc_client,
  .port = 23917,
  .reconnect_timeout = 17
};

struct conn_target default_child = {
  .min_connections = 1,
  .max_connections = 1,
  .type = &ct_rpc_client,
  .extra = (void *)&copyfast_rpc_child,
  .reconnect_timeout = 17
};



struct connection *get_target_connection (struct conn_target *targ) {
  struct connection *c;
  if (!targ->outbound_connections) {
    return 0;
  }
  c = targ->first_conn;
  while (1) {
    if (server_check_ready (c) == cr_ok) {
      return c;
    }
    if (c == targ->last_conn) { break;}
    c = c->next;
  }
  return 0;
}



void log_event (int level, char op, long long data) {
  if (level > log_verbosity) {
    return;
  }
  int pos = STATS->structured.update_pos;
  STATS->structured.update_operations[pos] = op;
  STATS->structured.update_data[pos] = data;
  STATS->structured.update_times[pos] = get_double_time_since_epoch ();
  STATS->structured.update_pos++;
  if (STATS->structured.update_pos == STATS_UPDATE_BUFF_SIZE) {
    STATS->structured.update_pos = 0;
  }
  pos = UPDATE_STATS->update_pos;
  UPDATE_STATS->update_operations[pos] = op;
  UPDATE_STATS->update_data[pos] = data;
  UPDATE_STATS->update_times[pos] = get_double_time_since_epoch ();
  UPDATE_STATS->update_pos++;
  if (UPDATE_STATS->update_pos == LONG_STATS_UPDATE_BUFF_SIZE) {
    UPDATE_STATS->update_pos = 0;
  }
}

int P[MAX_PACKET_LEN / 4], Q[MAX_PACKET_LEN / 4];


int rpc_send_children_request (struct connection *c) {
  vkprintf (2, "rpc_send_children_request: c = %p\n", c);
  struct rpc_children_request *T = (struct rpc_children_request *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_children_request), c, RPC_TYPE_CHILDREN_REQUEST) < 0) {
    return -1;
  }
  T->id = NODE_ID;
  STATS->structured.children_requests_sent ++;
  return rpc_send_query (T, c);
}


int rpc_send_join (struct connection *c) {    
  vkprintf (2, "rpc_send_join: c = %p\n", c);
  int i;
  for (i = 0; i < max_cluster; i++) if (Clusters[i]) {
    struct rpc_join *T = (struct rpc_join *)Q;
    if (rpc_create_query (T, sizeof (struct rpc_join), c, RPC_TYPE_JOIN) < 0) {
      return -1;
    }
    CC = Clusters[i];
    T->host = c->our_ip;
    host = c->our_ip;
    T->port = port;
    T->id = NODE_ID ? NODE_ID : CLUSTER_ID;
    T->binlog_position = BINLOG_POSITION;
    T->protocol_version = PROTOCOL_VERSION;
    STATS->structured.joined_sent++;
    rpc_send_query (T, c);
  }
  return 0;
}

int rpc_send_stats (struct connection *c) {    
  vkprintf (2, "rpc_send_stats: c = %p\n", c);
  struct rpc_stats *T = (struct rpc_stats *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_stats), c, RPC_TYPE_STATS) < 0) {
    return -1;
  }
  T->id = NODE_ID;
  memcpy (&T->stats, STATS, sizeof (union engine_stats));
  STATS->structured.stats_sent++;
  return rpc_send_query (T, c);
}

int rpc_send_update_stats (struct connection *c) {
  vkprintf (2, "rpc_send_update_stats: c = %p\n", c);
  struct rpc_update_stats *T = (struct rpc_update_stats *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_update_stats), c, RPC_TYPE_UPDATE_STATS) < 0) {
    return -1;
  }
  T->id = NODE_ID;
  memcpy (&T->stats, UPDATE_STATS, sizeof (struct long_update_stats));
  STATS->structured.update_stats_sent++;
  return rpc_send_query (T, c);
}

int rpc_execute_join_ack (struct connection *c, struct rpc_join_ack *P, int len) {
  vkprintf (2, "rpc_execute_join_ack: len = %d\n", len);
  if (len != sizeof (struct rpc_join_ack)) {
    return 0;
  }
  STATS->structured.join_ack_received++;
  NODE_ID = P->id;
  vkprintf (6, "%p %lld %lld\n", CC, NODE_ID, CLUSTER_ID);
  if (!(NODE_ID & ID_MASK) || (NODE_ID & CLUSTER_MASK) != CLUSTER_ID) {
    NODE_ID = CLUSTER_ID;
    return 0;
  }
  return rpc_send_children_request (c);
}

int rpc_execute_children_answer (struct connection *c, struct rpc_children_answer *P, int len) {
  vkprintf (2, "rpc_execute_children_answer: len = %d, children_num = %d\n", len, P->children_num);
  if (len != sizeof (struct rpc_children_answer) + sizeof (struct node) * P->children_num) {
    return 0;
  }
  vkprintf (6, "%p %lld %d\n", CC, NODE_ID, P->children_num);
  STATS->structured.children_received++;
  clear_all_children_connections ();
  create_children_connections (P->children, (P->children_num > MAX_CHILDREN ? MAX_CHILDREN : P->children_num));
  return 0;
}

int rpc_execute_kicked (struct connection *c, struct rpc_kicked *P, int len) {
  vkprintf (2, "rpc_kicked: len = %d\n", len);
  if (len != sizeof (struct rpc_kicked)) {
    return 0;
  }
  STATS->structured.kicked_received ++;
  NODE_ID = CLUSTER_ID;
  clear_all_children_connections ();
  return rpc_send_join (c);
}

int rpc_execute_delays (struct connection *c, struct rpc_delays *P, int len) {
  vkprintf (2, "rpc_delays: len = %d\n", len);

  if (len != sizeof (struct rpc_kicked)) {
    return 0;
  }
  STATS->structured.delays_received ++;
  SLOW_REQUEST_DELAY = P->slow_delay;
  MEDIUM_REQUEST_DELAY = P->medium_delay;
  return 0;
}

int rpc_execute_request_update_stats (struct connection *c, struct rpc_request_update_stats *P, int len) {
  vkprintf (2, "rpc_execute_request_update_stats: len = %d\n", len);
  if (len != sizeof (struct rpc_request_update_stats)) {
    return 0;
  }
  STATS->structured.request_update_stats_received ++;
  return rpc_send_update_stats (c);
}


int rpc_send_handshake (struct connection *c) {
  long long remote_id = get_id_by_connection (c);
  vkprintf (2, "rpc_send_handshake: remote_id = %lld\n", remote_id);
  if (!remote_id) {
    return 0;
  }
  struct rpc_handshake *T = (struct rpc_handshake *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_handshake), c, RPC_TYPE_HANDSHAKE) < 0) {
    return -1;
  }
  T->local_id = NODE_ID;
  T->remote_id = remote_id;
  T->binlog_position = BINLOG_POSITION;
  T->port = port;
  STATS->structured.handshake_sent ++;
  return rpc_send_query (T, c);
}

int rpc_send_handshake_accept (struct connection *c, long long remote_id) {
  vkprintf (2, "rpc_send_handshake_accept: remote_id = %lld\n", remote_id);
  if (!remote_id) {
    return 0;
  }
  struct rpc_handshake_accept *T = (struct rpc_handshake_accept *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_handshake_accept), c, RPC_TYPE_HANDSHAKE_ACCEPT) < 0) {
    return -1;
  }
  T->local_id = NODE_ID;
  T->remote_id = remote_id;
  T->binlog_position = BINLOG_POSITION;
  c->last_response_time = precise_now;
  STATS->structured.handshake_accept_sent ++;
  return rpc_send_query (T, c);
}

int rpc_send_handshake_reject (struct connection *c, long long remote_id) {
  vkprintf (2, "rpc_send_handshake_reject: remote_id = %lld\n", remote_id);
  if (!remote_id) {
    return 0;
  }
  struct rpc_handshake_reject *T = (struct rpc_handshake_reject *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_handshake_reject), c, RPC_TYPE_HANDSHAKE_REJECT) < 0) {
    return -1;
  }
  T->local_id = NODE_ID;
  T->remote_id = remote_id;
  STATS->structured.handshake_reject_sent ++;
  return rpc_send_query (T, c);
}

int rpc_send_divorce (struct connection *c) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_send_divorce\n");
  }
  struct rpc_divorce *T = (struct rpc_divorce *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_divorce), c, RPC_TYPE_DIVORCE) < 0) {
    return -1;
  }
  STATS->structured.divorce_sent ++;
  return rpc_send_query (T, c);
}

int rpc_send_binlog_info (struct connection *c, long long remote_id) {
  vkprintf (2, "rpc_send_binlog_info: remote_id = %lld\n", remote_id);
  if (!remote_id) {
    return 0;
  }
  struct rpc_binlog_info *T = (struct rpc_binlog_info *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_binlog_info), c, RPC_TYPE_BINLOG_INFO) < 0) {
    return -1;
  }
  T->local_id = NODE_ID;
  T->remote_id = remote_id;
  T->binlog_position = BINLOG_POSITION;
  STATS->structured.binlog_info_sent ++;
  return rpc_send_query (T, c);
}

int rpc_send_binlog_request (struct connection *c, long long remote_id, long long pos) {
  vkprintf (2, "rpc_send_binlog_request: remote_id = %lld, pos = %lld\n", remote_id, pos);
  if (!remote_id) {
    return 0;
  }
  if (pos <= 0) {
    pos = BINLOG_POSITION;
  }
  struct rpc_binlog_request *T = (struct rpc_binlog_request *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_binlog_request), c, RPC_TYPE_BINLOG_REQUEST) < 0) {
    return -1;
  }
  T->local_id = NODE_ID;
  T->remote_id = remote_id;
  T->binlog_position = pos;
  int color = get_relative_by_id (remote_id)->link_color;
  assert (0 <= color && color <= 2);
  STATS->structured.total_requested_num_color[color] ++;
  log_event (1, LOG_BINLOG_REQUEST_SENT, remote_id);
  STATS->structured.binlog_request_sent ++;
  return rpc_send_query (T, c);
}

int get_binlog_data (char *data, long long pos, int len);
unsigned long long get_crc64 (long long pos);
int rpc_send_binlog_data (struct connection *c, long long remote_id, long long pos) {
  vkprintf (2, "rpc_send_binlog_data: remote_id = %lld, pos = %lld\n", remote_id, pos);
  if (!remote_id) {
    return 0;
  }
  assert (pos < BINLOG_POSITION);
  int len = (BINLOG_POSITION - pos > MAX_SEND_LEN) ? MAX_SEND_LEN : BINLOG_POSITION - pos;
  struct rpc_binlog_data *T = (struct rpc_binlog_data *)Q;
  int llen = (len & 3) == 0 ? len : (len & ~3) + 4;
  assert (llen <= MAX_SEND_LEN);
  if (rpc_create_query (T, sizeof (struct rpc_binlog_data) + llen, c, RPC_TYPE_BINLOG_DATA) < 0) {
    return -1;
  }
  T->local_id = NODE_ID;
  T->remote_id = remote_id;
  T->binlog_position = pos;
  T->size = len;
  if (get_binlog_data (T->data, pos, len) < 0) {
    return 0;
  }
  T->crc64 = get_crc64 (pos);
  int color = get_relative_by_id (remote_id)->link_color;
  assert (0 <= color && color <= 2);
  STATS->structured.total_sent_num_color[color] ++;
  STATS->structured.total_sent_bytes_color[color] += len;
  STATS->structured.binlog_data_sent ++;
  return rpc_send_query (T, c);
}

int rpc_execute_handshake (struct connection *c, struct rpc_handshake *P, int len) {
  vkprintf (2, "rpc_execute_handshake: remote_id = %lld, len = %d\n", P->local_id, len);
  if (len != sizeof (struct rpc_handshake)) {
    return 0;
  }
  struct node node;
  node.host = c->remote_ip;
  node.port = P->port;
  node.id = P->local_id;
  add_parent (node, c);
  assert (update_relatives_binlog_position (P->local_id, P->binlog_position) >= 1);
  c->last_response_time = precise_now;
  STATS->structured.handshake_received ++;
  return rpc_send_handshake_accept (c, P->local_id);
}

int rpc_execute_handshake_accept (struct connection *c, struct rpc_handshake_accept *P, int len) {
  vkprintf (2, "rpc_execute_handshake_accept: remote_id = %lld, len = %d\n", P->local_id, len);
  if (len != sizeof (struct rpc_handshake_accept)) {
    return 0;
  }
  STATS->structured.handshake_accept_received ++;
  if (!get_relative_by_id (P->local_id)) {
    return rpc_send_handshake_reject (c, P->local_id);
  }
  c->last_response_time = precise_now;
  assert (update_relatives_binlog_position (P->local_id, P->binlog_position) >= 1);
  return 0;
}

int rpc_execute_handshake_reject (struct connection *c, struct rpc_handshake_reject *P, int len) {
  vkprintf (2, "rpc_execute_handshake_reject: remote_id = %lld, len = %d\n", P->local_id, len);
  if (len != sizeof (struct rpc_handshake_reject)) {
    return 0;
  }
  STATS->structured.handshake_reject_received ++;
  if (!get_relative_by_id (P->local_id)) {
    struct relative *x = get_relative_by_connection (c);
    if (x) {
      delete_relative (x, 1);
    }
    return 0;
  }
  return rpc_send_handshake (c);
}

int rpc_execute_divorce (struct connection *c, struct rpc_divorce *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_divorce: len = %d\n", len);
  }
  if (len != sizeof (struct rpc_divorce)) {
    return 0;
  }
  STATS->structured.divorce_received ++;
  struct relative *x = get_relative_by_connection (c);
  if (x) {
    delete_relative (x, 0);
  }
  return 0;
}

int rpc_execute_binlog_info (struct connection *c, struct rpc_binlog_info *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_binlog_info: remote_id = %lld, len = %d\n", P->local_id, len);
  }
  if (len != sizeof (struct rpc_binlog_info)) {
    return 0;
  }
  STATS->structured.binlog_info_received ++;
  if (!get_relative_by_id (P->local_id)) {
    return rpc_send_handshake_reject (c, P->local_id);
  }
  c->last_response_time = precise_now;
  update_relatives_binlog_position (P->local_id, P->binlog_position);
  return 0;
}

int rpc_execute_binlog_request (struct connection *c, struct rpc_binlog_request *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_binlog_request: remote_id = %lld, len = %d\n", P->local_id, len);
  }
  if (len != sizeof (struct rpc_binlog_request)) {
    return 0;
  }
  STATS->structured.binlog_request_received ++;
  if (!get_relative_by_id (P->local_id)) {
    return rpc_send_handshake_reject (c, P->local_id);
  }
  if (P->binlog_position >= BINLOG_POSITION) {
    return 0;
  }
  rpc_send_binlog_data (c, P->local_id, P->binlog_position);
  return 0;
}

int set_binlog_data (const char *data, long long pos, int len);
extern double last_sent_time;
int rpc_execute_binlog_data (struct connection *c, struct rpc_binlog_data *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_binlog_data: remote_id = %lld, len = %d\n", P->local_id, len);
  }
  LAST_BINLOG_REQUEST_TIME = 0;
  int llen = (P->size & 3) == 0 ? P->size : (P->size & ~3) + 4;
  if (len != sizeof (struct rpc_binlog_data) + llen) {
    fprintf (stderr, "Invalid length, skipping\n");
    return 0;
  }
  STATS->structured.binlog_data_received ++;
  if (!get_relative_by_id (P->local_id)) {
    return rpc_send_handshake_reject (c, P->local_id);
  }
  if (P->binlog_position > BINLOG_POSITION) {
    vkprintf (1, "Too new position\n");
    return 0;
  }
  if (P->binlog_position + P->size <= BINLOG_POSITION) {
    vkprintf (1, "Too old position\n");
    return 0;
  }
  unsigned long long cur_crc64 = P->crc64;
  if (P->binlog_position < BINLOG_POSITION) {
    cur_crc64 = ~crc64_partial (P->data, BINLOG_POSITION - P->binlog_position, ~cur_crc64);
  }
  if (cur_crc64 != CRC64) {
    vkprintf (1, "crc64 mistmatch P->binlog_pos = %lld, binlog_pos = %lld, our:%lld their:%lld\n", P->binlog_position, BINLOG_POSITION, cur_crc64, CRC64);
    return 0;
  }
  int color = get_relative_by_id (P->local_id)->link_color;
  assert (0 <= color && color <= 2);
  STATS->structured.total_received_num_color[color] ++;
  STATS->structured.total_received_bytes_color[color] += P->size;
  log_event (1, LOG_BINLOG_RECEIVED, P->local_id);
  set_binlog_data (P->data + (BINLOG_POSITION - P->binlog_position), BINLOG_POSITION, P->size - (BINLOG_POSITION - P->binlog_position));
  send_friends_binlog_position ();
  return 0;
}

int choose_cluster (long long local_id) {
  int i;
  for (i = 0; i < max_cluster; i++) if (Clusters[i] && Clusters[i]->node_id == local_id) {
    CC = Clusters[i];
    return i;
  }
  return -1;
}

int choose_cluster_f (long long local_id) {
  int i;
  for (i = 0; i < max_cluster; i++) if (Clusters[i] && (Clusters[i]->cluster_id & CLUSTER_MASK) == (local_id & CLUSTER_MASK)) {
    CC = Clusters[i];
    return i;
  }
  return -1;
}


int rpcc_execute (struct connection *c, int op, int len) {
  if (verbosity > 1) {
    fprintf (stderr, "rpcc_execute: fd=%d, op=%x, len=%d\n", c->fd, op, len);
  }
  if (len > MAX_PACKET_LEN) {
    return SKIP_ALL_BYTES;    
  }

  assert (read_in (&c->In, &P, len) == len);
  assert (rpc_check_crc32 (P));
  

  if ((op != RPC_TYPE_JOIN_ACK || choose_cluster_f (((struct rpcc_op *)P)->id) < 0) && choose_cluster (((struct rpcc_op *)P)->id) < 0) {
    return 0;
  }

  vkprintf (6, "%x\n", op);

  switch (op) {
  case RPC_TYPE_JOIN_ACK:
    return rpc_execute_join_ack (c, (struct rpc_join_ack *)P, len);
  case RPC_TYPE_CHILDREN_ANSWER:
    return rpc_execute_children_answer (c, (struct rpc_children_answer *)P, len);
  case RPC_TYPE_KICKED:
    return rpc_execute_kicked (c, (struct rpc_kicked *)P, len);
  case RPC_TYPE_REQUEST_UPDATE_STATS:
    return rpc_execute_request_update_stats (c, (struct rpc_request_update_stats *)P, len);
  case RPC_TYPE_DELAYS:
    return rpc_execute_delays (c, (struct rpc_delays *)P, len);
  }
  return 0;
}

int rpcs_execute (struct connection *c, int op, int len) {
  if (verbosity > 1) {
    fprintf (stderr, "rpcs_execute: fd=%d, op=%x, len=%d\n", c->fd, op, len);
  }
  if (len > MAX_PACKET_LEN) {
    return SKIP_ALL_BYTES;    
  }

  assert (read_in (&c->In, &P, len) == len);
  assert (rpc_check_crc32 (P));

  if (choose_cluster (((struct rpcs_op *)P)->remote_id) < 0 || (((struct rpcs_op *)P)->remote_id & CLUSTER_MASK) != (NODE_ID & CLUSTER_MASK)) {
    return 0;
  }

  switch (op) {
  case RPC_TYPE_HANDSHAKE:
    return rpc_execute_handshake (c, (struct rpc_handshake *)P, len);
  case RPC_TYPE_HANDSHAKE_ACCEPT:
    return rpc_execute_handshake_accept (c, (struct rpc_handshake_accept *)P, len);
  case RPC_TYPE_HANDSHAKE_REJECT:
    return rpc_execute_handshake_reject (c, (struct rpc_handshake_reject *)P, len);
  case RPC_TYPE_BINLOG_INFO:
    return rpc_execute_binlog_info (c, (struct rpc_binlog_info *)P, len);
  case RPC_TYPE_BINLOG_REQUEST:
    return rpc_execute_binlog_request (c, (struct rpc_binlog_request *)P, len);
  case RPC_TYPE_BINLOG_DATA:
    return rpc_execute_binlog_data (c, (struct rpc_binlog_data *)P, len);
  //case RPC_TYPE_DIVORCE:
  //  return rpc_execute_divorce (c, (struct rpc_divorce *)P, len);
  }

  return 0;
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  return -2;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  return -2;
}

int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg) {
  return -2;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  return -2;
}

int memcache_prepare_stats (struct connection *c) {
  int uptime = now - start_time;
  dyn_update_stats();

  stats_buff_len = snprintf (stats_buff, STATS_BUFF_SIZE,
      "heap_allocated\t%ld\n"
      "heap_max\t%ld\n"
      "wasted_heap_blocks\t%d\n"
      "wasted_heap_bytes\t%ld\n"
      "free_heap_blocks\t%d\n"
      "free_heap_bytes\t%ld\n"
      "uptime\t%d\n"
      "host\t%u\n"
      "active_connections\t%d\n"
      "active_outbound_connections\t%d\n"
      "nb_buffers_used\t%d\n"
      FULL_VERSION,
      //"version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n",
      (long) (dyn_cur - dyn_first),
      (long) (dyn_last - dyn_first),
      wasted_blocks,
      wasted_bytes,
      freed_blocks,
      freed_bytes,
      uptime,
      host,
      active_connections,
      active_outbound_connections,
      NB_used

      );
  return stats_buff_len;
}

int memcache_stats (struct connection *c) {
  int len = memcache_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

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
  exit(EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  exit(EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf(stderr, "got SIGHUP.\n");
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf(stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs ();
  signal(SIGUSR1, sigusr1_handler);
}

#define JOIN_REQUEST_TIMEOUT 2
#define DEFAULT_STATS_TIMEOUT 30

int binlog_flush (int force);

void cluster_cron (void) {
  if (((NODE_ID & ID_MASK) && LAST_CHILDREN_GET_TIME + CHILDREN_RENEW_TIME < precise_now) || 
    (!(NODE_ID & ID_MASK) && LAST_CHILDREN_GET_TIME + JOIN_RENEW_TIME < precise_now)) {
    struct connection *c = get_target_connection (main_targ);
    if (c) {
      LAST_CHILDREN_GET_TIME = precise_now;
      rpc_send_join (c);
    }
  }
  if ((NODE_ID & ID_MASK) && LAST_STATS_TIME + STATS_RENEW_TIME < precise_now) {
    struct connection *c = get_target_connection (main_targ);
    if (c) {
      LAST_STATS_TIME = precise_now;
      rpc_send_stats (c);
    }
  }
  send_friends_binlog_position ();
  binlog_flush (0);
}

void cron (void) {
  int i;
  for (i = 0; i < max_cluster; i++) if (Clusters[i]) {
    CC = Clusters[i];
    cluster_cron ();
  }
}

int sfd;

int get_binlog_data (char *data, long long pos, int len) {
  assert (len > 0);
  assert (pos + len <= BINLOG_POSITION);
  if (verbosity >= 4) {
    fprintf (stderr, "get_binlog_data: pos = %lld, len = %d\n", pos, len);
  }
  if (BINLOG_POSITION - pos + BINLOG_BUFFER <= BINLOG_BUFFER_WPTR) {
    if (verbosity >= 4) {
      fprintf (stderr, "copying from buffer\n");
    }
    memcpy (data, BINLOG_BUFFER_WPTR - (BINLOG_POSITION - pos), len);
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "reading from file\n");
    }

    STATS->structured.disk_read_time -= get_double_time_since_epoch ();
    assert (lseek (BINLOG_BUFFER_FD, pos, SEEK_SET) == pos);
    assert (read (BINLOG_BUFFER_FD, data, len) == len);
    STATS->structured.disk_read_time += get_double_time_since_epoch ();
  }
  if (verbosity >= 4) {
    fprintf (stderr, "copied data successfully\n");
  }
  return 0;
}

int binlog_flush (int force) {
  if (BINLOG_BUFFER_WPTR != BINLOG_BUFFER_FPTR) {
    assert (BINLOG_BUFFER_WPTR > BINLOG_BUFFER_FPTR);
    STATS->structured.disk_write_time -= get_double_time_since_epoch ();
    assert (lseek (BINLOG_BUFFER_FD, BINLOG_POSITION - (BINLOG_BUFFER_WPTR - BINLOG_BUFFER_FPTR), SEEK_SET) == BINLOG_POSITION - (BINLOG_BUFFER_WPTR - BINLOG_BUFFER_FPTR));
    assert (write (BINLOG_BUFFER_FD, BINLOG_BUFFER_FPTR, BINLOG_BUFFER_WPTR - BINLOG_BUFFER_FPTR) == BINLOG_BUFFER_WPTR - BINLOG_BUFFER_FPTR);
    assert (fsync (BINLOG_BUFFER_FD) >= 0);
    STATS->structured.disk_write_time += get_double_time_since_epoch ();
    BINLOG_BUFFER_FPTR = BINLOG_BUFFER_WPTR;
  }
  if (BINLOG_BUFFER_WPTR > BINLOG_BUFFER + BINLOG_BUFFER_SIZE) {
    memcpy (BINLOG_BUFFER, BINLOG_BUFFER_WPTR - BINLOG_BUFFER_SIZE, BINLOG_BUFFER_SIZE);
    BINLOG_BUFFER_WPTR = BINLOG_BUFFER + BINLOG_BUFFER_SIZE;
    BINLOG_BUFFER_FPTR = BINLOG_BUFFER_WPTR;
  }
  return 0;
}

void on_last_size (void) {
  if (BINLOG_POSITION > LAST_SIZE) {
    fprintf (stderr, "Expected size %lld, found %lld\n", LAST_SIZE, BINLOG_POSITION);
    return;
  }
  static char buf[1024];
  int s = BINLOG_NAME_LEN;
  memcpy (buf, BINLOG_NAME, s - 7);
  buf[s - 7] = 0;

  int r = link (BINLOG_NAME, buf);
  if (r < 0) {
    if (errno == EEXIST) {
      r = unlink (buf);
      if (r < 0) {
        fprintf (stderr, "Can not delete previous link: %m\n");
        assert (r == 0);
      }
      r = link (BINLOG_NAME, buf);
      if (r < 0) {
        fprintf (stderr, "Can not create link: %m\n");
        assert (r == 0);
      }
    } else {
      fprintf (stderr, "Can not create link: %m\n");
      assert (r == 0);
    }
  }
  assert (BINLOG_POSITION == LAST_SIZE);
}

int set_binlog_data (const char *data, long long pos, int len) {
  if (verbosity >= 4) {
    fprintf (stderr, "set_binlog_data: pos = %lld, len = %d\n", pos, len);
  }
  //long long old_binlog_position = BINLOG_POSITION;
  //restart_friends_timers ();
  generate_delays ();
  assert (pos == BINLOG_POSITION);
  assert (len <= BINLOG_BUFFER_SIZE);
  assert (pos + len < MAX_BINLOG_SIZE);
  if (len + BINLOG_BUFFER_WPTR > BINLOG_BUFFER_END) {
    binlog_flush (1);
  }  
  assert (len + BINLOG_BUFFER_WPTR <= BINLOG_BUFFER_END);
  memcpy (BINLOG_BUFFER_WPTR, data, len);
  BINLOG_POSITION += len;

  BINLOG_BUFFER_WPTR += len;

  long long x = (pos >> CRC64_ARRAY_STEP_LOG) + 1;
  while ((x << CRC64_ARRAY_STEP_LOG) <= pos + len) {
    CRC64_ARRAY[x] = ~crc64_partial (data, (x << CRC64_ARRAY_STEP_LOG) - pos, ~CRC64); 
    x++;
  }


  CRC64 = ~crc64_partial (data, len, ~CRC64);
  STATS->structured.last_binlog_update = get_double_time_since_epoch ();
  log_event (1, LOG_BINLOG_UPDATED, BINLOG_POSITION);

  if (BINLOG_POSITION >= LAST_SIZE) {
    on_last_size ();
  }
  binlog_flush (0);
  return 0;
}


double get_double_time_since_epoch() {
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec + 1e-6 * tv.tv_usec;
}

void update_binlog_buffer (long long old_binlog_position) {
  assert (BINLOG_POSITION > old_binlog_position);
  assert (BINLOG_BUFFER_WPTR == BINLOG_BUFFER_FPTR);
  long long size = BINLOG_POSITION - old_binlog_position;
  if (BINLOG_POSITION - old_binlog_position > BINLOG_BUFFER_SIZE) {
    STATS->structured.disk_read_time -= get_double_time_since_epoch ();
    assert (lseek (BINLOG_BUFFER_FD, BINLOG_POSITION - BINLOG_BUFFER_SIZE, SEEK_SET) == BINLOG_POSITION - BINLOG_BUFFER_SIZE);
    assert (read (BINLOG_BUFFER_FD, BINLOG_BUFFER, BINLOG_BUFFER_SIZE) == BINLOG_BUFFER_SIZE);
    STATS->structured.disk_read_time += get_double_time_since_epoch ();
    if (verbosity >= 4) {
      fprintf (stderr, "read %lld bytes of replicating file\n", (long long)BINLOG_BUFFER_SIZE);
    }
    BINLOG_BUFFER_WPTR = BINLOG_BUFFER + BINLOG_BUFFER_SIZE;
    BINLOG_BUFFER_FPTR = BINLOG_BUFFER_WPTR;
  } else {
    if (BINLOG_BUFFER_WPTR + size > BINLOG_BUFFER_END) {
      memcpy (BINLOG_BUFFER, BINLOG_BUFFER_WPTR - (BINLOG_BUFFER_SIZE - size), BINLOG_BUFFER_SIZE - size);
      STATS->structured.disk_read_time -= get_double_time_since_epoch ();
      BINLOG_BUFFER_WPTR = BINLOG_BUFFER + (BINLOG_BUFFER_SIZE - size);
      BINLOG_BUFFER_FPTR = BINLOG_BUFFER + (BINLOG_BUFFER_SIZE - size);
      STATS->structured.disk_read_time += get_double_time_since_epoch ();
    }
    assert (lseek (BINLOG_BUFFER_FD, BINLOG_POSITION - size, SEEK_SET) == BINLOG_POSITION - size);
    assert (read (BINLOG_BUFFER_FD, BINLOG_BUFFER_WPTR, size) == size);
    BINLOG_BUFFER_WPTR += size;
    BINLOG_BUFFER_FPTR += size;
    if (verbosity >= 4) {
      fprintf (stderr, "read %lld bytes of replicating file\n", size);
    }
  }
}

void update_crc64 (long long old_binlog_position) {
  assert (BINLOG_POSITION > old_binlog_position);
  while (old_binlog_position < BINLOG_POSITION) {
    long long len = BINLOG_POSITION - old_binlog_position;
    if (len > STATS_BUFF_SIZE) {
      len = STATS_BUFF_SIZE;
    }
    STATS->structured.disk_read_time -= get_double_time_since_epoch ();
    assert (lseek (BINLOG_BUFFER_FD, old_binlog_position, SEEK_SET) == old_binlog_position);
    assert (read (BINLOG_BUFFER_FD, stats_buff, len) == len);
    STATS->structured.disk_read_time += get_double_time_since_epoch ();
    long long x = (old_binlog_position >> CRC64_ARRAY_STEP_LOG) + 1;
    while ((x << CRC64_ARRAY_STEP_LOG) <= old_binlog_position + len) {
      CRC64_ARRAY[x] = ~crc64_partial (stats_buff, (x << CRC64_ARRAY_STEP_LOG) - old_binlog_position, ~CRC64); 
      x++;
    }
    old_binlog_position += len;
    CRC64 = ~crc64_partial (stats_buff, len, ~CRC64);
  }
}

unsigned long long get_crc64 (long long pos) {
  assert (pos <= BINLOG_POSITION);
  long long x = (pos >> CRC64_ARRAY_STEP_LOG) << CRC64_ARRAY_STEP_LOG;
  if (x >= BINLOG_POSITION - (BINLOG_BUFFER_WPTR - BINLOG_BUFFER)) {
    return ~crc64_partial (BINLOG_BUFFER_WPTR - (BINLOG_POSITION - x), pos - x, ~CRC64_ARRAY[x >> CRC64_ARRAY_STEP_LOG]);
  } else {
    assert (pos - x <= STATS_BUFF_SIZE);
    STATS->structured.disk_read_time -= get_double_time_since_epoch ();
    assert (lseek (BINLOG_BUFFER_FD, x, SEEK_SET) == x);
    assert (read (BINLOG_BUFFER_FD, stats_buff, pos - x) == pos - x);
    STATS->structured.disk_read_time += get_double_time_since_epoch (); 
    return ~crc64_partial (stats_buff, pos - x, ~CRC64_ARRAY[x >> CRC64_ARRAY_STEP_LOG]);
  }
}

void update_binlog_position (void) {
  struct stat t;
  fstat (BINLOG_BUFFER_FD, &t);
  long long old_binlog_position = BINLOG_POSITION;
  BINLOG_POSITION = t.st_size;
  assert (BINLOG_POSITION < MAX_BINLOG_SIZE);
  if (BINLOG_POSITION != old_binlog_position) {
    log_event (1, LOG_BINLOG_UPDATED, BINLOG_POSITION);
    update_crc64 (old_binlog_position);
    update_binlog_buffer (old_binlog_position);
    STATS->structured.last_binlog_update = get_double_time_since_epoch ();
    send_friends_binlog_position ();
  }
}


void open_binlog_file (void) {
  CC->fd = open (BINLOG_NAME, O_RDWR | O_CREAT, 0600);
  if (CC->fd < 0) {
    fprintf (stderr, "can not open file for replication (error %m)\n");
    fprintf (stderr, "file %s\n", BINLOG_NAME);
    exit (1);
  }
}

void open_all_binlogs (void) {
  int i;
  for (i = 0; i < max_cluster; i++) if (Clusters[i]) {
    CC = Clusters[i];
    open_binlog_file ();
  }
}

void update_all_positions (void) {
  int i;
  for (i = 0; i < max_cluster; i++) if (Clusters[i]) {
    CC = Clusters[i];
    update_binlog_position ();
  }
}

#define MAX_LINE_LEN 1000

int is_whitespace (char c) {
  return (c > 0) && (c <= 32);
}

int is_not_whitespace (char c) {
  return (c > 32);
}

char *eat_whitespace (char *s) {
  while (is_whitespace (*s)) {
    s++;
  }
  return s;
}

char *eat_not_whitespace (char *s) {
  while (is_not_whitespace (*s)) {
    s++;
  }
  return s;
}

void add_cluster (const char * prefix, int prefix_len, const char *arg, int f);
void delete_cluster (int i);
void check_manifest_line (int p) {
  static char c[MAX_LINE_LEN + 1];
  int l = p - START_MANIFEST_POSITION;
  if (l <= MAX_LINE_LEN && l > 4) {
    get_binlog_data (c, START_MANIFEST_POSITION, l);
    c[l] = 0;
    char *pp = c;
    int op = -1;
    if (l >= 6 && !memcmp (c, "start", 5)) {
      op = 1; 
      pp += 5;
    } else if (l >= 5 && !memcmp (c, "stop", 4)) {
      op = 2;
      pp += 4;
    } else if (l >= 8 && !memcmp (c, "version", 7)) {
      op = 3;
      pp += 7;
    }
    if (is_whitespace (*pp) && op > 0) {      
      pp ++;
      pp = eat_whitespace (pp);
      if (!*pp) {
        START_MANIFEST_POSITION = p + 1;
        return;
      }
      if (op == 1 || op == 2) {
        char *rr = pp;
        pp = eat_not_whitespace (pp);
        char *zz = pp;
        pp = eat_whitespace (pp);
        *zz = 0;
        if (pp == c + l && zz - rr > 0) {
          struct cluster *C = CC;
          int x = BINLOG_NAME_LEN - 1;
          while (x >= 0 && BINLOG_NAME[x] != '/') {
            x--;
          }
          add_cluster (BINLOG_NAME, x + 1, rr, (MAIN_REPLICA ? 2 : 0) + (op == 2 ? 4 : 0) + (1 << 30));
          CC = C;
        }
      } else {
        assert (op == 3);
        char *rr = pp;
        pp = eat_not_whitespace (pp);
        char *rr_end = pp;
        pp = eat_whitespace (pp);
        *rr_end = 0;
        if (!*pp) {
          START_MANIFEST_POSITION = p + 1;
          return;
        }
        int version = atoi (pp);
        pp = eat_not_whitespace (pp);
        pp = eat_whitespace (pp);
        if (!*pp) {
          START_MANIFEST_POSITION = p + 1;
          return;
        }
        long long size = atoll (pp);
        pp = eat_not_whitespace (pp);
        pp = eat_whitespace (pp);
        if (pp == c + l && rr_end > rr && version > 0 && size >= 0) {
          struct cluster *C = CC;
          int x = BINLOG_NAME_LEN - 1;
          while (x >= 0 && BINLOG_NAME[x] != '/') {
            x --;
          }
          int rrlen = rr_end - rr;
          int i;
          for (i = 0; i < max_cluster; i++) if (Clusters[i]) {
            //fprintf (stderr, "i = %d, binlog_name = %s, rr = %s, rrlen = %d\n", i, Clusters[i]->binlog_name, rr, rrlen);
            const char *s = Clusters[i]->binlog_name;
            int l = strlen (Clusters[i]->binlog_name) - 1;
            while (l >= 0 && s[l] != '/') { l --; }
            s = s + l + 1;
            if (!memcmp (s, rr, rrlen) && s[rrlen] == '.') {
              int old = atoi (s + rrlen + 1);
              if (old >= version) {
                vkprintf (0, "New version %d, old %d. Skipping\n", version, old);
                START_MANIFEST_POSITION = p + 1;
                return;
              }
              if (unlink (Clusters[i]->binlog_name) < 0) {
                fprintf (stderr, "Can not delete old file %s: %m\n", Clusters[i]->binlog_name);
              }
              delete_cluster (i);
            }
          }
          static char name[1024];
          memcpy (name, rr, rrlen);
          name[rrlen] = '.';
          sprintf (name + rrlen + 1, "%06d", version);
          add_cluster (BINLOG_NAME, x + 1, name, (MAIN_REPLICA ? 2 : 0) + (1 << 30));
          LAST_SIZE = size;
          CC = C;
        }
      }
    }
  }
  START_MANIFEST_POSITION = p + 1;
}

void check_manifest_updates (void) {
  assert (BINLOG_POSITION >= LAST_MANIFEST_POSITION);
  static char c[MAX_LINE_LEN + 1];
  while (LAST_MANIFEST_POSITION < BINLOG_POSITION) {
    int l = BINLOG_POSITION - LAST_MANIFEST_POSITION;
    if (l > MAX_LINE_LEN) {
      l = MAX_LINE_LEN;
    }
    get_binlog_data (c, LAST_MANIFEST_POSITION, l);
    int i;
    for (i = 0; i < l; i ++) if (c[i] == 10 || c[i] == 13) {
      check_manifest_line (LAST_MANIFEST_POSITION + i);
    }
    LAST_MANIFEST_POSITION += l;
  }
}

void cluster_precise_cron (void) {
  if (MAIN_REPLICA) {
    update_binlog_position ();
  } else {
    request_binlog ();
  }
  if (MANIFEST) {
    check_manifest_updates ();
  }
  delete_dead_connections ();
}

void precise_cron (void) {
  int i;
  for (i = 0; i < max_cluster; i++) if (Clusters[i]) {
    CC = Clusters[i];
    cluster_precise_cron ();
  }
}

struct cluster *alloc_new_cluster (void) {
  vkprintf (2, "Allocating new cluster\n");
  struct cluster *C = zmalloc0 (sizeof (struct cluster));
  C->binlog_buffer = malloc (BINLOG_BUFFER_SIZE * 2);
  assert (C->binlog_buffer);
  C->binlog_buffer_end = C->binlog_buffer + BINLOG_BUFFER_SIZE * 2;
  C->wptr = C->binlog_buffer;
  C->fptr = C->binlog_buffer;
  C->crc64_array = malloc ((MAX_BINLOG_SIZE >> CRC64_ARRAY_STEP_LOG) * 8);
  assert (C->crc64_array);
  return C;
}

void delete_cluster (int i) {
  vkprintf (2, "Deleting cluster\n");
  struct cluster *C = Clusters[i];
  C->ref_cnt --;
  assert (C->ref_cnt >= 0);
  if (!Clusters[i]->ref_cnt) {
    free (C->binlog_buffer);
    free (C->crc64_array);
    if (C->flags & 2) {
      zfree ((void *)C->cluster_name, C->cluster_name_len);
      zfree ((void *)C->binlog_name, C->binlog_name_len + 1);
    }
    close (C->fd);
    zfree (C, sizeof (struct cluster));
    Clusters[i] = 0;
  }
}

void add_cluster (const char * prefix, int prefix_len, const char *arg, int f) {
  vkprintf (2, "Adding new cluster\n");
  assert (f || !prefix);
  assert (arg);
  int i;
  for (i = 0; i < MAX_CLUSTERS; i++) if (!Clusters[i]) {
    break;
  }
  if (i == MAX_CLUSTERS) {
    fprintf (stderr, "Too many clusters\n");
    assert (0);
  }
  if (max_cluster <= i) {
    max_cluster = i + 1;
  }
  int ii = i;
  Clusters[i] = alloc_new_cluster ();
  CC = Clusters[i];
  if (!f) {
    if (*arg == '+') {
      MAIN_REPLICA = 1;
      arg ++;
    } else if (*arg == '-') {
      MAIN_REPLICA = 0;
      arg ++;
    }
    if (*arg == '@') {
      CC->flags |= 1;
      arg ++;
    }
  } else {
    MAIN_REPLICA = ((f & 2) != 0);
  }
  int l = strlen (arg);
  for (i = 0; i < l; i++) if (arg[i] == ':') {
    break;
  }
  if (i != l) {
    CLUSTER_NAME_LEN = i;
    CLUSTER_NAME = arg;
    BINLOG_NAME = arg + i + 1;
    BINLOG_NAME_LEN = strlen (BINLOG_NAME);
    vkprintf (1, "Cluster_name_len = %d, binlog_name_len = %d\n", CLUSTER_NAME_LEN, BINLOG_NAME_LEN);
  } else {
    const char *ptr = arg + strlen (arg);
    while (ptr >= arg && *ptr != '/') {
      ptr --;
    }
    ptr ++;
    CLUSTER_NAME = ptr;
    CLUSTER_NAME_LEN = strlen (ptr);
    BINLOG_NAME = arg;
    BINLOG_NAME_LEN = strlen (arg);
    vkprintf (1, "Cluster_name_len = %d, binlog_name_len = %d\n", CLUSTER_NAME_LEN, BINLOG_NAME_LEN);
  }
  if (f) {
    CC->flags |= 2;
    char *t = zmalloc (CLUSTER_NAME_LEN);
    memcpy (t, CLUSTER_NAME, CLUSTER_NAME_LEN);
    CLUSTER_NAME = t;
    t = zmalloc (BINLOG_NAME_LEN + prefix_len + 1);
    memcpy (t, prefix, prefix_len);
    memcpy (t + prefix_len, BINLOG_NAME, BINLOG_NAME_LEN + 1);
    BINLOG_NAME = t;
    BINLOG_NAME_LEN += prefix_len;
  }
  CLUSTER_ID  = ((unsigned long long)compute_crc32 (CLUSTER_NAME, CLUSTER_NAME_LEN)) << 32;
  REF_CNT ++;
  int j;
  for (j = 0; j < MAX_CLUSTERS; j++) if (ii != j && Clusters[j] && Clusters[j]->cluster_id == CLUSTER_ID) {
    assert (Clusters[j]->binlog_name_len == BINLOG_NAME_LEN);
    assert (!memcmp (Clusters[j]->binlog_name, BINLOG_NAME, BINLOG_NAME_LEN));
    if (!(f & 4)) {
      vkprintf (0, "Warning: duplicate replicating file\n");
      delete_cluster (ii);
      CC = Clusters[j];
      REF_CNT ++;
    } else {
      delete_cluster (ii);
      delete_cluster (j);
    }
    return;
  }
  if (f & 4) {
    delete_cluster (ii);
    return;
  }

  RELATIVES.next = &RELATIVES;
  RELATIVES.prev = &RELATIVES;
  RELATIVES.type = -1;

  REQUEST_DELAY[0] = DEFAULT_SLOW_REQUEST_DELAY;
  REQUEST_DELAY[1] = DEFAULT_MEDIUM_REQUEST_DELAY;
  REQUEST_DELAY[2] = DEFAULT_FAST_REQUEST_DELAY;
  
  STATS_RENEW_TIME = (drand48 () + 1) / 1.5 * DEFAULT_STATS_TIMEOUT;

  UPDATE_STATS = malloc (sizeof (struct long_update_stats));
  assert (UPDATE_STATS);

  CHILDREN_RENEW_TIME = DEFAULT_CHILDREN_RENEW_TIME;
  STATS_RENEW_TIME = DEFAULT_STATS_TIMEOUT;

  LAST_SIZE = 0x7fffffffffffffffll;
  if (f) {
    open_binlog_file ();
    update_binlog_position ();
  }
}

void start_server (void) { 
  //  struct sigaction sa;
  char buf[64];
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
    exit(3);
  }

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr(settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user(username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  open_all_binlogs (); 
  update_all_positions ();

  init_listening_connection (sfd, &ct_rpc_server, &copyfast_rpc_server);
 
  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigterm_handler);
  signal(SIGUSR1, sigusr1_handler);
  if (daemonize) {
    signal(SIGHUP, sighup_handler);
  }
  signal(SIGPIPE, SIG_IGN);

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (53);
    if (verbosity >= 10) {
      fprintf (stderr, "%d\n", __LINE__);
    }
    create_all_outbound_connections ();

    precise_cron ();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close(sfd);
}

/*
 *
 *    MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-N <network-description-file>] [-a server-host] [-P server-port] [-p port] [-c max_connections] [-l <log-name>] [-u <user-name>] <binlog_1> ... <binlog_N>\n"
    "\tReplica for copy-test-to-all\n"
    "\t-v\toutput statistical and debug information into stderr\n"
    "\t-N\tuse network description file. If absent all connections are supposed to be slow\n"
    "\t-a\tcopyfast-server host\n"
    "\t-P\tcopyfast-server port\n"
    "\t-c\tlimit for maximal number of connections\n"
    "\t<binlog>\tin format [+/-][<cluster-name>:]<file-name>. Use `+' for master mode and `-' otherwise. Default is `-'\n"
    FULL_VERSION,
    progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;

  set_debug_handlers ();

  progname = argv[0];

  char encr_file[256];
  char network_desc_file[256];
  int custom_encr = 0;


  while ((i = getopt (argc, argv, "vdc:a:p:E:P:N:u:l:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'c':
      maxconn = atoi(optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
        maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'a':
      strncpy (hostname, optarg, 255);
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'P':
      server_port = atoi(optarg);
      break;
    case 'E':
      strncpy (encr_file, optarg, 255);
      custom_encr = 1;
      break;
    case 'N':
      strncpy (network_desc_file, optarg, 255);
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'h':
      usage ();
      return 2;
    case 'd':
      daemonize ^= 1;
    }
  }
  
  PID.port = port;

  if (argc <= optind) {
    usage();
    return 2;
  }




  if (raise_file_rlimit(maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit(1);
  }

  if (!custom_encr) {
    aes_load_pwd_file (0);
  } else {
    aes_load_pwd_file (encr_file);
  }

  init_dyn_data();

  for (i = optind; i < argc; i++) {
    add_cluster (0, 0, argv[i], 0);
  }

  read_network_file (network_desc_file);

  start_time = time(0);
  
  struct hostent *h;
  if (!(h = gethostbyname (hostname)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    fprintf (stderr, "cannot resolve %s\n", hostname);
    exit (2);
  }
  default_ct.target = *((struct in_addr *) h->h_addr);
  default_ct.port = server_port;

  main_targ = create_target (&default_ct, 0);

  start_server();

  return 0;
}
