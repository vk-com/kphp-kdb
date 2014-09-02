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
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <signal.h>
#include <poll.h>
#include <netdb.h>

#include "crc32.h"
#include "kdb-data-common.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "net-crypto-aes.h"
#include "net-http-server.h"
#include "resolver.h"

#include "md5.h"

#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "net-rpc-common.h"
#include "rpc-const.h"

#include "net-mysql-client.h"

#include "php-engine-vars.h"
#include "php-master.h"

#include "php-runner.h"
#include "php-queries.h"

/** drinkless headers **/
#include "dl-utils-lite.h"

static void turn_sigterm_on (void);

#define MAX_TIMEOUT 30

#define RPC_DEFAULT_PING_INTERVAL 30.0
double rpc_ping_interval = RPC_DEFAULT_PING_INTERVAL;
#define RPC_STOP_INTERVAL (2 * rpc_ping_interval)
#define RPC_FAIL_INTERVAL (20 * rpc_ping_interval)

#define RPC_CONNECT_TIMEOUT 3

/***
  DB-client
 ***/
#define RESPONSE_FAIL_TIMEOUT 30.0

int proxy_client_execute (struct connection *c, int op);

int sqlp_authorized (struct connection *c);
int sqlp_becomes_ready (struct connection *c);
int sqlp_check_ready (struct connection *c);

struct mysql_client_functions db_client_outbound = {
  .execute = proxy_client_execute,
  .sql_authorized = sqlp_authorized,
  .sql_becomes_ready = sqlp_becomes_ready,
  .check_ready = sqlp_check_ready,
  .sql_flush_packet = sqlc_flush_packet,
  .sql_check_perm = sqlc_default_check_perm,
  .sql_init_crypto = sqlc_init_crypto,
};



char *sql_username = "boxed";
char *sql_password = "password";
char *sql_database = "boxed_base";

long long cur_request_id = -1;
/***
  MC-client
 ***/

int memcache_client_execute (struct connection *c, int op);
int memcache_client_check_ready (struct connection *c);
int memcache_connected (struct connection *c);

struct memcache_client_functions memcache_client_outbound = {
  .execute = memcache_client_execute,
  .check_ready = memcache_client_check_ready,
  .connected = memcache_connected,

  .flush_query = mcc_flush_query,
  .mc_check_perm = mcc_default_check_perm,
  .mc_init_crypto = mcc_init_crypto,
  .mc_start_crypto = mcc_start_crypto
};

/***
  RPC-client
 ***/
int rpcc_execute (struct connection *c, int op, int len);
int rpcc_send_query (struct connection *c);
int rpcc_check_ready (struct connection *c);
void prepare_rpc_query (struct connection *c, int *q, int qn);
void send_rpc_query (struct connection *c, int op, long long id, int *q, int qn);

struct rpc_client_functions rpc_client_outbound = {
  .execute = rpcc_execute, //replaced
  .check_ready = rpcc_check_ready, //replaced
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpcc_send_query //replaced
};


/***
  OUTBOUND CONNECTIONS
 ***/

struct conn_target default_ct = {
  .min_connections = 2,
  .max_connections = 3,
  .type = &ct_memcache_client,
  .extra = &memcache_client_outbound,
  .reconnect_timeout = 1
};

struct conn_target db_ct = {
  .min_connections = 2,
  .max_connections = 3,
  .type = &ct_mysql_client,
  .extra = &db_client_outbound,
  .reconnect_timeout = 1,
  .port = 3306
};

struct conn_target rpc_ct = {
  .min_connections = 2,
  .max_connections = 3,
  .type = &ct_rpc_client,
  .extra = (void *)&rpc_client_outbound,
  .port = 11210,
  .reconnect_timeout = 1
};


struct in_addr settings_addr;

struct connection *get_target_connection (struct conn_target *S, int force_flag) {
  struct connection *c, *d = NULL;
  int u = 10000;
  if (!S) {
    return NULL;
  }
  for (c = S->first_conn; c != (struct connection *)S; c = c->next) {
    int r = S->type->check_ready (c);
    if (r == cr_ok || (force_flag && r == cr_notyet)) {
      return c;
    } else if (r == cr_stopped && c->unreliability < u) {
      u = c->unreliability;
      d = c;
    }
  }
  return d;
}

struct connection *get_target_connection_force (struct conn_target *S) {
  struct connection *res = get_target_connection (S, 0);

  if (res == NULL) {
    create_new_connections (S);
    res = get_target_connection (S, 1);
  }

  return res;
}

int get_target_impl (struct conn_target *ct) {
  //TODO: fix ref_cnt overflow
  struct conn_target *res = create_target (ct, NULL);
  int res_id = (int)(res - Targets);
  return res_id;
}

int get_target_by_pid (int ip, int port, struct conn_target *ct) {
  ct->target.s_addr = htonl (ip);
  ct->port = port;

  return get_target_impl (ct);
}

int cur_lease_target_ip = -1;
int cur_lease_target_port = -1;
struct conn_target *cur_lease_target_ct = NULL;
int cur_lease_target = -1;

int get_lease_target_by_pid (int ip, int port, struct conn_target *ct) {
  if (ip == cur_lease_target_ip && port == cur_lease_target_port && ct == cur_lease_target_ct) {
    return cur_lease_target;
  }
  if (cur_lease_target != -1) {
    struct conn_target *old_target = &Targets[cur_lease_target];
    destroy_target (old_target);
  }
  cur_lease_target_ip = ip;
  cur_lease_target_port = port;
  cur_lease_target_ct = ct;
  cur_lease_target = get_target_by_pid (ip, port, ct);

  return cur_lease_target;
}

int get_target (const char *host, int port, struct conn_target *ct) {
  if (!(0 <= port && port < 0x10000)) {
    vkprintf (0, "bad port %d\n", port);
    return -1;
  }

  struct hostent *h;
  if (!(h = kdb_gethostbyname (host)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    vkprintf (0, "can't resolve host\n");
    return -1;
  }

  ct->target = *(struct in_addr *) h->h_addr;
  ct->port = port;

  return get_target_impl (ct);
}

double fix_timeout (double timeout) {
  if (timeout < 0) {
    return 0;
  }
  if (timeout > MAX_TIMEOUT) {
    return MAX_TIMEOUT;
  }
  return timeout;
}

/***
  HTTP INTERFACE
 ***/
void http_return (struct connection *c, const char *str, int len);

int delete_pending_query (struct conn_query *q) {
  vkprintf (1, "delete_pending_query(%p,%p)\n", q, q->requester);

  delete_conn_query (q);
  zfree (q, sizeof (*q));
  return 0;
}

struct conn_query_functions pending_cq_func = {
  .magic = (int)CQUERY_FUNC_MAGIC,
  .title = "pending-query",
  .wakeup = delete_pending_query,
  .close = delete_pending_query,
  .complete = delete_pending_query
};

/** delayed httq queries queue **/
struct connection pending_http_queue;
int php_worker_run_flag;

/** dummy request queue **/
struct connection dummy_request_queue;

/** net write command **/
/** do write delayed write to connection **/


typedef struct {
  command_t base;

  unsigned char *data;
  int len;
  long long extra;
} command_net_write_t;

void command_net_write_run_sql (command_t *base_command, void *data) {
  //fprintf (stderr, "command_net_write [ptr=%p]\n", base_command);
  command_net_write_t *command = (command_net_write_t *)base_command;

  assert (command->data != NULL);
  struct connection *d = (struct connection *)data;
  assert (d->status == conn_ready);

  /*
  {
    fprintf (stderr, "packet_len = %d\n", command->len);
    int s = command->len;
    fprintf (stderr, "got prefix [len = %d]\n", s);
    unsigned char *packet = command->data;
    if (s > 5) {
      int len = (packet[0]) + (packet[1] << 8) + (packet[2] << 16);
      int packet_number = packet[3];
      int command_type = packet[4];
      fprintf (stderr, "[len = %d], [num = %d], [type = %d], [%.*s]\n", len, packet_number, command_type, s - 5, packet + 5);
    }
  }
  */

  assert (write_out (&(d)->Out, command->data, command->len) == command->len);
  SQLC_FUNC (d)->sql_flush_packet (d, command->len - 4);

  flush_connection_output (d);
  d->last_query_sent_time = precise_now;
  d->status = conn_wait_answer;
  SQLC_DATA(d)->response_state = resp_first;


  free (command->data);
  command->data = NULL;
  command->len = 0;
}

void command_net_write_run_rpc (command_t *base_command, void *data) {
  command_net_write_t *command = (command_net_write_t *)base_command;
  //fprintf (stderr, "command_net_write [ptr=%p] [len = %d]\n", base_command, command->len);

  assert (command->data != NULL);
  struct connection *d = (struct connection *)data;
  //assert (d->status == conn_ready);

  send_rpc_query (d, RPC_INVOKE_REQ, command->extra, (int *)command->data, command->len);

  flush_connection_output (d);
  d->last_query_sent_time = precise_now;

  free (command->data);
  command->data = NULL;
  command->len = 0;
}


void command_net_write_free (command_t *base_command) {
  command_net_write_t *command = (command_net_write_t *)base_command;

  if (command->data != NULL) {
    free (command->data);
    command->data = NULL;
    command->len = 0;
  }
  free (command);
}

command_t command_net_write_sql_base = {
  .run = command_net_write_run_sql,
  .free = command_net_write_free
};

command_t command_net_write_rpc_base = {
  .run = command_net_write_run_rpc,
  .free = command_net_write_free
};


command_t *create_command_net_writer (const char *data, int data_len, command_t *base, long long extra) {
  command_net_write_t *command = malloc (sizeof (command_net_write_t));
  command->base.run = base->run;
  command->base.free = base->free;

  command->data = malloc ((size_t)data_len);
  memcpy (command->data, data, (size_t)data_len);
  command->len = data_len;
  command->extra = extra;

  return (command_t *)command;
}


/** php-script **/

int rpc_main_target = -1;
int rpc_lease_target = -1;
double rpc_lease_timeout = -1;
npid_t lease_pid;

double lease_stats_start_time;
double lease_stats_time;
long long lease_stats_cnt;

typedef enum {
  lst_off,
  lst_start,
  lst_on,
  lst_finish
} lease_state_t;
lease_state_t lease_state = lst_off;
int lease_ready_flag = 0;
void lease_change_state (lease_state_t new_state) {
  if (lease_state != new_state) {
    lease_state = new_state;
    lease_ready_flag = 0;
  }
}

#define run_once_count 1
const int queries_to_recreate_script = 100;

void *php_script;
typedef enum {http_worker, rpc_worker, once_worker} php_worker_mode_t;
typedef struct {
  struct connection *conn;

  php_query_data *data;

  int paused;
  int terminate_flag;
  const char *error_message;

  double init_time;
  double start_time;
  double finish_time;

  enum {phpq_try_start, phpq_init_script, phpq_run, phpq_free_script, phpq_finish} state;
  php_worker_mode_t mode;

  long long req_id;
  int target_fd;
} php_worker;

php_worker *php_worker_create (php_worker_mode_t mode, struct connection *c, http_query_data *http_data, rpc_query_data *rpc_data, double timeout, long long req_id) {
  php_worker *worker = dl_malloc (sizeof (php_worker));

  worker->data = php_query_data_create (http_data, rpc_data);
  worker->conn = c;
  assert (c != NULL);

  worker->state = phpq_try_start;
  worker->mode = mode;

  worker->init_time = precise_now;
  worker->finish_time = precise_now + timeout;

  worker->paused = 0;
  worker->terminate_flag = 0;
  worker->error_message = "no error";

  worker->req_id = req_id;

  if (worker->conn->target) {
    worker->target_fd = worker->conn->target - Targets;
  } else {
    worker->target_fd = -1;
  }

  vkprintf (2, "create php script [req_id = %016llx]\n", req_id);

  return worker;
}

void php_worker_free (php_worker *worker) {
  if (worker == NULL) {
    return;
  }

  php_query_data_free (worker->data);
  worker->data = NULL;

  dl_free (worker, sizeof (php_worker));
}

int has_pending_scripts() {
  return php_worker_run_flag || pending_http_queue.first_query != (struct conn_query *)&pending_http_queue;
}

/** trying to start query **/
void php_worker_try_start (php_worker *worker) {

  if (worker->terminate_flag) {
    worker->state = phpq_finish;
    return;
  }

  if (php_worker_run_flag) { // put connection into pending_http_query
    vkprintf (2, "php script [req_id = %016llx] is waiting\n", worker->req_id);

    struct conn_query *pending_q = zmalloc (sizeof (struct conn_query));

    pending_q->custom_type = 0;
    pending_q->outbound = (struct connection *)&pending_http_queue;
    assert (worker->conn != NULL);
    pending_q->requester = worker->conn;

    pending_q->cq_type = &pending_cq_func;
    pending_q->timer.wakeup_time = worker->finish_time;

    insert_conn_query (pending_q);

    worker->conn->status = conn_wait_net;

    worker->paused = 1;
    return;
  }

  php_worker_run_flag = 1;
  worker->state = phpq_init_script;
}

void php_worker_init_script (php_worker *worker) {
  double timeout = worker->finish_time - precise_now - 0.01;
  if (worker->terminate_flag || timeout < 0.2) {
    worker->state = phpq_finish;
    return;
  }

  /*if (http_sfd != -1) {*/
    /*epoll_remove (http_sfd);*/
  /*}*/

  get_utime_monotonic();
  worker->start_time = precise_now;
  vkprintf (1, "START php script [req_id = %016llx]\n", worker->req_id);
  immediate_stats.is_running = 1;
  immediate_stats.is_wait_net = 0;

  //init memory allocator for queries
  qmem_init();
  qresults_clean();

  script_t *script = get_script ("#0");
  dl_assert (script != NULL, "failed to get script");
  if (php_script == NULL) {
    php_script = php_script_create ((size_t)max_memory, (size_t)(8 << 20));
  }
  php_script_init (php_script, script, worker->data);
  php_script_set_timeout (timeout);
  worker->state = phpq_run;
}

void php_worker_terminate (php_worker *worker, int flag, const char *error_message) {
  worker->terminate_flag = 1;
  worker->error_message = error_message;
  if (flag) {
    vkprintf (0, "php_worker_terminate\n");
    worker->conn = NULL;
  }
}


void php_worker_run_query_x2 (php_worker *worker, php_query_x2_t *query) {
  php_script_query_readed (php_script);

//  worker->conn->status = conn_wait_net;
//  worker->conn->pending_queries = 1;

  static php_query_x2_answer_t res;
  res.x2 = query->val * query->val;

  query->base.ans = &res;

  php_script_query_answered (php_script);
}

void php_worker_run_query_connect (php_worker *worker, php_query_connect_t *query) {
  php_script_query_readed (php_script);

  static php_query_connect_answer_t res;

  switch (query->protocol) {
    case p_memcached:
      res.connection_id = get_target (query->host, query->port, &default_ct);
      break;
    case p_sql:
      res.connection_id = sql_target_id;
      break;
    case p_rpc:
      res.connection_id = get_target (query->host, query->port, &rpc_ct);
      break;
    default:
      assert ("unknown protocol" && 0);
  }

  query->base.ans = &res;

  php_script_query_answered (php_script);
}

extern conn_query_type_t mc_cq_func;
extern conn_query_type_t sql_cq_func;

struct conn_query *create_pnet_query (struct connection *http_conn, struct connection *mc_conn, net_ansgen_t *gen, double finish_time);
void create_pnet_delayed_query (struct connection *http_conn, struct conn_target *t, net_ansgen_t *gen, double finish_time);
int pnet_query_timeout (struct conn_query *q);

void net_error (net_ansgen_t *ansgen, php_query_base_t *query, const char *err) {
  ansgen->func->error (ansgen, err);
  query->ans = ansgen->ans;
  ansgen->func->free (ansgen);
  php_script_query_answered (php_script);
}

void php_worker_run_mc_query_packet (php_worker *worker, php_net_query_packet_t *query) {
  query_stats.desc = "MC";
  query_stats.query = query->data;

  php_script_query_readed (php_script);
  mc_ansgen_t *ansgen = mc_ansgen_packet_create();
  ansgen->func->set_query_type (ansgen, query->extra_type);

  net_ansgen_t *net_ansgen = (net_ansgen_t *)ansgen;
  int connection_id = query->connection_id;

  if (connection_id < 0 || connection_id >= MAX_TARGETS) {
    net_error (net_ansgen, (php_query_base_t *)query, "Invalid connection_id (1)");
    return;
  }

  struct conn_target *target = &Targets[connection_id];

  if (target == NULL) {
    net_error (net_ansgen, (php_query_base_t *)query, "Invalid connection_id (2)");
    return;
  }

  net_ansgen->func->set_desc (net_ansgen, qmem_pstr ("[%s:%d]", inet_ntoa (target->target), target->port));

  query_stats.port = target->port;

  struct connection *conn = get_target_connection_force (target);
  if (conn == NULL) {
    net_error (net_ansgen, (php_query_base_t *)query, "Failed to establish connection [probably reconnect timeout is not expired]");
    return;
  }

  if (conn->status != conn_connecting) {
    write_out (&conn->Out, query->data, query->data_len);
    MCC_FUNC (conn)->flush_query (conn);
  } else {
    if (conn->Tmp == NULL) {
      conn->Tmp = alloc_head_buffer();
    }
    write_out (conn->Tmp, query->data, query->data_len);
  }

  double timeout = fix_timeout (query->timeout) + precise_now;
  struct conn_query *cq = create_pnet_query (worker->conn, conn, (net_ansgen_t *)ansgen, timeout);

  if (query->extra_type & PNETF_IMMEDIATE) {
    pnet_query_timeout (cq);
  } else if (worker->conn != NULL) {
    worker->conn->status = conn_wait_net;
  }

  return;
}

void php_worker_run_sql_query_packet (php_worker *worker, php_net_query_packet_t *query) {
  query_stats.desc = "SQL";

  int connection_id = query->connection_id;
  php_script_query_readed (php_script);

  sql_ansgen_t *ansgen = sql_ansgen_packet_create();

  net_ansgen_t *net_ansgen = (net_ansgen_t *)ansgen;
  if (connection_id != sql_target_id) {
    net_error (net_ansgen, (php_query_base_t *)query, "Invalid connection_id (sql connection expected)");
    return;
  }

  if (connection_id < 0 || connection_id >= MAX_TARGETS) {
    net_error (net_ansgen, (php_query_base_t *)query, "Invalid connection_id (1)");
    return;
  }

  struct conn_target *target = &Targets[connection_id];

  if (target == NULL) {
    net_error (net_ansgen, (php_query_base_t *)query, "Invalid connection_id (2)");
    return;
  }

  net_ansgen->func->set_desc (net_ansgen, qmem_pstr ("[%s:%d]", inet_ntoa (target->target), target->port));

  struct connection *conn = get_target_connection (target, 0);

  double timeout = fix_timeout (query->timeout) + precise_now;
  if (conn != NULL && conn->status == conn_ready) {
//    assert (conn->type->check_ready (conn) == cr_ok);

    write_out (&conn->Out, query->data, query->data_len);
    SQLC_FUNC (conn)->sql_flush_packet (conn, query->data_len - 4);
    flush_connection_output (conn);
    conn->last_query_sent_time = precise_now;
    conn->status = conn_wait_answer;
    SQLC_DATA(conn)->response_state = resp_first;


    ansgen->func->set_writer (ansgen, NULL);
    ansgen->func->ready (ansgen, NULL);

    create_pnet_query (worker->conn, conn, net_ansgen, timeout);
  } else {
    int new_conn_cnt = create_new_connections (target);
    if (new_conn_cnt <= 0 && get_target_connection (target, 1) == NULL) {
      net_error (net_ansgen, (php_query_base_t *)query, "Failed to establish connection [probably reconnect timeout is not expired]");
      return;
    }

    ansgen->func->set_writer (ansgen, create_command_net_writer (query->data, query->data_len, &command_net_write_sql_base, -1));
    create_pnet_delayed_query (worker->conn, target, net_ansgen, timeout);
  }

  if (worker->conn != NULL) {
    worker->conn->status = conn_wait_net;
  }
}

void php_worker_run_rpc_query_packet (php_worker *worker, php_net_query_packet_t *query) {
  query_stats.desc = "RPC SEND";
  int connection_id = query->connection_id;
  php_script_query_readed (php_script);

  //fprintf (stderr, "[len = %d]\n", query->data_len);
  net_send_ansgen_t *ansgen = net_send_ansgen_create();

  net_ansgen_t *net_ansgen = (net_ansgen_t *)ansgen;

  vkprintf (4, "php_worker_run_rpc_query_packet [connection_id = %d]\n", connection_id);
  if (connection_id < 0 || connection_id >= MAX_TARGETS) {
    net_error (net_ansgen, (php_query_base_t *)query, "Invalid connection_id (1)");
    return;
  }

  struct conn_target *target = &Targets[connection_id];
  vkprintf (4, "[target = %p]\n", target);
  if (target == NULL) {
    net_error (net_ansgen, (php_query_base_t *)query, "Invalid connection_id (2)");
    return;
  }

  net_ansgen->func->set_desc (net_ansgen, qmem_pstr ("[%s:%d]", inet_ntoa (target->target), target->port));
  query_stats.port = target->port;

  struct connection *conn = get_target_connection (target, 0);

  long long qres_id = create_qres_ans();
  assert (qres_id != -1);
  ansgen->qres_id = qres_id;
  vkprintf (4, "php_worker_run_rpc_query_packet: create_rpc_query [conn = %p] [conn_status = %d] [data_len = %d] [rpc_id = <%lld>]\n",
            conn, conn != NULL ? conn->status : -1, query->data_len, ansgen->qres_id);


  qres_t *qres = get_qres (qres_id, qr_ans);
  assert (qres != NULL);

  double timeout = fix_timeout (query->timeout) + precise_now;
  qres_set_timeout (qres, timeout);


  if (conn != NULL) {
    assert (conn->type->check_ready (conn) == cr_ok);

    vkprintf (4, "php_worker_run_rpc_query_packet: send rpc query\n");

    send_rpc_query (conn, RPC_INVOKE_REQ, qres_id, (int *)query->data, query->data_len);
    flush_connection_output (conn);
    conn->last_query_sent_time = precise_now;

    ansgen->func->set_writer (ansgen, NULL);
    ansgen->func->send_and_finish (ansgen, NULL);

    query->base.ans = net_ansgen->ans;
    net_ansgen->func->free (net_ansgen);
    php_script_query_answered (php_script);
    return;
  } else {
    int new_conn_cnt = create_new_connections (target);
    vkprintf (4, "php_worker_run_rpc_query_packet: [new_conn_cnt = %d]\n", new_conn_cnt);
    if (new_conn_cnt <= 0 && get_target_connection (target, 1) == NULL) {
      net_error (net_ansgen, (php_query_base_t *)query, "Failed to establish connection [probably reconnect timeout is not expired]");
      return;
    }

    ansgen->func->set_writer (ansgen, create_command_net_writer (query->data, query->data_len, &command_net_write_rpc_base, qres_id));
    create_pnet_delayed_query (worker->conn, target, net_ansgen, timeout);
  }

  if (worker->conn != NULL) {
    worker->conn->status = conn_wait_net;
  }
}

void php_worker_run_get_query_packet (php_worker *worker, php_net_query_packet_t *query) {
  php_script_query_readed (php_script);

  if (query->protocol == p_get) {
    query_stats.desc = "RPC GET";
  } else if (query->protocol == p_get_id) {
    query_stats.desc = "RPC GET ID";
  }
  //fprintf (stderr, "[len = %d]\n", query->data_len);
  net_get_ansgen_t *ansgen = net_get_ansgen_create();
  net_ansgen_t *net_ansgen = (net_ansgen_t *)ansgen;

  //TODO: fix this hack
  assert (query->data_len == sizeof (long long));
  long long request_id = *(long long *)query->data;

  ansgen->func->set_id (ansgen, request_id);

  double timeout = ansgen->func->try_wait (ansgen, precise_now);
  if (timeout <= 1) {
    query->base.ans = net_ansgen->ans;
    net_ansgen->func->free (net_ansgen);
    php_script_query_answered (php_script);
    return;
  }

  query_stats.timeout = timeout - precise_now;
  create_pnet_query (worker->conn, &dummy_request_queue, net_ansgen, timeout);
  cur_request_id = request_id;

  //TODO: try to remove if
  if (worker->conn != NULL) {
    worker->conn->status = conn_wait_net;
  }
}


void php_worker_run_net_query_packet (php_worker *worker, php_net_query_packet_t *query) {

  switch (query->protocol) {
    case p_memcached:
      php_worker_run_mc_query_packet (worker, query);
      break;
    case p_sql:
      php_worker_run_sql_query_packet (worker, query);
      break;
    case p_rpc:
      php_worker_run_rpc_query_packet (worker, query);
      break;
    case p_get:
    case p_get_id:
      php_worker_run_get_query_packet (worker, query);
      break;
    default:
      assert (0);
  }

}

void php_worker_run_net_query (php_worker *worker, php_query_base_t *q_base) {
  switch (q_base->type & 0xFFFF) {
    case NETQ_PACKET:
      php_worker_run_net_query_packet (worker, (php_net_query_packet_t *)q_base);
      break;
    default:
      assert ("unknown net_query type"&& 0);
  }
}

void prepare_rpc_query_raw (int packet_id, int *q, int qn) {
  assert (sizeof (int) == 4);
  q[0] = qn;
  assert ((qn & 3) == 0);
  qn >>= 2;
  assert (qn >= 5);

  q[1] = packet_id;
  q[qn - 1] = (int)compute_crc32 (q, q[0] - 4);
}

void prepare_rpc_query (struct connection *c, int *q, int qn) {
  prepare_rpc_query_raw (RPCS_DATA(c)->out_packet_num++, q, qn);
}

void send_rpc_query (struct connection *c, int op, long long id, int *q, int qn) {
  q[2] = op;
  if (id != -1) {
    *(long long *)(q + 3) = id;
  }

  prepare_rpc_query (c, q, qn);

  vkprintf (4, "send_rpc_query: [len = %d] [op = %08x] [rpc_id = <%lld>]\n", q[0], op, id);
  assert (write_out (&c->Out, q, q[0]) == q[0]);

  RPCS_FUNC(c)->flush_packet (c);
}


void php_worker_run_rpc_send_message (php_worker *worker, php_query_rpc_message *ans) {
  if (worker->mode == rpc_worker) {
    struct connection *c = worker->conn;
    int *q = (int *)ans->data;
    int qn = ans->data_len;

    vkprintf (2, "going to send %d bytes to session [%016llx:%016llx]\n", qn, ans->auth_key_id, ans->session_id);
    send_rpc_query (c, RPC_SEND_SESSION_MSG, ans->auth_key_id, q, qn);
  }
  php_script_query_readed (php_script);
  php_script_query_answered (php_script);
}

void php_worker_run_rpc_answer_query (php_worker *worker, php_query_rpc_answer *ans) {
  if (worker->mode == rpc_worker) {
    struct connection *c = worker->conn;
    int *q = (int *)ans->data;
    int qn = ans->data_len;

    vkprintf (2, "going to send %d bytes as an answer [req_id = %016llx]\n", qn, worker->req_id);
    send_rpc_query (c, q[2] == 0 ? RPC_REQ_RESULT : RPC_REQ_ERROR, worker->req_id, q, qn);
  }
  php_script_query_readed (php_script);
  php_script_query_answered (php_script);
}

void php_worker_create_queue (php_worker *worker, php_query_create_queue_t *query) {
  php_script_query_readed (php_script);

  static php_query_create_queue_answer_t res;
  long long queue_id = create_qres_watchcat (query->request_ids, query->request_ids_len);
  res.queue_id = queue_id;

  query->base.ans = &res;

  php_script_query_answered (php_script);
}

int php_worker_http_load_post_impl (php_worker *worker, char *buf, int min_len, int max_len) {
  assert (worker != NULL);

  struct connection *c = worker->conn;
  double precise_now = get_utime_monotonic();

//  fprintf (stderr, "Trying to load data of len [%d;%d] at %.6lf\n", min_len, max_len, precise_now - worker->start_time);

  if (worker->finish_time < precise_now + 0.01) {
    return -1;
  }

  if (c == NULL || c->error) {
    return -1;
  }

  assert (!c->crypto);
  assert (c->basic_type != ct_pipe);
  assert (min_len <= max_len);

  int read = 0;
  int have_bytes = get_total_ready_bytes (&c->In);
  if (have_bytes > 0) {
    if (have_bytes > max_len) {
      have_bytes = max_len;
    }
    assert (read_in (&c->In, buf, have_bytes) == have_bytes);
    read += have_bytes;
  }

  struct pollfd poll_fds;
  poll_fds.fd = c->fd;
  poll_fds.events = POLLIN | POLLPRI;

  while (read < min_len) {
    precise_now = get_utime_monotonic();

    double left_time = worker->finish_time - precise_now;
    assert (left_time < 2000000.0);

    if (left_time < 0.01) {
      return -1;
    }

    int r = poll (&poll_fds, 1, (int)(left_time * 1000 + 1));
    int err = errno;
    if (r > 0) {
      assert (r == 1);

      r = recv (c->fd, buf + read, max_len - read, 0);
      err = errno;
/*
      if (r < 0) {
        fprintf (stderr, "Error recv: %m\n");
      } else {
        fprintf (stderr, "Received %d bytes at %.6lf\n", r, precise_now - worker->start_time);
      }
*/
      if (r > 0) {
        read += r;
      } else {
        if (r == 0) {
          return -1;
        }

        if (err != EAGAIN && err != EWOULDBLOCK && err != EINTR) {
          return -1;
        }
      }
    } else {
      if (r == 0) {
        return -1;
      }

//      fprintf (stderr, "Error poll: %m\n");
      if (err != EINTR) {
        return -1;
      }
    }
  }

//  fprintf (stderr, "%d bytes loaded\n", read);
  return read;
}

void php_worker_http_load_post (php_worker *worker, php_query_http_load_post_t *query) {
  php_script_query_readed (php_script);

  static php_query_http_load_post_answer_t res;
  res.loaded_bytes = php_worker_http_load_post_impl (worker, query->buf, query->min_len, query->max_len);
  query->base.ans = &res;

  php_script_query_answered (php_script);

  if (res.loaded_bytes < 0) {
    php_worker_terminate (worker, 1, "error during loading big post data"); //TODO we need to close connection. Do we need to pass 1 as second parameter?
  }
}

void php_worker_answer_query (php_worker *worker, void *ans) {
  assert (worker != NULL && ans != NULL);
  php_query_base_t *q_base = (php_query_base_t *)php_script_get_query (php_script);
  q_base->ans = ans;
  php_script_query_answered (php_script);
}

void php_worker_run_query (php_worker *worker) {
  php_query_base_t *q_base = (php_query_base_t *)php_script_get_query (php_script);

  qmem_free_ptrs();

  query_stats.q_id = query_stats_id;
  switch ((unsigned int)q_base->type & 0xFFFF0000) {
    case PHPQ_X2:
      query_stats.desc = "PHPQX2";
      php_worker_run_query_x2 (worker, (php_query_x2_t *)q_base);
      break;
    case PHPQ_RPC_ANSWER:
      query_stats.desc = "RPC_ANSWER";
      php_worker_run_rpc_answer_query (worker, (php_query_rpc_answer *)q_base);
      break;
    case PHPQ_RPC_MESSAGE:
      query_stats.desc = "RPC_SEND_MESSAGE";
      php_worker_run_rpc_send_message (worker, (php_query_rpc_message *)q_base);
      break;
    case PHPQ_CONNECT:
      query_stats.desc = "CONNECT";
      php_worker_run_query_connect (worker, (php_query_connect_t *)q_base);
      break;
    case PHPQ_NETQ:
      query_stats.desc = "NET";
      php_worker_run_net_query (worker, q_base);
      break;
    case PHPQ_CREATE_QUEUE:
      query_stats.desc = "CREATE_QUEUE";
      php_worker_create_queue (worker, (php_query_create_queue_t *)q_base);
      break;
    case PHPQ_HTTP_LOAD_POST:
      query_stats.desc = "HTTP_LOAD_POST";
      php_worker_http_load_post (worker, (php_query_http_load_post_t *)q_base);
      break;
    default:
      assert ("unknown php_query type" && 0);
  }
}

extern int rpc_stored;

void rpc_error (php_worker *worker, int code, const char *str) {
  struct connection *c = worker->conn;
  //fprintf (stderr, "RPC ERROR %s\n", str);
  static int q[10000];
  q[1] = RPCS_DATA(c)->out_packet_num++;
  q[2] = RPC_REQ_ERROR;
  *(long long *)(q + 3) = worker->req_id;
  q[5] = code;
  //TODO: write str

  char *buf = (char *)(q + 6);
  int all_len = 0;
  int sn = (int)strlen (str);

  if (sn > 5000) {
    sn = 5000;
  }

  if (sn < 254) {
    *buf++ = (char) (sn);
    all_len += 1;
  } else if (sn < (1 << 24)) {
    *buf++ = (char) (254);
    *buf++ = (char) (sn & 255);
    *buf++ = (char) ((sn >> 8) & 255);
    *buf++ = (char) ((sn >> 16) & 255);
    all_len += 4;
  } else {
    assert ("TODO: store too big string" && 0);
  }

  memcpy (buf, str, (size_t)sn);
  buf += sn;
  all_len += sn;
  while (all_len % 4 != 0) {
    *buf++ = 0;
    all_len++;
  }

  int qn = 7 + all_len / 4;
  q[0] = qn * 4;
  q[qn - 1] = (int)compute_crc32 (q, q[0] - 4);

  assert (write_out (&c->Out, q, q[0]) == q[0]);

  RPCS_FUNC(c)->flush_packet (c);
}

void php_worker_set_result (php_worker *worker, script_result *res) {
  if (worker->conn != NULL) {
    if (worker->mode == http_worker) {
      if (res == NULL) {
        http_return (worker->conn, "OK", 2);
      } else {
        if (0) {
          write_basic_http_header (
              worker->conn,
              res->return_code,
              0,
              res->body_len,
              res->headers,
              "text/html; charset=windows-1251"
              );
        } else {
          write_out (&worker->conn->Out, res->headers, res->headers_len);
        }
        write_out (&worker->conn->Out, res->body, res->body_len);
      }
    } else if (worker->mode == rpc_worker) {
      if (!rpc_stored) {
        rpc_error (worker, -505, "Nothing stored");
      }
    } else if (worker->mode == once_worker) {
      assert (write (1, res->body, (size_t)res->body_len) == res->body_len);
      run_once_return_code = res->exit_code;
    }
  }
}

void php_worker_run (php_worker *worker) {
  int f = 1;
  while (f) {
    if (worker->terminate_flag) {
      php_script_terminate (php_script, worker->error_message);
    }

    get_utime_monotonic();
//    fprintf (stderr, "state = %d, f = %d\n", php_script_get_state (php_script), f);
    switch (php_script_get_state (php_script)) {
      case rst_ready: {
        vkprintf (2, "before php_script_iterate [req_id = %016llx] (before swap context)\n", worker->req_id);
        immediate_stats.is_wait_net = 0;
        php_script_iterate (php_script);
        vkprintf (2, "after php_script_iterate [req_id = %016llx] (after swap context)\n", worker->req_id);
        break;
      }
      case rst_query: {
        vkprintf (2, "got query [req_id = %016llx]\n", worker->req_id);
        php_worker_run_query (worker);
        break;
      }
      case rst_query_running: {
        vkprintf (2, "paused due to query [req_id = %016llx]\n", worker->req_id);
        f = 0;
        worker->paused = 1;
        immediate_stats.is_wait_net = 1;
        break;
      }
      case rst_error: {
        vkprintf (2, "php script [req_id = %016llx]: ERROR (probably timeout)\n", worker->req_id);
        php_script_finish (php_script);

        if (worker->conn != NULL) {
          if (worker->mode == http_worker) {
            http_return (worker->conn, "ERROR", 5);
          } else if (worker->mode == rpc_worker) {
            if (!rpc_stored) {
              rpc_error (worker, -504, php_script_get_error (php_script));
            }
          }
        }

        worker->state = phpq_free_script;
        f = 0;
        break;
      }
      case rst_finished: {
        vkprintf (2, "php script [req_id = %016llx]: OK (still can return RPC_ERROR)\n", worker->req_id);
        script_result *res = php_script_get_res (php_script);

        php_worker_set_result (worker, res);

        php_script_finish (php_script);


        worker->state = phpq_free_script;
        f = 0;
        break;
      }
      default:
        assert ("php_worker_run: unexpected state" && 0);
    }

    //trying to work with net
    if (!worker->terminate_flag) {
      int new_net_events_cnt = epoll_fetch_events (0);
      if (new_net_events_cnt > 0) {
        vkprintf (2, "paused for some net activity [req_id = %016llx]\n", worker->req_id);
        f = 0;
        put_event_into_heap_tail (worker->conn->ev, 1);
        worker->conn->status = conn_wait_net;
        worker->paused = 1;
      }
    }
  }
}

void php_worker_free_script (php_worker *worker) {
  php_worker_run_flag = 0;
  int f = 0;

  get_utime_monotonic();
  double worked = precise_now - worker->start_time;
  double waited = worker->start_time - worker->init_time;

  vkprintf (1, "FINISH php script [query worked = %.5lf] [query waited for start = %.5lf] [req_id = %016llx]\n", worked, waited, worker->req_id);
  immediate_stats.is_running = 0;
  immediate_stats.is_wait_net = 0;
  if (worker->mode == once_worker) {
    static int left = run_once_count;
    if (!--left) {
      turn_sigterm_on();
    }
  }
  if (worked + waited > 1.0) {
    vkprintf (1, "ATTENTION php script [query worked = %.5lf] [query waited for start = %.5lf] [req_id = %016llx]\n", worked, waited, worker->req_id);
  }

  while (pending_http_queue.first_query != (struct conn_query *)&pending_http_queue && !f) {
    //TODO: is it correct to do it?
    struct conn_query *q = pending_http_queue.first_query;
    f = q->requester != NULL && q->requester->generation == q->req_generation;
    delete_pending_query (q);
  }

  qmem_clear();
  qresults_clean();

  php_script_clear (php_script);

  static int finished_queries = 0;
  if ((++finished_queries) % queries_to_recreate_script == 0) {
    php_script_free (php_script);
    php_script = NULL;
  }

  /*if (http_sfd != -1) {*/
    /*epoll_insert (http_sfd, EVT_RWX | EVT_LEVEL);*/
  /*}*/

  worker->state = phpq_finish;
}

int get_current_target (void);
void php_worker_finish (php_worker *worker) {
  vkprintf (2, "free php script [req_id = %016llx]\n", worker->req_id);
  if ((lease_state == lst_on || lease_state == lst_finish) && worker->target_fd == rpc_lease_target) {
    double worked = precise_now - worker->start_time;
    lease_stats_time += worked;
    lease_stats_cnt++;
  }

  php_worker_free (worker);
}

int php_worker_main (php_worker *worker) {
  worker->paused = 0;
  do {
    switch (worker->state) {
      case phpq_try_start:
        php_worker_try_start (worker);
        break;

      case phpq_init_script:
        php_worker_init_script (worker);
        break;

      case phpq_run:
        php_worker_run (worker);
        break;

      case phpq_free_script:
        php_worker_free_script (worker);
        break;

      case phpq_finish:
        php_worker_finish (worker);
        return 1;
    }
  } while (!worker->paused);

  assert (worker->conn->status == conn_wait_net);
  return 0;
}


int hts_php_wakeup (struct connection *c);
int hts_php_alarm (struct connection *c);

conn_type_t ct_php_engine_http_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "http_server",
  .accept = accept_new_connections,
  .init_accepted = hts_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = hts_parse_execute,
  .close = hts_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = hts_php_wakeup,
  .alarm = hts_php_alarm
};

int hts_php_wakeup (struct connection *c) {
  if (c->status == conn_wait_net || c->status == conn_wait_aio) {
    c->status = conn_expect_query;
    HTS_FUNC(c)->ht_wakeup (c);
  }
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  //c->generation = ++conn_generation;
  //c->pending_queries = 0;
  return 0;
}

int hts_php_alarm (struct connection *c) {
  HTS_FUNC(c)->ht_alarm (c);
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}


int hts_func_wakeup (struct connection *c);
int hts_func_alarm (struct connection *c);
int hts_func_execute (struct connection *c, int op);
int hts_func_close (struct connection *c, int who);

struct http_server_functions http_methods = {
  .execute = hts_func_execute,
  .ht_wakeup = hts_func_wakeup,
  .ht_alarm = hts_func_alarm,
  .ht_close = hts_func_close
};

static char *qPost, *qGet, *qUri, *qHeaders;
static int qPostLen, qGetLen, qUriLen, qHeadersLen;

static char no_cache_headers[] =
"Pragma: no-cache\r\n"
"Cache-Control: no-store\r\n";

#define HTTP_RESULT_SIZE  (4 << 20)

//static char http_body_buffer[HTTP_RESULT_SIZE], *http_w;
//#define http_w_end  (http_body_buffer + HTTP_RESULT_SIZE - 16384)
//void http_return (struct connection *c, const char *str, int len);

void http_return (struct connection *c, const char *str, int len) {
  if (len < 0) {
    len = (int)strlen (str);
  }
  write_basic_http_header (c, 200, 0, len, no_cache_headers, "text/javascript; charset=UTF-8");
  write_out (&c->Out, str, len);
}

#define MAX_POST_SIZE (1 << 18)

void hts_my_func_finish (struct connection *c) {
/*  c->status = conn_expect_query;
  clear_connection_timeout (c);

  if (!(D->query_flags & QF_KEEPALIVE)) {
    c->status = conn_write_close;
    c->parse_state = -1;
  }*/
}

int hts_stopped = 0;
void hts_stop() {
  if (hts_stopped) {
    return;
  }
  if (http_sfd != -1) {
    epoll_close (http_sfd);
    close (http_sfd);
    http_sfd = -1;
  }
  sigterm_time = precise_now + SIGTERM_WAIT_TIMEOUT;
  hts_stopped = 1;
}

void hts_at_query_end (struct connection *c, int check_keep_alive) {
  struct hts_data *D = HTS_DATA (c);

  clear_connection_timeout (c);
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  D->extra = NULL;
  if (check_keep_alive && !(D->query_flags & QF_KEEPALIVE)) {
    c->status = conn_write_close;
    c->parse_state = -1;
  } else {
    c->flags |= C_REPARSE;
  }
  assert (c->status != conn_wait_net);
}

int hts_func_execute (struct connection *c, int op) {
  struct hts_data *D = HTS_DATA(c);
  static char ReqHdr[MAX_HTTP_HEADER_SIZE];
  static char Post[MAX_POST_SIZE];

  if (sigterm_on && sigterm_time < precise_now) {
    return -501;
  }

  vkprintf (1, "in hts_execute: connection #%d, op=%d, header_size=%d, data_size=%d, http_version=%d\n",
      c->fd, op, D->header_size, D->data_size, D->http_ver);

  if (D->query_type != htqt_get && D->query_type != htqt_post) {
    D->query_flags &= ~QF_KEEPALIVE;
    return -501;
  }

  if (D->data_size > 0) {
    int have_bytes = get_total_ready_bytes (&c->In);
    if (have_bytes < D->data_size + D->header_size && D->data_size < MAX_POST_SIZE) {
      vkprintf (1, "-- need %d more bytes, waiting\n", D->data_size + D->header_size - have_bytes);
      return D->data_size + D->header_size - have_bytes;
    }
  }

  assert (D->header_size <= MAX_HTTP_HEADER_SIZE);
  assert (read_in (&c->In, &ReqHdr, D->header_size) == D->header_size);

  qHeaders = ReqHdr + D->first_line_size;
  qHeadersLen = D->header_size - D->first_line_size;
  assert (D->first_line_size > 0 && D->first_line_size <= D->header_size);

  vkprintf (1, "===============\n%.*s\n==============\n", D->header_size, ReqHdr);
  vkprintf (1, "%d,%d,%d,%d\n", D->host_offset, D->host_size, D->uri_offset, D->uri_size);

  vkprintf (1, "hostname: '%.*s'\n", D->host_size, ReqHdr + D->host_offset);
  vkprintf (1, "URI: '%.*s'\n", D->uri_size, ReqHdr + D->uri_offset);

//  D->query_flags &= ~QF_KEEPALIVE;

  if (0 < D->data_size && D->data_size < MAX_POST_SIZE) {
    assert (read_in (&c->In, Post, D->data_size) == D->data_size);
    Post[D->data_size] = 0;
    vkprintf (1, "have %d POST bytes: `%.80s`\n", D->data_size, Post);
    qPost = Post;
    qPostLen = D->data_size;
  } else {
    qPost = NULL;
    if (D->data_size > 0) {
      qPostLen = D->data_size;
    } else {
      qPostLen = 0;
    }
  }

  qUri = ReqHdr + D->uri_offset;
  qUriLen = D->uri_size;

  char *get_qm_ptr = memchr (qUri, '?', (size_t)qUriLen);
  if (get_qm_ptr) {
    qGet = get_qm_ptr + 1;
    qGetLen = (int)(qUri + qUriLen - qGet);
    qUriLen = (int)(get_qm_ptr - qUri);
  } else {
    qGet = NULL;
    qGetLen = 0;
  }

  if (qUriLen >= 200) {
    return -418;
  }

  vkprintf (1, "OK, lets do something\n");
  set_connection_timeout (c, script_timeout);

  /** save query here **/
  http_query_data *http_data = http_query_data_create (qUri, qUriLen, qGet, qGetLen, qHeaders, qHeadersLen, qPost,
      qPostLen, D->query_type == htqt_get ? "GET" : "POST", D->query_flags & QF_KEEPALIVE, c->remote_ip, c->remote_port);

  static long long http_script_req_id = 0;
  php_worker *worker = php_worker_create (http_worker, c, http_data, NULL, script_timeout, ++http_script_req_id);
  D->extra = worker;

  int res = php_worker_main (worker);
  if (res == 1) {
    hts_at_query_end (c, 0);
  } else {
    assert (c->pending_queries >= 0 && c->status == conn_wait_net);
  }
  return 0;
}

int hts_func_wakeup (struct connection *c) {
  struct hts_data *D = HTS_DATA(c);

  assert (c->status == conn_expect_query || c->status == conn_wait_net);
  c->status = conn_expect_query;

  php_worker *worker = D->extra;
  if (worker->finish_time < precise_now + 0.01) {
    php_worker_terminate (worker, 0, "timeout in http connection wakeup");
  }
  int res = php_worker_main (worker);
  if (res == 1) {
    hts_at_query_end (c, 1);
  } else {
    assert (c->pending_queries >= 0 && c->status == conn_wait_net);
  }
  return 0;
}

int hts_func_alarm (struct connection *c) {
  struct hts_data *D = HTS_DATA(c);

  //fprintf (stderr, "pending_queries = %d\n", c->pending_queries);

  assert (c->status == conn_expect_query || c->status == conn_wait_net);
  c->status = conn_expect_query;

  php_worker *worker = D->extra;
  php_worker_terminate (worker, 0, "http connection alarm");
  int res = php_worker_main (worker);
  if (res == 1) {
    hts_at_query_end (c, 1);
  } else {
    assert ("worker is unfinished after alarm" && 0);
  }
  return 0;
}

int hts_func_close (struct connection *c, int who) {
  struct hts_data *D = HTS_DATA(c);

  php_worker *worker = D->extra;
  if (worker != NULL) {
    php_worker_terminate (worker, 1, "http connection close");
    int res = php_worker_main (worker);
    D->extra = NULL;
    assert ("worker is unfinished after closing connection" && res == 1);
  }
  return 0;
}


/***
  RPC INTERFACE
 ***/
int rpcs_php_wakeup (struct connection *c);
int rpcs_php_alarm (struct connection *c);
int rpcs_php_close_connection (struct connection *c, int who);

int rpcc_php_wakeup (struct connection *c);
int rpcc_php_alarm (struct connection *c);
int rpcc_php_close_connection (struct connection *c, int who);

conn_type_t ct_php_engine_rpc_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "rpc_server",
  .accept = accept_new_connections,
  .init_accepted = rpcs_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = rpcs_parse_execute,
  .close = rpcs_php_close_connection, //replaced, then replaced back
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = rpcs_php_wakeup, //replaced
  .alarm = rpcs_php_alarm, //replaced
//#ifdef AES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
//#endif
};

conn_type_t ct_php_rpc_client = {
  .magic = CONN_FUNC_MAGIC,
  .title = "rpc_client",
  .accept = server_failed,
  .init_accepted = server_failed,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = rpcc_parse_execute,
  .close = rpcc_php_close_connection, //replaced from (client_close_connection)
  .free_buffers = free_connection_buffers,
  .init_outbound = rpcc_init_outbound,
  .connected = rpcc_connected,
  .wakeup = rpcc_php_wakeup, // replaced
  .alarm = rpcc_php_alarm, // replaced
  .check_ready = rpc_client_check_ready,
//#ifdef AES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
//#endif
};

int rpcs_php_wakeup (struct connection *c) {
  if (c->status == conn_wait_net) {
    c->status = conn_expect_query;
    RPCS_FUNC(c)->rpc_wakeup (c);
  }
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  //c->generation = ++conn_generation;
  //c->pending_queries = 0;
  return 0;
}

int rpcs_php_alarm (struct connection *c) {
  RPCS_FUNC(c)->rpc_alarm (c);
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}

int rpcs_php_close_connection (struct connection *c, int who) {
  if (RPCS_FUNC(c)->rpc_close != NULL) {
    RPCS_FUNC(c)->rpc_close (c, who);
  }

  return server_close_connection (c, who);
}


int rpcc_php_wakeup (struct connection *c) {
  if (c->status == conn_wait_net) {
    c->status = conn_expect_query;
    RPCC_FUNC(c)->rpc_wakeup (c);
  }
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  //c->generation = ++conn_generation;
  //c->pending_queries = 0;
  return 0;
}

int rpcc_php_alarm (struct connection *c) {
  RPCC_FUNC(c)->rpc_alarm (c);
  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  return 0;
}

int rpcc_php_close_connection (struct connection *c, int who) {
  if (RPCC_FUNC(c)->rpc_close != NULL) {
    RPCC_FUNC(c)->rpc_close (c, who);
  }

  return client_close_connection (c, who);
}

int rpcx_execute (struct connection *c, int op, int len);
int rpcx_func_wakeup (struct connection *c);
int rpcx_func_alarm (struct connection *c);
int rpcx_func_close (struct connection *c, int who);

int rpcc_func_ready (struct connection *c);

struct rpc_server_functions rpc_methods = {
  .execute = rpcx_execute, //replaced
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_wakeup = rpcx_func_wakeup, //replaced
  .rpc_alarm = rpcx_func_alarm, //replaced
  .rpc_close = rpcx_func_close, //replaced
};

struct rpc_client_functions rpc_client_methods = {
  .execute = rpcx_execute, //replaced
  .check_ready = rpcc_default_check_ready, //replaced
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpcc_func_ready,
  .rpc_wakeup = rpcx_func_wakeup, //replaced
  .rpc_alarm = rpcx_func_alarm, //replaced
  .rpc_close = rpcx_func_close, //replaced
};

struct conn_target rpc_client_ct = {
  .min_connections = 1,
  .max_connections = 1,
  //.type = &ct_rpc_client,
  .type = &ct_php_rpc_client,
  .extra = (void *)&rpc_client_methods,
  .port = 11210,
  .reconnect_timeout = 1
};


void send_rpc_query (struct connection *c, int op, long long id, int *q, int qn);
int ready_cnt = 0;
void rpc_send_ready (struct connection *c) {
  int q[100], qn = 0;
  qn += 2;
  q[qn++] = -1;
  q[qn++] = (int)c->our_ip; // addr
  q[qn++] = (int)c->our_port; // port
  q[qn++] = pid; // pid
  q[qn++] = start_time; // start_time
  q[qn++] = worker_id; // id
  q[qn++] = ready_cnt++; // ready_cnt
  qn++;
  send_rpc_query (c, RPC_READY, -1, q, qn * 4);
}

void rpc_send_stopped (struct connection *c) {
  int q[100], qn = 0;
  qn += 2;
  q[qn++] = -1;
  q[qn++] = (int)c->our_ip; // addr
  q[qn++] = (int)c->our_port; // port
  q[qn++] = pid; // pid
  q[qn++] = start_time; // start_time
  q[qn++] = worker_id; // id
  q[qn++] = ready_cnt++; // ready_cnt
  qn++;
  send_rpc_query (c, RPC_STOP_READY, -1, q, qn * 4);
}

void rpc_send_lease_stats (struct connection *c) {
  int q[100], qn = 0;
  qn += 2;
  q[qn++] = -1;
  *(npid_t *)(q + qn) = lease_pid;
  assert (sizeof (lease_pid) == 12);
  qn += 3;
  *(double *)(q + qn) = precise_now - lease_stats_start_time;
  qn += 2;
  *(double *)(q + qn) = lease_stats_time;
  qn += 2;
  q[qn++] = lease_stats_cnt;
  qn++;

  send_rpc_query (c, TL_KPHP_LEASE_STATS, -1, q, qn * 4);
}

int rpct_ready (int target_fd) {
  if (target_fd == -1) {
    return -1;
  }
  struct conn_target *target = &Targets[target_fd];
  struct connection *conn = get_target_connection (target, 0);
  if (conn == NULL) {
    return -2;
  }
  rpc_send_ready (conn);
  return 0;
}

void rpct_stop_ready (int target_fd) {
  if (target_fd == -1) {
    return;
  }
  struct conn_target *target = &Targets[target_fd];
  struct connection *conn = get_target_connection (target, 0);
  if (conn != NULL) {
    rpc_send_stopped (conn);
  }
}
void rpct_lease_stats (int target_fd) {
  if (target_fd == -1) {
    return;
  }
  struct conn_target *target = &Targets[target_fd];
  struct connection *conn = get_target_connection (target, 0);
  if (conn != NULL) {
    rpc_send_lease_stats (conn);
  }
}

int get_current_target (void) {
  if (lease_state == lst_off) {
    return rpc_main_target;
  }
  if (lease_state == lst_on) {
    return rpc_lease_target;
  }
  return -1;
}

int lease_off (void) {
  assert (lease_state == lst_off);
  if (!lease_ready_flag) {
    return 0;
  }
  if (has_pending_scripts()) {
    return 0;
  }
  if (rpct_ready (rpc_main_target) >= 0) {
    lease_ready_flag = 0;
    return 1;
  }
  return 0;
}

int lease_on (void) {
  assert (lease_state == lst_on);
  if (!lease_ready_flag) {
    return 0;
  }
  if (has_pending_scripts()) {
    return 0;
  }
  if (rpct_ready (rpc_lease_target) >= 0) {
    lease_ready_flag = 0;
    return 1;
  }
  return 0;
}

int lease_start (void) {
  assert (lease_state == lst_start);
  if (has_pending_scripts()) {
    return 0;
  }
  lease_change_state (lst_on);
  lease_ready_flag = 1;
  if (rpc_stopped) {
    lease_change_state (lst_finish);
  }
  return 1;
}

int lease_finish (void) {
  assert (lease_state == lst_finish);
  if (has_pending_scripts()) {
    return 0;
  }
  rpct_stop_ready (rpc_lease_target);
  rpct_lease_stats (rpc_main_target);
  lease_change_state (lst_off);
  lease_ready_flag = 1;
  return 1;
}

void run_rpc_lease (void) {
  int run_flag = 1;
  while (run_flag) {
    run_flag = 0;
    switch (lease_state) {
      case lst_off:
        run_flag = lease_off();
        break;
      case lst_start:
        run_flag = lease_start();
        break;
      case lst_on:
        run_flag = lease_on();
        break;
      case lst_finish:
        run_flag = lease_finish();
        break;
      default:
        assert (0);
    }
  }
}

void lease_cron (void) {
  int need = 0;

  if (lease_state == lst_on && rpc_lease_timeout < precise_now) {
    lease_change_state (lst_finish);
    need = 1;
  }
  if (lease_ready_flag) {
    need = 1;
  }
  if (need) {
    run_rpc_lease();
  }
}


void do_rpc_stop_lease (void) {
  if (lease_state != lst_on) {
    return;
  }
  lease_change_state (lst_finish);
  run_rpc_lease();
}

int do_rpc_start_lease (npid_t pid, double timeout) {
  if (rpc_main_target == -1) {
    return -1;
  }

  if (lease_state != lst_off) {
    return -1;
  }
  int target_fd = get_lease_target_by_pid (pid.ip, pid.port, &rpc_client_ct);
  if (target_fd == -1) {
    return -1;
  }
  if (target_fd == rpc_main_target) {
    vkprintf (0, "can't lease to itself\n");
    return -1;
  }
  if (rpc_stopped) {
    return -1;
  }

  rpc_lease_target = target_fd;
  rpc_lease_timeout = timeout;
  lease_pid = pid;

  lease_stats_cnt = 0;
  lease_stats_start_time = precise_now;
  lease_stats_time = 0;

  lease_change_state (lst_start);
  run_rpc_lease();

  return 0;
}

int rpcc_func_ready (struct connection *c) {
  c->last_query_sent_time = precise_now;
  c->last_response_time = precise_now;

  int target_fd = c->target - Targets;
  if (target_fd == get_current_target() && !has_pending_scripts()) {
    lease_ready_flag = 1;
    run_rpc_lease();
  }
  return 0;
}


void rpcc_stop () {
  if (rpc_client_target != -1) {
    struct conn_target *target = &Targets[rpc_client_target];
    struct connection *conn = get_target_connection (target, 0);
    if (conn != NULL) {
      rpc_send_stopped (conn);
    }
    do_rpc_stop_lease();
  }
  rpc_stopped = 1;
  sigterm_time = precise_now + SIGTERM_WAIT_TIMEOUT;
}

void rpcx_at_query_end (struct connection *c) {
  struct rpcs_data *D = RPCS_DATA(c);

  clear_connection_timeout (c);
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  D->extra = NULL;

  if (!has_pending_scripts()) {
    lease_ready_flag = 1;
    run_rpc_lease();
  }
  c->flags |= C_REPARSE;
  assert (c->status != conn_wait_net);
}

int rpcx_func_wakeup (struct connection *c) {
  struct rpcs_data *D = RPCS_DATA(c);

  assert (c->status == conn_expect_query || c->status == conn_wait_net);
  c->status = conn_expect_query;

  php_worker *worker = D->extra;
  int res = php_worker_main (worker);
  if (res == 1) {
    rpcx_at_query_end (c);
  } else {
    assert (c->pending_queries >= 0 && c->status == conn_wait_net);
  }
  return 0;
}

int rpcx_func_alarm (struct connection *c) {
  struct rpcs_data *D = RPCS_DATA(c);

  //fprintf (stderr, "pending_queries = %d\n", c->pending_queries);

  assert (c->status == conn_expect_query || c->status == conn_wait_net);
  c->status = conn_expect_query;

  php_worker *worker = D->extra;
  php_worker_terminate (worker, 0, "rpc connection alarm");
  int res = php_worker_main (worker);
  if (res == 1) {
    rpcx_at_query_end (c);
  } else {
    assert ("worker is unfinished after alarm" && 0);
  }
  return 0;
}

int rpcx_func_close (struct connection *c, int who) {
  struct rpcs_data *D = RPCS_DATA(c);

  php_worker *worker = D->extra;
  if (worker != NULL) {
    php_worker_terminate (worker, 1, "rpc connection close");
    int res = php_worker_main (worker);
    D->extra = NULL;
    assert ("worker is unfinished after closing connection" && res == 1);

    if (!has_pending_scripts()) {
      lease_ready_flag = 1;
      run_rpc_lease();
    }
  }

  return 0;
}


int rpcx_execute (struct connection *c, int op, int len) {
  struct rpcs_data *D = RPCS_DATA(c);

  vkprintf (1, "rpcs_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);

#define MAX_RPC_QUERY_LEN 126214400
  static char buf[MAX_RPC_QUERY_LEN];

  if (sigterm_on && sigterm_time < precise_now) {
    return SKIP_ALL_BYTES;
  }
  npid_t xpid;

  switch (op) {
  case TL_KPHP_STOP_LEASE:
    do_rpc_stop_lease();
    break;
  case TL_KPHP_START_LEASE:
  case RPC_INVOKE_KPHP_REQ:
  case RPC_INVOKE_REQ:
    if (len > MAX_RPC_QUERY_LEN) {
      return SKIP_ALL_BYTES;
    }

    assert (read_in (&c->In, buf, len) == len);
    assert (len % (int)sizeof (int) == 0);
    len /= (int)sizeof (int);
    if (len < 6) {
      return SKIP_ALL_BYTES;
    }

    int *v = (int *)buf;
    v += 3;
    len -= 4;

    if (op == TL_KPHP_START_LEASE) {
      if (len < 4) {
        return SKIP_ALL_BYTES;
      }
      assert (sizeof (xpid) == 12);
      xpid = *(npid_t *)v;
      v += 3;
      len -= 3;
      int timeout = *v++;
      len--;
      do_rpc_start_lease (xpid, precise_now + timeout);
      return 0;
      break;
    }

    long long req_id = *(long long *)v;
    v += 2;
    len -= 2;

    vkprintf (2, "got RPC_INVOKE_KPHP_REQ [req_id = %016llx]\n", req_id);
    set_connection_timeout (c, script_timeout);


    rpc_query_data *rpc_data = rpc_query_data_create (v, len, req_id, D->remote_pid.ip, D->remote_pid.port, D->remote_pid.pid, D->remote_pid.utime);

    php_worker *worker = php_worker_create (run_once ? once_worker : rpc_worker, c, NULL, rpc_data, script_timeout, req_id);
    D->extra = worker;

    int res = php_worker_main (worker);
    if (res == 1) {
      rpcx_at_query_end (c);
    } else {
      assert (c->pending_queries >= 0 && c->status == conn_wait_net);
    }

    return 0;
    break;
  }

  return SKIP_ALL_BYTES;
#undef MAX_RPC_QUERY_LEN
}


/***
  Common query function
 ***/

void pnet_query_answer (struct conn_query *q) {
  struct connection *req = q->requester;
  if (req != NULL && req->generation == q->req_generation) {
    void *extra = NULL;
    if (req->type == &ct_php_engine_rpc_server) {
      extra = RPCS_DATA (req)->extra;
    } else if (req->type == &ct_php_rpc_client) {
      extra = RPCC_DATA (req)->extra;
    } else if (req->type == &ct_php_engine_http_server) {
      extra = HTS_DATA (req)->extra;
    } else {
      assert ("unexpected type of connection\n" && 0);
    }
    php_worker_answer_query (extra, ((net_ansgen_t *)(q->extra))->ans);
  }
}

void pnet_query_delete (struct conn_query *q) {
  net_ansgen_t *ansgen = (net_ansgen_t *)q->extra;

  ansgen->func->free (ansgen);
  q->extra = NULL;

  delete_conn_query (q);
  zfree (q, sizeof (*q));
}

int pnet_query_timeout (struct conn_query *q) {

  net_ansgen_t *net_ansgen = (net_ansgen_t *)q->extra;
  net_ansgen->func->timeout (net_ansgen);

  pnet_query_answer (q);

  delete_conn_query_from_requester (q);

  return 0;
}

int pnet_query_term (struct conn_query *q) {
  net_ansgen_t *net_ansgen = (net_ansgen_t *)q->extra;
  net_ansgen->func->error (net_ansgen, "Connection closed by server");

  pnet_query_answer (q);
  pnet_query_delete (q);

  return 0;
}

int pnet_query_check (struct conn_query *q) {
  net_ansgen_t *net_ansgen = (net_ansgen_t *)q->extra;

  ansgen_state_t state = net_ansgen->state;
  switch (state) {
    case st_ansgen_done:
    case st_ansgen_error:
      pnet_query_answer (q);
      pnet_query_delete (q);
      break;

    case st_ansgen_wait:
      break;
  }

  return state == st_ansgen_error;
}

/***
  MEMCACHED CLIENT
 ***/

void data_read_conn (data_reader_t *reader, void *dest) {
  reader->readed = 1;
  read_in (&((struct connection *)(reader->extra))->In, dest, reader->len);
}

data_reader_t *create_data_reader (struct connection *c, int data_len) {
  static data_reader_t reader;

  reader.readed = 0;
  reader.len = data_len;
  reader.extra = c;

  reader.read = data_read_conn;

  return &reader;
}

int mc_query_value (struct conn_query *q, data_reader_t *reader) {
  mc_ansgen_t *ansgen = (mc_ansgen_t *)q->extra;
  ansgen->func->value (ansgen, reader);

  int err = pnet_query_check (q);
  return err;
}

int mc_query_other (struct conn_query *q, data_reader_t *reader) {
  mc_ansgen_t *ansgen = (mc_ansgen_t *)q->extra;
  ansgen->func->other (ansgen, reader);

  int err = pnet_query_check (q);
  return err;
}

int mc_query_version (struct conn_query *q, data_reader_t *reader) {
  mc_ansgen_t *ansgen = (mc_ansgen_t *)q->extra;
  ansgen->func->version (ansgen, reader);

  int err = pnet_query_check (q);
  return err;
}

int mc_query_end (struct conn_query *q) {
  mc_ansgen_t *ansgen = (mc_ansgen_t *)q->extra;
  ansgen->func->end (ansgen);

  return pnet_query_check (q);
}

int mc_query_xstored (struct conn_query *q, int is_stored) {
  mc_ansgen_t *ansgen = (mc_ansgen_t *)q->extra;
  ansgen->func->xstored (ansgen, is_stored);

  return pnet_query_check (q);
}

int mc_query_error (struct conn_query *q)  {
  net_ansgen_t *ansgen = (net_ansgen_t *)q->extra;
  ansgen->func->error (ansgen, "some protocol error");

  return pnet_query_check (q);
}

struct conn_query_functions pnet_cq_func = {
  .magic = (int)CQUERY_FUNC_MAGIC,
  .title = "pnet-cq-query",
  .wakeup = pnet_query_timeout,
  .close = pnet_query_term,
//  .complete = mc_query_done //???
};

struct conn_query_functions pnet_delayed_cq_func = {
  .magic = (int)CQUERY_FUNC_MAGIC,
  .title = "pnet-cq-query",
  .wakeup = pnet_query_term,
  .close = pnet_query_term,
//  .complete = mc_query_done //???
};


int memcache_client_check_ready (struct connection *c) {
  if (c->status == conn_connecting) {
    return c->ready = cr_ok;
  }

  /*assert (c->status != conn_none);*/
  if (c->status == conn_none || c->status == conn_connecting) {
    return c->ready = cr_notyet;
  }
  if (c->status == conn_error || c->ready == cr_failed) {
    return c->ready = cr_failed;
  }
  return c->ready = cr_ok;
}

int memcache_connected (struct connection *c) {
  if (c->Tmp != NULL) {
    int query_len = get_total_ready_bytes (c->Tmp);
    copy_through (&c->Out, c->Tmp, query_len);
  }

  return 0;
}

int memcache_client_execute (struct connection *c, int op) {
  struct mcc_data *D = MCC_DATA(c);
  int len, x = 0;
  char *ptr;

  vkprintf (1, "proxy_mc_client: op=%d, key_len=%d, arg#=%d, response_len=%d\n", op, D->key_len, D->arg_num, D->response_len);

  if (op == mcrt_empty) {
    return SKIP_ALL_BYTES;
  }

  struct conn_query *cur_query = NULL;
  if (c->first_query == (struct conn_query *)c) {
    if (op != mcrt_VERSION) {
      vkprintf (-1, "response received for empty query list? op=%d\n", op);
      if (verbosity > -2) {
        dump_connection_buffers (c);
      }
      D->response_flags |= 16;
      return SKIP_ALL_BYTES;
    }
  } else {
    cur_query = c->first_query;
  }

  c->last_response_time = precise_now;

  int query_len;
  data_reader_t *reader;
  switch (op) {
  case mcrt_empty:
    return SKIP_ALL_BYTES;

  case mcrt_VALUE:
    if (D->key_len > 0 && D->key_len <= MAX_KEY_LEN && D->arg_num == 2 && (unsigned) D->args[1] <= MAX_VALUE_LEN) {
      int needed_bytes = (int)(D->args[1] + D->response_len + 2 - c->In.total_bytes);
      if (needed_bytes > 0) {
        return needed_bytes;
      }
      nbit_advance (&c->Q, (int)D->args[1]);
      len = nbit_ready_bytes (&c->Q);
      assert (len > 0);
      ptr = nbit_get_ptr (&c->Q);
    } else {
      vkprintf (-1, "error at VALUE: op=%d, key_len=%d, arg_num=%d, value_len=%lld\n", op, D->key_len, D->arg_num, D->args[1]);

      D->response_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    if (len == 1) {
      nbit_advance (&c->Q, 1);
    }
    if (ptr[0] != '\r' || (len > 1 ? ptr[1] : *((char *) nbit_get_ptr (&c->Q))) != '\n') {
      vkprintf (-1, "missing cr/lf at VALUE: op=%d, key_len=%d, arg_num=%d, value_len=%lld\n", op, D->key_len, D->arg_num, D->args[1]);

      assert (0);

      D->response_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    len = 2;

    //vkprintf (1, "mcc_value: op=%d, key_len=%d, flags=%lld, time=%lld, value_len=%lld\n", op, D->key_len, D->args[0], D->args[1], D->args[2]);

    query_len = (int)(D->response_len + D->args[1] + len);
    reader = create_data_reader (c, query_len);
    x = mc_query_value (c->first_query, reader);
    if (x) {
      fail_connection (c, -7);
      c->ready = cr_failed;
    }
    if (reader->readed) {
      return 0;
    }
    assert (advance_skip_read_ptr (&c->In, query_len) == query_len);
    return 0;

  case mcrt_VERSION:
    c->unreliability >>= 1;
    vkprintf (3, "mcc_got_version: op=%d, key_len=%d, unreliability=%d\n", op, D->key_len, c->unreliability);

    if (cur_query != NULL) {
      query_len = D->response_len;
      reader = create_data_reader (c, query_len);
      x = mc_query_version (cur_query, reader);
      if (x) {
        fail_connection (c, -8);
        c->ready = cr_failed;
      }
      if (reader->readed) {
        return 0;
      }
    }
    return SKIP_ALL_BYTES;

  case mcrt_CLIENT_ERROR:
    vkprintf (-1, "CLIENT_ERROR received from connection %d (%s:%d)\n", c->fd, conv_addr (c->remote_ip, NULL), c->remote_port);
    //client_errors_received++;
  case mcrt_ERROR:
    //errors_received++;
    /*if (verbosity > -2 && errors_received < 32) {
      dump_connection_buffers (c);
      if (c->first_query != (struct conn_query *) c && c->first_query->req_generation == c->first_query->requester->generation) {
        dump_connection_buffers (c->first_query->requester);
      }
    }*/

    fail_connection (c, -5);
    c->ready = cr_failed;
    //conn_query will be closed during connection closing
    return SKIP_ALL_BYTES;
  case mcrt_STORED:
  case mcrt_NOTSTORED:
    x = mc_query_xstored (c->first_query, op == mcrt_STORED);
    if (x) {
      fail_connection (c, -6);
      c->ready = cr_failed;
    }
    return SKIP_ALL_BYTES;
  case mcrt_SERVER_ERROR:
  case mcrt_NUMBER:
  case mcrt_DELETED:
  case mcrt_NOTFOUND:
    query_len = D->response_len;
    reader = create_data_reader (c, query_len);
    x = mc_query_other (c->first_query, reader);
    if (x) {
      fail_connection (c, -7);
      c->ready = cr_failed;
    }
    if (reader->readed) {
      return 0;
    }
    assert (advance_skip_read_ptr (&c->In, query_len) == query_len);
    return 0;
    break;

  case mcrt_END:
    mc_query_end (c->first_query);
    return SKIP_ALL_BYTES;

  default:
    assert ("unknown state" && 0);
    x = 0;
  }

  assert ("unreachable position" && 0);
  return 0;
}


/***
  DB CLIENT
 ***/
int sql_query_packet (struct conn_query *q, data_reader_t *reader) {
  sql_ansgen_t *ansgen = (sql_ansgen_t *)q->extra;
  ansgen->func->packet (ansgen, reader);

  return pnet_query_check (q);
}

int sql_query_done (struct conn_query *q) {
  sql_ansgen_t *ansgen = (sql_ansgen_t *)q->extra;
  ansgen->func->done (ansgen);

  return pnet_query_check (q);
}

void create_pnet_delayed_query (struct connection *http_conn, struct conn_target *t, net_ansgen_t *gen, double finish_time) {
  struct conn_query *q = zmalloc (sizeof (struct conn_query));

  q->custom_type = 0;
  //q->outbound = conn;
  q->requester = http_conn;
  q->start_time = precise_now;

  q->extra = gen;

  q->cq_type = &pnet_delayed_cq_func;
  q->timer.wakeup_time = finish_time;

  insert_conn_query_into_list (q, (struct conn_query *)t);

  return;
}

struct conn_query *create_pnet_query (struct connection *http_conn, struct connection *conn, net_ansgen_t *gen, double finish_time) {
  struct conn_query *q = zmalloc (sizeof (struct conn_query));

  q->custom_type = 0;
  q->outbound = conn;
  q->requester = http_conn;
  q->start_time = precise_now;

  q->extra = gen;

  q->cq_type = &pnet_cq_func;
  q->timer.wakeup_time = finish_time;

  insert_conn_query (q);

  return q;
}

int sqlp_becomes_ready (struct connection *c) {
  struct conn_query *q;


  while (c->target->first_query != (struct conn_query *)(c->target)) {
    q = c->target->first_query;
    //    fprintf (stderr, "processing delayed query %p for target %p initiated by %p (%d:%d<=%d)\n", q, c->target, q->requester, q->requester->fd, q->req_generation, q->requester->generation);
    if (q->requester != NULL && q->requester->generation == q->req_generation) {
      // q->requester->status = conn_expect_query;   // !!NOT SURE THAT THIS CAN BE COMMENTED!!
      q->requester->queries_ok++;
      //waiting_queries--;

      net_ansgen_t *net_ansgen = (net_ansgen_t *)q->extra;
      create_pnet_query (q->requester, c, net_ansgen, q->timer.wakeup_time);

      delete_conn_query (q);
      zfree (q, sizeof (*q));

      sql_ansgen_t *ansgen = (sql_ansgen_t *)net_ansgen;
      ansgen->func->ready (ansgen, c);
      break;
    } else {
      //waiting_queries--;
      delete_conn_query (q);
      zfree (q, sizeof (*q));
    }
  }
  return 0;
}

int sqlp_check_ready (struct connection *c) {
  if (c->status == conn_ready && c->In.total_bytes > 0) {
    vkprintf (-1, "have %d bytes in outbound sql connection %d in state ready, closing connection\n", c->In.total_bytes, c->fd);
    c->status = conn_error;
    c->error = -3;
    fail_connection (c, -3);
    return c->ready = cr_failed;
  }
  if (c->status == conn_error) {
    c->error = -4;
    fail_connection (c, -4);
    return c->ready = cr_failed;
  }
  if (c->status == conn_wait_answer || c->status == conn_reading_answer) {
    if (!(c->flags & C_FAILED) && c->last_query_sent_time < precise_now - RESPONSE_FAIL_TIMEOUT - c->last_query_time && c->last_response_time < precise_now - RESPONSE_FAIL_TIMEOUT - c->last_query_time && !(SQLC_DATA(c)->extra_flags & 1)) {
      vkprintf (1, "failing outbound connection %d, status=%d, response_status=%d, last_response=%.6f, last_query=%.6f, now=%.6f\n", c->fd, c->status, SQLC_DATA(c)->response_state, c->last_response_time, c->last_query_sent_time, precise_now);
      c->error = -5;
      fail_connection (c, -5);
      return c->ready = cr_failed;
    }
  }
  return c->ready = (c->status == conn_ready ? cr_ok : (SQLC_DATA(c)->auth_state == sql_auth_ok ? cr_stopped : cr_busy));  /* was cr_busy instead of cr_stopped */
}


int sqlp_authorized (struct connection *c) {
  nbw_iterator_t it;
  unsigned temp = 0x00000000;
  int len = 0;
  char ptype = 2;

  if (!sql_database || !*sql_database) {
    SQLC_DATA(c)->auth_state = sql_auth_ok;
    c->status = conn_ready;
    SQLC_DATA(c)->packet_state = 0;
    vkprintf (1, "outcoming initdb successful\n");
    SQLC_FUNC(c)->sql_becomes_ready (c);
    return 0;
  }

  nbit_setw (&it, &c->Out);
  write_out (&c->Out, &temp, 4);

  len += write_out (&c->Out, &ptype, 1);
  if (sql_database && *sql_database) {
    len += write_out (&c->Out, sql_database, (int)strlen (sql_database));
  }

  nbit_write_out (&it, &len, 3);

  SQLC_FUNC(c)->sql_flush_packet (c, len);

  SQLC_DATA(c)->auth_state = sql_auth_initdb;
  return 0;
}

int stop_forwarding_response (struct connection *c) {
  //TODO: stop forwarding if requester is dead
  return 0;

/*  struct connection *d = c->first_query->requester;
  assert (d);
  if (d->generation != c->first_query->req_generation || d->Out.total_bytes + d->Out.unprocessed_bytes <= FORWARD_HIGH_WATERMARK) {
    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "forwarding response from outbound connection %d to connection %d stopped: already %d+%d bytes in output buffers.\n", c->fd, d->fd, d->Out.total_bytes, d->Out.unprocessed_bytes);
  }
  d->write_low_watermark = FORWARD_LOW_WATERMARK;
  c->flags |= C_STOPREAD;
  create_reverse_watermark_query (d, c);
  c->status = conn_wait_net;
  return 1;*/
}

int proxy_client_execute (struct connection *c, int op) {
  struct sqlc_data *D = SQLC_DATA(c);
  static char buffer[8];
  int b_len, field_cnt = -1;
  nb_iterator_t it;

  nbit_set (&it, &c->In);
  b_len = nbit_read_in (&it, buffer, 8);

  if (b_len >= 5) {
    field_cnt = buffer[4] & 0xff;
  }

  vkprintf (1, "proxy_db_client: op=%d, packet_len=%d, response_state=%d, field_num=%d\n", op, D->packet_len, D->response_state, field_cnt);

  if (c->first_query == (struct conn_query *)c) {
    vkprintf (-1, "response received for empty query list? op=%d\n", op);
    return SKIP_ALL_BYTES;
  }

  c->last_response_time = precise_now;

  if (stop_forwarding_response (c)) {
    return 0;
  }

  int query_len = D->packet_len + 4;
  data_reader_t *reader = create_data_reader (c, query_len);

  int x;
  switch (D->response_state) {
  case resp_first:
    //forward_response (c, D->packet_len, SQLC_DATA(c)->extra_flags & 1);
    x = sql_query_packet (c->first_query, reader);

    if (!reader->readed) {
      assert (advance_skip_read_ptr (&c->In, query_len) == query_len);
    }

    if (x) {
      fail_connection (c, -6);
      c->ready = cr_failed;
      return 0;
    }

    if (field_cnt == 0 && (SQLC_DATA(c)->extra_flags & 1)) {
      dl_unreachable ("looks like unused code");
      SQLC_DATA(c)->extra_flags |= 2;
      if (c->first_query->requester->generation != c->first_query->req_generation) {
        vkprintf (1, "outbound connection %d: nowhere to forward replication stream, closing\n", c->fd);
        c->status = conn_error;
      }
    } else if (field_cnt == 0 || field_cnt == 0xff) {
      D->response_state = resp_done;
    } else if (field_cnt < 0 || field_cnt >= 0xfe) {
      c->status = conn_error; // protocol error
    } else {
      D->response_state = resp_reading_fields;
    }
    break;
  case resp_reading_fields:
    //forward_response (c, D->packet_len, 0);
    x = sql_query_packet (c->first_query, reader);
    if (!reader->readed) {
      assert (advance_skip_read_ptr (&c->In, query_len) == query_len);
    }

    if (x) {
      fail_connection (c, -7);
      c->ready = cr_failed;
      return 0;
    }

    if (field_cnt == 0xfe) {
      D->response_state = resp_reading_rows;
    }
    break;
  case resp_reading_rows:
    //forward_response (c, D->packet_len, 0);
    x = sql_query_packet (c->first_query, reader);
    if (!reader->readed) {
      assert (advance_skip_read_ptr (&c->In, query_len) == query_len);
    }
    if (x) {
      fail_connection (c, -8);
      c->ready = cr_failed;
      return 0;
    }
    if (field_cnt == 0xfe) {
      D->response_state = resp_done;
    }
    break;
  case resp_done:
    vkprintf (-1, "unexpected packet from server!\n");
    assert (0);
  }

  if (D->response_state == resp_done) {
//    query_complete (c, 1);
    x = sql_query_done (c->first_query);
    if (x) {
      fail_connection (c, -9);
      c->ready = cr_failed;
      return 0;
    }

    //active_queries--;
    c->unreliability >>= 1;
    c->status = conn_ready;
    c->last_query_time = precise_now - c->last_query_sent_time;
    //SQLC_DATA(c)->extra_flags &= -2;
    sqlp_becomes_ready (c);
  }

  return 0;
}

/***
  RCP CLIENT
 ***/
int rpcc_check_ready (struct connection *c) {
  /*assert (c->status != conn_none);*/
  /*if (c->status == conn_none || c->status == conn_connecting || RPCC_DATA(c)->in_packet_num < 0) {*/
    /*return c->ready = cr_notyet;*/
  /*}*/
  /*if (c->status == conn_error || c->ready == cr_failed) {*/
    /*return c->ready = cr_failed;*/
  /*}*/
  /*return c->ready = cr_ok;*/

  if ((c->flags & C_FAILED) || c->status == conn_error) {
    if (verbosity > 1 && c->ready != cr_failed) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [FAILED] lq=%.03f lr=%.03f now=%.03f\n", c->fd, c->ready, cr_failed, c->last_query_sent_time, c->last_response_time, precise_now);
    }
    return c->ready = cr_failed;
  }

  if (RPCC_DATA(c)->in_packet_num < 0) {
    if (RPCC_DATA(c)->in_packet_num == -1 && c->status == conn_running) {
      if (verbosity > 1 && c->ready != cr_ok) {
        fprintf (stderr, "changing connection %d readiness from %d to %d [OK] lq=%.03f lr=%.03f now=%.03f\n", c->fd, c->ready, cr_ok, c->last_query_sent_time, c->last_response_time, precise_now);
      }
      return c->ready = cr_ok;
    }

    assert (c->last_query_sent_time != 0);
    if (c->last_query_sent_time < precise_now - RPC_CONNECT_TIMEOUT) {
      fail_connection (c, -6);
      return c->ready = cr_failed;
    }
    return c->ready = cr_notyet;
  }
  assert (c->status != conn_connecting);

  if (c->last_query_sent_time < precise_now - rpc_ping_interval) {
    net_rpc_send_ping (c, lrand48());
    c->last_query_sent_time = precise_now;
  }

  assert (c->last_response_time != 0);
  if (c->last_response_time < precise_now - RPC_FAIL_INTERVAL) {
    if (verbosity > 1 && c->ready != cr_failed) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [FAILED] lq=%.03f lr=%.03f now=%.03f\n", c->fd, c->ready, cr_failed, c->last_query_sent_time, c->last_response_time, precise_now);
    }
    fail_connection (c, -5);
    return c->ready = cr_failed;
  }

  if (c->last_response_time < precise_now - RPC_STOP_INTERVAL) {
    if (verbosity > 1 && c->ready != cr_stopped) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [STOPPED] lq=%.03f lr=%.03f now=%.03f\n", c->fd, c->ready, cr_stopped, c->last_query_sent_time, c->last_response_time, precise_now);
    }
    return c->ready = cr_stopped;
  }

  if (c->status == conn_wait_answer || c->status == conn_expect_query || c->status == conn_reading_answer) {
    if (verbosity > 1 && c->ready != cr_ok) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [OK] lq=%.03f lr=%.03f now=%.03f\n", c->fd, c->ready, cr_ok, c->last_query_sent_time, c->last_response_time, precise_now);
    }
    return c->ready = cr_ok;
  }

  if (c->status == conn_running) {
    if (verbosity > 1 && c->ready != cr_busy) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [BUSY] lq=%.03f lr=%.03f now=%.03f\n", c->fd, c->ready, cr_busy, c->last_query_sent_time, c->last_response_time, precise_now);
    }
    return c->ready = cr_busy;
  }

  assert (0);
  return c->ready = cr_failed;
}

int rpcc_send_query (struct connection *c) {
  //rpcc_ready
  c->last_query_sent_time = precise_now;
  c->last_response_time = precise_now;

  struct conn_query *q;
  dl_assert (c != NULL, "...");
  dl_assert (c->target != NULL, "...");

  while (c->target->first_query != (struct conn_query *)(c->target)) {
    q = c->target->first_query;
    dl_assert (q != NULL, "...");
    dl_assert (q->requester != NULL, "...");
    //    fprintf (stderr, "processing delayed query %p for target %p initiated by %p (%d:%d<=%d)\n", q, c->target, q->requester, q->requester->fd, q->req_generation, q->requester->generation);
    if (q->requester != NULL && q->requester->generation == q->req_generation) {
      q->requester->queries_ok++;
      //waiting_queries--;
      net_ansgen_t *net_ansgen = (net_ansgen_t *)q->extra;

      net_send_ansgen_t *ansgen = (net_send_ansgen_t *)net_ansgen;
      ansgen->func->send_and_finish (ansgen, c);

      pnet_query_answer (q);
      pnet_query_delete (q);
      break;
    } else {
      //waiting_queries--;
      delete_conn_query (q);
      zfree (q, sizeof (*q));
    }
  }
  return 0;
}

int net_get_query_done (struct conn_query *q) {
  net_get_ansgen_t *ansgen = (net_get_ansgen_t *)q->extra;
  assert (ansgen != NULL);
  assert (ansgen->func != NULL);
  assert (ansgen->func->done != NULL);
  ansgen->func->done (ansgen);

  return pnet_query_check (q);
}


void got_result_ (long long request_id) {
  vkprintf (2, "got_result [rpc_id = <%lld>], wait for [rpc_id = <%lld>]\n", request_id, cur_request_id);
  if (cur_request_id != request_id) {
    return;
  }

  cur_request_id = -1;

  while (dummy_request_queue.first_query != (struct conn_query *)&dummy_request_queue) {
    struct conn_query *q = dummy_request_queue.first_query;
    net_get_query_done (q);
    assert (dummy_request_queue.first_query != q);
  }
}

int rpcc_execute (struct connection *c, int op, int len) {
  vkprintf (1, "rpcc_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);

  int head[5];
  qres_t *qres;

  nb_iterator_t Iter;

  char *data;
  int data_len;

  c->last_response_time = precise_now;

  switch (op) {
    case RPC_REQ_ERROR:
    case RPC_REQ_RESULT:
      assert (len % (int)sizeof (int) == 0);
      len /= (int)sizeof (int);
      assert (len >= 6);

      nbit_set (&Iter, &c->In);
      assert (nbit_read_in (&Iter, head, sizeof (int) * 5) == sizeof (int) * 5);

      long long id = *(long long *)(&head[3]);

      qres = get_qres (id, qr_ans);
      if (qres == NULL) {
        got_result (id);
        break;
      }

      if (op == RPC_REQ_ERROR) {
        qres_error (qres);
        break;
      }

      data_len = len - 5 - 1;
      data = malloc (sizeof (int) * (size_t)data_len);
      assert (nbit_read_in (&Iter, data, data_len * (int)sizeof (int)) == data_len * (int)sizeof (int));

      nbit_clear (&Iter);

      if (qres_save (qres, data, data_len * (int)sizeof (int)) < 0) {
        free (data);
      }

      break;
    case RPC_PONG:
      break;
  }

  return SKIP_ALL_BYTES;
}

static char stats[65536];
static int stats_len;
void prepare_full_stats() {
  char *s = stats;
  int s_left = 65530;

  int uptime = now - start_time;
  worker_acc_stats.tot_idle_time = tot_idle_time;
  worker_acc_stats.tot_idle_percent =
    uptime > 0 ? tot_idle_time / uptime * 100 : 0;
  worker_acc_stats.a_idle_percent = a_idle_quotient > 0 ?
    a_idle_time / a_idle_quotient * 100 : 0;
  size_t acc_stats_size = sizeof (worker_acc_stats);
  assert (s_left > acc_stats_size);
  memcpy (s, &worker_acc_stats, acc_stats_size);
  s += acc_stats_size;
  s_left -= (int)acc_stats_size;

#define W(args...)  ({\
  int written_tmp___ = snprintf (s, (size_t)s_left, ##args);\
  if (written_tmp___ > s_left - 1) {\
    written_tmp___ = s_left - 1;\
  }\
  s += written_tmp___;\
  s_left -= written_tmp___;\
})

  int pid = (int)getpid();
  W ("pid %d\t%d\n", pid, pid);
  W ("active_special_connections %d\t%d\n", pid, active_special_connections);
  W ("max_special_connections %d\t%d\n", pid, max_special_connections);
//  W ("TODO: more stats\n");
#undef W
  stats_len = (int)(s - stats);
}

/***
  SERVER
 ***/
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
  if (logname && (fd = open (logname, O_WRONLY | O_APPEND | O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      assert (close (fd) >= 0);
    }
  }
  vkprintf (1, "logs reopened.\n");
}

static void sigint_immediate_handler (const int sig) {
  const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  _exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  _exit (1);
}

static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << sig);
  dl_signal (sig, sigint_immediate_handler);
}

static void turn_sigterm_on (void) {
  if (sigterm_on != 1) {
    sigterm_time = precise_now + SIGTERM_MAX_TIMEOUT;
    sigterm_on = 1;
  }
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  //pending_signals |= (1 << sig);
  turn_sigterm_on();
  dl_signal (sig, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  //unnecessary:
  //dl_signal (sig, sighup_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
}

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  //unnecessary:
  //dl_signal (sig, sigusr1_handler);
}

int pipe_packet_id = 0;
void write_full_stats_to_pipe() {
  if (master_pipe_write != -1) {
    prepare_full_stats();

    int qsize = stats_len + 1 + (int)sizeof (int) * 5;
    qsize = (qsize + 3) & -4;
    int *q = malloc ((size_t)qsize);
    memset (q, 0, (size_t)qsize);

    q[2] = RPC_PHP_FULL_STATS;
    memcpy (q + 3, stats, (size_t)stats_len);

    prepare_rpc_query_raw (pipe_packet_id++, q, qsize);
    int err = (int)write (master_pipe_write, q, (size_t)qsize);
    dl_passert (err == qsize, dl_pstr ("error during write to pipe [%d]\n", master_pipe_write));
    if (err != qsize) {
      kill (getppid(), 9);
    }
    free (q);
  }
}

sig_atomic_t pipe_fast_packet_id = 0;
void write_immediate_stats_to_pipe() {
  if (master_pipe_fast_write != -1) {
#define QSIZE (sizeof (php_immediate_stats_t) + 1 + sizeof (int) * 5 + 3) & -4
    int q[QSIZE];
    int qsize = QSIZE;
#undef QSIZE
    memset (q, 0, (size_t)qsize);

    q[2] = RPC_PHP_IMMEDIATE_STATS;
    memcpy (q + 3, &immediate_stats, sizeof (php_immediate_stats_t));

    prepare_rpc_query_raw (pipe_fast_packet_id++, q, qsize);
    int err = (int)write (master_pipe_fast_write, q, (size_t)qsize);
    dl_passert (err == qsize, dl_pstr ("error [%d] during write to pipe", errno));
  }
}

//Used for interaction with master.
int spoll_send_stats;
static void sigstats_handler (int signum, siginfo_t *info, void *data) {
  dl_assert (info != NULL, "SIGPOLL with no info");
  if (info->si_code == SI_QUEUE) {
    int code = info->si_value.sival_int;
    if ((code & 0xFFFF0000)== SPOLL_SEND_STATS) {
      if (code & SPOLL_SEND_FULL_STATS) {
        spoll_send_stats++;
      }
      if (code & SPOLL_SEND_IMMEDIATE_STATS) {
        write_immediate_stats_to_pipe();
      }
    }
  }
}

void cron (void) {
  if (master_flag == -1 && getppid() == 1) {
    turn_sigterm_on();
  }
}

int try_get_http_fd () {
  return server_socket (http_port, settings_addr, backlog, 0);
}

void start_server (void) {
  int i;
  int prev_time;
  double next_create_outbound = 0;

  if (run_once) {
    master_flag = 0;
    rpc_port = -1;
    http_port = -1;
    rpc_client_port = -1;
  }

  pending_signals = 0;
  if (daemonize) {
    setsid();

    dl_signal (SIGHUP, sighup_handler);
    reopen_logs();
  }
  if (master_flag) {
    vkprintf (-1, "master\n");
    if (rpc_port != -1) {
      vkprintf (-1, "rpc_port is ignored in master mode\n");
      rpc_port = -1;
    }

    if (0 && http_port != -1) {
      vkprintf (-1, "http_port is ignored in master mode\n");
      http_port = -1;
    }
  }

  init_netbuffers();

  init_epoll();
  if (master_flag) {
    start_master (http_port > 0 ? &http_sfd : NULL, &try_get_http_fd, http_port);

    if (logname_pattern != NULL) {
      reopen_logs();
    }
  }

  prev_time = 0;

  if (http_port > 0 && http_sfd < 0) {
    dl_assert (!master_flag, "failed to get http_fd\n");
    if (master_flag) {
      vkprintf (-1, "try_get_http_fd after start_master\n");
      exit (1);
    }
    http_sfd = try_get_http_fd();
    if (http_sfd < 0) {
      vkprintf (-1, "cannot open http server socket at port %d: %m\n", http_port);
      exit (1);
    }
  }

  if (rpc_port > 0 && rpc_sfd < 0) {
    rpc_sfd = server_socket (rpc_port, settings_addr, backlog, 0);
    if (rpc_sfd < 0) {
      vkprintf (-1, "cannot open rpc server socket at port %d: %m\n", rpc_port);
      exit (1);
    }
  }

  if (verbosity > 0 && http_sfd >= 0) {
    vkprintf (-1, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, NULL), port, http_sfd);
  }

  if (change_user (username) < 0) {
    vkprintf (-1, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (http_sfd >= 0) {
    init_listening_tcpv6_connection (http_sfd, &ct_php_engine_http_server, &http_methods, SM_SPECIAL);
  }

  if (rpc_sfd >= 0) {
    init_listening_connection (rpc_sfd, &ct_php_engine_rpc_server, &rpc_methods);
  }

  if (no_sql) {
    sql_target_id = -1;
  } else {
    sql_target_id = get_target ("localhost", 3306, &db_ct);
    assert (sql_target_id != -1);
  }

  if ((rpc_client_host != NULL) ^ (rpc_client_port != -1)) {
    vkprintf (-1, "warning: only rpc_client_host or rpc_client_port is defined\n");
  }
  if (rpc_client_host != NULL && rpc_client_port != -1) {
    vkprintf (-1, "create rpc client target: %s:%d\n", rpc_client_host, rpc_client_port);
    rpc_client_target = get_target (rpc_client_host, rpc_client_port, &rpc_client_ct);
    rpc_main_target = rpc_client_target;
  }

  if (run_once) {
    int pipe_fd[2];
    pipe (pipe_fd);

    int read_fd = pipe_fd[0];
    int write_fd = pipe_fd[1];

    rpc_client_methods.rpc_ready = NULL;
    create_pipe_reader (read_fd, &ct_php_rpc_client, &rpc_client_methods);

    int q[6];
    int qsize = 6 * sizeof (int);
    q[2] = RPC_INVOKE_REQ;
    int i;
    for (i = 0; i < run_once_count; i++) {
      prepare_rpc_query_raw (i, q, qsize);
      assert (write (write_fd, q, (size_t)qsize) == qsize);
    }
  }

  get_utime_monotonic();
  //create_all_outbound_connections();

  dl_signal (SIGTERM, sigterm_handler);
  dl_signal (SIGPIPE, SIG_IGN);
  dl_signal (SIGINT, sigint_handler);
  dl_signal (SIGUSR1, sigusr1_handler);
  dl_signal (SIGPOLL, sigpoll_handler);

  //using sigaction for sigpoll
  assert (SIGRTMIN <= SIGSTAT && SIGSTAT <= SIGRTMAX);
  dl_sigaction (SIGSTAT, NULL, dl_get_empty_sigset(), SA_SIGINFO | SA_ONSTACK | SA_RESTART, sigstats_handler);

  dl_allow_all_signals();

  vkprintf (1, "Server started\n");
  for (i = 0; !(pending_signals & ~((1ll << SIGUSR1) | (1ll << SIGHUP))); i++) {
    if (verbosity > 0 && !(i & 255)) {
      vkprintf (1, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
          active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    if (precise_now > next_create_outbound) {
      create_all_outbound_connections();
      next_create_outbound = precise_now + 0.03 + 0.02 * drand48();
    }

    while (spoll_send_stats > 0) {
      write_full_stats_to_pipe();
      spoll_send_stats--;
    }

    if (sigpoll_cnt > 0) {
      vkprintf (1, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      sigpoll_cnt = 0;
    }

    if (pending_signals & (1ll << SIGHUP)) {
      pending_signals &= ~(1ll << SIGHUP);
    }

    if (pending_signals & (1ll << SIGUSR1)) {
      pending_signals &= ~(1ll << SIGUSR1);

      reopen_logs();
    }

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    lease_cron();
    if (sigterm_on && !rpc_stopped) {
      rpcc_stop();
    }
    if (sigterm_on && !hts_stopped) {
      hts_stop();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;

    if (sigterm_on && precise_now > sigterm_time && !php_worker_run_flag &&
        pending_http_queue.first_query == (struct conn_query *)&pending_http_queue) {
      vkprintf (1, "Quitting because of sigterm\n");
      break;
    }
  }

  if (verbosity > 0 && pending_signals) {
    vkprintf (1, "Quitting because of pending signals = %llx\n", pending_signals);
  }

  if (http_sfd >= 0) {
    epoll_close (http_sfd);
    assert (close (http_sfd) >= 0);
  }
}

void read_engine_tag (const char *file_name);

void read_tl_config (const char *file_name);

void ini_set (const char *key, const char *value);

void init_scripts (void);

void static_init_scripts (void);

void init_all() {
  srand48 ((long)rdtsc());

  //init pending_http_queue
  pending_http_queue.first_query = pending_http_queue.last_query = (struct conn_query *)&pending_http_queue;
  php_worker_run_flag = 0;

  dummy_request_queue.first_query = dummy_request_queue.last_query = (struct conn_query *)&dummy_request_queue;

  //init php_script
  static_init_scripts();
  init_handlers();

  init_drivers();
  got_result = got_result_;

  init_scripts();

  worker_id = (int)lrand48();
}

/*
 *
 *    MAIN
 *
 */

void init_logname (char *src) {
  char *t = src;
  while (*t && *t != '%') {
    t++;
  }
  int has_percent = *t == '%';
  if (!has_percent) {
    logname = src;
    kprintf_multiprocessing_mode_enable();
    return;
  }

  char buf1[100];
  char buf2[100];
  int buf_len = 100;

  char *patt = buf1;
  char *plane = buf2;

  int was_percent = 0;
  while (*src) {
    assert (patt < buf1 + buf_len - 3);
    if (*src == '%') {
      if (!was_percent) {
        *patt++ = '%';
        *patt++ = 'd';
        was_percent = 1;
      }
    } else {
      *patt++ = *src;
      *plane++ = *src;
    }
    src++;
  }
  *patt = 0;
  patt = buf1;
  *plane = 0;
  plane = buf2;

  logname = strdup (plane);
  logname_pattern = strdup (patt);
}

/** main arguments parsing **/
void usage (void);

void usage_default_info (void) {
  printf ("\t%s\n", full_version_str);

}

#define ARGS_STR_DEFAULT "b:c:l:m:u:dhkv"

void usage_default_params (void) {
  printf ("[-d] [-k] [-v] [-m<memory>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-l<log-name>]");
}

void usage_default_desc (void) {
  printf ("\t-d\tdaemonize\n"
    "\t-k\tlock paged memory\n"
    "\t-v\toutput statistical and debug information into stderr\n"
    "\t-m<memory>\tmaximal size of used memory in megabytes not including zmemory for struct conn_query in mebibytes\n"
    "\t-u<username>\tuser name\n"
    "\t-b<backlog>\tset backlog\n"
    "\t-c<max-conn>\tset maximum connections number\n"
    "\t-l<log-name>\tset log name\n");
}

int main_args_default_handler (int i) {
  switch (i) {
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
  case 'l':
    init_logname (optarg);
    break;
  case 'm':
    max_memory = atoi (optarg);
    if (max_memory < 1) {
      max_memory = 1;
    }
    if (max_memory > 2047) {
      max_memory = 2047;
    }
    max_memory *= 1048576;
    break;
  case 'u':
    username = optarg;
    break;
  case 'd':
    daemonize ^= 1;
    break;
  case 'h':
    usage();
    return 2;
  case 'k':
    if (mlockall (MCL_CURRENT | MCL_FUTURE) != 0) {
      vkprintf (-1, "error: fail to lock paged memory\n");
    }
    break;
  case 'v':
    verbosity++;
    break;
  default:
    return 0;
  }
  return 1;
}

#define ARGS_STR "D:E:H:r:w:f:p:s:T:t:A:oq"

void usage_params (void) {
  printf ("[-H<port>] [-r<rpc_port>] [-w<host>:<port>] [-q] [f<workers_n>] [-D<key>=<value>] [-o] [-p<master_port>] [-s<cluster_name>] [-T<tl_config_file_name>] [-t<script_time_limit>]");
}

void usage_desc (void) {
  printf ("\t-D<key>=<value>\tset data for ini_get\n"
    "\t-H<port>\thttp port\n"
    "\t-r<rpc_port>\trpc_port\n"
    "\t-w<host>:<port>\thost and port for client mode\n"
    "\t-q\tno sql\n"
    "\t-E<error-tag-file>\tname of file with engine tag showed on every warning\n"
    "\t-f<workers_n>\trun workers_n workers\n"
    "\t-o\trun script once\n"
    "\t-p<master_port>\tport for memcached interface to master\n"
    "\t-s<cluster_name>\tused to distinguish clusters\n"
    "\t-T<tl_config_file_name>\tname of file with TL config\n"
    "\t-t<script_time_limit>\ttime limit for script in seconds\n"
    "\t-A<db_user:db_pass@db_name>\tconfig for mysql access\n"
    );
}

int main_args_handler (int i) {
  switch (i) {
  case 'A':
    {
      char *line = strdup(optarg);
      char *user_pass = strtok(line, "@");
      char *database = strtok(NULL, "@");
      char *user = strtok(user_pass, ":");
      char *pass = strtok(NULL, ":");

      sql_username = user;
      sql_password = pass;
      sql_database = database;
    }
  break;
  case 'D':
    {
      char *key = optarg, *value;
      char *eq = strchr (key, '=');
      if (eq == NULL) {
        vkprintf (-1, "-D option, can't find '='\n");
        usage();
        return 2;
      }
      value = eq + 1;
      *eq = 0;
      ini_set (key, value);
    }
    break;
  case 'H':
    http_port = atoi (optarg);
    break;
  case 'r':
    rpc_port = atoi (optarg);
    break;
  case 'w':
    rpc_client_host = optarg;
    {
      char *colon = strrchr ((char *)rpc_client_host, ':');
      if (colon == NULL) {
        vkprintf (-1, "-w option, can't find ':'\n");
        usage();
        return 2;
      }
      *colon++ = 0;
      rpc_client_port = atoi (colon);
    }
    break;
  case 'E':
    read_engine_tag (optarg);
    break;
  case 'f':
    workers_n = atoi (optarg);
    if (workers_n >= 0) {
      if (workers_n > MAX_WORKERS) {
        workers_n = MAX_WORKERS;
      }
      master_flag = 1;
    }
    break;
  case 'p':
    master_port = atoi (optarg);
    break;
  case 's':
    cluster_name = optarg;
    break;
  case 'T':
    read_tl_config (optarg);
    break;
  case 't':
    script_timeout = atoi (optarg);
    if (script_timeout < 1) {
      script_timeout = 1;
    }
    if (script_timeout > MAX_SCRIPT_TIMEOUT) {
      script_timeout = MAX_SCRIPT_TIMEOUT;
    }
    break;
  case 'o':
    run_once = 1;
    break;
  case 'q':
    no_sql = 1;
    break;
  default:
    return 0;
  }
  return 1;
}

void usage (void) {
  printf ("usage: %s ", progname);

  usage_default_params();
  printf (" ");
  usage_params();

  printf ("\n");

  usage_default_info();

  usage_default_desc();
  usage_desc();

  exit (2);
}


void parse_main_args_end (int argc, char *argv[]) {
  if (argc != optind) {
    usage();
  }
}

void parse_main_args (int argc, char *argv[]) {
  progname = argv[0];
  int i;
  while ((i = getopt (argc, argv, ARGS_STR_DEFAULT ARGS_STR)) != -1) {
    if (!main_args_default_handler (i) && !main_args_handler (i)) {
      usage();
    }
  }

  parse_main_args_end (argc, argv);
}


void init_default() {
  dl_set_default_handlers();
  now = (int)time (NULL);

  pid = getpid();
  // RPC part
  PID.port = (short)rpc_port;

  dynamic_data_buffer_size = (1 << 26);//26 for struct conn_query
  init_dyn_data();

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    vkprintf (-1, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  aes_load_pwd_file (NULL);

  if (change_user (username) < 0) {
    vkprintf (-1, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }
}

int main (int argc, char *argv[]) {
  dl_block_all_signals();
  tcp_maximize_buffers = 1;
  max_special_connections = 1;
  assert (offsetof (struct rpc_client_functions, rpc_ready) == offsetof (struct rpc_server_functions, rpc_ready));

  parse_main_args (argc, argv);

  load_time = -dl_time();

  init_default();

  init_all();

  load_time += dl_time();

  start_time = (int)time (NULL);

  start_server();

  vkprintf (1, "return 0;\n");
  if (run_once) {
    return run_once_return_code;
  }
  return 0;
}
