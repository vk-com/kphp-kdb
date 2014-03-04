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
              2012-2013 Vitaliy Valtman
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>
#include <stdarg.h>
#include <math.h>
#include <stddef.h>
#include <sys/time.h>
#include <kfs.h>
#include <signal.h>

#include "crc32.h"
#include "md5.h"
#include "kdb-data-common.h"
#include "resolver.h"
#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "server-functions.h"
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "net-crypto-aes.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "net-rpc-common.h"
#include "rpc-proxy.h"
#include "rpc-proxy-merge.h"
#include "rpc-proxy-merge-diagonal.h"
#include "rpc-proxy-binlog.h"
#include "vv-tree.h"
#include "vv-tl-parse.h"
#include "vv-io.h"
#include "kdb-rpc-proxy-binlog.h"
#include "TL/constants.h"
#include "net-tcp-connections.h"
#include "net-tcp-rpc-server.h"
#include "net-tcp-rpc-client.h"

#include "net-rpc-targets.h"

#define MAX_KEY_LEN 1000
#define MAX_KEY_RETRIES 3
#define MAX_VALUE_LEN (1 << 24)

#define VERSION_STR "rpc-proxy-0.22"

#define TCP_PORT 11213

#define MAX_NET_RES (1L << 16)

#define PING_INTERVAL 30.0
#define STOP_INTERVAL (2 * ping_interval)
#define FAIL_INTERVAL (20 * ping_interval)

#define RESPONSE_FAIL_TIMEOUT 5

#define CONNECT_TIMEOUT 3

#define MAX_USER_FRIENDS  65536

#define DEFAULT_MIN_CONNECTIONS 2
#define DEFAULT_MAX_CONNECTIONS 3

#define        SMALL_RESPONSE_THRESHOLD        256
#define        BIG_RESPONSE_THRESHOLD        14000

#ifndef COMMIT
#define COMMIT "unknown"
#endif


struct rpc_extension *extensions[MAX_EXTENSIONS];
struct rpc_schema *schemas[MAX_SCHEMAS];
int extensions_num;
int schemas_num;

extern int rpc_disable_crc32_check;

double ping_interval = PING_INTERVAL;

extern int binlog_mode_on;

int tcp_buffers = 0;
/*
 *
 *    MEMCACHED PORT
 *
 */


int rpc_proxy_stats (struct connection *c);
int rpc_check_ready (struct connection *c);
int rpc_proxy_client_ready (struct connection *c);
int tcp_rpc_proxy_client_ready (struct connection *c);
int rpc_proxy_close_conn (struct connection *c, int who);
int rpc_proxy_udp_execute (struct udp_msg *msg);
int rpc_proxy_tcp_execute (struct connection *c, int op, struct raw_message *msg);

struct memcache_server_functions mc_proxy_inbound = {
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = mcs_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = rpc_proxy_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct rpc_server_functions rpc_proxy_inbound = {
  .execute = rpc_proxy_server_execute,
  .check_ready = rpc_check_ready,
  .flush_packet = flush_later,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_close = rpc_proxy_close_conn,
  .memcache_fallback_type = &ct_memcache_server,
  .memcache_fallback_extra = &mc_proxy_inbound
};

struct rpc_client_functions rpc_proxy_outbound = {
  .execute = rpc_proxy_client_execute,
  .check_ready = rpc_check_ready,
  .flush_packet = flush_later,
  .rpc_check_perm = rpcc_default_check_perm,
  //.rpc_check_perm = 0,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpc_proxy_client_ready,
};

struct tcp_rpc_server_functions rpc_proxy_tcp = {
  .execute = rpc_proxy_tcp_execute,
  .check_ready = rpc_check_ready,
  .flush_packet = flush_later,
  .rpc_check_perm = tcp_rpcs_default_check_perm,
  .rpc_init_crypto = tcp_rpcs_init_crypto,
  .rpc_close = rpc_proxy_close_conn,
  .memcache_fallback_type = &ct_memcache_server,
  .memcache_fallback_extra = &mc_proxy_inbound
};

struct tcp_rpc_client_functions rpc_proxy_tcp_outbound = {
  .execute = rpc_proxy_tcp_execute,
  .check_ready = rpc_check_ready,
  .flush_packet = flush_later,
  .rpc_check_perm = tcp_rpcc_default_check_perm,
  //.rpc_check_perm = 0,
  .rpc_init_crypto = tcp_rpcc_init_crypto,
  .rpc_start_crypto = tcp_rpcc_start_crypto,
  .rpc_ready = tcp_rpc_proxy_client_ready,
};



struct udp_functions rpc_proxy_udp_server = {
  .magic = UDP_FUNC_MAGIC,
  .title = "rpc_proxy udp server",
  .process_msg = udp_target_process_msg
};

struct udp_target_methods rpc_proxy_udp_server_methods = {
  .on_receive = rpc_proxy_udp_execute
};

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;
int verbosity = 0, test_mode = 0;
int enable_ipv6;
int quit_steps;

int sfd;

char *fnames[3];
int fd[3];
long long fsize[3];

// unsigned char is_letter[256];
char *progname = "rpc-proxy", *username, *logname;

char extension_name[16];

int start_time;


int only_first_cluster;

extern int double_receive_count;
char *aes_pwd_file;

long long forwarded_queries;
long long received_answers;
long long received_errors;
long long timeouted_queries;
long long immediate_errors;
long long active_queries;
long long received_expired_answers;
long long skipped_answers;
long long sent_answers;
long long received_bad_answers;
long long received_bad_queries;
long long diagonal_queries;

int binlog_mode_on;

static struct rpc_current_query _CQ;
struct rpc_current_query *CQ = &_CQ;


struct rpc_query *default_create_rpc_query (long long new_qid);
struct rpc_query *default_double_receive_create_rpc_query (long long new_qid);


#define STATS_BUFF_SIZE (1 << 20)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];


static double get_double_time_since_epoch(void) __attribute__ ((unused));
static double get_double_time_since_epoch(void) {
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec + 1e-6 * tv.tv_usec;
}

int rpc_check_ready (struct connection *c) {
  double first_query_time = 0;
  double adj_precise_now = precise_now;

  if (c->status == conn_connecting || (c->last_response_time == 0 && (c->status == conn_wait_answer || c->status == conn_reading_answer)) ||  RPCS_DATA(c)->in_packet_num < 0) {
    if (c->last_query_sent_time < adj_precise_now - CONNECT_TIMEOUT) {
      fail_connection (c, -6);
      return cr_failed;
    }
    return cr_notyet;
  }
  if (c->last_query_sent_time < adj_precise_now - PING_INTERVAL) {
    net_rpc_send_ping (c, lrand48 ());
    c->last_query_sent_time = precise_now;
  }

  if (c->last_response_time < adj_precise_now - FAIL_INTERVAL || c->unreliability > 5000) {
    if (verbosity > 1 && c->ready != cr_failed) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [FAILED] fq=%.03f lq=%.03f lqt=%.03f lr=%.03f now=%.03f unr=%d\n", c->fd, c->ready, cr_failed, first_query_time, c->last_query_sent_time, c->last_query_timeout, c->last_response_time, precise_now, c->unreliability);
    }
    fail_connection (c, -5);
    return c->ready = cr_failed;
  }

  if (c->last_response_time < adj_precise_now - STOP_INTERVAL || c->unreliability > 2000) {
    if (verbosity > 1 && c->ready != cr_stopped) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [STOPPED] fq=%.03f lq=%.03f lqt=%.03f lr=%.03f now=%.03f unr=%d\n", c->fd, c->ready, cr_stopped, first_query_time, c->last_query_sent_time, c->last_query_timeout, c->last_response_time, precise_now, c->unreliability);
    }
    return c->ready = cr_stopped;
  }
    
  if (!(c->flags & C_FAILED) && c->last_response_time > 0 && (c->status == conn_wait_answer || c->status == conn_reading_answer)) {
    if (verbosity > 1 && c->ready != cr_ok) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [OK] fq=%.03f lq=%.03f lqt=%.03f lr=%.03f now=%.03f unr=%d\n", c->fd, c->ready, cr_ok, first_query_time, c->last_query_sent_time, c->last_query_timeout, c->last_response_time, precise_now, c->unreliability);
    }
    return c->ready = cr_ok;
  }

  return c->ready = cr_failed;
}

int rpc_proxy_client_ready (struct connection *c) {
  c->last_query_sent_time = precise_now;
  c->last_response_time = precise_now;
  c->unreliability = 0;
  assert (c->target);
  if (c->target->custom_field == -1) {
    c->target->custom_field = RPCC_DATA(c)->remote_pid.ip;
    rpc_target_insert_target_ext (c->target, c->target->custom_field);
  } else {
    unsigned ip = RPCC_DATA(c)->remote_pid.ip;
    assert (c->target->custom_field == ip || (c->target->custom_field == 0 && ip == PID.ip) || (c->target->custom_field == PID.ip && ip == 0));
  }
  return 0;
}

int tcp_rpc_proxy_client_ready (struct connection *c) {
  c->last_query_sent_time = precise_now;
  c->last_response_time = precise_now;
  c->unreliability = 0;
  assert (c->target);
  if (c->target->custom_field == -1) {
    c->target->custom_field = TCP_RPC_DATA(c)->remote_pid.ip;
    rpc_target_insert_target_ext (c->target, c->target->custom_field);
  } else {
    unsigned ip = TCP_RPC_DATA(c)->remote_pid.ip;
    assert (c->target->custom_field == ip || (c->target->custom_field == 0 && ip == PID.ip) || (c->target->custom_field == PID.ip && ip == 0));
  }
  return 0;
}

int rpc_proxy_close_conn (struct connection *c, int who) {
  rpc_target_delete_conn (c);
  return 0;
}

void rpc_extension_add (struct rpc_extension *E) {
  assert (extensions_num < MAX_EXTENSIONS);
  E->num = extensions_num;
  extensions[extensions_num ++] = E;
}

void rpc_schema_add (struct rpc_schema *E) {
  assert (schemas_num < MAX_SCHEMAS);
  E->num = schemas_num;
  schemas[schemas_num ++] = E;
}


/* {{{ */
  

void __conn_init_store (struct rpc_cluster_bucket *B, void *c, long long qid) {
  assert (c);
  if (!tcp_buffers) {
    tl_store_init (c, qid);
  } else {
    tl_store_init_tcp_raw_msg (c, qid);
  }
}

unsigned __conn_get_host (struct rpc_cluster_bucket *B) {
  return B->T->custom_field;
}

int __conn_get_port (struct rpc_cluster_bucket *B) {
  return B->T->port;
}

long long __conn_get_actor (struct rpc_cluster_bucket *B) {
  return B->A;
}

void *__conn_get_conn (struct rpc_cluster_bucket *B) {
  if (!B->RT) {
    if (B->T->custom_field != -1) {
      rpc_target_insert_target_ext (B->T, B->T->custom_field);      
      B->RT = rpc_target_lookup_target (B->T);
    } else {
      return 0;
    }
  }
  return rpc_target_choose_connection (B->RT, 0);
}

int __conn_get_multi_conn (struct rpc_cluster_bucket *B, void **buf, int n) {
  if (!B->RT) {
    if (B->T->custom_field != -1) {
      rpc_target_insert_target_ext (B->T, B->T->custom_field);      
      B->RT = rpc_target_lookup_target (B->T);
    } else {
      return 0;
    }
  }
  return rpc_target_choose_random_connections (B->RT, 0, n, (void *)buf);
}

int __conn_get_state (struct rpc_cluster_bucket *B) {
  if (!B->RT) {
    if (B->T->custom_field != -1) {
      rpc_target_insert_target_ext (B->T, B->T->custom_field);      
      B->RT = rpc_target_lookup_target (B->T);
    } else {
      return -1;
    }
  }
  return rpc_target_get_state (B->RT, 0);
}

enum tl_type __conn_get_type (struct rpc_cluster_bucket *B) {
  return tl_type_conn;
}

struct rpc_cluster_bucket_methods __conn_methods = {
  .init_store = __conn_init_store,
  .get_host = __conn_get_host,
  .get_port = __conn_get_port,
  .get_actor = __conn_get_actor,
  .get_conn = __conn_get_conn,
  .get_multi_conn = __conn_get_multi_conn,
  .get_state = __conn_get_state,
  .get_type = __conn_get_type
};

void __udp_init_store (struct rpc_cluster_bucket *B, void *c, long long qid) {
  struct udp_target *S = udp_target_set_choose_target (B->S);
  tl_store_init_raw_msg (S, qid);
}

unsigned __udp_get_host (struct rpc_cluster_bucket *B) {
  return B->S->ip;
}

int __udp_get_port (struct rpc_cluster_bucket *B) {
  return B->S->port;
}

long long __udp_get_actor (struct rpc_cluster_bucket *B) {
  return B->A;
}

void *__udp_get_conn (struct rpc_cluster_bucket *B) {
  struct udp_target *S = udp_target_set_choose_target (B->S);
  if (!S) { return 0; }
  return (void *)1l;
}

int __udp_get_multi_conn (struct rpc_cluster_bucket *B, void **buf, int n) {
  struct udp_target *S = udp_target_set_choose_target (B->S);
  if (!S) { return 0; }
  *buf = (void *)1l;
  return 1;
}

int __udp_get_state (struct rpc_cluster_bucket *B) {
  struct udp_target *S = udp_target_set_choose_target (B->S);
  if (!S) { return -1; }
  if (S->state == udp_ok) { 
    return 1;
  } else if (S->state == udp_stopped || S->state == udp_unknown) {
    return 0;
  } else {
    return -1;
  }
}

enum tl_type __udp_get_type (struct rpc_cluster_bucket *B) {
  return tl_type_raw_msg;
}

struct rpc_cluster_bucket_methods __udp_methods = {
  .init_store = __udp_init_store,
  .get_host = __udp_get_host,
  .get_port = __udp_get_port,
  .get_actor = __udp_get_actor,
  .get_conn = __udp_get_conn,
  .get_multi_conn = __udp_get_multi_conn,
  .get_type = __udp_get_type
};
/* }}} */

/* parse config functions {{{ */
/* file utils */

int open_file (int x, char *fname, int creat) {
  fnames[x] = fname;
  fd[x] = open (fname, creat > 0 ? O_RDWR | O_CREAT : O_RDONLY, 0600);
  if (creat < 0 && fd[x] < 0) {
    if (fd[x] < 0) {
      fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    }
    return -1;
  }
  if (fd[x] < 0) {
    fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    exit(1);
  }
  fsize[x] = lseek (fd[x], 0, SEEK_END);
  if (fsize[x] < 0) {
    fprintf (stderr, "%s: cannot seek %s: %m\n", progname, fname);
    exit(2);
  }
  lseek (fd[x], 0, SEEK_SET);
  if (verbosity) {
    fprintf (stderr, "opened file %s, fd=%d, size=%lld\n", fname, fd[x], fsize[x]);
  }
  return fd[x];
}

int default_fun_on_answer (void **IP, void **Data);
int default_fun_on_alarm (void **IP, void **Data);
int default_fun_on_free (void **IP, void **Data);
int default_on_net_fail (void **IP, void **Data);


/*
 *
 *  CONFIGURATION PARSER
 *
 */

#define IN_CLUSTER_CONFIG 1
#define IN_CLUSTER_CONNECTED  2

/*struct cluster_server {
  int id;
  char *hostname;
  int port;
  struct in_addr addr;
  struct connection *c;
  int conn, rconn;
  int in_cluster;
  int conn_retries;
  int reconnect_time;
  int list_len;
  int list_first;
};*/


struct conn_target default_rpc_ct = {
  .min_connections = DEFAULT_MIN_CONNECTIONS,
  .max_connections = DEFAULT_MAX_CONNECTIONS,
  .type = &ct_rpc_client,
  .extra = &rpc_proxy_outbound,
  .reconnect_timeout = 1
};

struct conn_target default_tcp_rpc_ct = {
  .min_connections = DEFAULT_MIN_CONNECTIONS,
  .max_connections = DEFAULT_MAX_CONNECTIONS,
  .type = &ct_tcp_rpc_client,
  .extra = &rpc_proxy_tcp_outbound,
  .reconnect_timeout = 1
};

struct rpc_config Config[2], *CurConf = Config, *NextConf = Config + 1;

DEFINE_TREE_STD_ALLOC (rpc_cluster,struct rpc_cluster *,std_ll_ptr_compare,int,std_int_compare)
tree_rpc_cluster_t *rpc_cluster_tree;

struct rpc_cluster *CC;

#define MAX_CONFIG_SIZE (1 << 20)

char config_buff[MAX_CONFIG_SIZE+4], *config_filename, *cfg_start, *cfg_end, *cfg_cur;
int config_bytes, cfg_lno, cfg_lex = -1;

static int cfg_skipspc (void) {
  while (*cfg_cur == ' ' || *cfg_cur == 9 || *cfg_cur == 13 || *cfg_cur == 10 || *cfg_cur == '#') {
    if (*cfg_cur == '#') {
      do cfg_cur++; while (*cfg_cur && *cfg_cur != 10);
      continue;
    }
    if (*cfg_cur == 10) { 
      cfg_lno++; 
    }
    cfg_cur++;
  }
  return (unsigned char) *cfg_cur;
}

static int cfg_skspc (void) {
  while (*cfg_cur == ' ' || *cfg_cur == 9) {
    cfg_cur++;
  }
  return (unsigned char) *cfg_cur;
}

static int cfg_getlex (void) {
  switch (cfg_skipspc()) {
  case ';':
  case ':':
  case '{':
  case '}':
    return cfg_lex = *cfg_cur++;
  case 'c':
    if (!memcmp (cfg_cur, "cluster", 7)) {
      cfg_cur += 7;
      return cfg_lex = 'c';
    }
    break;
  case 'e':
    if (!memcmp (cfg_cur, "extension_params", 16)) {
      cfg_cur += 16;
      return cfg_lex = 'E';
    }
    if (!memcmp (cfg_cur, "extension", 9)) {
      cfg_cur += 9;
      return cfg_lex = 'e';
    }
    break;
  case 'i':
    if (!memcmp (cfg_cur, "id", 2)) {
      cfg_cur += 2;
      return cfg_lex = 'i';
    }
    break;
  case 'm':
    if (!memcmp (cfg_cur, "mode", 4)) {
      cfg_cur += 4;
      return cfg_lex = 'm';
    }
    if (!memcmp (cfg_cur, "min_connections", 15)) {
      cfg_cur += 15;
      return cfg_lex = 'x';
    }
    if (!memcmp (cfg_cur, "max_connections", 15)) {
      cfg_cur += 15;
      return cfg_lex = 'X';
    }
    break;
  case 's':
    if (!memcmp (cfg_cur, "server", 6)) {
      cfg_cur += 6;
      return cfg_lex = 's';
    }
    if (!memcmp (cfg_cur, "step", 4)) {
      cfg_cur += 4;
      return cfg_lex = 'T';
    }
    if (!memcmp (cfg_cur, "schema", 6)) {
      cfg_cur += 6;
      return cfg_lex = 'S';
    }
    break;
  case 't':
    if (!memcmp (cfg_cur, "timeout", 7)) {
      cfg_cur += 7;
      return cfg_lex = 't';
    }
    break;
  case 'n':
    if (!memcmp (cfg_cur, "new_id", 6)) {
      cfg_cur += 6;
      return cfg_lex = 'n';
    }
    break;
  case 'p':
    if (!memcmp (cfg_cur, "proto", 5)) {
      cfg_cur += 5;
      return cfg_lex = 'p';
    }
    break;
  case 0:
    return cfg_lex = 0;
  }
  return cfg_lex = -1;
}

static int cfg_getword (void) {
  cfg_skspc();
  char *s = cfg_cur;
  if (*s != '[') {
    while ((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') || *s == '.' || *s == '-' || *s == '_') {
      s++;
    }
  } else {
    s++;
    while ((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') || *s == '.' || *s == '-' || *s == '_' || *s == ':') {
      s++;
    }
    if (*s == ']') {
      s++;
    }
  }
  return s - cfg_cur;
}

static long long cfg_getint (void) {
  cfg_skspc();
  char *s = cfg_cur;
  long long x = 0;
  while (*s >= '0' && *s <= '9') {
    x = x*10 + (*s++ - '0');
  }
  cfg_cur = s;
  return x;
}  

static int syntax (const char *msg) {
  char *ptr = cfg_cur, *end = ptr + 20;
  if (!msg) {
    msg = "syntax error";
  }
  if (cfg_lno) {
    fprintf (stderr, "%s:%d: ", config_filename, cfg_lno);
  }
  fprintf (stderr, "fatal: %s near ", msg);
  while (*ptr && *ptr != 13 && *ptr != 10) {
    putc (*ptr++, stderr);
    if (ptr > end) {
      fprintf (stderr, " ...");
      break;
    }
  }
  putc ('\n', stderr);

  return -1;
}

static void syntax_warning (const char *msg, ...) __attribute__ ((unused));
static void syntax_warning (const char *msg, ...) {
  char *ptr = cfg_cur, *end = ptr + 20;
  va_list args;
  if (cfg_lno) {
    fprintf (stderr, "%s:%d: ", config_filename, cfg_lno);
  }
  fputs ("warning: ", stderr);
  va_start (args, msg);
  vfprintf (stderr, msg, args);
  va_end (args);
  fputs (" near ", stderr);
  while (*ptr && *ptr != 13 && *ptr != 10) {
    putc (*ptr++, stderr);
    if (ptr > end) {
      fputs (" ...", stderr);
      break;
    }
  }
  putc ('\n', stderr);
}

static int expect_lexem (int lexem) {
  if (cfg_lex != lexem) {
    static char buff[32];
    sprintf (buff, "%c expected", lexem);
    return syntax (buff);
  } else {
    return 0;
  }
}

#define Expect(l) { int t = expect_lexem (l); if (t < 0) { return t; } }

void clear_config (struct rpc_config *RC, int do_destroy_targets) {
  int i, j;
  for (i = 0; i < RC->clusters_num; i++) {
    struct rpc_cluster *C = &RC->Clusters[i];
    if (do_destroy_targets && (C->proto != PROTO_UDP)) {
      for (j = 0; j < C->tot_buckets; j++) {
        vkprintf (1, "destroying target " IP_PRINT_STR ":%d\n", IP_TO_PRINT (C->buckets[j].methods->get_host (&C->buckets[j])), C->buckets[j].methods->get_port (&C->buckets[j]));
        destroy_target (C->buckets[j].T);
      }
      memset (C->buckets, 0, C->tot_buckets * sizeof (*C->buckets));
    }
    for (j = 0; j < C->extensions_num; j++) {
      if (extensions[C->extensions[j]]->del) {
        extensions[C->extensions[j]]->del (RC, C);
      }
    }
    if (schemas[C->schema_num]->del) {
      schemas[C->schema_num]->del (RC, C);
    }
    if (C->cluster_name) {
      zfree (C->cluster_name, strlen (C->cluster_name) + 1);
      C->cluster_name = 0;
    }
  }

  RC->ConfigServersCount = 0;
  RC->clusters_num = 0;
}


int parse_server (struct rpc_config *RC, struct rpc_cluster *C, int flags, int cluster_disabled) {
  unsigned char ipv6[16];
  unsigned ipv4 = 0;
  struct hostent *h;
  int port;
  assert (C->tot_buckets < MAX_CLUSTER_SERVERS);
  int l = cfg_getword ();  
  if (!l || l > 63) {
    return syntax ("hostname expected");
  }
  char c = cfg_cur[l];
  //hostname = cfg_cur;
  cfg_cur[l] = 0;

  if (!(h = kdb_gethostbyname (cfg_cur)) || !h->h_addr_list || !h->h_addr) {  
    kprintf ("cannot resolve %s\n", cfg_cur);
    return syntax ("cannot resolve hostname");
  }
  *(cfg_cur += l) = c;
  switch (h->h_addrtype) {
    case AF_INET:
      assert (h->h_length == 4);
      ipv4 = *((unsigned *) h->h_addr);
      break;
    case AF_INET6:
      assert (h->h_length == 16);
      memcpy (ipv6, h->h_addr, 16);      
      break;
    default:
      return syntax ("Can not resolve hostname: bad address type");
  }    

  cfg_getlex ();
  Expect (':');
  port = cfg_getint();

  if (!port) {
    return syntax ("port number expected");
  }

  if (port <= 0 || port >= 0x10000) {
    return syntax ("port number out of range");
  }

  unsigned pid_host = ipv4;
  unsigned pid_port = port;
  
  long long actor_id = ACTOR_ID_UNINIT;
  cfg_skspc();
  if (*cfg_cur == '!') {
    cfg_cur ++;
    actor_id = cfg_getint ();
    cfg_skspc();
  }
  if (*cfg_cur == '@') {
    cfg_cur ++;
    l = cfg_getword ();
    if (!l || l > 63) {
      return syntax ("hostname expected");
    }
    c = cfg_cur[l];
    //hostname = cfg_cur;
    cfg_cur[l] = 0;

    if (!(h = kdb_gethostbyname (cfg_cur)) || !h->h_addr_list || !h->h_addr) {
      kprintf ("cannot resolve %s\n", cfg_cur);
      return syntax ("cannot resolve hostname");
    }
    *(cfg_cur += l) = c;

    if (h->h_addrtype == AF_INET) {
      assert (h->h_length == 4);
      pid_host = *((unsigned *) h->h_addr);
    } else {
      return syntax ("cannot resolve hostname in pid");
    }

    cfg_skspc ();
    if (*cfg_cur == ':') {
      cfg_cur ++;
      pid_port = cfg_getint ();
    }    
  }

  if (C->proto == PROTO_TCP) {
    if (pid_host == ipv4) {
      pid_host = 0;
    }
    struct conn_target ct = tcp_buffers ? default_tcp_rpc_ct : default_rpc_ct;
    if (ipv4) {
      ct.target = *(struct in_addr *)&ipv4;
      memset (ct.target_ipv6, 0, 16);
    } else {
      ct.target.s_addr = 0;
      memcpy (ct.target_ipv6, h->h_addr, 16);
    }
    ct.port = port;

    ct.min_connections = C->min_connections;
    ct.max_connections = C->max_connections;
    ct.reconnect_timeout = 1.0 + 0.1 * drand48 ();

    if ((flags & 1) && !cluster_disabled) {
      int was_created = -1;
      struct conn_target *D = create_target (&ct, &was_created);
      //rpc_target_insert_target (D);
      if (was_created <= 0) {
        if (D->custom_field != -1 && pid_host) {
          if (D->custom_field != pid_host) {
            return syntax ("Bad pid host");
          }
        } else if (pid_host) {
          D->custom_field = pid_host;
          rpc_target_insert_target_ext (D, D->custom_field);
        }
        //  syntax_warning ("duplicate hostname:port %.*s:%d in cluster", l, hostname, default_ct.port);
      } else {
        D->custom_field = pid_host ? pid_host : -1;
        if (pid_host) {
          rpc_target_insert_target_ext (D, D->custom_field);
        }
      }

      RC->ConfigServers[RC->ConfigServersCount] = D;
      C->buckets[C->tot_buckets].T = D;
      C->buckets[C->tot_buckets].A = actor_id;
      C->buckets[C->tot_buckets].RT = D->custom_field != -1 ? rpc_target_lookup_target (D) : 0;
      C->buckets[C->tot_buckets].methods = &__conn_methods;
    }

    if (!cluster_disabled) {
      C->tot_buckets++;
      assert (RC->ConfigServersCount++ < MAX_CLUSTER_SERVERS);
    }

    vkprintf (1, "server #%d: ip %s, id %d\n", RC->ConfigServersCount, inet_ntoa (ct.target), ct.port);
  } else {
    pid_host = ntohl (pid_host);
    if (pid_host == 0x7f000001) {
      pid_host = PID.ip;
    }
    struct process_id pid;
    memset (&pid, 0, sizeof (pid));
    pid.ip = pid_host;
    pid.port = pid_port;
    if (ipv4) {
      set_4in6 (ipv6, ipv4);
    }
    if ((flags & 1) && !cluster_disabled) {
      struct udp_target *S = udp_target_create (&pid, ipv6, port, 0);
      C->buckets[C->tot_buckets].S = S->ST;
      C->buckets[C->tot_buckets].A = actor_id;
      C->buckets[C->tot_buckets].methods = &__udp_methods;
    }
    if (!cluster_disabled) {
      C->tot_buckets ++;
    }
    vkprintf (1, "server #-1: ip " IP_PRINT_STR ", id %d\n", IP_TO_PRINT (ipv4), port);
  }
  return 0;
}

// flags = 0 -- syntax check only (first pass), flags = 1 -- create targets and points as well (second pass)
int parse_config (struct rpc_config *RC, struct rpc_config *RC_Old, int flags) {
  int r, /*c,*/ l, i;
//  struct hostent *h;
  //char *hostname;
  int dup_port_checked, cluster_disabled;

  if (!(flags & 1)) {
    config_bytes = r = read (fd[0], config_buff, MAX_CONFIG_SIZE+1);
    if (r < 0) {
      fprintf (stderr, "error reading configuration file %s: %m\n", config_filename);
      return -2;
    }
    if (r > MAX_CONFIG_SIZE) {
      fprintf (stderr, "configuration file %s too long (max %d bytes)\n", config_filename, MAX_CONFIG_SIZE);
      return -2;
    }
  }

  cfg_cur = cfg_start = config_buff;
  cfg_end = cfg_start + config_bytes;
  *cfg_end = 0;
  cfg_lno = 0;
  
  if (!(flags & 1) && RC_Old) {
    for (i = 0; i < RC_Old->clusters_num; i++) {
      RC_Old->Clusters[i].other_cluster_no = -1;
    }
  }

  RC->ConfigServersCount = 0;
  RC->clusters_num = 0;
//  RC->kitten_php_cluster = 0;

  static struct rpc_config_create _Zconf;
  struct rpc_config_create *Zconf = &_Zconf;
  memset (Zconf, 0, sizeof (*Zconf));
  for (i = 0; i < RPC_FUN_NUM; i++) {
    Zconf->funs_last[i] = MAX_CLUSTER_FUNS;
    Zconf->funs[i][--Zconf->funs_last[i]] = rpc_fun_ok;
  }
  memset (RC->schema_extra, 0, sizeof (RC->schema_extra));
  memset (RC->extensions_extra, 0, sizeof (RC->extensions_extra));
  while (cfg_skipspc ()) {
    struct rpc_cluster *C = &RC->Clusters[RC->clusters_num];
    if (cfg_getlex () != 'c') {
      return syntax ("'cluster' expected");
    }
    assert (RC->clusters_num < MAX_CLUSTERS);
    l = cfg_getword ();
    if (!l) {
      return syntax ("cluster name expected");
    }
    C->cluster_no = RC->clusters_num++;

    if (C->cluster_name) {
      zfree (C->cluster_name, strlen (C->cluster_name) + 1);
    }
    C->cluster_name = zmalloc (l+1);
    memcpy (C->cluster_name, cfg_cur, l);
    C->cluster_name[l] = 0;
    cfg_cur += l;

    C->timeout = 0.3;
    C->min_connections = DEFAULT_MIN_CONNECTIONS;
    C->max_connections = DEFAULT_MAX_CONNECTIONS;
    C->step = 0;
    C->tot_buckets = 0;
    C->id = 0;
    C->schema = 0;
    C->schema_num = -1;
    C->new_id = 0;
    C->flags = 0;
    C->cluster_mode = 0;
    dup_port_checked = 0;
    cluster_disabled = 0;
    memset (&C->methods, 0, sizeof (C->methods));
    C->methods.create_rpc_query = default_create_rpc_query;
    C->methods.forward_target = default_query_forward;
    C->extensions_num = 0;

    static struct rpc_cluster_create _Z;
    struct rpc_cluster_create *Z = &_Z;
    memset (Z, 0, sizeof (*Z));
    int i;
    for (i = 0; i < RPC_FUN_NUM; i++) {
      Z->funs_last[i] = MAX_CLUSTER_FUNS;
      Z->funs[i][--Z->funs_last[i]] = rpc_fun_ok;
    }
    Z->funs[RPC_FUN_QUERY_ON_ANSWER][Z->funs_last[RPC_FUN_QUERY_ON_ANSWER]] = default_fun_on_answer;
    Z->funs[RPC_FUN_QUERY_ON_ALARM][Z->funs_last[RPC_FUN_QUERY_ON_ALARM]] = default_fun_on_alarm;
    Z->funs[RPC_FUN_QUERY_ON_FREE][Z->funs_last[RPC_FUN_QUERY_ON_FREE]] = default_fun_on_free;
    Z->funs[RPC_FUN_ON_NET_FAIL][Z->funs_last[RPC_FUN_ON_NET_FAIL]] = default_on_net_fail;

    if (!(flags & 1)) {
      C->other_cluster_no = -1;
    }

    cfg_getlex ();
    Expect ('{');
    cfg_getlex ();
    int x;
    while (cfg_lex != '}') {
      switch (cfg_lex) {
      case 'i':
        if (C->id) {
          return syntax ("second id in same cluster");
        }
        C->id = cfg_getint();
        if (!C->id) {
          return syntax ("id number expected");
        }
        if (C->id < 0) {
          return syntax("id out of range");
        }
        if (!(flags & 1)) {
          for (i = 0; i < RC->clusters_num - 1; i ++) {
            if (RC->Clusters[i].id == C->id) {
              return syntax ("duplicate id");
            }
          }
          dup_port_checked = 1;
        }
        break;
      case 'm':
        l = cfg_getword ();
        {
          int i;
          int ok = 0;
          for (i = 0; i < extensions_num; i++) {
            char *name = extensions[i]->name;
            if (l == strlen (name) && !memcmp (cfg_cur, name, l)) {
              C->extensions_params[C->extensions_num] = 0;
              C->extensions[C->extensions_num ++] = i;
              ok = 1;
              break;
            }
          }
          if (!ok) {
            return syntax ("Sorry, unsupported mode");
          }
        }
        cfg_cur += l;
        break;
      case 'e':
        l = cfg_getword ();
        {
          int i;
          int ok = 0;
          for (i = 0; i < extensions_num; i++) {
            char *name = extensions[i]->name;
            if (l == strlen (name) && !memcmp (cfg_cur, name, l)) {
              C->extensions_params[C->extensions_num] = 0;
              C->extensions[C->extensions_num ++] = i;              
              ok = 1;
              break;
            }
          }
          if (!ok) {
            return syntax ("Sorry, unsupported extension");
          }
        }
        cfg_cur += l;
        break;
      case 'E':
        l = cfg_getword ();
        {
          int i;
          int ok = 0;
          for (i = 0; i < extensions_num; i++) {
            char *name = extensions[i]->name;
            if (l == strlen (name) && !memcmp (cfg_cur, name, l)) {
              cfg_cur += l;
              l = cfg_getword ();
              C->extensions_params[C->extensions_num] = strndup (cfg_cur, l);
              C->extensions[C->extensions_num ++] = i;              
              ok = 1;
              break;
            }
          }
          if (!ok) {
            return syntax ("Sorry, unsupported extension");
          }
          cfg_cur += l;
        }
        break;
      case 't':
        C->timeout = cfg_getint ();
        if (C->timeout < 10 || C->timeout > 30000) {
          return syntax ("invalid timeout");
        }
        C->timeout /= 1000;
        break;
      case 'p':
        l = cfg_getword ();
        if (C->tot_buckets) {
          return syntax ("proto should be set before server entries\n");
        }
        if (l == 3) {
          if (!memcmp (cfg_cur, "udp", 3)) {
            C->proto = PROTO_UDP;
          } else if (!memcmp (cfg_cur, "tcp", 3)) {
            C->proto = PROTO_TCP;
          } else {
            return syntax ("Unknown proto\n");
          }
        } else {
          return syntax ("Unknown proto\n");
        }
        cfg_cur += l;
        break;
      case 'T':
        C->step = cfg_getint ();
        if (C->step < 0) {
          return syntax ("invalid step value");
        }
        break;
      case 'S':
        l = cfg_getword ();
        if (C->schema_num >= 0) {
          return syntax ("duplicate schema");
        }
        {
          int i;
          int ok = 0;
          for (i = 0; i < schemas_num; i++) {
            char *name = schemas[i]->name;
            if (l == strlen (name) && !memcmp (cfg_cur, name, l)) {
              x = schemas[i]->add (RC, RC_Old, C, Zconf, Z, flags, 0, 0);
              if (x < 0) {
                static char buf[1000];
                snprintf (buf, 999, "Error trying to add schema %s", name);
                return syntax (buf);
              }
              C->schema_num = i;
              C->schema = schemas[i]->name;
              ok = 1;
              break;
            }
          }
          if (!ok) {
            return syntax ("Sorry, unsupported schema");
          }
        }
        cfg_cur += l;
        break;
        
      case 's':
        {
          int x = parse_server (RC, C, flags, cluster_disabled);
          if (x < 0) { return x; }
          break;
        }
      case 'X':
        C->max_connections = cfg_getint ();
        if (C->max_connections < C->min_connections || C->max_connections > 1000) {
          return syntax ("invalid max connections");
        }
        break;
      case 'x':
        C->min_connections = cfg_getint ();
        if (C->min_connections < 1 || C->min_connections > C->max_connections) {
          return syntax ("invalid min connections");
        }
        break;
      case 'n':
        C->new_id = cfg_getint ();
        if (C->new_id < 0) {
          return syntax ("invalid new_id");
        }
        break;
      case 0:
        return syntax ("unexpected end of file");
      default:
        return syntax ("'server' expected");
      }
      cfg_getlex ();
      Expect (';');
      cfg_getlex ();
    }
    if (C->schema < 0) {
      return syntax ("schema expected");
    }
    
    int j;
    for (i = 0; i < C->extensions_num; i++) {
      for (j = i + 1; j < C->extensions_num; j++) if (extensions[C->extensions[j]]->priority < extensions[C->extensions[i]]->priority) {
        int t = C->extensions[i];
        C->extensions[i] = C->extensions[j];
        C->extensions[j] = t;
      }
    }
    for (i = 0; i < C->extensions_num; i++) {
      int x = extensions[C->extensions[i]]->add (RC, RC_Old, C, Zconf, Z, flags, C->extensions_params[i], C->extensions_params[i] ? strlen (C->extensions_params[i]) : 0);
      if (x < 0) {
        char buf[10000];
        snprintf (buf, 9999, "Can not add extension %s", extensions[C->extensions[i]]->name);
        return syntax (buf);
      }
    }
    int pos = 0;
    for (i = 0; i < RPC_FUN_NUM; i++) {
      int size = MAX_CLUSTER_FUNS - Z->funs_last[i];
      if (pos + size > MAX_CLUSTER_FUNS) {
        return syntax ("Too long threaded code");
      }
      memcpy (C->methods.funs + pos, Z->funs[i] + Z->funs_last[i], size * sizeof (rpc_fun_t));
      C->methods.fun_pos[i] = pos;
      pos += size;
    }

    if (!C->tot_buckets && !(C->flags & CF_ALLOW_EMPTY_CLUSTER)) {
      return syntax ("no servers in clusters");
    }
    /*if (!C->id) {
      return syntax ("no id in cluster");
    }*/
    if (!(flags & 1) && !dup_port_checked) {
      for (i = 0; i < RC->clusters_num - 1; i++) {
        if (RC->Clusters[i].id == C->id) {
          return syntax ("duplicate listen port");
        }
      }
    }
    if (!(flags & 1) && RC_Old) {
      for (i = 0; i < RC_Old->clusters_num; i++) {
        if (RC_Old->Clusters[i].id == C->id) {
          RC_Old->Clusters[i].other_cluster_no = C->cluster_no;
          C->other_cluster_no = i;
          break;
        }
      }
    }
    if (!C->methods.forward) {
      return syntax ("Schema not found");
    }
    if ((flags & 1)) {
      for (i = 0; i < C->tot_buckets; i++) {
        if (C->buckets[i].A == ACTOR_ID_UNINIT) {
          C->buckets[i].A = C->new_id;
        }
      }
    }
    if (verbosity > 1) {
      fprintf (stderr, "Cluster #%d (%s) with id %lld, %d servers\n", C->cluster_no, C->cluster_name, C->id, C->tot_buckets);
    }
    if (only_first_cluster && RC->clusters_num >= only_first_cluster) {

      break;
    }
  }
  if (/*!RC->ConfigServersCount ||*/ !RC->clusters_num) {
    return syntax ("no cluster defined");
  }
  int pos = 0;
  for (i = 0; i < RPC_FUN_NUM; i++) {
    int size = MAX_CLUSTER_FUNS - Zconf->funs_last[i];
    if (pos + size > MAX_CLUSTER_FUNS) {
      return syntax ("Too long threaded code");
    }
    memcpy (RC->methods.funs + pos, Zconf->funs[i] + Zconf->funs_last[i], size * sizeof (rpc_fun_t));
    RC->methods.fun_pos[i] = pos;
    pos += size;
  }

  return 0;
}

static int need_reload_config = 0;

int do_reload_config (int create_conn) {
  int res;
  need_reload_config = 0;

  fd[0] = open (config_filename, O_RDONLY);
  if (fd[0] < 0) {
    fprintf (stderr, "cannot re-read config file %s: %m\n", config_filename);
    return -1;
  }

  res = kdb_load_hosts ();
  if (res > 0 && verbosity > 0) {
    fprintf (stderr, "/etc/hosts changed, reloaded\n");
  }

  res = parse_config (NextConf, CurConf, 0);

  close (fd[0]);

  //  clear_config (NextConf);
  
  if (res < 0) {
    vkprintf (0, "error while re-reading config file %s, new configuration NOT applied\n", config_filename);
    return res;
  }

  res = parse_config (NextConf, CurConf, 1);

  if (res < 0) {
    vkprintf (0, "fatal error while re-reading config file %s\n", config_filename);
    clear_config (NextConf, 0);
    exit (-res);
  }

  struct rpc_config *tmp = CurConf;
  CurConf = NextConf;
  NextConf = tmp;

  rpc_cluster_tree = tree_clear_rpc_cluster (rpc_cluster_tree);
  int i;
  for (i = 0; i < CurConf->clusters_num; i++) {
    rpc_cluster_tree = tree_insert_rpc_cluster (rpc_cluster_tree, &CurConf->Clusters[i], lrand48 ());
  }

  clear_config (NextConf, 1);

  if (create_conn) {
    create_all_outbound_connections ();
  }

  CurConf->config_loaded_at = now ? now : time (0);
  CurConf->config_bytes = config_bytes;
  CurConf->config_md5_hex = zmalloc (33);
  md5_hex (config_buff, config_bytes, CurConf->config_md5_hex);
  CurConf->config_md5_hex[32] = 0;

  vkprintf (0, "configuration file %s re-read successfully, new configuration active\n", config_filename);

  return 0;
}

/* }}} */

/* Default split functions {{{ */
/*
 *
 *  PROXY MEMCACHE SERVER FUNCTIONS
 *
 */



struct rpc_cluster_bucket *calculate_key_target (const char *key, int key_len) {
  vkprintf (3, "calculate_key_target: key = %s, key_len = %d\n", key, key_len);
  void *T[3];
  T[0] = (void *)key;
  T[1] = (void *)(long)key_len;
  T[2] = 0;
  int r = RPC_FUN_START(CC->methods.fun_pos[RPC_FUN_STRING_FORWARD], T);
  if (r < 0) { return 0; }
  return T[2];
}

/* }}} */

/* {{{ stats */
int rpc_proxy_prepare_stats (struct connection *c, char *stats_buffer, int stats_buffer_len) {
  dyn_update_stats();
  int stats_len = prepare_stats (c, stats_buffer, stats_buffer_len);
  stats_len += snprintf (stats_buffer + stats_len, stats_buffer_len - stats_len,
    "heap_allocated\t%ld\n"
    "heap_max\t%ld\n"
    "wasted_heap_blocks\t%d\n"
    "wasted_heap_bytes\t%ld\n"
    "free_heap_blocks\t%d\n"
    "free_heap_bytes\t%ld\n"
    "config_filename\t%s\n"
    "config_loaded_at\t%d\n"
    "config_size\t%d\n"
    "config_md5\t%s\n"
    "forwarded_queries\t%lld\n"
    "active_queries\t%lld\n"
    "received_errors\t%lld\n"
    "received_answers\t%lld\n"
    "timeouted_queries\t%lld\n"
    "immediate_errors\t%lld\n"
    "received_expired_answers\t%lld\n"
    "skipped_answers\t%lld\n"
    "sent_answers\t%lld\n"
    "received_bad_answers\t%lld\n"
    "received_bad_queries\t%lld\n"
    "diagonal_queries\t%lld\n"
    "gathers_working\t%d\n"
    "gathers_total\t%lld\n"
    "double_receive_count\t%d\n"
    "ev_heap_size\t%d\n"
    ,
    (long)(dyn_cur - dyn_first),
    (long)(dyn_last - dyn_first),
    wasted_blocks,
    wasted_bytes,
    freed_blocks,
    freed_bytes,
    config_filename,
    CurConf->config_loaded_at,
    CurConf->config_bytes,
    CurConf->config_md5_hex,
    forwarded_queries,
    active_queries,
    received_errors,
    received_answers,
    timeouted_queries,
    immediate_errors,
    received_expired_answers,
    skipped_answers,
    sent_answers,
    received_bad_answers,
    received_bad_queries,
    diagonal_queries,
    gathers_working,
    gathers_total,
    double_receive_count,
    ev_heap_size
  );
  stats_len += snprintf (stats_buffer + stats_len, STATS_BUFF_SIZE - stats_len - 10, 
      "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
     "64-bit"
#else
     "32-bit"
#endif
      " after commit " COMMIT "\n");

  int i;
  for (i = 0; i < schemas_num; i++) if (schemas[i]->stat) {
    int p = schemas[i]->stat (stats_buffer + stats_len, stats_buffer_len - stats_len);
    stats_len += p;
    if (stats_len > stats_buffer_len) {
      stats_len = stats_buffer_len;
    }
  }
  for (i = 0; i < extensions_num; i++) if (extensions[i]->stat) {
    int p = extensions[i]->stat (stats_buffer + stats_len, stats_buffer_len - stats_len);
    stats_len += p;
    if (stats_len > stats_buffer_len) {
      stats_len = stats_buffer_len;
    }
  }
  return stats_len;
}

static char stats_buffer[65536];

int rpc_proxy_stats (struct connection *c) {
  int stats_len = rpc_proxy_prepare_stats (c, stats_buffer, sizeof (stats_buffer) - 6);
  write_out (&c->Out, stats_buffer, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

/* }}} */

/* {{{ Main net methods */
/*int rpc_conn_ready (struct connection *c) {
  server_check_ready (c);
  return c && (c->status == cr_ok || c->status == conn_running || c->status == conn_reading_query) && RPCS_DATA(c)->packet_num >= 0 && c->fd >= 0;
}*/
/* }}} */

/* {{{ Main rpc_query methods */
#define rpc_query_compare(a,b) (a->qid - b->qid)
#define rpc_query_hash(a) (a->qid)
DEFINE_HASH_STD_ALLOC(rpc_query,struct rpc_query *,rpc_query_compare,rpc_query_hash);

struct hash_elem_rpc_query *tarr[(1 << 20)];
struct hash_table_rpc_query rpc_query_hash_table = {
  .size = (1 << 20),
  .E = tarr,
  .mask = (1 << 20) - 1
};

struct rpc_query *get_rpc_query (long long qid) {
  struct hash_elem_rpc_query *T = hash_lookup_rpc_query (&rpc_query_hash_table, (void *)&qid);
  return T ? T->x : 0;
}

void default_on_alarm (struct rpc_query *q);
void delete_rpc_query (struct rpc_query *q);
int query_on_alarm (struct rpc_query *q);
static void rpc_query_timeout (struct rpc_query *q) {
  int t = query_on_alarm (q);
  if (t > 0) { return; }
/*  if (q->type.on_alarm) {
    q->type.on_alarm (q);
  } else {
    default_on_alarm (q);
  }*/
  delete_rpc_query (q);
}

static int rpc_query_timeout_gateway (struct event_timer *ev) {
  vkprintf (2, "Rpc query timeout\n");
  rpc_query_timeout ((struct rpc_query *)(((char *)ev) - offsetof(struct rpc_query,ev)));
  return 0;
}

struct rpc_query *create_rpc_query (long long qid, struct process_id pid, long long old_qid, enum tl_type in_type/*, void *in*/, struct rpc_query_type rpc_query_type, double timeout) {
  struct rpc_query *q = zmalloc (sizeof (*q));
  q->qid = qid;
  q->pid = pid;
  q->old_qid = old_qid;
//  q->in = in;
  q->in_type = in_type;
  q->start_time = precise_now;
  assert (!hash_lookup_rpc_query (&rpc_query_hash_table, q));
  hash_insert_rpc_query (&rpc_query_hash_table, q);
  q->ev.h_idx = 0;
  q->ev.wakeup = rpc_query_timeout_gateway;
  q->ev.wakeup_time = precise_now + (timeout ? timeout : CC->timeout);
  q->type = rpc_query_type;
  insert_event_timer (&q->ev);
  active_queries ++;
  return q;
}

void query_on_free (struct rpc_query *q);
void delete_rpc_query (struct rpc_query *q) {
  query_on_free (q);
}

long long last_qid;
long long get_free_rpc_qid (long long qid) {
  last_qid += lrand48 () ;
  last_qid ++;
  return last_qid;
}
/* }}} */

/* {{{ Default methods */

void default_on_alarm (struct rpc_query *q) {
  timeouted_queries ++;
  rpc_answers_timedout ++;
  if (tl_init_store (q->in_type, /*q->in,*/  &q->pid, q->old_qid) >= 0) {
    tl_fetch_set_error_format (TL_ERROR_QUERY_TIMEOUT, "Query timeout: working_time = %lf", precise_now - q->start_time);
    tl_store_end ();
  }
//  delete_rpc_query (q);
}

void default_on_free (struct rpc_query *q) {
  remove_event_timer (&q->ev); 
  hash_delete_rpc_query (&rpc_query_hash_table, q);
  zfree (q, sizeof (*q));
  active_queries --;
}

void default_on_answer (struct rpc_query *q) {
  if (tl_init_store (q->in_type, /*q->in,*/ &q->pid, q->old_qid) < 0) {
    skipped_answers ++;
    return;
  }
  tl_store_header (CQ->h);
  sent_answers ++;

  //rwm_check (TL_OUT_RAW_MSG);
  tl_copy_through (tl_fetch_unread (), 1);
  //rwm_check (TL_OUT_RAW_MSG);
  tl_store_end_ext (CQ->h->real_op);
}
/*
struct rpc_query_type default_rpc_query_type = {
  .on_answer = default_on_answer,
  .on_alarm = default_on_alarm,
  .on_free = default_on_free
};*/

int default_fun_on_answer (void **IP, void **Data) {
  struct rpc_query *q = *Data;
  default_on_answer (q);
  return 0;
}

int default_fun_on_alarm (void **IP, void **Data) {
  struct rpc_query *q = *Data;
  default_on_alarm (q);
  return 0;
}

int default_fun_on_free (void **IP, void **Data) {
  struct rpc_query *q = *Data;
  default_on_free (q);
  return 0;
}
/* }}} */

int rpc_proxy_store_init (struct rpc_cluster_bucket *B, long long new_qid) {
  void *conn = B->methods->get_conn (B);
  if (!conn) {
    return 0;
  } else {
    B->methods->init_store (B, conn, new_qid);
    return 1;
  }
  /*
  if (CC->proto == PROTO_TCP) {
    struct rpc_target *S = rpc_target_lookup_target (B->T);
    struct connection *d = rpc_target_choose_connection (S, 0);
    if (!d) {
      return 0;
    }
    tl_store_init (d, new_qid);
    return 1;
  } else {
    struct udp_target *S = udp_target_set_choose_target (B->S);
    if (!S || S->state == udp_failed) {
      return 0;
    }
    tl_store_init_raw_msg (S, new_qid);
    return 1;
  }*/
}

void query_on_answer (struct rpc_query *q) {
  remove_event_timer (&q->ev);
  int r = 0;
  if (q->type.on_answer) {
    r = (*q->type.on_answer) (((void **)q->type.on_answer) + 1, (void **)&q);
  } else {
    default_on_answer (q);
  }
  if (r <= 0) {
    delete_rpc_query (q);
  }
}

int query_on_alarm (struct rpc_query *q) {
  if (q->type.on_alarm) {
    return (*q->type.on_alarm) (((void **)q->type.on_alarm) + 1, (void **)&q);
  } else {
    default_on_alarm (q);
    return 0;
  }
}

void query_on_free (struct rpc_query *q) {
  if (q->type.on_free) {
    (*q->type.on_free) (((void **)q->type.on_free) + 1, (void **)&q);
  } else {
    default_on_free (q);
  }
}

struct rpc_query *default_create_rpc_query (long long new_qid) {
  struct rpc_query_type qt;
  qt.on_answer = CC->methods.funs + CC->methods.fun_pos[RPC_FUN_QUERY_ON_ANSWER];
  qt.on_alarm = CC->methods.funs + CC->methods.fun_pos[RPC_FUN_QUERY_ON_ALARM];
  qt.on_free = CC->methods.funs + CC->methods.fun_pos[RPC_FUN_QUERY_ON_FREE];
  return create_rpc_query (new_qid, *CQ->remote_pid, CQ->h->qid, CQ->in_type, /*CQ->in,*/ qt, CQ->h->custom_timeout * 0.001);
}

/* {{{ Default forward methods */

int default_query_forward_conn (struct rpc_cluster_bucket *B, void *conn, long long new_actor_id, long long new_qid, int advance) {
  vkprintf (1, "default_query_forward: CC->id = %lld, CC->timeout = %lf, new_qid = %lld\n", CC->id, CC->timeout, new_qid);
  assert (B);
  assert (conn);
  if (tl_fetch_error ()) {
    return -1;
  }
  CC->forwarded_queries ++;
  forwarded_queries ++;
  long long qid = CQ->h->qid;
  double save_timeout = CQ->h->custom_timeout;
  CQ->h->custom_timeout *= 0.9;

  B->methods->init_store (B, conn, new_qid);

  struct tl_query_header *h = CQ->h;
  assert (h->actor_id == CC->id);
  h->qid = new_qid;
  h->actor_id = new_actor_id;
  tl_store_header (h);
  h->qid = qid;
  h->actor_id = CC->id;
  h->custom_timeout = save_timeout;

  tl_copy_through (tl_fetch_unread (), advance);
  CC->methods.create_rpc_query (new_qid);

  tl_store_end_ext (CQ->h->real_op);
  return 0;
}

int default_query_forward (struct rpc_cluster_bucket *B, long long new_qid) {
  assert (B);
  if (!tl_fetch_error ()) {
    void *conn = B->methods->get_conn (B);
    if (!conn) {
      void *E[2];
      E[0] = B;
      E[1] = &new_qid;
      return RPC_FUN_START(CC->methods.fun_pos[RPC_FUN_ON_NET_FAIL], E);
    } else {
      default_query_forward_conn (B, conn, B->methods->get_actor (B), new_qid, 1);
      return 0;
    }
  } else {
    return -1;
  }
}

int default_query_diagonal_forward (void) {
  diagonal_queries ++;
  merge_forward (&diagonal_gather_methods);
  return 0;
}

int query_forward (struct rpc_cluster_bucket B) {
  assert (CC->methods.forward_target); 
  long long new_qid = get_free_rpc_qid (CQ->h->qid);
  return CC->methods.forward_target (&B, new_qid);
}

int default_firstint_forward_ext (void) {
  tl_fetch_int ();
  int n = tl_fetch_int ();
  if (n < 0) { n = -n; }
  if (n < 0) { n = 0; }
  if (tl_fetch_error ()) {
    tl_fetch_mark_delete ();
    return -1;
  }
  if (CC->step) {
    n /= CC->step;
  }
  assert (CC->tot_buckets);
  n %= CC->tot_buckets;
  tl_fetch_mark_restore ();
  return query_forward (CC->buckets[n]);
}
int default_firstint_forward (void) {
  tl_fetch_mark ();
  return default_firstint_forward_ext ();
}

int default_firstlong_forward_ext (void) {
  tl_fetch_int ();
  long long n = tl_fetch_long ();
  if (n < 0) { n = -n; }
  if (n < 0) { n = 0; }
  if (tl_fetch_error ()) {
    tl_fetch_mark_delete ();
    return -1;
  }
  if (CC->step) {
    n /= CC->step;
  }
  assert (CC->tot_buckets);
  n %= CC->tot_buckets;
  tl_fetch_mark_restore ();
  return query_forward (CC->buckets[n]);
}
int default_firstlong_forward (void) {
  tl_fetch_mark ();
  return default_firstlong_forward_ext ();
}

int default_unsigned_firstlong_forward_ext (void) {
  tl_fetch_int ();
  unsigned long long n = tl_fetch_long ();
  //#  if (n < 0) { n = -n; }
  //#  if (n < 0) { n = 0; }
  if (tl_fetch_error ()) {
    tl_fetch_mark_delete ();
    return -1;
  }
  if (CC->step) {
    n /= CC->step;
  }
  assert (CC->tot_buckets);
  n %= CC->tot_buckets;
  tl_fetch_mark_restore ();
  return query_forward (CC->buckets[n]);
}
int default_unsigned_firstlong_forward (void) {
  tl_fetch_mark ();
  return default_unsigned_firstlong_forward_ext ();
}


int default_random_forward_ext (void) {
  int n = lrand48 ();
  if (tl_fetch_error ()) {
    tl_fetch_mark_delete ();
    return -1;
  }
  assert (CC->tot_buckets);
  assert (n >= 0);
  n %= CC->tot_buckets;
  tl_fetch_mark_restore ();
  return query_forward (CC->buckets[n]);
}
int default_random_forward (void) {
  tl_fetch_mark ();
  return default_random_forward_ext ();
}

int default_vector_forward_ext (void) {
  tl_fetch_int (); //op
  int x = (CC->cluster_mode & 7);
  x --;
  if (x < 0) { x = 0; }
  int n = tl_fetch_int (); // n
  if (x > n - 1) { x = n - 1; }
  int i;
  for (i = 0; i < x; i++) {
    tl_fetch_int ();
  }
  n = tl_fetch_int ();
  if (n < 0) { n = -n; }
  if (n < 0) { n = 0; }
  if (tl_fetch_error ()) {
    tl_fetch_mark_delete ();
    return -1;
  }
  if (CC->step) {
    n /= CC->step;
  }
  assert (CC->tot_buckets);
  n %= CC->tot_buckets;
  tl_fetch_mark_restore ();
  return query_forward (CC->buckets[n]);
}
int default_vector_forward (void) {
  tl_fetch_mark ();
  return default_vector_forward_ext ();
}

int default_tuple_forward_ext (int size) {
  tl_fetch_int (); //op
  int x = (CC->cluster_mode & 7);
  x --;
  if (x < 0) { x = 0; }
  if (x > size - 1) { x = size - 1; }
  int i;
  for (i = 0; i < x; i++) {
    tl_fetch_int ();
  }
  int n = tl_fetch_int ();
  if (n < 0) { n = -n; }
  if (n < 0) { n = 0; }
  if (tl_fetch_error ()) {
    tl_fetch_mark_delete ();
    return -1;
  }
  if (CC->step) {
    n /= CC->step;
  }
  assert (CC->tot_buckets);
  n %= CC->tot_buckets;
  tl_fetch_mark_restore ();
  return query_forward (CC->buckets[n]);
}
int default_tuple_forward (int size) {
  tl_fetch_mark ();
  return default_tuple_forward_ext (size);
}

int default_string_forward_ext (void) {
  tl_fetch_int (); //op
  int key_len;
  char key[MAX_KEY_LEN + 1];
  key_len = tl_fetch_string0 (key, MAX_KEY_LEN);
  if (key_len < 0) {
    tl_fetch_mark_delete ();
    return -1;
  }
  tl_fetch_mark_restore ();
  struct rpc_cluster_bucket *B = calculate_key_target (key, key_len);
  if (!B) {
    tl_fetch_set_error_format (TL_ERROR_PROXY_NO_TARGET, "Can not find target for key %.*s%s", key_len <= 30 ? key_len : 27, key, key_len <= 30 ? "" : "...");
    return -1;
  }
  return query_forward (*B);
}
int default_string_forward (void) {
  tl_fetch_mark ();
  return default_string_forward_ext ();
}
/* }}} */

/* Threaded functions  {{{ */

int rpc_fun_ok (void **IP, void **Data) {
  return 0;
}

int default_on_net_fail (void **IP, void **Data) {
  tl_fetch_set_error ("Can not find working connection to target", TL_ERROR_NO_CONNECTIONS);
  return -1;
}
/* }}} */

struct rpc_cluster *get_cluster_by_id (long long id) {
  struct rpc_cluster **T = tree_lookup_value_rpc_cluster (rpc_cluster_tree, (void *)&id);
  return T ? *T : 0;
}

void _fail_query (enum tl_type type, struct process_id *PID, long long qid) {
  if (PID) {
    if (tl_init_store_keep_error (type, PID, qid) >= 0) {
      if (!tl_fetch_error ()) {
        tl_fetch_set_error ("Unknown error", TL_ERROR_UNKNOWN);
      }
      tl_store_end ();
    }
  }
}

int *__cluster_count;

void dump_cluster_id (struct rpc_cluster *C) {
  tl_store_long (C->id);
  (*__cluster_count) ++;
}

void tl_dump_clusters (void) {
  __cluster_count = tl_store_get_ptr (4);
  *__cluster_count = 0;
  tree_act_rpc_cluster (rpc_cluster_tree, dump_cluster_id);
}

void store_cluster_stats (struct rpc_cluster *C) {
  int stats_len = 0;
  static char buf[1 << 15];
  int i;
  int pos = 0;
  for (i = 0; i < C->extensions_num; i++) {
    if (i != 0) {
      buf[pos ++] = ',';
    }
    const char *s = extensions[C->extensions[i]]->name;
    int len = strlen (s);
    pos += sprintf (buf + pos, "%.*s", len > 100 ? 100 : len, s);
  }

  stats_len += snprintf (stats_buffer + stats_len, STATS_BUFF_SIZE - stats_len,
    "name\t%s\n"
    "id\t%lld\n"
    "new_id\t%lld\n"
    "tot_buckets\t%d\n"
    "schema\t%s\n"
    "extensions\t%s\n"
    "forwarded_queries\t%lld\n"
    ,
    C->cluster_name,
    C->id,
    C->new_id,
    C->tot_buckets,
    C->schema,
    buf,
    C->forwarded_queries
  );

  tl_store_stats (stats_buffer, 1);
}

void store_stats (void) {
  rpc_proxy_prepare_stats (0, stats_buffer, STATS_BUFF_SIZE - 2);
  tl_store_stats (stats_buffer, 0);
}

int do_forward (void) {
  int op = tl_fetch_lookup_int ();
  long long t;
  struct rpc_cluster *C;
  int i;
  switch (op) {
  case TL_ENGINE_NOP:
    tl_fetch_int ();
    tl_fetch_end ();
    //tl_store_init_any_keep_error (CQ->in_type, CQ->in, CQ->h->qid);
    tl_init_store (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    tl_store_int (TL_TRUE);
    tl_store_end ();
    return 0;
  case TL_RPC_PROXY_GET_CLUSTERS:
    tl_fetch_int ();
    tl_fetch_end ();
    //tl_store_init_any_keep_error (CQ->in_type, CQ->in, CQ->h->qid);
    tl_init_store (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    tl_dump_clusters ();
    tl_store_end ();
    return 0;
  case TL_RPC_PROXY_GET_CLUSTER_SIZE:
    tl_fetch_int ();
    t = tl_fetch_long ();
    tl_fetch_end ();
    //tl_store_init_any_keep_error (CQ->in_type, CQ->in, CQ->h->qid);
    tl_init_store (CQ->in_type, CQ->remote_pid, CQ->h->qid);

    C = get_cluster_by_id (t);
    if (!C) {
      tl_store_int (TL_MAYBE_FALSE);
    } else {
      tl_store_int (TL_MAYBE_TRUE);
      tl_store_int (C->tot_buckets);
    }
    tl_store_end ();
    return 0;
  case TL_RPC_PROXY_GET_CLUSTER_SERVERS:
    tl_fetch_int ();
    t = tl_fetch_long ();
    tl_fetch_end ();
    //tl_store_init_any_keep_error (CQ->in_type, CQ->in, CQ->h->qid);
    tl_init_store (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    C = get_cluster_by_id (t);
    if (!C) {
      tl_store_int (TL_MAYBE_FALSE);
    } else {
      tl_store_int (TL_MAYBE_TRUE);
      tl_store_int (C->tot_buckets);
      for (i = 0; i < C->tot_buckets; i++) {
        struct rpc_cluster_bucket *B = &C->buckets[i];
        tl_store_int (B->methods->get_host (B));
        tl_store_int (B->methods->get_port (B)); 
        tl_store_long (B->methods->get_actor (B)); 
      }
    }
    tl_store_end ();
    return 0;
  case TL_RPC_PROXY_GET_CLUSTER_STATS:
    tl_fetch_int ();
    t = tl_fetch_long ();
    tl_fetch_end ();
    //tl_store_init_any_keep_error (CQ->in_type, CQ->in, CQ->h->qid);
    tl_init_store (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    C = get_cluster_by_id (t);
    if (!C) {
      tl_store_int (TL_MAYBE_FALSE);
    } else {
      tl_store_int (TL_MAYBE_TRUE);
      store_cluster_stats (C);
    }
    tl_store_end ();
    return 0;
  case TL_RPC_PROXY_GET_BAD_TARGETS:
    tl_fetch_int ();
    tl_fetch_end ();
    //tl_store_init_any_keep_error (CQ->in_type, CQ->in, CQ->h->qid);
    tl_init_store (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    tl_store_int (TL_VECTOR);
    {
      int *count = tl_store_get_ptr (4);
      *count = 0;
      int i;
      for (i = 0; i < allocated_targets; i++) {
        struct conn_target *S = &Targets[i];
        assert (S);
        if (S->min_connections > 0 && !S->ready_outbound_connections) {
          if (S->target.s_addr) {
            int x =  (S->target.s_addr);            
            tl_store_int (ntohl (x));
          } else {
            tl_store_int (S->custom_field);
          }
          tl_store_int (S->port);
          (*count) ++;
        }
      }
    }
    tl_store_end ();
    return 0;
  case TL_ENGINE_STAT:
    tl_fetch_int ();
    tl_fetch_end ();
    //tl_store_init_any_keep_error (CQ->in_type, CQ->in, CQ->h->qid);
    tl_init_store (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    store_stats ();
    tl_store_end ();
    return 0;
  }
  int x;
  x = RPC_FUN_START(CC->methods.fun_pos[RPC_FUN_PREFORWARD_CHECK], 0);
  if (x < 0) { return x; }
  x = RPC_FUN_START(CC->methods.fun_pos[RPC_FUN_PREFORWARD_EDIT], 0);
  if (x < 0) { return x; }
  x = RPC_FUN_START(CC->methods.fun_pos[RPC_FUN_PREFORWARD_VIEW], 0);
  if (x < 0) { return x; }
  return CC->methods.forward ();
}

int on_receive (void) {
  return RPC_FUN_START(CC->methods.fun_pos[RPC_FUN_ON_RECEIVE], 0);
}


/* Extension clear_flags {{{ */

int do_clear_flags_extension (void **IP, void **Data) {
  CQ->h->flags &= 0x0000ffff;
  RPC_FUN_NEXT;
}

EXTENSION_ADD(clear_flags) {
  Z->lock |= (1 << RPC_FUN_PREFORWARD_EDIT);
  assert (Z->funs_last[RPC_FUN_PREFORWARD_EDIT] > 0);
  Z->funs[RPC_FUN_PREFORWARD_EDIT][--Z->funs_last[RPC_FUN_PREFORWARD_EDIT]] = do_clear_flags_extension;
  return 0;
}
EXTENSION_REGISTER(clear_flags,2)

/* }}} */

void client_execute (void) {
  if (CQ->h) {
    tl_query_header_delete (CQ->h);
    CQ->h = 0;
  }
  CQ->h = zmalloc (sizeof (*(CQ->h)));
  if (tl_fetch_query_answer_header (CQ->h) < 0) {
    received_bad_answers ++;
    vkprintf (3, "Can not fetch header\n");
    skipped_answers ++;
    CQ->fail_query (0, 0, 0);
    return;
  }
  if (CQ->h->op == RPC_REQ_ERROR || CQ->h->op == RPC_REQ_ERROR_WRAPPED) { received_errors ++; }
  else { received_answers ++; }
  
  struct rpc_query *q = get_rpc_query (CQ->h->qid);
  if (!q) {
    received_expired_answers ++;
    skipped_answers ++;
    CQ->fail_query (0, 0, 0);
    vkprintf (3, "Answer for unknown query (qid = %lld)\n", CQ->h->qid);
  } else {
    query_on_answer (q);
    vkprintf (3, "Query end\n");
  }
}

void server_execute (void) {
  if (CQ->h) {
    tl_query_header_delete (CQ->h);
    CQ->h = 0;
  }
  CQ->h = zmalloc (sizeof (*(CQ->h)));
  if (tl_fetch_query_header (CQ->h) < 0) {
    received_bad_queries ++;
    vkprintf (3, "Can not parse header: CQ->h.qid = %lld\n", CQ->h->qid);
    CQ->fail_query (CQ->in_type, CQ->h->qid ? CQ->remote_pid : 0, CQ->h->qid);
    return;
  }

  CC = get_cluster_by_id (CQ->h->actor_id);   
  vkprintf (2, "Got query: actor_id = %lld, unread_after_header: %d, flags = %x, schema = %s\n", CQ->h->actor_id, tl_fetch_unread (), CQ->h->flags, CC ? CC->schema : "unknown");
  if (!CC) {
    vkprintf (3, "Can not find cluster with actor_id = %lld\n", CQ->h->actor_id);
    tl_fetch_set_error_format (TL_ERROR_WRONG_ACTOR_ID, "Can not find actor with id %lld", CQ->h->actor_id);
    CQ->fail_query (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    return;
  }

  int r = on_receive ();
  if (r == -1) {
    return;
  }
  if (r == -2) {
    CQ->fail_query (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    return;
  }
  if (r == -3) {
    return;
  }

  int result = -1;
  if (CC->flags & CF_FORBID_FORCE_FORWARD) {
    result = do_forward ();
  } else {
    assert (CC->methods.forward_target);
    if (CQ->h->int_forward_keys_pos < CQ->h->int_forward_keys_num) {
      int n = CQ->h->int_forward_keys[CQ->h->int_forward_keys_pos ++];
      if (n < 0) { n = -n; }
      if (n < 0) { n = 0; }
      assert (CC->tot_buckets);
      n %= CC->tot_buckets;
      result = query_forward (CC->buckets[n]);
    } else if (CQ->h->string_forward_keys_pos < CQ->h->string_forward_keys_num && (CC->flags & CF_ALLOW_STRING_FORWARD)) {
      char *key = CQ->h->string_forward_keys[CQ->h->string_forward_keys_pos ++];
      assert (key);
      int key_len = strlen (key);

      struct rpc_cluster_bucket *B = calculate_key_target (key, key_len);
      result = B ? query_forward (*B) : -1;
    } else if (CQ->h->flags & TL_QUERY_HEADER_FLAG_INT_FORWARD) {
      int n = CQ->h->int_forward;
      if (n < 0) { n = -n; }
      if (n < 0) { n = 0; }
      assert (CC->tot_buckets);
      n %= CC->tot_buckets;
      result = query_forward (CC->buckets[n]);
    } else if (CQ->h->flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD && (CC->flags & CF_ALLOW_STRING_FORWARD)) {
      char *key = CQ->h->string_forward;
      assert (key);
      int key_len = strlen (key);
      struct rpc_cluster_bucket *B = calculate_key_target (key, key_len);
      result = B ? query_forward (*B) : -1;
    } else {
      result = do_forward ();    
    }
  }

  if (result < 0) {
    CQ->fail_query (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    vkprintf (3, "Forward methods failed.\n");
    return;
  } else {
    vkprintf (3, "Forwarded successfully\n");
    return;
  }
}

void custom_op_execute (int op) {
  void *T[3];
  T[0] = (void *)(long)op;
  RPC_CONF_FUN_START(CurConf->methods.fun_pos[RPC_FUN_CUSTOM_OP], T);
}

void common_execute (int op) {
  switch (op) {
  case RPC_REQ_RESULT:
  case RPC_REQ_ERROR:
    client_execute ();
    return;
  case RPC_INVOKE_REQ:
  case RPC_INVOKE_KPHP_REQ:
    server_execute ();
    return;
  case RPC_REQ_RUNNING:
  case RPC_PONG:
    return;
  default:
    custom_op_execute (op);
    return;
  }
}

int rpc_proxy_server_execute (struct connection *c, int op, int len) {
  vkprintf (2, "Function %s: c = %d, op = 0x%08x, len = %d\n", __func__, c ? c->fd : -1, op, len);
  c->last_response_time = precise_now;

  CQ->extra = c;
  //CQ->in = c;  
  CQ->in_type = tl_type_conn;
  CQ->remote_pid = &RPCC_DATA(c)->remote_pid;
  CQ->fail_query = (void *)_fail_query;
  
  rpc_target_insert_conn (c);
  nb_iterator_t _R;
  nbit_set (&_R, &c->In);
  tl_fetch_init_iterator (&_R, len - 4);

  common_execute (op);
  return SKIP_ALL_BYTES;
}

int rpc_proxy_client_execute (struct connection *c, int op, int len) {
  vkprintf (2, "Function %s: c = %d, op = 0x%08x, len = %d\n", __func__, c ? c->fd : -1, op, len);
  c->last_response_time = precise_now;
  
  CQ->extra = c;
  //CQ->in = c;
  CQ->in_type = tl_type_conn;
  CQ->remote_pid = &RPCC_DATA(c)->remote_pid;
  CQ->fail_query = (void *)_fail_query;
  
  rpc_target_insert_conn (c);
  nb_iterator_t _R;
  nbit_set (&_R, &c->In);
  tl_fetch_init_iterator (&_R, len - 4);

  common_execute (op);
  return SKIP_ALL_BYTES;
}

int rpc_proxy_udp_execute (struct udp_msg *msg) {
  vkprintf (2, "Function %s: PID = " PID_PRINT_STR ", len = %d\n", __func__, PID_TO_PRINT (&msg->S->PID), msg->raw.total_bytes);
  if (msg->raw.total_bytes < 4) { return 0; }
  struct raw_message m;
  rwm_clone (&m, &msg->raw);
  
  //CQ->in = (void *)msg->S;
  CQ->in_type = tl_type_raw_msg;
  CQ->remote_pid = &msg->S->PID;
  CQ->fail_query = (void *)_fail_query;
  
  tl_fetch_init_raw_message (&m, msg->raw.total_bytes);
  rwm_free (&m);
  int op = tl_fetch_lookup_int ();

  common_execute (op);
  return 0;
}

int rpc_proxy_tcp_execute (struct connection *c, int op, struct raw_message *msg) {
  vkprintf (2, "Function %s: c = %d, op = 0x%08x, len = %d\n", __func__, c ? c->fd : -1, op, msg->total_bytes);
  c->last_response_time = precise_now;
  rpc_target_insert_conn (c);
  if (msg->total_bytes < 4) { return 0; }

  struct raw_message m;
  rwm_clone (&m, msg);
  
  //CQ->in = (void *)msg->S;
  CQ->in_type = tl_type_tcp_raw_msg;
  CQ->remote_pid = &(TCP_RPC_DATA(c)->remote_pid);
  CQ->fail_query = (void *)_fail_query;

  
  tl_fetch_init_tcp_raw_message (&m, msg->total_bytes);
  rwm_free (&m);

  common_execute (op);
  return 0;
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
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_immediate_handler (const int sig) {
  fprintf (stderr, "SIGINT handled immediately.\n");
  //  flush_binlog_last();
  //  sync_binlog (2);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled immediately.\n");
  //  flush_binlog_last();
  //  sync_binlog (2);
  exit (1);
}

static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  pending_signals |= (1 << SIGINT);
  signal(SIGINT, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  pending_signals |= (1 << SIGTERM);
  signal(SIGTERM, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  // sync_binlog (2);
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs();
  // sync_binlog (2);
  signal(SIGUSR1, sigusr1_handler);
}


static void sigusr2_handler (const int sig) {
  if (verbosity > 0) {
    fprintf (stderr, "got SIGUSR2, config reload scheduled.\n");
  }
  need_reload_config++;
  signal (SIGUSR2, sigusr2_handler);
}

void read_binlog (const char *name) __attribute__ ((weak));
void read_binlog (const char *name) {
}

void write_index (void) __attribute__ ((weak));
void write_index (void) {
}

void flush_index (void) __attribute__ ((weak));
void flush_index (void) {
}

void cron (void) {
  dyn_garbage_collector ();
  if (binlog_mode_on) {
    flush_cbinlog (0);
    write_index ();
  }
}

int usfd;

void start_server (void) { 
  //  struct sigaction sa;
  int i;
  int prev_time;
  double next_create_outbound = 0;

  init_epoll();
  init_netbuffers();
  init_msg_buffers (0);

  prev_time = 0;

  if (daemonize) {
    setsid ();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }
  
  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, enable_ipv6 );
  }

  if (sfd < 0) {
    fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  if (!usfd) {
    usfd = server_socket (port, settings_addr, backlog, enable_ipv6 + 1);
  }

  if (usfd < 0) {
    fprintf (stderr, "cannot open udp port: %m\n");
    exit (1);
  }

  if (!tcp_buffers) {
    assert (init_listening_tcpv6_connection (sfd, &ct_rpc_server, &rpc_proxy_inbound, enable_ipv6) >= 0);
  } else {
    assert (init_listening_tcpv6_connection (sfd, &ct_tcp_rpc_server, &rpc_proxy_tcp, enable_ipv6) >= 0);
  }
 
  assert ((default_udp_socket = init_udp_port (usfd, port, &rpc_proxy_udp_server, &rpc_proxy_udp_server_methods, enable_ipv6)));

  get_utime_monotonic ();

  create_all_outbound_connections ();

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGUSR2, sigusr2_handler);
  signal (SIGPIPE, SIG_IGN);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);
    if (precise_now > next_create_outbound) {
      create_all_outbound_connections ();
      next_create_outbound = precise_now + 0.05 + 0.02 * drand48();
    }
    if (now != prev_time) {
      prev_time = now;
      cron ();
    }
    if (pending_signals) {
      break;
    }
    if (need_reload_config) {
      do_reload_config (1);
    }
    if (binlog_mode_on) {
      flush_cbinlog (0);
    }
    if (quit_steps && !--quit_steps) break;
  }
  
  epoll_close (sfd);
  close(sfd);

  if (binlog_mode_on) {
    flush_cbinlog (2);
    flush_index ();
  }
  kprintf ("Terminated (pending_signals = 0x%llx).\n", pending_signals);

}


/*
 *
 *    MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [options] cluster-name\n "
          "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
          "64-bit"
#else
          "32-bit"
#endif
          " after commit " COMMIT "\n"
          "\tRedistributes rpc queries to servers listed in <cluster-descr-file>\n",
/*          "\t-v\toutput statistical and debug information into stderr\n"
          "\t-f\tload only first cluster\n"
          "\t-X\tenable specified extension mode (allowed modes: text, lists, hints, logs, magus, watchcat, news, roundrobin, dot, search, statsx, friends, target, news_ug, news_comm, searchx, newsr)\n"
          "\t-H<heap-size>\tdefines maximum heap size\n"
          "\t-P\tpath to file with AES encryption key\n"
          "\t-C\t+1 do not check crc32, +2 do not send crc32\n"
          "\t-y\tcustom ping interval (default %lf)\n"
          "\t-B<binlog name>\tenable binlog mode\n"
          "\t-F\tflush binlog after each query\n"
          "\t-S\tbinlog slice size (default %lld)\n"
          ,*/
          progname
//          PING_INTERVAL,
//          max_binlog_size
          );
  parse_usage ();
  exit(2);
}

char *bname;
int f_parse_option (int val) {
  char c;
  long long x;
  switch (val) {
  case 'f':
    only_first_cluster ++;
    return 0;
  case 'T':
    ++test_mode;
    return 0;
  case 'N':
    tcp_buffers ++;
    return 0;
  case 'C':
    rpc_crc32_mode = atoi (optarg);
    rpc_disable_crc32_check = (rpc_crc32_mode & 1);
    return 0;
  case 'H': case 'S':
    c = 0;
    assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
    switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
    }
    if (val == 'H') {
      if (x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (100LL << 30))) {
        dynamic_data_buffer_size = x;
      }
    } else if (val == 'S') {
      max_binlog_size = x;
    }
    break;
  case 'y':
    ping_interval = atof (optarg);
    if (ping_interval <= 0) { ping_interval = PING_INTERVAL; }
    break;
  case 'B':
    bname = optarg;
    binlog_cyclic_mode = 1;
    binlog_mode_on |= 1;
    //max_binlog_size = 100000;
    break;
  case 'F':
    binlog_mode_on |= 2;
    break;
  case 'Q':
    vv_tl_drop_probability = atof (optarg);
    break;
  case 'n':
    errno = 0;
    nice (atoi (optarg));
    if (errno) {
      perror ("nice");
    }
    break;
  default:
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  rpc_disable_crc32_check = 1;
  int i;

  set_debug_handlers ();

  progname = argv[0];

  remove_parse_option ('B');
  parse_option ("first-cluster-only", required_argument, 0, 'f', "only first cluster");
  parse_option ("test-mode", no_argument, 0, 'T', 0);
  parse_option ("tcp-buffers", no_argument, 0, 'N', "new tcp buffers");
  parse_option ("crc32-mode", required_argument, 0, 'C', "crc32 mode: bit 0 disables crc32 check, bit 1 disables crc32 send (sends 0)");
  parse_option ("heap-size", required_argument, 0, 'H', "sets heap size. Supports K/M/G/T modifiers");
  parse_option ("max-binlog-size", required_argument, 0, 'S', "sets maximal binlog slice size. Supports K/M/G/T modifiers");
  parse_option ("ping-interval", required_argument, 0, 'y', "sets ping interval (only in tcp connections) (default %lf)", PING_INTERVAL);
  parse_option ("binlog-enable", required_argument, 0, 'B', "enables binlog mode. argument is binlog name");
  parse_option ("drop-probability", required_argument, 0, 'Q', "sets probability of dropping transmitted packet");
  parse_option (0, required_argument, 0, 'n', "sets niceness");

  parse_engine_options_long (argc, argv, f_parse_option);
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  PID.port = port;
  init_dyn_data ();
  init_server_PID (get_my_ipv4 (), port);

  if (raise_file_rlimit (maxconn + 16) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  config_filename = argv[optind];

  i = do_reload_config (0);

  if (i < 0) {
    fprintf (stderr, "config check failed!\n");
    return -i;
  }

  vkprintf (1, "config loaded!\n");

  aes_load_pwd_file (aes_pwd_file);

  if (port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, enable_ipv6);
    if (sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
    usfd = server_socket (port, settings_addr, backlog, enable_ipv6 + 1);
    if (usfd < 0) {
      kprintf ("cannot open udp server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  if (bname) {
    read_binlog (bname);
  }
  start_time = time (0);

  start_server ();

  return 0;
}

