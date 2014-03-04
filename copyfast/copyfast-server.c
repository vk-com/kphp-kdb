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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <signal.h>

#include "md5.h"
#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-rpc-server.h"
#include "net-crypto-aes.h"

#include "kdb-data-common.h"
#include "copyfast-rpc-layout.h"
#include "copyfast-common.h"

#include "copyfast-server.h"

#define VERSION_STR	"copyfast-server-0.32"

#define TCP_PORT 23917

#define MAX_NET_RES	(1L << 16)

#define LINE vkprintf (0, "at line %d\n", __LINE__)

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#ifdef __LP64__ 
#define __M__ "64-bit"
#else
#define __M__ "32-bit"
#endif
#define FULL_VERSION      "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " " __M__  " after commit " COMMIT "\n"

int verbosity, interactive, quit_steps, start_time;

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;

char *progname = "copyfast-server", *username, *binlogname, *logname;
char metaindex_fname_buff[256], binlog_fname_buff[256];

#define DEFAULT_SLOW_REQUEST_DELAY 1
#define DEFAULT_MEDIUM_REQUEST_DELAY 0.1
double slow_delay = DEFAULT_SLOW_REQUEST_DELAY;
double medium_delay = DEFAULT_MEDIUM_REQUEST_DELAY;

#define STATS_BUFF_SIZE	(1 << 20)
#define STOP_STATS (7 << 17)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];

int rpcs_execute (struct connection *c, int op, int len);

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

struct memcache_server_functions copyfast_server_memcache_inbound = {
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
  .memcache_fallback_extra = &copyfast_server_memcache_inbound
};

struct node_extra {
  double timestamp;
  long long binlog_position;
};

struct engine_node {
  struct node node;
  struct node_extra extra;
  union engine_stats stats;
  int time_requested;
  struct connection *connection;
  int generation;
  int protocol_version;
};


/*
 *
 * Tree structure
 *
 */

int alloc_tree_nodes;

static tree_t *new_tree_node (long long x, int y) {
  tree_t *P;
  P = zmalloc (sizeof (tree_t));
  assert (P);
  alloc_tree_nodes++;
  P->left = P->right = 0;
  P->size = 0;
  P->x = x;
  P->y = y;
  return P;
}

/*static void free_tree_node (tree_t *T) {
  zfree (T, sizeof (tree_t));
  alloc_tree_nodes--;
}*/

static tree_t *tree_lookup (tree_t *T, long long x) {
  while (T && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

static tree_t *tree_get (tree_t *T, int p) {
  if (!T) {
    return 0;
  }
  if (T->left && p < T->left->size) {
    return tree_get (T->left, p);
  }
  if (p == (T->left ? T->left->size : 0)) {
    return T;
  }
  return tree_get (T->right, p - 1 - (T->left ? T->left->size : 0));
}

static void tree_split (tree_t **L, tree_t **R, tree_t *T, long long x) {
  if (!T) { *L = *R = 0; return; }
  if (x < T->x) {
    *R = T;
    tree_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_split (&T->right, R, T->right, x);
  }
  T->size = 1 + (T->left ? T->left->size : 0) + (T->right ? T->right->size : 0);
}

static tree_t *tree_insert (tree_t *T, long long x, int y, long long data) {
  tree_t *P;
  if (!T) { 
    P = new_tree_node (x, y);
    P->data = data;
    P->size = 1;
    return P;
  }
  assert (x != T->x);
  if (T->y >= y) {
    if (x < T->x) {
      T->left = tree_insert (T->left, x, y, data);
    } else {
      T->right = tree_insert (T->right, x, y, data);
    }
    T->size = 1 + (T->left ? T->left->size : 0) + (T->right ? T->right->size : 0);
    return T;
  }
  P = new_tree_node (x, y);
  P->data = data;
  tree_split (&P->left, &P->right, T, x);
  P->size = 1 + (P->left ? P->left->size : 0) + (P->right ? P->right->size : 0);
  return P;
}

static tree_t *tree_merge (tree_t *L, tree_t *R) {
  if (!L) { return R; }
  if (!R) { return L; }
  assert (L->x < R->x);
  if (L->y > R->y) {
    L->right = tree_merge (L->right, R);
    L->size = 1 + (L->left ? L->left->size : 0) + (L->right ? L->right->size : 0);
    return L;
  } else {
    R->left = tree_merge (L, R->left);
    R->size = 1 + (R->left ? R->left->size : 0) + (R->right ? R->right->size : 0);
    return R;
  }
}

/*static tree_t *tree_delete (tree_t *T, long long x) {
  if (T->x == x) {
    tree_t *N = tree_merge (T->left, T->right);
    free_tree_node (T);
    return N;
  }
  if (x < T->x) {
    T->left = tree_delete (T->left, x);
  } else {
    T->right = tree_delete (T->right, x);
  }
  T->size = 1 + (T->left ? T->left->size : 0) + (T->right ? T->right->size : 0);
  return T;
}

static void free_tree (tree_t *T) {
  if (T) {
    free_tree (T->left);
    free_tree (T->right);
    free_tree_node (T);
  }
}*/

tree_t *id_tree, *nodes_tree, *cluster_tree;

#define MAX_NODES 1000000
struct engine_node nodes[MAX_NODES];
int nodes_num;
#define MAX_CLUSTERS 1000
long long clusters[MAX_CLUSTERS];
int clusters_num;

void add_cluster (long long id) {
  vkprintf (2, "adding cluster with id = %lld\n", id & CLUSTER_MASK);
  clusters[clusters_num ++] = id & CLUSTER_MASK;
  cluster_tree = tree_insert (cluster_tree, id & CLUSTER_MASK, lrand48 (), clusters_num - 1);
}

long long tree_id (unsigned host, int port, unsigned long long id) {
  assert (port >= 0 && port < (1 << 16));
  tree_t *T = tree_lookup (cluster_tree, id & CLUSTER_MASK);
  if (!T) {
    add_cluster (id);
    assert (T = tree_lookup (cluster_tree, id & CLUSTER_MASK));
  }
  assert (T->data >= 0 && T->data < (1 << 16));
  return ((T->data) << 48) + (((unsigned long long) host) << 16) + port;
  
}

union engine_stats min_stats, max_stats, sum_stats;


long long generate_id (long long x) {
  while (1) {
    int ok = ((x & ID_MASK) != 0);
    if (ok && tree_lookup (id_tree, x)) {
      ok = 0;
    }
    if (ok) {
      return x;
    }
    x = (x & CLUSTER_MASK) | (unsigned) lrand48();
  }
  return 0;
}

long long add_node (int host, int port, long long id, long long binlog_position, struct connection *c, int protocol_version) {
  assert (nodes_num < MAX_NODES);
  assert (c);
  long long x = tree_id (host, port, id);
  tree_t *T = tree_lookup (nodes_tree, x);
  if (T) {
    struct engine_node *node = &nodes[T->data];
    node->extra.binlog_position = binlog_position;
    node->extra.timestamp = precise_now;
    node->connection = c;
    node->generation = c->generation;
    node->protocol_version = protocol_version;
    return node->node.id;
  }
  if (nodes_num == MAX_NODES) {
    return 0;
  }
  id = generate_id (id);
  struct engine_node *node = &nodes[nodes_num ++];
  node->node.host = host;
  node->node.port = port;
  node->node.id = id;
  node->extra.binlog_position = binlog_position;
  node->extra.timestamp = precise_now;
  node->connection = c;
  node->generation = c->generation;
  node->protocol_version = protocol_version;
  nodes_tree = tree_insert (nodes_tree, x, lrand48 (), nodes_num - 1);
  id_tree = tree_insert (id_tree, id, lrand48 (), nodes_num - 1);
  return id;
}

int get_node_idx (long long id) {
  tree_t *T = tree_lookup (id_tree, id);
  if (T) {
    return T->data;
  }
  return -1;
}

int prefailed_nodes;
int failed_nodes;

long long total_binlog_size;
long long minimal_binlog_size;
long long maximal_binlog_size;
int total;

void update_nodes_stats (int use_crc32, int crc32) {
  prefailed_nodes = 0;
  failed_nodes = 0;
  total_binlog_size = 0;
  minimal_binlog_size = 0x7fffffffffffffffll;
  maximal_binlog_size = 0;
  int i;
  total = 0;
  for (i = 0; i < nodes_num; i++) if (!use_crc32 || crc32 == (nodes[i].node.id >> 32)){
    if (precise_now - nodes[i].extra.timestamp > 3 * DEFAULT_CHILDREN_RENEW_TIME) {
      prefailed_nodes ++; 
    }
    if (precise_now - nodes[i].extra.timestamp > 30 * DEFAULT_CHILDREN_RENEW_TIME) {
      failed_nodes ++; 
    }
    total_binlog_size += nodes[i].extra.binlog_position;
    if (nodes[i].extra.binlog_position < minimal_binlog_size) {
      minimal_binlog_size = nodes[i].extra.binlog_position;
    }
    if (nodes[i].extra.binlog_position > maximal_binlog_size) {
      maximal_binlog_size = nodes[i].extra.binlog_position;
    }
    total ++;
  }
}

long long stats_counters[1000];
int stats_num;

void update_stats (int use_crc32, int crc32) {
  
  memset (&min_stats, 0x7f, sizeof (union engine_stats));
  memset (&max_stats, 0x80, sizeof (union engine_stats));
  memset (&sum_stats, 0, sizeof (union engine_stats));
  int j;
  total = 0;
  for (j = 0; j < STATS_DOUBLE_NUM; j++) {
    min_stats.arrays.doubles[j] = 1e100;
    max_stats.arrays.doubles[j] = -1e100;
    sum_stats.arrays.doubles[j] = 0;
  }
  for (j = 0; j < nodes_num; j++) if (!use_crc32 || crc32 == (nodes[j].node.id >> 32)){
    union engine_stats* P = &nodes[j].stats;
    int i;
    for (i = 0; i < STATS_INT_NUM; i++) {
      if (min_stats.arrays.ints[i] > P->arrays.ints[i]) {
        min_stats.arrays.ints[i] = P->arrays.ints[i];
      }
      if (max_stats.arrays.ints[i] < P->arrays.ints[i]) {
        max_stats.arrays.ints[i] = P->arrays.ints[i];
      }
      sum_stats.arrays.ints[i] += P->arrays.ints[i];
    }
    for (i = 0; i < STATS_LONG_NUM; i++) {
      if (min_stats.arrays.longs[i] > P->arrays.longs[i]) {
        min_stats.arrays.longs[i] = P->arrays.longs[i];
      }
      if (max_stats.arrays.longs[i] < P->arrays.longs[i]) {
        max_stats.arrays.longs[i] = P->arrays.longs[i];
      }
      sum_stats.arrays.longs[i] += P->arrays.longs[i];
    }
    for (i = 0; i < STATS_DOUBLE_NUM; i++) {
      if (min_stats.arrays.doubles[i] > P->arrays.doubles[i]) {
        min_stats.arrays.doubles[i] = P->arrays.doubles[i];
      }
      if (max_stats.arrays.doubles[i] < P->arrays.doubles[i]) {
        max_stats.arrays.doubles[i] = P->arrays.doubles[i];
      }
      sum_stats.arrays.doubles[i] += P->arrays.doubles[i];
    }
    total ++;
  }
  /*long long *E = (long long *)P;
  int i;
  for (i = 0; i < 21; i++) {
    
  }*/
}

struct node children_list[MAX_CHILDREN];
int fnum[] = {3, 3, 3};

int generate_children (long long id) {
  assert (fnum[0] + fnum[1] + fnum[2] <= MAX_CHILDREN);
  struct node ch[3][MAX_CHILDREN];
  int fsize[3];
  tree_t *T;
  T = tree_lookup (id_tree, id); 
  if (!T) {
    return -1;
  }
  int ii = T->data; 
  vkprintf (3, "generate_children: ii = %d\n", ii);
  int color;
  tree_t *Rest = nodes_tree;
  tree_t *Done = 0;
  int old_size = nodes_tree->size;
  vkprintf (5, "old_size = %d nodes_tree->left = %p, nodes_tree->right = %p\n", old_size, nodes_tree->left, nodes_tree->right);
  tree_t *L, *R;
  long long x = tree_id (nodes[ii].node.host, nodes[ii].node.port, id);
  for (color = 2; color >= 0; color --) {
    fsize[color] = 0;
    if (!Rest) {
      continue;
    }
    int l = link_level (nodes[ii].node.host, color);
    long long minx = x & ~((1ll << (l + 16)) - 1);
    long long maxx = x |  ((1ll << (l + 16)) - 1);
    tree_t *M1, *M2;

    tree_split (&L, &M1, Rest, minx - 1);
    if (!M1) {
      Rest = L;
      continue;
    }
    
    tree_split (&M2, &R, M1, maxx);
    if (!M2) {
      Rest = tree_merge (L, R);
      continue;
    }

    Rest = tree_merge (L, R);

    int r = M2->size;
    assert (r >= 1);
    int i;
    static int used[MAX_CHILDREN + 1];
    for (i = 0; i < r && fsize[color] < fnum[color]; i++) {
      while (1) {
        int y = lrand48 () % r;
        T = tree_get (M2, y);
        assert (T);
        int j;
        int ok = 1;
        for (j = 0; j < i; j ++) if (used[j] == T->data) {
          ok = 0;
          break;
        }
        if (ok) {
          break;
        }
      }
      used[i] = T->data;
      if (T->data != ii) {
        vkprintf (3, "generate_children: T->data = %d\n", (int)T->data);
        ch[color][fsize[color]++] = nodes[T->data].node;
      }
    }
    tree_split (&L, &R, M2, x);
    vkprintf (5, "L->size = %d\n", L ? L->size : 0);
    Done = tree_merge (L, Done);
    vkprintf (5, "R->size = %d\n", R ? R->size : 0);
    Done = tree_merge (Done, R);
    vkprintf (5, "Done->size = %d\n", Done ? Done->size : 0);
    //M1 = tree_merge (M2, R);
    //nodes_tree = tree_merge (L, M1);
  }
  tree_split (&L, &R, Rest, x);
  Done = tree_merge (L, Done);
  Done = tree_merge (Done, R);
  nodes_tree = Done;
  assert (nodes_tree && nodes_tree->size == old_size);
  int xx = 0;
  int i;
  for (i = 0; i < 3; i++) {
    int j;
    for (j = 0; j < fsize[i] && j < fnum[i]; j++) {
      children_list[xx++] = ch[i][j];
    }
  }
  vkprintf (2, "generate_children: result = %d\n", xx);
  return xx;
}

/*int generate_children (long long id) {
  int size[3];
  assert (fnum[0] < MAX_CHILDREN);
  assert (fnum[1] < MAX_CHILDREN);
  assert (fnum[2] < MAX_CHILDREN);
  size[0] = size[1] = size[2] = 0;
  struct node ch[3][MAX_CHILDREN];
  int i;
  int ii = -1;
  for (i = 0; i < nodes_num; i++) if (nodes[i].node.id == id) {
    ii = i;
    break;
  }
  if (ii < 0) {
    return -1;
  }
  assert (ii >= 0);
  for (i = 0; i < nodes_num; i++) if (i != ii && (nodes[i].node.id & CLUSTER_MASK) == (id & CLUSTER_MASK)) {
    int x = link_color (nodes[i].node.host, nodes[ii].node.host);
    assert (x >= 0 && x <= 2);
    if (size[x] < fnum[x]) {
      ch[x][size[x]++] = nodes[i].node;
    } else {
      long p = (lrand48 () & 0x7fffffff) % (size[x] + 1);
      assert (p >= 0);
      if (p < fnum[x]) {
        ch[x][p] = nodes[i].node;
      }
      size[x] ++;
    }
  }
  int x = 0;
  for (i = 0; i < 3; i++) {
    int j;
    for (j = 0; j < size[i] && j < fnum[i]; j++) {
      children_list[x++] = ch[i][j];
    }
  }
  return x;
}*/

int P[MAX_PACKET_LEN / 4], Q[MAX_PACKET_LEN / 4];


int rpc_send_join_ack (struct connection *c, long long x) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_send_join_ack\n");
  }
  struct rpc_join_ack *T = (struct rpc_join_ack *)Q;
  rpc_create_query (T, sizeof (struct rpc_join_ack), c, RPC_TYPE_JOIN_ACK);
  T->id = x;
  return rpc_send_query (T, c);
}

int rpc_send_kicked (struct connection *c) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_send_kicked\n");
  }
  struct rpc_kicked *T = (struct rpc_kicked *)Q;
  rpc_create_query (T, sizeof (struct rpc_kicked), c, RPC_TYPE_KICKED);
  return rpc_send_query (T, c);
}

int rpc_send_request_update_stats (struct connection *c, long long id) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_send_request_update_stats: id = %lld\n", id);
  }
  struct rpc_request_update_stats *T = (struct rpc_request_update_stats *)Q;
  if (rpc_create_query (T, sizeof (struct rpc_request_update_stats), c, RPC_TYPE_REQUEST_UPDATE_STATS) < 0) {
    return -1;
  }
  return rpc_send_query (T, c);
}

int rpc_send_children_old (struct connection *c, long long id) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_send_children_old\n");
  }
  int x = generate_children (id);
  if (x < 0) {
    return 0;
  }
  struct rpc_children_answer_old *T = (struct rpc_children_answer_old *)Q;
  rpc_create_query (T, sizeof (struct rpc_children_answer_old) + x * sizeof (struct node), c, RPC_TYPE_CHILDREN_ANSWER_OLD);
  memcpy (T->children, children_list, x * sizeof (struct node));
  T->children_num = x;
  return rpc_send_query (T, c);
}

int rpc_send_children (struct connection *c, long long id) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_send_children\n");
  }
  int x = generate_children (id);
  if (x < 0) {
    return 0;
  }
  struct rpc_children_answer *T = (struct rpc_children_answer *)Q;
  rpc_create_query (T, sizeof (struct rpc_children_answer) + x * sizeof (struct node), c, RPC_TYPE_CHILDREN_ANSWER);
  memcpy (T->children, children_list, x * sizeof (struct node));
  T->children_num = x;
  T->id = id;
  return rpc_send_query (T, c);
}

int rpc_send_delays_old (struct connection *c) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_send_delays_old: slow_delay = %lf, medium_delay = %lf\n", slow_delay, medium_delay);
  }
  struct rpc_delays_old *T = (struct rpc_delays_old *)Q;
  rpc_create_query (T, sizeof (struct rpc_delays_old), c, RPC_TYPE_DELAYS_OLD);
  T->medium_delay = medium_delay;
  T->slow_delay = slow_delay;
  return rpc_send_query (T, c);
}

int rpc_send_delays (struct connection *c, long long id) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_send_delays: slow_delay = %lf, medium_delay = %lf\n", slow_delay, medium_delay);
  }
  struct rpc_delays *T = (struct rpc_delays *)Q;
  rpc_create_query (T, sizeof (struct rpc_delays), c, RPC_TYPE_DELAYS);
  T->medium_delay = medium_delay;
  T->slow_delay = slow_delay;
  T->id = id;
  return rpc_send_query (T, c);
}

int rpc_execute_join_old (struct connection *c, struct rpc_join_old *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_join_old: len = %d\n", len);
  }
  if (len != sizeof (struct rpc_join_old)) {
    return 0;
  }
  long long x = add_node (P->host ? P->host : c->remote_ip, P->port, P->id, P->binlog_position, c, 0);
  if (rpc_send_join_ack (c, x) < 0) {
    return -1;
  }
  return rpc_send_delays_old (c);
}

int rpc_execute_join (struct connection *c, struct rpc_join *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_join: len = %d\n", len);
  }
  if (len != sizeof (struct rpc_join)) {
    return 0;
  }
  long long x = add_node (P->host ? P->host : c->remote_ip, P->port, P->id, P->binlog_position, c, P->protocol_version);
  if (rpc_send_join_ack (c, x) < 0) {
    return -1;
  }
  return rpc_send_delays (c, x);
}

int rpc_execute_children_request (struct connection *c, struct rpc_children_request *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_children_request: len = %d\n", len);
  }
  if (len != sizeof (struct rpc_children_request)) {
    return 0;
  }
  int x = get_node_idx (P->id);
  if (nodes[x].protocol_version == 0) {
    return rpc_send_children_old (c, P->id);
  } else {
    return rpc_send_children (c, P->id);
  }
}

int rpc_execute_stats (struct connection *c, struct rpc_stats *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_stats: len = %d\n", len);
  }
  if (len != sizeof (struct rpc_stats)) {
    return 0;
  }
  int n = get_node_idx (P->id);
  if (n < 0) {
    return 0;
  }
  assert (0 <= n && n < nodes_num);
  memcpy (&nodes[n].stats, &P->stats, sizeof (union engine_stats));
  return 0;

}

int long_stats_num = -1;
struct long_update_stats long_stats;
int last_stats_time = 0;

int rpc_execute_update_stats (struct connection *c, struct rpc_update_stats *P, int len) {
  if (verbosity >= 2) {
    fprintf (stderr, "rpc_execute_update_stats: len = %d\n", len);
  }
  if (len != sizeof (struct rpc_update_stats)) {
    return 0;
  }
  int n = get_node_idx (P->id);
  if (n < 0) {
    return 0;
  }
  assert (0 <= n && n < nodes_num);
  nodes[n].time_requested = 0;
  long_stats_num = n;
  last_stats_time = now;
  memcpy (&long_stats, &P->stats, sizeof (struct long_update_stats));
  return 0;

}

int rpcs_execute (struct connection *c, int op, int len) {
  if (verbosity > 0) {
    fprintf (stderr, "rpcs_execute: fd=%d, op=%x, len=%d\n", c->fd, op, len);
  }
  if (len > MAX_PACKET_LEN) {
    return SKIP_ALL_BYTES;    
  }

  assert (read_in (&c->In, &P, len) == len);
  assert (rpc_check_crc32 (P));


  switch (op) {
  case RPC_TYPE_JOIN:
    return rpc_execute_join (c, (struct rpc_join *)P, len);
    break;

  case RPC_TYPE_JOIN_OLD:
    return rpc_execute_join_old (c, (struct rpc_join_old *)P, len);
    break;

  case RPC_TYPE_CHILDREN_REQUEST:
    return rpc_execute_children_request (c, (struct rpc_children_request *)P, len);
    break;

  case RPC_TYPE_STATS:
    return rpc_execute_stats (c, (struct rpc_stats *)P, len);
    break;

  case RPC_TYPE_UPDATE_STATS:
    return rpc_execute_update_stats (c, (struct rpc_update_stats *)P, len);
    break;
  }
  return 0;
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  return -2;
}

int memcache_prepare_stats (struct connection *c, int use_crc32, int crc32);
int memcache_get (struct connection *c, const char *key, int key_len) {
  int node, limit;
  long long int_mask, long_mask, double_mask, id;
  const char *orig_key = key;
  while (*key != 0 && *key != '@') {
    key ++;
  }
  int crc32 = 0;
  int use_crc32;
  if (*key == 0) {
    use_crc32 = 0;
    key = orig_key;
  } else {
    use_crc32 = 1;
    crc32 = compute_crc32 (orig_key, key - orig_key);
    key ++;
  }
  if (!strcmp (key, "stats")) {
    stats_buff_len = memcache_prepare_stats (c, use_crc32, crc32);
    return_one_key (c, orig_key, stats_buff, stats_buff_len);
  }
  if (!strcmp (key, "nodes")) {
    int i;
    stats_buff_len = 0;
    for (i = 0; i < nodes_num; i++) if (!use_crc32 || (nodes[i].node.id >> 32) == crc32) {
      if (stats_buff_len > STOP_STATS) { break; }
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%d: " IP " %d %lld %lld %lld\n", i, INT_TO_IP(nodes[i].node.host), nodes[i].node.port, nodes[i].node.id, nodes[i].stats.structured.binlog_position, nodes[i].stats.structured.crc64);
    }
    return_one_key (c, orig_key, stats_buff, stats_buff_len);
  }
  if (sscanf (key, "number%lld", &id) == 1) {
    stats_buff_len = snprintf (stats_buff, STATS_BUFF_SIZE, "%d\n", get_node_idx (id));
    return_one_key (c, orig_key, stats_buff, stats_buff_len);
  }
  if (!strcmp (key, "failed_nodes")) {
    int i;
    stats_buff_len = 0;
    for (i = 0; i < nodes_num; i++) if (precise_now - nodes[i].extra.timestamp > 3 * DEFAULT_CHILDREN_RENEW_TIME && (!use_crc32 || (nodes[i].node.id >> 32) == crc32)) {
      if (stats_buff_len > STOP_STATS) { break; }
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%d: "IP" %d %lld %lld %lld\n", i, INT_TO_IP(nodes[i].node.host), nodes[i].node.port, nodes[i].node.id, nodes[i].stats.structured.binlog_position, nodes[i].stats.structured.crc64);
    }
    return_one_key (c, orig_key, stats_buff, stats_buff_len);
  }
  if (sscanf (key, "nodes_stats%d_%d_%lld_%lld_%lld", &node, &limit, &int_mask, &long_mask, &double_mask) == 5 && node >= 0 && node < nodes_num && limit > 0) {
    stats_buff_len = 0;
    for (; limit > 0 && node < nodes_num && stats_buff_len < STATS_BUFF_SIZE; node ++) if (!use_crc32 || (nodes[node].node.id >> 32) == crc32) {
      int i;
      union engine_stats *S = &nodes[node].stats;
      if (stats_buff_len > STOP_STATS) { break; }
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%d", node); 
      for (i = 0; i < STATS_INT_NUM; i++) if ((1 << i) & int_mask) {
        if (stats_buff_len > STOP_STATS) { break; }
        stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, " %d", S->arrays.ints[i]); 
      }
      for (i = 0; i < STATS_LONG_NUM; i++) if ((1 << i) & long_mask) {
        if (stats_buff_len > STOP_STATS) { break; }
        stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, " %lld", S->arrays.longs[i]); 
      }
      for (i = 0; i < STATS_DOUBLE_NUM; i++) if ((1 << i) & double_mask) {
        if (stats_buff_len > STOP_STATS) { break; }
        stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, " %lf", S->arrays.doubles[i]); 
      }
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "\n");
      limit --;
    }
    return_one_key (c, orig_key, stats_buff, stats_buff_len);
  }
  if (sscanf (key, "node%d", &node) == 1 && node >= 0 && node < nodes_num) {
    assert (node >= 0 && node < nodes_num);
    int i;
    stats_buff_len = 0;
    union engine_stats *S = &nodes[node].stats;
    for (i = 0; i < STATS_INT_NUM; i++) {
      if (stats_buff_len > STOP_STATS) { break; }
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%s\t%d\n", stats_layout[i], S->arrays.ints[i]); 
    }
    for (i = 0; i < STATS_LONG_NUM; i++) {
      if (stats_buff_len > STOP_STATS) { break; }
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%s\t%lld\n", stats_layout[i + STATS_INT_NUM], S->arrays.longs[i]); 
    }
    for (i = 0; i < STATS_DOUBLE_NUM; i++) {
      if (stats_buff_len > STOP_STATS) { break; }
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%s\t%lf\n", stats_layout[i + STATS_INT_NUM + STATS_LONG_NUM], S->arrays.doubles[i]); 
    }
    int p = S->structured.update_pos;
    int k = 0;
    while (k < STATS_UPDATE_BUFF_SIZE) {
      if (S->structured.update_operations[p] == LOG_BINLOG_UPDATED) {
        if (stats_buff_len > STOP_STATS) { break; }
        stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "Time %lf: Binlog updated to position %lld\n", S->structured.update_times[p], S->structured.update_data[p]);
      }
      if (S->structured.update_operations[p] == LOG_BINLOG_RECEIVED) {
        if (stats_buff_len > STOP_STATS) { break; }
        stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "Time %lf: Binlog received from node %lld\n", S->structured.update_times[p], S->structured.update_data[p]);
      }
      if (S->structured.update_operations[p] == LOG_BINLOG_REQUEST_SENT) {
        if (stats_buff_len > STOP_STATS) { break; }
        stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "Time %lf: Binlog requested from node %lld\n", S->structured.update_times[p], S->structured.update_data[p]);
      }
      p++;
      if (p == STATS_UPDATE_BUFF_SIZE) {
        p = 0;
      }
      k++;
    }
    return_one_key (c, orig_key, stats_buff, stats_buff_len);
  }
  if (sscanf (key, "node_updates%d", &node) == 1 && node >= 0 && node < nodes_num) {
    assert (node >= 0 && node < nodes_num);
    if (node == long_stats_num && now - last_stats_time <= 10) {
      int p = long_stats.update_pos;
      int k = 0;
      stats_buff_len = 0;
      while (k < LONG_STATS_UPDATE_BUFF_SIZE) {
        if (long_stats.update_operations[p] == LOG_BINLOG_UPDATED) {
          if (stats_buff_len > STOP_STATS) { break; }
          stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "Time %lf: Binlog updated to position %lld\n", long_stats.update_times[p], long_stats.update_data[p]);
        }
        if (long_stats.update_operations[p] == LOG_BINLOG_RECEIVED) {
          if (stats_buff_len > STOP_STATS) { break; }
          stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "Time %lf: Binlog received from node %lld\n", long_stats.update_times[p], long_stats.update_data[p]);
        }
        if (long_stats.update_operations[p] == LOG_BINLOG_REQUEST_SENT) {
          if (stats_buff_len > STOP_STATS) { break; }
          stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "Time %lf: Binlog requested from node %lld\n", long_stats.update_times[p], long_stats.update_data[p]);
        }
        p++;
        if (p == LONG_STATS_UPDATE_BUFF_SIZE) {
          p = 0;
        }
        k++;
      }
      return_one_key (c, orig_key, stats_buff, stats_buff_len);
    } else {
      if (!nodes[node].connection || nodes[node].connection->generation != nodes[node].generation || server_check_ready (nodes[node].connection) != cr_ok || rpc_send_request_update_stats (nodes[node].connection, nodes[node].node.id) < 0) {
        return_one_key (c, orig_key, "No connection to node", 21);
      } else {
        return_one_key (c, orig_key, "Request sent", 12);
      }
    }      

  }
  if (sscanf (key, "set_delays%lf_%lf", &medium_delay, &slow_delay) >= 2) {
    stats_buff_len = snprintf (stats_buff, STATS_BUFF_SIZE, "Delays set\n");
    return_one_key (c, key, stats_buff, stats_buff_len);
  }
  if (!strcmp (key, "nodes_times")) {
    int i;
    stats_buff_len = 0;
    for (i = 0; i < nodes_num; i++) {
      if (stats_buff_len > STOP_STATS) { break; }
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%lf " IP " %d %lld %lld %d\n", nodes[i].stats.structured.last_binlog_update, INT_TO_IP(nodes[i].node.host), nodes[i].node.port, nodes[i].node.id, nodes[i].stats.structured.binlog_position, i);
    }
    return_one_key (c, key, stats_buff, stats_buff_len);
  }
  if (key_len >= 16 && !strncmp (key, "nodes_search", 12)) {    
    char type = key[12];
    char sign = key[13];
    char failed = key[14];
    stats_buff_len = 0;
    limit = 100000;
    if ((type == 'd' || type == 'l' || type == 'f') && (sign == '<' || sign == '>' || sign == '=') && (failed == 'F' || failed == 'A')) {
      long long lv = 0;
      double fv = 0;
      int number = -1;
      int count_mode = 1;
      int count = 0;
#define check(x,y) ((sign == '<' && (x) < (y)) || (sign == '=' && (x) == (y)) || (sign == '>' && (x) > (y)))      
      if (type == 'd') {
        if (sscanf (key + 15, "%d_%lld#%d\n", &number, &lv, &limit) >= 2 && number >= 0 && number < STATS_INT_NUM) {
          int i;
          for (i = 0; i < nodes_num; i++) if (failed == 'A' || precise_now - nodes[i].extra.timestamp < 3 * DEFAULT_CHILDREN_RENEW_TIME) {
            if (check (nodes[i].stats.arrays.ints[number], lv)) {
              if (!count_mode || count < limit) {
                if (stats_buff_len > STOP_STATS) { break; }
                stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%d " IP " %d %d\n", i, INT_TO_IP(nodes[i].node.host), nodes[i].node.port, nodes[i].stats.arrays.ints[number]);
                count++;
              } else {
                count ++;
              }
            }
          }
        }
      }
      if (type == 'l') {
        if (sscanf (key + 15, "%d_%lld#%d\n", &number, &lv, &limit) >= 2 && number >= 0 && number < STATS_LONG_NUM) {
          int i;
          for (i = 0; i < nodes_num; i++) if (failed == 'A' || precise_now - nodes[i].extra.timestamp < 3 * DEFAULT_CHILDREN_RENEW_TIME) {
            if (check (nodes[i].stats.arrays.longs[number], lv)) {
              if (!count_mode || count < limit) {
                if (stats_buff_len > STOP_STATS) { break; }
                stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%d " IP " %d %lld\n", i, INT_TO_IP(nodes[i].node.host), nodes[i].node.port, nodes[i].stats.arrays.longs[number]);
                count ++;
              } else {
                count ++;
              }
            }
          }
        }
      }
      if (type == 'f') {
        if (sscanf (key + 15, "%d_%lf#%d\n", &number, &fv, &limit) >= 2 && number >= 0 && number < STATS_DOUBLE_NUM) {
          int i;
          for (i = 0; i < nodes_num; i++) if (failed == 'A' || precise_now - nodes[i].extra.timestamp < 3 * DEFAULT_CHILDREN_RENEW_TIME) {
            if (check (nodes[i].stats.arrays.doubles[number], fv)) {
              if (!count_mode || count < limit) {
                if (stats_buff_len > STOP_STATS) { break; }
                stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%d " IP " %d %lf\n", i, INT_TO_IP(nodes[i].node.host), nodes[i].node.port, nodes[i].stats.arrays.doubles[number]);
                count ++;
              } else {
                count ++;
              }
            }
          }
        }
      }
#undef check
    if (count_mode) {
      stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%d\n", count);
    }
    return_one_key (c, key, stats_buff, stats_buff_len);

    }



  }
  return 0;
}

int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg) {
  return -2;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  return -2;
}

#define safediv(x,y) (y != 0) ? x / y : 0

int memcache_prepare_stats (struct connection *c, int use_crc32, int crc32) {
  int uptime = now - start_time;
  dyn_update_stats(); 
  update_nodes_stats (use_crc32, crc32);
  update_stats (use_crc32, crc32);

  stats_buff_len = snprintf (stats_buff, STATS_BUFF_SIZE,
      "heap_allocated\t%ld\n"
      "heap_max\t%ld\n"
      "wasted_heap_blocks\t%d\n"
      "wasted_heap_bytes\t%ld\n"
      "free_heap_blocks\t%d\n"
      "free_heap_bytes\t%ld\n"
      "uptime\t%d\n"
      "total_nodes\t%d\n"
      "prefailed_nodes\t%d\n"
      "failed_nodes\t%d\n"
      "total_binlog_size\t%lld\n"
      "minimal_binlog_size\t%lld\n"
      "maximal_binlog_size\t%lld\n"
      FULL_VERSION,
      //"version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ "\n",
      (long) (dyn_cur - dyn_first),
      (long) (dyn_last - dyn_first),
      wasted_blocks,
      wasted_bytes,
      freed_blocks,
      freed_bytes,
      uptime,
      nodes_num,
      prefailed_nodes,
      failed_nodes,
      total_binlog_size,
      minimal_binlog_size,
      maximal_binlog_size
      );
  int i;
  for (i = 0; i < STATS_INT_NUM; i++) {
    stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%s\t%d_%d_%d\n", stats_layout[i], min_stats.arrays.ints[i], max_stats.arrays.ints[i], safediv (sum_stats.arrays.ints[i], total)); 
  }
  for (i = 0; i < STATS_LONG_NUM; i++) {
    stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%s\t%lld_%lld_%lld\n", stats_layout[i + STATS_INT_NUM], min_stats.arrays.longs[i], max_stats.arrays.longs[i], safediv (sum_stats.arrays.longs[i], total)); 
  }
  for (i = 0; i < STATS_DOUBLE_NUM; i++) {
    stats_buff_len += snprintf (stats_buff + stats_buff_len, STATS_BUFF_SIZE - stats_buff_len, "%s\t%lf_%lf_%lf\n", stats_layout[i + STATS_INT_NUM + STATS_LONG_NUM], min_stats.arrays.doubles[i], max_stats.arrays.doubles[i], safediv (sum_stats.arrays.doubles[i], total)); 
  }
  return stats_buff_len;
}

int memcache_stats (struct connection *c) {
  int len = memcache_prepare_stats (c, 0, 0);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
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
  signal(SIGUSR1, sigusr1_handler);
}

void cron (void) {
}

int sfd;

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
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-N <network-description-file>] [-p <port>] [-c <max_connections>] [-S <slow_links_delay>] [-M <medium_links_delay>]\n"
	  "\tCore server for copy-test-to-all\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
    FULL_VERSION,
	  progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;

  set_debug_handlers ();

  char encr_file[256];
  int custom_encr = 0;
  progname = argv[0];
  char network_desc_file[256];
  while ((i = getopt (argc, argv, "vdc:E:p:N:hu:l:S:M:")) != -1) {
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
    case 'E':
      strncpy (encr_file, optarg, 255);
      custom_encr = 1;
      break;
    case 'N':
      strncpy (network_desc_file, optarg, 255);
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'S':
      slow_delay = atof(optarg);
      break;
    case 'M':
      medium_delay = atof(optarg);
      break;
    case 'h':
      usage ();
      exit (1);
      break;
    case 'd':
      daemonize ^= 1;
    }
  }
  if (argc != optind) {
    usage();
    return 2;
  }

  if (raise_file_rlimit(maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit(1);
  }
  init_dyn_data();

  read_network_file (network_desc_file);
  PID.port = port;


  if (!custom_encr) {
    aes_load_pwd_file (0);
  } else {
    aes_load_pwd_file (encr_file);
  }

  start_time = time(0);

  start_server();

  return 0;
}
