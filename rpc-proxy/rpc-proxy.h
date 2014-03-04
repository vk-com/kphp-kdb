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

#ifndef __RPC_PROXY_H__
#define __RPC_PROXY_H__

#include "vv-tl-parse.h"


#define RPC_PROXY_INDEX_MAGIC 0x162cb9e2

#define TL_MEMCACHE_SET 0xeeeb54c4
#define TL_MEMCACHE_ADD 0xa358f31c
#define TL_MEMCACHE_REPLACE 0x2ecdfaa2
#define TL_MEMCACHE_INCR 0x80e6c950
#define TL_MEMCACHE_DECR 0x6467e0d9
#define TL_MEMCACHE_DELETE 0xab505c0a
#define TL_MEMCACHE_GET 0xd33b13ae

#define TL_RPC_DEST_ACTOR 0x7568aabd
#define TL_RPC_DEST_ACTOR_FLAGS 0xf0a5acf7

#define MAX_CLUSTER_SERVERS 65536
#define MAX_CLUSTERS    256
#define MAX_EXTENSIONS 1000
#define MAX_SCHEMAS 1000

#define CLUSTER_MODE_UDP (1 << 30)

#define ACTOR_ID_UNINIT 0x7fffffffffffffffll

#define PROTO_TCP 0
#define PROTO_UDP 1

//bits 0..2 - kint forward

extern long long received_bad_answers;


typedef struct rpc_point rpc_point_t;

struct rpc_cluster_bucket;

struct rpc_point {
  unsigned long long x;
  //struct conn_target *target;
  struct rpc_cluster_bucket *B;
};

#pragma pack(push,4)
struct tl_rpc_req_header {
  int constructor_id;
  long long id;
  int flags;
};
#pragma pack(pop)

struct worker {
  struct process_id pid;
  struct connection *c;
};

typedef int (*rpc_fun_t)(void **IP, void **Data);

#define MAX_CLUSTER_FUNS 100
#define RPC_FUN_NUM 32

struct rpc_cluster_methods {
  int fun_pos[RPC_FUN_NUM];
  rpc_fun_t funs[MAX_CLUSTER_FUNS];
  int (*forward)(void);
  int (*pre_forward_target)(struct rpc_cluster_bucket *B, long long);
  int (*forward_target)(struct rpc_cluster_bucket *B, long long);
  struct rpc_query *(*create_rpc_query)(long long);
};

struct rpc_config_methods {
  int fun_pos[RPC_FUN_NUM];
  rpc_fun_t funs[MAX_CLUSTER_FUNS];
};

#define CF_ALLOW_STRING_FORWARD 1
#define CF_ALLOW_EMPTY_CLUSTER 2
#define CF_FORBID_FORCE_FORWARD 4

struct rpc_cluster_bucket_methods {
  void (*init_store)(struct rpc_cluster_bucket *self, void *conn, long long qid);
  unsigned (*get_host)(struct rpc_cluster_bucket *self);
  int (*get_port)(struct rpc_cluster_bucket *self);
  long long (*get_actor)(struct rpc_cluster_bucket *self);
  void *(*get_conn) (struct rpc_cluster_bucket *self);
  int (*get_multi_conn) (struct rpc_cluster_bucket *self, void **buf, int n);
  int (*get_state)(struct rpc_cluster_bucket *self);
  enum tl_type (*get_type)(struct rpc_cluster_bucket *self);
};

struct rpc_cluster_bucket {
  struct rpc_cluster_bucket_methods *methods;
  union {
    struct {
      struct conn_target *T;
      struct rpc_target *RT;
    };
    struct udp_target_set *S;
  };
  long long A;
};

struct rpc_cluster {
  long long id;
  long long new_id;
  int proto;
  int tot_buckets;
  int server_socket;
  int cluster_no;
  int other_cluster_no;
  int step;
  int min_connections, max_connections;
//  int points_num;
  int schema_num;
  int flags;
  const char *schema;
//  rpc_point_t *points;
  char *cluster_name;
  struct rpc_cluster_bucket buckets[MAX_CLUSTER_SERVERS];
  double timeout;
  long long cluster_mode;
  long long forwarded_queries;
  long long received_answers;
  long long received_errors;
  long long timeouted_queries;
  long long immediate_errors;
  struct rpc_cluster_methods methods;
  int extensions_num;
  int extensions[MAX_EXTENSIONS];
  void *extensions_extra[MAX_EXTENSIONS];
  char *extensions_params[MAX_EXTENSIONS];
  void *schema_extra;
};

struct rpc_config {
  int ConfigServersCount, clusters_num;
  int config_bytes, config_loaded_at;
  char *config_md5_hex;
  struct conn_target *ConfigServers[MAX_CLUSTER_SERVERS];
  struct rpc_cluster Clusters[MAX_CLUSTERS];
//  struct rpc_cluster *kitten_php_cluster;
  void *extensions_extra[MAX_EXTENSIONS];
  void *schema_extra[MAX_SCHEMAS];
  struct rpc_config_methods methods;
};

extern struct rpc_cluster *CC;
int rpc_proxy_server_execute (struct connection *c, int op, int len);
int rpc_proxy_client_execute (struct connection *c, int op, int len);

struct rpc_query;
struct rpc_query_type {
  rpc_fun_t *on_alarm;
  rpc_fun_t *on_answer;
  rpc_fun_t *on_free;
};

struct rpc_query {
  long long qid;
  struct process_id pid;
  long long old_qid;
  //void *in;
  enum tl_type in_type;
  void *extra;
  struct rpc_query_type type;
  struct event_timer ev;
  double start_time;
};

struct rpc_current_query {
  //void *in;
  void *extra;
  struct process_id *remote_pid;
  void (*fail_query)(enum tl_type type, struct process_id *PID, long long qid);
  enum tl_type in_type;
  struct tl_query_header *h;
};

struct in_target {
  int a, b;
  struct connection *first, *last;
  struct process_id PID;
};

extern struct rpc_current_query *CQ;

void _fail_query (enum tl_type type, struct process_id *PID, long long qid);
//void _fail_query_raw_msg (struct process_id *PID, long long qid);

void default_on_free (struct rpc_query *q);
long long get_free_rpc_qid (long long qid);
struct connection *get_target_connection (struct conn_target *S);
struct rpc_query *create_rpc_query (long long qid, struct process_id pid, long long old_qid, enum tl_type in_type, /*void *in,*/ struct rpc_query_type rpc_query_type, double timeout);
int default_query_forward (struct rpc_cluster_bucket *B, long long new_qid);
void default_on_alarm (struct rpc_query *q);
int default_query_forward_conn (struct rpc_cluster_bucket *B, void *conn, long long new_actor_id, long long new_qid, int advance);
int default_query_diagonal_forward (void);
int default_tuple_forward_ext (int size);
int default_tuple_forward (int size);
int default_firstint_forward (void);
int default_string_forward (void);
int default_vector_forward_ext (void);
int default_vector_forward (void);
int default_firstlong_forward_ext (void);
int default_firstlong_forward (void);
int default_unsigned_firstlong_forward_ext (void);
int default_unsigned_firstlong_forward (void);
int default_random_forward (void);

int rpc_proxy_store_init (struct rpc_cluster_bucket *B, long long new_qid);
struct rpc_cluster *get_cluster_by_id (long long id);
struct rpc_query *get_rpc_query (long long qid);
void query_on_free (struct rpc_query *q);

#define RPC_FUN_ON_RECEIVE 0
#define RPC_FUN_STRING_FORWARD 1
#define RPC_FUN_PREFORWARD_CHECK 2
#define RPC_FUN_PREFORWARD_EDIT 3
#define RPC_FUN_PREFORWARD_VIEW 4
#define RPC_FUN_QUERY_ON_ALARM 5
#define RPC_FUN_QUERY_ON_ANSWER 6
#define RPC_FUN_QUERY_ON_FREE 7
#define RPC_FUN_ON_NET_FAIL 8

#define RPC_FUN_CREATE_RPC_QUERY 16
#define RPC_FUN_FORWARD_TARGET 17
struct rpc_cluster_create {
  int lock;
  rpc_fun_t funs[RPC_FUN_NUM][MAX_CLUSTER_FUNS];
  int funs_last[RPC_FUN_NUM];
};

#define RPC_FUN_CUSTOM_OP 0
//#define RPC_FUN_GLOBAL_STAT 1
struct rpc_config_create {
  int lock;
  rpc_fun_t funs[RPC_FUN_NUM][MAX_CLUSTER_FUNS];
  int funs_last[RPC_FUN_NUM];
};


#define RPC_FUN_NEXT return ((rpc_fun_t)(*IP))(IP + 1, Data);
#define RPC_FUN_START(x,Data) CC->methods.funs[x] ((void **)(CC->methods.funs + x + 1), Data);
#define RPC_CONF_FUN_START(x,Data) CurConf->methods.funs[x] ((void **)(CurConf->methods.funs + x + 1), Data);
int rpc_fun_ok (void **IP, void **Data);

typedef int (*rpc_ext_add_t)(struct rpc_config *Conf, struct rpc_config *ConfOld, struct rpc_cluster *C, struct rpc_config_create *Zconf, struct rpc_cluster_create *Z, int flags, const char *param, int param_len);
typedef int (*rpc_ext_del_t)(struct rpc_config *Conf, struct rpc_cluster *C);
typedef int (*rpc_stat_t)(char *buf, int size);
#define rpc_schema_add_t rpc_ext_add_t
#define rpc_schema_del_t rpc_ext_del_t

struct rpc_extension {
  char *name;
  int priority; 
  int num;
  rpc_ext_add_t add;
  rpc_ext_del_t del;
  rpc_stat_t stat;
};

struct rpc_schema {
  char *name;
  int priority; 
  int num;
  rpc_schema_add_t add;
  rpc_schema_del_t del;
  rpc_stat_t stat;
};

void rpc_extension_add (struct rpc_extension *E);
void rpc_schema_add (struct rpc_schema *E);
struct rpc_query *default_create_rpc_query (long long new_qid);

#define EXTENSION_ADD(nm) \
  int rpc_ ## nm ##_extension_add (struct rpc_config *RC, struct rpc_config *RC_old, struct rpc_cluster *C, struct rpc_config_create *Zconf, struct rpc_cluster_create *Z, int flags, const char *param, int param_len)

#define EXTENSION_DEL(nm) \
  int rpc_ ## nm ##_extension_del (struct rpc_config *RC, struct rpc_cluster *C)
  

#define EXTENSION_REGISTER(nm,pr) \
  struct rpc_extension nm ## _extension = { \
    .name = #nm ,\
    .priority = pr,\
    .add = rpc_ ## nm ##_extension_add \
  }; \
  void register_extension_ ## nm (void) __attribute__ ((constructor)); \
  void register_extension_ ## nm (void) { \
    rpc_extension_add (&nm ## _extension); \
  }\

#define EXTENSION_REGISTER_DEL(nm,pr) \
  struct rpc_extension nm ## _extension = { \
    .name = #nm ,\
    .priority = pr,\
    .add = rpc_ ## nm ##_extension_add, \
    .del = rpc_ ## nm ##_extension_del  \
  }; \
  void register_extension_ ## nm (void) __attribute__ ((constructor)); \
  void register_extension_ ## nm (void) { \
    rpc_extension_add (&nm ## _extension); \
  }\

#define EXTENSION_REGISTER_NUM(nm,pr,var) \
  struct rpc_extension nm ## _extension = { \
    .name = #nm ,\
    .priority = pr,\
    .add = rpc_ ## nm ##_extension_add \
  }; \
  void register_extension_ ## nm (void) __attribute__ ((constructor)); \
  void register_extension_ ## nm (void) { \
    rpc_extension_add (&nm ## _extension); \
    var = nm ## _extension.num; \
  }\

#define EXTENSION_REGISTER_DEL_NUM(nm,pr,var) \
  struct rpc_extension nm ## _extension = { \
    .name = #nm ,\
    .priority = pr,\
    .add = rpc_ ## nm ##_extension_add, \
    .del = rpc_ ## nm ##_extension_del  \
  }; \
  void register_extension_ ## nm (void) __attribute__ ((constructor)); \
  void register_extension_ ## nm (void) { \
    rpc_extension_add (&nm ## _extension); \
    var = nm ## _extension.num; \
  }\

#define EXTENSION_REGISTER_NUM_STAT(nm,pr,var,statf) \
  struct rpc_extension nm ## _extension = { \
    .name = #nm ,\
    .priority = pr,\
    .add = rpc_ ## nm ##_extension_add, \
    .stat = statf \
  }; \
  void register_extension_ ## nm (void) __attribute__ ((constructor)); \
  void register_extension_ ## nm (void) { \
    rpc_extension_add (&nm ## _extension); \
    var = nm ## _extension.num; \
  }\

#define SCHEMA_ADD(nm) \
  int rpc_ ## nm ##_schema_add (struct rpc_config *RC, struct rpc_config *RC_old, struct rpc_cluster *C, struct rpc_config_create *Zconf, struct rpc_cluster_create *Z, int flags, const char *param, int param_len)
  

#define SCHEMA_REGISTER(nm,pr) \
  struct rpc_schema nm ## _schema = { \
    .name = #nm ,\
    .priority = pr,\
    .add = rpc_ ## nm ##_schema_add \
  }; \
  void register_schema_ ## nm (void) __attribute__ ((constructor)); \
  void register_schema_ ## nm (void) { \
    rpc_schema_add (&nm ## _schema); \
  }\

#define SCHEMA_REGISTER_NUM(nm,pr,var) \
  struct rpc_schema nm ## _schema = { \
    .name = #nm ,\
    .priority = pr,\
    .add = rpc_ ## nm ##_schema_add \
  }; \
  void register_schema_ ## nm (void) __attribute__ ((constructor)); \
  void register_schema_ ## nm (void) { \
    rpc_schema_add (&nm ## _schema); \
    var = nm ## _schema.num; \
  }\


#endif
