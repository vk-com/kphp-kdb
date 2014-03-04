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

#include "rpc-proxy.h"
#include "net-rpc-targets.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "vv-tree.h"

#define TL_RESPONSE_INDERECT 0x2194f56e

int KittenPhpNum;
int KittenPhpSecureNum;
extern long long active_queries;
extern long long forwarded_queries;
extern struct rpc_config *CurConf;


int rpc_conn_ready (struct connection *c) {
  server_check_ready (c);
  return c && (c->status == cr_ok || c->status == conn_running || c->status == conn_reading_query) && RPCS_DATA(c)->packet_num >= 0 && c->fd >= 0;
}

#define worker_cmp(a,b) (memcmp (&a, &b, sizeof (struct process_id)))
DEFINE_TREE_STD_ALLOC (worker,struct worker,worker_cmp,int,std_int_compare)
DEFINE_QUEUE (worker,struct process_id);

static struct tree_worker *worker_tree;
static struct queue_worker *worker_queue;

struct connection *kitten_php_get_connection (int store_error) {
  while (1) {
    if (!worker_queue) {
      assert (!worker_tree);
      if (store_error) {
        tl_fetch_set_error_format (TL_ERROR_NO_CONNECTIONS, "All kitten php are busy. Active_queries = %lld", active_queries);
      }
      return 0;
    }
    queue_worker_t *x = queue_get_first_worker (worker_queue);
    struct process_id pid = x->x;
    struct worker w = {
      .pid = pid, .c = 0
    };
    tree_worker_t *T = tree_lookup_worker (worker_tree, w);
    assert (T);
    w = T->x;
    worker_tree = tree_delete_worker (worker_tree, w);
    worker_queue = queue_del_first_worker (worker_queue);
    struct connection *c = w.c;
    if (c && !memcmp (&RPCS_DATA(c)->remote_pid, &w.pid, sizeof (w.pid)) && rpc_conn_ready (c)) {
      return c;
    }
  }
}

struct kitten_php_delay {
  struct event_timer ev;
  struct tl_query_header *h;
  //void *req;
  enum tl_type req_type;
  struct process_id req_pid;
  long long new_qid;
  int size;
  int queue;
  int *data;
};

DEFINE_QUEUE(delay,struct kitten_php_delay *);
struct queue_delay *delay_queue;

int kitten_php_delay_restart (struct event_timer *ev) {
  struct kitten_php_delay *q = (void *)ev;
  CC = CurConf->schema_extra[KittenPhpNum];
  vkprintf (2, "restart: CC = %p\n", CC);
  if (!CC) {
    _fail_query (q->req_type, &q->req_pid, CQ->h->qid);
    free (q->data);
    zfree (q, sizeof (*q));
    return 0;
  }
  assert (CC);

  struct connection *d = kitten_php_get_connection (1);
  if (!d) {
    if (!q->queue) {
      _fail_query (q->req_type, &q->req_pid, CQ->h->qid);
      free (q->data);
      zfree (q, sizeof (*q));
    } else {      
      delay_queue = queue_add_delay (delay_queue, q);
    }
    return 0;
  }
  
  forwarded_queries ++;
//  long long qid = q->h->qid;
  long long new_qid = q->new_qid; //get_free_rpc_qid (qid);

  tl_store_init (d, new_qid);

  struct tl_query_header *h = q->h;
  assert (h->actor_id == CC->id);
  h->qid = new_qid;
  h->actor_id = CC->new_id;
  tl_store_header (h);

  tl_store_raw_data ((void *)q->data, q->size);
//  CC->methods.create_rpc_query (new_qid);
  tl_store_end_ext (h->real_op);
  tl_query_header_delete (q->h);
  free (q->data);
  zfree (q, sizeof (*q));
  return 0;
}

int kitten_php_delay (int in_queue, int queue) {
  vkprintf (3, "Function %s\n", __func__);
  struct kitten_php_delay *q = zmalloc (sizeof (*q));
  q->h = tl_query_header_dup (CQ->h);
  q->size = tl_fetch_unread ();
  assert (!(q->size & 3));
  q->data = malloc (q->size);
  //q->req  = CQ->in;
  q->req_pid = *CQ->remote_pid;
  q->req_type = CQ->in_type;
  assert (q->data);
  tl_fetch_string_data ((char *)q->data, q->size);
  
  q->new_qid = get_free_rpc_qid (q->h->qid);
  if (!in_queue) {
    q->ev.h_idx = 0;
    q->ev.wakeup = kitten_php_delay_restart;
    q->ev.wakeup_time = precise_now + CQ->h->kitten_php_delay * 0.001;
    q->h->flags &= ~TL_QUERY_HEADER_FLAG_KPHP_DELAY;
    insert_event_timer (&q->ev);
  } else {
    delay_queue = queue_add_delay (delay_queue, q);
  }
  q->queue = queue;

  CC->methods.create_rpc_query (q->new_qid);
  return 0;
}

int kphp_query_forward_conn (struct connection *c, long long new_actor_id, long long new_qid, int advance) {
  vkprintf (1, "default_query_forward: CC->id = %lld, CC->timeout = %lf\n", CC->id, CC->timeout);
  assert (c);
  if (tl_fetch_error ()) {
    return -1;
  }
  CC->forwarded_queries ++;
  forwarded_queries ++;
  long long qid = CQ->h->qid;
  double save_timeout = CQ->h->custom_timeout;
  CQ->h->custom_timeout *= 0.9;

  tl_store_init (c, new_qid);

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

int kitten_php_forward (void) {
  vkprintf (2, "forward: CC = %p\n", CC);
  if (tl_fetch_error ()) {
    return -1;
  }
  if (CQ->h->kitten_php_delay > 0) {
    return kitten_php_delay (0, 0);
  }
  struct connection *c = kitten_php_get_connection (1);
  if (c) {
    vkprintf (2, "Forwarding: connect = %d\n", c->fd); 
    long long new_qid = get_free_rpc_qid (CQ->h->qid);
    return kphp_query_forward_conn (c, CC->new_id, new_qid, 1);
  } else {
    return -1;
  }
}

int kitten_php_queue_forward (void) {
  vkprintf (2, "forward: CC = %p\n", CC);
  if (tl_fetch_error ()) {
    return -1;
  }
  if (CQ->h->kitten_php_delay > 0) {
    return kitten_php_delay (0, 1);
  }
  struct connection *c = kitten_php_get_connection (0);
  if (c) {
    vkprintf (2, "Forwarding: connect = %d\n", c->fd); 
    long long new_qid = get_free_rpc_qid (CQ->h->qid);
    return kphp_query_forward_conn (c, CC->new_id, new_qid, 1);
  } else {
    kitten_php_delay (1, 1);
    return 0;
  }
}

int kitten_php_ready (int op, struct connection *c) {
  vkprintf (3, "Function %s: op = %08x, c = %d\n", __func__, op, c ? c->fd : -1);
  struct process_id pid = RPCS_DATA(c)->remote_pid;;
  //pid.ip = tl_fetch_int ();
  //pid.port = tl_fetch_int ();
  //pid.pid = tl_fetch_int ();
  //pid.utime = tl_fetch_int ();
  tl_fetch_skip_raw_data (24);
  tl_fetch_end ();
  if (tl_fetch_error ()) {
    vkprintf (3, "Parse failed\n");
    return -1;
  }
  struct worker w = {
    .pid = pid, .c = c
  };
  tree_worker_t *T = tree_lookup_worker (worker_tree, w);
  if (op == RPC_STOP_READY) {
    if (!T) {
      return 0;
    } else {
      T->x.c = 0;
      return 1;
    }
  } else {
    if (T) {
      return 0;
    } else {
      worker_tree = tree_insert_worker (worker_tree, w, lrand48 ());
      worker_queue = queue_add_worker (worker_queue, pid);
      if (delay_queue) {
        struct kitten_php_delay *q = queue_get_first_delay (delay_queue)->x;
        delay_queue = queue_del_first_delay (delay_queue);
        kitten_php_delay_restart (&q->ev);
      }
      return 1;
    }
  }
}

int kitten_php_current_count (void) {
  struct rpc_cluster *C = CurConf->schema_extra[KittenPhpNum];
  if (!C) { 
    return -1; 
  }
  return tree_count_worker (worker_tree);
}

int rpc_fun_kitten_php_start_stop (void **IP, void **Data) {
  int op = (long)*Data;
  if (op == RPC_READY || op == RPC_STOP_READY) {
    if (CQ->in_type != tl_type_conn) { return 0; }
    struct connection *c = CQ->extra;
    tl_fetch_skip (12);
    vkprintf (2, "Kitten php %s\n", op == RPC_READY ? "connected" : "disconnected");
    int res = kitten_php_ready (op, c);
    vkprintf (2, "Kitten_php_ready: res = %d, fetch_error = %s, new_size = %d\n", res, tl.error, kitten_php_current_count ());
    return 0;
  }
  RPC_FUN_NEXT;
}

SCHEMA_ADD(kitten_php) {
  if (C->methods.forward) {
    return -1;
  }
  if (RC->schema_extra[KittenPhpNum]) {
    return -1;
  }
  C->flags |= CF_ALLOW_EMPTY_CLUSTER;
  C->flags |= CF_FORBID_FORCE_FORWARD;
  RC->schema_extra[KittenPhpNum] = C;
  C->methods.forward = kitten_php_forward;
  Zconf->lock |= (1 << RPC_FUN_CUSTOM_OP);
 
  assert (Zconf->funs_last[RPC_FUN_CUSTOM_OP] > 0);
  Zconf->funs[RPC_FUN_CUSTOM_OP][--Zconf->funs_last[RPC_FUN_CUSTOM_OP]] = rpc_fun_kitten_php_start_stop;
  return 0;
}

SCHEMA_REGISTER_NUM(kitten_php,0,KittenPhpNum);

SCHEMA_ADD(kitten_php_secure) {
  if (C->methods.forward) {
    return -1;
  }
  if (RC->schema_extra[KittenPhpNum]) {
    return -1;
  }
  C->flags |= CF_ALLOW_EMPTY_CLUSTER;
  C->flags |= CF_FORBID_FORCE_FORWARD;
  RC->schema_extra[KittenPhpNum] = C;
  C->methods.forward = kitten_php_queue_forward;
  Zconf->lock |= (1 << RPC_FUN_CUSTOM_OP);
 
  assert (Zconf->funs_last[RPC_FUN_CUSTOM_OP] > 0);
  Zconf->funs[RPC_FUN_CUSTOM_OP][--Zconf->funs_last[RPC_FUN_CUSTOM_OP]] = rpc_fun_kitten_php_start_stop;
  return 0;
}

SCHEMA_REGISTER_NUM(kitten_php_secure,0,KittenPhpSecureNum);

int skip_response_inderect_on_answer (void **IP, void **Data) {
  int op = tl_fetch_lookup_int ();
  if (op == TL_RESPONSE_INDERECT) {
    return 1;
  }
  RPC_FUN_NEXT;
}

EXTENSION_ADD(skip_response_inderect) {
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ANSWER);
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ANSWER] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ANSWER][--Z->funs_last[RPC_FUN_QUERY_ON_ANSWER]] = skip_response_inderect_on_answer;

  return 0;
}

EXTENSION_REGISTER(skip_response_inderect,3)
