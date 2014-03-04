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
#include "vv-tree.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"

#include <assert.h>
int double_receive_count;

extern long long forwarded_queries;
extern long long sent_answers;
extern long long skipped_answers;


struct rpc_query_type default_double_receive_rpc_query_type;

int default_double_send_query_forward_conn (struct rpc_cluster_bucket *B, void *conn, long long new_qid, long long new_actor_id, int advance, int first) {
  vkprintf (1, "default_double_send_query_forward: CC->id = %lld, CC->timeout = %lf, new_qid = %lld\n", CC->id, CC->timeout, new_qid);
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
  if (first) {
    CC->methods.create_rpc_query (new_qid);
  }
  tl_store_end_ext (CQ->h->real_op);
  return 0;
}

/*struct rpc_query *default_double_receive_create_rpc_query (long long new_qid) {
  struct connection *c = CQ->in;
  return create_rpc_query (new_qid, RPCS_DATA(c)->remote_pid, CQ->h->qid, c, default_double_receive_rpc_query_type, CQ->h->custom_timeout * 0.001);
}*/

int default_double_send_query_forward (struct rpc_cluster_bucket *B, long long new_qid) {
  assert (B);
  if (!tl_fetch_error ()) {
    void *ca[3];
    int n = B->methods->get_multi_conn (B, ca, 3);
    if (n >= 1) {
      if (tl_fetch_error ()) {
        return -1;
      }
      int i;
      for (i = 0; i < n; i++) {
        assert (default_double_send_query_forward_conn (B, ca[i], new_qid, B->methods->get_actor (B), 0, i == 0) >= 0);
      }
      int offset = tl_fetch_unread ();
      assert (tl_fetch_move (offset) == offset);
    } else {
      void *conn = B->methods->get_conn (B);
      if (!conn) {
        //tl_fetch_set_error ("Can not find working connection to target", TL_ERROR_NO_CONNECTIONS);
        //return -1;
        void *E[2];
        E[0] = B;
        E[1] = &new_qid;
        return RPC_FUN_START(CC->methods.fun_pos[RPC_FUN_ON_NET_FAIL], E);
      }
      assert (default_query_forward_conn (B, conn, B->methods->get_actor (B), new_qid, 1) >= 0);
    }
    return 0;
  } else {
    return -1;
  }
}

void default_double_receive_on_answer (struct rpc_query *q) {
  struct connection *ca[3];
  int t = rpc_target_choose_random_connections (rpc_target_lookup (&q->pid), &q->pid, 2, ca);
  if (t >= 0 && q->in_type == tl_type_conn) {
    int i;
    int p = tl_fetch_unread ();
    for (i = 0; i < t; i ++) {
      struct connection *d = ca[i];
      tl_store_init (d, q->old_qid);
      tl_store_header (CQ->h);
      tl_copy_through (tl_fetch_unread (), 0);
      tl_store_end ();
    }
    sent_answers ++;
    assert (tl_fetch_move (p) == p);
  } else {
    if (tl_init_store (q->in_type, /*q->in,*/ &q->pid, q->old_qid) < 0) {
      skipped_answers ++;
      return;
    }
    tl_store_header (CQ->h);
    sent_answers ++;

    tl_copy_through (tl_fetch_unread (), 1);
    tl_store_end ();
  }
}

/* Double receive {{{ */

#pragma pack(push,4)
struct double_receive {
  struct process_id PID;
  long long qid;
};
#pragma pack(pop)

#define double_receive_cmp(a,b) (memcmp (a, b, sizeof (struct double_receive)))
DEFINE_TREE_STD_ALLOC (double_receive,struct double_receive *,double_receive_cmp,int,std_int_compare)
DEFINE_QUEUE (double_receive,struct double_receive *)

struct tree_double_receive *double_receive_tree;
struct queue_double_receive *double_receive_queue;
#define DOUBLE_RECEIVE_QUEUE_SIZE (1 << 16)

void dump (struct double_receive *s) {
  vkprintf (2, "DUMP: PID: %u.%d.%d.%d, qid = %lld, %p\n", s->PID.ip, s->PID.port, s->PID.utime, s->PID.pid, s->qid, s);
}

int double_receive_add (struct process_id PID, long long qid) {
  static struct double_receive e;
  e.PID = PID;
  e.qid = qid;
  if (tree_lookup_double_receive (double_receive_tree, &e)) {
    return 0;
  }
//  tree_check_double_receive (double_receive_tree);
  while (double_receive_count >= DOUBLE_RECEIVE_QUEUE_SIZE) {
    struct double_receive *s = queue_get_first_double_receive (double_receive_queue)->x;
    assert (s);
    double_receive_queue = queue_del_first_double_receive (double_receive_queue);
    double_receive_tree = tree_delete_double_receive (double_receive_tree, s);
    vkprintf (2, "Deleting double_receive PID: %u.%d.%d.%d, qid = %lld, %p\n", s->PID.ip, s->PID.port, s->PID.utime, s->PID.pid, s->qid, s);
    zfree (s, sizeof (*s));
    double_receive_count --;
  }
//  tree_check_double_receive (double_receive_tree);
  struct double_receive *s = zmalloc (sizeof (*s));
  s->PID = PID;
  s->qid = qid;
  vkprintf (2, "adding double_receive PID: %u.%d.%d.%d, qid = %lld, %p\n", s->PID.ip, s->PID.port, s->PID.utime, s->PID.pid, s->qid, s);
//  if (verbosity > 1) {
//    tree_act_double_receive (double_receive_tree, dump);
//  }
  double_receive_tree = tree_insert_double_receive (double_receive_tree, s, lrand48 ());
  double_receive_queue = queue_add_double_receive (double_receive_queue, s);
//  if (verbosity > 1) {
//    tree_act_double_receive (double_receive_tree, dump);
//  }
  double_receive_count ++;
//  tree_check_double_receive (double_receive_tree);
  return 1;
}

/* }}} */

int double_receive_on_receive (void) {
  int t = double_receive_add (*CQ->remote_pid, CQ->h->qid);
  assert (t >= 0);
  if (!t) {
    vkprintf (2, "Duplicate query. Skipping.");
    return SKIP_ALL_BYTES;
  }
  return 0;
}


int rpc_fun_double_receive_on_receive (void **IP, void **Data) {
  int t = double_receive_add (*CQ->remote_pid, CQ->h->qid);
  assert (t >= 0);
  if (!t) {
    vkprintf (2, "Duplicate query. Skipping.");
    return -1;
  }
  RPC_FUN_NEXT;
}

int rpc_fun_double_receive_on_answer (void **IP, void **Data) {
  struct rpc_query *q = *Data;
  default_double_receive_on_answer (q);
  return 0;
}

EXTENSION_ADD(double_receive) {
  assert (Z->funs_last[RPC_FUN_ON_RECEIVE] > 0);
  Z->funs[RPC_FUN_ON_RECEIVE][--Z->funs_last[RPC_FUN_ON_RECEIVE]] = rpc_fun_double_receive_on_receive;
  Z->lock |= (1 << RPC_FUN_ON_RECEIVE);
  if (Z->lock & (1 << RPC_FUN_QUERY_ON_ANSWER)) {
    return -1;
  }
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ANSWER] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ANSWER][--Z->funs_last[RPC_FUN_QUERY_ON_ANSWER]] = rpc_fun_double_receive_on_answer;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ANSWER);
  return 0;
}

EXTENSION_ADD(double_send) {
  if (Z->lock & (1 << RPC_FUN_FORWARD_TARGET)) {
    return -1;
  }
  //C->flags |= CF_FORBID_FORCE_FORWARD;
  C->methods.forward_target = default_double_send_query_forward;
  Z->lock |= (1 << RPC_FUN_FORWARD_TARGET);
  return 0;
}

EXTENSION_REGISTER(double_send,1)
EXTENSION_REGISTER(double_receive,0)
