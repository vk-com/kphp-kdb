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
#include "net-rpc-targets.h"

#include "kdb-rpc-proxy-binlog.h"

#include <assert.h>

#define RPC_REQ_RESULT_ACK 0x940320aa
#define RPC_REQ_RESULT_ACK_ACK 0x08e411ac

int secure_send_num;
int secure_receive_num;
int secure_send_binlog_num;
int secure_receive_binlog_num;

#pragma pack(push,4)
struct secure_send_extra {
  int state;
  int data_size;
  void *data;
  struct process_id pid;
  double timeout;
  int binlog;
  long long binlog_pos;
};

struct secure_receive_answer {
  long long qid;
  struct process_id pid;
  int answer_len;
  int answer_op;
  void *answer;
  struct tl_query_header *h;
  int binlog;
  long long binlog_pos;
};

struct binlog_pos {
  long long pos;
  unsigned crc32;
  int timestamp;
};
#pragma pack(pop)

extern int binlog_mode_on;

static long long resent_queries;
static long long resent_answer_ack;
static long long secure_send_s0;
static long long secure_send_s1;
static long long received_answer_ack_ack;
static long long received_answer_ack;
static long long secure_answer_allocated;
static long long sent_answer_ack_ack;
static long long sent_answer_ack;

#define secure_receive_answer_cmp(a,b) (memcmp (a, b, 14))
DEFINE_TREE_STD_ALLOC (secure_receive_answer,struct secure_receive_answer *,secure_receive_answer_cmp,int,std_int_compare)
struct tree_secure_receive_answer *secure_receive_answer_tree;

#define binlog_pos_cmp(a,b) ((a).pos - (b).pos)
DEFINE_TREE_STD_ALLOC (binlog_pos,struct binlog_pos,binlog_pos_cmp,int,std_int_compare)
struct tree_binlog_pos *binlog_pos_tree;

void update_index (long long pos, unsigned crc32, int timestamp);

struct binlog_pos get_cur_binlog_position (int replay) {
  struct binlog_pos t;
  if (replay) {
    relax_log_crc32 (0);
    assert (log_cur_pos () == log_crc32_pos);
  } else {
    relax_write_log_crc32 ();
    assert (log_last_pos () == log_crc32_pos);
  }
  //fprintf (stderr, "replay = %d, log_last_pos = %lld, log_crc32_pos = %lld\n", replay, log_last_pos (), log_crc32_pos);
  t.pos = log_crc32_pos;
  t.crc32 = ~log_crc32_complement;
  t.timestamp = now;
  return t;
}

void delete_binlog_pos (long long pos, int binlog_replay) {
  struct binlog_pos t;
  t.pos = pos;
  t.crc32 = 0;
  t.timestamp = 0;
  binlog_pos_tree = tree_delete_binlog_pos (binlog_pos_tree, t);
  if (!binlog_pos_tree) {
    t = get_cur_binlog_position (binlog_replay);
  } else {
    struct tree_binlog_pos *T = tree_get_min_binlog_pos (binlog_pos_tree);
    assert (T);
    t = T->x;
  }
  update_index (t.pos, t.crc32, t.timestamp);
}

void insert_binlog_pos (long long pos, int binlog_replay) {
  struct binlog_pos t;
  t = get_cur_binlog_position (binlog_replay);
  assert (t.pos == pos);
  binlog_pos_tree = tree_insert_binlog_pos (binlog_pos_tree, t, lrand48 ());
}

void query_forget (long long qid) {
  struct rpc_query *q = get_rpc_query (qid);    
  if (!q) { return; }
  struct secure_send_extra *E = q->extra;
  delete_binlog_pos (E->binlog_pos, 1);
  query_on_free (q);
}

void answer_forget (long long qid, struct process_id *pid) {
  struct secure_receive_answer t;
  t.qid = qid;
  t.pid = *pid;
  struct tree_secure_receive_answer *T = tree_lookup_secure_receive_answer (secure_receive_answer_tree, &t);
  if (T) {
    secure_receive_answer_tree = tree_delete_secure_receive_answer (secure_receive_answer_tree, T->x);
    struct secure_receive_answer *A = T->x;
    secure_answer_allocated --;
    free (A->answer);
    if (A->h) {
      tl_query_header_delete (A->h);
    }
    delete_binlog_pos (A->binlog_pos, 1);
    zfree (A, sizeof (*A));
  }
}

void query_tx (long long new_qid, long long qid, struct process_id *pid, long long cluster_id, double timeout, int size, const int *data) {
  struct rpc_query_type qt;
  CC = get_cluster_by_id (cluster_id);
  qt.on_answer = CC->methods.funs + CC->methods.fun_pos[RPC_FUN_QUERY_ON_ANSWER];
  qt.on_alarm = CC->methods.funs + CC->methods.fun_pos[RPC_FUN_QUERY_ON_ALARM];
  qt.on_free = CC->methods.funs + CC->methods.fun_pos[RPC_FUN_QUERY_ON_FREE];
  struct rpc_query *q = create_rpc_query (new_qid, *pid, qid, tl_type_none, qt, timeout);
  assert (q);
  struct secure_send_extra *E = zmalloc (sizeof (*E));
  E->state = 0;
  E->data_size = size;
  E->data = malloc (E->data_size);
  memcpy (E->data, data, E->data_size);
  E->pid = *pid;
  E->timeout = timeout;
  E->binlog = 1;
  q->extra = E;
  secure_send_s0 ++;
  insert_binlog_pos (log_cur_pos (), 1);
  E->binlog_pos = log_cur_pos ();
}

void answer_rx (long long qid) {
  struct rpc_query *q = get_rpc_query (qid);
  if (!q) { return; }
  remove_event_timer (&q->ev);
  struct secure_send_extra *E = q->extra;
  if (E->state == 1) {
    q->ev.wakeup_time = precise_now + E->timeout;
    insert_event_timer (&q->ev);
    return;
  }
  secure_send_s0 --;
  secure_send_s1 ++;
  E->state ++;
  free (E->data);
  E->data_size = 0;
  E->data = 0;
  q->ev.wakeup_time = precise_now + E->timeout;
  insert_event_timer (&q->ev);
}

void answer_tx (long long qid, struct process_id *pid, int op, int answer_len, int *answer) {
  struct secure_receive_answer *A = zmalloc (sizeof (*A));
  A->h = 0;
  A->qid = qid;
  A->pid = *pid;
  A->answer_op = op;
  A->binlog = 1;
  A->answer_len = answer_len;
  A->answer = malloc (A->answer_len);
  memcpy (A->answer, answer, answer_len);
  secure_receive_answer_tree = tree_insert_secure_receive_answer (secure_receive_answer_tree, A, lrand48 ());
  secure_answer_allocated ++;
  insert_binlog_pos (log_cur_pos (), 1);
  A->binlog_pos = log_cur_pos ();
}

void resend_query (struct rpc_query *q, struct process_id *pid) {
  resent_queries ++;
  struct connection *d = rpc_target_choose_connection (rpc_target_lookup_hp (pid->ip, pid->port), 0);
  if (!d) { return; }
  tl_store_init (d, q->qid);
  struct secure_send_extra *E = q->extra;
  tl_store_raw_data (E->data, E->data_size);
  tl_store_end_ext (RPC_INVOKE_REQ);
}

void resend_answer_ack (struct rpc_query *q, struct process_id *pid) {  
  resent_answer_ack ++;
  struct connection *d = rpc_target_choose_connection (rpc_target_lookup_hp (pid->ip, pid->port), 0);
  if (!d) { return; }
  tl_store_init (d, q->qid);
  tl_store_end_ext (RPC_REQ_RESULT_ACK);
}

int secure_send_on_alarm (void **IP, void **Data) {
  struct rpc_query *q = *Data;
  struct secure_send_extra *E = q->extra;
  vkprintf (2, "fun %s, E->state = %d\n", __func__, E->state);
//  CC = get_cluster_by_id (E->h->actor_id);
  switch (E->state) {
  case 0:
    resend_query (q, &E->pid);
    q->ev.wakeup_time = precise_now + E->timeout;
    insert_event_timer (&q->ev);
    return 1;
  case 1:
    resend_answer_ack (q, &E->pid);
    q->ev.wakeup_time = precise_now + E->timeout;
    insert_event_timer (&q->ev);
    return 1;
  default:
    assert (0);
    return 1;
  }
}

int secure_send_on_answer (void **IP, void **Data) {
  vkprintf (2, "fun %s\n", __func__);
  struct rpc_query *q = *Data;
  struct secure_send_extra *E = q->extra;
  if (E->state == 1) {
    //resend_answer_ack (q, &E->pid);
    q->ev.wakeup_time = precise_now + E->timeout;
    insert_event_timer (&q->ev);
    return 1;
  }
  secure_send_s0 --;
  secure_send_s1 ++;
  E->state ++;
  free (E->data);
  E->data_size = 0;
  E->data = 0;
  if (!tl_fetch_error ()) {
    tl_init_store (CQ->in_type, CQ->remote_pid, q->qid);
    //tl_store_init (CQ->in, q->qid);
    tl_store_end_ext (RPC_REQ_RESULT_ACK);
    sent_answer_ack ++;
  }
  if (E->binlog) {
    struct lev_answer_rx *L = alloc_log_event (LEV_ANSWER_RX, sizeof (*L), 0);
    L->qid = q->qid;
    if (binlog_mode_on & 2) {
      flush_cbinlog (0);
    }
  }
  int r = ((rpc_fun_t)(*IP))(IP + 1, Data);
  if (r < 0) { return r; }
  q->ev.wakeup_time = precise_now + E->timeout;
  insert_event_timer (&q->ev);
  return 1;
}

int secure_send_on_free (void **IP, void **Data) {
  vkprintf (2, "fun %s\n", __func__);
  struct rpc_query *q = *Data;
  struct secure_send_extra *E = q->extra;
  assert (E->state == 1);
  secure_send_s1 --;
  if (E->data) { free (E->data); }
  zfree (E, sizeof (*E));
  RPC_FUN_NEXT;
}

int secure_send_answer_ack_ack (void **IP, void **Data) {
  vkprintf (2, "fun %s\n", __func__);
  int op = (long)*Data;
  if (op == RPC_REQ_RESULT_ACK_ACK) {
    received_answer_ack_ack ++;
    tl_fetch_move (12);
    long long qid = tl_fetch_long ();
    struct rpc_query *q = get_rpc_query (qid);    
    if (!q) { return 0; }
    struct secure_send_extra *E = q->extra;
    if (E->binlog) {
      struct lev_query_forget *L = alloc_log_event (LEV_QUERY_FORGET, sizeof (*L), 0);
      L->qid = qid;
      delete_binlog_pos (E->binlog_pos, 0);
      if (binlog_mode_on & 2) {
        flush_cbinlog (0);
      }
    }
    query_on_free (q);
    return -1;
  }
  RPC_FUN_NEXT;
}

struct rpc_query *_secure_send_create_rpc_query (long long new_qid, int binlog) {
  struct rpc_query *q = default_create_rpc_query (new_qid);
  if (!q) { return q; }
  struct secure_send_extra *E = zmalloc (sizeof (*E));
  E->state = 0;
  E->data_size = tl.out_pos;
  E->data = malloc (E->data_size);
  memset (E->data, 0, E->data_size);
  assert (TL_OUT_TYPE == tl_type_conn);
  E->pid = RPCS_DATA(TL_OUT_CONN)->remote_pid;
  E->timeout = CC->timeout;
  E->binlog = 0;
  assert (tl_store_read_back_nondestruct (E->data, E->data_size) == E->data_size);
  secure_send_s0 ++;
/*  int i;
  for (i = 0; i < E->data_size / 4; i++) {
    fprintf (stderr, "%08x ", ((int *)E->data)[i]);
  }
  fprintf (stderr, "\n");*/
  q->extra = E;
  if (binlog) {
    E->binlog_pos = log_last_pos ();
    insert_binlog_pos (E->binlog_pos, 0);
    struct lev_query_tx *L = alloc_log_event (LEV_QUERY_TX, sizeof (*L) + E->data_size, 0);
    L->qid = q->qid;
    L->old_qid = q->old_qid;
    L->cluster_id = CC->id;    
    L->data_size = E->data_size;
    L->pid = E->pid;
    L->timeout = E->timeout;
    memcpy (L->data, E->data, E->data_size);
    E->binlog = 1;
    if (binlog_mode_on & 2) {
      flush_cbinlog (0);
    }
  }
  return q;
}

struct rpc_query *secure_send_create_rpc_query (long long new_qid) {
  return _secure_send_create_rpc_query (new_qid, 0);
}

struct rpc_query *secure_send_create_rpc_query_binlog (long long new_qid) {
  return _secure_send_create_rpc_query (new_qid, 1);
}

int _secure_receive_on_alarm (void **IP, void **Data, int binlog) {
  struct rpc_query *q = *Data;
  struct secure_receive_answer *A = zmalloc (sizeof (*A));
  A->h = 0;
  A->qid = q->old_qid;
  A->pid = q->pid;
  A->answer_op = RPC_REQ_ERROR_WRAPPED;
  static char buf [1000];
  sprintf (buf + 1, "Query timeout: working_time = %lf", precise_now - q->start_time);
  tl_fetch_set_error (buf + 1, TL_ERROR_QUERY_TIMEOUT);
  int len = strlen (buf + 1) + 1;
  int pad = ((len + 3) & ~3) - len;
  memset (buf + len, 0, pad);
  buf[0] = len;
  len += pad;
  assert (len % 4 == 0);
  A->answer_len = 4 + len;
  A->answer = malloc (A->answer_len);
  A->binlog = 0;
  *(int *)A->answer = TL_ERROR_QUERY_TIMEOUT;
  memcpy (((char *)(A->answer)) + 4, buf, len);
  secure_receive_answer_tree = tree_insert_secure_receive_answer (secure_receive_answer_tree, A, lrand48 ());
  secure_answer_allocated ++;
  if (binlog) {
    A->binlog_pos  = log_last_pos ();
    insert_binlog_pos (A->binlog_pos, 0);
    struct lev_answer_tx *L = alloc_log_event (LEV_ANSWER_TX, sizeof (*L) + A->answer_len, 0);
    L->qid = A->qid;
    L->pid = A->pid;
    L->op = A->answer_op;
    L->answer_len = A->answer_len;
    memcpy (L->answer, A->answer, A->answer_len);
    A->binlog = 1;
    if (binlog_mode_on & 2) {
      flush_cbinlog (0);
    }
  }
  RPC_FUN_NEXT;
}

int secure_receive_on_alarm (void **IP, void **Data) {
  return _secure_receive_on_alarm (IP, Data, 0);
}

int secure_receive_on_alarm_binlog (void **IP, void **Data) {
  return _secure_receive_on_alarm (IP, Data, 1);
}

int _secure_receive_on_answer (void **IP, void **Data, int binlog) {
  struct rpc_query *q = *Data;
  struct secure_receive_answer *A = zmalloc (sizeof (*A));
  A->qid = q->old_qid;
  A->pid = q->pid;
  //A->h = tl_query_header_dup (CQ->h);
  A->h = 0;
  static char buf[(1 << 17)];
  int r = tl_write_header (CQ->h, (int *)buf, (1 << 17));
  assert (r >= 0);
  A->answer_op = CQ->h->real_op;
  A->answer_len = tl_fetch_unread () + r;
  A->answer = malloc (A->answer_len);
  A->binlog = 0;
  memcpy (A->answer, buf, r);

  //int t = 
  tl_fetch_lookup_data (A->answer + r, A->answer_len - r);
  secure_receive_answer_tree = tree_insert_secure_receive_answer (secure_receive_answer_tree, A, lrand48 ());
  secure_answer_allocated ++;
  if (binlog) {
    A->binlog_pos  = log_last_pos ();
    insert_binlog_pos (A->binlog_pos, 0);
    struct lev_answer_tx *L = alloc_log_event (LEV_ANSWER_TX, sizeof (*L) + A->answer_len, 0);
    L->qid = A->qid;
    L->pid = A->pid;
    L->op = A->answer_op;
    L->answer_len = A->answer_len;
    memcpy (L->answer, A->answer, A->answer_len);
    A->binlog = 1;
    if (binlog_mode_on & 2) {
      flush_cbinlog (0);
    }
  }
  RPC_FUN_NEXT;
}

int secure_receive_on_answer (void **IP, void **Data) {
  return _secure_receive_on_answer (IP, Data, 0);
}

int secure_receive_on_answer_binlog (void **IP, void **Data) {
  return _secure_receive_on_answer (IP, Data, 1);
}

int secure_receive_answer_ack (void **IP, void **Data) {
  int op = (long)*Data;
  if (op == RPC_REQ_RESULT_ACK) {
    vkprintf (2, "fun %s\n", __func__);
    tl_fetch_move (12);
    received_answer_ack ++;
    //struct connection *c = CQ->in;
    struct secure_receive_answer t;
    t.qid = tl_fetch_long ();
//    tl_fetch_raw_data (&t, 8);
    if (tl_fetch_error ()) {
      return -1;
    }
    t.pid = *CQ->remote_pid;
    //t.pid = RPCC_DATA(c)->remote_pid;
    struct tree_secure_receive_answer *T = tree_lookup_secure_receive_answer (secure_receive_answer_tree, &t);
    if (T) {
      secure_receive_answer_tree = tree_delete_secure_receive_answer (secure_receive_answer_tree, T->x);
      struct secure_receive_answer *A = T->x;
      secure_answer_allocated --;
      free (A->answer);
      if (A->h) {
        tl_query_header_delete (A->h);
      }
      if (A->binlog) {
        struct lev_answer_forget *L = alloc_log_event (LEV_ANSWER_FORGET, sizeof (*L), 0);
        L->qid = t.qid;
        L->pid = t.pid;
        delete_binlog_pos (A->binlog_pos, 0);
        if (binlog_mode_on & 2) {
          flush_cbinlog (0);
        }
      }
      zfree (A, sizeof (*A));
    }
    //tl_store_init (c, t.qid);
    tl_init_store (CQ->in_type, CQ->remote_pid, t.qid);
    tl_store_end_ext (RPC_REQ_RESULT_ACK_ACK);
    sent_answer_ack_ack ++;
    return -1;
  }
  RPC_FUN_NEXT;
}

int rpc_fun_secure_receive_on_receive (void **IP, void **Data) {
  struct secure_receive_answer t;
  t.qid = CQ->h->qid;
  t.pid = *CQ->remote_pid;
  struct tree_secure_receive_answer *T = tree_lookup_secure_receive_answer (secure_receive_answer_tree, &t);
  if (!T) {
    RPC_FUN_NEXT;
  } else {
    struct secure_receive_answer *A = T->x;
    assert (A->answer);
    tl_init_store (CQ->in_type, CQ->remote_pid, CQ->h->qid);
    //tl_store_init_any (CQ->in_type, CQ->in, CQ->h->qid);
    if (A->h) {
      tl_store_header (A->h);
    }
    tl_store_raw_data (A->answer, A->answer_len);
    tl_store_end_ext (A->answer_op);
    return -1;
  }
}

int secure_send_global_stat (char *buf, int len) {
  return snprintf (buf, len, 
    "resent_queries\t%lld\n"
    "resent_answer_ack\t%lld\n"
    "secure_send_state0\t%lld\n"
    "secure_send_state1\t%lld\n"
    "received_answer_ack_ack\t%lld\n"
    "sent_answer_ack\t%lld\n",
    resent_queries,
    resent_answer_ack,
    secure_send_s0,
    secure_send_s1,
    received_answer_ack_ack,
    sent_answer_ack);
}

int secure_receive_global_stat (char *buf, int len) {
  return snprintf (buf, len, 
    "secure_answer_allocated\t%lld\n"
    "receive_answer_ack\t%lld\n"
    "sent_answer_ack_ack\t%lld\n"
    ,
    secure_answer_allocated,
    received_answer_ack,
    sent_answer_ack_ack
  );
}

int _secure_send_on_net_fail (void **IP, void **Data, int binlog) {
  long long new_qid = get_free_rpc_qid (0); //*(long long *)(Data[1]);
  struct rpc_query *q = default_create_rpc_query (new_qid);
  assert (q);
  struct secure_send_extra *E = zmalloc (sizeof (*E));
  static int buf[10000];
  
  struct tl_query_header *h = CQ->h;
  long long qid = h->qid;
  assert (h->actor_id == CC->id);
  h->qid = new_qid;
  h->actor_id = CC->new_id;
  int len = tl_write_header (CQ->h, buf, 10000);
  h->qid = qid;
  h->actor_id = CC->id;
  assert (len >= 0);

  E->state = 0;
  E->data_size = tl_fetch_unread () + len;
  E->data = malloc (E->data_size);
  memset (E->data, 0, E->data_size);
  struct rpc_cluster_bucket *B = *Data;
  assert (B);
  E->pid.ip = B->methods->get_host (B);
  E->pid.port = B->methods->get_port (B);
  E->timeout = CC->timeout;
  E->binlog = 0;

  memcpy (E->data, buf, len);
  tl_fetch_raw_data (E->data + len, E->data_size - len);
  secure_send_s0 ++;
  q->extra = E;
  if (binlog) {
    E->binlog_pos = log_last_pos ();
    insert_binlog_pos (E->binlog_pos, 0);
    struct lev_query_tx *L = alloc_log_event (LEV_QUERY_TX, sizeof (*L) + E->data_size, 0);
    L->qid = q->qid;
    L->old_qid = q->old_qid;
    L->cluster_id = CC->id;    
    L->data_size = E->data_size;
    L->pid = E->pid;
    L->timeout = E->timeout;
    memcpy (L->data, E->data, E->data_size);
    E->binlog = 1;
    if (binlog_mode_on & 2) {
      flush_cbinlog (0);
    }
  }
  return 0;
}

int secure_send_on_net_fail (void **IP, void **Data) {
  return _secure_send_on_net_fail (IP, Data, 0);
}

int secure_send_on_net_fail_binlog (void **IP, void **Data) {
  return _secure_send_on_net_fail (IP, Data, 1);
}

EXTENSION_ADD(secure_send) {
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ALARM] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ALARM][--Z->funs_last[RPC_FUN_QUERY_ON_ALARM]] = secure_send_on_alarm;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ALARM);
  
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ANSWER] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ANSWER][--Z->funs_last[RPC_FUN_QUERY_ON_ANSWER]] = secure_send_on_answer;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ANSWER);
  
  assert (Z->funs_last[RPC_FUN_QUERY_ON_FREE] > 0);
  Z->funs[RPC_FUN_QUERY_ON_FREE][--Z->funs_last[RPC_FUN_QUERY_ON_FREE]] = secure_send_on_free;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_FREE);
  
  assert (Z->funs_last[RPC_FUN_ON_NET_FAIL] > 0);
  Z->funs[RPC_FUN_ON_NET_FAIL][--Z->funs_last[RPC_FUN_ON_NET_FAIL]] = secure_send_on_net_fail;  
  Z->lock |= (1 << RPC_FUN_ON_NET_FAIL);  
 
  assert (Zconf->funs_last[RPC_FUN_CUSTOM_OP] > 0);
  Zconf->funs[RPC_FUN_CUSTOM_OP][--Zconf->funs_last[RPC_FUN_CUSTOM_OP]] = secure_send_answer_ack_ack;
  Zconf->lock |= (1 << RPC_FUN_CUSTOM_OP);
  
 
  if (Z->lock & (1 << RPC_FUN_CREATE_RPC_QUERY)) {
    return -1;
  }
  C->methods.create_rpc_query = secure_send_create_rpc_query;
  Z->lock |= (1 << RPC_FUN_CREATE_RPC_QUERY);
  return 0;
}
EXTENSION_REGISTER_NUM_STAT(secure_send,2,secure_send_num,secure_send_global_stat)


EXTENSION_ADD(secure_receive) {
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ALARM] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ALARM][--Z->funs_last[RPC_FUN_QUERY_ON_ALARM]] = secure_receive_on_alarm;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ALARM);
  
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ANSWER] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ANSWER][--Z->funs_last[RPC_FUN_QUERY_ON_ANSWER]] = secure_receive_on_answer;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ANSWER);
  
  assert (Zconf->funs_last[RPC_FUN_CUSTOM_OP] > 0);
  Zconf->funs[RPC_FUN_CUSTOM_OP][--Zconf->funs_last[RPC_FUN_CUSTOM_OP]] = secure_receive_answer_ack;
  Zconf->lock |= (1 << RPC_FUN_CUSTOM_OP);

  assert (Z->funs_last[RPC_FUN_ON_RECEIVE] > 0);
  Z->funs[RPC_FUN_ON_RECEIVE][--Z->funs_last[RPC_FUN_ON_RECEIVE]] = rpc_fun_secure_receive_on_receive;
  Z->lock |= (1 << RPC_FUN_ON_RECEIVE);
  
  return 0;  
}
EXTENSION_REGISTER_NUM_STAT(secure_receive,1,secure_receive_num,secure_receive_global_stat)

EXTENSION_ADD(secure_send_binlog) {
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ALARM] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ALARM][--Z->funs_last[RPC_FUN_QUERY_ON_ALARM]] = secure_send_on_alarm;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ALARM);
  
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ANSWER] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ANSWER][--Z->funs_last[RPC_FUN_QUERY_ON_ANSWER]] = secure_send_on_answer;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ANSWER);
  
  assert (Z->funs_last[RPC_FUN_QUERY_ON_FREE] > 0);
  Z->funs[RPC_FUN_QUERY_ON_FREE][--Z->funs_last[RPC_FUN_QUERY_ON_FREE]] = secure_send_on_free;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_FREE);
  
  assert (Z->funs_last[RPC_FUN_ON_NET_FAIL] > 0);
  Z->funs[RPC_FUN_ON_NET_FAIL][--Z->funs_last[RPC_FUN_ON_NET_FAIL]] = secure_send_on_net_fail_binlog;
  Z->lock |= (1 << RPC_FUN_ON_NET_FAIL);  
 
  assert (Zconf->funs_last[RPC_FUN_CUSTOM_OP] > 0);
  Zconf->funs[RPC_FUN_CUSTOM_OP][--Zconf->funs_last[RPC_FUN_CUSTOM_OP]] = secure_send_answer_ack_ack;
  Zconf->lock |= (1 << RPC_FUN_CUSTOM_OP);
  
 
  if (Z->lock & (1 << RPC_FUN_CREATE_RPC_QUERY)) {
    return -1;
  }
  C->methods.create_rpc_query = secure_send_create_rpc_query_binlog;
  Z->lock |= (1 << RPC_FUN_CREATE_RPC_QUERY);
  return 0;
}
EXTENSION_REGISTER_NUM_STAT(secure_send_binlog,2,secure_send_binlog_num,secure_send_global_stat)


EXTENSION_ADD(secure_receive_binlog) {
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ALARM] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ALARM][--Z->funs_last[RPC_FUN_QUERY_ON_ALARM]] = secure_receive_on_alarm_binlog;  
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ALARM);
  
  assert (Z->funs_last[RPC_FUN_QUERY_ON_ANSWER] > 0);
  Z->funs[RPC_FUN_QUERY_ON_ANSWER][--Z->funs_last[RPC_FUN_QUERY_ON_ANSWER]] = secure_receive_on_answer_binlog;
  Z->lock |= (1 << RPC_FUN_QUERY_ON_ANSWER);
  
  assert (Zconf->funs_last[RPC_FUN_CUSTOM_OP] > 0);
  Zconf->funs[RPC_FUN_CUSTOM_OP][--Zconf->funs_last[RPC_FUN_CUSTOM_OP]] = secure_receive_answer_ack;
  Zconf->lock |= (1 << RPC_FUN_CUSTOM_OP);

  assert (Z->funs_last[RPC_FUN_ON_RECEIVE] > 0);
  Z->funs[RPC_FUN_ON_RECEIVE][--Z->funs_last[RPC_FUN_ON_RECEIVE]] = rpc_fun_secure_receive_on_receive;
  Z->lock |= (1 << RPC_FUN_ON_RECEIVE);
  
  return 0;  
}
EXTENSION_REGISTER_NUM_STAT(secure_receive_binlog,1,secure_receive_binlog_num,secure_receive_global_stat)
