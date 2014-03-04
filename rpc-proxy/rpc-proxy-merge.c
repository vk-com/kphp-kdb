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
#include "rpc-proxy-merge.h"
#include "vv-tl-parse.h"
#include "net-rpc-targets.h"

int gathers_working;
long long gather_result_memory;
long long gathers_total;

int merge_init_response (struct gather *G) {
  return tl_init_store (G->in_type, /*G->in,*/ &G->pid, G->qid);
}

int empty_send (struct gather *G, int n) {
  return -1;
}

void empty_receive (struct gather *G, int n) {
}

void empty_end (struct gather *G) {
  merge_delete (G);
}
struct gather_methods gather_do_nothing_methods = {
  .on_start = 0,
  .on_send = empty_send,
  .on_error = empty_receive,
  .on_answer = empty_receive,
  .on_timeout = empty_receive,
  .on_end = empty_end,
  .on_send_end = empty_end
};

void merge_terminate_gather (struct gather *G) {
  G->methods = &gather_do_nothing_methods;
}

void merge_on_free (struct rpc_query *q);
void merge_on_answer (struct rpc_query *q);
void merge_on_alarm (struct rpc_query *q);

int query_merge_on_free (void **IP, void **Data) {
  struct rpc_query *q = *Data;
  merge_on_free (q);
  return 0;
}

int query_merge_on_answer (void **IP, void **Data) {
  struct rpc_query *q = *Data;
  merge_on_answer (q);
  return 0;
}

int query_merge_on_alarm (void **IP, void **Data) {
  struct rpc_query *q = *Data;
  merge_on_alarm (q);
  return 0;
}

rpc_fun_t _ans[1] = {query_merge_on_answer};
rpc_fun_t _fre[1] = {query_merge_on_free};
rpc_fun_t _ala[1] = {query_merge_on_alarm};

struct rpc_query_type merge_query_type = {
  .on_free = _fre,
  .on_answer = _ans,
  .on_alarm = _ala
};

void merge_on_alarm (struct rpc_query *q) {
  void **t = q->extra;
  struct gather *G = t[0];
  int num = (long)t[1];
  assert (num >= 0 && num <= G->tot_num - 1);
  assert (G->List[num].bytes == -2);
  G->timeouted_num ++;
  G->wait_num --;
  if (G->methods->on_timeout) {
    G->methods->on_timeout (G, num);
  }
  if (!G->wait_num) {
    G->methods->on_end (G);
  }
}

void merge_on_free (struct rpc_query *q) {
  free (q->extra);
  default_on_free (q);
}

void default_gather_on_answer (struct gather *G, int num) {
  int remaining_len = tl_fetch_unread ();
  assert (!(remaining_len & 3));
  G->List[num].bytes = remaining_len;
  G->List[num].data = malloc (remaining_len);
  tl_fetch_raw_data ((void *)G->List[num].data, remaining_len);
  gather_result_memory += remaining_len;
}

void default_gather_on_error (struct gather *G, int num) {
  G->List[num].bytes = -3;
}

int default_gather_on_send (struct gather *G, int num) {
  tl_copy_through (tl_fetch_unread (), 0);
  return 0;
}

void merge_save_query_remain (struct gather *G) {
  G->header = malloc (sizeof (*G->header));
  memcpy (G->header, &CQ->h, sizeof (*G->header));
  G->header->actor_id = CC->new_id;
  G->saved_query_len = tl_fetch_unread ();
  G->saved_query = malloc (G->saved_query_len);
  tl_fetch_lookup_data (G->saved_query, G->saved_query_len);
}

void merge_on_answer (struct rpc_query *q) {
  void **t = q->extra;
  struct gather *G = t[0];
  int num = (long)t[1];
  assert (num >= 0 && num <= G->tot_num - 1);
  assert (G->List[num].bytes == -2);
  G->wait_num --;
  if (CQ->h->op == RPC_REQ_RESULT) {
    G->received_num ++;
    if (G->methods->on_answer) {
      G->methods->on_answer (G, num);
    } else {
      default_gather_on_answer (G, num);
    }
  } else {
    G->errors_num ++;
    if (G->methods->on_error) {
      G->methods->on_error (G, num);
    } else {
      default_gather_on_error (G, num);
    }
  }
  if (!G->wait_num) {
    G->methods->on_end (G);
  }
}

void merge_delete (struct gather *G) {
  int i;
  for (i = 0; i < G->tot_num; i++) {
    if (G->List[i].bytes >= 0) {
      gather_result_memory -= G->List[i].bytes;
      free (G->List[i].data);
      G->List[i].bytes = -1;
    }
  }
  if (G->header) { free (G->header); }
  if (G->saved_query) { free (G->saved_query); }
  gathers_working --;
  free (G);
}

void merge_forward (struct gather_methods *methods) {
  assert (methods);
  if (tl_fetch_error ()) {
    return;
  }
  struct gather *G = malloc (sizeof (*G) + sizeof (struct gather_entry) * CC->tot_buckets);
  memset (G, 0, sizeof (*G));
  G->tot_num = CC->tot_buckets;
  int i;
  for (i = 0; i < G->tot_num; i++) {
    G->List[i].bytes = -1;
  }
  gathers_working ++;
  gathers_total ++;
  assert (methods);
  long long qid = CQ->h->qid;
  G->methods = methods;
  G->start_time = precise_now;
  G->wait_num = 0;
  //G->in = CQ->in;
  G->in_type = CQ->in_type;
  G->pid = *CQ->remote_pid;
  G->qid = qid;
  G->cluster = CC;
  if (methods->on_start) {
    G->extra = methods->on_start ();
    if (!G->extra) {
      merge_delete (G);
      return;
    }
  } else {
    G->extra = 0;
  }  
  assert (methods->on_end);
  struct tl_query_header *h = CQ->h;
  assert (h->actor_id == CC->id);
  h->actor_id = CC->new_id;
  for (i = 0; i < G->tot_num; i++) {
    long long new_qid = get_free_rpc_qid (qid);
//    rpc_proxy_init_store (&CC->buckets[i]);
    if (rpc_proxy_store_init (&CC->buckets[i], new_qid) <= 0) {
      G->not_sent_num ++;
      continue;
    }
  
    h->qid = new_qid;
    tl_store_header (h);

    int x;
    if (methods->on_send) {
      x = methods->on_send (G, i);
    } else {
      x = default_gather_on_send (G, i);
    }
    if (x >= 0) {
      tl_store_end_ext (RPC_INVOKE_REQ);
      G->List[i].bytes = -2;
      struct rpc_query *q = create_rpc_query (new_qid, G->pid, qid, G->in_type, /*G->in,*/ merge_query_type, h->custom_timeout * 0.001);
      q->extra = malloc (2 * sizeof (void *));
      ((void **)q->extra)[0] = G;
      ((void **)q->extra)[1] = (void *)(long)i;
      G->sent_num ++;
      G->wait_num ++;
    } else {
      tl_store_clear ();
      G->not_sent_num ++;
    }
  }
  h->qid = qid;
  h->actor_id = CC->id;
  if (methods->on_send_end) {
    methods->on_send_end (G);
  }
  if (!G->wait_num) {
    methods->on_end (G);
  }
}
