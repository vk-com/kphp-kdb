/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Vitaliy Valtman              
*/

#define _FILE_OFFSET_BITS 64
#include "vv-tl-aio.h"
#include "vv-tl-parse.h"
#include <stdio.h>
#include "kdb-data-common.h"
#include <memory.h>
#include "vv-tree.h"
#include "net-rpc-targets.h"

double tl_aio_timeout = 0.5;

DEFINE_QUEUE (finished_aio,struct tl_saved_query *);

static queue_finished_aio_t *finished_aio_queue;

void add_finished_query (struct tl_saved_query *q) {
  finished_aio_queue = queue_add_finished_aio (finished_aio_queue, q);
}


int tl_delete_aio_query (struct conn_query *q) {
  if (q->start_time > 0) {
    double q_time = get_utime (CLOCK_MONOTONIC) - q->start_time;
    total_aio_time += q_time;
    if (verbosity > 1) {
      fprintf (stderr, "delete_aio_query(%p): query time %.6f\n", q, q_time);
    }
  }
  active_aio_queries--;
  delete_conn_query (q);
  zfree (q, sizeof (struct conn_query));
  return 0;
}

int tl_aio_init_store (enum tl_type type, struct process_id *pid, long long qid) {
  if (type == tl_type_conn) {
    struct connection *d = rpc_target_choose_connection (rpc_target_lookup (pid), pid);
    if (d) {
      vkprintf (2, "Good connection\n");
      tl_store_init (d, qid);
      return 1;
    } else {
      vkprintf (2, "Bad connection\n");
      return -1;
    }
  } else if (type == tl_type_raw_msg) {
    struct udp_target *S = udp_target_lookup (pid);
    if (S && S->state != udp_failed) {
      tl_store_init_raw_msg (S, qid);
      return 1;
    } else {
      return -1;
    }
  } else {
    assert (0);
    return -1;
  }
}

void tl_aio_query_restart (struct tl_saved_query *s) {
  vkprintf (1, "%s\n", __func__);
// assert (s->out);
  assert (s->out_type == tl_type_conn || s->out_type == tl_type_raw_msg);
  assert (s->restart);
  int ok = 0;
  if (tl_aio_init_store (s->out_type, &s->pid, s->qid) >= 0) {
    ok = 1;
  }
  if (ok) {
    tl.attempt_num = s->attempt + 1;
    int x = s->restart (s);
    vkprintf (1, "restart result = %d, attempt_num = %d\n", x, s->attempt);
    if (x == -2) {
      if (s->attempt <= 3) {
        s->error_code = 0;
        assert (!s->wait_num);
        tl_store_clear ();
        tl_aio_start (WaitAioArr, WaitAioArrPos, tl_aio_timeout, s);        
        return;
      } else {
        s->error_code = TL_ERROR_AIO_MAX_RETRY_EXCEEDED;
        s->fail (s);
      }
    } else if (x < 0) {
      tl_fetch_set_error ("Unknown error", TL_ERROR_UNKNOWN);
    }
  } else {
    vkprintf (1, "Can not store result: connection is dead\n");
    tl_store_init (0, s->qid);
    s->fail (s);
  }
  tl_store_end ();
  s->free_extra (s);
  zfree (s, sizeof (*s));
}

void tl_aio_fail_start (struct tl_saved_query *s) {
  //assert (s->out);
  assert (s->out_type == tl_type_conn || s->out_type == tl_type_raw_msg);
  assert (s->fail);
  int ok = 0;
  if (tl_aio_init_store (s->out_type, &s->pid, s->qid) >= 0) {
    ok = 1;
  }
  if (!ok) {
    tl_store_init (0, s->qid);
  }
  s->fail (s);
  tl_store_end ();
}

void tl_aio_query_restart_all_finished (void) {
  while (finished_aio_queue) {
    tl_aio_query_restart (finished_aio_queue->x);
    finished_aio_queue = queue_del_first_finished_aio (finished_aio_queue);
  }
}

void tl_default_aio_fail (struct tl_saved_query *q) {
  tl_fetch_set_error ("Aio/binlog wait error", TL_ERROR_AIO_FAIL);
}

struct tl_saved_query *tl_saved_query_init (void) {
  struct tl_saved_query *q = zmalloc0 (sizeof (struct tl_saved_query));
  q->out_type = TL_IN_TYPE;
  q->qid = tl.out_qid;
  assert (TL_IN_PID);
  q->pid = *TL_IN_PID;
  q->attempt = tl.attempt_num;
  q->fail = tl_default_aio_fail;
  return q;
}

int tl_create_aio_query (struct aio_connection *a, struct connection *c, double timeout, struct conn_query_functions *cq, struct tl_saved_query *extra) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_query(%p[%d], %p[%d]): Q=%p\n", a, a->fd, c, c->fd, Q);
  }

  Q->custom_type = 0;
  Q->outbound = (struct connection *)a;
  Q->requester = c;
  Q->start_time = /*c->query_start_time*/get_utime (CLOCK_MONOTONIC);
  Q->extra = extra;
  Q->cq_type = cq;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  if (verbosity > 1 && a->first_query != (struct conn_query *) a) {
    fprintf (stderr, "!NOTICE! inserting second query to %p\n", a);
  }

  insert_conn_query (Q);
  c->generation ++;
  active_aio_queries++;
  tot_aio_queries++;

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query()\n");
  }

  return 1;
}

int tl_aio_on_timeout_default (struct conn_query *Q) {
  struct tl_saved_query *q = Q->extra;
  q->wait_num --;
  expired_aio_queries ++;
  if (!q->failed) {
    q->error_code = TL_ERROR_AIO_TIMEOUT;
    //q->fail (q);
    tl_aio_fail_start (q);
  }
  q->failed = 1;
  if (!q->wait_num) {
    q->free_extra (q);
    zfree (q, sizeof (*q));
  }
  tl_delete_aio_query (Q);
  return 0;
}

int tl_aio_on_fail_default (struct conn_query *Q) {
  struct tl_saved_query *q = Q->extra;
  q->wait_num --;
  //expired_aio_queries ++;
  if (!q->failed) {
    q->error_code = TL_ERROR_AIO_FAIL;
    //q->fail (q);
    tl_aio_fail_start (q);
  }
  q->failed = 1;
  if (!q->wait_num) {
    q->free_extra (q);
    zfree (q, sizeof (*q));
  }
  tl_delete_aio_query (Q);
  return 0;
}

int tl_aio_on_end_default (struct conn_query *Q) {
//  struct tl_saved_query *s = q->extra;
//  struct connection *c = s->c;
  struct tl_saved_query *q = Q->extra;
  q->wait_num --;
  if (!q->wait_num && !q->failed) {
    finished_aio_queue = queue_add_finished_aio (finished_aio_queue, q);
  }
  tl_delete_aio_query (Q);
  return 0;
}

conn_query_type_t default_tl_aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "default-aio-metafile-query",
  .wakeup = tl_aio_on_timeout_default,
  .close = tl_aio_on_fail_default,
  .complete = tl_aio_on_end_default
};

int tl_aio_start (struct aio_connection **aio_connections, int conn_num, double timeout, struct tl_saved_query *q) {
  assert (q->restart);
  q->attempt ++;

  assert (conn_num >= 0);
  if (!conn_num) {
    return 0;
  }

  int i;
  for (i = 0; i < conn_num; i++) {
    assert (aio_connections);
    tl_create_aio_query (aio_connections[i], &Connections[0], timeout, &default_tl_aio_metafile_query_type, q);
  }
  q->wait_num += conn_num;
  vkprintf (1, "Creating aio for rpc\n");
  return q->wait_num;
}

struct aio_connection *WaitAioArr[100];
int WaitAioArrPos;

void WaitAioArrClear (void) {
  WaitAioArrPos = 0;
}

int WaitAioArrAdd (struct aio_connection *conn) {
  if (WaitAioArrPos < 99) {
    WaitAioArr[WaitAioArrPos ++] = conn;
    return 1;
  } else {
    return 0;
  }
}
