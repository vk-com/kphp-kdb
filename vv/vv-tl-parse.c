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

#include "vv-tl-parse.h"

#include "vv-tl-aio.h"
#include "vv-tree.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "common-data.h"
#include "TL/constants.h"
#include "net-rpc-targets.h"
#include "net-tcp-connections.h"
#include "net-tcp-rpc-common.h"

int default_tl_udp_rpcs_execute (struct udp_msg *msg);
struct udp_functions tl_udp_server = {
  .magic = UDP_FUNC_MAGIC,
  .title = "rpc_proxy udp server",
  .process_msg = udp_target_process_msg
};

struct udp_target_methods tl_udp_server_methods = {
  .on_receive = default_tl_udp_rpcs_execute
};

extern struct in_addr settings_addr;
extern int backlog;
int usfd __attribute__ ((weak));
void add_udp_socket (int port, int mode) {
  if (!usfd) {
    usfd = server_socket (port, settings_addr, backlog, mode + 1);
    if (usfd < 0) {
      fprintf (stderr, "cannot open udp port: %m\n");
      exit (1);
    }
  }
  assert ((default_udp_socket = init_udp_port (usfd, port, &tl_udp_server, &tl_udp_server_methods, 1 + mode)));
}

double vv_tl_drop_probability;

//#define BINLOG_WAIT_QUERY_CMP(a,b) (if ((a)->wait_pos != (b)->wait_pos) 
struct binlog_wait_query {
  event_timer_t ev;
  void (*on_complete)(struct binlog_wait_query *);
  void (*on_alarm)(struct binlog_wait_query *);
  //void (*on_delete)(struct binlog_wait_query *);
  struct tl_saved_query *extra;
  long long wait_pos;
  double start_time;
  double timeout;
  int num;
};

long long binlog_wait_query_cmp (struct binlog_wait_query *a, struct binlog_wait_query *b) {
  if (a->wait_pos != b->wait_pos) {
    return a->wait_pos - b->wait_pos;
  }
  return a->num - b->num;
}

DEFINE_TREE_STD_ALLOC(binlog_wait,struct binlog_wait_query *,binlog_wait_query_cmp,int,std_int_compare);
tree_binlog_wait_t *binlog_wait_tree;
tree_binlog_wait_t *binlog_wait_time_tree;
long long next_binlog_wait_pos = 0x7fffffffffffffffll;
long long next_binlog_wait_time = 0x7fffffffffffffffll;
long long binlog_wait_queries;

struct tl_state tl;
struct tl_state *tlio = &tl;
int rpc_crc32_mode;

struct tl_act_extra *(*tl_parse_function)(long long actor_id);
void (*tl_stat_function)(void);

int tl_result_new_flags (int old_flags) {
  return old_flags & 0xffff;
}

int tl_result_get_header_len (struct tl_query_header *h) {
  if (!h->flags) { return 0; }
  int new_flags = tl_result_new_flags (h->flags);
  int s = 8;
  if (new_flags & TL_QUERY_HEADER_FLAG_RETURN_BINLOG_POS) {
    s += 8;
  }
  if (new_flags & TL_QUERY_HEADER_FLAG_RETURN_BINLOG_TIME) {
    s += 8;
  }
  return s;
}

long long log_last_pos (void) __attribute__ ((weak));
long long log_last_pos (void) {
  return 0;
}

int tl_result_make_header (int *ptr, struct tl_query_header *h) {
  int *p = ptr;
  if (!h->flags) { return 0; }
  int new_flags = tl_result_new_flags (h->flags);
  *p = RPC_REQ_RESULT_FLAGS; 
  p++;
  *p = new_flags;
  p ++;
  if (new_flags & TL_QUERY_HEADER_FLAG_RETURN_BINLOG_POS) {
    *(long long *)p = log_last_pos ();
    p += 2;
  }
  if (new_flags & TL_QUERY_HEADER_FLAG_RETURN_BINLOG_TIME) {
    *(long long *)p = get_precise_time (1000000);
    p += 2;
  }
  return (p - ptr) * 4;
}

int tl_query_act_restart (struct tl_saved_query *q) {
  vkprintf (1, "%s: q = %p\n", __func__, q);
//  return extra->act (extra);
  struct tl_act_extra *extra = q->extra;
  int z = tl_result_get_header_len (extra->header);
  int *hptr = tl_store_get_ptr (z);
  int v = ((struct tl_act_extra *)(q->extra))->act (q->extra);
  if (v >= 0) {
    assert (z == tl_result_make_header (hptr, extra->header));
  }
  return v;
}

void tl_query_act_free_extra (struct tl_saved_query *q) {
  ((struct tl_act_extra *)(q->extra))->free (q->extra);
}

void tl_default_act_free (struct tl_act_extra *extra) {
  tl_query_header_delete (extra->header);
  if (!(extra->flags & 1)) {
    return;
  }
  if (extra->flags & 2) {
    free (extra);
  } else {
    zfree (extra, extra->size);
  }
}

struct tl_act_extra *tl_default_act_dup (struct tl_act_extra *extra) {
  struct tl_act_extra *new = malloc (extra->size);
  memcpy (new, extra, extra->size);
  new->flags = (new->flags & ~1) | 2;
  return new;
}

int need_dup (struct tl_act_extra *extra) {
  return !(extra->flags & 1);
}

void tl_binlog_wait_pos_add (struct tl_saved_query *q, long long wait_pos, double timeout);
void tl_binlog_wait_time_add (struct tl_saved_query *q, long long wait_time, double timeout);

int tl_act_nop (struct tl_act_extra *extra) {
  tl_store_int (TL_TRUE);
  return 0;
}

void default_stat_function (void) {
  static char buf[(1 << 12)];
  prepare_stats (0, buf, (1 << 12) - 2);
  tl_store_stats (buf, 0);
}

int tl_act_stat (struct tl_act_extra *extra) {
  if (tl_stat_function) {
    tl_stat_function ();
  } else {
    default_stat_function ();
  }
  return 0;
}

struct tl_act_extra *tl_default_parse_function (long long actor_id) {
  if (actor_id) { 
    return 0; 
  }
  int f = tl_fetch_lookup_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  if (f == TL_ENGINE_NOP) {
    tl_fetch_int ();
    tl_fetch_end ();
    if (tl_fetch_error ()) {
      return 0;
    }
    struct tl_act_extra *extra = malloc (sizeof (*extra));
    memset (extra, 0, sizeof (*extra));
    extra->flags = 3;
    extra->size = sizeof (*extra);
    extra->act = tl_act_nop;
    return extra;
  }
  if (f == TL_ENGINE_STAT) {
    tl_fetch_int ();
    tl_fetch_end ();
    if (tl_fetch_error ()) {
      return 0;
    }
    struct tl_act_extra *extra = malloc (sizeof (*extra));
    memset (extra, 0, sizeof (*extra));
    extra->flags = 3;
    extra->size = sizeof (*extra);
    extra->act = tl_act_stat;
    return extra;
  }
  return 0;
}

int __tl_query_act (struct tl_query_header *h) {

//  int f = tl_fetch_int ();
  if (tl_fetch_error ()) {
    tl_fetch_set_error ("Unknown error occured", TL_ERROR_UNKNOWN);
    tl_store_end ();
    tl_query_header_delete (h);
    return 0;
  }
  struct tl_act_extra *extra = tl_default_parse_function (h->actor_id);
  if (!extra && tl_fetch_error ()) {
    tl_store_end ();
    tl_query_header_delete (h);
    return 0;
  }
  if (!extra) {
    extra = tl_parse_function (h->actor_id);
  }
  if (!extra) {
    tl_fetch_set_error ("Unknown error occured", TL_ERROR_UNKNOWN);
    tl_store_end ();
    tl_query_header_delete (h);
    return 0;
  }
  if (!extra->free) {
    extra->free = tl_default_act_free;
  }
  if (!extra->dup) {
    extra->dup = tl_default_act_dup;
  }
  assert (extra->act);
  assert (extra->free);
  assert (extra->dup);
  extra->header = h;

  int z = tl_result_get_header_len (extra->header);
  int *hptr = 0;
  if (!(TL_OUT_FLAGS & TLF_ALLOW_PREPEND)) {
    assert (TL_OUT_FLAGS & TLF_PERMANENT);
    hptr = tl_store_get_ptr (z);
  }

  if (h->wait_binlog_time && binlog_disabled) {
    long long pos = lookup_binlog_time (h->wait_binlog_time);
    vkprintf (2, "Wait binlog: time = %lld, pos = %lld, cur_pos = %lld\n", h->wait_binlog_time, pos, log_last_pos ());
    if (pos > 0) {
      if (pos > h->wait_binlog_pos) {
        h->wait_binlog_pos = pos;
      }
    } else {
      struct tl_saved_query *q = tl_saved_query_init ();
      q->restart = tl_query_act_restart;
      q->free_extra = tl_query_act_free_extra;
      q->fail = tl_default_aio_fail;
      q->extra = need_dup (extra) ? extra->dup (extra) : extra;
      //tl_create_aio_query (WaitAio, &Connections[0], 0.5, &tl_aio_metafile_query_type, q);
      //extra->header = malloc (sizeof (h));
      //memcpy (extra->header, &h, sizeof (h));
      tl_store_clear ();
      tl_binlog_wait_time_add (q, h->wait_binlog_time, h->custom_timeout ? 0.001 * h->custom_timeout : tl_aio_timeout);
      if (h->wait_binlog_pos) {
        tl_binlog_wait_pos_add (q, h->wait_binlog_pos, h->custom_timeout ? 0.001 * h->custom_timeout : tl_aio_timeout);
      }
      return 0;
    }
  }

  if (h->wait_binlog_pos && h->wait_binlog_pos > log_last_pos () && log_last_pos ()) {
    struct tl_saved_query *q = tl_saved_query_init ();
    q->restart = tl_query_act_restart;
    q->free_extra = tl_query_act_free_extra;
    q->fail = tl_default_aio_fail;
    q->extra = need_dup (extra) ? extra->dup (extra) : extra;
    //tl_create_aio_query (WaitAio, &Connections[0], 0.5, &tl_aio_metafile_query_type, q);
    //extra->header = malloc (sizeof (h));
    //memcpy (extra->header, &h, sizeof (h));
    tl_store_clear ();
    tl_binlog_wait_pos_add (q, h->wait_binlog_pos, h->custom_timeout ? 0.001 * h->custom_timeout : tl_aio_timeout);
    return 0;
  }

  int res = extra->act (extra);
  if (res < 0) {
    if (res != -2) {
      tl_fetch_set_error ("Unknown error occured", TL_ERROR_UNKNOWN);
      tl_store_end ();
//      tl_query_header_delete (h);
      extra->free (extra);
    } else {
      struct tl_saved_query *q = tl_saved_query_init ();
      q->restart = tl_query_act_restart;
      q->free_extra = tl_query_act_free_extra;
      q->fail = tl_default_aio_fail;
      q->extra = need_dup (extra) ? extra->dup (extra) : extra;
      //tl_create_aio_query (WaitAio, &Connections[0], 0.5, &tl_aio_metafile_query_type, q);
      //extra->header = malloc (sizeof (h));
      //memcpy (extra->header, &h, sizeof (h));
      tl_store_clear ();
      tl_aio_start (WaitAioArr, WaitAioArrPos, h->custom_timeout ? 0.001 * h->custom_timeout : tl_aio_timeout, q);
    }
  } else {
    if (TL_OUT_FLAGS & TLF_ALLOW_PREPEND) {
      hptr = tl_store_get_prepend_ptr (z);
    }
    assert (z == tl_result_make_header (hptr, extra->header));
    tl_store_end ();
//    tl_query_header_delete (h);
    extra->free (extra);
  }
  return 0;
}

int tl_query_act (struct connection *c, int op, int len) {
  if (op != RPC_INVOKE_REQ) {
    return SKIP_ALL_BYTES;
  }
  //WaitAio = WaitAio2 = WaitAio3 = 0;
  //WaitAioArrClear ();
  
  WaitAioArrClear ();
  tl_fetch_init (c, len - 4);
  struct tl_query_header *h = zmalloc (sizeof (*h));
  tl_fetch_query_header (h);
  tl_store_init_keep_error (c, h->qid);
  assert (h->op == op || tl_fetch_error ());

  __tl_query_act (h);
  assert (advance_skip_read_ptr (&c->In, 4 + tl_fetch_unread ()) == 4 + tl_fetch_unread ());
  return 0;
}

int tl_query_act_udp (struct udp_msg *msg) {
  //WaitAio = WaitAio2 = WaitAio3 = 0;
  //WaitAioArrClear ();
  
  WaitAioArrClear ();
  struct raw_message r;
  rwm_clone (&r, &msg->raw);
  tl_fetch_init_raw_message (&r, msg->raw.total_bytes);
  rwm_free (&r);
  struct tl_query_header *h = zmalloc (sizeof (*h));
  tl_fetch_query_header (h);
  assert (msg->S);
  tl_store_init_raw_msg_keep_error (msg->S, h->qid);

  if (h->op != RPC_INVOKE_REQ) {
    return 0;
  }

  __tl_query_act (h);
  return 0;
}

void tl_update_next_binlog_wait_pos (void) {
  tree_binlog_wait_t *T = tree_get_min_binlog_wait (binlog_wait_tree);
  next_binlog_wait_pos = T ? T->x->wait_pos : 0x7fffffffffffffffll;
}

void tl_binlog_wait_restart_all_finished (void) {
  long long cur_pos = log_last_pos ();
//  vkprintf (2, "cur_pos = %lld, next_pos = %lld\n", cur_pos, next_binlog_wait_pos);
  while (cur_pos >= next_binlog_wait_pos) {
    struct binlog_wait_query *Q = tree_get_min_binlog_wait (binlog_wait_tree)->x;
    assert (Q);
    Q->on_complete (Q);
    binlog_wait_tree = tree_delete_binlog_wait (binlog_wait_tree, Q);
    tl_update_next_binlog_wait_pos ();
    zfree (Q, sizeof (*Q));
  }
}

void tl_update_next_binlog_wait_time (void) {
  tree_binlog_wait_t *T = tree_get_min_binlog_wait (binlog_wait_time_tree);
  next_binlog_wait_time = T ? T->x->wait_pos : 0x7fffffffffffffffll;
}

void tl_binlog_wait_time_restart_all_finished (void) {
  while (lookup_binlog_time (next_binlog_wait_time) > 0) {
    assert (tree_get_min_binlog_wait (binlog_wait_time_tree));
    struct binlog_wait_query *Q = tree_get_min_binlog_wait (binlog_wait_time_tree)->x;
    assert (Q);
    Q->on_complete (Q);
    binlog_wait_time_tree = tree_delete_binlog_wait (binlog_wait_time_tree, Q);
    tl_update_next_binlog_wait_time ();
    zfree (Q, sizeof (*Q));
  }
}

void tl_binlog_wait_on_alarm (struct binlog_wait_query *Q) {
  struct tl_saved_query *q = Q->extra;
  vkprintf (1, "%s: wait_num = %d, failed = %d\n", __func__, q->wait_num, q->failed);
  binlog_wait_tree = tree_delete_binlog_wait (binlog_wait_tree, Q);
  tl_update_next_binlog_wait_pos ();
 
  q->wait_num --;
  expired_aio_queries ++;
  if (!q->failed) {
    q->error_code = TL_ERROR_BINLOG_WAIT_TIMEOUT;
    tl_aio_fail_start (q);
    //q->fail (q);
  }
  q->failed = 1;
  if (!q->wait_num) {
    q->free_extra (q);
    zfree (q, sizeof (*q));
  }
}

void tl_binlog_wait_on_complete (struct binlog_wait_query *Q) {
  struct tl_saved_query *q = Q->extra;
  vkprintf (1, "%s: wait_num = %d, failed = %d\n", __func__, q->wait_num, q->failed);
  remove_event_timer (&Q->ev);
  q->wait_num --;
  if (!q->wait_num && !q->failed) {
    add_finished_query (q);
  }
}

void tl_binlog_wait_time_on_alarm (struct binlog_wait_query *Q) {
  struct tl_saved_query *q = Q->extra;
  binlog_wait_time_tree = tree_delete_binlog_wait (binlog_wait_time_tree, Q);
  tl_update_next_binlog_wait_time ();
 
  q->wait_num --;
  expired_aio_queries ++;
  if (!q->failed) {
    q->error_code = TL_ERROR_BINLOG_WAIT_TIMEOUT;
    tl_aio_fail_start (q);
    //q->fail (q);
  }
  q->failed = 1;
  if (!q->wait_num) {
    q->free_extra (q);
    zfree (q, sizeof (*q));
  }
}

void tl_binlog_wait_time_on_complete (struct binlog_wait_query *Q) {
  struct tl_saved_query *q = Q->extra;
  remove_event_timer (&Q->ev);
  q->wait_num --;
  long long pos = lookup_binlog_time (Q->wait_pos);
  assert (pos > 0);
  if (log_last_pos () < pos && !q->failed) {
    tl_binlog_wait_pos_add (q, pos, Q->start_time + Q->timeout - precise_now);
  }

  if (!q->wait_num && !q->failed) {
    add_finished_query (q);
  }
}

int tl_binlog_wait_pos_alarm (struct event_timer *ev) {
 
  struct binlog_wait_query *Q = (void *)ev;
  //fprintf (stderr, "delete: Q = %p\n", Q);
  //binlog_wait_tree = tree_delete_binlog_wait (binlog_wait_tree, Q);
  Q->on_alarm (Q);
  zfree (Q, sizeof (*Q));
  return 0;
}

void tl_binlog_wait_pos_add (struct tl_saved_query *q, long long wait_pos, double timeout) {
  vkprintf (1, "Wait binlog pos: cur_pos = %lld, wait_pos = %lld, timeout = %lf\n", log_last_pos (), wait_pos, timeout);  
  struct binlog_wait_query *Q = zmalloc (sizeof (*Q));

  Q->ev.h_idx = 0;
  Q->ev.wakeup = tl_binlog_wait_pos_alarm;
  Q->ev.wakeup_time = precise_now + timeout;
  insert_event_timer (&Q->ev);
  Q->wait_pos = wait_pos;
  Q->start_time = precise_now;
  Q->extra = q;
  q->wait_num ++;
  Q->on_alarm = tl_binlog_wait_on_alarm;
  Q->on_complete = tl_binlog_wait_on_complete;
  Q->num = binlog_wait_queries ++;
  Q->timeout = timeout;

  //fprintf (stderr, "insert: Q = %p\n", Q);
  binlog_wait_tree = tree_insert_binlog_wait (binlog_wait_tree, Q, lrand48 ());
  tl_update_next_binlog_wait_pos ();
}


void tl_binlog_wait_time_add (struct tl_saved_query *q, long long wait_time, double timeout) {
  vkprintf (1, "Wait binlog time: cur_pos = %lld, wait_pos = %lld, timeout = %lf\n", log_last_pos (), wait_time, timeout);  
  struct binlog_wait_query *Q = zmalloc (sizeof (*Q));

  Q->ev.h_idx = 0;
  Q->ev.wakeup = tl_binlog_wait_pos_alarm;
  Q->ev.wakeup_time = precise_now + timeout;
  insert_event_timer (&Q->ev);
  Q->wait_pos = wait_time;
  Q->start_time = precise_now;
  Q->extra = q;
  q->wait_num ++;
  Q->on_alarm = tl_binlog_wait_time_on_alarm;
  Q->on_complete = tl_binlog_wait_time_on_complete;
  Q->num = binlog_wait_queries ++;
  Q->timeout = timeout;

  //fprintf (stderr, "insert: Q = %p\n", Q);
  binlog_wait_time_tree = tree_insert_binlog_wait (binlog_wait_time_tree, Q, lrand48 ());
  tl_update_next_binlog_wait_time ();
  vkprintf (1, "added successfully\n");
}

void tl_restart_all_ready (void) {
  tl_binlog_wait_time_restart_all_finished ();
  tl_binlog_wait_restart_all_finished ();
  tl_aio_query_restart_all_finished ();
}

void tl_query_header_delete (struct tl_query_header *h) {
  h->ref_cnt --;
  assert (h->ref_cnt >= 0);
  if (h->ref_cnt) { return; }
  if (h->string_forward) {
    free (h->string_forward);
  }
  int i;
  for (i = 0; i < h->string_forward_keys_num; i++) if (h->string_forward_keys[i]) {
    free (h->string_forward_keys[i]);
  }
  zfree (h, sizeof (*h));
}
  
struct tl_query_header *tl_query_header_dup (struct tl_query_header *h) {
  h->ref_cnt ++;
  return h;
}

int tl_fetch_set_error_format (int errnum, const char *format, ...) {
  if (tl.error) {
    return 0;
//    free (tl.error);
  }
  assert (format);
  static char s[10000];
  va_list l;
  va_start (l, format);
  vsnprintf (s, 9999, format, l);
  vkprintf (2, "Error %s\n", s);
  tl.errnum = errnum;
  tl.error = strdup (s);
  return 0;
}

int default_tl_close_conn (struct connection *c, int who) {
  rpc_target_delete_conn (c);
  return 0;
}

int default_tl_rpcs_execute (struct connection *c, int op, int len) {
  rpc_target_insert_conn (c);
  if (op != RPC_INVOKE_REQ) {
    return SKIP_ALL_BYTES;
  }
  TL_IN_PID = &RPCS_DATA(c)->remote_pid;
  WaitAioArrClear ();
  return tl_query_act (c, op, len);
}

int default_tl_udp_rpcs_execute (struct udp_msg *msg) {  
  WaitAioArrClear ();
  TL_IN_PID = &msg->S->PID;
  tl_query_act_udp (msg);
  return 0;
}

int tl_store_stats (const char *s, int raw) {
  int i, n = 0, key_start = 0, value_start = -1;
  for (i = 0; s[i]; i++) {
    if (s[i] == '\n') {
      if (value_start - key_start > 1 && value_start < i) {
        n++;
      }
      key_start = i + 1;
      value_start = -1;
    } else if (s[i] == '\t') {
      value_start = value_start == -1 ? i + 1 : -2;
    }
  }
  if (!raw) {
    tl_store_int (TL_STAT);
  }
  tl_store_int (n);
  key_start = 0;
  value_start = -1;
  int m = 0;
  for (i = 0; s[i]; i++) {
    if (s[i] == '\n') {
      if (value_start - key_start > 1 && value_start < i) {
        tl_store_string (s + key_start, value_start - key_start - 1); /* - 1 (trim tabular) */
        tl_store_string (s + value_start, i - value_start);
        m++;
      }
      key_start = i + 1;
      value_start = -1;
    } else if (s[i] == '\t') {
      value_start = value_start == -1 ? i + 1 : -2;
    }
  }
  assert (m == n);
  return n;
}

/* {{{ Conn methods */
static inline void __tl_conn_fetch_raw_data (void *buf, int len) {
  assert (read_in (&TL_IN_CONN->In, buf, len) == len);  
}

static inline void __tl_conn_fetch_move (int len) {
  if (len >= 0) {
    assert (advance_skip_read_ptr (&TL_IN_CONN->In, len) == len);
  } else {
    assert (0);
  }
}

static inline void __tl_conn_fetch_lookup (void *buf, int len) {
  nb_iterator_t R;
  nbit_set (&R, &TL_IN_CONN->In);
  assert (nbit_read_in (&R, buf, len) == len);
}

static inline void *__tl_conn_store_get_ptr (int len) {
  void *r = get_write_ptr (&TL_OUT_CONN->Out, len);
  advance_write_ptr (&TL_OUT_CONN->Out, len);
  return r;
}

static inline void __tl_conn_store_raw_data (const void *buf, int len) {
  assert (write_out (&TL_OUT_CONN->Out, buf, len) == len);
}

static inline void __tl_conn_store_read_back (int len) {
  assert (read_back (&TL_OUT_CONN->Out, 0, len) == len);
}

static inline void __tl_conn_store_read_back_nondestruct (void *buf, int len) {
  assert (read_back_nondestruct (&TL_OUT_CONN->Out, buf, len) == len);
}

static inline unsigned __tl_conn_store_crc32_partial (int len, unsigned start) {
  return ~count_crc32_back_partial (&TL_OUT_CONN->Out, len, ~start);
}

static inline void __tl_conn_conn_copy_through (int len, int advance) {
  if (advance) {
    assert (copy_through (&TL_OUT_CONN->Out, &TL_IN_CONN->In, len) == len);
  } else {
    assert (copy_through_nondestruct (&TL_OUT_CONN->Out, &TL_IN_CONN->In, len) == len);
  }
}

static inline void __tl_conn_raw_msg_copy_through (int len, int advance) {
  if (advance) {
    while (len) {
      int x = len >= MSG_STD_BUFFER ? MSG_STD_BUFFER : len; 
      void *buf = rwm_postpone_alloc (TL_OUT_RAW_MSG, x);
      assert (buf);
      assert (read_in (&TL_IN_CONN->In, buf, x) == x);
      len -= x;
    }
  } else {
    nb_iterator_t R;
    nbit_set (&R, &TL_IN_CONN->In);
    while (len) {
      int x = len >= MSG_STD_BUFFER ? MSG_STD_BUFFER : len; 
      void *buf = rwm_postpone_alloc (TL_OUT_RAW_MSG, x);
      assert (buf);
      assert (nbit_read_in (&R, buf, x) == x);
      len -= x;
    }
    nbit_clear (&R);
  }
}

static inline void __tl_conn_store_flush (void) {
  RPCS_FUNC(TL_OUT_CONN)->flush_packet(TL_OUT_CONN);
}

static inline void __tl_conn_store_clear (void) {
  if (TL_OUT) {
    __tl_conn_store_read_back (TL_OUT_POS + 20);
  }
}

static inline void __tl_conn_store_prefix (void) {
  int *p = TL_OUT_SIZE;
  p[0] = TL_OUT_POS + 24;
  p[1] = RPCS_DATA(TL_OUT_CONN)->out_packet_num ++;
}

/* }}} */

/* {{{ Nbit methods */
static inline void __tl_nbit_fetch_raw_data (void *buf, int len) {
  assert (nbit_read_in (TL_IN_NBIT, buf, len) == len);
}

static inline void __tl_nbit_fetch_move (int len) {
  assert (nbit_advance (TL_IN_NBIT, len) == len);
}

static inline void __tl_nbit_fetch_lookup (void *buf, int len) {
  assert (nbit_read_in (TL_IN_NBIT, buf, len) == len);
  assert (nbit_advance (TL_IN_NBIT, -len) == -len);
}

static inline void __tl_nbit_conn_copy_through (int len, int advance) {
  if (advance) {
    assert (nbit_copy_through (&TL_OUT_CONN->Out, TL_IN_NBIT, len) == len);
  } else {
    assert (nbit_copy_through_nondestruct (&TL_OUT_CONN->Out, TL_IN_NBIT, len) == len);
  }
}

static inline void __tl_nbit_raw_msg_copy_through (int len, int advance) {
  if (advance) {
    while (len) {
      int x = len >= MSG_STD_BUFFER ? MSG_STD_BUFFER : len; 
      void *buf = rwm_postpone_alloc (TL_OUT_RAW_MSG, x);
      assert (buf);
      assert (nbit_read_in (TL_IN_NBIT, buf, x) == x);
      len -= x;
    }
  } else {
    nb_iterator_t R = *TL_IN_NBIT;
    while (len) {
      int x = len >= MSG_STD_BUFFER ? MSG_STD_BUFFER : len; 
      void *buf = rwm_postpone_alloc (TL_OUT_RAW_MSG, x);
      assert (buf);
      assert (nbit_read_in (&R, buf, x) == x);
      len -= x;
    }
  }
}
  
static inline void __tl_nbit_fetch_mark (void) {
  assert (!TL_IN_MARK);
  nb_iterator_t *T = zmalloc (sizeof (*T));
  *T = *TL_IN_NBIT;
  TL_IN_MARK = T;
  TL_IN_MARK_POS = TL_IN_POS;
}

static inline void __tl_nbit_fetch_mark_restore (void) {
  assert (TL_IN_MARK);
  *TL_IN_NBIT = *(nb_iterator_t *)TL_IN_MARK;
  zfree (TL_IN_MARK, sizeof (nb_iterator_t));
  TL_IN_MARK = 0;
  int x = TL_IN_POS - TL_IN_MARK_POS;
  TL_IN_POS -= x;
  TL_IN_REMAINING += x;
}

static inline void __tl_nbit_fetch_mark_delete (void) {
  assert (TL_IN_MARK);
  zfree (TL_IN_MARK, sizeof (nb_iterator_t));
  TL_IN_MARK = 0;
}
/* }}} */

/* {{{ Raw msg methods */
static inline void __tl_raw_msg_fetch_raw_data (void *buf, int len) {
#ifdef ENABLE_UDP
  assert (rwm_fetch_data (TL_IN_RAW_MSG, buf, len) == len);
#endif
}

static inline void __tl_raw_msg_fetch_move (int len) {
#ifdef ENABLE_UDP
  assert (len >= 0);
  assert (rwm_fetch_data (TL_IN_RAW_MSG, 0, len) == len);
#endif
}

static inline void __tl_raw_msg_fetch_lookup (void *buf, int len) {
#ifdef ENABLE_UDP
  assert (rwm_fetch_lookup (TL_IN_RAW_MSG, buf, len) == len);
#endif
}
  
static inline void __tl_raw_msg_fetch_mark (void) {
  assert (!TL_IN_MARK);
  struct raw_message *T = zmalloc (sizeof (*T));
  rwm_clone (T, TL_IN_RAW_MSG);
  TL_IN_MARK = T;
  TL_IN_MARK_POS = TL_IN_POS;
}

static inline void __tl_raw_msg_fetch_mark_restore (void) {
  assert (TL_IN_MARK);
  rwm_free (TL_IN_RAW_MSG);
  *TL_IN_RAW_MSG = *(struct raw_message *)TL_IN_MARK;
  zfree (TL_IN_MARK, sizeof (struct raw_message));
  TL_IN_MARK = 0;
  int x = TL_IN_POS - TL_IN_MARK_POS;
  TL_IN_POS -= x;
  TL_IN_REMAINING += x;
}

static inline void __tl_raw_msg_fetch_mark_delete (void) {
  assert (TL_IN_MARK);
  rwm_free (TL_IN_MARK);
  zfree (TL_IN_MARK, sizeof (struct raw_message));
  TL_IN_MARK = 0;
}

static inline void *__tl_raw_msg_store_get_ptr (int len) {
  return rwm_postpone_alloc (TL_OUT_RAW_MSG, len);
}

static inline void *__tl_raw_msg_store_get_prepend_ptr (int len) {
  return rwm_prepend_alloc (TL_OUT_RAW_MSG, len);
}

static inline void __tl_raw_msg_store_raw_data (const void *buf, int len) {
  assert (rwm_push_data (TL_OUT_RAW_MSG, buf, len) == len);
}

static inline void __tl_raw_msg_store_read_back (int len) {
  assert (rwm_fetch_data_back (TL_OUT_RAW_MSG, 0, len) == len);
}

static inline void __tl_raw_msg_store_read_back_nondestruct (void *buf, int len) {
  struct raw_message r;
  rwm_clone (&r, TL_OUT_RAW_MSG);
  assert (rwm_fetch_data_back (&r, buf, len) == len);
  rwm_free (&r);
}

static void __m_to_conn (void *_c, const void *data, int len) {
  struct connection *c = (struct connection *)_c;
  assert (write_out (&c->Out, data, len) == len);
}

static inline void __tl_raw_msg_conn_copy_through (int len, int advance) {
  if (advance) {
    assert (rwm_process_and_advance (TL_IN_RAW_MSG, len, __m_to_conn, TL_OUT_CONN) == len);
  } else {
    assert (rwm_process (TL_IN_RAW_MSG, len, __m_to_conn, TL_OUT_CONN) == len);
  }
}

static inline void __tl_raw_msg_raw_msg_copy_through (int len, int advance) {
  if (!advance) {
    struct raw_message r;
    rwm_clone (&r, TL_IN_RAW_MSG);
    rwm_trunc (&r, len);
    rwm_union (TL_OUT_RAW_MSG, &r);
  } else {
    struct raw_message r;
    rwm_split_head (&r, TL_IN_RAW_MSG, len);
    rwm_union (TL_OUT_RAW_MSG, &r);
  }
}

static inline void __tl_raw_msg_fetch_clear (void) {
  if (TL_IN_RAW_MSG) {
    rwm_free (TL_IN_RAW_MSG);
    zfree (TL_IN_RAW_MSG, sizeof (struct raw_message));
    TL_IN = 0;
  }
}

static inline void __tl_raw_msg_store_clear (void) {
  if (TL_OUT_RAW_MSG) {
    rwm_free (TL_OUT_RAW_MSG);
    zfree (TL_OUT_RAW_MSG, sizeof (struct raw_message));
    TL_OUT = 0;
  }
}

static inline void __tl_raw_msg_store_flush (void) {
//  struct udp_target *S = (struct udp_target *)TL_OUT_EXTRA;
  assert (TL_OUT_RAW_MSG);
  assert (TL_OUT_EXTRA);
  udp_target_send ((struct udp_target *)TL_OUT_EXTRA, TL_OUT_RAW_MSG, 0);
  zfree (TL_OUT_RAW_MSG, sizeof (struct raw_message));
  TL_OUT = 0;
  //udp_target_flush ((struct udp_target *)TL_OUT_EXTRA);
}

/* }}} */

/* {{{ Tcp raw msg methods */

static inline void __tl_tcp_raw_msg_store_flush (void) {
//  struct udp_target *S = (struct udp_target *)TL_OUT_EXTRA;
  assert (TL_OUT_RAW_MSG);
  assert (TL_OUT_EXTRA);
  //udp_target_send ((struct udp_target *)TL_OUT_EXTRA, TL_OUT_RAW_MSG, 0);
  tcp_rpc_conn_send (TL_OUT_EXTRA, TL_OUT_RAW_MSG, 0);
  zfree (TL_OUT_RAW_MSG, sizeof (struct raw_message));
  //RPCS_FUNC((struct connection *)TL_OUT_EXTRA)->flush_packet(TL_OUT_EXTRA);
  flush_later (TL_OUT_EXTRA);
  TL_OUT = 0;
  //udp_target_flush ((struct udp_target *)TL_OUT_EXTRA);
}
/* }}} */

struct tl_in_methods tl_in_conn_methods = {
  .fetch_raw_data = __tl_conn_fetch_raw_data,
  .fetch_move = __tl_conn_fetch_move,
  .fetch_lookup = __tl_conn_fetch_lookup,
  .prepend_bytes = 8,
};

struct tl_in_methods tl_in_nbit_methods = {
  .fetch_raw_data = __tl_nbit_fetch_raw_data,
  .fetch_move = __tl_nbit_fetch_move,
  .fetch_lookup = __tl_nbit_fetch_lookup,
  .fetch_mark = __tl_nbit_fetch_mark,
  .fetch_mark_restore = __tl_nbit_fetch_mark_restore,
  .fetch_mark_delete = __tl_nbit_fetch_mark_delete,
  .prepend_bytes = 8,
};

struct tl_in_methods tl_in_raw_msg_methods = {
  .fetch_raw_data = __tl_raw_msg_fetch_raw_data,
  .fetch_move = __tl_raw_msg_fetch_move,
  .fetch_lookup = __tl_raw_msg_fetch_lookup,
  .fetch_clear = __tl_raw_msg_fetch_clear,
  .fetch_mark = __tl_raw_msg_fetch_mark,
  .fetch_mark_restore = __tl_raw_msg_fetch_mark_restore,
  .fetch_mark_delete = __tl_raw_msg_fetch_mark_delete,
  .flags = 0,
};

struct tl_out_methods tl_out_conn_methods = {
  .store_get_ptr = __tl_conn_store_get_ptr,
  .store_raw_data = __tl_conn_store_raw_data,
  .store_read_back = __tl_conn_store_read_back,
  .store_read_back_nondestruct = __tl_conn_store_read_back_nondestruct,
  .store_crc32_partial = __tl_conn_store_crc32_partial,
  .store_flush = __tl_conn_store_flush,
  .store_clear = __tl_conn_store_clear,
  .store_prefix = __tl_conn_store_prefix,
  .copy_through = 
    {
      0, // none
      0, // str
      __tl_conn_conn_copy_through, // conn
      __tl_nbit_conn_copy_through, // nbit
      __tl_raw_msg_conn_copy_through, // raw_msg
      __tl_raw_msg_conn_copy_through
    },
  .flags = TLF_PERMANENT | TLF_CRC32,
  .prepend_bytes = 8

};

struct tl_out_methods tl_out_raw_msg_methods = {
  .store_get_ptr = __tl_raw_msg_store_get_ptr,
  .store_get_prepend_ptr = __tl_raw_msg_store_get_prepend_ptr,
  .store_raw_data = __tl_raw_msg_store_raw_data,
  .store_read_back = __tl_raw_msg_store_read_back,
  .store_read_back_nondestruct = __tl_raw_msg_store_read_back_nondestruct,
  .store_clear = __tl_raw_msg_store_clear,
  .store_flush = __tl_raw_msg_store_flush,
  .copy_through = 
    {
      0, // none
      0, // str
      __tl_conn_raw_msg_copy_through, // conn
      __tl_nbit_raw_msg_copy_through, // nbit
      __tl_raw_msg_raw_msg_copy_through, // raw_msg
      __tl_raw_msg_raw_msg_copy_through
    },
  .flags = TLF_ALLOW_PREPEND
};

struct tl_out_methods tl_out_tcp_raw_msg_methods = {
  .store_get_ptr = __tl_raw_msg_store_get_ptr,
  .store_get_prepend_ptr = __tl_raw_msg_store_get_prepend_ptr,
  .store_raw_data = __tl_raw_msg_store_raw_data,
  .store_read_back = __tl_raw_msg_store_read_back,
  .store_read_back_nondestruct = __tl_raw_msg_store_read_back_nondestruct,
  .store_clear = __tl_raw_msg_store_clear,
  .store_flush = __tl_tcp_raw_msg_store_flush,
  .copy_through = 
    {
      0, // none
      0, // str
      __tl_conn_raw_msg_copy_through, // conn
      __tl_nbit_raw_msg_copy_through, // nbit
      __tl_raw_msg_raw_msg_copy_through, // raw_msg
      __tl_raw_msg_raw_msg_copy_through
    },
  .flags = TLF_ALLOW_PREPEND
};

int tl_fetch_set_error (const char *s, int errnum) {
  assert (s);
  if (TL_ERROR) {
    return 0;
  }
  vkprintf (2, "Error %s\n", s);
  TL_ERROR = strdup (s);
  TL_ERRNUM = errnum;
  return 0;
}

int __tl_fetch_init (void *in, void *in_extra, enum tl_type type, struct tl_in_methods *methods, int size) {
  if (TL_IN_METHODS && TL_IN_METHODS->fetch_clear) {
    TL_IN_METHODS->fetch_clear ();
  }
  assert (in);
  TL_IN_TYPE = type;
  TL_IN = in;
  TL_IN_REMAINING = size;
  TL_IN_POS = 0;

  TL_IN_METHODS = methods;
  TL_ATTEMPT_NUM = 0;
  if (TL_ERROR) {
    free (TL_ERROR);
    TL_ERROR = 0;
  }
  TL_ERRNUM = 0;
  TL_COPY_THROUGH = TL_OUT_METHODS ? TL_OUT_METHODS->copy_through[TL_IN_TYPE] : 0;
  return 0;
}

int tl_fetch_init (struct connection *c, int size) {
  return __tl_fetch_init (c, 0, tl_type_conn, &tl_in_conn_methods, size);
}

int tl_fetch_init_iterator (nb_iterator_t *it, int size) {
  return __tl_fetch_init (it, 0, tl_type_nbit, &tl_in_nbit_methods, size);
}

int tl_fetch_init_raw_message (struct raw_message *msg, int size) {
  struct raw_message *r = (struct raw_message *)zmalloc (sizeof (*r));
  *r = *msg;
  rwm_total_msgs ++;
  rwm_clean (msg);
  return __tl_fetch_init (r, 0, tl_type_raw_msg, &tl_in_raw_msg_methods, size);
}

int tl_fetch_init_tcp_raw_message (struct raw_message *msg, int size) {
  struct raw_message *r = (struct raw_message *)zmalloc (sizeof (*r));
  *r = *msg;
  rwm_total_msgs ++;
  rwm_clean (msg);
  return __tl_fetch_init (r, 0, tl_type_tcp_raw_msg, &tl_in_raw_msg_methods, size);
}

int tl_fetch_query_flags (struct tl_query_header *header) {
  int flags = tl_fetch_int ();
  int i;
  if (tl_fetch_error ()) {
    return -1;
  }
  if (header->flags & flags) {
    tl_fetch_set_error_format (TL_ERROR_HEADER, "Duplicate flags in header 0x%08x", header->flags & flags);
    return -1;
  }
  if (flags & ~TL_QUERY_HEADER_FLAG_MASK) {
    tl_fetch_set_error_format (TL_ERROR_HEADER, "Unsupported flags in header 0x%08x", (~TL_QUERY_HEADER_FLAG_MASK) & flags);
    return -1;
  }
  header->flags |= flags;
  if (flags & TL_QUERY_HEADER_FLAG_WAIT_BINLOG) {
    header->wait_binlog_pos = tl_fetch_long ();
    if (tl_fetch_error ()) {
      return -1;
    }
  }
  if (flags & TL_QUERY_HEADER_FLAG_KPHP_DELAY) {
    header->kitten_php_delay = tl_fetch_int ();
    if (tl_fetch_error ()) {
      return -1;
    }
    if (header->kitten_php_delay < 0) {
      header->kitten_php_delay = 0;
    }
    if (header->kitten_php_delay > 120000) {
      header->kitten_php_delay = 120000;
    }
  }
  if (flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS) {
    header->string_forward_keys_num = tl_fetch_int ();
    if (header->string_forward_keys_num < 0 || header->string_forward_keys_num > 10) {
      tl_fetch_set_error_format (TL_ERROR_HEADER, "Number of string forward keys should be in range 0..10. Value %d", header->string_forward_keys_num);
      return -1;
    }
    if (tl_fetch_error ()) {
      return -1;
    }
    for (i = 0; i < header->string_forward_keys_num; i++) {
      int l = tl_fetch_string_len (1000);
      if (tl_fetch_error ()) {
        return -1;
      }
      header->string_forward_keys[i] = (char *)malloc (l + 1);
      tl_fetch_string_data (header->string_forward_keys[i], l);
      header->string_forward_keys[i][l] = 0;
      if (tl_fetch_error ()) {
        return -1;
      }
    }
  }
  if (flags & TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS) {
    header->int_forward_keys_num = tl_fetch_int ();
    if (header->int_forward_keys_num < 0 || header->int_forward_keys_num > 10) {
      tl_fetch_set_error_format (TL_ERROR_HEADER, "Number of int forward keys should be in range 0..10. Value %d", header->int_forward_keys_num);
      return -1;
    }
    if (tl_fetch_error ()) {
      return -1;
    }
    for (i = 0; i < header->int_forward_keys_num; i++) {
      header->int_forward_keys[i] = tl_fetch_long ();
      if (tl_fetch_error ()) {
        return -1;
      }
    }
  }
  if (flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD) {
    int l = tl_fetch_string_len (1000);
    if (tl_fetch_error ()) {
      return -1;
    }
    header->string_forward = (char *)malloc (l + 1);
    tl_fetch_string_data (header->string_forward, l);
    header->string_forward[l] = 0;
    if (tl_fetch_error ()) {
      return -1;
    }
  }
  if (flags & TL_QUERY_HEADER_FLAG_INT_FORWARD) {
    header->int_forward = tl_fetch_long ();
    if (tl_fetch_error ()) {
      return -1;
    }
  }
  if (flags & TL_QUERY_HEADER_FLAG_WAIT_BINLOG_TIME) {
    header->wait_binlog_time = tl_fetch_long ();
    if (tl_fetch_error ()) {
      return -1;
    }
  }
  if (flags & TL_QUERY_HEADER_FLAG_CUSTOM_TIMEOUT) {
    header->custom_timeout = tl_fetch_int ();
    if (tl_fetch_error ()) {
      return -1;
    }
    if (header->custom_timeout < 0) {
      header->custom_timeout = 0;
    }
    if (header->custom_timeout > 120000) {
      header->custom_timeout = 120000;
    }
  }
  return 0;
}

int tl_fetch_query_header (struct tl_query_header *header) {
  assert (header);
  memset (header, 0, sizeof (*header));
  int t = tl_fetch_unread ();
  if (TL_IN_METHODS->prepend_bytes) {
    tl_fetch_skip (TL_IN_METHODS->prepend_bytes);
  }
  header->op = tl_fetch_int ();
  header->real_op = header->op;
  header->ref_cnt = 1;
  if (header->op != (int)RPC_INVOKE_REQ && header->op != (int)RPC_INVOKE_KPHP_REQ) {
    tl_fetch_set_error ("Expected RPC_INVOKE_REQ or RPC_INVOKE_KPHP_REQ", TL_ERROR_HEADER);
    return -1;
  }
  header->qid = tl_fetch_long ();
  if (header->op == (int)RPC_INVOKE_KPHP_REQ) {
    tl_fetch_raw_data (header->invoke_kphp_req_extra, 24);
    if (tl_fetch_error ()) {
      return -1;
    }
  }
  while (1) {
    int op = tl_fetch_lookup_int ();
    int ok = 1;
    switch (op) {
    case RPC_DEST_ACTOR:
      assert (tl_fetch_int () == (int)RPC_DEST_ACTOR);
      header->actor_id = tl_fetch_long ();
      break;
    case RPC_DEST_ACTOR_FLAGS:
      assert (tl_fetch_int () == (int)RPC_DEST_ACTOR_FLAGS);
      header->actor_id = tl_fetch_long ();
      tl_fetch_query_flags (header);
      break;
    case RPC_DEST_FLAGS:
      assert (tl_fetch_int () == (int)RPC_DEST_FLAGS);      
      tl_fetch_query_flags (header);
    default:
      ok = 0;
      break;
    }
    if (tl_fetch_error ()) {
      return -1;
    }
    if (!ok) { 
      rpc_queries_received ++;
      return t - tl_fetch_unread ();
    }
  }
}

int tl_fetch_query_answer_flags (struct tl_query_header *header) {
  int flags = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return -1;
  }
  if (header->flags & flags) {
    tl_fetch_set_error_format (TL_ERROR_HEADER, "Duplicate flags in header 0x%08x", header->flags & flags);
    return -1;
  }
  if (flags & ~TL_QUERY_RESULT_HEADER_FLAG_MASK) {
    tl_fetch_set_error_format (TL_ERROR_HEADER, "Unsupported flags in header 0x%08x", (~TL_QUERY_RESULT_HEADER_FLAG_MASK) & flags);
    return -1;
  }
  header->flags |= flags;
  if (flags & TL_QUERY_RESULT_HEADER_FLAG_BINLOG_POS) {
    header->binlog_pos = tl_fetch_long ();
    if (tl_fetch_error ()) {
      return -1;
    }
  }
  if (flags & TL_QUERY_RESULT_HEADER_FLAG_BINLOG_TIME) {
    header->binlog_time = tl_fetch_long ();
    if (tl_fetch_error ()) {
      return -1;
    }
  }
  return 0;
}

int tl_fetch_query_answer_header (struct tl_query_header *header) {
  assert (header);
  memset (header, 0, sizeof (*header));
  int t = tl_fetch_unread ();
  if (TL_IN_METHODS->prepend_bytes) {
    tl_fetch_skip (TL_IN_METHODS->prepend_bytes);
  }
  header->op = tl_fetch_int ();
  header->real_op = header->op;
  header->ref_cnt = 1;
  if (header->op != RPC_REQ_ERROR && header->op != RPC_REQ_RESULT ) {
    tl_fetch_set_error ("Expected RPC_REQ_ERROR or RPC_REQ_RESULT", TL_ERROR_HEADER);
    return -1;
  }
  header->qid = tl_fetch_long ();
  while (1) {
    int ok = 1;
    if (header->op != RPC_REQ_ERROR) {
      int op = tl_fetch_lookup_int ();
      switch (op) {
      case RPC_REQ_ERROR:
        assert (tl_fetch_int () == RPC_REQ_ERROR);
        header->op = RPC_REQ_ERROR_WRAPPED;
        tl_fetch_long ();
        break;
      case RPC_REQ_ERROR_WRAPPED:
        header->op = RPC_REQ_ERROR_WRAPPED;
        break;
      case RPC_REQ_RESULT_FLAGS:
        assert (tl_fetch_int () == (int)RPC_REQ_RESULT_FLAGS);
        tl_fetch_query_answer_flags (header);
        break;
      default:
        ok = 0;
        break;
      }
    } else {
      ok = 0;
    }
    if (tl_fetch_error ()) {
      return -1;
    }
    if (!ok) {
      if (header->op == RPC_REQ_ERROR || header->op == RPC_REQ_ERROR_WRAPPED) {
        rpc_answers_error ++;
      } else {
        rpc_answers_received ++;
      }
      return t - tl_fetch_unread ();
    }
  }
}

static inline int __tl_store_init (void *out, void *out_extra, enum tl_type type, struct tl_out_methods *methods, int size, int keep_error, long long qid) {
  if (TL_OUT_METHODS && TL_OUT_METHODS->store_clear) {
    TL_OUT_METHODS->store_clear ();
  }
  TL_OUT = out;
  TL_OUT_EXTRA = out_extra;
  if (out) {
    TL_OUT_METHODS = methods;
    TL_OUT_TYPE = type;
  } else {
    TL_OUT_TYPE = tl_type_none;
  }

  if (!(TL_OUT_METHODS->flags & TLF_ALLOW_PREPEND)) {
    TL_OUT_SIZE = (int *)TL_OUT_METHODS->store_get_ptr (12 + TL_OUT_METHODS->prepend_bytes);
  }
  
  if (!keep_error) {
    if (TL_ERROR) {
      free (TL_ERROR);
      TL_ERROR = 0;
      TL_ERRNUM = 0;
    }
  }
  TL_OUT_POS = 0;
  TL_OUT_QID = qid;
  TL_OUT_REMAINING = size;

  if (out) {
    TL_COPY_THROUGH = TL_OUT_METHODS ? TL_OUT_METHODS->copy_through[TL_IN_TYPE] : 0;
  } else {
    TL_COPY_THROUGH = 0;
  }
  return 0;
}

static inline int _tl_store_init (struct connection *c, long long qid, int keep_error) {
  if (c) {
    TL_OUT_PID = &(RPCS_DATA(c)->remote_pid);
  } else {
    TL_OUT_PID = 0;
  }
  return __tl_store_init (c, 0, tl_type_conn, &tl_out_conn_methods, (1 << 27), keep_error, qid);
}

int tl_store_init (struct connection *c, long long qid) {
  return _tl_store_init (c, qid, 0);
}

int tl_store_init_keep_error (struct connection *c, long long qid) {
  return _tl_store_init (c, qid, 1);
}

static inline int _tl_store_init_raw_msg (struct udp_target *S, long long qid, int keep_error) {
  if (S) {
    TL_OUT_PID = &(S->PID);
  } else {
    TL_OUT_PID = 0;
  }
  struct raw_message *d = 0;
  if (S) {
    d = (struct raw_message *)zmalloc (sizeof (*d));
    rwm_init (d, 0);
  }
  return __tl_store_init (d, S, tl_type_raw_msg, &tl_out_raw_msg_methods, (1 << 27), keep_error, qid);
}

int tl_store_init_raw_msg (struct udp_target *S, long long qid) {
  return _tl_store_init_raw_msg (S, qid, 0);
}

int tl_store_init_raw_msg_keep_error (struct udp_target *S, long long qid) {
  return _tl_store_init_raw_msg (S, qid, 1);
}

static inline int _tl_store_init_tcp_raw_msg (struct connection *c, long long qid, int keep_error) {
  if (c) {
    TL_OUT_PID = &(TCP_RPC_DATA(c)->remote_pid);
  } else {
    TL_OUT_PID = 0;
  }
  struct raw_message *d = 0;
  if (c) {
    d = (struct raw_message *)zmalloc (sizeof (*d));
    rwm_init (d, 0);
  }
  return __tl_store_init (d, c, tl_type_tcp_raw_msg, &tl_out_tcp_raw_msg_methods, (1 << 27), keep_error, qid);
}

int tl_store_init_tcp_raw_msg (struct connection *c, long long qid) {
  return _tl_store_init_tcp_raw_msg (c, qid, 0);
}

int tl_store_init_tcp_raw_msg_keep_error (struct connection *c, long long qid) {
  return _tl_store_init_tcp_raw_msg (c, qid, 1);
}

int tl_store_init_any (enum tl_type type, void *out, long long qid) {
  switch (type) {
  case tl_type_conn:
    return tl_store_init ((struct connection *)out, qid);
  case tl_type_raw_msg:
    return tl_store_init_raw_msg ((struct udp_target *)out, qid);
  case tl_type_tcp_raw_msg:
    return tl_store_init_tcp_raw_msg (out, qid);
  default:
    assert (0);
  }
}

int tl_store_init_any_keep_error (enum tl_type type, void *out, long long qid) {
  switch (type) {
  case tl_type_conn:
    return tl_store_init_keep_error ((struct connection *)out, qid);
  case tl_type_raw_msg:
    return tl_store_init_raw_msg_keep_error ((struct udp_target *)out, qid);
  case tl_type_tcp_raw_msg:
    return tl_store_init_tcp_raw_msg_keep_error (out, qid);
  default:
    assert (0);
  }
}

int tl_store_header (struct tl_query_header *header) {
  assert (tl_store_check (0) >= 0);  
  assert (header->op == (int)RPC_REQ_ERROR || header->op == (int)RPC_REQ_RESULT || header->op == (int)RPC_INVOKE_REQ || header->op == (int)RPC_REQ_ERROR_WRAPPED || header->op == (int)RPC_INVOKE_KPHP_REQ);
  if (header->op == (int)RPC_INVOKE_REQ || header->op == (int)RPC_INVOKE_KPHP_REQ) {
    if (header->op == (int)RPC_INVOKE_KPHP_REQ) {
      tl_store_raw_data (header->invoke_kphp_req_extra, 24);
    }
    if (header->actor_id || header->flags) {
      if (header->flags) {
        if ((header->flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS) && header->string_forward_keys_pos >= header->string_forward_keys_num) {
          header->flags &= ~TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS;
        }
        if ((header->flags & TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS) && header->int_forward_keys_pos >= header->int_forward_keys_num) {
          header->flags &= ~TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS;
        }
        tl_store_int (RPC_DEST_ACTOR_FLAGS);
        tl_store_long (header->actor_id);
        tl_store_int (header->flags);
        if (header->flags & TL_QUERY_HEADER_FLAG_WAIT_BINLOG) {
          tl_store_long (header->wait_binlog_pos);
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_KPHP_DELAY) {
          tl_store_int (header->kitten_php_delay);
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS) {
          tl_store_int (header->string_forward_keys_num - header->string_forward_keys_pos);
          int i;
          for (i = header->string_forward_keys_pos; i < header->string_forward_keys_num; i++) {
            tl_store_string (header->string_forward_keys[i], strlen (header->string_forward_keys[i]));
          }
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS) {
          tl_store_int (header->int_forward_keys_num - header->int_forward_keys_pos);
          int i;
          for (i = header->int_forward_keys_pos; i < header->int_forward_keys_num; i++) {
            tl_store_long (header->int_forward_keys[i]);
          }
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD) {
          tl_store_string (header->string_forward, strlen (header->string_forward));
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_INT_FORWARD) {
          tl_store_long (header->int_forward);
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_WAIT_BINLOG_TIME) {
          tl_store_long (header->wait_binlog_time);
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_CUSTOM_TIMEOUT) {
          tl_store_int (header->custom_timeout);
        }
      } else {
        tl_store_int (RPC_DEST_ACTOR);
        tl_store_long (header->actor_id);
      }
    }
  } else if (header->op == RPC_REQ_ERROR_WRAPPED) {
    tl_store_int (RPC_REQ_ERROR);
    tl_store_long (TL_OUT_QID);    
  } else if (header->op == RPC_REQ_RESULT) {
    if (header->flags) {
      tl_store_int (RPC_REQ_RESULT_FLAGS);
      tl_store_int (header->flags);
      if (header->flags & TL_QUERY_RESULT_HEADER_FLAG_BINLOG_POS) {
        tl_store_long (header->binlog_pos);
      }
      if (header->flags & TL_QUERY_RESULT_HEADER_FLAG_BINLOG_TIME) {
        tl_store_long (header->binlog_time);
      }
    }
  }
  return 0;
}

int tl_write_string (const char *s, int len, char *buf, int size) {
  assert (len >= 0);
  assert (len < (1 << 24));
  if (size < len + 4) { return -1; }
  if (len < 254) {
    *(buf ++) = len;
    memcpy (buf, s, len);
    int pad = (-(len + 1)) & 3;
    memset (buf + len, 0, pad);
    return 1 + len + pad;
  } else {
    *(buf ++) = 254;
    memcpy (buf, &len, 3);
    buf += 3;
    
    memcpy (buf, s, len);
    int pad = (-(len)) & 3;
    memset (buf + len, 0, pad);
    return 4 + len + pad;
  }
  return 0;
}


int tl_write_header (struct tl_query_header *header, int *buf, int size) {
  int _size = size;
  assert (header->op == (int)RPC_REQ_ERROR || header->op == (int)RPC_REQ_RESULT || header->op == (int)RPC_INVOKE_REQ || header->op == (int)RPC_REQ_ERROR_WRAPPED || header->op == (int)RPC_INVOKE_KPHP_REQ);
  if (header->op == (int)RPC_INVOKE_REQ || header->op == (int)RPC_INVOKE_KPHP_REQ) {
    if (header->op == (int)RPC_INVOKE_KPHP_REQ) {
      if (size < 24) { return -1; }
      memcpy (buf, header->invoke_kphp_req_extra, 24);
      buf += 24;
      size -= 24;
    }
    if (header->actor_id || header->flags) {
      if (header->flags) {
        if ((header->flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS) && header->string_forward_keys_pos >= header->string_forward_keys_num) {
          header->flags &= ~TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS;
        }
        if ((header->flags & TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS) && header->int_forward_keys_pos >= header->int_forward_keys_num) {
          header->flags &= ~TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS;
        }
        if (size < 16) { return -1; }
        *(buf ++) = RPC_DEST_ACTOR_FLAGS;
        *(long long *)buf = header->actor_id;
        buf += 2;
        *(buf ++) = header->flags;
        size -= 16;
        if (header->flags & TL_QUERY_HEADER_FLAG_WAIT_BINLOG) {
          if (size < 8) { return -1; }
          *(long long *)buf = header->wait_binlog_pos;
          buf += 2;
          size -= 8;
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_KPHP_DELAY) {
          if (size < 4) { return -1; }
          *(buf ++) = header->kitten_php_delay;
          size -= 4;
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS) {
          if (size < 4) { return -1; }
          *(buf ++) = header->string_forward_keys_num - header->string_forward_keys_pos;
          size -= 4;
          int i;
          for (i = header->string_forward_keys_pos; i < header->string_forward_keys_num; i++) {
            int t = tl_write_string (header->string_forward_keys[i], strlen (header->string_forward_keys[i]), (char *)buf, size);
            if (t < 0) { return t; }
            buf += (t / 4);
            size -= t;
          }
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS) {
          if (size < 4) { return -1; }
          *(buf ++) = header->int_forward_keys_num - header->int_forward_keys_pos;
          size -= 4;
          int i;
          for (i = header->int_forward_keys_pos; i < header->int_forward_keys_num; i++) {
            if (size < 8) { return -1; }
            *(long long *)buf = header->int_forward_keys[i];
            buf += 2;
            size -= 8;
          }
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_STRING_FORWARD) {
          int t = tl_write_string (header->string_forward, strlen (header->string_forward), (char *)buf, size);
          if (t < 0) { return -1; }
          buf += (t / 4);
          size -= t;
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_INT_FORWARD) {
          if (size < 8) { return -1; }
          *(long long *)buf = header->int_forward;
          buf += 2;
          size -= 8;
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_WAIT_BINLOG_TIME) {
          if (size < 8) { return -1; }
          *(long long *)buf = header->wait_binlog_time;
          buf += 2;
          size -= 8;
        }
        if (header->flags & TL_QUERY_HEADER_FLAG_CUSTOM_TIMEOUT) {
          if (size < 4) { return -1; }
          *(buf ++) = header->custom_timeout;
          size -= 4;
        }
        return _size - size;
      } else {
        if (size < 12) { return -1; }
        buf[0] = RPC_DEST_ACTOR;
        *(long long *)(buf + 1) = header->actor_id;
        return 12;
      }
    } else {
      return 0;
    }
  } else if (header->op == RPC_REQ_ERROR_WRAPPED) {
    if (size < 12) {
      return -1;
    }
    buf[0] = RPC_REQ_ERROR;
    *(long long *)(buf + 1) = TL_OUT_QID;
    return 12;
  } else if (header->op == RPC_REQ_RESULT) {
    if (header->flags) {
      if (size < 32) { return -1; }
      *(buf ++) = RPC_REQ_RESULT_FLAGS;
      *(buf ++) = header->flags;
      size -= 8;
      if (header->flags & TL_QUERY_RESULT_HEADER_FLAG_BINLOG_POS) {
        *(long long *)buf = header->binlog_pos;
        buf += 2;
        size -= 8;
      }
      if (header->flags & TL_QUERY_RESULT_HEADER_FLAG_BINLOG_TIME) {
        *(long long *)buf = header->binlog_time;
        buf += 2;
        size -= 8;
      }
      return _size - size; 
    }
    return 0;
  } else if (header->op == RPC_REQ_ERROR) {
    return 0;
  }
  return 0;
}

int tl_store_end_ext (int op) {
  if (TL_OUT_TYPE == tl_type_none) {
    return 0;
  }
  assert (TL_OUT);  
  assert (TL_OUT_TYPE);
  if (vv_tl_drop_probability) {
    if (memcmp (TL_OUT_PID, &PID, sizeof (struct process_id)) && drand48 () < vv_tl_drop_probability) {
      tl_store_clear ();
      return 0;
    }
  }
  if (TL_ERROR) {
//    tl_store_clear ();    
    tl_store_clean ();
    vkprintf (1, "tl_store_end: writing error %s, errnum %d, tl.out_pos = %d\n", TL_ERROR, TL_ERRNUM, TL_OUT_POS);
    //tl_store_clear ();
    tl_store_int (RPC_REQ_ERROR);
    tl_store_long (TL_OUT_QID);
    tl_store_int (TL_ERRNUM);
    tl_store_string (TL_ERROR, strlen (TL_ERROR));

    rpc_sent_errors ++;
  } else {
    if (op == RPC_REQ_RESULT) {
      rpc_sent_answers ++;
    } else {
      rpc_sent_queries ++;
    }
  }
  assert (!(TL_OUT_POS & 3));
  int *p;
  if (TL_OUT_FLAGS & TLF_ALLOW_PREPEND) {
    p = TL_OUT_SIZE = tl_store_get_prepend_ptr (TL_OUT_METHODS->prepend_bytes + 12);
  } else {
    p = TL_OUT_SIZE;
  }
  *(long long *)(p + (TL_OUT_METHODS->prepend_bytes) / 4 + 1) = TL_OUT_QID;
  *(p + (TL_OUT_METHODS->prepend_bytes) / 4) = op;

  if (TL_OUT_METHODS->store_prefix) {
    TL_OUT_METHODS->store_prefix ();
  }
  
  if (TL_OUT_FLAGS & TLF_CRC32) {
    int crc32 = 0;
    if (!(rpc_crc32_mode & 2)) {
      crc32 = TL_OUT_METHODS->store_crc32_partial (TL_OUT_POS + 12 + TL_OUT_METHODS->prepend_bytes, 0);
    }
    tl_store_int (crc32);
  }

  TL_OUT_METHODS->store_flush ();
  vkprintf (2, "tl_store_end: written %d bytes, qid = %lld, PID = " PID_PRINT_STR "\n", TL_OUT_POS, TL_OUT_QID, PID_TO_PRINT (TL_OUT_PID));
  TL_OUT = 0;
  TL_OUT_TYPE = tl_type_none;
  TL_OUT_METHODS = 0;
  return 0;
}

void default_peer_free (struct tl_peer *self) {
  zfree (self, sizeof (*self));
}

int peer_init_store (struct tl_peer *self, long long qid) {
  struct rpc_target *T = rpc_target_lookup (&self->PID);
  if (!T) {
    return -1;
  }
  struct connection *c = rpc_target_choose_connection (T, &self->PID);
  if (!c) {
    return -1;
  }
  tl_store_init (c, qid);
  return 0;
}

int peer_init_store_keep_error (struct tl_peer *self, long long qid) {
  struct rpc_target *T = rpc_target_lookup (&self->PID);
  if (!T) {
    return -1;
  }
  struct connection *c = rpc_target_choose_connection (T, &self->PID);
  if (!c) {
    return -1;
  }
  tl_store_init_keep_error (c, qid);
  return 0;
}

struct tl_peer_methods conn_peer_methods = {
  .free = default_peer_free,
  .init_store = peer_init_store,
  .init_store_keep_error = peer_init_store_keep_error,
};

int peer_udp_init_store (struct tl_peer *self, long long qid) {
  struct udp_target *S = udp_target_lookup (&self->PID);
  if (!S || S->state == udp_failed) {
    return -1;
  }
  tl_store_init_raw_msg (S, qid);
  return 0;
}

int peer_udp_init_store_keep_error (struct tl_peer *self, long long qid) {
  struct udp_target *S = udp_target_lookup (&self->PID);
  if (!S || S->state == udp_failed) {
    return -1;
  }
  tl_store_init_raw_msg_keep_error (S, qid);
  return 0;
}

struct tl_peer_methods udp_peer_methods = {
  .free = default_peer_free,
  .init_store = peer_udp_init_store,
  .init_store_keep_error = peer_udp_init_store_keep_error,
};

int peer_tcp_init_store (struct tl_peer *self, long long qid) {
  struct rpc_target *T = rpc_target_lookup (&self->PID);
  if (!T) {
    return -1;
  }
  struct connection *c = rpc_target_choose_connection (T, &self->PID);
  if (!c) {
    return -1;
  }
  tl_store_init_tcp_raw_msg (c, qid);
  return 0;
}

int peer_tcp_init_store_keep_error (struct tl_peer *self, long long qid) {
  struct rpc_target *T = rpc_target_lookup (&self->PID);
  if (!T) {
    return -1;
  }
  struct connection *c = rpc_target_choose_connection (T, &self->PID);
  if (!c) {
    return -1;
  }
  tl_store_init_tcp_raw_msg_keep_error (c, qid);
  return 0;
}

struct tl_peer_methods tcp_peer_methods = {
  .free = default_peer_free,
  .init_store = peer_tcp_init_store,
  .init_store_keep_error = peer_tcp_init_store_keep_error,
};

struct tl_peer *create_peer (enum tl_type type, struct process_id *PID) {
  switch (type) {
  case tl_type_conn:
    {
      struct tl_peer *p = zmalloc (sizeof (*p));
      p->PID = *PID;
      p->methods = &conn_peer_methods;
      return p;
    }
  case tl_type_raw_msg:
    {
      struct tl_peer *p = zmalloc (sizeof (*p));
      p->PID = *PID;
      p->methods = &udp_peer_methods;
      return p;
    }
  case tl_type_tcp_raw_msg:
    {
      struct tl_peer *p = zmalloc (sizeof (*p));
      p->PID = *PID;
      p->methods = &tcp_peer_methods;
      return p;
    }
  default:
    assert (0);
    return 0;
  }
}

int tl_init_store (enum tl_type type, struct process_id *pid, long long qid) {
  switch (type) {
  case tl_type_conn:
    {
      struct connection *d = rpc_target_choose_connection (rpc_target_lookup (pid), pid);
      if (d) {
        vkprintf (2, "Good connection\n");
        tl_store_init (d, qid);
        return 1;
      } else {
        vkprintf (2, "Bad connection\n");
        return -1;
      }
    }
  case tl_type_raw_msg:
    {
      struct udp_target *S = udp_target_lookup (pid);
      if (S && S->state != udp_failed) {
        tl_store_init_raw_msg (S, qid);
        return 1;
      } else {
        return -1;
      }
    }
  case tl_type_tcp_raw_msg:
    {
      struct connection *d = rpc_target_choose_connection (rpc_target_lookup (pid), pid);
      if (d) {
        vkprintf (2, "Good connection\n");
        tl_store_init_tcp_raw_msg (d, qid);
        return 1;
      } else {
        vkprintf (2, "Bad connection\n");
        return -1;
      }
    }
  default:
    fprintf (stderr, "type = %d\n", type);
    assert (0);
    return 0;
  }
}

int tl_init_store_keep_error (enum tl_type type, struct process_id *pid, long long qid) {
  switch (type) {
  case tl_type_conn:
    {
      struct connection *d = rpc_target_choose_connection (rpc_target_lookup (pid), pid);
      if (d) {
        vkprintf (2, "Good connection\n");
        tl_store_init_keep_error (d, qid);
        return 1;
      } else {
        vkprintf (2, "Bad connection\n");
        return -1;
      }
    }
  case tl_type_raw_msg:
    {
      struct udp_target *S = udp_target_lookup (pid);
      if (S && S->state != udp_failed) {
        tl_store_init_raw_msg_keep_error (S, qid);
        return 1;
      } else {
        return -1;
      }
    }
  case tl_type_tcp_raw_msg:
    {
      struct connection *d = rpc_target_choose_connection (rpc_target_lookup (pid), pid);
      if (d) {
        vkprintf (2, "Good connection\n");
        tl_store_init_tcp_raw_msg_keep_error (d, qid);
        return 1;
      } else {
        vkprintf (2, "Bad connection\n");
        return -1;
      }
    }
  default:
    fprintf (stderr, "type = %d\n", type);
    assert (0);
    return 0;
  }
}
