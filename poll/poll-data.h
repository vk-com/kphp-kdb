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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#pragma once

#include "net-connections.h"
#include "kfs.h"
#include "kdb-data-common.h"

#include "dl-utils.h"
#include "utils.h"

//#define TEST_MODE

#ifdef TEST_MODE
#  define TTL_EVENT 14
#else
#  define TTL_EVENT 1
#endif

#define MAX_QNAME 24
#define RSEED_LEN 7
#define KEY_LEN (MAX_QNAME + 1 + 8 + 8 + 8 + RSEED_LEN)//{qname}_{ip}{id}{secret}{random}
#define IS_UID(x) (dl_abs (x) < 2000000000)

extern double max_delay, sum_delay;
extern long long cnt_delay;
extern long long events_created, events_sent;

extern long events_memory, keys_memory, subscribers_memory, subscribers_cnt, str_memory, crypto_memory, queues_memory,
            time_keys_memory;
extern int events_cnt, keys_cnt, treap_cnt, treap_cnt_rev, queues_cnt;

#define STAT(x) (x)++
#define STAT_MAX(x, y) if ((y) > (x)) {(x) = (y);}
#define _STR(x) #x
#define STAT_OUT(x) W( _STR(x) "\t" "%lld" "\n", x);

extern long long send_changes_cnt,
  process_changes_cnt,
  changes_len_max,
  process_changes_total_len,
  changes_add_rev_cnt,
  changes_add_rev_len,
  changes_add_cnt,
  changes_add_len,
  changes_del_rev_cnt,
  changes_del_rev_len,
  changes_del_cnt,
  changes_del_len,
  to_add_overflow,
  to_del_overflow;

int init_queue_data (int schema);

int init_all (void);
void free_all (void);

#pragma pack(push,4)

typedef struct t_event event;
struct t_event {
  event *next; /* must be here ! */
  int ref_cnt, /* must be here ! */
      data_len;
  double created;
  char data[0];
};

typedef enum {Q_DEF} qtype;

typedef struct {
  event *ev_first;
  int ref_cnt; /* must be here ! */
  event *ev_last;
  int keys_cnt;
  struct conn_query *first_q, *last_q;  /* must be here ! */

  int subscr_cnt;

  ll id;

  char *name;
  char *extra;
  treap *subs;
} queue;

typedef struct t_qkey qkey;
struct t_qkey {
  char *name;
  event *st;
  queue *q;

  int timeout, ts;
  qkey *next_time, *prev_time;
  int prev_ts;
  event *prev_st;
  int lock;
  double subscribed;
  struct conn_query *conn;
};

typedef struct {
  int n;
  qkey **k;
  char *r;
} qkey_group;

#pragma pack(pop)

char *get_events_http (qkey *k);
char *get_events_http_group (qkey_group *k);

int do_add_event (char *qname, int qname_len, char *data, int data_len, int x, int y, int ttl);

char *get_timestamp_key (char *qname, int id, int ip, int timeout, char *extra, qtype tp);
int may_wait (char *s);

qkey *validate_key (char *key_name, int id, int ip, int req_ts, int a_release, char *err);

qkey_group* qkey_group_alloc (int n);
void qkey_group_free (qkey_group *k);
qkey_group *validate_key_group (char *keys, int id, int ip, int *req_ts, int req_ts_n, int a_release);

void qkey_clear_conn (char *kname);

void release_key (qkey *k);
void release_key_group (qkey_group *k);

void free_by_time (int mx);

int upd_secret (int id);

int subscribers_add_new (ll id, pli *a, int n);
int subscribers_add_new_rev (ll id, pli *a, int n);
int subscribers_del_old (ll id, pli *a, int n);
int subscribers_del_rev (ll id, pli *a, int n);

int get_queue_alias (char *s, ll *res);
queue *get_queue_by_alias (ll id);
queue *get_queue (char *name, int force);

long get_htbls_memory (void);

#define MAX_MEMORY 2000000000
