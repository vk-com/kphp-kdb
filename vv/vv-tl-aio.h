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

#ifndef __VV_TL_AIO_H__
#define __VV_TL_AIO_H__
#include "net-aio.h"
#include "net-rpc-server.h"
#include "kdb-data-common.h"
#include "vv-tl-parse.h"

struct tl_saved_query {
  int (*restart)(struct tl_saved_query *q);
  void (*fail)(struct tl_saved_query *q);
  void (*free_extra)(struct tl_saved_query *q);
  long long qid;
  //struct connection *c;
  //void *out;
  enum tl_type out_type;
  struct process_id pid;
  int wait_num;
  int failed;
  int attempt;
  int error_code;
  void *extra;
};

void add_finished_query (struct tl_saved_query *q);
void tl_aio_fail_start (struct tl_saved_query *s);

void tl_fail_default (struct tl_saved_query *q);
int tl_create_aio_query (struct aio_connection *a, struct connection *c, double timeout, struct conn_query_functions *cq, struct tl_saved_query *extra);
struct tl_saved_query *tl_saved_query_init (void);
int tl_aio_on_end_default (struct conn_query *q);
int tl_delete_aio_query (struct conn_query *q);
void tl_fail_default (struct tl_saved_query *q);
void tl_aio_query_restart_all_finished (void);
void tl_default_aio_fail (struct tl_saved_query *q);


extern conn_query_type_t default_tl_aio_metafile_query_type;
extern struct aio_connection *WaitAioArr[100];
extern int WaitAioArrPos;
void WaitAioArrClear (void);
int WaitAioArrAdd (struct aio_connection *conn);
extern double tl_aio_timeout;

struct saved_data_auto {
  int ref_cnt;
  int result;
  void *fun;
  int iv[16];
  long lv[16];
  double dv[16];
  void *pv[16];
};

static inline void tl_saved_query_free_auto (struct tl_saved_query *q) {
  if (*(int *)(q->extra) > 0) { return; };
  zfree (q->extra, sizeof (struct saved_data_auto));
}

int tl_aio_start (struct aio_connection **aio_connections, int conn_num, double timeout, struct tl_saved_query *q);

#define PATH_LIST_9(fun,extra) PATH_LIST_8(fun,extra) , fun(extra,9)
#define PATH_LIST_8(fun,extra) PATH_LIST_7(fun,extra) , fun(extra,8)
#define PATH_LIST_7(fun,extra) PATH_LIST_6(fun,extra) , fun(extra,7)
#define PATH_LIST_6(fun,extra) PATH_LIST_5(fun,extra) , fun(extra,6)
#define PATH_LIST_5(fun,extra) PATH_LIST_4(fun,extra) , fun(extra,5)
#define PATH_LIST_4(fun,extra) PATH_LIST_3(fun,extra) , fun(extra,4)
#define PATH_LIST_3(fun,extra) PATH_LIST_2(fun,extra) , fun(extra,3)
#define PATH_LIST_2(fun,extra) PATH_LIST_1(fun,extra) , fun(extra,2)
#define PATH_LIST_1(fun,extra) PATH_LIST_0(fun,extra) , fun(extra,1)
#define PATH_LIST_0(fun,extra)

#define PATH_LIST(i,fun,extra) PATH_LIST_ ## i (fun,extra)

#define EPATH_LIST_9(fun,extra) EPATH_LIST_8(fun,extra) , fun(extra,9)
#define EPATH_LIST_8(fun,extra) EPATH_LIST_7(fun,extra) , fun(extra,8)
#define EPATH_LIST_7(fun,extra) EPATH_LIST_6(fun,extra) , fun(extra,7)
#define EPATH_LIST_6(fun,extra) EPATH_LIST_5(fun,extra) , fun(extra,6)
#define EPATH_LIST_5(fun,extra) EPATH_LIST_4(fun,extra) , fun(extra,5)
#define EPATH_LIST_4(fun,extra) EPATH_LIST_3(fun,extra) , fun(extra,4)
#define EPATH_LIST_3(fun,extra) EPATH_LIST_2(fun,extra) , fun(extra,3)
#define EPATH_LIST_2(fun,extra) EPATH_LIST_1(fun,extra) , fun(extra,2)
#define EPATH_LIST_1(fun,extra) EPATH_LIST_0(fun,extra) fun(extra,1)
#define EPATH_LIST_0(fun,extra)

#define EPATH_LIST(i,fun,extra) EPATH_LIST_ ## i (fun,extra)

#define QATH_LIST_9(fun,extra) QATH_LIST_8(fun,extra) ; fun(extra,9)
#define QATH_LIST_8(fun,extra) QATH_LIST_7(fun,extra) ; fun(extra,8)
#define QATH_LIST_7(fun,extra) QATH_LIST_6(fun,extra) ; fun(extra,7)
#define QATH_LIST_6(fun,extra) QATH_LIST_5(fun,extra) ; fun(extra,6)
#define QATH_LIST_5(fun,extra) QATH_LIST_4(fun,extra) ; fun(extra,5)
#define QATH_LIST_4(fun,extra) QATH_LIST_3(fun,extra) ; fun(extra,4)
#define QATH_LIST_3(fun,extra) QATH_LIST_2(fun,extra) ; fun(extra,3)
#define QATH_LIST_2(fun,extra) QATH_LIST_1(fun,extra) ; fun(extra,2)
#define QATH_LIST_1(fun,extra) QATH_LIST_0(fun,extra) ; fun(extra,1)
#define QATH_LIST_0(fun,extra)

#define QATH_LIST(i,fun,extra) QATH_LIST_ ## i (fun,extra)

#define __tl_aio_pr(extra,i) extra ## i
#define __tl_aio_pr2(extra,i) x->extra ## v[i - 1] = extra ## i
#define tl_saved_query_init_auto(ints,longs,dbls,ptrs) \
static inline struct tl_saved_query *tl_saved_query_init_auto_ ## ints ## _ ## longs ## _ ## dbls ## _ ## ptrs (int (*restart)(struct tl_saved_query *), void *fun PATH_LIST (ints,__tl_aio_pr,int i) PATH_LIST(longs,__tl_aio_pr,long l) PATH_LIST (dbls,__tl_aio_pr,double d) PATH_LIST(ptrs,__tl_aio_pr,void *p)) { \
  struct tl_saved_query *q = tl_saved_query_init (); \
  q->restart = restart; \
  q->fail = tl_default_aio_fail; \
  q->free_extra = tl_saved_query_free_auto; \
  struct saved_data_auto *x = zmalloc (sizeof (*x)); \
  q->extra = x;  \
  x->fun = fun; \
  x->result = 0; \
  QATH_LIST(ints,__tl_aio_pr2,i); \
  QATH_LIST(longs,__tl_aio_pr2,l); \
  QATH_LIST(dbls,__tl_aio_pr2,d); \
  QATH_LIST(ptrs,__tl_aio_pr2,p); \
  return q; \
}\
static inline int tl_saved_query_restart_auto_ ## ints ## _ ## longs ## _ ## dbls ## _ ## _ ## ptrs (struct tl_saved_query *q) { \
  struct saved_data_auto *x = q->extra; \
  assert (x); \
  x->ref_cnt --; \
  if (!x->ref_cnt && !x->result) { \
    x->result = 1; \
    return 

//tl_saved_query_init_auto (1, 1, 0, 1);
//tl_saved_query_init_auto (0, 0, 0, 0);
//tl_saved_query_init_auto (4, 0, 0, 0);

#define TL_AIO_TRY(ints,longs,ptrs,fun,...) \
  { \
    int __tl_aio_try = fun (__VA_ARGS__); \
    if (__tl_aio_try != -2) { return __tl_aio_try; } \
    return tl_default_aio_wait (&WaitAio, 1, tl_saved_query_init_auto_ ## ints ## _ ## longs ## _ ## dbls ## _ ## ptrs (fun ## _restart, ##__VA_ARGS__)); \
  }

#define TL_AIO_TRY_WAIT_ARR(ints,longs,ptrs,fun,...) \
  { \
    int __tl_aio_try = fun (__VA_ARGS__); \
    if (__tl_aio_try != -2) { return __tl_aio_try; } \
    return tl_default_aio_wait_arr (WaitAioArr, WaitAioNum, tl_saved_query_init_auto_ ## ints ## _ ## longs ## _ ## ptrs (fun ## _restart, ##__VA_ARGS__)); \
  }

#endif
