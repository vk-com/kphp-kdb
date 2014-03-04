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

#ifndef __VV_TL_PARSE__
#define __VV_TL_PARSE__

#include "net-rpc-server.h"
#include "net-aio.h"
#include <assert.h>
#include <strings.h>
#include <memory.h>
#include <stdlib.h>
#include <stdio.h>
#include "net-buffers.h"
#include "net-connections.h"
#include "net-rpc-server.h"
#include "kdb-data-common.h"
#include "rpc-const.h"
#include "stdarg.h"
#include "net-msg.h"
#include "net-udp.h"
#include "net-udp-targets.h"
#include "vv-io.h"

#define ENABLE_UDP

//#define RPC_INVOKE_REQ 0x2374df3d
//#define RPC_REQ_RESULT 0x63aeda4e
//#define RPC_REQ_ERROR 0x7ae432f5

#define TL_ENGINE_NOP 0x166bb7c6

#define TL_QUERY_HEADER_FLAG_WAIT_BINLOG (1 << 16)
#define TL_QUERY_HEADER_FLAG_KPHP_DELAY (1 << 17)
#define TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS (1 << 18)
#define TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS (1 << 19)
#define TL_QUERY_HEADER_FLAG_STRING_FORWARD (1 << 20)
#define TL_QUERY_HEADER_FLAG_INT_FORWARD (1 << 21)
#define TL_QUERY_HEADER_FLAG_WAIT_BINLOG_TIME (1 << 22)
#define TL_QUERY_HEADER_FLAG_CUSTOM_TIMEOUT (1 << 23)

#define TL_QUERY_HEADER_FLAG_RETURN_BINLOG_POS 1
#define TL_QUERY_HEADER_FLAG_RETURN_BINLOG_TIME 2

#define TL_QUERY_RESULT_HEADER_FLAG_BINLOG_POS 1
#define TL_QUERY_RESULT_HEADER_FLAG_BINLOG_TIME 2

#define TLF_CRC32 1
#define TLF_PERMANENT 2
#define TLF_ALLOW_PREPEND 4

#define TL_QUERY_HEADER_FLAG_MASK (TL_QUERY_HEADER_FLAG_WAIT_BINLOG | TL_QUERY_HEADER_FLAG_RETURN_BINLOG_POS | TL_QUERY_HEADER_FLAG_KPHP_DELAY | TL_QUERY_HEADER_FLAG_STRING_FORWARD_KEYS | TL_QUERY_HEADER_FLAG_INT_FORWARD_KEYS | TL_QUERY_HEADER_FLAG_STRING_FORWARD | TL_QUERY_HEADER_FLAG_INT_FORWARD | TL_QUERY_HEADER_FLAG_WAIT_BINLOG_TIME | TL_QUERY_HEADER_FLAG_RETURN_BINLOG_TIME | TL_QUERY_HEADER_FLAG_CUSTOM_TIMEOUT)
#define TL_QUERY_RESULT_HEADER_FLAG_MASK (TL_QUERY_RESULT_HEADER_FLAG_BINLOG_POS | TL_QUERY_RESULT_HEADER_FLAG_BINLOG_TIME)

extern struct tl_act_extra *(*tl_parse_function)(long long actor_id);
extern void (*tl_stat_function)(void);
//int tl_query_act (struct connection *c, int op, int len);
struct tl_query_header *tl_query_header_dup (struct tl_query_header *h);
void tl_query_header_delete (struct tl_query_header *h);
int default_tl_rpcs_execute (struct connection *c, int op, int len);
int default_tl_close_conn (struct connection *c, int who);
int tl_store_stats (const char *s, int raw);
extern double vv_tl_drop_probability;

void tl_restart_all_ready (void);
void tl_default_act_free (struct tl_act_extra *extra);

struct tl_act_extra {
  int size;
  int flags;  
  int (*act)(struct tl_act_extra *data);
  void (*free)(struct tl_act_extra *data);
  struct tl_act_extra *(*dup)(struct tl_act_extra *data);
  struct tl_query_header *header;
  int extra[0];
};

static inline struct tl_act_extra *tl_act_extra_init (void *buf, int size, int (*act)(struct tl_act_extra *)) {
  struct tl_act_extra *extra = (struct tl_act_extra *)buf;
  extra->size = size + sizeof (*extra);
  extra->flags = 0;
  extra->act = act;
  extra->free = 0;
  extra->dup = 0;
  return extra;
}

#define RPC_REQ_ERROR_WRAPPED (RPC_REQ_ERROR + 1)

extern int rpc_crc32_mode;
extern long long rpc_queries_received, rpc_queries_ok, rpc_queries_error;

struct tl_in_methods {
  void (*fetch_raw_data)(void *buf, int len);
  void (*fetch_move)(int len);
  void (*fetch_lookup)(void *buf, int len);
  void (*fetch_clear)(void);
  void (*fetch_mark)(void);
  void (*fetch_mark_restore)(void);
  void (*fetch_mark_delete)(void);
  int flags;
  int prepend_bytes;
};

struct tl_out_methods {
  void *(*store_get_ptr)(int len);
  void *(*store_get_prepend_ptr)(int len);
  void (*store_raw_data)(const void *buf, int len);
  void (*store_read_back)(int len);
  void (*store_read_back_nondestruct)(void *buf, int len);
  unsigned (*store_crc32_partial)(int len, unsigned start);
  void (*store_flush)(void);
  void (*store_clear)(void);
  void (*copy_through[10])(int len, int advance);
  void (*store_prefix)(void);
  int flags;
  int prepend_bytes;
};

enum tl_type {
  tl_type_none,
  tl_type_conn,
  tl_type_str,
  tl_type_nbit,
  tl_type_raw_msg,
  tl_type_tcp_raw_msg
};

struct tl_state {
  enum tl_type in_type;
  struct tl_in_methods *in_methods;
  void *in;
  void *in_mark;

//  struct nb_iterator *in_it;
  int in_remaining;
  int in_pos;
  int in_mark_pos;

  enum tl_type out_type;
  struct tl_out_methods *out_methods;  
  void *out;
  void *out_extra;
  int out_pos;
  int out_remaining;
  int *out_size;

  //struct process_id *out_pid;

  //struct connection *err;
  char *error;
  int errnum;

  long long out_qid;
  //long long err_qid;

  int attempt_num;

  struct process_id *in_pid;
  struct process_id *out_pid;

  void (*copy_through)(int len, int advance);
};

struct tl_query_header {
  long long qid;
  long long actor_id;
  int flags;
  int op;
  int real_op;
  int ref_cnt;
  int kitten_php_delay;
  long long int_forward;
  int int_forward_keys_pos;
  int string_forward_keys_pos;
  int int_forward_keys_num;
  int string_forward_keys_num;
  long long wait_binlog_pos;
  long long binlog_pos;
  long long wait_binlog_time;
  long long binlog_time;
  int custom_timeout;
  char *string_forward;
  long long int_forward_keys[10];
  char *string_forward_keys[10];
  long long invoke_kphp_req_extra[3];
};

extern struct tl_state tl;
extern struct tl_state *tlio;

extern struct tl_in_methods tl_in_conn_methods;
extern struct tl_in_methods tl_in_nbit_methods;
extern struct tl_in_methods tl_in_raw_msg_methods;
extern struct tl_out_methods tl_out_conn_methods;
extern struct tl_out_methods tl_out_raw_msg_methods;
/*int tl_fetch_init (struct connection *c, int size);
int tl_fetch_init_iterator (nb_iterator_t *it, int size);
int tl_fetch_int (void);
long long tl_fetch_long (void);
int tl_fetch_string_len (int max_len);
//int tl_fetch_pad (void);
int tl_fetch_string_data (char *buf, int len);
int tl_fetch_skip_string_data (int len);
int tl_fetch_string (char *buf, int max_len);
int tl_fetch_check_str_end (int size);
int tl_fetch_error (void);
int tl_fetch_check_eof (void);
int tl_fetch_end (void);
int tl_fetch_unread (void);
int tl_fetch_set_error (char *s, int errnum);
int tl_fetch_check (int nbytes); // Sets error if bad
int tl_fetch_attempt_num (void);
int tl_fetch_skip (int len);

int tl_store_init (struct connection *c, long long qid);
int tl_store_int (int x);
int tl_store_long (long long x);
int tl_store_string_len (int len);
//int tl_store_pad (void);
int tl_store_string_data (const char *s, int len);
int tl_store_string (const char *s, int len);
int tl_store_end (void);
int tl_store_clean (void);
int tl_store_clear (void);*/

#define TL_IN (tlio->in)
#define TL_IN_CONN ((struct connection *)(tlio->in))
#define TL_IN_NBIT ((nb_iterator_t *)(tlio->in))
#define TL_IN_RAW_MSG ((struct raw_message *)(tlio->in))
#define TL_IN_TYPE (tlio->in_type)
#define TL_IN_REMAINING (tlio->in_remaining)
#define TL_IN_POS (tlio->in_pos)
#define TL_IN_METHODS (tlio->in_methods)
#define TL_IN_MARK (tlio->in_mark)
#define TL_IN_MARK_POS (tlio->in_mark_pos)
#define TL_IN_PID (tlio->in_pid)
#define TL_IN_FLAGS (tlio->in_methods->flags)

#define TL_OUT ((tlio->out))
#define TL_OUT_TYPE (tlio->out_type)
#define TL_OUT_SIZE (tlio->out_size)
#define TL_OUT_CONN ((struct connection *)(tlio->out))
#define TL_OUT_RAW_MSG ((struct raw_message *)(tlio->out))
#define TL_OUT_POS (tlio->out_pos)
#define TL_OUT_REMAINING (tlio->out_remaining)
#define TL_OUT_METHODS (tlio->out_methods)
#define TL_OUT_QID (tlio->out_qid)
#define TL_OUT_EXTRA (tlio->out_extra)
#define TL_OUT_PID (tlio->out_pid)
#define TL_OUT_FLAGS (tlio->out_methods->flags)

#define TL_ERROR (tlio->error)
#define TL_ERRNUM (tlio->errnum)

#define TL_COPY_THROUGH (tlio->copy_through)

#define TL_ATTEMPT_NUM (tlio->attempt_num)

void add_udp_socket (int port, int mode);
int tl_fetch_set_error_format (int errnum, const char *format, ...) __attribute__ (( format(printf,2,3) ));
int tl_fetch_init (struct connection *c, int size);
int tl_fetch_set_error (const char *s, int errnum);
int tl_fetch_init_iterator (nb_iterator_t *it, int size);
int tl_fetch_init_raw_message (struct raw_message *msg, int size);
int tl_fetch_init_tcp_raw_message (struct raw_message *msg, int size);
int tl_fetch_query_flags (struct tl_query_header *header);
int tl_fetch_query_header (struct tl_query_header *header);
int tl_fetch_query_answer_header (struct tl_query_header *header);
int tl_fetch_query_answer_flags (struct tl_query_header *header);
int tl_store_init (struct connection *c, long long qid);
int tl_store_init_keep_error (struct connection *c, long long qid);
int tl_store_init_raw_msg (struct udp_target *S, long long qid);
int tl_store_init_raw_msg_keep_error (struct udp_target *S, long long qid);
int tl_store_init_tcp_raw_msg (struct connection *c, long long qid);
int tl_store_init_any_keep_error (enum tl_type type, void *out, long long qid);
int tl_store_init_any (enum tl_type type, void *out, long long qid);
int tl_store_header (struct tl_query_header *header);
int tl_write_header (struct tl_query_header *header, int *buf, int size);
int tl_write_string (const char *s, int len, char *buf, int size);
int tl_store_end_ext (int op);
int tl_init_store (enum tl_type type, struct process_id *pid, long long qid);
int tl_init_store_keep_error (enum tl_type type, struct process_id *pid, long long qid);


struct tl_peer;
struct tl_peer_methods {
  void (*free)(struct tl_peer *self);
  int (*init_store)(struct tl_peer *self, long long qid);
  int (*init_store_keep_error)(struct tl_peer *self, long long qid);
};

struct tl_peer {
  struct tl_peer_methods *methods;
  struct process_id PID;
  int data[0];
};

struct tl_peer *create_peer (enum tl_type type,  struct process_id *PID);

static inline int tl_fetch_check (int nbytes) {
  if (!TL_IN_TYPE) {
    tl_fetch_set_error ("Trying to read from unitialized in buffer", TL_ERROR_INTERNAL);
    return -1;
  }
  if (nbytes >= 0) {
    if (TL_IN_REMAINING < nbytes) {
      tl_fetch_set_error_format (TL_ERROR_SYNTAX, "Trying to read %d bytes at position %d (size = %d)", nbytes, TL_IN_POS, TL_IN_POS + TL_IN_REMAINING);
      return -1;
    }
  } else {
    if (TL_IN_POS < -nbytes) {
      tl_fetch_set_error_format (TL_ERROR_SYNTAX, "Trying to read %d bytes at position %d (size = %d)", nbytes, TL_IN_POS, TL_IN_POS + TL_IN_REMAINING);
      return -1;
    }
  }
  if (TL_ERROR) {
    return -1;
  }
  return 0;
}


inline static void tl_fetch_raw_data (void *buf, int size) {
  TL_IN_METHODS->fetch_raw_data (buf, size);
  TL_IN_POS += size;
  TL_IN_REMAINING -= size;
}

inline static void tl_fetch_skip_raw_data (int size) {
  TL_IN_METHODS->fetch_move (size);
  TL_IN_POS += size;
  TL_IN_REMAINING -= size;
}

static inline int tl_fetch_lookup_int (void) {
  if (tl_fetch_check (4) < 0) {
    return -1;
  }
  int x;
  TL_IN_METHODS->fetch_lookup (&x, 4);
  return x;
}

static inline int tl_fetch_lookup_data (char *data, int len) {
  if (tl_fetch_check (len) < 0) {
    return -1;
  }
  TL_IN_METHODS->fetch_lookup (data, len);
  return len;
}

static inline int tl_fetch_int (void) {
  if (__builtin_expect (tl_fetch_check (4) < 0, 0)) {
    return -1;
  }
  int x;
  tl_fetch_raw_data (&x, 4);
  return x;
}

static inline double tl_fetch_double (void) {
  if (__builtin_expect (tl_fetch_check (sizeof (double)) < 0, 0)) {
    return -1;
  }
  double x;
  tl_fetch_raw_data (&x, sizeof (x));
  return x;
}

static inline long long tl_fetch_long (void) {
  if (__builtin_expect (tl_fetch_check (8) < 0, 0)) {
    return -1;
  }
  long long x;
  tl_fetch_raw_data (&x, 8);
  return x;
}

static inline void tl_fetch_mark (void) {
  TL_IN_METHODS->fetch_mark ();
}

static inline void tl_fetch_mark_restore (void) {
  TL_IN_METHODS->fetch_mark_restore ();
}

static inline void tl_fetch_mark_delete (void) {
  TL_IN_METHODS->fetch_mark_delete ();
}

static inline int tl_fetch_string_len (int max_len) {
  if (tl_fetch_check (4) < 0) {
    return -1;
  }
  int x = 0;
  tl_fetch_raw_data (&x, 1);
  if (x == 255) {
    tl_fetch_set_error ("String len can not start with 0xff", TL_ERROR_SYNTAX);
    return -1;
  }
  if (x == 254) {
    tl_fetch_raw_data (&x, 3);
  }
  if (x > max_len) {
    tl_fetch_set_error_format (TL_ERROR_TOO_LONG_STRING, "string is too long: max_len = %d, len = %d", max_len, x);
    return -1;
  }
  if (x > TL_IN_REMAINING) {
    tl_fetch_set_error_format (TL_ERROR_SYNTAX, "string is too long: remaining_bytes = %d, len = %d", TL_IN_REMAINING, x);
    return -1;
  }
  return x;
}

static inline int tl_fetch_pad (void) {
  if (tl_fetch_check (0) < 0) {
    return -1;
  }
  int t = 0;
  int pad = (-TL_IN_POS) & 3;
  assert (TL_IN_REMAINING >= pad);
  tl_fetch_raw_data (&t, pad);
  if (t) {
    tl_fetch_set_error ("Padding with non-zeroes", TL_ERROR_SYNTAX);
    return -1;
  }
  return pad;  
}

static inline int tl_fetch_string_data (char *buf, int len) {
  if (tl_fetch_check (len) < 0) {
    return -1;
  }
  tl_fetch_raw_data (buf, len);
  if (tl_fetch_pad () < 0) {
    return -1;
  }
  return len;
}

static inline int tl_fetch_skip_string_data (int len) {
  if (tl_fetch_check (len) < 0) {
    return -1;
  }
  tl_fetch_skip_raw_data (len);
  if (tl_fetch_pad () < 0) {
    return -1;
  }
  return len;
}

static inline int tl_fetch_string (char *buf, int max_len) {
  int l = tl_fetch_string_len (max_len);
  if (l < 0) {
    return -1;
  }
  if (tl_fetch_string_data (buf, l) < 0) {
    return -1;
  }
  return l;
}

static inline int tl_fetch_string0 (char *buf, int max_len) {
  int l = tl_fetch_string_len (max_len);
  if (l < 0) {
    return -1;
  }
  if (tl_fetch_string_data (buf, l) < 0) {
    return -1;
  }
  buf[l] = 0;
  return l;
}

static inline int tl_fetch_error (void) {
  return TL_ERROR != 0;
}

static inline int tl_fetch_check_eof (void) {
  return !TL_IN_REMAINING;
}

static inline int tl_fetch_end (void) {
  if (TL_IN_REMAINING) {
    tl_fetch_set_error_format (TL_ERROR_EXTRA_DATA, "extra %d bytes after query", TL_IN_REMAINING);
    return -1;
  }
  return 1;
}

static inline int tl_fetch_check_str_end (int size) {
  if (TL_IN_REMAINING != size + ((-size - TL_IN_POS) & 3)) {
    tl_fetch_set_error_format (TL_ERROR_EXTRA_DATA, "extra %d bytes after query", TL_IN_REMAINING - size - ((-size - TL_IN_POS) & 3));    
    return -1;
  }
  return 1;
}

static inline int tl_fetch_unread (void) {
  return TL_IN_REMAINING;
}

static inline int tl_fetch_attempt_num (void) {
  return TL_ATTEMPT_NUM;
}

static inline int tl_fetch_skip (int len) {
  if (tl_fetch_check (len) < 0) {
    return -1;
  }
  tl_fetch_skip_raw_data (len);
  return len;
}

static inline int tl_fetch_move (int offset) {
  if (tl_fetch_check (offset) < 0) {
    return -1;
  }
  TL_IN_METHODS->fetch_move (offset);
  TL_IN_POS += offset;
  TL_IN_REMAINING -= offset;
  return offset;
}

static inline int tl_store_check (int size) {
  if (TL_OUT_TYPE == tl_type_none) { return -1; }
  if (TL_OUT_REMAINING < size) { return -1; }
  return 0;
}

static inline void _tl_store_raw_data (const void *buf, int len) {
  TL_OUT_METHODS->store_raw_data (buf, len);
  TL_OUT_POS += len;
  TL_OUT_REMAINING -= len;
}

static inline void *tl_store_get_ptr (int size) {
  assert (tl_store_check (size) >= 0);
  if (!size) { return 0; }
  assert (size >= 0);
  void *x = TL_OUT_METHODS->store_get_ptr (size);
  TL_OUT_POS += size;
  TL_OUT_REMAINING -= size;
  return x;
}

static inline void *tl_store_get_prepend_ptr (int size) {
  assert (tl_store_check (size) >= 0);
  if (!size) { return 0; }
  assert (size >= 0);
  void *x = TL_OUT_METHODS->store_get_prepend_ptr (size);
  TL_OUT_POS += size;
  TL_OUT_REMAINING -= size;
  return x;
}

static inline int tl_store_int (int x) {
  assert (tl_store_check (4) >= 0);
  _tl_store_raw_data (&x, 4);
  return 0;
}

static inline int tl_store_long (long long x) {
  assert (tl_store_check (8) >= 0);
  _tl_store_raw_data (&x, 8);
  return 0;
}

static inline int tl_store_double (double x) {
  assert (tl_store_check (8) >= 0);
  _tl_store_raw_data (&x, 8);
  return 0;
}

static inline int tl_store_string_len (int len) {
  assert (tl_store_check (4) >= 0);
  assert (len >= 0);
  assert (len < (1 << 24));
  if (len < 254) {
    _tl_store_raw_data (&len, 1);
  } else {
    int x = 254;
    _tl_store_raw_data (&x, 1);
    _tl_store_raw_data (&len, 3);
  }
  return 0;
}

static inline int tl_store_pad (void) {
  assert (tl_store_check (0) >= 0);
  int x = 0;
  int pad = (-TL_OUT_POS) & 3;
  _tl_store_raw_data (&x, pad);
  return 0;
}

static inline int tl_store_string_data (const char *s, int len) {
  assert (tl_store_check (len) >= 0);
  _tl_store_raw_data (s, len);
  tl_store_pad ();
  return 0;
}

#define tl_store_raw_data(s,len) tl_store_string_data((char *)(s), len)
static inline int tl_store_string (const char *s, int len) {
  tl_store_string_len (len);
  tl_store_string_data (s, len);
  return 0;
}

static inline int tl_store_string0 (const char *s) {
  return tl_store_string (s, strlen (s));
}

static inline int tl_store_clear (void) {
  assert (TL_OUT);
  TL_OUT_METHODS->store_clear ();
  TL_OUT = 0;
  TL_OUT_TYPE = tl_type_none;
  return 0; 
}

static inline int tl_store_clean (void) {
  assert (TL_OUT);
  TL_OUT_METHODS->store_read_back (TL_OUT_POS);
  TL_OUT_REMAINING += TL_OUT_POS;
  TL_OUT_POS = 0;
  return 0; 
}

static inline int tl_store_read_back_nondestruct (char *buf, int size) {
  assert (size <= TL_OUT_POS);
  TL_OUT_METHODS->store_read_back_nondestruct (buf, size);
  return size;
}

static inline int tl_store_end (void) {
  return tl_store_end_ext (RPC_REQ_RESULT);
}

static inline int tl_copy_through (int len, int advance) {
  if (!TL_COPY_THROUGH || tl_fetch_check (len) < 0 || tl_store_check (len) < 0) {
    return -1;
  }
  TL_COPY_THROUGH (len, advance);
  if (advance) {
    TL_IN_POS += len;
    TL_IN_REMAINING -= len;
  }
  TL_OUT_POS += len;
  TL_OUT_REMAINING -= len;
  return len;
}

static inline int tl_fetch_int_range (int min, int max) {
  int x = tl_fetch_int ();
  if (x < min || x > max) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Expected int32 in range [%d,%d], %d presented", min, max, x);
  }
  return x;
}

static inline int tl_fetch_positive_int (void) {
  return tl_fetch_int_range (1, 0x7fffffff);
}

static inline int tl_fetch_nonnegative_int (void) {
  return tl_fetch_int_range (0, 0x7fffffff);
}

static inline long long tl_fetch_long_range (long long min, long long max) {
  long long x = tl_fetch_long ();
  if (x < min || x > max) {
    tl_fetch_set_error_format (TL_ERROR_VALUE_NOT_IN_RANGE, "Expected int64 in range [%lld,%lld], %lld presented", min, max, x);
  }
  return x;
}

static inline long long tl_fetch_positive_long (void) {
  return tl_fetch_long_range (1, 0x7fffffffffffffffll);
}

static inline long long tl_fetch_nonnegative_long (void) {
  return tl_fetch_long_range (0, 0x7fffffffffffffffll);
}

#define TL_PARSE_FUN_EX(tname,fname,dname,...) \
static struct tl_act_extra *fname (__VA_ARGS__) { \
  struct tl_act_extra *extra = tl_act_extra_init (stats_buff, sizeof (tname), dname); \
  tname *e __attribute__ ((unused)); \
  e = (void *)extra->extra; 

#define TL_PARSE_FUN(name,...) TL_PARSE_FUN_EX(struct tl_ ## name,tl_ ## name,tl_do_ ## name,__VA_ARGS__)

#define TL_PARSE_FUN_END \
  tl_fetch_end (); \
  if (tl_fetch_error ()) { \
    return 0; \
  } \
  return extra; \
}

/* ${engine}-interface-structures.h must contain #pragma pack(push,4) for use TL_DEFAULT_PARSE_FUN macro */
#define TL_DEFAULT_PARSE_FUN(name) \
  TL_PARSE_FUN(name, void) \
  tl_fetch_raw_data(e, sizeof(*e)); \
  TL_PARSE_FUN_END

#define TL_DO_FUN_EX(tname,dname) \
  static int dname (struct tl_act_extra *extra) { \
    tname *e = (void *)extra->extra;

#define TL_DO_FUN(name) TL_DO_FUN_EX(struct tl_ ## name __attribute__ ((unused)), tl_do_ ## name);

#define TL_DO_FUN_END \
  return 0; \
}

#endif
