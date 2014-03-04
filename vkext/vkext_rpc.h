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

#ifndef __VKEXT_RPC_H__
#define __VKEXT_RPC_H__

#include <php.h>
#include <php_ini.h>

#define RPC_BUF_SIZE (1 << 12)
#define RPC_SERVER_MAGIC 0x8940303d
#define RPC_BUFFER_MAGIC 0x8fa0da0c
#define RPC_MAX_QUERY_LEN (1 << 20)

//#define RPC_INVOKE_REQ 0x2374df3d
#define RPC_INVOKE_REQ  0x2374df3d
#define RPC_REQ_RUNNING 0x346d5efa
#define RPC_REQ_ERROR 0x7ae432f5
//#define RPC_REQ_RESULT 0x63aeda4e
#define RPC_REQ_RESULT 0x63aeda4e

#define RPC_PING 0x5730a2df
#define RPC_PONG 0x8430eaa7

#define RPC_SKIP 0

#define RPC_OUT_BUF_SIZE (1 << 14)
#define RPC_IN_BUF_SIZE (1 << 14)
#define RPC_IN_BUF_FREE_SPACE (1 << 10) 

#define TL_RPC_DEST_ACTOR 0x7568aabd
#define TL_RPC_DEST_ACTOR_FLAGS 0xf0a5acf7

#define RPC_MAX_QUERIES (1 << 18)
#define RPC_QUERIES_MASK (RPC_MAX_QUERIES - 1)

#define RPC_DEFAULT_QUERY_TIMEOUT 0.3
#define RPC_DEFAULT_OP_TIMEOUT 0.1

#define PING_TIMEOUT 0.1
#if defined(__i386__)

static __inline__ unsigned long long rdtsc(void)
{
  unsigned long long int x;
     __asm__ volatile ("rdtsc" : "=A" (x));
     return x;
}
#elif defined(__x86_64__)

static __inline__ unsigned long long rdtsc(void)
{
  unsigned hi, lo;
  __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
  return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}

#endif


enum rpc_status {
  rpc_status_disconnected,
  rpc_status_failed,
  rpc_status_connected
}; 

enum query_status {
  query_status_sent,
  query_status_running,
  query_status_receiving,
  query_status_error,
  query_status_ok
};

enum parse_status {
  parse_status_expecting_query,
  parse_status_reading_query,
  parse_status_skipping_query
};

struct rpc_buffer {
  int magic;
  char *sptr;
  char *eptr;
  char *rptr;
  char *wptr;
};

struct rpc_server {
  int magic;
  unsigned host;
  unsigned short port;
  char *error; /* last error message */
  double timeout; 
  long failed;
  double retry_interval;
  long long generation;
  enum rpc_status status;
  int errnum; /* last error code */
  int packet_num;
  int inbound_packet_num;
  int fd;
  int sfd;
  char out_buf[RPC_OUT_BUF_SIZE];
  char *out_rptr;
  char *out_wptr;
  int out_bytes;
  char in_buf[RPC_IN_BUF_SIZE];
  int in_bytes;
  int in_pos;

  enum parse_status parse_status;
  char *parse_buf;
  int parse_len;
  int parse_pos;
  int parse_crc32;
  int parse_real_crc32;
  int parse_op;
  long long parse_qid;
  double last_received_time;
};

struct rpc_server_collection {
  unsigned host;
  int port;
  int num;
  struct rpc_server **servers;
};

struct rpc_connection {
  int fd;
  struct rpc_server_collection *servers;
  long long default_actor_id;
  double default_query_timeout;
};

struct rpc_query {
  long long qid;
  long long queue_id;
  double start_time;
  double timeout;
  char *answer;
  int answer_len;
  enum query_status status;
  void *extra;
  void (*extra_free)(struct rpc_query *);
};

struct rpc_queue {
  long long qid;
  double timeout;
  int remaining;
  struct tree_qid *completed;
};


extern int fetch_extra;
struct rpc_queue *rpc_queue_get (long long id);
struct rpc_server *rpc_server_get (int fd);
void update_precise_now ();
extern double precise_now;

int do_rpc_fetch_int (char **error);
long long do_rpc_fetch_long (char **error);
double do_rpc_fetch_double (char **error);
int do_rpc_fetch_string (char **value);
int do_rpc_fetch_eof (char **error);
int do_rpc_fetch_get_pos (char **error);
int do_rpc_fetch_set_pos (int pos, char **error);

struct rpc_query *rpc_query_get (long long qid);
//struct rpc_server *do_new_rpc_connection (unsigned host, int port, double timeout, char **error, int *errnum);
//long long do_rpc_send_noflush (struct rpc_server *server, double timeout);
struct rpc_connection *do_new_rpc_connection (unsigned host, int port, int num, long long default_actor_id, double default_query_timeout, double connect_timeout, double retry_timeout, char **error, int *errnum);
struct rpc_query *do_rpc_send_noflush (struct rpc_connection *c, double timeout);
int do_rpc_flush_server (struct rpc_server *server, double timeout);
int do_rpc_flush (double timeout);

int do_rpc_get_and_parse (long long qid, double timeout);
int do_rpc_get (long long qid, double timeout, char **value);
long long do_rpc_get_any_qid (double timeout);
void do_rpc_parse (const char *s, int len);

void php_rpc_work (INTERNAL_FUNCTION_PARAMETERS);

long long do_rpc_queue_create (int size, long long *arr);
int do_rpc_queue_empty (struct rpc_queue *Q);
long long do_rpc_queue_next (struct rpc_queue *Q, double timeout);


void php_new_rpc_connection (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_clean (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_send (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_send_noflush (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_flush (INTERNAL_FUNCTION_PARAMETERS);

void php_rpc_fetch_int (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_fetch_long (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_fetch_double (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_fetch_string (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_fetch_end (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_fetch_eof (INTERNAL_FUNCTION_PARAMETERS);

void php_rpc_store_int (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_store_long (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_store_string (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_store_double (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_store_many (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_store_header (INTERNAL_FUNCTION_PARAMETERS);

void php_rpc_get_and_parse (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_get (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_get_any_qid (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_parse (INTERNAL_FUNCTION_PARAMETERS);

void php_rpc_queue_create (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_queue_empty (INTERNAL_FUNCTION_PARAMETERS);
void php_rpc_queue_next (INTERNAL_FUNCTION_PARAMETERS);

void php_vk_prepare_stats (INTERNAL_FUNCTION_PARAMETERS);
void php_tl_config_load (INTERNAL_FUNCTION_PARAMETERS);
void php_tl_config_load_file (INTERNAL_FUNCTION_PARAMETERS);

void rpc_on_minit (int module_number);
void rpc_on_rinit (int module_number);
void rpc_on_rshutdown (int module_number);

int do_rpc_lookup_int (void);

struct rpc_connection *rpc_connection_get (int fd);

char *parse_zend_string (zval **z, int *l);
long long parse_zend_long (zval **z);
double parse_zend_double (zval **z);

extern char *global_error;
extern int global_errnum;
#define RPC_DEFAULT_TIMEOUT 1000

/* retry failed server after x seconds */
#define RPC_DEFAULT_RETRY_TIMEOUT 0.1

#define RPC_CHUNK_SIZE 64 


#define RPC_NONCE 0x7acb87aa
#define RPC_HANDSHAKE 0x7682eef5
#define RPC_HANDSHAKE_ERROR 0x6a27beda

#define GLOBAL_GENERATION 0
struct rpc_nonce {
  int key_select;        /* least significant 32 bits of key to use */
  int crypto_schema;     /* 0 = NONE, 1 = AES */
  int crypto_ts;
  char crypto_nonce[16];
};

struct process_id {
  unsigned ip;
  short port;
  short pid;
  int utime;
};

extern struct process_id PID;

struct rpc_handshake {
  int flags;
  struct process_id sender_pid;
  struct process_id peer_pid;
};

struct rpc_handshake_error {
  int error_code;
  struct process_id sender_pid;
};

#define DECLARE_STAT(x) long long x ## _cnt; long long x ## _ticks; double x ## _time;
#define TACT_SPEED (1e-6 / 2266)
#define PRINT_STAT(x) "%20s %20lld %20lld %10.6f %10.6f %lld\n", #x, stats.x ## _cnt, stats.x ## _ticks, stats.x ## _ticks * TACT_SPEED, stats.x ## _time, (long long)(stats.x ## _ticks / (stats.x ## _cnt ? stats.x ## _cnt : 1))

struct stats {
  DECLARE_STAT(utime);
  DECLARE_STAT(write);
  DECLARE_STAT(read);
  DECLARE_STAT(force_read);
  DECLARE_STAT(recv);
  DECLARE_STAT(send);
  DECLARE_STAT(poll);
  DECLARE_STAT(read_in);
  DECLARE_STAT(write_out);
  DECLARE_STAT(rpc_read);
  DECLARE_STAT(rpc_write);
  DECLARE_STAT(rpc_write_handshake);
  DECLARE_STAT(rpc_send);
  DECLARE_STAT(rpc_flush);
  DECLARE_STAT(rpc_get_answer);
  DECLARE_STAT(rpc_get_and_parse);
  DECLARE_STAT(rpc_get);
  DECLARE_STAT(rpc_poll);
  DECLARE_STAT(realloc);
  DECLARE_STAT(emalloc);
  DECLARE_STAT(tmp);
  DECLARE_STAT(rpc_work);
  DECLARE_STAT(rpc_sock_connect);
  DECLARE_STAT(store);
  DECLARE_STAT(fetch);
  DECLARE_STAT(crc32);
  DECLARE_STAT(tree_insert);
  DECLARE_STAT(total);
  DECLARE_STAT(malloc);
  DECLARE_STAT(parse);
  DECLARE_STAT(precise_now_updates);
  DECLARE_STAT(connect);
  DECLARE_STAT(get_field);
  DECLARE_STAT(set_field);
  DECLARE_STAT(minit);
  long long malloc;
  long long emalloc;
  long long pmalloc;
  long long rmalloc;
};
extern struct stats stats;

#ifdef __LP64__
#define VV_RETURN_LONG(value) RETURN_LONG(value)
#else 
#define VV_RETURN_LONG(value) static char ___buf[32]; (void) sprintf (___buf, "%lld", value); RETURN_STRING (___buf, 1);
#endif

char *vv_strdup (const char *s, int len);
char *vv_estrdup (const char *s, int len);

#define VV_STR_RETURN_NOD(_value,_value_len) \
    return_value->type = IS_STRING;  \
    return_value->value.str.len = _value_len;  \
    return_value->value.str.val = _value;  \

#define VV_STR_RETURN_DUP(_value,_value_len) \
    return_value->type = IS_STRING;  \
    return_value->value.str.len = _value_len;  \
    return_value->value.str.val = vv_estrdup (_value, _value_len);  \


#define VV_STR_RETURN(value,value_len,dup) if (dup) { VV_STR_RETURN_DUP(value,value_len);} else { VV_STR_RETURN_NOD(value,value_len);}



//#define DEBUG_TIMER
//#define DEBUG_TICKS
//#define DEBUG_CNT
#define DEBUG_MEMORY
//#define PRINT_DEBUG_INFO

#ifdef DEBUG_TIMER
  #ifdef DEBUG_TICKS
    #define START_TIMER(x) stats.x ## _time -= get_utime_monotonic (); stats.x ## _ticks -= rdtsc ();
    #define END_TIMER(x) stats.x ## _time += get_utime_monotonic (); stats.x ## _ticks += rdtsc () + 1;
  #else
    #define START_TIMER(x) stats.x ## _time -= get_utime_monotonic (); 
    #define END_TIMER(x) stats.x ## _time += get_utime_monotonic (); 
  #endif
#else
  #ifdef DEBUG_TICKS
    #define START_TIMER(x) stats.x ## _ticks -= rdtsc ();
    #define END_TIMER(x) stats.x ## _ticks += rdtsc () + 1;
  #else
    #define START_TIMER(x) ;
    #define END_TIMER(x) ;
  #endif
#endif

#ifdef DEBUG_TICKS
  #define START_TICKS(x) stats.x ## _ticks -= rdtsc ();
  #define END_TICKS(x) stats.x ## _ticks += rdtsc () + 1;
#else
  #define START_TICKS(x) ;
  #define END_TICKS(x) ;
#endif

#ifdef DEBUG_CNT
#define ADD_CNT(x) stats.x ## _cnt ++;
#else
#define ADD_CNT(x) ;
#endif

#ifdef DEBUG_MEMORY
#define ADD_MALLOC(x) stats.malloc += x;
#define ADD_EMALLOC(x) stats.emalloc += x;
#define ADD_PMALLOC(x) stats.pmalloc += x;
#define ADD_RMALLOC(x) stats.rmalloc += x;
#define ADD_FREE(x) stats.malloc -= x;
#define ADD_EFREE(x) stats.emalloc -= x;
#define ADD_PFREE(x) stats.pmalloc -= x;
#define ADD_RFREE(x) stats.rmalloc -= x;
#define ADD_REALLOC(x,y) stats.malloc += (y - x);
#define ADD_EREALLOC(x,y) stats.emalloc += (y - x);
#define ADD_PREALLOC(x,y) stats.pmalloc += (y - x);
#define ADD_RREALLOC(x,y) stats.rmalloc += (y - x);
#else
#define ADD_MALLOC(x) ;
#define ADD_EMALLOC(x) ;
#define ADD_PMALLOC(x) ;
#define ADD_RMALLOC(x) ;
#define ADD_FREE(x) ;
#define ADD_EFREE(x) ;
#define ADD_PFREE(x) ;
#define ADD_RFREE(x) ;
#define ADD_REALLOC(x,y) ;
#define ADD_EREALLOC(x,y) ;
#define ADD_PREALLOC(x,y) ;
#define ADD_RREALLOC(x,y) ;
#endif
#endif

