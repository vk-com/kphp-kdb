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

    Copyright 2009-2013 Vkontakte Ltd
              2008-2013 Nikolai Durov
              2008-2013 Andrei Lopatin
*/

#ifndef __VK_NET_CONNECTIONS_H__
#define __VK_NET_CONNECTIONS_H__

#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-msg.h"

#define	BACKLOG	8192
#define MAX_CONNECTIONS	65536
#define MAX_TARGETS	65536
#define PRIME_TARGETS	99961

#define MAX_NET_RES	(1L << 16)

#define BUFF_SIZE	2048

#define	CONN_CUSTOM_DATA_BYTES	128

#define	NEED_MORE_BYTES	(~(-1 << 31))
#define	SKIP_ALL_BYTES	(-1 << 31)


/* for connection flags */
#define	C_WANTRD	1
#define	C_WANTWR	2
#define	C_WANTRW	(C_WANTRD | C_WANTWR)
#define	C_INCONN	4
#define	C_ERROR		8
#define	C_NORD		0x10
#define	C_NOWR		0x20
#define	C_NORW		(C_NORD | C_NOWR)
#define	C_INQUERY	0x40
#define	C_FAILED	0x80
#define	C_ALARM		0x100
#define C_AIO		0x200
#define C_INTIMEOUT	0x400
#define	C_STOPREAD	0x800
#define	C_REPARSE	0x1000
#define C_DFLUSH	0x2000
#define	C_IPV6		0x4000
#define	C_FAKE		0x8000
#define C_SPECIAL	0x10000
#define	C_NOQACK	0x20000
#define	C_RAWMSG	0x40000
#define	C_CRYPTOIN	0x100000
#define	C_CRYPTOOUT	0x200000

#define C_PERMANENT (C_IPV6 | C_RAWMSG)
/* for connection status */
enum {
  conn_none,		// closed/uninitialized
  conn_expect_query,	// wait for inbound query, no query bytes read
  conn_reading_query,	// query partially read, wait for more bytes
  conn_running,		// running a query, almost never appears
  conn_wait_aio,	// waiting for aio completion (while running a query)
  conn_wait_net,	// waiting for network (while running a query)
  conn_connecting,	// outbound socket, connecting to the other side
  conn_ready,		// outbound socket ready
  conn_sending_query,	// sending a query to outbound socket, almost never appears
  conn_wait_answer,	// waiting for an answer from an outbound socket
  conn_reading_answer,	// answer partially read, waiting for more bytes
  conn_error,		// connection in bad state (it will be probably closed)
  conn_wait_timeout,	// connection was in wait_aio/wait_net states, and it timed out
  conn_listen,		// listening for inbound connections
  conn_write_close,	// write all output buffer, then close; don't read input
  conn_total_states	// total number of connection states
};

/* for connection basic_type */
enum {
  ct_none,		// no connection (closed)
  ct_listen,		// listening socket
  ct_inbound,		// inbound connection
  ct_outbound,		// outbound connection
  ct_aio,		// used for aio file operations ( net-aio.h )
  ct_pipe,  // uset for pipe reading
};

/* for connection->ready of outbound connections */
enum {
  cr_notyet,		// not ready yet (e.g. logging in)
  cr_ok,		// working
  cr_stopped,		// stopped (don't send more queries)
  cr_busy,		// busy (sending queries not allowed by protocol)
  cr_failed		// failed (possibly timed out)
};


struct connection;

/* connection function table */

#define	CONN_FUNC_MAGIC	0x11ef55aa

typedef struct conn_functions {
  int magic;
  int flags;					/* may contain for example C_RAWMSG; (partially) inherited by inbound/outbound connections */
  char *title;
  int (*accept)(struct connection *c);		 /* invoked for listen/accept connections of this type */
  int (*init_accepted)(struct connection *c);	 /* initialize a new accept()'ed connection */
  int (*run)(struct connection *c);		 /* invoked when an event related to connection of this type occurs */
  int (*reader)(struct connection *c);		 /* invoked from run() for reading network data */
  int (*writer)(struct connection *c);		 /* invoked from run() for writing data */
  int (*close)(struct connection *c, int who);	 /* invoked from run() whenever we need to close connection */
  int (*free_buffers)(struct connection *c);	 /* invoked from close() to free all buffers */
  int (*parse_execute)(struct connection *c);	 /* invoked from reader() for parsing and executing one query */
  int (*init_outbound)(struct connection *c);	 /* initializes newly created outbound connection */
  int (*connected)(struct connection *c);	 /* invoked from run() when outbound connection is established */
  int (*wakeup)(struct connection *c);		 /* invoked from run() when pending_queries == 0 */
  int (*alarm)(struct connection *c);		 /* invoked when timer is out */
  int (*ready_to_write)(struct connection *c);   /* invoked from server_writer when Out.total_bytes crosses write_low_watermark ("greater or equal" -> "less") */
  int (*check_ready)(struct connection *c);	 /* updates conn->ready if necessary and returns it */
  int (*wakeup_aio)(struct connection *c, int r);/* invoked from net_aio.c::check_aio_completion when aio read operation is complete */
  int (*flush)(struct connection *c);		 /* generates necessary padding and writes as much bytes as possible */
  int (*crypto_init)(struct connection *c, void *key_data, int key_data_len);  /* < 0 = error */
  int (*crypto_free)(struct connection *c);
  int (*crypto_encrypt_output)(struct connection *c);  /* 0 = all ok, >0 = so much more bytes needed to encrypt last block */
  int (*crypto_decrypt_input)(struct connection *c);   /* 0 = all ok, >0 = so much more bytes needed to decrypt last block */
  int (*crypto_needed_output_bytes)(struct connection *c);	/* returns # of bytes needed to complete last output block */
} conn_type_t;

struct conn_target {
  int min_connections;
  int max_connections;
  struct connection *first_conn, *last_conn;
  struct conn_query *first_query, *last_query;
  conn_type_t *type;
  void *extra;
  struct in_addr target;
  unsigned char target_ipv6[16];
  int port;
  int active_outbound_connections, outbound_connections;
  int ready_outbound_connections;
  double next_reconnect, reconnect_timeout, next_reconnect_timeout;
  int custom_field;
  int refcnt;
};

#define	CQUERY_FUNC_MAGIC	0xDEADBEEF
struct conn_query;


typedef struct conn_query_functions {
  int magic;
  char *title;
  int (*parse_execute)(struct connection *c);
  int (*close)(struct conn_query *q);
  int (*wakeup)(struct conn_query *q);
  int (*complete)(struct conn_query *q);
} conn_query_type_t;

struct conn_query {
  int custom_type;
  int req_generation;
  struct connection *outbound;
  struct connection *requester;
  struct conn_query *next, *prev;
  conn_query_type_t *cq_type;
  void *extra;
  double start_time;
  event_timer_t timer;
};

struct connection {
  int fd;
  int flags;
  struct connection *next, *prev;
  struct conn_query *first_query, *last_query;
  conn_type_t *type;
  event_t *ev;
  void *extra;
  struct conn_target *target;
  int basic_type;
  int status;
  int error;
  int generation;
  int unread_res_bytes;
  int skip_bytes;
  int pending_queries;
  int queries_ok;
  char custom_data[CONN_CUSTOM_DATA_BYTES];
  unsigned our_ip, remote_ip;
  unsigned our_port, remote_port;
  unsigned char our_ipv6[16], remote_ipv6[16];
  double query_start_time;
  double last_query_time;
  double last_query_sent_time;
  double last_response_time;
  double last_query_timeout;
  event_timer_t timer;
  int unreliability;
  int ready;
  int parse_state;
  int write_low_watermark;
  void *crypto;
  int listening, listening_generation;
  int window_clamp;
  struct raw_message in_u, in, out, out_p;
  nb_iterator_t Q;
  netbuffer_t *Tmp, In, Out;
  char in_buff[BUFF_SIZE];
  char out_buff[BUFF_SIZE];
};

extern struct connection Connections[MAX_CONNECTIONS];
extern int max_connection;
extern int active_connections;
extern int active_special_connections, max_special_connections;
extern int outbound_connections, active_outbound_connections, ready_outbound_connections;
extern int conn_generation;
extern int ready_targets;
extern long long total_failed_connections, total_connect_failures;
extern long long rpc_queries_received, rpc_queries_ok, rpc_queries_error, 
                 rpc_answers_received, rpc_answers_ok, rpc_answers_error, rpc_answers_timedout,
                 rpc_sent_queries, rpc_sent_answers, rpc_sent_errors;

int init_listening_connection (int fd, conn_type_t *type, void *extra);
int init_listening_tcpv6_connection (int fd, conn_type_t *type, void *extra, int mode);

/* default methods */
int accept_new_connections (struct connection *c);
int server_read_write (struct connection *c);
int server_reader (struct connection *c);
int server_writer (struct connection *c);
int server_noop (struct connection *c);
int server_failed (struct connection *c);
int server_close_connection (struct connection *c, int who);
int client_close_connection (struct connection *c, int who);
int free_connection_buffers (struct connection *c);
int default_parse_execute (struct connection *c); // DO NOT USE!!!
int client_init_outbound (struct connection *c);
int server_check_ready (struct connection *c);

int conn_timer_wakeup_gateway (event_timer_t *et);
int server_read_write_gateway (int fd, void *data, event_t *ev);
int check_conn_functions (conn_type_t *type);

/* useful functions */

static inline char *show_ip46 (unsigned ip, const unsigned char ipv6[16]) { return ip ? show_ip (ip) : show_ipv6 (ipv6); }
static inline char *show_our_ip (struct connection *c) { return show_ip46 (c->our_ip, c->our_ipv6); }
static inline char *show_remote_ip (struct connection *c) { return show_ip46 (c->remote_ip, c->remote_ipv6); }


int prepare_stats (struct connection *c, char *buff, int buff_size);

int fail_connection (struct connection *c, int err);
int flush_connection_output (struct connection *c);
int flush_later (struct connection *c);

int set_connection_timeout (struct connection *c, double timeout);
int clear_connection_timeout (struct connection *c);

void dump_connection_buffers (struct connection *c);

int conn_rerun_later (struct connection *c, int ts_delta);

/* target handling */

extern struct conn_target Targets[MAX_TARGETS];
extern int allocated_targets;

struct conn_target **find_target (struct in_addr ad, int port, conn_type_t *target);
struct conn_target *create_target (struct conn_target *source, int *was_created);
int destroy_target (struct conn_target *S);

int create_new_connections (struct conn_target *S);
int create_all_outbound_connections (void);

int force_clear_connection (struct connection *c);

/* connection query queues */
int insert_conn_query (struct conn_query *q);
int push_conn_query (struct conn_query *q);
int insert_conn_query_into_list (struct conn_query *q, struct conn_query *h);
int push_conn_query_into_list (struct conn_query *q, struct conn_query *h);
int delete_conn_query (struct conn_query *q);
int delete_conn_query_from_requester (struct conn_query *q);

extern long long netw_queries, netw_update_queries;
int free_tmp_buffers (struct connection *c);

static inline int is_ipv6_localhost (unsigned char ipv6[16]) {
  return !*(long long *)ipv6 && ((long long *)ipv6)[1] == 1LL << 56;
}

#endif
