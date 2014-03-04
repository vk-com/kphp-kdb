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

#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include <assert.h>
#include "vkext_rpc.h"
#include "vkext.h"
#include "crc32.h"
#include <php_network.h>
#include "../vv/vv-tree.h"
#include <poll.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <fcntl.h>
#include "vkext_rpc_include.h"
#include "vkext_schema_memcache.h"

#if HAVE_ASSERT_H
#undef NDEBUG
#include <assert.h>
#endif /* HAVE_ASSERT_H */
struct process_id PID;
//int le_rpc;
//static zend_class_entry *rpc_class_entry_ptr;
//int fetch_extra;
int last_server_fd = 0;
int servers_size;
struct rpc_server **servers = 0;

struct pollfd *server_fds = 0;
int *server_fds_tmp = 0;

struct rpc_buffer *inbuf = 0;
struct rpc_buffer *outbuf = 0;

char *global_error = 0;
int global_errnum = 0;

long long last_qid;
double precise_now;
long long precise_now_ticks;

long long total_working_qid;
long long first_qid;

long long last_queue_id;
int last_connection_fd;
#define CHECK_CRC32
#define SEND_CRC32

long long global_generation;

struct stats stats;

double ping_timeout = PING_TIMEOUT;

#define IP_PRINT_STR "%u.%u.%u.%u"
#define IP_TO_PRINT(a) ((unsigned)(a)) >> 24, (((unsigned)(a)) >> 16) & 0xff, (((unsigned)(a)) >> 8) & 0xff, (((unsigned)(a))) & 0xff

#define rpc_server_cmp(a,b) (memcmp (&((a)->host), &((b)->host), 6))
DEFINE_TREE_STDNOZ_ALLOC (server_collection,struct rpc_server_collection *,rpc_server_cmp,int,std_int_compare)
tree_server_collection_t *server_collection_tree;

DEFINE_TREE_STDNOZ_ALLOC (queue,struct rpc_queue *,std_ll_ptr_compare,int,std_int_compare)
tree_queue_t *queue_tree;

#define rpc_connection_cmp(a,b) ((a)->fd - (b)->fd)
DEFINE_TREE_STDNOZ_ALLOC (connection,struct rpc_connection *,rpc_connection_cmp,int,std_int_compare)
tree_connection_t *rpc_connection_tree;

//#define rpc_query_cmp(a,b) ((a)->qid - (b)->qid)
//DEFINE_TREE_STDNOZ_ALLOC (query,struct rpc_query *,rpc_query_cmp,int,std_int_compare)
//tree_query_t *query_tree;

DEFINE_TREE_STDNOZ_ALLOC (qid,long long,std_int_compare,int,std_int_compare)
tree_qid_t *query_completed;

struct rpc_query queries[RPC_MAX_QUERIES];
/*
static struct rpc_server *rpc_server_new (unsigned host_len, unsigned short port, double timeout, double retry_interval);
static void rpc_server_sleep(struct rpc_server *server);
static void rpc_server_free (struct rpc_server *server);
static void rpc_server_seterror (struct rpc_server *server, const char *error, int errnum);
static void rpc_server_disconnect (struct rpc_server *server);
static void rpc_server_deactivate (struct rpc_server *server);
static int rpc_server_failure (struct rpc_server *server);
*/
int rpc_make_handshake (struct rpc_server *server, double timeout);
//static int rpc_sock_write (int sfd, const char *data, int min_len, int max_len, double timeout);
static double get_double_time_since_epoch(void) __attribute__ ((unused));
inline static double get_utime_monotonic (void) __attribute__ ((unused));
static struct timeval _convert_timeout_to_ts (double t) __attribute__ ((unused));
static void rpc_server_seterror (struct rpc_server *server, const char *error, int errnum);
static int rpc_server_failure (struct rpc_server *server);
static void rpc_server_clean (struct rpc_server *server);

long error_verbosity;

int active_net_connections;
int net_connections_fails;
int finished_queries;
int errored_queries;
int timedout_queries;
long long total_queries;

#define rpc_query_cmp(a,b) ((a)->qid - (b)->qid)
#define rpc_query_hash(a) ((a)->qid)
DEFINE_HASH_STDNOZ_ALLOC(query,struct rpc_query *,rpc_query_cmp,rpc_query_hash)

void update_precise_now ();
inline static int get_ms_timeout (double timeout) {
  if (timeout == 0) {
    return 0;
  }
  if (timeout < 0 || timeout >= 1e100) {
    return -1;
  }
  update_precise_now ();
  timeout -= precise_now;
  if (timeout < 0) {
    return 0;
  } else {
    return (int)(timeout * 1000);
  }
}

void update_precise_now () {
  long long x = rdtsc ();
  if (x - precise_now_ticks > 1000000) {
    ADD_CNT (precise_now_updates);
    precise_now_ticks = x;
    precise_now = get_utime_monotonic ();
  } else {
    precise_now += 1e-6;
  }
}

struct rpc_connection *rpc_connection_get (int fd) {
  struct rpc_connection **T = tree_lookup_value_connection (rpc_connection_tree, (void *)&fd);
  return T ? *T : 0;
}

static int rpc_sock_connect (unsigned host, int port, double timeout) { /* {{{ */
  ADD_CNT (rpc_sock_connect);
  START_TIMER (rpc_sock_connect);
  int sfd;
  if ((sfd = socket (AF_INET, SOCK_STREAM, 0)) == -1) {
    END_TIMER (rpc_sock_connect);
    return -1;
  }
  int flags = 1;
  setsockopt (sfd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof (flags));
  setsockopt (sfd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof (flags));
  setsockopt (sfd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof (flags));
  struct sockaddr_in addr;
  
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = htonl (host);

  fcntl (sfd, F_SETFL, O_NONBLOCK);
  if (connect (sfd, (struct sockaddr *) &addr, sizeof (addr)) == -1) {
    if (errno != EINPROGRESS) {
      END_TIMER (rpc_sock_connect);
      close (sfd);
      return -1;
    }
  }

  struct pollfd s;
  s.fd = sfd;
  s.events = POLLOUT;

  int t = get_ms_timeout (timeout);

  if (poll (&s, 1, t) <= 0) {
    close (sfd);
    END_TIMER (rpc_sock_connect);
    return -1;
  }

  
  END_TIMER (rpc_sock_connect);
  return sfd;
}
/* }}} */

/* static int rpc_sock_write (int sfd, const char *data, int min_len, int max_len, double timeout) { {{{ 
  ADD_CNT (write);
  START_TIMER (write);
  assert (sfd >= 0);
  int r = 0;
  struct pollfd s;
  s.fd = sfd;
  s.events = POLLOUT;
  int t = 0; //get_ms_timeout (timeout);
  int first = 1;
  do {
    if (poll (&s, 1, t) <= 0) {
      if (first) {
        first = 0;
        continue;
      }
      END_TIMER (write);
      return r;
    }    
    first = 0;
    ADD_CNT (send);
    int x = send (sfd, data, max_len - r, MSG_DONTWAIT);
    if (x > 0) {
      data += x;
      r += x;
    }
    if (x < 0 || (errno && errno != EAGAIN && errno != EINPROGRESS)) {
      END_TIMER (write);
      return r;
    }
  } while (r < min_len && (t = get_ms_timeout (timeout)));
  END_TIMER (write);
  return r;
}
 }}} */

/* {{{ static int rpc_sock_write_try (int sfd, const char *data, int len) { 
  ADD_CNT (write);
  START_TIMER (write);
  assert (sfd >= 0);
  int r = write (sfd, data, len);
  END_TIMER (write);
  return r;  
} }}} */

/* static int rpc_sock_read (int sfd, char *data, int min_len, int max_len, double timeout) {  {{{ 
  ADD_CNT(read);
  START_TIMER (read);
  assert (sfd >= 0);
  int r = 0;
  struct pollfd s;
  s.fd = sfd;
  s.events = POLLIN;
  int t = 0; //get_ms_timeout (timeout);
  int first = 1;
  do {
    if (poll (&s, 1, t) <= 0) {
      if (first) {
        first = 0;
        continue;
      }
      END_TIMER (read);
      return r;
    }
    first = 0;
    ADD_CNT(recv);
    int x = recv (sfd, data, max_len - r, MSG_DONTWAIT);
    if (x > 0) {
      data += x;
      r += x;
    }
    if (x < 0 || (errno && errno != EAGAIN && errno != EINPROGRESS)) {
      END_TIMER (read);
      return r;
    }
  } while (r < min_len && (t = get_ms_timeout (timeout)));
  END_TIMER (read);
  return r;
}
 }}} */

static int rpc_sock_write (struct rpc_server *server, double timeout, char *buf, int buf_len, int min_bytes) { /* {{{ */
  ADD_CNT (write);
  START_TIMER (write);
  if (server->sfd < 0) {
    END_TIMER (write);
    return -1;
  }
  int r = 0;
  int first = 1;
  
  struct pollfd s;
  s.fd = server->sfd;
  s.events = POLLOUT;
  static struct iovec t[3];
  int ss, sf;
  if (server->out_bytes) {
    if (server->out_bytes != RPC_OUT_BUF_SIZE && server->out_rptr <= server->out_wptr) {
      ss = 1;
      t[1].iov_base = server->out_rptr;
      t[1].iov_len = server->out_wptr - server->out_rptr;
    } else {
      ss = 0;
      t[0].iov_base = server->out_rptr;
      t[0].iov_len = server->out_buf + RPC_OUT_BUF_SIZE - server->out_rptr;
      t[1].iov_base = server->out_buf;
      t[1].iov_len = server->out_wptr - server->out_buf;
    }
  } else {
    ss = 2;
  }
  if (buf && buf_len) {
    sf = 3;
    t[2].iov_base = buf;
    t[2].iov_len = buf_len;
  } else {
    sf = 2;
  }
  if (!(sf - ss)) {
    END_TIMER (write);
    return 0; 
  }
  int tt = 0;
  do {
    if (poll (&s, 1, tt) <= 0) {
      if (first) {
        first = 0;
        continue;
      }
      END_TIMER (write);
      return r;
    }
    first = 0;
    ADD_CNT (send);
    int x = writev (server->sfd, t + ss, sf - ss);
    if (x < 0) {
      rpc_server_seterror (server, strerror (errno), errno);
      rpc_server_failure (server);
      END_TIMER (write);
      return -1;
    }
    r += x;
    if (x && ss == 0) {
      if (x >= t[0].iov_len) {
        x -= t[0].iov_len;
        server->out_rptr = server->out_buf;
        server->out_bytes -= t[0].iov_len;
        ss ++;
      } else {
        t[0].iov_len -= x;
        t[0].iov_base += x;
        server->out_rptr += x;
        server->out_bytes -= x;
        x = 0;
      }
    }
    if (x && ss == 1) {
      if (x >= t[1].iov_len) {
        x -= t[1].iov_len;
        server->out_rptr = server->out_wptr = server->out_buf;
        server->out_bytes = 0;
        ss ++;
      } else {
        t[1].iov_len -= x;
        t[1].iov_base += x;
        server->out_rptr += x;
        server->out_bytes -= x;
        x = 0;
      }
    }
    if (x && ss == 2) {
      if (x >= t[2].iov_len) {
        assert (x == t[2].iov_len);
        // buf += t[2].iov_len;
        // ss ++;
        break;
      } else {
        // buf += x;
        // buf_len -= x;
        t[2].iov_len -= x;
        t[2].iov_base += x;
      }
    }
  } while (r < min_bytes && ss < sf && (tt = get_ms_timeout (timeout)));
  END_TIMER (write);
  return r;
}
/* }}} */

/* buffered write {{{ */



static int rpc_flush_out (struct rpc_server *server, double timeout) {
  if (server->out_bytes) {
    if (rpc_sock_write (server, timeout, 0, 0, server->out_bytes) < 0) { 
      return -1;
    }
  }
  return server->out_bytes;
}

static int rpc_flush_out_force (struct rpc_server *server, double timeout) {
  if (rpc_flush_out (server, timeout) > 0) {
    rpc_server_seterror (server, "Flush timeout", 0);
    rpc_server_failure (server);
    return -1;
  }
  return 0;
}
/*
static int rpc_flush_all (double timeout) {
  ADD_CNT (rpc_flush_all);
  START_TIMER (rpc_flush_all);
  int i;
  int cc = 0;
  int x = 0;
  for (i = 0; i < last_server_fd; i++) if (servers[i] && servers[i]->out_bytes) {
    int r = rpc_sock_write_try (servers[i]->sfd, servers[i]->out_buf + servers[i]->out_pos, servers[i]->out_bytes);
    if (r < 0) {
      rpc_server_failure (servers[i]);      
    } else {
      servers[i]->out_pos += r;
      servers[i]->out_bytes -= r;
      if (servers[i]->out_bytes) { x ++; }
    }
  }
  if (!x) { 
    END_TIMER (rpc_flush_all);
    return 0; 
  }
  for (i = 0; i < last_server_fd; i++) if (servers[i] && servers[i]->status == rpc_status_connected && servers[i]->out_bytes) {
    server_fds[cc].fd = servers[i]->sfd;
    server_fds[cc].events = POLLOUT;
    server_fds_tmp[cc] = i;
    cc ++;
  }
  assert (cc == x);
  int t = 0;
  do {
    ADD_CNT (poll);
    START_TIMER (poll);
    poll (server_fds, cc, t);
    END_TIMER (poll);
    for (i = 0; i < cc; i++) if (server_fds[i].revents & POLLOUT) {
      int r = rpc_sock_write_try (servers[server_fds_tmp[i]]->sfd, servers[i]->out_buf + servers[i]->out_pos, servers[i]->out_bytes);
      if (r < 0) {
        rpc_server_failure (servers[server_fds_tmp[i]]);      
        server_fds[i].events = 0;
        x --;
      } else {
        servers[server_fds_tmp[i]]->out_pos += r;
        servers[server_fds_tmp[i]]->out_bytes -= r;
        if (!servers[server_fds_tmp[i]]->out_bytes) { server_fds[i].events = 0; x--; }
      }
      if (!x) { return 0; }
    }
  } while ((t = get_ms_timeout (timeout)));
  END_TIMER (rpc_flush_all);
  return x;
}*/

/*static int rpc_write_out (struct rpc_server *server, const char *data, int len, double timeout) {
  START_TIMER(write_out);
  int _len = len;
  if (server->out_wptr >= server->out_rptr && server->out_bytes < RPC_OUT_BUF_SIZE) {
    int q = server->out_buf + RPC_OUT_BUF_SIZE - server->out_wptr;
    if (q < 

    memcpy (server->out_wptr, server->out_buf + RPC_OUT_BUF_SIZE
  }
  if (server->out_wptr < server->out_rptr) {
    if (server->out_wptr + len >= server->out_rptr) {
      memcpy (
    }
  }
  if (len + server->out_bytes + server->out_pos >= RPC_OUT_BUF_SIZE) {
    memcpy (server->out_buf + server->out_pos + server->out_bytes, data, RPC_OUT_BUF_SIZE - server->out_bytes - server->out_pos);
    len -= RPC_OUT_BUF_SIZE - server->out_bytes - server->out_pos;
    data += RPC_OUT_BUF_SIZE - server->out_bytes - server->out_pos;
    server->out_bytes = RPC_OUT_BUF_SIZE - server->out_pos;
    if (rpc_flush_out (server, timeout) < 0) {
      END_TIMER(write_out);
      return -1;
    }
  }
  if (len >= RPC_OUT_BUF_SIZE) {
    assert (!server->out_bytes);
    assert (!server->out_pos);
    if (rpc_sock_write (server->sfd, data, len, len, timeout) != len) {
      END_TIMER(write_out);
      return -1;
    }
  } else {
    assert (len + server->out_bytes + server->out_pos < RPC_OUT_BUF_SIZE);
    memcpy (server->out_buf + server->out_bytes + server->out_pos, data, len);
    server->out_bytes += len;    
  }
  END_TIMER(write_out);
  return _len;
}*/

static int rpc_write_out (struct rpc_server *server, char *data, int len, double timeout) {
  if (server->sfd < 0) {
    return -1;
  }
  if (!len) {
    return 0;
  }
  int _len = len;
  ADD_CNT (write_out);
  START_TIMER (write_out);  
  int x = server->out_bytes;
  if (x + len >= RPC_OUT_BUF_SIZE) {
    int r = rpc_sock_write (server, timeout, data, len, x + len - RPC_OUT_BUF_SIZE + 1024);
    if (r < 0) {
      END_TIMER (write_out);
      return -1;
    }
    if (r > x) {
      data += (r - x);
      len -= (r - x);
    }
    x = server->out_bytes;
    if (x + len >= RPC_OUT_BUF_SIZE) {
      rpc_server_seterror (server, "Write timeout", 0);
      rpc_server_failure (server);
      END_TIMER (write_out);
      return -1;
    }
  }
  server->out_bytes += len;
  if (server->out_rptr <= server->out_wptr) {
    if (server->out_wptr + len <= server->out_buf + RPC_OUT_BUF_SIZE) {
      memcpy (server->out_wptr, data, len);
      server->out_wptr += len;
      END_TIMER (write_out);
      return _len;
    } else {
      int q = server->out_buf + RPC_OUT_BUF_SIZE - server->out_wptr;
      memcpy (server->out_wptr, data, q);
      memcpy (server->out_buf, data + q, len - q);
      server->out_wptr = server->out_buf + (len - q);
      END_TIMER (write_out);
      return _len;
    }
  } else {
    memcpy (server->out_wptr, data, len);
    server->out_wptr += len;
    END_TIMER (write_out);
    return _len;
  }
}

/*static int rpc_read_in (struct rpc_server *server, char *data, int len, double timeout) {
  int __len = len;
  START_TIMER (read_in);
  if (len <= server->in_bytes) {
    memcpy (data, server->in_buf + server->in_pos, len);
    server->in_pos += len;
    server->in_bytes -= len;
    END_TIMER (read_in);
    return len;
  } else {
    memcpy (data, server->in_buf + server->in_pos, server->in_bytes);
    len -= server->in_bytes;
    data += server->in_bytes;
    server->in_bytes = 0;
    server->in_pos = 0;
    if (len >= RPC_IN_BUF_SIZE) {
      if (rpc_sock_read (server->sfd, data, len, len, timeout) == len) {
        END_TIMER (read_in);
        return __len;
      } else {
        END_TIMER (read_in);
        return -1;
      }
    }
    int t = rpc_sock_read (server->sfd, server->in_buf, len, RPC_IN_BUF_SIZE, timeout);
    if (t < 0) {
      END_TIMER (read_in);
      return -1;
    }
    server->in_bytes += t;
    if (len <= server->in_bytes) {
      memcpy (data, server->in_buf, len);
      server->in_pos = len;
      server->in_bytes -= len;
      if (!server->in_bytes) {
        server->in_pos = 0;
      }
      END_TIMER (read_in);
      return __len;
    } else {
      END_TIMER (read_in);
      return -1;
    }
  }
  return 0;
}*/

static int rpc_read_in (struct rpc_server *server, char *data, int len, double timeout) {
  ADD_CNT (read_in);
  START_TIMER (read_in);
  if (len <= server->in_bytes) {
    memcpy (data, server->in_buf + server->in_pos, len);
    server->in_pos += len;
    server->in_bytes -= len;
    END_TIMER (read_in);
    return len;
  } else {    
    memcpy (data, server->in_buf + server->in_pos, server->in_bytes);
    int t = server->in_bytes;
    server->in_pos = 0;
    server->in_bytes = 0;
    END_TIMER (read_in);
    return t;
  }
}
/* }}} */

static int get_ready_bytes (struct rpc_server *server, int n) { /* {{{ */
  if ((server->parse_status == parse_status_expecting_query && server->in_bytes >= n) || 
      (server->parse_status == parse_status_reading_query && server->parse_pos == server->parse_len + 4)) {
    return 0;
  }
  ADD_CNT(read);
  START_TIMER (read);
  assert (server->sfd >= 0);
  static struct iovec t[3];
  int pos;
  if (RPC_IN_BUF_SIZE - server->in_pos - server->in_bytes < RPC_IN_BUF_FREE_SPACE && server->in_pos > RPC_IN_BUF_FREE_SPACE) {
    memcpy (server->in_buf, server->in_buf + server->in_pos, server->in_bytes);
    server->in_pos = 0;
  }
  t[2].iov_len = RPC_IN_BUF_SIZE - server->in_pos - server->in_bytes;
  t[2].iov_base = server->in_buf + server->in_pos + server->in_bytes;
  if (server->parse_status == parse_status_expecting_query || server->parse_pos == server->parse_len + 4) {
    pos = 2;
  } else {
    assert (server->parse_buf);
    if (server->parse_pos < server->parse_len) {
      t[0].iov_len = server->parse_len - server->parse_pos;
      t[0].iov_base = server->parse_buf + server->parse_pos;
      t[1].iov_len = 4;
      t[1].iov_base = &server->parse_real_crc32;
      pos = 0;
    } else {
      t[1].iov_len = 4 - (server->parse_pos - server->parse_len);
      t[1].iov_base = ((char *)&server->parse_real_crc32) + (server->parse_pos - server->parse_len);
      pos = 1;
    }
  }
  ADD_CNT (recv);
  START_TIMER (recv);
  int x = readv (server->sfd, t + pos, 3 - pos);
  if (x < 0 && (errno != EAGAIN && errno != EINPROGRESS)) {
    rpc_server_seterror (server, errno ? strerror (errno) : "Unknown error", errno);
    END_TIMER (read);
    return x;
  } else { 
    if (x < 0) { x = 0; }
    if (x > 0) { 
      update_precise_now ();
      server->last_received_time = precise_now;
    }
  }
  END_TIMER (recv);
  if (pos <= 1) {
    if (x <= server->parse_len + 4 - server->parse_pos) {
      server->parse_pos += x;
      x = 0;
    } else {
      x -= (server->parse_len + 4 - server->parse_pos);
      server->parse_pos = server->parse_len + 4;
    }
  }
  server->in_bytes += x;
  
  END_TIMER (read);
  return x;
}
/* }}} */

static int rpc_force_ready_bytes (struct rpc_server *server, int n, double timeout) { /* {{{ */
  int __n = n;
  if (n <= server->in_bytes) { return server->in_bytes; }
  ADD_CNT(force_read);
  START_TIMER (force_read);
  n -= server->in_bytes;


  int sfd = server->sfd;
  assert (sfd >= 0);
  
  struct pollfd s;
  s.fd = sfd;
  s.events = POLLIN | POLLERR | POLLNVAL | POLLHUP;
  int t = 0; //get_ms_timeout (timeout);
  int first = 1;
  do {
    errno = 0;
    if (poll (&s, 1, t) <= 0) {
      if (first) {
        first = 0;
        continue;
      }
      rpc_server_seterror (server, errno ? strerror (errno) : "Timeout", errno);
      END_TIMER (force_read);
      return __n - n;
    }
    first = 0;
    int t = get_ready_bytes (server, n);    
    if (t < 0) {
      return -1;
    }
    n -= t;
    if (n <= 0) { END_TIMER (force_read); return __n - n;}
    if (s.revents & (POLLERR | POLLNVAL | POLLHUP)) {
      return -1;
    }
  } while ((t = get_ms_timeout (timeout)));
  END_TIMER (force_read);
  return __n - n;
}
/* }}} */

/* rpc_query {{{ */

int max_query_id;
static void rpc_queue_add (struct rpc_queue *Q) {
  queue_tree = tree_insert_queue (queue_tree, Q, lrand48 ());
}

struct rpc_queue *rpc_queue_get (long long id) {
  struct rpc_queue **T = tree_lookup_value_queue (queue_tree, (void *)&id);
  return T ? *T : 0;
}

static void delete_query_from_queue (struct rpc_query *q) {
  if (q->queue_id && (q->status == query_status_ok || q->status == query_status_error)) {
    struct rpc_queue *Q = rpc_queue_get (q->queue_id);
    if (Q) {
      Q->completed = tree_delete_qid (Q->completed, q->qid);
      Q->remaining --;
    }
    q->queue_id  = 0;
  }
}

static struct rpc_query *rpc_query_alloc (double timeout) {
  ADD_CNT (tree_insert);
  START_TIMER (tree_insert);
  if (total_working_qid > RPC_MAX_QUERIES / 2) {
    END_TIMER (tree_insert);
    return 0;
  }
  last_qid ++;
  while (queries[(last_qid - first_qid) & RPC_QUERIES_MASK].qid >= first_qid) { last_qid ++; }
  int fd = (last_qid - first_qid) & RPC_QUERIES_MASK;
  if (fd >= max_query_id) {
    max_query_id = fd + 1;
  }
  //long long qid = last_qid + 1;
  long long qid = last_qid;
  update_precise_now ();
  struct rpc_query *q = &queries[(qid - first_qid) & RPC_QUERIES_MASK];
  memset (q, 0, sizeof (*q));
  q->qid = qid;
  q->start_time = precise_now;
  q->timeout = timeout;
/*  ADD_CNT(tree_insert);
  START_TICKS(tree_insert);
  query_tree = tree_insert_query (query_tree, q, lrand48 ());
  END_TICKS(tree_insert);*/
  total_working_qid ++;
  total_queries ++;
  END_TIMER (tree_insert);
  return q;
}

struct rpc_query *rpc_query_get (long long qid) {
  if (qid < first_qid) { return 0; }
  struct rpc_query *q = &queries[(qid - first_qid) & RPC_QUERIES_MASK];
  return q->qid == qid ? q : 0;
}
/*
static void rpc_query_free (struct rpc_query *q) {
  if (q->answer) {
    zzefree (q->answer, q->answer_len);
  }  
  delete_query_from_queue (q);
  if (q->extra_free) {
    q->extra_free (q);    
  }
  //zzfree (q, sizeof (*q));
}*/

static void rpc_query_delete (struct rpc_query *q) {
  //query_tree = tree_delete_query (query_tree, q);
  if (q->answer) {
    zzefree (q->answer, q->answer_len);
  }
  delete_query_from_queue (q);
  if (q->extra_free) {
    q->extra_free (q);    
  }
  q->qid = 0;
  
  total_working_qid --;
  //zzfree (q, sizeof (*q));
}

static void rpc_query_delete_nobuf (struct rpc_query *q) {
  //query_tree = tree_delete_query (query_tree, q);
  //zzfree (q, sizeof (*q));
  delete_query_from_queue (q);
  if (q->extra_free) {
    q->extra_free (q);    
  }
  q->qid = 0;
  total_working_qid --;
}
/* }}} */

static void update_pid (unsigned ip) { /* {{{ */
  if (!PID.pid) {
    PID.port = 0;
    PID.pid = getpid ();
    PID.utime = time (NULL);
  }
  if (!PID.ip && PID.ip != 0x7f000001) {
    PID.ip = ip;
  }
}
/* }}} */

static struct rpc_server *rpc_server_new (unsigned host, unsigned short port, double timeout, double retry_interval) { /* {{{ */
  struct rpc_server *server = zzmalloc (sizeof (*server));
  ADD_PMALLOC (sizeof (*server));
  memset (server, 0, sizeof (*server));
  
  server->host = host;
  
  server->port = port;
  server->status = rpc_status_disconnected;

  server->timeout = timeout;
  server->retry_interval = retry_interval;

  server->packet_num = -2;
  server->inbound_packet_num = -2;

  server->magic = RPC_SERVER_MAGIC;

  server->fd = last_server_fd ++;
  if (server->fd  >= servers_size) {
    int new_servers_size = servers_size * 2 + 100;
    servers = zzrealloc (servers, sizeof (void *) * servers_size, sizeof (void *) * new_servers_size);
    ADD_PREALLOC (sizeof (void *) * servers_size, sizeof (void *) * new_servers_size);
    server_fds = zzrealloc (server_fds, sizeof (struct pollfd) * servers_size, sizeof (struct pollfd) * new_servers_size);
    ADD_PREALLOC (sizeof (struct pollfd) * servers_size, sizeof (struct pollfd) * new_servers_size);
    server_fds_tmp = zzrealloc (server_fds_tmp, sizeof (int) * servers_size, sizeof (int) * new_servers_size);    
    ADD_PREALLOC (sizeof (int) * servers_size, sizeof (int) * new_servers_size);
    servers_size = new_servers_size;
  }
  servers[server->fd] = server;
  server->sfd = -1;
  rpc_server_clean (server);

  return server;
}
/* }}} */

static void rpc_server_sleep (struct rpc_server *server) { /* {{{ */
  if (server->error != NULL) {
    zzfree (server->error, strlen (server->error) + 1);
    server->error = NULL;
  }
}
/* }}} */

static int rpc_write_handshake (struct rpc_server *server, int op, double timeout);
static void rpc_server_free (struct rpc_server *server) __attribute__ ((unused));
static void rpc_server_free (struct rpc_server *server) /* {{{ */ {
  rpc_server_sleep (server);

  server->magic = 0;
  zzfree (server, sizeof (*server));
  ADD_PFREE (sizeof (*server));
}
/* }}} */

struct rpc_server *rpc_server_get (int fd) { /* {{{ */
  if (fd < 0 || fd >= last_server_fd) {
    return 0;
  }
  return servers[fd];
}
/* }}} */

static void rpc_global_seterror (const char *error, int errnum) { /* {{{ */
  if (error) {
    //fprintf (stderr, "error %s #%d\n", error, errnum);
    if (global_error) {
      zzfree (global_error, strlen (global_error) + 1);
    }
    global_error = strdup (error);
    ADD_MALLOC (strlen (error) + 1);
    global_errnum = errnum;
    if (error && error_verbosity >= 1) {
      printf ("Error %s (error_code %d)\n", error, errnum);
      if (error_verbosity >= 2) {
        print_backtrace ();
      }
    }
  }
}
/* }}} */

static void rpc_server_seterror (struct rpc_server *server, const char *error, int errnum) { /* {{{ */
  if (error) {
    if (server->error) {
      zzfree (server->error, strlen (server->error) + 1);
    }
    
    server->error = strdup (error);
    server->errnum = errnum;
    ADD_MALLOC (strlen (error) + 1);

    rpc_global_seterror (error, errnum);
  }
}
/* }}} */

static void rpc_server_disconnect (struct rpc_server *server) { /* {{{ */
  if (server->sfd >= 0) {
    close (server->sfd);
    server->sfd = -1;
    if (server->status == rpc_status_connected) {
      active_net_connections --;
    }
    net_connections_fails ++;
  }
  server->status = rpc_status_disconnected;
}
/* }}} */

static void rpc_server_clean (struct rpc_server *server) { /* {{{ */
  server->in_bytes = 0;
  server->out_wptr = server->out_rptr = server->out_buf;
  server->inbound_packet_num = server->packet_num = -2;
}
/* }}} */

static void rpc_server_deactivate (struct rpc_server *server) { /* {{{ */
  rpc_server_disconnect (server);
  rpc_server_clean (server);
  server->status = rpc_status_disconnected;
  update_precise_now ();
  server->failed = precise_now;
//  php_error_docref (NULL TSRMLS_CC, E_NOTICE, "Server " IP_PRINT_STR " (tcp %d) [fd = %d] failed with: %s (%d)", IP_TO_PRINT (server->host), server->port, server->fd, server->error, server->errnum);
}
/* }}} */

static int rpc_server_failure (struct rpc_server *server) { /* {{{ */
  if (server->status != rpc_status_disconnected) {
    rpc_server_deactivate (server);
    return 0;
  } else {
    return 1;
  }
}
/* }}} */

static struct timeval _convert_timeout_to_ts (double t) { /* {{{ */
  struct timeval tv;
  int secs = 0;

  secs = (int)t;
  tv.tv_sec = secs;
  tv.tv_usec = (int)(((t - secs) * 1e6) / 1000000);
  return tv;
}
/* }}} */

static int _rpc_connect_open (struct rpc_server *server, char **error_string, int *errnum) { /* {{{ */

  /* close open stream */
  if (server->sfd >= 0) {
    rpc_server_disconnect (server);
  }

  update_precise_now ();
  double t = precise_now + server->timeout;
  server->sfd = rpc_sock_connect (server->host, server->port, t);
  if (server->sfd < 0) {
    rpc_server_seterror (server, errno ? strerror (errno) : "Connect timed out", errno);
    rpc_server_deactivate (server);
    if (error_string) {
      *error_string = estrdup (errno ? strerror (errno) : "Connect timed out");
    }
    if (errnum) {
      *errnum = errno;
    }
    server->status = rpc_status_failed;
    return -1;
  }

  server->status = rpc_status_connected;
  active_net_connections ++;
  server->generation = global_generation;
  server->packet_num = -2;
  server->inbound_packet_num = -2;
  if (rpc_make_handshake (server, t) < 0) {
    rpc_server_seterror (server, "Rpc handshake failed", 0);
    if (error_string) {
      *error_string = estrdup (server->error);
    }
    if (errnum) {
      *errnum = server->errnum;
    }
    server->status = rpc_status_failed;
    return -1;
  } else {
    return 1;
  }
}
/* }}} */


static int rpc_ping_send (struct rpc_server *server, double timeout, long long value) { /* {{{ */
  assert (outbuf);
  buffer_clear (outbuf);
  buffer_write_reserve (outbuf, 12);
  buffer_write_long (outbuf, value);
  if (rpc_write_handshake (server, RPC_PING, timeout) < 0) {
    return -1;
  }
  return rpc_flush_out_force (server, timeout);
}
/* }}} */

static int rpc_work (struct rpc_server *server, int force_block_read, double timeout);
static int rpc_ping (struct rpc_server *server) { /* {{{ */
//  double timeout = 0.1;
  if (server->status != rpc_status_connected || server->sfd < 0) {
    return -1;
  }
  update_precise_now ();
  double t = precise_now + server->timeout;
  if (rpc_ping_send (server, t, lrand48 ()) < 0) {
    rpc_server_disconnect (server);
    return -1;
  }
  if (rpc_work (server, 1, t) < 0) {
    rpc_server_disconnect (server);
    return -1;
  }
  return 1;
}
/* }}} */

static int rpc_open (struct rpc_server *server, char **error_string, int *errnum) { /* {{{ */
  switch (server->status) {
    case rpc_status_disconnected:
      if (_rpc_connect_open (server, error_string, errnum) > 0) {
        return 1;
      } else {
        break;
      }
    
    case rpc_status_connected:
      update_precise_now ();
      if (precise_now - server->last_received_time > ping_timeout) {
        if (rpc_ping (server) > 0) {
          return 1;
        } else if (_rpc_connect_open (server, error_string, errnum) > 0) {
          return 1;
        } else {
          break;
        }
      } else {
        return 1;
      }


    case rpc_status_failed:
      update_precise_now ();
      if (server->retry_interval >= 0 && precise_now >= server->failed + server->retry_interval) {
        if (_rpc_connect_open (server, error_string, errnum) > 0) {
          return 1;
        }
      } else {
        if (error_string) {
          *error_string = estrdup ("server failed some time ago. Fail timeout not exceeded.");
          *errnum = 0;
        }
        break;
      }
      break;
  }
  return -1;
}
/* }}} */

static int rpc_write_handshake (struct rpc_server *server, int op, double timeout) { /* {{{ */
  ADD_CNT (rpc_write_handshake);
  START_TIMER (rpc_write_handshake);
  assert (op == RPC_NONCE || op == RPC_HANDSHAKE || op == RPC_PING);
  if (op == RPC_NONCE && server->packet_num != -2) {
    assert (0);
  }
  if (op == RPC_HANDSHAKE && server->packet_num != -1) {
    assert (0);
  }
  if (op == RPC_PING && server->packet_num < 0) {
    assert (0);
  }
  if (server->sfd < 0) {
    END_TIMER (rpc_write_handshake);
    return -1;
  }
  if (!outbuf) {
    END_TIMER (rpc_write_handshake);
    return -1;
  }
  int len = 16 + (outbuf->wptr - outbuf->rptr);
  unsigned crc32 = 0;

  outbuf->rptr -= 12;
  assert (outbuf->rptr >= outbuf->sptr);
  int *tmp = (void *)outbuf->rptr;
  tmp[0] = len;
  tmp[1] = server->packet_num ++;
  tmp[2] = op;
#ifdef SEND_CRC32
  ADD_CNT (crc32);
  START_TICKS (crc32);
  crc32 = compute_crc32 (outbuf->rptr, outbuf->wptr - outbuf->rptr);
  END_TICKS (crc32);
#endif
  buffer_write_int (outbuf, crc32);
  assert (outbuf->wptr - outbuf->rptr == len);
  
  if (rpc_write_out (server, outbuf->rptr, len, timeout) < 0) {
    END_TIMER (rpc_write_handshake);
    return -1;
  }
  buffer_clear (outbuf);
  END_TIMER (rpc_write_handshake);
  return 0;

}
/* }}} */

static struct rpc_server *choose_writable_server (struct rpc_server_collection *servers, double timeout) { /* {{{ */
  int i;
  int t = 0;   
  int first = 1;
  while (t || first) {
    int cc = 0;
    for (i = 0; i < servers->num; i++) if (servers->servers[i] && servers->servers[i]->status == rpc_status_connected) {
      server_fds[cc].fd = servers->servers[i]->sfd;
      server_fds[cc].events = POLLOUT | POLLERR | POLLHUP | POLLNVAL | POLLRDHUP;
      server_fds_tmp[cc] = i;
      cc ++;
    }
    if (!cc) { return 0; }
    t = get_ms_timeout (timeout);
    first = 0;

    ADD_CNT (poll);
    START_TIMER (poll);
    int r = poll (server_fds, cc, t);
    END_TIMER (poll);
    if (r < 0) {
      rpc_global_seterror (strerror (errno), errno);
      return 0;
    }
    if (r == 0) {
      return 0;
    }

    int k = 0;
    struct rpc_server *result = 0;
    for (i = 0; i < cc; i++) {
      if (server_fds[i].revents & (POLLERR | POLLHUP | POLLNVAL | POLLRDHUP)) {
        if (server_fds[i].revents & POLLRDHUP) {
          while (rpc_work (servers->servers[server_fds_tmp[i]], 0, 0) > 0); 
        }
        rpc_server_failure (servers->servers[server_fds_tmp[i]]);
      } else if (server_fds[i].revents & POLLOUT) {
        if (!(lrand48 () % (k + 1))) {
          result = servers->servers[server_fds_tmp[i]];
        }
        k ++;
      }
    }
    if (result) { return result; }
  }
  return 0;
}
/* }}} */

static int rpc_write (struct rpc_connection *c, long long qid, double timeout) { /* {{{ */
  ADD_CNT (rpc_write);
  START_TIMER (rpc_write);
//  struct rpc_server *server = c->server;
  struct rpc_server *server = choose_writable_server (c->servers, timeout);
  if (!server) {
    END_TIMER (rpc_write);
    return -1;
  }
  assert (qid && server->packet_num >= 0);

  if (server->sfd < 0) {
    END_TIMER (rpc_write);
    return -1;
  }

  if (!outbuf) {
    END_TIMER (rpc_write);
    return -1;
  }

  if (c->default_actor_id) {
    int x = *(int *)outbuf->rptr;
    if (x != TL_RPC_DEST_ACTOR && x != TL_RPC_DEST_ACTOR_FLAGS) {
      outbuf->rptr -= 12;
      assert (outbuf->rptr >= outbuf->sptr);
      *(int *)outbuf->rptr = TL_RPC_DEST_ACTOR;
      *(long long *)(outbuf->rptr + 4) = c->default_actor_id;
    }
  }
  int len = 24 + (outbuf->wptr - outbuf->rptr);
  unsigned crc32 = 0;
  outbuf->rptr -= 20;
  assert (outbuf->rptr >= outbuf->sptr);
  int *tmp = (void *)outbuf->rptr;
  tmp[0] = len;
  tmp[1] = server->packet_num ++;
  tmp[2] = RPC_INVOKE_REQ;
  *(long long *)(tmp + 3) = qid;
#ifdef SEND_CRC32
  ADD_CNT (crc32);
  START_TICKS (crc32);
  crc32 = compute_crc32 (outbuf->rptr, outbuf->wptr - outbuf->rptr);
  END_TICKS (crc32);
#endif
  buffer_write_int (outbuf, crc32);
  assert (outbuf->wptr - outbuf->rptr == len);
  
  if (rpc_write_out (server, outbuf->rptr, len, timeout) < 0) {
    rpc_server_failure (server);
    END_TIMER (rpc_write);
    return -1;
  }
  buffer_clear (outbuf);
  END_TIMER (rpc_write);
  return 0;
}
/* }}} */

static int rpc_nonce_execute (struct rpc_server *server, char *answer, int answer_len) { /* {{{ */
  if (answer_len != sizeof (struct rpc_nonce) || server->inbound_packet_num != -1) {    
    rpc_server_seterror (server, "Bad nonce packet", 0);
    return -1;
  }
  return 0;
} /* }}} */

static int rpc_handshake_execute (struct rpc_server *server, char *answer, int answer_len) { /* {{{ */
  if (answer_len != sizeof (struct rpc_handshake) || server->inbound_packet_num != 0) {
    rpc_server_seterror (server, "Bad handshake packet", 0);
    return -1;
  }
  struct rpc_handshake *S = (void *)answer;
  if ((S->peer_pid.port != PID.port) || (S->peer_pid.ip != PID.ip && S->peer_pid.ip && PID.ip) || S->sender_pid.port != server->port || (S->sender_pid.ip != server->host && S->sender_pid.ip && server->host && server->host != 0x7f000001)) {
    rpc_server_seterror (server, "Bad pid in handshake packet", 0);
    return -1;
  }
  return 0;
}
/* }}} */

static int rpc_handshake_error_execute (struct rpc_server *server, char *answer, int answer_len) { /* {{{ */
  rpc_server_seterror (server, "Rpc error", ((struct rpc_handshake_error *)answer)->error_code);
  return -1;
}
/* }}} */

static int rpc_handshake_send (struct rpc_server *server, double timeout) { /* {{{ */
  assert (server->sfd >= 0);
  
  if (!PID.pid || !PID.ip) {
    struct sockaddr_in S;
    socklen_t l = sizeof (S);
    getsockname (server->sfd, (void *)&S, &l);
    update_pid (ntohl (*(int *)&S.sin_addr));
  }
  struct rpc_handshake S = {
    .flags = 0,
    .sender_pid = PID,
    .peer_pid = { .ip = (server->host != 0x7f000001 ? server->host : 0), .port = server->port }
  };
  //server->outbuf = buffer_create (sizeof (S));
  buffer_clear (outbuf);
  buffer_write_reserve (outbuf, 12);
  buffer_write_data (outbuf, &S, sizeof (S));
  if (rpc_write_handshake (server, RPC_HANDSHAKE, timeout) < 0) {
    return -1;
  }
  return rpc_flush_out_force (server, timeout);
} 
/* }}} */

static int rpc_nonce_send (struct rpc_server *server, double timeout) { /* {{{ */
  struct rpc_nonce S = {
    .key_select = 0,
    .crypto_schema = 0
  };
  
  //server->outbuf = buffer_create (sizeof (S));
  assert (outbuf);
  buffer_clear (outbuf);
  buffer_write_reserve (outbuf, 12);
  buffer_write_data (outbuf, &S, sizeof (S));
  if (rpc_write_handshake (server, RPC_NONCE, timeout) < 0) {
    return -1;
  }
  return rpc_flush_out_force (server, timeout);
}
/* }}} */

/* static int rpc_fill_buf (struct rpc_server *server) {  {{{
  if (server->in_bytes >= 4) { return 1; }
  if (server->in_pos > 0) {
    if (server->in_bytes > 0) {
      memcpy (server->in_buf, server->in_buf + server->in_pos, server->in_bytes);
    }
    server->in_pos = 0;
  }
  int t = rpc_sock_read (server->sfd, server->in_buf + server->in_bytes, 4 - server->in_bytes, RPC_IN_BUF_SIZE - server->in_bytes, 0);
  if (t < 0) {
    rpc_server_failure (server);
    return -1;
  }
  server->in_bytes += t;
  return server->in_bytes >= 4;
}
 }}} */

static int rpc_read (struct rpc_server *server, int force_block_read, double timeout) { /* {{{ */
  ADD_CNT (rpc_read);
  START_TIMER (rpc_read);
  if (server->sfd < 0) {
    rpc_server_seterror (server, "Socket is closed", 0);
    END_TIMER (rpc_read);
    return -1;
  }
  if (server->parse_status == parse_status_expecting_query) {
    assert (!server->parse_buf);
    int t;
    if (force_block_read) {
      t = rpc_force_ready_bytes (server, 20, timeout);
    } else {
      t = get_ready_bytes (server, 20) + server->in_bytes;
    }
    if (t < server->in_bytes) { 
      END_TIMER (rpc_read);
      return -1; 
    }
    if (t < 20) { 
      if (force_block_read) {
        rpc_server_seterror (server, "Timeout", 0);
      }
      END_TIMER (rpc_read);
      return force_block_read ? -1 : 0; 
    }
    
    //fprintf (stderr, "t = %d\n", t);
    static int tmp[5];
    assert (rpc_read_in (server, (char *)tmp, 12, timeout) == 12);
    int len = tmp[0];

    if (len < 20 || (len & 3) || len > RPC_MAX_QUERY_LEN) {
      rpc_server_seterror (server, "Invalid length of answer", 0);
      END_TIMER (rpc_read);
      return -1;
    }

    assert (!server->parse_buf);
    server->parse_status = parse_status_reading_query;
    server->parse_op = tmp[2];

    if (tmp[1] != server->inbound_packet_num ++) {
      rpc_server_seterror (server, "Invalid packet num", 0);
      END_TIMER (rpc_read);
      return -1;
    }


    if (tmp[2] != RPC_HANDSHAKE && tmp[2] != RPC_NONCE && tmp[2] != RPC_HANDSHAKE_ERROR) {
      if (tmp[1] < 0) {
        rpc_server_seterror (server, "Invalid packet num (negative for non-handshake)", 0);
        END_TIMER (rpc_read);
        return -1;
      }
      if (tmp[2] == RPC_REQ_ERROR || tmp[2] == RPC_REQ_RESULT || tmp[2] == RPC_REQ_RUNNING) {
        assert (rpc_read_in (server, (char *)&tmp[3], 8, timeout) == 8);
        server->parse_qid = *(long long *)(tmp + 3);      
        #ifdef CHECK_CRC32
          ADD_CNT (crc32);
          START_TIMER (crc32);
          server->parse_crc32 = compute_crc32 (tmp, 20);
          END_TIMER (crc32);
        #endif
        server->parse_len = len - 24;
        server->parse_pos = 0;
        struct rpc_query *q = rpc_query_get (server->parse_qid);
        if (q) {
          q->status = query_status_receiving;
        } else {
          server->parse_op = RPC_SKIP;
        }
      } else {
        server->parse_qid = -1; 
        #ifdef CHECK_CRC32
          ADD_CNT (crc32);
          START_TIMER (crc32);
          server->parse_crc32 = compute_crc32 (tmp, 12);
          END_TIMER (crc32);
        #endif
        server->parse_len = len - 16;
        server->parse_pos = 0;
      }
    } else {
      if (tmp[1] >= 0) {
        rpc_server_seterror (server, "Invalid packet num (non-negative for nonce/handshake)", 0);
        END_TIMER (rpc_read);
        return -1;
      }
      #ifdef CHECK_CRC32
        ADD_CNT (crc32);        
        START_TIMER (crc32);
        server->parse_crc32 = compute_crc32 (tmp, 12);
        END_TIMER (crc32);
      #endif
      server->parse_len = len - 16;
      server->parse_pos = 0;
    }
    server->parse_buf = zzemalloc (server->parse_len);
  }

  if (server->parse_status == parse_status_reading_query) {
    assert (server->parse_buf);
    if (server->in_bytes) {
      if (server->parse_len - server->parse_pos >= server->in_bytes) {
        memcpy (server->parse_buf + server->parse_pos, server->in_buf + server->in_pos, server->in_bytes);
        server->parse_pos += server->in_bytes;
        server->in_bytes = 0;
        server->in_pos = 0;
      } else {
        if (server->parse_pos < server->parse_len) {
          memcpy (server->parse_buf + server->parse_pos, server->in_buf + server->in_pos, server->parse_len - server->parse_pos);
          server->in_bytes -= (server->parse_len - server->parse_pos);
          server->in_pos += (server->parse_len - server->parse_pos);
          server->parse_pos = server->parse_len;
        }
        int r = 4 + server->parse_len - server->parse_pos;
        if (server->in_bytes <= r) {
          memcpy (((char *)&server->parse_real_crc32) + (4 - r), server->in_buf + server->in_pos, server->in_bytes);
          server->parse_pos += server->in_bytes;
          server->in_bytes = 0;
          server->in_pos = 0;
        } else {
          memcpy (((char *)&server->parse_real_crc32) + (4 - r), server->in_buf + server->in_pos, r);
          server->in_bytes -= r;
          server->in_pos += r;
          server->parse_pos += r;
        }
      }
    }
    if (force_block_read) {
      rpc_force_ready_bytes (server, server->parse_len + 4 - server->parse_pos, timeout);
    } else {
      get_ready_bytes (server, server->parse_len + 4 - server->parse_pos);
    }
    if (server->parse_pos == server->parse_len + 4) {
      server->parse_status = parse_status_expecting_query;
      #ifdef CHECK_CRC32
        ADD_CNT (crc32);
        START_TIMER (crc32);
        server->parse_crc32 = ~crc32_partial (server->parse_buf, server->parse_len, ~server->parse_crc32);
        END_TIMER (crc32);
        if (server->parse_real_crc32 != server->parse_crc32) {
          rpc_server_seterror (server, "Crc32 mistmatch", 0);
          END_TIMER (rpc_read);
          return -1;
        }
      #endif
      struct rpc_query *q;
//      fprintf (stderr, "qid = %lld\n", server->parse_qid);
//      fprintf (stderr, "parse_op = 0x%08x\n", server->parse_op);
      switch (server->parse_op) {
      case RPC_SKIP:
        zzefree (server->parse_buf, server->parse_len);
        server->parse_buf = 0;
        END_TIMER (rpc_read);
        return 1;
      case RPC_NONCE:
        if (rpc_nonce_execute (server, server->parse_buf, server->parse_len) >= 0) {
          zzefree (server->parse_buf, server->parse_len);
          server->parse_buf = 0;
          END_TIMER (rpc_read);
          return rpc_handshake_send (server, timeout);
        } else {
          rpc_server_seterror (server, "Nonce failed", 0);
          zzefree (server->parse_buf, server->parse_len);
          server->parse_buf = 0;
          END_TIMER (rpc_read);
          return -1;
        }
      case RPC_HANDSHAKE:
        if (rpc_handshake_execute (server, server->parse_buf, server->parse_len) >= 0) {
          zzefree (server->parse_buf, server->parse_len);
          server->parse_buf = 0;
          END_TIMER (rpc_read);
          return 1;
        } else {
          rpc_server_seterror (server, "handshake failed", 0);
          zzefree (server->parse_buf, server->parse_len);
          server->parse_buf = 0;
          END_TIMER (rpc_read);
          return -1;
        }
      case RPC_HANDSHAKE_ERROR:
        rpc_handshake_error_execute (server, server->parse_buf, server->parse_len);
        zzefree (server->parse_buf, server->parse_len);
        server->parse_buf = 0;
        rpc_server_seterror (server, "handshake_error received", 0);
        END_TIMER (rpc_read);
        return -1;
      case RPC_REQ_RUNNING:
        q = rpc_query_get (server->parse_qid);
        if (q && q->status == query_status_sent) {
          q->status = query_status_running;
        }
        zzefree (server->parse_buf, server->parse_len);
        server->parse_buf = 0;
        END_TIMER (rpc_read);      
      case RPC_REQ_ERROR:
        q = rpc_query_get (server->parse_qid);
        if (!q) {
          zzefree (server->parse_buf, server->parse_len);
          server->parse_buf = 0;
          END_TIMER (rpc_read);
          return 1;
        }
        q->status = query_status_error;
        q->answer_len = server->parse_len;
        q->answer = server->parse_buf;
        server->parse_buf = 0;
        query_completed = tree_insert_qid (query_completed, q->qid, lrand48 ());
        if (q->queue_id) {
          struct rpc_queue *Q = rpc_queue_get (q->queue_id);
          if (Q) {
            Q->completed = tree_insert_qid (Q->completed, q->qid, lrand48 ());
          }
        }
        errored_queries ++;
        END_TIMER (rpc_read);
        return 1;
      case RPC_REQ_RESULT:
        q = rpc_query_get (server->parse_qid);
        if (!q) {
          zzefree (server->parse_buf, server->parse_len);
          server->parse_buf = 0;
          END_TIMER (rpc_read);
          return 1;
        }
        q->status = query_status_ok;
        q->answer_len = server->parse_len;
        q->answer = server->parse_buf;
        server->parse_buf = 0;
        query_completed = tree_insert_qid (query_completed, q->qid, lrand48 ());
        if (q->queue_id) {
          struct rpc_queue *Q = rpc_queue_get (q->queue_id);
          if (Q) {
            Q->completed = tree_insert_qid (Q->completed, q->qid, lrand48 ());
          }
        }
        finished_queries ++;
        END_TIMER (rpc_read);
        return 1;
      case RPC_PONG:
        zzefree (server->parse_buf, server->parse_len);
        server->parse_buf = 0;
        END_TIMER (rpc_read);
        return 1;
      default:
        zzefree (server->parse_buf, server->parse_len);
        server->parse_buf = 0;
        END_TIMER (rpc_read);
        return 0;
      }        
    } else {
      if (force_block_read) {
        rpc_server_seterror (server, "Timeout", 0);
        zzefree (server->parse_buf, server->parse_len);
        server->parse_buf = 0;
        server->parse_op = RPC_SKIP;
      }
      END_TIMER (rpc_read);
      return force_block_read ? -1 : 0; 
    }
  }
  
  END_TIMER (rpc_read);
  assert (0);
  return 0;
}
/* }}} */

static int rpc_work (struct rpc_server *server, int force_block_read, double timeout) { /* {{{ */
  ADD_CNT(rpc_work);
  int x = rpc_read (server, force_block_read, timeout);
  if (x < 0) {
    rpc_server_failure (server);
    return -1;
  } else if (x == 0) {
    return 0;
  } else {
    return 1;
  }
}
/* }}} */

static int rpc_poll (double timeout) { /* {{{ */
  ADD_CNT (rpc_poll);
  START_TIMER (rpc_poll);
  int i;
  int cc = 0;
  int x = 0;
  for (i = 0; i < last_server_fd; i++) if (servers[i] && servers[i]->in_bytes) {
    x += rpc_work (servers[i], 0, timeout);
    if (x) { END_TIMER (rpc_poll); return 1; }  
  }
  if (!total_working_qid) { return -1; }

  for (i = 0; i < last_server_fd; i++) if (servers[i] && servers[i]->status == rpc_status_connected) {
    server_fds[cc].fd = servers[i]->sfd;
    server_fds[cc].events = POLLIN;
    server_fds_tmp[cc] = i;
    cc ++;
  }
  if (!cc) { return -1; }
  int t = get_ms_timeout (timeout);
  ADD_CNT (poll);
  START_TIMER (poll);
  int r = poll (server_fds, cc, t);
  END_TIMER (poll);
  if (r < 0) {
    rpc_global_seterror (strerror (errno), errno);
    END_TIMER (rpc_poll);
    return -1;
  }
  for (i = 0; i < cc; i++) if (server_fds[i].revents & POLLIN) {
    x += rpc_work (servers[server_fds_tmp[i]], 0, timeout);
    if (x) { END_TIMER (rpc_poll); return 1; }  
  }
  //rpc_work (servers[0]);
  END_TIMER (rpc_poll);
  return x;
}
/* }}} */

static double get_double_time_since_epoch(void) { /* {{{ */
  struct timeval tv;
  gettimeofday(&tv, 0);
  return tv.tv_sec + 1e-6 * tv.tv_usec;
}
/* }}} */

static double get_utime_monotonic (void) { /* {{{ */
  ADD_CNT (utime);
  START_TICKS (utime);
  struct timespec T;
  assert (clock_gettime(CLOCK_MONOTONIC, &T) >= 0);
  END_TICKS (utime);
  return T.tv_sec + (double) T.tv_nsec * 1e-9;
}
/* }}} */

static int rpc_get_answer (struct rpc_query *q, double timeout) { /* {{{ */
  if (!q) {
    //fprintf (stderr, "Can not find query with id %lld\n", q->qid);
    return -1;
  }
  START_TIMER (rpc_get_answer);
  update_precise_now ();
  while (timeout > precise_now && (q->status == query_status_sent || q->status == query_status_running || q->status == query_status_receiving)) {
    rpc_poll (timeout);
    update_precise_now ();
  }
  switch (q->status) {
  case query_status_sent:
  case query_status_running:
  case query_status_receiving:
    rpc_query_delete (q);
    timedout_queries ++;
    END_TIMER (rpc_get_answer);
    return -1;
  case query_status_ok:
    query_completed = tree_delete_qid (query_completed, q->qid);
    END_TIMER (rpc_get_answer);
    return 1;
  case query_status_error:
    query_completed = tree_delete_qid (query_completed, q->qid);
    rpc_query_delete (q);
    END_TIMER (rpc_get_answer);
    return -1;
  }
  END_TIMER (rpc_get_answer);
  return 0;
}
/* }}} */

int rpc_make_handshake (struct rpc_server *server, double timeout) { /* {{{ */
  if (server->status != rpc_status_connected || server->sfd < 0) {
    return -1;
  }
  if (server->inbound_packet_num != -2 || server->packet_num != -2) {
    rpc_server_disconnect (server);
    return -1;
  }
  if (rpc_nonce_send (server, timeout) < 0) {
    rpc_server_disconnect (server);
    return -1;
  }
  if (rpc_work (server, 1, timeout) < 0) {
    rpc_server_disconnect (server);
    return -1;
  }
  if (rpc_work (server, 1, timeout) < 0) {
    rpc_server_disconnect (server);
    return -1;
  }
  assert (server->packet_num == 0);
  assert (server->inbound_packet_num == 0);
  return 0;
}
/* }}} */

unsigned rpc_resolve_hostname (const char *host) { /* {{{ */
  struct hostent *h;
  if (!(h = gethostbyname (host)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    return 0;
  }
  return ntohl (*((int *) h->h_addr));
}
/* }}} */

struct rpc_server_collection *rpc_find_persistent (unsigned host, int port, double timeout, double retry_interval) { /* {{{ */
  struct rpc_server_collection t;
  t.host = host;
  t.port = port;
  tree_server_collection_t *T = tree_lookup_server_collection (server_collection_tree, &t);
  struct rpc_server_collection *servers;
  if (T) {
    servers = T->x;
  } else {
    servers = zzmalloc0 (sizeof (*servers));    
    ADD_PMALLOC (sizeof (*servers));
    assert (servers);
    servers->host = host;
    servers->port = port;
    servers->num = 0;
//  servers->ti
//  servers = rpc_server_new (host, port, timeout, retry_interval);
    server_collection_tree = tree_insert_server_collection (server_collection_tree, servers, lrand48 ());
  }
//  server->timeout = timeout;
//  server->retry_interval = retry_interval;
  return servers;
}
/* }}} */ 

long long my_atoll (const char *s) {
  assert (s);
  int sign = 0;
  if (*s == '-') {
    s ++;
    sign = 1;
  }
  long long r = 0;
  while (*s && *s >= '0' && *s <= '9') {
    r = r * 10 + *(s ++) - '0';    
  }
  return sign ? -r : r;
}

long long parse_zend_long (zval **z) { /* {{{ */
  switch (Z_TYPE_PP (z)) {
  case IS_LONG:
    return Z_LVAL_PP (z);
  case IS_DOUBLE:
    return (long long)Z_DVAL_PP (z);
  case IS_STRING:
    return my_atoll (Z_STRVAL_PP (z));
  default:
    convert_to_long_ex (z);
    return Z_LVAL_PP (z);
  }
}
/* }}} */

double parse_zend_double (zval **z) { /* {{{ */
  switch (Z_TYPE_PP (z)) {
  case IS_LONG:
    return Z_LVAL_PP (z);
  case IS_DOUBLE:
    return Z_DVAL_PP (z);
  case IS_STRING:
    return atof (Z_STRVAL_PP (z));
  default:
    convert_to_double_ex (z);
    return Z_DVAL_PP (z);
  }
}
/* }}} */

char *parse_zend_string (zval **z, int *l) { /* {{{ */
  convert_to_string_ex (z);
  if (l) { *l = Z_STRLEN_PP (z); }
  return Z_STRVAL_PP (z);
}
/* }}} */

extern unsigned long long config_crc64;
extern int tl_constructors;
extern int tl_types;
extern int tl_functions;
extern int persistent_tree_nodes;
extern int dynamic_tree_nodes;
extern int total_ref_cnt;
extern int total_tl_working;
extern int total_tree_nodes_existed;
extern char *tl_config_name;

int rpc_prepare_stats (char *buf, int max_len) { /* {{{ */
  int x = 0;
#ifdef DEBUG_TICKS  
  x += snprintf (buf + x, max_len, PRINT_STAT(utime));
  x += snprintf (buf + x, max_len, PRINT_STAT(write));
  x += snprintf (buf + x, max_len, PRINT_STAT(read));
  x += snprintf (buf + x, max_len, PRINT_STAT(force_read));
  x += snprintf (buf + x, max_len, PRINT_STAT(recv));
  x += snprintf (buf + x, max_len, PRINT_STAT(send));
  x += snprintf (buf + x, max_len, PRINT_STAT(poll));
  x += snprintf (buf + x, max_len, PRINT_STAT(read_in));
  x += snprintf (buf + x, max_len, PRINT_STAT(write_out));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_read));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_write));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_write_handshake));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_send));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_flush));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_get_answer));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_get_and_parse));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_get));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_poll));
  x += snprintf (buf + x, max_len, PRINT_STAT(realloc));
  x += snprintf (buf + x, max_len, PRINT_STAT(emalloc));
  x += snprintf (buf + x, max_len, PRINT_STAT(tmp));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_work));
  x += snprintf (buf + x, max_len, PRINT_STAT(rpc_sock_connect));
  x += snprintf (buf + x, max_len, PRINT_STAT(store));
  x += snprintf (buf + x, max_len, PRINT_STAT(fetch));
  x += snprintf (buf + x, max_len, PRINT_STAT(crc32));
  x += snprintf (buf + x, max_len, PRINT_STAT(tree_insert));
  x += snprintf (buf + x, max_len, PRINT_STAT(total));
  x += snprintf (buf + x, max_len, PRINT_STAT(malloc));
  x += snprintf (buf + x, max_len, PRINT_STAT(parse));
  x += snprintf (buf + x, max_len, PRINT_STAT(precise_now_updates));
  x += snprintf (buf + x, max_len, PRINT_STAT(connect));
  x += snprintf (buf + x, max_len, PRINT_STAT(get_field));
  x += snprintf (buf + x, max_len, PRINT_STAT(set_field));
  x += snprintf (buf + x, max_len, PRINT_STAT(minit));
#endif
#ifdef DEBUG_MEMORY
  x += snprintf (buf + x, max_len, "malloc %lld, emalloc %lld, rmalloc %lld, pmalloc %lld\n", stats.malloc, stats.emalloc, stats.rmalloc, stats.pmalloc);
#endif
  x += snprintf (buf + x, max_len, 
    "active_net_connections\t%d\n"
    "net_connections_fails\t%d\n"
    "total_net_connections\t%d\n"
    "active_queries\t%d\n"
    "finished_queries\t%d\n"
    "errored_queries\t%d\n"
    "timedout_queries\t%d\n"
    "total_queries\t%lld\n"
    "last_error\t%s\n"
    "last_error_code\t%d\n"
    "tl_config_file\t%s\n"
    "tl_config_crc64\t0x%llx\n"
    "tl_functions\t%d\n"
    "tl_constructors\t%d\n"
    "tl_types\t%d\n"
    "persistent_tree_nodes\t%d\n"
    "dynamic_tree_nodes\t%d\n"
    "total_ref_cnt\t%d\n"
    "total_tree_nodes_existed\t%d\n"
    "total_tl_working\t%d\n"
    "ping_timeout\t%0.3f\n"
    ,
    active_net_connections,
    net_connections_fails,
    last_server_fd,
    total_working_qid,
    finished_queries,
    errored_queries,
    timedout_queries,
    total_queries,
    global_error,
    global_errnum,
    tl_config_name,
    config_crc64,
    tl_functions,
    tl_constructors,
    tl_types,
    persistent_tree_nodes,
    dynamic_tree_nodes,
    total_ref_cnt,
    total_tree_nodes_existed,
    total_tl_working,
    ping_timeout
    );
  return x;
}
/* }}} */

int do_rpc_fetch_get_pos (char **error) { /* {{{ */
  ADD_CNT (fetch);
  START_TIMER (fetch);
  if (!inbuf) {
    *error = strdup ("Trying to fetch from empty buffer\n");
    return 0;
  }
  assert (inbuf->magic == RPC_BUFFER_MAGIC);
  END_TIMER (fetch);
  *error = 0;
  return inbuf->rptr - inbuf->sptr;
}
/* }}} */

int do_rpc_fetch_set_pos (int pos, char **error) { /* {{{ */
  ADD_CNT (fetch);
  START_TIMER (fetch);
  if (!inbuf) {
    *error = strdup ("Trying to fetch from empty buffer\n");
    END_TIMER (fetch);
    return 0;
  }
  if (pos < 0 || inbuf->sptr + pos > inbuf->wptr) {
    *error = strdup ("Trying to set bad position\n");
    END_TIMER (fetch);
    return 0;
  }
  assert (inbuf->magic == RPC_BUFFER_MAGIC);
  *error = 0;
  inbuf->rptr = inbuf->sptr + pos;
  END_TIMER (fetch);
  return 1;
}
/* }}} */

int do_rpc_fetch_int (char **error) { /* {{{ */
  ADD_CNT (fetch);
  START_TIMER (fetch);
  if (!inbuf) {
    *error = strdup ("Trying to fetch from empty buffer\n");
    END_TIMER (fetch);
    return 0;
  }
  assert (inbuf->magic == RPC_BUFFER_MAGIC);
  
  int value;
  if (buffer_read_int (inbuf, &value) < 0) {
    *error = strdup ("Can not fetch int from inbuf");
    END_TIMER (fetch);
    return 0;
  } else {
    *error = 0;
    END_TIMER (fetch);
    return value;
  }
}
/* }}} */

int do_rpc_lookup_int (void) { /* {{{ */
  ADD_CNT (fetch);
  START_TIMER (fetch);
  if (!inbuf) {
    END_TIMER (fetch);
    return 0;
  }
  assert (inbuf->magic == RPC_BUFFER_MAGIC);
  END_TIMER (fetch);
  return *(int *)inbuf->rptr;
}
/* }}} */

long long do_rpc_fetch_long (char **error) { /* {{{ */
  ADD_CNT (fetch);
  START_TIMER (fetch);
  if (!inbuf) {
    *error = strdup ("Trying to fetch from empty buffer\n");
    return 0;
  }
  assert (inbuf->magic == RPC_BUFFER_MAGIC);
  
  long long value;
  if (buffer_read_long (inbuf, &value) < 0) {
    *error = strdup ("Can not fetch long from inbuf");
    END_TIMER (fetch);
    return 0;
  } else {
    END_TIMER (fetch);
    *error = 0;
    return value;
  }
}
/* }}} */

double do_rpc_fetch_double (char **error) { /* {{{ */
  ADD_CNT (fetch);
  START_TIMER (fetch);
  if (!inbuf) {
    *error = strdup ("Trying to fetch from empty buffer\n");
    return 0;
  }
  assert (inbuf->magic == RPC_BUFFER_MAGIC);
  
  double value;
  if (buffer_read_double (inbuf, &value) < 0) {
    *error = strdup ("Can not fetch double from inbuf");
    END_TIMER (fetch);
    return 0;
  } else {
    END_TIMER (fetch);
    *error = 0;
    return value;
  }
}
/* }}} */

int do_rpc_fetch_string (char **value) { /* {{{ */
  ADD_CNT (fetch);
  START_TIMER (fetch);
  if (!inbuf) {
    *value = strdup ("Trying fetch from empty buffer\n");
    END_TIMER (fetch);
    return -1;
  }
  assert (inbuf->magic == RPC_BUFFER_MAGIC);
  int value_len;
  if (buffer_read_string (inbuf, &value_len, value) < 0) {
    *value = strdup ("Can not fetch string from inbuf\n");
    END_TIMER (fetch);
    return -1;
  } else {
    END_TIMER (fetch);
    return value_len;
  }
}
/* }}} */

int do_rpc_fetch_eof (char **error) { /* {{{ */
  ADD_CNT (fetch);
  START_TIMER (fetch);
  if (!inbuf) {
    *error = "Trying fetch from empty buffer\n";
    END_TIMER (fetch);
    return 0;
  }
  assert (inbuf->magic == RPC_BUFFER_MAGIC);
  if (inbuf->rptr < inbuf->wptr) {
    *error = 0;
    END_TIMER (fetch);
    return 0;
  } else {
    *error = 0;
    END_TIMER (fetch);
    return 1;
  }
} /* }}} */

struct rpc_query *do_rpc_send_noflush (struct rpc_connection *c, double timeout) { /* {{{ */
  ADD_CNT (rpc_send);
  START_TIMER (rpc_send);
  if (!c || !c->servers) {
    END_TIMER (rpc_send);
    return 0;
  }

  
  struct rpc_query *q = rpc_query_alloc (timeout);
  if (!q) {
    END_TIMER (rpc_send);
    return 0;
  }
  if (rpc_write (c, q->qid, timeout) < 0) {
//    rpc_server_failure (c->server);
    rpc_query_delete (q);
    END_TIMER (rpc_send);
    return 0;
  }

  END_TIMER (rpc_send); 
  return q;
}
/* }}} */

int do_rpc_flush_server (struct rpc_server *server, double timeout) { /* {{{ */
  ADD_CNT (rpc_flush);
  START_TIMER (rpc_flush);
  if (!server || server->status != rpc_status_connected) {
    END_TIMER (rpc_flush);
    return 0;
  }
  if (rpc_flush_out_force (server, timeout) < 0) {
    rpc_server_failure (server);
    END_TIMER (rpc_flush);
    return -1;
  } else {
    END_TIMER (rpc_flush);
    return 1;
  }
}
/* }}} */

int do_rpc_flush (double timeout) { /* {{{ */
  int i;
  int bad = 0;
  for (i = 0; i < last_server_fd; i++) {
    if (do_rpc_flush_server (servers[i], timeout) < 0) {
      bad ++;
    }
  }
  return -bad;
}
/* }}} */

int do_rpc_get_and_parse (long long qid, double timeout) { /* {{{ */
  ADD_CNT (rpc_get_and_parse);
  START_TIMER (rpc_get_and_parse);
  struct rpc_query *q = rpc_query_get (qid);
  if (!q) {
    END_TIMER (rpc_get_and_parse);
    return -1;
  }
  if (timeout >= 0) {
    timeout += q->start_time;
  } else {
    timeout = q->timeout;
  }
  int r = rpc_get_answer (q, timeout);
  //fetch_extra = q->extra;
  if (r < 0) {
    END_TIMER (rpc_get_and_parse);
    return -1;
  } else {
    if (inbuf) {
      inbuf = buffer_delete (inbuf);      
    }
    //struct rpc_query *q = rpc_query_get (qid);
    assert (q);
    inbuf = buffer_create_data (q->answer, q->answer_len);    
    rpc_query_delete_nobuf (q);    
    END_TIMER (rpc_get_and_parse);
    return 1;
  }
}
/* }}} */

int do_rpc_get (long long qid, double timeout, char **value) { /* {{{ */
  ADD_CNT (rpc_get);
  START_TIMER (rpc_get);
  struct rpc_query *q = rpc_query_get (qid);
  if (!q) {
    END_TIMER (rpc_get);
    return -1;
  }
  if (timeout >= 0) {
    timeout += q->start_time;
  } else {
    timeout = q->timeout;
  }
  int r = rpc_get_answer (q, timeout);
  if (r < 0) {
    END_TIMER (rpc_get);
    return -1;
  } else {
    //struct rpc_query *q = rpc_query_get (qid);
    assert (q);
    *value = q->answer;
    int r = q->answer_len;
    rpc_query_delete_nobuf (q);    
    END_TIMER (rpc_get_and_parse);
    return r;
  }
}
/* }}} */

long long do_rpc_get_any_qid (double timeout) { /* {{{ */
  if (query_completed) {
    return query_completed->x;
  }
  if (timeout > precise_now) {
    rpc_poll (timeout);
    if (query_completed) {
      return query_completed->x;
    }
  }
  return -1;
}
/* }}} */

void do_rpc_parse (const char *s, int len) { /* {{{ */
  char *ans = zzemalloc (len);
  memcpy (ans, s, len);
  if (inbuf) {
    inbuf = buffer_delete (inbuf);      
  }
  inbuf = buffer_create_data (ans, len);
}
/* }}} */

struct rpc_connection *do_new_rpc_connection (unsigned host, int port, int num, long long default_actor_id, double default_query_timeout, double connect_timeout, double retry_timeout, char **error, int *errnum) { /* {{{ */
  ADD_CNT (connect);
  START_TIMER (connect);
  struct rpc_server_collection *servers = rpc_find_persistent (host, port, connect_timeout, retry_timeout);
  assert (servers);
  if (servers->num < num) {
    //fprintf (stderr, "( servers->num = %d, servers=%p, servers->servers = %p\n", servers->num, servers, servers->servers);
    servers->servers = zzrealloc (servers->servers, servers->num * sizeof (void *), num * sizeof (void *));
    //fprintf (stderr, ")");
    ADD_PREALLOC (servers->num * sizeof (void *), num * sizeof (void *));
    int i;
    for (i = servers->num; i < num; i++) {
      servers->servers[i] = rpc_server_new (host, port, default_query_timeout, retry_timeout);
    }
    servers->num = num;
  }

  int i;
  int cc = 0;
  for (i = 0; i < servers->num; i++) {
    if (rpc_open (servers->servers[i], error, errnum) >= 0) {
      cc ++;
    }
  }
  if (!cc) {
    END_TIMER (connect);
    return 0;
  }

  struct rpc_connection *c = zzmalloc (sizeof (*c));
  c->fd = last_connection_fd ++;
  c->servers = servers;
  c->default_actor_id = default_actor_id;
  c->default_query_timeout = default_query_timeout;
  rpc_connection_tree = tree_insert_connection (rpc_connection_tree, c, lrand48 ());
  END_TIMER (connect);
  return c;
}
/* }}} */

long long do_rpc_queue_create (int size, long long *arr) { /* {{{ */
  struct rpc_queue *Q = zzmalloc (sizeof (*Q));
  Q->remaining = size;
  Q->qid = last_queue_id ++;
  Q->completed = 0;
  int i;
  double last_timeout = 0;
  for (i = 0; i < size; i++) {
    struct rpc_query *q = rpc_query_get (arr[i]);
    assert (q);
    assert (!q->queue_id);
    q->queue_id = Q->qid;
    if (q->status == query_status_ok || q->status == query_status_error) {
      Q->completed = tree_insert_qid (Q->completed, q->qid, lrand48 ());
    } else {
      last_timeout = q->timeout;
    }
  }
  Q->timeout = last_timeout;
  rpc_queue_add (Q);
  return Q->qid; 
}
/* }}} */

int do_rpc_queue_empty (struct rpc_queue *Q) { /* {{{ */
  return !Q->remaining;
}
/* }}} */

long long do_rpc_queue_next (struct rpc_queue *Q, double timeout) { /* {{{ */
  assert (Q);
  if (Q->completed) {
    //fprintf (stderr, "!");
    return Q->completed->x;    
  }
  do {
    if (rpc_poll (timeout) < 0) {
      return 0;
    }
    if (Q->completed) {
      return Q->completed->x;
    }    
  } while (precise_now < timeout);
  return 0;
}
/* }}} */

void php_rpc_clean (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  do_rpc_clean ();
}
/* }}} */

void php_rpc_store_int (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z;
  if (zend_get_parameters_array_ex (1, &z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  END_TIMER (parse);
  do_rpc_store_int (parse_zend_long (z));
  RETURN_TRUE;
}
/* }}} */

void php_rpc_store_long (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z;
  if (zend_get_parameters_array_ex (1, &z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  END_TIMER (parse);
  do_rpc_store_long (parse_zend_long (z));
  RETURN_TRUE;
}
/* }}} */

void php_rpc_store_string (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z;
  if (zend_get_parameters_array_ex (1, &z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }

  convert_to_string_ex (z);
  END_TIMER (parse);
  do_rpc_store_string (Z_STRVAL_PP (z), Z_STRLEN_PP (z));
  RETURN_TRUE;
}
/* }}} */

void php_rpc_store_double (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z;
  if (zend_get_parameters_array_ex (1, &z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  END_TIMER (parse);
  do_rpc_store_double (parse_zend_double (z));
  RETURN_TRUE;
}
/* }}} */

void php_rpc_store_header (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[2];  
  if (zend_get_parameters_array_ex (argc > 1 ? 2 : 1, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }

  long long cluster_id = parse_zend_long (z[0]);
  int flags = 0;
  if (argc > 1) {
    flags = parse_zend_long (z[1]);
  }
  END_TIMER (parse);
  do_rpc_store_header (cluster_id, flags);
  RETURN_TRUE;
}
/* }}} */

void php_rpc_store_many (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval ***args;
  int argc = ZEND_NUM_ARGS ();
  args = zzemalloc (argc * sizeof (zval **));
  if (zend_get_parameters_array_ex (argc, args) == FAILURE) {
    zzefree (args, argc * sizeof (zval **));
    END_TIMER (parse);
    RETURN_FALSE;
  }
  convert_to_string_ex (args[0]);
  const char *format = Z_STRVAL_PP (args[0]);
  int format_len = Z_STRLEN_PP (args[0]);

  if (format_len != argc - 1) {
    zzfree (args, argc * sizeof (zval **));
    END_TIMER (parse);
    RETURN_FALSE;
  }


  END_TIMER (parse);
  assert (outbuf && outbuf->magic == RPC_BUFFER_MAGIC);
  int i;
  for (i = 1; i < argc; i ++) {
    switch (format[i - 1]) {
    case 's':
      convert_to_string_ex(args[i]);
      do_rpc_store_string (Z_STRVAL_PP (args[i]), Z_STRLEN_PP (args[i]));
      break;
    case 'l':
      do_rpc_store_long (parse_zend_long (args[i]));
      break;
    case 'd':
      do_rpc_store_int (parse_zend_long (args[i]));
      break;
    case 'f':
      do_rpc_store_double (parse_zend_double (args[i]));
      break;
    default:
      zzfree (args, argc * sizeof (zval **));
      RETURN_FALSE;
    }
  }
  zzefree (args, argc * sizeof (zval **));
  RETURN_TRUE;
}
/* }}} */

void php_rpc_fetch_int (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  char *t;
  int value = do_rpc_fetch_int (&t);
  if (!t) {
    RETURN_LONG (value);
  } else {
    php_error_docref (NULL TSRMLS_CC, E_WARNING, t);
    free (t);
    RETURN_FALSE;
  }
}
/* }}} */

char *vv_strdup (const char *s, int len) { /* {{{ */
  char *r = zzmalloc (len + 1);
  memcpy (r, s, len);
  r[len] = 0;
  return r;
}
/* }}} */

char *vv_estrdup (const char *s, int len) { /* {{{ */
  char *r = zzemalloc (len + 1);
  memcpy (r, s, len);
  r[len] = 0;
  return r;
}
/* }}} */

void php_rpc_fetch_long (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  char *t;
  long long value = do_rpc_fetch_long (&t);
  if (!t) {
    VV_RETURN_LONG (value);
  } else {
    php_error_docref (NULL TSRMLS_CC, E_WARNING, t);
    free (t);
    RETURN_FALSE;
  }
}
/* }}} */

void php_rpc_fetch_double (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  char *t;
  double value = do_rpc_fetch_double (&t);
  if (!t) {
    RETURN_DOUBLE (value);
  } else {
    php_error_docref (NULL TSRMLS_CC, E_WARNING, t);
    free (t);
    RETURN_FALSE;
  }
}
/* }}} */

void php_rpc_fetch_string (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  char *value;
  int value_len = do_rpc_fetch_string (&value);
  if (value_len < 0) {
    php_error_docref (NULL TSRMLS_CC, E_WARNING, value);
    free (value);
    RETURN_FALSE;    
  } else {
    ADD_RMALLOC (value_len + 1);
    VV_STR_RETURN_DUP (value,value_len);
  }
}
/* }}} */

void php_rpc_fetch_eof (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  char *t;
  int r = do_rpc_fetch_eof (&t);
  if (t) {
    php_error_docref (NULL TSRMLS_CC, E_WARNING, t);    
  } else {
    if (r) { RETURN_TRUE; } else { RETURN_FALSE;}
  }
}
/* }}} */

void php_rpc_fetch_end (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  char *t;
  int r = do_rpc_fetch_eof (&t);
  if (t) {
    php_error_docref (NULL TSRMLS_CC, E_WARNING, t);    
  } else {
    if (r) { 
      RETURN_TRUE; 
    } else {
      php_error_docref (NULL TSRMLS_CC, E_WARNING, "Ending fetch from non-empty buffer\n");
      RETURN_FALSE;
    }
  }
}
/* }}} */

void php_rpc_queue_create (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z[1];
  if (zend_get_parameters_array_ex (1, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  zval **arr = z[0];
  if (Z_TYPE_PP (arr) != IS_ARRAY) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  
  static long long qids[100000];
  
  HashPosition pos;
  zend_hash_internal_pointer_reset_ex (Z_ARRVAL_PP (arr), &pos);
  zval **zkey;
 
  int cc = 0;
  while (zend_hash_get_current_data_ex(Z_ARRVAL_PP (arr), (void **)&zkey, &pos) == SUCCESS) {
    long long qid = parse_zend_long (zkey);
    struct rpc_query *q = rpc_query_get (qid);
    if (q && !q->queue_id) {
      qids[cc ++] = qid;
    }
    zend_hash_move_forward_ex (Z_ARRVAL_PP(arr), &pos);
  }

  END_TIMER (parse);
  long long queue_id = do_rpc_queue_create (cc, qids);
  VV_RETURN_LONG (queue_id);
}
/* }}} */

void php_rpc_queue_empty (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z[1];
  if (zend_get_parameters_array_ex (1, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  long long qid = parse_zend_long (z[0]);
  struct rpc_queue *Q = rpc_queue_get (qid);
  if (!Q) {
    END_TIMER (parse);
    RETURN_TRUE;
  }
  END_TIMER (parse);
  if (do_rpc_queue_empty (Q)) {
    RETURN_TRUE;
  } else {
    RETURN_FALSE;
  }
}
/* }}} */

void php_rpc_queue_next (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z[2];
  int argc = ZEND_NUM_ARGS ();
  if (zend_get_parameters_array_ex (argc > 1 ? 2 : 1, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  long long qid = parse_zend_long (z[0]);
  struct rpc_queue *Q = rpc_queue_get (qid);
  double timeout;
  if (argc > 1) {
    update_precise_now ();
    timeout = precise_now + parse_zend_double (z[1]);
  } else {
    timeout = Q->timeout;
  }
  if (!Q) {
    END_TIMER (parse);
    RETURN_TRUE;
  }
  END_TIMER (parse);
  long long query_id = do_rpc_queue_next (Q, timeout);
  if (query_id > 0) {
    VV_RETURN_LONG (query_id);
  } else {
    RETURN_FALSE;
  }
}
/* }}} */

void php_rpc_send (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z[2];
  int argc = ZEND_NUM_ARGS ();
  if (zend_get_parameters_array_ex (argc > 1 ? 2 : 1, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  int fd = parse_zend_long (z[0]);

/*  if (fd < 0 || fd >= last_server_fd) {
    END_TIMER (parse);
    RETURN_FALSE;
  }

  struct rpc_server *server = servers[fd];*/
  struct rpc_connection *c = rpc_connection_get (fd);;
  if (!c) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  
  double timeout = argc > 1 ? parse_zend_double (z[1]) : c->default_query_timeout;
  END_TIMER (parse);
  update_precise_now ();
  timeout += precise_now;
  struct rpc_query *q = do_rpc_send_noflush (c, timeout);
  if (!q) {
    RETURN_FALSE;
  }
//  if (do_rpc_flush_server (c->server, timeout) < 0) {
  if (do_rpc_flush (timeout) < 0) {
    RETURN_FALSE;
  } else {
    VV_RETURN_LONG (q->qid);
  }
}
/* }}} */

void php_rpc_send_noflush (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z[2];
  int argc = ZEND_NUM_ARGS ();
  if (zend_get_parameters_array_ex (argc > 1 ? 2 : 1, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  int fd = parse_zend_long (z[0]);
  struct rpc_connection *c = rpc_connection_get (fd);;
  if (!c) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  
  double timeout = argc > 1 ? parse_zend_double (z[1]) : c->default_query_timeout;
  END_TIMER (parse);
  update_precise_now ();
  timeout += precise_now;
  struct rpc_query *q = do_rpc_send_noflush (c, timeout);

  if (!q) {
    RETURN_FALSE;
  } else {
    VV_RETURN_LONG (q->qid);
  }
}
/* }}} */

void php_rpc_flush (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[1];
  if (argc > 0) {
    if (zend_get_parameters_array_ex (1, z) == FAILURE) {
      END_TIMER (parse);
      RETURN_FALSE;
    }
  }
  
  double timeout = argc > 0 ? parse_zend_double (z[0]) : 0.3;
  END_TIMER (parse);
  update_precise_now ();  
  timeout += precise_now;
 
  if (do_rpc_flush (timeout) < 0) {
    RETURN_FALSE;
  } else {
    RETURN_TRUE;
  }
}
/* }}} */

void php_new_rpc_connection (INTERNAL_FUNCTION_PARAMETERS) {  /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z[6];
  int argc = ZEND_NUM_ARGS ();
  if (zend_get_parameters_array_ex (argc > 6 ? 6 : argc < 2 ? 2 : argc, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  convert_to_string_ex (z[0]);
  const char *host_name = Z_STRVAL_PP (z[0]);
//  int host_len = Z_STRLEN_PP (z[0]);
  int port = parse_zend_long (z[1]);
  long long default_actor_id = 0;
  if (argc >= 3) {
    default_actor_id = parse_zend_long (z[2]);
  }
  double default_query_timeout = RPC_DEFAULT_QUERY_TIMEOUT;
  if (argc >= 4) {
    default_query_timeout = parse_zend_double (z[3]);
  }
  double connect_timeout = RPC_DEFAULT_OP_TIMEOUT;
  if (argc >= 5) {
    connect_timeout = parse_zend_double (z[4]);
  }
  double retry_timeout = RPC_DEFAULT_RETRY_TIMEOUT;
  if (argc >= 6) {
    retry_timeout = parse_zend_double (z[5]);
  }

  unsigned host = rpc_resolve_hostname (host_name);

  if (!host) {
    php_error_docref (NULL TSRMLS_CC, E_WARNING, "Can't resolve hostname %s", host_name);
    END_TIMER (parse);
    RETURN_FALSE;
  }

  END_TIMER (parse);

  char *error_string = 0;
  int errnum ;

  struct rpc_connection *c;
  if (!(c = do_new_rpc_connection (host, port, 3, default_actor_id, default_query_timeout, connect_timeout, retry_timeout, &error_string, &errnum))) {
    php_error_docref (NULL TSRMLS_CC, E_WARNING, "Can't connect to %s:%d (ip " IP_PRINT_STR "), %s (%d)", host_name, port, IP_TO_PRINT (host), error_string ? error_string : "Unknown error", errnum);
    if (error_string) {
      efree (error_string);
    }
    RETURN_FALSE;
  }
  

  RETURN_LONG (c->fd);
}
/* }}} */

void php_rpc_get_and_parse (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[2];
  if (zend_get_parameters_array_ex (argc > 1 ? 2 : 1, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  long long qid = parse_zend_long (z[0]);
  double timeout = -1;
  if (argc > 1) {
    timeout = parse_zend_double (z[1]);
  }
  END_TIMER (parse);
  if (do_rpc_get_and_parse (qid, timeout) < 0) {
    RETURN_FALSE;
  } else {
    RETURN_TRUE;
  }
}
/* }}} */

void php_rpc_get (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  int argc = ZEND_NUM_ARGS ();
  zval **z[2];
  if (zend_get_parameters_array_ex (argc > 1 ? 2 : 1, z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  long long qid = parse_zend_long (z[0]);
  double timeout = -1;
  if (argc > 1) {
    timeout = parse_zend_double (z[1]);
  }
  END_TIMER (parse);
  int len;
  char *value;
  if ((len = do_rpc_get (qid, timeout, &value)) < 0) {
    RETURN_FALSE;
  } else {
    ADD_RMALLOC (len + 1);
    VV_STR_RETURN_NOD (value, len);
  }
}
/* }}} */

void php_rpc_get_any_qid (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z;
  int argc = ZEND_NUM_ARGS ();
  double timeout = 0.3;
  if (argc > 0) {
    if (zend_get_parameters_array_ex (1, &z) == FAILURE) {
      END_TIMER (parse);
      RETURN_FALSE;
    }
    timeout = parse_zend_double (z);
  }
  END_TIMER (parse);
  update_precise_now ();
  timeout += precise_now;
  long long qid;
  if ((qid = do_rpc_get_any_qid (timeout)) < 0) {
    RETURN_FALSE;
  } else {
    VV_RETURN_LONG (qid);
  }
}
/* }}} */

void php_rpc_parse (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  ADD_CNT (parse);
  START_TIMER (parse);
  zval **z;
  if (zend_get_parameters_array_ex (1, &z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  convert_to_string_ex (z);
  END_TIMER (parse);
  do_rpc_parse (Z_STRVAL_PP (z), Z_STRLEN_PP (z));
  RETURN_TRUE;
}
/* }}} */

void php_vk_prepare_stats (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  static char buf[1000000];
  rpc_prepare_stats (buf, 1000000);
  RETURN_STRING (buf, 1);
}
/* }}} */

void php_tl_config_load (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  int x = renew_tl_config ();
  if (x < 0) {
    RETURN_FALSE;
  } else {
    RETURN_TRUE;
  }
}
/* }}} */

void php_tl_config_load_file (INTERNAL_FUNCTION_PARAMETERS) { /* {{{ */
  zval **z;
  if (zend_get_parameters_array_ex (1, &z) == FAILURE) {
    END_TIMER (parse);
    RETURN_FALSE;
  }
  convert_to_string (*z);
  int x = read_tl_config (Z_STRVAL_PP (z));
  if (x < 0) {
    RETURN_FALSE;
  } else {
    RETURN_TRUE;
  }
}
/* }}} */

void rpc_on_minit (int module_number) { /* {{{ */
  ADD_CNT (minit);
  START_TIMER (minit);
  last_qid = lrand48 () * (1ll << 32) + lrand48 () + 1000000;;
  last_queue_id = (1 << 30) * 1ll + lrand48 (); 
  if (INI_STR ("tl.conffile")) {
    assert (read_tl_config (INI_STR ("tl.conffile")) >= 0);
    if (inbuf) {
      inbuf = buffer_delete (inbuf);
    }
  }
  if (INI_STR ("vkext.ping_timeout")) {
    ping_timeout = atof (INI_STR ("vkext.ping_timeout"));
    if (ping_timeout <= 0) {
      ping_timeout = PING_TIMEOUT;
    }
  }
  END_TIMER (minit);
}
/* }}} */

void rpc_on_rinit (int module_number) { /* {{{ */
  global_generation ++;
  error_verbosity = 0;
  if (inbuf) {
    inbuf = buffer_delete (inbuf);
  }
  if (outbuf) {
    outbuf = buffer_delete (outbuf);
  }
  outbuf = buffer_create (0);
  first_qid = last_qid;
  max_query_id = 0;
  int i;
  for (i = 0; i < last_server_fd; i++) {
    struct rpc_server *server = servers[i];
    if (server->sfd >= 0) {
      if (server->parse_status == parse_status_reading_query) {
        server->parse_buf = zzemalloc (server->parse_len);
      }
    }
  }
  last_connection_fd = 0;
  tl_parse_on_rinit ();
}
/* }}} */

/* {{{ shutdown methods */
void try_free_query (long long qid) {
  struct rpc_query *q = rpc_query_get (qid);
  if (q) {
    rpc_query_delete (q);
  }
}

void free_connection (struct rpc_connection *s) {
  zzfree (s, sizeof (*s));
}

void free_queries (void) {
  int i;
//  fprintf (stderr, "max_query_id = %d\n", max_query_id);
  for (i = 0; i < max_query_id; i++) if (queries[i].qid) {
    rpc_query_delete (&queries[i]);
  }
}

void free_queue (struct rpc_queue *Q) {
  tree_clear_qid (Q->completed);
  zzfree (Q, sizeof (*Q));
}
/* }}} */

void rpc_on_rshutdown (int module_number) { /* {{{ */
  if (inbuf) {
    inbuf = buffer_delete (inbuf);
  }
  if (outbuf) {
    outbuf = buffer_delete (outbuf);
  }
  //tree_act_query (query_tree, rpc_query_free);
  //query_tree = tree_clear_query (query_tree);
  tree_act_qid (query_completed, try_free_query);
  //total_working_qid = 0;
  query_completed = tree_clear_qid (query_completed);
  
  int i;
  for (i = 0; i < last_server_fd; i++) {
    struct rpc_server *server = servers[i];
    if (server->sfd >= 0) {
      if (server->parse_status == parse_status_reading_query) {
        zzefree (server->parse_buf, server->parse_len);
        server->parse_op = RPC_SKIP;
      }
    }
  }

  tree_act_connection (rpc_connection_tree, free_connection);
  rpc_connection_tree = tree_clear_connection (rpc_connection_tree);

  tree_act_queue (queue_tree, free_queue);
  queue_tree = tree_clear_queue (queue_tree);
 
  free_queries ();
  tl_parse_on_rshutdown ();
  tl_delete_old_configs ();

#ifdef PRINT_DEBUG_INFO
  if (1) {
#else
  if (error_verbosity > 0) {
#endif
    static char buf[1000000];
    rpc_prepare_stats (buf, 1000000);
    printf ("%s", buf);
  }

  //fprintf (stderr, "send_noflush %lf clean %lf\n", stats.send_noflush_time, stats.clean_time);
}
/* }}} */

//#include "vkext_tl_memcache.c"
//#include "vkext_tl_parse.c"
