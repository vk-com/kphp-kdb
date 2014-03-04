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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <aio.h>
#include <netdb.h>
#include <math.h>

#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "vv-io.h"
#include "pid.h"
struct process_id PID;

#define	USE_EPOLLET	0
#define	MAX_RECONNECT_INTERVAL	20

int max_connection;
struct connection Connections[MAX_CONNECTIONS];

extern int verbosity, now, start_time, maxconn;

struct in_addr settings_addr;
int active_connections, active_special_connections, max_special_connections = MAX_CONNECTIONS;
int outbound_connections, active_outbound_connections, ready_outbound_connections, listening_connections;
long long outbound_connections_created, inbound_connections_accepted;
int ready_targets;
int conn_generation;

long long netw_queries, netw_update_queries, total_failed_connections, total_connect_failures;

long long rpc_queries_received, rpc_queries_ok, rpc_queries_error, 
          rpc_answers_received, rpc_answers_ok, rpc_answers_error, rpc_answers_timedout,
          rpc_sent_queries, rpc_sent_answers, rpc_sent_errors;

int udp_error_crc32;
int udp_error_magic;
int udp_error_bad_flags;
int udp_error_pid;
int udp_error_parse;
int udp_error_crypto;
int udp_error_time;

int udp_tx_timeout_cnt;
int udp_rx_timeout_cnt;

long long udp_data_sent;
long long udp_data_body_sent;
long long udp_packets_sent;
long long udp_encrypted_packets_sent;
long long udp_unencrypted_packets_sent;

long long udp_data_received;
long long udp_data_body_received;
long long udp_packets_received;
long long udp_encrypted_packets_received;
long long udp_unencrypted_packets_received;

int rwm_total_msgs;
int rwm_total_msg_parts;

long long total_used_buffers_size;
int total_used_buffers;
long max_allocated_buffer_bytes, allocated_buffer_bytes;
int allocated_buffer_chunks, max_buffer_chunks;

int allocated_buffer_chunks, max_buffer_chunks;

int free_tmp_buffers (struct connection *c) {
  if (c->Tmp) {
    free_all_buffers (c->Tmp);
    c->Tmp = 0;
  }
  return 0;
}  

/* default value for conn->type->free_buffers */
int free_connection_buffers (struct connection *c) {
  free_tmp_buffers (c);
  free_all_buffers (&c->In);
  free_all_buffers (&c->Out);
  return 0;
}

int server_writer (struct connection *c);
int client_init_outbound (struct connection *c) {
  return 0;
}



/*
 * client
 */

    
int allocated_aes_crypto;

int quit_steps;

int my_pid;

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

int prepare_stats (struct connection *c, char *buff, int buff_size) {
  if (buff_size <= 0) {
    /* (SIGSEGV guard)                                */
    /* in snprintf function second arg type is size_t */
    return 0;
  }
  if (!my_pid) {
    my_pid = getpid ();
  }
  int uptime = now - start_time;

  int stats_len = snprintf (buff, buff_size,
			    "pid\t%d\n"
			    "start_time\t%d\n"
			    "current_time\t%d\n"
			    "uptime\t%d\n"
			    "tot_idle_time\t%.3f\n"
			    "average_idle_percent\t%.3f\n"
			    "recent_idle_percent\t%.3f\n"
			    "network_connections\t%d\n"
			    "encrypted_connections\t%d\n"
			    "max_network_connections\t%d\n"
			    "inbound_connections_accepted\t%lld\n"
			    "outbound_connections_created\t%lld\n"
			    "outbound_connections\t%d\n"
			    "active_outbound_connections\t%d\n"
			    "ready_outbound_connections\t%d\n"
			    "ready_targets\t%d\n"
			    "declared_targets\t%d\n"
			    "active_special_connections\t%d\n"
			    "max_special_connections\t%d\n"
			    "active_network_events\t%d\n"
			    "used_network_buffers\t%d\n"
			    "free_network_buffers\t%d\n"
			    "allocated_network_buffers\t%d\n"
			    "max_network_buffers\t%d\n"
			    "network_buffer_size\t%d\n"
			    "queries_total\t%lld\n"
			    "qps\t%.3f\n"
			    "update_queries_total\t%lld\n"
			    "update_qps\t%.3f\n"
			    "rpc_queries_received\t%lld\n"
			    "rpc_queries_ok\t%lld\n"
			    "rpc_queries_error\t%lld\n"
			    "rpc_answers_received\t%lld\n"
			    "rpc_answers_ok\t%lld\n"
			    "rpc_answers_error\t%lld\n"
			    "rpc_answers_timedout\t%lld\n"
			    "rpc_sent_queries\t%lld\n"
			    "rpc_sent_answers\t%lld\n"
			    "rpc_sent_errors\t%lld\n"
			    "rpc_qps\t%.3f\n",
			    my_pid,
			    start_time,
			    now,
			    uptime,
			    tot_idle_time,
			    uptime > 0 ? tot_idle_time / uptime * 100 : 0,
			    a_idle_quotient > 0 ? a_idle_time / a_idle_quotient * 100 : a_idle_time,
			    active_connections,
			    allocated_aes_crypto,
			    maxconn,
			    inbound_connections_accepted,
			    outbound_connections_created,
			    outbound_connections,
			    active_outbound_connections,
			    ready_outbound_connections,
			    ready_targets,
			    allocated_targets,
			    active_special_connections,
			    max_special_connections,
			    ev_heap_size,
			    NB_used,
			    NB_free,
			    NB_alloc,
			    NB_max,
			    NB_size,
			    netw_queries,
			    safe_div(netw_queries, uptime),
			    netw_update_queries,
			    safe_div(netw_update_queries, uptime),
			    rpc_queries_received, rpc_queries_ok, rpc_queries_error,
			    rpc_answers_received, rpc_answers_ok, rpc_answers_error, rpc_answers_timedout,
			    rpc_sent_queries, rpc_sent_answers, rpc_sent_errors,
			    safe_div(rpc_queries_received, uptime)
		  );

  if (stats_len >= buff_size) {
    return buff_size - 1;
  }
  
  if (udp_packets_received || udp_packets_sent) {
    stats_len += snprintf (buff + stats_len, buff_size - stats_len,
      "udp_error_crc32\t%d\n"
      "udp_error_magic\t%d\n"
      "udp_error_bad_flags\t%d\n"
      "udp_error_pid\t%d\n"
      "udp_error_parse\t%d\n"
      "udp_error_crypto\t%d\n"
      "udp_error_time\t%d\n"      
      "udp_tx_timeout_cnt\t%d\n"
      "udp_rx_timeout_cnt\t%d\n"
      "udp_data_sent\t%lld\n"
      "udp_data_body_sent\t%lld\n"
      "udp_packets_sent\t%lld\n"
      "udp_encrypted_packets_sent\t%lld\n"
      "udp_unencrypted_packets_sent\t%lld\n"
      "udp_data_received\t%lld\n"
      "udp_data_body_received\t%lld\n"
      "udp_packets_received\t%lld\n"
      "udp_encrypted_packets_received\t%lld\n"
      "udp_unencrypted_packets_received\t%lld\n"
      "PID\t" PID_PRINT_STR "\n"
      ,
      udp_error_crc32,
      udp_error_magic,
      udp_error_bad_flags,
      udp_error_pid,
      udp_error_parse,
      udp_error_crypto,
      udp_error_time,
      udp_tx_timeout_cnt,
      udp_rx_timeout_cnt,
      udp_data_sent,
      udp_data_body_sent,
      udp_packets_sent,
      udp_encrypted_packets_sent,
      udp_unencrypted_packets_sent,
      udp_data_received,
      udp_data_body_received,
      udp_packets_received,
      udp_encrypted_packets_received,
      udp_unencrypted_packets_received,
      PID_TO_PRINT (&PID)
    );
  }

  if (udp_packets_received || udp_packets_sent || allocated_buffer_chunks) {
    stats_len += snprintf (buff + stats_len, buff_size - stats_len,
      "rwm_total_messages\t%d\n"
      "rwm_total_message_parts\t%d\n"
      "total_used_buffers_size\t%lld\n"
      "total_used_buffers\t%d\n"
      "max_allocated_buffer_bytes\t%ld\n"
      "allocated_buffer_bytes\t%ld\n"
      "allocated_buffer_chunks\t%d\n"
      "max_buffer_chunks\t%d\n"
      ,
      rwm_total_msgs,
      rwm_total_msg_parts,
      total_used_buffers_size,
      total_used_buffers,
      max_allocated_buffer_bytes, 
      allocated_buffer_bytes,
      allocated_buffer_chunks, 
      max_buffer_chunks
    );
  }


  return stats_len;
}

static inline void disable_qack (int fd) {
  vkprintf (2, "disable TCP_QUICKACK for %d\n", fd);
  assert (setsockopt (fd, IPPROTO_TCP, TCP_QUICKACK, (int[]){0}, sizeof (int)) >= 0);
}

static inline void cond_disable_qack (struct connection *c) {
  if (c->flags & C_NOQACK) {
    disable_qack (c->fd);
  }
}

/*
static inline void cond_reset_cork (struct connection *c) {
  if (c->flags & C_NOQACK) {
    vkprintf (2, "disable TCP_CORK for %d\n", c->fd);
    assert (setsockopt (c->fd, IPPROTO_TCP, TCP_CORK, (int[]){0}, sizeof (int)) >= 0);
    vkprintf (2, "enable TCP_CORK for %d\n", c->fd);
    assert (setsockopt (c->fd, IPPROTO_TCP, TCP_CORK, (int[]){1}, sizeof (int)) >= 0);
  }
}
*/

int prepare_iovec (struct iovec *iov, int *iovcnt, int maxcnt, netbuffer_t *H) {
  int t = 0, i;
  nb_iterator_t Iter;
  nbit_set (&Iter, H);

  for (i = 0; i < maxcnt; i++) {
    int s = nbit_ready_bytes (&Iter);
    if (s <= 0) {
      break;
    }
    iov[i].iov_len = s;
    iov[i].iov_base = nbit_get_ptr (&Iter);
    assert (nbit_advance (&Iter, s) == s);
    t += s;
  }

  *iovcnt = i;

  return t;
}

/* returns # of bytes in c->Out remaining after all write operations;
   anything is written if (1) C_WANTWR is set 
                      AND (2) c->Out.total_bytes > 0 after encryption 
                      AND (3) C_NOWR is not set
   if c->Out.total_bytes becomes 0, C_WANTWR is cleared ("nothing to write") and C_WANTRD is set
   if c->Out.total_bytes remains >0, C_WANTRD is cleared ("stop reading until all bytes are sent")
*/ 
int server_writer (struct connection *c) {
  int r, s, t = 0, check_watermark;
  char *to;

  assert (c->status != conn_connecting);

  if (c->crypto) {
    assert (c->type->crypto_encrypt_output (c) >= 0);
  }

  do {
    check_watermark = (c->Out.total_bytes >= c->write_low_watermark);
    while ((c->flags & C_WANTWR) != 0) {
      // write buffer loop
      s = get_ready_bytes (&c->Out);

      if (!s) {
        c->flags &= ~C_WANTWR;
        break;
      }

      if (c->flags & C_NOWR) {
        break;
      }

      static struct iovec iov[64];
      int iovcnt = -1;

      s = prepare_iovec (iov, &iovcnt, 64, &c->Out);
      assert (iovcnt > 0 && s > 0);

      to = get_read_ptr (&c->Out);
      // r = send (c->fd, get_read_ptr (&c->Out), s, MSG_DONTWAIT | MSG_NOSIGNAL);

      // debug code
      #if 0
      long long before = rdtsc ();
      #endif
      
      //// cond_disable_qack (c);

      r = writev (c->fd, iov, iovcnt);

      #if 0
      long long interval = rdtsc () - before;

      if (interval > (long long)2e9) {
	int i;
	vkprintf (0, "--- writev had worked too long (%lld ticks), dumping parameters ---\n", interval);
	for (i = 0; i < iovcnt; i++) {
	  vkprintf (0, "  buffer %d: base %p, length %lu\n", i, iov[i].iov_base, (unsigned long)iov[i].iov_len);
	}
	vkprintf (0, "---\n");
      }
      #endif

      if (verbosity > 0) {
	if (verbosity > 6) {
	  fprintf (stderr, "send/writev() to %d: %d written out of %d in %d chunks at %p (%.*s)\n", c->fd, r, s, iovcnt, to, ((unsigned) r < 64) ? r : 64, to);
	} else {
	  fprintf (stderr, "send/writev() to %d: %d written out of %d in %d chunks\n", c->fd, r, s, iovcnt);
	}
        if (r < 0) {
	  perror ("send()");
	}
      }

      //// cond_disable_qack (c);

      if (r > 0) {
        advance_skip_read_ptr (&c->Out, r);
        t += r;
      }

      if (r < s) {
        c->flags |= C_NOWR;
      }

    }

    if (t) {
//      int tcp_flags;
//      setsockopt (c->fd, IPPROTO_TCP, TCP_NODELAY, &tcp_flags, sizeof(tcp_flags));
      // cond_disable_qack (c);
      // cond_reset_cork (c);

      free_unused_buffers (&c->Out);
      if (check_watermark && c->Out.total_bytes < c->write_low_watermark && c->type->ready_to_write) {
        c->type->ready_to_write (c);
        t = 0;
	if (c->crypto) {
	  assert (c->type->crypto_encrypt_output (c) >= 0);
	}
        if (c->Out.total_bytes > 0) {
          c->flags |= C_WANTWR;
        }
      }
    }
  } while ((c->flags & (C_WANTWR | C_NOWR)) == C_WANTWR);

  if (c->Out.total_bytes) {
    c->flags &= ~C_WANTRD;
  } else if (c->status != conn_write_close && !(c->flags & C_FAILED)) {
    c->flags |= C_WANTRD;
  }

  return c->Out.total_bytes;
}

/* reads and parses as much as possible, and returns:
   0 : all ok
   <0 : have to skip |res| bytes before invoking parse_execute
   >0 : have to read that much bytes before invoking parse_execute
   -1 : if c->error has been set
   NEED_MORE_BYTES=0x7fffffff : need at least one byte more 
*/
int server_reader (struct connection *c) {
  int res = 0, r, r1, s;
  char *to;

  while (1) {
    /* check whether it makes sense to try to read from this socket */
    int try_read = (c->flags & C_WANTRD) && !(c->flags & (C_NORD | C_FAILED | C_STOPREAD)) && !c->error;
    /* check whether it makes sense to invoke parse_execute() even if no new bytes are read */
    int try_reparse = (c->flags & C_REPARSE) && (c->status == conn_expect_query || c->status == conn_reading_query || c->status == conn_wait_answer || c->status == conn_reading_answer) && !c->skip_bytes;
    if (!try_read && !try_reparse) {
      break;
    }

    if (try_read) {
      /* Reader */
      if (c->status == conn_write_close) {
	free_all_buffers (&c->In);
	c->flags &= ~C_WANTRD;
	break;
      }

      to = get_write_ptr (&c->In, 512);
      s = get_write_space (&c->In);

      if (s <= 0) {
	free_all_buffers (&c->In);
	c->error = -1;
	return -1;
      }

      assert (to && s > 0);

      if (c->basic_type != ct_pipe) {
	//// cond_disable_qack (c);
        r = recv (c->fd, to, s, MSG_DONTWAIT);
      } else {
        r = read (c->fd, to, s);
      }

      if (r < s) { 
	c->flags |= C_NORD; 
      }

      if (verbosity > 0) {
	if (verbosity > 6) {
	  fprintf (stderr, "recv() from %d: %d read out of %d at %p (%.*s)\n", c->fd, r, s, to, ((unsigned) r < 64) ? r : 64, to);
	} else {
	  fprintf (stderr, "recv() from %d: %d read out of %d\n", c->fd, r, s);
	}
	if (r < 0 && errno != EAGAIN) {
	  perror ("recv()");
	}
      }

      if (r > 0) {

	// cond_disable_qack (c);

	advance_write_ptr (&c->In, r);

	s = c->skip_bytes;

	if (s && c->crypto) {
	  assert (c->type->crypto_decrypt_input (c) >= 0);
	}

	r1 = c->In.total_bytes;

	if (s < 0) {
	  // have to skip s more bytes
	  if (r1 > -s) {
	    r1 = -s;
	  }
	  advance_read_ptr (&c->In, r1);
	  c->skip_bytes = s += r1;

	  if (verbosity > 2) {
	    fprintf (stderr, "skipped %d bytes, %d more to skip\n", r1, -s);
	  }
	  if (s) {
	    continue;
	  }
	}

	if (s > 0) {
	  // need to read s more bytes before invoking parse_execute()
	  if (r1 >= s) {
	    c->skip_bytes = s = 0;
	  }
	  
	  vkprintf (1, "fetched %d bytes, %d available bytes, %d more to load\n", r, r1, s ? s - r1 : 0);
	  if (s) {
	    continue;
	  }
	}
      }
    } else {
      r = 0x7fffffff;
    }

    if (c->crypto) {
      assert (c->type->crypto_decrypt_input (c) >= 0);
    }

    while (!c->skip_bytes && (c->status == conn_expect_query || c->status == conn_reading_query ||
			      c->status == conn_wait_answer || c->status == conn_reading_answer)) {
      /* Parser */
      int conn_expect = (c->status - 1) | 1; // one of conn_expect_query and conn_wait_answer; using VALUES of these constants!
      c->flags &= ~C_REPARSE;
      if (!c->In.total_bytes) {
	/* encrypt output; why here? */
        if (c->crypto) {
          assert (c->type->crypto_encrypt_output (c) >= 0);
        }
        return 0;
      }
      if (c->status == conn_expect) {
        nbit_set (&c->Q, &c->In);
        c->parse_state = 0;
        c->status++;  // either conn_reading_query or conn_reading_answer
      } else if (!nbit_ready_bytes (&c->Q)) {
        break;
      }
      res = c->type->parse_execute (c);
      // 0 - ok/done, >0 - need that much bytes, <0 - skip bytes, or NEED_MORE_BYTES
      if (!res) {
        nbit_clear (&c->Q);
        if (c->status == conn_expect + 1) {  // either conn_reading_query or conn_reading_answer
          c->status--;
        }
        if (c->error) {
          return -1;
        }
      } else if (res != NEED_MORE_BYTES) {
	// have to load or skip abs(res) bytes before invoking parse_execute
        if (res < 0) {
          assert (!c->In.total_bytes);
          res -= c->In.total_bytes;
        } else {
          res += c->In.total_bytes;
        }
        c->skip_bytes = res;
        break;
      }
    }

    if (r <= 0) {
      break;
    }
  }

  if (c->crypto) {
    /* encrypt output once again; so that we don't have to check c->Out.unprocessed_bytes afterwards */
    assert (c->type->crypto_encrypt_output (c) >= 0);
  }

  return res;
}

int server_close_connection (struct connection *c, int who) {
  struct conn_query *q;

  clear_connection_timeout (c);

  if (c->first_query) {
    while (c->first_query != (struct conn_query *) c) {
      q = c->first_query;
      q->cq_type->close (q);
      if (c->first_query == q) {
        delete_conn_query (q);
      }
    }
  }

  if (c->type->crypto_free) {
    c->type->crypto_free (c);
  }

  if (c->target || c->next) {
    c->next->prev = c->prev;
    c->prev->next = c->next;
    c->prev = c->next = 0;
  }

  if (c->target) {
    --c->target->outbound_connections;
    --outbound_connections;
    if (c->status != conn_connecting) {
      --c->target->active_outbound_connections;
      --active_outbound_connections;
    }
  }

  c->status = conn_none;
  c->flags = 0;
  c->generation = -1;

  if (c->basic_type == ct_listen) {
    return 0;
  }

  return c->type->free_buffers(c);
}

int compute_next_reconnect (struct conn_target *S) {
  if (S->next_reconnect_timeout < S->reconnect_timeout || S->active_outbound_connections) {
    S->next_reconnect_timeout = S->reconnect_timeout;
  }
  S->next_reconnect = precise_now + S->next_reconnect_timeout;
  if (!S->active_outbound_connections && S->next_reconnect_timeout < MAX_RECONNECT_INTERVAL) {
    S->next_reconnect_timeout = S->next_reconnect_timeout * 1.5 + drand48 () * 0.2;
  }
  return S->next_reconnect;
}

int client_close_connection (struct connection *c, int who) {
  struct conn_query *q;
  struct conn_target *S = c->target;

  clear_connection_timeout (c);

  if (c->first_query) {
    while (c->first_query != (struct conn_query *) c) {
      q = c->first_query;
      q->cq_type->close (q);
      if (c->first_query == q) {
        delete_conn_query (q);
      }
    }
  }

  if (c->type->crypto_free) {
    c->type->crypto_free (c);
  }

  if (S) {
    c->next->prev = c->prev;
    c->prev->next = c->next;
    --S->outbound_connections;
    --outbound_connections;
    if (c->status != conn_connecting) {
      --S->active_outbound_connections;
      --active_outbound_connections;
    }
    if (S->outbound_connections < S->min_connections && precise_now >= S->next_reconnect && S->refcnt > 0) {
      create_new_connections (S);
      if (S->next_reconnect <= precise_now) {
        compute_next_reconnect (S);
      }
    }
  }

  c->status = conn_none;
  c->flags = 0;
  c->generation = -1;

  return c->type->free_buffers(c);
}

#if USE_EPOLLET
static inline int compute_conn_events (struct connection *c) {
  return (((c->flags & (C_WANTRD | C_STOPREAD)) == C_WANTRD) ? EVT_READ : 0) | (c->flags & C_WANTWR ? EVT_WRITE : 0) | EVT_SPEC;
}
#else
static inline int compute_conn_events (struct connection *c) {
  return (((c->flags & (C_WANTRD | C_STOPREAD)) == C_WANTRD) ? EVT_READ : 0) | (c->flags & C_WANTWR ? EVT_WRITE : 0) | EVT_SPEC 
       | (((c->flags & (C_WANTRD | C_NORD)) == (C_WANTRD | C_NORD))
         || ((c->flags & (C_WANTWR | C_NOWR)) == (C_WANTWR | C_NOWR)) ? EVT_LEVEL : 0);
}
#endif


void close_special_connection (struct connection *c) {
  if (c->basic_type != ct_listen) {
    if (--active_special_connections < max_special_connections && Connections[c->listening].basic_type == ct_listen && Connections[c->listening].generation == c->listening_generation) {
      epoll_insert (c->listening, EVT_RWX | EVT_LEVEL);
    }
  }
}

int force_clear_connection (struct connection *c) {
  vkprintf (1, "socket %d: forced closing\n", c->fd);
  if (c->status != conn_connecting) {
    active_connections--;
    if (c->flags & C_SPECIAL) {
      close_special_connection (c);
    }
  } else {
    total_connect_failures++;
  }
  c->type->close (c, 0);
  clear_connection_timeout (c);

  if (c->ev) {
    c->ev->data = 0;
  }
  memset (c, 0, sizeof(struct connection));

  return 1;
}

int out_total_processed_bytes (struct connection *c) {
  if (c->flags & C_RAWMSG) {
    return (c->crypto ? c->out_p.total_bytes : c->out.total_bytes);
  } else {
    return c->Out.total_bytes;
  }
}

int out_total_unprocessed_bytes (struct connection *c) {
  if (c->flags & C_RAWMSG) {
    return (c->crypto ? c->out.total_bytes : 0);
  } else {
    return c->Out.unprocessed_bytes;
  }
}

int server_read_write (struct connection *c) {
  int res, inconn_mask = c->flags | ~C_INCONN;
  event_t *ev = c->ev;

  vkprintf (2, "BEGIN processing connection %d, status=%d, flags=%d, pending=%d; epoll_ready=%d, ev->ready=%d\n", c->fd, c->status, c->flags, c->pending_queries, ev->epoll_ready, ev->ready);

  c->flags |= C_INCONN;

  if (ev->epoll_ready & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
    vkprintf (1, "socket %d: disconnected, cleaning\n", c->fd);
    force_clear_connection (c);
    return EVA_DESTROY;
  }

/*
  if (c->gather && (c->gather->timeout_time >= now || c->gather->ready_num == c->gather->wait_num)) {
  }
*/

  if (c->status == conn_connecting) { /* connecting... */
    if (ev->ready & EVT_WRITE) {
      vkprintf (1, "socket #%d to %s:%d becomes active\n", c->fd, inet_ntoa(c->target->target), c->target->port);
      struct conn_target *S = c->target;
      S->active_outbound_connections++;
      active_outbound_connections++;
      active_connections++;
      c->status = conn_wait_answer;
      c->flags = (c->flags & C_PERMANENT) | C_WANTRD | C_INCONN;
      c->type->connected (c);
    
      if (out_total_processed_bytes (c) > 0) {
        c->flags |= C_WANTWR;
      }
      c->type->check_ready (c);

      vkprintf (2, "socket #%d: ready=%d\n", c->fd, c->ready);
    }
    if (c->status == conn_connecting) {
      c->flags &= inconn_mask;
      kprintf ("socket #%d: unknown event before connecting (?)\n", c->fd);
      return EVT_SPEC;
    }
  }

  //// cond_disable_qack (c);

  if (c->flags & C_ALARM) {
    c->flags &= ~C_ALARM;
    c->type->alarm(c);
  }

  if (c->status == conn_wait_net && !c->pending_queries) {
    c->type->wakeup(c);
  }

  if (c->status == conn_wait_aio && !c->pending_queries) {
    c->type->wakeup(c);
  }

  if (c->flags & C_DFLUSH) {
    c->flags &= ~C_DFLUSH;
    c->type->flush (c);
    if (c->crypto && out_total_unprocessed_bytes (c) > 0) {
      assert (c->type->crypto_encrypt_output (c) >= 0);      
    }
    if (out_total_processed_bytes (c) > 0) {
      c->flags |= C_WANTWR;
    }
  }

  /* MAIN LOOP: run while either 
     1) we want and can read; 
     2) we want and can write; 
     3) we want re-parse input */
  while (
	 ((c->flags & C_WANTRD) && !(c->flags & (C_NORD | C_FAILED | C_STOPREAD))) || 
	 ((c->flags & C_WANTWR) && !(c->flags & (C_NOWR | C_FAILED))) || 
	 ((c->flags & C_REPARSE) && (c->status == conn_expect_query || c->status == conn_reading_query || c->status == conn_wait_answer || c->status == conn_reading_answer))
	 ) {

    if (c->error) {
      break;
    }

    if (out_total_processed_bytes (c) + out_total_unprocessed_bytes (c) > 0) {
      res = c->type->writer (c);   // if res > 0, then writer has cleared C_WANTRD and set C_NOWR
    }

    res = c->type->reader (c);  
    /* res = 0 if ok; 
       res != 0 on error, or if need to receive more bytes before proceeding (almost equal to c->skip_bytes);
         for conn_wait_net or conn_wait_aio (i.e. if further inbound processing is stalled), 
         we get res=NEED_MORE_BYTES.
       All available bytes have been already read into c->In.
       If we have run out of buffers for c->In, c->error = -1, res = -1. 
       As much output bytes have been encrypted as possible.
    */
    vkprintf (2, "server_reader=%d, ready=%02x, state=%02x\n", res, c->ev->ready, c->ev->state);
    if (res || c->skip_bytes) {
      /* we have processed as much inbound queries as possible, leaving main loop */
      break; 
    }

    if (out_total_processed_bytes (c) > 0) {
      c->flags |= C_WANTWR;
      res = c->type->writer(c);
    }
  }

  if (c->flags & C_DFLUSH) {
    c->flags &= ~C_DFLUSH;
    c->type->flush (c);
    if (c->crypto && out_total_unprocessed_bytes (c) > 0) {
      assert (c->type->crypto_encrypt_output (c) >= 0);      
    }
  }

  if (out_total_processed_bytes (c) > 0) {
    c->flags |= C_WANTWR;
    c->type->writer(c);
    if (!(c->flags & C_RAWMSG)) {
      free_unused_buffers(&c->In);
      free_unused_buffers(&c->Out);
    }
  }

  if (c->error || c->status == conn_error || (c->status == conn_write_close && !(c->flags & C_WANTWR)) || (c->flags & C_FAILED)) {
    vkprintf (1, "socket %d: closing and cleaning (error code=%d)\n", c->fd, c->error);

    if (c->status != conn_connecting) {
      active_connections--;
      if (c->flags & C_SPECIAL) {
	close_special_connection (c);
      }
    }
    c->type->close (c, 0);
    clear_connection_timeout (c);

    memset (c, 0, sizeof (struct connection));
    ev->data = 0;
    // close (fd);
    return EVA_DESTROY;
  }

  //// cond_disable_qack (c);

  c->flags &= inconn_mask;

  vkprintf (2, "END processing connection %d, status=%d, flags=%d, pending=%d\n", c->fd, c->status, c->flags, c->pending_queries);

  return compute_conn_events (c);
}

int server_read_write_gateway (int fd, void *data, event_t *ev) {
  struct connection *c = (struct connection *) data;
  assert (c);
  assert (c->type);

  if (ev->ready & EVT_FROM_EPOLL) {
    // update C_NORD / C_NOWR only if we arrived from epoll()
    ev->ready &= ~EVT_FROM_EPOLL;
    c->flags &= ~C_NORW;
    if ((ev->state & EVT_READ) && !(ev->ready & EVT_READ)) {
      c->flags |= C_NORD;
    }
    if ((ev->state & EVT_WRITE) && !(ev->ready & EVT_WRITE)) {
      c->flags |= C_NOWR;
    }
  }

  return c->type->run (c);
}

int conn_timer_wakeup_gateway (event_timer_t *et) {
  struct connection *c = (struct connection *) (((char *) et) - offsetof(struct connection, timer));
  vkprintf (2, "ALARM: awakening connection %d at %p, status=%d, pending=%d\n", c->fd, c, c->status, c->pending_queries);
  c->flags |= C_ALARM;
  put_event_into_heap (c->ev);
  return 0;
}

int set_connection_timeout (struct connection *c, double timeout) {
  c->timer.wakeup = conn_timer_wakeup_gateway;
  c->flags &= ~C_ALARM;
  if (timeout > 0) {
    c->timer.wakeup_time = precise_now + timeout;
    return insert_event_timer (&c->timer);
  } else {
    c->timer.wakeup_time = 0;
    return remove_event_timer (&c->timer);
  }
}

int clear_connection_timeout (struct connection *c) {
  c->flags &= ~C_ALARM;
  c->timer.wakeup_time = 0;
  return remove_event_timer (&c->timer);
}

int fail_connection (struct connection *c, int err) {
  if (!(c->flags & C_FAILED)) {
    total_failed_connections++;
    if (c->status == conn_connecting) {
      total_connect_failures++;
    }
  }
  c->flags |= C_FAILED;
  c->flags &= ~(C_WANTRD | C_WANTWR);
  if (c->status == conn_connecting) {
    c->target->active_outbound_connections++;
    active_outbound_connections++;
    active_connections++;
  }
  c->status = conn_error;
  if (c->error >= 0) {
    c->error = err;
  }
  put_event_into_heap (c->ev);
  return 0;
}

int flush_connection_output (struct connection *c) {
  if (out_total_processed_bytes (c) + out_total_unprocessed_bytes (c) > 0) {
    c->flags |= C_WANTWR;
    int res = c->type->writer (c);
    if (out_total_processed_bytes (c) > 0 && !(c->flags & C_INCONN)) {
      epoll_insert (c->fd, compute_conn_events (c));
    }
    return res;
  } else {
    return 0;
  }
}

int flush_later (struct connection *c) {
  if (out_total_processed_bytes (c) + out_total_unprocessed_bytes (c) > 0) {
    if (c->flags & C_DFLUSH) {
      return 1;
    }
    c->flags |= C_DFLUSH;
    if (!(c->flags & C_INCONN)) {
      put_event_into_heap (c->ev);
    }
    return 2;
  }
  return 0;
}

int accept_new_connections (struct connection *cc) {
  char buf[64], buf2[64];
  union sockaddr_in46 peer, self;
  unsigned peer_addrlen, self_addrlen;
  int cfd, acc = 0, flags;
  struct connection *c;
  event_t *ev;

  assert (cc->basic_type == ct_listen);
  do {
    peer_addrlen = sizeof (peer);
    self_addrlen = sizeof (self);
    memset (&peer, 0, sizeof (peer));
    memset (&self, 0, sizeof (self));
    cfd = accept (cc->fd, (struct sockaddr *) &peer, &peer_addrlen);
    if (cfd < 0) {
      if (!acc) {
	vkprintf (errno == EAGAIN ? 1 : 0, "accept(%d) unexpectedly returns %d: %m\n", cc->fd, cfd);
      }
      break;
    }
    acc++;
    inbound_connections_accepted++;
    getsockname (cfd, (struct sockaddr *) &self, &self_addrlen);
    assert (cfd < MAX_EVENTS);
    ev = Events + cfd;
    assert (peer_addrlen == self_addrlen);
    if (cc->flags & C_IPV6) {
      assert (peer_addrlen == sizeof (struct sockaddr_in6));
      assert (peer.a6.sin6_family == AF_INET6);
      assert (self.a6.sin6_family == AF_INET6);
    } else {
      assert (peer_addrlen == sizeof (struct sockaddr_in));
      assert (peer.a4.sin_family == AF_INET);
      assert (self.a4.sin_family == AF_INET);
    }
    // memcpy (&ev->peer, &peer, sizeof(peer));
    if (peer.a4.sin_family == AF_INET) {
      vkprintf (1, "accepted incoming connection of type %s at %s:%d -> %s:%d, fd=%d\n", cc->type->title, conv_addr (peer.a4.sin_addr.s_addr, buf), ntohs (peer.a4.sin_port), conv_addr (self.a4.sin_addr.s_addr, buf2), ntohs (self.a4.sin_port), cfd);
    } else {
      vkprintf (1, "accepted incoming ipv6 connection of type %s at [%s]:%d -> [%s]:%d, fd=%d\n", cc->type->title, conv_addr6 (peer.a6.sin6_addr.s6_addr, buf), ntohs (peer.a6.sin6_port), conv_addr6 (self.a6.sin6_addr.s6_addr, buf2), ntohs (self.a6.sin6_port), cfd);
    }
    if ((flags = fcntl (cfd, F_GETFL, 0) < 0) || fcntl (cfd, F_SETFL, flags | O_NONBLOCK) < 0) {
      kprintf ("cannot set O_NONBLOCK on accepted socket %d: %m\n", cfd);
      close (cfd);
      continue;
    }
    if (cfd >= MAX_CONNECTIONS || (cfd >= maxconn && maxconn)) {
      close (cfd);
      continue;
    }
    if (cfd > max_connection) {
      max_connection = cfd;
    }
    flags = 1;
    setsockopt (cfd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof (flags));
    if (tcp_maximize_buffers) {
      maximize_sndbuf (cfd, 0);
      maximize_rcvbuf (cfd, 0);
    }

    c = Connections + cfd;
    memset (c, 0, sizeof (struct connection));
    c->fd = cfd;
    c->ev = ev;
    c->generation = ++conn_generation;
    c->flags = C_WANTRD;
    if ((cc->flags & C_RAWMSG) || (cc->type->flags & C_RAWMSG)) {
      c->flags |= C_RAWMSG;
      c->In.state = c->Out.state = 0;
      rwm_init (&c->in, 0); 
      rwm_init (&c->out, 0); 
      rwm_init (&c->in_u, 0); 
      rwm_init (&c->out_p, 0); 
    } else {
      c->in.magic = c->out.magic = 0;
      init_builtin_buffer (&c->In, c->in_buff, BUFF_SIZE);
      init_builtin_buffer (&c->Out, c->out_buff, BUFF_SIZE);
    }
    c->timer.wakeup = conn_timer_wakeup_gateway;
    c->type = cc->type;
    c->extra = cc->extra;
    c->basic_type = ct_inbound;
    c->status = conn_expect_query;
    if (peer.a4.sin_family == AF_INET) {
      c->our_ip = ntohl (self.a4.sin_addr.s_addr);
      c->our_port = ntohs (self.a4.sin_port);
      c->remote_ip = ntohl (peer.a4.sin_addr.s_addr);
      c->remote_port = ntohs (peer.a4.sin_port);
    } else if (is_4in6 (peer.a6.sin6_addr.s6_addr)) {
      assert (is_4in6 (self.a6.sin6_addr.s6_addr));
      c->our_ip = ntohl (extract_4in6 (self.a6.sin6_addr.s6_addr));
      c->our_port = ntohs (self.a6.sin6_port);
      c->remote_ip = ntohl (extract_4in6 (peer.a6.sin6_addr.s6_addr));
      c->remote_port = ntohs (peer.a6.sin6_port);
    } else {
      c->our_port = ntohs (self.a6.sin6_port);
      c->remote_port = ntohs (peer.a6.sin6_port);
      memcpy (c->our_ipv6, self.a6.sin6_addr.s6_addr, 16);
      memcpy (c->remote_ipv6, peer.a6.sin6_addr.s6_addr, 16);
      c->flags |= C_IPV6;
    }
    c->first_query = c->last_query = (struct conn_query *) c;
    vkprintf (2, "accepted incoming connection of type %s at %s:%d -> %s:%d, fd=%d\n", c->type->title, show_remote_ip (c), c->remote_port, show_our_ip (c), c->our_port, cfd);
    if (c->type->init_accepted (c) >= 0) {
      epoll_sethandler (cfd, 0, server_read_write_gateway, c);
      epoll_insert (cfd, (c->flags & C_WANTRD ? EVT_READ : 0) | (c->flags & C_WANTWR ? EVT_WRITE : 0) | EVT_SPEC);
      active_connections++;
      c->listening = cc->fd;
      c->listening_generation = cc->generation;
      if (cc->flags & C_SPECIAL) {
	c->flags |= C_SPECIAL;
	if (active_special_connections >= max_special_connections) {
	  kprintf ("ERROR: forced to accept connection when special connections limit was reached (%d of %d)\n", active_special_connections, max_special_connections);
	}
	if (++active_special_connections >= max_special_connections) {
	  return EVA_REMOVE;
	}
      }
      if ((cc->flags & C_NOQACK)) {
	c->flags |= C_NOQACK;
	// disable_qack (c->fd);
      }
      c->window_clamp = cc->window_clamp;
      if (c->window_clamp) {
	if (setsockopt (cfd, IPPROTO_TCP, TCP_WINDOW_CLAMP, &c->window_clamp, 4) < 0) {
	  vkprintf (0, "error while setting window size for socket %d to %d: %m\n", cfd, c->window_clamp);
	} else {
	  int t1 = -1, t2 = -1;
	  socklen_t s1 = 4, s2 = 4;
	  getsockopt (cfd, IPPROTO_TCP, TCP_WINDOW_CLAMP, &t1, &s1);
	  getsockopt (cfd, SOL_SOCKET, SO_RCVBUF, &t2, &s2);
	  vkprintf (2, "window clamp for socket %d is %d, receive buffer is %d\n", cfd, t1, t2);
	}
      }
    } else {
      if (c->flags & C_RAWMSG) {
	rwm_free (&c->in);
	rwm_free (&c->out);
	rwm_free (&c->in_u);
	rwm_free (&c->out_p);
      }
      c->basic_type = ct_none;
      close (cfd);
    }
  } while (1);
  return EVA_CONTINUE;
}

int accept_new_connections_gateway (int fd, void *data, event_t *ev) {
  struct connection *cc = data;
  assert (cc->basic_type == ct_listen);
  assert (cc->type);
  return cc->type->accept (cc);
}

int server_check_ready (struct connection *c) {
  if (c->status == conn_none || c->status == conn_connecting) {
    return c->ready = cr_notyet;
  }
  if (c->status == conn_error || c->ready == cr_failed) {
    return c->ready = cr_failed;
  }
  return c->ready = cr_ok;
}


int server_noop (struct connection *c) {
  return 0;
}

int server_failed (struct connection *c) {
  kprintf ("connection %d: call to pure virtual method\n", c->fd);
  assert (0);
  return -1;
}

int check_conn_functions (conn_type_t *type) {
  if (type->magic != CONN_FUNC_MAGIC) {
    return -1;
  }
  if (!type->title) {
    type->title = "(unknown)";
  }
  if (!type->run) {
    type->run = server_read_write;
  }
  if (!type->accept) {
    type->accept = accept_new_connections;
  }
  if (!type->init_accepted) {
    type->init_accepted = server_noop;
  }
  if (!type->free_buffers) {
    type->free_buffers = free_connection_buffers;
  }
  if (!type->close) {
    type->close = server_close_connection;
  }
  if (!type->reader) {
    type->reader = server_reader;
    if (!type->parse_execute) {
      return -1;
    }
  }
  if (!type->writer) {
    type->writer = server_writer;
  }
  if (!type->init_outbound) {
    type->init_outbound = server_noop;
  }
  if (!type->wakeup) {
    type->wakeup = server_noop;
  }
  if (!type->alarm) {
    type->alarm = server_noop;
  }
  if (!type->connected) {
    type->connected = server_noop;
  }
  if (!type->flush) {
    type->flush = server_noop;
  }
  if (!type->check_ready) {
    type->check_ready = server_check_ready;
  }
  return 0;
}

int init_listening_connection_ext (int fd, conn_type_t *type, void *extra, int mode, int prio) {
  if (check_conn_functions (type) < 0) {
    return -1;
  }
  if (fd >= MAX_CONNECTIONS) {
    return -1;
  }
  if (fd > max_connection) {
    max_connection = fd;
  }
  struct connection *c = Connections + fd;

  memset (c, 0, sizeof (struct connection));

  c->fd = fd;
  c->type = type;
  c->extra = extra;
  c->basic_type = ct_listen;
  c->status = conn_listen;

  if ((mode & SM_LOWPRIO)) {
    prio = 10;
  }

  epoll_sethandler (fd, prio, accept_new_connections_gateway, c);

  //  if (!(mode & SM_SPECIAL) || active_special_connections < max_special_connections) {
  epoll_insert (fd, EVT_RWX | EVT_LEVEL);
  //  }

  if ((mode & SM_SPECIAL)) {
    c->flags |= C_SPECIAL;
  }

  if ((mode & SM_NOQACK)) {
    c->flags |= C_NOQACK;
    disable_qack (c->fd);
  }

  if ((mode & SM_IPV6)) {
    c->flags |= C_IPV6;
  }

  if ((mode & SM_RAWMSG)) {
    c->flags |= C_RAWMSG;
  }

  listening_connections++;

  return 0;
}

int init_listening_connection (int fd, conn_type_t *type, void *extra) {
  return init_listening_connection_ext (fd, type, extra, 0, -10);
}

int init_listening_tcpv6_connection (int fd, conn_type_t *type, void *extra, int mode) {
  return init_listening_connection_ext (fd, type, extra, mode, -10);
}
  

int default_parse_execute (struct connection *c) {
  return 0;
}


// Connection target handling functions

struct conn_target Targets[MAX_TARGETS];
struct conn_target *HTarget[PRIME_TARGETS];
int allocated_targets = 0;

struct conn_target **find_target (struct in_addr ad, int port, conn_type_t *type) {
  assert (ad.s_addr);
  unsigned h1 = ((unsigned long) type * 0xabacaba + ad.s_addr) % PRIME_TARGETS;
  unsigned h2 = ((unsigned long) type * 0xdabacab + ad.s_addr) % (PRIME_TARGETS - 1);
  h1 = (h1 * 239 + port) % PRIME_TARGETS;
  h2 = (h2 * 17 + port) % (PRIME_TARGETS - 1) + 1;
  while (HTarget[h1]) {
    if (HTarget[h1]->target.s_addr == ad.s_addr &&
        HTarget[h1]->port == port &&
	HTarget[h1]->type == type) {
      return HTarget + h1;
    }
    if ((h1 += h2) >= PRIME_TARGETS) {
      h1 -= PRIME_TARGETS;
    }
  }
  return HTarget + h1;
}


struct conn_target **find_target_ipv6 (unsigned char ad_ipv6[16], int port, conn_type_t *type) {
  assert (*(long long *)ad_ipv6 || ((long long *) ad_ipv6)[1]);
  unsigned h1 = ((unsigned long) type * 0xabacaba) % PRIME_TARGETS;
  unsigned h2 = ((unsigned long) type * 0xdabacab) % (PRIME_TARGETS - 1);
  int i;
  for (i = 0; i < 4; i++) {
    h1 = ((unsigned long long) h1 * 17239 + ((unsigned *) ad_ipv6)[i]) % PRIME_TARGETS;
    h2 = ((unsigned long long) h2 * 23917 + ((unsigned *) ad_ipv6)[i]) % (PRIME_TARGETS - 1);
  }
  h1 = (h1 * 239 + port) % PRIME_TARGETS;
  h2 = (h2 * 17 + port) % (PRIME_TARGETS - 1) + 1;
  while (HTarget[h1]) {
    if (
	((long long *)HTarget[h1]->target_ipv6)[1] == ((long long *)ad_ipv6)[1] &&
	*(long long *)HTarget[h1]->target_ipv6 == *(long long *)ad_ipv6 &&
        HTarget[h1]->port == port &&
	HTarget[h1]->type == type && !HTarget[h1]->target.s_addr) {
      return HTarget + h1;
    }
    if ((h1 += h2) >= PRIME_TARGETS) {
      h1 -= PRIME_TARGETS;
    }
  }
  return HTarget + h1;
}


struct conn_target *create_target (struct conn_target *source, int *was_created) {
  struct conn_target **targ = 
    source->target.s_addr ? 
    find_target (source->target, source->port, source->type) :
    find_target_ipv6 (source->target_ipv6, source->port, source->type);
  struct conn_target *t = *targ;
  if (t) {
    assert (t->refcnt >= 0);
    t->min_connections = source->min_connections;
    t->max_connections = source->max_connections;
    t->reconnect_timeout = source->reconnect_timeout;
    t->refcnt++;
    if (was_created) {
      *was_created = 0;
    }
  } else {
    assert (allocated_targets < MAX_TARGETS);
    t = *targ = &Targets[allocated_targets++];
    memcpy (t, source, sizeof (*source));
    t->first_conn = t->last_conn = (struct connection *) t;
    t->first_query = t->last_query = (struct conn_query *) t;
    t->refcnt = 1;
    if (was_created) {
      *was_created = 1;
    }
  }
  return t;
}

int destroy_target (struct conn_target *S) {
  assert (S);
  assert (S->type);
  assert (S->refcnt > 0);
  return --S->refcnt;
}


int create_new_connections (struct conn_target *S) {
  int count = 0, good_c = 0, bad_c = 0, stopped_c = 0, need_c;
  struct connection *c, *h;
  event_t *ev;

  assert (S->refcnt >= 0);

  for (c = S->first_conn; c != (struct connection *) S; c = c->next) {
    int cr = c->type->check_ready (c); 
    switch (cr) {
    case cr_notyet:
    case cr_busy:
      break;
    case cr_ok:
      good_c++;
      break;
    case cr_stopped:
      stopped_c++;
      break;
    case cr_failed:
      bad_c++;
      break;
    default:
      assert (0);
    }
  }

  S->ready_outbound_connections = good_c;
  need_c = S->min_connections + bad_c + ((stopped_c + 1) >> 1);
  if (need_c > S->max_connections) {
    need_c = S->max_connections;
  }

  if (precise_now < S->next_reconnect && !S->active_outbound_connections) {
    return 0;
  }

  while (S->outbound_connections < need_c) {
    if (verbosity > 0) {
      if (S->target.s_addr) {
	fprintf (stderr, "Creating NEW connection to %s:%d\n", inet_ntoa (S->target), S->port);
      } else {
	fprintf (stderr, "Creating NEW ipv6 connection to [%s]:%d\n", show_ipv6 (S->target_ipv6), S->port);
      }
    }
    int cfd = S->target.s_addr ? client_socket (S->target.s_addr, S->port, 0) : client_socket_ipv6 (S->target_ipv6, S->port, SM_IPV6);
    if (cfd < 0 && verbosity > 0) {
      compute_next_reconnect (S);
      if (verbosity > 0) {
	if (S->target.s_addr) {
	  fprintf (stderr, "error connecting to %s:%d: %m", inet_ntoa (S->target), S->port);
	} else {
	  fprintf (stderr, "error connecting to [%s]:%d\n", show_ipv6 (S->target_ipv6), S->port);
	}
      }
      return count;
    }
    if (cfd >= MAX_EVENTS || cfd >= MAX_CONNECTIONS) {
      close (cfd);
      compute_next_reconnect (S);
      if (verbosity > 0) {
	if (S->target.s_addr) {
	  fprintf (stderr, "out of sockets when connecting to %s:%d", inet_ntoa(S->target), S->port);
	} else {
	  fprintf (stderr, "out of sockets when connecting to [%s]:%d\n", show_ipv6 (S->target_ipv6), S->port);
	}
      }
      return count;
    }
    
    if (cfd > max_connection) {
      max_connection = cfd;
    }
    ev = Events + cfd;
    c = Connections + cfd;
    memset (c, 0, sizeof (struct connection));
    c->fd = cfd;
    c->ev = ev;
    c->target = S;
    c->generation = ++conn_generation;
    c->flags = C_WANTWR;

    if (S->type->flags & C_RAWMSG) {
      c->flags |= C_RAWMSG;
      c->In.state = c->Out.state = 0;
      rwm_init (&c->in, 0); 
      rwm_init (&c->out, 0); 
      rwm_init (&c->in_u, 0); 
      rwm_init (&c->out_p, 0); 
    } else {
      c->in.magic = c->out.magic = 0;
      init_builtin_buffer (&c->In, c->in_buff, BUFF_SIZE);
      init_builtin_buffer (&c->Out, c->out_buff, BUFF_SIZE);
    }

    c->timer.wakeup = conn_timer_wakeup_gateway;
    c->type = S->type;
    c->extra = S->extra;
    c->basic_type = ct_outbound;
    c->status = conn_connecting;
    c->first_query = c->last_query = (struct conn_query *) c;

    if (S->target.s_addr) {
      static struct sockaddr_in self;
      unsigned self_addrlen;
      memset (&self, 0, self_addrlen = sizeof (self));
      getsockname (c->fd, (struct sockaddr *) &self, &self_addrlen);
      c->our_ip = ntohl (self.sin_addr.s_addr);
      c->our_port = ntohs (self.sin_port);
      c->remote_ip = ntohl (S->target.s_addr);
      c->remote_port = S->port;
      vkprintf (2, "Created new outbound connection %s:%d -> %s:%d\n", show_our_ip (c), c->our_port, show_remote_ip (c), c->remote_port);
    } else {
      c->flags |= C_IPV6;
      static struct sockaddr_in6 self;
      unsigned self_addrlen;
      memset (&self, 0, self_addrlen = sizeof (self));
      getsockname (c->fd, (struct sockaddr *) &self, &self_addrlen);
      memcpy (c->our_ipv6, self.sin6_addr.s6_addr, 16);
      memcpy (c->remote_ipv6, S->target_ipv6, 16);
      c->our_port = ntohs (self.sin6_port);
      c->remote_port = S->port;
      vkprintf (2, "Created new outbound ipv6 connection [%s]:%d -> [%s]:%d\n", show_ipv6 (c->our_ipv6), c->our_port, show_ipv6 (c->remote_ipv6), c->remote_port);
    }

    if (c->type->init_outbound (c) >= 0) {
      epoll_sethandler (cfd, 0, server_read_write_gateway, c);
      epoll_insert (cfd, (c->flags & C_WANTRD ? EVT_READ : 0) | (c->flags & C_WANTWR ? EVT_WRITE : 0) | EVT_SPEC);
      outbound_connections++;
      S->outbound_connections++;
      outbound_connections_created++;
      count++;
    } else {
      if (c->flags & C_RAWMSG) {
	rwm_free (&c->in);
	rwm_free (&c->out);
	rwm_free (&c->in_u);
	rwm_free (&c->out_p);
      }
      c->basic_type = ct_none;
      close (cfd);
      compute_next_reconnect (S);
      return count;
    }

    h = (struct connection *)S;
    c->next = h;
    c->prev = h->prev;
    h->prev->next = c;
    h->prev = c;

    if (verbosity > 0) {
      if (c->flags & C_IPV6) {
	fprintf (stderr, "outbound ipv6 connection: handle %d to [%s]:%d\n", c->fd, show_ipv6 (S->target_ipv6), S->port);
      } else {
	fprintf (stderr, "outbound connection: handle %d to %s:%d\n", c->fd, inet_ntoa (S->target), S->port);
      }
    }
  }
  return count;
}

int create_all_outbound_connections (void) {
  int count = 0, i;
  get_utime_monotonic ();
  ready_outbound_connections = ready_targets = 0;
  for (i = 0; i < allocated_targets; i++) {
    if (Targets[i].type && Targets[i].refcnt > 0) {
      count += create_new_connections (&Targets[i]);
      if (Targets[i].ready_outbound_connections) {
        ready_outbound_connections += Targets[i].ready_outbound_connections;
        ready_targets++;
      }
    }
  }
  return count;    
}

int conn_rerun_later (struct connection *c, int ts_delta) {
  return put_event_into_heap_tail (c->ev, ts_delta);
}

int conn_event_wakeup_gateway (event_timer_t *et) {
  struct conn_query *q = (struct conn_query *) (((char *) et) - offsetof(struct conn_query, timer));
  vkprintf (2, "ALARM: awakened pending query %p [%d -> %d]\n", q, q->requester ? q->requester->fd : -1, q->outbound ? q->outbound->fd : -1);
  return q->cq_type->wakeup (q);
}


int insert_conn_query_into_list (struct conn_query *q, struct conn_query *h) {
  q->next = h;
  q->prev = h->prev;
  h->prev->next = q;
  h->prev = q;
  assert (q->requester);
  q->req_generation = q->requester->generation;
  q->requester->pending_queries++;
  q->timer.h_idx = 0;
  q->timer.wakeup = conn_event_wakeup_gateway;
  if (q->timer.wakeup_time > 0) {
    insert_event_timer (&q->timer);
  }
  return 0;
}

int push_conn_query_into_list (struct conn_query *q, struct conn_query *h) {
  q->prev = h;
  q->next = h->next;
  h->next->prev = q;
  h->next = q;
  assert (q->requester);
  q->req_generation = q->requester->generation;
  q->requester->pending_queries++;
  q->timer.h_idx = 0;
  q->timer.wakeup = conn_event_wakeup_gateway;
  if (q->timer.wakeup_time > 0) {
    insert_event_timer (&q->timer);
  }
  return 0;
}

int insert_conn_query (struct conn_query *q) {
  vkprintf (2, "insert_conn_query(%p)\n", q);
  struct conn_query *h = (struct conn_query *) q->outbound;
  return insert_conn_query_into_list (q, h);
}

int push_conn_query (struct conn_query *q) {
  vkprintf (2, "push_conn_query(%p)\n", q);
  struct conn_query *h = (struct conn_query *) q->outbound;
  return push_conn_query_into_list (q, h);
}


int delete_conn_query (struct conn_query *q) {
  vkprintf (2, "delete_conn_query (%p)\n", q);
  q->next->prev = q->prev;
  q->prev->next = q->next;
  if (q->requester && q->requester->generation == q->req_generation) {
    if (!--q->requester->pending_queries) {
      /* wake up master */
      vkprintf (2, "socket %d was the last one, waking master %d\n", q->outbound ? q->outbound->fd : -1, q->requester->fd);
      if (!q->requester->ev->in_queue) {
	put_event_into_heap (q->requester->ev);
      }
    }
  }
  q->requester = 0;
  q->outbound = 0;
  if (q->timer.h_idx) {
    remove_event_timer (&q->timer);
  }
  return 0;
}

//arseny30: added for php-engine
int delete_conn_query_from_requester (struct conn_query *q) {
  vkprintf (2, "delete_conn_query_from_requester (%p)\n", q);
  if (q->requester && q->requester->generation == q->req_generation) {
    if (!--q->requester->pending_queries) {
      /* wake up master */
      vkprintf (2, "socket %d was the last one, waking master %d\n", q->outbound ? q->outbound->fd : -1, q->requester->fd);
      if (!q->requester->ev->in_queue) {
        put_event_into_heap (q->requester->ev);
      }
    }
  }
  q->requester = 0;
  if (q->timer.h_idx) {
    remove_event_timer (&q->timer);
  }
  return 0;
}

void dump_connection_buffers (struct connection *c) {
  fprintf (stderr, "Dumping buffers of connection %d\nINPUT buffers of %d:\n", c->fd, c->fd);
  dump_buffers (&c->In);
  fprintf (stderr, "OUTPUT buffers of %d:\n", c->fd);
  dump_buffers (&c->Out);
  if (c->Tmp) {
    fprintf (stderr, "TEMP buffers of %d:\n", c->fd);
    dump_buffers (c->Tmp);
  }
  fprintf (stderr, "--- END (dumping buffers of connection %d) ---\n", c->fd);
}

