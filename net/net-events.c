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

#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pwd.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <ifaddrs.h>

#include "net-events.h"
#include "server-functions.h"

extern int verbosity;

/*
 * generic events (epoll-based) machinery
 */
int now;
double precise_now = -1;

double tot_idle_time, a_idle_time, a_idle_quotient;

volatile long long pending_signals;

event_t Events[MAX_EVENTS];
int min_timeout, epoll_fd;
long long ev_timestamp;

event_t *ev_heap[MAX_EVENTS+1];
int ev_heap_size;

event_timer_t *et_heap[MAX_EVENT_TIMERS+1];
int et_heap_size;

int epoll_remove (int fd);

double get_utime_monotonic (void) {
  struct timespec T;
  double res;
#if _POSIX_TIMERS
  assert (clock_gettime(CLOCK_MONOTONIC, &T) >= 0);
  res = T.tv_sec + (double) T.tv_nsec * 1e-9;
#else
#error "No high-precision clock"
  res = time();
#endif
  precise_now = res;
  return res;
}

int init_epoll (void) {
  int fd;
  if (epoll_fd) return 0;
  Events[0].fd = -1;
  fd = epoll_create (MAX_EVENTS);
  if (fd < 0) {
    perror ("epoll_create()");
    return -1;
  }
  epoll_fd = fd;
  return fd;
}

int cmp_ev (event_t *ev1, event_t *ev2) {
  int x = ev1->priority - ev2->priority;
  long long t;
  if (x) return x;
  t = ev1->timestamp - ev2->timestamp;
  return t < 0 ? -1 : (t ? 1 : 0);
}

event_t *pop_heap_head (void) {
  int i, j, N = ev_heap_size;
  event_t *ev, *x, *y;
  if (!N) return 0;
  ev = ev_heap[1];
  assert (ev && ev->in_queue == 1);
  ev->in_queue = 0;
  if (!--ev_heap_size) return ev;
  x = ev_heap[N--];
  i = 1;
  while (1) {
    j = (i << 1);
    if (j > N) break;
    if (j < N && cmp_ev(ev_heap[j], ev_heap[j+1]) > 0) j++;
    y = ev_heap[j];
    if (cmp_ev(x, y) <= 0) break;
    ev_heap[i] = y;
    y->in_queue = i;
    i = j;
  } 
  ev_heap[i] = x;
  x->in_queue = i;
  return ev;
}

int remove_event_from_heap (event_t *ev, int allow_hole) {
  int v = ev->fd, i, j, N = ev_heap_size;
  event_t *x;
  assert (v >= 0 && v < MAX_EVENTS && Events + v == ev);
  i = ev->in_queue;
  if (!i) return 0;
  assert (i > 0 && i <= N);
  ev->in_queue = 0;
  do {
    j = (i << 1);
    if (j > N) break;
    if (j < N && cmp_ev(ev_heap[j+1], ev_heap[j]) < 0) j++;
    ev_heap[i] = x = ev_heap[j];
    x->in_queue = i;
    i = j;
  } while(1);
  if (allow_hole) {
    ev_heap[i] = 0;
    return i;
  }
  if (i < N) {
    ev = ev_heap[N];
    ev_heap[N] = 0;
    while (i > 1) {
      j = (i >> 1);
      x = ev_heap[j];
      if (cmp_ev(x,ev) <= 0) break;
      ev_heap[i] = x;
      x->in_queue = i;
      i = j;
    }
    ev_heap[i] = ev;
    ev->in_queue = i;
  }
  ev_heap_size--;
  return N;
}

int put_event_into_heap (event_t *ev) {
  int v = ev->fd, i, j;
  event_t *x;
  assert (v >= 0 && v < MAX_EVENTS && Events + v == ev);
  i = ev->in_queue ? remove_event_from_heap (ev, 1) : ++ev_heap_size;
  assert (i <= MAX_EVENTS);
  while (i > 1) {
    j = (i >> 1);
    x = ev_heap[j];
    if (cmp_ev(x,ev) <= 0) break;
    ev_heap[i] = x;
    x->in_queue = i;
    i = j;
  }
  ev_heap[i] = ev;
  ev->in_queue = i;
  return i;
}

int put_event_into_heap_tail (event_t *ev, int ts_delta) {
  ev->timestamp = ev_timestamp + ts_delta;
  return put_event_into_heap (ev);
}

int epoll_sethandler (int fd, int prio, event_handler_t handler, void *data) {
  event_t *ev;
  assert (fd >= 0 && fd < MAX_EVENTS);
  ev = Events + fd;
  if (ev->fd != fd) {
    memset (ev, 0, sizeof(event_t));
    ev->fd = fd;
  }
  ev->priority = prio;
  ev->data = data;
  ev->work = handler;
  return 0;
}

int epoll_conv_flags (int flags) {
  int r = EPOLLERR;
  if (flags == 0x204) {
    return EPOLLIN;
  }
  if (!flags) {
    return 0;
  }
  if (!(flags & EVT_NOHUP)) {
    r |= EPOLLHUP;
  }
  if (flags & EVT_READ) {
    r |= EPOLLIN;
  }
  if (flags & EVT_WRITE) {
    r |= EPOLLOUT;
  }
  if (flags & EVT_SPEC) {
    r |= EPOLLRDHUP | EPOLLPRI;
  }
  if (!(flags & EVT_LEVEL)) {
    r |= EPOLLET;
  }
  return r;
}

int epoll_unconv_flags (int f) {
  int r = EVT_FROM_EPOLL;
  if (f & (EPOLLIN | EPOLLERR)) {
    r |= EVT_READ;
  }
  if (f & EPOLLOUT) {
    r |= EVT_WRITE;
  }
  if (f & (EPOLLRDHUP | EPOLLPRI)) {
    r |= EVT_SPEC;
  }
  return r;
}

int epoll_insert (int fd, int flags) {
  event_t *ev;
  int ef;
  struct epoll_event ee;
  if (!flags) {
    return epoll_remove (fd);
  }
  assert (fd >= 0 && fd < MAX_EVENTS);
  ev = Events + fd;
  if (ev->fd != fd) {
    memset (ev, 0, sizeof(event_t));
    ev->fd = fd;
  }
  flags &= EVT_NEW | EVT_NOHUP | EVT_LEVEL | EVT_RWX;
  ev->ready = 0; // !!! this bugfix led to some AIO-related bugs, now fixed with the aid of C_REPARSE flag
  if ((ev->state & (EVT_LEVEL | EVT_RWX | EVT_IN_EPOLL)) == flags + EVT_IN_EPOLL) {
    return 0;
  }
  ev->state = (ev->state & ~(EVT_LEVEL | EVT_RWX)) | (flags & (EVT_LEVEL | EVT_RWX));
  ef = epoll_conv_flags (flags);
  if (ef != ev->epoll_state || (flags & EVT_NEW) || !(ev->state & EVT_IN_EPOLL)) {
    ee.events = ef;
    ee.data.fd = fd; 

    vkprintf (1, "epoll_ctl(%d,%d,%d,%d,%08x)\n", epoll_fd, (ev->state & EVT_IN_EPOLL) ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, ee.data.fd, ee.events);

    if (epoll_ctl (epoll_fd, (ev->state & EVT_IN_EPOLL) ? EPOLL_CTL_MOD : EPOLL_CTL_ADD, fd, &ee) < 0) {
      perror("epoll_ctl()");
    }
    ev->state |= EVT_IN_EPOLL;
  }
  return 0;
}

int epoll_remove (int fd) {
  event_t *ev;
  assert (fd >= 0 && fd < MAX_EVENTS);
  ev = Events + fd;
  if (ev->fd != fd) return -1;
  if (ev->state & EVT_IN_EPOLL) {
    ev->state &= ~EVT_IN_EPOLL;
    if (epoll_ctl (epoll_fd, EPOLL_CTL_DEL, fd, 0) < 0) {
      perror ("epoll_ctl()");
    }
  }
  return 0;
}

int epoll_close (int fd) {
  event_t *ev;
  assert (fd >= 0 && fd < MAX_EVENTS);
  ev = Events + fd;
  if (ev->fd != fd) {
    return -1;
  }
  epoll_remove (fd);
  if (ev->in_queue) {
    remove_event_from_heap (ev, 0);
  }
  memset (ev, 0, sizeof (event_t));
  ev->fd = -1;
  return 0;
}

static inline int basic_et_adjust (event_timer_t *et, int i) {
  int j;
  while (i > 1) {
    j = (i >> 1);
    if (et_heap[j]->wakeup_time <= et->wakeup_time) {
      break;
    }
    et_heap[i] = et_heap[j];
    et_heap[i]->h_idx = i;
    i = j;
  }
  j = 2*i;
  while (j <= et_heap_size) {
    if (j < et_heap_size && et_heap[j]->wakeup_time > et_heap[j+1]->wakeup_time) {
      j++;
    }
    if (et->wakeup_time <= et_heap[j]->wakeup_time) {
      break;
    }
    et_heap[i] = et_heap[j];
    et_heap[i]->h_idx = i;
    i = j;
    j <<= 1;
  }
  et_heap[i] = et;
  et->h_idx = i;
  return i;
}

int insert_event_timer (event_timer_t *et) {
  int i;
  if (et->h_idx) {
    i = et->h_idx;
    assert (i > 0 && i <= et_heap_size && et_heap[i] == et);
  } else {
    assert (et_heap_size < MAX_EVENT_TIMERS);
    i = ++et_heap_size;
  }
  return basic_et_adjust (et, i);
}

int remove_event_timer (event_timer_t *et) {
  int i = et->h_idx;
  if (!i) {
    return 0;
  }
  assert (i > 0 && i <= et_heap_size && et_heap[i] == et);
  et->h_idx = 0;

  et = et_heap[et_heap_size--];
  if (i > et_heap_size) {
    return 1;
  }
  basic_et_adjust (et, i);
  return 1;
}

int epoll_run_timers (void) {
  double wait_time;
  event_timer_t *et;
  if (!et_heap_size) {
    return 100000;
  }
  wait_time = et_heap[1]->wakeup_time - precise_now;
  if (wait_time > 0) {
    //do not remove this useful debug!
    vkprintf (2, "%d event timers, next in %.3f seconds\n", et_heap_size, wait_time);
    return (int) (wait_time*1000) + 1;
  }
  while (et_heap_size > 0 && et_heap[1]->wakeup_time <= precise_now && !pending_signals) {
    et = et_heap[1];
    assert (et->h_idx == 1);
    remove_event_timer (et);
    et->wakeup (et); 
  }
  return 0;
}

epoll_func_vector_t epoll_pre_runqueue, epoll_post_runqueue, epoll_pre_event;

int epoll_runqueue (void) {
  event_t *ev;
  int res, fd, cnt = 0;
  if (!ev_heap_size) {
    return 0;
  }

  vkprintf (2, "epoll_runqueue: %d events\n", ev_heap_size);

  if (epoll_pre_runqueue) {
    (*epoll_pre_runqueue)();
  }
  ev_timestamp += 2;
  while (ev_heap_size && (ev = ev_heap[1])->timestamp < ev_timestamp && !pending_signals) {
    pop_heap_head();
    fd = ev->fd;
    assert (ev == Events + fd && fd >= 0 && fd < MAX_EVENTS);
    if (ev->work) {
      if (epoll_pre_event) {
        (*epoll_pre_event)();
      }
      res = ev->work(fd, ev->data, ev);
    } else {
      res = EVA_REMOVE;
    }
    if (res == EVA_REMOVE || res == EVA_DESTROY || res <= EVA_ERROR) {
      remove_event_from_heap (ev, 0);
      epoll_remove (ev->fd);
      if (res == EVA_DESTROY) {
	if (!(ev->state & EVT_CLOSED)) {
	  close (ev->fd);
	}
	memset (ev, 0, sizeof(event_t));
      }
      if (res <= EVA_FATAL) {
	perror ("fatal");
	exit(1);
      }
    } else if (res == EVA_RERUN) {
      ev->timestamp = ev_timestamp;
      put_event_into_heap (ev);
    } else if (res > 0) {
      epoll_insert (fd, res & 0xf);
    } else if (res == EVA_CONTINUE) {
      ev->ready = 0;
    }
    cnt++;
  }
  if (epoll_post_runqueue) {
    (*epoll_post_runqueue)();
  }
  return cnt;
}

struct epoll_event new_ev_list[MAX_EVENTS];

int epoll_fetch_events (int timeout) {
  int fd, i;
  int res = epoll_wait (epoll_fd, new_ev_list, MAX_EVENTS, timeout);
  if (res < 0 && errno == EINTR) {
    res = 0;
  }
  if (res < 0) {
    perror ("epoll_wait()");
  }
  if (verbosity > 1 && res) {
    kprintf ("epoll_wait(%d, ...) = %d\n", epoll_fd, res);
  }
  for (i = 0; i < res; i++) {
    fd = new_ev_list[i].data.fd;
    assert (fd >= 0 && fd < MAX_EVENTS);
    event_t *ev = Events + fd;
    assert (ev->fd == fd);
    ev->ready |= epoll_unconv_flags (ev->epoll_ready = new_ev_list[i].events);
    ev->timestamp = ev_timestamp;
    put_event_into_heap (ev);
  }
  return res;
}

int epoll_work (int timeout) {
  int res;
  int timeout2 = 10000;
  if (ev_heap_size || et_heap_size) {
    now = time (0);
    get_utime_monotonic ();
    do {
      epoll_runqueue ();
      timeout2 = epoll_run_timers ();
    } while ((timeout2 <= 0 || ev_heap_size) && !pending_signals);
  }
  if (pending_signals) {
    return 0;
  }

  double epoll_wait_start = get_utime_monotonic ();

  res = epoll_fetch_events (timeout < timeout2 ? timeout : timeout2);

  double epoll_wait_time = get_utime_monotonic () - epoll_wait_start;
  tot_idle_time += epoll_wait_time;
  a_idle_time += epoll_wait_time;

  now = time (0);
  static int prev_now = 0;
  if (now > prev_now && now < prev_now + 60) {
    while (prev_now < now) {
      a_idle_time *= 100.0 / 101;
      a_idle_quotient = a_idle_quotient * (100.0/101) + 1;
      prev_now++;
    }
  } else {
    prev_now = now;
  }

  epoll_run_timers ();
  return epoll_runqueue();
}

// ------- end of definitions ----------

/*
 * end (events)
 */
  

// From memcached.c: socket functions

int new_socket (int mode, int nonblock) {
  int sfd;
  int flags;

  if ((sfd = socket (mode & SM_IPV6 ? AF_INET6 : AF_INET, mode & SM_UDP ? SOCK_DGRAM : SOCK_STREAM, 0)) == -1) {
    perror ("socket()");
    return -1;
  }

  if (mode & SM_IPV6) {
    flags = (mode & SM_IPV6_ONLY) != 0;
    if (setsockopt (sfd, IPPROTO_IPV6, IPV6_V6ONLY, &flags, 4) < 0) {
      perror ("setting IPV6_V6ONLY");
      close (sfd);
      return -1;
    }
  }

  if (!nonblock) {
    return sfd;
  }

  if ((flags = fcntl (sfd, F_GETFL, 0)) < 0 || fcntl (sfd, F_SETFL, flags | O_NONBLOCK) < 0) {
    perror ("setting O_NONBLOCK");
    close (sfd);
    return -1;
  }
  return sfd;
}


/*
 * Sets a socket's send buffer size to the maximum allowed by the system.
 */
void maximize_sndbuf (int sfd, int max) {
  socklen_t intsize = sizeof(int);
  int last_good = 0;
  int min, avg;
  int old_size;

  if (max <= 0) {
    max = MAX_UDP_SENDBUF_SIZE;
  }

  /* Start with the default size. */
  if (getsockopt (sfd, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize)) {
    if (verbosity > 0) {
      perror ("getsockopt (SO_SNDBUF)");
    }
    return;
  }

  /* Binary-search for the real maximum. */
  min = last_good = old_size;
  max = MAX_UDP_SENDBUF_SIZE;

  while (min <= max) {
    avg = ((unsigned int) min + max) / 2;
    if (setsockopt (sfd, SOL_SOCKET, SO_SNDBUF, &avg, intsize) == 0) {
      last_good = avg;
      min = avg + 1;
    } else {
      max = avg - 1;
    }
  }

  vkprintf (2, "<%d send buffer was %d, now %d\n", sfd, old_size, last_good);
}

/*
 * Sets a socket's receive buffer size to the maximum allowed by the system.
 */
void maximize_rcvbuf (int sfd, int max) {
  socklen_t intsize = sizeof(int);
  int last_good = 0;
  int min, avg;
  int old_size;

  if (max <= 0) {
    max = MAX_UDP_RCVBUF_SIZE;
  }

  /* Start with the default size. */
  if (getsockopt (sfd, SOL_SOCKET, SO_RCVBUF, &old_size, &intsize)) {
    if (verbosity > 0) {
      perror ("getsockopt (SO_RCVBUF)");
    }
    return;
  }

  /* Binary-search for the real maximum. */
  min = last_good = old_size;
  max = MAX_UDP_RCVBUF_SIZE;

  while (min <= max) {
    avg = ((unsigned int) min + max) / 2;
    if (setsockopt (sfd, SOL_SOCKET, SO_RCVBUF, &avg, intsize) == 0) {
      last_good = avg;
      min = avg + 1;
    } else {
      max = avg - 1;
    }
  }

  vkprintf (2, ">%d receive buffer was %d, now %d\n", sfd, old_size, last_good);
}

int tcp_maximize_buffers;

int server_socket (int port, struct in_addr in_addr, int backlog, int mode) {
  int sfd;
  struct linger ling = {0, 0};
  int flags = 1;

  if ((sfd = new_socket (mode, 1)) == -1) {
    return -1;
  }

  if (mode & SM_UDP) {
    maximize_sndbuf (sfd, 0);
    maximize_rcvbuf (sfd, 0);
    setsockopt (sfd, SOL_IP, IP_RECVERR, &flags, sizeof (flags));
  } else {
    setsockopt (sfd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof (flags));
    if (tcp_maximize_buffers) {
      maximize_sndbuf (sfd, 0);
      maximize_rcvbuf (sfd, 0);
    }
    setsockopt (sfd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof (flags));
    setsockopt (sfd, SOL_SOCKET, SO_LINGER, &ling, sizeof (ling));
    setsockopt (sfd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof (flags));
  }

  if (!(mode & SM_IPV6)) {
    struct sockaddr_in addr;
    memset (&addr, 0, sizeof (addr));
  
    addr.sin_family = AF_INET;
    addr.sin_port = htons (port);
    addr.sin_addr = in_addr;
    if (bind (sfd, (struct sockaddr *) &addr, sizeof (addr)) == -1) {
      perror ("bind()");
      close (sfd);
      return -1;
    }
  } else {
    struct sockaddr_in6 addr;
    memset (&addr, 0, sizeof (addr));

    addr.sin6_family = AF_INET6;
    addr.sin6_port = htons (port);
    addr.sin6_addr = in6addr_any;

    if (bind (sfd, (struct sockaddr *) &addr, sizeof (addr)) == -1) {
      perror ("bind()");
      close (sfd);
      return -1;
    }
  }
  if (!(mode & SM_UDP) && listen (sfd, backlog) == -1) {
//    perror("listen()");
    close (sfd);
    return -1;
  }
  return sfd;
}

int client_socket (in_addr_t in_addr, int port, int mode) {
  int sfd;
  struct sockaddr_in addr;
  int flags = 1;

  if (mode & SM_IPV6) {
    return -1;
  }

  if ((sfd = new_socket (mode, 1)) == -1) {
    return -1;
  }

  if (mode & SM_UDP) {
    maximize_sndbuf (sfd, 0);
    maximize_rcvbuf (sfd, 0);
    setsockopt (sfd, SOL_IP, IP_RECVERR, &flags, sizeof (flags));
  } else {
    setsockopt (sfd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof (flags));
    if (tcp_maximize_buffers) {
      maximize_sndbuf (sfd, 0);
      maximize_rcvbuf (sfd, 0);
    }
    setsockopt (sfd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof (flags));
    setsockopt (sfd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof (flags));
  }

  memset (&addr, 0, sizeof (addr));

  addr.sin_family = AF_INET;
  addr.sin_port = htons (port);
  addr.sin_addr.s_addr = in_addr;
 
  if (connect (sfd, (struct sockaddr *) &addr, sizeof (addr)) == -1 && errno != EINPROGRESS) {
    perror ("connect()");
    close (sfd);
    return -1;
  }

  return sfd;

}

int client_socket_ipv6 (const unsigned char in6_addr_ptr[16], int port, int mode) {
  int sfd;
  struct sockaddr_in6 addr;
  int flags = 1;

  if (!(mode & SM_IPV6)) {
    return -1;
  }

  if ((sfd = new_socket (mode, 1)) == -1) {
    return -1;
  }

  if (mode & SM_UDP) {
    maximize_sndbuf (sfd, 0);
    maximize_rcvbuf (sfd, 0);
  } else {
    setsockopt (sfd, SOL_SOCKET, SO_REUSEADDR, &flags, sizeof (flags));
    if (tcp_maximize_buffers) {
      maximize_sndbuf (sfd, 0);
      maximize_rcvbuf (sfd, 0);
    }
    setsockopt (sfd, SOL_SOCKET, SO_KEEPALIVE, &flags, sizeof (flags));
    setsockopt (sfd, IPPROTO_TCP, TCP_NODELAY, &flags, sizeof (flags));
  }

  memset (&addr, 0, sizeof (addr));

  addr.sin6_family = AF_INET6;
  addr.sin6_port = htons (port);
  memcpy (&addr.sin6_addr, in6_addr_ptr, 16);
 
  if (connect (sfd, (struct sockaddr *) &addr, sizeof (addr)) == -1 && errno != EINPROGRESS) {
    perror ("connect()");
    close (sfd);
    return -1;
  }

  return sfd;

}

unsigned get_my_ipv4 (void) {
  struct ifaddrs *ifa_first, *ifa;
  unsigned my_ip = 0, my_netmask = -1; 
  char *my_iface = 0;
  if (getifaddrs (&ifa_first) < 0) {
    perror ("getifaddrs()");
    return 0;
  }
  for (ifa = ifa_first; ifa; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr->sa_family != AF_INET) {
      continue;
    }
    if (!strncmp (ifa->ifa_name, "lo", 2)) {
      continue;
    }
    unsigned ip = ntohl (((struct sockaddr_in *) ifa->ifa_addr)->sin_addr.s_addr);
    unsigned mask = ntohl (((struct sockaddr_in *) ifa->ifa_netmask)->sin_addr.s_addr);
    // fprintf (stderr, "%08x %08x\t%s\n", ip, mask, ifa->ifa_name);
    if ((ip & (-1 << 24)) == (10 << 24) && mask < my_netmask) {
      my_ip = ip;
      my_netmask = mask;
      my_iface = ifa->ifa_name;
    }
  }
  vkprintf (1, "using main IP %d.%d.%d.%d/%d at interface %s\n", (my_ip >> 24), (my_ip >> 16) & 255, (my_ip >> 8) & 255, my_ip & 255,
	    __builtin_clz (~my_netmask), my_iface ?: "(none)"); 
  freeifaddrs (ifa_first);
  return my_ip;
}

/* IPv4/IPv6 address formatting functions */

char *conv_addr (in_addr_t a, char *buf) {
  static char abuf[64];
  if (!buf) {
    buf = abuf;
  }
  sprintf (buf, "%d.%d.%d.%d", a&255, (a>>8)&255, (a>>16)&255, a>>24);
  return buf;
}

int conv_ipv6_internal (const unsigned short a[8], char *buf) {
  int i, j = 0, k = 0, l = 0;
  for (i = 0; i < 8; i++) {
    if (a[i]) {
      if (j > l) {
	l = j;
	k = i;
      }
      j = 0;
    } else {
      j++;
    }
  }
  if (j == 8) {
    memcpy (buf, "::", 3);
    return 2;
  }
  if (l == 5 && a[5] == 0xffff) {
    return sprintf (buf, "::ffff:%d.%d.%d.%d", a[6]&255, a[6]>>8, a[7]&255, a[7]>>8);
  }
  char *ptr = buf;
  if (l) {
    for (i = 0; i < k - l; i++) {
      ptr += sprintf (ptr, "%x:", ntohs (a[i]));
    }
    if (!i || k == 8) {
      *ptr++ = ':';
    }
    for (i = k; i < 8; i++) {
      ptr += sprintf (ptr, ":%x", ntohs (a[i]));
    } 
  } else {
    for (i = 0; i < 7; i++) {
      ptr += sprintf (ptr, "%x:", ntohs (a[i]));
    }
    ptr += sprintf (ptr, "%x", ntohs (a[i]));
  }
  return ptr - buf;
}

char *conv_addr6 (const unsigned char a[16], char *buf) {
  static char abuf[64];
  if (!buf) {
    buf = abuf;
  }
  conv_ipv6_internal ((const unsigned short *) a, buf);
  return buf;
}

char *show_ip (unsigned ip) {
  static char abuf[256], *ptr = abuf;
  char *res;
  if (ptr > abuf + 200) {
    ptr = abuf;
  }
  res = ptr;
  ptr += sprintf (ptr, "%d.%d.%d.%d", ip >> 24, (ip >> 16) & 0xff, (ip >> 8) & 0xff, ip & 0xff) + 1;
  return res;
}

char *show_ipv6 (const unsigned char ipv6[16]) {
  static char abuf[256], *ptr = abuf;
  char *res;
  if (ptr > abuf + 200) {
    ptr = abuf;
  }
  res = ptr;
  ptr += conv_ipv6_internal ((const unsigned short *) ipv6, ptr) + 1;
  return res;
}

