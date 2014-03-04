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

    Copyright 2009 Vkontakte Ltd
              2009 Nikolai Durov
              2009 Andrei Lopatin
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
#include <aio.h>
#include <netdb.h>
#include <signal.h>

#include "kdb-data-common.h"
#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-aio.h"

extern int verbosity;

long long tot_aio_queries = 0, active_aio_queries = 0, expired_aio_queries = 0;
double total_aio_time;

struct {
  int fd, flags;
  struct aio_connection *first, *last;
} aio_list = {
  .first = (struct aio_connection *)&aio_list,
  .last = (struct aio_connection *)&aio_list
};

struct aio_connection *create_aio_read_connection (int fd, void *target, off_t offset, int len, conn_type_t *type, void *extra) {
  struct aio_connection *a = zmalloc0 (sizeof (struct aio_connection));

  if (verbosity > 1) {
    fprintf (stderr, "in create_aio_read_connection(%d,%p,%lld,%d,%p): allocated at %p\n", fd, target, (long long) offset, len, extra, a);
  }

  a->fd = fd;
  a->flags = C_AIO;
  a->type = type;
  a->cb = zmalloc0 (sizeof (struct aiocb));
  a->first_query = a->last_query = (struct conn_query *)a;
  a->extra = extra;
  a->basic_type = ct_aio;
  a->status = conn_wait_aio;

  a->next = (struct aio_connection *)&aio_list;
  a->prev = aio_list.last;
  
  aio_list.last->next = a;
  aio_list.last = a;

  a->cb->aio_fildes = fd;
  a->cb->aio_buf = target;
  a->cb->aio_offset = offset;
  a->cb->aio_nbytes = len;
  a->cb->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
  a->cb->aio_sigevent.sigev_signo = SIGPOLL;

  if (aio_read (a->cb) < 0) {
    fprintf (stderr, "aio_read returned -1: %m\n");
    exit (3);
  };

  return a;
}

int aio_errors_verbosity;

int check_aio_completion (struct aio_connection *a) {

  if (verbosity > 1) {
    fprintf (stderr, "in check_aio_completion(%p [first_query=%p, last_query=%p])\n", a, a->first_query, a->last_query);
  }

  errno = 0;

  int err = aio_error (a->cb);
  
  if (err == EINPROGRESS) {
    if (verbosity > 1) {
      fprintf (stderr, "aio_query %p in progress...\n", a);
    }
    if (a->first_query == (struct conn_query *) a) {
      if (verbosity > 0) {
        fprintf (stderr, "aio_query %p in progress, but all conn_queries are dead, canceling aio.\n", a);
      }
      if (aio_cancel (a->fd, a->cb) == AIO_NOTCANCELED) {
        if (verbosity > 0) {
          fprintf (stderr, "aio_cancel(%d,%p) returns AIO_NOTCANCELED\n", a->fd, a->cb);
        }
        return 0;
      }
      err = aio_error (a->cb);
      if (err == EINPROGRESS) {
        if (verbosity > 0) {
          fprintf (stderr, "aio_query %p still in progress.\n", a);
        }
        return 0;
      }
    } else {
      return 0;
    }
  }

  int res = aio_return (a->cb);
  if (verbosity > 1 || (aio_errors_verbosity && err)) {
    fprintf (stderr, "aio_return() returns %d, errno=%d (%s)\n", res, err, strerror (err));
  }

  a->type->wakeup_aio ((struct connection *)a, res);

  a->next->prev = a->prev;
  a->prev->next = a->next;

  struct conn_query *tmp, *tnext;

  for (tmp = a->first_query; tmp != (struct conn_query *)a; tmp = tnext) {
    tnext = tmp->next;
//    fprintf (stderr, "scanning aio_completion %p,next = %p\n", tmp, tnext);
    if (res >= 0) {
      tmp->cq_type->complete (tmp);
    } else {
      tmp->cq_type->close (tmp);
    }
  }

  if (verbosity > 2) {
    fprintf (stderr, "freeing aio_connection at %p\n", a);
  }

  zfree (a->cb, sizeof (struct aiocb));
  zfree (a, sizeof (struct aio_connection));

  return 1;
}


int check_all_aio_completions (void) {
  struct aio_connection *tmp, *tnext;
  int sum = 0;

  if (verbosity > 3) {
    fprintf (stderr, "check_all_aio_completions ()\n");
  }

  for (tmp = aio_list.first; tmp != (struct aio_connection *)&aio_list; tmp = tnext) {
    tnext = tmp->next;
    //fprintf (stderr, "scanning aio_connection %p,next = %p\n", tmp, tnext);
    sum += check_aio_completion (tmp);
  }
  if (verbosity > 1 && sum > 0) {
    fprintf (stderr, "check_all_aio_completions returns %d\n", sum);
  }
  return sum;
}


int create_aio_query (struct aio_connection *a, struct connection *c, double timeout, struct conn_query_functions *cq) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_query(%p[%d], %p[%d]): Q=%p\n", a, a->fd, c, c->fd, Q);
  }

  Q->custom_type = 0;
  Q->outbound = (struct connection *)a;
  Q->requester = c;
  Q->start_time = /*c->query_start_time*/get_utime (CLOCK_MONOTONIC);
  Q->extra = 0;
  Q->cq_type = cq;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  if (verbosity > 1 && a->first_query != (struct conn_query *) a) {
    fprintf (stderr, "!NOTICE! inserting second query to %p\n", a);
  }

  insert_conn_query (Q);
  active_aio_queries++;
  tot_aio_queries++;

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query()\n");
  }

  return 1;
}


int delete_aio_query (struct conn_query *q) {
  if (q->start_time > 0) {
    double q_time = get_utime (CLOCK_MONOTONIC) - q->start_time;
    total_aio_time += q_time;
    if (verbosity > 1) {
      fprintf (stderr, "delete_aio_query(%p): query time %.6f\n", q, q_time);
    }
  }
  active_aio_queries--;
  delete_conn_query (q);
  zfree (q, sizeof (*q));
  return 0;
}


int aio_query_timeout (struct conn_query *q) {
  if (verbosity > 0) {
    fprintf (stderr, "query %p of connection %p (fd=%d) timed out, unreliability=%d\n", q, q->outbound, q->outbound->fd, q->outbound->unreliability);
  }
  expired_aio_queries++;
  delete_aio_query (q);
  return 0;
}



int conn_schedule_aio (struct aio_connection *a, struct connection *c, double timeout, struct conn_query_functions *cq) {
  if (verbosity > 1) {
    fprintf (stderr, "in conn_schedule_aio(%p,%p)\n", a, c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  create_aio_query (a, c, timeout, cq);
  c->status = conn_wait_aio;

  return -1;
}
