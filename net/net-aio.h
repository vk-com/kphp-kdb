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

#ifndef __VK_NET_AIO_H__
#define __VK_NET_AIO_H__

#include "net-connections.h"
#include <aio.h>

struct aio_connection {
  int fd;
  int flags;
  struct aio_connection *next, *prev;
  struct conn_query *first_query, *last_query;
  conn_type_t *type;
  struct aiocb *cb;
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
};

extern long long tot_aio_queries, active_aio_queries, expired_aio_queries;
extern double total_aio_time;

struct aio_connection *create_aio_read_connection (int fd, void *target, off_t offset, int len, conn_type_t *type, void *extra);
int check_aio_completion (struct aio_connection *a);
int create_aio_query (struct aio_connection *a, struct connection *c, double timeout, struct conn_query_functions *cq);
int conn_schedule_aio (struct aio_connection *a, struct connection *c, double timeout, struct conn_query_functions *cq);

int aio_query_timeout (struct conn_query *q);
int delete_aio_query (struct conn_query *q);
int check_all_aio_completions (void);

#endif
