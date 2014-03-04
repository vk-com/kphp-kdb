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

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman
*/

#ifndef __NET_RPC_TARGETS__
#define __NET_RPC_TARGETS__
#include "net-connections.h"
#include "net-rpc-common.h"
struct rpc_target {
  int inbound_num;
  int b;
  struct connection *first, *last;
  struct conn_target *target;
  struct process_id PID;
};

struct rpc_target *rpc_target_lookup (struct process_id *PID);
struct rpc_target *rpc_target_lookup_hp (unsigned host, int port);
struct rpc_target *rpc_target_lookup_target (struct conn_target *targ);

struct connection *rpc_target_choose_connection (struct rpc_target *S, struct process_id *PID);
int rpc_target_choose_random_connections (struct rpc_target *S, struct process_id *PID, int limit, struct connection *buf[]);

void rpc_target_insert_conn (struct connection *c);
void rpc_target_insert_target (struct conn_target *t);
void rpc_target_insert_target_ext (struct conn_target *t, unsigned host);
void rpc_target_delete_conn (struct connection *c);

int rpc_target_get_state (struct rpc_target *S, struct process_id *PID);
#endif
