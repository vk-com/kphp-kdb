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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Vitaliy Valtman
*/

#ifndef __COPYFAST_ENGINE_H__
#define __COPYFAST_ENGINE_H__
#include "net-connections.h"
#include "copyfast-rpc-layout.h"
#include "copyfast-engine-data.h"

int rpc_send_children_request (struct connection *c);
int rpc_send_join (struct connection *c);
int rpc_send_stats (struct connection *c);
int rpc_send_handshake (struct connection *c);
int rpc_send_handshake_accept (struct connection *c, long long remote_id);
int rpc_send_handshake_reject (struct connection *c, long long remote_id);
int rpc_send_binlog_info (struct connection *c, long long remote_id);
int rpc_send_binlog_request (struct connection *c, long long remote_id, long long pos);
int rpc_send_binlog_data (struct connection *c, long long remote_id, long long pos);
int rpc_send_divorce (struct connection *c);

extern union engine_stats stats;
extern struct conn_target default_child;
extern unsigned host;

struct connection *get_target_connection (struct conn_target *targ);
double get_double_time_since_epoch();

#define BINLOG_BUFFER_SIZE (1 << 19)
#define MAX_SEND_LEN (1 << 16)
#ifdef _LP64
#define MAX_BINLOG_SIZE 1000000000000ll
#else
#define MAX_BINLOG_SIZE 10000000000ll
#endif
#define CRC64_ARRAY_STEP_LOG 16
#define CRC64_ARRAY_STEP (1 << CRC64_ARRAY_STEP_LOG)
struct cluster_timers {
  double last_children_get_time;
  double last_stats_time;
  double last_binlog_request_time;

  double join_renew_time;
  double stats_renew_time;
  double children_renew_time;

  double slow_request_delay;
  double medium_request_delay;

  double request_delay[3];
};

struct cluster {
  long long node_id;
  long long cluster_id;
  union engine_stats stats;
  
  int main_replica;
  int fd;
  
  struct relative relatives;

  char *binlog_buffer, *wptr, *fptr, *binlog_buffer_end;
  unsigned long long *crc64_array;

  struct long_update_stats *update_stats;

  struct cluster_timers cluster_timers;

  long long start_manifest_position;
  long long last_manifest_position;

  int ref_cnt;
  int flags;

  int cluster_name_len;
  int binlog_name_len;
  const char *cluster_name;
  const char *binlog_name;

  long long last_size;
};
extern struct cluster *CC;

#define STATS (&(CC->stats))
#define UPDATE_STATS (CC->update_stats)
#define NODE_ID (CC->node_id)
#define CLUSTER_ID (CC->cluster_id)
#define BINLOG_POSITION (STATS->structured.binlog_position)
#define CRC64 (STATS->structured.crc64)

#define BINLOG_BUFFER (CC->binlog_buffer)
#define BINLOG_BUFFER_WPTR (CC->wptr)
#define BINLOG_BUFFER_FPTR (CC->fptr)
#define BINLOG_BUFFER_END (CC->binlog_buffer_end)
#define BINLOG_BUFFER_FD (CC->fd)

#define SLOW_REQUEST_DELAY (CC->cluster_timers.slow_request_delay)
#define MEDIUM_REQUEST_DELAY (CC->cluster_timers.medium_request_delay)

#define LAST_CHILDREN_GET_TIME (CC->cluster_timers.last_children_get_time)
#define LAST_STATS_TIME (CC->cluster_timers.last_stats_time)
#define STATS_RENEW_TIME (CC->cluster_timers.stats_renew_time)
#define JOIN_RENEW_TIME (CC->cluster_timers.join_renew_time)
#define STATS_RENEW_TIME (CC->cluster_timers.stats_renew_time)
#define CHILDREN_RENEW_TIME (CC->cluster_timers.children_renew_time)

#define MAIN_REPLICA (CC->main_replica)
#define CRC64_ARRAY (CC->crc64_array)
#define BINLOG_NAME_LEN (CC->binlog_name_len)
#define BINLOG_NAME (CC->binlog_name)
#define CLUSTER_NAME_LEN (CC->cluster_name_len)
#define CLUSTER_NAME (CC->cluster_name)

#define RELATIVES (CC->relatives)
#define LAST_BINLOG_REQUEST_TIME (CC->cluster_timers.last_binlog_request_time)
#define REQUEST_DELAY (CC->cluster_timers.request_delay)

#define REF_CNT (CC->ref_cnt)
#define MANIFEST (CC->flags & 1)
#define ALLOCATED_HERE (CC->flags & 2)

#define START_MANIFEST_POSITION (CC->start_manifest_position)
#define LAST_MANIFEST_POSITION (CC->last_manifest_position)

#define LAST_SIZE (CC->last_size)
#endif
