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

#ifndef __COPYFAST_RPC_LAYOUT_H__
#define __COPYFAST_RPC_LAYOUT_H__

#include "kdb-binlog-common.h"


#pragma	pack(push,4)

#define RPC_TYPE_JOIN_OLD 0x76af3412
#define RPC_TYPE_JOIN 0x78923400
#define RPC_TYPE_JOIN_ACK 0x2817a0fc
#define RPC_TYPE_KICKED 0x177a762a
#define RPC_TYPE_CHILDREN_REQUEST 0x348a0651
#define RPC_TYPE_CHILDREN_ANSWER_OLD 0x5890ac73
#define RPC_TYPE_CHILDREN_ANSWER 0x7891a800
#define RPC_TYPE_STATS 0x234f8735
#define RPC_TYPE_REQUEST_UPDATE_STATS 0x3764ab0f
#define RPC_TYPE_UPDATE_STATS 0x7605032f

#define RPC_TYPE_HANDSHAKE 0x214aba83
#define RPC_TYPE_HANDSHAKE_ACCEPT 0x3145abf0
#define RPC_TYPE_HANDSHAKE_REJECT 0x5310a00b

#define RPC_TYPE_BINLOG_INFO 0xab01253f
#define RPC_TYPE_BINLOG_REQUEST 0xfa480ab1
#define RPC_TYPE_BINLOG_DATA 0xe213a079

#define RPC_TYPE_DIVORCE 0xab317f62
#define RPC_TYPE_DELAYS_OLD 0xc0848a02
#define RPC_TYPE_DELAYS 0xfa789200

#define MAX_CHILDREN 100

struct rpc_op {
  int len;
  int rpc_num;
  int rpc_op;
  int crc32;
};

struct rpcs_op {
  int len;
  int rpc_num;
  int rpc_op;
  long long local_id;
  long long remote_id;
  int crc32;
};

struct rpcc_op {
  int len;
  int rpc_num;
  int rpc_op;
  long long id;
  int crc32;
};

struct rpc_join_old {
  int len;
  int rpc_num;
  int rpc_op;
  int host;
  int port;
  long long id;
  long long binlog_position;
  int crc32;
};

struct rpc_join {
  int len;
  int rpc_num;
  int rpc_op;
  int host;
  int port;
  int protocol_version;
  long long id;
  long long binlog_position;
  int crc32;
};

struct rpc_join_ack {
  int len;
  int rpc_num;
  int rpc_op;  
  long long id;
  int crc32;
};

struct rpc_kicked {
  int len;
  int rpc_num;
  int rpc_op;
  int crc32;
};

struct rpc_children_request {
  int len;
  int rpc_num;
  int rpc_op;
  long long id;
  int crc32;
};

struct node {
  int host;
  int port;
  long long id;
};

struct rpc_children_answer_old {
  int len;
  int rpc_num;
  int rpc_op;
  int children_num;
  struct node children[0];
  int crc32;
};

struct rpc_children_answer {
  int len;
  int rpc_num;
  int rpc_op;
  long long id;
  int children_num;
  struct node children[0];
  int crc32;
};

struct rpc_delays {
  int len;
  int rpc_num;
  int rpc_op;
  long long id;
  double medium_delay;
  double slow_delay;
  int crc32;
};

struct rpc_delays_old {
  int len;
  int rpc_num;
  int rpc_op;
  double medium_delay;
  double slow_delay;
  int crc32;
};

#define TIMESTAMP_NUM_LOG 10
#define TIMESTAMP_NUM (1 << TIMESTAMP_NUM_LOG)
#define TIMESTAMP_NUM_MASK (TIMESTAMP_NUM - 1)
#define TIMESTAMP_STEP_LOG 12
#define TIMESTAMP_STEP (1 << TIMESTAMP_STEM_LOG)
#define TIMESTAMP_STEP_MASK (TIMESTAMP_STEP - 1)

#define STATS_INT_NUM 14
#define STATS_LONG_NUM 32
#define STATS_DOUBLE_NUM 4
#define STATS_EXTRA_INTS_NUM TIMESTAMP_NUM

#define STATS_UPDATE_BUFF_SIZE 32
union engine_stats {
  struct {
    int total_children;
    int total_parents;
    int total_links_color[3];
    int total_sent_num_color[3];
    int total_received_num_color[3];
    int total_requested_num_color[3];
    long long binlog_position;
    long long total_sent_bytes_color[3];
    long long total_received_bytes_color[3];
    long long crc64;
    long long last_known_binlog_position;
    long long children_requests_sent;
    long long joined_sent;
    long long stats_sent;
    long long update_stats_sent;
    long long join_ack_received;
    long long children_received;
    long long kicked_received;
    long long delays_received;
    long long request_update_stats_received;
    long long handshake_sent;
    long long handshake_accept_sent;
    long long handshake_reject_sent;
    long long divorce_sent;
    long long binlog_info_sent;
    long long binlog_request_sent;
    long long binlog_data_sent;
    long long handshake_received;
    long long handshake_accept_received;
    long long handshake_reject_received;
    long long divorce_received;
    long long binlog_info_received;
    long long binlog_request_received;
    long long binlog_data_received;
    double last_binlog_update;
    double last_known_binlog_position_time;
    double disk_read_time;
    double disk_write_time;
    int update_pos;
    char update_operations[STATS_UPDATE_BUFF_SIZE];
    long long update_data[STATS_UPDATE_BUFF_SIZE];
    double update_times[STATS_UPDATE_BUFF_SIZE];
    //int timestamps[TIMESTAMP_NUM];
  } structured;
  struct {
    int ints[STATS_INT_NUM];
    long long longs[STATS_LONG_NUM];
    double doubles[STATS_DOUBLE_NUM];
    //int extra_ints[STATS_EXTRA_INTS_NUM];
  } arrays;
};

#define LONG_STATS_UPDATE_BUFF_SIZE 10000
struct long_update_stats {
  int update_pos;
  char update_operations[LONG_STATS_UPDATE_BUFF_SIZE];
  long long update_data[LONG_STATS_UPDATE_BUFF_SIZE];
  double update_times[LONG_STATS_UPDATE_BUFF_SIZE];
};

struct rpc_update_stats {
  int len;
  int rpc_num;
  int rpc_op;
  long long id;
  struct long_update_stats stats;
  int crc32;
};

struct rpc_request_update_stats {
  int len;
  int rpc_num;
  int rpc_op;
  int crc32;
};

struct rpc_stats {
  int len;
  int rpc_num;
  int rpc_op;
  long long id;
  union engine_stats stats;
  int crc32;
};


struct rpc_handshake {
  int len;
  int rpc_num;
  int rpc_op;
  long long local_id;
  long long remote_id;
  long long binlog_position;
  int port;
  int crc32;
};

struct rpc_handshake_accept {
  int len;
  int rpc_num;
  int rpc_op;
  long long local_id;
  long long remote_id;
  long long binlog_position;
  int crc32;
};

struct rpc_handshake_reject {
  int len;
  int rpc_num;
  int rpc_op;
  long long local_id;
  long long remote_id;
  int crc32;
};

struct rpc_divorce {
  int len;
  int rpc_num;
  int rpc_op;
  int crc32;
};

struct rpc_binlog_info {
  int len;
  int rpc_num;
  int rpc_op;
  long long local_id;
  long long remote_id;
  long long binlog_position;
  int crc32;
};

struct rpc_binlog_request {
  int len;
  int rpc_num;
  int rpc_op;
  long long local_id;
  long long remote_id;
  long long binlog_position;
  int crc32;
};

struct rpc_binlog_data {
  int len;
  int rpc_num;
  int rpc_op;
  long long local_id;
  long long remote_id;
  long long binlog_position;
  unsigned long long crc64;
  int size;
  char data[0];
  int crc32;
};

struct rpc_op *rpc_alloc_query (int len, struct connection *c, int op);
int rpc_create_query (void *R, int len, struct connection *c, int op);

#define DEFAULT_CHILDREN_RENEW_TIME 120


#pragma	pack(pop)
#endif
