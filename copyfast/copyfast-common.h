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

#ifndef __COPYFAST_COMMON_H__
#define __COPYFAST_COMMON_H__
#pragma	pack(push,4)


#define MAX_PACKET_LEN (1 << 20)

#define IP "%u.%u.%u.%u"
#define INT_TO_IP(x) (x >> 24) & 255, (x >> 16) & 255, (x >> 8) & 255, x & 255

#define CLUSTER_MASK 0xffffffff00000000ll
#define ID_MASK 0x00000000ffffffffll

#define rpc_op_crc32(R) (*(((int *)(R)) + ((((struct rpc_op *)R)->len / 4) - 1)))
#define rpc_check_crc32(R) (rpc_op_crc32(R) == compute_crc32 ((R), ((struct rpc_op *)R)->len - 4))
#define rpc_set_crc32(R) rpc_op_crc32(R) = compute_crc32 ((R), ((struct rpc_op *)R)->len - 4)



int read_network_file (char *filename);
int link_color (unsigned ip1, unsigned ip2);
int link_level (unsigned ip, int color);


int rpc_create_query (void *_R, int len, struct connection *c, int op);
int rpc_send_query (void *_R, struct connection *c);


extern char *stats_layout[];
#define LOG_BINLOG_UPDATED 0x17
#define LOG_BINLOG_RECEIVED 0x32
#define LOG_BINLOG_REQUEST_SENT 0x43
#define LOG_CHILDREN_REQUEST 0x57
#define LOG_CHILDREN_ANSWER 0x62
#define LOG_JOIN_REQUEST 0x73
#define LOG_JOIN_ACK 0x83
#define LOG_STATS_SEND 0x92
#define LOG_UPDATE_STATS_SEND 0x98

#pragma	pack(pop)
#endif
