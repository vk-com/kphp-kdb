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
              2012-2013 Anton Maydell
*/

#ifndef __STORAGE_RPC_H__
#define __STORAGE_RPC_H__

#include "net-connections.h"

#define MAX_PACKET_LEN 0x1000000

#define RPC_INVOKE_REQ  0x2374df3d
#define RPC_REQ_ERROR   0x7ae432f5
#define RPC_REQ_RESULT  0x63aeda4e

#include "TL/constants.h"

#pragma	pack(push,4)

struct storage_rpc_upload_file {
  long long volume_id;
  unsigned char file_data[0];
};

struct storage_rpc_file_location {
  int magic;
  long long volume_id;
  int local_id;
  long long secret;
};

struct storage_rpc_partial {
  int offset;
  int limit;
};

struct storage_rpc_check_file {
  long long volume_id;
  long long file_size;
  double max_disk_used_space_percent;
};

struct storage_rpc_forward {
  int fwd_address;
  int fwd_port;
  int fwd_pid;
  int fwd_start_time;
  int fwd_header_ints;
};

#pragma	pack(pop)

void rpc_clear_packet (int reserve_space_for_op);
void rpc_out_ints (int *what, int len);
void rpc_out_int (int x);
void rpc_out_long (long long x);
void rpc_out_string (const char *str);
void rpc_out_cstring (const char *str, long len);
void rpc_readin (struct connection *c, int len);
int rpc_fetch_ints (int *a, int len);
int rpc_end_of_fetch (void);

struct connection *get_target_connection (struct conn_target *targ);
int rpc_send_error (struct connection *c, long long req_id, int error_code, char *format, ...) __attribute__ ((format (printf, 4, 5)));
int rpc_send_packet (struct connection *c, int op);

#endif


