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
              2011-2013 Anton Maydell
*/

#ifndef __COPYEXEC_RPC_LAYOUT_H__
#define __COPYEXEC_RPC_LAYOUT_H__

#pragma	pack(push,4)
#define COPYEXEC_RPC_TYPE_HANDSHAKE 0x96a6d5a6
#define COPYEXEC_RPC_TYPE_ERR_HANDSHAKE 0x681424c4
#define COPYEXEC_RPC_TYPE_GET_POS 0x53023780
#define COPYEXEC_RPC_TYPE_VALUE_POS 0x3f43c4d5
#define COPYEXEC_RPC_TYPE_SEND_DATA 0xafd8e8b2
#define COPYEXEC_RPC_TYPE_PING 0x80536066

struct copyexec_rpc_handshake {
  unsigned long long volume_id;
  unsigned long long random_tag;
  int hostname_length;
  int pid;
  char hostname[0];
};

struct copyexec_rpc_handshake_error {
  int error_code;
};

struct copyexec_rpc_pos {
  long long binlog_pos;
};

struct copyexec_rpc_send_data {
  long long binlog_pos;
  int transaction_id;
  unsigned result;
};

#pragma	pack(pop)
#endif
