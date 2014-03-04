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
              2012-2013 Vitaliy Valtman
*/

#ifndef __KDB_RPC_PROXY_BINLOG_H__
#define __KDB_RPC_PROXY_BINLOG_H__

#include "pid.h"

#ifndef	RPC_PROXY_SCHEMA_BASE
#define	RPC_PROXY_SCHEMA_BASE	0xf9a90000
#endif

#define	RPC_PROXY_SCHEMA_V1	0xf9a90101

#define LEV_RPC_QUERY_RECEIVE 0x34a74df8

#define LEV_ANSWER_FORGET 0x620d1e8b
#define LEV_QUERY_FORGET 0x7faf00a1
#define LEV_QUERY_TX 0xd04e69cb
#define LEV_ANSWER_TX 0x138fb353
#define LEV_ANSWER_RX 0x50137bc0

#pragma pack(push,4)

struct lev_answer_forget {
  int type;
  long long qid;
  struct process_id pid;
};

struct lev_query_forget {
  int type;
  long long qid;
};

struct lev_query_tx {
  int type;
  long long qid;
  long long old_qid;
  long long cluster_id;
  int data_size;
  struct process_id pid;
  double timeout;
  int data[0];
};

struct lev_answer_tx {
  int type;
  long long qid;
  struct process_id pid;
  int op;
  int answer_len;
  int answer[0];
};

struct lev_answer_rx {
  int type;
  long long qid;
};
#pragma pack(pop)

#endif
