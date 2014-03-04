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

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman
*/

#ifndef __KDB_SEQMAP_BINLOG_H__
#define __KDB_SEQMAP_BINLOG_H__

#define MAX_KEY_LEN 16
#define MAX_DATA_LEN (1 << 18) 

#define LEV_SEQ_STORE_TIME 0x8954a300
#define LEV_SEQ_STORE_INF 0x9f849d00
#define LEV_SEQ_DELETE 0x238aaa00


struct lev_seq_store_time {
  int type;
  int time;
  int value_len;
  int data[0];
};

struct lev_seq_store_inf {
  int type;
  int value_len;
  int data[0];
};

struct lev_seq_delete {
  int type;
  int data[0];
};

#endif
