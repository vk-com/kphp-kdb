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

#ifndef __KDB_CACHE_BINLOG_H__
#define __KDB_CACHE_BINLOG_H__

#include "kdb-binlog-common.h"
#pragma	pack(push,4)

#ifndef	CACHE_SCHEMA_BASE
#define	CACHE_SCHEMA_BASE	0x3ded0000
#endif

#define	CACHE_SCHEMA_V1	0x3ded0101

#define LEV_CACHE_ACCESS_SHORT 0x53434100
#define LEV_CACHE_ACCESS_LONG  0x4c434100
#define LEV_CACHE_URI_ADD      0x41495200
#define LEV_CACHE_URI_DELETE   0x08e0e100
#define LEV_CACHE_SET_SIZE_SHORT 0x35f5503e
#define LEV_CACHE_SET_SIZE_LONG 0xc21411a9
#define LEV_CACHE_SET_NEW_LOCAL_COPY_SHORT 0xb3329e67
#define LEV_CACHE_SET_NEW_LOCAL_COPY_LONG 0xebd5e600
#define LEV_CACHE_DELETE_LOCAL_COPY_SHORT 0xb6da07dd
#define LEV_CACHE_DELETE_LOCAL_COPY_LONG 0x23505000
#define LEV_CACHE_CHANGE_DISK_STATUS 0x3011d100
#define LEV_CACHE_DELETE_REMOTE_DISK 0x6682a9b3
#define LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_SHORT 0x3456c28c
#define LEV_CACHE_SET_LOCAL_COPY_YELLOW_LIGHT_LONG  0x46359c00

struct lev_cache_change_disk_status {
  lev_type_t type;
  int packed_location;
};

struct lev_cache_set_new_local_copy_short {
  lev_type_t type;
  int packed_location;
  unsigned char data[8];
};

struct lev_cache_set_new_local_copy_long {
  lev_type_t type;
  unsigned char md5[16];
  char data[0];
};

struct lev_cache_set_local_copy_yellow_light_short {
  lev_type_t type;
  int yellow_light_duration;
  int packed_location;
  unsigned char data[8];
};

struct lev_cache_set_local_copy_yellow_light_long {
  lev_type_t type;
  int yellow_light_duration;
  unsigned char md5[16];
  char data[0];
};

struct lev_cache_access_short {
  lev_type_t type;
  unsigned char data[8];
};

struct lev_cache_access_long {
  lev_type_t type;
  unsigned char data[16];
};

struct lev_cache_uri {
  lev_type_t type;
  char data[0];
};

struct lev_cache_set_size_short {
  lev_type_t type;
  unsigned size;
  char data[8];
};

struct lev_cache_set_size_long {
  lev_type_t type;
  long long size;
  char data[16];
};

#pragma	pack(pop)

#endif


