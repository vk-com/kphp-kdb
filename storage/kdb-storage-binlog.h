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

#ifndef __KDB_STORAGE_BINLOG_H__
#define __KDB_STORAGE_BINLOG_H__

#pragma	pack(push,4)

#ifdef NGX_HTTP_STORAGE_MODULE
typedef unsigned lev_type_t;
#else
#include "kdb-binlog-common.h"
#endif


#ifndef	STORAGE_SCHEMA_BASE
#define	STORAGE_SCHEMA_BASE	0x805a0000
#endif

#define	STORAGE_SCHEMA_V1	0x805a0101

#define LEV_STORAGE_START_SIZE 36
#define LEV_STORAGE_FILE 0x56296b56
#define LEV_STORAGE_HIDE_FILE 0x6d28d31b

struct lev_storage_file {
  lev_type_t type;
  unsigned long long secret;
  unsigned char md5[16];
  unsigned int local_id;
  unsigned int size;
  unsigned crc32;
  int content_type;
  int mtime;
  unsigned char data[0];
};

#pragma	pack(pop)

#endif
