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
                   2011 Anton Maydell
*/

#ifndef __KDB_PMEMCACHED_BINLOG_H__
#define __KDB_PMEMCACHED_BINLOG_H__

#include "kdb-binlog-common.h"
#include "pmemcached-data.h"

#include <stddef.h>

#pragma	pack(push,4)

#ifndef	PMEMCACHED_SCHEMA_BASE
#define	PMEMCACHED_SCHEMA_BASE	0x37450000
#endif

#define	PMEMCACHED_SCHEMA_V1	0x37450101

#define LEV_PMEMCACHED_DELETE 0x1ace7893
#define LEV_PMEMCACHED_STORE 0x27827d00
#define LEV_PMEMCACHED_STORE_FOREVER 0x29aef200
#define LEV_PMEMCACHED_GET 0x3a789adb
#define LEV_PMEMCACHED_INCR 0x4fe23098
#define LEV_PMEMCACHED_INCR_TINY 0x5ac40900

//#define LOG_PMEMCACHED_GET

struct lev_pmemcached_delete {
  lev_type_t type;
  short key_len;
  char key[1];
};

struct lev_pmemcached_store {
  lev_type_t type;
  short key_len;
  short flags;
  int data_len;
  int delay;
  char data[1]; // the first part contains bytes from key, the second - value for this key
};

struct lev_pmemcached_store_forever {
  lev_type_t type;
  short key_len;
  int data_len;
  char data[1]; // the first part contains bytes from key, the second - value for this key
};


struct lev_pmemcached_get {
  lev_type_t type;
  long long hash;
  short key_len;
  char key[1];
};

struct lev_pmemcached_incr {
  lev_type_t type;
  long long arg;
  short key_len;
  char key[1]; 
};

struct lev_pmemcached_incr_tiny {
  lev_type_t type;
  short key_len;
  char key[1]; 
};


#pragma	pack(pop)

#endif
