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
              2013 Anton Maydell
*/

#ifndef __KDB_WEIGHTS_BINLOG_H__
#define __KDB_WEIGHTS_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	WEIGHTS_SCHEMA_BASE
#define	WEIGHTS_SCHEMA_BASE	0x521e0000
#endif

#define	WEIGHTS_SCHEMA_V1 0x521e0101

#define LEV_WEIGHTS_INCR 0x759a2000
#define LEV_WEIGHTS_SET_HALF_LIFE 0x1c751700

struct lev_weights_incr {
  lev_type_t type; /* contains coord_id */
  int vector_id;
  int value;
};

struct lev_weights_set_half_life {
  lev_type_t type; /* contains coord_id */
  int half_life;
};

#pragma	pack(pop)

#endif
