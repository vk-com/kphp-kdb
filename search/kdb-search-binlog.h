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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
              2010-2011 Vitaliy Valtman
              2010-2013 Anton Maydell
*/

#ifndef __KDB_SEARCH_BINLOG_H__
#define __KDB_SEARCH_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	SEARCH_SCHEMA_BASE
#define	SEARCH_SCHEMA_BASE	0xbeef0000
#endif

#define	SEARCH_SCHEMA_V1	0xbeef0101

#define LEV_SEARCH_TEXT_SHORT	0x1bca3400
#define LEV_SEARCH_TEXT_LONG	0x45a14521
#define LEV_SEARCH_DELETE_ITEM	0xa6524ac6

#define	LEV_SEARCH_SET_RATE	0x61ae5e72
#define	LEV_SEARCH_SET_RATE2	0x4d432307
#define	LEV_SEARCH_SET_RATES	0x5418ba3c
#define LEV_SEARCH_SET_RATE_NEW 0x8cd13400
#define LEV_SEARCH_SET_HASH 0x9baba777

#define	LEV_SEARCH_INCR_RATE	0x114ca245
#define	LEV_SEARCH_INCR_RATE2	0x62acdd26
#define	LEV_SEARCH_INCR_RATE_SHORT	0x31acda00
#define	LEV_SEARCH_INCR_RATE2_SHORT	0x2a257800
#define LEV_SEARCH_INCR_RATE_NEW 0x7a5f1200
#define LEV_SEARCH_RESET_ALL_RATES 0x5dd903af

#define LEV_SEARCH_ITEM_ADD_TAGS 0x5dd3ad00

struct lev_search_item_add_tags {
  lev_type_t type;
  long long obj_id;
  char text[1];
};

struct lev_search_text_short_entry {
  lev_type_t type;
  int rate;
  long long obj_id;
  int rate2;
  char text[1];
};

struct lev_search_text_long_entry {
  lev_type_t type;
  int rate;
  long long obj_id;
  int rate2;
  unsigned short text_len;
  char text[1];
};

struct lev_search_delete_item {
  lev_type_t type;
  long long obj_id;
};

struct lev_search_set_rate {
  lev_type_t type;
  int rate;
  long long obj_id;
};

struct lev_search_set_rates {
  lev_type_t type;
  int rate;
  long long obj_id;
  int rate2;
};

struct lev_search_incr_rate {
  lev_type_t type;
  int rate_incr;
  long long obj_id;
};

struct lev_search_incr_rate_short {
  lev_type_t type;
  long long obj_id;
};


struct lev_search_incr_rate_new {
  lev_type_t type;
  int rate_incr;
  long long obj_id;
};

struct lev_search_set_rate_new {
  lev_type_t type;
  int rate;
  long long obj_id;
};

struct lev_search_set_hash {
  lev_type_t type;
  long long obj_id;
  long long hash;
};

struct lev_search_reset_all_rates {
  lev_type_t type;
  int rate_id;
};

#pragma	pack(pop)

#endif
