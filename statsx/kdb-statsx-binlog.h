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
              2011-2013 Vitaliy Valtman
              2011-2013 Anton Maydell
*/

#ifndef __KDB_STATS_BINLOG_H__
#define __KDB_STATS_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	STATS_SCHEMA_BASE
#define	STATS_SCHEMA_BASE	0x3ae60000
#endif

#define	STATS_SCHEMA_V1	0x3ae60101

#define LEV_STATS_VISITOR_OLD	0x5a138b26
#define LEV_STATS_VISITOR	0x59023400
#define LEV_STATS_VISITOR_OLD_EXT	0x278c7e78
#define LEV_STATS_VISITOR_EXT	0x3560ab00
#define	LEV_STATS_VIEWS_EXT	0x19ac1231
#define	LEV_STATS_VIEWS		0x127d2f00
#define	LEV_STATS_COUNTER_ON	0x27a123e4
#define	LEV_STATS_COUNTER_OFF	0x4571a25e
#define LEV_STATS_TIMEZONE 0x65afb100
#define LEV_STATS_VISITOR_VERSION 0x723841fa
#define LEV_STATS_DELETE_COUNTER 0xa8907621

#define LEV_STATS_VISITOR_OLD_64	0x5a138b27
#define LEV_STATS_VISITOR_64	0x59023500
#define LEV_STATS_VISITOR_OLD_EXT_64	0x278c7e79
#define LEV_STATS_VISITOR_EXT_64	0x3560ac00
#define	LEV_STATS_VIEWS_EXT_64	0x19ac1232
#define	LEV_STATS_VIEWS_64		0x127d3100
#define	LEV_STATS_COUNTER_ON_64	0x27a123e5
#define	LEV_STATS_COUNTER_OFF_64	0x4571a25f
#define LEV_STATS_TIMEZONE_64 0x65afb200
#define LEV_STATS_VISITOR_VERSION_64 0x723841fb
#define LEV_STATS_DELETE_COUNTER_64 0xa8907622

struct lev_stats_visitor {
  lev_type_t type;
  int cnt_id;
  int user_id;
};

struct lev_stats_visitor_version {
  lev_type_t type;
  int cnt_id;
  int version;
};

struct lev_stats_visitor_old_ext {
  lev_type_t type;
  int cnt_id;
  int user_id;
  int city;
  char sex_age; // sex*16+age
  char m_status;
  char polit_views;
  char section;
};

struct lev_stats_visitor_ext {
  lev_type_t type;
  int cnt_id;
  int user_id;
  int city;
  int country;
  int geoip_country;
  char sex_age; // sex*16+age
  char m_status;
  char polit_views;
  char section;
  char source;
};

struct lev_stats_views {
  lev_type_t type;
  int cnt_id;
};

struct lev_stats_views_ext {
  lev_type_t type;
  int cnt_id;
  int views;
};

struct lev_stats_counter {
  lev_type_t type;
  int cnt_id;
};

struct lev_timezone {
  lev_type_t type;
  int cnt_id;
};

struct lev_delete_counter {
  lev_type_t type;
  int cnt_id;
};


struct lev_stats_visitor64 {
  lev_type_t type;
  long long cnt_id;
  int user_id;
};

struct lev_stats_visitor_version64 {
  lev_type_t type;
  long long cnt_id;
  int version;
};

struct lev_stats_visitor_old_ext64 {
  lev_type_t type;
  long long cnt_id;
  int user_id;
  int city;
  char sex_age; // sex*16+age
  char m_status;
  char polit_views;
  char section;
};

struct lev_stats_visitor_ext64 {
  lev_type_t type;
  long long cnt_id;
  int user_id;
  int city;
  int country;
  int geoip_country;
  char sex_age; // sex*16+age
  char m_status;
  char polit_views;
  char section;
  char source;
};

struct lev_stats_views64 {
  lev_type_t type;
  long long cnt_id;
};

struct lev_stats_views_ext64 {
  lev_type_t type;
  long long cnt_id;
  int views;
};

struct lev_stats_counter64 {
  lev_type_t type;
  long long cnt_id;
};

struct lev_timezone64 {
  lev_type_t type;
  long long cnt_id;
};

struct lev_delete_counter64 {
  lev_type_t type;
  long long cnt_id;
};

#pragma	pack(pop)

#endif
