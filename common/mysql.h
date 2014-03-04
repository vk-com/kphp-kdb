/*
    This file is part of VK/KittenPHP-DB-Engine.

    VK/KittenPHP-DB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2010 Vkontakte Ltd
              2010 Nikolai Durov
              2010 Andrei Lopatin
*/

#ifndef __MYSQL_H__
#define	__MYSQL_H__

#define MYSQL_COM_INIT_DB	2
#define MYSQL_COM_QUERY		3
#define MYSQL_COM_PING		14
#define MYSQL_COM_BINLOG_DUMP	18

#define cp1251_general_ci	51

#pragma	pack(push,1)

struct mysql_auth_packet_end {
  int thread_id;
  char scramble1[8];
  char filler;
  short server_capabilities;
  char server_language;
  short server_status;
  char filler2[13];
  char scramble2[13];
};

#pragma	pack(pop)

/* for sqls_data.auth_state */

enum sql_auth {
  sql_noauth,
  sql_auth_sent,
  sql_auth_initdb,
  sql_auth_ok
};

#endif
