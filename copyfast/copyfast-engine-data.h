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
*/

#ifndef __COPYFAST_ENGINE_DATA_H__
#define __COPYFAST_ENGINE_DATA_H__

#define DEFAULT_SLOW_REQUEST_DELAY 1
#define DEFAULT_MEDIUM_REQUEST_DELAY 0.1
#define DEFAULT_FAST_REQUEST_DELAY -0.1

#define REQUEST_BINLOG_DELAY 1

#define IDLE_LIMIT 5
struct relative {
  struct relative *next, *prev;
  struct node node;
  int link_color;
  int type;
  double timestamp;
  union {
    struct {
      struct conn_target *targ;
    } targ;
    struct {
      struct connection *conn;
      int generation;
    } conn;
  } conn;
  long long binlog_position;  
};


struct relative *get_relative_by_id (long long id);
struct connection *get_relative_connection (struct relative *x);
void delete_relative (struct relative *x, int force);
void add_child (struct node child);
void add_parent (struct node child, struct connection *c);
void clear_all_children_connections (void);
void delete_dead_connections (void);
int update_relatives_binlog_position (long long id, long long binlog_position);
long long get_id_by_connection (struct connection *c);
void restart_friends_timers (void);
void create_children_connections (struct node *children, int children_num);
void send_friends_binlog_position (void);
void request_binlog (void);
struct relative *get_relative_by_connection (struct connection *c);
void generate_delays (void);
#endif
