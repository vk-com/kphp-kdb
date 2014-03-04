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

#define _FILE_OFFSET_BITS 64

#include "copyfast-engine.h"
#include "copyfast-common.h"
#include "copyfast-rpc-layout.h"
#include "copyfast-engine-data.h"
#include "net-connections.h"
#include "assert.h"
#include "kdb-data-common.h"
#include "stdio.h"
#include "stdlib.h"
#include "math.h"

struct relative *get_relative_by_id (long long id) {
  struct relative *cur = RELATIVES.next;
  while (cur->type != -1) {
    if (cur->node.id == id) {
      return cur;
    }
    cur = cur->next;
  }
  return 0;
}

struct connection *get_relative_connection (struct relative *x) {
  if (!x) {
    return 0;
  }
  struct connection *c;
  if (x->type == 0) {
    c = get_target_connection (x->conn.targ.targ);
  } else if (x->type == 1) {
    c = x->conn.conn.conn;
    if (c && c->generation != x->conn.conn.generation) {
      c = 0;
    }
  } else {
    assert (0);
  }
  return c;
}

void delete_relative (struct relative *x, int force) {
  assert (x);
  if (verbosity >= 3) {
    fprintf (stderr, "delete_relative force = %d, x->type = %d\n", force, x->type);
  }
  struct connection *c = get_relative_connection (x);
  struct connection *s;
  if (x->type == 0) {
    s = x->conn.targ.targ->first_conn;
    while (s != (struct connection *)x->conn.targ.targ) {
      fail_connection (s, -1);
      s = s->next;
    }
  } else {
    s = x->conn.conn.conn;
    if (s && s->generation != x->conn.conn.generation) {
      s = 0;
    }
    if (s) {
      fail_connection (s, -1);
    }
  }

  if (x->type == 0) {
    destroy_target (x->conn.targ.targ);
    STATS->structured.total_children --;
  } else {
    STATS->structured.total_parents --;
  }
  STATS->structured.total_links_color[x->link_color]--;
  x->next->prev = x->prev;
  x->prev->next = x->next;
  zfree (x, sizeof (struct relative));
  if (c) {
    if (force) {
      if (server_check_ready (c) == cr_ok) {
        rpc_send_divorce (c);
      }
    }
  }
  if (verbosity >= 6) {
    fprintf (stderr, "delete_relative: done\n");
  }
}

void add_child (struct node child) {
  struct relative *cur = zmalloc (sizeof (struct relative));
  cur->next = RELATIVES.next;
  cur->prev = &RELATIVES;
  cur->prev->next = cur;
  cur->next->prev = cur;
  cur->node = child;
  cur->type = 0;
  int x = ntohl (child.host);
  default_child.target = *(struct in_addr *)&x;
  default_child.port = child.port;
  cur->conn.targ.targ = create_target (&default_child, 0);
  cur->link_color = link_color (cur->node.host, host);
  STATS->structured.total_links_color[cur->link_color]++;
  STATS->structured.total_children ++;
}

void add_parent (struct node child, struct connection *c) {
  struct relative *cur = zmalloc (sizeof (struct relative));
  cur->next = RELATIVES.next;
  cur->prev = &RELATIVES;
  cur->prev->next = cur;
  cur->next->prev = cur;
  cur->node = child;
  cur->type = 1;
  cur->conn.conn.conn = c;
  cur->conn.conn.generation = c->generation;
  cur->link_color = link_color (cur->node.host, host);
  STATS->structured.total_links_color[cur->link_color]++;
  STATS->structured.total_parents ++;
}

void clear_all_children_connections (void) {
  struct relative *cur = RELATIVES.next;
  while (cur->type != -1) {
    cur = cur->next;
    if (cur->prev->type == 0) {
      delete_relative (cur->prev, 1);
    }
  }
}

void delete_dead_connections (void) {
  struct relative *cur = RELATIVES.next;
  while (cur->type != -1) {
    cur = cur->next;
    if (cur->prev->type == 1) {
      if (cur->prev->conn.conn.conn->generation != cur->prev->conn.conn.generation ||
        server_check_ready (cur->prev->conn.conn.conn) != cr_ok || 
        cur->prev->conn.conn.conn->last_response_time + IDLE_LIMIT < precise_now) {
        delete_relative (cur->prev, 1);
      }
    }
  }
}

int update_relatives_binlog_position (long long id, long long binlog_position) {
  int i = 0;
  struct relative *cur = RELATIVES.next;
  while (cur->type != -1) {
    if (cur->node.id == id) {
      if (cur->binlog_position <= BINLOG_POSITION && binlog_position > BINLOG_POSITION) {
        cur->timestamp = precise_now; 
      }
      cur->binlog_position = binlog_position;
      i++;
    }
    cur = cur->next;
  }
  if (binlog_position > STATS->structured.last_known_binlog_position) {
    STATS->structured.last_known_binlog_position = binlog_position;
    STATS->structured.last_known_binlog_position_time = get_double_time_since_epoch();
  }
  return i;
}

long long get_id_by_connection (struct connection *c) {
  struct relative *cur = RELATIVES.next;
  while (cur->type != -1) {
    if (cur->type == 0 && cur->conn.targ.targ->first_conn == c) {
      return cur->node.id;
    }
    cur = cur->next;
  }
  return 0;
}

struct relative *get_relative_by_connection (struct connection *c) {
  struct relative *cur = RELATIVES.next;
  while (cur->type != -1) {
    if (cur->type == 1 && cur->conn.conn.conn == c && cur->conn.conn.generation == c->generation) {
      return cur;
    }
    if (cur->type == 0 && cur->conn.targ.targ->first_conn == c) {
      return cur;
    }
    cur = cur->next;
  }
  return 0;
}

void restart_friends_timers (void) {
  struct relative *cur = RELATIVES.next;
  while (cur->type != -1) {
    cur->timestamp = precise_now;
    cur = cur->next;
  }
}

void create_children_connections (struct node *children, int children_num) {
  int i;
  for (i = 0; i < children_num; i++) {
    add_child (children[i]);
  }
}

void send_friends_binlog_position (void) {
  struct relative *cur = RELATIVES.next;
  while (cur != &RELATIVES) {
    struct connection *c = get_relative_connection (cur);
    if (c) {
      rpc_send_binlog_info (c, cur->node.id);
    }
    cur = cur->next;
  }
}

void generate_delays (void) {
  double e = drand48 ();
  if (e < 0.1) {
    e = 0.1;
  }
  e = -log (e);
  assert (e >= 0);
  REQUEST_DELAY[0] = SLOW_REQUEST_DELAY * e;
  REQUEST_DELAY[1] = MEDIUM_REQUEST_DELAY * e;
  REQUEST_DELAY[2] = -0.1;
}
/*void request_binlog (void) {
  if (LAST_BINLOG_REQUEST_TIME + REQUEST_BINLOG_DELAY > now) {
    return;
  }
  struct relative *cur = RELATIVES.next;
  struct relative *best = 0;
  long long best_pos = BINLOG_POSITION;
  int best_color = 0;
  while (cur->type != -1) {
    if (cur->binlog_position > BINLOG_POSITION && cur->link_color >= best_color &&
      (cur->link_color > best_color || cur->binlog_position > best_pos)) {
      if (precise_now >= cur->timestamp + REQUEST_DELAT[cur->link_color]) {
        best_pos = cur->binlog_position;
        best_color = cur->link_color;
        best = cur;
      }
    }
    cur = cur->next;
  }
  if (best) {
    assert (best_color >= 0 && best_color <= 2);
    struct connection *c = get_relative_connection (best);
    if (c) {
      rpc_send_binlog_request (c, best->node.id, -1);
      LAST_BINLOG_REQUEST_TIME = precise_now;
    }
  }
}*/

void request_binlog (void) {
  if (LAST_BINLOG_REQUEST_TIME + REQUEST_BINLOG_DELAY > precise_now) {
    return;
  }
  struct relative *cur = RELATIVES.next;
  struct relative *best = 0;
  int best_color = 0;
  int ncolor = 0;
  while (cur->type != -1) {
    if (cur->binlog_position > BINLOG_POSITION && precise_now >= cur->timestamp + REQUEST_DELAY[cur->link_color]) {
      if (cur->link_color == best_color){
        ncolor ++;
        if (lrand48 () % (ncolor) == 0) {
            best = cur;
        }
      } else if (cur->link_color > best_color) {
        ncolor = 1;
        best_color = cur->link_color;
        best = cur;
      }
    }
    cur = cur->next;
  }
  if (best) {
    assert (best_color >= 0 && best_color <= 2);
    struct connection *c = get_relative_connection (best);
    if (c) {
      rpc_send_binlog_request (c, best->node.id, -1);
      LAST_BINLOG_REQUEST_TIME = precise_now;
    }
  }
}
