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
#include "stdio.h"
#include "assert.h"
#include "net-connections.h"
#include "copyfast-common.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "copyfast-rpc-layout.h"

extern int verbosity;



/****
 *
 * netconf file routines 
 *
 ****/

struct netrule {
  int type;
  int ip;
  int mask1;
  int level1;
  int mask2;
  int level2;
};


#define MAX_RULES 100
struct netrule network[MAX_RULES];
int rules_num;


int read_rule (char *s, struct netrule *A) {
  unsigned x,y,z,t;
  unsigned mask1, mask2;
  int type = 0;
  if (sscanf (s, "%u.%u.%u.%u/%u /%u", &x, &y, &z, &t, &mask1, &mask2) == 6) {
    type = 2;
  } else if (sscanf (s, "%u.%u.%u.%u/%u", &x, &y, &z, &t, &mask1) == 5)  {
    type = 1;
  }
  if (type) {
    if (x >= 256 || y >= 256 || z >= 256 || t >= 256 || mask1 >= 32 || (type == 2 && mask2 >= 32)) {
      return 0;
    }
    A->type = type;
    A->ip = (x << 24) + (y << 16) + (z << 8) + t;
    A->mask1 = (1 << (32 - mask1)) - 1;
    A->mask2 = (1 << (32 - mask2)) - 1;
    A->level1 = 32 - mask1;
    A->level2 = 32 - mask2;
    return 1;
  } else {
    return 0;
  }
}

int read_network_file (char *filename) {
  FILE *f = fopen (filename, "rt");
  if (!f) {
    fprintf (stderr, "can not open network file (error %m)\n");
    return 0;
  }
  char buf[256];
  while (!feof (f)) {
    if (rules_num >= MAX_RULES) {
      fclose (f);
      return rules_num;
    }
    fgets (buf, 255, f);
    rules_num += read_rule(buf, &network[rules_num]);
  }
  if (verbosity) {
    fprintf (stderr, "Read %d rules from network description file\n", rules_num);
  }
  fclose (f);
  return rules_num;
}

int check_rule (struct netrule *A, unsigned ip) {
  assert (A->type);
  return (ip & ~(A->mask1)) == A->ip;
}

int check_common_rule (struct netrule *A, unsigned ip1, unsigned ip2) {
  assert (A->type);
  if (A->type == 1) {
    return check_rule (A, ip1) && check_rule (A, ip2);
  } else {
    return 2 * (check_rule (A, ip1) && check_rule (A, ip2) && ((ip1 & ~(A->mask2)) == (ip2 & ~(A->mask2))));
  }
}

int link_color (unsigned ip1, unsigned ip2) {
  int i;
  int best = 0;
  for (i = 0; i < rules_num; i++) {
    int r = check_common_rule (&network[i], ip1, ip2);
    if (r == 2) {
      return 2;
    }
    if (r == 1) {
      best = 1;
    }
  }
  return best;
}

int link_level (unsigned ip, int color) {
  if (color == 0) {
    return 32;
  }
  if (color == 1) {
    int i;
    int l = 0;
    for (i = 0; i < rules_num; i++) if (network[i].type == 1) {
      struct netrule *A = &network[i];
      if (check_rule (A, ip) && A->level1 > l) {
        l = A->level1;
      }
    }
    return l;
  }
  if (color == 2) {
    int i;
    int l = 0;
    for (i = 0; i < rules_num; i++) if (network[i].type == 2) {
      struct netrule *A = &network[i];
      if (check_rule (A, ip) && A->level2 > l) {
        l = A->level2;
      }
    }
    return l;
  }
  return 0;
}



/****
 *
 * rpc send/receive routines 
 *
 ****/

int rpc_create_query (void *_R, int len, struct connection *c, int op) {
  if (verbosity >= 4) {
    fprintf (stderr, "creating query... len = %d, op = %x\n", len, op);
  }
  assert (len <= MAX_PACKET_LEN && len >= 16);
  if (!c || server_check_ready (c) != cr_ok) {
    if (verbosity >= 4) {
      fprintf (stderr, "not_created: connection_failedn\n");
    }
    return -1;
  }
  int *R = _R;
  R[0] = len;
  R[1] = RPCS_DATA(c)->out_packet_num ++;
  R[2] = op;
  return 0;
}

int rpc_send_query (void *_R, struct connection *c) {
  int *R = _R;
  if (verbosity >= 4) {
    fprintf (stderr, "sending query... len = %d, op = %x\n", R[0], R[2]);
  }
  if (verbosity >= 6) {
    fprintf (stderr, "c = %p, server_check_ready = %d (cr_ok = %d)\n", c, server_check_ready (c), cr_ok);
  }
  assert (c && server_check_ready(c) == cr_ok);
  assert (R[0] <= MAX_PACKET_LEN && R[0] >= 16 && R[0] % 4 == 0);
  if (verbosity >= 10) {
    fprintf (stderr, "LINE %d:", __LINE__);
  }
  rpc_set_crc32 (R);
  if (verbosity >= 10) {
    fprintf (stderr, "LINE %d:", __LINE__);
  }
  write_out (&c->Out, R, R[0]);
  if (verbosity >= 10) {
    fprintf (stderr, "LINE %d:", __LINE__);
    fprintf (stderr, "%p %p %p\n", c->extra, RPCS_FUNC(c)->flush_packet, rpcc_flush_packet);
  }
  RPCS_FUNC(c)->flush_packet(c);
  if (verbosity >= 4) {
    fprintf (stderr, "message_sent\n");
  }
  return 0;
}


/****
 *
 * rpc stats layout
 *
 ****/


char *stats_layout[STATS_INT_NUM + STATS_LONG_NUM + STATS_DOUBLE_NUM] = {"total_children", "total_parents", 
                        "slow_links_num", "medium_links_num", "fast_links_num", 
                        "slow_links_sent_num", "medium_links_sent_num", "fast_links_sent_num", 
                        "slow_links_received_num", "medium_links_received_num", "fast_links_received_num", 
                        "slow_links_requested_num", "medium_links_requested_num", "fast_links_requested_num", 
                        "binlog_position",
                        "slow_links_sent_bytes", "medium_links_sent_bytes", "fast_links_sent_bytes", 
                        "slow_links_received_bytes", "medium_links_received_bytes", "fast_links_received_bytes",
                        "crc64", "last_known_binlog_position",
                        "children_requests_sent", "joined_sent", "stats_sent", "update_stats_sent",
                        "join_ack_received", "children_received", "kicked_received", "delays_received", "request_update_stats_received",
                        "handshake_sent", "handshake_accept_sent", "handshake_reject_sent", "divorce_sent",
                        "binlog_info_sent", "binlog_request_sent", "binlog_data_sent", 
                        "handshake_received", "handshake_accept_received", "handshake_reject_received", "divorce_received", 
                        "binlog_info_received", "binlog_request_received", "binlog_data_received",
                        "last_binlog_update_time", "last_known_binlog_position_time", "disk_read_time", "disk_write_time"};
