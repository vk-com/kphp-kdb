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
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
              2011-2013 Vitaliy Valtman
*/

#ifndef __MC_PROXY_H__
#define __MC_PROXY_H__

#define CLUSTER_MODE_MEMCACHED  0
#define CLUSTER_MODE_FIRSTINT 1
#define CLUSTER_MODE_SECONDINT 2
#define CLUSTER_MODE_THIRDINT 3
#define CLUSTER_MODE_FOURTHINT 4
#define CLUSTER_MODE_FIFTHINT 5
#define CLUSTER_MODE_RANDOM 15
#define CLUSTER_MODE_PERSISTENT 16
#define CLUSTER_MODE_DISABLED	255

#define CLUSTER_MODE_TEXT	0x100
#define CLUSTER_MODE_LISTS	0x200
#define CLUSTER_MODE_HINTS 	0x400
#define CLUSTER_MODE_LOGS	0x800
#define CLUSTER_MODE_NEWS	0x1000
#define CLUSTER_MODE_ROUNDROBIN	0x2000
#define CLUSTER_MODE_BAYES	0x4000


// Merge: 0x8000 ... 0x78000 (Total 15 modes)
#define CLUSTER_MODE_SEARCH	(1 << 15)
#define CLUSTER_MODE_TARG	(2 << 15)
#define CLUSTER_MODE_NEWS_UG	(3 << 15)
#define CLUSTER_MODE_NEWS_COMM	(4 << 15)
#define CLUSTER_MODE_STATSX	(5 << 15)
#define CLUSTER_MODE_FRIENDS (6 << 15)
#define CLUSTER_MODE_SEARCHX	(7 << 15)
#define CLUSTER_MODE_HINTS_MERGE (8 << 15)
#define CLUSTER_MODE_NEWSR (9 << 15)
#define CLUSTER_MODE_RANDOM_MERGE (10 << 15)
#define CLUSTER_MODE_NEWS_G (11 << 15)


#define CLUSTER_MODE_DOT	0x80000
#define CLUSTER_MODE_MAGUS	0x100000
#define CLUSTER_MODE_WATCHCAT	0x200000
#define CLUSTER_MODE_SUPPORT	0x2000000
#define CLUSTER_MODE_PHOTO	0x4000000
#define CLUSTER_MODE_ANTISPAM	0x10000000
#define CLUSTER_MODE_TEMP	0x20000000
//#define CLUSTER_MODE_NEWS_G	0x100000000ll

#define CLUSTER_MODE_MERGE 0x78000

#define TEXT_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_TEXT) != 0)
#define LISTS_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_LISTS) != 0)
#define HINTS_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_HINTS) != 0)
#define LOGS_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_LOGS) != 0)
#define NEWS_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_NEWS) != 0)
#define ROUND_ROBIN_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_ROUNDROBIN) != 0)
#define BAYES_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_BAYES) != 0)
#define SEARCH_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_SEARCH)
#define TARG_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_TARG)
#define NEWS_UG_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_NEWS_UG)
#define NEWS_COMM_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_NEWS_COMM)
#define STATSX_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_STATSX)
#define FRIENDS_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_FRIENDS)
#define SEARCHX_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_SEARCHX)
#define HINTS_MERGE_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_HINTS_MERGE)
#define	NEWSR_EXTENSION  ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_NEWSR)
#define	RANDOM_MERGE_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_RANDOM_MERGE)
#define	NEWS_G_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) == CLUSTER_MODE_NEWS_G)
//#define MERGE_EXTENSION ((CC->cluster_mode & (CLUSTER_MODE_SEARCH | CLUSTER_MODE_TARG | CLUSTER_MODE_NEWS_UG | CLUSTER_MODE_NEWS_COMM | CLUSTER_MODE_STATSX | CLUSTER_MODE_FRIENDS | CLUSTER_MODE_SEARCHX | CLUSTER_MODE_HINTS_MERGE | CLUSTER_MODE_NEWSR | CLUSTER_MODE_RANDOM_MERGE)) != 0)
#define MERGE_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MERGE) != 0)
#define MAGUS_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_MAGUS) != 0)
#define WATCHCAT_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_WATCHCAT) != 0)
#define SUPPORT_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_SUPPORT) != 0)
#define PHOTO_EXTENSION ((CC->cluster_mode & CLUSTER_MODE_PHOTO) != 0)
#define	DOT_EXTENSION  ((CC->cluster_mode & CLUSTER_MODE_DOT) != 0)
#define	ANTISPAM_EXTENSION  ((CC->cluster_mode & CLUSTER_MODE_ANTISPAM) != 0)
#define	TEMP_EXTENSION  ((CC->cluster_mode & CLUSTER_MODE_TEMP) != 0)


#define MAX_CLUSTER_SERVERS 65536
#define MAX_CLUSTERS    256

typedef struct mc_point mc_point_t;

struct mc_point {
  unsigned long long x;
  struct conn_target *target;
};

struct mc_cluster {
  int port;
  int crypto;
  int cluster_mode;
  int tot_buckets;
  int server_socket;
  int cluster_no;
  int other_cluster_no;
  int step;
  int min_connections, max_connections;
  int points_num;
  mc_point_t *points;
  char *cluster_name;
  struct memcache_server_functions mc_proxy_inbound;
  struct connection *listening_connection;
  struct conn_target *buckets[MAX_CLUSTER_SERVERS];
  double get_timeout, set_timeout;
  double a_req, a_rbytes, a_sbytes, a_timeouts;
  long long t_req, t_rbytes, t_sbytes, t_timeouts;
};

extern struct mc_cluster *CC;
struct connection *get_target_connection (struct conn_target *S);
struct conn_query *create_query (struct connection *d, struct connection *c, double timeout);
struct conn_query *create_query_type (struct connection *d, struct connection *c, double timeout, int type);
int query_complete (struct connection *c, int ok);
int query_complete_custom (struct conn_query *q, int ok);

#endif
