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
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>
#include <openssl/sha.h>
#include <signal.h>

#include "kdb-data-common.h"
#include "resolver.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-mysql-server.h"
#include "net-mysql-client.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#define	MAX_NAME_LEN	250

#define	VERSION_STR "db-proxy-0.20"

#define TCP_PORT 3306

#define MAX_NET_RES	(1L << 16)

#define	PING_INTERVAL	10
#define	RESPONSE_FAIL_TIMEOUT	10.0

#define FORWARDED_QUERY_TIMEOUT		3.0
#define FORWARDED_QUERY_TIMEOUT_LARGER	3.3

#define	FORWARD_LOW_WATERMARK	(1L << 24)
#define	FORWARD_HIGH_WATERMARK	(1L << 25)

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	RND_A	1664525
#define RND_C	1013904223

#define RND_A2	1103515245
#define RND_C2	12345

int proxy_server_execute (struct connection *c, int op);
int proxy_client_execute (struct connection *c, int op);

int sqlp_password (struct connection *c, const char *user, char buffer[20]);
int sqlp_wakeup (struct connection *c);
int sqlp_alarm (struct connection *c);
int sqlp_authorized (struct connection *c);
int sqlp_becomes_ready (struct connection *c);
int sqlp_check_ready (struct connection *c);
int sqlps_ready_to_write (struct connection *c);

int mc_db_proxy_stats (struct connection *c);
int mc_db_proxy_get (struct connection *c, const char *key, int len);

struct mysql_server_functions db_proxy_inbound = {
  .execute = proxy_server_execute,
  .sql_password = sqlp_password,
  .sql_wakeup = sqlp_wakeup,
  .sql_alarm = sqlp_alarm,
  .sql_flush_packet = sqls_flush_packet,
  .sql_ready_to_write = sqlps_ready_to_write,
  .sql_check_perm = sqls_default_check_perm,
  .sql_init_crypto = sqls_init_crypto,
  .sql_start_crypto = sqls_start_crypto
};

struct mysql_client_functions db_proxy_outbound = {
  .execute = proxy_client_execute,
  .sql_authorized = sqlp_authorized,
  .sql_becomes_ready = sqlp_becomes_ready,
  .check_ready = sqlp_check_ready,
  .sql_flush_packet = sqlc_flush_packet,
  .sql_check_perm = sqlc_default_check_perm,
  .sql_init_crypto = sqlc_init_crypto,
};

char *sql_username = "boxed";
char *sql_password = "password";
char *sql_database = "boxed_base";

char *sql_username_r = "replicator";
char *sql_password_r = "replicator_password";
char *sql_database_r = "";

struct memcache_server_functions db_proxy_memcache_inbound = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = mc_db_proxy_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = mc_db_proxy_stats,
  .mc_wakeup = mcs_do_wakeup,
  .mc_alarm = mcs_do_wakeup,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};


int backlog = BACKLOG, port = TCP_PORT, memcache_port = 0, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;
int verbosity = 0, test_mode = 0, round_robin = 0, replicator_mode;
int quit_steps;

char *fnames[3];
int fd[3];
long long fsize[3];

// unsigned char is_letter[256];
char *progname = "db-proxy", *username, *logname;

int start_time;
long long netw_queries, minor_update_queries, search_queries;
long long tot_response_words, tot_response_bytes, tot_ok_gathers, tot_bad_gathers;
double tot_ok_gathers_time, tot_bad_gathers_time;

int active_queries, waiting_queries;
int config_reload_time;

long long tot_forwarded_queries, tot_delayed_queries, expired_forwarded_queries, expired_delayed_queries;
long long dropped_queries, failed_queries;

int watermark_queries, watermark_rev_queries;
long long tot_watermark_queries, tot_watermark_rev_queries;

//int reconnect_retry_interval = 20;
//int connection_query_timeout = 1;
//int connection_disable_interval = 20;


#define STATS_BUFF_SIZE	(1 << 20)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];

/* file utils */

int open_file (int x, char *fname, int creat) {
  fnames[x] = fname;
  fd[x] = open (fname, creat > 0 ? O_RDWR | O_CREAT : O_RDONLY, 0600);
  if (creat < 0 && fd[x] < 0) {
    if (fd[x] < 0) {
      fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    }
    return -1;
  }
  if (fd[x] < 0) {
    fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    exit(1);
  }
  fsize[x] = lseek (fd[x], 0, SEEK_END);
  if (fsize[x] < 0) {
    fprintf (stderr, "%s: cannot seek %s: %m\n", progname, fname);
    exit(2);
  }
  lseek (fd[x], 0, SEEK_SET);
  if (verbosity) {
    fprintf (stderr, "opened file %s, fd=%d, size=%lld\n", fname, fd[x], fsize[x]);
  }
  return fd[x];
}

int db_proxy_prepare_stats (struct connection *c) {
  int stats_len = prepare_stats (c, stats_buff, STATS_BUFF_SIZE - 6);
  int uptime = now - start_time;

  dyn_update_stats ();

  stats_len += snprintf (stats_buff + stats_len, STATS_BUFF_SIZE - 6 - stats_len,
	"version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " after commit " COMMIT "\n"
	"heap_allocated\t%ld\n"
	"heap_max\t%ld\n"
        "wasted_heap_blocks\t%d\n"
	"wasted_heap_bytes\t%ld\n"
	"free_heap_blocks\t%d\n"
	"free_heap_bytes\t%ld\n"
  	"config_reload_time\t%d\n"
  	"active_queries\t%d\n"
  	"waiting_queries\t%d\n"
  	"forwarded_qps\t%.3lf\n"
  	"total_forwarded_queries\t%lld\n"
  	"total_delayed_queries\t%lld\n"
  	"expired_forwarded_queries\t%lld\n"
  	"expired_delayed_queries\t%lld\n"
	"immediate_dropped_queries\t%lld\n"
	"immediate_failed_queries\t%lld\n"
  	"watermark_queries\t%d %d\n"
  	"total_watermark_queries\t%lld %lld\n"
  	"total_failed_connections\t%lld\n"
  	"total_connect_failures\t%lld\n",
	(long) (dyn_cur - dyn_first),
	(long) (dyn_last - dyn_first),
	wasted_blocks,
	wasted_bytes,
	freed_blocks,
	freed_bytes,
  	config_reload_time,
  	active_queries,
  	waiting_queries,
  	uptime > 0 ? (double) tot_forwarded_queries / uptime : 0.0,
  	tot_forwarded_queries,
  	tot_delayed_queries,
  	expired_forwarded_queries,
  	expired_delayed_queries,
	dropped_queries,
	failed_queries,
  	watermark_queries, watermark_rev_queries,
  	tot_watermark_queries, tot_watermark_rev_queries,
  	total_failed_connections,
  	total_connect_failures);
  return stats_len;
}

int mc_db_proxy_stats (struct connection *c) {
  int stats_len = db_proxy_prepare_stats (c);
  write_out (&c->Out, stats_buff, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}



#define	MAX_RES 262144
int R[MAX_RES+16], *R_end, R_cnt;


void store_connection (struct connection *c) {
  R_end[0] = c->fd;
  R_end[1] = c->status;
  R_end[2] = c->flags;
  R_end[3] = c->ready;
  R_end[4] = c->unreliability;
  R_end += 5;
}


int dbp_get_connections (struct connection *c, const char *key, int key_len) {
  int i;
  R_end = R;
  for (i = 0; i <= max_connection && R_end < R + MAX_RES; i++) {
    store_connection (&Connections[i]);
  }
  return return_one_key_list (c, key, key_len, max_connection+1, 5, R, R_end - R);
}

int dbp_get_targets (struct connection *c, const char *key, int key_len) {
  int i;
  R_end = R;
  for (i = 0; i < allocated_targets && R_end < R + MAX_RES; i++) {
    R_end[0] = i;
    R_end[1] = Targets[i].outbound_connections;
    R_end[2] = Targets[i].active_outbound_connections;
    R_end[3] = Targets[i].ready_outbound_connections;
    R_end[4] = Targets[i].next_reconnect;
    R_end += 5;
  }
  return return_one_key_list (c, key, key_len, allocated_targets, 5, R, R_end - R);
}


int dbp_get_bad_targets (struct connection *c, const char *key, int key_len) {
  int i;
  R_end = R;
  for (i = 0; i < allocated_targets && R_end < R + MAX_RES; i++) {
    struct conn_target *S = &Targets[i];
    assert (S);
    if (S->min_connections > 0 && !S->active_outbound_connections) {
      R_end[0] = ntohl (S->target.s_addr);
      R_end[1] = S->port;
      R_end += 2;
    }
  }
  return return_one_key_list (c, key, key_len, allocated_targets, 2, R, R_end - R);
}


int dump_outbound_connection (struct connection *c, char *buff, int max_len) {
  struct sqlc_data *D = SQLC_DATA(c);
  return snprintf (buff, max_len,
                         "fd = %d, %s:%d, flags = %d , status = %d, err = %d, gen = %d, skip = %d, basic_type = %d\n"
                         "  Inb = %d + %d, Outb = %d + %d, pend = %d, ready = %d, parse_state=%d, type = %p\n"
                         "  auth state = %d, packet_state = %d\n"
                         "  last_response_time = %.6f, last_query_sent_time = %.6f, last_query_time = %.6f\n",
                         c->fd, show_ip (c->remote_ip), c->remote_port, c->flags, c->status, c->error, c->generation, c->skip_bytes, c->basic_type,
                         c->In.total_bytes, c->In.unprocessed_bytes, c->Out.total_bytes, c->Out.unprocessed_bytes,
                         c->pending_queries, c->ready, c->parse_state, c->type, 
                         D->auth_state, D->response_state,
                         c->last_response_time, c->last_query_sent_time, c->last_query_time);

}


int mc_db_proxy_get (struct connection *c, const char *key, int len) {
  if (len >= 5 && !strncmp (key, "stats", 5)) {
    int len = db_proxy_prepare_stats (c);
    return_one_key (c, key, stats_buff, len);
    return 0;
  }

  if (len >= 8 && !strncmp (key, "#inbound", 8)) {
    int i, ptr = 0;
    for (i = 0; i < MAX_CONNECTIONS; i++) {
      struct connection *c = &Connections[i];
      if (c->basic_type == ct_inbound && ptr < STATS_BUFF_SIZE - 1024) {
        struct sqls_data *D = SQLS_DATA(c);
        ptr += snprintf (stats_buff + ptr, STATS_BUFF_SIZE - 6 - ptr,
                         "fd = %d, %s:%d, flags = %d , status = %d, err = %d, gen = %d, skip = %d, basic_type = %d\n"
                         "  Inb = %d + %d, Outb = %d + %d, pend = %d, ready = %d, parse_state=%d, type = %p\n"
                         "  auth state = %d, packet_state = %d\n"
                         "  last_response_time = %.6f, last_query_time = %.6f\n",
                         c->fd, show_ip (c->remote_ip), c->remote_port, c->flags, c->status, c->error, c->generation, c->skip_bytes, c->basic_type,
                         c->In.total_bytes, c->In.unprocessed_bytes, c->Out.total_bytes, c->Out.unprocessed_bytes,
                         c->pending_queries, c->ready, c->parse_state, c->type, 
                         D->auth_state, D->query_state,
                         c->last_response_time, c->last_query_sent_time);


      }
    }
    return_one_key (c, key, stats_buff, ptr);
    return 0;
  }

  if (len >= 9 && !strncmp (key, "#outbound", 9)) {
    int i, ptr = 0;
    for (i = 0; i < MAX_CONNECTIONS; i++) {
      struct connection *c = &Connections[i];
      if (c->basic_type == ct_outbound && ptr < STATS_BUFF_SIZE - 1024) {
        ptr += dump_outbound_connection (c, stats_buff + ptr, STATS_BUFF_SIZE - 6 - ptr);
      }
    }
    return_one_key (c, key, stats_buff, ptr);
    return 0;
  }

    
  if (!strcmp (key, "#bad_targets")) {
    dbp_get_bad_targets (c, key, len);
    return 0;
  }

  if (!strcmp (key, "#bad_connections")) {
    int i, ptr = 0;
    for (i = 0; i < allocated_targets && R_end < R + MAX_RES; i++) {
      struct conn_target *S = &Targets[i];
      assert (S);
      if (S->min_connections > 0 && !S->active_outbound_connections) {
        struct connection *c;
        for (c = S->first_conn; c != (struct connection *)S; c = c->next) {
          if (c->basic_type == ct_outbound && ptr < STATS_BUFF_SIZE - 1024) {
            ptr += dump_outbound_connection (c, stats_buff + ptr, STATS_BUFF_SIZE - 6 - ptr);
          }
        }
      }
    }
    return_one_key (c, key, stats_buff, ptr);
    return 0;
  }

  if (!strcmp (key, "#targets")) {
    dbp_get_targets (c, key, len);
    return 0;
  } 
  
  if (!strcmp (key, "#connections")) {
    dbp_get_connections (c, key, len);
    return 0;
  } 
  
  return 0;
}



/*
 *
 *	CONFIGURATION PARSER
 *
 */

#define	MAX_CLUSTER_SERVERS	16384

#define	IN_CLUSTER_CONFIG	1
#define	IN_CLUSTER_CONNECTED	2


struct conn_target default_ct = {
.min_connections = 2,
.max_connections = 7,
.type = &ct_mysql_client,
.extra = &db_proxy_outbound,
.reconnect_timeout = 1,
.port = 3306
};

int default_target_port = 3306;


#define	MAX_CONFIG_SIZE	(1 << 19)

struct db_table;

#define FE_HOSTNAME	1
#define	FE_TABLEREF	2
#define	FE_NULL		8

#define MAX_FWD_CHAIN_LEN	32
#define MAX_HEIGHT	16

struct fwd_entry {
  struct fwd_entry *fwd_next;
  union {
    struct conn_target *target;
    struct db_table *table_ref;
    char *name;
  };
  int flags;
  int weight;
};

struct db_table {
  struct fwd_entry *fwd_first;
  char *table_name;
  short table_name_len;
  short height;
  int tot_weight;
};

char config_buff[MAX_CONFIG_SIZE+4], *config_filename, *cfg_start, *cfg_end, *cfg_cur;
int config_bytes, cfg_lno, cfg_lex = -1;

struct db_table *universal_target;

#define HASH_PRIME 23917

typedef struct db_table db_hash_table_t[HASH_PRIME];
typedef struct db_table *db_hash_table_p;


db_hash_table_t db_hash0, db_hash1;
struct db_table *db_hash = db_hash0, *db_hash_new = db_hash1;

void clear_fwd_chain (struct fwd_entry *FE) {
  while (FE) {
    struct fwd_entry *FN = FE->fwd_next;
    if (FE->flags & FE_HOSTNAME) {
      destroy_target (FE->target);
    }
    zfree (FE, sizeof (struct fwd_entry));
    FE = FN;
  }
}

void clear_db_hash_table (db_hash_table_p db_hash) {
  int i;
  for (i = 0; i < HASH_PRIME; i++) {
    struct db_table *table = db_hash + i;
    if (table->table_name_len) {
      zfree (table->table_name, table->table_name_len + 1);
    }
    if (table->fwd_first && table->fwd_first != (void *) -1) {
      clear_fwd_chain (table->fwd_first);
    }
  }
  memset (db_hash, 0, sizeof (db_hash_table_t));
}

struct db_table *get_db_hash (db_hash_table_p db_hash, char *name, int l, int force) {
  int h1 = l, h2 = l, t;
  char *ptr;
  for (ptr = name, t = l; t--; ptr++) {
    if (*ptr >= 'A' && *ptr <= 'Z') {
      *ptr += 32;
    }
    h1 = (h1 * 239 + *ptr) % HASH_PRIME;
    h2 = (h2 * 17 + *ptr) % (HASH_PRIME - 1);
  }
  ++h2;
  while (db_hash[h1].table_name) {
    if (l == db_hash[h1].table_name_len && 
         !memcmp (db_hash[h1].table_name, name, l)) {
      return &db_hash[h1];
    }
    h1 += h2;
    if (h1 >= HASH_PRIME) { 
      h1 -= HASH_PRIME;
    }
  }
  if (force) {
    char *temp = zmalloc (l + 1);
    memcpy (temp, name, l);
    temp[l] = 0;
    db_hash[h1].table_name = temp;
    db_hash[h1].table_name_len = l;
    return &db_hash[h1];
  }
  return 0;
}

int dfs_check (struct db_table *A) {
  if (A->height != 1000) {
    return A->height;
  }
  A->height++;
  struct fwd_entry *FE;
  int mx = 0;
  for (FE = A->fwd_first; FE; FE = FE->fwd_next) {
    if (FE->flags == FE_TABLEREF) {
      int t = dfs_check (FE->table_ref);
      if (t > mx) {
	mx = t;
      }
      if (t > MAX_HEIGHT) {
	break;
      }
    }
  }
  if (mx != 999) {
    mx++;
  }
  return A->height = mx;
}

static int cfg_skipspc (void) {
  while (*cfg_cur == ' ' || *cfg_cur == 9 || *cfg_cur == 13 || *cfg_cur == 10 || *cfg_cur == '#') {
    if (*cfg_cur == '#') {
      do cfg_cur++; while (*cfg_cur && *cfg_cur != 10);
      continue;
    }
    if (*cfg_cur == 10) { 
      cfg_lno++; 
    }
    cfg_cur++;
  }
  return (unsigned char) *cfg_cur;
}

static int cfg_skspc (void) {
  while (*cfg_cur == ' ' || *cfg_cur == 9 || *cfg_cur == 10 || *cfg_cur == 13) {
    cfg_cur++;
  }
  return (unsigned char) *cfg_cur;
}

static int cfg_getlex (void) {
  switch (cfg_skipspc()) {
  case '*':
  case '$':
  case '=':
  case ';':
  case ':':
  case '{':
  case '}':
    return cfg_lex = *cfg_cur++;
  case 'l':
    if (!memcmp (cfg_cur, "listen", 6)) {
      cfg_cur += 6;
      return cfg_lex = 'l';
    }
    break;
  case 0:
    return cfg_lex = 0;
  }
  return cfg_lex = -1;
}

static int cfg_getword (void) {
  cfg_skspc();
  char *s = cfg_cur;
  while ((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') || *s == '.' || *s == '-' || *s == '_') {
    s++;
  }
  return s - cfg_cur;
}

/*static int cfg_getint (void) {
  cfg_skspc();
  char *s = cfg_cur;
  int x = 0;
  while (*s >= '0' && *s <= '9') {
    x = x*10 + (*s++ - '0');
  }
  cfg_cur = s;
  return x;
}*/

static int syntax (const char *msg) {
  char *ptr = cfg_cur, *end = ptr + 20;
  if (!msg) {
    msg = "syntax error";
  }
  if (cfg_lno) {
    fprintf (stderr, "%s:%d: ", config_filename, cfg_lno);
  }
  fprintf (stderr, "fatal: %s near ", msg);
  while (*ptr && *ptr != 13 && *ptr != 10) {
    putc (*ptr++, stderr);
    if (ptr > end) {
      fprintf (stderr, " ...");
      break;
    }
  }
  putc ('\n', stderr);

  return -1;
}

static int expect (int lexem) {
  if (cfg_lex != lexem) {
    static char buff[32];
    sprintf (buff, "%c expected", lexem);
    return syntax (buff);
  }
  return 0;
}

#define Expect(l) { int t = expect(l); if (t < 0) { return t; } }

// flags: +1 = do not read file, use buffer data;  +2 = skip target creation;  +4 = pre-build table set only
int parse_config (db_hash_table_p db_hash, int flags) {
  int r, l;
  //char *ptr, *s;
  struct hostent *h;
  struct db_table *B;

  if (!(flags & 1)) {
    config_bytes = r = read (fd[0], config_buff, MAX_CONFIG_SIZE+1);
  } else {
    r = config_bytes;
  }

  if (r < 0) {
    fprintf (stderr, "error reading configuration file %s: %m\n", config_filename);
    return -2;
  }
  if (r > MAX_CONFIG_SIZE) {
    fprintf (stderr, "configuration file %s too long (max %d bytes)\n", config_filename, MAX_CONFIG_SIZE);
    return -2;
  }
  cfg_cur = cfg_start = config_buff;
  cfg_end = cfg_start + r;
  *cfg_end = 0;
  cfg_lno = 0;

  while (1) {
    //fprintf (stderr, "cfg_cur points to: %.*s\n", 10, cfg_cur);
    cfg_skipspc ();

    if (*cfg_cur == '*' && (cfg_cur[1] == ' ' || cfg_cur[1] == '\t')) {
      l = 1;
    } else if (*cfg_cur == '@') {
      int vt = 0;
      cfg_cur++;
      l = cfg_getword ();
      if (l == sizeof("min_connections") - 1 && !memcmp (cfg_cur, "min_connections", l)) {
        vt = 1;
      } else if (l == sizeof("max_connections") - 1 && !memcmp (cfg_cur, "max_connections", l)) {
        vt = 2;
      } else if (l == sizeof("target_port") - 1 && !memcmp (cfg_cur, "target_port", l)) {
        vt = 3;
      } else if (l == sizeof("sql_password") - 1 && !memcmp (cfg_cur, "sql_password", l)) {
        vt = 16;
      };
      if (!vt) {
        return syntax ("unknown variable");
      }
      cfg_cur += l;
      l = cfg_getword ();
      char *tmp;

      if (vt < 16) {
        int val = strtoul (cfg_cur, &tmp, 0);
        if (tmp != cfg_cur + l) {
          return syntax ("integer expected");
        }
        cfg_cur += l;
        switch (vt) {
        case 1:
          if (val >= 0 && val <= 16000) {
            default_ct.min_connections = val;
          }
          break;
        case 2:
          if (val >= 0 && val <= 16383) {
            default_ct.max_connections = val;
          }
          break;
        case 3:
          if (val > 0 && val < 65536) {
            default_ct.port = default_target_port = val;
          }
          break;
        }
      } else {
        tmp = malloc (l + 1);
        memcpy (tmp, cfg_cur, l);
        tmp[l] = 0;
        cfg_cur += l;
        switch (vt) {
        case 16:
          sql_password = tmp;
//          fprintf (stderr, "sql_password set to '%s'\n", sql_password);
          break;
        }
      }
      cfg_getlex ();
      Expect (';');
      continue;
    } else {
      l = cfg_getword ();
    }
    //fprintf (stderr, "word length: %d\n", l);
    if (!l) {
      break; 
    }
    if (l > 255) {
      return syntax ("table name too long");
    }
    B = get_db_hash (db_hash, cfg_cur, l, 1);
    if (flags & 4) {
      if (B->fwd_first) {
	return syntax ("duplicate table name");
      } else {
	B->fwd_first = (void *) -1;
      }
    } else {
      assert (B->fwd_first == ((flags & 2) ? (void *) -1 : 0));
      B->fwd_first = 0;
    }
    B->height = 0;

    cfg_cur += l;

    int count = 0;
    struct fwd_entry *FP = 0;

    while (1) {
      int c = cfg_skspc ();
      int type = 0;
      if (c == ';' || count > MAX_FWD_CHAIN_LEN) {
	break;
      }
      count++;
      if (c == '$') {
	type = FE_TABLEREF;
	cfg_cur++;
      } else if (c == '=') {
	type = FE_HOSTNAME;
	cfg_cur++;
      } else if (c == '?') {
	type = FE_NULL;
	l = 1;
      }

      if (type != FE_NULL) {
	l = cfg_getword ();
      }

      if (!l) {
	return syntax ("hostname or identifier expected");
      }

      c = cfg_cur[l];
      cfg_cur[l] = 0;

      if (c == ':') {
	if (type == FE_TABLEREF) {
	  return syntax ("colon unexpected after a table reference");
	}
	if (type == FE_NULL) {
	  return syntax ("colon unexpected after null reference");
	}
	type = FE_HOSTNAME;
      }

      struct db_table *B1 = 0;

      if (!(type & (FE_HOSTNAME | FE_NULL))) {
	B1 = get_db_hash (db_hash, cfg_cur, l, 0);
	if (B1) {
	  if (B1 == B && !type) {
	    type = FE_HOSTNAME;
	  } else {
	    type = FE_TABLEREF;
	  }
	}
      }
	
      if (!type && !(flags & 4)) {
	type = FE_HOSTNAME;
      }

      if (type == FE_HOSTNAME) {

	if (!(h = kdb_gethostbyname (cfg_cur)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
	  return syntax("cannot resolve hostname");
	}
	default_ct.target = *((struct in_addr *) h->h_addr);
	default_ct.port = default_target_port;
	default_ct.reconnect_timeout = 1.0 + 0.1 * drand48 ();

	if (default_ct.min_connections > default_ct.max_connections) {
	  return syntax("min_connections is greater than max_connections");
	}

	*(cfg_cur += l) = c;

	if (*cfg_cur == ':') {
	  char *tmp;
	  cfg_getlex ();
	  Expect (':');
	  l = cfg_getword ();
	  c = cfg_cur[l];
	  cfg_cur[l] = 0;
	  default_ct.port = strtoul (cfg_cur, &tmp, 10);

          //fprintf (stderr, "parsed port %d\n", default_ct.port);

	  if (tmp != cfg_cur + l || (unsigned)default_ct.port > 65535) {
	    return syntax ("port number expected");
	  }
	  *(cfg_cur += l) = c;
	}
      } else {
	*(cfg_cur += l) = c;
      }

      int weight = 1;
      if (cfg_skspc() == '*') {
	char *tmp;
	cfg_getlex ();
	Expect ('*');
	l = cfg_getword ();
	c = cfg_cur[l];
	cfg_cur[l] = 0;
	weight = strtol (cfg_cur, &tmp, 10);
	if (tmp != cfg_cur + l || !weight || (unsigned) weight > 65535) {
	    return syntax ("weight from 1 to 1000 expected");
	}
	*(cfg_cur += l) = c;
      }

      if (!(flags & 6)) {
	struct fwd_entry *FE = zmalloc (sizeof (struct fwd_entry));
	FE->fwd_next = 0;
	FE->flags = type;
	FE->weight = weight;
	B->tot_weight += weight;
	
	switch (type) {
	case FE_NULL:
	  FE->target = 0;
	  break;
	case FE_HOSTNAME:
	  FE->target = create_target (&default_ct, 0);

	  if (verbosity > 0) {
	    fprintf (stderr, "server: ip %s, port %d\n", inet_ntoa (FE->target->target), FE->target->port);
	  }
	  break;
	case FE_TABLEREF:
	  B->height = 1000;
	  FE->table_ref = B1;
	  break;
	default:
	  assert (0);
	}

	if (!FP) {
	  B->fwd_first = FE;
	} else {
	  FP->fwd_next = FE;
	}
	FP = FE;
      }
    }

    if (!count) {
      return syntax ("target identifier expected");
    }

    cfg_getlex ();
    Expect (';');
  }

  cfg_getlex ();
  if (cfg_lex) {
    return syntax ("EOF expected");
  }

  if (!(flags & 6)) {
    int i;
    for (i = 0; i < HASH_PRIME; i++) {
      struct db_table *table = db_hash + i;
      if (table->table_name && table->height == 1000) {
	if (dfs_check (table) > MAX_HEIGHT) {
	  static char buff[1024];
	  snprintf (buff, 1023, "circular reference or too long indirection chain for '%.255s'", table->table_name); 
	  return syntax (buff);
	}
      }
    }

    universal_target = get_db_hash (db_hash, "*", 1, 0);
  }

  return 0;
}

static int need_reload_config = 0;

int do_reload_config (int create_conn) {
  int res;
  need_reload_config = 0;

  fd[0] = open (config_filename, O_RDONLY);
  if (fd[0] < 0) {
    fprintf (stderr, "cannot re-read config file %s: %m\n", config_filename);
    return -3;
  }

  res = kdb_load_hosts ();
  if (res > 0 && verbosity > 0) {
    fprintf (stderr, "/etc/hosts changed, reloaded\n");
  }

  res = parse_config (db_hash_new, 4);

  close (fd[0]);

  if (res < 0) {
    clear_db_hash_table (db_hash_new);
    fprintf (stderr, "error while re-reading config file %s, new configuration NOT applied\n", config_filename);
    return res;
  }

  res = parse_config (db_hash_new, 3);

  if (res < 0) {
    clear_db_hash_table (db_hash_new);
    fprintf (stderr, "error while re-reading config file %s, new configuration NOT applied\n", config_filename);
    return res;
  }

  res = parse_config (db_hash_new, 1);

  if (res < 0) {
    clear_db_hash_table (db_hash_new);
    fprintf (stderr, "fatal error while re-reading config file %s\n", config_filename);
    exit (-res);
  }

  clear_db_hash_table (db_hash);

  db_hash_table_p tmp = db_hash;
  db_hash = db_hash_new;
  db_hash_new = tmp;

  if (create_conn) {
    create_all_outbound_connections ();
  }

  config_reload_time = now;

  if (verbosity > 0) {
    fprintf (stderr, "configuration file %s re-read successfully, new configuration active\n", config_filename);
  }

  return 0;
}

/*
 *
 *	PROXY SQL SERVER FUNCTIONS
 *
 */

int forward_query (struct connection *c, int query_len, char *name, int name_len, int timeout, int op, int q_type);


struct conn_target *calculate_table_target (char *name, int name_len, int is_update, unsigned seed) {
  if (name_len < 0 || name_len > MAX_NAME_LEN) {
    return 0;
  }

  struct db_table *B = get_db_hash (db_hash, name, name_len, 0);
  if (!B) {
    if (!universal_target) {
      return 0;
    }
    B = universal_target;
  }

  if (!seed) {
    is_update = 1;
  }

  int it = 0;
  while (1) {
    assert (B->tot_weight > 0);
    struct fwd_entry *FE = B->fwd_first;
    assert (FE);
    if (!is_update) {
      int rnd = (seed * (unsigned long long) B->tot_weight) >> 32;
      seed = seed * RND_A + RND_C;
      while ((rnd -= FE->weight) >= 0) {
	FE = FE->fwd_next;
	assert (FE);
      }
    }
    switch (FE->flags) {
    case FE_HOSTNAME:
      return FE->target;
    case FE_NULL:
      return 0;
    case FE_TABLEREF:
      B = FE->table_ref;
      break;
    default:
      assert (0);
    }
    assert (it++ <= MAX_HEIGHT);
  }
}

static inline void rotate_target (struct conn_target *S, struct connection *c) {
  S->last_conn->next = S->first_conn;
  S->first_conn->prev = S->last_conn;
  S->first_conn = c->next;
  S->last_conn = c;
  S->first_conn->prev = S->last_conn->next = (struct connection *) S;
}

struct connection *get_target_connection (struct conn_target *S) {
  struct connection *c, *d = 0;
  int r, pr = cr_notyet, u = 10000;
  if (!S) {
    return 0;
  }
  for (c = S->first_conn; c != (struct connection *) S; c = c->next) {
    r = sqlp_check_ready (c);
    if (verbosity > 1) {
      fprintf (stderr, "checked connection %p (%d %s:%d): ready status = %d (connect status %d)\n", c, c->fd, show_ip (c->remote_ip), c->remote_port, r, c->status);
    }
    if (r == cr_ok) {
      if (round_robin) {
        rotate_target (S, c);
      }
      return c;
    } else
    if (r == cr_busy) {
      u = -(1 << 30);
      d = c;
      pr = r;
    } else if (r == cr_stopped && c->unreliability < u) {
      u = c->unreliability;
      d = c;
      pr = r;
    }
  }

  if (S->next_reconnect_timeout > 5.0 && (pr == cr_notyet || pr == cr_busy)) {
    return 0;
  }

  /* all connections failed? */
  return d;
}

char *t_skipspc (char *ptr, char *end) {
  while (1) {
    while (ptr < end && (*ptr == ' ' || *ptr == 9 || *ptr == 13 || *ptr == 10)) {
      ptr++;
    }
    if (ptr + 2 <= end && ptr[0] == '*' && ptr[1] == '/') {
      ptr += 2;
      continue;
    }
    if (ptr + 2 > end || ptr[0] != '/' || ptr[1] != '*') {
      break;
    }
    ptr += 2;
    if (ptr < end && (ptr[0] == '!' || ptr[0] == '?')) {
      ptr++;
      continue;
    }
    while (ptr + 2 <= end && (ptr[0] != '*' || ptr[1] != '/')) {
      ptr++;
    }
    if (ptr + 2 > end) {
      return end;
    }
    ptr += 2;
  }
  return ptr;
}

int t_getlex (char *ptr, char *end) {
  char *start = ptr;
  if (ptr >= end) {
    return 0;
  }
  switch (*ptr) {
  case '`':
    ptr++;
    while (ptr < end && *ptr >= '0' && *ptr != '`') {
      if (*ptr >= 'a' && *ptr <= 'z') {
        *ptr -= 0x20;
      }
      ptr++;
    }
    return (ptr < end && *ptr == '`' ? ptr - start + 1 : -1);
  case 39:
    ptr++;
    while (ptr < end && *ptr != 39) {
      ptr++;
    }
    return (ptr < end && *ptr == 39 ? ptr - start + 1 : -1);
  case '"':
    ptr++;
    while (*ptr != '"' && ptr < end) {
      if (*ptr == '\\' && ptr + 1 < end) {
        ptr += 2;
      } else {
        ptr++;
      }
    }
    if (ptr < end) {
      return ptr + 1 - start;
    } else {
      return -1;
    }
  case 'A' ... 'Z':  case 'a' ... 'z':  case '_':
    while (ptr < end && ((*ptr >= 'A' && *ptr <= 'Z') || (*ptr >= 'a' && *ptr <= 'z') || (*ptr >= '0' && *ptr <= '9') || *ptr == '_')) {
      if (*ptr >= 'a' && *ptr <= 'z') {
        *ptr -= 0x20;
      }
      ptr++;
    }
    return ptr - start;
  case '\t': case '\r': case '\n': case ' ':
    return -1;
  case '0' ... '9':
    while (ptr < end && (*ptr >= '0' && *ptr <= '9')) {
      ptr++;
    }
    if (ptr + 1 < end && *ptr == '.' && (ptr[1] >= '0' && ptr[1] <= '9')) {
      ptr += 2;
      while (ptr < end && (*ptr >= '0' && *ptr <= '9')) {
        ptr++;
      }
    }
    return ptr - start;
  default:
    return 1;
  }
}
    

char *find_table_name (char *ptr, char *end, int *len, int *timeout) {
  int clen, next = 0;
  *len = 0;
  *timeout = 0;
  while (ptr < end) {
    ptr = t_skipspc (ptr, end);
    clen = t_getlex (ptr, end);
    if (clen <= 0) {
      return 0;
    }
    if (next == 1) {
      if ((*ptr >= 'A' && *ptr <= 'Z') || (*ptr >= 'a' && *ptr <= 'z')) {
        *len = clen;
        return ptr;
      }
      if (*ptr == '`' && clen >= 3 && ptr[clen-1] == '`') {
        *len = clen - 2;
        return ptr + 1;
      }
      return 0;
    }
    if (next == 2) {
      if (*ptr >= '0' && *ptr <= '9' && ((clen == 3 && ptr[1] == '.') || (clen == 4 && ptr[2] == '.'))) {
        char buf[8], *tmp;
        memcpy (buf, ptr, clen);
        buf[clen - 2] = buf[clen - 1];
        buf[clen - 1] = 0;
        int cur = strtoul (buf, &tmp, 10);
        if (tmp == buf + clen - 1 && cur >= 1 && cur <= 300) {
          *timeout = cur;
        }
      }
      next = 0;
    }
    if ((*ptr >= 'A' && *ptr <= 'Z') || (*ptr >= 'a' && *ptr <= 'z')) {
      next = 0;
      if (clen == 4 && !memcmp (ptr, "INTO", 4)) {
        next = 1;
      } else if (clen == 4 && !memcmp (ptr, "FROM", 4)) {
        next = 1;
      } else if (clen == 5 && !memcmp (ptr, "TABLE", 5)) {
        next = 1;
      } else if (clen == 6 && !memcmp (ptr, "UPDATE", 6)) {
        next = 1;
      } else if (clen == 7 && !memcmp (ptr, "TIMEOUT", 7)) {
        next = 2;
      }
    }
    ptr += clen;
  }
  return 0;
}

// -1 - SELECT
// 0 - unknown
// 1 - modification
int find_query_type (char *ptr, char *end) {
  int clen;
  while (ptr < end) {
    ptr = t_skipspc (ptr, end);
    clen = t_getlex (ptr, end);
    if (clen <= 0) {
      return 0;
    }
    if ((*ptr >= 'A' && *ptr <= 'Z') || (*ptr >= 'a' && *ptr <= 'z')) {
      if (clen == 6) {
	if (!memcmp (ptr, "SELECT", 6)) {
	  return -1;
	} else if (!memcmp (ptr, "UPDATE", 6)) {
	  return 1;
	} else if (!memcmp (ptr, "INSERT", 6)) {
	  return 1;
	} else if (!memcmp (ptr, "DELETE", 6)) {
	  return 1;
	}
      }
      if (clen == 4 && !memcmp (ptr, "SHOW", 4)) {
	return -1;
      }
    }
    ptr += clen;
  }
  return 0;
}

int proxy_server_execute (struct connection *c, int op) {
  struct sqls_data *D = SQLS_DATA(c);
  nb_iterator_t R;
  int len = 0, res, timeout = 0;
  char *ptr, *ptr_e, *table = 0;
  static char query_buffer[16384];


  if (verbosity > 0) {
    fprintf (stderr, "proxy_db_server: op=%d\n", op);
  }

  res = sqls_builtin_execute (c, op);
  if (res != SKIP_ALL_BYTES) {
    return res;
  }

  int q_type = 0;

  if (op == MYSQL_COM_BINLOG_DUMP && replicator_mode) {
    if (verbosity > 1) {
      fprintf (stderr, "connection %d: forwarding sql op %d in special replicator mode\n", c->fd, op);
    }
  } else if (op != MYSQL_COM_QUERY) {
    if (verbosity > 1) {
      fprintf (stderr, "connection %d: unknown sql op %d, sending Failed packed\n", c->fd, op);
    }
    send_error_packet (c, 1045, 28000, "Failed", 6, ++D->output_packet_seq);
    return SKIP_ALL_BYTES;
  } else {
    nbit_set (&R, &c->In);
    len = nbit_read_in (&R, query_buffer, 16380);
    assert (len >= 5);
    ptr = query_buffer + 5;
    ptr_e = query_buffer + len;

    if (verbosity > 0) {
      fprintf (stderr, "got query: '%.*s'\n", ptr_e - ptr < 256 ? (int) (ptr_e - ptr) : 256, ptr);
    }

    table = find_table_name (ptr, ptr_e, &len, &timeout);
    q_type = find_query_type (ptr, ptr_e);

    if (verbosity > 0) {
      fprintf (stderr, "table name: '%.*s', query type %d\n", len, table, q_type);
    }
  }

  if (!table && !universal_target) {
    if (verbosity > 1) {
      fprintf (stderr, "connection %d: didn't detect table name in query, sending Failed\n", c->fd);
    }
    send_error_packet (c, 1045, 28000, "Failed", 6, ++D->output_packet_seq);
    return SKIP_ALL_BYTES;
  }

  c->query_start_time = get_utime_monotonic ();
  c->queries_ok = 0;

  switch (D->query_state) {
    case query_none:
      if (forward_query (c, D->packet_len, table, len, timeout, op, q_type) < 0) {
        if (verbosity > 1) {
          fprintf (stderr, "connection %d: no target to forward query for table (%.*s), sending Failed\n", c->fd, len, table);
        }
	dropped_queries++;
        send_error_packet (c, 1045, 28000, "Failed", 6, ++D->output_packet_seq);
      }
      return 0;
    case query_failed:
      if (verbosity > 1) {
        fprintf (stderr, "connection %d: query is in query_failed state, sending Failed and skipping query\n", c->fd);
      }
      failed_queries++;
      send_error_packet (c, 1045, 28000, "Failed", 6, ++D->output_packet_seq);
      D->query_state = query_none;
      c->generation = ++conn_generation;
      c->pending_queries = 0;
      return SKIP_ALL_BYTES;
    default:
      fprintf (stderr, "connection %d: invalid query_state %d\n", c->fd, D->query_state);
      assert (0);
  }

  return 0;
}


int tot_queries;

int delete_query (struct conn_query *q);
int query_timeout (struct conn_query *q);
int delayed_query_timeout (struct conn_query *q);
int delete_watermark_query (struct conn_query *q);


conn_query_type_t proxy_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "db-proxy-query",
.parse_execute = server_failed,
.close = delete_query,
.wakeup = query_timeout
};

conn_query_type_t proxy_delayed_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "db-proxy-delayed_query",
.parse_execute = server_failed,
.close = delete_query,
.wakeup = delayed_query_timeout
};

conn_query_type_t proxy_watermark_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "db-proxy-watermark_query",
.parse_execute = server_failed,
.close = delete_watermark_query,
.wakeup = delete_watermark_query
};

int create_delayed_query (struct conn_target *t, struct connection *c, double timeout) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_delayed_query(%p, %p[%d]): Q=%p\n", t, c, c->fd, Q);
  }

  /*Q->custom_type = SQLS_DATA(c)->query_type;*/
  Q->requester = c;
  Q->start_time = c->query_start_time;
  Q->extra = 0;
  Q->cq_type = &proxy_delayed_query_type;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  SQLS_DATA(c)->query_state = query_wait_target;

  insert_conn_query_into_list (Q, (struct conn_query *)t);
  waiting_queries++;
  tot_delayed_queries++;

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query_into_list()\n");
  }

  return 1;
}



int create_query (struct connection *d, struct connection *c, double timeout) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_query(%p[%d], %p[%d]): Q=%p\n", d, d->fd, c, c->fd, Q);
  }

  /*Q->custom_type = SQLS_DATA(c)->query_type;*/
  Q->outbound = d;
  Q->requester = c;
  Q->start_time = c->query_start_time;
  Q->extra = 0;
  Q->cq_type = &proxy_query_type;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  SQLS_DATA(c)->query_state = query_running;

  insert_conn_query (Q);
  active_queries++;
  tot_forwarded_queries++;

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query()\n");
  }

  return 1;
}

int create_reverse_watermark_query (struct connection *c, struct connection *d) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_rev_watermark_query(%p[%d], %p[%d]): Q=%p\n", d, d->fd, c, c->fd, Q);
  }

  /*Q->custom_type = SQLS_DATA(c)->query_type;*/
  Q->outbound = c;
  Q->requester = d;
  Q->start_time = precise_now;
  Q->extra = 0;
  Q->cq_type = &proxy_watermark_query_type;
  Q->timer.wakeup_time = 0;

//  SQLS_DATA(c)->query_state = query_running;

  push_conn_query (Q);
  watermark_rev_queries++;
  tot_watermark_rev_queries++;

  if (verbosity > 1) {
    fprintf (stderr, "after push_conn_query()\n");
  }

  return 1;
}



//After call to this function, there are no additional bytes in c->In to skip
int forward_query (struct connection *c, int query_len, char *name, int name_len, int timeout, int op, int q_type) {
  int i;
  struct conn_target *S; 
  struct connection *d = 0;
  unsigned seed = SQLS_DATA(c)->custom;

  if (verbosity > 1) {
    fprintf (stderr, "forward_query at connection %d, table '%.*s': op=%d, q_type=%d, seed=%u\n", c->fd, name_len, name, op, q_type, seed);
  }

  for (i = 0; i < 10 && !d; i++) {
    S = calculate_table_target (name, name_len, q_type != -1, seed);
    d = get_target_connection (S);
    seed = seed * RND_A2 + RND_C2;
  }

  double smaller_timeout, larger_timeout;

  if (!d) {
    if (verbosity > 0) {
      fprintf (stderr, "cannot find connection to target %p (%s:%d) for table %.*s, dropping query\n", S, S ? conv_addr(S->target.s_addr, 0) : "?", S ? S->port : 0, name_len, name);
    }
    assert (advance_skip_read_ptr (&c->In, query_len + 4) == query_len + 4);
    return -1;
  }

  if (q_type != -1) {
    SQLS_DATA(c)->custom = 0;  // force master for all future queries in this session
    if (verbosity > 1) {
      fprintf (stderr, "connection %d: non-select query detected, all further queries will be forwarder to master copies\n", c->fd);
    }
  }

  if (!timeout) {
    smaller_timeout = FORWARDED_QUERY_TIMEOUT;
    larger_timeout = FORWARDED_QUERY_TIMEOUT_LARGER;
    if (d->status == conn_ready) {
      d->last_query_time = 0;
    }
  } else {
    smaller_timeout = timeout * 0.1;
    larger_timeout = smaller_timeout + (FORWARDED_QUERY_TIMEOUT_LARGER - FORWARDED_QUERY_TIMEOUT);
    if (d->status == conn_ready) {
      d->last_query_time = smaller_timeout;
    }
  }


  if (d->status != conn_ready) {
    c->generation = ++conn_generation;
    c->pending_queries = 0;
    create_delayed_query (S, c, larger_timeout);
    c->status = conn_wait_net;
    set_connection_timeout (c, smaller_timeout);
  } else {
    c->generation = ++conn_generation;
    c->pending_queries = 0;
    create_query (d, c, op == MYSQL_COM_BINLOG_DUMP ? 1e6 : larger_timeout);
    assert (copy_through (&d->Out, &c->In, query_len + 4) == query_len + 4);

    if (op == MYSQL_COM_BINLOG_DUMP) {
      SQLC_DATA(d)->extra_flags |= 1;
    }

    SQLC_FUNC(d)->sql_flush_packet (d, query_len);
    c->status = conn_wait_net;
    flush_connection_output (d);
    d->last_query_sent_time = precise_now;
    d->status = conn_wait_answer;
    SQLC_DATA(d)->response_state = resp_first;

    if (op != MYSQL_COM_BINLOG_DUMP) {
      set_connection_timeout (c, smaller_timeout);
    }
  }
  return 0;
}


int forward_response (struct connection *c, int response_len, int wakeflag) {
  struct connection *d = c->first_query->requester;
  assert (d);
  if (d->generation == c->first_query->req_generation) {
    SQLS_DATA(d)->output_packet_seq = SQLC_DATA(c)->packet_seq;
    assert (copy_through (&d->Out, &c->In, response_len + 4) == response_len + 4);
    SQLS_FUNC(d)->sql_flush_packet (d, response_len);
    if (wakeflag) {
      d->flags |= C_WANTWR;
      put_event_into_heap (d->ev);
    }
  } else {
    assert (advance_skip_read_ptr (&c->In, response_len + 4) == response_len + 4);
  }
//  flush_connection_output (d);
  return 0;
}

int stop_forwarding_response (struct connection *c) {
  struct connection *d = c->first_query->requester;
  assert (d);
  if (d->generation != c->first_query->req_generation || d->Out.total_bytes + d->Out.unprocessed_bytes <= FORWARD_HIGH_WATERMARK) {
    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "forwarding response from outbound connection %d to connection %d stopped: already %d+%d bytes in output buffers.\n", c->fd, d->fd, d->Out.total_bytes, d->Out.unprocessed_bytes);
  }
  d->write_low_watermark = FORWARD_LOW_WATERMARK;
  c->flags |= C_STOPREAD;
  create_reverse_watermark_query (d, c);
  c->status = conn_wait_net;
  return 1;
}


int sqlp_becomes_ready (struct connection *c) {
  struct conn_query *q;

  while (c->target->first_query != (struct conn_query *)(c->target)) {
    q = c->target->first_query;
    //    fprintf (stderr, "processing delayed query %p for target %p initiated by %p (%d:%d<=%d)\n", q, c->target, q->requester, q->requester->fd, q->req_generation, q->requester->generation);
    if (q->requester->generation == q->req_generation) {
      // q->requester->status = conn_expect_query;   // !!NOT SURE THAT THIS CAN BE COMMENTED!!
      q->requester->queries_ok++;
      waiting_queries--;
      delete_conn_query (q);
      zfree (q, sizeof (*q));
      break;
    } else {
      waiting_queries--;
      delete_conn_query (q);
      zfree (q, sizeof (*q));
    }
  }
  return 0;
}

int query_complete (struct connection *c, int ok) {
  struct conn_query *q = c->first_query;
  struct connection *d = c->first_query->requester;
  assert (d);
  if (d->generation == c->first_query->req_generation) {
    d->queries_ok += ok;
    assert (SQLS_DATA(d)->query_state == query_running);
    SQLS_DATA(d)->query_state = query_none;
  }
  active_queries--;
  c->unreliability >>= 1;
  delete_conn_query (q);
  zfree (q, sizeof (*q));
  c->status = conn_ready;
  c->last_query_time = precise_now - c->last_query_sent_time;
  SQLC_DATA(c)->extra_flags &= -2;
  sqlp_becomes_ready (c);
  return 0;
}

int delete_query (struct conn_query *q) {
  active_queries--;
  delete_conn_query (q);
  zfree (q, sizeof (*q));
  return 0;
}

int query_timeout (struct conn_query *q) {
  q->outbound->unreliability += 1000;
  if (verbosity > 0) {
    fprintf (stderr, "query %p of connection %p (fd=%d) timed out, unreliability=%d\n", q, q->outbound, q->outbound->fd, q->outbound->unreliability);
  }
  assert (q->outbound);
  q->outbound->status = conn_error;
  q->outbound->error = -239;
  q->outbound->flags |= C_FAILED;
  if (!q->outbound->ev->in_queue) {
    put_event_into_heap (q->outbound->ev);
  }
  delete_query (q);
  expired_forwarded_queries++;
  return 0;
}


int delayed_query_timeout (struct conn_query *q) {
  if (verbosity > 0) {
    fprintf (stderr, "delayed query %p timed out\n", q);
  }
  if (q->requester && q->requester->generation == q->req_generation) {
    assert (SQLS_DATA(q->requester)->query_state == query_wait_target);
    SQLS_DATA(q->requester)->query_state = query_failed;
  }
  active_queries++;
  delete_query (q);
  waiting_queries--;
  expired_forwarded_queries++;
  expired_delayed_queries++;
  return 0;
}

int delete_watermark_query (struct conn_query *q) {
  struct connection *c = q->outbound, *d = q->requester;
  assert (c);
  if (c->basic_type == ct_outbound) {
    watermark_queries--;
  } else {
    watermark_rev_queries--;
  }
  c->write_low_watermark = 0;
  if (d && d->generation == q->req_generation) {
    /* wake up requester */
    if (verbosity > 0) {
      fprintf (stderr, "socket %d is below low_write_watermark (%d bytes in output buffer), waking requester %d to resume reading\n", c->fd, c->Out.total_bytes, d->fd);
    }
    assert (d->status == conn_wait_net);
    d->status = (c->basic_type == ct_outbound ? conn_expect_query : conn_wait_answer);
    d->flags &= ~C_STOPREAD;
    if (!d->ev->in_queue) {
      put_event_into_heap (d->ev);
    }
  }

  delete_conn_query (q);
  zfree (q, sizeof (*q));
  return 0;
}

int sqlps_ready_to_write (struct connection *c) {
  struct conn_query *q = c->first_query;

  if (verbosity > 1) {
    fprintf (stderr, "mysql server connection %d is ready to write\n", c->fd);
  }

  if (q != (struct conn_query *) c) {
    assert (q->cq_type == &proxy_watermark_query_type);
    q->cq_type->wakeup (q);
  }

  return 0;
}

int proxy_client_execute (struct connection *c, int op) {
  struct sqlc_data *D = SQLC_DATA(c);
  static char buffer[8];
  int b_len, field_cnt = -1;
  nb_iterator_t it;

  nbit_set (&it, &c->In);
  b_len = nbit_read_in (&it, buffer, 8);

  if (b_len >= 5) {
    field_cnt = buffer[4] & 0xff;
  }

  if (verbosity > 0) {
    fprintf (stderr, "proxy_db_client: op=%d, packet_len=%d, response_state=%d, field_num=%d\n", op, D->packet_len, D->response_state, field_cnt);
  }

  if (c->first_query == (struct conn_query *)c) {
    if (verbosity >= 0) {
      fprintf (stderr, "response received for empty query list? op=%d\n", op);
      if (verbosity > -2) {
        dump_connection_buffers (c);
        if (c->first_query != (struct conn_query *) c && c->first_query->req_generation == c->first_query->requester->generation) {
          dump_connection_buffers (c->first_query->requester);
        }
      }
    }
    return SKIP_ALL_BYTES;
  }

  c->last_response_time = precise_now;

  if (stop_forwarding_response (c)) {
    return 0;
  }

  switch (D->response_state) {
  case resp_first:
    forward_response (c, D->packet_len, SQLC_DATA(c)->extra_flags & 1);
    if (field_cnt == 0 && (SQLC_DATA(c)->extra_flags & 1)) {
      SQLC_DATA(c)->extra_flags |= 2;
      if (c->first_query->requester->generation != c->first_query->req_generation) {
        if (verbosity > 0) {
          fprintf (stderr, "outbound connection %d: nowhere to forward replication stream, closing\n", c->fd);
        }
        c->status = conn_error;
      }
    } else if (field_cnt == 0 || field_cnt == 0xff) {
      D->response_state = resp_done;
    } else if (field_cnt < 0 || field_cnt >= 0xfe) {
      c->status = conn_error;	// protocol error
    } else {
      D->response_state = resp_reading_fields;
    }
    break;
  case resp_reading_fields:
    forward_response (c, D->packet_len, 0);
    if (field_cnt == 0xfe) {
      D->response_state = resp_reading_rows;
    }
    break;
  case resp_reading_rows:
    forward_response (c, D->packet_len, 0);
    if (field_cnt == 0xfe) {
      D->response_state = resp_done;
    }
    break;
  case resp_done:
    fprintf (stderr, "unexpected packet from server!\n");
    assert (0);
  }

  if (D->response_state == resp_done) {
    query_complete (c, 1);
  }

  return 0;
}


int sqlp_wakeup (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "connection %d wakes up, query_state=%d, queries_ok=%d\n", c->fd, SQLS_DATA(c)->query_state, c->queries_ok);
  }

  if (!c->queries_ok) {
    if (c->status != conn_wait_net) {
      fprintf (stderr, "connection %d (IN: %d+%d, OUT: %d+%d, FLAGS: %d) is in state %d, conn_wait_net expected!\n", c->fd, c->In.total_bytes, c->In.unprocessed_bytes, c->Out.total_bytes, c->Out.unprocessed_bytes, c->flags, c->status);
    }
    assert (c->status == conn_wait_net);
    switch (SQLS_DATA(c)->query_state) {
    case query_wait_target:
      if (verbosity > 1) {
        fprintf (stderr, "connection %d: switching query_state to query_failed\n", c->fd);
      }
      SQLS_DATA(c)->query_state = query_failed;
      break;
    case query_running:
      if (verbosity > 0) {
        fprintf (stderr, "connection %d: sending Failed packet because of timeout\n", c->fd);
      }
      send_error_packet (c, 1045, 28000, "Failed", 6, ++SQLS_DATA(c)->output_packet_seq);
      SQLS_DATA(c)->query_state = query_none;
      //      c->status = conn_error;
      //      c->error = -7;
      break;
    default:
      fprintf (stderr, "connection %d awakened in impossible query_state\n", c->fd);
      assert (SQLS_DATA(c)->query_state == query_wait_target || SQLS_DATA(c)->query_state == query_running);
    }
  } else if (SQLS_DATA(c)->query_state == query_wait_target) {
    if (verbosity > 1) {
      fprintf (stderr, "connection %d: have ready target, switching query_state from query_wait_target to query_none\n", c->fd);
    }
    SQLS_DATA(c)->query_state = query_none;
  }

  assert (c->queries_ok >= 0);
  c->queries_ok = -1000;

  c->flags |= C_REPARSE;

  if (c->Out.total_bytes + c->Out.unprocessed_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  //c->target->first_query = c->target->last_query = (struct conn_query *)c->target;
  if (c->status != conn_error) {
    c->status = conn_expect_query;
  }

  clear_connection_timeout (c);
  return 0;
}

int sqlp_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "proxy_mysql_server connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  if (c->status != conn_wait_net) {
    fprintf (stderr, "connection %d (IN: %d+%d, OUT: %d+%d, FLAGS: %d) is in state %d, conn_wait_net expected!\n", c->fd, c->In.total_bytes, c->In.unprocessed_bytes, c->Out.total_bytes, c->Out.unprocessed_bytes, c->flags, c->status);
  }
  assert (c->status == conn_wait_net);
//  send_error_packet (c, 1045, 28000, "Failed", 6, ++SQLS_DATA(c)->output_packet_seq);
  return sqlp_wakeup (c);
  /*c->status = conn_error;
  if (verbosity > 0) {
    fprintf (stderr, "sql server connection timeout for connection %d --> switching to error status\n", c->fd);
  }*/
  //return /*-1*/0;
}

int sqlp_check_ready (struct connection *c) {
  if (c->status == conn_ready && c->In.total_bytes > 0) {
    fprintf (stderr, "have %d bytes in outbound sql connection %d in state ready, closing connection\n", c->In.total_bytes, c->fd);
    c->status = conn_error;
    c->error = -3;
    fail_connection (c, -3);
    return c->ready = cr_failed;
  }
  if (c->status == conn_error) {
    c->error = -4;
    fail_connection (c, -4);
    return c->ready = cr_failed;
  }
  if (c->status == conn_wait_answer || c->status == conn_reading_answer) {
    if (!(c->flags & C_FAILED) && c->last_query_sent_time < precise_now - RESPONSE_FAIL_TIMEOUT - c->last_query_time && c->last_response_time < precise_now - RESPONSE_FAIL_TIMEOUT - c->last_query_time && !(SQLC_DATA(c)->extra_flags & 1)) {
      if (verbosity > 0) {
        fprintf (stderr, "failing outbound connection %d, status=%d, response_status=%d, last_response=%.6f, last_query=%.6f, now=%.6f\n", c->fd, c->status, SQLC_DATA(c)->response_state, c->last_response_time, c->last_query_sent_time, precise_now);
      }
      c->error = -5;
      fail_connection (c, -5);
      return c->ready = cr_failed;
    }
  }
  return c->ready = (c->status == conn_ready ? cr_ok : (SQLC_DATA(c)->auth_state == sql_auth_ok ? cr_stopped : cr_busy));  /* was cr_busy instead of cr_stopped */
}


int sqlp_password (struct connection *c, const char *user, char buffer[20]) {
  memset (buffer, 0, 20);

  if (!strcmp (user, sql_username)) {
    unsigned char buffer2[20];
    SHA1 ((unsigned char *)sql_password, strlen (sql_password), buffer2);
    SHA1 (buffer2, 20, (unsigned char *)buffer);
    SQLS_DATA(c)->custom = mrand48();
    return 1;
  }
  
  return 0;
}


/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

void reopen_logs(void) {
  int fd;
  fflush(stdout);
  fflush(stderr);
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (logname && (fd = open(logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}


static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  exit (EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  exit (EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs();
  signal (SIGUSR1, sigusr1_handler);
}

static void sigusr2_handler (const int sig) {
  if (verbosity > 0) {
    fprintf (stderr, "got SIGUSR2, config reload scheduled.\n");
  }
  need_reload_config++;
  signal (SIGUSR2, sigusr2_handler);
}

void cron (void) {
  create_all_outbound_connections ();
}

int sqlp_authorized (struct connection *c) {
  nbw_iterator_t it;
  unsigned temp = 0x00000000;
  int len = 0;
  char ptype = 2;

  if (!sql_database || !*sql_database) {
    SQLC_DATA(c)->auth_state = sql_auth_ok;
    c->status = conn_ready;
    SQLC_DATA(c)->packet_state = 0;
    if (verbosity > 1) {
      fprintf (stderr, "outcoming initdb successful\n");
    }
    SQLC_FUNC(c)->sql_becomes_ready (c);
    return 0;
  }

  nbit_setw (&it, &c->Out);
  write_out (&c->Out, &temp, 4);

  len += write_out (&c->Out, &ptype, 1);
  if (sql_database && *sql_database) {
    len += write_out (&c->Out, sql_database, strlen (sql_database));
  }

  nbit_write_out (&it, &len, 3);

  SQLC_FUNC(c)->sql_flush_packet (c, len);

  SQLC_DATA(c)->auth_state = sql_auth_initdb;
  return 0;
}


int sfd, memcache_sfd;

void start_server (void) { 
  //  struct sigaction sa;
  int i;
  int prev_time;
  double next_create_outbound = 0;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  init_listening_connection (sfd, &ct_mysql_server, &db_proxy_inbound);

  if (memcache_port) {
    init_listening_connection (memcache_sfd, &ct_memcache_server, &db_proxy_memcache_inbound);
  }

  create_all_outbound_connections ();

  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigterm_handler);
  signal(SIGUSR1, sigusr1_handler);
  signal(SIGUSR2, sigusr2_handler);
  signal(SIGPIPE, SIG_IGN);
  if (daemonize) {
    signal(SIGHUP, sighup_handler);
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (50);
    if (precise_now > next_create_outbound) {
      create_all_outbound_connections ();
      next_create_outbound = precise_now + 0.03 + 0.02 * drand48();
    }
    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    if (need_reload_config) {
      do_reload_config (1);
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close(sfd);
  
  //  flush_binlog_ts();
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-p<port>] [-s<memcache port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-l<log-name>] [-C] <cluster-descr-file>\n"
	  "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " after commit " COMMIT "\n"
	  "\tRedistributes MySQL queries to servers listed in <cluster-descr-file>\n"
	  "\t-C\tCheck cluster file syntax only, then exit\n"
	  "\t-r\tUse round-robin to select outbound connections\n"
	  "\t-v\toutput statistical and debug information into stderr\n",
	  progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i, syntax_check = 0;

  set_debug_handlers ();

  progname = argv[0];
  while ((i = getopt (argc, argv, "b:c:l:p:rs:n:dCRThu:v")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'b':
      backlog = atoi(optarg);
      if (backlog <= 0) {
        backlog = BACKLOG;
      }
      break;
    case 'c':
      maxconn = atoi(optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
	maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'p':
      port = atoi(optarg);
      break;
    case 's':
      memcache_port = atoi(optarg);
      break;
    case 'n':
      errno = 0;
      nice (atoi (optarg));
      if (errno) {
        perror ("nice");
      }
      break;
    case 'R':
      sql_username = sql_username_r;
      sql_password = sql_password_r;
      sql_database = sql_database_r;
      replicator_mode++;
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'T':
      test_mode++;
      break;
    case 'C':
      syntax_check = 1;
      break;
    case 'r':
      round_robin++;
      break;
    case 'd':
      daemonize ^= 1;
    }
  }
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  init_dyn_data ();

  config_filename = argv[optind];

  i = do_reload_config (0);

  if (i < 0) {
    fprintf (stderr, "config check failed!\n");
    return -i;
  }

  if (verbosity > 0) {
    fprintf (stderr, "config loaded!\n");
  }

  if (syntax_check) {
    return 0;
  }

  aes_load_pwd_file (0);

  if (raise_file_rlimit(maxconn + 16) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit(1);
  }

  sfd = server_socket (port, settings_addr, backlog, 0);
  if (sfd < 0) {
    fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
    exit(1);
  }
  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr(settings_addr.s_addr, 0), port, sfd);
  }
  
  if (memcache_port) {
    memcache_sfd = server_socket (memcache_port, settings_addr, backlog, 0);
    if (memcache_sfd < 0) {
      fprintf(stderr, "cannot open memcache server socket at port %d: %m\n", memcache_port);
      exit(1);
    }
    if (verbosity) {
      fprintf (stderr, "created memcache listening socket at %s:%d, fd=%d\n", conv_addr(settings_addr.s_addr, 0), memcache_port, memcache_sfd);
    }
  }

  start_time = time(0);

  start_server();

  return 0;
}

