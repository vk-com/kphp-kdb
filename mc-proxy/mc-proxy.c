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

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>
#include <stdarg.h>
#include <math.h>
#include <signal.h>

#include "crc32.h"
#include "md5.h"
#include "kdb-data-common.h"
#include "resolver.h"
#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "server-functions.h"
#include "net-memcache-server.h"
#include "net-memcache-client.h"
#include "net-crypto-aes.h"
#include "mc-proxy.h"
#ifdef SEARCH_MODE_ENABLED
  #include "mc-proxy-merge-extension.h"
#endif

#define MAX_KEY_LEN 1000
#define MAX_KEY_RETRIES 3
#define MAX_VALUE_LEN (1 << 24)

#define VERSION_STR "mc-proxy-0.21"

#define TCP_PORT 11213

#define MAX_NET_RES (1L << 16)

#define PING_INTERVAL 120.0
#define	MIN_PING_INTERVAL	1.0
#define	MAX_PING_INTERVAL	300.0
#define RESPONSE_FAIL_TIMEOUT 5

#define CONNECT_TIMEOUT 3

#define MAX_USER_FRIENDS  65536

#define DEFAULT_MIN_CONNECTIONS 2
#define DEFAULT_MAX_CONNECTIONS 3

#define	SMALL_RESPONSE_THRESHOLD	256
#define	BIG_RESPONSE_THRESHOLD	14000

#define	AMORT_FACTOR	0.99

#ifndef COMMIT
#define COMMIT "unknown"
#endif

/*
 *
 *    MEMCACHED PORT
 *
 */

int proxy_server_execute (struct connection *c, int op);
int proxy_client_execute (struct connection *c, int op);

int mcp_get_start (struct connection *c);
int mcp_get (struct connection *c, const char *key, int key_len);
int mcp_get_end (struct connection *c, int key_count);
int mcp_wakeup (struct connection *c);
int mcp_alarm (struct connection *c);
int mcp_stats (struct connection *c);
int mcp_check_ready (struct connection *c);
int mcp_connected (struct connection *c);

int mcc_crypto_check_perm (struct connection *c);

struct memcache_server_functions mc_proxy_inbound = {
  .execute = proxy_server_execute,
  .mc_get_start = mcp_get_start,
  .mc_get = mcp_get,
  .mc_get_end = mcp_get_end,
  .mc_version = mcs_version,
  .mc_stats = mcp_stats,
  .mc_wakeup = mcp_wakeup,
  .mc_alarm = mcp_alarm,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct memcache_client_functions mc_proxy_outbound = {
  .execute = proxy_client_execute,
  .check_ready = mcp_check_ready,
  .flush_query = mcc_flush_query,
  .connected = mcp_connected,
  .mc_check_perm = mcc_default_check_perm,
  .mc_init_crypto = mcc_init_crypto,
  .mc_start_crypto = mcc_start_crypto
};

struct memcache_client_functions mc_proxy_outbound_crypto = {
  .execute = proxy_client_execute,
  .check_ready = mcp_check_ready,
  .flush_query = mcc_flush_query,
  .connected = mcp_connected,
  .mc_check_perm = mcc_crypto_check_perm,
  .mc_init_crypto = mcc_init_crypto,
  .mc_start_crypto = mcc_start_crypto
};


int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;
int verbosity = 0;
int quit_steps;

char *fnames[3];
int fd[3];
long long fsize[3];

// unsigned char is_letter[256];
char *progname = "mc-proxy", *username, *logname;

char extension_name[16];

int start_time;
long long netw_queries, minor_update_queries, search_queries;
long long tot_response_words, tot_response_bytes, tot_ok_gathers, tot_bad_gathers;
double tot_ok_gathers_time, tot_bad_gathers_time;

int active_queries;

int only_first_cluster;

long long tot_forwarded_queries, expired_forwarded_queries;
long long diagonal_received_queries, diagonal_forwarded_total, tot_skipped_answers;
long long immediate_forwarded_queries;
long long dropped_overflow_responses;

long long errors_received, client_errors_received;

char *aes_pwd_file;

double ping_interval = PING_INTERVAL;

#define STATS_BUFF_SIZE (1 << 20)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];

struct conn_query *create_query (struct connection *d, struct connection *c, double timeout);

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

/*
 *
 *  CONFIGURATION PARSER
 *
 */

#define IN_CLUSTER_CONFIG 1
#define IN_CLUSTER_CONNECTED  2

/*struct cluster_server {
  int id;
  char *hostname;
  int port;
  struct in_addr addr;
  struct connection *c;
  int conn, rconn;
  int in_cluster;
  int conn_retries;
  int reconnect_time;
  int list_len;
  int list_first;
};*/


int default_min_connections = DEFAULT_MIN_CONNECTIONS;
int default_max_connections = DEFAULT_MAX_CONNECTIONS;

struct conn_target default_ct = {
.min_connections = DEFAULT_MIN_CONNECTIONS,
.max_connections = DEFAULT_MAX_CONNECTIONS,
.type = &ct_memcache_client,
.extra = &mc_proxy_outbound,
.reconnect_timeout = 1
};

struct mc_config {
  int ConfigServersCount, clusters_num;
  int config_bytes, config_loaded_at;
  char *config_md5_hex;
  struct conn_target *ConfigServers[MAX_CLUSTER_SERVERS];
  struct mc_cluster Clusters[MAX_CLUSTERS];
} Config[2], *CurConf = Config, *NextConf = Config + 1;

struct mc_cluster *CC;

#define MAX_CONFIG_SIZE (1 << 20)

char config_buff[MAX_CONFIG_SIZE+4], *config_filename, *cfg_start, *cfg_end, *cfg_cur;
int config_bytes, cfg_lno, cfg_lex = -1;

int default_cluster_mode;  

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
  while (*cfg_cur == ' ' || *cfg_cur == 9) {
    cfg_cur++;
  }
  return (unsigned char) *cfg_cur;
}

static int cfg_getlex (void) {
  switch (cfg_skipspc()) {
  case ';':
  case ':':
  case '{':
  case '}':
    return cfg_lex = *cfg_cur++;
  case 'c':
    if (!memcmp (cfg_cur, "cluster", 7)) {
      cfg_cur += 7;
      return cfg_lex = 'c';
    }
    if (!memcmp (cfg_cur, "crypto", 6)) {
      cfg_cur += 6;
      return cfg_lex = 'C';
    }
    break;
  case 'd':
    if (!memcmp (cfg_cur, "disable", 7)) {
      cfg_cur += 7;
      return cfg_lex = 'd';
    } 
    break;
  case 'e':
    if (!memcmp (cfg_cur, "extension", 9)) {
      cfg_cur += 9;
      return cfg_lex = 'e';
    }
    break;
  case 'g':
    if (!memcmp (cfg_cur, "get_timeout", 11)) {
      cfg_cur += 11;
      return cfg_lex = 'G';
    }
    break;
  case 'l':
    if (!memcmp (cfg_cur, "listen", 6)) {
      cfg_cur += 6;
      return cfg_lex = 'l';
    }
    break;
  case 'm':
    if (!memcmp (cfg_cur, "mode", 4)) {
      cfg_cur += 4;
      return cfg_lex = 'm';
    }
    if (!memcmp (cfg_cur, "min_connections", 15)) {
      cfg_cur += 15;
      return cfg_lex = 'x';
    }
    if (!memcmp (cfg_cur, "max_connections", 15)) {
      cfg_cur += 15;
      return cfg_lex = 'X';
    }
    break;
  case 'p':
    if (!memcmp (cfg_cur, "points", 6)) {
      cfg_cur += 6;
      return cfg_lex = 'p';
    }
    break;
  case 's':
    if (!memcmp (cfg_cur, "server", 6)) {
      cfg_cur += 6;
      return cfg_lex = 's';
    }
    if (!memcmp (cfg_cur, "set_timeout", 11)) {
      cfg_cur += 11;
      return cfg_lex = 'S';
    }
    if (!memcmp (cfg_cur, "step", 4)) {
      cfg_cur += 4;
      return cfg_lex = 'T';
    }
    break;
  case 't':
    if (!memcmp (cfg_cur, "timeout", 7)) {
      cfg_cur += 7;
      return cfg_lex = 't';
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

static int cfg_getint (void) {
  cfg_skspc();
  char *s = cfg_cur;
  int x = 0;
  while (*s >= '0' && *s <= '9') {
    x = x*10 + (*s++ - '0');
  }
  cfg_cur = s;
  return x;
}  

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

/*
static void syntax_warning (const char *msg, ...) {
  char *ptr = cfg_cur, *end = ptr + 20;
  va_list args;
  if (cfg_lno) {
    fprintf (stderr, "%s:%d: ", config_filename, cfg_lno);
  }
  fputs ("warning: ", stderr);
  va_start (args, msg);
  vfprintf (stderr, msg, args);
  va_end (args);
  fputs (" near ", stderr);
  while (*ptr && *ptr != 13 && *ptr != 10) {
    putc (*ptr++, stderr);
    if (ptr > end) {
      fputs (" ...", stderr);
      break;
    }
  }
  putc ('\n', stderr);
}
*/

static int expect_lexem (int lexem) {
  if (cfg_lex != lexem) {
    static char buff[32];
    sprintf (buff, "%c expected", lexem);
    return syntax (buff);
  } else {
    return 0;
  }
}

#define Expect(l) { int t = expect_lexem (l); if (t < 0) { return t; } }

static void build_mc_points (struct mc_cluster *C);

void clear_config (struct mc_config *MC, int do_destroy_targets) {
  int i, j;
  for (i = 0; i < MC->clusters_num; i++) {
    struct mc_cluster *C = &MC->Clusters[i];
    if (do_destroy_targets) {
      for (j = 0; j < C->tot_buckets; j++) {
        vkprintf (1, "destroying target %s:%d\n", inet_ntoa (C->buckets[j]->target), C->buckets[j]->port);
	destroy_target (C->buckets[j]);
      }
      memset (C->buckets, 0, C->tot_buckets * sizeof (struct conn_target *));
    }
    if (C->points) {
      free (C->points);
      C->points = 0;
    }
    if (C->cluster_name) {
      zfree (C->cluster_name, strlen (C->cluster_name) + 1);
      C->cluster_name = 0;
    }
  }

  MC->ConfigServersCount = 0;
  MC->clusters_num = 0;
}

// flags = 0 -- syntax check only (first pass), flags = 1 -- create targets and points as well (second pass)
int parse_config (struct mc_config *MC, struct mc_config *MC_Old, int flags) {
  int r, c, l, i;
  struct hostent *h;
  char *hostname;
  int dup_port_checked, cluster_disabled;

  if (!(flags & 1)) {
    config_bytes = r = read (fd[0], config_buff, MAX_CONFIG_SIZE+1);
    if (r < 0) {
      fprintf (stderr, "error reading configuration file %s: %m\n", config_filename);
      return -2;
    }
    if (r > MAX_CONFIG_SIZE) {
      fprintf (stderr, "configuration file %s too long (max %d bytes)\n", config_filename, MAX_CONFIG_SIZE);
      return -2;
    }
  }

  cfg_cur = cfg_start = config_buff;
  cfg_end = cfg_start + config_bytes;
  *cfg_end = 0;
  cfg_lno = 0;
  
  if (!(flags & 1) && MC_Old) {
    for (i = 0; i < MC_Old->clusters_num; i++) {
      MC_Old->Clusters[i].other_cluster_no = -1;
    }
  }

  MC->ConfigServersCount = 0;
  MC->clusters_num = 0;
  while (cfg_skipspc ()) {
    struct mc_cluster *C = &MC->Clusters[MC->clusters_num];
    if (cfg_getlex () != 'c') {
      return syntax ("'cluster' expected");
    }
    assert (MC->clusters_num < MAX_CLUSTERS);
    l = cfg_getword ();
    if (!l) {
      return syntax ("cluster name expected");
    }
    C->cluster_mode = default_cluster_mode;
    memcpy (&C->mc_proxy_inbound, &mc_proxy_inbound, sizeof (struct memcache_server_functions));
    C->mc_proxy_inbound.info = C;
    C->cluster_no = MC->clusters_num++;

    if (C->cluster_name) {
      zfree (C->cluster_name, strlen (C->cluster_name) + 1);
    }
    C->cluster_name = zmalloc (l+1);
    memcpy (C->cluster_name, cfg_cur, l);
    C->cluster_name[l] = 0;
    cfg_cur += l;

    C->get_timeout = 0.3;
    C->set_timeout = 0.5;
    C->min_connections = default_min_connections;
    C->max_connections = default_max_connections;
    C->step = 0;
    C->tot_buckets = 0;
    C->port = 0;
    C->points_num = 0;
    C->crypto = 0;
    dup_port_checked = 0;
    cluster_disabled = 0;
    C->a_req = C->a_rbytes = C->a_sbytes = C->a_timeouts = 0;
    C->t_req = C->t_rbytes = C->t_sbytes = C->t_timeouts = 0;

    if (!(flags & 1)) {
      C->other_cluster_no = -1;
    }

    cfg_getlex ();
    Expect ('{');
    cfg_getlex ();
    while (cfg_lex != '}') {
      switch (cfg_lex) {
      case 'l':
        if (C->port) {
          return syntax ("second listen in same cluster");
        }
        C->port = cfg_getint();
        if (!C->port) {
          return syntax ("port number expected");
        }
        if (C->port <= 0 || C->port >= 0x10000) {
          return syntax("port number out of range");
        }
	if (!(flags & 1)) {
	  for (i = 0; i < MC->clusters_num-1; i++) {
	    if (MC->Clusters[i].port == C->port) {
	      return syntax ("duplicate listen port");
	    }
	  }
	  dup_port_checked = 1;
	}
        break;
      case 'p':
        if (C->points_num) {
          return syntax ("redefined points number for same cluster");
        }
        C->points_num = cfg_getint ();
        if (C->points_num < 0 || C->points_num > 1000) {
          return syntax ("points number should be between 0 and 1000");
        }
        break;
      case 'm':
        l = cfg_getword ();
        if (l == 9 && !memcmp (cfg_cur, "memcached", 9)) {
          C->cluster_mode = (C->cluster_mode & -256) | CLUSTER_MODE_MEMCACHED;
        } else if (l == 8 && !memcmp (cfg_cur, "firstint", 8)) {
          C->cluster_mode = (C->cluster_mode & -256) | CLUSTER_MODE_FIRSTINT;
        } else if (l == 6 && !memcmp (cfg_cur, "random", 6)) {
          C->cluster_mode = (C->cluster_mode & -256) | CLUSTER_MODE_RANDOM;
        } else if (l == 9 && !memcmp (cfg_cur, "secondint", 9)) {
          C->cluster_mode = (C->cluster_mode & -256) | CLUSTER_MODE_SECONDINT;
        } else if (l == 8 && !memcmp (cfg_cur, "thirdint", 8)) {
          C->cluster_mode = (C->cluster_mode & -256) | CLUSTER_MODE_THIRDINT;
        } else if (l == 9 && !memcmp (cfg_cur, "fourthint", 9)) {
          C->cluster_mode = (C->cluster_mode & -256) | CLUSTER_MODE_FOURTHINT;
        } else if (l == 8 && !memcmp (cfg_cur, "fifthint", 8)) {
          C->cluster_mode = (C->cluster_mode & -256) | CLUSTER_MODE_FIFTHINT;
        } else if (l == 10 && !memcmp (cfg_cur, "persistent", 10)) {
          C->cluster_mode = (C->cluster_mode & -256) | CLUSTER_MODE_PERSISTENT;
        } else {
          return syntax ("unknown cluster mode");
        }
        cfg_cur += l;
        break;
      case 'e':
        l = cfg_getword ();
        if (l == 4 && !memcmp (cfg_cur, "text", 4)) {
          C->cluster_mode |= CLUSTER_MODE_TEXT;
        } else if (l == 5 && !memcmp (cfg_cur, "lists", 5)) {
          C->cluster_mode |= CLUSTER_MODE_LISTS;
        } else if (l == 5 && !memcmp (cfg_cur, "hints", 5)) {
          C->cluster_mode |= CLUSTER_MODE_HINTS;
        } else if (l == 4 && !memcmp (cfg_cur, "logs", 4)) {
          C->cluster_mode |= CLUSTER_MODE_LOGS;
        } else if (l == 5 && !memcmp (cfg_cur, "magus", 5)) {
          C->cluster_mode |= CLUSTER_MODE_MAGUS;
        } else if (l == 4 && !memcmp (cfg_cur, "news", 4)) {
          C->cluster_mode |= CLUSTER_MODE_NEWS;
        } else if (l == 10 && !memcmp (cfg_cur, "roundrobin", 10)) {
          C->cluster_mode |= CLUSTER_MODE_ROUNDROBIN;
        } else if (l == 5 && !memcmp (cfg_cur, "bayes", 5)) {
          C->cluster_mode |= CLUSTER_MODE_BAYES;
        } else if (l == 7 && !memcmp (cfg_cur, "support", 7)) {
          C->cluster_mode |= CLUSTER_MODE_SUPPORT;
        } else if (l == 5 && !memcmp (cfg_cur, "photo", 5)) {
          C->cluster_mode |= CLUSTER_MODE_PHOTO;
        } else if (l == 3 && !memcmp (cfg_cur, "dot", 3)) {
          C->cluster_mode |= CLUSTER_MODE_DOT;
        } else if (l == 4 && !memcmp (cfg_cur, "temp", 4)) {
          C->cluster_mode |= CLUSTER_MODE_TEMP;
        } else if (l == 8 && !memcmp (cfg_cur, "antispam", 8)) {
          C->cluster_mode |= CLUSTER_MODE_ANTISPAM;
        } else if (l == 6 && !memcmp (cfg_cur, "search", 6)){
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_SEARCH;
        } else if (l == 4 && !memcmp (cfg_cur, "targ", 4)){
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_TARG;
        } else if (l == 7 && !memcmp (cfg_cur, "news_ug", 7)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_NEWS_UG;
        } else if (l == 6 && !memcmp (cfg_cur, "news_g", 6)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_NEWS_G;
        } else if (l == 5 && !memcmp (cfg_cur, "newsr", 5)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_NEWSR;
        } else if (l == 9 && !memcmp (cfg_cur, "news_comm", 9)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_NEWS_COMM;
        } else if (l == 6 && !memcmp (cfg_cur, "statsx", 6)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_STATSX;
        } else if (l == 7 && !memcmp (cfg_cur, "friends", 7)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_FRIENDS;
        } else if (l == 7 && !memcmp (cfg_cur, "searchx", 7)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_SEARCHX;
        } else if (l == 8 && !memcmp (cfg_cur, "watchcat", 8)) {
          C->cluster_mode |= CLUSTER_MODE_WATCHCAT;
        } else if (l == 11 && !memcmp (cfg_cur, "hints_merge", 11)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_HINTS_MERGE;
        } else if (l == 12 && !memcmp (cfg_cur, "random_merge", 12)) {
          if (C->cluster_mode & CLUSTER_MODE_MERGE) {
            return syntax ("Can not enable two merge extensions");
          }
          C->cluster_mode |= CLUSTER_MODE_RANDOM_MERGE;
        } else {
          return syntax ("unknown extension");
        }
        cfg_cur += l;
        break;
      case 't':
        C->get_timeout = cfg_getint ();
        if (C->get_timeout < 10 || C->get_timeout > 30000) {
          return syntax ("invalid timeout");
        }
        C->get_timeout /= 1000;
        C->set_timeout = C->get_timeout + 0.2;
        break;
      case 'C':
	C->crypto = 1;
	break;
      case 'T':
        C->step = cfg_getint ();
        if (C->step < 0) {
          return syntax ("invalid step value");
        }
        break;
      case 'G':
        C->get_timeout = cfg_getint ();
        if (C->get_timeout < 10 || C->get_timeout > 30000) {
          return syntax ("invalid timeout");
        }
        C->get_timeout /= 1000;
        break;
      case 'S':
        C->set_timeout = cfg_getint ();
        if (C->set_timeout < 10 || C->set_timeout > 30000) {
          return syntax ("invalid timeout");
        }
        C->set_timeout /= 1000;
        break;
      case 's':
        assert (C->tot_buckets < MAX_CLUSTER_SERVERS);
        l = cfg_getword ();
        if (!l || l > 63) {
          return syntax ("hostname expected");
        }
        c = cfg_cur[l];
        hostname = cfg_cur;
        cfg_cur[l] = 0;

        if (!(h = kdb_gethostbyname (cfg_cur)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
          return syntax ("cannot resolve hostname");
        }
        default_ct.target = *((struct in_addr *) h->h_addr);

        *(cfg_cur += l) = c;
        cfg_getlex ();
        Expect (':');
        default_ct.port = cfg_getint();
        if (!default_ct.port) {
          return syntax ("port number expected");
        }
        
        if (default_ct.port <= 0 || default_ct.port >= 0x10000) {
          return syntax ("port number out of range");
        }

        default_ct.min_connections = C->min_connections;
        default_ct.max_connections = C->max_connections;
        default_ct.reconnect_timeout = 1.0 + 0.1 * drand48 ();

	default_ct.extra = (C->crypto ? &mc_proxy_outbound_crypto : &mc_proxy_outbound);

	if ((flags & 1) && !cluster_disabled) {
	  int was_created = -1;
	  struct conn_target *D = create_target (&default_ct, &was_created);
	  if (was_created <= 0) {
	    //  syntax_warning ("duplicate hostname:port %.*s:%d in cluster", l, hostname, default_ct.port);
	  }

	  MC->ConfigServers[MC->ConfigServersCount] = D;
	  C->buckets[C->tot_buckets] = D;
	}

	if (!cluster_disabled) {
	  C->tot_buckets++;
	  assert (MC->ConfigServersCount++ < MAX_CLUSTER_SERVERS);
	}

        vkprintf (1, "server #%d: ip %s, port %d\n", MC->ConfigServersCount, inet_ntoa (default_ct.target), default_ct.port);
        break;
      case 'X':
        C->max_connections = cfg_getint ();
        if (C->max_connections < C->min_connections || C->max_connections > 1000) {
          return syntax ("invalid max connections");
        }
        break;
      case 'x':
        C->min_connections = cfg_getint ();
        if (C->min_connections < 1 || C->min_connections > C->max_connections) {
          return syntax ("invalid min connections");
        }
        break;
      case 'd':
	if (C->tot_buckets) {
	  return syntax ("'disabled' must appear before any server declaration in cluster");
	}
	cluster_disabled = 1;
	//	C->cluster_mode |= CLUSTER_MODE_DISABLED;
	break;
      case 0:
        return syntax ("unexpected end of file");
      default:
        return syntax ("'server' expected");
      }
      cfg_getlex ();
      Expect (';');
      cfg_getlex ();
    }
    if (cluster_disabled) {
      if (verbosity > 1) {
	fprintf (stderr, "Cluster #%d (%s) on port %d, mode %04x : DISABLED\n", C->cluster_no, C->cluster_name, C->port, C->cluster_mode);
      }
      MC->clusters_num--;
      continue;
    }
    if (!C->tot_buckets) {
      return syntax ("no servers in clusters");
    }
    if (!C->port) {
      C->port = port;
    }
    if (!(flags & 1) && !dup_port_checked) {
      for (i = 0; i < MC->clusters_num - 1; i++) {
	if (MC->Clusters[i].port == C->port) {
	  return syntax ("duplicate listen port");
	}
      }
    }
    if (!(flags & 1) && MC_Old) {
      for (i = 0; i < MC_Old->clusters_num; i++) {
	if (MC_Old->Clusters[i].port == C->port) {
	  MC_Old->Clusters[i].other_cluster_no = C->cluster_no;
	  C->other_cluster_no = i;
	  break;
	}
      }
    }
    if (C->points_num) {
      if ((C->cluster_mode & 255) != CLUSTER_MODE_MEMCACHED) {
        return syntax ("points can be defined for memcached mode clusters only");
      }
      if (flags & 1) {
	build_mc_points (C);
      }
    }
    if (verbosity > 1) {
      fprintf (stderr, "Cluster #%d (%s) on port %d, mode %04x, %d servers\n", C->cluster_no, C->cluster_name, C->port, C->cluster_mode, C->tot_buckets);
    }
    if (only_first_cluster && MC->clusters_num >= only_first_cluster) {
      break;
    }
  }
  if (!MC->ConfigServersCount || !MC->clusters_num) {
    return syntax ("no cluster defined");
  }

  return 0;
}

int try_open_new_listening_sockets (struct mc_config *MC) {
  int i, j, sfd;
  for (i = 0; i < MC->clusters_num; i++) {
    if (MC->Clusters[i].other_cluster_no >= 0) {
      continue;
    }
    sfd = server_socket (MC->Clusters[i].port, settings_addr, backlog, 0);
    if (sfd >= MAX_CONNECTIONS) {
      vkprintf (0, "cannot open server socket at port %d: too many open connections (fd=%d)\n", MC->Clusters[i].port, sfd);
      close (sfd);
      sfd = -1;
    }
    if (sfd < 0) {
      vkprintf (0, "cannot open server socket at port %d: %m\n", MC->Clusters[i].port);
      for (j = 0; j < i; j++) {
	if (MC->Clusters[j].other_cluster_no < 0) {
	  sfd = MC->Clusters[j].server_socket;
	  close (sfd);
	  MC->Clusters[j].server_socket = -1;
	  vkprintf (1, "closed newly-opened listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, 0), MC->Clusters[j].port, sfd);
	}
      }
      return -2;
    }
    vkprintf (1, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, 0), MC->Clusters[i].port, sfd);
    MC->Clusters[i].server_socket = sfd;
  }
  return 0;
}

int transfer_listening_sockets (struct mc_config *MC, struct mc_config *MC_Old) {
  int i, j, k;
  for (i = 0; i < MC->clusters_num; i++) {
    struct mc_cluster *C = &MC->Clusters[i];
    j = C->other_cluster_no;
    if (j >= 0) {
      assert (j < MC_Old->clusters_num);
      struct mc_cluster *OC = &MC_Old->Clusters[j];
      assert (OC->port == C->port && OC->other_cluster_no == i);
      C->server_socket = OC->server_socket;
      C->listening_connection = OC->listening_connection;
      OC->server_socket = -1;
      OC->listening_connection = 0;
      C->listening_connection->extra = &C->mc_proxy_inbound;
    } else {
      assert (init_listening_connection (C->server_socket, &ct_memcache_server, &C->mc_proxy_inbound) >= 0);
      C->listening_connection = Connections + C->server_socket;
    }
  }
  for (k = 0; k <= max_connection; k++) { 
    struct connection *c = Connections + k;
    if (c->basic_type != ct_inbound || c->fd != k) {
      continue;
    }
    struct mc_cluster *OC = ((struct memcache_server_functions *) c->extra)->info;
    assert (OC && &OC->mc_proxy_inbound == c->extra);
    j = OC->cluster_no;
    i = OC->other_cluster_no;
    assert (OC == &MC_Old->Clusters[j]);
    if (i >= 0) {
      struct mc_cluster *C = &MC->Clusters[i];
      assert (C->cluster_no == i && C->other_cluster_no == j);
      vkprintf (2, "transferring inbound connection #%d (port %d) from old cluster %d to new cluster %d\n", k, OC->port, j, i);
      c->extra = &C->mc_proxy_inbound;
    } else {
      vkprintf (2, "closing inbound connection #%d (port %d) belonging to old cluster %d, no new cluster\n", k, OC->port, j);
      force_clear_connection (c);
      epoll_close (k);
      close (k);
    }
  }
  for (i = 0; i < MC_Old->clusters_num; i++) {
    struct mc_cluster *OC = &MC_Old->Clusters[i];
    if (OC->other_cluster_no == -1) {
      assert (OC->server_socket >= 0);
      k = OC->server_socket;
      vkprintf (1, "closing unneeded listening connection #%d for port %d belonging to old cluster %d (%s)\n", k, OC->port, i, OC->cluster_name);
      force_clear_connection (&Connections[k]);
      epoll_close (k);
      close (k);
      OC->server_socket = -1;
      OC->listening_connection = 0;
    } else {
      assert (OC->server_socket == -1 && !OC->listening_connection);
    }
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
    return -1;
  }

  res = kdb_load_hosts ();
  if (res > 0 && verbosity > 0) {
    fprintf (stderr, "/etc/hosts changed, reloaded\n");
  }

  res = parse_config (NextConf, CurConf, 0);

  close (fd[0]);

  //  clear_config (NextConf);
  
  if (res < 0) {
    vkprintf (0, "error while re-reading config file %s, new configuration NOT applied\n", config_filename);
    return res;
  }

  res = try_open_new_listening_sockets (NextConf);
  if (res < 0) {
    vkprintf (0, "error while re-reading config file %s, new configuration NOT applied: cannot open listening ports\n", config_filename);
    return res;
  }

  res = parse_config (NextConf, CurConf, 1);

  if (res < 0) {
    clear_config (NextConf, 0);
    vkprintf (0, "fatal error while re-reading config file %s\n", config_filename);
    exit (-res);
  }

  if (create_conn) {
    transfer_listening_sockets (NextConf, CurConf);
  }

  struct mc_config *tmp = CurConf;
  CurConf = NextConf;
  NextConf = tmp;

  clear_config (NextConf, 1);

  if (create_conn) {
    create_all_outbound_connections ();
  }

  CurConf->config_loaded_at = now ? now : time (0);
  CurConf->config_bytes = config_bytes;
  CurConf->config_md5_hex = zmalloc (33);
  md5_hex (config_buff, config_bytes, CurConf->config_md5_hex);
  CurConf->config_md5_hex[32] = 0;

  vkprintf (0, "configuration file %s re-read successfully, new configuration active\n", config_filename);

  return 0;
}

void adjust_cluster_stats (void) {
  int i;
  struct mc_cluster *C = CurConf->Clusters;
  for (i = 0; i < CurConf->clusters_num; i++, C++) {
    C->a_req *= AMORT_FACTOR;
    C->a_rbytes *= AMORT_FACTOR;
    C->a_sbytes *= AMORT_FACTOR;
    C->a_timeouts *= AMORT_FACTOR;
  }
}

/*
 *
 *  PROXY MEMCACHE SERVER FUNCTIONS
 *
 */

int forward_query (struct connection *c, int query_len, const char *key, int key_len);

static inline int flush_output (struct connection *c) {
//  fprintf (stderr, "flush_output (%d)\n", c->fd);
  return MCC_FUNC (c)->flush_query (c);
}

static inline void accumulate_query_timeout (struct connection *c, double query_timeout) {
  if (c->last_query_timeout < query_timeout) {
    c->last_query_timeout = query_timeout;
    assert (query_timeout < 32);
  }
}

struct mc_point_descr {
  unsigned ip;
  short sugar;
  short port;
};

static int cmp_points (mc_point_t *A, mc_point_t *B) {
  if (A->x < B->x) {
    return -1;
  } else if (A->x > B->x) {
    return 1;
  } else if (A->target->target.s_addr < B->target->target.s_addr) {
    return -1;
  } else if (A->target->target.s_addr > B->target->target.s_addr) {
    return 1;
  } else {
    return A->target->port - B->target->port;
  }
}

static void sort_points (mc_point_t *A, int N) {
  int i, j;
  mc_point_t h, t;
  if (N <= 0) {
    return;
  }
  if (N == 1) {
    if (cmp_points (&A[0], &A[1]) > 0) {
      t = A[0];
      A[0] = A[1];
      A[1] = t;
    }
    return;
  }
  i = 0;
  j = N;
  h = A[j >> 1];
  do {
    while (cmp_points (&A[i], &h) < 0) { i++; }
    while (cmp_points (&A[j], &h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  sort_points (A+i, N-i);
  sort_points (A, j);
}

static void build_mc_points (struct mc_cluster *C) {
  if (verbosity >= 3) {
    fprintf (stderr, "--> started\n");
  }

  int N = C->tot_buckets, K = C->points_num, NK = N*K, i, j;

  double t1 = get_utime (CLOCK_MONOTONIC);

  static struct mc_point_descr Point;

  assert (N > 0 && K > 0 && N <= 100000 && K <= 1000);

  C->points = malloc (sizeof (mc_point_t) * C->points_num * C->tot_buckets);
  assert (C->points);

  mc_point_t *ptr = C->points;
 
  for (i = 0; i < N; i++) {
    Point.ip = C->buckets[i]->target.s_addr;
    Point.port = C->buckets[i]->port;
    C->buckets[i]->custom_field = i;
    for (j = 0; j < K; j++) {
      ptr->target = C->buckets[i];
      Point.sugar = j;
      ptr->x = crc64 (&Point, sizeof (Point)) * 7047438495421301423LL;
      ptr++;
    }
  }

  double t2 = get_utime (CLOCK_MONOTONIC);
  sort_points (C->points, NK - 1);
  double t3 = get_utime (CLOCK_MONOTONIC);

  if (verbosity >= 3) {
    long long CC[N];

    for (i = 0; i < N; i++) {
      CC[i] = 0;
    }

    for (i = 0; i < NK - 1; i++) {
      if (i > NK - 100) {
        fprintf (stderr, "%llu %d\n", C->points[i].x, C->points[i].target->custom_field);
      }
    }
  
    double D = 0, S = ((double)(1LL << 32)) * (double)(1LL << 32), Z = S / N;

    for (i = 0; i < NK - 1; i++) {
      CC[C->points[i].target->custom_field] += C->points[i+1].x - C->points[i].x;
      if ((C->points[i+1].x - C->points[i].x) / Z > 100 || (C->points[i+1].x - C->points[i].x) / Z < 0) {
        fprintf (stderr, "%d: %llu %d\n", i, C->points[i].x, C->points[i].target->custom_field);
        fprintf (stderr, "%d: %llu %d\n", i + 1, C->points[i + 1].x, C->points[i + 1].target->custom_field);
      }
    }
    CC[C->points[NK - 1].target->custom_field] += C->points[0].x - C->points[NK - 1].x;

    long long min = 0x7fffffffffffffffLL, max = 0;

    for (i = 0; i < N; i++) {
      if (i > N - 100) {
        fprintf (stderr, "%.6f\n", CC[i] / Z);
      }
      if (CC[i] > max) { 
        max = CC[i];
      }
      if (CC[i] < min) { 
        min = CC[i];
      }
      D += (double) CC[i] * CC[i];
    }
    double t4 = get_utime (CLOCK_MONOTONIC);

    fprintf (stderr, "\nN=%d K=%d avg=%.3f dev=%.3f min=%.3f max=%.3f\n", N, K, 1.0, sqrt (D/(N*Z*Z) - 1.0), min / Z, max / Z);
    fprintf (stderr, "Eval time %.3f, sort time %.3f, stat time %.3f\n", t2 - t1, t3 - t2, t4 - t3);
  }

  for (i = 0; i < N; i++) {
    C->buckets[i]->custom_field = 0;
  }
}


#define MAX_RETRIES 20

unsigned hash_key (const char *key, int key_len) {
//  unsigned hash = (crc32 (key, key_len) >> 16) & 0x7fff;
  unsigned hash = (compute_crc32 (key, key_len) >> 8) & 0x7fffff;
  return hash ? hash : 1;
}

unsigned long long extract_num (const char *key, int key_len, char **eptr) {
  const char *ptr = key, *ptr_e = key + key_len, *num_st;
  unsigned long long x;
  while (ptr < ptr_e && (*ptr < '0' || *ptr > '9')) {
    ptr++;
  }
  if (ptr == ptr_e) {
    if (eptr) {
      *eptr = (char *)ptr;
    }
    return (unsigned long long) -1;
  }
  do {
    num_st = ptr;
    x = 0;
    while (ptr < ptr_e && *ptr >= '0' && *ptr <= '9') {
      if (x >= 0x7fffffffffffffffLL / 10) {
        if (eptr) {
          *eptr = (char *)ptr;
        }
        return (unsigned long long) -1;
      }
      x = x*10 + (*ptr - '0');
      ptr++;
    }
    if (ptr == num_st) {
      if (eptr) {
        *eptr = (char *)ptr;
      }
      return (unsigned long long) -1;
    }
  } while (num_st == key && ptr < ptr_e && *ptr++ == '~');
  if (eptr) {
    *eptr = (char *)ptr;
  }
  return x;
}

struct conn_target *calculate_key_target (const char *key, int key_len) {
  int i = 0;
  unsigned hash;
  struct conn_target *S;
  static char key_buffer[MAX_KEY_LEN+4];

  if (!CC->tot_buckets || key_len <= 0 || key_len > MAX_KEY_LEN) {
    return 0;
  }

  if (key_len >= 3 && key[key_len - 1] == '$' && key[key_len - 2] == '#' && key[key_len - 3] == '@') {
    char *tmp;
    for (tmp = (char *)(key + key_len - 4); tmp >= (char *)key; --tmp) {
      if (*tmp == '#') {
        break;
      }
    }
    if (tmp >= (char *)key) {
      char *ttt;
      unsigned r = strtoul (tmp + 1, &ttt, 10);
      if (ttt > tmp + 1 && ttt == key + key_len - 3) {
        S = CC->buckets[r % CC->tot_buckets];
        return S;
      }
    }
  }

  if ((CC->cluster_mode & 255) == CLUSTER_MODE_RANDOM) {
    for (i = 0; i < 10; i++) {
      S = CC->buckets[lrand48 () % CC->tot_buckets];
      if (S && S->active_outbound_connections) {
        return S;
      }
    }
    return 0;
  }

  if ((CC->cluster_mode & 255) == CLUSTER_MODE_FIRSTINT) {
    unsigned long long longhash = extract_num (key, key_len, 0);
    if (verbosity > 1) {
      fprintf (stderr, "extract_num(%.*s) = %llu\n", key_len, key, longhash);
    }
    if ((long long) longhash == -1) {
      return 0;
    }
    if (CC->step > 0) {
      longhash /= CC->step;
    }
    S = CC->buckets[longhash % CC->tot_buckets];
    return S->active_outbound_connections ? S : 0;
  }

  if ((CC->cluster_mode & 255) > CLUSTER_MODE_FIRSTINT && (CC->cluster_mode & 255) <= CLUSTER_MODE_FIFTHINT) {
    int k = (CC->cluster_mode & 255) - (CLUSTER_MODE_FIRSTINT - 1);
    char *p1 = (char *) key, *p2;
    int clen = key_len;
    unsigned long long longhash = 0;
    for (i = 0; i < k; i++) {
      longhash = extract_num (p1, clen, &p2);
      if ((long long) longhash == -1) {
        return 0;
      }
      assert (p2 >= p1 && p2 <= p1 + clen);
      clen -= p2 - p1;
      p1 = p2;
    }
    if (CC->step > 0) {
      longhash /= CC->step;
    }
    S = CC->buckets[longhash % CC->tot_buckets];
    return S->active_outbound_connections ? S : 0;
  }


  if (DOT_EXTENSION) {
    char *dot_pos = memchr (key, '.', key_len);
    if (dot_pos) {
      key_len = dot_pos - key;
    }
  }

  if (CC->points) {
    unsigned long long x = crc64 (key, key_len);
    int a = -1, b = CC->tot_buckets * CC->points_num, c;
    while (b - a > 1) {
      c = (a + b) >> 1;
      if (x < CC->points[c].x) {
        b = c;
      } else {
        a = c;
      }
    }

    assert (CC->points_num > 0);

    for (i = 0; i < MAX_RETRIES; i++) {
      if (a < 0) {
        a += CC->points_num;
      }
      S = CC->points[a].target;
      if (S->active_outbound_connections) {
        return S;
      }
      a--;
    }

    return 0;
  }
  
  int z = 0;
  if (key_len >= 4 && *key == '#' && *(key + 1) == '#')  {
    z = 2;
    while (z < key_len && key[z] != '#') {
      z ++;
    }
    if (z < key_len - 1 && key[z] == '#' && key[z + 1] == '#') {
      z += 2;
    } else {
      z = 0;
    }
    if (z >= key_len) {
      z = 0;
    }
  }
  hash = hash_key (key + z, key_len - z);
  if (CC->step > 0) {
    hash /= CC->step;
  }
  S = CC->buckets[hash % CC->tot_buckets];
  while (!S->active_outbound_connections && (CC->cluster_mode & 255) == CLUSTER_MODE_MEMCACHED) {
    if (!i) {
      memcpy (key_buffer+2, key + z, key_len - z);
      key_buffer[1] = '0';
      key_buffer[0] = '0';
    }
    if (++i > MAX_RETRIES) {
      return 0;
    }
    key_buffer[1]++;
    if (i < 10) {
      hash += hash_key (key_buffer+1, key_len + 1 - z);
    } else {
      if (key_buffer[1] == ':') {
        key_buffer[1] = '0';
        key_buffer[0]++;
      }
      hash += hash_key (key_buffer, key_len + 2 - z);
    }
    S = CC->buckets[hash % CC->tot_buckets];
  }

  return S;
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
  int r, u = 10000;
  if (!S) {
    return 0;
  }
  for (c = S->first_conn; c != (struct connection *) S; c = c->next) {
    r = mcp_check_ready (c);
    if (r == cr_ok) {
      if (ROUND_ROBIN_EXTENSION) {
        rotate_target (S, c);
      }
      return c;
    } else if (r == cr_stopped && c->unreliability < u) {
      u = c->unreliability;
      d = c;
    }
  }
  /* all connections failed? */
  return d;
}

static int RR[MAX_USER_FRIENDS + 16], RA[MAX_USER_FRIENDS + 16];

static void ursort (int *A, int L, int R) {
  int i, j, x, y;
  if (L >= R) {
    return;
  }
  x = RR[(L + R) >> 1];
  i = L; j = R;
  do {
    while (RR[i] < x) { i++; }
    while (x < RR[j]) { j--; }
    if (i <= j) {
      y = RR[i]; RR[i] = RR[j]; RR[j] = y;
      y = A[i]; A[i++] = A[j]; A[j--] = y;
    }
  } while (i <= j);
  ursort (A, L, j);
  ursort (A, i, R);
}


int text_forward_query (char *key, int key_len, int N, int *A, struct connection *c) {
  int i, tb = CC->tot_buckets, j, res = 0, k, st = CC->step > 0 ? CC->step : 1, cur_forwarded_queries = 0, cur_forwarded_bytes = 0;
  struct connection *d;
  struct conn_query *Q;
  struct conn_target *S;

  if (verbosity > 0) {
    fprintf (stderr, "in text_forward_query(%s, %d, %d; %d %d %d)\n", key, c->fd, N, A[0], A[1], A[2]);
  }

  ++diagonal_received_queries;

  if (N <= 0 || N >= MAX_USER_FRIENDS || !tb || (CC->cluster_mode & 255) != CLUSTER_MODE_FIRSTINT) {
    return 0;
  }
  assert (A && key);
  for (i = 0; i < N; i++) {
    RR[i] = (A[i] / st) % tb;
  }

  CC->listening_connection->query_start_time = c->query_start_time;

  ursort (A, 0, N - 1);
  RR[N] = tb;
  j = 0;
  for (i = 1; i <= N; i++) {
    if (RR[i] != RR[i - 1]) {
      if (RR[j] >= 0) {
        A[j - 1] = i - j;
        S = CC->buckets[RR[j]];
        d = get_target_connection (S);
        if (d) {
          Q = create_query (d, CC->listening_connection, CC->set_timeout);
          Q->custom_type = MCS_DATA(c)->query_type | 0x2000;

          if (verbosity > 1) {
            fprintf (stderr, "Forwarded online friends list (key = %s) to bucket %d:", key, RR[j]);
            for (k = j - 1; k < i; k++) {
              fprintf (stderr, " %d", A[k]);
            }
            fprintf (stderr, "\n");
          }

          static char sm_buff[32];
          int l = sprintf (sm_buff, " 0 0 %d\r\n", (i - j + 1) * 4);

          write_out (&d->Out, "set ", 4);
          write_out (&d->Out, key, key_len);
          write_out (&d->Out, sm_buff, l);
          write_out (&d->Out, A + (j - 1), (i - j + 1) * 4);
          write_out (&d->Out, "\r\n", 2);

	  cur_forwarded_queries++;
	  cur_forwarded_bytes += 4 + key_len + l + (i - j + 1) * 4 + 2;

          res += i - j;

          flush_output (d);
        }
      }
      j = i;
    }
  }

  if (res) {
    CC->a_req += cur_forwarded_queries;
    CC->a_sbytes += cur_forwarded_bytes;
    CC->t_req += cur_forwarded_queries;
    CC->t_sbytes += cur_forwarded_bytes;
    diagonal_forwarded_total += res;
    CC->listening_connection->generation++;
  }

  return res;
}

int diagonal_forward_query (struct connection *c, int query_len) {
  int i, tb = CC->tot_buckets, res = 0, cur_forwarded_queries = 0, cur_forwarded_bytes = 0;
  if (query_len <= 0 || query_len > STATS_BUFF_SIZE || !tb) {
    advance_read_ptr (&c->In, query_len);
    return -1;
  }
  assert (read_in (&c->In, stats_buff, query_len) == query_len);

  CC->listening_connection->query_start_time = c->query_start_time;

  ++diagonal_received_queries;

  for (i = 0; i < tb; i++) {
    struct conn_target *S = CC->buckets[i];
    struct connection *d = get_target_connection (S);
    if (d) {
      struct conn_query *Q = create_query (d, CC->listening_connection, CC->set_timeout);
      Q->custom_type = MCS_DATA(c)->query_type | 0x2000;

      if (verbosity > 1) {
        fprintf (stderr, "Forwarding to bucket %d query %.*s\n", i, query_len < 64 ? query_len : 64, stats_buff);
      }
    
      assert (write_out (&d->Out, stats_buff, query_len) == query_len);
      cur_forwarded_queries++;
      cur_forwarded_bytes += query_len;

      flush_output (d);
      d->last_query_sent_time = precise_now;
      accumulate_query_timeout (d, CC->set_timeout);
      res++;
    }
  }

  if (res) {
    diagonal_forwarded_total += res;
    CC->listening_connection->generation++;
    CC->a_req += cur_forwarded_queries;
    CC->a_sbytes += cur_forwarded_bytes;
    CC->t_req += cur_forwarded_queries;
    CC->t_sbytes += cur_forwarded_bytes;
  }

  return res;
}

int immediate_forward_query (struct connection *c, int query_len, const char *key, int key_len, int cut_offset, int cut_bytes) {
  struct conn_target *S = calculate_key_target (key + cut_bytes, key_len - cut_bytes);
  struct connection *d = get_target_connection (S);
  if (!d) {
    if (verbosity > 0) {
      fprintf (stderr, "cannot find connection to target %s:%d for key %.*s, dropping query\n", S ? conv_addr(S->target.s_addr, 0) : "?", S ? S->port : 0, key_len, key);
    }
    assert (advance_skip_read_ptr (&c->In, query_len) == query_len);
    return -1;
  }
  if (verbosity > 1) {
    fprintf (stderr, "immediate_forward_query for key '%.*s' -> '%.*s', %d bytes forwarded %d->%d\n",
  key_len, key, key_len - cut_bytes, key + cut_bytes, query_len - cut_bytes, c->fd, d->fd);
  }
  if (cut_bytes) {
    assert (cut_bytes > 0 && cut_offset > 0 && (long long) cut_offset + cut_bytes < query_len);
    assert (copy_through (&d->Out, &c->In, cut_offset) == cut_offset);
    assert (advance_skip_read_ptr (&c->In, cut_bytes) == cut_bytes);
    query_len -= cut_offset + cut_bytes;
  }
  assert (copy_through (&d->Out, &c->In, query_len) == query_len);

  CC->a_req++;
  CC->a_sbytes += query_len + cut_offset;
  CC->t_req++;
  CC->t_sbytes += query_len + cut_offset;

  CC->listening_connection->query_start_time = c->query_start_time;

  struct conn_query *Q = create_query (d, CC->listening_connection, CC->set_timeout + 0.2);
  Q->custom_type = MCS_DATA(c)->query_type | 0x2000;
  CC->listening_connection->generation++;

  immediate_forwarded_queries++;
  
  flush_output (d);
  d->last_query_sent_time = precise_now;
  accumulate_query_timeout (d, CC->set_timeout);

  return 0;
}



int proxy_server_execute (struct connection *c, int op) {
  struct mcs_data *D = MCS_DATA(c);
  static char key_buffer[MAX_KEY_LEN+4];
  nb_iterator_t R;
  int len, res, x;
  char *ptr, *ptr_e, *ptr_s;
  int diagonal_forward = 0;

  if (verbosity > 0) {
    fprintf (stderr, "proxy_mc_server: op=%d, key_len=%d, arg#=%d, query_len=%d\n", op, D->key_len, D->arg_num, D->query_len);
  }

  CC = ((struct memcache_server_functions *) c->extra)->info;
  assert (CC && &CC->mc_proxy_inbound == c->extra);

  int restrict_stats = 0;
  if (CC->crypto) {
    // accept unencrypted connections from localhost only
    if (!((c->our_ip == 0x7f000001 && c->remote_ip == 0x7f000001) || c->crypto)) {
      restrict_stats = 1;
    }
  }

  c->query_start_time = get_utime_monotonic ();
  c->queries_ok = 0;

  if (op != mct_empty) {
    netw_queries++;
    if (op != mct_get) {
      netw_update_queries++;
    }
  }

  if (op != mct_get && op != mct_empty && op != mct_version) {
    free_tmp_buffers (c);
  }

  if (restrict_stats && op != mct_stats && op != mct_version && op != mct_empty) {
    D->query_flags |= 16;
    assert (advance_skip_read_ptr (&c->In, D->query_len) == D->query_len);
    return 0;
  }

  switch (op) {

  case mct_empty:
    break;

  case mct_set:
  case mct_add:
  case mct_replace:

    if (D->key_len > 0 && D->key_len <= MAX_KEY_LEN && D->arg_num == 3 && (unsigned) D->args[2] <= MAX_VALUE_LEN) {
      int needed_bytes = D->args[2] + D->query_len + 2 - c->In.total_bytes;
      if (needed_bytes > 0) {
        return needed_bytes;
      }
      nbit_advance (&c->Q, D->args[2]);
      len = nbit_ready_bytes (&c->Q);
      assert (len > 0);
      ptr = nbit_get_ptr (&c->Q);
    } else {
      D->query_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    if (len == 1) {
      nbit_advance (&c->Q, 1);
    }
    if (ptr[0] != '\r' || (len > 1 ? ptr[1] : *((char *) nbit_get_ptr(&c->Q))) != '\n') {
      D->query_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    len = 2;

    nbit_set (&R, &c->In);
    assert (nbit_advance (&R, D->key_offset) == D->key_offset);
    assert (nbit_read_in (&R, key_buffer, D->key_len) == D->key_len);
    key_buffer[D->key_len] = 0;

    if (verbosity > 0) {
      fprintf (stderr, "mc_set: op=%d, key '%s', key_len=%d, flags=%lld, time=%lld, value_len=%lld\n", op, key_buffer, D->key_len, D->args[0], D->args[1], D->args[2]);
    }

#ifdef SEARCH_MODE_ENABLED
    if (MERGE_EXTENSION && search_check (mct_set, key_buffer, D->key_len)) {
      advance_skip_read_ptr (&c->In, D->query_len);
      search_store (c, op, key_buffer, D->key_len, (int) D->args[0], (int)D->args[1], (int)D->args[2]);
      return 0;
    }
#endif

    if (D->key_len >= 10 && !memcmp (key_buffer, "#verbosity", 10) && D->args[2] == 1) {
      assert (advance_skip_read_ptr (&c->In, D->query_len) == D->query_len);
      assert (read_in (&c->In, stats_buff, 3) == 3);
      if (stats_buff[0] >= '0' && stats_buff[0] <= '9') {
	verbosity = stats_buff[0] - '0';
        write_out (&c->Out, "STORED\r\n", 8);
      } else {
	write_out (&c->Out, "NOT_STORED\r\n", 12);
      }
      return 0;
    }

    if (TEXT_EXTENSION && 
        (
         (D->key_len >= 6 && !strncmp (key_buffer, "online", 6)) ||
         (D->key_len >= 7 && !strncmp (key_buffer, "offline", 7))  
        )
       ) {
      int size = D->args[2], user_id = -1;
      int online = (key_buffer[1] == 'n');

      assert (advance_skip_read_ptr (&c->In, D->query_len) == D->query_len);

      if (size >= STATS_BUFF_SIZE || sscanf (key_buffer + 7 - online, "%d", &user_id) != 1 || user_id <= 0) {
  write_out (&c->Out, "NOT_STORED\r\n", 12);
        assert (advance_skip_read_ptr (&c->In, size + 2) == size + 2);
        return 0;
      }

      assert (read_in (&c->In, stats_buff, size) == size);
      int *A, N;
      if (size > 3 && stats_buff[3] == 0) {
        N = *(int *)stats_buff;
        if (N <= 0 || N > MAX_USER_FRIENDS || size != 4 * N + 4) {
	  write_out (&c->Out, "NOT_STORED\r\n", 12);
          return -2;
        }
        A = (int *)(stats_buff + 4);
      } else {
        N = 0;
        stats_buff[size] = 0;
        char *ptr = stats_buff;
        while (N < MAX_USER_FRIENDS && *ptr) {
          char *oldptr;
          RA[++N] = strtoul (oldptr = ptr, &ptr, 10);
          if (ptr == oldptr || (*ptr && *ptr++ != ',')) {
            write_out (&c->Out, "NOT_STORED\r\n", 12);
            return -2;
          }
        }
        if (*ptr) {
          write_out (&c->Out, "NOT_STORED\r\n", 12);
          return -2;
        }
        A = RA + 1;
      }
      int res = text_forward_query (key_buffer, D->key_len, N, A, c);
      if (res > 0) {
        write_out (&c->Out, "STORED\r\n", 8);
      } else {
        write_out (&c->Out, "NOT_STORED\r\n", 12);
      } 
      return -2;
    }

    if (*key_buffer == '^' && D->key_len > 1) {
      if (immediate_forward_query (c, D->query_len + D->args[2] + len, key_buffer, D->key_len, D->key_offset, 1) < 0) {
        write_out (&c->Out, "NOT_STORED\r\n", 12);
      } else {
        write_out (&c->Out, "STORED\r\n", 8);
      }
      return 0;
    }

    if (HINTS_EXTENSION && D->key_len >= 11 && (!memcmp (key_buffer, "object_text", 11) || !memcmp (key_buffer, "object_type", 11))) {
      diagonal_forward = 1;
    }

    if (WATCHCAT_EXTENSION && D->key_len >= 5 && !strncmp (key_buffer, "entry", 5)) {
      diagonal_forward = 1;
    }

    if ((SEARCH_EXTENSION || SEARCHX_EXTENSION) &&
        ((D->key_len == 22 && !memcmp (key_buffer, "delete_items_with_hash", 18) && (!memcmp (key_buffer + 18, "hash", 4) || !memcmp (key_buffer + 19, "ate", 3))) ||
         (D->key_len == 17 && !memcmp (key_buffer, "change_many_", 12) && !memcmp (key_buffer + 13, "ates", 4)) ||
         (D->key_len >= 24 && !memcmp (key_buffer, "hashlist_assign_max_", 20) && !memcmp (key_buffer+21, "ate", 3)))) {
      diagonal_forward = 1;
    }

    if (NEWSR_EXTENSION && D->key_len >= 14 && !memcmp (key_buffer, "recommend_rate", 14)) {
      diagonal_forward = 1;
    }

    if (diagonal_forward) {
      if (diagonal_forward_query (c, D->query_len + D->args[2] + len) <= 0) {
        write_out (&c->Out, "NOT_STORED\r\n", 12);
      } else {
        write_out (&c->Out, "STORED\r\n", 8);
      }
      return 0;
    }

    ptr = key_buffer;
    x = 0;
    if (x < D->key_len && key_buffer[x] == '-') {
      x++;
    }
    while (x < D->key_len && key_buffer[x] >= '0' && key_buffer[x] <= '9') {
      x++;
    }
    if (x < D->key_len - 1 && key_buffer[x] == '@' && (x > 1 || key_buffer[0] != '-')) {
      x++;
      ptr = key_buffer + x;
      x = D->key_len - x;
    } else {
      ptr = key_buffer;
      x = D->key_len;
    }

    if (
        (
         ((CC->cluster_mode & 255) == CLUSTER_MODE_FIRSTINT || TEMP_EXTENSION) && ((x >= 4 && !memcmp (ptr, "temp", 4)) || (x >= 5 && !memcmp (ptr, "xtemp", 5)))
        ) ||
        (LOGS_EXTENSION && x >= 6 && !memcmp (ptr, "select", 6)) ||
        (MAGUS_EXTENSION && x >= 5 && !memcmp (ptr, "hints", 5)) ||
        (HINTS_EXTENSION && x >= 10 && !memcmp (ptr, "sort_by_ra", 10)) ||
        (NEWS_EXTENSION && 
         (
          (x >= 8 && !memcmp (ptr, "userlist", 8)) ||
          (x >= 10 && !memcmp (ptr, "objectlist", 10)) ||
          (x >= 6 && !memcmp (ptr, "urlist", 6))
         )
        ) ||
        (BAYES_EXTENSION && x >= 12 && !memcmp (ptr, "current_text", 12)) ||
        (ANTISPAM_EXTENSION && x >= 7 && !memcmp (ptr, "matches", 7)) ||
        (SUPPORT_EXTENSION && x >= 8 && !memcmp (ptr, "question", 8)) ||
        (PHOTO_EXTENSION && x >= 22 && !memcmp (ptr, "photos_overview_albums", 22)) ||
        (FRIENDS_EXTENSION && x >= 8 && !memcmp (ptr, "userlist", 8))
       ) {
      int query_len = D->query_len + D->args[2] + len; 
      c->Tmp = alloc_head_buffer ();
      assert (copy_through (c->Tmp, &c->In, query_len) == query_len);
      if (verbosity > 0) {
        fprintf (stderr, "stored tmp key to %p (%d bytes)\n", c->Tmp, query_len);
      }
      write_out (&c->Out, "STORED\r\n", 8);
    } else {
      if (forward_query (c, D->query_len + D->args[2] + len, key_buffer, D->key_len) < 0) {
        write_out (&c->Out, "NOT_STORED\r\n", 12);
      }
    }

    return 0;

  case mct_incr:
  case mct_decr:
  case mct_delete:

    if (!(D->key_len > 0 && D->key_len <= MAX_KEY_LEN && D->arg_num <= 1 && (D->arg_num || D->query_type == mct_delete))) {
      D->query_flags |= 16;
      return SKIP_ALL_BYTES;
    }

    nbit_set (&R, &c->In);
    assert (nbit_advance (&R, D->key_offset) == D->key_offset);
    assert (nbit_read_in (&R, key_buffer, D->key_len) == D->key_len);
    key_buffer[D->key_len] = 0;

    if (verbosity > 0) {
      fprintf (stderr, "mc_incr: op=%d, key '%s', key_len=%d, arg=%lld\n", op, key_buffer, D->key_len, D->args[0]);
    }

    if (LISTS_EXTENSION && op == mct_delete && D->key_len >= 6 && !strncmp (key_buffer, "object", 6)) {
      diagonal_forward = 1;
    }

    if (HINTS_EXTENSION && op == mct_delete && D->key_len >= 11 && !strncmp (key_buffer, "object_text", 11)) {
      diagonal_forward = 1;
    }

    if (diagonal_forward) {
      if (diagonal_forward_query (c, D->query_len) <= 0) {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
      } else {
        write_out (&c->Out, "DELETED\r\n", 9);
      }
      return 0;
    }

    if (*key_buffer == '^' && D->key_len > 1) {
      if (immediate_forward_query (c, D->query_len, key_buffer, D->key_len, D->key_offset, 1) < 0) {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
      } else if (op == mct_delete) {
        write_out (&c->Out, "DELETED\r\n", 9);
      } else {
        write_out (&c->Out, "0\r\n", 3);
      }
      return 0;
    }

    if (forward_query (c, D->query_len, key_buffer, D->key_len) < 0) {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }

    return 0;

  case mct_get:

    assert (advance_skip_read_ptr (&c->In, 4) == 4);
    x = 0;
    while (1) {
      ptr = ptr_s = get_read_ptr (&c->In);
      ptr_e = ptr + get_ready_bytes (&c->In);
      while (ptr < ptr_e && (*ptr == ' ' || *ptr == '\t')) {
        ptr++;
      }
      if (ptr == ptr_e) {
        advance_read_ptr (&c->In, ptr - ptr_s);
        continue;
      }
      if (*ptr == '\r' || *ptr == '\n') {
        break;
      }
      len = 0;
      while (ptr < ptr_e && *ptr != ' ' && *ptr != '\t' && *ptr != '\r' && *ptr != '\n') {
        if (len <= MAX_KEY_LEN) {
          key_buffer[len++] = *ptr;
        }
        ptr++;
        if (ptr == ptr_e) {
          advance_read_ptr (&c->In, ptr - ptr_s);
          ptr = ptr_s = get_read_ptr (&c->In);
          ptr_e = ptr + get_ready_bytes (&c->In);
        }
      }
      assert (ptr < ptr_e);
      if (len <= MAX_KEY_LEN) {
        key_buffer[len] = 0;
        if (!x) {
          MCS_FUNC(c)->mc_get_start (c);
        }
        MCS_FUNC(c)->mc_get (c, key_buffer, len);
        x++;
      }
      advance_read_ptr (&c->In, ptr - ptr_s);
    }
    assert (ptr < ptr_e && (*ptr == '\r' || *ptr == '\n'));
    ptr++;
    if (ptr < ptr_e && ptr[-1] == '\r' && ptr[0] == '\n') {
      ptr++;
    }
    advance_read_ptr (&c->In, ptr - ptr_s);

    if (x) {
      MCS_FUNC(c)->mc_get_end (c, x);
    } else {
      write_out (&c->Out, "END\r\n", 5);
    }

    return 0;

  case mct_stats:

    if (D->key_len || D->arg_num) {
      D->query_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    assert (advance_skip_read_ptr (&c->In, D->query_len) == D->query_len);

    res = MCS_FUNC(c)->mc_stats (c);
    
    return 0;

  case mct_version:

    if (D->key_len || D->arg_num) {
      D->query_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    assert (advance_skip_read_ptr (&c->In, D->query_len) == D->query_len);

    res = MCS_FUNC(c)->mc_version (c);
    
    return 0;

  case mct_CLOSE:
    c->status = conn_error;
    c->error = -1;
    return 0;


  default:
    D->query_flags |= 16;
    break;
  }

  if (op != mct_empty) {
    fprintf (stderr, "advance_skip_read_ptr(%p,%d), op=%d\n", &c->In, D->query_len, op);
  }

  assert (advance_skip_read_ptr (&c->In, D->query_len) == D->query_len);
  return 0;
}

int mcp_prepare_stats (struct connection *c, char *stats_buffer, int stats_buffer_len) {
  dyn_update_stats();
  int stats_len = prepare_stats (c, stats_buffer, stats_buffer_len);
  if (*extension_name) {
    stats_len += snprintf (stats_buffer + stats_len, stats_buffer_len - stats_len, "extension\t%s\n", extension_name);
  }
  stats_len += snprintf (stats_buffer + stats_len, stats_buffer_len - stats_len,
    "heap_allocated\t%ld\n"
    "heap_max\t%ld\n"
    "wasted_heap_blocks\t%d\n"
    "wasted_heap_bytes\t%ld\n"
    "free_heap_blocks\t%d\n"
    "free_heap_bytes\t%ld\n"
    "config_filename\t%s\n"
    "config_loaded_at\t%d\n"
    "config_size\t%d\n"
    "config_md5\t%s\n"
    "cluster_name\t%s\n"
    "cluster_number\t%d\n"
    "cluster_mode\t0x%04x\n"
    "get_timeout\t%.3f\n"
    "set_timeout\t%.3f\n"
    "tot_clusters\t%d\n"
    "cluster_size\t%d\n"
    "points_per_server\t%d\n"
    "active_queries\t%d\n"
    "total_forwarded_queries\t%lld\n"
    "expired_forwarded_queries\t%lld\n"
    "diagonal_received_queries\t%lld\n"
    "diagonal_forwarded_total\t%lld\n"
    "immediate_forwarded_queries\t%lld\n"
    "dropped_overflow_responses\t%lld\n"
    "tot_skipped_answers\t%lld\n"
    "errors_received\t%lld\n"
    "client_errors_received\t%lld\n"
    "total_failed_connections\t%lld\n"
    "total_connect_failures\t%lld\n",
    (long)(dyn_cur - dyn_first),
    (long)(dyn_last - dyn_first),
    wasted_blocks,
    wasted_bytes,
    freed_blocks,
    freed_bytes,
    config_filename,
    CurConf->config_loaded_at,
    CurConf->config_bytes,
    CurConf->config_md5_hex,
    CC->cluster_name,
    CC->cluster_no,
    CC->cluster_mode,
    CC->get_timeout,
    CC->set_timeout,
    CurConf->clusters_num,
    CC->tot_buckets,
    CC->points_num,
    active_queries,
    tot_forwarded_queries,
    expired_forwarded_queries,
    diagonal_received_queries,
    diagonal_forwarded_total,
    immediate_forwarded_queries,
    dropped_overflow_responses,
    tot_skipped_answers,
    errors_received,
    client_errors_received,
    total_failed_connections,
    total_connect_failures);
#ifdef SEARCH_MODE_ENABLED
  stats_len += search_stats (stats_buffer + stats_len, stats_buffer_len - stats_len);
#endif
  return stats_len;
}

static char mcp_stats_buffer[65536];

int mcp_stats (struct connection *c) {
  int stats_len = mcp_prepare_stats (c, mcp_stats_buffer, sizeof (mcp_stats_buffer) - 6);
  stats_len += snprintf (mcp_stats_buffer + stats_len, STATS_BUFF_SIZE - stats_len - 10, 
      "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
     "64-bit"
#else
     "32-bit"
#endif
      " after commit " COMMIT "\n");
  write_out (&c->Out, mcp_stats_buffer, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int mcp_get_stats (struct connection *c, const char *key) {
  int stats_len = mcp_prepare_stats (c, mcp_stats_buffer, sizeof (mcp_stats_buffer) - 6);
  return return_one_key (c, key, mcp_stats_buffer, stats_len);
}

#define EXPQ_SIZE 128
int Bq[EXPQ_SIZE*3], Bq_W;

void store_expired_target (int ip_addr, int port) {
  int i = Bq_W++ & (EXPQ_SIZE - 1);
  Bq[i*3] = ip_addr;
  Bq[i*3+1] = port;
  Bq[i*3+2] = now;
}

#define MAX_RES 262144
int R[MAX_RES+16], *R_end, R_cnt;

int mcp_get_targets (struct connection *c, const char *key, int key_len) {
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

int mcp_get_bad_targets (struct connection *c, const char *key, int key_len) {
  int i;
  R_end = R;
  for (i = 0; i < allocated_targets && R_end < R + MAX_RES; i++) {
    struct conn_target *S = &Targets[i];
    assert (S);
    if (S->min_connections > 0 && !S->ready_outbound_connections) {
      R_end[0] = ntohl (S->target.s_addr);
      R_end[1] = S->port;
      R_end += 2;
    }
  }
  return return_one_key_list (c, key, key_len, allocated_targets, 2, R, R_end - R);
}

int mcp_get_expired_targets (struct connection *c, const char *key, int key_len) {
  int i = Bq_W - EXPQ_SIZE + 1;
  R_end = R;
  if (i < 0) {
    i = 0;
  }
  for (; i < Bq_W; i++) {
    R_end[0] = Bq[(i & (EXPQ_SIZE - 1)) * 3];
    R_end[1] = Bq[(i & (EXPQ_SIZE - 1)) * 3 + 1];
    R_end[2] = Bq[(i & (EXPQ_SIZE - 1)) * 3 + 2];
    R_end += 3;
  }
  return return_one_key_list (c, key, key_len, Bq_W, 3, R, R_end - R);
}

void store_connection (struct connection *c) {
  R_end[0] = c->fd;
  R_end[1] = c->status;
  R_end[2] = c->flags;
  R_end[3] = c->ready;
  R_end[4] = c->unreliability;
  R_end += 5;
}


int mcp_get_connections (struct connection *c, const char *key, int key_len) {
  int i;
  R_end = R;
  for (i = 0; i <= max_connection && R_end < R + MAX_RES; i++) {
    store_connection (&Connections[i]);
  }
  return return_one_key_list (c, key, key_len, max_connection+1, 5, R, R_end - R);
}

int mcp_get_compute_target (struct connection *c, const char *key, int key_len) {
  struct conn_target *S = calculate_key_target (key+6, key_len-6);
  if (!S) {
    return 0;
  }
  R[0] = ntohl (S->target.s_addr);
  R[1] = S->port;
  return return_one_key_list (c, key, key_len, 1, 2, R, 2);
}


int mcp_get_last_complete_count (struct connection *c, const char *key, int key_len) {
  return return_one_key (c, key, mcp_stats_buffer, sprintf (mcp_stats_buffer, "%d", MCS_DATA(c)->last_complete_count));
}

int mcp_get_cluster_size (struct connection *c, const char *key) {
  return return_one_key (c, key, mcp_stats_buffer, sprintf (mcp_stats_buffer, "%d", CC->tot_buckets));
}


int mcp_get_cluster_targets (struct connection *c, const char *key, int key_len) {
  int i;
  R_end = R;
  for (i = 0; i < CC->tot_buckets && R_end < R + MAX_RES; i++) {
    struct conn_target *S = CC->buckets[i];
    assert (S);
    R_end[0] = ntohl (S->target.s_addr);
    R_end[1] = S->port;
    R_end += 2;
  }
  return return_one_key_list (c, key, key_len, allocated_targets, 2, R, R_end - R);
}


int mcp_get_clusters_stats (struct connection *c, const char *key, int key_len) {
  int i;
  char *stats_buff_ptr = stats_buff;
  struct mc_cluster *C = CurConf->Clusters;
 
  stats_buff_ptr += sprintf (stats_buff_ptr, "Port\tBuckets\tReq*\tTX*\tRX*\tTOut*\tTot_Req\tTot_TX\tTot_RX\tTot_T/O\tName\n");
  for (i = 0; i < CurConf->clusters_num; i++, C++) {
    assert (stats_buff_ptr < stats_buff + STATS_BUFF_SIZE - 1024);
    stats_buff_ptr += sprintf (stats_buff_ptr, "%d\t%d\t%.1f\t%.1f\t%.1f\t%.1f\t%lld\t%lld\t%lld\t%lld\t%s\n",
			       C->port, C->tot_buckets,
			       C->a_req, C->a_sbytes, C->a_rbytes, C->a_timeouts,
			       C->t_req, C->t_sbytes, C->t_rbytes, C->t_timeouts,
			       C->cluster_name);
  }
  return return_one_key (c, key, stats_buff, stats_buff_ptr - stats_buff);
}



int tot_queries;

int delete_query (struct conn_query *q);
int query_timeout (struct conn_query *q);


conn_query_type_t proxy_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "mc-proxy-query",
.parse_execute = server_failed,
.close = delete_query,
.wakeup = query_timeout
};

struct conn_query *create_query (struct connection *d, struct connection *c, double timeout) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_query(%p[%d], %p[%d]): Q=%p\n", d, d->fd, c, c->fd, Q);
  }

  Q->custom_type = MCS_DATA(c)->query_type;
  Q->outbound = d;
  Q->requester = c;
  Q->start_time = c->query_start_time;
  Q->extra = 0;
  Q->cq_type = &proxy_query_type;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  insert_conn_query (Q);
  active_queries++;
  tot_forwarded_queries++;

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query()\n");
  }

  return Q;
}

struct conn_query *create_query_type (struct connection *d, struct connection *c, double timeout, int type) {
  struct conn_query *Q = zmalloc (sizeof (struct conn_query));

  if (verbosity > 1) {
    fprintf (stderr, "create_query(%p[%d], %p[%d]): Q=%p\n", d, d->fd, c, c->fd, Q);
  }

  Q->custom_type = type;
  Q->outbound = d;
  Q->requester = c;
  Q->start_time = c->query_start_time;
  Q->extra = 0;
  Q->cq_type = &proxy_query_type;
  Q->timer.wakeup_time = (timeout > 0 ? Q->start_time + timeout : 0);

  insert_conn_query (Q);
  active_queries++;
  tot_forwarded_queries++;

  if (verbosity > 1) {
    fprintf (stderr, "after insert_conn_query()\n");
  }

  return Q;
}

int get_targets = 0, get_keys = 0, get_search_queries = 0;
struct conn_target *get_target[MAX_CLUSTER_SERVERS];
struct connection *get_connection[MAX_CLUSTER_SERVERS];

int mcp_get_start (struct connection *c) {
  get_targets = 0;
  get_search_queries = 0;
  get_keys = 0;
  c->generation = ++conn_generation;
  c->pending_queries = 0;
  MCS_DATA(c)->last_complete_count = MCS_DATA(c)->complete_count;
  MCS_DATA(c)->complete_count = 0;
  return 0;
}

int mcp_get (struct connection *c, const char *key, int key_len) {
  if (key_len > 1 && key[0] == '#') {
    if (!strcmp (key, "#targets")) {
      mcp_get_targets (c, key, key_len);
      return 1;
    } else if (!strcmp (key, "#connections")) {
      mcp_get_connections (c, key, key_len);
      return 1;
    } else if (!strcmp (key, "#bad_targets")) {
      mcp_get_bad_targets (c, key, key_len);
      return 1;
    } else if (!strcmp (key, "#expired_targets")) {
      mcp_get_expired_targets (c, key, key_len);
      return 1;
    } else if (!strncmp (key, "#calc#", 6)) {
      mcp_get_compute_target (c, key, key_len);
      return 1;
    } else if (!strcmp (key, "#last_complete_count")) {
      mcp_get_last_complete_count (c, key, key_len);
      return 1;
    } else if (!strcmp (key, "#last_complete_count#")) {
      mcp_get_last_complete_count (c, key, key_len);
      return 1;
    } else if (!strcmp (key, "#stats")) {
      mcp_get_stats (c, key);
      return 1;
    } else if (!strcmp (key, "#cluster_targets")) {
      mcp_get_cluster_targets (c, key, key_len);
      return 1;
    } else if (!strcmp (key, "#clusters_stats")) {
      mcp_get_clusters_stats (c, key, key_len);
      return 1;
    } else if (!strcmp (key, "#current_cluster_size")) {
      mcp_get_cluster_size (c, key);
      return 1;
    } else if (!strcmp (key, "#free_block_stats")) {
      return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, FreeCnt, MAX_RECORD_WORDS);
      return 0;
    } else if (!strcmp (key, "#used_block_stats")) {
      return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, UsedCnt, MAX_RECORD_WORDS);
      return 0;
    } else if (!strcmp (key, "#allocation_stats")) {
      return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, NewAllocations[0], MAX_RECORD_WORDS*4);
      return 0;
    } else if (!strcmp (key, "#split_block_stats")) {
      return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, SplitBlocks, MAX_RECORD_WORDS);
      return 0;
    }
  }
#ifdef SEARCH_MODE_ENABLED
  int search_mode = MERGE_EXTENSION && search_check (mct_get, key, key_len);
  if (search_mode) {
    if (verbosity >= 4) {
      fprintf (stderr, "use search methods from merge extension (key = %s)\n", key);
    }
    search_create (c, (char *)key, key_len);
    if (verbosity >= 4) {
      fprintf (stderr, "use search methods from merge ended successfully\n");
    }
    return 1;
  }
#endif
  struct conn_target *S = calculate_key_target (key, key_len);
  struct connection *d;
  int x;
  if (verbosity > 1) {
    fprintf (stderr, "proxy_get (%.*s): target=%p\n", key_len, key, S);
  }
  if (!S) {
    return 0;
  }
  if (S->custom_field) {
#ifdef SEARCH_MODE_ENABLED
    if (S->custom_field < 0) {
      x = -S->custom_field - 1;
      assert ((unsigned) x < (unsigned) get_targets);
      write_out (&get_connection[x]->Out, "get ", 4);
      S->custom_field = - S->custom_field;
    }
#endif
    x = S->custom_field - 1;
    assert ((unsigned) x < (unsigned) get_targets);
    assert (get_target[x] == S);
    d = get_connection[x];
    write_out (&d->Out, " ", 1);
  } else {
    d = get_target_connection (S);
    if (!d) {
      return 0;
    }
    assert (get_targets < MAX_CLUSTER_SERVERS);
    x = get_targets++;
    S->custom_field = x+1;
    get_target[x] = S;
    get_connection[x] = d;
    if (/*get_targets == 1 && */ c->Tmp) {
      int query_len = get_total_ready_bytes (c->Tmp);
      assert (copy_through_nondestruct (&d->Out, c->Tmp, query_len) == query_len);
      CC->a_req++;
      CC->t_req++;
      CC->a_sbytes += query_len;
      CC->t_sbytes += query_len;
      if (verbosity > 1) {
        fprintf (stderr, "proxy_get (%.*s): forwarded %d saved bytes\n", key_len, key, query_len);
      }
    }
    write_out (&d->Out, "get ", 4);
    CC->a_sbytes += 6;
    CC->t_sbytes += 6;
    CC->a_req++;
    CC->t_req++;
    if (verbosity > 1) {
      fprintf (stderr, "proxy_get (%.*s): connection=%p fd=%d status=%d ready=%d unreliable=%d\n", key_len, key, d, d->fd, d->status, d->ready, d->unreliability);
    }
  }
  write_out (&d->Out, key, key_len);
  CC->a_sbytes += key_len;
  CC->t_sbytes += key_len;
  get_keys++;
  return 1;
}

int mcp_get_end (struct connection *c, int key_count) {
  int i;
  struct connection *d;
  if (verbosity > 1) {
    fprintf (stderr, "proxy_get_end (%d)\n", key_count);
  }
  if (!get_targets) {
    free_tmp_buffers (c);
    write_out (&c->Out, "END\r\n", 5);
    return 0;
  }
  c->status = conn_wait_net;

  for (i = 0; i < get_targets; i++) {
    struct conn_query *Q;
    int x = get_target[i]->custom_field;
    get_target[i]->custom_field = 0;

    if (x > 0) {
      d = get_connection[i];
      write_out (&d->Out, "\r\n", 2);
      /* create query structure related to c & d */
      Q = create_query (d, c, CC->get_timeout + 0.2);
      if (/* !i && */ c->Tmp) {
        Q->custom_type |= 0x1000;
      }
      flush_output (d);
      d->last_query_sent_time = precise_now;
      accumulate_query_timeout (d, CC->get_timeout);
    }
  }
  set_connection_timeout (c, CC->get_timeout);
  free_tmp_buffers (c);
  return 0;
}

int forward_query (struct connection *c, int query_len, const char *key, int key_len) {
  struct conn_target *S = calculate_key_target (key, key_len);
  struct connection *d = get_target_connection (S);
  if (!d) {
    if (verbosity > 0) {
      fprintf (stderr, "cannot find connection to target %s:%d for key %.*s, dropping query\n", S ? conv_addr(S->target.s_addr, 0) : "?", S ? S->port : 0, key_len, key);
    }
    assert (advance_skip_read_ptr (&c->In, query_len) == query_len);
    return -1;
  }
  assert (copy_through (&d->Out, &c->In, query_len) == query_len);

  CC->a_req++;
  CC->a_sbytes += query_len;
  CC->t_req++;
  CC->t_sbytes += query_len;

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  create_query (d, c, CC->set_timeout + 0.2);
  c->status = conn_wait_net;
  flush_output (d);
  d->last_query_sent_time = precise_now;
  accumulate_query_timeout (d, CC->set_timeout);
  set_connection_timeout (c, CC->set_timeout);
  return 0;
}


int forward_response (struct connection *c, int response_len) {
  struct connection *d = c->first_query->requester;
  assert (d);
  if (d->generation == c->first_query->req_generation) {
    CC = ((struct memcache_server_functions *) d->extra)->info;
    assert (CC && &CC->mc_proxy_inbound == d->extra);

    if (response_len > SMALL_RESPONSE_THRESHOLD && NB_used * 16 > NB_max * 12 && (NB_used * 16 > NB_max * 15 || response_len > BIG_RESPONSE_THRESHOLD)) {
      dropped_overflow_responses++;
      assert (advance_skip_read_ptr (&c->In, response_len) == response_len);
    } else {
      assert (copy_through (&d->Out, &c->In, response_len) == response_len);
    }
    CC->a_rbytes += response_len;
    CC->t_rbytes += response_len;
  } else {
    assert (advance_skip_read_ptr (&c->In, response_len) == response_len);
  }
  return 0;
}

int query_complete (struct connection *c, int ok) {
  struct conn_query *q = c->first_query;
  struct connection *d = c->first_query->requester;
  assert (d);
  if (d->generation == c->first_query->req_generation) {
    d->queries_ok += ok;
    MCS_DATA(d)->complete_count++;
  }
  active_queries--;
  c->unreliability >>= 1;
  delete_conn_query (q);
  zfree (q, sizeof (*q));
  return 0;
}

int query_complete_custom (struct conn_query *q, int ok) {
  struct connection *c = q->outbound;
  struct connection *d = q->requester;
  assert (d);
  assert (c);
  if (d->generation == q->req_generation) {
    d->queries_ok += ok;
    MCS_DATA(d)->complete_count++;
  }
  active_queries--;
  c->unreliability >>= 1;
  delete_conn_query (q);
  zfree (q, sizeof (*q));
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
  expired_forwarded_queries++;

  struct conn_target *S = q->outbound->target;
  store_expired_target (ntohl (S->target.s_addr), S->port);

  return 0;
}

int proxy_client_execute (struct connection *c, int op) {
  struct mcc_data *D = MCC_DATA(c);
  int len, x = 0;
  char *ptr;

  int get_allows_stored = 0, ignore_response = 0;
  
  if (verbosity > 0) {
    fprintf (stderr, "proxy_mc_client: op=%d, key_len=%d, arg#=%d, response_len=%d\n", op, D->key_len, D->arg_num, D->response_len);
  }

  if (c->first_query == (struct conn_query *)c && op != mcrt_empty && op != mcrt_VERSION) {
    if (verbosity >= 0) {
      fprintf (stderr, "response received for empty query list? op=%d\n", op);
      if (verbosity > -2) {
        dump_connection_buffers (c);
        if (c->first_query != (struct conn_query *) c && c->first_query->req_generation == c->first_query->requester->generation) {
          dump_connection_buffers (c->first_query->requester);
        }
      }
    }
    D->response_flags |= 16;
    return SKIP_ALL_BYTES;
  }

  if (c->first_query != (struct conn_query *) c && op != mcrt_empty && op != mcrt_VERSION) {
    get_allows_stored = c->first_query->custom_type & 0x1000;
    ignore_response = c->first_query->custom_type & 0x2000;
    c->first_query->custom_type &= 0xfff;
  }

  c->last_response_time = precise_now;

  switch (op) {

  case mcrt_empty:
    return SKIP_ALL_BYTES;

  case mcrt_VALUE:

    if (D->key_len > 0 && D->key_len <= MAX_KEY_LEN && D->arg_num == 2 && (unsigned) D->args[1] <= MAX_VALUE_LEN) {
      int needed_bytes = D->args[1] + D->response_len + 2 - c->In.total_bytes;
      if (needed_bytes > 0) {
        return needed_bytes;
      }
      nbit_advance (&c->Q, D->args[1]);
      len = nbit_ready_bytes (&c->Q);
      assert (len > 0);
      ptr = nbit_get_ptr (&c->Q);
    } else {
      fprintf (stderr, "error at VALUE: op=%d, key_len=%d, arg_num=%d, value_len=%lld\n", op, D->key_len, D->arg_num, D->args[1]);
     
      if (verbosity > -2) {
        dump_connection_buffers (c);
        if (c->first_query != (struct conn_query *) c && c->first_query->req_generation == c->first_query->requester->generation) {
          dump_connection_buffers (c->first_query->requester);
        }
      }

      D->response_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    if (len == 1) {
      nbit_advance (&c->Q, 1);
    }
    if (ptr[0] != '\r' || (len > 1 ? ptr[1] : *((char *) nbit_get_ptr(&c->Q))) != '\n') {
      fprintf (stderr, "missing cr/lf at VALUE: op=%d, key_len=%d, arg_num=%d, value_len=%lld\n", op, D->key_len, D->arg_num, D->args[1]);

      if (verbosity > -2) {
        dump_connection_buffers (c);
        if (c->first_query != (struct conn_query *) c && c->first_query->req_generation == c->first_query->requester->generation) {
          dump_connection_buffers (c->first_query->requester);
        }
      }

      D->response_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    len = 2;

    if (verbosity > 0) {
      fprintf (stderr, "mcc_value: op=%d, key_len=%d, flags=%lld, time=%lld, value_len=%lld\n", op, D->key_len, D->args[0], D->args[1], D->args[2]);
    }

    if (c->first_query->custom_type != mct_get || get_allows_stored) {
      fprintf (stderr, "VALUE obtained for a non-get query: op=%d, query_type=%d, key_len=%d, arg_num=%d, value_len=%lld, get_allows_stored=%d\n", op, c->first_query->custom_type, D->key_len, D->arg_num, D->args[1], get_allows_stored);
      D->response_flags |= 16;
      return SKIP_ALL_BYTES;
    }

    int search_mode = 0;
#ifdef SEARCH_MODE_ENABLED
    if (MERGE_EXTENSION) {
      static char buff[1024];
      nb_iterator_t R;
      nbit_set (&R, &c->In);
      assert (nbit_advance (&R, D->key_offset) == D->key_offset);
      int l = nbit_read_in (&R, buff, 1023);
      buff[l] = 0;
      search_mode = search_check (mct_get, buff, l);
    }
#endif
    if (!search_mode) {
      forward_response (c, D->response_len + D->args[1] + len);
    } else {
#ifdef SEARCH_MODE_ENABLED
      search_merge (c, D->response_len + D->args[1] + len);
#else 
      assert (0);
#endif
    }

    return 0;

  case mcrt_VERSION:

    c->unreliability >>= 1;
    if (verbosity > 0) {
      fprintf (stderr, "mcc_got_version: op=%d, key_len=%d, unreliability=%d\n", op, D->key_len, c->unreliability);
    }

    return SKIP_ALL_BYTES;

  case mcrt_CLIENT_ERROR:
    fprintf (stderr, "CLIENT_ERROR received from connection %d (%s:%d)\n", c->fd, conv_addr (c->remote_ip, 0), c->remote_port);
    client_errors_received++;
  case mcrt_ERROR:
    errors_received++;
    if (verbosity > -2 && errors_received < 32) {
      dump_connection_buffers (c);
      if (c->first_query != (struct conn_query *) c && c->first_query->req_generation == c->first_query->requester->generation) {
        dump_connection_buffers (c->first_query->requester);
      }
    }
    fail_connection (c, -5);
    c->ready = cr_failed;
    return SKIP_ALL_BYTES;
  case mcrt_SERVER_ERROR:
    x = -1 & (~(1 << mct_get));
    break;

  case mcrt_NUMBER:
    x = (1 << mct_incr) | (1 << mct_decr);
    break;

  case mcrt_STORED:
  case mcrt_NOTSTORED:
    x = (1 << mct_set) | (1 << mct_add) | (1 << mct_replace);
    if (get_allows_stored) {
      x |= (1 << mct_get);
    }
    break;

  case mcrt_DELETED:
    x = 1 << mct_delete;
    break;

  case mcrt_NOTFOUND:
    x = (1 << mct_delete) | (1 << mct_incr) | (1 << mct_decr);
    break;

  case mcrt_END:
    x = 1 << mct_get;
    break;

  default:
    x = 0;
  }

#ifdef SEARCH_MODE_ENABLED
  if (MERGE_EXTENSION && op == mcrt_END) {
    while (c->first_query != (struct conn_query *)c) {
      struct conn_query *q = c->first_query;
      if (q->extra) {
        if (verbosity > 1) {
          fprintf (stderr, "Deleting unanswered search query %p\n", q);
        }
        search_skip (c, q);
        //query_complete (c, 1);
      } else {
        break;
      }
    }
    assert (c->first_query != (struct conn_query *)c);
  }
#endif

  if (D->key_len || D->arg_num != (op == mcrt_NUMBER) || ((1 << c->first_query->custom_type) & x) == 0) {
    fprintf (stderr, "INCORRECT response obtained for a query: op=%d, query_type=%d, key_len=%d, arg_num=%d, allowed_mask=%d\n", op, c->first_query->custom_type, D->key_len, D->arg_num, x);
    D->response_flags |= 16;
    return SKIP_ALL_BYTES;
  }

  if (verbosity > 0) {
    fprintf (stderr, "mcc_op: op=%d, key_len=%d, arg=%lld\n", op, D->key_len, D->args[0]);
  }

  if (c->first_query->custom_type == mct_get && (op == mcrt_STORED || op == mcrt_NOTSTORED)) {
    if (verbosity > 1) {
      fprintf (stderr, "STORED/NOT_STORED response for get, skipping\n");
    }
    return SKIP_ALL_BYTES;
  }

  if (ignore_response) {
    if (verbosity > 1) {
      fprintf (stderr, "STORED/NOT_STORED/DELETED/... response (%d) for immediate/diagonal distribution query, skipping\n", op);
    }
    ++tot_skipped_answers;
    query_complete (c, 1);
    return SKIP_ALL_BYTES;
  }

  if (c->first_query->custom_type == mct_get || op == mcrt_END) {
    query_complete (c, op == mcrt_END);
    return SKIP_ALL_BYTES;
  }

  forward_response (c, D->response_len);
  query_complete (c, 1);

  return 0;
}


int mcp_wakeup (struct connection *c) {
  struct mcs_data *D = MCS_DATA(c);

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_expect_query;

  switch (D->query_type & 0x2fff) {
  case mct_get:
    write_out (&c->Out, "END\r\n", 5);
    break;
  case mct_incr:
  case mct_decr:
  case mct_delete:
    if (!c->queries_ok) {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }
    break;
  case mct_add:
  case mct_replace:
  case mct_set:
    if (!c->queries_ok) {
      write_out (&c->Out, "NOT_STORED\r\n", 12);
    }
    break;
  }
  clear_connection_timeout (c);
  mcs_pad_response (c);
  return 0;
}

int mcp_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "proxy_memcache_server connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }

  CC = ((struct memcache_server_functions *) c->extra)->info;
  assert (CC && &CC->mc_proxy_inbound == c->extra);
  CC->a_timeouts ++;
  CC->t_timeouts ++;
  
  assert (c->status == conn_wait_net);
  return mcp_wakeup (c);
}

int mcc_crypto_check_perm (struct connection *c) {
  int x = mcc_default_check_perm (c);
  if (x > 0) {
    return 2;
  } else {
    return x;
  }
}

int mcp_connected (struct connection *c) {
  c->last_query_time = drand48() * 0.1;
  return 0;
}

int mcp_check_ready (struct connection *c) {
  double first_query_time = 0;
  double adj_precise_now = precise_now - c->last_query_timeout;

  if (c->status == conn_connecting || (c->last_response_time == 0 && (c->status == conn_wait_answer || c->status == conn_reading_answer))) {
    if (c->last_query_sent_time < adj_precise_now - CONNECT_TIMEOUT) {
      fail_connection (c, -6);
      return c->ready = cr_failed;
    }
    return c->ready = cr_notyet;
  }

  if (c->last_query_sent_time < adj_precise_now - ping_interval - c->last_query_time && c->first_query == (struct conn_query *) c) {
    write_out (&c->Out, "version\r\n", 9);
    c->last_query_sent_time = precise_now;
    c->last_query_time = drand48() * 0.1;
    flush_output (c);
  }
  if (c->first_query != (struct conn_query *) c) {
    first_query_time = c->first_query->start_time;
  }
  if (c->last_response_time < adj_precise_now - ping_interval*2 || (first_query_time > 0 && first_query_time < adj_precise_now - RESPONSE_FAIL_TIMEOUT) || c->unreliability > 5000) {
    if (verbosity > 1 && c->ready != cr_failed) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [FAILED] fq=%.03f lq=%.03f lqt=%.03f lr=%.03f now=%.03f unr=%d\n", c->fd, c->ready, cr_failed, first_query_time, c->last_query_sent_time, c->last_query_timeout, c->last_response_time, precise_now, c->unreliability);
    }
    fail_connection (c, -5);
    return c->ready = cr_failed;
  }
  if (c->last_response_time < adj_precise_now - ping_interval - 3 || (first_query_time > 0 && first_query_time < adj_precise_now - 3) || c->unreliability > 2000) {
    if (verbosity > 1 && c->ready != cr_stopped) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [STOPPED] fq=%.03f lq=%.03f lqt=%.03f lr=%.03f now=%.03f unr=%d\n", c->fd, c->ready, cr_stopped, first_query_time, c->last_query_sent_time, c->last_query_timeout, c->last_response_time, precise_now, c->unreliability);
    }
    return c->ready = cr_stopped;
  }
    
  if (!(c->flags & C_FAILED) && c->last_response_time > 0 && (c->status == conn_wait_answer || c->status == conn_reading_answer)) {
    if (verbosity > 1 && c->ready != cr_ok) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [OK] fq=%.03f lq=%.03f lqt=%.03f lr=%.03f now=%.03f unr=%d\n", c->fd, c->ready, cr_ok, first_query_time, c->last_query_sent_time, c->last_query_timeout, c->last_response_time, precise_now, c->unreliability);
    }
    return c->ready = cr_ok;
  }

  return c->ready = cr_failed;
}


/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

void reopen_logs (void) {
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
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
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
  kprintf ("SIGTERM handled.\n");
  exit (EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs ();
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
  dyn_garbage_collector ();
  adjust_cluster_stats ();
}

void start_server (void) { 
  //  struct sigaction sa;
  int i;
  int prev_time;
  double next_create_outbound = 0;

  init_epoll();
  init_netbuffers();

  prev_time = 0;
  ping_interval *= 0.95 + 0.1 * drand48 ();

  if (daemonize) {
    setsid ();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  for (i = 0; i < CurConf->clusters_num; i++) {
    assert (init_listening_connection (CurConf->Clusters[i].server_socket, &ct_memcache_server, &CurConf->Clusters[i].mc_proxy_inbound) >= 0);
    CurConf->Clusters[i].listening_connection = Connections + CurConf->Clusters[i].server_socket;
  }

  get_utime_monotonic ();

  create_all_outbound_connections ();

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGUSR2, sigusr2_handler);
  signal (SIGPIPE, SIG_IGN);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);
    if (precise_now > next_create_outbound) {
      create_all_outbound_connections ();
      next_create_outbound = precise_now + 0.05 + 0.02 * drand48 ();
    }
    if (now != prev_time) {
      prev_time = now;
      cron ();
    }
    if (need_reload_config) {
#ifdef SEARCH_MODE_ENABLED
      vkprintf (0, "error: cannot reload configuration in mc-proxy-search mode\n");
      need_reload_config = 0;
#else
      do_reload_config (1);
#endif
    }
    if (quit_steps && !--quit_steps) break;
  }

}

/*
 *
 *    MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-l<log-name>] [-F] [-X <extension>] [-P <passfile>] [-y <default-min-connections] [-z <default-max-connections>] [-f] <cluster-descr-file>\n"
	  "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
	  "64-bit"
#else
	  "32-bit"
#endif
	  " after commit " COMMIT "\n"
	  "\tRedistributes memcache queries to servers listed in <cluster-descr-file>\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-F\tchange default cluster mode to distribution by user_id\n"
	  "\t-f\tload only first cluster\n"
	  "\t-X\tenable specified extension mode (allowed modes: text, lists, hints, logs, magus, watchcat, news, roundrobin, dot, search, statsx, friends, target, news_ug, news_comm, searchx, newsr)\n"
          "\t-H<heap-size>\tdefines maximum heap size\n"
	  "\t-T<ping-interval>\tsets ping interval for remote servers (default: %.1f)\n"
	  "\t-y\tdefines default minimum number of connections (default: 2)\n"
	  "\t-z\tdefines default maximum number of connections (default: 3)\n"
	  "\t-P\tpath to file with AES encryption key\n",
	  progname,
	  (double) PING_INTERVAL);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;
  long long x;
  char c;

  set_debug_handlers ();

  progname = argv[0];
  char *custom_encr = 0;
  while ((i = getopt (argc, argv, "b:c:l:p:n:dhu:vfFP:X:E:H:T:y:z:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'F':
      default_cluster_mode++;
      break;
    case 'f':
      only_first_cluster++;
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
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
        maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'y':
      i = atoi (optarg);
      if (i >= 1 && i <= 1000) {
	default_min_connections = i;
      }
      break;
    case 'z':
      i = atoi (optarg);
      if (i >= 1 && i <= 1000) {
	default_max_connections = i;
      }
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'n':
      errno = 0;
      nice (atoi (optarg));
      if (errno) {
        perror ("nice");
      }
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'P':
      aes_pwd_file = optarg;
      break;
    case 'X':
      strncpy (extension_name, optarg, 15);
      if (!strcmp (optarg, "text")) {
        default_cluster_mode |= CLUSTER_MODE_TEXT;
      } else if (!strcmp (optarg, "lists")) {
        default_cluster_mode |= CLUSTER_MODE_LISTS;
      } else if (!strcmp (optarg, "hints")) {
        default_cluster_mode |= CLUSTER_MODE_HINTS;
      } else if (!strcmp (optarg, "logs")) {
        default_cluster_mode |= CLUSTER_MODE_LOGS;
      } else if (!strcmp (optarg, "magus")) {
        default_cluster_mode |= CLUSTER_MODE_MAGUS;
      } else if (!strcmp (optarg, "news")) {
        default_cluster_mode |= CLUSTER_MODE_NEWS;
      } else if (!strcmp (optarg, "roundrobin")) {
        default_cluster_mode |= CLUSTER_MODE_ROUNDROBIN;
      } else if (!strcmp (optarg, "bayes")) {
        default_cluster_mode |= CLUSTER_MODE_BAYES;
      } else if (!strcmp (optarg, "antispam")) {
        default_cluster_mode |= CLUSTER_MODE_ANTISPAM;
      } else if (!strcmp (optarg, "temp")) {
        default_cluster_mode |= CLUSTER_MODE_TEMP;
      } else if (!strcmp (optarg, "support")) {
        default_cluster_mode |= CLUSTER_MODE_SUPPORT;
      } else if (!strcmp (optarg, "photo")) {
        default_cluster_mode |= CLUSTER_MODE_PHOTO;
      } else if (!strcmp (optarg, "dot")) {
        default_cluster_mode |= CLUSTER_MODE_DOT;
      } else if (!strcmp (optarg, "search")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_SEARCH;
      } else if (!strcmp (optarg, "targ")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_TARG;
      } else if (!strcmp (optarg, "news_ug")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_NEWS_UG;
      } else if (!strcmp (optarg, "news_g")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_NEWS_G;
      } else if (!strcmp (optarg, "news_comm")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_NEWS_COMM;
      } else if (!strcmp (optarg, "newsr")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_NEWSR;
      } else if (!strcmp (optarg, "statsx")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_STATSX;
      } else if (!strcmp (optarg, "friends")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_FRIENDS;
      } else if (!strcmp (optarg, "searchx")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_SEARCHX;
      } else if (!strcmp (optarg, "watchcat")) {
        default_cluster_mode |= CLUSTER_MODE_WATCHCAT;
      } else if (!strcmp (optarg, "hints_merge")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_HINTS_MERGE;
      } else if (!strcmp (optarg, "random_merge")) {
        if (default_cluster_mode & CLUSTER_MODE_MERGE) {
          fprintf (stderr, "Can not enable to merge extensions");
          return 2;
        }
        default_cluster_mode |= CLUSTER_MODE_RANDOM_MERGE;
      } else {
        usage ();
        return 2;
      }
      break;
    case 'T':
      ping_interval = atof (optarg);
      if (ping_interval < MIN_PING_INTERVAL || ping_interval > MAX_PING_INTERVAL) {
	usage ();
	return 2;
      }
      break;
    case 'E':
      custom_encr = optarg;
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'H':
      c = 0;
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
      }
      if (x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (100LL << 30))) {
	dynamic_data_buffer_size = x;
      }
      break;
    }
  }
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  if (default_max_connections < default_min_connections) {
    kprintf ("WARNING: adjusted default maximum number of connections to be equal to default minimum number of connections\n");
    default_max_connections = default_min_connections;
  }

  init_dyn_data ();

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  config_filename = argv[optind];

  i = do_reload_config (0);

  if (i < 0) {
    fprintf (stderr, "config check failed!\n");
    return -i;
  }

  vkprintf (1, "config loaded!\n");

  if (custom_encr) {
    aes_load_pwd_file (custom_encr);
  } else {
    aes_load_pwd_file (aes_pwd_file);
  }

  start_time = time (0);

  start_server ();

  return 0;
}

