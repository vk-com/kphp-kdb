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
              2011-2013 Nikolai Durov
              2011-2013 Andrei Lopatin
                   2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64
#define _XOPEN_SOURCE 500
#define _BSD_SOURCE

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <aio.h>
#include <sys/mman.h>

#include "md5.h"
#include "crc32.h"
#include "kdb-data-common.h"
#include "kfs.h"
#include "resolver.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "net-crypto-aes.h"
#include "common-data.h"
#include "monitor/monitor-common.h"
#include "rpc-const.h"

#include "replicator-protocol.h"

#define VERSION_STR	"replicator-0.2-r14"

#define TCP_PORT 2369

#define MAX_NET_RES	(1L << 16)

#define HASH_FILE_BYTES	(1 << 17)

#define	SEND_BUFFER_SIZE	(1L << 22)
#define	DIRTY_THRESHOLD_LOW	(1L << 20)
#define	DIRTY_THRESHOLD_HIGH	(1L << 21)
#define MAX_UPDATE_SIZE		(1L << 16)
#define MAX_NET_OUT_QUEUE_BYTES	(1L << 20)

#define MAX_CLIENT_UNFLUSHED_BYTES	(1L << 25)
#define MAX_FLUSH_INTERVAL		5.0

#define	MIN_TIME_UPDATE_INTERVAL	((1LL << 32) * 5 / 1000)	// 5 ms
#define	MAX_PENDING_BYTES_THRESHOLD	(1 << 16)
#define	NOTSENT_SPAM_INTERVAL	10

#define MAX_LOCAL_RBS 4096


#define PING_INTERVAL 1.0
#define STOP_INTERVAL (2 * ping_interval)
#define FAIL_INTERVAL (20 * ping_interval)
#define RESPONSE_FAIL_TIMEOUT 5
#define CONNECT_TIMEOUT 3
double ping_interval = PING_INTERVAL;

static int double_send_recv = 0;

/*
 *
 *		REPLICATOR
 *
 */

struct worker_stats {
  int cnt;
  int updated_at;
  int conf_master_binlogs, conf_slave_binlogs, broken_master_binlogs, broken_slave_binlogs;
  int active_repl_servers, active_repl_clients, servers_sentall;
  int slave_cnt[4];
  long long errors_sent, errors_received;
  long long data_packets_sent, data_packets_received;
  long long rotate_packets_sent, rotate_packets_received, unknown_packets_received;
  long long data_bytes_sent, data_bytes_received;
  long long tot_read_bytes, tot_unflushed_bytes;
  long long posinfo_packets_sent, posinfo_packets_not_sent, posinfo_packets_received;
};

struct worker_stats *WStats, SumStats;
int worker_id;

int verbosity, quit_steps, start_time, enable_ipv6;
int check_all_locations;
static int monitor_priority = 1;

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;


char *fnames[3];
int fd[3];
long long fsize[3];

char *progname = "replicator", *username, *logname, *hostname;
char hostname_buffer[256];
int hostname_len;
char *aes_pwd_file = 0;

#define STATS_BUFF_SIZE	(1 << 20)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];

static volatile int sigrtmax_cnt = 0;

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
 *	CONFIGURATION PARSER
 *
 */

#define	MAX_CLUSTER_SERVERS	16384

#define	IN_CLUSTER_CONFIG	1
#define	IN_CLUSTER_CONNECTED	2

struct rpc_client_functions replicator_rpc_client;

struct conn_target default_ct = {
.min_connections = 1,
.max_connections = 1,
.type = &ct_rpc_client,
.extra = &replicator_rpc_client,
.reconnect_timeout = 1,
.port = TCP_PORT,
};

int default_target_port = TCP_PORT;


#define	MAX_CONFIG_SIZE	(1 << 22)

struct location_list_entry {
  struct location_list_entry *next;
  char *location;
  int location_len, flags;
};

struct remote_binlog {
  struct location_list_entry *first;
  char *binlog_tag;
  int binlog_tag_len;
  int flags;
};

#define LRF_MASTER_DESCR	1
#define LRF_LOCAL	2
#define LRF_MASTER	4
#define LRF_BROKEN	8
#define LRF_LOCK_BINLOG_FAIL	16
#define LRF_TARGET_CR_FAIL	32
#define LRF_READONLY_MODE	64
#define LRF_CHANGED_TIME	128

struct related_binlog_status {
  int cnt;
  int updated_at;
  int flags;
  int status;
};

struct related_binlog {
  struct remote_binlog *binlog;
  struct repl_server_status *server_first, *server_last;
  struct location_list_entry *local;
  struct location_list_entry *master;
  int targets;
  struct conn_target *target[2];
  struct repl_client_status *client[2];
  struct related_binlog_status *status;
  char *path;
  kfs_replica_handle_t kfs_replica;
  kfs_file_handle_t kfs_binlog;
  long long min_binlog_pos, slice_binlog_pos, max_binlog_pos;
  md5_hash_t first_file_start_hash;
  long long replica_name_hash;
  long long engine_log_pos, engine_time;
  int first_file_start_size;
  int flags;
  int update_generation;
};

char config_buff[MAX_CONFIG_SIZE+4], *config_filename, *cfg_start, *cfg_end, *cfg_cur;
int config_bytes, cfg_lno, cfg_lex = -1;

struct related_binlog LR[MAX_LOCAL_RBS];
int local_rbs, local_masters, orig_local_rbs;

int pids[MAX_LOCAL_RBS];
int slave_mode, workers, parent_pid;

struct related_binlog_status *RBStats;

static void update_rb_stats (struct related_binlog *R);

#define HASH_PRIME 170239

struct remote_binlog *rb_hash[HASH_PRIME];

struct remote_binlog *get_rb_hash (char *name, int l, int force) {
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
  while (rb_hash[h1]) {
    if (l == rb_hash[h1]->binlog_tag_len &&
        !memcmp (rb_hash[h1]->binlog_tag, name, l)) {
      return rb_hash[h1];
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
    rb_hash[h1] = zmalloc (sizeof (struct remote_binlog));
    rb_hash[h1]->binlog_tag = temp;
    rb_hash[h1]->binlog_tag_len = l;
    rb_hash[h1]->first = 0;
    rb_hash[h1]->flags = 0;
    return rb_hash[h1];
  }
  return 0;
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

static int cfg_skonlyspc (void) {
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

static int cfg_getword (int allow_newline) {
  if (allow_newline) {
    cfg_skspc();
  } else {
    cfg_skonlyspc();
  }
  char *s = cfg_cur;
  while ((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') || *s == '.' || *s == '-' || *s == '_' || *s == ':' || *s == '/' || *s == '[' || *s == ']' || *s == '@') {
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
/*
static int expect (int lexem) {
  if (cfg_lex != lexem) {
    static char buff[32];
    sprintf (buff, "%c expected", lexem);
    return syntax (buff);
  }
  return 0;
}*/

int check_location (struct remote_binlog *B) {
  assert (B);

  int l_cnt = 0, m_cnt = 0;
  struct location_list_entry *Q;
  for (Q = B->first; Q; Q = Q->next) {
    if (Q->flags & 2) {
      l_cnt++;
    }
    if (Q->flags & 1) {
      m_cnt++;
    }
  }

  if (l_cnt > 1) {
    fprintf (stderr, "error: binlog %s has %d local locations:", B->binlog_tag, l_cnt);
    for (Q = B->first; Q; Q = Q->next) {
      if (Q->flags & 2) {
        fprintf (stderr, " %s", Q->location);
      }
    }
    fprintf (stderr, "\n");
    return 1;
  }

  if (!m_cnt) {
    fprintf (stderr, "error: binlog %s has no master location\n", B->binlog_tag);
    return 1;
  }

  if (m_cnt > 1) {
    fprintf (stderr, "error: binlog %s has %d master locations:", B->binlog_tag, m_cnt);
    for (Q = B->first; Q; Q = Q->next) {
      if (Q->flags & 1) {
        fprintf (stderr, " %s", Q->location);
      }
    }
    fprintf (stderr, "\n");
    return 1;
  }

  return 0;
}

struct location_list_entry *get_master (struct remote_binlog *B) {
  assert (B);

  struct location_list_entry *Q;
  for (Q = B->first; Q; Q = Q->next) {
    if (Q->flags & 1) {
      return Q;
    }
  }
  return 0;
}

#define Expect(l) { int t = expect(l); if (t < 0) { return t; } }


int parse_config (int flags) {
  int r, l, i;
  //struct hostent *h;
  struct remote_binlog *B;

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
  cfg_lno = 1;

  while (1) {
    cfg_skipspc ();

    l = cfg_getword (1);
    if (!l) {
      break;
    }
    B = get_rb_hash (cfg_cur, l, 1);

    cfg_cur += l;

    while (*cfg_cur == 32 || *cfg_cur == 9) {
      cfg_skonlyspc();
      int flags = 1;

      if (*cfg_cur == '#') {
        break;
      }
      if (*cfg_cur == '*') {
        flags = 0;
        cfg_cur++;
      }
      l = cfg_getword (0);
      if (!l) {
        if (!flags) {
          return syntax ("'*' must be followed with target description");
        }
        break;
      }

      struct location_list_entry *Q = B->first;
      while (Q) {
        if (l == Q->location_len && !memcmp (Q->location, cfg_cur, l)) {
          Q->flags |= flags;
          break;
        }
        Q = Q->next;
      }

      if (!Q) {
        char *s = zmalloc (l + 1);
        memcpy (s, cfg_cur, l);
        s[l] = 0;
        if (!memchr (s, ':', l)) {
          fprintf (stderr, "error at line %d: location %s for binlog %s is not of <hostname>:<path> format\n", cfg_lno, s, B->binlog_tag);
          return -1;
        }
        Q = zmalloc (sizeof (struct location_list_entry));
        Q->next = B->first;
        Q->location = s;
        Q->location_len = l;
        B->first = Q;
        if (l > hostname_len && !memcmp (s, hostname, hostname_len) && s[hostname_len] == ':') {
          flags |= 2;
          if (!(B->flags & 2)) {
            B->flags |= 2;
            /* add here B to local interest group */
            assert (local_rbs < MAX_LOCAL_RBS);
            for (i = 0; i < local_rbs; i++) {
              if (LR[i].local->location_len == l && !memcmp (LR[i].local->location, s, l)) {
                fprintf (stderr, "error: location %s suggested for two different binlogs %s and %s\n", s, B->binlog_tag, LR[i].binlog->binlog_tag);
                return -1;
              }
            }
            LR[local_rbs].local = Q;
            LR[local_rbs].server_first = LR[local_rbs].server_last = (struct repl_server_status *) &LR[local_rbs];
            LR[local_rbs++].binlog = B;
            if (verbosity > 0) {
              fprintf (stderr, "found related binlog: %s at %s\n", B->binlog_tag, Q->location);
            }
          }
        }
        Q->flags = flags;
      }
      cfg_cur += l;
    }

    cfg_skonlyspc();
    if (*cfg_cur != 13 && *cfg_cur != 10 && *cfg_cur != '#') {
      break;
    }

  }

  cfg_getlex ();
  if (cfg_lex) {
    return syntax ("EOF expected");
  }

  if (verbosity > 0) {
    fprintf (stderr, "%d related binlogs found\n", local_rbs);
  }

  if (check_all_locations) {
    int h, loc_errors = 0;
    for (h = 0; h < HASH_PRIME; h++) {
      if (rb_hash[h]) {
        loc_errors += check_location (rb_hash[h]);
      }
    }
    if (loc_errors) {
      fprintf (stderr, "%d wrong remote binlog descriptions found in configuration file\n", loc_errors);
      return -1;
    }
  }

  int loc_errors = 0;
  for (i = 0; i < local_rbs; i++) {
    loc_errors += check_location (LR[i].binlog);
    LR[i].master = get_master (LR[i].binlog);
    if (LR[i].master == LR[i].local) {
      LR[i].binlog->flags |= 4;
      local_masters++;
    }
    LR[i].flags = LR[i].binlog->flags;
  }

  if (loc_errors) {
    fprintf (stderr, "%d wrong related binlog descriptions found\n", loc_errors);
    return -1;
  }

  if (verbosity > 0) {
    fprintf (stderr, "found %d related binlog descriptions, we (%s) hold master copy for %d of them\n", local_rbs, hostname, local_masters);
    for (i = 0; i < local_rbs; i++) {
      B = LR[i].binlog;
      if (B->flags & 4) {
        fprintf (stderr, "binlog #%d: %s, master at %s\n", i, B->binlog_tag, LR[i].local->location);
      } else {
        fprintf (stderr, "binlog #%d: %s, slave at %s, master is at %s\n", i, B->binlog_tag, LR[i].local->location, LR[i].master->location);
      }
    }
  }

  return 0;
}

/*
 *
 *	REPLICATOR KFS/BINLOG FILE IO
 *
 */

int conf_master_binlogs, conf_slave_binlogs, broken_master_binlogs, broken_slave_binlogs;

int valid_binlog_path (char *path) {
  int i = 0;
  char *ptr = path;
  if (!*path || *path == '/' || *path == '.') {
    return 0;
  }
  while (*ptr) {
    if (!(*ptr == '/' || *ptr == '.' || *ptr == '-' || *ptr == '_' || (*ptr >= '0' && *ptr <= '9') || (*ptr >= 'a' && *ptr <= 'z') || (*ptr >= 'A' && *ptr <= 'Z'))) {
      return 0;
    }
    if (*ptr == '.' && (ptr[-1] == '.' || ptr[-1] == '/' || ptr[-1] == '_' || ptr[-1] == '-')) {
      return 0;
    }
    ptr++;
    i++;
  }
  if (i >= 64 || ptr[-1] == '.' || ptr[-1] == '/') {
    return 0;
  }
  return 1;
}

static void convert_md5_to_hex (char *output, md5_hash_t hash) {
  static char hcyf[16] = "0123456789abcdef";
  int i;
  for (i = 0; i < 16; i++) {
    output[2*i] = hcyf[(hash[i] >> 4) & 15];
    output[2*i+1] = hcyf[hash[i] & 15];
  }
  output[32] = 0;
}

static long long get_orig_file_size (struct kfs_file_info *FI) {
  if (FI->flags & 16) {
    kfs_binlog_zip_header_t *H = load_binlog_zip_header (FI);
    if (!H) {
      return -1;
    }
    return H->orig_file_size;
  }
  return FI->file_size;
}

int load_binlog_data (struct related_binlog *R) {
  char *colon = memchr (R->binlog->binlog_tag, ':', R->binlog->binlog_tag_len), *at = NULL;
  if (colon == NULL) {
    at = memchr (R->binlog->binlog_tag, '@', R->binlog->binlog_tag_len);
  }
  static unsigned char md5buf[16];
  md5 ((unsigned char *)R->kfs_replica->replica_prefix, R->kfs_replica->replica_prefix_len, md5buf);
  R->replica_name_hash = *(long long *)md5buf;
  if (colon && colon[1] == '-' && !colon[2]) {
    colon = 0;
  }
  if (!R->kfs_replica->binlog_num) {
    assert (!R->max_binlog_pos);
    R->min_binlog_pos = 0;
    R->max_binlog_pos = 0;
    R->slice_binlog_pos = 0;
    R->first_file_start_size = -1;
    return (R->flags & 4) ? -1 : 0;
  }
  long long old_max_binlog_pos = R->max_binlog_pos;
  R->min_binlog_pos = get_binlog_start_pos (R->kfs_replica, 0, 0);
  assert (R->min_binlog_pos >= 0);
  R->slice_binlog_pos = get_binlog_start_pos (R->kfs_replica, R->kfs_replica->binlog_num - 1, &R->max_binlog_pos);

  struct kfs_file_info *FJ = R->kfs_replica->binlogs[R->kfs_replica->binlog_num - 1];
  if (FJ->flags & 16) {
    /* assume that zipped binlog is always less than original */
    R->max_binlog_pos = R->slice_binlog_pos + FJ->file_size;
  }

  assert (R->slice_binlog_pos >= R->min_binlog_pos && R->max_binlog_pos >= R->slice_binlog_pos);
  assert (R->max_binlog_pos >= old_max_binlog_pos);
  if (verbosity > 0) {
    fprintf (stderr, "binlog %s : have %d binlog files, corresponding to absolute binlog positions %lld..%lld\n", R->path, R->kfs_replica->binlog_num, R->min_binlog_pos, R->max_binlog_pos);
  }
  struct kfs_file_info *FI = R->kfs_replica->binlogs[0];
  const long long orig_file_size = get_orig_file_size (FI);
  if (colon && !R->min_binlog_pos && orig_file_size < (1L << 20) && ((R->flags & 4) || R->kfs_replica->binlog_num > 1)) {
    fprintf (stderr, "binlog %s : first file %s must be at least mebibyte long with hash %s, but this is not so\n", R->path, FI->filename, colon + 1);
    return -1;
  }

  if (!R->min_binlog_pos) {
    char hash[33];
    const int start_size = orig_file_size < (1 << 20) ? orig_file_size : (1 << 20);
    if (start_size != R->first_file_start_size) {
      assert (start_size > R->first_file_start_size);
      if (at) {
        const int fd = preload_file_info (FI);
        if (fd == -2) {
          fprintf (stderr, "%s: preload_file_info for file '%s' failed.\n", __func__, FI->filename);
          return -1;
        }
        if (fd >= 0) {
          assert (!close (fd));
        }
        assert (FI->start);
        if (FI->flags & 16) {
          kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) FI->start;
          if (!(H->format & KFS_BINLOG_ZIP_FORMAT_FLAG_HAS_TAG)) {
            fprintf (stderr, "%s: '%s' zipped header didn't contain tag (24th bit of format field is unset).\n", __func__, FI->filename);
            return -1;
          }
          memcpy (R->first_file_start_hash, H->first_128k_md5, 16);
        } else {
          if (kfs_get_tag ((unsigned char *) FI->start, FI->preloaded_bytes, R->first_file_start_hash) < 0) {
            fprintf (stderr, "%s: '%s' didn't contain LEV_TAG\n", __func__, FI->filename);
            return -1;
          }
        }
        R->first_file_start_size = start_size;
        convert_md5_to_hex (hash, R->first_file_start_hash);
        if (strcmp (at + 1, hash)) {
          fprintf (stderr, "tag mismatch for %s: %s expected, %s found\n", FI->filename, at + 1, hash);
          return -1;
        }
      } else {
        if (FI->flags & 16) {
          assert (FI->start);
          memcpy (R->first_file_start_hash, ((kfs_binlog_zip_header_t *) FI->start)->first_1m_md5, sizeof (md5_hash_t));
        } else {
          int fd = open (FI->filename, O_RDONLY);
          if (fd < 0) {
            fprintf (stderr, "cannot open file %s: %m\n", FI->filename);
            return -1;
          }
          int r = read (fd, stats_buff, start_size);
          if (r != start_size) {
            fprintf (stderr, "error reading first mebibyte of %s: only %d bytes read: %m\n", FI->filename, r);
            close (fd);
            return -1;
          }
          close (fd);
          if (R->first_file_start_size > 0) {
            md5_hash_t old_prefix_hash;
            md5 ((unsigned char *) stats_buff, R->first_file_start_size, old_prefix_hash);
            if (memcmp (R->first_file_start_hash, old_prefix_hash, sizeof (md5_hash_t))) {
              fprintf (stderr, "fatal: first binlog slice %s prefix hash changed (old prefix length %d)\n", FI->filename, R->first_file_start_size);
              exit (1);
            }
          }
          md5 ((unsigned char *) stats_buff, start_size, R->first_file_start_hash);
        }
        R->first_file_start_size = start_size;
        if (orig_file_size >= (1L << 20) && colon) {
          convert_md5_to_hex (hash, R->first_file_start_hash);
          if (strcmp (colon + 1, hash)) {
            fprintf (stderr, "md5 hash mismatch for %s: %s expected, %s found\n", FI->filename, colon + 1, hash);
            return -1;
          }
        }
      }
    }
  } else {
    memset (R->first_file_start_hash, 0, sizeof (md5_hash_t));
    R->first_file_start_size = -1;
  }

  return 0;
}

int check_binlog (struct related_binlog *R) {
  assert (R && R->local && R->local->location_len > hostname_len && !memcmp (R->local->location, hostname, hostname_len) && R->local->location[hostname_len] == ':');
  char *path = R->path = R->local->location + hostname_len + 1;
  R->flags = R->binlog->flags;
  if (!valid_binlog_path (path)) {
    fprintf (stderr, "local binlog path %s for binlog %s is invalid, ignoring entry\n", path, R->binlog->binlog_tag);
    return -1;
  }
  if (workers) {
    return -1;
  }
  R->kfs_replica = open_replica (path, R->flags & 4 ? 0 : 1);
  R->update_generation = sigrtmax_cnt;
  if (!R->kfs_replica) {
    fprintf (stderr, "error: cannot open kfs replica %s for binlog tag %s, ignoring entry\n", path, R->binlog->binlog_tag);
    return -1;
  }
  R->first_file_start_size = -1;
  R->max_binlog_pos = 0;
  return load_binlog_data (R);
}

int lock_last_binlog (struct related_binlog *R) {
  R->kfs_binlog = open_binlog (R->kfs_replica, R->max_binlog_pos);
  if (!R->kfs_binlog) {
    return R->kfs_replica->binlog_num ? -1 : 0;
  }
  if (!binlog_is_last (R->kfs_binlog)) {
    kprintf ("%s: binlog '%s' isn't last.\n", __func__, R->kfs_binlog->info->filename);
    return -1;
  }

  if (R->kfs_binlog->info->flags & 16) {
    struct stat st;
    if (!stat (R->kfs_binlog->info->filename, &st) && !(st.st_mode & 0222)) {
      /* it is possible that somebody deleted slices after current */
      /* in this case we need temporarly restore write permissions */
      if (chmod (R->kfs_binlog->info->filename, 0640) < 0) {
        kprintf ("%s: fail to set write mode for the file '%s'. %m\n", __func__, R->kfs_binlog->info->filename);
      }
    }
  }

  long long end_pos = append_to_binlog_ext (R->kfs_binlog, 1);
  if (end_pos == -1) {
    kprintf ("%s: append_to_binlog_ext for binlog '%s' failed.\n", __func__, R->kfs_binlog->info->filename);
    return -1;
  }
  if (end_pos != R->max_binlog_pos) {
    fprintf (stderr, "error: binlog %s: last binlog position suddenly changed from %lld to %lld, somebody is still writing?\n", R->kfs_binlog->info->filename, R->max_binlog_pos, end_pos);
    return -1;
  }
  return 0;
}

#ifndef h_addr
#define h_addr h_addr_list[0]
#endif

int create_slave_target (struct related_binlog *R) {
  char *s = R->master->location, *t = strrchr (s, ':');
  struct hostent *h;
  assert (t && t >= s && t <= s + 63);
  char remote_hostname[64];
  memcpy (remote_hostname, s, t - s);
  remote_hostname[t - s] = 0;
  if (!(h = kdb_gethostbyname (remote_hostname)) || !h->h_addr_list || !h->h_addr) {
    fprintf (stderr, "cannot resolve %s\n", remote_hostname);
    exit (2);
  }
  switch (h->h_addrtype) {
  case AF_INET:
    assert (h->h_length == 4);
    default_ct.target = *((struct in_addr *) h->h_addr);
    memset (default_ct.target_ipv6, 0, 16);
    break;
  case AF_INET6:
    assert (h->h_length == 16);
    default_ct.target.s_addr = 0;
    memcpy (default_ct.target_ipv6, h->h_addr, 16);
    break;
  default:
    fprintf (stderr, "cannot resolve %s: bad address type %d\n", remote_hostname, h->h_addrtype);
    exit (2);
  }

  if (verbosity > 0) {
    fprintf (stderr, "creating target %s:%d (ip %s)\n", remote_hostname, default_ct.port, default_ct.target.s_addr ? inet_ntoa (default_ct.target) : show_ipv6 (default_ct.target_ipv6));
  }

  assert (!(R->flags & LRF_BROKEN));
  R->targets = double_send_recv ? 2 : 1;
  int i;
  default_ct.min_connections = default_ct.max_connections = R->targets;
  for (i = 0; i < R->targets; i++) {
    R->target[i] = create_target (&default_ct, 0);
    if (!R->target[i]) {
      R->targets = 0;
      return -1;
    }
  }
  return 0;
}

int check_binlog_files (void) {
  int bad = 0, i;
  for (i = 0; i < local_rbs; i++) {
    if (check_binlog (&LR[i]) < 0) {
      LR[i].flags |= LRF_BROKEN;  // BROKEN
      bad++;
    } else if (!(LR[i].flags & LRF_MASTER) && binlog_disabled) {
      LR[i].flags |= LRF_BROKEN | LRF_READONLY_MODE;
      bad++;
    } else if (!(LR[i].flags & LRF_MASTER) && lock_last_binlog (&LR[i]) < 0) {
      LR[i].flags |= LRF_BROKEN | LRF_LOCK_BINLOG_FAIL;
      bad++;
    } else if (!(LR[i].flags & LRF_MASTER) && create_slave_target (&LR[i]) < 0) {
      LR[i].flags |= LRF_BROKEN | LRF_TARGET_CR_FAIL;
      bad++;
    }
    if (LR[i].flags & LRF_MASTER) {
      conf_master_binlogs++;
      if (LR[i].flags & LRF_BROKEN) {
        broken_master_binlogs++;
      }
    } else if (!workers) {
      conf_slave_binlogs++;
      if (LR[i].flags & LRF_BROKEN) {
        broken_slave_binlogs++;
      }
    }
  }
  return local_rbs - bad;
}

int update_related_binlog (struct related_binlog *R) {
  vkprintf (1, "reloading kfs data for %s\n", R->path);
  R->update_generation = sigrtmax_cnt;
  assert (update_replica (R->kfs_replica, 0) >= 0);
  return load_binlog_data (R);
}

/*
 *
 *	GENERIC REPLICATOR CLIENT/SERVER FUNCTIONS
 *
 */

#define RC_UPTODATE	32

/* identified by handshake_id */
struct repl_client_status {
  struct repl_client_status *h_next;
  struct related_binlog *rb;
  struct connection *conn;
  int conn_generation;
  int flags;
  int handshake_id;
  int session_id;
  long long unflushed_bytes;
  double last_flush_time;
};

#define	RS_REPL_STARTED	1
#define	RS_LOADDISK	2
#define RS_LOGROTATE	4
#define	RS_ZEROSTART	8
#define	RS_ALLREAD	16
#define	RS_ALLSENT	32
#define RS_ZIPPED 64
#define RS_FATAL	128

/* identified by session_id */
struct repl_server_status {
  struct repl_server_status *h_next;
  struct repl_server_status *server_next, *server_prev;
  struct related_binlog *rb;
  struct connection *conn;
  int conn_generation;
  int flags;
  int handshake_id;
  int session_id;
  kfs_file_handle_t binlog;
  //long long client_file_size;
  long long client_log_pos;
  long long client_log_wrpos;		// already on client's disk
  long long client_log_recvpos;		// acknowledged by client ; client_log_pos >= client_log_recvpos >= client_log_wrpos
  long long buffer_file_pos;		// always corresponding to kfs_file_handle_t binlog
  long long logrotate_pos;		// log position at which we initiate a log_rotate event
  int buffer_size;
  char *buffer, *rptr, *wptr;
};

int process_server (struct repl_server_status *S);

int active_repl_servers, active_repl_clients;
int servers_readall, servers_sentall;

#define	PARTNER_HASH_SIZE	17239

struct repl_server_status *ServerHash[PARTNER_HASH_SIZE];
struct repl_client_status *ClientHash[PARTNER_HASH_SIZE];

struct repl_server_status *get_server_by_session (int session_id, int force) {
  int h = session_id % PARTNER_HASH_SIZE;
  if (h < 0) {
    h += PARTNER_HASH_SIZE;
  }
  struct repl_server_status **p = &ServerHash[h], *q = *p;
  while (q) {
    if (q->session_id == session_id) {
      if (force < 0) {
        *p = q->h_next;
        q->h_next = 0;
        zfree (q, sizeof (struct repl_server_status));
        return 0;
      }
      return q;
    }
    p = &q->h_next;
    q = *p;
  }
  assert (force >= 0);
  if (!force) {
    return 0;
  }
  q = zmalloc0 (sizeof (struct repl_server_status));
  q->h_next = ServerHash[h];
  q->session_id = session_id;
  ServerHash[h] = q;
  active_repl_servers++;
  return q;
};

struct repl_client_status *get_client_by_handshake (int handshake_id, int force) {
  int h = handshake_id % PARTNER_HASH_SIZE;
  if (h < 0) {
    h += PARTNER_HASH_SIZE;
  }
  struct repl_client_status **p = &ClientHash[h], *q = *p;
  while (q) {
    if (q->handshake_id == handshake_id) {
      if (force < 0) {
        *p = q->h_next;
        q->h_next = 0;
        zfree (q, sizeof (struct repl_client_status));
        return 0;
      }
      return q;
    }
    p = &q->h_next;
    q = *p;
  }
  assert (force >= 0);
  if (!force) {
    return 0;
  }
  q = zmalloc0 (sizeof (struct repl_client_status));
  q->h_next = ClientHash[h];
  q->handshake_id = handshake_id;
  ClientHash[h] = q;
  active_repl_clients++;
  return q;
};

int compute_start_end_hashes (int fd, long long file_size, md5_hash_t start_hash, md5_hash_t end_hash) {
  assert (fd > 0 && file_size >= 0);
  if (file_size <= HASH_FILE_BYTES) {
    assert (pread (fd, stats_buff, file_size, 0) == file_size);
    md5 ((unsigned char *) stats_buff, file_size, start_hash);
    memcpy (end_hash, start_hash, sizeof (md5_hash_t));
  } else {
    assert (pread (fd, stats_buff, HASH_FILE_BYTES, 0) == HASH_FILE_BYTES);
    md5 ((unsigned char *) stats_buff, HASH_FILE_BYTES, start_hash);
    assert (pread (fd, stats_buff, HASH_FILE_BYTES, file_size - HASH_FILE_BYTES) == HASH_FILE_BYTES);
    md5 ((unsigned char *) stats_buff, HASH_FILE_BYTES, end_hash);
  }
  return 0;
}

int last_handshake_id, last_session_id;
long long errors_sent, errors_received, data_packets_sent, data_packets_received;
long long rotate_packets_sent, rotate_packets_received, unknown_packets_received;
long long data_bytes_sent, data_bytes_received;
long long posinfo_packets_sent, posinfo_packets_not_sent, posinfo_packets_received;

long long tot_unflushed_bytes, tot_read_bytes;

#define MAX_PACKET_INTS	8192
#define	MAX_PACKET_PAYLOAD	((MAX_PACKET_INTS - 3) * 4)
static int send_packet[MAX_PACKET_INTS];
static int recv_packet[MAX_PACKET_INTS];

void *alloc_packet (int packet_bytes) {
  assert ((unsigned) packet_bytes <= MAX_PACKET_PAYLOAD);
  return send_packet + 2;
}

void push_packet (struct connection *c, int packet_bytes) {
  assert ((unsigned) packet_bytes <= MAX_PACKET_PAYLOAD);
  int pad_bytes = -packet_bytes & 3;
  char *ptr = (char *) send_packet + 8 + packet_bytes;
  memset (ptr, 0, pad_bytes);
  int plen = (packet_bytes + 12 + 3) >> 2;
  send_packet[0] = plen * 4;
  send_packet[1] = RPCC_DATA(c)->out_packet_num++;
  send_packet[plen - 1] = compute_crc32 (send_packet, plen * 4 - 4);
  assert (write_out (&c->Out, send_packet, plen * 4) == plen * 4);
  flush_later (c);
  // RPCC_FUNC(c)->flush_packet(c);
}

static void push_data_ack_packet (struct connection *c, int handshake_id, int session_id, long long pos) {
  struct repl_data_ack *A = alloc_packet (sizeof (struct repl_data_ack));
  A->type = P_REPL_DATA_ACK;
  A->handshake_id = handshake_id;
  A->session_id = session_id;
  A->binlog_written_pos = A->binlog_received_pos = pos;
  push_packet (c, sizeof (struct repl_data_ack));
}

int init_client_session (struct related_binlog *R, struct connection *c, int target_no) {
  assert (!R->client[target_no]);
  int handshake_id = (++last_handshake_id & 0x3fffffff) + 1;
  assert (!get_client_by_handshake (handshake_id, 0));
  struct repl_client_status *C = R->client[target_no] = get_client_by_handshake (handshake_id, 1);
  C->rb = R;
  C->conn = c;
  C->conn_generation = c->generation;
  if (verbosity > 0) {
    fprintf (stderr, "sending handshake to client %p, handshake_id=%d\n", C, handshake_id);
  }
  assert ((R->kfs_binlog && R->kfs_replica->binlog_num) || (!R->kfs_binlog && !R->kfs_replica->binlog_num));
  int binlog_tag_len = R->binlog->binlog_tag_len;
  char *binlog_slice_name = R->kfs_binlog ? R->kfs_binlog->info->filename : "";
  int binlog_slice_name_len = 0;
  if (binlog_slice_name) {
    char *ptr = strrchr (binlog_slice_name, '/');
    if (ptr) {
      binlog_slice_name = ptr + 1;
    }
    binlog_slice_name_len = strlen (binlog_slice_name);
  }
  if (!R->min_binlog_pos && R->kfs_replica->binlog_num) {
    assert (load_binlog_data (R) >= 0);
  }
  struct kfs_file_info *FI = R->kfs_binlog ? R->kfs_binlog->info : 0;
  int rec_size = sizeof (struct repl_handshake) + binlog_tag_len + binlog_slice_name_len + 2;

  struct repl_handshake *HS = alloc_packet (rec_size);
  HS->type = P_REPL_HANDSHAKE;
  HS->handshake_id = handshake_id;
  HS->flags = 0;
  HS->binlog_slice_start_pos = R->slice_binlog_pos;
  HS->binlog_slice_end_pos = R->max_binlog_pos;
  HS->binlog_file_size = FI ? FI->file_size : -1;
  HS->binlog_first_file_size = !R->min_binlog_pos && R->kfs_replica->binlog_num ? get_orig_file_size (R->kfs_replica->binlogs[0]) : -1;
  int start_bytes = HS->binlog_first_file_size < (1L << 20) ? HS->binlog_first_file_size : (1L << 20);
  assert (start_bytes == R->first_file_start_size);
  memcpy (HS->binlog_first_file_start_hash, &R->first_file_start_hash, sizeof (md5_hash_t));
  if (!FI) {
    memset (HS->binlog_file_start_hash, 0, sizeof (md5_hash_t));
    memset (HS->binlog_file_end_hash, 0, sizeof (md5_hash_t));
  } else {
    compute_start_end_hashes (R->kfs_binlog->fd, FI->file_size, HS->binlog_file_start_hash, HS->binlog_file_end_hash);
  }
  HS->binlog_tag_len = binlog_tag_len;
  HS->binlog_slice_name_len = binlog_slice_name_len;
  memcpy (HS->binlog_tag, R->binlog->binlog_tag, binlog_tag_len);
  HS->binlog_tag[binlog_tag_len] = 0;
  memcpy (HS->binlog_tag + binlog_tag_len + 1, binlog_slice_name, binlog_slice_name_len);
  HS->binlog_tag[binlog_tag_len + 1 + binlog_slice_name_len] = 0;

  vkprintf (2, "sending handshake packet to %s:%d, handshake_id=%d, for binlog tag %s, slice name %s (size %lld, pos %lld..%lld)\n", show_remote_ip (c), c->remote_port, HS->handshake_id, HS->binlog_tag, binlog_slice_name, HS->binlog_file_size, HS->binlog_slice_start_pos, HS->binlog_slice_end_pos);

  push_packet (c, rec_size);

  return 0;
}

int send_error (struct connection *c, int handshake_id, int session_id, int code, const char *str, ...) __attribute__ ((format (printf, 5, 6)));
int send_error (struct connection *c, int handshake_id, int session_id, int code, const char *str, ...) {
  va_list tmp;
  va_start (tmp, str);
  int msg_len = vsnprintf (stats_buff, STATS_BUFF_SIZE - 16, str, tmp);
  assert (msg_len < STATS_BUFF_SIZE);
  vkprintf (0, "sending error %d (session %d:%d) to connection %d (%s:%d): %s\n", code, handshake_id, session_id, c->fd, show_remote_ip (c), c->remote_port, stats_buff);
  int rec_len = sizeof (struct repl_error) + msg_len + 1;
  struct repl_error *RE = alloc_packet (rec_len);
  RE->type = P_REPL_ERROR;
  RE->handshake_id = handshake_id;
  RE->session_id = session_id;
  RE->error = code;
  memcpy (RE->error_message, stats_buff, msg_len + 1);
  push_packet (c, rec_len);
  errors_sent++;
  return code;
}

int zipped_filename (const char *filename) {
  const int l = strlen (filename);
  if (l > 7 && !strcmp (filename + l - 7, ".bin.bz")) {
    return 1;
  }
  if (l > 4 && !strcmp (filename + l - 4, ".bin")) {
    return 0;
  }
  return -1;
}

const char *msg_unlink_slice = "server has zipped slice, but client has original slice ";

int process_handshake_packet (struct connection *c, struct repl_handshake *HS, int len) {
  int S_flags;
  assert (HS->type == P_REPL_HANDSHAKE && len >= sizeof (struct repl_handshake));
  if ((unsigned) HS->binlog_tag_len >= 1024 || (unsigned) HS->binlog_slice_name_len >= 1024) {
    return send_error (c, HS->handshake_id, 0, R_ERROR_EINVAL, "bad handshake packet: strings longer than 1024 bytes");
  }
  if (((HS->binlog_tag_len + HS->binlog_slice_name_len + 2 + sizeof (struct repl_handshake) + 3) & -4) != len) {
    return send_error (c, HS->handshake_id, 0, R_ERROR_EINVAL, "bad handshake packet: incorrect total packet size");
  }
  if (HS->binlog_tag[HS->binlog_tag_len] || HS->binlog_slice_name[HS->binlog_tag_len + 1 + HS->binlog_slice_name_len]) {
    return send_error (c, HS->handshake_id, 0, R_ERROR_EINVAL, "bad handshake packet: strings are not zero-terminated");
  }
  char *client_slice_name = HS->binlog_slice_name + HS->binlog_tag_len + 1;
  vkprintf (2, "received handshake packet from %s:%d, handshake_id=%d, for binlog tag %s, slice name %s (size %lld, pos %lld..%lld)\n", show_remote_ip (c), c->remote_port, HS->handshake_id, HS->binlog_tag, client_slice_name, HS->binlog_file_size, HS->binlog_slice_start_pos, HS->binlog_slice_end_pos);
  struct remote_binlog *remb = get_rb_hash (HS->binlog_tag, HS->binlog_tag_len, 0);
  if (!remb) {
    return send_error (c, HS->handshake_id, 0, R_ERROR_ENOENT, "we don't know anything about requested binlog tag");
  }
  int i;
  struct related_binlog *R = 0;
  for (i = 0; i < local_rbs; i++) {
    if (LR[i].binlog == remb) {
      R = &LR[i];
    }
  }
  if (!R) {
    return send_error (c, HS->handshake_id, 0, R_ERROR_ENOENT, "no copies of requested binlog are present here");
  }
  if ((R->flags & LRF_BROKEN) != 0) {
    return send_error (c, HS->handshake_id, 0, R_ERROR_ENOENT, "local copy of requested binlog is broken");
  }
  if (!(R->flags & LRF_MASTER)) {  // is this check really necessary?
    return send_error (c, HS->handshake_id, 0, R_ERROR_ENOENT, "don't have master copy of requested binlog");
  }

  if (!HS->binlog_slice_name_len) {
    if (HS->binlog_first_file_size != -1 || HS->binlog_file_size != -1 || HS->binlog_slice_start_pos != 0 || HS->binlog_slice_end_pos != 0) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EINVAL, "no slice name passed but binlog files exist at client or log position is non-zero");
    }
    if (verbosity > 0) {
      fprintf (stderr, "client requested replication of %s without initial files\n", HS->binlog_tag);
    }
    if (R->min_binlog_pos) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_ENOENT, "replication from position 0 is impossible : binlog start slice not found on server");
    }
  } else {
    if (HS->binlog_file_size <= 0) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EINVAL, "last binlog slice cannot be empty");
    }
    if (!(HS->binlog_slice_start_pos >= 0 && HS->binlog_slice_end_pos >= 0 && HS->binlog_file_size >= HS->binlog_slice_end_pos - HS->binlog_slice_start_pos)) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EINVAL, "impossible binlog slice start/end log positions");
    }
  }

  if ((HS->binlog_first_file_size >= 0 && !R->min_binlog_pos && HS->binlog_first_file_size > R->kfs_replica->binlogs[0]->file_size) || R->update_generation != sigrtmax_cnt) {
    update_related_binlog (R);
  }

  if (HS->binlog_first_file_size >= 0) {
    kfs_file_handle_t binlog = 0;
    if (!R->min_binlog_pos) {
      int start_size = (HS->binlog_first_file_size < (1 << 20) ? HS->binlog_first_file_size : (1 << 20));
      if (HS->binlog_first_file_size > get_orig_file_size (R->kfs_replica->binlogs[0])) {
        return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "first binlog file is longer at slave than at master");
      }
      const int has_tag = memchr (HS->binlog_tag, '@', HS->binlog_tag_len) != NULL;
      if (start_size == R->first_file_start_size || has_tag) {
        binlog = (void *) -1;
        if (memcmp (HS->binlog_first_file_start_hash, R->first_file_start_hash, sizeof (md5_hash_t))) {
          return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "first binlog file hash mismatch");
        } else if (verbosity > 0) {
          if (has_tag) {
            fprintf (stderr, "first binlog file tag check passed\n");
          } else {
            fprintf (stderr, "first binlog file prefix check passed (%d first bytes checked)\n", start_size);
          }
        }
      } else {
        assert (start_size < R->first_file_start_size);
        binlog = open_binlog (R->kfs_replica, 0);
        if (binlog) {
          assert (!(binlog->info->flags & 16)); /* why we pack first binlog if it's original size is lesser than 1m? */ 
          int fd = binlog->fd;
          static md5_hash_t start_hash;
          assert (pread (fd, stats_buff, start_size, 0) == start_size);
          close_binlog (binlog, 1);
          md5 ((unsigned char *) stats_buff, start_size, start_hash);
          if (memcmp (HS->binlog_first_file_start_hash, start_hash, sizeof (md5_hash_t))) {
            return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "first binlog file hash mismatch");
          } else if (verbosity > 0) {
            fprintf (stderr, "first binlog file prefix check passed (%d first bytes checked)\n", start_size);
          }
        }
      }
    }
    if (!binlog) {
      fprintf (stderr, "warning: cannot check hash of first binlog for %s\n", R->path);
    }
  } else {
    if (HS->binlog_first_file_size != -1 || (HS->binlog_slice_start_pos <= 0 && HS->binlog_file_size >= 0)) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EINVAL, "received invalid first file size");
    }
  }

  assert (HS->binlog_slice_start_pos >= 0);

  kfs_file_handle_t binlog = open_binlog (R->kfs_replica, HS->binlog_slice_start_pos);
  if (!binlog) {
    return send_error (c, HS->handshake_id, 0, R_ERROR_ENOENT, "cannot start replication from binlog position requested by client: binlog slice absent on server");
  }

  long long binlog_start_pos = binlog->info->log_pos;
  long long binlog_end_pos = binlog_start_pos + binlog->info->file_size - 4096 * binlog->info->kfs_headers;
  if (verbosity > 0) {
    fprintf (stderr, "opening local slice %s (covering %lld..%lld)\n", binlog->info->filename, binlog_start_pos, binlog_end_pos);
  }

  if (binlog_start_pos != HS->binlog_slice_start_pos) {
    return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "client binlog slice start position does not match master");
  }

  const int server_zipped = zipped_filename (binlog->info->filename);
  assert (server_zipped >= 0);

  if (HS->binlog_file_size >= 0) {
    const int client_zipped = zipped_filename (client_slice_name);
    if (client_zipped < 0) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "bad slice suffix");
    }

    if (server_zipped != client_zipped) {
      return server_zipped ?
        send_error (c, HS->handshake_id, 0, R_ERROR_EUNLINKSLICE, "%s%s", msg_unlink_slice, client_slice_name) :
        send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "client has zipped slice, but server has original slice");
    }

    if (binlog_end_pos < HS->binlog_slice_end_pos) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "client binlog slice is longer than at master");
    }
    if (binlog_end_pos - 36 < HS->binlog_slice_end_pos && HS->binlog_slice_end_pos < binlog_end_pos && R->slice_binlog_pos > binlog_start_pos) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "client binlog slice trailing LEV_ROTATE_TO record is truncated");
    }
    if (binlog->info->kfs_headers * 4096 != HS->binlog_file_size - (HS->binlog_slice_end_pos - HS->binlog_slice_start_pos)) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "client and master binlog slice kfs headers are of different size");
    }
    char *local_slice_name = binlog->info->filename;
    char *ptr = strrchr (local_slice_name, '/');
    if (ptr) {
      local_slice_name = ptr + 1;
    }
    /* useless check TODO: check its uselessness
       if (strcmp (local_slice_name, client_slice_name)) {
       return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "client and master binlog slice names differ");
       }
     */
    static md5_hash_t start_hash, end_hash;
    compute_start_end_hashes (binlog->fd, HS->binlog_file_size, start_hash, end_hash);
    if (memcmp (HS->binlog_file_start_hash, start_hash, sizeof (md5_hash_t))) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "client and master binlog slice start hashes differ");
    }
    if (memcmp (HS->binlog_file_end_hash, end_hash, sizeof (md5_hash_t))) {
      return send_error (c, HS->handshake_id, 0, R_ERROR_EBADSLICE, "client and master binlog slice end hashes differ, client file is not a prefix of master copy");
    }
    if (verbosity > 0) {
      fprintf (stderr, "remote binlog slice of length %lld appears to be prefix of local binlog slice %s of length %lld\n", HS->binlog_file_size, local_slice_name, binlog->info->file_size);
    }
    // close_binlog (binlog, 1);
    S_flags = RS_REPL_STARTED;
    // here: start replication
    int n = 36;
    if (server_zipped) {
      S_flags |= RS_ZIPPED;
      n = 0;
    }
    if (binlog_end_pos - n <= HS->binlog_slice_end_pos && R->slice_binlog_pos > binlog_start_pos) {
      S_flags |= RS_LOGROTATE;
    }
  } else {
    S_flags = RS_REPL_STARTED | RS_ZEROSTART | RS_LOGROTATE;
    // here: start replication from empty set of files
  }

  if (verbosity > 0) {
    fprintf (stderr, "handshake: all sanity checks passed!\n");
  }

  int session_id = (++last_session_id & 0x3fffffff) + 1;
  assert (!get_server_by_session (session_id, 0));
  struct repl_server_status *S = get_server_by_session (session_id, 1);

  S->server_prev = R->server_last;
  S->server_next = (struct repl_server_status *) R;
  R->server_last->server_next = S;
  R->server_last = S;

  S->rb = R;
  S->handshake_id = HS->handshake_id;
  S->flags = 0;
  S->conn = c;
  S->conn_generation = c->generation;

  //S->client_file_size = HS->binlog_file_size;
  S->client_log_recvpos = S->client_log_wrpos = S->client_log_pos = HS->binlog_slice_end_pos;

  S->buffer_size = SEND_BUFFER_SIZE;
  S->buffer = malloc (S->buffer_size);
  S->rptr = S->wptr = S->buffer;
  S->flags = S_flags;
  if (!(S->flags & RS_ZEROSTART)) {
    S->flags |= RS_LOADDISK;
    S->buffer_file_pos = S->client_log_pos - binlog_start_pos + binlog->info->kfs_headers * 4096;
    S->logrotate_pos = -1;
    S->binlog = binlog;
    if (S->flags & RS_LOGROTATE) {
      S->logrotate_pos = HS->binlog_slice_end_pos;
    }
  } else {
    close_binlog (binlog, 1);
    S->buffer_file_pos = 0;
    S->logrotate_pos = 0;
    S->binlog = 0;
  }

  struct repl_handshake_ok *HO = alloc_packet (sizeof (struct repl_handshake_ok));
  HO->type = P_REPL_HANDSHAKE_OK;
  HO->handshake_id = HS->handshake_id;
  HO->session_id = session_id;
  HO->binlog_slice_start_pos = binlog_start_pos;
  HO->binlog_slice_end_pos = binlog_end_pos;
  HO->binlog_last_start_pos = R->slice_binlog_pos;
  HO->binlog_last_end_pos = R->max_binlog_pos;

  vkprintf (2, "acknowledging handshake to %s:%d, session=%d:%d, for binlog tag %s, current slice pos %lld..%lld, last slice pos %lld..%lld\n", show_remote_ip (c), c->remote_port, HO->handshake_id, session_id, HS->binlog_tag, HO->binlog_slice_start_pos, HO->binlog_slice_end_pos, HO->binlog_last_start_pos, HO->binlog_last_end_pos);

  push_packet (c, sizeof (struct repl_handshake_ok));

  return 0;
}


int destroy_client (struct repl_client_status *C) {
  int j;
  vkprintf (1, "destroyed client %d\n", C->handshake_id);
  for (j = 0; j < C->rb->targets; j++) {
    if (C->rb->client[j] == C) {
      C->rb->client[j] = 0;
    }
  }
  get_client_by_handshake (C->handshake_id, -1);
  assert (--active_repl_clients >= 0);
  return 0;
}


int destroy_server (struct repl_server_status *S) {
  vkprintf (1, "destroyed server %d\n", S->session_id);
  S->server_next->server_prev = S->server_prev;
  S->server_prev->server_next = S->server_next;
  if (S->buffer) {
    // will be funny if we have pending aio_read in this buffer
    free (S->buffer);
  }
  if (S->binlog) {
    close_binlog (S->binlog, 1);
  }
  get_server_by_session (S->session_id, -1);
  assert (--active_repl_servers >= 0);
  return 0;
}

int process_handshake_ok_packet (struct connection *c, struct repl_handshake_ok *HO) {
  struct repl_client_status *C = get_client_by_handshake (HO->handshake_id, 0);
  vkprintf (2, "received handshake acknowledgement packet from %s:%d, session %d:%d, current slice %lld..%lld, last slice %lld..%lld\n", show_remote_ip (c), c->remote_port, HO->handshake_id, HO->session_id, HO->binlog_slice_start_pos, HO->binlog_slice_end_pos, HO->binlog_last_start_pos, HO->binlog_last_end_pos);
  if (!C) {
    return send_error (c, HO->handshake_id, HO->session_id, R_ERROR_EBADFD, "unknown handshake id");
  }
  if (C->session_id) {
    return send_error (c, HO->handshake_id, HO->session_id, R_ERROR_EBADFD, "handshake already complete for this id");
  }

  struct related_binlog *R = C->rb;
  C->session_id = HO->session_id;

  if (HO->binlog_slice_start_pos > HO->binlog_slice_end_pos || HO->binlog_last_start_pos > HO->binlog_last_end_pos ||
      HO->binlog_last_end_pos < HO->binlog_slice_end_pos || HO->binlog_last_start_pos < HO->binlog_slice_start_pos ||
      HO->binlog_slice_start_pos < 0 || HO->binlog_slice_end_pos < 0 ||
      (HO->binlog_last_start_pos > HO->binlog_slice_start_pos && HO->binlog_last_start_pos < HO->binlog_slice_end_pos)) {
    destroy_client (C);
    return send_error (c, HO->handshake_id, HO->session_id, R_ERROR_EBADSLICE, "invalid binlog start/end positions");
  }

  if (R->kfs_replica) {
    if (R->slice_binlog_pos != HO->binlog_slice_start_pos || R->max_binlog_pos > HO->binlog_slice_end_pos) {
      destroy_client (C);
      return send_error (c, HO->handshake_id, HO->session_id, R_ERROR_EBADSLICE, "binlog start/end positions mismatch: client %lld..%lld, server %lld..%lld", R->slice_binlog_pos, R->max_binlog_pos, HO->binlog_slice_start_pos, HO->binlog_slice_end_pos);
    }
    if (HO->binlog_last_end_pos == R->max_binlog_pos) {
      C->flags |= RC_UPTODATE;
    }
  } else {
    if (HO->binlog_slice_start_pos || HO->binlog_slice_end_pos < 0) {
      destroy_client (C);
      return send_error (c, HO->handshake_id, HO->session_id, R_ERROR_EBADSLICE, "binlog start/end positions mismatch: client %lld..%lld, server %lld..%lld", R->slice_binlog_pos, R->max_binlog_pos, HO->binlog_slice_start_pos, HO->binlog_slice_end_pos);
    }
  }

  return 0;
};

static int bad_error_packet (struct repl_error *E, int len) {
  return E->error_message[len - sizeof (struct repl_error) - 1];
}

int process_generic_error_packet (struct connection *c, struct repl_error *E, int len) {
  errors_received++;
  if (!bad_error_packet (E, len)) {
    vkprintf (0, "Received error packet in session %d:%d from %s:%d, error code %d: %s\n", E->handshake_id, E->session_id, show_remote_ip (c),
	      c->remote_port, E->error, E->error_message);
  } else {
    vkprintf (0, "Received malformed error packet in session %d:%d from %s:%d, error code %d\n", E->handshake_id, E->session_id, show_remote_ip (c),
	      c->remote_port, E->error);
  }
  return 0;
}

static char *basename (char *filename) {
  char *p = strrchr (filename, '/');
  return p ? (p + 1) : filename;
}

int process_client_error_packet (struct connection *c, struct repl_error *E, int len) {
  struct repl_client_status *C = get_client_by_handshake (E->handshake_id, 0);
  if (C) {
    if (!bad_error_packet (E, len) && !E->session_id && E->error == R_ERROR_EUNLINKSLICE && !strncmp (E->error_message, msg_unlink_slice, strlen (msg_unlink_slice))) {
      char *client_slice_name = E->error_message + strlen (msg_unlink_slice);
      struct related_binlog *R = C->rb;
      assert (R && R->kfs_binlog);
      vkprintf (2, "%s: client_slice_name = '%s', R->kfs_binlog_info->filename = '%s'\n", __func__, client_slice_name, R->kfs_binlog->info->filename);
      if (!strcmp (client_slice_name, basename (R->kfs_binlog->info->filename))) {
        if (!unlink (R->kfs_binlog->info->filename)) {
          kprintf ("original slice '%s' was successfully unlinked\n", R->kfs_binlog->info->filename);
          if (R->kfs_replica) {
            close_replica (R->kfs_replica);
            R->kfs_replica = 0;
          }
          if (!check_binlog (R) && !lock_last_binlog (R)) {
            destroy_client (C);
            int j;
            for (j = 0; j < R->targets; j++) {
              if (R->client[j] == 0) {
                return init_client_session (R, c, j);
              }
            }
            return 0;
          }
        } else {
          kprintf ("fail to unlink '%s'. %m\n", R->kfs_binlog->info->filename);
        }
      }
    }
    destroy_client (C);
  }
  return process_generic_error_packet (c, E, len);
}

int process_server_error_packet (struct connection *c, struct repl_error *E, int len) {
  struct repl_server_status *S = get_server_by_session (E->session_id, 0);
  if (S) {
    destroy_server (S);
  }
  return process_generic_error_packet (c, E, len);
}

int check_flush_client (struct repl_client_status *C) {
  double p_now = get_utime (CLOCK_MONOTONIC);
  if (C->unflushed_bytes >= MAX_CLIENT_UNFLUSHED_BYTES || (C->unflushed_bytes && p_now > C->last_flush_time + MAX_FLUSH_INTERVAL)) {
    struct related_binlog *R = C->rb;
    vkprintf (2, "%lld unflushed bytes pending in binlog slice %s, syncing...\n", C->unflushed_bytes, R->kfs_binlog->info->filename);
    assert (fsync (R->kfs_binlog->fd) >= 0);
    tot_unflushed_bytes -= C->unflushed_bytes;
    C->unflushed_bytes = 0;
    C->last_flush_time = p_now;
    return 1;
  }
  return 0;
}

int process_data_packet (struct connection *c, struct repl_data *D, int len) {
  struct repl_client_status *C = get_client_by_handshake (D->handshake_id, 0);
  vkprintf (2, "received data packet from %s:%d, session %d:%d, flags %d, current slice starts at %lld, binlog pos %lld, %d data bytes\n", show_remote_ip (c), c->remote_port, D->handshake_id, D->session_id, D->flags, D->A.binlog_slice_start_pos, D->A.binlog_slice_cur_pos, D->A.data_size);
  data_packets_received++;
  if (!C) {
    return send_error (c, D->handshake_id, D->session_id, R_ERROR_EBADFD, "unknown handshake id");
  }
  if (C->session_id != D->session_id) {
    destroy_client (C);
    return send_error (c, D->handshake_id, D->session_id, R_ERROR_EBADFD, "handshake/session id mismatch");
  }
  if (((D->A.data_size + 3) & -4) != len - 4) {
    destroy_client (C);
    return send_error (c, D->handshake_id, D->session_id, R_ERROR_EINVAL, "packet size mismatch");
  }
  if (D->A.headers_size) {
    destroy_client (C);
    return send_error (c, D->handshake_id, D->session_id, R_ERROR_EINVAL, "headers size is non-zero");
  }
  if (D->flags & -2) {
    destroy_client (C);
    return send_error (c, D->handshake_id, D->session_id, R_ERROR_EINVAL, "unknown flags");
  }
  struct related_binlog *R = C->rb;
  if (!double_send_recv && R->slice_binlog_pos != D->A.binlog_slice_start_pos) {
    destroy_client (C);
    return send_error (c, D->handshake_id, D->session_id, R_ERROR_EBADSLICE, "binlog slice start position mismatch: expected %lld, found %lld", R->slice_binlog_pos, D->A.binlog_slice_start_pos);
  }
  if (double_send_recv && R->slice_binlog_pos < D->A.binlog_slice_start_pos) {
    destroy_client (C);
    return send_error (c, D->handshake_id, D->session_id, R_ERROR_EBADSLICE, "binlog slice start position in the future: expected %lld, found %lld", R->slice_binlog_pos, D->A.binlog_slice_start_pos);
  }
  long long skip_bytes = 0;
  if (double_send_recv && R->max_binlog_pos > D->A.binlog_slice_cur_pos) {
    skip_bytes = R->max_binlog_pos - D->A.binlog_slice_cur_pos;
  } else {
    if (R->max_binlog_pos != D->A.binlog_slice_cur_pos) {
      destroy_client (C);
      return send_error (c, D->handshake_id, D->session_id, R_ERROR_EBADSLICE, "binlog slice current position mismatch: expected %lld, found %lld", R->max_binlog_pos, D->A.binlog_slice_cur_pos);
    }
    assert (R->kfs_binlog->info->file_size == R->max_binlog_pos - R->slice_binlog_pos + R->kfs_binlog->info->kfs_headers * 4096);
  }

  len -= 4;
  if (skip_bytes > 0) {
    vkprintf (3, "%s: Skip %lld bytes from connection %d, session %d:%d\n", __func__, skip_bytes, c->fd, D->handshake_id, D->session_id);
  }

  int data_len = D->A.data_size;
  const long long binlog_written_pos = D->A.binlog_slice_cur_pos + data_len;
  while (len > 0) {
    int sz = get_ready_bytes (&c->In);
    char *ptr = get_read_ptr (&c->In);
    if (sz > len) {
      sz = len;
    }
    assert (sz > 0);
    int sz2 = data_len < sz ? data_len : sz;
    int sz3 = skip_bytes < sz2 ? skip_bytes : sz2;
    if (sz3 > 0) {
      skip_bytes -= sz3;
      data_len -= sz3;
      ptr += sz3;
      sz2 -= sz3;
    }
    if (sz2 > 0) {
      vkprintf (2, "writing %d bytes to %s from position %lld (binlog position %lld)\n", sz2, R->kfs_binlog->info->filename, R->kfs_binlog->info->file_size, R->max_binlog_pos);
      assert (pwrite (R->kfs_binlog->fd, ptr, sz2, R->kfs_binlog->info->file_size) == sz2);
      R->kfs_binlog->info->file_size += sz2;
      R->max_binlog_pos += sz2;
      C->unflushed_bytes += sz2;
      tot_unflushed_bytes += sz2;
      data_len -= sz2;
    }
    advance_read_ptr (&c->In, sz);
    len -= sz;
  }
  assert (advance_skip_read_ptr (&c->In, 4) == 4);

  check_flush_client (C);

  if (D->flags & RDF_SENTALL) {
    C->flags |= RC_UPTODATE;
  } else {
    C->flags &= ~RC_UPTODATE;
  }

  data_bytes_received += D->A.data_size;

  assert (double_send_recv || binlog_written_pos == R->max_binlog_pos);
  push_data_ack_packet (c, D->handshake_id, D->session_id, binlog_written_pos);
  return 0;
}

int process_data_ack_packet (struct connection *c, struct repl_data_ack *A) {
  struct repl_server_status *S = get_server_by_session (A->session_id, 0);
  vkprintf (2, "received data ack packet from %s:%d, session %d:%d, binlog written up to %lld, received up to %lld\n", show_remote_ip (c), c->remote_port, A->handshake_id, A->session_id, A->binlog_written_pos, A->binlog_received_pos);
  if (!S) {
    return send_error (c, A->handshake_id, A->session_id, R_ERROR_EBADFD, "unknown session id");
  }
  if (S->handshake_id != A->handshake_id) {
    destroy_server (S);
    return send_error (c, A->handshake_id, A->session_id, R_ERROR_EBADFD, "handshake/session id mismatch");
  }
  if (A->binlog_written_pos < 0 || A->binlog_received_pos < A->binlog_written_pos) {
    destroy_server (S);
    return send_error (c, A->handshake_id, A->session_id, R_ERROR_EINVAL, "incorrect log positions");
  }
  if (A->binlog_written_pos > S->client_log_pos || A->binlog_written_pos < S->client_log_wrpos || A->binlog_received_pos < S->client_log_recvpos) {
    destroy_server (S);
    return send_error (c, A->handshake_id, A->session_id, R_ERROR_EINVAL, "acknowledged log positions go backwards and/or after the end of transmitted data");
  }
  S->client_log_wrpos = A->binlog_written_pos;
  S->client_log_recvpos = A->binlog_received_pos;
  process_server (S);
  return 0;
}


int process_rotate_packet (struct connection *c, struct repl_rotate *RT, int len) {
  struct repl_client_status *C = get_client_by_handshake (RT->handshake_id, 0);
  vkprintf (1, "Received rotate packet from %s:%d, session %d:%d, %d+%d+%d bytes, flags %d, slice 1 pos %lld..%lld, slice2 pos %lld..%lld\n",
	    show_remote_ip (c), c->remote_port, C->handshake_id, C->session_id,
	    RT->A1.data_size, RT->A2.headers_size, RT->A2.data_size, RT->flags,
	    RT->A1.binlog_slice_start_pos, RT->A1.binlog_slice_cur_pos, RT->A2.binlog_slice_start_pos, RT->A2.binlog_slice_cur_pos);
  rotate_packets_received++;
  int create_new = !RT->A2.binlog_slice_start_pos;
  if (!C) {
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EBADFD, "unknown handshake id");
  }
  if (C->session_id != RT->session_id) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EBADFD, "handshake/session id mismatch");
  }
  if (sizeof (struct repl_rotate) + 1LL + (RT->binlog_slice2_name_len | 3) + RT->A1.headers_size + RT->A1.data_size + RT->A2.headers_size + RT->A2.data_size != len) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid rotate packet size");
  }

  const int zipped = zipped_filename (RT->binlog_slice2_name);
  if (zipped < 0) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid slice suffix");
  }

  if (zipped) {
    if (RT->A2.headers_size || RT->A2.data_size < sizeof (kfs_binlog_zip_header_t)) {
      destroy_client (C);
      return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid rotate packet content (zipped binlog");  
    }
  } else {
    if ((RT->A2.headers_size & 4095) ||
        (!create_new && RT->A2.data_size != 36) || RT->A2.data_size < 24 || RT->A2.data_size > 1024 ||
        RT->A2.headers_size < 0 ||
        RT->A2.headers_size > 8192) {
      destroy_client (C);
      return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid rotate packet content (original binlog)");
    }
  }

  if ((RT->flags & -2) || RT->A1.headers_size || (RT->A1.data_size != 0 && RT->A1.data_size != 36)) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid rotate packet content");
  }
  if (create_new && (RT->A1.binlog_slice_start_pos != -1 || RT->A1.binlog_slice_cur_pos || RT->A1.data_size || RT->A2.binlog_slice_start_pos)) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid rotate packet content for zero start position");
  }
  if (!create_new && (RT->A1.binlog_slice_start_pos < 0 || RT->A1.binlog_slice_cur_pos <= 0 || RT->A2.binlog_slice_start_pos <= 0)) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid rotate packet content for non-zero start position");
  }
  struct related_binlog *R = C->rb;

  if (double_send_recv && ((R->slice_binlog_pos >= RT->A2.binlog_slice_start_pos) || (R->max_binlog_pos >= RT->A2.binlog_slice_start_pos + RT->A2.data_size)) ) {
    vkprintf (2, "Skip rotate packet from connection %d in session %d:%d\n", c->fd, RT->handshake_id, RT->session_id);
    push_data_ack_packet (c, RT->handshake_id, RT->session_id, RT->A2.binlog_slice_start_pos + RT->A2.data_size);
    return 0;
  }

  if ((create_new && !R->slice_binlog_pos ? -1 : R->slice_binlog_pos) != RT->A1.binlog_slice_start_pos) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EBADSLICE, "binlog slice start position mismatch: expected %lld, found %lld", R->slice_binlog_pos, RT->A1.binlog_slice_start_pos);
  }
  /* RT->A1.data_size == 0 -> rotated from zipped binlog */
  if (RT->A1.data_size && R->max_binlog_pos != RT->A1.binlog_slice_cur_pos) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EBADSLICE, "binlog slice current position mismatch: expected %lld, found %lld", R->max_binlog_pos, RT->A1.binlog_slice_cur_pos);
  }
  if (RT->A1.binlog_slice_cur_pos + RT->A1.data_size != RT->A2.binlog_slice_start_pos ||
      RT->A2.binlog_slice_start_pos != RT->A2.binlog_slice_cur_pos ||
      RT->binlog_slice2_name[RT->binlog_slice2_name_len]) {
    destroy_client (C);
    return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid rotate packet content");
  }

  kfs_file_handle_t OldBinlog = R->kfs_binlog, NextBinlog = 0;
  assert (!OldBinlog || OldBinlog->info->file_size == R->max_binlog_pos - R->slice_binlog_pos + OldBinlog->info->kfs_headers * 4096);

  char *data1 = RT->binlog_slice2_name + (RT->binlog_slice2_name_len | 3) + 1;
  char *data2 = data1 + RT->A1.data_size;

  struct lev_rotate_from *new_rotate_from = (struct lev_rotate_from *) (zipped ? (data2 + offsetof (kfs_binlog_zip_header_t, first36_bytes)) : (data2 + RT->A2.headers_size));
  struct lev_start *new_start = (struct lev_start *) new_rotate_from;

  if (create_new) {
    if (new_start->type != LEV_START) {
      destroy_client (C);
      return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "remote first binlog slice doesn't start with LEV_START");
    }
    if (!zipped) {
      assert ((unsigned) new_start->extra_bytes <= 1000);
      int S_size = 24 + ((new_start->extra_bytes + 3) & -4);
      assert (RT->A2.data_size == S_size);
    } else {
      assert (!RT->A2.headers_size);
    }
    assert (!OldBinlog);
    assert (R->kfs_replica);
    assert (!R->kfs_replica->binlog_num);

    vkprintf (0, "Creating completely new replicated binlog %s\n", R->kfs_replica->replica_prefix);
    NextBinlog = create_first_binlog (R->kfs_replica, data2, RT->A2.headers_size + RT->A2.data_size, strstr (RT->binlog_slice2_name, ".000000.bin") ? 1 : 0, 1, zipped);
    if (!NextBinlog) {
      fprintf (stderr, "failed to create first binlog file for %s\n", R->kfs_replica->replica_prefix);
      assert (0);
    }
  } else {
    if (RT->A1.data_size != 36 && RT->A1.data_size != 0) {
      destroy_client (C);
      return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "rotate packet is expected to contain exactly 36 last bytes of previous slice");
    }
    struct lev_rotate_to *new_rotate_to = 0;
    if (RT->A1.data_size) {
      new_rotate_to = (struct lev_rotate_to *) data1;
      if (new_rotate_to->type != LEV_ROTATE_TO) {
        destroy_client (C);
        return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "previous binlog slice doesn't end with LEV_ROTATE_TO");
      }
    } else {
      assert (OldBinlog);
      if (!(OldBinlog->info->flags & 16)) {
        char *a = alloca (36);
        int r = pread (OldBinlog->fd, a, 36, OldBinlog->info->file_size - 36);
        if (r != 36) {
          destroy_client (C);
          return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "read last 36 bytes from old binlog failed. %s", r < 0 ? strerror (errno) : "");
        }
        new_rotate_to = (struct lev_rotate_to *) a;
      } else {
        kfs_binlog_zip_header_t *H = load_binlog_zip_header (OldBinlog->info);
        if (!H) {
          destroy_client (C);
          return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "loading old binlog zipped header failed");
        }
        new_rotate_to = (struct lev_rotate_to *) H->last36_bytes;
      }
    }
    assert (new_rotate_to);
    if (new_rotate_from->type != LEV_ROTATE_FROM) {
      destroy_client (C);
      return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "remote next binlog slice doesn't start with LEV_ROTATE_FROM");
    }
    if (new_rotate_from->cur_log_pos != RT->A2.binlog_slice_start_pos) {
      destroy_client (C);
      return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "invalid starting position in received LEV_ROTATE_FROM record");
    }

    //Are new_rotate_from and new_rotate_to correspond to each other?
    if (match_rotate_logevents (new_rotate_to, new_rotate_from) <= 0) {
      destroy_client (C);
      return send_error (c, RT->handshake_id, RT->session_id, R_ERROR_EINVAL, "ROTATE_TO and ROTATE_FROM are mismatched");
    }

    assert (OldBinlog);
    assert (fsync (OldBinlog->fd) >= 0);

    NextBinlog = create_next_binlog_ext (OldBinlog, new_rotate_from->cur_log_pos, new_rotate_from->cur_log_hash, 1, zipped);
    if (!NextBinlog) {
      fprintf (stderr, "failed to create next binlog file after %s at position %lld\n", OldBinlog->info->filename, log_pos);
      assert (0);
    }
  }

  if (!create_new && write (NextBinlog->fd, data2, RT->A2.headers_size + RT->A2.data_size) != RT->A2.headers_size + RT->A2.data_size) {
    fprintf (stderr, "unable to write %d bytes into next binlog file %s (log position %lld): %m\n", RT->A2.headers_size + RT->A2.data_size, NextBinlog->info->filename, RT->A2.binlog_slice_start_pos);
    unlink (NextBinlog->info->filename);
    close_binlog (NextBinlog, 1);
    assert (0);
  }

  if (OldBinlog) {
    int sz = RT->A1.data_size;
    vkprintf (2, "writing %d bytes to %s from position %lld (binlog position %lld)\n", sz, OldBinlog->info->filename, OldBinlog->info->file_size, R->max_binlog_pos);
    if (pwrite (OldBinlog->fd, data1, sz, OldBinlog->info->file_size) != sz || fsync (OldBinlog->fd) < 0) {
      fprintf (stderr, "fatal: error while writing last %d bytes to binlog slice %s\n", sz, OldBinlog->info->filename);
      unlink (NextBinlog->info->filename);
      close_binlog (NextBinlog, 1);
      assert (0);
    }
    OldBinlog->info->file_size += sz;
    R->max_binlog_pos += sz;

    if (OldBinlog->info->flags & 16) {
      if (chmod (OldBinlog->info->filename, 0440) < 0) {
        kprintf ("%s: failed to changing mode 0440 for the file '%s'. %m\n", __func__, OldBinlog->info->filename);
      }
    }

    close_binlog (OldBinlog, 1);
    R->kfs_binlog = 0;
  }

  R->kfs_binlog = NextBinlog;
  if (!create_new) {
    NextBinlog->info->file_size = RT->A2.headers_size + RT->A2.data_size;
  } else {
    assert (NextBinlog->info->file_size == RT->A2.headers_size + RT->A2.data_size);
  }

  if (!RT->A1.data_size) {
    assert (R->max_binlog_pos <= RT->A2.binlog_slice_start_pos);
    R->max_binlog_pos = RT->A2.binlog_slice_start_pos;
  }

  R->slice_binlog_pos = R->max_binlog_pos;
  R->max_binlog_pos += RT->A2.data_size;

  assert (R->slice_binlog_pos == RT->A2.binlog_slice_cur_pos);
  assert (NextBinlog->info->log_pos == R->slice_binlog_pos);

  long long keep_pos = NextBinlog->info->log_pos, keep_size = NextBinlog->info->file_size;

  update_related_binlog (R);
  assert (lock_whole_file (NextBinlog->fd, F_WRLCK) > 0);

  vkprintf (3, "%s: R->kfs_replica->binlog_num = %d, R->kfs_replica->binlogs[R->kfs_replica->binlog_num-1] = %p, NextBinlog->info = %p\n", __func__, R->kfs_replica->binlog_num, R->kfs_replica->binlogs[R->kfs_replica->binlog_num-1], NextBinlog->info);

  assert (R->kfs_replica->binlog_num > 0 && R->kfs_replica->binlogs[R->kfs_replica->binlog_num-1] == NextBinlog->info);
  assert (NextBinlog->info->log_pos == keep_pos && NextBinlog->info->file_size == keep_size);

  tot_unflushed_bytes -= C->unflushed_bytes;
  C->unflushed_bytes = 0;
  C->last_flush_time = get_utime (CLOCK_MONOTONIC);
  C->flags &= ~RC_UPTODATE;

  data_bytes_received += RT->A1.data_size + RT->A2.headers_size + RT->A2.data_size;

  vkprintf (0, "Created next binlog slice %s, initial size %lld, start log position %lld\n", NextBinlog->info->filename, NextBinlog->info->file_size, NextBinlog->info->log_pos);

  push_data_ack_packet (c, RT->handshake_id, RT->session_id, R->max_binlog_pos);

  return 0;
}


int process_pos_info_packet (struct connection *c, struct repl_pos_info *RP) {
  posinfo_packets_received++;
  struct repl_client_status *C = get_client_by_handshake (RP->handshake_id, 0);
  vkprintf (2, "received server time packet from %s:%d, session %d:%d, position %lld, time %016llx\n", show_remote_ip (c), c->remote_port, RP->handshake_id, RP->session_id, RP->binlog_pos, RP->binlog_time);
  if (!C) {
    return send_error (c, RP->handshake_id, RP->session_id, R_ERROR_EBADFD, "unknown handshake id");
  }
  if (C->session_id != RP->session_id) {
    destroy_client (C);
    return send_error (c, RP->handshake_id, RP->session_id, R_ERROR_EBADFD, "handshake/session id mismatch");
  }

  long long current_time = get_precise_time (1000000);

  struct related_binlog *R = C->rb;

  if (RP->binlog_time > current_time + (5LL << 32) || RP->binlog_time < current_time - (30LL << 32)) {
    vkprintf (1, "WARNING: server time packet from %s:%d, session %d:%d, position %lld, time %016llx for binlog %s is ignored because of bad time (current time %016llx)\n", show_remote_ip (c), c->remote_port, RP->handshake_id, RP->session_id, RP->binlog_pos, RP->binlog_time, R->kfs_replica->replica_prefix, current_time);
    return 0;
  }

  if ((unsigned long long) RP->binlog_pos > 1LL << 56) {
    vkprintf (1, "WARNING: server time packet from %s:%d, session %d:%d, position %lld, time %016llx for binlog %s is ignored because of bad position\n", show_remote_ip (c), c->remote_port, RP->handshake_id, RP->session_id, RP->binlog_pos, RP->binlog_time, R->kfs_replica->replica_prefix);
    return 0;
  }

  int res = update_binlog_postime_info (R->replica_name_hash, RP->binlog_pos, RP->binlog_time);
  vkprintf (2, "Result of updating binlog position info for replica %s to position %lld, time %016llx is %d\n", R->kfs_replica->replica_prefix, RP->binlog_pos, RP->binlog_time, res);

  return 0;
}



/*
 *
 *	DISK DATA LOAD FUNCTIONS
 *
 */

static inline long get_buffer_bytes (struct repl_server_status *S) {
  long cur_buffer_bytes = S->wptr - S->rptr;
  if (cur_buffer_bytes < 0) {
    cur_buffer_bytes += S->buffer_size;
    assert (cur_buffer_bytes > 0);
  }
  return cur_buffer_bytes;
}

int load_data (struct repl_server_status *S) {
  assert (S->buffer);
  if (!(S->flags & RS_LOADDISK)) {
    return 0;
  }
  if (S->rptr == S->wptr && S->rptr != S->buffer) {
    S->rptr = S->wptr = S->buffer;
  }
  long cur_buffer_bytes = get_buffer_bytes (S);
  if (cur_buffer_bytes > ((3 * S->buffer_size) >> 2)) {
    return 0;
  }
  char *w_end = S->buffer + S->buffer_size;
  if (S->wptr == w_end) {
    S->wptr = S->buffer;
    assert (S->rptr > S->wptr);
  }
  if (S->rptr > S->wptr) {
    w_end = S->rptr - 4;
    assert (w_end > S->wptr);
  }
  long to_read = w_end - S->wptr;
  long long start_file_pos = S->buffer_file_pos + cur_buffer_bytes;
  assert (to_read > 0 && to_read <= S->buffer_size);
  assert (S->client_log_pos - S->binlog->info->log_pos + S->binlog->info->kfs_headers * 4096 == S->buffer_file_pos);
  long r = pread (S->binlog->fd, S->wptr, to_read, start_file_pos);
  if (r < 0) {
    vkprintf (0, "error reading file %s at position %lld: %m\n", S->binlog->info->filename, start_file_pos);
  }
  assert (r >= 0 && r <= to_read);
  vkprintf (2, "reading %ld bytes from %s at position %lld: %ld bytes read\n", to_read, S->binlog->info->filename, start_file_pos, r);
  S->wptr += r;
  tot_read_bytes += r;
  long long end_file_pos = start_file_pos + r;
  if (r < to_read && end_file_pos < S->binlog->info->file_size) {
    vkprintf (0, "error reading file %s at position %lld: read only %ld bytes out of %lld\n", S->binlog->info->filename, start_file_pos, r, S->binlog->info->file_size - start_file_pos);
    exit (1);
  }
  if (end_file_pos > S->binlog->info->file_size && S->binlog->info->log_pos < S->rb->slice_binlog_pos) {
    vkprintf (0, "error reading file %s at position %lld: read up to position %lld past end of file\n", S->binlog->info->filename, start_file_pos, end_file_pos);
    exit (1);
  }
  if (end_file_pos > S->binlog->info->file_size) {
    assert (S->binlog->info->log_pos == S->rb->slice_binlog_pos);
    long long delta = end_file_pos - S->binlog->info->file_size;
    S->binlog->info->file_size += delta;
    S->rb->max_binlog_pos += delta;
  }
  if (r < to_read && end_file_pos >= S->binlog->info->file_size && S->binlog->info->log_pos == S->rb->slice_binlog_pos) {
    S->flags |= RS_ALLREAD;
  } else {
    S->flags &= ~RS_ALLREAD;
  }
  return r;
}

int load_all_data (void) {
  int i;
  for (i = 0; i < local_rbs; i++) {
    struct related_binlog *R = &LR[i];
    if ((R->flags & (LRF_MASTER | LRF_BROKEN)) == LRF_MASTER) {
      struct repl_server_status *S;
      for (S = R->server_first; S != (struct repl_server_status *) R; S = S->server_next) {
        if (S->flags & RS_LOADDISK) {
          load_data (S);
        }
      }
    }
  }
  return 0;
}

int classify_slave (struct related_binlog *R) {
  if ((R->flags & LRF_MASTER)) {
    return 4;
  }
  if ((R->flags & LRF_BROKEN)) {
    return 0;
  }
  int j, r = 1;
  for (j = 0; j < R->targets; j++) {
    int w = (!R->client[j]) ? 1 : (!(R->client[j]->flags & RC_UPTODATE)) ? 2 : 3;
    if (r < w) {
      r = w;
    }
  }
  return r;
}

void compute_slaves_stats (int slave_cnt[4]) {
  int i;
  memset (slave_cnt, 0, 16);
  for (i = 0; i < local_rbs; i++) {
    struct related_binlog *R = &LR[i];
    if (!(R->flags & LRF_MASTER)) {
      slave_cnt[classify_slave (R)]++;
    }
    update_rb_stats (R);
  }
}

inline unsigned char load_byte (struct repl_server_status *S, char **ptr) {
  --*ptr;
  if (*ptr < S->buffer) {
    *ptr += S->buffer_size;
  }
  return (unsigned char)**ptr;
}

int check_last36_bytes (struct repl_server_status *S) {
  char *ptr = S->wptr;
  int val = 0, res = 0, i;
  for (i = 0; i < 36 && ptr != S->rptr; i++) {
    val = (val << 8) | load_byte (S, &ptr);
    if (val == LEV_ROTATE_TO) {
      res = i + 1;
    }
  }
  return res;
}



int send_update_packet (struct connection *c, struct repl_server_status *S, int size) {
  int pad_bytes = -size & 3;
  send_packet[0] = sizeof (struct repl_data) + size + 12 + pad_bytes;
  send_packet[1] = RPCC_DATA(c)->out_packet_num++;
  struct repl_data *D = (struct repl_data *)(send_packet + 2);
  D->type = P_REPL_DATA;
  D->handshake_id = S->handshake_id;
  D->session_id = S->session_id;
  D->flags = 0;
  if ((S->flags & RS_ALLREAD) && size == get_buffer_bytes (S)) {
    S->flags |= RS_ALLSENT;
    D->flags |= RDF_SENTALL;
  } else {
    S->flags &= ~RS_ALLSENT;
  }
  D->A.headers_size = 0;
  D->A.data_size = size;
  D->A.binlog_slice_start_pos = S->binlog->info->log_pos;
  D->A.binlog_slice_cur_pos = S->client_log_pos;
  vkprintf (2, "sending data packet of %d bytes to session %d:%d at binlog position %lld (slice starts from %lld)\n", size, D->handshake_id, D->session_id, D->A.binlog_slice_cur_pos, D->A.binlog_slice_start_pos);
  int crc32 = crc32_partial (send_packet, sizeof (struct repl_data) + 8, -1);
  write_out (&c->Out, send_packet, sizeof (struct repl_data) + 8);
  assert (S->client_log_pos - S->binlog->info->log_pos + S->binlog->info->kfs_headers * 4096 == S->buffer_file_pos);
  S->client_log_pos += size;
  S->buffer_file_pos += size;
  data_bytes_sent += size;
  while (size > 0) {
    long l = (S->rptr <= S->wptr ? S->wptr : S->buffer + S->buffer_size) - S->rptr;
    if (l > size) {
      l = size;
    }
    assert (l > 0);
    write_out (&c->Out, S->rptr, l);
    crc32 = crc32_partial (S->rptr, l, crc32);
    size -= l;
    S->rptr += l;
    if (S->rptr >= S->buffer + S->buffer_size) {
      S->rptr -= S->buffer_size;
      assert (S->rptr == S->buffer);
      if (S->wptr >= S->buffer + S->buffer_size) {
        S->wptr -= S->buffer_size;
        assert (S->wptr == S->buffer);
      }
    }
  }
  if (pad_bytes) {
    static int pad = 0;
    write_out (&c->Out, &pad, pad_bytes);
    crc32 = crc32_partial (&pad, pad_bytes, crc32);
  }
  crc32 = ~crc32;
  write_out (&c->Out, &crc32, 4);
  // RPCC_FUNC(c)->flush_packet(c);
  flush_later (c);
  data_packets_sent++;
  return 1;
}


int process_server (struct repl_server_status *S) {
  if (!(S->flags & RS_REPL_STARTED) || !S->buffer) {
    return 0;
  }
  long cur_buffer_bytes = get_buffer_bytes (S);
  if (!cur_buffer_bytes) {
    return 0;
  }
  if (S->conn->Out.total_bytes + S->conn->Out.unprocessed_bytes > MAX_NET_OUT_QUEUE_BYTES) {
    return 0;
  }
  long long unconfirmed_bytes = S->client_log_pos - S->client_log_wrpos;
  assert (unconfirmed_bytes >= 0);
  if (unconfirmed_bytes >= DIRTY_THRESHOLD_LOW) {
    return 0;
  }
  long long buffer_end_pos = S->buffer_file_pos + cur_buffer_bytes;
  int can_send = DIRTY_THRESHOLD_HIGH - (int) unconfirmed_bytes;

  if (!(S->flags & RS_ZIPPED)) {
    if (buffer_end_pos < S->binlog->info->file_size && buffer_end_pos > S->binlog->info->file_size - 36) {
      cur_buffer_bytes = S->binlog->info->file_size - 36 - S->buffer_file_pos;
    } else if (buffer_end_pos >= S->binlog->info->file_size && can_send >= cur_buffer_bytes - 36) {
      assert (buffer_end_pos == S->binlog->info->file_size);
      if (S->binlog->info->log_pos != S->rb->slice_binlog_pos) {
        assert (S->binlog->info->log_pos < S->rb->slice_binlog_pos);
        if (cur_buffer_bytes > 0) {
          cur_buffer_bytes -= 36;
        }
        assert (cur_buffer_bytes >= 0);
        S->logrotate_pos = S->client_log_pos + cur_buffer_bytes;
        S->flags |= RS_LOGROTATE;
      } else {
        if (check_last36_bytes (S)) {
          load_data (S);
          long old_cur_buffer_bytes = cur_buffer_bytes;
          cur_buffer_bytes = S->wptr - S->rptr;
          if (cur_buffer_bytes < 0) {
            cur_buffer_bytes += S->buffer_size;
            assert (cur_buffer_bytes > 0);
          }
          assert (cur_buffer_bytes >= old_cur_buffer_bytes);
          if (check_last36_bytes (S) == 36) {
            assert (cur_buffer_bytes >= 36);
            cur_buffer_bytes -= 36;
            assert (cur_buffer_bytes >= 0);
            S->logrotate_pos = S->client_log_pos + cur_buffer_bytes;
            S->flags |= RS_LOGROTATE;
          }
        }
      }
    }
  } else {
    if (buffer_end_pos == S->binlog->info->file_size) {
      S->flags |= RS_LOGROTATE;
      S->logrotate_pos = S->client_log_pos + cur_buffer_bytes;
    }
  }

  if (cur_buffer_bytes <= 0) {
    return 0;
  }

  if (can_send > cur_buffer_bytes) {
    can_send = cur_buffer_bytes;
  }
  if (can_send > MAX_UPDATE_SIZE) {
    can_send = MAX_UPDATE_SIZE;
  }
  // now send can_send bytes to client
  return send_update_packet (S->conn, S, can_send);
}

static int get_bz_header_size (struct kfs_file_info *FI) {
  assert (FI->flags & 16);
  long long orig_file_size = get_orig_file_size (FI);
  return kfs_bz_compute_header_size (orig_file_size);
}

int process_server_rotate (struct repl_server_status *S) {
  if (!(S->flags & RS_LOGROTATE) || S->client_log_wrpos < S->logrotate_pos) {
    return 0;
  }
  struct related_binlog *R = S->rb;
  assert (S->client_log_wrpos == S->logrotate_pos && S->client_log_recvpos == S->logrotate_pos && S->client_log_pos == S->logrotate_pos);
  vkprintf (1, "reached LOGROTATE at %s position %lld\n", S->binlog ? S->binlog->info->filename : "(init-binlog)", S->logrotate_pos);
  int pending_bytes = (int) get_buffer_bytes (S);
  assert (pending_bytes == 0 || pending_bytes == 36);
  assert (!pending_bytes || S->binlog);
  vkprintf (2, "buffer file pos %lld, pending %d bytes, file size %lld\n",
	    S->buffer_file_pos, pending_bytes, S->binlog ? S->binlog->info->file_size : 0);
  assert (S->buffer_file_pos + pending_bytes == (S->binlog ? S->binlog->info->file_size : 0));
  if ((S->binlog && S->binlog->info->log_pos == R->slice_binlog_pos) || R->update_generation != sigrtmax_cnt) {
    // have reached end of "last" binlog slice, have to re-read file list from disk
    update_related_binlog (R);
  }

  if (((S->flags & RS_ZIPPED) && S->binlog)) {
    S->logrotate_pos = S->binlog->info->log_pos + get_orig_file_size (S->binlog->info);
    assert (!pending_bytes);
  }

  const long long next_start_pos = S->logrotate_pos + pending_bytes;

  kfs_file_handle_t next_binlog = open_binlog (R->kfs_replica, next_start_pos);
  if (!next_binlog) {
    S->flags |= RS_FATAL;
    return send_error (S->conn, S->handshake_id, S->session_id, R_ERROR_EBADSLICE, "next binlog slice for %s at position %lld not found on server", R->path, next_start_pos);
  }
  if (next_binlog->info->log_pos != next_start_pos) {
    S->flags |= RS_FATAL;
    return send_error (S->conn, S->handshake_id, S->session_id, R_ERROR_EBADSLICE, "next binlog slice for %s at position %lld not found on server (next slice starts at %lld instead)", R->path, next_start_pos, next_binlog->info->log_pos);
  }

  S->flags &= ~RS_ZIPPED;

  int start_data_bytes, next_start_bytes;
  if (next_binlog->info->flags & 16) {
    next_start_bytes = start_data_bytes = get_bz_header_size (next_binlog->info);
    S->flags |= RS_ZIPPED;
  } else {
    start_data_bytes = next_start_pos ? 36 : 1024;
    next_start_bytes = next_binlog->info->kfs_headers * 4096 + start_data_bytes;
    assert (next_start_bytes >= 36 && next_start_bytes <= 8192 + 1024);
  }

  char *slice_name = next_binlog->info->filename;
  char *slash = strrchr (slice_name, '/');
  if (slash) {
    slice_name = slash + 1;
  }
  assert (slice_name[0]);
  int slice_name_len = strlen (slice_name);
  assert (slice_name_len < 128);

  int packet_size = sizeof (struct repl_rotate) + (slice_name_len | 3) + 1 + pending_bytes + next_start_bytes;
  struct repl_rotate *RT = alloc_packet (packet_size);

  RT->type = P_REPL_ROTATE;
  RT->handshake_id = S->handshake_id;
  RT->session_id = S->session_id;
  RT->flags = 0;
  RT->A1.headers_size = 0;
  RT->A1.data_size = pending_bytes;
  RT->A1.binlog_slice_start_pos = S->binlog ? S->binlog->info->log_pos : -1;
  RT->A1.binlog_slice_cur_pos = S->logrotate_pos;
  RT->A2.headers_size = next_start_bytes - start_data_bytes;
  RT->A2.data_size = start_data_bytes;
  RT->A2.binlog_slice_start_pos = next_start_pos;
  RT->A2.binlog_slice_cur_pos = next_start_pos;
  RT->binlog_slice2_name_len = slice_name_len;
  memcpy (RT->binlog_slice2_name, slice_name, slice_name_len + 1);
  int x = slice_name_len + 1;
  char *ptr = RT->binlog_slice2_name + x;
  while (x & 3) {
    *ptr++ = 0;
    x++;
  }
  int size = pending_bytes;
  while (size > 0) {
    long l = (S->rptr <= S->wptr ? S->wptr : S->buffer + S->buffer_size) - S->rptr;
    if (l > size) {
      l = size;
    }
    assert (l > 0);
    memcpy (ptr, S->rptr, l);
    size -= l;
    S->rptr += l;
    ptr += l;
    if (S->rptr >= S->buffer + S->buffer_size) {
      S->rptr -= S->buffer_size;
      assert (S->rptr == S->buffer);
      if (S->wptr >= S->buffer + S->buffer_size) {
        S->wptr -= S->buffer_size;
        assert (S->wptr == S->buffer);
      }
    }
  }
  assert (S->rptr == S->wptr);
  long r;
  if (next_binlog->info->flags & 16) {
    assert (next_binlog->info->start);
    assert (next_start_bytes <= next_binlog->info->preloaded_bytes);
    memcpy (ptr, next_binlog->info->start, next_start_bytes);
    r = next_start_bytes;
  } else {
    r = pread (next_binlog->fd, ptr, next_start_bytes, 0);
    if (r < 0) {
      fprintf (stderr, "error reading first %d bytes of binlog file %s: %m\n", next_start_bytes, next_binlog->info->filename);
      exit (1);
    }
    tot_read_bytes += r;
    vkprintf (2, "reading first %d bytes of next binlog file %s (%ld bytes read)\n", next_start_bytes, next_binlog->info->filename, r);
  }

  close_binlog (S->binlog, 1);
  S->binlog = next_binlog;

  if (next_start_pos) {
    struct lev_rotate_from *RotateFrom = (struct lev_rotate_from *) (
      (next_binlog->info->flags & 16) ?
      ((kfs_binlog_zip_header_t * ) next_binlog->info->start)->first36_bytes :
      (ptr + next_start_bytes - start_data_bytes));
    assert (r == next_start_bytes);
    assert (RotateFrom->type == LEV_ROTATE_FROM);
    assert (RotateFrom->cur_log_pos == next_start_pos);
  } else {
    if (!(next_binlog->info->flags & 16)) {
      struct lev_start *Start = (struct lev_start *) (ptr + next_start_bytes - start_data_bytes);
      assert (r <= next_start_bytes && r >= next_start_bytes - start_data_bytes + 24);
      assert (Start->type == LEV_START);
      assert ((unsigned) Start->extra_bytes <= 1000);
      int St_size = 24 + ((Start->extra_bytes + 3) & -4);
      assert (next_start_bytes - start_data_bytes + St_size <= r);
      int drop_start_bytes = start_data_bytes - St_size;
      assert (drop_start_bytes >= 0 && drop_start_bytes < start_data_bytes && !(drop_start_bytes & 3));
      next_start_bytes -= drop_start_bytes;
      packet_size -= drop_start_bytes;
      RT->A2.data_size = start_data_bytes -= drop_start_bytes;
    }
  }

  S->client_log_pos = next_start_pos + start_data_bytes;
  S->buffer_file_pos = next_start_bytes;
  S->flags &= ~(RS_LOGROTATE | RS_ALLSENT);
  S->flags |= RS_LOADDISK;
  S->logrotate_pos = -1;

  data_bytes_sent += RT->A1.data_size + RT->A2.headers_size + RT->A2.data_size;

  vkprintf (1, "Server is sending rotate packet to %s:%d, session %d:%d, slice name %s, %d+(%d+%d) bytes, slice 1 pos %lld..%lld, slice2 pos %lld..%lld\n",
	    show_remote_ip (S->conn), S->conn->remote_port, S->handshake_id, S->session_id,
	    RT->binlog_slice2_name, RT->A1.data_size, RT->A2.headers_size, RT->A2.data_size,
	    RT->A1.binlog_slice_start_pos, RT->A1.binlog_slice_cur_pos, RT->A2.binlog_slice_start_pos, RT->A2.binlog_slice_cur_pos);

  push_packet (S->conn, packet_size);

  rotate_packets_sent++;

  return 0;
}

int process_server_time (struct repl_server_status *S) {
  if (!(S->flags & RS_REPL_STARTED) || !S->buffer) {
    return 0;
  }
  struct related_binlog *R = S->rb;

  if (S->conn->Out.total_bytes > MAX_PENDING_BYTES_THRESHOLD) {
    static int last_spam;
    vkprintf (now / NOTSENT_SPAM_INTERVAL != last_spam ? 0 : 2, 
	      "Server is NOT sending server time packet to %s:%d, session %d:%d, position %lld, time %016llx; %d bytes in output buffer (%lld such errors)\n", 
	      show_remote_ip (S->conn), S->conn->remote_port, S->handshake_id, S->session_id,
	      R->engine_log_pos, R->engine_time, S->conn->Out.total_bytes, posinfo_packets_not_sent);
    posinfo_packets_not_sent++;
    last_spam = now / NOTSENT_SPAM_INTERVAL;
    return 1;
  }
    
  struct repl_pos_info *RP = alloc_packet (sizeof (struct repl_pos_info));
  RP->type = P_REPL_POS_INFO;
  RP->handshake_id = S->handshake_id;
  RP->session_id = S->session_id;
  RP->binlog_pos = R->engine_log_pos;
  RP->binlog_time = R->engine_time;
  posinfo_packets_sent++;
  vkprintf (2, "Server is sending server time packet to %s:%d, session %d:%d, position %lld, time %016llx; %d bytes in output buffer\n", 
	    show_remote_ip (S->conn), S->conn->remote_port, S->handshake_id, S->session_id,
	    RP->binlog_pos, RP->binlog_time, S->conn->Out.total_bytes);
  push_packet (S->conn, sizeof (struct repl_pos_info));

  return 0;
}


int process_all_servers (void) {
  int i;
  servers_readall = servers_sentall = 0;
  for (i = 0; i < local_rbs; i++) {
    struct related_binlog *R = &LR[i];
    if ((R->flags & (LRF_MASTER | LRF_BROKEN)) == LRF_MASTER) {
      struct repl_server_status *S, *NS;
      for (S = R->server_first; S != (struct repl_server_status *) R; S = NS) {
        NS = S->server_next;
        if (S->conn->generation != S->conn_generation || (S->flags & RS_FATAL) != 0) {
          destroy_server (S);
          continue;
        }

        process_server (S);
        process_server_rotate (S);
	if (R->flags & LRF_CHANGED_TIME) {
	  process_server_time (S);
	}

        if (S->flags & RS_ALLREAD) {
          servers_readall++;
        }
        if (S->flags & RS_ALLSENT) {
          servers_sentall++;
        }
      }
      R->flags &= ~LRF_CHANGED_TIME;
    }
    int j;
    for (j = 0; j < R->targets; j++) {
      if (R->client[j]) {
        struct repl_client_status *C = R->client[j];
        if (C->conn->generation != C->conn_generation) {
          destroy_client (C);
        } else {
          check_flush_client (C);
        }
      }
    }
  }
  return 0;
}

/*
 *
 *	MULTIPROCESS STATISTICS
 *
 */

static void update_rb_stats_copy (struct related_binlog *R, struct related_binlog_status *S, int status) {
  S->cnt++;
  __sync_synchronize();
  S->updated_at = now;
#define UPD(x)	S->x = R->x;
  UPD (flags);
  S->status = status;
#undef UPD
  __sync_synchronize();
  S->cnt++;
  __sync_synchronize();
}

static void update_rb_stats (struct related_binlog *R) {
  if (R->status) {
    int st = classify_slave (R);
    update_rb_stats_copy (R, R->status, st);
    update_rb_stats_copy (R, R->status + 1, st);
  }
}

static void update_local_stats_copy (struct worker_stats *S, int slave_cnt[4]) {
  S->cnt++;
  __sync_synchronize();
  S->updated_at = now;
#define UPD(x)	S->x = x;
  UPD (conf_master_binlogs);
  UPD (conf_slave_binlogs);
  UPD (broken_master_binlogs);
  UPD (broken_slave_binlogs);
  UPD (active_repl_servers);
  UPD (active_repl_clients);
  UPD (servers_sentall);
  UPD (slave_cnt[0]);
  UPD (slave_cnt[1]);
  UPD (slave_cnt[2]);
  UPD (slave_cnt[3]);
  UPD (errors_sent);
  UPD (errors_received);
  UPD (data_packets_sent);
  UPD (data_packets_received);
  UPD (rotate_packets_sent);
  UPD (rotate_packets_received);
  UPD (unknown_packets_received);
  UPD (data_bytes_sent);
  UPD (data_bytes_received);
  UPD (tot_read_bytes);
  UPD (tot_unflushed_bytes);
  UPD (posinfo_packets_sent);
  UPD (posinfo_packets_not_sent);
  UPD (posinfo_packets_received);
#undef UPD
  __sync_synchronize();
  S->cnt++;
  __sync_synchronize();
}

static inline void add_stats (struct worker_stats *W) {
#define UPD(x)	SumStats.x += W->x;
  UPD (conf_master_binlogs);
  UPD (conf_slave_binlogs);
  UPD (broken_master_binlogs);
  UPD (broken_slave_binlogs);
  UPD (active_repl_servers);
  UPD (active_repl_clients);
  UPD (servers_sentall);
  UPD (slave_cnt[0]);
  UPD (slave_cnt[1]);
  UPD (slave_cnt[2]);
  UPD (slave_cnt[3]);
  UPD (errors_sent);
  UPD (errors_received);
  UPD (data_packets_sent);
  UPD (data_packets_received);
  UPD (rotate_packets_sent);
  UPD (rotate_packets_received);
  UPD (unknown_packets_received);
  UPD (data_bytes_sent);
  UPD (data_bytes_received);
  UPD (tot_read_bytes);
  UPD (tot_unflushed_bytes);
  UPD (posinfo_packets_sent);
  UPD (posinfo_packets_not_sent);
  UPD (posinfo_packets_received);
#undef UPD
}

void update_local_stats (void) {
  if (!slave_mode) {
    return;
  }
  static int slave_cnt[4];
  compute_slaves_stats (slave_cnt);

  update_local_stats_copy (WStats + worker_id * 2, slave_cnt);
  update_local_stats_copy (WStats + worker_id * 2 + 1, slave_cnt);
}

void compute_stats_sum (void) {
  if (!workers) {
    return;
  }
  memset (&SumStats, 0, sizeof (SumStats));
  int i;
  for (i = 0; i < workers; i++) {
    static struct worker_stats W;
    struct worker_stats *F;
    int s_cnt;
    do {
      F = WStats + i * 2;
      do {
        s_cnt = (++F)->cnt;
        if (!(s_cnt & 1)) {
          break;
        }
        s_cnt = (--F)->cnt;
      } while (s_cnt & 1);
      memcpy (&W, F, sizeof (W));
    } while (s_cnt != F->cnt);
    add_stats (&W);
  }
}

/*
 *
 *	REPLICATOR RPC INTERFACE
 *
 */

int rpcs_execute (struct connection *c, int op, int len);
int rpcc_execute (struct connection *c, int op, int len);
int rpcc_ready (struct connection *c);
int rpc_check_ready (struct connection *c);

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

struct memcache_server_functions replicator_memcache_inbound = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_wakeup = mcs_do_wakeup,
  .mc_alarm = mcs_do_wakeup,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

struct rpc_server_functions replicator_rpc_server = {
  .execute = rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .memcache_fallback_type = &ct_memcache_server,
  .memcache_fallback_extra = &replicator_memcache_inbound
};

struct rpc_client_functions replicator_rpc_client = {
  .execute = rpcc_execute,
  .check_ready = rpc_check_ready,
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpcc_ready
};

int replicator_prepare_stats (void) {
//  int uptime = now - start_time;
  static int slave_cnt[4];
  compute_slaves_stats (slave_cnt);
  compute_stats_sum ();

#define S(x)	((x)+(SumStats.x))
  return stats_buff_len = snprintf (
    stats_buff, STATS_BUFF_SIZE,
    "heap_used\t%ld\n"
    "heap_max\t%ld\n"
    "conf_master_binlogs\t%d\n"
    "conf_slave_binlogs\t%d\n"
    "broken_master_binlogs\t%d\n"
    "broken_slave_binlogs\t%d\n"
    "active_replication_servers\t%d\n"
    "active_replication_clients\t%d\n"
    "servers_uptodate\t%d\n"
    "slaves_broken_notconn_late_uptodate\t%d %d %d %d\n"
    "worker_processes\t%d\n"
    "monitor_priority\t%d\n"
    "monitor_pid\t%d\n"
    "monitor_am\t%d\n"
    "last_session_id\t%d\n"
    "last_handshake_id\t%d\n"
    "errors_sent\t%lld\n"
    "errors_received\t%lld\n"
    "data_packets_sent\t%lld\n"
    "data_packets_received\t%lld\n"
    "rotate_packets_sent\t%lld\n"
    "rotate_packets_received\t%lld\n"
    "posinfo_packets_sent\t%lld\n"
    "posinfo_packets_not_sent\t%lld\n"
    "posinfo_packets_received\t%lld\n"
    "unknown_packets_received\t%lld\n"
    "data_bytes_sent\t%lld\n"
    "data_bytes_received\t%lld\n"
    "total_bytes_read\t%lld\n"
    "unsynced_bytes\t%lld\n"
    "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n",
    (long) (dyn_cur - dyn_first),
    (long) (dyn_last - dyn_first),
    S(conf_master_binlogs), S(conf_slave_binlogs), S(broken_master_binlogs), S(broken_slave_binlogs),
    S(active_repl_servers), S(active_repl_clients), S(servers_sentall),
    S(slave_cnt[0]), S(slave_cnt[1]), S(slave_cnt[2]), S(slave_cnt[3]),
    workers,
    monitor_priority, get_monitor_pid (), am_monitor,
    last_session_id, last_handshake_id,
    S(errors_sent), S(errors_received),
    S(data_packets_sent), S(data_packets_received),
    S(rotate_packets_sent), S(rotate_packets_received), 
    S(posinfo_packets_sent), S(posinfo_packets_not_sent), S(posinfo_packets_received), 
    S(unknown_packets_received),
    S(data_bytes_sent), S(data_bytes_received),
    S(tot_read_bytes), S(tot_unflushed_bytes)
  );
#undef S
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  return -2;
}

int prepare_slaves_stats (char *buffer, int mask) {
  int i;
  char *ptr = buffer;
  static char *status_strings[] = {"broken", "notconn", "late", "uptodate", "master"};
  for (i = 0; i < orig_local_rbs; i++) {
    int type = LR[i].status->status;
    if ((mask & (1 << type))) {
      int j = sprintf (ptr, "%d\t%s\t%s\n", i, type[status_strings], LR[i].path);
      ptr += j;
    }
  }
  return ptr - buffer;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = replicator_prepare_stats ();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, key, stats_buff, len + len2);
    return 0;
  }
  if (key_len >= 13 && !strncmp (key, "slaves_broken", 13)) {
    return_one_key (c, key, stats_buff, prepare_slaves_stats (stats_buff, 1));
    return 0;
  }
  if (key_len >= 14 && !strncmp (key, "slaves_notconn", 14)) {
    return_one_key (c, key, stats_buff, prepare_slaves_stats (stats_buff, 2));
    return 0;
  }
  if (key_len >= 11 && !strncmp (key, "slaves_late", 11)) {
    return_one_key (c, key, stats_buff, prepare_slaves_stats (stats_buff, 4));
    return 0;
  }
  if (key_len >= 15 && !strncmp (key, "slaves_uptodate", 15)) {
    return_one_key (c, key, stats_buff, prepare_slaves_stats (stats_buff, 8));
    return 0;
  }
  if (key_len >= 10 && !strncmp (key, "slaves_all", 10)) {
    return_one_key (c, key, stats_buff, prepare_slaves_stats (stats_buff, -1));
    return 0;
  }
  if (key_len >= 10 && !strncmp (key, "slaves_bad", 10)) {
    return_one_key (c, key, stats_buff, prepare_slaves_stats (stats_buff, ~24));
    return 0;
  }
  return 0;
}

int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg) {
  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_stats (struct connection *c) {
  int len = replicator_prepare_stats ();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int rpc_check_ready (struct connection *c) {
  double first_query_time = 0;
  double adj_precise_now = precise_now;

  if (c->status == conn_connecting || (c->last_response_time == 0 && (c->status == conn_wait_answer || c->status == conn_reading_answer)) ||  RPCS_DATA(c)->in_packet_num < 0) {
    if (c->last_query_sent_time < adj_precise_now - CONNECT_TIMEOUT) {
      fail_connection (c, -6);
      return cr_failed;
    }
    return cr_notyet;
  }
  if (c->last_query_sent_time < adj_precise_now - ping_interval) {
    net_rpc_send_ping (c, lrand48 ());
    c->last_query_sent_time = precise_now;
  }

  if (c->last_response_time < adj_precise_now - FAIL_INTERVAL || c->unreliability > 5000) {
    if (verbosity > 1 && c->ready != cr_failed) {
      fprintf (stderr, "changing connection %d readiness from %d to %d [FAILED] fq=%.03f lq=%.03f lqt=%.03f lr=%.03f now=%.03f unr=%d\n", c->fd, c->ready, cr_failed, first_query_time, c->last_query_sent_time, c->last_query_timeout, c->last_response_time, precise_now, c->unreliability);
    }
    fail_connection (c, -5);
    return c->ready = cr_failed;
  }

  if (c->last_response_time < adj_precise_now - STOP_INTERVAL || c->unreliability > 2000) {
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

int rpcs_execute (struct connection *c, int op, int len) {
  if (verbosity > 0) {
    fprintf (stderr, "rpcs_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);
  }
  if (verbosity > 1) {
    dump_next_rpc_packet_limit (c, 64);
  }

  if (len > MAX_PACKET_INTS * 4) {
    fprintf (stderr, "rpcs_execute: received too long packet from connection %d : type=%08x len=%d\n", c->fd, op, len);
    unknown_packets_received++;
    return SKIP_ALL_BYTES;
  }

  assert (read_in (&c->In, &recv_packet, len) == len);

  switch (op) {
  case P_REPL_HANDSHAKE:
    if (len >= 12 + sizeof (struct repl_handshake)) {
      process_handshake_packet (c, (struct repl_handshake *) (recv_packet + 2), len - 12);
      return 0;
    }
    break;
  case P_REPL_ERROR:
    if (len >= 16 + sizeof (struct repl_error)) {
      process_server_error_packet (c, (struct repl_error *) (recv_packet + 2), len - 12);
      return 0;
    }
    break;
  case P_REPL_DATA_ACK:
    if (len == 12 + sizeof (struct repl_data_ack)) {
      process_data_ack_packet (c, (struct repl_data_ack *) (recv_packet + 2));
      return 0;
    }
    break;
  }

  unknown_packets_received++;
  return 0;
}

int rpcc_execute (struct connection *c, int op, int len) {
  c->last_response_time = precise_now;
  if (op == RPC_PONG) { return SKIP_ALL_BYTES; }
  if (verbosity > 0) {
    fprintf (stderr, "rpcc_execute: fd=%d, op=%d, len=%d\n", c->fd, op, len);
  }
  if (verbosity > 1) {
    dump_next_rpc_packet_limit (c, 64);
  }

  if (op == P_REPL_DATA && len >= 12 + sizeof (struct repl_data)) {
    assert (read_in (&c->In, &recv_packet, 8 + sizeof (struct repl_data)) == 8 + sizeof (struct repl_data));
    int res = process_data_packet (c, (struct repl_data *) (recv_packet + 2), len - 8 - sizeof (struct repl_data));
    if (res < 0) {
      assert (advance_skip_read_ptr (&c->In, len - 8 - sizeof (struct repl_data)) == len - 8 - sizeof (struct repl_data));
    }

    return 0;
  }

  if (len > MAX_PACKET_INTS * 4) {
    fprintf (stderr, "rpcc_execute: received too long packet from connection %d : type=%08x len=%d\n", c->fd, op, len);
    unknown_packets_received++;
    return SKIP_ALL_BYTES;
  }

  assert (read_in (&c->In, &recv_packet, len) == len);

  switch (op) {
  case P_REPL_HANDSHAKE_OK:
    if (len == 12 + sizeof (struct repl_handshake_ok)) {
      process_handshake_ok_packet (c, (struct repl_handshake_ok *) (recv_packet + 2));
      return 0;
    }
    break;
  case P_REPL_ERROR:
    if (len >= 16 + sizeof (struct repl_error)) {
      process_client_error_packet (c, (struct repl_error *) (recv_packet + 2), len - 12);
      return 0;
    }
    break;
  case P_REPL_ROTATE:
    if (len >= 12 + sizeof (struct repl_rotate)) {
      process_rotate_packet (c, (struct repl_rotate *) (recv_packet + 2), len - 12);
      return 0;
    }
    break;
  case P_REPL_POS_INFO:
    if (len == 12 + sizeof (struct repl_pos_info)) {
      process_pos_info_packet (c, (struct repl_pos_info *) (recv_packet + 2));
      return 0;
    }
    break;
  }
  unknown_packets_received++;
  return 0;
}

int rpcc_ready (struct connection *c) {
  int i;
  assert (c->target);
  vkprintf (1, "outbound rpc connection %d for target %p becomes ready\n", c->fd, c->target);
  for (i = 0; i < local_rbs; i++) {
    if (!(LR[i].flags & (LRF_MASTER | LRF_BROKEN)))  {
      int j;
      for (j = 0; j < LR[i].targets; j++) {
        if (!LR[i].client[j] && LR[i].target[j] == c->target) {
          vkprintf (3, "%s: i = %d, j = %d. Before call init_client_session.\n", __func__, i, j);
          init_client_session (&LR[i], c, j);
        }
      }
    }
  }
  return 0;
}

/*
 *
 *	MAIN
 *
 */

void reopen_logs (void) {
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2 (fd, 0);
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2)
      close (fd);
  }
  if (logname && (fd = open (logname, O_WRONLY | O_APPEND | O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2)
      close (fd);
  }
  if (verbosity > 0 && !slave_mode) {
    fprintf (stderr, "logs reopened.\n");
  }
}


static void check_children_dead (void) {
  int i, j;
  for (j = 0; j < 11; j++) {
    for (i = 0; i < workers; i++) {
      if (pids[i]) {
        int status = 0;
        int res = waitpid (pids[i], &status, WNOHANG);
        if (res == pids[i]) {
          if (WIFEXITED (status) || WIFSIGNALED (status)) {
            pids[i] = 0;
          } else {
            break;
          }
        } else if (res == 0) {
          break;
        } else if (res != -1 || errno != EINTR) {
          pids[i] = 0;
        } else {
          break;
        }
      }
    }
    if (i == workers) {
      break;
    }
    if (j < 10) {
      usleep (100000);
    }
  }
  if (j == 11) {
    int cnt = 0;
    for (i = 0; i < workers; i++) {
      if (pids[i]) {
        ++cnt;
        kill (pids[i], SIGKILL);
      }
    }
    kprintf ("WARNING: %d children unfinished --> they are now killed\n", cnt);
  }
}


static void kill_children (int signal) {
  int i;
  assert (workers);
  for (i = 0; i < workers; i++) {
    if (pids[i]) {
      kill (pids[i], signal);
    }
  }
}

static void sigint_immediate_handler (const int sig) {
  static const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  static const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigint_handler (const int sig) {
  static const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  pending_signals |= 1 << sig;
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  static const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  pending_signals |= 1 << sig;
  signal (sig, sigterm_immediate_handler);
}

volatile int sighup_cnt = 0, sigusr1_cnt = 0;

static void sighup_handler (const int sig) {
  static const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  sighup_cnt++;
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  static const char message[] = "got SIGUSR1.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  sigusr1_cnt++;
  signal (SIGUSR1, sigusr1_handler);
}

static void sigrtmax_handler (const int sig) {
  static const char message[] = "got SIGUSR3, incr update related binlog generation number.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  sigrtmax_cnt++;
}


void check_children_status (void) {
  if (workers) {
    int i;
    for (i = 0; i < workers; i++) {
      int status = 0;
      int res = waitpid (pids[i], &status, WNOHANG);
      if (res == pids[i]) {
        if (WIFEXITED (status) || WIFSIGNALED (status)) {
          kprintf ("Child %d terminated, aborting\n", pids[i]);
          pids[i] = 0;
          kill_children (SIGTERM);
          check_children_dead ();
          exit (EXIT_FAILURE);
        }
      } else if (res == 0) {
      } else if (res != -1 || errno != EINTR) {
        kprintf ("Child %d: unknown result during wait (%d, %m), aborting\n", pids[i], res);
        pids[i] = 0;
        kill_children (SIGTERM);
        check_children_dead ();
        exit (EXIT_FAILURE);
      }
    }
  } else if (slave_mode) {
    if (getppid () != parent_pid) {
      kprintf ("Parent %d is changed to %d, aborting\n", parent_pid, getppid ());
      exit (EXIT_FAILURE);
    }
  }
}

struct related_binlog *binlog_matches[CDATA_PIDS + 1];
long long binlog_hashes[CDATA_PIDS + 1];


void match_binlogs (void) {
  int i, j;
  if (!CData) {
    return;
  }
  for (i = 0; i < active_pnum; i++) {
    static struct proc_user_info PData;
    int res = fetch_process_data (&PData, active_pids[i]);
    if (res == 2) {
      if (binlog_hashes[i] != PData.binlog_name_hash) {
	binlog_hashes[i] = PData.binlog_name_hash;
	for (j = 0; j < local_rbs; j++) {
	  if (LR[j].replica_name_hash == binlog_hashes[i]) {
	    break;
	  }
	}
	binlog_matches[i] = (j == local_rbs) ? 0 : &LR[j];
      }
      if (binlog_matches[i] && PData.binlog_pos) {
	long long engine_time = binlog_matches[i]->engine_time;
	if (/* binlog_matches[i]->engine_log_pos != PData.binlog_pos || */
	    engine_time > PData.updated_at || engine_time < PData.updated_at - MIN_TIME_UPDATE_INTERVAL) {
	  binlog_matches[i]->engine_log_pos = PData.binlog_pos;
	  binlog_matches[i]->engine_time = PData.updated_at;
	  binlog_matches[i]->flags |= LRF_CHANGED_TIME;
	}
      }
    }
  }
}


void cron (void) {
  rescan_pid_table ();
  create_all_outbound_connections ();
  check_children_status ();
  // update_local_stats ();
}

int sfd;

void start_server (void) {
  //  struct sigaction sa;
  char buf[64];
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (!slave_mode) {
    if (!sfd) {
      sfd = server_socket (port, settings_addr, backlog, enable_ipv6);
    }

    if (sfd < 0) {
      fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
      exit(3);
    }

    if (verbosity) {
      fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, buf), port, sfd);
    }
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  if (!slave_mode) {
    init_listening_tcpv6_connection (sfd, &ct_rpc_server, &replicator_rpc_server, enable_ipv6);
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }
  signal (SIGRTMAX, sigrtmax_handler);

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (20);

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (pending_signals) {
      break;
    }

    if (__sync_fetch_and_and (&sigusr1_cnt, 0)) {
      reopen_logs ();
      if (workers) {
        kill_children (SIGUSR1);
      }
    }

    load_all_data ();
    match_binlogs ();
    process_all_servers ();
    update_local_stats ();

    if (!slave_mode) {
      monitor_work ();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (workers) {
    if (pending_signals & (1 << SIGTERM)) {
      kill_children (SIGTERM);
    }
    check_children_dead ();
  }

  if (!slave_mode) {
    vkprintf (1, "Running sync\n");
    sync ();
  }

  epoll_close (sfd);
  close(sfd);

  kprintf ("Replicator says goodbye\n");
}

/*
 *
 *		MAIN
 *
 */

int detect_hostname (void) {
  int r, i;
  if (!hostname || !*hostname) {
    hostname = getenv ("HOSTNAME");
    if (!hostname || !*hostname) {
      int fd = open ("/etc/hostname", O_RDONLY);
      if (fd < 0) {
        fprintf (stderr, "cannot read /etc/hostname: %m\n");
        exit (2);
      }
      r = read (fd, hostname_buffer, 256);
      if (r <= 0 || r >= 256) {
        fprintf (stderr, "cannot read hostname from /etc/hostname: %d bytes read\n", r);
        exit (2);
      }
      hostname_buffer[r] = 0;
      close (fd);
      hostname = hostname_buffer;
      while (*hostname == 9 || *hostname == 32) {
        hostname++;
      }
      i = 0;
      while (hostname[i] > 32) {
        i++;
      }
      hostname[i] = 0;
    }
  }
  if (!hostname || !*hostname) {
    fprintf (stderr, "fatal: cannot detect hostname\n");
    exit (2);
  }
  i = 0;
  while ((hostname[i] >= '0' && hostname[i] <= '9') || hostname[i] == '.' || hostname[i] == '-' || hostname[i] == '_' || (hostname[i] >= 'A' && hostname[i] <= 'Z') || (hostname[i] >= 'a' && hostname[i] <= 'z')) {
    i++;
  }
  if (hostname[i] || i >= 64) {
    fprintf (stderr, "fatal: bad hostname '%s'\n", hostname);
    exit (2);
  }
  if (verbosity > 0) {
    fprintf (stderr, "hostname is %s\n", hostname);
  }
  return 0;
}


void usage (void) {
  printf ("usage: %s [-6] [-v] [-r] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-l<log-name>] [-P <passfile>] [-M <workers>] [-q<monitor-priority>] [-T<ping-interval>] <configuration-file>\n"
  	  "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
	  "64-bit"
#else
	  "32-bit"
#endif
	  " after commit " COMMIT "\n"
	  "\tActs as a binlog replication server/client based on layout specified in configuration file.\n"
	  "\t-6\tenable IPv6\n"
	  "\t-C\tsyntax check only\n"
	  "\t-L\tcheck all locations, not only related to this host\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-r\tact as server only (do not write anything on disk)\n"
	  "\t-M<workers>\tcreate additional worker processes (only slave)\n"
	  "\t-q<priority>\tset monitor priority (0..3), default 1\n"
	  "\t-Z<heap-size>\tdefines maximum heap size\n"
	  "\t-P\tpath to AES key file\n"
          "\t-D\tenable double send/receive mode\n"
          "\t-T\tsets ping interval (default %0.3lf)\n"
    ,
	  progname, 
    PING_INTERVAL
    );
  exit(2);
}

int main (int argc, char *argv[]) {
  int i, syntax_check = 0;
  long long x;
  char c;

  set_debug_handlers ();
  srand48 (rdtsc());

  progname = argv[0];
  while ((i = getopt (argc, argv, "6CDH:LM:P:T:Z:b:c:dhl:p:q:ru:v")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'C':
      syntax_check = 1;
      break;
    case 'D':
      double_send_recv = 1;
      break;
    case 'L':
      check_all_locations = 1;
      break;
    case 'b':
      backlog = atoi (optarg);
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
    case 'p':
      port = atoi(optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'r':
      binlog_disabled++;
      break;
    case 'd':
      daemonize ^= 1;
      break;
    case 'P':
      aes_pwd_file = optarg;
      break;
    case 'H':
      hostname = optarg;
      break;
    case 'M':
      workers = atoi (optarg);
      assert (workers >= 0);
      break;
    case 'Z':
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
    case '6':
      enable_ipv6 = SM_IPV6;
      break;
    case 'q':
      monitor_priority = atoi (optarg) & 3;
      break;
    case 'T':
      ping_interval = atof (optarg);
      if (ping_interval <= 0) {
        ping_interval = PING_INTERVAL;
      }
      break;
    default:
      assert (0);
    }
  }
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  PID.port = port;
  default_ct.port = port;
  default_target_port = port;

  detect_hostname ();
  hostname_len = strlen (hostname);

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  if (port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, enable_ipv6);
    if (sfd < 0) {
      fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  init_dyn_data();

  config_filename = argv[optind];

  open_file (0, config_filename, 0);
  i = parse_config (0);

  close (fd[0]);

  if (i < 0) {
    fprintf (stderr, "config check failed!\n");
    return -i;
  }

  if (verbosity > 0) {
    fprintf (stderr, "config loaded!\n");
  }

  if (workers > local_rbs) {
    workers = local_rbs;
  }

  if (syntax_check) {
    return 0;
  }

  if (!local_rbs) {
    fprintf (stderr, "nothing to do: no related binlogs found in %s for hostname %s, exiting\n", config_filename, hostname);
    exit (0);
  }

  if (daemonize && !workers) {
    setsid();
  }

  orig_local_rbs = local_rbs;

  RBStats = mmap (0, 2 * local_rbs * sizeof (struct related_binlog_status), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
  assert (RBStats);

  for (i = 0; i < local_rbs; i++) {
    LR[i].status = RBStats + 2 * i;
  }

  if (workers) {
    if (!kdb_hosts_loaded) {
      kdb_load_hosts ();
    }
    for (i = 0; i < local_rbs; i++) {
      assert (!(LR[i].binlog->flags & 4));
    }
    WStats = mmap (0, 2 * workers * sizeof (struct worker_stats), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    assert (WStats);
    kprintf_multiprocessing_mode_enable ();
    vkprintf (0, "creating %d workers\n", workers);
    for (i = 0; i < workers; i++) {
      int pid = fork ();
      assert (pid >= 0);
      if (!pid) {
        int j = i, k = 0;
        while (j < local_rbs) {
          memcpy (LR + k, LR + j, sizeof (struct related_binlog));
          j += workers;
          k++;
        }
        worker_id = i;
        local_rbs = k;
        workers = 0;
        slave_mode = 1;
        parent_pid = getppid ();
        assert (parent_pid > 1);
        break;
      } else {
        pids[i] = pid;
      }
    }
  }

  aes_load_pwd_file (aes_pwd_file);

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (check_binlog_files () <= 0 && !workers) {
    fprintf (stderr, "fatal: do not have needed binlog files\n");
    exit (3);
  }

  if (workers) {
    local_rbs = 0;
  }

  init_common_data (0, 0);

  start_time = time(0);

  if (!slave_mode) {
    init_monitor (monitor_priority);
  }
  rescan_pid_table ();

  last_handshake_id = lrand48 ();
  last_session_id = lrand48 ();

  start_server();

  return 0;
}
