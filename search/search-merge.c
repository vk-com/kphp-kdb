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

    Copyright 2009-2010 Vkontakte Ltd
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
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

#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "estimate-split.h"

#define VERSION_STR "search-merge-0.1"

#define BACKLOG 8192
#define TCP_PORT 11211

#define MAX_NET_RES (1L << 16)

#define BUFF_SIZE 4096


/*
 *
 *    SEARCH ENGINE
 *
 */

int verbosity = 0, interactive = 0;

char *fnames[3];
int fd[3];
long long fsize[3];

// unsigned char is_letter[256];
char *progname = "search-merge", *username, *logname;
char metaindex_fname_buff[256];

/* stats counters */
int cache_miss;
int start_time;
long long binlog_loaded_size;
long long cache_misses, cache_hits, netw_queries, newmsg_queries, delmsg_queries, search_queries;
long long tot_response_words, tot_response_bytes;
double cache_missed_qt, cache_hit_qt, binlog_load_time;

#define STATS_BUFF_SIZE (16 << 10)
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

/* text->word->hash parser */

/*
void init_is_letter (void) {
  int i;

  memset (is_letter, 32, sizeof (is_letter));
  is_letter[0] = 0;

  for (i = 'A'; i <= 'Z'; i++) is_letter[i] = i + 32;
  for (i = 'a'; i <= 'z'; i++) is_letter[i] = i;
  is_letter[0xa8] = is_letter[0xb8] = 0xe5;
  for (i = 0xc0; i <= 0xdf; i++) is_letter[i] = i + 32;
  for (i = 0xe0; i <= 0xff; i++) is_letter[i] = i;
}
*/

/*
 *
 *  CONFIGURATION PARSER
 *
 */

#define MAX_CLUSTER_SERVERS 1024

struct cluster_server {
  int id;
  char *hostname;
  int port;
  struct in_addr addr;
  struct connection *c;
  int conn, rconn;
  int conn_retries;
  int reconnect_time;
};

struct cluster_server CS[MAX_CLUSTER_SERVERS];
int CSN;

#define MAX_CONFIG_SIZE (1 << 16)

char config_buff[MAX_CONFIG_SIZE+4], *config_filename, *cfg_start, *cfg_end, *cfg_cur;
int config_bytes, cfg_lno;

static int cfg_skipspc(void) {
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

static int cfg_skspc(void) {
  while (*cfg_cur == ' ' || *cfg_cur == 9) {
    cfg_cur++;
  }
  return (unsigned char) *cfg_cur;
}

static void syntax (const char *msg) __attribute__ ((noreturn));

static void syntax (const char *msg) {
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

  exit(2);
}

void parse_config (void) {
  int r, c;
  char *ptr, *s;
  struct cluster_server *D;
  struct hostent *h;
  config_bytes = r = read (fd[0], config_buff, MAX_CONFIG_SIZE+1);
  if (r < 0) {
    fprintf (stderr, "error reading configuration file %s: %m\n", config_filename);
    exit (2);
  }
  if (r > MAX_CONFIG_SIZE) {
    fprintf (stderr, "configuration file %s too long (max %d bytes)\n", config_filename, MAX_CONFIG_SIZE);
    exit (2);
  }
  cfg_cur = cfg_start = config_buff;
  cfg_end = cfg_start + r;
  *cfg_end = 0;
  cfg_lno = 0;

  CSN = 0;
  while (cfg_skipspc()) {
    ptr = s = cfg_cur;
    while ((*s >= 'a' && *s <= 'z') || (*s >= 'A' && *s <= 'Z') || (*s >= '0' && *s <= '9') || *s == '.' || *s == '-' || *s == '_') {
      s++;
    }
    if (s == ptr) {
      syntax("hostname expected");
      return;
    }
    if (s > ptr + 63) {
      syntax("hostname too long");
      return;
    }
    c = *s;
    *s = 0;
    if (CSN >= MAX_CLUSTER_SERVERS) {
      syntax("too many servers in cluster");
      return;
    }
    D = CS + CSN;
    memset (D, 0, sizeof (*D));
    D->id = CSN;
    D->hostname = ptr;
    D->port = -1;
    if (!(h = gethostbyname (ptr)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
      syntax("cannot resolve hostname");
      return;
    }
    D->addr = *((struct in_addr *) h->h_addr);

    cfg_cur = ptr = s;
    *s = c;
    if (cfg_skspc() != ':') {
      syntax("':' expected");
    }
    cfg_cur++;
    cfg_skspc();
    if (*cfg_cur < '0' || *cfg_cur > '9') {
      syntax("port number expected");
    }
    D->port = strtol (cfg_cur, &cfg_cur, 10);
    if (D->port <= 0 || D->port >= 0x10000) {
      syntax("port number out of range");
    }
    if (cfg_skspc() != ';') {
      syntax("';' expected");
    }
    cfg_cur++;

    CSN++;
    if (verbosity > 0) {
      fprintf (stderr, "server #%d: ip %s, port %d\n", CSN, inet_ntoa(D->addr), D->port);
    }
  }
  if (!CSN) {
    fprintf (stderr, "fatal: no cluster servers defined\n");
    exit(1);
  }
}

/*
 *
 *    SERVER
 *
 */

/* for connection state */
#define C_WANTRD  1
#define C_WANTWR  2
#define C_WANTRW  (C_WANTRD | C_WANTWR)
#define C_INCONN  4
#define C_ERROR   8
#define C_NORD    16
#define C_NOWR    32
#define C_NORW    (C_NORD | C_NOWR)
#define C_INQUERY 64

struct connection {
  int fd;
  int state;
  event_t *ev;
  struct connection *prev, *next;
  int change;
  int error;
  char *wra, *wre;
  void *obj;
  int obj_type;
  int generation;
  struct connection *master;
  int master_generation;
  int unread_res_bytes;
  struct cluster_server *serv;
  struct gather_data *gather;
  netbuffer_t *Tmp, In, Out;
  char in_buff[BUFF_SIZE];
  char out_buff[BUFF_SIZE];
};

struct connection *create_client (struct cluster_server *S);


struct connection Connections[MAX_CONNECTIONS];

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 1, is_server = 0;
struct in_addr settings_addr;
int active_connections;
int conn_generation;

char *conv_addr(in_addr_t a, char *buf) {
  sprintf (buf, "%d.%d.%d.%d", a&255, (a>>8)&255, (a>>16)&255, a>>24);
  return buf;
}

/* merge data structures */

#define GATHER_QUERY_TIMEOUT  5

#define CHUNK_INTS  60

struct gather_chunk {
  struct gather_chunk *next;
  int data[CHUNK_INTS];
};

struct gather_entry {
  int num;
  int res_bytes;
  int res_read;
  struct gather_chunk *first, *last;
};

#define GD_MAGIC 0x54781300
#define GD_MAGIC_MASK -0x100
#define GD_EXTRA  1
#define GD_DROP_EXTRA 2
#define GD_REVERSE  8
#define GD_RAW    4

struct gather_data {
  struct gather_data *prev, *next;
  struct connection *c;
  double start_time;
  nb_allocator_t Alloc;
  int magic;
  int tot_num;
  int wait_num;
  int ready_num;
  int error_num;
  int timeout_time;
  int limit;
  char *orig_key;
  int orig_key_len;
  char *new_key;
  int new_key_len;
  int sum;
  struct gather_entry List[1];
};

/* end (merge structures) */

static void free_gather (struct gather_data *G);

void free_connection_buffers (struct connection *c) {
  if (c->gather && c->gather->c == c) {
    free_gather (c->gather);
  }
  c->gather = 0;
  if (c->Tmp) {
    free_all_buffers (c->Tmp);
    c->Tmp = 0;
  }
  free_all_buffers (&c->In);
  free_all_buffers (&c->Out);
}

int server_writer (struct connection *c);

/*
 * client
 */

int outbound_connections, active_outbound_connections;

struct gather_data GDH = { .prev = &GDH, .next = &GDH };
int active_gathers;

static void free_gather (struct gather_data *G) {
  if (!G) {
    return;
  }
  assert ((G->magic & GD_MAGIC_MASK) == GD_MAGIC);
  G->magic = 0;
  assert (active_gathers > 0);
  G->prev->next = G->next;
  G->next->prev = G->prev;
  active_gathers--;
}

struct gather_data *get_client_gather (struct connection *c) {
  if (c->unread_res_bytes && c->gather && c->master && c->gather->c == c->master && c->master->generation == c->master_generation) {
    return c->gather;
  }
  if (verbosity > 0 && c->unread_res_bytes && c->gather) {
    fprintf (stderr, "connection %d: orphaned by master connection %d\n", c->fd, c->master ? c->master->fd : -1);
  }
  c->gather = 0;
  c->master = 0;
  c->master_generation = 0;
  return 0;
}


int client_execute (struct connection *c, char *cmd, int len) {
  char *ptr = cmd, *to = cmd + len;
  char *key;
  int key_len;
  char *tp;
  int flags, data_len;
  struct gather_data *G;
  int i;

  if (verbosity > 0) {
    fprintf (stderr, "client #%d: got command '%s'\n", c->fd, cmd);
  }

  if (!len) {
    return 0;
  }

  if (!strcmp (cmd, "END")) {
    return 0;
  }

  if (!strncmp (cmd, "VALUE ", 6)) {
    ptr += 6;
    while (ptr < to && *ptr == ' ') {
      ptr++;
    }
    if (ptr >= to) {
      return 0;
    }
    key = ptr;
    while (ptr < to && (unsigned char) *ptr > ' ') {
      ptr++;
    }
    if (ptr >= to) {
      return 0;
    }
    key_len = ptr - key;

    if (*ptr != ' ') {
      return -1;
    }

    while (*ptr == ' ') {
      ptr++;
    }

    flags = strtol (ptr, &tp, 10);
    if (tp == ptr || *tp != ' ') {
      return -1;
    }

    ptr = tp;
    while (*ptr == ' ') {
      ptr++;
    }

    data_len = strtol (ptr, &tp, 10);
    if (tp == ptr) {
      return -1;
    }

    ptr = tp;
    while (*ptr == ' ') {
      ptr++;
    }

    if (*ptr || data_len <= 0) {
      return -1;
    }

    c->unread_res_bytes = data_len;
    c->master = 0;
    c->gather = 0;
    key[key_len] = 0;

    if (verbosity > 0) {
      fprintf (stderr, "client socket #%d: receiving key '%s', flags=%d, data bytes=%d\n", c->fd, key, flags, data_len);
    }

    if (data_len < 4 || data_len > (8 << 20) || ((data_len & 3) && data_len > 2048)) {
      return -1;
    }

    i = c->serv->id;
    assert (i >= 0 && i < CSN);

    for (G = GDH.next; G != &GDH; G = G->next) {
      if (G->new_key_len == key_len && !memcmp (G->new_key, key, key_len) && i < G->tot_num && !G->List[i].res_bytes) {
  G->List[i].res_bytes = data_len;
  c->master = G->c;
  c->master_generation = G->c->generation;
  c->gather = G;
  assert (G->c->gather == G);
  if (verbosity > 0) {
    fprintf (stderr, "found master connection %d for %d.\n", c->master->fd, c->fd);
  }
  return 1;
      }
    }

    if (verbosity > 0) {
      fprintf (stderr, "no master connection found, skipping %d bytes\n", data_len);
    }

    return 1;

  }

  if (verbosity > 0) {
    fprintf (stderr, "unknown command, closing socket\n");
  }

  return -1;
}

static int client_result_alloc (nb_allocator_t Alloc, struct gather_entry *E, char **to) {
  int b = E->res_read, r;
  struct gather_chunk *C;
  assert (b >= 4 && b <= E->res_bytes);
  *to = 0;
  if (b == E->res_bytes) {
    return 0;
  }
  r = (b - 4) % (4 * CHUNK_INTS);
  if (r) {
    *to = (char *) E->last->data + r;
    return 4 * CHUNK_INTS - r;
  }
  C = nbr_alloc (Alloc, sizeof (struct gather_chunk));
  C->next = 0;
  if (!E->first) {
    E->first = E->last = C;
  } else {
    E->last->next = C;
    E->last = C;
  }
  *to = (char *) C->data;
  return 4 * CHUNK_INTS;
}

static int client_read_special (struct connection *c) {
  int x = c->serv->id, t = 0, s, u;
  char *st, *to, *ptr;
  struct gather_entry *E;

  if (verbosity > 0) {
    fprintf (stderr, "in client_read_special for %d, %d unread bytes\n", c->fd, c->unread_res_bytes);
  }

  assert (x >= 0 && x < CSN);
  while (c->unread_res_bytes && (s = get_ready_bytes (&c->In)) > 0) {

    if (s > c->unread_res_bytes) {
      s = c->unread_res_bytes;
    }

    ptr = st = get_read_ptr (&c->In);
    if (c->gather && c->gather->tot_num > x) {
      E = &c->gather->List[x];

      if (verbosity > 1) {
        fprintf (stderr, "inside client_read_special for %d, %d unread bytes, %d bytes read, %d bytes total, %d bytes ready\n", c->fd, c->unread_res_bytes, E->res_read, E->res_bytes, s);
      }

      if (!E->res_read) {
        if (s < 4) {
          s = force_ready_bytes (&c->In, 16);
          if (s >= 4) {
            continue;
          }
          return 0;
        }
        E->num = *((int *) ptr);
        E->res_read = 4;
        if (E->num >= 0 && E->num <= 0x1000000) {
          ptr += 4;
          s -= 4;
          if (verbosity > 1) {
            fprintf (stderr, "got %d from %d\n", E->num, c->serv->id);
          }
        } else {
          E->num = -2;
          E->res_bytes += 4;
          c->gather->error_num++;
        }
      }

      while (s > 0 && (u = client_result_alloc (c->gather->Alloc, E, &to)) > 0) {
        if (verbosity > 1) {
          fprintf (stderr, "inside client_read_special for %d read loop: %d ready to read, %d in target chunk buffer at %p\n", c->fd, s, u, to);
        }
        if (u > s) { u = s; }
          memcpy (to, ptr, u);
        ptr += u;
        s -= u;
        E->res_read += u;
      }

      s = ptr - st;
    }

    if (verbosity > 1) {
      fprintf (stderr, "inside client_read_special for %d: advance read pointer by %d bytes\n", c->fd, s);
    }
    advance_read_ptr (&c->In, s);

    t += s;
    c->unread_res_bytes -= s;
    free_unused_buffers (&c->In);
  }

  if (!c->unread_res_bytes && c->gather && c->gather->tot_num >= x) {
    /* das ist alles */
    if (verbosity > 0) {
      fprintf (stderr, "socket %d completes reading data for master %d\n", c->fd, c->master->fd);
    }
    E = &c->gather->List[x];
    if (E->res_read != E->res_bytes) {
      fprintf (stderr, "res_read=%d res_bytes=%d unread=%d num=%d\n", E->res_read, E->res_bytes, c->unread_res_bytes, E->num);
      fprintf (stderr, "in connection %d gathering for master %d.%d, tot=%d err=%d ready=%d\n", c->fd, c->master->fd, x, c->gather->wait_num, c->gather->error_num, c->gather->ready_num);
    }
    assert (E->res_read == E->res_bytes);
    assert (++c->gather->ready_num <= c->gather->wait_num);
    if (c->gather->ready_num == c->gather->wait_num) {
      /* wake up master */
      if (verbosity > 0) {
  fprintf (stderr, "socket %d was the last one, waking master %d\n", c->fd, c->master->fd);
      }
      if (!c->master->ev->in_queue) {
  put_event_into_heap (c->master->ev);
      }
    }
    c->master = 0;
    c->gather = 0;
    c->master_generation = 0;
    //  c->state &= ~2;
  }

  return t;
}

int client_reader (struct connection *c) {

  char *st, *ptr, *to;
  int r, s, t = 0;

  if (verbosity > 1) {
    fprintf (stderr, "in client_reader(%d)\n", c->fd);
  }

  if (c->unread_res_bytes) {
    get_client_gather (c);
    t = client_read_special (c);
  }

  to = get_write_ptr (&c->In, 512);
  s = get_write_space (&c->In);

  if (s <= 0) {
    free_all_buffers(&c->In);
    c->error = 8;
    to = get_write_ptr (&c->In, 512);
    s = get_write_space (&c->In);
  }

  assert (to && s > 0);

  if (!(c->state & C_NORD)) {
    r = recv (c->fd, to, s, MSG_DONTWAIT);

    if (verbosity > 0) {
      fprintf (stderr, "recv() from %d: %d read out of %d at %p\n", c->fd, r, s, to);
      if (r < 0 && errno != EAGAIN) perror ("recv()");
    }

  } else {
    r = 0;
  }

  if (r < s) {
    c->state |= C_NORD;
  }

  if (r > 0) {
    advance_write_ptr (&c->In, r);
  } else if (c->unread_res_bytes) {
    c->state |= C_WANTRD;
    return 2;
  }

  if (c->unread_res_bytes) {
    get_client_gather (c);
    client_read_special (c);
    return 0;
  }

  do {
    ptr = st = get_read_ptr (&c->In);
    s = get_ready_bytes (&c->In);
    if (!s) { return 0; }
    to = st + (s > 1024 ? 1024 : s);

    while (ptr < to && *ptr != 10) {
      ptr++;
    }

    if (ptr >= to) {
      if (to - st >= 1024) {
  if (verbosity > 0) {
    fprintf (stderr, "client socket #%d: command line longer than 1024 bytes\n", c->fd);
  }
  advance_read_ptr (&c->In, to - st);
  return -1;
      } else {
  r = force_ready_bytes (&c->In, 1024);
  return r <= s ? -1 : 0;
      }
    }

    to = ptr;
    s = to - st + 1;
    *to = 0;
    if (to > st && to[-1] == '\r') {
      *--to = 0;
    }

    r = client_execute (c, st, to - st);

    advance_read_ptr (&c->In, s);

    if (r < 0) {
      return -1;
    }

    free_unused_buffers (&c->In);
  } while (!r);

  return r;
}

int clear_client (struct connection *c) {
  struct cluster_server *S = c->serv;

  if (verbosity > 0) {
    fprintf (stderr, "cleaning outbound socket %d...\n", c->fd);
  }

  if (c->state == C_INCONN) {
    fprintf (stderr, "could not connect to %s:%d\n", inet_ntoa(c->serv->addr), c->serv->port);
    /* remove from servers's list ... */
  } else {
    S->rconn--;
    assert (S->rconn >= 0);
    active_outbound_connections--;
  }
  S->conn--;
  assert (S->conn >= 0);
  outbound_connections--;
  if (c->prev) { c->prev->next = c->next; }
  if (c->next) { c->next->prev = c->prev; }
  if (S->c == c) { S->c = c->next; }
  free_connection_buffers (c);

  if (c->ev) {
    c->ev->data = 0;
  }
  memset (c, 0, sizeof(struct connection));

  assert (S->conn >= 0);
  if (!S->conn) {
    S->c = 0;
    /* no connections left, try to reconnect */
    if (now >= S->reconnect_time) {
      S->reconnect_time = now + 1;
      S->conn_retries++;
      if (verbosity > 0) {
  fprintf (stderr, "trying to reconnect to %s:%d...\n", inet_ntoa (S->addr), S->port);
      }
      create_client (S);
    }
  }

  return EVA_DESTROY;
}

int client_worker (int fd, void *data, event_t *ev) {
  struct connection *c = (struct connection *) ev->data;
  struct cluster_server *S;
  int res;
  assert (c);

  if (ev->epoll_ready & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
    if (verbosity > 0) {
      fprintf (stderr, "outbound socket %d: disconnected, cleaning\n", fd);
    }

    // close (fd);
    return clear_client (c);
  }

  if (c->state == C_INCONN) { /* connecting... */
    if (ev->ready & EVT_WRITE) {
      if (verbosity > 0) {
  fprintf (stderr, "socket #%d to %s:%d becomes active\n", c->fd, inet_ntoa(c->serv->addr), c->serv->port);
      }
      c->state = C_WANTRD;
      S = c->serv;
      S->rconn++;
      S->conn_retries = 0;
      S->reconnect_time = 0;
      active_outbound_connections++;
      return EVT_READ | EVT_SPEC;
    }
    fprintf (stderr, "socket #%d: unknown event before connecting (?)\n", c->fd);
    return EVT_SPEC;
  }

  c->state &= ~C_NORW;
  if ((ev->state & EVT_READ) && !(ev->ready & EVT_READ)) { c->state |= C_NORD; }
  if ((ev->state & EVT_WRITE) && !(ev->ready & EVT_WRITE)) { c->state |= C_NOWR; }

  if (verbosity > 0) {
    fprintf (stderr, "outbound socket #%d: woke up, state=%d\n", c->fd, c->state);
  }

  server_writer (c);

  //  while (c->state > 0 && (c->state & 3) && (get_ready_bytes(&c->In) > 0 || (!(c->state & 16) && ((ev->ready & EVT_READ) || !(ev->state & EVT_READ))))) {
  res = 0;
  while (c->state > 0 && (res == 1 || ((c->state & C_WANTRD) && !(c->state & C_NORD)))) {

    res = client_reader (c);
    if (verbosity > 1) {
      fprintf (stderr, "client_reader(%d) returned %d\n", c->fd, res);
    }
    if (res < 0) {
      c->error |= 8;
    }
    free_unused_buffers (&c->In);
    if (res < 0 || res > 1) {
      break;
    }
  }

  if (c->error) {
    if (verbosity > 0) {
      fprintf (stderr, "client socket #%d: error, closing socket.\n", c->fd);
    }
    return clear_client (c);
  }

  free_unused_buffers (&c->In);
  free_unused_buffers (&c->Out);

  return get_ready_bytes (&c->Out) > 0 ? EVT_SPEC | EVT_READ | EVT_WRITE : EVT_SPEC | EVT_READ;

}

struct connection *create_client (struct cluster_server *S) {
  int cfd = client_socket (S->addr.s_addr, S->port, 0);
  struct connection *c, *h;
  event_t *ev;

  if (cfd < 0) {
    fprintf (stderr, "error connecting to %s:%d: %m", inet_ntoa(S->addr), S->port);
    exit(1);
  }

  assert (cfd < MAX_EVENTS);

  ev = Events + cfd;
  //  memcpy (&ev->peer, &peer, sizeof(peer));

  c = Connections + cfd;
  memset (c, 0, sizeof (struct connection));
  c->fd = cfd;
  c->ev = ev;
  c->serv = S;

  init_builtin_buffer (&c->In, c->in_buff, BUFF_SIZE);
  init_builtin_buffer (&c->Out, c->out_buff, BUFF_SIZE);

  epoll_sethandler (cfd, 0, client_worker, c);
  epoll_insert (cfd, EVT_WRITE | EVT_SPEC);
  if (verbosity > 1) {
    fprintf (stderr, "after insert()\n");
  }

  if (!S->c) {
    c->next = c->prev = S->c = c;
  } else {
    h = S->c;
    c->next = h;
    c->prev = h->prev;
    h->prev->next = c;
    h->prev = c;
  }

  c->state = C_INCONN;  /* connecting */
  S->conn++;
  outbound_connections++;

  if (verbosity > 0) {
    fprintf (stderr, "outbound connection #%d: handle %d to %s:%d\n", S->id, c->fd, inet_ntoa(S->addr), S->port);
  }

  return c;
}

/*
 * end (client)
 */

/* -------- LIST GATHER/MERGE ------------- */

enum { g_id_asc = 1, g_id_desc = 9, g_rate_asc = 2, g_rate_desc = 10, g_desc = 8, g_double = 2 } GH_mode;

typedef struct gather_heap_entry {
  struct gather_chunk *cur_chunk;
  int *cur, *last;
  int remaining;
  int value;
  int x;
} gh_entry_t;

gh_entry_t GH_E[MAX_CLUSTER_SERVERS];
gh_entry_t *GH[MAX_CLUSTER_SERVERS+1];
int GH_N, GH_total;

void clear_gather_heap (int mode) {
  if (mode < 0) { mode = 8 - mode; }
  GH_mode = mode;
  GH_N = 0;
  GH_total = 0;
}

static inline void load_heap_v (gh_entry_t *H) {
  int *data = H->cur;
  int x = (data[1] ? data[1] : data[0]);
  int value = (GH_mode & g_double ? data[2] : x);
  if (GH_mode & g_desc) {
    H->x = -x;
    H->value = -value;
  } else {
    H->x = x;
    H->value = value;
  }
}

static int gather_heap_insert (struct gather_entry *GE) {
  int cnt, cnt2, sz;
  gh_entry_t *H;
  assert (GH_N < MAX_CLUSTER_SERVERS);
  if (GE->num <= 0 || GE->res_read < 8 || !GE->first) {
    return 0;
  }
  cnt2 = cnt = (GE->res_read >> 2) - 1;
  sz = (GH_mode & g_double) ? 3 : 2;
  cnt /= sz;
  cnt2 = cnt * sz;
  if (cnt <= 0) {
    return 0;
  }
  GH_total += cnt;

  H = &GH_E[GH_N];
  H->remaining = cnt;
  H->cur_chunk = GE->first;
  H->cur = H->cur_chunk->data;
  H->last = H->cur + (cnt2 < CHUNK_INTS ? cnt2 : CHUNK_INTS);
  load_heap_v (H);

  int v = H->value, vx = H->x, i = ++GH_N, j;
  while (i > 1) {
    j = (i >> 1);
    if (GH[j]->value < v || (GH[j]->value == v && GH[j]->x <= vx)) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;

  return 1;
}

static int *get_gather_heap_head (void) {
  return GH_N ? GH[1]->cur : 0;
}

static void gather_heap_advance (void) {
  gh_entry_t *H;
  int sz = (GH_mode & g_double ? 3 : 2);
  if (!GH_N) { return; }
  H = GH[1];
  H->cur += sz;
  if (!--H->remaining) {
    H = GH[GH_N--];
    if (!GH_N) { return; }
  } else {
    if (H->cur >= H->last) {
      H->cur_chunk = H->cur_chunk->next;
      assert (H->cur_chunk);
      H->cur = H->cur_chunk->data;
      int t = H->remaining * sz;
      if (t > CHUNK_INTS) { t = CHUNK_INTS; }
      H->last = H->cur + t;
    }
    load_heap_v (H);
  }
  int i = 1, j, v = H->value, vx = H->x;
  while (1) {
    j = i*2;
    if (j > GH_N) { break; }
    if (j < GH_N && (GH[j+1]->value < GH[j]->value || (GH[j+1]->value == GH[j]->value && GH[j+1]->x < GH[j]->x))) { j++; }
    if (v < GH[j]->value || (v == GH[j]->value && vx <= GH[j]->x)) { break; }
    GH[i] = GH[j];
    i = j;
  }
  GH[i] = H;
}

int R[10000];

/* -------- END (LIST GATHER/MERGE) ------- */

int quit_steps;

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

int prepare_stats (void) {
  int uptime = now - start_time;

  return stats_buff_len = snprintf (stats_buff, STATS_BUFF_SIZE,
      "start_time\t%d\n"
      "current_time\t%d\n"
      "uptime\t%d\n"
      "network_connections\t%d\n"
      "max_network_connections\t%d\n"
      "outbound_connections_config\t%d\n"
      "outbound_connections\t%d\n"
      "outbound_connections_active\t%d\n"
      "active_gathers\t%d\n"
      "active_network_events\t%d\n"
      "used_network_buffers\t%d\n"
      "max_network_buffers\t%d\n"
      "network_buffer_size\t%d\n"
      "queries_total\t%lld\n"
      "qps\t%.3f\n"
      "END\n",
      start_time,
      now,
      uptime,
      active_connections,
      maxconn,
      CSN,
      outbound_connections,
      active_outbound_connections,
      active_gathers,
      ev_heap_size,
      NB_used,
      NB_max,
      NB_size,
      netw_queries,
      safe_div(netw_queries, uptime)
      );
}


int return_key_value (struct connection *c, char *key, char *val, int vlen) {
  static char buff[2048];
  int l = sprintf (buff, "VALUE %s 0 %d\r\n", key, vlen);
  assert (l <= 2048);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\nEND\r\n", 7);
  return 0;
}

int return_key_len_value (struct connection *c, char *key, int key_len, char *val, int vlen) {
  static char buff[2048];
  assert (key_len <= 1024);
  int l = sprintf (buff, "VALUE %.*s 0 %d\r\n", key_len, key, vlen);
  assert (l <= 2048);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\nEND\r\n", 7);
  return 0;
}

#define MAX_RES 10000

int Q_order, Q_limit, Q_slice_limit, Q_raw;
char *Qs, *Q_limit_ptr, *Q_limit_end;
int R_cnt;
int R[MAX_RES];

static char *parse_search_extras (char *ptr) {
  Q_raw = 0;
  Q_order = 0;
  Q_limit = 0;
  Q_slice_limit = 0;

  Q_limit_ptr = Q_limit_end = 0;

  if (*ptr != '#') {
    return ptr;
  }
  ptr++;
  while (*ptr != '(' && *ptr != '[') {
    switch (*ptr) {
    case '%':
      Q_raw = 1;
      ptr++;
      break;
    case 'i':
      Q_order = 1;
      ptr++;
      break;
    case 'I':
      Q_order = 9;
      ptr++;
      break;
    case 'r':
      Q_order = 2;
      ptr++;
      break;
    case 'R':
      Q_order = 10;
      ptr++;
      break;
    case 's':
      Q_order = 3;
      ptr++;
      break;
    case 'S':
      Q_order = 11;
      ptr++;
      break;
    case 'P':
      Q_order |= 4;
      ptr++;
      break;
    case '?':
      Q_order = 7;
      ptr++;
      break;
    case '0' ... '9':
      Q_limit_ptr = ptr;
      Q_limit = strtol (ptr, &ptr, 10);
      Q_limit_end = ptr;
      if (Q_limit < 0) { Q_limit = 0; }
      if (Q_limit > MAX_RES) { Q_limit = MAX_RES; }
      break;
    default:
      return ptr;
    }
  }
  return ptr;
}

int do_search_query (struct connection *c, char *key, int len) {
  char *q_end;
  char buff[512];
  int w, i = -1;
  struct gather_data *G;

  Q_raw = 0;

  if (!strncmp (key, "search", 6)) {
    i = 6;
  } else {
    return -1;
  }

  if (verbosity > 0) {
    fprintf (stderr, "got: %s\n", key);
  }

  assert (!c->gather);

  Qs = key + i; /* compile_query (key+7); */
  parse_search_extras(Qs);

  if (0) {
    w = sprintf (buff, "ERROR near '%.256s'\n", q_end);
    if (verbosity > 0) {
      fprintf (stderr, "ERROR near '%.256s'\n", q_end);
    }
    return return_key_value (c, key, buff, w);
  }

  if (!active_outbound_connections) {
    return return_key_value (c, key, "ERROR: no servers available", strlen("ERROR: no servers available"));
  }

  int ready_num = 0;

  for (w = 0; w < CSN; w++) {
    struct connection *d, *h;
    if (CS[w].rconn && CS[w].c) {
      d = h = CS[w].c;
      do {
  if (d->state >= 0 && !(d->state & C_INCONN)) {
    ready_num++;
    break;
  }
  d = d->next;
      } while (d != h);
    }
  }

  if (ready_num * 5 <= CSN * 4) {
    return return_key_value (c, key, "ERROR_INCOMPLETE", 16);
  }

  Q_slice_limit = estimate_split (Q_limit, CSN);

  w = sizeof (struct gather_data) + (CSN - 1) * sizeof (struct gather_entry);
  G = nbr_alloc (&c->Tmp, w);
  assert (G);

  memset (G, 0, w);

  //  fprintf (stderr, "after nbr_alloc: Tmp=%p prev=%p next=%p state=%08x;\n", c->Tmp, c->Tmp ? c->Tmp->prev : 0, c->Tmp ? c->Tmp->next : 0, c->Tmp ? c->Tmp->state : 0);

  G->c = c;
  G->magic = GD_MAGIC + ((Q_order & 7) > 1 ? GD_EXTRA : 0) + (Q_order & 8 ? GD_REVERSE : 0) + (Q_raw ? GD_RAW : 0) + (Q_order == 7 ? GD_DROP_EXTRA : 0);

  c->gather = G;
  c->state |= C_INQUERY;

  G->next = &GDH;
  G->prev = GDH.prev;
  GDH.prev->next = G;
  GDH.prev = G;
  active_gathers++;

  G->Alloc = &c->Tmp;
  G->tot_num = CSN;
  G->wait_num = 0;
  G->ready_num = 0;
  G->error_num = 0;
  G->timeout_time = now + GATHER_QUERY_TIMEOUT;
  G->limit = Q_limit;
  G->start_time = get_utime(CLOCK_MONOTONIC);

  G->orig_key = nbr_alloc (&c->Tmp, len+1);
  memcpy (G->orig_key, key, len+1);
  G->orig_key_len = len;

  G->new_key = nbr_alloc (&c->Tmp, len+3);

  if (Q_slice_limit < Q_limit && key[i] == '#' && Q_limit > 0 && Q_slice_limit > 0 && Q_limit_ptr) {
    i = Q_limit_ptr - key;
    assert ((unsigned) i < 2048);
    memcpy (G->new_key, key, i);
    i += sprintf (G->new_key + i, "%d%%", Q_slice_limit);
    w = key + len - Q_limit_end;
    assert ((unsigned) w < 2048);
    memcpy (G->new_key + i, Q_limit_end, w);
    assert (i + w < len + 3);
    G->new_key[i+w] = 0;
    G->new_key_len = i + w;
  } else if (Q_raw) {
    memcpy (G->new_key, key, len+1);
    G->new_key_len = len;
  } else if (key[i] == '#') {
    memcpy (G->new_key, key, i+1);
    G->new_key[i+1] = '%';
    memcpy (G->new_key+i+2, key+i+1, len-i);
    G->new_key_len = len + 1;
  } else {
    memcpy (G->new_key, key, i);
    G->new_key[i] = '#';
    G->new_key[i+1] = '%';
    memcpy (G->new_key+i+2, key+i, len-i+1);
    G->new_key_len = len + 2;
  }

  //  fprintf (stderr, "again after nbr_alloc: Tmp=%p prev=%p next=%p state=%08x;\n", c->Tmp, c->Tmp ? c->Tmp->prev : 0, c->Tmp ? c->Tmp->next : 0, c->Tmp ? c->Tmp->state : 0);
  if (verbosity > 0) {
    fprintf (stderr, "sending to %d servers query 'get %s'\n", CSN, G->new_key);
  }

  for (i = 0; i < CSN; i++) {
    struct connection *d, *h;
    G->List[i].num = -3;
    if (CS[i].rconn && CS[i].c) {
      d = h = CS[i].c;
      do {
        if (d->state >= 0 && !(d->state & C_INCONN)) {
          G->List[i].num = -1;
          G->wait_num++;
          write_out (&d->Out, "get ", 4);
          write_out (&d->Out, G->new_key, G->new_key_len);
          write_out (&d->Out, "\r\n", 2);
          /* wake d up */
          d->state |= C_WANTRW;
          if (!d->ev->in_queue) {
            put_event_into_heap (d->ev);
          }
          break;
        }
        d = d->next;
      } while (d != h);
    }
  }

  return 0;
}

int end_search_query (struct connection *c) {
  static char buff[2048];
  int w, i;
  int res;
  char *ptr, *size_ptr, *s;
  char *key;
  int len;
  struct gather_data *G = c->gather;
  struct gather_entry *D;

  assert (c->gather);
  assert ((G->magic & GD_MAGIC_MASK) == GD_MAGIC);
  key = G->orig_key;
  len = G->orig_key_len;

  Q_limit = G->limit;
  Q_raw = ((G->magic & GD_RAW) != 0);
  Q_order = (G->magic & GD_EXTRA ? 2 : 1);
  if (G->magic & GD_REVERSE) {
    Q_order += 8;
  }
  if (G->magic & GD_DROP_EXTRA) {
    Q_order = 7;
  }

  c->gather = 0;
  G->c = 0;
  c->state &= ~C_INQUERY;
  c->state |= C_WANTRW;

  if (verbosity > 0) {
    double ti = get_utime(CLOCK_MONOTONIC);
    fprintf (stderr, "end_search_query(): elapsed_time=%.08f; key='%s', now=%d, timeout=%d; clients:tot=%d,avail=%d,ready=%d,error=%d\n",
       ti - G->start_time, key, now, G->timeout_time,
       G->tot_num, G->wait_num, G->ready_num, G->error_num);
  }

  if (*key == 'p') {
    if (Q_limit <= 0) { Q_limit = 1000; }
    if (Q_limit > 10000) { Q_limit = 10000; }
    memset (R, 0, Q_limit * sizeof(int));
    Q_order = 0;
    R_cnt = 0;
  } else {
    clear_gather_heap (Q_order);
  }

  D = 0;
  res = 0;
  /* sum results */
  for (i = 0; i < G->tot_num; i++) {
    if (G->List[i].num >= 0) {
      res += G->List[i].num;
      gather_heap_insert (&G->List[i]);
    } else if (G->List[i].num == -2) {
      D = &G->List[i];
    }
  }
  G->sum = res;

  if (verbosity > 0) {
    fprintf (stderr, "result = %d\n", res);
  }

  /* naughty: free all data even if we are going to use them */
  free_gather (G);

  if (D) {
    /* have error */
    w = D->res_read - 4;
    if (w > 0) {
      ptr = (char *) D->first->data;
      if (w >= CHUNK_INTS * 4) {
        w = CHUNK_INTS * 4 - 1;
      }
      ptr[w] = 0;
      if (verbosity > 0) {
        fprintf (stderr, "got error message: %s\n", ptr);
      }
      return return_key_value (c, key, ptr, w);
    }
    return return_key_value (c, key, "ERROR_UNKNOWN", 13);
  }

  if ((G->ready_num - G->error_num) * 5 <= G->tot_num * 4) {
    return return_key_value (c, key, "ERROR_INCOMPLETE", 16);
  }

  if (!Q_limit || !res) {
    w = sprintf (buff, "%d", res);
    return return_key_value (c, key, buff, w);
  }

  write_out (&c->Out, "VALUE ", 6);
  write_out (&c->Out, key, len);

  ptr = get_write_ptr (&c->Out, 512);
  if (!ptr) return -1;
  s = ptr + 464;

  memcpy (ptr, " 0 .........\r\n", 14);
  size_ptr = ptr + 3;
  ptr += 14;
  if (!Q_raw) {
    ptr += w = sprintf (ptr, "%d", res);
  } else {
    *((int *) ptr) = res;
    ptr += w = 4;
  }

  int have_extras = (((Q_order + 1) & 7) > 2);

  for (i = 0; i < Q_limit; i++) {
    int t;
    int *Res = get_gather_heap_head ();
    if (!Res) {
      break;
    }
    if (ptr >= s) {
      advance_write_ptr (&c->Out, ptr - (s - 464));
      ptr = get_write_ptr (&c->Out, 512);
      if (!ptr) return -1;
      s = ptr + 464;
    }
    if (!Q_raw) {
      *ptr++ = ',';  w++;
      if (!Res[1]) {
        w += t = sprintf (ptr, "%d", Res[0]);
      } else {
        w += t = sprintf (ptr, "%d_%d", Res[0], Res[1]);
      }
      ptr += t;
      t = 0;
      if (have_extras) {
        w += t = sprintf (ptr, ",%d", Res[2]);
      }
    } else {
      t = (have_extras ? 12 : 8);
      memcpy (ptr, Res, t);
      w += t;
    }
    ptr += t;
    gather_heap_advance ();
  }
  size_ptr[sprintf (size_ptr, "% 9d", w)] = '\r';
  memcpy (ptr, "\r\nEND\r\n", 7);
  ptr += 7;
  advance_write_ptr (&c->Out, ptr - (s - 464));

  return 0;
}

void net_readvar_callback (struct connection *c) {
  //  int i, j;

  assert (c->Tmp);
//  Msg = (raw_message_t *) c->Tmp->start;

//  Msg->data[Msg->len] = 0;

  if (verbosity > 0) {
//    fprintf (stderr, "(%d) got new message %d of user %d, len %d : %-40s...\n",
//       Msg->no_reply, Msg->message_id, Msg->user_id, Msg->len, Msg->data);
  }

  free_all_buffers (c->Tmp);
  c->Tmp = 0;

//  if (!Msg->no_reply) {
//    write_out (&c->Out, "STORED\r\n", 8);
//  }


}


int exec_mc_store (int op, struct connection *c, char *key, int len, int flags, int expire, int bytes, int no_reply)
{
  int r, uid, msgid;
//  raw_message_t *Msg;

  key[len] = 0;

  if (verbosity > 0) {
    fprintf (stderr, "mc_store: op=%d, key=\"%s\", flags=%d, expire=%d, bytes=%d, noreply=%d\n", op, key, flags, expire, bytes, no_reply);
  }

  if (op == 1 && bytes < 64 && sscanf (key, "newmsg%d,%d", &uid, &msgid) == 2) {

    newmsg_queries++;

    if (!c->Tmp) {
      c->Tmp = alloc_buffer();
      assert (c->Tmp);
    }

/*
    Msg = (raw_message_t *) c->Tmp->start;
    Msg->user_id = uid;
    Msg->message_id = msgid;
    Msg->no_reply = no_reply;
    Msg->len = bytes;

    Msg->data = c->wra = c->Tmp->start + sizeof (raw_message_t);
*/
    c->wre = c->wra + bytes;
    assert (c->wre < c->Tmp->end);

    r = read_in (&c->In, c->wra, bytes);

    if (verbosity > 0) {
      fprintf (stderr, "read_in=%d, need=%d\n", r, bytes);
    }

    if (r > 0) {
      c->wra += r;
    }

    if (r == bytes) {
      c->wra = c->wre = 0;
      net_readvar_callback(c);
    }

    return 0;
  }

  if (!no_reply) {
    write_out (&c->Out, "NOT_STORED\r\n", 12);
  }

  return bytes;
}

int parse_mc_store (int op, struct connection *c, char *str, int len) {
  char *key, *ptr;
  int key_len, flags, expire, bytes, noreply;

  while (*str == ' ') str++;
  key = str;
  while (*str > ' ') str++;
  key_len = str - key;
  if (!key_len || *str != ' ') return -1;

  flags = strtol (str, &ptr, 10);
  if (ptr == str) return -1;
  str = ptr;

  expire = strtol (str, &ptr, 10);
  if (ptr == str) return -1;
  str = ptr;

  bytes = strtol (str, &ptr, 10);
  if (ptr == str) return -1;
  str = ptr;

  noreply = strtol (str, &ptr, 10);
  if (ptr == str) { noreply = 0; } else { str = ptr; }

  while (*str == ' ') str++;
  if (*str) return -1;

  return exec_mc_store (op, c, key, key_len, flags, expire, bytes, noreply);
}

int exec_delete (struct connection *c, char *str, int len) {

  while (*str == ' ') { str++; }

  if (verbosity > 0) {
    fprintf (stderr, "delete \"%s\"\n", str);
  }

  return 0;
}

int execute (struct connection *c, char *str, int len) {
  int op = 0;

  if (len && str[len-1] == 13) len--;
  str[len] = 0;

  if (verbosity) {
    fprintf (stderr, "got command \"%s\" from %d\n", str, c->fd);
  }

  if (c->error & 8) {
    write_out (&c->Out, "ERROR\n", 6);
    return 0;
  }

  if (!len) {
    return 0;
  }

  netw_queries++;

  if (!strcmp(str, "QUIT") && !quit_steps) {
    if (verbosity) {
      printf ("Quitting in 3 seconds.\n");
    }
    quit_steps = 3;
    write_out (&c->Out, "OK\n", 3);
    return 0;
  }
  if (!strcmp(str, "CLOSE") && c->error != -3) {
    if (verbosity) {
      printf ("Closing connection by user's request.\n");
    }
    c->error = -3;
    write_out (&c->Out, "CLOSING\n", 8);
    return -3;
  }

  if (len <= 1020 && !strncmp (str, "get search", 10)) {
    if (0) {
      write_out (&c->Out, "END\r\n", 5);
      return 0;
    }

    double t = get_utime(CLOCK_MONOTONIC);

    int x = do_search_query(c, str+4, len-4);

    t = get_utime(CLOCK_MONOTONIC) - t;
    if (verbosity > 0) {
      fprintf (stderr, "search query time: %.6lfs\n", t);
    }

    if (x < 0) {
      fprintf (stderr, "do_search_query('%s',%d) = %d\n", str+4, len-4, x);
      write_out (&c->Out, "ERROR\r\n", 7);
      return 0;
    }
    if (!x) {
      // return write_out (c, "END\r\n", 5) < 0 ? -2 : 0;
    }

    return 0;

  }

  if (len >= 9 && !strncmp (str, "get stats", 9)) {
    return_key_len_value (c, str + 4, len - 4, stats_buff, prepare_stats());
    return 0;
  }

  if (!strcmp (str, "stats")) {
    write_out (&c->Out, stats_buff, prepare_stats());
    return 0;
  }

  if (len == 7 && !strncmp (str, "version", 7)) {
    write_out (&c->Out, "VERSION " VERSION_STR "\r\n", sizeof ("VERSION " VERSION_STR "\r\n") - 1);
    return 0;
  }

  if (!strncmp (str, "get ", 4)) {
    write_out (&c->Out, "END\r\n", 5);
    return 0;
  }

  if (len >= 10 && !memcmp (str, "delete ", 7)) {
    if (exec_delete (c, str+7, len-7) > 0) {
      write_out (&c->Out, "DELETED\r\n", 9);
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }
    return 0;
  }

  if (!strncmp (str, "set ", 4)) {
    op = 1;
    str += 4;
    len -= 4;
  }

  if (op > 0) {
    return parse_mc_store (op, c, str, len);
  }

  write_out (&c->Out, "ERROR\r\n", 7);
  return 0;
}

int server_writer (struct connection *c) {
  int r, s, t = 0;
  char *to;

  while ((c->state & C_WANTWR) != 0) {
    // write buffer loop
    s = get_ready_bytes (&c->Out);

    if (!s) {
      c->state &= ~C_WANTWR;
      break;
    }

    if (c->state & C_NOWR) {
      break;
    }

    to = get_read_ptr (&c->Out);

    r = send (c->fd, to, s, MSG_DONTWAIT | MSG_NOSIGNAL);

    if (verbosity > 0) {
      fprintf (stderr, "send() to %d: %d written out of %d at %p\n", c->fd, r, s, to);
      if (r < 0) perror ("send()");
    }

    if (r > 0) {
      advance_read_ptr (&c->Out, r);
      t += r;
    }

    if (r < s) {
      c->state |= C_NOWR;
    }

  }

  if (t) {
    free_unused_buffers (&c->Out);
  }

  return c->state & C_WANTWR ? EVT_WRITE : 0;
}

int server_reader (struct connection *c) {
  int res = 0, r, s, read_failures = 0;
  char *ptr, *st, *to;

  while ((c->state & C_WANTRD) && !(c->state & C_NORD)) {

    if (c->wra < c->wre) {

      s = c->wre - c->wra;
      r = recv (c->fd, c->wra, s, MSG_DONTWAIT);

      if (r < s) { c->state |= C_NORD; }

      if (verbosity > 0) {
  fprintf (stderr, "inner recv() from %d: %d read out of %d at %p\n", c->fd, r, s, c->wra);
  if (r < 0 && errno != EAGAIN) { perror ("recv()"); }
      }

      if (r <= 0) {
  return 0;
      }

      c->wra += r;
      if (c->wra == c->wre) {
  c->wra = c->wre = 0;
  net_readvar_callback (c);
      }

      continue;
    }

    to = get_write_ptr (&c->In, 512);
    s = get_write_space (&c->In);

    if (s <= 0) {
      fprintf (stderr, "no input buffers?\n");
      free_all_buffers(&c->In);
      c->error = 8;
      to = get_write_ptr (&c->In, 512);
      s = get_write_space (&c->In);
    }

    assert (to && s > 0);

    r = recv (c->fd, to, s, MSG_DONTWAIT);

    if (r < s) { c->state |= C_NORD; }

    if (verbosity > 0) {
      fprintf (stderr, "recv() from %d: %d read out of %d at %p\n", c->fd, r, s, to);
      if (r < 0 && errno != EAGAIN) { perror ("recv()"); }
    }

    if (r > 0) {
      advance_write_ptr (&c->In, r);
    }
    if (r < 0 && ++read_failures > 5) {
      break;
    }

    do {
      res = 0;

      if (c->gather) {
  break;
      }

      ptr = st = get_read_ptr (&c->In);
      s = get_ready_bytes (&c->In);
      if (s > 1024) { s = 1024; }
      to = st + s;

      while (ptr < to && *ptr != 10) { ptr++; }

      if (ptr == to) {
  if (s == 1024) {
    if (verbosity > 0) {
      fprintf (stderr, "error: command line longer than 1024 bytes, closing connection\n");
    }
    c->error = -5;
    return -1;
  }
  r = force_ready_bytes (&c->In, 1024);
  if (r > s) {
    continue;
  } else {
    break;
  }
      }

      advance_read_ptr (&c->In, ptr + 1 - st);

      res = execute (c, st, ptr - st);

      if (res < 0 || (c->state & C_INQUERY)) {
  break;
      } else if (res > 0) {
  advance_skip_read_ptr (&c->In, res);
  res = 0;
      }

      c->error = 0;

    } while (c->wra == c->wre);

    if (res <= -2 || (c->state & C_INQUERY)) break;
  }

  return res;
}

int server_read_write (int fd, void *data, event_t *ev) {
  struct connection *c = (struct connection *) data;
  int res;
  assert (c);

  if (ev->epoll_ready & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
    if (verbosity > 0) {
      fprintf (stderr, "socket %d: disconnected, cleaning\n", fd);
    }
    free_connection_buffers (c);
    memset (c, 0, sizeof(struct connection));
    ev->data = 0;
    active_connections--;
    // close (fd);
    return EVA_DESTROY;
  }

  c->state &= ~C_NORW;
  if ((ev->state & EVT_READ) && !(ev->ready & EVT_READ)) { c->state |= C_NORD; }
  if ((ev->state & EVT_WRITE) && !(ev->ready & EVT_WRITE)) { c->state |= C_NOWR; }

  if (c->gather && (now >= c->gather->timeout_time || c->gather->ready_num == c->gather->wait_num)) {
    res = end_search_query (c);
    free_all_buffers (c->Tmp);
    c->Tmp = 0;
  }

  while ((((c->state & (C_WANTRD | C_INQUERY)) == C_WANTRD) && !(c->state & C_NORD)) || ((c->state & C_WANTWR) && !(c->state & C_NOWR))) {

    res = server_writer (c);
    if (res) { break; }

    if (!c->gather && !(c->state & C_INQUERY)) {
      res = server_reader (c);
      if (verbosity) {
  fprintf (stderr, "server_reader=%d, ready=%02x, state=%02x\n", res, ev->ready, ev->state);
      }
      if (res > 0) { break; }
    }

    if (get_ready_bytes(&c->Out)) {
      c->state |= C_WANTWR;
    }
  }

  free_unused_buffers(&c->In);
  free_unused_buffers(&c->Out);

  if (c->error < 0) {
    if (verbosity > 0) {
      fprintf (stderr, "socket %d: closing and cleaning (error code=%d)\n", fd, c->error);
    }
    free_connection_buffers (c);
    memset (c, 0, sizeof(struct connection));
    ev->data = 0;
    active_connections--;
    // close (fd);
    return EVA_DESTROY;
  }

  return (c->state & C_WANTRD ? EVT_READ : 0) | (c->state & C_WANTWR ? EVT_WRITE : 0) | EVT_SPEC;
}

int accept_new_connections (int fd, void *data, event_t *ev) {
  char buf[32];
  struct sockaddr_in peer;
  unsigned addrlen;
  int cfd, acc = 0;
  struct connection *c;
  do {
    addrlen = sizeof(peer);
    memset (&peer, 0, sizeof(peer));
    cfd = accept (fd, (struct sockaddr *) &peer, &addrlen);
    if (cfd < 0) {
      if (!acc && verbosity > 0) {
  fprintf (stderr, "accept(%d) unexpectedly returns %d\n", fd, cfd);
      }
      break;
    } else acc++;
    assert (cfd < MAX_EVENTS);
    ev = Events + cfd;
    memcpy (&ev->peer, &peer, sizeof(peer));
    if (verbosity > 0) {
      fprintf (stderr, "accepted incoming connection at %s:%d, fd=%d\n", conv_addr(ev->peer.sin_addr.s_addr, buf), ev->peer.sin_port, cfd);
    }
    if (cfd >= MAX_CONNECTIONS) {
      close (cfd);
      continue;
    }
    c = Connections + cfd;
    memset (c, 0, sizeof (struct connection));
    c->fd = cfd;
    c->ev = ev;
    c->generation = ++conn_generation;
    c->state = C_WANTRD;
    init_builtin_buffer (&c->In, c->in_buff, BUFF_SIZE);
    init_builtin_buffer (&c->Out, c->out_buff, BUFF_SIZE);
    epoll_sethandler (cfd, 0, server_read_write, c);
    epoll_insert (cfd, EVT_READ | EVT_SPEC);
    active_connections++;
  } while (1);
  return EVA_CONTINUE;
}

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
  printf("SIGINT handled.\n");
  //  flush_binlog_ts();
  exit(EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  printf("SIGTERM handled.\n");
  //  flush_binlog_ts();
  exit(EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf(stderr, "got SIGHUP.\n");
  //  flush_binlog_ts();
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf(stderr, "got SIGUSR1, rotate logs.\n");
  //  flush_binlog_ts();
  reopen_logs();
  signal(SIGUSR1, sigusr1_handler);
}

void cron (void) {
  int i, j;
  struct gather_data *G;
  //  flush_binlog();
  for (i = 0; i < CSN; i++) {
    if (!CS[i].conn && now >= CS[i].reconnect_time) {
      j = CS[i].conn_retries++;
      CS[i].reconnect_time = now + (j < 10 ? 1 : 5);
      create_client(CS+i);
    }
  }

  for (G = GDH.next; G != &GDH; G = G->next) {
    assert (G->c);
    if (G->timeout_time <= now) {
      if (verbosity > 0) {
  fprintf (stderr, "gather of master connection %d: TIMEOUT (now=%d, timeout=%d), waking up\n", G->c->fd, now, G->timeout_time);
      }
      if (!G->c->ev->in_queue) {
  put_event_into_heap (G->c->ev);
      }
    }
  }

}

int sdf;

void start_server (void) {
  //  struct sigaction sa;
  char buf[64];
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (!sdf) {
    sdf = server_socket (port, settings_addr, backlog, 0);
  }

  if (sdf < 0) {
    fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
    exit(3);
  }

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr(settings_addr.s_addr, buf), port, sdf);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user(username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  epoll_sethandler (sdf, -10, accept_new_connections, 0);
  epoll_insert (sdf, 7);

  for (i = 0; i < CSN; i++) {
    create_client (CS+i);
  }

  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigterm_handler);
  signal(SIGUSR1, sigusr1_handler);
  signal(SIGPIPE, SIG_IGN);
  if (daemonize) {
    signal(SIGHUP, sighup_handler);
    reopen_logs();
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (500);
    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sdf);
  close(sdf);

  //  flush_binlog_ts();
}

/*
 *
 *    MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-l<log-name>] <cluster-descr-file>\n"
    "\tRedistributes search queries to servers listed in <cluster-descr-file>\n"
    "\t-v\toutput statistical and debug information into stderr\n",
    progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;

  progname = argv[0];
  while ((i = getopt (argc, argv, "b:c:l:p:dhu:v")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'b':
      backlog = atoi(optarg);
      if (backlog <= 0) backlog = BACKLOG;
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
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'd':
      daemonize ^= 1;
    }
  }
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  open_file (0, config_filename = argv[optind], 0);
  parse_config ();

  if (raise_file_rlimit(maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit(1);
  }

  sdf = server_socket (port, settings_addr, backlog, 0);
  if (sdf < 0) {
    fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
    exit(1);
  }


  start_time = time(0);

  start_server();

  return 0;
}
