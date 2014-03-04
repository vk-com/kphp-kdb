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

    Copyright      2009 Vkontakte Ltd
              2008-2009 Nikolai Durov
              2008-2009 Andrei Lopatin
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

#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "msg-search-data.h"
#include "msg-search-binlog.h"

#define	VERSION_STR	"msg-search-engine-0.2"

#undef MAX_CONNECTIONS

#define	BACKLOG	8192
#define TCP_PORT 11211
#define MAX_CONNECTIONS	16384

#define READ_AHEAD	(1L << 16)
#define MAX_USERS	(1L << 24)

#define MAX_QUERY_WORDS	16
#define MAX_MESSAGE_WORDS	(1L << 12)
#define MAX_MESSAGE_SIZE	(14L << 10)

#define MAX_USER_DATA	(1L << 25)
#define USER_DATA_BUFF	(1L << 26)
#define MAX_LOADED_USERS	128

#define MAX_INTERMEDIATE_SIZE	(1L << 16)
#define MAX_NET_RES	(1L << 16)

#define BUFF_SIZE	4096


/* in-core loaded user data */

typedef struct loaded_user loaded_user_t;

struct loaded_user {
  int user_id;
  int lru;
  int state;
  char *ptr;
  int size;
  long long start_time;
  struct aiocb aio;
};

/*
 *
 *		SEARCH ENGINE
 *
 */

int verbosity = 0, interactive = 0;

char *fnames[3];
int fd[3];
long long fsize[3];

unsigned char is_letter[256];
char *progname = "search-engine", *username, *binlogname, *logname;
char metaindex_fname_buff[256], binlog_fname_buff[256];

/* stats counters */
int cache_miss;
int start_time;
long long binlog_loaded_size;
long long cache_misses, cache_hits, netw_queries, newmsg_queries, delmsg_queries, search_queries;
long long tot_response_words, tot_response_bytes;
double cache_missed_qt, cache_hit_qt, binlog_load_time;

#define STATS_BUFF_SIZE	(16 << 10)
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

int Qw = 0, MaxQw = MAX_QUERY_WORDS, ResL = 0, ResR = 0;
hash_t Q[MAX_MESSAGE_WORDS];
int Qc[MAX_QUERY_WORDS];
int *Ql[MAX_QUERY_WORDS], *Res;

int ResBuff[MAX_INTERMEDIATE_SIZE];

int process_text (char *text) {
  //fprintf (stderr, "%d %d %s\n", msg_id, order, text);

  Qw = 0;

  while (is_letter[(unsigned char)*text] == 32) text++;

  while (*text) {
    char *sav = text;
    int i;
    union {
      unsigned char data[16];
      hash_t hash;
    } md5_h;

    for (;is_letter[(unsigned char)*text] > 32;++text) *text = is_letter[(unsigned char)*text];

    md5 ((unsigned char *) sav, text - sav, md5_h.data);

    for (;is_letter[(unsigned char)*text] == 32; ++text);

    if (MaxQw <= MAX_QUERY_WORDS) {
      for (i = 0; i < Qw; i++) {
	if (Q[i] == md5_h.hash) {
	  break;
	}
      }
      if (i < Qw) continue;
    }

    if (Qw == MaxQw) {
      return -1;
    }
    Q[Qw++] = md5_h.hash;

    //fprintf (stderr, "%llu\n", md5_h.hash);

  }

  return Qw;
}


/* static userdata loader */

int M_cnt, cur_user;
int udata_len;
userlist_entry_t UE;
pair_t *P;
int *D;
int Pc, Dc;

userlist_entry_t Meta[MAX_METAINDEX_USERS];

int loaded_users_cnt, loaded_users_max, loaded_users_lru, loaded_users_size;
loaded_user_t LoadedUsers[MAX_LOADED_USERS];
char udata[USER_DATA_BUFF+16];

int drop_lru_user (void) {
  int i, j = -1, lru = 0x7fffffff;
  for (i = 0; i < loaded_users_max; i++) {
    if (LoadedUsers[i].state > 0 && LoadedUsers[i].lru < lru) {
      lru = LoadedUsers[i].lru;
      j = i;
    }
  }
  if (j >= 0) {
    if (verbosity > 0) {
      fprintf (stderr, "drop_lru_user(): drop slot #%d at %p, size %d of user %d\n",
	       j, LoadedUsers[j].ptr, LoadedUsers[j].size, LoadedUsers[j].user_id);
    }
    loaded_users_size -= LoadedUsers[j].size;
    LoadedUsers[j].state = 0;
    assert (--loaded_users_cnt >= 0);
    if (j == loaded_users_max - 1) {
      assert (--loaded_users_max >= 0);
    }
  }
  return j;
}

loaded_user_t *alloc_new_preloaded_user (int user_id, int size) {
  int i, j;
  char *prev, *cur;
  loaded_user_t *U;

  assert (size > 0);
  size = (size + 15) & -16;
  if (size >= MAX_USER_DATA) return 0;

  do {

    prev = udata + ((16 - (long) udata) & 15);
    j = -1;

    for (i = 0, U = LoadedUsers; i < loaded_users_max; i++, U++) {
      if (U->state > 0) {
	cur = U->ptr;
	if (verbosity > 0) {
	  fprintf (stderr, "skipping occupied slot #%d at %p, size %d for user %d [prev=%p]\n",
		   i, cur, U->size, U->user_id, prev);
	}
	assert (cur >= prev);
	if (cur >= prev + size) {
	  if (verbosity > 0) {
	    fprintf (stderr, "allocating slot #%d at %p, size %d for user %d\n",
		     j, prev, size, user_id);
	  }
	  assert (j >= 0);
	  U = LoadedUsers + j;
	  loaded_users_cnt++;
	  memset (U, 0, sizeof(loaded_user_t));
	  U->state = 1;
	  U->user_id = user_id;
	  U->ptr = prev;
	  U->size = size;
	  loaded_users_size += size;
	  U->lru = ++loaded_users_lru;
	  return U;
	}
	prev = cur + U->size;
	j = -1;
      } else if (j < 0) {
	j = i;
      }
    }
    if (j < 0) {
      j = loaded_users_max;
    }
    if (j < MAX_LOADED_USERS && prev + size <= udata + USER_DATA_BUFF) {
      if (j == loaded_users_max) {
	loaded_users_max++;
      }
      if (verbosity > 0) {
	fprintf (stderr, "allocating new slot #%d (out of %d) at %p, size %d for user %d\n",
		 j, loaded_users_max, prev, size, user_id);
      }
      U = LoadedUsers + j;
      loaded_users_cnt++;
      memset (U, 0, sizeof(loaded_user_t));
      U->state = 1;
      U->user_id = user_id;
      U->ptr = prev;
      U->size = size;
      loaded_users_size += size;
      U->lru = ++loaded_users_lru;
      return U;
    }
    /* HACK - DROP ALL CACHE IF ALLOCATION FAILS */
    // drop_lru_user();
    if (!loaded_users_cnt) { break; }
    loaded_users_max = loaded_users_cnt = loaded_users_size = 0;

  } while (1);

  return 0;
}

loaded_user_t *find_preloaded_user (int user_id) {
  int i;
  for (i = 0; i < loaded_users_max; i++) {
    if (LoadedUsers[i].user_id == user_id && LoadedUsers[i].state > 0) {
      LoadedUsers[i].lru = ++loaded_users_lru;
      return &LoadedUsers[i];
    }
  }
  return 0;
}

int load_user (int user_id) {
  long long len;
  int a = -1, b = M_cnt, c;
  loaded_user_t *LU;
  // fprintf (stderr, "loading user %d; cnt=%d, 1st=%ld:\n", user_id, b, Meta[0].user_id);
  if (user_id <= 0) { 
    return -1;
  }
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (Meta[c].user_id <= user_id) a = c; else b = c;
  }
  if (a < 0 || Meta[a].user_id != user_id) return cur_user = 0;

  UE = Meta[a];
  assert (UE.offset >= 0 && UE.offset <= fsize[0]);
  assert (UE.hash_cnt >= 0 && UE.list_cnt >= 0);
  len = (long long) UE.hash_cnt * 16 + (long long) UE.list_cnt * 4;
  assert (UE.offset + len <= fsize[0]);
  assert ((unsigned long long) len <= MAX_USER_DATA);

  if (!len) return cur_user = 0;

  LU = find_preloaded_user (user_id);
  c = 1;

  if (!LU) {
    LU = alloc_new_preloaded_user (user_id, len);
    assert (LU);
    assert (lseek (fd[0], UE.offset, SEEK_SET) == UE.offset);
    assert (read (fd[0], LU->ptr, len) == len);
    LU->state = 2;
    c = 0;
    cache_miss++;
  }

  assert (LU->state >= 2); // 1 means "aio_read incomplete"
  LU->state++;
  cur_user = user_id;

  udata_len = len;
  Pc = UE.hash_cnt;
  Dc = UE.list_cnt;
  P = (pair_t *) LU->ptr;
  D = (int *) (P + Pc);
  if (verbosity) {
    fprintf (stderr, "user %d loaded at %p: offset=%lld, bytes=%d, lists=%d, entries=%d %s\n",
	     user_id, P, UE.offset, udata_len, Pc, Dc, c ? "(cache hit)" : "(cache miss)");
  }
  return cur_user;
}

/* ------------- static search ---------------- */

user_mod_header_t *UH;

int lookup_list (hash_t h, int **ptr) {
  int a = -1, b = Pc, c;
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (P[c].hash <= h) a = c; else b = c;
  }

  if (a < 0 || P[a].hash != h) return 0;

  c = P[a].order;
  b = P[a].message_id;
  if (verbosity) {
    fprintf (stderr, "lookup_list: found hash=%llu, message/offset=%d, count=%d\n", h, b, c);
  }
  assert (c > 0 && (c <= Dc || c == 1));

  if (c == 1) {
    if (ptr) *ptr = &P[a].message_id;
    return c;
  }

  assert (b >= 0 && b + c <= Dc);
  if (ptr) *ptr = D + b;
  return c;
}

int *intersect_lists (int *A, int *Ae, int *B, int *Be, int *D, int *De) {
  int x, *C = B;

  while (A < Ae) {
    x = *A++;
    if (x > 0) {
      while (B < Be && (unsigned) *B > (unsigned) x) B++;
      if (B == Be) continue;
      if (*B == x) {
	if (D < De) *D++ = x;
      }
    } else {
      while (C < Be && (unsigned) *C < (unsigned) x) C++;
      if (C == Be) continue;
      if (*C == x) {
	if (D < De) *D++ = x;
      }
    }
  }

  return D;
}

static int prefetch_search_lists (void) {
  int i, j, cnt;
  hash_t h;
  int *L = 0;

  if (!cur_user) {
    for (i = 0; i < Qw; i++) {
      Qc[i] = 0;
    }
    return Qw;
  }

  for (i = 0; i < Qw; i++) {
    h = Q[i];
    cnt = lookup_list (h, &L);
    j = i;
    while (j && Qc[j-1] > cnt) {
      Q[j] = Q[j-1];  Qc[j] = Qc[j-1];  Ql[j] = Ql[j-1];
      j--;
    }
    Q[j] = h;  Qc[j] = cnt;  Ql[j] = L;
  }
  return Qw;
}


int do_search (int first_pos, int first_neg, int last_pos, int last_neg) {
  int i, j, t;

  ResL = 0;
  Res = 0;

  if (!Qw) {
    if (verbosity) {
      fprintf (stderr, "empty query\n");
    }
    return 0;
  }

  if (verbosity) {
    for (i = 0; i < Qw; i++) {
      fprintf (stderr, "%llu *%d .. ", Q[i], Qc[i]);
    }
    fprintf (stderr, "\n");
  }

  Res = Ql[0];
  ResL = Qc[0];

  if (Qc[0] > MAX_INTERMEDIATE_SIZE - ResR) {
    if (verbosity) {
      fprintf (stderr, "list too long: %d entries for %llu\n", Qc[0], Q[0]);
    }
    return -1;
  }

  j = 0;
  for (i = 0; i < ResL; i++) {
    t = Res[i];
    if ((t > first_pos && t < last_pos) || (t < first_neg && t > last_neg)) {
      ResBuff[ResR + j++] = t;
    }
  }

  if (verbosity) {
    fprintf (stderr, "list pruned, only %d entries out of %d remained\n", j, ResL);
  }

  Res = ResBuff + ResR;
  ResL = j;

  
  if (!ResL) {
    if (verbosity) {
      fprintf (stderr, "empty list for %llu\n", Q[0]);
    }
    return 0;
  }

  if (Qw == 1) {
    if (verbosity) {
      fprintf (stderr, "one-word query, loaded %d entries for %llu\n", Qc[0], Q[0]);
    }
    return ResL;
  }

  Res = Ql[0];
  ResL = Qc[0];
  for (i = 1; i < Qw; i++) {
    ResL = intersect_lists (Res, Res+ResL, Ql[i], Ql[i]+Qc[i], ResBuff+ResR, ResBuff+MAX_INTERMEDIATE_SIZE) - ResBuff - ResR;
    Res = ResBuff + ResR;
    if (verbosity > 0) {
      fprintf (stderr, "intersect_lists(): %d entries in result\n", ResL);
    }
    if (!ResL) return 0;
  }

  return ResL;
      
}

int search_in_msg (hash_t *H, int hc) {
  int i, a, b, c;
  // fprintf (stderr, "have %d hashes in this msg: %llu, %llu, ...\n", hc, H[0], H[1]);
  for (i = 0; i < Qw; i++) {
    hash_t h = Q[i];
    a = -1;  b = hc;
    while (b - a > 1) {
      c = (a + b) >> 1;
      if (H[c] > h) { b = c; } else { a = c; }
    }
    if (a < 0 || H[a] != h) { return 0; }
  }
  return 1;
}

/* user loaded OR cur_user=0,  UH points to dynamical data OR 0. */

int do_combined_search (void) {
  message_t *M;
  int prune_neg = 0, prune_pos = 0, first_neg = -0x7fffffff, first_pos = 0x7fffffff;
    
  ResL = 0;

  prefetch_search_lists();

  if (UH) {
    for (M = UH->msgs; M; M = M->prev) {
      if (M->message_id < 0 && M->message_id > first_neg) {
	first_neg = M->message_id;
      }
      if (M->message_id > 0 && M->message_id < first_pos) {
	first_pos = M->message_id;
      }
      if (search_in_msg (M->hashes, M->hc)) {
	ResBuff[ResL++] = M->message_id;
	if (ResL >= MAX_INTERMEDIATE_SIZE) {
	  return ResL;
	}
      }
    }
    prune_pos = UH->pos_to;
    prune_neg = UH->neg_to;
    if (verbosity > 0) {
      fprintf (stderr, "recent_search(): %d messages found\n", ResL);
    }
  }

  ResR = ResL;
  ResL = 0;
  if (cur_user > 0) {
    do_search (prune_pos, prune_neg, first_pos, first_neg);
  }

  ResL += ResR;
  Res = ResBuff;

  if (UH && UH->delmsg_cnt) {
    int *A = UH->delmsg_list, *B = A + UH->delmsg_cnt - 1;
    int i, j = 0, t;
    if (verbosity > 0) {
      fprintf (stderr, "user has %d deleted messages %d %d %d ..., pruning\n", UH->delmsg_cnt, A[0], A[1], A[2]);
    }
    assert (A);
    for (i = 0; i < ResL; i++) {
      t = ResBuff[i];
      if (t < 0) {
	while (A <= B && *A < t) { A++; }
	if (A <= B && *A == t) {
	  A++;
	} else {
	  ResBuff[j++] = t;
	}
      } else {
	while (A <= B && *B > t) { B--; }
	if (A <= B && *B == t) {
	  B--;
	} else {
	  ResBuff[j++] = t;
	}
      }
    }
    if (verbosity > 0) {
      fprintf (stderr, "pruned: resulting list contains %d, original %d\n", j, ResL);
    }
    ResL = j;
  }
  
  return ResL;
}

/*
 *
 *		SERVER
 *
 */



struct connection {
  int fd;
  event_t *ev;
  int state;
  int error;
  char *wra, *wre;
  void *obj;
  int obj_type;
  netbuffer_t *Tmp, In, Out;
  char in_buff[BUFF_SIZE];
  char out_buff[BUFF_SIZE];
};

struct connection Connections[MAX_CONNECTIONS];

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 1, is_server = 0;
struct in_addr settings_addr;
int active_connections;

char *conv_addr(in_addr_t a, char *buf) {
  sprintf (buf, "%d.%d.%d.%d", a&255, (a>>8)&255, (a>>16)&255, a>>24);
  return buf;
}

void free_connection_buffers (struct connection *c) {
  if (c->Tmp) {
    free_buffer (c->Tmp);
    c->Tmp = 0;
  }
  free_all_buffers (&c->In);
  free_all_buffers (&c->Out);
}

int quit_steps;

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

int prepare_stats (void) {
  int uptime = now - start_time;
  int log_uncommitted = compute_uncommitted_log_bytes();

  return stats_buff_len = snprintf (stats_buff, STATS_BUFF_SIZE,
		  "start_time\t%d\n"
		  "current_time\t%d\n"
		  "uptime\t%d\n"
		  "binlog_original_size\t%lld\n"
		  "binlog_loaded_bytes\t%lld\n"
		  "binlog_load_time\t%.6fs\n"
		  "current_binlog_size\t%lld\n"
		  "binlog_uncommitted_bytes\t%d\n"
		  "binlog_path\t%s\n"
		  "binlog_first_timestamp\t%d\n"
		  "binlog_read_timestamp\t%d\n"
		  "binlog_last_timestamp\t%d\n"
		  "indexfile_users\t%d\n"
		  "indexfile_size\t%lld\n"
		  "indexfile_path\t%s\n"
		  "datafile_size\t%lld\n"
		  "datafile_path\t%s\n"
		  "network_connections\t%d\n"
		  "max_network_connections\t%d\n"
		  "active_network_events\t%d\n"
		  "used_network_buffers\t%d\n"
		  "max_network_buffers\t%d\n"
		  "network_buffer_size\t%d\n"
		  "queries_total\t%lld\n"
		  "queries_newmsg\t%lld\n"
		  "queries_delmsg\t%lld\n"
		  "queries_search\t%lld\n"
		  "queries_cache_hit\t%lld\n"
		  "queries_cache_miss\t%lld\n"
		  "queries_time_search\t%.6fs\n"
		  "queries_time_cache_hit\t%.6fs\n"
		  "queries_time_cache_miss\t%.6fs\n"
		  "total_response_words\t%lld\n"
		  "total_response_bytes\t%lld\n"
		  "qps\t%.3f\n"
		  "qps_search\t%.3f\n"
		  "used_dyndata_bytes\t%ld\n"
		  "max_dyndata_bytes\t%ld\n"
		  "dyndata_messages\t%lld\n"
		  "dyndata_messages_bytes\t%lld\n"
		  "dyndata_users\t%d\n"
		  "max_dyndata_users\t%d\n"
		  "cached_users\t%d\n"
		  "max_cached_users\t%d\n"
		  "cached_users_bytes\t%d\n",
		  start_time,
		  now,
		  uptime,
		  binlog_loaded_size,
		  log_readto_pos,
		  binlog_load_time,
		  log_pos,
		  log_uncommitted,
		  binlogname ? (sizeof(binlogname) < 250 ? binlogname : "(too long)") : "(none)",
		  log_first_ts,
		  log_read_until,
		  log_last_ts,
		  M_cnt,
		  fsize[1],
		  strlen(fnames[1]) < 250 ? fnames[1] : "(too long)",
		  fsize[0],
		  strlen(fnames[0]) < 250 ? fnames[0] : "(too long)",
		  active_connections,
		  maxconn,
		  ev_heap_size,
		  NB_used,
		  NB_max,
		  NB_size,
		  netw_queries,
		  newmsg_queries,
		  delmsg_queries,
		  cache_hits + cache_misses,
		  cache_hits,
		  cache_misses,
		  cache_hit_qt + cache_missed_qt,
		  cache_hit_qt,
		  cache_missed_qt,
		  tot_response_words,
		  tot_response_bytes,
		  safe_div(netw_queries, uptime),
		  safe_div(cache_hits+cache_misses, uptime),
		  (long)(dyn_cur - dyn_first),
		  (long)(dyn_last - dyn_first),
		  tot_kept_messages,
		  tot_kept_msg_bytes,
		  u_cnt,
		  3*PRIME/4,
		  loaded_users_cnt,
		  loaded_users_max,
		  loaded_users_size
		  );
}
		  

int return_key_value (struct connection *c, char *key, char *val, int vlen) {
  char buff[512];
  int l = sprintf (buff, "VALUE %s 0 %d\r\n", key, vlen);
  write_out (&c->Out, buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\nEND\r\n", 7);
  return 0;
}

int do_search_query (struct connection *c, char *key, int len) {
  int user_id, max_res, i, w;
  char *ptr, *s, *size_ptr;

  if (strncmp (key, "search(", 7)) { return -1; }
  s = key + 7;
  user_id = strtol (s, &ptr, 10);
  if (ptr == s || *ptr != ',' || user_id <= 0 || user_id > (1L<<30)) { 
    return -1; 
  }
  s = ptr+1;
  max_res = strtol (s, &ptr, 10);
  if (ptr == s || *ptr != ',' || max_res <= 0 || max_res > MAX_NET_RES) {
    return -1;
  }

  MaxQw = MAX_QUERY_WORDS;
  process_text(ptr+1);

  if (verbosity) {
    fprintf (stderr, "%d search query: uid=%d, max_res=%d, keywords=%d, text='%s'\n", now, user_id, max_res, Qw, ptr+1);
  }

  UH = dyn_user_header(user_id,0);
  if (!load_user(user_id) && !UH) {
    return return_key_value (c, key, "no_user", 7);
  }

  if (!Qw) {
    return return_key_value (c, key, "empty_query", 11);
  }

  if (do_combined_search() < 0) {
    return return_key_value (c, key, "search_error", 12);
  }

  ptr = get_write_ptr (&c->Out, 512);
  if (!ptr) return -1;
  s = ptr + 480;
  memcpy (ptr, "VALUE ", 6);
  ptr += 6;
  memcpy (ptr, key, len);
  ptr += len;
  memcpy (ptr, " 0 .........\r\n", 14);
  size_ptr = ptr + 3;
  ptr += 14;
  ptr += w = sprintf (ptr, "%d", ResL);
  for (i = 0; i < ResL && i < max_res; i++) {
    int t;
    if (ptr >= s) {
      advance_write_ptr (&c->Out, ptr - (s - 480));
      ptr = get_write_ptr (&c->Out, 512);
      if (!ptr) return -1;
      s = ptr + 480;
    }
    *ptr++ = ',';  w++;
    w += t = sprintf (ptr, "%d", Res[i]);
    ptr += t;
  }
  size_ptr[sprintf (size_ptr, "% 9d", w)] = '\r';
  memcpy (ptr, "\r\nEND\r\n", 7);
  ptr += 7;
  advance_write_ptr (&c->Out, ptr - (s - 480));

  tot_response_words += ResL;
  tot_response_bytes += w;

  return 0;
}

void hsort (int a, int b) {
  int i, j;
  hash_t h, t;
  if (a >= b) return;
  i = a;
  j = b;
  h = Q[(a+b) >> 1];
  do {
    while (Q[i] < h) i++;
    while (Q[j] > h) j--;
    if (i <= j) {
      t = Q[i];  Q[i++] = Q[j];  Q[j--] = t;
    }
  } while (i <= j);
  hsort (a, j);
  hsort (i, b);
}

void net_readvar_callback (struct connection *c) {
  raw_message_t *Msg;
  int i, j;
  hash_t h, *ptr;

  assert (c->Tmp);
  Msg = (raw_message_t *) c->Tmp->start;

  Msg->data[Msg->len] = 0;

  if (verbosity > 0) {
    fprintf (stderr, "(%d) got new message %d of user %d, len %d : %-40s...\n",
	     Msg->no_reply, Msg->message_id, Msg->user_id, Msg->len, Msg->data);
  }

  free_buffer (c->Tmp);
  c->Tmp = 0;

  if (!Msg->no_reply) {
    write_out (&c->Out, "STORED\r\n", 8);
  }

  MaxQw = MAX_MESSAGE_WORDS;
  process_text (Msg->data);

  if (!Qw) {
    return;
  }

  hsort (0, Qw-1);
  h = Q[0];
  j = 1;

  for (i = 1; i < Qw; i++) {
    if (Q[i] != h) { Q[j++] = h = Q[i]; }
  }

  Qw = j;

  ptr = dyn_alloc_new_msg (Msg->user_id, Msg->message_id, Qw);
  assert (ptr);
  memcpy (ptr, Q, Qw * 8);

  ptr = alloc_log_event (LE_NEWMSG_MAGIC + Qw, Msg->user_id, Msg->message_id, Qw);
  if (ptr) {
    memcpy (ptr, Q, Qw * 8);
  }

  if (verbosity > 0) {
    fprintf (stderr, "stored new message %d of user %d: %d hashes\n", Msg->message_id, Msg->user_id, Qw);
  }

}

    
int exec_mc_store (int op, struct connection *c, char *key, int len, int flags, int expire, int bytes, int no_reply)
{
  int r, uid, msgid;
  raw_message_t *Msg;

  key[len] = 0;

  if (verbosity > 0) {
    fprintf (stderr, "mc_store: op=%d, key=\"%s\", flags=%d, expire=%d, bytes=%d, noreply=%d\n", op, key, flags, expire, bytes, no_reply);
  }

  if (op == 1 && bytes < MAX_MESSAGE_SIZE + 64 && sscanf (key, "newmsg%d,%d", &uid, &msgid) == 2) {

    newmsg_queries++;

    if (!c->Tmp) {
      c->Tmp = alloc_buffer();
      assert (c->Tmp);
    }

    Msg = (raw_message_t *) c->Tmp->start;
    Msg->user_id = uid;
    Msg->message_id = msgid;
    Msg->no_reply = no_reply;
    Msg->len = bytes;

    Msg->data = c->wra = c->Tmp->start + sizeof (raw_message_t);
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
  int uid, msgid;

  while (*str == ' ') { str++; }

  if (verbosity > 0) {
    fprintf (stderr, "delete \"%s\"\n", str);
  }

  switch (*str) {

  case 'd':
    if (sscanf (str, "delmsg%d,%d ", &uid, &msgid) == 2 && uid > 0 && msgid) {
      delmsg_queries++;
      if (verbosity >= 1) {
	fprintf (stderr, "delete_msg(%d,%d)\n", uid, msgid);
      }
      alloc_log_event (LE_DELMSG_MAGIC, uid, msgid, 0);
      dyn_delete_msg (dyn_user_header (uid, 1), msgid);
      return 1;
    }
    break;

  case 'u':
    if (sscanf (str, "undelmsg%d,%d ", &uid, &msgid) == 2 && uid > 0 && msgid) {
      delmsg_queries++;
      if (verbosity >= 1) {
	fprintf (stderr, "undelete_msg(%d,%d)\n", uid, msgid);
      }
      alloc_log_event (LE_UNDELMSG_MAGIC, uid, msgid, 0);
      dyn_undelete_msg (dyn_user_header (uid, 1), msgid);
      return 1;
    }
    break;

  case 'w':
    if (sscanf (str, "wipemsg%d,%d ", &uid, &msgid) == 2 && uid > 0 && msgid) {
      delmsg_queries++;
      if (verbosity >= 1) {
	fprintf (stderr, "wipe_msg(%d,%d)\n", uid, msgid);
      }
      alloc_log_event (LE_DELOLDMSG_MAGIC, uid, msgid, 0);
      dyn_delall_msg (dyn_user_header (uid, 1), msgid);
      return 1;
    }
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
  if (!strcmp(str, "CLOSE") && c->state != -3) {
    if (verbosity) {
      printf ("Closing connection by user's request.\n");
    }
    c->error = -3;
    write_out (&c->Out, "CLOSING\n", 8);
    return -3;
  }

  if (len <= 250 && !strncmp (str, "get search(", 11)) {
    double t = get_utime(CLOCK_MONOTONIC);
    cache_miss = 0;

    int x = do_search_query(c, str+4, len-4);

    t = get_utime(CLOCK_MONOTONIC) - t;
    if (verbosity > 0) {
      fprintf (stderr, "search query time: %.6lfs (cache %s)\n", t, cache_miss ? "miss" : "hit");
    }
    if (cache_miss) { cache_misses++; cache_missed_qt += t; }
    else { cache_hits++; cache_hit_qt += t; }
 
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

  if (len == 9 && !strncmp (str, "get stats", 9)) {
    return_key_value (c, "stats", stats_buff, prepare_stats());
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
  int r, s, fd = c->fd;
  event_t *ev = c->ev;

  if (c->state > 0) {
    // write buffer loop
    s = get_ready_bytes (&c->Out);

    while ((s = get_ready_bytes (&c->Out)) && (!(ev->state & EVT_WRITE) || (ev->ready & EVT_WRITE))) {
      char *to = get_read_ptr (&c->Out);

      r = send (fd, to, s, MSG_DONTWAIT);

      if (verbosity > 0) {
	fprintf (stderr, "send() to %d: %d written out of %d at %p\n", fd, r, s, to);
	if (r < 0) perror ("send()");
      }

      if (r > 0) {
	advance_read_ptr (&c->Out, r);
      }

      if (r < s) {
	ev->ready &= ~EVT_WRITE;
	break;
      }
    }

    if (s > 0) return EVT_WRITE;
      
    c->state = 0;
  }
  return 0;
}

int server_reader (struct connection *c) {
  int res = 0, r, s, fd = c->fd;
  event_t *ev = c->ev;
  char *ptr, *st, *to;

  while ((ev->ready & EVT_READ) || !(ev->state & EVT_READ)) {

    if (c->wra < c->wre) {

      s = c->wre - c->wra;
      r = recv (fd, c->wra, s, MSG_DONTWAIT);
    
      if (r < s) ev->ready &= ~EVT_READ;

      if (verbosity > 0) {
	fprintf (stderr, "inner recv() from %d: %d read out of %d at %p\n", fd, r, s, c->wra);
	if (r < 0) perror ("recv()");
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
      free_all_buffers(&c->In);
      c->error = 8;
      to = get_write_ptr (&c->In, 512);
      s = get_write_space (&c->In);
    }

    assert (to && s > 0);

    r = recv (fd, to, s, MSG_DONTWAIT);

    if (r < s) ev->ready &= ~EVT_READ;

    if (verbosity > 0) {
      fprintf (stderr, "recv() from %d: %d read out of %d at %p\n", fd, r, s, to);
      if (r < 0) perror ("recv()");
    }

    if (r <= 0) break;

    advance_write_ptr (&c->In, r);

    do {
      res = 0;
      ptr = st = get_read_ptr (&c->In);
      to = st + get_ready_bytes (&c->In);

      while (ptr < to && *ptr != 10) ptr++;

      if (ptr == to) {
	break;
      }

      advance_read_ptr (&c->In, ptr + 1 - st);

      res = execute (c, st, ptr - st);

      if (res < 0) {
	break;
      } else if (res > 0) {
	advance_read_ptr (&c->In, res);
	res = 0;
      }

      c->error &= ~8;

    } while(c->wra == c->wre);

    if (res <= -2) break;
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

  res = server_writer(c);
  if (res) return res;

  do {
    res = server_reader(c);
    if (verbosity) {
      fprintf (stderr, "server_reader=%d, ready=%02x, state=%02x\n", res, ev->ready, ev->state);
    }
    if (res > 0 || c->wra) break;
    if (get_ready_bytes(&c->Out)) {
      c->state = 1;
    }
    res = server_writer(c);
  } while (!res && (ev->ready & EVT_READ) && c->state != -3); 

  free_unused_buffers(&c->In);
  free_unused_buffers(&c->Out);

  if (c->error == -3) {
    if (verbosity > 0) {
      fprintf (stderr, "socket %d: closing and cleaning\n", fd);
    }
    free_connection_buffers (c);
    memset (c, 0, sizeof(struct connection));
    ev->data = 0;
    active_connections--;
    // close (fd);
    return EVA_DESTROY;
  }

  return c->state == 1 && !c->wra ? EVT_WRITE | EVT_SPEC : EVT_READ | EVT_SPEC;
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
  flush_binlog_ts();
  exit(EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  printf("SIGTERM handled.\n");
  flush_binlog_ts();
  exit(EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf(stderr, "got SIGHUP.\n");
  flush_binlog_ts();
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf(stderr, "got SIGUSR1, rotate logs.\n");
  flush_binlog_ts();
  reopen_logs();
  signal(SIGUSR1, sigusr1_handler);
}

void cron (void) {
  flush_binlog();
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

  if (binlogname) {
    open_file (2, binlogname, 1);
    set_log_data (fd[2], fsize[2]);
  }
 
  epoll_sethandler (sdf, -10, accept_new_connections, 0);
  epoll_insert (sdf, 7);

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
    epoll_work (1000);
    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sdf);
  close(sdf);
  
  flush_binlog_ts();
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-i] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] <huge-index-file> [<metaindex-file>]\n"
	  "\tPerforms search queries using given indexes\n"
	  "\tIf <metaindex-file> is not specified, <huge-index-file>.idx is used\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-i\twork interactively\n",
	  progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;
  char *ptr;
  static char line[128];

  progname = argv[0];
  while ((i = getopt (argc, argv, "a:b:c:l:p:dihu:v")) != -1) {
    switch (i) {
    case 'v':
      verbosity = 1;
      break;
    case 'i':
      interactive = 1;
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
    case 'a':
      binlogname = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'd':
      daemonize ^= 1;
    }
  }
  if (argc != optind + 1 && argc != optind + 2) {
    usage();
    return 2;
  }

  if (raise_file_rlimit(maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit(1);
  }

  if (!interactive) {
    sdf = server_socket (port, settings_addr, backlog, 0);
    if (sdf < 0) {
      fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
      exit(1);
    }
  }

  init_is_letter ();

  open_file (0, argv[optind], 0);
  if (optind + 1 < argc) {
    open_file (1, argv[optind+1], 0);
  } else {
    assert (strlen (fnames[0]) <= 250);
    sprintf (metaindex_fname_buff, "%s.idx", fnames[0]);
    open_file (1, metaindex_fname_buff, 0);
  }

  if (binlogname) {
    if (binlogname[0] == '.' && strlen(binlogname) + strlen (fnames[0]) < 250) {
      sprintf (binlog_fname_buff, "%s%s", fnames[0], binlogname);
      binlogname = binlog_fname_buff;
    }
    if (verbosity > 0) {
      fprintf (stderr, "opening binlog file %s\n", binlogname);
    }
    open_file (2, binlogname, -1);
  }

  assert (fsize[1] <= sizeof(Meta));
  assert (read (fd[1], Meta, sizeof(Meta)) == fsize[1]);
  M_cnt = fsize[1] / sizeof(userlist_entry_t);
  close (fd[1]);

  if (verbosity) {
    fprintf (stderr, "%d superindex entries loaded (max %ld)\n", M_cnt, MAX_METAINDEX_USERS);
  }

  init_dyn_data();

  if (fsize[2] && fd[2]) {
    if (verbosity) {
      fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, fsize[2]);
    }
    binlog_load_time = get_utime(CLOCK_MONOTONIC);

    clear_log();
    set_log_data(fd[2], fsize[2]);
    replay_log(0);

    binlog_load_time = get_utime(CLOCK_MONOTONIC) - binlog_load_time;
    binlog_loaded_size = fsize[2];

    if (verbosity) {
      fprintf (stderr, "replay binlog file: done, pos=%lld, alloc_mem=%ld out of %ld, time %.06lfs\n", 
	       log_pos, (long)(dyn_cur - dyn_first), (long)(dyn_last - dyn_first), binlog_load_time);
    }
    close (fd[2]);
    fd[2] = -1;
  }

  clear_log();
  start_time = time(0);


  if (!interactive) {

    start_server();

  } else {
    int show = 1;

    do {
      if (show) { puts ("enter user_id followed by query, empty line = exit"); show = 0; }
      if (!fgets (line, 127, stdin) || !*line || line[0] == '\n') break;
      i = strtol (line, &ptr, 10);
      if (ptr == line || i <= 0) { show = 1; continue; }
      Qw = 0;
      if (process_text (ptr) <= 0) { show = 1; continue; }
      if (load_user (i) <= 0) {
	puts ("none");
	continue;
      }
      prefetch_search_lists();
      do_search(0,0,0x7fffffff,-0x7fffffff);
      if (ResL <= 0) {
	puts ("0:[]");
	continue;
      }
      printf ("%d:[", ResL);
      for (i = 0; i < ResL; i++) {
	if (i) { putchar (','); }
	if (i > 100) { printf ("..."); break; }
	printf ("%d", Res[i]);
      }
      printf ("]\n");

    } while (1);
  }

  return 0;
}
