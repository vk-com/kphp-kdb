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

#include "msg-search-binlog.h"

#define MAX_LOG_TS_INTERVAL	100

#define DELMSG_LIST_MIN	8

extern int verbosity, now;

char *binlogname;
long long binlog_size;
int binlog_fd;


/* dynamic data management */

int u_cnt, purged_msgs_cnt;
long long tot_kept_messages, tot_kept_msg_bytes;
user_mod_header_t *UH;
int Users[PRIME];
user_mod_header_t *UserModHeaders[PRIME];
char *DynData, *dyn_first, *dyn_cur, *dyn_last;

void init_dyn_data (void) {
  DynData = malloc (DYNAMIC_DATA_BUFFER_SIZE+16);
  assert (DynData);
  dyn_first = DynData + ((16 - (long) DynData) & 15);
  dyn_cur = dyn_first;
  dyn_last = dyn_first + DYNAMIC_DATA_BUFFER_SIZE;
  if (u_cnt) {
    memset (Users, 0, sizeof (Users));
    u_cnt = 0;
  }
}

void *dyn_alloc (int size, int align) {
  char *r = dyn_cur + ((align - (long) dyn_cur) & (align - 1));
  size = (size + 3) & -4;
  assert (size < DYNAMIC_DATA_BUFFER_SIZE);
  if (dyn_last - r < size) {
    if (verbosity > 0) {
      fprintf (stderr, "unable to allocate %d bytes\n", size);
    }
    return 0;
  }
  dyn_cur = r + size;
  return r;
}

user_mod_header_t *dyn_user_header (int user_id, int force) {
  int h = user_id % PRIME;
  int h1 = 1 + user_id % (PRIME - 1);
  user_mod_header_t *R;

  assert (user_id > 0);

  while (Users[h] && Users[h] != user_id) {
    h += h1;
    if (h >= PRIME) { 
      h -= PRIME; 
    }
  }
  if (Users[h] == user_id) {
    if (force < 0) {
      UserModHeaders[h] = 0;
    }
    return UserModHeaders[h];
  }
  if (force <= 0) {
    return 0;
  }

  R = dyn_alloc (sizeof(user_mod_header_t), sizeof(int));
  if (!R) { 
    return 0;
  }
  memset (R, 0, sizeof(user_mod_header_t));

  assert (u_cnt <= 3*PRIME/4);
  ++u_cnt;

  Users[h] = user_id;
  UserModHeaders[h] = R;
  return R;
}

hash_t *dyn_alloc_new_msg (int user_id, int message_id, int hcount) {
  user_mod_header_t *H = dyn_user_header(user_id, 1);
  message_t *R;
  int size = sizeof(message_t) + (hcount - 1)*sizeof(hash_t);
  if (!H) { return 0; }
  R = dyn_alloc (size, sizeof(int));
  if (!R) { return 0; }
  R->prev = H->msgs;
  R->user_id = user_id;
  R->message_id = message_id;
  R->hc = hcount;
  H->msgs = R;
  tot_kept_messages++;
  tot_kept_msg_bytes += size;
  return R->hashes;
}

void dyn_delete_msg (user_mod_header_t *H, int message_id) {
  int a, b, c;
  int *L;
  assert (H);
  if (H->delmsg_cnt == H->delmsg_max) {
    int t = H->delmsg_max << 1;
    if (!t) { t = DELMSG_LIST_MIN; }
    L = dyn_alloc (sizeof(int) * t, sizeof(int));
    if (H->delmsg_max) {
      memcpy (L, H->delmsg_list, H->delmsg_max * sizeof(int));
    }
    H->delmsg_list = L;
    H->delmsg_max = t;
  } else {
    L = H->delmsg_list;
    assert (L);
  }
  assert (H->delmsg_cnt < H->delmsg_max);

  a = -1; 
  b = H->delmsg_cnt;
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (message_id < L[c]) { b = c; } else { a = c; }
  }
  if (a >= 0 && L[a] == message_id) { return; }
  for (c = H->delmsg_cnt; c > b; c--) { L[c] = L[c-1]; }
  L[b] = message_id;
  H->delmsg_cnt++;
}

void dyn_undelete_msg (user_mod_header_t *H, int message_id) {
  int a, b, c;
  int *L;
  assert (H);
  L = H->delmsg_list;
  if (!L) { 
    return;
  }
  assert (L);

  a = -1;
  b = H->delmsg_cnt;
  if (b <= 0) { return; }

  while (b - a > 1) {
    c = (a + b) >> 1;
    if (message_id < L[c]) { b = c; } else { a = c; }
  }
  if (a < 0 || L[a] != message_id) { return; }

  for (b = --H->delmsg_cnt; a < b; a++) {
    L[a] = L[a+1];
  }
  assert (b >= 0);
}

void dyn_delall_msg (user_mod_header_t *H, int message_id) {
  assert (H);
  if (message_id > 0 && H->pos_to < message_id) { 
    H->pos_to = message_id; 
  }
  if (message_id < 0 && H->neg_to > message_id) { 
    H->neg_to = message_id; 
  }
}

void dyn_purge_deleted_user_messages (user_mod_header_t *H) {
  int *A, *B;
  int pos_to, neg_to, msg_id;
  message_t *Msg, **ptr;

  if (!H || !H->delmsg_list || !H->delmsg_cnt || !H->msgs) {
    return;
  }

  A = H->delmsg_list;
  B = A + H->delmsg_cnt - 1;
  pos_to = H->pos_to;
  neg_to = H->neg_to;
  Msg = H->msgs;
  ptr = &H->msgs;

  while (Msg) {
    msg_id = Msg->message_id;
    if (msg_id > 0) {
      while (B >= A && *B > msg_id) { B--; }
      if ((B >= A && *B == msg_id) || msg_id <= pos_to) {
	*ptr = Msg = Msg->prev;
	purged_msgs_cnt++;
	continue;
      }
    } else {
      while (B >= A && *A < msg_id) { A++; }
      if ((B >= A && *A == msg_id) || msg_id >= neg_to) {
	*ptr = Msg = Msg->prev;
	purged_msgs_cnt++;
	continue;
      }
    }
    ptr = &Msg->prev;
    Msg = Msg->prev;
  }
}

void dyn_purge_all_deleted_messages (void) {
  int i;
  for (i = 0; i < PRIME; i++) {
    if (UserModHeaders[i]) {
      dyn_purge_deleted_user_messages(UserModHeaders[i]);
    }
  }
}
    

/* log replay */

int binlog_fd;
char *log_start, *log_rptr, *log_wptr, *log_end, *log_endw;
long long binlog_size, log_pos, log_readto_pos, log_cutoff_pos;
int log_first_ts, log_read_until, log_last_ts;
int log_time_cutoff, log_scan_mode;
char LogBuffer[ULOG_BUFFER_SIZE+16];

int replay_logevent (struct log_event *E, int size) {
  int l = E->type - LE_NEWMSG_MAGIC;
  hash_t *p;

  if (size < 4) { return -2; }

  if ((unsigned) l < LE_NEWMSG_MAGIC_MAX - LE_NEWMSG_MAGIC) {
    if (size < l * 8 + 12) { return -1; }
    if (!log_scan_mode) {
      p = dyn_alloc_new_msg (E->user_id, E->msg_id, l);
      assert (p);
      memcpy (p, ((char *) E) + 12, l * 8);
    }
    return l * 8 + 12;
  }

  switch (E->type) {
  case LE_PAD_MAGIC:
    return 4;
  case LE_DELMSG_MAGIC:
    if (size < 12) { return -2; }
    if (!log_scan_mode) {
      dyn_delete_msg (dyn_user_header (E->user_id, 1), E->msg_id);
    }
    return 12;
  case LE_UNDELMSG_MAGIC:
    if (size < 12) { return -2; }
    dyn_undelete_msg (dyn_user_header (E->user_id, 1), E->msg_id);
    return 12;
  case LE_DELOLDMSG_MAGIC:
    if (size < 12) { return -2; }
    dyn_delall_msg (dyn_user_header (E->user_id, 1), E->msg_id);
    return 12;
  case LE_TIMESTAMP_MAGIC:
    if (size < 8) { return -2; }
    if (!log_first_ts) {
      log_first_ts = E->user_id;
    }
    if (log_read_until > E->user_id) {
      fprintf (stderr, "time goes back from %d to %d in log file\n", log_read_until, E->user_id);
    }
    log_read_until = E->user_id;
    if (E->user_id >= log_time_cutoff && !log_scan_mode) {
      log_cutoff_pos = log_pos - size;
      log_scan_mode = 1;
      if (verbosity) {
	fprintf (stderr, "reached timestamp %d above cutoff %d at binlog position %lld, entering scan mode 1\n",
		 E->user_id, log_time_cutoff, (long long) log_cutoff_pos);
      }
    }
    return 8;
  }
  fprintf (stderr, "unknown magic in log file: %08x\n", E->type);
  return -1;
}
  
void replay_log (int cutoff_time) {
  int r;
  if (!binlog_size) { return; }

  log_time_cutoff = cutoff_time ? cutoff_time : 0x7fffffff;
  log_scan_mode = 0;
  log_cutoff_pos = -1;

  while (1) {
    if (log_rptr == log_wptr) {
      r = read (binlog_fd, log_start, log_end - log_start);
      if (verbosity > 0) {
	fprintf (stderr, "read %d bytes from binlog %s\n", r, binlogname);
      }
      log_rptr = log_start;
      log_wptr = log_start + r;
      log_pos += r;
      if (!r) { break; }
    }
    if (log_rptr >= log_end - (1L << 16)) {
      memcpy (log_start, log_rptr, log_end - log_rptr);
      log_wptr -= (log_rptr - log_start);
      log_rptr = log_start;
      r = read (binlog_fd, log_wptr, log_end - log_wptr);
      if (verbosity > 0) {
	fprintf (stderr, "read %d bytes from binlog %s\n", r, binlogname);
      }
      log_wptr += r;
      log_pos += r;
      if (!r) { break; }
    }
    r = log_wptr - log_rptr;
    if (r < 4) { break; }
    
    r = replay_logevent ((struct log_event *) log_rptr, r);
    if (r < 0) { break; }
    log_rptr += r;
  }
  if (log_rptr < log_wptr && verbosity) {
    r = log_wptr - log_rptr;
    log_pos -= r;
    fprintf (stderr, "replay binlog: %d bytes left unread at position %lld\n", r, (long long) log_pos);
  }
  log_readto_pos = log_pos;
}

void clear_log (void) {
  log_rptr = log_wptr = log_start = LogBuffer + ((16 - (long) LogBuffer) & 15);
  log_end = log_start + ULOG_BUFFER_SIZE;
  log_endw = 0;
  log_last_ts = 0;
  binlog_fd = -1;
  binlog_size = 0;
}

void set_log_data (int logfd, long long logsize) {
  binlog_fd = logfd;
  binlog_size = logsize;
}

void flush_binlog (void) {
  int w, s;
  if (!binlogname || log_rptr == log_wptr) {
    return;
  }
  if (verbosity > 0) {
    fprintf (stderr, "%d flush_binlog()\n", now);
  }
  if (log_endw) {
    assert (log_wptr < log_rptr && log_rptr <= log_endw); 
    s = log_endw - log_rptr;
    if (s > 0) {
      assert (lseek (binlog_fd,log_pos, SEEK_SET) == log_pos);
      w = write (binlog_fd, log_rptr, s);
      if (w < 0) {
	fprintf (stderr, "error writing %d bytes at %lld to %s: %m\n", s, (long long) log_pos, binlogname);
	return;
      }
      log_rptr += w;
      log_pos += w;
      if (w < s) {
	return;
      }
    }
    log_rptr = log_start;
    log_endw = 0;
  }
  assert (log_rptr <= log_wptr);
  s = log_wptr - log_rptr;
  if (s > 0) {
    assert (lseek (binlog_fd, log_pos, SEEK_SET) == log_pos);
    w = write (binlog_fd, log_rptr, s);
    if (w < 0) {
      fprintf (stderr, "error writing %d bytes at %lld to %s: %m\n", s, (long long) log_pos, binlogname);
      return;
    }
    log_rptr += w;
    log_pos += w;
  }
}

/* new log events */

hash_t *alloc_log_event (int type, int user_id, int message_id, int extra) {
  int b = 12 + 8 * extra;
  struct log_event *EV;
  if (!binlogname) {
    return 0;
  }
  if (type == LE_TIMESTAMP_MAGIC) {
    b = 8;
    user_id = now ? now : time(0);
    if (log_last_ts < user_id) {
      log_last_ts = user_id;
    }
  } else if (now > log_last_ts + MAX_LOG_TS_INTERVAL) {
    alloc_log_event (LE_TIMESTAMP_MAGIC, 0, 0, 0);
  }
  if (b > log_end - log_wptr) {
    log_endw = log_wptr;
    log_wptr = log_start;
  }
  if (log_wptr < log_rptr && log_wptr + b >= log_rptr) {
    fprintf (stderr, "fatal: binlog buffer overflow!\n");
    exit(3);
  }
  EV = (struct log_event *) log_wptr;
  log_wptr += b;

  EV->type = type;
  EV->user_id = user_id;

  if (type == LE_TIMESTAMP_MAGIC) {
    return 0;
  }

  EV->msg_id = message_id;
  return (hash_t *) (((char *) EV) + 12);
}

void flush_binlog_ts (void) {
  if (log_last_ts) {
    alloc_log_event (LE_TIMESTAMP_MAGIC, 0, 0, 0);
  }
  flush_binlog();
}

int compute_uncommitted_log_bytes (void) {
  int log_uncommitted = log_wptr - log_rptr;
  if (log_uncommitted < 0) {
    log_uncommitted += log_endw - log_start;
  }
  return log_uncommitted;
}
