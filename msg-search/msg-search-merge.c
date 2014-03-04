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

#include "server-functions.h"
#include "msg-search-data.h"
#include "msg-search-binlog.h"

#define DEFAULT_CUTOFF_SECONDS	3600
#define FILE_BUFFER_SIZE	(1L << 25)

#define	MAX_USER_ID	(1L << 27)

#define MAX_PAIRS (1L << 24)
#define MAX_DATA  (1L << 24)
#define MAX_HASHES	(1L << 24)

int verbosity = 0, interactive = 0;
int cutoff_seconds = DEFAULT_CUTOFF_SECONDS;

char *fnames[6];
int fd[6];
long long fsize[6];

char *progname = "msg-search-merge", *username, *binlogname, *logname;
char *binlog_suffix = ".bin", *metaindex_suffix = ".idx";
char old_logname[256], new_logname[256], new_metaindexname[256];

/* stats counters */
long long binlog_loaded_size;
double binlog_load_time;
int start_time, now;

/* file utils */

int open_file (int x, char *fname, int creat) {
  fnames[x] = fname;
  fsize[x] = -1;
  fd[x] = open (fname, creat > 0 ? O_RDWR | O_CREAT : O_RDONLY, 0600);
  if (creat < 0 && fd[x] < 0) {
    fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
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

void fail (void) {
  int i;
  fprintf (stderr, "fatal error, deleting all output files...\n");
  for (i = 2; i <= 4; i++) {
    if (fd[i] >= 0) { close (fd[2]); }
    if (fnames[i]) { 
      fprintf (stderr, "deleting %s\n", fnames[i]); 
      unlink (fnames[i]); 
    }
  }
  exit (2);
}

long long rpos, wpos, log_wpos;
char *riptr, *wiptr, *roptr, *woptr, ibuff[FILE_BUFFER_SIZE], obuff[FILE_BUFFER_SIZE];

static void flush_out (void) {
  int res, len = woptr - roptr;
  if (len > 0) {
    res = write (fd[2], roptr, len);
    if (res < len) {
      fprintf (stderr, "error writing to %s: %d bytes written out of %d: %m\n", fnames[2], res, len);
    }
    assert (res == len);
  }
  woptr = roptr = obuff;
}
    

static void write_out (const void *data, int size) {
  int len;
  assert ((unsigned) size < FILE_BUFFER_SIZE * 8);
  if (!woptr) { woptr = roptr = obuff; }
  while (size > 0) {
    len = obuff + FILE_BUFFER_SIZE - woptr;
    if (len > size) { len = size; }
    assert (len > 0);
    memcpy (woptr, data, len);
    data = ((char *) data) + len;
    woptr += len;
    size -= len;
    wpos += len;
    if (woptr == obuff + FILE_BUFFER_SIZE) {
      flush_out();
    }
  }
}    

/* data */

user_header_t *UserHdr, User;
user_mod_header_t *UserMod;
int cur_userhdr_size;
pair_t *CurH;
int *CurL;

long long tot_dropped_pairs, tot_imported_pairs, tot_Dc, tot_Qc;
int max_Dc, max_Qc, max_Dc_user, max_Qc_user, tot_binlog_messages, users_dropped, users_added;

int M_cnt;
userlist_entry_t MetaA[MAX_METAINDEX_USERS], MetaB[MAX_METAINDEX_USERS], *MetaP[MAX_METAINDEX_USERS];

int load_next_user (void) {
  int rb, watermark = sizeof(user_header_t);

  riptr += cur_userhdr_size;
  rpos += cur_userhdr_size;
  cur_userhdr_size = User.user_id = 0;
  assert (riptr <= wiptr);

  while (1) {
    if (riptr == wiptr) {
      riptr = wiptr = ibuff;
    }
    if (wiptr - riptr < watermark) {
      memcpy (ibuff, riptr, wiptr - riptr);
      wiptr -= riptr - ibuff;
      riptr = ibuff;
      rb = read (fd[0], wiptr, ibuff + FILE_BUFFER_SIZE - wiptr);
      if (rb < 0) {
	fprintf (stderr, "fatal: error reading file %s at position %lld: %m\n", fnames[0], (long long) rpos);
	fail();
	return -1;
      }
      wiptr += rb;
    }
    if (wiptr == riptr) {
      return 0; // EOF
    }
    if (wiptr - riptr < watermark) {
      fprintf (stderr, "error reading file %s at pos %lld: %ld bytes available, %d required\n",
	       fnames[0], (long long) rpos, (long) (wiptr - riptr), watermark);
      return 0;
    }

    UserHdr = (user_header_t *) riptr;

    assert (UserHdr->magic == MAIL_INDEX_BLOCK_MAGIC);
    assert (UserHdr->user_id > 0);
    assert ((unsigned) UserHdr->hash_cnt <= MAX_HASHES && (unsigned) UserHdr->list_cnt <= MAX_DATA);

    cur_userhdr_size = sizeof (user_header_t) + UserHdr->hash_cnt * 16 + UserHdr->list_cnt * 4;

    if (wiptr - riptr < cur_userhdr_size) {
      watermark = cur_userhdr_size;
      continue;
    }

    memcpy (&User, UserHdr, sizeof (user_header_t));
    CurH = (pair_t *) (riptr + sizeof (user_header_t));
    CurL = (int *) (riptr + sizeof (user_header_t) + User.hash_cnt * 16);

    return User.user_id;
  }

  return -1;
}

int Pc, Qc, Dc, Dc0, dropped_pairs;
pair_t P[MAX_PAIRS], Q[MAX_PAIRS];
int D[MAX_DATA];

static void my_psort (int a, int b) {
  pair_t h, t;
  int i, j;
  if (a >= b) return;
  i = a;  j = b;
  h = P[(a+b)>>1];
  do {
    while (P[i].hash < h.hash || (P[i].hash == h.hash && P[i].order < h.order)) i++;
    while (P[j].hash > h.hash || (P[j].hash == h.hash && P[j].order > h.order)) j--;
    if (i <= j) {
      t = P[i];  P[i++] = P[j];  P[j--] = t;
    }
  } while (i <= j);
  my_psort (a, j);
  my_psort (i, b);
}

static void prune_list (int *start, int *stop, int pos_thr, int neg_thr) {
  int *A, *B;
  int pos_to, neg_to, msg_id;

  if (UserMod) {
    pos_to = UserMod->pos_to;
    neg_to = UserMod->neg_to;
    A = UserMod->delmsg_list;
    if (A) {
      B = A + UserMod->delmsg_cnt - 1;
    } else {
      B = 0;
      A = B + 1;
    }
  } else {
    pos_to = neg_to = 0;
    B = 0;
    A = B + 1;
  }

  while (start < stop) {
    msg_id = *start++;
    if (msg_id > 0) {
      while (A <= B && *B > msg_id) { B--; }
      if ((A <= B && msg_id == *B) || msg_id <= pos_to || msg_id >= pos_thr) {
	dropped_pairs++;
      } else {
	D[Dc++] = msg_id;
      }
    } else {
      while (A <= B && *A < msg_id) { A++; }
      if ((A <= B && msg_id == *A) || msg_id >= neg_to || msg_id <= neg_thr) {
	dropped_pairs++;
      } else {
	D[Dc++] = msg_id;
      }
    }
  }
}

static void merge_hash_lists (pair_t *old_list, pair_t *start, int cnt) {
  int pos_thr = 0x7fffffff, neg_thr = -0x7fffffff, i;
  hash_t h;

  if (cnt) { 
    h = start->hash; 
  } else if (old_list) {
    h = old_list->hash;
  } else {
    return;
  }

  Dc0 = Dc;
  assert (Dc + cnt <= MAX_DATA);

  for (i = 0; i < cnt; i++) {
    int msg_id = (start++)->message_id;
    if (msg_id > 0) { pos_thr = msg_id; } else { neg_thr = msg_id; }
    D[Dc++] = msg_id;
  }

  if (old_list && old_list->order) {
    int cnt = old_list->order;
    int *ptr = (cnt == 1 ? &old_list->message_id : CurL + old_list->message_id);
    assert ((unsigned) cnt < MAX_DATA && Dc + cnt <= MAX_DATA);
    prune_list (ptr, ptr+cnt, pos_thr, neg_thr);
  }

  if (Dc > Dc0) {
    assert (Qc < MAX_PAIRS);
    Q[Qc].hash = h;
    Q[Qc].order = Dc - Dc0;
    if (Dc - Dc0 > 1) {
      Q[Qc++].message_id = Dc0;
    } else {
      Q[Qc++].message_id = D[Dc0];
      Dc = Dc0;
    }
  }
}

static void add_to_userlist (void) {
  userlist_entry_t *P = MetaA + M_cnt;
  assert (M_cnt < MAX_METAINDEX_USERS);
  if (User.hash_cnt <= 0) { return; }

  P->user_id = User.user_id;
  P->hash_cnt = User.hash_cnt;
  P->list_cnt = User.list_cnt;
  P->file_no = 0;
  P->offset = wpos + sizeof(user_header_t);
  MetaP[M_cnt++] = P;

  tot_dropped_pairs += dropped_pairs;
  dropped_pairs = 0;
  tot_Dc += User.list_cnt;
  if (User.list_cnt > max_Dc) { max_Dc = User.list_cnt; max_Dc_user = User.user_id; }
  tot_Qc += User.hash_cnt;
  if (User.hash_cnt > max_Qc) { max_Qc = User.hash_cnt; max_Qc_user = User.user_id; }

}
  
static void do_merge (void) {
  message_t *Msg;
  int i, j, k;
  if (!UserMod) {
    add_to_userlist ();
    write_out (riptr, cur_userhdr_size);
    return;
  }

  Pc = 0;

  for (i = 1, Msg = UserMod->msgs; Msg; Msg = Msg->prev, i++) {
    int msg_id = Msg->message_id;
    hash_t *ptr = Msg->hashes;
    tot_binlog_messages++;
    tot_imported_pairs += Msg->hc;
    for (j = Msg->hc; j; j--) {
      assert (Pc < MAX_PAIRS);
      P[Pc].hash = *ptr++;
      P[Pc].order = i;
      P[Pc++].message_id = msg_id;
    }
  }

  my_psort (0, Pc-1);

  Qc = Dc = k = 0;

  for (i = 0, j = 0; i < Pc; i = j) {
    hash_t h = P[i].hash;
    while (k < User.hash_cnt && CurH[k].hash < h) {
      merge_hash_lists (&CurH[k++], P+i, 0);
    }
    while (j < Pc && P[j].hash == h) { j++; }
    if (k < User.hash_cnt && CurH[k].hash == h) {
      merge_hash_lists (&CurH[k++], P+i, j-i);
    } else {
      merge_hash_lists (0, P+i, j-i);
    }
  }

  while (k < User.hash_cnt) {
    merge_hash_lists (&CurH[k++], P+i, 0);
  }

  User.hash_cnt = Qc;
  User.list_cnt = Dc;
  if (Qc) {
    add_to_userlist ();
    write_out (&User, sizeof(User));
    write_out (Q, Qc * 16);
    write_out (D, Dc * 4);
  } else {
    users_dropped++;
    if (verbosity > 1) {
      fprintf (stderr, "old user %d dropped from index (no messages left)\n", User.user_id);
    }
  }
}

static void my_sort (int a, int b) {
  userlist_entry_t *t;
  int h, i, j;
  if (a >= b) return;
  i = a;  j = b;
  h = MetaP[(a+b)>>1]->user_id;
  do {
    while (MetaP[i]->user_id < h) i++;
    while (MetaP[j]->user_id > h) j--;
    if (i <= j) {
      t = MetaP[i];  MetaP[i++] = MetaP[j];  MetaP[j--] = t;
    }
  } while (i <= j);
  my_sort (a, j);
  my_sort (i, b);
}


    
/*
 *
 *		MAIN
 *
 */

int percent (long long a, long long b) {
  if (b <= 0 || a <= 0) return 0;
  if (a >= b) return 100;
  return (a*100 / b);
}

void output_stats (void) {
  fprintf (stderr, "\nmessages purged from binlog: %d\n", purged_msgs_cnt);
  fprintf (stderr, "messages imported from binlog: %d\n", tot_binlog_messages);
  fprintf (stderr, "pairs imported from binlog: %lld\n", tot_imported_pairs);
  fprintf (stderr, "pairs dropped from old data: %lld\n", tot_dropped_pairs);
  fprintf (stderr, "total pairs output: %lld\n", tot_Dc);
  fprintf (stderr, "old users dropped: %d\n", users_dropped);
  fprintf (stderr, "new users added: %d\n", users_added);
  fprintf (stderr, "maximal pairs used: %d out of %ld (%d%%) for user %d\n", max_Dc, MAX_DATA, percent(max_Dc, MAX_DATA), max_Dc_user);
  fprintf (stderr, "total hashes output: %lld\n", tot_Qc);
  fprintf (stderr, "maximal hashes used: %d out of %ld (%d%%) for user %d\n", max_Qc, MAX_HASHES, percent(max_Qc, MAX_HASHES), max_Qc_user);
  fprintf (stderr, "output users: %d out of %ld (%d%%)\n", M_cnt, MAX_METAINDEX_USERS, percent(M_cnt, MAX_METAINDEX_USERS));
  fprintf (stderr, "binlog size: old %lld, new %lld\n", (long long) fsize[1], (long long) log_wpos);
  fprintf (stderr, "huge index size: old %lld, new %lld\n", (long long) fsize[0], (long long) wpos);
  fprintf (stderr, "used time: %ld seconds\n\n", time(0) - start_time);
}

void usage (void) {
  printf ("usage: %s [-v] [-u<username>] [-c<cutoff-seconds>] [-a<binlog-suffix>] [-i<metaindex-suffix>] <old-huge-index-file> <new-huge-index-file>\n"
	  "\tCombines <old-huge-index-file> with corresponding binlog, obtained by appending <binlog-suffix> to <old-huge-index-file>, into <new-huge-index-file>, generating corresponding new binlog and metaindex files\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-u<username>\tassume identity of specified user (if invoked with root privileges)\n"
	  "\t-c<cutoff-seconds>\tscan old binlog until <cutoff-seconds> ago, after that point process only undeletion operations (3600)\n"
	  "\t-a<binlog-suffix>\tsuffix used to obtain binlog filename from huge index filename (.bin)\n"
	  "\t-i<metaindex-suffix>\tsuffix used to obtain metaindex filename (.idx)\n",
	  progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;

  progname = argv[0];
  while ((i = getopt (argc, argv, "hvu:c:a:i:")) != -1) {
    switch (i) {
    case 'v':
      verbosity = 1;
      break;
    case 'h':
      usage();
      return 2;
    case 'u':
      username = optarg;
      break;
    case 'a':
      binlog_suffix = optarg;
      break;
    case 'i':
      metaindex_suffix = optarg;
      break;
    case 'c':
      cutoff_seconds = atoi(optarg);
      if (cutoff_seconds > 1000000 || cutoff_seconds < 0) {
	cutoff_seconds = DEFAULT_CUTOFF_SECONDS;
      }
    }
  }
  if (argc != optind + 2) {
    usage();
    return 2;
  }

  if (username && change_user(username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  open_file (0, argv[optind], 0);
  snprintf (old_logname, 254, "%s%s", argv[optind], binlog_suffix);
  binlogname = old_logname;
  open_file (1, old_logname, 0);

  open_file (2, argv[optind+1], 1);
  if (fsize[2]) {
    fprintf (stderr, "new huge index file %s already exists and is not empty\n", fnames[2]);
    return 1;
  }

  snprintf (new_logname, 254, "%s%s", argv[optind+1], binlog_suffix);
  open_file (3, new_logname, 1);
  if (fsize[3]) {
    fprintf (stderr, "new binlog file %s already exists and is not empty\n", fnames[3]);
    return 1;
  }

  snprintf (new_metaindexname, 254, "%s%s", argv[optind+1], metaindex_suffix);
  open_file (4, new_metaindexname, 1);
  if (fsize[4]) {
    fprintf (stderr, "new metaindex file %s already exists and is not empty\n", fnames[4]);
    return 1;
  }

  init_dyn_data();

  start_time = now = time(0);

  if (fsize[1] && fd[1]) {
    if (verbosity) {
      fprintf (stderr, "replaying binlog file %s (size %lld), cutoff time=%d\n", binlogname, fsize[1], start_time - cutoff_seconds);
    }
    binlog_load_time = get_utime(CLOCK_MONOTONIC);

    clear_log ();
    set_log_data (fd[1], fsize[1]);
    replay_log (start_time - cutoff_seconds);

    binlog_load_time = get_utime(CLOCK_MONOTONIC) - binlog_load_time;
    binlog_loaded_size = log_pos;

    if (log_cutoff_pos < 0) {
      log_cutoff_pos = log_pos;
    }

    if (verbosity) {
      fprintf (stderr, "replay binlog file: done, pos=%lld, cutoff_pos=%lld, alloc_mem=%ld out of %ld, time %.06lfs\n", 
	       (long long) log_pos, (long long) log_cutoff_pos, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), binlog_load_time);
    }
  }

  clear_log();
  dyn_purge_all_deleted_messages();

  if (verbosity) {
    fprintf (stderr, "purged %d recently added and deleted messages\n", purged_msgs_cnt);
    fprintf (stderr, "scanning data file \"%s\", size=%lld\n", fnames[0], fsize[0]);
  }

  while (load_next_user() > 0) {
    if (User.user_id && (unsigned) User.user_id < MAX_USER_ID) {
      UserMod = dyn_user_header (User.user_id, 0);
      if (UserMod && UserMod->delmsg_cnt == -239) {
	fprintf (stderr, "duplicate user id %d in file %s at position %lld\n", User.user_id, fnames[0], (long long) rpos);
      } else {
	// fprintf (stderr, "processing user %ld\n", User.user_id);
	do_merge ();
	if (UserMod) {
	  UserMod->delmsg_cnt = -239;
	}
      }
    } else {
      users_dropped++;
      fprintf (stderr, "dropping messages of user %d (user_id out of bounds)\n", User.user_id);
    }
  }

  if (verbosity) {
    fprintf (stderr, "scanning new users...\n");
  }

  User.magic = MAIL_INDEX_BLOCK_MAGIC;

  for (i = 0; i < PRIME; i++) {
    UserMod = UserModHeaders[i];
    if (UserMod && UserMod->delmsg_cnt != -239) {
      User.user_id = Users[i];
      users_added++;
      if (User.user_id && (unsigned) User.user_id < MAX_USER_ID) {
	User.hash_cnt = User.list_cnt = 0;
	CurH = 0;
	CurL = 0;
	if (verbosity > 1) {
	  fprintf (stderr, "adding new user %d\n", User.user_id);
	}
	do_merge ();
      } else {
	users_dropped++;
	fprintf (stderr, "ignoring messages of new user %d (user_id out of bounds)\n", User.user_id);
      }
    }
  }
 
  flush_out();

  if (verbosity) {
    fprintf (stderr, "file %s created, total size %lld\n", fnames[2], (long long) wpos);
  }
  close (fd[2]);

  my_sort (0, M_cnt - 1);

  for (i = 0; i < M_cnt; i++) {
    MetaB[i] = *MetaP[i];
  }
  i = write (fd[4], MetaB, M_cnt * sizeof(userlist_entry_t));
  assert (i == M_cnt * sizeof(userlist_entry_t));
  close (fd[4]);

  if (verbosity) {
    fprintf (stderr, "%d userlist entries (%ld bytes) written to %s\n", M_cnt, (long)M_cnt*sizeof(userlist_entry_t), fnames[4]);
  }

  assert (lseek (fd[1], log_cutoff_pos, SEEK_SET) >= 0);
  do {
    i = read(fd[1], obuff, FILE_BUFFER_SIZE);
    assert (write (fd[3], obuff, i) == i);
    log_wpos += i;
  } while (i);
  close (fd[3]);

  if (verbosity) {
    fprintf (stderr, "%lld bytes copied from old binlog %s, position %lld to new binlog %s\n", (long long) log_wpos, fnames[1], (long long) log_cutoff_pos, fnames[3]);
    output_stats();
  }

  return 0;
}
