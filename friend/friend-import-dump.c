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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include "server-functions.h"
#include "kdb-friends-binlog.h"
#include "crc32.h"

#define	TF_NONE		0
#define	TF_FRIENDS	1
#define	TF_MEM_PRIV	2
#define	TF_ALBUMS_PRIV	3
#define	TF_VIDEOS_PRIV	4
#define	TF_BLOGS_PRIV	5
#define	TF_MEM_PREFS	6

#define	READ_BUFFER_SIZE	(1 << 24)
#define	WRITE_BUFFER_SIZE	(1 << 24)
#define	READ_THRESHOLD		(1 << 17)
#define	WRITE_THRESHOLD		(1 << 18)

#define	MAX_USERS	(1 << 20)

char *progname = "friend-import-dump.c", *username, *src_fname, *targ_fname;
int verbosity = 0, output_format = 0, table_format = 0, split_mod = 0, split_rem = 0;
int Args_per_line;
int src_fd, targ_fd;
long long rd_bytes, wr_bytes;

char allowed[256];

int line_no, adj_rec;
char *rptr, *rend, *wptr, *wst;
char RB[READ_BUFFER_SIZE+4], WB[WRITE_BUFFER_SIZE+4];

int prepare_read (void) {
  int a, b;
  if (rptr == rend && rptr) {
    return 0;
  }
  if (!rptr) {
    rptr = rend = RB + READ_BUFFER_SIZE;
  }
  a = rend - rptr;
  assert (a >= 0);
  if (a >= READ_THRESHOLD || rend < RB + READ_BUFFER_SIZE) {
    return a;
  }
  memcpy (RB, rptr, a);
  rend -= rptr - RB;
  rptr = RB;
  b = RB + READ_BUFFER_SIZE - rend;
  a = read (src_fd, rend, b);
  if (a < 0) {
    fprintf (stderr, "error reading %s: %m\n", src_fname);
    *rend = 0;
    return rend - rptr;
  } else {
    rd_bytes += a;
  }
  if (verbosity > 0) {
    fprintf (stderr, "read %d bytes from %s\n", a, src_fname);
  }
  rend += a;
  *rend = 0;
  return rend - rptr;
}

void flush_out (void) {
  int a, b = wptr - wst;
  assert (b >= 0);
  if (!b) {
    wptr = wst = WB;
    return;
  }
  a = write (targ_fd, wst, b);
  if (a > 0) {
    wr_bytes += a;
  }
  if (a < b) {
    fprintf (stderr, "error writing to %s: %d bytes written out of %d: %m\n", targ_fname, a, b);
    exit(3);
  }
  if (verbosity > 0) {
    fprintf (stderr, "%d bytes written to %s\n", a, targ_fname);
  }
  wptr = wst = WB;
}

void prepare_write (int x) {
  if (x < 0) {
    x = WRITE_THRESHOLD;
  }
  assert (x > 0 && x <= WRITE_THRESHOLD);
  if (!wptr) {
    wptr = wst = WB;
  }
  if (WB + WRITE_BUFFER_SIZE - wptr < x) {
    flush_out();
  }
}

void *write_alloc (int s) {
  char *p;
  int t = (s + 3) & -4;
  assert (s > 0 && s <= WRITE_THRESHOLD);
  prepare_write (t);
  p = wptr;
  wptr += t;
  while (s < t) {
    p[s++] = LEV_ALIGN_FILL;
  }
  return p;
}

#define MAXV 64
#define MAX_STRLEN 32768
char S[MAXV+1][MAX_STRLEN];
int L[MAXV+1];
long long I[MAXV+1];

int read_record (void) {
  int i, c = '\t', state;
  char *ptr, *end;
  prepare_read();
  assert (Args_per_line > 0 && Args_per_line < MAXV);
  if (rptr == rend) {
    return 0;
  }
  for (i = 0; i < Args_per_line && c != '\n'; i++) {
    assert (c == '\t');
    ptr = &S[i][0];
    end = ptr + MAX_STRLEN - 2;
    state = 0;
    do {
      assert (rptr < rend && ptr < end);
      c = *rptr++;
      if (c == '\n' || c == '\t') {
	if (state == '\\') {
	  ptr--;
	} else {
	  break;
	}
      }
      if (c == '\\' && state == '\\') {
	state = 0;
	ptr--;
      } else {
	state = c;
      }
      *ptr++ = c;
    } while (1);
    *ptr = 0;
    L[i] = ptr - S[i];
    I[i] = atoll(S[i]);
  }
  assert (i == Args_per_line);
  line_no++;
  return 1;
}

int get_dump_format (char *str) {
  if (!strncmp (str, "friends", 7)) {
    return TF_FRIENDS;
  }
  if (!strncmp (str, "members_privacy", 15)) {
    return TF_MEM_PRIV;
  }
  if (!strncmp (str, "mempriv", 7)) {
    return TF_MEM_PRIV;
  }
  if (!strncmp (str, "albumpriv", 9)) {
    return TF_ALBUMS_PRIV;
  }
  if (!strncmp (str, "albums_privacy", 14)) {
    return TF_ALBUMS_PRIV;
  }
  if (!strncmp (str, "videos_privacy", 14)) {
    return TF_VIDEOS_PRIV;
  }
  if (!strncmp (str, "blog_posts_privacy", 18)) {
    return TF_BLOGS_PRIV;
  }
  if (!strncmp (str, "members_prefs", 13)) {
    return TF_MEM_PREFS;
  }

  return TF_NONE;
}

char *fname_last (char *ptr) {
  char *s = ptr;
  while (*ptr) {
    if (*ptr++ == '/') {
      s = ptr;
    }
  }
  return s;
}

/********** some binlog functions ***********/

void start_binlog (int schema_id, char *str) {
  int len = str ? strlen(str)+1 : 0;
  int extra = (len + 3) & -4;
  if (len == 1) { extra = len = 0; }
  struct lev_start *E = write_alloc(sizeof(struct lev_start) - 4 + extra);
  E->type = LEV_START;
  E->schema_id = schema_id;
  E->extra_bytes = extra;
  E->split_mod = split_mod;
  E->split_min = split_rem;
  E->split_max = split_rem + 1;
  if (len) {
    memcpy (E->str, str, len);
  }
}

/*
 *
 *  TABLE/HEAP FUNCTIONS
 *
 */

#define	MAX_GROUPS	(1 << 30)
#define	MAX_GID		(1 << 24)
char *groups_fname, *groups_fname2;
char GT[MAX_GROUPS], *GA, *GB, *GC = GT, *GS = GT;
int Gc, Gd;

void *gmalloc (unsigned size) {
  void *res;
  assert (size <= MAX_GROUPS);
  assert (GS >= GT && GS <= GT + MAX_GROUPS - 8);
  res = GS += (- (long) GS) & 3;
  assert (GT + MAX_GROUPS - GS >= size);
  GS += size;
  return res;
}

/*
 * BEGIN MAIN
 */

int user_id;

int conv_uid (int id) {
  if (id <= 0 || id >= split_mod * MAX_USERS) {
    return -1;
  }
  return id % split_mod == split_rem ? id / split_mod : -1;
}

void log_0ints (long type) {
  struct lev_user_generic *L = write_alloc (8);
  L->type = type;
  L->user_id = user_id;
}
  
void log_1int (long type, long arg) {
  struct lev_user_generic *L = write_alloc (12);
  L->type = type;
  L->user_id = user_id;
  L->a = arg;
}
  
void log_2ints (long type, long arg1, long arg2) {
  struct lev_user_generic *L = write_alloc (16);
  L->type = type;
  L->user_id = user_id;
  L->a = arg1;
  L->b = arg2;
}

/* friends */

struct friend *Friends[MAX_USERS];

struct friend {
  struct friend *next;
  int id;
  int cat;
};

enum {
  fr_id, fr_user_id, fr_friend_id, fr_confirmed, fr_cat,
  fr_END
};

void process_friends_row (void) {
  user_id = I[fr_friend_id];
  int friend_id = I[fr_user_id];
  int uid = conv_uid (user_id);

  if (uid < 0 || friend_id <= 0) {
    return;
  }

  if (I[fr_confirmed] > 0) {
    struct friend *p = gmalloc (sizeof (struct friend));
    p->next = Friends[uid];
    p->id = friend_id;
    p->cat = (I[fr_cat] & 0x1fe) | 1;
    Friends[uid] = p;

    adj_rec++;
  } else if (!I[fr_confirmed]) {
    struct lev_add_friend *E = write_alloc (16);
    E->type = LEV_FR_ADD_FRIENDREQ;
    E->user_id = user_id;
    E->friend_id = friend_id;
    E->cat = (I[fr_cat] & 0x1fe) | 1;

    adj_rec++;
  }

}

#define	MAX_IMPORT_FRIENDS	16384

int lists_output, ext_lists_output;
int list_len_cnt[MAX_IMPORT_FRIENDS+1];

void output_user_friends (int user_id, struct friend *list) {
  int cnt = 0, cnt2 = 0;
  struct friend *p = list;
  if (!list) { 
    return; 
  }
  while (p) {
    if (p->cat != 1) {
      cnt2++;
    }
    p = p->next;
    cnt++;
    assert (cnt <= 1000000);
  }
  p = list;
  if (cnt > MAX_IMPORT_FRIENDS) {
    if (verbosity > 0) {
      fprintf (stderr, "warning: user %d has %d friends, only %d imported\n", 
	user_id, cnt, MAX_IMPORT_FRIENDS);
    }
    while (cnt > MAX_IMPORT_FRIENDS) {
      if (p->cat != 1) {
        cnt2--;
      }
      p = p->next;
      cnt--;
    }
  }
  list_len_cnt[cnt]++;
  if (!cnt2) {
    int *q;

    if (cnt >= 256) {
      struct lev_setlist_long *E = write_alloc (12 + 4*cnt);
      E->type = LEV_FR_SETLIST_LONG;
      E->user_id = user_id;
      E->num = cnt;
      q = E->L + cnt;
    } else {
      struct lev_setlist *E = write_alloc (8 + 4*cnt);
      E->type = LEV_FR_SETLIST + cnt;
      E->user_id = user_id;
      q = E->L + cnt;
    }
    while (p) {
      *--q = p->id;
      assert (p->cat == 1);
      p = p->next;
    }

    lists_output++;

  } else {
    id_cat_pair_t *r;

    if (cnt >= 256) {
      struct lev_setlist_cat_long *E = write_alloc (12 + 8*cnt);
      E->type = LEV_FR_SETLIST_CAT_LONG;
      E->user_id = user_id;
      E->num = cnt;
      r = E->L + cnt;
    } else {
      struct lev_setlist_cat *E = write_alloc (8 + 8*cnt);
      E->type = LEV_FR_SETLIST_CAT + cnt;
      E->user_id = user_id;
      r = E->L + cnt;
    }
    while (p) {
      r--;
      r->id = p->id;
      r->cat = p->cat;
      p = p->next;
    }

    ext_lists_output++;
  }
}

void output_all_friends (void) {
  int i;
  for (i = 0; i < MAX_USERS; i++) {
    if (Friends[i]) {
      output_user_friends (i*split_mod + split_rem, Friends[i]);
    }
  }
}

/* members_privacy */

enum {
  mp_id, mp_notify_mail, mp_event_privacy, mp_pm_privacy, mp_wall_privacy,
  mp_group_invitations, mp_photo_privacy, mp_friends_privacy,
  mp_profile_privacy, mp_m_privacy, mp_h_privacy, mp_i_privacy,
  mp_END
};

privacy_key_t PK_groups_invite, PK_events_invite, PK_mail_send, PK_wall_send,
  PK_graffiti_send, PK_photos_with, PK_friends, PK_profile,
  PK_mobile, PK_home, PK_icq, PK_gifts,
  PK_album, PK_note, PK_notecomm, PK_video;

static privacy_key_t crc32_str (const char *str) {
  return compute_crc32 (str, strlen(str));
}

void prepare_privacy_keys (void) {
#define	PK_TWO(x,y)	{ PK_##x##_##y = (crc32_str(#x) << 32) + crc32_str("_"#y); }
#define	PK_ONE(x)	{ PK_##x = (crc32_str(#x) << 32); }
  PK_TWO (groups, invite);
  PK_TWO (events, invite);
  PK_TWO (mail, send);
  PK_TWO (wall, send);
  PK_TWO (graffiti, send);
  PK_TWO (photos, with);
  PK_ONE (friends);
  PK_ONE (profile);
  PK_ONE (mobile);
  PK_ONE (home);
  PK_ONE (icq);
  PK_ONE (gifts);
  PK_ONE (album);
  PK_ONE (note);
  PK_ONE (notecomm);
  PK_ONE (video);
#undef PK_ONE
#undef PK_TWO
}

void output_conv_privacy (privacy_key_t key, int priv, const char *rules) {
  int res = -1;
  const char *aux = "";
  assert (key);

//  fprintf (stderr, "output_conv_privacy(%016llx, %d, %s)\n", key, priv, rules);

  if (priv < 0) {
    res = ((-priv & 0x1fe) >> 1);
    if (!res) {
      res = CAT_FR_PACKED;
    }
    res |= PL_M_CAT | PL_M_ALLOW;
  } else if (priv*2 < strlen (rules)) {
    aux = rules + priv*2;
  } else {
    aux = "*A";
  }

  if (aux[0] == '*') {
    switch (aux[1]) {
    case '0':
      res = PL_M_ALLOW + PL_M_CAT + CAT_FR_PACKED;
      break;
    case 'A':
      res = PL_M_ALLOW + PL_M_CAT + CAT_FR_ALL;
      break;
    case 'G':
      res = PL_M_ALLOW + PL_M_CAT + CAT_FR_FR;
      break;
    }
  }

  struct lev_set_privacy *E = write_alloc (16 + (res != -1 ? 4 : 0));
  E->type = LEV_FR_SET_PRIVACY_FORCE;
  E->user_id = user_id;
  E->key = key;
  if (res != -1) {
    E->type++;
    E->List[0] = res;
  }

//  fprintf (stderr, " = %08x\n", res);
}

void process_mempriv_row (void) {
  user_id = I[mp_id];
  int uid = conv_uid (user_id);

  if (uid < 0) {
    return;
  }

  output_conv_privacy (PK_groups_invite,	I[mp_notify_mail],	"*G*A*0*A/A");
  output_conv_privacy (PK_events_invite,	I[mp_event_privacy],	"*G*A*0*A/A");
  output_conv_privacy (PK_mail_send,	I[mp_pm_privacy],	"*G*A*0*A/A");
  output_conv_privacy (PK_wall_send,	I[mp_wall_privacy],	"*G*A*0*A/A");
  output_conv_privacy (PK_graffiti_send,I[mp_group_invitations],"*0*A/A");
  output_conv_privacy (PK_photos_with,	I[mp_photo_privacy],	"*G*A*0*A/A");
  output_conv_privacy (PK_friends,	I[mp_friends_privacy],	"*G*A*0*A/A");
  output_conv_privacy (PK_profile,	I[mp_profile_privacy],	"*G*A*0*A/A");
  output_conv_privacy (PK_mobile,	I[mp_m_privacy],	"*G*A*0*A/A");
  output_conv_privacy (PK_home,		I[mp_h_privacy],	"*G*A*0*A/A");
  output_conv_privacy (PK_icq,		I[mp_i_privacy],	"*G*A*0*A/A");

  adj_rec++;
}

/* albums */

enum {
  al_id, al_user_id, al_group_id, al_owner_id, al_type,
  al_END
};

void process_albums_priv_row (void) {
  user_id = I[al_owner_id];
  int uid = conv_uid (user_id);

  if (uid < 0 || user_id <= 0 || user_id != I[al_user_id] || I[al_id] <= 0) {
    return;
  }

  output_conv_privacy (PK_album+I[al_id], I[al_type], "*G*A*0*A/A");

  adj_rec++;
}


/* videos */

enum {
  vi_id, vi_owner_id, vi_user_id, vi_privacy,
  vi_END
};

void process_videos_priv_row (void) {
  user_id = I[vi_owner_id];
  int uid = conv_uid (user_id);

  if (uid < 0 || user_id <= 0 || user_id != I[vi_user_id] || I[vi_id] <= 0) {
    return;
  }

  output_conv_privacy (PK_video+I[vi_id], I[vi_privacy], "*A*0*G*A/A");

  adj_rec++;
}

/* blog_posts */

enum {
  bp_id, bp_user_id, bp_view_privacy, bp_comment_privacy,
  bp_END
};

void process_blogs_priv_row (void) {
  user_id = I[bp_user_id];
  int uid = conv_uid (user_id);

  if (uid < 0 || user_id <= 0 || I[bp_id] <= 0) {
    return;
  }

  output_conv_privacy (PK_note+I[bp_id], I[bp_view_privacy], "*A*G*0*A/A");
  output_conv_privacy (PK_notecomm+I[bp_id], I[bp_comment_privacy], "*A*G*0*A/A");

  adj_rec++;
}

/* members_prefs */

enum {
  me_id, me_member_id, me_key, me_value,
  me_END
};

void process_members_prefs_row (void) {
  user_id = I[me_member_id];
  int uid = conv_uid (user_id);

  if (uid < 0 || user_id <= 0 || strcmp (S[me_key], "giftsVisible")) {
    return;
  }

  output_conv_privacy (PK_gifts, I[me_value], "*G*A*0/A");

  adj_rec++;
}

/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
	   "read: %lld bytes, %d records read, %d processed\n"
	   "written: %lld bytes\n"
	   "temp data: %ld bytes allocated, %d+%d in read/write maps\n"
	   "lists output: %d basic, %d extended\n",
	   rd_bytes, line_no, adj_rec, wr_bytes, (long)(GS - GC), Gc, Gd,
	   lists_output, ext_lists_output);
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-m<rem>,<mod>] [-g<filename>] [-f<format>] [-o<output-class>] <input-file> [<output-file>]\n"
	   "\tConverts tab-separated table dump into KDB binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-u<username>\tassume identity of given user\n"
	   "\t-g<filename>\tloads auxiliary data from given file\n"
	   "\t-m<rem>,<mod>\tslice parameters: consider only users with uid %% mod == rem\n"
	   "\t-o<int>\tdetermines output format\n"
	   "\t-f<format>\tdetermines dump format, one of members_lite, members, education, ...\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvu:m:f:g:o:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'm':
      assert (sscanf(optarg, "%d,%d", &split_rem, &split_mod) == 2);
      assert (split_mod > 0 && split_mod <= 1000 && split_rem >= 0 && split_rem < split_mod);
      break;
    case 'f':
      table_format = get_dump_format(optarg);
      if (!table_format) {
	fprintf (stderr, "fatal: unsupported table dump format: %s\n", optarg);
	return 2;
      }
      break;
    case 'o':
      output_format = atol (optarg);
      break;
    case 'g':
      if (groups_fname) {
        groups_fname2 = optarg;
      } else {
        groups_fname = optarg;
      }
      break;
    case 'u':
      username = optarg;
      break;
    }
  }

  if (optind >= argc || optind + 2 < argc) {
    usage();
    return 2;
  }

  src_fname = argv[optind];

  if (username && change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    return 1;
  }

  if (groups_fname) {
    src_fd = open (groups_fname, O_RDONLY);
    if (src_fd < 0) {
      fprintf (stderr, "cannot open %s: %m\n", groups_fname);
      return 1;
    }
    Gc = read (src_fd, GT, MAX_GROUPS);
    if (verbosity > 0) {
      fprintf (stderr, "read %d bytes from %s\n", Gc, groups_fname);
    }
    assert (Gc >= 0 && Gc < MAX_GROUPS);
    close (src_fd);
    src_fd = 0;
    GA = GT;
    GS = GC = GB = GA + ((Gc + 3) & -4);
  }

  if (groups_fname2) {
    src_fd = open (groups_fname2, O_RDONLY);
    if (src_fd < 0) {
      fprintf (stderr, "cannot open %s: %m\n", groups_fname2);
      return 1;
    }
    Gd = read (src_fd, GB, GA + MAX_GROUPS - GB);
    if (verbosity > 0) {
      fprintf (stderr, "read %d bytes from %s\n", Gd, groups_fname2);
    }
    assert (Gd >= 0 && Gd < MAX_GROUPS);
    close (src_fd);
    src_fd = 0;
    GS = GC = GB + ((Gd + 3) & -4);
  }

  src_fd = open (src_fname, O_RDONLY);
  if (src_fd < 0) {
    fprintf (stderr, "cannot open %s: %m\n", src_fname);
    return 1;
  }

  if (!table_format) {
    table_format = get_dump_format (fname_last (src_fname));
    if (!table_format) {
      fprintf (stderr, "fatal: cannot determine table type from filename %s\n", src_fname);
    }
  }

  if (optind + 1 < argc) {
    targ_fname = argv[optind+1];
    targ_fd = open (targ_fname, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (targ_fd < 0) {
      fprintf (stderr, "cannot create %s: %m\n", targ_fname);
      return 1;
    }
  } else {
    targ_fname = "stdout";
    targ_fd = 1;
  }

  switch (table_format) {
  case TF_FRIENDS:
    assert (split_mod);
    Args_per_line = fr_END;
    start_binlog(FRIENDS_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_friends_row();
    }
    output_all_friends();
    break;
  case TF_MEM_PRIV:
    assert (split_mod);
    prepare_privacy_keys ();
    Args_per_line = mp_END;
    start_binlog(FRIENDS_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_mempriv_row();
    }
    break;
  case TF_ALBUMS_PRIV:
    assert (split_mod);
    prepare_privacy_keys ();
    Args_per_line = al_END;
    start_binlog(FRIENDS_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_albums_priv_row();
    }
    break;
  case TF_VIDEOS_PRIV:
    assert (split_mod);
    prepare_privacy_keys ();
    Args_per_line = vi_END;
    start_binlog(FRIENDS_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_videos_priv_row();
    }
    break;
  case TF_BLOGS_PRIV:
    assert (split_mod);
    prepare_privacy_keys ();
    Args_per_line = bp_END;
    start_binlog(FRIENDS_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_blogs_priv_row();
    }
    break;
  case TF_MEM_PREFS:
    assert (split_mod);
    prepare_privacy_keys ();
    Args_per_line = me_END;
    start_binlog(FRIENDS_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_members_prefs_row();
    }
    break;
  default:
    fprintf (stderr, "unknown table type\n");
    exit(1);
  }

  flush_out();
  if (targ_fd != 1) {
    if (fdatasync(targ_fd) < 0) {
      fprintf (stderr, "error syncing %s: %m", targ_fname);
      exit (1);
    }
    close (targ_fd);
  }

  if (verbosity > 0) {
    output_stats();
  }

  if (verbosity > 1) {
    if (table_format == TF_FRIENDS) {
      for (i = 0; i <= MAX_IMPORT_FRIENDS; i++) {
        printf ("%d\n", list_len_cnt[i]);
      }
    }
  }

  return 0;
}
