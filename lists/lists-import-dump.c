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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include "server-functions.h"
#include "kdb-lists-binlog.h"
#include "crc32.h"

#define	TF_NONE		0
#define	TF_GROUP_MEMBERS	1
#define	TF_MEMBER_GROUPS	2
#define TF_GROUPS_SHORT		3
#define TF_APP_FANS		4
#define TF_FAN_APPS		5
#define TF_APP_FANS_SETTINGS	6
#define TF_FAN_APPS_SETTINGS	7
#define TF_APP_FANS_NOTIFY	8
#define TF_FAN_APPS_NOTIFY	9
#define TF_FAN_APPS_BALANCE	10
#define TF_FAMILY	        11
#define TF_WIDGET_COMMENTS      12
#define TF_WIDGET_VOTES         13
#define TF_BLACKLIST            14
#define TF_BLACKLISTED          15
#define TF_BANLIST              16
#define TF_BANLISTED            17
#define TF_PHOTO_REV            18

#define	READ_BUFFER_SIZE	(1 << 24)
#define	WRITE_BUFFER_SIZE	(1 << 24)
#define	READ_THRESHOLD		(1 << 17)
#define	WRITE_THRESHOLD		(1 << 18)

#define	MAX_USERS	(1 << 23)

char *progname = "lists-import-dump", *username, *src_fname, *targ_fname;
int verbosity = 0, output_format = 0, table_format = 0, split_mod = 0, split_rem = 0, allow_negative = 0;
int Args_per_line;
int src_fd, targ_fd;
long long rd_bytes, wr_bytes;

char allowed[256];

int last_date = 0; // some imports support date

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
  if (!strncmp (str, "member_groups", 13)) {
    return TF_MEMBER_GROUPS;
  }
  if (!strncmp (str, "group_members", 13)) {
    return TF_GROUP_MEMBERS;
  }
  if (!strncmp (str, "groups_short", 12)) {
    return TF_GROUPS_SHORT;
  }
  if (!strncmp (str, "app_fans_settings", 17) || !strncmp (str, "app_members_settings", 20)) {
    return TF_APP_FANS_SETTINGS;
  }
  if (!strncmp (str, "fan_apps_settings", 17) || !strncmp (str, "member_apps_settings", 20)) {
    return TF_FAN_APPS_SETTINGS;
  }
  if (!strncmp (str, "fan_apps_balance", 16) || !strncmp (str, "member_apps_balance", 19)) {
    return TF_FAN_APPS_BALANCE;
  }
  if (!strncmp (str, "app_fans_notify", 15) || !strncmp (str, "app_members_notify", 18)) {
    return TF_APP_FANS_NOTIFY;
  }
  if (!strncmp (str, "fan_apps_notify", 15) || !strncmp (str, "member_apps_notify", 18)) {
    return TF_FAN_APPS_NOTIFY;
  }
  if (!strncmp (str, "app_fans", 8) || !strncmp (str, "app_members", 11)) {
    return TF_APP_FANS;
  }
  if (!strncmp (str, "fan_apps", 8) || !strncmp (str, "member_apps", 11)) {
    return TF_FAN_APPS;
  }
  if (!strncmp (str, "family", 6)) {
    return TF_FAMILY;
  }
  if (!strncmp (str, "widget_comments", 15)) {
    return TF_WIDGET_COMMENTS;
  }
  if (!strncmp (str, "widget_votes", 12)) {
    return TF_WIDGET_VOTES;
  }
  if (!strncmp (str, "blacklist", 10)) {
    return TF_BLACKLIST;
  }
  if (!strncmp (str, "blacklisted", 12)) {
    return TF_BLACKLISTED;
  }
  if (!strncmp (str, "banlist", 8)) {
    return TF_BANLIST;
  }
  if (!strncmp (str, "banlisted", 10)) {
    return TF_BANLISTED;
  }
  if (!strncmp (str, "photorev", 9)) {
    return TF_PHOTO_REV;
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
  struct lev_start *E = write_alloc (sizeof(struct lev_start) - 4 + extra);
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

list_id_t list_id;

int conv_uid (list_id_t id) {
  if (id < 0 && allow_negative) {
    id = -id;
  }
  if (id <= 0 || id >= split_mod * MAX_USERS) {
    return -1;
  }
  return id % split_mod == split_rem ? id / split_mod : -1;
}

int check_id( unsigned id ) {
  // assert (id >= 0 && id < split_mod * MAX_USERS);
  assert (id >= 0);
  return id % split_mod == split_rem;
}

void log_0ints (long type) {
  struct lev_generic *L = write_alloc (8);
  L->type = type;
  L->a = list_id;
}
  
void log_1int (long type, long arg) {
  struct lev_generic *L = write_alloc (12);
  L->type = type;
  L->a = list_id;
  L->b = arg;
}
  
void log_2ints (long type, long arg1, long arg2) {
  struct lev_generic *L = write_alloc (16);
  L->type = type;
  L->a = list_id;
  L->b = arg1;
  L->c = arg2;
}

void log_timestamp (unsigned timestamp) {
  struct lev_generic *L = write_alloc (8);
  L->type = LEV_TIMESTAMP;
  L->a = timestamp;
}

void log_11_set (unsigned l, unsigned e, unsigned value, unsigned flags) {
  struct lev_generic *L = write_alloc (16);
  L->type = LEV_LI_SET_ENTRY + flags;
  L->a = l;
  L->b = e;
  L->c = value;
}

void log_x22_set (unsigned l1, unsigned l2, unsigned e1, unsigned e2, unsigned value, unsigned char flags) {
  struct lev_generic *L = write_alloc (24);
  L->type = LEV_LI_SET_ENTRY + flags;
  L->a = l1;
  L->b = l2;
  L->c = e1;
  L->d = e2;
  L->e = value;
}

void log_x32_set (unsigned l1, unsigned l2, unsigned l3, unsigned e1, unsigned e2, unsigned value, unsigned char flags) {
  struct lev_generic *L = write_alloc (28);
  L->type = LEV_LI_SET_ENTRY + flags;
  L->a = l1;
  L->b = l2;
  L->c = l3;
  L->d = e1;
  L->e = e2;
  L->f = value;
}

/* group_members */

enum {
  gm_id, gm_user_id, gm_group_id, gm_confirmed, gm_who_invited,
  gm_END
};

void process_group_members_row (void) {
  int user_id = I[gm_user_id];
  int confirmed = I[gm_confirmed];
  int who_invited = I[gm_who_invited];
  list_id = I[gm_group_id];  /* ... that's list_id, actually ... */
  if (conv_uid (list_id) < 0 || list_id <= 0 || user_id <= 0 || confirmed <= 0 || confirmed > 2) {
    return;
  }
  if (list_id < Gc) {
    if (GT[list_id] == 3) {
      confirmed += 4;
    } else if (GT[list_id] != 2) {
      return;
    }
  }
  log_2ints (LEV_LI_SET_ENTRY + confirmed + 1, user_id, who_invited > 0 ? who_invited : 0);
  adj_rec++;
}

/* member_groups */

enum {
  mg_id, mg_user_id, mg_group_id, mg_confirmed, mg_who_invited,
  mg_END
};

void process_member_groups_row (void) {
  int group_id = I[mg_group_id];
  int confirmed = I[mg_confirmed];
  int who_invited = I[mg_who_invited];
  list_id = I[mg_user_id];
  if (conv_uid (list_id) < 0 || list_id <= 0 || group_id <= 0 || confirmed <= 0 || confirmed > 2) {
    return;
  }
  if (group_id < Gc) {
    if (GT[group_id] == 3) {
      confirmed += 4;
    } else if (GT[group_id] != 2) {
      return;
    }
  }
  log_2ints (LEV_LI_SET_ENTRY + confirmed + 1, group_id, who_invited > 0 ? who_invited : 0);
  adj_rec++;
}

/* groups_short */

enum {
  gs_id, gs_is_event,
  gs_END
};

void process_groups_short_row (void) {
  int group_id = I[gs_id];
  int is_event = I[gs_is_event];
  if (group_id <= 0 || group_id >= MAX_GROUPS || (is_event & -2)) {
    return;
  }
  if (Gc <= group_id) {
    Gc = group_id + 1;
  }
  GT[group_id] = 2+is_event;
  adj_rec++;
}

/* fan_apps */

enum {
  af_id, af_app_id, af_user_id, af_ordering, af_message, af_END
};

void process_fan_apps_row (void) {
  int app_id = I[af_app_id];
  list_id = I[af_user_id];
  if (conv_uid (list_id) < 0 || app_id <= 0) {
    return;
  }
  log_2ints (LEV_LI_SET_ENTRY + 19, app_id, I[af_ordering]);
  if (L[af_message] > 0 && strcmp (S[af_message], "\\N") && L[af_message] < 256) {
    struct lev_set_entry_text *E = write_alloc (12 + L[af_message]);
    E->type = LEV_LI_SET_ENTRY_TEXT + L[af_message];
    E->list_id = list_id;
    E->object_id = app_id;
    memcpy (E->text, S[af_message], L[af_message]);
  }
  adj_rec++;
}

void process_app_fans_row (void) {
  int user_id = I[af_user_id];
  list_id = I[af_app_id];
  if (conv_uid (list_id) < 0 || !user_id) {
    return;
  }
  log_2ints (LEV_LI_SET_ENTRY + (user_id < 0 ? 23 : 19), user_id, 0);
  adj_rec++;
}


/* fan_apps_settings */

enum {
  as_id, as_user_id, as_app_id, as_coins, as_can_notify,
  as_can_access_friends, as_can_access_photos, as_can_access_audio, as_can_access_video,
  as_END
};

void process_fan_apps_settings_row (void) {
  int app_id = I[as_app_id], flags = 0, i;
  list_id = I[as_user_id];
  if (conv_uid (list_id) < 0 || app_id <= 0) {
    return;
  }
  for (i = as_can_notify; i <= as_can_access_audio; i++) {
    if (I[i]) {
      flags += (1 << i);
    }
  }
  if (!I[as_can_notify]) {
    log_1int (LEV_LI_DECR_FLAGS + (1 << as_can_notify), app_id);
  }
  log_1int (LEV_LI_INCR_FLAGS + flags, app_id);
  adj_rec++;
}

void process_app_fans_settings_row (void) {
  int user_id = I[as_user_id], flags = 0, i;
  list_id = I[as_app_id];
  if (conv_uid (list_id) < 0 || !user_id) {
    return;
  }
  for (i = as_can_notify; i <= as_can_access_audio; i++) {
    if (I[i]) {
      flags += (1 << i);
    }
  }
  if (!I[as_can_notify]) {
    log_1int (LEV_LI_DECR_FLAGS + (1 << as_can_notify), user_id);
  }
  log_1int (LEV_LI_INCR_FLAGS + flags, user_id);
  adj_rec++;
}

void process_fan_apps_balance_row (void) {
  int app_id = I[as_app_id];
  list_id = I[as_user_id];
  if (conv_uid (list_id) < 0 || app_id <= 0 || !I[as_coins]) {
    return;
  }
  log_2ints (LEV_LI_SET_ENTRY + 0, app_id, I[as_coins]);
  adj_rec++;
}

/* app_notify */

enum {
  an_id, an_app_id, an_user_id, an_inviter_id, an_date, an_message,
  an_END
};


void process_fan_apps_notify_row (void) {
  int app_id = I[an_app_id];
  list_id = I[an_user_id];
  if (conv_uid (list_id) < 0 || list_id <= 0 || app_id <= 0) {
    return;
  }
  log_2ints (LEV_LI_ADD_ENTRY + 2, app_id, I[an_inviter_id]);
  adj_rec++;
}

void process_app_fans_notify_row (void) {
  int user_id = I[an_user_id];
  list_id = I[an_app_id];
  if (conv_uid (list_id) < 0 || user_id <= 0) {
    return;
  }
  log_2ints (LEV_LI_ADD_ENTRY + 2, user_id, I[an_inviter_id]);
  adj_rec++;
}

/* widget_comments */

enum {
  wc_id, wc_page, wc_owner, wc_ext, wc_post, wc_app, wc_date, wc_likes, wc_comments, wc_lastcomm,
  wc_END
};

void process_widget_comments_row (void) {
  int likes = I[wc_likes];
  int comments = I[wc_comments];
  int app_id = I[wc_app];
  int date = I[wc_date];
  if (date < last_date) {
    fprintf (stderr, "date goes back by %d seconds\n", last_date - date);
  } else if (date > last_date) {
    log_timestamp (date);
    last_date = date;
  }
  if (!check_id (app_id)) {
    return;
  }
  log_x22_set (app_id, I[wc_page], I[wc_owner], I[wc_post], likes, !!likes * 2 + !!comments);
  adj_rec++;
}

/* widget_votes */

enum {
  wv_id, wv_user, wv_option, wv_voting, wv_sex, wv_age, wv_status, wv_voted,
  wv_END
};

void process_widget_votes_row (void) {
  int user = I[wv_user];
  int voting = I[wv_voting];
  int option = I[wv_option];
  int date = I[wv_voted];
  if (date < last_date - 10) {
    fprintf (stderr, "date goes back by %d seconds\n", last_date - date);
  } else if (date > last_date) {
    log_timestamp (date);
    last_date = date;
  }
  if (check_id (option)) {
    log_11_set (option, user, 0, 0);
    adj_rec++;
  }
  if (check_id (user)) {
    log_11_set (-user, voting, option, 0);
    adj_rec++;
  }
}

/* banlist && banlisted */

enum {
  ba_id, ba_group_id, ba_enemy, ba_admin, ba_date,
  ba_END
};

void process_banlist_row (void) {
  int group_id = I[ba_group_id];
  int enemy = I[ba_enemy];
  int admin = I[ba_admin];
  int date = I[ba_date]; 
  if (enemy <= 0 || group_id <= 0) {
    // fprintf (stderr, "skipping bogus entry (%d,%d,%d,%d)\n", owner, group_id, admin, date);
    return;
  }
  if (!check_id (group_id)) {
    return;
  }
  if (date != -1 && date < last_date - 10) {
    fprintf (stderr, "date goes back by %d seconds\n", last_date - date);
  } else if (date > last_date) {
    log_timestamp (date);
    last_date = date;
  }
  log_11_set (-group_id, enemy, admin, 0);
  adj_rec++;
}

void process_banlisted_row (void) {
  int group_id = I[ba_group_id];
  int enemy = I[ba_enemy];
  int admin = I[ba_admin];
  int date = I[ba_date]; 
  if (enemy <= 0 || group_id <= 0) {
    // fprintf (stderr, "skipping bogus entry (%d,%d,%d,%d)\n", owner, group_id, admin, date);
    return;
  }
  if (!check_id (enemy)) {
    return;
  }
  if (date != -1 && date < last_date - 10) {
    fprintf (stderr, "date goes back by %d seconds\n", last_date - date);
  } else if (date > last_date) {
    log_timestamp (date);
    last_date = date;
  }
  log_11_set (enemy, -group_id, admin, 0);
  adj_rec++;
}

/* blacklist && blacklisted */

enum {
  bl_id, bl_owner, bl_enemy, 
  bl_END
};

void process_blacklist_row (void) {
  int owner = I[bl_owner];
  int enemy = I[bl_enemy];
  if (enemy <= 0 || owner <= 0) {
    // fprintf (stderr, "skipping bogus entry (%d,%d,%d,%d)\n", owner, enemy, admin, date);
    return;
    }
  if (!check_id (owner)) {
    return;
  }
  log_11_set (owner, enemy, 0, 0);
  adj_rec++;
}

void process_blacklisted_row (void) {
  int owner = I[bl_owner];
  int enemy = I[bl_enemy];
  if (enemy <= 0 || owner <= 0) {
    // fprintf (stderr, "skipping bogus entry (%d,%d,%d,%d)\n", owner, enemy, admin, date);
    return;
  }
  if (!check_id (enemy)) {
    return;
  }
  log_11_set (enemy, owner, 0, 0);
  adj_rec++;
}


/* photo_rev */
enum {
  pr_server, pr_owner, pr_id, pr_orig_user, pr_orig_album, pr_photo,
  pr_END
};

void process_photo_rev_row (void) {
  int orig_user = I[pr_orig_user], orig_album = I[pr_orig_album], server = I[pr_server];
  int owner = I[pr_owner], id = I[pr_id];
  if (!check_id (orig_user)) {
    return;
  }
  log_x32_set (orig_user, orig_album, server, owner, id, 0, 0);
  struct lev_generic *E = write_alloc (24 + L[pr_photo]);
  E->type = LEV_LI_SET_ENTRY_TEXT + L[pr_photo];
  E->a = orig_user;
  E->b = orig_album;
  E->c = server;
  E->d = owner;
  E->e = id;
  memcpy (&E->f, S[pr_photo], L[pr_photo]);
  adj_rec++;
}

/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
	   "read: %lld bytes, %d records read, %d processed\n"
	   "written: %lld bytes\n"
	   "temp data: %ld bytes allocated, %d+%d in read/write maps\n",
	   rd_bytes, line_no, adj_rec, wr_bytes, (long)(GS - GC), Gc, Gd
	  );
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-n] [-u<username>] [-m<rem>,<mod>] [-g<filename>] [-f<format>] [-o<output-class>] <input-file> [<output-file>]\n"
	   "\tConverts tab-separated table dump into KDB binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-u<username>\tassume identity of given user\n"
	   "\t-g<filename>\tloads auxiliary data from given file\n"
	   "\t-m<rem>,<mod>\tslice parameters: consider only objects with id %% mod == rem\n"
	   "\t-n\tindex objects with negative ids\n"
	   "\t-o<int>\tdetermines output format\n"
	   "\t-f<format>\tdetermines dump format, one of group_members, app_fans, family, ...\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  set_debug_handlers();
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvnu:m:f:g:o:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'n':
      allow_negative = 1;
      break;
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
  case TF_GROUP_MEMBERS:
    assert (split_mod);
    assert (Gc > 0);
    Args_per_line = gm_END;
    start_binlog(LISTS_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_group_members_row();
    }
    break;
  case TF_MEMBER_GROUPS:
    assert (split_mod);
    assert (Gc > 0);
    Args_per_line = mg_END;
    start_binlog(LISTS_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_member_groups_row();
    }
    break;
  case TF_GROUPS_SHORT:
    Args_per_line = gs_END;
    while (read_record() > 0) {
      process_groups_short_row();
    }
    if (output_format == 1 && Gc) {
      assert (write (targ_fd, GT, Gc) == Gc);
    }
    break;
  case TF_FAN_APPS:
    assert (split_mod);
    allow_negative = 1;
    Args_per_line = af_END;
    start_binlog(LISTS_SCHEMA_V1, "member_apps");
    while (read_record() > 0) {
      process_fan_apps_row();
    }
    break;
  case TF_APP_FANS:
    assert (split_mod);
    Args_per_line = af_END;
    start_binlog(LISTS_SCHEMA_V1, "app_members");
    while (read_record() > 0) {
      process_app_fans_row();
    }
    break;
  case TF_FAN_APPS_SETTINGS:
    assert (split_mod);
    Args_per_line = as_END;
    while (read_record() > 0) {
      process_fan_apps_settings_row();
    }
    break;
  case TF_APP_FANS_SETTINGS:
    assert (split_mod);
    Args_per_line = as_END;
    while (read_record() > 0) {
      process_app_fans_settings_row();
    }
    break;
  case TF_FAN_APPS_NOTIFY:
    assert (split_mod);
    Args_per_line = an_END;
    while (read_record() > 0) {
      process_fan_apps_notify_row();
    }
    break;
  case TF_APP_FANS_NOTIFY:
    assert (split_mod);
    Args_per_line = an_END;
    while (read_record() > 0) {
      process_app_fans_notify_row();
    }
    break;
  case TF_FAN_APPS_BALANCE:
    assert (split_mod);
    start_binlog(LISTS_SCHEMA_V1, "member_apps_balance");
    Args_per_line = as_END;
    while (read_record() > 0) {
      process_fan_apps_balance_row();
    }
    break;
  case TF_FAMILY:
    assert (split_mod);
    start_binlog(LISTS_SCHEMA_CUR, "family");
    break;
  case TF_WIDGET_COMMENTS:
    assert (split_mod);
    Args_per_line = wc_END;
    start_binlog (LISTS_SCHEMA_V3, "\x01\x02\x02\x01");
    while (read_record () > 0) {
      process_widget_comments_row ();
    }
    break;
  case TF_WIDGET_VOTES:
    assert (split_mod);
    Args_per_line = wv_END;
    start_binlog (LISTS_SCHEMA_V1, "");
    while (read_record () > 0) {
      process_widget_votes_row ();
    }
    break;
  case TF_BLACKLIST:
    assert (split_mod);
    Args_per_line = bl_END;
    start_binlog (LISTS_SCHEMA_V1, "");
    while (read_record () > 0) {
      process_blacklist_row ();
    }
    break;
  case TF_BLACKLISTED:
    assert (split_mod);
    Args_per_line = bl_END;
    start_binlog (LISTS_SCHEMA_V1, "");
    while (read_record () > 0) {
      process_blacklisted_row ();
    }
    break;
  case TF_BANLIST:
    assert (split_mod);
    Args_per_line = ba_END;
    //    start_binlog (LISTS_SCHEMA_V1, "");
    while (read_record () > 0) {
      process_banlist_row ();
    }
    break;
  case TF_BANLISTED:
    assert (split_mod);
    Args_per_line = ba_END;
    //    start_binlog (LISTS_SCHEMA_V1, "");
    while (read_record () > 0) {
      process_banlisted_row ();
    }
    break;
  case TF_PHOTO_REV:
    assert (split_mod);
    Args_per_line = pr_END;
    start_binlog (LISTS_SCHEMA_V3, "\x01\x03\x02\x01");
    while (read_record () > 0) {
      process_photo_rev_row ();
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

  return 0;
}
