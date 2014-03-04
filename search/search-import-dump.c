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
#include "kdb-search-binlog.h"

#define	TF_NONE		0
#define	TF_AUDIO	1
#define	TF_VIDEO	2
#define	TF_APPS		3
#define	TF_GROUPS	4
#define	TF_EVENTS	5
#define	TF_BLOG_POSTS	6
#define	TF_MEMLITE	7
#define	TF_MARKET_ITEMS	8
#define	TF_QUESTIONS	9
#define	TF_TOPICS	10
#define	TF_MINIFEED	11

#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 17)
#define	WRITE_THRESHOLD		(1L << 16)

char *progname = "search-import-dump", *username, *src_fname, *targ_fname, *groups_fname;
int verbosity = 0, output_format = 0, table_format = 0, split_mod = 0, split_rem = 0;
int Args_per_line;
int src_fd, targ_fd, map_fd;
long long rd_bytes, wr_bytes;

int line_no, adj_rec;
char *rptr, *rend, *wptr, *wst;
char RB[READ_BUFFER_SIZE+4], WB[WRITE_BUFFER_SIZE+4];


#define	MAP_SIZE	(1 << 24)
char Map[MAP_SIZE];
int map_size, map_changes;

void store_map (int x, int v) {
  if (x <= 0 || x >= MAP_SIZE) { return; }
  if (x >= map_size) { map_size = x + 1; }
  Map[x] = v;
  map_changes++;
}

int get_map (int x) {
  if (x >= 0 && x < map_size) { return Map[x]; }
  return -1;
}

void load_map (int force) {
  if (!groups_fname) {
    if (!force) {
      fprintf (stderr, "warning: no map file specified\n");
    } else {
      fprintf (stderr, "fatal: no map file specified\n");
      exit (2);
    }
    return;
  }
  map_fd = open (groups_fname, O_RDONLY);
  if (map_fd < 0) {
    fprintf (stderr, "cannot open map file %s: %m", groups_fname);
    if (force) {
      exit (2);
    }
    return;
  }
  map_size = read (map_fd, Map, MAP_SIZE);
  close (map_fd);
  if (map_size < 0) {
    map_size = 0;
  }
  if (verbosity > 0) {
    fprintf (stderr, "read %d bytes from map file %s\n", map_size, groups_fname);
  }
}


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
#define MAX_STRLEN 65536
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
#define IS_PFX(s)	!strncmp(str,s,sizeof(s)-1)
  if (IS_PFX("audio")) {
    return TF_AUDIO;
  }

  if (IS_PFX("video")) {
    return TF_VIDEO;
  }

  if (IS_PFX("applications")) {
    return TF_APPS;
  }

  if (IS_PFX("groups")) {
    return TF_GROUPS;
  }

  if (IS_PFX("events")) {
    return TF_EVENTS;
  }

  if (IS_PFX("blog_posts")) {
    return TF_BLOG_POSTS;
  }

  if (IS_PFX("memlite")) {
    return TF_MEMLITE;
  }

  if (IS_PFX("members_lite")) {
    return TF_MEMLITE;
  }

  if (IS_PFX("market_items")) {
    return TF_MARKET_ITEMS;
  }

  if (IS_PFX("question")) {
    return TF_QUESTIONS;
  }

  if (IS_PFX("topics")) {
    return TF_TOPICS;
  }

  if (IS_PFX("minifeed")) {
    return TF_MINIFEED;
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
 * BEGIN MAIN
 */

int fits (int id) {
  if (!split_mod) {
    return 1;
  }
  if (id < 0) {
    id = -id;
  }
  return id % split_mod == split_rem;
}

void log_1int (long type, long arg) {
  struct lev_generic *L = write_alloc (8);
  L->type = type;
  L->a = arg;
}

void log_2ints (long type, long arg1, long arg2) {
  struct lev_generic *L = write_alloc (12);
  L->type = type;
  L->a = arg1;
  L->b = arg2;
}


int make_tag (char *tag, int type, unsigned value) {
  char *p = tag;
  *p++ = 0x1f;
  *p++ = type;
  while (value >= 0x40) {
    *p++ = 0x80 + (value & 0x7f);
    value >>= 7;
  }
  *p++ = 0x40 + value;
  *p = 0;
  return p - tag;
}

/* audio */

enum {
  au_id, au_owner_id, au_user_id, au_group_id, au_server,
  au_source, au_source_owner_id, au_source_user_id,
  au_uploaded, au_position, au_duration, au_folder_id,
  au_has_lyrics, au_hidden, au_num, au_rate, au_privacy,
  au_bytes, au_md5_high, au_md5_low,
  au_ip, au_port, au_front, au_ua_hash,
  au_performer, au_title, au_audio,
  au_END
};

void process_audio_row (void) {
  char *p, *q;
  int len, i, c;
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (I[au_source] || !fits(I[au_owner_id])) {
    return;
  }

  len = L[au_performer] + L[au_title] + 1;
  if (len > 4095) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = (I[au_has_lyrics] > 0);
    LS->rate2 = I[au_duration];
    LS->obj_id = (I[au_id] << 32) + (unsigned) I[au_owner_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = (I[au_has_lyrics] > 0);
    LL->rate2 = I[au_duration];
    LL->obj_id = (I[au_id] << 32) + (unsigned) I[au_owner_id];
    LL->text_len = len;
    q = LL->text;
  }

  p = S[au_performer];
  for (i = L[au_performer]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = '\t';

  p = S[au_title];
  for (i = L[au_title]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;

}

/* video */

enum {
  vi_id, vi_owner_id, vi_user_id, vi_ordering, vi_folder_id,
  vi_privacy, vi_comm_privacy, vi_date, vi_uploaded, vi_updated,
  vi_duration, vi_num, vi_read_num, vi_views,
  vi_source, vi_source_owner, vi_source_user, vi_reply_to, vi_reply_to_owner,
  vi_rate, vi_server, vi_random_tag, vi_title, vi_description,
  vi_END
};

void process_video_row (void) {
  char *p, *q;
  int len, i, c;
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (I[vi_source] || I[vi_uploaded] != 1 || I[vi_privacy] || !fits(I[vi_owner_id])) {
    return;
  }

  len = L[vi_title] + L[vi_description] + 1;
  if (len > 4095) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = 0;
    LS->rate2 = I[vi_duration];
    LS->obj_id = (I[vi_id] << 32) + (unsigned) I[vi_owner_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = 0;
    LL->rate2 = I[vi_duration];
    LL->obj_id = (I[vi_id] << 32) + (unsigned) I[vi_owner_id];
    LL->text_len = len;
    q = LL->text;
  }

  p = S[vi_title];
  for (i = L[vi_title]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = '\t';

  p = S[vi_description];
  for (i = L[vi_description]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;

}

/* applications */

enum {
  ap_id, ap_author_id, ap_name, ap_description, ap_secret, ap_secret2,
  ap_enabled, ap_enabled_by_author, ap_enabled_by_us, ap_has_container, ap_has_padding, ap_group_id,
  ap_date, ap_server, ap_source, ap_width, ap_height,
  ap_thumb_source, ap_thumb_server,
  ap_size, ap_type, ap_language_id, ap_edit_date, ap_ip, ap_front, ap_port, ap_ua_hash,
  ap_balance, ap_needs_coins, ap_page_id, ap_needs_ads,
  ap_server_icon, ap_source_icon, ap_layout, ap_domain, ap_rate, ap_uses_api, ap_deposit,
  ap_END
};

void process_applications_row (void) {
  char *p, *q;
  int len, i, c, tag_len;
  static char tag[64];
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (!I[ap_server] || !fits(I[ap_id])) {
    return;
  }

  i = I[ap_type];
  if (i > 0) {
    tag_len = make_tag (tag, 'T', i);
  } else {
    tag_len = 0;
  }

  i = I[ap_enabled];
  if (i == 2 || i == -2 || i == 3 || i == -3) {
    tag_len += make_tag (tag + tag_len, 'B', 1);
  }

  if (I[ap_language_id] >= 0) {
    tag_len += make_tag (tag + tag_len, 'L', I[ap_language_id]);
  }

  tag_len += make_tag (tag + tag_len, 'A', 0) + 1;

  len = tag_len + L[ap_name] + L[ap_description] + 1;

  if (len > 4095) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = I[ap_rate];
    LS->rate2 = I[ap_size];
    LS->obj_id = I[ap_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = I[ap_rate];
    LL->rate2 = I[ap_size];
    LL->obj_id = I[ap_id];
    LL->text_len = len;
    q = LL->text;
  }

  if (tag_len) {
    memcpy (q, tag, tag_len);
    q += tag_len;
    q[-1] = ' ';
  }

  p = S[ap_name];
  for (i = L[ap_name]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = '\t';

  p = S[ap_description];
  for (i = L[ap_description]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}

/* groups */

enum {
  gr_id, gr_name, gr_description, gr_group_type,
  gr_photo, gr_recent_news, gr_email, gr_website,
  gr_country, gr_country_id, gr_city, gr_city_id,
  gr_enable_wall, gr_enable_photos, gr_enable_files,
  gr_enable_timetable, gr_access, gr_updated,
  gr_num, gr_main_admin_id, gr_net, gr_photo_ext,
  gr_applied_num, gr_album_id,
  gr_is_event, gr_start_date, gr_finish_date,
  gr_group_id, gr_address, gr_phone,
  gr_can_post_topics, gr_enable_board,
  gr_server, gr_server2, gr_voting_id,
  gr_subtype, gr_type,
  gr_enable_video, gr_enable_graffiti, gr_enable_flash,
  gr_can_post_albums, gr_can_post_video,
  gr_blocked, gr_trusted, gr_domain,
  gr_district_id, gr_station_id, gr_street_id,
  gr_house_id, gr_place_id,
  gr_date, gr_ip, gr_port, gr_front, gr_ua_hash,
  gr_last_admin_id, gr_last_date, gr_last_edit_ip, gr_last_port,
  gr_last_front, gr_last_ua_hash,
  gr_END
};

void process_groups_row (void) {
  char *p, *q;
  int len, i, c, tags_len;
  static char tags[256];
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (I[gr_id] > 0) {
    store_map (I[gr_id], I[gr_access]);
  }

  if (!I[gr_id] || !fits(I[gr_id]) || I[gr_is_event]) {
    return;
  }

  p = tags;

  if (I[gr_type] > 0) {
    p += make_tag (p, 'T', I[gr_type]);
  }
  if (I[gr_subtype] > 0) {
    p += make_tag (p, 'S', I[gr_subtype]);
  }
  if (I[gr_country_id] > 0) {
    p += make_tag (p, 'C', I[gr_country_id]);
  }
  if (I[gr_city_id] > 0) {
    p += make_tag (p, 'c', I[gr_city_id]);
  }

  tags_len = p - tags;
  if (tags_len > 0) {
    *p = ' ';
    tags_len++;
  }

  len = tags_len + L[gr_name] + L[gr_description] + 1;

  if (len > 8192) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = 0;
    LS->rate2 = I[gr_num];
    LS->obj_id = I[gr_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = 0;
    LL->rate2 = I[gr_num];
    LL->obj_id = I[gr_id];
    LL->text_len = len;
    q = LL->text;
  }

  if (tags_len) {
    memcpy (q, tags, tags_len);
    q += tags_len;
  }

  p = S[gr_name];
  for (i = L[gr_name]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = '\t';

  p = S[gr_description];
  for (i = L[gr_description]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}

/* events */

void process_events_row (void) {
  char *p, *q;
  int len, i, c, tags_len;
  static char tags[256];
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (!I[gr_id] || !fits(I[gr_id]) || I[gr_is_event] != 1) {
    return;
  }

  p = tags;

  if (I[gr_group_type] >= 0) {
    p += make_tag (p, 'T', I[gr_group_type]);
  }
  if (I[gr_country_id] > 0) {
    p += make_tag (p, 'C', I[gr_country_id]);
  }
  if (I[gr_city_id] > 0) {
    p += make_tag (p, 'c', I[gr_city_id]);
  }

  tags_len = p - tags;
  if (tags_len > 0) {
    *p = ' ';
    tags_len++;
  }

  len = tags_len + L[gr_name] + L[gr_description] + 1;

  if (len > 8192) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = I[gr_start_date];
    LS->rate2 = I[gr_num];
    LS->obj_id = I[gr_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = I[gr_start_date];
    LL->rate2 = I[gr_num];
    LL->obj_id = I[gr_id];
    LL->text_len = len;
    q = LL->text;
  }

  if (tags_len) {
    memcpy (q, tags, tags_len);
    q += tags_len;
  }

  p = S[gr_name];
  for (i = L[gr_name]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = '\t';

  p = S[gr_description];
  for (i = L[gr_description]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}

/* blog_posts */

enum {
  bp_id, bp_user_id, bp_date, bp_ncom, bp_read_ncom,
  bp_view_privacy, bp_comment_privacy,
  bp_ip, bp_port, bp_front, bp_ua_hash,
  bp_title, bp_post, bp_tags,
  bp_END
};

void process_blog_posts_row (void) {
  char *p, *q;
  int len, i, c;
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (!I[bp_id] || !fits(I[bp_user_id]) || I[bp_view_privacy]) {
    return;
  }

  len = L[bp_title] + L[bp_post] + 1;

  if (len > 49152) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = I[bp_date];
    LS->rate2 = I[bp_ncom];
    LS->obj_id = ((long long) I[bp_id] << 32) + (unsigned) I[bp_user_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = I[bp_date];
    LL->rate2 = I[bp_ncom];
    LL->obj_id = ((long long) I[bp_id] << 32) + (unsigned) I[bp_user_id];
    LL->text_len = len;
    q = LL->text;
  }

  p = S[bp_title];
  for (i = L[bp_title]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = '\t';

  p = S[bp_post];
  for (i = L[bp_post]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}

/* members_lite */

enum {
  ml_id, ml_rate, ml_cute, ml_bday_day, ml_bday_month, ml_bday_year,
  ml_sex, ml_status, ml_political, ml_uni_country, ml_uni_city,
  ml_university, ml_faculty, ml_chair_id, ml_graduation,
  ml_edu_form, ml_edu_status, ml_logged, ml_ip, ml_front, ml_port,
  ml_user_agent, ml_profile_privacy,
  ml_first_name, ml_last_name, ml_server, ml_photo,
  ml_END
};

void process_memlite_row (void) {
  char *p, *q;
  int len, i, c;
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (!I[ml_id] || !fits(I[ml_id]) || I[ml_profile_privacy] == 4) {
    return;
  }

  len = L[ml_first_name] + L[ml_last_name] + 1;

  if (len > 1024) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = I[ml_rate];
    LS->rate2 = I[ml_cute];
    LS->obj_id = I[ml_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = I[ml_rate];
    LL->rate2 = I[ml_cute];
    LL->obj_id = I[ml_id];
    LL->text_len = len;
    q = LL->text;
  }

  p = S[ml_first_name];
  for (i = L[ml_first_name]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = '\t';

  p = S[ml_last_name];
  for (i = L[ml_last_name]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}


/* market_items */

enum {
  mi_id, mi_price, mi_price_real, mi_currency,
  mi_privacy, mi_created_at, mi_closed_at,
  mi_user_id, mi_category, mi_country, mi_city,
  mi_thumb_id, mi_views, mi_votes, mi_section,
  mi_ip, mi_port, mi_front, mi_cost, mi_checked,
  mi_ua_hash, mi_name, mi_description,
  mi_END
};

void process_market_row (void) {
  char *p, *q;
  int len, i, c, tags_len;
  static char tags[256];
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (!I[mi_id] || !fits(I[mi_id]) || I[mi_privacy]) {
    return;
  }

  p = tags;

  if (I[mi_category] > 0) {
    p += make_tag (p, 'T', I[mi_category]);
  }
  if (I[mi_section] > 0) {
    p += make_tag (p, 'S', I[mi_section]);
  }
  if (I[mi_country] > 0) {
    p += make_tag (p, 'C', I[mi_country]);
  }
  if (I[mi_city] > 0) {
    p += make_tag (p, 'c', I[mi_city]);
  }

  tags_len = p - tags;
  if (tags_len > 0) {
    *p = ' ';
    tags_len++;
  }

  len = tags_len + L[mi_name] + L[mi_description] + 1;

  if (len > 8192) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = (I[mi_cost] <= I[mi_votes] ? I[mi_cost] : 0);
    LS->rate2 = (I[mi_price] > 0 ? I[mi_price] : 1000000000);
    LS->obj_id = I[mi_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = (I[mi_cost] <= I[mi_votes] ? I[mi_cost] : 0);
    LL->rate2 = (I[mi_price] > 0 ? I[mi_price] : 1000000000);
    LL->obj_id = I[mi_id];
    LL->text_len = len;
    q = LL->text;
  }

  if (tags_len) {
    memcpy (q, tags, tags_len);
    q += tags_len;
  }

  p = S[mi_name];
  for (i = L[mi_name]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = '\t';

  p = S[mi_description];
  for (i = L[mi_description]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}

/* questions */

enum {
  qu_id, qu_owner_id, qu_type, qu_visibility,
  qu_date, qu_message, qu_updated, qu_num,
  qu_last_post_id, qu_last_poster_id, qu_new_num,
  qu_END
};

void process_questions_row (void) {
  char *p, *q;
  int len, i, c, tags_len;
  static char tags[256];
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (!I[qu_id] || !fits(I[qu_owner_id])) {
    return;
  }

  p = tags;

  if (I[qu_type] > 0) {
    p += make_tag (p, 'T', I[qu_type]);
  }

  tags_len = p - tags;
  if (tags_len > 0) {
    *p = ' ';
    tags_len++;
  }

  len = tags_len + L[qu_message];

  if (len > 8192) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = I[qu_date];
    LS->rate2 = I[qu_num];
    LS->obj_id = ((long long) I[qu_id] << 32) + (unsigned) I[qu_owner_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = I[qu_date];
    LL->rate2 = I[qu_num];
    LL->obj_id = ((long long) I[qu_id] << 32) + (unsigned) I[qu_owner_id];
    LL->text_len = len;
    q = LL->text;
  }

  if (tags_len) {
    memcpy (q, tags, tags_len);
    q += tags_len;
  }

  p = S[qu_message];
  for (i = L[qu_message]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}

/* topics */

enum {
  to_id, to_group_id, to_posts, to_updated, to_started,
  to_starter_id, to_last_poster_id, to_ordering,
  to_flags, to_ip, to_front, to_port, to_user_agent,
  to_title, to_last_poster_name,
  to_END
};

void process_topics_row (void) {
  char *p, *q;
  int len, i, c, tags_len;
  static char tags[256];
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (!I[to_id] || I[to_group_id] <= 0 || !fits(I[to_group_id])) {
    return;
  }

  i = get_map (I[to_group_id]);
  if (i > 0) {
    return;
  }

  p = tags;

  tags_len = p - tags;
  if (tags_len > 0) {
    *p = ' ';
    tags_len++;
  }

  len = tags_len + L[to_title];

  if (len > 8192) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = I[to_started];
    LS->rate2 = I[to_posts];
    LS->obj_id = ((long long) I[to_id] << 32) + (unsigned) -I[to_group_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = I[to_started];
    LL->rate2 = I[to_posts];
    LL->obj_id = ((long long) I[to_id] << 32) + (unsigned) -I[to_group_id];
    LL->text_len = len;
    q = LL->text;
  }

  if (tags_len) {
    memcpy (q, tags, tags_len);
    q += tags_len;
  }

  p = S[to_title];
  for (i = L[to_title]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}

/* minifeed */

enum {
  mf_id, mf_user_id, mf_created, mf_text,
  mf_END
};

void process_minifeed_row (void) {
  char *p, *q;
  int len, i, c;
  struct lev_search_text_short_entry *LS;
  struct lev_search_text_long_entry *LL;

  if (!I[mf_id] || !fits(I[mf_user_id])) {
    return;
  }

  len = L[mf_text];

  if (len > 8192) {
    return;
  }

  if (len < 256) {
    LS = write_alloc (21+len);
    LS->type = LEV_SEARCH_TEXT_SHORT + len;
    LS->rate = I[mf_created];
    LS->rate2 = 0;
    LS->obj_id = ((long long) I[mf_id] << 32) + (unsigned) I[mf_user_id];
    q = LS->text;
  } else {
    LL = write_alloc (23+len);
    LL->type = LEV_SEARCH_TEXT_LONG;
    LL->rate = I[mf_created];
    LL->rate2 = 0;
    LL->obj_id = ((long long) I[mf_id] << 32) + (unsigned) I[mf_user_id];
    LL->text_len = len;
    q = LL->text;
  }

  p = S[mf_text];
  for (i = L[mf_text]; i > 0; i--) {
    c = (unsigned char) *p++;
    if (c < ' ') { c = ' '; }
    *q++ = c;
  }

  *q++ = 0;

  adj_rec++;
}





/*
 * END MAIN
 */

void output_stats (void) {
  fprintf (stderr,
	   "read: %lld bytes, %d records read, %d processed\n"
	   "written: %lld bytes\n",
	   rd_bytes, line_no, adj_rec, wr_bytes);
}


void usage (void) {
  fprintf (stderr, "usage:\t%s [-v] [-u<username>] [-m<rem>,<mod>] [-g<filename>] [-f<format>] [-o<output-class>] <input-file> [<output-file>]\n"
	   "\tConverts tab-separated table dump into KDB binlog. "
	   "If <output-file> is specified, resulting binlog is appended to it.\n"
	   "\t-h\tthis help screen\n"
	   "\t-v\tverbose mode on\n"
	   "\t-u<username>\tassume identity of given user\n"
	   "\t-m<rem>,<mod>\tslice parameters: consider only users with uid %% mod == rem\n"
	   "\t-o<int>\tdetermines output format\n"
	   "\t-g<map-file>\tsets map file\n"
	   "\t-f<format>\tdetermines dump format, one of audio, video, groups, ...\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hvu:m:f:g:o:")) != -1) {
    switch (i) {
    case 'v':
      verbosity = 1;
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
    case 'g':
      groups_fname = optarg;
      break;
    case 'o':
      output_format = atol (optarg);
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
  case TF_AUDIO:
    Args_per_line = au_END;
    start_binlog(SEARCH_SCHEMA_V1, "audio_search");
    while (read_record() > 0) {
      process_audio_row();
    }
    break;
  case TF_VIDEO:
    Args_per_line = vi_END;
    start_binlog(SEARCH_SCHEMA_V1, "video_search");
    while (read_record() > 0) {
      process_video_row();
    }
    break;
  case TF_APPS:
    Args_per_line = ap_END;
    start_binlog(SEARCH_SCHEMA_V1, "apps_search");
    while (read_record() > 0) {
      process_applications_row();
    }
    break;
  case TF_GROUPS:
    Args_per_line = gr_END;
    start_binlog(SEARCH_SCHEMA_V1, "group_search");
    while (read_record() > 0) {
      process_groups_row();
    }
    break;
  case TF_EVENTS:
    Args_per_line = gr_END;
    start_binlog(SEARCH_SCHEMA_V1, "event_search");
    while (read_record() > 0) {
      process_events_row();
    }
    break;
  case TF_BLOG_POSTS:
    Args_per_line = bp_END;
    start_binlog(SEARCH_SCHEMA_V1, "blog_posts_search");
    while (read_record() > 0) {
      process_blog_posts_row();
    }
    break;
  case TF_MEMLITE:
    Args_per_line = ml_END;
    start_binlog(SEARCH_SCHEMA_V1, "member_name_search");
    while (read_record() > 0) {
      process_memlite_row();
    }
    break;
  case TF_MARKET_ITEMS:
    Args_per_line = mi_END;
    start_binlog(SEARCH_SCHEMA_V1, "market_search");
    while (read_record() > 0) {
      process_market_row();
    }
    break;
  case TF_QUESTIONS:
    Args_per_line = qu_END;
    start_binlog(SEARCH_SCHEMA_V1, "question_search");
    while (read_record() > 0) {
      process_questions_row();
    }
    break;
  case TF_TOPICS:
    load_map (1);
    Args_per_line = to_END;
    start_binlog(SEARCH_SCHEMA_V1, "topic_search");
    while (read_record() > 0) {
      process_topics_row();
    }
    break;
  case TF_MINIFEED:
    Args_per_line = mf_END;
    start_binlog(SEARCH_SCHEMA_V1, "status_search");
    while (read_record() > 0) {
      process_minifeed_row();
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

  if (map_size > 0 && map_changes > 0 && groups_fname) {
    map_fd = open (groups_fname, O_WRONLY | O_CREAT | O_TRUNC, 0640);
    if (map_fd < 0) {
      fprintf (stderr, "cannot create map file %s: %m\n", groups_fname);
      exit (1);
    }
    assert (write (map_fd, Map, map_size) == map_size);
    close (map_fd);
    if (verbosity > 0) {
      fprintf (stderr, "%d bytes written to map file %s\n", map_size, groups_fname);
    }
  }

  if (verbosity > 0) {
    output_stats();
  }

  return 0;
}
