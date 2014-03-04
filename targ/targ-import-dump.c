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
              2009-2010 Andrew Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include "server-functions.h"
#include "kdb-targ-binlog.h"

#define	TF_NONE		0
#define	TF_MEMLITE	1
#define	TF_EDUCATION	2
#define	TF_MEMSHORT	3
#define	TF_MEMEXTRA	4
#define	TF_ADDRESSES	5
#define TF_GROUPSHORT	6
#define	TF_MEMGROUPS	7
#define	TF_MILITARY	8
#define	TF_NAMES	9
#define	TF_MATCHES	10

#define	READ_BUFFER_SIZE	(1L << 24)
#define	WRITE_BUFFER_SIZE	(1L << 24)
#define	READ_THRESHOLD		(1L << 17)
#define	WRITE_THRESHOLD		(1L << 16)

char *progname = "targ-import-dump", *username, *src_fname, *targ_fname;
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
  if (!strncmp (str, "memlite", 7) || !strncmp (str, "members_lite", 12)) {
    return TF_MEMLITE;
  }
  if (!strncmp (str, "education", 9)) {
    return TF_EDUCATION;
  }
  if (!strncmp (str, "memshort", 8) || !strncmp (str, "members_short", 13)) {
    return TF_MEMSHORT;
  }
  if (!strncmp (str, "memextra", 8) || !strncmp (str, "members_extra", 13)) {
    return TF_MEMEXTRA;
  }
  if (!strncmp (str, "addresses", 9)) {
    return TF_ADDRESSES;
  }
  if (!strncmp (str, "groupshort", 10) || !strncmp (str, "groups_short", 12)) {
    return TF_GROUPSHORT;
  }
  if (!strncmp (str, "memgroups", 9) || !strncmp (str, "member_groups", 13)) {
    return TF_MEMGROUPS;
  }
  if (!strncmp (str, "military", 8)) {
    return TF_MILITARY;
  }
  if (!strncmp (str, "names", 5)) {
    return TF_NAMES;
  }
  if (!strncmp (str, "matches", 7)) {
    return TF_MATCHES;
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

int user_id;

int fits (int id) {
  if (!split_mod) {
    return 1;
  }
  return id % split_mod == split_rem;
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
  

/* memlite */

enum {
  ml_id, ml_rate, ml_cute, ml_bday_day, ml_bday_month, ml_bday_year,
  ml_sex, ml_status, ml_political, ml_uni_country, ml_uni_city,
  ml_university, ml_faculty, ml_chair_id, ml_graduation, 
  ml_edu_form, ml_edu_status, ml_logged, ml_ip, ml_front, ml_port,
  ml_user_agent, ml_profile_privacy, 
  ml_first_name, ml_last_name, ml_server, ml_photo
};

void process_memlite_row (void) {
  struct lev_birthday *LB;

  if (!fits(I[ml_id])) {
    return;
  }

  user_id = I[ml_id];

  if (I[ml_bday_day] || I[ml_bday_month] || I[ml_bday_year]) {
    LB = write_alloc (sizeof (struct lev_birthday));
    LB->type = LEV_TARG_BIRTHDAY;
    LB->user_id = I[ml_id];
    LB->day = I[ml_bday_day];
    LB->month = I[ml_bday_month];
    LB->year = I[ml_bday_year];
  }

  if (I[ml_uni_country] || I[ml_uni_city]) {
    log_2ints (LEV_TARG_UNIVCITY, I[ml_uni_country], I[ml_uni_city]);
  }

  if (I[ml_ip] || I[ml_logged]) {
    log_2ints (LEV_TARG_ONLINE, I[ml_ip], I[ml_logged]);
  }

  if (I[ml_sex]) {
    log_1int (LEV_TARG_SEX, I[ml_sex]);
  }

  if (I[ml_political]) {
    log_1int (LEV_TARG_POLITICAL, I[ml_political]);
  }

  if (I[ml_rate]) {
    if (I[ml_cute]) {
      log_2ints (LEV_TARG_RATECUTE, I[ml_rate], I[ml_cute]);
    } else {
      log_1int (LEV_TARG_RATE, I[ml_rate]);
    }
  } else if (I[ml_cute]) {
    log_1int (LEV_TARG_CUTE, I[ml_cute]);
  }

  if (I[ml_profile_privacy] != 1) {
    log_1int (LEV_TARG_PRIVACY, I[ml_profile_privacy]);
  }

  if (I[ml_status]) {
    log_1int (LEV_TARG_MSTATUS, I[ml_status]);
  }

  if (L[ml_photo] > 4) {
    log_0ints (LEV_TARG_USERFLAGS + 1);
  }

  adj_rec++;

}

/* education */

enum {
  ed_id, ed_university, ed_faculty, ed_graduation,
  ed_city, ed_form, ed_status, ed_chair, ed_country,
  ed_user_id, ed_prime
};

void process_education_row (void) {
  struct lev_education *E;
  user_id = I[ed_user_id];

  if (!fits(user_id)) {
    return;
  }

//  log_0ints (LEV_TARG_EDUCLEAR);

  E = write_alloc (sizeof (*E));
  E->type = I[ed_prime] ? LEV_TARG_EDUADD_PRIM : LEV_TARG_EDUADD;
  E->user_id = I[ed_user_id];
  E->grad_year = I[ed_graduation];
  E->chair = I[ed_chair];
  E->faculty = I[ed_faculty];
  E->university = I[ed_university];
  E->city = I[ed_city];
  E->country = I[ed_country];
  E->edu_form = I[ed_form];
  E->edu_status = I[ed_status];
  E->reserved = 0;

  adj_rec++;

}

/* members_short */

enum {
  ms_id,
  ms_first_name,
  ms_last_name,
  ms_maiden_name,
  ms_religion,
  ms_school,
  ms_work,
  ms_hometown,
  ms_END
};

static char *Pp;

static inline int pa_skip (void) {
  if ((Pp[0] == '^' || !Pp[0]) && Pp[1] == '*' && Pp[2] == '^') {
    Pp += 3;
    return 1;
  } else {
    return 0;
  }
}

static inline int pa_int (void) {
  char *ptr;
  int res = strtol (Pp, &ptr, 10);
  Pp = ptr;
  return res;
}

static inline void pa_str (void) {
  while (*Pp && (Pp[0] != '^' || Pp[1] != '*' || Pp[2] != '^') 
  	     && (Pp[0] != '%' || Pp[1] != '*' || Pp[2] != '%')) {
    if ((unsigned char) *Pp < ' ') { *Pp = ' '; }
    Pp++;
  }
  *Pp = 0;
}
  

void process_memshort_row (void) {
  struct lev_religion *ER;
  static struct lev_school TS;
  static struct lev_company TC;
  struct lev_school *ES;
  struct lev_company *EC;
  char *str, *str2, *p, *q;
  int len, len2, i;
  user_id = I[ms_id];
  if (!fits (user_id)) {
    return;
  }

  assert (L[ms_first_name] <= 128 && L[ms_last_name] <= 128);

  p = S[ms_first_name];
  q = 0;
  for (i = L[ms_first_name]; i > 0; i--, p++) {
    if (*p == '\t' && !q) {
      q = p;
    } else if ((unsigned char) *p < ' ' || *p == '<' || *p == '>') {
      *p = ' ';
    }
  }
  if (!q) {
    for (p = S[ms_first_name]; *p; p++) {
      if (*p == ' ') {
	q = p;
	*p = '\t';
	break;
      }
    }
    if (!q) {
      q = p;
      p[0] = '\t';
      p[1] = 0;
      L[ms_first_name]++;
    }
  }
  
  p = S[ms_last_name];
  for (i = L[ms_last_name]; i > 0; i--, p++) {
    if ((unsigned char) *p < ' ' || *p == '<' || *p == '>') {
      *p = ' ';
    }
  }

  p = S[ms_maiden_name];
  for (i = L[ms_maiden_name]; i > 0; i--, p++) {
    if ((unsigned char) *p < ' ' || *p == '<' || *p == '>') {
      *p = ' ';
    }
  }

  //  fprintf (stderr, "%d [%s][%s]\n", user_id, S[ms_first_name], S[ms_last_name]);
  len = L[ms_first_name] + 1 + L[ms_last_name] + 1 + L[ms_maiden_name];
  if (len >= 256) {
    L[ms_maiden_name] = 0;
    len = L[ms_first_name] + 1 + L[ms_last_name] + 1;
    if (len >= 256) {
      L[ms_last_name] = 253 - L[ms_first_name];
      len = 255;
    }
  }

  struct lev_username *LU = write_alloc (len + 9);

  LU->type = LEV_TARG_USERNAME + len;
  LU->user_id = I[ms_id];
  memcpy (LU->s, S[ms_first_name], L[ms_first_name]);
  LU->s[L[ms_first_name]] = 9;
  memcpy (LU->s + L[ms_first_name] + 1, S[ms_last_name], L[ms_last_name]);
  LU->s[L[ms_first_name] + 1 + L[ms_last_name]] = 9;
  memcpy (LU->s + L[ms_first_name] + 1 + L[ms_last_name] + 1, S[ms_maiden_name], L[ms_maiden_name]);
  LU->s[len] = 0;

  
  if (L[ms_religion] && L[ms_religion] <= 120) {
    len = L[ms_religion];
    ER = (struct lev_religion *) write_alloc (9+len);
    ER->type = LEV_TARG_RELIGION + len;
    ER->user_id = user_id;
    for (str = S[ms_religion]; *str; str++) {
      if ((unsigned char) *str < ' ') {
        *str = ' ';
      }
    }
    strcpy (ER->str, S[ms_religion]);
  }

  if (L[ms_hometown] && L[ms_hometown] <= 250) {
    len = L[ms_hometown];
    struct lev_hometown *EH = (struct lev_hometown *) write_alloc (9+len);
    EH->type = LEV_TARG_HOMETOWN + len;
    EH->user_id = user_id;
    for (str = S[ms_hometown]; *str; str++) {
      if ((unsigned char) *str < ' ') {
        *str = ' ';
      }
    }
    strcpy (EH->text, S[ms_hometown]);
  }

  log_0ints (LEV_TARG_SCH_CLEAR);
  Pp = S[ms_school];
  Pp[L[ms_school]+1] = 0;
  do {
#define SKIP	if (!pa_skip()) { break; }
    TS.user_id = user_id;
    TS.country = pa_int();
    SKIP
    TS.city = pa_int();
    SKIP
    pa_str();
    SKIP
    TS.school = pa_int();
    SKIP
    pa_str();
    SKIP
    TS.grad = pa_int();
    SKIP
    TS.start = pa_int();
    SKIP
    TS.finish = pa_int();
    SKIP
    TS.sch_class = pa_int();
    SKIP
    str = Pp;
    pa_str();
    SKIP
    TS.sch_type = pa_int();
    len = strlen (str);
    if (len <= 250) {
      TS.type = LEV_TARG_SCH_ADD + len;
      ES = write_alloc (len+26);
      memcpy (ES, &TS, 25);
      strcpy (ES->spec, str);
    }
    if (Pp[1] != '*' || Pp[2] != '%') { break; }
    Pp += 3;
  } while(1);
  if (Pp[0] || Pp[1]) {
    if (!Pp[0]) { Pp++; }
    if (verbosity > 1) {
      fprintf (stderr, "user=%d: remaining schools: '%s'\n", user_id, Pp);
    }
  }

  log_0ints (LEV_TARG_COMP_CLEAR);
  Pp = S[ms_work];
  Pp[L[ms_work]+1] = 0;
  do {
    TC.user_id = user_id;
    TC.country = pa_int();
    SKIP
    if (Pp[0] == 'n' && Pp[1] == 'i' && Pp[2] == 'l') {
      Pp += 3;
      TC.city = 0;
    } else {
      TC.city = pa_int();
    }
    SKIP
    pa_str();
    SKIP
    TC.company = pa_int();
    SKIP
    str = Pp;
    pa_str();
    SKIP
    TC.start = pa_int();
    SKIP
    TC.finish = pa_int();
    SKIP
    str2 = Pp;
    pa_str();
    len = strlen(str);
    len2 = strlen(str2);
    if (len <= 250 && len2 <= 250) {
      TC.type = LEV_TARG_COMP_ADD + 1 + len + len2;
      EC = write_alloc (len + len2 + 23);
      memcpy (EC, &TC, 21);
      strcpy (EC->text, str);
      EC->text[len] = 9;
      strcpy (EC->text + len + 1, str2);
    }
    if (Pp[1] != '*' || Pp[2] != '%') { break; }
    Pp += 3;
  } while(1);
  if (Pp[0] || Pp[1]) {
    if (!Pp[0]) { Pp++; }
    fprintf (stderr, "user=%d: remaining companies: '%s'\n", user_id, Pp);
  }
#undef SKIP
  adj_rec++;
}

/* members_extra */

enum {
 me_id,
 me_lang,
 me_text
};

#define MAX_TERMS	(1L << 26)
#define MAX_CHARS	(1L << 30)

int Tc;
char *TL[MAX_TERMS];
char *TS, *Tw;

struct cword {
  long num;
  char c;
  char s[1];
};

static void asort (int a, int b) {
  int i, j;
  char *h, *t;
  if (a >= b) { return; }
  i = a;  j = b;  h = TL[(a+b)>>1];
  do {
    while (strcmp (TL[i], h) < 0) { i++; }
    while (strcmp (TL[j], h) > 0) { j--; }
    if (i <= j) {
      t = TL[i];  TL[i++] = TL[j];  TL[j--] = t;
    }
  } while (i <= j);
  asort (a, j);
  asort (i, b);
}

static void flush_words (void) {
  int i, j, len;
  struct cword *W;
  if (!Tc) { return; }
  if (verbosity > 0) {
    fprintf (stderr, "sorting words: %d words, %ld chars\n", Tc, (long)(Tw - TS));
  }
  asort (0, Tc-1);
  i = 0;
  while (i < Tc) {
    j = i+1;
    len = strlen (TL[i]);
    while (j < Tc && !strcmp (TL[i], TL[j])) { j++; }
    W = write_alloc (6 + len);
    W->num = j - i;
    W->c = len;
    memcpy (W->s, TL[i], len+1);
    i = j;
  }
  W = write_alloc (4);
  W->num = -1;
  Tc = 0;
  Tw = TS;
}


static void keep_word (char *str, int len) {
  if (len < 2 || len >= 64) { return; }
  memcpy (Tw, str, len+1);
  TL[Tc++] = Tw;
  Tw += (len + 4) & -4;
  if (Tw >= TS + MAX_CHARS - 512 || Tc >= MAX_TERMS) {
    flush_words ();
  }
}

void init_allowed (void) {
  int i;
  memset (allowed, 0, 256);
  for (i = '0'; i <= '9'; i++) { allowed[i] = 4; }
  for (i = 'A'; i <= 'Z'; i++) { allowed[i] = 2; }
  for (i = 'a'; i <= 'z'; i++) { allowed[i] = 1; }
  allowed['-'] = 8;
  allowed[','] = 8;
}

void process_memextra_row (void) {
  int uid = I[me_id] / 1000;
  int type = (I[me_id] % 1000) / 10;
  char *p, *q, *r;
  int c;
  user_id = uid;

  if (!fits(uid)) {
    return;
  }
  if (type <= 0 || type > 7) {
    return;
  }
//  fprintf (stderr, "orig: '%s'\n", S[me_text]);

  if (output_format != 1) {
    struct lev_interests *D = write_alloc (11+L[me_text]);
    D->type = LEV_TARG_INTERESTS + type;
    D->user_id = user_id;
    D->len = L[me_text];
    strcpy (D->text, S[me_text]);

    adj_rec++;
    return;
  }

  p = q = S[me_text];
  while (*p) {
    c = (unsigned char) *p;
    if (c == '&') {
      if (p[1] == '#') {
        c = strtol(p+2, &r, 10);
        if (!*r) { break; }
        if (r > p+2 && *r == ';') {
          if (c < 128 || c == 10006 || c == 8236) {
            p = r + 1;
            *q++ = ' ';
            continue;
          }
          while (p <= r) { 
            *q++ = *p++;
          }
          continue;
        }
        p += 2;
        continue;
      }
      if (p[1] >= 'a' && p[1] <= 'z') {
        r = p + 1;
        while (*r >= 'a' && *r <= 'z') { r++; }
        if (!*r) {
          break;
        }
        if (*r == ';') {
          p = r + 1;
          *q++ = ' ';
          continue;
        }
      }
      *q++ = ' ';
      p++;
      continue;
    }
    if (c == '<') {
      if (p[1] == 'b' && p[2] == 'r' && p[3] == '>') {
        *q++ = ' ';
        p += 4;
        continue;
      }
      if (p[1] == '/' && p[2] == 'b' && p[3] == 'r' && p[4] == '>') {
        *q++ = ' ';
        p += 5;
        continue;
      }
    }
    if (c < 128 && !allowed[c]) { c = ' '; }
    if (c == 0xa8) { c = 0xc5; }
    if (c == 0xb8) { c = 0xe5; }
//    if (c == '>' || c < ' ') { c = ' '; }
    *q++ = c;
    p++;
  }
  q[0] = 0;

  c = 0;
  for (p = q = S[me_text]; *p; p++) {
    if (*p != c) { 
      *q++ = c = *p;
      if (c != ' ') { c = 0; }
    }
  }

  q[0] = q[1] = 0;
        
  p = S[me_text];

//  fprintf (stderr, "text: '%s'\n", p);

  do {
    while ((unsigned char) *p <= ' ' && *p) { p++; }
    if (!*p) { break; }
    if (*p == ',') { p++; continue; }
    q = p;
    while (*p && *p != ',') { p++; }
    while (p > q && p[-1] == ' ') { p--; }
    *p = 0;
    if (output_format == 1) {
      keep_word (q, p-q);
    }
  } while (1);

  adj_rec++;

}

/* group_short */

enum {
  gs_id, gs_type, gs_subtype
};

#define	MAX_GROUPS	(1 << 30)
#define	MAX_GID		(1 << 24)
char *groups_fname, *groups_fname2;
char GT[MAX_GROUPS], *GA, *GB, *GC;
int Gc, Gd;

void process_groupshort_row (void) {
  int x = I[gs_id], y = I[gs_subtype];
  if (x <= 0 || x >= MAX_GROUPS || y <= 0 || y >= 128) {
    return;
  }
  GT[x] = y;
  if (x >= Gc) { Gc = x+1; }
  adj_rec++;
}

/* member_groups */

enum {
  mg_id,
  mg_user_id,
  mg_group_id,
  mg_confirmed,
  mg_who_invited
};

void process_memgroups_row (void) {
  struct lev_user_generic *E;
  int x = I[mg_group_id];
  user_id = I[mg_user_id];

  if (x <= 0 || x >= MAX_GID || !fits(user_id) || I[mg_confirmed] != 2) {
    return;
  }

  if (x < Gc && GT[x] > 0) {
    E = write_alloc (8);
    E->type = LEV_TARG_GRTYPE_ADD + GT[x];
    E->user_id = user_id;
    adj_rec++;
  }

  if (!GA) {
    GC = GB = GA = GT;
  }
  
  if (GC <= GT + MAX_GROUPS - 8) {
    assert (GC);
    ((int *) GC)[0] = x;
    ((int *) GC)[1] = user_id;
    GC += 8;
  }
}

void mg_sort (long long *X, int b) {
  if (b <= 0) { return; }
  int i = 0, j = b;
  long long h = X[b >> 1], t;
  do {
    while (X[i] < h) { i++; }
    while (X[j] > h) { j--; }
    if (i <= j) {
      t = X[i];  X[i++] = X[j];  X[j--] = t;
    }
  } while (i <= j);
  mg_sort (X, j);
  mg_sort (X + i, b - i);
}

void flush_memgroups (void) {
  int num = ((GC - GB) >> 3);
  assert (num >= 0);
  if (!num) {
    return;
  }
  mg_sort ((long long *) GB, num-1);

  int *p = (int *) GB;
  while ((char *) p < GC) {
    int user_id = p[1], k = 0, z = 0, *q = p, *r;
    while ((char *) p < GC && p[1] == user_id) { 
      if (p[0] != z) {
      	assert (p[0] > z);
        z = p[0];
        k++;
      }
      p += 2;  
    }
    if (k < 256) {
      struct lev_groups *E = write_alloc (8 + k*4);
      E->type = LEV_TARG_GROUP_ADD + k;
      E->user_id = user_id;
      r = E->groups;
    } else {
      if (k >= 4096) { k = 4095; }
      struct lev_groups_ext *E = write_alloc (12 + k*4);
      E->type = LEV_TARG_GROUP_EXT_ADD;
      E->user_id = user_id;
      E->gr_num = k;
      r = E->groups;
    }
    z = 0;
    while (k > 0) {
      if (q[0] > z) {
        *r++ = z = q[0];
        k--;
      }
      q += 2;
    }
    assert (q <= p);
  }
}
    

/* addresses */

enum {
  adr_id, adr_type, adr_member_id, 
  adr_country_id, adr_city_id, adr_district_id, adr_station_id, adr_street_id,
  adr_house_id, adr_house, adr_place_id, 
  adr_zipcode, adr_commentary, adr_since, adr_until
};

char *lookup_name (char *D, int DL, int id, int *len, int max_len) {
  *len = 0;
  if (!D || DL < MAX_GID * 4 || id <= 0 || id >= MAX_GID) {
    return 0;
  }
  int x = ((int *) D)[id];
  if (x < MAX_GID * 4 || x + 4 > DL) {
    return 0;
  }
  int l = *((int *) (D + x));
  if ((unsigned) l > (unsigned) max_len || x + l + 5 > DL) {
    return 0;
  }
  *len = l;
  return D + x + 4;
}

void process_addresses_row (void) {
  struct lev_address_extended *A;
  char *house, *name;
  int house_len, name_len;
  int len;

  user_id = I[adr_member_id];

  if (!fits(user_id)) {
    return;
  }

  house = lookup_name (GA, Gc, I[adr_house_id], &house_len, 64);
  name = lookup_name (GB, Gd, I[adr_place_id], &name_len, 128);

  len = house_len + 1 + name_len;

  A = write_alloc (len + 27);
  A->type = LEV_TARG_ADDR_EXT_ADD + len;
  A->user_id = user_id;
  A->city = I[adr_city_id];
  A->district = I[adr_district_id];
  A->station = I[adr_station_id];
  A->street = I[adr_street_id];
  A->country = I[adr_country_id];
  A->atype = I[adr_type];

  if (house_len) {
    memcpy (A->text, house, house_len);
  }
  A->text[house_len] = 9;
  if (name_len) {
    memcpy (A->text + house_len + 1, name, name_len);
  }
  A->text[len] = 0;

  adj_rec++;

}

/* military */

enum {
  mi_id,
  mi_member_id,
  mi_country_id,
  mi_started,
  mi_finished,
  mi_unit_id
};

void process_military_row (void) {
  struct lev_military *E;
  user_id = I[mi_member_id];

  if (!fits(user_id)) {
    return;
  }

  E = write_alloc (sizeof (*E));
  E->type = LEV_TARG_MIL_ADD;
  E->user_id = I[mi_member_id];
  E->unit_id = I[mi_unit_id];
  E->start = I[mi_started];
  E->finish = I[mi_finished];

  adj_rec++;

}

/* names */

void process_names_row (void) {
  int x = I[0], len = L[1];

  if (x <= 0 || x >= MAX_GID) {
    return;
  }

  assert (Gc < MAX_GROUPS - len - 16);

  ((int *) GT)[x] = Gc;
  *((int *) (GT + Gc)) = len;
  Gc += 4;
  memcpy (GT + Gc, S[1], len+1);
  Gc = (Gc + len + 4) & -4;

  assert (Gc <= MAX_GROUPS);

  adj_rec++;

}

/* matches_polls */

enum {
  mp_id,
  mp_city,
  mp_years,
  mp_sex,
  mp_date,
  mp_category,
  mp_question,
  mp_END
};

void process_matches_row (void) {
  struct lev_proposal *E;
  int len = L[mp_question];
  char *str = S[mp_question];
  int i;
  user_id = I[mp_id];

  if (!fits(user_id) || I[mp_category] || !len || len >= 1024) {
    return;
  }

  E = write_alloc (11 + len);
  E->type = LEV_TARG_PROPOSAL;
  E->user_id = user_id;
  E->len = len;

  for (i = 0; i < len; i++) {
    if ((unsigned char) str[i] < ' ') {
      str[i] = ' ';
    }
  }

  strcpy (E->text, str);
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
	   "\t-g<filename>\tloads auxiliary data from given file\n"
	   "\t-m<rem>,<mod>\tslice parameters: consider only users with uid %% mod == rem\n"
	   "\t-o<int>\tdetermines output format\n"
	   "\t-f<format>\tdetermines dump format, one of members_lite, members, education, ...\n",
	   progname);
}

int main (int argc, char *argv[]) {
  int i;
  progname = argv[0];
  init_allowed();
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
    GC = GB = GA + ((Gc + 3) & -4);
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
    GC = GB + ((Gd + 3) & -4);
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
  case TF_MEMLITE:
    Args_per_line = 27;
    start_binlog(TARG_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_memlite_row();
    }
    break;
  case TF_EDUCATION:
    Args_per_line = 11;
    start_binlog(TARG_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_education_row();
    }
    break;
  case TF_MEMSHORT:
    Args_per_line = ms_END;
    start_binlog(TARG_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_memshort_row();
    }
    break;
  case TF_MEMEXTRA:
    Args_per_line = 3;
    if (output_format != 1) {
      start_binlog(TARG_SCHEMA_V1, "");
    }
    Tw = TS = malloc (MAX_CHARS);
    assert (TS);
    while (read_record() > 0) {
      process_memextra_row();
    }
    if (Tc) { flush_words(); }
    break;
  case TF_ADDRESSES:
    Args_per_line = 15;
    start_binlog(TARG_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_addresses_row();
    }
    break;
  case TF_GROUPSHORT:
    Args_per_line = 3;
    while (read_record() > 0) {
      process_groupshort_row();
    }
    if (output_format == 1 && Gc) {
      assert (write (targ_fd, GT, Gc) == Gc);
    }
    break;
  case TF_MEMGROUPS:
    assert (Gc > 0);
    Args_per_line = 5;
    start_binlog(TARG_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_memgroups_row();
    }
    flush_memgroups ();
    break;
  case TF_MILITARY:
    Args_per_line = 6;
    start_binlog(TARG_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_military_row();
    }
    break;
  case TF_NAMES:
    Args_per_line = 2;
    Gc = MAX_GID * 4;
    while (read_record() > 0) {
      process_names_row();
    }
    if (output_format == 1 && Gc) {
      assert (write (targ_fd, GT, Gc) == Gc);
    }
    break;
  case TF_MATCHES:
    Args_per_line = mp_END;
    start_binlog(TARG_SCHEMA_V1, "");
    while (read_record() > 0) {
      process_matches_row();
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
