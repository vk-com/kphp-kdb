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
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <math.h>
#include <stddef.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "isearch-data.h"
#include "dl.h"
#include "dl-aho.h"
#include "../common/utf8_utils.h"
#include "utf8_utils.h"

#define MAX_BUFF (1 << 24)
char buff[MAX_BUFF];
char buff2[MAX_BUFF];

int index_mode;
long max_memory = MAX_MEMORY;
int header_size;
rating lowest_rate;
int binlog_readed;
int find_bad_requests;

int jump_log_ts;
long long jump_log_pos;
unsigned int jump_log_crc32;

int use_stemmer;
int ratingT;
int bestn;
int best[MAX_BEST];

index_header header;

#define ALPH_N (26 + 32)
char *common_spelling_errors[ALPH_N] = {
  "aeu",
  "bp",
  "ck",
  "dt",
  "eaiy",
  "fvw",
  "ghj",
  "hg",
  "ie",
  "jg",
  "kc",
  "l",
  "mn",
  "nm",
  "o",
  "pb",
  "q",
  "r",
  "sxz",
  "td",
  "ua",
  "vfw",
  "wvf",
  "xs",
  "ye",
  "zs",
  "àî",
  "áï",
  "âô",
  "ãê",
  "äò",
  "åèýÿ",
  "æø",
  "çñ",
  "èåÿ",
  "é",
  "êã",
  "ë",
  "ìí",
  "íì",
  "îà",
  "ïá",
  "ð",
  "ñç",
  "òä",
  "óþ",
  "ôâ",
  "õ",
  "ö",
  "÷",
  "øæ",
  "ù",
  "úü",
  "û",
  "üú",
  "ýå",
  "þ",
  "ÿåè",
  };

char *short_distance_errors[ALPH_N] = {
  "aqwsz",
  "bvghn",
  "cxdfv",
  "dserfcx",
  "ewsdr",
  "fdrtgvc",
  "gftyhbv",
  "hgyujnb",
  "iujko",
  "jhuikmn",
  "kjiolm",
  "lkop",
  "mnjk",
  "nbhjm",
  "oiklp",
  "pol",
  "qaw",
  "redft",
  "sawedxz",
  "trfgy",
  "uyhji",
  "vcfgb",
  "wqase",
  "xzsdc",
  "ytghu",
  "zasx",
  "àâêåïìñ",
  "áüëäþ",
  "âûóêàñ÷",
  "ãíðîø",
  "äëùçæþá",
  "åêàïí",
  "æäçõýþ",
  "çùäæõ",
  "èìïðò",
  "éôö",
  "êóâàå",
  "ëîøùäáü",
  "ìñàïè",
  "íåïðã",
  "îðãøëüò",
  "ïàåíðèì",
  "ðïíãîòè",
  "ñ÷âàì",
  "òèðîü",
  "óöûâê",
  "ôéöûÿ",
  "õçæýú",
  "öéôûó",
  "÷ÿûâñ",
  "øãîëù",
  "ùøëäç",
  "úõý",
  "ûôöóâ÷ÿ",
  "üòîëá",
  "ýæõú",
  "þáäæ",
  "ÿôû÷",
  };


/*
 *
 *         ISEARCH
 *
 */


#ifdef TYPES

#define MAXT 15
char *types[] = {
  "",
  "audio",
  "video",
  "people",
  "groups",
  "events",
  "notes" ,
  "topics",
  "statuses",
  "apps",
  "goods",
  "ads",
  "questions",
  "publics"
};

typedef long long tdata[MAXT];

// stats for each word
hmap_ll_int h_word;
int wordn, wordr;
tdata *words;

#endif


#ifndef MAXQ
#  define MAXQ 10
#endif

#define H_MUL 999983ll
#define H_ADD 327623
typedef int data[MAXQ];

// map from prefixes to suggetion ids, and suggestions by ids
hmap_ll_int h_pref;
int prefn, prefr;
data *suggs;

// map from queries to their ids, and their ratings
// ratings[0] == 0
hmap_ll_int h_id;
int idn, idr;
rating *ratings;

int *names;
int *stemmed_names;

//names of queries
char *names_buff;
int namesn, namesr;


/*
 *
 *     STATISTICS
 *
 */


#define STAT_EV 1000000
#define STAT_T 60
#define STAT_CNT 1000
#define STAT_MAX_RATE 10000000
#define STAT_BUCKET_SIZE 100
#define STAT_ST (STAT_MAX_RATE / STAT_BUCKET_SIZE + 3)

q_info *q_entry;
int *q_rev;
hmap_ll_int h_q;
int qn, qr;

int q_events[STAT_EV], qel, qer, qen;

void del_bucket (int v) {
  q_info *t = &q_entry[v];
  q_entry[t->prev_bucket].next_bucket = t->next_bucket;
  q_entry[t->next_bucket].prev_bucket = t->prev_bucket;
}

void add_bucket (int x, int bucket)  {
  q_info *entry = &q_entry[x];
  int f = bucket,
      y = q_entry[bucket].prev_bucket;

  entry->next_bucket = f;
  q_entry[f].prev_bucket = x;

  entry->prev_bucket = y;
  q_entry[y].next_bucket = x;
}

int add_q (int val) {
  long long h = val;
  h += H_ADD;
  h *= H_MUL;
  hmap_pair_ll_int tmp;
  tmp.x = h;
  tmp.y = 0;
  int *pid = &(hmap_ll_int_add (&h_q, tmp)->y);
  if (*pid == 0) {
    if (qn + 3 > qr) {
      int nqr = qr * 2 + 3;
      q_entry = dl_realloc (q_entry, sizeof (q_info) * (nqr + STAT_ST), sizeof (q_info) * (qr + STAT_ST));
      q_rev = dl_realloc (q_rev, sizeof (int) * nqr, sizeof (int) * qr);

      memset (q_entry + qr + STAT_ST, 0, sizeof (q_info) * (nqr - qr));
      memset (q_rev + qr, 0, sizeof (int) * (nqr - qr));

      qr = nqr;
    }

    *pid = ++qn;
    q_rev[qn] = val;
//    memset (&q_entry[qn + STAT_ST], 0, sizeof (q_entry[qn + STAT_ST]));
  }

  return *pid;
}

void upd_stat_ (int id, int add) {
  if (add != 1 && add != -1) {
    assert (0);
    return;
  }
  if (id < 0) {
    id = -id;
    add = -add;
  }

  assert (1 <= id && id <= idn);

  id = add_q (id);

  id += STAT_ST;
  int old_bucket = (q_entry[id].val + STAT_BUCKET_SIZE - 1) / STAT_BUCKET_SIZE;
  assert (old_bucket < STAT_ST);

  q_entry[id].val += add;
  if (q_entry[id].val > STAT_MAX_RATE) {
    q_entry[id].val = STAT_MAX_RATE;
  }

  if (q_entry[id].val < 0) {
    q_entry[id].val = 0;
  }

  int new_bucket = (q_entry[id].val + STAT_BUCKET_SIZE - 1) / STAT_BUCKET_SIZE;
  assert (new_bucket < STAT_ST);

  if (new_bucket != old_bucket) {
    int p = old_bucket;
    if (old_bucket) {
      del_bucket (id);

      if (old_bucket > new_bucket) {
        p = q_entry[old_bucket].prev_used;
      }

      if (q_entry[old_bucket].next_bucket == old_bucket) {
        p = q_entry[old_bucket].prev_used;
        int t = q_entry[p].next_used = q_entry[old_bucket].next_used;
        q_entry[t].prev_used = p;
      }
    }

    if (new_bucket) {
      if (q_entry[new_bucket].next_bucket == new_bucket) {
        int t = q_entry[new_bucket].next_used = q_entry[p].next_used;
        q_entry[t].prev_used = new_bucket;

        q_entry[p].next_used = new_bucket;
        q_entry[new_bucket].prev_used = p;
      }

      add_bucket (id, new_bucket);
    }
  }
}

void upd_stat (int id) {
  assert (-idn <= id && id <= idn && id != 0);

  if (qen > STAT_EV - 5) {
    upd_stat_ (q_events[qel++], -1);
    qen--;
    if (qel == STAT_EV) {
      qel = 0;
    }
  }

  q_events[qer] = id;
  upd_stat_ (q_events[qer++], +1);
  qen++;
  if (qer == STAT_EV) {
    qer = 0;
  }
}

int black_list_check (char *s);

int *get_top_v (int need) {
  int *v = (int *)buff;
  v[0] = 0;
  int t = q_entry[0].prev_used;

  int ignore_black_list = 0;
  if (need < 0) {
    need = -need;
    ignore_black_list = 1;
  }
  if (need > 10000) {
    need = 10000;
  }
  if (need < 0) {
    need = 0;
  }

  assert ((need + 1) * sizeof (int) < MAX_BUFF);
  while (t && need) {
    int st = t, curr = t;

    assert (0 <= curr && curr < qr);
    assert (q_entry[curr].next_bucket != st);

    while (q_entry[curr].next_bucket != st && need) {
      if (!(0 <= curr && curr < STAT_ST + qr)) {
        fprintf (stderr, "%d < %d < %d\n", 0, curr, STAT_ST + qr);
        assert (0 <= curr && curr < STAT_ST + qr);
      }

      curr = q_entry[curr].next_bucket;

      if (!(STAT_ST <= curr && curr < qr + STAT_ST)) {
        fprintf (stderr, "%d < %d < %d\n", STAT_ST, curr, qr + STAT_ST);
        assert (STAT_ST <= curr && curr < qr + STAT_ST);
      }

      v[++v[0]] = q_rev[curr - STAT_ST];
      if (ignore_black_list || !black_list_check (names_buff + names[v[v[0]]])) {
        need--;
      } else {
        v[0]--;
      }
    }
    t = q_entry[t].prev_used;
  }
  return v;
}

void get_top (char *buf, int cnt, int buf_len) {
  int *v = get_top_v (cnt);
  char *f = buf;
  int i;
  buf[0] = 0;
  for (i = 1; i <= v[0] && (int)strlen (names_buff + names[v[i]]) + 1 + (buf - f) + 1 < buf_len; i++) {
    buf += sprintf (buf, "%s\n", names_buff + names[v[i]] + 1);
//    buf += sprintf (buf, "%s ("FD" / %d)\n", names_buff + names[v[i]], ratings[v[i]], q_entry[v[i] + STAT_ST].val);
  }
}

int *get_best_v (int need) {
  int *v = (int *)buff;
  v[0] = 0;

  int ignore_black_list = 0;
  if (need < 0) {
    need = -need;
    ignore_black_list = 1;
  }
  if (need > MAX_BEST) {
    need = MAX_BEST;
  }
  assert ((need + 1) * sizeof (int) < MAX_BUFF);

  int t;
  for (t = 0; t < bestn && need; t++) {
    if (ignore_black_list || !black_list_check (names_buff + names[best[t]])) {
      v[++v[0]] = best[t];
      need--;
    }
  }
  return v;
}

void get_best (char *buf, int cnt, int buf_len) {
  int *v = get_best_v (cnt);
  char *f = buf;
  int i;
  buf[0] = 0;
  for (i = 1; i <= v[0] && (int)strlen (names_buff + names[v[i]]) + 1 + (buf - f) + 1 < buf_len; i++) {
    buf += sprintf (buf, "%s\n", names_buff + names[v[i]] + 1);
  }
}

/*
 *
 *    UTILS
 *
 */

inline int is_letter (int x) {
  return ('a' <= x && x <= 'z') || (1072 <= x && x <= 1103);
}

// all texts are smaller then 32768
#define MAX_S_LEN 150000

int *prepare_str_UTF8 (int *v_init, int keep_trailing_space) {
  static int v[MAX_S_LEN];

  assert (v_init != v);
  int i;
  v[0] = 32;
  for (i = 0; v_init[i]; i++) {
    v[i + 1] = convert_prep (v_init[i]);
  }
  v[i + 1] = 0;

  i = 1;
  int ni = 1;
  while (v[i] == 32) {
    i++;
  }
  while (v[i]) {
    while (v[i] && v[i] != 32) {
      v[ni++] = v[i++];
    }
    if (v[i] == 32) {
      v[ni++] = 32;
      while (v[i] == 32) {
        i++;
      }
    }
  }
  if (ni > 1 && v[ni - 1] == ' ' && !keep_trailing_space) {
    ni--;
  }
  v[ni++] = 0;

  return v;
}

char *prepare_str (char *x, int keep_trailing_space) {
  static int prep_ibuf[MAX_S_LEN];
  static char prep_buf[MAX_S_LEN];

  if (x == NULL || strlen (x) >= MAX_S_LEN / 4) {
    return NULL;
  }

  good_string_to_utf8 ((unsigned char *)x, prep_ibuf);
  int *v = prepare_str_UTF8 (prep_ibuf, keep_trailing_space);
  char *s = prep_buf;

  while (*v != 0) {
    s += put_char_utf8 (*v++, s);
  }
  *s++ = 0;

  assert (s - prep_buf < MAX_S_LEN);

  return prep_buf;
}


int stem_str (const char *s) {
  int i, n;
  static int v[MAX_S_LEN];
  good_string_to_utf8 ((unsigned char *)s, v);

  for (n = 0; v[n]; n++) ;

  if (n >= 3) {
    for (i = n - 1; i >= 0 && is_letter (v[i]); i--) {
    }
    if (v[n - 1] <= ' ') {
      for (i = 0; s[i]; i++) {
        fprintf (stderr, "%d ", (int)s[i]);
      }
      fprintf (stderr, "\n");
      for (i = 0; v[i]; i++) {
        fprintf (stderr, "%d ", (int)v[i]);
      }
      fprintf (stderr, "\n");
    }
    assert (v[n - 1] > ' ');
    if (v[n - 1] > 'z') {
      v[i + 1 + stem_rus_utf8i (v + i + 1, 1)] = 0;
    } else {
      v[i + 1 + stem_engi (v + i + 1)] = 0;
    }
  }

  long long h = 0;
  for (n = 0; v[n]; n++) {
    h = h * H_MUL + v[n];
  }

  return (int)(h + H_ADD);
}


long long *gen_hashes (char *s) {
  if (verbosity > 2) {
    fprintf (stderr, "  gen_hashes <%s>\n", s);
  }
  static long long buf[MAX_S_LEN], h = 0;

  int i = 0, ri = 0;

  while (s[i]) {
    while (s[i] != ' ' && s[i])   {
      h = h * H_MUL + s[i++];
    }

    if (h) {
      buf[ri++] = h;
      h = 0;
    }

    if (s[i]) {
      i++;
    }
  }
  buf[ri] = 0;

  return buf;
}

//less or equal
inline int rating_cmp (const int a, const int b) {
  return ratings[a] > ratings[b] || (ratings[a] == ratings[b] && a <= b);
}

inline void rating_incr (const int a, rating cnt) {
#ifdef FADING
  rating dt = ((rating)(now - ratingT)) / RATING_NORM;
  ratings[a] += expf (dt) * cnt;
#else
  ratings[a] += cnt;
  if (ratings[a] < 0) {
    ratings[a] = 0;
  }
#endif
}

long long hp[MAX_S_LEN];
int alph[100], alph_n;

inline int get_id_by_hash (const long long h) {
  hmap_pair_ll_int fi;
  fi.x = h + H_ADD;
  hmap_pair_ll_int *tmp = hmap_ll_int_get (&h_id, fi);

  if (tmp == NULL) {
    return 0;
  }
  return tmp->y;
}

int get_id (int *v) {
  long long h = 0;
  int i;
  for (i = 0; v[i]; i++) {
    h = h * H_MUL + v[i];
  }

  return get_id_by_hash (h);
}

int add_id (char *s, int *v, int debug) {
  long long h = 0;
  int i;
  for (i = 0; v[i]; i++) {
    h = h * H_MUL + v[i];
  }

  h += H_ADD;
  hmap_pair_ll_int tmp;
  tmp.x = h;
  tmp.y = 0;
  int *pid = &(hmap_ll_int_add (&h_id, tmp)->y);

  if (idn + 3 > idr) {
    int nidr = idr * 2 + 3;
    names = dl_realloc (names, sizeof (int) * nidr, sizeof (int) * idr);
    stemmed_names = dl_realloc (stemmed_names, sizeof (int) * nidr, sizeof (int) * idr);
    ratings = dl_realloc (ratings, sizeof (rating) * nidr, sizeof (rating) * idr);
    ratings[0] = 0;
    idr = nidr;

    assert (names != NULL);
    assert (stemmed_names != NULL);
    assert (ratings != NULL);
  }

  if (*pid == 0) {
    *pid = ++idn;

    int sn = strlen (s);
    if (namesn + sn + 3 > namesr) {
      int nnamesr = namesr * 2 + sn + 3;
      names_buff = dl_realloc (names_buff, sizeof (char) * nnamesr, sizeof (char) * namesr);
      namesr = nnamesr;

      assert (names_buff != NULL);
    }

    names[idn] = namesn;
    if (use_stemmer) {
      stemmed_names[idn] = stem_str (s);
    } else {
      stemmed_names[idn] = idn;
    }
    if (debug) {
      fprintf (stderr, "<%s>\n", s);
    }

    strcpy (names_buff + namesn, s);
    namesn += sn + 1;
    ratings[idn] = 0;
  }

  return *pid;
}

int add_pref (long long h) {
  h += H_ADD;
  hmap_pair_ll_int tmp;
  tmp.x = h;
  tmp.y = 0;
  int *pid = &(hmap_ll_int_add (&h_pref, tmp)->y);

  if (prefn + 3 > prefr) {
    int nprefr = prefr * 2 + 3;
    suggs = dl_realloc (suggs, sizeof (data) * nprefr, sizeof (data) * prefr);
    prefr = nprefr;
  }

  if (*pid == 0) {
    *pid = ++prefn;
    memset (suggs[prefn], 0, sizeof (data));
  }

  return *pid;
}

int get_pref (long long h) {
  h += H_ADD;
  hmap_pair_ll_int tmp, *ptmp;
  tmp.x = h;
  tmp.y = 0;
  ptmp = hmap_ll_int_get (&h_pref, tmp);
  if (ptmp == NULL) {
    return 0;
  }

  return ptmp->y;
}

#ifdef TYPES
int add_word (long long h) {
  h += H_ADD;
  hmap_pair_ll_int tmp;
  tmp.x = h;
  tmp.y = 0;
  int *pid = &(hmap_ll_int_add (&h_word, tmp)->y);

  if (wordn + 3 > wordr) {
    int nwordr = wordr * 2 + 3;
    words = dl_realloc (words, sizeof (tdata) * nwordr, sizeof (tdata) * wordr);
    wordr = nwordr;
  }

  if (*pid == 0) {
    *pid = ++wordn;
    memset (words[wordn], 0, sizeof (tdata));
  }

  return *pid;
}

static int load_word (long long h) {
  h += H_ADD;
  hmap_pair_ll_int tmp, *ptmp;
  tmp.x = h;
  tmp.y = 0;
  ptmp = hmap_ll_int_get (&h_word, tmp);
  if (ptmp == NULL) {
    return 0;
  }

  return ptmp->y;
}
#endif

//probability for misprinting one letter by another
double prob[128][128];

/*
 *  Global variables for rating comparision
 */
int try; // try == need keyboard layout change
int translit; // translit == need transliteration
int len; // length of current request
int diff; // position of first changed symbol or -1
int A, B; // if diff != -1 this is first changed symbol and for what it changed

float get_rating (const int a) {
  float res = logf (ratings[a] + 1.0f) / logf (2.0f);

  if (translit) {
    res /= 1.3;
  }

  if (diff != -1) {
    res *= (diff + 2 * len + 1);
    res /= (6 * len + 3);

    if (len <= 6) {
      res /= (7 - len);
    }

#ifdef SLOW
    if (A != B) {
      if (is_letter (A) && is_letter (B)) {
        res *= prob[A % 1072 % 128][B % 1072 % 128];
      } else {
        res *= 0.5f;
      }
    }
#endif
  }

  if (try) {
    res /= 10;

    if (translit) {
      res /= 2;
    }
  }

  return res;
}

#define swap(a, b) { typeof (a) t = a; a = b; b = t; }

data q;
float rating_q[MAXQ];

void merge (data p) {
  int ip, iq;

  for (ip = 0; ip < MAXQ && ratings[p[ip]] > RATING_MIN; ip++) {
    if (!black_list_check (names_buff + names[p[ip]])) {
//      fprintf (stderr, "<%s>\n", names_buff + names[p[ip]]);
      float rb = get_rating (p[ip]);

      for (iq = 0; iq < MAXQ; iq++)
        if (stemmed_names[q[iq]] == stemmed_names[p[ip]]) {
          if (rb > rating_q[iq]) {
            rating_q[iq] = rb;

            while (iq > 0 && rating_q[iq - 1] < rating_q[iq]) {
              swap (rating_q[iq - 1], rating_q[iq]);
              swap (q[iq - 1], q[iq]);
            }
          }

          break;
        }

      if (iq == MAXQ) {
        if (rating_q[MAXQ - 1] < rb) {
          int i;
          for (i = MAXQ - 2; i >= 0 && rating_q[i] < rb; i--) {
            q[i + 1] = q[i];
            rating_q[i + 1] = rating_q[i];
          }
          q[i + 1] = p[ip];
          rating_q[i + 1] = rb;
        }
      }
    }
  }
}

void update_answer_suggestions (long long h, int _diff, int _A, int _B) {
  int sid = get_pref (h);
  if (sid) {
    diff = _diff;
    A = _A;
    B = _B;
    merge (suggs[sid]);
  }
}

int cur_id; // global variable for id of current request
void update_answer_correct_mistake (long long h, int _diff, int _A, int _B) {
  int id = get_id_by_hash (h);
  if (id != 0 && id != cur_id && ratings[id] > lowest_rate && !black_list_check (names_buff + names[id])) {
    diff = _diff;
    A = _A;
    B = _B;
    data temp = {id};
    merge (temp);
  }
}

int mistakes_cnt;
int mistakes[100000];
char *mark;

void update_answer_mistakes (long long h, int _diff, int _A, int _B) {
  int id = get_id_by_hash (h);
  if (!mark[id] && ratings[id] > lowest_rate && !black_list_check (names_buff + names[id])) {
    int j;
    for (j = 0; j < mistakes_cnt && id != mistakes[j]; j++) {
    }
    if (j == mistakes_cnt) {
      mistakes[mistakes_cnt++] = id;
    }
  }
}

void get_hints_UTF8 (int *v, void (*update_answer) (long long h, int _diff, int _A, int _B) ) {
  static long long pref_h[MAX_S_LEN], suff_h[MAX_S_LEN];
  int i, j, n;

  translit = 0;

  for (n = 0; v[n]; n++) ;
  assert (0 <= n && n + 1 < MAX_S_LEN);

/*  if (binlog_readed) {
    fprintf (stderr, "    -----------------");
    for (i = 0; i < n; i++)
      fprintf (stderr, "%c", (char)v[i]);
    fprintf (stderr, "----------------\n");
  }*/

  len = n;

  pref_h[0] = 0;
  for (i = 0; i < n; i++) {
    pref_h[i + 1] = pref_h[i] * H_MUL + v[i];
  }

  suff_h[n] = 0;
  for (i = n - 1; i >= 0; i--) {
    suff_h[i] = v[i] * hp[n - i - 1] + suff_h[i + 1];
  }

  // no misprints
  update_answer (suff_h[0], -1, -1, -1);

  // one extra letter
  for (i = 0; i < n; i++) {
    update_answer (pref_h[i] * hp[n - i - 1] + suff_h[i + 1], i, -1, -1);
  }

  // swap
  for (i = 0; i + 1 < n; i++) {
    update_answer (pref_h[i] * hp[n - i] + v[i + 1] * hp[n - i - 1] + v[i] * hp[n - i - 2] + suff_h[i + 2], i, -1, -1);
  }

  // misprint
  for (i = 0; i < n; i++) {
    for (j = 0; j < alph_n; j++) {
      update_answer (pref_h[i] * hp[n - i] + alph[j] * hp[n - i - 1] + suff_h[i + 1], i, v[i], alph[j]);
    }
  }

  // missed letters
  for (i = 0; i <= n; i++) {
    for (j = 0; j < alph_n; j++) {
      update_answer (pref_h[i] * hp[n - i + 1] + alph[j] * hp[n - i] + suff_h[i], i, -1, -1);
    }
  }

  //TODO double letters....

  translit = 1;

  int hn, thn;
  long long *h = pref_h;
  translit_from_en_to_ru (v, h, &hn);
  translit_from_ru_to_en (v, h + hn, &thn);
  hn += thn;

  assert (hn < MAX_S_LEN);

  for (i = 0; i + 1 < hn; i++) {
    for (j = hn - 1; j > i; j--) {
      if (h[j - 1] > h[j]) {
        long long tmp = h[j - 1];
        h[j - 1] = h[j];
        h[j] = tmp;
      }
    }
  }

  for (i = 0; i < hn; i++) {
    if (i == 0 || h[i] != h[i - 1]) {
      update_answer (h[i], -1, -1, -1);
    }
  }
}


inline int bad_letters (int x) {
  switch (x) {
  case 1093://õ
  case 1098://ú
  case 1078://æ
  case 1101://ý
  case 1073://á
  case 1102://þ
  case 1105://¸
  case 1061://Õ
  case 1066://Ú
  case 1046://Æ
  case 1069://Ý
  case 1041://Á
  case 1070://Þ
  case 1025://¨
    return 1;
  }
  return 0;
}

#ifdef TYPES
double *get_types (char *buff) {
  static double p[MAXT];
  char *s = prepare_str (buff, 0);

//  fprintf (stderr, "  get_types ... <%s>\n", s);
  long long *hs = gen_hashes (s);
  int hn = 0;
  while (hs[hn]) {
    hn++;
  }
//  fprintf (stderr, "  hn = %d\n", hn);

  int i, j;
  int id = add_word (0);
  long long *all = words[id];

  for (i = 0; i < MAXT; i++) {
    p[i] = all[i];
  }

  for (i = 0; i < hn; i++) {
    int id = load_word (hs[i]);

    long long *a = words[id];

    for (j = 0; j < MAXT; j++) {
      double mul;
      if (all[j] > 5) {
        mul = (double)a[j] / all[j];
      } else {
        mul = 0;
      }

      if (mul < 1e-7) {
        mul = 1e-7;
      }
      if (mul > 1 - 1e-7) {
        mul = 1 - 1e-7;
      }
      p[j] *= mul;
    }
  }

  double sum = 0;
  for (j = 0; j < MAXT; j++) {
    sum += p[j];
  }

  for (j = 0; j < MAXT; j++) {
    p[j] /= sum;
  }

  return p;
}
#endif

// 1 -- hints
// 2 -- types
// 3 -- debug
void get_hints (char *buf, int mode, int buf_len) {
  static int v[MAX_S_LEN];
  if (strlen (buf) > MAX_S_LEN - 2) {
    buf[0] = 0;
    return;
  }

  int i;
  if (mode & 1) {
    memset (q, 0, sizeof (q));
    memset (rating_q, 0, sizeof (rating_q));

    good_string_to_utf8 ((unsigned char *)buf, v);

    for (try = 0; try < 2; try++) {
      int *s = prepare_str_UTF8 (v, 1);
      assert (s != NULL);

      get_hints_UTF8 (s, update_answer_suggestions);

      for (i = 0; v[i]; i++) {
        if (bad_letters (v[i])) {
          try = 1;
          break;
        }
        v[i] = convert_language (v[i]);
      }
    }
  }

#ifdef TYPES
  double *p = NULL;
  if (mode & 2) {
    p = get_types (buf);
  }
#endif

  *buf = 0;

  if (mode & 1) {
    for (i = 0; i < MAXQ && q[i] && (i == 0 || rating_q[i - 1] < 2 * rating_q[i]) && rating_q[i] > 0.4f; i++) {
#ifdef TYPES
      if (mode & 2) {
        buf += sprintf (buf, "'%s' (%d) ("FD" / %f)\n", names_buff + names[q[i]] + 1, stemmed_names[q[i]], ratings[q[i]], rating_q[i]);
      } else {
#endif
        if (i) {
          *buf++ = '\n';
        }
        buf += sprintf (buf, "%s", names_buff + names[q[i]] + 1);
#ifdef TYPES
      }
#endif
    }
  }

#ifdef TYPES
  if (mode & 2) {
    if (mode & 1) {
      buf += sprintf (buf, "\n");
    }
    for (i = 0; i < MAXT; i++) {
      int best = 0, j;
      for (j = 1; j < MAXT; j++) {
        if (p[j] > p[best]) {
          best = j;
        }
      }

      if (mode & 1) {
        if (p[best] <= 1e-6) {
          break;
        }
        buf += sprintf (buf, "%s (%lf)\n", types[best], p[best]);
      } else {
        if (p[best] <= 0.01) {
          break;
        }

        if (i) {
          *buf++ = ',';
        }
        buf += sprintf (buf, "%s:%d", types[best], (int)floor (p[best] * 100 + 0.5 + 1e-9));
      }
      p[best] = -1;
    }
  }
#endif
}

void get_suggestion (char *buf, int buf_len) {
  static int v[MAX_S_LEN];
  if (strlen (buf) > MAX_S_LEN - 2) {
    buf[0] = 0;
    return;
  }

  int i;
  memset (q, 0, sizeof (q));
  memset (rating_q, 0, sizeof (rating_q));

  good_string_to_utf8 ((unsigned char *)buf, v);

  for (try = 0; try < 2; try++) {
    int *s = prepare_str_UTF8 (v, 1);
    assert (s != NULL);

    if (try == 0) {
      cur_id = get_id (s);
    }

    get_hints_UTF8 (s, update_answer_correct_mistake);

    for (i = 0; v[i]; i++) {
      if (bad_letters (v[i])) {
        try = 1;
        break;
      }
      v[i] = convert_language (v[i]);
    }
  }

  if (q[0] && rating_q[0] > 0.4f && strlen (names_buff + names[q[0]] + 1) + 1 < buf_len) {
    sprintf (buf, "%s", names_buff + names[q[0]] + 1);
  } else {
    *buf = 0;
  }
}

int isearch_set_stat (struct lev_isearch_set_stat *E) {
#ifdef TYPES
  int stype = E->type & 0xFF;
#endif

  int cnt = E->cnt, add = 1;//, uid = E->uid;
#ifdef FADING
  if (cnt < 0) {
    cnt = 0;
  }
#else
  if (cnt < 0) {
    add = -1;
    cnt = -cnt;
  }
#endif

  if (verbosity > 0) {
    static int _cnt = 0;
    _cnt++;
    if (_cnt % 100000 == 0) {
      static int prev = 0;

      fprintf (stderr, "\r%d, id_n = %10d(%5d), pref_n = %d", _cnt / 100000, idn, idn - prev, prefn);
#ifdef TYPES
      fprintf (stderr, "\r, word_n = %d", wordn);
#endif
      prev = idn;
    }
  }

  char *s = E->text;
//  fprintf (stderr, "%s\n", E->text);
  int ok = 1;
  while (s[0]) {
    int j;

    for (j = 0; s[j] && s[j] != '\t'; j++);
    char c = s[j];
    s[j] = 0;

    char *ss = prepare_str (s, 0);
    if (ss != NULL && strlen (ss) > 1 && cnt > 0) {
      static int v[MAX_S_LEN];
      good_string_to_utf8 ((unsigned char *)ss, v);

      int id = add_id (ss, v, 0);
      upd_stat (id * add);
#ifdef FADING
      rating_incr (id, log (1.0 + cnt) * 1.4426950408889634073599246810019);
#else
      rating_incr (id, add);
#endif

#ifdef TYPES
      if (strlen (ss) > 1) {
        long long *hs = gen_hashes (ss);

        int hn = 0, i;
        while (hs[hn])  {
          hn++;
        }
        hs[hn++] = 0;

        for (i = 0; i < hn; i++) {
          int id = add_word (hs[i]);
          words[id][stype] += (long long) (log (3.0 + cnt) * 0.7213475204445);
        }
      }
#endif
    } else {
      ok = 0;
    }

    s += j;
    if (c == '\t') {
      *s++ = c;
    }
  }

  return ok;
}


int tl_do_isearch_get_hints (struct tl_act_extra *extra) {
  struct tl_isearch_get_hints *e = (struct tl_isearch_get_hints *)extra->extra;

  static int v[MAX_S_LEN];
  if (e->query_len > MAX_S_LEN - 2) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Query is too long");
    return -1;
  }

  memset (q, 0, sizeof (q));
  memset (rating_q, 0, sizeof (rating_q));

  good_string_to_utf8 ((unsigned char *)e->query, v);

  int i;
  for (try = 0; try < 2; try++) {
    int *s = prepare_str_UTF8 (v, 1);
    assert (s != NULL);

    get_hints_UTF8 (s, update_answer_suggestions);

    for (i = 0; v[i]; i++) {
      if (bad_letters (v[i])) {
        try = 1;
        break;
      }
      v[i] = convert_language (v[i]);
    }
  }

  tl_store_int (TL_VECTOR);
  int *size = tl_store_get_ptr (4);
  for (i = 0; i < MAXQ && q[i] && (i == 0 || rating_q[i - 1] < 2 * rating_q[i]) && rating_q[i] > 0.4f; i++) {
    tl_store_string0 (names_buff + names[q[i]] + 1);
  }
  *size = i;

  return 0;
}

#ifdef TYPES
int tl_do_isearch_get_types (struct tl_act_extra *extra) {
  struct tl_isearch_get_types *e = (struct tl_isearch_get_types *)extra->extra;

  if (e->query_len > MAX_S_LEN - 2) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Query is too long");
    return -1;
  }

  double *p = get_types (e->query);

  tl_store_int (TL_ISEARCH_TYPE_INFO);
  int *size = tl_store_get_ptr (4);
  int i;
  for (i = 0; i < MAXT; i++) {
    int best = 0, j;
    for (j = 1; j < MAXT; j++) {
      if (p[j] > p[best]) {
        best = j;
      }
    }

    if (p[best] <= 0.01) {
      break;
    }
    tl_store_int (best);
    tl_store_double (p[best]);

    p[best] = -1;
  }
  *size = i;

  return 0;
}
#endif

int tl_do_isearch_get_top (struct tl_act_extra *extra) {
  struct tl_isearch_get_top *e = (struct tl_isearch_get_top *)extra->extra;

  int *v = get_top_v (e->limit);

  tl_store_int (TL_VECTOR);
  tl_store_int (v[0]);
  int i;
  for (i = 1; i <= v[0]; i++) {
    tl_store_string0 (names_buff + names[v[i]] + 1);
  }

  return 1;
}

int tl_do_isearch_get_best (struct tl_act_extra *extra) {
  struct tl_isearch_get_best *e = (struct tl_isearch_get_best *)extra->extra;

  int *v = get_best_v (e->limit);

  tl_store_int (TL_VECTOR);
  tl_store_int (v[0]);
  int i;
  for (i = 1; i <= v[0]; i++) {
    tl_store_string0 (names_buff + names[v[i]] + 1);
  }

  return 1;
}

/*
 *
 *     BLACK LIST
 *
 */


int aho_black_list_size;
trie_node *black_list;
trie_arr_node *aho_black_list;

void black_list_set_string (char *buff, int f) {
  if (verbosity > 2) {
    fprintf (stderr, "set %d string <%s>\n", f, buff);
  }
  int size = strlen (buff);
  char *s = buff;
  int i;
  char t;
  for (i = 0; i <= size; i++) {
    if (buff[i] == '\t' || buff[i] == 0) {
      t = buff[i];
      buff[i] = 0;
      if (*s) {
        if (f) {
          trie_del (black_list, prepare_str (s, 1));
        } else {
          trie_add (&black_list, prepare_str (s, 1));
        }
      }
      if (verbosity > 2) {
        fprintf (stderr, " -- <%s>\n", s);
      }
      s = buff + i + 1;
      buff[i] = t;
    }
  }
}

int black_list_set (struct lev_isearch_black_list *E) {
  black_list_set_string (E->text, E->type & 1);
  return 1;
}

int black_list_check (char *s) {
  if (aho_black_list == NULL) {
    return 0;
  }
  return trie_arr_check (aho_black_list, s);
}

char *black_list_get (void) {
  trie_encode (black_list, buff, 0);
  int len = 0;
  trie_arr_text_save ((trie_arr_node *)buff, buff2, &len);
  if (len) {
    len--;
  }

  buff2[len] = 0;
  return buff2;
}

void black_list_force (void) {
  dl_free (aho_black_list, aho_black_list_size);

  aho_black_list_size = trie_encode (black_list, buff, 0);
  trie_arr_aho ((trie_arr_node *)buff);
  aho_black_list = dl_malloc (aho_black_list_size);
  memcpy (aho_black_list, buff, aho_black_list_size);
}


/*
 *
 *           BINLOG
 *
 */

int do_isearch_set_stat (int uid, int stype, int cnt, char *text, int text_len) {
  if (text_len >= 32768 || text_len == 0) {
    return 0;
  }
  if (stype < 0 || stype >= 256) {
    return 0;
  }

  struct lev_isearch_set_stat *E =
    alloc_log_event (LEV_ISEARCH_SET_STAT + stype, offsetof (struct lev_isearch_set_stat, text) + 1 + text_len, 0);

  E->uid = uid;
  E->cnt = cnt;
  E->text_len = text_len;
  memcpy (E->text, text, sizeof (char) * (text_len + 1));

  return isearch_set_stat (E);
}

int do_black_list_add (const char *s, int text_len) {
  if (text_len >= 32768 || text_len <= 1) {
    return 0;
  }

  struct lev_isearch_black_list *E =
    alloc_log_event (LEV_ISEARCH_BLACK_LIST, offsetof (struct lev_isearch_black_list, text) + 1 + text_len, 0);

  E->text_len = text_len;
  memcpy (E->text, s, sizeof (char) * (text_len + 1));

  return black_list_set (E);
}

int do_black_list_delete (const char *s, int text_len) {
  if (text_len >= 32768 || text_len <= 1) {
    return 0;
  }

  struct lev_isearch_black_list *E =
    alloc_log_event (LEV_ISEARCH_BLACK_LIST + 1, offsetof (struct lev_isearch_black_list, text) + 1 + text_len, 0);

  E->text_len = text_len;
  memcpy (E->text, s, sizeof (char) * (text_len + 1));

  return black_list_set (E);
}


int isearch_replay_logevent (struct lev_generic *E, int size);

int init_isearch_data (int schema) {
  replay_logevent = isearch_replay_logevent;
  return 0;
}

void try_init_local_uid (void) {
  static int was = 0;
  static int old_log_split_min, old_log_split_max, old_log_split_mod;

  if (was) {
//    fprintf (stderr, "%d vs %d | %d vs %d | %d vs %d\n", old_log_split_min, log_split_min, old_log_split_max, log_split_max, old_log_split_mod, log_split_mod);
    assert (old_log_split_min == log_split_min && old_log_split_max == log_split_max && old_log_split_mod == log_split_mod);
    return;
  }

  was = 1;
  old_log_split_min = log_split_min;
  old_log_split_max = log_split_max;
  old_log_split_mod = log_split_mod;

  log_schema = ISEARCH_SCHEMA_V1;
  init_isearch_data (log_schema);
}

static int isearch_le_start (struct lev_start *E) {
  if (E->schema_id != ISEARCH_SCHEMA_V1) {
    return -1;
  }

  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

  try_init_local_uid();

  return 0;
}

int isearch_replay_logevent (struct lev_generic *E, int size) {
  int s;

  if (index_mode) {
    if (dl_get_memory_used() > 2.0 * max_memory) {
      save_index();
      exit (13);
    }
  }

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return isearch_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_ISEARCH_SET_STAT ... LEV_ISEARCH_SET_STAT + 255:
    if (size < (int)sizeof (struct lev_isearch_set_stat)) {
      return -2;
    }
    s = ((struct lev_isearch_set_stat *) E)->text_len;
    if (s < 0) {
      return -4;
    }

    s += 1 + offsetof (struct lev_isearch_set_stat, text);
    if (size < s) {
      return -2;
    }
    isearch_set_stat ((struct lev_isearch_set_stat *)E);
    return s;
  case LEV_ISEARCH_BLACK_LIST ... LEV_ISEARCH_BLACK_LIST + 1:
    if (size < (int)sizeof (struct lev_isearch_black_list)) {
      return -2;
    }
    s = ((struct lev_isearch_black_list *) E)->text_len;
    if (s < 0) {
      return -4;
    }
    s += 1 + offsetof (struct lev_isearch_black_list, text);
    if (size < s) {
      return -2;
    }
    black_list_set ((struct lev_isearch_black_list *)E);
    return s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


/*
 *
 *         ISEARCH INDEX
 *
 */


long long get_index_header_size (index_header *header) {
  return sizeof (index_header);
}


void load_black_list (int size) {
  assert (size + 1 < MAX_BUFF);

  int r = read (fd[0], buff, size);
  if (verbosity > 1) {
    fprintf (stderr, "black list read %d\n", size);
  }
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position 0: %m\n", r, size);
  }
  assert (r == size);

  buff[size] = 0;

  if (verbosity > 2) {
    fprintf (stderr, "ADD %s\n", buff);
  }
  black_list_set_string (buff, 0);

  aho_black_list_size = trie_encode (black_list, buff, 0);
  trie_arr_aho ((trie_arr_node *)buff);
  aho_black_list = dl_malloc (aho_black_list_size);
  memcpy (aho_black_list, buff, aho_black_list_size);
}

int save_black_list (void) {
  // TODO: fix to trie_save_text
  trie_encode (black_list, buff, 0);
  int len = 0;
  trie_arr_text_save ((trie_arr_node *)buff, buff2, &len);
  trie_arr_aho ((trie_arr_node *)buff);

  assert (write (fd[1], buff2, len) == len);

  //fprintf (stderr, "black_list_write = %d <%s>\n", len, buff2);
  return len;
}


int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    header.magic = ISEARCH_INDEX_MAGIC;
    header.log_pos0 = 0;
    header.log_pos1 = 0;
    header.log_timestamp = 0;
    header.log_split_min = 0;
    header.log_split_max = 1000000000;
    header.log_split_mod = 1;
    header.log_pos0_crc32 = 0;
    header.log_pos1_crc32 = 0;

    header.black_list_size = 0;
    header.prefn = 0;
    header.idn = 0;
    header.namesn = 0;
    header.wordn = 0;
    header.bestn = 0;

    header.created_at = time (NULL);
    header_size = sizeof (index_header);
    header.lowest_rate = lowest_rate = -1.0;
    header.use_stemmer = 1;

    black_list = NULL;
    aho_black_list = NULL;

    return 0;
  }

  fd[0] = Index->fd;
  int offset = Index->offset;
  //fsize = Index->info->file_size - Index->offset;
  // read header
  assert (lseek (fd[0], offset, SEEK_SET) == offset);

  int size = sizeof (index_header);
  int r = read (fd[0], &header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %d: %m\n", r, size, offset);
  }
  assert (r == size);

  if (header.magic != ISEARCH_INDEX_MAGIC) {
    fprintf (stderr, "bad isearch index file header\n");
    assert (0);
  }

  if (header.maxq != MAXQ && header.maxq) {
    fprintf (stderr, "MAXQ was changed, need full reindexing\n");
    assert (0);
  }

#ifdef TYPES
  if (header.maxt != MAXT && header.maxt) {
#else
  if (header.maxt) {
#endif
    fprintf (stderr, "MAXT was changed, need full reindexing\n");
    assert (0);
  }

#ifdef NOFADING
  if (header.nofading != 1) {
#else
  if (header.nofading == 1) {
#endif
    fprintf (stderr, "FADING was changed, need full reindexing\n");
    assert (0);
  }

  load_black_list (header.black_list_size);

  prefn = header.prefn;
  idn = header.idn;
  namesn = header.namesn;
#ifdef TYPES
  wordn = header.wordn;
#else
  assert (header.wordn == 0);
#endif
  bestn = header.bestn;
  assert (read (fd[0], best, bestn * sizeof (int)) == (ssize_t)(bestn * sizeof (int)));

  int tsize = header.h_pref_size < header.h_id_size ? header.h_id_size : header.h_pref_size;
  if (tsize < header.h_word_size) {
    tsize = header.h_word_size;
  }

  char *buff = dl_malloc (tsize);
  int len;

  //hmap_ll_int h_pref;
  assert (read (fd[0], buff, header.h_pref_size) == header.h_pref_size);
  hmap_ll_int_decode (&h_pref, buff, header.h_pref_size);

  //data *suggs;
  if (prefn) { // fail -- this if is important
    prefr = prefn * 2 + 3;
    suggs = dl_malloc0 (sizeof (data) * (prefr));
    len = sizeof (data) * (prefn + 1);
    assert (read (fd[0], suggs, len) == len);
  }

  //hmap_ll_int h_id;
  assert (read (fd[0], buff, header.h_id_size) == header.h_id_size);
  hmap_ll_int_decode (&h_id, buff, header.h_id_size);

  if (idn) {
    idr = idn * 2 + 3;
    namesr = namesn * 2 + 3;

    //rating *ratings;
    ratings = dl_malloc0 (sizeof (rating) * idr);
    len = sizeof (rating) * (idn + 1);
    assert (read (fd[0], ratings, len) == len);

    //int *names;
    names = dl_malloc0 (sizeof (int) * idr);
    len = sizeof (int) * (idn + 1);
    assert (read (fd[0], names, len) == len);

    //int *stemmed_names;
    stemmed_names = dl_malloc0 (sizeof (int) * idr);
    if (header.use_stemmer) {
      len = sizeof (int) * (idn + 1);
      assert (read (fd[0], stemmed_names, len) == len);
    }

    //char *names_buff;
    names_buff = dl_malloc0 (sizeof (char) * namesr);
    len = sizeof (char) * (namesn + 1);
    assert (read (fd[0], names_buff, len) == len);
  }

#ifdef TYPES
  //hmap_ll_int h_pref;
  assert (read (fd[0], buff, header.h_word_size) == header.h_word_size);
  hmap_ll_int_decode (&h_word, buff, header.h_word_size);

  if (wordn) {
    wordr = wordn * 2 + 3;
    words = dl_malloc0 (sizeof (tdata) * (wordr));
    len = sizeof (tdata) * (wordn + 1);
    assert (read (fd[0], words, len) == len);
  }
#endif

  dl_free (buff, tsize);

  assert (header.log_split_max);
  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;

  header_size = get_index_header_size (&header);
  lowest_rate = header.lowest_rate;

  if (verbosity > 1) {
    fprintf (stderr, "header loaded %d %d %d\n", log_split_min, log_split_max, log_split_mod);
    fprintf (stderr, "ok\n");
  }
  return 1;
}

rating qfind (rating *a, int l, int r, int k) {
  assert (l <= k && k <= r);

  if (l == r) {
    return a[l];
  }

  rating c = a[l + rand() % (r - l + 1)], t;
  int i = l, j = r;

  while (i <= j) {
    while (a[i] < c) {
      i++;
    }
    while (c < a[j]) {
      j--;
    }
    if (i <= j) {
      t = a[i];
      a[i] = a[j];
      a[j] = t;
      i++;
      j--;
    }
  }

  if (k <= j) {
    return qfind (a, l, j, k);
  }
  return qfind (a, j + 1, r, k);
}


void upd (data q, int id) {
  int last_id = q[MAXQ - 1];
  if (rating_cmp (id, last_id)) {
    int i, was_id = (last_id == id);
    for (i = MAXQ - 2; i >= 0 && rating_cmp (id, q[i]); i--) {
      if (was_id) {
        q[i + 1] = q[i];
      }
      was_id |= (q[i] == id);
    }

    if (!was_id) {
      for (was_id = MAXQ - 2; was_id > i; was_id--) {
        q[was_id + 1] = q[was_id];
      }
    }
    q[i + 1] = id;
  }
}

void fix_query (char *s) {
  static int v[MAX_S_LEN];

  if (black_list_check (s)) {
    return;
  }

  char *ss = prepare_str (s, 0);
  good_string_to_utf8 ((unsigned char *)ss, v);                				
  int i;
  for (i = 0; v[i]; i++) ;
  if (i <= 3) {
    return;
  }

  int id = add_id (ss, v, 1);

  long long ch = 0;
  for (i = 0; v[i]; i++) {
    #ifndef SLOW
    long long nch = ch;
    int j;
    #endif

    ch = ch * H_MUL + v[i];
    int sid = add_pref (ch);
    upd (suggs[sid], id);

    #ifndef SLOW
    v[i] = '?';
    for (j = i; v[j]; j++) {
      nch = nch * H_MUL + v[j];
      int sid = add_pref (nch);
      upd (suggs[sid], id);
    }
    #endif
  }
}

int cmp_rating (const void * _a, const void * _b) {
  int a = *(int *)_a, b = *(int *)_b;
  if (ratings[a] > ratings[b]) {
    return -1;
  }
  return ratings[a] < ratings[b];
}

int save_index (void) {
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (log_cur_pos() == jump_log_pos) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
       newidxname, jump_log_pos);
    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "creating index %s at log position %lld\n", newidxname, log_cur_pos());
  }

  fd[1] = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
  if (fd[1] < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  header.magic = ISEARCH_INDEX_MAGIC;
  header.created_at = time (NULL);

  long long fCurr = get_index_header_size (&header);
  assert (lseek (fd[1], fCurr, SEEK_SET) == fCurr);

  int len;
  len = save_black_list();
  fCurr += len;
  header.black_list_size = len;

#ifdef FADING
  rating mult = expf (((rating)(ratingT - header.created_at)) / RATING_NORM);
  int j;
  for (j = 1; j <= idn; j++) {
    ratings[j] *= mult;
  }
#endif

  hmap_ll_int_free (&h_id);
  int tidn = idn, i;
  idn = 0;
  namesn = 0;
  for (i = 1; i <= tidn; i++) {
    static int v[MAX_S_LEN];
    char *ss = prepare_str (names_buff + names[i], 0);
    good_string_to_utf8 ((unsigned char *)ss, v);
    if (ratings[i] > RATING_MIN) {
      rating t = ratings[i];
      int id = add_id (ss, v, 0);
      ratings[id] = t;
    }
  }

  dl_free (suggs, sizeof (data) * (prefr));
  hmap_ll_int_free (&h_pref);
  prefn = prefr = 0;
  suggs = NULL;

  trie_arr_node *ptmp = aho_black_list;
  aho_black_list = (trie_arr_node *)buff;

  if (idn) {
    int tidn = idn;
    rating *trate = dl_malloc (idn * sizeof (rating));
    memcpy (trate, ratings + 1, idn * sizeof (rating));

    int k = idn * 0.001 * NOISE_PERCENT;

    rating lowest_rate = qfind (trate, 0, idn - 1, k);
    header.lowest_rate = lowest_rate;

    for (i = 1; i <= idn; i++) {
      if (lowest_rate <= ratings[i]) {
        fix_query (names_buff + names[i]);
      }
    }

    int *sorted = dl_malloc0 (sizeof (int) * (idn + 1));
    int all_cnt = 0, ii, j = 0;

    for (i = 1; i <= idn; i++) {
      if (ratings[i] >= lowest_rate) {
        sorted[all_cnt++] = i;
      }
    }

    qsort (sorted, all_cnt, sizeof (int), cmp_rating);
    for (ii = 0; ii < all_cnt && j < MAX_BEST; ii++) {
      i = sorted[ii];
      int l = strlen (names_buff + names[i]);
      if (l >= 3 && l + 1 < MAX_S_LEN / 4) {
        sorted[j++] = i;
      }
    }
    header.bestn = j;
    assert (write (fd[1], sorted, j * sizeof (int)) == (ssize_t)(j * sizeof (int)));


    if (find_bad_requests) {
      int v[MAX_S_LEN];

//      fprintf (stderr, "prefn = %d\nidn = %d\nnamesn = %d\nwordn = %d\n", prefn, idn, namesn, wordn);

      fprintf (stderr, "\n---- WRONG KEYBOARD LAYOUT BEGIN ----\n\n");

      for (ii = 0; ii < all_cnt; ii++) {
        i = sorted[ii];
        if (!black_list_check (names_buff + names[i])) {
          int l = strlen (names_buff + names[i]);
          if (l >= 3 && l + 1 < MAX_S_LEN / 4) {
            good_string_to_utf8 ((unsigned char *)(names_buff + names[i]), v);

            for (j = 0; v[j] && !bad_letters (v[j]); j++) {
              v[j] = convert_language (v[j]);
            }

            if (!v[j] && j >= 4) {
              int id = get_id (prepare_str_UTF8 (v, 0));
              if (id != 0 && id != i && ratings[id] > lowest_rate) {
                if (!black_list_check (names_buff + names[id])) {
                  fprintf (stderr, "%s %s\n", names_buff + names[i] + 1, names_buff + names[id] + 1);
                }
              }
            }
          }
        }
      }

      fprintf (stderr, "\n---- WRONG KEYBOARD LAYOUT  END  ----\n\n");

      fprintf (stderr,   "----       MISTAKES        BEGIN ----\n\n");

      mark = dl_malloc0 (idn + 1);
      mark[0] = 1;

      for (ii = 0; ii < all_cnt; ii++) {
        i = sorted[ii];
        if (!mark[i]) {
          int l = strlen (names_buff + names[i]);
          if (l >= 3 && l + 1 < MAX_S_LEN / 4) {
            good_string_to_utf8 ((unsigned char *)(names_buff + names[i]), v);

            int *s = prepare_str_UTF8 (v, 0);
            assert (s != NULL);
            cur_id = get_id (s);
            mark[cur_id] = 1;

            int j = 0;
            while (s[j]) {
              j++;
            }
            if (j >= 5) {
              mistakes_cnt = 0;
              get_hints_UTF8 (s, update_answer_mistakes);

              if (mistakes_cnt) {
                int w = 0;
                for (j = 0; j < mistakes_cnt; j++) {
                  if (!black_list_check (names_buff + names[mistakes[j]])) {
                    if (stemmed_names[i] != stemmed_names[mistakes[j]]) {
                      w = 1;
                    }
                  }
                }

                if (w) {
                  if (black_list_check (names_buff + names[i])) {
                    fprintf (stderr, "!!!!! ");
                  }
                  fprintf (stderr, "%s ", names_buff + names[i] + 1);

                  w = 0;
                  for (j = 0; j < mistakes_cnt; j++) {
                    if (!black_list_check (names_buff + names[mistakes[j]])) {
                      if (stemmed_names[i] != stemmed_names[mistakes[j]]) {
                        fprintf (stderr, "%c %s", ":;"[w], names_buff + names[mistakes[j]] + 1);
                        w = 1;
                      }
                      mark[mistakes[j]] = 1;
                    }
                  }
                  fprintf (stderr, "\n");
                }
              }
            }
          }
        }
      }

      dl_free (mark, idn + 1);

      fprintf (stderr,   "----       MISTAKES         END  ----\n\n");
    }

    dl_free (sorted, sizeof (int) * (idn + 1));
    if (tidn != idn) {
      fprintf (stderr, "tidn = %d\n", tidn);
      fprintf (stderr, "idn = %d\n", idn);
      assert (tidn == idn);
    }
    dl_free (trate, idn * sizeof (rating));
  } else {
    header.lowest_rate = -2.0;
  }

  aho_black_list = ptmp;

  header.prefn = prefn;
  header.idn = idn;
  header.namesn = namesn;
#ifdef TYPES
  header.wordn = wordn;
  header.maxt = MAXT;
#endif
  header.use_stemmer = 1;
  header.maxq = MAXQ;
#ifdef NOFADING
  header.nofading = 1;
#endif

  if (verbosity > 2) {
    fprintf (stderr, "prefn = %d\nidn = %d\nnamesn = %d\n", prefn, idn, namesn);
#ifdef TYPES
    fprintf (stderr, "word_n = %d\n", wordn);
#endif
  }

  int tsize = h_pref.size < h_id.size  ? h_id.size : h_pref.size;
#ifdef TYPES
  if (tsize < h_word.size) {
    tsize = h_word.size;
  }
#endif
  tsize = sizeof (int) * 2 + tsize * sizeof (h_pref.e[0]);
  char *buff = dl_malloc (tsize);

  //hmap_ll_int h_pref;
  len = hmap_ll_int_encode (&h_pref, buff, tsize);
  assert (write (fd[1], buff, len) == len);
  header.h_pref_size = len;
  fCurr += len;

  //data *suggs;
  if (prefn) { // fail -- this if is important
    len = sizeof (data) * (prefn + 1);
    assert (write (fd[1], suggs, len) == len);
    fCurr += len;
  }

  //hmap_ll_int h_id;
  len = hmap_ll_int_encode (&h_id, buff, tsize);
  assert (write (fd[1], buff, len) == len);
  header.h_id_size = len;
  fCurr += len;

  if (idn) {
    //rating *ratings;
    len = sizeof (rating) * (idn + 1);
    assert (write (fd[1], ratings, len) == len);
    fCurr += len;

    //int *names;
    len = sizeof (int) * (idn + 1);
    assert (write (fd[1], names, len) == len);
    fCurr += len;

    if (header.use_stemmer) {
      //int *stemmed_names;
      len = sizeof (int) * (idn + 1);
      assert (write (fd[1], stemmed_names, len) == len);
      fCurr += len;
    }

    //char *names_buff;
    len = sizeof (char) * (namesn + 1);
    assert (write (fd[1], names_buff, len) == len);
    fCurr += len;
  }

#ifdef TYPES
  //hmap_ll_int h_word;
  len = hmap_ll_int_encode (&h_word, buff, tsize);
  assert (write (fd[1], buff, len) == len);
  header.h_word_size = len;
  fCurr += len;

  if (wordn) {
    //tdata words;
    len = sizeof (tdata) * (wordn + 1);
    assert (write (fd[1], words, len) == len);
    fCurr += len;
  }
#endif

  dl_free (buff, tsize);
  //write header
  header.log_pos1 = log_cur_pos();
  header.log_timestamp = log_last_ts;
  header.log_split_min = log_split_min;
  header.log_split_max = log_split_max;
  header.log_split_mod = log_split_mod;
  relax_log_crc32 (0);
  header.log_pos1_crc32 = ~log_crc32_complement;

  assert (lseek (fd[1], 0, SEEK_SET) == 0);
  assert (write (fd[1], &header, sizeof (header)) == sizeof (header));

  assert (fsync (fd[1]) >= 0);
  assert (close (fd[1]) >= 0);

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);

  return 0;
}


void process_errors (char *errors[ALPH_N], double pr) {
  int i, j;
  for (i = 0; i < ALPH_N; i++) {
    int A = (unsigned char)errors[i][0] % 224;
    for (j = 1; errors[i][j]; j++)
      prob[A][(unsigned char)errors[i][j] % 224] = pr;
  }
}

int init_all (kfs_file_handle_t Index) {
  log_ts_exact_interval = 1;

  hmap_ll_int_init (&h_pref);
  hmap_ll_int_init (&h_id);
#ifdef TYPES
  hmap_ll_int_init (&h_word);
#endif

  int f = load_index (Index);
  ratingT = header.created_at;
  use_stemmer = header.use_stemmer;

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  int i;
  qr = STAT_ST;
  q_entry = dl_malloc0 (sizeof (q_info) * (STAT_ST + qr));
  q_rev = dl_malloc0 (sizeof (int) * qr);
  for (i = 1; i < STAT_ST; i++) {
    q_entry[i].prev_bucket = i;
    q_entry[i].next_bucket = i;
  }
  q_entry[0].next_used = 0;
  q_entry[0].prev_used = 0;

  hp[0] = 1;
  for (i = 1; i < MAX_S_LEN; i++) {
    hp[i] = hp[i - 1] * H_MUL;
  }

#ifdef SLOW
  for (i = 0; i < 26; i++) {
    alph[alph_n++] = i + 'a';
  }

  for (i = 1072; i <= 1103; i++) {
    alph[alph_n++] = i;
  }
#else
  alph[alph_n++] = '?';
#endif

  process_errors (short_distance_errors, 0.5);
  process_errors (common_spelling_errors, 1.0);

  int j;
  for (i = 0; i < 128; i++) {
    for (j = 0; j < 128; j++) {
      prob[i][j] = (1.0 + prob[i][j]) * 0.5;
    }
  }

  stem_init();

  if (f) {
    try_init_local_uid();
  }
  return 1;
}

void free_all (void) {
  if (verbosity) {
#ifdef TYPES
    dl_free (words, wordr * sizeof (tdata));
    hmap_ll_int_free (&h_word);
#endif

    dl_free (suggs, prefr * sizeof (data));
    dl_free (ratings, idr * sizeof (rating));
    dl_free (names, idr * sizeof (int));
    dl_free (stemmed_names, idr * sizeof (int));
    dl_free (names_buff, namesr * sizeof (char));
    dl_free (q_entry, (qr + STAT_ST) * sizeof (q_info));
    dl_free (q_rev, qr * sizeof (int));

    hmap_ll_int_free (&h_pref);
    hmap_ll_int_free (&h_id);
    hmap_ll_int_free (&h_q);

    dl_free (aho_black_list, aho_black_list_size);

    fprintf (stderr, "Memory left: %lld\n", dl_get_memory_used());
    assert (dl_get_memory_used() == 0);
  }
}

