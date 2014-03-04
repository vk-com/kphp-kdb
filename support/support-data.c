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

    Copyright 2011-2012 Vkontakte Ltd
              2011-2012 Arseny Smirnov
              2011-2012 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64
#define _XOPEN_SOURCE 500

#include <ctype.h>
#include <math.h>
#include <stdio.h>
#include <stddef.h>
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


#include "crc32.h"
#include "stemmer-new.h"

#include "kdb-support-binlog.h"
#include "support-data.h"

#include "dl-utils.h"
#include "dl.def"

int index_mode;
long max_memory = MAX_MEMORY;
int header_size;
int binlog_readed;

int jump_log_ts;
long long jump_log_pos;
unsigned int jump_log_crc32;

#define DEBUG_BUFF_SIZE (1 << 24)

char debug_buff[DEBUG_BUFF_SIZE];
char *ds;
int debug_error;

void debug (char const *msg, ...) {
  va_list args;

  va_start (args, msg);
  int left = DEBUG_BUFF_SIZE - 1 - (ds - debug_buff);
  int wr = vsnprintf (ds, left, msg, args);

  if (wr < left) {
    ds += wr;
  } else {
    ds += left;
    debug_error = 1;
  }
}
	
void debug_init (void) {
  ds = debug_buff;
  debug_error = 0;
  debug_buff[0] = 0;
}
	
int support_replay_logevent (struct lev_generic *E, int size);


char *known_key_words[] = {
  "test"
};

/*
 *
 *        MAIN
 *
 */


char buf[MAX_LEN];

typedef struct answers_listx answers_list;
typedef answers_list *answers_list_ptr;

struct answers_listx {
  struct lev_support_add_answer *E;
  int *q_hashes;
  int q_hashes_len;
  answers_list_ptr prev;
  answers_list_ptr next;
};

answers_list_ptr answers;
int answers_cnt;

set_int key_words;
map_int_int q_cnt;
map_int_vptr user_id_to_answer;
map_int_set_int word_to_answers;

int is_subdomain_symbol (int c) {
  switch (c) {
    case 'a' ... 'z':
    case 'A' ... 'Z':
    case '0' ... '9':
    case '_':
    case '.':
      return 1;
    default:
      return 0;
  }
}

int is_domain_symbol (int c) {
  switch (c) {
    case 'a' ... 'z':
    case 'A' ... 'Z':
    case '0' ... '9':
    case '-':
    case '~':
    case '_':
    case '.':
      return 1;
    default:
      return 0;
  }
}

void delete_links (char *v) {
  static int pv[MAX_LEN];
  static int f[MAX_LEN];

  int i, j;

  for (i = j = 0; v[i]; i++) {
    f[j] = i;
    if (v[i + 1] == '#' && (v[i] == '&' || v[i] == '$')) {
      int r = 0, ti = i;
      if (v[i + 2] != 'x') {
        for (i += 2; v[i] != ';' && v[i]; i++) {
          if ('0' <= v[i] && v[i] <= '9') {
            r = r * 10 + v[i] - '0';
          } else {
            break;
          }
        }
      } else {
        for (i += 3; v[i] != ';' && v[i]; i++) {
          if (('0' <= v[i] && v[i] <= '9') ||
              ('a' <= v[i] && v[i] <= 'f') ||
              ('A' <= v[i] && v[i] <= 'F')) {
            r = r * 16;
            if (v[i] <= '9') {
              r += v[i] - '0';
            } else if (v[i] <= 'F') {
              r += v[i] - 'A' + 10;
            } else {
              r += v[i] - 'a' + 10;
            }
          } else {
            break;
          }
        }
      }
      if (r == 0) {
        pv[j++] = v[i = ti];
      } else {
        pv[j++] = r;
        if (v[i] != ';') {
          i--;
        }
      }
    } else if (v[i] == '%' && '0' <= v[i + 1] && v[i + 1] <= '7' &&
                            (('0' <= v[i + 2] && v[i + 2] <= '9') ||
                             ('a' <= v[i + 2] && v[i + 2] <= 'f') ||
                             ('A' <= v[i + 2] && v[i + 2] <= 'F'))) {
      int r = (v[i + 1] - '0') * 16;
      if (v[i + 2] <= '9') {
        r += v[i + 2] - '0';
      } else if (v[i + 2] <= 'F') {
        r += v[i + 2] - 'A' + 10;
      } else {
        r += v[i + 2] - 'a' + 10;
      }
      i += 2;
      pv[j++] = r;
    } else {
      pv[j++] = v[i];
    }
  }
  f[j] = i;
  pv[j] = 0;

  for (i = 0; i < j; i++) {
    if ('A' <= pv[i] && pv[i] <= 'Z') {
      pv[i] = pv[i] - 'A' + 'a';
    }
  }

  int n;
  for (i = 0; pv[i]; i++) {
    if (pv[i] == 'h' && pv[i + 1] == 't' && pv[i + 2] == 't' && pv[i + 3] == 'p') {
      int add = 0;
      if (pv[i + 4] == 's') {
        add = 1;
      }
      if (pv[i + 4 + add] == ':' && pv[i + 5 + add] == '/' && pv[i + 6 + add] == '/') {
        for (j = i + 7 + add; is_domain_symbol (pv[j]); j++) ;
        if (j == i + 7 + add) {
          continue;
        }
        if (pv[j] == ':') {
          j++;
          while ('0' <= pv[j] && pv[j] <= '9') {
            j++;
          }
        }

        if (pv[j] == '/') {
          while (is_domain_symbol (pv[j]) || pv[j] == '/') {
            j++;
          }

          if (pv[j] == '?') {
            j++;
            while (is_domain_symbol (pv[j]) || pv[j] == '=' || pv[j] == '&' || pv[j] == '+') {
              j++;
            }
          }

          if (pv[j] == '#') {
            while (is_domain_symbol (pv[j]) || pv[j] == '#' || pv[j] == '/') {
              j++;
            }
          }
        }

        for (n = f[j] - 1; n >= f[i]; n--) {
          v[n] = ' ';
        }
      }
    }
  }

  for (i = 0; pv[i]; i++) {
    if (pv[i] == '@') {
      for (j = i - 1; j >= 0 && is_domain_symbol (pv[j]); j--) ;
      for (n = i + 1; is_domain_symbol (pv[n]); n++) ;

      if (j < i - 1 && n > i + 1) {
        for (n = f[n] - 1; n >= f[j + 1]; n--) {
          v[n] = ' ';
        }
      }
    }
  }
}

inline int is_letter (char x) {
  return ('a' <= x && x <= 'z') || ('A' <= x && x <= 'Z') || ('а' <= x && x <= 'я') || ('А' <= x && x <= 'Я') || ('ё' == x) || ('Ё' == x);
}

inline int is_russian_letter (char x) {
  return ('а' <= x && x <= 'я') || ('А' <= x && x <= 'Я') || ('ё' == x) || ('Ё' == x);
}

inline int is_english_letter (char x) {
  x |= 0x20;
  return 'a' <= x && x <= 'z';
}

void delete_html_entities (char *v) {
  int i;

  for (i = 0; v[i]; i++) {
    int ti = i;
    if (v[i + 1] == '#' && (v[i] == '&' || v[i] == '$')) {
      if (v[i + 2] != 'x') {
        for (i += 2; '0' <= v[i] && v[i] <= '9'; i++) {
        }
      } else {
        for (i += 3; ('0' <= v[i] && v[i] <= '9') || ('a' <= v[i] && v[i] <= 'f') || ('A' <= v[i] && v[i] <= 'F'); i++) {
        }
      }
      while (ti != i) {
        v[ti++] = ' ';
      }

      if (v[i] != ';') {
        i--;
      }
    } else if (v[i] == '%' && '0' <= v[i + 1] && v[i + 1] <= '7' &&
                            (('0' <= v[i + 2] && v[i + 2] <= '9') ||
                             ('a' <= v[i + 2] && v[i + 2] <= 'f') ||
                             ('A' <= v[i + 2] && v[i + 2] <= 'F'))) {
      i += 2;

      while (ti != i) {
        v[ti++] = ' ';
      }
    }
  }

#define check(s, l)                         \
  if (!strncmp (v + i + 2, s + 2, l - 2)) { \
    int end = i + l;                        \
    while (i < end) {                       \
      v[i++] = ' ';                         \
    }                                       \
    i--;                                    \
  }

  for (i = 0; v[i]; i++) {
    if (v[i] == '&' && v[i + 1] == '#') {
      check ("&#amp;", 6);
      check ("&#gt;", 5);
      check ("&#lt;", 5);
      check ("&#quot;", 7);
      check ("&#33;", 5);
      check ("&#34;", 5);
      check ("&#36;", 5);
      check ("&#39;", 5);
      check ("&#60;", 5);
      check ("&#62;", 5);
      check ("&#8232;", 7);
      check ("&#8233;", 7);
    }
  }

#undef check
}

int *split (char *s, int *ans_n_return, int debug) {
  static int ans[MAX_LEN];
  int ans_n = 0;

  int i, j = 0;
  delete_links (s);
  delete_html_entities (s);

  for (i = 0; s[i]; ) {
    if (i >= 2 && s[i] == ' ' && (s[i - 1] == 'е' || s[i - 1] == 'Е') && (s[i - 2] == 'н' || s[i - 2] == 'Н') && (i == 2 || !is_letter(s[i - 3]))) { //не с глаголами пишется слитно
      while (s[i] == ' ') {
        i++;
      }
    }

    if (!is_letter (s[i]) && (s[i] < '0' || s[i] > '9')) { //leave only letters and digits
      s[i] = ' ';
    }
    if (!j || s[j - 1] != s[i]) { //и кто только придумал удвоенные буквы? И заодно много пробелов
      s[j++] = s[i];
    }
    i++;
  }
  s[j] = 0;

  for (i = 0; s[i]; i++) { //сonverting 'c' to 'с' and 'ё' to 'е'
    if (s[i] == 'c' && (i == 0 || s[i - 1] == ' ' || is_russian_letter (s[i - 1])) && (s[i + 1] == 0 || s[i + 1] == ' ' || is_russian_letter (s[i + 1]))) {
      s[i] = 'с';
    }
    if (s[i] == 'с' && (i == 0 || s[i - 1] == ' ' || is_english_letter (s[i - 1])) && (s[i + 1] == 0 || s[i + 1] == ' ' || is_english_letter (s[i + 1]))) {
      s[i] = 'c';
    }
    if (s[i] == 'ё' || s[i] == 'Ё') {
      s[i] = 'е';
    }
  }

  char *ptr = s;
  static char buf[MAX_LEN];

  while (*ptr) {
    int len = get_word (ptr);
    assert (len >= 0);

    if (len > 0) {
      int l = my_lc_str (buf, ptr, len);
      buf[l] = 0;

      if (len >= 3) {
        ans[ans_n] = compute_crc32 (buf, l);
//        fprintf (stderr, "%s\n", buf);
        if (!ans[ans_n]) {
          ans[ans_n] = 1;
        }
        ans_n++;

        if (debug) {
          fprintf (stderr, "%10d %s\n", ans[ans_n - 1], buf);
        }
      }
    }
    ptr += len;

    len = get_notword (ptr);
    if (len < 0) {
      break;
    }
    ptr += len;
  }

  if (debug) {
    fprintf (stderr, "ans_n = %d\n", ans_n);
  }
  dl_qsort_int (ans, ans_n);
  *ans_n_return = dl_unique_int (ans, ans_n);
  ans[*ans_n_return] = 0;

  return ans;
}


double word_weight (int word) {
  int *t = map_int_int_get (&q_cnt, word), cnt = 0;

  if (t != NULL) {
    cnt = *t;
  }

  if (cnt <= 2) {
    return 0.0;
  }

  if (set_int_get (&key_words, word) != NULL) {
    return 0.1;
  }

  return 1.0 / sqrt (cnt + 100);
}

double similarity (int *x, int *y, double *a, double *b) {
  *a = 0, *b = 0;

  int xi = 0, yi = 0;
  while (x[xi] || y[yi]) {
    if (x[xi] == y[yi]) {
      *a += 2.0 * word_weight (x[xi]);
      xi++;
      yi++;
    } else {
      if (x[xi] && (y[yi] == 0 || x[xi] < y[yi])) {
        *b += word_weight (x[xi]);
        xi++;
      } else {
        *b += word_weight (y[yi]);
        yi++;
      }
    }
  }

  if (*a + *b < 1e-9) {
    return -1.0;
  }
  return (*a - *b) * 1.0 / (*a + *b);
}

char *get_answer_from_qa (char *s) {
  while (*s != '\t') {
    ++s;
  }
  return ++s;
}

void debug_hashes (int *v) {
  while (*v) {
    fprintf (stderr, "%d ", *v++);
  }
  fprintf (stderr, "\n");
}


answers_list_ptr get_universal_answer (int russian) {
  static struct answers_listx answer_rus = {
    .E = (struct lev_support_add_answer *)("Пустой вопрос\tНе могли бы Вы описать Вашу проблему более подробно?" - offsetof (struct lev_support_add_answer, question_with_answer)),
    .next = NULL
  };

  static struct answers_listx answer_eng = {
    .E = (struct lev_support_add_answer *)("Empty question\tCould you explain your problem more concretely?" - offsetof (struct lev_support_add_answer, question_with_answer)),
    .next = NULL
  };

  return russian ? &answer_rus : &answer_eng;
}

set_int interested;

void add_to_interested (int *user_id) {
  set_int_add (&interested, *user_id);
}

char *get_answer (int user_id, int agent_id, int len, char *question, int cnt, int with_question) {
  if (cnt > MAX_RES) {
    cnt = MAX_RES;
  }

  if (verbosity > 0) {
    fprintf (stderr, "Question of len %d: \"%s\"\n", len, question);
  }

  debug_init();

  memcpy (buf, question, len + 1);

  int vs_n;
  int *vs = split (buf, &vs_n, with_question && (verbosity > 0));

  if (strchr (question, '?') == NULL && vs_n <= 1) {
    vs[vs_n = 0] = 0;
  }

  set_int_init (&interested);

  int i;
  for (i = 0; vs[i]; i++) {
    set_int *s = map_int_set_int_get (&word_to_answers, vs[i]);
    if (s != NULL && (set_int_used (s) < 10000 || set_int_get (&key_words, vs[i]) != NULL)) {
      set_int_foreach (s, add_to_interested);
    } else if (with_question && s != NULL && verbosity > 0) {
      fprintf (stderr, "Word %11d has skipped: %d\n", vs[i], set_int_used (s));
    }
  }
  if (set_int_used (&interested) == 0) {
    for (i = 0; vs[i]; i++) {
      set_int *s = map_int_set_int_get (&word_to_answers, vs[i]);
      if (s != NULL) {
        set_int_foreach (s, add_to_interested);
      }
    }
  }

  static answers_list_ptr besti[MAX_RES + 1];
  static double bestd[MAX_RES + 1];
  int bestn = 0;

  int j;
  double a, b, d;

  static int user_ids[MAX_ANSWERS + 1];
  int n = set_int_values (&interested, user_ids, MAX_ANSWERS);
  assert (n == set_int_used (&interested));
  if (n > MAX_ANSWERS) {
    n = MAX_ANSWERS;
  }
  if (with_question && verbosity > 0) {
    fprintf (stderr, "----------  %d questions left from %d from %d words  ----------\n", n, map_int_vptr_used (&user_id_to_answer), i);
  }

  while (n-- > 0) {
    void **cur_ = map_int_vptr_get (&user_id_to_answer, user_ids[n]);
    assert (cur_ != NULL);
    answers_list_ptr cur = *cur_;
    assert (cur->q_hashes);
    d = similarity (vs, cur->q_hashes, &a, &b) + cur->E->mark * 0.2;

    bestd[bestn] = d;
    besti[bestn] = cur;

    for (j = bestn; j > 0 && (bestd[j] > bestd[j - 1] + 1e-9 || (bestd[j] > bestd[j - 1] - 1e-9 && besti[j]->E->user_id > besti[j - 1]->E->user_id && rand() % 3)); j--) {
      dl_swap (bestd[j], bestd[j - 1]);
      dl_swap (besti[j], besti[j - 1]);
    }

    if (bestn < cnt) {
      bestn++;
    }
  }

  if ((bestn == 0 && strchr (question, '?') != NULL) || (bestn > 0 && bestd[0] < -0.5)) {
    bestn = 1;
    bestd[0] = -0.239;

    for (i = 0; question[i] && !is_russian_letter (question[i]); i++) {
    }

    besti[0] = get_universal_answer (question[i]);
  }

  debug ("a:%d:{", bestn);
  for (j = 0; j < bestn; j++) {
    char *cur_ans = get_answer_from_qa (besti[j]->E->question_with_answer);
    if (with_question) {
      cur_ans = besti[j]->E->question_with_answer;
    }

    debug("i:%d;a:2:{i:0;d:%.3lf;i:1;s:%d:\"%s\";}", j, bestd[j] >= 1.0 ? 1.0 : bestd[j] < -1.0 ? -1.0 : bestd[j], strlen (cur_ans), cur_ans);

    if (with_question && verbosity > 0 && besti[j]->next != NULL) {
      d = similarity (vs, besti[j]->q_hashes, &a, &b);
      fprintf (stderr, "\nQ: (%d) %s\n\nScore: %.3lf (a = %.3lf, b = %.3lf)\n\nA: %s\n", besti[j]->E->user_id, besti[j]->E->question_with_answer, d, a, b, cur_ans);
    }
  }
  debug ("}");

  set_int_free (&interested);
  return debug_buff;
}


int get_lev_support_add_answer_size (struct lev_support_add_answer *E) {
  return E->type - LEV_SUPPORT_ADD_ANSWER + offsetof (struct lev_support_add_answer, question_with_answer);
}

void process_answer (answers_list_ptr answer, int add) {
  struct lev_support_add_answer *E = answer->E;
  int user_id = E->user_id;

  assert (add == -1 || add == 1);

  if (add == 1) {
    *map_int_vptr_add (&user_id_to_answer, user_id) = answer;
  } else {
    map_int_vptr_del (&user_id_to_answer, user_id);
  }

  answers_cnt += add;

  int cnt = add * (2 * E->mark + 1);
  if (E->mark < 0) {
    cnt = add;
  }
  cnt = add;

  if (add == 1) {
    char *a = E->question_with_answer;
    while (*a != '\t') {
      a++;
    }
    memcpy (buf, E->question_with_answer, a - E->question_with_answer);
    buf[a - E->question_with_answer] = 0;

    int *vq = split (buf, &answer->q_hashes_len, 0);
    int size = sizeof (int) * (answer->q_hashes_len + 1);
    answer->q_hashes = dl_malloc (size);
    memcpy (answer->q_hashes, vq, size);
  }

  int *vq = answer->q_hashes;

  int i;
  for (i = 0; vq[i]; i++) {
    int *t = map_int_int_add (&q_cnt, vq[i]);
    set_int *s = map_int_set_int_add (&word_to_answers, vq[i]);
    if (*t == 0) {
      set_int_init (s);
    }

    if (add == -1) {
      int old_used = set_int_used (s);
      set_int_del (s, user_id);
      assert (set_int_used (s) == old_used - 1 && old_used > 0);
    } else {
      int old_used = set_int_used (s);
      set_int_add (s, user_id);
      assert (set_int_used (s) == old_used + 1);
    }

    *t += cnt;
    assert (*t >= 0);

    if (*t == 0) {
      assert (set_int_used (s) == 0);
      set_int_free (s);
      map_int_set_int_del (&word_to_answers, vq[i]);
    }
    if (*t == 0) {
      map_int_int_del (&q_cnt, vq[i]);
    }
  }

  if (add == -1) {
    int size = sizeof (int) * (answer->q_hashes_len + 1);
    dl_free (answer->q_hashes, size);
  }
}

int change_mark (int user_id, int mark) {
  void **cur_ = map_int_vptr_get (&user_id_to_answer, user_id);
  if (cur_ != NULL) {
    answers_list_ptr cur = *cur_;
    if (cur->E->mark != mark) {
      process_answer (cur, -1);
      cur->E->mark = mark;
      process_answer (cur, 1);
    }
  }

  return 1;
}


int add_answer (struct lev_support_add_answer *E) {
  int len = E->type - LEV_SUPPORT_ADD_ANSWER - 1;
  assert (E->question_with_answer[len] == 0);

  if (verbosity > 1) {
    fprintf (stderr, "Adding answer of length %d: \"%s\"\n", len, E->question_with_answer);
  }

  if (map_int_vptr_get (&user_id_to_answer, E->user_id) != NULL) {
    return change_mark (E->user_id, E->mark);
  }

  char *s = E->question_with_answer;
  int tab_cnt = 0, letter_cnt = 0;
  while (*s) {
    if (*s == '\t') {
      if (letter_cnt == 0) {
        tab_cnt++;
      }

      tab_cnt++;
      letter_cnt = 0;
    }
    letter_cnt += is_letter (*s++);
  }
  assert (s - E->question_with_answer == len);

  if (tab_cnt != 1 || letter_cnt == 0) {
    return 0;
  }

  if (E->agent_id < 0) {
    return 1;
  }

  if (answers_cnt >= MAX_ANSWERS) {
    assert (answers->next != answers);
    process_answer (answers->next, -1);
    dl_free (answers->next->E, get_lev_support_add_answer_size (answers->next->E));

    answers->prev->next = answers->next;
    answers->next->prev = answers->prev;
    answers->prev = answers->next;
    answers->next = answers->next->next;
    answers->next->prev = answers;
    answers->prev->next = answers;
  } else {
    answers->prev->next = dl_malloc (sizeof (answers_list));
    answers->prev->next->prev = answers->prev;
    answers->prev = answers->prev->next;
    answers->prev->next = answers;
  }

  int size = get_lev_support_add_answer_size (E);
  answers->prev->E = dl_malloc (size);
  memcpy (answers->prev->E, E, size);

  process_answer (answers->prev, 1);

  return 1;
}

int set_mark (struct lev_support_set_mark *E) {
  if (verbosity > 1) {
    fprintf (stderr, "Setting mark for question %d to %d\n", E->user_id, E->mark);
  }

  return change_mark (E->user_id, E->mark);
}

int delete_answer (struct lev_support_delete_answer *E) {
  if (verbosity > 1) {
    fprintf (stderr, "Deleting answer for question %d\n", E->user_id);
  }

  void **cur_ = map_int_vptr_get (&user_id_to_answer, E->user_id);
  if (cur_ != NULL) {
    answers_list_ptr cur = *cur_;
    cur->next->prev = cur->prev;
    cur->prev->next = cur->next;
    fprintf (stderr, "%d %d %d\n", cur->prev->E->user_id, cur->E->user_id, cur->next->E->user_id);
    process_answer (cur, -1);
    dl_free (cur->E, get_lev_support_add_answer_size (cur->E));
    dl_free (cur, sizeof (answers_list));
  }

  return 1;
}

/*
 *
 *       BINLOG
 *
 */


int init_support_data (int schema) {
  replay_logevent = support_replay_logevent;
  return 0;
}

int do_add_answer (int user_id, int agent_id, int mark, int len, char *question_with_answer) {
  if (len < 3 || len >= MAX_LEN) {
    return 0;
  }

  int size = offsetof (struct lev_support_add_answer, question_with_answer) + len + 1;
  struct lev_support_add_answer *E =
    alloc_log_event (LEV_SUPPORT_ADD_ANSWER + len + 1, size, user_id);

  E->agent_id = agent_id;
  E->mark = mark;
  memcpy (E->question_with_answer, question_with_answer, len);
  E->question_with_answer[len] = 0;

  return add_answer (E);
}

int do_set_mark (int user_id, int mark) {
  struct lev_support_set_mark *E =
    alloc_log_event (LEV_SUPPORT_SET_MARK, sizeof (struct lev_support_set_mark), user_id);

  E->mark = mark;

  return set_mark (E);
}

int do_delete_answer (int user_id) {
  struct lev_support_delete_answer *E =
    alloc_log_event (LEV_SUPPORT_DELETE_ANSWER, sizeof (struct lev_support_delete_answer), user_id);

  return delete_answer (E);
}



void try_init_local_uid (void) {
  static int was = 0;
  static int old_log_split_min, old_log_split_max, old_log_split_mod;

  if (was) {
//    fprintf (stderr, "%d vs %d | %d vs %d | %d vs %d\n", old_log_split_min, log_split_min, old_log_split_max, log_split_max, old_log_split_mod, log_split_mod);
    assert (old_log_split_min == log_split_min && old_log_split_max == log_split_max && old_log_split_mod == log_split_mod);
    return;
  } else {
    was = 1;
    old_log_split_min = log_split_min;
    old_log_split_max = log_split_max;
    old_log_split_mod = log_split_mod;
  }

  log_schema = SUPPORT_SCHEMA_V1;
  init_support_data (log_schema);
}

static int support_le_start (struct lev_start *E) {
  if (E->schema_id != SUPPORT_SCHEMA_V1) {
    return -1;
  }

  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

  try_init_local_uid();

  return 0;
}

int _eventsLeft = 0;

int support_replay_logevent (struct lev_generic *E, int size) {
  if (index_mode) {
    if (((_eventsLeft && --_eventsLeft == 0) || dl_get_memory_used() > max_memory) && !binlog_readed) {
      binlog_readed = 1;
      save_index();
      exit (13);
    }
  }

  int s;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return support_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_SUPPORT_ADD_ANSWER + 3 ... LEV_SUPPORT_ADD_ANSWER + MAX_LEN:
    if (size < (int)sizeof (struct lev_support_add_answer)) {
      return -2;
    }
    s = get_lev_support_add_answer_size ((struct lev_support_add_answer *)E);
    if (size < s) {
      return -2;
    }
    add_answer ((struct lev_support_add_answer *)E);
    return s;
  case LEV_SUPPORT_SET_MARK:
    STANDARD_LOG_EVENT_HANDLER(lev_support, set_mark);
  case LEV_SUPPORT_DELETE_ANSWER:
    STANDARD_LOG_EVENT_HANDLER(lev_support, delete_answer);
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


/*
 *
 *        INDEX
 *
 */


#pragma pack(push, 4)

typedef struct {
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  unsigned long long slice_hash;
  int log_timestamp;
  int log_split_min, log_split_max, log_split_mod;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;

  int reserved[48];
} index_header;

#pragma pack(pop)

index_header header;

long long get_index_header_size (index_header *header) {
  return sizeof (index_header);
}

int load_header (kfs_file_handle_t Index) {
  if (Index == NULL) {
    fd[0] = -1;

    header.log_pos0 = 0;
    header.log_pos1 = 0;
    header.log_timestamp = 0;
    header.log_split_min = 0;
    header.log_split_max = 1;
    header.log_split_mod = 1;
    header.log_pos0_crc32 = 0;
    header.log_pos1_crc32 = 0;

    header.created_at = time (NULL);
    header_size = sizeof (index_header);

    return 0;
  }

  dbg ("load_header\n");

  fd[0] = Index->fd;
  int offset = Index->offset;

  //read header
  assert (lseek (fd[0], offset, SEEK_SET) == offset);

  int size = sizeof (index_header);
  int r = read (fd[0], &header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %d: %m\n", r, size, offset);
    assert (r == size);
  }

  if (header.magic != SUPPORT_INDEX_MAGIC) {
    fprintf (stderr, "bad support index file header\n");
    fprintf (stderr, "magic = 0x%08x // offset = %d\n", header.magic, offset);
    assert (0);
  }

  assert (header.log_split_max);
  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;

  header_size = get_index_header_size (&header);

  if (verbosity > 1) {
    fprintf (stderr, "header loaded %d %d %d %d\n", fd[0], log_split_min, log_split_max, log_split_mod);
    fprintf (stderr, "ok\n");
  }

  return 1;
}

int save_index (void) {
  char *newidxname = NULL;

  dbg ("save_index\n");
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

  if (!binlog_disabled) {
    relax_write_log_crc32();
  } else {
    relax_log_crc32 (0);
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  fd[1] = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
  if (fd[1] < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  header.magic = SUPPORT_INDEX_MAGIC;
  header.created_at = time (NULL);

  long long fCurr = get_index_header_size (&header);
  assert (lseek (fd[1], fCurr, SEEK_SET) == fCurr);

  // write header
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
  fd[1] = -1;

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);
  return 0;
}

int init_all (kfs_file_handle_t Index) {
  if (verbosity > 1) {
    fprintf (stderr, "Init_all started\n");
  }

  int f = load_header (Index);

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  if (f) {
    try_init_local_uid();
  }

  answers = dl_malloc (sizeof (answers_list));

  answers->next = answers;
  answers->prev = answers;

  use_stemmer = 1;
  init_is_letter();
  enable_is_letter_sigils();
  stem_init();

  set_int_init (&key_words);
  map_int_int_init (&q_cnt);
  map_int_vptr_init (&user_id_to_answer);
  map_int_set_int_init (&word_to_answers);

  char s[101];
  int key_words_n = sizeof (known_key_words) / sizeof (char *);
  while (key_words_n-- > 0) {
    int res, l = strlen (known_key_words[key_words_n]);
    assert (l < 100);
    memcpy (s, known_key_words[key_words_n], l + 1);

    int *v = split (s, &res, 0);
    assert (res == 1);

    set_int_add (&key_words, v[0]);
  }

  if (verbosity > 1) {
    fprintf (stderr, "Init_all finished\n");
  }

  return f;
}

void free_all (void) {
  if (verbosity) {
    while (answers_cnt > 0) {
      assert (answers->next != answers);
      process_answer (answers->next, -1);

      answers_list_ptr next = answers->next;
      dl_free (next->E, get_lev_support_add_answer_size (next->E));
      answers->next = next->next;
      answers->next->prev = answers;

      dl_free (next, sizeof (answers_list));
    }
    assert (answers->next == answers);

    assert (map_int_int_used (&q_cnt) == 0);
    assert (map_int_vptr_used (&user_id_to_answer) == 0);
    assert (map_int_set_int_used (&word_to_answers) == 0);

    set_int_free (&key_words);
    map_int_int_free (&q_cnt);
    map_int_vptr_free (&user_id_to_answer);
    map_int_set_int_free (&word_to_answers);

    dl_free (answers, sizeof (answers_list));

    fprintf (stderr, "Memory left: %ld\n", (long)dl_get_memory_used());
    assert (dl_get_memory_used() == 0);
  }
}
