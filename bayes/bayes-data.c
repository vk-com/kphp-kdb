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

#include <stddef.h>
#include <stdarg.h>

#include "bayes-data.h"

#define DEBUG_BUFF_SIZE 100000

char debug_buff[DEBUG_BUFF_SIZE];
int debug;

int debug_on;
char *ds;

inline void debugp (char const *msg, ...) {
  if (debug_on) {
    va_list args;

    va_start (args, msg);
    int left = DEBUG_BUFF_SIZE - 1 - (ds - debug_buff);
    int wr = vsnprintf (ds, left, msg, args);

    if (wr < left) {
      ds += wr;
    } else {
      ds += left;
    }

    va_end (args);
  }
}

int index_mode;
long max_memory = MAX_MEMORY;
int cur_users;
int index_users, header_size;
long long teach_messages;
int binlog_readed;

int jump_log_ts;
long long jump_log_pos;
unsigned jump_log_crc32;
long long allocated_metafile_bytes;

int fd[2];

char EMPTY__METAFILE[3 * sizeof (int)] = {0};

long long del_by_LRU;

void bind_user_metafile (user *u);
void unbind_user_metafile (user *u);

index_header header;
user *users, *LRU_head;
lookup_table user_table;
void load_user_metafile (user *u, int local_id, int no_aio);

int max_words = 40000000;

int user_loaded (user *u) {
  return u->metafile_len >= 0 && u->aio == NULL;
}

int local_uid (int user_id) {
  if (user_id <= 0) {
    return -1;
  }
  if (user_id % log_split_mod != log_split_min) {
    return -1;
  }

  return ltbl_add (&user_table, user_id);
}

user *conv_uid (int user_id) {
  int local_id = local_uid (user_id);
  if (local_id == -1) {
    return NULL;
  }

  assert (local_id < user_table.to.size);
  if (local_id + 1 == user_table.to.size && index_mode) {
    save_index();
    exit (13);
  }
  assert (local_id + 1 < user_table.to.size);
  return &users[local_id];
}

void del_user_used (user *u) {
  assert (u->prev_used != NULL);
  assert (u->next_used != NULL);

  u->next_used->prev_used = u->prev_used;
  u->prev_used->next_used = u->next_used;

  u->prev_used = NULL;
  u->next_used = NULL;
}

void add_user_used (user *u) {
  assert (u != NULL);
  assert (user_loaded (u));

  assert (u->prev_used == NULL);
  assert (u->next_used == NULL);

  user *y = LRU_head->prev_used;

  u->next_used = LRU_head;
  LRU_head->prev_used = u;

  u->prev_used = y;
  y->next_used = u;
}

int unload_user_metafile (user *u);

void user_unload (user *u) {
  assert (u != NULL);

  if (!user_loaded (u)) {
    fprintf (stderr, "%d\n", ltbl_get_rev (&user_table, (int)(u - users)));
    assert (0);
    return;
  }

  del_user_used (u);
  unload_user_metafile (u);
  cur_users--;
  assert (!user_loaded (u));
}

int user_LRU_unload (void) {
  user *u = LRU_head->next_used;
  if (u == LRU_head) {
    return -1;
  }

  del_by_LRU++;
  user_unload (u);
  return 0;
}

long long get_del_by_LRU (void) {
  return del_by_LRU;
}


/*
 *
 *         MESSAGE
 *
 */


void msg_free (message *msg) {
 if (msg->text != NULL) {
    qfree (msg->text, msg->len);
    msg->text = NULL;
  }
  msg->len = 0;
  msg->random_tag = -1;
}

char *msg_get_buf (message *msg) {
  if (msg == NULL) {
    return NULL;
  }

  return msg->text;
}

int msg_reinit (message *msg, int len, int random_tag) {
  msg->text = qrealloc (msg->text, len + 1, msg->len);
  if (msg->text == NULL) {
    msg->len = 0;
    return -1;
  }

  msg->len = len + 1;
  msg->text[len] = 0;
  msg->random_tag = random_tag;

  return 0;
}

int msg_verify (message *msg, int random_tag) {
  if (msg == NULL) {
    return -1;
  }

  if (msg->random_tag != random_tag) {
    //TODO should I really do this?
    msg_free (msg);

    return -1;
  }

  return 0;
}


/*
 *
 *         BLACK LIST
 *
 */

black_list *bl_head;

void black_list_init (black_list *bl) {
  bl->text_len = -1;
  bl->text = NULL;
  bl->next = NULL;
}

void black_list_do_delete (black_list *bl) {
  black_list *next = bl->next;
  bl->next = bl->next->next;
  qfree (next->text, next->text_len + 1);
  black_list_init (next);
  qfree (next, sizeof (black_list));
}

int black_list_add (struct lev_bayes_black_list *E) {
  black_list *cur = bl_head;
  while (cur->next != NULL && (cur->next->text_len != E->text_len || strcmp (cur->next->text, E->text))) {
    cur = cur->next;
  }

  if (cur->next != NULL) {
    if (E->type & 1) {
      black_list_do_delete (cur);
      return 1;
    } else {
      return 0;
    }
  } else {
    if (E->type & 1) {
      return 0;
    } else {
      cur->next = qmalloc (sizeof (black_list));
      black_list_init (cur->next);
      cur->next->text_len = E->text_len;
      cur->next->text = qmalloc (E->text_len + 1);
      memcpy (cur->next->text, E->text, E->text_len + 1);
      return 1;
    }
  }
}

int black_list_get (char *buf, int buf_len) {
  black_list *cur = bl_head;
  int cur_len = 0;
  while (cur->next != NULL && cur->next->text_len + cur_len + 1 < buf_len) {
    memcpy (buf + cur_len, cur->next->text, cur->next->text_len);
    cur_len += cur->next->text_len;
    buf[cur_len++] = '\t';
    cur = cur->next;
  }
  if (cur_len == 0) {
    cur_len = 1;
  }
  buf[cur_len - 1] = 0;

  if (cur->next != NULL) {
    return -1;
  }

  return cur_len - 1;
}

void black_list_set (char *buf, int len) {
  black_list *cur = bl_head;
  int i, j;
  for (i = 0; i < len; i++) {
    for (j = i; j < len && buf[j] != '\t'; j++) ;
    buf[j] = 0;

    assert (cur->next == NULL);
    cur->next = qmalloc (sizeof (black_list));
    black_list_init (cur->next);
    cur->next->text_len = j - i;
    cur->next->text = qmalloc (j - i + 1);
    memcpy (cur->next->text, buf + i, j - i + 1);
    cur = cur->next;

    i = j;
  }
}

/*
 *
 *         BAYES
 *
 */


#define MAX_WORDS (1 << 20)
ll words[MAX_WORDS];
int v[MAX_WORDS], words_len;
double words_prob[MAX_WORDS];

bayes global_bayes;

int cmp_dbl (const void *x, const void *y) {
  return *(double *)x < *(double *)y;
}

int QL, QR;
void my_qsort (double *a, int l, int r) {
  if (l >= r || (l > QL && r < QR)) {
    return;
  }

  int i = l, j = r;
  double t, c = a[rand() % (r - l + 1) + l];

  while (i <= j) {
    while (a[i] < c) {
      i++;
    }
    while (a[j] > c) {
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

  my_qsort (a, l, j);
  my_qsort (a, i, r);
}

void bayes_add_word (bayes *b, ll word, int add_ham, int add_spam) {
  pair *tmp;
  tmp = qhtbl_add (&b->cnt, word);
  tmp->ham += add_ham;
  tmp->spam += add_spam;

  if (tmp->ham > 1000000000 || tmp->spam > 1000000000) {
    tmp->ham /= 2;
    tmp->spam /= 2;
  }

  if (tmp->ham < 0) {
    tmp->ham = 0;
  }

  if (tmp->spam < 0) {
    tmp->spam = 0;
  }

  return;
}

//TODO inline this
int bayes_get_count (bayes *b, ll word, int type) {
  pair *tmp = qhtbl_get (&b->cnt, word);
  if (tmp == NULL)
    return 0;
  if (type == SPAM) {
    return tmp->spam;
  } else {
    return tmp->ham;
  }
}

//TODO inline this
int bayes_local_get_count (char *b, ll word, int type) {
  int n = ((int *)b)[0];
  entry_t *a = (entry_t *)(b + sizeof (int));

  if (n == 0) {
    return 0;
  }

  int l = 0, r = n;
  while (l + 1 < r) {
    int m = l + ((r - l) >> 1);
    if (a[m].h > word) {
      r = m;
    } else {
      l = m;
    }
  }

  if (a[l].h != word)
    return 0;

  if (type == SPAM) {
    return a[l].val.spam;
  } else {
    return a[l].val.ham;
  }
}

int get_type_lang (int c) {
  switch (c) {
    case 'a' ... 'z':
    case 'A' ... 'Z':
    case '0':
      return 0;
    case 0x0410 ... 0x042F:
    case 0x0430 ... 0x044F:
    case 0x0401:
    case 0x0451:
      return 1;
    default:
      return 2;
  }
}

int correct_lang (int *v) {
  switch (*v) {
    case 'a':
      *v = 0x0430;
      return 1;
    case 'A':
      *v = 0x0410;
      return 1;
    case 'e':
      *v = 0x0435;
      return 1;
    case 'E':
      *v = 0x0415;
      return 1;
    case 'i':
      *v = 0x0418;
      return 1;
    case 'I':
      *v = 0x0438;
      return 1;
    default:
      return 0;
  }
}

int is_letter (int x) {
  switch (x) {
    case 'a' ... 'z':
    case 0x0430 ... 0x044F:
    case 0x0451:
    case 'A' ... 'Z':
    case 0x0410 ... 0x042F:
    case 0x0401:
      return 1;
  }
  return 0;
//  return ('a' <= x && x <= 'z') || ('A' <= x && x <= 'Z') || (x >= 128);
}

int get_type_case (int c) {
  switch (c) {
    case 'a' ... 'z':
    case 0x0430 ... 0x044F:
    case 0x0451:
      return 0;
    case 'A' ... 'Z':
    case 0x0410 ... 0x042F:
    case 0x0401:
      return 1;
    default:
      return 2;
  }
}

#define KLUDGE 1023482800

void add_kludge (int type, int num) {
  if (debug) {
    fprintf (stderr, "KLUDGE %d %d\n", type, num);
  }
  debugp ("KLUDGE %d %d | %lld\n", type, num, (long long)(ll)((ll)(KLUDGE + type) * HASH_MUL + num));
  words[words_len++] = (ll)(KLUDGE + type) * HASH_MUL + num;
}

void add_word (int *v) {
//TODO: remove useless put_sting_utf8

  char tmp[10000];
  put_string_utf8 (v, tmp);

  if (debug) {
    fprintf (stderr, "WORD %s\n", tmp);
  }

  ll res = 0;
  while (*v) {
    res = (res * HASH_MUL) + *v++;
  }

  debugp ("WORD %s | %lld\n", tmp, (long long)((ll)res));

  words[words_len++] = res;
}

int cmp_ll (const void *_x, const void *_y) {
  ll x = *(ll *)_x, y = *(ll *)_y;
  if (x < y) {
    return -1;
  } else if (y < x) {
    return 1;                     	
  }
  return 0;
}

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

int delete_links (void) {
//  return 1;

  static int pv[MAX_WORDS];
  static int f[MAX_WORDS];
  static char bad[MAX_WORDS];

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
        bad[j] = 0;
        pv[j++] = v[i = ti];
      } else {
        bad[j] = 1;
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
      if (r != ':' && r != '/' && r != '=' && r != '?' && r != '&' && r != '+') {
        bad[j] = 1;
      } else {
        bad[j] = 0;
      }
      pv[j++] = r;
    } else {
      bad[j] = 0;
      pv[j++] = v[i];
    }
  }
  f[j] = i;
  pv[j] = 0;

  for (i = 0; i < j; i++) {
    if ('A' <= pv[i] && pv[i] <= 'Z') {
      pv[i] = pv[i] - 'A' + 'a';
      bad[i] += 2;
    }
  }

  int rbad = 0, n;
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

        int j0 = j;

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

        while (j > i) {
          rbad |= (bad[--j] & 1);
          if (j < j0) {
            rbad |= (bad[--j] & 2);
          }
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


  if (rbad) {
    return -1;
  }
  return 0;
}


char buf_debug[1000000];

int get_words (char *s) {
  if (verbosity > 2 && binlog_readed) {
    fprintf (stderr, "in get words\n");
  }

  words_len = 0;
  int i;

  if (strlen (s) + 1 > MAX_WORDS) {
    s[MAX_WORDS - 1] = 0;
  }

  for (i = strlen (s); i > 0 && s[i - 1] != '\t'; i--) ;

  bayes_string_to_utf8 ((unsigned char *)(s + i), v);
  s = NULL;

  if (verbosity > 1 && binlog_readed) {
    put_string_utf8 (v, buf_debug);
    fprintf (stderr, "bayes_get_words: %s\n", buf_debug);
  }

  int result = delete_links();

  int cur_mixed_lang = 0;
  for (i = 0; v[i] && v[i + 1] && cur_mixed_lang < 25; i++) {
    if ((get_type_lang (v[i]) + get_type_lang (v[i + 1]) == 1)) {
      if (!correct_lang (&v[i]) && !correct_lang (&v[i + 1])) {
        add_kludge (0, cur_mixed_lang++);
        while (get_type_lang (v[i + 1]) != 2) {
          i++;
        }
      }
    }
  }

  int cur_mixed_case = 0;
  for (i = 0; v[i] && v[i + 1] && cur_mixed_case < 25; i++) {
//    if (get_type_case (v[i]) == 0 && get_type_case (v[i + 1]) == 1 && ((i != 0 && is_letter (v[i - 1])) || is_letter (v[i + 2]))) {
    if (get_type_case (v[i]) == 0 && get_type_case (v[i + 1]) == 1 && get_type_case (v[i + 2]) == 1) {
      add_kludge (1, cur_mixed_case++);
    }
  }

  int cur_len = -words_len;
  int j = 0, t;
  while (v[j]) {
    while (v[j] && !is_letter (v[j])) {
      j++;
    }
    if (v[j]) {
      i = j;
      while (v[i] && is_letter (v[i])) {
        i++;
      }

      if (v[i]) {
        t = i + 1;
        v[i] = 0;
      } else {
        t = i;
      }

      if (i - j > 2) {
        add_word (&v[j]);
      }
      j = t;
    }
  }

  cur_len += words_len;
  if (cur_len >= 11) {
    cur_len = (cur_len - 11)/ 4 + 11;
  }

  if (cur_len > 25) {
    cur_len = 25;
  }

  add_kludge (2, cur_len);

  qsort (words, words_len, sizeof (ll), cmp_ll);
  j = 1;
  for (i = 1; i < words_len; i++)
    if (words[i] != words[i - 1])
      words[j++] = words[i];
  words_len = j;

  words[words_len] = 0;

  if (verbosity > 2 && binlog_readed) {
    fprintf (stderr, "go out get words\n");
  }

  return result;
}

double bayes_is_spam_prob (message *msg, int random_tag) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  if (verbosity > 2) {
    fprintf (stderr, "bayes_is_spam_prob\n%s\n", msg->text);
  }


  black_list *cur = bl_head;
  while (cur->next != NULL && !strstr (msg->text, cur->next->text)) {
    cur = cur->next;
  }

  if (cur->next != NULL) {
    return 1.0;
  }

             	
  long long nbad = global_bayes.nbad, ngood = global_bayes.ngood, all = nbad + ngood;

  if (!nbad || !ngood) {
    return 0.0;
  }

  debug = 0;

  if (debug) {
    debug_on = 1;
    ds = debug_buff;
  }

/*  int out = 0;
  char *out_s = strstr (msg->text, "out ");
  if (out_s != NULL) {
    sscanf (out_s + 5, "%d", &out);
  }

  if (out == 1) {
    return 0.0;
  }*/

  int uid = 0;
  char *uid_s = strstr (msg->text, "uid ");
  if (uid_s != NULL) {
    sscanf (uid_s + 5, "%d", &uid);
  }
  int local_id = local_uid (uid);
  user *u = conv_uid (uid);

  long long nbad_u = 0, ngood_u = 0, all_u = 0;
  double mul_u = 0.0;

  if (u != NULL) {
    load_user_metafile (u, local_id, NOAIO);

    if (!user_loaded (u)) {
      return -2;
    }

    nbad_u = u->b.nbad + ((int *)u->metafile)[SPAM];
    ngood_u = u->b.ngood + ((int *)u->metafile)[HAM];
    all_u = nbad_u + ngood_u;


    if (all_u >= 50 && nbad_u * 1.0 / ngood_u <= 5 * 1.0 * nbad / ngood) {
      mul_u = (double)all / all_u / 10;
    }
  }
//  mul_u = 0;

  if (get_words (msg->text) < 0) {
    return 1.0;
  }

  double p1 = 3.5 * nbad / all, p2 = 1.0 * ngood / all;

  int i;

  if (debug) {
    fprintf (stderr, "mul_u = %.6lf, ngood = %lld, nbad = %lld, ngood_u = %lld, nbad_u = %lld\n", mul_u, ngood, nbad, ngood_u, nbad_u);
  }
  debugp ("mul_u = %.6lf, ngood = %lld, nbad = %lld, ngood_u = %lld, nbad_u = %lld\n", mul_u, ngood, nbad, ngood_u, nbad_u);

  for (i = 0; words[i]; i++) {
    double gcnt = bayes_get_count (&global_bayes, words[i], HAM),
           bcnt = bayes_get_count (&global_bayes, words[i], SPAM),
           total = gcnt + bcnt;

    if (debug) {
      fprintf (stderr, "(%.3lf;%.3lf)%c", gcnt, bcnt, " \n"[!words[i + 1]]);
    }

    if (u != NULL && mul_u > 1e-9) {
      gcnt += mul_u * (ngood_u + 0.0) / (nbad_u + ngood_u + 0) * total / all;
      bcnt += mul_u * (nbad_u + 0.0) / (nbad_u + ngood_u + 0) * total / all;

      gcnt += bayes_get_count (&u->b, words[i], HAM) * mul_u,
      bcnt += bayes_get_count (&u->b, words[i], SPAM) * mul_u,

      gcnt += bayes_local_get_count (u->metafile + 2 * sizeof (int), words[i], HAM) * mul_u;
      bcnt += bayes_local_get_count (u->metafile + 2 * sizeof (int), words[i], SPAM) * mul_u;
    }

    if (debug) {
      fprintf (stderr, "(%.3lf;%.3lf)%c", gcnt, bcnt, " \n"[!words[i + 1]]);
    }


    double g = p1 * (gcnt + (ngood + 1.0) / (nbad + ngood + 1) );
    double b = p2 * (bcnt + (nbad + 1.0) / (nbad + ngood + 1) );

    if (debug) {
      fprintf (stderr, "(%.3lf;%.3lf)%c", g, b, " \n"[!words[i + 1]]);
    }

    words_prob[i] = b / (g + b);

    if (words_prob[i] < 0.01) {
      words_prob[i] = 0.01;
    }

    if (words_prob[i] > 0.99) {
      words_prob[i] = 0.99;
    }

    debugp ("%lld : %.7lf     (%.3lf+%lf;%.3lf+%lf)\n", (long long)words[i], words_prob[i], g, gcnt, b, bcnt);
  }

  int wn = i;

  QL = BAYES_MAX_WORDS - 1;
  QR = wn - BAYES_MAX_WORDS;

  my_qsort (words_prob, 0, wn - 1);

  if (debug) {
    for (i = 0; i < wn; i++) {
      fprintf (stderr, "%.3lf%c", words_prob[i], " \n"[i + 1 == wn]);
    }
  }

  int res_cnt = BAYES_MAX_WORDS;
  int l = 0, r = wn - 1;
  p1 = 1.0, p2 = 1.0;

  while (res_cnt-- > 0 && l <= r) {
    if (1.0 < words_prob[r] + words_prob[l]) {
       p1 *= words_prob[r];
       p2 *= 1 - words_prob[r--];
    } else {
       p1 *= words_prob[l];
       p2 *= 1 - words_prob[l++];
    }
  }

  if (debug) {
    fprintf (stderr, "RES: %.3lf\n", p1 / (p1 + p2));
  }

  if (debug) {
    fprintf (stderr, "%s\n", debug_buff);
  }

  if (debug) {
    debug_on = 0;
    debug = 0;
  }

  return p1 / (p1 + p2);
}

int bayes_is_spam (message *msg, int random_tag) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  if (verbosity > 2) {
    fprintf (stderr, "bayes_is_spam\n%s\n", msg->text);
  }
  debugp ("CHECK: <%s>\n", msg->text);

  double result = bayes_is_spam_prob (msg, random_tag);

  debugp ("RES: %.9lf\n", result);
  debugp ("%s\n", result > BAYES_SPAM_LIMIT ? "spam" : "ham");

  if (result < -1.99999) {
    return -2;
  }

  return result > BAYES_SPAM_LIMIT;
}


int bayes_is_spam_debug (message *msg, int random_tag, char *debug_s) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  debug_on = 1;
  ds = debug_s;

  bayes_is_spam (msg, random_tag);

  *ds = 0;
  debug_on = 0;

  return 0;
}


int bayes_set (char *text, int text_len, int type) {
  if (verbosity > 1 && binlog_readed) {
    fprintf (stderr, "bayes_set %d : %s\n", type, text);
  }

  int i;

  int out = 0;
  char *out_s = strstr (text, "out ");
  if (out_s != NULL) {
    sscanf (out_s + 5, "%d", &out);
  }

  int uid = 0;
  char *uid_s = strstr (text, "uid ");
  if (uid_s != NULL) {
    sscanf (uid_s + 5, "%d", &uid);
  }

  user *u = conv_uid (uid);

  if (!out || u != NULL) {
    teach_messages++;
    get_words (text);
  } else {
    return 0;
  }

  int add_ham = 0, add_spam = 0;
  switch (type) {
    case HAM:
      add_ham = 1;
      break;
    case SPAM:
      add_spam = 1;
      break;
    case HAM + 2:
      add_ham = -1;
      break;
    case SPAM + 2:
      add_spam = -1;
      break;
    case HAM + 4:
      add_ham = 1;
      add_spam = -1;
      break;
    case SPAM + 4:
      add_ham = -1;
      add_spam = 1;
      break;
  }

//  if (u != NULL) {
//    fprintf (stderr, "bayes_set %d %d %d : %s\n", type, add_ham, add_spam, text);
//  }

  if (!out) {
    for (i = 0; words[i]; i++) {
      bayes_add_word (&global_bayes, words[i], add_ham, add_spam);
    }

    global_bayes.ngood += add_ham;
    global_bayes.nbad += add_spam;
  }

  if (u != NULL) {
    for (i = 0; words[i]; i++) {
      bayes_add_word (&u->b, words[i], add_ham, add_spam);
    }

    u->b.ngood += add_ham;
    u->b.nbad += add_spam;
  }
  return 1;
}

int bayes_lev_set (struct lev_bayes_set *E) {
  return bayes_set (E->text, E->text_len, E->type & 15);
}

int ok_cp1251 (char *s, int n) {
  int i;
  for (i = 1; i < n; i++) {
    if (s[i] == (char)0x98) {
      return 0;
    }
  }

  return 1;
}

long long get_words_cnt (void) {
  return global_bayes.cnt.size;
}


/*
 *
 *           BINLOG
 *
 */


int do_bayes_set (char *text, int text_len, int type) {
  if (text_len >= 32768 || text_len == 0) {
    return 0;
  }

  struct lev_bayes_set *E =
    alloc_log_event (LEV_BAYES_SET + type, offsetof (struct lev_bayes_set, text) + 1 + text_len, 0);

  E->text_len = text_len;
  memcpy (E->text, text, sizeof (char) * (text_len + 1));

  bayes_lev_set (E);

  return 1;
}

int do_bayes_set_spam (message *msg, int random_tag) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  if (verbosity > 2) {
    fprintf (stderr, "bayes_set_spam\n%s\n", msg->text);
  }

  return do_bayes_set (msg->text, msg->len - 1, SPAM);
}

int do_bayes_set_ham (message *msg, int random_tag) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  if (verbosity > 2) {
    fprintf (stderr, "bayes_set_ham\n%s\n", msg->text);
  }

  return do_bayes_set (msg->text, msg->len - 1, HAM);
}

int do_bayes_unset_spam (message *msg, int random_tag) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  if (verbosity > 2) {
    fprintf (stderr, "bayes_set_spam\n%s\n", msg->text);
  }

  return do_bayes_set (msg->text, msg->len - 1, SPAM + 2);
}

int do_bayes_unset_ham (message *msg, int random_tag) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  if (verbosity > 2) {
    fprintf (stderr, "bayes_set_ham\n%s\n", msg->text);
  }

  return do_bayes_set (msg->text, msg->len - 1, HAM + 2);
}

int do_bayes_reset_spam (message *msg, int random_tag) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  if (verbosity > 2) {
    fprintf (stderr, "bayes_set_spam\n%s\n", msg->text);
  }

  return do_bayes_set (msg->text, msg->len - 1, SPAM + 4);
}

int do_bayes_reset_ham (message *msg, int random_tag) {
  if (msg_verify (msg, random_tag) < 0) {
    return -1;
  }

  if (verbosity > 2) {
    fprintf (stderr, "bayes_set_ham\n%s\n", msg->text);
  }

  return do_bayes_set (msg->text, msg->len - 1, HAM + 4);
}


int do_black_list_add (const char *s) {
  int text_len = strlen (s);
  if (text_len >= 32768 || text_len == 0) {
    return 0;
  }

  struct lev_bayes_black_list *E =
    alloc_log_event (LEV_BAYES_BLACK_LIST, offsetof (struct lev_bayes_black_list, text) + 1 + text_len, 0);

  E->text_len = text_len;
  memcpy (E->text, s, sizeof (char) * (text_len + 1));

  return black_list_add (E);
}

int do_black_list_delete (const char *s) {
  int text_len = strlen (s);
  if (text_len >= 32768 || text_len == 0) {
    return 0;
  }

  struct lev_bayes_black_list *E =
    alloc_log_event (LEV_BAYES_BLACK_LIST + 1, offsetof (struct lev_bayes_black_list, text) + 1 + text_len, 0);

  E->text_len = text_len;
  memcpy (E->text, s, sizeof (char) * (text_len + 1));

  return black_list_add (E);
}



int bayes_replay_logevent (struct lev_generic *E, int size);

int init_bayes_data (int schema) {
  replay_logevent = bayes_replay_logevent;
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

  int i;
  for (i = 1; i <= header.user_cnt; i++) {
    local_uid (header.user_index[i].id);
  }

  was = 1;
  old_log_split_min = log_split_min;
  old_log_split_max = log_split_max;
  old_log_split_mod = log_split_mod;

  log_schema = BAYES_SCHEMA_V1;
  init_bayes_data (log_schema);
}

static int bayes_le_start (struct lev_start *E) {
  if (E->schema_id != BAYES_SCHEMA_V1) {
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

int bayes_replay_logevent (struct lev_generic *E, int size) {
//  fprintf (stderr, "bayes_replay_logevent %lld\n", log_cur_pos());
//  fprintf (stderr, "%x\n", E->type);
  if (index_mode) {
    if ((_eventsLeft && --_eventsLeft == 0) || get_memory_used() > max_memory * 3 / 2) {
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
    return bayes_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_BAYES_SET ... LEV_BAYES_SET + 15:
    if (size < (int)sizeof (struct lev_bayes_set)) {
      return -2;
    }
    s = ((struct lev_bayes_set *) E)->text_len;
    if (s < 0) {
      return -4;
    }
    s += 1 + offsetof (struct lev_bayes_set, text);
    if (size < s) {
      return -2;
    }
    bayes_lev_set ((struct lev_bayes_set *)E);
    return s;
  case LEV_BAYES_BLACK_LIST ... LEV_BAYES_BLACK_LIST + 1:
    if (size < (int)sizeof (struct lev_bayes_black_list)) {
      return -2;
    }
    s = ((struct lev_bayes_black_list *) E)->text_len;
    if (s < 0) {
      return -4;
    }
    s += 1 + offsetof (struct lev_bayes_black_list, text);
    if (size < s) {
      return -2;
    }
    black_list_add ((struct lev_bayes_black_list *)E);
    return s;
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}


/*
 *
 *         BAYES INDEX AIO
 *
 */


struct aio_connection *WaitAio;
int onload_user_metafile (struct connection *c, int read_bytes);

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_user_metafile
};

conn_query_type_t aio_metafile_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "bayes-data-aio-metafile-query",
.wakeup = aio_query_timeout,
.close = delete_aio_query,
.complete = delete_aio_query
};

void load_user_metafile (user *u, int local_id, int no_aio) {
  static struct aio_connection empty_aio_conn;

  no_aio |= debug_on;
  WaitAio = NULL;

  if (user_loaded (u)) {
    return;
  }

  if (local_id > header.user_cnt || header.user_index[local_id].size == 0) {
    u->metafile = EMPTY__METAFILE;
    u->metafile_len = 12;

    empty_aio_conn.extra = u;
    empty_aio_conn.basic_type = ct_aio;
    u->aio = &empty_aio_conn;

    onload_user_metafile ((struct connection *)(&empty_aio_conn), u->metafile_len);
    return;
  }

  if (u->aio != NULL) {
    check_aio_completion (u->aio);
    if (u->aio != NULL) {
      WaitAio = u->aio;
      return;
    }

    if (u->metafile) {
      return;
    } else {
      fprintf (stderr, "Previous AIO query failed for user at %p, scheduling a new one\n", u);
    }
  }

  u->metafile_len = header.user_index[local_id].size;
  u->metafile = qmalloc (u->metafile_len);
  if (u->metafile == NULL) {
    fprintf (stderr, "no space to load metafile - cannot allocate %d bytes (%lld currently used)\n", u->metafile_len, (long long)get_memory_used());
    assert (0);
  }
  allocated_metafile_bytes += u->metafile_len;

  if (verbosity > 2) {
    fprintf (stderr, "*** Scheduled reading user data from index %d at position %lld, %d bytes, noaio = %d\n", fd[0], header.user_index[local_id].shift, u->metafile_len, no_aio);
  }

  assert (1 <= local_id && local_id <= header.user_cnt);
  if (no_aio) {
    double disk_time = -get_utime (CLOCK_MONOTONIC);

    assert (lseek (fd[0], header.user_index[local_id].shift, SEEK_SET) == header.user_index[local_id].shift);
    int size = header.user_index[local_id].size;
    int r = read (fd[0], u->metafile, size);
    if (r != size) {
      fprintf (stderr, "error reading user %d from index file: read %d bytes instead of %d at position %lld: %m\n", local_id, r, size, header.user_index[local_id].shift);
      assert (r == size);
    }

    disk_time += get_utime (CLOCK_MONOTONIC);
    if (verbosity > 2) {
      fprintf (stderr, "  disk time = %.6lf\n", disk_time);
    }

    empty_aio_conn.extra = u;
    empty_aio_conn.basic_type = ct_aio;
    u->aio = &empty_aio_conn;

    assert (u->aio != NULL);

    onload_user_metafile ((struct connection *)(&empty_aio_conn), u->metafile_len);
  } else {
    u->aio = create_aio_read_connection (fd[0], u->metafile, header.user_index[local_id].shift, u->metafile_len, &ct_metafile_aio, u);
    assert (u->aio != NULL);
    WaitAio = u->aio;
  }

  return;
}

int onload_user_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 2) {
    fprintf (stderr, "onload_user_metafile (%p,%d)\n", c, read_bytes);
  }

  struct aio_connection *a = (struct aio_connection *)c;
  user *u = (user *) a->extra;

  assert (a->basic_type == ct_aio);
  assert (u != NULL);

  if (u->aio != a) {
    fprintf (stderr, "assertion (u->aio == a) will fail\n");
    fprintf (stderr, "%p != %p\n", u->aio, a);
  }

  assert (u->aio == a);

  if (read_bytes != u->metafile_len) {
    if (verbosity > 0) {
      fprintf (stderr, "ERROR reading user: read %d bytes out of %d: %m\n", read_bytes, u->metafile_len);
    }

    qfree (u->metafile, u->metafile_len);
    allocated_metafile_bytes -= u->metafile_len;
    u->metafile = NULL;
    u->metafile_len = -1;
    u->aio = NULL;
    return 0;
  }
  assert (read_bytes == u->metafile_len);

  if (verbosity > 2) {
    fprintf (stderr, "*** Read user: read %d bytes\n", read_bytes);
  }

  u->aio = NULL;

  bind_user_metafile (u);

  add_user_used (u);
  cur_users++;

  while (allocated_metafile_bytes > max_memory * MEMORY_USER_PERCENT) {
    if (user_LRU_unload() == -1) {
      assert (0);
    }
  }

  return 1;
}

int unload_user_metafile (user *u) {
  assert (u != NULL);

  int user_id = 0;

  if (verbosity > 1) {
    user_id = ltbl_get_rev (&user_table, (int)(u - users));
  }

  if (verbosity > 2) {
    fprintf (stderr, "unload_user_metafile (%d)\n", user_id);
  }

  if (!u || !user_loaded (u)) {
    if (verbosity > 1) {
      fprintf (stderr, "cannot unload user metafile (%d)\n", user_id);
      assert (0);
    }
    return 0;
  }

  allocated_metafile_bytes -= u->metafile_len;

  unbind_user_metafile (u);

  if (verbosity > 2) {
    fprintf (stderr, "unload_user_metafile (%d) END\n", user_id);
  }

  return 1;
}

void bind_user_metafile (user *u) {
  if (verbosity > 2) {
    fprintf (stderr, "bind user metafile local id = %d (%p)\n", (int)(u - users), u);
  }
  int local_id = (int)(u - users);

  if (u->metafile == NULL || u->metafile == EMPTY__METAFILE) {
    return;
  }

  assert (1 <= local_id && local_id <= header.user_cnt);
}

void unbind_user_metafile (user *u) {
  if (u == NULL) {
    return;
  }

  if (verbosity > 2) {
    fprintf (stderr, "unbind_user_metafile\n");
  }

  if (u->metafile != NULL && u->metafile != EMPTY__METAFILE) {
    qfree (u->metafile, u->metafile_len);
  }

  u->metafile = NULL;
  u->metafile_len = -1;
}


/*
 *
 *         BAYES INDEX
 *
 */


char *buff;
entry_t *new_buff;


long long get_index_header_size (index_header *header) {
  return sizeof (index_header) + sizeof (user_index_data) * (header->user_cnt + 1);
}


int black_list_save (void) {
  int len = black_list_get (buff, max_words * sizeof (entry_t));
  len++;
  assert (write (fd[1], buff, len) == len);
  return len;
}

void black_list_load (int size) {
  static char buff[1 << 20];
  assert (size < (1 << 20));

  int r = read (fd[0], buff, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position ???: %m\n", r, size);
    assert (r == size);
  }

  black_list_set (buff, size - 1);
}


long long bayes_global_save (bayes global_bayes) {
  qhash_table *old_table = &global_bayes.cnt;
  int old_size = global_bayes.cnt.size, old_n = global_bayes.cnt.n, new_n;

  new_n = 3 * old_size / 2;
  if (new_n < max_words) {
    new_n = max_words;
  }

  assert (new_n >= old_table->n);
  int regenerate = (new_n > old_table->n);

  qhash_table *new_table;
  if (!regenerate) {
    new_table = old_table;
  } else {
    int i, limit = new_n / 40000000;

//    fprintf (stderr, "regenerate : limit(%d)\n", limit);

    if (limit >= 1) {
      int new_size = 0;

      for (i = 0; i < old_table->n; i++) {
        if (old_table->e[i].h != 0 && old_table->e[i].val.spam + old_table->e[i].val.ham > limit) {
					new_size++;
        }
      }
      new_n = 3 * new_size / 2;
      if (new_n < max_words) {
        new_n = max_words;
      }
      global_bayes.cnt.size = new_size;

//      fprintf (stderr, "regenerate : old_n(%d) new_n(%d)\n", old_size, new_size);
    }

    new_table = qmalloc0 (sizeof (qhash_table));
    qhtbl_init (new_table);
    qhtbl_set_size (new_table, new_n);

    for (i = 0; i < old_table->n; i++) {
      if (old_table->e[i].h != 0 && old_table->e[i].val.spam + old_table->e[i].val.ham > limit) {
        *qhtbl_add (new_table, old_table->e[i].h) = old_table->e[i].val;
      }
    }
    global_bayes.cnt.n = new_n;
  }

  long long size1 = 3 * sizeof (int) + 2 * sizeof (long long);
  assert (write (fd[1], &global_bayes, size1) == size1);

  long long size2 = sizeof (entry_t) * new_table->n;
  assert (write (fd[1], new_table->e, size2) == size2);

  if (regenerate) {
    qhtbl_free (new_table);
    qfree (new_table, sizeof (qhash_table));
    global_bayes.cnt.n = old_n;
    global_bayes.cnt.size = old_size;
  }
  return size1 + size2;
}

void bayes_global_load (bayes *global_bayes) {
  int size = 3 * sizeof (int) + 2 * sizeof (long long);
  int r = read (fd[0], global_bayes, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position ???: %m\n", r, size);
    assert (r == size);
  }

  int new_n = global_bayes->cnt.n,
      new_size = global_bayes->cnt.size;
  if (new_size > max_words) {
    max_words = new_size;
  }

  qhtbl_init (&global_bayes->cnt);
  qhtbl_set_size (&global_bayes->cnt, new_n);
  global_bayes->cnt.size = new_size;

  size = sizeof (entry_t) * new_n;
  r = read (fd[0], global_bayes->cnt.e, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position ???: %m\n", r, size);
    assert (r == size);
  }
}


int load_header (kfs_file_handle_t Index) {
  if (Index == NULL) {
    fd[0] = -1;

    header.user_cnt = 0;
    header.user_index = NULL;

    header.log_pos0 = 0;
    header.log_pos1 = 0;
    header.log_timestamp = 0;
    header.log_split_min = 0;
    header.log_split_max = 1;
    header.log_split_mod = 1;
    header.log_pos0_crc32 = 0;
    header.log_pos1_crc32 = 0;

    header.teach_messages = 0;
    header.black_list_size = 0;

    header.created_at = time (NULL);
    header_size = sizeof (index_header);

    global_bayes.nbad = global_bayes.ngood = 0;
    qhtbl_init (&global_bayes.cnt);
    qhtbl_set_size (&global_bayes.cnt, max_words);

    return 0;
  }

  fd[0] = Index->fd;
  int offset = Index->offset;

  //read header
  assert (lseek (fd[0], offset, SEEK_SET) == offset);

  int size = sizeof (index_header) - sizeof (long);
  int r = read (fd[0], &header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %d: %m\n", r, size, offset);
    assert (r == size);
  }

  size = sizeof (user_index_data) * (header.user_cnt + 1);
  header.user_index = qmalloc (size);

  r = read (fd[0], header.user_index, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d: %m\n", r, size);
    assert (r == size);
  }

  assert (header.log_split_max);
  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;

  header_size = get_index_header_size (&header) - sizeof (long);

  bayes_global_load (&global_bayes);
  black_list_load (header.black_list_size);

  teach_messages = header.teach_messages;

  if (verbosity > 1) {
    fprintf (stderr, "header loaded %d %d %d %d\n", fd[0], log_split_min, log_split_max, log_split_mod);
    fprintf (stderr, "ok\n");
  }
  return 1;
}

void free_header (index_header *header) {
  if (header->user_index != NULL) {
    qfree (header->user_index, sizeof (user_index_data) * (header->user_cnt + 1));
  }
}

int entry_t_cmp (const void *a, const void *b) {
  ll x = ((entry_t *)a)->h, y = ((entry_t *)b)->h;
  if (x < y) {
    return -1;
  }
  if (x > y) {
    return 1;
  }
  return 0;
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

  if (!binlog_disabled) {
    relax_write_log_crc32();
  } else {
    relax_log_crc32 (0);
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  header.user_cnt = user_table.currId - 1;
  assert (header.user_cnt >= 0);

  fd[1] = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
  if (fd[1] < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  header.magic = BAYES_INDEX_MAGIC;

  header.user_index = qmalloc0 (sizeof (user_index_data) * (header.user_cnt + 1));
  header.created_at = time (NULL);

  long long fCurr = get_index_header_size (&header) - sizeof (long);
  assert (lseek (fd[1], fCurr, SEEK_SET) == fCurr);

  fCurr += bayes_global_save (global_bayes);
  header.black_list_size = black_list_save();
  fCurr += header.black_list_size;

  header.teach_messages = teach_messages;

  // for each user
  int u_id;
  for (u_id = 1; u_id <= header.user_cnt; u_id++) {
    if (verbosity > 1) {
      fprintf (stderr, "u_id = %d, fd = %d\n", u_id, fd[1]);
    }
    user *u = &users[u_id];

    load_user_metafile (u, u_id, 1);
    assert (u->metafile_len != -1);

    header.user_index[u_id].id = ltbl_get_rev (&user_table, u_id);
    if (verbosity > 1) {
      fprintf (stderr, "user_id = %d\n", header.user_index[u_id].id);
    }
    header.user_index[u_id].shift = fCurr;

    assert (local_uid (header.user_index[u_id].id) != -1);

    qhash_table *old_table = &u->b.cnt;
    assert (old_table->size < 4000000);

    int i, j = 0;
    for (i = 0; i < old_table->n; i++) {
      if (old_table->e[i].h != 0) {
        new_buff[j++] = old_table->e[i];
      }
    }

    assert (j == old_table->size);
    qsort (new_buff, j, sizeof (entry_t), entry_t_cmp);

    int i1 = 0, i2 = 0, n1 = ((int *)u->metafile)[2], n2 = j;

    assert (n1 + n2 + 3 < max_words);

    entry_t *entry_buff = (entry_t *)( ((int *)buff) + 3);
    i = 0;
    entry_t *old_buff = (entry_t *)( ((int *)u->metafile) + 3);
    while (i1 < n1 || i2 < n2) {
      if (i1 == n1 || (i2 < n2 && old_buff[i1].h > new_buff[i2].h)) {
        entry_buff[i++] = new_buff[i2++];
      } else if (i2 == n2 || old_buff[i1].h < new_buff[i2].h) {
        entry_buff[i++] = old_buff[i1++];
      } else {
        entry_buff[i] = old_buff[i1++];
        entry_buff[i].val.ham += new_buff[i2].val.ham;
        entry_buff[i++].val.spam += new_buff[i2++].val.spam;
      }
    }
    ((int *)buff)[HAM] = u->b.ngood + ((int *)u->metafile)[HAM];
    ((int *)buff)[SPAM] = u->b.nbad + ((int *)u->metafile)[SPAM];
    ((int *)buff)[2] = i;

    int buff_sz = sizeof (entry_t) * i + sizeof (int) * 3;

//  write user
    assert (write (fd[1], buff, buff_sz) == buff_sz);
    header.user_index[u_id].size = buff_sz;
    fCurr += header.user_index[u_id].size;
    assert (user_LRU_unload() != -1);
  }

  // write header
  header.log_pos1 = log_cur_pos();
  header.log_timestamp = log_last_ts;
  header.log_split_min = log_split_min;
  header.log_split_max = log_split_max;
  header.log_split_mod = log_split_mod;
  relax_log_crc32 (0);
  header.log_pos1_crc32 = ~log_crc32_complement;

/*  header.log_pos1 = 0;
  header.log_pos1_crc32 = 0;*/

  assert (lseek (fd[1], 0, SEEK_SET) == 0);
  assert (write (fd[1], &header, sizeof (header) - sizeof (long)) == (ssize_t)(sizeof (header) - sizeof (long)));
  assert (write (fd[1], header.user_index, sizeof (user_index_data) * (header.user_cnt + 1)) == (ssize_t)(sizeof (user_index_data) * (header.user_cnt + 1)));

  free_header (&header);

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

void user_init (user *u) {
  u->metafile = NULL;
  u->metafile_len = -1;
  u->aio = NULL;
  u->next_used = NULL;
  u->prev_used = NULL;

  u->b.ngood = 0;
  u->b.nbad = 0;
  qhtbl_init (&u->b.cnt);
}

int init_all (kfs_file_handle_t Index) {
  int i;

  log_ts_exact_interval = 1;

  ltbl_init (&user_table);

  bl_head = qmalloc (sizeof (black_list));
  black_list_init (bl_head);

  int f = load_header (Index);

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  int user_cnt = index_users = header.user_cnt;

  if (user_cnt < 1000000) {
    user_cnt = 1000000;
  }

  assert (user_cnt >= 1000000);
  user_cnt *= 1.1;

  while (user_cnt % 2 == 0 || user_cnt % 5 == 0) {
    user_cnt++;
  }

  ltbl_set_size (&user_table, user_cnt);
  users = qmalloc (sizeof (user) * user_cnt);

  for (i = 0; i < user_cnt; i++) {
    user_init (&users[i]);
  }

  LRU_head = users;
  LRU_head->next_used = LRU_head->prev_used = LRU_head;

  if (f) {
    try_init_local_uid();
  }

  if (index_mode) {
    buff = qmalloc (max_words * sizeof (entry_t));
    new_buff = qmalloc (4000000 * sizeof (entry_t));
  }

  return f;
}

void free_all (void) {
  if (verbosity) {
    while (user_LRU_unload() != -1) {
    }

    qhtbl_free (&global_bayes.cnt);
    int i;
    for (i = 0; i < user_table.size; i++) {
      qhtbl_free (&users[i].b.cnt);
    }

    if (index_mode) {
      qfree (buff, max_words * sizeof (entry_t));
      qfree (new_buff, 4000000 * sizeof (entry_t));
    }

    while (bl_head->next != NULL) {
      black_list_do_delete (bl_head);
    }
    qfree (bl_head, sizeof (black_list));

    qfree (users, sizeof (user) * user_table.size);

    ltbl_free (&user_table);
    free_header (&header);

    fprintf (stderr, "Memory left: %ld\n", get_memory_used() - htbl_get_memory());
    assert (get_memory_used() - htbl_get_memory() == 0);
  }
}
