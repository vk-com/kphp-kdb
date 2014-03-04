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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <aio.h>
#include <byteswap.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "net-connections.h"
#include "net-aio.h"
#include "kdb-targ-binlog.h"
#include "targ-data.h"
#include "targ-index.h"
#include "targ-search.h"
#include "targ-index-layout.h"
#include "word-split.h"
#include "translit.h"
#include "server-functions.h"
#include "stemmer.h"
#include "crc32.h"
#include "listcomp.h"

#ifndef MAXINT
#define MAXINT	0x7fffffff
#endif

#ifndef MININT
#define MININT	-0x80000000
#endif

/*
 *
 *		PEOPLE SEARCH/TARGETING
 *
 */

int tot_queries;
double tot_queries_time;


int global_birthday_in_query = 0, global_birthday_left_month, global_birthday_right_month;
int global_birthday_left_day_min, global_birthday_left_day_max, global_birthday_right_day_min, global_birthday_right_day_max;
int global_birthday_month[MAX_BIRTHDAY_SOON + 3], global_birthday_day[MAX_BIRTHDAY_SOON + 3];
int global_birthday_shift_left[3], global_birthday_shift_right[3];

int global_age_in_query = 0, global_age_day, global_age_month, global_age_year;
int global_future_age_day, global_future_age_month, global_future_age_year;

/* --------- aux userlist for queries -------- */

int aux_userlist[MAX_AUX_USERS];
int aux_userlist_size;
int aux_userlist_tag;

int global_userlist_in_query;

void aux_sort (long a, long b) {
  if (a >= b) {
    return;
  }
  int h = aux_userlist[(a + b) >> 1], t;
  long i = a, j = b;
  do {
    while (aux_userlist[i] < h) { i++; }
    while (aux_userlist[j] > h) { j--; }
    if (i <= j) {
      t = aux_userlist[i];
      aux_userlist[i++] = aux_userlist[j];
      aux_userlist[j--] = t;
    }
  } while (i <= j);
  aux_sort (a, j);
  aux_sort (i, b);
}

int preprocess_aux_userlist (void) {
  long i, j;
  vkprintf (2, "preprocess_aux_userlist: size=%d tag=%d A=%d %d %d...\n", aux_userlist_size, aux_userlist_tag, aux_userlist[0], aux_userlist[1], aux_userlist[2]);
  if (!aux_userlist_size || !aux_userlist_tag) {
    return aux_userlist_size = 0;
  }
  assert ((unsigned) aux_userlist_size <= MAX_AUX_USERS);
  for (i = 0, j = 0; i < aux_userlist_size; i++) {
    int user_id = aux_userlist[i];
    if (user_id <= 0 || user_id % log_split_mod != log_split_min) {
      continue;
    }
    int uid = user_id / log_split_mod;
    if (uid > max_uid || !User[uid]) {
      continue;
    }
    aux_userlist[j++] = uid;
  }
  if (!j) {
    return aux_userlist_size = 0;
  }
  for (i = 1; i < j; i++) {
    if (aux_userlist[i] > aux_userlist[i-1]) {
      break;
    }
  }
  if (i < j) {
    aux_sort (0, j - 1);
  }
  aux_userlist_size = j;
  for (i = 1, j = 1; i < aux_userlist_size; i++) {
    if (aux_userlist[i] > aux_userlist[i-1]) {
      aux_userlist[j++] = aux_userlist[i];
    }
  }
  vkprintf (2, "AFTER preprocess_aux_userlist: size=%ld tag=%d A=%d %d %d...\n", j, aux_userlist_tag, aux_userlist[0], aux_userlist[1], aux_userlist[2]);
  return aux_userlist_size = j;
}

/* ----------- simple profiler --------------- */

#define PROFILERS_NUM	16

volatile unsigned long long profiler[PROFILERS_NUM], profiler_cnt[PROFILERS_NUM], profiling_start;

#define START_PROFILER	{ profiling_start = rdtsc(); }
#define PROFILER(x)	{ profiler[x] += rdtsc() - profiling_start;  profiler_cnt[x]++; }

void dump_profiler_data (void) {
  long i, j = PROFILERS_NUM;
  while (j > 0 && !profiler_cnt[j-1]) {
    j--;
  }
  fprintf (stderr, "%d queries performed in %.6f seconds, %.6f average; tot_users=%d\n", tot_queries, tot_queries_time, tot_queries ? tot_queries_time / tot_queries : 0, tot_users);
  for (i = 0; i < j; i++) {
    fprintf (stderr, "%ld:%llu/%llu=%.0f ", i, profiler[i], profiler_cnt[i], profiler_cnt[i] ? (double) profiler[i] / profiler_cnt[i] : 0.0);
  }
  fprintf (stderr, "\n");
}

/* -------------- query parser --------------- */

struct query_keyword_descr QueryKW[] = {
{q_id, 129, 1, 2000000000, "id"},
{q_name, 131, 8, 64, "name"},
{q_country, 128, 1, 255, "country"},
{q_city, 128, 1, -1, "city"},
{q_bday, 129, 1, 31, "bday_day"},
{q_bmonth, 129, 1, 12, "bday_month"},
{q_byear, 129, 1900, 2017, "bday_year"},
{q_political, 128, 0, MAX_POLITICAL, "political"},
{q_sex, 128, 0, MAX_SEX, "sex"},
{q_operator, 128, 0, MAX_OPERATOR, "operator"},
{q_browser, 129, 0, MAX_BROWSER, "browser"},
{q_region, 128, 0, MAX_REGION, "region"},
{q_height, 129, 0, MAX_HEIGHT, "height"},
{q_smoking, 128, 0, MAX_SMOKING, "smoking"},
{q_alcohol, 128, 0, MAX_ALCOHOL, "alcohol"},
{q_ppriority, 128, 0, MAX_PPRIORITY, "personal_priority"},
{q_iiothers, 128, 0, MAX_IIOTHERS, "important_in_others"},
{q_hidden, 129, 0, MAX_HIDDEN, "hidden"},
{q_cvisited, 129, 0, MAX_CVISITED, "countries_visited"},
{q_timezone, 129, -MAX_TIMEZONE, MAX_TIMEZONE, "timezone"},
{q_mstatus, 128, 0, MAX_MSTATUS, "status"},
{q_education, 135, 8, 0, "education"},
{q_uni_country, 8, 1, 255, "country"},
{q_uni_city, 8, 1, -1, "city"},
{q_univ, 8, 1, -1, "university"},
{q_faculty, 8, 1, -1, "faculty"},
{q_chair, 8, 1, -1, "chair"},
{q_graduation, 9, 1900, 2033, "graduation"},
{q_edu_form, 8, 0, MAX_EDU_FORM, "form"},
{q_edu_status, 8, 0, MAX_EDU_STATUS, "status"},
{q_school, 135, 16, 0, "school"},
{q_sch_country, 16, 1, 255, "country"},
{q_sch_city, 16, 1, -1, "city"},
{q_sch_id, 16, 0, -1, "id"},
{q_sch_grad, 17, 1900, 2033, "graduation"},
{q_sch_class, 16, 1, 102, "class"},
{q_sch_spec, 19, 8, 64, "spec"},
{q_address, 135, 32, 0, "address"},
{q_adr_country, 32, 1, 255, "country"},
{q_adr_city, 32, 1, -1, "city"},
{q_adr_district, 32, 1, -1, "district"},
{q_adr_station, 32, 1, -1, "station"},
{q_adr_street, 32, 1, -1, "street"},
{q_adr_type, 32, 1, -1, "type"},
{q_adr_house, 35, 8, 64, "house"},
{q_adr_name, 35, 8, 128, "name"},
{q_group, 134, 64, 0, "group"},
{q_grp_type, 64, 1, 127, "type"},
{q_grp_id, 64, -1, -1, "id"},
{q_company, 135, 256, 0, "company"},
{q_company_name, 259, 8, 128, "name"},
{q_job, 259, 8, 128, "job"},
{q_name_interests, 131, 16, 128, "name_interests"},
{q_interests, 131, 8, 128, "interests"},
{q_religion, 131, 8, 32, "religion"},
{q_hometown, 131, 8, 32, "hometown"},
{q_proposal, 131, 8, 128, "proposal"},
{q_online, 129, 0, 0x7fffffff, "online"},
{q_has_photo, 128, 0, 1, "has_photo"},
{q_uses_apps, 128, 0, 1, "uses_apps"},
{q_pays_money, 128, 0, 1, "pays_money"},
{q_privacy, 128, 0, MAX_PRIVACY, "privacy"},
{q_military, 135, 512, 0, "military"},
{q_mil_unit, 512, 1, -1, "unit"},
{q_mil_start, 513, 1900, 2033, "start"},
{q_mil_finish, 513, 1900, 2033, "finish"},
{q_lang, 134, 1024, 0, "lang"},
{q_lang_id, 1024, -1, -1, "id"},
{q_random, 129, 0, 1000, "random"},
{q_true, -6, 0, 0, "true"},
{q_false, -6, 0, 0, "false"},
{q_birthday_today, 130, 0, 0, "birthday_today"},
{q_birthday_tomorrow, 130, 0, 0, "birthday_tomorrow"},
{q_birthday_soon, 128, 0, MAX_BIRTHDAY_SOON, "birthday_soon"},
{q_age, 129, 0, 127, "age"},
{q_future_age, 129, 0, 127, "future_age"},
{q_inlist, 130, 0, 0, "in_list"},
{q_gcountry, 128, 0, MAX_GCOUNTRY, "gcountry"},
{q_custom1, 129, 0, MAX_CUSTOM1, "custom1"},
{q_custom2, 129, 0, MAX_CUSTOM2, "custom2"},
{q_custom3, 129, 0, MAX_CUSTOM3, "custom3"},
{q_custom4, 129, 0, MAX_CUSTOM4, "custom4"},
{q_custom5, 129, 0, MAX_CUSTOM5, "custom5"},
{q_custom6, 129, 0, MAX_CUSTOM6, "custom6"},
{q_custom7, 129, 0, MAX_CUSTOM7, "custom7"},
{q_custom8, 129, 0, MAX_CUSTOM8, "custom8"},
{q_custom9, 129, 0, MAX_CUSTOM9, "custom9"},
{q_custom10, 129, 0, MAX_CUSTOM10, "custom10"},
{q_custom11, 129, 0, MAX_CUSTOM11, "custom11"},
{q_custom12, 129, 0, MAX_CUSTOM12, "custom12"},
{q_custom13, 129, 0, MAX_CUSTOM13, "custom13"},
{q_custom14, 129, 0, MAX_CUSTOM14, "custom14"},
{q_custom15, 129, 0, MAX_CUSTOM15, "custom15"},
{q_none, 0, 0, 0, 0}
};

query_t Q[MAX_QUERY_NODES];
int Qw;
char *Qs, *Qs0;
query_t *Qq, *Qfree;
int Q_order, Q_limit, Q_raw;

query_t Q_true = {.type = q_true};

query_t *Q_aux[MAX_AUX_QUERIES];
int Q_aux_num;

static inline int skip_spc (void) {
  while (*Qs == ' ' || *Qs == 9) { Qs++; }
  return (unsigned char) *Qs;
}

query_t *new_qnode (int type, int val) {
  query_t *A;
  if (Qfree) {
    A = Qfree;
    assert (A >= Q && A < Q + MAX_QUERY_NODES && !A->type);
    Qfree = A->right;
  } else {
    assert (Qw >= 0 && Qw <= MAX_QUERY_NODES);
    if (Qw >= MAX_QUERY_NODES) { return 0; }
    A = Q + Qw;
    Qw++;
  }
  memset (A, 0, sizeof(query_t));
  A->max_res = INFTY;
  A->type = type;
  A->value2 = A->value = val;
  return A;
}

void free_qnode (query_t *Q) {
  Q->type = q_none;
  Q->right = Qfree;
  Qfree = Q;
}

#define	GRAY	(-1 << 31)

static inline void negate_query (query_t *Q) {
  Q->flags ^= 1;
  if (Q->max_res != GRAY) {
    Q->max_res = ~Q->max_res;
  }
}

query_t *parse_wordlist (int and, int type, int maxw) {
  query_t *A = 0, *B, *C;
  char *end;
  int len, c = 0;
  static char buff[256];
  int type1 = type, type2 = type;

  if (type == q_name_interests) {
    type1 = q_name;
    type2 = q_interests;
  }

  if (*Qs == '\'' || *Qs == '"') {
    c = *Qs++;
    skip_spc();
    end = Qs;
    while (*end && *end != c) {
      end++;
    }
    if (!*end) { return 0; }
    end++;
  } else {
    end = Qs;
    while (*end && *end != ')' && *end != '|' && *end != '#') { end++; }
  }
  while (Qs < end) {
    len = get_notword (Qs);
    if (len < 0) { break; }
    Qs += len;
    if (Qs > end) { Qs = end; break; }
    len = get_word (Qs);
    if (len > end - Qs) { len = end - Qs; }
    if (len <= 0 || len >= 250) { break; }
    if (!maxw--) { return 0; }
    lc_str (buff, Qs, len);
    C = new_qnode (type, len);
    if (!C) { 
      Qs += len;
      return 0; 
    }
    C->hash = (word_crc64 (buff, len) + type1) | (1ULL << 63);
    C->hash2 = (word_crc64 (buff, my_lc_str (buff, Qs, len)) + type2) | (1ULL << 63);
    C->complexity = 4;
    C->flags |= 32;
    Qs += len;
    if (!A) {
      A = C;
    } else {
      B = A;
      A = new_qnode (and, 0);
      if (!A) { return 0; }
      A->left = B;
      A->right = C;
      A->complexity = B->complexity + C->complexity;
    }
  }
  return A;
}

query_t *parse_query (int mode);

query_t *parse_queryB (int mode) {
  query_t *A, *B;
  int len, value, c;
  struct query_keyword_descr *KW;
  if (skip_spc() == '(') {
    Qs++;
    A = parse_query (mode);
    if (!A) { return A; }
    while (*Qs == ' ' || *Qs == 9) { Qs++; }
    if (*Qs != ')') { return 0; }
    Qs++;
    return A;
  }
  if (*Qs == '!') {
    Qs++;
    A = parse_queryB (mode);
    if (!A) { return 0; }
    A->flags ^= 1;
    return A;
  }
  len = 0;
  if (*Qs >= 'a' && *Qs <= 'z') {
    while (len <= 32 && ((*Qs >= 'a' && *Qs <= 'z') || *Qs == '_' || (*Qs >= '0' && *Qs <= '9'))) {
      len++;
      Qs++;
    }
  }
  if (!len) { return 0; }
  Qs -= len;
  for (KW = QueryKW; KW->q_type; KW++) {
    if ((KW->flags & mode) && !strncmp (KW->str, Qs, len) && !KW->str[len]) {
      break;
    }
  }
  if (!KW->q_type) { return 0; }
  Qs += len;
  skip_spc();
  c = 0;

  if ((KW->flags & 7) == 7) {
    if (Qs[0] != '.') { return 0; }
    Qs++;
    B = parse_queryB (KW->minv);
    if (!B) { return 0; }
    A = new_qnode (KW->q_type, 0);
    if (!A) { return 0; }
    A->left = B;
    A->complexity = B->complexity * 4;
    return A;
  }

  if ((KW->flags & 7) == 6) {
    if (Qs[0] != '.') { return 0; }
    Qs++;
    A = parse_queryB (KW->minv);
    if (!A) { return 0; }
    return A;
  }

  if ((KW->flags & 7) == 3) {
    if (*Qs != '=') { return 0; }
    Qs++;
    skip_spc();
    return parse_wordlist (q_and, KW->q_type, KW->minv);
  }

  if ((KW->flags & 7) == 2) {
    A = new_qnode (KW->q_type, 0);
    if (A->type != q_true && A->type != q_false) {
      A->complexity = 4;
    }
    return A;
  }

  if ((KW->flags & 7) == 1 && (Qs[0] == '>' || Qs[0] == '<') && Qs[1] == '=') {
    c = Qs[0];
    Qs++;
  }

  if (*Qs != '=') { 
    return 0; 
  }
  Qs++;
  skip_spc();

  char *Qs1;
  value = strtol (Qs, &Qs1, 10);
  if (Qs1 == Qs) { 
    return 0; 
  }
  if ((KW->minv != -1 && value < KW->minv) || (KW->maxv != -1 && value > KW->maxv)) { 
    return 0; 
  }
  Qs = Qs1;
  A = new_qnode (KW->q_type, value);
  if (!A) { return 0; }
  if ((KW->flags & 7) == 1) {
    A->flags |= 16;
    A->value2 = value;
    if (c == '<' && (value != KW->minv || value == -1)) { 
      A->value = /* KW->minv */ (KW->minv > 0 ? KW->minv : MININT); 
    }
    if (c == '>' && (value != KW->maxv || value == -1)) { 
      A->value2 = /* KW->maxv */ MAXINT; 
    }
    A->complexity = (A->value == MININT || A->value2 == MAXINT || A->value == A->value2 ? 1 : 2);
  } else {
    A->flags |= 8;
    A->complexity = (A->type == q_birthday_soon ? 4 : 1);
  }
  return A;
}

query_t *parse_queryA (int mode) {
  query_t *A, *B, *C;
  A = parse_queryB (mode);
  if (!A) { return A; }
  while (skip_spc() == '&') {
    Qs++;
    B = parse_queryB (mode);
    if (!B) { return B; }
    C = new_qnode (q_and, 0);
    if (!C) { return C; }
    C->left = A;
    C->right = B;
    C->complexity = A->complexity + B->complexity;
    A = C;
  }
  return A;
}

query_t *parse_query (int mode) {
  query_t *A, *B, *C;
  A = parse_queryA (mode);
  if (!A) { return A; }
  while (skip_spc() == '|') {
    Qs++;
    B = parse_queryA (mode);
    if (!B) { return B; }
    C = new_qnode (q_or, 0);
    if (!C) { return C; }
    C->left = A;
    C->right = B;
    C->complexity = A->complexity + B->complexity;
    A = C;
  }
  return A;
}

char *compile_query (char *str) {
  Qw = 0;
  Qfree = 0;
  Qs = Qs0 = str;
  vkprintf (3, "parsing query `%s'\n", str);
  Qq = parse_query (128);
  Q_order = 0;
  Q_limit = 0;
  Q_aux_num = 0;
  return Qq ? 0 : Qs;
}

void clear_query_list (void) {
  vkprintf (3, "clearing query list\n");
  Qw = 0;
  Qfree = 0;
  Qs = Qs0 = 0;
  Qq = 0;
  Q_order = 0;
  Q_limit = 0;
  Q_aux_num = 0;
}  

query_t *compile_add_query (char **str_ptr) {
  vkprintf (3, "parsing additional query %.40s...\n", *str_ptr);
  if (Qq) {
    if (Q_aux_num == MAX_AUX_QUERIES) {
      return 0;
    }
    Q_aux[Q_aux_num++] = Qq;
    Qq = 0;
  }
  Qs = *str_ptr;
  query_t *R = Qq = parse_query (128);
  *str_ptr = Qs;
  return R;
}

/* -------------- reporting/sorting found users ------------ */

int R_cnt, R_tot;
int R[MAX_USERS*2+2];

store_res_func_t store_res;
postprocess_res_func_t postprocess_res;

void store_res_std (int uid) {
  int i, j, r;
  R_tot++;
  if (R_position > 0) {
    r = user_ad_price (uid, R_position);
    if (r >= Q_limit) { 
      r = Q_limit - 1; 
    } else if (r < 0) {
      r = 0;
    }
    R[r]++;
    return;
  } else if (R_position == (-1 << 31)) {
    return;
  }
  if (Q_limit <= 0) { return; }
  switch (Q_order) {
  case 0:
  case 1:
  default:
    if (R_cnt < Q_limit) {
      R[R_cnt++] = uid;
    }
    break;
  case -1:
    if (R_cnt == Q_limit) { R_cnt = 0; }
    R[R_cnt++] = uid;
    break;
  case -2:
    r = UserRate[uid];
    if (R_cnt == Q_limit) {
      if (UserRate[R[1]] >= r) { return; }
      i = 1;
      while (1) {
        j = i*2;
        if (j > R_cnt) { break; }
        if (j < R_cnt && UserRate[R[j+1]] < UserRate[R[j]]) { j++; }
        if (UserRate[R[j]] >= r) { break; }
        R[i] = R[j];
        i = j;
      }
      R[i] = uid;
    } else {
      i = ++R_cnt;
      while (i > 1) {
        j = (i >> 1);
        if (UserRate[R[j]] <= r) { break; }
        R[i] = R[j];
        i = j;
      }
      R[i] = uid;
    }
    break;
  case 2:
    r = UserRate[uid];
    if (R_cnt == Q_limit) {
      if (UserRate[R[1]] <= r) { return; }
      i = 1;
      while (1) {
        j = i*2;
        if (j > R_cnt) { break; }
        if (j < R_cnt && UserRate[R[j+1]] > UserRate[R[j]]) { j++; }
        if (UserRate[R[j]] <= r) { break; }
        R[i] = R[j];
        i = j;
      }
      R[i] = uid;
    } else {
      i = ++R_cnt;
      while (i > 1) {
        j = (i >> 1);
        if (UserRate[R[j]] >= r) { break; }
        R[i] = R[j];
        i = j;
      }
      R[i] = uid;
    }
    break;
  }    
}

void postprocess_res_std (void) {
  int i, j, k, t, r;
  if (R_position) { return; }
  switch (Q_order) {
  case -1:
    k = R_cnt - 1;
    for (i = 0; i < k - i; i++) {
      t = R[k-i];  R[k-i] = R[i];  R[i] = t;
    }
    if (R_tot >= Q_limit) {
      k = R_cnt + Q_limit - 1;
      for (i = R_cnt; i < k - i; i++) {
        t = R[k-i];  R[k-i] = R[i];  R[i] = t;
      }
      R_cnt = Q_limit;
    }
    break;
  case 2:
    if (!R_cnt) { return; }
    for (k = R_cnt - 1; k > 0; k--) {
      t = R[k+1];
      R[2*k] = log_split_min + log_split_mod * R[1];
      R[2*k+1] = UserRate[R[1]];
      r = UserRate[t];
      i = 1;
      while (1) {
        j = 2*i;
        if (j > k) { break; }
        if (j < k && UserRate[R[j+1]] > UserRate[R[j]]) { j++; }
        if (r >= UserRate[R[j]]) { break; }
        R[i] = R[j];
        i = j;
      }
      R[i] = t;
    }
    R[0] = log_split_min + log_split_mod * R[1];
    R[1] = UserRate[R[1]];
    R_cnt *= 2;
    return;
  case -2:
    if (!R_cnt) { return; }
    for (k = R_cnt - 1; k > 0; k--) {
      t = R[k+1];
      R[2*k] = log_split_min + log_split_mod * R[1];
      R[2*k+1] = UserRate[R[1]];
      r = UserRate[t];
      i = 1;
      while (1) {
        j = 2*i;
        if (j > k) { break; }
        if (j < k && UserRate[R[j+1]] < UserRate[R[j]]) { j++; }
        if (r <= UserRate[R[j]]) { break; }
        R[i] = R[j];
        i = j;
      }
      R[i] = t;
    }
    R[0] = log_split_min + log_split_mod * R[1];
    R[1] = UserRate[R[1]];
    R_cnt *= 2;
    return;
  }      
  for (i = 0; i < R_cnt; i++) {
    R[i] = log_split_min + log_split_mod * R[i];
  }
}

void clear_res (void) {
  R_cnt = R_tot = 0;
  if (R_position != (-1 << 31)) {
    store_res = store_res_std;
    postprocess_res = postprocess_res_std;
  }
}

/* -------------- estimate subquery complexity, optimize query ------------- */

static void print_query (query_t *Q);

static inline int min (int x, int y) { return x < y ? x : y; }
static inline int max (int x, int y) { return x > y ? x : y; }

static inline int estimate_users_in_range (int min_user_id, int max_user_id) {
  int _min_uid = (min_user_id - log_split_min + log_split_mod - 1) / log_split_mod;
  int _max_uid = ((long long) max_user_id - log_split_min + log_split_mod) / log_split_mod - 1;
  if (_max_uid > max_uid) {
    _max_uid = max_uid;
  }
  if (_min_uid <= 0) {
    _min_uid = 0;
  }
  return _min_uid <= _max_uid ? _max_uid - _min_uid + 1 : 0;
}

int gray_threshold, gray_threshold2;

#define IS_GRAY(x)	((x)==GRAY)
#define IS_BIG(x)	((unsigned) (x) > (unsigned) GRAY)
#define IS_SMALL(x)	((x) >= 0)
#define IS_GRAYISH(x)	((x) > gray_threshold || (x) < gray_threshold2)
#define IS_SMALLISH(x)	((unsigned) (x) <= (unsigned) gray_threshold)
#define IS_BIGGISH(x)	((unsigned) (x) >= (unsigned) gray_threshold2)
#define IS_ALWAYS_FALSE(x)	((x) == 0)
#define IS_ALWAYS_TRUE(x)	((x) == -1)
#define Q_IS_SMALL(q)	IS_SMALL((q)->max_res)
#define Q_IS_GRAY(q)	IS_GRAY((q)->max_res)
#define Q_IS_BIG(q)	IS_BIG((q)->max_res)
#define Q_IS_SMALLISH(q)	IS_SMALLISH((q)->max_res)
#define Q_IS_GRAYISH(q)	IS_GRAYISH((q)->max_res)
#define Q_IS_BIGGISH(q)	IS_BIGGISH((q)->max_res)

static inline void relax_max_res (query_t *Q) {
  int a = Q->left->max_res, b = Q->right->max_res, r;
  switch (Q->type) {
  case q_and:
    if (IS_SMALL(a) && IS_SMALL(b)) {
      r = min (a, b);
    } else if (IS_SMALL(a)) {
      r = a;
    } else if (IS_SMALL(b)) {
      r = b;
    } else if (IS_GRAY(a) || IS_GRAY(b)) {
      r = GRAY;
    } else {
      r = a + b + 1;
      if (r < ~INFTY) {
	r = ~INFTY;
      }
    }
    break;
  case q_or:
    if (IS_BIG(a) && IS_BIG(b)) {
      r = max (a, b);
    } else if (IS_BIG(a)) {
      r = a;
    } else if (IS_BIG(b)) {
      r = b;
    } else if (IS_GRAY(a) || IS_GRAY(b)) {
      r = GRAY;
    } else {
      r = a + b;
      if (r > INFTY) {
	r = INFTY;
      }
    }
    break;
  default:
    assert (0);
  }
  Q->max_res = r;
  Q->complexity = Q->left->complexity + Q->right->complexity;
}

// (OLD:) new Q->flags: 1 = has list, 2 = is exact, 4 = is AND, 8 = is OR/NOT
// (OLD:) fa: +1 = in OR/NOT branch, +2 = in quantifier (address/education/military/...) branch
// NEW: Q->flags: +1 = negate all expression, +2 = is quantifier, +4 = is exact, +8 = int equality, +16 = int range, +32 = word

int estimate_query_complexity (query_t *Q, int fa) {
  int a, b, r = GRAY, t;

  if (Q->flags & 1) {
    fa |= 1;
  }

  switch (Q->type) {
  case q_and:
    a = estimate_query_complexity (Q->left, fa);
    b = estimate_query_complexity (Q->right, fa);
    if (IS_SMALL(a) && IS_SMALL(b)) {
      r = min (a, b);
    } else if (IS_SMALL(a)) {
      r = a;
    } else if (IS_SMALL(b)) {
      r = b;
    } else if (IS_GRAY(a) || IS_GRAY(b)) {
      r = GRAY;
    } else {
      r = a + b + 1;
      if (r < ~INFTY) {
	r = ~INFTY;
      }
    }
    //    Q->flags = (Q->left->flags & Q->right->flags) | 4 | ((Q->left->flags | Q->right->flags) & 8);
    break;
  case q_or:
    fa |= 1;
    a = estimate_query_complexity (Q->left, fa);
    b = estimate_query_complexity (Q->right, fa);
    if (IS_BIG(a) && IS_BIG(b)) {
      r = max (a, b);
    } else if (IS_BIG(a)) {
      r = a;
    } else if (IS_BIG(b)) {
      r = b;
    } else if (IS_GRAY(a) || IS_GRAY(b)) {
      r = GRAY;
    } else {
      r = a + b;
      if (r > INFTY) {
	r = INFTY;
      }
    }
    //    Q->flags = (Q->left->flags & Q->right->flags) | 8 | ((Q->left->flags | Q->right->flags) & 4);
    break;
  case q_true:
    r = -1;
    Q->flags |= 4;
    break;
  case q_inlist:
    if (aux_userlist_tag && aux_userlist_size) {
      if (!global_userlist_in_query++) {
	preprocess_aux_userlist ();
      }
      if (aux_userlist_size) {
	r = aux_userlist_size;
	break;
      }
    }
    Q->type = q_false;
  case q_false:
    r = 0;
    Q->flags |= 4;
    break;
  case q_online:
    /* online tree "optimization" commented out for the time being
    if (is_valid_online_stamp (Q->value) && is_valid_online_stamp (Q->value2)) {
      if (Q->value > Q->value2 || Q->value > ocur_now) {
        r = 0;
      } else {
        r = online_get_cyclic_interval (online_convert_time (Q->value), online_convert_time (Q->value2));
      }
      Q->flags |= 4;
    }
    */
    if (Q->value > Q->value2 || Q->value > ocur_now) {
      r = 0;
    }
    break;
  case q_id:
    r = estimate_users_in_range (Q->value, Q->value2);
    break;
  case q_birthday_today:
  case q_birthday_tomorrow:
  case q_birthday_soon:
    global_birthday_in_query = 1;
  case q_random:
  case q_grp_type:
  case q_privacy:
    break;
  case q_age:
  case q_future_age:
    global_age_in_query = 1;
    break;
  case q_country:
  case q_city:
  case q_bday:
  case q_bmonth:
  case q_byear:
  case q_political:
  case q_sex:
  case q_operator:
  case q_browser:
  case q_region:
  case q_height:
  case q_smoking:
  case q_alcohol:
  case q_ppriority:
  case q_iiothers:
  case q_hidden:
  case q_cvisited:
  case q_timezone:
  case q_mstatus:
  case q_has_photo:
  case q_uses_apps:
  case q_pays_money:
  case q_gcountry:
  case q_custom1...q_custom15:

  case q_grp_id:
  case q_lang_id:
    if (Q->value == Q->value2 && Q->value != 0) {
      Q->hash = Q->hash2 = field_value_hash (Q->type, Q->value);
      r = get_word_count_nomult (Q->hash);
      Q->flags |= 4;
    }
    break;

  case q_uni_country:
  case q_uni_city:
  case q_univ:
  case q_faculty:
  case q_chair:
  case q_graduation:
  case q_edu_form:
  case q_edu_status:

  case q_sch_country:
  case q_sch_city:
  case q_sch_id:
  case q_sch_grad:
  case q_sch_class:

  case q_adr_country:
  case q_adr_city:
  case q_adr_district:
  case q_adr_station:
  case q_adr_street:
  case q_adr_type:

  case q_mil_unit:
  case q_mil_start:
  case q_mil_finish:

    if (Q->value == Q->value2 && Q->value != 0) {
      int t;
      Q->hash = Q->hash2 = field_value_hash (Q->type, Q->value);
      r = get_word_count_upperbound (Q->hash, &t);
      if (t) {
	Q->flags |= 4;
      }
    }
    break;

  case q_name:
    r = get_word_count_nomult (Q->hash);
    Q->flags |= 4;
    break;

  case q_name_interests:
    a = get_word_count_nomult (Q->hash);
    b = get_word_count_nomult (Q->hash2);
    r = a + b;
    if (!a || !b) {
      Q->flags |= 4;
    }
    break;

  case q_interests:
  case q_religion:
  case q_hometown:
  case q_proposal:
    r = get_word_count_nomult (Q->hash2);
    Q->flags |= 4;
    break;

  case q_job:
  case q_company_name:
  case q_sch_spec:
  case q_adr_house:
  case q_adr_name:
    r = get_word_count_upperbound (Q->hash2, &t);
    if (t && !(Q->flags & 1)) {
      Q->flags |= 4;
    }
    break;
  case q_education:
  case q_school:
  case q_address:
  case q_company:
  case q_military:
    Q->flags |= 2;
    r = estimate_query_complexity (Q->left, fa | 2);
    if (!IS_SMALL(r)) {
      r = GRAY;
    }
    //    Q->flags = Q->left->flags;
    //    if (Q->flags & 4) { Q->flags &= ~2; }
    break;
  default:
    assert (Q->type == 0xbeda);
    return -1;
  }
  if (r != GRAY && (Q->flags & 1)) {
    r = ~r;
  }
  return (Q->max_res = r);
}

static query_t *Qb[MAX_QUERY_NODES], **Qbw;

static void unpack_top_expr (query_t *Q, int op, int negate) {
  int nn = negate ^ (Q->flags & 1);
  if (Q->type == (op ^ nn)) {
    unpack_top_expr (Q->left, op, nn);
    unpack_top_expr (Q->right, op, nn);
    free_qnode (Q);
  } else {
    if (negate) {
      negate_query (Q);
    }
    if (Q->max_res != (op == q_and ? -1 : 0)) {
      assert (Qbw < Qb + MAX_QUERY_NODES);
      *Qbw++ = Q;
    } else {
      free_qnode (Q);
    }
  } 
}

void qb_sort (query_t **Qa, int b) {
  if (b <= 0) { return; }
  query_t *t;
  int i = 0, j = b;
  unsigned h = Qa[b >> 1]->max_res;
  int h2 = Qa[b >> 1]->complexity;
  do {
    while ((unsigned) Qa[i]->max_res < h || ((unsigned) Qa[i]->max_res == h && Qa[i]->complexity < h2)) {
      i++; 
    }
    while ((unsigned) Qa[j]->max_res > h || ((unsigned) Qa[j]->max_res == h && Qa[j]->complexity > h2)) { 
      j--; 
    }
    if (i <= j) {
      t = Qa[i];  Qa[i++] = Qa[j];  Qa[j--] = t;
    }
  } while (i <= j);
  qb_sort (Qa, j);
  qb_sort (Qa + i, b - i);
}

void qb_sort_rev (query_t **Qa, int b) {
  if (b <= 0) { return; }
  query_t *t;
  int i = 0, j = b;
  unsigned h = Qa[b >> 1]->max_res;
  int h2 = Qa[b >> 1]->complexity;
  do {
    while ((unsigned) Qa[i]->max_res > h || ((unsigned) Qa[i]->max_res == h && Qa[i]->complexity < h2)) {
      i++; 
    }
    while ((unsigned) Qa[j]->max_res < h || ((unsigned) Qa[j]->max_res == h && Qa[j]->complexity > h2)) { 
      j--; 
    }
    if (i <= j) {
      t = Qa[i];  Qa[i++] = Qa[j];  Qa[j--] = t;
    }
  } while (i <= j);
  qb_sort_rev (Qa, j);
  qb_sort_rev (Qa + i, b - i);
}

query_t *build_op_chain (int op, query_t **QA, int N) {
  assert (N >= 2);
  query_t *Q, *R, *P = new_qnode (op, 0);
  P->left = QA[N-2];
  P->right = QA[N-1];
  relax_max_res (P);
  Q = P;
  N -= 2;
  while (N) {
    R = new_qnode (op, 0);
    R->left = QA[--N];
    R->right = Q;
    Q = R;
    relax_max_res (Q);
  }
  Q->last = P;
  return Q;
}
    
query_t *optimize_query (query_t *Q) {
  int op, N, is_big, max_res = Q->max_res;
  if (IS_ALWAYS_FALSE (max_res)) {
    if (Q->type != q_false) {
      Q->type = q_false;
      Q->flags = 4;
      Q->complexity = 0;
    }
    return Q;
  }
  if (IS_ALWAYS_TRUE (max_res)) {
    if (Q->type != q_false) {
      Q->type = q_false;
      Q->flags = 5;
      Q->complexity = 0;
    }
    return Q;
  }
  if (Q->type == q_and || Q->type == q_or) {
    // fprintf (stderr, "optimizing: ");  print_query (Q);  fprintf (stderr, "\n");
    is_big = IS_BIG (max_res);
    op = Q->type ^ (Q->flags & 1) ^ is_big;
    Qbw = Qb;
    unpack_top_expr (Q, op, is_big);
    N = Qbw - Qb;
    assert (N > 0);
    if (N == 1) {
      Q = Qb[0];
      if (is_big) {
	negate_query (Q);
      }
      return optimize_query (Q);
    }

    // if not is_big: our expression is OP (Qb[0], ..., Qb[N-1])
    // if is_big:     our expression is NOT OP (Qb[0], ..., Qb[N-1])
    if (op == q_or) {
      qb_sort_rev (Qb, N - 1);	// OR: more probable conditions first, in case of a tie -- less complex first
      Q = build_op_chain (q_or, Qb, N);
    } else if (Q_IS_GRAYISH (Q)) {
      qb_sort (Qb, N - 1);	// AND: less probable conditions first, in case of a tie -- less complex first
      Q = build_op_chain (q_and, Qb, N);
    } else {
      qb_sort (Qb, N - 1);	// AND: less probable conditions first, in case of a tie -- less complex first

      int i = N;
      while (i > 0 && Q_IS_BIGGISH (Qb[i-1])) {
	i--;
      }
      assert (i > 0);
      if (i < N - 1) {
	Qb[i] = build_op_chain (q_and, Qb + i, N - i);
	N = i + 1;
	if (Q_IS_GRAYISH (Qb[i])) {
	  i++;
	}
      }
      int j = i;
      while (j > 0 && Q_IS_GRAYISH (Qb[j-1])) {
	j--;
      }
      assert (j > 0);
      if (j < i - 1) {
	Qb[j] = build_op_chain (q_and, Qb + j, i - j);
	if (i < N) {
	  Qb[j+1] = Qb[i];
	  N = j + 2;
	} else {
	  N = j + 1;
	}
      }
      Q = build_op_chain (q_and, Qb, N);
    }
    if (is_big) {
      negate_query (Q);
    }
    assert (Q->max_res == max_res);

    // fprintf (stderr, "intermediate result: ");  print_query (Q);  fprintf (stderr, "\n");

    query_t *P = Q;
    while (1) {
      assert (P->type == op);
      P->left = optimize_query (P->left);
      if (P == Q->last) {
	break;
      }
      P = P->right;
    }
    P->right = optimize_query (P->right);

    // fprintf (stderr, "final result: ");  print_query (Q);  fprintf (stderr, "\n");
    return Q;
  }
  if (Q->flags & 2) {
    Q->left = optimize_query (Q->left);
    if ((Q->left->flags & 5) == 4) {
      Q->flags |= 4;
    }
    return Q;
  }
  return Q;
}

static void print_query (query_t *Q) {
  int op = Q->type;
  switch (Q->type) {
  case q_and:
  case q_or:
    fprintf (stderr, "(%s%s:%d,%d ", Q->flags & 1 ? "!" : "", Q->type == q_and ? "and" : "or", Q->max_res, Q->complexity);
    while (Q->type == op) {
      print_query (Q->left);
      fprintf (stderr, " ");
      Q = Q->right;
      if (Q->flags & 1) {
	break;
      }
    }
    print_query (Q);
    fprintf (stderr, ")");
    return;
  case q_false:
    fprintf (stderr, "(%sfalse:%d,%d)", Q->flags & 1 ? "!" : "", Q->max_res, Q->complexity);
    return;
  case q_true:
    fprintf (stderr, "(%strue:%d,%d)", Q->flags & 1 ? "!" : "", Q->max_res, Q->complexity);
    return;
  case q_not:
    fprintf (stderr, "(not ");
    print_query (Q->left);
    fprintf (stderr, ")");
    return;
  }
  struct query_keyword_descr *KW;
  for (KW = QueryKW; KW->q_type; KW++) {
    if (KW->q_type == op) {
      break;
    }
  }
  assert (KW->q_type);
  switch (KW->flags & 7) {
  case 0:
    fprintf (stderr, "(%s%s%d:%d,%d %d)", Q->flags & 1 ? "!" : "", KW->str, op, Q->max_res, Q->complexity, Q->value);
    break;
  case 1:
    fprintf (stderr, "(%s%s%d:%d,%d %d %d)", Q->flags & 1 ? "!" : "", KW->str, op, Q->max_res, Q->complexity, Q->value, Q->value2);
    break;
  case 2:
    fprintf (stderr, "(%s%s%d:%d,%d)", Q->flags & 1 ? "!" : "", KW->str, op, Q->max_res, Q->complexity);
    break;
  case 3:
    fprintf (stderr, "(%s%s%d:%d,%d <str>)", Q->flags & 1 ? "!" : "", KW->str, op, Q->max_res, Q->complexity);
    break;
  case 7:
    assert (Q->flags & 2);
    fprintf (stderr, "(%s%s%d:%d,%d ", Q->flags & 1 ? "!" : "", KW->str, op, Q->max_res, Q->complexity);
    print_query (Q->left);
    break;
  default:
    assert (0);
  }
}

/* ------------- checking condition for one user (for complete scanning) ----------- */

#define in_hashlist	in_hashlist_local

static inline int in_hashlist (hash_t x, hash_list_t *L) {
  int a, b, c;
  if (!L) { return 0; }
  a = -1;  b = L->len;
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (L->A[c] < x) {
      a = c;
    } else if (L->A[c] > x) {
      b = c;
    } else {
      return c + 1;
    }
  }
  return 0;
}

int in_array (int x, int List[], int len) {
  int a = -1, b = len, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (List[c] <= x) {
      a = c;
    } else {
      b = c;
    }
  }
  if (a >= 0 && List[a] == x) {
    return a;
  } else {
    return -1;
  }
}

int in_short_array (int x, short List[], int len) {
  int a = -1, b = len, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (List[c] <= x) {
      a = c;
    } else {
      b = c;
    }
  }
  if (a >= 0 && List[a] == x) {
    return a;
  } else {
    return -1;
  }
}

#if DETERMINISTIC_RANDOM

static unsigned rnd_seed_a, rnd_seed_b;

static void user_randomize (unsigned seed) {
  seed *= 2654435773U;
  seed /= 42950;
  rnd_seed_a = 7 + 10 * (seed % 100);
  rnd_seed_b = (seed / 100) % 1000;
}

static int user_random (user_t *U) {
  return ((unsigned) U->uid * rnd_seed_a + rnd_seed_b) % 1000;
}

#else

static int cur_rnd_run, cur_rnd_num;

static void user_randomize (unsigned seed) {
  if (cur_rnd_run < 10000 && now) { 
    cur_rnd_run = now; 
  }
  cur_rnd_run++;
  cur_rnd_num = 0;
}

static int user_random (user_t *U) {
  int x = U->uid;
  int y = cur_rnd_run;
  unsigned long long z = 17*x + 239*y + 563 * cur_rnd_num + 13;
  cur_rnd_num = z;
  return z % 1000;
}

#endif

/* ================ condition compiler ================== */

/* -------------- condition interpreter ----------------- */

#define CONDITION_ARGS	condition_t Cond, user_t *U, void *X, int uid

typedef struct condition *condition_t;

typedef int (*condition_func_t)(CONDITION_ARGS);

struct condition {
  condition_func_t eval;
  condition_t true_branch;
  condition_t false_branch;
  int val, val2;
};
  
struct condition_basic {
  condition_func_t eval;
  condition_t true_branch;
  condition_t false_branch;
};
  
struct condition_with_hash {
  condition_func_t eval;
  condition_t true_branch;
  condition_t false_branch;
  hash_t word;
  hash_t word2;
};

#define DECLARE_CHECK(__name)	static int check_##__name (CONDITION_ARGS)

#define RETURN_TRUE	return Cond->true_branch->eval (Cond->true_branch, U, X, uid);
#define RETURN_FALSE	return Cond->false_branch->eval (Cond->false_branch, U, X, uid);
#define RETURN_COND(__expr)	\
			Cond = (__expr) ? Cond->true_branch : Cond->false_branch; \
			return Cond->eval (Cond, U, X, uid);

#define DEFINE_COND_SPECIAL(__name,__expr) \
	DECLARE_CHECK (__name##_eq) { \
	  RETURN_COND ((__expr)); \
	}

#define DEFINE_COND_EQ(__name,__expr) \
	DECLARE_CHECK (__name##_eq) { \
	  RETURN_COND ((__expr) == Cond->val); \
	}

#define DEFINE_COND_RANGE(__name,__expr) \
	DEFINE_COND_EQ (__name, __expr) \
	DECLARE_CHECK (__name##_leq) { \
	  RETURN_COND ((__expr) <= Cond->val); \
	} \
	DECLARE_CHECK (__name##_geq) { \
	  RETURN_COND ((__expr) >= Cond->val); \
	} \
	DECLARE_CHECK (__name##_range) { \
	  int t = (__expr); \
	  RETURN_COND (t >= Cond->val && t <= Cond->val2); \
	} \
	DECLARE_CHECK (__name##_pos_leq) { \
	  int t = (__expr); \
	  RETURN_COND (t > 0 && t <= Cond->val); \
	}

#define DEFINE_COND_WORD(__name,__expr) \
	DECLARE_CHECK (__name##_word) { \
	  hash_list_t *List = (__expr); \
	  RETURN_COND (List && in_hashlist (((struct condition_with_hash *) Cond)->word, List)); \
	}

#define DEFINE_QUANTIFIER(__name,__first,__next) \
	DECLARE_CHECK (__name##_first) { \
	  X = __first; \
	  RETURN_COND (X); \
	} \
	DECLARE_CHECK (__name##_next) { \
	  X = __next; \
	  RETURN_COND (X); \
	}

DECLARE_CHECK (true) {
  return 1;
}

DECLARE_CHECK (false) {
  return 0;
}

struct condition trueCondition = { .eval = check_true };
struct condition falseCondition = { .eval = check_false };

#define	CONDITION_TRUE	&trueCondition
#define	CONDITION_FALSE	&falseCondition

DEFINE_COND_RANGE (random, user_random (U));
DEFINE_COND_RANGE (id, U->user_id);
DEFINE_COND_EQ (country, U->uni_country);
DEFINE_COND_EQ (city, U->uni_city);
DEFINE_COND_RANGE (bday, U->bday_day);
DEFINE_COND_RANGE (bmonth, U->bday_month);
DEFINE_COND_RANGE (byear, U->bday_year);
DEFINE_COND_EQ (political, U->political);
DEFINE_COND_EQ (sex, U->sex);
DEFINE_COND_EQ (operator, U->operator);
DEFINE_COND_RANGE (browser, U->browser);
DEFINE_COND_EQ (region, U->region);
DEFINE_COND_RANGE (height, U->height);
DEFINE_COND_EQ (smoking, U->smoking);
DEFINE_COND_EQ (alcohol, U->alcohol);
DEFINE_COND_EQ (ppriority, U->ppriority);
DEFINE_COND_EQ (iiothers, U->iiothers);
DEFINE_COND_RANGE (hidden, U->hidden);
DEFINE_COND_RANGE (cvisited, U->cvisited);
DEFINE_COND_EQ (gcountry, U->gcountry);
DEFINE_COND_RANGE (custom1, U->custom1);
DEFINE_COND_RANGE (custom2, U->custom2);
DEFINE_COND_RANGE (custom3, U->custom3);
DEFINE_COND_RANGE (custom4, U->custom4);
DEFINE_COND_RANGE (custom5, U->custom5);
DEFINE_COND_RANGE (custom6, U->custom6);
DEFINE_COND_RANGE (custom7, U->custom7);
DEFINE_COND_RANGE (custom8, U->custom8);
DEFINE_COND_RANGE (custom9, U->custom9);
DEFINE_COND_RANGE (custom10, U->custom10);
DEFINE_COND_RANGE (custom11, U->custom11);
DEFINE_COND_RANGE (custom12, U->custom12);
DEFINE_COND_RANGE (custom13, U->custom13);
DEFINE_COND_RANGE (custom14, U->custom14);
DEFINE_COND_RANGE (custom15, U->custom15);
DEFINE_COND_RANGE (timezone, U->timezone);
DEFINE_COND_EQ (mstatus, U->mstatus);
DEFINE_COND_SPECIAL (grp_type, (U->user_group_types[Cond->val >> 5] >> (Cond->val & 31)) & 1);  
DEFINE_COND_SPECIAL (grp_id, U->grp && in_array (Cond->val, U->grp->G, U->grp->cur_groups) >= 0);
DEFINE_COND_SPECIAL (lang_id, U->langs && in_short_array (Cond->val, U->langs->L, U->langs->cur_langs) >= 0);
DEFINE_COND_WORD (name, U->name_hashes);
DEFINE_COND_WORD (interests, U->inter_hashes);
DEFINE_COND_WORD (religion, U->religion_hashes);
DEFINE_COND_WORD (hometown, U->hometown_hashes);
DEFINE_COND_WORD (proposal, U->proposal_hashes);
DEFINE_COND_RANGE (online, U->last_visited);
DEFINE_COND_EQ (privacy, U->privacy);
DEFINE_COND_EQ (has_photo, U->has_photo & 0x81);
DEFINE_COND_EQ (uses_apps, (U->has_photo & 0x82) >> 1);
DEFINE_COND_EQ (pays_money, (U->has_photo & 0x84) >> 2);

#define Education ((struct education *) X)
DEFINE_QUANTIFIER (education, U->edu, Education->next);
DEFINE_COND_EQ (uni_country, Education->country);
DEFINE_COND_EQ (uni_city, Education->city);
DEFINE_COND_EQ (univ, Education->university);
DEFINE_COND_EQ (faculty, Education->faculty);
DEFINE_COND_EQ (chair, Education->chair);
DEFINE_COND_RANGE (graduation, Education->grad_year);
DEFINE_COND_EQ (edu_form, Education->edu_form);
DEFINE_COND_EQ (edu_status, Education->edu_status);
#undef Education

#define Military ((struct military *) X)
DEFINE_QUANTIFIER (military, U->mil, Military->next);
DEFINE_COND_EQ (mil_unit, Military->unit_id);
DEFINE_COND_RANGE (mil_start, Military->start);
DEFINE_COND_RANGE (mil_finish, Military->finish);
#undef Military

#define Address ((struct address *) X)
DEFINE_QUANTIFIER (address, U->addr, Address->next);
DEFINE_COND_EQ (adr_country, Address->country);
DEFINE_COND_EQ (adr_city, Address->city);
DEFINE_COND_EQ (adr_district, Address->district);
DEFINE_COND_EQ (adr_station, Address->station);
DEFINE_COND_EQ (adr_street, Address->street);
DEFINE_COND_EQ (adr_type, Address->atype);
DEFINE_COND_WORD (adr_house, Address->house_hashes);
DEFINE_COND_WORD (adr_name, Address->name_hashes);
#undef Address

#define Company ((struct company *) X)
DEFINE_QUANTIFIER (company, U->work, Company->next);
DEFINE_COND_WORD (job, Company->job_hashes);
DEFINE_COND_WORD (company_name, Company->name_hashes);
#undef Company

#define School ((struct school *) X)
DEFINE_QUANTIFIER (school, U->sch, School->next);
DEFINE_COND_EQ (sch_country, School->country);
DEFINE_COND_EQ (sch_city, School->city);
DEFINE_COND_EQ (sch_id, School->school);
DEFINE_COND_RANGE (sch_grad, School->grad);
DEFINE_COND_EQ (sch_class, School->sch_class);
DEFINE_COND_WORD (sch_spec, School->spec_hashes);
#undef School

DECLARE_CHECK (birthday_soon_eq) {
  if (U->bday_month == global_birthday_left_month) {
    if (U->bday_day < global_birthday_left_day_min || U->bday_day > global_birthday_left_day_max) {
      RETURN_FALSE;
    }
  } else if (U->bday_month == global_birthday_right_month) {
    if (U->bday_day < global_birthday_right_day_min || U->bday_day > global_birthday_right_day_max) {
      RETURN_FALSE;
    }
  } else {
    RETURN_FALSE;
  }
  int x = Cond->val;
  int i;
  for (i = 0; i < 3; i++) {
    if (U->bday_day == global_birthday_day[i+x] && U->bday_month == global_birthday_month[i+x]) {
      RETURN_COND (global_birthday_shift_left[i] <= U->timezone && U->timezone <= global_birthday_shift_right[i]);
    }
  }
  RETURN_FALSE;
}

DECLARE_CHECK (age_eq) {
  if (U->bday_year == global_age_year - Cond->val) {
    RETURN_COND (U->bday_month < global_age_month || (U->bday_month == global_age_month && U->bday_day <= global_age_day));
  }
  if (U->bday_year == global_age_year - Cond->val - 1) {
    RETURN_COND (U->bday_month > global_age_month || (U->bday_month == global_age_month && U->bday_day > global_age_day));
  }
  RETURN_FALSE;
}

DECLARE_CHECK (age_leq) {
  if (U->bday_year >= global_age_year - Cond->val) {
    RETURN_TRUE;
  }
  if (U->bday_year == global_age_year - Cond->val - 1) {
    RETURN_COND (U->bday_month > global_age_month || (U->bday_month == global_age_month && U->bday_day > global_age_day));
  }
  RETURN_FALSE;
}

DECLARE_CHECK (age_geq) {
  if (!U->bday_year) {
    RETURN_FALSE;
  }
  if (U->bday_year < global_age_year - Cond->val) {
    RETURN_TRUE;
  }
  if (U->bday_year == global_age_year - Cond->val) {
    RETURN_COND (U->bday_month < global_age_month || (U->bday_month == global_age_month && U->bday_day <= global_age_day));
  }
  RETURN_FALSE;
}

DECLARE_CHECK (age_range) {
  if (U->bday_year > global_age_year - Cond->val) {
    RETURN_FALSE;
  }
  if (U->bday_year < global_age_year - Cond->val2 - 1) {
    RETURN_FALSE;
  }
  if (U->bday_year == global_age_year - Cond->val2 - 1) {
    RETURN_COND (U->bday_month > global_age_month || (U->bday_month == global_age_month && U->bday_day > global_age_day));
  }
  if (U->bday_year == global_age_year - Cond->val) {
    RETURN_COND (U->bday_month < global_age_month || (U->bday_month == global_age_month && U->bday_day <= global_age_day));
  }
  RETURN_TRUE;
}

DECLARE_CHECK (age_pos_leq) {
  if (U->bday_year >= global_age_year - Cond->val) {
    RETURN_TRUE;
  }
  if (U->bday_year == global_age_year - Cond->val - 1) {
    RETURN_COND (U->bday_month > global_age_month || (U->bday_month == global_age_month && U->bday_day > global_age_day));
  }
  RETURN_FALSE;
}

DECLARE_CHECK (future_age_eq) {
  if (U->bday_year == global_future_age_year - Cond->val) {
    RETURN_COND (U->bday_month < global_future_age_month || (U->bday_month == global_future_age_month && U->bday_day <= global_future_age_day));
  }
  if (U->bday_year == global_future_age_year - Cond->val - 1) {
    RETURN_COND (U->bday_month > global_future_age_month || (U->bday_month == global_future_age_month && U->bday_day > global_future_age_day));
  }
  RETURN_FALSE;
}

DECLARE_CHECK (future_age_leq) {
  if (U->bday_year >= global_future_age_year - Cond->val) {
    RETURN_TRUE;
  }
  if (U->bday_year == global_future_age_year - Cond->val - 1) {
    RETURN_COND (U->bday_month > global_future_age_month || (U->bday_month == global_future_age_month && U->bday_day > global_future_age_day));
  }
  RETURN_FALSE;
}

DECLARE_CHECK (future_age_geq) {
  if (!U->bday_year) {
    RETURN_FALSE;
  }
  if (U->bday_year < global_future_age_year - Cond->val) {
    RETURN_TRUE;
  }
  if (U->bday_year == global_future_age_year - Cond->val) {
    RETURN_COND (U->bday_month < global_future_age_month || (U->bday_month == global_future_age_month && U->bday_day <= global_future_age_day));
  }
  RETURN_FALSE;
}

DECLARE_CHECK (future_age_range) {
  if (U->bday_year > global_future_age_year - Cond->val) {
    RETURN_FALSE;
  }
  if (U->bday_year < global_future_age_year - Cond->val2 - 1) {
    RETURN_FALSE;
  }
  if (U->bday_year == global_future_age_year - Cond->val2 - 1) {
    RETURN_COND (U->bday_month > global_future_age_month || (U->bday_month == global_future_age_month && U->bday_day > global_future_age_day));
  }
  if (U->bday_year == global_future_age_year - Cond->val) {
    RETURN_COND (U->bday_month < global_future_age_month || (U->bday_month == global_future_age_month && U->bday_day <= global_future_age_day));
  }
  RETURN_TRUE;
}

DECLARE_CHECK (future_age_pos_leq) {
  if (U->bday_year >= global_future_age_year - Cond->val) {
    RETURN_TRUE;
  }
  if (U->bday_year == global_future_age_year - Cond->val - 1) {
    RETURN_COND (U->bday_month > global_future_age_month || (U->bday_month == global_future_age_month && U->bday_day > global_future_age_day));
  }
  RETURN_FALSE;
}

DECLARE_CHECK (name_interests_word) {
  RETURN_COND ((U->name_hashes && in_hashlist (((struct condition_with_hash *) Cond)->word, U->name_hashes)) 
	|| (U->inter_hashes && in_hashlist (((struct condition_with_hash *) Cond)->word2, U->inter_hashes)));
}

DECLARE_CHECK (user_inlist) {
  //  vkprintf (2, "checking whether uid %d is in list\n", uid);
  long a = -1, b = aux_userlist_size, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (aux_userlist[c] <= uid) {
      a = c;
    } else {
      b = c;
    }
  }
  RETURN_COND (a >= 0 && aux_userlist[a] == uid);
}

condition_t CurrentCond;

inline int user_matches_condition (user_t *U, condition_t C, int uid) {
  return C->eval (C, U, 0, uid);
}

inline int user_matches (user_t *U) {
  return user_matches_condition (U, CurrentCond, U->uid);
}

/* ------------ building condition from query ----------- */

static condition_t compile_condition_inlist (condition_t true_branch, condition_t false_branch, int opt_level) {
  condition_t A = zmalloc (offsetof (struct condition, val));
  A->eval = check_user_inlist;
  A->true_branch = true_branch;
  A->false_branch = false_branch;
  return A;
}


static condition_t compile_condition_eq (int value, condition_t true_branch, condition_t false_branch, condition_func_t check_eq) {
  condition_t A = zmalloc (offsetof (struct condition, val2));
  A->eval = check_eq;
  A->true_branch = true_branch;
  A->false_branch = false_branch;
  A->val = value;
  return A;
}

static condition_t compile_condition_range (int min_value, int max_value, condition_t true_branch, condition_t false_branch, condition_func_t check_eq, condition_func_t check_leq, condition_func_t check_geq, condition_func_t check_range, condition_func_t check_pos_leq) {
  condition_t A = zmalloc (min_value == max_value || min_value == 1 || min_value == MININT || max_value == MAXINT ? offsetof (struct condition, val2) : sizeof (struct condition));
  A->true_branch = true_branch;
  A->false_branch = false_branch;
  if (min_value == max_value) {
    A->eval = check_eq;
    A->val = min_value;
  } else if (min_value == MININT) {
    A->eval = check_leq;
    A->val = max_value;
  } else if (max_value == MAXINT) {
    A->eval = check_geq;
    A->val = min_value;
  } else if (min_value == 1) {
    A->eval = check_pos_leq;
    A->val = max_value;
  } else {
    A->eval = check_range;
    A->val = min_value;
    A->val2 = max_value;
  }
  return A;
}

static condition_t compile_condition_word (hash_t word, condition_t true_branch, condition_t false_branch, condition_func_t check_word) {
  struct condition_with_hash *A = zmalloc (offsetof (struct condition_with_hash, word2));
  A->eval = check_word;
  A->true_branch = true_branch;
  A->false_branch = false_branch;
  A->word = word;
  return (condition_t) A;
}

static condition_t compile_condition_twowords (hash_t word, hash_t word2, condition_t true_branch, condition_t false_branch, condition_func_t check_word) {
  struct condition_with_hash *A = zmalloc (sizeof (struct condition_with_hash));
  A->eval = check_word;
  A->true_branch = true_branch;
  A->false_branch = false_branch;
  A->word = word;
  A->word2 = word2;
  return (condition_t) A;
}

#define COMPILE_QUANTIFIER(__name) \
	case q_##__name: \
	  quantifier_first = check_##__name##_first; \
	  quantifier_next = check_##__name##_next; \
	  break;

#define COMPILE_COND_EQ(__name) \
	case q_##__name: \
	  return compile_condition_eq (Q->value, true_branch, false_branch, check_##__name##_eq); 

#define COMPILE_COND_RANGE(__name) \
	case q_##__name: \
	  return compile_condition_range (Q->value, Q->value2, true_branch, false_branch, check_##__name##_eq, check_##__name##_leq, check_##__name##_geq, check_##__name##_range, check_##__name##_pos_leq); 

#define COMPILE_COND_WORD(__name) \
	case q_##__name: \
	  return compile_condition_word (Q->hash2, true_branch, false_branch, check_##__name##_word); 

#define COMPILE_COND_WORD_NOSTEM(__name) \
	case q_##__name: \
	  return compile_condition_word (Q->hash, true_branch, false_branch, check_##__name##_word); 

#define COMPILE_COND_TWOWORDS(__name) \
	case q_##__name: \
	  return compile_condition_twowords (Q->hash, Q->hash2, true_branch, false_branch, check_##__name##_word); 

condition_t build_condition (query_t *Q, condition_t true_branch, condition_t false_branch, int opt_level) {
  if (Q->flags & 1) {
    condition_t tmp = true_branch;
    true_branch = false_branch;
    false_branch = tmp;
  }
  if (Q->flags & 2) {
    condition_func_t quantifier_first, quantifier_next;
    switch (Q->type) {
      COMPILE_QUANTIFIER (education);
      COMPILE_QUANTIFIER (military);
      COMPILE_QUANTIFIER (address);
      COMPILE_QUANTIFIER (company);
      COMPILE_QUANTIFIER (school);
    default:
      assert (0);
    }
    condition_t A = zmalloc (sizeof (struct condition_basic));
    condition_t B = zmalloc (sizeof (struct condition_basic));
    A->eval = quantifier_first;
    B->eval = quantifier_next;
    A->false_branch = B->false_branch = false_branch;
    A->true_branch = B->true_branch = build_condition (Q->left, true_branch, B, opt_level);
    return A;
  }

  switch (Q->type) {
  case q_and:
    return build_condition (Q->left, build_condition (Q->right, true_branch, false_branch, opt_level), false_branch, opt_level);
  case q_or:
    return build_condition (Q->left, true_branch, build_condition (Q->right, true_branch, false_branch, opt_level), opt_level);
  case q_not:
    return build_condition (Q->left, false_branch, true_branch, opt_level);
  case q_false:
    return false_branch;
  case q_true:
    return true_branch;
  case q_inlist:
    return compile_condition_inlist (true_branch, false_branch, opt_level);
  case q_birthday_today:
  case q_birthday_tomorrow:
    Q->value = Q->type - q_birthday_today;
    COMPILE_COND_EQ (birthday_soon);

    COMPILE_COND_RANGE (random);
    COMPILE_COND_RANGE (id);
    COMPILE_COND_EQ (country);
    COMPILE_COND_EQ (city);
    COMPILE_COND_RANGE (bday);
    COMPILE_COND_RANGE (bmonth);
    COMPILE_COND_RANGE (byear);
    COMPILE_COND_RANGE (age);
    COMPILE_COND_RANGE (future_age);
    COMPILE_COND_EQ (political);
    COMPILE_COND_EQ (sex);
    COMPILE_COND_EQ (operator);
    COMPILE_COND_RANGE (browser);
    COMPILE_COND_EQ (region);
    COMPILE_COND_RANGE (height);
    COMPILE_COND_EQ (smoking);
    COMPILE_COND_EQ (alcohol);
    COMPILE_COND_EQ (ppriority);
    COMPILE_COND_EQ (iiothers);
    COMPILE_COND_RANGE (hidden);
    COMPILE_COND_RANGE (cvisited);
    COMPILE_COND_EQ (gcountry);
    COMPILE_COND_RANGE (custom1);
    COMPILE_COND_RANGE (custom2);
    COMPILE_COND_RANGE (custom3);
    COMPILE_COND_RANGE (custom4);
    COMPILE_COND_RANGE (custom5);
    COMPILE_COND_RANGE (custom6);
    COMPILE_COND_RANGE (custom7);
    COMPILE_COND_RANGE (custom8);
    COMPILE_COND_RANGE (custom9);
    COMPILE_COND_RANGE (custom10);
    COMPILE_COND_RANGE (custom11);
    COMPILE_COND_RANGE (custom12);
    COMPILE_COND_RANGE (custom13);
    COMPILE_COND_RANGE (custom14);
    COMPILE_COND_RANGE (custom15);
    COMPILE_COND_RANGE (timezone);
    COMPILE_COND_EQ (mstatus);
    COMPILE_COND_EQ (grp_type);  
    COMPILE_COND_EQ (grp_id);
    COMPILE_COND_EQ (lang_id);
    COMPILE_COND_WORD_NOSTEM (name);
    COMPILE_COND_WORD (interests);
    COMPILE_COND_TWOWORDS (name_interests);
    COMPILE_COND_WORD (religion);
    COMPILE_COND_WORD (hometown);
    COMPILE_COND_WORD (proposal);
    COMPILE_COND_RANGE (online);
    COMPILE_COND_EQ (privacy);
    COMPILE_COND_EQ (has_photo);
    COMPILE_COND_EQ (uses_apps);
    COMPILE_COND_EQ (pays_money);

    COMPILE_COND_EQ (uni_country);
    COMPILE_COND_EQ (uni_city);
    COMPILE_COND_EQ (univ);
    COMPILE_COND_EQ (faculty);
    COMPILE_COND_EQ (chair);
    COMPILE_COND_RANGE (graduation);
    COMPILE_COND_EQ (edu_form);
    COMPILE_COND_EQ (edu_status);

    COMPILE_COND_EQ (mil_unit);
    COMPILE_COND_RANGE (mil_start);
    COMPILE_COND_RANGE (mil_finish);

    COMPILE_COND_EQ (adr_country);
    COMPILE_COND_EQ (adr_city);
    COMPILE_COND_EQ (adr_district);
    COMPILE_COND_EQ (adr_station);
    COMPILE_COND_EQ (adr_street);
    COMPILE_COND_EQ (adr_type);
    COMPILE_COND_WORD (adr_house);
    COMPILE_COND_WORD (adr_name);

    COMPILE_COND_WORD (job);
    COMPILE_COND_WORD (company_name);

    COMPILE_COND_EQ (sch_country);
    COMPILE_COND_EQ (sch_city);
    COMPILE_COND_EQ (sch_id);
    COMPILE_COND_RANGE (sch_grad);
    COMPILE_COND_EQ (sch_class);
    COMPILE_COND_WORD (sch_spec);

  default:
    assert (0);
  }
}

inline condition_t build_condition_from_query (query_t *Q, int opt_level) {
  return build_condition (Q, CONDITION_TRUE, CONDITION_FALSE, opt_level);
}

/* ================ iterator machinery ================== */

/* ----------------- empty iterator ---------------------- */

int empty_iterator_jump_to (iterator_t I, int req_pos) {
  return INFTY;
}

void init_empty_iterator (iterator_t I) {
  I->jump_to = empty_iterator_jump_to;
  I->pos = INFTY;
}

iterator_t build_empty_iterator (void) {
  iterator_t I = zmalloc (sizeof (struct iterator));
  init_empty_iterator (I);
  return I;
}

/* ------------ basic (atomic) iterators ----------------- */

#define MAX_DEPTH	125

struct tree_subiterator {
  int pos;
  int mult;
  int sp;
  treeref_t S[MAX_DEPTH];
};

struct wordlist_subiterator {
  int pos;
  int mult;
  unsigned char *data_end;
  struct mlist_decoder *mdec;
};


struct tree_iterator {
  iterator_jump_func_t jump_to;
  int pos, mult;
  struct tree_subiterator TS;
};

struct wordlist_iterator {
  iterator_jump_func_t jump_to;
  int pos, mult;
  struct wordlist_subiterator WS;
};

struct wordlist_tree_iterator {
  iterator_jump_func_t jump_to;
  int pos, mult;
  struct wordlist_subiterator WS;
  struct tree_subiterator TS;
};


struct everything_iterator {
  iterator_jump_func_t jump_to;
  int pos;
};

void init_tree_subiterator (struct tree_subiterator *TI, treeref_t tree);
void init_wordlist_subiterator (struct wordlist_subiterator *WI, unsigned char *data, int len);

int tree_iterator_jump_to (iterator_t I, int req_pos);
int wordlist_iterator_jump_to (iterator_t I, int req_pos);
int wordlist_tree_iterator_jump_to (iterator_t I, int req_pos);
int everything_iterator_jump_to (iterator_t I, int req_pos);

iterator_t build_word_iterator (hash_t word);
iterator_t build_everything_iterator (void);

void init_tree_subiterator (struct tree_subiterator *TI, treeref_t tree) {
  assert (tree);
  long i = 0;
  treespace_t TS = WordSpace;
  struct intree_node *TC;
  do {
    TC = TS_NODE (tree);
    TI->S[i++] = tree;
    tree = TC->left;
  } while (tree);
  TI->pos = TC->x;
  TI->mult = TC->z;
  TI->sp = i;
  assert (i <= MAX_DEPTH);
}

static inline int tree_subiterator_next (struct tree_subiterator *TI) {
  assert (TI->sp > 0);
  long i = TI->sp;
  treespace_t TS = WordSpace;
  treeref_t T = TI->S[--i];
  struct intree_node *TC = TS_NODE (T);
  T = TC->right;
  while (T) {
    TC = TS_NODE (T);
    TI->S[i++] = T;
    T = TC->left;
  }
  assert (i <= MAX_DEPTH);
  TI->sp = i;
  if (!i) {
    return TI->pos = INFTY;
  }
  TC = TS_NODE (TI->S[i - 1]);
  TI->mult = TC->z;
  return TI->pos = TC->x;
}
  
int tree_subiterator_jump_to (struct tree_subiterator *TI, int req_pos) {
  if (req_pos == TI->pos + 1) {
    return tree_subiterator_next (TI);
  }
  assert (req_pos > TI->pos && TI->sp > 0);
  long i = TI->sp;
  treespace_t TS = WordSpace;
  struct intree_node *TC;
  treeref_t T;
  while (i > 1 && TS_NODE (TI->S[i-2])->x <= req_pos) {
    i--;
  }
  TC = TS_NODE (TI->S[i-1]);
  if (TC->x == req_pos) {
    TI->sp = i;
    TI->mult = TC->z;
    return TI->pos = req_pos;
  }
  i--;
  T = TC->right;
  while (T) {
    TC = TS_NODE (T);
    if (req_pos < TC->x) {
      TI->S[i++] = T;
      T = TC->left;
    } else if (req_pos == TC->x) {
      TI->S[i++] = T;
      TI->sp = i;
      TI->mult = TC->z;
      return TI->pos = req_pos;
    } else {
      T = TC->right;
    }
  }
  assert (i <= MAX_DEPTH);
  TI->sp = i;
  if (!i) {
    return TI->pos = INFTY;
  } 
  TC = TS_NODE (TI->S[i - 1]);
  TI->mult = TC->z;
  return TI->pos = TC->x;
}

void init_wordlist_subiterator (struct wordlist_subiterator *WI, unsigned char *data, int len) {
  WI->mdec = zmalloc_mlist_decoder (idx_max_uid + 1, -1, data, 0, INTERPOLATIVE_CODE_JUMP_SIZE);
  WI->data_end = data + len;
  WI->pos = mlist_decode_pair (WI->mdec, &WI->mult);
}

static inline int wordlist_subiterator_next (struct wordlist_subiterator *WI) {
  int res = mlist_decode_pair (WI->mdec, &WI->mult);
  return WI->pos = (res < 0x7fffffff ? res : INFTY);
}

int wordlist_subiterator_jump_to (struct wordlist_subiterator *WI, int req_pos) {
  if (req_pos == WI->pos + 1) {
    return wordlist_subiterator_next (WI);
  } else {
    assert (req_pos > WI->pos);
    int res = mlist_forward_decode_idx (WI->mdec, req_pos, &WI->mult);
    return WI->pos = (res < 0x7fffffff ? res : INFTY);
  }
}


int tree_iterator_jump_to (iterator_t I, int req_pos) {
  struct tree_iterator *TI = (struct tree_iterator *) I;
  int res = tree_subiterator_jump_to (&TI->TS, req_pos);
  if (res == INFTY) {
    TI->jump_to = empty_iterator_jump_to;
  } else {
    assert ((TI->mult = TI->TS.mult) > 0);
  }
  return TI->pos = res;
}

int wordlist_iterator_jump_to (iterator_t I, int req_pos) {
  struct wordlist_iterator *WI = (struct wordlist_iterator *) I;
  int res = wordlist_subiterator_jump_to (&WI->WS, req_pos);
  if (res == INFTY) {
    WI->jump_to = empty_iterator_jump_to;
  } else {
    assert ((WI->mult = WI->WS.mult) > 0);
  }
  return WI->pos = res;
}

int wordlist_tree_iterator_jump_to (iterator_t I, int req_pos) {
  struct wordlist_tree_iterator *WTI = (struct wordlist_tree_iterator *) I;
  int a = WTI->TS.pos;
  if (a < req_pos) {
    a = tree_subiterator_jump_to (&WTI->TS, req_pos);
  }
  int b = WTI->WS.pos;
  if (b < req_pos) {
    b = wordlist_subiterator_jump_to (&WTI->WS, req_pos);
  }
  while (1) {
    if (a < b) {
      assert ((WTI->mult = WTI->TS.mult) > 0);
      return WTI->pos = a;
    } else if (a > b) {
      assert ((WTI->mult = WTI->WS.mult) > 0);
      return WTI->pos = b;
    } else if (a == INFTY) {
      WTI->jump_to = empty_iterator_jump_to;
      return WTI->pos = a;
    } else if (WTI->WS.mult + WTI->TS.mult > 0) {
      WTI->mult = WTI->WS.mult + WTI->TS.mult;
      return WTI->pos = a;
    }
    a = tree_subiterator_next (&WTI->TS);
    b = wordlist_subiterator_next (&WTI->WS);
  }
}


int everything_iterator_jump_to (iterator_t I, int req_pos) {
  assert (req_pos > I->pos);
  while (req_pos <= max_uid && !User[req_pos]) {
    ++req_pos;
  }
  if (req_pos > max_uid) {
    I->jump_to = empty_iterator_jump_to;
    return I->pos = INFTY;
  }
  return I->pos = req_pos;
}

iterator_t build_word_iterator (hash_t word) {
  struct hash_word *W = get_hash_node (word, 0);
  treeref_t tree = W ? W->word_tree : 0;
  int len;
  unsigned char *data = idx_word_lookup (word, &len);
  assert (tree || data);
  if (!tree && !data) {
    return build_empty_iterator ();
  }
  if (!data) {
    struct tree_iterator *I = zmalloc (sizeof (struct tree_iterator));
    init_tree_subiterator (&I->TS, tree);
    assert ((I->mult = I->TS.mult) > 0);
    I->pos = I->TS.pos;
    I->jump_to = tree_iterator_jump_to;
    return (iterator_t) I;
  }
  if (!tree) {
    struct wordlist_iterator *I = zmalloc (sizeof (struct wordlist_iterator));
    init_wordlist_subiterator (&I->WS, data, len);
    assert ((I->mult = I->WS.mult) > 0);
    I->pos = I->WS.pos;
    I->jump_to = wordlist_iterator_jump_to;
    return (iterator_t) I;
  }
  struct wordlist_tree_iterator *I = zmalloc (sizeof (struct wordlist_tree_iterator));
  init_wordlist_subiterator (&I->WS, data, len);
  init_tree_subiterator (&I->TS, tree);
  I->jump_to = wordlist_tree_iterator_jump_to;
  if (I->TS.pos < I->WS.pos) {
    assert ((I->mult = I->TS.mult) > 0);
    I->pos = I->TS.pos;
  } else if (I->TS.pos > I->WS.pos) {
    assert ((I->mult = I->WS.mult) > 0);
    I->pos = I->WS.pos;
  } else if (I->TS.mult + I->WS.mult > 0) {
    I->mult = I->TS.mult + I->WS.mult;
    I->pos = I->TS.pos;
  } else {
    assert (I->TS.mult + I->WS.mult == 0);
    wordlist_tree_iterator_jump_to ((iterator_t) I, I->TS.pos + 1);
  }
  return (iterator_t) I;
}


iterator_t build_everything_iterator (void) {
  struct iterator *I = zmalloc (sizeof (struct iterator));
  I->jump_to = everything_iterator_jump_to;
  I->pos = -1;
  everything_iterator_jump_to (I, 0);
  return I;
}

struct condition_iterator {
  iterator_jump_func_t jump_to;
  int pos;
  condition_t Cond;
};

int condition_iterator_jump_to (iterator_t I, int req_pos) {
  assert (req_pos > I->pos);
  condition_t Cond = ((struct condition_iterator *) I)->Cond;
  while (req_pos <= max_uid && (!User[req_pos] || !user_matches_condition (User[req_pos], Cond, req_pos))) {
    ++req_pos;
  }
  if (req_pos > max_uid) {
    I->jump_to = empty_iterator_jump_to;
    return I->pos = INFTY;
  }
  return I->pos = req_pos;
}

iterator_t build_condition_iterator (condition_t Cond) {
  struct condition_iterator *I = zmalloc (sizeof (struct condition_iterator));
  I->pos = -1;
  I->Cond = Cond;
  I->jump_to = condition_iterator_jump_to;
  condition_iterator_jump_to ((iterator_t) I, 0);
  return (iterator_t) I;
}

struct id_range_iterator {
  iterator_jump_func_t jump_to;
  int pos;
  int lim;
};

int id_range_iterator_jump_to (iterator_t I, int req_pos) {
  assert (req_pos > I->pos);
  while (req_pos <= ((struct id_range_iterator *) I)->lim) {
    if (User[req_pos]) {
      return I->pos = req_pos;
    }
    ++req_pos;
  }
  I->jump_to = empty_iterator_jump_to;
  return I->pos = INFTY;
}

iterator_t build_id_range_iterator (int min_user_id, int max_user_id) {
  int _min_uid = (min_user_id - log_split_min + log_split_mod - 1) / log_split_mod;
  int _max_uid = (max_user_id - log_split_min + log_split_mod) / log_split_mod - 1;
  if (_max_uid > max_uid) {
    _max_uid = max_uid;
  }
  if (_min_uid <= 0) {
    _min_uid = 0;
  }
  if (_min_uid > _max_uid) {
    return build_empty_iterator ();
  }
  struct id_range_iterator *I = zmalloc (sizeof (struct id_range_iterator));
  I->jump_to = id_range_iterator_jump_to;
  I->pos = _min_uid - 1;
  I->lim = _max_uid;
  id_range_iterator_jump_to ((iterator_t) I, _min_uid);
  return (iterator_t) I;
}

/* ------------------ explicit list iterator ------------------- */

struct explicit_list_iterator {
  iterator_jump_func_t jump_to;
  int pos;
  int *cur, *last;
};

int explicit_list_iterator_jump_to (iterator_t I, int req_pos) {
  struct explicit_list_iterator *IL = (struct explicit_list_iterator *) I;
  assert (req_pos > IL->pos);
  if (IL->cur == IL->last) {
    IL->jump_to = empty_iterator_jump_to;
    return IL->pos = INFTY;
  }
  if (req_pos <= *IL->cur) {
    return IL->pos = *(IL->cur++);
  }
  int *A = IL->cur;
  long a = 0, b = 1, c = IL->last - A;
  if (b < c) {
    while (A[b] < req_pos) {  // a < b < c, A[a] < req_pos
      a = b;
      b <<= 1;
      if (b >= c) {
	b = c;
	break;
      }
    }
  }
  // A[a] < req_pos <= A[b] , 0 <= a < b <= c
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (A[c] < req_pos) {
      a = c;
    } else {
      b = c;
    }
  }
  A += b;
  if (A == IL->last) {
    IL->cur = A;
    IL->jump_to = empty_iterator_jump_to;
    return IL->pos = INFTY;
  }
  IL->cur = A + 1;
  return IL->pos = *A;
}

iterator_t build_explicit_list_iterator (int *List, int size) {
  if (size <= 0) {
    return build_empty_iterator ();
  }
  struct explicit_list_iterator *IL = zmalloc (sizeof (struct explicit_list_iterator));
  IL->jump_to = explicit_list_iterator_jump_to;
  IL->last = List + size;
  IL->cur = List + 1;
  IL->pos = List[0];
  //  vkprintf (2, "building explicit list iterator of length %d, first value %d\n", size, IL->pos);
  return (iterator_t) IL;
}

/* ---------------- compound binary iterators ------------------ */

struct binary_operation_iterator {
  iterator_jump_func_t jump_to;
  int pos;
  iterator_t A, B;
};

int dummy_iterator_jump_to (iterator_t I, int req_pos);

int binary_intersection_iterator_jump_to (iterator_t I, int req_pos);
iterator_t build_binary_intersection_iterator (iterator_t A, iterator_t B);

int binary_union_iterator_jump_to (iterator_t I, int req_pos);
iterator_t build_binary_union_iterator (iterator_t A, iterator_t B);

int binary_subtraction_iterator_jump_to (iterator_t I, int req_pos);
iterator_t build_binary_subtraction_iterator (iterator_t A, iterator_t B);

int dummy_iterator_jump_to (iterator_t I, int req_pos) {
  struct binary_operation_iterator *X = (struct binary_operation_iterator *) I;
  int res = X->A->jump_to (X->A, req_pos);
  if (res == INFTY) {
    X->jump_to = empty_iterator_jump_to;
  }
  return X->pos = res;
}

int binary_intersection_iterator_jump_to (iterator_t I, int req_pos) {
  struct binary_operation_iterator *X = (struct binary_operation_iterator *) I;
  int a = X->A->pos;
  int b = X->B->pos;
  if (a < req_pos) {
    a = X->A->jump_to (X->A, req_pos);
  }
  if (b < a) {
    b = X->B->jump_to (X->B, a);
  }
  while (a != b) {
    if (a < b) {
      a = X->A->jump_to (X->A, b);
    } else {
      b = X->B->jump_to (X->B, a);
    }
  }
  if (a == INFTY) {
    X->jump_to = empty_iterator_jump_to;
  }
  return X->pos = a;
}

int binary_union_iterator_jump_to (iterator_t I, int req_pos) {
  struct binary_operation_iterator *X = (struct binary_operation_iterator *) I;
  int a = X->A->pos;
  int b = X->B->pos;
  if (a < req_pos) {
    a = X->A->jump_to (X->A, req_pos);
  }
  if (b < req_pos) {
    b = X->B->jump_to (X->B, req_pos);
  }
  if (a < b) {
    if (b == INFTY) {
      X->jump_to = dummy_iterator_jump_to;
    }
    return X->pos = a;
  } else if (a > b) {
    if (a == INFTY) {
      X->jump_to = dummy_iterator_jump_to;
      X->A = X->B;
    }
    return X->pos = b;
  } else {
    if (a == INFTY) {
      X->jump_to = empty_iterator_jump_to;
    }
    return X->pos = a;
  }
}

int binary_subtraction_iterator_jump_to (iterator_t I, int req_pos) {
  struct binary_operation_iterator *X = (struct binary_operation_iterator *) I;
  int a = X->A->pos;
  int b = X->B->pos;
  if (a < req_pos) {
    a = X->A->jump_to (X->A, req_pos);
  }
  if (b < a) {
    b = X->B->jump_to (X->B, a);
  }
  while (a == b) {
    if (a == INFTY) {
      X->jump_to = empty_iterator_jump_to;
      return X->pos = a;
    }
    a = X->A->jump_to (X->A, a + 1);
    b = X->B->jump_to (X->B, a);
  }
  if (b == INFTY) {
    X->jump_to = dummy_iterator_jump_to;
  }
  return X->pos = a;
}

iterator_t build_binary_intersection_iterator (iterator_t A, iterator_t B) {
  struct binary_operation_iterator *I = zmalloc (sizeof (struct binary_operation_iterator));
  I->jump_to = binary_intersection_iterator_jump_to;
  I->pos = -1;
  I->A = A;
  I->B = B;
  binary_intersection_iterator_jump_to ((iterator_t) I, 0);
  return (iterator_t) I;
}

iterator_t build_binary_union_iterator (iterator_t A, iterator_t B) {
  struct binary_operation_iterator *I = zmalloc (sizeof (struct binary_operation_iterator));
  I->jump_to = binary_union_iterator_jump_to;
  I->pos = min (A->pos, B->pos);
  I->A = A;
  I->B = B;
  return (iterator_t) I;
}

iterator_t build_binary_subtraction_iterator (iterator_t A, iterator_t B) {
  struct binary_operation_iterator *I = zmalloc (sizeof (struct binary_operation_iterator));
  I->jump_to = binary_subtraction_iterator_jump_to;
  I->pos = -1;
  I->A = A;
  I->B = B;
  binary_subtraction_iterator_jump_to ((iterator_t) I, 0);
  return (iterator_t) I;
}

// TO OPTIMIZE !!!
iterator_t build_complement_iterator (iterator_t A) {
  return build_binary_subtraction_iterator (build_everything_iterator (), A);
}

struct condition_filter_iterator {
  iterator_jump_func_t jump_to;
  int pos;
  condition_t Cond;
  iterator_t A;
};

int condition_filter_iterator_jump_to (iterator_t I, int req_pos) {
  struct condition_filter_iterator *IF = (struct condition_filter_iterator *) I;
  int x = IF->pos;
  if (req_pos > x) {
    x = IF->A->jump_to (IF->A, req_pos);
  }
  while (1) {
    if (x > max_uid) {
      IF->jump_to = empty_iterator_jump_to;
      return IF->pos = INFTY;
    }
    assert ((unsigned) x <= (unsigned) max_uid && User[x]);
    if (user_matches_condition (User[x], IF->Cond, x)) {
      return IF->pos = x;
    }
    x = IF->A->jump_to (IF->A, x + 1);
  }
}
  
iterator_t build_condition_filter_iterator (iterator_t A, condition_t Cond) {
  struct condition_filter_iterator *I = zmalloc (sizeof (struct condition_filter_iterator));
  I->jump_to = condition_filter_iterator_jump_to;
  I->Cond = Cond;
  I->A = A;
  I->pos = A->pos;
  condition_filter_iterator_jump_to ((iterator_t) I, A->pos);
  return (iterator_t) I;
}

/* ------------ compound multiple-operand iterator  ------------ */

struct intersection_condition_iterator {
  iterator_jump_func_t jump_to;
  int pos, cnt;
  condition_t Cond;
  iterator_t A[0];
};


iterator_t prebuild_multiple_intersection_iterator (int N) {
  assert (N >= 2 && N <= 4096);
  struct intersection_condition_iterator *I = zmalloc0 (offsetof (struct intersection_condition_iterator, A) + sizeof (iterator_t) * N);
  I->pos = -1;
  I->cnt = N;
  return (iterator_t)I;
}


int multiple_intersection_jump_to (iterator_t I, int req_pos) {
  struct intersection_condition_iterator *ICI = (struct intersection_condition_iterator *)I;
  long i, N = ICI->cnt;
  int x;
  do {
    x = ICI->A[0]->pos;
    if (x < req_pos) {
      x = ICI->A[0]->jump_to (ICI->A[0], req_pos);
    }
    if (x == INFTY) {
      I->jump_to = empty_iterator_jump_to;
      return I->pos = INFTY;
    }
    for (i = 1; i < N; i++) {
      req_pos = ICI->A[i]->pos;
      if (req_pos < x) {
	req_pos = ICI->A[i]->jump_to (ICI->A[i], x);
      }
      if (req_pos > x) {
	break;
      }
    }
    if (req_pos == INFTY) {
      I->jump_to = empty_iterator_jump_to;
      return I->pos = INFTY;
    }
  } while (i < N);
  return I->pos = x;
}

int multiple_intersection_with_condition_jump_to (iterator_t I, int req_pos) {
  struct intersection_condition_iterator *ICI = (struct intersection_condition_iterator *)I;
  long i, N = ICI->cnt;
  int x;
  while (1) {
    do {
      x = ICI->A[0]->pos;
      if (x < req_pos) {
	x = ICI->A[0]->jump_to (ICI->A[0], req_pos);
      }
      if (x == INFTY) {
	I->jump_to = empty_iterator_jump_to;
	return I->pos = INFTY;
      }
      for (i = 1; i < N; i++) {
	req_pos = ICI->A[i]->pos;
	if (req_pos < x) {
	  req_pos = ICI->A[i]->jump_to (ICI->A[i], x);
	}
	if (req_pos > x) {
	  break;
	}
      }
      if (req_pos == INFTY) {
	I->jump_to = empty_iterator_jump_to;
	return I->pos = INFTY;
      }
    } while (i < N);
    if (user_matches_condition (User[x], ICI->Cond, x)) {
      break;
    }
    req_pos = x + 1;
  }
  return I->pos = x;
}


int multiple_intersection_subtraction_jump_to (iterator_t I, int req_pos) {
  struct intersection_condition_iterator *ICI = (struct intersection_condition_iterator *)I;
  long i, N = ICI->cnt - 1;
  int x;
  while (1) {
    x = ICI->A[0]->pos;
    if (x < req_pos) {
      x = ICI->A[0]->jump_to (ICI->A[0], req_pos);
    }
    if (x == INFTY) {
      I->jump_to = empty_iterator_jump_to;
      return I->pos = INFTY;
    }
    for (i = 1; i < N; i++) {
      req_pos = ICI->A[i]->pos;
      if (req_pos < x) {
	req_pos = ICI->A[i]->jump_to (ICI->A[i], x);
      }
      if (req_pos > x) {
	break;
      }
    }
    if (i < N) {
      if (req_pos == INFTY) {
	I->jump_to = empty_iterator_jump_to;
	return I->pos = INFTY;
      }
      continue;
    }
    req_pos = ICI->A[i]->pos;
    if (req_pos < x) {
      req_pos = ICI->A[i]->jump_to (ICI->A[i], x);
    }
    if (req_pos == INFTY) {
      ICI->cnt--;
      I->jump_to = multiple_intersection_jump_to;
      return I->pos = x;
    }
    if (req_pos != x) {
      return I->pos = x;
    }
    ++req_pos;
  }
}

int multiple_intersection_subtraction_with_condition_jump_to (iterator_t I, int req_pos) {
  struct intersection_condition_iterator *ICI = (struct intersection_condition_iterator *)I;
  long i, N = ICI->cnt - 1;
  int x;
  while (1) {
    x = ICI->A[0]->pos;
    if (x < req_pos) {
      x = ICI->A[0]->jump_to (ICI->A[0], req_pos);
    }
    if (x == INFTY) {
      I->jump_to = empty_iterator_jump_to;
      return I->pos = INFTY;
    }
    for (i = 1; i < N; i++) {
      req_pos = ICI->A[i]->pos;
      if (req_pos < x) {
	req_pos = ICI->A[i]->jump_to (ICI->A[i], x);
      }
      if (req_pos > x) {
	break;
      }
    }
    if (i < N) {
      if (req_pos == INFTY) {
	I->jump_to = empty_iterator_jump_to;
	return I->pos = INFTY;
      }
      continue;
    }
    req_pos = ICI->A[i]->pos;
    if (req_pos < x) {
      req_pos = ICI->A[i]->jump_to (ICI->A[i], x);
    }
    if (req_pos == INFTY) {
      ICI->cnt--;
      I->jump_to = multiple_intersection_with_condition_jump_to;
      if (user_matches_condition (User[x], ICI->Cond, x)) {
	return I->pos = x;
      } else {
	return multiple_intersection_with_condition_jump_to (I, x + 1);
      }
    }
    if (req_pos != x && user_matches_condition (User[x], ICI->Cond, x)) {
      return I->pos = x;
    }
    req_pos = x + 1;
  }
}


iterator_t build_multiple_intersection_iterator (iterator_t I) {
  struct intersection_condition_iterator *ICI = (struct intersection_condition_iterator *)I;
  if (ICI->Cond) {
    ICI->jump_to = multiple_intersection_with_condition_jump_to;
  } else {
    ICI->jump_to = multiple_intersection_jump_to;
  }
  ICI->jump_to (I, 0);
  return I;
}


iterator_t build_multiple_intersection_iterator_with_subtraction (iterator_t I) {
  struct intersection_condition_iterator *ICI = (struct intersection_condition_iterator *)I;
  if (ICI->Cond) {
    ICI->jump_to = multiple_intersection_subtraction_with_condition_jump_to;
  } else {
    ICI->jump_to = multiple_intersection_subtraction_jump_to;
  }
  ICI->jump_to (I, 0);
  return I;
}

struct union_iterator {
  iterator_jump_func_t jump_to;
  int pos;
  int cnt;
  iterator_t H[0];
};

iterator_t prebuild_multiple_union_iterator (int N) {
  assert (N >= 2 && N <= 4096);
  struct union_iterator *I = zmalloc0 (offsetof (struct union_iterator, H) + sizeof (iterator_t) * (N + 1));
  I->pos = -1;
  I->cnt = N;
  return (iterator_t)I;
}

int multiple_union_jump_to (iterator_t I, int req_pos) {
  struct union_iterator *IU = (struct union_iterator *)I;
  long N = IU->cnt;
  while (IU->H[1]->pos < req_pos) {
    int x = IU->H[1]->jump_to (IU->H[1], req_pos);
    iterator_t A = IU->H[1];
    if (x == INFTY) {
      /*if (N == 1) {
	jump_to = empty_iterator_jump_to;
	return I->pos = INFTY;
      }
      if (N == 2) {
	jump_to = dummy_iterator_jump_to;
	((struct binary_operation_iterator *)I)->A = IU->H[2];
	if (IU->H[2]->pos >= req_pos) {
	  return I->pos = IU->H[2]->pos;
	}
	return dummy_iterator_jump_to (I, req_pos);
	}*/
      if (N <= 3) {
	assert (N == 3);
	I->jump_to = binary_union_iterator_jump_to;
	((struct binary_operation_iterator *)I)->A = IU->H[2];
	((struct binary_operation_iterator *)I)->B = IU->H[3];
	return binary_union_iterator_jump_to (I, req_pos);
      }
      A = IU->H[N];
      IU->cnt = --N;
      x = A->pos;
    }
    long i = 1;
    while (1) {
      long j = i << 1;
      if (j > N) {
	break;
      }
      if (j < N && IU->H[j]->pos > IU->H[j + 1]->pos) {
	++j;
      }
      if (x <= IU->H[j]->pos) {
	break;
      }
      IU->H[i] = IU->H[j];
      i = j;
    }
    IU->H[i] = A;
  }
  return IU->pos = IU->H[1]->pos;
}

iterator_t build_multiple_union_iterator (iterator_t I) {
  struct union_iterator *IU = (struct union_iterator *) I;
  long N = IU->cnt, k;

  //  fprintf (stderr, "building heap for multiple union iterator of size %ld\n", N);

  I->jump_to = multiple_union_jump_to;
  for (k = N >> 1; k >= 1; k--) {
    iterator_t A = IU->H[k];
    int x = A->pos;
    long i = k;
    while (1) {
      long j = i << 1;
      if (j > N) {
	break;
      }
      if (j < N && IU->H[j]->pos > IU->H[j + 1]->pos) {
	++j;
      }
      if (x <= IU->H[j]->pos) {
	break;
      }
      IU->H[i] = IU->H[j];
      i = j;
    }
    IU->H[i] = A;
  }
  I->pos = IU->H[1]->pos;
  return I;
}

/* ------------ build condition from iterator ----------- */

struct condition_with_iterator {
  condition_func_t eval;
  condition_t true_branch;
  condition_t false_branch;
  iterator_t Iter;
};

DECLARE_CHECK (iterator_condition) {
  iterator_t I = ((struct condition_with_iterator *) Cond)->Iter;
  int x = U->uid, y = I->pos;
  if (y < x) {
    y = I->jump_to (I, x);
  }
  RETURN_COND (y == x);
}

condition_t build_condition_from_iterator (iterator_t I, condition_t true_branch, condition_t false_branch) {
  struct condition_with_iterator *Cond = zmalloc (sizeof (struct condition_with_iterator));
  Cond->eval = check_iterator_condition;
  Cond->true_branch = true_branch;
  Cond->false_branch = false_branch;
  Cond->Iter = I;
  return (condition_t) Cond;
}

/* ------------ building iterator from query ------------ */

static query_t *apply_quantifier (int quantifier, query_t *B) {
  query_t *A = new_qnode (quantifier, 0);

  assert (A);
  A->left = B;
  A->complexity = B->complexity * 4;
  A->max_res = B->max_res;
  A->flags = 2;

  return A;
} 

query_t *lift_quantifier (int quantifier, query_t *Q) {
  if ((Q->flags & 1) || !Q_IS_SMALLISH (Q)) {
    return 0;
  }
  if (Q->type == q_and || Q->type == q_or) {
    query_t *A = lift_quantifier (quantifier, Q->left);
    query_t *B = lift_quantifier (quantifier, Q->right);
    if (!A) {
      return B;
    }
    if (!B) {
      return A;
    }
    query_t *C = new_qnode (Q->type, 0);
    C->left = A;
    C->right = B;
    relax_max_res (C);
    if (B->type == Q->type && !(B->flags & 1)) {
      C->last = B->last;
    } else {
      C->last = C;
    }
    return C;
  }
  return apply_quantifier (quantifier, Q);
}

iterator_t build_iterator_from_query (query_t *Q, int flags) {
  int op = Q->type;

  if (Q_IS_GRAYISH (Q)) {
    //    fprintf (stderr, "building condition iterator for subexpression ");
    //    print_query (Q);
    //    fprintf (stderr, "\n");
    return build_condition_iterator (build_condition_from_query (Q, flags & 1));
  }

  if (Q_IS_SMALL (Q)) {
    assert (!(Q->flags & 1));

    if (op == q_and || op == q_or) {
      query_t *A = Q;
      int N = 2, gray = 0, big = 0;
      while (A != Q->last) {
	A = A->right;
	N++;
      }
      if (op == q_or) {
	//	fprintf (stderr, "building OR iterator for chain of %d subiterators\n", N);
	if (N == 2) {
	  assert (Q_IS_SMALL (Q->left) && Q_IS_SMALL (Q->right));
	  return build_binary_union_iterator (build_iterator_from_query (Q->left, flags), build_iterator_from_query (Q->right, flags));
	}
	iterator_t I = prebuild_multiple_union_iterator (N);
	long i = 1;
	for (A = Q; i < N; A = A->right, i++) {
	  assert (Q_IS_SMALL (A->left));
	  ((struct union_iterator *) I)->H[i] = build_iterator_from_query (A->left, flags);
	}
	assert (Q_IS_SMALL (A));
	((struct union_iterator *) I)->H[i] = build_iterator_from_query (A, flags);
	return build_multiple_union_iterator (I);
      }
      if (op == q_and) {
	if (Q_IS_GRAYISH (A->right)) {
	  gray++;
	  N--;
	} else if (Q_IS_BIG (A->right)) {
	  big++;
	  N--;
	  if (Q_IS_GRAYISH (A->left)) {
	    gray++;
	    N--;
	  }
	}
	//	fprintf (stderr, "building AND iterator for chain of %d+%d+%d subiterators\n", N, gray, big);
	assert (N > 0);
	if (N == 1) {
	  assert (Q_IS_SMALL (Q->left));
	  if (big) {
	    negate_query (Q->right);
	    iterator_t I = build_binary_subtraction_iterator (build_iterator_from_query (Q->left, flags), build_iterator_from_query (Q->right, flags));
	    negate_query (Q->right);
	    return I;
	  }
	  assert (Q_IS_GRAYISH (Q->right));
	  return build_condition_filter_iterator (build_iterator_from_query (Q->left, flags), build_condition_from_query (Q->right, flags & 1));
	}
	if (N == 2 && !big && !gray) {
	  assert (Q_IS_SMALL (Q->left) && Q_IS_SMALL (Q->right));
	  return build_binary_intersection_iterator (build_iterator_from_query (Q->left, flags), build_iterator_from_query (Q->right, flags));
	}
	struct intersection_condition_iterator *ICI = (void *) prebuild_multiple_intersection_iterator (N + big);
	long i = 0;
	for (A = Q; i < N - 1; A = A->right, i++) {
	  assert (Q_IS_SMALL (A->left));
	  ICI->A[i] = build_iterator_from_query (A->left, flags);
	}
	if (!big && !gray) {
	  assert (Q_IS_SMALL (A));
	  ICI->A[i] = build_iterator_from_query (A, flags);
	  return build_multiple_intersection_iterator ((iterator_t) ICI);
	}
	assert (Q_IS_SMALL (A->left));
	ICI->A[i] = build_iterator_from_query (A->left, flags);
	A = A->right;
	if (!big) {
	  ICI->Cond = build_condition_from_query (A, flags & 1);
	  return build_multiple_intersection_iterator ((iterator_t) ICI);
	}
	if (gray) {
	  assert (Q_IS_GRAYISH (A->left));
	  ICI->Cond = build_condition_from_query (A->left, flags & 1);
	  A = A->right;
	}
	negate_query (A);
	ICI->A[N] = build_iterator_from_query (A, flags);
	negate_query (A);
	return build_multiple_intersection_iterator_with_subtraction ((iterator_t) ICI);
      }
      assert (0);
    }
    if (op == q_false) {
      return build_empty_iterator ();
    }
    if (op == q_inlist) {
      return build_explicit_list_iterator (aux_userlist, aux_userlist_size);
    }
    if (Q->flags & 24) {
      // numeric atom
      if (Q->type == q_id) {
	return build_id_range_iterator (Q->value, Q->value2);
      }
      //      fprintf (stderr, "building numeric atom iterator for list %016llx\n", Q->hash);

      if (Q->flags & 16) {
	assert (Q->value == Q->value2);
      }
      assert (Q->hash);
      return build_word_iterator (Q->hash);
    }
    if (Q->flags & 32) {
      // word atom
      if (Q->type == q_name) {
	return build_word_iterator (Q->hash);
      } else if (Q->type == q_name_interests) {
	if (!get_word_count_nomult (Q->hash2)) {
	  return build_word_iterator (Q->hash);
	} else if (!get_word_count_nomult (Q->hash)) {
	  return build_word_iterator (Q->hash2);
	}
	return build_binary_union_iterator (build_word_iterator (Q->hash), build_word_iterator (Q->hash2));
      }
      //      fprintf (stderr, "building word atom iterator for list %016llx\n", Q->hash2);
      return build_word_iterator (Q->hash2);
    }
    if (Q->flags & 2) {
      // quantifier
      if (Q->left->type == q_or && !(Q->left->flags & 1)) {
	// quantifier applied to OR chain, lift everything small
	//	fprintf (stderr, "sifting quantifiers in expression "); print_query (Q); fprintf (stderr, "\n");

	query_t *A = Q->left;
	while (A->type == q_or && !(A->flags & 1)) {
	  assert (Q_IS_SMALLISH (A->left));
	  A->left = apply_quantifier (Q->type, A->left);
	  A = A->right;
	}
	assert (A == Q->left->last->right);
	assert (Q_IS_SMALLISH (A));
	Q->left->last->right = apply_quantifier (Q->type, A);
	Q->type = q_none;

	//	fprintf (stderr, "resulting expression is "); print_query (Q->left); fprintf (stderr, "\n");

	return build_iterator_from_query (Q->left, flags);
      }

      if (Q->left->flags & 56) {
	return build_iterator_from_query (Q->left, flags);
      }

      assert (Q->left->type == q_and && !(Q->left->flags & 1));

      //      fprintf (stderr, "lifting quantifiers in expression "); print_query (Q); fprintf (stderr, "\n");

      query_t *R = lift_quantifier (Q->type, Q->left);

      //      fprintf (stderr, "resulting expression is "); if (R) { print_query (R); } fprintf (stderr, R ? "\n" : " TRUE\n");

      if (R) {
	return build_condition_filter_iterator (build_iterator_from_query (R, flags), build_condition_from_query (Q, flags & 1));
      }
      assert (0);
    }
    assert (0); 
  }
  if (Q_IS_BIG (Q)) {
    negate_query (Q);
    iterator_t A = build_iterator_from_query (Q, flags);
    negate_query (Q);
    return build_complement_iterator (A);
  }

  assert (0);
}

/* -------------- main query functions ------------------ */

condition_t AuxCond[MAX_AUX_QUERIES];

static void perform_query_rate_left (utree_t *T) {
  if (!T) {
    return;
  }

  perform_query_rate_left (T->left);

  if (R_cnt >= Q_limit * 2) {
    return;
  }

  user_t *U = (user_t *)T;
  if (user_matches (U)) {
    R[R_cnt++] = U->user_id;
    R[R_cnt++] = U->rate >> 8;
  }

  if (R_cnt >= Q_limit * 2) {
    return;
  }

  perform_query_rate_left (T->right);
}


static void perform_query_rate_right (utree_t *T) {
  if (!T) {
    return;
  }

  perform_query_rate_right (T->right);

  if (R_cnt >= Q_limit * 2) {
    return;
  }

  user_t *U = (user_t *)T;
  if (user_matches (U)) {
    R[R_cnt++] = U->user_id;
    R[R_cnt++] = U->rate >> 8;
  }

  if (R_cnt >= Q_limit * 2) {
    return;
  }

  perform_query_rate_right (T->left);
}


int perform_query_mem (void) {
  int i;

  if (verbosity > 1) {
    fprintf (stderr, "performing query by complete scanning with pre-compiled condition\n");
  }
  condition_t C = build_condition_from_query (Qq, 1);

  for (i = 0; i <= max_uid; i++) {
    if (User[i] && user_matches_condition (User[i], C, i)) {
      store_res (i);
    }
  }

  postprocess_res();

  return R_tot;
}

int perform_query_iterator (void) {
  if (verbosity > 1) {
    fprintf (stderr, "performing query via iterators\n");
  }

  PROFILER (6);

  iterator_t I = build_iterator_from_query (Qq, 1);

  PROFILER (7);

  int x = I->pos;
  while (x < INFTY) {
    store_res (x);
    x = I->jump_to (I, x + 1);
  }

  PROFILER (8);

  postprocess_res ();

  PROFILER (9);

  return R_tot;
}

//

static int IL, IBuff[MAX_IB_SIZE+1];

static void IBuff_sort (int a, int b) {
  int i, j, h, t;
  if (a >= b) { return; }
  h = IBuff[(a+b)>>1];
  i = a;
  j = b;
  do {
    while (IBuff[i] < h) { i++; }
    while (IBuff[j] > h) { j--; }
    if (i <= j) {
      t = IBuff[i];  IBuff[i++] = IBuff[j];  IBuff[j--] = t;
    }
  } while (i <= j);
  IBuff_sort (a, j);
  IBuff_sort (i, b);
}

static inline int online_time_interval_unpack (int ts1, int ts2) {
  IL = (ts1 > ocur_now) ? 0 : online_interval_unpack (IBuff, ts1, ts2);
  IBuff_sort (0, IL - 1);
  IBuff[IL] = ~(-1 << 31);
  return IL;
}

static void init_global_birthday_query_data () {
  int i;
  struct tm *tmp;
  time_t nowd = now + LOCAL_TIMEZONE_SHIFT_MINUTES * 60 - 86400;
  int dshift = (nowd % 86400) / 60;

  global_birthday_shift_left[0] = -MAX_TIMEZONE;
  global_birthday_shift_right[0] = -dshift - 1;
  global_birthday_shift_left[1] = -dshift;
  global_birthday_shift_right[1] = 1439 - dshift;
  global_birthday_shift_left[2] = 1440 - dshift;
  global_birthday_shift_right[2] = MAX_TIMEZONE;

  for (i = 0; i < MAX_BIRTHDAY_SOON + 3; i++) {
    tmp = gmtime (&nowd);
    global_birthday_day[i] = tmp->tm_mday;
    global_birthday_month[i] = tmp->tm_mon + 1;
    nowd += 86400;
  }

  global_birthday_left_month = global_birthday_month[0];
  global_birthday_right_month = -1;

  for (i = 1; i < MAX_BIRTHDAY_SOON + 3; i++) {
    if (global_birthday_month[i] != global_birthday_month[i-1]) {
      global_birthday_right_month = global_birthday_month[i];
      global_birthday_right_day_min = global_birthday_day[i];
      break;
    }
  }

  global_birthday_left_day_min = global_birthday_day[0];
  global_birthday_left_day_max = global_birthday_day[i-1];
  global_birthday_right_day_max = global_birthday_day[MAX_BIRTHDAY_SOON + 2];
}

static void init_global_age_query_data (void) {
  struct tm *tmp;
  time_t nowd = now + LOCAL_TIMEZONE_SHIFT_MINUTES * 60;
  tmp = gmtime (&nowd);
  global_age_day = tmp->tm_mday;
  global_age_month = tmp->tm_mon + 1;
  global_age_year = tmp->tm_year + 1900;

  nowd += AD_RECOMPUTE_INTERVAL;
  tmp = gmtime (&nowd);
  global_future_age_day = tmp->tm_mday;
  global_future_age_month = tmp->tm_mon + 1;
  global_future_age_year = tmp->tm_year + 1900;
}

int perform_query_internal (int seed) {
  long i;

  START_PROFILER;
  START_PROFILER;
  PROFILER (0);

  global_birthday_in_query = global_age_in_query = global_userlist_in_query = 0;

  assert (Qq);

  if (Q_limit > tot_users) {
    Q_limit = tot_users;
  }

  clear_res();
  clear_tmp_word_data();

  gray_threshold = (tot_users >> 2);
  gray_threshold2 = ~gray_threshold;

  if (!estimate_query_complexity (Qq, 0)) {
    PROFILER (1);
    return 0;
  }

  for (i = 0; i < Q_aux_num; i++) {
    estimate_query_complexity (Q_aux[i], 0);
  }

  PROFILER (2);

  if (verbosity > 2) {
    print_query (Qq); fprintf (stderr, "\n");
  }

  Qq = optimize_query (Qq);

  if (verbosity > 2) {
    print_query (Qq); fprintf (stderr, "\n");
  }

  for (i = 0; i < Q_aux_num; i++) {
    Q_aux[i] = optimize_query (Q_aux[i]);
  }

  PROFILER (3);

  if (global_birthday_in_query) {
    init_global_birthday_query_data ();
  }

  if (global_age_in_query) {
    init_global_age_query_data ();
  }

  user_randomize (log_last_ts + seed * 17239);

  int is_ad_query = R_position;
  int res_size = -1;

  if ((Qq->flags & 4) && !is_ad_query) {
    res_size = (Qq->max_res >= 0 ? Qq->max_res : Qq->max_res + tot_users + 1);
    assert (res_size > 0);
    if (Q_limit <= 0) {
      return res_size;
    }
    if (Q_limit > res_size) {
      Q_limit = res_size;
    }
  }

  if (res_size >= 0 && (Q_order == 2 || Q_order == -2) && (long long) Q_limit * tot_users < res_size * (res_size + (long long) Q_limit * (32 - __builtin_clz (res_size)))) {
    //"true" or single exact list: replace complete scanning with rate tree traversal
    vkprintf (3, "performing query via rate tree: limit=%d tot_users=%d res_size=%d\n", Q_limit, tot_users, res_size);

    CurrentCond = build_condition_from_query (Qq, 0);

    for (i = 0; i < Q_aux_num; i++) {
      AuxCond[i] = build_condition_from_query (Q_aux[i], 0);
    }  

    PROFILER (4);

    assert (Q_limit > 0);
    if (Q_order == 2) {
      perform_query_rate_left (rate_tree);
    } else if (Q_order == -2) {
      perform_query_rate_right (rate_tree);
    }

    PROFILER (5);

    return res_size;
  }

  for (i = 0; i < Q_aux_num; i++) {
    AuxCond[i] = build_condition_from_query (Q_aux[i], 1);
  }

  return perform_query_iterator ();

  //    return perform_query_mem ();
}

int perform_query (int seed) {
  dyn_mark_t heap_state;
  dyn_mark (heap_state);

  double query_time = -get_utime (CLOCK_MONOTONIC);

  int res = perform_query_internal (seed);

  query_time += get_utime (CLOCK_MONOTONIC);
  if (verbosity > 1) {
    fprintf (stderr, "query performed in %.6f seconds\n", query_time);
  }

  tot_queries_time += query_time;
  tot_queries++;

  profiler_cnt[15]++;
  profiler[15] += res;

  //fprintf (stderr, "%d users for query '%s'\n", res, Qs0);  

  if (verbosity > 2 && !(tot_queries & (QUERY_STATS_PERIODICITY - 1))) {
    dump_profiler_data ();
  }

  dyn_release (heap_state);
  return res;
}

void store_res_aud (int uid) {
  long i;
  vkprintf (3, "store_res_aud(%d)\n", uid);
  for (i = 0; (int) i < Q_aux_num; i++) {
    if (user_matches_condition (User[uid], AuxCond[i], uid)) {
      R[i]++;
    }
  }
  R_tot++;
}

int perform_audience_query (void) {
  vkprintf (1, "perform audience query() for %d auxiliary queries\n", Q_aux_num);
  memset (R, 0, Q_aux_num * 4);
  R_position = (-1 << 31);
  store_res = store_res_aud;
  postprocess_res = postprocess_res_std;
  int res = perform_query (0);
  R_cnt = Q_aux_num;
  R_position = 0;
  return res;
}


static int targ_aud_position, targ_aud_cpv, targ_aud_and_mask, targ_aud_xor_mask;
static int RX[4*MAX_AUX_QUERIES + 4];

void store_res_targ_aud (int uid) {
  vkprintf (3, "store_res_targ_aud(%d)\n", uid);
  R[R_cnt++] = uid;
  long i;
  for (i = 0; (int) i < Q_aux_num; i++) {
    if (user_matches_condition (User[uid], AuxCond[i], uid)) {
      RX[3*i + 3]++;
    }
  }
  RX[0]++;
  R_tot++;
}

void scan_targ_aud_user (int uid) {
  long i;
  vkprintf (3, "scan_targ_aud_user(%d)\n", uid);
  int val = user_cpv_is_enough (uid, targ_aud_position, targ_aud_cpv, targ_aud_and_mask, targ_aud_xor_mask);
  for (i = 0; (int) i < Q_aux_num; i++) {
    if (user_matches_condition (User[uid], AuxCond[i], uid)) {
      RX[3*i + 4]++;
      if (val) {
	RX[3*i + 5]++;
      }
    }
  }
  RX[1]++;
  if (val) {
    RX[2]++;
  }
}


void postprocess_res_targ_aud (void) {
  int N = R_cnt;
  if (Q_limit > N) {
    Q_limit = N;
  }
  long M = Q_limit, i;
  for (i = 0; (int) i < R_cnt; i++) {
    if (lrand48() % N < M) {
      M--;
      scan_targ_aud_user (R[i]);
    }
    N--;
  }
}


int perform_targ_audience_query (int position, int cpv, int and_mask, int xor_mask) {
  vkprintf (1, "perform targ_audience query(position=%d, cpv=%d, mask=%d:%d) for %d auxiliary queries\n", position, cpv, and_mask, xor_mask, Q_aux_num);
  if (position <= 0 || position > 100 || Q_limit < 0 || Q_limit > 1000) {
    return -1;
  }
  if (!Q_limit) {
    Q_limit = 50;
  }
  memset (RX, 0, (Q_aux_num + 1) * 12);
  R_position = (-1 << 31);
  store_res = store_res_targ_aud;
  postprocess_res = postprocess_res_targ_aud;
  targ_aud_position = position;
  targ_aud_cpv = cpv;
  targ_aud_and_mask = and_mask;
  targ_aud_xor_mask = xor_mask;
  int res = perform_query (0);
  R_cnt = 3 * Q_aux_num + 2;
  R_position = 0;
  memcpy (R, RX + 1, R_cnt * 4);
  return res;
}
