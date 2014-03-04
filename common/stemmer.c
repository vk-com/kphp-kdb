/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2010-2011 Vkontakte Ltd
              2010-2011 Arseny Smirnov
              2010-2011 Aliaksei Levin
*/

#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "stemmer.h"

int stemmer_version = 1;

/*
 *
 *         LETTERS CONVERT FUNCTIONS
 *
 */

inline int conv_rus_win1251 (const char c) {
  switch (c) {
    case 'а' ... 'я' :
      return c - 'а';
    case 'А' ... 'Я' :
      return c - 'А';
    case 'Ё':
    case 'ё':
      return 'е' - 'а';
    default:
      return 33;
  }
}

inline int conv_rus_utf8 (const int c) {
  switch (c) {
    case 1081://й
      return 9;
    case 1094://ц
      return 22;
    case 1091://у
      return 19;
    case 1082://к
      return 10;
    case 1077://е
      return 5;
    case 1085://н
      return 13;
    case 1075://г
      return 3;
    case 1096://ш
      return 24;
    case 1097://щ
      return 25;
    case 1079://з
      return 7;
    case 1093://х
      return 21;
    case 1098://ъ
      return 26;
    case 1092://ф
      return 20;
    case 1099://ы
      return 27;
    case 1074://в
      return 2;
    case 1072://а
      return 0;
    case 1087://п
      return 15;
    case 1088://р
      return 16;
    case 1086://о
      return 14;
    case 1083://л
      return 11;
    case 1076://д
      return 4;
    case 1078://ж
      return 6;
    case 1101://э
      return 29;
    case 1103://я
      return 31;
    case 1095://ч
      return 23;
    case 1089://с
      return 17;
    case 1084://м
      return 12;
    case 1080://и
      return 8;
    case 1090://т
      return 18;
    case 1100://ь
      return 28;
    case 1073://б
      return 1;
    case 1102://ю
      return 30;
    case 1105://ё
      return 5;
    case 1049://Й
      return 9;
    case 1062://Ц
      return 22;
    case 1059://У
      return 19;
    case 1050://К
      return 10;
    case 1045://Е
      return 5;
    case 1053://Н
      return 13;
    case 1043://Г
      return 3;
    case 1064://Ш
      return 24;
    case 1065://Щ
      return 25;
    case 1047://З
      return 7;
    case 1061://Х
      return 21;
    case 1066://Ъ
      return 26;
    case 1060://Ф
      return 20;
    case 1067://Ы
      return 27;
    case 1042://В
      return 2;
    case 1040://А
      return 0;
    case 1055://П
      return 15;
    case 1056://Р
      return 16;
    case 1054://О
      return 14;
    case 1051://Л
      return 11;
    case 1044://Д
      return 4;
    case 1046://Ж
      return 6;
    case 1069://Э
      return 29;
    case 1071://Я
      return 31;
    case 1063://Ч
      return 23;
    case 1057://С
      return 17;
    case 1052://М
      return 12;
    case 1048://И
      return 8;
    case 1058://Т
      return 18;
    case 1068://Ь
      return 28;
    case 1041://Б
      return 1;
    case 1070://Ю
      return 30;
    case 1025://Ё
      return 5;
    default:
      return 33;
  }
}

inline int conv_eng (const char c) {
  switch (c) {
    case 'a' ... 'z' :
      return c - 'a';
    case 'A' ... 'Z' :
      return c - 'A';
    case '\'':
      return 26;
    default:
      return 33;
  }
}

/*
 *
 *         TRIE UTILS
 *
 */

#define maxc 35
//max size of alphabet

#define maxn 100000
//max string length

#define MAX_TYPE 1000
//max type stored in vertex

typedef struct {
  int ne[maxc];
  int type;
} vertex;

//max number of trie nodes
#define maxmem 2000
vertex mem[maxmem];
int mn = 0;


void trie_rus_add (int st, const char *s, int t) {
  int v = st;
  int n = strlen (s);

  while (n--) {
    int c = conv_rus_win1251(s[n]);
    if (!mem[v].ne[c]) {
      mem[v].ne[c] = mn++;
    }
    v = mem[v].ne[c];
  }

  assert (mem[v].type == 0);
  mem[v].type = t;
}

void trie_eng_add (int st, const char *s, int t) {
  int v = st;
  int n = strlen (s);

  while (n--) {
    int c = conv_eng (s[n]);
    if (!mem[v].ne[c]) {
      mem[v].ne[c] = mn++;
    }
    v = mem[v].ne[c];
  }

  assert (mem[v].type == 0);
  mem[v].type = t;
}

void add (int root, const char **s, int n, int t) {
  int i;
  static char s1[maxn], s2[maxn];

  if (t & 1) {
    s1[0] = 'а';
    s2[0] = 'я';
  }

  for (i = 0; i < n; i++) {
    if (t & 1) {
      strcpy (s1 + 1, s[i]);
      strcpy (s2 + 1, s[i]);
      trie_rus_add (root, s1, t);
      trie_rus_add (root, s2, t);
    } else {
      trie_rus_add (root, s[i], t);
    }
  }
  return;
}

static int best_t, best_d;
void trie_rus_try (int root, int *s, int n) {
  int v = root;
  best_t = MAX_TYPE;

  while (n--) {
    if (!mem[v].ne[s[n]]) {
      break;
    }
    v = mem[v].ne[s[n]];
    if (mem[v].type) {
      if (mem[v].type <= best_t) {
        best_t = mem[v].type;
        best_d = n;
      }
    }
  }

  if (best_t & 1) {
    best_d++;
  }
}

void trie_eng_try (int root, int *s, int n) {
  int v = root;
  best_t = MAX_TYPE;

  while (n--) {
    if (!mem[v].ne[s[n]]) {
      break;
    }
    v = mem[v].ne[s[n]];
    if (mem[v].type) {
      best_t = mem[v].type;
      best_d = n;
    }
  }
}

/*
 *
 *         UTF8 UTILS
 *
 */

int stem_string_to_utf8 (const unsigned char *s, int *v) {
  int n = 0;
#define CHECK(x) if (!(x)) {v[n] = 0; return n;}

  int a, b, c, d;

  while (1) {
    a = *s++;
    CHECK(a != 0) ;
    if ((a & 0x80) == 0) {
      v[n++] = a;
    } else if ((a & 0x40) == 0) {
      CHECK(0);
    } else if ((a & 0x20) == 0) {
      b = *s++;
      CHECK((b & 0xc0) == 0x80);
      v[n++] = ((a & 0x1f) << 6) | (b & 0x3f);
    } else if ((a & 0x10) == 0) {
      b = *s++;
      CHECK((b & 0xc0) == 0x80);
      c = *s++;
      CHECK((c & 0xc0) == 0x80);
      v[n++] = ((a & 0x0f) << 12) | ((b & 0x3f) << 6) | (c & 0x3f);
    } else if ((a & 0x08) == 0) {
      b = *s++;
      CHECK((b & 0xc0) == 0x80);
      c = *s++;
      CHECK((c & 0xc0) == 0x80);
      d = *s++;
      CHECK((d & 0xc0) == 0x80);
      v[n++] = ((a & 0x07) << 18) | ((b & 0x3f) << 12) | ((c & 0x3f) << 6) | (d & 0x3f);
    } else {
      CHECK(0);
    }
  }
}
#undef CHECK

//buffers for stemmed string
int stem_ts[maxn];
int stem_ts_init[maxn];

/*
 *
 *         RUSSIAN STEMMER
 *
 */

const char *type_1[] = {"в", "вши", "вшись"},
           *type_2[] = {"ив", "ивши", "ившись", "ыв", "ывши", "ывшись"},
           *type_4[] = {"ее", "ие", "ые", "ое", "ими", "ыми", "ей", "ий", "ый", "ой", "ем", "им", "ым", "ом", "его", "ого", "ему", "ому", "их", "ых", "ую", "юю", "ая", "яя", "ою", "ею"},
           *type_5[] = {"ем", "нн", "вш", "ющ", "щ"},
           *type_6[] = {"ивш", "ывш", "ующ"},
           *type_7[] = {"ла", "на", "ете", "йте", "ли", "й", "л", /*"ем",*/ "н", "ло", "но", "ет", "ют", "ны", "ть", "ешь", "нно"},
           *type_8[] = {"ила", "ыла", "ена", "ейте", "уйте", "ите", "или", "ыли", /*"ей",*/ "уй", "ил", "ыл",/* "им",*//* "ым",*/ "ен", "ило", "ыло", "ено", "ят", "ует", "уют", "ит", "ыт", "ены", "ить", "ыть", "ишь",/* "ую",*/ "ю"},
           *type_10[] = {"а", "ев", "ов",/* "ие",*/ "ье", "е", "иями", "ями", "ами", "еи", "ии", "и", "ией",/* "ей",*//* "ой",*//* "ий",*/ "й", "иям", "ям", "ием",/* "ем",*/ "ам",/* "ом",*/ "о", "у", "ах", "иях", "ях", "ы", "ь", "ию", "ью",/* "ю",*/ "ия", "ья", "я"};

const char *_vowels_rus = "уеыаоэяиюё";
int vowels_rus[maxc];
int root_ru, root_ru_part;

void gen_trie_rus (void) {
#define ADD(x, y) add (y, type_ ## x, sizeof (type_ ## x) / sizeof (char*), x);
  root_ru = mn++;
  root_ru_part = mn++;
  ADD(1, root_ru);
  ADD(2, root_ru);
  ADD(4, root_ru);
  ADD(5, root_ru_part);
  ADD(6, root_ru_part);
  ADD(7, root_ru);
  ADD(8, root_ru);
  ADD(10, root_ru);
#undef ADD
}

void gen_vowels_rus (void) {
  int i;
  for (i = 0; _vowels_rus[i]; i++) {
    vowels_rus[conv_rus_win1251(_vowels_rus[i])] = 1;
  }
}

inline int stem_rus (int n, int *delete_penultimate_letter) {
  int *s = stem_ts;
  int rv = 0, r2 = 0;

  while (s[rv] != -1 && !vowels_rus[s[rv]]) {
    rv++;
  }
  if (s[rv] != -1) {
    rv++;
  }

  r2 = rv;
  while (s[r2] != -1 && vowels_rus[s[r2]]) {
    r2++;
  }
  while (s[r2] != -1 && !vowels_rus[s[r2]]) {
    r2++;
  }
  while (s[r2] != -1 && vowels_rus[s[r2]]) {
    r2++;
  }
  if (s[r2] != -1) {
    r2++;
  }

  s += rv;
  n -= rv;
  trie_rus_try (root_ru, s, n);

  if (best_t <= 2) {
    n = best_d;
  } else {
    if (n >= 2 && s[n - 2] == conv_rus_win1251 ('с') && (s[n - 1] == conv_rus_win1251 ('ь') || s[n - 1] == conv_rus_win1251 ('я'))) {
      n -= 2;
      trie_rus_try (root_ru, s, n);
    }
    if (best_t != MAX_TYPE) {
      n = best_d;

      if (best_t == 4) {
        trie_rus_try (root_ru_part, s, n);
        if (best_t != MAX_TYPE) {
          n = best_d;
        }
      }
    }
  }

  if (n >= 1 && s[n - 1] == conv_rus_win1251 ('и')) {
    n--;
  }

  r2 -= rv;
  if (n >= r2 + 3 && s[n - 3] == conv_rus_win1251 ('о') && s[n - 2] == conv_rus_win1251 ('с') && s[n - 1] == conv_rus_win1251 ('т')) {
    n -= 3;
  } else
  if (n >= r2 + 4 && s[n - 4] == conv_rus_win1251 ('о') && s[n - 3] == conv_rus_win1251 ('с') && s[n - 2] == conv_rus_win1251 ('т') &&
                     s[n - 1] == conv_rus_win1251 ('ь')) {
    n -= 4;
  }

  if (n >= 1 && s[n - 1] == conv_rus_win1251 ('ь')) {
    n -= 1;
  } else {
    if (n >= 3 && s[n - 3] == conv_rus_win1251 ('е') && s[n - 2] == conv_rus_win1251 ('й') && s[n - 1] == conv_rus_win1251 ('ш')) {
      n -= 3;
    } else
    if (n >= 4 && s[n - 4] == conv_rus_win1251 ('е') && s[n - 3] == conv_rus_win1251 ('й') &&
        s[n - 2] == conv_rus_win1251 ('ш') && s[n - 1] == conv_rus_win1251 ('е')) {
      n -= 4;
    }

    if (n >= 2 && s[n - 2] == conv_rus_win1251 ('н') && s[n - 1] == conv_rus_win1251 ('н')) {
      n -= 1;
    }
  }

  //our addon, 'ьц' -> 'ц', 'ец' -> 'ц', 'ок' -> 'к', 'ек' -> 'к', 'ьк' -> 'к'
  if (n >= 3 &&
      ((s[n - 1] == conv_rus_win1251 ('ц') || s[n - 1] == conv_rus_win1251 ('к')) &&
       (s[n - 2] == conv_rus_win1251 ('е') || s[n - 2] == conv_rus_win1251 ('ь') || s[n - 2] == conv_rus_win1251 ('о')))) {
    *delete_penultimate_letter = 1;
  } else {
    *delete_penultimate_letter = 0;
  }


  return rv + n;
}

int stem_rus_win1251 (const char *s, int len, int *delete_penultimate_letter) {
  int n;
  for (n = 0; n < len && s[n]; n++) {
    stem_ts[n] = conv_rus_win1251 (s[n]);
  }
  stem_ts[n] = -1;
  return stem_rus (n, delete_penultimate_letter);
}

int stem_rus_utf8 (const char *s, int *delete_penultimate_letter) {
  int i, n = stem_string_to_utf8 ((unsigned char *)s, stem_ts);
  for (i = 0; i < n; i++) {
    stem_ts[i] = conv_rus_utf8 (stem_ts[i]);
  }
  stem_ts[n] = -1;
  return stem_rus (n, delete_penultimate_letter);
}

int stem_rus_utf8i (const int *v, int *delete_penultimate_letter) {
  int i;
  for (i = 0; v[i]; i++) {
    stem_ts[i] = conv_rus_utf8 (v[i]);
  }
  stem_ts[i] = -1;
  return stem_rus (i, delete_penultimate_letter);
}

/*
 *
 *         ENGLISH STEMMER
 *
 */

const char *_vowels_eng = "aeiouy";
const char *_double_eng = "bdfgmnprt";
const char *_li_endings_eng = "cdeghkmnrt";
int vowels_eng[maxc];
int double_eng[maxc];
int li_endings_eng[maxc];
int syllable_exceptions_eng[maxc];


const char *eng_step4[] = {"ion", "al", "ance", "ence", "er", "ic", "able", "ible", "ant", "ement", "ment", "ent", "ism", "ate", "iti", "ous", "ive", "ize"};
const char *eng_step2[] = {"tional", "enci", "anci", "abli", "entli", "izer", "ization", "ational", "ation", "ator", "alism", "aliti", "alli", "fulness", "ousli", "ousness", "iveness", "iviti", "biliti", "bli", "ogi", "fulli", "lessli", "li"};

int root_eng_step4;
int root_eng_step2;

void gen_trie_eng (void) {
  root_eng_step4 = mn++;
  int n = sizeof (eng_step4) / sizeof (char *), i;
  for (i = 0; i < n; i++)
    trie_eng_add (root_eng_step4, eng_step4[i], i + 1);

  root_eng_step2 = mn++;
  n = sizeof (eng_step2) / sizeof (char *);
  for (i = 0; i < n; i++)
    trie_eng_add (root_eng_step2, eng_step2[i], i + 1);
}


void gen_vowels_eng (void) {
  int i;
  for (i = 0; _vowels_eng[i]; i++) {
    vowels_eng[conv_eng(_vowels_eng[i])] = 1;
  }
}

void gen_double_eng (void) {
  int i;
  for (i = 0; _double_eng[i]; i++) {
    double_eng[conv_eng(_double_eng[i])] = 1;
  }
}

void gen_li_endings_eng (void) {
  int i;
  for (i = 0; _li_endings_eng[i]; i++) {
    li_endings_eng[conv_eng(_li_endings_eng[i])] = 1;
  }
}

void gen_syllable_exceptions_eng (void) {
  syllable_exceptions_eng[conv_eng('w')] = 1;
  syllable_exceptions_eng[conv_eng('x')] = 1;
  syllable_exceptions_eng[27] = 1;
}

int short_syllable_eng (int *s, int n) {
  if (n >= 2 && vowels_eng[s[n - 2]]) {
     if ((n == 2 && !vowels_eng[s[n - 1]]) ||
                   (!vowels_eng[s[n - 3]] && !vowels_eng[s[n - 1]] && !syllable_exceptions_eng[s[n - 1]])) {
       return 1;
     }
  }
  return 0;
}

int last_letter;

inline int do_stem_eng (int n) {
  int *s = stem_ts;

  last_letter = 0;

  //Step 0.

  if (n <= 2) {
    return n;
  }

  if (n >= 1 && s[n - 1] == conv_eng ('\'')) {
    n--;
  }

  if (n <= 2) {
    return n;
  }

  if (n >= 2 && s[n - 2] == conv_eng ('\'') && s[n - 1] == conv_eng ('s')) {
    n -= 2;
  }

  int i;
  for (i = 0; i < n; i++) {
    if (s[i] == conv_eng ('y') && (i == 0 || vowels_eng[s[i - 1]]))
      s[i] = 27;
  }

  int rv = 0;
  while (s[rv] != -1 && !vowels_eng[s[rv]]) {
    rv++;
  }
  if (s[rv] != -1) {
    rv++;
  }

  //Step 1a.
  if (n >= 3) {
    if (s[n - 1] == conv_eng ('d')) {
      if (n >= 4 && s[n - 2] == conv_eng ('e') && s[n - 3] == conv_eng ('i')) {
        n -= (n >= 5) + 1;
      }
    } else if (s[n - 1] == conv_eng ('s') && s[n - 2] != conv_eng ('u') && s[n - 2] != conv_eng ('s')) {
      if (n >= 4 && s[n - 2] == conv_eng ('e') && s[n - 3] == conv_eng ('i')) {
        n -= (n >= 5) + 1;
      } else if (n >= 5 && s[n - 2] == conv_eng ('e') && s[n - 3] == conv_eng ('s') && s[n - 4] == conv_eng ('s')) {
        n -= 2;
      } else {
        if (rv < n - 1) {
          n--;
        }
      }
    }
  }

  //Step 1b.

  int r1 = rv;
  while (s[r1] != -1 && vowels_eng[s[r1]]) {
    r1++;
  }
  if (s[r1] != -1) {
    r1++;
  }

  int tn = n;
  if (n >= 2 && s[n - 1] == conv_eng ('y') && s[n - 2] == conv_eng ('l')) {
    tn = n - 2;
  }
  if (tn >= 3) {
    if (s[tn - 1] == conv_eng ('d') && s[tn - 2] == conv_eng ('e') && s[tn - 2] == s[tn - 3]) {
      if (tn - 3 >= r1) {
        n = tn - 1;
      }
    } else if ((s[tn - 1] == conv_eng ('d') && s[tn - 2] == conv_eng ('e') && (tn -= 2)) ||
               (s[tn - 1] == conv_eng ('g') && s[tn - 2] == conv_eng ('n') && s[tn - 3] == conv_eng ('i') && (tn -= 3))) {
      if (rv <= tn) {
        n = tn;
        if (n >= 2) {
          if (s[n - 1] == s[n - 2] && double_eng[s[n - 1]]) {
            n--;
          } else if ((s[n - 1] == conv_eng ('t') && s[n - 2] == conv_eng ('a')) ||
                     (s[n - 1] == conv_eng ('l') && s[n - 2] == conv_eng ('b')) ||
                     (s[n - 1] == conv_eng ('z') && s[n - 2] == conv_eng ('i'))) {
              s[n++] = conv_eng ('e');
              last_letter = 'e';
          } else
          if (r1 >= n && short_syllable_eng(s, n)) {
              s[n++] = conv_eng ('e');
              last_letter = 'e';
          }
        }
      }
    }
  }

  //Step 1c.

  if (n >= 3 && s[n - 1] == conv_eng('y') && !vowels_eng[s[n - 2]]) {
    s[n - 1] = conv_eng('i');
    last_letter = 'i';
  }

  //Step 2.


  int r2 = r1;
  while (s[r2] != -1 && !vowels_eng[s[r2]]) {
    r2++;
  }
  while (s[r2] != -1 && vowels_eng[s[r2]]) {
    r2++;
  }
  if (s[r2] != -1) {
    r2++;
  }

//  printf("%d %c %c\n", n, (char)('a' + s[n - 1]), (char)last_letter);

  trie_eng_try (root_eng_step2, s, n);
//  printf("%d %d %d\n", best_t, best_d, r1);
  if (best_t != MAX_TYPE && best_d >= r1) {
    switch (best_t) {
      case 1: //tional -> tion
        n -= 2;
        break;
      case 2 ... 4: //enci -> ence; anci -> ance; abli -> able
        s[n - 1] = conv_eng('e');
        last_letter = 'e';
        break;
      case 5: //entli -> ent
        n -= 2;
        last_letter = 0;
        break;
      case 6 ... 10: //izer, ization -> ize; ational, ation, ator -> ate
        n = best_d + 3;
        s[n - 1] = conv_eng('e');
        last_letter = 'e';
        break;
      case 11 ... 13: //alism, aliti, alli -> al
        n = best_d + 2;
        last_letter = 0;
        break;
      case 14 ... 16: //fulness -> ful; ousli, ousness -> ous
        n = best_d + 3;
        last_letter = 0;
        break;
      case 17 ... 18: //iveness, iviti -> ive
        n = best_d + 3;
        s[n - 1] = conv_eng('e');
        last_letter = 'e';
        break;
      case 19 ... 20: //biliti, bli -> ble
        n = best_d + 3;
        s[n - 1] = conv_eng('e');
        s[n - 2] = conv_eng('l');
        last_letter = 'l';
        break;
      case 21: //ogi -> og (if preceded by l)
        if (best_d >= 1 && s[best_d - 1] == conv_eng('l')) {
          n--;
        }
        last_letter = 0;
        break;
      case 22 ... 23: //fulli -> ful; lessli -> less
        n -= 2;
        last_letter = 0;
        break;
      case 24: //li ->  (if preceded by a valid li-ending)
        if (n >= 3 && li_endings_eng[s[n - 3]]) {
          n -= 2;
          last_letter = 0;
        }
        break;
    }
  }

  //Step 3.

  if (n >= 3 + r1) {
    switch (s[n - 1]) {
      case 'l' - 'a'://conv_eng('l'):
        switch (s[n - 3]) {
          case 'c' - 'a'://conv_eng('c'):
            if (n >= 4 + r1 && s[n - 2] == conv_eng('a') && s[n - 4] == conv_eng('i')) { //ical -> ic
              n -= 2;
            }
            break;
          case 'f' - 'a'://conv_eng('f'):
            if (s[n - 2] == conv_eng('u')) { //ful ->
              n -= 3;
            }
            break;
          case 'n' - 'a'://conv_eng('n'):
            if (n >= 6 + r1 && s[n - 2] == conv_eng('a') && s[n - 4] == conv_eng('o')
                            && s[n - 5] == conv_eng('i') && s[n - 6] == conv_eng('t')) {
              if (s[n - 7] == conv_eng('a')) {
                if (n >= 7 + r1) { //ational -> ate
                  n -= 5;
                  s[n++] = conv_eng('e');
                  last_letter = 'e';
                }
              } else { //tional -> tion
                n -= 2;
              }
            }
            break;
        }
        break;
      case 'e' - 'a'://conv_eng('e'):
        switch (s[n - 2]) {
          case 'z' - 'a'://conv_eng('z'):
            if (n >= 5 + r1 && s[n - 3] == conv_eng('i') && s[n - 4] == conv_eng('l') && s[n - 5] == conv_eng('a')) { //alize -> al
              n -= 3;
              last_letter = 0;
            }
            break;
          case 't' - 'a'://conv_eng('t'):
            if (n >= 5 + r1 && s[n - 3] == conv_eng('a') && s[n - 4] == conv_eng('c') && s[n - 5] == conv_eng('i')) { //icate -> ic
              n -= 3;
              last_letter = 0;
            }
            break;
          case 'v' - 'a'://conv_eng('v'):
            if (n >= 5 + r2 && s[n - 3] == conv_eng('i') && s[n - 4] == conv_eng('t') && s[n - 5] == conv_eng('a')) { //ative ->
              n -= 5;
              last_letter = 0;
            }
            break;
        }

        break;
      case 'i' - 'a'://conv_eng('i'):
        if (n >= 5 + r1 && s[n - 2] == conv_eng('t') && s[n - 3] == conv_eng('i')
                        && s[n - 4] == conv_eng('c') && s[n - 5] == conv_eng('i')) { //iciti -> ic
          n -= 3;
          last_letter = 0;
        }
        break;
      case 's' - 'a'://conv_eng('s'):
        if (n >= 4 + r1 && s[n - 2] == conv_eng('s') && s[n - 3] == conv_eng('e') && s[n - 4] == conv_eng('n')) { //ness ->
          n -= 4;
        }
        break;
    }
  }

  // Step 4.

  trie_eng_try (root_eng_step4, s, n);
  if (best_t != MAX_TYPE && best_d >= r2) {
    if (best_t != 1 || (best_d >= 1 && (s[best_d - 1] == conv_eng('s') || s[best_d - 1] == conv_eng('t')))) { //[t|s]ion
      n = best_d;
      last_letter = 0;
    }
  }

  // Step 5.

//  printf("%d %c %c\n", n, (char)('a' + s[n - 1]), (char)last_letter);

  if (n >= 1) {
    if (s[n - 1] == conv_eng('l') && n - 1 >= r2 && n >= 2 && s[n - 2] == conv_eng('l')) {
      n--;
    } else if (s[n - 1] == conv_eng('e') && ((n - 1 >= r2) || (n - 1 >= r1 && !short_syllable_eng(s, n - 1)))) {
      if (last_letter == 'e') {
        last_letter = 0;
      }
      n--;
    }
  }

  return n;
}

int stem_eng (const char *s, char *res, int len) {
  int n;
  for (n = 0; n < len && s[n]; n++) {
    stem_ts[n] = conv_eng (s[n]);
  }
  stem_ts[n] = -1;
  int len2 = do_stem_eng (n);
  memcpy (res, s, len2 * sizeof (char));
  if (last_letter && len2 > 0) {
    res[len2 - 1] = last_letter;
  }
  res[len2] = 0;
  return len2;
}

int stem_engi (int *v) {
  int n;
  for (n = 0; v[n]; n++) {
    stem_ts[n] = conv_eng (v[n]);
  }
  stem_ts[n] = -1;

  int len2 = do_stem_eng (n);
  if (last_letter && len2 > 0) {
    v[len2 - 1] = last_letter;
  }
  v[len2] = 0;
  return len2;
}

/*
 *  stemmer exterface
 */

int use_stemmer;

int my_lc_str (char *buff, const char *text, int len) {
  int x, c;
  if (!use_stemmer || len < 3) {
    lc_str (buff, text, len);
    return len;
  }
  c = get_str_class (text, len);
  if ((c & 12) == 12) {
    int delete_penultimate_letter;
    x = stem_rus_win1251 (text, len, &delete_penultimate_letter);
    assert (x > 0 && x <= len);
    lc_str (buff, text, x);
    if (delete_penultimate_letter) {
      assert (x >= 2);
      x--;
      buff[x - 1] = buff[x];
    }
    return x;
  }
  if (c & 8) {
    int i;
    for (i = 0; i < len; i++) {
      if (text[i] < 0) {
        break;
      }
    }
    if (i < len) {
      lc_str (buff, text, len);
      return len;
    }
    x = stem_eng (text, buff, len);
    assert (x > 0 && x <= len);
    lc_str (buff, buff, x);
    return x;
  }
  lc_str (buff, text, len);
  return len;
}

/*
 *
 *         STEMMERS INITIALISATION
 *
 */

void stem_init (void) {
  gen_trie_rus ();
  gen_vowels_rus ();

  gen_trie_eng ();
  gen_vowels_eng ();
  gen_double_eng ();
  gen_li_endings_eng ();
  gen_syllable_exceptions_eng ();
}
