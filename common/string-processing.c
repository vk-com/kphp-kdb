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

    Copyright 2010-2012 Vkontakte Ltd
              2010-2012 Arseny Smirnov
              2010-2012 Aliaksei Levin
                   2012 Anton Timofeev
                   2012 Sergey Kopeliovich
*/

/**
 * Common string proccessing module.
 * Implementation moved from logs-engine.
 *
 * Originally written:    Arseny Smirnov & Alexey Levin
 * Moved from log-engine: Anton Timofeev
 * Created: 01.04.2012
 */

#define _FILE_OFFSET_BITS 64

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "utf8_utils.h"
#include "string-processing.h"

static char str_buf[STRING_PROCESS_BUF_SIZE];
static int str_buf_n = 0;
int sp_errno = 0;

inline void sp_init (void) {
  str_buf_n = 0;
}

char *sp_str_alloc (int len) {
//  we don't need to reset error level on each call
//  we check error level only once after all allocations have done
//  sp_errno = 0; // reset error level
  len++;
  if (str_buf_n + len > STRING_PROCESS_BUF_SIZE) {
    sp_errno = 2;
    return NULL;
  }

  char *res = str_buf + str_buf_n;
  str_buf_n += len;

  return res;
}

char *sp_str_pre_alloc (int len) {
//  we don't need to reset error level on each call
//  we check error level only once after all allocations have done
//  sp_errno = 0; // reset error level
  if (str_buf_n + len + 1 > STRING_PROCESS_BUF_SIZE) {
    sp_errno = 2;
    return NULL;
  }

  return str_buf + str_buf_n;
}

static inline int cmp_char (const void *a, const void *b) {
  return (int)(*(char *)a) - (int)(*(char *)b);
}

char *sp_sort (char *s) {
  int l = strlen (s);
  char *t = sp_str_alloc (l);
  if (t != 0) {
    memcpy (t, s, (l + 1) * sizeof (char));
    qsort (t, l, sizeof (char), cmp_char);
  }

  return t;
}

char *sp_to_upper (char *s) {
  int l = strlen (s);
  char *t = sp_str_alloc (l);
  if (t != 0) {
    int i;
    for (i = 0; i <= l; i++) {
      switch ((unsigned char)s[i]) {
        case 'a' ... 'z':
          t[i] = s[i] + 'A' - 'a';
          break;
        case 0xE0 ... 0xFF:
          t[i] = s[i] + 'À' - 'à';
          break;
        case 0x83:
          t[i] = 0x81;
          break;
        case 0xA2:
          t[i] = 0xA1;
          break;
        case 0xB8:
          t[i] = 0xA8;
          break;
        case 0xBC:
          t[i] = 0xA3;
          break;
        case 0xB4:
          t[i] = 0xA5;
          break;
        case 0xB3:
        case 0xBE:
          t[i] = s[i] - 1;
          break;
        case 0x98:
        case 0xA0:
        case 0xAD:
          t[i] = ' ';
          break;
        case 0x90:
        case 0x9A:
        case 0x9C ... 0x9F:
        case 0xBA:
        case 0xBF:
          t[i] = s[i] - 16;
          break;
        default:
          t[i] = s[i];
      }
    }
  }

  return t;
}

static inline char to_lower (const char c) {
  switch ((unsigned char)c) {
    case 'A' ... 'Z':
      return c + 'a' - 'A';
    case 0xC0 ... 0xDF:
      return c + 'à' - 'À';
    case 0x81:
      return 0x83;
    case 0xA1:
      return 0xA2;
    case 0xA8:
      return 0xB8;
    case 0xA3:
      return 0xBC;
    case 0xA5:
      return 0xB4;
    case 0xB2:
    case 0xBD:
      return c + 1;
    case 0x98:
    case 0xA0:
    case 0xAD:
      return ' ';
    case 0x80:
    case 0x8A:
    case 0x8C ... 0x8F:
    case 0xAA:
    case 0xAF:
      return c + 16;
  }
  return c;
}

char *sp_to_lower (char *s) {
  int l = strlen (s);
  char *t = sp_str_alloc (l);
  if (t != 0) {
    int i;
    for (i = 0; i <= l; i++) {
      t[i] = to_lower (s[i]);
    }
  }

  return t;
}

static inline char simplify (const char c) {
  unsigned char cc = to_lower (c);
  switch (cc) {
    case '0' ... '9':
    case 'a' ... 'z':
    case 0xE0 ... 0xFF:
    case 0:
      return cc;
    case 0x83:
    case 0xB4:
      return 0xE3;
    case 0xA2:
      return 0xF3;
    case 0xB8:
      return 0xE5;
    case 0xB3:
    case 0xBF:
      return 'i';
    case 0xBE:
      return 's';
    case 0x9A:
      return 0xEB;
    case 0x9C:
      return 0xED;
    case 0x9D:
      return 0xEA;
    case 0x90:
    case 0x9E:
      return 'h';
    case 0xBA:
      return 0xFD;
    case 0xBC:
      return 'j';
    case 0xA9:
      return 'c';
    case 0xAE:
      return 'r';
    case 0xB5:
      return 'm';
  }
  return 0;
}

char *sp_simplify (const char *s) {
  int l = strlen (s);
  char *t = sp_str_pre_alloc (l);
  if (t != 0) {
    int nl = 0, i;
    for (i = 0; i < l; i++) {
      char c = simplify (s[i]);
      if (c != 0) {
        t[nl++] = c;
      }
    }
    t[nl] = 0;

    char *new_t = sp_str_alloc (nl);
    assert (t == new_t);
  }

  return t;
}

static inline char conv_letter (const char c) {
  switch (c) {
    case 'a':
      return 'à';
    case 'b':
      return 'ü';
    case 'c':
      return 'ñ';
    case 'd':
      return 'd';
    case 'e':
      return 'å';
    case 'f':
      return 'f';
    case 'g':
      return 'g';
    case 'h':
      return 'h';
    case 'i':
      return 'i';
    case 'j':
      return 'j';
    case 'k':
      return 'ê';
    case 'l':
      return '1';
    case 'm':
      return 'ò';
    case 'n':
      return 'ï';
    case 'o':
      return 'î';
    case 'p':
      return 'ð';
    case 'q':
      return 'q';
    case 'r':
      return 'r';
    case 's':
      return 's';
    case 't':
      return 'ò';
    case 'u':
      return 'è';
    case 'v':
      return 'v';
    case 'w':
      return 'w';
    case 'x':
      return 'õ';
    case 'y':
      return 'ó';
    case 'z':
      return 'z';
    case 'à' ... 'ÿ':
      return c;
    case '0':
      return 'î';
    case '1' ... '9':
      return c;
    default:
      return c;
  }
}

static inline char next_character (const char *s, int *_i) {
  int i = *_i;
  char cur = s[i];
  if (cur == '&') {
    if (s[i + 1] == 'a' && s[i + 2] == 'm' && s[i + 3] == 'p' && s[i + 4] == ';') {
      i += 4;
    } else if (s[i + 1] == '#') {
      int r = 0, ti = i;
      for (i += 2; '0' <= s[i] && s[i] <= '9'; i++) {
        r = r * 10 + s[i] - '0';
      }
      if (s[i] == ';') {
        int c = remove_diacritics (r);
        if (c <= 255) {
          cur = c;
        } else {
          cur = 0;
        }
      } else {
        i = ti;
      }
    } else if (s[i + 1] == 'l' && s[i + 2] == 't' && s[i + 3] == ';') {
      i += 3, cur = '<';
    } else if (s[i + 1] == 'g' && s[i + 2] == 't' && s[i + 3] == ';') {
      i += 3, cur = '>';
    } else if (s[i + 1] == 'q' && s[i + 2] == 'u' && s[i + 3] == 'o' && s[i + 4] == 't' && s[i + 5] == ';') {
      i += 5, cur = '"';
    }
  } else if (cur == '<') {
    if (s[i + 1] == 'b' && s[i + 2] == 'r' && s[i + 3] == '>') {
      i += 3, cur = '\n';
    }
  }
  *_i = i;

  return cur;
}

char *sp_full_simplify (const char *s) {
  int l = strlen (s);
  char *t = sp_str_pre_alloc (l);
  if (t != 0) {
    int nl = 0, i;
    for (i = 0; i < l; i++) {
      char c = simplify (next_character (s, &i));
      if (c != 0) {
        t[nl++] = conv_letter (c);
      }
    }
    t[nl] = 0;

    char *new_t = sp_str_alloc (nl);
    assert (t == new_t);
  }

  return t;
}

char *sp_deunicode (char *s) {
  int l = strlen (s);
  char *t = sp_str_pre_alloc (l);
  if (t != 0) {
    int nl = 0, i;
    for (i = 0; i < l; i++) {
      char c = next_character (s, &i);
      if (c != 0) {
        t[nl++] = c;
      }
    }
    t[nl] = 0;

    char *new_t = sp_str_alloc (nl);
    assert (t == new_t);
  }

  return t;
}
