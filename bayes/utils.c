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

#define	_FILE_OFFSET_BITS	64

#include "utils.h"

size_t memory_used = 0;

void *qmalloc (size_t x) {
  memory_used += x;
  return malloc (x);
}

void *qmalloc0 (size_t x) {
  memory_used += x;
  return calloc (x, 1);
}

void *qrealloc (void *p, size_t x, size_t old) {
  memory_used += x - old;
  return realloc (p, x);
}

void qfree (void *p, size_t x) {
  memory_used -= x;
  return free (p);
}

long get_memory_used (void) {
  return memory_used;
}

void bayes_string_to_utf8 (unsigned char *s, int *v) {
  good_string_to_utf8 (s, v);

  int i;
  for (i = 0; v[i]; i++) {
    v[i] = remove_diacritics (v[i]);
  }

/*
  int j;
  for (i = j = 0; v[i]; i++) {
    if (v[i + 1] == '#' && (v[i] == '&' || v[i] == '$')) {
      int r = 0, ti = i;
      if (v[i + 2] != 'x') {
        for (i += 2; v[i] != ';' && v[i]; i++) {
          if ('0' <= v[i] && v[i]<='9') {
            r = r * 10 + v[i] - '0';
          } else {
            break;
          }
        }
      } else {
        for (i += 3; v[i] != ';' && v[i]; i++) {
          if (('0' <= v[i] && v[i]<='9') ||
              ('a' <= v[i] && v[i] <= 'f') ||
              ('A' <= v[i] && v[i] <= 'F')) {
            r = r * 16;
            if (v[i] <= '9') {
              r += v[i] -'0';
            } else if (v[i] <= 'F') {
              r += v[i] -'A' + 10;
            } else {
              r += v[i] -'a' + 10;
            }
          } else {
            break;
          }
        }
      }
      if (r == 0) {
        v[j++] = v[i = ti];
      } else {
        v[j++] = r;
        if (v[i] != ';') {
          i--;
        }
      }
    } else if (v[i] == '%' && '0' <= v[i + 1] && v[i + 1] <= '7' &&
                            (('0' <= v[i + 2] && v[i + 2] <= '9') ||
                             ('a' <= v[i + 2] && v[i + 2] <= 'f') ||
                             ('A' <= v[i + 2] && v[i + 2] <= 'F'))) {
      int r;
      if (v[i + 2] <= '9') {
        r = v[i + 2] -'0';
      } else if (v[i + 2] <= 'F') {
        r = v[i + 2] -'A' + 10;
      } else {
        r = v[i + 2] -'a' + 10;
      }
      r += (v[i + 1] - '0') * 16;
      i += 2;
      v[j++] = r;
    } else {
      v[j++] = v[i];
    }
  }
  v[j++] = 0;*/
}
