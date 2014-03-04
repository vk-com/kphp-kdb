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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

void string_to_utf8 (const unsigned char *s, int *v);
void good_string_to_utf8 (const unsigned char *s, int *v);

void put_string_utf8 (const int *v, char *s);

unsigned int convert_prep (unsigned int x);
int remove_diacritics (int c);


#ifdef __cplusplus
}
#endif


static inline int get_char_utf8 (unsigned int *x, const char *s) {
#define CHECK(condition) if (!(condition)) {*x = 0xFFFFFFFF; return -1;}
  int a = (unsigned char)s[0];
  if ((a & 0x80) == 0) {
    *x = a;
    return (a != 0);
  }

  CHECK ((a & 0x40) != 0);

  int b = (unsigned char)s[1];
  CHECK((b & 0xc0) == 0x80);
  if ((a & 0x20) == 0) {
    CHECK((a & 0x1e) > 0);
    *x = (((a & 0x1f) << 6) | (b & 0x3f));
    return 2;
  }

  int c = (unsigned char)s[2];
  CHECK((c & 0xc0) == 0x80);
  if ((a & 0x10) == 0) {
    CHECK(((a & 0x0f) | (b & 0x20)) > 0);
    *x = (((a & 0x0f) << 12) | ((b & 0x3f) << 6) | (c & 0x3f));
    return 3;
  }

  int d = (unsigned char)s[3];
  CHECK((d & 0xc0) == 0x80);
  if ((a & 0x08) == 0) {
    CHECK(((a & 0x07) | (b & 0x30)) > 0);
    *x = (((a & 0x07) << 18) | ((b & 0x3f) << 12) | ((c & 0x3f) << 6) | (d & 0x3f));
    return 4;
  }
  
  int e = (unsigned char)s[4];
  CHECK((e & 0xc0) == 0x80);
  if ((a & 0x04) == 0) {
    CHECK(((a & 0x03) | (b & 0x38)) > 0);
    *x = (((a & 0x03) << 24) | ((b & 0x3f) << 18) | ((c & 0x3f) << 12) | ((d & 0x3f) << 6) | (e & 0x3f));
    return 5;
  }
  
  int f = (unsigned char)s[5];
  CHECK((f & 0xc0) == 0x80);
  if ((a & 0x02) == 0) {
    CHECK(((a & 0x01) | (b & 0x3c)) > 0);
    *x = (((a & 0x01) << 30) | ((b & 0x3f) << 24) | ((c & 0x3f) << 18) | ((d & 0x3f) << 12) | ((e & 0x3f) << 6) | (f & 0x3f));
    return 6;
  }

  CHECK(0);
#undef CHECK
}

static inline int put_char_utf8 (unsigned int x, char *s) {
  if (x <= 0x7f) {
    s[0] = x;
    return 1;
  } else if (x <= 0x7ff) {
    s[0] = ((x >>  6) | 0xc0) & 0xdf;
    s[1] = ((x      ) | 0x80) & 0xbf;
    return 2;
  } else if (x <= 0xffff) {
    s[0] = ((x >> 12) | 0xe0) & 0xef;
    s[1] = ((x >>  6) | 0x80) & 0xbf;
    s[2] = ((x      ) | 0x80) & 0xbf;
    return 3;
  } else if (x <= 0x1fffff) {
    s[0] = ((x >> 18) | 0xf0) & 0xf7;
    s[1] = ((x >> 12) | 0x80) & 0xbf;
    s[2] = ((x >>  6) | 0x80) & 0xbf;
    s[3] = ((x      ) | 0x80) & 0xbf;
    return 4;
  } else {
    //ASSERT(0, "bad output");
  }
  return 0;
}

