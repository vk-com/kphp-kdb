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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#pragma once

#include <climits>

#define FAST_EXCEPTIONS

const int STRLEN_WARNING_FLAG = 1 << 30;
const int STRLEN_OBJECT       = -3;
const int STRLEN_ERROR        = -2;
const int STRLEN_DYNAMIC      = -1;
const int STRLEN_UNKNOWN      = STRLEN_ERROR;
const int STRLEN_FALSE        = 0;
const int STRLEN_FALSE_       = STRLEN_FALSE;
const int STRLEN_BOOL         = 1;
const int STRLEN_BOOL_        = STRLEN_BOOL;
const int STRLEN_INT          = 11;
const int STRLEN_FLOAT        = 21;
const int STRLEN_ARRAY        = 5;
const int STRLEN_ARRAY_       = STRLEN_ARRAY | STRLEN_WARNING_FLAG;
const int STRLEN_STRING       = STRLEN_DYNAMIC;
const int STRLEN_VAR          = STRLEN_DYNAMIC;
const int STRLEN_UINT         = STRLEN_OBJECT;
const int STRLEN_LONG         = STRLEN_OBJECT;
const int STRLEN_ULONG        = STRLEN_OBJECT;
const int STRLEN_MC           = STRLEN_ERROR;
const int STRLEN_DB           = STRLEN_ERROR;
const int STRLEN_RPC          = STRLEN_ERROR;
const int STRLEN_EXCEPTION    = STRLEN_OBJECT;
const int STRLEN_CLASS        = STRLEN_ERROR;
const int STRLEN_VOID         = STRLEN_ERROR;
const int STRLEN_ANY          = STRLEN_ERROR;
const int STRLEN_CREATE_ANY   = STRLEN_ERROR;

inline int string_hash (const char *p, int l) __attribute__ ((always_inline));

int string_hash (const char *p, int l) {
  static const unsigned int HASH_MUL_ = 1915239017;
  unsigned int hash = 2147483648u;

  int prev = (l & 3);
  for (int i = 0; i < prev; i++) {
    hash = hash * HASH_MUL_ + p[i];
  }

  const unsigned int *p_uint = (unsigned int *)(p + prev);
  l >>= 2;
  while (l-- > 0) {
    hash = hash * HASH_MUL_ + *p_uint++;
  }
  return (int)hash;
}

inline bool php_is_int (const char *s, int l) __attribute__ ((always_inline));

bool php_is_int (const char *s, int l) {
  if ((s[0] - '-') * (s[0] - '+') == 0) { // no need to check l > 0
    s++;
    l--;

    if ((unsigned int)(s[0] - '1') > 8) {
      return false;
    }
  } else {
    if ((unsigned int)(s[0] - '1') > 8) {
      return l == 1 && s[0] == '0';
    }
  }

  if ((unsigned int)(l - 1) > 9) {
    return false;
  }
  if (l == 10) {
    int val = 0;
    for (int j = 1; j < l; j++) {
      if (s[j] > '9' || s[j] < '0') {
        return false;
      }
      val = val * 10 + s[j] - '0';
    }

    if (s[0] != '2') {
      return s[0] == '1';
    }
    return val < 147483648;
  }

  for (int j = 1; j < l; j++) {
    if (s[j] > '9' || s[j] < '0') {
      return false;
    }
  }
  return true;
}

inline bool php_try_to_int (const char *s, int l, int *val) __attribute__ ((always_inline));

bool php_try_to_int (const char *s, int l, int *val) {
  int mul;
  if (s[0] == '-') { // no need to check l > 0
    mul = -1;
    s++;
    l--;

    if ((unsigned int)(s[0] - '1') > 8) {
      return false;
    }
  } else {
    if ((unsigned int)(s[0] - '1') > 8) {
      *val = 0;
      return l == 1 && s[0] == '0';
    }
    mul = 1;
  }

  if ((unsigned int)(l - 1) > 9) {
    return false;
  }
  if (l == 10) {
    if (s[0] > '2') {
      return false;
    }

    *val = s[0] - '0';
    for (int j = 1; j < l; j++) {
      if (s[j] > '9' || s[j] < '0') {
        return false;
      }
      *val = *val * 10 + s[j] - '0';
    }

    if (*val > 0 || (*val == -2147483648 && mul == -1)) {
      *val = *val * mul;
      return true;
    }
    return false;
  }

  *val = s[0] - '0';
  for (int j = 1; j < l; j++) {
    if (s[j] > '9' || s[j] < '0') {
      return false;
    }
    *val = *val * 10 + s[j] - '0';
  }

  *val *= mul;
  return true;
}

//returns len of raw string representation or -1 on error
inline int string_raw_len (int src_len) {
  if (src_len < 0 || src_len >= (1 << 30) - 13) {
    return -1;
  }

  return src_len + 13;
}

//returns len of raw string representation and writes it to dest or returns -1 on error
inline int string_raw (char *dest, int dest_len, const char *src, int src_len) {
  int raw_len = string_raw_len (src_len);
  if (raw_len == -1 || raw_len > dest_len) {
    return -1;
  }
  int *dest_int = reinterpret_cast <int *> (dest);
  dest_int[0] = src_len;
  dest_int[1] = src_len;
  dest_int[2] = 0;
  memcpy (dest + 3 * sizeof (int), src, src_len);
  dest[3 * sizeof (int) + src_len] = '\0';

  return raw_len;
}

