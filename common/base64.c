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

    Copyright 2011 Vkontakte Ltd
              2011 Anton Maydell
*/

#include <assert.h>
#include <string.h>
#include "base64.h"

int number_to_base62 (long long number, char *output, int olen) {
  int o = 0;
  static const char* const symbols = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  assert (number >= 0);
  if (!number) {
    if (o >= olen) return -1;
    output[o++] = '0';    
  }
  while (number) {
    if (o >= olen) return -1;    
    output[o++] =  symbols[(int) (number % 62)];    
    number /= 62;    
  }
  if (o >= olen) {
    return -1;
  }
  output[o] = 0;    
  
  int i = 0, j = o - 1;
  while (i < j) {
    char t = output[i]; output[i] = output[j]; output[j] = t;
    i++; 
    j--;
  }

  return 0;
}

static inline unsigned char next_input_uchar (const unsigned char *const input, int ilen, int *i) {
  if (*i >= ilen) { return 0; }
  return input[(*i)++];
}

static const char* const symbols64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";

int base64_encode (const unsigned char *const input, int ilen, char *output, int olen) {
  int i, j = 0;
  char buf[4];
  for (i = 0; i < ilen; ) {
    int old_i = i;
    int o = next_input_uchar (input, ilen, &i);
    o <<= 8;
    o |= next_input_uchar (input, ilen, &i);
    o <<= 8;
    o |= next_input_uchar (input, ilen, &i);
    int l = i - old_i;
    assert (l > 0 && l <= 3);
    int u;
    for (u = 3; u >= 0; u--) {
      buf[u] = symbols64[o & 63];
      o >>= 6;
    }
    if (l == 1) { 
      buf[2] = buf[3] = '='; 
    }
    else if (l == 2) { 
      buf[3] = '='; 
    }
    if (j + 3 >= olen) {
      return -1;
    }
    memcpy (&output[j], buf, 4);
    j += 4;
  }
  if (j >= olen) {
    return -1;
  }
  output[j++] = 0;
  return 0;
}

int base64_decode (const char *const input, unsigned char *output, int olen) {
  static int tbl_symbols64_initialized = 0;
  static char tbl_symbols64[256];
  int ilen = strlen (input);
  int i, j = 0;
  if (!tbl_symbols64_initialized) {
    memset (tbl_symbols64, 0xff, 256);
    for (i = 0; i <= 64; i++) {
      tbl_symbols64[(int) symbols64[i]] = i; 
    }
    tbl_symbols64_initialized = 1;
  }
  if (ilen & 3) {
    return -2;
  }
  char buf[3];
  for (i = 0; i < ilen; i += 4) {
    int o = 0, l = 3, u = 0;
    do {
      int c = tbl_symbols64[(unsigned char) input[i+u]];
      if (c < 0) {
        return -3;
      }
      if (c == 64) {
        switch (u) {
          case 0:
          case 1:
            return -4;
          case 2:
            if (tbl_symbols64[(unsigned char) input[i+3]] != 64) {
              return -5;
            }
            o <<= 12;
            l = 1;
            break;
          case 3:
            o <<= 6;
            l = 2;
            break;
        }
        break;
      }
      o <<= 6;
      o |= c;
    } while (++u < 4);
    u = 2;
    do {
      buf[u] = o & 255;
      o >>= 8;
    } while (--u >= 0);
    if (j + l > olen) {
      return -1;
    }
    memcpy (&output[j], buf, l);
    j += l;
  }
  return j;
}

int base64_to_base64url (const char *const input, char *output, int olen) {
  int i = 0;
  while (input[i] && i < olen) {
    if (input[i] == '+') {
      output[i] = '-';
    } else if (input[i] == '/') {
      output[i] = '_';
    } else if (input[i] != '=') {
      output[i] = input[i];
    } else {
      break;
    }
    i++;
  }
  if (i >= olen) {
    return -1;
  }
  output[i] = 0;
  return 0;
}

int base64url_to_base64 (const char *const input, char *output, int olen) {
  int i = 0;
  while (input[i] && i < olen) {
    if (input[i] == '-') {
      output[i] = '+';
    } else if (input[i] == '_') {
      output[i] = '/';
    } else {
      output[i] = input[i];
    }
    i++;
  }
  if (((i + 3) & -4) >= olen) {    
    return -1;
  }
  while (i & 3) {
    output[i++] = '=';
  }
  output[i] = 0;
  assert (i < olen);
  return 0;
}


static const char* const url_symbols64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

int base64url_encode (const unsigned char *const input, int ilen, char *output, int olen) {
  int i, j = 0;
  char buf[4];
  for (i = 0; i < ilen; ) {
    int old_i = i;
    int o = next_input_uchar (input, ilen, &i);
    o <<= 8;
    o |= next_input_uchar (input, ilen, &i);
    o <<= 8;
    o |= next_input_uchar (input, ilen, &i);
    int l = i - old_i;
    assert (l > 0 && l <= 3);
    int u;
    for (u = 3; u >= 0; u--) {
      buf[u] = url_symbols64[o & 63];
      o >>= 6;
    }
    l++;
    if (j + l >= olen) {
      return -1;
    }
    memcpy (&output[j], buf, l);
    j += l;
  }
  if (j >= olen) {
    return -1;
  }
  output[j++] = 0;
  return 0;
}

int base64url_decode (const char *const input, unsigned char *output, int olen) {
  static int tbl_url_symbols64_initialized = 0;
  static char tbl_url_symbols64[256];
  int ilen = strlen (input);
  int i, j = 0;
  if (!tbl_url_symbols64_initialized) {
    memset (tbl_url_symbols64, 0xff, 256);
    for (i = 0; i < 64; i++) {
      tbl_url_symbols64[(int) url_symbols64[i]] = i; 
    }
    tbl_url_symbols64_initialized = 1;
  }
  char buf[3];
  for (i = 0; i < ilen; i += 4) {
    int o = 0, l = 3, u = 0;
    do {
      if (i + u >= ilen) {
        switch (u) {
          case 0:
          case 1:
            return -4;
          case 2:
            o <<= 12;
            l = 1;
            break;
          case 3:
            o <<= 6;
            l = 2;
            break;
        }
        break;
      }
      int c = tbl_url_symbols64[(unsigned char) input[i+u]];
      if (c < 0) {
        return -3;
      }
      o <<= 6;
      o |= c;
    } while (++u < 4);
    u = 2;
    do {
      buf[u] = o & 255;
      o >>= 8;
    } while (--u >= 0);
    if (j + l > olen) {
      return -1;
    }
    memcpy (&output[j], buf, l);
    j += l;
  }
  return j;
}

