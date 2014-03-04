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

#define _FILE_OFFSET_BITS 64

#include "math_functions.h"

#include <sys/time.h>//gettimeofday

#include "string_functions.h"//lhex_digits, TODO

string f$decbin (int number) {
  unsigned int v = number;

  char s[66];
  int i = 65;

  do {
    s[--i] = (char)((v & 1) + '0');
    v >>= 1;
  } while (v > 0);

  return string (s + i, (dl::size_type)(65 - i));
}

string f$dechex (int number) {
  unsigned int v = number;

  char s[17];
  int i = 16;

  do {
    s[--i] = lhex_digits[v & 15];
    v >>= 4;
  } while (v > 0);

  return string (s + i, 16 - i);
}

int f$hexdec (const string &number) {
  unsigned int v = 0;
  for (int i = 0; i < (int)number.size(); i++) {
    char c = number[i];
    if ('0' <= c && c <= '9') {
      v = v * 16 + c - '0';
    } else {
      c |= 0x20;
      if ('a' <= c && c <= 'f') {
        v = v * 16 + c - 'a' + 10;
      }
    }
  }

  return v;
}

static int make_seed (void) {
  struct timespec T;
  php_assert (clock_gettime (CLOCK_REALTIME, &T) >= 0);
  return ((int)T.tv_nsec * 123456789) ^ ((int)T.tv_sec * 987654321);
}

void f$srand (int seed) {
  if (seed == INT_MIN) {
    seed = make_seed();
  }
  srand (seed);
}

int f$rand (void) {
  return rand();
}

int f$rand (int l, int r) {
  if (l > r) {
    return 0;
  }
  unsigned int diff = (unsigned int)r - (unsigned int)l + 1u;
  unsigned int shift;
  if (diff <= RAND_MAX + 1u) {
    unsigned int upper_bound = ((RAND_MAX + 1u) / diff) * diff;
    unsigned int r;
    do {
      r = rand();
    } while (r > upper_bound);
    shift = r % diff;
  } else {
    shift = f$rand (0, (diff >> 1) - 1) * 2u + (rand() & 1);
  }
  return l + shift;
}

int f$getrandmax (void) {
  return RAND_MAX;
}

void f$mt_srand (int seed) {
  if (seed == INT_MIN) {
    seed = make_seed();
  }
  srand (seed);
}

int f$mt_rand (void) {
  return rand();
}

int f$mt_rand (int l, int r) {
  return f$rand (l, r);
}

int f$mt_getrandmax (void) {
  return RAND_MAX;
}


var f$min (const var &a) {
  return f$min (a.as_array ("min", 1));
}

var f$max (const var &a) {
  return f$max (a.as_array ("max", 1));
}


var f$abs (const var &v) {
  var num = v.to_numeric();
  if (num.is_int()) {
    return abs (num.to_int());
  }
  return fabs (num.to_float());
}

string f$base_convert (const string &number, int frombase, int tobase) {
  if (frombase < 2 || frombase > 36) {
    php_warning ("Wrong parameter frombase (%d) in function base_convert", frombase);
    return number;
  }
  if (tobase < 2 || tobase > 36) {
    php_warning ("Wrong parameter tobase (%d) in function base_convert", tobase);
    return number;
  }

  int l = number.size(), f = 0;
  string result;
  if (number[0] == '-' || number[0] == '+') {
    f++;
    l--;
    if (number[0] == '-') {
      result.push_back ('-');
    }
  }
  if (l == 0) {
    php_warning ("Wrong parameter number (%s) in function base_convert", number.c_str());
    return number;
  }

  const char *digits = "0123456789abcdefghijklmnopqrstuvwxyz";

  string n (l, false);
  for (int i = 0; i < l; i++) {
    const char *s = (const char *)memchr (digits, tolower (number[i + f]), 36);
    if (s == NULL || (int)(s - digits) >= frombase) {
      php_warning ("Wrong character '%c' at position %d in parameter number (%s) in function base_convert", number[i + f], i + f, number.c_str());
      return number;
    }
    n[i] = (char)(s - digits);
  }

  int um, st = 0;
  while (st < l) {
    um = 0;
    for (int i = st; i < l; i++) {
      um = um * frombase + n[i];
      n[i] = (char)(um / tobase);
      um %= tobase;
    }
    while (st < l && n[st] == 0) {
      st++;
    }
    result.push_back (digits[um]);
  }

  int i = f, j = (int)result.size() - 1;
  while (i < j) {
    swap (result[i++], result[j--]);
  }

  return result;
}

int pow_int (int x, int y) {
  int res = 1;
  while (y > 0) {
    if (y & 1) {
      res *= x;
    }
    x *= x;
    y >>= 1;
  }
  return res;
}

double pow_float (double x, double y) {
  if (x < 0.0) {
    return 0.0;
  }

  if (x == 0.0) {
    return y == 0.0;
  }

  return pow (x, y);
}

var f$pow (const var &num, const var &deg) {
  if (num.is_int() && deg.is_int() && deg.to_int() >= 0) {
    return pow_int (num.to_int(), deg.to_int());
  } else {
    return pow_float (num.to_float(), deg.to_float());
  }
}

double f$round (double v, int precision) {
  if (abs (precision) > 100) {
    php_warning ("Wrong parameter precision (%d) in function round", precision);
    return v;
  }

  double mul = pow (10.0, (double)precision);
  return round (v * mul) / mul;
}
