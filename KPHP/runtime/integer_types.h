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

#include "kphp_core.h"

//We want these types to be as fast as native types, no all definitions will be in header

class Long {
  inline void convert_from (const string &s);

public:
  long long l;

  explicit inline Long (void);
  inline Long (long long l);

  inline Long (int i);
  explicit inline Long (double f);
  explicit inline Long (const string &s);
  explicit inline Long (const var &v);

  inline Long (const Long &other);
  inline Long& operator = (Long other);
};

inline Long f$labs (Long lhs);

inline Long f$ldiv (Long lhs, Long rhs);

inline Long f$lmod (Long lhs, Long rhs);

inline Long f$lpow (Long lhs, int deg);

inline Long f$ladd (Long lhs, Long rhs);

inline Long f$lsub (Long lhs, Long rhs);

inline Long f$lmul (Long lhs, Long rhs);

inline Long f$lshl (Long lhs, int rhs);

inline Long f$lshr (Long lhs, int rhs);

inline Long f$lnot (Long lhs);

inline Long f$lor (Long lhs, Long rhs);

inline Long f$land (Long lhs, Long rhs);

inline Long f$lxor (Long lhs, Long rhs);

inline int f$lcomp (Long lhs, Long rhs);

inline Long f$longval (const long long &val);

inline Long f$longval (const bool &val);

inline Long f$longval (const int &val);

inline Long f$longval (const double &val);

inline Long f$longval (const string &val);

inline Long f$longval (const var &val);

template <class T>
inline Long f$new_Long (const T &val);

inline bool f$boolval (Long val);

inline int f$intval (Long val);

inline int f$safe_intval (Long val);

inline double f$floatval (Long val);

inline string f$strval (Long val);

inline string_buffer &operator + (string_buffer &buf, Long x);


class ULong {
  inline void convert_from (const string &s);

public:
  unsigned long long l;

  explicit inline ULong (void);
  inline ULong (unsigned long long l);

  inline ULong (int i);
  explicit inline ULong (double f);
  explicit inline ULong (const string &s);
  explicit inline ULong (const var &v);

  inline ULong (const ULong &other);
  inline ULong& operator = (ULong other);
};

inline ULong f$uldiv (ULong lhs, ULong rhs);

inline ULong f$ulmod (ULong lhs, ULong rhs);

inline ULong f$ulpow (ULong lhs, int deg);

inline ULong f$uladd (ULong lhs, ULong rhs);

inline ULong f$ulsub (ULong lhs, ULong rhs);

inline ULong f$ulmul (ULong lhs, ULong rhs);

inline ULong f$ulshl (ULong lhs, int rhs);

inline ULong f$ulshr (ULong lhs, int rhs);

inline ULong f$ulnot (ULong lhs);

inline ULong f$ulor (ULong lhs, ULong rhs);

inline ULong f$uland (ULong lhs, ULong rhs);

inline ULong f$ulxor (ULong lhs, ULong rhs);

inline int f$ulcomp (ULong lhs, ULong rhs);

inline ULong f$ulongval (const unsigned long long &val);

inline ULong f$ulongval (const bool &val);

inline ULong f$ulongval (const int &val);

inline ULong f$ulongval (const double &val);

inline ULong f$ulongval (const string &val);

inline ULong f$ulongval (const var &val);

template <class T>
inline ULong f$new_ULong (const T &val);

inline bool f$boolval (ULong val);

inline int f$intval (ULong val);

inline int f$safe_intval (ULong val);

inline double f$floatval (ULong val);

inline string f$strval (ULong val);

inline string_buffer &operator + (string_buffer &buf, ULong x);


class UInt {
  inline void convert_from (const string &s);

public:
  unsigned int l;

  explicit inline UInt (void);
  inline UInt (unsigned int l);

  inline UInt (int i);
  explicit inline UInt (double f);
  explicit inline UInt (const string &s);
  explicit inline UInt (const var &v);

  inline UInt (const UInt &other);
  inline UInt& operator = (UInt other);
};

inline UInt f$uidiv (UInt lhs, UInt rhs);

inline UInt f$uimod (UInt lhs, UInt rhs);

inline UInt f$uipow (UInt lhs, int deg);

inline UInt f$uiadd (UInt lhs, UInt rhs);

inline UInt f$uisub (UInt lhs, UInt rhs);

inline UInt f$uimul (UInt lhs, UInt rhs);

inline UInt f$uishl (UInt lhs, int rhs);

inline UInt f$uishr (UInt lhs, int rhs);

inline UInt f$uinot (UInt lhs);

inline UInt f$uior (UInt lhs, UInt rhs);

inline UInt f$uiand (UInt lhs, UInt rhs);

inline UInt f$uixor (UInt lhs, UInt rhs);

inline int f$uicomp (UInt lhs, UInt rhs);

inline UInt f$uintval (const unsigned int &val);

inline UInt f$uintval (const bool &val);

inline UInt f$uintval (const int &val);

inline UInt f$uintval (const double &val);

inline UInt f$uintval (const string &val);

inline UInt f$uintval (const var &val);

template <class T>
inline UInt f$new_UInt (const T &val);

inline bool f$boolval (UInt val);

inline int f$intval (UInt val);

inline int f$safe_intval (UInt val);

inline double f$floatval (UInt val);

inline string f$strval (UInt val);

inline string_buffer &operator + (string_buffer &buf, UInt x);


inline const Long &f$longval (const Long &val);

inline Long f$longval (ULong val);

inline Long f$longval (UInt val);

inline ULong f$ulongval (Long val);

inline const ULong &f$ulongval (const ULong &val);

inline ULong f$ulongval (UInt val);

inline const UInt &f$uintval (const UInt &val);

inline UInt f$uintval (Long val);

inline UInt f$uintval (ULong val);


/*
 *
 *     IMPLEMENTATION
 *
 */


void Long::convert_from (const string &s) {
  int mul = 1, len = s.size(), cur = 0;
  if (len > 0 && (s[0] == '-' || s[0] == '+')) {
    if (s[0] == '-') {
      mul = -1;
    }
    cur++;
  }
  while (cur + 1 < len && s[cur] == '0') {
    cur++;
  }

  bool need_warning = (cur == len || len - cur > 19);
  l = 0;
  while ('0' <= s[cur] && s[cur] <= '9') {
    l = l * 10 + (s[cur++] - '0');
  }
  if (need_warning || cur < len || l % 10 != s[len - 1] - '0') {
    php_warning ("Wrong conversion from string \"%s\" to Long\n", s.c_str());
  }
  l *= mul;
}

Long::Long (void): l (0) {
}

Long::Long (long long l): l (l) {
}

Long::Long (int i): l (i) {
}

Long::Long (double f): l ((long long)f) {
}

Long::Long (const string &s) {
  convert_from (s);
}

Long::Long (const var &v) {
  if (likely (v.is_int())) {
    l = v.to_int();
  } else if (likely (v.is_string())) {
    convert_from (v.to_string());
  } else if (likely (v.is_float())) {
    l = (long long)v.to_float();
  } else {
    l = v.to_int();
  }
}

Long::Long (const Long &other): l (other.l) {
}

Long& Long::operator = (Long other) {
  l = other.l;
  return *this;
}

Long f$labs (Long lhs) {
  return Long (lhs.l < 0 ? -lhs.l : lhs.l);
}

Long f$ldiv (Long lhs, Long rhs) {
  if (rhs.l == 0) {
    php_warning ("Long division by zero");
    return 0;
  }

  return Long (lhs.l / rhs.l);
}

Long f$lmod (Long lhs, Long rhs) {
  if (rhs.l == 0) {
    php_warning ("Long modulo by zero");
    return 0;
  }

  return Long (lhs.l % rhs.l);
}

Long f$lpow (Long lhs, int deg) {
  long long result = 1;
  long long mul = lhs.l;

  while (deg > 0) {
    if (deg & 1) {
      result *= mul;
    }
    mul *= mul;
    deg >>= 1;
  }

  return Long (result);
}

Long f$ladd (Long lhs, Long rhs) {
  return Long (lhs.l + rhs.l);
}

Long f$lsub (Long lhs, Long rhs) {
  return Long (lhs.l - rhs.l);
}

Long f$lmul (Long lhs, Long rhs) {
  return Long (lhs.l * rhs.l);
}

Long f$lshl (Long lhs, int rhs) {
  return Long (lhs.l << rhs);
}

Long f$lshr (Long lhs, int rhs) {
  return Long (lhs.l >> rhs);
}

Long f$lnot (Long lhs) {
  return Long (~lhs.l);
}

Long f$lor (Long lhs, Long rhs) {
  return Long (lhs.l | rhs.l);
}

Long f$land (Long lhs, Long rhs) {
  return Long (lhs.l & rhs.l);
}

Long f$lxor (Long lhs, Long rhs) {
  return Long (lhs.l ^ rhs.l);
}

int f$lcomp (Long lhs, Long rhs) {
  if (lhs.l < rhs.l) {
    return -1;
  }
  return lhs.l > rhs.l;
}

Long f$longval (const long long &val) {
  return Long (val);
}

Long f$longval (const bool &val) {
  return Long (val);
}

Long f$longval (const int &val) {
  return Long (val);
}

Long f$longval (const double &val) {
  return Long (val);
}

Long f$longval (const string &val) {
  return Long (val);
}

Long f$longval (const var &val) {
  return Long (val);
}

template <class T>
Long f$new_Long (const T &val) {
  return f$longval (val);
}

bool f$boolval (Long val) {
  return val.l;
}

int f$intval (Long val) {
  return (int)val.l;
}

int f$safe_intval (Long val) {
  if ((int)val.l != val.l) {
    php_warning ("Integer overflow on converting %lld to int", val.l);
  }
  return (int)val.l;
}

double f$floatval (Long val) {
  return (double)val.l;
}

string f$strval (Long val) {
  int negative = 0;
  long long result = val.l;
  if (result < 0) {
    negative = 1;
    result = -result;
    if (result < 0) {
      return string ("-9223372036854775808", 20);
    }
  }

  char buf[20], *end_buf = buf + 20;
  do {
    *--end_buf = (char)(result % 10 + '0');
    result /= 10;
  } while (result > 0);
  if (negative) {
    *--end_buf = '-';
  }

  return string (end_buf, (dl::size_type)(buf + 20 - end_buf));
}

string_buffer &operator + (string_buffer &buf, Long x) {
  return buf + x.l;
}


void ULong::convert_from (const string &s) {
  bool need_warning = false;
  int mul = 1, len = s.size(), cur = 0;
  if (len > 0 && (s[0] == '-' || s[0] == '+')) {
    if (s[0] == '-') {
      mul = -1;
      need_warning = true;
    }
    cur++;
  }
  while (cur + 1 < len && s[cur] == '0') {
    cur++;
  }

  need_warning |= (cur == len || len - cur > 20);
  l = 0;
  while ('0' <= s[cur] && s[cur] <= '9') {
    l = l * 10 + (s[cur++] - '0');
  }
  if (need_warning || cur < len || l % 10 != (unsigned long long)(s[len - 1] - '0')) {
    php_warning ("Wrong conversion from string \"%s\" to ULong\n", s.c_str());
  }
  l *= mul;
}

ULong::ULong (void): l (0) {
}

ULong::ULong (unsigned long long l): l (l) {
}

ULong::ULong (int i): l ((unsigned long long)i) {
}

ULong::ULong (double f): l ((unsigned long long)f) {
}

ULong::ULong (const string &s) {
  convert_from (s);
}

ULong::ULong (const var &v) {
  if (likely (v.is_int())) {
    l = (unsigned int)v.to_int();
  } else if (likely (v.is_string())) {
    convert_from (v.to_string());
  } else if (likely (v.is_float())) {
    l = (unsigned long long)v.to_float();
  } else {
    l = v.to_int();
  }
}

ULong::ULong (const ULong &other): l (other.l) {
}

ULong& ULong::operator = (ULong other) {
  l = other.l;
  return *this;
}

ULong f$uldiv (ULong lhs, ULong rhs) {
  if (rhs.l == 0) {
    php_warning ("ULong division by zero");
    return 0;
  }

  return ULong (lhs.l / rhs.l);
}

ULong f$ulmod (ULong lhs, ULong rhs) {
  if (rhs.l == 0) {
    php_warning ("ULong modulo by zero");
    return 0;
  }

  return ULong (lhs.l % rhs.l);
}

ULong f$ulpow (ULong lhs, int deg) {
  unsigned long long result = 1;
  unsigned long long mul = lhs.l;

  while (deg > 0) {
    if (deg & 1) {
      result *= mul;
    }
    mul *= mul;
    deg >>= 1;
  }

  return ULong (result);
}

ULong f$uladd (ULong lhs, ULong rhs) {
  return ULong (lhs.l + rhs.l);
}

ULong f$ulsub (ULong lhs, ULong rhs) {
  return ULong (lhs.l - rhs.l);
}

ULong f$ulmul (ULong lhs, ULong rhs) {
  return ULong (lhs.l * rhs.l);
}

ULong f$ulshl (ULong lhs, int rhs) {
  return ULong (lhs.l << rhs);
}

ULong f$ulshr (ULong lhs, int rhs) {
  return ULong (lhs.l >> rhs);
}

ULong f$ulnot (ULong lhs) {
  return ULong (~lhs.l);
}

ULong f$ulor (ULong lhs, ULong rhs) {
  return ULong (lhs.l | rhs.l);
}

ULong f$uland (ULong lhs, ULong rhs) {
  return ULong (lhs.l & rhs.l);
}

ULong f$ulxor (ULong lhs, ULong rhs) {
  return ULong (lhs.l ^ rhs.l);
}

int f$ulcomp (ULong lhs, ULong rhs) {
  if (lhs.l < rhs.l) {
    return -1;
  }
  return lhs.l > rhs.l;
}

ULong f$ulongval (const unsigned long long &val) {
  return ULong (val);
}

ULong f$ulongval (const bool &val) {
  return ULong (val);
}

ULong f$ulongval (const int &val) {
  return ULong (val);
}

ULong f$ulongval (const double &val) {
  return ULong (val);
}

ULong f$ulongval (const string &val) {
  return ULong (val);
}

ULong f$ulongval (const var &val) {
  return ULong (val);
}

template <class T>
ULong f$new_ULong (const T &val) {
  return f$ulongval (val);
}

bool f$boolval (ULong val) {
  return (bool)val.l;
}

int f$intval (ULong val) {
  return (int)val.l;
}

int f$safe_intval (ULong val) {
  if (val.l >= 2147483648llu) {
    php_warning ("Integer overflow on converting %llu to int", val.l);
  }
  return (int)val.l;
}

double f$floatval (ULong val) {
  return (double)val.l;
}

string f$strval (ULong val) {
  unsigned long long result = val.l;

  char buf[20], *end_buf = buf + 20;
  do {
    *--end_buf = (char)(result % 10 + '0');
    result /= 10;
  } while (result > 0);

  return string (end_buf, (dl::size_type)(buf + 20 - end_buf));
}

string_buffer &operator + (string_buffer &buf, ULong x) {
  return buf + x.l;
}


void UInt::convert_from (const string &s) {
  bool need_warning = false;
  int mul = 1, len = s.size(), cur = 0;
  if (len > 0 && (s[0] == '-' || s[0] == '+')) {
    if (s[0] == '-') {
      mul = -1;
      need_warning = true;
    }
    cur++;
  }
  while (cur + 1 < len && s[cur] == '0') {
    cur++;
  }

  need_warning |= (cur == len || len - cur > 10);
  l = 0;
  while ('0' <= s[cur] && s[cur] <= '9') {
    l = l * 10 + (s[cur++] - '0');
  }
  if (need_warning || cur < len || l % 10 != (unsigned long long)(s[len - 1] - '0')) {
    php_warning ("Wrong conversion from string \"%s\" to UInt\n", s.c_str());
  }
  l *= mul;
}

UInt::UInt (void): l (0) {
}

UInt::UInt (unsigned int l): l (l) {
}

UInt::UInt (int i): l (i) {
}

UInt::UInt (double f): l ((unsigned int)f) {
}

UInt::UInt (const string &s) {
  convert_from (s);
}

UInt::UInt (const var &v) {
  if (likely (v.is_int())) {
    l = (unsigned int)v.to_int();
  } else if (likely (v.is_string())) {
    convert_from (v.to_string());
  } else if (likely (v.is_float())) {
    l = (unsigned int)v.to_float();
  } else {
    l = (unsigned int)v.to_int();
  }
}

UInt::UInt (const UInt &other): l (other.l) {
}

UInt& UInt::operator = (UInt other) {
  l = other.l;
  return *this;
}

UInt f$uidiv (UInt lhs, UInt rhs) {
  if (rhs.l == 0) {
    php_warning ("UInt division by zero");
    return 0;
  }

  return UInt (lhs.l / rhs.l);
}

UInt f$uimod (UInt lhs, UInt rhs) {
  if (rhs.l == 0) {
    php_warning ("UInt modulo by zero");
    return 0;
  }

  return UInt (lhs.l % rhs.l);
}

UInt f$uipow (UInt lhs, int deg) {
  unsigned int result = 1;
  unsigned int mul = lhs.l;

  while (deg > 0) {
    if (deg & 1) {
      result *= mul;
    }
    mul *= mul;
    deg >>= 1;
  }

  return UInt (result);
}

UInt f$uiadd (UInt lhs, UInt rhs) {
  return UInt (lhs.l + rhs.l);
}

UInt f$uisub (UInt lhs, UInt rhs) {
  return UInt (lhs.l - rhs.l);
}

UInt f$uimul (UInt lhs, UInt rhs) {
  return UInt (lhs.l * rhs.l);
}

UInt f$uishl (UInt lhs, int rhs) {
  return UInt (lhs.l << rhs);
}

UInt f$uishr (UInt lhs, int rhs) {
  return UInt (lhs.l >> rhs);
}

UInt f$uinot (UInt lhs) {
  return UInt (~lhs.l);
}

UInt f$uior (UInt lhs, UInt rhs) {
  return UInt (lhs.l | rhs.l);
}

UInt f$uiand (UInt lhs, UInt rhs) {
  return UInt (lhs.l & rhs.l);
}

UInt f$uixor (UInt lhs, UInt rhs) {
  return UInt (lhs.l ^ rhs.l);
}

int f$uicomp (UInt lhs, UInt rhs) {
  if (lhs.l < rhs.l) {
    return -1;
  }
  return lhs.l > rhs.l;
}

UInt f$uintval (const unsigned int &val) {
  return UInt (val);
}

UInt f$uintval (const bool &val) {
  return UInt (val);
}

UInt f$uintval (const int &val) {
  return UInt (val);
}

UInt f$uintval (const double &val) {
  return UInt (val);
}

UInt f$uintval (const string &val) {
  return UInt (val);
}

UInt f$uintval (const var &val) {
  return UInt (val);
}

template <class T>
UInt f$new_UInt (const T &val) {
  return f$uintval (val);
}

bool f$boolval (UInt val) {
  return val.l;
}

int f$intval (UInt val) {
  return val.l;
}

int f$safe_intval (UInt val) {
  if (val.l >= 2147483648u) {
    php_warning ("Integer overflow on converting %u to int", val.l);
  }
  return val.l;
}

double f$floatval (UInt val) {
  return val.l;
}

string f$strval (UInt val) {
  unsigned int result = val.l;

  char buf[20], *end_buf = buf + 20;
  do {
    *--end_buf = (char)(result % 10 + '0');
    result /= 10;
  } while (result > 0);

  return string (end_buf, (dl::size_type)(buf + 20 - end_buf));
}

string_buffer &operator + (string_buffer &buf, UInt x) {
  return buf + x.l;
}


const Long &f$longval (const Long &val) {
  return val;
}

Long f$longval (ULong val) {
  return (long long)val.l;
}

Long f$longval (UInt val) {
  return (long long)val.l;
}

ULong f$ulongval (Long val) {
  return (unsigned long long)val.l;
}

const ULong &f$ulongval (const ULong &val) {
  return val;
}

ULong f$ulongval (UInt val) {
  return (unsigned long long)val.l;
}

const UInt &f$uintval (const UInt &val) {
  return val;
}

UInt f$uintval (Long val) {
  return (unsigned int)val.l;
}

UInt f$uintval (ULong val) {
  return (unsigned int)val.l;
}


