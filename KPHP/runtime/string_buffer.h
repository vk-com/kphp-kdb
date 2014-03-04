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

/*
 *
 *   Do not include with file directly
 *   Include kphp_core.h instead
 *
 */

class string_buffer {
  static const dl::size_type MIN_BUFFER_LEN = 266175;
  static const dl::size_type MAX_BUFFER_LEN = (1 << 24);
  inline string_buffer (const string_buffer &other);
  inline string_buffer &operator = (const string_buffer &other);

  char *buffer_end;
  char *buffer_begin;
  dl::size_type buffer_len;

  inline void resize (dl::size_type new_buffer_len);
  inline void reserve_at_least (dl::size_type new_buffer_len);

public:
  inline explicit string_buffer (dl::size_type buffer_len = 4000);

  inline string_buffer &clean (void);

  inline string_buffer &operator + (char c);
  inline string_buffer &operator + (const char *s);
  inline string_buffer &operator + (double f);
  inline string_buffer &operator + (const string &s);
  inline string_buffer &operator + (bool x);
  inline string_buffer &operator + (int x);
  inline string_buffer &operator + (unsigned int x);
  inline string_buffer &operator + (long long x);
  inline string_buffer &operator + (unsigned long long x);
  inline string_buffer &operator + (const var &v);

  inline string_buffer &append (const char *str, int len);

  inline void append_unsafe (const char *str, int len);

  inline void append_char (char c) __attribute__ ((always_inline));//unsafe

  template <class T>
  inline string_buffer &operator += (const T &s) {
    return *this + s;
  }

  inline void reserve (int len);

  inline dl::size_type size (void) const;

  inline char *buffer (void);
  inline const char *buffer (void) const;

  inline const char *c_str (void);
  inline string str (void) const;

  inline bool set_pos (int pos);

  inline ~string_buffer (void);
};

string_buffer static_SB __attribute__ ((weak));
string_buffer static_SB_spare __attribute__ ((weak));
