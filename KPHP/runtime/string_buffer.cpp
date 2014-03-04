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

inline void string_buffer::resize (dl::size_type new_buffer_len) {
  if (new_buffer_len < MIN_BUFFER_LEN) {
    new_buffer_len = MIN_BUFFER_LEN;
  }
  if (new_buffer_len >= MAX_BUFFER_LEN) {
    if (buffer_len + 1 < MAX_BUFFER_LEN) {
      new_buffer_len = MAX_BUFFER_LEN - 1;
    } else {
      php_critical_error ("maximum buffer size exceeded. buffer_len = %d, new_buffer_len = %d", buffer_len, new_buffer_len);
    }
  }

  dl::size_type current_len = size();
  dl::static_reallocate ((void **)&buffer_begin, new_buffer_len, &buffer_len);
  buffer_end = buffer_begin + current_len;
}

inline void string_buffer::reserve_at_least (dl::size_type need) {
  dl::size_type new_buffer_len = need + size();
  while (unlikely (buffer_len < new_buffer_len)) {
    resize (((new_buffer_len * 2 + 1 + 64) | 4095) - 64);
  }
}

string_buffer::string_buffer (dl::size_type buffer_len):
    buffer_end ((char *)dl::static_allocate (buffer_len)),
    buffer_begin (buffer_end),
    buffer_len (buffer_len) {
}

string_buffer &string_buffer::clean (void) {
  buffer_end = buffer_begin;
  return *this;
}

string_buffer &string_buffer::operator + (char c) {
  reserve_at_least (1);

  *buffer_end++ = c;

  return *this;
}


string_buffer &string_buffer::operator + (const char *s) {
  while (*s != 0) {
    reserve_at_least (1);

    *buffer_end++ = *s++;
  }

  return *this;
}

string_buffer &string_buffer::operator + (double f) {
  return *this + string (f);
}

string_buffer &string_buffer::operator + (const string &s) {
  dl::size_type l = s.size();
  reserve_at_least (l);

  memcpy (buffer_end, s.c_str(), l);
  buffer_end += l;

  return *this;
}

string_buffer &string_buffer::operator + (bool x) {
  if (x) {
    reserve_at_least (1);
    *buffer_end++ = '1';
  }

  return *this;
}

string_buffer &string_buffer::operator + (int x) {
  reserve_at_least (11);

  if (x < 0) {
    if (x == INT_MIN) {
      append ("-2147483648", 11);
      return *this;
    }
    x = -x;
    *buffer_end++ = '-';
  }

  char *left = buffer_end;
  do {
    *buffer_end++ = (char)(x % 10 + '0');
    x /= 10;
  } while (x > 0);

  char *right = buffer_end - 1;
  while (left < right) {
    char t = *left;
    *left++ = *right;
    *right-- = t;
  }

  return *this;
}

string_buffer &string_buffer::operator + (unsigned int x) {
  reserve_at_least (10);

  char *left = buffer_end;
  do {
    *buffer_end++ = (char)(x % 10 + '0');
    x /= 10;
  } while (x > 0);

  char *right = buffer_end - 1;
  while (left < right) {
    char t = *left;
    *left++ = *right;
    *right-- = t;
  }

  return *this;
}

string_buffer &string_buffer::operator + (long long x) {
  reserve_at_least (20);

  if (x < 0) {
    if (x == (long long)9223372036854775808ull) {
      append ("-9223372036854775808", 20);
      return *this;
    }
    x = -x;
    *buffer_end++ = '-';
  }

  char *left = buffer_end;
  do {
    *buffer_end++ = (char)(x % 10 + '0');
    x /= 10;
  } while (x > 0);

  char *right = buffer_end - 1;
  while (left < right) {
    char t = *left;
    *left++ = *right;
    *right-- = t;
  }

  return *this;
}

string_buffer &string_buffer::operator + (unsigned long long x) {
  reserve_at_least (20);

  char *left = buffer_end;
  do {
    *buffer_end++ = (char)(x % 10 + '0');
    x /= 10;
  } while (x > 0);

  char *right = buffer_end - 1;
  while (left < right) {
    char t = *left;
    *left++ = *right;
    *right-- = t;
  }

  return *this;
}

string_buffer &string_buffer::operator + (const var &v) {
  switch (v.type) {
    case var::NULL_TYPE:
      return *this;
    case var::BOOLEAN_TYPE:
      return (v.b ? *this + '1' : *this);
    case var::INTEGER_TYPE:
      return *this + v.i;
    case var::FLOAT_TYPE:
      return *this + string (v.f);
    case var::STRING_TYPE:
      return *this + *STRING(v.s);
    case var::ARRAY_TYPE:
      php_warning ("Convertion from array to string");
      return append ("Array", 5);
    case var::OBJECT_TYPE:
      return *this + OBJECT(v.o)->to_string();
    default:
      php_assert (0);
      exit (1);
  }
}

dl::size_type string_buffer::size (void) const {
  return (dl::size_type)(buffer_end - buffer_begin);
}

char *string_buffer::buffer (void) {
  return buffer_begin;
}

const char *string_buffer::buffer (void) const {
  return buffer_begin;
}

const char *string_buffer::c_str (void) {
  reserve_at_least (1);
  *buffer_end = 0;

  return buffer_begin;
}

string string_buffer::str (void) const {
  php_assert (size() <= buffer_len);
  return string (buffer_begin, size());
}

bool string_buffer::set_pos (int pos) {
  php_assert ((dl::size_type)pos <= buffer_len);
  buffer_end = buffer_begin + pos;
  return true;
}

string_buffer::~string_buffer (void) {
  dl::static_deallocate ((void **)&buffer_begin, &buffer_len);
}

string_buffer &string_buffer::append (const char *str, int len) {
  reserve_at_least (len);

  memcpy (buffer_end, str, len);
  buffer_end += len;

  return *this;
}

void string_buffer::append_unsafe (const char *str, int len) {
  memcpy (buffer_end, str, len);
  buffer_end += len;
}

void string_buffer::append_char (char c) {
  *buffer_end++ = c;
}


void string_buffer::reserve (int len) {
  reserve_at_least (len + 1);
}

