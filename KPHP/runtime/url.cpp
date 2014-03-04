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

#include "url.h"

#include "common/base64.h"

#include "array_functions.h"
#include "regexp.h"

string f$base64_decode (const string &s) {
  int result_len = s.size() / 4 * 3;
  string res (result_len, false);
  result_len = base64_decode (s.c_str(), reinterpret_cast <unsigned char *> (res.buffer()), result_len + 1);

  if (result_len < 0) {
    return string();
  }

  res.shrink (result_len);
  return res;
}

string f$base64_encode (const string &s) {
  int result_len = (s.size() + 2) / 3 * 4 ;
  string res (result_len, false);
  result_len = base64_encode (reinterpret_cast <const unsigned char *> (s.c_str()), (int)s.size(), res.buffer(), result_len + 1);

  if (result_len != 0) {
    return string();
  }

  return res;
}

static void parse_str_set_array_value (var &arr, const string &key, const string &value) {
  const char *left_br_pos = key.c_str();
  php_assert (*left_br_pos == '[');
  const char *right_br_pos = (const char *)memchr (left_br_pos, ']', key.size());
  if (right_br_pos != NULL) {
    string next_key (left_br_pos + 1, (dl::size_type)(right_br_pos - left_br_pos - 1));
    if (!arr.is_array()) {
      arr = array <var> ();
    }

    if (next_key.empty()) {
      next_key = string (arr.to_array().get_next_key());
    }

    if (right_br_pos[1] == '[') {
      parse_str_set_array_value (arr[next_key], string (right_br_pos + 1, (dl::size_type)(key.c_str() + key.size() - (right_br_pos + 1))), value);
    } else {
      arr.set_value (next_key, value);
    }
    return;
  }

  arr = value;
}

void parse_str_set_value (var &arr, const string &key, const string &value) {
  const char *key_c = key.c_str();
  const char *left_br_pos = (const char *)memchr (key_c, '[', key.size());
  if (left_br_pos != NULL) {
    return parse_str_set_array_value (arr[string (key_c, (dl::size_type)(left_br_pos - key_c))], string (left_br_pos, (dl::size_type)(key_c + key.size() - left_br_pos)), value);
  }

  arr.set_value (key, value);
}

void f$parse_str (const string &str, var &arr) {
  if (!arr.is_array()) {
    arr = array <var> ();
  }

  array <string> par = explode ('&', str, INT_MAX);
  int len = par.count();
  for (int i = 0; i < len; i++) {
    const char *cur = par.get_value (i).c_str();
    const char *eq_pos = strchrnul (cur, '=');
    string value;
    if (*eq_pos) {
      value = f$urldecode (string (eq_pos + 1, (dl::size_type)(strlen (eq_pos + 1))));
    }

    string key (cur, (dl::size_type)(eq_pos - cur));
    key = f$urldecode (key);
    parse_str_set_value (arr, key, value);
  }
}

var f$parse_url (const string &s, int component) {
  var result;
  static const char *regexp = "~^(?:([^:/?#]+):)?(?:///?(?:(?:(?:([^:@?#/]+)(?::([^@?#/]+))?)@)?([^/?#:]+)(?::([0-9]+))?))?([^?#]*)(?:\\?([^#]*))?(?:#(.*))?$~";//I'm too lazy
  f$preg_match (string (regexp, (dl::size_type)(strlen (regexp))), s, result);
  if (!result.is_array()) {
    return false;
  }

  swap (result[2], result[4]);
  swap (result[3], result[5]);
  result[3].convert_to_int();

  if ((unsigned int)component < 8u) {
    if (result[component + 1].empty()) {
      return var();
    }
    return result.get_value (component + 1);
  }
  if (component != -1) {
    php_warning ("Wrong parameter component = %d in function parse_str", component);
    return false;
  }

  array <var> res;
  if (!result[1].empty()) {
    res.set_value (string ("scheme", 6), result[1]);
  }
  if (!result[2].empty()) {
    res.set_value (string ("host", 4), result[2]);
  }
  if (!result[3].empty()) {
    res.set_value (string ("port", 4), result[3]);
  }
  if (!result[4].empty()) {
    res.set_value (string ("user", 4), result[4]);
  }
  if (!result[5].to_string().empty()) {
    res.set_value (string ("pass", 4), result[5]);
  }
  if (!result[6].to_string().empty()) {
    res.set_value (string ("path", 4), result[6]);
  }
  if (!result[7].to_string().empty()) {
    res.set_value (string ("query", 5), result[7]);
  }
  if (!result[8].to_string().empty()) {
    res.set_value (string ("fragment", 8), result[8]);
  }

  return res;
}

string f$rawurldecode (const string &s) {
  static_SB.clean().reserve (s.size());
  for (int i = 0; i < (int)s.size(); i++) {
    if (s[i] == '%') {
      int num_high = hex_to_int (s[i + 1]);
      if (num_high < 16) {
        int num_low = hex_to_int (s[i + 2]);
        if (num_low < 16) {
          static_SB.append_char ((num_high << 4) + num_low);
          i += 2;
          continue;
        }
      }
    }
    static_SB.append_char (s[i]);
  }
  return static_SB.str();
}


static const char *good_url_symbols =
    "00000000000000000000000000000000"
    "00000000000001101111111111000000"
    "01111111111111111111111111100001"
    "01111111111111111111111111100000"
    "00000000000000000000000000000000"
    "00000000000000000000000000000000"
    "00000000000000000000000000000000"
    "00000000000000000000000000000000";//[0-9a-zA-Z-_.]

string f$rawurlencode (const string &s) {
  static_SB.clean().reserve (3 * s.size());
  for (int i = 0; i < (int)s.size(); i++) {
    if (good_url_symbols[(unsigned char)s[i]] == '1') {
      static_SB.append_char (s[i]);
    } else {
      static_SB.append_char ('%');
      static_SB.append_char (uhex_digits[(s[i] >> 4) & 15]);
      static_SB.append_char (uhex_digits[s[i] & 15]);
    }
  }
  return static_SB.str();
}

string f$urldecode (const string &s) {
  static_SB.clean().reserve (s.size());
  for (int i = 0; i < (int)s.size(); i++) {
    if (s[i] == '%') {
      int num_high = hex_to_int (s[i + 1]);
      if (num_high < 16) {
        int num_low = hex_to_int (s[i + 2]);
        if (num_low < 16) {
          static_SB.append_char ((num_high << 4) + num_low);
          i += 2;
          continue;
        }
      }
    } else if (s[i] == '+') {
      static_SB.append_char (' ');
      continue;
    }
    static_SB.append_char (s[i]);
  }
  return static_SB.str();
}

string f$urlencode (const string &s) {
  static_SB.clean().reserve (3 * s.size());
  for (int i = 0; i < (int)s.size(); i++) {
    if (good_url_symbols[(unsigned char)s[i]] == '1') {
      static_SB.append_char (s[i]);
    } else if (s[i] == ' ') {
      static_SB.append_char ('+');
    } else {
      static_SB.append_char ('%');
      static_SB.append_char (uhex_digits[(s[i] >> 4) & 15]);
      static_SB.append_char (uhex_digits[s[i] & 15]);
    }
  }
  return static_SB.str();
}
