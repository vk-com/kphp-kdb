/*
    This file is part of VK/KittenDB-Engine.

    VK/KittenDB-Engine is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenDB-Engine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with VK/KittenDB-Engine.  If not, see <http://www.gnu.org/licenses/>.

    This program is released under the GPL with the additional exemption 
    that compiling, linking, and/or using OpenSSL is allowed.
    You are free to remove this exemption from derived works.

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#pragma once

#include "kphp_core.h"

extern const string COLON;
extern const string CP1251;
extern const string DOT;
extern const string NEW_LINE;
extern const string SPACE;
extern const string WHAT;

const int PHP_BUF_LEN = (1 << 23);//TODO remove usages of static buffer
extern char php_buf[PHP_BUF_LEN + 1];

extern const char lhex_digits[17];
extern const char uhex_digits[17];

extern int str_replace_count_dummy;

inline int hex_to_int (char c);


string f$addcslashes (const string &str, const string &what);

string f$addslashes (const string &str);

string f$bin2hex (const string &str);

string f$chop (const string &s, const string &what = WHAT);

string f$chr (int v);

var f$count_chars (const string &str, int mode = 0);

string f$hex2bin (const string &str);

const int ENT_HTML401 = 0;
const int ENT_COMPAT = 0;
const int ENT_QUOTES = 1;
const int ENT_NOQUOTES = 2;

string f$htmlentities (const string &str);

string f$html_entity_decode (const string &str, int flags = ENT_COMPAT | ENT_HTML401, const string &encoding = CP1251);

string f$htmlspecialchars (const string &str, int flags = ENT_COMPAT | ENT_HTML401);

string f$htmlspecialchars_decode (const string &str, int flags = ENT_COMPAT | ENT_HTML401);

string f$lcfirst (const string &str);

string f$lcwords (const string &str);

int f$levenshtein (const string &str1, const string &str2);

string f$ltrim (const string &s, const string &what = WHAT);

string f$mysql_escape_string (const string &str);

string f$nl2br (const string &str, bool is_xhtml = true);

inline string f$number_format (double number, int decimals = 0);

inline string f$number_format (double number, int decimals, const string &dec_point);

inline string f$number_format (double number, int decimals, const var &dec_point);

string f$number_format (double number, int decimals, const string &dec_point, const string &thousands_sep);

inline string f$number_format (double number, int decimals, const string &dec_point, const var &thousands_sep);

inline string f$number_format (double number, int decimals, const var &dec_point, const string &thousands_sep);

inline string f$number_format (double number, int decimals, const var &dec_point, const var &thousands_sep);

int f$ord (const string &s);

string f$pack (const array <var> &a);

int f$printf (const array <var> &a);

string f$rtrim (const string &s, const string &what = WHAT);

OrFalse <string> f$setlocale (int category, const string &locale);

string f$sprintf (const array <var> &a);

string f$stripslashes (const string &str);

int f$strcasecmp (const string &lhs, const string &rhs);

int f$strcmp (const string &lhs, const string &rhs);

OrFalse <int> f$stripos (const string &haystack, const string &needle, int offset = 0);

inline OrFalse <int> f$stripos (const string &haystack, const var &needle, int offset = 0);

OrFalse <string> f$stristr (const string &haystack, const string &needle, bool before_needle = false);

inline OrFalse <string> f$stristr (const string &haystack, const var &needle, bool before_needle = false);

inline int f$strlen (const string &s);

int f$strncmp (const string &lhs, const string &rhs, int len);

OrFalse <string> f$strpbrk (const string &haystack, const string &char_list);

OrFalse <int> f$strpos (const string &haystack, const string &needle, int offset = 0);

inline OrFalse <int> f$strpos (const string &haystack, const var &needle, int offset = 0);

OrFalse <int> f$strrpos (const string &haystack, const string &needle, int offset = 0);

inline OrFalse <int> f$strrpos (const string &haystack, const var &needle, int offset = 0);

OrFalse <int> f$strripos (const string &haystack, const string &needle, int offset = 0);

inline OrFalse <int> f$strripos (const string &haystack, const var &needle, int offset = 0);

string f$strrev (const string &str);

OrFalse <string> f$strstr (const string &haystack, const string &needle, bool before_needle = false);

inline OrFalse <string> f$strstr (const string &haystack, const var &needle, bool before_needle = false);

string f$strtolower (const string &str);

string f$strtoupper (const string &str);

string f$strtr (const string &subject, const string &from, const string &to);

template <class T>
string f$strtr (const string &subject, const array <T> &replace_pairs);

inline string f$strtr (const string &subject, const var &from, const var &to);

inline string f$strtr (const string &subject, const var &replace_pairs);

const int STR_PAD_LEFT = 0;
const int STR_PAD_RIGHT = 1;
const int STR_PAD_BOTH = 2;

string f$str_pad (const string &input, int len, const string &pad_str = SPACE, int pad_type = STR_PAD_RIGHT);

string f$str_repeat (const string &s, int multiplier);

string f$str_replace (const string &search, const string &replace, const string &subject, int &replace_count = str_replace_count_dummy);

var f$str_replace (const var &search, const var &replace, const var &subject, int &replace_count = str_replace_count_dummy);

array <string> f$str_split (const string &str, int split_length = 1);

string f$substr (const string &str, int start, int length = INT_MAX);

int f$substr_count (const string &haystack, const string &needle, int offset = 0, int length = INT_MAX);

string f$trim (const string &s, const string &what = WHAT);

string f$ucfirst (const string &str);

string f$ucwords (const string &str);

array <var> f$unpack (const string &pattern, const string &data);

string f$wordwrap (const string &str, int width = 75, string brk = NEW_LINE, bool cut = false);

/*
 *
 *     IMPLEMENTATION
 *
 */


int hex_to_int (char c) {
  if ('0' <= c && c <= '9') {
    return c - '0';
  }
  c |= 0x20;
  if ('a' <= c && c <= 'f') {
    return c - 'a' + 10;
  }
  return 16;
}

string f$number_format (double number, int decimals) {
  return f$number_format (number, decimals, DOT, COLON);
}

string f$number_format (double number, int decimals, const string &dec_point) {
  return f$number_format (number, decimals, dec_point, COLON);
}

string f$number_format (double number, int decimals, const var &dec_point) {
  return f$number_format (number, decimals, dec_point.is_null() ? DOT : dec_point.to_string(), COLON);
}

string f$number_format (double number, int decimals, const string &dec_point, const var &thousands_sep) {
  return f$number_format (number, decimals, dec_point, thousands_sep.is_null() ? COLON : thousands_sep.to_string());
}

string f$number_format (double number, int decimals, const var &dec_point, const string &thousands_sep) {
  return f$number_format (number, decimals, dec_point.is_null() ? DOT : dec_point.to_string(), thousands_sep);
}

string f$number_format (double number, int decimals, const var &dec_point, const var &thousands_sep) {
  return f$number_format (number, decimals, dec_point.is_null() ? DOT : dec_point.to_string(), thousands_sep.is_null() ? COLON : thousands_sep.to_string());
}

int f$strlen (const string &s) {
  return (int)s.size();
}

OrFalse <int> f$stripos (const string &haystack, const var &needle, int offset) {
  if (needle.is_string()) {
    return f$stripos (haystack, needle.to_string(), offset);
  } else {
    return f$stripos (haystack, string (1, (char)needle.to_int()), offset);
  }
}

OrFalse <string> f$stristr (const string &haystack, const var &needle, bool before_needle) {
  if (needle.is_string()) {
    return f$stristr (haystack, needle.to_string(), before_needle);
  } else {
    return f$stristr (haystack, string (1, (char)needle.to_int()), before_needle);
  }
}

OrFalse <int> f$strpos (const string &haystack, const var &needle, int offset) {
  if (needle.is_string()) {
    return f$strpos (haystack, needle.to_string(), offset);
  } else {
    return f$strpos (haystack, string (1, (char)needle.to_int()), offset);
  }
}

OrFalse <int> f$strrpos (const string &haystack, const var &needle, int offset) {
  if (needle.is_string()) {
    return f$strrpos (haystack, needle.to_string(), offset);
  } else {
    return f$strrpos (haystack, string (1, (char)needle.to_int()), offset);
  }
}

OrFalse <int> f$strripos (const string &haystack, const var &needle, int offset) {
  if (needle.is_string()) {
    return f$strripos (haystack, needle.to_string(), offset);
  } else {
    return f$strripos (haystack, string (1, (char)needle.to_int()), offset);
  }
}

OrFalse <string> f$strstr (const string &haystack, const var &needle, bool before_needle) {
  if (needle.is_string()) {
    return f$strstr (haystack, needle.to_string(), before_needle);
  } else {
    return f$strstr (haystack, string (1, (char)needle.to_int()), before_needle);
  }
}

template <class T>
string f$strtr (const string &subject, const array <T> &replace_pairs) {
  const char *piece = subject.c_str(), *piece_end = subject.c_str() + subject.size();
  string result;
  while (1) {
    const char *best_pos = NULL;
    int best_len = -1;
    string replace;
    for (typename array <T>::const_iterator p = replace_pairs.begin(); p != replace_pairs.end(); ++p) {
      const string search = f$strval (p.get_key());
      int search_len = search.size();
      if (search_len == 0) {
        return subject;
      }
      const char *pos = static_cast <const char *> (memmem (static_cast <const void *> (piece), (size_t)(piece_end - piece), static_cast <const void *> (search.c_str()), (size_t)search_len));
      if (pos != NULL && (best_pos == NULL || best_pos > pos || (best_pos == pos && search_len > best_len))) {
        best_pos = pos;
        best_len = search_len;
        replace = f$strval (p.get_value());
      }
    }
    if (best_pos == NULL) {
      result.append (piece, (dl::size_type)(piece_end - piece));
      break;
    }

    result.append (piece, (dl::size_type)(best_pos - piece));
    result.append (replace);

    piece = best_pos + best_len;
  }

  return result;
}

inline string f$strtr (const string &subject, const var &from, const var &to) {
  return f$strtr (subject, from.to_string(), to.to_string());
}

inline string f$strtr (const string &subject, const var &replace_pairs) {
  return f$strtr (subject, replace_pairs.as_array ("strtr", 2));
}

