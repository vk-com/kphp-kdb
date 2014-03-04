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

string f$base64_decode (const string &s);

string f$base64_encode (const string &s);

template <class T>
string f$http_build_query (const array <T> &a);

void parse_str_set_value (var &arr, const string &key, const string &value);

void f$parse_str (const string &str, var &arr);

var f$parse_url (const string &s, int component = -1);

string f$rawurldecode (const string &s);

string f$rawurlencode (const string &s);

string f$urldecode (const string &s);

string f$urlencode (const string &s);

/*
 *
 *     IMPLEMENTATION
 *
 */

template <class T>
string http_build_query_get_param (const string &key, const T &a) {
  if (f$is_array (a)) {
    string result;
    int first = 1;
    for (typename array <T>::const_iterator p = a.begin(); p != a.end(); ++p) {
      if (!first) {
        result.push_back ('&');
      }
      first = 0;
      result.append (http_build_query_get_param ((static_SB.clean() + key + '[' + p.get_key() + ']').str(), p.get_value()));
    }
    return result;
  } else {
    return (static_SB.clean() + f$urlencode (key) + '=' + f$urlencode (f$strval (a))).str();
  }
}


template <class T>
string f$http_build_query (const array <T> &a) {
  string result;
  int first = 1;
  for (typename array <T>::const_iterator p = a.begin(); p != a.end(); ++p) {
    if (!first) {
      result.push_back ('&');
    }
    first = 0;
    result.append (http_build_query_get_param (f$strval (p.get_key()), p.get_value()));
  }

  return result;
}
