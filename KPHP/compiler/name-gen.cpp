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

#include "name-gen.h"
#include "stage.h"
#include "io.h"
#include "data.h"

string register_unique_name (const string &prefix) {
  //static set <string> v;
  //static volatile int x = 0;
  //AutoLocker <volatile int *> locker (&x);
  //if (v.count (prefix)) {
    //fprintf (stderr, "%s\n", prefix.c_str());
    //assert (0);
  //}
  //v.insert (prefix);
  return prefix;
}

string gen_const_string_name (const string &str) {
  AUTO_PROF (next_const_string_name);
  unsigned long long h = hash_ll (str);
  char tmp[50];
  sprintf (tmp, "const_string$us%llx", h);
  return tmp;
}

string gen_const_regexp_name (const string &str) {
  AUTO_PROF (next_const_string_name);
  unsigned long long h = hash_ll (str);
  char tmp[50];
  sprintf (tmp, "const_regexp$us%llx", h);
  return tmp;
}

string gen_unique_name (string prefix, bool flag) {
  for (int i = 0; i < (int)prefix.size(); i++) {
    int c = prefix[i];
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
    } else {
      prefix[i] = '_';
    }
  }
  prefix += "$u";
  AUTO_PROF (next_name);
  FunctionPtr function = stage::get_function();
  if (function.is_null() || flag) {
    return register_unique_name (prefix);
  }
  map <long long, int> *name_gen_map = &function->name_gen_map;
  int h = hash (function->name);
  long long ph = hash_ll (prefix);
  int *i = &(*name_gen_map)[ph];
  int cur_i = (*i)++;
  char tmp[50];
  sprintf (tmp, "%x_%d", h, cur_i);
  return register_unique_name (prefix + tmp);
}

