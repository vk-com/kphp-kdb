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
#include "../common.h"
#include "bicycle.h"
static inline int is_alpha (int c);
static inline int is_alphanum (int c);
static inline int is_digit (int c);
static inline int conv_oct_digit (int c);
static inline int conv_hex_digit (int c);

template <class T> void clear (T &val);
template <class T> void save_to_ptr (T *ptr, const T &data);

template <class ArrayType, class IndexType>
IndexType dsu_get (const ArrayType &arr, IndexType i);

template <class ArrayType, class IndexType>
void dsu_uni (ArrayType *arr, IndexType i, IndexType j);

template <class T> class Enumerator {
public:
  vector <T> vars;
  int size (void) {
    return vars.size();
  }
  Enumerator() {
  }
  int next_id(const T &new_data) {
    vars.push_back (new_data);
    return (int)vars.size();
  }
  T &operator[] (int id) {
    assert (0 < id && id <= (int)vars.size());
    return vars[id - 1];
  }
private:
  DISALLOW_COPY_AND_ASSIGN (Enumerator);
};

template <typename KeyT, typename EntryT> class MapToId {
public:
  explicit MapToId (Enumerator <EntryT> *cur_id);

  int get_id (const KeyT &name);
  int add_name (const KeyT &name, const EntryT &add);
  EntryT &operator[] (int id);

  set <int> get_ids();

private:
  map <KeyT, int> name_to_id;
  Enumerator <EntryT> *items;
  DISALLOW_COPY_AND_ASSIGN (MapToId);
};

class string_ref {
private:
  const char *s, *t;
public:
  string_ref();
  string_ref (const char *s, const char *t);
  int length() const;
  const char *begin() const;
  const char *end() const;
  bool eq (const char *q);
  string str() const;
  const char *c_str() const;
  inline operator string(){
    return string (s, t);
  }
};
inline string_ref::string_ref()
  :s (NULL), t (NULL) {
}
inline string_ref::string_ref (const char *s, const char *t)
  : s (s), t (t) {
}
inline int string_ref::length() const {
  return (int)(t - s);
}
inline const char *string_ref::begin() const {
  return s;
}
inline const char *string_ref::end() const {
  return t;
}
inline bool string_ref::eq (const char *q) {
  int qn = (int)strlen (q);
  return qn == length() && !strncmp (q, begin(), qn);
}
inline string_ref string_ref_dup (const string &s) {
  char *buf = new char[s.length()];
  memcpy (buf, &s[0], s.size());
  return string_ref (buf, buf + s.length());
}
inline string string_ref::str() const {
  return string (begin(), end());
}
inline const char *string_ref::c_str() const {
  return str().c_str();
}

inline string int_to_str (int x) {
  char tmp[50];
  sprintf (tmp, "%d", x);
  return tmp;
}

#define FOREACH(v, i_) for (__typeof (all (v)) i_ = all(v); !i_.empty(); i_.next())
template <class T> void my_unique (T *v);


#include "utils.hpp"
#include "graph.h"
