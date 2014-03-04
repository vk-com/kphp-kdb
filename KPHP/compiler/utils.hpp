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

static inline int is_alpha (int c) {
  return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || (c == '_');
}

static inline int is_alphanum (int c) {
  return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || (c == '_') || ('0' <= c && c <= '9');
}

static inline int is_digit (int c) {
  return '0' <= c && c <= '9';
}

static inline int conv_oct_digit (int c) {
  if ('0' <= c && c <= '7') {
    return c - '0';
  }
  return -1;
}

static inline int conv_hex_digit (int c) {
  if ('0' <= c && c <= '9') {
    return c - '0';
  }
  c |= 0x20;
  if ('a' <= c && c <= 'f') {
    return c - 'a' + 10;
  }
  return -1;
}

template <class T>
void clear (T &val) {
  val.clear();
}

template <class T> void save_to_ptr (T *ptr, const T &data) {
  if (ptr != NULL) {
    *ptr = data;
  }
}

template <class ArrayType, class IndexType>
IndexType dsu_get (ArrayType *arr, IndexType i) {
  if ((*arr)[i] == i) {
    return i;
  }
  return (*arr)[i] = dsu_get (arr, (*arr)[i]);
}
template <class ArrayType, class IndexType>
void dsu_uni (ArrayType *arr, IndexType i, IndexType j) {
  i = dsu_get (arr, i);
  j = dsu_get (arr, j);
  if (!(i == j)) {
    if (rand() & 1) {
      (*arr)[i] = j;
    } else {
      (*arr)[j] = i;
    }
  }
}


template <typename KeyT, typename EntryT> MapToId<KeyT, EntryT>::MapToId (Enumerator <EntryT> *items)
  : items (items) {
}

template <typename KeyT, typename EntryT> int MapToId<KeyT, EntryT>::get_id (const KeyT &name) {
  __typeof (name_to_id.begin()) res = name_to_id.find(name);
  if (res == name_to_id.end()) {
    return 0;
  }
  return res->second;
}

template <typename KeyT, typename EntryT> int MapToId<KeyT, EntryT>::add_name (const KeyT &name, const EntryT &add) {
  int &id = name_to_id[name];
  if (id == 0) {
    id = items->next_id (add);
  }
  return id;
}

template <typename KeyT, typename EntryT> EntryT &MapToId<KeyT, EntryT>::operator[] (int id) {
  return (*items)[id];
}

template <typename KeyT, typename EntryT> set <int> MapToId <KeyT, EntryT>::get_ids() {
  set <int> res;
  for (__typeof (name_to_id.begin()) i = name_to_id.begin(); i != name_to_id.end(); i++) {
    res.insert (i->second);
  }
  return res;
}

template <class T> void my_unique (T *v) {
  sort (v->begin(), v->end());
  v->erase (std::unique (v->begin(), v->end()), v->end());
}
