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

#include "utils.h"

/*** Id ***/
template <class IdData>
Id<IdData>::Id () : ptr (NULL) {}
template <class IdData>
Id<IdData>::Id (IdData * ptr) : ptr (ptr) {}
template <class IdData>
Id<IdData>::Id (const Id <IdData> &id) : ptr (id.ptr) {}
template <class IdData>
Id<IdData> &Id<IdData>::operator = (const Id <IdData> &id) {
  ptr = id.ptr;
  return *this;
}
template <class IdData>
bool Id<IdData>::is_null() const {
  return ptr == NULL;
}
template <class IdData>
bool Id<IdData>::not_null() const {
  return ptr != NULL;
}
template <class IdData>
IdData &Id<IdData>::operator *() const {
  assert (ptr != NULL);
  return *ptr;
}
template <class IdData>
IdData *Id<IdData>::operator ->() const {
  assert (ptr != NULL);
  return ptr;
}

template <class IdData>
inline string &Id<IdData>::str() {
  return ptr->str_val;
}

/*
template <class IdData>
bool Id<IdData>::operator < (const Id <IdData> &b) {
  return get_index (*this) < get_index (b);
}
template <class IdData>
bool Id<IdData>::operator == (const Id <IdData> &b) {
  return get_index (*this) < get_index (b);
} */


template <class IdData>
bool operator < (const Id <IdData> &a, const Id <IdData> &b) {
  return get_index (a) < get_index (b);
}
template <class IdData>
bool operator == (const Id <IdData> &a, const Id <IdData> &b) {
  bool fa = a.is_null();
  bool fb = b.is_null();
  if (fa || fb) {
    return fa && fb;
  }
  return get_index (a) == get_index (b);
}

template <class IdData>
void Id<IdData>::clear() {
  delete ptr; //TODO: be very-very carefull with it
  ptr = NULL;
}
template <class IdData>
template <class IndexType>
typename DataTraits <IdData>::value_type Id<IdData>::operator[] (const IndexType &i) const {
  return (*ptr)[i];
}

/*** [get|set]_index ***/
template <class T>
int get_index (const Id <T> &i) {
  assert (i.ptr != NULL);
  return i.ptr->id;
}

template <class T>
void set_index (Id <T> *d, int index) {
  d->ptr->id = index;
}

/*** IdMap ***/
template <class DataType> IdMap<DataType>::IdMap() : def_val() {
}
template <class DataType> IdMap<DataType>::IdMap (int size, DataType val)
  : def_val (val), data (size, def_val) {
}

template <class DataType>
template <class IndexType>
DataType &IdMap <DataType>::operator[] (const IndexType &i) {
  int index = get_index (i);
  assert (index >= 0);
  dl_assert (index < (int)data.size(), dl_pstr ("%d of %d\n", index, (int)data.size()));
  return data[index];
}

template <class DataType>
template <class IndexType>
const DataType &IdMap <DataType>::operator[] (const IndexType &i) const {
  int index = get_index (i);
  assert (index >= 0);
  dl_assert (index < (int)data.size(), dl_pstr ("%d of %d\n", index, (int)data.size()));
  return data[index];
}

template <class DataType>
typename IdMap<DataType>::iterator IdMap<DataType>::begin() {
  return data.begin();
}
template <class DataType>
typename IdMap<DataType>::iterator IdMap<DataType>::end() {
  return data.end();
}
template <class DataType>
void IdMap<DataType>::clear() {
  data.clear();
}

template <class DataType> void IdMap<DataType>::renumerate_ids (const vector <int> &new_ids) {
  int max_id = *std::max_element (new_ids.begin(), new_ids.end());
  vector <DataType> new_data (max_id + 1);

  for (int i = 0; i < (int)new_ids.size(); i++) {
    int new_id = new_ids[i];
    if (new_id >= 0) {
      std::swap (new_data[new_id], data[i]);
      //set_index (&new_data[new_id], new_id);
    }
  }

  std::swap (data, new_data);
}

template <class DataType> void IdMap <DataType>::update_size (int n) {
  assert ((int)data.size() <= n);
  data.resize (n, def_val);
}

/*** IdGen ***/
template <class IdType>
IdGen<IdType>::IdGen() : n(0) {
}
template <class IdType>
void IdGen<IdType>::add_id_map (IdMapBase *to_add) {
  to_add->update_size ((n | 0xff) + 1);
  id_maps.push_back (to_add);
}
template <class IdType>
void IdGen<IdType>::remove_id_map (IdMapBase *to_remove) {
  id_maps.erase (find (id_maps.begin(), id_maps.end(), to_remove));
}
template <class IdType>
int IdGen<IdType>::init_id (IdType *to_add) {
  set_index (to_add, n);

  if ((n & 0xff) == 0) {
    int real_n = (n | 0xff) + 1;
    ids.update_size (real_n);

    for (int i = 0; i < (int)id_maps.size(); i++) {
      id_maps[i]->update_size (real_n);
    }
  }

  ids[*to_add] = *to_add;
  n++;
  return n - 1;
}
template <class IdType>
int IdGen<IdType>::size() {
  return n;
}
template <class IdType>
typename IdGen <IdType>::iterator IdGen<IdType>::begin() {
  return ids.begin();
}
template <class IdType>
typename IdGen <IdType>::iterator IdGen<IdType>::end() {
  return ids.begin() + size();
}
template <class IdType>
void IdGen<IdType>::clear() {
  n = 0;
  std::for_each (ids.begin(), ids.end(), ::clear <IdType>);
  ::clear (ids);
  for (int i = 0; i < (int)id_maps.size(); i++) {
    ::clear (*id_maps[i]);
  }
  ::clear (id_maps);
}
template <class IdType>
template <class IndexType>
void IdGen<IdType>::delete_ids (const vector <IndexType> &to_del) {
  vector <int> new_ids (size(), 0);

  for (int i = 0; i < (int)to_del.size(); i++) {
    new_ids[get_index(to_del[i])] = -1;
    clear (ids[to_del[i]]);
  }

  for (int i = 0, cur = 0; i < size(); i++) {
    if (new_ids[i] == 0) {
      new_ids[i] = cur++;
    }
  }

  ids.renumerate_ids (new_ids);
  for (int i = 0; i < (int)id_maps.size(); i++) {
    id_maps[i]->renumerate_ids (new_ids);
  }

  for (int i = 0; i < size(); i++) {
    set_index (ids[i], i);
  }
}

/*** XNameToId ***/
template <typename T> XNameToId<T>::XNameToId (IdGen <T> *new_id_gen) {
  init (new_id_gen);
}
template <typename T> void XNameToId<T>::clear() {
  id_gen = NULL;
  name_to_id.clear();
}
template <typename T> void XNameToId<T>::init (IdGen <T> *new_id_gen) {
  clear();
  id_gen = new_id_gen;
}

template <typename T> T XNameToId<T>::get_id (const string &name) {
  __typeof (name_to_id.begin()) res = name_to_id.find(name);
  if (res == name_to_id.end()) {
    return T();
  }
  return res->second;
}

template <typename T> bool XNameToId<T>::add_name (const string &name, T* new_id) {
  T *id = &name_to_id[name];
  if (id->is_null()) {
    id_gen->init_id (new_id);
    *id = *new_id;
  } else {
    return false;
  }
  return true;
}

template <typename T> T *XNameToId<T>::operator[] (int id) {
  return id_gen[id];
}

template <typename T> set <T> XNameToId <T>::get_ids() {
  set <T> res;
  for (__typeof (name_to_id.begin()) i = name_to_id.begin(); i != name_to_id.end(); i++) {
    res.insert (i->second);
  }
  return res;
}


/*** Range ***/
template <class Iterator>
bool Range<Iterator>::empty() {
  return begin == end;
}
template <class Iterator>
size_t Range<Iterator>::size() {
  return end - begin;
}
template <class Iterator>
typename Range<Iterator>::value_type &Range<Iterator>::operator[] (size_t i) {
  return begin[i];
}
template <class Iterator>
typename Range<Iterator>::reference_type Range<Iterator>::operator * () {
  typedef typename std::iterator_traits <Iterator>::value_type value_type;
  return *begin;
}
template <class Iterator>
typename Range<Iterator>::value_type *Range<Iterator>::operator -> () {
  return &*begin;
}
template <class Iterator>
void Range<Iterator>::next() {
  begin++;
}
template <class Iterator>
Range<Iterator>::Range() {
}
template <class Iterator>
Range<Iterator>::Range (Iterator begin, Iterator end)
  : begin (begin), end (end) {
}

template <class A> Range<typename A::iterator> all (A &v) {
  return Range <typename A::iterator> (v.begin(), v.end());
}
template <class A> Range<typename A::const_iterator> all (const A &v) {
  return Range <typename A::const_iterator> (v.begin(), v.end());
}
template <class A> Range<typename A::reverse_iterator> reversed_all (A &v) {
  return Range <typename A::reverse_iterator> (v.rbegin(), v.rend());
}
template <class A> Range<typename A::const_reverse_iterator> reversed_all (const A &v) {
  return Range <typename A::const_reverse_iterator> (v.rbegin(), v.rend());
}
template <class T> Range <T> all (const Range <T> &range) {
  return range;
}

