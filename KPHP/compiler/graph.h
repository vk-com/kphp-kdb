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

/**Data Traits**/
struct UnexistingType;
template <class T>
struct DataTraits {
  typedef UnexistingType value_type;
};

/*** Id ***/
template <class IdData>
struct Id {
  IdData *ptr;
  Id();
  explicit Id (IdData * ptr);
  Id (const Id &id);
  Id &operator = (const Id &id);
  IdData &operator *() const;
  bool is_null() const;
  bool not_null() const;
  void clear();

  IdData *operator ->() const;

  //bool operator < (const Id <IdData>&);
  //bool operator == (const Id <IdData>&);

  template <class IndexType>
  typename DataTraits<IdData>::value_type operator[] (const IndexType &i) const;
  string &str();
};

template <class T>
bool operator < (const Id <T> &a, const Id <T> &b);
template <class T>
bool operator == (const Id <T> &a, const Id <T> &b);

/*** [get|set]_index ***/
template <class T>
int get_index (const T &i);

template <class T>
void set_index (T &d, int index);

/*** IdMapBase ***/
struct IdMapBase {
  virtual void renumerate_ids (const vector <int> &new_ids) = 0;
  virtual void update_size (int new_max_id) = 0;
  virtual void clear() = 0;
  virtual ~IdMapBase(){}
};

/*** IdMap ***/
template <class DataType>
struct IdMap : public IdMapBase {
  DataType def_val;
  vector <DataType> data;

  typedef typename vector <DataType>::iterator iterator;
  IdMap();
  IdMap (int size, DataType val = DataType());
  template <class IndexType>
  DataType &operator[] (const IndexType &i);
  template <class IndexType>
  const DataType &operator[] (const IndexType &i) const;

  iterator begin();
  iterator end();
  void clear();

  void renumerate_ids (const vector <int> &new_ids);

  void update_size (int n);
};

/*** IdGen ***/
template <class IdType>
struct IdGen {
  typedef typename IdMap <IdType>::iterator iterator;

  vector <IdMapBase *> id_maps;
  IdMap <IdType> ids;
  int n;

  IdGen();
  void add_id_map (IdMapBase *to_add);
  void remove_id_map (IdMapBase *to_remove);

  int init_id (IdType *id);
  int size();
  iterator begin();
  iterator end();
  void clear();

  template <class IndexType>
  void delete_ids (const vector <IndexType> &to_del);
};

/*** XNameToId ***/
template <typename T> class XNameToId {
public:
  explicit XNameToId (IdGen <T> *id_gen);
  void clear();
  void init (IdGen <T> *id_gen);

  T get_id (const string &name);
  bool add_name (const string &name, T *new_id);
  T *operator[] (int id);

  set <T> get_ids();
private:
  map <string, T> name_to_id;
  IdGen <T> *id_gen;
  DISALLOW_COPY_AND_ASSIGN (XNameToId);
};


/*** Range ***/
template <class Iterator>
struct Range {
  Iterator begin, end;
  typedef typename std::iterator_traits <Iterator>::value_type value_type;
  typedef typename std::iterator_traits <Iterator>::reference reference_type;
  bool empty();
  size_t size();
  value_type &operator[] (size_t i);
  reference_type operator * ();
  value_type *operator -> ();
  void next();
  Range();
  Range (Iterator begin, Iterator end);
};

template <class A> Range<typename A::iterator> all (A &v);
template <class A> Range<typename A::const_iterator> all (const A &v);
template <class A> Range<typename A::reverse_iterator> reversed_all (A &v);
template <class A> Range<typename A::const_reverse_iterator> reversed_all (const A &v);

template <class T> Range <T> all (const Range <T> &range);

#include "graph.hpp"
