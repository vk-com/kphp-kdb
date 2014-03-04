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
#include <cstdio>
#include <cassert>
#include <cstring>
#include <map>
#include <string>
#include <algorithm>

#include "utils.h"
#include "data_ptr.h"

#include "bicycle.h"

//H
/*** PrimitiveType ***/

//foreach_ptype.h contain calls of FOREACH_PTYPE(tp) for each primitive_type name.
//All new primitive types should be added to foreach_ptype.h
//All primitive type names must start with tp_
typedef enum {
#define FOREACH_PTYPE(tp) tp,
#include "foreach_ptype.h"
  ptype_size
} PrimitiveType;

//interface to PrimitiveType
template <PrimitiveType T_ID> inline const char *ptype_name();
const char *ptype_name (PrimitiveType tp);
PrimitiveType get_ptype_by_name (const string &s);
PrimitiveType type_lca (PrimitiveType a, PrimitiveType b);
bool can_store_bool (PrimitiveType tp);

/*** Key ***/
class Key {
private:
  int id;
  static Enumerator <string> string_keys;
  static Enumerator <int> int_keys;
  static MapToId <string, string> string_key_map;
  static MapToId <int, int> int_key_map;

  explicit Key (int id);

public:
  Key();
  Key (const Key &other);
  Key &operator = (const Key &other);
  typedef enum {string_key_e, int_key_e, any_key_e} KeyType;
  static Key any_key ();
  static Key string_key (const string &key);
  static Key int_key (int key);
  string to_string() const;

  KeyType type() const;
  const string &strval() const;
  int intval() const;
  friend inline bool operator < (const Key &a, const Key &b);
  friend inline bool operator > (const Key &a, const Key &b);
  friend inline bool operator <= (const Key &a, const Key &b);
  friend inline bool operator != (const Key &a, const Key &b);
  friend inline bool operator == (const Key &a, const Key &b);
};
inline bool operator <= (const Key &a, const Key &b);
inline bool operator < (const Key &a, const Key &b);
inline bool operator > (const Key &a, const Key &b);
inline bool operator != (const Key &a, const Key &b);
inline bool operator == (const Key &a, const Key &b);

/*** MultiKey ***/
class MultiKey {
private:
  vector <Key> keys_;
public:
  typedef vector<Key>::const_iterator iterator;
  typedef vector<Key>::const_reverse_iterator reverse_iterator;
  typedef Range <iterator> range;
  MultiKey();
  MultiKey (const MultiKey &multi_key);
  MultiKey &operator = (const MultiKey &multi_key);
  MultiKey (const vector <Key> &keys);
  void push_back (const Key &key);
  string to_string() const;

  iterator begin() const;
  iterator end() const;
  reverse_iterator rbegin() const;
  reverse_iterator rend() const;

  static vector <MultiKey> any_key_vec;
  static void init_static();
  static const MultiKey &any_key (int depth);
};

/*** TypeData ***/
// read/write/lookup at
// check if something changed since last
typedef unsigned long type_flags_t;

class TypeData {
public:
  typedef pair <Key, TypeData *> KeyValue;
  typedef vector <KeyValue> NextT;
  typedef NextT::const_iterator lookup_iterator;
  typedef NextT::iterator iterator;
  typedef long generation_t;
private:
  PrimitiveType ptype_;
  ClassPtr class_type_;
  type_flags_t flags_;
  generation_t generation_;

  TypeData *parent_;
  TypeData *any_next_;
  NextT next_;

  static TLS <generation_t> current_generation_;
  TypeData();
  explicit TypeData (PrimitiveType ptype);
  TypeData &operator = (const TypeData &from);

  TypeData *at (const Key &key) const;
  TypeData *at_force (const Key &key);

  typedef enum {write_flag_e = 1, read_flag_e = 2, or_false_flag_e = 4, error_flag_e = 8} flag_id_t;
  template <flag_id_t FLAG> bool get_flag() const;
  template <flag_id_t FLAG> void set_flag (bool f);

  TypeData *write_at (const Key &key);
  bool or_false_flag() const;
public:
  class Writer {
  private:
    TypeData *type_data_;
    Key old_key_;
    iterator cur_;
    vector <pair <Key, TypeData *> > new_next_;
  public:
    explicit Writer (TypeData *type_data);
    ~Writer();
    TypeData *write_at (const Key &key);
    void flush();
  };

  TypeData (const TypeData &from);
  ~TypeData();

  PrimitiveType ptype() const;
  type_flags_t flags() const;
  void set_ptype (PrimitiveType new_ptype);

  ClassPtr class_type() const;
  void set_class_type (ClassPtr new_class_type);

  void set_or_false_flag (bool f);
  bool use_or_false() const;
  bool write_flag() const;
  void set_write_flag (bool f);
  bool read_flag() const;
  void set_read_flag (bool f);
  bool error_flag() const;
  void set_error_flag (bool f);
  void set_flags (type_flags_t new_flags);

  bool structured() const;
  void make_structured();
  generation_t generation() const;
  void on_changed();
  TypeData *clone() const;

  TypeData *lookup_at (const Key &key) const;
  lookup_iterator lookup_begin() const;
  lookup_iterator lookup_end() const;

  const TypeData *read_at (const Key &key);
  const TypeData *const_read_at (const Key &key) const;
  const TypeData *const_read_at (const MultiKey &multi_key) const;
  const TypeData *read_at_dfs (MultiKey::iterator begin, MultiKey::iterator end);
  const TypeData *read_at (const MultiKey &multi_key);
  void set_lca (const TypeData *rhs, bool save_or_false = true);
  void set_lca_at (const MultiKey &multi_key, const TypeData *rhs, bool save_or_false = true);
  void set_lca (PrimitiveType ptype);
  void fix_inf_array();

  static vector <TypeData*> primitive_types;
  static vector <TypeData*> array_types;
  static void init_static();
  static const TypeData *get_type (PrimitiveType type);
  static const TypeData *get_type (PrimitiveType array, PrimitiveType type);
  //FIXME:??
  static void inc_generation();
  static generation_t current_generation();
  static void upd_generation (TypeData::generation_t other_generation);
};

inline bool operator < (const TypeData::KeyValue &a, const TypeData::KeyValue &b);
bool operator < (const TypeData &a, const TypeData &b);
bool operator == (const TypeData &a, const TypeData &b);

string type_out (const TypeData *type);
int type_strlen (const TypeData *type);

void test_TypeData();
void test_PrimitiveType();
#include "types.hpp"
