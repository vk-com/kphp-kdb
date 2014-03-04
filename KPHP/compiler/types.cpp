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

#include "types.h"
#include "data.h"

/*** PrimitiveType ***/
map <string, PrimitiveType> name_to_ptype;
void get_ptype_by_name_init() {
#define FOREACH_PTYPE(tp) name_to_ptype[PTYPE_NAME(tp)] = tp;
#include "foreach_ptype.h"
}

PrimitiveType get_ptype_by_name (const string &s) {
  static bool inited = false;
  if (!inited) {
    get_ptype_by_name_init();
    inited = true;
  }
  map <string, PrimitiveType>::iterator it = name_to_ptype.find (s);
  if (it == name_to_ptype.end()) {
    return tp_Error;
  }
  return it->second;
}

const char *ptype_name (PrimitiveType id) {
  switch (id) {
#define FOREACH_PTYPE(tp) case tp: return PTYPE_NAME (tp);
#include "foreach_ptype.h"
  default:
    return NULL;
  }
}

bool can_store_bool (PrimitiveType tp) {
  return tp == tp_var || tp == tp_MC || tp == tp_DB ||
         tp == tp_Exception || tp == tp_RPC || tp == tp_bool ||
         tp == tp_Any;
}
PrimitiveType type_lca (PrimitiveType a, PrimitiveType b) {
  if (a == b) {
    return a;	
  }
  if (a == tp_Error || b == tp_Error) {
    return tp_Error;
  }

  if (a == tp_Any) {
    return tp_Any;
  }
  if (b == tp_Any) {
    return a;
  }

  if (b == tp_CreateAny) {
    return tp_Any;
  }

  if (a > b) {
    std::swap (a, b);
  }

  if (b == tp_Error) {
    return tp_Error;
  }

  if (a == tp_Unknown) {
    return b;
  }

  if (b >= tp_MC && a >= tp_int) { // Memcache and e.t.c can store only bool
    return tp_Error;
  }

  if (b == tp_void) { // void can't store anything (except void)
    return tp_Error;
  }

  if ((b == tp_UInt || b == tp_Long || b == tp_ULong) && a != tp_int) { // UInt, Long and ULong can store only int
    return tp_Error;
  }

  if (tp_int <= a && a <= tp_float && tp_int <= b && b <= tp_float) { // float can store int
    return tp_float;
  }

  if (tp_bool <= a && a <= tp_var && tp_bool <= b && b <= tp_var) {
    return tp_var;
  }

  return max (a, b);
}

/*** Key ***/
Enumerator <string> Key::string_keys;
Enumerator <int> Key::int_keys;
MapToId <string, string> Key::string_key_map (&string_keys);
MapToId <int, int> Key::int_key_map (&int_keys);

Key::Key()
  : id (-1) {
}
Key::Key (const Key &other)
  : id (other.id) {
}
Key &Key::operator = (const Key &other) {
  id = other.id;
  return *this;
}
Key::Key (int id)
  : id (id) {
}

Key Key::any_key() {
  return Key (0);
}

Key Key::string_key (const string &key) {
  int id = string_key_map.add_name (key, key);
  dl_assert (id != -1, "bug");
  Key res = Key (id * 2 + 2);
  return res;
}

Key Key::int_key (int key) {
  int id = int_key_map.add_name (key, key);
  dl_assert (id != -1, "bug");
  Key res = Key (id * 2 + 1);
  return res;
}

Key::KeyType Key::type() const {
  if (id == 0) {
    return any_key_e;
  }
  if (id > 0 && id % 2 == 1) {
    return int_key_e;
  }
  if (id > 0 && id % 2 == 0) {
    return string_key_e;
  }
  dl_unreachable ("invalid id in Key instance");
  return any_key_e;
}

const string &Key::strval() const {
  dl_assert (type() == string_key_e, "");
  return string_keys[(id - 1) / 2];
}

int Key::intval() const {
  dl_assert (type() == int_key_e, "");
  return int_keys[id / 2];
}

string Key::to_string() const {
  switch (type()) {
    case int_key_e:
      return dl_pstr ("%d", intval());
    case string_key_e:
      return dl_pstr ("%s", strval().c_str());
    case any_key_e:
      return "Any";
    default:
      dl_unreachable("...");
  }
  return "fail";
}

/*** MultiKey ***/
MultiKey::MultiKey()
  : keys_() {
}

MultiKey::MultiKey (const vector <Key> &keys)
  : keys_ (keys) {
}

MultiKey::MultiKey (const MultiKey &multi_key)
  : keys_ (multi_key.keys_) {
}
MultiKey &MultiKey::operator = (const MultiKey &multi_key) {
  if (this == &multi_key) {
    return *this;
  }
  keys_ = multi_key.keys_;
  return *this;
}
void MultiKey::push_back (const Key &key) {
  keys_.push_back (key);
}
string MultiKey::to_string() const {
  string res;
  for (MultiKey::range i = all (*this); !i.empty(); i.next()) {
    res += "[";
    const Key &key = *i;
    res += key.to_string();
    res += "]";
  }
  return res;
}
MultiKey::iterator MultiKey::begin() const {
  return keys_.begin();
}
MultiKey::iterator MultiKey::end() const {
  return keys_.end();
}
MultiKey::reverse_iterator MultiKey::rbegin() const {
  return keys_.rbegin();
}
MultiKey::reverse_iterator MultiKey::rend() const {
  return keys_.rend();
}

vector <MultiKey> MultiKey::any_key_vec;
void MultiKey::init_static() {
  vector <Key> keys;
  for (int i = 0; i < 10; i++) {
    any_key_vec.push_back (MultiKey (keys));
    keys.push_back (Key::any_key());
  }
}
const MultiKey &MultiKey::any_key (int depth) {
  if (depth >= 0 && depth < (int)any_key_vec.size()) {
    return any_key_vec[depth];
  }
  kphp_fail();
  return any_key_vec[0];
}


/*** TypeData::Writer ***/
TypeData::Writer::Writer (TypeData *type_data)
  : type_data_ (type_data), old_key_ (Key::any_key()), cur_ (type_data->next_.begin()), new_next_() {
  dl_assert (type_data != NULL, "TypeData::Writer can't be initialized by NULL");
}

TypeData::Writer::~Writer() {
  dl_assert (type_data_ == NULL, "no flush was called for TypeData::Writer");
}

TypeData * TypeData::Writer::write_at (const Key &key) {
  dl_assert (old_key_ <= key, "keys must be asked in ascending order");
  old_key_ = key;

  for (iterator end = type_data_->next_.end(); cur_ != end && cur_->first <= key; cur_++) {
    if (cur_->first == key) {
      return cur_->second;
    }
  }

  if (new_next_.empty() || new_next_.back().first != key) {
    new_next_.push_back (make_pair (key, TypeData::get_type (tp_Unknown)->clone()));
  }
  return new_next_.back().second;
}

void TypeData::Writer::flush() {
  if (!new_next_.empty()) {
    NextT merged_next (new_next_.size() + type_data_->next_.size());
    iterator end = merge (new_next_.begin(), new_next_.end(), type_data_->next_.begin(), type_data_->next_.end(), merged_next.begin());
    dl_assert (end == merged_next.end(), "bug in TypeData::Writer");
    swap (type_data_->next_, merged_next);
    type_data_->on_changed();
  }
  type_data_ = NULL;
}


/*** TypeData ***/
vector <TypeData*> TypeData::primitive_types;
vector <TypeData*> TypeData::array_types;
void TypeData::init_static() {
  if (!primitive_types.empty()) {
    return;
  }
  primitive_types.resize (ptype_size);
  array_types.resize (ptype_size);
  #define FOREACH_PTYPE(tp) primitive_types[tp] = new TypeData (tp);
  #include "foreach_ptype.h"

  #define FOREACH_PTYPE(tp) \
    array_types[tp] = new TypeData (tp_array); \
    array_types[tp]->set_lca_at (MultiKey::any_key (1), primitive_types[tp ==  tp_Any ? tp_CreateAny : tp]);
  #include "foreach_ptype.h"
}
const TypeData *TypeData::get_type (PrimitiveType type) {
  return primitive_types[type];
}
const TypeData *TypeData::get_type (PrimitiveType array, PrimitiveType type) {
  if (array != tp_array) {
    return get_type (array);
  }
  return array_types[type];
}

TypeData::TypeData()
  : ptype_ (tp_Unknown), flags_ (0), generation_ (current_generation()),
    parent_ (NULL), any_next_ (NULL), next_() {
}
TypeData::TypeData (PrimitiveType ptype)
  : ptype_ (ptype), flags_ (0), generation_ (current_generation()),
    parent_ (NULL), any_next_ (NULL), next_() {
  if (ptype_ == tp_False) {
    set_or_false_flag (true);
    ptype_ = tp_Unknown;
  }
}
TypeData::TypeData (const TypeData &from) :
  ptype_ (from.ptype_),
  class_type_ (from.class_type_),
  flags_ (from.flags_),
  generation_ (from.generation_),
  parent_ (NULL),
  any_next_ (NULL),
  next_ (from.next_) {
  if (from.any_next_ != NULL) {
    any_next_ = from.any_next_->clone();
    any_next_->parent_ = this;
  }
  for (NextT::iterator it = next_.begin(); it != next_.end(); it++) {
    TypeData *ptr = it->second;
    assert (ptr != NULL);
    ptr = ptr->clone();
    ptr->parent_ = this;
    it->second = ptr;
  }
}

TypeData::~TypeData() {
  assert (parent_ == NULL);

  if (any_next_ != NULL) {
    any_next_->parent_ = NULL;
    delete any_next_;
  }
  for (NextT::iterator it = next_.begin(); it != next_.end(); it++) {
    TypeData *ptr = it->second;
    ptr->parent_ = NULL;
    delete ptr;
  }
}

TypeData *TypeData::at (const Key &key) const {
  dl_assert (structured(), "bug in TypeData");
  if (key == Key::any_key()) {
    return any_next_;
  }
  lookup_iterator it = lower_bound (next_.begin(), next_.end(), KeyValue (key, NULL));
  if (it != next_.end() && it->first == key) {
    return it->second;
  }
  return NULL;
}

TypeData *TypeData::at_force (const Key &key) {
  dl_assert (structured(), "bug in TypeData");

  TypeData *res = at (key);
  if (res != NULL) {
    return res;
  }

  TypeData *value = get_type (tp_Unknown)->clone();
  value->parent_ = this;
  value->on_changed();

  if (key == Key::any_key()) {
    any_next_ = value;
    return value;
  }

  KeyValue to_add (key, value);
  NextT::iterator insert_pos = lower_bound (next_.begin(), next_.end(), KeyValue (key, NULL));
  next_.insert (insert_pos, to_add);

  return value;
}

PrimitiveType TypeData::ptype() const {
  return ptype_;
}

void TypeData::set_ptype (PrimitiveType new_ptype) {
  if (new_ptype != ptype_) {
    ptype_ = new_ptype;
    if (new_ptype == tp_Error) {
      set_error_flag (true);
    }
    on_changed();
  }
}

ClassPtr TypeData::class_type() const {
  if (ptype() == tp_Class) {
    return class_type_;
  }
  return ClassPtr();
}

void TypeData::set_class_type (ClassPtr new_class_type) {
  if (class_type_.is_null()) {
    class_type_ = new_class_type;
    on_changed();
  } else if (!(class_type_ == new_class_type)) {
    set_ptype (tp_Error);
  }
}

type_flags_t TypeData::flags() const {
  return flags_;
}

template <TypeData::flag_id_t FLAG> bool TypeData::get_flag() const {
  return flags_ & FLAG;
}
template <TypeData::flag_id_t FLAG> void TypeData::set_flag (bool f) {
  bool old_f = get_flag <FLAG> ();
  if (old_f) {
    dl_assert (f, dl_pstr ("It is forbidden to remove flag %d", FLAG));
  } else if (f) {
    flags_ |= FLAG;
    on_changed();
  }
}

template<> void TypeData::set_flag <TypeData::error_flag_e> (bool f) {
  if (f) {
    if (!get_flag <error_flag_e>()) {
      flags_ |= error_flag_e;
      if (parent_ != NULL) {
        parent_->set_flag <error_flag_e> (true);
      }
    }
  }
}

void TypeData::set_flags (type_flags_t new_flags) {
  dl_assert ((flags_ & new_flags) == flags_, "It is forbiddent to remove flag");
  if (flags_ != new_flags) {
    if (new_flags & error_flag_e) {
      set_error_flag (true);
    }
    flags_ = new_flags;
    on_changed();
  }
}
bool TypeData::or_false_flag() const {
  return get_flag <or_false_flag_e>();
}
void TypeData::set_or_false_flag (bool f) {
  set_flag <or_false_flag_e> (f);
}
bool TypeData::write_flag() const {
  return get_flag <read_flag_e>();
}
void TypeData::set_write_flag (bool f) {
  set_flag <write_flag_e> (f);
}
bool TypeData::read_flag() const {
  return get_flag <read_flag_e>();
}
void TypeData::set_read_flag (bool f) {
  set_flag <read_flag_e> (f);
}
bool TypeData::error_flag() const {
  return get_flag <error_flag_e>();
}
void TypeData::set_error_flag (bool f) {
  set_flag <error_flag_e> (f);
}

bool TypeData::use_or_false() const {
  return !can_store_bool(ptype()) && or_false_flag();
}

bool TypeData::structured() const {
  //Will be changed
  return ptype() == tp_array;
}
TypeData::generation_t TypeData::generation() const {
  return generation_;
}

void TypeData::on_changed() {
  generation_ = current_generation();
  if (parent_ != NULL) {
    if (parent_->generation_ < current_generation()) {
      parent_->on_changed();
    }
  }
}

TypeData *TypeData::clone() const {
  assert (this != NULL);
  return new TypeData (*this);
}

const TypeData *TypeData::read_at (const Key &key) {
  if (ptype() == tp_var) {
    return get_type (tp_var);
  }
  if (!structured()) {
    return get_type (tp_Unknown);
  }
  TypeData *res = at_force (key);
  res->set_read_flag (true);

  if (key != Key::any_key()) {
    TypeData *any_value = at_force (Key::any_key());
    any_value->set_read_flag (true);
  }

  return res;
}
const TypeData *TypeData::const_read_at (const Key &key) const {
  if (ptype() == tp_var) {
    return get_type (tp_var);
  }
  if (!structured()) {
    return get_type (tp_Unknown);
  }
  TypeData *res = at (key);
  if (res == NULL) {
    return get_type (tp_Unknown);
  }
  return res;
}

const TypeData *TypeData::const_read_at (const MultiKey &multi_key) const {
  const TypeData *res = this;
  for (MultiKey::range i = all (multi_key); !i.empty(); i.next()) {
    res = res->const_read_at (*i);
  }
  return res;
}

const TypeData *TypeData::read_at_dfs (MultiKey::iterator begin, MultiKey::iterator end) {
  if (begin == end) {
    return this;
  }

  const Key &key = *begin;
  TypeData *key_value = at_force (key);
  key_value->set_read_flag (true);

  if (key != Key::any_key()) {
    TypeData *any_value = at_force (Key::any_key());
    any_value->read_at_dfs (begin + 1, end);
  }

  return key_value->read_at_dfs (begin + 1, end);
}

const TypeData *TypeData::read_at (const MultiKey &multi_key) {
  set_read_flag (true);
  return read_at_dfs (multi_key.begin(), multi_key.end());
}

void TypeData::make_structured() {
  if (ptype() <= tp_array) {
    PrimitiveType new_type = type_lca (ptype(), tp_array);
    set_ptype (new_type);
  }
}
TypeData *TypeData::write_at (const Key &key) {
  make_structured();
  if (!structured()) {
    return NULL;
  }
  TypeData *res = at_force (key);
  res->set_write_flag (true);
  return res;
}

TypeData *TypeData::lookup_at (const Key &key) const {
  if (!structured()) {
    return NULL;
  }
  TypeData *res = at (key);
  return res;
}

TypeData::lookup_iterator TypeData::lookup_begin() const {
  if (!structured()) {
    return next_.end();
  }
  return next_.begin();
}
TypeData::lookup_iterator TypeData::lookup_end() const {
  return next_.end();
}

void TypeData::set_lca (const TypeData *rhs, bool save_or_false) {
  if (rhs == NULL) {
    return;
  }
  TypeData *lhs = this;

  PrimitiveType new_type = type_lca (lhs->ptype(), rhs->ptype());
  lhs->set_ptype (new_type);

  type_flags_t mask = save_or_false ? -1 : ~or_false_flag_e;
  type_flags_t new_flags = lhs->flags_ | (rhs->flags_ & mask);
  lhs->set_flags (new_flags);

  if (rhs->ptype() == tp_Class) {
    lhs->set_class_type (rhs->class_type());
  }

  if (!lhs->structured()) {
    return;
  }

  TypeData *lhs_any_key = lhs->at_force (Key::any_key());
  TypeData *rhs_any_key = rhs->lookup_at (Key::any_key());
  //if (lhs != rhs_any_key) {
    lhs_any_key->set_lca (rhs_any_key, true);
  //}

  {
    TypeData::Writer writer (lhs);
    for (__typeof (rhs->lookup_begin()) rhs_it = rhs->lookup_begin(); rhs_it != rhs->lookup_end(); rhs_it++) {
      Key rhs_key = rhs_it->first;
      TypeData *rhs_value = rhs_it->second;
      TypeData *lhs_value = writer.write_at (rhs_key);

      lhs_value->set_lca (rhs_value, true);
    }
    writer.flush();
  }
}

void TypeData::set_lca_at (const MultiKey &multi_key, const TypeData *rhs, bool save_or_false) {
  TypeData *cur = this;
  for (MultiKey::iterator it = multi_key.begin(); it != multi_key.end(); it++) {
    cur = cur->write_at (*it);
    if (cur == NULL) {
      return;
    }
  }
  cur->set_lca (rhs, save_or_false);
  for (MultiKey::reverse_iterator it = multi_key.rbegin(); it != multi_key.rend(); it++) {
    cur = cur->parent_;
    if (*it != Key::any_key()) {
      TypeData *any_value = cur->at_force (Key::any_key());
      TypeData *key_value = cur->write_at (*it);
      any_value->set_lca (key_value, true);
    }
  }
}

void TypeData::fix_inf_array() {
  //hack: used just to make current version stable
  int depth = 0;
  TypeData *cur = this;
  while (cur != NULL) {
    cur = cur->lookup_at (Key::any_key());
    depth++;
  }
  if (depth > 6) {
    set_lca_at (MultiKey::any_key (6), TypeData::get_type (tp_var));
  }
}

void TypeData::set_lca (PrimitiveType ptype) {
  set_lca (TypeData::get_type (ptype), true);
}
TLS <TypeData::generation_t> TypeData::current_generation_;
void TypeData::inc_generation() {
  (*current_generation_)++;
}
TypeData::generation_t TypeData::current_generation() {
  return *current_generation_;
}
void TypeData::upd_generation (TypeData::generation_t other_generation) {
  if (other_generation >= *current_generation_) {
    *current_generation_ = other_generation;
  }
}

static inline bool cmp (const TypeData *a, const TypeData *b) {
  if (a == b) {
    return 0;
  }
  if (a == NULL) {
    return -1;
  }
  if (b == 0) {
    return 1;
  }
  if (a->ptype() < b->ptype()) {
    return -1;
  }
  if (a->ptype() > b->ptype()) {
    return +1;
  }
  if (a->flags() < b->flags()) {
    return -1;
  }
  if (a->flags() > b->flags()) {
    return +1;
  }

  int res;
  res = cmp (a->lookup_at (Key::any_key()), b->lookup_at (Key::any_key()));
  if (res) {
    return res;
  }

  TypeData::lookup_iterator a_begin = a->lookup_begin(),
    a_end = a->lookup_end(),
    b_begin = b->lookup_begin(),
    b_end = b->lookup_end();
  int a_size = (int)(a_end - a_begin);
  int b_size = (int)(b_end - b_begin);
  if (a_size < b_size) {
    return -1;
  }
  if (a_size > b_size) {
    return +1;
  }

  while (a_begin != a_end) {
    if (a_begin->first < b_begin->first) {
      return -1;
    }
    if (a_begin->first > b_begin->first) {
      return +1;
    }
    res = cmp (a_begin->second, b_begin->second);
    if (res) {
      return res;
    }
    a_begin++;
    b_begin++;
  }
  return 0;
}

bool operator < (const TypeData &a, const TypeData &b) {
  return cmp (&a, &b) < 0;
}
bool operator == (const TypeData &a, const TypeData &b) {
  return cmp (&a, &b) == 0;
}

void type_out_impl (const TypeData *type, string *res) {
  bool or_false = type->use_or_false();
  PrimitiveType tp = type->ptype();

  if (or_false && tp == tp_Unknown) {
    or_false = false;
    tp = tp_bool;
  }

  if (or_false) {
    *res += "OrFalse < ";
  }

  if (tp == tp_Class) {
    *res += type->class_type()->name;
  } else if (tp == tp_DB) {
    *res += "MyDB";
  } else if (tp == tp_MC) {
    *res += "MyMemcache";
  } else if (tp == tp_RPC) {
    *res += "rpc_connection";
  } else if (tp == tp_float) {
    *res += "double";
  } else {
    *res += ptype_name (tp);
  }

  type = type->lookup_at (Key::any_key());
  if (type != NULL) {
    *res += "< ";
    type_out_impl (type, res);
    *res += " >";
  }

  if (or_false) {
    *res += " >";
  }
}

string type_out (const TypeData *type) {
  string res;
  type_out_impl (type, &res);
  return res;
}

int type_strlen (const TypeData *type) {
  PrimitiveType tp = type->ptype();
  switch (tp) {
    case tp_Unknown:
      if (type->use_or_false()) {
        return STRLEN_FALSE;
      }
      return STRLEN_UNKNOWN;
    case tp_False:
      return STRLEN_FALSE_;
    case tp_bool:
      return STRLEN_BOOL_;
    case tp_int:
      return STRLEN_INT;
    case tp_float:
      return STRLEN_FLOAT;
    case tp_array:
      return STRLEN_ARRAY_;
    case tp_string:
      return STRLEN_STRING;
    case tp_var:
      return STRLEN_VAR;
    case tp_UInt:
      return STRLEN_UINT;
    case tp_Long:
      return STRLEN_LONG;
    case tp_ULong:
      return STRLEN_ULONG;
    case tp_MC:
      return STRLEN_MC;
    case tp_DB:
      return STRLEN_DB;
    case tp_RPC:
      return STRLEN_RPC;
    case tp_Exception:
      return STRLEN_EXCEPTION;
    case tp_Class:
      return STRLEN_CLASS;
    case tp_void:
      return STRLEN_VOID;
    case tp_Error:
      return STRLEN_ERROR;
    case tp_Any:
      return STRLEN_ANY;
    case tp_CreateAny:
      return STRLEN_CREATE_ANY;
    case tp_regexp:
    case ptype_size:
      kphp_fail();
  }
  return STRLEN_ERROR;
}

void test_PrimitiveType() {
  const char *cur;
  cur = ptype_name <tp_Unknown>();
  assert (cur != NULL);
  assert (!strcmp (cur, "Unknown"));

  cur = ptype_name <ptype_size>();
  assert (cur == NULL);

  cur = ptype_name (ptype_size);
  assert (cur == NULL);

  cur = ptype_name (tp_array);
  assert (cur != NULL);
  assert (!strcmp (cur, "array"));


  PrimitiveType tp;
  tp = get_ptype_by_name ("array");
  assert (tp == tp_array);
  tp = get_ptype_by_name ("MC");
  assert (tp == tp_MC);
  tp = get_ptype_by_name ("XX");
  assert (tp == tp_Error);
  tp = get_ptype_by_name ("X");
  assert (tp == tp_Error);
  tp = get_ptype_by_name ("");
  assert (tp == tp_Error);
}

void test_TypeData() {
  TypeData *a = TypeData::get_type (tp_Unknown)->clone();

  assert (type_out (a) == "Unknown");
  a->set_lca_at (MultiKey::any_key (2), TypeData::get_type (tp_int));
  assert (type_out (a) == "array< array< int > >");
  a->set_lca (TypeData::get_type (tp_var));
  assert (type_out (a) == "var");
  a->set_lca_at (MultiKey::any_key (2), TypeData::get_type (tp_int));
  assert (type_out (a) == "var");


  printf ("OK\n");
}
