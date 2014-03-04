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


stdClass::stdClass (void): data (create (0)) {
}


void stdClass::mutate_if_needed (void) {
  if (data->string_size * 5 > 3 * data->string_buf_size) {
    array_inner *new_array = array_inner::create (0, data->string_size * 2 + 1, false);

    for (const string_hash_entry *it = data->begin(); it != data->end(); it = data->next (it)) {
      new_array->set_map_value (it->int_key, it->string_key, it->value, false);
    }

    data->dispose();
    data = new_array;
  }
}

stdClass::array_inner *stdClass::create (int new_size) {
  //TODO create (0)

  if (new_size > array_inner::MAX_HASHTABLE_SIZE) {
    php_critical_error ("max object size exceeded");
  }

  new_size = 2 * new_size + 3;
  if (new_size % 5 == 0) {
    new_size += 2;
  }

  array_inner *p = (array_inner *)dl::allocate0 ((dl::size_type)(sizeof (array_inner) + new_size * sizeof (string_hash_entry)));
  p->ref_cnt = 0;
  p->max_key = -1;
  p->end()->next = p->get_pointer (p->end());
  p->end()->prev = p->get_pointer (p->end());
  p->int_buf_size = 0;
  p->string_buf_size = new_size;
  return p;
}

const char* stdClass::get_class (void) const {
  return "stdClass";
}

array <string> stdClass::sleep (void) {
  array <string> result (array_size (data->string_size, 0, true));
  for (const string_hash_entry *it = data->begin(); it != data->end(); it = data->next (it)) {
    result.push_back (it->string_key);
  }

  return result;
}

void stdClass::wakeup (void) {
}

void stdClass::set (const string &key, const var &value) {
  mutate_if_needed();
  data->set_map_value (key.hash(), key, value, false);
}

var stdClass::get (const string &key) {
  return data->get_value (key.hash(), key);
}

bool stdClass::isset (const string &key) {
  return data->isset_value (key.hash(), key);
}

void stdClass::unset (const string &key) {
  return data->unset_map_value (key.hash(), key);
}

string stdClass::to_string (void) {
  php_critical_error ("object of class %s can not be converted to string", get_class());
}

array <var> stdClass::to_array (void) {
  array <var> result (array_size (0, data->string_size, false));
  for (const string_hash_entry *it = data->begin(); it != data->end(); it = data->next (it)) {
    if (it->string_key.is_int()) {
      result.p->set_map_value (it->string_key.to_int(), it->value, false);
    } else {
      result.p->set_map_value (it->int_key, it->string_key, it->value, false);
    }
  }

  return result;
}

var stdClass::call (const string &name, const array <var> &params) {
  php_critical_error ("call to undefined method %s::%s()", get_class(), name.c_str());
}

stdClass::~stdClass (void) {
  for (const string_hash_entry *it = data->begin(); it != data->end(); it = data->next (it)) {
    it->value.~var();
    it->string_key.~string();
  }

  dl::deallocate ((void *)data, (dl::size_type)(sizeof (array_inner) + data->string_buf_size * sizeof (array <var, var>::string_hash_entry)));
}


template <class T>
object_ptr <T>::object_ptr (void): o (static_cast <stdClass *> (dl::allocate (sizeof (T)))) {
  new (o) T();
}

template <class T>
object_ptr <T>::object_ptr (const object_ptr &other): o (other.o) {
  o->data->ref_cnt++;
}

template <class T>
template <class T1>
object_ptr <T>::object_ptr (const object_ptr <T1> &other): o (dynamic_cast <T *> (other.o)) {
  if (o == NULL) {
    T some_object;
    php_critical_error ("can't cast object of type %s to type %s", other.o->get_class(), some_object.get_class());
  }
  o->data->ref_cnt++;
}

template <class T>
object_ptr <T>& object_ptr <T>::operator = (const object_ptr &other) {
  other.o->data->ref_cnt++;
  destroy();
  o = other.o;
  o->data->ref_cnt++;
  other.o->data->ref_cnt--;
  return *this;
}

template <class T>
template <class T1>
object_ptr <T>& object_ptr <T>::operator = (const object_ptr <T1> &other) {
  other.o->data->ref_cnt++;
  destroy();
  o = other.o;
  o->data->ref_cnt++;
  other.o->data->ref_cnt--;
  return *this;
}

template <class T>
void object_ptr <T>::destroy (void) {
  if (o->data && o->data->ref_cnt-- <= 0) {
    o->~T();
    dl::deallocate (o, sizeof (T));
  }
}

template <class T>
object_ptr <T>::~object_ptr (void) {
  destroy();
}


template <class T>
array <string> object_ptr <T>::sleep (void) {
  return o->sleep();
}

template <class T>
void object_ptr <T>::wakeup (void) {
  return o->wakeup();
}

template <class T>
void object_ptr <T>::set (const string &key, const var &value) {
  return o->set (key, value);
}

template <class T>
var object_ptr <T>::get (const string &key) {
  return o->get (key);
}

template <class T>
bool object_ptr <T>::isset (const string &key) {
  return o->isset (key);
}

template <class T>
void object_ptr <T>::unset (const string &key) {
  return o->unset (key);
}


template <class T>
string object_ptr <T>::to_string (void) {
  return o->to_string();
}

template <class T>
array <var> object_ptr <T>::to_array (void) {
  return o->to_array();
}

template <class T>
void object_ptr <T>::swap (object_ptr &other) {
  ::swap (o, other.o);
}


template <class T>
int object_ptr <T>::get_reference_counter (void) const {
  return o->data->ref_cnt + 1;
}


template <class T>
void swap (object_ptr <T> &lhs, object_ptr <T> &rhs) {
  lhs.swap (rhs);
}


template <class T>
bool eq2 (const object_ptr <T> &lhs, const object_ptr <T> &rhs) {
  if (strcmp (lhs.o->get_class(), rhs.o->get_class())) {
    return false;
  }
  return eq2 (lhs.o->to_array(), rhs.o->to_array());
}

template <class T1, class T2>
bool eq2 (const object_ptr <T1> &lhs, const object_ptr <T2> &rhs) {
  if (strcmp (lhs.o->get_class(), rhs.o->get_class())) {
    return false;
  }
  return eq2 (lhs.o->to_array(), rhs.o->to_array());
}

template <class T>
bool equals (const object_ptr <T> &lhs, const object_ptr <T> &rhs) {
  return lhs.o == rhs.o;
}

template <class T1, class T2>
bool equals (const object_ptr <T1> &lhs, const object_ptr <T2> &rhs) {
  return lhs.o == rhs.o;
}

