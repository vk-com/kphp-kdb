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

/*
template <class T>
class force_convert_to <T, T> {
public:
  inline static T& convert (T &x) {
    return x;
  }

  inline static const T& convert (const T &x) {
    return x;
  }
};
*/

template <class T>
class force_convert_to <T, typename array_stored_type <T>::type> {
public:
  inline static T& convert (typename array_stored_type <T>::type &x) {
    return x;
  }

  inline static const T& convert (const typename array_stored_type <T>::type &x) {
    return x;
  }
};

template <>
class force_convert_to <bool, var> {
public:
  inline static bool& convert (var &x) {
    return x.b;
  }

  inline static const bool& convert (const var &x) {
    return x.b;
  }
};

template <>
class force_convert_to <int, var> {
public:
  inline static int& convert (var &x) {
    return x.i;
  }

  inline static const int& convert (const var &x) {
    return x.i;
  }
};

template <>
class force_convert_to <double, var> {
public:
  inline static double& convert (var &x) {
    return x.f;
  }

  inline static const double& convert (const var &x) {
    return x.f;
  }
};

template <>
class force_convert_to <string, var> {
public:
  inline static string& convert (var &x) {
    return *STRING(x.s);
  }

  inline static const string& convert (const var &x) {
    return *STRING(x.s);
  }
};

template <>
class force_convert_to <array <var>, var> {
public:
  inline static array <var>& convert (var &x) {
    return *ARRAY(x.a);
  }

  inline static const array <var>& convert (const var &x) {
    return *ARRAY(x.a);
  }
};


template <class T, class TT>
typename array <T, TT>::key_type array <T, TT>::int_hash_entry::get_key (void) const {
  return key_type (int_key);
}

template <class T, class TT>
typename array <T, TT>::key_type array <T, TT>::string_hash_entry::get_key (void) const {
  return key_type (string_key);
}

template <class T, class TT>
bool array <T, TT>::is_int_key (const typename array <T, TT>::key_type &key) {
  return key.is_int();
}

template <class T, class TT>
int array <T, TT>::array_inner::choose_bucket (const int key, const int buf_size) {
  return (unsigned int)(key) /* 2654435761u */ % buf_size;
}

template <class T, class TT>
const typename array <T, TT>::entry_pointer_type array <T, TT>::array_inner::EMPTY_POINTER = entry_pointer_type();

template <class T, class TT>
const T array <T, TT>::array_inner::empty_T = T();


template <class T, class TT>
bool array <T, TT>::array_inner::is_vector (void) const {
  return string_buf_size == -1;
}


template <class T, class TT>
typename array <T, TT>::list_hash_entry *array <T, TT>::array_inner::get_entry (entry_pointer_type pointer) const {
  return (list_hash_entry *)((char *)this + pointer);
//  return pointer;
}

template <class T, class TT>
typename array <T, TT>::entry_pointer_type array <T, TT>::array_inner::get_pointer (list_hash_entry *entry) const {
  return (entry_pointer_type)((char *)entry - (char *)this);
//  return entry;
}


template <class T, class TT>
const typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::begin (void) const {
  return (const string_hash_entry *)get_entry (end_.next);
}

template <class T, class TT>
const typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::next (const string_hash_entry *p) const {
  return (const string_hash_entry *)get_entry (p->next);
}

template <class T, class TT>
const typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::prev (const string_hash_entry *p) const {
  return (const string_hash_entry *)get_entry (p->prev);
}

template <class T, class TT>
const typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::end (void) const {
  return (const string_hash_entry *)&end_;
}

template <class T, class TT>
typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::begin (void) {
  return (string_hash_entry *)get_entry (end_.next);
}

template <class T, class TT>
typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::next (string_hash_entry *p) {
  return (string_hash_entry *)get_entry (p->next);
}

template <class T, class TT>
typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::prev (string_hash_entry *p) {
  return (string_hash_entry *)get_entry (p->prev);
}

template <class T, class TT>
typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::end (void) {
  return (string_hash_entry *)&end_;
}

template <class T, class TT>
bool array <T, TT>::array_inner::is_string_hash_entry (const string_hash_entry *p) const {
  return p >= get_string_entries();
}

template <class T, class TT>
const typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::get_string_entries (void) const {
  return (const string_hash_entry *)(int_entries + int_buf_size);
}

template <class T, class TT>
typename array <T, TT>::string_hash_entry *array <T, TT>::array_inner::get_string_entries (void) {
  return (string_hash_entry *)(int_entries + int_buf_size);
}


template <class T, class TT>
typename array <T, TT>::array_inner* array <T, TT>::array_inner::create (int new_int_size, int new_string_size, bool is_vector) {
  if (new_int_size + new_string_size > MAX_HASHTABLE_SIZE) {
    php_critical_error ("max array size exceeded");
  }

  if (new_int_size < 0) {
    new_int_size = 0;
  }
  if (new_string_size < 0) {
    new_string_size = 0;
  }
  if (new_int_size + new_string_size < MIN_HASHTABLE_SIZE) {
//    new_int_size = MIN_HASHTABLE_SIZE;
  }

  if (is_vector) {
    php_assert (new_string_size == 0);
    new_int_size += 2;

    array_inner *p = (array_inner *)dl::allocate ((dl::size_type)(sizeof (array_inner) + new_int_size * sizeof (TT)));
    p->ref_cnt = 0;
    p->max_key = -1;
    p->int_size = 0;
    p->int_buf_size = new_int_size;
    p->string_size = 0;
    p->string_buf_size = -1;
    return p;
  }

  new_int_size = 2 * new_int_size + 3;
  if (new_int_size % 5 == 0) {
    new_int_size += 2;
  }
  new_string_size = 2 * new_string_size + 3;
  if (new_string_size % 5 == 0) {
    new_string_size += 2;
  }

  array_inner *p = (array_inner *)dl::allocate0 ((dl::size_type)(sizeof (array_inner) + new_int_size * sizeof (int_hash_entry) + new_string_size * sizeof (string_hash_entry)));
  p->ref_cnt = 0;
  p->max_key = -1;
  p->end()->next = p->get_pointer (p->end());
  p->end()->prev = p->get_pointer (p->end());
  p->int_buf_size = new_int_size;
  p->string_buf_size = new_string_size;
  return p;
}

template <class T, class TT>
void array <T, TT>::array_inner::dispose (void) {
  if (dl::memory_begin <= (size_t)this && (size_t)this < dl::memory_end) {
    ref_cnt--;
    if (ref_cnt <= -1) {
      if (is_vector()) {
        for (int i = 0; i < int_size; i++) {
          ((TT *)int_entries)[i].~TT();
        }

        dl::deallocate ((void *)this, (dl::size_type)(sizeof (array_inner) + int_buf_size * sizeof (TT)));
        return;
      }

      for (const string_hash_entry *it = begin(); it != end(); it = next (it)) {
        it->value.~TT();
        if (is_string_hash_entry (it)) {
          it->string_key.~string();
        }
      }

      dl::deallocate ((void *)this, (dl::size_type)(sizeof (array_inner) + int_buf_size * sizeof (int_hash_entry) + string_buf_size * sizeof (string_hash_entry)));
    }
  }
}


template <class T, class TT>
typename array <T, TT>::array_inner *array <T, TT>::array_inner::ref_copy (void) {
  if (dl::memory_begin <= (size_t)this && (size_t)this < dl::memory_end) {
    ref_cnt++;
  }
  return this;
}


template <class T, class TT>
const var array <T, TT>::array_inner::get_var (int int_key) const {
  if (is_vector()) {
    if ((unsigned int)int_key < (unsigned int)int_size) {
      return force_convert_to <T>::convert (((const TT *)int_entries)[int_key]);
    }

    return var();
  }

  int bucket = choose_bucket (int_key, int_buf_size);
  while (int_entries[bucket].next != EMPTY_POINTER && int_entries[bucket].int_key != int_key) {
    if (unlikely (++bucket == int_buf_size)) {
      bucket = 0;
    }
  }

  if (int_entries[bucket].next == EMPTY_POINTER) {
    return var();
  }

  return force_convert_to <T>::convert (int_entries[bucket].value);
}


template <class T, class TT>
const T array <T, TT>::array_inner::get_value (int int_key) const {
  if (is_vector()) {
    if ((unsigned int)int_key < (unsigned int)int_size) {
      return force_convert_to <T>::convert (((const TT *)int_entries)[int_key]);
    }

    return empty_T;
  }

  int bucket = choose_bucket (int_key, int_buf_size);
  while (int_entries[bucket].next != EMPTY_POINTER && int_entries[bucket].int_key != int_key) {
    if (unlikely (++bucket == int_buf_size)) {
      bucket = 0;
    }
  }

  if (int_entries[bucket].next == EMPTY_POINTER) {
    return empty_T;
  }

  return force_convert_to <T>::convert (int_entries[bucket].value);
}


template <class T, class TT>
T& array <T, TT>::array_inner::push_back_vector_value (const TT &v) {
  php_assert (int_size < int_buf_size);
  new (&((TT *)int_entries)[int_size]) TT (v);
  max_key++;
  int_size++;

  return force_convert_to <T>::convert (((TT *)int_entries)[max_key]);
}

template <class T, class TT>
T& array <T, TT>::array_inner::set_vector_value (int int_key, const TT &v, bool save_value) {
  if (int_key == int_size) {
    return push_back_vector_value (v);
  } else {
    if (!save_value) {
      ((TT *)int_entries)[int_key] = v;
    }
    return force_convert_to <T>::convert (((TT *)int_entries)[int_key]);
  }
}


template <class T, class TT>
T& array <T, TT>::array_inner::set_map_value (int int_key, const TT &v, bool save_value) {
  int bucket = choose_bucket (int_key, int_buf_size);
  while (int_entries[bucket].next != EMPTY_POINTER && int_entries[bucket].int_key != int_key) {
    if (unlikely (++bucket == int_buf_size)) {
      bucket = 0;
    }
  }

  if (int_entries[bucket].next == EMPTY_POINTER) {
    int_entries[bucket].int_key = int_key;

    int_entries[bucket].prev = end()->prev;
    get_entry (end()->prev)->next = get_pointer (&int_entries[bucket]);

    int_entries[bucket].next = get_pointer (end());
    end()->prev = get_pointer (&int_entries[bucket]);

    new (&int_entries[bucket].value) TT(v);

    int_size++;

    if (int_key > max_key) {
      max_key = int_key;
    }
  } else {
    if (!save_value) {
      int_entries[bucket].value = v;
    }
  }

  return force_convert_to <T>::convert (int_entries[bucket].value);
}

template <class T, class TT>
bool array <T, TT>::array_inner::has_key (int int_key) const {
  if (is_vector()) {
    return ((unsigned int)int_key < (unsigned int)int_size);
  }

  int bucket = choose_bucket (int_key, int_buf_size);
  while (int_entries[bucket].next != EMPTY_POINTER && int_entries[bucket].int_key != int_key) {
    if (unlikely (++bucket == int_buf_size)) {
      bucket = 0;
    }
  }

  return int_entries[bucket].next != EMPTY_POINTER;
}

template <class T, class TT>
bool array <T, TT>::array_inner::isset_value (int int_key) const {
  return has_key (int_key);
}

template <>
inline bool array <var, var>::array_inner::isset_value (int int_key) const {
  if (is_vector()) {
    return ((unsigned int)int_key < (unsigned int)int_size && !((var *)int_entries)[int_key].is_null());
  }

  int bucket = choose_bucket (int_key, int_buf_size);
  while (int_entries[bucket].next != EMPTY_POINTER && int_entries[bucket].int_key != int_key) {
    if (unlikely (++bucket == int_buf_size)) {
      bucket = 0;
    }
  }

  return int_entries[bucket].next != EMPTY_POINTER && !int_entries[bucket].value.is_null();
}

template <class T, class TT>
void array <T, TT>::array_inner::unset_vector_value (void) {
  ((TT *)int_entries)[max_key].~TT();
  max_key--;
  int_size--;
}

template <class T, class TT>
void array <T, TT>::array_inner::unset_map_value (int int_key) {
  int bucket = choose_bucket (int_key, int_buf_size);
  while (int_entries[bucket].next != EMPTY_POINTER && int_entries[bucket].int_key != int_key) {
    if (unlikely (++bucket == int_buf_size)) {
      bucket = 0;
    }
  }

  if (int_entries[bucket].next != EMPTY_POINTER) {
    int_entries[bucket].int_key = 0;

    get_entry (int_entries[bucket].prev)->next = int_entries[bucket].next;
    get_entry (int_entries[bucket].next)->prev = int_entries[bucket].prev;

    int_entries[bucket].next = EMPTY_POINTER;
    int_entries[bucket].prev = EMPTY_POINTER;

    int_entries[bucket].value.~TT();

    int_size--;

#define FIXD(a) ((a) >= int_buf_size ? (a) - int_buf_size : (a))
#define FIXU(a, m) ((a) <= (m) ? (a) + int_buf_size : (a))
    int j, rj, ri = bucket;
    for (j = bucket + 1; 1; j++) {
      rj = FIXD(j);
      if (int_entries[rj].next == EMPTY_POINTER) {
        break;
      }

      int bucket_j = choose_bucket (int_entries[rj].int_key, int_buf_size);
      int wnt = FIXU(bucket_j, bucket);

      if (wnt > j || wnt <= bucket) {
        list_hash_entry *ei = int_entries + ri, *ej = int_entries + rj;
        memcpy (ei, ej, sizeof (int_hash_entry));
        ej->next = EMPTY_POINTER;

        get_entry (ei->prev)->next = get_pointer (ei);
        get_entry (ei->next)->prev = get_pointer (ei);

        ri = rj;
        bucket = j;
      }
    }
#undef FIXU
#undef FIXD
  }
}


template <class T, class TT>
const var array <T, TT>::array_inner::get_var (int int_key, const string &string_key) const {
  if (is_vector()) {
    return var();
  }

  const string_hash_entry *string_entries = get_string_entries();
  int bucket = choose_bucket (int_key, string_buf_size);
  while (string_entries[bucket].next != EMPTY_POINTER && (string_entries[bucket].int_key != int_key || string_entries[bucket].string_key != string_key)) {
    if (unlikely (++bucket == string_buf_size)) {
      bucket = 0;
    }
  }

  if (string_entries[bucket].next == EMPTY_POINTER) {
    return var();
  }

  return force_convert_to <T>::convert (string_entries[bucket].value);
}

template <class T, class TT>
const T array <T, TT>::array_inner::get_value (int int_key, const string &string_key) const {
  if (is_vector()) {
    return empty_T;
  }

  const string_hash_entry *string_entries = get_string_entries();
  int bucket = choose_bucket (int_key, string_buf_size);
  while (string_entries[bucket].next != EMPTY_POINTER && (string_entries[bucket].int_key != int_key || string_entries[bucket].string_key != string_key)) {
    if (unlikely (++bucket == string_buf_size)) {
      bucket = 0;
    }
  }

  if (string_entries[bucket].next == EMPTY_POINTER) {
    return empty_T;
  }

  return force_convert_to <T>::convert (string_entries[bucket].value);
}

template <class T, class TT>
T& array <T, TT>::array_inner::set_map_value (int int_key, const string &string_key, const TT &v, bool save_value) {
  string_hash_entry *string_entries = get_string_entries();
  int bucket = choose_bucket (int_key, string_buf_size);
  while (string_entries[bucket].next != EMPTY_POINTER && (string_entries[bucket].int_key != int_key || string_entries[bucket].string_key != string_key)) {
    if (unlikely (++bucket == string_buf_size)) {
      bucket = 0;
    }
  }

  if (string_entries[bucket].next == EMPTY_POINTER) {
    string_entries[bucket].int_key = int_key;
    new (&string_entries[bucket].string_key) string (string_key);

    string_entries[bucket].prev = end()->prev;
    get_entry (end()->prev)->next = get_pointer (&string_entries[bucket]);

    string_entries[bucket].next = get_pointer (end());
    end()->prev = get_pointer (&string_entries[bucket]);

    new (&string_entries[bucket].value) TT(v);

    string_size++;
  } else {
    if (!save_value) {
      string_entries[bucket].value = v;
    }
  }

  return force_convert_to <T>::convert (string_entries[bucket].value);
}

template <class T, class TT>
bool array <T, TT>::array_inner::has_key (int int_key, const string &string_key) const {
  if (is_vector()) {
    return false;
  }

  const string_hash_entry *string_entries = get_string_entries();
  int bucket = choose_bucket (int_key, string_buf_size);
  while (string_entries[bucket].next != EMPTY_POINTER && (string_entries[bucket].int_key != int_key || string_entries[bucket].string_key != string_key)) {
    if (unlikely (++bucket == string_buf_size)) {
      bucket = 0;
    }
  }

  return string_entries[bucket].next != EMPTY_POINTER;
}

template <class T, class TT>
bool array <T, TT>::array_inner::isset_value (int int_key, const string &string_key) const {
  return has_key (int_key, string_key);
}

template <>
inline bool array <var, var>::array_inner::isset_value (int int_key, const string &string_key) const {
  if (is_vector()) {
    return false;
  }

  const string_hash_entry *string_entries = get_string_entries();
  int bucket = choose_bucket (int_key, string_buf_size);
  while (string_entries[bucket].next != EMPTY_POINTER && (string_entries[bucket].int_key != int_key || string_entries[bucket].string_key != string_key)) {
    if (unlikely (++bucket == string_buf_size)) {
      bucket = 0;
    }
  }

  return string_entries[bucket].next != EMPTY_POINTER && !string_entries[bucket].value.is_null();
}

template <class T, class TT>
void array <T, TT>::array_inner::unset_map_value (int int_key, const string &string_key) {
  string_hash_entry *string_entries = get_string_entries();
  int bucket = choose_bucket (int_key, string_buf_size);
  while (string_entries[bucket].next != EMPTY_POINTER && (string_entries[bucket].int_key != int_key || string_entries[bucket].string_key != string_key)) {
    if (unlikely (++bucket == string_buf_size)) {
      bucket = 0;
    }
  }

  if (string_entries[bucket].next != EMPTY_POINTER) {
    string_entries[bucket].int_key = 0;
    string_entries[bucket].string_key.~string();

    get_entry (string_entries[bucket].prev)->next = string_entries[bucket].next;
    get_entry (string_entries[bucket].next)->prev = string_entries[bucket].prev;

    string_entries[bucket].next = EMPTY_POINTER;
    string_entries[bucket].prev = EMPTY_POINTER;

    string_entries[bucket].value.~TT();

    string_size--;

#define FIXD(a) ((a) >= string_buf_size ? (a) - string_buf_size : (a))
#define FIXU(a, m) ((a) <= (m) ? (a) + string_buf_size : (a))
    int j, rj, ri = bucket;
    for (j = bucket + 1; 1; j++) {
      rj = FIXD(j);
      if (string_entries[rj].next == EMPTY_POINTER) {
        break;
      }

      int bucket_j = choose_bucket (string_entries[rj].int_key, string_buf_size);
      int wnt = FIXU(bucket_j, bucket);

      if (wnt > j || wnt <= bucket) {
        list_hash_entry *ei = string_entries + ri, *ej = string_entries + rj;
        memcpy (ei, ej, sizeof (string_hash_entry));
        ej->next = EMPTY_POINTER;

        get_entry (ei->prev)->next = get_pointer (ei);
        get_entry (ei->next)->prev = get_pointer (ei);

        ri = rj;
        bucket = j;
      }
    }
#undef FIXU
#undef FIXD
  }
}


template <class T, class TT>
bool array <T, TT>::is_vector (void) const {
  return p->is_vector();
}

template <class T, class TT>
void array <T, TT>::mutate_if_needed (void) {
  array_inner *new_array = NULL;
  if (p->is_vector()) {
    if (p->ref_cnt > 0 || dl::memory_begin > (size_t)p || (size_t)p >= dl::memory_end) {
      new_array = array_inner::create (p->int_size * 2, 0, true);

      int size = p->int_size;
      TT *it = (TT *)p->int_entries;

      for (int i = 0; i < size; i++) {
        new_array->push_back_vector_value (it[i]);
      }
    } else if (p->int_size == p->int_buf_size) {
      p = (array_inner *)dl::reallocate ((void *)p,
                                         (dl::size_type)(sizeof (array_inner) + 2 * p->int_buf_size * sizeof (TT)),
                                         (dl::size_type)(sizeof (array_inner) + p->int_buf_size * sizeof (TT)));
      p->int_buf_size *= 2;
    }
  } else {
    int mul = 2;
    if (p->int_size * 5 > 3 * p->int_buf_size || p->string_size * 5 > 3 * p->string_buf_size ||
        ((mul = 1) && (p->ref_cnt > 0 || dl::memory_begin > (size_t)p || (size_t)p >= dl::memory_end))) {
      new_array = array_inner::create (p->int_size * mul + 1, p->string_size * mul + 1, false);

      for (const string_hash_entry *it = p->begin(); it != p->end(); it = p->next (it)) {
        if (p->is_string_hash_entry (it)) {
          new_array->set_map_value (it->int_key, it->string_key, it->value, false);
        } else {
          new_array->set_map_value (it->int_key, it->value, false);
        }
      }
    }
  }

  if (new_array != NULL) {
    p->dispose();
    p = new_array;
  }
}

template <class T, class TT>
array <T, TT>::const_iterator::const_iterator (void): self (NULL), entry (NULL) {
}

template <class T, class TT>
array <T, TT>::const_iterator::const_iterator (const typename array <T, TT>::array_inner *self, const list_hash_entry *entry): self (self), entry (entry) {
}

template <class T, class TT>
const T array <T, TT>::const_iterator::get_value (void) const {
  if (self->is_vector()) {
    return force_convert_to <T>::convert (*(const TT *)entry);
  }

  return force_convert_to <T>::convert (((const int_hash_entry *)entry)->value);
}

template <class T, class TT>
typename array <T, TT>::key_type array <T, TT>::const_iterator::get_key (void) const {
  if (self->is_vector()) {
    return key_type ((int)((const TT *)entry - (const TT *)self->int_entries));
  }

  if (self->is_string_hash_entry ((const string_hash_entry *)entry)) {
    return ((const string_hash_entry *)entry)->get_key();
  } else {
    return ((const int_hash_entry *)entry)->get_key();
  }
}

template <class T, class TT>
typename array <T, TT>::const_iterator& array <T, TT>::const_iterator::operator -- (void) {
  if (self->is_vector()) {
    entry = (const list_hash_entry *)((const TT *)entry - 1);
  } else {
    entry = (const list_hash_entry *)self->prev ((const string_hash_entry *)entry);
  }
  return *this;
}

template <class T, class TT>
typename array <T, TT>::const_iterator& array <T, TT>::const_iterator::operator ++ (void) {
  if (self->is_vector()) {
    entry = (const list_hash_entry *)((const TT *)entry + 1);
  } else {
    entry = (const list_hash_entry *)self->next ((const string_hash_entry *)entry);
  }
  return *this;
}

template <class T, class TT>
bool array <T, TT>::const_iterator::operator == (const array <T, TT>::const_iterator &other) const {
  return entry == other.entry;
}

template <class T, class TT>
bool array <T, TT>::const_iterator::operator != (const array <T, TT>::const_iterator &other) const {
  return entry != other.entry;
}


template <class T, class TT>
typename array <T, TT>::const_iterator array <T, TT>::begin (void) const {
  if (is_vector()) {
    return typename array <T, TT>::const_iterator (p, p->int_entries);
  }

  return typename array <T, TT>::const_iterator (p, (const list_hash_entry *)p->begin());
}

template <class T, class TT>
typename array <T, TT>::const_iterator array <T, TT>::middle (int n) const {
  int l = count();

  if (is_vector()) {
    if (n < 0) {
      n += l;
      if (n < 0) {
        return end();
      }
    }
    if (n >= l) {
      return end();
    }

    return typename array <T, TT>::const_iterator (p, (list_hash_entry *)((TT *)p->int_entries + n));
  }

  if (n < -l / 2) {
    n += l;
    if (n < 0) {
      return end();
    }
  }

  if (n > l / 2) {
    n -= l;
    if (n >= 0) {
      return end();
    }
  }

  const string_hash_entry *result;
  if (n < 0) {
    result = p->end();
    while (n < 0) {
      n++;
      result = p->prev (result);
    }
  } else {
    result = p->begin();
    while (n > 0) {
      n--;
      result = p->next (result);
    }
  }
  return typename array <T, TT>::const_iterator (p, (const list_hash_entry *)result);
}

template <class T, class TT>
typename array <T, TT>::const_iterator array <T, TT>::end (void) const {
  if (is_vector()) {
    return typename array <T, TT>::const_iterator (p, (const list_hash_entry *)((const TT *)p->int_entries + p->int_size));
  }

  return typename array <T, TT>::const_iterator (p, (const list_hash_entry *)p->end());
}


template <class T, class TT>
array <T, TT>::iterator::iterator (void): self (NULL), entry (NULL) {
}

template <class T, class TT>
array <T, TT>::iterator::iterator (typename array <T, TT>::array_inner *self, list_hash_entry *entry): self (self), entry (entry) {
}

template <class T, class TT>
T& array <T, TT>::iterator::get_value (void) {
  if (self->is_vector()) {
    return force_convert_to <T>::convert (*(TT *)entry);
  }

  return force_convert_to <T>::convert (((int_hash_entry *)entry)->value);
}

template <class T, class TT>
typename array <T, TT>::key_type array <T, TT>::iterator::get_key (void) const {
  if (self->is_vector()) {
    return key_type ((int)((const TT *)entry - (const TT *)self->int_entries));
  }

  if (self->is_string_hash_entry ((const string_hash_entry *)entry)) {
    return ((const string_hash_entry *)entry)->get_key();
  } else {
    return ((const int_hash_entry *)entry)->get_key();
  }
}

template <class T, class TT>
typename array <T, TT>::iterator& array <T, TT>::iterator::operator -- (void) {
  if (self->is_vector()) {
    entry = (list_hash_entry *)((TT *)entry - 1);
  } else {
    entry = (list_hash_entry *)self->prev ((string_hash_entry *)entry);
  }
  return *this;
}

template <class T, class TT>
typename array <T, TT>::iterator& array <T, TT>::iterator::operator ++ (void) {
  if (self->is_vector()) {
    entry = (list_hash_entry *)((TT *)entry + 1);
  } else {
    entry = (list_hash_entry *)self->next ((string_hash_entry *)entry);
  }
  return *this;
}

template <class T, class TT>
bool array <T, TT>::iterator::operator == (const array <T, TT>::iterator &other) const {
  return entry == other.entry;
}

template <class T, class TT>
bool array <T, TT>::iterator::operator != (const array <T, TT>::iterator &other) const {
  return entry != other.entry;
}


template <class T, class TT>
typename array <T, TT>::iterator array <T, TT>::begin (void) {
  mutate_if_needed();

  if (is_vector()) {
    return typename array <T, TT>::iterator (p, p->int_entries);
  }

  return typename array <T, TT>::iterator (p, p->begin());
}

template <class T, class TT>
typename array <T, TT>::iterator array <T, TT>::middle (int n) {
  int l = count();

  if (is_vector()) {
    if (n < 0) {
      n += l;
      if (n < 0) {
        return end();
      }
    }
    if (n >= l) {
      return end();
    }

    return typename array <T, TT>::iterator (p, (list_hash_entry *)((TT *)p->int_entries + n));
  }

  if (n < -l / 2) {
    n += l;
    if (n < 0) {
      return end();
    }
  }

  if (n > l / 2) {
    n -= l;
    if (n >= 0) {
      return end();
    }
  }

  string_hash_entry *result;
  if (n < 0) {
    result = p->end();
    while (n < 0) {
      n++;
      result = p->prev (result);
    }
  } else {
    result = p->begin();
    while (n > 0) {
      n--;
      result = p->next (result);
    }
  }
  return typename array <T, TT>::iterator (p, result);
}

template <class T, class TT>
typename array <T, TT>::iterator array <T, TT>::end (void) {
  if (is_vector()) {
    return typename array <T, TT>::iterator (p, (list_hash_entry *)((TT *)p->int_entries + p->int_size));
  }

  return typename array <T, TT>::iterator (p, p->end());
}


template <class T, class TT>
void array <T, TT>::convert_to_map (void) {
  array_inner *new_array = array_inner::create (p->int_size + 4, p->int_size + 4, false);

  for (int it = 0; it != p->int_size; it++) {
    new_array->set_map_value (it, ((TT *)p->int_entries)[it], false);
  }

  php_assert (new_array->max_key == p->max_key);

  p->dispose();
  p = new_array;
}

template <class T, class TT>
template <class T1, class TT1>
void array <T, TT>::copy_from (const array <T1, TT1> &other) {
  array_inner *new_array = array_inner::create (other.p->int_size, other.p->string_size, other.is_vector());

  if (new_array->is_vector()) {
    int size = other.p->int_size;
    TT1 *it = (TT1 *)other.p->int_entries;

    for (int i = 0; i < size; i++) {
      new_array->push_back_vector_value (convert_to <T>::convert (force_convert_to <T1>::convert (it[i])));
    }
  } else {
    for (const typename array <T1, TT1>::string_hash_entry *it = other.p->begin(); it != other.p->end(); it = other.p->next (it)) {
      if (other.p->is_string_hash_entry (it)) {
        new_array->set_map_value (it->int_key, it->string_key, convert_to <T>::convert (force_convert_to <T1>::convert (it->value)), false);
      } else {
        new_array->set_map_value (it->int_key, convert_to <T>::convert (force_convert_to <T1>::convert (it->value)), false);
      }
    }
  }

  p = new_array;

  php_assert (new_array->int_size == other.p->int_size);
  php_assert (new_array->string_size == other.p->string_size);
}


template <class T, class TT>
array <T, TT>::array (void): p (array_inner::create (0, 0, true)) {
}


template <class T, class TT>
array <T, TT>::array (const array_size &s): p (array_inner::create (s.int_size, s.string_size, s.is_vector)) {
}


template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1): p (array_inner::create (2, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2): p (array_inner::create (3, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2, const T &a3): p (array_inner::create (4, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
  p->push_back_vector_value (a3);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4): p (array_inner::create (5, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
  p->push_back_vector_value (a3);
  p->push_back_vector_value (a4);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5): p (array_inner::create (6, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
  p->push_back_vector_value (a3);
  p->push_back_vector_value (a4);
  p->push_back_vector_value (a5);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6): p (array_inner::create (7, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
  p->push_back_vector_value (a3);
  p->push_back_vector_value (a4);
  p->push_back_vector_value (a5);
  p->push_back_vector_value (a6);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6, const T &a7): p (array_inner::create (8, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
  p->push_back_vector_value (a3);
  p->push_back_vector_value (a4);
  p->push_back_vector_value (a5);
  p->push_back_vector_value (a6);
  p->push_back_vector_value (a7);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6, const T &a7, const T &a8): p (array_inner::create (9, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
  p->push_back_vector_value (a3);
  p->push_back_vector_value (a4);
  p->push_back_vector_value (a5);
  p->push_back_vector_value (a6);
  p->push_back_vector_value (a7);
  p->push_back_vector_value (a8);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6, const T &a7, const T &a8, const T &a9): p (array_inner::create (10, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
  p->push_back_vector_value (a3);
  p->push_back_vector_value (a4);
  p->push_back_vector_value (a5);
  p->push_back_vector_value (a6);
  p->push_back_vector_value (a7);
  p->push_back_vector_value (a8);
  p->push_back_vector_value (a9);
}

template <class T, class TT>
array <T, TT>::array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6, const T &a7, const T &a8, const T &a9, const T &a10): p (array_inner::create (11, 0, true)) {
  p->push_back_vector_value (a0);
  p->push_back_vector_value (a1);
  p->push_back_vector_value (a2);
  p->push_back_vector_value (a3);
  p->push_back_vector_value (a4);
  p->push_back_vector_value (a5);
  p->push_back_vector_value (a6);
  p->push_back_vector_value (a7);
  p->push_back_vector_value (a8);
  p->push_back_vector_value (a9);
  p->push_back_vector_value (a10);
}

template <class T, class TT>
array <T, TT>::array (const array <T, TT> &other): p (other.p->ref_copy()) {
}

template <class T, class TT>
template <class T1, class TT1>
array <T, TT>::array (const array <T1, TT1> &other) {
  copy_from (other);
}

template <class T, class TT>
template <class T1>
array <T, TT>::array (const array <T1, TT> &other): p ((typename array::array_inner *)other.p->ref_copy()) {//TODO very-very bad
}


template <class T, class TT>
array <T, TT>& array <T, TT>::operator = (const array &other) {
  typename array::array_inner *other_copy = other.p->ref_copy();
  destroy();
  p = other_copy;
  return *this;
}

template <class T, class TT>
template <class T1, class TT1>
array <T, TT>& array <T, TT>::operator = (const array <T1, TT1> &other) {
  typename array <T1, TT1>::array_inner *other_copy = other.p->ref_copy();
  destroy();
  copy_from (other);
  other_copy->dispose();
  return *this;
}

template <class T, class TT>
template <class T1>
array <T, TT>& array <T, TT>::operator = (const array <T1, TT> &other) {
  typename array::array_inner *other_copy = (typename array::array_inner *)other.p->ref_copy();
  destroy();
  p = other_copy;
  return *this;
}


template <class T, class TT>
void array <T, TT>::destroy (void) {
  p->dispose();
}

template <class T, class TT>
array <T, TT>::~array (void) {
  if (p) {//for zeroed global variables
    destroy();
  }
}


template <class T, class TT>
T& array <T, TT>::operator[] (int int_key) {
  if (is_vector()) {
    if ((unsigned int)int_key <= (unsigned int)p->int_size) {
      mutate_if_needed();
      return p->set_vector_value (int_key, array_inner::empty_T, true);
    }

    convert_to_map();
  } else {
    mutate_if_needed();
  }

  return p->set_map_value (int_key, array_inner::empty_T, true);
}

template <class T, class TT>
T& array <T, TT>::operator[] (const string &string_key) {
  int int_val;
  if ((unsigned int)(string_key[0] - '-') < 13u && string_key.try_to_int (&int_val)) {
    return (*this)[int_val];
  }

  if (is_vector()) {
    convert_to_map();
  } else {
    mutate_if_needed();
  }

  return p->set_map_value (string_key.hash(), string_key, array_inner::empty_T, true);
}

template <class T, class TT>
T& array <T, TT>::operator[] (const var &v) {
  switch (v.type) {
    case var::NULL_TYPE:
      return (*this)[string()];
    case var::BOOLEAN_TYPE:
      return (*this)[v.b];
    case var::INTEGER_TYPE:
      return (*this)[v.i];
    case var::FLOAT_TYPE:
      return (*this)[(int)v.f];
    case var::STRING_TYPE:
      return (*this)[*STRING(v.s)];
    case var::ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return (*this)[ARRAY(v.a)->to_int()];
    case var::OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return (*this)[1];
    default:
      php_assert (0);
      exit (1);
  }
}

template <class T, class TT>
T& array <T, TT>::operator[] (const const_iterator &it) {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    if (is_vector()) {
      if ((unsigned int)key <= (unsigned int)p->int_size) {
        mutate_if_needed();
        return p->set_vector_value (key, array_inner::empty_T, true);
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    return p->set_map_value (key, array_inner::empty_T, true);
  } else {
    string_hash_entry *entry = (string_hash_entry *)it.entry;
    bool is_string_entry = it.self->is_string_hash_entry (entry);

    if (is_vector()) {
      if (!is_string_entry && (unsigned int)entry->int_key <= (unsigned int)p->int_size) {
        mutate_if_needed();
        return p->set_vector_value (entry->int_key, array_inner::empty_T, true);
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    if (is_string_entry) {
      return p->set_map_value (entry->int_key, entry->string_key, array_inner::empty_T, true);
    } else {
      return p->set_map_value (entry->int_key, array_inner::empty_T, true);
    }
  }
}

template <class T, class TT>
T& array <T, TT>::operator[] (const iterator &it) {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    if (is_vector()) {
      if ((unsigned int)key <= (unsigned int)p->int_size) {
        mutate_if_needed();
        return p->set_vector_value (key, array_inner::empty_T, true);
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    return p->set_map_value (key, array_inner::empty_T, true);
  } else {
    const string_hash_entry *entry = (const string_hash_entry *)it.entry;
    bool is_string_entry = it.self->is_string_hash_entry (entry);

    if (is_vector()) {
      if (!is_string_entry && (unsigned int)entry->int_key <= (unsigned int)p->int_size) {
        mutate_if_needed();
        return p->set_vector_value (entry->int_key, array_inner::empty_T, true);
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    if (is_string_entry) {
      return p->set_map_value (entry->int_key, entry->string_key, array_inner::empty_T, true);
    } else {
      return p->set_map_value (entry->int_key, array_inner::empty_T, true);
    }
  }
}


template <class T, class TT>
void array <T, TT>::set_value (int int_key, const T &v) {
  if (is_vector()) {
    if ((unsigned int)int_key <= (unsigned int)p->int_size) {
      mutate_if_needed();
      p->set_vector_value (int_key, v, false);
      return;
    }

    convert_to_map();
  } else {
    mutate_if_needed();
  }

  p->set_map_value (int_key, v, false);
}

template <class T, class TT>
void array <T, TT>::set_value (const string &string_key, const T &v) {
  int int_val;
  if ((unsigned int)(string_key[0] - '-') < 13u && string_key.try_to_int (&int_val)) {
    set_value (int_val, v);
    return;
  }

  if (is_vector()) {
    convert_to_map();
  } else {
    mutate_if_needed();
  }

  p->set_map_value (string_key.hash(), string_key, v, false);
}

template <class T, class TT>
void array <T, TT>::set_value (const var &v, const T &value) {
  switch (v.type) {
    case var::NULL_TYPE:
      return set_value (string(), value);
    case var::BOOLEAN_TYPE:
      return set_value (v.b, value);
    case var::INTEGER_TYPE:
      return set_value (v.i, value);
    case var::FLOAT_TYPE:
      return set_value ((int)v.f, value);
    case var::STRING_TYPE:
      return set_value (*STRING(v.s), value);
    case var::ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return set_value (ARRAY(v.a)->to_int(), value);
    case var::OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return set_value (1, value);
    default:
      php_assert (0);
      exit (1);
  }
}

template <class T, class TT>
void array <T, TT>::set_value (const const_iterator &it) {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    if (is_vector()) {
      if ((unsigned int)key <= (unsigned int)p->int_size) {
        mutate_if_needed();
        p->set_vector_value (key, *(TT *)it.entry, false);
        return;
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    p->set_map_value (key, *(TT *)it.entry, false);
  } else {
    string_hash_entry *entry = (string_hash_entry *)it.entry;
    bool is_string_entry = it.self->is_string_hash_entry (entry);

    if (is_vector()) {
      if (!is_string_entry && (unsigned int)entry->int_key <= (unsigned int)p->int_size) {
        mutate_if_needed();
        p->set_vector_value (entry->int_key, entry->value, false);
        return;
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    if (is_string_entry) {
      p->set_map_value (entry->int_key, entry->string_key, entry->value, false);
    } else {
      p->set_map_value (entry->int_key, entry->value, false);
    }
  }
}

template <class T, class TT>
void array <T, TT>::set_value (const iterator &it) {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    if (is_vector()) {
      if ((unsigned int)key <= (unsigned int)p->int_size) {
        mutate_if_needed();
        p->set_vector_value (key, *(TT *)it.entry, false);
        return;
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    p->set_map_value (key, *(TT *)it.entry, false);
  } else {
    string_hash_entry *entry = (string_hash_entry *)it.entry;
    bool is_string_entry = it.self->is_string_hash_entry (entry);

    if (is_vector()) {
      if (!is_string_entry && (unsigned int)entry->int_key <= (unsigned int)p->int_size) {
        mutate_if_needed();
        p->set_vector_value (entry->int_key, entry->value, false);
        return;
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    if (is_string_entry) {
      p->set_map_value (entry->int_key, entry->string_key, entry->value, false);
    } else {
      p->set_map_value (entry->int_key, entry->value, false);
    }
  }
}


template <class T, class TT>
const var array <T, TT>::get_var (int int_key) const {
  return p->get_var (int_key);
}

template <class T, class TT>
const var array <T, TT>::get_var (const string &string_key) const {
  int int_val;
  if ((unsigned int)(string_key[0] - '-') < 13u && string_key.try_to_int (&int_val)) {
    return p->get_var (int_val);
  }

  return p->get_var (string_key.hash(), string_key);
}

template <class T, class TT>
const var array <T, TT>::get_var (const var &v) const {
  switch (v.type) {
    case var::NULL_TYPE:
      return get_var (string());
    case var::BOOLEAN_TYPE:
      return get_var (v.b);
    case var::INTEGER_TYPE:
      return get_var (v.i);
    case var::FLOAT_TYPE:
      return get_var ((int)v.f);
    case var::STRING_TYPE:
      return get_var (*STRING(v.s));
    case var::ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return get_var (ARRAY(v.a)->to_int());
    case var::OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return get_var (1);
    default:
      php_assert (0);
      exit (1);
  }
}


template <class T, class TT>
const T array <T, TT>::get_value (int int_key) const {
  return p->get_value (int_key);
}

template <class T, class TT>
const T array <T, TT>::get_value (const string &string_key) const {
  int int_val;
  if ((unsigned int)(string_key[0] - '-') < 13u && string_key.try_to_int (&int_val)) {
    return p->get_value (int_val);
  }

  return p->get_value (string_key.hash(), string_key);
}

template <class T, class TT>
const T array <T, TT>::get_value (const var &v) const {
  switch (v.type) {
    case var::NULL_TYPE:
      return get_value (string());
    case var::BOOLEAN_TYPE:
      return get_value (v.b);
    case var::INTEGER_TYPE:
      return get_value (v.i);
    case var::FLOAT_TYPE:
      return get_value ((int)v.f);
    case var::STRING_TYPE:
      return get_value (*STRING(v.s));
    case var::ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return get_value (ARRAY(v.a)->to_int());
    case var::OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return get_value (1);
    default:
      php_assert (0);
      exit (1);
  }
}

template <class T, class TT>
const T array <T, TT>::get_value (const const_iterator &it) const {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    return p->get_value (key);
  } else {
    const string_hash_entry *entry = (const string_hash_entry *)it.entry;

    if (it.self->is_string_hash_entry (entry)) {
      return p->get_value (entry->int_key, entry->string_key);
    } else {
      return p->get_value (entry->int_key);
    }
  }
}

template <class T, class TT>
const T array <T, TT>::get_value (const iterator &it) const {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    return p->get_value (key);
  } else {
    const string_hash_entry *entry = (const string_hash_entry *)it.entry;

    if (it.self->is_string_hash_entry (entry)) {
      return p->get_value (entry->int_key, entry->string_key);
    } else {
      return p->get_value (entry->int_key);
    }
  }
}


template <class T, class TT>
bool array <T, TT>::has_key (int int_key) const {
  return p->has_key (int_key);
}

template <class T, class TT>
bool array <T, TT>::has_key (const string &string_key) const {
  int int_val;
  if ((unsigned int)(string_key[0] - '-') < 13u && string_key.try_to_int (&int_val)) {
    return p->has_key (int_val);
  }

  return p->has_key (string_key.hash(), string_key);
}

template <class T, class TT>
bool array <T, TT>::has_key (const var &v) const {
  switch (v.type) {
    case var::NULL_TYPE:
      return has_key (string());
    case var::BOOLEAN_TYPE:
      return has_key (v.b);
    case var::INTEGER_TYPE:
      return has_key (v.i);
    case var::FLOAT_TYPE:
      return has_key ((int)v.f);
    case var::STRING_TYPE:
      return has_key (*STRING(v.s));
    case var::ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return has_key (ARRAY(v.a)->to_int());
    case var::OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return has_key (1);
    default:
      php_assert (0);
      exit (1);
  }
}

template <class T, class TT>
bool array <T, TT>::has_key (const const_iterator &it) const {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    return p->has_key (key);
  } else {
    const string_hash_entry *entry = (const string_hash_entry *)it.entry;

    if (it.self->is_string_hash_entry (entry)) {
      return p->has_key (entry->int_key, entry->string_key);
    } else {
      return p->has_key (entry->int_key);
    }
  }
}

template <class T, class TT>
bool array <T, TT>::has_key (const iterator &it) const {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    return p->has_key (key);
  } else {
    const string_hash_entry *entry = (const string_hash_entry *)it.entry;

    if (it.self->is_string_hash_entry (entry)) {
      return p->has_key (entry->int_key, entry->string_key);
    } else {
      return p->has_key (entry->int_key);
    }
  }
}

template <class T, class TT>
bool array <T, TT>::isset (int int_key) const {
  return p->isset_value (int_key);
}

template <class T, class TT>
bool array <T, TT>::isset (const string &string_key) const {
  int int_val;
  if ((unsigned int)(string_key[0] - '-') < 13u && string_key.try_to_int (&int_val)) {
    return p->isset_value (int_val);
  }

  return p->isset_value (string_key.hash(), string_key);
}

template <class T, class TT>
bool array <T, TT>::isset (const var &v) const {
  switch (v.type) {
    case var::NULL_TYPE:
      return isset (string());
    case var::BOOLEAN_TYPE:
      return isset (v.b);
    case var::INTEGER_TYPE:
      return isset (v.i);
    case var::FLOAT_TYPE:
      return isset ((int)v.f);
    case var::STRING_TYPE:
      return isset (*STRING(v.s));
    case var::ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return isset (ARRAY(v.a)->to_int());
    case var::OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return isset (1);
    default:
      php_assert (0);
      exit (1);
  }
}

template <class T, class TT>
bool array <T, TT>::isset (const const_iterator &it) const {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    return p->isset_value (key);
  } else {
    const string_hash_entry *entry = (const string_hash_entry *)it.entry;

    if (it.self->is_string_hash_entry (entry)) {
      return p->isset_value (entry->int_key, entry->string_key);
    } else {
      return p->isset_value (entry->int_key);
    }
  }
}

template <class T, class TT>
bool array <T, TT>::isset (const iterator &it) const {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    return p->isset_value (key);
  } else {
    const string_hash_entry *entry = (const string_hash_entry *)it.entry;

    if (it.self->is_string_hash_entry (entry)) {
      return p->isset_value (entry->int_key, entry->string_key);
    } else {
      return p->isset_value (entry->int_key);
    }
  }
}


template <class T, class TT>
void array <T, TT>::unset (int int_key) {
  if (is_vector()) {
    if ((unsigned int)int_key >= (unsigned int)p->int_size) {
      return;
    }
    if (int_key == p->max_key) {
      mutate_if_needed();
      return p->unset_vector_value();
    }
    convert_to_map();
  } else {
    mutate_if_needed();
  }

  return p->unset_map_value (int_key);
}

template <class T, class TT>
void array <T, TT>::unset (const string &string_key) {
  int int_val;
  if ((unsigned int)(string_key[0] - '-') < 13u && string_key.try_to_int (&int_val)) {
    return unset (int_val);
  }

  if (is_vector()) {
    return;
  }

  mutate_if_needed();
  return p->unset_map_value (string_key.hash(), string_key);
}

template <class T, class TT>
void array <T, TT>::unset (const var &v) {
  switch (v.type) {
    case var::NULL_TYPE:
      return unset (string());
    case var::BOOLEAN_TYPE:
      return unset (v.b);
    case var::INTEGER_TYPE:
      return unset (v.i);
    case var::FLOAT_TYPE:
      return unset ((int)v.f);
    case var::STRING_TYPE:
      return unset (*STRING(v.s));
    case var::ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return unset (ARRAY(v.a)->to_int());
    case var::OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return unset (1);
    default:
      php_assert (0);
      exit (1);
  }
}

template <class T, class TT>
void array <T, TT>::unset (const const_iterator &it) {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    if (is_vector()) {
      if ((unsigned int)key >= (unsigned int)p->int_size) {
        return;
      }

      if (key == p->max_key) {
        mutate_if_needed();
        return p->unset_vector_value();
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    return p->unset_map_value (key);
  } else {
    string_hash_entry *entry = (string_hash_entry *)it.entry;
    bool is_string_entry = it.self->is_string_hash_entry (entry);

    if (is_vector()) {
      if (is_string_entry || (unsigned int)entry->int_key >= (unsigned int)p->int_size) {
        return;
      }

      if (entry->int_key == p->max_key) {
        mutate_if_needed();
        return p->unset_vector_value();
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    if (is_string_entry) {
      return p->unset_map_value (entry->int_key, entry->string_key);
    } else {
      return p->unset_map_value (entry->int_key);
    }
  }
}

template <class T, class TT>
void array <T, TT>::unset (const iterator &it) {
  if (it.self->is_vector()) {
    int key = (int)((TT *)it.entry - (TT *)it.self->int_entries);

    if (is_vector()) {
      if ((unsigned int)key >= (unsigned int)p->int_size) {
        return;
      }

      if (key == p->max_key) {
        mutate_if_needed();
        return p->unset_vector_value();
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    return p->unset_map_value (key);
  } else {
    string_hash_entry *entry = (string_hash_entry *)it.entry;
    bool is_string_entry = it.self->is_string_hash_entry (entry);

    if (is_vector()) {
      if (is_string_entry || (unsigned int)entry->int_key >= (unsigned int)p->int_size) {
        return;
      }

      if (entry->int_key == p->max_key) {
        mutate_if_needed();
        return p->unset_vector_value();
      }

      convert_to_map();
    } else {
      mutate_if_needed();
    }

    if (is_string_entry) {
      return p->unset_map_value (entry->int_key, entry->string_key);
    } else {
      return p->unset_map_value (entry->int_key);
    }
  }
}


template <class T, class TT>
bool array <T, TT>::empty (void) const {
  return p->int_size + p->string_size == 0;
}

template <class T, class TT>
int array <T, TT>::count (void) const {
  return p->int_size + p->string_size;
}

template <class T, class TT>
array_size array <T, TT>::size (void) const {
  return array_size (p->int_size, p->string_size, is_vector());
}

template <class T, class TT>
template <class T1, class TT1>
void array <T, TT>::merge_with (const array <T1, TT1> &other) {
  for (typename array <T1, TT1>::const_iterator it = other.begin(); it != other.end(); ++it) {
    if (it.self->is_vector()) {//TODO move if outside for
      mutate_if_needed();

      if (is_vector()) {
        p->push_back_vector_value (force_convert_to <T1>::convert (*(TT1 *)it.entry));
      } else {
        p->set_map_value (get_next_key(), force_convert_to <T1>::convert (*(TT1 *)it.entry), false);
      }
    } else {
      const typename array <T1, TT1>::string_hash_entry *entry = (const typename array <T1, TT1>::string_hash_entry *)it.entry;
      const T1 &value = force_convert_to <T1>::convert (entry->value);

      if (it.self->is_string_hash_entry (entry)) {
        if (is_vector()) {
          convert_to_map();
        } else {
          mutate_if_needed();
        }

        p->set_map_value (entry->int_key, entry->string_key, value, false);
      } else {
        mutate_if_needed();

        if (is_vector()) {
          p->push_back_vector_value (value);
        } else {
          p->set_map_value (get_next_key(), value, false);
        }
      }
    }
  }
}

template <class T, class TT>
const array <T, TT> array <T, TT>::operator + (const array <T, TT> &other) const {
  array <T, TT> result (size() + other.size());

  if (is_vector()) {
    int size = p->int_size;
    TT *it = (TT *)p->int_entries;

    if (result.is_vector()) {
      for (int i = 0; i < size; i++) {
        result.p->push_back_vector_value (it[i]);
      }
    } else {
      for (int i = 0; i < size; i++) {
        result.p->set_map_value (i, it[i], false);
      }
    }
  } else {
    for (const string_hash_entry *it = p->begin(); it != p->end(); it = p->next (it)) {
      if (p->is_string_hash_entry (it)) {
        result.p->set_map_value (it->int_key, it->string_key, it->value, false);
      } else {
        result.p->set_map_value (it->int_key, it->value, false);
      }
    }
  }

  if (other.is_vector()) {
    int size = other.p->int_size;
    TT *it = (TT *)other.p->int_entries;

    if (result.is_vector()) {
      for (int i = p->int_size; i < size; i++) {
        result.p->push_back_vector_value (it[i]);
      }
    } else {
      for (int i = 0; i < size; i++) {
        result.p->set_map_value (i, it[i], true);
      }
    }
  } else {
    for (const string_hash_entry *it = other.p->begin(); it != other.p->end(); it = other.p->next (it)) {
      if (other.p->is_string_hash_entry (it)) {
        result.p->set_map_value (it->int_key, it->string_key, it->value, true);
      } else {
        result.p->set_map_value (it->int_key, it->value, true);
      }
    }
  }

  return result;
}

template <class T, class TT>
array <T, TT>& array <T, TT>::operator += (const array <T, TT> &other) {
  if (is_vector()) {
    if (other.is_vector()) {
      int size = other.p->int_size;
      TT *it = (TT *)other.p->int_entries;

      if (p->int_buf_size < size + 2) {
        int new_size = max (size + 2, p->int_buf_size * 2);
        p = (array_inner *)dl::reallocate ((void *)p,
                                           (dl::size_type)(sizeof (array_inner) + new_size * sizeof (TT)),
                                           (dl::size_type)(sizeof (array_inner) + p->int_buf_size * sizeof (TT)));
        p->int_buf_size = new_size;
      }

      for (int i = p->int_size; i < size; i++) {
        p->push_back_vector_value (it[i]);
      }

      return *this;
    } else {
      array_inner *new_array = array_inner::create (p->int_size + other.p->int_size + 4, other.p->string_size + 4, false);
      TT *it = (TT *)p->int_entries;

      for (int i = 0; i != p->int_size; i++) {
        new_array->set_map_value (i, it[i], false);
      }

      p->dispose();
      p = new_array;
    }
  } else {
    if (p == other.p) {
      return *this;
    }

    int new_int_size = p->int_size + other.p->int_size;
    int new_string_size = p->string_size + other.p->string_size;

    if (new_int_size * 5 > 3 * p->int_buf_size || new_string_size * 5 > 3 * p->string_buf_size ||
        (p->ref_cnt > 0 || dl::memory_begin > (size_t)p || (size_t)p >= dl::memory_end)) {
      array_inner *new_array = array_inner::create (max (new_int_size, 2 * p->int_size) + 1, max (new_string_size, 2 * p->string_size) + 1, false);

      for (const string_hash_entry *it = p->begin(); it != p->end(); it = p->next (it)) {
        if (p->is_string_hash_entry (it)) {
          new_array->set_map_value (it->int_key, it->string_key, it->value, false);
        } else {
          new_array->set_map_value (it->int_key, it->value, false);
        }
      }

      p->dispose();
      p = new_array;
    }
  }

  if (other.is_vector()) {
    int size = other.p->int_size;
    TT *it = (TT *)other.p->int_entries;

    for (int i = 0; i < size; i++) {
      p->set_map_value (i, it[i], true);
    }
  } else {
    for (string_hash_entry *it = other.p->begin(); it != other.p->end(); it = other.p->next (it)) {
      if (other.p->is_string_hash_entry (it)) {
        p->set_map_value (it->int_key, it->string_key, it->value, true);
      } else {
        p->set_map_value (it->int_key, it->value, true);
      }
    }
  }

  return *this;
}


template <class T, class TT>
void array <T, TT>::push_back (const T &v) {
  mutate_if_needed();

  if (is_vector()) {
    p->push_back_vector_value (v);
  } else {
    p->set_map_value (get_next_key(), v, false);
  }
}

template <class T, class TT>
void array <T, TT>::push_back (const const_iterator &it) {
  if (it.self->is_vector()) {
    mutate_if_needed();

    if (is_vector()) {
      p->push_back_vector_value (*(TT *)it.entry);
    } else {
      p->set_map_value (get_next_key(), *(TT *)it.entry, false);
    }
  } else {
    string_hash_entry *entry = (string_hash_entry *)it.entry;

    if (it.self->is_string_hash_entry (entry)) {
      if (is_vector()) {
        convert_to_map();
      } else {
        mutate_if_needed();
      }

      p->set_map_value (entry->int_key, entry->string_key, entry->value, false);
    } else {
      mutate_if_needed();

      if (is_vector()) {
        p->push_back_vector_value (entry->value);
      } else {
        p->set_map_value (get_next_key(), entry->value, false);
      }
    }
  }
}

template <class T, class TT>
void array <T, TT>::push_back (const iterator &it) {
  if (it.self->is_vector()) {
    mutate_if_needed();

    if (is_vector()) {
      p->push_back_vector_value (*(TT *)it.entry);
    } else {
      p->set_map_value (get_next_key(), *(TT *)it.entry, false);
    }
  } else {
    string_hash_entry *entry = (string_hash_entry *)it.entry;

    if (it.self->is_string_hash_entry (entry)) {
      if (is_vector()) {
        convert_to_map();
      } else {
        mutate_if_needed();
      }

      p->set_map_value (entry->int_key, entry->string_key, entry->value, false);
    } else {
      mutate_if_needed();

      if (is_vector()) {
        p->push_back_vector_value (entry->value);
      } else {
        p->set_map_value (get_next_key(), entry->value, false);
      }
    }
  }
}

template <class T, class TT>
const T array <T, TT>::push_back_return (const T &v) {
  mutate_if_needed();

  if (is_vector()) {
    return p->push_back_vector_value (v);
  } else {
    return p->set_map_value (get_next_key(), v, false);
  }
}


template <class T, class TT>
int array <T, TT>::get_next_key (void) const {
  return p->max_key + 1;
}

template <class T, class TT>
template <class T1>
array <T, TT>::compare_list_entry_by_value <T1>::compare_list_entry_by_value (const T1 &comp): comp (comp) {
}

template <class T, class TT>
template <class T1>
array <T, TT>::compare_list_entry_by_value <T1>::compare_list_entry_by_value (const compare_list_entry_by_value <T1> &comp): comp (comp.comp) {
}

template <class T, class TT>
template <class T1>
bool array <T, TT>::compare_list_entry_by_value <T1>::operator () (const typename array <T, TT>::int_hash_entry *lhs, const typename array <T, TT>::int_hash_entry *rhs) const {
  return comp (force_convert_to <T>::convert (lhs->value), force_convert_to <T>::convert (rhs->value)) > 0;
}

template <class T, class TT>
template <class T1>
array <T, TT>::compare_TT_by_T <T1>::compare_TT_by_T (const T1 &comp): comp (comp) {
}

template <class T, class TT>
template <class T1>
array <T, TT>::compare_TT_by_T <T1>::compare_TT_by_T (const compare_TT_by_T <T1> &comp): comp (comp.comp) {
}

template <class T, class TT>
template <class T1>
bool array <T, TT>::compare_TT_by_T <T1>::operator () (const TT &lhs, const TT &rhs) const {
  return comp (force_convert_to <T>::convert (lhs), force_convert_to <T>::convert (rhs)) > 0;
}

template <class T, class TT>
template <class T1>
void array <T, TT>::sort (const T1 &compare, bool renumber) {
  int n = count();

  if (renumber) {
    if (n == 0) {
      return;
    }

    if (!is_vector()) {
      array_inner *res = array_inner::create (n, 0, true);
      for (string_hash_entry *it = p->begin(); it != p->end(); it = p->next (it)) {
        res->push_back_vector_value (it->value);
      }

      p->dispose();
      p = res;
    } else {
      mutate_if_needed();
    }

    dl::sort <T, TT, compare_TT_by_T <T1> > ((TT *)p->int_entries, (TT *)p->int_entries + n, compare_TT_by_T <T1> (compare));
    return;
  }

  if (n <= 1) {
    return;
  }

  if (is_vector()) {
    convert_to_map();
  } else {
    mutate_if_needed();
  }

  int_hash_entry **arTmp = (int_hash_entry **) dl::allocate ((dl::size_type)(n * sizeof (int_hash_entry *)));
  int i = 0;
  for (string_hash_entry *it = p->begin(); it != p->end(); it = p->next (it)) {
    arTmp[i++] = (int_hash_entry *)it;
  }
  php_assert (i == n);

  dl::sort <int_hash_entry *, int_hash_entry *, compare_list_entry_by_value <T1> > (arTmp, arTmp + n, compare_list_entry_by_value <T1> (compare));

  arTmp[0]->prev = p->get_pointer (p->end());
  p->end()->next = p->get_pointer (arTmp[0]);
  for (int j = 1; j < n; j++) {
    arTmp[j]->prev = p->get_pointer (arTmp[j - 1]);
    arTmp[j - 1]->next = p->get_pointer (arTmp[j]);
  }
  arTmp[n - 1]->next = p->get_pointer (p->end());
  p->end()->prev = p->get_pointer (arTmp[n - 1]);

  dl::deallocate (arTmp, (dl::size_type)(n * sizeof (int_hash_entry *)));
}


template <class T, class TT>
template <class T1>
void array <T, TT>::ksort (const T1 &compare) {
  int n = count();
  if (n <= 1) {
    return;
  }

  if (is_vector()) {
    convert_to_map();
  } else {
    mutate_if_needed();
  }

  array <key_type> keys (array_size (n, 0, true));
  for (string_hash_entry *it = p->begin(); it != p->end(); it = p->next (it)) {
    if (p->is_string_hash_entry (it)) {
      keys.p->push_back_vector_value (it->get_key());
    } else {
      keys.p->push_back_vector_value (((int_hash_entry *)it)->get_key());
    }
  }

  key_type *keysp = (key_type *)keys.p->int_entries;
  dl::sort <key_type, key_type, T1> (keysp, keysp + n, compare);

  list_hash_entry *prev = (list_hash_entry *)p->end();
  for (int j = 0; j < n; j++) {
    list_hash_entry *cur;
    if (is_int_key (keysp[j])) {
      int int_key = keysp[j].to_int();
      int bucket = array_inner::choose_bucket (int_key, p->int_buf_size);
      while (p->int_entries[bucket].int_key != int_key) {
        if (unlikely (++bucket == p->int_buf_size)) {
          bucket = 0;
        }
      }
      cur = (list_hash_entry *)&p->int_entries[bucket];
    } else {
      string string_key = keysp[j].to_string();
      int int_key = string_key.hash();
      string_hash_entry *string_entries = p->get_string_entries();
      int bucket = array_inner::choose_bucket (int_key, p->string_buf_size);
      while ((string_entries[bucket].int_key != int_key || string_entries[bucket].string_key != string_key)) {
        if (unlikely (++bucket == p->string_buf_size)) {
          bucket = 0;
        }
      }
      cur = (list_hash_entry *)&string_entries[bucket];
    }

    cur->prev = p->get_pointer (prev);
    prev->next = p->get_pointer (cur);

    prev = cur;
  }
  prev->next = p->get_pointer (p->end());
  p->end()->prev = p->get_pointer (prev);
}


template <class T, class TT>
void array <T, TT>::swap (array <T, TT> &other) {
  array_inner *tmp = p;
  p = other.p;
  other.p = tmp;
}

template <class T, class TT>
T array <T, TT>::pop (void) {
  if (empty()) {
//    php_warning ("Cannot use function array_pop on empty array");
    return array_inner::empty_T;
  }

  mutate_if_needed();

  if (is_vector()) {
    TT *it = (TT *)p->int_entries;
    T result = force_convert_to <T>::convert (it[p->max_key]);

    p->unset_vector_value();

    return result;
  } else {
    string_hash_entry *it = p->prev (p->end());
    T result = force_convert_to <T>::convert (it->value);

    if (p->is_string_hash_entry (it)) {
      p->unset_map_value (it->int_key, it->string_key);
    } else {
      p->unset_map_value (it->int_key);
    }

    return result;
  }
}

template <class T, class TT>
T array <T, TT>::shift (void) {
  if (count() == 0) {
    php_warning ("Cannot use array_shift on empty array");
    return array_inner::empty_T;
  }

  if (is_vector()) {
    mutate_if_needed();

    TT *it = (TT *)p->int_entries;
    T res = force_convert_to <T>::convert (*it);

    it->~TT();
    memmove (it, it + 1, --p->int_size * sizeof (TT));
    p->max_key--;

    return res;
  } else {
    array_size new_size = size().cut (count() - 1);
    bool is_v = (new_size.string_size == 0);

    array_inner *new_array = array_inner::create (new_size.int_size, new_size.string_size, is_v);
    string_hash_entry *it = p->begin();
    T res = force_convert_to <T>::convert (it->value);

    it = p->next (it);
    while (it != p->end()) {
      if (p->is_string_hash_entry (it)) {
        new_array->set_map_value (it->int_key, it->string_key, it->value, false);
      } else {
        if (is_v) {
          new_array->push_back_vector_value (it->value);
        } else {
          new_array->set_map_value (new_array->max_key + 1, it->value, false);
        }
      }

      it = p->next (it);
    }

    p->dispose();
    p = new_array;

    return res;
  }
}

template <class T, class TT>
int array <T, TT>::unshift (const T &val) {
  mutate_if_needed();

  if (is_vector()) {
    TT *it = (TT *)p->int_entries;
    memmove (it + 1, it, p->int_size++ * sizeof (TT));
    p->max_key++;
    new (it) TT (val);
  } else {
    array_size new_size = size();
    bool is_v = (new_size.string_size == 0);

    array_inner *new_array = array_inner::create (new_size.int_size + 1, new_size.string_size, is_v);
    string_hash_entry *it = p->begin();

    if (is_v) {
      new_array->push_back_vector_value (val);
    } else {
      new_array->set_map_value (0, val, false);
    }

    while (it != p->end()) {
      if (p->is_string_hash_entry (it)) {
        new_array->set_map_value (it->int_key, it->string_key, it->value, false);
      } else {
        if (is_v) {
          new_array->push_back_vector_value (it->value);
        } else {
          new_array->set_map_value (new_array->max_key + 1, it->value, false);
        }
      }

      it = p->next (it);
    }

    p->dispose();
    p = new_array;
  }

  return count();
}


template <class T, class TT>
bool array <T, TT>::to_bool (void) const {
  return (bool)(p->int_size + p->string_size);
}

template <class T, class TT>
int array <T, TT>::to_int (void) const {
  return p->int_size + p->string_size;
}

template <class T, class TT>
double array <T, TT>::to_float (void) const {
  return p->int_size + p->string_size;
}

template <class T, class TT>
const object array <T, TT>::to_object (void) const {
  object res;
  array <var, var>::array_inner *data = res.o->data;
  for (string_hash_entry *it = p->begin(); it != p->end(); it = p->next (it)) {
    if (p->is_string_hash_entry (it)) {
      data->set_map_value (it->int_key, it->string_key, it->value, false);
    } else {
      string string_key (it->int_key);
      data->set_map_value (string_key.hash(), string_key, it->value, false);
    }
  }
  return res;
}


template <class T, class TT>
int array <T, TT>::get_reference_counter (void) const {
  return p->ref_cnt + 1;
}


template <class T, class TT>
void swap (array <T, TT> &lhs, array <T, TT> &rhs) {
  lhs.swap (rhs);
}

template <class T>
const array <T> array_add (array <T> a1, const array <T> &a2) {
  return a1 += a2;
}


template <class T, class TT>
bool eq2 (const array <T, TT> &lhs, const array <T, TT> &rhs) {
  if (rhs.count() != lhs.count()) {
    return false;
  }

  for (typename array <T, TT>::const_iterator rhs_it = rhs.begin(); rhs_it != rhs.end(); ++rhs_it) {
    if (!lhs.has_key (rhs_it) || !eq2 (lhs.get_value (rhs_it), rhs_it.get_value())) {
      return false;
    }
  }

  return true;
}

template <class T1, class TT1, class T2, class TT2>
bool eq2 (const array <T1, TT1> &lhs, const array <T2, TT2> &rhs) {
  if (rhs.count() != lhs.count()) {
    return false;
  }

  for (typename array <T2, TT2>::const_iterator rhs_it = rhs.begin(); rhs_it != rhs.end(); ++rhs_it) {
    typename array <T2, TT2>::key_type key = rhs_it.get_key();
    if (!lhs.has_key (key) || !eq2 (lhs.get_value (key), rhs_it.get_value())) {
      return false;
    }
  }

  return true;
}


template <class T, class TT>
bool equals (const array <T, TT> &lhs, const array <T, TT> &rhs) {
  if (lhs.count() != rhs.count()) {
    return false;
  }

  for (typename array <T, TT>::const_iterator lhs_it = lhs.begin(), rhs_it = rhs.begin(); lhs_it != lhs.end(); ++lhs_it, ++rhs_it) {
    if (!equals (lhs_it.get_key(), rhs_it.get_key()) || !equals (lhs_it.get_value(), rhs_it.get_value())) {
      return false;
    }
  }

  return true;
}

template <class T1, class TT1, class T2, class TT2>
bool equals (const array <T1, TT1> &lhs, const array <T2, TT2> &rhs) {
  if (lhs.count() != rhs.count()) {
    return false;
  }

  typename array <T2, TT2>::const_iterator rhs_it = rhs.begin();
  for (typename array <T1, TT1>::const_iterator lhs_it = lhs.begin(); lhs_it != lhs.end(); ++lhs_it, ++rhs_it) {
    if (!equals (lhs_it.get_key(), rhs_it.get_key()) || !equals (lhs_it.get_value(), rhs_it.get_value())) {
      return false;
    }
  }

  return true;
}


