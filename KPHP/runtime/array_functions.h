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
#include "string_functions.h"

template <class T>
string f$implode (const string &s, const array <T> &a);

array <string> explode (char delimiter, const string &str, int limit = INT_MAX);

array <string> f$explode (const string &delimiter, const string &str, int limit = INT_MAX);


template <class T>
array < array <T> > f$array_chunk (const array <T> &a, int chunk_size, bool preserve_keys = false);

template <class T>
array <T> f$array_slice (const array <T> &a, int offset, int length = INT_MAX, bool preserve_keys = false);

template <class T>
array <T> f$array_splice (array <T> &a, int offset, int length = INT_MAX);

template <class T, class T1>
array <T> f$array_splice (array <T> &a, int offset, int length = INT_MAX, const array <T1> &replacement = array <var> ());

template <class T>
array <T> f$array_filter (const array <T> &a);

template <class T, class T1>
array <T> f$array_filter (const array <T> &a, const T1 callback);

template <class T, class T1>
array <var> f$array_map (const T1 callback, const array <T> &a);

template <class T>
T f$array_merge (const T &a1);

template <class T>
T f$array_merge (const T &a1, const T &a2);

template <class T>
T f$array_merge (const T &a1, const T &a2, const T &a3, const T &a4  = T(), const T &a5  = T(), const T &a6  = T(),
                                                        const T &a7  = T(), const T &a8  = T(), const T &a9  = T(),
                                                        const T &a10 = T(), const T &a11 = T(), const T &a12 = T());


template <class T, class T1>
array <T> f$array_intersect_key (const array <T> &a1, const array <T1> &a2);

template <class T, class T1>
array <T> f$array_intersect (const array <T> &a1, const array <T1> &a2);

template <class T, class T1>
array <T> f$array_diff_key (const array <T> &a1, const array <T1> &a2);

template <class T, class T1>
array <T> f$array_diff (const array <T> &a1, const array <T1> &a2);

template <class T, class T1, class T2>
array <T> f$array_diff (const array <T> &a1, const array <T1> &a2, const array <T2> &a3);

template <class T>
array <T> f$array_reverse (const array <T> &a, bool preserve_keys = false);


template <class T>
T f$array_shift (array <T> &a);

template <class T, class T1>
int f$array_unshift (array <T> &a, const T1 &val);


template <class T>
bool f$array_key_exists (int int_key, const array <T> &a);

template <class T>
bool f$array_key_exists (const string &string_key, const array <T> &a);

template <class T>
bool f$array_key_exists (const var &v, const array <T> &a);


template <class T, class T1>
typename array <T>::key_type f$array_search (const T1 &val, const array <T> &a);

template <class T>
typename array <T>::key_type f$array_rand (const array <T> &a);

template <class T>
var f$array_rand (const array <T> &a, int num);


template <class T>
array <typename array <T>::key_type> f$array_keys (const array <T> &a);

template <class T>
array <T> f$array_values (const array <T> &a);

template <class T>
array <T> f$array_unique (const array <T> &a);

template <class T>
array <int> f$array_count_values (const array <T> &a);

array <array <var>::key_type> f$array_flip (const array <var> &a);

array <array <var>::key_type> f$array_flip (const array <int> &a);

array <array <var>::key_type> f$array_flip (const array <string> &a);

template <class T>
array <typename array <T>::key_type> f$array_flip (const array <T> &a);

template <class T, class T1>
bool f$in_array (const T1 &value, const array <T> &a, bool strict = false);


template <class T>
array <T> f$array_fill (int start_index, int num, const T &value);

template <class T>
array <T> f$array_fill_keys (const array <int> &a, const T &value);

template <class T>
array <T> f$array_fill_keys (const array <string> &a, const T &value);

template <class T>
array <T> f$array_fill_keys (const array <var> &a, const T &value);

template <class T1, class T>
array <T> f$array_fill_keys (const array <T1> &a, const T &value);

template <class T>
array <T> f$array_combine (const array <int> &keys, const array <T> &values);

template <class T>
array <T> f$array_combine (const array <string> &keys, const array <T> &values);

template <class T>
array <T> f$array_combine (const array <var> &keys, const array <T> &values);

template <class T1, class T>
array <T> f$array_combine (const array <T1> &keys, const array <T> &values);

template <class T1, class T2>
int f$array_push (array <T1> &a, const T2 &val);

template <class T1, class T2, class T3>
int f$array_push (array <T1> &a, const T2 &val2, const T3 &val3);

template <class T1, class T2, class T3, class T4>
int f$array_push (array <T1> &a, const T2 &val2, const T3 &val3, const T4 &val4);

template <class T1, class T2, class T3, class T4, class T5>
int f$array_push (array <T1> &a, const T2 &val2, const T3 &val3, const T4 &val4, const T5 &val5);

template <class T1, class T2, class T3, class T4, class T5, class T6>
int f$array_push (array <T1> &a, const T2 &val2, const T3 &val3, const T4 &val4, const T5 &val5, const T6 &val6);

template <class T>
T f$array_pop (array <T> &a);


array <int> f$range (int from, int to);

array <string> f$range (const string &from_s, const string &to_s);

template <class T, class T1>
array <var> f$range (const T &from, const T1 &to);


template <class T>
void f$shuffle (array <T> &a);

const int SORT_REGULAR = 0;
const int SORT_NUMERIC = 1;
const int SORT_STRING = 2;

template <class T>
void f$sort (array <T> &a, int flag = SORT_REGULAR);

template <class T>
void f$rsort (array <T> &a, int flag = SORT_REGULAR);

template <class T, class T1>
void f$usort (array <T> &a, const T1 &compare);

template <class T>
void f$asort (array <T> &a, int flag = SORT_REGULAR);

template <class T>
void f$arsort (array <T> &a, int flag = SORT_REGULAR);

template <class T, class T1>
void f$uasort (array <T> &a, const T1 &compare);

template <class T>
void f$ksort (array <T> &a, int flag = SORT_REGULAR);

template <class T>
void f$krsort (array <T> &a, int flag = SORT_REGULAR);

template <class T, class T1>
void f$uksort (array <T> &a, const T1 &compare);


int f$array_sum (const array <int> &a);

double f$array_sum (const array <double> &a);

double f$array_sum (const array <var> &a);

template <class T>
double f$array_sum (const array <T> &a);


template <class T>
var f$getKeyByPos (const array <T> &a, int pos);

template <class T>
T f$getValueByPos (const array <T> &a, int pos);

template <class T>
inline array <T> f$create_vector (int n, const T &default_value);

inline array <var> f$create_vector (int n);


/*
 *
 *     IMPLEMENTATION
 *
 */


template <class T>
string f$implode (const string &s, const array <T> &a) {
  string_buffer &SB = static_SB;
  SB.clean();

  typename array <T>::const_iterator it = a.begin(), it_end = a.end();
  if (it != it_end) {
    SB += it.get_value();
    ++it;
  }
  while (it != it_end) {
    SB += s;
    SB += it.get_value();
    ++it;
  }

  return SB.str();
}


template <class T>
array <array <T> > f$array_chunk (const array <T> &a, int chunk_size, bool preserve_keys = false) {
  if (chunk_size <= 0) {
    php_warning ("Parameter chunk_size if function array_chunk must be positive");
    return array <array <T> > ();
  }
  array <array <T> > result (array_size (a.count() / chunk_size + 1, 0, true));

  array_size new_size = a.size().cut (chunk_size);
  if (!preserve_keys) {
    new_size.int_size = min (chunk_size, a.count());
    new_size.string_size = 0;
    new_size.is_vector = true;
  }

  array <T> res (new_size);
  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    if (res.count() == chunk_size) {
      result.push_back (res);
      res = array <T> (new_size);
    }

    if (preserve_keys) {
      res.set_value (it);
    } else {
      res.push_back (it.get_value());
    }
  }

  if (res.count()) {
    result.push_back (res);
  }

  return result;
}

template <class T>
array <T> f$array_slice (const array <T> &a, int offset, int length = INT_MAX, bool preserve_keys = false) {
  if (length == 0) {
    length = INT_MAX;
  }

  int size = a.count();
  if (offset < 0) {
    offset += size;
  }
  if (offset > size) {
    offset = size;
  }
  if (length < 0) {
    length = size - offset + length;
  }
  if (length <= 0) {
    return array <T> ();
  }
  if (offset < 0) {
    length += offset;
    offset = 0;
  }
  if (size - offset < length) {
    length = size - offset;
  }

  array_size result_size = a.size().cut (length);
  result_size.is_vector = (!preserve_keys && result_size.string_size == 0) || (preserve_keys && offset == 0 && a.is_vector());

  array <T> result (result_size);
  typename array <T>::const_iterator it = a.middle (offset);
  while (length-- > 0) {
    if (preserve_keys) {
      result.set_value (it);
    } else {
      result.push_back (it);
    }
    ++it;
  }

  return result;
}

template <class T>
array <T> f$array_splice (array <T> &a, int offset, int length) {
  return f$array_splice (a, offset, length, array <T>());
}

template <class T, class T1>
array <T> f$array_splice (array <T> &a, int offset, int length, const array <T1> &replacement) {
  int size = a.count();
  if (offset < 0) {
    offset += size;
  }
  if (offset > size) {
    offset = size;
  }
  if (length < 0) {
    length = size - offset + length;
  }
  if (length <= 0) {
    length = 0;
  }
  if (offset < 0) {
    length += offset;
    offset = 0;
  }
  if (size - offset < length) {
    length = size - offset;
  }

  if (offset == size) {
    a.merge_with (replacement);
    return array <T> ();
  }

  array <T> result (a.size().cut (length));
  array <T> new_a (a.size().cut (size - length) + replacement.size());
  int i = 0;
  for (typename array <T>::iterator it = a.begin(); it != a.end(); ++it, i++) {
    if (i == offset) {
      for (typename array <T1>::const_iterator it_r = replacement.begin(); it_r != replacement.end(); ++it_r) {
        new_a.push_back (it_r.get_value());
      }
    }
    if (i < offset || i >= offset + length) {
      new_a.push_back (it);
    } else {
      result.push_back (it);
    }
  }
  a = new_a;

  return result;
}

template <class T>
array <T> f$array_filter (const array <T> &a) {
  array <T> result (a.size());
  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    if (f$boolval (it.get_value())) {
      result.set_value (it);
    }
  }

  return result;
}

template <class T, class T1>
array <T> f$array_filter (const array <T> &a, const T1 callback) {
  array <T> result (a.size());
  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    if (f$boolval (callback (it.get_value()))) {
      result.set_value (it);
    }
  }

  return result;
}


template <class T, class T1>
array <var> f$array_map (const T1 callback, const array <T> &a) {
  php_assert (callback != NULL);

  array <var> result (a.size());
  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    result.set_value (it.get_key(), callback (it.get_value()));
  }

  return result;
}

template <class T>
T f$array_merge (const T &a1) {
  T result (a1.size());
  result.merge_with (a1);
  return result;
}

template <class T>
T f$array_merge (const T &a1, const T &a2) {
  T result (a1.size() + a2.size());
  result.merge_with (a1);
  result.merge_with (a2);
  return result;
}

template <class T>
T f$array_merge (const T &a1, const T &a2, const T &a3, const T &a4  = T(), const T &a5  = T(), const T &a6  = T(),
                                                        const T &a7  = T(), const T &a8  = T(), const T &a9  = T(),
                                                        const T &a10 = T(), const T &a11 = T(), const T &a12 = T()) {
  T result (a1.size() + a2.size() + a3.size() + a4.size() + a5.size() + a6.size() + a7.size() + a8.size() + a9.size() + a10.size() + a11.size() + a12.size());
  result.merge_with (a1);
  result.merge_with (a2);
  result.merge_with (a3);
  result.merge_with (a4);
  result.merge_with (a5);
  result.merge_with (a6);
  result.merge_with (a7);
  result.merge_with (a8);
  result.merge_with (a9);
  result.merge_with (a10);
  result.merge_with (a11);
  result.merge_with (a12);
  return result;
}


template <class T, class T1>
array <T> f$array_intersect_key (const array <T> &a1, const array <T1> &a2) {
  array <T> result (a1.size().min (a2.size()));
  for (typename array <T>::const_iterator it = a1.begin(); it != a1.end(); ++it) {
    if (a2.has_key (it.get_key())) {
      result.set_value (it);
    }
  }
  return result;
}

template <class T, class T1>
array <T> f$array_intersect (const array <T> &a1, const array <T1> &a2) {
  array <T> result (a1.size().min (a2.size()));

  array <int> values (array_size (0, a2.count(), false));
  for (typename array <T1>::const_iterator it = a2.begin(); it != a2.end(); ++it) {
    values.set_value (f$strval (it.get_value()), 1);
  }

  for (typename array <T>::const_iterator it = a1.begin(); it != a1.end(); ++it) {
    if (values.has_key (f$strval (it.get_value()))) {
      result.set_value (it);
    }
  }
  return result;
}

template <class T, class T1>
array <T> f$array_diff_key (const array <T> &a1, const array <T1> &a2) {
  array <T> result (a1.size());
  for (typename array <T>::const_iterator it = a1.begin(); it != a1.end(); ++it) {
    if (!a2.has_key (it.get_key())) {
      result.set_value (it);
    }
  }
  return result;
}

template <class T, class T1>
array <T> f$array_diff (const array <T> &a1, const array <T1> &a2) {
  array <T> result (a1.size());

  array <int> values (array_size (0, a2.count(), false));
  for (typename array <T1>::const_iterator it = a2.begin(); it != a2.end(); ++it) {
    values.set_value (f$strval (it.get_value()), 1);
  }

  for (typename array <T>::const_iterator it = a1.begin(); it != a1.end(); ++it) {
    if (!values.has_key (f$strval (it.get_value()))) {
      result.set_value (it);
    }
  }
  return result;
}

template <class T, class T1, class T2>
array <T> f$array_diff (const array <T> &a1, const array <T1> &a2, const array <T2> &a3) {
  return f$array_diff (f$array_diff (a1, a2), a3);
}

template <class T>
array <T> f$array_reverse (const array <T> &a, bool preserve_keys) {
  array <T> result (a.size());

  for (typename array <T>::const_iterator it = a.end(); it != a.begin(); ) {
    --it;

    if (!preserve_keys) {
      result.push_back (it);
    } else {
      result.set_value (it);
    }
  }
  return result;
}


template <class T>
T f$array_shift (array <T> &a) {
  return a.shift();
}

template <class T, class T1>
int f$array_unshift (array <T> &a, const T1 &val) {
  return a.unshift (val);
}


template <class T>
bool f$array_key_exists (int int_key, const array <T> &a) {
  return a.has_key (int_key);
}

template <class T>
bool f$array_key_exists (const string &string_key, const array <T> &a) {
  return a.has_key (string_key);
}

template <class T>
bool f$array_key_exists (const var &v, const array <T> &a) {
  return a.has_key (v);
}


template <class T, class T1>
typename array <T>::key_type f$array_search (const T1 &val, const array <T> &a) {
  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    if (eq2 (it.get_value(), val)) {
      return it.get_key();
    }
  }

  return typename array <T>::key_type (false);
}

template <class T>
typename array <T>::key_type f$array_rand (const array <T> &a) {
  int size = a.count();
  if (size == 0) {
    return typename array <T>::key_type();
  }

  return a.middle (rand() % size).get_key();
}

template <class T>
var f$array_rand (const array <T> &a, int num) {
  if (num == 1) {
    return f$array_rand (a);
  }

  int size = a.count();
  if (num > size) {
    num = size;
  }
  if (num <= 0) {
    php_warning ("Parameter num of array_rand must be positive");
    return array <typename array <T>::key_type> ();
  }

  array <typename array <T>::key_type> result (array_size (num, 0, true));
  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    if (rand() % (size--) < num) {
      result.push_back (it.get_key());
      --num;
    }
  }

  return result;
}


template <class T>
array <typename array <T>::key_type> f$array_keys (const array <T> &a) {
  array <typename array <T>::key_type> result (array_size (a.count(), 0, true));

  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    result.push_back (it.get_key());
  }

  return result;
}

template <class T>
array <T> f$array_values (const array <T> &a) {
  array <T> result (array_size (a.count(), 0, true));

  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    result.push_back (it.get_value());
  }

  return result;
}

template <class T>
array <T> f$array_unique (const array <T> &a) {
  array <int> values (array_size (a.count(), a.count(), false));
  array <T> result (a.size());

  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    const T &value = it.get_value();
    int &cnt = values[f$strval (value)];
    if (!cnt) {
      cnt = 1;
      result.set_value (it);
    }
  }
  return result;
}

template <class T>
array <int> f$array_count_values (const array <T> &a) {
  array <int> result (array_size (0, a.count(), false));

  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    ++result[f$strval (it.get_value())];
  }
  return result;
}


template <class T>
array <typename array <T>::key_type> f$array_flip (const array <T> &a) {//TODO optimize
  return f$array_flip (array <var> (a));
}


template <class T, class T1>
bool f$in_array (const T1 &value, const array <T> &a, bool strict) {
  if (!strict) {
    for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
      if (eq2 (it.get_value(), value)) {
        return true;
      }
    }
  } else {
    for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
      if (equals (it.get_value(), value)) {
        return true;
      }
    }
  }
  return false;
}



template <class T>
array <T> f$array_fill (int start_index, int num, const T &value) {
  if (num <= 0) {
    php_warning ("Parameter num of array_fill must be positive");
    return array <T> ();
  }
  array <T> result (array_size (num, 0, start_index == 0));
  result.set_value (start_index, value);
  while (--num > 0) {
    result.push_back (value);
  }

  return result;
}

template <class T>
array <T> f$array_fill_keys (const array <int> &a, const T &value) {
  array <T> result (array_size (a.count(), 0, false));
  for (array <int>::const_iterator it = a.begin(); it != a.end(); ++it) {
    result.set_value (it.get_value(), value);
  }

  return result;
}

template <class T>
array <T> f$array_fill_keys (const array <string> &a, const T &value) {
  array <T> result (array_size (0, a.count(), false));
  for (array <string>::const_iterator it = a.begin(); it != a.end(); ++it) {
    result.set_value (it.get_value(), value);
  }

  return result;
}

template <class T>
array <T> f$array_fill_keys (const array <var> &a, const T &value) {
  array <T> result (array_size (a.count(), a.count(), false));
  for (array <var>::const_iterator it = a.begin(); it != a.end(); ++it) {
    const var &key = it.get_value();
    if (!key.is_int()) {
      result.set_value (key.to_string(), value);
    } else {
      result.set_value (key.to_int(), value);
    }
  }

  return result;
}

template <class T1, class T>
array <T> f$array_fill_keys (const array <T1> &a, const T &value) {
  return f$array_fill_keys (array <var> (a), value);
}


template <class T>
array <T> f$array_combine (const array <int> &keys, const array <T> &values) {
  if (keys.count() != values.count()) {
    php_warning ("Size of arrays keys and values must be the same in function array_combine");
    return array <T> ();
  }

  array <T> result (array_size (keys.count(), 0, false));
  typename array <T>::const_iterator it_values = values.begin();
  for (array <int>::const_iterator it_keys = keys.begin(); it_keys != keys.end(); ++it_keys, ++it_values) {
    result.set_value (it_keys.get_value(), it_values.get_value());
  }

  return result;
}

template <class T>
array <T> f$array_combine (const array <string> &keys, const array <T> &values) {
  if (keys.count() != values.count()) {
    php_warning ("Size of arrays keys and values must be the same in function array_combine");
    return array <T> ();
  }

  array <T> result (array_size (0, keys.count(), false));
  typename array <T>::const_iterator it_values = values.begin();
  for (array <string>::const_iterator it_keys = keys.begin(); it_keys != keys.end(); ++it_keys, ++it_values) {
    result.set_value (it_keys.get_value(), it_values.get_value());
  }

  return result;
}

template <class T>
array <T> f$array_combine (const array <var> &keys, const array <T> &values) {
  if (keys.count() != values.count()) {
    php_warning ("Size of arrays keys and values must be the same in function array_combine");
    return array <T> ();
  }

  array <T> result (array_size (keys.count(), keys.count(), false));
  typename array <T>::const_iterator it_values = values.begin();
  for (array <var>::const_iterator it_keys = keys.begin(); it_keys != keys.end(); ++it_keys, ++it_values) {
    const var &key = it_keys.get_value();
    if (!key.is_int()) {
      result.set_value (key.to_string(), it_values.get_value());
    } else {
      result.set_value (key.to_int(), it_values.get_value());
    }
  }

  return result;
}

template <class T1, class T>
array <T> f$array_combine (const array <T1> &keys, const array <T> &values) {
  return f$array_combine (array <var> (keys), values);
}


template <class T, class T1>
array <var> f$range (const T &from, const T1 &to) {
  if (f$is_string (from) && f$is_string (to)) {
    return f$range (f$strval (from), f$strval (to));
  }
  return f$range (f$intval (from), f$intval (to));
}


template <class T1, class T2>
int f$array_push (array <T1> &a, const T2 &val) {
  a.push_back (val);
  return a.count();
}

template <class T1, class T2, class T3>
int f$array_push (array <T1> &a, const T2 &val2, const T3 &val3) {
  a.push_back (val2);
  a.push_back (val3);
  return a.count();
}

template <class T1, class T2, class T3, class T4>
int f$array_push (array <T1> &a, const T2 &val2, const T3 &val3, const T4 &val4) {
  a.push_back (val2);
  a.push_back (val3);
  a.push_back (val4);
  return a.count();
}


template <class T1, class T2, class T3, class T4, class T5>
int f$array_push (array <T1> &a, const T2 &val2, const T3 &val3, const T4 &val4, const T5 &val5) {
  a.push_back (val2);
  a.push_back (val3);
  a.push_back (val4);
  a.push_back (val5);
  return a.count();
}

template <class T1, class T2, class T3, class T4, class T5, class T6>
int f$array_push (array <T1> &a, const T2 &val2, const T3 &val3, const T4 &val4, const T5 &val5, const T6 &val6) {
  a.push_back (val2);
  a.push_back (val3);
  a.push_back (val4);
  a.push_back (val5);
  a.push_back (val6);
  return a.count();
}


template <class T>
T f$array_pop (array <T> &a) {
  return a.pop();
}


template <class T>
void f$shuffle (array <T> &a) {//TODO move into array
  int n = a.count();
  if (n <= 1) {
    return;
  }

  array <T> result (array_size (n, 0, true));
  for (typename array <T>::iterator it = a.begin(); it != a.end(); ++it) {
    result.push_back (it.get_value());
  }

  for (int i = 1; i < n; i++) {
    swap (result[i], result[rand() % (i + 1)]);
  }

  a = result;
}

template <class T>
struct sort_compare {
  bool operator () (const T &h1, const T &h2) const {
    return gt (h1, h2);
  }
};

template <class T>
struct sort_compare_numeric {
  bool operator () (const T &h1, const T &h2) const {
    return f$floatval (h1) > f$floatval (h2);
  }
};

template <>
struct sort_compare_numeric <int> {
  bool operator () (int h1, int h2) const {
    return h1 > h2;
  }
};

template <class T>
struct sort_compare_string {
  bool operator () (const T &h1, const T &h2) const {
    return f$strval (h1).compare (f$strval (h2)) > 0;
  }
};

template <class T>
void f$sort (array <T> &a, int flag) {
  switch (flag) {
    case SORT_REGULAR:
      return a.sort (sort_compare <T> (), true);
    case SORT_NUMERIC:
      return a.sort (sort_compare_numeric <T> (), true);
    case SORT_STRING:
      return a.sort (sort_compare_string <T> (), true);
    default:
      php_warning ("Unsupported sort_flag in function sort");
  }
}


template <class T>
struct rsort_compare {
  bool operator () (const T &h1, const T &h2) const {
    return lt (h1, h2);
  }
};

template <class T>
struct rsort_compare_numeric {
  bool operator () (const T &h1, const T &h2) const {
    return f$floatval (h1) < f$floatval (h2);
  }
};

template <>
struct rsort_compare_numeric <int> {
  bool operator () (int h1, int h2) const {
    return h1 < h2;
  }
};

template <class T>
struct rsort_compare_string {
  bool operator () (const T &h1, const T &h2) const {
    return f$strval (h1).compare (f$strval (h2)) < 0;
  }
};

template <class T>
void f$rsort (array <T> &a, int flag) {
  switch (flag) {
    case SORT_REGULAR:
      return a.sort (rsort_compare <T> (), true);
    case SORT_NUMERIC:
      return a.sort (rsort_compare_numeric <T> (), true);
    case SORT_STRING:
      return a.sort (rsort_compare_string <T> (), true);
    default:
      php_warning ("Unsupported sort_flag in function rsort");
  }
}


template <class T, class T1>
void f$usort (array <T> &a, const T1 &compare) {
  return a.sort (compare, true);
}


template <class T>
void f$asort (array <T> &a, int flag) {
  switch (flag) {
    case SORT_REGULAR:
      return a.sort (sort_compare <T> (), false);
    case SORT_NUMERIC:
      return a.sort (sort_compare_numeric <T> (), false);
    case SORT_STRING:
      return a.sort (sort_compare_string <T> (), false);
    default:
      php_warning ("Unsupported sort_flag in function asort");
  }
}

template <class T>
void f$arsort (array <T> &a, int flag) {
  switch (flag) {
    case SORT_REGULAR:
      return a.sort (rsort_compare <T> (), false);
    case SORT_NUMERIC:
      return a.sort (rsort_compare_numeric <T> (), false);
    case SORT_STRING:
      return a.sort (rsort_compare_string <T> (), false);
    default:
      php_warning ("Unsupported sort_flag in function arsort");
  }
}

template <class T, class T1>
void f$uasort (array <T> &a, const T1 &compare) {
  return a.sort (compare, false);
}


template <class T>
void f$ksort (array <T> &a, int flag) {
  switch (flag) {
    case SORT_REGULAR:
      return a.ksort (sort_compare <typename array <T>::key_type> ());
    case SORT_NUMERIC:
      return a.ksort (sort_compare_numeric <typename array <T>::key_type> ());
    case SORT_STRING:
      return a.ksort (sort_compare_string <typename array <T>::key_type> ());
    default:
      php_warning ("Unsupported sort_flag in function ksort");
  }
}

template <class T>
void f$krsort (array <T> &a, int flag) {
  switch (flag) {
    case SORT_REGULAR:
      return a.ksort (rsort_compare <typename array <T>::key_type> ());
    case SORT_NUMERIC:
      return a.ksort (rsort_compare_numeric <typename array <T>::key_type> ());
    case SORT_STRING:
      return a.ksort (rsort_compare_string <typename array <T>::key_type> ());
    default:
      php_warning ("Unsupported sort_flag in function krsort");
  }
}

template <class T, class T1>
void f$uksort (array <T> &a, const T1 &compare) {
  return a.ksort (compare);
}


template <class T>
double f$array_sum (const array <T> &a) {
  double result = 0;

  for (typename array <T>::const_iterator it = a.begin(); it != a.end(); ++it) {
    result += f$floatval (it.get_value());
  }

  return result;
}


template <class T>
var f$getKeyByPos (const array <T> &a, int pos) {
  typename array <T>::const_iterator it = a.middle (pos);
  if (it == a.end()) {
    return var();
  }
  return it.get_key();
}

template <class T>
T f$getValueByPos (const array <T> &a, int pos) {
  typename array <T>::const_iterator it = a.middle (pos);
  if (it == a.end()) {
    return T();
  }
  return it.get_value();
}

template <class T>
array <T> f$create_vector (int n, const T &default_value) {
  array <T> res (array_size (n, 0, true));
  for (int i = 0; i < n; i++) {
    res.push_back (default_value);
  }
  return res;
}

array <var> f$create_vector (int n) {
  array <var> res (array_size (n, 0, true));
  empty_var = var();
  for (int i = 0; i < n; i++) {
    res.push_back (empty_var);
  }
  return res;
}

