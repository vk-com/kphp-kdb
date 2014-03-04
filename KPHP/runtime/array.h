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
 *
 *   Do not include with file directly
 *   Include kphp_core.h instead
 *
 */

struct array_size {
  int int_size;
  int string_size;
  bool is_vector;

  inline array_size (int int_size, int string_size, bool is_vector): int_size (int_size), string_size (string_size), is_vector (is_vector) {}

  inline array_size operator + (const array_size &other) const {
    return array_size (int_size + other.int_size, string_size + other.string_size, is_vector && other.is_vector);
  }

  inline array_size& cut (int length) {
    if (int_size > length) {
      int_size = length;
    }
    if (string_size > length) {
      string_size = length;
    }
    return *this;
  }

  inline array_size& min (const array_size &other) {
    if (int_size > other.int_size) {
      int_size = other.int_size;
    }
    if (string_size > other.string_size) {
      string_size = other.string_size;
    }
    is_vector &= other.is_vector;
    return *this;
  }
};

template <class T>
class array_stored_type {
public:
  typedef T type;
};
/*
template <>
class array_stored_type <bool> {
public:
  typedef var type;
};

template <>
class array_stored_type <int> {
public:
  typedef var type;
};

template <>
class array_stored_type <double> {
public:
  typedef var type;
};

template <>
class array_stored_type <string> {
public:
  typedef var type;
};

template <>
class array_stored_type <array <var, var> > {
public:
  typedef var type;
};
*/

template <class T, class TT = typename array_stored_type <T>::type>
class force_convert_to;


namespace dl {
  template <class T, class TT, class T1>
  void sort (TT *begin_init, TT *end_init, const T1 &compare) {
    TT *begin_stack[32];
    TT *end_stack[32];

    begin_stack[0] = begin_init;
    end_stack[0] = end_init - 1;

    for (int depth = 0; depth >= 0; --depth) {
      TT *begin = begin_stack[depth];
      TT *end = end_stack[depth];

      while (begin < end) {
        int offset = (end - begin) >> 1;
        swap (force_convert_to <T>::convert (*begin), force_convert_to <T>::convert (begin[offset]));

        TT *i = begin + 1, *j = end;

        while (1) {
          for ( ; i < j && compare (*begin, *i) > 0; i++) {
          }

          for ( ; i <= j && compare (*j, *begin) > 0; j--) {
          }

          if (i >= j) {
            break;
          }

          swap (force_convert_to <T>::convert (*i++), force_convert_to <T>::convert (*j--));
        }

        swap (force_convert_to <T>::convert (*begin), force_convert_to <T>::convert (*j));

        if (j - begin <= end - j) {
          if (j + 1 < end) {
            begin_stack[depth] = j + 1;
            end_stack[depth++] = end;
          }
          end = j - 1;
        } else {
          if (begin < j - 1) {
            begin_stack[depth] = begin;
            end_stack[depth++] = j - 1;
          }
          begin = j + 1;
        }
      }
    }
  }
}


template <class T, class TT = typename array_stored_type <T>::type>
class array {

public:
//  typedef string key_type;
  typedef var key_type;

  inline static bool is_int_key (const key_type &key);

private:

//  struct list_hash_entry;
//  typedef list_hash_entry *entry_pointer_type;
  typedef dl::size_type entry_pointer_type;

  struct list_hash_entry {
    entry_pointer_type next;
    entry_pointer_type prev;
  };

  struct int_hash_entry : list_hash_entry {
    TT value;

    int int_key;

    inline key_type get_key (void) const;
  };

  struct string_hash_entry : list_hash_entry {
    TT value;

    int int_key;
    string string_key;

    inline key_type get_key (void) const;
  };

  template <class T1>
  class compare_list_entry_by_value {
    private:
      const T1& comp;

    public:
      inline compare_list_entry_by_value (const T1 &comp);

      inline compare_list_entry_by_value (const compare_list_entry_by_value &comp);

      inline bool operator () (const int_hash_entry *lhs, const int_hash_entry *rhs) const;
  };

  template <class T1>
  class compare_TT_by_T {
    private:
      const T1& comp;

    public:
      inline compare_TT_by_T (const T1 &comp);

      inline compare_TT_by_T (const compare_TT_by_T &comp);

      inline bool operator () (const TT &lhs, const TT &rhs) const;
  };


  struct array_inner {
    //if key is number, key contains this number, there is no string_key.
    //if key is string, key contains hash of this string, string_key contains this string.
    //empty hash_entry identified by (next == EMPTY_POINTER)
    //vector is_identified by string_buf_size == -1

    static const int MAX_HASHTABLE_SIZE = (1 << 26);
    static const int MIN_HASHTABLE_SIZE = 1;
    static const int DEFAULT_HASHTABLE_SIZE = (1 << 3);

    static const entry_pointer_type EMPTY_POINTER;
    static const T empty_T;

    int ref_cnt;
    int max_key;
    list_hash_entry end_;
    int int_size;
    int int_buf_size;
    int string_size;
    int string_buf_size;
    int_hash_entry int_entries[0];

    inline bool is_vector (void) const __attribute__ ((always_inline));

    inline list_hash_entry *get_entry (entry_pointer_type pointer) const __attribute__ ((always_inline));
    inline entry_pointer_type get_pointer (list_hash_entry *entry) const __attribute__ ((always_inline));

    inline const string_hash_entry *begin (void) const __attribute__ ((always_inline));
    inline const string_hash_entry *next (const string_hash_entry *p) const __attribute__ ((always_inline));
    inline const string_hash_entry *prev (const string_hash_entry *p) const __attribute__ ((always_inline));
    inline const string_hash_entry *end (void) const __attribute__ ((always_inline));

    inline string_hash_entry *begin (void) __attribute__ ((always_inline));
    inline string_hash_entry *next (string_hash_entry *p) __attribute__ ((always_inline));
    inline string_hash_entry *prev (string_hash_entry *p) __attribute__ ((always_inline));
    inline string_hash_entry *end (void) __attribute__ ((always_inline));

    inline bool is_string_hash_entry (const string_hash_entry *p) const __attribute__ ((always_inline));
    inline const string_hash_entry *get_string_entries (void) const __attribute__ ((always_inline));
    inline string_hash_entry *get_string_entries (void) __attribute__ ((always_inline));

    inline static int choose_bucket (const int key, const int buf_size) __attribute__ ((always_inline));

    inline static array_inner *create (int new_int_size, int new_string_size, bool is_vector);

    inline void dispose (void) __attribute__ ((always_inline));

    inline array_inner *ref_copy (void) __attribute__ ((always_inline));

    inline const var get_var (int int_key) const;
    inline const T get_value (int int_key) const;
    inline T& push_back_vector_value (const TT &v) /*__attribute__ ((always_inline))*/;//TODO receive T
    inline T& set_vector_value (int int_key, const TT &v, bool save_value) /*__attribute__ ((always_inline))*/;
    inline T& set_map_value (int int_key, const TT &v, bool save_value) /*__attribute__ ((always_inline))*/;
    inline bool has_key (int int_key) const;
    inline bool isset_value (int int_key) const;
    inline void unset_vector_value (void);
    inline void unset_map_value (int int_key);

    inline const var get_var (int int_key, const string &string_key) const;
    inline const T get_value (int int_key, const string &string_key) const;
    inline T& set_map_value (int int_key, const string &string_key, const TT &v, bool save_value) /*__attribute__ ((always_inline))*/;
    inline bool has_key (int int_key, const string &string_key) const;
    inline bool isset_value (int int_key, const string &string_key) const;
    inline void unset_map_value (int int_key, const string &string_key);

    inline array_inner (void) __attribute__ ((always_inline));
    inline array_inner (const array_inner &other) __attribute__ ((always_inline));
    inline array_inner& operator = (const array_inner &other) __attribute__ ((always_inline));
  };

  array_inner *p;

  inline void mutate_if_needed (void);

  inline void convert_to_map (void);

  template <class T1, class TT1>
  inline void copy_from (const array <T1, TT1> &other);

  inline void destroy (void) __attribute__ ((always_inline));

public:

  class const_iterator {
  private:
    const array_inner *self;
    const list_hash_entry *entry;
  public:
    inline const_iterator (void) __attribute__ ((always_inline));
    inline const_iterator (const array_inner *self, const list_hash_entry *entry) __attribute__ ((always_inline));

    inline const T get_value (void) const __attribute__ ((always_inline));
    inline key_type get_key (void) const __attribute__ ((always_inline));
    inline const_iterator& operator ++ (void) __attribute__ ((always_inline));
    inline const_iterator& operator -- (void) __attribute__ ((always_inline));
    inline bool operator == (const const_iterator &other) const __attribute__ ((always_inline));
    inline bool operator != (const const_iterator &other) const __attribute__ ((always_inline));

    friend class array;

    template <class T1, class TT1>
    friend class array;
  };

  class iterator {
  private:
    array_inner *self;
    list_hash_entry *entry;
  public:
    inline iterator (void) __attribute__ ((always_inline));
    inline iterator (array_inner *self, list_hash_entry *entry) __attribute__ ((always_inline));

    inline T& get_value (void) __attribute__ ((always_inline));
    inline key_type get_key (void) const __attribute__ ((always_inline));
    inline iterator& operator ++ (void) __attribute__ ((always_inline));
    inline iterator& operator -- (void) __attribute__ ((always_inline));
    inline bool operator == (const iterator &other) const __attribute__ ((always_inline));
    inline bool operator != (const iterator &other) const __attribute__ ((always_inline));

    friend class array;

    template <class T1, class TT1>
    friend class array;
  };

  inline array (void) __attribute__ ((always_inline));

  inline explicit array (const array_size &s) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2, const T &a3) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6, const T &a7) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6, const T &a7, const T &a8) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6, const T &a7, const T &a8, const T &a9) __attribute__ ((always_inline));

  inline array (const T &a0, const T &a1, const T &a2, const T &a3, const T &a4, const T &a5, const T &a6, const T &a7, const T &a8, const T &a9, const T &a10) __attribute__ ((always_inline));

  inline array (const array &other) __attribute__ ((always_inline));

  template <class T1, class TT1>
  inline array (const array <T1, TT1> &other);

  template <class T1>
  inline array (const array <T1, TT> &other) __attribute__ ((always_inline));

  inline array& operator = (const array &other) __attribute__ ((always_inline));

  template <class T1, class TT1>
  inline array& operator = (const array <T1, TT1> &other);

  template <class T1>
  inline array& operator = (const array <T1, TT> &other) __attribute__ ((always_inline));

  inline ~array (void) /*__attribute__ ((always_inline))*/;

  inline bool is_vector (void) const __attribute__ ((always_inline));

  T& operator[] (int int_key);
  T& operator[] (const string &s);
  T& operator[] (const var &v);
  T& operator[] (const const_iterator &it);
  T& operator[] (const iterator &it);

  void set_value (int int_key, const T &v);
  void set_value (const string &s, const T &v);
  void set_value (const var &v, const T &value);
  void set_value (const const_iterator &it);
  void set_value (const iterator &it);

  const var get_var (int int_key) const;
  const var get_var (const string &s) const;
  const var get_var (const var &v) const;

  const T get_value (int int_key) const;
  const T get_value (const string &s) const;
  const T get_value (const var &v) const;
  const T get_value (const const_iterator &it) const;
  const T get_value (const iterator &it) const;

  void push_back (const T &v);
  void push_back (const const_iterator &it);
  void push_back (const iterator &it);
  const T push_back_return (const T &v);

  inline int get_next_key (void) const __attribute__ ((always_inline));

  bool has_key (int int_key) const;
  bool has_key (const string &s) const;
  bool has_key (const var &v) const;
  bool has_key (const const_iterator &it) const;
  bool has_key (const iterator &it) const;

  bool isset (int int_key) const;
  bool isset (const string &s) const;
  bool isset (const var &v) const;
  bool isset (const const_iterator &it) const;
  bool isset (const iterator &it) const;

  void unset (int int_key);
  void unset (const string &s);
  void unset (const var &v);
  void unset (const const_iterator &it);
  void unset (const iterator &it);

  inline bool empty (void) const __attribute__ ((always_inline));
  inline int count (void) const __attribute__ ((always_inline));

  inline array_size size (void) const __attribute__ ((always_inline));

  template <class T1, class TT1>
  void merge_with (const array <T1, TT1> &other);

  const array operator + (const array &other) const;
  array& operator += (const array &other);

  inline const_iterator begin (void) const __attribute__ ((always_inline));
  inline const_iterator middle (int n) const __attribute__ ((always_inline));
  inline const_iterator end (void) const __attribute__ ((always_inline));

  inline iterator begin (void) __attribute__ ((always_inline));
  inline iterator middle (int n) __attribute__ ((always_inline));
  inline iterator end (void) __attribute__ ((always_inline));

  template <class T1>
  void sort (const T1 &compare, bool renumber);

  template <class T1>
  void ksort (const T1 &compare);

  inline void swap (array &other) __attribute__ ((always_inline));


  T pop (void);

  T shift (void);

  int unshift (const T &val);


  inline bool to_bool (void) const __attribute__ ((always_inline));
  inline int to_int (void) const __attribute__ ((always_inline));
  inline double to_float (void) const __attribute__ ((always_inline));
  inline const object to_object (void) const;


  int get_reference_counter (void) const;


  template <class T1, class TT1>
  friend class array;

  friend class var;

  friend class stdClass;
};

template <class T, class TT>
inline void swap (array <T, TT> &lhs, array <T, TT> &rhs);

template <class T>
inline const array <T> array_add (array <T> a1, const array <T> &a2);

template <class T, class TT>
inline bool eq2 (const array <T, TT> &lhs, const array <T, TT> &rhs);

template <class T1, class TT1, class T2, class TT2>
inline bool eq2 (const array <T1, TT1> &lhs, const array <T2, TT2> &rhs);

template <class T, class TT>
inline bool equals (const array <T, TT> &lhs, const array <T, TT> &rhs);

template <class T1, class TT1, class T2, class TT2>
inline bool equals (const array <T1, TT1> &lhs, const array <T2, TT2> &rhs);
