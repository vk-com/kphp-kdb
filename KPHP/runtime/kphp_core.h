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

#include "allocator.h"
#include "include.h"
#include "string.h"
#include "array.h"
#include "class.h"
#include "variable.h"
#include "string_buffer.h"

#define STRING(s) ((string *)&s)
#define ARRAY(a) ((array <var> *)&a)
#define CONST_ARRAY(a) ((const array <var> *)&a)
#define OBJECT(o) ((object *)&o)

#include "string.cpp"
#include "array.cpp"
#include "class.cpp"
#include "variable.cpp"
#include "string_buffer.cpp"

#undef STRING
#undef ARRAY
#undef CONST_ARRAY
#undef OBJECT

class UnknownType {
};

#define unimplemented_function(name, args...) ({                              \
  php_critical_error ("unimplemented_function: %s", f$strval (name).c_str()); \
  1;                                                                          \
})

#define require(flag_name, action) ({ \
  flag_name = true;                   \
  action;                             \
})

#define require_once(flag_name, action) ({ \
  if (!flag_name) {                        \
    require (flag_name, action);           \
  }                                        \
})

#define f$func_get_args() (VA_LIST)
#define f$func_get_arg(i) (VA_LIST.get_value (i))
#define f$func_num_args() (f$count (VA_LIST))
#define INIT_VAR(type, x) new (&x) type()
#define CLEAR_VAR(type, x) memset (&x, 0, sizeof (x))

#define SAFE_SET_OP(a, op, b, b_type) ({b_type b_tmp___ = b; a op b_tmp___;})
#define SAFE_SET_FUNC_OP(a, func, b, b_type) ({b_type b_tmp___ = b; func (a, b_tmp___);})
#define SAFE_INDEX(a, b, b_type) a[({b_type b_tmp___ = b; b_tmp___;})]
#define SAFE_SET_VALUE(a, b, b_type, c, c_type) ({b_type b_tmp___ = b; c_type c_tmp___ = c; (a).set_value (b_tmp___, c_tmp___);})
#define SAFE_PUSH_BACK(a, b, b_type) ({b_type b_tmp___ = b; a.push_back (b_tmp___);})
#define SAFE_PUSH_BACK_RETURN(a, b, b_type) ({b_type b_tmp___ = b; a.push_back_return (b_tmp___);})
#define NOERR(a, a_type) ({php_disable_warnings++; a_type a_tmp___ = a; php_disable_warnings--; a_tmp___;})
#define NOERR_VOID(a) ({php_disable_warnings++; a; php_disable_warnings--;})


inline bool lt (const bool &lhs, const bool &rhs);

template <class T2>
inline bool lt (const bool &lhs, const T2 &rhs);

template <class T1>
inline bool lt (const T1 &lhs, const bool &rhs);

template <class T1, class T2>
inline bool lt (const T1 &lhs, const T2 &rhs);

template <class T1, class T2>
inline bool lt (const OrFalse <T1> &lhs, const T2 &rhs);

template <class T1, class T2>
inline bool lt (const T1 &lhs, const OrFalse <T2> &rhs);

template <class T>
inline bool lt (const OrFalse <T> &lhs, const OrFalse <T> &rhs);

template <class T1, class T2>
inline bool lt (const OrFalse <T1> &lhs, const OrFalse <T2> &rhs);

template <class T>
inline bool lt (const bool &lhs, const OrFalse <T> &rhs);

template <class T>
inline bool lt (const OrFalse <T> &lhs, const bool &rhs);



inline bool gt (const bool &lhs, const bool &rhs);

template <class T2>
inline bool gt (const bool &lhs, const T2 &rhs);

template <class T1>
inline bool gt (const T1 &lhs, const bool &rhs);

template <class T1, class T2>
inline bool gt (const T1 &lhs, const T2 &rhs);

template <class T1, class T2>
inline bool gt (const OrFalse <T1> &lhs, const T2 &rhs);

template <class T1, class T2>
inline bool gt (const T1 &lhs, const OrFalse <T2> &rhs);

template <class T>
inline bool gt (const OrFalse <T> &lhs, const OrFalse <T> &rhs);

template <class T1, class T2>
inline bool gt (const OrFalse <T1> &lhs, const OrFalse <T2> &rhs);

template <class T>
inline bool gt (const bool &lhs, const OrFalse <T> &rhs);

template <class T>
inline bool gt (const OrFalse <T> &lhs, const bool &rhs);


inline bool leq (const bool &lhs, const bool &rhs);

template <class T2>
inline bool leq (const bool &lhs, const T2 &rhs);

template <class T1>
inline bool leq (const T1 &lhs, const bool &rhs);

template <class T1, class T2>
inline bool leq (const T1 &lhs, const T2 &rhs);

template <class T1, class T2>
inline bool leq (const OrFalse <T1> &lhs, const T2 &rhs);

template <class T1, class T2>
inline bool leq (const T1 &lhs, const OrFalse <T2> &rhs);

template <class T>
inline bool leq (const OrFalse <T> &lhs, const OrFalse <T> &rhs);

template <class T1, class T2>
inline bool leq (const OrFalse <T1> &lhs, const OrFalse <T2> &rhs);

template <class T>
inline bool leq (const bool &lhs, const OrFalse <T> &rhs);

template <class T>
inline bool leq (const OrFalse <T> &lhs, const bool &rhs);


inline bool geq (const bool &lhs, const bool &rhs);

template <class T2>
inline bool geq (const bool &lhs, const T2 &rhs);

template <class T1>
inline bool geq (const T1 &lhs, const bool &rhs);

template <class T1, class T2>
inline bool geq (const T1 &lhs, const T2 &rhs);

template <class T1, class T2>
inline bool geq (const OrFalse <T1> &lhs, const T2 &rhs);

template <class T1, class T2>
inline bool geq (const T1 &lhs, const OrFalse <T2> &rhs);

template <class T>
inline bool geq (const OrFalse <T> &lhs, const OrFalse <T> &rhs);

template <class T1, class T2>
inline bool geq (const OrFalse <T1> &lhs, const OrFalse <T2> &rhs);

template <class T>
inline bool geq (const bool &lhs, const OrFalse <T> &rhs);

template <class T>
inline bool geq (const OrFalse <T> &lhs, const bool &rhs);


inline double divide (int lhs, int rhs);

inline double divide (double lhs, int rhs);

inline double divide (const string &lhs, int rhs);

inline double divide (const var &lhs, int rhs);


inline double divide (int lhs, double rhs);

inline double divide (double lhs, double rhs);

inline double divide (const string &lhs, double rhs);

inline double divide (const var &lhs, double rhs);


inline double divide (int lhs, string rhs);

inline double divide (double lhs, string rhs);

inline double divide (const string &lhs, string rhs);

inline double divide (const var &lhs, string rhs);


inline double divide (int lhs, const var &rhs);

inline double divide (double lhs, const var &rhs);

inline double divide (const string &lhs, const var &rhs);

inline double divide (const var &lhs, const var &rhs);


inline double divide (bool lhs, bool rhs);

template <class T>
inline double divide (bool lhs, const T &rhs);

template <class T>
inline double divide (const T &lhs, bool rhs);

template <class T>
inline double divide (bool lhs, const array <T> &rhs);

template <class T>
inline double divide (const array <T> &lhs, bool rhs);

template <class T>
inline double divide (bool lhs, const object_ptr <T> &rhs);

template <class T>
inline double divide (const object_ptr <T> &lhs, bool rhs);


template <class T, class T1>
inline double divide (const array <T> &lhs, const T1 &rhs);

template <class T, class T1>
inline double divide (const T1 &lhs, const array <T> &rhs);


template <class T, class T1>
inline double divide (const object_ptr <T> &lhs, const T1 &rhs);

template <class T, class T1>
inline double divide (const T1 &lhs, const object_ptr <T> &rhs);


template <class T>
inline double divide (const array <T> &lhs, const array <T> &rhs);

template <class T>
inline double divide (const object_ptr <T> &lhs, const object_ptr <T> &rhs);

template <class T, class T1>
inline double divide (const array <T> &lhs, const array <T1> &rhs);

template <class T, class T1>
inline double divide (const object_ptr <T> &lhs, const object_ptr <T1> &rhs);

template <class T, class T1>
inline double divide (const array <T> &lhs, const object_ptr <T1> &rhs);

template <class T, class T1>
inline double divide (const object_ptr <T1> &lhs, const array <T> &rhs);


template <class T1, class T2>
inline double divide (const OrFalse <T1> &lhs, const T2 &rhs); //not defined

template <class T1, class T2>
inline double divide (const T1 &lhs, const OrFalse <T2> &rhs); //not defined


inline int modulo (int lhs, int rhs);

template <class T1, class T2>
inline int modulo (const T1 &lhs, const T2 &rhs);


template <class T1, class T2>
inline T1 &divide_self (T1 &lhs, const T2 &rhs);


inline int &modulo_self (int &lhs, int rhs);

template <class T1, class T2>
inline T1 &modulo_self (T1 &lhs, const T2 &rhs);


template <class T0, class T>
inline void assign (T0 &dest, const T &from);


inline const bool &f$boolval (const bool &val);

inline bool f$boolval (const int &val);

inline bool f$boolval (const double &val);

inline bool f$boolval (const string &val);

template <class T>
inline bool f$boolval (const array <T> &val);

template <class T>
inline bool f$boolval (const object_ptr <T> &val);

inline bool f$boolval (const var &val);


inline int f$intval (const bool &val);

inline const int &f$intval (const int &val);

inline int f$intval (const double &val);

inline int f$intval (const string &val);

template <class T>
inline int f$intval (const array <T> &val);

template <class T>
inline int f$intval (const object_ptr <T> &val);

inline int f$intval (const var &val);


inline int f$safe_intval (const bool &val);

inline const int &f$safe_intval (const int &val);

inline int f$safe_intval (const double &val);

inline int f$safe_intval (const string &val);

template <class T>
inline int f$safe_intval (const array <T> &val);

template <class T>
inline int f$safe_intval (const object_ptr <T> &val);

inline int f$safe_intval (const var &val);


inline double f$floatval (const bool &val);

inline double f$floatval (const int &val);

inline const double &f$floatval (const double &val);

inline double f$floatval (const string &val);

template <class T>
inline double f$floatval (const array <T> &val);

template <class T>
inline double f$floatval (const object_ptr <T> &val);

inline double f$floatval (const var &val);


inline string f$strval (const bool &val);

inline string f$strval (const int &val);

inline string f$strval (const double &val);

inline string f$strval (const string &val);

template <class T>
inline string f$strval (const array <T> &val);

template <class T>
inline string f$strval (const object_ptr <T> &val);

inline string f$strval (const var &val);


template <class T>
inline array <var> f$arrayval (const T &val);

template <class T>
inline const array <T> &f$arrayval (const array <T> &val);

template <class T>
inline array <var> f$arrayval (const object_ptr <T> &val);

inline array <var> f$arrayval (const var &val);


template <class T>
inline object f$objectval (const T &val);

inline const object &f$objectval (const object &val);

template <class T>
inline object f$objectval (const object_ptr <T> &val);

template <class T>
inline object f$objectval (const array <T> &val);

inline object f$objectval (const var &val);


inline bool& boolval_ref (bool &val);

inline bool& boolval_ref (var &val);

inline const bool& boolval_ref (const bool &val);

inline const bool& boolval_ref (const var &val);


inline int& intval_ref (int &val);

inline int& intval_ref (var &val);

inline const int& intval_ref (const int &val);

inline const int& intval_ref (const var &val);


inline double& floatval_ref (double &val);

inline double& floatval_ref (var &val);

inline const double& floatval_ref (const double &val);

inline const double& floatval_ref (const var &val);


inline string& strval_ref (string &val);

inline string& strval_ref (var &val);

inline const string& strval_ref (const string &val);

inline const string& strval_ref (const var &val);


template <class T>
inline array <T>& arrayval_ref (array <T> &val, const char *function, int parameter_num);

inline array <var>& arrayval_ref (var &val, const char *function, int parameter_num);

template <class T>
inline const array <T>& arrayval_ref (const array <T> &val, const char *function, int parameter_num);

inline const array <var>& arrayval_ref (const var &val, const char *function, int parameter_num);


template <class T>
bool f$boolval (const OrFalse <T> &v);

template <class T>
bool eq2 (const OrFalse <T> &v, bool value);

template <class T>
bool eq2 (bool value, const OrFalse <T> &v);

template <class T>
bool eq2 (const OrFalse <T> &v, const OrFalse <T> &value);

template <class T, class T1>
bool eq2 (const OrFalse <T> &v, const OrFalse <T1> &value);

template <class T, class T1>
bool eq2 (const OrFalse <T> &v, const T1 &value);

template <class T, class T1>
bool eq2 (const T1 &value, const OrFalse <T> &v);

template <class T>
bool equals (const OrFalse <T> &value, const OrFalse <T> &v);

template <class T, class T1>
bool equals (const OrFalse <T1> &value, const OrFalse <T> &v);

template <class T, class T1>
bool equals (const T1 &value, const OrFalse <T> &v);

template <class T, class T1>
bool equals (const OrFalse <T> &v, const T1 &value);


template <class T>
class convert_to {
public:
  static inline const T& convert (const T &val);

  static inline T convert (const Unknown &val);

  template <class T1>
  static inline T convert (const T1 &val);
};


inline bool f$empty (const bool &v);

inline bool f$empty (const int &v);

inline bool f$empty (const double &v);

inline bool f$empty (const string &v);

inline bool f$empty (const var &v);

template <class T>
inline bool f$empty (const array <T> &v);

template <class T>
inline bool f$empty (const object_ptr <T> &v);


inline bool f$is_numeric (const bool &v);

inline bool f$is_numeric (const int &v);

inline bool f$is_numeric (const double &v);

inline bool f$is_numeric (const string &v);

inline bool f$is_numeric (const var &v);

template <class T>
inline bool f$is_numeric (const array <T> &v);

template <class T>
inline bool f$is_numeric (const object_ptr <T> &v);


inline bool f$is_null (const bool &v);

inline bool f$is_null (const int &v);

inline bool f$is_null (const double &v);

inline bool f$is_null (const string &v);

inline bool f$is_null (const var &v);

template <class T>
inline bool f$is_null (const array <T> &v);

template <class T>
inline bool f$is_null (const object_ptr <T> &v);


inline bool f$is_bool (const bool &v);

inline bool f$is_bool (const int &v);

inline bool f$is_bool (const double &v);

inline bool f$is_bool (const string &v);

inline bool f$is_bool (const var &v);

template <class T>
inline bool f$is_bool (const array <T> &v);

template <class T>
inline bool f$is_bool (const object_ptr <T> &v);


inline bool f$is_int (const bool &v);

inline bool f$is_int (const int &v);

inline bool f$is_int (const double &v);

inline bool f$is_int (const string &v);

inline bool f$is_int (const var &v);

template <class T>
inline bool f$is_int (const array <T> &v);

template <class T>
inline bool f$is_int (const object_ptr <T> &v);


inline bool f$is_float (const bool &v);

inline bool f$is_float (const int &v);

inline bool f$is_float (const double &v);

inline bool f$is_float (const string &v);

inline bool f$is_float (const var &v);

template <class T>
inline bool f$is_float (const array <T> &v);

template <class T>
inline bool f$is_float (const object_ptr <T> &v);


inline bool f$is_scalar (const bool &v);

inline bool f$is_scalar (const int &v);

inline bool f$is_scalar (const double &v);

inline bool f$is_scalar (const string &v);

inline bool f$is_scalar (const var &v);

template <class T>
inline bool f$is_scalar (const array <T> &v);

template <class T>
inline bool f$is_scalar (const object_ptr <T> &v);


inline bool f$is_string (const bool &v);

inline bool f$is_string (const int &v);

inline bool f$is_string (const double &v);

inline bool f$is_string (const string &v);

inline bool f$is_string (const var &v);

template <class T>
inline bool f$is_string (const array <T> &v);

template <class T>
inline bool f$is_string (const object_ptr <T> &v);


inline bool f$is_array (const bool &v);

inline bool f$is_array (const int &v);

inline bool f$is_array (const double &v);

inline bool f$is_array (const string &v);

inline bool f$is_array (const var &v);

template <class T>
inline bool f$is_array (const array <T> &v);

template <class T>
inline bool f$is_array (const object_ptr <T> &v);


inline bool f$is_object (const bool &v);

inline bool f$is_object (const int &v);

inline bool f$is_object (const double &v);

inline bool f$is_object (const string &v);

inline bool f$is_object (const var &v);

template <class T>
inline bool f$is_object (const array <T> &v);

template <class T>
inline bool f$is_object (const object_ptr <T> &v);


template <class T>
inline bool f$is_integer (const T &v);

template <class T>
inline bool f$is_long (const T &v);

template <class T>
inline bool f$is_double (const T &v);

template <class T>
inline bool f$is_real (const T &v);


inline const char *get_type_c_str (const bool &v);

inline const char *get_type_c_str (const int &v);

inline const char *get_type_c_str (const double &v);

inline const char *get_type_c_str (const string &v);

inline const char *get_type_c_str (const var &v);

template <class T>
inline const char *get_type_c_str (const array <T> &v);

template <class T>
inline const char *get_type_c_str (const object_ptr <T> &v);

template <class T>
inline string f$get_type (const T &v);


inline const string get_value (const string &v, int int_key);
inline const string get_value (const string &v, const string &string_key);
inline const string get_value (const string &v, const var &key);

inline const var get_value (const var &v, int int_key);
inline const var get_value (const var &v, const string &string_key);
inline const var get_value (const var &v, const var &key);

template <class T>
inline const T get_value (const array <T> &v, int int_key);
template <class T>
inline const T get_value (const array <T> &v, const string &string_key);
template <class T>
inline const T get_value (const array <T> &v, const var &key);

inline const string get_value (const OrFalse <string> &v, int int_key);
inline const string get_value (const OrFalse <string> &v, const string &string_key);
inline const string get_value (const OrFalse <string> &v, const var &key);

template <class T>
inline const T get_value (const OrFalse <array <T> > &v, int int_key);
template <class T>
inline const T get_value (const OrFalse <array <T> > &v, const string &string_key);
template <class T>
inline const T get_value (const OrFalse <array <T> > &v, const var &key);

template <class T>
inline const var get_value (const T &v, int int_key);
template <class T>
inline const var get_value (const T &v, const string &string_key);
template <class T>
inline const var get_value (const T &v, const var &key);


inline int f$count (const var &v);

template <class T>
inline int f$count (const array <T> &a);

template <class T>
inline int f$count (const T &v);


template <class T>
int f$sizeof (const T &v);


inline string& append (string &dest, const string &from);

template <class T>
inline string& append (string &dest, const T &from);

template <class T>
inline var& append (var &dest, const T &from);

template <class T0, class T>
inline T0& append (T0 &dest, const T &from);


inline string f$gettype (const var &v);

template <class T>
inline bool f$function_exists (const T &a1);


const int E_ERROR = 1;
const int E_WARNING = 2;
const int E_PARSE = 4;
const int E_NOTICE = 8;
const int E_CORE_ERROR = 16;
const int E_CORE_WARNING = 32;
const int E_COMPILE_ERROR = 64;
const int E_COMPILE_WARNING = 128;
const int E_USER_ERROR = 256;
const int E_USER_WARNING = 512;
const int E_USER_NOTICE = 1024;
const int E_STRICT = 2048;
const int E_RECOVERABLE_ERROR = 4096;
const int E_DEPRECATED = 8192;
const int E_USER_DEPRECATED = 16384;
const int E_ALL = 32767;

inline var f$error_get_last (void);

inline int f$error_reporting (int level);

inline int f$error_reporting (void);

inline void f$warning (const string &message);

inline int f$memory_get_static_usage (void);

inline int f$memory_get_peak_usage (bool real_usage = false);

inline int f$memory_get_usage (bool real_usage = false);

inline int f$memory_get_total_usage (void);


template <class T, class TT>
inline int f$get_reference_counter (const array <T, TT> &v);

template <class T>
inline int f$get_reference_counter (const object_ptr <T> &v);

inline int f$get_reference_counter (const string &v);

inline int f$get_reference_counter (const var &v);


template <class T>
inline T& val (T &x);

template <class T>
inline const T& val (const T &x);

template <class T>
inline T& ref (T &x);

template <class T>
inline const T& val (const OrFalse <T> &x);

template <class T>
inline T& val (OrFalse <T> &x);

template <class T>
inline T& ref (OrFalse <T> &x);


template <class T, class TT>
inline typename array <T, TT>::iterator begin (array <T, TT> &x);

template <class T, class TT>
inline typename array <T, TT>::const_iterator begin (const array <T, TT> &x);

inline array <var, var>::iterator begin (var &x);

inline array <var, var>::const_iterator begin (const var &x);

template <class T, class TT>
inline typename array <T, TT>::iterator begin (OrFalse < array <T, TT> > &x);

template <class T, class TT>
inline typename array <T, TT>::const_iterator begin (const OrFalse < array <T, TT> > &x);

template <class T, class TT>
inline typename array <T, TT>::iterator end (array <T, TT> &x);

template <class T, class TT>
inline typename array <T, TT>::const_iterator end (const array <T, TT> &x);

inline array <var, var>::iterator end (var &x);

inline array <var, var>::const_iterator end (const var &x);

template <class T, class TT>
inline typename array <T, TT>::iterator end (OrFalse < array <T, TT> > &x);

template <class T, class TT>
inline typename array <T, TT>::const_iterator end (const OrFalse < array <T, TT> > &x);


inline var& clean_array (var &v);

template <class T>
inline array <T>& clean_array (array <T> &a);

template <class T>
inline OrFalse <array <T> >& clean_array (OrFalse <array <T> > &a);

template <class T>
inline void unset (array <T> &x);

inline void unset (var &x);


/*
 *
 *     IMPLEMENTATION
 *
 */


bool lt (const bool &lhs, const bool &rhs) {
  return lhs < rhs;
}

template <class T2>
bool lt (const bool &lhs, const T2 &rhs) {
  return lhs < f$boolval (rhs);
}

template <class T1>
bool lt (const T1 &lhs, const bool &rhs) {
  return f$boolval (lhs) < rhs;
}

template <class T1, class T2>
bool lt (const T1 &lhs, const T2 &rhs) {
  return lhs < rhs;
}

template <class T1, class T2>
bool lt (const OrFalse <T1> &lhs, const T2 &rhs) {
  return lhs.bool_value ? lt (lhs.value, rhs) : lt (false, rhs);
}

template <class T1, class T2>
bool lt (const T1 &lhs, const OrFalse <T2> &rhs) {
  return rhs.bool_value ? lt (lhs, rhs.value) : lt (lhs, false);
}

template <class T>
bool lt (const OrFalse <T> &lhs, const OrFalse <T> &rhs) {
  return lhs.bool_value ? lt (lhs.value, rhs) : lt (false, rhs);
}

template <class T1, class T2>
bool lt (const OrFalse <T1> &lhs, const OrFalse <T2> &rhs) {
  return lhs.bool_value ? lt (lhs.value, rhs) : lt (false, rhs);
}

template <class T>
bool lt (const bool &lhs, const OrFalse <T> &rhs) {
  return lt (lhs, f$boolval (rhs));
}

template <class T>
bool lt (const OrFalse <T> &lhs, const bool &rhs) {
  return lt (f$boolval (lhs), rhs);
}


bool gt (const bool &lhs, const bool &rhs) {
  return lhs > rhs;
}

template <class T2>
bool gt (const bool &lhs, const T2 &rhs) {
  return lhs > f$boolval (rhs);
}

template <class T1>
bool gt (const T1 &lhs, const bool &rhs) {
  return f$boolval (lhs) > rhs;
}

template <class T1, class T2>
bool gt (const T1 &lhs, const T2 &rhs) {
  return lhs > rhs;
}

template <class T1, class T2>
bool gt (const OrFalse <T1> &lhs, const T2 &rhs) {
  return lhs.bool_value ? gt (lhs.value, rhs) : gt (false, rhs);
}

template <class T1, class T2>
bool gt (const T1 &lhs, const OrFalse <T2> &rhs) {
  return rhs.bool_value ? gt (lhs, rhs.value) : gt (lhs, false);
}

template <class T>
bool gt (const OrFalse <T> &lhs, const OrFalse <T> &rhs) {
  return lhs.bool_value ? gt (lhs.value, rhs) : gt (false, rhs);
}

template <class T1, class T2>
bool gt (const OrFalse <T1> &lhs, const OrFalse <T2> &rhs) {
  return lhs.bool_value ? gt (lhs.value, rhs) : gt (false, rhs);
}

template <class T>
bool gt (const bool &lhs, const OrFalse <T> &rhs) {
  return gt (lhs, f$boolval (rhs));
}

template <class T>
bool gt (const OrFalse <T> &lhs, const bool &rhs) {
  return gt (f$boolval (lhs), rhs);
}


bool leq (const bool &lhs, const bool &rhs) {
  return lhs <= rhs;
}

template <class T2>
bool leq (const bool &lhs, const T2 &rhs) {
  return lhs <= f$boolval (rhs);
}

template <class T1>
bool leq (const T1 &lhs, const bool &rhs) {
  return f$boolval (lhs) <= rhs;
}

template <class T1, class T2>
bool leq (const T1 &lhs, const T2 &rhs) {
  return lhs <= rhs;
}

template <class T1, class T2>
bool leq (const OrFalse <T1> &lhs, const T2 &rhs) {
  return lhs.bool_value ? leq (lhs.value, rhs) : leq (false, rhs);
}

template <class T1, class T2>
bool leq (const T1 &lhs, const OrFalse <T2> &rhs) {
  return rhs.bool_value ? leq (lhs, rhs.value) : leq (lhs, false);
}

template <class T>
bool leq (const OrFalse <T> &lhs, const OrFalse <T> &rhs) {
  return lhs.bool_value ? leq (lhs.value, rhs) : leq (false, rhs);
}

template <class T1, class T2>
bool leq (const OrFalse <T1> &lhs, const OrFalse <T2> &rhs) {
  return lhs.bool_value ? leq (lhs.value, rhs) : leq (false, rhs);
}

template <class T>
bool leq (const bool &lhs, const OrFalse <T> &rhs) {
  return leq (lhs, f$boolval (rhs));
}

template <class T>
bool leq (const OrFalse <T> &lhs, const bool &rhs) {
  return leq (f$boolval (lhs), rhs);
}


bool geq (const bool &lhs, const bool &rhs) {
  return lhs >= rhs;
}

template <class T2>
bool geq (const bool &lhs, const T2 &rhs) {
  return lhs >= f$boolval (rhs);
}

template <class T1>
bool geq (const T1 &lhs, const bool &rhs) {
  return f$boolval (lhs) >= rhs;
}

template <class T1, class T2>
bool geq (const T1 &lhs, const T2 &rhs) {
  return lhs >= rhs;
}

template <class T1, class T2>
bool geq (const OrFalse <T1> &lhs, const T2 &rhs) {
  return lhs.bool_value ? geq (lhs.value, rhs) : geq (false, rhs);
}

template <class T1, class T2>
bool geq (const T1 &lhs, const OrFalse <T2> &rhs) {
  return rhs.bool_value ? geq (lhs, rhs.value) : geq (lhs, false);
}

template <class T>
bool geq (const OrFalse <T> &lhs, const OrFalse <T> &rhs) {
  return lhs.bool_value ? geq (lhs.value, rhs) : geq (false, rhs);
}

template <class T1, class T2>
bool geq (const OrFalse <T1> &lhs, const OrFalse <T2> &rhs) {
  return lhs.bool_value ? geq (lhs.value, rhs) : geq (false, rhs);
}

template <class T>
bool geq (const bool &lhs, const OrFalse <T> &rhs) {
  return geq (lhs, f$boolval (rhs));
}

template <class T>
bool geq (const OrFalse <T> &lhs, const bool &rhs) {
  return geq (f$boolval (lhs), rhs);
}


double divide (int lhs, int rhs) {
  if (rhs == 0) {
    php_warning ("Integer division by zero");
    return 0;
  }

  return double (lhs) / rhs;
}

double divide (double lhs, int rhs) {
  if (rhs == 0) {
    php_warning ("Integer division by zero");
    return 0;
  }

  return lhs / rhs;
}

double divide (const string &lhs, int rhs) {
  return divide (f$floatval (lhs), rhs);
}

double divide (const var &lhs, int rhs) {
  return divide (f$floatval (lhs), rhs);
}


double divide (int lhs, double rhs) {
  if (rhs == 0) {
    php_warning ("Float division by zero");
    return 0;
  }

  return lhs / rhs;
}

double divide (double lhs, double rhs) {
  if (rhs == 0) {
    php_warning ("Float division by zero");
    return 0;
  }

  return lhs / rhs;
}

double divide (const string &lhs, double rhs) {
  return divide (f$floatval (lhs), rhs);
}

double divide (const var &lhs, double rhs) {
  return divide (f$floatval (lhs), rhs);
}


double divide (int lhs, string rhs) {
  return divide (lhs, f$floatval (rhs));
}

double divide (double lhs, string rhs) {
  return divide (lhs, f$floatval (rhs));
}

double divide (const string &lhs, string rhs) {
  return divide (f$floatval (lhs), f$floatval (rhs));
}

double divide (const var &lhs, string rhs) {
  return divide (lhs, f$floatval (rhs));
}


double divide (int lhs, const var &rhs) {
  return divide (lhs, f$floatval (rhs));
}

double divide (double lhs, const var &rhs) {
  return divide (lhs, f$floatval (rhs));
}

double divide (const string &lhs, const var &rhs) {
  return divide (f$floatval (lhs), rhs);
}

double divide (const var &lhs, const var &rhs) {
  return f$floatval (lhs / rhs);
}


double divide (bool lhs, bool rhs) {
  php_warning ("Both arguments of operator '/' are bool");
  return lhs;
}

template <class T>
double divide (bool lhs, const T &rhs) {
  php_warning ("First argument of operator '/' is bool");
  return divide ((int)lhs, f$floatval (rhs));
}

template <class T>
double divide (const T &lhs, bool rhs) {
  php_warning ("Second argument of operator '/' is bool");
  return f$floatval (lhs);
}

template <class T>
double divide (bool lhs, const array <T> &rhs) {
  php_warning ("Unsupported operand types for operator '/' bool and array");
  return 0.0;
}

template <class T>
double divide (const array <T> &lhs, bool rhs) {
  php_warning ("Unsupported operand types for operator '/' array and bool");
  return 0.0;
}

template <class T>
double divide (bool lhs, const object_ptr <T> &rhs) {
  php_warning ("Unsupported operand types for operator '/' bool and object");
  return 0.0;
}

template <class T>
double divide (const object_ptr <T> &lhs, bool rhs) {
  php_warning ("Unsupported operand types for operator '/' object and bool");
  return 0.0;
}


template <class T, class T1>
double divide (const array <T> &lhs, const T1 &rhs) {
  php_warning ("First argument of operator '/' is array");
  return divide (f$count (lhs), rhs);
}

template <class T, class T1>
double divide (const T1 &lhs, const array <T> &rhs) {
  php_warning ("Second argument of operator '/' is array");
  return divide (lhs, f$count (rhs));
}


template <class T, class T1>
double divide (const object_ptr <T> &lhs, const T1 &rhs) {
  php_warning ("First argument of operator '/' is object");
  return divide (1.0, rhs);
}

template <class T, class T1>
double divide (const T1 &lhs, const object_ptr <T> &rhs) {
  php_warning ("Second argument of operator '/' is object");
  return lhs;
}


template <class T>
double divide (const array <T> &lhs, const array <T> &rhs) {
  php_warning ("Unsupported operand types for operator '/' array and array");
  return 0.0;
}

template <class T>
double divide (const object_ptr <T> &lhs, const object_ptr <T> &rhs) {
  php_warning ("Unsupported operand types for operator '/' object and object");
  return 0.0;
}

template <class T, class T1>
double divide (const array <T> &lhs, const array <T1> &rhs) {
  php_warning ("Unsupported operand types for operator '/' array and array");
  return 0.0;
}

template <class T, class T1>
double divide (const object_ptr <T> &lhs, const object_ptr <T1> &rhs) {
  php_warning ("Unsupported operand types for operator '/' object and object");
  return 0.0;
}

template <class T, class T1>
double divide (const array <T> &lhs, const object_ptr <T1> &rhs) {
  php_warning ("Unsupported operand types for operator '/' array and object");
  return 0.0;
}

template <class T, class T1>
double divide (const object_ptr <T1> &lhs, const array <T> &rhs) {
  php_warning ("Unsupported operand types for operator '/' object and array");
  return 0.0;
}


int modulo (int lhs, int rhs) {
  if (rhs == 0) {
    php_warning ("Modulo by zero");
    return 0;
  }
  return lhs % rhs;
}

template <class T1, class T2>
int modulo (const T1 &lhs, const T2 &rhs) {
  int div = f$intval (lhs);
  int mod = f$intval (rhs);

  if (neq2 (div, lhs)) {
    php_warning ("First parameter of operator %% is not an integer");
  }
  if (neq2 (mod, rhs)) {
    php_warning ("Second parameter of operator %% is not an integer");
  }

  if (mod == 0) {
    php_warning ("Modulo by zero");
    return 0;
  }
  return div % mod;
}


template <class T1, class T2>
T1 &divide_self (T1 &lhs, const T2 &rhs) {
  return lhs = divide (lhs, rhs);
}


int &modulo_self (int &lhs, int rhs) {
  return lhs = modulo (lhs, rhs);
}

template <class T1, class T2>
T1 &modulo_self (T1 &lhs, const T2 &rhs) {
  return lhs = modulo (lhs, rhs);
}


template <class T0, class T>
void assign (T0 &dest, const T &from) {
  dest = from;
}


const bool &f$boolval (const bool &val) {
  return val;
}

bool f$boolval (const int &val) {
  return val;
}

bool f$boolval (const double &val) {
  return val;
}

bool f$boolval (const string &val) {
  return val.to_bool();
}

template <class T>
bool f$boolval (const array <T> &val) {
  return !val.empty();
}

template <class T>
bool f$boolval (const object_ptr <T> &val) {
  return true;
}

bool f$boolval (const var &val) {
  return val.to_bool();
}


int f$intval (const bool &val) {
  return val;
}

const int &f$intval (const int &val) {
  return val;
}

int f$intval (const double &val) {
  return (int)val;
}

int f$intval (const string &val) {
  return val.to_int();
}

template <class T>
int f$intval (const array <T> &val) {
  php_warning ("Wrong convertion from array to int");
  return val.to_int();
}

template <class T>
int f$intval (const object_ptr <T> &val) {
  php_warning ("Wrong convertion from object to int");
  return 1;
}

int f$intval (const var &val) {
  return val.to_int();
}


int f$safe_intval (const bool &val) {
  return val;
}

const int &f$safe_intval (const int &val) {
  return val;
}

int f$safe_intval (const double &val) {
  if (fabs (val) > 2147483648) {
    php_warning ("Wrong convertion from double %.6lf to int", val);
  }
  return (int)val;
}

int f$safe_intval (const string &val) {
  return val.safe_to_int();
}

template <class T>
int f$safe_intval (const array <T> &val) {
  php_warning ("Wrong convertion from array to int");
  return val.to_int();
}

template <class T>
int f$safe_intval (const object_ptr <T> &val) {
  php_warning ("Wrong convertion from object to int");
  return 1;
}

int f$safe_intval (const var &val) {
  return val.safe_to_int();
}


double f$floatval (const bool &val) {
  return val;
}

double f$floatval (const int &val) {
  return val;
}

const double &f$floatval (const double &val) {
  return val;
}

double f$floatval (const string &val) {
  return val.to_float();
}

template <class T>
double f$floatval (const array <T> &val) {
  php_warning ("Wrong convertion from array to float");
  return val.to_float();
}

template <class T>
double f$floatval (const object_ptr <T> &val) {
  php_warning ("Wrong convertion from object to float");
  return 1.0;
}

double f$floatval (const var &val) {
  return val.to_float();
}


string f$strval (const bool &val) {
  return (val ? string ("1", 1) : string());
}

string f$strval (const int &val) {
  return string (val);
}

string f$strval (const double &val) {
  return string (val);
}

string f$strval (const string &val) {
  return val;
}

template <class T>
string f$strval (const array <T> &val) {
  php_warning ("Convertion from array to string");
  return string ("Array", 5);
}

template <class T>
string f$strval (const object_ptr <T> &val) {
  return val.to_string();
}

string f$strval (const var &val) {
  return val.to_string();
}


template <class T>
array <var> f$arrayval (const T &val) {
  array <var> res (array_size (1, 0, true));
  res.push_back (val);
  return res;
}

template <class T>
const array <T> &f$arrayval (const array <T> &val) {
  return val;
}

template <class T>
array <var> f$arrayval (const object_ptr <T> &val) {
  return val.to_array();
}

array <var> f$arrayval (const var &val) {
  return val.to_array();
}


template <class T>
inline object f$objectval (const T &val) {
  object res;
  res.set (string ("scalar", 6), val);
  return res;
}

inline const object &f$objectval (const object &val) {
  return val;
}

template <class T>
inline object f$objectval (const object_ptr <T> &val) {
  return val;
}

template <class T>
inline object f$objectval (const array <T> &val) {
  return val.to_object();
}

inline object f$objectval (const var &val) {
  return val.to_object();
}


bool& boolval_ref (bool &val) {
  return val;
}

bool& boolval_ref (var &val) {
  return val.as_bool ("unknown", -1);
}

const bool& boolval_ref (const bool &val) {
  return val;
}

const bool& boolval_ref (const var &val) {
  return val.as_bool ("unknown", -1);
}


int& intval_ref (int &val) {
  return val;
}

int& intval_ref (var &val) {
  return val.as_int ("unknown", -1);
}

const int& intval_ref (const int &val) {
  return val;
}

const int& intval_ref (const var &val) {
  return val.as_int ("unknown", -1);
}


double& floatval_ref (double &val) {
  return val;
}

double& floatval_ref (var &val) {
  return val.as_float ("unknown", -1);
}

const double& floatval_ref (const double &val) {
  return val;
}

const double& floatval_ref (const var &val) {
  return val.as_float ("unknown", -1);
}


string& strval_ref (string &val) {
  return val;
}

string& strval_ref (var &val) {
  return val.as_string ("unknown", -1);
}

const string& strval_ref (const string &val) {
  return val;
}

const string& strval_ref (const var &val) {
  return val.as_string ("unknown", -1);
}


template <class T>
array <T>& arrayval_ref (array <T> &val, const char *function, int parameter_num) {
  return val;
}

array <var>& arrayval_ref (var &val, const char *function, int parameter_num) {
  return val.as_array (function, parameter_num);
}

template <class T>
const array <T>& arrayval_ref (const array <T> &val, const char *function, int parameter_num) {
  return val;
}

const array <var>& arrayval_ref (const var &val, const char *function, int parameter_num) {
  return val.as_array (function, parameter_num);
}


template <class T>
bool f$boolval (const OrFalse <T> &v) {
  return likely (v.bool_value) ? f$boolval (v.value) : false;
}

template <class T>
bool eq2 (const OrFalse <T> &v, bool value) {
  return likely (v.bool_value) ? eq2 (v.value, value) : eq2 (false, value);
}

template <class T>
bool eq2 (bool value, const OrFalse <T> &v) {
  return likely (v.bool_value) ? eq2 (v.value, value) : eq2 (false, value);
}

template <class T>
bool eq2 (const OrFalse <T> &v, const OrFalse <T> &value) {
  return likely (v.bool_value) ? eq2 (v.value, value) : eq2 (false, value);
}

template <class T, class T1>
bool eq2 (const OrFalse <T> &v, const OrFalse <T1> &value) {
  return likely (v.bool_value) ? eq2 (v.value, value) : eq2 (false, value);
}

template <class T, class T1>
bool eq2 (const OrFalse <T> &v, const T1 &value) {
  return likely (v.bool_value) ? eq2 (v.value, value) : eq2 (false, value);
}

template <class T, class T1>
bool eq2 (const T1 &value, const OrFalse <T> &v) {
  return likely (v.bool_value) ? eq2 (v.value, value) : eq2 (false, value);
}

template <class T>
bool equals (const OrFalse <T> &value, const OrFalse <T> &v) {
  return likely (v.bool_value) ? equals (v.value, value) : equals (false, value);
}

template <class T, class T1>
bool equals (const OrFalse <T1> &value, const OrFalse <T> &v) {
  return likely (v.bool_value) ? equals (v.value, value) : equals (false, value);
}

template <class T, class T1>
bool equals (const T1 &value, const OrFalse <T> &v) {
  return likely (v.bool_value) ? equals (v.value, value) : equals (false, value);
}

template <class T, class T1>
bool equals (const OrFalse <T> &v, const T1 &value) {
  return likely (v.bool_value) ? equals (v.value, value) : equals (false, value);
}


template <class T>
const T& convert_to <T>::convert (const T &val) {
  return val;
}

template <class T>
T convert_to <T>::convert (const Unknown &val) {
  return T();
}

template <class T>
template <class T1>
T convert_to <T>::convert (const T1 &val) {
  return val;
}

template <>
template <class T1>
bool convert_to <bool>::convert (const T1 &val) {
  return f$boolval (val);
}

template <>
template <class T1>
int convert_to <int>::convert (const T1 &val) {
  return f$intval (val);
}

template <>
template <class T1>
double convert_to <double>::convert (const T1 &val) {
  return f$floatval (val);
}

template <>
template <class T1>
string convert_to <string>::convert (const T1 &val) {
  return f$strval (val);
}

template <>
template <class T1>
array <var> convert_to <array <var> >::convert (const T1 &val) {
  return f$arrayval (val);
}

template <>
template <class T1>
object convert_to <object>::convert (const T1 &val) {
  return f$objectval (val);
}

template <>
template <class T1>
var convert_to <var>::convert (const T1 &val) {
  return var (val);
}


bool f$empty (const bool &v) {
  return !v;
}

bool f$empty (const int &v) {
  return v == 0;
}

bool f$empty (const double &v) {
  return v == 0.0;
}

bool f$empty (const string &v) {
  int l = v.size();
  return l == 0 || (l == 1 && v[0] == '0');
}

template <class T>
bool f$empty (const array <T> &a) {
  return a.empty();
}

template <class T>
bool f$empty (const object_ptr <T> &a) {
  return false;
}

bool f$empty (const var &v) {
  return v.empty();
}


int f$count (const var &v) {
  return v.count();
}

template <class T>
int f$count (const array <T> &a) {
  return a.count();
}

template <class T>
int f$count (const T &v) {
  php_warning ("Count on non-array");
  return 1;
}

template <class T>
int f$sizeof (const T &v) {
  return f$count (v);
}


bool f$is_numeric (const bool &v) {
  (void)v;
  return false;
}

bool f$is_numeric (const int &v) {
  (void)v;
  return true;
}

bool f$is_numeric (const double &v) {
  (void)v;
  return true;
}

bool f$is_numeric (const string &v) {
  return v.is_numeric();
}

bool f$is_numeric (const var &v) {
  return v.is_numeric();
}

template <class T>
bool f$is_numeric (const array <T> &v) {
  (void)v;
  return false;
}

template <class T>
bool f$is_numeric (const object_ptr <T> &v) {
  (void)v;
  return false;
}


bool f$is_null (const bool &v) {
  (void)v;
  return false;
}

bool f$is_null (const int &v) {
  (void)v;
  return false;
}

bool f$is_null (const double &v) {
  (void)v;
  return false;
}

bool f$is_null (const string &v) {
  (void)v;
  return false;
}

bool f$is_null (const var &v) {
  return v.is_null();
}

template <class T>
bool f$is_null (const array <T> &v) {
  (void)v;
  return false;
}

template <class T>
bool f$is_null (const object_ptr <T> &v) {
  (void)v;
  return false;
}


bool f$is_bool (const bool &v) {
  (void)v;
  return true;
}

bool f$is_bool (const int &v) {
  (void)v;
  return false;
}

bool f$is_bool (const double &v) {
  (void)v;
  return false;
}

bool f$is_bool (const string &v) {
  (void)v;
  return false;
}

bool f$is_bool (const var &v) {
  return v.is_bool();
}

template <class T>
bool f$is_bool (const array <T> &v) {
  (void)v;
  return false;
}

template <class T>
bool f$is_bool (const object_ptr <T> &v) {
  (void)v;
  return false;
}


bool f$is_int (const bool &v) {
  (void)v;
  return false;
}

bool f$is_int (const int &v) {
  (void)v;
  return true;
}

bool f$is_int (const double &v) {
  (void)v;
  return false;
}

bool f$is_int (const string &v) {
  (void)v;
  return false;
}

bool f$is_int (const var &v) {
  return v.is_int();
}

template <class T>
bool f$is_int (const array <T> &v) {
  (void)v;
  return false;
}

template <class T>
bool f$is_int (const object_ptr <T> &v) {
  (void)v;
  return false;
}


bool f$is_float (const bool &v) {
  (void)v;
  return false;
}

bool f$is_float (const int &v) {
  (void)v;
  return false;
}

bool f$is_float (const double &v) {
  (void)v;
  return true;
}

bool f$is_float (const string &v) {
  (void)v;
  return false;
}

bool f$is_float (const var &v) {
  (void)v;
  return v.is_float();
}

template <class T>
bool f$is_float (const array <T> &v) {
  (void)v;
  return false;
}

template <class T>
bool f$is_float (const object_ptr <T> &v) {
  (void)v;
  return false;
}


bool f$is_scalar (const bool &v) {
  (void)v;
  return true;
}

bool f$is_scalar (const int &v) {
  (void)v;
  return true;
}

bool f$is_scalar (const double &v) {
  (void)v;
  return true;
}

bool f$is_scalar (const string &v) {
  (void)v;
  return true;
}

bool f$is_scalar (const var &v) {
  return v.is_scalar();
}

template <class T>
bool f$is_scalar (const array <T> &v) {
  (void)v;
  return false;
}

template <class T>
bool f$is_scalar (const object_ptr <T> &v) {
  (void)v;
  return false;
}


bool f$is_string (const bool &v) {
  (void)v;
  return false;
}

bool f$is_string (const int &v) {
  (void)v;
  return false;
}

bool f$is_string (const double &v) {
  (void)v;
  return false;
}

bool f$is_string (const string &v) {
  (void)v;
  return true;
}

bool f$is_string (const var &v) {
  return v.is_string();
}

template <class T>
bool f$is_string (const array <T> &v) {
  (void)v;
  return false;
}

template <class T>
bool f$is_string (const object_ptr <T> &v) {
  (void)v;
  return false;
}


bool f$is_array (const bool &v) {
  (void)v;
  return false;
}

bool f$is_array (const int &v) {
  (void)v;
  return false;
}

bool f$is_array (const double &v) {
  (void)v;
  return false;
}

bool f$is_array (const string &v) {
  (void)v;
  return false;
}

bool f$is_array (const var &v) {
  return v.is_array();
}

template <class T>
bool f$is_array (const array <T> &v) {
  (void)v;
  return true;
}

template <class T>
bool f$is_array (const object_ptr <T> &v) {
  (void)v;
  return false;
}


bool f$is_object (const bool &v) {
  (void)v;
  return false;
}

bool f$is_object (const int &v) {
  (void)v;
  return false;
}

bool f$is_object (const double &v) {
  (void)v;
  return false;
}

bool f$is_object (const string &v) {
  (void)v;
  return false;
}

bool f$is_object (const var &v) {
  return v.is_object();
}

template <class T>
bool f$is_object (const array <T> &v) {
  (void)v;
  return false;
}

template <class T>
bool f$is_object (const object_ptr <T> &v) {
  (void)v;
  return true;
}


template <class T>
bool f$is_integer (const T &v) {
  return f$is_int (v);
}

template <class T>
bool f$is_long (const T &v) {
  return f$is_int (v);
}

template <class T>
bool f$is_double (const T &v) {
  return f$is_float (v);
}

template <class T>
bool f$is_real (const T &v) {
  return f$is_float (v);
}


const char *get_type_c_str (const bool &v) {
  (void)v;
  return "boolean";
}

const char *get_type_c_str (const int &v) {
  (void)v;
  return "integer";
}

const char *get_type_c_str (const double &v) {
  (void)v;
  return "double";
}

const char *get_type_c_str (const string &v) {
  (void)v;
  return "string";
}

const char *get_type_c_str (const var &v) {
  return v.get_type_c_str();
}

template <class T>
const char *get_type_c_str (const array <T> &v) {
  (void)v;
  return "array";
}

template <class T>
const char *get_type_c_str (const object_ptr <T> &v) {
  (void)v;
  return "object";
}


template <class T>
string f$get_type (const T &v) {
  char *res = get_type_c_str (v);
  return string (res, strlen (res));
}


const string get_value (const string &v, int int_key) {
  return v.get_value (int_key);
}

const string get_value (const string &v, const string &string_key) {
  return v.get_value (string_key);
}

const string get_value (const string &v, const var &key) {
  return v.get_value (key);
}

const var get_value (const var &v, int int_key) {
  return v.get_value (int_key);
}

const var get_value (const var &v, const string &string_key) {
  return v.get_value (string_key);
}

const var get_value (const var &v, const var &key) {
  return v.get_value (key);
}

template <class T>
const T get_value (const array <T> &v, int int_key) {
  return v.get_value (int_key);
}

template <class T>
const T get_value (const array <T> &v, const string &string_key) {
  return v.get_value (string_key);
}

template <class T>
const T get_value (const array <T> &v, const var &key) {
  return v.get_value (key);
}

const string get_value (const OrFalse <string> &v, int int_key) {
  return v.val().get_value (int_key);
}

const string get_value (const OrFalse <string> &v, const string &string_key) {
  return v.val().get_value (string_key);
}

const string get_value (const OrFalse <string> &v, const var &key) {
  return v.val().get_value (key);
}

template <class T>
const T get_value (const OrFalse <array <T> > &v, int int_key) {
  return v.val().get_value (int_key);
}

template <class T>
const T get_value (const OrFalse <array <T> > &v, const string &string_key) {
  return v.val().get_value (string_key);
}

template <class T>
const T get_value (const OrFalse <array <T> > &v, const var &key) {
  return v.val().get_value (key);
}

template <class T>
const var get_value (const T &v, int int_key) {
  (void)int_key;
  php_warning ("Cannot use a value of type %s as an array", get_type_c_str (v));
  return var();
}

template <class T>
const var get_value (const T &v, const string &string_key) {
  (void)string_key;
  php_warning ("Cannot use a value of type %s as an array", get_type_c_str (v));
  return var();
}

template <class T>
const var get_value (const T &v, const var &key) {
  (void)key;
  php_warning ("Cannot use a value of type %s as an array", get_type_c_str (v));
  return var();
}


string& append (string &dest, const string &from) {
  return dest.append (from);
}

template <class T>
string& append (string &dest, const T &from) {
  return dest.append (f$strval (from));
}

template <class T>
var& append (var &dest, const T &from) {
  return dest.append (f$strval (from));
}

template <class T0, class T>
T0& append (T0 &dest, const T &from) {
  php_warning ("Wrong arguments types %s and %s for operator .=", get_type_c_str (dest), get_type_c_str (from));
  return dest;
}


string f$gettype (const var &v) {
  return v.get_type();
}

template <class T>
bool f$function_exists (const T &a1) {
  return true;
}


var f$error_get_last (void) {
  return var();
}

int f$error_reporting (int level) {
  int prev = php_warning_level;
  if ((level & E_ALL) == E_ALL) {
    php_warning_level = 3;
  }
  if (0 <= level && level <= 3) {
    php_warning_level = level;
  }
  return prev;
}

int f$error_reporting (void) {
  return php_warning_level;
}

void f$warning (const string &message) {
  php_warning ("%s", message.c_str());
}

int f$memory_get_static_usage (void) {
  return (int)dl::static_memory_used;
}

int f$memory_get_peak_usage (bool real_usage) {
  if (real_usage) {
    return (int)dl::max_real_memory_used;
  } else {
    return (int)dl::max_memory_used;
  }
}

int f$memory_get_usage (bool real_usage) {
  return (int)dl::memory_used;
}

int f$memory_get_total_usage (void) {
  return (int)dl::memory_get_total_usage();
}


template <class T, class TT>
int f$get_reference_counter (const array <T, TT> &v) {
  return v.get_reference_counter();
}

template <class T>
int f$get_reference_counter (const object_ptr <T> &v) {
  return v.get_reference_counter();
}

int f$get_reference_counter (const string &v) {
  return v.get_reference_counter();
}

int f$get_reference_counter (const var &v) {
  return v.get_reference_counter();
}


template <class T>
T& val (T &x) {
  return x;
}

template <class T>
const T& val (const T &x) {
  return x;
}

template <class T>
T& ref (T &x) {
  return x;
}

template <class T>
const T& val (const OrFalse <T> &x) {
  return x.val();
}

template <class T>
T& val (OrFalse <T> &x) {
  return x.val();
}

template <class T>
T& ref (OrFalse <T> &x) {
  return x.ref();
}


template <class T, class TT>
typename array <T, TT>::iterator begin (array <T, TT> &x) {
  return x.begin();
}

template <class T, class TT>
typename array <T, TT>::const_iterator begin (const array <T, TT> &x) {
  return x.begin();
}

array <var, var>::iterator begin (var &x) {
  return x.begin();
}

array <var, var>::const_iterator begin (const var &x) {
  return x.begin();
}

template <class T, class TT>
typename array <T, TT>::iterator begin (OrFalse < array <T, TT> > &x) {
  if (!x.bool_value) {
    php_warning ("Invalid argument supplied for foreach(), false is given");
  }
  return x.value.begin();
}

template <class T, class TT>
typename array <T, TT>::const_iterator begin (const OrFalse < array <T, TT> > &x) {
  if (!x.bool_value) {
    php_warning ("Invalid argument supplied for foreach(), false is given");
  }
  return x.value.begin();
}

template <class T, class TT>
typename array <T, TT>::iterator end (array <T, TT> &x) {
  return x.end();
}

template <class T, class TT>
typename array <T, TT>::const_iterator end (const array <T, TT> &x) {
  return x.end();
}

array <var, var>::iterator end (var &x) {
  return x.end();
}

array <var, var>::const_iterator end (const var &x) {
  return x.end();
}

template <class T, class TT>
typename array <T, TT>::iterator end (OrFalse < array <T, TT> > &x) {
  if (!x.bool_value) {
    php_warning ("Invalid argument supplied for foreach(), false is given");
  }
  return x.value.end();
}

template <class T, class TT>
typename array <T, TT>::const_iterator end (const OrFalse < array <T, TT> > &x) {
  if (!x.bool_value) {
    php_warning ("Invalid argument supplied for foreach(), false is given");
  }
  return x.value.end();
}


var& clean_array (var &v) {
  v = array <var> ();
  return v;
}

template <class T>
array <T>& clean_array (array <T> &a) {
  return a = array <T> ();
}

template <class T>
OrFalse <array <T> >& clean_array (OrFalse <array <T> > &a) {
  return a = array <T> ();
}

template <class T>
void unset (array <T> &x) {
  clean_array (x);
}

void unset (var &x) {
  x = var();
}

