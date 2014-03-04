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

bool empty_bool __attribute__ ((weak));
int empty_int __attribute__ ((weak));
double empty_float __attribute__ ((weak));
string empty_string __attribute__ ((weak));

char empty_array_var_storage[sizeof (array <var>)] __attribute__ ((weak));
array <var> *empty_array_var __attribute__ ((weak)) = reinterpret_cast <array <var> *> (empty_array_var_storage);

var empty_var __attribute__ ((weak));

void var::copy_from (const var &other) {
  switch (other.type) {
    case NULL_TYPE:
      break;
    case BOOLEAN_TYPE:
      b = other.b;
      break;
    case INTEGER_TYPE:
      i = other.i;
      break;
    case FLOAT_TYPE:
      f = other.f;
      break;
    case STRING_TYPE:
      new (&s) string (*STRING(other.s));
      break;
    case ARRAY_TYPE:
      new (&a) array <var> (*ARRAY(other.a));
      break;
    case OBJECT_TYPE:
      new (&o) object (*OBJECT(other.o));
      break;
  }
  type = other.type;
}

var::var (void): type (NULL_TYPE) {
}

var::var (const Unknown &u): type (NULL_TYPE) {
  php_assert ("Unknown used!!!" && 0);
}

var::var (bool b): type (BOOLEAN_TYPE),
                   b (b) {
}

var::var (int i): type (INTEGER_TYPE),
                  i (i) {
}

var::var (double f): type (FLOAT_TYPE),
                     f (f) {
}

var::var (const string &s_): type (STRING_TYPE) {
  new (&s) string (s_);
}

var::var (const char *s_, int len): type (STRING_TYPE) {
  new (&s) string (s_, len);
}

template <class T>
var::var (const array <T> &a_): type (ARRAY_TYPE) {
  new (&a) array <var> (a_);
}

template <class T>
var::var (const object_ptr <T> &o_): type (OBJECT_TYPE) {
  new (&o) object (o_);
}


var::var (const OrFalse <int> &v) {
  if (likely (v.bool_value)) {
    type = INTEGER_TYPE;
    i = v.value;
  } else {
    type = BOOLEAN_TYPE;
    b = false;
  }
}

var::var (const OrFalse <double> &v) {
  if (likely (v.bool_value)) {
    type = FLOAT_TYPE;
    f = v.value;
  } else {
    type = BOOLEAN_TYPE;
    b = false;
  }
}

var::var (const OrFalse <string> &v) {
  if (likely (v.bool_value)) {
    type = STRING_TYPE;
    new (&s) string (v.value);
  } else {
    type = BOOLEAN_TYPE;
    b = false;
  }
}

template <class T>
var::var (const OrFalse <array <T> > &v) {
  if (likely (v.bool_value)) {
    type = ARRAY_TYPE;
    new (&a) array <var> (v.value);
  } else {
    type = BOOLEAN_TYPE;
    b = false;
  }
}

template <class T>
var::var (const OrFalse <object_ptr <T> > &v) {
  if (likely (v.bool_value)) {
    type = OBJECT_TYPE;
    new (&o) object (v.value);
  } else {
    type = BOOLEAN_TYPE;
    b = false;
  }
}


var::var (const var &v) {
  copy_from (v);
}

var& var::operator = (bool other) {
  switch (type) {
    case NULL_TYPE:
      type = BOOLEAN_TYPE;
      b = other;
      return *this;
    case BOOLEAN_TYPE:
      b = other;
      return *this;
    case INTEGER_TYPE:
      type = BOOLEAN_TYPE;
      b = other;
      return *this;
    case FLOAT_TYPE:
      type = BOOLEAN_TYPE;
      b = other;
      return *this;
    case STRING_TYPE:
      STRING(s)->~string();
      type = BOOLEAN_TYPE;
      b = other;
      return *this;
    case ARRAY_TYPE:
      ARRAY(a)->~array <var>();
      type = BOOLEAN_TYPE;
      b = other;
      return *this;
    case OBJECT_TYPE:
      OBJECT(o)->~object();
      type = BOOLEAN_TYPE;
      b = other;
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

var& var::operator = (int other) {
  switch (type) {
    case NULL_TYPE:
      type = INTEGER_TYPE;
      i = other;
      return *this;
    case BOOLEAN_TYPE:
      type = INTEGER_TYPE;
      i = other;
      return *this;
    case INTEGER_TYPE:
      i = other;
      return *this;
    case FLOAT_TYPE:
      type = INTEGER_TYPE;
      i = other;
      return *this;
    case STRING_TYPE:
      STRING(s)->~string();
      type = INTEGER_TYPE;
      i = other;
      return *this;
    case ARRAY_TYPE:
      ARRAY(a)->~array <var>();
      type = INTEGER_TYPE;
      i = other;
      return *this;
    case OBJECT_TYPE:
      OBJECT(o)->~object();
      type = INTEGER_TYPE;
      i = other;
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

var& var::operator = (double other) {
  switch (type) {
    case NULL_TYPE:
      type = FLOAT_TYPE;
      f = other;
      return *this;
    case BOOLEAN_TYPE:
      type = FLOAT_TYPE;
      f = other;
      return *this;
    case INTEGER_TYPE:
      type = FLOAT_TYPE;
      f = other;
      return *this;
    case FLOAT_TYPE:
      f = other;
      return *this;
    case STRING_TYPE:
      STRING(s)->~string();
      type = FLOAT_TYPE;
      f = other;
      return *this;
    case ARRAY_TYPE:
      ARRAY(a)->~array <var>();
      type = FLOAT_TYPE;
      f = other;
      return *this;
    case OBJECT_TYPE:
      OBJECT(o)->~object();
      type = FLOAT_TYPE;
      f = other;
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

var& var::operator = (const string &other) {
  switch (type) {
    case NULL_TYPE:
      type = STRING_TYPE;
      new (&s) string (other);
      return *this;
    case BOOLEAN_TYPE:
      type = STRING_TYPE;
      new (&s) string (other);
      return *this;
    case INTEGER_TYPE:
      type = STRING_TYPE;
      new (&s) string (other);
      return *this;
    case FLOAT_TYPE:
      type = STRING_TYPE;
      new (&s) string (other);
      return *this;
    case STRING_TYPE:
      *STRING(s) = other;
      return *this;
    case ARRAY_TYPE:
      ARRAY(a)->~array <var>();
      type = STRING_TYPE;
      new (&s) string (other);
      return *this;
    case OBJECT_TYPE:
      OBJECT(o)->~object();
      type = STRING_TYPE;
      new (&s) string (other);
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

var& var::assign (const char *other, int len) {
  switch (type) {
    case NULL_TYPE:
      type = STRING_TYPE;
      new (&s) string (other, len);
      return *this;
    case BOOLEAN_TYPE:
      type = STRING_TYPE;
      new (&s) string (other, len);
      return *this;
    case INTEGER_TYPE:
      type = STRING_TYPE;
      new (&s) string (other, len);
      return *this;
    case FLOAT_TYPE:
      type = STRING_TYPE;
      new (&s) string (other, len);
      return *this;
    case STRING_TYPE:
      STRING(s)->assign (other, len);
      return *this;
    case ARRAY_TYPE:
      ARRAY(a)->~array <var>();
      type = STRING_TYPE;
      new (&s) string (other, len);
      return *this;
    case OBJECT_TYPE:
      OBJECT(o)->~object();
      type = STRING_TYPE;
      new (&s) string (other, len);
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

template <class T>
var& var::operator = (const array <T> &other) {
  switch (type) {
    case NULL_TYPE:
      type = ARRAY_TYPE;
      new (&a) array <var> (other);
      return *this;
    case BOOLEAN_TYPE:
      type = ARRAY_TYPE;
      new (&a) array <var> (other);
      return *this;
    case INTEGER_TYPE:
      type = ARRAY_TYPE;
      new (&a) array <var> (other);
      return *this;
    case FLOAT_TYPE:
      type = ARRAY_TYPE;
      new (&a) array <var> (other);
      return *this;
    case STRING_TYPE:
      STRING(s)->~string();
      type = ARRAY_TYPE;
      new (&a) array <var> (other);
      return *this;
    case ARRAY_TYPE:
      *ARRAY(a) = other;
      return *this;
    case OBJECT_TYPE:
      OBJECT(o)->~object();
      type = ARRAY_TYPE;
      new (&a) array <var> (other);
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

template <class T>
var& var::operator = (const object_ptr <T> &other) {
  switch (type) {
    case NULL_TYPE:
      type = OBJECT_TYPE;
      new (&o) object (other);
      return *this;
    case BOOLEAN_TYPE:
      type = OBJECT_TYPE;
      new (&o) object (other);
      return *this;
    case INTEGER_TYPE:
      type = OBJECT_TYPE;
      new (&o) object (other);
      return *this;
    case FLOAT_TYPE:
      type = OBJECT_TYPE;
      new (&o) object (other);
      return *this;
    case STRING_TYPE:
      STRING(s)->~string();
      type = OBJECT_TYPE;
      new (&o) object (other);
      return *this;
    case ARRAY_TYPE:
      ARRAY(a)->~array <var>();
      type = OBJECT_TYPE;
      new (&o) object (other);
      return *this;
    case OBJECT_TYPE:
      *OBJECT(o) = other;
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

var& var::operator = (const var &other) {
  if (this != &other) {
    destroy();
    copy_from (other);
  }
  return *this;
/*
  switch (other.type) {
    case NULL_TYPE:
      destroy();
      type = NULL_TYPE;
      return *this;
    case BOOLEAN_TYPE:
      return *this = other.b;
    case INTEGER_TYPE:
      return *this = other.i;
    case FLOAT_TYPE:
      return *this = other.f;
    case STRING_TYPE:
      return *this = *STRING(other.s);
    case ARRAY_TYPE:
      return *this = *ARRAY(other.a);
    case OBJECT_TYPE:
      return *this = *OBJECT(other.o);
    default:
      php_assert (0);
      exit (1);
  }*/
}



var& var::operator = (const OrFalse <int> &other) {
  destroy();

  if (likely (other.bool_value)) {
    type = INTEGER_TYPE;
    i = other.value;
  } else {
    type = BOOLEAN_TYPE;
    b = false;
  }

  return *this;
}

var& var::operator = (const OrFalse <double> &other) {
  destroy();

  if (likely (other.bool_value)) {
    type = FLOAT_TYPE;
    f = other.value;
  } else {
    type = BOOLEAN_TYPE;
    b = false;
  }

  return *this;
}

var& var::operator = (const OrFalse <string> &other) {
  if (likely (other.bool_value)) {
    *this = other.value;
  } else {
    destroy();
    type = BOOLEAN_TYPE;
    b = false;
  }
  return *this;
}

template <class T>
var& var::operator = (const OrFalse <array <T> > &other) {
  if (likely (other.bool_value)) {
    *this = other.value;
  } else {
    destroy();
    type = BOOLEAN_TYPE;
    b = false;
  }
  return *this;
}

template <class T>
var& var::operator = (const OrFalse <object_ptr <T> > &other) {
  if (likely (other.bool_value)) {
    *this = other.value;
  } else {
    destroy();
    type = BOOLEAN_TYPE;
    b = false;
  }
  return *this;
}


const var var::operator - (void) const {
  var arg1 = to_numeric();

  if (arg1.type == INTEGER_TYPE) {
    arg1.i = -arg1.i;
  } else {
    arg1.f = -arg1.f;
  }
  return arg1;
}

const var var::operator + (void) const {
  return to_numeric();
}


int var::operator ~(void) const {
  return ~to_int();
}


var& var::operator += (const var &other) {
  if (likely (type == INTEGER_TYPE && other.type == INTEGER_TYPE)) {
    i += other.i;
    return *this;
  }

  if (unlikely (type == ARRAY_TYPE || other.type == ARRAY_TYPE)) {
    if (type == ARRAY_TYPE && other.type == ARRAY_TYPE) {
      *ARRAY(a) += *ARRAY(other.a);
    } else {
      php_warning ("Unsupported operand types for operator += (%s and %s)", get_type_c_str(), other.get_type_c_str());
    }
    return *this;
  }

  convert_to_numeric();
  const var arg2 = other.to_numeric();

  if (type == INTEGER_TYPE) {
    if (arg2.type == INTEGER_TYPE) {
      i += arg2.i;
    } else {
      type = FLOAT_TYPE;
      f = i + arg2.f;
    }
  } else {
    if (arg2.type == INTEGER_TYPE) {
      f += arg2.i;
    } else {
      f += arg2.f;
    }
  }

  return *this;
}

var& var::operator -= (const var &other) {
  if (likely (type == INTEGER_TYPE && other.type == INTEGER_TYPE)) {
    i -= other.i;
    return *this;
  }

  convert_to_numeric();
  const var arg2 = other.to_numeric();

  if (type == INTEGER_TYPE) {
    if (arg2.type == INTEGER_TYPE) {
      i -= arg2.i;
    } else {
      type = FLOAT_TYPE;
      f = i - arg2.f;
    }
  } else {
    if (arg2.type == INTEGER_TYPE) {
      f -= arg2.i;
    } else {
      f -= arg2.f;
    }
  }

  return *this;
}

var& var::operator *= (const var &other) {
  if (likely (type == INTEGER_TYPE && other.type == INTEGER_TYPE)) {
    i *= other.i;
    return *this;
  }

  convert_to_numeric();
  const var arg2 = other.to_numeric();

  if (type == INTEGER_TYPE) {
    if (arg2.type == INTEGER_TYPE) {
      i *= arg2.i;
    } else {
      type = FLOAT_TYPE;
      f = i * arg2.f;
    }
  } else {
    if (arg2.type == INTEGER_TYPE) {
      f *= arg2.i;
    } else {
      f *= arg2.f;
    }
  }

  return *this;
}

var& var::operator /= (const var &other) {
  if (likely (type == INTEGER_TYPE && other.type == INTEGER_TYPE)) {
    if (i % other.i == 0) {
      i /= other.i;
    } else {
      type = FLOAT_TYPE;
      f = (double)i / other.i;
    }
    return *this;
  }

  convert_to_numeric();
  const var arg2 = other.to_numeric();

  if (arg2.type == INTEGER_TYPE) {
    if (arg2.i == 0) {
      php_warning ("Integer division by zero");
      type = BOOLEAN_TYPE;
      b = false;
      return *this;
    }

    if (type == INTEGER_TYPE) {
      if (i % arg2.i == 0) {
        i /= arg2.i;
      } else {
        type = FLOAT_TYPE;
        f = (double)i / other.i;
      }
    } else {
      f /= arg2.i;
    }
  } else {
    if (arg2.f == 0) {
      php_warning ("Float division by zero");
      type = BOOLEAN_TYPE;
      b = false;
      return *this;
    }

    if (type == INTEGER_TYPE) {
      type = FLOAT_TYPE;
      f = i / arg2.f;
    } else {
      f /= arg2.f;
    }
  }

  return *this;
}

var& var::operator %= (const var &other) {
  int div = other.to_int();
  if (div == 0) {
    php_warning ("Modulo by zero");
    *this = false;
    return *this;
  }
  convert_to_int();
  i %= div;

  return *this;
}


var& var::operator &= (const var &other) {
  convert_to_int();
  i &= other.to_int();
  return *this;
}

var& var::operator |= (const var &other) {
  convert_to_int();
  i |= other.to_int();
  return *this;
}

var& var::operator ^= (const var &other) {
  convert_to_int();
  i ^= other.to_int();
  return *this;
}

var& var::operator <<= (const var &other) {
  convert_to_int();
  i <<= other.to_int();
  return *this;
}

var& var::operator >>= (const var &other) {
  convert_to_int();
  i >>= other.to_int();
  return *this;
}


var& var::safe_set_add (const var &other) {
  if (likely (type == INTEGER_TYPE && other.type == INTEGER_TYPE)) {
    ::safe_set_add (i, other.i);
    return *this;
  }

  if (unlikely (type == ARRAY_TYPE || other.type == ARRAY_TYPE)) {
    if (type == ARRAY_TYPE && other.type == ARRAY_TYPE) {
      *ARRAY(a) += *ARRAY(other.a);
    } else {
      php_warning ("Unsupported operand types for operator += (%s and %s)", get_type_c_str(), other.get_type_c_str());
    }
    return *this;
  }

  convert_to_numeric();
  const var arg2 = other.to_numeric();

  if (type == INTEGER_TYPE) {
    if (arg2.type == INTEGER_TYPE) {
      ::safe_set_add (i, arg2.i);
    } else {
      type = FLOAT_TYPE;
      f = i + arg2.f;
    }
  } else {
    if (arg2.type == INTEGER_TYPE) {
      f += arg2.i;
    } else {
      f += arg2.f;
    }
  }

  return *this;
}

var& var::safe_set_sub (const var &other) {
  if (likely (type == INTEGER_TYPE && other.type == INTEGER_TYPE)) {
    ::safe_set_sub (i, other.i);
    return *this;
  }

  convert_to_numeric();
  const var arg2 = other.to_numeric();

  if (type == INTEGER_TYPE) {
    if (arg2.type == INTEGER_TYPE) {
      ::safe_set_sub (i, arg2.i);
    } else {
      type = FLOAT_TYPE;
      f = i - arg2.f;
    }
  } else {
    if (arg2.type == INTEGER_TYPE) {
      f -= arg2.i;
    } else {
      f -= arg2.f;
    }
  }

  return *this;
}

var& var::safe_set_mul (const var &other) {
  if (likely (type == INTEGER_TYPE && other.type == INTEGER_TYPE)) {
    ::safe_set_mul (i, other.i);
    return *this;
  }

  convert_to_numeric();
  const var arg2 = other.to_numeric();

  if (type == INTEGER_TYPE) {
    if (arg2.type == INTEGER_TYPE) {
      ::safe_set_mul (i, arg2.i);
    } else {
      type = FLOAT_TYPE;
      f = i * arg2.f;
    }
  } else {
    if (arg2.type == INTEGER_TYPE) {
      f *= arg2.i;
    } else {
      f *= arg2.f;
    }
  }

  return *this;
}

var& var::safe_set_shl (const var &other) {
  safe_convert_to_int();
  ::safe_set_shl (i, other.safe_to_int());
  return *this;
}


var& var::operator ++ (void) {
  switch (type) {
    case NULL_TYPE:
      type = INTEGER_TYPE;
      i = 1;
      return *this;
    case BOOLEAN_TYPE:
      php_warning ("Can't apply operator ++ to boolean");
      return *this;
    case INTEGER_TYPE:
      ++i;
      return *this;
    case FLOAT_TYPE:
      f += 1;
      return *this;
    case STRING_TYPE:
      *this = STRING(s)->to_numeric();
      return ++(*this);
    case ARRAY_TYPE:
      php_warning ("Can't apply operator ++ to array");
      return *this;
    case OBJECT_TYPE:
      php_warning ("Can't apply operator ++ to object");
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

const var var::operator ++ (int) {
  switch (type) {
    case NULL_TYPE:
      type = INTEGER_TYPE;
      i = 1;
      return var();
    case BOOLEAN_TYPE:
      php_warning ("Can't apply operator ++ to boolean");
      return b;
    case INTEGER_TYPE: {
      var res (i);
      ++i;
      return res;
    }
    case FLOAT_TYPE: {
      var res (f);
      f += 1;
      return res;
    }
    case STRING_TYPE: {
      var res (*STRING(s));
      *this = STRING(s)->to_numeric();
      (*this)++;
      return res;
    }
    case ARRAY_TYPE:
      php_warning ("Can't apply operator ++ to array");
      return *ARRAY(a);
    case OBJECT_TYPE:
      php_warning ("Can't apply operator ++ to object");
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

var& var::operator -- (void) {
  if (likely (type == INTEGER_TYPE)) {
    --i;
    return *this;
  }

  switch (type) {
    case NULL_TYPE:
      php_warning ("Can't apply operator -- to null");
      return *this;
    case BOOLEAN_TYPE:
      php_warning ("Can't apply operator -- to boolean");
      return *this;
    case INTEGER_TYPE:
      --i;
      return *this;
    case FLOAT_TYPE:
      f -= 1;
      return *this;
    case STRING_TYPE:
      *this = STRING(s)->to_numeric();
      return --(*this);
    case ARRAY_TYPE:
      php_warning ("Can't apply operator -- to array");
      return *this;
    case OBJECT_TYPE:
      php_warning ("Can't apply operator -- to object");
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

const var var::operator -- (int) {
  if (likely (type == INTEGER_TYPE)) {
    var res (i);
    --i;
    return res;
  }

  switch (type) {
    case NULL_TYPE:
      php_warning ("Can't apply operator -- to null");
      return var();
    case BOOLEAN_TYPE:
      php_warning ("Can't apply operator -- to boolean");
      return b;
    case INTEGER_TYPE: {
      var res (i);
      --i;
      return res;
    }
    case FLOAT_TYPE: {
      var res (f);
      f -= 1;
      return res;
    }
    case STRING_TYPE: {
      var res (*STRING(s));
      *this = STRING(s)->to_numeric();
      (*this)--;
      return res;
    }
    case ARRAY_TYPE:
      php_warning ("Can't apply operator -- to array");
      return *ARRAY(a);
    case OBJECT_TYPE:
      php_warning ("Can't apply operator -- to object");
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}


var& var::safe_incr_pre (void) {
  switch (type) {
    case NULL_TYPE:
      type = INTEGER_TYPE;
      i = 1;
      return *this;
    case BOOLEAN_TYPE:
      php_warning ("Can't apply operator ++ to boolean");
      return *this;
    case INTEGER_TYPE:
      ::safe_incr_pre (i);
      return *this;
    case FLOAT_TYPE:
      f += 1;
      return *this;
    case STRING_TYPE:
      *this = STRING(s)->to_numeric();
      return safe_incr_pre();
    case ARRAY_TYPE:
      php_warning ("Can't apply operator ++ to array");
      return *this;
    case OBJECT_TYPE:
      php_warning ("Can't apply operator ++ to object");
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

const var var::safe_incr_post (void) {
  switch (type) {
    case NULL_TYPE:
      type = INTEGER_TYPE;
      i = 1;
      return var();
    case BOOLEAN_TYPE:
      php_warning ("Can't apply operator ++ to boolean");
      return b;
    case INTEGER_TYPE: {
      var res (i);
      ::safe_incr_post (i);
      return res;
    }
    case FLOAT_TYPE: {
      var res (f);
      f += 1;
      return res;
    }
    case STRING_TYPE: {
      var res (*STRING(s));
      *this = STRING(s)->to_numeric();
      safe_incr_post();
      return res;
    }
    case ARRAY_TYPE:
      php_warning ("Can't apply operator ++ to array");
      return *ARRAY(a);
    case OBJECT_TYPE:
      php_warning ("Can't apply operator ++ to object");
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

var& var::safe_decr_pre (void) {
  switch (type) {
    case NULL_TYPE:
      php_warning ("Can't apply operator -- to null");
      return *this;
    case BOOLEAN_TYPE:
      php_warning ("Can't apply operator -- to boolean");
      return *this;
    case INTEGER_TYPE:
      ::safe_decr_pre (i);
      return *this;
    case FLOAT_TYPE:
      f -= 1;
      return *this;
    case STRING_TYPE:
      *this = STRING(s)->to_numeric();
      return safe_decr_pre();
    case ARRAY_TYPE:
      php_warning ("Can't apply operator -- to array");
      return *this;
    case OBJECT_TYPE:
      php_warning ("Can't apply operator -- to object");
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}

const var var::safe_decr_post (void) {
  switch (type) {
    case NULL_TYPE:
      php_warning ("Can't apply operator -- to null");
      return var();
    case BOOLEAN_TYPE:
      php_warning ("Can't apply operator -- to boolean");
      return b;
    case INTEGER_TYPE: {
      var res (i);
      ::safe_decr_post (i);
      return res;
    }
    case FLOAT_TYPE: {
      var res (f);
      f -= 1;
      return res;
    }
    case STRING_TYPE: {
      var res (*STRING(s));
      *this = STRING(s)->to_numeric();
      safe_decr_post();
      return res;
    }
    case ARRAY_TYPE:
      php_warning ("Can't apply operator -- to array");
      return *ARRAY(a);
    case OBJECT_TYPE:
      php_warning ("Can't apply operator -- to object");
      return *this;
    default:
      php_assert (0);
      exit (1);
  }
}


bool var::operator ! (void) const {
  return !to_bool();
}


var& var::append (const string &v) {
  if (unlikely (type != STRING_TYPE)) {
    convert_to_string();
  }
  STRING(s)->append (v);
  return *this;
}


void var::destroy (void) {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
      break;
    case STRING_TYPE:
      STRING(s)->~string();
      break;
    case ARRAY_TYPE:
      ARRAY(a)->~array <var>();
      break;
    case OBJECT_TYPE:
      OBJECT(o)->~object();
      break;
    default:
      php_assert (0);
      exit (1);
  }
}

var::~var (void) {
  destroy();
  type = NULL_TYPE;
}


const var var::to_numeric (void) const {
  switch (type) {
    case NULL_TYPE:
      return 0;
    case BOOLEAN_TYPE:
      return (b ? 1 : 0);
    case INTEGER_TYPE:
      return i;
    case FLOAT_TYPE:
      return f;
    case STRING_TYPE:
      return STRING(s)->to_numeric();
    case ARRAY_TYPE:
      php_warning ("Wrong convertion from array to number");
      return ARRAY(a)->to_int();
    case OBJECT_TYPE:
      php_warning ("Wrong convertion from object to number");
      return 1;
    default:
      php_assert (0);
      exit (1);
  }
}


bool var::to_bool (void) const {
  switch (type) {
    case NULL_TYPE:
      return false;
    case BOOLEAN_TYPE:
      return b;
    case INTEGER_TYPE:
      return (bool)i;
    case FLOAT_TYPE:
      return (bool)f;
    case STRING_TYPE:
      return STRING(s)->to_bool();
    case ARRAY_TYPE:
      return !ARRAY(a)->empty();
    case OBJECT_TYPE:
      return true;
    default:
      php_assert (0);
      exit (1);
  }
}

int var::to_int (void) const {
  switch (type) {
    case NULL_TYPE:
      return 0;
    case BOOLEAN_TYPE:
      return (int)b;
    case INTEGER_TYPE:
      return i;
    case FLOAT_TYPE:
      return (int)f;
    case STRING_TYPE:
      return STRING(s)->to_int();
    case ARRAY_TYPE:
      php_warning ("Wrong convertion from array to int");
      return ARRAY(a)->to_int();
    case OBJECT_TYPE:
      php_warning ("Wrong convertion from object to int");
      return 1;
    default:
      php_assert (0);
      exit (1);
  }
}

double var::to_float (void) const {
  switch (type) {
    case NULL_TYPE:
      return 0.0;
    case BOOLEAN_TYPE:
      return (b ? 1.0 : 0.0);
    case INTEGER_TYPE:
      return (double)i;
    case FLOAT_TYPE:
      return f;
    case STRING_TYPE:
      return STRING(s)->to_float();
    case ARRAY_TYPE:
      php_warning ("Wrong convertion from array to float");
      return ARRAY(a)->to_float();
    case OBJECT_TYPE:
      php_warning ("Wrong convertion from object to float");
      return 1.0;
    default:
      php_assert (0);
      exit (1);
  }
}

const string var::to_string (void) const {
  switch (type) {
    case NULL_TYPE:
      return string();
    case BOOLEAN_TYPE:
      return (b ? string ("1", 1) : string());
    case INTEGER_TYPE:
      return string (i);
    case FLOAT_TYPE:
      return string (f);
    case STRING_TYPE:
      return *STRING(s);
    case ARRAY_TYPE:
      php_warning ("Convertion from array to string");
      return string ("Array", 5);
    case OBJECT_TYPE:
      return OBJECT(o)->to_string();
    default:
      php_assert (0);
      exit (1);
  }
}

const array <var> var::to_array (void) const {
  switch (type) {
    case NULL_TYPE:
      return array <var>();
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE: {
      array <var> res (array_size (1, 0, true));
      res.push_back (*this);
      return res;
    }
    case ARRAY_TYPE:
      return *ARRAY(a);
    case OBJECT_TYPE:
      return OBJECT(o)->to_array();
    default:
      php_assert (0);
      exit (1);
  }
}

const object var::to_object (void) const {
  switch (type) {
    case NULL_TYPE:
      return object();
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE: {
      object res;
      res.set (string ("scalar", 6), *this);
      return res;
    }
    case ARRAY_TYPE:
      return ARRAY(a)->to_object();
    case OBJECT_TYPE:
      return *OBJECT(o);
    default:
      php_assert (0);
      exit (1);
  }
}


int var::safe_to_int (void) const {
  switch (type) {
    case NULL_TYPE:
      return 0;
    case BOOLEAN_TYPE:
      return (int)b;
    case INTEGER_TYPE:
      return i;
    case FLOAT_TYPE:
      if (fabs (f) > 2147483648) {
        php_warning ("Wrong convertion from double %.6lf to int", f);
      }
      return (int)f;
    case STRING_TYPE:
      return STRING(s)->safe_to_int();
    case ARRAY_TYPE:
      php_warning ("Wrong convertion from array to int");
      return ARRAY(a)->to_int();
    case OBJECT_TYPE:
      php_warning ("Wrong convertion from object to int");
      return 1;
    default:
      php_assert (0);
      exit (1);
  }
}


void var::convert_to_numeric (void) {
  switch (type) {
    case NULL_TYPE:
      type = INTEGER_TYPE;
      i = 0;
      return;
    case BOOLEAN_TYPE:
      type = INTEGER_TYPE;
      i = b;
      return;
    case INTEGER_TYPE:
    case FLOAT_TYPE:
      return;
    case STRING_TYPE:
      *this = STRING(s)->to_numeric();
      return;
    case ARRAY_TYPE: {
      php_warning ("Wrong convertion from array to number");
      int int_val = ARRAY(a)->to_int();
      ARRAY(a)->~array <var>();
      type = INTEGER_TYPE;
      i = int_val;
      return;
    }
    case OBJECT_TYPE:
      php_warning ("Wrong convertion from object to number");
      OBJECT(o)->~object();
      type = INTEGER_TYPE;
      i = 1;
      return;
    default:
      php_assert (0);
      exit (1);
  }
}

void var::convert_to_bool (void) {
  switch (type) {
    case NULL_TYPE:
      type = BOOLEAN_TYPE;
      b = 0;
      return;
    case BOOLEAN_TYPE:
      return;
    case INTEGER_TYPE:
      type = BOOLEAN_TYPE;
      b = (bool)i;
      return;
    case FLOAT_TYPE:
      type = BOOLEAN_TYPE;
      b = (bool)f;
      return;
    case STRING_TYPE: {
      bool bool_val = STRING(s)->to_bool();
      STRING(s)->~string();
      type = BOOLEAN_TYPE;
      b = bool_val;
      return;
    }
    case ARRAY_TYPE: {
      bool bool_val = ARRAY(a)->to_bool();
      ARRAY(a)->~array <var>();
      type = BOOLEAN_TYPE;
      b = bool_val;
      return;
    }
    case OBJECT_TYPE:
      OBJECT(o)->~object();
      type = BOOLEAN_TYPE;
      b = true;
      return;
    default:
      php_assert (0);
      exit (1);
  }
}

void var::convert_to_int (void) {
  switch (type) {
    case NULL_TYPE:
      type = INTEGER_TYPE;
      i = 0;
      return;
    case BOOLEAN_TYPE:
      type = INTEGER_TYPE;
      i = b;
      return;
    case INTEGER_TYPE:
      return;
    case FLOAT_TYPE:
      type = INTEGER_TYPE;
      i = (int)f;
      return;
    case STRING_TYPE: {
      int int_val = STRING(s)->to_int();
      STRING(s)->~string();
      type = INTEGER_TYPE;
      i = int_val;
      return;
    }
    case ARRAY_TYPE: {
      php_warning ("Wrong convertion from array to int");
      int int_val = ARRAY(a)->to_int();
      ARRAY(a)->~array <var>();
      type = INTEGER_TYPE;
      i = int_val;
      return;
    }
    case OBJECT_TYPE:
      php_warning ("Wrong convertion from object to int");
      OBJECT(o)->~object();
      type = INTEGER_TYPE;
      i = 1;
      return;
    default:
      php_assert (0);
      exit (1);
  }
}

void var::convert_to_float (void) {
  switch (type) {
    case NULL_TYPE:
      type = FLOAT_TYPE;
      f = 0.0;
      return;
    case BOOLEAN_TYPE:
      type = FLOAT_TYPE;
      f = b;
      return;
    case INTEGER_TYPE:
      type = FLOAT_TYPE;
      f = (double)i;
      return;
    case FLOAT_TYPE:
      return;
    case STRING_TYPE: {
      double float_val = STRING(s)->to_float();
      STRING(s)->~string();
      type = FLOAT_TYPE;
      f = float_val;
      return;
    }
    case ARRAY_TYPE: {
      php_warning ("Wrong convertion from array to float");
      double float_val = ARRAY(a)->to_float();
      ARRAY(a)->~array <var>();
      type = FLOAT_TYPE;
      f = float_val;
      return;
    }
    case OBJECT_TYPE:
      php_warning ("Wrong convertion from object to float");
      OBJECT(o)->~object();
      type = FLOAT_TYPE;
      f = 1.0;
      return;
    default:
      php_assert (0);
      exit (1);
  }
}

void var::convert_to_string (void) {
  switch (type) {
    case NULL_TYPE:
      type = STRING_TYPE;
      new (&s) string();
      return;
    case BOOLEAN_TYPE:
      type = STRING_TYPE;
      if (b) {
        new (&s) string ("1", 1);
      } else {
        new (&s) string();
      }
      return;
    case INTEGER_TYPE:
      type = STRING_TYPE;
      new (&s) string (i);
      return;
    case FLOAT_TYPE:
      type = STRING_TYPE;
      new (&s) string (f);
      return;
    case STRING_TYPE:
      return;
    case ARRAY_TYPE:
      php_warning ("Converting from array to string");
      ARRAY(a)->~array <var>();
      type = STRING_TYPE;
      new (&s) string ("Array", 5);
      return;
    case OBJECT_TYPE: {
      string res = OBJECT(o)->to_string();
      OBJECT(o)->~object();
      type = STRING_TYPE;
      new (&s) string (res);
      return;
    }
    default:
      php_assert (0);
      exit (1);
  }
}


void var::safe_convert_to_int (void) {
  switch (type) {
    case NULL_TYPE:
      type = INTEGER_TYPE;
      i = 0;
      return;
    case BOOLEAN_TYPE:
      type = INTEGER_TYPE;
      i = (int)b;
      return;
    case INTEGER_TYPE:
      return;
    case FLOAT_TYPE:
      type = INTEGER_TYPE;
      if (fabs (f) > 2147483648) {
        php_warning ("Wrong convertion from double %.6lf to int", f);
      }
      i = (int)f;
      return;
    case STRING_TYPE: {
      int int_val = STRING(s)->safe_to_int();
      STRING(s)->~string();
      type = INTEGER_TYPE;
      i = int_val;
      return;
    }
    case ARRAY_TYPE: {
      php_warning ("Wrong convertion from array to int");
      int int_val = ARRAY(a)->to_int();
      ARRAY(a)->~array <var>();
      type = INTEGER_TYPE;
      i = int_val;
      return;
    }
    case OBJECT_TYPE:
      php_warning ("Wrong convertion from object to int");
      OBJECT(o)->~object();
      type = INTEGER_TYPE;
      i = 1;
      return;
    default:
      php_assert (0);
      exit (1);
  }
}


const bool& var::as_bool (const char *function, int parameter_num) const {
  switch (type) {
    case NULL_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE:
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be boolean, %s is given", function, parameter_num, get_type_c_str());
      empty_bool = false;
      return empty_bool;
    case BOOLEAN_TYPE:
      return b;
    default:
      php_assert (0);
      exit (1);
  }
}

const int& var::as_int (const char *function, int parameter_num) const {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE:
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be int, %s is given", function, parameter_num, get_type_c_str());
      empty_int = 0;
      return empty_int;
    case INTEGER_TYPE:
      return i;
    default:
      php_assert (0);
      exit (1);
  }
}

const double& var::as_float (const char *function, int parameter_num) const {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case STRING_TYPE:
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be float, %s is given", function, parameter_num, get_type_c_str());
      empty_float = 0;
      return empty_float;
    case FLOAT_TYPE:
      return f;
    default:
      php_assert (0);
      exit (1);
  }
}

const string& var::as_string (const char *function, int parameter_num) const {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be string, %s is given", function, parameter_num, get_type_c_str());
      empty_string = string();
      return empty_string;
    case STRING_TYPE:
      return *STRING(s);
    default:
      php_assert (0);
      exit (1);
  }
}

const array <var>& var::as_array (const char *function, int parameter_num) const {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be array, %s is given", function, parameter_num, get_type_c_str());
      *empty_array_var = array <var>();
      return *empty_array_var;
    case ARRAY_TYPE:
      return *ARRAY(a);
    default:
      php_assert (0);
      exit (1);
  }
}


bool& var::as_bool (const char *function, int parameter_num) {
  switch (type) {
    case INTEGER_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE:
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be boolean, %s is given", function, parameter_num, get_type_c_str());
      empty_bool = false;
      return empty_bool;
    case NULL_TYPE:
      convert_to_bool();
    case BOOLEAN_TYPE:
      return b;
    default:
      php_assert (0);
      exit (1);
  }
}

int& var::as_int (const char *function, int parameter_num) {
  switch (type) {
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be int, %s is given", function, parameter_num, get_type_c_str());
      empty_int = 0;
      return empty_int;
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE:
      convert_to_int();
    case INTEGER_TYPE:
      return i;
    default:
      php_assert (0);
      exit (1);
  }
}

double& var::as_float (const char *function, int parameter_num) {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case STRING_TYPE:
      convert_to_float();
    case FLOAT_TYPE:
      return f;
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be float, %s is given", function, parameter_num, get_type_c_str());
      empty_float = 0;
      return empty_float;
    default:
      php_assert (0);
      exit (1);
  }
}

string& var::as_string (const char *function, int parameter_num) {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
      convert_to_string();
    case STRING_TYPE:
      return *STRING(s);
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be string, %s is given", function, parameter_num, get_type_c_str());
      empty_string = string();
      return empty_string;
    default:
      php_assert (0);
      exit (1);
  }
}

array <var>& var::as_array (const char *function, int parameter_num) {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE:
    case OBJECT_TYPE:
      php_warning ("%s() expects parameter %d to be array, %s is given", function, parameter_num, get_type_c_str());
      *empty_array_var = array <var>();
      return *empty_array_var;
    case ARRAY_TYPE:
      return *ARRAY(a);
    default:
      php_assert (0);
      exit (1);
  }
}


bool var::is_numeric (void) const {
  switch (type) {
    case NULL_TYPE:
    case BOOLEAN_TYPE:
      return false;
    case INTEGER_TYPE:
    case FLOAT_TYPE:
      return true;
    case STRING_TYPE:
      return STRING(s)->is_numeric();
    case ARRAY_TYPE:
    case OBJECT_TYPE:
      return false;
    default:
      php_assert (0);
      exit (1);
  }
}

bool var::is_scalar (void) const {
  return type != NULL_TYPE && type != ARRAY_TYPE && type != OBJECT_TYPE;
}


bool var::is_null (void) const {
  return type == NULL_TYPE;
}

bool var::is_bool (void) const {
  return type == BOOLEAN_TYPE;
}

bool var::is_int (void) const {
  return type == INTEGER_TYPE;
}

bool var::is_float (void) const {
  return type == FLOAT_TYPE;
}

bool var::is_string (void) const {
  return type == STRING_TYPE;
}

bool var::is_array (void) const {
  return type == ARRAY_TYPE;
}

bool var::is_object (void) const {
  return type == OBJECT_TYPE;
}


inline const char *var::get_type_c_str (void) const {
  switch (type) {
    case NULL_TYPE:
      return "NULL";
    case BOOLEAN_TYPE:
      return "boolean";
    case INTEGER_TYPE:
      return "integer";
    case FLOAT_TYPE:
      return "double";
    case STRING_TYPE:
      return "string";
    case ARRAY_TYPE:
      return "array";
    case OBJECT_TYPE:
      return "object";
    default:
      php_assert (0);
      exit (1);
  }
}

inline const string var::get_type (void) const {
  const char *result = get_type_c_str();
  return string (result, (dl::size_type)strlen (result));
}


bool var::empty (void) const {
  return !to_bool();
}

int var::count (void) const {
  switch (type) {
    case NULL_TYPE:
      return 0;
    case BOOLEAN_TYPE:
    case INTEGER_TYPE:
    case FLOAT_TYPE:
    case STRING_TYPE:
      return 1;
    case ARRAY_TYPE:
      return ARRAY(a)->count();
    case OBJECT_TYPE:
      return 1;
    default:
      php_assert (0);
      exit (1);
  }
}


void var::swap (var &other) {
  ::swap (type, other.type);
  ::swap (f, other.f);
}


var& var::operator[] (int int_key) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      php_warning ("Lvalue string offset doesn't supported");
      empty_var = var();
      return empty_var;
    }

    if (type == NULL_TYPE || (type == BOOLEAN_TYPE && !b)) {
      type = ARRAY_TYPE;
      new (&a) array <var>();
    } else {
      php_warning ("Cannot use a value \"%s\" of type %s as an array, index = %d", to_string().c_str(), get_type_c_str(), int_key);

      empty_var = var();
      return empty_var;
    }
  }
  return (*ARRAY(a))[int_key];
}

var& var::operator[] (const string &string_key) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      php_warning ("Lvalue string offset doesn't supported");
      empty_var = var();
      return empty_var;
    }

    if (type == NULL_TYPE || (type == BOOLEAN_TYPE && !b)) {
      type = ARRAY_TYPE;
      new (&a) array <var>();
    } else {
      php_warning ("Cannot use a value \"%s\" of type %s as an array, index = %s", to_string().c_str(), get_type_c_str(), string_key.c_str());

      empty_var = var();
      return empty_var;
    }
  }

  return (*ARRAY(a))[string_key];
}

var& var::operator[] (const var &v) {
  switch (v.type) {
    case NULL_TYPE:
      return (*this)[string()];
    case BOOLEAN_TYPE:
      return (*this)[v.b];
    case INTEGER_TYPE:
      return (*this)[v.i];
    case FLOAT_TYPE:
      return (*this)[(int)v.f];
    case STRING_TYPE:
      return (*this)[*STRING(v.s)];
    case ARRAY_TYPE:
      php_warning ("Illegal offset type %s", v.get_type_c_str());
      return (*this)[ARRAY(v.a)->to_int()];
    case OBJECT_TYPE:
      php_warning ("Illegal offset type %s", v.get_type_c_str());
      return (*this)[1];
    default:
      php_assert (0);
      exit (1);
  }
}

var& var::operator[] (const array <var>::const_iterator &it) {
  return (*ARRAY(a))[it];
}

var& var::operator[] (const array <var>::iterator &it) {
  return (*ARRAY(a))[it];
}


void var::set_value (int int_key, const var &v) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      char c = (v.to_string())[0];

      if (int_key >= 0) {
        int l = STRING(s)->size();
        if (int_key >= l) {
          STRING(s)->append (int_key + 1 - l, ' ');
        } else {
          STRING(s)->make_not_shared();
        }

        (*STRING(s))[int_key] = c;
      } else {
        php_warning ("Illegal string offset %d", int_key);
      }
      return;
    }

    if (type == NULL_TYPE || (type == BOOLEAN_TYPE && !b)) {
      type = ARRAY_TYPE;
      new (&a) array <var>();
    } else {
      php_warning ("Cannot use a value \"%s\" of type %s as an array, index = %d", to_string().c_str(), get_type_c_str(), int_key);
      return;
    }
  }
  return ARRAY(a)->set_value (int_key, v);
}

void var::set_value (const string &string_key, const var &v) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      int int_val;
      if (!string_key.try_to_int (&int_val)) {
        php_warning ("Illegal string offset \"%s\"", string_key.c_str());
        int_val = string_key.to_int();
      }
      if (int_val < 0) {
        return;
      }

      char c = (v.to_string())[0];

      int l = STRING(s)->size();
      if (int_val >= l) {
        STRING(s)->append (int_val + 1 - l, ' ');
      } else {
        STRING(s)->make_not_shared();
      }

      (*STRING(s))[int_val] = c;
      return;
    }

    if (type == NULL_TYPE || (type == BOOLEAN_TYPE && !b)) {
      type = ARRAY_TYPE;
      new (&a) array <var>();
    } else {
      php_warning ("Cannot use a value \"%s\" of type %s as an array, index = %s", to_string().c_str(), get_type_c_str(), string_key.c_str());
      return;
    }
  }

  return ARRAY(a)->set_value (string_key, v);
}

void var::set_value (const var &v, const var &value) {
  switch (v.type) {
    case NULL_TYPE:
      return set_value (string(), value);
    case BOOLEAN_TYPE:
      return set_value (v.b, value);
    case INTEGER_TYPE:
      return set_value (v.i, value);
    case FLOAT_TYPE:
      return set_value ((int)v.f, value);
    case STRING_TYPE:
      return set_value (*STRING(v.s), value);
    case ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return;
    case OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return;
    default:
      php_assert (0);
      exit (1);
  }
}

void var::set_value (const array <var>::const_iterator &it) {
  return ARRAY(a)->set_value (it);
}

void var::set_value (const array <var>::iterator &it) {
  return ARRAY(a)->set_value (it);
}


const var var::get_value (int int_key) const {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      if ((dl::size_type)int_key >= STRING(s)->size()) {
        return string();
      }
      return string (1, (*STRING(s))[int_key]);
    }

    if (type != NULL_TYPE && (type != BOOLEAN_TYPE || b)) {
      php_warning ("Cannot use a value \"%s\" of type %s as an array, index = %d", to_string().c_str(), get_type_c_str(), int_key);
    }
    return var();
  }

  return ARRAY(a)->get_value (int_key);
}

const var var::get_value (const string &string_key) const {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      int int_val;
      if (!string_key.try_to_int (&int_val)) {
        php_warning ("Illegal string offset \"%s\"", string_key.c_str());
        int_val = string_key.to_int();
      }
      if ((dl::size_type)int_val >= STRING(s)->size()) {
        return string();
      }
      return string (1, (*STRING(s))[int_val]);
    }

    if (type != NULL_TYPE && (type != BOOLEAN_TYPE || b)) {
      php_warning ("Cannot use a value \"%s\" of type %s as an array, index = %s", to_string().c_str(), get_type_c_str(), string_key.c_str());
    }
    return var();
  }

  return ARRAY(a)->get_value (string_key);
}

const var var::get_value (const var &v) const {
  switch (v.type) {
    case NULL_TYPE:
      return get_value (string());
    case BOOLEAN_TYPE:
      return get_value (v.b);
    case INTEGER_TYPE:
      return get_value (v.i);
    case FLOAT_TYPE:
      return get_value ((int)v.f);
    case STRING_TYPE:
      return get_value (*STRING(v.s));
    case ARRAY_TYPE:
      php_warning ("Illegal offset type %s", v.get_type_c_str());
      return var();
    case OBJECT_TYPE:
      php_warning ("Illegal offset type %s", v.get_type_c_str());
      return var();
    default:
      php_assert (0);
      exit (1);
  }
}

const var var::get_value (const array <var>::const_iterator &it) const {
  return ARRAY(a)->get_value (it);
}

const var var::get_value (const array <var>::iterator &it) const {
  return ARRAY(a)->get_value (it);
}


void var::push_back (const var &v) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == NULL_TYPE || (type == BOOLEAN_TYPE && !b)) {
      type = ARRAY_TYPE;
      new (&a) array <var>();
    } else {
      php_warning ("[] operator not supported for type %s", get_type_c_str());
      return;
    }
  }

  return ARRAY(a)->push_back (v);
}

const var var::push_back_return (const var &v) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == NULL_TYPE || (type == BOOLEAN_TYPE && !b)) {
      type = ARRAY_TYPE;
      new (&a) array <var>();
    } else {
      php_warning ("[] operator not supported for type %s", get_type_c_str());
      empty_var = var();
      return empty_var;
    }
  }

  return ARRAY(a)->push_back_return (v);
}


bool var::isset (int int_key) const {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      return (dl::size_type)int_key < STRING(s)->size();
    }

    if (type != NULL_TYPE && (type != BOOLEAN_TYPE || b)) {
      php_warning ("Cannot use variable of type %s as array in isset", get_type_c_str());
    }
    return false;
  }

  return ARRAY(a)->isset (int_key);
}

bool var::isset (const string &string_key) const {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      int int_val;
      if (!string_key.try_to_int (&int_val)) {
        php_warning ("Illegal string offset \"%s\"", string_key.c_str());
        int_val = string_key.to_int();
      }
      return (dl::size_type)int_val < STRING(s)->size();
    }

    if (type != NULL_TYPE && (type != BOOLEAN_TYPE || b)) {
      php_warning ("Cannot use variable of type %s as array in isset", get_type_c_str());
    }
    return false;
  }

  return ARRAY(a)->isset (string_key);
}

bool var::isset (const var &v) const {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type == STRING_TYPE) {
      return (dl::size_type)(v.to_int()) < STRING(s)->size();
    }

    if (type != NULL_TYPE && (type != BOOLEAN_TYPE || b)) {
      php_warning ("Cannot use variable of type %s as array in isset", get_type_c_str());
    }
    return false;
  }

  switch (v.type) {
    case NULL_TYPE:
      return ARRAY(a)->isset (string());
    case BOOLEAN_TYPE:
      return ARRAY(a)->isset (v.b);
    case INTEGER_TYPE:
      return ARRAY(a)->isset (v.i);
    case FLOAT_TYPE:
      return ARRAY(a)->isset ((int)v.f);
    case STRING_TYPE:
      return ARRAY(a)->isset (*STRING(v.s));
    case ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return false;
    case OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return false;
    default:
      php_assert (0);
      exit (1);
  }
}

bool var::isset (const array <var>::const_iterator &it) const {
  return ARRAY(a)->isset (it);
}

bool var::isset (const array <var>::iterator &it) const {
  return ARRAY(a)->isset (it);
}


void var::unset (int int_key) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type != NULL_TYPE && (type != BOOLEAN_TYPE || b)) {
      php_warning ("Cannot use variable of type %s as array in unset", get_type_c_str());
    }
    return;
  }

  return ARRAY(a)->unset (int_key);
}

void var::unset (const string &string_key) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type != NULL_TYPE && (type != BOOLEAN_TYPE || b)) {
      php_warning ("Cannot use variable of type %s as array in unset", get_type_c_str());
    }
    return;
  }

  return ARRAY(a)->unset (string_key);
}

void var::unset (const var &v) {
  if (unlikely (type != ARRAY_TYPE)) {
    if (type != NULL_TYPE && (type != BOOLEAN_TYPE || b)) {
      php_warning ("Cannot use variable of type %s as array in unset", get_type_c_str());
    }
    return;
  }

  switch (v.type) {
    case NULL_TYPE:
      return ARRAY(a)->unset (string());
    case BOOLEAN_TYPE:
      return ARRAY(a)->unset (v.b);
    case INTEGER_TYPE:
      return ARRAY(a)->unset (v.i);
    case FLOAT_TYPE:
      return ARRAY(a)->unset ((int)v.f);
    case STRING_TYPE:
      return ARRAY(a)->unset (*STRING(v.s));
    case ARRAY_TYPE:
      php_warning ("Illegal offset type array");
      return;
    case OBJECT_TYPE:
      php_warning ("Illegal offset type object");
      return;
    default:
      php_assert (0);
      exit (1);
  }
}

void var::unset (const array <var>::const_iterator &it) {
  return ARRAY(a)->unset (it);
}

void var::unset (const array <var>::iterator &it) {
  return ARRAY(a)->unset (it);
}


array <var>::const_iterator var::begin (void) const {
  if (likely (type == ARRAY_TYPE)) {
    return CONST_ARRAY(a)->begin();
  }
  php_warning ("Invalid argument supplied for foreach(), %s \"%s\" is given", get_type_c_str(), to_string().c_str());
  return array <var>::const_iterator();
}

array <var>::const_iterator var::end (void) const {
  if (likely (type == ARRAY_TYPE)) {
    return CONST_ARRAY(a)->end();
  }
  return array <var>::const_iterator();
}


array <var>::iterator var::begin (void) {
  if (likely (type == ARRAY_TYPE)) {
    return ARRAY(a)->begin();
  }
  php_warning ("Invalid argument supplied for foreach(), %s \"%s\" is given", get_type_c_str(), to_string().c_str());
  return array <var>::iterator();
}

array <var>::iterator var::end (void) {
  if (likely (type == ARRAY_TYPE)) {
    return ARRAY(a)->end();
  }
  return array <var>::iterator();
}


int var::get_reference_counter (void) const {
  switch (type) {
    case NULL_TYPE:
      return -1;
    case BOOLEAN_TYPE:
      return -2;
    case INTEGER_TYPE:
      return -3;
    case FLOAT_TYPE:
      return -4;
    case STRING_TYPE:
      return STRING(s)->get_reference_counter();
    case ARRAY_TYPE:
      return ARRAY(a)->get_reference_counter();
    case OBJECT_TYPE:
      return OBJECT(o)->get_reference_counter();
    default:
      php_assert (0);
      exit (1);
  }
}


const var operator + (const var &lhs, const var &rhs) {
  if (likely (lhs.type == var::INTEGER_TYPE && rhs.type == var::INTEGER_TYPE)) {
    return lhs.i + rhs.i;
  }

  if (lhs.type == var::ARRAY_TYPE && rhs.type == var::ARRAY_TYPE) {
    return *ARRAY(lhs.a) + *ARRAY(rhs.a);
  }

  const var arg1 = lhs.to_numeric();
  const var arg2 = rhs.to_numeric();

  if (arg1.type == var::INTEGER_TYPE) {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.i + arg2.i;
    } else {
      return arg1.i + arg2.f;
    }
  } else {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.f + arg2.i;
    } else {
      return arg1.f + arg2.f;
    }
  }
}

const var operator - (const var &lhs, const var &rhs) {
  if (likely (lhs.type == var::INTEGER_TYPE && rhs.type == var::INTEGER_TYPE)) {
    return lhs.i - rhs.i;
  }

  const var arg1 = lhs.to_numeric();
  const var arg2 = rhs.to_numeric();

  if (arg1.type == var::INTEGER_TYPE) {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.i - arg2.i;
    } else {
      return arg1.i - arg2.f;
    }
  } else {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.f - arg2.i;
    } else {
      return arg1.f - arg2.f;
    }
  }
}

const var operator * (const var &lhs, const var &rhs) {
  if (likely (lhs.type == var::INTEGER_TYPE && rhs.type == var::INTEGER_TYPE)) {
    return lhs.i * rhs.i;
  }

  const var arg1 = lhs.to_numeric();
  const var arg2 = rhs.to_numeric();

  if (arg1.type == var::INTEGER_TYPE) {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.i * arg2.i;
    } else {
      return arg1.i * arg2.f;
    }
  } else {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.f * arg2.i;
    } else {
      return arg1.f * arg2.f;
    }
  }
}

const var operator / (const var &lhs, const var &rhs) {
  if (likely (lhs.type == var::INTEGER_TYPE && rhs.type == var::INTEGER_TYPE)) {
    if (rhs.i == 0) {
      php_warning ("Integer division by zero");
      return false;
    }

    if (lhs.i % rhs.i == 0) {
      return lhs.i / rhs.i;
    }
    return (double)lhs.i / rhs.i;
  }

  const var arg1 = lhs.to_numeric();
  const var arg2 = rhs.to_numeric();

  if (arg2.type == var::INTEGER_TYPE) {
    if (arg2.i == 0) {
      php_warning ("Integer division by zero");
      return false;
    }

    if (arg1.type == var::INTEGER_TYPE) {
      if (arg1.i % arg2.i == 0) {
        return arg1.i / arg2.i;
      }
      return (double)arg1.i / arg2.i;
    } else {
      return arg1.f / arg2.i;
    }
  } else {
    if (arg2.f == 0.0) {
      php_warning ("Float division by zero");
      return false;
    }

    if (arg1.type == var::INTEGER_TYPE) {
      return arg1.i / arg2.f;
    } else {
      return arg1.f / arg2.f;
    }
  }
}

const var operator % (const var &lhs, const var &rhs) {
  int div = rhs.to_int();
  if (div == 0) {
    php_warning ("Modulo by zero");
    return false;
  }
  return lhs.to_int() % div;
}


const var operator - (const string &lhs) {
  var arg1 = lhs.to_numeric();

  if (arg1.type == var::INTEGER_TYPE) {
    arg1.i = -arg1.i;
  } else {
    arg1.f = -arg1.f;
  }
  return arg1;
}

const var operator + (const string &lhs) {
  return lhs.to_numeric();
}


int operator & (const var &lhs, const var &rhs) {
  return lhs.to_int() & rhs.to_int();
}
int operator | (const var &lhs, const var &rhs) {
  return lhs.to_int() | rhs.to_int();
}
int operator ^ (const var &lhs, const var &rhs) {
  return lhs.to_int() ^ rhs.to_int();
}
int operator << (const var &lhs, const var &rhs) {
  return lhs.to_int() << rhs.to_int();
}
int operator >> (const var &lhs, const var &rhs) {
  return lhs.to_int() >> rhs.to_int();
}


const var safe_add (const var &lhs, const var &rhs) {
  if (likely (lhs.type == var::INTEGER_TYPE && rhs.type == var::INTEGER_TYPE)) {
    return safe_add (lhs.i, rhs.i);
  }

  if (lhs.type == var::ARRAY_TYPE && rhs.type == var::ARRAY_TYPE) {
    return *ARRAY(lhs.a) + *ARRAY(rhs.a);
  }

  const var arg1 = lhs.to_numeric();
  const var arg2 = rhs.to_numeric();

  if (arg1.type == var::INTEGER_TYPE) {
    if (arg2.type == var::INTEGER_TYPE) {
      return safe_add (arg1.i, arg2.i);
    } else {
      return arg1.i + arg2.f;
    }
  } else {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.f + arg2.i;
    } else {
      return arg1.f + arg2.f;
    }
  }
}

const var safe_sub (const var &lhs, const var &rhs) {
  if (likely (lhs.type == var::INTEGER_TYPE && rhs.type == var::INTEGER_TYPE)) {
    return safe_sub (lhs.i, rhs.i);
  }

  const var arg1 = lhs.to_numeric();
  const var arg2 = rhs.to_numeric();

  if (arg1.type == var::INTEGER_TYPE) {
    if (arg2.type == var::INTEGER_TYPE) {
      return safe_sub (arg1.i, arg2.i);
    } else {
      return arg1.i - arg2.f;
    }
  } else {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.f - arg2.i;
    } else {
      return arg1.f - arg2.f;
    }
  }
}

const var safe_mul (const var &lhs, const var &rhs) {
  if (likely (lhs.type == var::INTEGER_TYPE && rhs.type == var::INTEGER_TYPE)) {
    return safe_mul (lhs.i, rhs.i);
  }

  const var arg1 = lhs.to_numeric();
  const var arg2 = rhs.to_numeric();

  if (arg1.type == var::INTEGER_TYPE) {
    if (arg2.type == var::INTEGER_TYPE) {
      return safe_mul (arg1.i, arg2.i);
    } else {
      return arg1.i * arg2.f;
    }
  } else {
    if (arg2.type == var::INTEGER_TYPE) {
      return arg1.f * arg2.i;
    } else {
      return arg1.f * arg2.f;
    }
  }
}

const var safe_shl (const var &lhs, const var &rhs) {
  return safe_shl (lhs.safe_to_int(), rhs.safe_to_int());
}


bool eq2 (const var &lhs, const var &rhs) {
  if (unlikely (lhs.type == var::STRING_TYPE)) {
    if (likely (rhs.type == var::STRING_TYPE)) {
      return eq2 (*STRING(lhs.s), *STRING(rhs.s));
    } else if (unlikely (rhs.type == var::NULL_TYPE)) {
      return STRING(lhs.s)->size() == 0;
    }
  } else if (unlikely (rhs.type == var::STRING_TYPE)) {
    if (unlikely (lhs.type == var::NULL_TYPE)) {
      return STRING(rhs.s)->size() == 0;
    }
  }
  if (lhs.type == var::BOOLEAN_TYPE || rhs.type == var::BOOLEAN_TYPE || lhs.type == var::NULL_TYPE || rhs.type == var::NULL_TYPE) {
    return lhs.to_bool() == rhs.to_bool();
  }

  if (unlikely (lhs.type == var::ARRAY_TYPE || rhs.type == var::ARRAY_TYPE)) {
    if (likely (lhs.type == var::ARRAY_TYPE && rhs.type == var::ARRAY_TYPE)) {
      return eq2 (*ARRAY(lhs.a), *ARRAY(rhs.a));
    }

    php_warning ("Unsupported operand types for operator == (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return false;
  }

  if (unlikely (lhs.type == var::OBJECT_TYPE || rhs.type == var::OBJECT_TYPE)) {
    if (likely (lhs.type == var::OBJECT_TYPE && rhs.type == var::OBJECT_TYPE)) {
      return eq2 (*OBJECT(lhs.o), *OBJECT(rhs.o));
    }

    php_warning ("Unsupported operand types for operator == (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return false;
  }

  return lhs.to_float() == rhs.to_float();
}

bool neq2 (const var &lhs, const var &rhs) {
  return !eq2 (lhs, rhs);
}

bool operator <= (const var &lhs, const var &rhs) {
  if (unlikely (lhs.type == var::STRING_TYPE)) {
    if (likely (rhs.type == var::STRING_TYPE)) {
      if (STRING(lhs.s)[0][0] <= '9' && STRING(rhs.s)[0][0] <= '9') {
        int lhs_int_val, rhs_int_val;
        if (STRING(lhs.s)->try_to_int (&lhs_int_val) && STRING(rhs.s)->try_to_int (&rhs_int_val)) {
          return lhs_int_val <= rhs_int_val;
        }

        double lhs_float_val, rhs_float_val;
        if (STRING(lhs.s)->try_to_float (&lhs_float_val) && STRING(rhs.s)->try_to_float (&rhs_float_val)) {
          STRING(lhs.s)->warn_on_float_conversion();
          STRING(rhs.s)->warn_on_float_conversion();
          if (is_ok_float (lhs_float_val) && is_ok_float (rhs_float_val)) {
            return lhs_float_val <= rhs_float_val;
          }
        }
      }

      return STRING(lhs.s)->compare (*STRING(rhs.s)) <= 0;
    } else if (unlikely (rhs.type == var::NULL_TYPE)) {
      return STRING(lhs.s)->size() == 0;
    }
  } else if (unlikely (rhs.type == var::STRING_TYPE)) {
    if (unlikely (lhs.type == var::NULL_TYPE)) {
      return true;
    }
  }
  if (lhs.type == var::BOOLEAN_TYPE || rhs.type == var::BOOLEAN_TYPE || lhs.type == var::NULL_TYPE || rhs.type == var::NULL_TYPE) {
    return lhs.to_bool() <= rhs.to_bool();
  }

  if (unlikely (lhs.type == var::ARRAY_TYPE || rhs.type == var::ARRAY_TYPE)) {
    if (likely (lhs.type == var::ARRAY_TYPE && rhs.type == var::ARRAY_TYPE)) {
      return ARRAY(lhs.a)->count() <= ARRAY(rhs.a)->count();
    }

    php_warning ("Unsupported operand types for operator <= (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return rhs.type == var::ARRAY_TYPE;
  }

  if (unlikely (lhs.type == var::OBJECT_TYPE || rhs.type == var::OBJECT_TYPE)) {
    php_warning ("Unsupported operand types for operator <= (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return rhs.type == var::OBJECT_TYPE;
  }

  return lhs.to_float() <= rhs.to_float();
}

bool operator >= (const var &lhs, const var &rhs) {
  if (unlikely (lhs.type == var::STRING_TYPE)) {
    if (likely (rhs.type == var::STRING_TYPE)) {
      if (STRING(lhs.s)[0][0] <= '9' && STRING(rhs.s)[0][0] <= '9') {
        int lhs_int_val, rhs_int_val;
        if (STRING(lhs.s)->try_to_int (&lhs_int_val) && STRING(rhs.s)->try_to_int (&rhs_int_val)) {
          return lhs_int_val >= rhs_int_val;
        }

        double lhs_float_val, rhs_float_val;
        if (STRING(lhs.s)->try_to_float (&lhs_float_val) && STRING(rhs.s)->try_to_float (&rhs_float_val)) {
          STRING(lhs.s)->warn_on_float_conversion();
          STRING(rhs.s)->warn_on_float_conversion();
          if (is_ok_float (lhs_float_val) && is_ok_float (rhs_float_val)) {
            return lhs_float_val >= rhs_float_val;
          }
        }
      }

      return STRING(lhs.s)->compare (*STRING(rhs.s)) >= 0;
    } else if (unlikely (rhs.type == var::NULL_TYPE)) {
      return true;
    }
  } else if (unlikely (rhs.type == var::STRING_TYPE)) {
    if (unlikely (lhs.type == var::NULL_TYPE)) {
      return STRING(rhs.s)->size() == 0;
    }
  }
  if (lhs.type == var::BOOLEAN_TYPE || rhs.type == var::BOOLEAN_TYPE || lhs.type == var::NULL_TYPE || rhs.type == var::NULL_TYPE) {
    return lhs.to_bool() >= rhs.to_bool();
  }

  if (unlikely (lhs.type == var::ARRAY_TYPE || rhs.type == var::ARRAY_TYPE)) {
    if (likely (lhs.type == var::ARRAY_TYPE && rhs.type == var::ARRAY_TYPE)) {
      return ARRAY(lhs.a)->count() >= ARRAY(rhs.a)->count();
    }

    php_warning ("Unsupported operand types for operator >= (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return lhs.type == var::ARRAY_TYPE;
  }

  if (unlikely (lhs.type == var::OBJECT_TYPE || rhs.type == var::OBJECT_TYPE)) {
    php_warning ("Unsupported operand types for operator >= (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return lhs.type == var::OBJECT_TYPE;
  }

  return lhs.to_float() >= rhs.to_float();
}

bool operator < (const var &lhs, const var &rhs) {
  if (unlikely (lhs.type == var::STRING_TYPE)) {
    if (likely (rhs.type == var::STRING_TYPE)) {
      if (STRING(lhs.s)[0][0] <= '9' && STRING(rhs.s)[0][0] <= '9') {
        int lhs_int_val, rhs_int_val;
        if (STRING(lhs.s)->try_to_int (&lhs_int_val) && STRING(rhs.s)->try_to_int (&rhs_int_val)) {
          return lhs_int_val < rhs_int_val;
        }

        double lhs_float_val, rhs_float_val;
        if (STRING(lhs.s)->try_to_float (&lhs_float_val) && STRING(rhs.s)->try_to_float (&rhs_float_val)) {
          STRING(lhs.s)->warn_on_float_conversion();
          STRING(rhs.s)->warn_on_float_conversion();
          if (is_ok_float (lhs_float_val) && is_ok_float (rhs_float_val)) {
            return lhs_float_val < rhs_float_val;
          }
        }
      }

      return STRING(lhs.s)->compare (*STRING(rhs.s)) < 0;
    } else if (unlikely (rhs.type == var::NULL_TYPE)) {
      return false;
    }
  } else if (unlikely (rhs.type == var::STRING_TYPE)) {
    if (unlikely (lhs.type == var::NULL_TYPE)) {
      return STRING(rhs.s)->size() != 0;
    }
  }
  if (lhs.type == var::BOOLEAN_TYPE || rhs.type == var::BOOLEAN_TYPE || lhs.type == var::NULL_TYPE || rhs.type == var::NULL_TYPE) {
    return lhs.to_bool() < rhs.to_bool();
  }

  if (unlikely (lhs.type == var::ARRAY_TYPE || rhs.type == var::ARRAY_TYPE)) {
    if (likely (lhs.type == var::ARRAY_TYPE && rhs.type == var::ARRAY_TYPE)) {
      return ARRAY(lhs.a)->count() < ARRAY(rhs.a)->count();
    }

    php_warning ("Unsupported operand types for operator < (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return lhs.type != var::ARRAY_TYPE;
  }

  if (unlikely (lhs.type == var::OBJECT_TYPE || rhs.type == var::OBJECT_TYPE)) {
    php_warning ("Unsupported operand types for operator < (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return lhs.type != var::OBJECT_TYPE;
  }

  return lhs.to_float() < rhs.to_float();
}

bool operator > (const var &lhs, const var &rhs) {
  if (unlikely (lhs.type == var::STRING_TYPE)) {
    if (likely (rhs.type == var::STRING_TYPE)) {
      if (STRING(lhs.s)[0][0] <= '9' && STRING(rhs.s)[0][0] <= '9') {
        int lhs_int_val, rhs_int_val;
        if (STRING(lhs.s)->try_to_int (&lhs_int_val) && STRING(rhs.s)->try_to_int (&rhs_int_val)) {
          return lhs_int_val > rhs_int_val;
        }

        double lhs_float_val, rhs_float_val;
        if (STRING(lhs.s)->try_to_float (&lhs_float_val) && STRING(rhs.s)->try_to_float (&rhs_float_val)) {
          STRING(lhs.s)->warn_on_float_conversion();
          STRING(rhs.s)->warn_on_float_conversion();
          if (is_ok_float (lhs_float_val) && is_ok_float (rhs_float_val)) {
            return lhs_float_val > rhs_float_val;
          }
        }
      }

      return STRING(lhs.s)->compare (*STRING(rhs.s)) > 0;
    } else if (unlikely (rhs.type == var::NULL_TYPE)) {
      return STRING(lhs.s)->size() != 0;
    }
  } else if (unlikely (rhs.type == var::STRING_TYPE)) {
    if (unlikely (lhs.type == var::NULL_TYPE)) {
      return false;
    }
  }
  if (lhs.type == var::BOOLEAN_TYPE || rhs.type == var::BOOLEAN_TYPE || lhs.type == var::NULL_TYPE || rhs.type == var::NULL_TYPE) {
    return lhs.to_bool() > rhs.to_bool();
  }

  if (unlikely (lhs.type == var::ARRAY_TYPE || rhs.type == var::ARRAY_TYPE)) {
    if (likely (lhs.type == var::ARRAY_TYPE && rhs.type == var::ARRAY_TYPE)) {
      return ARRAY(lhs.a)->count() > ARRAY(rhs.a)->count();
    }

    php_warning ("Unsupported operand types for operator > (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return rhs.type != var::ARRAY_TYPE;
  }

  if (unlikely (lhs.type == var::OBJECT_TYPE || rhs.type == var::OBJECT_TYPE)) {
    php_warning ("Unsupported operand types for operator > (%s and %s)", lhs.get_type_c_str(), rhs.get_type_c_str());
    return rhs.type != var::OBJECT_TYPE;
  }

  return lhs.to_float() > rhs.to_float();
}

bool equals (const var &lhs, const var &rhs) {
  if (lhs.type != rhs.type) {
    return false;
  }

  switch (lhs.type) {
    case var::NULL_TYPE:
      return true;
    case var::BOOLEAN_TYPE:
      return lhs.b == rhs.b;
    case var::INTEGER_TYPE:
      return lhs.i == rhs.i;
    case var::FLOAT_TYPE:
      return lhs.f == rhs.f;
    case var::STRING_TYPE:
      return *STRING(lhs.s) == *STRING(rhs.s);
    case var::ARRAY_TYPE:
      return equals (*ARRAY(lhs.a), *ARRAY(rhs.a));
    case var::OBJECT_TYPE:
      return equals (*OBJECT(lhs.o), *OBJECT(rhs.o));
    default:
      php_assert (0);
      exit (1);
  }
}

bool not_equals (const var &lhs, const var &rhs) {
  return !equals (lhs, rhs);
}


void swap (var &lhs, var &rhs) {
  lhs.swap (rhs);
}


template <class T, class T1>
array <T>& safe_set_add (array <T> &lhs, const array <T1> &rhs) {
  return lhs += rhs;
}

template <class T>
array <T> safe_add (const array <T> &lhs, const array <T> &rhs) {
  return lhs + rhs;
}


var& safe_set_add (var &lhs, const var &rhs) {
  return lhs.safe_set_add (rhs);
}

var& safe_set_sub (var &lhs, const var &rhs) {
  return lhs.safe_set_sub (rhs);
}

var& safe_set_mul (var &lhs, const var &rhs) {
  return lhs.safe_set_mul (rhs);
}

var& safe_set_shl (var &lhs, const var &rhs) {
  return lhs.safe_set_shl (rhs);
}


int& safe_set_add (int &lhs, int rhs) {
  return lhs = safe_add (lhs, rhs);
}

int& safe_set_sub (int &lhs, int rhs) {
  return lhs = safe_sub (lhs, rhs);
}

int& safe_set_mul (int &lhs, int rhs) {
  return lhs = safe_mul (lhs, rhs);
}

int& safe_set_shl (int &lhs, int rhs) {
  return lhs = safe_shl (lhs, rhs);
}


int safe_add (int lhs, int rhs) {
  long long res = (long long)lhs + rhs;
  int resi = (int)res;
  if (resi != res) {
    php_warning ("Integer overflow in %d + %d", lhs, rhs);
  }

  return resi;
}

int safe_sub (int lhs, int rhs) {
  long long res = (long long)lhs - rhs;
  int resi = (int)res;
  if (resi != res) {
    php_warning ("Integer overflow in %d - %d", lhs, rhs);
  }

  return resi;
}

int safe_mul (int lhs, int rhs) {
  long long res = (long long)lhs * rhs;
  int resi = (int)res;
  if (resi != res) {
    php_warning ("Integer overflow in %d * %d", lhs, rhs);
  }

  return resi;
}

int safe_shl (int lhs, int rhs) {
  if ((unsigned int)rhs >= 32u) {
    php_warning ("Wrong right parameter %d in << operator", rhs);
    rhs = 0;
  }
  int res = lhs << rhs;
  if ((res >> rhs) != lhs) {
    php_warning ("Integer overflow in %d << %d", lhs, rhs);
  }
  return res;
}


double& safe_set_add (double &lhs, double rhs) {
  return lhs += rhs;
}

double& safe_set_sub (double &lhs, double rhs) {
  return lhs -= rhs;
}

double& safe_set_mul (double &lhs, double rhs) {
  return lhs *= rhs;
}


int safe_add (bool lhs, bool rhs) {
  return lhs + rhs;
}

int safe_add (bool lhs, int rhs) {
  return lhs + rhs;
}

double safe_add (bool lhs, double rhs) {
  return lhs + rhs;
}

int safe_add (int lhs, bool rhs) {
  return lhs + rhs;
}

double safe_add (int lhs, double rhs) {
  return lhs + rhs;
}

double safe_add (double lhs, bool rhs) {
  return lhs + rhs;
}

double safe_add (double lhs, int rhs) {
  return lhs + rhs;
}

double safe_add (double lhs, double rhs) {
  return lhs + rhs;
}

int safe_sub (bool lhs, bool rhs) {
  return lhs - rhs;
}

int safe_sub (bool lhs, int rhs) {
  return lhs - rhs;
}

double safe_sub (bool lhs, double rhs) {
  return lhs - rhs;
}

int safe_sub (int lhs, bool rhs) {
  return lhs - rhs;
}

double safe_sub (int lhs, double rhs) {
  return lhs - rhs;
}

double safe_sub (double lhs, bool rhs) {
  return lhs - rhs;
}

double safe_sub (double lhs, int rhs) {
  return lhs - rhs;
}

double safe_sub (double lhs, double rhs) {
  return lhs - rhs;
}

int safe_mul (bool lhs, bool rhs) {
  return lhs * rhs;
}

int safe_mul (bool lhs, int rhs) {
  return lhs * rhs;
}

double safe_mul (bool lhs, double rhs) {
  return lhs * rhs;
}

int safe_mul (int lhs, bool rhs) {
  return lhs * rhs;
}

double safe_mul (int lhs, double rhs) {
  return lhs * rhs;
}

double safe_mul (double lhs, bool rhs) {
  return lhs * rhs;
}

double safe_mul (double lhs, int rhs) {
  return lhs * rhs;
}

double safe_mul (double lhs, double rhs) {
  return lhs * rhs;
}


int& safe_incr_pre (int &lhs) {
  if (lhs == INT_MAX) {
    php_warning ("Integer overflow in ++%d", lhs);
  }
  return ++lhs;
}

int& safe_decr_pre (int &lhs) {
  if (lhs == INT_MIN) {
    php_warning ("Integer overflow in --%d", lhs);
  }
  return --lhs;
}

int safe_incr_post (int &lhs) {
  if (lhs == INT_MAX) {
    php_warning ("Integer overflow in %d++", lhs);
  }
  return lhs++;
}

int safe_decr_post (int &lhs) {
  if (lhs == INT_MIN) {
    php_warning ("Integer overflow in %d--", lhs);
  }
  return lhs--;
}


double& safe_incr_pre (double &lhs) {
  return ++lhs;
}

double& safe_decr_pre (double &lhs) {
  return --lhs;
}

double safe_incr_post (double &lhs) {
  return lhs++;
}

double safe_decr_post (double &lhs) {
  return lhs--;
}


var& safe_incr_pre (var &lhs) {
  return lhs.safe_incr_pre();
}

var& safe_decr_pre (var &lhs) {
  return lhs.safe_decr_pre();
}

var safe_incr_post (var &lhs) {
  return lhs.safe_incr_post();
}

var safe_decr_post (var &lhs) {
  return lhs.safe_decr_post();
}


bool eq2 (bool lhs, bool rhs) {
  return lhs == rhs;
}

bool eq2 (int lhs, int rhs) {
  return lhs == rhs;
}

bool eq2 (double lhs, double rhs) {
  return lhs == rhs;
}

bool eq2 (bool lhs, int rhs) {
  return lhs != !rhs;
}

bool eq2 (bool lhs, double rhs) {
  return lhs != (rhs == 0.0);
}

bool eq2 (int lhs, bool rhs) {
  return rhs != !lhs;
}

bool eq2 (double lhs, bool rhs) {
  return rhs != (lhs == 0.0);
}

bool eq2 (int lhs, double rhs) {
  return lhs == rhs;
}

bool eq2 (double lhs, int rhs) {
  return lhs == rhs;
}


bool eq2 (bool lhs, const string &rhs) {
  return lhs == rhs.to_bool();
}

bool eq2 (int lhs, const string &rhs) {
  return lhs == rhs.to_float();
}

bool eq2 (double lhs, const string &rhs) {
  return lhs == rhs.to_float();
}

bool eq2 (const string &lhs, bool rhs) {
  return rhs == lhs.to_bool();
}

bool eq2 (const string &lhs, int rhs) {
  return rhs == lhs.to_float();
}

bool eq2 (const string &lhs, double rhs) {
  return rhs == lhs.to_float();
}


template <class T>
bool eq2 (bool lhs, const array <T> &rhs) {
  return lhs == !rhs.empty();
}

template <class T>
bool eq2 (int lhs, const array <T> &rhs) {
  php_warning ("Unsupported operand types for operator == (int and array)");
  return false;
}

template <class T>
bool eq2 (double lhs, const array <T> &rhs) {
  php_warning ("Unsupported operand types for operator == (float and array)");
  return false;
}

template <class T>
bool eq2 (const string &lhs, const array <T> &rhs) {
  php_warning ("Unsupported operand types for operator == (string and array)");
  return false;
}

template <class T>
bool eq2 (const array <T> &lhs, bool rhs) {
  return rhs == !lhs.empty();
}

template <class T>
bool eq2 (const array <T> &lhs, int rhs) {
  php_warning ("Unsupported operand types for operator == (array and int)");
  return false;
}

template <class T>
bool eq2 (const array <T> &lhs, double rhs) {
  php_warning ("Unsupported operand types for operator == (array and float)");
  return false;
}

template <class T>
bool eq2 (const array <T> &lhs, const string &rhs) {
  php_warning ("Unsupported operand types for operator == (array and string)");
  return false;
}


template <class T>
bool eq2 (bool lhs, const object_ptr <T> &rhs) {
  return lhs;
}

template <class T>
bool eq2 (int lhs, const object_ptr <T> &rhs) {
  php_warning ("Unsupported operand types for operator == (int and object)");
  return false;
}

template <class T>
bool eq2 (double lhs, const object_ptr <T> &rhs) {
  php_warning ("Unsupported operand types for operator == (float and object)");
  return false;
}

template <class T>
bool eq2 (const string &lhs, const object_ptr <T> &rhs) {
  php_warning ("Unsupported operand types for operator == (string and object)");
  return false;
}

template <class T, class T1>
bool eq2 (const array <T1> &lhs, const object_ptr <T> &rhs) {
  php_warning ("Unsupported operand types for operator == (array and object)");
  return false;
}

template <class T>
bool eq2 (const object_ptr <T> &lhs, bool rhs) {
  return rhs;
}

template <class T>
bool eq2 (const object_ptr <T> &lhs, int rhs) {
  php_warning ("Unsupported operand types for operator == (object and int)");
  return false;
}

template <class T>
bool eq2 (const object_ptr <T> &lhs, double rhs) {
  php_warning ("Unsupported operand types for operator == (object and float)");
  return false;
}

template <class T>
bool eq2 (const object_ptr <T> &lhs, const string &rhs) {
  php_warning ("Unsupported operand types for operator == (object and string)");
  return false;
}

template <class T, class T1>
bool eq2 (const object_ptr <T> &lhs, const array <T1> &rhs) {
  php_warning ("Unsupported operand types for operator == (object and array)");
  return false;
}


bool eq2 (bool lhs, const var &rhs) {
  return lhs == rhs.to_bool();
}

bool eq2 (int lhs, const var &rhs) {
  switch (rhs.type) {
    case var::NULL_TYPE:
      return lhs == 0;
    case var::BOOLEAN_TYPE:
      return !!lhs == rhs.b;
    case var::INTEGER_TYPE:
      return lhs == rhs.i;
    case var::FLOAT_TYPE:
      return lhs == rhs.f;
    case var::STRING_TYPE:
      return lhs == STRING(rhs.s)->to_float();
    case var::ARRAY_TYPE:
      php_warning ("Unsupported operand types for operator == (int and array)");
      return false;
    case var::OBJECT_TYPE:
      php_warning ("Unsupported operand types for operator == (int and object)");
      return false;
    default:
      php_assert (0);
      exit (1);
  }
}

bool eq2 (double lhs, const var &rhs) {
  switch (rhs.type) {
    case var::NULL_TYPE:
      return lhs == 0.0;
    case var::BOOLEAN_TYPE:
      return (lhs != 0.0) == rhs.b;
    case var::INTEGER_TYPE:
      return lhs == rhs.i;
    case var::FLOAT_TYPE:
      return lhs == rhs.f;
    case var::STRING_TYPE:
      return lhs == STRING(rhs.s)->to_float();
    case var::ARRAY_TYPE:
      php_warning ("Unsupported operand types for operator == (float and array)");
      return false;
    case var::OBJECT_TYPE:
      php_warning ("Unsupported operand types for operator == (float and object)");
      return false;
    default:
      php_assert (0);
      exit (1);
  }
}

bool eq2 (const string &lhs, const var &rhs) {
  return eq2 (var (lhs), rhs);
}

template <class T>
bool eq2 (const array <T> &lhs, const var &rhs) {
  if (likely (rhs.is_array())) {
    return eq2 (lhs, *ARRAY(rhs.a));
  }

  if (rhs.is_bool()) {
    return lhs.empty() != rhs.b;
  }
  if (rhs.is_null()) {
    return lhs.empty();
  }

  php_warning ("Unsupported operand types for operator == (array and %s)", rhs.get_type_c_str());
  return false;
}

template <class T>
bool eq2 (const object_ptr <T> &lhs, const var &rhs) {
  if (likely (rhs.is_object())) {
    return eq2 (lhs, *OBJECT(rhs.o));
  }

  if (rhs.is_bool()) {
    return rhs.b;
  }
  if (rhs.is_null()) {
    return false;
  }

  php_warning ("Unsupported operand types for operator == (object and %s)", rhs.get_type_c_str());
  return false;
}

bool eq2 (const var &lhs, bool rhs) {
  return rhs == lhs.to_bool();
}

bool eq2 (const var &lhs, int rhs) {
  switch (lhs.type) {
    case var::NULL_TYPE:
      return rhs == 0;
    case var::BOOLEAN_TYPE:
      return !!rhs == lhs.b;
    case var::INTEGER_TYPE:
      return rhs == lhs.i;
    case var::FLOAT_TYPE:
      return rhs == lhs.f;
    case var::STRING_TYPE:
      return rhs == STRING(lhs.s)->to_float();
    case var::ARRAY_TYPE:
      php_warning ("Unsupported operand types for operator == (array and int)");
      return false;
    case var::OBJECT_TYPE:
      php_warning ("Unsupported operand types for operator == (object and int)");
      return false;
    default:
      php_assert (0);
      exit (1);
  }
}

bool eq2 (const var &lhs, double rhs) {
  switch (lhs.type) {
    case var::NULL_TYPE:
      return rhs == 0.0;
    case var::BOOLEAN_TYPE:
      return (rhs != 0.0) == lhs.b;
    case var::INTEGER_TYPE:
      return rhs == lhs.i;
    case var::FLOAT_TYPE:
      return rhs == lhs.f;
    case var::STRING_TYPE:
      return rhs == STRING(lhs.s)->to_float();
    case var::ARRAY_TYPE:
      php_warning ("Unsupported operand types for operator == (array and float)");
      return false;
    case var::OBJECT_TYPE:
      php_warning ("Unsupported operand types for operator == (object and float)");
      return false;
    default:
      php_assert (0);
      exit (1);
  }
}

bool eq2 (const var &lhs, const string &rhs) {
  return eq2 (var (rhs), lhs);
}

template <class T>
bool eq2 (const var &lhs, const array <T> &rhs) {
  if (likely (lhs.is_array())) {
    return eq2 (*ARRAY(lhs.a), rhs);
  }

  if (lhs.is_bool()) {
    return rhs.empty() != lhs.b;
  }
  if (lhs.is_null()) {
    return rhs.empty();
  }

  php_warning ("Unsupported operand types for operator == (%s and array)", lhs.get_type_c_str());
  return false;
}

template <class T>
bool eq2 (const var &lhs, const object_ptr <T> &rhs) {
  if (likely (lhs.is_object())) {
    return eq2 (*OBJECT(lhs.o), rhs);
  }

  if (lhs.is_bool()) {
    return lhs.b;
  }
  if (lhs.is_null()) {
    return false;
  }

  php_warning ("Unsupported operand types for operator == (%s and object)", lhs.get_type_c_str());
  return false;
}


template <class T1, class T2>
bool neq2 (const T1 &lhs, const T2 &rhs) {
  return !eq2 (lhs, rhs);
}


bool equals (bool lhs, const var &rhs) {
  return rhs.is_bool() && equals (lhs, rhs.b);
}

bool equals (int lhs, const var &rhs) {
  return rhs.is_int() && equals (lhs, rhs.i);
}

bool equals (double lhs, const var &rhs) {
  return rhs.is_float() && equals (lhs, rhs.f);
}

bool equals (const string &lhs, const var &rhs) {
  return rhs.is_string() && equals (lhs, *STRING(rhs.s));
}

template <class T>
bool equals (const array <T> &lhs, const var &rhs) {
  return rhs.is_array() && equals (lhs, *ARRAY(rhs.a));
}

template <class T>
bool equals (const object_ptr <T> &lhs, const var &rhs) {
  return rhs.is_object() && equals (lhs, *OBJECT(rhs.o));
}

bool equals (const var &lhs, bool rhs) {
  return lhs.is_bool() && equals (rhs, lhs.b);
}

bool equals (const var &lhs, int rhs) {
  return lhs.is_int() && equals (rhs, lhs.i);
}

bool equals (const var &lhs, double rhs) {
  return lhs.is_float() && equals (rhs, lhs.f);
}

bool equals (const var &lhs, const string &rhs) {
  return lhs.is_string() && equals (rhs, *STRING(lhs.s));
}

template <class T>
bool equals (const var &lhs, const array <T> &rhs) {
  return lhs.is_array() && equals (rhs, *ARRAY(lhs.a));
}

template <class T>
bool equals (const var &lhs, const object_ptr <T> &rhs) {
  return lhs.is_object() && equals (rhs, *OBJECT(lhs.o));
}

template <class T>
bool equals (const T &lhs, const T &rhs) {
  return lhs == rhs;
}

template <class T1, class T2>
bool equals (const T1 &lhs, const T2 &rhs) {
  return false;
}


template <class T1, class T2>
bool not_equals (const T1 &lhs, const T2 &rhs) {
  return !equals (lhs, rhs);
}


template <class T>
bool eq2 (const var &v, const OrFalse <T> &value) {
  return likely (value.bool_value) ? eq2 (value.value, v) : eq2 (false, v);
}

template <class T>
bool eq2 (const OrFalse <T> &value, const var &v) {
  return likely (value.bool_value) ? eq2 (value.value, v) : eq2 (false, v);
}

template <class T>
bool equals (const OrFalse <T> &value, const var &v) {
  return likely (value.bool_value) ? equals (value.value, v) : equals (false, v);
}

template <class T>
bool equals (const var &v, const OrFalse <T> &value) {
  return likely (value.bool_value) ? equals (value.value, v) : equals (false, v);
}

template <class T>
bool not_equals (const OrFalse <T> &value, const var &v) {
  return likely (value.bool_value) ? not_equals (value.value, v) : not_equals (false, v);
}

template <class T>
bool not_equals (const var &v, const OrFalse <T> &value) {
  return likely (value.bool_value) ? not_equals (value.value, v) : not_equals (false, v);
}
