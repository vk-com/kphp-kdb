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

#include <pcre.h>

#include "kphp_core.h"
#include "mbstring.h"

namespace re2 {
  class RE2;
};

extern int preg_replace_count_dummy;

const bool fix_php_bugs = false;

const int PREG_PATTERN_ORDER = 1;
const int PREG_SET_ORDER = 2;
const int PREG_OFFSET_CAPTURE = 4;

const int PREG_SPLIT_NO_EMPTY = 8;
const int PREG_SPLIT_DELIM_CAPTURE = 16;
const int PREG_SPLIT_OFFSET_CAPTURE = 32;

const int PCRE_RECURSION_LIMIT = 100000;
const int PCRE_BACKTRACK_LIMIT = 1000000;

const int MAX_SUBPATTERNS = 512;

enum {
  PHP_PCRE_NO_ERROR = 0,
  PHP_PCRE_INTERNAL_ERROR,
  PHP_PCRE_BACKTRACK_LIMIT_ERROR,
  PHP_PCRE_RECURSION_LIMIT_ERROR,
  PHP_PCRE_BAD_UTF8_ERROR,
};


class regexp {
private:
  int subpatterns_count;
  int named_subpatterns_count;
  bool is_utf8;
  bool is_static;

  string *subpattern_names;

  pcre *pcre_regexp;
  re2::RE2 *RE2_regexp;

  void clean (void);

  int exec (const string &subject, int offset, bool second_try) const;

  static bool is_valid_RE2_regexp (const char *regexp_string, int regexp_len);

  static pcre_extra extra;

  static int pcre_last_error;

  static int submatch[3 * MAX_SUBPATTERNS];

  template <class T>
  inline string get_replacement (const T &replace_val, const string &subject, int count) const;

  regexp (const regexp &);//DISABLE copy constructor
  regexp& operator = (const regexp &);//DISABLE copy assignment

public:
  regexp (void);

  explicit regexp (const string &regexp_string);
  regexp (const char *regexp_string, int regexp_len);  

  void init (const string &regexp_string);
  void init (const char *regexp_string, int regexp_len);

  OrFalse <int> match (const string &subject, bool all_matches) const;

  OrFalse <int> match (const string &subject, var &matches, bool all_matches) const;

  OrFalse <int> match (const string &subject, var &matches, int flags, bool all_matches) const;

  OrFalse <array <var> > split (const string &subject, int limit, int flags) const;

  template <class T>
  var replace (const T &replace_val, const string &subject, int limit, int &replace_count) const;

  static int last_error (void);

  ~regexp();

  static void init_static (void);
};


inline void preg_add_match (array <var> &v, const var &match, const string &name);

inline int preg_get_backref (const char **str, int *backref);


inline OrFalse <int> f$preg_match (const regexp &regex, const string &subject);

inline OrFalse <int> f$preg_match_all (const regexp &regex, const string &subject);

inline OrFalse <int> f$preg_match (const regexp &regex, const string &subject, var &matches);

inline OrFalse <int> f$preg_match_all (const regexp &regex, const string &subject, var &matches);

inline OrFalse <int> f$preg_match (const regexp &regex, const string &subject, var &matches, int flags);

inline OrFalse <int> f$preg_match_all (const regexp &regex, const string &subject, var &matches, int flags);

inline OrFalse <int> f$preg_match (const string &regex, const string &subject);

inline OrFalse <int> f$preg_match_all (const string &regex, const string &subject);

inline OrFalse <int> f$preg_match (const string &regex, const string &subject, var &matches);

inline OrFalse <int> f$preg_match_all (const string &regex, const string &subject, var &matches);

inline OrFalse <int> f$preg_match (const string &regex, const string &subject, var &matches, int flags);

inline OrFalse <int> f$preg_match_all (const string &regex, const string &subject, var &matches, int flags);

inline OrFalse <int> f$preg_match (const var &regex, const string &subject);

inline OrFalse <int> f$preg_match_all (const var &regex, const string &subject);

inline OrFalse <int> f$preg_match (const var &regex, const string &subject, var &matches);

inline OrFalse <int> f$preg_match_all (const var &regex, const string &subject, var &matches);

inline OrFalse <int> f$preg_match (const var &regex, const string &subject, var &matches, int flags);

inline OrFalse <int> f$preg_match_all (const var &regex, const string &subject, var &matches, int flags);

inline var f$preg_replace (const regexp &regex, const string &replace_val, const string &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

inline var f$preg_replace (const regexp &regex, const var &replace_val, const string &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

inline var f$preg_replace (const regexp &regex, const string &replace_val, const var &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

inline var f$preg_replace (const regexp &regex, const var &replace_val, const var &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

template <class T1, class T2>
inline var f$preg_replace (const string &regex, const T1 &replace_val, const T2 &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

inline var f$preg_replace (const var &regex, const string &replace_val, const string &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

inline var f$preg_replace (const var &regex, const string &replace_val, const var &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

inline var f$preg_replace (const var &regex, const var &replace_val, const string &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

inline var f$preg_replace (const var &regex, const var &replace_val, const var &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

template <class T>
var f$preg_replace_callback (const regexp &regex, const T &replace_val, const string &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

template <class T>
var f$preg_replace_callback (const regexp &regex, const T &replace_val, const var &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

template <class T, class T2>
var f$preg_replace_callback (const string &regex, const T &replace_val, const T2 &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

template <class T>
var f$preg_replace_callback (const var &regex, const T &replace_val, const string &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

template <class T>
var f$preg_replace_callback (const var &regex, const T &replace_val, const var &subject, int limit = -1, int &replace_count = preg_replace_count_dummy);

inline OrFalse <array <var> > f$preg_split (const regexp &regex, const string &subject, int limit = -1, int flags = 0);

inline OrFalse <array <var> > f$preg_split (const string &regex, const string &subject, int limit = -1, int flags = 0);

inline OrFalse <array <var> > f$preg_split (const var &regex, const string &subject, int limit = -1, int flags = 0);

string f$preg_quote (const string &str, const string &delimiter = string());

inline int f$preg_last_error (void);


/*
 *
 *     IMPLEMENTATION
 *
 */


template <>
inline string regexp::get_replacement (const string &replace_val, const string &subject, int count) const {
  int len = (int)replace_val.size();

  static_SB.clean();
  for (int i = 0; i < len; i++) {
    int backref = -1;
    if (replace_val[i] == '\\' && (replace_val[i + 1] == '\\' || replace_val[i + 1] == '$')) {
      i++;
    } else if ((replace_val[i] == '\\' || replace_val[i] == '$') && '0' <= replace_val[i + 1] && replace_val[i + 1] <= '9') {
      if ('0' <= replace_val[i + 2] && replace_val[i + 2] <= '9') {
        backref = (replace_val[i + 1] - '0') * 10 + (replace_val[i + 2] - '0');
        i += 2;
      } else {
        backref = replace_val[i + 1] - '0';
        i++;
      }
    } else if (replace_val[i] == '$' && replace_val[i + 1] == '{' && '0' <= replace_val[i + 2] && replace_val[i + 2] <= '9') {
      if ('0' <= replace_val[i + 3] && replace_val[i + 3] <= '9') {
        if (replace_val[i + 4] == '}') {
          backref = (replace_val[i + 2] - '0') * 10 + (replace_val[i + 3] - '0');
          i += 4;
        }
      } else {
        if (replace_val[i + 3] == '}') {
          backref = replace_val[i + 2] - '0';
          i += 3;
        }
      }
    }

    if (backref == -1) {
      static_SB += replace_val[i];
    } else {
      if (backref < count) {
        int index = backref + backref;
        static_SB.append (subject.c_str() + submatch[index], submatch[index + 1] - submatch[index]);
      }
    }
  }
  return static_SB.str();//TODO optimize
}

template <class T>
string regexp::get_replacement (const T &replace_val, const string &subject, int count) const {
  int size = fix_php_bugs ? subpatterns_count : count;
  array <var> result_set (array_size (size, named_subpatterns_count, named_subpatterns_count == 0));

  if (named_subpatterns_count) {
    for (int i = 0; i < size; i++) {
      const string match_str (subject.c_str() + submatch[i + i], submatch[i + i + 1] - submatch[i + i]);

      preg_add_match (result_set, match_str, subpattern_names[i]);
    }
  } else {
    for (int i = 0; i < size; i++) {
      result_set.push_back (string (subject.c_str() + submatch[i + i], submatch[i + i + 1] - submatch[i + i]));
    }
  }

  return f$strval (replace_val (result_set));
}


template <class T>
var regexp::replace (const T &replace_val, const string &subject, int limit, int &replace_count) const {
  pcre_last_error = 0;
  int result_count = 0;//calls can be recursive, can't write to replace_count directly

  if (pcre_regexp == NULL && RE2_regexp == NULL) {
    return var();
  }

  if (is_utf8 && !mb_UTF8_check (subject.c_str())) {
    pcre_last_error = PCRE_ERROR_BADUTF8;
    return var();
  }

  if (limit == 0 || limit == -1) {
    limit = INT_MAX;
  }

  int offset = 0;
  int last_match = 0;
  bool second_try = false;

  string result;
  while (offset <= (int)subject.size() && limit > 0) {
    int count = exec (subject, offset, second_try);

//    fprintf (stderr, "Found %d submatches at %d:%d from pos %d, pcre_last_error = %d\n", count, submatch[0], submatch[1], offset, pcre_last_error);
    if (count == 0) {
      if (second_try) {
        second_try = false;
        do {
          offset++;
        } while (is_utf8 && offset < (int)subject.size() && (((unsigned char)subject[offset]) & 0xc0) == 0x80);
        continue;
      }

      break;
    }

    result_count++;
    limit--;

    int match_begin = submatch[0];
    offset = submatch[1];

    result.append (subject.c_str() + last_match, (dl::size_type)(match_begin - last_match));
    result.append (get_replacement (replace_val, subject, count));

    second_try = (match_begin == offset);

    last_match = offset;
  }

  replace_count = result_count;
  if (pcre_last_error) {
    return var();
  }

  if (replace_count == 0) {
    return subject;
  }

  result.append (subject.c_str() + last_match, (dl::size_type)(subject.size() - last_match));
  return result;
}


void preg_add_match (array <var> &v, const var &match, const string &name) {
  if (name.size()) {
    v.set_value (name, match);
  }

  v.push_back (match);
}

OrFalse <int> f$preg_match (const regexp &regex, const string &subject) {
  return regex.match (subject, false);
}

OrFalse <int> f$preg_match_all (const regexp &regex, const string &subject) {
  return regex.match (subject, true);
}

OrFalse <int> f$preg_match (const regexp &regex, const string &subject, var &matches) {
  return regex.match (subject, matches, false);
}

OrFalse <int> f$preg_match_all (const regexp &regex, const string &subject, var &matches) {
  return regex.match (subject, matches, true);
}

OrFalse <int> f$preg_match (const regexp &regex, const string &subject, var &matches, int flags) {
  return regex.match (subject, matches, flags, false);
}

OrFalse <int> f$preg_match_all (const regexp &regex, const string &subject, var &matches, int flags) {
  return regex.match (subject, matches, flags, true);
}

OrFalse <int> f$preg_match (const string &regex, const string &subject) {
  return f$preg_match (regexp (regex), subject);
}

OrFalse <int> f$preg_match_all (const string &regex, const string &subject) {
  return f$preg_match_all (regexp (regex), subject);
}

OrFalse <int> f$preg_match (const string &regex, const string &subject, var &matches) {
  return f$preg_match (regexp (regex), subject, matches);
}

OrFalse <int> f$preg_match_all (const string &regex, const string &subject, var &matches) {
  return f$preg_match_all (regexp (regex), subject, matches);
}

OrFalse <int> f$preg_match (const string &regex, const string &subject, var &matches, int flags) {
  return f$preg_match (regexp (regex), subject, matches, flags);
}

OrFalse <int> f$preg_match_all (const string &regex, const string &subject, var &matches, int flags) {
  return f$preg_match_all (regexp (regex), subject, matches, flags);
}

OrFalse <int> f$preg_match (const var &regex, const string &subject) {
  return f$preg_match (regexp (regex.to_string()), subject);
}

OrFalse <int> f$preg_match_all (const var &regex, const string &subject) {
  return f$preg_match_all (regexp (regex.to_string()), subject);
}

OrFalse <int> f$preg_match (const var &regex, const string &subject, var &matches) {
  return f$preg_match (regexp (regex.to_string()), subject, matches);
}

OrFalse <int> f$preg_match_all (const var &regex, const string &subject, var &matches) {
  return f$preg_match_all (regexp (regex.to_string()), subject, matches);
}

OrFalse <int> f$preg_match (const var &regex, const string &subject, var &matches, int flags) {
  return f$preg_match (regexp (regex.to_string()), subject, matches, flags);
}

OrFalse <int> f$preg_match_all (const var &regex, const string &subject, var &matches, int flags) {
  return f$preg_match_all (regexp (regex.to_string()), subject, matches, flags);
}


var f$preg_replace (const regexp &regex, const string &replace_val, const string &subject, int limit, int &replace_count) {
  return regex.replace (replace_val, subject, limit, replace_count);
}

var f$preg_replace (const regexp &regex, const var &replace_val, const string &subject, int limit, int &replace_count) {
  if (replace_val.is_array()) {
    php_warning ("Parameter mismatch, pattern is a string while replacement is an array");
    return false;
  }

  return f$preg_replace (regex, replace_val.to_string(), subject, limit, replace_count);
}

var f$preg_replace (const regexp &regex, const string &replace_val, const var &subject, int limit, int &replace_count) {
  return f$preg_replace (regex, var (replace_val), subject, limit, replace_count);
}

var f$preg_replace (const regexp &regex, const var &replace_val, const var &subject, int limit, int &replace_count) {
  if (replace_val.is_array()) {
    php_warning ("Parameter mismatch, pattern is a string while replacement is an array");
    return false;
  }

  if (subject.is_array()) {
    replace_count = 0;
    int replace_count_one;
    const array <var> &subject_arr = subject.as_array ("", -1);
    array <var> result (subject_arr.size());
    for (array <var>::const_iterator it = subject_arr.begin(); it != subject_arr.end(); ++it) {
      var cur_result = f$preg_replace (regex, replace_val.to_string(), it.get_value().to_string(), limit, replace_count_one);
      if (!cur_result.is_null()) {
        result.set_value (it.get_key(), cur_result);
        replace_count += replace_count_one;
      }
    }
    return result;
  } else {
    return f$preg_replace (regex, replace_val.to_string(), subject.to_string(), limit, replace_count);
  }
}

template <class T1, class T2>
var f$preg_replace (const string &regex, const T1 &replace_val, const T2 &subject, int limit, int &replace_count) {
  return f$preg_replace (regexp (regex), replace_val, subject, limit, replace_count);
}

var f$preg_replace (const var &regex, const string &replace_val, const string &subject, int limit, int &replace_count) {
  return f$preg_replace (regex, var (replace_val), var (subject), limit, replace_count);
}

var f$preg_replace (const var &regex, const string &replace_val, const var &subject, int limit, int &replace_count) {
  return f$preg_replace (regex, var (replace_val), subject, limit, replace_count);
}

var f$preg_replace (const var &regex, const var &replace_val, const string &subject, int limit, int &replace_count) {
  if (regex.is_array()) {
    var result = subject;

    replace_count = 0;
    int replace_count_one;

    if (replace_val.is_array()) {
      array <var>::const_iterator cur_replace_val = replace_val.begin();

      for (array <var>::const_iterator it = regex.begin(); it != regex.end(); ++it) {
        string replace_value;
        if (cur_replace_val != replace_val.end()) {
          replace_value = cur_replace_val.get_value().to_string();
          ++cur_replace_val;
        }

        result = f$preg_replace (it.get_value().to_string(), replace_value, result.to_string(), limit, replace_count_one);
        replace_count += replace_count_one;
      }
    } else {
      string replace_value = replace_val.to_string();

      for (array <var>::const_iterator it = regex.begin(); it != regex.end(); ++it) {
        result = f$preg_replace (it.get_value().to_string(), replace_value, result.to_string(), limit, replace_count_one);
        replace_count += replace_count_one;
      }
    }
    
    return result;
  } else {
    if (replace_val.is_array()) {
      php_warning ("Parameter mismatch, pattern is a string while replacement is an array");
      return false;
    }

    return f$preg_replace (regex.to_string(), replace_val.to_string(), subject, limit, replace_count);
  }
}

var f$preg_replace (const var &regex, const var &replace_val, const var &subject, int limit, int &replace_count) {
  if (subject.is_array()) {
    replace_count = 0;
    int replace_count_one;
    const array <var> &subject_arr = subject.as_array ("", -1);
    array <var> result (subject_arr.size());
    for (array <var>::const_iterator it = subject_arr.begin(); it != subject_arr.end(); ++it) {
      var cur_result = f$preg_replace (regex, replace_val, it.get_value().to_string(), limit, replace_count_one);
      if (!cur_result.is_null()) {
        result.set_value (it.get_key(), cur_result);
        replace_count += replace_count_one;
      }
    }
    return result;
  } else {
    return f$preg_replace (regex, replace_val, subject.to_string(), limit, replace_count);
  }
}

template <class T>
var f$preg_replace_callback (const regexp &regex, const T &replace_val, const string &subject, int limit, int &replace_count) {
  return regex.replace (replace_val, subject, limit, replace_count);
}

template <class T>
var f$preg_replace_callback (const regexp &regex, const T &replace_val, const var &subject, int limit, int &replace_count) {
  if (subject.is_array()) {
    replace_count = 0;
    int replace_count_one;
    const array <var> &subject_arr = subject.as_array ("", -1);
    array <var> result (subject_arr.size());
    for (array <var>::const_iterator it = subject_arr.begin(); it != subject_arr.end(); ++it) {
      var cur_result = f$preg_replace_callback (regex, replace_val, it.get_value().to_string(), limit, replace_count_one);
      if (!cur_result.is_null()) {
        result.set_value (it.get_key(), cur_result);
        replace_count += replace_count_one;
      }
    }
    return result;
  } else {
    return f$preg_replace_callback (regex, replace_val, subject.to_string(), limit, replace_count);
  }
}

template <class T, class T2>
var f$preg_replace_callback (const string &regex, const T &replace_val, const T2 &subject, int limit, int &replace_count) {
  return f$preg_replace_callback (regexp (regex), replace_val, subject, limit, replace_count);
}

template <class T>
var f$preg_replace_callback (const var &regex, const T &replace_val, const string &subject, int limit, int &replace_count) {
  if (regex.is_array()) {
    var result = subject;

    replace_count = 0;
    int replace_count_one;

    for (array <var>::const_iterator it = regex.begin(); it != regex.end(); ++it) {
      result = f$preg_replace_callback (it.get_value().to_string(), replace_val, result.to_string(), limit, replace_count_one);
      replace_count += replace_count_one;
    }
    
    return result;
  } else {
    return f$preg_replace_callback (regex.to_string(), replace_val, subject, limit, replace_count);
  }
}

template <class T>
var f$preg_replace_callback (const var &regex, const T &replace_val, const var &subject, int limit, int &replace_count) {
  if (subject.is_array()) {
    replace_count = 0;
    int replace_count_one;
    const array <var> &subject_arr = subject.as_array ("", -1);
    array <var> result (subject_arr.size());
    for (array <var>::const_iterator it = subject_arr.begin(); it != subject_arr.end(); ++it) {
      var cur_result = f$preg_replace_callback (regex, replace_val, it.get_value().to_string(), limit, replace_count_one);
      if (!cur_result.is_null()) {
        result.set_value (it.get_key(), cur_result);
        replace_count += replace_count_one;
      }
    }
    return result;
  } else {
    return f$preg_replace_callback (regex, replace_val, subject.to_string(), limit, replace_count);
  }
}

OrFalse <array <var> > f$preg_split (const regexp &regex, const string &subject, int limit, int flags) {
  return regex.split (subject, limit, flags);
}

OrFalse <array <var> > f$preg_split (const string &regex, const string &subject, int limit, int flags) {
  return f$preg_split (regexp (regex), subject, limit, flags);
}

OrFalse <array <var> > f$preg_split (const var &regex, const string &subject, int limit, int flags) {
  return f$preg_split (regexp (regex.to_string()), subject, limit, flags);
}

int f$preg_last_error (void) {
  return regexp::last_error();
}


