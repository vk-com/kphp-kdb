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

#define _FILE_OFFSET_BITS 64

#include <cstddef>
#include <re2/re2.h>

#include "regexp.h"

int preg_replace_count_dummy;

static re2::StringPiece RE2_submatch[MAX_SUBPATTERNS];
int regexp::submatch[3 * MAX_SUBPATTERNS];
pcre_extra regexp::extra;

regexp::regexp (void):
    subpatterns_count (0),
    named_subpatterns_count (0),
    is_utf8 (false),
    is_static (false),
    subpattern_names (NULL),
    pcre_regexp (NULL),
    RE2_regexp (NULL) {
}

regexp::regexp (const string &regexp_string):
    subpatterns_count (0),
    named_subpatterns_count (0),
    is_utf8 (false),
    is_static (false),
    subpattern_names (NULL),
    pcre_regexp (NULL),
    RE2_regexp (NULL) {
  init (regexp_string);
}

regexp::regexp (const char *regexp_string, int regexp_len):
    subpatterns_count (0),
    named_subpatterns_count (0),
    is_utf8 (false),
    is_static (false),
    subpattern_names (NULL),
    pcre_regexp (NULL),
    RE2_regexp (NULL) {
  init (regexp_string, regexp_len);
}

bool regexp::is_valid_RE2_regexp (const char *regexp_string, int regexp_len) {
//  return false;
  int brackets_depth = 0;
  bool prev_is_group = false;

  for (int i = 0; i < regexp_len; i++) {
    switch (regexp_string[i]) {
      case -128 ... -1:
      case 1 ... 35:
      case 37 ... 39:
      case ',' ... '>':
      case '@':
      case 'A' ... 'Z':
      case ']':
      case '_':
      case '`':
      case 'a' ... 'z':
      case '}' ... 127:
        prev_is_group = true;
        break;
      case '$':
      case '^':
      case '|':
        prev_is_group = false;
        break;
      case 0:
        php_warning ("Regexp contains symbol with code 0 after %s", regexp_string);
        return false;        
      case '(':
        brackets_depth++;

        if (regexp_string[i + 1] == '*') {
          return false;
        }

        if (regexp_string[i + 1] == '?') {
          if (regexp_string[i + 2] == ':') {
            i += 2;
            break;
          }
          if (regexp_string[i + 2] == ':') {
            i += 2;
            break;
          }
          return false;
        }
        prev_is_group = false;
        break;
      case ')':
        brackets_depth--;
        if (brackets_depth < 0) {
          return false;
        }
        prev_is_group = true;
        break;
      case '+':
      case '*':
        if (!prev_is_group) {
          return false;
        }

        prev_is_group = false;
        break;
      case '?':
        if (!prev_is_group && (i == 0 || (regexp_string[i - 1] != '+' && regexp_string[i - 1] != '*' && regexp_string[i - 1] != '?'))) {
          return false;
        }

        prev_is_group = false;
        break;
      case '{': {
        if (!prev_is_group) {
          return false;
        }

        i++;
        int k = 0;
        while ('0' <= regexp_string[i] && regexp_string[i] <= '9') {
          i++;
          k++;
        }

        if (k > 2) {
          return false;
        }

        if (regexp_string[i] == ',') {
          i++;
        }

        k = 0;
        while ('0' <= regexp_string[i] && regexp_string[i] <= '9') {
          k++;
          i++;
        }

        if (k > 2) {
          return false;
        }

        if (regexp_string[i] != '}') {
          php_warning ("Wrong regexp %s", regexp_string);
          return false;
        }

        prev_is_group = false;
        break;
      }
      case '[':
        if (regexp_string[i + 1] == '^') {
          i++;
        }
        if (regexp_string[i + 1] == ']') {
          i++;
        }
        while (true) {
          while (++i < regexp_len && regexp_string[i] != '\\' && regexp_string[i] != ']' && regexp_string[i] != '[') {
          }

          if (i == regexp_len) {
            return false;
          }

          if (regexp_string[i] == '\\') {
            switch (regexp_string[i + 1]) {
              case 'x':
                if (isxdigit (regexp_string[i + 2]) &&
                    isxdigit (regexp_string[i + 3])) {
                  i += 3;
                  continue;
                }
                return false;
              case '0':
                if ('0' <= regexp_string[i + 2] && regexp_string[i + 2] <= '7' && 
                    '0' <= regexp_string[i + 3] && regexp_string[i + 3] <= '7') {
                  i += 3;
                  continue;
                }
                return false;
              case 'a':
              case 'f':
              case 'n':
              case 'r':
              case 't':
              case 'd':
              case 'D':
              case 's':
              case 'S':
              case 'w':
              case 'W':
              case -128 ... '/':
              case ':' ... '@':
              case '[' ... '`':
              case '{' ... 127:
                i++;
                continue;
              default:
                return false;
            }
          } else if (regexp_string[i] == '[') {
            if (regexp_string[i + 1] == ':') {
              return false;
            }
          } else if (regexp_string[i] == ']') {
            break;
          }
        }
        prev_is_group = true;
        break;
      case '\\':
        switch (regexp_string[i + 1]) {
          case 'x':
            if (isxdigit (regexp_string[i + 2]) &&
                isxdigit (regexp_string[i + 3])) {
              i += 3;
              break;
            }
            return false;
          case '0':
            if ('0' <= regexp_string[i + 2] && regexp_string[i + 2] <= '7' && 
                '0' <= regexp_string[i + 3] && regexp_string[i + 3] <= '7') {
              i += 3;
              break;
            }
            return false;
          case 'a':
          case 'f':
          case 'n':
          case 'r':
          case 't':
          case 'b':
          case 'B':
          case 'd':
          case 'D':
          case 's':
          case 'S':
          case 'w':
          case 'W':
          case -128 ... '/':
          case ':' ... '@':
          case '[' ... '`':
          case '{' ... 127:
            i++;
            break;
          default:
            return false;
        }
        
        prev_is_group = true;
        break;
      default:
        php_critical_error ("Wrong char range assumed");
    }
  }
  if (brackets_depth != 0) {
    php_warning ("Brackets mismatch in regexp %s", regexp_string);
    return false;
  }

  return true;
}

void regexp::init (const string &regexp_string) {
  static char regexp_cache_storage[sizeof (array <regexp *>)];
  static array <regexp *> *regexp_cache = (array <regexp *> *)regexp_cache_storage;
  static long long regexp_last_query_num = -1;

  is_static = (dl::memory_begin == 0);

  if (!is_static) {
    if (dl::query_num != regexp_last_query_num) {
      new (regexp_cache_storage) array <regexp *>();
      regexp_last_query_num = dl::query_num;
    }

    regexp *re = regexp_cache->get_value (regexp_string);
    if (re != NULL) {
      php_assert (!re->is_static);

      subpatterns_count = re->subpatterns_count;
      named_subpatterns_count = re->named_subpatterns_count;
      is_utf8 = re->is_utf8;
      is_static = re->is_static;

      subpattern_names = re->subpattern_names;

      pcre_regexp = re->pcre_regexp;
      RE2_regexp = re->RE2_regexp;

      return;
    }
  }

  init (regexp_string.c_str(), regexp_string.size());

  if (!is_static) {
    regexp *re = static_cast <regexp *> (dl::allocate (sizeof (regexp)));
    new (re) regexp();

    re->subpatterns_count = subpatterns_count;
    re->named_subpatterns_count = named_subpatterns_count;
    re->is_utf8 = is_utf8;
    re->is_static = is_static;

    re->subpattern_names = subpattern_names;

    re->pcre_regexp = pcre_regexp;
    re->RE2_regexp = RE2_regexp;

    regexp_cache->set_value (regexp_string, re);
  }
}

void regexp::init (const char *regexp_string, int regexp_len) {
  if (regexp_len == 0) {
    php_warning ("Empty regular expression");
    return;
  }

  char start_delimiter = regexp_string[0], end_delimiter;
  switch (start_delimiter) {
    case '(':
      end_delimiter = ')';
      break;
    case '[':
      end_delimiter = ']';
      break;
    case '{':
      end_delimiter = '}';
      break;
    case '>':
      end_delimiter = '>';
      break;
    case '!' ... '\'':
    case '*' ... '/':
    case ':':
    case ';':
    case '=':
    case '?':
    case '@':
    case '^':
    case '_':
    case '`':
    case '|':
    case '~':
      end_delimiter = start_delimiter;
      break;
    default:
      php_warning ("Wrong start delimiter in regular expression \"%s\"", regexp_string);
      return;
  }

  int regexp_end = regexp_len - 1;
  while (regexp_end > 0 && regexp_string[regexp_end] != end_delimiter) {
    regexp_end--;
  }

  if (regexp_end == 0) {
    php_warning ("No ending matching delimiter '%c' found", end_delimiter);
    return;
  }

  static_SB.clean().append (regexp_string + 1, regexp_end - 1);

  is_static = (dl::memory_begin == 0);

  dl::use_script_allocator = !is_static;

  is_utf8 = false;
  int pcre_options = 0;
  RE2::Options RE2_options (RE2::Latin1);
  RE2_options.set_log_errors (false);
  bool can_use_RE2 = true;

  for (int i = regexp_end + 1; i < regexp_len; i++) {
    switch (regexp_string[i]) {
      case 'i':
        pcre_options |= PCRE_CASELESS;
        RE2_options.set_case_sensitive (false);
        break;
      case 'm':
        pcre_options |= PCRE_MULTILINE;
        RE2_options.set_one_line (false);
        can_use_RE2 = false;//supported by RE2::Regexp but disabled in an interface while not using posix_syntax
        break;
      case 's':
        pcre_options |= PCRE_DOTALL;
        RE2_options.set_dot_nl (true);
        break;
      case 'x':
        pcre_options |= PCRE_EXTENDED;
        can_use_RE2 = false;
        break;

      case 'A':
        pcre_options |= PCRE_ANCHORED;
        can_use_RE2 = false;
        break;
      case 'D':
        pcre_options |= PCRE_DOLLAR_ENDONLY;
        can_use_RE2 = false;
        break;
      case 'S':
        php_warning ("study doesn't supported");
        break;
      case 'U':
        pcre_options |= PCRE_UNGREEDY;
        can_use_RE2 = false;//supported by RE2::Regexp but there is no such an option
        break;
      case 'X':
        pcre_options |= PCRE_EXTRA;
        break;
      case 'u':
        pcre_options |= PCRE_UTF8;
        RE2_options.set_encoding (RE2::Options::EncodingUTF8);
        is_utf8 = true;
        break;

      default:
        php_warning ("unknown modifier '%c' found", regexp_string[i]);
        clean();
        return;
    }
  }

  can_use_RE2 = can_use_RE2 && is_valid_RE2_regexp (static_SB.c_str(), static_SB.size());

  if (is_utf8 && !mb_UTF8_check (static_SB.c_str())) {
    php_warning ("Regexp \"%s\" contains not UTF-8 symbols\n", static_SB.c_str());
    clean();
    return;
  }
  
  bool need_pcre = false;
  if (can_use_RE2) {
    RE2_regexp = new RE2 (re2::StringPiece (static_SB.c_str(), static_SB.size()), RE2_options);
    if (!RE2_regexp->ok()) {
      php_warning ("RE2 compilation of regexp \"%s\" failed. Error %d at %s\n", static_SB.c_str(), RE2_regexp->error_code(), RE2_regexp->error().c_str());

      delete RE2_regexp;
      RE2_regexp = NULL;
    } else {
      std::string min_str;
      std::string max_str;

      if (!RE2_regexp->PossibleMatchRange (&min_str, &max_str, 1) || min_str.empty()) {//rough estimate for "can match empty string"
        need_pcre = true;
      }
    }

    //We can not mimic PCRE now, but we can't check this. There is no such a function in the interface.
    //So just ignore this distinction
  }

  if (RE2_regexp == NULL || need_pcre) {
    const char *error;
    int erroffset;
    pcre_regexp = pcre_compile (static_SB.c_str(), pcre_options, &error, &erroffset, NULL);

    if (pcre_regexp == NULL) {
      php_warning ("Regexp compilation failed: %s at offset %d", error, erroffset);
      clean();
      return;
    }
  }

  //compile has finished

  named_subpatterns_count = 0;
  if (RE2_regexp) {
    subpatterns_count = RE2_regexp->NumberOfCapturingGroups();
  } else {
    php_assert (pcre_fullinfo (pcre_regexp, NULL, PCRE_INFO_CAPTURECOUNT, &subpatterns_count) == 0);

    if (subpatterns_count) {
      php_assert (pcre_fullinfo (pcre_regexp, NULL, PCRE_INFO_NAMECOUNT, &named_subpatterns_count) == 0);

      if (named_subpatterns_count > 0) {
        subpattern_names = new string[subpatterns_count + 1];

        int name_entry_size;
        php_assert (pcre_fullinfo (pcre_regexp, NULL, PCRE_INFO_NAMEENTRYSIZE, &name_entry_size) == 0);

        char *name_table;
        php_assert (pcre_fullinfo (pcre_regexp, NULL, PCRE_INFO_NAMETABLE, &name_table) == 0);

        for (int i = 0; i < named_subpatterns_count; i++) {
          int name_id = (((unsigned char)name_table[0]) << 8) + (unsigned char)name_table[1];
          string name (name_table + 2, (dl::size_type)strlen (name_table + 2));
          if (name.is_int()) {
            php_warning ("Numeric named subpatterns are not allowed");
          } else {
            subpattern_names[name_id] = name;
          }
          name_table += name_entry_size;
        }
      }
    }
  }
  subpatterns_count++;

  if (subpatterns_count > MAX_SUBPATTERNS) {
    php_warning ("Maximum number of subpatterns %d exceeded, %d subpatterns found", MAX_SUBPATTERNS, subpatterns_count);
    clean();
    return;
  }

  dl::use_script_allocator = false;
}

void regexp::clean (void) {
  if (!is_static) {
    //from cache
    dl::use_script_allocator = false;
    return;
  }

  dl::use_script_allocator = !is_static;

  subpatterns_count = 0;
  named_subpatterns_count = 0;
  is_utf8 = false;
  is_static = false;

  if (pcre_regexp != NULL) {
    pcre_free (pcre_regexp);
    pcre_regexp = NULL;
  }

  delete RE2_regexp;
  RE2_regexp = NULL;

  delete[] subpattern_names;
  subpattern_names = NULL;

  dl::use_script_allocator = false;
}

regexp::~regexp (void) {
  clean();
}


int regexp::pcre_last_error;

int regexp::exec (const string &subject, int offset, bool second_try) const {
  if (RE2_regexp && !second_try) {
    dl::enter_critical_section();//OK
    dl::use_script_allocator = !is_static;
    if (!RE2_regexp->Match (re2::StringPiece (subject.c_str(), (int)subject.size()), offset, (int)subject.size(), RE2::UNANCHORED, RE2_submatch, subpatterns_count)) {
      dl::use_script_allocator = false;
      dl::leave_critical_section();
      return 0;
    }
    dl::use_script_allocator = false;
    dl::leave_critical_section();

    int count = -1;
    for (int i = 0; i < subpatterns_count; i++) {
      if (RE2_submatch[i].data() == NULL) {
        submatch[i + i] = -1;
        submatch[i + i + 1] = -1;
      } else {
        submatch[i + i] = RE2_submatch[i].data() - subject.c_str();
        submatch[i + i + 1] = submatch[i + i] + RE2_submatch[i].size();
        count = i;
      }
    }
    php_assert (count >= 0);

    return count + 1;
  }

  php_assert (pcre_regexp);

  int options = second_try ? PCRE_NO_UTF8_CHECK | PCRE_NOTEMPTY_ATSTART | PCRE_NOTEMPTY_ATSTART : PCRE_NO_UTF8_CHECK;
  dl::enter_critical_section();//OK
  int count = pcre_exec (pcre_regexp, &extra, subject.c_str(), subject.size(), offset, options, submatch, 3 * subpatterns_count);
  dl::leave_critical_section();

  php_assert (count != 0);
  if (count == PCRE_ERROR_NOMATCH) {
    return 0;
  }
  if (count < 0) {
    pcre_last_error = count;
    return 0;
  }

  return count;
}


OrFalse <int> regexp::match (const string &subject, bool all_matches) const {
  pcre_last_error = 0;

  if (pcre_regexp == NULL && RE2_regexp == NULL) {
    return false;
  }

  if (is_utf8 && !mb_UTF8_check (subject.c_str())) {
    pcre_last_error = PCRE_ERROR_BADUTF8;
    return false;
  }

  bool second_try = false;//set after matching an empty string
  pcre_last_error = 0;

  int result = 0;
  int offset = 0;
  while (offset <= (int)subject.size()) {
    if (exec (subject, offset, second_try) == 0) {
      if (second_try) {
        second_try = false;
        do {
          offset++;
        } while (is_utf8 && offset < (int)subject.size() && (((unsigned char)subject[offset]) & 0xc0) == 0x80);
        continue;
      }

      break;
    }
    
    result++;

    second_try = (submatch[0] == submatch[1]);

    offset = submatch[1];
  }

  if (pcre_last_error == 0) {
    return result;
  } else {
    return false;
  }
}

OrFalse <int> regexp::match (const string &subject, var &matches, bool all_matches) const {
  pcre_last_error = 0;

  if (pcre_regexp == NULL && RE2_regexp == NULL) {
    return false;
  }

  if (is_utf8 && !mb_UTF8_check (subject.c_str())) {
    pcre_last_error = PCRE_ERROR_BADUTF8;
    return false;
  }

  if (all_matches) {
    matches = array <var> (array_size (subpatterns_count, named_subpatterns_count, named_subpatterns_count == 0));
    for (int i = 0; i < subpatterns_count; i++) {
      if (named_subpatterns_count && !subpattern_names[i].empty()) {
        matches.set_value (subpattern_names[i], array <var> ());
      }
      matches.push_back (array <var> ());
    }
  }

  bool second_try = false;//set after matching an empty string
  pcre_last_error = 0;

  int result = 0;
  int offset = 0;
  while (offset <= (int)subject.size()) {
    int count = exec (subject, offset, second_try);

    if (count == 0) {
      if (second_try) {
        second_try = false;
        do {
          offset++;
        } while (is_utf8 && offset < (int)subject.size() && (((unsigned char)subject[offset]) & 0xc0) == 0x80);
        continue;
      }

      if (!all_matches) {
        matches = array <var> ();
      }

      break;
    }
    
    result++;

    if (all_matches) {
      for (int i = 0; i < subpatterns_count; i++) {
        const string match_str (subject.c_str() + submatch[i + i], submatch[i + i + 1] - submatch[i + i]);
        if (named_subpatterns_count && subpattern_names[i].size()) {
          matches[subpattern_names[i]].push_back (match_str);
        }
        matches[i].push_back (match_str);
      }
    } else {
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

      matches = result_set;
      break;
    }

    second_try = (submatch[0] == submatch[1]);

    offset = submatch[1];
  }

  if (pcre_last_error == 0) {
    return result;
  } else {
    return false;
  }
}

OrFalse <int> regexp::match (const string &subject, var &matches, int flags, bool all_matches) const {
  pcre_last_error = 0;

  if (pcre_regexp == NULL && RE2_regexp == NULL) {
    return false;
  }

  if (is_utf8 && !mb_UTF8_check (subject.c_str())) {
    pcre_last_error = PCRE_ERROR_BADUTF8;
    return false;
  }

  bool offset_capture = false;
  if (flags & PREG_OFFSET_CAPTURE) {
    flags &= ~PREG_OFFSET_CAPTURE;
    offset_capture = true;
  }

  bool pattern_order = all_matches;
  if (all_matches) {
    if (flags & PREG_PATTERN_ORDER) {
      flags &= ~PREG_PATTERN_ORDER;
    } else if (flags & PREG_SET_ORDER) {
      flags &= ~PREG_SET_ORDER;
      pattern_order = false;
    }
  }

  if (flags) {
    php_warning ("Invalid parameter flags specified to preg_match%s", all_matches ? "_all" : "");
  }

  if (pattern_order) {
    matches = array <var> (array_size (subpatterns_count, named_subpatterns_count, named_subpatterns_count == 0));
    for (int i = 0; i < subpatterns_count; i++) {
      if (named_subpatterns_count && !subpattern_names[i].empty()) {
        matches.set_value (subpattern_names[i], array <var> ());
      }
      matches.push_back (array <var> ());
    }
  } else {
    matches = array <var> ();
  }

  bool second_try = false;//set after matching an empty string

  int result = 0;
  int offset = 0;
  array <var> empty_match (string(), -1);
  while (offset <= (int)subject.size()) {
    int count = exec (subject, offset, second_try);

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
    
    result++;

    if (pattern_order) {
      for (int i = 0; i < subpatterns_count; i++) {
        const string match_str (subject.c_str() + submatch[i + i], submatch[i + i + 1] - submatch[i + i]);
        if (offset_capture && (fix_php_bugs || i < count)) {
          array <var> match (match_str, submatch[i + i]);

          if (named_subpatterns_count && subpattern_names[i].size()) {
            matches[subpattern_names[i]].push_back (match);
          }
          matches[i].push_back (match);
        } else {
          if (named_subpatterns_count && subpattern_names[i].size()) {
            matches[subpattern_names[i]].push_back (match_str);
          }
          matches[i].push_back (match_str);
        }
      }
    } else {
      int size = fix_php_bugs ? subpatterns_count : count;
      array <var> result_set (array_size (size, named_subpatterns_count, named_subpatterns_count == 0));

      if (named_subpatterns_count) {
        for (int i = 0; i < size; i++) {
          const string match_str (subject.c_str() + submatch[i + i], submatch[i + i + 1] - submatch[i + i]);

          if (offset_capture) {
            preg_add_match (result_set, array <var> (match_str, submatch[i + i]), subpattern_names[i]);
          } else {
            preg_add_match (result_set, match_str, subpattern_names[i]);
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          const string match_str (subject.c_str() + submatch[i + i], submatch[i + i + 1] - submatch[i + i]);

          if (offset_capture) {
            result_set.push_back (array <var> (match_str, submatch[i + i]));
          } else {
            result_set.push_back (match_str);
          }
        }
      }

      if (all_matches) {
        matches.push_back (result_set);
      } else {
        matches = result_set;
        break;
      }
    }

    second_try = (submatch[0] == submatch[1]);

    offset = submatch[1];
  }

  if (pcre_last_error == 0) {
    return result;
  } else {
    return false;
  }
}

OrFalse <array <var> > regexp::split (const string &subject, int limit, int flags) const {
  pcre_last_error = 0;

  if (pcre_regexp == NULL && RE2_regexp == NULL) {
    return false;
  }

  if (is_utf8 && !mb_UTF8_check (subject.c_str())) {
    pcre_last_error = PCRE_ERROR_BADUTF8;
    return false;
  }

  bool no_empty = false;
  if (flags & PREG_SPLIT_NO_EMPTY) {
    flags &= ~PREG_SPLIT_NO_EMPTY;
    no_empty = true;
  }

  bool delim_capture = false;
  if (flags & PREG_SPLIT_DELIM_CAPTURE) {
    flags &= ~PREG_SPLIT_DELIM_CAPTURE;
    delim_capture = true;
  }

  bool offset_capture = false;
  if (flags & PREG_SPLIT_OFFSET_CAPTURE) {
    flags &= ~PREG_SPLIT_OFFSET_CAPTURE;
    offset_capture = true;
  }

  if (flags) {
    php_warning ("Invalid parameter flags specified to preg_split");
  }

  if (limit == 0 || limit == -1) {
    limit = INT_MAX;
  }

  int offset = 0;
  int last_match = 0;
  bool second_try = false;

  array <var> result;
  while (offset <= (int)subject.size() && limit > 1) {
    int count = exec (subject, offset, second_try);

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

    if (submatch[0] != last_match || !no_empty) {
      string match_str (subject.c_str() + last_match, submatch[0] - last_match);

      if (offset_capture) {
        result.push_back (array <var> (match_str, last_match));
      } else {
        result.push_back (match_str);
      }

      last_match = submatch[1];

      limit--;
    }

    if (delim_capture) {
      for (int i = 1; i < count; i++) {
        if (submatch[i + i + 1] != submatch[i + i] || !no_empty) {
          string match_str (subject.c_str() + submatch[i + i], submatch[i + i + 1] - submatch[i + i]);

          if (offset_capture) {
            result.push_back (array <var> (match_str, submatch[i + i]));
          } else {
            result.push_back (match_str);
          }
        }
      }
    }

    second_try = (submatch[0] == submatch[1]);

    offset = submatch[1];
  }

  if (last_match < (int)subject.size() || !no_empty) {
    string match_str (subject.c_str() + last_match, subject.size() - last_match);

    if (offset_capture) {
      result.push_back (array <var> (match_str, last_match));
    } else {
      result.push_back (match_str);
    }
  }

  if (pcre_last_error == 0) {
    return result;
  } else {
    return false;
  }
}

int regexp::last_error (void) {
  switch (pcre_last_error) {
    case PHP_PCRE_NO_ERROR:
      return PHP_PCRE_NO_ERROR;
    case PCRE_ERROR_MATCHLIMIT:
      return PHP_PCRE_BACKTRACK_LIMIT_ERROR;
    case PCRE_ERROR_RECURSIONLIMIT:
      return PHP_PCRE_RECURSION_LIMIT_ERROR;
    case PCRE_ERROR_BADUTF8:
      return PHP_PCRE_BAD_UTF8_ERROR;
    default:
      php_assert (0);
      exit (1);
  }
}


string f$preg_quote (const string &str, const string &delimiter) {
  int len = str.size();

  static_SB.clean().reserve (4 * len);

  for (int i = 0; i < len; i++) {
    switch (str[i]) {
      case '.':
      case '\\':
      case '+':
      case '*':
      case '?':
      case '[':
      case '^':
      case ']':
      case '$':
      case '(':
      case ')':
      case '{':
      case '}':
      case '=':
      case '!':
      case '>':
      case '<':
      case '|':
      case ':':
      case '-':
        static_SB.append_char ('\\');
        static_SB.append_char (str[i]);
        break;
      case '\0':
        static_SB.append_char ('\\');
        static_SB.append_char ('0');
        static_SB.append_char ('0');
        static_SB.append_char ('0');
        break;
      default:
        if (str[i] == delimiter[0]) {
          static_SB.append_char ('\\');
        }
        static_SB.append_char (str[i]);
        break;
    }
  }

  return static_SB.str();
}


void regexp::init_static (void) {
  pcre_malloc = dl::malloc_replace;
  pcre_free = dl::free_replace;

  extra.flags = PCRE_EXTRA_MATCH_LIMIT | PCRE_EXTRA_MATCH_LIMIT_RECURSION;
  extra.match_limit = PCRE_BACKTRACK_LIMIT;
  extra.match_limit_recursion = PCRE_RECURSION_LIMIT;
}

