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
#define _FILE_OFFSET_BITS 64
#define _GNU_SOURCE 1

#include <cctype>
#include <climits>
#include <cmath>
#include <cstdarg>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <ctime>

#include <cxxabi.h>
#include <execinfo.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <new>

#include "allocator.h"
#include "fast-backtrace.h"
#include "../common-php-functions.h"

#define likely(x) __builtin_expect ((x),1)
#define unlikely(x) __builtin_expect ((x),0)

inline void php_warning (char const *message, ...) __attribute__ ((format (printf, 1, 2)));

inline void php_assert__ (const char *msg, const char *file, int line);

template <class T>
inline void swap (T &lhs, T &rhs);


template <class T, class TT>
class array;

class stdClass;

template <class T>
class object_ptr;

template <class T>
class convert_to;

typedef object_ptr <stdClass> object;

class var;

int php_disable_warnings __attribute__ ((weak)) = 0;
int php_warning_level __attribute__ ((weak)) = 2;

const char *engine_tag __attribute__ ((weak)) = "[";
const char *engine_pid __attribute__ ((weak)) = "] ";

void php_warning (char const *message, ...) {
  if (php_warning_level == 0 || php_disable_warnings) {
    return;
  }

  dl::enter_critical_section();//OK

  va_list args;
  va_start (args, message);

  fprintf (stderr, "%s%d%sWarning: ", engine_tag, (int)time (NULL), engine_pid);
  vfprintf (stderr, message, args);
  fprintf (stderr, "\n");
  va_end (args);

  if (php_warning_level >= 1) {
    fprintf (stderr, "------- Stack Backtrace -------\n");

    void *buffer[64];
    int nptrs = fast_backtrace (buffer, 64);

    if (php_warning_level == 1) {
      for (int i = 1; i + 2 < nptrs; i++) {
        fprintf (stderr, "%p\n", buffer[i]);
      }

    } else if (php_warning_level == 2) {
      char **strings = backtrace_symbols (buffer, nptrs);

      if (strings != NULL) {
        for (int i = 1; i < nptrs; i++) {
          char *mangled_name = NULL, *offset_begin = NULL, *offset_end = NULL;
          for (char *p = strings[i]; *p; ++p) {
            if (*p == '(') {
              mangled_name = p;
            } else if (*p == '+' && mangled_name != NULL) {
              offset_begin = p;
            } else if (*p == ')' && offset_begin != NULL) {
              offset_end = p;
              break;
            }
          }
          if (offset_end != NULL) {
            size_t copy_name_len = offset_begin - mangled_name;
            char *copy_name = (char *)malloc (copy_name_len);
            if (copy_name != NULL) {
              memcpy (copy_name, mangled_name + 1, copy_name_len - 1);
              copy_name[copy_name_len - 1] = 0;

              int status;
              char *real_name = abi::__cxa_demangle (copy_name, NULL, NULL, &status);
              if (status < 0) {
                real_name = copy_name;
              }
              fprintf (stderr, "(%d) %.*s : %s+%.*s%s\n", i, (int)(mangled_name - strings[i]), strings[i], real_name, (int)(offset_end - offset_begin - 1), offset_begin + 1, offset_end + 1);
              if (status == 0) {
                free (real_name);
              }

              free (copy_name);
            }
          }
        }

        free (strings);
      } else {
        backtrace_symbols_fd (buffer, nptrs, 2);
      }
    } else if (php_warning_level == 3) {
      char pid_buf[30];
      sprintf (pid_buf, "%d", getpid());
      char name_buf[512];
      ssize_t res = readlink ("/proc/self/exe", name_buf, 511);
      if (res >= 0) {
        name_buf[res] = 0;
        int child_pid = fork();
        if (!child_pid) {
          dup2 (2, 1); //redirect output to stderr
          execlp ("gdb", "gdb", "--batch", "-n", "-ex", "thread", "-ex", "bt", name_buf, pid_buf, NULL);
          _exit (0); /* If gdb failed to start */
        } else {
          waitpid (child_pid, NULL, 0);
        }
      } else {
        fprintf (stderr, "can't get name of executable file to pass to gdb\n");
      }
    }

    fprintf (stderr, "-------------------------------\n\n");
  }

  dl::leave_critical_section();
}


#define php_assert(EX) (void)((EX) || (php_assert__ (#EX, __FILE__, __LINE__), 0))

void php_assert__ (const char *msg, const char *file, int line) {
  php_warning ("Assertion \"%s\" failed in file %s on line %d", msg, file, line);
  raise (SIGUSR2);
  fprintf (stderr, "_exiting in php_assert\n");
  _exit (1);
}

#define php_critical_error(format, ...) {                                                                 \
  php_warning ("Critical error \"" format "\" in file %s on line %d", ##__VA_ARGS__, __FILE__, __LINE__); \
  raise (SIGUSR2);                                                                                        \
  fprintf (stderr, "_exiting in php_critical_error\n");                                                   \
  _exit (1);                                                                                              \
}


template <class T>
inline void swap (T &lhs, T &rhs) {
  const T copy_lhs (lhs);
  lhs = rhs;
  rhs = copy_lhs;
}


class Unknown {
};

template <class T>
bool eq2 (Unknown lhs, const T &rhs) {
  php_assert ("Comparison of Unknown" && 0);
  return false;
}

template <class T>
bool eq2 (const T &lhs, Unknown rhs) {
  php_assert ("Comparison of Unknown" && 0);
  return false;
}

inline bool eq2 (Unknown lhs, Unknown rhs) {
  php_assert ("Comparison of Unknown" && 0);
  return false;
}

template <class T>
bool equals (Unknown lhs, const T &rhs) {
  php_assert ("Comparison of Unknown" && 0);
  return false;
}

template <class T>
bool equals (const T &lhs, Unknown rhs) {
  php_assert ("Comparison of Unknown" && 0);
  return false;
}

inline bool equals (Unknown lhs, Unknown rhs) {
  php_assert ("Comparison of Unknown" && 0);
  return true;
}


template <class T>
class OrFalse {
public:
  T value;
  bool bool_value;

  OrFalse (void): value(), bool_value() {
  }

  OrFalse (bool x): value(), bool_value (x) {
    php_assert (x == false);
  }

  template <class T1>
  OrFalse (const T1 &x): value (x), bool_value (true) {
  }

  template <class T1>
  OrFalse (const OrFalse <T1> &other): value (other.value), bool_value (other.bool_value) {
  }


  OrFalse& operator = (bool x) {
    value = T();
    bool_value = x;
    php_assert (x == false);
    return *this;
  }

  template <class T1>
  inline OrFalse& operator = (const OrFalse <T1> &other) {
    value = other.value;
    bool_value = other.bool_value;
    return *this;
  }

  template <class T1>
  inline OrFalse& operator = (const T1 &x) {
    value = x;
    bool_value = true;
    return *this;
  }


  T& ref (void) {
    bool_value = true;
    return value;
  }

  T& val (void) {
    return value;
  }

  const T& val (void) const {
    return value;
  }
};


template <>
class OrFalse <bool>;

template <>
class OrFalse <var>;

template <>
template <class T>
class OrFalse <OrFalse <T> >;


template <class T>
inline T min (const T &lhs, const T &rhs) {
  if (lhs < rhs) {
    return lhs;
  }
  return rhs;
}

template <class T>
inline T max (const T &lhs, const T &rhs) {
  if (lhs > rhs) {
    return lhs;
  }
  return rhs;
}
