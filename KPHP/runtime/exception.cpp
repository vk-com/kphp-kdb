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

#include "exception.h"

#include "string_functions.h"//strtoupper

array <array <string> > f$debug_backtrace (void) {
  dl::enter_critical_section();//OK
  void *buffer[64];
  int nptrs = fast_backtrace (buffer, 64);
  dl::leave_critical_section();

  const string function_key ("function", 8);

  array <array <string> > res (array_size (nptrs - 4, 0, true));
  char buf[20];
  for (int i = 1; i < nptrs; i++) {
    array <string> current (array_size (0, 1, false));
    dl::enter_critical_section();//OK
    snprintf (buf, 19, "%p", buffer[i]);
    dl::leave_critical_section();
    current.set_value (function_key, string (buf, (dl::size_type)strlen (buf)));
    res.push_back (current);
  }

  return res;

/*  dl::enter_critical_section();//NOT OK
  void *buffer[64];
  int nptrs = fast_backtrace (buffer, 64);
  char **names = backtrace_symbols (buffer, nptrs);
  php_assert (names != NULL);

  const string file_key ("file", 4);
  const string function_key ("function", 8);

  array <array <string> > res (array_size (nptrs - 1, 0, true));
  for (int i = 1; i < nptrs; i++) {
    array <string> current (array_size (0, 2, false));
    char *name_pos = strpbrk (names[i], "( ");
    php_assert (name_pos != NULL);

    current.set_value (file_key, string (names[i], name_pos - names[i]));

    if (*name_pos == '(') {
      ++name_pos;
      char *end = strchr (name_pos, '+');
      if (end == NULL) {
        current.set_value (function_key, string());
      } else {
        string mangled_name (name_pos, (dl::size_type)(end - name_pos));

        int status;
        char *real_name = abi::__cxa_demangle (mangled_name.c_str(), NULL, NULL, &status);
        if (status < 0) {
          current.set_value (function_key, mangled_name);
        } else {
          current.set_value (function_key, string (real_name, strlen (real_name)));
          free (real_name);
        }
      }
    }

    string &func = current[function_key];
    if (func[0] == 'f' && func[1] == '$') {
      const char *s = static_cast <const char *> (memchr (static_cast <void *> (func.c_str() + 2), '(', func.size() - 2));
      if (s != NULL) {
        func = func.substr (2, s - func.c_str() - 2);
        if (func[0] == '_' && func[1] == 't' && func[2] == '_' && func[3] == 's' && func[4] == 'r' && func[5] == 'c' && func[6] == '_' && (int)func.size() > 18) {
          func = func.substr (7, func.size() - 18);
          func.append (".php", 4);
        }

        res.push_back (current);
      }
    }
  }

  free (names);
  dl::leave_critical_section();
  return res;*/
}


#ifdef FAST_EXCEPTIONS
Exception *CurException;
#endif

Exception& Exception::operator = (bool value) {
  bool_value = value;
  return *this;
}
Exception::Exception():
    bool_value (false) {
}

Exception::Exception (bool value) {
  bool_value = value;
}


Exception::Exception (const string &file, int line, const string &message, int code):
    bool_value (true),
    message (message),
    code (code),
    file (file),
    line (line),
    trace (f$debug_backtrace()) {
}

string f$exception_getMessage (const Exception &e) {
  return e.message;
}

int f$exception_getCode (const Exception &e) {
  return e.code;
}

string f$exception_getFile (const Exception &e) {
  return e.file;
}

int f$exception_getLine (const Exception &e) {
  return e.line;
}

array <array <string> > f$exception_getTrace (const Exception &e) {
  return e.trace;
}

Exception f$new_Exception (const string &file, int line, const string &message, int code) {
  return Exception (file, line, message, code);
}

Exception f$err (const string &file, int line, const string &code, const string &desc) {
  return Exception (file, line, (static_SB.clean() + "ERR_" + f$strtoupper (code) + ": " + desc).str(), 0);
}


bool f$boolval (const Exception &my_exception) {
  return f$boolval (my_exception.bool_value);
}

bool eq2 (const Exception &my_exception, bool value) {
  return my_exception.bool_value == value;
}

bool eq2 (bool value, const Exception &my_exception) {
  return value == my_exception.bool_value;
}

bool equals (bool value, const Exception &my_exception) {
  return equals (value, my_exception.bool_value);
}

bool equals (const Exception &my_exception, bool value) {
  return equals (my_exception.bool_value, value);
}

bool not_equals (bool value, const Exception &my_exception) {
  return not_equals (value, my_exception.bool_value);
}

bool not_equals (const Exception &my_exception, bool value) {
  return not_equals (my_exception.bool_value, value);
}


string f$exception_getTraceAsString (const Exception &e) {
  static_SB.clean();
  for (int i = 0; i < e.trace.count(); i++) {
    array <string> current = e.trace.get_value (i);
    static_SB + '#' + i + ' ' + current.get_value (string ("file", 4)) + ": " + current.get_value (string ("function", 8)) + "\n";
  }
  return static_SB.str();
}


void exception_init_static (void) {
#ifdef FAST_EXCEPTIONS
  CurException = NULL;
#endif
}