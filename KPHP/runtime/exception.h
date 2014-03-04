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

array <array <string> > f$debug_backtrace (void);


class Exception {
public:
  bool bool_value;

  string message;
  int code;
  string file;
  int line;
  array <array <string> > trace;

  Exception();
  Exception (const string &file, int line, const string &message = string(), int code = 0);

  Exception& operator = (bool value);
  Exception (bool value);
};

#ifdef FAST_EXCEPTIONS
extern Exception *CurException;

#  define THROW_EXCEPTION(e) CurException = (Exception *)dl::allocate (sizeof (Exception)); new (CurException) Exception ((e))
#  define TRY_CALL(CallT, ResT, call) ({CallT x_tmp___ = (call); if (CurException) return (ResT()); x_tmp___;})
#  define TRY_CALL_VOID(ResT, call) ({(call); if (CurException) return (ResT()); void();})

#  define TRY_CALL_(CallT, call, action) ({CallT x_tmp___ = (call); if (CurException) {action;} x_tmp___;})
#  define TRY_CALL_VOID_(call, action) ({(call); if (CurException) {action;} void();})

#  define TRY_CALL_EXIT(CallT, message, call) ({CallT x_tmp___ = (call); if (CurException) {php_critical_error (message);} x_tmp___;})
#  define TRY_CALL_VOID_EXIT(message, call) ({(call); if (CurException) {php_critical_error (message);} void();})
#  define FREE_EXCEPTION dl::deallocate (CurException, sizeof (Exception)); CurException = NULL
#else
#  define THROW_EXCEPTION(e) throw (e)
#  define TRY_CALL(CallT, ResT, call) (call)
#  define TRY_CALL_VOID(ResT, call) (call)
#  define TRY_CALL_EXIT(CallT, message, call) ({ CallT x_tmp___ = CallT(); try { x_tmp___ = (call); } catch (Exception &e) { php_critical_error (message); } x_tmp___;})
#  define TRY_CALL_VOID_EXIT(message, call) ({try { (call); } catch (Exception &e_tmp___) { php_critical_error (message); } void();})
#endif


Exception f$new_Exception (const string &file, int line, const string &message = string(), int code = 0);

Exception f$err (const string &file, int line, const string &code, const string &desc = string());


string f$exception_getMessage (const Exception &e);

int f$exception_getCode (const Exception &e);

string f$exception_getFile (const Exception &e);

int f$exception_getLine (const Exception &e);

array <array <string> > f$exception_getTrace (const Exception &e);

string f$exception_getTraceAsString (const Exception &e);


bool f$boolval (const Exception &my_exception);

bool eq2 (const Exception &my_exception, bool value);

bool eq2 (bool value, const Exception &my_exception);

bool equals (bool value, const Exception &my_exception);

bool equals (const Exception &my_exception, bool value);

bool not_equals (bool value, const Exception &my_exception);

bool not_equals (const Exception &my_exception, bool value);


void exception_init_static (void);