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
#define compiler_assert(x, y, level)  ({\
  int kphp_error_res__ = 0;\
  if (!(x)) {\
    kphp_error_res__ = 1;\
    on_compilation_error (#x, __FILE__, __LINE__, y, level);\
  }\
  kphp_error_res__;\
})

#define kphp_warning(y)  compiler_assert (0, y, WRN_ASSERT_LEVEL)
#define kphp_error(x, y) compiler_assert (x, y, CE_ASSERT_LEVEL)
#define kphp_error_act(x, y, act) if (kphp_error (x, y)) act;
#define kphp_error_return(x, y) kphp_error_act (x, y, return)
#define kphp_assert(x) compiler_assert (x, "", FATAL_ASSERT_LEVEL)
#define kphp_fail() kphp_assert (0); _exit(1);

typedef enum {WRN_ASSERT_LEVEL, CE_ASSERT_LEVEL, FATAL_ASSERT_LEVEL} AssertLevelT;

void on_compilation_error (const char *description, const char *file_name, int line_number,
  const char *full_description, AssertLevelT assert_level);

#include "location.h"
namespace stage {
  struct StageInfo {
    string name;
    Location location;
    bool global_error_flag;
    bool error_flag;
    StageInfo() :
      name(),
      location(),
      global_error_flag (false),
      error_flag (false) {
    }
  };

  StageInfo *get_stage_info_ptr();

  void error();
  bool has_error();
  bool has_global_error();
  void die_if_global_errors();

  Location *get_location_ptr();
  const Location &get_location();
  void set_location (const Location &new_location);

  void print (FILE *f);
  void print_file (FILE *f);
  void print_function (FILE *f);
  void print_line (FILE *f);
  void print_comment (FILE *f);
  void get_function_history (stringstream &ss, FunctionPtr function);
  string get_function_history();

  void set_name (const string &name);
  const string &get_name();

  void set_file (SrcFilePtr file);
  void set_function (FunctionPtr function);
  void set_line (int line);
  SrcFilePtr get_file (void);
  FunctionPtr get_function (void);
  int get_line (void);

  const string &get_file_name();
  const string &get_function_name();
  string to_str (const Location &new_location);
}


