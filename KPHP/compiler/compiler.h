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
#include "../common.h"
class Compiler;

class CompilerArgs {
public:
  vector <string> include_dirs;
  vector <string> main_file_names;
  string functions_txt;
  string dest_dir;
  string base_dir;
  string index_path;
  int verbosity;
  bool use_subdirs;
  bool use_safe_integer_arithmetic;
  int threads_number;
  CompilerArgs();
  void set_use_safe_integer_arithmetic (bool new_use_safe_integer_arithmetic);
  void add_include (const string &s);
  void add_main_file (const string &s);
  void set_functions_txt (const string &s);
  void set_dest_dir (const string &s);
  void set_base_dir (const string &s);
  void set_index_path (const string &index_path);
  void set_use_subdirs (bool new_use_subdirs);
  void set_threads_number (int new_threads_number);
  void set_verbosity (int new_verbosity);
};


void compiler_execute (CompilerArgs *args);
