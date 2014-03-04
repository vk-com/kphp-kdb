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

#include "stage.h"
#include "data.h"
#include "io.h"
#include "../common.h"

#include "bicycle.h"

const char *get_assert_level_desc (AssertLevelT assert_level) {
  switch (assert_level) {
    case WRN_ASSERT_LEVEL:
      return "Warning";
    case CE_ASSERT_LEVEL:
      return "Compilation error";
    case FATAL_ASSERT_LEVEL:
      return "Fatal error";
    default:
      assert (0);
  }
}
volatile int ce_locker;
void on_compilation_error (const char *description, const char *file_name, int line_number,
  const char *full_description, AssertLevelT assert_level) {

  AutoLocker <volatile int *> locker (&ce_locker);
  printf ("%s [gen by %s at %d]\n", get_assert_level_desc (assert_level), file_name, line_number);
  stage::print (stdout);
  printf (" %s\n\n", full_description);
  if (assert_level == FATAL_ASSERT_LEVEL) {
    printf ("Compilation failed.\n"
            "It is probably happened due to incorrect or unsupported PHP input.\n"
            "But it is still bug in compiler.\n");
    abort();
  }
  if (assert_level == CE_ASSERT_LEVEL) {
    stage::error();
  }
}

Location::Location() :
  line (-1) {
}
void Location::set_file (SrcFilePtr new_file) {
  file = new_file;
  function = FunctionPtr();
  line = 0;
}
void Location::set_function (FunctionPtr new_function) {
  if (new_function.not_null()) {
    file = new_function->file_id;
  }
  function = new_function;
  line = 0;
}
void Location::set_line (int new_line) {
  line = new_line;
}
SrcFilePtr Location::get_file() const {
  return file;
}
FunctionPtr Location::get_function() const {
  return function;
}
int Location::get_line() const {
  return line;
}

namespace stage {
  static TLS <StageInfo> stage_info;
}

void stage::print (FILE *f) {
  fprintf (f, "In stage = [%s]:\n", get_name().c_str());
  fprintf (f, "  ");
  print_file (f);
  fprintf (f, "  ");
  print_function (f);
  fprintf (f, "  ");
  print_line (f);
  print_comment (f);
}
void stage::print_file (FILE *f) {
  fprintf (f, "[file = %s]\n", get_file_name().c_str());
}
void stage::print_function (FILE *f) {
  fprintf (f, "[function = %s]\n", get_function_name().c_str());
}
void stage::print_line (FILE *f) {
  if (get_line() > 0) {
    fprintf (f, "[line = %d]\n", get_line());
  }
}
void stage::print_comment (FILE *f) {
  if (get_line() > 0) {
    string_ref comment = get_file()->get_line (get_line());
    fprintf (f, "//%4d:", get_line());
    for (int j = 0, nj = comment.length(); j < nj; j++) {
      int c = comment.begin()[j];
      if (c == '\n') {
        putc ('\\', f);
        putc ('n', f);
      } else if (c > 13) {
        putc (c, f);
      }
    }
    if (comment.length() > 0 && comment.begin()[comment.length() - 1] == '\\') {
      putc (';', f);
    }
    putc ('\n', f);
  }
}

void stage::get_function_history (stringstream &ss, FunctionPtr function) {
  if (function.is_null()) {
    return;
  }
  get_function_history (ss, function->req_id);
  ss << "->[" << function->file_id->short_file_name << ":" << function->name << "]";
}

string stage::get_function_history() {
  stringstream ss;
  get_function_history (ss, get_function());
  return ss.str();
}

stage::StageInfo *stage::get_stage_info_ptr() {
  return &*stage_info;
}

void stage::set_name (const string &name) {
  get_stage_info_ptr()->name = name;
  get_stage_info_ptr()->error_flag = false;
}
void stage::error() {
  get_stage_info_ptr()->global_error_flag = true;
  get_stage_info_ptr()->error_flag = true;
}
bool stage::has_error() {
  return get_stage_info_ptr()->error_flag;
}
bool stage::has_global_error() {
  for (int i = 0; i < stage_info.size(); i++) {
    if (stage_info.get (i)->global_error_flag) {
      return true;
    }
  }
  return false;
}
void stage::die_if_global_errors() {
  if (stage::has_global_error()) {
    printf ("Compilation terminated due to errors\n");
    exit (1);
  }
}

const string &stage::get_name() {
  return get_stage_info_ptr()->name;
}

Location *stage::get_location_ptr() {
  return &get_stage_info_ptr()->location;
}
const Location &stage::get_location() {
  return *get_location_ptr();
}
void stage::set_location (const Location &new_location) {
  if (new_location.get_file().is_null()) {
    return;
  }
  *get_location_ptr() = new_location;
}

void stage::set_file (SrcFilePtr file) {
  get_location_ptr()->set_file (file);
}
void stage::set_function (FunctionPtr file) {
  get_location_ptr()->set_function (file);
}
void stage::set_line (int line) {
  get_location_ptr()->set_line (line);
}

SrcFilePtr stage::get_file (void) {
  return get_location().get_file();
}
FunctionPtr stage::get_function (void) {
  return get_location().get_function();
}
int stage::get_line (void) {
  return get_location().get_line();
}

const string &stage::get_file_name (void) {
  static string no_file = "unknown";
  SrcFilePtr file = get_file();
  if (file.is_null()) {
    return no_file;
  }
  return file->file_name;
}

const string &stage::get_function_name (void) {
  static string no_function = "unknown";
  FunctionPtr function = get_function();
  if (function.is_null()) {
    return no_function;
  }
  return function->name;
}

string stage::to_str (const Location &new_location) {
  set_location (new_location);

  stringstream ss;
  ss << get_file_name() << ":" << get_function_name() << ":" << get_line();
  return ss.str();
}
