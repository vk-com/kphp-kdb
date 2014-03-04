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
inline bool operator < (const FileInfo &a, const FileInfo &b) {
  //TODO: compare by inode?
  return a.path < b.path;
}
inline bool operator == (const FileInfo &a, const FileInfo &b) {
  return a.path == b.path;
}
inline bool operator != (const FileInfo &a, const FileInfo &b) {
  return a.path != b.path;
}

template <class DataStream>
void CompilerCore::require_function_set (FunctionSetPtr function_set, FunctionPtr by_function,
                                         DataStream &os) {
  if (function_set->is_required) {
    return;
  }

  AutoLocker <FunctionSetPtr> locker (function_set);
  if (function_set->is_required) {
    return;
  }
  function_set->is_required = true;
  function_set->req_id = by_function;

  for (int i = 0, ni = function_set->size(); i < ni; i++) {
    FunctionPtr function = function_set[i];
    kphp_assert (function.not_null());
    function->is_required = true;
    function->req_id = function_set->req_id;
    os << function;
  }
}

template <class DataStream>
void CompilerCore::require_function_set (function_set_t type, const string &name,
                                         FunctionPtr by_function, DataStream &os) {
  FunctionSetPtr function_set = get_function_set (type, name, true);
  kphp_assert (function_set.not_null());
  require_function_set (function_set, by_function, os);
}

template <class DataStream>
void CompilerCore::register_function_header (VertexAdaptor <meta_op_function> function_header,
                                             DataStream &os) {
  const string &function_name = function_header->name().as <op_func_name>()->str_val;
  FunctionSetPtr function_set = get_function_set (fs_function, function_name, true);
  kphp_assert (function_set.not_null());

  {
    AutoLocker <FunctionSetPtr> locker (function_set);
    kphp_error_return (
      function_set->header.is_null(),
      dl_pstr ("Several headers for one function [%s] are found", function_name.c_str())
    );
    function_set->header = function_header;
  }

  require_function_set (function_set, FunctionPtr(), os);
}

template <class DataStream>
void CompilerCore::register_function (VertexPtr root, DataStream &os) {
  if (root.is_null()) {
    return;
  }
  FunctionPtr function;
  if (root->type() == op_function || root->type() == op_func_decl) {
    function = create_function (root);
  } else if (root->type() == op_extern_func) {
    register_function_header (root, os);
    return;
  } else {
    kphp_fail();
  }
  FunctionSetPtr function_set = get_function_set (fs_function, function->name, true);

  bool force_flag = function->type() == FunctionData::func_global;
  if (add_to_function_set (
        function_set,
        function,
        force_flag)) {
    function->is_required = true;
    if (force_flag) {
      function->req_id = stage::get_file()->req_id;
    } else {
      function->req_id = function_set->req_id;
    }
    os << function;
  }
};

template <class DataStream>
void CompilerCore::register_main_file (const string &file_name, DataStream &os) {
  SrcFilePtr res = register_file (file_name);
  if (res.not_null() && try_require_file (res)) {
    if (!args().functions_txt.empty()) {
      string prefix = "<?php require_once (\"" + args().functions_txt + "\");?>";
      res->add_prefix (prefix);
    }
    main_files.push_back (res);
    os << res;
  }
}
template <class DataStream>
pair <SrcFilePtr, bool> CompilerCore::require_file (const string &file_name, DataStream &os) {
  SrcFilePtr file = register_file (file_name);
  bool required = false;
  if (file.not_null() && try_require_file (file)) {
    required = true;
    os << file;
  }
  return make_pair (file, required);
}

