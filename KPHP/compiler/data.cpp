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

#include "data.h"
#include "bicycle.h"

//IdGen <VertexPtr> tree_id_gen;
//BikeIdGen <VertexPtr> bike_tree_id_gen;

VertexPtr reg_vertex_impl (VertexPtr v) {
  //Suddenly I don't need it anymore;
  return v;
}


/*** VarData ***/
VarData::VarData (VarData::Type type_) :
  type_ (type_),
  id (-1),
  tinf (NULL),
  tinf_node (VarPtr (this)),
  init_val (NULL),
  static_id(),
  bad_vars (NULL),
  is_constant (false),
  is_reference (false),
  uninited_flag (false),
  optimize_flag (false),
  tinf_flag (false),
  global_init_flag (false) {
}

void VarData::set_uninited_flag (bool f) {
  uninited_flag = f;
}
bool VarData::get_uninited_flag() {
  return uninited_flag;
}

/*** ClassData ***/
ClassData::ClassData() {
  is_inited = false;
  is_required = false;
}

/*** FunctionSet ***/
int FunctionSet::size() {
  return (int)functions.size();
}

FunctionPtr FunctionSet::operator[] (int i) {
  kphp_assert (0 <= i && i < size());
  return functions[i];
}

FunctionSet::FunctionSet()
  : is_required (false) {
}

bool FunctionSet::add_function (FunctionPtr new_function) {
  std::vector <FunctionPtr>::iterator match = std::find (functions.begin(), functions.end(), new_function);
  if (match != functions.end()) {
    return false;
  }

  functions.push_back (new_function);
  return true;
}



/*** FunctionMatcher ***/
FunctionMatcher::FunctionMatcher() {
}

void FunctionMatcher::set_function_set (FunctionSetPtr new_function_set) {
  match = FunctionPtr();
  function_set = new_function_set;
}

void FunctionMatcher::try_match (VertexPtr call) {
  kphp_assert (function_set.not_null());
  if (function_set->size() != 1) {
    return;
  }
  match = function_set[0];
}

FunctionPtr FunctionMatcher::get_function() {
  return match;
}

bool FunctionMatcher::is_ready() {
  return match.not_null();
}



/*** FunctionData ***/
FunctionData::FunctionData () :
  root (NULL),
  is_required (false),
  type_ (func_local),
  bad_vars (NULL),
  file_id (),
  req_id(),
  class_id(),
  varg_flag (false),
  tinf (0),
  tinf_state (0),
  const_data (NULL),
  min_argn (0),
  is_extern (false),
  used_in_source (false),
  is_callback (false) {
}

FunctionData::FunctionData (VertexPtr root) :
  root (root),
  is_required (false),
  type_ (func_local),
  bad_vars (NULL),
  file_id (),
  req_id(),
  class_id(),
  varg_flag (false),
  tinf (0),
  tinf_state (0),
  const_data (NULL),
  min_argn (0),
  is_extern (false),
  used_in_source (false),
  is_callback (false) {
}



/*** DefineData ***/
DefineData::DefineData ()
  : val (NULL) {
}

DefineData::DefineData (VertexPtr val, DefineType type_)
  : val (val), type_ (type_) {
}
int tree_node_dfs_cnt = 0;
