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
#include "bicycle.h"

#include "stage.h"

/*** Function Pass ***/
class FunctionPassBase {
  protected:
    FunctionPtr current_function;
  public:
    virtual ~FunctionPassBase() {
    }
    struct LocalT {
    };
    virtual string get_description() {
      return "unknown function pass";
    }
    bool default_check_function (FunctionPtr function) {
      return function.not_null() && function->root.not_null();
    }
    virtual bool check_function (FunctionPtr function) {
      return default_check_function (function);
    }
    void init() {
    }
    bool on_start (FunctionPtr function) {
      if (!check_function (function)) {
        return false;
      }
      stage::set_name (get_description());
      stage::set_function (function);
      current_function = function;
      return true;
    }
    void on_finish() {
    }
    VertexPtr on_enter_vertex (VertexPtr vertex, LocalT *local) {
      return vertex;
    }
    void on_enter_edge (VertexPtr vertex, LocalT *local, VertexPtr dest_vertex, LocalT *dest_local) {
    }
    template <class VisitT>
    bool user_recursion (VertexPtr vertex, LocalT *local, VisitT &visit) {
      return false;
    }
    bool need_recursion (VertexPtr vertex, LocalT *local) {
      return true;
    }
    void on_exit_edge (VertexPtr vertex, LocalT *local, VertexPtr from_vertex, LocalT *from_local) {
    }
    VertexPtr on_exit_vertex (VertexPtr vertex, LocalT *local) {
      return vertex;
    }
};

template <class FunctionPassT>
VertexPtr run_function_pass (VertexPtr vertex, FunctionPassT *pass, typename FunctionPassT::LocalT *local);

template <class FunctionPassT>
struct VisitVertex {
  VertexPtr vertex;
  FunctionPassT *pass;
  typename FunctionPassT::LocalT *local;

  VisitVertex (VertexPtr vertex, FunctionPassT *pass, typename FunctionPassT::LocalT *local) :
    vertex (vertex),
    pass (pass),
    local (local){
    }
  inline void operator () (VertexPtr &child) {
    typename FunctionPassT::LocalT child_local;
    pass->on_enter_edge (vertex, local, child, &child_local);
    child = run_function_pass (child, pass, &child_local);
    pass->on_exit_edge (vertex, local, child, &child_local);
  }
};

template <class FunctionPassT>
VertexPtr run_function_pass (VertexPtr vertex, FunctionPassT *pass, typename FunctionPassT::LocalT *local) {
  stage::set_location (vertex->get_location());

  vertex = pass->on_enter_vertex (vertex, local);

  VisitVertex <FunctionPassT> visit (vertex, pass, local);
  if (!pass->user_recursion(vertex, local, visit) && pass->need_recursion (vertex, local)) {
    FOREACH_VERTEX (vertex, i) {
      visit (*i);
    }
  }

  vertex = pass->on_exit_vertex (vertex, local);

  return vertex;
}

template <class FunctionPassT>
bool run_function_pass (FunctionPtr function, FunctionPassT *pass) {
  pass->init();
  if (!pass->on_start (function)) {
    return false;
  }
  typename FunctionPassT::LocalT local;
  function->root = run_function_pass (function->root, pass, &local);
  pass->on_finish();
  return true;
}

template <class FunctionPassT>
class FunctionPassF {
  public:
    DUMMY_ON_FINISH
    template <class OutputStream> void execute (FunctionPtr function, OutputStream &os) {
      FunctionPassT pass;
      run_function_pass (function, &pass);
      if (stage::has_error()) {
        return;
      }
      os << function;
    }
};

template <class Pass>
class FunctionPassPipe {
  public:
    typedef Pipe <FunctionPassF <Pass>, DataStream <FunctionPtr>, DataStream <FunctionPtr> > Self;
};
