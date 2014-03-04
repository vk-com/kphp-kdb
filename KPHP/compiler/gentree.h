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

#include "token.h"
#include "operation.h"
#include "data.h"

struct GenTree;
typedef VertexPtr (GenTree::*GetFunc) ();


struct GenTreeCallbackBase {
  virtual void register_function (VertexPtr func) = 0;
  virtual void register_class (const ClassInfo &info) = 0;
  virtual ~GenTreeCallbackBase(){}
};

class GenTree {
public:
  struct AutoLocation {
    int line_num;
    explicit AutoLocation (const GenTree *gen)
      : line_num (gen->line_num) {
    }
  };
  static inline void set_location (VertexPtr v, const AutoLocation &location);

  int line_num;

  GenTree ();

  void init (const vector <Token *> *tokens_new, GenTreeCallbackBase *callback_new);
  void register_function (VertexPtr func);
  bool in_class();
  void enter_class (const string &class_name);
  ClassInfo &cur_class();
  void exit_and_register_class (VertexPtr root);
  void enter_function();
  void exit_function();

  bool expect (TokenType tp, const string &msg);
  bool test_expect (TokenType tp);

  void next_cur();
  int open_parent();

  VertexPtr embrace (VertexPtr v);
  VertexPtr gen_list (VertexPtr node, GetFunc f, TokenType delim, int flags);
  PrimitiveType get_ptype();
  PrimitiveType get_type_help (void);
  VertexPtr get_type_rule_func (void);
  VertexPtr get_type_rule_ (void);
  VertexPtr get_type_rule (void);

  static VertexPtr conv_to (VertexPtr x, PrimitiveType tp, int ref_flag = 0);
  template <PrimitiveType ToT> static VertexPtr conv_to (VertexPtr x);
  static VertexPtr get_actual_value (VertexPtr v);
  static bool has_return (VertexPtr v);
  static void func_force_return (VertexPtr root, VertexPtr val = VertexPtr());
  static void for_each (VertexPtr root, void (*callback) (VertexPtr ));
  static string get_memfunc_prefix (const string &name);

  VertexPtr get_func_param();
  VertexPtr get_foreach_param();
  VertexPtr get_var_name();
  VertexPtr get_var_name_ref();
  VertexPtr get_expr_top();
  VertexPtr get_postfix_expression (VertexPtr res);
  VertexPtr get_unary_op();
  VertexPtr get_binary_op (int bin_op_cur, int bin_op_end, GetFunc next, bool till_ternary);
  VertexPtr get_expression_impl (bool till_ternary);
  VertexPtr get_expression();
  VertexPtr get_statement();
  VertexPtr get_seq();
  VertexPtr post_process (VertexPtr root);
  bool check_seq_end();
  bool check_statement_end();
  VertexPtr run();

  static bool is_superglobal (const string &s);

  template <Operation EmptyOp> bool gen_list (vector <VertexPtr> *res, GetFunc f, TokenType delim);
  template <Operation Op> VertexPtr get_conv();
  template <Operation Op> VertexPtr get_varg_call();
  template <Operation Op> VertexPtr get_require();
  template <Operation Op, Operation EmptyOp> VertexPtr get_func_call();
  VertexPtr get_string();
  VertexPtr get_string_build();
  VertexPtr get_def_value();
  template <PrimitiveType ToT> static VertexPtr conv_to_lval (VertexPtr x);
  template <Operation Op> VertexPtr get_multi_call (GetFunc f);
  VertexPtr get_return();
  VertexPtr get_exit();
  template <Operation Op> VertexPtr get_break_continue();
  VertexPtr get_foreach();
  VertexPtr get_while();
  VertexPtr get_if();
  VertexPtr get_for();
  VertexPtr get_do();
  VertexPtr get_switch();
  VertexPtr get_function (bool anonimous_flag = false);
  VertexPtr get_class();
private:
  const vector <Token *> *tokens;
  GenTreeCallbackBase *callback;
  int in_func_cnt_;
  vector <Token *>::const_iterator cur, end;
  vector <ClassInfo> class_stack;
};
void gen_tree_init();

void php_gen_tree (vector <Token *> *tokens, const string &main_func_name, GenTreeCallbackBase *callback);

template <PrimitiveType ToT>
VertexPtr GenTree::conv_to_lval (VertexPtr x) {
  VertexPtr res;
  switch (ToT) {
    case tp_array: {
      CREATE_VERTEX (v, op_conv_array_l, x);
      res = v;
      break;
    }
    case tp_int: {
      CREATE_VERTEX (v, op_conv_int_l, x);
      res = v;
      break;
    }
  }
  ::set_location (res, x->get_location());
  return res;
}
template <PrimitiveType ToT>
VertexPtr GenTree::conv_to (VertexPtr x) {
  VertexPtr res;
  switch (ToT) {
    case tp_int: {
      CREATE_VERTEX (v, op_conv_int, x);
      res = v;
      break;
    }
    case tp_bool: {
      CREATE_VERTEX (v, op_conv_bool, x);
      res = v;
      break;
    }
    case tp_string: {
      CREATE_VERTEX (v, op_conv_string, x);
      res = v;
      break;
    }
    case tp_float: {
      CREATE_VERTEX (v, op_conv_float, x);
      res = v;
      break;
    }
    case tp_array: {
      CREATE_VERTEX (v, op_conv_array, x);
      res = v;
      break;
    }
    case tp_UInt: {
      CREATE_VERTEX (v, op_conv_uint, x);
      res = v;
      break;
    }
    case tp_Long: {
      CREATE_VERTEX (v, op_conv_long, x);
      res = v;
      break;
    }
    case tp_ULong: {
      CREATE_VERTEX (v, op_conv_ulong, x);
      res = v;
      break;
    }
    case tp_regexp: {
      CREATE_VERTEX (v, op_conv_regexp, x);
      res = v;
      break;
    }
  }
  ::set_location (res, x->get_location());
  return res;
}

inline void GenTree::set_location (VertexPtr v, const GenTree::AutoLocation &location) {
  v->location.line = location.line_num;
}
