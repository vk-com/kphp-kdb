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
#include "compiler-core.h"

//NB: rl_val, tinf should make sence after this function
class OptimizationPass : public FunctionPassBase {
  private:
    AUTO_PROF (optimization);
  public:
    string get_description() {
      return "Optimization";
    }

    bool check_function (FunctionPtr function) {
      return default_check_function (function) && function->type() != FunctionData::func_extern;
    }

    VertexPtr optimize_set_push_back (VertexAdaptor <op_set> set_op) {
      if (set_op->lhs()->type() != op_index) {
        return set_op;
      }
      VertexAdaptor <op_index> index = set_op->lhs();
      if (index->has_key() && set_op->rl_type != val_none) {
        return set_op;
      }

      VertexPtr a, b, c;
      a = index->array();
      if (index->has_key()) {
        b = index->key();
      }
      c = set_op->rhs();

      VertexPtr save_root = set_op;
      if (b.is_null()) {
        if (set_op->rl_type == val_none) {
          CREATE_VERTEX (push_back, op_push_back, a, c);
          set_op = push_back;
        } else {
          CREATE_VERTEX (push_back_return, op_push_back_return, a, c);
          set_op = push_back_return;
        }
      } else {
        CREATE_VERTEX (set_value, op_set_value, a, b, c);
        set_op = set_value;
      }
      set_op->location = save_root->get_location();
      set_op->extra_type = op_ex_internal_func;
      set_op->rl_type = save_root->rl_type;
      return set_op;
    }

    void collect_concat (VertexPtr root, vector <VertexPtr> *collected) {
      if (root->type() == op_string_build || root->type() == op_concat) {
        FOREACH_VERTEX (root, i) {
          collect_concat (*i, collected);
        }
      } else {
        collected->push_back (root);
      }
    }
    VertexPtr optimize_string_building (VertexPtr root) {
      vector <VertexPtr> collected;
      collect_concat (root, &collected);
      CREATE_VERTEX (new_root, op_string_build, collected);
      new_root->rl_type = root->rl_type;

      return new_root;
    }

    VertexPtr optimize_postfix_inc (VertexPtr root) {
      if (root->rl_type == val_none) {
        CREATE_VERTEX (new_root, op_prefix_inc, root.as <op_postfix_inc>()->expr());
        new_root->rl_type = root->rl_type;
        root = new_root;
      }
      return root;
    }

    VertexPtr optimize_postfix_dec (VertexPtr root) {
      if (root->rl_type == val_none) {
        CREATE_VERTEX (new_root, op_prefix_dec, root.as <op_postfix_dec>()->expr());
        new_root->rl_type = root->rl_type;
        root = new_root;
      }
      return root;
    }

    VertexPtr optimize_index (VertexAdaptor <op_index> index) {
      bool has_key = index->has_key();
      if (!has_key) {
        if (index->rl_type == val_l) {
          kphp_error (0, "Unsupported []");
        } else {
          kphp_error (0, "Cannot use [] for reading");
        }
        return index;
      }
      if (index->rl_type != val_l) {
        index->extra_type = op_ex_index_rval;
      }
      return index;
    }

    template <Operation FromOp, Operation ToOp>
      VertexPtr fix_int_const (VertexPtr from, const string &from_func) {
        VertexPtr *tmp;
        if (from->type() == FromOp) {
          tmp = &from.as <FromOp>()->expr();
        } else if (from->type() == op_func_call &&
                   from.as <op_func_call>()->str_val == from_func) {
          tmp = &from.as <op_func_call>()->args()[0];
        } else {
          return from;
        }
        if ((*tmp)->type() == op_minus) {
          tmp = &(*tmp).as <op_minus>()->expr();
        }
        if ((*tmp)->type() != op_int_const) {
          return from;
        }

        CREATE_VERTEX (res, ToOp);
        res->str_val = (*tmp)->get_string();
        //FIXME: it should be a copy
        res->rl_type = from->rl_type;
        *tmp = res;
        return from;
      }

    VertexPtr fix_int_const (VertexPtr root) {
      root = fix_int_const <op_conv_uint, op_uint_const> (root, "uintval");
      root = fix_int_const <op_conv_long, op_long_const> (root, "longval");
      root = fix_int_const <op_conv_ulong, op_ulong_const> (root, "ulongval");
      return root;
    }

    VertexPtr remove_extra_conversions (VertexPtr root) {
      VertexPtr expr = root.as <meta_op_unary_op> ()->expr();
      const TypeData *tp = tinf::get_type (expr);
      if (tp->use_or_false() == false) {
        VertexPtr res;
        if ((root->type() == op_conv_int || root->type() == op_conv_int_l) && tp->ptype() == tp_int) {
          res = expr;
        } else if (root->type() == op_conv_bool && tp->ptype() == tp_bool) {
          res = expr;
        } else if (root->type() == op_conv_float && tp->ptype() == tp_float) {
          res = expr;
        } else if (root->type() == op_conv_string && tp->ptype() == tp_string) {
          res = expr;
        } else if ((root->type() == op_conv_array || root->type() == op_conv_array_l) && tp->ptype() == tp_array) {
          res = expr;
        } else if (root->type() == op_conv_uint && tp->ptype() == tp_UInt) {
          res = expr;
        } else if (root->type() == op_conv_long && tp->ptype() == tp_Long) {
          res = expr;
        } else if (root->type() == op_conv_ulong && tp->ptype() == tp_ULong) {
          res = expr;
        //} else if (root->type() == op_conv_regexp && tp->ptype() != tp_string) {
          //res = expr;
        }
        if (res.not_null()) {
          res->rl_type = root->rl_type;
          root = res;
        }
      }
      return root;
    }

    VertexPtr on_enter_vertex (VertexPtr root, LocalT *local) {
      if (OpInfo::type (root->type()) == conv_op || root->type() == op_conv_array_l || root->type() == op_conv_int_l) {
        root = remove_extra_conversions (root);
      }

      if (root->type() == op_set) {
        root = optimize_set_push_back (root);
      } else if (root->type() == op_string_build || root->type() == op_concat) {
        root = optimize_string_building (root);
      } else if (root->type() == op_postfix_inc) {
        root = optimize_postfix_inc (root);
      } else if (root->type() == op_postfix_dec) {
        root = optimize_postfix_dec (root);
      } else if (root->type() == op_index) {
        root = optimize_index (root);
      }

      root = fix_int_const (root);

      if (root->rl_type != val_none/* && root->rl_type != val_error*/) {
        tinf::get_type (root);
      }
      return root;
    }

    template <class VisitT>
    bool user_recursion (VertexPtr root, LocalT *local, VisitT &visit) {
      if (root->type() == op_var) {
        VarPtr var = root->get_var_id();
        kphp_assert (var.not_null());
        if (var->init_val.not_null()) {
          if (try_optimize_var (var)) {
            visit (var->init_val);
          }
        }
      }
      return false;
    }
};
