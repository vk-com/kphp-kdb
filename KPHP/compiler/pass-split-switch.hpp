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
/*** Replace cases in big global functions with functions call ***/
class SplitSwitchPass : public FunctionPassBase {
  private:
    AUTO_PROF (split_switch);
    int depth;
    vector <VertexPtr> new_functions;

    static VertexPtr fix_break_continue (VertexAdaptor <meta_op_goto> goto_op,
        const string &state_name, int cycle_depth) {
      int depth = -1;
      if (goto_op->empty()) {
        depth = 1;
      } else {
        VertexPtr label = goto_op->expr();
        if (label->type() == op_int_const) {
          depth = atoi (label->get_string().c_str());
        }
      }
      if (depth != cycle_depth) {
        return goto_op;
      }

      CREATE_VERTEX (minus_one, op_int_const);
      minus_one->str_val = "-1";
      CREATE_VERTEX (state, op_var);
      state->str_val = state_name;
      CREATE_VERTEX (expr, op_set, state, minus_one);

      //TODO: auto_return instead of return true!
      CREATE_VERTEX (true_val, op_true);
      CREATE_VERTEX (new_return, op_return, true_val);
      CREATE_VERTEX (seq, op_seq, expr, new_return);
      return seq;
    }

    static VertexPtr prepare_switch_func (
        VertexPtr root,
        const string &state_name,
        int cycle_depth) {
      if (root->type() == op_return) {
        CREATE_VERTEX (one, op_int_const);
        one->str_val = "1";
        CREATE_VERTEX (state, op_var);
        state->str_val = state_name;
        CREATE_VERTEX (expr, op_set, state, one);
        CREATE_VERTEX (seq, op_seq, expr, root);
        return seq;
      }
      if ((root->type() == op_continue || root->type() == op_break)) {
        return fix_break_continue (root, state_name, cycle_depth);
      }

      for (VertexRange i = all (*root); !i.empty(); i.next()) {
        //TODO: hack... write proper Range
        bool is_cycle = OpInfo::type ((*i)->type()) == cycle_op;
        i[0] = prepare_switch_func (*i, state_name, cycle_depth + is_cycle);
      }

      return root;
    }

  public:
    SplitSwitchPass() :
      depth (0) {
    }
    string get_description() {
      return "Split switch";
    }
    bool check_function (FunctionPtr function) {
      return default_check_function (function) && function->type() == FunctionData::func_global;
    }
    VertexPtr on_enter_vertex (VertexPtr root, LocalT *local) {
      depth++;
      if (root->type() != op_switch) {
        return root;
      }

      VertexAdaptor <op_switch> switch_v = root;

      for (VertexRange i = switch_v->cases(); !i.empty(); i.next()) {
        VertexPtr cs = *i;
        VertexAdaptor <op_seq> seq;
        if (cs->type() == op_case) {
          seq = cs.as <op_case>()->cmd();
        } else if (cs->type() == op_default) {
          seq = cs.as <op_default>()->cmd();
        } else {
          kphp_fail();
        }

        string func_name_str = gen_unique_name ("switch_func");
        CREATE_VERTEX (func_name, op_func_name);
        func_name->str_val = func_name_str;

        CREATE_VERTEX (case_state, op_var);
        case_state->ref_flag = 1;
        string case_state_name = gen_unique_name ("switch_case_state");
        case_state->str_val = case_state_name;

        CREATE_VERTEX (case_state_3, op_var);
        case_state_3->str_val = case_state_name;
        case_state_3->extra_type = op_ex_var_superlocal;
        CREATE_VERTEX (case_state_0, op_var);
        case_state_0->str_val = case_state_name;
        case_state_0->extra_type = op_ex_var_superlocal;
        CREATE_VERTEX (case_state_1, op_var);
        case_state_1->str_val = case_state_name;
        case_state_1->extra_type = op_ex_var_superlocal;
        CREATE_VERTEX (case_state_2, op_var);
        case_state_2->str_val = case_state_name;
        case_state_2->extra_type = op_ex_var_superlocal;

        CREATE_VERTEX (case_state_param, op_func_param, case_state);
        CREATE_VERTEX (func_params, op_func_param_list, case_state_param);
        CREATE_VERTEX (func, op_function, func_name, func_params, seq);
        func->extra_type = op_ex_func_switch;
        func = prepare_switch_func (func, case_state_name, 1);
        GenTree::func_force_return (func);
        new_functions.push_back (func);

        CREATE_VERTEX (func_call, op_func_call, case_state_0);
        func_call->str_val = func_name_str;

        string case_res_name = gen_unique_name ("switch_case_res");
        CREATE_VERTEX (case_res, op_var);
        case_res->str_val = case_res_name;
        case_res->extra_type = op_ex_var_superlocal;
        CREATE_VERTEX (case_res_copy, op_var);
        case_res_copy->str_val = case_res_name;
        case_res_copy->extra_type = op_ex_var_superlocal;
        CREATE_VERTEX (run_func, op_set, case_res, func_call);


        CREATE_VERTEX (zero, op_int_const);
        zero->str_val = "0";
        CREATE_VERTEX (one, op_int_const);
        one->str_val = "1";
        CREATE_VERTEX (minus_one, op_int_const);
        minus_one->str_val = "-1";

        CREATE_VERTEX (eq_one, op_eq2, case_state_1, one);
        CREATE_VERTEX (eq_minus_one, op_eq2, case_state_2, minus_one);

        CREATE_VERTEX (cmd_one, op_return, case_res_copy);
        CREATE_VERTEX (one_2, op_int_const);
        one_2->str_val = "1";
        CREATE_VERTEX (cmd_minus_one, op_break, one_2);

        CREATE_VERTEX (init, op_set, case_state_3, zero);
        CREATE_VERTEX (if_one, op_if, eq_one, cmd_one);
        CREATE_VERTEX (if_minus_one, op_if, eq_minus_one, cmd_minus_one);

        vector <VertexPtr> new_seq_next;
        new_seq_next.push_back (init);
        new_seq_next.push_back (run_func);
        new_seq_next.push_back (if_one);
        new_seq_next.push_back (if_minus_one);

        CREATE_VERTEX (new_seq, op_seq, new_seq_next);
        cs->back() = new_seq;
      }

      return root;
    }

    bool need_recursion (VertexPtr root, LocalT *local) {
      return depth < 2 || root->type() == op_seq || root->type() == op_try;
    }
    VertexPtr on_exit_vertex (VertexPtr root, LocalT *local) {
      depth--;
      return root;
    }

    const vector <VertexPtr> &get_new_functions() {
      return new_functions;
    }
};

