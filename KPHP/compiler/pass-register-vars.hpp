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

/*** Replace constant expressions with const variables ***/
class CollectConstVarsPass : public FunctionPassBase {
  private:
    AUTO_PROF (collect_const_vars);
    int in_param_list;
  public:
    struct LocalT : public FunctionPassBase::LocalT {
      bool need_recursion_flag;
    };
    CollectConstVarsPass() :
      in_param_list (0) {
    }
    string get_description() {
      return "Collect constants";
    }

    /*** Serialize big consts if possible ***/
    bool check_const (VertexPtr v, int *nodes_cnt) {
      (*nodes_cnt)++;

      if (!(v->type() == op_string || v->type() == op_int_const || v->type() == op_array ||
            v->type() == op_float_const || v->type() == op_true || v->type() == op_false)) {
        return false;
      }
      if (v->type() == op_array) {
        int has_key = false, no_key = false;

        for (VertexRange i = all (*v); !i.empty(); i.next()) {
          VertexPtr cur = *i;
          if (cur->type() == op_double_arrow) {
            VertexAdaptor <op_double_arrow> arrow = cur;
            VertexPtr key = arrow->key();
            VertexPtr value = arrow->value();

            has_key = true;

            if (key->type() != op_int_const && key->type() != op_string) {
              return false;
            }

            if (!check_const (key, nodes_cnt) || !check_const (value, nodes_cnt)) {
              return false;
            }
          } else {
            no_key = true;
            if (!check_const (cur, nodes_cnt)) {
              return false;
            }
          }
        }
        if (has_key && no_key) {
          return false;
        }
      }
      return true;
    }


    void serialize_int (string int_val, string *s) {
      int val;
      sscanf (int_val.c_str(), "%i", &val);
      int_val = int_to_str (val);
      (*s) += "i:";
      (*s) += int_val;
      (*s) += ";";
    }

    void serialize_float (string float_val, string *s) {
      (*s) += "d:";
      (*s) += float_val;
      (*s) += ";";
    }

    void serialize_bool (bool f, string *s) {
      (*s) += "b:";
      (*s) += f ? "1" : "0";
      (*s) += ";";
    }

    void serialize_string (string str_val, string *s) {
      (*s) += "s:";
      (*s) += int_to_str ((int)str_val.size());
      (*s) += ":";
      (*s) += "\"";
      (*s) += str_val;
      (*s) += "\"";
      (*s) += ";";
    }

    void serialize_array (VertexAdaptor <op_array> arr, string *s) {
      VertexRange args = arr->args();
      int ni = (int)args.size();
      (*s) += "a:";
      (*s) += int_to_str (ni);
      (*s) += ":{";

      for (int i = 0; i < ni; i++) {
        VertexPtr cur = args[i];
        if (cur->type() == op_double_arrow) {
          VertexAdaptor <op_double_arrow> arrow = cur;
          serialize_const (arrow->key(), s);
          serialize_const (arrow->value(), s);
        } else {
          serialize_int (int_to_str (i), s);
          serialize_const (cur, s);
        }
      }

      (*s) += "}";
    }

    void serialize_const (VertexPtr v, string *s) {
      switch (v->type()) {
        case op_int_const:
          serialize_int (v.as <op_int_const>()->str_val, s);
          break;
        case op_true:
          serialize_bool (true, s);
          break;
        case op_false:
          serialize_bool (false, s);
          break;
        case op_float_const:
          serialize_float (v.as <op_float_const>()->str_val, s);
          break;
        case op_string:
          serialize_string (v.as <op_string>()->str_val, s);
          break;
        case op_array:
          serialize_array (v, s);
          break;
        default:
          assert (0 && "unexpected type");
          break;
      }
    }

    VertexPtr optimize_const (VertexPtr v) {
      // 1. only strings, ints and arrays
      // 2. more than 100 nodes
      // 3. in arrays every element has a key or no element has a key
      if (v->type() != op_array) {
        return v;
      }

      FunctionSetPtr unserialize_func_set = G->get_function_set (fs_function, "unserialize", true);
      kphp_assert (unserialize_func_set->size() == 1);
      FunctionPtr unserialize_func_id = unserialize_func_set[0];
      kphp_assert (unserialize_func_id.not_null());


      int nodes_cnt = 0;
      bool valid;
      valid = check_const (v, &nodes_cnt);

      if (nodes_cnt < 100 || !valid) {
        return v;
      }

      string serialized_str;
      serialize_const (v, &serialized_str);

      CREATE_VERTEX (serialized, op_string);
      serialized->str_val = serialized_str;
      CREATE_VERTEX (unserialize, op_func_call, serialized);
      unserialize->str_val = "unserialize";
      unserialize = set_func_id (unserialize, unserialize_func_id);

      CREATE_VERTEX (arrayval, op_conv_array, unserialize);
      return arrayval;
    }

    VertexPtr create_const_variable (VertexPtr root) {
      string name;
      bool global_init_flag = false;
      if (root->type() == op_string) {
        name = gen_const_string_name (root.as <op_string>()->str_val);
        global_init_flag = true;
      } else if (root->type() == op_conv_regexp &&
                 root.as <op_conv_regexp>()->expr()->type() == op_string) {
        name = gen_const_regexp_name (
            root.as <op_conv_regexp>()->expr().
                 as <op_string>()->str_val);
        global_init_flag = true;
      } else {
        name = gen_unique_name ("const_var");
      }

      CREATE_VERTEX (var, op_var);
      var->str_val = name;
      var->extra_type = op_ex_var_const;

      VarPtr var_id = G->get_global_var (name, VarData::var_const_t, optimize_const (root));
      var_id->global_init_flag = global_init_flag;

      if (in_param_list > 0) {
        current_function->header_const_var_ids.insert (var_id);
      } else {
        current_function->const_var_ids.insert (var_id);
      }

      var->set_var_id (var_id);
      return var;
    }

    bool check_function (FunctionPtr function) {
      return default_check_function (function) && function->type() != FunctionData::func_extern;
    }

    VertexPtr on_enter_vertex (VertexPtr root, LocalT *local) {
      in_param_list += root->type() == op_func_param_list;

      local->need_recursion_flag = false;
      if (root->type() == op_define_val || root->type() == op_defined ||
          root->type() == op_require || root->type() == op_require_once) {
        return root;
      }

      if (root->const_type == cnst_const_val) {
        bool conv_to_const = false;
        conv_to_const |= root->type() == op_string || root->type() == op_array ||
          root->type() == op_concat || root->type() == op_string_build ||
          root->type() == op_func_call;

        if (root->type() == op_conv_regexp) {
          VertexPtr expr = root.as <op_conv_regexp>()->expr();
          conv_to_const |= expr->type() == op_string || expr->type() == op_concat ||
            expr->type() == op_string_build;
        }

        if (conv_to_const) {
          return create_const_variable (root);
        }
      }

      local->need_recursion_flag = true;
      return root;
    }
    bool need_recursion (VertexPtr root, LocalT *local) {
      return local->need_recursion_flag;
    }
    VertexPtr on_exit_vertex (VertexPtr root, LocalT *local) {
      in_param_list -= root->type() == op_func_param_list;
      return root;
    }
};

/*** Register variables ***/
// 1. Function parametres (with default values)
// 2. Global variables
// 3. Static local variables (with default values)
// 4. Local variables
class RegisterVariables : public FunctionPassBase {
  private:
    AUTO_PROF (register_variables);
    map <string, VarPtr> registred_vars;
    bool global_function_flag;
    int in_param_list;
  public:

    struct LocalT : public FunctionPassBase::LocalT {
      bool need_recursion_flag;
      LocalT() : need_recursion_flag (true) {}
    };

    RegisterVariables() :
      global_function_flag (false),
      in_param_list (0) {
    }

    string get_description() {
      return "Register variables";
    }

    VarPtr create_global_var (const string &name) {
      VarPtr var = G->get_global_var (name, VarData::var_global_t, VertexPtr());
      pair <map <string, VarPtr>::iterator, bool> it = registred_vars.insert (make_pair (name, var));
      if (it.second == false) {
        VarPtr old_var = it.first->second;
        kphp_error (
          old_var.ptr == var.ptr,
          dl_pstr ("conflict in variable names [%s]", old_var->name.c_str())
        );
      } else {
        if (in_param_list > 0) {
          current_function->header_global_var_ids.push_back (var);
        } else {
          current_function->global_var_ids.push_back (var);
        }
      }
      return var;
    }

    VarPtr create_local_var (const string &name, VarData::Type type, bool create_flag) {
      map <string, VarPtr>::iterator it = registred_vars.find (name);
      if (it != registred_vars.end()) {
        kphp_error (!create_flag, "Redeclaration of local variable");
        return it->second;
      }
      VarPtr var = G->create_local_var (current_function, name, type);
      kphp_error (registred_vars.insert (make_pair (name, var)).second == true, "Redeclaration of local variable");

      return var;
    }

    VarPtr get_global_var (const string &name) {
      map <string, VarPtr>::iterator it = registred_vars.find (name);
      if (it != registred_vars.end()) {
        return it->second;
      }
      return create_global_var (name);
    }
    VarPtr get_var (const string &name) {
      return create_local_var (name, VarData::var_local_t, false);
    }

    void register_global_var (VertexAdaptor <op_var> var_vertex) {
      string name = var_vertex->str_val;
      var_vertex->set_var_id (create_global_var (name));
    }

    bool is_const (VertexPtr v) {
      return v->const_type == cnst_const_val ||
        (v->type() == op_var && v->get_var_id()->type() == VarData::var_const_t) ||
        v->type() == op_define_val;
    }
    bool is_global_var (VertexPtr v) {
      return v->type() == op_var && v->get_var_id()->type() == VarData::var_global_t;
    }

    void register_static_var (VertexAdaptor <op_var> var_vertex, VertexPtr default_value) {
      kphp_error_return (!global_function_flag, "Keyword 'static' used in global function");

      string name = var_vertex->str_val;
      VarPtr var = create_local_var (name, VarData::var_static_t, true);
      var->static_id = current_function;
      if (default_value.not_null()) {
        if (!kphp_error (is_const (default_value), dl_pstr ("Default value of [%s] is not constant", name.c_str()))) {
          var->init_val = default_value;
        }
      }
      var_vertex->set_var_id (var);
    }

    void register_param_var (VertexAdaptor <op_var> var_vertex, VertexPtr default_value) {
      string name = var_vertex->str_val;
      VarPtr var = create_local_var (name, VarData::var_param_t, true);
      var->is_reference = var_vertex->ref_flag;
      kphp_assert (var.not_null());
      if (default_value.not_null()) {
        if (!kphp_error (
              is_const (default_value) || is_global_var (default_value),
              dl_pstr ("Default value of [%s] is not constant", name.c_str()))) {
          var->init_val = default_value;
        }
      }
      var_vertex->set_var_id (var);
    }

    void register_var (VertexAdaptor <op_var> var_vertex) {
      VarPtr var;
      string name = var_vertex->str_val;
      if ((var_vertex->extra_type != op_ex_var_superlocal && global_function_flag) || 
          var_vertex->extra_type == op_ex_var_superglobal) {
        var = get_global_var (name);
      } else {
        var = get_var (name);
      }
      var_vertex->set_var_id (var);
    }

    void visit_global_vertex (VertexAdaptor <op_global> global) {
      for (VertexRange i = global->args(); !i.empty(); i.next()) {
        VertexPtr var = *i;
        kphp_error_act (
          var->type() == op_var,
          "unexpected expression in 'global'",
          continue
        );
        register_global_var (var);
      }
    }

    void visit_static_vertex (VertexAdaptor <op_static> stat) {
      for (VertexRange i = stat->args(); !i.empty(); i.next()) {
        VertexAdaptor <op_var> var;
        VertexPtr default_value;

        VertexPtr node = *i;
        if (node->type() == op_var) {
          var = node;
        } else if (node->type() == op_set) {
          VertexAdaptor <op_set> set_expr = node;
          var = set_expr->lhs();
          kphp_error_act (
            var->type() == op_var,
            "unexpected expression in 'static'",
            continue
          );
          default_value = set_expr->rhs();
        } else {
          kphp_error_act (0, "unexpected expression in 'static'", continue);
        }

        register_static_var (var, default_value);
      }
    }
    template <class VisitT>
    void visit_func_param_list (VertexAdaptor <op_func_param_list> list, VisitT &visit) {
      for (VertexRange i = list->params(); !i.empty(); i.next()) {
        kphp_assert ((*i).not_null());
        kphp_assert ((*i)->type() == op_func_param);
        VertexAdaptor <op_func_param> param = *i;
        VertexAdaptor <op_var> var = param->var();
        VertexPtr default_value;
        if (param->has_default()) {
          default_value = param->default_value();
          visit (param->default_value());
        }
        register_param_var (var, default_value);
      }
    }
    void visit_var (VertexAdaptor <op_var> var) {
      if (var->get_var_id().not_null()) {
        kphp_assert (var->get_var_id()->type() == VarData::var_const_t);
        return;
      }
      register_var (var);
    }

    bool check_function (FunctionPtr function) {
      return default_check_function (function) && function->type() != FunctionData::func_extern;
    }

    bool on_start (FunctionPtr function) {
      if (!FunctionPassBase::on_start (function)) {
        return false;
      }
      global_function_flag = function->type() == FunctionData::func_global ||
        function->type() == FunctionData::func_switch;
      return true;
    }

    VertexPtr on_enter_vertex (VertexPtr root, LocalT *local) {
      kphp_assert (root.not_null());
      if (root->type() == op_global) {
        visit_global_vertex (root);
        local->need_recursion_flag = false;
        CREATE_VERTEX (empty, op_empty);
        return empty;
      } else if (root->type() == op_static) {
        visit_static_vertex (root);
        local->need_recursion_flag = false;
        CREATE_VERTEX (empty, op_empty);
        return empty;
      } else if (root->type() == op_var) {
        visit_var (root);
        local->need_recursion_flag = false;
      }
      return root;
    }

    template <class VisitT>
    bool user_recursion (VertexPtr v, LocalT *local, VisitT &visit) {
      if (v->type() == op_func_param_list) {
        in_param_list++;
        visit_func_param_list (v, visit);
        in_param_list--;
        return true;
      }
      return false;
    }


    bool need_recursion (VertexPtr root, LocalT *local) {
      return local->need_recursion_flag;
    }
};
