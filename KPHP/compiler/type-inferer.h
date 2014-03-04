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

#include "type-inferer-core.h"
#include "stage.h"
#include "data.h"
#include "function-pass.h"

tinf::Node *get_tinf_node (VertexPtr vertex);
tinf::Node *get_tinf_node (VarPtr var);
void init_functions_tinf_nodes (FunctionPtr function);
tinf::Node *get_tinf_node (FunctionPtr function, int id);

const TypeData *get_type (VertexPtr vertex, tinf::TypeInferer *inferer);
const TypeData *get_type (VarPtr var, tinf::TypeInferer *inferer);
const TypeData *get_type (FunctionPtr function, int id, tinf::TypeInferer *inferer);
const TypeData *fast_get_type (VertexPtr vertex);
const TypeData *fast_get_type (VarPtr var);
const TypeData *fast_get_type (FunctionPtr function, int id);
//TODO: remove extra CREATE_VERTEX?

class Restriction : public tinf::RestrictionBase {
  public:
    Location location;
    Restriction ()
      : location (stage::get_location()) {
      }
    virtual ~Restriction() {
    }
    void check() {
      if (!check_impl()) {
        stage::set_location (location);
        kphp_error (0, get_description());
      }
    }
};

class RestrictionLess : public Restriction {
  public:
    tinf::Node *a_, *b_;
    string desc;

    RestrictionLess (tinf::Node *a, tinf::Node *b) :
      a_ (a),
      b_ (b) {

      }
    const char *get_description() {
      return dl_pstr ("type inference error [%s] <%s <= %s>", desc.c_str(),
          a_->get_description().c_str(), b_->get_description().c_str());
    }
    bool check_impl() {
      const TypeData *a_type = a_->get_type(), *b_type = b_->get_type();

      TypeData *new_type = b_type->clone();
      string first = type_out (new_type);
      new_type->set_lca (a_type);
      string second = type_out (new_type);

      bool ok = first == second;
      delete new_type;

      if (!ok) {
        desc = type_out (a_type) + " <= " + type_out (b_type);
      }

      return ok;
    }
};

typedef enum {
  ifi_error = -1,
  ifi_unset = 1,
  ifi_isset = 1 << 1,
  ifi_is_bool = 1 << 2,
  ifi_is_numeric = 1 << 3,
  ifi_is_scalar = 1 << 4,
  ifi_is_null = 1 << 5,
  ifi_is_integer = 1 << 6,
  ifi_is_long = 1 << 7,
  ifi_is_float = 1 << 8,
  ifi_is_double = 1 << 9,
  ifi_is_real = 1 << 10,
  ifi_is_string = 1 << 11,
  ifi_is_array = 1 << 12,
  ifi_is_object = 1 << 13
} is_func_id_t;

inline is_func_id_t get_ifi_id_ (VertexPtr v) {
  if (v->type() == op_unset) {
    return ifi_unset;
  }
  if (v->type() == op_isset) {
    return ifi_isset;
  }
  if (v->type() == op_func_call) {
    const string &name = v->get_func_id()->name;
    if (name[0] == 'i' && name[1] == 's') {
      if (name == "is_bool") {
        return ifi_is_bool;
      }
      if (name == "is_numeric") {
        return ifi_is_numeric;
      }
      if (name == "is_scalar") {
        return ifi_is_scalar;
      }
      if (name == "is_null") {
        return ifi_is_null;
      }
      if (name == "is_bool") {
        return ifi_is_bool;
      }
      if (name == "is_integer") {
        return ifi_is_integer;
      }
      if (name == "is_long") {
        return ifi_is_long;
      }
      if (name == "is_float") {
        return ifi_is_float;
      }
      if (name == "is_double") {
        return ifi_is_double;
      }
      if (name == "is_real") {
        return ifi_is_real;
      }
      if (name == "is_string") {
        return ifi_is_string;
      }
      if (name == "is_array") {
        return ifi_is_array;
      }
      if (name == "is_object") {
        return ifi_is_object;
      }
    }
  }
  return ifi_error;
}

struct LValue {
  tinf::Node *value;
  const MultiKey *key;
  inline LValue();
  inline LValue (tinf::Node *value, const MultiKey *key);
  inline LValue (const LValue &from);
  inline LValue &operator = (const LValue &from);
};

struct RValue {
  const TypeData *type;
  tinf::Node *node;
  const MultiKey *key;
  bool drop_or_false;
  inline RValue();
  inline RValue (const TypeData *type, const MultiKey *key = NULL);
  inline RValue (tinf::Node *node, const MultiKey *key = NULL);
  inline RValue (const RValue &from);
  inline RValue &operator = (const RValue &from);
};

LValue::LValue() :
  value (NULL),
  key (NULL) {
  }
LValue::LValue (tinf::Node *value, const MultiKey *key) :
  value (value),
  key (key) {
  }
LValue::LValue (const LValue &from) :
  value (from.value),
  key (from.key) {
  }
LValue &LValue::operator = (const LValue &from) {
  value = from.value;
  key = from.key;
  return *this;
}

RValue::RValue() :
  type (NULL),
  node (NULL),
  key (NULL),
  drop_or_false (false) {
  }
RValue::RValue (const TypeData *type, const MultiKey *key) :
  type (type),
  node (NULL),
  key (key),
  drop_or_false (false) {
  }
RValue::RValue (tinf::Node *node, const MultiKey *key) :
  type (NULL),
  node (node),
  key (key),
  drop_or_false (false) {
  }
RValue::RValue (const RValue &from) :
  type (from.type),
  node (from.node),
  key (from.key),
  drop_or_false (from.drop_or_false) {
  }
RValue &RValue::operator = (const RValue &from) {
  type = from.type;
  node = from.node;
  key = from.key;
  drop_or_false = from.drop_or_false;
  return *this;
}

inline RValue drop_or_false (RValue rvalue) {
  rvalue.drop_or_false = true;
  return rvalue;
}

inline RValue as_rvalue (PrimitiveType primitive_type) {
  return RValue (TypeData::get_type (primitive_type));
}
inline RValue as_rvalue (VertexPtr v, const MultiKey *key = NULL) {
  return RValue (get_tinf_node (v), key);
}
inline RValue as_rvalue (FunctionPtr function, int id) {
  return RValue (get_tinf_node (function, id));
}
inline RValue as_rvalue (VarPtr var) {
  return RValue (get_tinf_node (var));
}
inline RValue as_rvalue (const TypeData *type, const MultiKey *key = NULL) {
  return RValue (type, key);
}
inline const RValue &as_rvalue (const RValue &rvalue) {
  return rvalue;
}

class CollectMainEdgesCallbackBase {
  public:
    virtual ~CollectMainEdgesCallbackBase() {
    }
    virtual void require_node (const RValue &rvalue) = 0;
    virtual void create_set (const LValue &lvalue, const RValue &rvalue) = 0;
    virtual void create_less (const RValue &lhs, const RValue &rhs) = 0;
};

class CollectMainEdgesPass : public FunctionPassBase {
  private:
    CollectMainEdgesCallbackBase *callback_;

    RValue as_set_value (VertexPtr v) {
      if (v->type() == op_set) {
        return as_rvalue (v.as <op_set>()->rhs());
      }

      if (v->type() == op_list) {
        VertexAdaptor <op_list> list = v;
        CREATE_VERTEX (new_v, op_index, list->array());
        return as_rvalue (new_v);
      }

      if (v->type() == op_prefix_inc ||
          v->type() == op_prefix_dec ||
          v->type() == op_postfix_dec ||
          v->type() == op_postfix_inc) {
        VertexAdaptor <meta_op_unary_op> unary = v;
        CREATE_VERTEX (one, op_int_const);
        CREATE_VERTEX  (res, op_add, unary->expr(), one);
        return as_rvalue (res);
      }

      if (OpInfo::arity (v->type()) == binary_opp) {
        VertexAdaptor <meta_op_binary_op> binary = v;
        CREATE_META_VERTEX_2 (
            res,
            meta_op_binary_op, OpInfo::base_op (v->type()),
            binary->lhs(), binary->rhs()
        );
        return as_rvalue (res);
      }

      kphp_fail();
      return RValue();
    }

    LValue as_lvalue (VertexPtr v) {
      int depth = 0;
      if (v->type() == op_foreach_param) {
        depth++;
        v = v.as <op_foreach_param>()->xs();
      }
      while (v->type() == op_index) {
        depth++;
        v = v.as <op_index>()->array();
      }

      tinf::Node *value = NULL;
      if (v->type() == op_var) {
        value = get_tinf_node (v->get_var_id());
      } else if (v->type() == op_conv_array_l || v->type() == op_conv_int_l) {
        kphp_assert (depth == 0);
        return as_lvalue (v.as <meta_op_unary_op>()->expr());
      } else if (v->type() == op_array) {
        kphp_fail();
      } else if (v->type() == op_func_call) {
        value = get_tinf_node (v.as <op_func_call>()->get_func_id(), -1);
      } else {
        kphp_error (0, dl_pstr ("Bug in compiler: Trying to use [%s] as lvalue", OpInfo::str (v->type()).c_str()));
        kphp_fail();
      }

      kphp_assert (value != 0);
      return LValue (value, &MultiKey::any_key (depth));
    }

    LValue as_lvalue (FunctionPtr function, int id) {
      return LValue (get_tinf_node (function, id), &MultiKey::any_key (0));
    }

    LValue as_lvalue (VarPtr var) {
      return LValue (get_tinf_node (var), &MultiKey::any_key (0));
    }

    const LValue &as_lvalue (const LValue &lvalue) {
      return lvalue;
    }

    void do_create_set (const LValue &lvalue, const RValue &rvalue) {
      callback_->create_set (lvalue, rvalue);
    }
    void do_create_less (const RValue &lhs, const RValue &rhs) {
      callback_->create_less (lhs, rhs);
    }
    void do_require_node (const RValue &a) {
      callback_->require_node (a);
    }

    template <class A, class B> void create_set (const A &a, const B &b) {
      do_create_set (as_lvalue (a), as_rvalue (b));
    }
    template <class A, class B> void create_less (const A &a, const B &b) {
      do_create_less (as_rvalue (a), as_rvalue (b));
    }
    template <class A> void require_node (const A &a) {
      do_require_node (as_rvalue (a));
    }


    void add_type_rule (VertexPtr v) {
      if (v->type() == op_function ||
          v->type() == op_func_decl ||
          v->type() == op_extern_func) {
        return;
      }

      switch (v->type_rule->type()) {
        case op_common_type_rule:
          create_set (v, v->type_rule);
          break;
        case op_gt_type_rule:
          create_less (v->type_rule, v);
          break;
        case op_lt_type_rule:
          create_less (v, v->type_rule);
          break;
        case op_eq_type_rule:
          create_less (v->type_rule, v);
          create_less (v, v->type_rule);
          break;
        default:
          assert (0 && "unreachable");
      }
    }

    void add_type_help (VertexPtr v) {
      if (v->type() != op_var) {
        return;
      }
      create_set (v, v->type_help);
    }

    void on_func_call (VertexAdaptor <op_func_call> call) {
      FunctionPtr function = call->get_func_id();
      VertexRange function_params = get_function_params (function->root);

      //hardcoded hack
      if (function->name == "array_unshift" || function->name == "array_push") {
        VertexRange args = call->args();
        LValue val = as_lvalue (args[0]);

        MultiKey *key = new MultiKey (*val.key);
        key->push_back (Key::any_key());
        val.key = key;

        args.next();
        FOREACH_VERTEX (args, i) {
          create_set (val, *i);
        }
      }


      if (!function->varg_flag) {
        int ii = 0;
        FOREACH_VERTEX (call->args(), arg) {

          if (!function->is_extern) {
            create_set (as_lvalue (function, ii), *arg);
          }

          VertexAdaptor <meta_op_func_param> param = function_params[ii];
          if (param->var()->ref_flag) {
            create_set (*arg, as_rvalue (function, ii));
          }

          ii++;
        }
      } else {
        FOREACH_VERTEX (call->args(), arg) {
          //meant <= array <var>, but it is array, so <= var is enough
          if (!function->is_extern) {
            create_less (*arg, tp_var);
          }
        }
      }

    }

    void on_return (VertexAdaptor <op_return> v) {
      create_set (as_lvalue (stage::get_function(), -1), v->expr());
    }

    void on_foreach (VertexAdaptor <op_foreach> foreach_op) {
      VertexAdaptor <op_foreach_param> params = foreach_op->params();
      VertexPtr xs, x, key;
      xs = params->xs();
      x = params->x();
      if (params->has_key()) {
        key = params->key();
      }
      if (x->ref_flag) {
        LValue xs_tinf = as_lvalue (xs);
        create_set (xs_tinf, tp_array);

        create_set (params, x->get_var_id());
      }
      create_set (x->get_var_id(), params);
      if (key.not_null()) {
        create_set (key->get_var_id(), tp_var);
      }
    }

    void on_index (VertexAdaptor <op_index> index) {
      create_set (index->array(), tp_array);
    }

    void on_list (VertexAdaptor <op_list> list) {
      create_less (tp_array, list->array());

      //Improve it!
      RValue val = as_set_value (list);

      FOREACH_VERTEX (list->list(), i) {
        VertexPtr cur = *i;
        if (cur->type() != op_lvalue_null) {
          create_set (cur, val);
        }
      }
    }

    void on_throw (VertexAdaptor <op_throw> throw_op) {
      create_less (tp_Exception, throw_op->expr());
      create_less (throw_op->expr(), tp_Exception);
    }

    void on_set_op (VertexPtr v) {
      VertexPtr lval;
      if (OpInfo::arity (v->type()) == binary_opp) {
        lval = v.as <meta_op_binary_op>()->lhs();
      } else if (OpInfo::arity (v->type()) == unary_opp) {
        lval = v.as <meta_op_unary_op>()->expr();
      } else {
        kphp_fail();
      }
      create_set (lval, as_set_value (v));
    }


    void ifi_fix (VertexPtr v) {
      is_func_id_t ifi_tp = get_ifi_id_ (v);
      if (ifi_tp == ifi_error) {
        return;
      }
      FOREACH_VERTEX (v, i) {
        VertexPtr cur = *i;
        if (cur->type() == op_var && cur->get_var_id()->type() == VarData::var_const_t) {
          continue;
        }
        if (cur->type() == op_var || (ifi_tp > ifi_isset && cur->type() == op_index)) {
          create_set (cur, tp_var);
        }

        if (cur->type() == op_var && ifi_tp != ifi_unset) {
          get_tinf_node (cur)->isset_flags |= ifi_tp;
        }
      }
    }

    void on_function (FunctionPtr function) {
      VertexRange params = get_function_params (function->root);
      int params_n = (int)params.size();

      for (int i = -1; i < params_n; i++) {
        require_node (as_rvalue (function, i));
      }

      if (function->is_extern) {
        PrimitiveType ret_type = function->root->type_help;
        if (ret_type == tp_Unknown) {
          ret_type = tp_var;
        }
        create_set (as_lvalue (function, -1), ret_type);

        for (int i = 0; i < params_n; i++) {
          PrimitiveType ptype = params[i]->type_help;
          if (ptype == tp_Unknown) {
            ptype = tp_Any;
          }
          //FIXME: type is const...
          create_set (as_lvalue (function, i), TypeData::get_type (ptype, tp_Any));
        }
      } else {
        for (int i = 0; i < params_n; i++) {
          //FIXME?.. just use pointer to node?..
          create_set (as_lvalue (function, i), function->param_ids[i]);
          create_set (function->param_ids[i], as_rvalue (function, i));
          if (function->varg_flag || function->is_callback) {
            create_set (as_lvalue (function, i), tp_var);
          }
        }
        if (function->used_in_source) {
          for (int i = -1; i < params_n; i++) {
            PrimitiveType x = tp_Unknown;

            if (i == -1) {
              if (function->root->type_rule.not_null()) {
                kphp_error_act (function->root->type_rule->type() == op_common_type_rule, "...", continue);
                VertexAdaptor <op_common_type_rule> common_type_rule = function->root->type_rule;
                VertexPtr rule = common_type_rule->expr();
                kphp_error_act (rule->type() == op_type_rule, "...", continue);
                x = rule->type_help;
              }
            } else {
              x = params[i]->type_help;
            }

            if (x == tp_Unknown) {
              x = tp_var;
            }
            const TypeData *tp = TypeData::get_type (x, tp_var);
            create_less (as_rvalue (function, i), tp);
            create_set (as_lvalue (function, i), tp);
          }

        }
      }
    }

  public:
    explicit CollectMainEdgesPass (CollectMainEdgesCallbackBase *callback) :
      callback_ (callback) {
    }

    string get_description() {
      return "Collect main tinf edges";
    }
    bool on_start (FunctionPtr function) {
      if (!FunctionPassBase::on_start (function)) {
        return false;
      }
      on_function (function);
      if (function->type() == FunctionData::func_extern) {
        return false;
      }
      return true;
    }

    VertexPtr on_enter_vertex (VertexPtr v, LocalT *local) {
      if (v->type_rule.not_null()) {
        add_type_rule (v);
      }
      //FIXME: type_rule should be used indead of type_help
      if (v->type_help != tp_Unknown) {
        add_type_help (v);
      }

      switch (v->type()) {
        //FIXME: varg_flag, is_callback
        case op_func_call:
          on_func_call (v);
          break;
        case op_return:
          on_return (v);
          break;
        case op_foreach:
          on_foreach (v);
          break;
        case op_index:
          on_index (v);
          break;
        case op_list:
          on_list (v);
          break;
        case op_throw:
          on_throw (v);
          break;
        default:
          break;
      }
      if (OpInfo::rl (v->type()) == rl_set ||
          v->type() == op_prefix_inc ||
          v->type() == op_postfix_inc ||
          v->type() == op_prefix_dec ||
          v->type() == op_postfix_dec) {
        on_set_op (v);
      }

      ifi_fix (v);

      return v;
    }

    void on_var (VarPtr var) {
      if (var->tinf_flag) {
        return;
      }
      //FIXME: use holder_function!!!
      if (!__sync_bool_compare_and_swap (&var->tinf_flag, false, true)) {
        return;
      }
      require_node (var);
      if (var->init_val.not_null()) {
        create_set (var, var->init_val);
      }
    }

    void on_define (DefinePtr def) {
      require_node (def->val);
    }

    void on_finish() {
      FOREACH (current_function->local_var_ids, var) {
        on_var (*var);
      }
      FOREACH (current_function->global_var_ids, var) {
        on_var (*var);
      }
      FOREACH (current_function->static_var_ids, var) {
        on_var (*var);
      }
      FOREACH (current_function->header_global_var_ids, var) {
        on_var (*var);
      }
      FOREACH (current_function->const_var_ids, var) {
        on_var (*var);
      }
      FOREACH (current_function->header_const_var_ids, var) {
        on_var (*var);
      }
      FOREACH (current_function->param_ids, var) {
        on_var (*var);
      }
      FOREACH (current_function->define_ids, def) {
        on_define (*def);
      }
    }
};

class NodeRecalc {
  protected:
    TypeData *new_type_;
    tinf::Node *node_;
    tinf::TypeInferer *inferer_;
    vector <TypeData *> types_stack;
  public:
    const TypeData *new_type();
    void add_dependency_impl (tinf::Node *from, tinf::Node *to);
    void add_dependency (const RValue &rvalue);
    void set_lca_at (const MultiKey *key, const RValue &rvalue);
    void set_lca_at (const MultiKey *key, VertexPtr expr);
    void set_lca_at (const MultiKey *key, PrimitiveType ptype);
    void set_lca (const RValue &rvalue);
    void set_lca (PrimitiveType ptype);
    void set_lca (FunctionPtr function, int id);
    void set_lca (VertexPtr vertex, const MultiKey *key = NULL);
    void set_lca (const TypeData *type, const MultiKey *key  = NULL);
    void set_lca (VarPtr var);
    NodeRecalc (tinf::Node *node, tinf::TypeInferer *inferer);
    virtual ~NodeRecalc(){}
    void on_changed();
    void push_type();
    TypeData *pop_type();
    virtual bool auto_edge_flag();

    virtual void do_recalc() = 0;

    void run();
};

class VarNodeRecalc : public NodeRecalc {
  public:
    VarNodeRecalc (tinf::VarNode *node, tinf::TypeInferer *inferer);
    void do_recalc();
};

class ExprNodeRecalc : public NodeRecalc {
  private:
    template <PrimitiveType tp>
      void recalc_ptype();

    void recalc_require (VertexAdaptor <op_require> require);
    void recalc_ternary (VertexAdaptor <op_ternary> ternary);
    void apply_type_rule_func (VertexAdaptor <op_type_rule_func> func, VertexPtr expr);
    void apply_type_rule_type (VertexAdaptor <op_type_rule> rule, VertexPtr expr);
    void apply_arg_ref (VertexAdaptor <op_arg_ref> arg, VertexPtr expr);
    void apply_index (VertexAdaptor <op_index> index, VertexPtr expr);
    void apply_type_rule (VertexPtr rule, VertexPtr expr);
    void recalc_func_call (VertexAdaptor <op_func_call> call);
    void recalc_var (VertexAdaptor <op_var> var);
    void recalc_push_back_return (VertexAdaptor <op_push_back_return> pb);
    void recalc_index (VertexAdaptor <op_index> index);
    void recalc_set (VertexAdaptor <op_set> set);
    void recalc_double_arrow (VertexAdaptor <op_double_arrow> arrow);
    void recalc_foreach_param (VertexAdaptor <op_foreach_param> param);
    void recalc_conv_array (VertexAdaptor <meta_op_unary_op> conv);
    void recalc_min_max (VertexAdaptor <meta_op_builtin_func> func);
    void recalc_array (VertexAdaptor <op_array> array);
    void recalc_plus_minus (VertexAdaptor <meta_op_unary_op> expr);
    void recalc_inc_dec (VertexAdaptor <meta_op_unary_op> expr);
    void recalc_arithm (VertexAdaptor <meta_op_binary_op> expr);
    void recalc_define_val (VertexAdaptor <op_define_val> define_val);
    void recalc_expr (VertexPtr expr);
  public:
    ExprNodeRecalc (tinf::ExprNode *node, tinf::TypeInferer *inferer);
    tinf::ExprNode *get_node();
    void do_recalc();

    bool auto_edge_flag();
};


