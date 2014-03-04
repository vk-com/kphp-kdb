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

#include "type-inferer.h"

void init_functions_tinf_nodes (FunctionPtr function) {
  assert (function->tinf_state == 1);
  VertexRange params = get_function_params (function->root);
  function->tinf_nodes.resize (params.size() + 1);
  for (int i = 0; i < (int)function->tinf_nodes.size(); i++) {
    function->tinf_nodes[i].param_i = i - 1;
    function->tinf_nodes[i].function_ = function;

  }

  __sync_synchronize();
  function->tinf_state = 2;
}

tinf::Node *get_tinf_node (FunctionPtr function, int id) {
  if (function->tinf_state == 0) {
    if (__sync_bool_compare_and_swap (&function->tinf_state, 0, 1)) {
      init_functions_tinf_nodes (function);
    }
  }
  while (function->tinf_state != 2) {
  }

  assert (-1 <= id && id + 1 < (int)function->tinf_nodes.size());
  return &function->tinf_nodes[id + 1];
}

tinf::Node *get_tinf_node (VertexPtr vertex) {
  return &vertex->tinf_node;
}
tinf::Node *get_tinf_node (VarPtr vertex) {
  return &vertex->tinf_node;
}

const TypeData *get_type (VertexPtr vertex, tinf::TypeInferer *inferer) {
  return inferer->get_type (get_tinf_node (vertex));
}

const TypeData *get_type (VarPtr var, tinf::TypeInferer *inferer) {
  return inferer->get_type (get_tinf_node (var));
}

const TypeData *get_type (FunctionPtr function, int id, tinf::TypeInferer *inferer) {
  return inferer->get_type (get_tinf_node (function, id));
}

const TypeData *fast_get_type (VertexPtr vertex) {
  return get_tinf_node (vertex)->get_type();
}
const TypeData *fast_get_type (VarPtr var) {
  return get_tinf_node (var)->get_type();
}
const TypeData *fast_get_type (FunctionPtr function, int id) {
  return get_tinf_node (function, id)->get_type();
}

void tinf::VarNode::recalc (tinf::TypeInferer *inferer) {
  VarNodeRecalc f(this, inferer);
  f.run();
}

void tinf::ExprNode::recalc (tinf::TypeInferer *inferer) {
  ExprNodeRecalc f(this, inferer);
  f.run();
}

string tinf::VarNode::get_description() {
  string res;
  if (function_.is_null()) {
    if (var_.not_null() && var_->holder_func.not_null()) {
      function_ = var_->holder_func;
    }
  }
  if (function_.not_null()) {
    res += function_->name + ":";
  }
  if (var_.is_null()) {
    res += "param[" + int_to_str (param_i) + "]"; //FIXME
  } else {
    res += "var[" + var_->name + "]";
  }
  return res;
}

string tinf::TypeNode::get_description() {
  return "type[" + type_out (type_) + "]";
}
string tinf::ExprNode::get_description() {
  return "expr[" + OpInfo::str (expr_->type()) + "]";
}

const TypeData *NodeRecalc::new_type() {
  return new_type_;
}

bool NodeRecalc::auto_edge_flag() {
  return false;
}
void NodeRecalc::add_dependency_impl (tinf::Node *from, tinf::Node *to) {
  tinf::Edge *e = new tinf::Edge();
  e->from = from;
  e->to = to;
  e->from_at = NULL;
  inferer_->add_edge (e);
  inferer_->add_node (e->to);
}
void NodeRecalc::add_dependency (const RValue &rvalue) {
  if (auto_edge_flag() && rvalue.node != NULL) {
    add_dependency_impl (node_, rvalue.node);
  }
}
void NodeRecalc::set_lca_at (const MultiKey *key, const RValue &rvalue) {
  if (new_type_->error_flag()) {
    return;
  }
  const TypeData *type = NULL;
  if (rvalue.node != NULL) {
    if (auto_edge_flag()) {
      add_dependency_impl (node_, rvalue.node);
    }
    __sync_synchronize();
    type = rvalue.node->get_type();
  } else if (rvalue.type != NULL) {
    type = rvalue.type;
  } else {
    kphp_fail();
  }

  if (rvalue.key != NULL) {
    type = type->const_read_at (*rvalue.key);
  }
  if (key == NULL) {
    key = &MultiKey::any_key (0);
  }

  if (type->error_flag()) {
    return;
  }
  if (types_stack.empty()) {
    new_type_->set_lca_at (*key, type, !rvalue.drop_or_false);
  } else {
    types_stack.back()->set_lca_at (*key, type, !rvalue.drop_or_false);
  }
  if (new_type_->error_flag()) {
    kphp_error (0, dl_pstr ("Type Error [%s]\n", node_->get_description().c_str()));
  }
}
void NodeRecalc::set_lca_at (const MultiKey *key, VertexPtr expr) {
  set_lca_at (key, as_rvalue (expr));
}
void NodeRecalc::set_lca_at (const MultiKey *key, PrimitiveType ptype) {
  set_lca_at (key, as_rvalue (ptype));
}

void NodeRecalc::set_lca (const RValue &rvalue) {
  set_lca_at (NULL, rvalue);
}
void NodeRecalc::set_lca (PrimitiveType ptype) {
  set_lca (as_rvalue (ptype));
}
void NodeRecalc::set_lca (FunctionPtr function, int id) {
  return set_lca (as_rvalue (function, id));
}
void NodeRecalc::set_lca (VertexPtr vertex, const MultiKey *key /* = NULL*/) {
  return set_lca (as_rvalue (vertex, key));
}
void NodeRecalc::set_lca (const TypeData *type, const MultiKey *key /* = NULL*/) {
  return set_lca (as_rvalue (type, key));
}
void NodeRecalc::set_lca (VarPtr var) {
  return set_lca (as_rvalue (var));
}

NodeRecalc::NodeRecalc (tinf::Node *node, tinf::TypeInferer *inferer) :
  node_ (node),
  inferer_ (inferer)
{
}

void NodeRecalc::on_changed() {
  __sync_synchronize();

  node_->set_type (new_type_);
  new_type_ = NULL;

  __sync_synchronize();

  AutoLocker <Lockable *> locker (node_);
  FOREACH (node_->rev_next_range(), it) {
    tinf::Edge *e = *it;
    inferer_->recalc_node (e->from);
  }
}

void NodeRecalc::run() {
  const TypeData *old_type = node_->get_type();
  new_type_ = old_type->clone();

  TypeData::upd_generation (new_type_->generation());
  TypeData::generation_t old_generation = new_type_->generation();
  TypeData::inc_generation();

  do_recalc();
  new_type_->fix_inf_array();

  //fprintf (stderr, "upd %d %p %s %s->%s %d\n", get_thread_id(), node_, node_->get_description().c_str(), type_out (node_->get_type()).c_str(), type_out (new_type_).c_str(), new_type_->generation() != old_generation);
  if (new_type_->generation() != old_generation) {
    on_changed();
  }

  delete new_type_;
}

void NodeRecalc::push_type() {
  types_stack.push_back (TypeData::get_type (tp_Unknown)->clone());
}
TypeData *NodeRecalc::pop_type() {
  TypeData *result = types_stack.back();
  types_stack.pop_back();
  return result;
}

VarNodeRecalc::VarNodeRecalc (tinf::VarNode *node, tinf::TypeInferer *inferer) :
  NodeRecalc (node, inferer) {
  }

void VarNodeRecalc::do_recalc() {
  //fprintf (stderr, "recalc var %d %p %s\n", get_thread_id(), node_, node_->get_description().c_str());

  if (inferer_->is_finished()) {
    kphp_error (0, dl_pstr ("%s: %d\n", node_->get_description().c_str(), node_->recalc_cnt_));
    kphp_fail();
  }
  FOREACH (node_->next_range(), it) {
    tinf::Edge *e = *it;
    set_lca_at (e->from_at, e->to);
    inferer_->add_node (e->to);
  }
}
template <PrimitiveType tp>
void ExprNodeRecalc::recalc_ptype() {

  set_lca (TypeData::get_type (tp));
}

void ExprNodeRecalc::recalc_require (VertexAdaptor <op_require> require) {
  FunctionPtr last_function = require->back()->get_func_id();
  set_lca (last_function, -1);
}

void ExprNodeRecalc::recalc_ternary (VertexAdaptor <op_ternary> ternary) {
  set_lca (ternary->true_expr());
  set_lca (ternary->false_expr());
}

void ExprNodeRecalc::apply_type_rule_func (VertexAdaptor <op_type_rule_func> func, VertexPtr expr) {
  if (func->str_val == "lca") {
    FOREACH_VERTEX (func->args(), i) {
      //TODO: is it hack?
      apply_type_rule (*i, expr);
    }
    return;
  }
  if (func->str_val == "OrFalse") {
    if (kphp_error (!func->args().empty(), "OrFalse with no arguments")) {
      recalc_ptype <tp_Error>();
    } else {
      apply_type_rule (func->args()[0], expr);
      recalc_ptype <tp_False> ();
    }
    return;
  }
  kphp_error (0, dl_pstr ("unknown type_rule function [%s]", func->str_val.c_str()));
  recalc_ptype <tp_Error>();
}
void ExprNodeRecalc::apply_type_rule_type (VertexAdaptor <op_type_rule> rule, VertexPtr expr) {
  set_lca (rule->type_help);
  if (!rule->empty()) {
    push_type();
    apply_type_rule (rule->args()[0], expr);
    TypeData *tmp = pop_type();
    set_lca_at (&MultiKey::any_key (1), tmp);
    delete tmp;
  }
}
void ExprNodeRecalc::apply_arg_ref (VertexAdaptor <op_arg_ref> arg, VertexPtr expr) {
  int i = arg->int_val;
  if (expr.is_null() || i < 1 || expr->type() != op_func_call ||
      i > (int)get_function_params (expr->get_func_id()->root).size()) {
    kphp_error (0, "error in type rule");
    recalc_ptype <tp_Error>();
  }

  VertexRange call_args = expr.as <op_func_call>()->args();
  if (i - 1 < (int)call_args.size()) {
    set_lca (call_args[i - 1]);
  }
}
void ExprNodeRecalc::apply_index (VertexAdaptor <op_index> index, VertexPtr expr) {
  push_type ();
  apply_type_rule (index->array(), expr);
  TypeData *type = pop_type();
  set_lca (type, &MultiKey::any_key (1));
  delete type;
}

void ExprNodeRecalc::apply_type_rule (VertexPtr rule, VertexPtr expr) {
  switch (rule->type()) {
    case op_type_rule_func:
      apply_type_rule_func (rule, expr);
      break;
    case op_type_rule:
      apply_type_rule_type (rule, expr);
      break;
    case op_arg_ref:
      apply_arg_ref (rule, expr);
      break;
    case op_index:
      apply_index (rule, expr);
      break;
    default:
      kphp_error (0, "error in type rule");
      recalc_ptype <tp_Error>();
      break;
  }
}

void ExprNodeRecalc::recalc_func_call (VertexAdaptor <op_func_call> call) {
  FunctionPtr function = call->get_func_id();
  if (function->root->type_rule.not_null()) {
    apply_type_rule (function->root->type_rule.as <meta_op_type_rule>()->expr(), call);
  } else {
    set_lca (function, -1);
  }
}

void ExprNodeRecalc::recalc_var (VertexAdaptor <op_var> var) {
  set_lca (var->get_var_id());
}

void ExprNodeRecalc::recalc_push_back_return (VertexAdaptor <op_push_back_return> pb) {
  set_lca (pb->array(), &MultiKey::any_key (1));
}

void ExprNodeRecalc::recalc_index (VertexAdaptor <op_index> index) {
  set_lca (index->array(), &MultiKey::any_key (1));
}

void ExprNodeRecalc::recalc_set (VertexAdaptor <op_set> set) {
  set_lca (set->lhs());
}

void ExprNodeRecalc::recalc_double_arrow (VertexAdaptor <op_double_arrow> arrow) {
  set_lca (arrow->value());
}

void ExprNodeRecalc::recalc_foreach_param (VertexAdaptor <op_foreach_param> param) {
  set_lca (param->xs(), &MultiKey::any_key (1));
}

void ExprNodeRecalc::recalc_conv_array (VertexAdaptor <meta_op_unary_op> conv) {
  VertexPtr arg = conv->expr();
  //FIXME: (extra dependenty)
  add_dependency (as_rvalue (arg));
  if (fast_get_type (arg)->ptype() == tp_array) {
    set_lca (drop_or_false (as_rvalue (arg)));
  } else {
    recalc_ptype <tp_array>();
    if (fast_get_type (arg)->ptype() != tp_Unknown) { //hack
      set_lca_at (&MultiKey::any_key (1), tp_var);
    }
  }
}

void ExprNodeRecalc::recalc_min_max (VertexAdaptor <meta_op_builtin_func> func) {
  VertexRange args = func->args();
  if (args.size() == 1) {
    set_lca (args[0], &MultiKey::any_key (1));
  } else {
    FOREACH_VERTEX (args, i) {
      set_lca (*i);
    }
  }
}

void ExprNodeRecalc::recalc_array (VertexAdaptor <op_array> array) {
  recalc_ptype <tp_array>();
  FOREACH_VERTEX (array->args(), i) {
    set_lca_at (&MultiKey::any_key (1), *i);
  }
}

void ExprNodeRecalc::recalc_plus_minus (VertexAdaptor <meta_op_unary_op> expr) {
  set_lca (drop_or_false (as_rvalue (expr->expr())));
  if (new_type()->ptype() == tp_string) {
    recalc_ptype <tp_var>();
  }
}

void ExprNodeRecalc::recalc_inc_dec (VertexAdaptor <meta_op_unary_op> expr) {
  //or false ???
  set_lca (drop_or_false (as_rvalue (expr->expr())));
}

void ExprNodeRecalc::recalc_arithm (VertexAdaptor <meta_op_binary_op> expr) {
  VertexPtr lhs = expr->lhs();
  VertexPtr rhs = expr->rhs();

  //FIXME: (extra dependency)
  add_dependency (as_rvalue (lhs));
  add_dependency (as_rvalue (rhs));

  if (fast_get_type (lhs)->ptype() == tp_bool) {
    recalc_ptype <tp_int>();
  } else {
    set_lca (drop_or_false (as_rvalue (lhs)));
  }

  if (fast_get_type (rhs)->ptype() == tp_bool) {
    recalc_ptype <tp_int>();
  } else {
    set_lca (drop_or_false (as_rvalue (rhs)));
  }

  if (new_type()->ptype() == tp_string) {
    recalc_ptype <tp_var>();
  }
}

void ExprNodeRecalc::recalc_define_val (VertexAdaptor <op_define_val> define_val) {
  //TODO: fix?
  set_lca (define_val->get_define_id()->val);
}

void ExprNodeRecalc::recalc_expr (VertexPtr expr) {
  switch (expr->type()) {
    case op_require:
      recalc_require (expr);
      break;
    case op_ternary:
      recalc_ternary (expr);
      break;
    case op_func_call:
      recalc_func_call (expr);
      break;
    case op_common_type_rule:
    case op_gt_type_rule:
    case op_lt_type_rule:
    case op_eq_type_rule:
      apply_type_rule (expr.as <meta_op_type_rule>()->expr(), VertexPtr());
      break;
    case op_var:
      recalc_var (expr);
      break;
    case op_push_back_return:
      recalc_push_back_return (expr);
      break;
    case op_index:
      recalc_index (expr);
      break;
    case op_set:
      recalc_set (expr);
      break;
    case op_false:
      recalc_ptype <tp_False>();
      break;
    case op_log_or_let:
    case op_log_and_let:
    case op_log_xor_let:
    case op_log_or:
    case op_log_and:
    case op_log_not:
    case op_conv_bool:
    case op_true:
    case op_eq2:
    case op_eq3:
    case op_neq2:
    case op_neq3:
    case op_lt:
    case op_gt:
    case op_le:
    case op_ge:
    case op_isset:
    case op_exit:
      recalc_ptype <tp_bool>();
      break;

    case op_conv_string:
    case op_concat:
    case op_string_build:
    case op_string:
      recalc_ptype <tp_string>();
      break;

    case op_conv_int:
    case op_conv_int_l:
    case op_int_const:
    case op_mod:
    case op_not:
    case op_or:
    case op_and:
      recalc_ptype <tp_int>();
      break;

    case op_conv_float:
    case op_float_const:
    case op_div:
      recalc_ptype <tp_float>();
      break;

    case op_conv_uint:
      recalc_ptype <tp_UInt>();
      break;

    case op_conv_long:
      recalc_ptype <tp_Long>();
      break;

    case op_conv_ulong:
      recalc_ptype <tp_ULong>();
      break;

    case op_conv_regexp:
      recalc_ptype <tp_regexp>();
      break;

    case op_double_arrow:
      recalc_double_arrow (expr);
      break;

    case op_foreach_param:
      recalc_foreach_param (expr);
      break;

    case op_conv_array:
    case op_conv_array_l:
      recalc_conv_array (expr);
      break;

    case op_min:
    case op_max:
      recalc_min_max (expr);
      break;

    case op_array:
      recalc_array (expr);
      break;

    case op_conv_var:
    case op_null:
      recalc_ptype <tp_var>();
      break;

    case op_plus:
    case op_minus:
      recalc_plus_minus (expr);
      break;

    case op_prefix_inc:
    case op_prefix_dec:
    case op_postfix_inc:
    case op_postfix_dec:
    case op_noerr:
      recalc_inc_dec (expr);
      break;

    case op_sub:
    case op_add:
    case op_mul:
    case op_shl:
    case op_shr:
      recalc_arithm (expr);
      break;

    case op_define_val:
      recalc_define_val (expr);
      break;

    default:
      recalc_ptype <tp_var>();
      break;
  }
}

bool ExprNodeRecalc::auto_edge_flag() {
  return node_->get_recalc_cnt() == 0;
}
ExprNodeRecalc::ExprNodeRecalc (tinf::ExprNode *node, tinf::TypeInferer *inferer) :
  NodeRecalc (node, inferer) {
  }
tinf::ExprNode *ExprNodeRecalc::get_node() {
  return (tinf::ExprNode *)node_;
}
void ExprNodeRecalc::do_recalc() {
  tinf::ExprNode *node = get_node();
  VertexPtr expr = node->get_expr();
  //fprintf (stderr, "recalc expr %d %p %s\n", get_thread_id(), node_, node_->get_description().c_str());
  stage::set_location (expr->get_location());
  recalc_expr (expr);
}

const TypeData *tinf::get_type (VertexPtr vertex) {
  return get_type (vertex, tinf::get_inferer());
}
const TypeData *tinf::get_type (VarPtr var) {
  return get_type (var, tinf::get_inferer());
}
const TypeData *tinf::get_type (FunctionPtr function, int id) {
  return get_type (function, id, tinf::get_inferer());
}

