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

#include "gentree.h"

#include "io.h"
#include "stage.h"

GenTree::GenTree () {
}

#define CE(x) if (!(x)) {return VertexPtr();}

void GenTree::init (const vector <Token *> *tokens_new, GenTreeCallbackBase *callback_new) {
  line_num = 0;
  in_func_cnt_ = 0;
  tokens = tokens_new;
  callback = callback_new;
  class_stack = vector <ClassInfo>();

  cur = tokens->begin();
  end = tokens->end();

  kphp_assert (cur != end);
  end--;
  kphp_assert ((*end)->type() == tok_end);

  line_num = (*cur)->line_num;
  stage::set_line (line_num);
}

bool GenTree::in_class() {
  return !class_stack.empty();
}
ClassInfo &GenTree::cur_class() {
  kphp_assert (in_class());
  return class_stack.back();
}
void GenTree::register_function (VertexPtr func) {
  stage::set_line (0);
  func = post_process (func);

  if (in_class()) {
    cur_class().members.push_back (func);
  } else {
    callback->register_function (func);
  }
}
void GenTree::enter_class (const string &class_name) {
  class_stack.push_back (ClassInfo());
  cur_class().name = class_name;
}
void GenTree::exit_and_register_class (VertexPtr root) {
  kphp_assert (in_class());
  cur_class().root = root;
  callback->register_class (cur_class());
  class_stack.pop_back();
}

void GenTree::enter_function() {
  in_func_cnt_++;
}
void GenTree::exit_function() {
  in_func_cnt_--;
}

void GenTree::next_cur (void) {
  if (cur != end) {
    cur++;
    if ((*cur)->line_num != -1) {
      line_num = (*cur)->line_num;
      stage::set_line (line_num);
    }
  }
}

bool GenTree::test_expect (TokenType tp) {
  return (*cur)->type() == tp;
}

#define expect(tp, msg) ({ \
  bool res;\
  if (kphp_error (test_expect (tp), dl_pstr ("Expected %s, found '%s'", msg, cur == end ? "END OF FILE" : (*cur)->to_str().c_str()))) {\
    res = false;\
  } else {\
    next_cur();\
    res = true;\
  }\
  res; \
})

#define expect2(tp1, tp2, msg) ({ \
  kphp_error (test_expect (tp1) || test_expect (tp2), dl_pstr ("Expected %s, found '%s'", msg, cur == end ? "END OF FILE" : (*cur)->to_str().c_str())); \
  if (cur != end) {next_cur();} \
  1;\
})

VertexPtr GenTree::get_var_name() {
  AutoLocation var_location (this);

  if ((*cur)->type() != tok_var_name) {
    return VertexPtr();
  }
  CREATE_VERTEX (var, op_var);
  var->str_val = (*cur)->str_val;

  set_location (var, var_location);

  next_cur();
  return var;
}

VertexPtr GenTree::get_var_name_ref() {
  int ref_flag = 0;
  if ((*cur)->type() == tok_and) {
    next_cur();
    ref_flag = 1;
  }

  VertexPtr name = get_var_name();
  if (name.not_null()) {
    name->ref_flag = ref_flag;
  } else {
    kphp_error (ref_flag == 0, "Expected var name");
  }
  return name;
}

int GenTree::open_parent() {
  if ((*cur)->type() == tok_oppar) {
    next_cur();
    return 1;
  }
  return 0;
}

#define close_parent(is_opened)\
  if (is_opened) {\
    CE (expect (tok_clpar, "')'"));\
  }

template <Operation EmptyOp>
bool GenTree::gen_list (vector <VertexPtr> *res, GetFunc f, TokenType delim) {
  //Do not clear res. Result must be appended to it.
  bool prev_delim = false;
  bool next_delim = true;

  while (next_delim) {
    VertexPtr v = (this->*f)();
    next_delim = (*cur)->type() == delim;

    if (v.is_null()) {
      if (EmptyOp != op_err && (prev_delim || next_delim)) {
        if (EmptyOp == op_none) {
          break;
        }
        CREATE_VERTEX (tmp, EmptyOp);
        v = tmp;
      } else if (prev_delim) {
        kphp_error (0, "Expected something after ','");
        return false;
      } else {
        break;
      }
    }

    res->push_back (v);
    prev_delim = true;

    if (next_delim) {
      next_cur();
    }
  }

  return true;
}

template <Operation Op> VertexPtr GenTree::get_conv() {
  AutoLocation conv_location (this);
  next_cur();
  VertexPtr first_node = get_expression();
  CE (!kphp_error (first_node.not_null(), "get_conv failed"));
  CREATE_VERTEX (conv, Op, first_node);
  set_location (conv, conv_location);
  return conv;
}

template <Operation Op> VertexPtr GenTree::get_varg_call() {
  AutoLocation call_location (this);
  next_cur();

  CE (expect (tok_oppar, "'('"));

  AutoLocation args_location (this);
  vector <VertexPtr> args_next;
  bool ok_args_next = gen_list <op_err> (&args_next, &GenTree::get_expression, tok_comma);
  CE (!kphp_error (ok_args_next, "get_varg_call failed"));
  CREATE_VERTEX (args, op_array, args_next);
  set_location (args, args_location);

  CE (expect (tok_clpar, "')'"));

  CREATE_VERTEX (call, Op, args);
  set_location (call, call_location);
  return call;
}

template <Operation Op> VertexPtr GenTree::get_require() {
  AutoLocation require_location (this);
  next_cur();
  bool is_opened = open_parent();
  vector <VertexPtr> next;
  bool ok_next = gen_list <op_err> (&next, &GenTree::get_expression, tok_comma);
  CE (!kphp_error (ok_next, "get_require_list failed"));
  close_parent (is_opened);
  CREATE_VERTEX (require, Op, next);
  set_location (require, require_location);
  return require;
}
template <Operation Op, Operation EmptyOp> VertexPtr GenTree::get_func_call() {
  AutoLocation call_location (this);
  const string &name = (*cur)->str_val;
  next_cur();

  CE (expect (tok_oppar, "'('"));
  vector <VertexPtr> next;
  bool ok_next = gen_list<EmptyOp> (&next, &GenTree::get_expression, tok_comma);
  CE (!kphp_error (ok_next, "get_reqire failed"));
  CE (expect (tok_clpar, "')'"));

  CREATE_VERTEX (call, Op, next);
  set_location (call, call_location);

  //hack..
  if (Op == op_func_call) {
    VertexAdaptor <op_func_call> func_call = call;
    func_call->str_val = name;
  }
  return call;
}

VertexPtr GenTree::get_string() {
  CREATE_VERTEX (str, op_string);
  set_location (str, AutoLocation(this));
  str->str_val = (*cur)->str_val;
  next_cur();
  return str;
}

VertexPtr GenTree::get_string_build() {
  AutoLocation sb_location (this);
  vector <VertexPtr> v_next;
  next_cur();
  while (cur != end && (*cur)->type() != tok_str_end) {
    if ((*cur)->type() == tok_str) {
      v_next.push_back (get_string());
    } else if ((*cur)->type() == tok_expr_begin) {
      next_cur();

      VertexPtr add = get_expression();
      CE (!kphp_error (add.not_null(), "Bad expression in string"));
      v_next.push_back (add);

      CE (expect (tok_expr_end, "'}'"));
    } else {
      VertexPtr add = get_expression();
      CE (!kphp_error (add.not_null(), "Bad expression in string"));
      v_next.push_back (add);
    }
  }
  CE (expect (tok_str_end, "'\"'"));
  CREATE_VERTEX (sb, op_string_build, v_next);
  set_location (sb, sb_location);
  return sb;
}

VertexPtr GenTree::get_postfix_expression (VertexPtr res) {
  //postfix operators ++, --, [], ->
  bool need = true;
  while (need && cur != end) {
    vector <Token*>::const_iterator op = cur;
    TokenType tp = (*op)->type();
    need = false;

    if (tp == tok_inc) {
      CREATE_VERTEX (v, op_postfix_inc, res);
      set_location (v, AutoLocation(this));
      res = v;
      need = true;
      next_cur();
    } else if (tp == tok_dec) {
      CREATE_VERTEX (v, op_postfix_dec, res);
      set_location (v, AutoLocation(this));
      res = v;
      need = true;
      next_cur();
    } else if (tp == tok_opbrk || tp == tok_opbrc) {
      AutoLocation location (this);
      next_cur();
      VertexPtr i = get_expression();
      if (tp == tok_opbrk) {
        CE (expect (tok_clbrk, "']'"));
      } else {
        CE (expect (tok_clbrc, "'}'"));
      }
      //TODO: it should be to separate operations
      if (i.is_null()) {
        CREATE_VERTEX (v, op_index, res);
        res = v;
      } else {
        CREATE_VERTEX (v, op_index, res, i);
        res = v;
      }
      set_location (res, location);
      need = true;
    } else if (tp == tok_arrow) {
      AutoLocation location (this);
      next_cur();
      VertexPtr i = get_expr_top();
      CE (!kphp_error (i.not_null(), "Failed to parse right argument of '->'"));
      CREATE_VERTEX (v, op_arrow, res, i);
      res = v;
      set_location (res, location);
      need = true;
    }

  }
  return res;
}

VertexPtr GenTree::get_expr_top() {
  vector <Token*>::const_iterator op = cur;

  VertexPtr res, first_node;
  TokenType type = (*op)->type();

  bool return_flag = true;
  switch (type) {
    case tok_line_c: {
      CREATE_VERTEX (v, op_int_const);
      set_location (v, AutoLocation(this));
      v->str_val = int_to_str (stage::get_line());
      res = v;
      next_cur();
      break;
    }
    case tok_file_c: {
      CREATE_VERTEX (v, op_string);
      set_location (v, AutoLocation(this));
      v->str_val = stage::get_file()->unified_file_name;
      next_cur();
      res = v;
      break;
    }
    case tok_func_c: {
      CREATE_VERTEX (v, op_function_c);
      set_location (v, AutoLocation(this));
      next_cur();
      res = v;
      break;
    }
    case tok_int_const: {
      CREATE_VERTEX (v, op_int_const);
      set_location (v, AutoLocation(this));
      v->str_val = (*cur)->str_val;
      next_cur();
      res = v;
      break;
    }
    case tok_float_const: {
      CREATE_VERTEX (v, op_float_const);
      set_location (v, AutoLocation(this));
      v->str_val = (*cur)->str_val;
      next_cur();
      res = v;
      break;
    }
    case tok_null: {
      CREATE_VERTEX (v, op_null);
      set_location (v, AutoLocation(this));
      next_cur();
      res = v;
      break;
    }
    case tok_false: {
      CREATE_VERTEX (v, op_false);
      set_location (v, AutoLocation(this));
      next_cur();
      res = v;
      break;
    }
    case tok_true: {
      CREATE_VERTEX (v, op_true);
      set_location (v, AutoLocation(this));
      next_cur();
      res = v;
      break;
    }
    case tok_var_name: {
      res = get_var_name();
      return_flag = false;
      break;
    }
    case tok_str:
      res = get_string();
      break;
    case tok_conv_int:
      res = get_conv <op_conv_int>();
      break;
    case tok_conv_bool:
      res = get_conv <op_conv_bool>();
      break;
    case tok_conv_float:
      res = get_conv <op_conv_float>();
      break;
    case tok_conv_string:
      res = get_conv <op_conv_string>();
      break;
    case tok_conv_long:
      res = get_conv <op_conv_long>();
      break;
    case tok_conv_uint:
      res = get_conv <op_conv_uint>();
      break;
    case tok_conv_ulong:
      res = get_conv <op_conv_ulong>();
      break;
    case tok_conv_array:
      res = get_conv <op_conv_array>();
      break;

    case tok_print: {
      AutoLocation print_location (this);
      next_cur();
      first_node = get_expression();
      CE (!kphp_error (first_node.not_null(), "Failed to get print argument"));
      first_node = conv_to <tp_string> (first_node);
      CREATE_VERTEX (print, op_print, first_node);
      set_location (print, print_location);
      res = print;
      break;
    }

    case tok_pack:
      res = get_varg_call <op_pack>();
      break;
    case tok_printf:
      res = get_varg_call <op_printf>();
      break;
    case tok_sprintf:
      res = get_varg_call <op_sprintf>();
      break;
    case tok_store_many:
      res = get_varg_call <op_store_many>();
      break;
    case tok_exit:
      res = get_exit();
      break;
    case tok_require:
      res = get_require <op_require>();
      break;
    case tok_require_once:
      res = get_require <op_require_once>();
      break;

    case tok_func_name:
      cur++;
      if (!test_expect (tok_oppar)) {
        cur--;
        CREATE_VERTEX (v, op_func_name);
        set_location (v, AutoLocation(this));
        next_cur();
        v->str_val = (*op)->str_val;
        res = v;
        return_flag = true;
        break;
      }
      cur--;
      res = get_func_call <op_func_call, op_err>();
      return_flag = false;
      break;
    case tok_function:
      res = get_function (true);
      break;
    case tok_isset:
      res = get_func_call <op_isset, op_err>();
      break;
    case tok_array:
      res = get_func_call <op_array, op_none>();
      break;
    case tok_list:
      res = get_func_call <op_list_ce, op_lvalue_null>();
      break;
    case tok_defined:
      res = get_func_call <op_defined, op_err>();
      break;
    case tok_min: {
      VertexAdaptor <op_min> min_v = get_func_call <op_min, op_err>();
      VertexRange args = min_v->args();
      if (args.size() == 1) {
        args[0] = GenTree::conv_to (args[0], tp_array);
      }
      res = min_v;
      break;
    }
    case tok_max: {
      VertexAdaptor <op_max> max_v = get_func_call <op_max, op_err>();
      VertexRange args = max_v->args();
      if (args.size() == 1) {
        args[0] = GenTree::conv_to (args[0], tp_array);
      }
      res = max_v;
      break;
    }

    case tok_oppar:
      next_cur();
      res = get_expression();
      CE (!kphp_error (res.not_null(), "Failed to parse expression after '('"));
      res->parent_flag = true;
      CE (expect (tok_clpar, "')'"));
      break;
    case tok_str_begin:
      res = get_string_build();
      break;
    default:
      return VertexPtr();
  }

  if (return_flag) {
    return res;
  }

  res = get_postfix_expression (res);
  return res;
}

VertexPtr GenTree::get_unary_op() {
  Operation tp = OpInfo::tok_to_unary_op[(*cur)->type()];
  if (tp != op_err) {
    vector <Token*>::const_iterator op = cur;
    AutoLocation expr_location (this);
    next_cur();

    VertexPtr left = get_unary_op();
    if (left.is_null()) {
      return VertexPtr();
    }

    if (tp == op_log_not) {
      left = conv_to <tp_bool> (left);
    }
    if (tp == op_not) {
      left = conv_to <tp_int> (left);
    }
    CREATE_META_VERTEX_1 (expr, meta_op_unary_op, tp, left);
    set_location (expr, expr_location);
    return expr;
  }

  VertexPtr res = get_expr_top();
  return res;
}


VertexPtr GenTree::get_binary_op (int bin_op_cur, int bin_op_end, GetFunc next, bool till_ternary) {
  if (bin_op_cur == bin_op_end) {
    return (this->*next)();
  }

  VertexPtr left = get_binary_op (bin_op_cur + 1, bin_op_end, next, till_ternary);
  if (left.is_null()) {
    return VertexPtr();
  }

  bool need = true;
  bool ternary = bin_op_cur == OpInfo::ternaryP;
  //fprintf (stderr, "get binary op: [%d..%d], cur = %d[%s]\n", bin_op_cur, bin_op_end, tok_priority[cur == end ? 0 : (*cur)->type()], cur == end ? "<none>" : (*cur)->to_str().c_str());
  while (need && cur != end) {
    Operation tp = OpInfo::tok_to_binary_op[(*cur)->type()];
    if (tp == op_err || OpInfo::priority (tp) != bin_op_cur) {
      break;
    }
    if (ternary && till_ternary) {
      break;
    }
    AutoLocation expr_location (this);

    bool left_to_right = OpInfo::fixity (tp) == left_opp;

    vector <Token*>::const_iterator op = cur;
    next_cur();
    VertexPtr right, third;
    if (ternary) {
      right = get_expression();
    } else {
      right = get_binary_op (bin_op_cur + left_to_right, bin_op_end, next, till_ternary && bin_op_cur >= OpInfo::ternaryP);
    }
    if (right.is_null()) {
      kphp_error (0, dl_pstr ("Failed to parse second argument in [%s]", OpInfo::str (tp).c_str()));
      return VertexPtr();
    }

    if (ternary) {
      CE (expect (tok_colon, "':'"));
      //third = get_binary_op (bin_op_cur + 1, bin_op_end, next);
      third = get_expression_impl (true);
      if (third.is_null()) {
        kphp_error (0, dl_pstr ("Failed to parse third argument in [%s]", OpInfo::str (tp).c_str()));
        return VertexPtr();
      }
      left = conv_to <tp_bool> (left);
    }


    if (tp == op_log_or || tp == op_log_and || tp == op_log_or_let || tp == op_log_and_let || tp == op_log_xor_let) {
      left = conv_to <tp_bool> (left);
      right = conv_to <tp_bool> (right);
    }
    if (tp == op_set_or || tp == op_set_and || tp == op_set_xor || tp == op_set_shl || tp == op_set_shr) {
      right = conv_to <tp_int> (right);
    }
    if (tp == op_or || tp == op_and || tp == op_xor) {
      left = conv_to <tp_int> (left);
      right = conv_to <tp_int> (right);
    }

    VertexPtr expr;
    if (ternary) {
      CREATE_VERTEX (v, op_ternary, left, right, third);
      expr = v;
    } else {
      CREATE_META_VERTEX_2 (v, meta_op_binary_op, tp, left, right);
      expr = v;
    }
    set_location (expr, expr_location);

    left = expr;

    need = need && (left_to_right || ternary);
  }
  return left;

}

VertexPtr GenTree::get_expression_impl (bool till_ternary) {
  return get_binary_op (OpInfo::bin_op_begin, OpInfo::bin_op_end, &GenTree::get_unary_op, till_ternary);
}
VertexPtr GenTree::get_expression() {
  return get_expression_impl (false);
}

VertexPtr GenTree::embrace (VertexPtr v) {
  if (v->type() != op_seq) {
    CREATE_VERTEX (brace, op_seq, v);
    ::set_location (brace, v->get_location());
    return brace;
  }

  return v;
}

VertexPtr GenTree::get_def_value() {
  VertexPtr val;

  if ((*cur)->type() == tok_eq1) {
    next_cur();
    val = get_expression();
    kphp_error (val.not_null(), "Cannot parse function parameter");
  }

  return val;
}

VertexPtr GenTree::get_func_param () {
  VertexPtr res;
  AutoLocation st_location (this);
  if (test_expect (tok_func_name)) { // callback
    CREATE_VERTEX (name, op_func_name);
    set_location (name, st_location);
    name->str_val = (*cur)->str_val;
    next_cur();

    CE (expect (tok_oppar, "'('"));
    int param_cnt = 0;
    if (!test_expect (tok_clpar)) {
      while (true) {
        param_cnt++;
        CE (expect (tok_var_name, "'var_name'"));
        if (test_expect (tok_clpar)) {
          break;
        }
        CE (expect (tok_comma, "','"));
      }
    }
    CE (expect (tok_clpar, "')'"));

    vector <VertexPtr> next;
    next.push_back (name);
    VertexPtr def_val = get_def_value();
    if (def_val.not_null()) {
      next.push_back (def_val);
    }
    CREATE_VERTEX (v, op_func_param_callback, next);
    set_location (v, st_location);
    v->param_cnt = param_cnt;
    res = v;
  } else {
    VertexPtr name = get_var_name_ref();
    if (name.is_null()) {
      return VertexPtr();
    }

    vector <VertexPtr> next;
    next.push_back (name);

    PrimitiveType tp = get_type_help();

    VertexPtr def_val = get_def_value();
    if (def_val.not_null()) {
      next.push_back (def_val);
    }
    CREATE_VERTEX (v, op_func_param, next);
    set_location (v, st_location);

    if (tp != tp_Unknown) {
      v->type_help = tp;
    }
    res = v;
  }

  return res;
}

VertexPtr GenTree::get_foreach_param () {
  AutoLocation location (this);
  VertexPtr xs = get_expression();
  CE (!kphp_error (xs.not_null(), ""));

  CE (expect (tok_as, "'as'"));

  VertexPtr x, key;
  x = get_var_name_ref();
  CE (!kphp_error (x.not_null(), ""));
  if ((*cur)->type() == tok_double_arrow) {
    next_cur();
    key = x;
    x = get_var_name_ref();
    CE (!kphp_error (x.not_null(), ""));
  }

  vector <VertexPtr> next;
  next.push_back (xs);
  next.push_back (x);
  if (key.not_null()) {
    next.push_back (key);
  }
  CREATE_VERTEX (res, op_foreach_param, next);
  set_location (res, location);
  return res;
}

VertexPtr GenTree::conv_to (VertexPtr x, PrimitiveType tp, int ref_flag) {
  if (ref_flag) {
    switch (tp) {
      case tp_array:
        return conv_to_lval <tp_array> (x);
      case tp_int:
        return conv_to_lval <tp_int> (x);
      case tp_var:
        return x;
        break;
      default:
        kphp_error (0, "convert_to not array with ref_flag");
        return x;
    }
  }
  switch (tp) {
    case tp_int:
      return conv_to <tp_int> (x);
    case tp_bool:
      return conv_to <tp_bool> (x);
    case tp_string:
      return conv_to <tp_string> (x);
    case tp_float:
      return conv_to <tp_float> (x);
    case tp_array:
      return conv_to <tp_array> (x);
    case tp_UInt:
      return conv_to <tp_UInt> (x);
    case tp_Long:
      return conv_to <tp_Long> (x);
    case tp_ULong:
      return conv_to <tp_ULong> (x);
    case tp_regexp:
      return conv_to <tp_regexp> (x);
    default:
      return x;
  }
}

VertexPtr GenTree::get_actual_value (VertexPtr v) {
  if (v->type() == op_var && v->extra_type == op_ex_var_const) {
    return v->get_var_id()->init_val;
  }
  return v;
}

PrimitiveType GenTree::get_ptype() {
  PrimitiveType tp;
  TokenType tok = (*cur)->type();
  switch (tok) {
    case tok_int:
      tp = tp_int;
      break;
    case tok_string:
      tp = tp_string;
      break;
    case tok_float:
      tp = tp_float;
      break;
    case tok_array:
      tp = tp_array;
      break;
    case tok_bool:
      tp = tp_bool;
      break;
    case tok_var:
      tp = tp_var;
      break;
    case tok_Exception:
      tp = tp_Exception;
      break;
    case tok_func_name:
      tp = get_ptype_by_name ((*cur)->str_val);
      break;
    default:
      tp = tp_Error;
      break;
  }
  if (tp != tp_Error) {
    next_cur();
  }
  return tp;
}

PrimitiveType GenTree::get_type_help (void) {
  PrimitiveType res = tp_Unknown;
  if ((*cur)->type() == tok_triple_colon) {
    next_cur();
    res = get_ptype();
    kphp_error (res != tp_Error, "Cannot parse type");
  }
  return res;
}

VertexPtr GenTree::get_type_rule_func (void) {
  AutoLocation rule_location (this);
  string_ref name = (*cur)->str_val;
  next_cur();
  CE (expect (tok_lt, "<"));
  vector <VertexPtr> next;
  bool ok_next = gen_list<op_err> (&next, &GenTree::get_type_rule_, tok_comma);
  CE (!kphp_error (ok_next, "Failed get_type_rule_func"));
  CE (expect (tok_gt, ">"));

  CREATE_VERTEX (rule, op_type_rule_func, next);
  set_location (rule, rule_location);
  rule->str_val = name;
  return rule;
}
VertexPtr GenTree::get_type_rule_ (void) {
  PrimitiveType tp = get_ptype();
  TokenType tok = (*cur)->type();
  VertexPtr res = VertexPtr(NULL);
  if (tp != tp_Error) {
    AutoLocation arr_location (this);

    VertexPtr first;
    if (tp == tp_array && tok == tok_lt) {
      next_cur();
      first = get_type_rule_();
      if (kphp_error (first.not_null(), "Cannot parse type_rule (1)")) {
        return VertexPtr();
      }
      CE (expect (tok_gt, "'>'"));
    }

    vector <VertexPtr> next;
    if (first.not_null()) {
      next.push_back (first);
    }
    CREATE_VERTEX (arr, op_type_rule, next);
    arr->type_help = tp;
    set_location (arr, arr_location);
    res = arr;
  } else if (tok == tok_func_name) {
    if ((*cur)->str_val.eq ("lca") || (*cur)->str_val.eq ("OrFalse")) {
      res = get_type_rule_func ();
    } else if ((*cur)->str_val.eq ("self")) {
      CREATE_VERTEX (self, op_self);
      res = self;
    } else if ((*cur)->str_val.eq ("CONST")) {
      next_cur();
      res = get_type_rule_();
      if (res.not_null()) {
        res->extra_type = op_ex_rule_const;
      }
    } else {
      kphp_error (
        0,
        dl_pstr ("Can't parse type_rule. Unknown string [%s]", (*cur)->str_val.c_str())
      );
    }
  } else if (tok == tok_xor) {
    next_cur();
    if (kphp_error (test_expect (tok_int_const), "Int expected")) {
      return VertexPtr();
    }
    int cnt = 0;
    for (const char *s = (*cur)->str_val.begin(), *t = (*cur)->str_val.end(); s != t; s++) {
      cnt = cnt * 10 + *s - '0';
    }
    CREATE_VERTEX (v, op_arg_ref);
    set_location (v, AutoLocation(this));
    v->int_val = cnt;
    res = v;
    next_cur();
    while (test_expect(tok_opbrk)) {
      AutoLocation opbrk_location (this);
      next_cur();
      CE (expect (tok_clbrk, "]"));
      CREATE_VERTEX (index, op_index, res);
      set_location (index, opbrk_location);
      res = index;
    }
  }
  return res;
}

VertexPtr GenTree::get_type_rule (void) {
  vector <Token*>::const_iterator op = cur;
  VertexPtr res, first;

  TokenType tp = (*cur)->type();
  if (tp == tok_triple_colon || tp == tok_triple_eq ||
      tp == tok_triple_lt || tp == tok_triple_gt) {
    AutoLocation rule_location (this);
    next_cur();
    first = get_type_rule_();

    kphp_error_act (
      first.not_null(),
      "Cannot parse type rule",
      return VertexPtr()
    );

    CREATE_META_VERTEX_1 (rule, meta_op_base, OpInfo::tok_to_op[tp], first);

    set_location (rule, rule_location);
    res = rule;
  }
  return res;
}

void GenTree::func_force_return (VertexPtr root, VertexPtr val) {
  if (root->type() != op_function) {
    return;
  }
  VertexAdaptor <op_function> func = root;

  VertexPtr cmd = func->cmd();
  assert (cmd->type() == op_seq);

  if (val.is_null()) {
    CREATE_VERTEX (return_val, op_null);
    val = return_val;
  }

  CREATE_VERTEX (return_node, op_return, val);
  vector <VertexPtr> next = cmd->get_next();
  next.push_back (return_node);
  CREATE_VERTEX (seq, op_seq, next);
  func->cmd() = seq;
}

template <Operation Op>
VertexPtr GenTree::get_multi_call (GetFunc f) {
  TokenType type = (*cur)->type();
  AutoLocation seq_location (this);
  next_cur();

  vector <VertexPtr> next;
  bool ok_next = gen_list<op_err> (&next, f, tok_comma);
  CE (!kphp_error (ok_next, "Failed get_multi_call"));

  for (int i = 0, ni = (int)next.size(); i < ni; i++) {
    if (type == tok_echo || type == tok_dbg_echo) {
      next[i] = conv_to <tp_string> (next[i]);
    }
    CREATE_VERTEX (v, Op, next[i]);
    ::set_location (v, next[i]->get_location());
    next[i] = v;
  }
  CREATE_VERTEX (seq, op_seq, next);
  set_location (seq, seq_location);
  return seq;
}


VertexPtr GenTree::get_return() {
  AutoLocation ret_location (this);
  next_cur();
  VertexPtr return_val = get_expression();
  if (return_val.is_null()) {
    CREATE_VERTEX (tmp, op_null);
    set_location (tmp, AutoLocation(this));
    return_val = tmp;
  }
  CREATE_VERTEX (ret, op_return, return_val);
  set_location (ret, ret_location);
  CE (expect (tok_semicolon, "';'"));
  return ret;
}

VertexPtr GenTree::get_exit() {
  AutoLocation exit_location (this);
  next_cur();
  bool is_opened = open_parent();
  VertexPtr exit_val;
  if (is_opened) {
    exit_val = get_expression();
    close_parent (is_opened);
  }
  if (exit_val.is_null()) {
    CREATE_VERTEX (tmp, op_int_const);
    tmp->str_val = "0";
    exit_val = tmp;
  }
  CREATE_VERTEX (v, op_exit, exit_val);
  set_location (v, exit_location);
  return v;
}

template <Operation Op>
VertexPtr GenTree::get_break_continue() {
  AutoLocation res_location (this);
  next_cur();
  VertexPtr first_node = get_expression();
  CE (expect (tok_semicolon, "';'"));

  if (first_node.is_null()) {
    CREATE_VERTEX (one, op_int_const);
    one->str_val = "1";
    first_node = one;
  }

  CREATE_VERTEX (res, Op, first_node);
  set_location (res, res_location);
  return res;
}

VertexPtr GenTree::get_foreach() {
  AutoLocation foreach_location (this);
  next_cur();

  CE (expect (tok_oppar, "'('"));
  VertexPtr first_node = get_foreach_param();
  CE (!kphp_error (first_node.not_null(), "Failed to parse 'foreach' params"));

  CE (expect (tok_clpar, "')'"));

  VertexPtr second_node = get_statement();
  CE (!kphp_error (second_node.not_null(), "Failed to parse 'foreach' body"));

  CREATE_VERTEX (foreach, op_foreach, first_node, embrace (second_node));
  set_location (foreach, foreach_location);
  return foreach;
}

VertexPtr GenTree::get_while() {
  AutoLocation while_location (this);
  next_cur();
  CE (expect (tok_oppar, "'('"));
  VertexPtr first_node = get_expression();
  CE (!kphp_error (first_node.not_null(), "Failed to parse 'while' condition"));
  first_node = conv_to <tp_bool> (first_node);
  CE (expect (tok_clpar, "')'"));

  VertexPtr second_node = get_statement();
  CE (!kphp_error (second_node.not_null(), "Failed to parse 'while' body"));

  CREATE_VERTEX (while_vertex, op_while, first_node, embrace (second_node));
  set_location (while_vertex, while_location);
  return while_vertex;
}

VertexPtr GenTree::get_if() {
  AutoLocation if_location (this);
  VertexPtr if_vertex;
  next_cur();
  CE (expect (tok_oppar, "'('"));
  VertexPtr first_node = get_expression();
  CE (!kphp_error (first_node.not_null(), "Failed to parse 'if' condition"));
  first_node = conv_to <tp_bool> (first_node);
  CE (expect (tok_clpar, "')'"));

  VertexPtr second_node = get_statement();
  CE (!kphp_error (second_node.not_null(), "Failed to parse 'if' body"));
  second_node = embrace (second_node);

  VertexPtr third_node = VertexPtr();
  if ((*cur)->type() == tok_else) {
    next_cur();
    third_node = get_statement();
    CE (!kphp_error (third_node.not_null(), "Failed to parse 'else' statement"));
  }

  if (third_node.not_null()) {
    if (third_node->type() != op_if) {
      third_node = embrace (third_node);
    }
    CREATE_VERTEX (v, op_if, first_node, second_node, third_node);
    if_vertex = v;
  } else {
    CREATE_VERTEX (v, op_if, first_node, second_node);
    if_vertex = v;
  }
  set_location (if_vertex, if_location);
  return if_vertex;
}

VertexPtr GenTree::get_for() {
  AutoLocation for_location (this);
  next_cur();
  CE (expect (tok_oppar, "'('"));

  AutoLocation pre_cond_location (this);
  vector <VertexPtr> first_next;
  bool ok_first_next = gen_list <op_err> (&first_next, &GenTree::get_expression, tok_comma);
  CE (!kphp_error (ok_first_next, "Failed to parse 'for' precondition"));
  CREATE_VERTEX (pre_cond, op_seq, first_next);
  set_location (pre_cond, pre_cond_location);

  CE (expect (tok_semicolon, "';'"));

  AutoLocation cond_location (this);
  vector <VertexPtr> second_next;
  bool ok_second_next = gen_list <op_err> (&second_next, &GenTree::get_expression, tok_comma);
  CE (!kphp_error (ok_second_next, "Failed to parse 'for' action"));
  if (second_next.empty()) {
    CREATE_VERTEX (v_true, op_true);
    second_next.push_back (v_true);
  } else {
    second_next.back() = conv_to <tp_bool> (second_next.back());
  }
  CREATE_VERTEX (cond, op_seq_comma, second_next);
  set_location (cond, cond_location);

  CE (expect (tok_semicolon, "';'"));

  AutoLocation post_cond_location (this);
  vector <VertexPtr> third_next;
  bool ok_third_next = gen_list <op_err> (&third_next, &GenTree::get_expression, tok_comma);
  CE (!kphp_error (ok_third_next, "Failed to parse 'for' postcondition"));
  CREATE_VERTEX (post_cond, op_seq, third_next);
  set_location (post_cond, post_cond_location);

  CE (expect (tok_clpar, "')'"));

  VertexPtr cmd = get_statement();
  CE (!kphp_error (cmd.not_null(), "Failed to parse 'for' statement"));

  cmd = embrace (cmd);
  CREATE_VERTEX (for_vertex, op_for, pre_cond, cond, post_cond, cmd);
  set_location (for_vertex, for_location);
  return for_vertex;
}

VertexPtr GenTree::get_do() {
  AutoLocation do_location (this);
  next_cur();
  VertexPtr first_node = get_statement();
  CE (!kphp_error (first_node.not_null(), "Failed to parser 'do' condition"));

  CE (expect (tok_while, "'while'"));

  CE (expect (tok_oppar, "'('"));
  VertexPtr second_node = get_expression();
  CE (!kphp_error (second_node.not_null(), "Faild to parse 'do' statement"));
  second_node = conv_to <tp_bool> (second_node);
  CE (expect (tok_clpar, "')'"));
  CE (expect (tok_semicolon, "';'"));

  CREATE_VERTEX (do_vertex, op_do, second_node, first_node);
  set_location (do_vertex, do_location);
  return do_vertex;
}

VertexPtr GenTree::get_switch() {
  AutoLocation switch_location (this);
  vector <VertexPtr> switch_next;

  next_cur();
  CE (expect (tok_oppar, "'('"));
  VertexPtr switch_val = get_expression();
  CE (!kphp_error (switch_val.not_null(), "Failed to parse 'switch' expression"));
  switch_next.push_back (switch_val);
  CE (expect (tok_clpar, "')'"));

  CE (expect (tok_opbrc, "'{'"));

  while ((*cur)->type() != tok_clbrc) {
    TokenType cur_type = (*cur)->type();
    VertexPtr case_val;

    AutoLocation case_location (this);
    if (cur_type == tok_case) {
      next_cur();
      case_val = get_expression();
      CE (!kphp_error (case_val.not_null(), "Failed to parse 'case' value"));

      CE (expect2 (tok_colon, tok_semicolon, "':'"));
    }
    if (cur_type == tok_default) {
      next_cur();
      CE (expect2 (tok_colon, tok_semicolon, "':'"));
    }

    AutoLocation seq_location (this);
    vector <VertexPtr> seq_next;
    while (cur != end) {
      if ((*cur)->type() == tok_clbrc) {
        break;
      }
      if ((*cur)->type() == tok_case) {
        break;
      }
      if ((*cur)->type() == tok_default) {
        break;
      }
      VertexPtr cmd = get_statement();
      if (cmd.not_null()) {
        seq_next.push_back (cmd);
      }
    }

    CREATE_VERTEX (seq, op_seq, seq_next);
    set_location (seq, seq_location);
    if (cur_type == tok_case) {
      CREATE_VERTEX (case_block, op_case, case_val, seq);
      set_location (case_block, case_location);
      switch_next.push_back (case_block);
    } else if (cur_type == tok_default) {
      CREATE_VERTEX (case_block, op_default, seq);
      set_location (case_block, case_location);
      switch_next.push_back (case_block);
    }
  }

  CREATE_VERTEX (switch_vertex, op_switch, switch_next);
  set_location (switch_vertex, switch_location);

  CE (expect (tok_clbrc, "'}'"));
  return switch_vertex;
}

static volatile int anonimous_func_id = 0;
VertexPtr GenTree::get_function (bool anonimous_flag) {
  AutoLocation func_location (this);

  TokenType type = (*cur)->type();
  kphp_assert (test_expect (tok_function) || test_expect (tok_ex_function));
  next_cur();

  string name_str;
  AutoLocation name_location (this);
  if (anonimous_flag) {
    string tmp_name = "a43f9d1_";
    int id = atomic_int_inc (&anonimous_func_id);
    tmp_name += int_to_str (id);
    name_str = tmp_name;
  } else {
    CE (expect (tok_func_name, "'tok_func_name'"));
    cur--;
    name_str = (*cur)->str_val;
    next_cur();
  }
  if (in_class()) {
    name_str = "mf_" + name_str;
  }
  CREATE_VERTEX (name, op_func_name);
  set_location (name, name_location);
  name->str_val = name_str;

  bool auto_flag = false;
  if (test_expect (tok_auto)) {
    next_cur();
    auto_flag = true;
  }

  CE (expect (tok_oppar, "'('"));

  AutoLocation params_location (this);
  vector <VertexPtr> params_next;
  if (in_class()) {
    CREATE_VERTEX (this_var, op_var);
    set_location (this_var, params_location);
    this_var->str_val = "this$";
    this_var->extra_type = op_ex_var_this;
    CREATE_VERTEX (param, op_func_param, this_var);
    params_next.push_back (param);
  }
  bool varg_flag = false;
  if (test_expect (tok_varg)) {
    varg_flag = true;
    next_cur();
  } else {
    bool ok_params_next = gen_list<op_err> (&params_next, &GenTree::get_func_param, tok_comma);
    CE (!kphp_error (ok_params_next, "Failed to parse function params"));
  }
  CREATE_VERTEX (params, op_func_param_list, params_next);
  set_location (params, params_location);

  CE (expect (tok_clpar, "')'"));

  bool throws_flag = false;
  if (test_expect (tok_throws)) {
    throws_flag = true;
    CE (expect (tok_throws, "'throws'"));
  }

  VertexPtr type_rule = get_type_rule();

  VertexPtr cmd;
  if ((*cur)->type() == tok_opbrc) {
    enter_function();
    cmd = get_statement();
    exit_function();
    kphp_error (type != tok_ex_function, "Extern function header should not have a body");
    CE (!kphp_error (cmd.not_null(), "Failed to parse function body"));
  } else {
    CE (expect (tok_semicolon, "';'"));
  }

  if (cmd.not_null()) {
    cmd = embrace (cmd);
  }

  VertexPtr res;
  if (cmd.is_null()) {
    if (type == tok_ex_function) {
      CREATE_VERTEX (func, op_extern_func, name, params);
      res = func;
    } else if (type == tok_function) {
      CREATE_VERTEX (func, op_func_decl, name, params);
      res = func;
    }
  } else {
    CREATE_VERTEX (func, op_function, name, params, cmd);
    res = func;
    func_force_return (res);
  }
  set_location (res, func_location);
  res->type_rule = type_rule;

  res->auto_flag = auto_flag;
  res->varg_flag = varg_flag;
  res->throws_flag = throws_flag;

  if (in_class()) {
    res->extra_type = op_ex_func_member;
  } else if (in_func_cnt_ == 0) {
    res->extra_type = op_ex_func_global;
  }

  register_function (res);

  if (anonimous_flag) {
    CREATE_VERTEX (func_ptr, op_func_name);
    set_location (func_ptr, name_location);
    func_ptr->str_val = name->str_val;
    return func_ptr;
  }
  return VertexPtr();
}

bool GenTree::check_seq_end() {
  if (!test_expect (tok_clbrc)) {
    kphp_error (0, "Failed to parse sequence");
    while (cur != end && !test_expect (tok_clbrc)) {
      next_cur();
    }
  }
  return expect (tok_clbrc, "'}'");
}

bool GenTree::check_statement_end() {
  //if (test_expect (tok_clbrc)) {
    //return true;
  //}
  if (!test_expect (tok_semicolon)) {
    kphp_error (0, "Failed to parse statement");
    while (cur != end && !test_expect (tok_clbrc) && !test_expect (tok_semicolon)) {
      next_cur();
    }
  }
  return expect (tok_semicolon, "';'");
}

VertexPtr GenTree::get_class() {
  AutoLocation class_location (this);
  CE (expect (tok_class, "'class'"));
  CE (!kphp_error (test_expect (tok_func_name), "Class name expected"));

  string name_str = (*cur)->str_val;
  CREATE_VERTEX (name, op_func_name);
  set_location (name, AutoLocation (this));
  name->str_val = name_str;

  next_cur();

  enter_class (name_str);
  VertexPtr class_body = get_statement();
  CE (!kphp_error (class_body.not_null(), "Failed to parse class body"));

  CREATE_VERTEX (class_vertex, op_class, name);
  set_location (class_vertex, class_location);

  exit_and_register_class (class_vertex);
  return VertexPtr();
}

VertexPtr GenTree::get_statement() {
  vector <Token*>::const_iterator op = cur;

  VertexPtr res, first_node, second_node, third_node, forth_node, tmp_node;
  TokenType type = (*op)->type();

  VertexPtr type_rule;
  switch (type) {
    case tok_class:
      res = get_class();
      return VertexPtr();
    case tok_opbrc:
      next_cur();
      res = get_seq();
      kphp_error (res.not_null(), "Failed to parse sequence");
      CE (check_seq_end());
      return res;
    case tok_return:
      res = get_return();
      return res;
    case tok_continue:
      res = get_break_continue <op_continue>();
      return res;
    case tok_break:
      res = get_break_continue <op_break>();
      return res;
    case tok_unset:
      res = get_func_call <op_unset, op_err>();
      CE (check_statement_end());
      return res;
    case tok_var_dump:
      res = get_func_call <op_var_dump, op_err>();
      CE (check_statement_end());
      return res;
    case tok_define:
      res = get_func_call <op_define, op_err>();
      CE (check_statement_end());
      return res;
    case tok_define_raw:
      res = get_func_call <op_define_raw, op_err>();
      CE (check_statement_end());
      return res;
    case tok_global:
      res = get_multi_call <op_global>(&GenTree::get_var_name);
      CE (check_statement_end());
      return res;
    case tok_static:
      res = get_multi_call <op_static>(&GenTree::get_expression);
      CE (check_statement_end());
      return res;
    case tok_echo:
      res = get_multi_call <op_echo>(&GenTree::get_expression);
      CE (check_statement_end());
      return res;
    case tok_dbg_echo:
      res = get_multi_call <op_dbg_echo>(&GenTree::get_expression);
      CE (check_statement_end());
      return res;
    case tok_throw: {
      AutoLocation throw_location (this);
      next_cur();
      first_node = get_expression();
      CE (!kphp_error (first_node.not_null(), "Empty expression in throw"));
      CREATE_VERTEX (throw_vertex, op_throw, first_node);
      set_location (throw_vertex, throw_location);
      CE (check_statement_end());
      return throw_vertex;
    }

    case tok_while:
      return get_while();
    case tok_if:
      return get_if();
    case tok_for:
      return get_for();
    case tok_do:
      return get_do();
    case tok_foreach:
      return get_foreach();
    case tok_switch:
      return get_switch();

    case tok_ex_function:
    case tok_function:
      return get_function();

    case tok_try: {
      AutoLocation try_location (this);
      next_cur();
      first_node = get_statement();
      CE (!kphp_error (first_node.not_null(), "Cannot parse try block"));
      CE (expect (tok_catch, "'catch'"));
      CE (expect (tok_oppar, "'('"));
      CE (expect (tok_Exception, "'Exception'"));
      second_node = get_expression();
      CE (!kphp_error (second_node.not_null(), "Cannot parse catch ( ??? )"));
      CE (!kphp_error (second_node->type() == op_var, "Expected variable name in 'catch'"));
      second_node->type_help = tp_Exception;

      CE (expect (tok_clpar, "')'"));
      third_node = get_statement();
      CE (!kphp_error (third_node.not_null(), "Cannot parse catch block"));
      CREATE_VERTEX (try_vertex, op_try, embrace (first_node), second_node, embrace (third_node));
      set_location (try_vertex, try_location);
      return try_vertex;
    }
    case tok_break_file: {
      CREATE_VERTEX (v, op_break_file);
      set_location (v, AutoLocation(this));
      next_cur();
      return v;
    }
    case tok_inline_html: {
      CREATE_VERTEX (html_code, op_string);
      set_location (html_code, AutoLocation(this));
      html_code->str_val = (*cur)->str_val;

      CREATE_VERTEX (echo_cmd, op_echo, html_code);
      set_location (echo_cmd, AutoLocation(this));
      next_cur();
      return echo_cmd;
    }
    case tok_at: {
      AutoLocation noerr_location (this);
      next_cur();
      first_node = get_statement();
      CE (first_node.not_null());
      CREATE_VERTEX (noerr, op_noerr, first_node);
      set_location (noerr, noerr_location);
      return noerr;
    }
    case tok_clbrc: {
      return res;
    }
    default:
      res = get_expression();
      if (res.is_null()) {
        if ((*cur)->type() == tok_semicolon) {
          CREATE_VERTEX (empty, op_empty);
          set_location (empty, AutoLocation(this));
          res = empty;
        } else {
          CE (check_statement_end());
          return res;
        }
      } else {
        type_rule = get_type_rule();
        res->type_rule = type_rule;
      }
      CE (check_statement_end());
      //CE (expect (tok_semicolon, "';'"));
      return res;
  }
  kphp_fail();
}

VertexPtr GenTree::get_seq() {
  vector <VertexPtr> seq_next;
  AutoLocation seq_location (this);

  while (cur != end && !test_expect (tok_clbrc)) {
    VertexPtr cur_node = get_statement();
    if (cur_node.is_null()) {
      continue;
    }
    seq_next.push_back (cur_node);
  }
  CREATE_VERTEX (seq, op_seq, seq_next);
  set_location (seq, seq_location);

  return seq;
}

bool GenTree::is_superglobal (const string &s) {
  static set <string> names;
  static bool is_inited = false;
  if (!is_inited) {
    is_inited = true;
    names.insert ("_SERVER");
    names.insert ("_GET");
    names.insert ("_POST");
    names.insert ("_FILES");
    names.insert ("_COOKIE");
    names.insert ("_REQUEST");
    names.insert ("_ENV");
  }
  return names.count (s);
}

bool GenTree::has_return (VertexPtr v) {
  if (v->type() == op_return) {
    return true;
  }

  for (VertexRange i = all (*v); !i.empty(); i.next()) {
    if (has_return (*i)) {
      return true;
    }
  }

  return false;
}

string GenTree::get_memfunc_prefix (const string &name) {
  static map <string, int> name_to_id;
  static const int memcached_id = 1;
  static const int exception_id = 2;
  static bool inited = false;
  if (inited == false) {
    name_to_id["get"             ] = memcached_id;
    name_to_id["connect"         ] = memcached_id;
    name_to_id["pconnect"        ] = memcached_id;
    name_to_id["rpc_connect"     ] = memcached_id;
    name_to_id["set"             ] = memcached_id;
    name_to_id["delete"          ] = memcached_id;
    name_to_id["addServer"       ] = memcached_id;
    name_to_id["add"             ] = memcached_id;
    name_to_id["increment"       ] = memcached_id;
    name_to_id["decrement"       ] = memcached_id;
    name_to_id["replace"         ] = memcached_id;
    name_to_id["getLastQueryTime"] = memcached_id;
    name_to_id["bufferNextLog"   ] = memcached_id;
    name_to_id["flushLogBuffer"  ] = memcached_id;
    name_to_id["clearLogBuffer"  ] = memcached_id;

    name_to_id["getMessage"      ] = exception_id;
    name_to_id["getCode"         ] = exception_id;
    name_to_id["getLine"         ] = exception_id;
    name_to_id["getFile"         ] = exception_id;
    name_to_id["getTrace"        ] = exception_id;
    name_to_id["getTraceAsString"] = exception_id;

    inited = true;
  }

  map <string, int>::iterator it = name_to_id.find (name);
  if (it == name_to_id.end()) {
    return "mf_";
  }

  int res = it->second;
  if (res == memcached_id) {
    return "memcached_";
  } else if (res == exception_id) {
    return "exception_";
  } else {
    kphp_fail();
  }
  return "";
}

VertexPtr GenTree::post_process (VertexPtr root) {
  if (root->type() == op_func_call && (int)root->size() == 1) {
    VertexAdaptor <op_func_call> call = root;
    string str = call->get_string();

    Operation op = op_err;
    if (str == "strval") {
      op = op_conv_string;
    } else if (str == "intval") {
      op = op_conv_int;
    } else if (str == "boolval") {
      op = op_conv_bool;
    } else if (str == "floatval") {
      op = op_conv_float;
    } else if (str == "arrayval") {
      op = op_conv_array;
    } else if (str == "uintval") {
      op = op_conv_uint;
    } else if (str == "longval") {
      op = op_conv_long;
    } else if (str == "ulongval") {
      op = op_conv_ulong;
    }
    if (op != op_err) {
      VertexPtr arg = call->args()[0];
      CREATE_META_VERTEX_1 (new_root, meta_op_base, op, arg);
      ::set_location (new_root, root->get_location());
      return post_process (new_root);
    }
  }

  if (root->type() == op_minus) {
    VertexAdaptor <op_minus> minus = root;
    VertexPtr maybe_num = minus->expr();
    if (maybe_num->type() == op_int_const) {
      VertexAdaptor <op_int_const> num = maybe_num;
      num->str_val = "-" + num->str_val;
      minus->expr() = VertexPtr();
      return post_process (num);
    }
  }

  if (root->type() == op_set) {
    VertexAdaptor <op_set> set_op = root;
    if (set_op->lhs()->type() == op_list_ce) {
      vector <VertexPtr> next;
      next = set_op->lhs()->get_next();
      next.push_back (set_op->rhs());
      CREATE_VERTEX (list, op_list, next);
      ::set_location (list, root->get_location());
      return post_process (list);
    }
  }

  if (root->type() == op_define || root->type() == op_define_raw) {
    VertexAdaptor <meta_op_define> define = root;
    VertexPtr name = define->name();
    if (name->type() == op_func_name) {
      CREATE_VERTEX (new_name, op_string);
      new_name->str_val = name.as <op_func_name>()->str_val;
      ::set_location (new_name, name->get_location());
      define->name() = new_name;
    }
  }

  if (root->type() == op_function && root->get_string() == "requireOnce") {
    CREATE_VERTEX (empty, op_empty);
    return post_process (empty);
  }

  if (root->type() == op_func_call && root->get_string() == "call_user_func_array") {
    VertexRange args = root.as <op_func_call>()->args();
    kphp_error ((int)args.size() == 2, dl_pstr ("Call_user_func_array expected 2 arguments, got %d", (int)root->size()));
    kphp_error (args[0]->type() == op_string, "First argument of call_user_func_array must be a const string");
    CREATE_VERTEX (arg, op_varg, args[1]);
    ::set_location (arg, args[1]->get_location());
    CREATE_VERTEX (new_root, op_func_call, arg);
    ::set_location (new_root, arg->get_location());
    new_root->str_val = args[0].as <op_string>()->str_val;
    return post_process (new_root);
  }

  FOREACH_VERTEX (root, i) {
    *i = post_process (*i);
  }

  if (root->type() == op_var) {
    if (is_superglobal (root->get_string())) {
      root->extra_type = op_ex_var_superglobal;
    }
  }

  if (root->type() == op_arrow) {
    VertexAdaptor <op_arrow> arrow = root;
    VertexPtr rhs = arrow->rhs();

    VertexAdaptor <op_func_call> func;
    if (rhs->type() == op_func_call) {
      func = rhs;
    } else if (rhs->type() == op_func_name) {
      CREATE_VERTEX (new_func, op_func_call);
      VertexAdaptor <op_func_name> func_ptr = rhs;
      ::set_location (new_func, func_ptr->get_location());
      new_func->str_val = "get" + func_ptr->str_val;
      arrow->rhs() = new_func;
      func = new_func;
    }
    kphp_error (func->type() == op_func_call, "Operator '->' expects function call as its right operand");

    string prefix = get_memfunc_prefix (func->str_val);
    if (prefix.empty()) {
      kphp_error (0, dl_pstr ("Unknown member functions [%s->%s]",
                             arrow->lhs()->type() == op_var ? arrow->lhs().as <op_var>()->str_val.c_str() : "unknown",
                             func->str_val.c_str()));
    } else {
      vector <VertexPtr> new_next;
      new_next.push_back (arrow->lhs());

      const vector <VertexPtr> &old_next = func->get_next();
      new_next.insert (new_next.end(), old_next.begin(), old_next.end());

      CREATE_VERTEX (new_root, op_func_call, new_next);
      ::set_location (new_root, root->get_location());

      string new_func_name = prefix + func->str_val;
      new_root->str_val = new_func_name;

      root = new_root;
    }
  }

  return root;
}

VertexPtr GenTree::run() {
  VertexPtr res = get_statement();
  kphp_assert (res.is_null());
  if (cur != end) {
    fprintf (stderr, "line %d: something wrong\n", line_num);
    kphp_error (0, "Cannot compile (probably problems with brace balance)");
  }

  return res;
}

void GenTree::for_each (VertexPtr root, void (*callback) (VertexPtr )) {
  callback (root);

  FOREACH_VERTEX (root, i) {
    for_each (*i, callback);
  }
}

void gen_tree_init() {
  GenTree::is_superglobal("");
  GenTree::get_memfunc_prefix ("");
}

void php_gen_tree (vector <Token *> *tokens, const string &main_func_name, GenTreeCallbackBase *callback) {
  GenTree gen;
  gen.init (tokens, callback);
  gen.run();
}

#undef CE
