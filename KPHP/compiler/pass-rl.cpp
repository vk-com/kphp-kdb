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

#include "pass-rl.h"

#include "stage.h"
#include "data.h"

void rl_none_calc (VertexPtr root, int except) {
  int ii = 0;
  FOREACH_VERTEX (root, i) {
    if (ii != except) {
      rl_calc (*i, val_none);
    } else {
      rl_calc (*i, val_r);
    }
    ii++;
  }
}

void rl_r_calc (VertexPtr root, int except) {
  int ii = 0;
  FOREACH_VERTEX (root, i) {
    if (ii != except) {
      rl_calc (*i, val_r);
    } else {
      rl_calc (*i, val_l);
    }
    ii++;
  }
}

void rl_l_calc (VertexPtr root, int except) {
  int ii = 0;
  FOREACH_VERTEX (root, i) {
    if (ii != except) {
      rl_calc (*i, val_l);
    } else {
      rl_calc (*i, val_r);
    }
    ii++;
  }
}

void rl_func_call_calc (VertexPtr root, RLValueType expected_rl_type) {
  kphp_error (expected_rl_type != val_l, "Function result cannot be used as lvalue");
  switch (root->type()) {
    case op_list:
      rl_l_calc (root, (int)root->size() - 1);
      return;
    case op_seq_comma:
      rl_none_calc (root, (int)root->size() - 1);
      return;
    case op_array: //TODO: in fact it is wrong
    case op_print:
    case op_pack:
    case op_printf:
    case op_sprintf:
    case op_store_many:
    case op_min:
    case op_max:
    case op_defined:
    case op_require:
      rl_r_calc (root, -1);
      return;
    case op_func_call:
      break;
    default:
      kphp_fail();
      break;
  }
  FunctionPtr f = root->get_func_id();
  if (f->varg_flag) {
    rl_r_calc (root, -1);
    return;
  }
  VertexRange params = f->root.as <meta_op_function>()->params().
                               as <op_func_param_list>()->params();
  assert (root->size() <= (int)params.size());

  FOREACH_VERTEX (root, i) {
    if ((*params)->type() == op_func_param_callback) {
    } else {
      VertexAdaptor <op_func_param> param = *params;
      RLValueType tp = param->var()->ref_flag ? val_l : val_r;
      rl_calc (*i, tp);
    }

    params.next();
  }
}

void rl_other_calc (VertexPtr root, RLValueType expected_rl_type) {
  switch (root->type()) {
    case op_conv_array_l:
    case op_conv_int_l:
    case op_new:
      rl_l_calc (root, -1);
      break;
    case op_noerr:
      rl_calc (root.as <op_noerr>()->expr(), expected_rl_type);
      break;
    default:
      assert ("Unknown operation in rl_other_calc" && 0);
      break;
  }
}

void rl_common_calc (VertexPtr root, RLValueType expected_rl_type) {
  kphp_assert (expected_rl_type == val_none);
  switch (root->type()) {
    case op_if:
    case op_do:
    case op_while:
    case op_switch:
    case op_case:
      rl_none_calc (root, 0);
      break;
    case op_require:
    case op_require_once:
    case op_return:
    case op_break:
    case op_continue:
    case op_dbg_echo:
    case op_echo:
    case op_throw:
    case op_var_dump:
      rl_r_calc (root, -1);
      break;
    case op_unset: //TODO: fix it (???)
      rl_l_calc (root, -1);
      break;
    case op_try:
    case op_seq:
    case op_foreach:
    case op_default:
      rl_none_calc (root, -1);
      break;
    case op_for:
      //TODO: it may be untrue
      rl_none_calc (root, 1);
      break;
    case op_global:
    case op_static:
    case op_empty:
    case op_func_decl:
    case op_extern_func:
      break;
    case op_foreach_param:
      if (root.as <op_foreach_param>()->x()->ref_flag) {
        rl_l_calc (root, -1);
      } else {
        rl_l_calc (root, 0);
      }
      break;
    case op_function:
      rl_calc (root.as <op_function>()->cmd(), val_none);
      break;

    default:
      kphp_fail();
      break;
  }
  return;
}

void rl_calc (VertexPtr root, RLValueType expected_rl_type) {
  stage::set_location (root->get_location());

  root->rl_type = expected_rl_type;

  Operation tp = root->type();

  //fprintf (stderr, "rl_calc (%p = %s)\n", root, OpInfo::str (tp).c_str());

  switch (OpInfo::rl (tp)) {
    case rl_set:
      switch (expected_rl_type) {
        case val_r:
        case val_none: {
          VertexAdaptor <meta_op_binary_op> set_op = root;
          rl_calc (set_op->lhs(), val_l);
          rl_calc (set_op->rhs(), val_r);
          break;
        }
        case val_l:
          kphp_error (0, dl_pstr ("trying to use result of [%s] as lvalue", OpInfo::str (tp).c_str()));
          break;
        default:
          kphp_fail();
          break;
      }
      break;
    case rl_index: {
      VertexAdaptor <op_index> index = root;
      VertexPtr array = index->array();
      switch (expected_rl_type) {
        case val_l:
          rl_calc (array, val_l);
          if (index->has_key()) {
            rl_calc (index->key(), val_r);
          }
          break;
        case val_r:
        case val_none:
          kphp_error (array->type() == op_var || array->type() == op_index || array->type() == op_func_call,
              "op_index has to be used on lvalue");
          rl_calc (array, val_r);

          if (index->has_key()) {
            rl_calc (index->key(), val_r);
          }
          break;
        default:
          assert (0);
          break;
      }
      break;
    }
    case rl_op:
      switch (expected_rl_type) {
        case val_l:
          kphp_error (0, "Can't make result of operation to be lvalue");
          break;
        case val_r:
        case val_none:
          rl_r_calc (root, -1);
          break;
        default:
          assert (0);
          break;
      }
      break;

    case rl_op_l:
      switch (expected_rl_type) {
        case val_l:
          kphp_error (0, "Can't make result of operation to be lvalue");
          break;
        case val_r:
        case val_none:
          rl_calc (root.as <meta_op_unary_op>()->expr(), val_l);
          break;
        default:
          kphp_fail();
          break;
      }
      break;

    case rl_const:
      switch (expected_rl_type) {
        case val_l:
          kphp_error (0, "Can't make const to be lvalue");
          break;
        case val_r:
        case val_none:
          break;
        default:
          kphp_fail();
          break;
      }
      break;
    case rl_var:
      switch (expected_rl_type) {
        case val_l:
          kphp_error (root->extra_type != op_ex_var_const, "Can't make const to be lvalue");
          break;
        case val_r:
        case val_none:
          break;
        default:
          kphp_fail();
          break;
      }
      break;
    case rl_other:
      rl_other_calc (root, expected_rl_type);
      break;
    case rl_func:
      rl_func_call_calc (root, expected_rl_type);
      break;
    case rl_mem_func:
      rl_r_calc (root, -1);
      break;
    case rl_common:
      rl_common_calc (root, expected_rl_type);
      break;

    default:
      kphp_fail();
      break;
  }
}
