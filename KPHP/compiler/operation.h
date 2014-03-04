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
#include "utils.h"

#include "token.h"

typedef enum {
  op_err = 0,
#define FOREACH_OP(op) op,
#include "foreach_op.h"
  Operation_size
} Operation;

typedef enum {
  op_ex_none = 0,
  op_ex_func_switch,
  op_ex_func_global,
  op_ex_func_member,
  op_ex_func_new,
  op_ex_var_const,
  op_ex_var_superglobal,
  op_ex_var_superlocal,
  op_ex_var_this,
  op_ex_rule_const,
  op_ex_internal_func,
  op_ex_index_rval,
  op_ex_safe_version
} OperationExtra;

typedef enum {error_op = 0, common_op, cycle_op, binary_op, binary_func_op, prefix_op, postfix_op, ternary_op, conv_op} OperationType;
typedef enum {rl_error = 0, rl_set, rl_index, rl_op, rl_common, rl_const, rl_var, rl_func, rl_mem_func, rl_op_l, rl_other} RLOperationType;
typedef enum {val_error = 0, val_r, val_l, val_none} RLValueType;
typedef enum {cnst_error = -1, cnst_not_func, cnst_nonconst_func, cnst_const_func, cnst_func} ConstOperationType;
typedef enum {cnst_error_ = 0, cnst_not_val, cnst_nonconst_val, cnst_const_val} ConstValueType;

const int no_property = 0;
typedef enum {
  omain_none = no_property,
  statement_opp,
  expression_opp
} opp_main_t;

typedef enum {
  oarity_none = no_property,
  unary_opp,
  binary_opp,
  ternary_opp
} opp_arity_t;

typedef enum {
  ofixity_none = no_property,
  right_opp,
  left_opp
} opp_fixity_t;

typedef enum {
  oxfix_none = no_property,
  prefix_opp,
  postfix_opp,
  funcfix_opp
} opp_xfix_t;

typedef enum {
  omath_none = no_property,
  logic_opp,
  compare_opp,
  bitwise_opp,
  arithmetic_opp
} opp_math_t;

typedef enum {
  oextra_none = no_property,
  set_opp,
  conv_opp
} opp_extra_t;


typedef enum {
  ominor_none = no_property,
  cycle_opp,
  term_opp,
  operator_opp
} opp_minor_t;

struct OpProperties {
  Operation op;
  Operation base_op; // op_add for op_set_add

  int priority;
  opp_main_t main;
  opp_arity_t arity;
  opp_xfix_t xfix;
  opp_fixity_t fixity;
  opp_math_t math;
  opp_minor_t minor;
  opp_extra_t extra;

  RLOperationType rl;
  ConstOperationType cnst;
  OperationType type;

  string description;
  string str;
};

struct OpInfo {
  static int was_init_static;
  //TODO: assert that 255 is enough
  static Operation tok_to_op[255];
  static Operation tok_to_binary_op[255];
  static Operation tok_to_unary_op[255];
  static OpProperties P[Operation_size];

  static int bin_op_begin, bin_op_end;
  static int ternaryP;

  static inline void add_binary_op (int priority, TokenType tok, Operation op);
  static inline void add_unary_op (int priority, TokenType tok, Operation op);
  static inline void add_op (TokenType tok, Operation op);
  static void init_static();

private:
  static inline OpProperties &get_properties (Operation op) {
    return P[op];
  }
  static inline void set_priority (Operation op, int priority) {
    get_properties (op).priority = priority;
  }

public:
  static inline const OpProperties &properties (Operation op) {
    return get_properties (op);
  }
  static inline RLOperationType rl (Operation op) {
    return properties (op).rl;
  }
  static inline ConstOperationType cnst (Operation op) {
    return properties (op).cnst;
  }
  static inline OperationType type (Operation op) {
    return properties (op).type;
  }
  static inline const string &str (Operation op) {
    return properties (op).str;
  }
  static inline Operation base_op (Operation op) {
    return properties (op).base_op;
  }
  static inline int priority (Operation op) {
    return properties (op).priority;
  }
  static inline opp_fixity_t fixity (Operation op) {
    return properties (op).fixity;
  }
  static inline opp_arity_t arity (Operation op) {
    return properties (op).arity;
  }
};

