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
#include "utils.h"

enum TokenType {
  tok_empty,
  tok_int_const,
  tok_float_const,
  tok_null,
  tok_inline_html,
  tok_str,
  tok_str_begin,
  tok_str_end,
  tok_expr_begin,
  tok_expr_end,
  tok_var_name,
  tok_func_name,
  tok_while,
  tok_for,
  tok_foreach,
  tok_if,
  tok_else,
  tok_elseif,
  tok_break,
  tok_continue,
  tok_echo,
  tok_dbg_echo,
  tok_var_dump,
  tok_function,
  tok_ex_function,
  tok_auto,
  tok_min,
  tok_max,
  tok_varg,
  tok_array,
  tok_as,
  tok_case,
  tok_switch,
  tok_class,
  tok_const,
  tok_default,
  tok_do,
  tok_eval,
  tok_exit,
  tok_return,
  tok_list,
  tok_include,
  tok_include_once,
  tok_require,
  tok_require_once,
  tok_print,
  tok_unset,
  tok_var,
  tok_global,
  tok_static,
  tok_goto,
  tok_isset,
  tok_pack,
  tok_printf,
  tok_sprintf,
  tok_store_many,
  tok_eq1,
  tok_eq2,
  tok_eq3,
  tok_lt,
  tok_gt,
  tok_le,
  tok_ge,
  tok_neq2,
  tok_neq3,
  tok_neq_lg,
  tok_oppar,
  tok_clpar,
  tok_opbrc,
  tok_clbrc,
  tok_opbrk,
  tok_clbrk,
  tok_semicolon,
  tok_comma,
  tok_dot,
  tok_colon,

  tok_at,

  tok_inc,
  tok_dec,
  tok_plus,
  tok_minus,
  tok_times,
  tok_divide,
  tok_mod,
  tok_and,
  tok_or,
  tok_xor,
  tok_not,
  tok_log_not,
  tok_question,

  tok_leq,
  tok_shl,
  tok_geq,
  tok_shr,
  tok_neq,
  tok_set_add,
  tok_set_sub,
  tok_set_mul,
  tok_set_div,
  tok_set_mod,
  tok_set_and,
  tok_log_and,
  tok_log_and_let,
  tok_set_or,
  tok_log_or,
  tok_log_or_let,
  tok_set_xor,
  tok_log_xor,
  tok_log_xor_let,
  tok_set_shr,
  tok_set_shl,
  tok_set_dot,
  tok_double_arrow,
  tok_double_colon,
  tok_arrow,

  tok_class_c,
  tok_file_c,
  tok_func_c,
  tok_dir_c,
  tok_line_c,
  tok_method_c,

  tok_int,
  tok_float,
  tok_string,
  tok_object,
  tok_bool,

  tok_conv_int,
  tok_conv_float,
  tok_conv_string,
  tok_conv_array,
  tok_conv_object,
  tok_conv_bool,
  tok_conv_var,
  tok_conv_uint,
  tok_conv_long,
  tok_conv_ulong,

  tok_false,
  tok_true,

  tok_define,
  tok_defined,
  tok_define_raw,

  tok_triple_colon,
  tok_triple_gt,
  tok_triple_lt,
  tok_triple_eq,

  tok_throw,
  tok_new,
  tok_Exception,

  tok_try,
  tok_catch,
  tok_throws,

  tok_break_file,

  tok_end
};

struct Token {
  TokenType type_;
  string_ref str_val;
  string_ref debug_str;

  int line_num;

  explicit Token (TokenType type);
  Token (TokenType type, const string_ref &s);
  Token (TokenType type, const char *s, const char *t);

  inline TokenType &type() {return type_;}

  string to_str();
};
