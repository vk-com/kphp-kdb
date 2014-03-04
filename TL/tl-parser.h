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
              2012-2013 Anton Maydell
*/

#ifndef __TL_PARSER_H__
#define __TL_PARSER_H__

#include <stdio.h>
#include "tl-utils.h"

struct tl_token {
  char *text;
  struct tl_token *next;
};

enum tl_expression_type {
  tlet_simple = 0,
  tlet_polymorphic = 1,
  tlet_polymorphic_instance = 2,
};

struct tl_expression {
  enum tl_expression_type type;
  char *text;
  struct tl_expression *next, *prev;
  struct tl_token *left, *right;
  char *right_name; /* contains right side with sugar: Vector<int> */
  struct tl_expression *rnext, *rtail; /* used in tl_expression_find_next_type */
  int section;
  unsigned magic;
  int flag_builtin:1;
  int flag_expanded:1;
  int flag_visited:1;
};

#define TL_MAX_ERRORS 16

#define TL_SECTION_TYPES 0
#define TL_SECTION_FUNCTIONS 1

#define TL_COMPILER_INITIALIZED_MAGIC 0xcc8b35ae

struct tl_compiler {
  struct tl_expression expr[2];
  struct tl_hashmap *hm_magic[2];
  struct tl_hashmap *hm_combinator[2];
  struct tl_hashmap *hm_composite_typename;

  /* serialization */
  struct tl_expression *serialized_first_function_expr;

  struct tl_buffer tmp_error_buff;
  char *error_msg[TL_MAX_ERRORS];
  int magic;
  int errors;

  int flag_reading_builtin_schema:1;
/* set this flag for (id (1 2 3 10)) unserialize output,
    default output: id:(1 2 3 10)
 */
  int flag_unserialize_strict_lisp:1;
  int flag_output_magic:1;
  int flag_code_vector:1;
};

const char *tl_get_section_name (int section);

struct tl_expression *tl_list_expressions_find_by_magic (struct tl_expression *L, int magic);
struct tl_expression *tl_expression_find_by_magic (struct tl_compiler *C, int magic);

void tl_compiler_clear_errors (struct tl_compiler *C);
void tl_compiler_add_error (struct tl_compiler *C);
void tl_compiler_print_errors (struct tl_compiler *C, FILE *f);

int tl_success (struct tl_compiler *C, int old_errors);
int tl_failf (struct tl_compiler *C, const char *format, ...) __attribute__ ((format (printf, 2, 3)));
int tl_failfp (struct tl_compiler *C, const char *input, const char *cur, const char *format, ...) __attribute__ ((format (printf, 4, 5)));

struct tl_expression *tl_list_expressions_find_by_combinator_name (struct tl_compiler *C, int section, char *const combinator_name, const char *type_name);
struct tl_expression *tl_expression_find_first_by_composite_typename (struct tl_compiler *C, char *composite_typename);
char *tl_expression_get_vector_item_type (struct tl_expression *E);

char *tl_readfile (struct tl_compiler *C, const char *const filename) __attribute((warn_unused_result));

int tl_schema_read (struct tl_compiler *C, const char *input);
int tl_schema_read_from_file (struct tl_compiler *C, const char *const filename);
void tl_compiler_free (struct tl_compiler *C);
int tl_write_expressions_to_file (struct tl_compiler *C, const char *const filename);
int tl_function_help (struct tl_compiler *C, char *rpc_function_name, FILE *f);
int tl_schema_print_unused_types (struct tl_compiler *C, FILE *f);

char *tl_expression_get_argument_type (struct tl_expression *E, char *arg_name);
#endif

