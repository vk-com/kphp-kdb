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

#ifndef __TL_SCHEME_H__
#define __TL_SCHEME_H__

/* Programming language SCHEME */

#include <stdio.h>

extern struct tl_scheme_object obj_empty_list;
extern struct tl_scheme_object obj_open_square_bracket;

enum tl_scheme_object_type {
  tlso_int,
  tlso_long,
  tlso_double,
  tlso_str,
  tlso_function,
  tlso_list,
  tlso_open_round_bracket,
  tlso_open_square_bracket
};

struct tl_scheme_object {
  enum tl_scheme_object_type type;
  union {
    int i;
    long long l;
    double d;
    char *s;
    struct { struct tl_scheme_object *car, *cdr; } p;
  } u;
};

const char *tl_strtype (struct tl_scheme_object *O);
int tl_scheme_int_value (struct tl_scheme_object *O, int *value);
int tl_scheme_long_value (struct tl_scheme_object *O, long long *value);
int tl_scheme_double_value (struct tl_scheme_object *O, double *value);
int tl_scheme_string_value (struct tl_scheme_object *O, char **value);
int tl_scheme_length (struct tl_scheme_object *A);
struct tl_scheme_object *tl_scheme_reverse (struct tl_scheme_object *A) __attribute((warn_unused_result));
struct tl_scheme_object *tl_scheme_cons (struct tl_scheme_object *A, struct tl_scheme_object *B) __attribute((warn_unused_result));

struct tl_scheme_object *tl_scheme_object_new (enum tl_scheme_object_type type);
void tl_scheme_object_free (struct tl_scheme_object *A);
void tl_scheme_object_sbdump (struct tl_buffer *b, struct tl_scheme_object *O);
void tl_scheme_object_sbdump_indent (struct tl_buffer *b, struct tl_scheme_object *O, int depth);
void tl_scheme_object_dump (FILE *f, struct tl_scheme_object *O);
struct tl_scheme_object *tl_scheme_parse_file (struct tl_compiler *C, const char *const filename);
struct tl_scheme_object *tl_scheme_parse (struct tl_compiler *C, const char *const text);
void tl_set_expand_utf8 (int i);

#endif

