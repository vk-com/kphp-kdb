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

#ifndef __ICPL_DATA_H__
#define __ICPL_DATA_H__

#define COMBINATORS_PRIME 10007

typedef unsigned int icpl_nat_t;

typedef struct icpl_pattern icpl_pattern_t;

typedef struct {
  char *name;
  int name_length;
  int arity;
  icpl_pattern_t *first, *last;
} icpl_combinator_t;

typedef enum {
  lt_combinator = 0,     /* '[A-Z][A-Za-z_]*' or builtin combinator */
  lt_nat = 1,            /* [0-9]+ */
  lt_obrace  = 2,        /* '(' */
  lt_cbrace = 3,         /* ')' */
  lt_statement_end = 4,  /* ';' */
  lt_variable = 5,       /* '[a-z]' */
  lt_any = 6,            /* '_' */
  lt_arity_division = 7, /* '/' */
  lt_equal = 8,          /* '=' */
  lt_eof = 9
} icpl_lex_type_t;

typedef struct {
  int start;
  int length;
  icpl_lex_type_t type;
} icpl_token_t;

typedef enum {
  ct_node,
  ct_nat,
  ct_variable,
  ct_combinator,
  ct_any
} icpl_cell_type_t;

typedef struct icpl_cell {
  union {
    struct { struct icpl_cell *left, *right; } p;
    icpl_nat_t i;
    icpl_combinator_t *C;
    char variable_name;
  } u;
  int type;
} icpl_cell_t;


/* special combinators:
   #INT_VALUE#
   #_#
   #a#
   #b#
   #z#
*/

struct icpl_pattern {
  icpl_combinator_t *c;
  icpl_cell_t *left;
  icpl_cell_t *right;
  icpl_pattern_t *next;
};

void icpl_init (void);
int icpl_parse (char *input);

#endif
