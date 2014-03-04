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

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <assert.h>
#include "icpl-data.h"
#include "kdb-data-common.h"
#include "server-functions.h"

int combinators;

static icpl_nat_t parse_nat (const char *input, int l) {
  icpl_nat_t u = 0;
  int i;
  for (i = 0; i < l; i++) {
    assert (isdigit (input[i]));
    u = u * 10 + (input[i] - 48);
  }
  return u;
}

icpl_combinator_t *COMBINATORS[COMBINATORS_PRIME];

icpl_combinator_t *get_combinator_f (const char *name, int length, int force) {
  unsigned char h1 = 0, h2 = 0;
  int i;
  for (i = 0; i < length; i++) {
    h1 = 239 * h1 + name[i];
    h2 = 3 * h2 + name[i];
  }
  h1 %= COMBINATORS_PRIME;
  h2 = 1 + (h2 % (COMBINATORS_PRIME - 1));
  icpl_combinator_t *D;
  while ((D = COMBINATORS[h1])) {
    if (D->name_length == length && !memcmp (name, D->name, length)) {
      return D;
    }
    if ( (h1 += h2) >= COMBINATORS_PRIME) {
      h1 -= COMBINATORS_PRIME;
    }
  }
  if (!force) {
    return NULL;
  }
  combinators++;
  assert (combinators < (0.8 * COMBINATORS_PRIME));
  icpl_combinator_t *C = zmalloc0 (sizeof (*C));
  C->name = zmalloc (length);
  memcpy (C->name, name, length);
  C->name_length = length;
  C->arity = -1;
  return COMBINATORS[h1] = C;
}

int icpl_failf (const char *input, int pos, const char *format, ...) __attribute__ ((format (printf, 3, 4)));
int icpl_failf (const char *input, int pos, const char *format, ...) {
  const int old_errno = errno;
  int line = 0, column = 0;
  const char *cur = input + pos, *line_start = input, *s;
  for (s = input; s != cur; s++) {
    if (*s == '\n') {
      line++;
      line_start = s;
      column = 0;
    } else {
      column++;
    }
  }
  fprintf (stderr, "[line:%d, column:%d] ", line, column);
  va_list ap;
  va_start (ap, format);
  vfprintf (stderr, format, ap);
  va_end (ap);
  fputc ('\n', stderr);
  int i;
  s = line_start;
  for (i = 0, s = line_start; i < column; i++, s++) {
    fputc (*s, stderr);
  }
  if (column) {
    fputc ('\n', stderr);
    for (i = 0; i < column; i++) {
      fputc (' ', stderr);
    }
  }
  while (*s && *s != '\n') {
    fputc (*s, stderr);
    s++;
  }
  fputc ('\n', stderr);
  errno = old_errno;
  return -1;
}

/******************** cell ********************/
static icpl_cell_t *cell_new_node (icpl_cell_t *left, icpl_cell_t *right)  __attribute__ ((warn_unused_result));
static icpl_cell_t *cell_new_node (icpl_cell_t *left, icpl_cell_t *right) {
  if (left == NULL) {
    return right;
  }
  icpl_cell_t *c = zmalloc0 (sizeof (*c));
  c->type = ct_node;
  c->u.p.left = left;
  c->u.p.right = right;
  return c;
}

static icpl_cell_t *cell_new_combinator (const char *name, int length)  __attribute__ ((warn_unused_result));
static icpl_cell_t *cell_new_combinator (const char *name, int length) {
  icpl_cell_t *c = zmalloc0 (sizeof (*c));
  c->type = ct_combinator;
  c->u.C = get_combinator_f (name, length, 1);
  return c;
}

static icpl_cell_t *cell_new_nat (const char *input, int l)  __attribute__ ((warn_unused_result));
static icpl_cell_t *cell_new_nat (const char *input, int l) {
  icpl_cell_t *c = zmalloc0 (sizeof (*c));
  c->type = ct_nat;
  c->u.i = parse_nat (input, l);
  return c;
}

static icpl_cell_t *cell_new_variable (char variable_name)  __attribute__ ((warn_unused_result));
static icpl_cell_t *cell_new_variable (char variable_name) {
  icpl_cell_t *c = zmalloc0 (sizeof (*c));
  c->type = ct_variable;
  c->u.variable_name = variable_name;
  return c;
}

static icpl_cell_t *cell_new_any (void)  __attribute__ ((warn_unused_result));
static icpl_cell_t *cell_new_any (void) {
  icpl_cell_t *c = zmalloc0 (sizeof (*c));
  c->type = ct_any;
  return c;
}

/*
static icpl_cell_t *cell_new_from_token (const char *input, icpl_token_t *T)  __attribute__ ((warn_unused_result));
static icpl_cell_t *cell_new_from_token (const char *input, icpl_token_t *T) {
  switch (T->type) {
    case lt_nat: return cell_new_nat (input + T->start, T->length);
    case lt_variable: return cell_new_variable (input[T->start]);
    case lt_any: return cell_any ();
    case lt_combinator: return cell_new_combinator (input + T->start, T->length);
    default: assert (0);
  }
}
*/

/******************** lex ********************/
#define TOKEN(x) { T->start = start; T->length = k - start + 1; T->type = x; return 0; }
static int next_token (const char *input, int start, icpl_token_t *T) {
  while (input[start] && isspace (input[start])) {
    start++;
  }
  int k = start;
  switch (input[k]) {
    case   0: TOKEN(lt_eof);
    case '(': TOKEN(lt_obrace);
    case ')': TOKEN(lt_cbrace);
    case ';': TOKEN(lt_statement_end);
    case 'a' ... 'z':
      if (isalnum (input[k+1])) {
        return icpl_failf (input, k+1, "variable should be single latin letter");
      }
      TOKEN(lt_variable);
    case '_':
      if (isalnum (input[k+1])) {
        return icpl_failf (input, k+1, "alnum character right after underscore is forbidden");
      }
      TOKEN(lt_any);
    case '/':
      if (isdigit(input[k+1])) {
        TOKEN(lt_arity_division);
      }
      TOKEN(lt_combinator);
    break;
    case '=':
      if (input[k+1] == '=') {
        k++;
        TOKEN(lt_combinator);
      }
      TOKEN(lt_equal);
    break;
    case '+':
    case '-':
    case '*':
      TOKEN(lt_combinator);
    case '<':
    case '>':
      if (input[k+1] == '=') {
        k++;
      }
      TOKEN(lt_combinator);
    case '!':
      if (input[k+1] == '=') {
        k++;
        TOKEN(lt_combinator);
      }
      return icpl_failf (input, k, "expected '=' after '!'");
    case 'A' ... 'Z':
      while (isalnum (input[k]) || input[k] == '_') {
        k++;
      }
      k--;
      TOKEN(lt_combinator);
    case '0' ... '9':
      while (isdigit (input[k])) {
        k++;
      }
      k--;
      TOKEN(lt_nat);
  }
  return icpl_failf (input, k, "???");
}
#undef TOKEN

#define MAX_STATEMENT_TOKEN 65536

static int icpl_lex_next_token (const char *input, int *pos, icpl_token_t *T) {
  int r = next_token (input, *pos, T);
  if (r < 0) {
    return r;
  }
  vkprintf (4, "token: %.*s, type: %d\n", T->length, input + T->start, T->type);
  *pos = T->start + T->length;
  return r;
}

static int icpl_cell_arity (icpl_cell_t *R) {
  if (R->type != ct_node) {
    return 0;
  }
  return 1 + icpl_cell_arity (R->u.p.left);
}

static int parse_expr (const char *input, icpl_token_t *a, int tokens, icpl_cell_t **R) {
  int i, j, s;
  icpl_cell_t *c, *r = NULL;
  *R = NULL;
  for (i = 0; i < tokens; i++) {
     switch (a[i].type) {
       case lt_obrace:
         s = 1;
         for (j = i + 1; s > 0 && j < tokens; j++) {
           switch (a[j].type) {
             case lt_obrace: s++; break;
             case lt_cbrace: s--; break;
             default: break;
           }
         }
         if (s > 0) {
           return icpl_failf (input, a[i].start, "closing bracket wasn't found");
         }
         //a[i]   : '('
         //a[j-1] : ')'
         if (parse_expr (input, a + (i + 1), j - (i + 2), &c) < 0) {
           return -1;
         }
         r = cell_new_node (r, c);
         i = j;
       break;
       case lt_cbrace: return icpl_failf (input, a[i].start, "unexpected closing bracket");
       case lt_combinator: r = cell_new_node (r, cell_new_combinator (input + a[i].start, a[i].length)); break;
       case lt_nat: r = cell_new_node (r, cell_new_nat (input + a[i].start, a[i].length)); break;
       case lt_variable: r = cell_new_node (r, cell_new_variable (input[a[i].start])); break;
       case lt_any: r = cell_new_node (r, cell_new_any ()); break;
       default: fprintf (stderr, "Unexpected lt (%d) at pos %d\n", a[i].type, i); assert (0);
     }
  }
  *R = r;
  return 0;
}

int icpl_parse (char *input) {
  int i, k = 0;
  icpl_token_t T;
  for (;;) {
    if (icpl_lex_next_token (input, &k, &T) < 0) {
      return -1;
    }
    if (T.type == lt_eof) {
      break;
    }
    if (T.type != lt_combinator) {
      return icpl_failf (input, T.start, "expected combinator, but '%.*s' found", T.length, input + T.start);
    }
    icpl_combinator_t *C = get_combinator_f (input + T.start, T.length, 1);

    static icpl_token_t a[MAX_STATEMENT_TOKEN];
    int n;
    for (n = 0; ; n++) {
      if (n >= MAX_STATEMENT_TOKEN) {
        return icpl_failf (input, a[n-1].start, "too many tokens in statement");
      }
      if (icpl_lex_next_token (input, &k, &a[n]) < 0) {
        return -1;
      }
      if (a[n].type == lt_statement_end) {
        break;
      } else if (a[n].type == lt_eof) {
        return icpl_failf (input, T.start, "expected semicolon, but EOF found");
      }
    }

    if (a[0].type == lt_arity_division) {
      if (n != 2 || a[1].type != lt_nat) {
        return icpl_failf (input, a[0].start + a[0].length, "illegal arity declaration");
      }
      icpl_nat_t x = parse_nat (input + a[1].start, a[1].length);
      if (C->arity >= 0 && C->arity != x) {
        return icpl_failf (input, a[0].start, "duplicate arity declaration, old arity is %d", C->arity);
      }
      if (x >= 1000) {
        return icpl_failf (input, a[0].start, "arity is too big");
      }
      C->arity = x;
      continue;
    }

    int e = -1;
    for (i = 0; i < n; i++) {
      switch (a[i].type) {
        case lt_arity_division: return icpl_failf (input, a[i].start, "unexpected arity division token");
        case lt_eof: return icpl_failf (input, a[i].start, "expected semicolon, but EOF found");
        case lt_equal:
          if (e >= 0) {
            return icpl_failf (input, a[i].start, "two '=' tokens in statement");
          }
          e = i;
        break;
        default: break;
      }
    }

    if (e < 0) {
      /* eval */
      for (i = 0; i < n; i++) {
        switch (a[i].type) {
          case lt_variable: return icpl_failf (input, a[i].start, "unexpected variable in evaluated statement");
          case lt_any: return icpl_failf (input, a[i].start, "unexpected undescore in evaluated statement");
          default: break;
        }
      }
      icpl_cell_t *r;
      if (parse_expr (input, a, n, &r) < 0) {
        return -1;
      }
    } else {
      /* add pattern */
      icpl_pattern_t *p = zmalloc0 (sizeof (*p));
      if (parse_expr (input, a, e, &p->left) < 0) {
        return -1;
      }
      if (parse_expr (input, a + e + 1, n - (e + 1), &p->right) < 0) {
        return -1;
      }
      if (C->first != NULL) {
        C->last->next = p;
        C->last = p;
      } else {
        C->first = C->last = p;
      }
      int pattern_arity = icpl_cell_arity (p->left);
      if (C->arity < 0) {
        C->arity = pattern_arity;
      } else {
        if (C->arity != pattern_arity) {
          icpl_failf (input, T.start, "%s arity is %d, pattern's arity is %d", C->name, C->arity, pattern_arity);
        }
      }
    }
  }
  return 0;
}

/* builtin */
void icpl_parse_builtins (void) {
  if (icpl_parse ("True/0; False/0; + /2; - /2; * /2; / /2; == /2; < /2; > /2; <= /2; != /2; >= /2;") < 0) {
    exit (1);
  }
}

void icpl_init (void) {
  icpl_parse_builtins ();
}
