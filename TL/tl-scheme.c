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

#define	_FILE_OFFSET_BITS	64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <assert.h>
#include "tl-parser.h"
#include "tl-scheme.h"
#include "tl-utils.h"
#include "kdb-data-common.h"

struct tl_scheme_object obj_empty_list = {
  .type = tlso_list,
  .u = { .p = { .car = NULL, .cdr = NULL } }
};

static struct tl_scheme_object obj_open_round_bracket = {
  .type = tlso_open_round_bracket
};

struct tl_scheme_object obj_open_square_bracket = {
  .type = tlso_open_square_bracket
};

const char *tl_strtype (struct tl_scheme_object *O) {
  switch (O->type) {
    case tlso_int: return "int";
    case tlso_long: return "long";
    case tlso_double: return "double";
    case tlso_str: return "str";
    case tlso_function: return "function";
    case tlso_list: return "list";
    case tlso_open_round_bracket: return "(";
    case tlso_open_square_bracket: return "[";
  }
  return NULL;
}

static int expand_utf8 = 0;
void tl_set_expand_utf8 (int i) {
  expand_utf8 = i;
}

struct tl_scheme_object *tl_scheme_object_new (enum tl_scheme_object_type type) {
  if (type == tlso_list) {
    return &obj_empty_list;
  }
  assert (type != tlso_open_square_bracket && type != tlso_open_round_bracket);
  struct tl_scheme_object *O = zmalloc0 (sizeof (struct tl_scheme_object));
  O->type = type;
  return O;
}

static void tl_scheme_object_sbdump_str (struct tl_buffer *b, const char *z) {
  const char *s;
  if (verbosity >= 4) {
    fprintf (stderr, "tl_scheme_object_bdump_str:");
    for (s = z; *s; s++) {
      fprintf (stderr, " %02x", (int) (unsigned char) *s);
    }
    fprintf (stderr, "\n");
    for (s = z; *s; s++) {
      fprintf (stderr, "  %c",  *s);
    }
    fprintf (stderr, "\n");
  }
  tl_string_buffer_append_char (b, '"');
  for (s = z; *s; s++) {
    switch (*s) {
      case '\n':
        tl_string_buffer_append_char (b, '\\');
        tl_string_buffer_append_char (b, 'n');
      break;
      case '\r':
        tl_string_buffer_append_char (b, '\\');
        tl_string_buffer_append_char (b, 'r');
      break;
      case '\t':
        tl_string_buffer_append_char (b, '\\');
        tl_string_buffer_append_char (b, 't');
      break;
      case '\\':
        tl_string_buffer_append_char (b, '\\');
        tl_string_buffer_append_char (b, '\\');
      break;
      case '"':
        tl_string_buffer_append_char (b, '\\');
        tl_string_buffer_append_char (b, '"');
      break;
      default:
        if (isprint (*s) || !expand_utf8) {
          tl_string_buffer_append_char (b, *s);
        } else {
          tl_string_buffer_printf (b, "\\x%02x", (int) (unsigned char) *s);
        }
      break;
    }
  }
  tl_string_buffer_append_char (b, '"');
}

int tl_scheme_object_is_colon_terminated_function (struct tl_scheme_object *O) {
  if (O->type != tlso_function) {
    return 0;
  }
  int l = strlen (O->u.s);
  return (l > 0 && O->u.s[l-1] == ':') ? 1 : 0;
}

void tl_scheme_object_sbdump (struct tl_buffer *b, struct tl_scheme_object *O) {
  char ch, obrace, cbrace;
  struct tl_scheme_object *p;
  switch (O->type) {
    case tlso_int:
      tl_string_buffer_printf (b, "%d", O->u.i);
    break;
    case tlso_long:
      tl_string_buffer_printf (b, "%lld", O->u.l);
    break;
    case tlso_double:
      tl_string_buffer_printf (b, "%.15lg", O->u.d);
    break;
    case tlso_str:
      tl_scheme_object_sbdump_str (b, O->u.s);
    break;
    case tlso_function:
      tl_string_buffer_printf (b, "%s", O->u.s);
    break;
    case tlso_list:
      ch = 0;
      if (O->u.p.cdr != NULL && O->u.p.car->type == tlso_open_square_bracket) {
        obrace = '[';
        cbrace = ']';
        O = O->u.p.cdr;
      } else {
        obrace = '(';
        cbrace = ')';
      }
      tl_string_buffer_append_char (b, obrace);
      for (p = O; p != &obj_empty_list; p = p->u.p.cdr) {
        assert (p->type == tlso_list);
        if (ch != 0) {
          tl_string_buffer_append_char (b, ch);
        }
        tl_scheme_object_sbdump (b, p->u.p.car);
        ch = tl_scheme_object_is_colon_terminated_function (p->u.p.car) ? 0 : ' ';
      }
      tl_string_buffer_append_char (b, cbrace);
    break;
    case tlso_open_round_bracket:
      tl_string_buffer_printf (b, "(");
    break;
    case tlso_open_square_bracket:
      tl_string_buffer_printf (b, "[");
    break;
  }
}

static void indent (struct tl_buffer *b, int depth) {
  while (--depth >= 0) {
    tl_string_buffer_append_char (b, '\t');
  }
}

static int tl_scheme_object_contain_list (struct tl_scheme_object *O) {
  if (O->type != tlso_list) {
    return 0;
  }
  while (O != &obj_empty_list) {
    assert (O->type == tlso_list);
    struct tl_scheme_object *A = O->u.p.car;
    if (A->type == tlso_list) {
      return 1;
    }
    O = O->u.p.cdr;
  }
  return 0;
}

void tl_scheme_object_sbdump_indent (struct tl_buffer *b, struct tl_scheme_object *O, int depth) {
  char ch, obrace, cbrace;
  struct tl_scheme_object *p;
  if (!tl_scheme_object_contain_list (O)) {
    tl_scheme_object_sbdump (b, O);
    return;
  }
  if (O->u.p.cdr != NULL && O->u.p.car->type == tlso_open_square_bracket) {
    obrace = '[';
    cbrace = ']';
    O = O->u.p.cdr;
    ch = ' ';
  } else {
    obrace = '(';
    cbrace = ')';
    ch = 0;
  }
  tl_string_buffer_append_char (b, obrace);
  for (p = O; p != &obj_empty_list; p = p->u.p.cdr) {
    assert (p->type == tlso_list);
    if (ch != 0) {
      tl_string_buffer_append_char (b, '\n');
      indent (b, depth+1);
    }
    tl_scheme_object_sbdump_indent (b, p->u.p.car, depth + 1);
    ch = tl_scheme_object_is_colon_terminated_function (p->u.p.car) ? 0 : ' ';
  }
  tl_string_buffer_append_char (b, '\n');
  indent (b, depth);
  tl_string_buffer_append_char (b, cbrace);
}

void tl_scheme_object_dump (FILE *f, struct tl_scheme_object *O) {
  struct tl_buffer b;
  tl_string_buffer_init (&b);
  tl_scheme_object_sbdump (&b, O);
  tl_string_buffer_append_char (&b, 0);
  fprintf (f, "%s", b.buff);
  tl_string_buffer_free (&b);
}

void tl_scheme_object_free (struct tl_scheme_object *A) {
  switch (A->type) {
    case tlso_list:
      if (A != &obj_empty_list) {
        tl_scheme_object_free (A->u.p.car);
        if (A->u.p.cdr != &obj_empty_list) {
          tl_scheme_object_free (A->u.p.cdr);
        }
      }
    break;
    case tlso_str:
    case tlso_function:
      cstr_free (&A->u.s);
    break;
    case tlso_open_round_bracket:
    case tlso_open_square_bracket:
      return;
    case tlso_int:
    case tlso_long:
    case tlso_double:
    break;
  }
  zfree (A, sizeof (struct tl_scheme_object));
}


struct tl_scheme_object *tl_scheme_cons (struct tl_scheme_object *A, struct tl_scheme_object *B) {
  if (verbosity >= 3) {
    fprintf (stderr, "(cons ");
    tl_scheme_object_dump (stderr, A);
    fprintf (stderr, " ");
    tl_scheme_object_dump (stderr, B);
    fprintf (stderr, ")\n");
  }
  if (B->type != tlso_list) {
    return NULL;
  }
  struct tl_scheme_object *O = zmalloc0 (sizeof (struct tl_scheme_object));
  O->type = tlso_list;
  O->u.p.car = A;
  O->u.p.cdr = B;
  return O;
}

/* NOTICE: this function change input list A */
struct tl_scheme_object *tl_scheme_reverse (struct tl_scheme_object *A) {
  if (verbosity >= 3) {
    fprintf (stderr, "(reverse ");
    tl_scheme_object_dump (stderr, A);
    fprintf (stderr, ")\n");
  }
  if (A->type != tlso_list) {
    return NULL;
  }
  struct tl_scheme_object *O = &obj_empty_list, *p, *w;
  for (p = A; p != &obj_empty_list; p = w) {
    assert (p->type == tlso_list);
    w = p->u.p.cdr;
    p->u.p.cdr = O;
    O = p;
  }
  return O;
}

int tl_scheme_length (struct tl_scheme_object *A) {
  if (A->type != tlso_list) {
    return -1;
  }
  int t = 0;
  while (A != &obj_empty_list) {
    assert (A->type == tlso_list);
    t++;
    A = A->u.p.cdr;
  }
  return t;
}

int tl_scheme_int_value (struct tl_scheme_object *O, int *value) {
  switch (O->type) {
    case tlso_int:
      *value = O->u.i;
      return 0;
    case tlso_long:
      if (INT_MIN <= O->u.l && O->u.l <= INT_MAX) {
        *value = (int) (O->u.l);
        return 0;
      }
    default:
    break;
  }
  return -1;
}

int tl_scheme_long_value (struct tl_scheme_object *O, long long *value) {
  switch (O->type) {
    case tlso_int:
      *value = O->u.i;
      return 0;
    case tlso_long:
      *value = O->u.l;
      return 0;
    default:
    break;
  }
  return -1;
}

int tl_scheme_double_value (struct tl_scheme_object *O, double *value) {
  switch (O->type) {
    case tlso_int:
      *value = O->u.i;
      return 0;
    case tlso_long:
      *value = O->u.l;
      return 0;
    case tlso_double:
      *value = O->u.d;
      return 0;
    default:
    break;
  }
  return -1;
}

int tl_scheme_string_value (struct tl_scheme_object *O, char **value) {
  switch (O->type) {
    case tlso_str:
      *value = O->u.s;
       return 0;
    default:
    break;
  }
  return -1;
}

struct tl_scheme_parse_resource {
  struct tl_scheme_object *stack;
  struct tl_buffer sb;
};

void tl_scheme_parse_cleanup (struct tl_scheme_parse_resource *R) {
  if (R->stack) {
    tl_scheme_object_free (R->stack);
  }
  tl_string_buffer_free (&R->sb);
}

static struct tl_scheme_object *tl_scheme_cons_number (long long r, struct tl_scheme_object *stack) {
  struct tl_scheme_object *O;
  if (INT_MIN <= r && r <= INT_MAX) {
    O = tl_scheme_object_new (tlso_int);
    O->u.i = (int) r;
  } else {
    O = tl_scheme_object_new (tlso_long);
    O->u.l = r;
  }
  stack = tl_scheme_cons (O, stack);
  assert (stack);
  return stack;
}

struct tl_scheme_object *tl_scheme_parse (struct tl_compiler *C, const char *const text) {
  struct tl_scheme_object *O;
  struct tl_scheme_parse_resource R;
  R.stack = tl_scheme_object_new (tlso_list);
  tl_string_buffer_init (&R.sb);
  int state = 0, negative = 0;
  long long r = 0;
  const char *s, *t = NULL;
  double d;
  int i, digits = -987654321;
  char c;
  for (s = text; *s; s++) {
    if (verbosity >= 3) {
      fprintf (stderr, "cur:%c, state: %d\n", *s, state);
    }
    switch (state) {
    case 0:
       if (isalpha (*s)) {
         t = s;
         state = 3;
       } else if (isspace (*s)) {
       } else if (*s == '(') {
         R.stack = tl_scheme_cons (&obj_open_round_bracket, R.stack);
         assert (R.stack);
       } else if (*s == '[') {
         R.stack = tl_scheme_cons (&obj_open_square_bracket, R.stack);
         assert (R.stack);
       } else if (*s == ')') {
         struct tl_scheme_object *p, *h = tl_scheme_object_new (tlso_list);
         for (p = R.stack; p != &obj_empty_list; p = p->u.p.cdr) {
           assert (p->type == tlso_list);
           struct tl_scheme_object *a = p->u.p.car;
           if (a->type == tlso_open_round_bracket) {
             R.stack = p->u.p.cdr;
             R.stack = tl_scheme_cons (h, R.stack);
             break;
           } else if (a->type == tlso_open_square_bracket) {
             tl_failfp (C, text, s, "found '[' instead of '('");
             tl_scheme_parse_cleanup (&R);
             return NULL;
           } else {
             //id:E -> ("id" E)
             if (a->type == tlso_function) {
               char *z = a->u.s;
               int l = strlen (z);
               assert (l > 0);
               if (z[l-1] == ':') {
                 if (h == &obj_empty_list) {
                   tl_failfp (C, text, s, "unexpected ')' after %s", z);
                   tl_scheme_parse_cleanup (&R);
                   return NULL;
                 }
                 char *y = cstr_substr (z, 0, l - 1);
                 cstr_free (&a->u.s);
                 a->u.s = y;
                 a->type = tlso_str;
                 struct tl_scheme_object *b = h->u.p.cdr;
                 h->u.p.cdr = &obj_empty_list;
                 a = tl_scheme_cons (a, h);
                 h = b;
               }
             }
             h = tl_scheme_cons (a, h);
             assert (h);
           }
         }
         if (p == &obj_empty_list) {
           tl_failfp (C, text, s, "didn't find openning bracket '('");
           tl_scheme_parse_cleanup (&R);
           return NULL;
         }
       } else if (*s == ']') {
         struct tl_scheme_object *p, *h = tl_scheme_object_new (tlso_list);
         for (p = R.stack; p != &obj_empty_list; p = p->u.p.cdr) {
           assert (p->type == tlso_list);
           struct tl_scheme_object *a = p->u.p.car;
           if (a->type == tlso_open_square_bracket) {
             h = tl_scheme_cons (&obj_open_square_bracket, h);
             assert (h);
             R.stack = p->u.p.cdr;
             R.stack = tl_scheme_cons (h, R.stack);
             break;
           } else if (a->type == tlso_open_round_bracket) {
             tl_failfp (C, text, s, "found '(' instead of '['");
             tl_scheme_parse_cleanup (&R);
             return NULL;
           } else {
             h = tl_scheme_cons (a, h);
             assert (h);
           }
         }
         if (p == &obj_empty_list) {
           tl_failfp (C, text, s, "didn't find openning bracket '['");
           tl_scheme_parse_cleanup (&R);
           return NULL;
         }
       } else if (*s == '-' || isdigit(*s)) {
         t = s;
         negative = *s == '-';
         r = 0;
         digits = 0;
         if (isdigit (*s)) {
           r = (*s) - '0';
           assert (digits >= 0);
           digits++;
         }
         state = 1;
       } else if (*s == '"') {
         tl_string_buffer_clear (&R.sb);
         state = 2;
       } else if (*s == ';') {
         state = 4;
       } else {
         tl_failfp (C, text, s, "illegal expression first character");
         tl_scheme_parse_cleanup (&R);
         return NULL;
       }
    break;
    /* number */
    case 1:
      if (isdigit(*s)) {
        r = r * 10 + ((*s) - '0');
        assert (digits >= 0);
        digits++;
      } else if (isspace (*s) || *s == '(' || *s == ')' || *s == '[' || *s == ']') {
        if (negative) {
          r *= -1;
        }
        R.stack = tl_scheme_cons_number (r, R.stack);
        s--;
        state = 0;
      } else if (*s == '.' || *s == 'e' || *s == 'E') {
        c = 'a';
        i = -1;
        if (sscanf (t, "%lf%n%c", &d, &i, &c) >= 2 && i >= 0 && (isspace (c) || c == ')' || c == ']')) {
          O = tl_scheme_object_new (tlso_double);
          O->u.d = d;
          R.stack = tl_scheme_cons (O, R.stack);
          assert (R.stack);
          s = t + i - 1;
        } else {
          tl_failfp (C, text, s, "expected space or bracket after double");
          tl_scheme_parse_cleanup (&R);
          return NULL;
        }
        state = 0;
      } else if (*s == 'x' && s[-1] == '0' && digits == 1) {
        digits = 0;
        state = 9;
      } else {
        tl_failfp (C, text, s, "expected space or bracket after number");
        tl_scheme_parse_cleanup (&R);
        return NULL;
      }
    break;
    /* hex number */
    case 9:
      if (isxdigit (*s)) {
        r <<= 4;
        if (isdigit (*s)) {
          r += ((*s) - '0');
        } else {
          r += tolower (*s) - ('a' - 10);
        }
        assert (digits >= 0);
        digits++;
      } else if (isspace (*s) || *s == '(' || *s == ')' || *s == '[' || *s == ']') {
        if (negative) {
          r *= -1;
        }
        R.stack = tl_scheme_cons_number (r, R.stack);
        s--;
        state = 0;
      } else {
        tl_failfp (C, text, s, "expected space or bracket after hexnumber");
        tl_scheme_parse_cleanup (&R);
        return NULL;
      }
    break;
    /* str */
    case 2:
      if (*s == '"') {
        O = tl_scheme_object_new (tlso_str);
        O->u.s = tl_string_buffer_to_cstr (&R.sb);
        R.stack = tl_scheme_cons (O, R.stack);
        assert (R.stack);
        state = 0;
      } else if (*s == '\\') {
        state = 6;
      } else {
        tl_string_buffer_append_char (&R.sb, *s);
      }
    break;
    /* string after \ */
    case 6:
      switch (*s) {
        case '"':
          tl_string_buffer_append_char (&R.sb, '"');
          state = 2;
        break;
        case '\\':
          tl_string_buffer_append_char (&R.sb, '\\');
          state = 2;
        break;
        case 'n':
          tl_string_buffer_append_char (&R.sb, '\n');
          state = 2;
        break;
        case 'r':
          tl_string_buffer_append_char (&R.sb, '\r');
          state = 2;
        break;
        case 't':
          tl_string_buffer_append_char (&R.sb, '\t');
          state = 2;
        break;
        case 'x':
          state = 7;
        break;
        default:
          tl_failfp (C, text, s, "unimplemented escape sequence \\%c", *s);
          tl_scheme_parse_cleanup (&R);
          return NULL;
        break;
      }
    break;
    case 7:
      switch (tolower(*s)) {
        case '0' ... '9':
          i = (*s) - '0';
        break;
        case 'a' ... 'f':
          i = *s - 87;
        break;
        default:
          tl_failfp (C, text, s, "unknown escape sequence \\x%c", *s);
          tl_scheme_parse_cleanup (&R);
          return NULL;
        break;
      }
      i <<= 4;
      state = 8;
    break;
    case 8:
      switch (tolower(*s)) {
        case '0' ... '9':
          i |= (*s) - '0';
        break;
        case 'a' ... 'f':
          i |= *s - 87;
        break;
        default:
          tl_failfp (C, text, s, "unknown escape sequence \\x%c%c", s[-1], *s);
          tl_scheme_parse_cleanup (&R);
          return NULL;
        break;
      }
      tl_string_buffer_append_char (&R.sb, (char) i);
      state = 2;
    break;
    /* function name */
    case 3:
      if (isalpha (*s) || *s == '_' || *s == '.' || *s == '<' || *s == '>') {
      } else if (*s == ':') {
        /* sugar */
        O = tl_scheme_object_new (tlso_function);
        O->u.s = cstr_substr (t, 0, s - t + 1);
        R.stack = tl_scheme_cons (O, R.stack);
        assert (R.stack);
        state = 0;
      } else {
        O = tl_scheme_object_new (tlso_function);
        O->u.s = cstr_substr (t, 0, s - t);
        R.stack = tl_scheme_cons (O, R.stack);
        assert (R.stack);
        s--;
        state = 0;
      }
    break;
    /* after ';' */
    case 4:
      if (*s == ';') {
        state = 5;
      } else {
        tl_failfp (C, text, s, "expected ;");
        tl_scheme_parse_cleanup (&R);
        return NULL;
      }
    break;
    /* comment */
    case 5:
      if (*s == '\n') {
        state = 0;
      }
    break;
    }
  }

  if (state) {
    tl_failfp (C, text, s, "expected closing bracket");
    tl_scheme_parse_cleanup (&R);
    return NULL;
  }
  O = tl_scheme_reverse (R.stack);
  R.stack = NULL;
  tl_scheme_parse_cleanup (&R);
  return O;
}

struct tl_scheme_object *tl_scheme_parse_file (struct tl_compiler *C, const char *const filename) {
  char *a = tl_readfile (C, filename);
  if (a == NULL) {
    return NULL;
  }
  struct tl_scheme_object *O = tl_scheme_parse (C, a);
  free (a);
  return O;
}

