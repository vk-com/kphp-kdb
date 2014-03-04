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

//#include <stdarg.h>
#include <errno.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#include "tl-serialize.h"
#include "tl-scheme.h"
#include "tl-utils.h"

static int tl_serialize_failf (struct tl_compiler *C, struct tl_scheme_object *E, const char *format, ...) __attribute__ ((format (printf, 3, 4)));

static int tl_serialize_failf (struct tl_compiler *C, struct tl_scheme_object *E, const char *format, ...) {
  const int old_errno = errno;
  tl_string_buffer_clear (&C->tmp_error_buff);
  tl_scheme_object_sbdump (&C->tmp_error_buff, E);
  tl_string_buffer_append_char (&C->tmp_error_buff, ':');
  tl_string_buffer_append_char (&C->tmp_error_buff, ' ');
  va_list ap;
  va_start (ap, format);
  tl_string_buffer_vprintf (&C->tmp_error_buff, format, ap);
  va_end (ap);
  errno = old_errno;
  tl_compiler_add_error (C);
  return -1;
}

int tl_expression_serialize_builtin_type (struct tl_compiler *C, struct tl_scheme_object *E, const char *const name, struct tl_int_array *a) {
  int i;
  long long l;
  double d;
  char *s;
  switch (tolower (name[0])) {
    case 'd':
      if (!strcmp (name + 1, "ouble")) {
        if (tl_scheme_double_value (E, &d) < 0) {
          return tl_serialize_failf (C, E, "isn't of type 'double'");
        }
        if (isupper (name[0])) {
          if (tl_int_array_append (a, CODE_double) < 0) {
            return tl_serialize_failf (C, E, "output buffer overflow");
          }
        }
        if (tl_int_array_append_double (a, d) < 0) {
          return tl_serialize_failf (C, E, "output buffer overflow");
        }
        return 1;
      }
    break;
    case 'i':
      if (!strcmp (name + 1, "nt")) {
        if (tl_scheme_int_value (E, &i) < 0) {
          return tl_serialize_failf (C, E, "isn't of type 'int'");
        }
        if (isupper (name[0])) {
          if (tl_int_array_append (a, CODE_int) < 0) {
            return tl_serialize_failf (C, E, "output buffer overflow");
          }
        }
        if (tl_int_array_append (a, i) < 0) {
          return tl_serialize_failf (C, E, "output buffer overflow");
        }
        return 1;
      }
    break;
    case 'l':
      if (!strcmp (name + 1, "ong")) {
        if (tl_scheme_long_value (E, &l) < 0) {
          return tl_serialize_failf (C, E, "isn't of type 'long'");
        }
        if (isupper (name[0])) {
          if (tl_int_array_append (a, CODE_long) < 0) {
            return tl_serialize_failf (C, E, "output buffer overflow");
          }
        }
        if (tl_int_array_append_long (a, l) < 0) {
          return tl_serialize_failf (C, E, "output buffer overflow");
        }
        return 1;
      }
    break;
    case 's':
      if (!strcmp (name + 1, "tring")) {
        if (tl_scheme_string_value (E, &s) < 0) {
          return tl_serialize_failf (C, E, "isn't of type 'string'");
        }
        if (isupper (name[0])) {
          if (tl_int_array_append (a, CODE_string) < 0) {
            return tl_serialize_failf (C, E, "output buffer overflow");
          }
        }
        if (tl_int_array_append_string (a, s) < 0) {
          return tl_serialize_failf (C, E, "output buffer overflow");
        }
        return 1;
      }
    break;
  }

  return 0;
}

int tl_expression_serialize_type (struct tl_compiler *C, struct tl_scheme_object *G, char *name, struct tl_int_array *a);

int tl_expression_serialize_general (struct tl_compiler *C, struct tl_scheme_object *E, int section, const char *const type_name, struct tl_int_array *a) {
  const int old_errors = C->errors;
  const int mark = tl_int_array_mark (a);
  if (E->type != tlso_list) {
    tl_int_array_release (a, mark);
    return tl_serialize_failf (C, E, "expected LIST object");
  }
  if (E == &obj_empty_list) {
    tl_int_array_release (a, mark);
    return tl_serialize_failf (C, E, "empty expression");
  }
  struct tl_scheme_object *A = E->u.p.car;

  if (A->type != tlso_function) {
    tl_int_array_release (a, mark);
    return tl_serialize_failf (C, E, "expected combinator name");
  }
  char *combinator_name = A->u.s;
  struct tl_expression *B = tl_list_expressions_find_by_combinator_name (C, section, combinator_name, type_name);
  if (B == NULL) {
    tl_int_array_release (a, mark);
    return tl_serialize_failf (C, E, "didn't find combinator '%s' with result type '%s'", combinator_name, type_name);
  }

  if (section == TL_SECTION_FUNCTIONS && C->serialized_first_function_expr == NULL) {
    C->serialized_first_function_expr = B;
  }

  if (tl_int_array_append (a, B->magic) < 0) {
    tl_int_array_release (a, mark);
    return tl_serialize_failf (C, E, "output buffer overflow");
  }

  if (type_name && !section && strcmp (B->right_name, type_name)) {
    tl_int_array_release (a, mark);
    return tl_serialize_failf (C, E, "types not equal (combinator: %s, rhs_type: %s, expected_type: %s", combinator_name, B->right_name, type_name);
  }

  struct tl_token *T;
  A = E;
  for (T = B->left->next; T != NULL; T = T->next) {
    char *q = strchr (T->text, ':');
    if (q == NULL) {
      assert (0); //TODO: anonymus function arguments
    } else {
      struct tl_scheme_object *prev_A = A;
      A = A->u.p.cdr;
      if (A == &obj_empty_list) {
        tl_int_array_release (a, mark);
        return tl_serialize_failf (C, E, "not enough args, expected %s", T->text);
      }

      struct tl_scheme_object *P = prev_A, *I = A;
      int arg_serialized = 0;
      while (I != &obj_empty_list) {
        //tl_scheme_object_dump (stderr, I); fprintf (stderr, "\n");
        assert (I->type == tlso_list);
        struct tl_scheme_object *D = I->u.p.car;
        if (D->type == tlso_list) {
          struct tl_scheme_object *F = D->u.p.car, *G = D->u.p.cdr;
          assert (G->type == tlso_list);
          if (G != &obj_empty_list && G->u.p.cdr == &obj_empty_list && F->type == tlso_str && !strncmp (T->text, F->u.s, q - T->text)) {
            if (I != A) {
              P->u.p.cdr = I->u.p.cdr;
              I->u.p.cdr = A;
              prev_A->u.p.cdr = I;
              A = I;
            }
            if (tl_expression_serialize_type (C, G->u.p.car, q + 1, a) < 0) {
              tl_int_array_release (a, mark);
              return tl_serialize_failf (C, E, "fail to serialize type %s", q + 1);
            }
            arg_serialized = 1;
            break;
          }
        }
        P = I;
        I = I->u.p.cdr;
      }

      if (arg_serialized) {
        continue;
      }

      if (tl_expression_serialize_type (C, A->u.p.car, q + 1, a) < 0) {
        tl_int_array_release (a, mark);
        return tl_serialize_failf (C, E, "fail to serialize type %s", q + 1);
      }
    }
  }
  return tl_success (C, old_errors);
}

int tl_expression_serialize_type (struct tl_compiler *C, struct tl_scheme_object *G, char *name, struct tl_int_array *a) {
  int i;
  struct tl_expression *H;
  const int old_errors = C->errors;
  if (verbosity >= 3) {
    fprintf (stderr, "tl_expression_serialize_type (G:");
    tl_scheme_object_dump (stderr, G);
    fprintf (stderr, ", name: %s, a->pos: %d\n", name, a->pos);
  }
  const int mark = tl_int_array_mark (a);

  int serialize_object = -1;
  if (!strcmp (name, "Object")) {
    serialize_object = 1;
    if (G->type != tlso_list) {
      static const char *boxed_builtin_typenames[] = { "Int", "Long", "Double", "String", NULL };
      for (i = 0; boxed_builtin_typenames[i]; i++) {
        int r = tl_expression_serialize_builtin_type (C, G, boxed_builtin_typenames[i], a);
        if (r < 0) {
          tl_int_array_release (a, mark);
        } else if (r > 0) {
          return tl_success (C, old_errors);
        }
      }
    } else if (G->type == tlso_list && G != &obj_empty_list && (G->u.p.car->type == tlso_function)) {
      H = tl_list_expressions_find_by_combinator_name (C, TL_SECTION_TYPES, G->u.p.car->u.s, NULL);
      if (H && !tl_expression_serialize_general (C, G, TL_SECTION_TYPES, H->right_name, a)) {
        return tl_success (C, old_errors);
      }
    }
    H = C->expr[TL_SECTION_TYPES].next;
    //tl_success (C, old_errors+1);
    //tl_int_array_release (a, mark);
  } else {
    serialize_object = 0;
    int r = tl_expression_serialize_builtin_type (C, G, name, a);
    if (r < 0) {
      tl_int_array_release (a, mark);
      return -1;
    }
    if (r > 0) {
      return tl_success (C, old_errors);
    }

    if (G->type == tlso_list && G != &obj_empty_list && G->u.p.car->type == tlso_function) {
      if (!tl_expression_serialize_general (C, G, TL_SECTION_TYPES, name, a)) {
        return tl_success (C, old_errors);
      }
    }
    H = tl_expression_find_first_by_composite_typename (C, name);
  }

  const int old_errors2 = C->errors;

  while ((serialize_object && H != &C->expr[TL_SECTION_TYPES]) || (!serialize_object && H != NULL)) {
    if (verbosity >= 4) {
      fprintf (stderr, "H->left->text: %s, H->right_name: %s\n", H->left->text, H->right_name);
    }
    //tl_success (C, old_errors);

    if (!strcmp ("vector", H->left->text) && !H->flag_builtin) {
      if (G->type != tlso_list) {
        tl_serialize_failf (C, G, "expected LISP list");
        goto fail;
      }

      int l = tl_scheme_length (G) - 1; /* first vector element is '[' */
      if (tl_int_array_append (a, C->flag_code_vector ? CODE_vector : H->magic) < 0 || tl_int_array_append (a, l) < 0) {
        tl_serialize_failf (C, G, "output buffer overflow");
        goto fail;
      }

      char *alpha = tl_expression_get_vector_item_type (H);
      if (alpha == NULL) {
        tl_serialize_failf (C, G, "tl_expression_get_vector_item_type returns NULL");
        goto fail;
      }
      struct tl_scheme_object *O;
      int k;
      for (O = G, k = 0; O != &obj_empty_list; O = O->u.p.cdr, k++) {
        assert (O->type == tlso_list);
        struct tl_scheme_object *A = O->u.p.car;
        if (!k) {
          if (A->type != tlso_open_square_bracket) {
            tl_serialize_failf (C, G, "vector should be enclosed into square brackets");
            goto fail;
          }
        } else {
          if (tl_expression_serialize_type (C, A, alpha, a) < 0) {
            tl_serialize_failf (C, G, "fail to serialize vector item with type %s", alpha);
            goto fail;
          }
        }
      }
      return tl_success (C, old_errors);
    }
    fail:
    tl_int_array_release (a, mark);
    H = serialize_object ? H->next : H->rnext;
  }
  tl_success (C, old_errors2);
  tl_int_array_release (a, mark);
  return tl_serialize_failf (C, G, "can't serialize as %s", name);
}

int tl_expression_serialize (struct tl_compiler *C, struct tl_scheme_object *E, struct tl_int_array *a) {
  return tl_expression_serialize_general (C, E, TL_SECTION_FUNCTIONS, NULL, a);
}

int tl_serialize (struct tl_compiler *C, struct tl_scheme_object *expressions, int *out, int olen) {
  struct tl_int_array a;
  tl_compiler_clear_errors (C);
  tl_int_array_init (&a, out, olen);
  struct tl_scheme_object *O = expressions, *p;
  for (p = O; p != &obj_empty_list; p = p->u.p.cdr) {
    int r = tl_expression_serialize (C, p->u.p.car, &a);
    if (r < 0) {
      return r;
    }
  }
  return a.pos;
}

int tl_expression_unserialize_builtin_type (struct tl_compiler *C, int *input, int ilen, const char *name, struct tl_scheme_object **R) {
  if (name == NULL) {
    return 0;
  }
  int i = 0;
  switch (tolower (name[0])) {
    case 'd':
      if (!strcmp (name + 1, "ouble")) {
        if (isupper (name[0])) {
          if (i >= ilen) {
            return tl_failf (C, "not enough input to unserialize %s", name);
          }
          if (input[i] != CODE_double) {
            return tl_failf (C, "unserialize Double failed, expected magic 0x%08x but 0x%08x found", CODE_double, input[i]);
          }
          i++;
        }
        if (i >= ilen - 1) {
          return tl_failf (C, "not enough input to unserialize %s", name);
        }
        *R = tl_scheme_object_new (tlso_double);
        (*R)->u.d = *((double *) &input[i]);
        i += 2;
        return i;
      }
    break;
    case 'i':
      if (!strcmp (name + 1, "nt")) {
        if (isupper (name[0])) {
          if (i >= ilen) {
            return tl_failf (C, "not enough input to unserialize %s", name);
          }
          if (input[i] != CODE_int) {
            return tl_failf (C, "unserialize Int failed, expected magic 0x%08x but 0x%08x found", CODE_int, input[i]);
          }
          i++;
        }
        if (i >= ilen) {
          return tl_failf (C, "not enough input to unserialize %s", name);
        }
        *R = tl_scheme_object_new (tlso_int);
        (*R)->u.i = input[i++];
        return i;
      }
    break;
    case 'l':
      if (!strcmp (name + 1, "ong")) {
        if (isupper (name[0])) {
          if (i >= ilen) {
            return tl_failf (C, "not enough input to unserialize %s", name);
          }
          if (input[i] != CODE_long) {
            return tl_failf (C, "unserialize Long failed, expected magic 0x%08x but 0x%08x found", CODE_long, input[i]);
          }
          i++;
        }
        if (i >= ilen - 1) {
          return tl_failf (C, "not enough input to unserialize %s", name);
        }
        *R = tl_scheme_object_new (tlso_long);
        (*R)->u.l = *((long long *) &input[i]);
        i += 2;
        return i;
      }
    break;
    case 's':
      if (!strcmp (name + 1, "tring")) {
        if (isupper (name[0])) {
          if (i >= ilen) {
            return tl_failf (C, "not enough input to unserialize %s", name);
          }
          if (input[i] != CODE_string) {
            return tl_failf (C, "unserialize String failed, expected magic 0x%08x but 0x%08x found", CODE_string, input[i]);
          }
          i++;
        }
        if (i >= ilen) {
          return tl_failf (C, "not enough input to unserialize %s", name);
        }
        char *s;
        int l = tl_fetch_string (input + i, ilen - i, &s, NULL, 1);
        if (l < 0) {
          return tl_failf (C, "tl_fetch_string fail");
        }
        *R = tl_scheme_object_new (tlso_str);
        (*R)->u.s = s;
        return i + l;
      }
    break;
  }
  return 0;
}

int tl_expression_unserialize (struct tl_compiler *C, int *input, int ilen, int section_mask, const char *type_name, struct tl_scheme_object **R) {
  const int old_errors = C->errors;
  //TODO: unserialize null
  int i = tl_expression_unserialize_builtin_type (C, input, ilen, type_name, R);
  if (verbosity >= 4) {
    fprintf (stderr, "tl_expression_unserialize_builtin_type (..., ilen:%d, type_name: %s, ...) returns %d.\n", ilen, type_name, i);
  }
  if (i) {
    return i;
  }
  assert (!i);

  if (i >= ilen) {
    return tl_failf (C, "not enough input to unserialize %s", type_name ? type_name : "???");
  }

  int section;
  struct tl_scheme_object *stack, *O;
  for (section = 1; section >= 0; section--) {
    if (!((1 << section) & section_mask)) {
      continue;
    }
    int magic = input[i++];
    struct tl_expression *E = NULL;
    if (magic == CODE_vector) {
       E = tl_expression_find_first_by_composite_typename (C, (char *) type_name);
       if (E) {
         assert (E->rnext == NULL);
       }
    } else {
      struct tl_hashmap **V = &C->hm_magic[section];
      struct tl_expression TM;
      TM.magic = magic;
      E = tl_hashmap_get_f (V, &TM, 0);
    }

    if (E == NULL) {
      return tl_failf (C, "couldn't find 0x%08x magic in %s", magic, tl_get_section_name (section));
    }

    if (!strcmp (E->left->text, "vector")) {
      const char *alpha = tl_expression_get_vector_item_type (E);
      if (alpha == NULL) {
        return tl_failf (C, "tl_expression_get_vector_item_type returns NULL");
      }
      if (i >= ilen) {
        return tl_failf (C, "not enough input to unserialize %s length", type_name ? type_name : "???");
      }
      int vector_length = input[i++];
      if (vector_length < 0) {
        return tl_failf (C, "negative %s length", type_name ? type_name : "???");
      }
      stack = tl_scheme_object_new (tlso_list);
      stack = tl_scheme_cons (&obj_open_square_bracket, stack);
      assert (stack);
      int k;
      for (k = 0; k < vector_length; k++) {
        int r = tl_expression_unserialize (C, input + i, ilen - i, 1, alpha, &O);
        if (r < 0) {
          tl_scheme_object_free (stack);
          return tl_failf (C, "fail to unserialize vector %d-th element of type %s", k, alpha);
        }
        i += r;
        stack = tl_scheme_cons (O, stack);
      }
      *R = tl_scheme_reverse (stack);
      stack = NULL;

      (void) tl_success (C, old_errors);
      return i;
    }

    stack = tl_scheme_object_new (tlso_list);
    O = tl_scheme_object_new (tlso_function);
    O->u.s = cstr_dup (E->left->text);
    stack = tl_scheme_cons (O, stack);
    assert (stack);
    struct tl_token *T;
    for (T = E->left->next; T != NULL; T = T->next) {
      char *q = strchr (T->text, ':');
      if (q == NULL) {
        assert (0);
      } else {
        int r = tl_expression_unserialize (C, input + i, ilen - i, 1, q + 1, &O);
        if (r < 0) {
          tl_scheme_object_free (stack);
          return tl_failf (C, "fail to unserialize arg %.*s", (int) (q - T->text), T->text);
        }
        i += r;

        if (C->flag_unserialize_strict_lisp) {
          struct tl_scheme_object *P = tl_scheme_object_new (tlso_list), *Q = tl_scheme_object_new (tlso_str);
          Q->u.s = cstr_substr (T->text, 0, q - T->text);
          P = tl_scheme_cons (O, P);
          assert (P);
          P = tl_scheme_cons (Q, P);
          assert (P);
          stack = tl_scheme_cons (P, stack);
          assert (stack);
        } else {
          struct tl_scheme_object *Q = tl_scheme_object_new (tlso_function);
          Q->u.s = cstr_substr (T->text, 0, q - T->text + 1);
          stack = tl_scheme_cons (Q, stack);
          assert (stack);
          stack = tl_scheme_cons (O, stack);
          assert (stack);
        }
      }
    }
    *R = tl_scheme_reverse (stack);
    stack = NULL;
    (void) tl_success (C, old_errors);
    return i;
  }
  return -1;
}

int tl_unserialize (struct tl_compiler *C, struct tl_buffer *b, int *input, int ilen, int section_mask) {
  struct tl_scheme_object *O = NULL;
  tl_compiler_clear_errors (C);
  int r = tl_expression_unserialize (C, input, ilen, section_mask, NULL, &O);
  if (r >= 0 && O != NULL) {
    tl_scheme_object_sbdump (b, O);
    tl_scheme_object_free (O);
  }
  return r;
}

int tl_serialize_rpc_function_call (struct tl_compiler *C, const char *const text, int *out, int olen, char **result_typename) {
  struct tl_scheme_object *O = tl_scheme_parse (C, text);
  *result_typename = NULL;
  if (O == NULL) {
    return -1;
  }
  if (O->u.p.cdr != &obj_empty_list) {
    tl_compiler_clear_errors (C);
    tl_scheme_object_free (O);
    return tl_failf (C, "expected single rpc call");
  }

  int r = tl_serialize (C, O, out, olen);
  if (r >= 0 && result_typename != NULL) {
    assert (C->serialized_first_function_expr);
    assert (C->serialized_first_function_expr->right_name);
    *result_typename = cstr_dup (C->serialized_first_function_expr->right_name);
  }
  return r;
}

int tl_unserialize_rpc_function_result (struct tl_compiler *C, struct tl_buffer *b, int *input, int ilen, char *result_typename, int indentation) {
  struct tl_scheme_object *O = NULL;
  tl_compiler_clear_errors (C);
  int r = tl_expression_unserialize (C, input, ilen, 1 << TL_SECTION_TYPES, result_typename, &O);
  if (r >= 0 && O != NULL) {
    if (indentation) {
      tl_scheme_object_sbdump_indent (b, O, 0);
    } else {
      tl_scheme_object_sbdump (b, O);
    }
    tl_scheme_object_free (O);
  }
  return r;
}
