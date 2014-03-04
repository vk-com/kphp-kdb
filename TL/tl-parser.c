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
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/stat.h>
#include "kdb-data-common.h"
#include "tl-utils.h"
#include "tl-parser.h"
#include "server-functions.h"
#include "crc32.h"

/*********************
*********************/

/********************* constants *********************/

const char *tl_builtin_shema =
  "int ? = Int;\n"
  "long ? = Long;\n"
  "double ? = Double;\n"
  "string ? = String;\n"
  "vector # [ alpha ] = Vector<alpha>;\n"
  "coupleInt int alpha = CoupleInt<alpha>;\n"
  "coupleStr string alpha = CoupleStr<alpha>;\n"
  "intHash vector coupleInt alpha = IntHash<alpha>;\n"
  "strHash vector coupleStr alpha = StrHash<alpha>;\n"
  "intSortedHash intHash alpha = IntSortedHash<alpha>;\n"
  "strSortedHash strHash alpha = StrSortedHash<alpha>;\n"
  "null = Null;\n"
  "---functions---\n";

const char *tl_get_section_name (int section) {
  static const char *sections_names[2] = { "section types", "section functions" };
  if (section < 0 || section > 1) {
    return "unknown section";
  }
  return sections_names[section];
}

/********************* token routines *********************/
static struct tl_token *list_token_reverse (struct tl_token *L) {
  struct tl_token *U, *V, *A = NULL;
  for (U = L; U != NULL; U = V) {
    V = U->next;
    U->next = A;
    A = U;
  }
  return A;
}

static void tl_token_length (struct tl_token *T, int *tokens, int *total_length) {
  *tokens = *total_length = 0;
  while (T != NULL) {
    (*tokens)++;
    (*total_length) += strlen (T->text);
    T = T->next;
  }
}

static struct tl_token *tl_token_clone (struct tl_token *T) {
  struct tl_token *head = NULL;
  while (T != NULL) {
    struct tl_token *A = zmalloc0 (sizeof (struct tl_token));
    A->text = cstr_dup (T->text);
    A->next = head;
    head = A;
    T = T->next;
  }
  return list_token_reverse (head);
}

static const char *reserved_words_polymorhic[] = {
  "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", NULL
};

static int tl_token_is_variable_type (struct tl_token *T) {
  if (verbosity >= 4) {
    fprintf (stderr, "tl_token_is_variable_type: %s\n", T->text);
  }

  int i;
  for (i = 0; reserved_words_polymorhic[i]; i++) {
    if (!strcmp (reserved_words_polymorhic[i], T->text)) {
      return i + 1;
    }
  }
  return 0;
}

static const char *tl_token_skip_namespace (struct tl_token *T) __attribute((warn_unused_result));
static const char *tl_token_skip_namespace (struct tl_token *T) {
  char *r = strchr (T->text, '.');
  if (r == NULL) {
    return T->text;
  }
  return r;
}

/*
static int tl_token_is_combinator_name (struct tl_token *T) {
  char *r = tl_token_skip_namespace (T);
  return islower(r[0]) ? 1 : 0;
}
*/

static int tl_token_is_type_name (struct tl_token *T) {
  const char *r = tl_token_skip_namespace (T);
  return isupper(r[0]) ? 1 : 0;
}

int tl_token_polymorphic_match (struct tl_token *L, struct tl_token *R) {
  assert (L != NULL && R != NULL);
  if (strcmp (L->text, R->text)) {
    return 0;
  }
  while (L && R) {
    L = L->next;
    R = R->next;
  }
  return (L == NULL && R == NULL) ? 1 : 0;
}

/********************* hash functions  *********************/

static int tl_expression_int_hash_compare (const void *a, const void *b) {
  const struct tl_expression *x = a, *y = b;
  return (x->magic < y->magic) ? -1 : ((x->magic > y->magic) ? 1 : 0);
}

static int tl_expression_combinator_hash_compare (const void *a, const void *b) {
  const struct tl_expression *x = a, *y = b;
  return strcmp (x->left->text, y->left->text);
}

static int tl_expression_right_name_hash_compare (const void *a, const void *b) {
  const struct tl_expression *x = a, *y = b;
  return strcmp (x->right_name, y->right_name);
}

static inline void tl_str_compute_hash (struct tl_hashmap *self, const char *s, int *h1, int *h2) {
  unsigned int p1 = 0, p2 = 0;
  while (*s) {
    p1 = 239 * p1 + (*s);
    p2 = 3 * p2 + (*s);
    s++;
  }
  *h1 = p1 % self->size;
  *h2 = 1 + (p2 % (self->size - 1));
}

static void tl_expression_int_hash_compute_hash (struct tl_hashmap *self, void *p, int *h1, int *h2) {
  const struct tl_expression *E = p;
  *h1 = ((unsigned) E->magic) % self->size;
  *h2 = 1 + (((unsigned) E->magic) % (self->size - 1));
}

static void tl_expression_combinator_hash_compute_hash (struct tl_hashmap *self, void *p, int *h1, int *h2) {
  const struct tl_expression *E = p;
  tl_str_compute_hash (self, E->left->text, h1, h2);
}

static void tl_expression_right_name_hash_compute_hash (struct tl_hashmap *self, void *p, int *h1, int *h2) {
  const struct tl_expression *E = p;
  tl_str_compute_hash (self, E->right_name, h1, h2);
}

/********************* cleanup routines *********************/
void tl_list_token_free (struct tl_token *L) {
  struct tl_token *E, *T;
  for (E = L; E != NULL; E = T) {
    T = E->next;
    cstr_free (&E->text);
    zfree (E, sizeof (*E));
  }
}

static void tl_expression_free (struct tl_expression *E) {
  cstr_free (&E->text);
  cstr_free (&E->right_name);
  tl_list_token_free (E->left); E->left = NULL;
  tl_list_token_free (E->right); E->right = NULL;
  zfree (E, sizeof (*E));
}

static void tl_list_expressions_free (struct tl_compiler *C, int section) {
  assert (section >= 0 && section < 2);
  struct tl_expression *L = &C->expr[section], *E, *T;
  for (E = L->next; E != L; E = T) {
    assert (E->section == section);
    T = E->next;
    tl_expression_free (E);
  }
  L->prev = L->next = L;
}

void tl_compiler_check (struct tl_compiler *C, char *msg, struct tl_expression *E) {
  int i;
  fprintf (stderr, "before %s (E: %p, E->text: %s)\n", msg, E, E->text);
  for (i = 0; i < 2; i++) {
    struct tl_expression *L = &C->expr[i], *E;
    for (E = L->next; E != L; E = E->next) {
      assert (E->section == i);
    }
  }
}

void tl_compiler_free (struct tl_compiler *C) {
  int i;
  for (i = 0; i < TL_MAX_ERRORS; i++) {
    cstr_free (&C->error_msg[i]);
  }
  tl_string_buffer_free (&C->tmp_error_buff);
  for (i = 0; i < 2; i++) {
    tl_list_expressions_free (C, i);
    tl_hashmap_free (&C->hm_magic[i]);
    tl_hashmap_free (&C->hm_combinator[i]);
  }
  tl_hashmap_free (&C->hm_composite_typename);
}

void tl_compiler_clear_errors (struct tl_compiler *C) {
  (void) tl_success (C, 0);
}

void tl_compiler_add_error (struct tl_compiler *C) {
  vkprintf (2, "%s (%p): %.*s\n", __func__, C, C->tmp_error_buff.pos, C->tmp_error_buff.buff);
  if (C->errors < TL_MAX_ERRORS) {
    C->error_msg[C->errors++] = tl_string_buffer_to_cstr (&C->tmp_error_buff);
    tl_string_buffer_clear (&C->tmp_error_buff);
  }
}

void tl_compiler_print_errors (struct tl_compiler *C, FILE *f) {
  int i;
  for (i = 0; i < C->errors; i++) {
    fprintf (f, "%s\n", C->error_msg[i]);
  }
}

/********************* error formating routines *********************/

int tl_success (struct tl_compiler *C, int old_errors) __attribute((warn_unused_result));
int tl_success (struct tl_compiler *C, int old_errors) {
  vkprintf (5, "tl_success (%p, %d)\n", C, old_errors);
  while (C->errors > old_errors) {
    C->errors--;
    if (C->error_msg[C->errors]) {
      cstr_free (&C->error_msg[C->errors]);
    }
  }
  return 0;
}

int tl_failf (struct tl_compiler *C, const char *format, ...) {
  const int old_errno = errno;
  tl_string_buffer_clear (&C->tmp_error_buff);
  va_list ap;
  va_start (ap, format);
  tl_string_buffer_vprintf (&C->tmp_error_buff, format, ap);
  va_end (ap);
  errno = old_errno;
  tl_compiler_add_error (C);
  return -1;
}

int tl_failfp (struct tl_compiler *C, const char *input, const char *cur, const char *format, ...) {
  const int old_errno = errno;
  int line = 0, column = 0;
  const char *s, *line_start = input;
  for (s = input; s != cur; s++) {
    if (*s == '\n') {
      line++;
      line_start = s;
      column = 0;
    } else {
      column++;
    }
  }
  tl_string_buffer_clear (&C->tmp_error_buff);
  tl_string_buffer_printf (&C->tmp_error_buff, "[%d:%d] ", line, column);
  va_list ap;
  va_start (ap, format);
  tl_string_buffer_vprintf (&C->tmp_error_buff, format, ap);
  va_end (ap);
  tl_string_buffer_append_char (&C->tmp_error_buff, '\n');
  int i;
  s = line_start;
  for (i = 0, s = line_start; i < column; i++, s++) {
    tl_string_buffer_append_char (&C->tmp_error_buff, *s);
  }
  if (column) {
    tl_string_buffer_append_char (&C->tmp_error_buff, '\n');
    for (i = 0; i < column; i++) {
      tl_string_buffer_append_char (&C->tmp_error_buff, ' ');
    }
  }
  while (*s && *s != '\n') {
    tl_string_buffer_append_char (&C->tmp_error_buff, *s++);
  }


  errno = old_errno;
  tl_compiler_add_error (C);
  return -1;
}

enum tl_schema_split_state {
  tsss_start_expression,
  tsss_after_slash,
  tsss_line_comment,
  tsss_expression
};

static void tl_add_expression (struct tl_compiler *C, int section, const char *text) {
  vkprintf (3, "tl_add_expression (section:%d, text:%s)\n", section, text);
  assert (section >= 0 && section < 2);
  struct tl_expression *E = zmalloc0 (sizeof (struct tl_expression));
  E->section = section;
  E->flag_builtin = C->flag_reading_builtin_schema;
  struct tl_expression *L = &C->expr[section];
  E->text = cstr_dup (text);
  struct tl_expression *A = L->prev;
  A->next = E; E->prev = A;
  E->next = L; L->prev = E;
}

static int tl_schema_split (struct tl_compiler *C, const char *input) {
  int section = 0;
  const char *s;
  struct tl_buffer b;
  tl_string_buffer_init (&b);

  enum tl_schema_split_state state = tsss_start_expression;
  for (s = input; *s; s++) {
    switch (state) {
    case tsss_start_expression:
      if (isspace (*s)) {
      } else if (*s == '/') {
        state = tsss_after_slash;
      } else if (isalpha (*s)) {
        b.pos = 0;
        tl_string_buffer_append_char (&b, *s);
        state = tsss_expression;
      } else if (*s == '-') {
        if (!strncmp (s, "---functions---", 15)) {
          if (++section > 1) {
            tl_string_buffer_free (&b);
            return tl_failfp (C, input, s, "too many ---functions--- sections");
          }
          s += 14;
        } else {
          tl_string_buffer_free (&b);
          return tl_failfp (C, input, s, "expected ---functions---");
        }
      } else {
        tl_string_buffer_free (&b);
        return tl_failfp (C, input, s, "illegal first expression's character (%c)", *s);
      }
    break;
    case tsss_after_slash:
      if (*s == '/') {
        state = tsss_line_comment;
      } else {
        tl_string_buffer_free (&b);
        return tl_failfp (C, input, s, "expected second slash, but %c found", *s);
      }
    break;
    case tsss_line_comment:
      if (*s == '\n') {
        state = tsss_start_expression;
      }
    break;
    case tsss_expression:
      if (*s == ';') {
        tl_string_buffer_append_char (&b, 0);
        tl_add_expression (C, section, b.buff);
        state = tsss_start_expression;
      } else if (isspace (*s)) {
        if (' ' != b.buff[b.pos-1]) {
          tl_string_buffer_append_char (&b, ' ');
        }
      } else {
        tl_string_buffer_append_char (&b, *s);
      }
    break;
    }
  }

  tl_string_buffer_free (&b);

  if (state == tsss_expression) {
    return tl_failfp (C, input, s, "last expression doesn't end by semicolon");
  }

  if (state == tsss_after_slash) {
    return tl_failfp (C, input, s, "found EOF, but expected second slash");
  }

  if (!section) {
    return tl_failfp (C, input, s, "don't find section '---functions---'");
  }

  return 0;
}

int tl_expression_remove_sugar (struct tl_compiler *C, struct tl_expression *E, char *buf) {
  vkprintf (4, "tl_expression_remove_sugar (\"%s\")\n", buf);
  char *s;
  int n = 0;
  for (s = buf; *s; s++) {
    if (*s == '<') {
      *s = ' ';
      n++;
    } else if (*s == '>') {
      *s = ' ';
      if (--n < 0) {
        return tl_failf (C, "tl_expression_remove_sugar: too many '>', expr: %s", E->text);
      }
    } else if (*s == ',') {
      if (n > 0) {
        *s = ' ';
      }
    }
  }
  if (n > 0) {
    return tl_failf (C, "tl_expression_remove_sugar: too many '<', expr: %s", E->text);
  }
  cstr_remove_extra_spaces (buf);
  vkprintf (4, "after removing sugar: %s\n", buf);
  return 0;
}

struct tl_token *tl_expresion_split (struct tl_compiler *C, struct tl_expression *E, const char *const expression, int remove_sugar) {
  int l = strlen (expression) + 1;
  char *s = zmalloc (l);
  strcpy (s, expression);

  if (remove_sugar) {
    if (tl_expression_remove_sugar (C, E, s) < 0) {
      return NULL;
    }
  }

  char *p;
  struct tl_token *head = NULL;
  for (p = strtok (s, " "); p != NULL; p = strtok (NULL, " ")) {
    struct tl_token *T = zmalloc0 (sizeof (struct tl_token));

#define ZHUKOV_BYTES_HACK
#ifdef ZHUKOV_BYTES_HACK
    /* dirty hack for Zhukov request */
    if (!strcmp (p, "bytes")) {
      T->text = cstr_dup ("string");
    } else if (!strcmp (p, "Bytes")) {
      T->text = cstr_dup ("String");
    } else {
      int l = strlen (p);
      if (l >= 6 && !strcmp (p + l - 6, ":bytes")) {
        T->text = zmalloc (l + 2);
        strcpy (T->text, p);
        strcpy (T->text + l - 6, ":string");
      } else if (l >= 6 && !strcmp (p + l - 6, ":Bytes")) {
        T->text = zmalloc (l + 2);
        strcpy (T->text, p);
        strcpy (T->text + l - 6, ":String");
      } else {
        T->text = cstr_dup (p);
      }
    }
#else
    T->text = cstr_dup (p);
#endif
    T->next = head;
    head = T;
  }
  zfree (s, l);
  return list_token_reverse (head);
}

char *cstr_join_with_sugar (struct tl_token *T) {
  int i, n, l;
  tl_token_length (T, &n, &l);
  assert (n >= 1);
  if (n == 1) {
    return cstr_dup (T->text);
  }
  l += n + 1;
  char *buf = tl_zzmalloc (l), *p = buf;
  for (i = 0; T != NULL; T = T->next, i++) {
    p += sprintf (p, "%s", T->text);
    *p++ = i ? ((i == n - 1) ? '>' : ',') : '<';
  }
  *p++ = 0;
  assert (p == buf + l);
  return buf;
}

void tl_expression_expand (struct tl_expression *E, struct tl_expression *R) {
  assert (R->type == tlet_polymorphic);
  assert (E->type == tlet_polymorphic_instance);
  struct tl_token *L = tl_token_clone (R->left);
  struct tl_token *x, *y, *z;
  for (x = L->next; x != NULL; x = x->next) {
    for (y = R->right->next, z = E->left->next; y != NULL; y = y->next, z = z->next) {
      if (!strcmp (x->text, y->text)) {
        cstr_free (&x->text);
        x->text = cstr_dup (z->text);
      }
    }
  }
  E->right = E->left;
  E->left = L;
  E->type = tlet_simple;
  E->right_name = cstr_join_with_sugar (E->right);
  E->flag_expanded = 1;
}

int tl_expression_is_polymorhic (struct tl_expression *E) {
  struct tl_token *u = E->right;
  if (u == NULL) {
    return 0;
  }
  if (!tl_token_is_type_name (u)) {
    return 0;
  }
  u = u->next;
  if (u == NULL) {
    return 0;
  }
  while (u != NULL) {
    if (!tl_token_is_variable_type (u)) {
      return 0;
    }
    u = u->next;
  }
  return 1;
}

int tl_expression_parse (struct tl_compiler *C, struct tl_expression *E) {
  char *p = strchr (E->text, '=');
  if (p == NULL) {
    E->left = tl_expresion_split (C, E, E->text, 1);
    if (E->left == NULL) {
      return -1;
    }
    E->right = NULL;
    E->type = tlet_polymorphic_instance;
    if (E->section) {
      return tl_failf (C, "polymorphic instance in the '---functions---' section, expr: %s", E->text);
    }
    struct tl_expression *A = C->expr[TL_SECTION_TYPES].next;
    while (A != E) {
      assert (A != &C->expr[TL_SECTION_TYPES]);
      if (A->type == tlet_polymorphic && tl_token_polymorphic_match (A->right, E->left)) {
        tl_expression_expand (E, A);
        return 0;
      }
      A = A->next;
    }
    return tl_failf (C, "don't find polymorphic rule, expr: %s", E->text);
  }
  if (p == E->text || p[-1] != ' ' || p[1] != ' ') {
    return tl_failf (C, "'=' should be surrounded by spaces, expr: %s", E->text);
  }
  if (strchr (p + 1, '=') != NULL) {
    return tl_failf (C, "'=' occures multiple times, expr: %s", E->text);
  }

  char *first = cstr_substr (E->text, 0, p - E->text - 1);
  E->left = tl_expresion_split (C, E, first, 0);
  cstr_free (&first);

  if (E->left == NULL) {
    return tl_failf (C, "empty lhs, expr: %s\n", E->text);
  }

  char *q = strchr (E->left->text, '#');
  if (q != NULL) {
    unsigned int magic = 0;
    char ch;
    int r = sscanf (q + 1, "%x%c", &magic, &ch);
    if (r != 1) {
      return tl_failf (C, "can't parse combinator magic number, expr: %s", E->text);
    }
    E->magic = magic;
    char *old = E->left->text;
    E->left->text = cstr_substr (old, 0, q - old);
    cstr_free (&old);
  }
  E->type = tlet_simple;
  E->right = tl_expresion_split (C, E, p + 1, E->section ? 0 : 1);
  if (E->right == NULL) {
    if (!C->errors) {
      return tl_failf (C, "empty rhs, expr: %s\n", E->text);
    }
    return -1;
  }

  if (E->section == TL_SECTION_FUNCTIONS) {
    E->right_name = cstr_join_with_sugar (E->right);
    return 0;
  }

  /* type declaration section */
  if (tl_expression_is_polymorhic (E)) {
    E->type = tlet_polymorphic;
    return 0;
  }

  if (E->right->next) {
    return tl_failf (C, "rhs contains more than one word, but it isn't polymorhic (args should be in ['alpha', 'beta', ...]), expr: %s\n", E->text);
  }

  E->right_name = cstr_join_with_sugar (E->right);

  return 0;
}

char *tl_expression_join (struct tl_compiler *C, struct tl_expression *E, int output_magic) {
  struct tl_token *T;
  struct tl_buffer b;
  tl_string_buffer_init (&b);
  for (T = E->left; T != NULL; T = T->next) {
    if (b.pos) {
      tl_string_buffer_append_char (&b, ' ');
    }
    tl_string_buffer_append_cstr (&b, T->text);
    if (output_magic && T == E->left) {
      tl_string_buffer_printf (&b, "#%x", E->magic);
    }
  }
  if (E->right) {
    tl_string_buffer_append_char (&b, ' ');
    tl_string_buffer_append_char (&b, '=');
    for (T = E->right; T != NULL; T = T->next) {
      tl_string_buffer_append_char (&b, ' ');
      tl_string_buffer_append_cstr (&b, T->text);
    }
  }
  tl_string_buffer_append_char (&b, 0);
  int r = tl_expression_remove_sugar (C, E, b.buff);
  if (r < 0) {
    tl_string_buffer_free (&b);
    return NULL;
  }
  char *res = cstr_dup (b.buff);
  tl_string_buffer_free (&b);
  return res;
}

int tl_expression_compute_magic (struct tl_compiler *C, struct tl_expression *E) {
  if (E->type != tlet_simple) {
    return 0;
  }
  unsigned m = compute_crc32 (E->text, strlen (E->text));
  if (E->magic && E->magic != m) {
     return tl_failf (C, "tl_expression_compute_magic: magic in schema (0x%x) isn't equal to computed magic (0x%x), expr: %s", E->magic, m, E->text);
  }
  E->magic = m;
  return 0;
}

char *tl_expression_get_vector_item_type (struct tl_expression *E) {
  if (E->right == NULL) {
    return NULL;
  }
  struct tl_token *T = E->right;
  if (T == NULL) {
    return NULL;
  }
  T = T->next;
  if (T->next != NULL) {
    return NULL;
  }
  return T->text;
}

int tl_list_expressions_parse (struct tl_compiler *C, struct tl_expression *L) {
  struct tl_expression *E, *W;
  for (E = L->next; E != L; E = W) {
    W = E->next;
    if (tl_expression_parse (C, E) < 0) {
      return -1;
    }
    char *t = tl_expression_join (C, E, 0);
    if (t == NULL) {
      return -1;
    }
    cstr_free (&E->text);
    E->text = t;
    if (tl_expression_compute_magic (C, E) < 0) {
      return -1;
    }
    if (E->type == tlet_simple) {
      struct tl_expression *A = tl_hashmap_get_f (&C->hm_magic[E->section], E, 1);
      if (A != E) {
        if (!strcmp (A->text, E->text)) {
          if (verbosity >= 1) {
            fprintf (stderr, "duplicate expression: %s\n", E->text);
          }
          struct tl_expression *u = E->prev, *v = E->next;
          u->next = v; v->prev = u;
          tl_expression_free (E);
          continue;
        } else {
          return tl_failf (C, "magic collision for expressions %s and %s", A->text, E->text);
        }
      }

      if (!(E->flag_expanded)) {
        A = tl_hashmap_get_f (&C->hm_combinator[E->section], E, 1);
        if (A != E) {
          return tl_failf (C, "combinator collision for expressions %s and %s", A->text, E->text);
        }
      }

      if (E->type == tlet_simple && E->right_name != NULL && E->section == TL_SECTION_TYPES) {
        struct tl_expression *A = tl_hashmap_get_f (&C->hm_composite_typename, E, 1);
        if (A == E) {
          E->rtail = E;
        } else {
          A->rtail->rnext = E;
          A->rtail = E;
        }
        E->rnext = NULL;
      }
    }
  }
  return 0;
}

struct tl_expression *tl_expression_find_by_magic (struct tl_compiler *C, int magic) {
  int i;
  struct tl_expression T;
  T.magic = magic;
  for (i = 0; i < 2; i++) {
    struct tl_expression *E = tl_hashmap_get_f (&C->hm_magic[i], &T, 0);
    if (E) {
      return E;
    }
  }
  return NULL;
}

struct tl_expression *tl_list_expressions_find_by_combinator_name (struct tl_compiler *C, int section, char *const combinator_name, const char *type_name) {
  if (type_name != NULL && !strcmp (type_name, "Object")) {
    type_name = NULL;
  }
  assert (section >= 0 && section < 2);
  struct tl_expression TE, *E, *L;
  struct tl_token T;
  T.text = combinator_name;
  TE.left = &T;
  E = tl_hashmap_get_f (&C->hm_combinator[section], &TE, 0);
  if (E && (type_name == NULL || !strcmp (E->right_name, type_name))) {
    return E;
  }

  if (section == TL_SECTION_TYPES) {
    L = &C->expr[section];
    for (E = L->next; E != L; E = E->next) {
      assert (E->left);
      if (E->type == tlet_simple && E->flag_expanded && !strcmp (E->left->text, combinator_name) && (type_name == NULL || !strcmp (E->right_name, type_name))) {
        return E;
      }
    }
  }

  return NULL;
}

struct tl_expression *tl_expression_find_first_by_composite_typename (struct tl_compiler *C, char *composite_typename) {
  struct tl_expression T = { .right_name = composite_typename };
  return tl_hashmap_get_f (&C->hm_composite_typename, &T, 0);
}

/*
struct tl_expression *tl_list_expressions_find_next_type (struct tl_expression *cur, struct tl_expression *end, const char *const name) {
  struct tl_expression *E;
  for (E = cur->next; E != end; E = E->next) {
    if (E->type == tlet_simple && E->right_name) {
      if (verbosity >= 5) {
        fprintf (stderr, "strcmp (\"%s\", \"%s\")\n", E->right_name, name);
      }
      if (!strcmp (E->right_name, name)) {
        return E;
      }
    }
  }
  return end;
}
*/

void tl_list_expressions_init (struct tl_expression *E) {
  vkprintf (4, "tl_list_expressions_init (%p)\n", E);
  E->prev = E->next = E;
}

void tl_compiler_init (struct tl_compiler *C) {
  int i;
  if (C->magic == TL_COMPILER_INITIALIZED_MAGIC) {
    return;
  }
  memset (C, 0, sizeof (*C));
  C->magic = TL_COMPILER_INITIALIZED_MAGIC;
  tl_string_buffer_init (&C->tmp_error_buff);
  for (i = 0; i < 2; i++) {
    tl_list_expressions_init (&C->expr[i]);
    C->hm_magic[i] = tl_hashmap_alloc (TL_MIN_HASHMAP_SIZE);
    C->hm_magic[i]->compare = tl_expression_int_hash_compare;
    C->hm_magic[i]->compute_hash = tl_expression_int_hash_compute_hash;
    C->hm_combinator[i] = tl_hashmap_alloc (TL_MIN_HASHMAP_SIZE);
    C->hm_combinator[i]->compare = tl_expression_combinator_hash_compare;
    C->hm_combinator[i]->compute_hash = tl_expression_combinator_hash_compute_hash;
  }
  C->hm_composite_typename = tl_hashmap_alloc (TL_MIN_HASHMAP_SIZE);
  C->hm_composite_typename->compare = tl_expression_right_name_hash_compare;
  C->hm_composite_typename->compute_hash = tl_expression_right_name_hash_compute_hash;
  C->flag_output_magic = 1;
}

int tl_schema_read (struct tl_compiler *C, const char *input) {
  tl_compiler_init (C);
  C->flag_reading_builtin_schema = 1;
  int r = tl_schema_split (C, tl_builtin_shema);
  assert (!r);
  C->flag_reading_builtin_schema = 0;

  r = tl_schema_split (C, input);
  if (r < 0) {
    return r;
  }

  int i;
  for (i = 0; i < 2; i++) {
    r = tl_list_expressions_parse (C, &C->expr[i]);
    if (r < 0) {
      return r;
    }
  }

  return 0;
}

char *tl_readfile (struct tl_compiler *C, const char *const filename) {
  int fd = open (filename, O_RDONLY);
  if (fd < 0) {
    tl_failf (C, "open (\"%s\") fail. %m", filename);
    return NULL;
  }
  struct stat b;
  if (fstat (fd, &b) < 0) {
    tl_failf (C, "fstat (\"%s\") fail. %m", filename);
    close (fd);
    return NULL;
  }
  if (b.st_size > 0x1000000) {
    tl_failf (C, "\"%s\" file too big", filename);
    close (fd);
    return NULL;
  }
  char *a = malloc (b.st_size + 1);
  assert (a);
  assert (read (fd, a, b.st_size) == b.st_size);
  a[b.st_size] = 0;
  return a;
}

int tl_schema_read_from_file (struct tl_compiler *C, const char *const filename) {
  tl_compiler_init (C);
  char *a = tl_readfile (C, filename);
  if (a == NULL) {
    return -1;
  }
  int r = tl_schema_read (C, a);
  free (a);

  return r;
}

void tl_expression_write_to_file (FILE *f, struct tl_compiler *C, struct tl_expression *E) {
  int output_magic = C->flag_output_magic;
  if (E->type != tlet_simple) {
    output_magic = 0;
  }
  char *t = NULL;
  if (output_magic) {
    t = tl_expression_join (C, E, 1);
    assert (t != NULL);
  }
  fprintf (f, "%s;\n", t ? t : E->text);
  if (t != NULL) {
    cstr_free (&t);
  }
}

void tl_write_list_expressions_free_to_file (FILE *f, struct tl_compiler *C, struct tl_expression *L) {
  struct tl_expression *E;
  int old_output_magic = C->flag_output_magic;
  C->flag_output_magic = 1;
  for (E = L->next; E != L; E = E->next) {
    tl_expression_write_to_file (f, C, E);
  }
  C->flag_output_magic = old_output_magic;
}

int tl_write_expressions_to_file (struct tl_compiler *C, const char *const filename) {
  FILE *f = fopen (filename, "w");
  if (f == NULL) {
    return tl_failf (C, "fopen (\"%s\", \"w\") fail. %m", filename);
  }
  tl_write_list_expressions_free_to_file (f, C, &C->expr[TL_SECTION_TYPES]);
  fprintf (f, "---functions---\n");
  tl_write_list_expressions_free_to_file (f, C, &C->expr[TL_SECTION_FUNCTIONS]);
  fclose (f);
  return 0;
}

static void tl_expression_dfs_visit (struct tl_compiler *C, struct tl_expression *E);

static void dfs_typename_visit (struct tl_compiler *C, char *typename) {
  if (typename == NULL) {
    return;
  }
  struct tl_expression *E = tl_expression_find_first_by_composite_typename (C, typename);
  while (E != NULL) {
    tl_expression_dfs_visit (C, E);
    E = E->rnext;
  }
}

static void tl_expression_dfs_visit (struct tl_compiler *C, struct tl_expression *E) {
  if (E->flag_visited) {
    return;
  }
  E->flag_visited = 1;
  struct tl_token *T;
  for (T = E->left->next; T != NULL; T = T->next) {
    char *q = strchr (T->text, ':');
    if (q == NULL) {
      dfs_typename_visit (C, q);
    } else {
      dfs_typename_visit (C, q+1);
    }
  }

  if (E->flag_expanded) {
    for (T = E->right->next; T != NULL; T = T->next) {
      dfs_typename_visit (C, T->text);
    }
  }

  dfs_typename_visit (C, E->right_name);
}

int tl_function_help (struct tl_compiler *C, char *rpc_function_name, FILE *f) {
  struct tl_expression *F = tl_list_expressions_find_by_combinator_name (C, TL_SECTION_FUNCTIONS, rpc_function_name, NULL);
  if (F == NULL) {
    return -1;
  }
  tl_expression_dfs_visit (C, F);
  F->flag_visited = 0;
  struct tl_expression *E;
  for (E = C->expr[TL_SECTION_TYPES].next; E != &C->expr[TL_SECTION_TYPES]; E = E->next) {
    if (E->flag_visited) {
      tl_expression_write_to_file (f, C, E);
      E->flag_visited = 0;
    }
  }
  tl_expression_write_to_file (f, C, F);
  return 0;
}

int tl_schema_print_unused_types (struct tl_compiler *C, FILE *f) {
  struct tl_expression *L = &C->expr[TL_SECTION_FUNCTIONS], *E;
  for (E = L->next; E != L; E = E->next) {
    tl_expression_dfs_visit (C, E);
  }
  for (E = L->next; E != L; E = E->next) {
    E->flag_visited = 0;
  }
  L = &C->expr[TL_SECTION_TYPES];
  for (E = L->next; E != L; E = E->next) {
    if (!E->flag_visited && !E->flag_builtin) {
      tl_expression_write_to_file (f, C, E);
    }
  }
  return 0;
}

char *tl_expression_get_argument_type (struct tl_expression *E, char *arg_name) {
  if (E == NULL || arg_name == NULL) {
    return NULL;
  }
  struct tl_token *T = E->left;
  if (T == NULL) {
    return NULL;
  }
  int l = strlen (arg_name);
  T = T->next;
  while (T != NULL) {
    if (!strncmp (T->text, arg_name, l) && T->text[l] == ':') {
      return T->text + (l + 1);
    }
    T = T->next;
  }
  return NULL;
}
