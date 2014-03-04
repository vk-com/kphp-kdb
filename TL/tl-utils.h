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

#ifndef __TL_UTILS_H__
#define __TL_UTILS_H__

#include <stdarg.h>

extern int verbosity;

void *tl_zzmalloc (int size);
void tl_zzfree (void *src, int size);

char *cstr_dup (const char *const input);
char *cstr_substr (const char *const input, int start, int end);
void cstr_free (char **s);
/* NOTICE: buf shouldn't be allocated by zmalloc */
/* removes double, leading, trailing spaces */
void cstr_remove_extra_spaces (char *buf);

#define TL_STRING_BUFFER_MIN_SIZE 4096

struct tl_buffer {
  char *buff;
  int pos;
  int size;
};

struct tl_int_array {
  int *buff;
  int pos;
  int size;
};

void tl_string_buffer_init (struct tl_buffer *b);
void tl_string_buffer_free (struct tl_buffer *b);
void tl_string_buffer_clear (struct tl_buffer *b);
void tl_string_buffer_append_char (struct tl_buffer *b, char ch);
void tl_string_buffer_append_cstr (struct tl_buffer *b, const char *s);
void tl_string_buffer_printf (struct tl_buffer *b, const char *format, ...) __attribute__ ((format (printf, 2, 3)));
void tl_string_buffer_vprintf (struct tl_buffer *b, const char *format, va_list ap);
char *tl_string_buffer_to_cstr (struct tl_buffer *b) __attribute((warn_unused_result));

int tl_int_array_mark (struct tl_int_array *a);
void tl_int_array_release (struct tl_int_array *a, int mark);
void tl_int_array_init (struct tl_int_array *a, int *buff, int size);
int tl_int_array_append (struct tl_int_array *a, int i) __attribute((warn_unused_result));
int tl_int_array_append_long (struct tl_int_array *a, long long l);
int tl_int_array_append_double (struct tl_int_array *a, double d);
int tl_int_array_append_string (struct tl_int_array *a, char *s);

int tl_fetch_string (int *in_ptr, int ilen, char **s, int *slen, int allocate_new_cstr);

#define TL_MIN_HASHMAP_SIZE 1000

struct tl_hashmap {
  void **h;
  int (*compare)(const void *, const void *);
  void (*compute_hash)(struct tl_hashmap *self, void *p, int *h1, int *h2);
  int size;
  int filled;
  int n;
};

struct tl_hashmap *tl_hashmap_alloc (int n);
void tl_hashmap_free (struct tl_hashmap **V);
void *tl_hashmap_get_f (struct tl_hashmap **V, void *p, int force);
#endif

