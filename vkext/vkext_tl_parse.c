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
              2012-2013 Vitaliy Valtman
*/

#include <php.h>
#include "vkext.h"
#include "vkext_rpc.h"

struct tl {
  char *error;
  int errnum;
};
struct tl tl;

void tl_parse_init (void) {
  if (tl.error) {
    free (tl.error);
    tl.error = 0;
  }
  tl.errnum = 0;
}

int tl_parse_int (void) {
  if (tl.error) { return -1; }
  return do_rpc_fetch_int (&tl.error);
}

long long tl_parse_long (void) {
  if (tl.error) { return -1; }
  return do_rpc_fetch_long (&tl.error);
}

double tl_parse_double (void) {
  if (tl.error) { return -1; }
  return do_rpc_fetch_double (&tl.error);
}

int tl_parse_string (char **s) {
  if (tl.error) { return -1; }
  int len = do_rpc_fetch_string (s);
  if (len < 0) {
    tl.error = strdup (*s);
    *s = 0;
    return -1;
  }
  char *t = malloc (len + 1);
  memcpy (t, *s, len);
  t[len] = 0;
  *s = t;
  return len;
}

int tl_eparse_string (char **s) {
  if (tl.error) { return -1; }
  int len = do_rpc_fetch_string (s);
  if (len < 0) {
    tl.error = strdup (*s);
    *s = 0;
    return -1;
  }
  char *t = emalloc (len + 1);
  memcpy (t, *s, len);
  t[len] = 0;
  *s = t;
  return len;
}

char *tl_parse_error (void) {
  return tl.error;
}

void tl_set_error (const char *error) {
  if (tl.error) { return; }
  tl.error = strdup (error);
}

void tl_parse_end (void) {
  if (tl.error) { return; }
  if (!do_rpc_fetch_eof (&tl.error)) {
    tl_set_error ("Extra data");
  }
}

int tl_parse_save_pos (void) {
  char *error = 0;
  int r = do_rpc_fetch_get_pos (&error);
  if (error) {
    tl_set_error (error);
    return 0;
  } else {
    return r;
  }
}

int tl_parse_restore_pos (int pos) {
  char *error = 0;
  do_rpc_fetch_set_pos (pos, &error);
  if (error) {  
    tl_set_error (error);
    return 0;
  } else {
    return 1;
  }
}

void tl_parse_clear_error (void) {
  if (tl.error) {
    free (tl.error);
    tl.error = 0;
  }
}
