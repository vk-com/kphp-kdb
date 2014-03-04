/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2010-2011 Vkontakte Ltd
              2010      Nikolai Durov
              2010      Andrei Lopatin
	      2011	Anton Maydell
*/

#include <ctype.h>
#include <assert.h>
#include "net-parse.h"


void nb_reader_set (struct nb_reader *I, struct netbuffer *In, const int bytes) {
  I->bytes = bytes;
  I->bytes_read = 0;
  nbit_set (&I->it, In);
  I->p = I->ptr_s = nbit_get_ptr (&I->it);
  I->ptr_e = I->ptr_s + nbit_ready_bytes (&I->it);
}

int nb_reader_getc (struct nb_reader *I) {
  if (I->bytes_read >= I->bytes) {
    return -1;
  }
  if (I->p >= I->ptr_e) {
    nbit_advance (&I->it, I->ptr_e - I->ptr_s);
    I->p = I->ptr_s = nbit_get_ptr (&I->it);
    I->ptr_e = I->ptr_s + nbit_ready_bytes (&I->it);
  }
  assert (I->p < I->ptr_e);
  I->bytes_read++;
  return *(I->p)++;
}

int nb_reader_parse_int (struct nb_reader *I, int *x, int *c) {
  int sgn = 0, r = 0;
  *c = nb_reader_getc (I);
  if (*c == '-') {
    sgn = 1;
    *c = nb_reader_getc (I);
  } 
  
  if (*c < 0 || !isdigit(*c)) {
    return 0; 
  }
  
  do {
    if (r > 0x7fffffff / 10) {
      return 0;
    }
    r = r * 10 + (*c - '0');
    if (r < 0) {
      return 0;
    }
    *c = nb_reader_getc (I);
  } while (*c >= 0 && isdigit(*c));

  if (sgn) {
    r = -r;
  }
  *x = r;
  return 1;
}

int np_news_parse_list (int *Res, const int max_size, const int arity, netbuffer_t *In, const int bytes) {
  if (!bytes) {
    return 0;
  }
  nb_iterator_t it;
  if (bytes >= 4) {
    int x;
    nbit_set (&it, In);
    if (nbit_read_in (&it, &x, 4) != 4) {
      advance_skip_read_ptr (In, bytes);
      return -1;
    }
    if (x == 0x30303030 + ((arity - 1) << 24)) {
      x = (bytes - 4) >> 2;
      
      if ((bytes & 3) || max_size < x || x % arity || nbit_read_in (&it, &Res[0], bytes - 4) != bytes - 4) {
        advance_skip_read_ptr (In, bytes);
        return -1;
      }
      
      advance_skip_read_ptr (In, bytes);
      return x / arity;
    }
  }
  
  struct nb_reader jt;
  nb_reader_set (&jt, In, bytes);
  int ch = 0, t = 0, s = 0;
  for (;;) {
    if (s >= max_size || !nb_reader_parse_int (&jt, &Res[s++], &ch)) {
      advance_skip_read_ptr (In, bytes);
      return -1;
    }
    if (ch < 0) {
      break;
    }
    if (++t == arity) {
      t = 0;
    }
    if (ch != (t ? '_' : ',')) {
      advance_skip_read_ptr (In, bytes);
      return -1;
    }
  }
  advance_skip_read_ptr (In, bytes);
  return (s % arity) ? -1 : s / arity;
}

int np_parse_list_str (int *Res, const int max_size, const int arity, const char *ptr, const int bytes) {
  const char *ptr_e = ptr + bytes;
  int s = 0, t = 0, x, sgn;

  while (ptr < ptr_e) {
    x = 0;
    sgn = 0;

    if (*ptr == '-') {
      ptr++;
      sgn = 1;
    }
    
    if (!isdigit (*ptr)) {
      return -1;
    }

    while (ptr < ptr_e && isdigit (*ptr)) {
      if (x > 0x7fffffff / 10) {
        return -1;
      }
      x = x*10 + (*ptr++ - '0');
      if (x < 0) {
        return -1;
      }
    }
    if (sgn) {
      x = -x;
    }
    if (++t == arity) {
      t = 0;
    }
    if (s >= max_size || (ptr < ptr_e && *ptr != (t ? '_' : ','))) {
      return -1;
    }
    Res[s++] = x;
    if (ptr == ptr_e) {
      return t ? -1 : s / arity;
    }
    assert (*ptr == (t ? '_' : ','));
    ptr++;
  };

  assert (ptr == ptr_e);
  return t ? -1 : s / arity;
}

