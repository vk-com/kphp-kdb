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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#pragma once

#define get_code_len(d) (((2 * (d)) + 7) / 8)
#define get_used_len(d) ((((((2 * (d)) + 7) / 8) + 3) & -4))
#define get_sums_len(d) (((2 * (d)) / 64 + 1) * sizeof (int))

#define get_bit(code, i) ((((char *)code)[i >> 3] >> (i & 7)) & 1)
#define set_bit(code, i) ((char *)code)[i >> 3] |= (1 << (i & 7))
//#define get_2bits(code, i) ((code[i >> 2] >> ((i & 3) << 1)) & 3)
//#define set_2bits(code, i, x) (code[i >> 2] = (code[i >> 2] & ~(3 << ((i & 3) << 1))) | (x << ((i & 3) << 1)))

// hash from certain n strings to 0..(2*d - 1)
// hash(s) =
//           h0 = poly_h(s, mul0) % d;
//           h1 = poly_h(s, mul1) % d + d;
//           if (get_bit(code, h0) ^ get_bit(code, h1))
//              return h1;
//           return h0;
typedef struct {
  unsigned char *code;
  int *sums;
  int *used;
  int mul0, mul1, d;
} perfect_hash;

void ph_init (perfect_hash *h);
void ph_free (perfect_hash *h);
void ph_generate (perfect_hash *h, long long *s, int n);
int ph_h (perfect_hash *h, long long s);

int ph_encode (perfect_hash *h, unsigned char *s);
int ph_decode (perfect_hash *h, unsigned char *s);

