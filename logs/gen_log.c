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

#include <stdio.h>
#include <string.h>

struct lev_start {
  int type;
  int schema_id;
  int extra_bytes;
  int split_mod;
  int split_min;
  int split_max;
  char str[4];		
} a;

int main (void) {
  FILE *f = fopen ("index.bin", "wb");

  memset (&a, 0, sizeof (a));
  a.type = 0x044c644b;
  a.schema_id = 0x21090101;
  a.split_mod = 1;
  a.split_min = 0;
  a.split_max = 1;

  fwrite (&a, sizeof (a) - 4, 1, f);

  fclose (f);
  return 0;
}