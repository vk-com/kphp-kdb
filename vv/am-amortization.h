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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#ifndef __AM_AMORTIZATION__
#define __AM_AMORTIZATION__

#include <math.h>

#define AMORTIZATION_TABLE_SQRT_SIZE_BITS 9
#define AMORTIZATION_TABLE_SQRT_SIZE (1<<AMORTIZATION_TABLE_SQRT_SIZE_BITS)
#define AMORTIZATION_TABLE_SQRT_SIZE_MASK (AMORTIZATION_TABLE_SQRT_SIZE-1)

extern int tot_amortization_tables;

typedef struct  {
  double hi_exp[AMORTIZATION_TABLE_SQRT_SIZE], lo_exp[AMORTIZATION_TABLE_SQRT_SIZE];
  double c;
  int T;
  int refcnt;
} time_amortization_table_t;

time_amortization_table_t *time_amortization_table_alloc (int T);
void time_amortization_table_free (time_amortization_table_t **p);

extern inline double time_amortization_table_fast_exp (time_amortization_table_t *self, int dt) {
  return (dt < AMORTIZATION_TABLE_SQRT_SIZE * AMORTIZATION_TABLE_SQRT_SIZE) ?
          self->hi_exp[dt >> AMORTIZATION_TABLE_SQRT_SIZE_BITS] * self->lo_exp[dt & AMORTIZATION_TABLE_SQRT_SIZE_MASK] :
          exp (self->c * dt);
}
#endif
