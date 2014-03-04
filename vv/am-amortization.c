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

#include <stdlib.h>
#include <assert.h>
#include "am-amortization.h"
#include "kdb-data-common.h"

int tot_amortization_tables;

time_amortization_table_t *time_amortization_table_alloc (int T) {
  int i;
  time_amortization_table_t *self = zmalloc (sizeof (time_amortization_table_t));
  self->refcnt = 1;
  self->c = -(M_LN2 / T);
  self->T = T;
  const double hi_mult = exp (self->c * AMORTIZATION_TABLE_SQRT_SIZE), lo_mult = exp (self->c);
  self->hi_exp[0] = self->lo_exp[0] = 1.0;
  for (i = 1; i < AMORTIZATION_TABLE_SQRT_SIZE; i++) {
    self->hi_exp[i] = self->hi_exp[i-1] * hi_mult;
    self->lo_exp[i] = self->lo_exp[i-1] * lo_mult;
  }
  return self;
}

void time_amortization_table_free (time_amortization_table_t **p) {
  if (p == NULL) {
    return;
  }
  if (*p == NULL) {
    return;
  }
  (*p)->refcnt--;
  assert ((*p)->refcnt >= 0);
  if (!(*p)->refcnt) {
    zfree (*p, sizeof (time_amortization_table_t));
    tot_amortization_tables--;
  }
  *p = NULL;
}

double time_amortization_table_fast_exp (time_amortization_table_t *self, int dt) {
  return (dt < AMORTIZATION_TABLE_SQRT_SIZE * AMORTIZATION_TABLE_SQRT_SIZE) ?
          self->hi_exp[dt >> AMORTIZATION_TABLE_SQRT_SIZE_BITS] * self->lo_exp[dt & AMORTIZATION_TABLE_SQRT_SIZE_MASK] :
          exp (self->c * dt);
}
