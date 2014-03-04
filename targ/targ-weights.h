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

    Copyright 2013 Vkontakte Ltd
              2013 Anton Maydell
*/

#ifndef __TARG_WEIGHTS_H__
#define __TARG_WEIGHTS_H__

extern const int weights_coords;
extern int targ_weights_last_update_time, tot_weights_vectors;
extern long long tot_weights_vector_bytes, weights_small_updates, weights_updates;
#pragma	pack(push,4)
typedef struct {
  int relaxation_time;
  double values[0];
} targ_weights_vector_t;
#pragma	pack(pop)

targ_weights_vector_t *targ_weights_vector_alloc (void);
void targ_weights_vector_free (targ_weights_vector_t *V);
int targ_weights_create_target (const char *address);

int targ_weights_small_update (int vector_id, int coord_id, int relaxation_time, int value);
int targ_weights_update (int vector_id, int relaxation_time, int coords, int *values);
double targ_weights_at (targ_weights_vector_t *weights, int coord_id);

#endif
