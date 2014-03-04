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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "dl-default.h"


/*
 *
 * pair <ll, int>
 *
 */
#define TA ll
#define TB int
#define TNAME shmap_pair_ll_int
#include "dl-pair.c"
#define dl_hash_shmap_pair_ll_int(p) ((p).x * 659203823431245141ll + 123456789)
#include "dl-undef.h"

/*
 *
 * strict_hash_map <ll, int>
 *
 */
#define DATA_T shmap_pair_ll_int
#define TNAME shmap_ll_int
#define IMPLEMENTATION OPEN
#define STORE_HASH OFF
#define STRICT OFF
#define WAIT_FREE OFF
#define EMPTY(p) ((p).x == 0)

#include "dl-hashtable.c"

#include "dl-undef.h"


/*hmap_ll_int h;
hmap_ll_vptr vh;*/

shmap_ll_int h;
/*
void dump() {
  fprintf (stderr, "DUMP\n");
  fprintf (stderr, "hn = %d\n", h.size);
  int i;
  for (i = 0; i < h.size; i++) {
   fprintf (stderr, "%d: %lld(%d) %d\n", i, h.e[i].data.x, (unsigned int)(h.e[i].data.x * 659203823431245141ll) % h.size, h.e[i].data.y);
  }
  fprintf (stderr, "[------------------------------]\n");
} */

int main (void) {
  shmap_ll_int_init (&h);
//  shmap_ll_int_set_size (&h, 1);
  char op[50];
  int tn = 0;
  while (scanf ("%s", op) == 1) {
    tn++;
    if (tn % 1000 == 0) {
      fprintf (stderr, "%d\n", tn);
    }
    ll x;
    int val;
    if (op[0] == 's') {
      scanf ("%lld%d", &x, &val);

      shmap_pair_ll_int p;
      p.x = x;
      shmap_ll_int_add (&h, p)->y = val;
    } else if (op[0] == 'd') {
      scanf ("%lld", &x);

/*      if (x == 7) {
        dump();
      }*/

      shmap_pair_ll_int p;
      p.x = x;
      shmap_ll_int_del (&h, p);
/*
      if (x == 7) {
        dump();
      }        */

    } else if (op[0] == 'g') {
      scanf ("%lld%d", &x, &val);

      shmap_pair_ll_int p, *r;
      p.x = x;
      r = shmap_ll_int_get (&h, p);
      int cval = -1;
      if (r != NULL) {
        cval = r->y;
      }
      if (val != cval) {
        fprintf (stderr, "%d vs %d\n", val, cval);
      }
      assert (val == cval);
    }
//    fprintf (stderr, "%s %lld %d\n", op, x, val);
//    fflush (stdout);

    int cnt;
    scanf ("%d", &cnt);
    //assert (cnt == h.used);
  }


/*  hmap_ll_int_init (&h);
  hmap_ll_int_set_size (&h, 20);

  hmap_ll_vptr_init (&vh);
  hmap_ll_vptr_set_size (&vh, 20);

  int i;
  for (i = 1; i < 300000; i++) {
    hmap_pair_ll_int p;
    p.x = i * 1000000000ll;
    p.y = i;
    hmap_ll_int_add (&h, p);
  }
  for (i = 1; i < 300000; i++) {
    hmap_pair_ll_int p, *r;
    p.x = i * 1000000000ll;
    r = hmap_ll_int_get (&h, p);
    assert (p.x == r->x);
    assert (p.x == r->y * 1000000000ll);
//    fprintf (stderr, "%lld(%lld->%d)\n", p.x, r->x, r->y);
  }*/

  return 0;
}
