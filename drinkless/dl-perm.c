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

#include "dl-perm.h"

void _go (dl_prm_ptr v, int d);
void _go_rev (dl_prm_ptr v, int d);
void dl_perm_dbg (dl_perm *p);



dl_prm_ptr dl_alloc_prm (void) {
  dl_prm_ptr res =  dl_malloc0 (sizeof (dl_prm));
  return res;
}

void dl_free_prm (dl_prm_ptr v) {
  dl_free (v, sizeof (dl_prm));
}

void dl_prm_free (dl_prm_ptr v) {
  if (v == NULL) {
    return;
  }
  dl_prm_free (v->l);
  dl_prm_free (v->r);
  dl_free_prm (v);
}

void dl_prm_rev_split (dl_prm_ptr v, int x, dl_prm_ptr *a, dl_prm_ptr *b) {
  while (v != NULL) {
    if (v->a < x) {
      *a = v;
      a = &v->xr;
      v = v->xr;
    } else {
      *b = v;
      b = &v->xl;
      v = v->xl;
    }
  }
  *a = *b = NULL;
}


void dl_prm_rev_merge (dl_prm_ptr *tr, dl_prm_ptr a, dl_prm_ptr b) {
  while (a != NULL || b != NULL) {
     if (a == NULL || (b != NULL && b->y > a->y)) {
       *tr = b;
       tr = &b->xl;
       b = b->xl;
     } else {
       *tr = a;
       tr = &a->xr;
       a = a->xr;
     }
  }
  *tr = NULL;
}

void dl_prm_rev_add (dl_prm_ptr *v, dl_prm_ptr u) {
  while (*v != NULL && (*v)->y >= u->y) {
    if ((*v)->a < u->a) {
      v = &(*v)->xr;
    } else {
      v = &(*v)->xl;
    }
  }
  dl_prm_rev_split (*v, u->a, &u->xl, &u->xr);
  *v = u;
}

dl_prm_ptr dl_prm_rev_del (dl_prm_ptr *v, int x) {
  //fprintf (stderr, "del %d\n", x);
  //_go_rev (*v, 0);
  while (*v != NULL) {
    //fprintf (stderr, "?%d\n", (*v)->a);
    if ((*v)->a  == x) {
      dl_prm_ptr t = *v;
      //fprintf (stderr, "found %d %d\n", (*v)->a, (*v)->b);
      dl_prm_rev_merge (v, t->xl, t->xr);

      return t;
    } else if ((*v)->a < x) {
      v = &(*v)->xr;
    } else if ((*v)->a > x) {
      v = &(*v)->xl;
    }
  }
  return NULL;
}


#define LEN(v) ((v) ? (v)->len : 0)


void dl_prm_fix (dl_prm_ptr t) {
  while (t != NULL) {
    t->len = t->b - t->a + LEN(t->l) + LEN(t->r);
    t = t->p;
  }
}

void fix (dl_prm_ptr *a, dl_prm_ptr *b, dl_prm_ptr *rv) {
  if (*a && *b) {
    while ((*a)->r) {
      a = &(*a)->r;
    }
    while ((*b)->l) {
      b = &(*b)->l;
    }
    if ((*a)->b == (*b)->a) {
      dl_prm_ptr tmp = *b;
      *b = tmp->r;

      (*a)->b = tmp->b;

      dl_prm_fix (tmp->p);
      assert (dl_prm_rev_del (rv, tmp->a) == tmp);
      dl_free_prm (tmp);

      dl_prm_fix (*a);
    }
  }
}

void dl_prm_merge (dl_prm_ptr *tr, dl_prm_ptr a, dl_prm_ptr b, dl_prm_ptr *rv) {
  dl_prm_ptr p = NULL;

  fix (&a, &b, rv);

  while (a || b) {
    if (b == NULL || (a != NULL && a->y > b->y)) {
      a->len += LEN(b);

      a->p = p;
      p = a;

      *tr = a;
      tr = &a->r;
      a = a->r;
    } else {
      b->len += LEN(a);

      b->p = p;
      p = b;

      *tr = b;
      tr = &b->l;
      b = b->l;
    }
  }
  *tr = NULL;
}

void dl_prm_split_node (dl_prm_ptr v, dl_prm_ptr *a, dl_prm_ptr *b, int x, dl_prm_ptr *rv) {
  x += v->a;
  if (x == v->a) {
    *a = NULL;
    *b = v;
  } else if (x == v->b) {
    *a = v;
    *b = NULL;
  } else if (v->a < x && x < v->b) {
    *b = dl_alloc_prm();
    (*b)->a = x;
    (*b)->b = v->b;
    (*b)->y = rand();
    (*b)->len = (*b)->b - (*b)->a;

    *a = v;
    (*a)->b = x;
    (*a)->len = (*a)->b - (*a)->a;

    dl_prm_rev_add (rv, *b);
  } else {
    assert (0);
  }
}

void dl_prm_extract (dl_prm_ptr v, dl_prm_ptr *a, dl_prm_ptr *b, dl_prm_ptr *m, int x) {
  assert (v != NULL);

  dl_prm_ptr pa = NULL, pb = NULL;

  while (v != NULL) {
//    fprintf (stderr, "v = %p\n", v);
    int xn = LEN(v->l);
    if (x < xn) {
      v->p = pb;
      pb = v;

      *b = v;
      b = &v->l;
      v = v->l;
    } else {
      x -= xn;
      if (x < v->b - v->a) {
        //found
        *a = v->l;
        if (*a) {
          (*a)->p = pa;
        }

        *b = v->r;
        if (*b) {
          (*b)->p = pb;
        }

        *m = v;
        v->l = v->r = v->p = NULL;
        v->len = v->b - v->a;

        dl_prm_fix (pa);
        dl_prm_fix (pb);
        return;
      }
      x -= v->b - v->a;

      v->p = pa;
      pa = v;

      *a = v;
      a = &v->r;
      v = v->r;
    }

  }
  assert (0);
}

int dl_prm_conv_to_array (dl_prm_ptr v, int *a, int n) {
  if (v == NULL) {
    return 0;
  }
  int ln = dl_prm_conv_to_array (v->l, a, n);
  a += ln;
  n -= ln;

  int x;
  for (x = v->a; x < v->b; x++) {
    if (n) {
      *a++ = x;
      n--;
      ln++;
    }
  }
  ln += dl_prm_conv_to_array (v->r, a, n);
  return ln;
}

int dl_perm_conv_to_array (dl_perm *p, int *a, int n) {
  dl_prm_conv_to_array (p->v, a, n);
  return LEN (p->v);
}

int dl_prm_slice (dl_prm_ptr v, int *a, int n, int offset) {
  if (n == 0 || v == NULL) {
    return 0;
  }
  int ln = LEN (v->l), res  = 0;
  if (offset < ln) {
    res = dl_prm_slice (v->l, a, n, offset);
    a += res;
    n -= res;
    offset = 0;
  } else {
    offset -= ln;
  }

  int vn = v->b - v->a;
  if (offset < v->b - v->a) {
    int x;
    for (x = v->a + offset; x < v->b && n > 0; x++) {
      *a++ = x;
      n--;
      res++;
    }
    offset = 0;
  } else {
    offset -= vn;
  }

  res += dl_prm_slice (v->r, a, n, offset);

  return res;
}

int dl_perm_slice (dl_perm *p, int *a, int n, int offset) {
  if (offset < 0) {
    offset = 0;
  }
  dl_prm_slice (p->v, a, n, offset);
  return max (LEN (p->v) - offset, 0);
}


int dl_perm_get_i (dl_perm *p, int i) {
  if (!(0 <= i && i < p->len)) {
    return -1;
  }
  dl_prm_ptr v = p->v;
  while (v != NULL) {
    int xn = LEN(v->l);
    if (i < xn) {
      v = v->l;
    } else {
      i -= xn;
      if (i < v->b - v->a) {
        return v->a + i;
      }
      i -= v->b - v->a;
      v = v->r;
    }
  }
  assert (0);
}

int dl_perm_get_rev_i (dl_perm *p, int i) {
  if (!(0 <= i && i < p->n)) {
    return -1;
  }
  //fprintf (stderr, "get rev : %d\n", i);
  //_go_rev (p->rv, 0);
  dl_prm_ptr v = p->rv, u;
  while (v != NULL) {
    if (i < v->a) {
      v = v->xl;
    } else if (i < v->b) {
      //fprintf (stderr, "node located [%d;%d]\n", v->a, v->b);
      int res = i - v->a + LEN (v->l);
      while ((u = v->p) != NULL) {
        if (u->r == v) {
          res += LEN(u->l) + u->b - u->a;
        }
        v = u;
      }
      //fprintf (stderr, "res = %d\n", res);
      return res;
    } else {
      v = v->xr;
    }
  }
  return -1;
//  assert (0);
}

int dl_perm_move_and_create (dl_perm *pp, int id, int i) {
  if (!(0 <= id && id < pp->n)) {
    return -1;
  }
  if (!(0 <= i && i <= pp->len)) {
    return -1;
  }
  //TODO: replace "assert" with "return -1"
  assert (dl_perm_get_rev_i (pp, id) == -1);

  //dbg ("dl_perm_move_and_create (id = %d) (i = %d) (len = %d)\n", id, i, pp->len);
  //dl_perm_dbg (pp);

  dl_prm_ptr p[10] = {NULL};
  if (i == pp->len) {
    p[0] = pp->v;
  } else {
    dl_prm_extract (pp->v, &p[0], &p[4], &p[1], i);
    i -= LEN (p[0]);
    dl_prm_split_node (p[1], &p[1], &p[3], i, &pp->rv);
  }

  dl_prm_ptr v = dl_alloc_prm();
  v->a = id;
  v->b = id + 1;
  v->len = v->b - v->a;
  v->y = rand();
  dl_prm_rev_add (&pp->rv, v);

  p[2] = v;

  int s;
  for (s = 0; s < 4; s++) {
    dl_prm_merge (&p[s + 1], p[s], p[s + 1], &pp->rv);
  }

  pp->v = p[4];
  pp->len++;

  return 0;
}

int dl_perm_move (dl_perm *pp, int i, int j) {
  if (!(0 <= i && i < pp->len)) {
    return -1;
  }
  if (!(0 <= j && j < pp->len)) {
    return -1;
  }
  if (i == j) {
    return 0;
  }

//  fprintf (stderr, "MoVE %d %d\n", i, j);

  dl_prm_ptr p[10] = {NULL}, add;
  dl_prm_extract (pp->v, &p[5], &p[8], &p[6], i);
  i -= LEN(p[5]);
//  fprintf (stderr, "len = %d\n", LEN(p[5]));
//  _go (p[5], 0);

  dl_prm_split_node (p[6], &p[6], &add, i, &pp->rv);
  dl_prm_split_node (add, &add, &p[7], 1, &pp->rv);

  int s;
  for (s = 0; s <= 3; s++) {
    int xn = LEN(p[s + 5]);
    if (j < xn) {
      dl_prm_extract (p[s + 5], &p[s], &p[s + 4], &p[s + 1], j);
      p[s + 5] = NULL;
      j -= LEN(p[s]);
      dl_prm_split_node (p[s + 1], &p[s + 1], &p[s + 3], j, &pp->rv);
      p[s + 2] = add;
      add = NULL;
      break;
    } else {
      j -= xn;
      p[s] = p[s + 5];
      p[s + 5] = NULL;
    }
  }
  p[9] = add;

/*  for (s = 0; s <= 9; s++) {
    fprintf (stderr, "s = %d\n", s);
    _go (p[s], 0);
  }
*/
  for (s = 0; s < 9; s++) {
    dl_prm_merge (&p[s + 1], p[s], p[s + 1], &pp->rv);
  }

  pp->v = p[9];

  return 0;
}

int dl_perm_del (dl_perm *pp, int i) {
  if (!(0 <= i && i < pp->len)) {
    return -1;
  }

//  fprintf (stderr, "DeL %d\n", i);

  dl_prm_ptr p[6] = {NULL}, add;
  dl_prm_extract (pp->v, &p[0], &p[3], &p[1], i);
  i -= LEN(p[0]);

  dl_prm_split_node (p[1], &p[1], &add, i, &pp->rv);
  dl_prm_split_node (add, &add, &p[2], 1, &pp->rv);

  int s;
  for (s = 0; s < 3; s++) {
    dl_prm_merge (&p[s + 1], p[s], p[s + 1], &pp->rv);
  }

  pp->v = p[3];

  assert (dl_prm_rev_del (&pp->rv, add->a) == add);
  dl_free_prm (add);
  pp->len--;

  return 0;
}

void dl_perm_inc (dl_perm *p, int n) {
  if (unlikely (n <= 0)) {
    return;
  }
  dl_prm_ptr v = dl_alloc_prm();
  v->a = p->n;
  p->n += n;
  p->len += n;
  v->b = p->n;
  v->len = v->b - v->a;
  v->y = rand();
  dl_prm_rev_add (&p->rv, v);
  dl_prm_merge (&p->v, p->v, v, &p->rv);
}

void dl_perm_inc_pass (dl_perm *p, int n, int pass_n) {
  if (unlikely (n <= 0)) {
    return;
  }
  assert (pass_n <= n);

  if (pass_n == n) {
    p->n += n;
    return;
  }

  dl_prm_ptr v = dl_alloc_prm();
  v->a = p->n + pass_n;
  p->n += n;
  p->len += n - pass_n;
  v->b = p->n;
  v->len = v->b - v->a;
  v->y = rand();
  dl_prm_rev_add (&p->rv, v);
  dl_prm_merge (&p->v, p->v, v, &p->rv);
}


int dl_perm_is_trivial (dl_perm *p) {
  return p->n == p->len && (p->v == NULL || (p->v->l == NULL && p->v->r == NULL));
}

int dl_perm_is_trivial_pass (dl_perm *p, int pass_n) {
  return p->n == p->len + pass_n && (p->v == NULL || (p->v->l == NULL && p->v->r == NULL && p->v->a == pass_n));
}



void dl_perm_init (dl_perm *p) {
  p->n = 0;
  p->len = 0;
  p->v = NULL;
  p->rv = NULL;
}

void dl_perm_free (dl_perm *p) {
  dl_prm_free (p->v);
}

int dl_perm_len (dl_perm *p) {
  return p->len;
}

void _go (dl_prm_ptr v, int d) {
  if (v == NULL) {
    return;
  }
  _go (v->l, d + 1);
  fprintf (stderr, "%*s%p(%d;%d [%d]) : %d, %p\n", d, "", v, v->a, v->b, v->y, v->len, v->p);
  _go (v->r, d + 1);
}

void _go_rev (dl_prm_ptr v, int d) {
  if (v == NULL) {
    return;
  }
  _go_rev (v->xl, d + 1);
  fprintf (stderr, "%*s%p(%d;%d [%d])\n", d, "", v, v->a, v->b, v->y);
  _go_rev (v->xr, d + 1);
}

void dl_perm_dbg (dl_perm *p) {
  fprintf (stderr, "%d : \n", p->n);
  _go (p->v, 0);
}

void dl_perm_rev_dbg (dl_perm *p) {
  fprintf (stderr, "%d : \n", p->n);
  _go_rev (p->rv, 0);
}

int cnt (dl_prm_ptr p) {
  if (p) {
    return 1 + cnt (p->l) + cnt (p->r);
  }
  return 0;
}


perm_list* list_alloc (int a, int b) {
  perm_list *v = dl_malloc (sizeof (perm_list));
  v->l = v->r = v;
  v->a = a;
  v->b = b;
  return v;
}
void list_free (perm_list *v) {
  dl_free (v, sizeof (perm_list));
}

void list_add (perm_list *a, perm_list *y) {
  perm_list *b = a->r;
  a->r = y;
  y->l = a;
  y->r = b;
  b->l = y;
}

void list_del (perm_list *x) {
  perm_list *a = x->l, *b = x->r;
  a->r = b;
  b->l = a;
}

int dl_perm_list_is_trivial (dl_perm_list *p) {
  return p->n == p->len && (p->v->r == p->v || p->v->r->r == p->v);
}

int dl_perm_list_conv_to_array (dl_perm_list *p, int *a, int n) {
  perm_list *st = p->v, *v = st->r;

  int ln = 0;

  while (v != st) {
    int x;
    for (x = v->a; x < v->b; x++) {
      if (n) {
        *a++ = x;
        n--;
        ln++;
      }
    }
    v = v->r;
  }
  return p->len;
}

int dl_perm_list_slice (dl_perm_list *p, int *a, int n, int offset) {
  perm_list *st = p->v, *v = st->r;

  int ln = 0;

  while (v != st) {
    int x;
    for (x = v->a; x < v->b; x++) {
      if (offset > 0) {
        offset--;
      } else {
        if (n) {
          *a++ = x;
          n--;
          ln++;
        }
      }
    }
    v = v->r;
  }
  return max (p->len - offset, 0);
}


void dl_perm_list_init (dl_perm_list *p) {
  p->n = 0;
  p->len = 0;
  p->v = list_alloc (0, 0);
}

void dl_perm_list_free (dl_perm_list *p) {
  perm_list *st = p->v, *v = st;
  do {
    perm_list *t = v;
    v = v->r;
    list_free (t);
  } while (v != st);
}

perm_list *move_r (perm_list *v, int n) {
  while (n > 0) {
    int cn = v->b - v->a;
    if (cn <= n) {
      n -= cn;
      v = v->r;
    } else {
      perm_list *u = list_alloc (v->a + n, v->b);
      v->b = v->a + n;
      list_add (v, u);
      return v;
    }
  }
  return v->l;
}

perm_list *move_l (perm_list *v, int n) {
  while (n > 0) {
    int cn = v->b - v->a;
    if (cn <= n) {
      n -= cn;
      v = v->l;
    } else {
      perm_list *u = list_alloc (v->b - n, v->b);
      v->b -= n;
      list_add (v, u);
      return v;
    }
  }
  return v;
}


void dl_perm_list_dbg (dl_perm_list *p) {
  perm_list *st;
  for (st = p->v->r; st != p->v; st = st->r) {
    fprintf (stderr, "[%d,%d] ", st->a, st->b);
  }
  fprintf (stderr, "\n");
}


void dl_perm_list_inc (dl_perm_list *p, int n) {
  if (n > 0) {
    int a = p->n,
        b = p->n += n;
    p->len += n;
    list_add (p->v->l, list_alloc (a, b));
  }
}

int dl_perm_list_get_i (dl_perm_list *p, int i) {
/*  if (!(0 <= i && i < p->len)) {
    return -1;
  }*/
  perm_list *v = p->v;
  int t;
  while (i >= (t = v->b - v->a)) {
    i -= t;
    v = v->r;
  }
  return v->a + i;
}

int dl_perm_list_get_rev_i (dl_perm_list *p, int i) {
/*  if (!(0 <= i && i < p->n)) {
    return -1;
  }*/

  perm_list *v = p->v;
  int res = 0;
  while (i < v->a || i >= v->b) {
    res += v->b - v->a;
    v = v->r;
    if (v == p->v) {
      return -1;
    }
  }

  return res + i - v->a;
}

int dl_perm_list_move_and_create (dl_perm_list *p, int id, int i) {
  if (!(0 <= id && id < p->n)) {
    return -1;
  }
  if (!(0 <= i && i <= p->len)) {
    return -1;
  }
  //TODO: replace "assert" with "return -1"
  assert (dl_perm_list_get_rev_i (p, id) == -1);

  perm_list *v = move_r (p->v->r, i), *add;
  add = list_alloc (id, id + 1);
  list_add (v, add);

  p->len++;

  return 0;
}

int dl_perm_list_move (dl_perm_list *p, int i, int j) {
  if (!(0 <= i && i < p->len)) {
    return -1;
  }
  if (!(0 <= j && j < p->len)) {
    return -1;
  }
  if (i == j) {
    return 0;
  }

  perm_list *v = move_r (p->v->r, i)->r, *add;
  if (v->a + 1 == v->b) {
    add = v;
    list_del (v);
  } else {
    add = list_alloc (v->a, v->a + 1);
    v->a++;
  }
  v = move_r (p->v->r, j);

  list_add (v, add);
  return 0;
}

int dl_perm_list_del (dl_perm_list *p, int i) {
  if (!(0 <= i && i < p->len)) {
    return -1;
  }

  dl_perm_list_move (p, i, 0);
  perm_list *v = p->v->r;
  if (v->a + 1 == v->b) {
    list_del (v);
    list_free (v);
  } else {
    v->a++;
  }
  p->len--;
  return 0;
}

int dl_perm_list_len (dl_perm_list *p) {
  return p->len;
}
