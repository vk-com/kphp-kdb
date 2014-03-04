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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <stddef.h>
#include <string.h>

#include "utils.h"
#include "crc32.h"

int _fix;

char _EMPTY__METAFILE[20] = {0};
char *EMPTY__METAFILE = (char *)_EMPTY__METAFILE;
int EMPTY__METAFILE_LEN = 20;

/***
  Temporary memory
 ***/

char tmp_mem[MAX_TMP_MEM];
int tmp_mem_n;

void tmp_mem_init (void) {
  tmp_mem_n = 0;
}

inline void *tmp_mem_alloc (int n) {
  char *res = tmp_mem + tmp_mem_n;
  tmp_mem_n += n;
  assert (tmp_mem_n <= MAX_TMP_MEM);
  return res;
}

char *tmp_mem_strdup (char *s) {
  int n = strlen (s) + 1;
  char *v = tmp_mem_alloc (n);
  memcpy (v, s, n);
  return v;
}

char *tmp_mem_strdupn (char *s, int n) {
  char *v = tmp_mem_alloc (n + 1);
  memcpy (v, s, n);
  v[n] = 0;
  return v;
}

char *tmp_mem_dup (char *s, int n) {
  char *v = tmp_mem_alloc (n);
  memcpy (v, s, n);
  return v;
}

/***
  Lookup
 ***/

void lookup_init (lookup *l) {
  map_int_int_init (&l->new_v);
  l->x = NULL;
  l->y = NULL;
  l->n = -2;

#ifdef DL_DEBUG
  l->real_n = 0;
#endif
}
void lookup_free (lookup *l) {
  map_int_int_free (&l->new_v);
  l->x = NULL;
  l->y = NULL;
  l->n = -2;

#ifdef LOOKUP_CNT
  l->real_n = 0;
#endif
}

int lookup_save_expected_len (lookup *l) {
  return map_int_int_used (&l->new_v) + l->n;
}

int lookup_get_ids (lookup *l, int *v, int mx) {
  int *st = v;
  int i;
  for (i = 0; i < l->n; i++) {
    if (map_int_int_get (&l->new_v, l->x[i]) == NULL) {
      if (mx > 0) {
        *v++ = l->x[i];
        mx--;
      }
    }
  }

  int nn = map_int_int_used (&l->new_v);
  if (nn == 0) {
    return v - st;
  }

  size_t ab_size = nn * sizeof (int);
  int *a = dl_malloc (ab_size);
  int *b = dl_malloc (ab_size);

  assert (map_int_int_pairs (&l->new_v, a, b, nn) == nn);
  for (i = 0; i < nn; i++) {
    if (b[i] != -1) {
      if (mx > 0) {
        *v++ = a[i];
        mx--;
      }
    }
  }

  dl_free (a, ab_size);
  dl_free (b, ab_size);

  return v - st;
}

int lookup_save_prepare (lookup *l, int *x, int *y, int n, int none) {
  int new_n = map_int_int_used (&l->new_v);

  assert (n >= new_n + l->n);

  size_t new_size = sizeof (int) * new_n;
  int *new_x = dl_malloc (new_size),
      *new_y = dl_malloc (new_size);

  size_t v_size = sizeof (pair_int_int) * new_n;
  pair_int_int *v = dl_malloc (v_size);

  assert (map_int_int_pairs (&l->new_v, new_x, new_y, new_n) == new_n);
  int i;
  for (i = 0; i < new_n; i++) {
    v[i].x = new_x[i];
    v[i].y = new_y[i];
  }

  dl_qsort_pair_int_int (v, new_n);

  int ai = 0, an = l->n, bi = 0, bn = new_n, ci = 0;
  while (ai < an || bi < bn) {
    if (ai == an || (bi < bn && (l->x[ai] >= v[bi].x))) {
      if (v[bi].y != none) {
        x[ci] = v[bi].x;
        y[ci++] = v[bi].y;
      }
      if (ai != an && l->x[ai] == v[bi].x) {
        ai++;
      }
      bi++;
    } else {
      x[ci] = l->x[ai];
      y[ci] = l->y[ai];
      ai++;
      ci++;
    }
  }

  dl_free (new_x, new_size);
  dl_free (new_y, new_size);
  dl_free (v, v_size);

  return ci;
}

int lookup_save_write (int *x, int *y, int n, char *s, int sn) {
  char *st = s;

  assert (sn >= (int)sizeof (int));
  WRITE_INT (s, n);
  sn -= sizeof (int);

  assert (sn >= (int)sizeof (int) * 2 * n);
  int d = sizeof (int) * n;
  memcpy (s, x, d);
  s += d;
  memcpy (s, y, d);
  s += d;

  return s - st;
}

int lookup_save (lookup *l, char *buf, int sn, int none) {
  char *s = buf;

  int mx_n = lookup_save_expected_len (l);
  size_t xy_size = sizeof (int) * mx_n;
  int *x = dl_malloc (xy_size),
     *y = dl_malloc (xy_size);

  int n = lookup_save_prepare (l, x, y, mx_n, none);

  int sdv = lookup_save_write (x, y, n, s, sn);
  s += sdv;
  sn -= sdv;

  dl_free (x, xy_size);
  dl_free (y, xy_size);

  return s - buf;
}

void lookup_unload (lookup *l) {
  l->x = NULL;
  l->y = NULL;
  l->n = -1;
}

void lookup_load (lookup *l, char *metafile, int metafile_len) {
  if (metafile != NULL) {
    assert (metafile_len >= (int)sizeof (int));

    int n;
    READ_INT (metafile, n);

#ifdef LOOKUP_CNT
    if (l->n == -2) {
      l->real_n += n;
    }
#endif

    l->n = n;

    assert (metafile_len == (int)sizeof (int) + (int)sizeof (int) * l->n * 2);
  } else {
    l->n = 0;
  }
  l->x = (int *)metafile;
  l->y = (int *)metafile + l->n;
}

size_t lookup_load_old (lookup *l, char *metafile, int max_metafile_len, int n) {
  size_t size = 0;

#ifdef LOOKUP_CNT
    if (l->n == -2) {
      l->real_n += n;
    }
#endif

  l->n = n;

  size = sizeof (int) * l->n * 2;
//  assert (max_metafile_len >= (int)size);

  l->x = (int *)metafile;
  l->y = (int *)metafile + l->n;
  return size;
}

int lookup_conv (lookup *l, int x, int none) {
  int *y = map_int_int_get (&l->new_v, x);
  if (y != NULL) {
    return *y;
  }

  int res = dl_find_int (l->x, l->n, x);
  if (res != l->n) {
    return l->y[res];
  }

  return none;
}

void lookup_set (lookup *l, int x, int y) {
  //dbg ("SET %d->%d\n", x, y);
#ifdef LOOKUP_CNT
  int old_val = lookup_conv (l, x, 0);
  l->real_n += (y != 0) - (old_val != 0);
#endif

  *map_int_int_add (&l->new_v, x) = y;
}

/***
  Data
 ***/

int data_get_local_id_by_id (data *d, int id) {
  return lookup_conv (&d->id_to_local_id, id, -1);
}

typedef struct restore_list_t restore_list;

struct restore_list_t {
  restore_list *prev, *next;
  int val;
};

inline void restore_list_connect (restore_list *a, restore_list *b) {
  a->next = b;
  b->prev = a;
}

restore_list *restore_list_alloc (int val) {
  restore_list *res = dl_malloc (sizeof (restore_list));
  restore_list_connect (res, res);
  res->val = val;
  return res;
}

restore_list *restore_list_create_node (restore_list *prev, restore_list *next, int val) {
  restore_list *res = dl_malloc (sizeof (restore_list));

  restore_list_connect (res, next);
  restore_list_connect (prev, res);

  res->val = val;

  return res;
}

int restore_list_is_empty (restore_list *v) {
  if (v != NULL) {
    assert (v->next != NULL);
  }
  return v == NULL || v->next == v;
}

void restore_list_free (restore_list *v) {
  if (v == NULL) {
    return;
  }
  assert (restore_list_is_empty (v));
  dl_free (v, sizeof (*v));
}

void restore_list_free_rec (restore_list *v) {
  //dbg ("restore_list_free_rev (v = %p)\n", v);
  restore_list *end = v;
  do {
    restore_list *next = v->next;
    dl_free (v, sizeof (*v));
    v = next;
  } while (v != end);
}

restore_list *restore_list_pop_front (restore_list *v) {
  restore_list *res;
  if (v->next == v) {
    res = NULL;
  } else {
    restore_list_connect (v->prev, v->next);
    res = v->next;
  }

  restore_list_connect (v, v);
  restore_list_free (v);
  return res;
}

/*
void restore_list_push_front (restore_list *v, int val) {
  restore_list_create_node (v, v->next, val);
}
*/
void restore_list_push_back (restore_list *v, int val) {
  restore_list_create_node (v->prev, v, val);
}

/*
void restore_list_check (restore_list *v) {
  if (v == NULL) {
    return;
  }
  restore_list *end = v;
  int cnt = 0;
  int a[55];
  do {
    a[cnt] = v->val;
    v = v->next;
    assert (cnt++ < 50);
  } while (v != end);

  int i, j;
  for (i = 0; i < cnt; i++) {
    for (j = i + 1; j < cnt; j++) {
      assert (a[i] != a[j]);
    }
  }
}

*/
void restore_list_dbg (restore_list *v) {
  restore_list *end = v;
  dbg ("-------\n");
  do {
    dbg ("(%p;%d)\n", v, v->val);
    v = v->next;
  } while (v != end);
}

int restore_list_del (restore_list *v, int needed_id, restore_list **before_id, restore_list **after_id, int *prev_id) {
  //restore_list_check (v);
  //restore_list_dbg (v);
  restore_list *i = v->next;

  while (i != v) {
    if (i->val == needed_id) {
      *prev_id = v->val;

      if (i->next != v) {
        *after_id = restore_list_create_node (v->prev, i->next, needed_id);
      } else {
        *after_id = NULL;
      }
      restore_list_connect (i->prev, v);
      *before_id = v;

      restore_list_connect (i, i);

      restore_list_free (i);

      return 1;
    }
    i = i->next;
  }
  return 0;
}

void restore_list_append (restore_list *v, restore_list *to_append) {
  //dbg ("restore_list_append %p %p\n", v->prev, v);
  restore_list *to_append_prev = to_append->prev;
  restore_list_connect (v->prev, to_append);
  restore_list_connect (to_append_prev, v);
}


int *_restore_info, _restore_info_n, _restore_info_maxn;

#define CONV_ID(x) (-(x + 2))

void dump_restore (int *i, vptr *_v) {
  restore_list *v = *(restore_list **)_v;

  assert (_restore_info_n < _restore_info_maxn);
  _restore_info[_restore_info_n++] = CONV_ID (v->val);

  restore_list *end = v;
  while (v->next != end) {
    v = v->next;

    assert (_restore_info_n < _restore_info_maxn);
    _restore_info[_restore_info_n++] = v->val;
  }
}

int read_restore (int *v, int n, map_int_vptr *h) {
  int i;
  restore_list *cur = NULL;
  for (i = 0; i < n; i++) {
    int x = v[i];
    if (x < 0) {
      cur = restore_list_alloc (CONV_ID (x));
    } else {
      restore_list_push_back (cur, x);
    }
    if (i + 1 == n || v[i + 1] < 0) {
      assert (cur != NULL);
      assert (cur->next != cur);
      assert (map_int_vptr_get (h, cur->val) == NULL);
      *map_int_vptr_add (h, cur->val) = cur;
    }
  }
  return sizeof (int) * n;
}


void data_restore_append (data *d, int a, int b) {
  restore_list **b_list_ptr = (restore_list **)map_int_vptr_get (&d->restore_info, b);
  if (b_list_ptr != NULL) {
    restore_list *b_list = *b_list_ptr;
    assert (!restore_list_is_empty (b_list));

    restore_list **a_list_ptr = (restore_list **)map_int_vptr_add (&d->restore_info, a);
    if (*a_list_ptr == NULL) {
      *a_list_ptr = restore_list_alloc(a);
    }
    restore_list *a_list = *a_list_ptr;

    b_list = restore_list_pop_front (b_list);
    restore_list_append (a_list, b_list);
    //restore_list_check (a_list);
    map_int_vptr_del (&d->restore_info, b);
  }
}

void data_restore_dbg (data *d, int v) {
  restore_list **list = (restore_list **)map_int_vptr_get (&d->restore_info, v);
  dbg ("list %d: %p\n", v, list);
  if (list != NULL) {
    restore_list_dbg (*list);
  }
}


// ..ab...c.. ---> ...a...cb..
void data_restore_fix (data *d, int a, int b, int c) {
  //assert (0);

  //b append to a
  data_restore_append (d, a, b);
  //c append to b
  data_restore_append (d, b, c);
}


void data_restore_add (data *d, int id, int prev_id) {
//  dbg ("data_restore_add (id = %d) (prev_id = %d)\n", id, prev_id);
  restore_list **prev_id_list_ptr = (restore_list **)map_int_vptr_add (&d->restore_info, prev_id);
  if (*prev_id_list_ptr == NULL) {
    *prev_id_list_ptr = restore_list_alloc (prev_id);
  }
  restore_list *prev_id_list = *prev_id_list_ptr;

  restore_list **id_list_ptr = (restore_list **) map_int_vptr_get (&d->restore_info, id);
  restore_list *id_list;
  if (id_list_ptr == NULL) {
    id_list = restore_list_alloc (id);
  } else {
    id_list = *id_list_ptr;
    map_int_vptr_del (&d->restore_info, id);
  }

  restore_list_append (prev_id_list, id_list);
}

int _needed_id, _found, *_prev_id;
restore_list *_found_a, *_found_b;
//TODO: break when result is found
void search_for_id (int *prev, vptr *_list_ptr) {
  if (_found > 0) {
    return;
  }
  restore_list *list = (restore_list *) *_list_ptr;

  _found = restore_list_del (list, _needed_id, &_found_a, &_found_b, _prev_id);
}


//returns 1 if ok, -1 if failed. result is stored in prev_id.
int data_restore_del (data *d, int needed_id, int *prev_id) {
//  dbg ("data_restore_del\n");
  _prev_id = prev_id;
  _found = -1;
  _needed_id = needed_id;
  _prev_id = prev_id;
  map_int_vptr_foreach (&d->restore_info, search_for_id);

//  dbg ("_found = %d\n", _found);
  if (_found > 0) {
    if (restore_list_is_empty (_found_a)) {
      int a = _found_a->val;
      restore_list_free (_found_a);
      map_int_vptr_del (&d->restore_info, a);
    }

    if (restore_list_is_empty (_found_b)) {
      restore_list_free (_found_b);
    } else {
      int  b = _found_b->val;
      *(restore_list **)map_int_vptr_add (&d->restore_info, b) = _found_b;
    }
  }

  return _found;
}

int data_get_ids (data *d, int *v, int mx) {
  return lookup_get_ids (&d->id_to_local_id, v, mx);
}

int data_get_pos_by_local_id (data *d, int local_id) {
  return PERM (get_rev_i, &d->prm, local_id);
}

int data_get_local_id_by_pos (data *d, int pos) {
  return PERM (get_i, &d->prm, pos);
}

int data_get_hidden_state (data *d, int id) {
  return lookup_conv (&d->hidden_state, id, 0);
}

int data_get_cnt (data *d) {
  return PERM (len, &d->prm);
}

int data_hide (data *d, int id, int tm) {
  int local_id = data_get_local_id_by_id (d, id);
  if (local_id < 0) {
    return -1;
  }

  int pos = data_get_pos_by_local_id (d, local_id);
  assert (pos >= 0);

  int prev_local_id = pos == 0 ? -1 : PERM (get_i, &d->prm, pos - 1);
  data_restore_add (d, local_id, prev_local_id);

  lookup_set (&d->hidden_state, id, tm);

  return PERM (del, &d->prm, pos);
}

int data_hide_expected_size (data *d) {
#ifdef LOOKUP_CNT
  return d->hidden_state.real_n;
#else
  return lookup_save_expected_len (&d->hidden_state);
#endif
}

int data_del (data *d, int id) {
//  dbg ("DATA_DEL %d\n", id);
  int local_id = data_get_local_id_by_id (d, id);
  if (local_id < 0) {
    return -1;
  }

  //*map_int_int_add (&d->new_id_to_i, id) = -1;
  int pos = data_get_pos_by_local_id (d, local_id);
  if (pos >= 0) {
    PERM (del, &d->prm, pos);

    int prev_local_id = pos == 0 ? -1 : PERM (get_i, &d->prm, pos - 1);
    data_restore_append (d, prev_local_id, local_id);
  }

  lookup_set (&d->id_to_local_id, id, -1);

  //TODO: lazy_set
  lookup_set (&d->hidden_state, id, 0);

  dyn_object **dyn_obj = (dyn_object **)map_int_vptr_get (&d->dyn_snapshots, local_id + 1);
  if (dyn_obj != NULL) {
    DATA_FUNC(d)->free_dyn (NULL, dyn_obj);
    map_int_vptr_del (&d->dyn_snapshots, local_id + 1);
  }

  return 0;
}

int data_move (data *d, int i, int j) {
  return PERM (move, &d->prm, i, j);
}

int data_move_and_create (data *d, int i, int j) {
//  dbg ("move and create %d %d\n", i, j);
  return PERM (move_and_create, &d->prm, i, j);
}

//with restore
int data_move_new (data *d, int i, int j, int local_i, int local_j) {
  if (1) {
    int prev_i = i == 0 ? -1 : PERM (get_i, &d->prm, i - 1);
    assert (prev_i >= -1);
    data_restore_fix (d, prev_i, local_i, local_j);
  }

  return data_move (d, i, j);
}

int data_restore (data *d, int id) {
  if (data_get_hidden_state (d, id) == 0) {
    return -1;
  }

  int local_id = data_get_local_id_by_id (d,  id);
  if (local_id < 0) {
    return -1;
  }

  int prev_local_id;
//  dbg ("data_restore (local_id = %d)\n", local_id);
  if (data_restore_del (d, local_id, &prev_local_id) <= 0) {
    dbg ("data_restore_del failed\n");
    return -1;
  }

  int i = local_id,
      j;
  assert (data_get_pos_by_local_id (d, i) == -1);
  if (prev_local_id == -1) {
    j = 0;
  } else {
    j = data_get_pos_by_local_id (d, prev_local_id) + 1;
//    dbg ("(prev_local_id = %d) (pos = %d)\n", prev_local_id, j - 1);
    assert (j != 0);
  }

  assert (data_move_and_create (d, i, j) > -1);

  lookup_set (&d->hidden_state, id, 0);
  return 0;
}

/*
void tmp_dbg (int *i, void **v) {
  dbg ("!![%d->(%p:%p)]\n", *i, v, *v);
}
*/
int data_add_change (data *d, change *ch, int local_id) {
//  int pos = data_get_pos_by_local_id (d, local_id);
//  dbg ("ADD CHANGE to (local_id = %d) (pos = %d)\n", local_id, pos);

  dyn_object **dyn_obj = (dyn_object **)map_int_vptr_add (&d->dyn_snapshots, local_id + 1);
  DATA_FUNC(d)->add_change(dyn_obj, ch);
  if (*dyn_obj == NULL) {
    map_int_vptr_del (&d->dyn_snapshots, local_id + 1);
  }
//  map_int_vptr_foreach (&d->dyn_snapshots, tmp_dbg);
  return 0;
}

int data_add_object (data *d, int id) {
//  dbg ("ADD OBJECT %d\n", id);

  int local_id = data_get_local_id_by_id (d, id), is_hidden = 0;
  if (local_id != -1) {
    is_hidden = data_get_hidden_state (d, id) != 0;
    if (!is_hidden) {
      return -1;
    }
  }

  if (is_hidden) {
    data_del (d, id);
  }

  int res = d->objects_n + d->new_objects_n;
  lookup_set (&d->id_to_local_id, id, res);
  d->new_objects_n++;

  PERM (inc, &d->prm, 1);
  return res;
}

int data_get_actual_object (data *d, int local_id, actual_object *o) {
  //  dbg ("data_get_actual_object (local_id = %d)\n", local_id);
  if (local_id < 0 || local_id >= d->objects_n + d->new_objects_n) {
    return -1;
  }

  if (local_id < d->objects_n) {
    o->obj = d->objects + d->objects_offset[local_id];
    o->obj_len = d->objects_offset[local_id + 1] - d->objects_offset[local_id];
  } else {
    o->obj = NULL;
    o->obj_len = 0;
  }

  dyn_object **tdyn = (dyn_object **)map_int_vptr_get (&d->dyn_snapshots, local_id + 1);
  if (tdyn == NULL) {
    o->dyn = NULL;
  } else {
    o->dyn = *tdyn;
  }

  return 0;
}

/*
void DUMP (char *s, int len) {
  int *v = (int *)s;
  len /= sizeof (int);
  int i;
  for (i = 0; i < len; i++) {
    dbg ("%d: [%d]\n", i * 4, v[i]);
  }
}*/

void data_load (data *d, char *metafile, int metafile_len) {
  char *s = metafile, *t = s + metafile_len;

  int init_perm = d->objects_n == -2;

  READ_INT (s, d->objects_n);

//  dbg ("LOAD DATA: (objects_n = %d) (metafile_len = %d)\n", d->objects_n, metafile_len);
  //dbg ("crc32 = %d\n", compute_crc32 (metafile, metafile_len));
  //DUMP (metafile, metafile_len);

  d->objects_offset = (int *)s;
  s += sizeof (int) * (d->objects_n + 1);

  //TODO: check it
  s += lookup_load_old (&d->id_to_local_id, s, t - s, d->objects_n);

  d->objects = s;

  s = d->objects + d->objects_offset[d->objects_n];

  if (t != s && d->objects_n) {// new index
    int lookup_len;
    assert (s + sizeof (int) <= t);
    READ_INT (s, lookup_len);
    assert (s + lookup_len <= t);
    lookup_load (&d->hidden_state, s, lookup_len);
    s += lookup_len;

    if (init_perm) {
      int restore_n;
      assert (s + sizeof (int) <= t);
      READ_INT (s, restore_n);

      int restore_len = sizeof (int) * restore_n;
      assert (s + restore_len <= t);
      s += read_restore ((int *)s, restore_n, &d->restore_info);
    }
    //assert (s == s + restore_len);
  } else {
    lookup_load (&d->hidden_state, NULL, 0);
  }
  //s += lookup_load (d->id_to_local_id, s, t - s);

  //d->ids = (int *)s;
  //s += sizeof (int) * d->objects_n;

  //d->old_perm = (int *)s;
  //s += sizeof (int) * d->objects_n;

  //assert (s <= t);

  if (init_perm) {
    //PERM (inc, &d->prm, d->objects_n);
    PERM (inc_pass, &d->prm, d->objects_n, d->hidden_state.n);
  }


  //if (DATA_FUNC (d)->object_onload != NULL) {
  //  int i;
  //  actual_object o;
  //  for (i = 0; i < d->objects_n; i++) {
  //    data_get_actual_object (d, i, &o);
  //    int f = o->dyn != NULL;
  //    DATA_FUNC (d)->object_onload (&o);
  //    if (f && o->dyn == NULL) {
  //      map_int_vptr_del (&d->dyn_snapshots, local_id + 1);
  //    }
  //  }
  //}
}


// return whether data contains something useful
int data_is_empty (data *d, int pass_n) {
  return  d->new_objects_n == 0  &&
          PERM (is_trivial_pass, &d->prm, pass_n) &&
          map_int_vptr_used (&d->dyn_snapshots) == 0 &&
          map_int_int_used (&d->hidden_state.new_v) == 0 &&
          1;
}

int data_unload (data *d) {
  d->objects_n = -1;
  d->objects = NULL;
  d->objects_offset = NULL;
  lookup_unload (&d->id_to_local_id);
  int pass_n = d->hidden_state.n;
  lookup_unload (&d->hidden_state);
//  dbg ("DATA_UNLOAD %d\n", data_is_empty (d, pass_n));
  return data_is_empty (d, pass_n);
}

int data_loaded (data *d) {
  return d->objects_n >= 0;
}

void free_restore_list (int *i, vptr *v) {
  restore_list *list = (restore_list *)*v;
  restore_list_free_rec (list);
}

void data_free (data *d) {
  lookup_free (&d->id_to_local_id);
  lookup_free (&d->hidden_state);

  PERM (free, &d->prm);
  map_int_vptr_foreach (&d->dyn_snapshots, DATA_FUNC(d)->free_dyn);
  map_int_vptr_free (&d->dyn_snapshots);


  map_int_vptr_foreach (&d->restore_info, free_restore_list);
  map_int_vptr_free (&d->restore_info);
}

int data_get_actual_objects (data *d, actual_object *o, int mx) {
  static int p[1000];
  int n = PERM (slice, &d->prm, p, 1000, 0);
  assert (n < 1000);
  if (n > mx) {
    n = mx;
  }
  int i;
  for (i = 0; i < n; i++) {
    assert (data_get_actual_object (d, p[i], &o[i]) > -1);
  }

  return n;
}

int data_slice (data *d, actual_object *o, int n, int offset) {
  static int p[MAX_RESULT];
  if (n > MAX_RESULT) {
    n = MAX_RESULT;
  }
  int i, rn = PERM (slice, &d->prm, p, n, offset);

  if (n > rn) {
    n = rn;
  }
  for (i = 0; i < n; i++) {
    assert (data_get_actual_object (d, p[i], &o[i]) > -1);
    o[i].deleted = 0;
  }

  return rn;
}

int data_slice_filtered (data *d, actual_object *o, int n, int offset, predicate *pred) {
  static int p[MAX_RESULT];

  int total_n = PERM (slice, &d->prm, p, MAX_RESULT, 0), rn = 0, i;

  actual_object ao;
  for (i = 0; i < total_n && rn < n; i++) {
    assert (data_get_actual_object (d, p[i], &ao) > -1);
    if (DL_CALL (pred, eval, &ao)) {
      if (offset <= 0) {
        o[rn] = ao;
        o[rn++].deleted = 0;
      } else {
        offset--;
      }
    }
  }

  return rn;
}

int data_save (data *d, char *buf, int buf_size, int max_objects_n, int *deleted, int *deleted_n_ptr, int deleted_max_n) {
  int i;

  char *s = buf;
  int sn = buf_size;

  int max_local_id = d->objects_n + d->new_objects_n + 1;

  if (max_objects_n < 0 || max_objects_n > max_local_id) {
    max_objects_n = max_local_id;
  }


  int c_mx_n = lookup_save_expected_len (&d->id_to_local_id);

  size_t c_size = sizeof (int) * c_mx_n;
  int *cx = dl_malloc (c_size),
      *cy = dl_malloc (c_size);

  int cn = lookup_save_prepare (&d->id_to_local_id, cx, cy, c_mx_n, -1), ci = 0;

  size_t hidden_ids_size = sizeof (int) * lookup_save_expected_len (&d->hidden_state);
  int *hidden_ids = dl_malloc (hidden_ids_size),
    hidden_ids_n = 0;

  // gen permutation of unhidden elements
  assert (max_local_id >= cn);
  size_t perm_size = sizeof (int) * max_local_id;
  int *perm = dl_malloc (perm_size),
      *perm_flag = dl_malloc0 (perm_size),
      *inv_perm = dl_malloc (perm_size),
      *next = dl_malloc (perm_size);

  int n = PERM (slice, &d->prm, perm, cn, 0);
  assert (n <= cn);

  int shift;
  if (max_objects_n < n) {
    shift = n - max_objects_n;
  } else {
    shift = 0;
  }

  // gen inversed permutation
  for (i = 0; i < n; i++) {
    inv_perm[perm[i]] = i;
  }

  //init next
  for (i = 0; i < n; i++) {
    next[perm[i]] = perm[i];
  }

  //dbg ("hidden photos expected count = %d\n", data_hide_expected_size (d));

  int deleted_n = 0;
  for (i = 0; i < cn; i++) {
    int id = cx[i],
      local_id = cy[i];

    int hidden_state = data_get_hidden_state (d, id);

    if (hidden_state != 0) {
      hidden_state--;
      lookup_set (&d->hidden_state, cx[i], hidden_state) ;

      if (hidden_state != 0) {
        assert (hidden_state > 0);
        cx[ci] = id;
        cy[ci] = local_id;
        ci++;

        hidden_ids[hidden_ids_n++] = local_id;
      } else {
        if (deleted != NULL && deleted_n < deleted_max_n) {
          deleted[deleted_n] = id;
        }
        deleted_n++;
      }
    } else {
      if (shift <= 0) {
        cx[ci] = id;
        cy[ci] = local_id;

        ci++;
      } else {
        int pos = inv_perm[local_id];
        if (pos == 0) {
          next[local_id] = -1;
        } else {
          next[local_id] = perm[pos - 1];
        }
        perm_flag[pos] = 1;

        if (deleted != NULL && deleted_n < deleted_max_n) {
          deleted[deleted_n] = id;
        }
        deleted_n++;
        shift--;
      }
    }
  }

  size_t new_local_id_size = sizeof (int) * max_local_id;
  int *new_local_id = dl_malloc (new_local_id_size);
  memset (new_local_id, -1, new_local_id_size);

  size_t new_perm_size = sizeof (int) * ci;
  int *new_perm = dl_malloc (new_perm_size);
  int new_perm_n = 0;

  int cur_id = 0;
  for (i = 0; i < hidden_ids_n; i++) {
    //dbg ("nhidden (old_id = %d) -> (new_id = %d)\n", hidden_ids[i], cur_id);
    int id = hidden_ids[i];

    new_perm[cur_id] = id;
    new_local_id[id] = cur_id++;
  }

  for (i = 0; i < n; i++) {
    int id = perm[i];
    if (perm_flag[i]) { // deleted
      next[id] = next[id] == -1 ? -1 : next[next[id]];
      if (next[id] != -1) {
        assert (next[next[id]] == next[id]);
      }
      data_restore_append (d, next[id], id);
    } else {
      assert (next[id] == id);

      //dbg ("uhidden (old_id = %d) -> (new_id = %d)\n", id, cur_id);
      new_perm[cur_id] = id;
      new_local_id[id] = cur_id++;
    }
  }
  new_perm_n = cur_id;

  assert (new_perm_n == ci);

  for (i = 0; i < ci; i++) {
    int y = cy[i];
    cy[i] = new_local_id[y];
    assert (cy[i] != -1);
    //new_local_id[y] = -1;
  }

  if (deleted_n_ptr != NULL) {
    *deleted_n_ptr = deleted_n;
  }

  //dbg ("(hidden_n = %d) (deleted_n = %d) (cur_id - hidden_n = %d) == (cn = %d)\n", hidden_ids_n, deleted_n, cur_id - hidden_ids_n, cn);

  assert ((int)sizeof (int) <= sn);
  assert (ci >= 0);
  WRITE_INT (s, ci);
  sn -= sizeof (int);

  int header_size = sizeof (int) * (ci + 1);
  int *header = (int *)s;

  assert (header_size <= sn);
  s += header_size;
  sn -= header_size;

  //TODO: no n in old metafile, just 2 arrays
  int ids_size = sizeof (int) * ci;
  assert (ids_size <= sn);
  memcpy (s, cx, ids_size);
  s += ids_size;
  sn -= ids_size;


  int old_perm_size = sizeof (int) * ci;
  assert (old_perm_size <= sn);
  memcpy (s, cy, old_perm_size);
  s += old_perm_size;
  sn -= old_perm_size;


  actual_object o;
  char *st = s;
  for (i = 0; i < ci; i++) {
    header[i] = s - st;
    assert (data_get_actual_object (d, new_perm[i], &o) > -1);
    int len = DATA_FUNC(d)->object_save (&o, s, sn);
    s += len;
    sn -= len;
    assert (sn >= 0);
  }
  header[ci] = s - st;

  assert ((int)sizeof (int) <= sn);
  int *lookup_len = (int *)s;
  s += sizeof (int);
  sn -= sizeof (int);
  s += *lookup_len = lookup_save (&d->hidden_state, s, sn, 0);
  sn -= *lookup_len;


  //DUMP (st, s - st);

  _restore_info_maxn = max_local_id;
  int _restore_info_size = sizeof (int) * _restore_info_maxn;
  _restore_info = dl_malloc (_restore_info_size);
  _restore_info_n = 0;

  map_int_vptr_foreach (&d->restore_info, dump_restore);

  int *restore = _restore_info, restore_n = 0, f = 0;
  for (i = 0; i < _restore_info_n; i++) {
    int x = restore[i];
    if (x < 0) {
      //dbg ("{%d}\n", CONV_ID (x));
      if (f) {
        restore_n--;
      }

      x = CONV_ID (x);
      x = x == -1 ? -1 : next[x];

      if (x != -1) {
        assert (new_local_id[x] >= hidden_ids_n);
        assert (next[x] == x);
      }
      x = x == -1 ? -1 : new_local_id[x];
      restore[restore_n++] = CONV_ID (x);
      f = 1;
    } else {
      //dbg ("---%d\n", x);
      x = new_local_id [x];

      if (x >= 0) {
        assert (x < hidden_ids_n);
        restore[restore_n++] = x;
        f = 0;
      }
    }
  }
  if (f == 1) {
    restore_n--;
  }

  assert ((int)sizeof (int) <= sn);
  WRITE_INT (s, restore_n);
  sn -= sizeof (int);

  size_t restore_size = sizeof (int) * restore_n;
  assert ((int)restore_size < sn);
  memcpy (s, restore, restore_size);
  s += restore_size;
  sn -= restore_size;


  dl_free (hidden_ids, hidden_ids_size);

  dl_free (perm, perm_size);
  dl_free (perm_flag, perm_size);
  dl_free (inv_perm, perm_size);
  dl_free (next, perm_size);

  dl_free (cx, c_size);
  dl_free (cy, c_size);

  dl_free (new_local_id, new_local_id_size);

  dl_free (_restore_info, _restore_info_size);
  dl_free (new_perm, new_perm_size);

  //dbg ("data_save: crc32 = %d\n", compute_crc32 (buf, s - buf));
  //DUMP (buf, s - buf);
  return s - buf;
}

void data_init (data *d, data_functions *f) {
  d->func = f;
  d->objects_n = -2;
  d->objects = NULL;
  d->objects_offset = NULL;
  d->new_objects_n = 0;
  lookup_init (&d->id_to_local_id);
  lookup_init (&d->hidden_state);
  //d->ids = NULL;
  //d->old_perm = NULL;
  PERM (init, &d->prm);
  map_int_vptr_init (&d->dyn_snapshots);
  map_int_vptr_init (&d->restore_info);
}

//TODO: init usually don't take a long time...
void data_iterator_init (data_iterator *it, data *d, int dir) {
  assert (dir == -1 || dir == +1);
  it->ids = d->id_to_local_id.x;
  it->old_perm = d->id_to_local_id.y;
  it->d = d;
  it->dir = dir;

  it->ids_n = d->objects_n;
  int new_n = it->new_ids_n = map_int_int_used (&d->id_to_local_id.new_v);

  it->new_ids = dl_malloc (new_n * sizeof (int));
  it->new_local_ids = dl_malloc (new_n * sizeof (int));

  assert (map_int_int_pairs (&d->id_to_local_id.new_v, it->new_ids, it->new_local_ids, new_n) == new_n);

  size_t b_size = sizeof (pair_int_int) * new_n;
  pair_int_int *b  = dl_malloc (b_size);
  int i;
  for (i = 0; i < new_n; i++) {
    b[i].x = it->new_ids[i];
    b[i].y = it->new_local_ids[i];
  }
  dl_qsort_pair_int_int (b, new_n);
  for (i = 0; i < new_n; i++) {
    it->new_ids[i] = b[i].x;
    it->new_local_ids[i] = b[i].y;
  }
  dl_free (b, b_size);

  if (dir < 0) {
    it->ids_i = it->ids_n - 1;
    it->ids_end = -1;

    it->new_ids_i = it->new_ids_n - 1;
    it->new_ids_end = -1;
  } else {
    it->ids_i = 0;
    it->ids_end = it->ids_n;

    it->new_ids_i = 0;
    it->new_ids_end = it->new_ids_n;
  }

}

int data_iterator_next (data_iterator *it) {
  //dbg ("data_iterator_next (ids_i = %d) (new_ids_i = %d)\n", it->ids_i, it->new_ids_i);
  while (it->ids_i != it->ids_end || it->new_ids_i != it->new_ids_end) {
    int id, local_id;
    if (it->new_ids_i == it->new_ids_end || (it->ids_i != it->ids_end && it->ids[it->ids_i] * it->dir <= it->new_ids[it->new_ids_i] * it->dir)) {
      if (it->new_ids_i != it->new_ids_end && it->ids[it->ids_i] == it->new_ids[it->new_ids_i]) {
        id = it->new_ids[it->new_ids_i];
        local_id = it->new_local_ids[it->new_ids_i];

        it->new_ids_i += it->dir;
        it->ids_i += it->dir;
      } else {
        id = it->ids[it->ids_i];
        local_id = it->old_perm[it->ids_i];
        it->ids_i += it->dir;
      }
    } else {
      id = it->new_ids[it->new_ids_i];
      local_id = it->new_local_ids[it->new_ids_i];
      it->new_ids_i += it->dir;
    }
    if (local_id >= 0 && data_get_hidden_state (it->d, id) == 0) {

      //dbg ("next (id = %d) (local_id = %d)\n", id, local_id);
      it->id = -id * it->dir;
      it->local_id = local_id;
      return 1;
    }
  }
  return 0;
}

void data_iterator_free (data_iterator *it) {
  dl_free (it->new_ids, sizeof (int) * it->new_ids_n);
  dl_free (it->new_local_ids, sizeof (int) * it->new_ids_n);
  memset (it, 0, sizeof (*it));
}

typedef struct {
  int val;
  int iter;
} val_iter_t;

#define dl_scmp_val_iter_t(a, b) ((a).val < (b).val)
vector (val_iter_t, h);

int iter_merge (data_iterator *its, int its_n, actual_object *r, int rn, int offset) {
  int i;

  if (offset < 0) {
    offset = 0;
  }

  h_size = 0;

  for (i = 0; i < its_n; i++) {
    if (data_iterator_next (&its[i])) {
      val_iter_t add = {its[i].id, i};
      //dbg ("add (%d;%d)\n", add.val, add.iter);
      vector_hpb (val_iter_t, h, add);
    }
  }

  int ri = -offset;
  while (ri < rn && h_size) {
    val_iter_t cur = vector_hpop (val_iter_t, h);
    //dbg ("extract (%d;%d)\n", cur.val, cur.iter);
    if (ri >= 0) {
      assert (data_get_actual_object (its[cur.iter].d, its[cur.iter].local_id, &r[ri]) > -1);
      r[ri].deleted = 0;
    }
    ri++;
    if (data_iterator_next (&its[cur.iter])) {
      val_iter_t add = {its[cur.iter].id, cur.iter};
      //dbg ("add (%d;%d)\n", add.val, add.iter);
      vector_hpb (val_iter_t, h, add);
    }
  }

  return ri;
}

void utils_free (void) {
  vector_free (h);
}
