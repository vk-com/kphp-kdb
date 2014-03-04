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

#include "dl.h"

#define perm dl_perm

#ifdef DL_DEBUG
#  define MAX_RESULT 15
#else
#  define MAX_RESULT (32767 * 32)
#endif
#ifdef DL_DEBUG
#  define LOOKUP_CNT
#endif

extern int _fix;
extern char _EMPTY__METAFILE[20];
extern char *EMPTY__METAFILE;
extern int EMPTY__METAFILE_LEN;

#define MAX_TMP_MEM 10000

void tmp_mem_init (void);
inline void *tmp_mem_alloc (int n);
char *tmp_mem_strdup (char *s);
char *tmp_mem_strdupn (char *s, int n);
char *tmp_mem_dup (char *s, int n);

#pragma pack(push, 4)

typedef struct {
  map_int_int new_v;

#ifdef LOOKUP_CNT
  int real_n;
#endif

  int *x, *y;
  int n;
} lookup;

#pragma pack(pop)

#define NULL_ALBUM 0

void lookup_init (lookup *l);
void lookup_free (lookup *l);
int lookup_save (lookup *l, char *buf, int buf_size, int none);
void lookup_unload (lookup *l);
void lookup_load (lookup *l, char *metafile, int metafile_len);
int lookup_conv (lookup *l, int x, int none);
void lookup_set (lookup *l, int x, int y);


/** kind of virtual classes **/
typedef void change;
typedef void object;
typedef void dyn_object;

#pragma pack(push, 4)

typedef struct {
  dyn_object *dyn;

  object *obj;

  int deleted;

  int obj_len;
} actual_object;

//TODO: off_t or size_t or int or something else
//      pack hash_tables if they are too sparse
typedef struct {
  int magic;
  char *title;
  int (*add_change) (dyn_object **o, change *c);
  void (*free_dyn) (int *i, dyn_object **o);

  // we may apply some changes to objects, after metafile is loaded
  //int (*object_onload) (actual_object *o);
  int (*object_save) (actual_object *o, char *buf, int buf_size);
  int (*object_get_id) (actual_object *o);

  //int (*object_apply) (actual_object *o);
  //int (*object_unload) (object *o);
} data_functions;

#define DATA_FUNC(d) ((d)->func)
typedef struct {
  data_functions *func;

  int objects_n;                  // on disk

  char *objects;                  // on disk
  int *objects_offset;            // on disk
  int new_objects_n;              // in mem

  // id->local_id
  //int *ids;                     // on disk
  //int *old_perm;                // on disk
  //map_int_int new_id_to_i;      // in mem
  lookup id_to_local_id;
  lookup hidden_state;

  perm prm;                       // in mem
  map_int_vptr dyn_snapshots;     // in mem

  map_int_vptr restore_info;      // in mem
} data;

int data_get_local_id_by_id (data *d, int id);
int data_get_pos_by_local_id (data *d, int local_id);
int data_get_local_id_by_pos (data *d, int pos);
int data_get_hidden_state (data *d, int id);
int data_hide (data *d, int id, int tm);
int data_hide_expected_size (data *d);
int data_del (data *d, int id);
int data_restore (data *d, int id);
int data_del_from_restore (data *d, int id);
int data_move (data *d, int i, int j);
int data_move_new (data *d, int i, int j, int local_i, int local_j);
int data_add_change (data *d, change *ch, int local_id);
int data_add_object (data *d, int id);
int data_get_actual_object (data *d, int local_id, actual_object *o);
int data_slice (data *d, actual_object *o, int n, int offset);
int data_slice_filtered (data *d, actual_object *o, int n, int offset, predicate *pred);
void data_load (data *d, char *metafile, int metafile_len);
int data_loaded (data *d);
//int data_is_empty (data *d);
int data_unload (data *d);
void data_free (data *d);
int data_save (data *d, char *buf, int buf_size, int max_objects_n, int *deleted, int *deleted_n, int deleted_max_n);
void data_init (data *d, data_functions *f);
int data_get_actual_objects (data *d, actual_object *o, int mx);
int data_get_cnt (data *d);
int data_get_ids (data *d, int *v, int mx);
int data_get_deleted_ids (data *d, int *v, int mx);


typedef struct {
  data *d;
  int *ids,     // from metafile
      *old_perm,// from metafile
      *new_ids;
  int dir;
  int *new_local_ids;
  int ids_n, new_ids_n,
      ids_i, new_ids_i,
      ids_end, new_ids_end;
  int id;
  int local_id;
} data_iterator;

void data_iterator_init (data_iterator *it, data *d, int dir);
int data_iterator_next (data_iterator *it);
void data_iterator_free (data_iterator *it);
int iter_merge (data_iterator *its, int its_n, actual_object *r, int rn, int offset);
void utils_free (void);

#pragma pack(pop)
