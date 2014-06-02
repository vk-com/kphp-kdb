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
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
              2011-2013 Vitaliy Valtman
*/

#define	_FILE_OFFSET_BITS	64       

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>

#include "net-connections.h"
#include "net-aio.h"

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kdb-lists-binlog.h"
#include "lists-data.h"
#include "lists-index-layout.h"
#include "server-functions.h"

#include "am-hash.h"
#include "vv-tl-aio.h"

#ifdef	DEBUG
# define inline
# define static
#endif

extern int now;
extern int verbosity;
extern int ignore_mode;

#define MAXINT  (0x7fffffff)
#define MAXLONG (0x7fffffffffffffffLL)

#ifdef LISTS64
# define MAX_OBJECT_ID MAXLONG
# define MAX_OBJECT_RES  (MAX_RES / 2)
# define MAX_LIST_ID MAXLONG
// #define Y_MULT     7047438495421301423LL
// #define Y_MULT_INV 8177354108323140687LL
# define idout	"%lld"
# define out_list_id(x)	(x)
# define out_object_id(x)	(x)
# define FIRST_INT(x)	((int)(x))
# define MAX_LIST_OBJECT_PAIR max_list_object_pair
ltree_x_t max_list_object_pair = {.list_id = (unsigned long long)-1LL, .object_id = MAXLONG};
#elif defined (LISTS_Z)
# define MAX_OBJECT_RES  (MAX_RES / object_id_ints)
# define MAX_OBJECT_ID max_object_id
# define MAX_LIST_ID max_list_id
# define MAX_LIST_OBJECT_PAIR max_list_object_pair
# define	idout	"%s"
extern char *out_list_id (list_id_t list_id);
extern char *out_object_id (object_id_t object_id);
# define FIRST_INT(x) ((x)[0])
#else
# define MAX_OBJECT_ID MAXINT
# define MAX_OBJECT_RES  MAX_RES
# define MAX_LIST_OBJECT_PAIR (MAXLONG)
# define MAX_LIST_ID MAXINT
# define	Y_MULT		1640859641
# define	Y_MULT_INV	265849417
# define idout	"%d"
# define out_list_id(x)	(x)
# define out_object_id(x)	(x)
# define FIRST_INT(x)	(x)
#endif

#ifdef LISTS_Z
# define IF_LISTS_Z(c)	(c)
#else
# define IF_LISTS_Z(c)	(1)
#endif

#ifdef	VALUES64
#define	valout	"%lld"
#else
#define	valout	"%d"
#endif

#define MIN_OBJECT_ID (~MAX_OBJECT_ID)

int lists_prime;
int max_lists;

int index_mode;
int remove_dates;
int now_override;
extern int metafile_mode;
int revlist_metafile_mode;
int crc32_check_mode;
extern char metafiles_order;

const char empty_string[] = {0};

int value_offset = -1;

extern int max_text_len;

int object_id_ints = 0, list_id_ints = 0;

#ifdef LISTS_Z
int object_list_ints = 0, ltree_node_size = -1, object_id_bytes = 0, list_id_bytes = 0, object_list_bytes = 0;
#define list_object_bytes	object_list_bytes
#define list_object_ints	object_list_ints
#define COPY_LIST_ID(dest,src)	memcpy ((dest), (src), list_id_bytes)
int payload_offset = -1, tree_ext_small_node_size = -1, tree_ext_large_node_size = -1;
int tree_ext_global_node_size = -1;
int list_struct_size = -1;
int cyclic_buffer_entry_size = -1, cyclic_buffer_entry_ints = -1;
int file_list_index_entry_size = -1;
int max_object_id[MAX_OBJECT_ID_INTS];
int max_list_id[MAX_LIST_ID_INTS];
int max_list_object_pair[MAX_OBJECT_ID_INTS + MAX_LIST_ID_INTS];
#define PAYLOAD(T) ((struct tree_payload *)(((char *)(T)) + payload_offset))
#define OARR_ENTRY(A,i) ((A) + object_id_ints * (long) (i))
#define OARR_ENTRY_ADJ(A,i) ((A) + object_id_ints_adjusted * (long) (i))
#define check_debug_list(list_id) (0)
#define check_ignore_list(list_id) (0)
#define	FLI_ADJUST(p)	((struct file_list_index_entry *) ((char *) (p) + list_id_bytes))
#define	FLI_ENTRY(i)	((char *) FileLists + (long) (i) * file_list_index_entry_size)
#define	FLI_ENTRY_ADJUSTED(i)	((struct file_list_index_entry *) (FileLists_adjusted + (long) (i) * file_list_index_entry_size))
#define	FLI_LIST_ID(p)	((list_id_t) (p))
#define	NEW_FLI_ENTRY(i)	((char *) NewFileLists + (long) (i) * file_list_index_entry_size)
#define	NEW_FLI_ENTRY_ADJUSTED(i)	((struct file_list_index_entry *) (NewFileLists_adjusted + (long) (i) * file_list_index_entry_size))
#define	MF_ADJUST(p)	((metafile_t *) ((char *) (p) + list_id_bytes))
#define	MF_READJUST(p)	((metafile_t *) ((char *) (p) - list_id_bytes))
#define	MF_MAGIC(p)	(MF_READJUST(p)->magic)
#define	MF_LIST_ID(p)	(MF_READJUST(p)->list_id)
#define	REVLIST_PAIR(RData,i)	((int *) ((char *) RData + (long) (i) * object_list_bytes))
#define	REVLIST_OBJECT_ID(RData,i)	((int *) ((char *) REVLIST_PAIR(RData,i) + list_id_bytes))
#define	REVLIST_LIST_ID(RData,i)	REVLIST_PAIR(RData,i)
#define	lev_list_id_bytes	list_id_bytes
#define	lev_list_object_bytes	object_list_bytes
#define	lev_object_id_bytes	object_id_bytes
#define	LEV_OBJECT_ID(E)	((int *) ((char *) (E) + list_id_bytes + 4))
#define	LEV_ADJUST_LO(E)	((void *) ((char *) (E) + list_object_bytes))
#define	LEV_ADJUST_L(E)		((void *) ((char *) (E) + list_id_bytes))
#else
#define COPY_LIST_ID(dest,src)	(dest) = (src)
#define ltree_node_size (sizeof (ltree_t))
#define tree_ext_small_node_size (sizeof (tree_ext_small_t))
#define tree_ext_global_node_size (sizeof (tree_ext_global_t))
#define tree_ext_large_node_size (sizeof (tree_ext_large_t))
#define list_struct_size (sizeof (list_t))
#define PAYLOAD(T) (&((T)->payload))
#define OARR_ENTRY(A,i) ((A)[(i)])
#define OARR_ENTRY_ADJ(A,i) ((A)[(i)*object_id_ints_adjusted])
#define cyclic_buffer_entry_size (sizeof (struct cyclic_buffer_entry))
#define cyclic_buffer_entry_ints (sizeof (struct cyclic_buffer_entry) / 4)
#define	file_list_index_entry_size (sizeof (struct file_list_index_entry))
#define check_debug_list(list_id) ((list_id) == debug_list_id)
#define check_ignore_list(list_id) ((list_id) == ignored_list2)
#define	FLI_ADJUST(p)	(p)
#define	FLI_ENTRY(i)	(FileLists + (i))
#define	FLI_ENTRY_ADJUSTED(i)	(FLI_ADJUST(FLI_ENTRY(i)))
#define	FLI_LIST_ID(p)	((p)->list_id)
#define	NEW_FLI_ENTRY(i)	(NewFileLists + (i))
#define	NEW_FLI_ENTRY_ADJUSTED(i)	(FLI_ADJUST(NEW_FLI_ENTRY(i)))
#define MF_ADJUST(p)	(p)
#define MF_READJUST(p)	(p)
#define	MF_MAGIC(p)	((p)->magic)
#define	MF_LIST_ID(p)	((p)->list_id)
#define	REVLIST_PAIR(RData,i)	RData[i].pair
#define	REVLIST_OBJECT_ID(RData,i)	RData[i].object_id
#define	REVLIST_LIST_ID(RData,i)	RData[i].list_id
#define	lev_list_id_bytes	0
#define	lev_list_object_bytes	0
#define	lev_object_id_bytes	0
#define	LEV_OBJECT_ID(E)	((E)->object_id)
#define	LEV_ADJUST_LO(E)	(E)
#define	LEV_ADJUST_L(E)		(E)
#endif

#define LPAYLOAD(T) (PAYLOAD(LARGE_NODE(T)))
#define	FLI_ENTRY_LIST_ID(i)	(FLI_LIST_ID(FLI_ENTRY(i)))
#define	NEW_FLI_ENTRY_LIST_ID(i)	(FLI_LIST_ID(NEW_FLI_ENTRY(i)))


long long malloc_memory;
extern int disable_revlist;

void *zzmalloc (int size) {
  malloc_memory += size;
  void *r = malloc (size);
  assert (r);
  return r;
}

void zzfree (void *ptr, int size) {
  malloc_memory -= size;
  free (ptr);
}

int *CB;
int id_ints;

/* ------ compute struct sizes -------- */

static void compute_struct_sizes (void) {
  assert (list_id_ints > 0 && list_id_ints <= MAX_LIST_ID_INTS);
  assert (object_id_ints > 0 && object_id_ints <= MAX_OBJECT_ID_INTS);
  // compute binlog record sizes
  
  // compute memory structure sizes

  #ifdef LISTS_Z
  int i;
  object_list_ints = object_id_ints + list_id_ints;
  ltree_node_size = sizeof (ltree_t) + object_list_ints * 4;
  object_id_bytes = object_id_ints * 4;
  list_id_bytes = list_id_ints * 4;
  object_list_bytes = object_id_bytes + list_id_bytes;
  list_struct_size = offsetof (list_t, list_id) + list_id_bytes;
  payload_offset = tree_ext_small_node_size = offsetof (tree_ext_small_t, x) + object_id_bytes;
  for (i = 0; i < object_id_ints; i++) {
    max_object_id[i] = MAXINT;
  }
  for (i = 0; i < list_id_ints; i++) {
    max_list_id[i] = MAXINT;
  }
  for (i = 0; i < object_id_ints + list_id_ints; i++) {
    max_list_object_pair[i] = MAXINT;
  }
  #ifdef _LP64 
  if (payload_offset & 4) {
    payload_offset += 4;
  }
  #endif
  tree_ext_global_node_size = offsetof (tree_ext_global_t, z) + object_id_bytes;
  tree_ext_large_node_size = payload_offset + sizeof (struct tree_payload);

  cyclic_buffer_entry_size = sizeof (struct cyclic_buffer_entry) + list_id_bytes;
  cyclic_buffer_entry_ints = (cyclic_buffer_entry_size >> 2);

  file_list_index_entry_size = sizeof (struct file_list_index_entry) + list_id_bytes;
  #endif

  // compute offsets for data access

  assert (!CB);
  CB = zzmalloc (CYCLIC_BUFFER_SIZE * cyclic_buffer_entry_size);
  assert (CB);

}

int get_text_len (const char *text) {
  int a = *(unsigned char *)text;
  if (a <= 253) {
    return a;
  }
  assert (a == 254);
  return (*(int *)text) & 0x00ffffff;
}

char *get_text_ptr (char *text) {
//  int len = get_text_len (text);
  return (*(unsigned char *)text) == 0xfe ? text + 4 : text + 1;
}

int get_text_data_len (const char *text) {
  int len = get_text_len (text);
  return len <= 253 ? len + 1 : len + 4;
}

/* --------- debug output --------------- */

#ifdef	LISTS_Z 
static inline char *out_int_vector (const int *A, int len) {
  static char buff[65536];
  static char *wptr = buff;
  int s = len * 12, i;
  if (wptr + s > buff + 65536) {
    wptr = buff;
  }
  char *res = wptr;
  assert (len > 0 && s < 65536);
  for (i = 0; i < len; i++) {
    wptr += sprintf (wptr, "%d:", A[i]);
  }
  wptr[-1] = 0;
  return res;
}  


char *out_list_id (list_id_t list_id) {
  return out_int_vector (list_id, list_id_ints);
}


char *out_object_id (object_id_t object_id) {
  return out_int_vector (object_id, object_id_ints);
}

#endif

/* --------- trees with 64-bit key ------ */

int alloc_ltree_nodes;

#ifdef LISTS64
static inline int ltree_x_less (ltree_x_t a, ltree_x_t b) {
  return a.object_id < b.object_id || (a.object_id == b.object_id && a.list_id < b.list_id);
}

static inline int ltree_x_compare (ltree_x_t a, ltree_x_t b) {
  if (a.object_id < b.object_id) {
    return -1;
  }
  if (a.object_id > b.object_id) {
    return 1;
  }
  if (a.list_id < b.list_id) {
    return -1;
  }
  if (a.list_id > b.list_id) {
    return 1;
  }
  return 0;
}

static inline int ltree_x_equal (ltree_x_t a, ltree_x_t b) {
  return a.object_id == b.object_id && a.list_id == b.list_id;
}
#elif defined (LISTS_Z)
static inline int ltree_x_less (ltree_x_t a, ltree_x_t b) {
  int i;
  for (i = 0; i < object_list_ints; i++) {
    if (a[i] < b[i]) {
      return 1;
    } else if (a[i] > b[i]) {
      return 0;
    }
  }
  return 0;
}
static inline int ltree_x_compare (ltree_x_t a, ltree_x_t b) {
  int i;
  for (i = 0; i < object_list_ints; i++) {
    if (a[i] < b[i]) {
      return -1;
    } else if (a[i] > b[i]) {
      return 1;
    }
  }
  return 0;
}
static inline int ltree_x_equal (ltree_x_t a, ltree_x_t b) {
  int i;
  for (i = 0; i < object_list_ints; i++) {
    if (a[i] != b[i]) {
      return 0;
    }
  }
  return 1;
}
#else
static inline int ltree_x_less (ltree_x_t a, ltree_x_t b) {
  return a < b;
}

static inline int ltree_x_compare (ltree_x_t a, ltree_x_t b) {
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}


static inline int ltree_x_equal (ltree_x_t a, ltree_x_t b) {
  return a == b;
}
#endif

static inline void combine_ltree_x (list_id_t list_id, object_id_t object_id, var_ltree_x_t *ltx) {
#ifdef LISTS64
  ltx->list_id = list_id;
  ltx->object_id = object_id;
#elif (defined (LISTS_Z))
  memcpy (*ltx, object_id, object_id_bytes);
  memcpy (((int *)(*ltx)) + object_id_ints, list_id, list_id_bytes);
#else
  *ltx = (((long long) object_id) << 32) + (unsigned) list_id;
#endif
}

static inline object_id_t ltree_x_object_id (ltree_x_t ltx) {
#ifdef LISTS64
  return ltx.object_id;
#elif (defined (LISTS_Z))
  return (int *)ltx;
#else
  return (object_id_t)(ltx >> 32);
#endif
}

static inline list_id_t ltree_x_list_id (ltree_x_t ltx) {
#ifdef LISTS64
  return ltx.list_id;
#elif (defined (LISTS_Z))
  return ((int *)ltx) + object_id_ints;
#else
  return (list_id_t)(ltx);
#endif
}


#ifdef LISTS64
static inline void upcopy_list_id (void *E, list_id_t list_id) {
  ((int *)E)[2] = (int)(list_id >> 32);
}
static inline void upcopy_object_id (void *E, object_id_t object_id) {
  ((int *)E)[2] = (int)(object_id >> 32);
}
static inline void upcopy_list_object_id (void *E, list_id_t list_id, object_id_t object_id) {
  ((int *)E)[2] = (int)(list_id >> 32);
  ((struct lev_new_entry *)E)->object_id = object_id;
}
#elif (defined (LISTS_Z))
static inline void upcopy_list_id (void *E, list_id_t list_id) {
  memcpy (((struct lev_new_entry *)E)->list_id + 1, list_id + 1, list_id_bytes - 4);
}
static inline void upcopy_object_id (void *E, object_id_t object_id) {
  memcpy (((struct lev_del_obj *)E)->object_id + 1, object_id + 1, object_id_bytes - 4);
}
static inline void upcopy_list_object_id (void *E, list_id_t list_id, object_id_t object_id) {
  memcpy (((struct lev_new_entry *)E)->list_id + 1, list_id + 1, list_id_bytes - 4);
  memcpy (((struct lev_new_entry *)E)->list_id + list_id_ints, object_id, object_id_bytes);
}
#else
static inline void upcopy_list_id (void *E, list_id_t list_id) {
}
static inline void upcopy_object_id (void *E, list_id_t list_id) {
}
static inline void upcopy_list_object_id (void *E, list_id_t list_id, object_id_t object_id) {
  ((struct lev_new_entry *)E)->object_id = object_id;
}
#endif


static ltree_t *new_ltree_node (ltree_x_t x, int y) {
  ltree_t *P = zmalloc (ltree_node_size);
  assert (P);
  alloc_ltree_nodes++;
  P->left = P->right = 0;
  P->y = y;
  #ifndef LISTS_Z
  P->x = x;
  #else
  memcpy (P->x, x, object_list_bytes);
  #endif
  return P;
}

static void free_ltree_node (ltree_t *T) {
  assert (--alloc_ltree_nodes >= 0);
  zfree (T, ltree_node_size);
}

static ltree_t *ltree_lookup (ltree_t *T, ltree_x_t x) {
  while (T) {
    int c = ltree_x_compare (x, T->x);
    if (!c) {
      return T;
    }
    T = (c > 0 ? T->right : T->left);
  }
  return T;
}


static ltree_t *ltree_insert (ltree_t *T, ltree_x_t x, int y) {
  ltree_t *Root = T, **U = &Root, **L, **R;

  while (T && T->y >= y) {
    U = ltree_x_less (x, T->x) ? &T->left : &T->right;
    T = *U;
  }

  *U = new_ltree_node (x, y);

  L = &(*U)->left;
  R = &(*U)->right;

  while (T) {
    if (ltree_x_less (x, T->x)) {
      *R = T;
      R = &T->left;
      T = *R;
    } else {
      *L = T;
      L = &T->right;
      T = *L;
    }
  }

  *L = *R = 0;

  return Root;
}

static ltree_t *ltree_merge (ltree_t *L, ltree_t *R) {
  ltree_t *Root, **U = &Root;

  while (L && R) {
    if (L->y > R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }

  *U = L ? L : R;

  return Root;
}

static ltree_t *ltree_delete (ltree_t *T, ltree_x_t x) {
  if (!T) {
    return 0;
  }
  ltree_t *Root = T, **U = &Root, *L, *R;
  if (!T) {
    return 0;
  }

  int r;

  while ((r = ltree_x_compare (x, T->x)) != 0) {
    U = (r < 0) ? &T->left : &T->right;
    T = *U;
    if (!T) {
      return Root;
    }
  }

  L = T->left;
  R = T->right;
  free_ltree_node (T);

  while (L && R) {
    if (L->y > R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }

  *U = L ? L : R;

  return Root;
}


/* ========= LISTREE FUNCTIONS ======== */

char *MData, *MDataEnd;

#define	NIL	((tree_ext_small_t *)&NIL_NODE)
#define	NILG	((tree_ext_global_t *)&NIL_NODE)
#define	NILL	((tree_ext_large_t *)&NIL_NODE)
#define	NILX	((tree_ext_xglobal_t *)&NIL_NODE)

const static struct tree_ext_small NIL_NODE = {
  .left = NIL, 
  .right = NIL, 
  .y = -1 << 31,
};


int alloc_large_nodes, alloc_global_nodes, alloc_small_nodes;

static int rpos_to_delta[4] = {0, 1, 0, -1};	// sin (k*pi/2)

#define	NODE_TYPE(p)	((p)->rpos & 3)
#define	NODE_RPOS(p)	((p)->rpos >> 2)
#define MAKE_RPOS(a,b)  (((a)<<2) + (b))
#define LARGE_NODE(T)   ((tree_ext_large_t *)(T))
#define GLOBAL_NODE(T)  ((tree_ext_global_t *)(T))
#define SMALL_NODE(T)   ((tree_ext_small_t *)(T))

static inline tree_ext_small_t *new_tree_subnode_small (object_id_t x, int y, int rpos) {
  tree_ext_small_t *P;
  P = zmalloc (tree_ext_small_node_size);
  assert (P);
  alloc_small_nodes++;
  P->left = P->right = NIL;
  P->y = y;
  P->rpos = rpos;
//  P->delta = rpos_to_delta[NODE_RPOS(p)];
  #ifdef LISTS_Z
  memcpy (P->x, x, object_id_bytes);
  #else
  P->x = x;
  #endif
  return P;
}

static inline tree_ext_large_t *new_tree_subnode_large (object_id_t x, int y, int rpos) {
  tree_ext_large_t * P;
  P = zmalloc (tree_ext_large_node_size);
  assert (P);
  alloc_large_nodes++;
  P->left = P->right = NILL;
  P->y = y;
  P->rpos = rpos;
//  P->delta = rpos_to_delta[NODE_RPOS(p)];
  #ifdef LISTS_Z
  memcpy (P->x, x, object_id_bytes);
  #else
  P->x = x;
  #endif
  memset (PAYLOAD (P), 0, sizeof (struct tree_payload));
  return P;
}


#ifdef LISTS_Z
static inline int object_id_less (object_id_t a, object_id_t b) {
  int i;
  for (i = 0; i < object_id_ints; i++) {
    if (a[i] < b[i]) {
      return 1;
    } else if (a[i] > b[i]) {
      return 0;
    }
  }
  return 0;
}
static inline int object_id_less_prefix (object_id_t a, object_id_t b) {
  int i;
  for (i = 0; i < id_ints; i++) {
    if (a[i] < b[i]) {
      return 1;
    } else if (a[i] > b[i]) {
      return 0;
    }
  }
  return 0;
}
static inline int object_id_compare (object_id_t a, object_id_t b) {
  int i;
  for (i = 0; i < object_id_ints; i++) {
    if (a[i] < b[i]) {
      return -1;
    } else if (a[i] > b[i]) {
      return 1;
    }
  }
  return 0;
}
static inline int object_id_compare_prefix (object_id_t a, object_id_t b) {
  int i;
  for (i = 0; i < id_ints; i++) {
    if (a[i] < b[i]) {
      return -1;
    } else if (a[i] > b[i]) {
      return 1;
    }
  }
  return 0;
}
static inline int object_id_equal (object_id_t a, object_id_t b) {
  int i;
  for (i = 0; i < object_id_ints; i++) {
    if (a[i] != b[i]) {
      return 0;
    }
  }
  return 1;
}
static inline int list_id_less (list_id_t a, list_id_t b) {
  int i;
  for (i = 0; i < list_id_ints; i++) {
    if (a[i] < b[i]) {
      return 1;
    } else if (a[i] > b[i]) {
      return 0;
    }
  }
  return 0;
}
static inline int list_id_compare (list_id_t a, list_id_t b) {
  int i;
  for (i = 0; i < list_id_ints; i++) {
    if (a[i] < b[i]) {
      return -1;
    } else if (a[i] > b[i]) {
      return 1;
    }
  }
  return 0;
}
static inline int list_id_equal (list_id_t a, list_id_t b) {
  int i;
  for (i = 0; i < list_id_ints; i++) {
    if (a[i] != b[i]) {
      return 0;
    }
  }
  return 1;
}
#else
static inline int object_id_less (object_id_t a, object_id_t b) {
  return a < b;
}
static inline int object_id_less_prefix (object_id_t a, object_id_t b) {
  return a < b;
}

static inline int object_id_compare (object_id_t a, object_id_t b) {
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}

static inline int object_id_compare_prefix (object_id_t a, object_id_t b) {
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}


static inline int object_id_equal (object_id_t a, object_id_t b) {
  return a == b;
}


static inline int list_id_less (list_id_t a, list_id_t b) {
  return a < b;
}

static inline int list_id_compare (list_id_t a, list_id_t b) {
  if (a < b) {
    return -1;
  }
  if (a > b) {
    return 1;
  }
  return 0;
}


static inline int list_id_equal (list_id_t a, list_id_t b) {
  return a == b;
}
#endif



static inline tree_ext_small_t *tree_ext_lookup (tree_ext_small_t *T, object_id_t x) {
  int p = 0;
  while (T != NIL && (p = object_id_compare (x, T->x)) != 0) {
    T = (p < 0) ? T->left : T->right;
  }
  return T;
}



static inline tree_ext_small_t *tree_ext_adjust_deltas (tree_ext_small_t *T, object_id_t x, int delta_incr) {
  int p;
  while (T != NIL && (p = object_id_compare (x, T->x)) != 0) {
    T->delta += delta_incr;
    T = (p < 0) ? T->left : T->right;
  }
  assert (T != NIL);
  T->delta += delta_incr;
  return T;
}


static inline void tree_ext_relax (tree_ext_small_t *T) {
  T->delta = T->left->delta + T->right->delta + rpos_to_delta[NODE_TYPE(T)];
}

static void tree_ext_split (tree_ext_small_t **L, tree_ext_small_t **R, tree_ext_small_t *T, object_id_t x) {
  if (T == NIL) { *L = *R = NIL; return; }
  if (object_id_less (x, T->x)) {
    *R = T;
    tree_ext_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_ext_split (&T->right, R, T->right, x);
  }
  tree_ext_relax (T);
}


static tree_ext_small_t *tree_ext_insert (tree_ext_small_t *T, object_id_t x, int y, tree_ext_small_t *N) {
  if (T->y > y) {
    if (object_id_less (x, T->x)) {
      T->left = tree_ext_insert (T->left, x, y, N);
    } else {
//    assert (x > T->x);
      T->right = tree_ext_insert (T->right, x, y, N);
    }
    tree_ext_relax (T);
    return T;
  }
  assert (object_id_equal (N->x, x) && N->y == y);
  tree_ext_split (&N->left, &N->right, T, x);
  tree_ext_relax (N);
  return N;
}


static tree_ext_small_t *tree_ext_merge (tree_ext_small_t *L, tree_ext_small_t *R) {
  if (L == NIL) { return R; }
  if (R == NIL) { return L; }
  if (L->y > R->y) {
    L->right = tree_ext_merge (L->right, R);
    tree_ext_relax (L);
    return L;
  } else {
    R->left = tree_ext_merge (L, R->left);
    tree_ext_relax (R);
    return R;
  }
}


static tree_ext_small_t *DeletedSubnode;

static tree_ext_small_t *tree_ext_delete (tree_ext_small_t *T, object_id_t x) {
  assert (T != NIL);
  int p = object_id_compare (x, T->x);
  if (!p) {
    tree_ext_small_t *N = tree_ext_merge (T->left, T->right);
    DeletedSubnode = T;
    return N;
  }
  if (p < 0) {
    T->left = tree_ext_delete (T->left, x);
  } else {
    T->right = tree_ext_delete (T->right, x);
  }
  tree_ext_relax (T);
  return T;
}


static inline void free_tree_ext_small_node (tree_ext_small_t *T) {
  alloc_small_nodes--;
  zfree (T, tree_ext_small_node_size);
}

static void free_tree_ext_small (tree_ext_small_t *T) {
  if (T != NIL) {
    free_tree_ext_small (T->left);
    free_tree_ext_small (T->right);
    free_tree_ext_small_node (T);
  }
}


static inline void free_tree_ext_large_node (tree_ext_large_t *T) {
  int tp = NODE_TYPE (T);
  struct tree_payload *P = PAYLOAD (T);
  if (tp == TF_ZERO || tp == TF_PLUS) {
    if (P->text && P->text != empty_string) {
      assert (P->text >= MDataEnd);
      zfree (P->text, get_text_data_len (P->text));
    }
    P->text = 0;
  } else {
    assert (!P->text);
  }
  alloc_large_nodes--;
  zfree (T, tree_ext_large_node_size);
}


/* GLOBAL TREES */
#ifdef LISTS_GT

static inline tree_ext_global_t *new_tree_subnode_global (global_id_t x, int y, int rpos, object_id_t z) {
  tree_ext_global_t *P;
  P = zmalloc (tree_ext_global_node_size);
  assert (P);
  alloc_global_nodes++;
  P->left = P->right = NILG;
  P->y = y;
  P->rpos = rpos;
  P->x = x;

  #ifdef LISTS_Z
  memcpy (P->z, z, object_id_bytes);
  #else
  P->z = z;
  #endif
//  P->delta = rpos_to_delta[NODE_RPOS(p)];
  return P;
}

static inline tree_ext_global_t *tree_ext_global_lookup (tree_ext_global_t *T, global_id_t x) {
  while (T != NILG && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}


static inline tree_ext_global_t *tree_ext_global_adjust_deltas (tree_ext_global_t *T, global_id_t x, int delta_incr) {
  while (T != NILG && x != T->x) {
    T->delta += delta_incr;
    T = (x < T->x) ? T->left : T->right;
  }
  assert (T != NILG);
  T->delta += delta_incr;
  return T;
}


static inline void tree_ext_global_relax (tree_ext_global_t *T) {
  T->delta = T->left->delta + T->right->delta + rpos_to_delta[NODE_TYPE(T)];
}


static void tree_ext_global_split (tree_ext_global_t **L, tree_ext_global_t **R, tree_ext_global_t *T, global_id_t x) {
  if (T == NILG) { *L = *R = NILG; return; }
  if (x < T->x) {
    *R = T;
    tree_ext_global_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_ext_global_split (&T->right, R, T->right, x);
  }
  tree_ext_global_relax (T);
}


static tree_ext_global_t *tree_ext_global_insert (tree_ext_global_t *T, global_id_t x, int y, tree_ext_global_t *N) {
  if (T->y > y) {
    if (x < T->x) {
      T->left = tree_ext_global_insert (T->left, x, y, N);
    } else {
//    assert (x > T->x);
      T->right = tree_ext_global_insert (T->right, x, y, N);
    }
    tree_ext_global_relax (T);
    return T;
  }
  assert (N->x == x && N->y == y);
  tree_ext_global_split (&N->left, &N->right, T, x);
  tree_ext_global_relax (N);
  return N;
}

static tree_ext_global_t *tree_ext_global_merge (tree_ext_global_t *L, tree_ext_global_t *R) {
  if (L == NILG) { return R; }
  if (R == NILG) { return L; }
  if (L->y > R->y) {
    L->right = tree_ext_global_merge (L->right, R);
    tree_ext_global_relax (L);
    return L;
  } else {
    R->left = tree_ext_global_merge (L, R->left);
    tree_ext_global_relax (R);
    return R;
  }
}

static tree_ext_global_t *DeletedSubnode_global;

static tree_ext_global_t *tree_ext_global_delete (tree_ext_global_t *T, global_id_t x) {
  assert (T != NILG);
  if (T->x == x) {
    tree_ext_global_t *N = tree_ext_global_merge (T->left, T->right);
    DeletedSubnode_global = T;
    return N;
  }
  if (x < T->x) {
    T->left = tree_ext_global_delete (T->left, x);
  } else {
    T->right = tree_ext_global_delete (T->right, x);
  }
  tree_ext_global_relax (T);
  return T;
}


static inline void free_tree_ext_global_node (tree_ext_global_t *T) {
  alloc_global_nodes--;
  zfree (T, tree_ext_global_node_size);
}


static void free_tree_ext_global (tree_ext_global_t *T) {
  if (T != NILG) {
    free_tree_ext_global (T->left);
    free_tree_ext_global (T->right);
    free_tree_ext_global_node (T);
  }
}
#endif

/* tree_and_list = (dynamic) tree + (static) list */

/*inline int listree_get_size (listree_t *U) {
  return U->N + U->root->delta;
}*/

/* returns kth value in (tree+list) in ascending order, k>=0
   if k >= (total size), returns -1
   if list is not present and is needed, returns -2
*/

typedef int (*report_tree)(tree_ext_small_t *node);
typedef int (*report_array)(listree_t *LT, int index);

report_tree in_tree;
report_array in_array;

static int listree_get_kth (listree_t *LT, int k) {
  tree_ext_small_t *T = *LT->root;
  int G_N = LT->N;
  if (k < 0 || k >= G_N + T->delta) {
    return -1;
  }
  int l = k;
  while (T != NIL) {
    /* T->lpos + T->left->delta = # of el. of joined list < T->x */
    if (l < G_N - NODE_RPOS(T) + T->left->delta) {
      T = T->left;
    } else if (l == G_N - NODE_RPOS(T) + T->left->delta && NODE_TYPE(T) != TF_MINUS) {
      return in_tree (T);
    } else {
      l -= T->left->delta + rpos_to_delta[NODE_TYPE(T)];
      T = T->right;
    }      
  }
  assert (l >= 0 && l < G_N);
  //assert (U->A);
  return in_array (LT, l);
}

static inline void listree_get_kth_last (listree_t *LT, int k) {
  tree_ext_small_t *T = *LT->root;
  listree_get_kth (LT, LT->N + T->delta - 1 - k);
}

/* returns position = (number of entries <= x) - 1 in given listree */
static int listree_get_pos_large (listree_direct_t *LD, object_id_t x, int inexact) {
  tree_ext_large_t *T = *LD->root;
  int G_N = LD->N;
  int sd = 0, a = 0, b = G_N - 1;
  while (T != NILL) {
    int c = G_N - NODE_RPOS(T);		// # of array elements < T->x, i.e. A[0]...A[c-1]
    /* T->lpos + T->left->delta = # of el. of joined list < T->x */
    int s = object_id_compare (x, T->x);
    if (s < 0) {
      T = T->left;
      b = c - 1;  // left subtree corresponds to [a .. c-1]
    } else if (!s) {
      assert (inexact || NODE_TYPE(T) != TF_MINUS);
      return c + sd + T->left->delta - (NODE_TYPE(T) == TF_MINUS);
    } else {
      a = c + (NODE_TYPE(T) != TF_PLUS);	// right subtree corresponds to [rl .. b]
      sd += T->left->delta + rpos_to_delta[NODE_TYPE(T)];
      T = T->right;
    }      
  }
  assert (a >= 0 && a <= b + inexact && b < G_N);
  b++;
  a--;
  while (b - a > 1) {
    int c = (a + b) >> 1;
    int s = object_id_compare (x, OARR_ENTRY (LD->A, c));
    if (s < 0) {
      b = c;
    } else if (s > 0) {
      a = c;
    } else {
      c += sd;
      assert (c >= 0 && c < G_N + (*LD->root)->delta);
      return c;
    }
  }
  assert (inexact);
  return a + sd;
}

/* returns position = (number of entries <= x) - 1 in given listree */
static int listree_get_pos_inderect (listree_t *LD, object_id_t x, int inexact) {
  tree_ext_small_t *T = *LD->root;
  int G_N = LD->N;
  int sd = 0, a = 0, b = G_N - 1;
  while (T != NIL) {
    int c = G_N - NODE_RPOS(T);		// # of array elements < T->x, i.e. A[0]...A[c-1]
    /* T->lpos + T->left->delta = # of el. of joined list < T->x */
    int s = object_id_compare (x, T->x);
    if (s < 0) {
      T = T->left;
      b = c - 1;  // left subtree corresponds to [a .. c-1]
    } else if (!s) {
      assert (inexact || NODE_TYPE(T) != TF_MINUS);
      return c + sd + T->left->delta - (NODE_TYPE(T) == TF_MINUS);
    } else {
      a = c + (NODE_TYPE(T) != TF_PLUS);	// right subtree corresponds to [rl .. b]
      sd += T->left->delta + rpos_to_delta[NODE_TYPE(T)];
      T = T->right;
    }      
  }
  assert (a >= 0 && a <= b + inexact && b < G_N);
  b++;
  a--;
  while (b - a > 1) {
    int c = (a + b) >> 1;
    int s = object_id_compare (x, OARR_ENTRY (LD->DA, LD->IA[c]));
    if (s < 0) {
      b = c;
    } else if (s > 0) {
      a = c;
    } else {
      c += sd;
      assert (c >= 0 && c < G_N + (*LD->root)->delta);
      return c;
    }
  }
  assert (inexact);
  return a + sd;
}


//int *RA, *RB;

/* returns elements a .. b (included) of merged list+tree */
static int listree_get_range_rec (tree_ext_small_t *T, listree_t *LT, int a, int b) {
  if (a > b) {
    return 1;
  }
  if (T == NIL) {
    while (a <= b) {
      in_array (LT, a++);
      //*RA++ = A[a++];
    }
    return 1;
  }
  int N = LT->N;
  int c = N - NODE_RPOS(T) + T->left->delta;		  /* # of el. of joined list < T->x */
  int c1 = c + (NODE_TYPE(T) == TF_MINUS ? 0 : 1);        /* # of el. of joined list <= T->x */
  int s = T->left->delta + rpos_to_delta[NODE_TYPE(T)];
  if (b < c) {
    return listree_get_range_rec (T->left, LT, a, b);
  }
  if (a >= c1) {
    return listree_get_range_rec (T->right, LT, a - s, b - s);
  }
  if (listree_get_range_rec (T->left, LT, a, c-1) < 0) {
    return -2;
  }
  /* now a < c1, b >= c, c <= c1 <= c+1 => a <= c, c1-1 <= b */
  if (c < c1) {
    in_tree (T);
    //*RA++ = T->x;
  }
  return listree_get_range_rec (T->right, LT, c1 - s, b - s);
}


// deletes some nodes (the nodes for which the virtual methods have returned false)
// returns 0 = ok, or the so-called "current_minus_node" (something deleted)
//
// if returns current_minus_node,  
//		  deletion is only partially complete:
//		  all listree elements up to current_minus_node have been already scanned,
//		  all listree elements after current_minus_node have not been scanned,
//		  current_minus_node is a new MINUS node corresponding to an array element to be deleted
// if returns 0,  execution is complete (*R contains new tree part of resulting listree)
//
// last_y is MAXINT for first call, otherwise it is equal to the value of y of the uplink
// a and b are array bounds of interval for current subtree, inclusive! (to traverse the whole tree&array, you need to set them to 0 and N - 1, respectively)
// traverses subtree with root in *R
// a and b are bounds of corresponding array part (A[a] .. A[b] inclusive)
static tree_ext_large_t *listree_delete_some_range_rec_large (tree_ext_large_t **R, listree_direct_t *LD, int a, int b, int last_y) {
  tree_ext_large_t *MN;
  if (SMALL_NODE (*R) == NIL) {
    int y;
    while (a <= b) {
      if (!in_array ((listree_t *)LD, a)) {
        // current_minus_node (aka MN) is created ONLY here, when we want to delete a pure array entry
        MN = new_tree_subnode_large (OARR_ENTRY (LD->A, a), y = lrand48 (), MAKE_RPOS (LD->N - a, TF_MINUS));
        if (y > last_y) {
          return MN;
        }
        *R = MN;
        tree_ext_relax (SMALL_NODE (*R));
        break;
      }
      a++;
    }
    if (SMALL_NODE (*R) == NIL) {
      return 0;
    }
  }

  tree_ext_large_t *T = *R;

  int N = LD->N; 
  int c = N - NODE_RPOS(T);		// # of array elements < T->x, i.e. A[0]...A[c-1]
  int lr = c - 1;			// left subtree corresponds to [a .. c-1]
  int rl = c + (NODE_TYPE(T) != TF_PLUS);	// right subtree corresponds to [rl .. b]

  assert (rl <= b + 1);

  MN = listree_delete_some_range_rec_large (&T->left, LD, a, lr, T->y);
  if (MN) {
    if (MN->y > last_y) {
      tree_ext_relax (SMALL_NODE (T));
      return MN;
    }
    tree_ext_split ((tree_ext_small_t **) &MN->left, (tree_ext_small_t **) &MN->right, SMALL_NODE (T), MN->x);
    *R = T = MN;
    tree_ext_relax (SMALL_NODE (T));
    c = N - NODE_RPOS(T);
    rl = c + 1; 
  }

  // now all listree elements up to T=*R have been processed, T and everything after haven't
  // c and rl are correct

  int delete_this = 0;

  if (NODE_TYPE(T) != TF_MINUS && !in_tree (SMALL_NODE (T))) {
    if (NODE_TYPE (T) == TF_PLUS) {
      delete_this = 1;
    } else {
      struct tree_payload *P = PAYLOAD (T);
      if (P->text) {
        if (P->text != empty_string) {
          zfree (P->text, get_text_data_len (P->text));
        }
        P->text = 0;
      }
      T->rpos |= TF_MINUS;
    }
    // T has to be relaxed afterwards, either by tree_ext_relax or by deleting
  }

  // here T itself has been processed, but its (possible) deletion has been delayed
  // we have to process the remainder to the right of T

  while (1) {
    MN = listree_delete_some_range_rec_large (&T->right, LD, rl, b, T->y); 
    if (delete_this) {
      *R = LARGE_NODE (tree_ext_merge (SMALL_NODE (T->left), SMALL_NODE(T->right)));
      free_tree_ext_large_node (T);
      T = *R;
      delete_this = 0;
    } else {
      tree_ext_relax (SMALL_NODE (T));
    }
    if (!MN || MN->y > last_y) {
      return MN;
    }
    tree_ext_split ((tree_ext_small_t **) &MN->left, (tree_ext_small_t **) &MN->right, SMALL_NODE (T), MN->x);
    *R = T = MN;
    tree_ext_relax (SMALL_NODE (T));
    c = N - NODE_RPOS(T);
    assert (c >= rl);
    rl = c + 1;
  }
}

/* elements k ... k+n-1 of joined list, returns # of written elements; k is zero-based */
static int listree_get_range (listree_t *LT, int k, int n) {
  tree_ext_small_t *T = *LT->root;
  int G_N = LT->N;
  int M = G_N + T->delta;
  n += k;
  if (k < 0) { 
    k = 0; 
  }
  if (n > M) {
    n = M;
  }
  if (n <= k) {
    return 0;
  }
  if (listree_get_range_rec (T, LT, k, n - 1) < 0) {
    return -2;
  } else {
    return n - k;
  }
}

/* elements k ... k+n-1 of reversed joined list, returns # of written elements */
/*static int listree_get_range_rev (listree_t *LT, int k, int n) {
  tree_ext_small_t *T = *LT->root;
  int G_N = LT->N;
  int M = G_N + T->delta;
  n += k;
  if (k < 0) { 
    k = 0; 
  }
  if (n > M) {
    n = M;
  }
  if (n <= k) {
    return 0;
  }
  if (listree_get_range_rec (T, LT, M - n, M - k - 1) < 0) {
    return -2;
  } else {
    return n - k;
  }
}*/


static int find_rpos_direct (listree_direct_t *LD, object_id_t object_id) {
  int l = -1, r = LD->N, x;

  /* A[N-i-1]<x<=A[N-i] means A[l] < x <= A[r]; i = N - r */
  while (r - l > 1) {
    x = (l + r) / 2;
    if (object_id_compare (object_id, OARR_ENTRY (LD->A, x)) <= 0) {
      r = x;
    } else {
      l = x;
    }
  }
  assert (r >= 0 && r <= LD->N);
  return LD->N - r;
}


#ifdef LISTS_GT
int find_rpos_global (listree_global_t *LG, global_id_t global_id) {
  int l = -1, r = LG->N, x;

  /* A[j]:=DA[IA[j]] ; A[N-i-1]<x<=A[N-i] means A[l] < x <= A[r]; i = N - r */
  while (r - l > 1) {
    x = (l + r) / 2;
    if (global_id <= (global_id_t)LG->DA[LG->IA[x]]) {
      r = x;
    } else {
      l = x;
    }
  }
  assert (r >= 0 && r <= LG->N);
  return LG->N - r;
}
#endif

int find_rpos_indirect (listree_t *LI, object_id_t object_id) {
  int l = -1, r = LI->N, x;

  /* A[j]:=DA[IA[j]] ; A[N-i-1]<x<=A[N-i] means A[l] < x <= A[r]; i = N - r */
  while (r - l > 1) {
    x = (l + r) / 2;
    if (object_id_compare (object_id, OARR_ENTRY (LI->DA, LI->IA[x])) <= 0) {
      r = x;
    } else {
      l = x;
    }
  }
  assert (r >= 0 && r <= LI->N);
  return LI->N - r;
}


static inline object_id_t get_data_direct (listree_direct_t *LD, int index) {
  return OARR_ENTRY (LD->A, index);
}

static inline object_id_t get_data_direct_compatible (listree_t *LT, int index) {
  return get_data_direct ((listree_direct_t *) LT, index);
}


#ifdef LISTS_GT
static inline int get_data_global (listree_global_t *LG, int index) {
  return LG->DA[LG->IA[index]];
}
#endif


static inline object_id_t get_data_indirect (listree_t *LI, int index) {
  return OARR_ENTRY (LI->DA, LI->IA[index]);
}


// deletes an existing node
static void listree_delete_small (listree_t *LI, object_id_t x, int y) {
  tree_ext_small_t **R = LI->root;
  int rpos;
  tree_ext_small_t *T = tree_ext_lookup (*R, x);

  if (T == NIL) {
    rpos = find_rpos_indirect (LI, x);
    assert (rpos > 0 && object_id_equal (get_data_indirect (LI, LI->N - rpos), x));
    tree_ext_small_t *node = new_tree_subnode_small (x, y, MAKE_RPOS (rpos, TF_MINUS));
    *R = tree_ext_insert (*R, node->x, node->y, node);
  } else {
    assert (NODE_TYPE(T) == TF_PLUS);
    DeletedSubnode = 0;
    *R = tree_ext_delete (*R, x);
    assert (DeletedSubnode == T);
    free_tree_ext_small_node (T);
  }
}


#ifdef LISTS_GT
static void listree_delete_global (listree_global_t *LG, global_id_t x, object_id_t z) {
  tree_ext_global_t **R = LG->root;
  int rpos;
  tree_ext_global_t *T = tree_ext_global_lookup (*R, x);

  if (T == NILG) {
    rpos = find_rpos_global (LG, x);
    assert (rpos > 0 && get_data_global (LG, LG->N - rpos) == x);
    tree_ext_global_t *node = new_tree_subnode_global (x, lrand48 (), MAKE_RPOS (rpos, TF_MINUS), z);
    *R = tree_ext_global_insert (*R, node->x, node->y, node);
  } else {
    assert (NODE_TYPE(T) == TF_PLUS);
    DeletedSubnode_global = 0;
    *R = tree_ext_global_delete (*R, x);
    assert (DeletedSubnode_global == T);
    free_tree_ext_global_node (T);
  }
}
#endif

// deletes an existing node
static void listree_delete_large (listree_direct_t *LD, object_id_t x) {
  tree_ext_large_t **R = LD->root;
  int rpos;
  tree_ext_large_t *T = LARGE_NODE (tree_ext_lookup (SMALL_NODE (*R), x));

  if (SMALL_NODE(T) == NIL) {
    rpos = find_rpos_direct (LD, x);
    assert (rpos > 0 && object_id_equal (get_data_direct (LD, LD->N - rpos), x));
    tree_ext_small_t *node = SMALL_NODE (new_tree_subnode_large (x, lrand48 (), MAKE_RPOS (rpos, TF_MINUS)));
    *R = LARGE_NODE (tree_ext_insert (SMALL_NODE (*R), node->x, node->y, node));
  } else {
    struct tree_payload *P;
    switch (NODE_TYPE(T)) {
//    case TF_REPLACED:
    case TF_ZERO:
      P = PAYLOAD (T);
      if (P->text) {
        if (P->text != empty_string) {
          zfree (P->text, get_text_data_len (P->text));
        }
        P->text = 0;
      }
      T->rpos |= TF_MINUS;	// execute recursive relax
      assert (tree_ext_adjust_deltas (SMALL_NODE (*R), x, -1) == SMALL_NODE(T));
      break;
    case TF_PLUS:
      DeletedSubnode = 0;
      *R = LARGE_NODE (tree_ext_delete (SMALL_NODE (*R), x));
      assert (DeletedSubnode == SMALL_NODE(T));
      free_tree_ext_large_node (T);
      break;
    default:
      assert (0);
    }
  }
}

// inserts a non-existing node
static void listree_insert_small (listree_t *LI, object_id_t x, int y) {
  tree_ext_small_t **R = LI->root;
  int rpos;
  tree_ext_small_t *T = tree_ext_lookup (*R, x);
  if (T == NIL) {
    rpos = find_rpos_indirect (LI, x);
    assert (!rpos || !object_id_equal (get_data_indirect (LI, LI->N - rpos), x));
    tree_ext_small_t *node = new_tree_subnode_small (x, y, MAKE_RPOS (rpos, TF_PLUS));
    *R = tree_ext_insert (*R, node->x, node->y, node);
  } else {
    assert (NODE_TYPE(T) == TF_MINUS);
    DeletedSubnode = 0;
    *R = tree_ext_delete (*R, x);
    assert (DeletedSubnode == T);
    free_tree_ext_small_node (T);
  }
}

#ifdef LISTS_GT
static void listree_insert_global (listree_global_t *LG, global_id_t x, object_id_t z) {
  tree_ext_global_t **R = LG->root;
  int rpos;
  tree_ext_global_t *T = tree_ext_global_lookup (*R, x);
  if (T == NILG) {
    rpos = find_rpos_global (LG, x);
    assert (!rpos || get_data_global (LG, LG->N - rpos) != x);
    tree_ext_global_t *node = new_tree_subnode_global (x, lrand48(), MAKE_RPOS (rpos, TF_PLUS), z);
    *R = tree_ext_global_insert (*R, node->x, node->y, node);
  } else {
    assert (NODE_TYPE(T) == TF_MINUS);
    DeletedSubnode_global = 0;
    *R = tree_ext_global_delete (*R, x);
    assert (DeletedSubnode_global == T);
    free_tree_ext_global_node (T);
  }
}
#endif

// inserts a non-existing node and returns pointer to its instance
static tree_ext_large_t *listree_insert_large (listree_direct_t *LD, object_id_t x) {
  tree_ext_large_t **R = LD->root;
  int rpos;
  tree_ext_small_t *T = tree_ext_lookup (SMALL_NODE (*R), x);
  if (T == NIL) {
    rpos = find_rpos_direct (LD, x);
    assert (!rpos || !object_id_equal (get_data_direct (LD, LD->N - rpos), x));
    tree_ext_large_t *node = new_tree_subnode_large (x, lrand48 (), MAKE_RPOS (rpos, TF_PLUS));
    *R = LARGE_NODE (tree_ext_insert (SMALL_NODE (*R), node->x, node->y, SMALL_NODE (node)));
    return node;
  } else {
    assert (NODE_TYPE(T) == TF_MINUS);
    T->rpos &= -4;	// TF_MINUS -> TF_ZERO
    assert (tree_ext_adjust_deltas (SMALL_NODE (*R), x, 1) == T);
    return LARGE_NODE (T);
  }
}


//replaces an EXISTING node (only for large trees) - returns pointer to tree element to be replaced
//DOES NOT guarantee, that all values are correctly loaded from metafile - it's caller's responsibility
static tree_ext_large_t *listree_replace_large (listree_direct_t *LD, object_id_t x) {
  tree_ext_large_t **R = LD->root;
  int rpos;
  tree_ext_small_t *T = tree_ext_lookup (SMALL_NODE (*R), x);
  if (T == NIL) {
    rpos = find_rpos_direct (LD, x);
    assert (rpos > 0 && object_id_equal (get_data_direct (LD, LD->N - rpos), x));
    tree_ext_large_t *node = new_tree_subnode_large (x, lrand48 (), MAKE_RPOS (rpos, TF_ZERO));
    *R = LARGE_NODE (tree_ext_insert (SMALL_NODE (*R), node->x, node->y, SMALL_NODE (node)));
    return node;
  } else {
    assert (NODE_TYPE (T) != TF_MINUS);
    return LARGE_NODE (T);
  }
}


static tree_ext_large_t *listree_lookup_large (listree_direct_t *LD, object_id_t x, int *index) {
  int rpos;
  tree_ext_large_t *T = LARGE_NODE (tree_ext_lookup (SMALL_NODE (*LD->root), x));
  if (SMALL_NODE (T) == NIL) {
    rpos = find_rpos_direct (LD, x);
    if (rpos == 0) {
      return 0;
    }
    rpos = LD->N - rpos;
    if (!object_id_equal (get_data_direct (LD, rpos), x)) {
      return 0;
    }
    *index = rpos;
    return (void *)-1;
  } else {
    if (NODE_TYPE (T) == TF_MINUS) {
      return 0;
    }
    return T;
  }
}

static tree_ext_large_t *listree_lookup_large_tree (listree_direct_t *LD, object_id_t x, int *index) {
  tree_ext_large_t *T = LARGE_NODE (tree_ext_lookup (SMALL_NODE (*LD->root), x));
  if (SMALL_NODE (T) == NIL) {
    return (void *)-1;
  } else {
    if (NODE_TYPE (T) == TF_MINUS) {
      return 0;
    }
    return T;
  }
}

/* --------- lists data ------------- */

int tot_lists;
global_id_t last_global_id;
long tot_list_entries;
int negative_list_id_offset;
int ignored_list2;

int debug_list_id = (-1L << 31);

ltree_t *object_tree;

//list_t *List[LISTS_PRIME];
list_t **List;

int lists_replay_logevent (struct lev_generic *E, int size);

int init_lists_data (int schema) {

  replay_logevent = lists_replay_logevent;

  memset (List, 0, sizeof(List));
  tot_lists = 0;
  tot_list_entries = 0;
  last_global_id = 0;
  object_tree = 0;

  assert (offsetof (struct list_tree_direct, N) == 0 && offsetof (struct list_tree_direct, root) == sizeof (void *));
  assert (offsetof (struct list_tree_global, N) == 0 && offsetof (struct list_tree_global, root) == sizeof (void *));
  assert (offsetof (struct list_tree_indirect, N) == 0 && offsetof (struct list_tree_indirect, root) == sizeof (void *));

  return 0;
}

static int lists_le_start (struct lev_start *E) {
  if (E->schema_id != LISTS_SCHEMA_CUR
#ifdef LISTS_Z
   && E->schema_id != LISTS_SCHEMA_V1
#endif
     ) {
    return -1;
  }

  int old_list_id_ints = -1, old_object_id_ints = -1;

  if (!log_split_mod) {
    assert (!log_split_min && !log_split_max);
    assert (!list_id_ints && !object_id_ints);
    log_split_min = E->split_min;
    log_split_max = E->split_max;
    log_split_mod = E->split_mod;
  } else {
    vkprintf (1, "warning: duplicate LEV_START at log position %lld, split %d..%d mod %d\n", log_cur_pos(), E->split_min, E->split_max, E->split_mod);
    assert (log_split_mod == E->split_mod);
    assert (log_split_min == E->split_min);
    assert (log_split_max == E->split_max);
    old_list_id_ints = list_id_ints;
    old_object_id_ints = object_id_ints;
  }
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);


  if (E->extra_bytes >= 6 && E->str[0] == 1) {
    struct lev_lists_start_ext *EX = (struct lev_lists_start_ext *) E;
    assert (EX->kludge_magic == 1 && EX->schema_id == LISTS_SCHEMA_V3);
    list_id_ints = EX->list_id_ints;
    object_id_ints = EX->object_id_ints;
    assert (EX->value_ints == sizeof (value_t) / 4);
    assert (!EX->extra_mask);
  } else {
#ifdef LISTS_Z
    if (E->schema_id != LISTS_SCHEMA_V1) {
      fprintf (stderr, "incorrect binlog for lists-x-engine");
      exit (1);
    } else {
      list_id_ints = object_id_ints = 1;
    }
#else
    list_id_ints = LIST_ID_INTS;
    object_id_ints = OBJECT_ID_INTS;
#endif
  }

#ifndef LISTS_Z
  assert (list_id_ints == 1 && object_id_ints == 1);
#endif

  if (old_object_id_ints > 0) {
    assert (object_id_ints == old_object_id_ints && list_id_ints == old_list_id_ints);
    assert (CB);
  } else {
    compute_struct_sizes ();
  }

  return 0;
}

/* =========== INDEX FUNCTIONS ===================== */

#define MAX_LISTS_INDEX_BYTES ((MAX_LISTS + 1) * file_list_index_entry_size)
#define	MAX_METAFILE_ENTRIES	(1 << 27)
#ifdef LISTS_Z
# define MIN_METAFILE_SIZE (sizeof (struct file_list_header) - sizeof (alloc_list_id_t) + list_id_bytes + 24)
#else
# define MIN_METAFILE_SIZE (sizeof (struct file_list_header) + 24)
#endif
#define METAFILE_HEADER(x) ((struct file_list_header *)(x))

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;

struct lists_index_header Header;

int idx_lists;
global_id_t idx_last_global_id;
long long idx_bytes, idx_loaded_bytes, idx_list_entries;
global_id_t first_extra_global_id = 1;
long long idx_fsize, idx_kfs_headers_size;
char *idx_filename;

struct file_list_index_entry *FileLists;
char *FileLists_adjusted;

char *MData, *MDataEnd;
file_revlist_entry_t *RData;

struct metafile {
  char *data;
  struct aio_connection *aio;
  int num;
  int next;
  int prev;
};

struct postponed_operation {
  struct postponed_operation *next;
  struct postponed_operation *prev;
  int size;
  int time;
  char E[0];
};
int head_metafile;
struct metafile **metafiles;
struct postponed_operation **postponed;


int tot_revlist_metafiles;
long long *revlist_metafiles_offsets;
file_revlist_entry_t *revlist_metafiles_bounds;
unsigned *revlist_metafiles_crc32;

struct revlist_iterator {
  file_revlist_entry_t *RData;
  int eof;
  long cur_pos;
  long items_num;
  int metafile_number;
  int change_metafile;
};

struct revlist_iterator revlist_iterator;

//int load_revlist_metafile_aio (int metafile_number, int use_aio);
int prepare_object_metafile_num (long rm, int use_aio);
static inline void *get_revlist_metafile_ptr (int p);
static inline long long get_revlist_metafile_offset (int p);
int check_revlist_metafile_loaded (int x, int use_aio);

int init_revlist_iterator (int metafile_number, int change_metafile) {
  vkprintf (2, "Loading revlist iterator at metafile %d of %d\n", metafile_number, tot_revlist_metafiles);
  if (metafile_number == -1) {
    change_metafile = 0;
  }
  revlist_iterator.metafile_number = metafile_number;
  revlist_iterator.cur_pos = 0;
  revlist_iterator.change_metafile = change_metafile;
  if (metafile_number >= tot_revlist_metafiles) {
    revlist_iterator.eof = 1;
    return 0;
  }
  if (metafile_number == -1) {
    revlist_iterator.RData = RData;
    revlist_iterator.items_num = idx_list_entries;
  } else {
    if (!check_revlist_metafile_loaded (metafile_number, -1)) {
      //assert (load_revlist_metafile_aio (metafile_number, -1) >= 0);
      assert (prepare_object_metafile_num (metafile_number, -1) >= 0);
    }
    revlist_iterator.RData = get_revlist_metafile_ptr (metafile_number);
#ifdef LISTS_Z
    revlist_iterator.items_num = (get_revlist_metafile_offset (metafile_number + 1) - get_revlist_metafile_offset (metafile_number)) / object_list_bytes;
#else
    revlist_iterator.items_num = (get_revlist_metafile_offset (metafile_number + 1) - get_revlist_metafile_offset (metafile_number)) >> 3;
#endif
  }
  revlist_iterator.eof = (revlist_iterator.items_num <= revlist_iterator.cur_pos);
  if (change_metafile && revlist_iterator.eof) {
    return init_revlist_iterator (metafile_number + 1, change_metafile);
  }
  return !revlist_iterator.eof;
}

void update_revlist_use (int x);

int advance_revlist_iterator (void) {
  if (revlist_iterator.eof) {
    return 0;
  }
  revlist_iterator.cur_pos ++;
  if (revlist_iterator.cur_pos == revlist_iterator.items_num) {
    if (!revlist_iterator.change_metafile) {
      revlist_iterator.eof = 1;
      return 0;
    } else {
      return init_revlist_iterator (revlist_iterator.metafile_number + 1, revlist_iterator.change_metafile);
    }
  } else {
    if (revlist_metafile_mode && revlist_iterator.change_metafile) {
      update_revlist_use (revlist_iterator.metafile_number);
    }
    return 1;
  }
}

static long revlist_lookup_left (object_id_t x, file_revlist_entry_t *RData, int size);


int advance_revlist_iterator_id (object_id_t object_id) {
  revlist_iterator.cur_pos = revlist_lookup_left (object_id, revlist_iterator.RData, revlist_iterator.items_num);
  if (revlist_iterator.cur_pos == revlist_iterator.items_num && revlist_iterator.change_metafile) {
    return init_revlist_iterator (revlist_iterator.metafile_number + 1, revlist_iterator.change_metafile);
  } else {
    revlist_iterator.eof = (revlist_iterator.cur_pos == revlist_iterator.items_num);
    return !revlist_iterator.eof;
  }
}

#define CUR_REVLIST_OBJECT_ID (REVLIST_OBJECT_ID(revlist_iterator.RData,revlist_iterator.cur_pos))
#define CUR_REVLIST_LIST_ID (REVLIST_LIST_ID(revlist_iterator.RData,revlist_iterator.cur_pos))
#define PREV_REVLIST_OBJECT_ID (REVLIST_OBJECT_ID(revlist_iterator.RData,revlist_iterator.cur_pos-1))
#define PREV_REVLIST_LIST_ID (REVLIST_LIST_ID(revlist_iterator.RData,revlist_iterator.cur_pos-1))


struct metafile_index {
  int idx;
  list_id_t *list_id;
};

struct metafile_index **metafile_indexes;

void mfi_sort (struct metafile_index **A, int b) {
  if (b <= 0) {
    return;
  }
  int i = 0, j = b;
  list_id_t *h = A[b>>1]->list_id;
  struct metafile_index *t;
  do {
    while (list_id_less (A[i]->list_id, h)) {
      i++;
    }
    while (list_id_less (h, A[j]->list_id)) {
      j--;
    }
    if (i <= j) {
      t = A[i];
      A[i] = A[j];
      A[j] = t;
      i++;
      j--;
    }
  } while (i <= j);
  mfi_sort (A, j);
  mfi_sort (A + i, b - i);
}

void make_metafile_indexes(void) {
    metafile_indexes = zzmalloc(sizeof (void *) * (Header.tot_lists + 1));
    int i;
    for(i = 0; i <= Header.tot_lists; i++) {
        metafile_indexes[i] = zzmalloc(sizeof(struct metafile_index));
        metafile_indexes[i]->idx = i;
        COPY_LIST_ID (metafile_indexes[i]->list_id, FLI_ENTRY_LIST_ID(i));
    }

    mfi_sort(metafile_indexes, Header.tot_lists - 1);

    for (i = 0; i < Header.tot_lists; i++) {
        assert(list_id_less (metafile_indexes[i]->list_id, metafile_indexes[i+1]->list_id));
        assert(list_id_equal(FLI_ENTRY_LIST_ID(metafile_indexes[i]->idx), metafile_indexes[i]->list_id));
    }
    assert(list_id_equal(MAX_LIST_ID, metafile_indexes[Header.tot_lists]->list_id));
}

void *load_metafile (void *data, long long offset, long long size, long max_size) {
  assert (size >= 0 && size <= (long long) (dyn_top - dyn_cur));
//  assert (size <= max_size || !max_size);
  assert (offset >= 0 && offset <= idx_fsize && offset + size <= idx_fsize);
  if (data == (void *)-1) {
    data = zzmalloc (size);
  }
  if (!data) {
    data = zmalloc (size);
    idx_bytes += size;
  }
  assert (lseek (Snapshot->fd, offset + idx_kfs_headers_size, SEEK_SET) == offset + idx_kfs_headers_size);
  char *r_data = data;
  while (size > 0) {
    long long r = kfs_read_file (Snapshot, r_data, size);
    if (r <= 0) {
      vkprintf (0, "error reading data from index file: read %lld bytes instead of %lld at position %lld: %m\n", r, size, offset + (r_data - (char *)data));
      assert (r == size);
      return 0;
    }
    r_data += r;
    size -= r;
    idx_loaded_bytes += r;
  }
  return data;
}

long long revlist_preloaded_bytes;
long long revlist_index_preloaded_bytes;

int noindex_init (void) {
  init_revlist_iterator (-1, 0);
  return 0;
}

void init_hash_table (int x) {
  x = (int)x * 1.5;
  lists_prime = am_choose_hash_prime ((x <= LISTS_PRIME ? LISTS_PRIME : x));
  List = malloc (lists_prime * sizeof (void *));
  memset (List, 0, lists_prime * sizeof (void *));
  max_lists = 0.7 * lists_prime;
}

int load_index (void) {
  int r;
  long i;
  long long fsize;

  assert (Snapshot);
  fsize = Snapshot->info->file_size - Snapshot->offset;

  r = kfs_read_file (Snapshot, &Header, sizeof (struct lists_index_header));
  if (r < sizeof (struct lists_index_header)) {
    vkprintf (0, "index file too short: only %d bytes read\n", r);
    return -2;
  }

  if (Header.magic != LISTS_INDEX_MAGIC && Header.magic != LISTS_INDEX_MAGIC_NEW && (Header.magic != LISTS_INDEX_MAGIC_NO_REVLIST || !disable_revlist)) {
    fprintf (stderr, "bad lists index file header\n");
    return -4;
  }

  if (Header.magic == LISTS_INDEX_MAGIC_NO_REVLIST) {
    assert (disable_revlist);
  }

  if ((Header.idx_mode & LIDX_HAVE_BIGVALUES) != LIDX_HAVE_BIGVALUES * (sizeof (value_t) / 4 - 1)) {
    fprintf (stderr, "value_t index size mismatch\n");
  }

  //assert ((unsigned) Header.tot_lists <= MAX_LISTS);
  init_hash_table (Header.tot_lists);
  assert (Header.last_global_id >= 0);
  last_global_id = idx_last_global_id = Header.last_global_id;
  first_extra_global_id = last_global_id + 1;


  //userlist_entry_size = sizeof (struct file_user_list_entry) + 4 * Header.sublists_num;

  assert (Header.list_index_offset == sizeof (struct lists_index_header));
  assert (Header.list_data_offset >= sizeof (struct lists_index_header) + sizeof (struct file_list_index_entry) * (Header.tot_lists + 1));
  assert (Header.revlist_data_offset >= Header.list_data_offset + Header.tot_lists * MIN_METAFILE_SIZE);
  assert (Header.revlist_data_offset <= Header.extra_data_offset);
  assert (Header.extra_data_offset <= Header.data_end_offset);
  assert (Header.data_end_offset == fsize);

#ifdef LISTS64
  fprintf (stderr, "index file unsupported in LISTS64 mode\n");
  return -4;
#endif
  
  assert (Header.kludge_magic == 0 || Header.kludge_magic == 1);
  if (!Header.kludge_magic) {
    assert (Header.magic == LISTS_INDEX_MAGIC);
    assert (!Header.list_id_ints && !Header.object_id_ints && !Header.value_ints && !(Header.params & (~1)));
    list_id_ints = 1;
    object_id_ints = 1;
  } else {
    assert (Header.list_id_ints > 0 && Header.list_id_ints <= MAX_LIST_ID_INTS);
    assert (Header.object_id_ints > 0 && Header.object_id_ints <= MAX_OBJECT_ID_INTS);
    assert (Header.value_ints == 1 || Header.value_ints == 2);
    assert (!(Header.params & ~(PARAMS_HAS_REVLIST_OFFSETS | PARAMS_HAS_CRC32)));  // NO EXTRA MASK YET
    assert (Header.value_ints * 4 == sizeof (value_t));
#ifndef LISTS_Z
    assert (Header.list_id_ints == 1 && Header.object_id_ints == 1);
#endif
    list_id_ints = Header.list_id_ints;
    object_id_ints = Header.object_id_ints;
  }

  compute_struct_sizes ();
  
  idx_filename = strdup (Snapshot->info->filename);
  idx_fsize = fsize;
  idx_kfs_headers_size = Snapshot->offset;
  idx_loaded_bytes = 0;
  idx_lists = Header.tot_lists;
#ifdef LISTS_Z
  idx_list_entries = (Header.extra_data_offset - Header.revlist_data_offset) / object_list_bytes;
#else
  idx_list_entries = (Header.extra_data_offset - Header.revlist_data_offset) >> 3;
#endif

  FileLists = load_metafile (0, Header.list_index_offset, (Header.tot_lists + 1) * file_list_index_entry_size, MAX_LISTS_INDEX_BYTES);

#ifdef LISTS_Z
  FileLists_adjusted = (char *) FileLists + list_id_bytes;
#endif

  revlist_metafile_mode = metafile_mode && (Header.params & PARAMS_HAS_REVLIST_OFFSETS) && Header.tot_revlist_metafiles && !disable_revlist;
  crc32_check_mode = Header.params & PARAMS_HAS_CRC32;

  if (crc32_check_mode) {
    assert (compute_crc32 (&Header, sizeof (Header) - 4) == Header.header_crc32);
    assert (compute_crc32 (FileLists, (Header.tot_lists + 1) * file_list_index_entry_size) == Header.filelist_crc32);
  }
  //revlist_metafile_mode = (Header.extra_mask & 1) && Header.tot_revlist_metafiles;
  vkprintf (1, "metafile_mode = %d\n", metafile_mode);
  vkprintf (1, "revlist_metafile_mode = %d\n", revlist_metafile_mode);

  assert (FLI_ENTRY_ADJUSTED(0)->list_file_offset >= Header.list_data_offset);
  for (i = 0; i < Header.tot_lists; i++) {
    if (FLI_ENTRY_ADJUSTED(i)->list_file_offset + MIN_METAFILE_SIZE > FLI_ENTRY_ADJUSTED(i+1)->list_file_offset) {
      fprintf (stderr, "%ld\n", i);
    }
    assert (FLI_ENTRY_ADJUSTED(i)->list_file_offset + MIN_METAFILE_SIZE <= FLI_ENTRY_ADJUSTED(i+1)->list_file_offset);
    /*
    if (!list_id_less (FLI_ENTRY_LIST_ID(i), FLI_ENTRY_LIST_ID(i+1))) {
      fprintf (stderr, "error in list index (%d entries): entry(%ld)=" idout " >= entry(%ld)=" idout "\n",
        Header.tot_lists,
        i, out_list_id (FLI_ENTRY_LIST_ID(i)), 
        i + 1, out_list_id (FLI_ENTRY_LIST_ID(i + 1)));
      assert (list_id_less (FLI_ENTRY_LIST_ID(i), FLI_ENTRY_LIST_ID(i+1)));
    }
    */
    assert ((unsigned) FLI_ENTRY_ADJUSTED(i)->flags < 16);
  }
  assert (FLI_ENTRY_ADJUSTED(i)->list_file_offset == Header.revlist_data_offset);
  assert (list_id_equal (FLI_ENTRY_LIST_ID(i), MAX_LIST_ID));

  if (metafile_mode) {
    if (!revlist_metafile_mode) {
      head_metafile = idx_lists;
    } else {
      head_metafile = idx_lists + 1 + Header.tot_revlist_metafiles;
    }
    metafiles = zmalloc0 (sizeof (struct metafile *) * (head_metafile + 1));
    metafiles[head_metafile] = zmalloc (sizeof (struct metafile));
    metafiles[head_metafile]->prev = head_metafile;
    metafiles[head_metafile]->next = head_metafile;
    postponed = zmalloc0 (sizeof (struct postponed_operation *) * (idx_lists));
    assert (postponed);
  } else {
    if (revlist_metafile_mode) {
      head_metafile = idx_lists + Header.tot_revlist_metafiles + 1;
      metafiles = zmalloc0 (sizeof (struct metafile *) * (head_metafile + 1));
      metafiles[head_metafile] = zmalloc (sizeof (struct metafile));
      metafiles[head_metafile]->prev = head_metafile;
      metafiles[head_metafile]->next = head_metafile;
    }
    MData = load_metafile (0, Header.list_data_offset, Header.revlist_data_offset - Header.list_data_offset, dyn_top - dyn_cur);
    MDataEnd = MData + Header.revlist_data_offset - Header.list_data_offset;
  }

  if (!revlist_metafile_mode) {
    if (!disable_revlist) {
      RData = load_metafile ((void *)-1, Header.revlist_data_offset, Header.extra_data_offset - Header.revlist_data_offset, sizeof (long) == 4 ? MAXINT : MAXLONG);
      revlist_preloaded_bytes = Header.extra_data_offset - Header.revlist_data_offset;
    } else {
      lseek (Snapshot->fd, Header.extra_data_offset, SEEK_SET);
    }
  } else {
    tot_revlist_metafiles = Header.tot_revlist_metafiles;
    lseek (Snapshot->fd, Header.extra_data_offset, SEEK_SET);
    revlist_metafiles_offsets = zzmalloc (sizeof (long long) * (tot_revlist_metafiles + 1));

    assert (kfs_read_file (Snapshot, revlist_metafiles_offsets, sizeof (long long) * (tot_revlist_metafiles + 1)) == sizeof (long long) * (tot_revlist_metafiles + 1));
    long long pos = Header.extra_data_offset + sizeof (long long) * (tot_revlist_metafiles + 1);
    long long pos2 = (crc32_check_mode) ? Header.data_end_offset - sizeof (unsigned) * (tot_revlist_metafiles + 1) : Header.data_end_offset ;
    revlist_metafiles_bounds = zzmalloc (Header.data_end_offset - pos);
    assert (kfs_read_file (Snapshot, revlist_metafiles_bounds, pos2 - pos) == pos2 - pos);
    if (crc32_check_mode) {
      revlist_metafiles_crc32 = zzmalloc (sizeof (unsigned) * (tot_revlist_metafiles + 1));
      assert (kfs_read_file (Snapshot, revlist_metafiles_crc32, sizeof (unsigned) * (tot_revlist_metafiles + 1)) == sizeof (unsigned) * (tot_revlist_metafiles + 1));
      unsigned t = compute_crc32 (revlist_metafiles_offsets, (tot_revlist_metafiles + 1) * sizeof (long long));
      assert (Header.revlist_arrays_crc32 ==  ~crc32_partial (revlist_metafiles_bounds, pos2 - pos, ~t));  
    }
    revlist_index_preloaded_bytes = Header.data_end_offset - Header.extra_data_offset;
  }

  if (!revlist_metafile_mode) {
    if (!disable_revlist) {
      init_revlist_iterator (-1, 0);
      while (advance_revlist_iterator ()) {
        int c = object_id_compare (PREV_REVLIST_OBJECT_ID, CUR_REVLIST_OBJECT_ID);
        if (c > 0) {
          fprintf (stderr, "error in revlist (%lld entries): entry(%ld)=" idout "#" idout " >= entry(%ld)=" idout "#" idout "\n",
            idx_list_entries,
            revlist_iterator.cur_pos - 1, out_object_id (PREV_REVLIST_OBJECT_ID), out_list_id (PREV_REVLIST_LIST_ID), 
            revlist_iterator.cur_pos, out_object_id (CUR_REVLIST_OBJECT_ID), out_list_id (CUR_REVLIST_LIST_ID));
          assert (c < 0);
        }
      }
    }
    /*for (i = 1; i < idx_list_entries; i++) {
      int c = object_id_compare (REVLIST_OBJECT_ID(RData,i-1), REVLIST_OBJECT_ID(RData,i));
      if (c > 0) {
        fprintf (stderr, "error in revlist (%lld entries): entry(%ld)=" idout "#" idout " >= entry(%ld)=" idout "#" idout "\n",
          idx_list_entries,
          i-1, out_object_id (REVLIST_OBJECT_ID(RData,i-1)), out_list_id (REVLIST_LIST_ID(RData,i-1)), 
          i, out_object_id (REVLIST_OBJECT_ID(RData,i)), out_list_id (REVLIST_LIST_ID(RData,i)));
        assert (c < 0);
      }
    }*/
  } else {
    for (i = 0; i < tot_revlist_metafiles; i++) {
      assert (revlist_metafiles_offsets[i] >= Header.revlist_data_offset && revlist_metafiles_offsets[i] <= Header.extra_data_offset);
      assert (revlist_metafiles_offsets[i] <= revlist_metafiles_offsets[i + 1]);
    }
    assert (revlist_metafiles_offsets[tot_revlist_metafiles] == Header.extra_data_offset);
    assert (revlist_metafiles_offsets[0] == Header.revlist_data_offset);
  }

  if (verbosity > 0) {
    fprintf (stderr, "finished loading index: %lld index bytes, %lld preloaded bytes\n", idx_bytes, idx_loaded_bytes);
  }

  log_split_min = Header.log_split_min;
  log_split_max = Header.log_split_max;
  log_split_mod = Header.log_split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  log_schema = LISTS_SCHEMA_CUR;
  init_lists_data (log_schema);

  tot_list_entries = idx_list_entries;
  last_global_id = idx_last_global_id;

  replay_logevent = lists_replay_logevent;

  jump_log_ts = Header.log_timestamp;
  jump_log_pos = Header.log_pos1;
  jump_log_crc32 = Header.log_pos1_crc32;

  make_metafile_indexes();

  return 0;
}


static inline void *get_metafile_ptr (int p);

static inline void *get_metafile_ptr_old (int p) {
  assert (p >= 0 && p < Header.tot_lists);
  return MData + (FLI_ENTRY_ADJUSTED(p)->list_file_offset - Header.list_data_offset)
#ifdef LISTS_Z
	 + list_id_bytes
#endif	
  ;
}

static inline long long get_metafile_offset (int p) {
  assert (p >= 0 && p <= Header.tot_lists);
  return (FLI_ENTRY_ADJUSTED(p)->list_file_offset) + idx_kfs_headers_size;
}


static inline long long get_revlist_metafile_offset (int p) {
  assert (p >= 0 && p <= tot_revlist_metafiles);
  return revlist_metafiles_offsets[p];
}


list_t *M_List;
metafile_t *M_metafile;
long M_metafile_size;
int M_tot_entries;
array_object_id_t *M_obj_id_list;		// 0. object_id[temp_id], 0<=temp_id<tot_entries, sorted
global_id_t *M_global_id_list;				// 1. global_id[temp_id]
int *M_sorted_global_id_list;			// 2. list of temp_id's ordered by corresp. global_id
int *M_sublist_temp_id_list;			// 3. temp_id's of each sublist, written one after another
int *M_sublist_temp_id_by_global;		// 4. same, but each sublist ordered by global_id
value_t *M_values;				// 5. if LF_HAVE_VALUES set
int *M_dates;					// 6. if LF_HAVE_DATES set
int *M_text_offset;				// 7. if LF_HAVE_TEXTS set
unsigned char *M_flags_small;			// 8. if LF_BIGFLAGS not set
int *M_flags;					// 8. if LF_BIGFLAGS set

char *M_text_start, *M_text_end;
long M_text_min_offset, M_text_max_offset;

listree_direct_t OTree;

//!!!
static inline int check_metafile_assertions (list_id_t list_id, int l) {
  assert (0 <= l && l < Header.tot_lists);
  metafile_t *M = get_metafile_ptr (l);
  assert (MF_MAGIC(M) == FILE_LIST_MAGIC && list_id_equal (MF_LIST_ID(M), list_id));
  assert (M->flags == FLI_ENTRY_ADJUSTED(l)->flags);
  assert ((unsigned) M->tot_entries <= MAX_METAFILE_ENTRIES);
  return 0;
}

int find_metafile (list_id_t list_id) {
  int l = -1, r = Header.tot_lists;
  int p = -1;
  while (r - l > 1) { // M[l] <= list_id < M[r] 
    int x = (l + r) >> 1;
    //fprintf (stderr, "%d %d %d %d %d\n", l, r, x, list_id, FLI_ENTRY_LIST_ID(x));
    p = list_id_compare (list_id, metafile_indexes[x]->list_id);
    if (p < 0) {
      r = x;
    } else {
      l = x;
    }
  }
  if (l >= 0 && list_id_equal (list_id, metafile_indexes[l]->list_id)) {
    return metafile_indexes[l]->idx;
  } else {
    return -1;
  }
}

static void unpack_metafile_pointers_short (list_t *L) {
  M_List = L;
  OTree.root = &L->o_tree;
  M_metafile = 0;
  M_metafile_size = 0;
  M_tot_entries = 0;
  M_obj_id_list = 0;
  M_global_id_list = 0;
  M_sorted_global_id_list = 0;
  M_sublist_temp_id_list = 0;
  M_sublist_temp_id_by_global = 0;
  M_values = 0;
  M_dates = 0;
  M_text_offset = 0;
  M_flags_small = 0;
  M_flags = 0;
  M_text_min_offset = 0;
  M_text_max_offset = 0;
  M_text_start = 0;
  M_text_end = 0;
  OTree.N = 0;
  OTree.A = 0;
}

static void unpack_metafile_pointers (list_t *L) {
  metafile_t *M = L->metafile_index != -1 ? get_metafile_ptr (L->metafile_index) : 0;
  M_List = L;
  OTree.root = &L->o_tree;
  /*if (M == M_metafile) {
    return;
  }*/
  if (!M) {
    M_metafile = 0;
    M_metafile_size = 0;
    M_tot_entries = 0;
    M_obj_id_list = 0;
    M_global_id_list = 0;
    M_sorted_global_id_list = 0;
    M_sublist_temp_id_list = 0;
    M_sublist_temp_id_by_global = 0;
    M_values = 0;
    M_dates = 0;
    M_text_offset = 0;
    M_flags_small = 0;
    M_flags = 0;
    M_text_min_offset = 0;
    M_text_max_offset = 0;
    M_text_start = 0;
    M_text_end = 0;
    OTree.N = 0;
    OTree.A = 0;
    return;
  }
  assert (MF_MAGIC(M) == FILE_LIST_MAGIC);
  M_metafile = M;
  int p = L->metafile_index;
  M_metafile_size = FLI_ENTRY_ADJUSTED(p+1)->list_file_offset - FLI_ENTRY_ADJUSTED(p)->list_file_offset;
  int tot_entries = M->tot_entries, flags = M->flags, *ptr;
  vkprintf (3, "Metafile flags: %d, tot_entries = %d\n", flags, tot_entries);
  assert ((unsigned) tot_entries <= MAX_METAFILE_ENTRIES);
  OTree.N = M_tot_entries = tot_entries;
  OTree.A = M_obj_id_list = (array_object_id_t *)(ptr = M->data);
  ptr += tot_entries * OBJECT_ID_INTS;
  M_global_id_list = (global_id_t *)ptr;
  ptr += tot_entries * (sizeof (global_id_t) / sizeof (int));
  M_sorted_global_id_list = ptr;
  ptr += tot_entries;
  M_sublist_temp_id_list = ptr;
  ptr += tot_entries;
  M_sublist_temp_id_by_global = ptr;
  ptr += tot_entries;
  if (flags & LF_HAVE_VALUES) {
    M_values = (value_t *) ptr;
#ifdef VALUES64
    ptr += 2*tot_entries;
#else
    ptr += tot_entries;
#endif
  } else {
    M_values = 0;
  }
  if (flags & LF_HAVE_DATES) {
    M_dates = ptr;
    ptr += tot_entries;
  } else {
    M_dates = 0;
  }
  if (flags & LF_HAVE_TEXTS) {
    M_text_offset = ptr;
    ptr += tot_entries + 1;
  } else {
    M_text_offset = 0;
  }
  if (flags & LF_BIGFLAGS) {
    M_flags = ptr;
    M_flags_small = 0;
    M_text_start = (char *) (ptr + tot_entries);
  } else {
    M_flags_small = (unsigned char *) ptr;
    M_flags = 0;
    M_text_start = (char *) ptr + tot_entries;
  }
  M_text_end = (char *) MF_READJUST (M_metafile) + M_metafile_size;
  assert (M_text_start <= M_text_end);
  M_text_min_offset = M_text_start - (char *) MF_READJUST (M_metafile);
  M_text_max_offset = M_metafile_size;
}

inline static int metafile_get_sublist_size (int sublist) {
  assert (!(sublist & -8));
  int x = M_metafile ? M_metafile->sublist_size_cum[sublist+1] - M_metafile->sublist_size_cum[sublist] : 0;
  x += M_List->o_tree_sub[sublist]->delta;
  return x;
}


inline static value_t metafile_get_value (int temp_id) {
  return M_values ? M_values[temp_id] : 0;
}

inline static int metafile_get_date (int temp_id) {
  return M_dates ? M_dates[temp_id] : 0;
}

inline static char *metafile_get_text (int temp_id, int *text_len) {
  vkprintf (4, "metafile_get_text (%d, %p)\n", temp_id, text_len);

  if (!M_text_offset) {
    *text_len = 0;
    return 0;
  }
  int offset = M_text_offset[temp_id];
  int tmplen = M_text_offset[temp_id+1] - offset;
  assert (offset >= M_text_min_offset && M_text_offset[temp_id+1] <= M_text_max_offset);
  *text_len = tmplen;
  vkprintf (4, "text offset is valid, calculated text len %d\n", tmplen);
  return tmplen ? (char *) (MF_READJUST (M_metafile)) + offset : 0;
}


inline static int metafile_get_flags (int temp_id) {
  return M_flags ? M_flags[temp_id] : M_flags_small[temp_id];
}


static inline void load_g_tree (listree_xglobal_t *LX) {
  LX->N = M_tot_entries;
  LX->IA = M_sorted_global_id_list;
  LX->DA = M_global_id_list;
  LX->root = &M_List->g_tree;
}


static inline void load_o_tree_sub (listree_t *LI, int sublist) {
  assert (!(sublist & -8));
  LI->root = &M_List->o_tree_sub[sublist];
  if (M_metafile) {
    int curptr = M_metafile->sublist_size_cum[sublist], nxtptr = M_metafile->sublist_size_cum[sublist + 1];
    assert (curptr >= 0 && curptr <= nxtptr && nxtptr <= M_tot_entries);
    LI->N = nxtptr - curptr;
    LI->IA = M_sublist_temp_id_list + curptr;
    LI->DA = M_obj_id_list;
  } else {
    LI->N = 0;
    LI->IA = 0;
    LI->DA = 0;
  }
}


static inline void load_g_tree_sub (listree_xglobal_t *LX, int sublist) {
  assert (!(sublist & -8));
  LX->root = &M_List->g_tree_sub[sublist];
  if (M_metafile) {
    int curptr = M_metafile->sublist_size_cum[sublist], nxtptr = M_metafile->sublist_size_cum[sublist + 1];
    assert (curptr >= 0 && curptr <= nxtptr && nxtptr <= M_tot_entries);
    LX->N = nxtptr - curptr;
    LX->IA = M_sublist_temp_id_by_global + curptr;
    LX->DA = M_global_id_list;
  } else {
    LX->N = 0;
    LX->IA = 0;
    LX->DA = 0;
  }
}

// 0 <= temp_id < M_tot_entries
// clones entry with given id from metafile into OTree
// OTree must not contain node with this object_id
static tree_ext_large_t *clone_metafile_entry (int temp_id) {
  assert (temp_id >= 0 && temp_id < M_tot_entries);
  object_id_t object_id = OARR_ENTRY (M_obj_id_list, temp_id);
  tree_ext_large_t *T = listree_replace_large (&OTree, object_id);
  assert (NODE_TYPE (T) == TF_ZERO);
  struct tree_payload *P = PAYLOAD (T);
  P->global_id = M_global_id_list[temp_id];
  P->text = 0;
  P->flags = metafile_get_flags (temp_id);
  P->value = metafile_get_value (temp_id);
  P->date = metafile_get_date (temp_id);
  return T;
}

/* some revlist functions */

/*
static int revlist_lookup (long long x) {
  int l = -1, r = idx_list_entries, h;
  while (r - l > 1) {  // RData[l] <= x < RData[r]
    h = (r + l) >> 1;
    if (RData[h].pair <= x) {
      l = h;
    } else {
      r = h;
    }
  }
  if (l >= 0 && RData[l].pair == x) {
    return l;
  }
  return -1;
}
*/

static long revlist_lookup_left (object_id_t x, file_revlist_entry_t *RData, int size) {
  long l = -1, r = size, h;
  while (r - l > 1) {  // RData[l] < x <= RData[r]
    h = (r + l) >> 1;
    if (object_id_less (REVLIST_OBJECT_ID(RData,h), x)) {
      l = h;
    } else {
      r = h;
    }
  }
  return r;
}

static long choose_revlist_metafile (object_id_t x) {
  return revlist_lookup_left (x, revlist_metafiles_bounds, tot_revlist_metafiles); 
}


/* ---- atomic data modification/replay binlog ----- */
int prepare_list_metafile (list_id_t list_id, int use_aio);
int prepare_list_metafile_num (int x, int use_aio);
int mark_list_metafile (list_id_t list_id);
void postpone (int x, struct lev_generic *E);
int check_metafile_loaded (int x, int use_aio);
int check_revlist_metafile_loaded (int x, int use_aio);

int conv_list_id (list_id_t list_id) {
#if 1
  #ifdef LISTS_Z
  int t = list_id[0];
  #else
  int t = list_id;
  #endif
  if (t == MAXINT) { 
    return -1; 
  }
  t %= log_split_mod;
  if (t != log_split_min && t != -log_split_min) { 
    return -1; 
  }
# ifdef LISTS64
  int p1 = (int)list_id;
  int p2 = (int)(list_id >> 32);
  int h1 = (p1 * 239 + p2) % lists_prime;
  int h2 = (p1 * 17 + p2) % (lists_prime - 1);
# elif defined (LISTS_Z)
  int h1 = 0, h2 = 0, i;
  for (i = 0; i < list_id_ints; i++) {
    h1 = (h1 * 239 + list_id[i]) % lists_prime;
    h2 = (h2 * 17 + list_id[i] + 239) % (lists_prime - 1);
  }
# else
  int h1 = list_id % lists_prime;
  int h2 = (list_id * 17 + 239) % (lists_prime - 1);
# endif
  if (h1 < 0) {
    h1 += lists_prime;
  }
  if (h2 < 0) {
    h2 += lists_prime - 1;
  }
  ++h2;
  while (List[h1]) {
    if (list_id_equal (List[h1]->list_id, list_id)) {
      return h1;
    }
    h1 += h2;
    if (h1 >= lists_prime) {
      h1 -= lists_prime;
    }
  }
  assert (tot_lists < max_lists);
  return h1;
#else
  int t = list_id % log_split_mod;
  list_id += negative_list_id_offset;
  if (list_id < 0) { return -1; }
  if (t != log_split_min && t != -log_split_min) { return -1; }
  list_id /= log_split_mod;
  if (t < 0 && --list_id < 0) {
    return -1;
  }
  return list_id < max_lists ? list_id : -1;
#endif
}

static inline void free_list_struct (list_t *L) {
  zfree (L, list_struct_size);
}

static list_t DummyList = {
  .metafile_index = -1,
  .o_tree = NILL,
  .g_tree = NILX,
  .o_tree_sub = {NIL, NIL, NIL, NIL, NIL, NIL, NIL, NIL},
  .g_tree_sub = {NILX, NILX, NILX, NILX, NILX, NILX, NILX, NILX}
};

static list_t *create_dummy_list(list_id_t list_id, int metafile_index) {
    list_t *res = zzmalloc(list_struct_size);
    COPY_LIST_ID (res->list_id, list_id);
    res->metafile_index = metafile_index;
    res->o_tree = NILL;
    res->g_tree = NILX;
    int i;
    for (i = 0; i < SUBCATS; i++) {
      res->o_tree_sub[i] = NIL;
      res->g_tree_sub[i] = NILX;
    }
    return res;
}

static list_t *__get_list_f (list_id_t list_id, int force) {
  int i = conv_list_id (list_id), k, metafile_index;
  list_t *L;
  if (i < 0) { return 0; }
  L = List[i];
  if (L || force < 0) { return L; }

  metafile_index = find_metafile (list_id);

  if (force != 1 && metafile_index < 0) {
    return 0;
  }

  if (force == 2) {
    L = &DummyList;
    L->metafile_index = metafile_index;
    #ifdef LISTS_Z
    memcpy (L->list_id, list_id, list_id_bytes);
    #else
    L->list_id = list_id;
    #endif
    return L;
  }

  vkprintf (4, "Allocating new list with id " idout "\n", out_list_id (list_id));
  L = zmalloc (list_struct_size);
//  memset (L, 0, offsetof (list_t, list_id));
  L->metafile_index = metafile_index;
  #ifdef LISTS_Z
  memcpy (L->list_id, list_id, list_id_bytes);
  #else
  L->list_id = list_id;
  #endif
  L->o_tree = NILL;
  L->g_tree = NILX;
  for (k = 0; k < 8; k++) {
    L->o_tree_sub[k] = NIL;
    L->g_tree_sub[k] = NILX;
  }

  List[i] = L;
  tot_lists++;

  return L;
}


static inline list_t *get_list_f (list_id_t list_id) {
  return __get_list_f (list_id, 1);
}


static inline list_t *get_list_m (list_id_t list_id) {
  return __get_list_f (list_id, 0);
}

static inline int get_list_metafile (list_id_t list_id) {
  int i = conv_list_id (list_id);
  if (i < 0) {
    return -1;
  }
  list_t *L = List[i];
  if (!L) {
    return find_metafile (list_id);
  }
  return L->metafile_index;
}
/*
static inline struct tree_node *lookup_list_entry (const list_t *L, object_id_t object_id) {
  return L ? (struct tree_node *) tree_lookup (L->tree, object_id) : 0;
}
*/

//just modifies slave trees
static inline void change_entry_flags_common (object_id_t object_id, global_id_t global_id, int oldflags, int flags) {
  int old_cat = oldflags & (SUBCATS - 1);
  int new_cat = flags & (SUBCATS - 1);
  if (new_cat != old_cat) {
    listree_t OT1, OT2;
    listree_xglobal_t GT1, GT2;
    load_o_tree_sub (&OT1, old_cat);  
    load_o_tree_sub (&OT2, new_cat);  
    load_g_tree_sub (&GT1, old_cat);  
    load_g_tree_sub (&GT2, new_cat);  
    listree_delete_small (&OT1, object_id, lrand48 ()/*global_id * Y_MULT*/);
    listree_insert_small (&OT2, object_id, lrand48 ()/*global_id * Y_MULT*/);
    #ifdef LISTS_GT
    listree_delete_global (&GT1, global_id, object_id);
    listree_insert_global (&GT2, global_id, object_id);
    #else
    listree_delete_small (&GT1, global_id, object_id * Y_MULT);
    listree_insert_small (&GT2, global_id, object_id * Y_MULT);
    #endif
  }
}


static inline void change_entry_flags_memory (tree_ext_large_t *T, int new_flags) {
  struct tree_payload *P = PAYLOAD (T);
  change_entry_flags_common (T->x, P->global_id, P->flags, new_flags);
  P->flags = new_flags;
}

int last_global_id_failed;

static int set_list_entry (struct lev_new_entry_ext *E, int ext, value_t value_override) {
  list_t *L;
  int new_cat;
  struct cyclic_buffer_entry *D;
  object_id_t E_object_id = LEV_OBJECT_ID (E);
  if (ignore_mode && ignore_list_check (E->list_id, E_object_id)) {
    vkprintf (1, "ignore set_list_entry: list=" idout " object= " idout "\n", out_list_id (E->list_id), out_object_id (E_object_id));
    return 0;
  }
  value_t E_value = (ext & 2) ? value_override : ((struct lev_new_entry_ext *) LEV_ADJUST_LO(E))->value;

  vkprintf (check_debug_list (E->list_id) ? 0 : 4, "set_list_entry: now=%d, flags=%02x, list=" idout ", object=" idout ", value=" valout ", op=%d\n", now, E->type & 0xff, out_list_id (E->list_id), out_object_id (E_object_id), E_value, (E->type & 0x300) >> 8);
 
  if ((check_ignore_list (E->list_id) || ignored_list2 == -1) && (E->type & 3) == 2) {
    return 0;
  }

  if (conv_list_id (E->list_id) < 0) {
    return -1;
  }
  

  if (metafile_mode) {
    //int metafile_number = find_metafile (E->list_id);
    //L = get_list_m (E->list_id);
    //int metafile_number = get_list_m (E->list_id)->metafile_index;
    int metafile_number = get_list_metafile (E->list_id);
    if (metafile_number >= 0) {
      if (!check_metafile_loaded (metafile_number, -1)) {
        postpone (metafile_number, (struct lev_generic *)E);
        if (E->type & 0x100) {
          static var_ltree_x_t ltree_key;
          combine_ltree_x (E->list_id, E_object_id, &ltree_key);
          if (!ltree_lookup (object_tree, ltree_key)) {
            object_tree = ltree_insert (object_tree, ltree_key, lrand48());
          }
        }
        return 1;
      } else if (metafile_mode & 2) {
        postpone (metafile_number, (struct lev_generic *)E);
      } else {
        if (!(metafile_mode & 1)) {
          mark_list_metafile (E->list_id);
        }
      }
    }
  }
  
  if (E->type & 0x100) {	// if SET or ADD...
    last_global_id++;
    if (!last_global_id) {
      last_global_id_failed = 0;
    }
    L = get_list_f (E->list_id);
    assert (L);
  } else {
    L = get_list_m (E->list_id);
    if (!L) {
      return 0;
    }
  }

  unpack_metafile_pointers (L);

  int p = -1;
  tree_ext_large_t *T = listree_lookup_large (&OTree, E_object_id, &p);
  
  if (T) {
    if (!(E->type & 0x200)) {	// if ADD...
      return 0;
    }
    if (T == (void *) -1) {
      if (!(metafile_mode & 1)) {
        assert (p >= 0 && p < M_tot_entries && object_id_equal (OARR_ENTRY(M_obj_id_list, p), E_object_id));
        value_t old_value = metafile_get_value (p);
        int old_flags = metafile_get_flags (p);
        if (old_value == E_value && old_flags == (E->type & 0xff)) {
          return 1;
        }
        if (!E_value || M_values) {
          // DIRTY HACK
          if (M_values) {
            M_values[p] = E_value;
          }
          if (M_flags) {
            M_flags[p] = E->type & 0xff;
          } else {
            M_flags_small[p] = E->type & 0xff;
          }
          change_entry_flags_common (OARR_ENTRY(M_obj_id_list, p), M_global_id_list[p], old_flags, E->type & 0xff);
          return 1;
        }      
      }
      T = clone_metafile_entry (p);
    }
    change_entry_flags_memory (T, E->type & 0xff);
    PAYLOAD (T)->value = E_value;
    /*if (verbosity > 2 || E->list_id == 704858) {
      fprintf (stderr, "entry " idout "_" idout " already exists at %p, flags=%d, value=" valout ", global_id=%d, date=%d\n", out_list_id (E->list_id), T->x, T, T->flags, T->value, T->global_id, T->date);
    }*/
    return 1;
  }
  if (!(E->type & 0x100)) {	// if REPLACE...
    return 0;
  }

  new_cat = E->type & (SUBCATS - 1);

  D = (struct cyclic_buffer_entry *) &CB[(last_global_id & (CYCLIC_BUFFER_SIZE-1)) * cyclic_buffer_entry_ints];
  D->flags = E->type & 0xff;
  D->value = E_value;
  D->global_id = last_global_id;
  D->date = now_override ? now_override : now;
  D->text = 0;
  if (ext == 1) {
    memcpy (D->extra, ((struct lev_new_entry_ext *) LEV_ADJUST_LO(E))->extra, 16);
  } else {
    memset (D->extra, 0, 16);
  }
#ifdef LISTS_Z
  memcpy (D->list_id, E->list_id, object_list_bytes);
#else
  D->list_id = E->list_id;
  D->object_id = E_object_id;
#endif

  T = listree_insert_large (&OTree, E_object_id);

  struct tree_payload *P = PAYLOAD (T);

  P->global_id = D->global_id;
  P->value = E_value;
  P->date = D->date;
  P->flags = D->flags;
  P->text = (char *) empty_string;

  listree_t OTreeSub;
  listree_xglobal_t GTree, GTreeSub;

  load_o_tree_sub (&OTreeSub, new_cat);  
  load_g_tree (&GTree);
  load_g_tree_sub (&GTreeSub, new_cat);  

  listree_insert_small (&OTreeSub, E_object_id, lrand48 ()/*D->global_id * Y_MULT*/);
  #ifdef LISTS_GT
  listree_insert_global (&GTree, D->global_id, E_object_id);
  listree_insert_global (&GTreeSub, D->global_id, E_object_id);
  #else
  listree_insert_small (&GTree, D->global_id, E_object_id * Y_MULT);
  listree_insert_small (&GTreeSub, D->global_id, E_object_id * Y_MULT);
  #endif

  static var_ltree_x_t ltree_key;
  combine_ltree_x (E->list_id, E_object_id, &ltree_key);
  if (NODE_TYPE(T) == TF_PLUS) {
//    assert (revlist_lookup (ltree_key) < 0);
//    assert (!ltree_lookup (object_tree, ltree_key));
    if (!ltree_lookup (object_tree, ltree_key)) {
      object_tree = ltree_insert (object_tree, ltree_key, lrand48());
    }
  }
  tot_list_entries++;

  return 1;
}

static int set_entry_text (list_id_t list_id, object_id_t object_id, const char *text, int len, struct lev_generic *E) {
  vkprintf (check_debug_list(list_id) ? 0 : 3, "set_entry_text: now=%d, list=" idout ", object=" idout ", text len=%d\n", now, out_list_id (list_id), out_object_id (object_id), len);
  if (conv_list_id (list_id) < 0 || (unsigned) len >= 256) {
    return -1;
  }
  if (ignore_mode && ignore_list_check (list_id, object_id)) {
    vkprintf (1, "ignore set_list_entry: list=" idout " object= " idout "\n", out_list_id (list_id), out_object_id (object_id));
    return 0;
  }
  
  if (metafile_mode) {
    //int metafile_number = find_metafile (list_id);
    //L = get_list_m (list_id);
    //int metafile_number = L->metafile_number;
    //int metafile_number = get_list_m (list_id)->metafile_number;
    int metafile_number = get_list_metafile (list_id);
    if (metafile_number >= 0) {
      if (!check_metafile_loaded (metafile_number, -1)) {
        postpone (metafile_number, (struct lev_generic *)E);
        return 1;
      } else if (metafile_mode & 2) {
        postpone (metafile_number, (struct lev_generic *)E);
      } else {
        if (!(metafile_mode & 1)) {
          mark_list_metafile (list_id);
        }
      }
    }
  }

  list_t *L = get_list_m (list_id);
  if (!L) {
    return -1;
  }

  unpack_metafile_pointers (L);

  int temp_id;
  struct tree_ext_large *T = listree_lookup_large (&OTree, object_id, &temp_id);
  if (!T) {
    return -1;
  }

  if (!len) {
    int old_len;
    char *old_text = (T == (void *)-1) ? metafile_get_text (temp_id, &old_len) : PAYLOAD (T)->text;
    if (!old_text || old_text == empty_string) {
      return 1;
    }
  }

  if (T == (void *) -1) {
    T = clone_metafile_entry (temp_id);
  }

  struct tree_payload *P = PAYLOAD (T);

  if (P->text && P->text != empty_string) {
    zfree (P->text, get_text_data_len (P->text));
  }
  if (!len) {
    P->text = (char *) empty_string;
    return 0;
  }
  if (len <= 253) {
    P->text = zmalloc (len + 1);
    P->text[0] = len;
    memcpy (P->text + 1, text, len);
  } else {
    assert (len < (1 << 24));
    P->text = zmalloc (len + 4);
    *(int *)P->text = len;
    P->text[0] = 0xfe;
    memcpy (P->text + 4, text, len);
  }
  return 1;
}


static int set_entry_flags (struct lev_set_flags *E) {
  tree_ext_large_t *T;
  list_t *L;
  int new_flags;
  object_id_t E_object_id = LEV_OBJECT_ID (E);
  
  if (ignore_mode && ignore_list_check (E->list_id, E_object_id)) {
    vkprintf (1, "ignore set_list_entry: list=" idout " object= " idout "\n", out_list_id (E->list_id), out_object_id (E_object_id));
    return 0;
  }

  vkprintf (check_debug_list(E->list_id) ? 0 : 3, "set_entry_flags: type=%08x, list=" idout ", object=" idout "\n", E->type, out_list_id (E->list_id), out_object_id (E_object_id));

  if (conv_list_id (E->list_id) < 0) {
    return -1;
  }
  
  if (metafile_mode) {
    //int metafile_number = find_metafile (E->list_id);
    //L = get_list_m (E->list_id);
    //int metafile_number = L->metafile_number;
    //int metafile_number = get_list_m (E->list_id)->metafile_number;
    int metafile_number = get_list_metafile (E->list_id);
    if (metafile_number >= 0) {
      if (!check_metafile_loaded (metafile_number, -1)) {
        postpone (metafile_number, (struct lev_generic *)E);
        return 1;
      } else if (metafile_mode & 2) {
        postpone (metafile_number, (struct lev_generic *)E);
      } else {
        if (!(metafile_mode & 1)) {
          mark_list_metafile (E->list_id);
        }
      }
    }
  }
  L = get_list_m (E->list_id);
  if (!L) {
    return -1;
  }

  unpack_metafile_pointers (L);

  int temp_id;
  T = listree_lookup_large (&OTree, E_object_id, &temp_id);

  if (!T) {
    return -1;
  }

  int old_flags;
  if (T == (void *)-1) {
    old_flags = metafile_get_flags (temp_id);
  } else {
    old_flags = PAYLOAD (T)->flags;
  }

  switch (E->type & -0x100) {
  case LEV_LI_SET_FLAGS:
    new_flags = E->type & 0xff;
    break;
  case LEV_LI_INCR_FLAGS:
    new_flags = old_flags | (E->type & 0xff);
    break;
  case LEV_LI_DECR_FLAGS:
    new_flags = old_flags & ~(E->type & 0xff);
    break;
  case LEV_LI_SET_FLAGS_LONG & -0x100:
    assert (E->type == LEV_LI_SET_FLAGS_LONG);
    new_flags = ((struct lev_set_flags_long *) LEV_ADJUST_LO(E))->flags;
    break;
  case LEV_LI_CHANGE_FLAGS_LONG & -0x100:
    assert (E->type == LEV_LI_CHANGE_FLAGS_LONG);
    new_flags = (old_flags & ((struct lev_change_flags_long *) LEV_ADJUST_LO(E))->and_mask) ^ ((struct lev_change_flags_long *) LEV_ADJUST_LO(E))->xor_mask;
    break;
  default:
    assert (0);
  }

  vkprintf (check_debug_list (E->list_id) ? 0 : 3, "set_entry_flags: new flags=%d\n", new_flags);

  if (old_flags == new_flags) {
    //return new_flags;
    return 1;
  }

  if (T == (void *) -1) {
    assert (temp_id >= 0 && temp_id < M_tot_entries && object_id_equal (OARR_ENTRY (M_obj_id_list, temp_id), E_object_id));
    if (!(metafile_mode & 1) && (!(new_flags & -0x100) || M_flags)) {
      // DIRTY HACK
      if (M_flags) {
        M_flags[temp_id] = new_flags;
      } else {
        M_flags_small[temp_id] = new_flags;
      }
      change_entry_flags_common (OARR_ENTRY (M_obj_id_list, temp_id), M_global_id_list[temp_id], old_flags, new_flags);
      return 1;
    }
    T = clone_metafile_entry (temp_id);
  }

  change_entry_flags_memory (T, new_flags);
//  return new_flags;
  return 1;
}

static long long set_incr_entry_value (list_id_t list_id, object_id_t object_id, value_t value, int incr, struct lev_generic *E) {
  struct tree_ext_large *T;
  list_t *L;
  
  if (ignore_mode && ignore_list_check (list_id, object_id)) {
    vkprintf (1, "ignore set_list_entry: list=" idout " object= " idout "\n", out_list_id (list_id), out_object_id (object_id));
    return 0;
  }

  vkprintf (check_debug_list (list_id) ? 0 : 3, "set_incr_list_value: now=%d, list=" idout ", object=" idout ", value=" valout ", incr=%d\n", now, out_list_id (list_id), out_object_id (object_id), value, incr);
  if (metafile_mode) {
    //int metafile_number = find_metafile (list_id);
    //L = get_list_m (list_id);
    //int metafile_number = L->metafile_number;
    //int metafile_number = get_list_m (list_id)->metafile_number;
    int metafile_number = get_list_metafile (list_id);
    if (metafile_number >= 0) {
      if (!check_metafile_loaded (metafile_number, -1)) {
        postpone (metafile_number, (struct lev_generic *)E);
        //static var_ltree_x_t ltree_key;
        //combine_ltree_x (list_id, object_id, &ltree_key);
        //if (!ltree_lookup (object_tree, ltree_key)) {
        //object_tree = ltree_insert (object_tree, ltree_key, lrand48());
        //}
        return 1;
      } else if (metafile_mode & 2) {
        postpone (metafile_number, (struct lev_generic *)E);
      } else {
        if (!(metafile_mode & 1)) {
          mark_list_metafile (list_id);
        }
      }
    }
  }

  L = get_list_m (list_id);
  if (!L) {
    return -1LL << 63;
  }

  unpack_metafile_pointers (L);

  int temp_id;

  T = listree_lookup_large (&OTree, object_id, &temp_id);
  if (!T) {
    return -1LL << 63;
  }

  value_t old_value = (T == (void *) -1) ? metafile_get_value (temp_id) : PAYLOAD (T)->value;
  if (incr) {
    value += old_value;
  }
  if (value == old_value) {
    return value;
  }

  if (T == (void *) -1) {
    // DIRTY HACK
    if (!(metafile_mode & 1)) {
      if (M_values) {
        M_values[temp_id] = value;
        return value;
      } else if (!value) {
        return value;
      }
    }
    T = clone_metafile_entry (temp_id);
  }

  PAYLOAD (T)->value = value;

  vkprintf (check_debug_list (list_id) ? 0 : 3, "set_incr_list_value: resulting value=" valout "\n", value);

  return value;
}

static int f_xor_c, f_and_c, f_and_s, f_xor_s, f_cnt;


object_id_t *temp_object_list;
int temp_object_list_size;
#define temp_object_buff_size 1000000
object_id_t temp_object_buff[temp_object_buff_size];

static int array_change_flags (listree_t *LT, int temp_id) {
  int old_flags;
// DIRTY HACK: CHANGE LOADED METAFILE
  assert ((unsigned) temp_id < (unsigned) M_tot_entries);
  if (!(metafile_mode & 1)) {
    if (M_flags) {
      old_flags = M_flags[temp_id];
      if (!((old_flags ^ f_xor_c) & f_and_c)) {
        M_flags[temp_id] = (old_flags & f_and_s) ^ f_xor_s;
        change_entry_flags_common (OARR_ENTRY (M_obj_id_list, temp_id), M_global_id_list[temp_id], old_flags, M_flags[temp_id]);
        f_cnt++;
      }
    } else {
      assert (M_flags_small);
      old_flags = M_flags_small[temp_id];
      if (!((old_flags ^ f_xor_c) & f_and_c)) {
        M_flags_small[temp_id] = (old_flags & f_and_s) ^ f_xor_s;
        change_entry_flags_common (OARR_ENTRY (M_obj_id_list, temp_id), M_global_id_list[temp_id], old_flags, M_flags_small[temp_id]);
        f_cnt++;
      }
    }
  } else {
    if (M_flags) {
      old_flags = M_flags[temp_id];
      if (!((old_flags ^ f_xor_c) & f_and_c)) {
        temp_object_list[temp_object_list_size ++] = OARR_ENTRY (M_obj_id_list, temp_id);
        f_cnt++;
      }
    } else {
      assert (M_flags_small);
      old_flags = M_flags_small[temp_id];
      if (!((old_flags ^ f_xor_c) & f_and_c)) {
        temp_object_list[temp_object_list_size ++] = OARR_ENTRY (M_obj_id_list, temp_id);
        f_cnt++;
      }
    }
  }
  return 0;
}

static int tree_change_flags (tree_ext_small_t *TS) {
  tree_ext_large_t *T = LARGE_NODE(TS);
  if (!((PAYLOAD (T)->flags ^ f_xor_c) & f_and_c)) {
    change_entry_flags_memory (T, (PAYLOAD (T)->flags & f_and_s) ^ f_xor_s);
    f_cnt++;
  }
  return 0;
}

int create_change_entry_flags (list_id_t list_id, object_id_t object_id, int and_flags, int xor_flags) {
  static char buf[1024];
  struct lev_change_flags_long *E = (void *)buf; //alloc_log_event (LEV_LI_CHANGE_FLAGS_LONG, sizeof (struct lev_change_flags_long) + lev_list_object_bytes, FIRST_INT(list_id));
  E->type = LEV_LI_CHANGE_FLAGS_LONG;
  ((struct lev_generic *)E)->a = FIRST_INT(list_id);
  upcopy_list_object_id (E, list_id, object_id);
  struct lev_change_flags_long *EE = LEV_ADJUST_LO (E);
  EE->and_mask = and_flags;
  EE->xor_mask = xor_flags;
  return set_entry_flags ((struct lev_set_flags *) E);
}

int postponed_replay;
static int change_sublist_flags (list_id_t list_id, int op, struct lev_generic *E) {
  if (metafile_mode) {
    //int metafile_number = find_metafile (list_id);
    //L = get_list_m (list_id);
    //int metafile_number = L->metafile_number;
    //int metafile_number = get_list_m (list_id)->metafile_number;
    int metafile_number = get_list_metafile (list_id);
    if (metafile_number >= 0) {
      if (!check_metafile_loaded (metafile_number, -1)) {
        postpone (metafile_number, (struct lev_generic *)E);
        return 1;
      } else if (metafile_mode & 2) {
        postpone (metafile_number, (struct lev_generic *)E);
      } else {
        if (!(metafile_mode & 1)) {
          mark_list_metafile (list_id);
        }
      }
    }
  }

  list_t *L = get_list_m (list_id);
  int i, s = 0, xor_c = op & 0xff, and_c = (op >> 8) & 0xff;

  if (!L) {
    return conv_list_id (list_id) < 0 ? -1 : 0;
  }

  unpack_metafile_pointers (L);

  for (i = 0; i < 8; i++) {
    if (!((i ^ xor_c) & 7 & and_c)) {
      s += metafile_get_sublist_size (i);
    }
  }

  if (!s) {
    return 0;
  }

  f_xor_c = xor_c;
  f_and_c = and_c;
  f_and_s = ((op >> 16) & 0xff) | -0x100;
  f_xor_s = (op >> 24) & 0xff;
  f_cnt = 0;

  in_array = array_change_flags;
  in_tree = tree_change_flags;

  vkprintf (2, "change_sublist_flags (" idout ", %02x, %02x, %02x, %02x)\n", out_list_id (list_id), f_xor_c, f_and_c, f_and_s, f_xor_s);
  
  long long msize = 0;
  if (metafile_mode & 1) {
    temp_object_list_size = 0;
    if (M_tot_entries + L->o_tree->delta <= temp_object_buff_size) {
      temp_object_list = temp_object_buff;
    } else {
      msize = sizeof (object_id_t) * (M_tot_entries + L->o_tree->delta);
      temp_object_list = zzmalloc (msize);
    }
  }
  listree_get_range ((listree_t *) &OTree, 0, MAXINT);
  if (metafile_mode & 1) {
    //struct lev_set_flags *E = zmalloc (sizeof (struct lev_set_flags) + lev_list_object_bytes);
    //struct lev_set_flags *E = alloc_log_event (LEV_LI_SET_FLAGS + set_flags, sizeof (struct lev_set_flags) + lev_list_object_bytes, FIRST_INT(list_id));
    //E->type = LEV_LI_SET_FLAGS + set_flags;
    int t = postponed_replay;
    postponed_replay = 1;
    for (i = 0; i < temp_object_list_size; i++) {
      create_change_entry_flags (list_id, temp_object_list[i], f_and_s, f_xor_s);
      //upcopy_list_object_id (E, list_id, temp_object_list[i]);
      //set_entry_flags (E); 
    }
    postponed_replay = t;
    //zfree (E, sizeof (struct lev_set_flags) + lev_list_object_bytes);
    if (temp_object_list != temp_object_buff) {
      zzfree (temp_object_list, msize);
    }
  }


  return f_cnt;
}

static inline void delete_list_entry_aux (object_id_t object_id, global_id_t global_id, int flags) {
  int cat = flags & (SUBCATS - 1);
  listree_t OTreeSub;
  listree_xglobal_t GTree, GTreeSub;
  load_o_tree_sub (&OTreeSub, cat);
  load_g_tree (&GTree);
  load_g_tree_sub (&GTreeSub, cat);
  listree_delete_small (&OTreeSub, object_id, lrand48 ()/*global_id * Y_MULT*/);
  #ifdef LISTS_GT
  listree_delete_global (&GTree, global_id, object_id);
  listree_delete_global (&GTreeSub, global_id, object_id);
  #else
  listree_delete_small (&GTree, global_id, object_id * Y_MULT);
  listree_delete_small (&GTreeSub, global_id, object_id * Y_MULT);
  #endif
}

static int delete_list (list_id_t list_id, struct lev_generic *E);

static int delete_entry (list_id_t list_id, object_id_t object_id, int delete_from_ltree, struct lev_generic *E) {
  vkprintf (check_debug_list (list_id) ? 0 : 4, "delete_entry list_id=" idout ", object_id=" idout "\n", out_list_id(list_id), out_object_id(object_id));
  if (ignore_mode && ignore_list_check (list_id, object_id)) {
    vkprintf (1, "ignore set_list_entry: list=" idout " object= " idout "\n", out_list_id (list_id), out_object_id (object_id));
    return 0;
  }
  int temp_id = -1, flags;
  global_id_t global_id;
  if (metafile_mode) {
    //int metafile_number = find_metafile (list_id);
    //L = get_list_m (list_id);
    //int metafile_number = L->metafile_number;
    //int metafile_number = get_list_m (list_id)->metafile_number;
    int metafile_number = get_list_metafile (list_id);
    if (metafile_number >= 0) {
      if (!check_metafile_loaded (metafile_number, -1)) {
        postpone (metafile_number, (struct lev_generic *)E);
        return 1;
      } else if (metafile_mode & 2) {
        assert (E);
        postpone (metafile_number, (struct lev_generic *)E);
      } else {
        if (!(metafile_mode & 1)) {
          mark_list_metafile (list_id);
        }
      }
    }
  }

  list_t *L = get_list_m (list_id);

  if (!L) {
    return 0;
  }
  unpack_metafile_pointers (L);

  tree_ext_large_t *T = listree_lookup_large (&OTree, object_id, &temp_id);
  if (!T) {
    return 0;
  }
  if (T == (void *) -1) {
    assert (temp_id >= 0 && temp_id < M_tot_entries && object_id_equal (OARR_ENTRY (M_obj_id_list, temp_id), object_id));
    global_id = M_global_id_list[temp_id];
    flags = metafile_get_flags (temp_id);
    T = clone_metafile_entry (temp_id);
  } else {
    struct tree_payload *P = PAYLOAD (T);
    global_id = P->global_id;
    flags = P->flags;
    if (delete_from_ltree && NODE_TYPE(T) == TF_PLUS) {
      static var_ltree_x_t ltree_key;
      combine_ltree_x (list_id, object_id, &ltree_key);
      object_tree = ltree_delete (object_tree, ltree_key);
    }
  }

  delete_list_entry_aux (object_id, global_id, flags);
  listree_delete_large (&OTree, object_id);

  tot_list_entries--;

  if (M_tot_entries + L->o_tree->delta == 0 && !(metafile_mode & 2) && !postponed_replay) {
    assert (delete_list (list_id, 0)); //free unused structures
  }

  return 1;
}

static list_id_t current_list_id;

static void delete_list_objects (tree_ext_large_t *T, int delete_from_object_tree) {
  static var_ltree_x_t ltx;
  if (SMALL_NODE(T) != NIL) {
    switch (NODE_TYPE(T)) {
    case TF_PLUS:
      combine_ltree_x (current_list_id, T->x, &ltx);
      if (delete_from_object_tree) {
        object_tree = ltree_delete (object_tree, ltx);
      }
      tot_list_entries--;
      break;
    case TF_MINUS:
      tot_list_entries++;
    }
    delete_list_objects (T->left, delete_from_object_tree);
    delete_list_objects (T->right, delete_from_object_tree);
    free_tree_ext_large_node (T);
  }
}


void clear_postponed (int x);
static int delete_list (list_id_t list_id, struct lev_generic *E) {
  int i, x = conv_list_id (list_id);
  if (metafile_mode) {
    //int metafile_number = find_metafile (list_id);
    //int metafile_number = get_list_m (list_id)->metafile_number;
    int metafile_number = get_list_metafile (list_id);
    if (metafile_number >= 0) {
      clear_postponed (metafile_number);
    }
    /*if (metafile_number >= 0) {
      if (!check_metafile_loaded (metafile_number, -1)) {
        postpone (metafile_number, (struct lev_generic *)E);
        return 1;
      } else if (metafile_mode & 2) {
        assert (E);
        postpone (metafile_number, (struct lev_generic *)E);
      } else {
        if (!(metafile_mode & 1)) {
          mark_list_metafile (list_id);
        }
      }
    }*/
  }

  list_t *L = get_list_m (list_id);
  
  vkprintf (check_debug_list (list_id) ? 0 : 4, "delete_list: now=%d, list=" idout "\n", now, out_list_id (list_id));

  if (!L) {
    return x < 0 ? -1 : 0;
  }

  current_list_id = list_id;
  delete_list_objects (L->o_tree, 1);
  L->o_tree = (tree_ext_large_t *)NIL;
  #ifdef LISTS_GT
  free_tree_ext_global (L->g_tree);
  #else
  free_tree_ext_small (L->g_tree);
  #endif
  L->g_tree = NILX;

  for (i = 0; i < SUBCATS; i++) {
    free_tree_ext_small (L->o_tree_sub[i]);
    L->o_tree_sub[i] = NIL;
    #ifdef LISTS_GT
    free_tree_ext_global (L->g_tree_sub[i]);
    #else
    free_tree_ext_small (L->g_tree_sub[i]);
    #endif
    L->g_tree_sub[i] = NILX;
  }

  if (L->metafile_index >= 0) {
    // DIRTY HACK
    if (!(metafile_mode)) {
      metafile_t *M = get_metafile_ptr (L->metafile_index);
      tot_list_entries -= M->tot_entries;
      memset (M->sublist_size_cum, 0, (SUBCATS + 1) * sizeof (int));
      M->flags = 0;
      FLI_ENTRY_ADJUSTED(L->metafile_index)->flags = 0;
    }  
  }

#if 0
  // since List[] is a hash table now, we cannot free allocated list structures
  List[x] = 0;
  free_list_struct (L);
  tot_lists--;
#else
  L->metafile_index = -1;
#endif
  M_metafile = (void *)-1;

  return 1;
}

static ltree_t *delete_object_refs (ltree_t *T, object_id_t object_id) {
  if (!T) {
    return 0;
  }
  object_id_t z = ltree_x_object_id (T->x);
  int p = object_id_compare (z, object_id);
  if (p < 0) {
    T->right = delete_object_refs (T->right, object_id);
    return T;
  } else if (p > 0) {
    T->left = delete_object_refs (T->left, object_id);
    return T;
  } else {
    vkprintf (2, "delete_list_entry(" idout "," idout ")\n", out_list_id (ltree_x_list_id (T->x)), out_object_id (z));
//  assert (z == object_id);
    struct lev_del_entry *E = 0;
    if (metafile_mode) {
      E = zmalloc (sizeof (struct lev_del_entry) + lev_list_object_bytes);
      E->type = LEV_LI_DEL_ENTRY;
      ((struct lev_generic *)E)->a = FIRST_INT (ltree_x_list_id (T->x));
      upcopy_list_object_id (E, ltree_x_list_id (T->x), z);
    }
    assert (delete_entry (ltree_x_list_id (T->x), z, 0, (struct lev_generic *)E) >= 0); // would like to assert > 0, but cannot because of DIRTY HACK in delete_list()
    if (E) {
      zfree (E, sizeof (struct lev_del_entry) + lev_list_object_bytes);
    }
    ltree_t *N = ltree_merge (delete_object_refs (T->left, object_id), delete_object_refs (T->right, object_id));
    free_ltree_node (T);
    return N;
  }
}
    
static int delete_object (object_id_t object_id) {
  assert (!disable_revlist);
  vkprintf (2, "delete_object (" idout ")\n", out_object_id (object_id));

  //long l = revlist_lookup_left (object_id);
  struct lev_del_entry *E = 0;
  if (metafile_mode) {
    E = zmalloc (sizeof (struct lev_del_entry) + lev_list_object_bytes);
    E->type = LEV_LI_DEL_ENTRY;
  }
  if (revlist_metafile_mode) {
    long rm = choose_revlist_metafile (object_id);
    vkprintf (4, "store rm=%ld/%d list_id=" idout ", object_id=" idout "\n", rm, tot_revlist_metafiles, out_list_id(REVLIST_LIST_ID(revlist_metafiles_bounds,rm)), out_object_id(REVLIST_OBJECT_ID(revlist_metafiles_bounds,rm)));
    if (verbosity >= 6) {
      int i;
      for (i = 0; i < tot_revlist_metafiles; i++) {
        fprintf (stderr, "store rm=%ld/%d list_id=" idout ", object_id=" idout "\n", rm, tot_revlist_metafiles, out_list_id(REVLIST_LIST_ID(revlist_metafiles_bounds,i)), out_object_id(REVLIST_OBJECT_ID(revlist_metafiles_bounds,i)));
      }
    }
    init_revlist_iterator (rm, 0);
    advance_revlist_iterator_id (object_id);
  } else {
    init_revlist_iterator (-1, 0);
    advance_revlist_iterator_id (object_id);
  }
  while (!revlist_iterator.eof && object_id_equal (CUR_REVLIST_OBJECT_ID, object_id)) {
    if (metafile_mode) {
      ((struct lev_generic *)E)->a = FIRST_INT (CUR_REVLIST_LIST_ID);
      upcopy_list_object_id (E, CUR_REVLIST_LIST_ID, object_id);
    }
    assert (delete_entry (CUR_REVLIST_LIST_ID, object_id, 0, (struct lev_generic *)E) >= 0);
    advance_revlist_iterator ();
  }
  if (E) {
    zfree (E, sizeof (struct lev_del_entry) + lev_list_object_bytes);
  }
  object_tree = delete_object_refs (object_tree, object_id);
  return 1;
}

static int array_delete_sublist (listree_t *LT, int temp_id) {
  if (!((metafile_get_flags (temp_id) ^ f_xor_c) & f_and_c)) {
    if (!(metafile_mode & 1)) {
      delete_list_entry_aux (OARR_ENTRY (M_obj_id_list, temp_id), M_global_id_list[temp_id], metafile_get_flags (temp_id));
      tot_list_entries--;
      f_cnt++;
      return 0; // delete this
    } else {
      temp_object_list[temp_object_list_size ++] = OARR_ENTRY (M_obj_id_list, temp_id);
      f_cnt++;
      return 1; // will be deleted, but later
    }
  } else {
    return 1; // do not touch
  }
}

static int tree_delete_sublist (tree_ext_small_t *T) {
  struct tree_payload *P = LPAYLOAD (T);
  if (!((P->flags ^ f_xor_c) & f_and_c)) {
    delete_list_entry_aux (T->x, P->global_id, P->flags);
    static var_ltree_x_t ltree_key;
    combine_ltree_x (M_List->list_id, T->x, &ltree_key);
    object_tree = ltree_delete (object_tree, ltree_key);
    tot_list_entries--;
    f_cnt++;
    return 0; // delete this
  } else {
    return 1; // do not touch
  }
}

int postponed_replay;
static int delete_sublist (list_id_t list_id, int op, struct lev_generic *E) {
  vkprintf (4, "delete_sublist\n");
  if (metafile_mode) {
    //int metafile_number = find_metafile (list_id);
    //int metafile_number = get_list_m (list_id)->metafile_number;
    int metafile_number = get_list_metafile (list_id);
    if (metafile_number >= 0) {
      if (!check_metafile_loaded (metafile_number, -1)) {
        postpone (metafile_number, (struct lev_generic *)E);
        return 1;
      } else if (metafile_mode & 2) {
        postpone (metafile_number, (struct lev_generic *)E);
      } else {
        if (!(metafile_mode & 1)) {
          mark_list_metafile (list_id);
        }
      }
    }
  }

  list_t *L = get_list_m (list_id);
  int i, s = 0, xor_c = op & 0xff, and_c = (op >> 8) & 0xff;

  if (!L) {
    return conv_list_id (list_id) < 0 ? -1 : 0;
  }

  unpack_metafile_pointers (L);

  for (i = 0; i < 8; i++) {
    if (!((i ^ xor_c) & 7 & and_c)) {
      s += metafile_get_sublist_size (i);
    }
  }

  if (!s) {
    return 0;
  }

  f_xor_c = xor_c;
  f_and_c = and_c;
  f_cnt = 0;

  in_array = array_delete_sublist;
  in_tree = tree_delete_sublist;

  long long msize = 0;
  if (metafile_mode & 1) {
    temp_object_list_size = 0;
    if (M_tot_entries + L->o_tree->delta <= temp_object_buff_size) {
      temp_object_list = temp_object_buff;
    } else {
      msize = sizeof (object_id_t) * (M_tot_entries + L->o_tree->delta);
      temp_object_list = zzmalloc (msize);
    }
  }
  assert (!listree_delete_some_range_rec_large (OTree.root, &OTree, 0, OTree.N - 1, MAXINT));
  if (metafile_mode & 1) {
    struct lev_del_entry *E = zmalloc (sizeof (struct lev_del_entry) + lev_list_object_bytes);
    E->type = LEV_LI_DEL_ENTRY;
    int t = postponed_replay;
    postponed_replay = 1;
    for (i = 0; i < temp_object_list_size; i++) {
      ((struct lev_generic *)E)->a = FIRST_INT(list_id);
      upcopy_list_object_id (E, list_id, temp_object_list[i]);
      assert (delete_entry (list_id, temp_object_list[i], 0, (struct lev_generic *)E) >= 0); 
    }
    postponed_replay = t;
    zfree (E, sizeof (struct lev_del_entry) + lev_list_object_bytes);
    if (temp_object_list != temp_object_buff) {
      zzfree (temp_object_list, msize);
    }
  }

  if (M_tot_entries + L->o_tree->delta == 0 && !(metafile_mode & 2)) {
    assert (delete_list (list_id, 0)); //free unused structures
  }

  return f_cnt;
}


/* --------- replay log ------- */

int lists_replay_logevent (struct lev_generic *EE, int size) {
  struct lev_new_entry_ext *E = (struct lev_new_entry_ext *)EE;
  int s;

  if (!postponed_replay && index_mode && dyn_free_bytes() < idx_min_free_heap && E->type != LEV_ROTATE_FROM) {
    if (verbosity > 0) {
      fprintf (stderr, "only %ld heap bytes remaining, writing intermediate index file\n", dyn_free_bytes());
    }
    s = write_index (0);
    exit (s ? s : 13);
  }

  switch (E->type) {
  case LEV_START:
    if (size < 24 || EE->b < 0 || EE->b > 4096) { return -2; }
    s = 24 + ((EE->b + 3) & -4);
    if (size < s) { return -2; }
    return lists_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (EE, size);
  case LEV_LI_SET_ENTRY ... LEV_LI_SET_ENTRY+0xff:
  case LEV_LI_ADD_ENTRY ... LEV_LI_ADD_ENTRY+0xff:
  case LEV_LI_REPLACE_ENTRY ... LEV_LI_REPLACE_ENTRY+0xff:
    if (size < sizeof (struct lev_new_entry) + lev_list_object_bytes) { return -2; }
    set_list_entry ((struct lev_new_entry_ext *) E, 0, 0);
    return sizeof (struct lev_new_entry) + lev_list_object_bytes;
  case LEV_LI_SET_ENTRY_LONG ... LEV_LI_SET_ENTRY_LONG+0xff:
  case LEV_LI_ADD_ENTRY_LONG ... LEV_LI_ADD_ENTRY_LONG+0xff:
  case LEV_LI_REPLACE_ENTRY_LONG ... LEV_LI_REPLACE_ENTRY_LONG+0xff:
    if (size < sizeof (struct lev_new_entry_long) + lev_list_object_bytes) { return -2; }
    set_list_entry ((struct lev_new_entry_ext *) E, 2, ((struct lev_new_entry_long *) E)->value);
    return sizeof (struct lev_new_entry_long) + lev_list_object_bytes;
  case LEV_LI_SET_ENTRY_EXT ... LEV_LI_SET_ENTRY_EXT+0xff:
  case LEV_LI_ADD_ENTRY_EXT ... LEV_LI_ADD_ENTRY_EXT+0xff:
    if (size < sizeof (struct lev_new_entry_ext) + lev_list_object_bytes) { return -2; }
    set_list_entry ((struct lev_new_entry_ext *) E, 1, 0);
    return sizeof (struct lev_new_entry_ext) + lev_list_object_bytes;
  case LEV_LI_SET_ENTRY_TEXT ... LEV_LI_SET_ENTRY_TEXT+0xff:
    s = E->type & 0xff;
    if (size < sizeof (struct lev_set_entry_text) + lev_list_object_bytes + s) { return -2; }
    set_entry_text (E->list_id, LEV_OBJECT_ID(E), ((struct lev_set_entry_text *) LEV_ADJUST_LO(E))->text, s, (struct lev_generic *)E);
    return sizeof (struct lev_set_entry_text) + lev_list_object_bytes + s;
  case LEV_LI_SET_ENTRY_TEXT_LONG:
    if (size < sizeof (struct lev_set_entry_text_long) + lev_list_object_bytes) { return -2; }
    s = ((struct lev_set_entry_text_long *) LEV_ADJUST_LO(E))->len;
    if (size < sizeof (struct lev_set_entry_text_long) + lev_list_object_bytes + s) { return -2; }
    set_entry_text (E->list_id, LEV_OBJECT_ID(E), ((struct lev_set_entry_text_long *) LEV_ADJUST_LO(E))->text, s, (struct lev_generic *)E);
    return sizeof (struct lev_set_entry_text) + lev_list_object_bytes + s;
  case LEV_LI_SET_FLAGS ... LEV_LI_SET_FLAGS+0xff:
    if (size < sizeof (struct lev_set_flags) + lev_list_object_bytes) { return -2; }
    set_entry_flags ((struct lev_set_flags *) E);
    return sizeof (struct lev_set_flags) + lev_list_object_bytes;
  case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
    if (size < sizeof (struct lev_set_flags) + lev_list_object_bytes) { return -2; }
    set_entry_flags ((struct lev_set_flags *) E);
    return sizeof (struct lev_set_flags) + lev_list_object_bytes;
  case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
    if (size < sizeof (struct lev_set_flags) + lev_list_object_bytes) { return -2; }
    set_entry_flags ((struct lev_set_flags *) E);
    return sizeof (struct lev_set_flags) + lev_list_object_bytes;
  case LEV_LI_SET_FLAGS_LONG:
    if (size < sizeof (struct lev_set_flags_long) + lev_list_object_bytes) { return -2; }
    set_entry_flags ((struct lev_set_flags *) E);
    return sizeof (struct lev_set_flags_long) + lev_list_object_bytes;
  case LEV_LI_CHANGE_FLAGS_LONG:
    if (size < sizeof (struct lev_change_flags_long) + lev_list_object_bytes) { return -2; }
    set_entry_flags ((struct lev_set_flags *) E);
    return sizeof (struct lev_change_flags_long) + lev_list_object_bytes;
  case LEV_LI_SET_VALUE:
    if (size < sizeof (struct lev_set_value) + lev_list_object_bytes) { return -2; }
    set_incr_entry_value (E->list_id, LEV_OBJECT_ID(E), ((struct lev_set_value *) LEV_ADJUST_LO(E))->value, 0, (struct lev_generic *)E);
    return sizeof (struct lev_set_value) + lev_list_object_bytes;
  case LEV_LI_SET_VALUE_LONG:
    if (size < sizeof (struct lev_set_value_long) + lev_list_object_bytes) { return -2; }
    set_incr_entry_value (E->list_id, LEV_OBJECT_ID(E), ((struct lev_set_value_long *) LEV_ADJUST_LO(E))->value, 0, (struct lev_generic *)E);
    return sizeof (struct lev_set_value_long) + lev_list_object_bytes;
  case LEV_LI_INCR_VALUE:
    if (size < sizeof (struct lev_set_value) + lev_list_object_bytes) { return -2; }
    set_incr_entry_value (E->list_id, LEV_OBJECT_ID(E), ((struct lev_set_value *) LEV_ADJUST_LO(E))->value, 1, (struct lev_generic *)E);
    return sizeof (struct lev_set_value) + lev_list_object_bytes;
  case LEV_LI_INCR_VALUE_TINY ... LEV_LI_INCR_VALUE_TINY + 0xff:
    if (size < sizeof (struct lev_del_entry) + lev_list_object_bytes) { return -2; }
    set_incr_entry_value (E->list_id, LEV_OBJECT_ID(E), (signed char) E->type, 1, (struct lev_generic *)E);
    return sizeof (struct lev_del_entry) + lev_list_object_bytes;
  case LEV_LI_INCR_VALUE_LONG:
    if (size < sizeof (struct lev_set_value_long) + lev_list_object_bytes) { return -2; }
    set_incr_entry_value (E->list_id, LEV_OBJECT_ID(E), ((struct lev_set_value_long *) LEV_ADJUST_LO(E))->value, 1, (struct lev_generic *)E);
    return sizeof (struct lev_set_value_long) + lev_list_object_bytes;
  case LEV_LI_DEL_LIST:
    if (size < sizeof (struct lev_del_list) + lev_list_id_bytes) { return -2; }
    delete_list (E->list_id, (struct lev_generic *)E);
    return sizeof (struct lev_del_list) + lev_list_id_bytes;
  case LEV_LI_DEL_OBJ:
    if (size < sizeof (struct lev_del_obj) + lev_object_id_bytes) { return -2; }
    delete_object (E->list_id); // note this is object id :)
    return sizeof (struct lev_del_obj) + lev_object_id_bytes;
  case LEV_LI_DEL_ENTRY:
    if (size < sizeof (struct lev_del_entry) + lev_list_object_bytes) { return -2; }
    delete_entry (E->list_id, LEV_OBJECT_ID(E), 1, (struct lev_generic *)E);
    return sizeof (struct lev_del_entry) + lev_list_object_bytes;
  case LEV_LI_SUBLIST_FLAGS:
    if (size < sizeof (struct lev_sublist_flags) + lev_list_id_bytes) { return -2; }
    change_sublist_flags (E->list_id, *((int *) &(((struct lev_sublist_flags *) LEV_ADJUST_L(E))->xor_cond)), (struct lev_generic *)E);
    return sizeof (struct lev_sublist_flags) + lev_list_id_bytes;
  case LEV_LI_DEL_SUBLIST:
    if (size < sizeof (struct lev_del_sublist) + lev_list_id_bytes) { return -2; }
    delete_sublist (E->list_id, *((int *) &(((struct lev_del_sublist *) LEV_ADJUST_L(E))->xor_cond)), (struct lev_generic *)E);
    return sizeof (struct lev_del_sublist) + lev_list_id_bytes;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, (long long) log_cur_pos());

  return -1;

}

/* adding new log entries */

long long tot_metafiles_memory;
long long tot_metafiles_marked_memory;
long long metafiles_load_success;
int metafiles_loaded;
long long metafiles_load_errors;
int metafiles_marked;
int data_metafiles_loaded;
int revlist_metafiles_loaded;
long long tot_aio_loaded_bytes;
long long tot_lost_aio_bytes;

extern long long memory_for_metafiles;

static inline void *get_metafile_ptr (int p) {
  if (!metafile_mode) {
    return get_metafile_ptr_old (p);
  } else {
    assert (p >= 0 && p < Header.tot_lists);
    assert (metafiles[p]);
    assert (metafiles[p]->data);
    return metafiles[p]->data
#ifdef LISTS_Z
	 + list_id_bytes
#endif	
    ;
  }
}

static inline void *get_revlist_metafile_ptr (int p) {
  assert (revlist_metafile_mode);
  assert (p >= 0 && p < tot_revlist_metafiles);
  assert (metafiles[p + Header.tot_lists + 1]);
  assert (metafiles[p + Header.tot_lists + 1]->data);
  return metafiles[p + Header.tot_lists + 1]->data;
}

void add_use (int x) {
  vkprintf (4, "add_use: x = %d\n", x);
  assert (metafiles[x]);
  assert (metafiles[x]->prev == -1 && metafiles[x]->next == -1);
  metafiles[x]->prev = metafiles[head_metafile]->prev;
  metafiles[metafiles[x]->prev]->next = x;
  metafiles[x]->next = head_metafile;
  metafiles[head_metafile]->prev = x;
}

void del_use (int x) {
  vkprintf (4, "del_use: x = %d, metafiles[x]->prev = %d, metafiles[x]->next = %d\n", x, metafiles[x]->prev, metafiles[x]->next);
  assert (metafiles[x]);
  assert (metafiles[x]->prev >= 0);
  assert (metafiles[x]->next >= 0);
  metafiles[metafiles[x]->prev]->next = metafiles[x]->next;
  metafiles[metafiles[x]->next]->prev = metafiles[x]->prev;
  metafiles[x]->prev = -1;
  metafiles[x]->next = -1;
}

void update_use (int x) {
  vkprintf (4, "update_use: x = %d\n", x);
  assert (0 <= x && x < head_metafile);
  assert (metafiles[x]);
  if (metafiles[x]->prev == -1 && metafiles[x]->next == -1) {
    return;
  }
  del_use (x);
  add_use (x);
}

void update_revlist_use (int x) {
  update_use (x + Header.tot_lists + 1);
}


//struct aio_connection *WaitAio;
int onload_metafile (struct connection *c, int read_bytes);

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_metafile
};

conn_query_type_t aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "list-aio-metafile-query",
  .wakeup = aio_query_timeout,
  .close = delete_aio_query,
  .complete = delete_aio_query
};

int postponed_operations_performed;
int postponed_operations_total;
long long postponed_operations_size;
int postponed_replay;

void do_postponed (int x) {
  assert (0 <= x && x < Header.tot_lists);
  struct postponed_operation *E = postponed[x];
  if (E) {
    struct postponed_operation *G = E, *F;
    postponed_replay = 1;
    do {
      now_override = E->time;
      assert (E->size == lists_replay_logevent ((struct lev_generic *)(E->E), E->size));
      now_override = 0;
      F = E;
      E = E->next;
      if (!(metafile_mode & 2)) {
        zfree (F, F->size + sizeof (struct postponed_operation));
      }
      postponed_operations_performed ++;
    } while (G != E && F != E);
    postponed_replay = 0;
  }
  if (!(metafile_mode & 2)) {
    postponed[x] = 0;
  }
}

void insert_postponed (int x, struct lev_generic *E, int size) {
  assert (0 <= x && x < Header.tot_lists);
  if (postponed_replay) {
    return;
  }
  vkprintf (4, "Insert postponed: metafile_number = %d, lev_type = %d, size = %d\n", x, E->type, size);
  postponed_operations_total ++;
  struct postponed_operation *F = zmalloc (size + sizeof (struct postponed_operation));
  postponed_operations_size += size + sizeof (struct postponed_operation);
  F->size = size;
  F->next = 0;
  F->prev = 0;
  F->time = now;
  memcpy (F->E, E, size);
  if (!postponed[x]) {
    postponed[x] = F;
    F->next = F;
    F->prev = F;
  } else {
    struct postponed_operation *G = postponed[x];
    F->prev = G->prev;
    F->next = G;
    G->prev = F;
    F->prev->next = F;
  }
}

void clear_postponed (int x) {
  assert (0 <= x && x < Header.tot_lists);
  if (postponed_replay) {
    return;
  }
  struct postponed_operation *E = postponed[x];
  if (E) {
    struct postponed_operation *G = E, *F;
    do {
      F = E;
      E = E->next;
      zfree (F, F->size + sizeof (struct postponed_operation));
    } while (G != E && F != E);
  }
  postponed[x] = 0;
}

int log_event_size (struct lev_generic *E) {
  assert (E);
  int s;
  switch (E->type) {
  case LEV_LI_SET_ENTRY ... LEV_LI_SET_ENTRY+0xff:
  case LEV_LI_ADD_ENTRY ... LEV_LI_ADD_ENTRY+0xff:
  case LEV_LI_REPLACE_ENTRY ... LEV_LI_REPLACE_ENTRY+0xff:
    return sizeof (struct lev_new_entry) + lev_list_object_bytes;
  case LEV_LI_SET_ENTRY_LONG ... LEV_LI_SET_ENTRY_LONG+0xff:
  case LEV_LI_ADD_ENTRY_LONG ... LEV_LI_ADD_ENTRY_LONG+0xff:
  case LEV_LI_REPLACE_ENTRY_LONG ... LEV_LI_REPLACE_ENTRY_LONG+0xff:
    return sizeof (struct lev_new_entry_long) + lev_list_object_bytes;
  case LEV_LI_SET_ENTRY_EXT ... LEV_LI_SET_ENTRY_EXT+0xff:
  case LEV_LI_ADD_ENTRY_EXT ... LEV_LI_ADD_ENTRY_EXT+0xff:
    return sizeof (struct lev_new_entry_ext) + lev_list_object_bytes;
  case LEV_LI_SET_ENTRY_TEXT ... LEV_LI_SET_ENTRY_TEXT+0xff:
    s = E->type & 0xff;
    return sizeof (struct lev_set_entry_text) + lev_list_object_bytes + s;
  case LEV_LI_SET_FLAGS ... LEV_LI_SET_FLAGS+0xff:
    return sizeof (struct lev_set_flags) + lev_list_object_bytes;
  case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
  case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
    return sizeof (struct lev_set_flags) + lev_list_object_bytes;
  case LEV_LI_SET_FLAGS_LONG:
    return sizeof (struct lev_set_flags_long) + lev_list_object_bytes;
  case LEV_LI_CHANGE_FLAGS_LONG:
    return sizeof (struct lev_change_flags_long) + lev_list_object_bytes;
  case LEV_LI_SET_VALUE:
    return sizeof (struct lev_set_value) + lev_list_object_bytes;
  case LEV_LI_SET_VALUE_LONG:
    return sizeof (struct lev_set_value_long) + lev_list_object_bytes;
  case LEV_LI_INCR_VALUE:
    return sizeof (struct lev_set_value) + lev_list_object_bytes;
  case LEV_LI_INCR_VALUE_LONG:
    return sizeof (struct lev_set_value_long) + lev_list_object_bytes;
  case LEV_LI_INCR_VALUE_TINY ... LEV_LI_INCR_VALUE_TINY + 0xff:
    return sizeof (struct lev_del_entry) + lev_list_object_bytes;
  case LEV_LI_DEL_LIST:
    return sizeof (struct lev_del_list) + lev_list_id_bytes;
  case LEV_LI_DEL_OBJ:
    return sizeof (struct lev_del_obj) + lev_object_id_bytes;
  case LEV_LI_DEL_ENTRY:
    return sizeof (struct lev_del_entry) + lev_list_object_bytes;
  case LEV_LI_SUBLIST_FLAGS:
    return sizeof (struct lev_sublist_flags) + lev_list_id_bytes;
  case LEV_LI_DEL_SUBLIST:
    return sizeof (struct lev_del_sublist) + lev_list_id_bytes;
  default:
    fprintf (stderr, "unknown record type %08x\n", E->type);
    assert (0);
    break;
  }
   
  return -1;
}

int insert_postponed_list (list_id_t list_id, struct lev_generic *E) {
  int x = find_metafile (list_id);
  assert (x >= 0);
  /*if (x < 0) {
    return 0;
  }*/
  int size = log_event_size (E);
  insert_postponed (x, E, size);
  return 1;
}

void postpone (int x, struct lev_generic *E) {
  assert (0 <= x && x < Header.tot_lists);
  insert_postponed (x, E, log_event_size (E));
}

int check_metafile_loaded (int x, int use_aio) {
  assert (0 <= x && x < Header.tot_lists + tot_revlist_metafiles + 1);
  if (metafiles[x] && metafiles[x]->data) {
    if (use_aio == 0) {
      assert (!metafiles[x]->aio);
    }
    if (use_aio < 0 && metafiles[x]->aio) {
      return 0;
    }
    return 1;
  } else {
    return 0;
  }
}

int check_revlist_metafile_loaded (int x, int use_aio) {
  if (x == -1) {
    return 1;
  }
  return check_metafile_loaded (x + Header.tot_lists + 1, use_aio);
}

extern int aio_errors_verbosity;
int onload_metafile (struct connection *c, int read_bytes) {
  vkprintf (4, "onload_metafile(%p,%d) total_aio_time = %lf\n", c, read_bytes, total_aio_time);

  struct aio_connection *a = (struct aio_connection *)c;
  struct metafile *meta = (struct metafile *) a->extra;

  assert (a->basic_type == ct_aio);
  assert (meta != NULL);          

  if (meta->aio != a) {
    fprintf (stderr, "assertion (meta->aio == a) will fail\n");
    fprintf (stderr, "%p != %p\n", meta->aio, a);
  }

  assert (meta->aio == a);

  long long size, offset;
  int x = meta->num;
  if (x <= Header.tot_lists) {
    offset = get_metafile_offset (x);
    assert (offset >= 0);
    size = get_metafile_offset (x + 1) - offset;
    assert (size >= 4);
  } else {
    assert (x <= Header.tot_lists + tot_revlist_metafiles);
    offset = get_revlist_metafile_offset (x - Header.tot_lists - 1);
    assert (offset >= 0);
    size = get_revlist_metafile_offset (x - Header.tot_lists) - offset;
    assert (size >= 4);
  }
  assert (!(size & 3));
  if (crc32_check_mode && read_bytes == size) {
    if (x <= Header.tot_lists) {
      assert (compute_crc32 (metafiles[x]->data, size - 4) == *(((unsigned *)(metafiles[x]->data)) + (size >> 2) - 1));
    } else {
      if (compute_crc32 (metafiles[x]->data, size) != revlist_metafiles_crc32[x - Header.tot_lists - 1]) {
        vkprintf (0, "x = %d, y = %d, total = %d\n", x, x - Header.tot_lists - 1, tot_revlist_metafiles);
      }
      assert (compute_crc32 (metafiles[x]->data, size) == revlist_metafiles_crc32[x - Header.tot_lists - 1]);
    }
  }    

  if (read_bytes != size) {
    if (verbosity > 0 || aio_errors_verbosity) {
      fprintf (stderr, "ERROR reading metafile #%d: read %d bytes out of %lld: %m\n", meta->num, read_bytes, size);
    }
  }
  if (verbosity > 2) {
    fprintf (stderr, "*** Read metafile: read %d bytes\n", read_bytes);
  }

  if (read_bytes != size) {
    meta->aio = NULL;
    zzfree (meta->data, size);  
    tot_metafiles_memory -= size;
    meta->data = 0;
    metafiles_load_errors ++;
  } else {
    kfs_buffer_crypt (Snapshot, meta->data, size, offset);
    meta->aio = NULL;
    metafiles_loaded ++;
    add_use (meta->num);
    metafiles_load_success ++;
    tot_aio_loaded_bytes += read_bytes;
    if (x <= Header.tot_lists) { 
      //list_t *L = get_list_m (FLI_ENTRY_LIST_ID (x));
      list_t *L = __get_list_f (FLI_ENTRY_LIST_ID (x), -1);
      if (!L || L->metafile_index >= 0) {
        assert (!L || L->metafile_index == meta->num);
        do_postponed (meta->num);
      }
      data_metafiles_loaded ++;
    } else {
      revlist_metafiles_loaded ++;
    }
  }
  vkprintf (4, "onload_metafile finished\n");
  return 1;
}



int load_metafile_aio (int x, int use_aio) {
  //assert (!use_aio);
  if (verbosity >= 4) {
    fprintf (stderr, "load_metafile_aio. x = %d, use_aio = %d\n", x, use_aio);
  }
  assert (x >= 0);
  if (!metafiles[x]) {
    metafiles[x] = zmalloc0 (sizeof (struct metafile));
    metafiles[x]->next = -1;
    metafiles[x]->prev = -1;
  }
  long long offset, size;
  if (x <= Header.tot_lists) {
    offset = get_metafile_offset (x);
    assert (offset >= 0);
    size = get_metafile_offset (x + 1) - offset;
    assert (size >= 4);
  } else {
    assert (x <= Header.tot_lists + tot_revlist_metafiles);
    offset = get_revlist_metafile_offset (x - Header.tot_lists - 1);
    assert (offset >= 0);
    size = get_revlist_metafile_offset (x - Header.tot_lists) - offset;
    assert (size >= 4);
  }
  metafiles[x]->num = x;
  if (!metafiles[x]->data) {
    //metafiles[x]->data = zmalloc (size);
    metafiles[x]->data = zzmalloc (size);
    assert (metafiles[x]->data);
    tot_metafiles_memory += size;
  }
  if (use_aio == 0 && !metafiles[x]->aio) {
    assert (lseek (Snapshot->fd, offset, SEEK_SET) == offset);
    assert (kfs_read_file (Snapshot, metafiles[x]->data, size) == size);
    if (crc32_check_mode) {
      if (x <= Header.tot_lists) {
        assert (compute_crc32 (metafiles[x]->data, size - 4) == *(((unsigned *)(metafiles[x]->data)) + (size >> 2) - 1));
      } else {
        assert (compute_crc32 (metafiles[x]->data, size) == revlist_metafiles_crc32[x - Header.tot_lists - 1]);
      }
    }    
    add_use (x);
    vkprintf (4, "load success #%d. memory %lld\n", x, tot_metafiles_memory);

    if (x < Header.tot_lists) {
      assert (*(int *)(metafiles[x]->data) == FILE_LIST_MAGIC);
      //list_t *L = get_list_m (FLI_ENTRY_LIST_ID (x));
      list_t *L = __get_list_f (FLI_ENTRY_LIST_ID (x), -1);
      vkprintf (4, "L = %p\n", L);
      if (!L || L->metafile_index >= 0) {
        assert (!L || L->metafile_index == x);
        do_postponed (x);
      }
      data_metafiles_loaded ++;
    } else {
      revlist_metafiles_loaded ++;
    }
    metafiles_loaded ++;
    return 1;
  } else {
    struct metafile *meta = metafiles[x];
    if (meta->aio) {
      check_aio_completion (meta->aio);
      if (meta->aio != NULL) {
        //WaitAio = meta->aio;
        WaitAioArrAdd (meta->aio);
        return -2;
      }
      if (meta->data) {
        return 1;
      } else {
        fprintf (stderr, "Previous AIO query failed at %p (metafile_num = %d), scheduling a new one\n", meta, meta->num);
        meta->data = zzmalloc (size);  
        tot_metafiles_memory += size;
      }
    }
    meta->aio = create_aio_read_connection (Snapshot->fd, meta->data, offset, size, &ct_metafile_aio, meta);
    vkprintf (4, "AIO query created\n");
    assert (meta->aio != NULL);
    //WaitAio = meta->aio;
    WaitAioArrAdd (meta->aio);

    return -2;    
  }
}

/*int load_revlist_metafile_aio (int x, int use_aio) {
	x += Header.tot_lists + 1;
  if (use_aio < 0 && metafiles[x] && metafiles[x]->aio) {
    assert (metafiles[x]->data);
    tot_lost_aio_bytes += get_metafile_offset (x + 1) - get_metafile_offset (x);
    vkprintf (0, "skipping pending aio query. Total lost memory %lld\n", tot_lost_aio_bytes);
    metafiles[x]->data = 0;
    metafiles[x]->aio = 0;
  }
  return load_metafile_aio (x, (use_aio > 0) || (metafiles[x] && metafiles[x]->aio));
}*/

static int clear_list (list_id_t list_id) {
  //int metafile_number = find_metafile (list_id);
  //int metafile_number = get_list_m (list_id)->metafile_number;
  int metafile_number = get_list_metafile (list_id);
  if (metafile_number < 0) {
    return 0;
  }
  assert (check_metafile_loaded (metafile_number, -1));

  list_t *L = get_list_m (list_id);
  
  //if (verbosity > 2 || check_debug_list (list_id)) {
    vkprintf (check_debug_list (list_id) ? 0 : 2, "clear_list: now=%d, num = %d, list=" idout "\n", now, metafile_number, out_list_id (list_id));
  //}

  if (!L) {
    return 0;
  }

  current_list_id = list_id;
  delete_list_objects (L->o_tree, 0);
  L->o_tree = (tree_ext_large_t *)NIL;
  #ifdef LISTS_GT
  free_tree_ext_global (L->g_tree);
  #else
  free_tree_ext_small (L->g_tree);
  #endif
  L->g_tree = NILX;

  int i;
  for (i = 0; i < SUBCATS; i++) {
    free_tree_ext_small (L->o_tree_sub[i]);
    L->o_tree_sub[i] = NIL;
    #ifdef LISTS_GT
    free_tree_ext_global (L->g_tree_sub[i]);
    #else
    free_tree_ext_small (L->g_tree_sub[i]);
    #endif
    L->g_tree_sub[i] = NILX;
  }

#if 0
  // since List[] is a hash table now, we cannot free allocated list structures
  List[x] = 0;
  free_list_struct (L);
  tot_lists--;
#else
#endif
  M_metafile = (void *)-1;
  return 1;
}

int unload_metafile (int x) {
  vkprintf (4, "unloading metafile %d. head_metafile = %d\n", x, head_metafile);
  assert (metafiles[x]);
  assert (metafiles[x]->prev >= 0);
  assert (metafiles[x]->next >= 0);
  assert (!metafiles[x]->aio);
  long long offset, size;
  if (x < Header.tot_lists) {
    offset = get_metafile_offset (x);
    assert (offset >= 0);
    size = get_metafile_offset (x + 1) - offset;
    assert (size >= 4);
    data_metafiles_loaded --;
    if (metafile_mode & 6) {
      clear_list (FLI_ENTRY_LIST_ID(x));
    }
  } else {
    assert (x <= Header.tot_lists + tot_revlist_metafiles);
    offset = get_revlist_metafile_offset (x - Header.tot_lists - 1);
    assert (offset >= 0);
    size = get_revlist_metafile_offset (x - Header.tot_lists) - offset;
    assert (size >= 4);
    revlist_metafiles_loaded --;
  }
  zzfree (metafiles[x]->data, size);
  del_use (x);
  zfree (metafiles[x], sizeof (struct metafile));
  metafiles[x] = 0;
  tot_metafiles_memory -= size;
  metafiles_loaded --;
  vkprintf (4, "unload success #%d. memory %lld\n", x, tot_metafiles_memory);
  return 1; 
}

int unload_LRU () {
  if (metafiles[head_metafile]->next == head_metafile) {
    return 0;
  }
  unload_metafile (metafiles[head_metafile]->next);
  return 1;
}

int mark_list_metafile (list_id_t list_id) {
  //int x = find_metafile (list_id);
  //int x = get_list_m (E->list_id)->metafile_number;
  int x = get_list_metafile (list_id);
  assert (x >= 0);
  //if (x < 0) {
  //  return 0;
  //}
  if (metafiles[x]->prev >= 0) {
    long long size = get_metafile_offset (x + 1) - get_metafile_offset (x);
    tot_metafiles_memory -= size;
    tot_metafiles_marked_memory += size;
    del_use (x);
    metafiles_marked ++;
    return 1;
  }          
  return 0;
}

int prepare_list_metafile (list_id_t list_id, int use_aio) {
  vkprintf (4, "preparing metafile...\n");
  while (tot_metafiles_memory > memory_for_metafiles && unload_LRU ());  
  //int x = find_metafile (list_id);
  //int x = get_list_m (E->list_id)->metafile_number;
  int x = get_list_metafile (list_id);
  if (x < 0) {
    return 0;
  }
  assert (x < Header.tot_lists);
  vkprintf (4, "preparing metafile #%d of %d\n", x, Header.tot_lists);
  if (use_aio < 0 && metafiles[x] && metafiles[x]->aio) {
    assert (metafiles[x]->data);
    tot_lost_aio_bytes += get_metafile_offset (x + 1) - get_metafile_offset (x);
    vkprintf (0, "skipping pending aio query. Total lost memory %lld\n", tot_lost_aio_bytes);
    metafiles[x]->data = 0;
    metafiles[x]->aio = 0;
  }
  if (!metafiles[x]) {
    metafiles[x] = zmalloc0 (sizeof (struct metafile));
    metafiles[x]->next = -1;
    metafiles[x]->prev = -1;
  } else {
    if (!metafiles[x]->aio && metafiles[x]->data) {
      update_use (x);
      return 1;
    }
  }
  return load_metafile_aio (x, (use_aio > 0) || metafiles[x]->aio);  
}

int prepare_list_metafile_num (int x, int use_aio) {
  vkprintf (4, "preparing metafile...\n");
  while (tot_metafiles_memory > memory_for_metafiles && unload_LRU ());  
  if (x < 0) {
    return 0;
  }
  vkprintf (4, "preparing metafile #%d of %d\n", x, Header.tot_lists);
  if (use_aio < 0 && metafiles[x] && metafiles[x]->aio) {
    if (!metafiles[x]->data) {
      fprintf (stderr, "ERROR!\n");
      fprintf (stderr, "Preparing metafile #%d of %d\n", x, Header.tot_lists);
      fprintf (stderr, "meta->aio = %p, metafiles[x] = %p\n", metafiles[x]->aio, metafiles[x]);
    }
    assert (metafiles[x]->data);
    tot_lost_aio_bytes += get_metafile_offset (x + 1) - get_metafile_offset (x);
    vkprintf (0, "skipping pending aio query. Total lost memory %lld\n", tot_lost_aio_bytes);
    metafiles[x]->data = 0;
    metafiles[x]->aio = 0;
  }
  if (!metafiles[x]) {
    metafiles[x] = zmalloc0 (sizeof (struct metafile));
    metafiles[x]->next = -1;
    metafiles[x]->prev = -1;
  } else {
    if (!metafiles[x]->aio && metafiles[x]->data) {
      update_use (x);
      return 1;
    }
  }
  return load_metafile_aio (x, (use_aio > 0) || metafiles[x]->aio);  
}

int prepare_object_metafile (object_id_t object_id, int use_aio) {
  long rm = choose_revlist_metafile (object_id);
  if (rm >= tot_revlist_metafiles) {
    return 0;
  }
  assert (rm >= 0);
  while (tot_metafiles_memory > memory_for_metafiles && unload_LRU ());  

  int x = rm + Header.tot_lists + 1;
  vkprintf (3, "prepare_object_metafile: x = %d, head_metafile = %d\n", x, head_metafile);
  if (use_aio < 0 && metafiles[x] && metafiles[x]->aio) {
    assert (metafiles[x]->data);
    tot_lost_aio_bytes += get_revlist_metafile_offset (x - Header.tot_lists) - get_revlist_metafile_offset (x - Header.tot_lists - 1);
    vkprintf (0, "skipping pending aio query. Total lost memory %lld\n", tot_lost_aio_bytes);
    metafiles[x]->data = 0;
    metafiles[x]->aio = 0;
  }
  if (!metafiles[x]) {
    metafiles[x] = zmalloc0 (sizeof (struct metafile));
    metafiles[x]->next = -1;
    metafiles[x]->prev = -1;
  } else {
    if (!metafiles[x]->aio && metafiles[x]->data) {
      update_use (x);
      return 1;
    }
  }
  return load_metafile_aio (x, (use_aio > 0) || metafiles[x]->aio);  
}

int prepare_object_metafile_num (long rm, int use_aio) {
  if (rm >= tot_revlist_metafiles || rm < 0) {
    return 0;
  }
  assert (rm >= 0);
  while (tot_metafiles_memory > memory_for_metafiles && unload_LRU ());  

  int x = rm + Header.tot_lists + 1;
  vkprintf (3, "prepare_object_metafile: x = %d, head_metafile = %d\n", x, head_metafile);
  if (use_aio < 0 && metafiles[x] && metafiles[x]->aio) {
    assert (metafiles[x]->data);
    tot_lost_aio_bytes += get_revlist_metafile_offset (x - Header.tot_lists) - get_revlist_metafile_offset (x - Header.tot_lists - 1);
    vkprintf (0, "skipping pending aio query. Total lost memory %lld\n", tot_lost_aio_bytes);
    metafiles[x]->data = 0;
    metafiles[x]->aio = 0;
  }
  if (!metafiles[x]) {
    metafiles[x] = zmalloc0 (sizeof (struct metafile));
    metafiles[x]->next = -1;
    metafiles[x]->prev = -1;
  } else {
    if (!metafiles[x]->aio && metafiles[x]->data) {
      update_use (x);
      return 1;
    }
  }
  return load_metafile_aio (x, (use_aio > 0) || metafiles[x]->aio);  
}

void dump_msizes (void) {
  int i;
  for (i = 0; i < Header.tot_lists; i++) {
    fprintf (stderr, "List " idout " %lld bytes\n", out_list_id (FLI_ENTRY_LIST_ID(i)), get_metafile_offset (i + 1) - get_metafile_offset (i)); 
  }
}

int do_delete_list (list_id_t list_id) {
  if (conv_list_id (list_id) < 0) {
    return -1;
  }
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  void *E = alloc_log_event (LEV_LI_DEL_LIST, sizeof (struct lev_del_list) + lev_list_id_bytes, FIRST_INT(list_id));
  upcopy_list_id (E, list_id);
  return delete_list (list_id, (struct lev_generic *)E);
}


int do_delete_object (object_id_t object_id) {
  if (revlist_metafile_mode && prepare_object_metafile (object_id, 1) < 0) {
    return -2;
  }
  void *E = alloc_log_event (LEV_LI_DEL_OBJ, sizeof (struct lev_del_obj) + lev_object_id_bytes, FIRST_INT(object_id));
  upcopy_object_id (E, object_id);
  return delete_object (object_id);
}


int do_delete_list_entry (list_id_t list_id, object_id_t object_id) {
  if (conv_list_id (list_id) < 0) {
    return -1;
  }
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }

  struct lev_del_entry *E = alloc_log_event (LEV_LI_DEL_ENTRY, sizeof (struct lev_del_entry) + lev_list_object_bytes, FIRST_INT (list_id));
  upcopy_list_object_id (E, list_id, object_id);

  return delete_entry (list_id, object_id, 1, (struct lev_generic *)E);
}

int do_delete_sublist (list_id_t list_id, int xor_cond, int and_cond) {
  if (conv_list_id (list_id) < 0) {
    return -1;
  }
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  struct lev_sublist_flags *EE, *E = alloc_log_event (LEV_LI_DEL_SUBLIST, sizeof (struct lev_del_sublist) + lev_list_id_bytes, FIRST_INT (list_id));
  upcopy_list_id (E, list_id);
  EE = LEV_ADJUST_L(E);
  EE->xor_cond = xor_cond;
  EE->and_cond = and_cond;
  EE->and_set = 0;
  EE->xor_set = 0;
  return delete_sublist (list_id, *(int *) &(EE->xor_cond), (struct lev_generic *)E);
}

int entry_exists (list_id_t list_id, object_id_t object_id);

/* mode: 0 = set, 1 = replace, 2 = add, 3 = set, but load metafile */
int do_add_list_entry (list_id_t list_id, object_id_t object_id, int mode, int flags, value_t value, const int *extra) {
  assert (mode >= 0 && mode <= 3);
  if (conv_list_id (list_id) < 0 || (flags & -0x100)) {
    return -1;
  }
  if (metafile_mode && mode != 0 && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  if (extra && !extra[0] && !extra[1] && !extra[2] && !extra[3]) {
    extra = 0;
  }
  if (extra && mode == 1) {
    extra = 0;
  }
#ifdef VALUES64
  if (extra && value != (int) value) {
    extra = 0;
  }
#endif
  if (mode == 3) {
    mode = 0;
  }

  if (mode == 1 && !entry_exists (list_id, object_id)) {
    return 0;
  }
  if (mode == 2 && entry_exists (list_id, object_id)) {
    return 0;
  }
  struct lev_new_entry_ext *E, *EE;
  int ext;
  if (extra) {
    E = alloc_log_event (LEV_LI_SET_ENTRY_EXT - (mode << 8) + flags, sizeof (struct lev_new_entry_ext) + lev_list_object_bytes, FIRST_INT (list_id));
    upcopy_list_object_id (E, list_id, object_id);
    EE = LEV_ADJUST_LO(E);
    EE->value = value;
    memcpy (EE->extra, extra, 16);
    ext = 0;
#ifdef VALUES64
  } else if (value != (int) value) {
    E = alloc_log_event (LEV_LI_SET_ENTRY_LONG - (mode << 8) + flags, sizeof (struct lev_new_entry_long) + lev_list_object_bytes, FIRST_INT (list_id));
    upcopy_list_object_id (E, list_id, object_id);
    struct lev_new_entry_long *EE = (struct lev_new_entry_long *) LEV_ADJUST_LO(E);
    EE->value = value;
    ext = 2;
#endif
  } else {
    E = alloc_log_event (LEV_LI_SET_ENTRY - (mode << 8) + flags, sizeof (struct lev_new_entry) + lev_list_object_bytes, FIRST_INT (list_id));
    upcopy_list_object_id (E, list_id, object_id);
    EE = LEV_ADJUST_LO(E);
    EE->value = value;
    ext = 1;
  }
  return set_list_entry (E, ext, value);
}


int do_change_entry_flags (list_id_t list_id, object_id_t object_id, int set_flags, int clear_flags) {
  int res = 0;
  if (conv_list_id (list_id) < 0) {
    return -1;
  }
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  if ((clear_flags | set_flags) == -1 && !(set_flags & -0x100)) {
    struct lev_set_flags *E = alloc_log_event (LEV_LI_SET_FLAGS + set_flags, sizeof (struct lev_set_flags) + lev_list_object_bytes, FIRST_INT(list_id));
    upcopy_list_object_id (E, list_id, object_id);
    return set_entry_flags (E);
  }
  if ((set_flags & -0x100) || (clear_flags & -0x100)) {
    if ((clear_flags | set_flags) == -1) {
      struct lev_set_flags_long *E = alloc_log_event (LEV_LI_SET_FLAGS_LONG, sizeof (struct lev_set_flags_long) + lev_list_object_bytes, FIRST_INT(list_id));
      upcopy_list_object_id (E, list_id, object_id);
      ((struct lev_set_flags_long *) LEV_ADJUST_LO (E))->flags = set_flags;
      return set_entry_flags ((struct lev_set_flags *) E);
    } else {
      struct lev_change_flags_long *E = alloc_log_event (LEV_LI_CHANGE_FLAGS_LONG, sizeof (struct lev_change_flags_long) + lev_list_object_bytes, FIRST_INT(list_id));
      upcopy_list_object_id (E, list_id, object_id);
      struct lev_change_flags_long *EE = LEV_ADJUST_LO (E);
      EE->and_mask = ~(clear_flags | set_flags);
      EE->xor_mask = set_flags;
      return set_entry_flags ((struct lev_set_flags *) E);
    }
  }
  if (clear_flags &= ~set_flags) {
    struct lev_set_flags *E = alloc_log_event (LEV_LI_DECR_FLAGS + clear_flags, sizeof (struct lev_set_flags) + lev_list_object_bytes, FIRST_INT(list_id));
    upcopy_list_object_id (E, list_id, object_id);
    res = set_entry_flags (E);
  }
  if (set_flags) {
    struct lev_set_flags *E = alloc_log_event (LEV_LI_INCR_FLAGS + set_flags, sizeof (struct lev_set_flags) + lev_list_object_bytes, FIRST_INT(list_id));
    upcopy_list_object_id (E, list_id, object_id);
    res = set_entry_flags (E);
  }
  return res;
}

int do_change_entry_text (list_id_t list_id, object_id_t object_id, const char *text, int len) {
  if (conv_list_id (list_id) < 0 || (unsigned) len >= max_text_len) {
    return -1;
  }
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  if (len < 256) {
    struct lev_set_entry_text *E = alloc_log_event (LEV_LI_SET_ENTRY_TEXT + len, sizeof (struct lev_set_entry_text) + len + lev_list_object_bytes, FIRST_INT(list_id));
    upcopy_list_object_id (E, list_id, object_id);
    memcpy (((struct lev_set_entry_text *) LEV_ADJUST_LO (E))->text, text, len);
    return set_entry_text (list_id, object_id, text, len, (struct lev_generic *)E);
  } else {
    struct lev_set_entry_text_long *E = alloc_log_event (LEV_LI_SET_ENTRY_TEXT_LONG, sizeof (struct lev_set_entry_text_long) + len + lev_list_object_bytes, FIRST_INT(list_id));
    upcopy_list_object_id (E, list_id, object_id);
    E->len = len;
    memcpy (((struct lev_set_entry_text *) LEV_ADJUST_LO (E))->text, text, len);
    return set_entry_text (list_id, object_id, text, len, (struct lev_generic *)E);
  }
}

long long do_change_entry_value (list_id_t list_id, object_id_t object_id, value_t value, int incr) {
  if (conv_list_id (list_id) < 0) {
    return -1LL << 63;
  }
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -1LL << 63;
  }
  struct lev_set_value *E;
  if (incr && value == (signed char) value) {
    E = alloc_log_event (LEV_LI_INCR_VALUE_TINY + (value & 0xff), sizeof (struct lev_del_entry) + lev_list_object_bytes, FIRST_INT (list_id));
#ifdef VALUES64
  } else if (value != (int) value) {
    E = alloc_log_event (incr ? LEV_LI_INCR_VALUE_LONG : LEV_LI_SET_VALUE_LONG, sizeof (struct lev_set_value_long) + lev_list_object_bytes, FIRST_INT (list_id));
    ((struct lev_set_value_long *) LEV_ADJUST_LO (E))->value = value;
#endif
  } else {
    E = alloc_log_event (incr ? LEV_LI_INCR_VALUE : LEV_LI_SET_VALUE, sizeof (struct lev_set_value) + lev_list_object_bytes, FIRST_INT (list_id));
    ((struct lev_set_value *) LEV_ADJUST_LO (E))->value = value;
  }

  upcopy_list_object_id (E, list_id, object_id);
  return set_incr_entry_value (list_id, object_id, value, incr, (struct lev_generic *)E);
}

int do_change_sublist_flags (list_id_t list_id, int xor_cond, int and_cond, int and_set, int xor_set) {
  if (conv_list_id (list_id) < 0) {
    return -1;
  }
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  struct lev_sublist_flags *EE, *E = alloc_log_event (LEV_LI_SUBLIST_FLAGS, sizeof (struct lev_sublist_flags) + lev_list_id_bytes, FIRST_INT (list_id));
  upcopy_list_id (E, list_id);
  EE = LEV_ADJUST_L(E);
  EE->xor_cond = xor_cond;
  EE->and_cond = and_cond;
  EE->and_set = and_set;
  EE->xor_set = xor_set;
  return change_sublist_flags (list_id, *(int *) &(EE->xor_cond), (struct lev_generic *)E);
}



long long do_add_incr_value (list_id_t list_id, object_id_t object_id, int flags, value_t value, const int *extra) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -1LL << 63;
  }
  if (entry_exists (list_id, object_id)) {
    return do_change_entry_value (list_id, object_id, value, 1);
  } else {
    if (do_add_list_entry (list_id, object_id, 2, flags, value, extra) <= 0) {
      return -1LL << 63;
    } else {
      return value;
    }
  }
}

/* simple data retrieval */

int fetch_list_entry (list_id_t list_id, object_id_t object_id, int result[13]) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  list_t *L = __get_list_f (list_id, 2);
  if (!L) {
    return -1;
  }

  unpack_metafile_pointers (L);

  int temp_id;
  tree_ext_large_t *T = listree_lookup_large (&OTree, object_id, &temp_id);

  if (!T) {
    return -1;
  }

  memset (result + 6, 0, 28);

  if (T != (void *) -1) {
    struct tree_payload *P = PAYLOAD (T);
    result[0] = P->flags;
    result[1] = P->date;
    *((long long *) (result + 2)) = (long long) P->global_id;
    *((long long *) (result + 4)) = (long long) P->value;

    if (P->text) {
      result[12] = get_text_len (P->text);
      *((char **) (result + 10)) = get_text_ptr (P->text);
    } else {
      assert (NODE_TYPE (T) != TF_PLUS);
      *((char **) (result + 10)) = metafile_get_text (OTree.N - NODE_RPOS (T), result + 12);
    }
  } else {
    result[0] = metafile_get_flags (temp_id);
    result[1] = metafile_get_date (temp_id);
    *((long long *) (result + 2)) = (long long) M_global_id_list[temp_id];
    *((long long *) (result + 4)) = (long long) metafile_get_value (temp_id);
    *((char **) (result + 10)) = metafile_get_text (temp_id, result + 12);
  }
  return 6;
}

int entry_exists (list_id_t list_id, object_id_t object_id) {
  if (metafile_mode && prepare_list_metafile (list_id, -1) < 0) {
    assert (0);
    return 0;
  }
  list_t *L = __get_list_f (list_id, -1);
  int metafile_index = -1;

  listree_direct_t OTree;
  tree_ext_large_t *Root;

  if (!L) {
    metafile_index = find_metafile (list_id);
    if (metafile_index < 0) {
      return 0;
    }
    Root = NILL;
    OTree.root = &Root;
  } else {
    metafile_index = L->metafile_index;
    OTree.root = &L->o_tree;
  }

  if (metafile_index >= 0) {
    metafile_t *M = get_metafile_ptr (metafile_index);
    OTree.N = M->tot_entries;
    OTree.A = (array_object_id_t *) M->data;
  } else {
    OTree.N = 0;
    OTree.A = 0;
  }

  int temp_id;
  tree_ext_large_t *T = listree_lookup_large_tree (&OTree, object_id, &temp_id);

  return T ? 1 : 0;
}

int entry_exists_nof (list_id_t list_id, object_id_t object_id) {
  list_t *L = __get_list_f (list_id, -1);
  if (metafile_mode & 1) {
    if (!L) {
      return 1;
    }
    unpack_metafile_pointers_short (L);
    int temp_id;
    tree_ext_large_t *T = listree_lookup_large_tree (&OTree, object_id, &temp_id);
    if ((T && (T != (void *)-1)) || ((T == (void *)-1) && get_list_metafile (list_id) >= 0)) {
      return 1;
    } else {
      return 0;
    }
  }
  if (metafile_mode) {
    if (!L) {
      return 1;
    }
    //int metafile_number = find_metafile (list_id);
    //int metafile_number = get_list_m (list_id)->metafile_number;
    int metafile_number = get_list_metafile (list_id);
    if (metafile_number < 0) {
      return 1;
    }    
    if (!check_metafile_loaded (metafile_number, -1)) {  // ??? NOT SURE about this -1, was 0 before
      return 1;
    }
    unpack_metafile_pointers (L);
  }
  return entry_exists (list_id, object_id);
}


/* counter_id = -1 (all) or 0..3 */
int get_list_counter (list_id_t list_id, int counter_id) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  list_t *L = __get_list_f (list_id, 2);
  if (!L) {
    return -1;
  }

  unpack_metafile_pointers (L);

  if ((unsigned) counter_id < SUBCATS) {
    return metafile_get_sublist_size (counter_id);
  } else {
    return M_tot_entries + L->o_tree->delta;
  }
}

int fetch_list_counters (list_id_t list_id, int result[SUBCATS+1]) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  int i;
  list_t *L = __get_list_f (list_id, 2);
  if (!L) {
    return -1;
  }
  unpack_metafile_pointers (L);

  result[0] = M_tot_entries + L->o_tree->delta;
  for (i = 0; i < 8; i++) {
    result[i + 1] = metafile_get_sublist_size (i);
  }

  return SUBCATS+1;
}

int get_entry_position (list_id_t list_id, object_id_t object_id) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  list_t *L = __get_list_f (list_id, 2);
  if (!L) {
    return -1;
  }
  unpack_metafile_pointers (L);

  return listree_get_pos_large (&OTree, object_id, 1);
}



/* ----- advanced data retrieval ---- */

int R[MAX_RES+64];
int *R_end, R_entry_size;

static int R_mode, R_limit;/*, R_skip, *R_max;*/
//static tree_t *R_tree;

static inline void store_object_id (int **P, object_id_t object_id) {
  //*((*(object_id_t **)P)++) = object_id;
  //GNU C MUST DIE
  int *PVal = *P;
  #ifdef LISTS_Z
  memcpy (PVal, object_id, object_id_bytes);
  #else
  *(object_id_t *)PVal = object_id;
  #endif
  *P = PVal + OBJECT_ID_INTS;
}

static inline void store_object_id_overlap (int **P, object_id_t object_id) {
  //*((*(object_id_t **)P)++) = object_id;
  //GNU C MUST DIE
  int *PVal = *P;
  #ifdef LISTS_Z
  memmove (PVal, object_id, object_id_bytes);
  #else
  *(object_id_t *)PVal = object_id;
  #endif
  *P = PVal + OBJECT_ID_INTS;
}

static inline void store_object_id_rev (int **P, object_id_t object_id) {
  //*(--(*(object_id_t **)P)) = object_id;
  //GNU C MUST DIE
  int *PVal = (*P) - OBJECT_ID_INTS;
  #ifdef LISTS_Z
  memcpy (PVal, object_id, object_id_bytes);
  #else
  *(object_id_t *)PVal = object_id;
  #endif
  *P = PVal;
}

static inline void store_value (int **P, value_t value) {
  //*((*(value_t **)P)++) = value;
  //GNU C MUST DIE
  int *PVal = *P;
  *(value_t *)PVal = value;
  *P = PVal + VALUE_INTS;
}

static inline void store_value_rev (int **P, value_t value) {
  //*(--(*(value_t **)P)) = value;
  //GNU C MUST DIE
  int *PVal = (*P) - VALUE_INTS;
  *(value_t *)PVal = value;
  *P = PVal;
}

static inline int barray_out_node (listree_t *LT, int temp_id) {
  int m = R_mode, *p = R_end;
  assert (R_end < R + MAX_RES);
  store_object_id (&p, OARR_ENTRY (M_obj_id_list, temp_id));
  if (m & 64) {
    *p++ = metafile_get_flags (temp_id);
  }
  if (m & 128) {
    *p++ = metafile_get_date (temp_id);
  }
  if (m & 256) {
    *p++ = M_global_id_list[temp_id];
  }
  if (m & 512) {
    store_value (&p, metafile_get_value (temp_id));
  }
  if (m & 1024) {

    int text_len;

    *((char **)p) = metafile_get_text (temp_id, &text_len);
    
    p += PTR_INTS;
    *p++ = text_len;
  }
  R_end = p;
  return 0;
}


static inline int btree_out_node (tree_ext_small_t *T) {
  int m = R_mode, *p = R_end;
  assert (R_end < R + MAX_RES);
  assert (T);
  store_object_id (&p, T->x);

  struct tree_payload *P = LPAYLOAD (T);
  
  //fprintf (stderr, "%p\n", p);
  if (m & 64) {
    *p++ = P->flags;
  }
  if (m & 128) {
    *p++ = P->date;
  }
  if (m & 256) {
    *p++ = P->global_id;
  }
  if (m & 512) {
    store_value (&p, P->value);
  }
  if (m & 1024) {
    int text_len;

    if (P->text) {
      *((char **)p) = P->text == empty_string ? 0 : get_text_ptr (P->text);
      text_len = get_text_len (P->text);
    } else {
      assert (NODE_TYPE (T) != TF_PLUS);
      *((char **)p) = metafile_get_text (OTree.N - NODE_RPOS (T), &text_len);
    }
    
    p += PTR_INTS;
    *p++ = text_len;
  }
  R_end = p;
  return 0;
}

static inline int barray_out_node_rev (listree_t *LT, int temp_id) {
  int m = R_mode, *p = R_end;
  assert (R_end < R + MAX_RES);
  //assert (LT);
  if (m & 1024) {
    int text_len;
    *((char **)(p - PTR_INTS - 1)) = metafile_get_text (temp_id, &text_len);
    *--p = text_len; 
    p -= PTR_INTS;
  }
  if (m & 512) {
    store_value_rev (&p, metafile_get_value (temp_id));
  }
  if (m & 256) {
    *--p = M_global_id_list[temp_id];
  }
  if (m & 128) {
    *--p = metafile_get_date (temp_id);
  }
  if (m & 64) {
    *--p = metafile_get_flags (temp_id);
  }
  store_object_id_rev (&p, OARR_ENTRY (M_obj_id_list, temp_id));
  R_end = p;
  return 0;
}


static inline int btree_out_node_rev (tree_ext_small_t *T) {
  int m = R_mode, *p = R_end;
  struct tree_payload *P = LPAYLOAD (T);
  if (m & 1024) {
    int text_len;

    if (P->text) {
      *((char **)(p - PTR_INTS - 1)) = P->text == empty_string ? 0 : get_text_ptr (P->text);
      text_len = get_text_len (P->text);
    } else {
      assert (NODE_TYPE (T) != TF_PLUS);
      *((char **)(p - PTR_INTS - 1)) = metafile_get_text (OTree.N - NODE_RPOS (T), &text_len);
    }

    *--p = text_len; 
    p -= PTR_INTS;
  }
  if (m & 512) {
    store_value_rev (&p, P->value);
  }
  if (m & 256) {
    *--p = P->global_id;
  }
  if (m & 128) {
    *--p = P->date;
  }
  if (m & 64) {
    *--p = P->flags;
  }
  store_object_id_rev (&p, T->x);
  R_end = p;
  return 0;
}


static inline int b_out (object_id_t object_id) {
  int m = R_mode;
  if (m & 0x7c0) {
    int temp_id = -1;
    tree_ext_large_t *T = listree_lookup_large (&OTree, object_id, &temp_id);
    if (T != (void *) -1) {
      return btree_out_node (SMALL_NODE(T));
    } else {
      return barray_out_node (0, temp_id);
    }
  } else {
    store_object_id (&R_end, object_id);
  }
  return 0;
}


static inline void b_out_void (object_id_t object_id) {
  (void) b_out (object_id);
}

static inline int b_out_g (object_id_t object_id, global_id_t global_id) {
  int m = R_mode;
  if (m & 0x6c0) {
    int temp_id = -1;
    tree_ext_large_t *T = listree_lookup_large (&OTree, object_id, &temp_id);
    if (T != (void *) -1) {
      return btree_out_node (SMALL_NODE(T));
    } else {
      return barray_out_node (0, temp_id);
    }
  } else {
    int *p = R_end;
    store_object_id (&p, object_id);
    if (m & 256) {
      *p++ = (int)global_id;
    }
    R_end = p;
  }
  return 0;
}


static inline int b_out_rev (object_id_t object_id) {
  int m = R_mode;
  if (m & 0x7c0) {
    int temp_id = -1;
    tree_ext_large_t *T = listree_lookup_large (&OTree, object_id, &temp_id);
    assert (T);
    if (T != (void *)-1) {
      return btree_out_node_rev (SMALL_NODE(T));
    } else {
      return barray_out_node_rev (0, temp_id);
    }
  } else {
    store_object_id_rev (&R_end, object_id);
  }
  return 0;
}


static inline int b_out_g_rev (object_id_t object_id, global_id_t global_id) {
  int m = R_mode;
  if (m & 0x6c0) {
    int temp_id = -1;
    tree_ext_large_t *T = listree_lookup_large (&OTree, object_id, &temp_id);
    if (T != (void *)-1) {
      return btree_out_node_rev (SMALL_NODE(T));
    } else {
      return barray_out_node_rev (0, temp_id);
    }
  } else {
    int *p = R_end;
    if (m & 256) {
      *--p = (int)global_id;
    }
    store_object_id_rev (&p, object_id);
    R_end = p;
  }
  return 0;
}


static inline int carray_out_node (listree_t *LI, int temp_id) {
  return b_out (OARR_ENTRY (LI->DA, LI->IA[temp_id]));
}

static inline int darray_out_node (listree_t *LI, int temp_id) {
  listree_xglobal_t *LX = (listree_xglobal_t *)LI;
  return b_out_g (OARR_ENTRY (M_obj_id_list, LX->IA[temp_id]), LX->DA[LX->IA[temp_id]]);
}


static inline int ctree_out_node (tree_ext_small_t *T) {
  return b_out (T->x);
}


static inline int dtree_out_node (tree_ext_small_t *T) {
  #ifdef LISTS_GT
  return b_out_g (GLOBAL_NODE (T)->z, GLOBAL_NODE (T)->x);
  #else
  return b_out_g (T->y * Y_MULT_INV, T->x);
  #endif
}


static inline int carray_out_node_rev (listree_t *LI, int temp_id) {
  return b_out_rev (OARR_ENTRY (LI->DA, LI->IA[temp_id]));
}


static inline int darray_out_node_rev (listree_t *LI, int temp_id) {
  listree_xglobal_t *LX = (listree_xglobal_t *)LI;
  return b_out_g_rev (OARR_ENTRY (M_obj_id_list, LX->IA[temp_id]), LX->DA[LX->IA[temp_id]]);
}


static inline int ctree_out_node_rev (tree_ext_small_t *T) {
  return b_out_rev (T->x);
}


static inline int dtree_out_node_rev (tree_ext_small_t *T) {
  #ifdef LISTS_GT
  return b_out_g_rev (GLOBAL_NODE (T)->z, GLOBAL_NODE (T)->x);
  #else
  return b_out_g_rev (T->y * Y_MULT_INV, T->x);
  #endif
}


int prepare_list (list_id_t list_id, int mode, int limit, int offset) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  int cat = mode & (SUBCATS-1);
  int entry_words = OBJECT_ID_INTS;
  int tot_size;
  list_t *L = __get_list_f (list_id, 2);

  R_mode = mode;
  R_end = R;

  vkprintf (2, "prepare_list(" idout ", %d, %d, %d) : L=%p, metafile_index=%d\n", out_list_id (list_id), mode, limit, offset, L, L ? L->metafile_index : -1);

  if (!L) {
    R_entry_size = 1;
    return conv_list_id (list_id) < 0 ? -1 : 0;
  }

  unpack_metafile_pointers (L);

  value_offset = -1;

  if (mode & 64) { entry_words++; }
  if (mode & 128) { entry_words++; }
  if (mode & 256) { entry_words++; }
  if (mode & 512) { 
    value_offset = entry_words;
    entry_words += VALUE_INTS; 
  }
  if (mode & 1024) { entry_words += PTR_INTS + 1; }

  R_entry_size = entry_words;

  if ((unsigned) offset > (1 << 30)) {
    return -1;
  }

  R_limit = MAX_RES / entry_words;
  if ((unsigned) limit < (unsigned) R_limit) {
    R_limit = limit;
  }

  if (!(mode & (2*SUBCATS-1))) {
    cat = -1;
    tot_size = M_tot_entries + L->o_tree->delta;
  } else {
    tot_size = metafile_get_sublist_size (cat);
  }

  if (!R_limit || offset >= tot_size) {
    return tot_size;
  }

  limit = tot_size - offset;
  if (limit > R_limit) { 
     limit = R_limit;
  }

  if (mode & 16) {
    R_end = R + limit * entry_words;
    offset = tot_size - offset - limit;
  } else {
    R_end = R;
  }

  listree_t LI, *LT = &LI;

  switch (mode & 48) {
  case 0:
    if (cat < 0) {
      in_array = barray_out_node;
      in_tree = btree_out_node;
      LT = (listree_t *) &OTree;
    } else {
      in_array = carray_out_node;
      in_tree = ctree_out_node;
      load_o_tree_sub (LT, cat);
    }
    break;
  case 16:
    if (cat < 0) {
      in_array = barray_out_node_rev;
      in_tree = btree_out_node_rev;
      LT = (listree_t *) &OTree;
    } else {
      in_array = carray_out_node_rev;
      in_tree = ctree_out_node_rev;
      load_o_tree_sub (LT, cat);
    }
    break;
  case 32:
    in_array = darray_out_node;
    in_tree = dtree_out_node;
    if (cat < 0) {
      load_g_tree ((listree_xglobal_t *)LT);
    } else {
      load_g_tree_sub ((listree_xglobal_t *)LT, cat);
    }
    break;
  case 48:
    in_array = darray_out_node_rev;
    in_tree = dtree_out_node_rev;
    if (cat < 0) {
      load_g_tree ((listree_xglobal_t *)LT);
    } else {
      load_g_tree_sub ((listree_xglobal_t *)LT, cat);
    }
    break;
  }

  listree_get_range (LT, offset, limit);

  vkprintf (2, "R: %p, R_end: %p, limit: %d, entry_words: %d\n", R, R_end, limit, entry_words);

  if (mode & 16) {
    assert (R_end == R);
    R_end = R + limit * entry_words;
  } else {
    assert (R_end == R + limit * entry_words);
  }

  return tot_size;
}

int get_entry_sublist_position (list_id_t list_id, object_id_t object_id, int mode) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  list_t *L = __get_list_f (list_id, 2);
  if (!L) {
    return -1;
  }
  unpack_metafile_pointers (L);
  int cat = mode & (SUBCATS-1);
  if (!(mode & (2*SUBCATS-1))) {
    cat = -1;
  }
  listree_t LI, *LT = &LI;

  if (cat < 0) {
    return listree_get_pos_large (&OTree, object_id, 1);
  } else {
    load_o_tree_sub (LT, cat);
    return listree_get_pos_inderect (LT, object_id, 1);
  }
  //return listree_get_pos_inderect (LT, object_id, 1);
}


static int account_min_date, account_max_date, account_date_step, account_date_buckets;

static inline int account_date (int date) {
  if (date < account_min_date) {
    R[0]++;
  } else if (date < account_max_date) {
    R[(date - account_min_date) / account_date_step + 1]++;
  } else {
    R[account_date_buckets + 1]++;
  }
  return 1;
}

static inline int barray_account_date (listree_t *LT, int temp_id) {
  return account_date (metafile_get_date (temp_id));
}

static inline int btree_account_date (tree_ext_small_t *T) {
  struct tree_payload *P = LPAYLOAD (T);
  return account_date (P->date);
}


static inline int b_account_date (object_id_t object_id) {
  int temp_id = -1;
  tree_ext_large_t *T = listree_lookup_large (&OTree, object_id, &temp_id);
  if (T != (void *) -1) {
    return btree_account_date (SMALL_NODE(T));
  } else {
    return barray_account_date (0, temp_id);
  }
}

static inline int carray_account_date (listree_t *LI, int temp_id) {
  return b_account_date (OARR_ENTRY (LI->DA, LI->IA[temp_id]));
}

static inline int ctree_account_date (tree_ext_small_t *T) {
  return b_account_date (T->x);
}

int prepare_list_date_distr (list_id_t list_id, int mode, int min_date, int max_date, int step) {
  if (min_date <= 0 || max_date <= min_date || step <= 0 || (unsigned) mode >= 16) {
    return -1;
  }
  int rem = (max_date - min_date) % step;
  int buckets = (max_date - min_date) / step;
  if (rem || buckets > MAX_RES - 2) {
    return -1;
  }
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }

  R_end = R;

  list_t *L = __get_list_f (list_id, 2);

  if (!L) {
    return conv_list_id (list_id) < 0 ? -1 : 0;
  }

  unpack_metafile_pointers (L);

  memset (R, 0, (buckets + 2) * 4);

  listree_t LI, *LT = &LI;

  if (!mode) {
    in_array = barray_account_date;
    in_tree = btree_account_date;
    LT = (listree_t *) &OTree;
  } else {
    in_array = carray_account_date;
    in_tree = ctree_account_date;
    load_o_tree_sub (LT, mode & 7);
  }
  account_min_date = min_date;
  account_max_date = max_date;
  account_date_step = step;
  account_date_buckets = buckets;
  
  listree_get_range (LT, 0, MAXINT);

  R_end = R + buckets + 2;
  
  return buckets + 2;
}

/* --- get (sub)list sorted by value --- */

static int __vsort_xor_mask, __vsort_and_mask, __vsort_limit, __vsort_scanned;
static int HN;

struct heap_entry {
  value_t value;
  global_id_t global_id;
  union {
    int temp_id;   // contains true temp_id*2+1
    tree_ext_large_t *node;
  };
};

static struct heap_entry H[VSORT_HEAP_SIZE];

int cmp_value_asc_global_desc (struct heap_entry *p, value_t value, global_id_t global_id) {
  if (p->value < value) {
    return -1;
  } else if (p->value > value) {
    return 1;
  } else {
    return global_id - p->global_id;
  }
}

int cmp_value_asc_global_asc (struct heap_entry *p, value_t value, global_id_t global_id) {
  if (p->value < value) {
    return -1;
  } else if (p->value > value) {
    return 1;
  } else {
    return p->global_id - global_id;
  }
}

int cmp_value_desc_global_desc (struct heap_entry *p, value_t value, global_id_t global_id) {
  if (p->value > value) {
    return -1;
  } else if (p->value < value) {
    return 1;
  } else {
    return global_id - p->global_id;
  }
}

int cmp_value_desc_global_asc (struct heap_entry *p, value_t value, global_id_t global_id) {
  if (p->value > value) {
    return -1;
  } else if (p->value < value) {
    return 1;
  } else {
    return p->global_id - global_id;
  }
}

typedef int (*heap_cmp_function_t)(struct heap_entry *, value_t, global_id_t);

heap_cmp_function_t heap_cmp, heap_cmp_functions[4] = {
  cmp_value_desc_global_desc,
  cmp_value_desc_global_asc,
  cmp_value_asc_global_desc,
  cmp_value_asc_global_asc,
};

static inline int heap_sift_down (value_t value, global_id_t  global_id) { 
  int i = 1, j;
  while (1) {
    j = i*2;
    if (j > HN) {
      break;
    }
    if (j < HN && heap_cmp (&H[j+1], H[j].value, H[j].global_id) < 0) {
      j++;
    }
    if (heap_cmp (&H[j], value, global_id) >= 0) {
      break;
    }
    H[i] = H[j];
    i = j;
  }
  return i;
}

static inline int heap_sift_up (value_t value, global_id_t global_id) {
  int i = ++HN, j;
  while (i > 1) {
    j = (i >> 1);
    if (heap_cmp (&H[j], value, global_id) <= 0) {
      break;
    }
    H[i] = H[j];
    i = j;
  }
  return i;
}

struct heap_entry *heap_insert (value_t value, global_id_t global_id) {
  int i;
  if (HN == __vsort_limit) {
    if (heap_cmp (&H[1], value, global_id) >= 0) {
      return 0;
    }
    i = heap_sift_down (value, global_id);
  } else {
    i = heap_sift_up (value, global_id);
  }
  H[i].value = value;
  H[i].global_id = global_id;
  return &H[i];
}

static inline void heap_pop (void) {
  assert (HN > 0);
  if (--HN) {
    int i = heap_sift_down (H[HN+1].value, H[HN+1].global_id);
    H[i] = H[HN+1];
  }
}
  
static int barray_scan_node (listree_t *LT, int temp_id) {
  int flags = metafile_get_flags (temp_id);
  if (((flags ^ __vsort_xor_mask) & __vsort_and_mask) != 0) {
    return 0;
  }
  global_id_t global_id = M_global_id_list[temp_id];
  value_t value = metafile_get_value (temp_id);
  struct heap_entry *h = heap_insert (value, global_id);
  if (h) {
    h->temp_id = temp_id * 2 + 1;
  }
  __vsort_scanned++;
  return 1;
}

static int btree_scan_node (tree_ext_small_t *T) {
  struct tree_payload *P = LPAYLOAD (T);
  if (((P->flags ^ __vsort_xor_mask) & __vsort_and_mask) != 0) {
    return 0;
  }
  struct heap_entry *h = heap_insert (P->value, P->global_id);
  if (h) {
    h->node = LARGE_NODE(T);
  }
  __vsort_scanned++;
  return 1;
}


int prepare_value_sorted_list (list_id_t list_id, int xor_mask, int and_mask, int mode, int limit) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  int entry_words = OBJECT_ID_INTS;
  int tot_size;
  list_t *L = __get_list_f (list_id, 2);

  R_mode = mode;
  R_end = R;

  vkprintf (2, "prepare_value_sorted_list(" idout ", %d, %d, %d, %d) : L=%p, metafile_index=%d\n", out_list_id (list_id), mode, xor_mask, and_mask, limit, L, L ? L->metafile_index : -1);

  if (!L) {
    R_entry_size = 1;
    return conv_list_id (list_id) < 0 ? -1 : 0;
  }

  unpack_metafile_pointers (L);

  value_offset = -1;

  if (mode & 64) { entry_words++; }
  if (mode & 128) { entry_words++; }
  if (mode & 256) { entry_words++; }
  if (mode & 512) { 
    value_offset = entry_words;
    entry_words += VALUE_INTS; 
  }
  if (mode & 1024) { entry_words += PTR_INTS + 1; }

  R_entry_size = entry_words;

  R_limit = MAX_RES / entry_words;
  if ((unsigned) limit < (unsigned) R_limit) {
    R_limit = limit;
  }
  if (R_limit >= VSORT_HEAP_SIZE) {
    R_limit = VSORT_HEAP_SIZE - 1;
  }

  tot_size = M_tot_entries + L->o_tree->delta;
  
  if (!tot_size || !limit) {
    return 0;
  }

  limit = tot_size;
  if (limit > R_limit) { 
     limit = R_limit;
  }

  __vsort_and_mask = and_mask;
  __vsort_xor_mask = xor_mask;
  __vsort_limit = limit;
  __vsort_scanned = 0;
  HN = 0;

  heap_cmp = heap_cmp_functions[mode & 3];

  listree_t LI, *LT = &LI;

  //  if (cat < 0) {
    // traverse all tree
  in_array = barray_scan_node;
  in_tree = btree_scan_node;
  LT = (listree_t *) &OTree;
  
/*  {
    // traverse (some) sublists
    in_array = carray_out_node;
    in_tree = ctree_out_node;
    load_o_tree_sub (LT, cat);
    } 
*/

  listree_get_range (LT, 0, MAXINT);

  assert (HN <= limit && HN <= __vsort_scanned);
  limit = HN;
  R_end = R + limit * entry_words;
  assert (R_end >= R && R_end <= R + MAX_RES);

  vkprintf (2, "R: %p, R_end: %p, limit: %d, scanned: %d, entry_words: %d\n", R, R_end, limit, __vsort_scanned, entry_words);

  /* fetch everything from heap to R_end in reverse order */
  while (HN > 0) {
    if (H[1].temp_id & 1) {
      barray_out_node_rev (0, H[1].temp_id >> 1);
    } else {
      btree_out_node_rev (SMALL_NODE (H[1].node));
    }
    heap_pop ();
  }

  assert (R_end == R);
  R_end = R + limit * entry_words;

  return __vsort_scanned;
}

static int __have_weights, object_id_ints_adjusted, object_id_bytes_adjusted;

static void isort (array_object_id_t *A, int b) {
  int i, j;
  var_object_id_t h, t;
  if (b <= 0) { return; }
  i = 0;  j = b;  
  #ifdef LISTS_Z
  memcpy (h, OARR_ENTRY_ADJ (A, b >> 1), object_id_bytes_adjusted);
  #else
  h = OARR_ENTRY_ADJ (A, b >> 1);
  #endif
  do {
    while (object_id_less (OARR_ENTRY_ADJ (A, i), h)) { i++; }
    while (object_id_less (h, OARR_ENTRY_ADJ (A, j))) { j--; }
    if (i <= j) {
      #ifdef LISTS_Z
      memcpy (t, OARR_ENTRY_ADJ (A, i), object_id_bytes_adjusted);
      memcpy (OARR_ENTRY_ADJ (A, i), OARR_ENTRY_ADJ (A, j), object_id_bytes_adjusted);
      memcpy (OARR_ENTRY_ADJ (A, j), t, object_id_bytes_adjusted);
      #else
      if (__have_weights) {
        t = A[2*i];  A[2*i] = A[2*j];  A[2*j] = t;
        t = A[2*i+1];  A[2*i+1] = A[2*j+1];  A[2*j] = t;
      } else {
        t = A[i];  A[i] = A[j];  A[j] = t;
      }
      #endif
      i++; 
      j--;
    }
  } while (i <= j);
  isort (A, j);
  #ifdef LISTS_Z
  isort (A+i*object_id_ints_adjusted, b-i);
  #else
  isort (A+i, b-i);
  #endif
}

static void isort_prefix (array_object_id_t *A, int b) {
  int i, j;
  var_object_id_t h, t;
  if (b <= 0) { return; }
  i = 0;  j = b;  
  #ifdef LISTS_Z
  memcpy (h, OARR_ENTRY_ADJ (A, b >> 1), object_id_bytes_adjusted);
  #else
  h = OARR_ENTRY_ADJ (A, b >> 1);
  #endif
  do {
    while (object_id_less_prefix (OARR_ENTRY_ADJ (A, i), h)) { i++; }
    while (object_id_less_prefix (h, OARR_ENTRY_ADJ (A, j))) { j--; }
    if (i <= j) {
      #ifdef LISTS_Z
      memcpy (t, OARR_ENTRY_ADJ (A, i), object_id_bytes_adjusted);
      memcpy (OARR_ENTRY_ADJ (A, i), OARR_ENTRY_ADJ (A, j), object_id_bytes_adjusted);
      memcpy (OARR_ENTRY_ADJ (A, j), t, object_id_bytes_adjusted);
      #else
      if (__have_weights) {
        t = A[2*i];  A[2*i] = A[2*j];  A[2*j] = t;
        t = A[2*i+1];  A[2*i+1] = A[2*j+1];  A[2*j] = t;
      } else {
        t = A[i];  A[i] = A[j];  A[j] = t;
      }
      #endif
      i++; 
      j--;
    }
  } while (i <= j);
  isort_prefix (A, j);
  #ifdef LISTS_Z
  isort_prefix (A+i*object_id_ints_adjusted, b-i);
  #else
  isort_prefix (A+i, b-i);
  #endif
}

/* ----- construct list intersections ----- */

static array_object_id_t *LA;

#ifdef LISTS_Z
static array_object_id_t *LE;
#endif

//#define MAX_INT 0x7fffffff


int (*report_x)(object_id_t x);
typedef object_id_t (*report_array_contents)(listree_t *LT, int index);

report_array_contents get_array;
void (*store_object_id_intersect_v)(object_id_t x);

static long long Sum;


static inline void store_to_rend_overlap (object_id_t x) {
  store_object_id_overlap (&R_end, x);
}


static inline int list_sum_x (object_id_t x) {
  int p;
  while (IF_LISTS_Z(LA < LE) && (p = object_id_compare (OARR_ENTRY (LA, 0), x)) < 0) {
    LA += object_id_ints_adjusted;
  }

  #ifdef LISTS_Z
  if (LA >= LE) {
    return 0;
  }
  #else
  if (*LA == MAX_OBJECT_ID) {
    return 0;
  }
  #endif
  if (!p) {
    LA += object_id_ints_adjusted;
    Sum += __have_weights ? LA[-1] : 1;
  }
  return 0;
}

static inline int list_intersect_x (object_id_t x) {
  int p;
  while (IF_LISTS_Z(LA < LE) && (p = object_id_compare (OARR_ENTRY (LA, 0), x)) < 0) {
    LA += object_id_ints_adjusted;
  }

  #ifdef LISTS_Z
  if (LA >= LE) {
    return 0;
  }
  #else
  if (*LA == MAX_OBJECT_ID) {
    return 0;
  }
  #endif
  if (!p) {
    store_object_id_intersect_v (OARR_ENTRY (LA, 0));
    LA += object_id_ints_adjusted;
  }
  return 0;
}


static inline int list_subtract_x (object_id_t x) {
  int p;
  while (IF_LISTS_Z(LA < LE) && (p = object_id_compare (OARR_ENTRY (LA, 0), x)) < 0) {
    store_object_id_intersect_v (OARR_ENTRY (LA, 0));
    LA += object_id_ints_adjusted;
  }
  #ifdef LISTS_Z
  if (LA >= LE) {
    return 0;
  }
  #else
  if (*LA == MAX_OBJECT_ID) {
    return 0;
  }
  #endif
  if (!p) {
    LA += object_id_ints_adjusted;
  }
  return 0;
}


static inline int list_sum_x_prefix (object_id_t x) {
  int p;
  while (IF_LISTS_Z(LA < LE) && (p = object_id_compare_prefix (OARR_ENTRY (LA, 0), x)) < 0) {
    LA += object_id_ints_adjusted;
  }

  #ifdef LISTS_Z
  if (LA >= LE) {
    return 0;
  }
  #else
  if (*LA == MAX_OBJECT_ID) {
    return 0;
  }
  #endif
  if (!p) {
    //LA += object_id_ints_adjusted;
    Sum += __have_weights ? LA[object_id_ints_adjusted-1] : 1;
  }
  return 0;
}

static inline int list_intersect_x_prefix (object_id_t x) {
  int p;
  while (IF_LISTS_Z(LA < LE) && (p = object_id_compare_prefix (OARR_ENTRY (LA, 0), x)) < 0) {
    LA += object_id_ints_adjusted;
  }

  #ifdef LISTS_Z
  if (LA >= LE) {
    return 0;
  }
  #else
  if (*LA == MAX_OBJECT_ID) {
    return 0;
  }
  #endif
  if (!p) {
    store_object_id_intersect_v (x);
    //LA += object_id_ints_adjusted;
  }
  return 0;
}


/*static inline int list_subtract_x_prefix (object_id_t x) {
  int p;
  while (IF_LISTS_Z(LA < LE) && (p = object_id_compare_prefix (OARR_ENTRY (LA, 0), x)) < 0) {
    store_object_id_intersect_v (x);
    LA += object_id_ints_adjusted;
  }
  #ifdef LISTS_Z
  if (LA >= LE) {
    return 0;
  }
  #else
  if (*LA == MAX_OBJECT_ID) {
    return 0;
  }
  #endif
  if (!p) {
    //LA += object_id_ints_adjusted;
  }
  return 0;
}*/

static void listree_intersect_array_rec (listree_t *LT, int a, int b, object_id_t upper_bound) {
  if (a > b) {
    return;
  }
  int m = (a + b) >> 1;
  object_id_t x = get_array (LT, m);
  if (object_id_less (OARR_ENTRY (LA, 0), x)) {
    listree_intersect_array_rec (LT, a, m - 1, x);
  }
  report_x (x);
  if (object_id_less (OARR_ENTRY (LA, 0), upper_bound)) {
    listree_intersect_array_rec (LT, m + 1, b, upper_bound);
  }
}


static void listree_intersect_range_rec (tree_ext_small_t *T, listree_t *LT, int a, int b, object_id_t upper_bound) {
  if (T == NIL) {
    listree_intersect_array_rec (LT, a, b, upper_bound);
    return;
  }

  int N = LT->N; 
  int c = N - NODE_RPOS(T);		// # of array elements < T->x, i.e. A[0]...A[c-1]
  int lr = c - 1;			// left subtree corresponds to [a .. c-1]
  int rl = c + (NODE_TYPE(T) != TF_PLUS);	// right subtree corresponds to [rl .. b]

  assert (rl <= b + 1);

  if (object_id_less (OARR_ENTRY (LA, 0), T->x)) {
    listree_intersect_range_rec (T->left, LT, a, lr, T->x);
  }

  if (NODE_TYPE(T) != TF_MINUS) {
    report_x (T->x);
  }

  if (object_id_less (OARR_ENTRY (LA, 0), upper_bound)) {
    listree_intersect_range_rec (T->right, LT, rl, b, upper_bound);
  }
}


static void listree_intersect_array_rec_intervals (listree_t *LT, int a, int b, object_id_t upper_bound) {
  if (a > b) {
    return;
  }
  int m = (a + b) >> 1;
  object_id_t x = get_array (LT, m);
  if (object_id_compare_prefix (OARR_ENTRY (LA, 0), x) <= 0) {
    listree_intersect_array_rec_intervals (LT, a, m - 1, x);
  }
  report_x (x);
  if (object_id_compare_prefix (OARR_ENTRY (LA, 0), upper_bound) <= 0) {
    listree_intersect_array_rec_intervals (LT, m + 1, b, upper_bound);
  }
}


static void listree_intersect_range_rec_intervals (tree_ext_small_t *T, listree_t *LT, int a, int b, object_id_t upper_bound) {
  if (T == NIL) {
    listree_intersect_array_rec_intervals (LT, a, b, upper_bound);
    return;
  }

  int N = LT->N; 
  int c = N - NODE_RPOS(T);		// # of array elements < T->x, i.e. A[0]...A[c-1]
  int lr = c - 1;			// left subtree corresponds to [a .. c-1]
  int rl = c + (NODE_TYPE(T) != TF_PLUS);	// right subtree corresponds to [rl .. b]

  assert (rl <= b + 1);

  if (object_id_compare_prefix (OARR_ENTRY (LA, 0), T->x) <= 0) {
    listree_intersect_range_rec_intervals (T->left, LT, a, lr, T->x);
  }

  if (NODE_TYPE(T) != TF_MINUS) {
    report_x (T->x);
  }

  if (object_id_compare_prefix (OARR_ENTRY (LA, 0), upper_bound) <= 0) {
    listree_intersect_range_rec_intervals (T->right, LT, rl, b, upper_bound);  
  }
}

int __prepare_list_intersection (list_id_t list_id, int mode, array_object_id_t List[], int LL, int flag, int have_weights) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  list_t *L = __get_list_f (list_id, 2);
  int i, j;

  R_end = R;
  __have_weights = have_weights;
  object_id_ints_adjusted = have_weights + OBJECT_ID_INTS;
  object_id_bytes_adjusted = object_id_ints_adjusted * 4;

  if (mode & 1984) {
    store_object_id_intersect_v = b_out_void;
    if (flag) {
      return -1;
    }
  } else {
    store_object_id_intersect_v = store_to_rend_overlap;
  }
  R_mode = mode;

  value_offset = -1;

  int entry_words = OBJECT_ID_INTS;
  if (mode & 64) { entry_words++; }
  if (mode & 128) { entry_words++; }
  if (mode & 256) { entry_words++; }
  if (mode & 512) { 
    value_offset = entry_words;
    entry_words += VALUE_INTS; 
  }
  if (mode & 1024) { entry_words += PTR_INTS + 1; }

  R_entry_size = entry_words;

  if ((unsigned) (mode & 63) > SUBCATS || LL < 0 || LL > MAX_OBJECT_RES) {
    return -1;
  }

  if (!LL) {
    return 0;
  }

  if (!L) {
    return conv_list_id (list_id) < 0 ? -1 : 0;
  }

  unpack_metafile_pointers (L);
  
  listree_t *LT, LI;

  //in_tree = list_tree_intersect;
  if ((mode & 63) == 0) {
    LT = (listree_t *) &OTree;
    get_array = get_data_direct_compatible;
  } else {
    load_o_tree_sub (LT = &LI, mode & (SUBCATS - 1));
    get_array = get_data_indirect;
  }

  for (i = 1; i < LL; i++) {
    if (object_id_compare (OARR_ENTRY_ADJ (List, i), OARR_ENTRY_ADJ (List, i-1)) <= 0) {
      break;
    }
  }

  if (i < LL) {
    isort (List, LL-1);
  }
    
  /*i = j = 0;
  while (i < LL && List[i] <= 0) {
    i++;
  }
  if (i == LL) {
    return 0;
  }


  List[j++] = List[i++];*/
  i = j = 1;
  while (i < LL) {
    if (object_id_compare (OARR_ENTRY_ADJ (List, i), OARR_ENTRY_ADJ (List, i-1)) > 0) {
      #ifdef LISTS_Z
      memcpy (OARR_ENTRY_ADJ (List, j), OARR_ENTRY_ADJ (List, i), object_id_bytes_adjusted);
      #else
      if (have_weights) {
        List[2*j] = List[2*i];
        List[2*j+1] = List[2*i+1];
      } else {
        List[j] = List[i];
      }
      #endif
      j++;
    }
    i++;
  }
  #ifdef LISTS_Z
  LE = OARR_ENTRY_ADJ (List, j);
  #else
  List[j*object_id_ints_adjusted] = MAX_OBJECT_ID; 
  #endif
  LA = List;
  
  listree_intersect_range_rec (*LT->root, LT, 0, LT->N - 1, MAX_OBJECT_ID);

  if (flag) {
    report_x (MAX_OBJECT_ID);
  }

  assert (!((R_end - R) % R_entry_size));

  return (R_end - R) / R_entry_size;
}

int __prepare_list_intersection_intervals (list_id_t list_id, int mode, array_object_id_t List[], int LL, int flag, int have_weights) {
  if (metafile_mode && prepare_list_metafile (list_id, 1) < 0) {
    return -2;
  }
  list_t *L = __get_list_f (list_id, 2);
  int i, j;

  R_end = R;
  __have_weights = have_weights;
  object_id_ints_adjusted = have_weights + id_ints;
  object_id_bytes_adjusted = object_id_ints_adjusted * 4;

  if (mode & 1984) {
    store_object_id_intersect_v = b_out_void;
    if (flag) {
      return -1;
    }
  } else {
    store_object_id_intersect_v = store_to_rend_overlap;
  }
  R_mode = mode;

  value_offset = -1;

  int entry_words = OBJECT_ID_INTS;
  if (mode & 64) { entry_words++; }
  if (mode & 128) { entry_words++; }
  if (mode & 256) { entry_words++; }
  if (mode & 512) { 
    value_offset = entry_words;
    entry_words += VALUE_INTS; 
  }
  if (mode & 1024) { entry_words += PTR_INTS + 1; }

  R_entry_size = entry_words;

  if ((unsigned) (mode & 63) > SUBCATS || LL < 0 || LL > MAX_OBJECT_RES) {
    return -1;
  }

  if (!LL) {
    return 0;
  }

  if (!L) {
    return conv_list_id (list_id) < 0 ? -1 : 0;
  }

  unpack_metafile_pointers (L);
  
  listree_t *LT, LI;

  //in_tree = list_tree_intersect;
  if ((mode & 63) == 0) {
    LT = (listree_t *) &OTree;
    get_array = get_data_direct_compatible;
  } else {
    load_o_tree_sub (LT = &LI, mode & (SUBCATS - 1));
    get_array = get_data_indirect;
  }

  for (i = 1; i < LL; i++) {
    if (object_id_compare_prefix (OARR_ENTRY_ADJ (List, i), OARR_ENTRY_ADJ (List, i-1)) <= 0) {
      break;
    }
  }

  if (i < LL) {
    isort_prefix (List, LL-1);
  }
    
  /*i = j = 0;
  while (i < LL && List[i] <= 0) {
    i++;
  }
  if (i == LL) {
    return 0;
  }


  List[j++] = List[i++];*/
  i = j = 1;
  while (i < LL) {
    if (object_id_compare_prefix (OARR_ENTRY_ADJ (List, i), OARR_ENTRY_ADJ (List, i-1)) > 0) {
      #ifdef LISTS_Z
      memcpy (OARR_ENTRY_ADJ (List, j), OARR_ENTRY_ADJ (List, i), object_id_bytes_adjusted);
      #else
      if (have_weights) {
        List[2*j] = List[2*i];
        List[2*j+1] = List[2*i+1];
      } else {
        List[j] = List[i];
      }
      #endif
      j++;
    }
    i++;
  }
  #ifdef LISTS_Z
  LE = OARR_ENTRY_ADJ (List, j);
  #else
  List[j*object_id_ints_adjusted] = MAX_OBJECT_ID; 
  #endif
  LA = List;
  
  listree_intersect_range_rec_intervals (*LT->root, LT, 0, LT->N - 1, MAX_OBJECT_ID);

  if (flag) {
    report_x (MAX_OBJECT_ID);
  }

  assert (!((R_end - R) % R_entry_size));

  return (R_end - R) / R_entry_size;
}


int prepare_list_intersection (list_id_t list_id, int mode, array_object_id_t List[], int LL, int have_weights, int _id_ints) {
  id_ints = _id_ints;
  if (id_ints == object_id_ints) {
    report_x = list_intersect_x;
    return __prepare_list_intersection (list_id, mode, List, LL, 0, have_weights);
  } else {
    report_x = list_intersect_x_prefix;
    return __prepare_list_intersection_intervals (list_id, mode, List, LL, 0, have_weights);
  }
}


int prepare_list_subtraction (list_id_t list_id, int mode, array_object_id_t List[], int LL, int have_weights, int _id_ints) {
  id_ints = _id_ints;
  if (id_ints == object_id_ints) {
    report_x = list_subtract_x;
    return __prepare_list_intersection (list_id, mode, List, LL, 1, have_weights);
  } else {
    return 0;
    /*report_x = list_subtract_x_prefix;
    return __prepare_list_intersection_intervals (list_id, mode, List, LL, 1, have_weights);*/
  }
}

long long prepare_list_sum (list_id_t list_id, int mode, array_object_id_t List[], int LL, int have_weights, int _id_ints) {
  id_ints = _id_ints;
  if (id_ints == object_id_ints) {
    report_x = list_sum_x;
    Sum = 0;
    int res = __prepare_list_intersection (list_id, mode, List, LL, 0, have_weights);
    return res < 0 ? res : Sum;
  } else {
    report_x = list_sum_x_prefix;
    Sum = 0;
    int res = __prepare_list_intersection_intervals (list_id, mode, List, LL, 0, have_weights);
    return res < 0 ? res : Sum;
  }
}


/*
 *
 * GENERIC BUFFERED WRITE
 *
 */

#define	BUFFSIZE	16777216

char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff, *wptr0 = Buff;
long long write_pos;
int metafile_pos;
int newidx_fd = -1;
kfs_snapshot_write_stream_t SWS;

unsigned metafile_crc32;

static inline long long get_write_pos (void) {
  int s = wptr - wptr0;
  wptr0 = wptr;
  metafile_pos += s;
  return write_pos += s;
}

static inline int get_metafile_pos (void) {
  int s = wptr - wptr0;
  wptr0 = wptr;
  write_pos += s;
  return metafile_pos += s;
}

static void clear_metafile_crc32 (void) {
  metafile_crc32 = 0;
}

void flushout (void) {
  int s;
  if (wptr > wptr0) {
    s = wptr - wptr0;
    write_pos += s;
    metafile_pos += s;
  }
  if (rptr < wptr) {
    s = wptr - rptr;
    if (newidx_fd >= 1) {
      assert (write (newidx_fd, rptr, s) == s);
    } else {
      kfs_sws_write (&SWS, rptr, s);
    }
  }
  rptr = wptr = wptr0 = Buff;
}

void clearin (void) {
  rptr = wptr = wptr0 = Buff + BUFFSIZE;
}

static inline void writeout (const void *D, size_t len) {
  const char *d = D;
  metafile_crc32 = ~crc32_partial (d, len, ~metafile_crc32);
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) { 
      memcpy (wptr, d, len);
      wptr += len;
      return;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout();
    }
  }                                
}

#define likely(x) __builtin_expect((x),1) 
#define unlikely(x) __builtin_expect((x),0)

static inline void writeout_long (long long value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 8)) {
    flushout();
  }
  *((long long *) wptr) = value;
  metafile_crc32 = ~crc32_partial (&value, 8, ~metafile_crc32);
  wptr += 8;
}

static inline void writeout_int (int value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 4)) {
    flushout();
  }
  *((int *) wptr) = value;
  metafile_crc32 = ~crc32_partial (&value, 4, ~metafile_crc32);
  wptr += 4;
}

static inline void writeout_char (char value) {
  if (unlikely (wptr == Buff + BUFFSIZE)) {
    flushout();
  }
  metafile_crc32 = ~crc32_partial (&value, 1, ~metafile_crc32);
  *wptr++ = value;
}


#ifdef LISTS_Z
static inline void writeout_object_id (object_id_t object_id) {
  writeout (object_id, object_id_bytes);
}

static inline void writeout_list_id (list_id_t list_id) {
  writeout (list_id, list_id_bytes);
}
#elif defined(LISTS64)
# define writeout_object_id	writeout_long
# define writeout_list_id	writeout_long
#else
# define writeout_object_id	writeout_int
# define writeout_list_id	writeout_int
#endif

void write_seek (long long new_pos) {
  flushout();
  assert (lseek (SWS.newidx_fd, new_pos, SEEK_SET) == new_pos);
  write_pos = new_pos;
}

#ifdef VALUES64
#define writeout_value writeout_long
#else
#define writeout_value writeout_int
#endif


/*
 *
 *	WRITE NEW INDEX
 *
 */

static list_t **LArr;

struct lists_index_header NewHeader;
struct file_list_index_entry *NewFileLists;
char *NewFileLists_adjusted;
static metafile_t *new_metafile;
int new_flags;
int metafiles_output;
long long new_total_entries;
struct list *cur_list;

int report_one (list_t *L) {
  if (metafile_mode) {
    if (L == &DummyList || L->o_tree->delta == 0) {
      return 1;
    } else {
      prepare_list_metafile_num (L->metafile_index, -1);
      unpack_metafile_pointers (L);

      return (M_tot_entries + L->o_tree->delta) > 0;
    }
  } else {
    return 1;
  }
}

#define	LF_BIGFLAGS	1
#define	LF_HAVE_VALUES	2
#define	LF_HAVE_DATES	4
#define	LF_HAVE_TEXTS	8

static int array_build_flags (listree_t *LT, int temp_id) {
  int fl = metafile_get_flags (temp_id);
  if ((unsigned)fl >= 256) {
    new_flags |= LF_BIGFLAGS;
  }
  if (metafile_get_value (temp_id)) {
    new_flags |= LF_HAVE_VALUES;
  }
  if (metafile_get_date (temp_id)) {
    new_flags |= LF_HAVE_DATES;
  }
  int text_len;
  metafile_get_text (temp_id, &text_len);
  if (text_len) {
    new_flags |= LF_HAVE_TEXTS;
  }
  return 0;
}


static int tree_build_flags (tree_ext_small_t *T) {
  struct tree_payload *P = LPAYLOAD (T);
  if ((unsigned)P->flags >= 256) {
    new_flags |= LF_BIGFLAGS;
  }

  if (P->value) {
    new_flags |= LF_HAVE_VALUES;
  }

  if (P->date) {
    new_flags |= LF_HAVE_DATES;
  }

  int text_len = -1;
  if (P->text) {
    text_len = get_text_len (P->text);
  } else {
    metafile_get_text (OTree.N - NODE_RPOS (T), &text_len);
  }
  assert (text_len >= 0);

  if (text_len) {
    new_flags |= LF_HAVE_TEXTS;
  }
  return 0;
}

static int array_write_object_id (listree_t *LT, int temp_id) {
  writeout_object_id (OARR_ENTRY (M_obj_id_list, temp_id));
  vkprintf (4, "lo: " idout " " idout "\n", out_list_id (cur_list->list_id), out_object_id (OARR_ENTRY (M_obj_id_list, temp_id)));
  ++new_total_entries;
  return 0;
}

static int tree_write_object_id (tree_ext_small_t *T) {
  writeout_object_id (T->x);
  vkprintf (4, "lo: " idout " " idout "\n", out_list_id (cur_list->list_id), out_object_id (T->x));
  ++new_total_entries;
  return 0;
}

static int array_write_global_id (listree_t *LT, int temp_id) {
  if (sizeof (global_id_t) == sizeof (int)) {
    writeout_int (M_global_id_list[temp_id]);
  } else if (sizeof (global_id_t) == sizeof (long long)) {
    writeout_long (M_global_id_list[temp_id]);
  } else {
    assert (0);
  }
  return 0;
}

static int tree_write_global_id (tree_ext_small_t *T) {
  if (sizeof (global_id_t) == sizeof (int)) {
    writeout_int (LPAYLOAD(T)->global_id);
  } else if (sizeof (global_id_t) == sizeof (long long)) {
    writeout_long (LPAYLOAD(T)->global_id);
  } else {
    assert (0);
  }
  return 0;
}

static int write_temp_id_by_object_id (object_id_t object_id) {
  writeout_int (listree_get_pos_large (&OTree, object_id, 0));
  return 0;
}

static int array_write_temp_id_by_other (listree_t *LT, int temp_id) {
  object_id_t object_id = OARR_ENTRY (M_obj_id_list, LT->IA[temp_id]);
  return write_temp_id_by_object_id (object_id);
}

static int tree_write_temp_id_by_global_id (tree_ext_small_t *T) {
  #ifdef LISTS_GT
  object_id_t object_id = GLOBAL_NODE(T)->z;
  #else
  object_id_t object_id = T->y * Y_MULT_INV;
  #endif
  return write_temp_id_by_object_id (object_id);
}

static int tree_write_temp_id_by_object_id (tree_ext_small_t *T) {
  return write_temp_id_by_object_id (T->x);
}


static int array_write_value (listree_t *LT, int temp_id) {
  writeout_value (metafile_get_value (temp_id));
  return 0;
}

static int tree_write_value (tree_ext_small_t *T) {
  writeout_value (LPAYLOAD(T)->value);
  return 0;
}


static int array_write_date (listree_t *LT, int temp_id) {
  writeout_int (metafile_get_date (temp_id));
  return 0;
}

static int tree_write_date (tree_ext_small_t *T) {
  writeout_int (LPAYLOAD(T)->date);
  return 0;
}


static long long new_text_cur_offset;


static int array_write_text_offset (listree_t *LT, int temp_id) {
  writeout_int (new_text_cur_offset);
  int text_len;
  metafile_get_text (temp_id, &text_len);
  new_text_cur_offset += text_len;
  return 0;
}

static int tree_write_text_offset (tree_ext_small_t *T) {
  writeout_int (new_text_cur_offset);
  struct tree_payload *P = LPAYLOAD (T);
  int text_len;
  if (P->text) {
    text_len = get_text_len (P->text);
  } else {
    metafile_get_text (OTree.N - NODE_RPOS (T), &text_len);
  }
  new_text_cur_offset += text_len;
  return 0;
}


static int array_write_bigflags (listree_t *LT, int temp_id) {
  writeout_int (metafile_get_flags (temp_id));
  return 0;
}

static int tree_write_bigflags (tree_ext_small_t *T) {
  writeout_int (LPAYLOAD(T)->flags);
  return 0;
}



static int array_write_smallflags (listree_t *LT, int temp_id) {
  writeout_char (metafile_get_flags (temp_id));
  return 0;
}

static int tree_write_smallflags (tree_ext_small_t *T) {
  writeout_char (LPAYLOAD(T)->flags);
  return 0;
}



static int array_write_text (listree_t *LT, int temp_id) {
  int text_len;
  char *p = metafile_get_text (temp_id, &text_len);
  writeout (p, text_len);
  return 0;
}


static int tree_write_text (tree_ext_small_t *T) {
  struct tree_payload *P = LPAYLOAD (T);
  int text_len;
  if (P->text) {
    text_len = get_text_len (P->text);
    writeout (get_text_ptr (P->text), text_len);
  } else {
    char *p = metafile_get_text (OTree.N - NODE_RPOS (T), &text_len);
    writeout (p, text_len);
  }
  return 0;
}





int write_metafile (list_t *L) {
  cur_list = L;
  int i;

  listree_t OTreeSub;
  listree_xglobal_t GTree, GTreeSub;

  if (metafile_mode && L->metafile_index >= 0) {
    assert (prepare_list_metafile_num (L->metafile_index, -1) >= 0);
  }
  unpack_metafile_pointers (L);

  int list_entries = M_tot_entries + L->o_tree->delta;

  if (!list_entries) {
    return 0;
  }

  clear_metafile_crc32 ();

  static metafile_t new_metafile_static;
  new_metafile = MF_ADJUST (&new_metafile_static);

  assert (list_entries > 0);

  vkprintf (3, "writing metafile #%d for list " idout " at position %lld\n", metafiles_output, out_list_id (L->list_id), get_write_pos());

  MF_MAGIC (new_metafile) = FILE_LIST_MAGIC;
  COPY_LIST_ID (MF_LIST_ID (new_metafile), L->list_id);

  new_metafile->sublist_size_cum[0] = 0;

  for (i = 1; i < 9; i++) {
    new_metafile->sublist_size_cum[i] = metafile_get_sublist_size (i - 1) + new_metafile->sublist_size_cum[i - 1];
  }

  assert (new_metafile->sublist_size_cum[8] == list_entries);

  new_flags = 0;

  in_tree = tree_build_flags;
  in_array = array_build_flags;

  listree_get_range ((listree_t *) &OTree, 0, MAXINT);

  if (remove_dates) {
    new_flags &= ~LF_HAVE_DATES;
  }

  new_metafile->flags = new_flags;

  NEW_FLI_ENTRY_ADJUSTED(metafiles_output)->flags = new_flags;
  long long start_write_pos = get_write_pos ();
  NEW_FLI_ENTRY_ADJUSTED(metafiles_output)->list_file_offset = start_write_pos;
  COPY_LIST_ID (NEW_FLI_ENTRY_LIST_ID (metafiles_output), L->list_id);

  writeout (&MF_MAGIC (new_metafile), 
#ifdef LISTS_Z
    sizeof (struct file_list_header) - sizeof (alloc_list_id_t) + list_id_bytes
#else
    sizeof (struct file_list_header)
#endif
  );

  in_tree = tree_write_object_id;
  in_array = array_write_object_id;

  listree_get_range ((listree_t *) &OTree, 0, MAXINT);

  in_tree = tree_write_global_id;
  in_array = array_write_global_id;

  listree_get_range ((listree_t *) &OTree, 0, MAXINT);

  load_g_tree ((listree_xglobal_t *) &GTree);

  in_tree = tree_write_temp_id_by_global_id;
  in_array = array_write_temp_id_by_other;

  listree_get_range ((listree_t *) &GTree, 0, MAXINT);

  in_tree = tree_write_temp_id_by_object_id;

  for (i = 0; i < 8; i++) {
    load_o_tree_sub (&OTreeSub, i);
    listree_get_range (&OTreeSub, 0, MAXINT);
  }

  in_tree = tree_write_temp_id_by_global_id;

  for (i = 0; i < 8; i++) {
    load_g_tree_sub (&GTreeSub, i);
    listree_get_range ((listree_t *)&GTreeSub, 0, MAXINT);
  }

  if (new_flags & LF_HAVE_VALUES) {
    in_tree = tree_write_value;
    in_array = array_write_value;

    listree_get_range ((listree_t *) &OTree, 0, MAXINT);
  }

  if (new_flags & LF_HAVE_DATES) {
    in_tree = tree_write_date;
    in_array = array_write_date;

    listree_get_range ((listree_t *) &OTree, 0, MAXINT);
  }

  if (new_flags & LF_HAVE_TEXTS) {
    new_text_cur_offset = get_write_pos () + list_entries * ((new_flags & LF_BIGFLAGS) ? 8 : 5) + 4 - start_write_pos;

    in_tree = tree_write_text_offset;
    in_array = array_write_text_offset;

    listree_get_range ((listree_t *) &OTree, 0, MAXINT);

    assert (new_text_cur_offset <= (MAXINT - 4));

    writeout_int (new_text_cur_offset);
  }

  if (new_flags & LF_BIGFLAGS) {
    in_tree = tree_write_bigflags;
    in_array = array_write_bigflags;
  } else {
    in_tree = tree_write_smallflags;
    in_array = array_write_smallflags;
  }

  listree_get_range ((listree_t *) &OTree, 0, MAXINT);


  if (new_flags & LF_HAVE_TEXTS) {
    in_tree = tree_write_text;
    in_array = array_write_text;

    listree_get_range ((listree_t *) &OTree, 0, MAXINT);

    assert (get_write_pos () == start_write_pos + new_text_cur_offset);
  }

  writeout ("\0\0\0\0", -get_write_pos() & 3);

  //vkprintf (0, "metafile #%d: crc = %u\n", metafiles_output, metafile_crc32);
  writeout_int (metafile_crc32);

  metafiles_output++;

  return 1;
}

/**
 * Note: function creates {@code LArr}.
 *
 * @param LArr1 Array[0 .. idx_lists - 1] Represents lists in snapshots.
 * @param LArr2 Array[0 .. tot_lists - 1] Represents in-memory cached lists.
 */
int traverse_all_lists (list_t **LArr1, int len1, list_t **LArr2, int len2, int (*process_one_list)(list_t *L)) {
  LArr = zzmalloc (sizeof (void *) * (len1 + len2));
  list_t *L;
  int i = 0, j = 0, k = 0, r = 0, rt;
  vkprintf (1, "traverse_all_lists: idx_lists = %d, memory_lists = %d\n", len1, len2);
  while (i < len1 && j < len2) {
    int c = list_id_compare (LArr1[i]->list_id, LArr2[j]->list_id);
    if (c < 0) {
      L = LArr1[i ++];
    } else if (!c) {
      L = LArr2[j ++];
      i ++;
    } else {
      L = LArr2[j ++];
      assert (L->metafile_index == -1);
    }
    rt = process_one_list(L);
    r += rt;
    if (rt) {
      LArr[k ++] = L;
    }
  }
  while (i < len1) {
    L = LArr1[i ++];
    rt = process_one_list(L);
    r += rt;
    if (rt) {
      LArr[k ++] = L;
    }
  }
  while (j < len2) {
    L = LArr2[j ++];
    assert (L && L->metafile_index == -1);
    if (L->metafile_index != -1) {
      vkprintf (0, "L->metafile_index = %d, index_lists = %d, list_id = " idout "\n", L->metafile_index, idx_lists, out_list_id (L->list_id));
    }
    assert (L->metafile_index == -1);
    rt = process_one_list(L);
    r += rt;
    if (rt) {
      LArr[k ++] = L;
    }
  }
  vkprintf (1, "traverse_all_list end: result = %d\n", r);
  return r;
}

static inline int max(int a, int b){
    return a > b ? a : b;
}

static int get_tree_date(tree_ext_large_t *t) {
    if (t == NILL) return 0;
    return max(t->payload.date, max(get_tree_date(t->left), get_tree_date(t->right)));
}

int get_list_date (list_id_t list_id) {
  if (metafile_mode && prepare_list_metafile (list_id, -1) < 0) {
    return 0;
  }
  
  list_t *L = __get_list_f (list_id, 2);
  if (!L) {
    return 0;
  }
  
  unpack_metafile_pointers (L);
  
  if (SMALL_NODE(*(OTree.root)) != NIL) {
        return get_tree_date(*(OTree.root));
  }
  
  int res = 0, i, len;
  for (i = 0, len = OTree.N; i < len; i++) {
    res = max(res, metafile_get_date (i));
  }
  
  return res;
}

void lsort (list_t **A, int b) {
  if (b <= 0) {
    return;
  }
  int i = 0, j = b;
  list_id_t h = A[b >> 1]->list_id; 
  list_t *t;
  do {
    while (list_id_less (A[i]->list_id, h)) {
      i++;
    }
    while (list_id_less (h, A[j]->list_id)) {
      j--;
    }
    if (i <= j) {
      t = A[i];
      A[i] = A[j];
      A[j] = t;
      i++;
      j--;
    }
  } while (i <= j);
  lsort (A, j);
  lsort (A + i, b - i);
}




void init_new_header0 (void) {
  NewHeader.magic = -1;
  NewHeader.log_pos1 = (long long) log_cur_pos();
  NewHeader.log_timestamp = log_read_until;
  NewHeader.idx_mode = (sizeof(value_t) / 4 - 1) * LIDX_HAVE_BIGVALUES;
  write_pos = 0;
  writeout (&NewHeader, sizeof(NewHeader));
}


long long output_revlist_entries;
//idx_list_entries

static var_list_id_t tmp_list_id;
static var_object_id_t tmp_object_id;
static int tmp_set;

static void check_tmp_list_object (list_id_t list_id, object_id_t object_id) {
//  fprintf (stderr, "revlist: entry(%lld)=" idout "#" idout "\n",
//      output_revlist_entries, out_object_id (object_id), out_list_id (list_id));
  if (tmp_set) {
    int c = object_id_compare (tmp_object_id, object_id);
    if (!c) {
      #ifdef LISTS_Z
      c = list_id_compare (tmp_list_id, list_id);
      #else
      if ((unsigned)tmp_list_id < (unsigned)list_id) {
        c = -1;
      } else 
      if ((unsigned)tmp_list_id == (unsigned)list_id) {
        c = 0;
      } else {
        c = 1;
      }
      #endif
    }
    if (c >= 0) {
      vkprintf (0, "error in revlist: entry(%lld)=" idout "#" idout " >= entry(%lld)=" idout "#" idout "\n",
      output_revlist_entries - 1, out_object_id (tmp_object_id), out_list_id (tmp_list_id), 
      output_revlist_entries, out_object_id (object_id), out_list_id (list_id));
      assert (c < 0);
    }
  }
  tmp_set = 1;
  #ifdef LISTS_Z
  memcpy (tmp_list_id, list_id, list_id_bytes);
  memcpy (tmp_object_id, object_id, object_id_bytes);
  #else
  tmp_list_id = list_id;
  tmp_object_id = object_id;
  #endif
}

int last_metafile_start;
int new_tot_revlist_metafiles;
long long new_revlist_metafiles_offsets[1000000];
unsigned new_revlist_metafiles_crc32[1000000];
char new_revlist_metafiles_list_object[100000000];
char *new_revlist_metafiles_list_object_pos;

void check_new_revlist_metafile_start (object_id_t object_id) {
  if (output_revlist_entries - last_metafile_start < 1000) {
    return;
  }
  int c = object_id_compare (tmp_object_id, object_id);
  if (c) {
    vkprintf (2, "New revlist metafile contains %lld items\n", output_revlist_entries - last_metafile_start);

    last_metafile_start = output_revlist_entries;
  }
}



void store_revlist_metafile (list_id_t list_id, object_id_t object_id) {
  if (output_revlist_entries == last_metafile_start) {
    if (new_tot_revlist_metafiles) {
      new_revlist_metafiles_crc32[new_tot_revlist_metafiles - 1] = metafile_crc32;
    }
    clear_metafile_crc32 ();
    assert (new_tot_revlist_metafiles < 1000000);
    new_revlist_metafiles_offsets[new_tot_revlist_metafiles] = get_write_pos ();
    #ifdef LISTS_Z
    new_revlist_metafiles_list_object_pos += list_id_bytes + object_id_bytes;
    #else
    new_revlist_metafiles_list_object_pos += sizeof (list_id_t) + sizeof (object_id_t);
    #endif
    assert (new_revlist_metafiles_list_object_pos < new_revlist_metafiles_list_object + 100000000);
    vkprintf (2, "store list_id=" idout ", object_id=" idout "\n", out_list_id(list_id), out_object_id(object_id));
    new_tot_revlist_metafiles++;
    assert (new_revlist_metafiles_list_object_pos <= new_revlist_metafiles_list_object + 100000000);
    assert (new_tot_revlist_metafiles <= 1000000);
  }
  #ifdef LISTS_Z
  memcpy (new_revlist_metafiles_list_object_pos - object_list_bytes, list_id, list_id_bytes);
  memcpy (new_revlist_metafiles_list_object_pos - object_id_bytes, object_id, object_id_bytes);
  #else
  *(list_id_t *)(new_revlist_metafiles_list_object_pos - sizeof (list_id) - sizeof (object_id)) = list_id;
  *(object_id_t *)(new_revlist_metafiles_list_object_pos - sizeof (object_id)) = object_id;
  #endif
}



void write_revlist_recursive (ltree_t *T, ltree_x_t R) {
  if (!T) {
    while (!revlist_iterator.eof) {
      object_id_t object_id = CUR_REVLIST_OBJECT_ID;
      list_id_t list_id = CUR_REVLIST_LIST_ID;
      int c = object_id_compare (object_id, ltree_x_object_id (R));
      if (!c) {
        #ifdef LISTS_Z
        c = list_id_compare (list_id, ltree_x_list_id (R));
        #else
        c = (unsigned)list_id >= (unsigned)ltree_x_list_id (R) ? 1 : -1;
        #endif
      }
      if (c >= 0) {
        break;
      }
      //if (metafile_mode) {
      //  assert (prepare_list_metafile (list_id, 0) >= 0);
      //}
      //fprintf (stderr, "%p\n", metafiles[Header.tot_lists + 1 + revlist_iterator.metafile_number]);
      if (entry_exists_nof (list_id, object_id) && 
          (!revlist_iterator.cur_pos ||
           !list_id_equal (PREV_REVLIST_LIST_ID, list_id) ||
           !object_id_equal (PREV_REVLIST_OBJECT_ID, object_id)
          )
         ) {
//        fprintf (stderr, "copy old revlist entry(%d) -> entry(%lld)\n", i, output_revlist_entries);
        check_new_revlist_metafile_start (object_id);
        store_revlist_metafile (list_id, object_id);
        writeout_list_id (list_id);
        writeout_object_id (object_id);
        check_tmp_list_object (list_id, object_id);
        vkprintf (4, "lo: " idout " " idout "\n", out_list_id (list_id), out_object_id (object_id));
        output_revlist_entries++;
      }
      advance_revlist_iterator ();
    }
    return;
  }

  if (T->left) {
    assert (ltree_x_less (T->left->x, T->x));
  }

  write_revlist_recursive (T->left, T->x);

  while (!revlist_iterator.eof && 
      list_id_equal (CUR_REVLIST_LIST_ID, ltree_x_list_id (T->x)) && 
      object_id_equal (CUR_REVLIST_OBJECT_ID, ltree_x_object_id (T->x))) {
    advance_revlist_iterator ();
  }
  // process T->x

  assert (ltree_x_less (T->x, R));
  
  vkprintf (check_debug_list (ltree_x_list_id (T->x)) ? 0 : 3, "Revlist index " idout " " idout " exists %d\n", out_list_id (ltree_x_list_id (T->x)), out_object_id (ltree_x_object_id (T->x)), entry_exists_nof (ltree_x_list_id (T->x), ltree_x_object_id (T->x)));

  if (entry_exists_nof (ltree_x_list_id (T->x), ltree_x_object_id (T->x))) {
//    fprintf (stderr, "creating new revlist entry(%lld) from T=%p\n", output_revlist_entries, T);
    check_new_revlist_metafile_start (ltree_x_object_id (T->x));
    store_revlist_metafile (ltree_x_list_id (T->x), ltree_x_object_id (T->x));
    writeout_list_id (ltree_x_list_id (T->x));
    writeout_object_id (ltree_x_object_id (T->x));
    check_tmp_list_object (ltree_x_list_id (T->x), ltree_x_object_id (T->x));
    vkprintf (4, "lo: " idout " " idout "\n", out_list_id (ltree_x_list_id (T->x)), out_object_id (ltree_x_object_id (T->x)));
    output_revlist_entries++;
  }

  if (T->right) {
    assert (ltree_x_less (T->x, T->right->x));
  }

  write_revlist_recursive (T->right, R);
}

void sort_revlist_part (file_revlist_entry_t *RData, long a, long b) {
  long i = a, j = b;
  var_list_id_t h, t;
  if (b <= a) {
    return;
  }
  #ifdef LISTS_Z
  memcpy (h, REVLIST_LIST_ID(RData,(a + b) >> 1), list_id_bytes);
  #else
  h = REVLIST_LIST_ID(RData,(a + b) >> 1);
  #endif
  do {
    #ifdef LISTS_Z
      while (list_id_less (REVLIST_LIST_ID(RData,i), h)) { i++; }
      while (list_id_less (h, REVLIST_LIST_ID(RData,j))) { j--; }
    #else
      while ((unsigned)REVLIST_LIST_ID(RData,i) < (unsigned)h) { i++; }
      while ((unsigned)h < (unsigned)REVLIST_LIST_ID(RData,j)) { j--; }
    #endif
    if (i <= j) {
      #ifdef LISTS_Z
      memcpy (t, REVLIST_LIST_ID(RData,i), list_id_bytes);
      memcpy (REVLIST_LIST_ID(RData,i), REVLIST_LIST_ID(RData,j), list_id_bytes);
      memcpy (REVLIST_LIST_ID(RData,j), t, list_id_bytes);
      #else
      t = REVLIST_LIST_ID(RData,i); 
      REVLIST_LIST_ID(RData,i) = REVLIST_LIST_ID(RData,j);  
      REVLIST_LIST_ID(RData,j) = t;
      #endif
      i++; 
      j--;
    }
  } while (i <= j);
  sort_revlist_part (RData, a, j);
  sort_revlist_part (RData, i, b);
}


void resort_revlist (void) {
  int cur_metafile;
  if (revlist_metafile_mode) {
    cur_metafile = 0;;
  } else {
    cur_metafile = -1;
  }
  object_id_t object_id;
  while (cur_metafile < tot_revlist_metafiles) {
    init_revlist_iterator (cur_metafile, 0);
    long i = 0;
    while (!revlist_iterator.eof) {
      object_id = CUR_REVLIST_OBJECT_ID;
      i = revlist_iterator.cur_pos;
      while (!revlist_iterator.eof && advance_revlist_iterator ()) {
        if (!object_id_equal (object_id, CUR_REVLIST_OBJECT_ID)) {
          break;
        }
      }
      sort_revlist_part (revlist_iterator.RData, i, revlist_iterator.cur_pos - 1);
    }
    cur_metafile ++;
  }
/*  for (i = 0; i < 1980 && i < idx_list_entries; i++) { 
    fprintf (stderr, "old revlist: entry(%d)=" idout "#" idout "\n",
      i, out_object_id (REVLIST_OBJECT_ID(i)), out_list_id (REVLIST_LIST_ID(i)));
  }*/

}


void write_revlist_metafiles_headers (void) {
  writeout (new_revlist_metafiles_offsets, (new_tot_revlist_metafiles + 1) * sizeof (long long));
  writeout (new_revlist_metafiles_list_object, new_revlist_metafiles_list_object_pos - new_revlist_metafiles_list_object);  
  writeout (new_revlist_metafiles_crc32, (new_tot_revlist_metafiles + 1) * sizeof (unsigned));  
}

void finish_revlist (void) {
  new_revlist_metafiles_crc32[new_tot_revlist_metafiles - 1] = metafile_crc32;
  new_revlist_metafiles_offsets[new_tot_revlist_metafiles] = get_write_pos ();
}
/*
static int percent (long long a, long long b) {
  if (b <= 0 || a <= 0) return 0;
  if (a >= b) return 100;
  return (a*100 / b);
}
*/
double idx_start_time, idx_revlist_start_time, idx_end_time;
extern double index_load_time, binlog_load_time;

static void output_index_stats (void) {
  fprintf (stderr, "%d total lists: %d lists from old index, %d in memory\n",
    NewHeader.tot_lists, Header.tot_lists, tot_lists);
  fprintf (stderr, "%lld total list entries (%.3f average); %d in-memory entries\n",
    new_total_entries, NewHeader.tot_lists ? (double) new_total_entries / NewHeader.tot_lists : 0, alloc_large_nodes);

  if (NewHeader.data_end_offset > 0) {
    fprintf (stderr, "index offsets: list_index=%lld list_data=%lld revlist_data=%lld extra_data=%lld end=%lld\n",
 	NewHeader.list_index_offset, NewHeader.list_data_offset, NewHeader.revlist_data_offset, NewHeader.extra_data_offset, NewHeader.data_end_offset);
  }
  fprintf (stderr, "total time: %.04fs = %.04fs + %.04fs + %.04fs (read index + read binlog + generate index)\n",
     idx_end_time - idx_start_time + index_load_time + binlog_load_time,
     index_load_time, binlog_load_time, idx_end_time - idx_start_time);
  fprintf (stderr, "used memory: %ld bytes out of %ld\n", (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first));
}


void do_all_postponed (void) {
  int i;
  if (!postponed) {
    assert (!Header.tot_lists);
    return;
  }
  assert (postponed);
  for (i = 0; i < Header.tot_lists; i++) {
    //if (postponed[i]) {
    if (postponed[i]) {
      assert (prepare_list_metafile_num (i, -1) >= 0);
    }
    if (postponed[i] && !(metafile_mode & 1)) {
      mark_list_metafile (FLI_ENTRY_LIST_ID (i));
    }
    //}
  }
}


struct metafile_date {
  int idx;
  int date;
};

void mfd_sort (struct metafile_date **A, int b) {
  if (b <= 0) {
    return;
  }
  int i = 0, j = b;
  int h = A[b >> 1]->date;
  struct metafile_date *t;
  do {
    while (A[i]->date < h) {
      i++;
    }
    while (h < A[j]->date) {
      j--;
    }
    if (i <= j) {
      t = A[i];
      A[i] = A[j];
      A[j] = t;
      i++;
      j--;
    }
  } while (i <= j);
  mfd_sort (A, j);
  mfd_sort (A + i, b - i);
}

int write_index (int writing_binlog) {
  if (metafile_mode & 2) {
    metafile_mode = 4;
  }
  if (metafile_mode & 1) {
    memory_for_metafiles = 1;
    while (tot_metafiles_memory > memory_for_metafiles && unload_LRU ());  
  }
  int i;

  if (!kfs_sws_open (&SWS, engine_snapshot_replica, log_cur_pos (), jump_log_pos)) {
    return 1;
  }

  idx_start_time = get_utime(CLOCK_MONOTONIC);

  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }

  if (metafile_mode) {
    do_all_postponed ();
  }

  // Begin create LArr1
  list_t **LArr1 = zzmalloc (sizeof (void *) * idx_lists);
  assert (LArr1);
  for (i = 0; i < idx_lists; i++) {
    LArr1[i] = create_dummy_list(FLI_ENTRY_LIST_ID(i), i);
  }

  lsort (LArr1, idx_lists - 1);

  for (i = 1; i < idx_lists; i++) {
    assert (list_id_less (LArr1[i-1]->list_id, LArr1[i]->list_id));
  }
  // End create LArr1


  // Begin create LArr2
  list_t **LArr2 = zzmalloc (sizeof (void *) * tot_lists);
  assert (LArr2);
  int lenLArr2 = 0;
  for (i = 0; i < lists_prime; i++) {
    if (List[i]) {
      LArr2[lenLArr2 ++] = List[i];
    }
  }
  assert (lenLArr2 == tot_lists);

  lsort (LArr2, tot_lists - 1);

  for (i = 1; i < tot_lists; i++) {
    assert (list_id_less (LArr2[i-1]->list_id, LArr2[i]->list_id));
  }
  // End create LArr2

  init_new_header0();

  NewHeader.tot_lists = traverse_all_lists (LArr1, idx_lists, LArr2, tot_lists, report_one);

  long long list_dir_size = (long long)(NewHeader.tot_lists + 1) * file_list_index_entry_size;
  assert (list_dir_size == (long)list_dir_size && list_dir_size >= 0);

  NewFileLists = zzmalloc (list_dir_size);
  assert (NewFileLists);

#ifdef LISTS_Z
  NewFileLists_adjusted = (char *) NewFileLists + list_id_bytes;
#endif

  NewHeader.list_index_offset = get_write_pos();
  NewHeader.list_data_offset = write_pos + list_dir_size;

  write_seek (NewHeader.list_data_offset);

  metafiles_output = 0;
  new_total_entries = 0;

  if (verbosity > 0) {
    fprintf (stderr, "writing metafiles\n");
  }

  if (metafiles_order == 1) {
    struct metafile_date **metafile_dates = zzmalloc(sizeof (void *) * NewHeader.tot_lists);
    int metafile_date_size = sizeof(struct metafile_date);
    
    for(i = 0; i < NewHeader.tot_lists; i++) {
        metafile_dates[i] = zzmalloc(metafile_date_size);
        metafile_dates[i]->idx = i;
        metafile_dates[i]->date = get_list_date(LArr[i]->list_id);
    }
    
    mfd_sort(metafile_dates, NewHeader.tot_lists - 1);

    for (i = 0; i < NewHeader.tot_lists; i++) {
      write_metafile(LArr[metafile_dates[i]->idx]);
      zzfree (metafile_dates[i], metafile_date_size);
    }
    
    zzfree (metafile_dates, sizeof (void *) * NewHeader.tot_lists);
  }
  else{
    lsort (LArr, NewHeader.tot_lists - 1);

    for (i = 0; i < NewHeader.tot_lists; i++) {
      write_metafile(LArr[i]);
    }
  }

  assert (metafiles_output == NewHeader.tot_lists);

  NewHeader.revlist_data_offset = get_write_pos ();

  COPY_LIST_ID (NEW_FLI_ENTRY_LIST_ID (metafiles_output), MAX_LIST_ID);
  NEW_FLI_ENTRY_ADJUSTED(metafiles_output)->list_file_offset = write_pos;
  
  output_revlist_entries = 0;

  idx_revlist_start_time = get_utime(CLOCK_MONOTONIC);

  if (verbosity > 0) {
    fprintf (stderr, "%d metafiles written (%lld entries, %lld bytes) in %.04fs, writing revlist\n", metafiles_output, new_total_entries, NewHeader.revlist_data_offset - NewHeader.list_data_offset, idx_revlist_start_time - idx_start_time);
  }

  if (!disable_revlist) {
    if (!revlist_metafile_mode) {
      resort_revlist ();
    }

    if (verbosity > 0) {
      fprintf (stderr, "revlist sorted\n");
    }

    if (revlist_metafile_mode) {
      init_revlist_iterator (0, 1);
    } else {
      init_revlist_iterator (-1, 0);
    }
    new_tot_revlist_metafiles = 0;
    last_metafile_start = 0;
    new_revlist_metafiles_list_object_pos = new_revlist_metafiles_list_object;
    write_revlist_recursive (object_tree, MAX_LIST_OBJECT_PAIR);
    finish_revlist ();


    if (verbosity > 0 || output_revlist_entries != new_total_entries) {
      fprintf (stderr, "%lld revlist entries written [%lld expected], %lld bytes in %.04fs\n", output_revlist_entries, new_total_entries, get_write_pos() - NewHeader.revlist_data_offset, get_utime(CLOCK_MONOTONIC) - idx_revlist_start_time);
    }

    if (revlist_metafile_mode) {
      if (revlist_iterator.metafile_number != tot_revlist_metafiles) {
        fprintf (stderr, "ERROR: revlist_iterator.metafile_number == %d, tot_revlist_metafiles = %d, eof = %d\n", revlist_iterator.metafile_number, tot_revlist_metafiles, revlist_iterator.eof);
      }
      assert (revlist_iterator.metafile_number == tot_revlist_metafiles);
    } else {
      assert (revlist_iterator.eof);
    }
    assert (output_revlist_entries == new_total_entries);
    NewHeader.params |= PARAMS_HAS_REVLIST_OFFSETS | PARAMS_HAS_CRC32;
    NewHeader.tot_revlist_metafiles = new_tot_revlist_metafiles;
  } else {
    NewHeader.params |= PARAMS_HAS_CRC32;
  }


  NewHeader.extra_data_offset = get_write_pos ();

  if (!disable_revlist) {
    write_revlist_metafiles_headers ();
  }


  NewHeader.data_end_offset = get_write_pos ();

  write_seek (NewHeader.list_index_offset);

  NewHeader.filelist_crc32 = compute_crc32 (NewFileLists, list_dir_size);
  kfs_sws_write (&SWS, NewFileLists, list_dir_size); /* destroy NewFileLists */
  zzfree (NewFileLists, list_dir_size);
  NewFileLists = NULL;

  if (verbosity > 0) {
    fprintf (stderr, "%lld list directory bytes written\n", list_dir_size);
  }

  write_seek (0);

  NewHeader.magic = !disable_revlist ? LISTS_INDEX_MAGIC_NEW : LISTS_INDEX_MAGIC_NO_REVLIST;
  NewHeader.created_at = time(0);
  NewHeader.log_pos0 = Header.log_pos1;
  NewHeader.log_pos1 = log_cur_pos();
  NewHeader.file_hash = Header.file_hash;
  NewHeader.slice_hash = Header.slice_hash;
  NewHeader.log_timestamp = log_read_until;
  NewHeader.log_split_min = log_split_min;
  NewHeader.log_split_max = log_split_max;
  NewHeader.log_split_mod = log_split_mod;
  NewHeader.last_global_id = last_global_id;
  NewHeader.log_pos0_crc32 = Header.log_pos1_crc32;
  NewHeader.log_pos1_crc32 = ~log_crc32_complement;
  NewHeader.kludge_magic = 1;
  NewHeader.list_id_ints = LIST_ID_INTS;
  NewHeader.object_id_ints = OBJECT_ID_INTS;
  NewHeader.value_ints = sizeof (value_t) / 4;
  unsigned t = compute_crc32 (new_revlist_metafiles_offsets, (new_tot_revlist_metafiles + 1) * sizeof (long long));
  t = ~crc32_partial (new_revlist_metafiles_list_object, new_revlist_metafiles_list_object_pos - new_revlist_metafiles_list_object, ~t);  
  NewHeader.revlist_arrays_crc32 = t;
  NewHeader.header_crc32 = compute_crc32 (&NewHeader, sizeof (NewHeader) - 4);

  writeout (&NewHeader, sizeof (NewHeader));

  for (i = 0; i < idx_lists; i++) {
    zzfree(LArr1[i], list_struct_size);
  }
  zzfree(LArr1, sizeof (void *) * idx_lists);
  zzfree(LArr2, sizeof (void *) * tot_lists);
  zzfree(LArr, sizeof (void *) * NewHeader.tot_lists);

  flushout ();

  kfs_sws_close (&SWS);

  idx_end_time = get_utime(CLOCK_MONOTONIC);

  if (verbosity > 0) {
    output_index_stats ();
  }

  return 0;
}

static int global_dump_fl, global_dump_rem, global_dump_mod;

static int tree_write_object_id_indirect (tree_ext_small_t *T) {
  writeout_object_id (T->x);
  ++new_total_entries;
  return 0;
}


static int array_write_object_id_indirect (listree_t *LT, int temp_id) {
  object_id_t object_id = OARR_ENTRY (M_obj_id_list, LT->IA[temp_id]);
  writeout_object_id (object_id);
  ++new_total_entries;
  return 0;
}


int dump_single_list (list_t *L) {
  listree_t OTreeSub;

  #ifdef LISTS_Z
  int t = L->list_id[0];
  #else
  int t = L->list_id;
  #endif
  if (t == MAXINT) { 
    return 0; 
  }
  t %= global_dump_mod;
  if (t != global_dump_rem && t != -global_dump_rem) { 
    return 0; 
  }

  unpack_metafile_pointers (L);

  new_total_entries = 0;

  int sz;

  if (!global_dump_fl) {
    sz = M_tot_entries + L->o_tree->delta;

    writeout_list_id (L->list_id);
    writeout_int (sz);

    in_tree = tree_write_object_id;
    in_array = array_write_object_id;


    listree_get_range ((listree_t *) &OTree, 0, MAXINT);
  } else {
    int sublist = global_dump_fl & 7;

    sz = metafile_get_sublist_size (sublist);

    if (!sz) {
      return 0;
    }

    writeout_list_id (L->list_id);
    writeout_int (sz);

    in_tree = tree_write_object_id_indirect; 
    in_array = array_write_object_id_indirect;

    load_o_tree_sub (&OTreeSub, sublist);
    listree_get_range (&OTreeSub, 0, MAXINT);
  }

  assert (sz == new_total_entries);

  return 1;
}

int dump_all_lists (int sublist, int dump_rem, int dump_mod) {
  newidx_fd = 1;
  global_dump_fl = sublist;
  assert ((unsigned) sublist <= 8);

  if (!dump_mod) {
    dump_mod = 1;
  }
  assert (dump_rem >= 0 && dump_rem < dump_mod);
  global_dump_rem = dump_rem;
  global_dump_mod = dump_mod;

  int i;
  // Begin create LArr1
  list_t **LArr1 = zzmalloc (sizeof (void *) * idx_lists);
  assert (LArr1);
  for (i = 0; i < idx_lists; i++) {
    LArr1[i] = create_dummy_list(FLI_ENTRY_LIST_ID(i), i);
  }

  lsort (LArr1, idx_lists - 1);

  for (i = 1; i < idx_lists; i++) {
    assert (list_id_less (LArr1[i-1]->list_id, LArr1[i]->list_id));
  }
  // End create LArr1

  // Begin create LArr2
  list_t **LArr2 = zzmalloc (sizeof (void *) * tot_lists);
  assert (LArr2);
  int lenLArr2 = 0;
  for (i = 0; i < lists_prime; i++) {
    if (List[i]) {
      LArr2[lenLArr2 ++] = List[i];
    }
  }
  assert (lenLArr2 == tot_lists);

  lsort (LArr2, tot_lists - 1);

  for (i = 1; i < tot_lists; i++) {
    assert (list_id_less (LArr2[i-1]->list_id, LArr2[i]->list_id));
  }
  // End create LArr2

  int res = traverse_all_lists (LArr1, idx_lists, LArr2, tot_lists, dump_single_list);

  for (i = 1; i < res; i++) {
    assert (list_id_less (LArr[i-1]->list_id, LArr[i]->list_id));
  }

  flushout ();

  newidx_fd = -1;
  return res;
}


//!!! Does not work
long long dump_all_value_sums (void) {
  int i;
  list_id_t x;
  list_t *L;
  long long cur = 0, sum = 0;

  for (i = 0; i < lists_prime; i++) {
    L = List[i];
    if (L) {
      //what the hell was here?
      //x = i * log_split_mod - negative_list_id_offset;
      //if (x >= 0) { 
      //  x += log_split_min; 
      //} else { 
      //  x -= log_split_min; 
      //}
      //sum += cur = sum_tree_values (L->tree);
      x = L->list_id;
      printf (idout "\t%lld\n", out_list_id (x), cur);
    }
  }

  return sum;
}

ltree_t *ignore_tree;
extern int ignore_timestamp;

int ignore_list_check (list_id_t list_id, object_id_t object_id) {
  if (now < ignore_timestamp) { return 0; }
  static var_ltree_x_t lkey;
  combine_ltree_x (list_id, object_id, &lkey);
  return ltree_lookup (ignore_tree, lkey) != 0;
}

void ignore_list_object_add (list_id_t list_id, object_id_t object_id) {
  static var_ltree_x_t lkey;
  combine_ltree_x (list_id, object_id, &lkey);
  ignore_tree = ltree_insert (ignore_tree, lkey, lrand48 ());
}
