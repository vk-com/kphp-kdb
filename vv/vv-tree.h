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
              2012-2013 Vitaliy Valtman              
*/

#define __DEFINE_TREE(prefix,name,value_t,value_compare,y_t,y_compare,new_tree_node,free_tree_node)            \
  __DECLARE_TREE(prefix,name,value_t,y_t)                                                                      \
                                                                                                      \
                                                                                                      \
  tree_ ## name ## _t *tree_lookup_ ## name (tree_ ## name ## _t *T, value_t x) {                     \
    long long c;                                                                                      \
    while (T && (c = value_compare (x, T->x))) {                                                      \
      T = (c < 0) ? T->left : T->right;                                                               \
    }                                                                                                 \
    return T;                                                                                         \
  }                                                                                                   \
                                                                                                      \
  value_t *tree_lookup_value_ ## name (tree_ ## name ## _t *T, value_t x) {                            \
    long long c;                                                                                      \
    while (T && (c = value_compare (x, T->x))) {                                                      \
      T = (c < 0) ? T->left : T->right;                                                               \
    }                                                                                                 \
    return T ? &T->x : 0;                                                                             \
  }                                                                                                   \
                                                                                                      \
  void tree_split_ ## name (tree_ ## name ## _t **L, tree_ ## name ## _t **R, tree_ ## name ## _t *T, value_t x) {                     \
    if (!T) { *L = *R = 0; return; }                                                                  \
    int c = value_compare (x, T->x);                                                                  \
    if (c < 0) {                                                                                      \
      *R = T;                                                                                         \
      tree_split_ ## name (L, &T->left, T->left, x);                                                  \
    } else {                                                                                          \
      *L = T;                                                                                         \
      tree_split_ ## name (&T->right, R, T->right, x);                                                \
    }                                                                                                 \
  }                                                                                                   \
                                                                                                      \
  tree_ ## name ## _t *tree_insert_ ## name (tree_ ## name ## _t *T, value_t x, y_t y) {                                    \
    tree_ ## name ## _t *P;                                                                                      \
    if (!T) {                                                                                         \
      P = new_tree_node (x, y);                                                                       \
      return P;                                                                                       \
    }                                                                                                 \
    long long c = y_compare (y, T->y);                                                                \
    if (c < 0) {                                                                                      \
      c = value_compare (x, T->x);                                                                    \
      assert (c);                                                                                     \
      if (c < 0) {                                                                                    \
        T->left = tree_insert_ ## name (T->left, x, y);                                               \
      } else {                                                                                        \
        T->right = tree_insert_ ## name (T->right, x, y);                                             \
      }                                                                                               \
      return T;                                                                                       \
    }                                                                                                 \
    P = new_tree_node (x, y);                                                                         \
    tree_split_ ## name (&P->left, &P->right, T, x);                                                  \
    return P;                                                                                         \
  }                                                                                                   \
                                                                                                      \
  tree_ ## name ## _t *tree_merge_ ## name (tree_ ## name ## _t *L, tree_ ## name ## _t *R) {         \
    if (!L) { return R; }                                                                             \
    if (!R) { return L; }                                                                             \
    if (y_compare (L->y, R->y) > 0) {                                                                 \
      L->right = tree_merge_ ## name (L->right, R);                                                   \
      return L;                                                                                       \
    } else {                                                                                          \
      R->left = tree_merge_ ## name (L, R->left);                                                     \
      return R;                                                                                       \
    }                                                                                                 \
  }                                                                                                   \
                                                                                                      \
  tree_ ## name ## _t *tree_delete_ ## name (tree_ ## name ## _t *T, value_t x) {                     \
    assert (T);                                                                                       \
    long long c = value_compare (x, T->x);                                                            \
    if (!c) {                                                                                         \
      tree_ ## name ## _t *N = tree_merge_ ## name (T->left, T->right);                               \
      free_tree_node (T);                                                                             \
      return N;                                                                                       \
    } else  if (c < 0) {                                                                              \
      T->left = tree_delete_ ## name (T->left, x);                                                    \
    } else {                                                                                          \
      T->right = tree_delete_ ## name (T->right, x);                                                  \
    }                                                                                                 \
    return T;                                                                                         \
  }                                                                                                   \
                                                                                                      \
                                                                                                      \
  tree_ ## name ## _t *tree_get_min_ ## name (tree_ ## name ## _t *T) {                               \
    while (T && T->left) {                                                                            \
      T = T->left;                                                                                    \
    }                                                                                                 \
    return T;                                                                                         \
  }                                                                                                   \
  void tree_act_ ## name (tree_ ## name ##_t *T, tree_ ## name ## _act_t A) {                         \
    if (!T) {                                                                                         \
      return;                                                                                         \
    }                                                                                                 \
    tree_act_ ## name (T->left, A);                                                                   \
    A (T->x);                                                                                         \
    tree_act_ ## name (T->right, A);                                                                  \
  }                                                                                                   \
  tree_ ## name ## _t *tree_clear_ ## name (tree_ ## name ## _t *T) { \
    if (!T) { \
      return 0; \
    } \
    tree_clear_ ## name (T->left); \
    tree_clear_ ## name (T->right); \
    tree_free_ ## name (T); \
    return 0; \
  }\
\
  void tree_check_ ## name (tree_ ## name ## _t *T) { \
    if (!T) { \
      return; \
    } \
    if (T->left) { \
      assert (value_compare (T->left->x, T->x) < 0); \
      tree_check_ ## name (T->left); \
    }\
    if (T->right) { \
      assert (value_compare (T->right->x, T->x) > 0); \
      tree_check_ ## name (T->right); \
    }\
  } \
  \
  int tree_count_ ## name (tree_ ## name ## _t *T) { \
    if (!T) { \
      return 0; \
    } \
    return 1 + tree_count_ ## name (T->left) + tree_count_ ## name (T->right); \
  } \



#define __DEFINE_TREE_STD_ALLOC_PREFIX(prefix,name,value_t,value_compare,y_t,y_compare)                               \
  __DECLARE_TREE_TYPE(name,value_t,y_t)                                                                      \
  prefix tree_ ## name ## _t *tree_alloc_ ## name (value_t x, y_t);                                          \
  prefix void tree_free_ ## name (tree_ ## name ## _t *T);                                                  \
  __DEFINE_TREE(prefix,name,value_t,value_compare,y_t,y_compare,tree_alloc_ ## name,tree_free_ ## name)       \
  tree_ ## name ## _t *tree_alloc_ ## name (value_t x, y_t y) {                                                  \
    tree_ ## name ## _t *T = zmalloc (sizeof (tree_ ## name ## _t));                                                        \
    T->x = x;                                                                                         \
    T->y = y; \
    T->left = T->right = 0; \
    return T;                                                                                         \
  }                                                                                                   \
  void tree_free_ ## name (tree_ ## name ## _t *T) {                                                             \
    zfree (T, sizeof (tree_ ## name ## _t));                                                                     \
  }                                                                                                   \

#define __DEFINE_TREE_STDNOZ_ALLOC_PREFIX(prefix,name,value_t,value_compare,y_t,y_compare)                               \
  __DECLARE_TREE_TYPE(name,value_t,y_t)                                                                      \
  prefix tree_ ## name ## _t *tree_alloc_ ## name (value_t x, y_t);                                          \
  prefix void tree_free_ ## name (tree_ ## name ## _t *T);                                                  \
  __DEFINE_TREE(prefix,name,value_t,value_compare,y_t,y_compare,tree_alloc_ ## name,tree_free_ ## name)       \
  tree_ ## name ## _t *tree_alloc_ ## name (value_t x, y_t y) {                                                  \
    tree_ ## name ## _t *T = malloc (sizeof (tree_ ## name ## _t));                                                        \
    assert (T); \
    T->x = x;                                                                                         \
    T->y = y; \
    T->left = T->right = 0; \
    return T;                                                                                         \
  }                                                                                                   \
  void tree_free_ ## name (tree_ ## name ## _t *T) {                                                             \
    free (T);                                                                     \
  }                                                                                                   \


#define DEFINE_TREE_STD_ALLOC(name,value_t,value_compare,y_t,y_compare) \
  __DEFINE_TREE_STD_ALLOC_PREFIX(static,name,value_t,value_compare,y_t,y_compare)

#define DEFINE_TREE_STDNOZ_ALLOC(name,value_t,value_compare,y_t,y_compare) \
  __DEFINE_TREE_STDNOZ_ALLOC_PREFIX(static,name,value_t,value_compare,y_t,y_compare)

#define DEFINE_TREE_STD_ALLOC_GLOBAL(name,value_t,value_compare,y_t,y_compare) \
  __DEFINE_TREE_STD_ALLOC_PREFIX(,name,value_t,value_compare,y_t,y_compare)

#define __DECLARE_TREE_TYPE(name,value_t,y_t) \
  struct tree_ ## name {                                                                                     \
    struct tree_ ## name *left, *right;                                                                      \
    value_t x;                                                                                        \
    y_t y;                                                                                            \
  };                                                                                                  \
  typedef struct tree_ ## name tree_ ## name ## _t;                                                                     \
  typedef void (*tree_ ## name ## _act_t)(value_t x); \

#define __DECLARE_TREE(prefix,name,value_t,y_t)       \
  prefix tree_ ## name ## _t *tree_lookup_ ## name (tree_ ## name ## _t *T, value_t x) __attribute__ ((unused));                                            \
  prefix value_t *tree_lookup_value_ ## name (tree_ ## name ## _t *T, value_t x) __attribute__ ((unused));                                            \
  prefix void tree_split_ ## name (tree_ ## name ## _t **L, tree_ ## name ## _t **R, tree_ ## name ## _t *T, value_t x) __attribute__ ((unused));                      \
  prefix tree_ ## name ## _t *tree_insert_ ## name (tree_ ## name ## _t *T, value_t x, y_t y) __attribute__ ((warn_unused_result,unused));                                     \
  prefix tree_ ## name ## _t *tree_merge_ ## name (tree_ ## name ## _t *L, tree_ ## name ## _t *R) __attribute__ ((unused));                                           \
  prefix tree_ ## name ## _t *tree_delete_ ## name (tree_ ## name ## _t *T, value_t x) __attribute__ ((warn_unused_result, unused));                                            \
  prefix tree_ ## name ## _t *tree_get_min_ ## name (tree_ ## name ## _t *T) __attribute__ ((unused));                                            \
  prefix void tree_act_ ## name (tree_ ## name ## _t *T, tree_ ## name ## _act_t A) __attribute__ ((unused)); \
  prefix tree_ ## name ## _t *tree_clear_ ## name (tree_ ## name ## _t *T) __attribute__ ((unused));                                            \
  prefix void tree_check_ ## name (tree_ ## name ## _t *T) __attribute__ ((unused));                                            \
  prefix int tree_count_ ## name (tree_ ## name ## _t *T) __attribute__ ((unused));                                            \
  

#define DEFINE_HASH(prefix,name,value_t,value_compare,value_hash) \
  prefix hash_elem_ ## name ## _t *hash_lookup_ ## name (hash_table_ ## name ## _t *T, value_t x) __attribute__ ((unused)); \
  prefix void hash_insert_ ## name (hash_table_ ## name ## _t *T, value_t x) __attribute__ ((unused)); \
  prefix int hash_delete_ ## name (hash_table_ ## name ## _t *T, value_t x) __attribute__ ((unused)); \
  prefix void hash_clear_ ## name (hash_table_ ## name ## _t *T) __attribute__ ((unused)); \
  prefix hash_elem_ ## name ## _t *hash_lookup_ ## name (hash_table_ ## name ## _t *T, value_t x) { \
    long long hash = value_hash (x); if (hash < 0) { hash = -hash; } if (hash < 0) { hash = 0;} \
    if (T->mask) { hash = hash & T->mask;} \
    else { hash %= (T->size);}  \
    if (!T->E[hash]) { return 0; } \
    hash_elem_ ## name ## _t *E = T->E[hash]; \
    do { \
      if (!value_compare (E->x, x)) { return E; } \
      E = E->next; \
    } while (E != T->E[hash]); \
    return 0; \
  } \
  \
  prefix void hash_insert_ ## name (hash_table_ ## name ## _t *T, value_t x) { \
    long long hash = value_hash (x); if (hash < 0) { hash = -hash; } if (hash < 0) { hash = 0;} \
    if (T->mask) { hash = hash & T->mask;} \
    else { hash %= (T->size);}  \
    hash_elem_ ## name ## _t *E = hash_alloc_ ## name (x); \
    if (T->E[hash]) { \
      E->next = T->E[hash]; \
      E->prev = T->E[hash]->prev; \
      E->next->prev = E; \
      E->prev->next = E; \
    } else { \
      T->E[hash] = E; \
      E->next = E; \
      E->prev = E; \
    } \
  } \
  \
  prefix int hash_delete_ ## name (hash_table_ ## name ## _t *T, value_t x) { \
    long long hash = value_hash (x); if (hash < 0) { hash = -hash; } if (hash < 0) { hash = 0;} \
    if (T->mask) { hash = hash & T->mask;} \
    else { hash %= (T->size);}  \
    if (!T->E[hash]) { return 0; } \
    hash_elem_ ## name ## _t *E = T->E[hash]; \
    int ok = 0; \
    do { \
      if (!value_compare (E->x, x)) { ok = 1; break; } \
      E = E->next; \
    } while (E != T->E[hash]); \
    if (!ok) { return 0; } \
    E->next->prev = E->prev; \
    E->prev->next = E->next; \
    if (T->E[hash] != E) { \
      hash_free_ ## name (E); \
    } else if (E->next == E) { \
      T->E[hash] = 0; \
      hash_free_ ## name (E); \
    } else { \
      T->E[hash] = E->next; \
      hash_free_ ## name (E); \
    } \
    return 1; \
  } \
  \
  prefix void hash_clear_ ## name (hash_table_ ## name ## _t *T) { \
    int i; \
    for (i = 0; i < T->size; i++) { \
      if (T->E[i]) { \
        hash_elem_ ## name ## _t *cur = T->E[i]; \
        hash_elem_ ## name ## _t *first = cur; \
        do { \
          void *next = cur->next; \
          hash_free_ ## name (cur); \
          cur = next; \
        } while (cur != first); \
        T->E[i] = 0; \
      } \
    } \
  } \


#define DEFINE_HASH_STD_ALLOC_PREFIX(prefix,name,value_t,value_compare,value_hash)\
  DECLARE_HASH_TYPE(name,value_t) \
  prefix hash_elem_ ## name ## _t *hash_alloc_ ## name (value_t x);                                          \
  prefix void hash_free_ ## name (hash_elem_ ## name ## _t *T);                                                  \
  DEFINE_HASH(prefix,name,value_t,value_compare,value_hash); \
  hash_elem_ ## name ## _t *hash_alloc_ ## name (value_t x) { \
    hash_elem_ ## name ## _t *E = zmalloc (sizeof (*E)); \
    E->x = x; \
    return E; \
  } \
  void hash_free_ ## name (hash_elem_ ## name ## _t *E) { \
    zfree (E, sizeof (*E)); \
  } \

#define DEFINE_HASH_STDNOZ_ALLOC_PREFIX(prefix,name,value_t,value_compare,value_hash)\
  DECLARE_HASH_TYPE(name,value_t) \
  prefix hash_elem_ ## name ## _t *hash_alloc_ ## name (value_t x);                                          \
  prefix void hash_free_ ## name (hash_elem_ ## name ## _t *T);                                                  \
  DEFINE_HASH(prefix,name,value_t,value_compare,value_hash); \
  hash_elem_ ## name ## _t *hash_alloc_ ## name (value_t x) { \
    hash_elem_ ## name ## _t *E = malloc (sizeof (*E)); \
    E->x = x; \
    return E; \
  } \
  void hash_free_ ## name (hash_elem_ ## name ## _t *E) { \
    free (E); \
  } \

#define DEFINE_HASH_STD_ALLOC(name,value_t,value_compare,value_hash) \
  DEFINE_HASH_STD_ALLOC_PREFIX(static,name,value_t,value_compare,value_hash)

#define DEFINE_HASH_STDNOZ_ALLOC(name,value_t,value_compare,value_hash) \
  DEFINE_HASH_STDNOZ_ALLOC_PREFIX(static,name,value_t,value_compare,value_hash)

#define DECLARE_HASH_TYPE(name,value_t) \
  struct hash_elem_ ## name { \
    struct hash_elem_ ## name *next, *prev;\
    value_t x;\
  }; \
  struct hash_table_ ## name {\
    struct hash_elem_ ## name **E; \
    int size; \
    int mask; \
  }; \
  typedef struct hash_elem_ ## name hash_elem_ ## name ## _t; \
  typedef struct hash_table_ ## name hash_table_ ## name ## _t; \


#define DEFINE_QUEUE_PREFIX(prefix,name,value_t) \
  DECLARE_QUEUE(prefix,name,value_t) \
  queue_ ## name ## _t *queue_get_first_ ## name (queue_ ## name ## _t *Q) { \
    return Q; \
  } \
  \
  queue_ ## name ## _t *queue_del_first_ ## name (queue_ ## name ## _t *Q) { \
    assert (Q); \
    queue_ ## name ## _t *R = Q->next; \
    R->prev = Q->prev; \
    R->prev->next = R; \
    zfree (Q, sizeof (queue_ ## name ## _t)); \
    return R == Q ? 0 : R; \
  } \
  \
  queue_ ## name ## _t *queue_add_ ## name (queue_ ## name ## _t *Q, value_t x) { \
    queue_ ## name ## _t *R = zmalloc (sizeof (queue_ ## name ## _t)); \
    R->x = x; \
    R->next = Q ? Q : R; R->prev = Q ? Q->prev : R; \
    R->next->prev = R; \
    R->prev->next = R; \
    return Q ? Q : R; \
  }

#define DECLARE_QUEUE(prefix,name,value_t) \
  struct queue_ ## name {                                                                                     \
    struct queue_ ## name *next, *prev;                                                                      \
    value_t x;                                                                                        \
  };                                                                                                  \
  typedef struct queue_ ## name queue_ ## name ## _t;                                                                     \
  prefix queue_ ## name ## _t *queue_get_first_ ## name (queue_ ## name ## _t *Q) __attribute__ ((unused)); \
  prefix queue_ ## name ## _t *queue_del_first_ ## name (queue_ ## name ## _t *Q) __attribute__ ((warn_unused_result,unused)); \
  prefix queue_ ## name ## _t *queue_add_ ## name (queue_ ## name ## _t *Q, value_t x) __attribute__ ((warn_unused_result,unused)); \

#define DEFINE_QUEUE(name,value_t) \
  DEFINE_QUEUE_PREFIX(static,name,value_t)

#define DEFINE_QUEUE_GLOBAL(name,value_t) \
  DEFINE_QUEUE_PREFIX(,name,value_t)


#define std_int_compare(a,b) ((a) - (b))
#define std_ll_ptr_compare(a,b) ((*(long long *)(a)) - (*(long long *)(b)))
#define std_int_hash(x) ((x) >= 0 ? (x) : -(x) >= 0 ? -(x) : 0)
