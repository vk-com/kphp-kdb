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

    Copyright 2012 Vkontakte Ltd
              2012 Sergey Kopeliovich <Burunduk30@gmail.com>
              2012 Anton Timofeev <atimofeev@vkontakte.ru>
*/

/**
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 *          Anton  Timofeev    (at1.030@gmail.com)
 * Created: 16.03.2012
 */

#pragma once

#include <assert.h>

#include "kdb-data-common.h" // zmalloc, zfree
#include "st-utils.h"        // bool

/**
 * Provides basic lists functionality, sequence of nodes:
 *   name##_t   node ::= (value_t x, node *next);
 *   (where 'name' is specific to each use case)
 * Consider that every node have unique parent pointer (called link):
 *   name##_t** link ::= &parent->next | &list_head_pointer
 * But one could have a lot of pointers to a node (called iter)
 *   name##_t*  iter ::= pointer to node
 * Iterators become incorrect after any operation that changes list topology
 * and must be used for read only list access (of course, you still may change
 * list values, i.e. iter->x and iterate over nodes).

 * To create empty list use:
 *   name##_t* head = 0; // So, &head is a link to the newly created list.

 * Use DECLARE_LIST(value_t, name) to declare specific list type,
 * and DEFINE_LIST (value_t, name) to define it's implementation.
 *
 * To define implementation of functions which require inequality operator (as !=)
 * on it's values 'value_t x' one could use extended versions of list definitions:
 * 1. DECLARE_LIST_NEQ (value_t, name)                 // compare elements as x1 != x2, (key ::= x)
 *    DEFINE_LIST_NEQ  (value_t, name)
 *
 * 2. DECLARE_LIST_NEQ_KEY (value_t, name, key_t)      // compare elements as x1.key != x2.key
 *    DEFINE_LIST_NEQ_KEY  (value_t, name, key_t, key)
 *
 * 3. DECLARE_LIST_NEQ_PTR_KEY (value_t, name, key_t)  // compare elements as x1->key != x2->key
 *    DEFINE_LIST_NEQ_PTR_KEY  (value_t, name, key_t, key)
 *
 * 4. DECLARE_LIST_NEQ_FUNC (value_t, name)            // compare elements as neq (x1, x2)
 *    DEFINE_LIST_NEQ_FUNC  (value_t, name, neq)

 * Remark from Sergey:
 *   All functions are implemented without recursion to have no STACK OVERFLOW.
 * Thank you Cap!
 */

#define DECLARE_LIST_IMPL(value_t, name)                               \
  typedef struct name name##_t;                                        \
  struct name {                                                        \
    value_t x;                                                         \
    name##_t *next;                                                    \
  };                                                                   \
  typedef name##_t  *name##_iter_t;                                    \
  typedef name##_t **name##_link_t;                                    \
  void          name##_clear     (name##_link_t first);                \
  void          name##_add       (name##_link_t where, value_t x);     \
  void          name##_add0      (name##_link_t where);                \
  void          name##_remove    (name##_link_t where);                \
  int           name##_size      (name##_iter_t first);                \

#define DECLARE_LIST_NEQ_IMPL(value_t, name, key_t)                    \
  DECLARE_LIST_IMPL (value_t, name)                                    \
  bool          name##_contains  (name##_iter_t first, key_t key);     \
  int           name##_count     (name##_iter_t first, key_t key);     \
  value_t*      name##_find_x    (name##_iter_t first, key_t key);     \
  name##_iter_t name##_find_iter (name##_iter_t first, key_t key);     \
  name##_link_t name##_find_link (name##_link_t first, key_t key);     \
  bool          name##_delete    (name##_link_t first, key_t key);     \

#define DEFINE_LIST_IMPL(value_t, name)                                \
  void name##_clear (name##_t **first) {                               \
    name##_t *it = *first;                                             \
    while (it) {                                                       \
      name##_t *next = it->next;                                       \
      zfree (it, sizeof (name##_t));                                   \
      it = next;                                                       \
    }                                                                  \
    *first = 0;                                                        \
  }                                                                    \
  void name##_add (name##_t **where, value_t x) {                      \
    name##_t *new_el = zmalloc (sizeof (name##_t));                    \
    new_el->next = *where;                                             \
    new_el->x = x;                                                     \
    *where = new_el;                                                   \
  }                                                                    \
  void name##_add0 (name##_t **where) {                                \
    name##_t *new_el = zmalloc0 (sizeof (name##_t));                   \
    new_el->next = *where;                                             \
    *where = new_el;                                                   \
  }                                                                    \
  void name##_remove (name##_t **where) {                              \
    /* assert (where && *where); */                                    \
    name##_t *to_del = *where;                                         \
    *where = (*where)->next;                                           \
    zfree (to_del, sizeof (name##_t));                                 \
  }                                                                    \
  int name##_size (name##_t *first) {                                  \
    int size = 0;                                                      \
    for (; first; first = first->next) {                               \
      ++size;                                                          \
    }                                                                  \
    return size;                                                       \
  }                                                                    \

// Implementation of DEFINE_LIST_NEQ_* macrosses.
#define DEFINE_LIST_NEQ_IMPL(value_t, name, key_t, lkey, neq, op)      \
  DEFINE_LIST_IMPL (value_t, name)                                     \
  bool name##_contains (name##_t *first, key_t key) {                  \
    while (first && (neq (first->lkey op key))) {                      \
      first = first->next;                                             \
    }                                                                  \
    return first != 0;                                                 \
  }                                                                    \
  int name##_count (name##_t *first, key_t key) {                      \
    int count = 0;                                                     \
    for (; first; first = first->next) {                               \
      if (!(neq (first->lkey op key))) {                               \
        ++count;                                                       \
      }                                                                \
    }                                                                  \
    return count;                                                      \
  }                                                                    \
  name##_t **name##_find_link (name##_t **first, key_t key) {          \
    while ((*first) && (neq ((*first)->lkey op key))) {                \
      first = &(*first)->next;                                         \
    }                                                                  \
    return first;                                                      \
  }                                                                    \
  name##_t *name##_find_iter (name##_t *first, key_t key) {            \
    while (first && (neq (first->lkey op key))) {                      \
      first = first->next;                                             \
    }                                                                  \
    return first;                                                      \
  }                                                                    \
  value_t* name##_find_x (name##_t *first, key_t key) {                \
    first = name##_find_iter (first, key);                             \
    return first ? &first->x : 0;                                      \
  }                                                                    \
  bool name##_delete (name##_t **first, key_t key) {                   \
    first = name##_find_link (first, key);                             \
    name##_t *to_del = *first;                                         \
    if (to_del == 0) {                                                 \
      return FALSE;                                                    \
    }                                                                  \
    *first = (*first)->next;                                           \
    zfree (to_del, sizeof (name##_t));                                 \
    return TRUE;                                                       \
  }                                                                    \

#define DECLARE_LIST(value_t, name)                                    \
  DECLARE_LIST_IMPL (value_t, name)
#define DECLARE_LIST_NEQ(value_t, name)                                \
  DECLARE_LIST_NEQ_IMPL (value_t, name, value_t)
#define DECLARE_LIST_NEQ_KEY(value_t, name, key_t)                     \
  DECLARE_LIST_NEQ_IMPL (value_t, name, key_t)
#define DECLARE_LIST_NEQ_PTR_KEY(value_t, name, key_t)                 \
  DECLARE_LIST_NEQ_IMPL (value_t, name, key_t)
#define DECLARE_LIST_NEQ_FUNC(value_t, name)                           \
  DECLARE_LIST_NEQ_IMPL (value_t, name, value_t)

#define DEFINE_LIST(value_t, name)                                     \
  DEFINE_LIST_IMPL (value_t, name)
#define DEFINE_LIST_NEQ(value_t, name)                                 \
  DEFINE_LIST_NEQ_IMPL (value_t, name, value_t, x, , !=)
#define DEFINE_LIST_NEQ_KEY(value_t, name, key_t, key)                 \
  DEFINE_LIST_NEQ_IMPL (value_t, name, key_t, x.key, , !=)
#define DEFINE_LIST_NEQ_PTR_KEY(value_t, name, key_t, key)             \
  DEFINE_LIST_NEQ_IMPL (value_t, name, key_t, x->key, , !=)
#define SKAT_LIST_COMMA ,
#define DEFINE_LIST_NEQ_FUNC(value_t, name, neq)                       \
  DEFINE_LIST_NEQ_IMPL (value_t, name, value_t, x, neq, SKAT_LIST_COMMA);
