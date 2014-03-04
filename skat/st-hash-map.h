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
 *          Anton  Timofeev    (atimofeev@vkontakte.ru)
 * Created: 03.04.2012
 */

#pragma once

#include "st-hash.h"
#include "st-list.h"

/**
 * Provides basic hashed mapping functionality.
 *   O(entries_number)
 *   - Init hashmap (need if it was not defined in the global scope):
 *       void <name>_init (<name>_t hm);
 *   - Clear all entries:
 *       void <name>_clear (<name>_t hm);
 *   - Execute 'functor' for each entry's value:
 *       void <name>_foreach   (<name>_t hm, void <name>_functor_t functor);
 *   - Compute statistics:
 *       st_hash_map_stats_t <name>_get_stats (<name>_t hm);
 *
 *   O(chain_length)
 *   - Determine if key (hash) is contained in hashmap:
 *       bool          <name>_contains  (<name>_t hm, hash_t h);
 *   - Find entries' values and links on them by hash:
 *       value_t      *<name>_find      (<name>_t hm, hash_t h);
 *       <name>link_t  <name>_find_link (<name>_t hm, hash_t h);
 *   - Find entry's value, create new one with default_value if not exists:
 *       value_t      *<name>_get       (<name>_t hm, hash_t h);
 *   - Find entry and delete it if search succeeded:
 *       bool          <name>_delete    (<name>_t hm, hash_t h);
 *
 *   O(1)
 *   - Insert/remove entry by link:
 *       void <name>_insert (<name>link_t where, <name>_entry_t entry);
 *       void <name>_remove (<name>link_t where);
 */

typedef struct {
  int chain_cnt;
  int chain_len_sum;
  int chain_len_max;
} st_hash_map_stats_t;

#define DECLARE_HASH_MAP_WITH_SIZE(value_t, name, hm_size)                     \
  typedef struct {                                                             \
    hash_t h;                                                                  \
    value_t value;                                                             \
  } name##_entry_t;                                                            \
  DECLARE_LIST_NEQ_KEY (name##_entry_t, name##_list, hash_t)                   \
  typedef name##_list_t     *name##_t[hm_size];                                \
  typedef name##_list_link_t name##_link_t;                                    \
  typedef void (*name##_functor_t)(value_t *);                                 \
  typedef void (*name##_functor_entry_t)(hash_t h, value_t *);                 \
                                                                               \
  /* O(the size of hash map) */                                                \
  void     name##_init          (name##_t hm);                                 \
  void     name##_clear         (name##_t hm);                                 \
  void     name##_foreach       (name##_t hm, name##_functor_t functor);       \
  void     name##_foreach_entry (name##_t hm, name##_functor_entry_t functor); \
  st_hash_map_stats_t name##_get_stats (name##_t hm);                          \
                                                                               \
  /* O(the length of chain) */                                                 \
  bool            name##_contains   (name##_t hm, hash_t h);                   \
  value_t        *name##_find       (name##_t hm, hash_t h);                   \
  name##_entry_t *name##_find_entry (name##_t hm, hash_t h);                   \
  name##_link_t   name##_find_link  (name##_t hm, hash_t h);                   \
  value_t        *name##_get        (name##_t hm, hash_t h);                   \
  bool            name##_delete     (name##_t hm, hash_t h);                   \
  void            name##_put        (name##_t hm, hash_t h, value_t value);    \
                                                                               \
  /* O(1) */                                                                   \
  void          name##_insert    (name##_link_t where, name##_entry_t entry);  \
  void          name##_insert0   (name##_link_t where, hash_t h);              \
  void          name##_remove    (name##_link_t where);                        \

#define LIST(hm, h, hm_size) hm[h % hm_size]
#define DEFINE_HASH_MAP_WITH_SIZE(value_t, name, hm_size)                      \
  DEFINE_LIST_NEQ_KEY (name##_entry_t, name##_list, hash_t, h)                 \
  void name##_init (name##_t hm) {                                             \
    int i;                                                                     \
    for (i = 0; i < hm_size; i++) {                                            \
      hm[i] = 0;                                                               \
    }                                                                          \
  }                                                                            \
  void name##_clear (name##_t hm) {                                            \
    int i;                                                                     \
    for (i = 0; i < hm_size; i++) {                                            \
      if (hm[i]) {                                                             \
        name##_list##_clear (&hm[i]);                                          \
        assert (hm[i] == 0);                                                   \
      }                                                                        \
    }                                                                          \
  }                                                                            \
  void name##_foreach (name##_t hm, name##_functor_t functor) {                \
    int i;                                                                     \
    for (i = 0; i < hm_size; i++) {                                            \
      name##_list_t *iter = hm[i];                                             \
      for (; iter; iter = iter->next) {                                        \
        functor (&iter->x.value);                                              \
      }                                                                        \
    }                                                                          \
  }                                                                            \
  void name##_foreach_entry (name##_t hm, name##_functor_entry_t functor) {    \
    int i;                                                                     \
    for (i = 0; i < hm_size; i++) {                                            \
      name##_list_t *iter = hm[i];                                             \
      for (; iter; iter = iter->next) {                                        \
        functor (iter->x.h, &iter->x.value);                                   \
      }                                                                        \
    }                                                                          \
  }                                                                            \
  st_hash_map_stats_t name##_get_stats (name##_t hm) {                         \
    st_hash_map_stats_t res = {0, };                                           \
    int i;                                                                     \
    for (i = 0; i < hm_size; i++) {                                            \
      name##_list_t *iter = hm[i];                                             \
      if (iter) {                                                              \
        int len = 0;                                                           \
        for (; iter; iter = iter->next) {                                      \
          ++len;                                                               \
        }                                                                      \
        ++res.chain_cnt;                                                       \
        res.chain_len_sum += len;                                              \
        if (res.chain_len_max < len) {                                         \
          res.chain_len_max = len;                                             \
        }                                                                      \
      }                                                                        \
    }                                                                          \
    return res;                                                                \
  }                                                                            \
  bool name##_contains (name##_t hm, hash_t h) {                               \
    return name##_list_contains (LIST (hm, h, hm_size), h);                    \
  }                                                                            \
  value_t *name##_find (name##_t hm, hash_t h) {                               \
    name##_list_iter_t found = name##_list_find_iter (LIST(hm, h, hm_size), h);\
    return found ? &found->x.value : 0;                                        \
  }                                                                            \
  name##_entry_t *name##_find_entry (name##_t hm, hash_t h) {                  \
    name##_list_iter_t found = name##_list_find_iter (LIST(hm, h, hm_size), h);\
    return found ? &found->x : 0;                                              \
  }                                                                            \
  name##_link_t name##_find_link (name##_t hm, hash_t h) {                     \
    return name##_list_find_link (&LIST (hm, h, hm_size), h);                  \
  }                                                                            \
  value_t *name##_get (name##_t hm, hash_t h) {                                \
    name##_link_t ln = name##_list_find_link(&LIST (hm, h, hm_size), h);       \
    if (!*ln) {                                                                \
      name##_list_add0 (ln);                                                   \
      (*ln)->x.h = h;                                                          \
    }                                                                          \
    return &(*ln)->x.value;                                                    \
  }                                                                            \
  void name##_put (name##_t hm, hash_t h, value_t value) {                     \
    *name##_get (hm, h) = value;                                               \
  }                                                                            \
  bool name##_delete (name##_t hm, hash_t h) {                                 \
    name##_link_t ln = name##_list_find_link (&LIST (hm, h, hm_size), h);      \
    if (!*ln) {                                                                \
      return FALSE;                                                            \
    }                                                                          \
    name##_list_remove (ln);                                                   \
    return TRUE;                                                               \
  }                                                                            \
  void name##_insert (name##_link_t where, name##_entry_t entry) {             \
    name##_list_add (where, entry);                                            \
  }                                                                            \
  void name##_insert0 (name##_link_t where, hash_t h) {                        \
    name##_list_add0 (where);                                                  \
    (*where)->x.h = h;                                                         \
  }                                                                            \
  void name##_remove (name##_link_t where) {                                   \
    name##_list_remove (where);                                                \
  }                                                                            \

#define DECLARE_HASH_MAP(value_t, name) \
  DECLARE_HASH_MAP_WITH_SIZE (value_t, name, ST_HASH_SET_SIZE)
#define DEFINE_HASH_MAP(value_t, name)  \
  DEFINE_HASH_MAP_WITH_SIZE  (value_t, name, ST_HASH_SET_SIZE)
#define DECDEF_HASH_MAP(value_t, name)  \
  DECLARE_HASH_MAP (value_t, name)      \
  DEFINE_HASH_MAP (value_t, name)
