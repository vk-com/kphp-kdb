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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 *          Anton  Timofeev    (atimofeev@vkontakte.ru)
 * Created: 16.03.2012
 */

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <assert.h>

#include "string-processing.h"
#include "dl-utils.h"

#include "st-heap.h"
#include "st-hash-map.h"
#include "antispam-common.h"
#include "antispam-db.h"
#include "antispam-db-trie.h"

#define ANSWER_CAPACITY 128
vec_int_t antispam_db_request;

// Stats
static int stats_all_matches_cnt = 0;
static int stats_matches_cnt = 0;
static int stats_matches_sum = 0;
static int stats_matches_max = 0;

#pragma pack(push,4)
typedef struct {
  trie_node_t *trie_node;
  ip_t         ip;
  uahash_t     uahash;

  int          id;
  flags_t      flags;
} pattern_t;
#pragma pack(pop)

#define HASH_PATTERN(p) HASH_TRIPLE ((uintptr_t)(p).trie_node, (p).ip, (p).uahash)

DECLARE_LIST_NEQ (pattern_t*, list_pattern)
DEFINE_LIST_NEQ  (pattern_t*, list_pattern)

DECLARE_HASH_MAP (pattern_t, id_to_pattern);
DEFINE_HASH_MAP  (pattern_t, id_to_pattern);
static id_to_pattern_t id_to_pattern;

DECLARE_HASH_MAP (list_pattern_t*, triple_to_patterns);
DEFINE_HASH_MAP (list_pattern_t*, triple_to_patterns);
static triple_to_patterns_t triple_to_pattern;

DECLARE_VEC (pattern_t *, vec_pattern)
static vec_pattern_t heap_pattern;

typedef char *(*simplifier_t)(const char *);

static char *no_simplify (const char *s) {
  return (char *)s;
}

static trie_node_t *root[SIMPLIFY_TYPE_COUNT];
static simplifier_t simplifiers[SIMPLIFY_TYPE_COUNT] = {no_simplify, sp_simplify, sp_full_simplify};

static int get_simplify_type ( flags_t flags ) {
  if (flags & FLAGS_SIMPLIFY_FULL)
    return SIMPLIFY_TYPE_FULL;
  if (flags & FLAGS_SIMPLIFY_PARTIAL)
    return SIMPLIFY_TYPE_PARTIAL;
  return SIMPLIFY_TYPE_NONE;
}

static trie_node_t *new_trie_node (byte p_ch, trie_node_t *parent) {
  trie_node_t *p = zmalloc (sizeof (trie_node_t));
  if (parent != 0) {
    parent->deg++;
  }
  p->ch = 1;
  p->next = 0; // there are no edges
  p->used = 0;
  p->patterns_cnt = 0;
  p->p_ch = p_ch;
  p->parent = parent;
  p->deg = 0;
  return p;
}

void antispam_init (void) {
  assert (MAX_PATTERN_LEN <= STRING_PROCESS_BUF_SIZE);

  int i;
  for (i = 0; i < SIMPLIFY_TYPE_COUNT; i++) {
    root[i] = new_trie_node (0, 0);
  }

  st_vec_create (heap_pattern, ANSWER_CAPACITY);
  st_vec_create (antispam_db_request, ANSWER_CAPACITY * 2);
}

static void trie_free (trie_node_t* v) {
  if (!v) {
    return;
  }

// type-1 : ch != 0, next != 0  there is only one edge "ch"
  if (v->ch != 0 && v->next != 0) {
    trie_free (v->next);

// type-2 : ch == 0, next != 0  there are many edges
  } else if (v->ch == 0 && v->next != 0) {
    int i;
    trie_node_t **e = v->next;
    for (i = 0; i < 256; ++i) {
      trie_free (e[i]);
    }
    zfree (e, sizeof (trie_node_t *) * 256);
  }
  zfree (v, sizeof (trie_node_t));
}

void antispam_finish (void) {
  triple_to_patterns_foreach (triple_to_pattern, list_pattern_clear);
  id_to_pattern_clear (id_to_pattern);
  triple_to_patterns_clear (triple_to_pattern);

  int i;
  for (i = 0; i < SIMPLIFY_TYPE_COUNT; i++) {
    trie_free (root[i]);
  }

  st_vec_destroy (heap_pattern);
  st_vec_destroy (antispam_db_request);
}

static trie_node_t **get_not_null (trie_node_t *v, byte i) {
  if (v->ch) {
    return (trie_node_t **)&v->next;
  } else {
    return ((trie_node_t **)v->next) + i;
  }
}

// It's useful function. Commented only because of it's not being used now.

/*
static trie_node_t *get (trie_node_t *v, byte i) {
  if (v->ch) {
    return i == v->ch ? (trie_node_t *)v->next : 0;
  } else {
    return ((trie_node_t **)v->next)[i];
  }
}
*/
/*
static void dbg_write_rev_str (trie_node_t *v) {
  putc('\'', stderr);
  for (; v; v = v->parent)
    putc(v->p_ch, stderr);
  putc('\'', stderr);
}
*/

// Suppose that there is no 'p.id' in id-tree
static inline bool add_unsafe (antispam_pattern_t p, const char *s) {
  int simplify_type = get_simplify_type(p.flags);
  sp_init();
  s = simplifiers[simplify_type](s);
  if (!s || (p.ip == 0 && p.uahash == 0 && strlen(s) <= 2)) {
    return FALSE;
  }

  trie_node_t *v = root[simplify_type]; // 0,1,2

  int k;
  //fprintf (stderr, "s = %s\n", s);
  while (*s) {
    byte i = (byte)(*s++);
    if (v->ch == 0) { // many edges
      trie_node_t **e = (trie_node_t **)v->next;
      if (e[i] == 0) {
        e[i] = new_trie_node (i, v);
      }
      v = e[i];
    } else if (v->next == 0) {
      v->ch = i;
      v->next = new_trie_node (i, v);
      v = v->next;
    } else if (v->ch == i) {
      v = v->next;
    } else {
      trie_node_t **e = zmalloc (sizeof (trie_node_t *) * 256);
      for (k = 0; k < 256; k++) {
        e[k] = 0;
      }
      e[v->ch] = v->next;
      e[i] = new_trie_node (i, v);
      v->ch = 0;
      v->next = e;
      v = e[i];
    }
  }

  // Create new pattern (get will always create new one here)
  pattern_t* pattern = id_to_pattern_get (id_to_pattern, p.id);
  pattern->trie_node = v;
  pattern->ip        = p.ip;
  pattern->uahash    = p.uahash;
  pattern->id        = p.id;
  pattern->flags     = p.flags;

  list_pattern_t **head = triple_to_patterns_get (triple_to_pattern, HASH_PATTERN (*pattern));
  list_pattern_add (head, pattern);

  ++v->patterns_cnt;

  return TRUE;
}

static inline void antispam_del_unsafe (int id, pattern_t* pattern) {
  trie_node_t *v = pattern->trie_node;
  --v->patterns_cnt;

  hash_t hash = HASH_PATTERN (*pattern);
  list_pattern_link_t head = triple_to_patterns_find (triple_to_pattern, hash);
  assert (head && list_pattern_delete (head, pattern));
  if (*head == 0) {
    assert (triple_to_patterns_delete (triple_to_pattern, hash));
  }
  assert (id_to_pattern_delete (id_to_pattern, id));

  while (v->parent != 0 && v->deg == 0 && v->patterns_cnt == 0) {
    trie_node_t *p = v->parent;
    *get_not_null (p, v->p_ch) = 0;
    if (--p->deg == 1) {
      trie_node_t *tmp, **e = (trie_node_t **)p->next;
      byte i = 0;
      while (!*e) {
        e++;
        i++;
      }
      tmp = *e;
      zfree (p->next, sizeof (trie_node_t *) * 256);
      p->next = tmp, p->ch = i;
    }
    zfree (v, sizeof (trie_node_t));
    v = p;
  }
}

bool antispam_add (antispam_pattern_t p, const char *s, bool replace) {
  pattern_t *pattern = id_to_pattern_find (id_to_pattern, p.id);
  if (pattern != 0) {
    if (!replace) {
      return FALSE;
    }
    antispam_del_unsafe (p.id, pattern);
  }
  return add_unsafe (p, s);
}
bool antispam_del (int id) {
  pattern_t *pattern = id_to_pattern_find (id_to_pattern, id);
  if (pattern == 0) {
    return FALSE;
  }
  antispam_del_unsafe (id, pattern);
  return TRUE;
}

DEFINE_HEAP_LEQ_PTR_KEY (pattern_t *, heap_dec, id, <=)
DEFINE_HEAP_LEQ_PTR_KEY (pattern_t *, heap_inc, id, >=)

static unsigned long long used_mark = 0;
#define ADD_IDS_FROM_IP_UAHASH_LIST(name, v, ip, uahash) {               \
  list_pattern_t** head = triple_to_patterns_find (                      \
    triple_to_pattern, HASH_TRIPLE ((uintptr_t)v, ip, uahash)            \
  );                                                                     \
  if (head != 0) {                                                       \
    list_pattern_t *p = *head;                                           \
    for (; p; p = p->next) {                                             \
      if (heap_size < limit) {                                           \
        st_vec_pb (heap_pattern, p->x);                                  \
        heap_##name##_up (heap_pattern.first - 1, ++heap_size);          \
      } else {                                                           \
        heap_##name##_update (heap_pattern.first - 1, heap_size, p->x);  \
      }                                                                  \
      ++ans_size;                                                        \
    }                                                                    \
  }                                                                      \
}
#define DEFINE_GET_MATCHES(name)                                                   \
static int antispam_get_matches_##name (ip_t ip, uahash_t uahash,                  \
                                        const char *query_text, int limit) {       \
  assert (limit >= 0);                                                             \
  st_vec_resize (heap_pattern, 0);                                                 \
  /* Secure reasons (moderators have ip = 0) */                                    \
  if (ip == 0 || uahash == 0) {                                                    \
    return 0;                                                                      \
  }                                                                                \
                                                                                   \
  int ans_size = 0, heap_size = 0, simplify_type;                                  \
  assert (++used_mark != 0);                                                       \
  for (simplify_type = 0; simplify_type < SIMPLIFY_TYPE_COUNT; simplify_type++) {  \
    sp_init();                                                                     \
    char *text = simplifiers[simplify_type](query_text);                           \
    if (!text) {                                                                   \
      continue;                                                                    \
    }                                                                              \
    for (; *text; text++) {                                                        \
      const char *s = text;                                                        \
      trie_node_t *v = root[simplify_type];                                        \
      /* printf ("s=%s\n", s); */                                                  \
      while (v) {                                                                  \
        byte i = *s;                                                               \
        /* printf ("s=%s, v=%p\n", s, v); */                                       \
        if (v->patterns_cnt != 0 && v->used != used_mark) {                        \
          v->used = used_mark;                                                     \
                                                                                   \
          ADD_IDS_FROM_IP_UAHASH_LIST(name, v, 0, 0);                              \
          ADD_IDS_FROM_IP_UAHASH_LIST(name, v, 0, uahash);                         \
          ADD_IDS_FROM_IP_UAHASH_LIST(name, v, ip, 0);                             \
          ADD_IDS_FROM_IP_UAHASH_LIST(name, v, ip, uahash);                        \
        }                                                                          \
        if (!i) {                                                                  \
          break;                                                                   \
        }                                                                          \
        if (v->ch == 0) { /* many edges */                                         \
          v = ((trie_node_t **)v->next)[i];                                        \
        } else {          /* only one edge */                                      \
          v = (v->ch == i) ? (trie_node_t *)v->next : 0;                           \
        }                                                                          \
        s++;                                                                       \
      }                                                                            \
    }                                                                              \
  }                                                                                \
  heap_##name##_sort (heap_pattern.first - 1, st_vec_size (heap_pattern));         \
  return ans_size;                                                                 \
}
DEFINE_GET_MATCHES (dec);
DEFINE_GET_MATCHES (inc);

int antispam_get_matches (ip_t ip, uahash_t uahash, const char *text, byte fields, int limit) {
  int matches_cnt = 0;
  if (limit >= 0) {
    matches_cnt = antispam_get_matches_inc (ip, uahash, text, limit);
  } else {
    matches_cnt = antispam_get_matches_dec (ip, uahash, text, -limit);
  }

  st_vec_resize (antispam_db_request, 0);
  pattern_t **first = heap_pattern.first;
  pattern_t **last  = first + st_vec_size (heap_pattern);
  for (; first != last; ++first) {
    if (fields & ANTISPAM_DB_FIELDS_IDS) {
      st_vec_pb (antispam_db_request, (*first)->id);
    }
    if (fields & ANTISPAM_DB_FIELDS_FLAGS) {
      st_vec_pb (antispam_db_request, (*first)->flags);
    }
  }

  ++stats_all_matches_cnt;
  if (matches_cnt != 0) {
    ++stats_matches_cnt;
    stats_matches_sum += matches_cnt;
    st_relax_max (stats_matches_max, matches_cnt);
  }
  return matches_cnt;
}

// Output shouldn't exceed MAX_PATTERN_LEN
bool antispam_serialize_pattern (int id, char *output/*, output_max = MAX_PATTERN_LEN*/) {
  pattern_t *pattern = id_to_pattern_find (id_to_pattern, id);
  if (pattern == 0) {
    return FALSE;
  }
  trie_node_t *v = pattern->trie_node;

  int length = 0; // '\0' exclusive
  trie_node_t const *p = v;
  for (; p->parent != 0; p = p->parent) {
    ++length;
  }
  assert (length + 40 <= MAX_PATTERN_LEN); // all 'output' must be bounded by MAX_PATTERN_LEN

  output += sprintf (output, "%d,%u,%u,%hu,", id, pattern->ip, pattern->uahash, (unsigned short)pattern->flags);

  output += length;
  *output-- = 0;
  for (p = v; p->parent != 0; p = p->parent) {
    *output-- = p->p_ch;
  }
  return TRUE;
}

void antispam_write_engine_stats (char** first, char* last) {
  st_hash_map_stats_t hm1_stats = id_to_pattern_get_stats (id_to_pattern);
  st_hash_map_stats_t hm2_stats = triple_to_patterns_get_stats (triple_to_pattern);

  #define WSL(name, format, value)  \
    assert (last - *first >= 1000); \
    *first += sprintf (*first, "%s\t" format, name, value); \

  #define W_HT_STATS(name, stats)                                                        \
    WSL (#name "_filled", "%.4f%%\n", 100.0 * (stats).chain_len_sum / ST_HASH_SET_SIZE); \
    *first += sprintf (*first, "%s\t%.4f/%d\n", #name"_chains",                          \
      (stats).chain_cnt == 0 ? 0.0 : (double)(stats).chain_len_sum / (stats).chain_cnt,  \
      (stats).chain_len_max);

  WSL ("patterns_cnt",  "%d\n", hm1_stats.chain_len_sum);
  WSL ("matching_probability", "%.4f%%\n", stats_all_matches_cnt == 0 ? 0.0 :
    100.0 * stats_matches_cnt / stats_all_matches_cnt);
  *first += sprintf (*first, "matches_cnt(mean/max)\t%.4f/%d\n",
    stats_matches_cnt == 0 ? 0.0 : stats_matches_sum / stats_matches_cnt,
    stats_matches_max);
  W_HT_STATS (id_to_pattern, hm1_stats);
  W_HT_STATS (triple_to_patterns, hm2_stats);

  #undef WSL
  #undef W_HT_STATS
}
