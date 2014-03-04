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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
                   2013 Anton Maydell
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
#include <sys/uio.h>
#include <aio.h>
#include <byteswap.h>
#include <math.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "net-connections.h"
#include "net-aio.h"
#include "kdb-targ-binlog.h"
#include "targ-data.h"
#include "targ-index.h"
#include "targ-search.h"
#include "targ-index-layout.h"
#include "targ-weights.h"
#include "word-split.h"
#include "translit.h"
#include "server-functions.h"
#include "stemmer.h"
#include "crc32.h"
#include "listcomp.h"

#define  zfree(a,b)	assert((char *)(a) >= dyn_first && (b) >= 0 && ((char *) (a)) + (b) <= dyn_cur);
#define  dyn_free(a,b,__c)	assert(__c==8 && (char *)(a) >= dyn_first && (b) >= 0 && ((char *) (a)) + (b) <= dyn_cur);
#undef	zfree
#undef	dyn_free

#define	zmalloc	zmalloc0
#undef	zmalloc

#define	u_malloc(s)	malloc(s)
#define	u_free(p,s)	free(p)

#define RETARGET_AD_TIMEOUT 3600

int index_mode;

int active_ad_nodes, inactive_ad_nodes, clicked_ad_nodes;

//struct aio_connection *WaitAio;

user_t *User[MAX_USERS];
int UserRate[MAX_USERS];

//by KOTEHOK 2010-04-24: added rate_tree & online_lists; UserOnline & UserIP
//removed (they were just assigned but never used)
utree_t *rate_tree;		// key = rating, from lowest to highest

struct olist_head OHead[OLIST_COUNT];
int ocur_ptr, ocur_now;

int ocntT[OLIST_COUNT * 2];	// segment tree of Online counters

struct advert *Ads[AD_TABLE_SIZE];
unsigned char AncientAdBitmap[MAX_ADS/8 + 1];

struct advert_head AHd_retarget = {.first = (struct advert *) &AHd_retarget, .last = (struct advert *) &AHd_retarget, .ad_id = -1};
struct advert_head AHd_lru = {.first = (struct advert *) &AHd_lru, .last = (struct advert *) &AHd_lru, .ad_id = -2};

struct targ_index_view_stats AdStats;

/* total_clicks = total_sump0; gml = "global" - "local"; total_sump* are "global"! */
double total_sump1, total_sump2, gml_total_sump1, gml_total_sump2;
long long total_sump0, gml_total_sump0;

int tot_users, max_uid;
int tot_groups, max_group_id, min_group_id;
int tot_ads, tot_ad_versions, active_ads, max_ad_id;

int tot_langs, max_lang_id, min_lang_id;
long long user_group_pairs, user_lang_pairs;
long long tot_views, tot_clicks, tot_click_money;

int tot_userlists;
long long tot_userlists_size;

int user_creations;

int targeting_disabled;
//int max_retarget_ad_id = 0x7fffffff;
int debug_user_id;

int R_position;

extern int now;
extern int verbosity;

int use_aio;
int delay_targeting = 1;

static void user_clear_interests (user_t *U, int type);
static void user_clear_education (user_t *U);
static void user_clear_schools (user_t *U);
static void user_clear_work (user_t *U);
static void user_clear_addresses (user_t *U);
static void user_clear_military (user_t *U);
static void user_clear_groups (user_t *U);
static void user_clear_langs (user_t *U);
static void exact_strfree (char *str);

/* ------ ads retargeting list handle ------ */

inline void remove_queue_ad (struct advert *A) {
  if (A->next) {
    A->next->prev = A->prev;
    A->prev->next = A->next;
    A->next = A->prev = 0;
  }
}


inline void insert_queue_ad_before (struct advert *W, struct advert *A) {
  A->next = W;
  A->prev = W->prev;
  W->prev->next = A;
  W->prev = A;
}


inline void insert_retarget_ad_last (struct advert *A) {
  insert_queue_ad_before ((struct advert *)&AHd_retarget, A);
}


inline void reinsert_retarget_ad_last (struct advert *A) {
  remove_queue_ad (A);
  insert_retarget_ad_last (A);
}

inline void insert_lru_ad_last (struct advert *A) {
  insert_queue_ad_before ((struct advert *) &AHd_lru, A);
}


inline void reinsert_lru_ad_last (struct advert *A) {
  remove_queue_ad (A);
  insert_lru_ad_last (A);
}


/* --------- uplink tree (rate tree) ---------- */
//may be rewrite with NIL fake element to eliminate NULL checks?

static void utree_split (utree_t **L, utree_t **R, utree_t *T, int x) {
  if (!T) { *L = *R = 0; return; }
  if (x < T->x) {
    *R = T;
    utree_split (L, &T->left, T->left, x);
    if (T->left) {
      T->left->uplink = T;
    }
  } else {
    *L = T;
    utree_split (&T->right, R, T->right, x);
    if (T->right) {
      T->right->uplink = T;
    }
  }
}


static utree_t *utree_merge (utree_t *L, utree_t *R) {
  if (!L) { return R; }
  if (!R) { return L; }
  if (L->y > R->y) {
    L->right = utree_merge (L->right, R);
    if (L->right) {
      L->right->uplink = L;
    }
    return L;
  } else {
    R->left = utree_merge (L, R->left);
    if (R->left) {
      R->left->uplink = R;
    }
    return R;
  }
}

static utree_t *utree_insert_node (utree_t *T, utree_t *P) {
  if (!T) {
    return P;
  }
  if (T->y >= P->y) {
    if (P->x < T->x) {
      T->left = utree_insert_node (T->left, P);
      if (T->left) {
        T->left->uplink = T;
      }
    } else {
      T->right = utree_insert_node (T->right, P);
      if (T->right) {
        T->right->uplink = T;
      }
    }
    return T;
  }
  utree_split (&P->left, &P->right, T, P->x);
  if (P->left) {
    P->left->uplink = P;
  }
  if (P->right) {
    P->right->uplink = P;
  }
  return P;
}

/* --------- online segment tree --------- */

static inline int online_get_interval (int l, int r) {
  l += OLIST_COUNT;
  r += OLIST_COUNT;

  int res = 0;

  while (l <= r) {
    if (l & 1) {
      res += ocntT[l++];
    }
    if (!(r & 1)) {
      res += ocntT[r--];
    }
    l >>= 1;
    r >>= 1;
  }

  return res;
}


static inline void online_increment (int p, int val) {
  p += OLIST_COUNT;
  assert (ocntT[p] + val >= 0);
  while (p) {
    ocntT[p] += val;
    p >>= 1;
  }
}


static inline void online_assign (int p, int val) {
  online_increment (p, val - ocntT[OLIST_COUNT + p]);
}

static inline int online_get_cyclic_interval (int a, int b) {
  if (a <= b) {
    return online_get_interval (a, b);
  } else {
    return ocntT[1] - online_get_interval (b + 1, a - 1);
  }
}

static inline int is_valid_online_stamp (int ts) {
  return ocur_now && ts > ocur_now - OLIST_COUNT;
}

static inline int online_convert_time (int ts) {
  assert (ocur_now && is_valid_online_stamp (ts));
  if (ts > ocur_now) {
    return ocur_ptr;
  }
  int res = ts - ocur_now + ocur_ptr;
  if (res < 0) {
    res += OLIST_COUNT;
  }
  assert (res >= 0 && res < OLIST_COUNT);
  return res;
}

/* --------- online linked list --------- */

static inline void online_list_init () {
  int i;
  for (i = 0; i < OLIST_COUNT; i++) {
    OHead[i].first = OHead[i].last = (olist_t *)&OHead[i];
  }
}


static inline void online_list_add_after (olist_t *where, olist_t *what) {
  what->prev = where;
  what->next = where->next;
  what->next->prev = what;
  where->next = what;
}


static inline void online_list_remove (olist_t *what) {
  what->next->prev = what->prev;
  what->prev->next = what->next;
}


static inline olist_t *user_to_olist (user_t *user) {
  return (olist_t *)((char *)(user) + offsetof (struct user, online_next));
}


static inline void online_list_clear (olist_t *head) {
  olist_t *tmp = head->next;
  while (tmp != head) {
    olist_t *tnext = tmp->next;
    tmp->next = tmp->prev = 0;
    tmp = tnext;
  }
  head->next = head->prev = head;
}

static inline int online_list_unpack (int *A, olist_t *head) {
  olist_t *tmp = head->next;
  int *PA = A;
  while (tmp != head) {
    *A++ = tmp->uid;
    tmp = tmp->next;
  }
  return A - PA;
}

int online_interval_unpack (int *A, int bt, int et) {
  if (bt > et || bt > ocur_now) {
    return 0;
  }
  int i;
  int *PA = A;
  int bp = online_convert_time (bt);
  int ep = online_convert_time (et);
  if (bp <= ep) {
    for (i = bp; i <= ep; i++) {
      A += online_list_unpack (A, (olist_t *)&OHead[i]);
    }
  } else {
    for (i = bp; i < OLIST_COUNT; i++) {
      A += online_list_unpack (A, (olist_t *)&OHead[i]);
    }
    for (i = 0; i <= ep; i++) {
      A += online_list_unpack (A, (olist_t *)&OHead[i]);
    }
  }
  return A - PA;
}

static void online_advance_now () {
   if (now < ocur_now) {
     return;
   }

   if (!ocur_now) {
     ocur_now = now;
     return;
   }

   
   if (now - ocur_now >= OLIST_COUNT) {
     int i;
     for (i = 0; i < OLIST_COUNT; i++) {
       online_list_clear ((olist_t *)&OHead[i]);
     }
     memset (ocntT, 0, sizeof (ocntT));
     ocur_ptr = 0;
   } else {
     int adv = now - ocur_now;
     while (adv) {
       ++ocur_ptr;
       if (ocur_ptr >= OLIST_COUNT) {
         ocur_ptr = 0;
       }
       online_list_clear ((olist_t *)&OHead[ocur_ptr]);
       online_assign (ocur_ptr, 0);
       --adv;
     }
   }
   ocur_now = now;
}

void user_online_tree_insert (user_t *U) {
  if (is_valid_online_stamp (U->last_visited)) {
    int p = online_convert_time (U->last_visited);
    online_list_add_after ((olist_t *)&OHead[p], user_to_olist (U));
    ocntT[p + OLIST_COUNT]++;
  }
}

void relax_online_tree (void) {
  long i;
  for (i = OLIST_COUNT - 1; i > 0; i--) {
    ocntT[i] = ocntT[2 * i] + ocntT[2 * i + 1];
  }
}


/* --------- word hash/tree --------- */

int hash_word_nodes;

struct hash_word *Hash[HASH_BUCKETS];

struct hash_word *get_hash_node (hash_t word, int force) {
  int t = (unsigned) word & (HASH_BUCKETS - 1);
  struct hash_word *p = Hash[t];
  while (p) {
    if (p->word == word) {
      return p;
    }
    p = p->next;
  }
  if (!force) {
    return 0;
  }
  hash_word_nodes++;
  p = zmalloc0 (sizeof (struct hash_word));
  p->word = word;
  p->next = Hash[t];
  Hash[t] = p;
  return p;
};

/*int get_word_count (hash_t word) {
  struct hash_word *W = get_hash_node (word, 0);
  return W ? W->num : 0;
  }*/

/* for word lists without multiplicities, exact result returned */
int get_word_count_nomult (hash_t word) {
  struct hash_word *W = get_hash_node (word, 0);
  return get_idx_word_list_len (word) + (W ? W->sum : 0);
}

/* for word lists with multiplicities, upper bound returned */
int get_word_count_upperbound (hash_t word, int *is_exact) {
  struct hash_word *W = get_hash_node (word, 0);
  int a = get_idx_word_list_len (word);
  int b = (W ? W->num : 0);
  *is_exact = (!a || !b);
  return a + b;
}

void add_user_word (int uid, hash_t word) {
  struct hash_word *W = get_hash_node (word, 1);
  W->word_tree = intree_incr_z (WordSpace, W->word_tree, uid, 1, &W->num);
  ++W->sum;
}

void delete_user_word (int uid, hash_t word) {
  struct hash_word *W = get_hash_node (word, 1);
  //!!! ASSERT there is enough occurences of such word in index (needs additional feedback from intree_incr_z?)
  W->word_tree = intree_incr_z (WordSpace, W->word_tree, uid, -1, &W->num);
  --W->sum;
}

void delete_user_hashlist (int uid, hash_list_t *H) {
  int i;
  if (!H || H->len <= 0) { return; }
  for (i = 0; i < H->len; i++) {
    delete_user_word (uid, H->A[i]);
  }
  dyn_free (H, 8 + H->len * sizeof(hash_t), 8);
}

void add_user_hashlist (int uid, hash_list_t *H) {
  int i;
  if (!H || H->len <= 0) { return; }
  for (i = 0; i < H->len; i++) {
    add_user_word (uid, H->A[i]);
  }
}

inline void user_change_field (int uid, int field_id, int old_value, int new_value) {
  if (old_value != 0) {
    delete_user_word (uid, field_value_hash (field_id, old_value));
  }
  if (new_value != 0) {
    add_user_word (uid, field_value_hash (field_id, new_value));
  }
}

inline void user_add_field (int uid, int field_id, int new_value) {
  if (new_value != 0) {
    add_user_word (uid, field_value_hash (field_id, new_value));
  }
}

inline void user_clear_field (int uid, int field_id, int old_value) {
  if (old_value != 0) {
    delete_user_word (uid, field_value_hash (field_id, old_value));
  }
}

/* --------- users data ------------- */

int targ_replay_logevent (struct lev_generic *E, int size);

static char *filter_simple_text (char *to, const char *text, int len);

int init_targ_data (int schema) {

  replay_logevent = targ_replay_logevent;

  memset (UserRate, -1, sizeof(UserRate));

  online_list_init ();

  tot_ads = tot_ad_versions = active_ads = 0;
  tot_users = max_uid = 0;
  tot_groups = max_group_id = min_group_id = 0;
  user_group_pairs = 0;

  return 0;
}

static int targ_le_start (struct lev_start *E) {
  if (E->schema_id != TARG_SCHEMA_V1) {
    return -1;
  }
  if (!log_split_mod) {
    log_split_min = E->split_min;
    log_split_max = E->split_max;
    log_split_mod = E->split_mod;
    assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);
  }

  return 0;
}

int conv_user_id (int user_id) {
  if (user_id <= 0) { return -1; }
  if (user_id % log_split_mod != log_split_min) { return -1; }
  user_id /= log_split_mod;
  return user_id < MAX_USERS ? user_id : -1;
}

user_t *get_user (int user_id) {
  int i = conv_user_id (user_id);
  return i >= 0 ? User[i] : 0;
}

user_t *get_user_f (int user_id) {
  int i = conv_user_id (user_id);
  user_t *U;
  if (i < 0) { return 0; }
  U = User[i];
  if (U) { return U; }
  U = zmalloc (sizeof (user_t));
  memset (U, 0, sizeof(user_t));
  U->rate = (lrand48 () & 255) - 256;
  U->uid = i;
  U->user_id = user_id;
  U->privacy = 1;
  U->prev_user_creations = ++user_creations;
  User[i] = U;
  if (i > max_uid) { max_uid = i; }
  tot_users++;
  //by KOTEHOK 2010-04-24
  U->cartesian_y = lrand48 ();
  rate_tree = utree_insert_node (rate_tree, (utree_t *)U);
  rate_tree->uplink = (utree_t *)&rate_tree;
  //end KOTEHOK 2010-04-24
  return U;
}

void rate_change (user_t *U, int new_rate) {
  if (!U) {
    return;
  }

  utree_t *new_tree = utree_merge (U->left_rate, U->right_rate);
  
  if (new_tree) {
    new_tree->uplink = U->uplink_rate;
  }

  //here we require "left" to be the first field of user_t
  //otherwise it will fail at root (be careful with this trick!)
  if (U->uplink_rate->left == (utree_t *)U) {
    U->uplink_rate->left = new_tree;
  } else {
    U->uplink_rate->right = new_tree;
  }

  U->rate = (new_rate << 8) + (lrand48 () & 255);

  U->left_rate = U->right_rate = U->uplink_rate = 0;

  rate_tree = utree_insert_node (rate_tree, (utree_t *)U);
  rate_tree->uplink = (utree_t *)&rate_tree;
  UserRate[U->uid] = new_rate;
}

struct advert *get_ad_f (int ad_id, int force) {
  struct advert *A;
  if ((unsigned) ad_id >= MAX_ADS || !ad_id) { 
    return 0; 
  }
  A = get_ad (ad_id);
  if (A || !force) { 
    return A; 
  }
  A = zmalloc (sizeof (struct advert));
  if (!A) { 
    return A; 
  }
  memset (A, 0, sizeof(*A));
  if (ad_id > max_ad_id) {
    max_ad_id = ad_id;
  }
  tot_ads++;
  if (ad_is_ancient (ad_id)) {
    AncientAdBitmap[ad_id >> 3] &= ~(1 << (ad_id & 7));
    A->flags = ADF_ANCIENT | ADF_NOTLOADED;
    ancient_ads_pending++;
  }
  vkprintf (4, "created ad id %d\n", ad_id);
  A->ad_id = ad_id;
  A->hash_next = Ads[ad_id & (AD_TABLE_SIZE - 1)];
  Ads[ad_id & (AD_TABLE_SIZE - 1)] = A;
  A->factor = 1.0;
  A->recent_views_limit = 0xffff;
  tot_ad_versions++;
  return A;
}

int prepare_words_hashlist (const char *str, int flags, int pattern, int translit, int type);
hash_list_t *save_words_hashlist_pattern (const char *str, int flags, int pattern, int translit, int type);
inline hash_list_t *save_words_hashlist (const char *str, int flags, int type);
static void rehash_user_interests (user_t *U);

int account_user_online (user_t *U) {
  if (!U) {
    return 0;
  }

  if (verbosity > 2) {
    fprintf (stderr, "setting last_visited for user %d to %d\n", U->user_id, now);
  }

  online_advance_now ();

  if (U->online_next) {
    online_list_remove (user_to_olist (U));
    if (verbosity > 2) {
      fprintf (stderr, "last_visited was %d, convert was %d\n", U->last_visited, online_convert_time (U->last_visited));
    }
    online_increment (online_convert_time (U->last_visited), -1);
  }

  if (is_valid_online_stamp (now)) {
    int p = online_convert_time (now);
    online_list_add_after ((olist_t *)&OHead[p], user_to_olist (U));
    if (verbosity > 2) {
      fprintf (stderr, "new convert is %d\n", p);
    }
    online_increment (p, +1);
  } else {
    U->online_next = U->online_prev = 0;
  }

  U->last_visited = now;

  return 1;
}


void process_user_online_lite (struct lev_online_lite *E) {
  user_t *U = get_user (E->user_id);
  if (U) {
    account_user_online (U);
  }
}

/* -------- cyclic views buffer --------- */

struct cyclic_views_entry CViews[CYCLIC_VIEWS_BUFFER_SIZE], *CV_r = CViews, *CV_w = CViews;

static inline void forget_view (struct cyclic_views_entry *CV) {
  struct advert *A = get_ad (CV->ad_id);
  assert (!A || --A->recent_views >= 0); 
}

void forget_one_view (void) {
  forget_view (CV_r);
  if (++CV_r == CViews + CYCLIC_VIEWS_BUFFER_SIZE) {
    CV_r = CViews;
  }
}

struct cyclic_views_entry *forget_old_views_upto (struct cyclic_views_entry *CV, int cutoff_time) {
  while (CV != CV_w) {
    if (CV->time > cutoff_time) {
      return CV;
    }
    forget_view (CV);
    if (++CV == CViews + CYCLIC_VIEWS_BUFFER_SIZE) {
      CV = CViews;
      break;
    }
  }
  while (CV < CV_w) {
    if (CV->time > cutoff_time) {
      return CV;
    }
    forget_view (CV);
    CV++;
  }
  return CV;
}

void forget_old_views (void) {
  CV_r = forget_old_views_upto (CV_r, log_last_ts - VIEWS_STATS_INTERVAL);
}

void register_one_view (struct advert *A, int user_id) {
  struct cyclic_views_entry *CV = CV_w;
  CV->ad_id = A->ad_id;
  CV->user_id = user_id;
  CV->time = log_last_ts;
  A->recent_views++;
  if (++CV_w == CViews + CYCLIC_VIEWS_BUFFER_SIZE) {
    CV_w = CViews;
  }
  if (CV_w == CV_r) {
    forget_one_view ();
  }
  forget_old_views ();
}

static inline int subtract_CV (struct cyclic_views_entry *CV_start, struct cyclic_views_entry *CV_end) {
  long t = CV_end - CV_start;
  return t >= 0 ? t : t + CYCLIC_VIEWS_BUFFER_SIZE;
}
 
int get_recent_views_num (void) {
  return subtract_CV (CV_r, CV_w);
}

/* --- user ad/click/view functions --- */

/*
static int user_insert_ad (int user_id, int ad_id, int ad_views) {
  if (user_id < 0 || user_id >= MAX_USERS || !ad_id) {
    return 0;
  }
  user_t *U = get_user (user_id);
  assert (U);

  if (intree_lookup (AdSpace, U->clicked_ads, ad_id)) {
    return 0;
  }

  if (intree_lookup (AdSpace, U->active_ads, ad_id)) {
    return 1;
  }

  treeref_t N;
  U->inactive_ads = intree_remove (AdSpace, U->inactive_ads, ad_id, &N);

  if (!N) {
    N = new_intree_node (AdSpace);
    TNODE(AdSpace,N)->x = ad_id;
    TNODE(AdSpace,N)->z = ad_views;
  }

  U->active_ads = intree_insert (AdSpace, U->active_ads, N);

  user_ad_pairs++;
}
*/

static int HN, __gsort_limit, __use_factor, __use_views_limit, __exclude_ad_id, __build_heap_multiplier;
static long long __build_heap_generation;
static int __and_mask = (254 << ADF_SITES_MASK_SHIFT), __xor_mask = 0;
static long long __cat_mask = -1LL;

struct heap_entry {
  int ad_id;
  int views;
  int domain;
  float expected_gain;
};


struct hash_entry {
  int heapref, reserved;
  long long generation;
};


#define HASH_BITS	8
#define HASH_BITS_POWER	(1 << HASH_BITS)

static struct heap_entry H[GSORT_HEAP_SIZE];
static struct hash_entry HS[HASH_BITS_POWER];


static inline int heap_sift_down (int i, float expected_gain) { 
  int j;
  while (1) {
    j = i * 2;
    if (j > HN) {
      break;
    }
    if (j < HN && H[j + 1].expected_gain < H[j].expected_gain) {
      j++;
    }
    if (H[j].expected_gain >= expected_gain) {
      break;
    }
    if (H[j].domain >= 0) {
      HS[H[j].domain].heapref = i;
    }
    H[i] = H[j];
    i = j;
  }
  return i;
}

static inline int heap_sift_up (float expected_gain) { 
  int i = ++HN, j;
  while (i > 1) {
    j = (i >> 1);
    if (H[j].expected_gain <= expected_gain) {
      break;
    }
    if (H[j].domain >= 0) {
      HS[H[j].domain].heapref = i;
    }
    H[i] = H[j];
    i = j;
  }
  return i;
}


static inline unsigned calc_domain_hash (int domain) {
  unsigned mval = domain * __build_heap_multiplier;
  return mval >> (32 - HASH_BITS);
}


struct heap_entry *heap_insert (float expected_gain, struct advert *A, int views) {
  int i = 0;
  if (__use_factor) {
    expected_gain *= A->factor;
  }

  if (A->domain) {
    int hval = calc_domain_hash (A->domain);
    if (HS[hval].generation == __build_heap_generation) {
      i = HS[hval].heapref;
      if (H[i].expected_gain >= expected_gain) {
	return 0;
      }
      i = heap_sift_down (i, expected_gain);
    }
  }

  if (!i) {
    if (HN == __gsort_limit) {
      if (H[1].expected_gain >= expected_gain) {
	return 0;
      }
      if (H[1].domain >= 0) {
	--HS[H[1].domain].generation;
      }
      i = heap_sift_down (1, expected_gain);
    } else {
      i = heap_sift_up (expected_gain);
    }
  }

  H[i].ad_id = A->ad_id;
  H[i].views = views;
  if (!A->domain) {
    H[i].domain = -1;
  } else {
    struct hash_entry *CHS = &HS[H[i].domain = calc_domain_hash (A->domain)];
    CHS->heapref = i;
    CHS->generation = __build_heap_generation;
  }
  H[i].expected_gain = expected_gain;
  return &H[i];
}

static inline void heap_pop (void) {
  assert (HN > 0);
  if (--HN) {
    int i = heap_sift_down (1, H[HN+1].expected_gain);
    H[i] = H[HN+1];
  }
}

inline double calculate_expected_view_gain (struct advert *A, int user_ad_views, int user_id) {
  return A->expected_gain;
}

static int __user_id;
static user_t *__user;

intree_traverse_func_t heap_push_user_ad;

int heap_push_user_ad_std (struct intree_node *N) {
  int ad_id = N->x;
  if (ad_id == __exclude_ad_id) {
    return 1;
  }
  int views = N->z;
  struct advert *A = get_ad (ad_id);
  if ((A->flags ^ __xor_mask) & __and_mask) {
    return 1;
  }
  if (__use_views_limit && A->recent_views >= A->recent_views_limit) {
    return 1;
  }
  if ((A->flags & ADF_LIMIT_VIEWS) && A->price <= 0 && views >= 100) {
    return 1;
  }
  double expected_gain = calculate_expected_view_gain (A, views, __user_id);
  heap_insert (expected_gain, A, views);
  return 1;
}


int is_user_in_group (struct user *U, int group_id) {
  struct user_groups *G = U->grp;
  if (G) {
    int l = -1, r = G->cur_groups;
    while (r - l > 1) {
      int m = (l + r) >> 1;
      if (group_id < G->G[m]) {
	r = m;
      } else {
	l = m;
      }
    }
  
    if (l >= 0 && G->G[l] == group_id) {
      return 1;
    }
  }
  return 0;
}

int heap_push_user_ad_ext (struct intree_node *N) {
  int ad_id = N->x;
  if (ad_id == __exclude_ad_id) {
    return 1;
  }

  int views = N->z;
  struct advert *A = get_ad (ad_id);
  if ((A->flags ^ __xor_mask) & __and_mask) {
    return 1;
  }

  if (__use_views_limit && A->recent_views >= A->recent_views_limit) {
    return 1;
  }

  if (A->group && is_user_in_group (__user, A->group)) {
    return 1;
  }

  long long cat_mask = (1LL << (A->category > 63 ? 0 : A->category));
  if (! (__cat_mask & cat_mask)) {
    return 1;
  }

  if (A->price <= 0) {
    if (!(A->flags & ADF_LIMIT_VIEWS) || views < 100) {
      heap_insert (A->expected_gain, A, views);
    }
    return 1;
  }
  long i = views + 1;
  if (i >= MAX_AD_VIEWS) {
    i = MAX_AD_VIEWS - 1;
  }

  double category_val = 0;
  if (A->category) {
    category_val += targ_weights_at (__user->weights, A->category);
  }
  if (A->subcategory) {
    category_val += targ_weights_at (__user->weights, A->subcategory);
  }

  if (category_val) {
    category_val *= log (2.0);
    category_val = ((((((1.0 / 24) * category_val) + (1.0 / 6)) * category_val) + (1.0 / 2)) * category_val + 1) * category_val + 1;
  } else {
    category_val = 1;
  }

  if (unlikely (!AdStats.g[i].views)) {
    i = 0;
    if (unlikely (!AdStats.g[0].views)) {
      heap_insert (A->expected_gain * category_val, A, views);
      return 1;
    }
  }
  double p = (double) AdStats.g[i].clicks / AdStats.g[i].views;
  double lambda = A->lambda * p;
  double delta = A->delta * p;
  if (HN == __gsort_limit && (lambda + 2 * delta) * category_val <= H[1].expected_gain) {
    return 1;
  }
  double expected_gain = (lambda + delta * (drand48() + drand48() - drand48() - drand48())) * category_val;
  heap_insert (expected_gain, A, views);
  return 1;
}

static int build_user_ad_heap (user_t *U, int limit) {
  assert (limit && (unsigned) limit < GSORT_HEAP_SIZE);
  __gsort_limit = limit;
  __user_id = U->user_id;
  __user = U;
  __build_heap_generation++;
  __build_heap_multiplier = (lrand48 () + 0x40000000) | 1;
  HN = 0;
  return intree_traverse (AdSpace, U->active_ads, heap_push_user_ad);
}

#define	ADJ_VIEW_THRESHOLD	(2*INIT_L_VIEWS)

static void adjust_ctr_counters (struct advert *A) {
  if (A->l_views >= 2*ADJ_VIEW_THRESHOLD && (double) A->l_clicked_old / A->l_views > 1.5*INIT_L_CTR) {
    int x = A->l_views / ADJ_VIEW_THRESHOLD;
    A->l_views /= x;
    A->l_clicked_old /= x;
  }
  A->g_views = 0;
  A->g_clicked_old = 0;
}

static int compute_projected_views (int audience) {
  if (audience <= 0) {
    return INIT_L_VIEWS;
  } else if (audience <= 1000) {
    return INIT_L_VIEWS * 8 / 10;  /* up to 1K users */
  } else if (audience <= 32000) {
    return INIT_L_VIEWS * 9 / 10;  /* 1K..32K users */
  } else if (audience <= 12000000) {
    return INIT_L_VIEWS;           /* 32K..12M users */
  } else if (audience <= 80000000) {
    return INIT_L_VIEWS * 12 / 10; /* 12M..80M users */
  } else {
    return INIT_L_VIEWS * 15 / 10; /* 80M..more spam bots */
  }
}

static inline double compute_ad_lambda_delta (int price, double sump0, double sump1, double sump2, double *delta) {
  if (sump1 <= 0 || sump2 <= 0) {
    *delta = price / sqrt (INITIAL_INV_D / 3);
    return price * INITIAL_LAMBDA;
  }
  double lambda, D = sump2;
  if (sump0) {
    lambda = sump0 / sump1;
    lambda = sump0 / (sump1 + lambda * sump2);
    D += sump0 / (lambda*lambda);
  } else {
    lambda = - sump1 / sump2;
  }
  lambda = (D * lambda + INITIAL_INV_D * INITIAL_LAMBDA) / (D + INITIAL_INV_D);
  D += INITIAL_INV_D;
  *delta = price / sqrt (D / 3);
  return lambda * price;
}

static void compute_ad_lambda (struct advert *A) {
  if (A->price <= 0) {
    return;
  }
  A->lambda = compute_ad_lambda_delta (A->price, A->g_sump0, A->g_sump1, A->g_sump2, &A->delta);
}

static double compute_estimated_gain_clicks (struct advert *A, long long g_clicked, long long g_views) {
  int projected_l_views = INIT_L_VIEWS;
  if (A->ext_users) {
    projected_l_views = compute_projected_views (A->ext_users);
  } else if (A->users) {
    projected_l_views = compute_projected_views (A->users * log_split_mod);
  }
  return A->price * (g_clicked * 0.1 + INIT_L_CLICKS) / (g_views * 0.1 + projected_l_views);
}

double compute_estimated_gain (struct advert *A) {
  compute_ad_lambda (A);
  if (A->price > 0) {
    int projected_l_views = INIT_L_VIEWS;
    if (A->ext_users) {
      projected_l_views = compute_projected_views (A->ext_users);
    } else if (A->users) {
      projected_l_views = compute_projected_views (A->users * log_split_mod);
    }
    A->expected_gain = A->price * (A->l_clicked_old + A->g_clicked_old * 0.1 + INIT_L_CLICKS) / 
                                  (A->l_views + A->g_views * 0.1 + projected_l_views);
  } else {
    A->expected_gain = -1.0 * A->price * VIEW_GAIN_MULTIPLIER / MONEY_SCALE;
  }
  return A->expected_gain;
}

int user_ad_price (int uid, int position) {
  if (uid < 0 || uid >= MAX_USERS || position <= 0 || position >= GSORT_HEAP_SIZE) {
    return 0;
  }

  struct user *U = User[uid];
  if (!U || !U->active_ads) {
    return 0;
  }

  build_user_ad_heap (U, position);
  if (HN < position) {
    return 0;
  }

  return (int) (H[1].expected_gain * CTR_GAIN_PRICE_MULTIPLIER + 0.5);
}

int user_cpv_is_enough (int uid, int position, int ad_cpv, int and_mask, int xor_mask) {
  if (uid < 0 || uid >= MAX_USERS || position <= 0 || position >= GSORT_HEAP_SIZE) {
    return 0;
  }

  struct user *U = User[uid];
  if (!U) { 
    return 0;
  }
  
  if (!U->active_ads) {
    return 1;
  }

  heap_push_user_ad = heap_push_user_ad_ext;
  __use_factor = 1;
  __use_views_limit = 0;
  __exclude_ad_id = 0;
  __and_mask = (and_mask & 0xff) << ADF_SITES_MASK_SHIFT;
  __xor_mask = (xor_mask & 0xff) << ADF_SITES_MASK_SHIFT;

  build_user_ad_heap (U, position);

  __use_factor = 0;
  __use_views_limit = 0;
  __and_mask = (254 << ADF_SITES_MASK_SHIFT);
  __xor_mask = 0;

  if (HN < position) {
    return 1;
  }

  return ad_cpv > (int) (H[1].expected_gain * CTR_GAIN_PRICE_MULTIPLIER + 0.5);
}

static int register_user_click (struct lev_targ_click_ext *E) {
  assert (E->type == LEV_TARG_CLICK || E->type == LEV_TARG_CLICK_EXT);
  if (targeting_disabled) {
    return 0;
  }
  user_t *U = get_user (E->user_id);

  account_user_online (U);

  int ad_id = E->ad_id;
  if (!U || ad_id <= 0 || ad_id >= MAX_ADS) { 
    return -1; 
  }

  struct advert *A = get_ad (ad_id);
  if (!A) {
    if (!ad_is_ancient (ad_id)) {
      return -1;
    }
    A = get_ad_f (ad_id, 1);
  }

  if (intree_lookup (AdSpace, U->clicked_ads, ad_id)) {
    /* duplicate click on this ad */
    return 0;
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }
    
  treeref_t RN;
  U->active_ads = intree_remove (AdSpace, U->active_ads, ad_id, &RN);

  if (RN) {
    /* user clicks on an active ad */
    assert ((A->flags & (ADF_ON | ADF_ANCIENT)) == ADF_ON);
    --active_ad_nodes;
  } else {
    /* user clicks on an inactive ad */
    U->inactive_ads = intree_remove (AdSpace, U->inactive_ads, ad_id, &RN);
    if (RN) {
      --inactive_ad_nodes;
      assert (!(A->flags & ADF_ANCIENT));
    } else {
      RN = new_intree_node (AdSpace);
      TNODE (AdSpace, RN)->x = ad_id;
      TNODE (AdSpace, RN)->z = 0;
    }
  }

  int cur_views = TNODE (AdSpace, RN)->z;
  if (cur_views <= 0) { // shit happens: user clicks on an ad we never showed her
    assert (!cur_views);
  }

  int price = (E->type == LEV_TARG_CLICK_EXT ? E->price : A->price);
  if ((price ^ A->price) < 0) {
    price = 0;
  }
  assert (A->price);
    
  if (!price) {
    TNODE (AdSpace, RN)->z = ~cur_views;
  }
      
  /* insert ad_id into clicked ads of user uid */

  U->clicked_ads = intree_insert (AdSpace, U->clicked_ads, RN);
  ++clicked_ad_nodes;

  if (!price) {
    return 0;
  }

  if ((A->flags & ADF_ON) || price < 0) {
    // NB: even if ad is globally active, it might be inactive for current user due to a recent retargeting
    // we account for the click anyway
    A->l_clicked_old++;
    A->l_clicked++;

    A->g_clicked++;
    A->l_sump0 += 1.0;
    A->g_sump0 += 1.0;

    if (cur_views >= MAX_AD_VIEWS) {
      cur_views = MAX_AD_VIEWS - 1;
    }
    if (unlikely (!AdStats.g[cur_views].views)) {
      cur_views = 0;
    }
    if (likely (AdStats.g[cur_views].views)) {
      double p = (double) AdStats.g[cur_views].clicks / AdStats.g[cur_views].views;
      if (likely (p <= A->l_sump1) && likely (p*p <= A->l_sump2)) {
	A->l_sump1 -= p;
	A->g_sump1 -= p;
	A->l_sump2 -= p*p;
	A->g_sump2 -= p*p;
	if (price > 0 && (A->flags & ADF_NEWVIEWS)) {
	  total_sump0++;
	  total_sump1 -= p;
	  total_sump2 -= p*p;
	}
      } else {
	A->g_sump1 -= A->l_sump1;
	A->g_sump2 -= A->l_sump2;
	if (price > 0 && (A->flags & ADF_NEWVIEWS)) {
	  total_sump0++;
	  total_sump1 -= A->l_sump1;
	  total_sump2 -= A->l_sump2;
	}
	A->l_sump1 = 0;
	A->l_sump2 = 0;
      }
    }

    if (price > 0) {
      A->click_money += price * MONEY_SCALE;
      tot_click_money += price * MONEY_SCALE;
      /* (maybe) update statistics of ad A using cur_views (TODO) */

      /* update global statistics using cur_views */
      if (cur_views > 0 && (A->flags & ADF_NEWVIEWS)) {
	++AdStats.l[0].clicks;
	++AdStats.g[0].clicks;
	if (cur_views >= MAX_AD_VIEWS) {
	  cur_views = MAX_AD_VIEWS - 1;
	}
	++AdStats.l[cur_views].clicks;
	++AdStats.g[cur_views].clicks;
      }
    }
    compute_estimated_gain (A);
    tot_clicks++;
    return 1;
  }

  return 0;
}


static void incr_ad_views (int ad_id, int new_views, int mode) {
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A) {
    if (!ad_is_ancient (ad_id)) {
      return;
    }
    A = get_ad_f (ad_id, 1);
  }
  /* have to load this ancient ad (exact views number is needed for stats) */
  assert (load_ancient_ad (A) >= 0);
  if ((unsigned) new_views < 0x100000) {
    A->views += new_views;
    A->l_views += new_views;
    tot_views += new_views;

    if (!mode && !(A->flags & ADF_ANYVIEWS)) {
      A->flags |= ADF_OLDVIEWS;
    }
    if (!mode && likely (AdStats.g[0].views)) {
      double p = (double) AdStats.g[0].clicks / AdStats.g[0].views;
      A->l_sump1 += new_views * p;
      A->g_sump1 += new_views * p;
      A->l_sump2 += new_views * p*p;
      A->g_sump2 += new_views * p*p;
    }
    if (A->price < 0) {
      A->click_money += -A->price * (long long) new_views;
      tot_click_money += -A->price * (long long) new_views;
    }

    compute_estimated_gain (A);
  }
}


static int register_user_view (struct lev_targ_user_view *E) {
  assert (E->type == LEV_TARG_USER_VIEW);
  user_t *U = get_user (E->user_id);

  account_user_online (U);

  if (targeting_disabled) {
    return 0;
  }

  int ad_id = E->ad_id;
  if (!U || ad_id <= 0 || ad_id >= MAX_ADS) { 
    return -1; 
  }

  struct advert *A = get_ad (ad_id);
  if (!A) {
    if (!ad_is_ancient (ad_id)) {
      return -1;
    }
    A = get_ad_f (ad_id, 1);
  }

  if (intree_lookup (AdSpace, U->clicked_ads, ad_id)) {
    /* ad already clicked, view ignored */
    return 0;
  }

  /* have to load this ancient ad (exact views number is needed for stats) */
  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }
    
  int cur_views;
  treeref_t N = intree_lookup (AdSpace, U->active_ads, ad_id);
  if (N) {
    cur_views = ++TNODE (AdSpace, N)->z;
  } else {
    N = intree_lookup (AdSpace, U->inactive_ads, ad_id);
    if (N) {
      cur_views = ++TNODE (AdSpace, N)->z;
    } else {
      N = new_intree_node (AdSpace);
      TNODE (AdSpace, N)->x = ad_id;
      cur_views = TNODE (AdSpace, N)->z = 1;
      U->inactive_ads = intree_insert (AdSpace, U->inactive_ads, N);
      ++inactive_ad_nodes;
    }
  }

  incr_ad_views (ad_id, 1, 1);
  // ???? account this view in the new way as well

  if (cur_views >= MAX_AD_VIEWS) {
    cur_views = MAX_AD_VIEWS - 1;
  }

  long i = cur_views;
  if (unlikely (!AdStats.g[i].views)) {
    i = 0;
  }
  if (likely (AdStats.g[i].views)) {
    double p = (double) AdStats.g[i].clicks / AdStats.g[i].views;
    A->l_sump1 += p;
    A->g_sump1 += p;
    A->l_sump2 += p*p;
    A->g_sump2 += p*p;
    if (A->price > 0 && (A->flags & ADF_NEWVIEWS)) {
      total_sump1 += p;
      total_sump2 += p*p;
    }
    compute_ad_lambda (A);
  }

  if (!(A->flags & ADF_ANYVIEWS)) {
    A->flags |= ADF_NEWVIEWS;
  }
  if ((A->flags & ADF_NEWVIEWS) && A->price > 0) {
    ++AdStats.l[0].views;
    ++AdStats.g[0].views;
    assert (cur_views > 0);
    ++AdStats.l[cur_views].views;
    ++AdStats.g[cur_views].views;
  }

  register_one_view (A, E->user_id);
      
  return 1;
}


static int compute_ad_user_list (struct advert *A) {
  if (A->user_list) {
    tot_userlists--;
    tot_userlists_size -= A->users; 
    free (A->user_list);
    A->user_list = 0;
    A->users = 0;
  }

  process_lru_ads ();

  aux_userlist_tag = 0;

  assert (!compile_query (A->query));
  R_position = -1;
  Q_limit = MAX_USERS;
  Q_order = 0;
  global_birthday_in_query = 0;

  int res = perform_query (A->ad_id);
  R_position = 0;

  if (global_birthday_in_query) {
    A->flags |= ADF_PERIODIC;
  } else {
    A->flags &= ~ADF_PERIODIC;
  }

  assert (res >= 0 && res == R_cnt);

  A->users = res;
  
  A->user_list = malloc (res * 4 + 4);
  memcpy (A->user_list, R, res * 4);
  A->user_list[res] = 0x7fffffff;
  A->userlist_computed_at = now;
  A->prev_user_creations = user_creations;

  tot_userlists++;
  tot_userlists_size += res; 

  return res;
}

static inline void activate_one_user_ad (user_t *U, int ad_id) {
  treeref_t RN;
  U->inactive_ads = intree_remove (AdSpace, U->inactive_ads, ad_id, &RN);
  if (!RN) {
    if (intree_lookup (AdSpace, U->clicked_ads, ad_id)) {
      return;
    }
    RN = new_intree_node (AdSpace);
    struct intree_node *C = TNODE (AdSpace, RN);
    C->x = ad_id;
    C->z = 0;
  } else {
    --inactive_ad_nodes;
  }
  ++active_ad_nodes;
  assert (!intree_lookup (AdSpace, U->active_ads, ad_id));  // !!! remove later
  U->active_ads = intree_insert (AdSpace, U->active_ads, RN);
}

static inline void deactivate_one_user_ad (user_t *U, int ad_id) {
  treeref_t RN;
  U->active_ads = intree_remove (AdSpace, U->active_ads, ad_id, &RN);
  if (RN) {
    --active_ad_nodes;
    if (TNODE (AdSpace, RN)->z) {
      ++inactive_ad_nodes;
      U->inactive_ads = intree_insert (AdSpace, U->inactive_ads, RN);
    } else {
      free_intree_node (AdSpace, RN);
    }
  }
}

/* inserts ad into active_ads of all users from A->user_list */
static void activate_ad (struct advert *A) {
  int *p = A->user_list, *q = p + A->users;
  int ad_id = A->ad_id, prev_user_creations = A->prev_user_creations;
  while (p < q) {
    int uid = *p++;
    user_t *U = User[uid];
    if (U && U->prev_user_creations <= prev_user_creations) {
      activate_one_user_ad (U, ad_id);
    }
  }
}

/* inserts ad into inactive_ads of all users from A->user_list */
static void deactivate_ad (struct advert *A) {
  int *p = A->user_list, *q = p + A->users;
  int ad_id = A->ad_id;
  while (p < q) {
    int uid = *p++;
    user_t *U = User[uid];
    if (U) {
      deactivate_one_user_ad (U, ad_id);
    }
  }
}

static void change_ad_userlist (struct advert *A, int old_users, int *old_user_list, int old_prev_user_creations) {
  assert (A && (A->flags && ADF_ON) && A->user_list);
  int ad_id = A->ad_id, prev_user_creations = A->prev_user_creations;
  int *p = A->user_list, *q = old_user_list;
  int x = *p++, y = *q++;
  user_t *U;
  while (1) {
    if (x < y) {
      U = User[x];
      if (U && U->prev_user_creations <= prev_user_creations) {
	activate_one_user_ad (U, ad_id);
      }
      x = *p++;
    } else if (x > y) {
      U = User[y];
      if (U) {
	deactivate_one_user_ad (U, ad_id);
      }
      y = *q++;
    } else if (x == 0x7fffffff) {
      break;
    } else {
      U = User[x];
      if (U && U->prev_user_creations > old_prev_user_creations && U->prev_user_creations <= prev_user_creations) {
	activate_one_user_ad (U, ad_id);
      }
      x = *p++;
      y = *q++;
    }
  }
  assert (q == old_user_list + old_users + 1 && p == A->user_list + A->users + 1);
}


static inline void apply_periodic_ad_state (struct advert *A) {
  if (A->flags & ADF_PERIODIC) {
    reinsert_retarget_ad_last (A);
    A->retarget_time = ((log_last_ts + (RETARGET_AD_TIMEOUT < 3600 ? 3600 : RETARGET_AD_TIMEOUT)) / 3600) * 3600;
    if (verbosity > 1) { 
      fprintf (stderr, "inserting ad #%d (%p) into retarget queue, retarget_time=%d\n", A->ad_id, A, A->retarget_time);
    }
  }
}

static int change_ad_target (struct advert *A) {
  assert (A && A->query && !(A->flags & ADF_ANCIENT));
  int *old_user_list = A->user_list;
  int old_users = A->users, old_prev_user_creations = A->prev_user_creations;

  A->user_list = 0;
  A->users = 0;

  compute_ad_user_list (A);

  if ((A->flags & (ADF_ON | ADF_DELAYED)) == ADF_ON) {
    assert (old_user_list);
    change_ad_userlist (A, old_users, old_user_list, old_prev_user_creations);
  } else {
    if (!(A->flags & ADF_ON)) {
      active_ads++;
    }
    if (!delay_targeting) {
      A->flags &= ~ADF_DELAYED;
      activate_ad (A);
    } else {
      A->flags |= ADF_DELAYED;
    }
  }

  if (old_user_list) {
    free (old_user_list);
    tot_userlists--;
    tot_userlists_size -= old_users;
  }

  compute_estimated_gain (A);

  A->flags |= ADF_ON;
  remove_queue_ad (A);

  apply_periodic_ad_state (A);

  return A->users;
}

static int ad_enable (struct advert *A, int price) {
  if (!A) { 
    return 0; 
  }
  if (A->flags & ADF_ANCIENT) {
    int res = load_ancient_ad (A);
    if (res < 0) {
      return res;
    }
    assert (A->disabled_since <= log_last_ts - AD_ANCIENT_DELAY);
  }

  if (price) {
    A->price = price;
  }

  assert (A->price);
  
  if (A->flags & ADF_ON) {
    if (A->userlist_computed_at <= log_last_ts - AD_RECOMPUTE_INTERVAL) {
      change_ad_target (A);
    } else {
      compute_estimated_gain (A);
    }
    return 0; 
  }

  if (!A->user_list || A->disabled_since <= log_last_ts - AD_ANCIENT_DELAY || A->userlist_computed_at <= log_last_ts - AD_RECOMPUTE_INTERVAL) {
    compute_ad_user_list (A);
  }

  remove_queue_ad (A);
  A->flags |= ADF_ON;
  active_ads++;

  if (!delay_targeting) {
    activate_ad (A);
  } else {
    A->flags |= ADF_DELAYED;
  }

  apply_periodic_ad_state (A);
  compute_estimated_gain (A);

  if (verbosity > 2) {
    fprintf (stderr, "enabled previously disabled ad #%d: %d users\n", A->ad_id, A->users);
  }

  return 1;
}

static int ad_disable (struct advert *A) {
  if (!A) { 
    return 0; 
  }
  if (!(A->flags & ADF_ON)) { 
    return 1; 
  }
  assert (!(A->flags & ADF_ANCIENT));

  A->flags &= ~ADF_ON;
  active_ads--;
  A->disabled_since = now;

  if (!(A->flags & ADF_DELAYED)) {
    deactivate_ad (A);
  } else {
    A->flags &= ~ADF_DELAYED;
  }

  if (verbosity > 2) {
    fprintf (stderr, "disabled previously enabled ad #%d: %d users\n", A->ad_id, A->users);
  }

  reinsert_lru_ad_last (A);
  process_lru_ads ();

  return 1;
}

int perform_delayed_ad_activation (void) {
  long i;
  int c = 0;
  delay_targeting = 0;
  for (i = 0; i < AD_TABLE_SIZE; i++) {
    struct advert *A;
    for (A = Ads[i]; A; A = A->hash_next) {
      if (A->flags & ADF_DELAYED) {
	assert ((A->flags & (ADF_ON | ADF_ANCIENT)) == ADF_ON);
	A->flags &= ~ADF_DELAYED;
	activate_ad (A);
	c++;
      }
    }
  }
  return c;
}

static int set_ad_price (struct lev_targ_ad_price *E) {
  if (!E->ad_price) {
    return 0;
  }
  struct advert *A = get_ad_f (E->ad_id, ad_is_ancient (E->ad_id));
  if (!A) {
    return 0;
  }
  /* have to load this ancient ad */
  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }
    
  A->price = E->ad_price;  
  compute_estimated_gain (A);
  return 1;
}


static int delete_user (struct lev_delete_user *E) {
  int user_id = E->user_id;
  int s = conv_user_id (user_id);
  if (s < 0) {
    return 0;
  }
  user_t *U = User[s];

  if (!U) {
    return 0;
  }

  --tot_users;

  if (U->online_next) {
    online_list_remove (user_to_olist (U));
    online_increment (online_convert_time (U->last_visited), -1);
  }

  U->online_next = U->online_prev = 0;

  utree_t *new_tree = utree_merge (U->left_rate, U->right_rate);

  if (new_tree) {
    new_tree->uplink = U->uplink_rate;
  }

  if (U->uplink_rate->left == (utree_t *) U) {
    U->uplink_rate->left = new_tree;
  } else {
    U->uplink_rate->right = new_tree;
  }

  U->uplink_rate = U->left_rate = U->right_rate = 0;

  user_clear_education (U);
  user_clear_schools (U);
  user_clear_addresses (U);
  user_clear_military (U);
  user_clear_work (U);
  user_clear_groups (U);
  user_clear_langs (U);
  user_clear_field (U->uid, q_sex, U->sex);
  user_clear_field (U->uid, q_operator, U->operator);
  user_clear_field (U->uid, q_browser, U->browser);
  user_clear_field (U->uid, q_region, U->region);
  user_clear_field (U->uid, q_height, U->height);
  user_clear_field (U->uid, q_smoking, U->smoking);
  user_clear_field (U->uid, q_alcohol, U->alcohol);
  user_clear_field (U->uid, q_ppriority, U->ppriority);
  user_clear_field (U->uid, q_iiothers, U->iiothers);
  user_clear_field (U->uid, q_hidden, U->hidden);
  user_clear_field (U->uid, q_cvisited, U->cvisited);
  user_clear_field (U->uid, q_gcountry, U->gcountry);
  user_clear_field (U->uid, q_custom1, U->custom1);
  user_clear_field (U->uid, q_custom2, U->custom2);
  user_clear_field (U->uid, q_custom3, U->custom3);
  user_clear_field (U->uid, q_custom4, U->custom4);
  user_clear_field (U->uid, q_custom5, U->custom5);
  user_clear_field (U->uid, q_custom6, U->custom6);
  user_clear_field (U->uid, q_custom7, U->custom7);
  user_clear_field (U->uid, q_custom8, U->custom8);
  user_clear_field (U->uid, q_custom9, U->custom9);
  user_clear_field (U->uid, q_custom10, U->custom10);
  user_clear_field (U->uid, q_custom11, U->custom11);
  user_clear_field (U->uid, q_custom12, U->custom12);
  user_clear_field (U->uid, q_custom13, U->custom13);
  user_clear_field (U->uid, q_custom14, U->custom14);
  user_clear_field (U->uid, q_custom15, U->custom15);
  user_clear_field (U->uid, q_timezone, U->timezone);
  user_clear_field (U->uid, q_has_photo, (U->has_photo & 0x81) == 1);
  user_clear_field (U->uid, q_uses_apps, (U->has_photo & 0x82) == 2);
  user_clear_field (U->uid, q_pays_money, (U->has_photo & 0x84) == 4);
  user_clear_field (U->uid, q_mstatus, U->mstatus);
  user_clear_field (U->uid, q_political, U->political);
  user_clear_field (U->uid, q_country, U->uni_country);
  user_clear_field (U->uid, q_city, U->uni_city);
  delete_user_hashlist (U->uid, U->name_hashes);
  delete_user_hashlist (U->uid, U->religion_hashes);
  delete_user_hashlist (U->uid, U->hometown_hashes);
  delete_user_hashlist (U->uid, U->proposal_hashes);
  user_clear_field (U->uid, q_byear, U->bday_year);
  user_clear_field (U->uid, q_bmonth, U->bday_month);
  user_clear_field (U->uid, q_bday, U->bday_day);
  exact_strfree (U->religion);
  exact_strfree (U->name);
  exact_strfree (U->hometown);
  exact_strfree (U->proposal);
  U->name = 0;
  user_clear_interests (U, -1);

  active_ad_nodes -= intree_free (AdSpace, U->active_ads);
  inactive_ad_nodes -= intree_free (AdSpace, U->inactive_ads);
  clicked_ad_nodes -= intree_free (AdSpace, U->clicked_ads);
  if (U->weights) {
    targ_weights_vector_free (U->weights);
    U->weights = NULL;
  }
  zfree (U, sizeof (user_t));
  User[s] = 0;

  return 1;
}

static void exact_strcpy (char *dest, const char *src, int len) {
  memcpy (dest, src, len);
  register int i;
  for (i = 0; i < len; i++) {
    if (!dest[i]) {
      dest[i] = '\n';
    }
  }
  dest[i] = 0;
}

char *exact_strdup (const char *src, int len) {
#ifdef	DEBUG_EXACT_STRINGS
  char *res = zmalloc (len + 1 + 8) + 4;
  *((int *) (res - 4)) = 0xdeadbeef;
  *((int *) (res + len + 1)) = 0xbeda3ae6;
#else
  char *res = zmalloc (len + 1);
#endif
  exact_strcpy (res, src, len);
  return res;
}

static char *exact_stralloc (int len) {
#ifdef	DEBUG_EXACT_STRINGS
  char *res = zmalloc (len + 1 + 8) + 4;
  *((int *) (res - 4)) = 0xdeadbeef;
  *((int *) (res + len + 1)) = 0xbeda3ae6;
#else
  char *res = zmalloc (len + 1);
#endif
  res[len] = 0;
  return res;
}

static void exact_strfree (char *str) {
  if (str) {
    int len = strlen (str);
#ifdef	DEBUG_EXACT_STRINGS
    assert (*((int *) (str - 4)) == 0xdeadbeef);
    assert (*((int *) (str + len + 1)) == 0xbeda3ae6);
    zfree (str - 4, len + 1 + 8);
#else
    zfree (str, len + 1);
#endif
  }
}

static int set_username (struct lev_username *E, int len) {
  int i, j = 0;
  int user_id = E->user_id;
  char *text = E->s;

  assert (!text[len]);
  /*  if (user_id == 20205559 && verbosity > 0) {
    fprintf (stderr, "name of %d: '%s' (len=%d; now=%d, logpos=%lld)\n", user_id, text, len, now, log_cur_pos());
    }*/
  for (i = 0; i < len; i++) {
    if (text[i] == 9) { j++; }
    else if ((unsigned char) text[i] < ' ') { 
      fprintf (stderr, "name of %d: '%s' (len=%d)\n", user_id, text, len);
//      assert ((unsigned char) text[i] >= ' ');
      break;
    }
  }
  assert (j >= 0);
  user_t *U = get_user_f (user_id);
  if (U) {
    exact_strfree (U->name);
    U->name = exact_strdup (text, len);
    delete_user_hashlist (U->uid, U->name_hashes);
    U->name_hashes = save_words_hashlist_pattern (U->name, 0, ~2, 1, q_name);
    add_user_hashlist (U->uid, U->name_hashes);

    rehash_user_interests (U);
    return 1;
  }
  return 0;
}

static int set_user_group_types (struct lev_targ_user_group_types *E) {
  user_t *U = get_user (E->user_id);
  if (!U) {
    return 0;
  }
  memcpy (U->user_group_types, E->user_group_types, 16);
  return 1;
}

static int set_user_single_group_type (struct lev_generic *E) {
  user_t *U = get_user (E->a);
  if (!U) {
    return 0;
  }
  unsigned type = E->type & 0x7f;
  U->user_group_types[type >> 5] |= (1 << (type & 31));
  return 1;
}

static void set_birthday (struct lev_birthday *E) {
  user_t *U = get_user (E->user_id);
  if (!U) {
    return;
  }
  if (!E->year || (E->year >= 1900 && E->year <= 2017)) {
    user_change_field (U->uid, q_byear, U->bday_year, E->year);
    U->bday_year = E->year;
  }
  if (E->month >= 0 && E->month <= 12) {
    user_change_field (U->uid, q_bmonth, U->bday_month, E->month);
    U->bday_month = E->month;
  }
  if (E->day >= 0 && E->day <= 31) {
    user_change_field (U->uid, q_bday, U->bday_day, E->day);
    U->bday_day = E->day;
  }
}

static void store_edu (int uid, struct education **to, struct lev_education *D) {
  int prim = D->type & 1;
  int edu_form = (unsigned char) D->edu_form;
  int edu_status = (unsigned char) D->edu_status;
  struct education *E;
  if (prim) {
    for (E = *to; E; E = E->next) {
      E->primary = 0;
    }
  }
  E = zmalloc (sizeof (struct education));
  memset (E, -1, sizeof(*E));
  E->next = *to;
  if (edu_form > MAX_EDU_FORM) { 
    edu_form = 0; 
  }
  if (edu_status > MAX_EDU_STATUS) { 
    edu_status = 0; 
  }
#define CPY(__x,__f) user_add_field (uid, __f, E->__x = D->__x);  
#define CPYL(__x,__f) user_add_field (uid, __f, E->__x = __x);  
  CPY(grad_year, q_graduation);
  CPY(chair, q_chair);
  CPY(faculty, q_faculty);
  CPY(university, q_univ);
  CPY(city, q_uni_city);
  CPY(country, q_uni_country);
  CPYL(edu_form, q_edu_form);
  CPYL(edu_status, q_edu_status);
#undef CPY
#undef CPYL
  E->primary = prim;
  *to = E;
}

static int store_school (const struct lev_school *E, int sz) {
  const char *ptr = E->spec;
  int i, len = E->type & 0xff;
  user_t *U;
  struct school *S;

  if (sz < 26+len) { return -2; }

  assert (!ptr[len]);
  for (i = 0; i < len; i++) {
    assert ((unsigned char) ptr[i] >= ' ');
  }

  U = get_user (E->user_id);
  if (U) {
    S = zmalloc (sizeof (struct school));
    memset (S, 0, sizeof(*S));
    S->next = U->sch;
    U->sch = S;
#define CPY(__x,__f) user_add_field (U->uid, __f, S->__x = E->__x);  
    CPY(city, q_sch_city);
    CPY(school, q_sch_id);
    S->spec = 0;
    if (len) {
      S->spec = exact_strdup (E->spec, len);
      S->spec_hashes = save_words_hashlist (S->spec, 0, q_sch_spec);
      add_user_hashlist (U->uid, S->spec_hashes);
    }
    S->start = E->start;
    S->finish = E->finish;
    CPY(grad, q_sch_grad);
    CPY(country, q_sch_country);
    CPY(sch_class, q_sch_class);
    S->sch_type = E->sch_type;
#undef CPY
  }
  return 26+len;

}

#define MAX_WORD_HASHES	8192
#define MAX_WORD_LEN	512

static int Hc;
static hash_t HL[MAX_WORD_HASHES];
static char Word[MAX_WORD_LEN+1];

static void hsort (int a, int b) {
  int i, j;
  hash_t t, h;
  if (a >= b) { return; }
  i = a;  j = b;  h = HL[(a+b)>>1];
  do {
    while (HL[i] < h) { i++; }
    while (HL[j] > h) { j--; }
    if (i <= j) {
      t = HL[i];  HL[i++] = HL[j];  HL[j--] = t;
    }
  } while (i <= j);
  hsort (a, j);
  hsort (i, b);
}

int prepare_words_hashlist (const char *str, int flags, int pattern, int translit, int type) {
  int len;
  static char trans_buff[256];
  if (!flags) {
    Hc = 0;
  }
  if (str) {
    while (*str) {
      len = get_notword (str);
      if (len < 0) {
        break;
      }
      for (; len > 0; len--) {
        if (*str++ == 9) {
          pattern >>= 1;
        }
      }
      len = get_word (str);
      if (len < 0 || Hc >= MAX_WORD_HASHES) { break; }
      if (!len) {
        continue;
      }
      if (len < MAX_WORD_LEN && (pattern & 1)) {
	int len2 = len;
	if (translit) {
	  lc_str (Word, str, len);
	} else {
	  len2 = my_lc_str (Word, str, len);
	}
	HL[Hc++] = (word_crc64 (Word, len2) + type) | (1ULL << 63);
	if (translit && Hc < MAX_WORD_HASHES) {
	  translit_str (trans_buff, 250, Word, len);
	  if (strcmp (trans_buff, Word)) {
	    HL[Hc++] = (word_crc64 (trans_buff, -1) + type) | (1ULL << 63);
	    if (verbosity > 4) {
	      fprintf (stderr, "translit: '%s' -> '%s'\n", Word, trans_buff);
	    }
	  }
	}
      }
      str += len;
    }
  }
  if (flags <= 0 && Hc > 0) {
    int i, j = 1;
    hash_t h;
    hsort (0, Hc-1);
    h = HL[0];
    for (i = 1; i < Hc; i++) {
      if (HL[i] != h) {
	HL[j++] = h = HL[i];
      }
    }
    Hc = j;
  }
  return Hc;
}

hash_list_t *save_words_hashlist_pattern (const char *str, int flags, int pattern, int translit, int type) {
  int c = prepare_words_hashlist (str, flags, pattern, translit, type);
  hash_list_t *R;
  if (!c) { return 0; }
  R = dyn_alloc (8 + c * sizeof(hash_t), 8);
  assert (R);
  assert (!((long) R & 7));
  R->size = R->len = c;
  memcpy (R->A, HL, c * sizeof(hash_t));
  return R;
}

inline hash_list_t *save_words_hashlist (const char *str, int flags, int type) {
  return save_words_hashlist_pattern (str, flags, -1, 0, type);
}


static int store_company (const struct lev_company *E, int sz) {
  const char *ptr = E->text;
  int i, j = 0, len = E->type & 0x1ff;
  user_t *U;
  struct company *C;

  if (sz < 22+len) { return -2; }

  assert (!ptr[len]);
  for (i = 0; i < len; i++) {
    if (ptr[i] == 9) { j++; }
    else { 
      assert ((unsigned char) ptr[i] >= ' ');
    }
  }
  assert (j == 1);

  U = get_user (E->user_id);
  if (U) {
    C = zmalloc (sizeof (struct company));
    memset (C, 0, sizeof(*C));
    C->next = U->work;
    U->work = C;
#define CPY(__x) C->__x = E->__x;  
    CPY(city);
    CPY(company);
    CPY(start);
    CPY(finish);
    CPY(country);
#undef CPY
    for (i = 0; i < len; i++) { 
      if (ptr[i] == 9) { break; }
    }
    if (i > 0) {
      C->company_name = exact_strdup (E->text, i);
      C->name_hashes = save_words_hashlist (C->company_name, 0, q_company_name);
      add_user_hashlist (U->uid, C->name_hashes);
    }
    if (i < len - 1) {
      ptr += i+1;
      C->job = exact_strdup (E->text+i+1, len-i-1);
      C->job_hashes = save_words_hashlist (C->job, 0, q_job);
      add_user_hashlist (U->uid, C->job_hashes);
    }
  }
  return 22+len;
}

static void store_address (user_t *U, const struct lev_address_extended *E, int len) {
  int i, uid = U->uid;
  struct address *A = zmalloc (sizeof (struct address)), **to = &U->addr;

  if (verbosity > 1 && U->user_id == debug_user_id) {
    fprintf (stderr, "store_addr(user_id=%d, country=%d, city=%d), now=%d\n", debug_user_id, E->country, E->city, now);
  }

  A->next = *to;
#define CPY(__x,__f) user_add_field (uid, __f, A->__x = E->__x);  
  CPY(atype, q_adr_type);
  CPY(country, q_adr_country);
  CPY(city, q_adr_city);
  CPY(district, q_adr_district);
  CPY(station, q_adr_station);
  CPY(street, q_adr_street);
  A->house = A->name = 0;
  A->house_hashes = A->name_hashes = 0;
  if (len > 0 && !E->text[len]) {
    for (i = 0; i < len; i++) {
      if (E->text[i] == 9) {
        break;
      }
    }
    if (i > 0) {
      A->house = exact_strdup (E->text, i);
      A->house_hashes = save_words_hashlist (A->house, 0, q_adr_house);
      add_user_hashlist (U->uid, A->house_hashes);
    }
    if (i < len - 1) {
      A->name = exact_strdup (E->text + i + 1, len - i - 1);
      A->name_hashes = save_words_hashlist (A->name, 0, q_adr_name);
      add_user_hashlist (U->uid, A->name_hashes);
    }
  }
#undef CPY
  *to = A;
}

static int store_interests (const struct lev_interests *E, int sz) {
  const char *ptr = E->text;
  int len = E->len;
  int type = E->type & 0xff;
  user_t *U = get_user (E->user_id);
  struct interest *I, **to;

  if (sz < len + 11) { return -2; }
  assert (!ptr[len]);
  assert (type > 0 && type <= MAX_INTERESTS);

  /*if (E->user_id == 4000822 && verbosity > 1) {
    fprintf (stderr, "time=%d, log_pos=%lld, user_id=%d, type=%d, len=%d, '%.*s'\n", now, log_cur_pos(), E->user_id, type, len, len, ptr);
    }*/

  if (!U) { return len+11; }

  to = &U->inter;
  for (I = U->inter; I; I = *to) {
    if (I->type == type) {
      *to = I->next;
      /* dealloc current I ... */
      zfree (I, strlen (I->text) + 1 + offsetof (struct interest, text));
      break;
    } else {
      to = &I->next;
    }
  }

  I = zmalloc (len + 1 + offsetof (struct interest, text));
  I->next = U->inter;
  I->type = type;
  I->flags = 0;
  exact_strcpy (I->text, ptr, len);
  U->inter = I;

  rehash_user_interests (U);

  return len + 11;
}

static int store_religion (struct lev_religion *E, int sz) {
  char *ptr = E->str;
  int len = E->type & 0xff;
  user_t *U;

  if (sz < len+9) { return -2; }

  assert (!ptr[len]);
//  fprintf (stderr, "%d: '%s'\n", E->user_id, E->str);

  U = get_user (E->user_id);

  if (U) {
    exact_strfree (U->religion);
    U->religion = exact_stralloc (len);
    filter_simple_text (U->religion, ptr, len);
    delete_user_hashlist (U->uid, U->religion_hashes);
    U->religion_hashes = save_words_hashlist (U->religion, 0, q_religion);
    add_user_hashlist (U->uid, U->religion_hashes);
  }

  return 9+len;
}

static void store_military (user_t *U, struct lev_military *E) {
  int uid = U->uid;
  struct military *M = zmalloc (sizeof (struct military)), **to = &U->mil;
  M->next = *to;
#define CPY(__x,__f) user_add_field (uid, __f, M->__x = E->__x);  
  CPY(unit_id, q_mil_unit);
  CPY(start, q_mil_start);
  CPY(finish, q_mil_finish);
#undef CPY
  *to = M;
}

/*static void del_all_users_from_group (treespace_t TS, treeref_t T, int group_id) {
  if (T) {
    struct intree_node *TC = TS_NODE(T);
    del_all_users_from_group (TS, TC->left, group_id);
    del_all_users_from_group (TS, TC->right, group_id);
    if (TC->z > 0) {
      assert ((unsigned) TC->x < MAX_USERS);
      struct user *U = User[TC->x];
      assert (U && U->grp);

      struct user_groups *G = U->grp;
      int l = -1, r = G->cur_groups;
      while (r - l > 1) {
	int m = (l + r) >> 1;
	if (group_id < G->G[m]) {
	  r = m;
	} else {
	  l = m;
	}
      }
    
      assert (l >= 0 && G->G[l] == group_id);
      memmove (G->G + l, G->G + (l + 1), (G->cur_groups - l - 1) * 4);

      G->cur_groups--;
    } else assert (TC->z > 0); // no index yet
    free_intree_node (TS, T);
  }
  }*/


static void delete_single_user_group_fast (int uid, int group_id) {
  assert ((unsigned)uid < MAX_USERS);
  struct user *U = User[uid];
  assert (U && U->grp);
  
  struct user_groups *G = U->grp;
  int l = -1, r = G->cur_groups;
  while (r - l > 1) {
    int m = (l + r) >> 1;
    if (group_id < G->G[m]) {
      r = m;
    } else {
      l = m;
    }
  }
  
  assert (l >= 0 && G->G[l] == group_id);
  memmove (G->G + l, G->G + (l + 1), (G->cur_groups - l - 1) * 4);
  
  G->cur_groups--;

  delete_user_word (uid, field_value_hash (q_grp_id, group_id));
  user_group_pairs--;
}


static int delete_group (struct lev_delete_group *E) {
  int group_id = E->group_id, num = 0, i;

  clear_tmp_word_data ();

  if (get_word_count_nomult (field_value_hash (q_grp_id, group_id))) {
    dyn_mark (0);
    struct mult_iterator *I = (struct mult_iterator *) build_word_iterator (field_value_hash (q_grp_id, group_id));
    while (I->pos < INFTY) {
      R[num++] = I->pos;
      I->jump_to ((iterator_t)I, I->pos + 1);
    }
    dyn_release (0);
    assert ((unsigned)num <= MAX_USERS);
    for (i = 0; i < num; i++) {
      delete_single_user_group_fast (R[i], group_id);
    }
  }

  return 1; // does now work
}


static int add_user_group (user_t *U, int group_id) {
  if (group_id > max_group_id) {
    max_group_id = group_id;
  }
  if (group_id < min_group_id) {
    min_group_id = group_id;
  }

  add_user_word (U->uid, field_value_hash (q_grp_id, group_id));

  user_group_pairs++;

  return 1;
}

static int del_user_group (user_t *U, int group_id) {
  delete_user_word (U->uid, field_value_hash (q_grp_id, group_id));
  user_group_pairs--;

  return 1;
}

static int add_groups (user_t *U, int List[], int len) {
  struct user_groups *G = U->grp;

  if (len < 0 || len > MAX_USER_GROUPS) {
    return -1;
  }
  if (!len || (G && G->cur_groups >= MAX_USER_GROUPS)) {
    return 0;
  }

  int i, j, c = 0;
  for (i = 1; i < len; i++) {
    if (List[i-1] >= List[i]) {
      fprintf (stderr, "add_groups: user_id=%d, len=%d,", U->user_id, len);
      int j;
      for (j = 0; j < len; j++) {
        fprintf (stderr, " %d", List[j]);
      }
      fprintf (stderr, ".\n");
    }
    assert (List[i-1] < List[i]);
  }

  if (G) {
    assert (G->cur_groups >= 0 && G->cur_groups <= G->tot_groups);
    i = j = 0;
    while (i < len && j < G->cur_groups) {
      if (List[i] < G->G[j]) { i++; }
      else if (List[i] > G->G[j]) { j++; }
      else { c++;  i++;  j++; }
    }
    int t = G->cur_groups + len - c, tt = G->tot_groups;
    if (t > tt) {
      while (t > tt) { tt <<= 1; }
      struct user_groups *GN = zmalloc (sizeof (struct user_groups) + 4*tt);
      memcpy (GN, G, sizeof(struct user_groups) + 4*G->cur_groups);
      zfree (G, sizeof (struct user_groups) + 4*G->tot_groups);
      U->grp = G = GN;
      G->tot_groups = tt;
    }

    i = len-1;
    j = G->cur_groups-1;
    G->cur_groups = t;

    while (i >= 0 && j >= 0) {
      assert (t > 0);
      if (List[i] > G->G[j]) {
        G->G[--t] = List[i];
        add_user_group (U, List[i--]);
      } else {
        if (List[i] == G->G[j]) {
          i--;
        }
        G->G[--t] = G->G[j--];
      }
    }
    while (i >= 0) {
      assert (t > 0);
      G->G[--t] = List[i];
      add_user_group (U, List[i--]);
    }
    while (j >= 0) {
      assert (t > 0);
      G->G[--t] = G->G[j--];
    }
    assert (!t);
    return len - c;
  }

  int tt = MIN_USER_GROUPS;
  while (tt < len) { tt <<= 1; }
  
  U->grp = G = zmalloc (sizeof (struct user_groups) + 4*tt);
  G->cur_groups = len;
  G->tot_groups = tt;
  
  for (i = 0; i < len; i++) {
    G->G[i] = List[i];
    add_user_group (U, List[i]);
  }

  return len;
}

static int del_groups (user_t *U, int List[], int len) {
  if (len < 0) {
    return -1;
  }
  if (!U || !len || !U->grp) {
    return 0;
  }

  struct user_groups *G = U->grp;
  int i, j = 0, k = 0;

  for (i = 1; i < len; i++) {
    assert (List[i-1] < List[i]);
  }


  for (i = 0; i < G->cur_groups; i++) {
    while (j < len && List[j] < G->G[i]) {
      j++;
    }
    if (j < len && List[j] == G->G[i]) {
      del_user_group (U, List[j++]);
    } else {
      G->G[k++] = G->G[i];
    }
  }

  i -= k;
  G->cur_groups = k;
  return i;
}

// sgn_mask >= 0 -- delete positive, <0 -- delete negative
static int del_some_groups (user_t *U, int sgn_mask) {
  if (!U || !U->grp) {
    return 0;
  }

  struct user_groups *G = U->grp;
  int i, k = 0;

  for (i = 0; i < G->cur_groups; i++) {
    if ((G->G[i] ^ sgn_mask) >= 0) {
      del_user_group (U, G->G[i]);
    } else {
      G->G[k++] = G->G[i];
    }
  }

  i -= k;
  G->cur_groups = k;
  return i;
}

static int add_user_lang (user_t *U, int lang_id) {
  if ((unsigned) lang_id >= MAX_LANGS) { 
    return -1;
  }
  if (lang_id > max_lang_id) {
    max_lang_id = lang_id;
  }
  if (lang_id < min_lang_id) {
    min_lang_id = lang_id;
  }

  user_lang_pairs++;

  add_user_word (U->uid, field_value_hash (q_lang_id, lang_id));

  return 1;
}

static int del_user_lang (user_t *U, int lang_id) {
  if ((unsigned) lang_id >= MAX_LANGS) { 
    return -1;
  }

  user_lang_pairs--;

  delete_user_word (U->uid, field_value_hash (q_lang_id, lang_id));

  return 1;
}

static int add_langs (user_t *U, int List[], int len) {
  struct user_langs *L = U->langs;

  if (len < 0 || len > MAX_USER_LANGS) {
    return -1;
  }
  if (!len || (L && L->cur_langs >= MAX_USER_LANGS)) {
    return 0;
  }

  int i, j, c = 0;
  assert (List[0] >= 0);
  for (i = 1; i < len; i++) {
    if (List[i-1] >= List[i]) {
      fprintf (stderr, "add_langs: user_id=%d, len=%d,", U->user_id, len);
      int j;
      for (j = 0; j < len; j++) {
        fprintf (stderr, " %d", List[j]);
      }
      fprintf (stderr, ".\n");
    }
    assert (List[i-1] < List[i]);
  }

  if (L) {
    assert (L->cur_langs >= 0 && L->cur_langs <= L->tot_langs);
    i = j = 0;
    while (i < len && j < L->cur_langs) {
      if (List[i] < L->L[j]) { i++; }
      else if (List[i] > L->L[j]) { j++; }
      else { c++;  i++;  j++; }
    }
    int t = L->cur_langs + len - c, tt = L->tot_langs;
    if (t > tt) {
      while (t > tt) { tt <<= 1; }
      struct user_langs *LN = zmalloc (sizeof (struct user_langs) + 2 * tt);
      memcpy (LN, L, sizeof(struct user_langs) + 2 * L->cur_langs);
      zfree (L, sizeof (struct user_langs) + 2 * L->tot_langs);
      U->langs = L = LN;
      L->tot_langs = tt;
    }

    i = len-1;
    j = L->cur_langs-1;
    L->cur_langs = t;

    while (i >= 0 && j >= 0) {
      assert (t > 0);
      if (List[i] > L->L[j]) {
        L->L[--t] = List[i];
        add_user_lang (U, List[i--]);
      } else {
        if (List[i] == L->L[j]) {
          i--;
        }
        L->L[--t] = L->L[j--];
      }
    }
    while (i >= 0) {
      assert (t > 0);
      L->L[--t] = List[i];
      add_user_lang (U, List[i--]);
    }
    while (j >= 0) {
      assert (t > 0);
      L->L[--t] = L->L[j--];
    }
    assert (!t);
    return len - c;
  }

  int tt = MIN_USER_LANGS;
  while (tt < len) { tt <<= 1; }
  
  U->langs = L = zmalloc (sizeof (struct user_langs) + 2 * tt);
  L->cur_langs = len;
  L->tot_langs = tt;
  
  for (i = 0; i < len; i++) {
    L->L[i] = List[i];
    add_user_lang (U, List[i]);
  }

  return len;
}

static int del_langs (user_t *U, int List[], int len) {
  if (len < 0) {
    return -1;
  }
  if (!U || !len || !U->langs) {
    return 0;
  }

  struct user_langs *L = U->langs;
  int i, j = 0, k = 0;

  assert (List[0] >= 0);
  for (i = 1; i < len; i++) {
    assert (List[i-1] < List[i]);
  }


  for (i = 0; i < L->cur_langs; i++) {
    while (j < len && List[j] < L->L[i]) {
      j++;
    }
    if (j < len && List[j] == L->L[i]) {
      del_user_lang (U, List[j++]);
    } else {
      L->L[k++] = L->L[i];
    }
  }

  i -= k;
  L->cur_langs = k;
  return i;
}

static void rehash_user_interests (user_t *U) {
  struct interest *I;
  if (!U) {
    return;
  }
  Hc = 0;
  for (I = U->inter; I; I = I->next) {
    prepare_words_hashlist (I->text, 1, -1, 0, q_interests);
  }
  if (U->name) {
    prepare_words_hashlist (U->name, 1, 2, 0, q_interests);
  }
  delete_user_hashlist (U->uid, U->inter_hashes);
  U->inter_hashes = save_words_hashlist (0, -1, q_interests);
  add_user_hashlist (U->uid, U->inter_hashes);
}

static int perform_targeting (struct lev_targ_target *E) {
  struct advert *A;
  assert (E->ad_query_len > 0 && !E->ad_query[E->ad_query_len]);
  assert (E->ad_query_len <= MAX_QUERY_STRING_LEN);
  if (E->ad_id <= 0 || targeting_disabled) {
    if (verbosity > 1) {
      fprintf (stderr, "store_targeting(): ad_id=%d, ad_price=%d\n", E->ad_id, E->ad_price);
    }
    return 15 + E->ad_query_len;
  }
  assert (E->ad_price != 0 && E->ad_id > 0);
  A = get_ad_f (E->ad_id, 1);

  if (!A) { return -1; }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  if (A->price != E->ad_price && E->ad_price != 0) { 
    A->price = E->ad_price; 
  }

  if (!A->query || strcmp (A->query, E->ad_query)) {
    remove_queue_ad (A);
    adjust_ctr_counters (A);
    if (!A->query) {
      exact_strfree (A->query);
    }
    A->query = exact_strdup (E->ad_query, E->ad_query_len);
    if (A->price) {
      change_ad_target (A);
    }
  } else if (!(A->flags & ADF_ON) && A->price) {
    ad_enable (A, A->price);
  }
  
  compute_estimated_gain (A);

  return 15 + E->ad_query_len;
}

int set_has_photo (struct lev_generic *E) {
  user_t *U = get_user (E->a);
  if (!U) { return 0; }
  int has_photo = E->type & 0xff;
  user_change_field (U->uid, q_has_photo, (U->has_photo & 0x81) == 1, (has_photo & 0x81) == 1);
  user_change_field (U->uid, q_uses_apps, (U->has_photo & 0x82) == 2, (has_photo & 0x82) == 2);
  user_change_field (U->uid, q_pays_money, (U->has_photo & 0x84) == 4, (has_photo & 0x84) == 4);
  U->has_photo = has_photo;
  return 1;
}

int set_privacy (struct lev_generic *E) {
  user_t *U = get_user (E->a);
  if (!U || E->b < 0 || E->b > MAX_PRIVACY) { return 0; }
  U->privacy = E->b;
  return 1;
}

#define set_something(something,SOMETHING)			       \
int set_##something (struct lev_generic *E) {			       \
  assert (E->type == LEV_TARG_##SOMETHING);			       \
  user_t *U = get_user (E->a);					       \
  if (!U || E->b < 0 || E->b > MAX_##SOMETHING) { return 0; }	       \
  user_change_field (U->uid, q_##something, U->something, E->b);       \
  U->something = E->b;						       \
  return 1;							       \
}

#define set_something_fix(something,SOMETHING)			       \
int set_##something (struct lev_generic *E) {			       \
  assert (E->type == LEV_TARG_##SOMETHING);			       \
  user_t *U = get_user (E->a);					       \
  if (!U) { return 0; }						       \
  int val = E->b;						       \
  if (val < 0 || val > MAX_##SOMETHING) { val = 0; }		       \
  user_change_field (U->uid, q_##something, U->something, val);	       \
  U->something = val;						       \
  return 1;							       \
}

#define set_something_negative(something,SOMETHING)		       \
int set_##something (struct lev_generic *E) {			       \
  assert (E->type == LEV_TARG_##SOMETHING);			       \
  user_t *U = get_user (E->a);					       \
  if (!U || E->b < -MAX_##SOMETHING || E->b > MAX_##SOMETHING) { return 0; } \
  user_change_field (U->uid, q_##something, U->something, E->b);	\
  U->something = E->b;						       \
  return 1;							       \
}

set_something_fix(sex,SEX)
set_something(operator,OPERATOR)
set_something(browser,BROWSER)
set_something(region,REGION)
set_something(height,HEIGHT)
set_something(smoking,SMOKING)
set_something(alcohol,ALCOHOL)
set_something(ppriority,PPRIORITY)
set_something(iiothers,IIOTHERS)
set_something(hidden,HIDDEN)
set_something(cvisited,CVISITED)
set_something(gcountry,GCOUNTRY)
set_something(custom1,CUSTOM1)
set_something(custom2,CUSTOM2)
set_something(custom3,CUSTOM3)
set_something(custom4,CUSTOM4)
set_something(custom5,CUSTOM5)
set_something(custom6,CUSTOM6)
set_something(custom7,CUSTOM7)
set_something(custom8,CUSTOM8)
set_something(custom9,CUSTOM9)
set_something(custom10,CUSTOM10)
set_something(custom11,CUSTOM11)
set_something(custom12,CUSTOM12)
set_something(custom13,CUSTOM13)
set_something(custom14,CUSTOM14)
set_something(custom15,CUSTOM15)
set_something_negative(timezone,TIMEZONE)
set_something(political,POLITICAL)
set_something(mstatus,MSTATUS)



void set_country_city (struct lev_univcity *E) {
  user_t *U = get_user (E->user_id);
  if (U && E->uni_country >= 0 && E->uni_country <= 255 && E->uni_city >= 0) { 
    user_change_field (U->uid, q_country, U->uni_country, E->uni_country);
    user_change_field (U->uid, q_city, U->uni_city, E->uni_city);
    U->uni_country = E->uni_country;
    U->uni_city = E->uni_city;
  }
}


void set_rate_cute (struct lev_ratecute *E) {
  assert (E->type == LEV_TARG_RATECUTE);
  user_t *U = get_user (E->user_id);
  if (U) {
    U->cute = E->cute;
    rate_change (U, E->rate);
  }
}


void set_rate (struct lev_rate *E) {
  assert (E->type == LEV_TARG_RATE);
  user_t *U = get_user (E->user_id);
  if (U) {
    rate_change (U, E->rate);
  }
}


void set_cute (struct lev_cute *E) {
  assert (E->type == LEV_TARG_CUTE);
  user_t *U = get_user (E->user_id);
  if (U) {
    U->cute = E->cute;
  }
}


static void user_clear_interests (user_t *U, int type) {
  assert (type <= MAX_INTERESTS && type >= -1);
  struct interest *I, **to = &U->inter;
  int deleted = 0;
  for (I = U->inter; I; I = *to) {
    if (I->type == type || type <= 0) {
      *to = I->next;
      deleted++;
      /* dealloc current I ... */
      zfree (I, strlen (I->text) + 1 + offsetof (struct interest, text));
    } else {
      to = &I->next;
    }
  }
  if (deleted > 0 || type < 0) {
    rehash_user_interests (U);
  }
}


static void user_clear_education (user_t *U) {
  struct education *E, *Nx;
  for (E = U->edu; E; E = Nx) {
#define DEL(__x,__f) user_clear_field (U->uid, __f, E->__x);  
    DEL(grad_year, q_graduation);
    DEL(chair, q_chair);
    DEL(faculty, q_faculty);
    DEL(university, q_univ);
    DEL(city, q_uni_city);
    DEL(country, q_uni_country);
    DEL(edu_form, q_edu_form);
    DEL(edu_status, q_edu_status);
#undef DEL
    Nx = E->next;
    zfree (E, sizeof (struct education));
  }
  U->edu = 0;
}


static void user_clear_schools (user_t *U) {
  struct school *S, *Nx;
  for (S = U->sch; S; S = Nx) {
#define DEL(__x,__f) user_clear_field (U->uid, __f, S->__x);  
    DEL(city, q_sch_city);
    DEL(school, q_sch_id);
    DEL(grad, q_sch_grad);
    DEL(country, q_sch_country);
    DEL(sch_class, q_sch_class);
#undef DEL
    delete_user_hashlist (U->uid, S->spec_hashes);
    exact_strfree (S->spec);
    Nx = S->next;
    zfree (S, sizeof (struct school));
  }
  U->sch = 0;
}


static void user_clear_work (user_t *U) {
  struct company *C, *Nx;
  for (C = U->work; C; C = Nx) {
    delete_user_hashlist (U->uid, C->job_hashes);
    delete_user_hashlist (U->uid, C->name_hashes);
    exact_strfree (C->company_name);
    exact_strfree (C->job);
    Nx = C->next;
    zfree (C, sizeof (struct company));
  }
  U->work = 0;
}


static void user_clear_addresses (user_t *U) {
  struct address *A, *Nx;
  if (verbosity > 1 && U->user_id == debug_user_id) {
    fprintf (stderr, "clear_addr(user_id=%d), now=%d\n", debug_user_id, now);
  }
  for (A = U->addr; A; A = Nx) {
#define DEL(__x,__f) user_clear_field (U->uid, __f, A->__x);  
    DEL(atype, q_adr_type);
    DEL(country, q_adr_country);
    DEL(city, q_adr_city);
    DEL(district, q_adr_district);
    DEL(station, q_adr_station);
    DEL(street, q_adr_street);
#undef DEL
    delete_user_hashlist (U->uid, A->house_hashes);
    delete_user_hashlist (U->uid, A->name_hashes);
    exact_strfree (A->house);
    exact_strfree (A->name);
    Nx = A->next;
    if (((long) Nx) & 3) {
      fprintf (stderr, "A=%p, next=%p\n", A, Nx);
    }
    if (((long) A) & 3) {
      fprintf (stderr, "zfree(%p), dyn_first=%p, dyn_cur=%p\n", A, dyn_first, dyn_cur);
    }
    zfree (A, sizeof (struct address));
  }
  U->addr = 0;
}

static void user_clear_military (user_t *U) {
  struct military *M, *Nx;
  for (M = U->mil; M; M = Nx) {
#define DEL(__x,__f) user_clear_field (U->uid, __f, M->__x);  
    DEL(unit_id, q_mil_unit);
    DEL(start, q_mil_start);
    DEL(finish, q_mil_finish);
#undef DEL
    Nx = M->next;
    zfree (M, sizeof (struct military));
  }
  U->mil = 0;
}

static void user_clear_groups (user_t *U) {
  struct user_groups *G = U->grp;

  if (G) {
    int i;
    for (i = 0; i < G->cur_groups; i++) { 
      del_user_group (U, G->G[i]);
    }
    zfree (G, sizeof (struct user_groups) + G->tot_groups*4);
  }

  U->grp = 0;
}

static void user_clear_langs (user_t *U) {
  struct user_langs *L = U->langs;

  if (L) {
    int i;
    for (i = 0; i < L->cur_langs; i++) { 
      del_user_lang (U, L->L[i]);
    }
    zfree (L, sizeof (struct user_langs) + L->tot_langs*2);
  }

  U->langs = 0;
}





int store_hometown (user_t *U, char *hometown, int len) {
  int i;
  assert (len <= 255);
  assert (!hometown[len]);
  for (i = 0; i < len; i++) {
    assert ((unsigned char) hometown[i] >= ' ');
  }
  exact_strfree (U->hometown);
  delete_user_hashlist (U->uid, U->hometown_hashes);

  if (len) {
    U->hometown = exact_strdup (hometown, len);
    U->hometown_hashes = save_words_hashlist (U->hometown, 0, q_hometown);
    add_user_hashlist (U->uid, U->hometown_hashes);
  } else {
    U->hometown = 0;
    U->hometown_hashes = 0;
  }

  return 1;
}

int store_proposal (user_t *U, char *proposal, int len) {
  int i;
  assert (len <= 1023);
  assert ((!len && !proposal) || !proposal[len]);
  for (i = 0; i < len; i++) {
    assert ((unsigned char) proposal[i] >= ' ');
  }
  exact_strfree (U->proposal);
  delete_user_hashlist (U->uid, U->proposal_hashes);

  if (len) {
    U->proposal = exact_strdup (proposal, len);
    prepare_words_hashlist ("anyproposal", 0, -1, 0, q_proposal);
    U->proposal_hashes = save_words_hashlist (U->proposal, -1, q_proposal);
    add_user_hashlist (U->uid, U->proposal_hashes);
  } else {
    U->proposal = 0;
    U->proposal_hashes = 0;
  }

  return 1;
}

int user_clear_proposal (user_t *U) {
  int res = (U->proposal != 0);
  store_proposal (U, 0, 0);
  return res;
}

#define clear_something(something) void clear_##something (struct lev_generic *E) { \
  user_t *U = get_user (E->a);						\
  if (U) {								\
    user_clear_##something (U);						\
  }									\
}

clear_something(education);
clear_something(schools);
clear_something(work);
clear_something(addresses);
clear_something(military);
clear_something(groups);
clear_something(langs);
clear_something(proposal);

static int clear_positive_groups (struct lev_generic *E) {
  return del_some_groups (get_user (E->a), 0);
}

static int clear_negative_groups (struct lev_generic *E) {
  return del_some_groups (get_user (E->a), -1);
}

static void clear_interests (struct lev_generic *E) {
  user_t *U = get_user (E->a);
  if (U) {
    user_clear_interests (U, E->type & 0xff);
  }
}

static int modify_stats (struct lev_targ_stat_load *E, struct advert *A) {
  tot_clicks -= A->l_clicked;
  tot_views -= A->views;
  tot_click_money -= A->click_money;

  A->l_clicked = E->clicked > 0 ? E->clicked : 0;
  A->click_money = E->click_money > 0 ? E->click_money * MONEY_SCALE : 0;
  A->views = E->views > 0 ? E->views : 0;
  A->l_clicked_old = E->l_clicked > 0 ? E->l_clicked : 0;
  A->l_views = E->l_views > 0 ? E->l_views : 0;

  tot_clicks += A->l_clicked;
  tot_views += A->views;
  tot_click_money += A->click_money;
  
  total_sump0 -= A->l_sump0;
  total_sump1 -= A->l_sump1;
  total_sump2 -= A->l_sump2;
  A->g_sump0 -= A->l_sump0;
  A->g_sump1 -= A->l_sump1;
  A->g_sump2 -= A->l_sump2;
  A->l_sump0 = 0;
  A->l_sump1 = 0;
  A->l_sump2 = 0;
  A->flags &= ~ADF_ANYVIEWS;
  compute_estimated_gain (A);

  return 0;
}


static inline void retarget_ad (struct advert *A) {
  if (targeting_disabled) {
    return;
  }
  assert (A);
  remove_queue_ad (A);
  assert ((A->flags & (ADF_ON | ADF_PERIODIC)) == (ADF_ON | ADF_PERIODIC));
  vkprintf (2, "invoking change_ad_target (%d, '%s')\n", A->ad_id, A->query);
  change_ad_target (A);
}


static int set_ad_ctr (struct lev_generic *E) {
  if (targeting_disabled) {
    return 0;
  }
  struct advert *A = get_ad_f (E->a, 0);
  assert (A && A->price > 0);
  long long g_clicks, g_views;

  if (E->type == LEV_TARG_AD_SETCTR) {
    g_clicks = ((struct lev_targ_ad_setctr *)E)->clicks;
    g_views = ((struct lev_targ_ad_setctr *)E)->views;
  } else {
    assert ((E->type & -0x100) == LEV_TARG_AD_SETCTR_PACK);
    g_clicks = E->type & 0xff;
    g_views = ((struct lev_targ_ad_setctr_pack *)E)->views;
  }
  assert (g_clicks >= 0 && g_views > 0 && g_clicks < MAX_G_CLICKS);

  A->g_clicked_old = g_clicks - A->l_clicked_old * 10LL;
  A->g_views = g_views - A->l_views * 10LL;
  compute_estimated_gain (A);

  return 1;
}

static int set_ad_sump (struct lev_targ_ad_setsump *E) {
  if (targeting_disabled) {
    return 0;
  }
  struct advert *A = get_ad_f (E->ad_id, 0);
  assert (A && A->price > 0);
  assert (E->type == LEV_TARG_AD_SETSUMP);
  assert (E->sump0 >= 0 && E->sump1 >= 0 && E->sump2 >= 0);

  A->g_sump0 = (double) E->sump0 * (1.0 / (1LL << 32));
  A->g_sump1 = (double) E->sump1 * (1.0 / (1LL << 32));
  A->g_sump2 = (double) E->sump2 * (1.0 / (1LL << 32));

  compute_estimated_gain (A);

  return 1;
}

static int set_ad_aud (struct lev_targ_ad_setaud *E) {
  if (targeting_disabled) {
    return 0;
  }
  struct advert *A = get_ad_f (E->ad_id, 0);
  assert (A && !(A->flags & ADF_ANCIENT) && E->aud > 0 && E->aud < MAX_AD_AUD && A->price > 0);
  A->ext_users = E->aud;
  compute_estimated_gain (A);
  return 1;
}

static int set_global_click_stats (struct lev_targ_global_click_stats *E) {
  int i, len = E->len;
  assert (len == MAX_AD_VIEWS);
  long long sum_v = 0, sum_c = 0;
  for (i = 0; i < MAX_AD_VIEWS; i++) {
    assert (!(E->stats[i].views < 0 || E->stats[i].clicks < 0 || E->stats[i].clicks > E->stats[i].views || E->stats[i].views > (long long) 1e15));
    if (i > 0) {
      sum_v += E->stats[i].views;
      sum_c += E->stats[i].clicks;
      assert (sum_v <= (long long) 1e15);
    }
  }
  assert (sum_v == E->stats[0].views && sum_c == E->stats[0].clicks);
  memcpy (AdStats.g, E->stats, 16 * MAX_AD_VIEWS);
  return 1;
}

static int ad_limit_user_views (struct lev_generic *E) {
  int ad_id = E->a;
  int views = E->type & 0xffff;
  assert (!views || views == 100);
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));
  if (!A) { 
    return -1; 
  }

  assert (load_ancient_ad (A) >= 0);
  assert (A->price < 0);

  if (!views) {
    A->flags &= ~ADF_LIMIT_VIEWS;
  } else {
    A->flags |= ADF_LIMIT_VIEWS;
  }

  return 1;
}

static int ad_change_sites (struct lev_generic *E) {
  int ad_id = E->a;
  int allow = (E->type & -0x100) == LEV_TARG_AD_SETSITEMASK ? E->type & 0xff : E->type & 1;
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));
  if (!A) { 
    return -1; 
  }

  assert (load_ancient_ad (A) >= 0);

  A->flags = (A->flags & ~ADF_SITES_MASK) | (allow << ADF_SITES_MASK_SHIFT);

  return 1;
}

static int ad_limit_recent_views (struct lev_generic *E) {
  int ad_id = E->a;
  int views = E->type & 0xffff;
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));
  if (!A) { 
    return -1; 
  }

  assert (load_ancient_ad (A) >= 0);

  A->recent_views_limit = views;

  return 1;
}

static int ad_set_factor (struct lev_targ_ad_setint *E) {
  assert (E->value >= 0 && E->value <= 1000000);
  struct advert *A = get_ad_f (E->ad_id, ad_is_ancient (E->ad_id));
  if (!A) { 
    return -1; 
  }

  assert (load_ancient_ad (A) >= 0);

  A->factor = E->value * 1e-6;

  return 1;
}

static int ad_set_domain (struct lev_targ_ad_setint *E) {
  assert (E->value >= 0 && E->value <= MAX_AD_DOMAIN);
  struct advert *A = get_ad_f (E->ad_id, ad_is_ancient (E->ad_id));
  if (!A) { 
    return -1; 
  }

  assert (load_ancient_ad (A) >= 0);

  A->domain = E->value;

  return 1;
}

static int ad_set_group (struct lev_targ_ad_setint *E) {
  assert (E->value != (-1 << 31));
  struct advert *A = get_ad_f (E->ad_id, ad_is_ancient (E->ad_id));
  if (!A) { 
    return -1; 
  }

  assert (load_ancient_ad (A) >= 0);

  A->group = E->value;

  return 1;
}

static int ad_set_categories (struct lev_targ_ad_setint *E) {
  short category = E->value & 0xffff, subcategory = E->value >> 16;
  assert (category >= 0 && category <= MAX_AD_CATEGORY && subcategory >= 0 && subcategory <= MAX_AD_CATEGORY);
  struct advert *A = get_ad_f (E->ad_id, ad_is_ancient (E->ad_id));
  if (!A) { 
    return -1; 
  }

  assert (load_ancient_ad (A) >= 0);

  A->category = category;
  A->subcategory = subcategory;

  return 1;
}

int targ_replay_logevent (struct lev_generic *E, int size) {
  int s, t;
  user_t *U;
  struct advert *A;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return targ_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_TARG_USERNAME ... LEV_TARG_USERNAME+0xff:
    s = E->type & 0xff;
    if (size < 9 + s) { return -2; }
    set_username ((struct lev_username *)E, s);
    return 9 + s;
  case LEV_TARG_USER_HASPHOTO ... LEV_TARG_USER_HASPHOTO+0xff:
    if (size < 8) { return -2; }
    set_has_photo (E);
    return 8;
#define case_LEV_TARG_SOMETHING(something,SOMETHING)	\
    case LEV_TARG_##SOMETHING: \
      if (size < 12) { return -2; } \
      set_##something (E);\
      return 12;
  case_LEV_TARG_SOMETHING(political,POLITICAL);
  case_LEV_TARG_SOMETHING(mstatus,MSTATUS);
  case_LEV_TARG_SOMETHING(sex,SEX);
  case_LEV_TARG_SOMETHING(operator,OPERATOR);
  case_LEV_TARG_SOMETHING(browser,BROWSER);
  case_LEV_TARG_SOMETHING(region,REGION);
  case_LEV_TARG_SOMETHING(height,HEIGHT);
  case_LEV_TARG_SOMETHING(smoking,SMOKING);
  case_LEV_TARG_SOMETHING(alcohol,ALCOHOL);
  case_LEV_TARG_SOMETHING(ppriority,PPRIORITY);
  case_LEV_TARG_SOMETHING(iiothers,IIOTHERS);
  case_LEV_TARG_SOMETHING(hidden,HIDDEN);
  case_LEV_TARG_SOMETHING(cvisited,CVISITED);
  case_LEV_TARG_SOMETHING(gcountry,GCOUNTRY);
  case_LEV_TARG_SOMETHING(custom1,CUSTOM1);
  case_LEV_TARG_SOMETHING(custom2,CUSTOM2);
  case_LEV_TARG_SOMETHING(custom3,CUSTOM3);
  case_LEV_TARG_SOMETHING(custom4,CUSTOM4);
  case_LEV_TARG_SOMETHING(custom5,CUSTOM5);
  case_LEV_TARG_SOMETHING(custom6,CUSTOM6);
  case_LEV_TARG_SOMETHING(custom7,CUSTOM7);
  case_LEV_TARG_SOMETHING(custom8,CUSTOM8);
  case_LEV_TARG_SOMETHING(custom9,CUSTOM9);
  case_LEV_TARG_SOMETHING(custom10,CUSTOM10);
  case_LEV_TARG_SOMETHING(custom11,CUSTOM11);
  case_LEV_TARG_SOMETHING(custom12,CUSTOM12);
  case_LEV_TARG_SOMETHING(custom13,CUSTOM13);
  case_LEV_TARG_SOMETHING(custom14,CUSTOM14);
  case_LEV_TARG_SOMETHING(custom15,CUSTOM15);
  case_LEV_TARG_SOMETHING(privacy,PRIVACY);
  case_LEV_TARG_SOMETHING(timezone,TIMEZONE);
  case LEV_TARG_UNIVCITY:
    if (size < 16) { return -2; }
    set_country_city ((struct lev_univcity *) E);
    return 16;
  case LEV_TARG_BIRTHDAY:
    if (size < 12) { return -2; }
    set_birthday((struct lev_birthday *) E); 
    return 12;
  case LEV_TARG_RATECUTE:
    if (size < 16) { return -2; }
    set_rate_cute ((struct lev_ratecute *) E);
    return 16;
  case LEV_TARG_RATE:
    if (size < 12) { return -2; }
    set_rate ((struct lev_rate *) E);
    return 12;
  case LEV_TARG_CUTE:
    if (size < 12) { return -2; }
    set_cute ((struct lev_cute *) E);
    return 12;
  case LEV_TARG_ONLINE:
    if (size < 16) { return -2; }
    process_user_online_lite ((struct lev_online_lite *) E);
    return 16;
  case LEV_TARG_ONLINE_LITE:
    if (size < 8) { return -2; }
    process_user_online_lite ((struct lev_online_lite *) E);
    return 8;
  case LEV_TARG_DELUSER:
    if (size < 8) { return -2; }
    delete_user ((struct lev_delete_user *) E);
    return 8;
  case LEV_TARG_DELGROUP:
    if (size < 8) { return -2; }
    delete_group ((struct lev_delete_group *) E);
    return 8;
  case LEV_TARG_EDUADD ... LEV_TARG_EDUADD_PRIM:
    if (size < 32) { return -2; }
    U = get_user (E->a);
    if (U) {
      store_edu (U->uid, &U->edu, (struct lev_education *) E);
    }
    return 32;
  case LEV_TARG_RELIGION ... LEV_TARG_RELIGION+0xff:
    return store_religion ((struct lev_religion *) E, size);
  case LEV_TARG_HOMETOWN ... LEV_TARG_HOMETOWN+0xff:
    t = E->type & 0xff;
    if (size < 9 + t) { return -2; }
    U = get_user (E->a);
    if (U) {
      store_hometown (U, ((struct lev_hometown *) E)->text, t);
    }
    return 9 + t;
  case LEV_TARG_PROPOSAL:
    if (size < 11) { return -2; }
    t = ((struct lev_proposal *) E)->len;
    if (size < 11 + t) { return -2; }
    U = get_user (E->a);
    if (U) {
      store_proposal (U, ((struct lev_proposal *) E)->text, t);
    }
    return 11 + t;
#define case_LEV_TARG_SOMETHING_CLEAR(something,SOMETHING)	\
    case LEV_TARG_##SOMETHING##_CLEAR: \
      if (size < 8) { return -2; } \
      clear_##something (E);\
      return 8;
    case_LEV_TARG_SOMETHING_CLEAR(education, EDU);
    case_LEV_TARG_SOMETHING_CLEAR(schools, SCH);
    case_LEV_TARG_SOMETHING_CLEAR(work, COMP);
    case_LEV_TARG_SOMETHING_CLEAR(addresses, ADDR);
    case_LEV_TARG_SOMETHING_CLEAR(military, MIL);
    case_LEV_TARG_SOMETHING_CLEAR(groups, GROUP);
    case_LEV_TARG_SOMETHING_CLEAR(positive_groups, GROUP_POS);
    case_LEV_TARG_SOMETHING_CLEAR(negative_groups, GROUP_NEG);
    case_LEV_TARG_SOMETHING_CLEAR(langs, LANG);
    case_LEV_TARG_SOMETHING_CLEAR(proposal, PROPOSAL);
  case LEV_TARG_SCH_ADD ... LEV_TARG_SCH_ADD+0xff:
    return store_school ((struct lev_school *) E, size);
  case LEV_TARG_COMP_ADD ... LEV_TARG_COMP_ADD+0x1ff:
    return store_company ((struct lev_company *) E, size);
  case LEV_TARG_ADDR_ADD:
    if (size < 26) { return -2; }
    U = get_user (E->a);
    if (U) {
      store_address (U, (struct lev_address_extended *) E, 0);
    }
    return 26;
  case LEV_TARG_ADDR_EXT_ADD ... LEV_TARG_ADDR_EXT_ADD + 0xff:
    t = (E->type & 0xff);
    if (size < t + 27) { return -2; }
    U = get_user (E->a);
    if (U) {
      store_address (U, (struct lev_address_extended *) E, t);
    }
    return t + 27;
  case LEV_TARG_GRTYPE_CLEAR:
    if (size < 8) { return -2; }
    U = get_user (E->a);
    if (U) {
      for (t = 0; t < 4; t++) {
	U->user_group_types[t] = 0;
      }
    }
    return 8;
  case LEV_TARG_GRTYPE_ADD ... LEV_TARG_GRTYPE_ADD+0x7f:
    if (size < 8) { return -2; }
    set_user_single_group_type (E);
    return 8;
  case LEV_TARG_INTERESTS_CLEAR ... LEV_TARG_INTERESTS_CLEAR + MAX_INTERESTS:
    if (size < 8) { return -2; }
    clear_interests (E);
    return 8;
  case LEV_TARG_INTERESTS+1 ... LEV_TARG_INTERESTS + MAX_INTERESTS:
    return store_interests ((struct lev_interests *) E, size);
  case LEV_TARG_MIL_ADD:
    if (size < 16) { return -2; }
    U = get_user (E->a);
    if (U) {
      store_military (U, (struct lev_military *) E);
    }
    return 16;
  case LEV_TARG_GROUP_ADD + 1 ... LEV_TARG_GROUP_ADD + 0xff:
    t = E->type & 0xff;
    if (size < 8 + t * 4) { return -2; }
    U = get_user (E->a);
    if (U) {
      add_groups (U, ((struct lev_groups *) E)->groups, t);
    }
    return 8 + t * 4;
  case LEV_TARG_GROUP_EXT_ADD:
    t = ((struct lev_groups_ext *) E)->gr_num;
    if (t <= 0 || t > MAX_USER_LEV_GROUPS || size < 12 + t * 4) { return -2; }
    U = get_user (E->a);
    if (U) {
      add_groups (U, ((struct lev_groups_ext *) E)->groups, t);
    }
    return 12 + t * 4;
  case LEV_TARG_GROUP_DEL + 1 ... LEV_TARG_GROUP_DEL + 0xff:
    t = E->type & 0xff;
    if (size < 8 + t * 4) { return -2; }
    U = get_user (E->a);
    if (U) {
      del_groups (U, ((struct lev_groups *) E)->groups, t);
    }
    return 8 + t * 4;
  case LEV_TARG_GROUP_EXT_DEL:
    t = ((struct lev_groups_ext *) E)->gr_num;
    if (t <= 0 || t > MAX_USER_LEV_GROUPS || size < 12 + t * 4) { return -2; }
    U = get_user (E->a);
    if (U) {
      del_groups (U, ((struct lev_groups_ext *) E)->groups, t);
    }
    return 12 + t * 4;
  case LEV_TARG_LANG_ADD + 1 ... LEV_TARG_LANG_ADD + 0xff:
    t = E->type & 0xff;
    if (size < 8 + t * 4) { return -2; }
    U = get_user (E->a);
    if (U) {
      add_langs (U, ((struct lev_langs *) E)->langs, t);
    }
    return 8 + t * 4;
  case LEV_TARG_LANG_DEL + 1 ... LEV_TARG_LANG_DEL + 0xff:
    t = E->type & 0xff;
    if (size < 8 + t * 4) { return -2; }
    U = get_user (E->a);
    if (U) {
      del_langs (U, ((struct lev_langs *) E)->langs, t);
    }
    return 8 + t * 4;
  case LEV_TARG_TARGET:
    if (size < 15 || size < 15 + ((struct lev_targ_target *) E)->ad_query_len) {
      return -2;
    }
    return perform_targeting ((struct lev_targ_target *) E);
  case LEV_TARG_AD_ON:
    if (size < 8) { return -2; }
    ad_enable (get_ad_f (E->a, ad_is_ancient (E->a)), 0);
    return 8;
  case LEV_TARG_AD_OFF:
    if (size < 8) { return -2; }
    ad_disable (get_ad_f (E->a, 0));
    return 8;
  case LEV_TARG_AD_PRICE:
    if (size < 12) { return -2; }
    set_ad_price ((struct lev_targ_ad_price *) E);
    return 12;
  case LEV_TARG_CLICK:
    if (size < 12) { return -2; }
    register_user_click ((struct lev_targ_click_ext *)E);
    return 12;
  case LEV_TARG_CLICK_EXT:
    if (size < 16) { return -2; }
    register_user_click ((struct lev_targ_click_ext *)E);
    return 16;
  case LEV_TARG_VIEWS:
    if (size < 12) { return -2; }
    incr_ad_views (E->a, E->b, 0);
    return 12;
  case LEV_TARG_USER_VIEW:
    if (size < 12) { return -2; }
    register_user_view ((struct lev_targ_user_view *)E);
    return 12;
  case LEV_TARG_STAT_LOAD:
    if (size < sizeof (struct lev_targ_stat_load)) { return -2; }
    A = get_ad_f (E->a, 0);
    if (A) {
      modify_stats ((struct lev_targ_stat_load *) E, A);
    } else {
      assert (!ad_is_ancient (E->a));
    }
    return sizeof (struct lev_targ_stat_load);
  case LEV_TARG_AD_RETARGET:
    if (size < 8) { return -2; }
    retarget_ad (get_ad_f (E->a, 0));
    return 8;
  case LEV_TARG_AD_SETAUD:
    if (size < 12) { return -2; }
    set_ad_aud ((struct lev_targ_ad_setaud *)E);
    return 12;
  case LEV_TARG_AD_SETCTR:
    if (size < 20) { return -2; }
    set_ad_ctr (E);
    return 20;
  case LEV_TARG_AD_SETCTR_PACK ... LEV_TARG_AD_SETCTR_PACK + 0xff:
    if (size < 12) { return -2; }
    set_ad_ctr (E);
    return 12;
  case LEV_TARG_AD_SETSUMP:
    if (size < 32) { return -2; }
    set_ad_sump ((struct lev_targ_ad_setsump *) E);
    return 32;
  case LEV_TARG_AD_LIMIT_USER_VIEWS ... LEV_TARG_AD_LIMIT_USER_VIEWS + 0xffff:
    if (size < 8) { return -2; }
    ad_limit_user_views (E);
    return 8;
  case LEV_TARG_AD_DO_NOT_ALLOW_SITES ... LEV_TARG_AD_DO_ALLOW_SITES:
  case LEV_TARG_AD_SETSITEMASK ... LEV_TARG_AD_SETSITEMASK + 0xff:
    if (size < 8) { return -2; }
    ad_change_sites (E);
    return 8;
  case LEV_TARG_AD_SETFACTOR:
    if (size < 12) { return -2; }
    ad_set_factor ((struct lev_targ_ad_setint *) E);
    return 12;
  case LEV_TARG_AD_SETDOMAIN:
    if (size < 12) { return -2; }
    ad_set_domain ((struct lev_targ_ad_setint *) E);
    return 12;
  case LEV_TARG_AD_SETCATEGORIES:
    if (size < 12) { return -2; }
    ad_set_categories ((struct lev_targ_ad_setint *) E);
    return 12;
  case LEV_TARG_AD_SETGROUP:
    if (size < 12) { return -2; }
    ad_set_group ((struct lev_targ_ad_setint *) E);
    return 12;
  case LEV_TARG_AD_LIMIT_RECENT_VIEWS ... LEV_TARG_AD_LIMIT_RECENT_VIEWS + 0xffff:
    if (size < 8) { return -2; }
    ad_limit_recent_views (E);
    return 8;
  case LEV_TARG_GLOBAL_CLICK_STATS:
    if (size < 8) { return -2; }
    if (E->a != MAX_AD_VIEWS) { return -1; }
    if (size < 8 + 16 * E->a) { return -2; }
    set_global_click_stats ((struct lev_targ_global_click_stats *) E);
    return 8 + 16 * E->a;
  case LEV_TARG_USER_GROUP_TYPES:
    if (size < 24) { return -2; }
    set_user_group_types ((struct lev_targ_user_group_types *) E);
    return 24;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}

/* adding new log entries */

int do_delete_group (int group_id) {
  struct lev_delete_group *E = alloc_log_event (LEV_TARG_DELGROUP, 8, group_id);
  return delete_group (E);
}

int perform_pricing (int position, int flags, int and_mask, int xor_mask) {
  int res;
  if (position <= 0 || position > 100) { return -1; }
  R_position = position;

  if (Q_limit <= 0) { Q_limit = 1000; }
  if (Q_limit > 10000) { Q_limit = 10000; }
  memset (R, 0, Q_limit * sizeof(int));

  heap_push_user_ad = (flags & 8) ? heap_push_user_ad_ext : heap_push_user_ad_std;
  __use_factor = 1;
  __use_views_limit = 0;
  __exclude_ad_id = 0;
  __and_mask = (and_mask & 0xff) << ADF_SITES_MASK_SHIFT;
  __xor_mask = (xor_mask & 0xff) << ADF_SITES_MASK_SHIFT;
  __cat_mask = -1LL;

  res = perform_query (0);

  __use_factor = 0;
  __use_views_limit = 0;
  __and_mask = (254 << ADF_SITES_MASK_SHIFT);
  __xor_mask = 0;

  R_position = 0;
  if (res > 0) { 
    R_cnt = Q_limit; 
    while (R_cnt && !R[R_cnt-1]) { R_cnt--; }
  }
  return res;
}

int perform_ad_pricing (int ad_id, int position, int flags, int and_mask, int xor_mask, int max_users) {
  R_cnt = 0;
  if (ad_id <= 0 || position <= 0 || position > 100 || max_users <= 0 || max_users > 1000) { 
    return -1; 
  }
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A || !A->user_list) {
    return -1;
  }

  if (Q_limit <= 0) {
    Q_limit = 1000;
  }
  if (Q_limit > 10000) {
    Q_limit = 10000;
  }
  memset (R, 0, Q_limit * sizeof(int));

  heap_push_user_ad = (flags & 8) ? heap_push_user_ad_ext : heap_push_user_ad_std;
  __use_factor = 1;
  __use_views_limit = 0;
  __exclude_ad_id = ad_id;
  __and_mask = (and_mask & 0xff) << ADF_SITES_MASK_SHIFT;
  __xor_mask = (xor_mask & 0xff) << ADF_SITES_MASK_SHIFT;
  __cat_mask = -1LL;

  long i, N = A->users;
  if (max_users > N) {
    max_users = N;
  }
  long M = max_users;
  for (i = 0; i < A->users; i++) {
    if (lrand48() % N < M) {
      M--;
      int r = user_ad_price (A->user_list[i], position);
      if (r >= Q_limit) {
	r = Q_limit - 1;
      } else if (r < 0) {
	r = 0;
      }
      R[r]++;
    }
    N--;
  }
  __use_factor = 0;
  __use_views_limit = 0;
  __exclude_ad_id = 0;
  __and_mask = (254 << ADF_SITES_MASK_SHIFT);
  __xor_mask = 0;

  R_cnt = Q_limit; 
  while (R_cnt && !R[R_cnt-1]) { R_cnt--; }
  return max_users;
}

/* interface to user ad list */
// +1 - write expected gain
// +2 - write price
// +4 - write views
// +8 - use new algorithm
// +16 - on external site
int compute_user_ads (int user_id, int limit, int flags, int and_mask, int xor_mask, long long cat_mask) {
  user_t *U = get_user (user_id);
  int N;

// fprintf (stderr, "compute_user_ads(%d,%d,%d,%d:%d): U=%p\n", user_id, limit, flags, and_mask, xor_mask, U);

  if (!U || limit <= 0) { return -1; }

  if (limit > (MAX_USERS >> 2)) {
    limit = (MAX_USERS >> 2);
  }

  if (flags & 16) {
    and_mask |= 1;
    xor_mask |= 1;
  }

  heap_push_user_ad = (flags & 8) ? heap_push_user_ad_ext : heap_push_user_ad_std;
  __use_factor = 1;
  __use_views_limit = 1;
  __exclude_ad_id = 0;
  __and_mask = (and_mask & 0xff) << ADF_SITES_MASK_SHIFT;
  __xor_mask = (xor_mask & 0xff) << ADF_SITES_MASK_SHIFT;
  __cat_mask = cat_mask;

  build_user_ad_heap (U, limit);

  __use_factor = 0;
  __use_views_limit = 0;
  __and_mask = (254 << ADF_SITES_MASK_SHIFT);
  __xor_mask = 0;
  __cat_mask = -1LL;

  N = HN;
  assert (N >= 0 && N <= limit);

  if (flags & 7) {
    int *R_end = R + HN * (1 + (flags & 1) + ((flags >> 1) & 1) + ((flags >> 2) & 1));
    while (HN > 0) {
      long ad_id = H[1].ad_id;
      if (flags & 4) {
	*--R_end = H[1].views;
      }
      if (flags & 2) {
	*--R_end = get_ad (ad_id)->price;
      }
      if (flags & 1) {
	double gain = H[1].expected_gain * EXPECTED_GAIN_USERADS_MULTIPLIER;
	*--R_end = (int) (gain < 0 ? 0 : (gain > 2e9 ? 2e9 : gain + 0.5));
      }
      *--R_end = ad_id;
      heap_pop ();
    }
    assert (R_end == R);
  } else {
    while (HN > 0) {
      R[HN-1] = H[1].ad_id;
      heap_pop ();
    }
  }

  return R_cnt = N;
}


void sort_recent_views (long long *A, long B) {
  if (B <= 0) {
    return;
  }
  long i = 0, j = B;			       
  long long h = A[j >> 1], t;
  do {
    while (A[i] > h) {
      ++i;
    }
    while (h > A[j]) {
      --j;
    }
    if (i <= j) {
      t = A[i]; A[i] = A[j]; A[j] = t;
      ++i; --j;
    }
  } while (i <= j);
  sort_recent_views (A, j);
  sort_recent_views (A + i, B - i);
}

static void do_llswap (int *R, long l) {
  long i;
  if (Q_order == -2) {
    return;
  } else if (Q_order < 0) {
    for (i = 0; i < l; i++) {
      int t = R[2*i];
      R[2*i] = R[2*i+1];
      R[2*i+1] = t;
    }
  } else if (Q_order == 2) {
    for (i = 0; i < l; i++) {
      R[2*i+1] = -R[2*i+1];
    }
  } else {
    for (i = 0; i < l; i++) {
      int t = R[2*i];
      R[2*i] = -R[2*i+1];
      R[2*i+1] = -t;
    }
  }
}

int postprocess_recent_list (int count) {
  R_cnt = 0;
  if (!count || Q_limit < 0) {
    return 0;
  }
  if (!Q_limit) {
    return count;
  }
  sort_recent_views ((long long *)R, count - 1);
  long i, j = 0;
  R[1] = 1;
  for (i = 1; i < count; i++) {
    if (R[i * 2] == R[j * 2]) {
      ++R[j * 2 + 1];
    } else {
      ++j;
      R[j * 2] = R[i * 2];
      R[j * 2 + 1] = 1;
    }
  }
  ++j;
  do_llswap (R, j);
  sort_recent_views ((long long *)R, j - 1);
  do_llswap (R, j);
  if (Q_limit > j) {
    Q_limit = j;
  }
  R_cnt = Q_limit * 2;
  return count;
}

int compute_recent_views_stats (void) {
  if (!Q_limit) {
    R_cnt = 0;
    return get_recent_views_num ();
  }
  assert (CYCLIC_VIEWS_BUFFER_SIZE <= MAX_USERS / 2);
  struct cyclic_views_entry *from;
  long long *dest = (long long *) R;

  if (CV_r > CV_w) {
    for (from = CV_r; from < CViews + CYCLIC_VIEWS_BUFFER_SIZE; from++) {
      *dest++ = from->ad_id;
    }
    for (from = CViews; from < CV_w; from++) {
      *dest++ = from->ad_id;
    }
  } else {
    for (from = CV_r; from < CV_w; from++) {
      *dest++ = from->ad_id;
    }
  }
  int cnt = dest - (long long *)R;
  return postprocess_recent_list (cnt);
}

int compute_recent_ad_viewers (int ad_id) {
  assert (CYCLIC_VIEWS_BUFFER_SIZE <= MAX_USERS / 2);
  struct cyclic_views_entry *from;
  long long *dest = (long long *) R;

  if (CV_r > CV_w) {
    for (from = CV_r; from < CViews + CYCLIC_VIEWS_BUFFER_SIZE; from++) {
      if (from->ad_id == ad_id) { 
	*dest++ = from->user_id;
      }
    }
    for (from = CViews; from < CV_w; from++) {
      if (from->ad_id == ad_id) { 
	*dest++ = from->user_id;
      }
    }
  } else {
    for (from = CV_r; from < CV_w; from++) {
      if (from->ad_id == ad_id) { 
	*dest++ = from->user_id;
      }
    }
  }
  int cnt = dest - (long long *)R;
  return postprocess_recent_list (cnt);
}

int compute_recent_user_ads (int user_id) {
  assert (CYCLIC_VIEWS_BUFFER_SIZE <= MAX_USERS / 2);
  struct cyclic_views_entry *from;
  long long *dest = (long long *) R;

  if (CV_r > CV_w) {
    for (from = CV_r; from < CViews + CYCLIC_VIEWS_BUFFER_SIZE; from++) {
      if (from->user_id == user_id) { 
	*dest++ = from->ad_id;
      }
    }
    for (from = CViews; from < CV_w; from++) {
      if (from->user_id == user_id) { 
	*dest++ = from->ad_id;
      }
    }
  } else {
    for (from = CV_r; from < CV_w; from++) {
      if (from->user_id == user_id) { 
	*dest++ = from->ad_id;
      }
    }
  }
  int cnt = dest - (long long *)R;
  return postprocess_recent_list (cnt);
}


int get_user_groups (int user_id) {
  user_t *U = get_user (user_id);
  if (!U || !U->grp) {
    R_cnt = 0;
    return 0;
  }
  struct user_groups *G = U->grp;
  int cnt = G->cur_groups;
  R_cnt = (cnt > MAX_USERS ? MAX_USERS : cnt);
  memcpy (R, G->G, R_cnt * 4);
  return cnt;
}

int do_ad_price_enable (int ad_id, int price) {
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));
  int res;
  if (!A) {
    if (verbosity > 0) {
      fprintf (stderr, "warning: enabling undefined ad %d\n", ad_id);
    }
    return 0;
  }
  res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }
  res = 0;
  if (price != 0 && A->price != price) {
    struct lev_targ_ad_price *E = alloc_log_event (LEV_TARG_AD_PRICE, 12, ad_id);
    E->ad_price = price;
    set_ad_price (E);
    res = 1; 
  }
  if (!(A->flags & ADF_ON)) {
    alloc_log_event (LEV_TARG_AD_ON, 8, ad_id);
    ad_enable (A, 0);
    res |= 1;
  }
  return res;
}

int do_ad_disable (int ad_id) {
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A && verbosity > 0 && !ad_is_ancient (ad_id)) {
    fprintf (stderr, "warning: disabling undefined ad %d\n", ad_id);
  }
  if (A && (A->flags & ADF_ON)) {
    alloc_log_event (LEV_TARG_AD_OFF, 8, ad_id);
    ad_disable (A);
    return 1;
  }
  return 0;
}

int do_perform_targeting (int ad_id, int price, int factor, char *query) {
  struct advert *A = get_ad_f (ad_id, 1);
  int len = strlen (query);

  if (verbosity > 0) {
    fprintf (stderr, "in do_perform_targeting(%d,%d,%d,'%s'):\n", ad_id, price, factor, query);
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  aux_userlist_tag = 0;

  if (compile_query (query) || !Qq || !A || !price || ad_id <= 0 || len > MAX_QUERY_STRING_LEN) {
    return -1;
  }

  if (factor && (factor <= (int)1e5 || factor > (int)1e6)) {
    return -1;
  }

  if (A->query && !strcmp (A->query, query)) {
    assert (do_ad_price_enable (ad_id, price) >= 0);
    if (factor) {
      assert (do_ad_set_factor (ad_id, factor) == 1);
    }
    return A->users;
  }

  struct lev_targ_target *E = alloc_log_event (LEV_TARG_TARGET, 15 + len, ad_id);
  E->ad_price = price;
  E->ad_query_len = len;
  memcpy (E->ad_query, query, len + 1);

  assert (perform_targeting (E) >= 0);

  if (factor) {
    assert (do_ad_set_factor (ad_id, factor) == 1);
  }

  return A->users; 
}



int do_register_user_click (int user_id, int ad_id, int price) {
  struct advert *A = get_ad_f (ad_id, 0);
  struct lev_targ_click_ext *E;
  int uid = conv_user_id (user_id);
  if (verbosity > 1) {
    fprintf (stderr, "user %d clicks on ad %d, price %d\n", user_id, ad_id, price);
  }
  if (uid < 0) {
    if (verbosity > 0) {
      fprintf (stderr, "error: unknown user %d, click neglected\n", user_id);
    }
    return -1;
  }
  if (!A || (A->flags & ADF_ANCIENT) || ad_became_ancient (A) || (A->price > 0 && price < 0)) {
    if (verbosity > 0) {
      fprintf (stderr, "warning: user %d clicks on ad %d, price %d; known ad price is %d, click ignored\n", 
	user_id, ad_id, price, A ? A->price : -1);
    }
    return -1;
  }
  if (A->price == price || (A->price < 0 && price < 0)) {
    E = alloc_log_event (LEV_TARG_CLICK, 12, ad_id);
  } else {
    E = alloc_log_event (LEV_TARG_CLICK_EXT, 16, ad_id);
    E->price = price;
  }
  E->user_id = user_id;

  return register_user_click (E);
}

int do_register_user_view (int user_id, int ad_id) {
  struct advert *A = get_ad_f (ad_id, 0);
  int uid = conv_user_id (user_id);

  if (verbosity > 2) {
    fprintf (stderr, "user %d views ad %d\n", user_id, ad_id);
  }

  if (uid < 0) {
    if (verbosity > 0) {
      fprintf (stderr, "error: unknown user %d, view ignored\n", user_id);
    }
    return -1;
  }
  if (!A || (A->flags & ADF_ANCIENT) || ad_became_ancient (A)) {
    if (verbosity > 0) {
      fprintf (stderr, "error: unknown or ancient ad %d, view ignored\n", ad_id);
    }
    return -1;
  }

  struct lev_targ_user_view *E = alloc_log_event (LEV_TARG_USER_VIEW, 12, ad_id);
  E->user_id = user_id;

  return register_user_view (E);
}


int compute_ad_clicks (int ad_id) {
  struct advert *A = get_ad_f (ad_id, 0);
  return A ? A->l_clicked : 0;
}

int compute_ad_money (int ad_id) {
  struct advert *A = get_ad_f (ad_id, 0);
  return A ? A->click_money : 0;
}

int compute_ad_views (int ad_id) {
  struct advert *A = get_ad_f (ad_id, 0);
  return A ? A->views : 0;
}

int compute_ad_recent_views (int ad_id) {
  struct advert *A = get_ad_f (ad_id, 0);
  return A ? A->recent_views : 0;
}

int compute_ad_info (int ad_id, long long *res) {
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A) { return 0; }
  res[0] = A->ad_id;
  res[1] = A->flags;
  res[2] = A->price;
  res[3] = A->users;
  res[4] = A->views;
  res[5] = A->l_clicked;
  res[6] = A->click_money;
  res[7] = A->l_clicked_old;
  res[8] = A->l_views;
  res[9] = A->g_clicked_old;
  res[10] = A->g_views;
  res[11] = A->expected_gain * 1e9;
  res[12] = A->ext_users;
  res[13] = A->g_clicked;
  res[14] = A->l_sump0 * 1e9 + 0.5;
  res[15] = A->l_sump1 * 1e9 + 0.5;
  res[16] = A->l_sump2 * 1e9 + 0.5;
  res[17] = A->g_sump0 * 1e9 + 0.5;
  res[18] = A->g_sump1 * 1e9 + 0.5;
  res[19] = A->g_sump2 * 1e9 + 0.5;
  if (A->price > 0) {
    res[20] = (A->lambda / A->price) * 1e9;
    res[21] = (A->delta / A->price) * (1e9 / sqrt (3));
  } else {
    res[20] = res[21] = 0;
  }
  res[22] = A->recent_views;
  res[23] = A->recent_views_limit;
  res[24] = A->factor * 1e6 + 0.5;
  res[25] = A->domain;
  res[26] = A->group;
  res[27] = A->category;
  res[28] = A->subcategory;
  return 29;
}

int compute_ad_ctr (int ad_id, long long *res) {
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A) { return 0; }
  res[0] = A->l_clicked_old;
  res[1] = A->l_views;
  res[2] = A->l_sump0 * (1LL << 32) + 0.5;
  res[3] = A->l_sump1 * (1LL << 32) + 0.5;
  res[4] = A->l_sump2 * (1LL << 32) + 0.5;
  return 5;
}

int compute_ad_user_clicked (int user_id, int ad_id) {
  user_t *U = get_user (user_id);
  if (!U) {
    return 0;
  }
  treeref_t N = intree_lookup (AdSpace, U->clicked_ads, ad_id);
  if (!N) {
    return 0;
  }
  int x = TNODE(AdSpace, N)->z;
  return x + (x >= 0);
}


int do_set_ad_ctr (int ad_id, long long g_clicks, long long g_views) {
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A || (A->flags & ADF_ANCIENT) || ad_became_ancient (A) || g_clicks < 0 || g_views <= 0 || g_clicks >= MAX_G_CLICKS) { 
    return 0; 
  }
  if (A->price <= 0) {
    return 1;
  }
  double new_estimated_gain = compute_estimated_gain_clicks (A, g_clicks, g_views);
  if (new_estimated_gain >= (1 - ESTIMATED_GAIN_EPS) * A->expected_gain && new_estimated_gain <= (1 + ESTIMATED_GAIN_EPS) * A->expected_gain) {
    return 1;
  }
  if (g_clicks < 256 && g_views <= 0x7fffffff) {
    struct lev_targ_ad_setctr_pack *E = (struct lev_targ_ad_setctr_pack *)alloc_log_event (LEV_TARG_AD_SETCTR_PACK + g_clicks, 12, ad_id);
    E->views = g_views;
    return set_ad_ctr ((struct lev_generic *)E);
  } else {
    struct lev_targ_ad_setctr *E = (struct lev_targ_ad_setctr *)alloc_log_event (LEV_TARG_AD_SETCTR, 20, ad_id);
    E->clicks = g_clicks;
    E->views = g_views;
    return set_ad_ctr ((struct lev_generic *)E);
  }
}

int do_set_ad_sump (int ad_id, long long g_sump0, long long g_sump1, long long g_sump2) {
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A || (A->flags & ADF_ANCIENT) || ad_became_ancient (A) || g_sump0 < 0 || g_sump1 <= 0 || g_sump2 <= 0) {
    return 0; 
  }
  if (A->price <= 0) {
    return 1;
  }
  double f = 1.0 / (1LL << 32), new_delta;
  double new_lambda = compute_ad_lambda_delta (A->price, g_sump0 * f, g_sump1 * f, g_sump2 * f, &new_delta);
  if (new_delta >= (1 - ESTIMATED_GAIN_EPS) * A->delta && new_delta <= (1 + ESTIMATED_GAIN_EPS) * A->delta && new_lambda >= (1 - ESTIMATED_GAIN_EPS) * A->lambda && new_lambda <= (1 + ESTIMATED_GAIN_EPS) * A->lambda) {
    return 1;
  }
  struct lev_targ_ad_setsump *E = (struct lev_targ_ad_setsump *) alloc_log_event (LEV_TARG_AD_SETSUMP, 32, ad_id);
  E->sump0 = g_sump0;
  E->sump1 = g_sump1;
  E->sump2 = g_sump2;
  return set_ad_sump (E);
}

int do_set_ad_ctr_sump (int ad_id, long long g_clicks, long long g_views, long long g_sump0, long long g_sump1, long long g_sump2) {
  return do_set_ad_ctr (ad_id, g_clicks, g_views) & do_set_ad_sump (ad_id, g_sump0, g_sump1, g_sump2);
}


int do_set_ad_aud (int ad_id, int aud) {
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A || (A->flags & ADF_ANCIENT) || ad_became_ancient (A) || aud <= 0 || aud >= MAX_AD_AUD) {
    return 0;
  }
  if (A->price <= 0 || A->ext_users == aud) {
    return 1;
  }
  struct lev_targ_ad_setaud *E = (struct lev_targ_ad_setaud *)alloc_log_event (LEV_TARG_AD_SETAUD, 12, ad_id);
  E->aud = aud;
  return set_ad_aud (E);
}



char *get_ad_query (int ad_id) {
  struct advert *A = get_ad_f (ad_id, 0);
  if (!A) { return 0; }
  return A->query;
}

int do_user_visit (int user_id, const char *addr, int len) {
  user_t *U = get_user (user_id);

  if (!U) {
    if (verbosity > 0) {
      fprintf (stderr, "error: unknown user %d, visit neglected\n", user_id);
    }
    return 0;
  }

  if (verbosity > 1) {
    fprintf (stderr, "setting last_visited for user %d to %d\n", user_id, now);
  }

  if (now - U->last_visited >= MAX_ONLINE_DELAY) {
    struct lev_online_lite *E = alloc_log_event (LEV_TARG_ONLINE_LITE, 8, user_id);
    process_user_online_lite (E);
  }

  return 1;
}

int do_ad_limit_user_views (int ad_id, int views) {
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));

  if (verbosity > 0) {
    fprintf (stderr, "in do_ad_limit_user_views (%d, %d):\n", ad_id, views);
  }

  if (!A || (views && views != 100)) {
    return 0;
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  if (A->price >= 0) {
    return 0;
  }

  struct lev_generic *E = alloc_log_event (LEV_TARG_AD_LIMIT_USER_VIEWS + views, 8, ad_id);
  return ad_limit_user_views (E);
}

int do_ad_change_sites (int ad_id, int ext_sites) {
 struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));

  if (verbosity > 0) {
    fprintf (stderr, "in do_ad_change_sites (%d, %d):\n", ad_id, ext_sites);
  }

  if (!A || (ext_sites & -0x100)) {
    return 0;
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  struct lev_generic *E = alloc_log_event (LEV_TARG_AD_SETSITEMASK + ext_sites, 8, ad_id);
  return ad_change_sites (E);
}

int do_ad_limit_recent_views (int ad_id, int views) {
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));

  if (verbosity > 0) {
    fprintf (stderr, "in do_ad_limit_recent_views (%d, %d):\n", ad_id, views);
  }

  if (!A || views < 0) {
    return 0;
  }

  if (views >= 0x10000) {
    views = 0xffff;
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  if (A->recent_views_limit == views) {
    return 1;
  }

  struct lev_generic *E = alloc_log_event (LEV_TARG_AD_LIMIT_RECENT_VIEWS + views, 8, ad_id);
  return ad_limit_recent_views (E);
}

int do_ad_set_factor (int ad_id, int factor) {
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));

  if (verbosity > 0) {
    fprintf (stderr, "in do_ad_set_factor (%d, %d):\n", ad_id, factor);
  }

  if (!A || factor <= (int) 1e5 || factor > (int) 1e6) {
    return 0;
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  if ((int) (A->factor * 1e6 + 0.5) == factor) {
    return 1;
  }

  struct lev_targ_ad_setint *E = alloc_log_event (LEV_TARG_AD_SETFACTOR, 12, ad_id);
  E->value = factor;

  return ad_set_factor (E);
}

int do_ad_set_domain (int ad_id, int domain) {
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));

  if (verbosity > 0) {
    fprintf (stderr, "in do_ad_set_domain (%d, %d):\n", ad_id, domain);
  }

  if (!A || domain < 0 || domain > MAX_AD_DOMAIN) {
    return 0;
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  if (A->domain == domain) {
    return 1;
  }

  struct lev_targ_ad_setint *E = alloc_log_event (LEV_TARG_AD_SETDOMAIN, 12, ad_id);
  E->value = domain;

  return ad_set_domain (E);
}

int do_ad_set_categories (int ad_id, int category, int subcategory) {
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));

  if (verbosity > 0) {
    fprintf (stderr, "in do_ad_set_categories (%d, %d, %d):\n", ad_id, category, subcategory);
  }

  if (!A || category < 0 || category > MAX_AD_CATEGORY || subcategory < 0 || subcategory > MAX_AD_CATEGORY) {
    return 0;
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  if (A->category == category && A->subcategory == subcategory) {
    return 1;
  }

  struct lev_targ_ad_setint *E = alloc_log_event (LEV_TARG_AD_SETCATEGORIES, 12, ad_id);
  E->value = category | (subcategory << 16);

  return ad_set_categories (E);
}

int do_ad_set_group (int ad_id, int group) {
  struct advert *A = get_ad_f (ad_id, ad_is_ancient (ad_id));

  if (verbosity > 0) {
    fprintf (stderr, "in do_ad_set_group (%d, %d):\n", ad_id, group);
  }

  if (!A || group == (-1 << 31)) {
    return 0;
  }

  int res = load_ancient_ad (A);
  if (res < 0) {
    return res;
  }

  if (A->group == group) {
    return 1;
  }

  struct lev_targ_ad_setint *E = alloc_log_event (LEV_TARG_AD_SETGROUP, 12, ad_id);
  E->value = group;

  return ad_set_group (E);
}

/*
 *
 *  UPDATE USER INFO
 *
 */

/*static char *filter_tagged_text (char *to, const char *text, int len) {
  char *q = to, *end = to + len;
  while (q < end) {
    if (*text == 0x1f) {
      do {
	*q++ = *text++;
      } while (q < end && (unsigned char) *text >= 0x40);
    } else if ((unsigned char) *text < ' ' && *text != 9) {
      *q++ = ' ';
      text++;
    } else {
      *q++ = *text++;
    }
  }
  *q = 0;
  return to;
}*/

static char *filter_text (char *to, const char *text, int len) {
  char *q = to, *end = to + len;
  while (q < end) {
    if ((unsigned char) *text < ' ' && *text != 9) {
      *q++ = ' ';
      text++;
    } else {
      *q++ = *text++;
    }
  }
  *q = 0;
  return to;
}


static char *filter_simple_text (char *to, const char *text, int len) {
  char *q = to, *end = to + len;
  while (q < end) {
    if ((unsigned char) *text < ' ') {
      *q++ = ' ';
      text++;
    } else {
      *q++ = *text++;
    }
  }
  *q = 0;
  return to;
}


int do_set_rate_cute (int user_id, int rate, int cute) {
  if (!get_user (user_id)) { return 0; }
  struct lev_ratecute *E = alloc_log_event (LEV_TARG_RATECUTE, 16, user_id);
  E->rate = rate;
  E->cute = cute;
  set_rate_cute (E);
  return 1;
}

int do_set_cute (int user_id, int cute) {
  if (!get_user (user_id)) { return 0; }
  struct lev_cute *E = alloc_log_event (LEV_TARG_CUTE, 12, user_id);
  E->cute = cute;
  set_cute (E);
  return 1;
}

int do_set_rate (int user_id, int rate) {
  if (!get_user (user_id)) { return 0; }
  struct lev_rate *E = alloc_log_event (LEV_TARG_RATE, 12, user_id);
  E->rate = rate;
  set_rate (E);
  return 1;
}

int do_set_has_photo (int user_id, int flags) {
  if ((flags & (-1 << 24)) || !get_user (user_id)) {
    return 0;
  }
  struct lev_generic *E = alloc_log_event (LEV_TARG_USER_HASPHOTO + (flags & 0xff), 8, user_id);
  return set_has_photo (E);
}

int get_has_photo (int user_id) {
  user_t *U = get_user (user_id);
  if (U) {
    return U->has_photo & 0x07;
  }
  return -1;
}

int do_set_username (int user_id, const char *text, int len) {
  if ((unsigned)len >= 256 || conv_user_id (user_id) < 0) {
    return 0;
  }
  struct lev_username *E = alloc_log_event (LEV_TARG_USERNAME + len, 9 + len, user_id);
  filter_text (E->s, text, len);
  return set_username (E, len);
}


int do_set_user_group_types (int user_id, unsigned user_group_types[4]) {
  user_t *U = get_user (user_id);
  if (!U) {
    return 0;
  }
  if (!memcmp (user_group_types, U->user_group_types, 16)) {
    return 1;
  }
  struct lev_targ_user_group_types *E = (struct lev_targ_user_group_types *) alloc_log_event (LEV_TARG_USER_GROUP_TYPES, 24, user_id);
  memcpy (E->user_group_types, user_group_types, 16);
  return set_user_group_types (E);
}

int do_set_user_single_group_type (int user_id, unsigned type) {
  user_t *U = get_user (user_id);
  if (!U || type > 127) {
    return 0;
  }
  if (U->user_group_types[type >> 5] & (1 << (type & 31))) {
    return 1;
  }
  struct lev_generic *E = alloc_log_event (LEV_TARG_GRTYPE_ADD + type, 8, user_id);
  return set_user_single_group_type (E);
}

#define do_set_something(something,SOMETHING) int do_set_##something (int user_id, int something) {\
  if (something < 0 || something > MAX_##SOMETHING) {\
    return 0;\
  }\
  user_t *U = get_user (user_id);\
  if (!U) {\
    return 0;\
  }\
  if (U->something == something) {\
    return 1;\
  }\
  struct lev_generic *E = alloc_log_event (LEV_TARG_##SOMETHING, 12, user_id);\
  E->b = something;\
  return set_##something (E);\
}

do_set_something(sex,SEX)
do_set_something(operator,OPERATOR)
do_set_something(browser,BROWSER)
do_set_something(region,REGION)
do_set_something(height,HEIGHT)
do_set_something(smoking,SMOKING)
do_set_something(alcohol,ALCOHOL)
do_set_something(ppriority,PPRIORITY)
do_set_something(iiothers,IIOTHERS)
do_set_something(hidden,HIDDEN)
do_set_something(cvisited,CVISITED)
do_set_something(gcountry,GCOUNTRY)
do_set_something(custom1,CUSTOM1)
do_set_something(custom2,CUSTOM2)
do_set_something(custom3,CUSTOM3)
do_set_something(custom4,CUSTOM4)
do_set_something(custom5,CUSTOM5)
do_set_something(custom6,CUSTOM6)
do_set_something(custom7,CUSTOM7)
do_set_something(custom8,CUSTOM8)
do_set_something(custom9,CUSTOM9)
do_set_something(custom10,CUSTOM10)
do_set_something(custom11,CUSTOM11)
do_set_something(custom12,CUSTOM12)
do_set_something(custom13,CUSTOM13)
do_set_something(custom14,CUSTOM14)
do_set_something(custom15,CUSTOM15)
do_set_something(privacy,PRIVACY)
do_set_something(political,POLITICAL)
do_set_something(mstatus,MSTATUS)

#define do_set_something_negative(something,SOMETHING) int do_set_##something (int user_id, int something) {\
  if (something < -MAX_##SOMETHING || something > MAX_##SOMETHING) {\
    return 0;\
  }\
  user_t *U = get_user (user_id);\
  if (!U) {\
    return 0;\
  }\
  if (U->something == something) {\
    return 1;\
  }\
  struct lev_generic *E = alloc_log_event (LEV_TARG_##SOMETHING, 12, user_id);\
  E->b = something;\
  return set_##something (E);\
}

do_set_something_negative(timezone,TIMEZONE)

int do_set_country_city (int user_id, int country, int city) {
  if (country < 0 || country >= 256 || city < 0 || !get_user (user_id)) {
    return 0;
  }
  struct lev_univcity *E = alloc_log_event (LEV_TARG_UNIVCITY, 16, user_id);
  E->uni_country = country;
  E->uni_city = city;
  set_country_city (E);
  return 1;
}

int do_set_birthday (int user_id, int day, int month, int year) {
  if (day < 0 || day > 31 || month < 0 || month > 12 || (year < 1900 && year) || year > 2008 || !get_user (user_id)) {
    return 0;
  }
  struct lev_birthday *E = alloc_log_event (LEV_TARG_BIRTHDAY, 12, user_id);
  E->year = year;
  E->day = day;
  E->month = month;
  set_birthday (E);
  return 1;
}

int do_set_religion (int user_id, const char *text, int len) {
  if (len < 0 || len >= 256 || conv_user_id (user_id) < 0) {
    return 0;
  }
  struct lev_religion *E = alloc_log_event (LEV_TARG_RELIGION + len, 9+len, user_id);

  filter_simple_text (E->str, text, len);

  user_t *U = get_user (E->user_id);

  if (U) {
    exact_strfree (U->religion);
    U->religion = exact_strdup (E->str, len);
    delete_user_hashlist (U->uid, U->religion_hashes);
    U->religion_hashes = save_words_hashlist (U->religion, 0, q_religion);
    add_user_hashlist (U->uid, U->religion_hashes);
    return 1;
  }

  return 0;
}

int do_set_hometown (int user_id, const char *text, int len) {
  if (len < 0 || len >= 256 || conv_user_id (user_id) < 0) {
    return 0;
  }
  struct lev_hometown *E = alloc_log_event (LEV_TARG_HOMETOWN + len, 9+len, user_id);

  filter_simple_text (E->text, text, len);

  user_t *U = get_user (E->user_id);

  if (U) {
    return store_hometown (U, E->text, len);
  } else {
    return 0;
  }
};

int do_set_proposal (int user_id, const char *text, int len) {
  if (len < 0 || len >= 1024 || conv_user_id (user_id) < 0) {
    return 0;
  }
  struct lev_proposal *E = alloc_log_event (LEV_TARG_PROPOSAL, 11+len, user_id);

  E->len = len;
  filter_simple_text (E->text, text, len);

  user_t *U = get_user (E->user_id);

  if (U) {
    return store_proposal (U, E->text, len);
  } else {
    return 0;
  }
};


int do_set_school (int user_id, struct school *S) {
  int len = S->spec ? strlen (S->spec) : 0;
  if (len >= 256 || conv_user_id (user_id) < 0) {
    return 0;
  }
  struct lev_school *E = alloc_log_event (LEV_TARG_SCH_ADD + len, 26 + len, user_id);
  E->city = S->city;
  E->school = S->school;
  E->start = S->start;
  E->finish = S->finish;
  E->grad = S->grad;
  E->country = S->country;
  E->sch_class = S->sch_class;
  E->sch_type = S->sch_type;
  if (len) {
    filter_simple_text (E->spec, S->spec, len);
  }
  E->spec[len] = 0;
  store_school (E, 26+len);
  return 1;
}

int do_set_education (int user_id, struct education *E) {
  user_t *U = get_user (user_id);
  if (!U) {
    return 0;
  }

  struct lev_education *D = alloc_log_event (
    LEV_TARG_EDUADD + !!E->primary, sizeof (struct lev_education), user_id
  );

#define CPY(__x) D->__x = E->__x;  
  CPY(grad_year);
  CPY(chair);
  CPY(faculty);
  CPY(university);
  CPY(city);
  CPY(country);
  CPY(edu_form);
  CPY(edu_status);
#undef CPY
  store_edu (U->uid, &U->edu, D);
  return 1;
}

int do_set_work (int user_id, struct company *C) {
  int len1 = C->company_name ? strlen (C->company_name) : 0;
  int len2 = C->job ? strlen (C->job) : 0;
  if (len1 + len2 >= 255 || conv_user_id (user_id) < 0) {
    return 0;
  }
  struct lev_company *E = alloc_log_event (LEV_TARG_COMP_ADD + (len1 + len2 + 1),
    len1 + len2 + 23, user_id);

#define CPY(__x) E->__x = C->__x;  
  CPY(city);
  CPY(company);
  CPY(start);
  CPY(finish);
  CPY(country);
#undef CPY

  if (len1) {
    filter_simple_text (E->text, C->company_name, len1);
  }
  E->text[len1] = 9;
  if (len2) {
    filter_simple_text (E->text + len1 + 1, C->job, len2);
  }
  E->text[len1 + len2 + 1] = 0;
  store_company (E, len1 + len2 + 23);
  return 1;
}

int do_set_military (int user_id, int unit_id, int start, int finish) {
  user_t *U = get_user (user_id);
  if (!U) {
    return 0;
  }

  struct lev_military *E = alloc_log_event (LEV_TARG_MIL_ADD, sizeof (struct lev_military), user_id);

  E->unit_id = unit_id;
  E->start = start;
  E->finish = finish;

  store_military (U, E);
  return 1;
}

int do_set_address (int user_id, struct address *A) {
  user_t *U = get_user (user_id);
  if (!U) {
    return 0;
  }

  int len1 = A->house ? strlen (A->house) : 0;
  int len2 = A->name ? strlen (A->name) : 0;

  if (len1 + len2 + 1 >= 256) { 
    return 0; 
  }

  struct lev_address_extended *E = alloc_log_event (LEV_TARG_ADDR_EXT_ADD + (len1 + len2 + 1),
    (len1 + len2 + 1) + 27, user_id);

#define CPY(__x) E->__x = A->__x;  
  CPY(atype);
  CPY(country);
  CPY(city);
  CPY(district);
  CPY(station);
  CPY(street);
#undef CPY

  if (len1) {
    filter_simple_text (E->text, A->house, len1);
  }
  E->text[len1] = 9;
  if (len2) {
    filter_simple_text (E->text + len1 + 1, A->name, len2);
  }
  E->text[len1 + len2 + 1] = 0;
  store_address (U, E, len1 + len2 + 1);
  return 1;
}

int do_set_interest (int user_id, const char *text, int len, int type) {
  if (type < 1 || type > MAX_INTERESTS || (unsigned) len > 65520) {
    return 0;
  }
  struct lev_interests *E = alloc_log_event (LEV_TARG_INTERESTS + type, len + 11, user_id);
  E->len = len;
  filter_text (E->text, text, len);
  store_interests (E, len + 11);
  return 1;
}

int do_set_user_group (int user_id, int group_id) {
  user_t *U = get_user (user_id);
  if (!U) {
    return 0;
  }

  struct lev_groups *E = alloc_log_event (LEV_TARG_GROUP_ADD + 1, 12, user_id);
  E->groups[0] = group_id;

  return add_groups (U, E->groups, 1);
}

int do_set_user_lang (int user_id, int lang_id) {
  user_t *U = get_user (user_id);
  if (!U || lang_id < 0 || lang_id >= MAX_LANGS) {
    return 0;
  }

  struct lev_langs *E = alloc_log_event (LEV_TARG_LANG_ADD + 1, 12, user_id);
  E->langs[0] = lang_id;

  return add_langs (U, E->langs, 1);
}

int do_delete_user (int user_id) {
  if (conv_user_id (user_id) < 0) {
    return 0;
  }
  struct lev_delete_user *E = alloc_log_event (LEV_TARG_DELUSER, 8, user_id);
  return delete_user (E);
}


#define DO_DELETE_SOMETHING(something, SOMETHING) int do_delete_##something (int user_id) {\
  user_t *U = get_user(user_id);					\
  if (!U) {								\
    return 0;								\
  }									\
  struct lev_generic *E = alloc_log_event (LEV_TARG_##SOMETHING##_CLEAR, 8, user_id);\
  clear_##something (E);						\
  return 1;								\
}

DO_DELETE_SOMETHING(education, EDU);
DO_DELETE_SOMETHING(schools, SCH);
DO_DELETE_SOMETHING(work, COMP);
DO_DELETE_SOMETHING(addresses, ADDR);
DO_DELETE_SOMETHING(military, MIL);
DO_DELETE_SOMETHING(groups, GROUP);
DO_DELETE_SOMETHING(positive_groups, GROUP_POS);
DO_DELETE_SOMETHING(negative_groups, GROUP_NEG);
DO_DELETE_SOMETHING(langs, LANG);
DO_DELETE_SOMETHING(proposal, PROPOSAL);

int do_delete_interests (int user_id, int type) {
  user_t *U = get_user (user_id);
  if (!U || (unsigned) type > MAX_INTERESTS) {
    return 0;
  }
  struct lev_generic *E = alloc_log_event (LEV_TARG_INTERESTS_CLEAR + type, 8, user_id);
  clear_interests (E);
  return 1;
}

int do_delete_user_group (int user_id, int group_id) {
  user_t *U = get_user (user_id);
  if (!U) {
    return 0;
  }

  struct lev_groups *E = alloc_log_event (LEV_TARG_GROUP_DEL + 1, 12, user_id);
  E->groups[0] = group_id;

  return del_groups (U, E->groups, 1);
}

int do_del_user_groups (int user_id, int List[], int len) {
  user_t *U = get_user (user_id);
  if (!U || !len) {
    return 0;
  }
  if ((unsigned) len > MAX_USER_LEV_GROUPS) {
    return -1;
  }
  int i;
  for (i = 1; i < len; i++) {
    if (List[i] <= List[i-1]) {
      return -1;
    }
  }

  if (len < 256) {
    struct lev_groups *E = alloc_log_event (LEV_TARG_GROUP_DEL + len, 8 + len * 4, user_id);
    memcpy (E->groups, List, len * 4);
  } else {
    struct lev_groups_ext *E = alloc_log_event (LEV_TARG_GROUP_EXT_DEL, 12 + len * 4, user_id);
    E->gr_num = len;
    memcpy (E->groups, List, len * 4);
  }

  return del_groups (U, List, len);
}

int do_add_user_groups (int user_id, int List[], int len) {
  user_t *U = get_user (user_id);
  if (!U || !len) {
    return 0;
  }
  if ((unsigned) len > MAX_USER_LEV_GROUPS) {
    return -1;
  }
  int i;
  for (i = 1; i < len; i++) {
    if (List[i] <= List[i-1]) {
      return -1;
    }
  }

  if (len < 256) {
    struct lev_groups *E = alloc_log_event (LEV_TARG_GROUP_ADD + len, 8 + len * 4, user_id);
    memcpy (E->groups, List, len * 4);
  } else {
    struct lev_groups_ext *E = alloc_log_event (LEV_TARG_GROUP_EXT_ADD, 12 + len * 4, user_id);
    E->gr_num = len;
    memcpy (E->groups, List, len * 4);
  }

  return add_groups (U, List, len);
}

int do_set_user_groups (int user_id, int List[], int len) {
  user_t *U = get_user (user_id);
  if (!U) {
    return 0;
  }
  if ((unsigned) len > MAX_USER_LEV_GROUPS) {
    return -1;
  }
  int i;
  for (i = 1; i < len; i++) {
    if (List[i] <= List[i-1]) {
      return -1;
    }
  }

  if (!len) {
    return do_delete_groups (user_id);
  }
  do_delete_groups (user_id);
  
  return do_add_user_groups (user_id, List, len);
}

int do_delete_user_lang (int user_id, int lang_id) {
  user_t *U = get_user (user_id);
  if (!U || lang_id < 0 || lang_id >= MAX_LANGS) {
    return 0;
  }

  struct lev_langs *E = alloc_log_event (LEV_TARG_LANG_DEL + 1, 12, user_id);
  E->langs[0] = lang_id;

  return del_langs (U, E->langs, 1);
}

int do_set_global_click_stats (int len, struct views_clicks_ll *A) {
  int i;
  fprintf (stderr, "do_set_global_click_stats(%d)\n", len);
  if (len != MAX_AD_VIEWS) {
    return 0;
  }
  long long sum_v = 0, sum_c = 0;
  for (i = 0; i < MAX_AD_VIEWS; i++) {
    if (A[i].views < 0 || A[i].clicks < 0 || A[i].clicks > A[i].views || A[i].views > (long long) 1e15) {
      return 0;
    }
    if (i > 0) {
      sum_v += A[i].views;
      sum_c += A[i].clicks;
      if (sum_v > (long long) 1e15) {
        return 0;
      }
    }
  }
  if (sum_v != A[0].views || sum_c != A[0].clicks) {
    return 0;
  }
  struct lev_targ_global_click_stats *E = alloc_log_event (LEV_TARG_GLOBAL_CLICK_STATS, len * 16 + 8, len);
  memcpy (E->stats, A, len * 16);
  return set_global_click_stats (E);
}

int get_user_rate (int user_id) {
  user_t *U = get_user (user_id);
  return U ? U->rate : (-1 << 31);
}


int retarget_dynamic_ads (void) {
  if (binlog_disabled || targeting_disabled) {
    return 0;
  }
  if (!now) {
    now = time (0);
  }
  vkprintf (3, "retarget_dynamic_ads() first=%d last=%d now=%d\n", AHd_retarget.first->ad_id, AHd_retarget.last->ad_id, now);
  if (AHd_retarget.first != (struct advert *) &AHd_retarget) {
    struct advert *A = AHd_retarget.first;
    vkprintf (3, "first ad in retarget queue: ad #%d (%p), retarget_time=%d, now=%d\n", A->ad_id, A, A->retarget_time, now);
    if (A->retarget_time <= /* now */ log_last_ts) {
      alloc_log_event (LEV_TARG_AD_RETARGET, 8, A->ad_id);
      retarget_ad (A);
      return 1;
    }
  }
  return 0;
}

int retarget_all_dynamic_ads (void) {
  int res = 0;
  while (retarget_dynamic_ads () > 0 && !pending_signals) {
    res++;
  }
  return res;
}

int process_lru_ads (void) {
  int res = 0;
  vkprintf (3, "process_lru_ads() first=%d last=%d\n", AHd_lru.first->ad_id, AHd_lru.last->ad_id);
  while (AHd_lru.first != (struct advert *) &AHd_lru && AHd_lru.first->disabled_since <= log_last_ts - AD_ANCIENT_DELAY) {
    struct advert *A = AHd_lru.first;
    vkprintf (3, "first ad in lru queue: ad #%d (%p), disabled_since=%d, log_last_ts=%d\n", A->ad_id, A, A->disabled_since, log_last_ts);
    remove_queue_ad (A);
    assert (!(A->flags & ADF_ON));
    if (A->user_list) {
      tot_userlists--;
      tot_userlists_size -= A->users; 
      free (A->user_list);
      A->user_list = 0;
      A->userlist_computed_at = 0;
    }
    ++res;
  }
  return res;
}

