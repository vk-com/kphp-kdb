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

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kdb-friends-binlog.h"
#include "friend-data.h"
#include "server-functions.h"
#include "kfs.h"

extern int now;
extern int verbosity;

static inline void writeout (const void *D, size_t len);
static inline void writeout_char (char value);
static void *readin (size_t len);
static void readadv (size_t len);
static int unconv_uid (int user_id);

/* ---------- tree functions ------------ */

int alloc_tree_nodes;

static tree_t *new_tree_node (int x, int y) {
  tree_t *P;
  P = zmalloc (sizeof (tree_t));
  assert (P);
  alloc_tree_nodes++;
  P->left = P->right = 0;
  P->x = x;
  P->y = y;
  return P;
}

static void free_tree_node (tree_t *T) {
  zfree (T, sizeof (tree_t));
  alloc_tree_nodes--;
}

static tree_t *tree_lookup (tree_t *T, int x) {
  while (T && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

static void tree_split (tree_t **L, tree_t **R, tree_t *T, int x) {
  if (!T) { *L = *R = 0; return; }
  if (x < T->x) {
    *R = T;
    tree_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_split (&T->right, R, T->right, x);
  }
}

static tree_t *tree_insert (tree_t *T, int x, int y, int cat, int date) {
  tree_t *P;
  if (!T) { 
    P = new_tree_node (x, y);
    P->cat = cat;
    P->date = date;
    return P;
  }
  assert (x != T->x);
  if (T->y >= y) {
    if (x < T->x) {
      T->left = tree_insert (T->left, x, y, cat, date);
    } else {
      T->right = tree_insert (T->right, x, y, cat, date);
    }
    return T;
  }
  P = new_tree_node (x, y);
  P->cat = cat;
  P->date = date;
  tree_split (&P->left, &P->right, T, x);
  return P;
}

static tree_t *tree_merge (tree_t *L, tree_t *R) {
  if (!L) { return R; }
  if (!R) { return L; }
  if (L->y > R->y) {
    L->right = tree_merge (L->right, R);
    return L;
  } else {
    R->left = tree_merge (L, R->left);
    return R;
  }
}

static tree_t *tree_delete (tree_t *T, int x) {
  if (T->x == x) {
    tree_t *N = tree_merge (T->left, T->right);
    free_tree_node (T);
    return N;
  }
  if (x < T->x) {
    T->left = tree_delete (T->left, x);
  } else {
    T->right = tree_delete (T->right, x);
  }
  return T;
}

static void free_tree (tree_t *T) {
  if (T) {
    free_tree (T->left);
    free_tree (T->right);
    free_tree_node (T);
  }
}

static void dump_tree (tree_t *T) {
  if (T) {
    writeout_char (1);
  } else {
    writeout_char (0);
    return;
  }
  writeout (&T->x, 16);
  dump_tree (T->left);
  dump_tree (T->right);
}

static tree_t *read_tree (void) {
  char *ptr = readin (1);
  if (!ptr) {
    fprintf (stderr, "Unexpected end of file in read_tree\n");
    return 0;
  }
  char c = ptr[0];
  readadv (1);
  assert (c == 0 || c == 1);
  if (!c) {
    return 0;
  }
  ptr = readin (16);
  if (!ptr) {
    fprintf (stderr, "Unexpected end of file in read_tree\n");
    return 0;
  }
  tree_t *T = new_tree_node (0, 0);
  memcpy (&T->x, ptr, 16);
  readadv (16);
  T->left = read_tree ();
  T->right = read_tree ();
  return T;
}

/* ---------- rev_friends tree functions ------------ */

int alloc_rev_friends_nodes;
int rev_friends_intersect_len;
int rev_friends_intersect_pos;
int rev_friends_intersect_list[MAX_FRIENDS];

static rev_friends_t *new_rev_friends_node (int x1, int x2, int y) {
  rev_friends_t *P;
  P = zmalloc (sizeof (rev_friends_t));
  assert (P);
  alloc_rev_friends_nodes++;
  P->left = P->right = 0;
  P->x1 = x1;
  P->x2 = x2;
  P->y = y;
  return P;
}

static void free_rev_friends_node (rev_friends_t *T) {
  zfree (T, sizeof (rev_friends_t));
  alloc_rev_friends_nodes--;
}

/* static rev_friends_t *rev_friends_lookup (rev_friends_t *T, int x1, int x2) {
  while (T && x1 != T->x1 && x2 != T->x2) {
    T = (x1 < T->x1 || (x1 == T->x1 && x2 < T->x2)) ? T->left : T->right;
  }
  return T;
} */

static void rev_friends_split (rev_friends_t **L, rev_friends_t **R, rev_friends_t *T, int x1, int x2) {
  if (!T) { *L = *R = 0; return; }
  if (x1 < T->x1 || (x1 == T->x1 && x2 < T->x2)) {
    *R = T;
    rev_friends_split (L, &T->left, T->left, x1, x2);
  } else {
    *L = T;
    rev_friends_split (&T->right, R, T->right, x1, x2);
  }
}

static rev_friends_t *rev_friends_insert (rev_friends_t *T, int x1, int x2, int y, int date) {
  rev_friends_t *P;
  if (!T) { 
    P = new_rev_friends_node (x1, x2, y);
    P->date = date;
    return P;
  }
  assert (x1 != T->x1 || x2 != T->x2);
  if (T->y >= y) {
    if (x1 < T->x1 || (x1 == T->x1 && x2 < T->x2)) {
      T->left = rev_friends_insert (T->left, x1, x2, y, date);
    } else {
      T->right = rev_friends_insert (T->right, x1, x2, y, date);
    }
    return T;
  }
  P = new_rev_friends_node (x1, x2, y);
  P->date = date;
  rev_friends_split (&P->left, &P->right, T, x1, x2);
  return P;
}

static rev_friends_t *rev_friends_merge (rev_friends_t *L, rev_friends_t *R) {
  if (!L) { return R; }
  if (!R) { return L; }
  if (L->y > R->y) {
    L->right = rev_friends_merge (L->right, R);
    return L;
  } else {
    R->left = rev_friends_merge (L, R->left);
    return R;
  }
}

static rev_friends_t *rev_friends_delete (rev_friends_t *T, int x1, int x2) {
  assert (T);
  if (T->x1 == x1 && T->x2 == x2) {
    rev_friends_t *N = rev_friends_merge (T->left, T->right);
    free_rev_friends_node (T);
    return N;
  }
  if (x1 < T->x1 || (x1 == T->x1 && x2 < T->x2)) {
    T->left = rev_friends_delete (T->left, x1, x2);
  } else {
    T->right = rev_friends_delete (T->right, x1, x2);
  }
  return T;
}

static void rev_friends_find (rev_friends_t *T, int x1) {
  if (!T) {
    return;
  }
  if (T->x1 >= x1) {
    rev_friends_find (T->left, x1);
  }
  if (T->x1 == x1 && rev_friends_intersect_len < MAX_FRIENDS) {
    rev_friends_intersect_list[rev_friends_intersect_len++] = T->x2;
  }
  if (T->x1 <= x1) {
    rev_friends_find (T->right, x1);
  }
}

static int rev_friends_intersect (rev_friends_t *T, int x1) {
  if (!T) {
    return 0;
  }
  int result = 0;
  if (T->x1 >= x1) {
    result += rev_friends_intersect (T->left, x1);
  }
  if (T->x1 == x1) {
    while (rev_friends_intersect_pos < rev_friends_intersect_len && rev_friends_intersect_list[rev_friends_intersect_pos] < T->x2) {
      rev_friends_intersect_pos ++;
    }
    if (rev_friends_intersect_pos < rev_friends_intersect_len && rev_friends_intersect_list[rev_friends_intersect_pos] == T->x2) {
      result ++;
      rev_friends_intersect_pos ++;
    }
  }
  if (T->x1 <= x1) {
    result += rev_friends_intersect (T->right, x1);
  }
  return result;
}

static int rev_friends_intersect_constructive (rev_friends_t *T, int x1, int *res, int max_res) {
  if (!T) {
    return 0;
  }
  int result = 0;
  if (T->x1 >= x1) {
    result += rev_friends_intersect_constructive (T->left, x1, res, max_res);
  }
  if (T->x1 == x1) {
    while (rev_friends_intersect_pos < rev_friends_intersect_len && rev_friends_intersect_list[rev_friends_intersect_pos] < T->x2) {
      rev_friends_intersect_pos ++;
    }
    if (rev_friends_intersect_pos < rev_friends_intersect_len && rev_friends_intersect_list[rev_friends_intersect_pos] == T->x2 && result < max_res) {
      res[result ++] = unconv_uid (rev_friends_intersect_list[rev_friends_intersect_pos ++]);
    }
  }
  if (T->x1 <= x1) {
    result += rev_friends_intersect_constructive (T->right, x1, res + result, max_res - result);
  }
  return result;
}

/* static void free_rev_friends (rev_friends_t *T) {
  if (T) {
    free_rev_friends (T->left);
    free_rev_friends (T->right);
    free_rev_friends_node (T);
  }
} */

static void dump_rev_friends (rev_friends_t *T) {
  if (T) {
    writeout_char (4);
  } else {
    writeout_char (3);
    return;
  }
  writeout (&T->x1, 16);
  dump_rev_friends (T->left);
  dump_rev_friends (T->right);
}

static rev_friends_t *read_rev_friends (void) {
  char *ptr = readin (1);
  if (!ptr) {
    fprintf (stderr, "Unexpected end of file in read_tree\n");
    return 0;
  }
  char c = ptr[0];
  readadv (1);
  assert (c == 4 || c == 3);
  if (c == 3) {
    return 0;
  }
  ptr = readin (16);
  if (!ptr) {
    fprintf (stderr, "Unexpected end of file in read_tree\n");
    return 0;
  }
  rev_friends_t *T = new_rev_friends_node (0, 0, 0);
  memcpy (&T->x1, ptr, 16);
  readadv (16);
  T->left = read_rev_friends ();
  T->right = read_rev_friends ();
  return T;
}

static rev_friends_t *rev_friends_delete_tree (rev_friends_t *T, int x2, tree_t *A) {
  if (!A) {
    return T;
  }
  T = rev_friends_delete (T, A->x, x2);
  T = rev_friends_delete_tree (T, x2, A->left);
  T = rev_friends_delete_tree (T, x2, A->right);
  return T;
}

/* --------- privacy trees ------------ */

int privacy_nodes, tot_privacy_len;

static int compute_privacy_len (privacy_t *T) {
  int t = T->y & 255;
  return t == 255 ? T->List[0] : t;
}

static int compute_privacy_size (privacy_t *T) {
  int t = T->y & 255;
  return sizeof(privacy_t) + 4*(t == 255 ? T->List[0]+1 : t) + 4;
}

static void free_privacy_node (privacy_t *T) {
  tot_privacy_len -= compute_privacy_len (T);
  privacy_nodes--;
  zfree (T, compute_privacy_size(T));
}

static privacy_t *privacy_lookup (privacy_t *T, privacy_key_t x) {
  while (T && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

static void privacy_split (privacy_t **L, privacy_t **R, privacy_t *T, privacy_key_t x) {
  if (!T) { *L = *R = 0; return; }
  if (x < T->x) {
    *R = T;
    privacy_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    privacy_split (&T->right, R, T->right, x);
  }
}

/*static privacy_t *privacy_insert (privacy_t *T, privacy_t *P) {
  if (!T) {
    P->left = P->right = 0;
    return P;
  }
  if (T->y >= P->y) {
    if (P->x < T->x) {
      T->left = privacy_insert (T->left, P);
    } else {
      T->right = privacy_insert (T->right, P);
    }
    return T;
  }
  privacy_split (&P->left, &P->right, T, P->x);
  return P;
}*/

static privacy_t *privacy_merge (privacy_t *L, privacy_t *R) {
  if (!L) { return R; }
  if (!R) { return L; }
  if (L->y > R->y) {
    L->right = privacy_merge (L->right, R);
    return L;
  } else {
    R->left = privacy_merge (L, R->left);
    return R;
  }
}

static privacy_t *privacy_delete (privacy_t *T, privacy_key_t x) {
  if (T->x == x) {
    privacy_t *N = privacy_merge (T->left, T->right);
    free_privacy_node (T);
    return N;
  }
  if (x < T->x) {
    T->left = privacy_delete (T->left, x);
  } else {
    T->right = privacy_delete (T->right, x);
  }
  return T;
}

static privacy_t *privacy_replace (privacy_t *T, privacy_t *N, int insertion_found) {
  int insertion_current = 1;
  if (T) {
    if (N->x == T->x) {
      N->y = (N->y & 255) | (T->y & -256);
      N->left = T->left;
      N->right = T->right;
      free_privacy_node (T);
      return N;
    }
    insertion_current = N->y > (T->y | 255);
    if (N->x < T->x) {
      privacy_t *R = privacy_replace (T->left, N, insertion_current);
      if (R) {
        T->left = R;
        return T;
      }
    } else {
      privacy_t *R = privacy_replace (T->right, N, insertion_current);
      if (R) {
        T->right = R;
        return T;
      }
    }
  }
  if (!insertion_found && insertion_current) {
    privacy_split (&N->left, &N->right, T, N->x);
    return N;
  } else {
    return 0;
  }
}

static void free_privacy_tree (privacy_t *T) {
  if (T) {
    free_privacy_tree (T->left);
    free_privacy_tree (T->right);
    free_privacy_node (T);
  }
}

static void dump_privacy_tree (privacy_t *T) {
  if (T) {
    writeout_char (3);
  } else {
    writeout_char (2);
    return;
  }
  writeout (&T->x, compute_privacy_size (T) - 2 * sizeof (privacy_t *));
  dump_privacy_tree (T->left);
  dump_privacy_tree (T->right);
}


static privacy_t *read_privacy (void) {
  char *ptr = readin (1);
  if (!ptr) {
    fprintf (stderr, "Unexpected end of file in read_privacy\n");
    return 0;
  }
  char c = ptr[0];
  readadv (1);
  assert (c == 2 || c == 3);
  if (c == 2) {
    return 0;
  }
  ptr = readin (16);
  if (!ptr) {
    fprintf (stderr, "Unexpected end of file in read_privacy\n");
    return 0;
  }
  int l = compute_privacy_size ((privacy_t *)(ptr - 2 * sizeof (privacy_t *)));
  privacy_t *T = zmalloc (l);
  ptr = readin (l - 2 * sizeof (privacy_t *));
  if (!ptr) {
    fprintf (stderr, "Unexpected end of file in read_privacy\n");
  }
  memcpy (&T->x, ptr, l - 2 * sizeof (privacy_t *));
  readadv (l - 2 * sizeof (privacy_t *));
  T->left = read_privacy ();
  T->right = read_privacy ();
  assert (l == compute_privacy_size (T));
  tot_privacy_len += l;
  privacy_nodes++;
  return T;
}

/* --------- friends data ------------- */

int ignored_delete_user_id;

int tot_users, max_uid;
user_t *User[MAX_USERS];

extern int reverse_friends_mode;
rev_friends_t *rev_friends;

int friends_replay_logevent (struct lev_generic *E, int size);

int init_friends_data (int schema) {

  replay_logevent = friends_replay_logevent;

  memset (User, 0, sizeof(User));
  max_uid = tot_users = 0;

  return 0;
}

static int friends_le_start (struct lev_start *E) {
  if (E->schema_id != FRIENDS_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

/* ---- atomic data modification/replay binlog ----- */

int conv_uid (int user_id) {
  if (user_id <= 0) { return -1; }
  if (user_id % log_split_mod != log_split_min) { return -1; }
  user_id /= log_split_mod;
  return user_id < MAX_USERS ? user_id : -1;
}

static int unconv_uid (int user_id) {
	if (user_id >= 0) { return user_id * log_split_mod + log_split_min; }
	return -1;
}

static user_t *get_user (int user_id) {
  int i = conv_uid (user_id);
  return i >= 0 ? User[i] : 0;
}

static void free_user_struct (user_t *U) {
  zfree (U, sizeof (user_t));
}

static user_t *get_user_f (int user_id) {
  int i = conv_uid (user_id);
  user_t *U;
  if (i < 0) { return 0; }
  U = User[i];
  if (U) { return U; }
  U = zmalloc (sizeof (user_t));
  memset (U, 0, sizeof(user_t));
  U->user_id = i;
  U->cat_mask = 1;
  User[i] = U;
  if (i > max_uid) { max_uid = i; }
  tot_users++;

  for (i = 0; i < 31; i++) {
    U->cat_ver[i] = i;
  }
 
  return U;
}

static int free_friend_list (user_t *U) {
  if (!U) { return 0; }
  if (reverse_friends_mode) {
    rev_friends = rev_friends_delete_tree (rev_friends, U->user_id, U->fr_tree);
  }
  free_tree (U->fr_tree);
  U->fr_tree = 0;
  U->fr_cnt = 0;
  return 1;
}

int set_friend_list (user_t *U, int len, const id_cat_pair_t *A, const int *B) {
  int i, m = 1;
  tree_t *T = 0;
  if (!U) { return -1; }
  if (verbosity >= 4) {
    fprintf (stderr, "set_friend_list. user_id = %d\n", U->user_id);
  }
  free_friend_list (U);
  if (A) {
    for (i = 1; i <= len; i++) {
      int x = A->id;
      tree_t *P = tree_lookup (T, x);
      m |= A->cat;
      if (P) {
        P->cat = A->cat | 1;
        P->date = i;
      } else {
        T = tree_insert (T, x, lrand48(), A->cat | 1, i);
        U->fr_cnt++;
        if (reverse_friends_mode) {
          rev_friends = rev_friends_insert (rev_friends, x, U->user_id, lrand48(), 0);
        }
      }
      A++;
    }
  } else {
    for (i = 1; i <= len; i++) {
      int x = *B++;
      tree_t *P = tree_lookup (T, x);
      if (P) {
        P->date = i;
        P->cat = 1;
      } else {
        T = tree_insert (T, x, lrand48(), 1, i);
        U->fr_cnt++;
        if (reverse_friends_mode) {
          rev_friends = rev_friends_insert (rev_friends, x, U->user_id, lrand48(), 0);
        }
      }
    }
  }

  U->fr_tree = T;
  U->fr_last_date = len;
  U->cat_mask = m;

  return U->fr_cnt;
}

static int delete_user (int user_id) {
  int i = conv_uid (user_id);
  user_t *U;
  if (i < 0 || user_id == ignored_delete_user_id) {
    return -1;
  }
  if (!User[i]) {
    return 0;
  }
  U = User[i];
  User[i] = 0;

  //free_tree (U->fr_tree);
  free_friend_list (U);
  free_tree (U->req_tree);
  free_tree (U->req_time_tree);
  free_privacy_tree (U->pr_tree);

  U->fr_tree = 0;
  U->req_tree = 0;
  U->req_time_tree = 0;
  U->pr_tree = 0;
  U->fr_cnt = 0;
  U->req_cnt = 0;
  U->user_id = -1;

  free_user_struct (U);
  tot_users--;

  return 1;
}

static int add_friend_request (user_t *U, int friend_id, int cat, int force) {
  tree_t *P;
  if (!U || friend_id <= 0) {
    return -1;
  }
  P = tree_lookup (U->req_tree, friend_id);
  if (P) {
    if (force == 2) {
      return 0;
    }
    P->cat = cat;
    P = tree_lookup (U->req_time_tree, -P->date);
    assert (P);
    P->cat = cat;
    return P->cat;
  } else if (force) {
    if (tree_lookup (U->fr_tree, friend_id)) {
      return -1;
    }
    if (now > U->req_last_date) {
      U->req_last_date = now;
    } else {
      U->req_last_date++;
    }
    U->req_tree = tree_insert (U->req_tree, friend_id, lrand48(), cat, U->req_last_date);
    U->req_time_tree = tree_insert (U->req_time_tree, -U->req_last_date, lrand48(), cat, friend_id);
    U->req_cnt++;
    return cat;
  } else {
    return 0;
  }
}

static int delete_friend_request (user_t *U, int friend_id) {
  tree_t *P;
  if (!U || friend_id <= 0) {
    return -1;
  }
  P = tree_lookup (U->req_tree, friend_id);
  if (P) {
    int date = P->date;
    U->req_tree = tree_delete (U->req_tree, friend_id);
    U->req_time_tree = tree_delete (U->req_time_tree, -date);
    U->req_cnt--;
    assert (U->req_cnt >= 0);
    return 1;
  }
  return 0;
}

static int delete_all_friend_requests (user_t *U) {
  if (!U) {
    return -1;
  }
  free_tree (U->req_tree);
  free_tree (U->req_time_tree);
  U->req_tree = 0;
  U->req_time_tree = 0;
  U->req_cnt = 0;
  return 1;
}

static int change_friend_cat (user_t *U, int friend_id, int cat_xor, int cat_and) {
  tree_t *P;
  if (!U || friend_id <= 0) {
    return -1;
  }
  if (verbosity >= 4) {
    fprintf (stderr, "add_friend. user_id = %d, friend_id = %d\n", U->user_id, friend_id);
  }
  P = tree_lookup (U->fr_tree, friend_id);
  if (P) {
    P->cat = ((P->cat & cat_and) ^ cat_xor) | 1;
    U->cat_mask |= P->cat;
    return P->cat;
  } else {
    return 0;
  }
}

static int add_friend (user_t *U, int friend_id, int cat) {
  tree_t *P;
  if (!U || friend_id <= 0) {
    return -1;
  }
  if (verbosity >= 4) {
    fprintf (stderr, "add_friend. user_id = %d, friend_id = %d\n", U->user_id, friend_id);
  }
  P = tree_lookup (U->fr_tree, friend_id);
  if (P) {
    P->cat = cat | 1;
    U->cat_mask |= P->cat;
    return P->cat;
  } else {

    if (U->fr_cnt >= MAX_FRIENDS) {
      return -1;
    }

    delete_friend_request (U, friend_id);

    if (now > U->fr_last_date) {
      U->fr_last_date = now;
    } else {
      U->fr_last_date++;
    }

    U->fr_tree = tree_insert (U->fr_tree, friend_id, lrand48(), cat | 1, U->fr_last_date);
    U->fr_cnt++;

    if (reverse_friends_mode) {
      rev_friends = rev_friends_insert (rev_friends, friend_id, U->user_id, lrand48(), 0);
    }

    return cat | 1;
  }
}

static int delete_friend (user_t *U, int friend_id) {
  tree_t *P;
  if (!U || friend_id <= 0) {
    return -1;
  }
  if (verbosity >= 4) {
    fprintf (stderr, "delete_friend. user_id = %d, friend_id = %d\n", U->user_id, friend_id);
  }
  P = tree_lookup (U->fr_tree, friend_id);
  if (P) {
    U->fr_tree = tree_delete (U->fr_tree, friend_id);
    U->fr_cnt--;
    assert (U->fr_cnt >= 0);
    if (reverse_friends_mode) {
      rev_friends = rev_friends_delete (rev_friends, friend_id, U->user_id);
    }
    return 1;
  }
  return 0;
}

static int change_friend_logevent (struct lev_generic *E) {
  int s;
  switch (E->type) {
  case LEV_FR_ADD_FRIEND:
    return add_friend (get_user_f (E->a), E->b, E->c);
  case LEV_FR_DEL_FRIEND:
    return delete_friend (get_user (E->a), E->b);
  case LEV_FR_EDIT_FRIEND:
    return change_friend_cat (get_user (E->a), E->b, E->c, 0);
  case LEV_FR_EDIT_FRIEND_OR:
    return change_friend_cat (get_user (E->a), E->b, E->c, ~E->c);
  case LEV_FR_EDIT_FRIEND_AND:
    return change_friend_cat (get_user (E->a), E->b, 0, E->c);
  case LEV_FR_ADDTO_CAT+1 ... LEV_FR_ADDTO_CAT+0x1f:
    s = (1 << (E->type & 0x1f));
    return change_friend_cat (get_user (E->a), E->b, s, ~s);
  case LEV_FR_REMFROM_CAT+1 ... LEV_FR_REMFROM_CAT+0x1f:
    s = (1 << (E->type & 0x1f));
    return change_friend_cat (get_user (E->a), E->b, 0, ~s);
  default:
    assert (0 && "unknown change friend log event");
  }
  return -1;
}

static void tree_clean_cat (tree_t *T, int mask) {
  if (T) {
    T->cat &= mask;
    tree_clean_cat (T->left, mask);
    tree_clean_cat (T->right, mask);
  }
}

static int delete_friend_category (user_t *U, int cat, int op) {
  if (!U || cat <= 0 || cat >= 32) {
    return -1;
  }
  tree_clean_cat (U->fr_tree, ~(1 << cat));
  if (op) {
    U->cat_ver[cat] += 0x100;
    U->cat_mask &= ~(1 << cat);
  }
  return 1;
}

static int *LiA, *LiB;

static void tree_set_catlist (tree_t *T, int mask) {
  if (!T) {
    return;
  }
  tree_set_catlist (T->left, mask);
  while (LiA < LiB && *LiA < T->x) {
    LiA++;
  }
  if (LiA < LiB && T->x == *LiA) {
    T->cat |= mask;
    LiA++;
  } else {
    T->cat &= ~mask;
  }
  tree_set_catlist (T->right, mask);
}
    

static int set_category_friend_list (user_t *U, int cat, int *List, int len) {
  int i;
  if (!U || cat <= 0 || cat >= 32 || (unsigned) len > 16384) {
    return -1;
  }
  assert (!len || List[0] > 0);
  for (i = 1; i < len; i++) {
    assert (List[i-1] < List[i]);
  }
  LiA = List;
  LiB = List + len;
  tree_set_catlist (U->fr_tree, 1 << cat);
  U->cat_mask |= (1 << cat);
  return 1;
}

static int delete_privacy (user_t *U, privacy_key_t key) {
  if (!U || !key) {
    return -1;
  }
  if (privacy_lookup (U->pr_tree, key)) {
    U->pr_tree = privacy_delete (U->pr_tree, key);
    return 1;
  } else {
    return 0;
  }
}


/* --------- some privacy list functions ---------- */

#define MAX_PRIVACY_LEN	2048

int P[MAX_PRIVACY_LEN], PL;

static int parse_privacy (const char *text, int len) {
  const char *end = text + len;
  int x, y;
  PL = 0;
  while (text < end) {
    switch (*text) {
    case '+':
      x = PL_M_USER | PL_M_ALLOW;
      break;
    case '*':
      x = PL_M_CAT | PL_M_ALLOW;
      break;
    case '-':
      x = PL_M_USER | PL_M_DENY;
      break;
    case '/':
      x = PL_M_CAT | PL_M_DENY;
      break;
    default:
      return -1;
    }
    if (++text >= end) {
      return -1;
    }
    if (*text > '9') {
      if (x & PL_M_CAT) {
        switch (*text) {
        case 'A':
          y = CAT_FR_ALL;
          break;
        case 'G':
          y = CAT_FR_FR;
          break;
        default:
          return -1;
        }
        text++;
      } else {
        return -1;
      }
    } else {
      if (*text < '0') {
        return -1;
      }
      y = 0;
      while (text < end && *text >= '0' && *text <= '9') {
        if (y > PL_M_MASK / 10) {
          return -1;
        }
        y = y * 10 + (*text++ - '0');
      }
      if (y > PL_M_MASK || ((x & PL_M_CAT) && y > 30)) {
        return -1;
      }
    }
    P[PL++] = x | y;  
    if (PL >= MAX_PRIVACY_LEN) {
      return -1;
    }
  }
  P[PL] = -1;
  return PL;
}

static int remove_unused_uids (int owner_id) {
  int *A = P, *B = P, x;
  while (*A != -1) {
    x = *A++;
    if (!(x & PL_M_CAT)) {
      x &= PL_M_MASK;
      if (!x || x == owner_id) {
        continue;
      }
      int *C;
      for (C = P; C < B; C++) {
        if ((*C & PL_M_MASK) == x) {
          break;
        }
      }
      if (C < B) {
        continue;
      }
      x = A[-1];
    }
    *B++ = x;
  }
  *B = -1;
  PL = B - P;
  return PL;
}

static int remove_unused_cats (void) {
  int *A = P, *B = P, x, m = 0, m1 = 0;
  while (*A != -1 && m1 != CAT_FR_ALL) {
    x = *A++;
    if (x & PL_M_CAT) {
      x &= PL_M_MASK;
      if (x == CAT_FR_ALL || x == CAT_FR_FR) {
        if (m1 >= x) {
          continue;
        }
        m1 |= x;
        m = -1;
      } else {
        x = (1 << (x & 0x1f));
        if (m & x) {
          continue;
        }
        if (x == 1) {
          x = -1;
        }
        m |= x;
      }
      x = A[-1];
    }
    *B++ = x;
  }
  *B = -1;
  PL = B - A;
  return PL;
}

static void privacy_resort (int a, int b, int mode) {
  int i, j, h, t;
  if (a >= b) { return; }
  h = P[(a+b)>>1] ^ mode;
  i = a;
  j = b;
  do {
    while ((P[i] ^ mode) < h) { i++; }
    while ((P[j] ^ mode) > h) { j--; }
    if (i <= j) {
      t = P[i];  P[i++] = P[j];  P[j--] = t;
    }
  } while (i <= j);
  privacy_resort (a, j, mode);
  privacy_resort (i, b, mode);
}

static int resort_series (void) {
  int a = 0, b;

  while (P[a] != -1) {
    b = a;
    while (P[a] > 0) { a++; }
    privacy_resort (b, a-1, 0x60000000);
    b = a;
    while (P[a] < -1) { a++; }
    privacy_resort (b, a-1, 0x60000000);
  }

  while (1) {
    while (a > 0 && P[a-1] < 0) { a--; }
    P[a] = -1;
    while (a > 0 && !(P[a-1] & PL_M_CAT)) { a--; }
    b = a;
    while (P[a] != -1) {
      if (P[a] > 0) {
	P[b++] = P[a];
      }
      a++;
    }
    P[b] = -1;
    a = b;
    if (!a) {
      break;
    }
    if (P[a-1] >= 0) {
      b = a - 1;
      while (b > 0 && P[b-1] >= 0) { b--; }
      privacy_resort (b, a-1, 0x60000000);
      break;
    }
  }

  PL = a;
  return a;
}

static void save_privacy_cats (user_t *U) {
  int *A;
  for (A = P; *A != -1; A++) {
    int x = *A;
    if ((x & 0x60000000) == 0x40000000) {
      assert (!(x & 0x3fffffe0));
      *A = (x & -0x40000000) + U->cat_ver[x & 0x1f];
    }
  }
}

static int improve_privacy (int owner_id) {
  remove_unused_uids (owner_id);
  remove_unused_cats ();
  resort_series ();
  remove_unused_cats ();
  resort_series ();
  return PL;
}

int parse_prepare_privacy (const char *text, int len, int owner_id) {
  int L = parse_privacy (text, len);
  if (L < 0) {
    return L;
  }
  return improve_privacy (owner_id);
}

privacy_t *alloc_privacy (void) {
  int len = PL, *A, sz;
  assert (P[PL] == -1);
  if (len >= 255) {
    len++;
  }

  sz = sizeof(privacy_t) + len*4 + 4;
  tot_privacy_len += PL;
  privacy_nodes++;

  privacy_t *T = zmalloc (sz);
  memset (T, 0, sizeof(privacy_t));
  T->y = (lrand48() << 8) + (len >= 255 ? 255 : len);
  A = T->List;
  if (len >= 255) {
    *A++ = PL;
  }

  memcpy (A, P, (PL+1)*4);

  if (verbosity > 2) {
    fprintf (stderr, "in-core privacy: ");
    for (sz = 0; sz <= PL; sz++) {
      fprintf (stderr, " %08x", A[sz]);
    }
    fprintf (stderr, "\n");
  }

  return T;
}

/* pack/unpack privacy */

int Q[MAX_PRIVACY_LEN], QL;

int pack_privacy (void) {
  int *A = P, *B = Q;
  while (*A != -1) {
    int x = *A++;
    if ((x & PL_M_CAT) && (x & PL_M_MASK) < CAT_FR_FR) {
      if (!(x & 0x1f)) {
        *B++ = (x & ~PL_M_MASK) | CAT_FR_PACKED;
        continue;
      }
      int m = 0, s = x;
      while (1) {
        m |= (1 << ((x & 0x1f) - 1));
        x = *A;
        if (x == -1 || ((s ^ x) < 0) || !(x & PL_M_CAT) || (x & PL_M_MASK) >= CAT_FR_FR || !(x & 0x1f)) {
          break;
        }
        A++;
      }
      x = s & ~PL_M_MASK;
      m &= PL_M_MASK;
      if (m >= CAT_FR_PACKED) {
        *B++ = x + (m & 0xffff);
        m &= -0x10000;
      }
      x |= m;
    }
    *B++ = x;
  }
  QL = B - Q;

  if (verbosity > 2) {
    int i;
    fprintf (stderr, "packed privacy: ");
    for (i = 0; i < QL; i++) {
      fprintf (stderr, " %08x", Q[i]);
    }
    fprintf (stderr, "\n");
  }

  return QL;
}

int unpack_privacy (const int *PP, int L) {
  const int *PE = PP + L;
  int *A = P;
  assert (L <= MAX_PRIVACY_LEN);
  while (PP < PE) {
    int x = *PP++, s;
    assert (A < P + MAX_PRIVACY_LEN - 32);
    if (!(x & PL_M_CAT)) {
      *A++ = x;
      continue;
    }
    s = x & ~PL_M_MASK;
    switch (x & PL_M_MASK) {
    case CAT_FR_ALL:
    case CAT_FR_FR:
      *A++ = x;
      continue;
    case CAT_FR_PACKED:
      *A++ = s;
      continue;
    }
    int i;
    for (i = 0; i < 30; i++) {
      if (x & 1) {
        *A++ = s + i + 1;
      }
      x >>= 1;
    }
  }
  *A = -1;
  PL = A - P;
  return PL;
}

/* set/replace privacy */

static int set_privacy (user_t *U, privacy_key_t privacy_key, int force) {
  if (!U || !privacy_key) {
    return -1;
  }
  if (!force && !privacy_lookup (U->pr_tree, privacy_key)) {
    return -1;
  }

  save_privacy_cats (U);

  privacy_t *P = alloc_privacy();
  P->x = privacy_key;

  U->pr_tree = privacy_replace (U->pr_tree, P, 0);
  return 1;
}

/* --------- replay log ------- */

int friends_replay_logevent (struct lev_generic *E, int size) {
  int s;

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return friends_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_FR_SETLIST_CAT_LONG:
    if (size < 12) { return -2; }
    s = ((struct lev_setlist_cat_long *) E)->num;
    if (s & -0x10000) { return -4; }
    if (size < 12 + 8*s) { return -2; }
    if (verbosity >= 4) {
      fprintf (stderr, "set_friend_list.\n");
    }
    set_friend_list (get_user_f (E->a), s, ((struct lev_setlist_cat_long *) E)->L, 0);
    return 12 + 8*s;
  case LEV_FR_SETLIST_CAT ... LEV_FR_SETLIST_CAT+0xff:
    s = E->type & 0xff;
    if (size < 8 + 8*s) { return -2; }
    if (verbosity >= 4) {
      fprintf (stderr, "set_friend_list.\n");
    }
    set_friend_list (get_user_f (E->a), s, ((struct lev_setlist_cat *) E)->L, 0);
    return 8 + 8*s;
  case LEV_FR_SETLIST_LONG:
    if (size < 12) { return -2; }
    s = ((struct lev_setlist_long *) E)->num;
    if (s & -0x10000) { return -4; }
    if (size < 12 + 4*s) { return -2; }
    if (verbosity >= 4) {
      fprintf (stderr, "set_friend_list.\n");
    }
    set_friend_list (get_user_f (E->a), s, 0, ((struct lev_setlist_long *) E)->L);
    return 12 + 4*s;
  case LEV_FR_SETLIST ... LEV_FR_SETLIST+0xff:
    s = E->type & 0xff;
    if (size < 8 + 4*s) { return -2; }
    if (verbosity >= 4) {
      fprintf (stderr, "set_friend_list.\n");
    }
    set_friend_list (get_user_f (E->a), s, 0, ((struct lev_setlist *) E)->L);
    return 8 + 4*s;
  case LEV_FR_USERDEL:
    if (size < 8) { return -2; }
    if (verbosity >= 4) {
      fprintf (stderr, "delete_user.\n");
    }
    delete_user (E->a);
    return 8;
  case LEV_FR_ADD_FRIENDREQ:
    if (size < 16) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "add_friend_request.\n");
      }
      add_friend_request (get_user_f (E->a), E->b, E->c, 1);
    }
    return 16;
  case LEV_FR_REPLACE_FRIENDREQ:
    if (size < 16) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "add_friend_request.\n");
      }
      add_friend_request (get_user (E->a), E->b, E->c, 0);
    }
    return 16;
  case LEV_FR_NEW_FRIENDREQ:
    if (size < 16) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "add_friend_request.\n");
      }
      add_friend_request (get_user_f (E->a), E->b, E->c, 2);
    }
    return 16;
  case LEV_FR_DEL_FRIENDREQ:
    if (size < 12) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "delete_friend_request.\n");
      }
      delete_friend_request (get_user (E->a), E->b);
    }
    return 12;
  case LEV_FR_ADD_FRIEND:
  case LEV_FR_EDIT_FRIEND:
  case LEV_FR_EDIT_FRIEND_OR:
  case LEV_FR_EDIT_FRIEND_AND:
    if (size < 16) { return -2; }
    change_friend_logevent (E);
    return 16;
  case LEV_FR_DEL_FRIEND:
  case LEV_FR_ADDTO_CAT+1 ... LEV_FR_ADDTO_CAT+0x1f:
  case LEV_FR_REMFROM_CAT+1 ... LEV_FR_REMFROM_CAT+0x1f:
    if (size < 12) { return -2; }
    change_friend_logevent (E);
    return 12;
  case LEV_FR_DEL_CAT+1 ... LEV_FR_DEL_CAT+0x1f:
    if (size < 8) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "delete_friend_category.\n");
      }
      delete_friend_category (get_user (E->a), E->type & 0xff, 1);
    }
    return 8;
  case LEV_FR_CLEAR_CAT+1 ... LEV_FR_CLEAR_CAT+0x1f:
    if (size < 8) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "delete_friend_category.\n");
      }
      delete_friend_category (get_user (E->a), E->type & 0xff, 0);
    }
    return 8;
  case LEV_FR_CAT_SETLIST+1 ... LEV_FR_CAT_SETLIST+0x1f:
    if (size < 12) { return -2; }
    s = ((struct lev_setlist_long *) E)->num;
    if (s & -0x10000) { return -4; }
    if (size < 12 + 4*s) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "set_category_friend_list.\n");
      }
      set_category_friend_list (get_user_f (E->a), E->type & 0xff, ((struct lev_setlist_long *) E)->L, s);
    }
    return 12 + 4*s;
  case LEV_FR_SET_PRIVACY ... LEV_FR_SET_PRIVACY+0xff:
  case LEV_FR_SET_PRIVACY_FORCE ... LEV_FR_SET_PRIVACY_FORCE+0xff:
    s = (E->type & 0xff);
    if (size < 16 + 4*s) { return -2; }
    if (!reverse_friends_mode) {
      unpack_privacy (((struct lev_set_privacy *) E)->List, s);
      if (verbosity >= 4) {
        fprintf (stderr, "set_privacy.\n");
      }
      set_privacy (E->type & 0x100 ? get_user_f (E->a) : get_user (E->a), ((struct lev_set_privacy *) E)->key, E->type & 0x100);
    }
    return 16 + 4*s;
  case LEV_FR_SET_PRIVACY_LONG ... LEV_FR_SET_PRIVACY_LONG+1:
    if (size < 20) { return -2; }
    s = ((struct lev_set_privacy_long *) E)->len;
    if (size < 20 + 4*s) { return -2; }
    if (!reverse_friends_mode) {
      unpack_privacy (((struct lev_set_privacy_long *) E)->List, s);
      if (verbosity >= 4) {
        fprintf (stderr, "set_privacy.\n");
      }
      set_privacy (E->type & 1 ? get_user_f (E->a) : get_user (E->a), ((struct lev_set_privacy_long *) E)->key, E->type & 1);
    }
    return 20 + 4*s;
  case LEV_FR_DEL_PRIVACY:
    if (size < 16) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "delete_privacy.\n");
      }
      delete_privacy (get_user (E->a), ((struct lev_del_privacy *) E)->key);
    }
    return 16;
  case LEV_FR_DELALL_REQ:
    if (size < 8) { return -2; }
    if (!reverse_friends_mode) {
      if (verbosity >= 4) {
        fprintf (stderr, "delete_all_friend_requests.\n");
      }
      delete_all_friend_requests (get_user (E->a));
    }
    return 8;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;

}
/* adding new log entries */

int do_delete_user (int user_id) {
  if (conv_uid (user_id) < 0) {
    return -1;
  }
  alloc_log_event (LEV_FR_USERDEL, 8, user_id);
  return delete_user (user_id);
}

/* friend requests */

int do_add_friend_request (int user_id, int friend_id, int cat, int force) {
  user_t *U;
  if (conv_uid (user_id) < 0 || friend_id <= 0 || cat < 0 || (unsigned) force > 2) {
    return -1;
  }

  struct lev_add_friend *E = alloc_log_event (force ? (force > 1 ? LEV_FR_NEW_FRIENDREQ : LEV_FR_ADD_FRIENDREQ) : LEV_FR_REPLACE_FRIENDREQ, 16, user_id);
  E->friend_id = friend_id;
  E->cat = cat;

  U = (force ? get_user_f (user_id) : get_user (user_id));
  return add_friend_request (U, friend_id, cat, force);
}

int do_delete_friend_request (int user_id, int friend_id) {
  struct lev_del_friend *E;

  if (conv_uid (user_id) < 0 || friend_id <= 0) {
    return -1;
  }

  E = alloc_log_event (LEV_FR_DEL_FRIENDREQ, 12, user_id);
  E->friend_id = friend_id;

  return delete_friend_request (get_user (user_id), friend_id);
}

int do_delete_all_friend_requests (int user_id) {

  if (conv_uid (user_id) < 0) {
    return -1;
  }

  alloc_log_event (LEV_FR_DELALL_REQ, 8, user_id);

  return delete_all_friend_requests (get_user (user_id));
}

/* friends */

// it is known that x = 2^s, return s.
static int b_log2 (unsigned x) {
  x &= -x;
  return x ? __builtin_ctz (x) : -1;
}

int do_add_friend (int user_id, int friend_id, int cat_xor, int cat_and, int force) {
  if (conv_uid (user_id) < 0 || friend_id <= 0 || cat_xor < 0) {
    return -1;
  }
  if ((force && cat_and) || (cat_xor & cat_and)) {
    return -1;
  }
  if (force) {
    struct lev_add_friend *E = alloc_log_event (LEV_FR_ADD_FRIEND, 16, user_id);
    E->friend_id = friend_id;
    E->cat = cat_xor | 1;
    return change_friend_logevent ((struct lev_generic *) E);
  }

  if ((cat_and & 0x7ffffffe) == 0x7ffffffe && !cat_xor) {
    return get_friend_cat (user_id, friend_id);
  }

  if (!get_user (user_id)) {
    return -1;
  }

  int i, m, res = -1;
  m = ~(cat_and | cat_xor) & 0x7ffffffe;
  if (m) {
    if (!(m & (m - 1))) {
      i = b_log2 (m);
      struct lev_del_friend *E = alloc_log_event (LEV_FR_REMFROM_CAT + i, 12, user_id);
      E->friend_id = friend_id;
      res = change_friend_logevent ((struct lev_generic *) E);
    } else if (cat_and & 0x7ffffffe) {
      struct lev_add_friend *E = alloc_log_event (LEV_FR_EDIT_FRIEND_AND, 16, user_id);
      E->friend_id = friend_id;
      E->cat = cat_and | 1;
      res = change_friend_logevent ((struct lev_generic *) E);
    }
  }

  cat_xor &= 0x7ffffffe;
  if (!(cat_and & 0x7ffffffe)) {
    struct lev_add_friend *E = alloc_log_event (LEV_FR_EDIT_FRIEND, 16, user_id);
    E->friend_id = friend_id;
    E->cat = cat_xor | 1;
    res = change_friend_logevent ((struct lev_generic *) E);
  } else if (cat_xor) {
    if (!(cat_xor & (cat_xor - 1))) {
      i = b_log2 (cat_xor);
      struct lev_del_friend *E = alloc_log_event (LEV_FR_ADDTO_CAT + i, 12, user_id);
      E->friend_id = friend_id;
      res = change_friend_logevent ((struct lev_generic *) E);
    } else {
      struct lev_add_friend *E = alloc_log_event (LEV_FR_EDIT_FRIEND_OR, 16, user_id);
      E->friend_id = friend_id;
      E->cat = cat_xor;
      res = change_friend_logevent ((struct lev_generic *) E);
    }
  }

  assert (res != -1);

  return res;
}

int do_delete_friend (int user_id, int friend_id) {
  struct lev_del_friend *E;

  if (conv_uid (user_id) < 0 || friend_id <= 0) {
    return -1;
  }

  E = alloc_log_event (LEV_FR_DEL_FRIEND, 12, user_id);
  E->friend_id = friend_id;

  return change_friend_logevent ((struct lev_generic *) E);
}

int do_delete_friend_category (int user_id, int cat) {
  if (conv_uid (user_id) < 0 || cat <= 0 || cat > 30) {
    return -1;
  }
  alloc_log_event (LEV_FR_DEL_CAT+cat, 8, user_id);

  return delete_friend_category (get_user (user_id), cat, 1);
}


static void il_sort (int *A, int b) {
  if (b <= 0) { return; }
  int h = A[b >> 1], i = 0, j = b, t;
  do {
    while (A[i] < h) { i++; }
    while (A[j] > h) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  il_sort (A, j);
  il_sort (A+i, b-i);
}
  

int do_set_category_friend_list (int user_id, int cat, int *List, int len) {
  user_t *U = get_user (user_id);
  if (!U || cat <= 0 || cat > 30) {
    return -1;
  }
  int i = 0, j = 1;

  il_sort (List, len-1);
  while (i < len && List[i] <= 0) { i++; }
  List += i;
  len -= i;
  if (len > 0) {
    for (i = 1; i < len; i++) {
      if (List[i] > List[i-1]) {
        List[j++] = List[i];
      }
    }
    len = j;
  }

  struct lev_setlist_long *E = alloc_log_event (LEV_FR_CAT_SETLIST + cat, 12 + 4 * len, user_id);
  E->num = len;
  memcpy (E->L, List, len*4);

  return set_category_friend_list (U, cat, E->L, len);
}

int do_delete_privacy (int user_id, privacy_key_t privacy_key) {
  if (conv_uid (user_id) < 0 || !privacy_key) {
    return -1;
  }

  struct lev_del_privacy *E = alloc_log_event (LEV_FR_DEL_PRIVACY, 16, user_id);
  E->key = privacy_key;

  return delete_privacy (get_user (user_id), privacy_key);
}

int do_set_privacy (int user_id, privacy_key_t privacy_key, const char *text, int len, int force) {
  user_t *U;

  if (conv_uid (user_id) < 0 || !privacy_key) {
    return -1;
  }

  if (parse_prepare_privacy (text, len, user_id) < 0) {
    return -1;
  }

  pack_privacy ();

  if (QL < 256) {
    struct lev_set_privacy *E = alloc_log_event (LEV_FR_SET_PRIVACY + QL + (force ? 0x100 : 0), 16+QL*4, user_id);
    E->key = privacy_key;
    memcpy (E->List, Q, QL*4);
  } else {
    struct lev_set_privacy_long *E = alloc_log_event (LEV_FR_SET_PRIVACY_LONG + (force ? 1 : 0), 20+QL*4, user_id);
    E->key = privacy_key;
    E->len = QL;
    memcpy (E->List, Q, QL*4);
  }

  if (!force) {
    U = get_user (user_id);
  } else {
    U = get_user_f (user_id);
  }

  return set_privacy (U, privacy_key, force);
}


/* access data */

#define	AUX_HEAP_SIZE	16384

int R[MAX_RES], *R_end;
static int R_cat_mask, R_mode, R_max;
static tree_t *H[AUX_HEAP_SIZE];
static int HN;

static void tree_fetch (tree_t *T) {
  if (!T || R_end > R + MAX_RES - 3) {
    return;
  }
  tree_fetch (T->left);
  if (T->cat & R_cat_mask || R_cat_mask == -1) {
    int m = R_mode;
    *R_end++ = T->x;
    if (m & 3) {
      *R_end++ = T->cat;
    }
    if (m & 2) {
      *R_end++ = T->date;
    }
  }
  tree_fetch (T->right);
}

static void tree_fetch_max (tree_t *T) {
  if (!T || R_end > R + MAX_RES - 3 || R_end == R + (R_mode + 1) * R_max) {
    return;
  }
  tree_fetch_max (T->left);
  if (R_end == R + (R_mode + 1) * R_max) {
    return;
  }
  if (T->cat & R_cat_mask || R_cat_mask == -1) {
    int m = R_mode;
    *R_end++ = T->x;
    if (m & 3) {
      *R_end++ = T->cat;
    }
    if (m & 2) {
      *R_end++ = T->date;
    }
  }
  tree_fetch_max (T->right);
}

int prepare_friends (int user_id, int cat_mask, int mode) {
  user_t *U = get_user (user_id);
  if ((!U && conv_uid (user_id) < 0) || mode < 0 || mode > 2) {
    return -1;
  }
  R_end = R;
  if (!U) {
    return 0;
  }
  R_cat_mask = cat_mask;
  R_mode = mode;
  tree_fetch (U->fr_tree);
  return (R_end - R) / (mode + 1);
}

static void aux_heap_insert (tree_t *I) {
  int r = I->date, i, j;
  if (HN == R_max) {
    if (!HN || H[1]->date >= r) { return; }
    i = 1;
    while (1) {
      j = i*2;
      if (j > HN) { break; }
      if (j < HN && H[j+1]->date < H[j]->date) { j++; }
      if (H[j]->date >= r) { break; }
      H[i] = H[j];
      i = j;
    }
    H[i] = I;
  } else {
    i = ++HN;
    assert (HN < AUX_HEAP_SIZE);
    while (i > 1) {
      j = (i >> 1);
      if (H[j]->date <= r) { break; }
      H[i] = H[j];
      i = j;
    }
    H[i] = I;
  }
}

static void aux_heap_sort (void) {
  int i, j, k, r;
  if (!HN) { return; }
  for (k = HN - 1; k > 0; k--) {
    tree_t *t = H[k+1];
    H[k+1] = H[1];
    r = t->date;
    i = 1;
    while (1) {
      j = 2*i;
      if (j > k) { break; }
      if (j < k && H[j+1]->date < H[j]->date) { j++; }
      if (r <= H[j]->date) { break; }
      H[i] = H[j];
      i = j;
    }
    H[i] = t;
  }
  for (i = 0; i < HN; i++) {
    H[i] = H[i+1];
  }
}

static void tree_fetch_recent (tree_t *T) {
  if (!T) {
    return;
  }
  aux_heap_insert (T);
  tree_fetch_recent (T->left);
  tree_fetch_recent (T->right);
}

int prepare_recent_friends (int user_id, int num) {
  user_t *U = get_user (user_id);
  if (!U || num <= 0 || num > 1000) {
    return -1;
  }
  R_max = num;
  HN = 0;

  tree_fetch_recent (U->fr_tree);
  aux_heap_sort ();

  R_end = R;
  int i;
  for (i = 0; i < HN; i++) {
    R_end[0] = H[i]->x;
    R_end[1] = H[i]->cat;
    R_end[2] = H[i]->date;
    R_end += 3;
  }

  return U->fr_cnt;
}

int prepare_friend_requests (int user_id, int num) {
  user_t *U = get_user (user_id);

  if ((!U && conv_uid (user_id) < 0) || num < -2 || num > 10000) {
    return -1;
  }

  R_end = R;

  if (!U) {
    return 0;
  }

  if (num < 0) {
    R_cat_mask = -1;
    R_mode = (num == -1 ? 2 : 0);

    tree_fetch (U->req_tree);

    return U->req_cnt;
  } else if (!num) {
    return U->req_cnt;
  }

  R_cat_mask = -1;
  R_mode = 2;
  R_max = num;

  tree_fetch_max (U->req_time_tree);
  int *R_tmp = R;
  while (R_tmp != R_end) {
    int t = R_tmp[0];
    R_tmp[0] = R_tmp[2];
    R_tmp[2] = -t;
    R_tmp += 3;
  }

  return U->req_cnt;
}


int get_friend_cat (int user_id, int friend_id) {
  user_t *U = get_user (user_id);
  if (!U || friend_id <= 0) {
    return -1;
  }
  tree_t *N = tree_lookup (U->fr_tree, friend_id);
  return N ? N->cat : -1;
}

int get_friend_request_cat (int user_id, int friend_id) {
  user_t *U = get_user (user_id);
  if (!U || friend_id <= 0) {
    return -1;
  }
  tree_t *N = tree_lookup (U->req_tree, friend_id);
  return N ? N->cat : -1;
}

int prepare_privacy_str (char *buff, int user_id, privacy_key_t privacy_key) {
  user_t *U = get_user (user_id);
  char *ptr = buff;

  if (!U || !privacy_key) {
    return -1;
  }
  privacy_t *P = privacy_lookup (U->pr_tree, privacy_key);
  if (!P) {
    return -1;
  }

  int len = (P->y & 0xff), *A = P->List;

  if (len == 0xff) {
    len = *A++;
    assert (len >= 0xff && len < MAX_PRIVACY_LEN);
  }
  assert (A[len] == -1);

  while (1) {
    int x = *A++, y;
    if (x == -1) {
      break;
    }
    if (!x) {
      continue;
    }
    y = x & PL_M_MASK;
    if (x & PL_M_CAT) {
      if (y != CAT_FR_FR && y != CAT_FR_ALL && y != U->cat_ver[y & 0x1f]) {
        A[-1] = 0;
        continue;
      }
      if (y >= CAT_FR_FR) {
        y |= -0x100;
      } else {
        y &= 0x1f;
      }
    }
    *ptr++ = ((unsigned) x >> 30)["+*-/"];
    if (y < 0) {
      assert (y == -1 || y == -3);
      *ptr++ = (y == -1 ? 'A' : 'G');
    } else {
      ptr += sprintf (ptr, "%d", y);
    }
  }
  *ptr = 0;
  return ptr - buff;
}

/* CHECK privacy */

int is_friends_friend (int user_id, int checker_id) {
  return -1;  // DUNNO
}

int check_privacy (int checker_id, int user_id, privacy_key_t privacy_key) {
  if (checker_id < 0 || user_id <= 0 || !privacy_key) {
    return -1;
  }
  if (!checker_id) {
    checker_id = -1;
  }
  if (checker_id == user_id) {
    return 3;
  }

  user_t *U = get_user (user_id);
  if (!U) {
    return -1;
  }

  privacy_t *P = privacy_lookup (U->pr_tree, privacy_key);
  if (!P) {
    return -1;
  }

  int *A = P->List, len = (P->y & 0xff);
  if (len == 255) {
    len = *A++;
    assert (len >= 255 && len <= MAX_PRIVACY_LEN);
  }
  assert (A[len] == -1);

  int x, y, z = 0, t = 3, m = -1;

  while (t) {
    x = *A++;
    if (x == -1) {
      t = 0;
      break;
    }
    if (!x) {
      continue;
    }
    y = x & PL_M_MASK;
    if (x & PL_M_CAT) {
      if (y == CAT_FR_ALL) {
        break;
      }
      if (y == CAT_FR_FR) {
        if (m == -1) {
          tree_t *N = tree_lookup (U->fr_tree, checker_id);
          m = (N ? N->cat : 0);
        }
        int w = (m > 0 ? 1 : is_friends_friend (user_id, checker_id));
        if (w > 0) {
          break;
        } else if (w < 0) {
          if (x >= 0) {
            z |= t & 1;
          }
          t &= 2;
        }
        continue;
      }
      if (y != U->cat_ver[y & 0x1f]) {
        A[-1] = 0;
        continue;
      }
      if (m == -1) {
        tree_t *N = tree_lookup (U->fr_tree, checker_id);
        m = (N ? N->cat : 0);
      }
      if ((m >> (y & 0x1f)) & 1) {
        break;
      }
    } else if (y == checker_id) {
      break;
    }
  }

  if (x >= 0) {
    z |= t;
  }
  return z;
}

void get_common_friends_num (int user_id, int user_num, const int *userlist, int *resultlist) {
  rev_friends_intersect_len = 0;
  rev_friends_find (rev_friends, user_id);
  assert (rev_friends_intersect_len <= MAX_FRIENDS);
  int i;
  for (i = 0; i < user_num; i++) {
    rev_friends_intersect_pos = 0;
    resultlist[i] = rev_friends_intersect (rev_friends, userlist[i]);
  }
}

int get_common_friends (int user_id, int user_num, const int *userlist, int *resultlist, int max_result) {
  rev_friends_intersect_len = 0;
  rev_friends_find (rev_friends, user_id);
  assert (rev_friends_intersect_len <= MAX_FRIENDS);
  rev_friends_intersect_pos = 0;
  return rev_friends_intersect_constructive (rev_friends, userlist[0], resultlist, max_result);
}
/*
 *
 * GENERIC BUFFERED WRITE
 *
 */

#define	BUFFSIZE	16777216

char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff, *wptr0 = Buff, *rend = Buff;
long long write_pos;
long long rbytes;
int newidx_fd;
int idx_fd;
int metafile_pos;

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

void flushout (void) {
  int w, s;
  if (wptr > wptr0) {
    s = wptr - wptr0;
    write_pos += s;
    metafile_pos += s;
  }
  if (rptr < wptr) {
    s = wptr - rptr;
    w = write (newidx_fd, rptr, s);
    assert (w == s);
  }
  rptr = wptr = wptr0 = Buff;
}

void clearin (void) {
  rptr = wptr = wptr0 = Buff + BUFFSIZE;
}

static inline void writeout (const void *D, size_t len) {
  const char *d = D;
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
  wptr += 8;
}

static inline void writeout_int (int value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 4)) {
    flushout();
  }
  *((int *) wptr) = value;
  wptr += 4;
}

static inline void writeout_short (int value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 2)) {
    flushout();
  }
  *((short *) wptr) = value;
  wptr += 2;
}

static inline void writeout_char (char value) {
  if (unlikely (wptr == Buff + BUFFSIZE)) {
    flushout();
  }
  *wptr++ = value;
}

static void write_seek (long long new_pos) {
  flushout();
  assert (lseek (newidx_fd, new_pos, SEEK_SET) == new_pos);
  write_pos = new_pos;
}

static void *readin (size_t len) {
  assert (len >= 0);
  if (rptr + len <= wptr) {
    return rptr;
  }
  if (wptr < Buff + BUFFSIZE) {
    return 0;
  }
  memcpy (Buff, rptr, wptr - rptr);
  wptr -= rptr - Buff;
  rptr = Buff;
  int r = read (idx_fd, wptr, Buff + BUFFSIZE - wptr);
  if (r < 0) {
    fprintf (stderr, "error reading file: %m\n");
  } else {
    wptr += r;
  }
  if (rptr + len <= wptr) {
    return rptr;
  } else {
    return 0;
  }
}

void readadv (size_t len) {
  assert (len >= 0 && len <= wptr - rptr);
  rptr += len;
  rbytes += len;
}



extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;

int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    return 0;
  }
  idx_fd = Index->fd;
  index_header header;
  assert (read (idx_fd, &header, sizeof (index_header)) == sizeof (index_header));
  if ((!reverse_friends_mode || header.magic != REVERSE_FRIEND_INDEX_MAGIC) &&
      (reverse_friends_mode || header.magic != FRIEND_INDEX_MAGIC)) {
    fprintf (stderr, "index file is not for friends-engine. Magic = %x.\n", header.magic);
    return -1;
  }
  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;

  tot_users = header.tot_users;
  int i;
  clearin ();
  long long fr_tree_bytes = 0;
  long long req_tree_bytes = 0;
  long long req_time_tree_bytes = 0;
  long long pr_tree_bytes = 0; 
  for (i = 0; i < tot_users; i++) {
    if (verbosity >= 3) {
      fprintf (stderr, "reading user %d of %d\n", i + 1, tot_users);
    }
    char *ptr = readin (4 + 8 + 16 + 32 * 4);
    if (!ptr) {
      fprintf (stderr, "Unexpected end of file");
      assert (0);
    }
    int x = *((int *)ptr);
    readadv (4);
    ptr += 4;

    assert (x >= 0 && x < MAX_USERS);
    User[x] = zmalloc0 (sizeof (user_t));
    memcpy (&User[x]->user_id, rptr, 8);
    readadv (8);
    ptr += 8;

    memcpy (&User[x]->req_cnt, rptr, 16 + 32 * 4);
    readadv (16 + 32 * 4);
    ptr += 16 + 32 * 4;

    fr_tree_bytes -= rbytes;
    User[x]->fr_tree = read_tree ();
    fr_tree_bytes += rbytes;
    if (!reverse_friends_mode) {
      req_tree_bytes -= rbytes;
      User[x]->req_tree = read_tree ();
      req_tree_bytes += rbytes;
      req_time_tree_bytes -= rbytes;
      User[x]->req_time_tree = read_tree ();
      req_time_tree_bytes += rbytes;
      pr_tree_bytes -= rbytes;
      User[x]->pr_tree = read_privacy ();
      pr_tree_bytes += rbytes;
    }
  }
  if (reverse_friends_mode) {
    rev_friends = read_rev_friends ();
  }
  assert (rptr == wptr);
  replay_logevent = friends_replay_logevent;
  if (verbosity) {
    fprintf (stderr, "Index loaded. %lld bytes for friends, %lld for friend requests, %lld for time friend requests and %lld for privacy\n", fr_tree_bytes, req_tree_bytes, req_time_tree_bytes, pr_tree_bytes);
  }
  return 0;
}


int save_index (int writing_binlog) {
  char *newidxname = NULL;

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (log_cur_pos() == jump_log_pos) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
       newidxname, jump_log_pos);
    return 0;
  } 

  if (verbosity > 0) {
    fprintf (stderr, "creating index %s at log position %lld\n", newidxname, log_cur_pos());
  }

  newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }
  index_header header;
  memset (&header, 0, sizeof (header));

  if (!reverse_friends_mode) {
    header.magic = FRIEND_INDEX_MAGIC;
  } else {
    header.magic = REVERSE_FRIEND_INDEX_MAGIC;
  }
  header.created_at = time (NULL);
  header.log_pos1 = log_cur_pos ();
  header.log_timestamp = log_read_until;
  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }
  header.log_pos1_crc32 = ~log_crc32_complement;
  header.log_split_min = log_split_min;
  header.log_split_max = log_split_max;
  header.log_split_mod = log_split_mod;
  header.tot_users = tot_users;

  write_seek (0);
  writeout (&header, sizeof (header));

  if (verbosity >= 2) {
    fprintf (stderr, "Written header\n");
  }

  int cc = 0;
  int i;
  for (i = 0; i < MAX_USERS; i++) {
    if (User[i]) {
      if (verbosity >= 3) {
        fprintf (stderr, "Writing user %d (real %d)\n", cc, i);
      }
      cc++;
      writeout_int (i);
      writeout (&User[i]->user_id, 8);
      writeout (&User[i]->req_cnt, 16 + 32 * 4);
      dump_tree (User[i]->fr_tree);
      if (!reverse_friends_mode) {
        dump_tree (User[i]->req_tree);
        dump_tree (User[i]->req_time_tree);
        dump_privacy_tree (User[i]->pr_tree);
      }
    }
  }
  assert (cc == tot_users);
  if (reverse_friends_mode) {
    dump_rev_friends (rev_friends);
  }
  flushout ();

  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);

  if (verbosity >= 3) {
    fprintf (stderr, "writing index done\n");
  }

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);

  return 0;
}
