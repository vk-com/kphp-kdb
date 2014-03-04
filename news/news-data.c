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
              2010-2011 Nikolai Durov
              2010-2011 Andrei Lopatin
              2011-2013 Vitaliy Valtman
              2011-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <time.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kdb-news-binlog.h"
#include "news-data.h"
#include "server-functions.h"
#include "kfs.h"
#include "net-aio.h"
#include "vv-tl-aio.h"

#define	BOOKMARKS_MALLOC	0
#define	METAFILE_MALLOC_THRESHOLD		0
#define ALLOW_OLD_INDEX_MODE	0
#define MAX_NEW_BOOKMARK_USERS	10000000

extern int index_mode;

int max_items[32] = {
2, 2, 60, 2, 2, 60, 60, 60,
60, 60, 60, 60, 2, 2, 60, 2,
2, 2, 2, 2, 60, 2, 2, 60,
60, 60, 60, 2, 2, 2, 2, 2 };

int grouping_type[32] = {
0, 0, 1, 2, 2, 1, 1, 1,
1, 1, 1, 1, 2, 2, 1, 0,
2, 2, 2, 2, 1, 2, 2, 1,
1, 1, 1, 0, 0, 0, 0, 0
};

int max_num[3] = {0, 100, 200};
/* --------- item lists ------------ */


int max_news_days = MAX_NEWS_DAYS, min_logevent_time;
int binlog_read;

int items_kept, comments_kept, userplace_kept;
long long garbage_objects_collected = 0,
          garbage_users_collected = 0,
          items_removed_in_process_new = 0,
          items_removed_in_prepare_updates = 0,
          dups_removed_in_process_raw_updates = 0,
          dups_users_removed_from_urlist = 0;
static item_t *new_item (void) {
  assert (UG_MODE);
  items_kept++;
  return zmalloc0 (sizeof (item_t));
}

static void free_item (item_t *A) {
  assert (UG_MODE);
  items_kept--;
  zfree (A, sizeof (item_t));
}

static comment_t *new_comment (void) {
  assert (COMM_MODE);
  comments_kept++;
  return zmalloc0 (sizeof (comment_t));
}

static void free_comment (comment_t *A) {
  assert (COMM_MODE);
  comments_kept--;
  zfree (A, sizeof (comment_t));
}

static notify_t *new_notify_item (void) {
  assert (NOTIFY_MODE);
  items_kept++;
  notify_t *p = zmalloc0 (sizeof (notify_t));
  vkprintf (6, "new_notify_item: result = %p\n", p);
  return p;
}

static void free_notify_item (notify_t *A) {
  assert (NOTIFY_MODE);
  assert (items_kept-- > 0);
  zfree (A, sizeof (notify_t));
}

static userplace_t *new_userplace (void) {
  assert (NOTIFY_MODE);
  userplace_kept ++;
  return zmalloc0 (sizeof (userplace_t));
}

static void free_userplace (userplace_t *A) {
  assert (NOTIFY_MODE);
  assert (userplace_kept -- > 0);
  zfree (A, sizeof (userplace_t));
}

/* returns number removed items */
static int remove_old_items (user_t *U) {
  assert (UG_MODE);
  int x = U->tot_items, y;
  if (!x) {
    return 0;
  }
  y = now - (max_news_days + 1) * 86400;

  item_t *p = U->last, *q;
  const int old_tot_items = x;
  while (p != (item_t *) U && (x > MAX_USER_ITEMS || p->date <= y)) {
    q = p->prev;
    free_item (p);
    x--;
    p = q;
  }
  U->last = p;
  p->next = (item_t *) U;
  U->tot_items = x;
  assert (U->tot_items >= 0);
  return old_tot_items - x;
}

static void free_item_list (user_t *U) {
  assert (UG_MODE);
  item_t *p = U->first, *q;
  while (p != (item_t *) U) {
    q = p->next;
    U->tot_items--;
    assert (U->tot_items >= 0);
    free_item (p);
    p = q;
  }
  assert (!U->tot_items);
  U->first = U->last = (item_t *) U;
}

static int remove_old_comments (place_t *U) {
  assert (COMM_MODE);
  int x = U->tot_comments, y;
  if (!x) {
    return 0;
  }
  y = now - (max_news_days + 1) * 86400;

  comment_t *p = U->last, *q;
  const int old_tot_comments = x;
  while (p != (comment_t *) U && (x > MAX_PLACE_COMMENTS || (p->date & 0x7fffffff) <= y)) {
    q = p->prev;
    free_comment (p);
    x--;
    p = q;
  }
  U->last = p;
  p->next = (comment_t *) U;
  U->tot_comments = x;
  assert (U->tot_comments >= 0);
  return old_tot_comments - x;
}

static void free_comment_list (place_t *U) {
  assert (COMM_MODE);
  comment_t *p = U->first, *q;
  while (p != (comment_t *) U) {
    q = p->next;
    U->tot_comments--;
    assert (U->tot_comments >= 0);
    free_comment (p);
    p = q;
  }
  assert (!U->tot_comments);
  U->first = U->last = (comment_t *) U;
}

static place_t *get_place_f (int type, int owner, int place, int force);
static userplace_t *get_userplace_f (int type, int owner, int place, int user_id, int force);
static user_t *get_user_f (int user_id, int force);

int tot_user_notify_allocated;

static void delete_notify (notify_t *p, userplace_t *up, notify_user_t *U, notify_place_t *V) {
  assert (NOTIFY_MODE);
  vkprintf (5, "delete_notity: p = %p, up = %p, U = %p, V = %p, p->random_tag = %d\n", p, up, U, V, p->random_tag);
  vkprintf (5, "delete_notity: p->prev = %p, p->next = %p\n", p->prev, p->next);
  p->next->prev = p->prev;
  p->prev->next = p->next;
  if (!U) {
    U = (notify_user_t *)get_user_f (up->user_id, 0);
    assert (U);
  }
  if (!V) {
    V = (notify_place_t *)get_place_f (up->type, up->owner, up->place, 0);
    assert (V);
  }
  assert (--up->allocated_items >= 0);
  assert (--U->allocated_items >= 0);
  assert (--V->allocated_items >= 0);
  tot_user_notify_allocated --;
  if (p->random_tag < 0) {
    up->total_items += p->random_tag;
    assert (up->total_items >= 0);
    U->total_items += p->random_tag;
    assert (U->total_items >= 0);
    V->total_items += p->random_tag;
    assert (V->total_items >= 0);
  }
  p->random_tag = -1000000000;
  free_notify_item (p);
}

static void delete_userplace_use (userplace_t *up, notify_user_t *U) {
  assert (NOTIFY_MODE);
  if (!U) {
    U = (notify_user_t *)get_user_f (up->user_id, 0);
    assert (U);
  }
  if (up->uprev != (userplace_t *)U) {
    up->uprev->unext = up->unext;
  } else {
    U->first = up->unext;
  }
  if (up->unext != (userplace_t *)U) {
    up->unext->uprev = up->uprev;
  } else {
    U->last = up->uprev;
  }
}

static void add_userplace_use (userplace_t *up, notify_user_t *U) {
  assert (NOTIFY_MODE);
  if (!U) {
    U = (notify_user_t *)get_user_f (up->user_id, 0);
    assert (U);
  }
  up->uprev = (userplace_t *)U;
  up->unext = U->first;
  U->first = up;
  if (up->unext != (userplace_t *)U) {
    up->unext->uprev = up;
  } else {
    U->last = up;
  }
}

static void update_userplace_use (userplace_t *up, notify_user_t *U) {
  assert (NOTIFY_MODE);
  if (!U) {
    U = (notify_user_t *)get_user_f (up->user_id, 0);
    assert (U);
  }
  delete_userplace_use (up, U);
  add_userplace_use (up, U);
}

static void delete_userplace (userplace_t *up, notify_user_t *U, notify_place_t *V) {
  assert (NOTIFY_MODE);
  assert (!up->allocated_items);
  assert (!up->total_items);
  assert (up->first == (notify_t *)up);
  assert (up->last == (notify_t *)up);
  vkprintf (5, "delete_userplace: up = %p, U = %p, V = %p, up->user_id = %d\n", up, U, V, up->user_id);
  vkprintf (5, "delete_userplace: up->unext = %p, up->uprev = %p\n", up->unext, up->uprev);

  if (!U) {
    U = (notify_user_t *)get_user_f (up->user_id, 0);
    assert (U);
  }
  if (!V) {
    V = (notify_place_t *)get_place_f (up->type, up->owner, up->place, 0);
    assert (V);
  }

  delete_userplace_use (up, U);

  if (up->pprev != (userplace_t *)V) {
    up->pprev->pnext = up->pnext;
  } else {
    V->first = up->pnext;
  }
  if (up->pnext != (userplace_t *)V) {
    up->pnext->pprev = up->pprev;
  } else {
    V->last = up->pprev;
  }

  assert (up == get_userplace_f (up->type, up->owner, up->place, up->user_id, -1));
  up->user_id = -1;
  free_userplace (up);
}

static int remove_old_notify_items_userplace (userplace_t *up, notify_user_t *U, notify_place_t *V) {
  assert (NOTIFY_MODE);
  vkprintf (4, "remove_old_notify_items_userplace: up = %p\n", up);
  vkprintf (5, "remove_old_notify_items_userplace: up->type = %d, up->owner = %d, up->place = %d, up->user_id = %d\n", up->type, up->owner, up->place, up->user_id);
  if (!up->allocated_items) {
    return 0;
  }
  if (!U) {
    U = (notify_user_t *)get_user_f (up->user_id, 0);
    assert (U);
  }
  if (!V) {
    V = (notify_place_t *)get_place_f (up->type, up->owner, up->place, 0);
    assert (V);
  }
  int d = now - (max_news_days + 1) * 86400;

  notify_t *p = (notify_t *)up->last, *q, *r;
  const int old_tot_items = up->allocated_items;
  while (p != (notify_t *) up && ((up->allocated_items > max_items[up->type]) || (p->date & 0x7fffffff) <= d)) {
    q = p->prev;
    if ((p->date & 0x7fffffff) <= d || p->random_tag > 0) {
      if (p->date < 0 && p->random_tag > 0) {
        for (r = q; r->random_tag > 0; r = r->prev) {
          assert (r != (notify_t *)up);
        }
        r->random_tag ++;
        assert (r->random_tag <= 0);
        up->total_items --;
        U->total_items --;
        V->total_items --;
      }
      delete_notify (p, up, U, V);
    }
    p = q;
  }
  return old_tot_items - up->allocated_items;
}

static int free_notify_items_userplace (userplace_t *up, notify_user_t *U, notify_place_t *V) {
  assert (NOTIFY_MODE);
  vkprintf (5, "free_notify_items_userplace: up = %p, up->user_id = %d, up->type = %d, up->owner = %d, up->place = %d\n", up, up->user_id, up->type, up->owner, up->place);
  if (!up->allocated_items) {
    return 0;
  }
  if (!U) {
    U = (notify_user_t *)get_user_f (up->user_id, 0);
    assert (U);
  }
  if (!V) {
    V = (notify_place_t *)get_place_f (up->type, up->owner, up->place, 0);
    assert (V);
  }

  notify_t *p = (notify_t *)up->last, *q;
  const int old_tot_items = up->allocated_items;
  while (p != (notify_t *) up) {
    q = p->prev;
    delete_notify (p, up, U, V);
    p = q;
  }
  return old_tot_items - up->allocated_items;
}

static int delete_place (int type, int owner, int place);
static int remove_old_notify_items (notify_user_t *U) {
  assert (NOTIFY_MODE);
  vkprintf (4, "remove_old_notify_items: U = %p\n", U);
  if (!U->allocated_items) {
    return 0;
  }
  userplace_t *up = U->first, *uq;
  const int old_tot_items = U->allocated_items;
  int num[3] = {0, 0, 0};
  while (up != (userplace_t *)U) {
    uq = up->unext;
    vkprintf (6, "up->user_id = %d, U->user_id = %d\n", up->user_id, U->user_id);
    int t = grouping_type[up->type];
    notify_place_t *V = (notify_place_t *)get_place_f (up->type, up->owner, up->place, 0);
    assert (V);

    if (num[t] >= max_num[t]) {
      free_notify_items_userplace (up, U, V);
      delete_userplace (up, U, V);
    } else {
      remove_old_notify_items_userplace (up, U, V);
      if (!up->allocated_items) {
        delete_userplace (up, U, V);
      } else {
        num[t] ++;
      }
    }
    if (!V->allocated_items) {
      delete_place (V->type, V->owner, V->place);
    }
    up = uq;
  }
  //vkprintf (1, "U->total_items = %d, U->allocated_items = %d, items_kept = %d\n", U->total_items, U->allocated_items, notif_kept);
  assert (items_kept == tot_user_notify_allocated);
  if ((items_kept & ((1 << 15) - 1)) == 0) {
    vkprintf (1, "items_kept = %d\n", items_kept);
  }
  //assert (U->allocated_items <= 700);
  return old_tot_items - U->allocated_items;
}

static void free_notify_user_list (notify_user_t *U) {
  assert (NOTIFY_MODE);
  userplace_t *up = (userplace_t *) U->first, *uq;
  while (up != (userplace_t *) U) {
    uq = up->unext;
    free_notify_items_userplace (up, U, 0);
    notify_place_t *V = (notify_place_t *)get_place_f (up->type, up->owner, up->place, 0);
    assert (V);

    delete_userplace (up, U, V);

    if (!V->allocated_items) {
      delete_place (V->type, V->owner, V->place);
    }

    up = uq;
  }
  assert (!U->total_items);
  assert (!U->allocated_items);
}

static void free_notify_place_list (notify_place_t *V) {
  assert (NOTIFY_MODE);
  userplace_t *up = (userplace_t *) V->first, *uq;
  while (up != (userplace_t *) V) {
    uq = up->pnext;
    assert (up->type == V->type && up->owner == V->owner && up->place == V->place);
    free_notify_items_userplace (up, 0, V);
    delete_userplace (up, 0, V);
    up = uq;
  }
  assert (!V->total_items);
  assert (!V->allocated_items);
}


/* --------- news data ------------- */
#if BOOKMARKS_MALLOC
# define NIL_BOOKMARK	-1
#else
# define NIL_BOOKMARK	0
#endif

int tot_users, tot_places, max_uid, ug_mode;
unsigned allowed_types_mask;
user_t *User[MAX_USERS];
place_t *Place[PLACES_HASH];
userplace_t *UserPlace[PLACES_HASH];

int index_exists;

struct bookmark_user new_users[MAX_NEW_BOOKMARK_USERS];
int new_users_number;

int add_del_bookmark (int user_id, int type, int owner, int place, int y);

int news_replay_logevent (struct lev_generic *E, int size);

static void recommend_rate_tbl_reset (void);

int init_news_data (int schema) {

  replay_logevent = news_replay_logevent;

  recommend_rate_tbl_reset ();

  memset (User, 0, sizeof(User));
  max_uid = tot_users = 0;

  memset (Place, 0, sizeof (Place));
  tot_places = 0;

  return 0;
}

static int news_get_allowed_types_mask (int user_mode) {
  switch (user_mode) {
    case -1: return GROUP_TYPES_MASK;
    case 0: return USER_TYPES_MASK;
    case 1: return COMMENT_TYPES_MASK;
    case 2: return NOTIFY_TYPES_MASK;
    case 3: return RECOMMEND_TYPES_MASK;
  }
  kprintf ("%s: unknown user mode %d.\n", __func__, user_mode);
  assert (0);
  return 0;
}

static int news_le_start (struct lev_start *E) {
  switch (E->schema_id) {
  case NEWS_SCHEMA_USER_V1:
    ug_mode = 0;
    allowed_types_mask = USER_TYPES_MASK;
    break;
  case NEWS_SCHEMA_GROUP_V1:
    ug_mode = -1;
    allowed_types_mask = GROUP_TYPES_MASK;
    break;
  case NEWS_SCHEMA_COMMENT_V1:
    ug_mode = 1;
    allowed_types_mask = COMMENT_TYPES_MASK;
    break;
  case NEWS_SCHEMA_NOTIFY_V1:
    ug_mode = 2;
    allowed_types_mask = NOTIFY_TYPES_MASK;
    break;
  case NEWS_SCHEMA_RECOMMEND_V1:
    ug_mode = 3;
    allowed_types_mask = RECOMMEND_TYPES_MASK;
    break;
  default:
    return -1;
  }
  assert (allowed_types_mask == news_get_allowed_types_mask (ug_mode));
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

  return 0;
}

/* ---- atomic data modification/replay binlog ----- */

static inline int valid_type (unsigned t) {
  return t < 32 && ((allowed_types_mask >> t) & 1);
}

//static int check_place (int place) {
//  assert (ug_mode > 0);
//  if (place <= 0 || place % log_split_mod != log_split_min) { return -1; }
//  return 0;
//}

static int check_obj (int type, int owner, int place) {
  assert (COMM_MODE || NOTIFY_MODE);
  if (COMM_MODE) {
    return (check_split (place) || check_split (owner)) && (type >= 20) && (type <= 24);
  } else {
    return (type >= 0) && (type <= 31);
  }
}

static place_t *get_place_f (int type, int owner, int place, int force) {
  assert (NOTIFY_MODE || COMM_MODE);
  vkprintf (5, "get_place_f: type = %d, owner = %d, place = %d, force = %d\n", type, owner, place, force);
  unsigned h = ((unsigned) (type * 17239 + owner * 239 + place)) % PLACES_HASH;
  place_t **p = Place + h, *V;
  while (*p) {
    V = *p;
    if (V->place == place && V->owner == owner && V->type == type) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = Place[h];
        Place[h] = V;
      }
      vkprintf (5, "get_place_f: result = %p\n", V);
      return V;
    }
    p = &V->hnext;
  }
  if (force <= 0) {
    vkprintf (5, "get_place_f: nothing found\n");
    return 0;
  }
  if (COMM_MODE) {
    V = zmalloc0 (sizeof (place_t));
  } else {
    assert (NOTIFY_MODE);
    V = zmalloc0 (sizeof (notify_place_t));
  }
  vkprintf (4, "Creating new place: type = %d, owner = %d, place = %d: p = %p\n", type, owner, place, V);
  V->type = type;
  V->owner = owner;
  V->place = place;
  V->hnext = Place[h];
  V->first = V->last = (comment_t *) V;
  Place[h] = V;
  tot_places++;
  return V;
}

static userplace_t *get_userplace_f (int type, int owner, int place, int user_id, int force) {
  assert (NOTIFY_MODE);
  unsigned h = ((unsigned) (type * 17239 + owner * 239 + place + 3 * user_id)) % PLACES_HASH;
  userplace_t **p = UserPlace + h, *up;
  while (*p) {
    up = *p;
    if (up->place == place && up->owner == owner && up->type == type && up->user_id == user_id) {
      *p = up->hnext;
      if (force >= 0) {
        up->hnext = UserPlace[h];
        UserPlace[h] = up;
      }
      return up;
    }
    p = &up->hnext;
  }
  if (force <= 0) {
    return 0;
  }
  up = new_userplace ();
  vkprintf (4, "Creating new userplace: type = %d, owner = %d, place = %d, user_id = %d: p = %p\n", type, owner, place, user_id, up);
  up->type = type;
  up->owner = owner;
  up->place = place;
  up->user_id = user_id;
  up->hnext = UserPlace[h];
  up->first = up->last = (notify_t *) up;
  UserPlace[h] = up;

  notify_user_t *U = (notify_user_t *)get_user_f (up->user_id, 1);
  assert (U);
  add_userplace_use (up, U);

  notify_place_t *V = (notify_place_t *)get_place_f (type, owner, place, 1);
  assert (V);
  up->pprev = (userplace_t *)V;
  up->pnext = V->first;
  V->first = up;
  if (up->pnext != (userplace_t *)V) {
    up->pnext->pprev = up;
  } else {
    V->last = up;
  }
  return up;
}

int conv_uid (int user_id) {
  assert (UG_MODE || NOTIFY_MODE || RECOMMEND_MODE);
  if (UG_MODE) {
    user_id = (user_id ^ ug_mode) - ug_mode;
  }
  if (user_id <= 0 ) { return -1; }
  if (user_id % log_split_mod != log_split_min) { return -1; }
  user_id /= log_split_mod;
  return user_id < MAX_USERS ? user_id : -1;
}

static user_t *get_user (int user_id) {
  int i = conv_uid (user_id);
  if (i >= 0 && User[i]) {
    assert (i == User[i]->user_id);
  }
  return i >= 0 ? User[i] : 0;
}

static void free_user_struct (user_t *U) {
  if (UG_MODE) {
    zfree (U, sizeof (user_t));
  } else if (NOTIFY_MODE) {
    zfree (U, sizeof (notify_user_t));
  } else {
    assert (RECOMMEND_MODE);
    zfree (U, sizeof (recommend_user_t));
  }
}

static user_t *get_user_f (int user_id, int force) {
  vkprintf (5, "get_user_f: user_id = %d, force = %d\n", user_id, force);
  if (force == 0) {
    return get_user (user_id);
  }
  int i = conv_uid (user_id);
  vkprintf (5, "conv_id = %d\n", i);
  user_t *U;
  if (i < 0) { return 0; }
  U = User[i];
  if (U) { return U; }
  if (UG_MODE) {
    U = zmalloc0 (sizeof (user_t));
    U->priv_mask = 1;
    U->first = U->last = (item_t *) U;
    U->user_id = i;
  } else if (NOTIFY_MODE) {
    U = zmalloc0 (sizeof (notify_user_t));
    ((notify_user_t *) U)->first = ((notify_user_t *) U)->last = (userplace_t *) U;
    ((notify_user_t *) U)->user_id = i;
  } else {
    assert (RECOMMEND_MODE);
    U = zmalloc0 (sizeof (recommend_user_t));
    ((recommend_user_t *) U)->first = ((recommend_user_t *) U)->last = (recommend_item_t *) U;
    ((recommend_user_t *) U)->user_id = i;
  }
  vkprintf (5, "creating new user: U = %p, user_id = %d\n", U, user_id);
  User[i] = U;
  if (i > max_uid) { max_uid = i; }
  tot_users++;
  return U;
}

static void free_recommend_user_list (recommend_user_t *U);

static int delete_user_by_idx (int i) {
  if (i < 0) {
    return -1;
  }
  user_t *U = User[i];
  if (!U) {
    return 0;
  }
  User[i] = 0;

  U->user_id = -1;

  assert (UG_MODE || NOTIFY_MODE || RECOMMEND_MODE);
  if (UG_MODE) {
    free_item_list (U);
  } else if (NOTIFY_MODE) {
    free_notify_user_list ((notify_user_t *) U);
  } else {
    assert (RECOMMEND_MODE);
    free_recommend_user_list ((recommend_user_t *) U);
  }

  free_user_struct (U);
  tot_users--;
  return 1;
}

static int delete_user (int user_id) {
  if (now < min_logevent_time) {
    return 0;
  }
  return delete_user_by_idx (conv_uid (user_id));
}

static int set_privacy (int user_id, int mask) {
  assert (UG_MODE);
  user_t *U = get_user_f (user_id, 1);
  if (!U) {
    return -1;
  }
  U->priv_mask = mask | 1;
  return 1;
}

static int process_news_item (struct lev_news_item *E) {
  if (now < min_logevent_time) {
    return 0;
  }
  user_t *U = get_user_f (E->user_id, 1);
  int t = E->type & 0xff;
  if (!U || !UG_MODE || !valid_type (t)) {
    return -1;
  }
  /*
  if ((t == 6 || E->user_id == 1034437) && verbosity > 2) {
     fprintf (stderr, "pos=%lld: process_news_item(%d,%d,%d,%d,%d,%d,%d,%d)\n", log_cur_pos(), E->user_id, now, t, E->user, E->group, E->owner, E->place, E->item);
  }
  */

  item_t *p, *q = U->first;

  if (q != (item_t *) U && q->date == now && q->type == t &&
      q->item == E->item && q->place == E->place && q->owner == E->owner &&
      q->user == E->user && q->group == E->group) {
    return 0;
  }

  p = new_item ();

  p->next = q;
  q->prev = p;
  p->prev = (item_t *) U;
  U->first = p;
  U->tot_items++;

  p->type = t;
  p->date = now;
  p->random_tag = lrand48() & 0x7fffffff;

  p->user = E->user;
  p->group = E->group;
  p->owner = E->owner;
  p->place = E->place;
  p->item = E->item;

  vkprintf (2, "new record stored: user_id=%d type=%d date=%d tag=%d\n", E->user_id, p->type, p->date, p->random_tag);

  items_removed_in_process_new += remove_old_items (U);

  return 1;
}

static int delete_place (int type, int owner, int place) {
  if (now < min_logevent_time) {
    return 0;
  }
  if (!check_obj (type, owner, place)) {
    return -1;
  }
  place_t *V = get_place_f (type, owner, place, -1);
  vkprintf (2, "delete_place: V = %p\n", V);
  if (!V) {
    return 0;
  }

  assert (COMM_MODE || NOTIFY_MODE);
  if (COMM_MODE) {
    free_comment_list (V);
    V->place = -1;
    zfree (V, sizeof (place_t));
  } else {
    free_notify_place_list ((notify_place_t *)V);
    V->place = -1;
    zfree (V, sizeof (notify_place_t));
  }
  tot_places--;
  return 1;
}

static int show_hide_comment (int type, int owner, int place, int item, int shown) {
  if (now < min_logevent_time) {
    return 0;
  }
  if (!check_obj (type, owner, place) || !COMM_MODE || !valid_type (type)) {
    return -1;
  }

  place_t *V = get_place_f (type, owner, place, 0);
  if (!V) {
    return 0;
  }

  comment_t *p;
  int res = 0;
  for (p = V->first; p != (comment_t *) V; p = p->next) {
    if (p->item == item && (p->date ^ shown) >= 0) {
      res++;
      p->date ^= (-1 << 31);
    }
  }

  return res;
}


static int process_news_comment (struct lev_news_comment *E) {
  if (now < min_logevent_time) {
    return 0;
  }
  int t = E->type & 0xff;
  if (!check_obj (t, E->owner, E->place) || !COMM_MODE) {
    return -1;
  }
  if (index_mode) {
    return 0;
  }

  place_t *V = get_place_f (t, E->owner, E->place, 1);

  comment_t *p = new_comment (), *q = V->first;

  p->next = q;
  q->prev = p;
  p->prev = (comment_t *) V;
  V->first = p;
  V->tot_comments++;

  p->date = now;
  p->random_tag = lrand48() & 0x7fffffff;

  p->user = E->user;
  p->group = E->group;
  p->item = E->item;

  vkprintf (2, "new comment stored: place=%d:%d:%d item=%d date=%d tag=%d\n", V->type, V->owner, V->place, p->item, p->date, p->random_tag);

  items_removed_in_process_new += remove_old_comments (V);
  return 1;
}

static int process_news_notify (struct lev_news_notify *E) {
  if (now < min_logevent_time) {
    return 0;
  }
  int t = E->type & 0xff;
  if (!NOTIFY_MODE || !valid_type (t)) {
    return -1;
  }
  userplace_t *up = get_userplace_f (t, E->owner, E->place, E->user_id, 1);
  if (!up) {
    return -1;
  }

  notify_user_t *U = (notify_user_t *)get_user_f (E->user_id, 0);
  assert (U);
  notify_place_t *V = (notify_place_t *)get_place_f (t, E->owner, E->place, 0);
  assert (V);

  update_userplace_use (up, U);

  /*
  if ((t == 6 || E->user_id == 1034437) && verbosity > 2) {
     fprintf (stderr, "pos=%lld: process_news_item(%d,%d,%d,%d,%d,%d,%d,%d)\n", log_cur_pos(), E->user_id, now, t, E->user, E->group, E->owner, E->place, E->item);
  }
  */

  notify_t *p, *q = up->first;
  if (q == (notify_t *)up || q->date / 86400 != now / 86400) {
    p = new_notify_item ();
    p->next = up->first;
    p->next->prev = p;
    p->prev = (notify_t *)up;
    up->first = p;
    p->random_tag = 0;
    p->date = (((now)/ 86400) + 1) * 86400 - 1;

    U->allocated_items ++;
    tot_user_notify_allocated ++;
    V->allocated_items ++;
    up->allocated_items ++;
  }

  assert (up->first->random_tag <= 0);
  up->first->random_tag --;
  p = new_notify_item ();

  p->next = up->first->next;
  up->first->next = p;
  p->prev = up->first;
  p->next->prev = p;

  U->total_items ++;
  V->total_items ++;
  up->total_items ++;
  U->allocated_items ++;
  tot_user_notify_allocated ++;
  V->allocated_items ++;
  up->allocated_items ++;



  p->date = now;
  p->random_tag = lrand48() & 0x7fffffff;
  if (p->random_tag == 0) {
    p->random_tag = 1;
  }
  p->user = E->user;
  p->item = E->item;

  vkprintf (2, "new record stored: user_id=%d type=%d date=%d tag=%d\n", E->user_id, t, p->date, p->random_tag);

  items_removed_in_process_new += remove_old_notify_items (U);

  return 1;
}

static int show_hide_notify_userplace (userplace_t *up, notify_user_t *U, notify_place_t *V,  int item, int shown) {
  if (now < min_logevent_time) {
    return 0;
  }
  notify_t *p;
  int res = 0;
  for (p = up->first; p != (notify_t *) up; p = p->next) {
    if (p->item == item && (p->date ^ shown) >= 0) {
      res++;
      p->date ^= (-1 << 31);
    }
  }
  return res;
}

static int show_hide_notify (int type, int owner, int place, int item, int shown) {
  vkprintf (4, "show_hide_notify: type = %d, owner = %d, place = %d, item = %d, shown = %d, time = %lf\n", type, owner, place, item, shown, (double)time (0));
  if (now < min_logevent_time) {
    return 0;
  }
  if (!check_obj (type, owner, place) || !NOTIFY_MODE || !valid_type (type)) {
    return -1;
  }

  notify_place_t *V = (notify_place_t *)get_place_f (type, owner, place, 0);
  vkprintf (4, "V = %p, time = %lf\n", V, (double)time (0));
  if (!V) {
    return 0;
  }

  userplace_t *up = V->first;
  int res = 0;
  while (up != (userplace_t *)V) {
    res += show_hide_notify_userplace (up, 0, V, item, shown);
    up = up->pnext;
  }


  return res;
}

static int show_hide_user_notify (int user_id, int type, int owner, int place, int item, int shown) {
  vkprintf (4, "show_hide_user_notify: type = %d, owner = %d, place = %d, item = %d, shown = %d, time = %lf\n", type, owner, place, item, shown, (double)time (0));
  if (now < min_logevent_time) {
    return 0;
  }
  userplace_t *up = get_userplace_f (type, owner, place, user_id, 0);
  if (!up) {
    return 0;
  }
  return show_hide_notify_userplace (up, 0, 0, item, shown);
}

/* --------- replay log ------- */
static int process_news_recommend (struct lev_news_recommend *E);
static int set_recommend_rate (struct lev_news_set_recommend_rate *E);

long long last_log_pos;

int news_replay_logevent (struct lev_generic *E, int size) {
  int s;

  update_offsets (0);

  vkprintf (5, "E->type = %x\n", E->type);

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return news_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_TO:
  case LEV_ROTATE_FROM:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_NEWS_USERDEL:
    if (size < 8) { return -2; }
    if (!UG_MODE) { return -1; }
    delete_user (E->a);
    return 8;
  case LEV_NEWS_PRIVACY:
    if (size < 12) { return -2; }
    if (!UG_MODE) { return -1; }
    set_privacy (E->a, E->b);
    return 12;
  case LEV_NEWS_ITEM+1 ... LEV_NEWS_ITEM+20:
    if (size < 28) { return -2; }
    if (!UG_MODE) { return -1; }
    if (ug_mode == 0 && E->type ==  LEV_NEWS_ITEM+20) { return -1; }
    process_news_item ((struct lev_news_item *) E);
    return 28;
  case LEV_NEWS_COMMENT+20 ... LEV_NEWS_COMMENT+24:
    if (size < 24) { return -2; }
    if (!COMM_MODE) { return -1; }
    process_news_comment ((struct lev_news_comment *) E);
    return 24;
  case LEV_NEWS_PLACEDEL+1 ... LEV_NEWS_PLACEDEL+24:
    if (size < 12) { return -2; }
    if (!COMM_MODE && !NOTIFY_MODE) { return -1; }
    if (!check_obj (E->type & 0xff, E->a, E->b)) { return -1;}
    delete_place (E->type & 0xff, E->a, E->b);
    return 12;
  case LEV_NEWS_HIDEITEM+1 ... LEV_NEWS_HIDEITEM+24:
    if (size < 16) { return -2; }
    if (!COMM_MODE && !NOTIFY_MODE) { return -1; }
    if (!check_obj (E->type & 0xff, E->a, E->b)) { return -1;}
    if (COMM_MODE) {
      show_hide_comment (E->type & 0xff, E->a, E->b, E->c, 0);
    } else {
      show_hide_notify (E->type & 0xff, E->a, E->b, E->c, 0);
    }
    return 16;
  case LEV_NEWS_SHOWITEM+1 ... LEV_NEWS_SHOWITEM+24:
    if (size < 16) { return -2; }
    if (!COMM_MODE && !NOTIFY_MODE) { return -1; }
    if (!check_obj (E->type & 0xff, E->a, E->b)) { return -1;}
    if (COMM_MODE) {
      show_hide_comment (E->type & 0xff, E->a, E->b, E->c, -1);
    } else {
      show_hide_notify (E->type & 0xff, E->a, E->b, E->c, -1);
    }
    return 16;
  case LEV_NEWS_HIDEUSERITEM+0 ... LEV_NEWS_HIDEUSERITEM+31:
    if (size < 20) { return -2; }
    if (!NOTIFY_MODE) { return -1; }
    if (!check_obj (E->type & 0xff, E->a, E->b)) { return -1;}
    show_hide_user_notify (E->d, E->type & 0xff, E->a, E->b, E->c, 0);
    return 20;
  case LEV_NEWS_SHOWUSERITEM+0 ... LEV_NEWS_SHOWUSERITEM+31:
    if (size < 20) { return -2; }
    if (!NOTIFY_MODE) { return -1; }
    if (!check_obj (E->type & 0xff, E->a, E->b)) { return -1;}
    show_hide_user_notify (E->d, E->type & 0xff, E->a, E->b, E->c, -1);
    return 20;
  case LEV_NEWS_NOTIFY+0 ... LEV_NEWS_NOTIFY+31:
    if (size < (int) sizeof (struct lev_news_notify)) { return -2; }
    if (!NOTIFY_MODE) { return -1; }
    process_news_notify ((struct lev_news_notify *) E);
    return sizeof (struct lev_news_notify);
  case LEV_BOOKMARK_INSERT + 20 ... LEV_BOOKMARK_INSERT + 24:
    if (size < (int) sizeof (struct lev_bookmark_insert)) { return -2; }
    if (!COMM_MODE) { return -1; }
    if (log_cur_pos () >= last_log_pos) {
      add_del_bookmark (E->a, E->type & 0xff, E->b, E->c, 1);
    }
    if (dyn_free_bytes() < 1024 + MAX_NEW_BOOKMARK_USERS * sizeof (struct bookmark) ||
        new_users_number > (MAX_NEW_BOOKMARK_USERS >> 1)) {
      save_index (0);
      exit (13);
    }
    return sizeof (struct lev_bookmark_insert);
  case LEV_BOOKMARK_DELETE + 20 ... LEV_BOOKMARK_DELETE + 24:
    if (size < (int) sizeof (struct lev_bookmark_insert)) { return -2; }
    if (!COMM_MODE) { return -1; }
    if (log_cur_pos () >= last_log_pos) {
      add_del_bookmark (E->a, E->type & 0xff, E->b, E->c, 0);
    }
    if (dyn_free_bytes() < 1024 + MAX_NEW_BOOKMARK_USERS * sizeof (struct bookmark) ||
        new_users_number > (MAX_NEW_BOOKMARK_USERS >> 1)) {
      save_index (0);
      exit (13);
    }
    return sizeof (struct lev_bookmark_insert);
  case LEV_NEWS_RECOMMEND+0 ... LEV_NEWS_RECOMMEND+31:
    if (size < (int) sizeof (struct lev_news_recommend)) { return -2; }
    if (!RECOMMEND_MODE) { return -1; }
    process_news_recommend ((struct lev_news_recommend *) E);
    return sizeof (struct lev_news_recommend);
  case LEV_NEWS_SET_RECOMMEND_RATE+0 ... LEV_NEWS_SET_RECOMMEND_RATE+31:
    if (size < (int) sizeof (struct lev_news_set_recommend_rate)) { return -2; }
    if (!RECOMMEND_MODE) { return -1; }
    set_recommend_rate ((struct lev_news_set_recommend_rate *) E);
    return sizeof (struct lev_news_set_recommend_rate);
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;

}

/* adding new log entries */

int do_delete_user (int user_id) {
  if (conv_uid (user_id) < 0) {
    return -1;
  }
  alloc_log_event (LEV_NEWS_USERDEL, 8, user_id);
  return delete_user (user_id);
}

int do_set_privacy (int user_id, int mask) {
  if (conv_uid (user_id) < 0) {
    return -1;
  }
  struct lev_privacy *E = alloc_log_event (LEV_NEWS_PRIVACY, 12, user_id);
  E->privacy = mask | 1;
  return set_privacy (user_id, mask);
}


int do_process_news_item (int user_id, int type, int user, int group, int owner, int place, int item) {
  if (conv_uid (user_id) < 0 || !UG_MODE || !valid_type (type)) {
    return -1;
  }

  struct lev_news_item *E = alloc_log_event (LEV_NEWS_ITEM + type, 28, user_id);
  E->user = user;
  E->group = group;
  E->owner = owner;
  E->place = place;
  E->item = item;

  vkprintf (2, "created news item type %08x, user_id=%d\n", E->type, E->user_id);

  return process_news_item (E);
}


int do_delete_place (int type, int owner, int place) {
  if (!check_obj (type, owner, place)) {
    return -1;
  }

  struct lev_news_place_delete *E = alloc_log_event (LEV_NEWS_PLACEDEL + type, 12, owner);
  E->place = place;

  return delete_place (type, owner, place);
}

int do_delete_comment (int type, int owner, int place, int item) {
  if (!check_obj (type, owner, place)) {
    return -1;
  }
  struct lev_news_comment_hide *E = alloc_log_event (LEV_NEWS_HIDEITEM + type, 16, owner);
  E->place = place;
  E->item = item;

  if (COMM_MODE) {
    return show_hide_comment (type, owner, place, item, 0);
  } else {
    return show_hide_notify (type, owner, place, item, 0);
  }
}

int do_undelete_comment (int type, int owner, int place, int item) {
  if (!check_obj (type, owner, place)) {
    return -1;
  }
  struct lev_news_comment_hide *E = alloc_log_event (LEV_NEWS_SHOWITEM + type, 16, owner);
  E->place = place;
  E->item = item;

  if (COMM_MODE) {
    return show_hide_comment (type, owner, place, item, -1);
  } else {
    return show_hide_notify (type, owner, place, item, -1);
  }
}

int do_delete_user_comment (int user_id, int type, int owner, int place, int item) {
  if (!NOTIFY_MODE || !check_obj (type, owner, place)) {
    return -1;
  }
  struct lev_news_user_comment_hide *E = alloc_log_event (LEV_NEWS_HIDEUSERITEM + type, 20, owner);
  E->place = place;
  E->item = item;
  E->user_id = user_id;

  return show_hide_user_notify (user_id, type, owner, place, item, 0);
}

int do_undelete_user_comment (int user_id, int type, int owner, int place, int item) {
  if (!NOTIFY_MODE || !check_obj (type, owner, place)) {
    return -1;
  }
  struct lev_news_user_comment_hide *E = alloc_log_event (LEV_NEWS_SHOWUSERITEM + type, 20, owner);
  E->place = place;
  E->item = item;
  E->user_id = user_id;

  return show_hide_user_notify (user_id, type, owner, place, item, -1);
}

int do_process_news_comment (int type, int user, int group, int owner, int place, int item) {
  if (!COMM_MODE || !check_obj (type, owner, place)) {
    return -1;
  }

  struct lev_news_comment *E = alloc_log_event (LEV_NEWS_COMMENT + type, 24, user);
  E->group = group;
  E->owner = owner;
  E->place = place;
  E->item = item;

  vkprintf (2, "created news comment type %08x, place_id=%d\n", E->type, E->place);

  return process_news_comment (E);
}


int do_process_news_notify (int user_id, int type, int user, int owner, int place, int item) {
  if (conv_uid (user_id) < 0 || !NOTIFY_MODE || !valid_type (type)) {
    return -1;
  }

  struct lev_news_notify *E = alloc_log_event (LEV_NEWS_NOTIFY + type, sizeof (struct lev_news_notify), user_id);
  E->user = user;
  E->owner = owner;
  E->place = place;
  E->item = item;

  vkprintf (2, "created news item type %08x, user_id=%d\n", E->type, E->user_id);

  return process_news_notify (E);
}
/* access data */


int R[MAX_RES], *R_end;

int get_privacy_mask (int user_id) {
  user_t *U = get_user (user_id);
  if (!U) {
    return conv_uid (user_id) < 0 ? -1 : 1;
  }
  return U->priv_mask;
}

void clear_result_buffer (void) {
  R_end = R;
}

int prepare_raw_updates (int user_id, int mask, int start_time, int end_time) {
  user_t *U = get_user (user_id);
  if (!U) {
    return conv_uid (user_id) < 0 ? -1 : 0;
  }

  if (end_time <= 0) {
    end_time = 0x7fffffff;
  }

  mask &= ~U->priv_mask;
  if (!mask || start_time >= end_time) {
    return 0;
  }

  items_removed_in_prepare_updates += remove_old_items (U);

  int found = 0;
  item_t *p;
  for (p = U->first; p != (item_t *) U; p = p->next) {
    if (p->date < start_time) {
      break;
    }
    if (p->date <= end_time && ((mask >> p->type) & 1)) {
      found++;
      if (R_end <= R + MAX_RES - 9) {
        R_end[0] = user_id;
        R_end[1] = p->date;
        R_end[2] = p->random_tag;
        R_end[3] = p->type;
        R_end[4] = p->user;
        R_end[5] = p->group;
        R_end[6] = p->owner;
        R_end[7] = p->place;
        R_end[8] = p->item;
        R_end += 9;
      }
    }
  }

  return found;
}

int prepare_raw_comm_updates (int type, int owner, int place, int start_time, int end_time) {
  /*if (ug_mode <= 0 || !valid_type (type) || check_place (place) < 0) {
    fprintf (stderr, "%d %d(%d) %d(%d)\n", ug_mode, valid_type (type), type, check_place (place), place);
    return -1;
  } */

  place_t *V = get_place_f (type, owner, place, 0);
  if (!V) {
    return 0;
  }

  if (end_time <= 0) {
    end_time = 0x7fffffff;
  }

  items_removed_in_prepare_updates += remove_old_comments (V);

  int found = 0;
  comment_t *p;
  for (p = V->first; p != (comment_t *) V; p = p->next) {
    if (p->date < 0) {
      continue;
    }
    if (p->date < start_time) {
      break;
    }
    if (p->date <= end_time) {
      found++;
      if (R_end <= R + MAX_RES - 8) {
        R_end[0] = p->date;
        R_end[1] = p->random_tag;
        R_end[2] = V->type;
        R_end[3] = p->user;
        R_end[4] = p->group;
        R_end[5] = V->owner;
        R_end[6] = V->place;
        R_end[7] = p->item;
        R_end += 8;
      }
    }
  }

  return found;
}

int prepare_raw_notify_updates (int user_id, int mask, int start_time, int end_time, int extra) {
  notify_user_t *U = (notify_user_t *)get_user_f (user_id, 0);
  if (!U) {
    return conv_uid (user_id) < 0 ? -1 : 0;
  }

  if (end_time <= 0) {
    end_time = 0x7fffffff;
  }

  if (!mask || start_time >= end_time) {
    return 0;
  }

  items_removed_in_prepare_updates += remove_old_notify_items ((notify_user_t *)U);

  int found = 0;
  userplace_t *up;
  for (up = U->first; up != (userplace_t *)U; up = up->unext) if ((mask >> up->type) & 1) {
    notify_t *p;
    int y = 0, z = 0;
    for (p = up->last; p != (notify_t *) up; p = p->prev) {
      if (p->date < 0) {
        y++;
        continue;
      }
      if (p->random_tag <= 0) {
        z = y;
        y = 0;
      }
      /*if (p->date < start_time) {
        break;
      }*/
      if (p->date >= start_time && p->date <= end_time && (p->random_tag > 0 || extra)) {
        found++;
        if (R_end <= R + MAX_RES - 9) {
          R_end[0] = user_id;
          R_end[1] = p->date;
          if (p->random_tag > 0) {
            R_end[2] = p->random_tag;
          } else {
            R_end[2] = p->random_tag + z;
            assert (R_end[2] <= 0);
          }
          R_end[3] = up->type;
          R_end[4] = p->user;
          R_end[5] = up->owner;
          R_end[6] = up->place;
          R_end[7] = p->item;
          if (R_end[2] != 0) {
            R_end += 8;
          }
        }
      }
    }
  }
  return found;
}

int garbage_uid = 0;

static int remove_old_recommend_items (recommend_user_t *U);

static inline int collect_garbage_items (int steps) {
  assert (UG_MODE || NOTIFY_MODE || RECOMMEND_MODE);
  int old_items_kept = items_kept;
  int i = garbage_uid, seek_steps = steps * 10;
  do {
    assert (i >= 0 && i < MAX_USERS);
    user_t *M = User[i];
    if (M) {
      int t;
      if (UG_MODE) {
        remove_old_items (M);
        t = M->tot_items;
      } else if (NOTIFY_MODE) {
        remove_old_notify_items ((notify_user_t *) M);
        t = ((notify_user_t *) M)->total_items;
      } else {
        assert (RECOMMEND_MODE);
        remove_old_recommend_items ((recommend_user_t *) M);
        t = ((recommend_user_t *) M)->total_items;
      }
      if (!t && RECOMMEND_MODE) {
        garbage_users_collected++;
        delete_user_by_idx (i);
      }
      steps--;
    }
    i += 239;
    if (i >= MAX_USERS) {
      i -= MAX_USERS;
    }
  } while (i != garbage_uid && steps > 0 && --seek_steps > 0);
  garbage_uid = i;
  garbage_objects_collected += old_items_kept - items_kept;
  return 1;
}

static inline int collect_garbage_comments (int steps) {
  assert (COMM_MODE);
  const int old_comments_kept = comments_kept;
  int i = garbage_uid, max_uid = PLACES_HASH, seek_steps = steps * 10;
  do {
    place_t *M = Place[i];
    while (M) {
      remove_old_comments (M);
      steps--;
      M = M->hnext;
    }
    i++;
    if (i >= max_uid) {
      i -= max_uid;
    }
  } while (i != garbage_uid && steps > 0 && --seek_steps > 0);
  garbage_uid = i;
  garbage_objects_collected += old_comments_kept - comments_kept;
  return 1;
}

int news_collect_garbage (int steps) {
  return (COMM_MODE) ? collect_garbage_comments (steps) : collect_garbage_items (steps);
}

int newidx_fd, idx_fd;
long long days_log_offset[100000];
unsigned days_log_crc32[100000];
int days_log_ts[100000];
int last_day, next_day_start;

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
int next_check_day_start;
int next_check_day;
int check_index_mode;
int regenerate_index_mode;

void update_offsets (int writing_binlog) {
  if (now >= next_day_start) {
    int day = now / 86400;
    int log_ts = log_last_ts;
    if (writing_binlog) {
      relax_write_log_crc32 ();
    } else {
      relax_log_crc32 (0);
    }
    int log_crc32 = ~log_crc32_complement;
    long long log_pos = log_crc32_pos;
    int i;
    for (i = last_day + 1; i <= day; i++) {
      days_log_offset [i] = log_pos;
      days_log_crc32[i] = log_crc32;
      days_log_ts[i] = log_ts;
    }
    last_day = day;
    next_day_start = (last_day + 1) * 86400;
  } else if (check_index_mode) {
    //fprintf (stderr, "%d %d\n", now, next_check_day_start);
    while (days_log_offset[next_check_day] && log_cur_pos () >= days_log_offset[next_check_day]) {
      assert (log_cur_pos () == days_log_offset[next_check_day]);
      relax_log_crc32 (0);
      assert (~log_crc32_complement == days_log_crc32[next_check_day]);
      next_check_day ++;
      next_check_day_start = next_check_day * 86400;
    }
  }
}


long long index_large_data_offset;
void save_bookmarks (index_header *header);
void load_bookmarks (index_header *header);
void load_recommend_rate_tbl (index_header *header);
void save_recommend_rate_tbl (index_header *header);
void load_privacy (index_header *header);
void save_privacy (index_header *header);

int load_index (kfs_file_handle_t Index) {
  if (Index == NULL) {
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    return 0;
  }
  idx_fd = Index->fd;
  index_header header;
  read (idx_fd, &header, sizeof (index_header));
  if (header.magic != NEWS_INDEX_MAGIC) {
    fprintf (stderr, "index file is not for news-engine\n");
    return -1;
  }
  ug_mode = header.ug_mode;
  allowed_types_mask = news_get_allowed_types_mask (ug_mode);
  if (header.allowed_types_mask != allowed_types_mask) {
    kprintf ("%s: WARNING. header allowed_types_mask (0x%08x), expected allowed_types_mask for ug_mode (%d) is 0x%08x. Skipping header value.\n",
      __func__, header.allowed_types_mask, ug_mode, allowed_types_mask);
  }
  log_split_min = header.log_split_min;
  log_split_max = header.log_split_max;
  log_split_mod = header.log_split_mod;
  last_log_pos = header.log_pos1;

  read (idx_fd, days_log_offset, sizeof (days_log_offset));
  read (idx_fd, days_log_crc32, sizeof (days_log_crc32));
  read (idx_fd, days_log_ts, sizeof (days_log_ts));
  int cur_day = time (NULL) / 86400;
  cur_day -= (max_news_days + 1);
  if (check_index_mode) {
    next_check_day = cur_day + 1;
    next_check_day_start = next_check_day * 86400;
  }
  if (regenerate_index_mode) {
    last_day = cur_day;
  } else {
    last_day = header.last_day;
  }
  //fprintf (stderr, "%d %d %d %d\n", regenerate_index_mode, check_index_mode, last_day, cur_day);
  next_day_start = (last_day + 1) * 86400;
  assert (cur_day >= 0);
  vkprintf (1, "cur_day = %d\n", cur_day);
  if (!index_mode || regenerate_index_mode || check_index_mode) {
    int t = cur_day;
    while (t >= 0 && !days_log_offset[t]) {
      t --;
    }
    if (t < 0) { t = cur_day; }
    jump_log_pos = days_log_offset[t];
    jump_log_crc32 = days_log_crc32[t];
    jump_log_ts = days_log_ts[t];
  } else {
    jump_log_pos = header.log_pos1;
    jump_log_crc32 = header.log_pos1_crc32;
    jump_log_ts = header.log_timestamp;
  }

  if (verbosity >= 4) {
    int i;
    for (i = cur_day; i < header.last_day; i++) {
      vkprintf (4, "offsets: %d %lld %u %d\n", i, days_log_offset[i], days_log_crc32[i], days_log_ts[i]);
    }
  }

  replay_logevent = news_replay_logevent;
  if (COMM_MODE) {
    load_bookmarks (&header);
  }
  if (RECOMMEND_MODE) {
    load_recommend_rate_tbl (&header);
  }

  if (UG_MODE) {
    load_privacy (&header);
  }

  index_exists = 1;
  return 0;
}

long long cur_offset;


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

  header.magic = 0;
  header.created_at = time (NULL);
  header.log_pos1 = log_cur_pos ();
  header.log_timestamp = log_last_ts;
  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }
  header.log_pos1_crc32 = ~log_crc32_complement;
  header.ug_mode = ug_mode;
  header.allowed_types_mask = allowed_types_mask;
  header.log_split_min = log_split_min;
  header.log_split_max = log_split_max;
  header.log_split_mod = log_split_mod;
  header.last_day = last_day;
  assert (write (newidx_fd, &header, sizeof (header)) == sizeof (header));
  assert (write (newidx_fd, days_log_offset, sizeof (days_log_offset)) == sizeof (days_log_offset));
  assert (write (newidx_fd, days_log_crc32, sizeof (days_log_crc32)) == sizeof (days_log_crc32));
  assert (write (newidx_fd, days_log_ts, sizeof (days_log_ts)) == sizeof (days_log_ts));

  if (COMM_MODE) {
    save_bookmarks (&header);
  }
  if (RECOMMEND_MODE) {
    save_recommend_rate_tbl (&header);
  }
  if (UG_MODE) {
    save_privacy (&header);
  }
  header.magic = NEWS_INDEX_MAGIC;
  header.small_data_offset += sizeof (header) + sizeof (days_log_offset) + sizeof (days_log_crc32) + sizeof (days_log_ts);
  header.large_data_offset += sizeof (header) + sizeof (days_log_offset) + sizeof (days_log_crc32) + sizeof (days_log_ts);
  lseek (newidx_fd, 0, SEEK_SET);
  assert (write (newidx_fd, &header, sizeof (header)) == sizeof (header));
  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);

  if (verbosity >= 3) {
    fprintf (stderr, "index written ok\n");
  }

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);

  return 0;
}

/****
 *
 * Buffered write
 *
 ****/

#define BUFFSIZE (1L << 22)
char buff[BUFFSIZE], *wptr = buff;

void clearin (void) {
  wptr = buff;
}

void flushout (void) {
  write (newidx_fd, buff, wptr - buff);
  if (verbosity > 0) {
    fprintf (stderr, "written %ld bytes\n", (long)(wptr - buff));
  }
  clearin ();
}

void writeint (int x) {
  if (wptr + sizeof (int) > buff + BUFFSIZE) {
    flushout ();
  }
  *(int *)wptr = x;
  wptr += sizeof (int);
}

void writeout (void *x, int len) {
  while (len > 0) {
    if (wptr + len <= buff + BUFFSIZE) {
      memcpy (wptr, x, len);
      wptr += len;
      return;
    }
    int a = buff + BUFFSIZE - wptr;
    memcpy (wptr, x, a);
    wptr += a;
    x = (char *)x + a;
    len -= a;
    flushout ();
  }
}

void load_privacy (index_header *header) {
  static int x[1 << 20];
  //assert (header->small_data_offset >= sizeof (*header));
  //assert (lseek (idx_fd, header->small_data_offset, SEEK_SET) == header->small_data_offset);
  while (1) {
    int l = read (idx_fd, x, 1 << 20);
    if (!l) {
      break;
    }
    assert ((l & 7) == 0);
    int i;
    for (i = 0; i < (l >> 3); i++) {
      if (verbosity >= 3) {
        fprintf (stderr, "uid = %d, priv_mask = %d\n", x[2 * i], x[2 * i + 1]);
      }
      user_t *U = get_user_f (x[2 * i], 1);
      U->priv_mask = x[2 * i + 1];
      assert (U->priv_mask & 1);
    }
  }
}

void save_privacy (index_header *header) {
  clearin ();
  int i;
  for (i = 0; i < MAX_USERS; i++) if (User[i] && User[i]->priv_mask != 1) {
    writeint (User[i]->user_id * log_split_mod + log_split_min);
    writeint (User[i]->priv_mask);
    assert (User[i]->priv_mask & 1);
  }
  flushout ();
}

/****
 *
 * Bookmarks
 *
 ****/


struct bookmark *bookmarks = 0;
int bookmarks_size = 0;
int bookmarks_ptr = 0;
int bookmarks_min_size = 1000000;
double bookmarks_coef = 1.5;

#if BOOKMARKS_MALLOC
int next_bookmark_ptr (void) {
  if (bookmarks_ptr < bookmarks_size) {
    return bookmarks_ptr++;
  }
  if (!bookmarks_size) {
    bookmarks_size = bookmarks_min_size;
    bookmarks = malloc (sizeof (struct bookmark) * bookmarks_min_size);
    assert (bookmarks);
    return bookmarks_ptr++;
  } else {
    bookmarks_size = (int) (bookmarks_coef * bookmarks_size);
    bookmarks = realloc (bookmarks, sizeof (struct bookmark) * bookmarks_size);
    assert (bookmarks);
    return bookmarks_ptr ++;
  }
}
#else
int next_bookmark_ptr (void) {
  if (!bookmarks) {
    assert (!bookmarks_ptr);
    bookmarks = ztmalloc (4);
    assert (bookmarks);
  }
  struct bookmark *next_bookmark = ztmalloc (sizeof (struct bookmark));
  assert (next_bookmark == bookmarks + bookmarks_ptr - 1);
  return --bookmarks_ptr;
}
#endif


inline int pack_type (int type) {
	return type - 20;
}

inline int unpack_type (int type) {
	return type + 20;
}

inline int mydiv (int x) {
  if (x > 0) {
    //assert (x % log_split_mod == log_split_min);
    return x / log_split_mod + 1;
  } else {
    //assert ((-x) % log_split_mod == log_split_min);
    return x / log_split_mod - 1;
  }
}

inline int mymul (int x) {
  if (x > 0) {
    return (x - 1) * log_split_mod + log_split_min;
  } else {
    return (x + 1) * log_split_mod - log_split_min;
  }
}

#define myabs(x) ((x) > 0 ? (x) : -(x))

#define PACK_BOOKMARK(a,type,owner,place) \
if (myabs(owner) % log_split_mod == log_split_min) { \
	a = (((unsigned long long)(8 + pack_type(type))) << 60) | ((((unsigned)(mydiv(owner)) & 0x0fffffff) * 1ull) << 32) | (unsigned)place; \
} else { \
	a = (((unsigned long long)(0 + pack_type(type))) << 60) | ((((unsigned)owner) * 1ull) << 28) | ((unsigned)mydiv(place) & 0x0fffffff); \
}


#define UNPACK_BOOKMARK(a,type,owner,place) \
type = (((unsigned long long)a) >> 60); \
if (type >= 8) { \
	type = unpack_type (type - 8); \
	owner = ((((unsigned long long)a) >> 32) & 0x0fffffff); \
  if (owner & 0x08000000) { \
    owner |= 0xf0000000; \
  } \
  owner = mymul (owner); \
	place = (((unsigned long long)a) & 0xffffffff); \
} else { \
	type  = unpack_type (type); \
	owner = ((((unsigned long long)a) >> 28) & 0xffffffff); \
	place = (((unsigned long long)a) & 0x0fffffff); \
  if (place & 0x08000000) { \
    place |= 0xf0000000; \
  } \
  place = mymul (place); \
}

struct bookmark_user *small_users;
int small_users_number;
long long *small_bookmarks;

struct bookmark_user *large_users;
int large_users_number;
struct metafile *large_metafiles;


int metafiles_load_errors;
int metafiles_loaded;
int metafiles_load_success;
int metafiles_unloaded;
long long metafiles_cache_miss;
long long metafiles_cache_ok;
long long metafiles_cache_loading;
long long tot_aio_loaded_bytes;
long long allocated_metafiles_size;
long long max_allocated_metafiles_size = DEFAULT_MAX_ALLOCATED_METAFILES_SIZE;

double new_users_perc (void) {
  return 1.0 * new_users_number / MAX_NEW_BOOKMARK_USERS;
}

static inline void *metafile_alloc (long size) {
  void *ptr = (size >= METAFILE_MALLOC_THRESHOLD ? malloc (size) : zmalloc (size));
  if (ptr) {
    allocated_metafiles_size += size;
  }
  return ptr;
}

static inline void metafile_free (void *ptr, long size) {
  allocated_metafiles_size -= size;
  if (size >= METAFILE_MALLOC_THRESHOLD) {
    free (ptr);
  } else {
    zfree (ptr, size);
  }
}

void del_use (int pos) {
  large_metafiles[large_metafiles[pos].next].prev = large_metafiles[pos].prev;
  large_metafiles[large_metafiles[pos].prev].next = large_metafiles[pos].next;
  large_metafiles[pos].next = -1;
  large_metafiles[pos].prev = -1;
}

void add_use (int pos) {
  large_metafiles[pos].next = large_users_number;
  large_metafiles[pos].prev = large_metafiles[large_users_number].prev;
  large_metafiles[large_metafiles[pos].next].prev = pos;
  large_metafiles[large_metafiles[pos].prev].next = pos;
}

void renew_use (int pos) {
  del_use (pos);
  add_use (pos);
}


int unload_metafile (int pos) {
  if (large_metafiles[pos].aio) {
    return 0;
  }
  if (!large_metafiles[pos].data) {
    assert (0);
    return 0;
  }
  del_use (pos);
  metafile_free (large_metafiles[pos].data, sizeof (long long) * (large_users[pos + 1].offset - large_users[pos].offset));
  large_metafiles[pos].data = 0;
  metafiles_loaded--;
  metafiles_unloaded++;
  return 1;
}

int free_by_LRU (void) {
  if (large_metafiles[large_users_number].next == large_users_number) {
    return 0;
  }
  return unload_metafile (large_metafiles[large_users_number].next);
}




/*struct aio_connection *WaitAio;*/
int onload_metafile (struct connection *c, int read_bytes);

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_metafile
};

conn_query_type_t aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "news-comm-aio-metafile-query",
  .wakeup = aio_query_timeout,
  .close = delete_aio_query,
  .complete = delete_aio_query
};


static void load_metafile_aio (int pos) {
  WaitAioArrClear ();
  //WaitAio = NULL;

  assert (0 <= pos && pos < large_users_number);
  assert (large_metafiles[pos].data);

  struct metafile *meta = &large_metafiles[pos];
  int user_id = large_users[pos].user_id;
  long data_len = sizeof (long long) * (large_users[pos + 1].offset - large_users[pos].offset);
  long long idx_offset = index_large_data_offset + sizeof (long long) * (long long)large_users[pos].offset;

  if (verbosity >= 2) {
    fprintf (stderr, "loading metafile %d for user %d (%ld bytes at position %lld) in aio mode\n", pos, user_id, data_len, idx_offset);
  }

  if (meta->aio != NULL) {
    check_aio_completion (meta->aio);
    if (meta->aio != NULL) {
      //WaitAio = meta->aio;
      WaitAioArrAdd (meta->aio);
      return;
    }

    if (meta->data) {
      return;
    } else {
      fprintf (stderr, "Previous AIO query failed at %p, scheduling a new one\n", meta);
      while (!(meta->data = metafile_alloc (data_len))) {
        assert (free_by_LRU ());
      }
    }
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "No previous aio found for this metafile\n");
    }
  }

  if (verbosity >= 4) {
    fprintf (stderr, "AIO query creating...\n");
  }
  assert (meta->data);
  meta->aio = create_aio_read_connection (idx_fd, meta->data, idx_offset, data_len, &ct_metafile_aio, meta);
  if (verbosity >= 4) {
    fprintf (stderr, "AIO query created\n");
  }
  assert (meta->aio != NULL);
  //WaitAio = meta->aio;
  WaitAioArrAdd (meta->aio);

  return;
}

int onload_metafile (struct connection *c, int read_bytes) {
  if (verbosity >= 2) {
    fprintf (stderr, "onload_metafile(%p,%d)\n", c, read_bytes);
  }

  struct aio_connection *a = (struct aio_connection *)c;
  struct metafile *meta = (struct metafile *) a->extra;

  assert (a->basic_type == ct_aio);
  assert (meta != NULL);

  if (meta->aio != a) {
    fprintf (stderr, "assertion (meta->aio == a) will fail\n");
    fprintf (stderr, "%p != %p\n", meta->aio, a);
  }

  assert (meta->aio == a);

  int pos = meta - large_metafiles;
  assert (0 <= pos && pos < large_users_number);

  if (verbosity >= 2) {
    fprintf (stderr, "*** Read metafile %d for large user %d : read %d bytes at position %lld\n", pos, large_users[pos].user_id, read_bytes, index_large_data_offset + sizeof (long long) * (long long)large_users[pos].offset);
  }

  if (read_bytes != sizeof (long long) * (large_users[pos + 1].offset - large_users[pos].offset)) {
    if (verbosity >= 0) {
      fprintf (stderr, "ERROR reading metafile %d for large user %d at position %lld: read %d bytes out of %ld: %m\n", pos, large_users[pos].user_id, index_large_data_offset + sizeof (long long) * (long long)large_users[pos].offset, read_bytes, (long)sizeof (long long) * (large_users[pos + 1].offset - large_users[pos].offset));
    }
    meta->aio = NULL;
    metafile_free (meta->data, sizeof (long long) * (large_users[pos + 1].offset - large_users[pos].offset));
    meta->data = 0;
    metafiles_load_errors ++;
  } else {
    meta->aio = NULL;
    metafiles_loaded ++;
    add_use (pos);
    metafiles_load_success ++;
    tot_aio_loaded_bytes += read_bytes;
  }
  return 1;
}

/* use_aio = -1 means "don't use aio even if a pending aio request exists" */
int load_metafile (int pos, int use_aio) {
  assert (0 <= pos && pos < large_users_number);

  if (use_aio < 0 && large_metafiles[pos].data && large_metafiles[pos].aio) {
    if (verbosity >= 0) {
      fprintf (stderr, "forced re-loading of pending aio query for large metafile %d without aio\n", pos);
    }
    // --- MEMORY LEAK for case if aio is really active ---
    // we don't free buffer so that aio might finish loading data, then it will fail in onload_metafile(), but no data corruption will occur
    // metafile_free (large_metafiles[pos].data, sizeof (long long) * (large_users[pos + 1].offset - large_users[pos].offset));
    large_metafiles[pos].data = 0;
    large_metafiles[pos].aio = 0;
  }

  if (large_metafiles[pos].data) {
    if (large_metafiles[pos].aio) {
      load_metafile_aio (pos);
      metafiles_cache_loading ++;
      if (!large_metafiles[pos].aio) {
        return 1;
      } else {
        return -2;
      }
    }
    if (use_aio > 0) {
      metafiles_cache_ok ++;
    }
    return 1;
  }

  assert (!large_metafiles[pos].aio);

  long long len = sizeof (long long) * (large_users[pos + 1].offset - large_users[pos].offset);
  while (allocated_metafiles_size + len > max_allocated_metafiles_size && free_by_LRU ());
  while (!(large_metafiles[pos].data = metafile_alloc (len))) {
    assert (free_by_LRU ());
  }

  if (use_aio <= 0) {
    //fprintf (stderr, "%lld - %lu\n", index_large_data_offset + sizeof (long long) * (long long)large_users[pos].offset, sizeof (long long) * (large_users[pos + 1].offset - large_users[pos].offset));
    lseek (idx_fd, index_large_data_offset + sizeof (long long) * (long long)large_users[pos].offset, SEEK_SET);
    assert ((long long)(read (idx_fd, large_metafiles[pos].data, len)) == len);
    add_use (pos);
    metafiles_loaded ++;
    return 1;
  } else {
    load_metafile_aio (pos);
    metafiles_cache_miss ++;
    return -2;
  }
  return 0;
}

struct user_iterator {
  int new_pos;
  int small_pos;
  int large_pos;
  int value;
};

struct user_iterator user_iterator;

void init_user_iterator (void) {
  user_iterator.new_pos = 0;
  user_iterator.small_pos = 0;
  user_iterator.large_pos = 0;
}

#define MAXINT 0x7fffffff

inline int min (int a, int b) {
  return (a < b) ? (a) : (b);
}

int advance_user_iterator (void) {
  int new_value = user_iterator.new_pos < new_users_number ? new_users[user_iterator.new_pos].user_id : MAXINT;
  int small_value = user_iterator.small_pos < small_users_number ? small_users[user_iterator.small_pos].user_id : MAXINT;
  int large_value = user_iterator.large_pos < large_users_number ? large_users[user_iterator.large_pos].user_id : MAXINT;
  int mi = min (new_value, min (small_value, large_value));
  if (mi == MAXINT) {
    return -1;
  }
  user_iterator.value = mi;
  if (new_value == mi) {
    user_iterator.new_pos++;
  }
  if (small_value == mi) {
    user_iterator.small_pos++;
  }
  if (large_value == mi) {
    user_iterator.large_pos++;
  }
  return 1;
}



#define ITERATOR_TYPE_LARGE_USERS 0
#define ITERATOR_TYPE_SMALL_USERS 1

struct iterator {
  int type;
  int new_pos;
  long long *old_pos;
  long long *last_old_pos;
  long long value;
};

struct iterator iterator;

int binary_search (int v, struct bookmark_user *users, int num) {
  int l = -1;
  int r = num;
  while (r - l > 1) {
    int x = (l + r) >> 1;
    assert (x >= 0);
    if (users[x].user_id <= v) {
      l = x;
    } else {
      r = x;
    }
  }
  if (l >= 0 && users[l].user_id == v) {
    return l;
  } else {
    return -1;
  }
}

int init_iterator (int user_id, int use_aio) {
  if (index_exists) {
    int t_old = binary_search (user_id, small_users, small_users_number);
    if (t_old < 0) {
      t_old = binary_search (user_id, large_users, large_users_number);
      if (t_old >= 0) {
        int r = load_metafile (t_old, use_aio);
        if (r < 0) {
          return r;
        }
        renew_use (t_old);
        iterator.type = ITERATOR_TYPE_LARGE_USERS;
      }
    } else {
      iterator.type = ITERATOR_TYPE_SMALL_USERS;
    }
    if (t_old < 0) {
      iterator.old_pos = 0;
      iterator.last_old_pos = 0;
    } else if (iterator.type == ITERATOR_TYPE_SMALL_USERS) {
      iterator.old_pos = small_bookmarks + small_users[t_old].offset;
      iterator.last_old_pos = small_bookmarks + small_users[t_old + 1].offset;
    } else {
      assert (iterator.type == ITERATOR_TYPE_LARGE_USERS);
      iterator.old_pos = (long long *)large_metafiles[t_old].data;
      iterator.last_old_pos = ((long long *)large_metafiles[t_old].data) + (large_users[t_old + 1].offset - large_users[t_old].offset);
    }
  } else {
    iterator.type = ITERATOR_TYPE_LARGE_USERS;
    iterator.old_pos = iterator.last_old_pos = 0;
  }
  int t_new = binary_search (user_id, new_users, new_users_number);
  if (t_new < 0) {
    iterator.new_pos = NIL_BOOKMARK;
  } else {
    iterator.new_pos = bookmarks[new_users[t_new].offset].next;
  }
  return 0;
}

int advance_iterator (void) {
  #define new_end (iterator.new_pos == NIL_BOOKMARK)
  #define old_end (iterator.old_pos == iterator.last_old_pos)
  #define new_value (bookmarks[iterator.new_pos].value)
  #define old_value (*iterator.old_pos)
  #define new_adv (iterator.new_pos = bookmarks[iterator.new_pos].next)
  #define old_adv (iterator.old_pos ++)
  #define new_y (bookmarks[iterator.new_pos].y)
  while (1) {
    if (new_end && old_end) {
      return -1;
    } else {
      if (new_end || (!old_end && old_value < new_value)) {
        iterator.value = old_value;
        old_adv;
        return 0;
      } else if (old_end || (!new_end && new_value < old_value)) {
        if (new_y > 0) {
          iterator.value = new_value;
          new_adv;
          return 0;
        } else {
          new_adv;
        }
      } else {
        if (new_y > 0) {
          iterator.value = new_value;
          old_adv;
          new_adv;
          return 0;
        } else {
          old_adv;
          new_adv;
        }
      }
    }
  }
  #undef new_end
  #undef old_end
  #undef new_value
  #undef old_value
  #undef new_adv
  #undef old_adv
  #undef new_y
}



int get_bookmarks_num (int user_id, int use_aio) {
  int res = init_iterator (user_id, use_aio);
  if (res < 0) {
    return res;
  }
  res = 0;
  while (advance_iterator () >= 0) {
    res ++;
  }
  return res;
}

#define	ANS_BUFF_SIZE	1048576
#define	ANS_BUFF_SIZE_SMALLER	(ANS_BUFF_SIZE >> 1)

long long ans_buff[ANS_BUFF_SIZE];

int get_bookmarks_packed (int user_id, long long *Q, int max_res) {
  int res = init_iterator (user_id, -1);
  if (res < 0) {
    return res;
  }
  res = 0;
  while (res < max_res && advance_iterator () >= 0) {
    Q[res++] = iterator.value;
  }
  return res;
}

static inline int cmp_bookmark (struct bookmark *x, struct bookmark *y) {
  if (x->next < y->next) {
    return -1;
  } else if (x->next > y->next) {
    return 1;
  } else if (x->value < y->value) {
    return -1;
  } else if (x->value > y->value) {
    return 1;
  } else if (x->y < y->y) {
    return -1;
  } else if (x->y > y->y) {
    return 1;
  } else {
    return 0;
  }
}

static void sort_bookmarks (struct bookmark *A, int b) {
  if (b <= 0) {
    return;
  }
  int i = 0, j = b;
  struct bookmark h = A[b >> 1], t;
  do {
    while (cmp_bookmark (A + i, &h) < 0) { i++; }
    while (cmp_bookmark (A + j, &h) > 0) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  sort_bookmarks (A + i, b - i);
  sort_bookmarks (A, j);
}

void build_bookmark_lists (void) {
  int i = bookmarks_ptr, user_id, q = 0, t;
  long long value;
  assert (!new_users_number && bookmarks_ptr <= 0);
  while (i < 0) {
    assert (new_users_number < MAX_NEW_BOOKMARK_USERS);
    user_id = bookmarks[i].next;
    if (!q) {
      t = next_bookmark_ptr ();
    } else {
      t = q;
      q = bookmarks[q].next;
    }
    new_users[new_users_number].user_id = user_id;
    new_users[new_users_number].offset = t;
    new_users_number++;
    do {
      bookmarks[t].next = i;
      bookmarks[i].y &= 1;
      t = i;
      value = bookmarks[i++].value;
      while (i < 0 && bookmarks[i].value == value && bookmarks[i].next == user_id) {
        bookmarks[i].next = q;
        q = i++;
      }
    } while (i < 0 && bookmarks[i].next == user_id);
    bookmarks[t].next = 0;
  }
}

void save_bookmarks (index_header *header) {
  clearin ();
  int small_offset = 0;
  int large_offset = 0;
  int small_users_n = 0;
  int large_users_n = 0;
  if (index_mode > ALLOW_OLD_INDEX_MODE && bookmarks_ptr) {
    assert (bookmarks_ptr < 0 && bookmarks);
    if (verbosity > 0) {
      fprintf (stderr, "sorting %d bookmarks...\n", -bookmarks_ptr);
    }
    sort_bookmarks (bookmarks + bookmarks_ptr, - bookmarks_ptr - 1);
    if (verbosity > 0) {
      fprintf (stderr, "building bookmark lists...\n");
    }
    build_bookmark_lists ();
  }
  if (verbosity > 0) {
    fprintf (stderr, "writing bookmark lists...\n");
  }
  init_user_iterator ();
  int total_users = 0;
  while (advance_user_iterator () >= 0) {
    int user_id = user_iterator.value;
    int n = get_bookmarks_num (user_id, -1);
    assert (n >= 0);
    total_users++;
    if (verbosity >= 4) {
      fprintf (stderr, "user #%d: %d, n = %d, max_small = %d, small_cnt = %d\n", total_users, user_id, n, MAX_SMALL_BOOKMARK, small_users_n);
    }
    if (n == 0) {
      continue;
    }
    assert (n > 0);
    if (n <= MAX_SMALL_BOOKMARK) {
      writeint (user_id);
      writeint (small_offset);
      small_offset += n;
      assert (small_offset >= 0);
      small_users_n ++;
    }
  }
  writeint (0);
  writeint (small_offset);
  init_user_iterator ();
  while (advance_user_iterator () >= 0) {
    int user_id = user_iterator.value;
    int n = get_bookmarks_num (user_id, -1);
    assert (n >= 0);
    if (n == 0) {
      continue;
    }
    assert (n > 0);
    if (n > MAX_SMALL_BOOKMARK) {
      if (n > ANS_BUFF_SIZE) {
        n = ANS_BUFF_SIZE_SMALLER;
      }
      writeint (user_id);
      writeint (large_offset);
      large_offset += n;
      assert (large_offset >= 0);
      large_users_n ++;
    }
  }
  writeint (0);
  writeint (large_offset);
  small_offset = 0;
  init_user_iterator ();
  while (advance_user_iterator () >= 0) {
    int user_id = user_iterator.value;
    int n = get_bookmarks_num (user_id, -1);
    assert (n >= 0);
    if (n == 0) {
      continue;
    }
    assert (n > 0);
    if (n <= MAX_SMALL_BOOKMARK) {
      assert (n == get_bookmarks_packed (user_id, ans_buff, ANS_BUFF_SIZE));
      assert (n <= ANS_BUFF_SIZE);
      small_offset += n;
      writeout (ans_buff, n * sizeof (long long));
    }
  }
  large_offset = 0;
  init_user_iterator ();
  while (advance_user_iterator () >= 0) {
    int user_id = user_iterator.value;
    int n = get_bookmarks_num (user_id, -1);
    assert (n >= 0);
    if (n == 0) {
      continue;
    }
    assert (n > 0);
    if (n > MAX_SMALL_BOOKMARK) {
      if (n > ANS_BUFF_SIZE) {
        fprintf (stderr, "user %d: has %d bookmarks, leaving only %d in index\n", user_id, n, ANS_BUFF_SIZE_SMALLER);
        n = ANS_BUFF_SIZE_SMALLER;
      }
      assert (n == get_bookmarks_packed (user_id, ans_buff, n));
      assert (n <= ANS_BUFF_SIZE);
      large_offset += n;
      writeout (ans_buff, n * sizeof (long long));
    }
  }
  flushout ();

  header->small_data_offset = (small_users_n + 1) * sizeof (struct bookmark_user) + (large_users_n + 1) * sizeof (struct bookmark_user);
  header->large_data_offset = header->small_data_offset + sizeof (long long) * small_offset;
  header->small_users = small_users_n;
  header->large_users = large_users_n;

  if (verbosity) {
    fprintf (stderr, "small_users = %d, large_users = %d\n", small_users_n, large_users_n);
  }
}

void load_bookmarks (index_header *header) {
  int i;
  assert (sizeof (struct bookmark_user) == 8);
  index_large_data_offset = header->large_data_offset;

  small_users_number = header->small_users;
  small_users = zmalloc (sizeof (struct bookmark_user) * (small_users_number + 1));
  read (idx_fd, small_users, sizeof (struct bookmark_user) * (small_users_number + 1));

  for (i = 0; i < small_users_number; i++) {
    assert (small_users[i+1].offset > small_users[i].offset);
    assert (small_users[i+1].user_id > small_users[i].user_id || i == small_users_number - 1);
  }
  assert (!small_users[0].offset);

  large_users_number = header->large_users;
  large_users = zmalloc (sizeof (struct bookmark_user) * (large_users_number + 1));
  read (idx_fd, large_users, sizeof (struct bookmark_user) * (large_users_number + 1));

  for (i = 0; i < large_users_number; i++) {
    assert (large_users[i+1].offset > large_users[i].offset);
    assert (large_users[i+1].user_id > large_users[i].user_id || i == large_users_number - 1);
  }
  assert (!large_users[0].offset);
  // TODO: check boundary values for small/large_users[0].offset and large_users[large_users_number].offset

  small_bookmarks = zmalloc (small_users[small_users_number].offset * sizeof (long long));
  read (idx_fd, small_bookmarks, small_users[small_users_number].offset * sizeof (long long));

  large_metafiles = zmalloc (sizeof (struct metafile) * (large_users_number + 1));
  if (verbosity) {
    fprintf (stderr, "small_users = %d, large_users = %d\n", small_users_number, large_users_number);
  }
  large_metafiles[large_users_number].next = large_users_number;
  large_metafiles[large_users_number].prev = large_users_number;
}

int get_bookmarks (int user_id, int mask, int *Q, int max_res) {
  int res = init_iterator (user_id, 1);
  if (res < 0) {
    return res;
  }
  res = 0;
  while (res < max_res && advance_iterator () >= 0) {
    UNPACK_BOOKMARK(iterator.value, Q[3 * res + 0], Q[3 * res + 1], Q[3 * res + 2]);
    if ((1 << Q[3 * res + 0]) & mask) {
      res ++;
    }
  }
  return res;
}


void insert_bookmark_log (int user_id, int type, int owner, int place, int y) {
  struct lev_bookmark_insert *E = alloc_log_event ((y ? LEV_BOOKMARK_INSERT : LEV_BOOKMARK_DELETE) + type, sizeof (struct lev_bookmark_insert), user_id);
  E->owner = owner;
  E->place = place;
}

int insert_new_users (int user_id) {
  int l = -1;
  int r = new_users_number;
  while (r - l > 1) {
    int x = (r + l) >> 1;
    if (new_users[x].user_id <= user_id) {
      l = x;
    } else {
      r = x;
    }
  }
  if (l >= 0 && new_users[l].user_id == user_id) {
    return l;
  }
  l++;
  assert (new_users_number < MAX_NEW_BOOKMARK_USERS);
  memmove (new_users + l + 1, new_users + l, sizeof (struct bookmark_user) * (new_users_number - l));
  new_users[l].user_id = user_id;
  new_users[l].offset = next_bookmark_ptr ();
  bookmarks[new_users[l].offset].next = NIL_BOOKMARK;
  new_users_number ++;
  return l;
}

int insert_bookmark (int user_id, long long value, int y, int write) {
  if (index_mode > ALLOW_OLD_INDEX_MODE) {
    int t = next_bookmark_ptr ();
    assert (write && (t & 0xc0000000) == 0xc0000000);	// won't work in BOOKMARK_MALLOC mode
    bookmarks[t].next = user_id;
    bookmarks[t].y = (t << 1) + (y & 1);
    bookmarks[t].value = value;
    return 1;
  }
  int t = binary_search (user_id, new_users, new_users_number);
  if (t < 0) {
    if (!write) {
      return 0;
    }
    t = insert_new_users (user_id);
    assert (t == binary_search (user_id, new_users, new_users_number));
    if (verbosity >= 4) {
      fprintf (stderr, "Inserted user %d (total %d new users)\n", user_id, new_users_number);
    }
  }
  int x = new_users[t].offset;
  while (1) {
    int x1 = bookmarks[x].next;
    if (x1 == NIL_BOOKMARK || bookmarks[x1].value > value) {
      if (!write) {
        return 0;
      }
      int t = next_bookmark_ptr ();
      bookmarks[t].next = x1;
      bookmarks[t].value = value;
      bookmarks[t].y = y;
      bookmarks[x].next = t;
      break;
    }
    if (bookmarks[x1].value == value) {
      if (!write) {
        return bookmarks[x1].y == y;
      }
      if (y != bookmarks[x1].y) {
        bookmarks[x1].y = y;
      }
      break;
    }
    x = x1;
  }
  return 1;
}

int add_del_bookmark (int user_id, int type, int owner, int place, int y) {
  if (verbosity > 2) {
    fprintf (stderr, "add_del_bookmark: %d - %d - %d\n", type, owner, place);
  }
  if (!check_obj (type, owner, place) || user_id <= 0) {
    return 0;
  }
  long long t;
  PACK_BOOKMARK (t, type, owner, place);
  insert_bookmark (user_id, t, y, 1);
  return 1;
}

int do_add_del_bookmark (int user_id, int type, int owner, int place, int y) {
  //fprintf (stderr, "%d (%d, %d) - (%d, %d) %d\n", type, owner, place, log_split_min, log_split_mod, user_id);
  if (!check_obj (type, owner, place) || user_id <= 0) {
    return 0;
  }
  long long t;
  PACK_BOOKMARK (t, type, owner, place);
  int x, yy, z;
  UNPACK_BOOKMARK (t, x, yy, z);
  //fprintf (stderr, "%x(%x) %x(%x) %x(%x) %llx\n", type, x, owner, yy, place, z, t);
  assert (x == type && yy == owner && z == place);
  if (!insert_bookmark (user_id, t, y, 0)) {
    insert_bookmark_log (user_id, type, owner, place, y);
    return add_del_bookmark (user_id, type, owner, place, y);
  }
  return 1;
}

int check_split (int n) {
  return (n > 0) ? (n % log_split_mod == log_split_min) : ((-n) % log_split_mod == log_split_min);
}

void test (int a, int b, int c) {
	int _a, _b, _c;
	long long x;
	PACK_BOOKMARK(x,a,b,c);
	UNPACK_BOOKMARK(x,_a,_b,_c);
	assert (a == _a && b == _b && c == _c);
}

struct hashset_int {
  int size;
  int filled;
  int n;
  int *h;
};

/* returns prime number which greater than 1.5n and not greater than 1.1 * 1.5 * n */
int get_hashtable_size (int n) {
  static const int p[] = {1103,1217,1361,1499,1657,1823,2011,2213,2437,2683,2953,3251,3581,3943,4339,
  4783,5273,5801,6389,7039,7753,8537,9391,10331,11369,12511,13763,15149,16673,18341,20177,22229,
  24469,26921,29629,32603,35869,39461,43411,47777,52561,57829,63617,69991,76991,84691,93169,102497,
  112757,124067,136481,150131,165161,181693,199873,219871,241861,266051,292661,321947,354143, 389561, 428531};
  /*
  471389,518533,570389,627433,690187,759223,835207,918733,1010617,1111687,1222889,1345207,
  1479733,1627723,1790501,1969567,2166529,2383219,2621551,2883733,3172123,3489347,3838283,4222117,
  4644329,5108767,5619667,6181639,6799811,7479803,8227787,9050599,9955697,10951273,12046403,13251047,
  14576161,16033799,17637203,19400929,21341053,23475161,25822679,28404989,31245491,34370053,37807061,
  41587807,45746593,50321261,55353391,60888739,66977621,73675391,81042947,89147249,98061979,107868203,
  118655027,130520531,143572609,157929907,173722907,191095213,210204763,231225257,254347801,279782593,
  307760897,338536987,372390691,409629809,450592801,495652109,545217341,599739083,659713007,725684317,
  798252779,878078057,965885863,1062474559};
  */
  const int lp = sizeof (p) / sizeof (p[0]);
  int a = -1;
  int b = lp;
  n += n >> 1;
  while (b - a > 1) {
    int c = ((a + b) >> 1);
    if (p[c] <= n) { a = c; } else { b = c; }
  }
  if (a < 0) { a++; }
  assert (a < lp-1);
  return p[a];
}

int hashset_int_init (struct hashset_int *H, int n) {
  H->filled = 0;
  H->n = n;
  if (!n) { return 0; }
  H->size = get_hashtable_size (n);
  H->h = zmalloc0 (H->size * sizeof(H->h[0]));
  return (H->h != 0);
}

int hashset_int_get (struct hashset_int *H, int id) {
  int h1, h2;
  h1 = ((unsigned int) id) % H->size;
  h2 = 1 + ((unsigned int) id) % (H->size - 1);
  while (H->h[h1] != 0) {
    if (H->h[h1] == id) {
      return 1;
    }
    h1 += h2;
    if (h1 >= H->size) { h1 -= H->size; }
  }
  return 0;
}

int hashset_int_insert (struct hashset_int *H, int id) {
  int h1, h2;
  h1 = ((unsigned int) id) % H->size;
  h2 = 1 + ((unsigned int) id) % (H->size - 1);
  while (H->h[h1] != 0) {
    if (H->h[h1] == id) {
      return 0;
    }
    h1 += h2;
    if (h1 >= H->size) { h1 -= H->size; }
  }
  H->h[h1] = id;
  H->filled++;
  return 1;
}

/******************** Recommend News ********************/
/* 11 -> 48.54518518518518 days */
#define AMORTIZATION_TABLE_SQRT_SIZE_BITS 11
#define AMORTIZATION_TABLE_SQRT_SIZE (1<<AMORTIZATION_TABLE_SQRT_SIZE_BITS)
#define AMORTIZATION_TABLE_SQRT_SIZE_MASK (AMORTIZATION_TABLE_SQRT_SIZE-1)

static double recommend_rate_tbl[32*256];
static void recommend_rate_tbl_reset (void) {
  int i;
  for (i = sizeof (recommend_rate_tbl) / sizeof (recommend_rate_tbl[0]) - 1; i >= 0; i--) {
    recommend_rate_tbl[i] = 1.0;
  }
}

static struct time_amortization_table {
  double hi_exp[AMORTIZATION_TABLE_SQRT_SIZE], lo_exp[AMORTIZATION_TABLE_SQRT_SIZE];
  double c;
  int T;
} TAT = {
  .T = -1
};

static void time_amortization_table_init (int T) {
  int i;
  if (TAT.T == T) { return; }
  TAT.c = -(M_LN2 / T);
  TAT.T = T;
  double hi_mult = exp (TAT.c * AMORTIZATION_TABLE_SQRT_SIZE);
  double lo_mult = exp (TAT.c);
  TAT.hi_exp[0] = TAT.lo_exp[0] = 1.0;
  for (i = 1; i < AMORTIZATION_TABLE_SQRT_SIZE; i++) {
    TAT.hi_exp[i] = TAT.hi_exp[i-1] * hi_mult;
    TAT.lo_exp[i] = TAT.lo_exp[i-1] * lo_mult;
  }
}

static inline double time_amortization_table_fast_exp (int dt) {
  return (dt < AMORTIZATION_TABLE_SQRT_SIZE * AMORTIZATION_TABLE_SQRT_SIZE) ?
          TAT.hi_exp[dt >> AMORTIZATION_TABLE_SQRT_SIZE_BITS] * TAT.lo_exp[dt & AMORTIZATION_TABLE_SQRT_SIZE_MASK] :
          exp (TAT.c * dt);
}

void recommend_init_raw_updates (void) {
  assert (tot_places == 0);
  assert (2 * RECOMMEND_PLACES_HASH <= PLACES_HASH);
}

static recommend_item_t *new_recommend_item (void) {
  assert (RECOMMEND_MODE);
  items_kept++;
  return zmalloc0 (sizeof (recommend_item_t));
}

static void free_recommend_item (recommend_item_t *A) {
  assert (RECOMMEND_MODE);
  items_kept--;
  zfree (A, sizeof (*A));
}

static void remove_recommend_item_from_user_list (recommend_user_t *U, recommend_item_t *I) {
  if (U->first == I) {
    U->first = I->next;
  }
  if (U->last == I) {
    U->last = I->prev;
  }
  recommend_item_t *u = I->prev, *v = I->next;
  if (u != (recommend_item_t *) U) {
    u->next = v;
  }
  if (v != (recommend_item_t *) U) {
    v->prev = u;
  }
  free_recommend_item (I);
}

static void free_recommend_user_list (recommend_user_t *U) {
  assert (RECOMMEND_MODE);
  recommend_item_t *p = U->first, *q;
  while (p != (recommend_item_t *) U) {
    q = p->next;
    U->total_items--;
    assert (U->total_items >= 0);
    free_recommend_item (p);
    p = q;
  }
  assert (!U->total_items);
  U->first = U->last = (recommend_item_t *) U;
}

static recommend_place_t *get_recommend_place_f (int type, int owner, int place, int force) {
  assert (RECOMMEND_MODE);
  unsigned h = ((unsigned) (type * 17239 + owner * 239 + place)) % RECOMMEND_PLACES_HASH;
  recommend_place_t **p = ((recommend_place_t **) Place) + h, *V;
  while (*p) {
    V = *p;
    if (V->place == place && V->owner == owner && V->type == type) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = (recommend_place_t *) Place[h];
        Place[h] = (place_t *) V;
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force <= 0 || tot_places >= MAX_GROUPS) {
    return 0;
  }
  V = zmalloc (sizeof (recommend_place_t));
  V->type = type;
  V->owner = owner;
  V->place = place;
  V->hnext = (recommend_place_t *) Place[h];
  V->weight = 0.0;
  V->last_user = 0;
  V->actions_bitset = 0;
  V->users = 0;
  Place[h] = (place_t *) V;
  tot_places++;
  return V;
}

static int remove_old_recommend_items (recommend_user_t *U) {
  assert (RECOMMEND_MODE);
  int x = U->total_items, y;
  if (!x) {
    return 0;
  }
  y = now - (max_news_days + 1) * 86400;

  recommend_item_t *p = U->last, *q;
  const int old_total_items = x;
  while (p != (recommend_item_t *) U && (x > MAX_USER_ITEMS || p->date <= y)) {
    q = p->prev;
    free_recommend_item (p);
    x--;
    p = q;
  }
  U->last = p;
  p->next = (recommend_item_t *) U;
  U->total_items = x;
  assert (U->total_items >= 0);
  return old_total_items - x;
}

int get_recommend_rate (int type, int action, double *rate) {
  if (type >= 0 && type < 32 && action >= 0 && action < 256) {
    *rate = recommend_rate_tbl[(type << 8) + action];
    return 0;
  }
  return -1;
}

static inline double recommend_item_get_rate (recommend_item_t *p) {
  double rate;
  if (!get_recommend_rate (p->type, p->action, &rate)) {
    return rate;
  }
  return 1.0;
}

static struct hashset_int mandatory_owners_hashset, forbidden_owners_hashset;

inline int recommend_check_owner (int owner) {
  if (mandatory_owners_hashset.n && !hashset_int_get (&mandatory_owners_hashset, owner)) {
    return 0;
  }
  return !hashset_int_get (&forbidden_owners_hashset, owner);
}

int recommend_process_raw_updates (int user_id, int user_rate, int mask, int start_time, int end_time, int timestamp, const int min_item_creation_time) {
  recommend_user_t *U = (recommend_user_t *) get_user (user_id);
  if (!U) {
    return conv_uid (user_id) < 0 ? -1 : 0;
  }

  if (end_time <= 0) {
    end_time = 0x7fffffff;
  }

  if (!mask || start_time >= end_time) {
    return 0;
  }

  items_removed_in_prepare_updates += remove_old_recommend_items (U);

  int sign_timestamp = 1;
  if (timestamp < 0) {
    sign_timestamp = -1;
  }

  int found = 0;
  recommend_item_t *p, *w;
  for (p = U->first; p != (recommend_item_t *) U; p = w) {
    w = p->next;
    if (p->date < start_time) {
      break;
    }
    if (p->date <= end_time && ((mask >> p->type) & 1) && sign_timestamp * p->item_creation_time > timestamp && p->item_creation_time >= min_item_creation_time && recommend_check_owner (p->owner)) {
      recommend_place_t *q = get_recommend_place_f (p->type, p->owner, p->place, 1);
      if (q != NULL) {
        if (!(p->action & 0xffffffe0)) {
          int bit = 1 << p->action;
          if (q->last_user != user_id) {
            q->last_user = user_id;
            q->actions_bitset = bit;
            q->users++;
          } else {
            if (q->actions_bitset & bit) {
              remove_recommend_item_from_user_list (U, p);
              dups_removed_in_process_raw_updates++;
              continue;
            }
            q->actions_bitset |= bit;
          }
        }
        q->item_creation_time = p->item_creation_time;
        q->weight += recommend_item_get_rate (p) * user_rate;
        found++;
      }
    }
  }
  return found;
}

static int cmp_recommend_item (const void *a, const void *b) {
  const recommend_place_t *aa = *(const recommend_place_t **) a;
  const recommend_place_t *bb = *(const recommend_place_t **) b;
  if (aa->type < bb->type) {
    return -1;
  }
  if (aa->type > bb->type) {
    return 1;
  }
  if (aa->owner < bb->owner) {
    return -1;
  }
  if (aa->owner > bb->owner) {
    return 1;
  }
  if (aa->place < bb->place) {
    return -1;
  }
  if (aa->place > bb->place) {
    return 1;
  }
  return 0;
}

int recommend_finish_raw_updates (int T) {
  int h, t = 0;
  recommend_place_t *p;
  R_end = R;
  int w = RECOMMEND_PLACES_HASH;
  for (h = 0; h < RECOMMEND_PLACES_HASH; h++) {
    if ((p = (recommend_place_t *) Place[h]) != NULL) {
      do {
        Place[w++] = (place_t *) p;
        p = p->hnext;
      } while (p != NULL);
      Place[h] = NULL;
    }
  }
  qsort (Place + RECOMMEND_PLACES_HASH, w - RECOMMEND_PLACES_HASH, sizeof (Place[0]), cmp_recommend_item);

  time_amortization_table_init (T);
  for (h = RECOMMEND_PLACES_HASH; h < w; h++) {
    p = (recommend_place_t *) Place[h];
    assert (R_end <= R + MAX_RES - 6);
    R_end[0] = p->type;
    R_end[1] = p->owner;
    R_end[2] = p->place;
    R_end[3] = p->users;
    int dt = now - p->item_creation_time;
    if (dt > 0) {
      p->weight *= time_amortization_table_fast_exp (dt);
    }
    memcpy (R_end + 4, &p->weight, 8);
    R_end += 6;
    t++;
    //zfree (p, sizeof (recommend_place_t));
    Place[h] = NULL;
  }
  assert (t == tot_places);
  tot_places = 0;
  return t;
}

static int recommend_cmp_pair1 (const void *x, const void *y) {
  const int *a = (const int *) x;
  const int *b = (const int *) y;
  if (a[0] < b[0]) { return -1; }
  if (a[0] > b[0]) { return 1; }
  if (a[1] > b[1]) { return -1; }
  if (a[1] < b[1]) { return 1; }
  return 0;
}

static int recommend_cmp_pair2 (const void *x, const void *y) {
  const int *a = (const int *) x;
  const int *b = (const int *) y;
  if (a[1] > b[1]) { return -1; }
  if (a[1] < b[1]) { return 1; }
  if (a[0] < b[0]) { return -1; }
  if (a[0] > b[0]) { return 1; }
  return 0;
}

int recommend_prepare_raw_updates (int *Q, int QL, int mask, int st_time, int end_time, int excluded_user_id, int timestamp, int T) {
  int i, mandatory_owners = 0, forbidden_owners = 1;
  dyn_mark_t mrk;
  dyn_mark (mrk);
  for (i = 0; i < QL; i++) {
    if (Q[(i<<1)+1] == -2) {
      mandatory_owners++;
    } else if (Q[(i<<1)+1] == -1) {
      forbidden_owners++;
    }
  }
  hashset_int_init (&mandatory_owners_hashset, mandatory_owners);
  hashset_int_init (&forbidden_owners_hashset, forbidden_owners);
  for (i = 0; i < QL; ) {
    if (Q[(i<<1)+1] == -2) {
      hashset_int_insert (&mandatory_owners_hashset, Q[i<<1]);
      QL--;
      if (i != QL) {
        memcpy (Q + (i<<1), Q + (QL<<1), 8);
      }
      continue;
    } else if (Q[(i<<1)+1] == -1) {
      hashset_int_insert (&forbidden_owners_hashset, Q[i<<1]);
      QL--;
      if (i != QL) {
        memcpy (Q + (i<<1), Q + (QL<<1), 8);
      }
      continue;
    }
    i++;
  }
  hashset_int_insert (&forbidden_owners_hashset, excluded_user_id);

  /* remove duplicate users */
  int m = 0;
  qsort (Q, QL, 8, recommend_cmp_pair1);
  for (i = 1; i < QL; i++) {
    if (Q[2*i] != Q[2*m]) {
      ++m;
      Q[2*m] = Q[2*i];
      Q[2*m+1] = Q[2*i+1];
    }
  }
  m++;
  dups_users_removed_from_urlist += QL - m;
  QL = m;
  /* sort in rating decreasing order */
  qsort (Q, QL, 8, recommend_cmp_pair2);

  const int min_item_creation_time = now - 30 * T;
  recommend_init_raw_updates ();
  for (i = 0; i < QL; i++) {
    recommend_process_raw_updates (Q[(i<<1)], Q[(i<<1)+1], mask, st_time, end_time, timestamp, min_item_creation_time);
  }
  int res = recommend_finish_raw_updates (T);
  dyn_release (mrk);
  return res;
}

static recommend_item_t *recommend_item_f (int user_id, int type, int owner, int place, int action) {
  recommend_user_t *U = (recommend_user_t *) get_user_f (user_id, 0);
  if (U == NULL) {
    return NULL;
  }
  int steps = RECOMMEND_FIND_ITEM_DUPS_STEPS;
  recommend_item_t *q = U->first;
  while (steps > 0 && q != (recommend_item_t *) U) {
    if (q->owner == owner && q->place == place && q->type == type && q->action == action) {
      return q;
    }
    q = q->next;
    steps--;
  }
  return NULL;
}

static int process_news_recommend (struct lev_news_recommend *E) {
  if (now < min_logevent_time) {
    return 0;
  }
  recommend_user_t *U = (recommend_user_t *) get_user_f (E->user_id, 1);
  int t = E->type & 0xff;
  if (!U || !RECOMMEND_MODE || !valid_type (t)) {
    return -1;
  }

  recommend_item_t *p, *q = U->first;

  p = new_recommend_item ();

  p->next = q;
  q->prev = p;
  p->prev = (recommend_item_t *) U;
  U->first = p;
  U->total_items++;

  p->owner = E->owner;
  p->place = E->place;
  //p->item = E->item;
  p->item_creation_time = E->item_creation_time;
  p->date = now;
  p->type = t;
  p->action = E->action;

  items_removed_in_process_new += remove_old_recommend_items (U);

  return 1;
}

int do_process_news_recommend (int user_id, int type, int owner, int place, int action, int item, int item_creation_time) {
  if (conv_uid (user_id) < 0 || !RECOMMEND_MODE || !valid_type (type)) {
    return -1;
  }
  if (recommend_item_f (user_id, type, owner, place, action) != NULL) {
    vkprintf (4, "recommend_item_f (%d, %d, %d, %d, %d) returns not NULL.\n", user_id, type, owner, place, action);
    return 0;
  }

  struct lev_news_recommend *E = alloc_log_event (LEV_NEWS_RECOMMEND + type, sizeof (*E), user_id);
  E->owner = owner;
  E->place = place;
  E->action = action;
  E->item = item;
  //E->rate = rate;
  E->item_creation_time = item_creation_time;

  return process_news_recommend (E);
}

static int set_recommend_rate (struct lev_news_set_recommend_rate *E) {
  if (E->action < 0 || E->action > 255) {
    return 0;
  }
  recommend_rate_tbl[((E->type & 31) << 8) + E->action] = E->rate;
  return 1;
}

int do_set_recommend_rate (int type, int action, double rate) {
  if (!RECOMMEND_MODE || !valid_type (type)) {
    return -1;
  }
  if (action < 0 || action > 255) {
    return -1;
  }
  if (fabs (recommend_rate_tbl[(type << 8) + action] - rate) < 1e-9) {
    return -1;
  }
  struct lev_news_set_recommend_rate *E = alloc_log_event (LEV_NEWS_SET_RECOMMEND_RATE + type, sizeof (*E), action);
  E->rate = rate;
  return set_recommend_rate (E);
}

void load_recommend_rate_tbl (index_header *header) {
  unsigned long long tbl_crc64;
  assert (read (idx_fd, recommend_rate_tbl, sizeof (recommend_rate_tbl)) == sizeof (recommend_rate_tbl));
  assert (read (idx_fd, &tbl_crc64, 8) == 8);
  assert (crc64 (recommend_rate_tbl, sizeof (recommend_rate_tbl)) == tbl_crc64);
}

void save_recommend_rate_tbl (index_header *header) {
  unsigned long long tbl_crc64 = crc64 (recommend_rate_tbl, sizeof (recommend_rate_tbl));
  assert (write (newidx_fd, recommend_rate_tbl, sizeof (recommend_rate_tbl)) == sizeof (recommend_rate_tbl));
  assert (write (newidx_fd, &tbl_crc64, 8) == 8);
}
