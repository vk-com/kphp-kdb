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
              2011-2013 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "copyexec-results-data.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "copyexec-err.h"
#include "kdb-copyexec-binlog.h"

/*********************************** host_id functions **************************************************/
#define MAX_HOSTS_PRIME 99991
#define MAX_HOSTS (8 * MAX_HOSTS_PRIME / 10)

/* handshake: we known random_tag, hostname, ip */

static host_t *RT_HOSTS[MAX_HOSTS_PRIME];
static host_t *HOSTS[MAX_HOSTS]; // host_id -> (host_t *), host_id starts from 1
static host_t *CONN_FD_HOST[MAX_CONNECTIONS];
int hosts = 0;

host_t *get_host_by_random_tag (unsigned long long random_tag, int force) {
  unsigned h1 = ((unsigned) random_tag) % MAX_HOSTS_PRIME;
  const unsigned h2 = 1 + (((unsigned) (random_tag >> 32)) % (MAX_HOSTS_PRIME - 1));
  while (RT_HOSTS[h1] != NULL) {
    if (RT_HOSTS[h1]->random_tag == random_tag) {
      return RT_HOSTS[h1];
    }
    if ((h1 += h2) >= MAX_HOSTS_PRIME) {
      h1 -= MAX_HOSTS_PRIME;
    }
  }

  if (force > 0) {
    if (hosts >= MAX_HOSTS - 2) {
      kprintf ("Too many hosts, MAX_HOST_PRIME in %s should be increased.\n", __FILE__);
      exit (1);
    }
    host_t *H = zmalloc0 (sizeof (*H));
    H->random_tag = random_tag;
    H->host_id = ++hosts;
    HOSTS[hosts] = H;
    RT_HOSTS[h1] = H;
    return H;
  }

  return NULL;
}

host_t *get_host_by_connection (struct connection *c) {
  if (c->fd < 0 || c->fd >= MAX_CONNECTIONS) {
    return NULL;
  }
  host_t *H = CONN_FD_HOST[c->fd];
  if (H == NULL) {
    vkprintf (3, "CONN_FD_HOST[%d] = NULL\n", c->fd);
    return NULL;
  }
  if (H->generation != c->generation) {
    vkprintf (3, "H->generation (%d) != c->generation (%d)\n", H->generation, c->generation);
    return CONN_FD_HOST[c->fd] = NULL;
  }
  return H;
}

int get_host_on_connect (struct connection *c, unsigned long long volume_id, unsigned long long random_tag, const char *const hostname, int pid, host_t **R) {
  if (c->fd < 0) {
    return COPYEXEC_ERR_INVAL;
  }
  const unsigned ip = c->remote_ip;
  *R = NULL;
  host_t *H = get_host_by_random_tag (random_tag, 1);
  if (H->ip == 0) {
    H->volume_id = volume_id;
    H->binlog_pos = 0;
    H->hostname = zstrdup (hostname);
    H->ip = ip;
    H->pid = pid;
    H->generation = c->generation;
    CONN_FD_HOST[c->fd] = H;
    *R = H;
    return COPYEXEC_RESULT_NEW_HOST;
  } else {
    if (H->volume_id != volume_id) {
      return COPYEXEC_ERR_VOLUME_ID_MISMATCHED;
    }
    if (strcmp (H->hostname, hostname)) {
      return COPYEXEC_ERR_HOSTNAME_MISMATCHED;
    }
    H->generation = c->generation;
    CONN_FD_HOST[c->fd] = H;
    *R = H;
    if (H->ip != ip) {
      H->ip = ip;
      H->pid = pid;
      return COPYEXEC_RESULT_IP_CHANGED;
    }
    if (H->pid != pid) {
      H->pid = pid;
      return COPYEXEC_RESULT_PID_CHANGED;
    }
    return 0;
  }
}

int do_connect (struct connection *c, unsigned long long volume_id, unsigned long long random_tag, const char *const hostname, int pid, host_t **R) {
  vkprintf (3, "do_connect (c: %p, volume_id: 0x%llx, random_tag: 0x%llx, hostname: %s, pid: %d)\n", c, volume_id, random_tag, hostname, pid);
  int r = get_host_on_connect (c, volume_id, random_tag, hostname, pid, R);
  vkprintf (4, "get_host_on_connect returns %d.\n", r);
  if (r < 0) {
    return r;
  }
  (*R)->last_action_time = now;
  if (r != 0) {
    assert ((*R) != NULL);
    int l = strlen (hostname);
    struct lev_copyexec_result_connect *E = alloc_log_event (LEV_COPYEXEC_RESULT_CONNECT, sizeof (*E) + l, 0);
    E->random_tag = random_tag;
    E->volume_id = volume_id;
    E->ip = (*R)->ip;
    E->hostname_length = l;
    memcpy (E->hostname, hostname, l);
    E->pid = pid;
  }
  return r;
}

/******************** tree functions ********************/
typedef struct tree {
  struct tree *left, *right;
  int x; /* host_id */
  int y;
  unsigned value; /* result */
} tree_t;

int alloc_tree_nodes, free_tree_nodes;
tree_t free_tree_head = {.left = &free_tree_head, .right = &free_tree_head};

static tree_t *new_tree_node (int x, int y, unsigned value) {
  tree_t *P;
  if (free_tree_nodes > 0) {
    free_tree_nodes--;
    assert (free_tree_nodes >= 0);
    P = free_tree_head.right;
    assert (P != &free_tree_head && P->left == &free_tree_head);
    P->right->left = &free_tree_head;
    free_tree_head.right = P->right;
  } else {
    P = zmalloc (sizeof (tree_t));
    assert (P);
    alloc_tree_nodes++;
  }
  P->left = P->right = NULL;
  P->x = x;
  P->y = y;
  P->value = value;
  return P;
}

static tree_t *tree_lookup (tree_t *T, int x) {
  while (T) {
    if (x < T->x) {
      T = T->left;
    } else if (x > T->x) {
      T = T->right;
    } else {
      return T;
    }
  }
  return T;
}

static void tree_split (tree_t **L, tree_t **R, tree_t *T, int x) {
  if (!T) {
    *L = *R = NULL;
    return;
  }
  if (x < T->x) {
    *R = T;
    tree_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_split (&T->right, R, T->right, x);
  }
}

static tree_t *tree_insert (tree_t *T, int x, int y, unsigned value) {
  if (!T) {
    return new_tree_node (x, y, value);
  }
  if (T->y >= y) {
    if (x < T->x) {
      T->left = tree_insert (T->left, x, y, value);
    } else {
      T->right = tree_insert (T->right, x, y, value);
    }
    return T;
  }
  tree_t *P = new_tree_node (x, y, value);
  tree_split (&P->left, &P->right, T, x);
  return P;
}

int tree_depth (tree_t *T, int d) {
  if (!T) { return d; }
  int u = tree_depth (T->left, d + 1);
  int v = tree_depth (T->right, d + 1);
  return (u > v ? u : v);
}

static void free_tree_node (tree_t *T) {
  (T->right = free_tree_head.right)->left = T;
  free_tree_head.right = T;
  T->left = &free_tree_head;
  free_tree_nodes++;
}

void free_tree (tree_t *T) {
  if (T) {
    free_tree (T->left);
    free_tree (T->right);
    free_tree_node (T);
  }
}

/************************************ status functions ***************************************************************/

static char *status_to_alpha (int tp) {
  switch (tp) {
    case ts_running:
      return "running";
    case ts_ignored:
      return "ignored";
      break;
    case ts_interrupted:
      return "interrupted";
      break;
    case ts_cancelled:
      return "cancelled";
      break;
    case ts_terminated:
      return "terminated";
      break;
    case ts_failed:
      return "failed";
      break;
    case ts_decryption_failed:
      return "decryption_failed";
      break;
    case ts_io_failed:
      return "io_failed";
    default:
      return "unknown";
  }
}

int alpha_to_status (const char *const status) {
  int i;
  for (i = 0; i < 16; i++) {
    if (!strcmp (status, status_to_alpha (i))) {
      return i;
    }
  }
  return -1;
}


/************************************ transactions functions ***************************************************************/
typedef struct transaction {
  struct transaction *prev, *next; /* LRU */
  struct transaction *hnext;
  tree_t *root; //map (host_id -> result)
  unsigned long long volume_id;
  int transaction_id;
} transaction_t;

static void transaction_free (transaction_t *T);

/************************************ LRU ******************************************************************/
int lru_size, max_lru_size=4096;
transaction_t lru_list  = {.prev = &lru_list, .next = &lru_list};

void lru_add (transaction_t *T) {
  transaction_t *u = &lru_list, *v = u->next;
  u->next = T; T->prev = u;
  v->prev = T; T->next = v;
  lru_size++;
}

void lru_remove (transaction_t *T) {
  transaction_t *u = T->prev, *v = T->next;
  if (u == NULL && v == NULL) {
    return;
  }
  assert (u != NULL && v != NULL);
  u->next = v;
  v->prev = u;
  T->prev = T->next = NULL;
  lru_size--;
}

void lru_reuse (transaction_t *T) {
  lru_remove (T);
  lru_add (T);
}

#define HASH_MASK 0x3fff
int tot_memory_transactions;
transaction_t *H[HASH_MASK+1];

transaction_t *get_transaction_f (unsigned long long volume_id, int transaction_id, int force);

static void transaction_free (transaction_t *T) {
  lru_remove (T);
  get_transaction_f (T->volume_id, T->transaction_id, -1);
  free_tree (T->root);
  zfree (T, sizeof (*T));
  tot_memory_transactions--;
}

static void transaction_lru_gc (void) {
  assert (lru_size == tot_memory_transactions);
  while (lru_size >= max_lru_size) {
    transaction_t *T = lru_list.prev;
    assert (T);
    transaction_free (T);
  }
}

transaction_t *get_transaction_f (unsigned long long volume_id, int transaction_id, int force) {
  unsigned h = volume_id >> 32;
  h = h * 10007 + ((unsigned) volume_id);
  h = h * 10007 + transaction_id;
  h &= HASH_MASK;
  transaction_t **p = &(H[h]), *q;
  while (1) {
    q = *p;
    if (q == NULL) {
      break;
    }
    if (q->transaction_id == transaction_id && q->volume_id == volume_id) {
      *p = q->hnext;
      if (force >= 0) {
        q->hnext = H[h];
        H[h] = q;
        lru_reuse (q);
      }
      return q;
    }
    p = &(q->hnext);
  }
  if (force > 0) {
    transaction_lru_gc ();
    q = zmalloc0 (sizeof (transaction_t));
    tot_memory_transactions++;
    q->volume_id = volume_id;
    q->transaction_id = transaction_id;
    q->hnext = H[h];
    H[h] = q;
    lru_add (q);
    return q;
  }
  return NULL;
}

static int transaction_set_result (host_t *H, int transaction_id, unsigned result) {
  transaction_t *T = get_transaction_f (H->volume_id, transaction_id, 1);
  tree_t *P = tree_lookup (T->root, H->host_id);
  if (P != NULL) {
    P->value = result;
  } else {
    T->root = tree_insert (T->root, H->host_id, lrand48 (), result);
  }
  return 0;
}

static int set_connect (struct lev_copyexec_result_connect *E) {
  host_t *H = get_host_by_random_tag (E->random_tag, 1);
  H->volume_id = E->volume_id;
  H->ip = E->ip;
  H->pid = E->pid;
  H->last_action_time = now;
  const int l = E->hostname_length, old_l = (H->hostname) ? (int) strlen (H->hostname) : -1;
  if (l != old_l || memcmp (H->hostname, E->hostname, l)) {
    if (H->hostname) {
      kprintf ("set_connect: hostname didn't matched. Old hostname = %s, new hostname = %.*s. (log_cur_pos: %lld)\n", H->hostname, E->hostname_length, E->hostname, log_cur_pos ());
      exit (1);
    }
    H->hostname = zmalloc0 (l + 1);
    memcpy (H->hostname, E->hostname, l);
  }
  return 0;
}

static int set_result (host_t *H, struct lev_copyexec_result_data *E) {
  if (H == NULL) {
    H = get_host_by_random_tag (E->random_tag, 0);
    if (H == NULL) {
      kprintf ("set_result: get_host_by_random_tag (0x%llx) returns NULL. (log_cur_pos: %lld)\n", E->random_tag, log_cur_pos ());
      exit (1);
    }
  }
  assert (H->random_tag == E->random_tag);
  if (H->binlog_pos >= E->binlog_pos) {
    kprintf ("set_result: H->binlog_pos >= E->binlog_pos, H->binlog_pos = 0x%llx, E->binlog_pos = 0x%llx. (log_cur_pos: %lld)\n", H->binlog_pos, E->binlog_pos, log_cur_pos ());
    exit (1);
    return -1;
  }
  H->binlog_pos = E->binlog_pos;
  if (!H->first_data_time) {
    H->first_data_time = now;
  }
  H->last_data_time = H->last_action_time = now;
  return transaction_set_result (H, E->transaction_id, E->result);
}

int do_set_result (struct connection *c, int transaction_id, unsigned result, long long binlog_pos) {
  host_t *H = get_host_by_connection (c);
  if (H == NULL) {
    return COPYEXEC_ERR_DISCONNECT;
  }

  if (H->binlog_pos >= binlog_pos) {
    return COPYEXEC_ERR_OLD_RESULT;
  }

  struct lev_copyexec_result_data *E = alloc_log_event (LEV_COPYEXEC_RESULT_DATA, sizeof (*E), 0);
  E->random_tag = H->random_tag;
  E->transaction_id = transaction_id;
  E->result = result;
  E->binlog_pos = binlog_pos;
  return set_result (H, E);
}

int set_enable (unsigned long long random_tag, int enable) {
  host_t *H = get_host_by_random_tag (random_tag, 0);
  if (H == NULL) {
    return -1;
  }
  int disabled = enable ? 0 : 1;
  if (H->disabled == disabled) {
    return 1;
  }
  H->disabled = disabled;
  return 0;
}

int do_set_enable (unsigned long long random_tag, int enable) {
  int r = set_enable (random_tag, enable);
  if (!r) {
    struct lev_copyexec_result_enable *E = alloc_log_event (enable ? LEV_COPYEXEC_RESULT_ENABLE : LEV_COPYEXEC_RESULT_DISABLE, sizeof (*E), 0);
    E->random_tag = random_tag;
  }
  return r;
}

/******************** extract results functions ********************/
struct pair_hostid_result {
  int host_id;
  unsigned result;
};

static int cmp_pair_hostid_result (const void *a, const void *b) {
  const struct pair_hostid_result *x = (const struct pair_hostid_result *) a;
  const struct pair_hostid_result *y = (const struct pair_hostid_result *) b;
  if (x->result < y->result) {
    return -1;
  }
  if (x->result > y->result) {
    return  1;
  }
  return 0;
}

static void tree_get_pairs_hostid_result (struct pair_hostid_result *a, int *k, unsigned long long volume_id, tree_t *P) {
  if (P == NULL) {
    return;
  }
  tree_get_pairs_hostid_result (a, k, volume_id, P->left);
  const int first_host_id = (!(*k)) ? 1 : (a[(*k)-1].host_id + 1);
  int i;

  for (i = first_host_id; i < P->x; i++) {
    if (HOSTS[i]->volume_id == volume_id && !HOSTS[i]->disabled) {
      assert ((*k) < hosts);
      a[*k].host_id = i;
      a[*k].result = 0;
      (*k)++;
    }
  }

  assert ((*k) < hosts);
  assert (HOSTS[P->x]->volume_id == volume_id);
  if (!HOSTS[P->x]->disabled) {
    a[*k].host_id = P->x;
    a[*k].result = P->value;
    (*k)++;
  }
  tree_get_pairs_hostid_result (a, k, volume_id, P->right);
}

static void filter_pairs_hostid_result (struct pair_hostid_result *a, int *k, unsigned result_or, unsigned result_and) {
  int i, n = 0;
  for (i = 0; i < (*k); i++) {
    if ((a[i].result & result_and) == result_or) {
      if (n < i) {
        memcpy (&a[n], &a[i], sizeof (a[0]));
      }
      n++;
    }
  }
  *k = n;
}

static struct pair_hostid_result *get_pairs_hostid_result (int *k, unsigned long long volume_id, tree_t *P) {
  *k = 0;
  struct pair_hostid_result *a = calloc (hosts, sizeof (a[0]));
  if (a == NULL) {
    return NULL;
  }
  tree_get_pairs_hostid_result (a, k, volume_id, P);
  const int first_host_id = (!(*k)) ? 1 : (a[(*k)-1].host_id + 1);
  int i;
  for (i = first_host_id; i <= hosts; i++) {
    if (HOSTS[i]->volume_id == volume_id && !HOSTS[i]->disabled) {
      assert ((*k) < hosts);
      a[*k].host_id = i;
      a[*k].result = 0;
      (*k)++;
    }
  }
  vkprintf (4, "get_pairs_hostid_result: *k = %d\n", *k);
  return a;
}

char *get_status_freqs (unsigned long long volume_id, int transaction_id) {
  char buf[64];
  transaction_t *T = get_transaction_f (volume_id, transaction_id, 0);
  if (T == NULL) {
    return NULL;
  }
  int n;
  struct pair_hostid_result *a = get_pairs_hostid_result (&n, volume_id, T->root);

  if (a == NULL) {
    return NULL;
  }

  if (n == 0) {
    free (a);
    return strdup ("");
  }

  unsigned *c = calloc (16, sizeof (c[0]));
  int i, l = 0;
  for (i = 0; i < n; i++) {
    c[a[i].result >> 28]++;
  }
  free (a);
  for (i = 0; i < 16; i++) {
    if (c[i]) {
      int o = snprintf (buf, sizeof (buf), "%s,%d,", status_to_alpha (i), c[i]);
      assert (o < (int) sizeof (buf));
      l += o;
    }
  }
  char *z = malloc (l+1), *p = z;
  assert (p);
  for (i = 0; i < 16; i++) {
    if (c[i]) {
      p += sprintf (p, "%s,%d,", status_to_alpha (i), c[i]);
    }
  }
  assert (p == (z + l));
  *(--p) = 0; /* erase last comma */
  free (c);
  return z;
}

static void result_as_unsigned (char *buf, int buf_len, unsigned result) {
  assert (snprintf (buf, buf_len, "%u", result) < buf_len);
}

static void result_as_status_result (char *buf, int buf_len, unsigned result) {
  int status = result >> 28;
  result &= 0x0fffffffU;
  assert (snprintf (buf, buf_len, "%s:0x%04x", status_to_alpha (status), result) < buf_len);
}

static char *get_freqs (unsigned long long volume_id, int transaction_id, void (*fp_result_to_alpha)(char *buf, int buf_size, unsigned result)) {
  char buf[64];
  transaction_t *T = get_transaction_f (volume_id, transaction_id, 0);
  if (T == NULL) {
    return NULL;
  }
  int n;
  struct pair_hostid_result *a = get_pairs_hostid_result (&n, volume_id, T->root);
  vkprintf (4, "get_pair_hostide_result (volume_id: %llu) found %d pairs(s), %d hosts\n", volume_id, n, hosts);
  assert (n <= hosts);
  if (a == NULL) {
    return NULL;
  }

  if (n == 0) {
    return strdup ("");
  }

  qsort (a, n, sizeof (a[0]), cmp_pair_hostid_result);
  int i, m = 0;
  a[0].host_id = 1;
  for (i = 1; i < n; i++) {
    if (a[m].result == a[i].result) {
      a[m].host_id++;
    } else {
      a[++m].result = a[i].result;
      a[m].host_id = 1;
    }
  }
  m++;
  int l = 0;
  for (i = 0; i < m; i++) {
    fp_result_to_alpha (buf, sizeof (buf), a[i].result);
    l += strlen (buf);
    int o = snprintf (buf, sizeof (buf), ",%d,", a[i].host_id);
    assert (o < (int) sizeof (buf));
    l += o;
  }
  char *z = malloc (l+1), *p = z;
  assert (p);
  for (i = 0; i < m; i++) {
    fp_result_to_alpha (buf, sizeof (buf), a[i].result);
    p += sprintf (p, "%s,%d,", buf, a[i].host_id);
  }
  assert (p == (z + l));
  *(--p) = 0; /* erase last comma */
  free (a);
  return z;
}

char *get_results_freqs (unsigned long long volume_id, int transaction_id) {
  return get_freqs (volume_id, transaction_id, result_as_unsigned);
}

/* srfreqs */
char *get_status_results_freqs (unsigned long long volume_id, int transaction_id) {
  return get_freqs (volume_id, transaction_id, result_as_status_result);
}

char *get_hosts_list (unsigned long long volume_id, int transaction_id, unsigned result_or, unsigned result_and) {
  transaction_t *T = get_transaction_f (volume_id, transaction_id, 0);
  if (T == NULL) {
    return NULL;
  }
  int n;
  struct pair_hostid_result *a = get_pairs_hostid_result (&n, volume_id, T->root);
  if (a == NULL) {
    return NULL;
  }

  filter_pairs_hostid_result (a, &n, result_or, result_and);

  if (!n) {
    free (a);
    return strdup ("");
  }

  int i, l = 0;

  for (i = 0; i < n; i++) {
    l += strlen (HOSTS[a[i].host_id]->hostname) + 1;
  }

  char *z = malloc (l), *p = z;
  if (z == NULL) {
    free (a);
    return NULL;
  }

  for (i = 0; i < n; i++) {
    if (i > 0) {
      *p++= ',';
    }
    strcpy (p, HOSTS[a[i].host_id]->hostname);
    p += strlen (p);
  }
  vkprintf (4, "p = %p, z + l = %p\n", p, z + l);
  assert (p == (z + l - 1));
  free (a);
  return z;
}

char *get_hosts_list_by_status (unsigned long long volume_id, int transaction_id, const char *const status) {
  char buf[64];
  int s = alpha_to_status (status);
  if (s < 0) {
    vkprintf (3, "get_hosts_list_by_status: unknown status \"%s\"\n", status);
    return NULL;
  }
  transaction_t *T = get_transaction_f (volume_id, transaction_id, 0);
  if (T == NULL) {
    vkprintf (3, "get_hosts_list_by_status: get_transaction_f (0x%llx, %d) returns NULL.\n", volume_id, transaction_id);
    return NULL;
  }
  int n;
  struct pair_hostid_result *a = get_pairs_hostid_result (&n, volume_id, T->root);
  if (a == NULL) {
    vkprintf (3, "get_hosts_list_by_status: get_pairs_hostid_result returns NULL.\n");
    return NULL;
  }

  filter_pairs_hostid_result (a, &n, ((unsigned) s) << 28, 0xf0000000U);

  if (n == 0) {
    free (a);
    return strdup ("");
  }

  int i, l = 0;

  for (i = 0; i < n; i++) {
    a[i].result &= 0x0fffffffU;
  }

  for (i = 0; i < n; i++) {
    int o = snprintf (buf, sizeof (buf), "0x%04x", a[i].result);
    assert (o < (int) sizeof (buf));
    l += o + strlen (HOSTS[a[i].host_id]->hostname) + 2; /* 2 : two commas */
  }
  char *z = malloc (l), *p = z;
  for (i = 0; i < n; i++) {
    if (i > 0) {
      *p++ = ',';
    }
    p += sprintf (p, "%s,0x%04x", HOSTS[a[i].host_id]->hostname, a[i].result);
  }
  assert (p == (z + (l - 1)));
  free (a);
  return z;
}

/* srhosts */
char *get_hosts_list_by_status_and_result (unsigned long long volume_id, int transaction_id, const char *const status, unsigned result) {
  vkprintf (3, "get_hosts_list_by_status_and_result (volume_id:%llu, transaction_id: %d, status: %s, result:0x%x)\n", volume_id, transaction_id, status, result);
  int s = alpha_to_status (status);
  if (s < 0) {
    vkprintf (3, "get_hosts_list_by_status_and_result: unknown status \"%s\"\n", status);
    return NULL;
  }
  return get_hosts_list (volume_id, transaction_id, (((unsigned) s) << 28) | result, 0xffffffffU);
}

char *get_dead_hosts_list (unsigned long long volume_id, int delay) {
  vkprintf (3, "get_dead_hosts_list (volume_id:%llu, delay: %d)\n", volume_id, delay);
  int *a = calloc (hosts, sizeof (a[0]));
  if (a == NULL) {
    return NULL;
  }
  int i, n = 0, l = 0, t = now - delay;
  for (i = 1; i <= hosts; i++) {
    if (HOSTS[i]->volume_id == volume_id && !HOSTS[i]->disabled && HOSTS[i]->last_action_time < t) {
      a[n++] = i;
      l += strlen (HOSTS[i]->hostname) + 1;
    }
  }
  if (!n) {
    return strdup ("");
  }
  char *z = malloc (l), *p = z;
  if (z == NULL) {
    free (a);
    return NULL;
  }

  for (i = 0; i < n; i++) {
    if (i > 0) {
      *p++= ',';
    }
    strcpy (p, HOSTS[a[i]]->hostname);
    p += strlen (p);
  }
  assert (p == (z + l - 1));
  free (a);
  return z;
}

char *get_dead_hosts_list_full (unsigned long long volume_id, int delay) {
  vkprintf (3, "get_dead_hosts_list_full (volume_id:%llu, delay: %d)\n", volume_id, delay);
  char buf[1024];
  int *a = calloc (hosts, sizeof (a[0]));
  if (a == NULL) {
    return NULL;
  }
  int i, n = 0, l = 0, t = now - delay;
  for (i = 1; i <= hosts; i++) {
    if (HOSTS[i]->volume_id == volume_id && !HOSTS[i]->disabled && HOSTS[i]->last_action_time < t) {
      a[n++] = i;
      l += snprintf (buf, sizeof (buf), "%llu,%s,0x%llx,%d,%d\n", HOSTS[i]->volume_id, HOSTS[i]->hostname, HOSTS[i]->random_tag, HOSTS[i]->first_data_time, HOSTS[i]->last_data_time);
    }
  }
  if (!n) {
    return strdup ("");
  }
  l++;
  char *z = malloc (l), *p = z;
  if (z == NULL) {
    free (a);
    return NULL;
  }

  for (i = 0; i < n; i++) {
    p += sprintf (p, "%llu,%s,0x%llx,%d,%d\n", HOSTS[a[i]]->volume_id, HOSTS[a[i]]->hostname, HOSTS[a[i]]->random_tag, HOSTS[a[i]]->first_data_time, HOSTS[a[i]]->last_data_time);
  }
  assert (p == (z + l - 1));
  free (a);
  return z;
}

static int weak_cmp_phost_t (const host_t *a, const host_t *b) {
  if (a->volume_id < b->volume_id) {
    return -1;
  }
  if (a->volume_id > b->volume_id) {
    return 1;
  }
  return strcmp (a->hostname, b->hostname);
}

static int cmp_phost_t (const void *x, const void *y) {
  const host_t *a = *(const host_t **) x, *b = *(const host_t **) y;
  int r = weak_cmp_phost_t (a, b);
  if (r) {
    return r;
  }
  if (a->first_data_time < b->first_data_time) {
    return -1;
  }
  if (a->first_data_time > b->first_data_time) {
    return 1;
  }
  if (a->last_data_time < b->last_data_time) {
    return -1;
  }
  if (a->last_data_time > b->last_data_time) {
    return 1;
  }
  return 0;
}

char *get_collisions_list (void) {
  vkprintf (3, "get_collistions_list: hosts = %d\n", hosts);
  int i, j, k;
  char buf[1024];
  if (!hosts) {
    return NULL;
  }
  host_t **a = calloc (hosts, sizeof (a[0]));
  if (a == NULL) {
    return NULL;
  }
  int n = 0;
  for (i = 0; i < hosts; i++) {
    if (!HOSTS[i+1]->disabled) {
      a[n++] = HOSTS[i+1];
    }
  }
  if (!n) {
    return NULL;
  }
  qsort (a, n, sizeof (a[0]), cmp_phost_t);
  int l = 0;
  for (i = 0; i < n; i = j) {
    for (j = i + 1; j < n && !weak_cmp_phost_t (a[i], a[j]); j++) { }
    int m = j - i;
    if (m > 1) {
      for (k = i; k < j; k++) {
        l += snprintf (buf, sizeof (buf), "%llu,%s,0x%llx,%d,%d\n", a[k]->volume_id, a[k]->hostname, a[k]->random_tag, a[k]->first_data_time, a[k]->last_data_time);
      }
    }
  }
  if (l == 0) {
    return strdup ("");
  }
  l++;
  char *z = malloc (l), *p = z;
  if (z == NULL) {
    free (a);
    return NULL;
  }
  for (i = 0; i < n; i = j) {
    for (j = i + 1; j < n && !weak_cmp_phost_t (a[i], a[j]); j++) { }
    int m = j - i;
    if (m > 1) {
      for (k = i; k < j; k++) {
        p += sprintf (p, "%llu,%s,0x%llx,%d,%d\n", a[k]->volume_id, a[k]->hostname, a[k]->random_tag, a[k]->first_data_time, a[k]->last_data_time);
      }
    }
  }
  free (a);
  assert (p == (z + l - 1));
  return z;
}

static int cmp_unsigned_long_long (const void *a, const void *b) {
  const unsigned long long x = *((const unsigned long long *) a);
  const unsigned long long y = *((const unsigned long long *) b);
  return (x < y) ? -1 : (x > y) ? 1 : 0;
}

char *get_volumes (void) {
  char buf[32];
  if (hosts <= 0) {
    return strdup ("");
  }
  int i, m = 0, l = 0;
  unsigned long long *v = calloc (hosts, sizeof (v[0]));
  for (i = 0; i < hosts; i++) {
    v[i] = HOSTS[i+1]->volume_id;
  }
  qsort (v, hosts, sizeof (v[0]), cmp_unsigned_long_long);
  for (i = 1; i < hosts; i++) {
    if (v[i] != v[m]) {
      v[++m] = v[i];
    }
  }
  m++;
  for (i = 0; i < m; i++) {
    int o = snprintf (buf, sizeof (buf), "%llu", v[i]);
    assert (o < sizeof (buf));
    l += o + 1;
  }
  char *z = malloc (l), *p = z;
  for (i = 0; i < m; i++) {
    if (i > 0) {
      *p++ = ',';
    }
    p += sprintf (p, "%llu", v[i]);
  }
  free (v);
  assert (p == (z + (l - 1)));
  return z;
}

char *get_disabled (unsigned long long volume_id) {
  vkprintf (3, "get_disabled (volume_id: %llu)\n", volume_id);
  char buf[32];
  if (hosts <= 0) {
    return strdup ("");
  }
  int i, l = 0, m = 0;
  host_t *h;
  for (i = 1; i <= hosts; i++) {
    h = HOSTS[i];
    if (h->disabled && h->volume_id == volume_id) {
      l += snprintf (buf, sizeof (buf), "%s,0x%llx,%d,%d\n", h->hostname, h->random_tag, h->first_data_time, h->last_data_time);
      m++;
    }
  }
  if (!m) {
    return strdup ("");
  }
  vkprintf (4, "get_disabled: l = %d, m = %d\n", l, m);

  char *z = malloc (l + 1), *p = z;
  if (z == NULL) {
    return NULL;
  }

  for (i = 1; i <= hosts; i++) {
    h = HOSTS[i];
    if (h->disabled && h->volume_id == volume_id) {
      p += sprintf (p, "%s,0x%llx,%d,%d\n", h->hostname, h->random_tag, h->first_data_time, h->last_data_time);
    }
  }

  assert (p == (z + l));
  return z;
}

/******************** replay_logevent  ********************/

int copyexec_result_replay_logevent (struct lev_generic *E, int size);
int init_copyexec_result_data (int schema) {
  replay_logevent = copyexec_result_replay_logevent;
  return 0;
}

static int copyexec_result_le_start (struct lev_start *E) {
  if (E->schema_id != COPYEXEC_RESULT_SCHEMA_V1) {
    return -1;
  }

  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

int copyexec_result_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return copyexec_result_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
    case LEV_TAG:
      return default_replay_logevent (E, size);
    case LEV_COPYEXEC_RESULT_CONNECT:
      s = sizeof (struct lev_copyexec_result_connect);
      if (size < s) { return -2; }
      s += ((struct lev_copyexec_result_connect *) E)->hostname_length;
      if (size < s) { return -2; }
      set_connect ((struct lev_copyexec_result_connect *) E);
      return s;
    case LEV_COPYEXEC_RESULT_DATA:
      s = sizeof (struct lev_copyexec_result_data);
      if (size < s) { return -2; }
      set_result (NULL, (struct lev_copyexec_result_data *) E);
      return s;
    case LEV_COPYEXEC_RESULT_DISABLE:
    case LEV_COPYEXEC_RESULT_ENABLE:
      s = sizeof (struct lev_copyexec_result_enable);
      if (size < s) { return -2; }
      set_enable (((struct lev_copyexec_result_enable *) E)->random_tag, (E->type == LEV_COPYEXEC_RESULT_ENABLE) ? 1 : 0 );
      return s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;

}

