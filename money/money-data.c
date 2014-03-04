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

    Copyright 2010-2012 Vkontakte Ltd
              2010-2012 Nikolai Durov
              2010-2012 Andrei Lopatin
*/
#define	_FILE_OFFSET_BITS	64

#include <sys/timerfd.h>
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
#include "kdb-money-binlog.h"
#include "money-data.h"
#include "server-functions.h"
#include "md5.h"
#include "kfs.h"

#define MASTER_CANCEL_MEMORY_CODE	0x003ABECA30B3EBCALL

extern int now;
extern int verbosity;

int master_slave_mode;
int split_min, split_max = 1, split_mod = 1;

int index_mode;
int use_aio;

/* --------- money data ------------- */

int global_engine_status;
long long special_acc_id;

int tot_account_types, tot_accounts, frozen_accounts, special_accounts;
int tot_transactions, temp_transactions, committed_transactions, cancelled_transactions, committed_operations, tot_locks;
int cancelled_long_transactions, committed_long_transactions, expired_long_transactions, locked_long_transactions;
int cancelled_committed_transactions;

long long currency_sum_pos[MAX_CURRENCIES];
int currency_accounts[MAX_CURRENCIES];
int currency_operations[MAX_CURRENCIES];

long long last_transaction_id;

struct account_type *AccTypes[MAX_ACCOUNT_TYPE+1];
account_t *AccHash[ACC_HASH_SIZE];

transaction_t *TrHash[TRANS_HASH_SIZE];

struct transaction_hash_head {
  transaction_t *th_first, *th_last;
} TrTempHash[TRANS_TEMP_HASH_SIZE];

transaction_t *DelayQueues[DELAY_QUEUES];
int dq_now, dq;

transaction_t *LH[LOCK_HEAP_SIZE];
int LHN;

int money_replay_logevent (struct lev_generic *E, int size);

int init_money_data (int schema) {
  int i;

  replay_logevent = money_replay_logevent;

  assert (!tot_account_types && !tot_accounts && !tot_transactions);

  for (i = 0; i < TRANS_TEMP_HASH_SIZE; i++) {
    TrTempHash[i].th_first = TrTempHash[i].th_last = (transaction_t *) &TrTempHash[i];
  }

  return 0;
}


static int money_le_start (struct lev_start *E) {
  if (E->schema_id != MONEY_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);
  assert (log_split_mod == 1);

  return 0;
}


static int account_bucket (int acc_type_id, long long acc_id) {
  return ((acc_id >> 24) * 239 + acc_id + acc_type_id * 997) & (ACC_HASH_SIZE - 1);
}


account_t *get_account (int acc_type_id, long long acc_id) {
  int b = account_bucket (acc_type_id, acc_id);
  account_t *ptr;

  for (ptr = AccHash[b]; ptr; ptr = ptr->h_next) {
    if (acc_id == ptr->acc_id && acc_type_id == ptr->acc_type->acc_type_id) {
      return ptr;
    }
  }
  return 0;
}


static int add_account (struct account *A) {
  int b = account_bucket (A->acc_type->acc_type_id, A->acc_id);

  A->h_next = AccHash[b];
  AccHash[b] = A;
  return 0;
}

// adds a transaction into an account transaction list; T->state must be at least 'declared'
static int add_acc_transaction (struct account *A, struct transaction *T) {
  if (!A->trans_alloc) {
    A->trans_alloc = 8;
    A->acc_transactions = zmalloc (8 * sizeof (void *));
  }
  if (A->trans_num == A->trans_alloc) {
    struct transaction **N = zmalloc ((A->trans_alloc << 1) * sizeof (void *));
    memcpy (N, A->acc_transactions, A->trans_alloc * sizeof (void *));
    zfree (A->acc_transactions, A->trans_alloc * sizeof (void *));
    A->acc_transactions = N;
    A->trans_alloc <<= 1;
  }

  A->acc_transactions[A->trans_num++] = T;
  return A->trans_num;
}


static int transaction_bucket (long long transaction_id) {
  return (transaction_id % (TRANS_HASH_SIZE - 5)) & (TRANS_HASH_SIZE - 1);
}

transaction_t *get_transaction (long long transaction_id) {
  int b = transaction_bucket (transaction_id);
  transaction_t *ptr;

  for (ptr = TrHash[b]; ptr; ptr = ptr->h_next) {
    if (ptr->transaction_id == transaction_id) {
      return ptr;
    }
  }
  return 0;
}

static int add_transaction (transaction_t *T) {
  int b = transaction_bucket (T->transaction_id);
  assert (!T->h_next);

  T->h_next = TrHash[b];
  TrHash[b] = T;
  return 0;
}

static int transaction_temp_bucket (int temp_id) {
  return temp_id & (TRANS_TEMP_HASH_SIZE - 1);
}

transaction_t *get_temp_transaction (int temp_id) {
  int b = transaction_temp_bucket (temp_id);
  transaction_t *ptr;

  for (ptr = TrTempHash[b].th_first; ptr != (void *) &TrTempHash[b]; ptr = ptr->th_next) {
    if (ptr->temp_id == temp_id) {
      return ptr;
    }
  }
  return 0;
}

static int add_temp_transaction (transaction_t *T) {
  int b = transaction_temp_bucket (T->temp_id);
  assert (!T->th_next && !T->th_prev);

  T->th_next = TrTempHash[b].th_first;
  T->th_prev = (void *) &TrTempHash[b];
  TrTempHash[b].th_first->th_prev = T;
  TrTempHash[b].th_first = T;

  return 0;
}

static int remove_temp_transaction (transaction_t *T) {
  assert (T->th_next && T->th_prev);
  T->th_prev->th_next = T->th_next;
  T->th_next->th_prev = T->th_prev;
  T->th_prev = T->th_next = 0;
  return 0;
}

static int lock_heap_adjust (int i, transaction_t *T) {
  int j, x = T->long_locked_until;
  while (i > 1) {
    j = (i >> 1);
    if (LH[j]->long_locked_until <= x) {
      break;
    }
    LH[i] = LH[j];
    LH[i]->long_lock_heap_pos = i;
    i = j;
  }
  while (2*i <= LHN) {
    j = 2*i;
    if (j < LHN && LH[j+1]->long_locked_until < LH[j]->long_locked_until) {
      j++;
    }
    if (LH[j]->long_locked_until >= x) {
      break;
    }
    LH[i] = LH[j];
    LH[i]->long_lock_heap_pos = i;
    i = j;
  }
  LH[i] = T;
  T->long_lock_heap_pos = i;
  return i;
}

static void put_into_lock_heap (transaction_t *T) {
  assert (!T->long_lock_heap_pos && T->long_locked_until);
  assert (LHN < LOCK_HEAP_SIZE);
  locked_long_transactions = ++LHN;
  lock_heap_adjust (LHN, T);
}

static void remove_from_lock_heap (transaction_t *T) {
  int i = T->long_lock_heap_pos;
  assert (i > 0 && i <= LHN && LH[i] == T);
  T->long_lock_heap_pos = 0;
  locked_long_transactions = --LHN;
  if (i <= LHN) {
    lock_heap_adjust (i, LH[LHN+1]);
  }
}

/* replay log */

static int create_account_type (struct lev_money_new_atype *E) {
  struct account_type *A;
  int t = E->acc_type_id;
  assert (t > 0 && t <= MAX_ACCOUNT_TYPE);
  assert (E->currency >= MIN_CURRENCY_ID && E->currency <= MAX_CURRENCY_ID);
  if (!AccTypes[t]) {
    AccTypes[t] = A = zmalloc0 (sizeof (struct account_type));
    A->currency = E->currency;
    A->acc_type_id = t;
    A->acc_class = E->acc_class;
    A->creator_id = E->creator_id;
    A->ip = E->ip;
  } else {
    A = AccTypes[t];
    assert (A->currency == E->currency && A->creator_id == E->creator_id && A->acc_class == E->acc_class);
    zfree (A->comment, A->comm_len + 1);
  }
  A->auth_code = E->auth_code;
  A->admin_code = E->admin_code;
  A->access_code = E->access_code;
  A->withdraw_code = E->withdraw_code;
  A->block_code = E->block_code;
  A->create_code = E->create_code;
  A->comm_len = E->comm_len;
  assert (A->comm_len >= 0 && A->comm_len <= 4096);
  A->comment = zmalloc (E->comm_len + 1);
  memcpy (A->comment, E->comment, A->comm_len + 1);
  assert (!A->comment[A->comm_len]);

  ++tot_account_types;

  return 0;
}


static int create_account (struct lev_money_new_acc *E, int flag) {
  struct account *A;

  assert ((unsigned) E->acc_type_id <= MAX_ACCOUNT_TYPE && AccTypes[E->acc_type_id]);
  assert (!get_account (E->acc_type_id, E->acc_id));
  assert (E->currency == AccTypes[E->acc_type_id]->currency);

  A = zmalloc0 (sizeof (struct account));
  A->acc_type = AccTypes[E->acc_type_id];
  A->acc_id = E->acc_id;

  A->owner_id = E->owner_id;
  A->ip = E->ip;
  A->auth_code = E->auth_code;

  if (flag) {
    A->access_code = E->access_code;
    A->withdraw_code = E->withdraw_code;
    if (E->comm_len) {
      A->comm_len = E->comm_len;
      assert (A->comm_len >= 0 && A->comm_len <= 4096);
      A->comment = zmalloc (E->comm_len + 1);
      memcpy (A->comment, E->comment, E->comm_len + 1);
      assert (!A->comment[A->comm_len]);
    }
  }

  add_account (A);

  ++tot_accounts;
  if (A->acc_type->acc_class & 4) {
    ++special_accounts;
  }

  ++currency_accounts[E->currency-MIN_CURRENCY_ID];

  return 0;
}

static transaction_t *alloc_new_transaction (int parties) {
  transaction_t *T;
  assert (parties >= 2 && parties <= 10);
  T = zmalloc0 (offsetof (struct transaction, T) + parties * sizeof (struct transaction_party));
  T->status = -1;
  T->created_at = now;
  T->parties = parties;
  return T;
}
  


static int declare_transaction (struct lev_money_trans_declare *E) {
  transaction_t *T = get_temp_transaction (E->temp_id);
  struct lev_one_transaction *ET = E->T;
  char *comment;
  int i, j, k;
  static long long CV[16];
  static int CC[16];
  account_t *A;

  assert (E->transaction_id);
  assert (E->temp_id > 0);
  assert (E->parties >= 2 && E->parties <= 10);
  assert ((unsigned) E->comm_len <= 4095);

  if (E->transaction_id > last_transaction_id) {
    last_transaction_id = E->transaction_id;
  }

  comment = (char *) &E->T[E->parties];
  assert (!E->comm_len || !comment[E->comm_len]);

  k = 0;
  for (i = 0; i < E->parties; i++, ET++) {
    assert ((unsigned) ET->acc_type_id <= MAX_ACCOUNT_TYPE && AccTypes[ET->acc_type_id]);
    assert (AccTypes[ET->acc_type_id]->currency == ET->currency);
    assert (ET->acc_incr < MAX_ACC_INCR && ET->acc_incr >= -MAX_ACC_INCR);
//    assert (ET->acc_incr);
    assert (ET->acc_type_id);
    for (j = 0; j < i; j++) {
      assert (E->T[j].acc_id != ET->acc_id || E->T[j].acc_type_id != ET->acc_type_id);
    }
    for (j = 0; j < k; j++) {
      if (ET->currency == CC[j]) {
        CV[j] += ET->acc_incr;
        break;
      }
    }
    if (j == k) {
      CC[k] = ET->currency;
      CV[k++] = ET->acc_incr;
    }
  }

  for (j = 0; j < k; j++) {
    assert (!CV[j]);
  }

  if (T) {
    assert (T->status <= trs_temp_locked);
    assert (T->transaction_id == E->transaction_id);
    assert (T->parties == E->parties);
    for (i = 0; i < E->parties; i++) {
      assert (E->T[i].acc_incr == T->T[i].acc_incr && E->T[i].auth_code == T->T[i].auth_code);
      A = get_account (E->T[i].acc_type_id, E->T[i].acc_id);
      assert (A && T->T[i].tr_account == A);
      add_acc_transaction (A, T);
    }
    T->status += 2;
  } else {
    assert (!get_transaction (E->transaction_id));
    T = alloc_new_transaction (E->parties);
    T->transaction_id = E->transaction_id;
    T->auth_code = E->auth_code;
    T->status = trs_declared;
    T->temp_id = E->temp_id;
    T->parties = E->parties;
    T->declared_date = E->declared_date;
    T->ip = E->ip;
    T->comment_len = E->comm_len;
    if (E->comm_len) {
      T->comment = zmalloc (E->comm_len + 1);
      memcpy (T->comment, comment, E->comm_len + 1);
    }
    for (i = 0; i < E->parties; i++) {
      T->T[i].acc_incr = E->T[i].acc_incr;
      T->T[i].auth_code = E->T[i].auth_code;
      A = get_account (E->T[i].acc_type_id, E->T[i].acc_id);
      assert (A);
      T->T[i].tr_account = A;
      T->T[i].acc_type_id = E->T[i].acc_type_id;
      T->T[i].acc_id = E->T[i].acc_id;
      add_acc_transaction (A, T);
    }
    add_temp_transaction (T);
    add_transaction (T);
    tot_transactions++;
  }

  return 1;
}

// 0 = transaction possible
// -48+x = not enough money on party x
// -32+x = access denied for party x
// -1 = GPF
static int check_transaction (transaction_t *T) {
  int i, c;
  assert (T->status <= trs_declared_locked || T->status == trs_long_locked);
  for (i = 0; i < T->parties; i++) {
    long long x = T->T[i].acc_incr;
    account_t *A = T->T[i].tr_account;
    if (!A) {
      return -32 + i;
    }
//    assert (x);
    c = A->acc_type->acc_class;
    // main idea: 0 is negative (withdrawal operation) 
    if (x > 0) {
      assert (x < MAX_ACC_INCR);
      if ((A->acc_state & 2) || (c & 2)) {
        return -32 + i;
      }
      if (A->balance + x >= MAX_BALANCE) {
        return -1;
      }
    } else {
      assert (x >= -MAX_ACC_INCR);
      if ((A->acc_state & 1) || (c & 1)) {
        return -32 + i;
      }
      if (T->status != trs_long_locked && A->withdraw_code && T->T[i].auth_code != A->withdraw_code) {
        return -32 + i;
      }
      if (T->status & 1) {
        assert (A->lock_num > 0 && A->locked + x >= 0);
        assert (A->balance >= A->locked || (c & 4));
      } else {
        if (A->balance + x < A->locked && !(c & 4)) {
          return -48+i;
        }
      }
      if (A->balance + x < -MAX_BALANCE || A->locked - x >= MAX_BALANCE) {
        return -1;
      }
    }
  }
  return 0;
}

// must be executed only if check_transaction() succeeds
// -1: error (?)
// 0: already locked
// 1: locked
static int lock_transaction (transaction_t *T) {
  int i, c;
  assert (T->status <= trs_declared_locked);
  if (T->status & 1) {
    return 0;
  }
  for (i = 0; i < T->parties; i++) {
    long long x = T->T[i].acc_incr;
    account_t *A = T->T[i].tr_account;
    assert (A);  // was: A && x
    c = A->acc_type->acc_class;
    if (x <= 0) {
      assert (!(A->acc_state & 1) && !(c & 1));
      assert (x >= -MAX_ACC_INCR);
      A->lock_num++;
      A->locked -= x;
      assert (A->locked < MAX_BALANCE);
      assert (A->balance >= A->locked || (c & 4));
      tot_locks++;
    }
  }
  T->status++;
  return 1;
}

static int unlock_transaction (transaction_t *T) {
  int i, c;
  assert (T->status <= trs_declared_locked);
  if (!(T->status & 1)) {
    return 0;
  }
  for (i = 0; i < T->parties; i++) {
    long long x = T->T[i].acc_incr;
    account_t *A = T->T[i].tr_account;
    assert (A);   // was: A && x
    c = A->acc_type->acc_class;
    if (x <= 0) {
      assert (x >= -MAX_ACC_INCR);
      assert (!(A->acc_state & 1) && !(c & 1));
      A->lock_num--;
      assert (A->lock_num >= 0 && (A->balance >= A->locked || (c & 4)));
      A->locked += x;
      assert (A->locked >= 0);
      tot_locks--;
    }
  }
  T->status--;
  return 1;
}

static void dump_transaction (transaction_t *T) {
  int i, N;
  fprintf (stderr, "*** DUMPING TRANSACTION %p ***\n", T);
  fprintf (stderr, "trans_id=%lld, date=%d, declared_date=%d, temp_id=%d, status=%d\n",
    T->transaction_id, T->created_at, T->declared_date, T->temp_id, T->status);
  N = T->parties;
  for (i = 0; i < N; i++) {
    account_t *A = T->T[i].tr_account;
    fprintf (stderr, "party #%d: acc=%d:%lld incr=%lld currency=%d\n", i+1, A ? A->acc_type->acc_type_id : 0, A ? A->acc_id : 0, T->T[i].acc_incr, A ? A->acc_type->currency : 0);
  }
  if (T->comment_len) {
    fprintf (stderr, "COMMENT: %.*s\n", T->comment_len, T->comment);
  }
}

// ALWAYS execute check_transaction() before writing commit_transaction into binlog
static int commit_transaction (struct lev_money_trans_commit *E) {
  int i, j, k = 0, c;
  static long long CV[16];
  static int CC[16];
  transaction_t *T = get_transaction (E->transaction_id);
  assert (E->transaction_id && T);
  if (T->status == trs_long_locked) {
    assert (E->temp_id == -2 && !get_temp_transaction (E->temp_id));
    assert (T->long_lock_heap_pos && T->long_locked_until);
    remove_from_lock_heap (T);
    T->long_lock_heap_pos = E->temp_id;
    T->status = trs_declared_locked;
  } else {
    assert (E->temp_id == T->temp_id && E->temp_id > 0 && T == get_temp_transaction (E->temp_id));
    assert (T->status == trs_declared || T->status == trs_declared_locked);
    assert (!T->long_lock_heap_pos && !T->long_locked_until);
  }
  T->long_locked_until = now;
  unlock_transaction (T);
  for (i = 0; i < T->parties; i++) {
    long long x = T->T[i].acc_incr;
    account_t *A = T->T[i].tr_account;
    assert (A);	  // was: A && x
    c = A->acc_type->acc_class;
    if (x > 0) {
      assert (x < MAX_ACC_INCR);
      assert (!((A->acc_state & 2) || (c & 2)));
      A->balance += x;
      assert (A->balance < MAX_BALANCE);
      currency_sum_pos[A->acc_type->currency-MIN_CURRENCY_ID] += x;
    } else {
      assert (x >= -MAX_ACC_INCR);
      assert (!((A->acc_state & 1) || (c & 1)));
      A->balance += x;
      assert (A->balance >= -MAX_BALANCE && A->locked >= 0 && (A->balance >= A->locked || (c & 4)));
    }
    if (A->acc_id == special_acc_id && special_acc_id) {
      fprintf (stderr, "CHANGE: %d:%lld += %lld, BALANCE=%lld\n", A->acc_type->acc_type_id, A->acc_id, x, A->balance);
      dump_transaction (T);
    }
    c = A->acc_type->currency;
    for (j = 0; j < k; j++) {
      if (c == CC[j]) {
        CV[j] += x;
        break;
      }
    }
    if (j == k) {
      CC[k] = c;
      CV[k++] = x;
    }
    currency_operations[c-MIN_CURRENCY_ID]++;
    committed_operations++;
  }

  for (j = 0; j < k; j++) {
    assert (!CV[j]);
  }

  T->status = trs_committed;
  if (E->temp_id > 0) {
    remove_temp_transaction (T);
  } else {
    committed_long_transactions++;
  }
  committed_transactions++;

  return 1;
}

static int cancel_transaction (struct lev_money_trans_cancel *E) {
  transaction_t *T = get_transaction (E->transaction_id);
  assert (E->transaction_id && T);
  if (T->status == trs_long_locked) {
    assert (E->temp_id < 0 && E->temp_id >= -3 && !get_temp_transaction (E->temp_id));
    assert (T->long_lock_heap_pos && T->long_locked_until);
    remove_from_lock_heap (T);
    T->long_lock_heap_pos = E->temp_id;
    T->status = trs_declared_locked;
  } else {
    assert (E->temp_id == T->temp_id && E->temp_id > 0);
    assert (T == get_temp_transaction (E->temp_id));
    assert (T->status == trs_declared || T->status == trs_declared_locked);
    assert (!T->long_lock_heap_pos && !T->long_locked_until);
  }
  T->long_locked_until = now;
  unlock_transaction (T);

  T->status = trs_cancelled;
  if (E->temp_id > 0) {
    remove_temp_transaction (T);
  } else if (E->temp_id == -3) {
    expired_long_transactions++;
  } else {
    cancelled_long_transactions++;
  }
  cancelled_transactions++;

  return 1;
}

static int long_lock_transaction (struct lev_money_trans_lock *E) {
  transaction_t *T = get_transaction (E->transaction_id);
  assert (E->transaction_id && T);
  assert (E->temp_id == T->temp_id && E->temp_id && T == get_temp_transaction (E->temp_id));
  assert (T->status == trs_declared || T->status == trs_declared_locked);

  assert (!T->long_locked_until && !T->long_lock_heap_pos);
  assert (E->lock_seconds >= MIN_LOCK_SECONDS && E->lock_seconds <= MAX_LOCK_SECONDS);

  assert (check_transaction (T) >= 0);

  lock_transaction (T);
  assert (T->status == trs_declared_locked);

  T->status = trs_long_locked;
  remove_temp_transaction (T);
  
  T->long_locked_until = now + E->lock_seconds;
  T->long_lock_cancel_timeout = 0;
  T->long_lock_secret = E->lock_secret;

  put_into_lock_heap (T);

  return 1;
}

static int check_cancellation_possibility (transaction_t *T) {
  int i;
  if (!T || T->status != trs_committed || T->tr_cancel_peer) {
    return -1;
  }
  for (i = 0; i < T->parties; i++) {
    long long x = -T->T[i].acc_incr;
    account_t *A = T->T[i].tr_account;
    if (!A) {
      return -32 + i;
    }
    int c = A->acc_type->acc_class;
    if (x >= 0) {
      assert (x < MAX_ACC_INCR);
      if ((A->acc_state & 1) || (c & 1)) {
        return -32 + i;
      }
      if (A->balance + x >= MAX_BALANCE) {
        return -1;
      }
    } else {
      assert (x >= -MAX_ACC_INCR);
      if ((A->acc_state & 2) || (c & 2)) {
        return -32 + i;
      }
      if (A->balance + x < A->locked && !(c & 4)) {
        return -48 + i;
      }
      if (A->balance + x < -MAX_BALANCE || A->locked - x >= MAX_BALANCE) {
        return -1;
      }
    }
  }
  return 1;
}

static int cancel_committed (struct lev_money_cancel_committed *E) {
  int i, j, k = 0, c;
  static long long CV[16];
  static int CC[16];
  transaction_t *T = get_transaction (E->original_transaction_id), *NT;

  assert (E->original_transaction_id && T && E->new_transaction_id && !get_transaction (E->new_transaction_id));
  assert (T->status == trs_committed);

  assert (check_cancellation_possibility (T) == 1);

  NT = alloc_new_transaction (T->parties);
  NT->transaction_id = E->new_transaction_id;
  
  NT->parties = T->parties;
  NT->declared_date = E->cancel_time;
  NT->ip = E->ip;
  NT->comment_len = E->comm_len;
  if (E->comm_len) {
    NT->comment = zmalloc (E->comm_len + 1);
    memcpy (NT->comment, E->comment, E->comm_len + 1);
  }
  for (i = 0; i < T->parties; i++) {
    NT->T[i].acc_incr = -T->T[i].acc_incr;
    NT->T[i].auth_code = MASTER_CANCEL_MEMORY_CODE;
    account_t *A = NT->T[i].tr_account = T->T[i].tr_account;
    add_acc_transaction (A, NT);
  }
  add_transaction (NT);
  tot_transactions++;

  NT->tr_cancel_peer = T;
  T->tr_cancel_peer = NT;

  assert (NT->transaction_id > T->transaction_id);

  for (i = 0; i < NT->parties; i++) {
    long long x = NT->T[i].acc_incr;
    account_t *A = NT->T[i].tr_account;
    assert (A);   // was: A && x
    c = A->acc_type->acc_class;
    if (x >= 0) {  // here 0 turns out to be positive!
      assert (x < MAX_ACC_INCR);
      assert (!((A->acc_state & 1) || (c & 1)));
      A->balance += x;
      assert (A->balance < MAX_BALANCE);
      currency_sum_pos[A->acc_type->currency-MIN_CURRENCY_ID] += x;
    } else {
      assert (x >= -MAX_ACC_INCR);
      assert (!((A->acc_state & 2) || (c & 2)));
      A->balance += x;
      assert (A->balance >= -MAX_BALANCE && A->locked >= 0 && (A->balance >= A->locked || (c & 4)));
    }
    if (A->acc_id == special_acc_id && special_acc_id) {
      fprintf (stderr, "CHANGE: %d:%lld += %lld, BALANCE=%lld\n", A->acc_type->acc_type_id, A->acc_id, x, A->balance);
      dump_transaction (NT);
    }
    c = A->acc_type->currency;
    for (j = 0; j < k; j++) {
      if (c == CC[j]) {
        CV[j] += x;
        break;
      }
    }
    if (j == k) {
      CC[k] = c;
      CV[k++] = x;
    }
    currency_operations[c-MIN_CURRENCY_ID]++;
    committed_operations++;
  }

  for (j = 0; j < k; j++) {
    assert (!CV[j]);
  }

  NT->status = trs_committed;
  NT->long_locked_until = now;

  committed_transactions++;
  cancelled_committed_transactions++;
  return 1;
}



int money_replay_logevent (struct lev_generic *E, int size) {
  int s, t;
  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return money_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_MONEY_NEW_ATYPE:
    if (size < sizeof (struct lev_money_new_atype)) { return -2; }
    s = ((struct lev_money_new_atype *) E)->comm_len;
    if (s < 0 || s > 4095) { return -4; }
    s += 1 + offsetof (struct lev_money_new_atype, comment);
    if (size < s) { return -2; }
    create_account_type ((struct lev_money_new_atype *) E);
    return s;
  case LEV_MONEY_NEW_ACC:
    if (size < sizeof (struct lev_money_new_acc)) { return -2; }
    s = ((struct lev_money_new_acc *) E)->comm_len;
    if (s < 0 || s > 4095) { return -4; }
    s += 1 + offsetof (struct lev_money_new_acc, comment);
    if (size < s) { return -2; }
    create_account ((struct lev_money_new_acc *) E, 1);
    return s;
  case LEV_MONEY_NEW_ACC_SHORT:
    if (size < sizeof (struct lev_money_new_acc_short)) { return -2; }
    create_account ((struct lev_money_new_acc *) E, 0);
    return sizeof (struct lev_money_new_acc_short);
  case LEV_MONEY_TRANS_DECLARE:
    if (size < offsetof (struct lev_money_trans_declare, T)) { return -2; }
    s = ((struct lev_money_trans_declare *) E)->comm_len;
    t = ((struct lev_money_trans_declare *) E)->parties;
    if (s < 0 || s > 4095 || t < 2 || t > 10) { return -4; }
    if (s) { s++; }
    s = offsetof (struct lev_money_trans_declare, T) + t * sizeof (struct lev_one_transaction) + s;
    if (size < s) { return -2; }
    declare_transaction ((struct lev_money_trans_declare *) E);
    return s;
  case LEV_MONEY_TRANS_COMMIT:
    if (size < sizeof (struct lev_money_trans_commit)) { return -2; }
    commit_transaction ((struct lev_money_trans_commit *) E);
    return sizeof (struct lev_money_trans_commit);
  case LEV_MONEY_TRANS_CANCEL:
    if (size < sizeof (struct lev_money_trans_cancel)) { return -2; }
    cancel_transaction ((struct lev_money_trans_cancel *) E);
    return sizeof (struct lev_money_trans_cancel);
  case LEV_MONEY_TRANS_LOCK:
    if (size < sizeof (struct lev_money_trans_lock)) { return -2; }
    long_lock_transaction ((struct lev_money_trans_lock *) E);
    return sizeof (struct lev_money_trans_lock);
  case LEV_MONEY_CANCEL_COMMITTED:
    if (size < sizeof (struct lev_money_cancel_committed)) { return -2; }
    s = ((struct lev_money_cancel_committed *) E)->comm_len;
    if (s < 0 || s > 4095) { return -4; }
    if (s) { s++; }
    s += offsetof (struct lev_money_cancel_committed, comment);
    if (size < s) { return -2; }
    cancel_committed ((struct lev_money_cancel_committed *) E);
    return s;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -1;
}

/* adding new log entries */

static int check_signature (char auth_signature[32], char *temp, int L, money_auth_code_t code);

int create_temp_transaction (struct lev_money_trans_declare *E) {
  struct account_type *AT;
  transaction_t *T;
  int i, j, k = 0;
  static long long CV[16];
  static int CC[16];

  if (E->temp_id <= 0 || E->declared_date > now + 1 || E->declared_date < now - 20) {
    return -1;
  }
  if ((unsigned) E->comm_len > 4095) {
    return -1;
  }
  if (E->parties < 2 || E->parties > 10) {
    return -1;
  }

  if (get_temp_transaction (E->temp_id)) {
    return -1;
  }
  if (E->transaction_id && get_transaction (E->transaction_id)) {
    return -1;
  }

  for (i = 0; i < E->parties; i++) {
    if ((unsigned) E->T[i].acc_type_id > MAX_ACCOUNT_TYPE) {
      return -1;
    }
    AT = AccTypes[E->T[i].acc_type_id];
    if (!AT) {
      return -1;
    }
    if (AT->currency != E->T[i].currency) {
      return -1;
    }
    if (E->T[i].acc_incr < -MAX_ACC_INCR || E->T[i].acc_incr >= MAX_ACC_INCR /* || !E->T[i].acc_incr */) {
      return -1;
    }
    for (j = 0; j < i; j++) {
      if (E->T[j].acc_id == E->T[i].acc_id && E->T[j].acc_type_id == E->T[i].acc_type_id) {
        return -1;
      }
    }
    int c = AT->currency;
    for (j = 0; j < k; j++) {
      if (c == CC[j]) {
        CV[j] += E->T[i].acc_incr;
        break;
      }
    }
    if (j == k) {
      CC[k] = c;
      CV[k++] = E->T[i].acc_incr;
    }
  }
  
  for (j = 0; j < k; j++) {
    if (CV[j]) {
      return -1;
    }
  }

  T = zmalloc0 (offsetof (transaction_t, T) + sizeof (struct transaction_party) * E->parties);

  T->transaction_id = E->transaction_id;
  T->auth_code = E->auth_code;
  T->unlock_next = DelayQueues[(dq + 15) & (DELAY_QUEUES - 1)];
  DelayQueues[(dq + 15) & (DELAY_QUEUES - 1)] = T;

  T->status = trs_temp;
  T->temp_id = E->temp_id;
  T->created_at = now;
  T->locked_until = dq + 15;
  T->parties = E->parties;
  T->declared_date = E->declared_date;
  T->ip = E->ip;
  T->comment_len = E->comm_len;
  if (E->comm_len) {
    T->comment = zmalloc (E->comm_len + 1);
    memcpy (T->comment, &E->T[E->parties], E->comm_len + 1);
  } else {
    T->comment = 0;
  }

  for (i = 0; i < E->parties; i++) {
    T->T[i].tr_account = get_account (E->T[i].acc_type_id, E->T[i].acc_id);
    T->T[i].acc_incr = E->T[i].acc_incr;
    T->T[i].auth_code = E->T[i].auth_code;
  }

  add_temp_transaction (T);
  temp_transactions++;

  return 1;
}

int do_create_account (int acc_type_id, long long acc_id, money_auth_code_t auth_code, int owner, unsigned ip, 
                         money_auth_code_t access_code, money_auth_code_t withdraw_code, 
                         char *comment, int comm_len) {
  struct account_type *AT;
  account_t *A; 
  struct lev_money_new_acc *E;

  if (verbosity > 1) {
    fprintf (stderr, "do_create_account(): acc_type_id=%d, acc_id=%lld, owner=%d, comment=%.*s\n", acc_type_id, acc_id, owner, (comm_len < 32) ? comm_len : 32, comment);
  }

  if (acc_type_id <= 0 || acc_type_id > MAX_ACCOUNT_TYPE) {
    return -1;
  }

  AT = AccTypes[acc_type_id];
  if (!AT) {
    return -1;
  }

  A = get_account (acc_type_id, acc_id);
  if (A) {
    return -1;
  }

  if ((AT->acc_class && !AT->create_code) || (AT->create_code != auth_code && AT->create_code)) {
    return -1;
  }

  if (AT->acc_class && !withdraw_code) {
    return -1;
  }

  if (AT->acc_class && !comm_len) {
    return -1;
  }

  if (comm_len < 0 || comm_len > 4095) {
    return -1;
  }

  if (!access_code && !withdraw_code && !comm_len) {
    E = alloc_log_event (LEV_MONEY_NEW_ACC_SHORT, sizeof (struct lev_money_new_acc_short), 0);
  } else {
    E = alloc_log_event (LEV_MONEY_NEW_ACC, offsetof (struct lev_money_new_acc, comment) + comm_len + 1, 0);
    E->access_code = access_code;
    E->withdraw_code = withdraw_code;
    E->comm_len = comm_len;
    if (comm_len) {
      memcpy (E->comment, comment, comm_len + 1);
    } else {
      E->comment[0] = 0;
    }
  }

  E->currency = AT->currency;
  E->acc_type_id = acc_type_id;
  E->acc_id = acc_id;
  E->owner_id = owner;
  E->ip = ip;
  E->auth_code = auth_code;

  create_account (E, access_code || withdraw_code || comm_len);

  flush_binlog_forced (0);

  return 1;
}

int do_check_transaction (int temp_id) {
  transaction_t *T = get_temp_transaction (temp_id);
  if (!T) {
    return 0;
  }
  int res = check_transaction (T);
  return res < 0 ? res : 3;
}

long long get_next_transaction_id (void) {
  long long x = (1LL << 32) * get_utime (CLOCK_REALTIME);
  if (x <= last_transaction_id) {
    x = last_transaction_id + (lrand48() % 239) + 1;
  }
  assert (!get_transaction (x));
  return (last_transaction_id = x);
}

static int do_declare_transaction (transaction_t *T) {
  assert (T && T->transaction_id && (T->status == trs_temp || T->status == trs_temp_locked));

  int s = T->comment_len, t = T->parties, i;
  struct lev_money_trans_declare *E = alloc_log_event (LEV_MONEY_TRANS_DECLARE, offsetof (struct lev_money_trans_declare, T) + t * sizeof (struct lev_one_transaction) + s + (s > 0), 0);

  E->temp_id = T->temp_id;
  E->declared_date = T->declared_date;
  E->ip = T->ip;
  E->auth_code = T->auth_code;
  E->transaction_id = T->transaction_id;
  E->comm_len = s;
  E->parties = t;

  for (i = 0; i < t; i++) {
    struct lev_one_transaction *EP = E->T + i;
    account_t *A = T->T[i].tr_account;
    EP->currency = A->acc_type->currency;
    EP->acc_type_id = A->acc_type->acc_type_id;
    EP->acc_id = A->acc_id;
    EP->acc_incr = T->T[i].acc_incr;
    EP->auth_code = T->T[i].auth_code;
  }

  if (s) {
    memcpy (E->T + t, T->comment, s + 1);
  }

  assert (declare_transaction (E) == 1);

  add_transaction (T);

  temp_transactions--;
  tot_transactions++;

  return 1;
}


// buffer at least 256 bytes long (long long)
int do_commit_transaction (int temp_id, long long *transaction_id, char buffer[256]) {
  transaction_t *T = get_temp_transaction (temp_id);
  *transaction_id = 0;
  *buffer = 0;
  if (!T) {
    return 0;
  }
  int res = check_transaction (T);
  if (res < 0) {
    return res;
  }
  if (T->status != trs_temp && T->status != trs_temp_locked) {
    return -1;
  }
  if (!T->transaction_id) {
    T->transaction_id = get_next_transaction_id();
  }

  assert (do_declare_transaction (T) == 1);

  struct lev_money_trans_commit *EC = alloc_log_event (LEV_MONEY_TRANS_COMMIT, sizeof (struct lev_money_trans_commit), temp_id);

  EC->temp_id = T->temp_id;
  EC->transaction_id = T->transaction_id;

  assert (commit_transaction (EC) == 1);

  add_temp_transaction (T);

  *transaction_id = T->transaction_id;

  sprintf (buffer, "%16llx_%08x%08x", T->transaction_id, T->declared_date, T->temp_id);

  flush_binlog_forced (0);

  return 1;
}

int do_lock_transaction (int temp_id) {
  transaction_t *T = get_temp_transaction (temp_id);
  if (!T) {
    return 0;
  }
  int res = check_transaction (T);
  if (res < 0) {
    return res;
  }
  if (T->status != trs_temp) {
    return 0;
  }
  if (!T->locked_until) {
    assert (!T->unlock_next);
    T->unlock_next = DelayQueues[(dq + 15) & (DELAY_QUEUES-1)];
    DelayQueues[(dq + 15) & (DELAY_QUEUES-1)] = T;
  }
  if (T->locked_until < dq + 15) {
    T->locked_until = dq + 15;
  }

  assert (lock_transaction (T) == 1);
  T->status = trs_temp_locked;

  flush_binlog_forced (0);

  return 2;
}

int delete_temp_transaction (int temp_id) {
  transaction_t *T = get_temp_transaction (temp_id);
  if (!T) {
    return 0;
  }
  if (T->status != trs_temp && T->status != trs_temp_locked) {
    return 0;
  }
  if (T->status == trs_temp_locked) {
    assert (check_transaction (T) >= 0);
    assert (unlock_transaction (T) > 0);
  }
  T->status = trs_deleting;
  if (!T->locked_until) {
    remove_temp_transaction (T);
    assert (!T->h_next);
    T->status = -1;
    zfree (T, offsetof (transaction_t, T) + sizeof (struct transaction_party) * T->parties);
    temp_transactions--;
  } else {
    remove_temp_transaction (T);
    assert (!T->h_next);
  }

  return 1;
}

/* long locks */

void compute_lock_codes (transaction_t *T, money_auth_code_t codes[2]) {
  static char buff[256];
  assert (T->long_lock_secret > 0);
  int L = sprintf (buff, "#Tr%lld\xcc%dXPEH", T->transaction_id, T->long_lock_secret);
  md5 ((unsigned char *) buff, L, (void *) codes);
  codes[0] ^= codes[1];
}

int do_long_lock_transaction (int temp_id, int seconds, long long *transaction_id, money_auth_code_t codes[2]) {
  transaction_t *T = get_temp_transaction (temp_id);
  *transaction_id = 0;
  if (!T || seconds < MIN_LOCK_SECONDS || seconds > MAX_LOCK_SECONDS) {
    return 0;
  }
  int res = check_transaction (T);
  if (res < 0) {
    return res;
  }
  if (T->status != trs_temp && T->status != trs_temp_locked) {
    return -1;
  }
  if (!T->transaction_id) {
    T->transaction_id = get_next_transaction_id();
  }

  assert (do_declare_transaction (T) == 1);

  struct lev_money_trans_lock *EL = alloc_log_event (LEV_MONEY_TRANS_LOCK, sizeof (struct lev_money_trans_lock), temp_id);

  EL->temp_id = T->temp_id;
  EL->transaction_id = T->transaction_id;
  EL->lock_seconds = seconds;
  EL->lock_secret = (lrand48() ^ now ^ T->temp_id) & 0x7fffffff;
  if (!EL->lock_secret) {
    EL->lock_secret++;
  }

  assert (long_lock_transaction (EL) == 1);

  add_temp_transaction (T);

  *transaction_id = T->transaction_id;
  compute_lock_codes (T, codes);

  return 2;
}

int do_long_commit_transaction (long long transaction_id, money_auth_code_t auth_code, char buffer[256]) {
  money_auth_code_t codes[2];
  transaction_t *T = get_transaction (transaction_id);
  if (!T) {
    return 0;
  }
  if (T->status != trs_long_locked) {
    return -1;
  }
  compute_lock_codes (T, codes);
  if (auth_code != codes[0]) {
    return -1;
  }
  int res = check_transaction (T);
  if (res < 0) {
    return res;
  }
  struct lev_money_trans_commit *EC = alloc_log_event (LEV_MONEY_TRANS_COMMIT, sizeof (struct lev_money_trans_commit), -2);

  EC->temp_id = -2;
  EC->transaction_id = T->transaction_id;

  assert (commit_transaction (EC) == 1);

  sprintf (buffer, "%16llx_%08x%08x", T->transaction_id, T->declared_date, T->temp_id);

  flush_binlog_forced (0);

  return 1;
}


int do_long_cancel_transaction (long long transaction_id, money_auth_code_t auth_code) {
  money_auth_code_t codes[2];
  transaction_t *T = get_transaction (transaction_id);
  if (!T) {
    return 0;
  }
  if (T->status != trs_long_locked) {
    return -1;
  }
  compute_lock_codes (T, codes);
  if (auth_code != codes[0] && (auth_code != codes[1] || now < T->long_lock_cancel_timeout)) {
    return -1;
  }
  struct lev_money_trans_cancel *EC = alloc_log_event (LEV_MONEY_TRANS_CANCEL, sizeof (struct lev_money_trans_cancel), -2);

  EC->temp_id = (auth_code == codes[0] ? -2 : -1);
  EC->transaction_id = T->transaction_id;

  assert (cancel_transaction (EC) == 1);

  flush_binlog_forced (0);

  return 3;
}

int long_check_transaction (long long transaction_id, money_auth_code_t auth_code) {
  money_auth_code_t codes[2];
  transaction_t *T = get_transaction (transaction_id);
  if (!T) {
    return 0;
  }
  if (T->status != trs_cancelled && T->status != trs_long_locked && T->status != trs_committed) {
    return 0;
  }
  if (!T->long_lock_secret) {
    return -1;
  }
  compute_lock_codes (T, codes);
  if (auth_code != codes[0] && auth_code != codes[1]) {
    return -1;
  }
  if (T->status == trs_committed) {
    return 1;
  }
  if (T->status == trs_cancelled) {
    return 3;
  }
  if (auth_code == codes[0]) {
    T->long_lock_cancel_timeout = now + LONG_LOCK_CANCEL_TIMEOUT;
  }
  return 2;
}

money_auth_code_t cancel_master_code = 0x932c007b0c705d6bULL;

int do_cancel_committed (long long transaction_id, int sign_time, char auth_signature[32], unsigned ip, char *comment, long long *new_transaction_id) {
  transaction_t *T = get_transaction (transaction_id);
  static char temp[8192];
  int L;

  if (!T) {
    return 0;
  }

  if (sign_time < now - 30 || sign_time > now + 10) {
    return -2;
  }

  L = sprintf (temp, "%lld,%d,%u", transaction_id, sign_time, ip);
  if (!check_signature (auth_signature, temp, L, cancel_master_code)) {
    return -2;
  }

  int res = check_cancellation_possibility (T);
  if (res != 1) {
    return res;
  }

  L = comment ? strlen (comment) : 0;

  if (L > 250) {
    return -1;
  }

  struct lev_money_cancel_committed *EC = alloc_log_event (LEV_MONEY_CANCEL_COMMITTED, offsetof (struct lev_money_cancel_committed, comment) + (L ? L + 1 : 0), sign_time);
  EC->cancel_time = sign_time;
  EC->ip = ip;
  EC->comm_len = L;
  EC->original_transaction_id = transaction_id;
  if (L) {
    memcpy (EC->comment, comment, L+1);
  }
  *new_transaction_id = EC->new_transaction_id = get_next_transaction_id();

  assert (cancel_committed (EC) == 1);

  flush_binlog_forced (0);

  return 1;
}



/* delayed operations */

int scan_delay_queues (void) {
  transaction_t *T, *NT;
  struct lev_money_trans_cancel *EC;
  int x = 0;

  if (dq_now == now) {
    return 0;
  }
  dq_now = now;
  dq++;
  NT = DelayQueues[dq & (DELAY_QUEUES - 1)];
  DelayQueues[dq & (DELAY_QUEUES - 1)] = 0;
  while (NT) {
    T = NT;
    NT = T->unlock_next;
    assert (T->locked_until >= dq && T->locked_until < dq + DELAY_QUEUES);
    if (T->locked_until != dq) {
      T->unlock_next = DelayQueues[T->locked_until & (DELAY_QUEUES - 1)];
      DelayQueues[T->locked_until & (DELAY_QUEUES - 1)] = T;
      continue;
    }
    x++;
    T->unlock_next = 0;
    T->locked_until = 0;

    if (verbosity > 1) {
      fprintf (stderr, "timeout, freeing temp transaction %d (status %d)...\n", T->temp_id, T->status);
    }

    if (T->status == trs_committed || T->status == trs_cancelled || T->status == trs_long_locked) {
      if (T->th_next) {
        remove_temp_transaction (T);
      }
      continue;
    }
    assert (T->status == trs_temp || T->status == trs_temp_locked || T->status == trs_deleting);

    if (T->status != trs_deleting) {
      unlock_transaction (T);
      remove_temp_transaction (T);
    }
    
    assert (!T->h_next);
    zfree (T, offsetof (transaction_t, T) + sizeof (struct transaction_party) * T->parties);
    temp_transactions--;
  }

  if (binlog_disabled) {
    return x;
  }

  while (LHN > 0 && LH[1]->long_locked_until <= now) {
    T = LH[1];
    if (verbosity > 1) {
      fprintf (stderr, "long lock for transaction %p (%lld) expires: %d <= %d\n", T, T->transaction_id, T->long_locked_until, now);
    }
    assert (T->status == trs_long_locked && !T->th_next && T->long_lock_heap_pos == 1);
    EC = alloc_log_event (LEV_MONEY_TRANS_CANCEL, sizeof (struct lev_money_trans_cancel), -3);
    EC->temp_id = -3;
    EC->transaction_id = T->transaction_id;
    assert (cancel_transaction (EC) == 1);
  }

  return x;
}

static int xor_hcyf (int a, int b) {
  if (a >= 'A') { a -= 7; }
  if (b >= 'B') { b -= 7; }
  a = (a ^ b) & 15;
  return a < 10 ? '0' + a : '0' + 0x27 + a;
}

static int check_signature (char auth_signature[32], char *temp, int L, money_auth_code_t code) {
  int i;
  if (!auth_signature) {
    return 0;
  }
  sprintf (temp + L, "%016llx", code);
  md5_hex (temp, L + 16, temp + L + 16);
  vkprintf (1, "in check_signature: md5_hex(%.*s)=%.32s, xor with %.32s\n", L+16, temp, temp+L+16, auth_signature);
  for (i = 0; i < 32; i++) {
    temp[L+16+i] = xor_hcyf (temp[L+16+i], auth_signature[i]);
  }
  md5_hex (temp + L + 16, 32, temp + L + 48);
  vkprintf (1, "xor=%.32s, md5(xor)=%.16s\n", temp+L+16, temp+L+48);
  temp[L+64] = 0;
  return strtoull (temp + L + 48, 0, 16) == code; 
}

money_auth_code_t check_auth_code (char *auth_signature, char *signed_string, int acc_type_id, long long acc_id) {
  account_t *A = get_account (acc_type_id, acc_id);
  int L = strlen (signed_string);
  static char temp[8192];
  if (verbosity > 1) {
    fprintf (stderr, "check_auth_code: signature='%.32s', signed_string='%s', acc_type=%d, acc_id=%lld, secret=%016llx\n", auth_signature, signed_string, acc_type_id, acc_id, A ? A->withdraw_code : 0);
  }
  if (L >= 4096 || (unsigned) acc_type_id > MAX_ACCOUNT_TYPE) {
    return -1;
  }
  memcpy (temp, signed_string, L);
  if (acc_id && A) {
    if (check_signature (auth_signature, temp, L, A->withdraw_code)) {
      return A->withdraw_code;
    }
    if (check_signature (auth_signature, temp, L, A->access_code)) {
      return A->access_code;
    }
  }
  struct account_type *AT = AccTypes[acc_type_id];
  if (!AT) {
    return -1;
  }
  if (check_signature (auth_signature, temp, L, AT->withdraw_code)) {
    return AT->withdraw_code;
  }
  if (check_signature (auth_signature, temp, L, AT->access_code)) {
    return AT->access_code;
  }
  if (check_signature (auth_signature, temp, L, AT->create_code)) {
    return AT->create_code;
  }
  return -1;
}


long long get_balance (int acc_type_id, long long acc_id, money_auth_code_t auth_code, int *currency, long long *locked) {
  if (verbosity > 1) {
    fprintf (stderr, "in get_balance(%d,%lld)\n", acc_type_id, acc_id);
  }

  account_t *A = get_account (acc_type_id, acc_id);
  if (!A) {
    return BALANCE_NO_ACCOUNT;
  }

  if (A->access_code && auth_code != A->access_code) {
    return ERROR_BALANCE;
  }

  *currency = A->acc_type->currency;
  *locked = A->locked;

  return A->balance;
}

static inline void store_transaction_short (long long **RR, transaction_t *T, int flags) {
  *((*RR)++) = T->transaction_id;
  if (flags & 1) {
    *((*RR)++) = trs_deleting - T->status;
  }
  if (flags & 2) {
    *((*RR)++) = T->created_at;
  }
}

int get_account_transactions (int acc_type_id, long long acc_id, int flags, int from, int to, money_auth_code_t auth_code, long long *R, int *R_cnt, int R_size) {
  account_t *A = get_account (acc_type_id, acc_id);
  int N, i;
  long long *RR = R, *R_end;
  if (!A) {
    return -1;
  }
  if (A->access_code && A->access_code != auth_code && A->withdraw_code != auth_code && !(A->acc_type->access_code == auth_code && auth_code)) {
    return -1;
  }
  N = A->trans_num;
  assert (N >= 0);
  *R_cnt = 0;
  if (!from || !to || !N) {
    return N;
  }
  if (from < 0) {
    from += N;
  } else {
    from--;
  }
  if (to < 0) {
    to += N;
  } else {
    to--;
  }

  if (from >= N) {
    from = N - 1;
  }
  if (to >= N) {
    to = N - 1;
  }

  if (from < 0) {
    from = 0;
  }
  if (to < 0) {
    to = 0;
  }

  R_end = R + (R_size - 16);

  if (from <= to) {
    for (i = from; i <= to && R <= R_end; i++) {
      store_transaction_short (&R, A->acc_transactions[i], flags);
    }
  } else {
    for (i = from; i >= to && R <= R_end; i--) {
      store_transaction_short (&R, A->acc_transactions[i], flags);
    }
  }
  *R_cnt = R - RR;
  return N;
}

int get_transaction_data (long long transaction_id, int sign_time, char auth_signature[32], long long *R, int *R_cnt, char **comment) {
  transaction_t *T = get_transaction (transaction_id);
  int i, N, L, ok;
  static char temp[128];
  if (!T) {
    return -1;
  }
  if (T->status <= trs_temp_locked) {
    return -1;
  }
  L = sprintf (temp, "%lld,%d", transaction_id, sign_time);
  N = T->parties;
  ok = 0;
  for (i = 0; i < N; i++) {
    account_t *A = T->T[i].tr_account;
    assert (A);
    if (!A->access_code 
     || check_signature (auth_signature, temp, L, A->access_code) || check_signature (auth_signature, temp, L, A->withdraw_code) 
     || check_signature (auth_signature, temp, L, A->acc_type->access_code) || check_signature (auth_signature, temp, L, A->acc_type->withdraw_code)) {
      ok = 1;
      break;
    }
  }
  if (!ok) {
    return -1;
  }
  R[0] = trs_deleting - T->status;
  R[1] = T->created_at;
  R[2] = T->long_locked_until;
  R[3] = T->long_lock_cancel_timeout;
  R[4] = T->ip;
  R[5] = T->tr_cancel_peer ? T->tr_cancel_peer->transaction_id : 0;
  R[6] = R[7] = 0;
  R += 8;
  for (i = 0; i < N; i++) {
    R[0] = T->T[i].tr_account->acc_type->acc_type_id;
    R[1] = T->T[i].tr_account->acc_id;
    R[2] = T->T[i].acc_incr;
    R[3] = T->T[i].tr_account->acc_type->currency;
    R += 4;
  }
  *R_cnt = 8 + 4*N;
  *comment = T->comment;

  return N;
}

/**************** INDEX FUNCTIONS ****************/

int write_index (int writing_binlog) {
  kprintf ("fatal: cannot write index\n");
  return -1;
}

int load_index (kfs_file_handle_t Index) {
  assert (Index);
  kprintf ("fatal: cannot load index %s\n", Index->info->filename);
  return 7;
}
