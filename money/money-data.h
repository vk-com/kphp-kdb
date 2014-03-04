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
#ifndef __MONEY_DATA_H__
#define __MONEY_DATA_H__

#include "kdb-data-common.h"
#include "kdb-money-binlog.h"
#include "kfs.h"

#define ERROR_BALANCE	(-1LL << 63)
#define	BALANCE_NO_ACCOUNT	(ERROR_BALANCE + 1)

#define	ACC_HASH_SIZE	(1L << 24)
#define	TRANS_HASH_SIZE	(1L << 26)
#define	TRANS_TEMP_HASH_SIZE	(1L << 20)
#define	DELAY_QUEUES	32

#define	MAX_ACC_INCR	(1LL << 48)
#define	MAX_BALANCE	(1LL << 56)

#define	LOCK_HEAP_SIZE	(1L << 20)

#define	LONG_LOCK_CANCEL_TIMEOUT	3600

#define	MASTER_MODE	1
#define COMPATIBILITY_MODE	0
#define SLAVE_MODE	-1

struct account_type {
  int currency;
  int acc_type_id;
  int acc_class;
  int creator_id;
  money_auth_code_t auth_code;		// authority to create a new type (from create_code of acc_type 0 or -1)
  money_auth_code_t admin_code;		// code to edit codes of this entry
  money_auth_code_t access_code;	// master code to access all accounts of this type
  money_auth_code_t withdraw_code;	// master code to withdraw from all accounts of this type
  money_auth_code_t block_code;		// master code to block all accounts of this type
  money_auth_code_t create_code;	// master code to create accounts of this type
  unsigned ip;
  int comm_len;
  char *comment;
  long long accounts_num;
  long long accounts_total;
};

typedef struct account account_t;

#define	ACCS_BLOCKED_WITHDRAW	1
#define	ACCS_BLOCKED_DEPOSIT	2

//struct transaction;

struct account {
  struct account *h_next;
  struct account_type *acc_type;
  long long acc_id;
  long long balance;
  long long locked;	
  int lock_num;
  int acc_state;
  int owner_id;
  unsigned ip;
  money_auth_code_t auth_code;		// authority to create this account (derived from create_code of corresponding type or type 0) 
  money_auth_code_t access_code;	// code to access this account
  money_auth_code_t withdraw_code;	// code to withdraw from this account
  int trans_num;
  int trans_alloc;
  struct transaction **acc_transactions;
  int comm_len;
  char *comment;
};

struct transaction_party {
  account_t *tr_account;
  int acc_type_id;
  long long acc_id;
  long long acc_incr;
  money_auth_code_t auth_code;
};

#define	TRS_LOCKED	1

enum trans_state {
  trs_temp, 			// has temp_id, but no transaction_id; only in delay+temp queues; no binlog entry
  trs_temp_locked, 		// same, but with locked amounts
  trs_declared, 		// declared in binlog, has transaction_id + temp_id; must be cancelled or committed soon
  trs_declared_locked, 		// same, but with amounts locked
  trs_cancelled, 
  trs_long_locked,		// locked for a long time; may become cancelled or committed afterwards
  trs_committed, 
  trs_deleting
};

typedef struct transaction transaction_t;

struct transaction {
  struct transaction *th_next, *th_prev;	// temp_id hash
  long long transaction_id;
  money_auth_code_t auth_code;
  struct transaction *unlock_next;		// locked_until list
  struct transaction *h_next;			// transaction_id hash
  struct transaction *tr_cancel_peer;
  int status;
  int temp_id;
  int created_at;
  int locked_until;
  int long_locked_until;		// when it is committed/cancelled, time is written here
  int long_lock_secret;			// if/after trs_long_locked, contains some secret
  int long_lock_cancel_timeout;		// if nonzero, unixtime when cancelling by cancel_code will become possible
  int long_lock_heap_pos;		// if cancelled or committed afterwards, contains -1 (cancelled by cancel_code) or -2 (cancelled by commit_code) or -3 (cancelled by timeout)
  int parties;				// 2 <= parties <= 10
  int declared_date;
  unsigned ip;
  int comment_len;
  char *comment;
  struct transaction_party T[MAX_TRANSACTION_PARTIES];
};

int init_money_data (int schema);

extern int master_slave_mode;
extern int split_min, split_max, split_mod;

extern int index_mode;
extern int use_aio;

extern int global_engine_status;
extern long long special_acc_id;

extern int tot_account_types, tot_accounts, frozen_accounts, special_accounts;
extern int tot_transactions, temp_transactions, committed_transactions, cancelled_transactions, committed_operations, cancelled_committed_transactions, locked_long_transactions, tot_locks;

extern long long currency_sum_pos[MAX_CURRENCIES];
extern int currency_accounts[MAX_CURRENCIES];
extern int currency_operations[MAX_CURRENCIES];

int create_temp_transaction (struct lev_money_trans_declare *E);
int do_create_account (int acc_type_id, long long acc_id, money_auth_code_t auth_code, int owner, unsigned ip, 
                         money_auth_code_t access_code, money_auth_code_t withdraw_code, 
                         char *comment, int comm_len);
int do_check_transaction (int temp_id);
int do_lock_transaction (int temp_id);
int do_commit_transaction (int temp_id, long long *transaction_id, char buffer[256]);
int delete_temp_transaction (int temp_id);

int do_long_lock_transaction (int temp_id, int seconds, long long *transaction_id, money_auth_code_t codes[2]);
int do_long_commit_transaction (long long transaction_id, money_auth_code_t auth_code, char buffer[256]);
int do_long_cancel_transaction (long long transaction_id, money_auth_code_t auth_code);
int long_check_transaction (long long transaction_id, money_auth_code_t auth_code);

int do_cancel_committed (long long transaction_id, int sign_time, char auth_signature[32], unsigned ip, char *comment, long long *new_transaction_id);

money_auth_code_t check_auth_code (char *auth_signature, char *signed_string, int acc_type_id, long long acc_id);

long long get_balance (int acc_type_id, long long acc_id, money_auth_code_t auth_code, int *currency, long long *locked);

int get_account_transactions (int acc_type_id, long long acc_id, int flags, int from, int to, money_auth_code_t auth_code, long long *R, int *R_cnt, int R_size);
int get_transaction_data (long long transaction_id, int sign_time, char auth_signature[32], long long *R, int *R_cnt, char **comment);

int scan_delay_queues (void);

int write_index (int writing_binlog);
int load_index (kfs_file_handle_t Index);


#endif
