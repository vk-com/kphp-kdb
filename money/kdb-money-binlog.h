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

    Copyright 2010 Vkontakte Ltd
              2010 Nikolai Durov
              2010 Andrei Lopatin
*/
#ifndef __KDB_MONEY_BINLOG_H__
#define __KDB_MONEY_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	MONEY_SCHEMA_BASE
#define	MONEY_SCHEMA_BASE	0xf00d0000
#endif

#define	MONEY_SCHEMA_V1	0xf00d0101

#define LEV_MONEY_NEW_ACC		0x39e4fdcc
#define LEV_MONEY_NEW_ACC_SHORT		0x382301d1
#define LEV_MONEY_NEW_ATYPE		0x6d01cefa
#define	LEV_MONEY_TRANS_DECLARE		0x1844b12c
#define	LEV_MONEY_TRANS_COMMIT		0x2dacd00d
#define	LEV_MONEY_TRANS_CANCEL		0x5196beda
#define	LEV_MONEY_TRANS_LOCK		0x4284fb30
#define	LEV_MONEY_CANCEL_COMMITTED	0x70cc3ae6
//#define LEV_MONEY_CHANGE_ACC_CODES	0x4f6384ae

#define	MAX_LOCK_SECONDS		(30*86400)
#define	MIN_LOCK_SECONDS		60

typedef	unsigned long long money_auth_code_t;

struct lev_money_new_acc {
  lev_type_t type;
  short currency;
  short acc_type_id;
  long long acc_id;
  int owner_id;
  unsigned ip;
  money_auth_code_t auth_code;		// authority to create this account (derived from create_code of corresponding type or type 0) 
  money_auth_code_t access_code;	// code to access this account
  money_auth_code_t withdraw_code;	// code to withdraw from this account
  short comm_len;
  char comment[1];
};

struct lev_money_new_acc_short {
  lev_type_t type;
  short currency;
  short acc_type_id;
  long long acc_id;
  int owner_id;
  unsigned ip;
  money_auth_code_t auth_code;
};

/* if redeclared, only comment and codes may change; must authorise with previous admin_code */
struct lev_money_new_atype {
  lev_type_t type;
  short currency;
  short acc_type_id;
  int acc_class;
  int creator_id;
  money_auth_code_t auth_code;		// authority to create a new type (from create_code of acc_type 0 or -1)
  money_auth_code_t admin_code;		// code to edit codes of this entry
  money_auth_code_t access_code;	// master code to access all accounts of this type
  money_auth_code_t withdraw_code;	// master code to withdraw from all accounts of this type
  money_auth_code_t block_code;		// master code to block all accounts of this type
  money_auth_code_t create_code;	// master code to create accounts of this type
  unsigned ip;
  short comm_len;
  char comment[1];
};

struct lev_one_transaction {
  short currency;
  short acc_type_id;
  long long acc_id;
  long long acc_incr;
  long long auth_code;
};

struct lev_money_trans_declare {
  lev_type_t type;
  int temp_id;
  int declared_date;
  unsigned ip;
  money_auth_code_t auth_code;
  long long transaction_id;
  int comm_len;
  int parties;				// 2 <= parties <= 10
  struct lev_one_transaction T[10];	// T[parties]
  // comm_len + 1 bytes follow, containing transaction comment if comm_len > 0
};

/* ANY transaction declaration must be followed by trans_cancel, trans_commit or trans_lock soon;
   in the latter case it can be followed by trans_cancel or trans_commit with temp_id = -1 or -2 afterwards */

struct lev_money_trans_cancel {
  lev_type_t type;
  int temp_id;
  long long transaction_id;
};

struct lev_money_trans_commit {
  lev_type_t type;
  int temp_id;
  long long transaction_id;
};

struct lev_money_trans_lock {
  lev_type_t type;
  int temp_id;
  long long transaction_id;
  int lock_seconds;
  int lock_secret;
};

struct lev_money_cancel_committed {
  lev_type_t type;
  int cancel_time;
  long long new_transaction_id;
  long long original_transaction_id;
  unsigned ip;
  int comm_len;
  char comment[1];
};

#define	MIN_CURRENCY_ID	-1000
#define	MAX_CURRENCY_ID	1000
#define	MAX_CURRENCIES	(MAX_CURRENCY_ID-MIN_CURRENCY_ID+1)
#define	MAX_ACCOUNT_TYPE	(27 * 27 * 27 - 1)
#define	MAX_TRANSACTION_PARTIES	10

#pragma	pack(pop)

#endif
