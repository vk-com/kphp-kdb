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

#ifndef __KDB_COPYEXEC_BINLOG_H__
#define __KDB_COPYEXEC_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	COPYEXEC_AUX_SCHEMA_BASE
#define	COPYEXEC_AUX_SCHEMA_BASE	0x29790000
#endif

#ifndef	COPYEXEC_MAIN_SCHEMA_BASE
#define	COPYEXEC_MAIN_SCHEMA_BASE	0xeda90000
#endif

#ifndef	COPYEXEC_RESULT_SCHEMA_BASE
#define	COPYEXEC_RESULT_SCHEMA_BASE	0xedaa0000
#endif

#define	COPYEXEC_AUX_SCHEMA_V1	0x29790101
#define	COPYEXEC_MAIN_SCHEMA_V1	0xeda90101
#define	COPYEXEC_RESULT_SCHEMA_V1	0xedaa0101

/******************************** aux binlog logevents **********************************************************/

#define LEV_COPYEXEC_AUX_TRANSACTION 0xd6092da7
#define LEV_COPYEXEC_AUX_TRANSACTION_HEADER 0x9d92f4a4
#define LEV_COPYEXEC_AUX_TRANSACTION_FOOTER 0x6cbd68b0
#define LEV_COPYEXEC_AUX_TRANSACTION_CMD_FILE 0x6af155fe
#define LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC 0x60f59866
#define LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC_CHECK 0x60f59867
#define LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC_RESULT 0x60f59868
#define LEV_COPYEXEC_AUX_TRANSACTION_CMD_WAIT 0x7c08808e
#define LEV_COPYEXEC_AUX_TRANSACTION_CMD_KILL 0x85e1821e
#define LEV_COPYEXEC_AUX_TRANSACTION_CMD_CANCEL 0x4c434e43
#define LEV_COPYEXEC_AUX_TRANSACTION_CMD_RESULT 0x820bffb4
#define LEV_COPYEXEC_AUX_TRANSACTION_SYNCHRONIZE 0x434e5953

#define MASK_RERUN_TRANSACTION 0x80000000
#define MASK_IMPORTANT_TRANSACTION 0x40000000
#define MASK_WAITING_TRANSACTION 0x20000000

struct lev_copyexec_aux_transaction {
  lev_type_t type;
  unsigned key_id;
  unsigned size; /* crypted size */
  unsigned long long crypted_body_crc64;
  unsigned crc32;
};

struct lev_copyexec_aux_transaction_header {
  /* this log event hasn't type, purpose to make aes-key attacks more complex */
  int transaction_id;
  long long binlog_pos;
  unsigned time;
  unsigned size;
  int mask;
};

struct lev_copyexec_aux_transaction_footer {
  unsigned char sha1[20];
};

struct lev_copyexec_aux_transaction_cmd_exec {
  lev_type_t type;
  unsigned command_size;
  char data[0];
};

struct lev_copyexec_aux_transaction_cmd_result {
  lev_type_t type;
  int result;
};

struct lev_copyexec_aux_transaction_cmd_file {
  lev_type_t type;
  int mode;
  unsigned short uid;
  unsigned short gid;
  unsigned actime;   /* access time */
  unsigned modtime;  /* modification time */
  unsigned compressed_size;
  unsigned size;
  unsigned compressed_crc32;
  unsigned crc32;
  unsigned short filename_size;
  unsigned short reserved;
  char data[0]; /* filename + gz compressed stream */
};

struct lev_copyexec_aux_transaction_cmd_wait {
  lev_type_t type;
  int transaction_id;
};

struct lev_copyexec_aux_transaction_cmd_kill {
  lev_type_t type;
  int transaction_id;
  int signal;
};

/******************************** main binlog logevents **********************************************************/
#define LEV_COPYEXEC_MAIN_TRANSACTION_STATUS 0x54535400 //.TST
#define LEV_COPYEXEC_MAIN_COMMAND_BEGIN 0x47454243 //CBEG
#define LEV_COPYEXEC_MAIN_COMMAND_END 0x444e4543 //CEND
#define LEV_COPYEXEC_MAIN_TRANSACTION_ERROR 0x52524554 //TERR
#define LEV_COPYEXEC_MAIN_COMMAND_WAIT 0x54494157 //WAIT
#define LEV_COPYEXEC_MAIN_COMMAND_KILL 0x4c4c494b //KILL
#define LEV_COPYEXEC_MAIN_TRANSACTION_SKIP 0x50494b53 //SKIP

enum transaction_status {
  ts_unset = 0,
  ts_running = 1,
  ts_ignored = 2,
  ts_interrupted = 3,
  ts_cancelled = 4,
  ts_terminated = 5,
  ts_failed = 6,
  ts_io_failed = 7,
  ts_decryption_failed = 8,
};

struct lev_copyexec_main_transaction_status {
  lev_type_t type;
  int transaction_id;
  long long binlog_pos;
  int mask;
  int pid;
  int creation_time;
  int result;
  long long st_dev;
  long long st_ino;
};

struct lev_copyexec_main_transaction_err {
  lev_type_t type;
  int transaction_id;
  int error_msg_size;
  char data[0];
};

struct lev_copyexec_main_command_wait {
  lev_type_t type;
  int waiting_transaction_id;
  int working_transaction_id;
};

struct lev_copyexec_main_command_kill {
  lev_type_t type;
  int waiting_transaction_id;
  int working_transaction_id;
  int signal;
};

struct lev_copyexec_main_command_begin {
  lev_type_t type;
  int transaction_id;
  int command_id;
  short pid;
  short command_size;
  char data[0];
};

struct lev_copyexec_main_command_end {
  lev_type_t type;
  int transaction_id;
  int command_id;
  int pid;
  int status;
  int stdout_size;
  int stderr_size;
  unsigned short saved_stdout_size;
  unsigned short saved_stderr_size;
  char data[0];
};

struct lev_copyexec_main_transaction_skip {
  lev_type_t type;
  int first_transaction_id;
};

/**************************************** Result binlog logevents *****************************************/
#define LEV_COPYEXEC_RESULT_CONNECT 0x42686903
#define LEV_COPYEXEC_RESULT_DATA 0xed7dddf9
#define LEV_COPYEXEC_RESULT_DISABLE 0x90145583
#define LEV_COPYEXEC_RESULT_ENABLE 0x90145584

struct lev_copyexec_result_connect {
  lev_type_t type;
  unsigned long long random_tag;
  unsigned long long volume_id;
  unsigned ip;
  int hostname_length;
  int pid;
  char hostname[0];
};

struct lev_copyexec_result_data {
  lev_type_t type;
  unsigned long long random_tag;
  int transaction_id;
  unsigned result;
  long long binlog_pos;
};

struct lev_copyexec_result_enable {
  lev_type_t type;
  unsigned long long random_tag;
};
#pragma	pack(pop)

#endif


