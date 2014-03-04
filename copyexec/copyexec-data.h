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

#ifndef __KDB_COPYEXEC_DATA_H__
#define __KDB_COPYEXEC_DATA_H__

#include "kdb-copyexec-binlog.h"
#include "kfs.h"
#include "copyexec-err.h"

extern char *public_key_prefix;
extern char *tmp_dir;
extern int instance_mask, first_transaction_id;
extern long long aux_log_readto_pos, aux_log_read_start;
extern int tot_ignored, tot_interrupted, tot_cancelled, tot_terminated, tot_failed, tot_decryption_failed, tot_io_failed;
extern int fd_aux_binlog;

extern int transactions, tot_memory_transactions, transaction_running_queue_size;
extern unsigned long long main_volume_id, aux_volume_id, random_tag;
extern int sfd;

extern pid_t main_pid, results_client_pid;
extern int main_process_creation_time, results_client_creation_time;

typedef struct {
  long long binlog_pos;
  unsigned char *input;
  int ilen;
  int key_id;
  int transaction_id;
} replay_transaction_info_t;

void reopen_logs (void);
void get_running_lists_size (int *child_size, int *auto_size);

void set_sigusr1_handler (void);
void set_sigusr2_handler (void);

void change_process_name (char *new_name);
void copyexec_main_process_init (void);
int get_process_creation_time (pid_t pid);
int interrupted_by_signal (void);
void copyexec_main_sig_handler (const int sig);
void check_superuser (void);
void check_superuser_main_binlog (void);
void copy_argv (int argc, char *argv[]); /* for changing process cmd in ps */

int check_mask (int mask);
void do_set_status (struct lev_copyexec_main_transaction_status *E);

int transaction_check_child_status (void);
int transaction_check_auto_status (void);
int transaction_child_kill (int sig);
int transaction_auto_kill (int sig);
int find_running_transactions (void);

void exec_transaction (replay_transaction_info_t *T);

int copyexec_main_le_start (struct lev_start *E);

int copyexec_aux_binlog_readonly_open (const char *const aux_binlog_name);
void copyexec_aux_binlog_seek (void);
int copyexec_aux_replay_binlog (long long start_pos, void (*fp_replay_transaction)(replay_transaction_info_t *));
int aux_load_index (kfs_file_handle_t Index);
int aux_save_index (int writing_binlog);
void make_empty_binlog (const char *binlog_name, const char *const volume_name, int schema_id, const void *const lev_start_extra, int lev_start_extra_len, const void *const extra_logevents, int extra_len);
int find_last_synchronization_point (void);

#endif

