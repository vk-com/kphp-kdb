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

#ifndef __KDB_COPYEXEC_RESULT_DATA_H__
#define __KDB_COPYEXEC_RESULT_DATA_H__

#include "net-connections.h"

#define COPYEXEC_RESULT_IP_CHANGED 1
#define COPYEXEC_RESULT_PID_CHANGED 2
#define COPYEXEC_RESULT_NEW_HOST 3

typedef struct host {
  unsigned long long volume_id;
  unsigned long long random_tag;
  long long binlog_pos;
  char *hostname;
  unsigned ip;
  int host_id;
  int generation;
  int pid;
  int first_data_time, last_data_time;
  int last_action_time;
  int disabled;
} host_t;

extern int hosts, tot_memory_transactions;
extern int lru_size, max_lru_size;
extern int alloc_tree_nodes, free_tree_nodes;

host_t *get_host_by_connection (struct connection *c);
int do_connect (struct connection *c, unsigned long long volume_id, unsigned long long random_tag, const char *const hostname, int pid, host_t **R);
int do_set_result (struct connection *c, int transaction_id, unsigned result, long long binlog_pos);
int do_set_enable (unsigned long long random_tag, int enable);

/* get_* functions: returned NUL terminzated string should be deallocated using free function */
char *get_status_freqs (unsigned long long volume_id, int transaction_id);
char *get_results_freqs (unsigned long long volume_id, int transaction_id);
char *get_status_results_freqs (unsigned long long volume_id, int transaction_id);
char *get_hosts_list (unsigned long long volume_id, int transaction_id, unsigned result_or, unsigned result_and);
char *get_hosts_list_by_status (unsigned long long volume_id, int transaction_id, const char *const status);
char *get_hosts_list_by_status_and_result (unsigned long long volume_id, int transaction_id, const char *const status, unsigned result);
char *get_dead_hosts_list (unsigned long long volume_id, int delay);
char *get_dead_hosts_list_full (unsigned long long volume_id, int delay);
char *get_volumes (void);
char *get_collisions_list (void);
char *get_disabled (unsigned long long volume_id);
#endif

