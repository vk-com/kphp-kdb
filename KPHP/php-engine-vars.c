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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64
#include <string.h>

#include "php-engine-vars.h"

/***
 DEFAULT GLOBAL VARIABLES
 ***/

const char *full_version_str = NAME_VERSION " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

/** engine variables **/
char *logname_pattern = NULL;
int logname_id = 0;
long long max_memory = 1 << 29;
int http_port;

int worker_id = -1;
int pid = -1;

int no_sql = 0;

int master_flag = 0; // 1 -- master, 0 -- single process, -1 -- child
int workers_n = 0;

int run_once = 0;
int run_once_return_code = 0;

/** stats **/
double load_time;
acc_stats_t worker_acc_stats;

/** http **/
int http_port = -1;
int http_sfd = -1;
struct in_addr settings_addr;
char buf[MAX_VALUE_LEN];

volatile int sigpoll_cnt;

/** rcp **/
long long rpc_failed, rpc_sent, rpc_received, rpc_received_news_subscr, rpc_received_news_redirect;
int rpc_port = -1;
int rpc_sfd = -1;
int rpc_client_port = -1;
const char *rpc_client_host = NULL;
int rpc_client_target = -1;

/** sigterm **/
double sigterm_time = 0;
int sigterm_on = 0;
int rpc_stopped = 0;

/** master **/
const char *cluster_name = "default";
int master_port = -1;
int master_sfd = -1;
int master_sfd_inited = 0;
int master_pipe_write = -1;
int master_pipe_fast_write = -1;

/** script **/
struct php_immediate_stats immediate_stats = {
  .is_running = 0,
  .is_wait_net = 0
};

/***
  GLOBAL VARIABLES
 ***/
int sql_target_id = -1;
int in_ready = 0;
int script_timeout = 30;
