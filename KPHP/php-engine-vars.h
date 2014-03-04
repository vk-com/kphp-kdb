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

#pragma once

// Names and versions
#define VERSION "0.01"
#define NAME "php-engine"
#define NAME_VERSION NAME "-" VERSION
#ifndef COMMIT
#  define COMMIT "unknown"
#endif
extern const char *full_version_str;

// for: struct in_addr
#include <netinet/in.h>
#include <stdio.h>
#include <signal.h>
/***
 DEFAULT GLOBAL VARIABLES
 ***/
#define MAX_VALUE_LEN (1 << 24)
#define MAX_KEY_LEN 1000

/** engine variables **/
extern char *logname_pattern;
extern int logname_id;
extern long long max_memory;
extern int http_port;

extern int worker_id;
extern int pid;

extern int no_sql;

extern int master_flag;
extern int workers_n;

extern int run_once;
extern int run_once_return_code;

/** stats **/
extern double load_time;
//uptime
typedef struct {
  long tot_queries;
  double worked_time;
  double net_time;
  double script_time;
  long tot_script_queries;
  double tot_idle_time;
  double tot_idle_percent;
  double a_idle_percent;
  int cnt;
} acc_stats_t;
extern acc_stats_t worker_acc_stats;

/** http **/
extern int http_port;
extern int http_sfd;
extern struct in_addr settings_addr;

extern char buf[];
extern volatile int sigpoll_cnt;

/** rcp **/
extern long long rpc_failed, rpc_sent, rpc_received, rpc_received_news_subscr, rpc_received_news_redirect;
extern int rpc_port;
extern int rpc_sfd;
extern int rpc_client_port;
extern const char *rpc_client_host;
extern int rpc_client_target;

/** master **/
extern const char *cluster_name;
extern int master_port;
extern int master_sfd;
extern int master_sfd_inited;
extern int master_pipe_write;
extern int master_pipe_fast_write;

/** sigterm **/
extern double sigterm_time;
extern int sigterm_on;
extern int rpc_stopped;

/** script **/
typedef struct php_immediate_stats {
  sig_atomic_t is_running;
  sig_atomic_t is_wait_net;
} php_immediate_stats_t;
extern php_immediate_stats_t immediate_stats;

/***
  GLOBAL VARIABLES
 ***/
extern int sql_target_id;
extern int in_ready;
extern int script_timeout;

#define RPC_INVOKE_KPHP_REQ 0x99a37fda
#define RPC_INVOKE_REQ 0x2374df3d
#define RPC_REQ_RUNNING 0x346d5efa
#define RPC_REQ_ERROR 0x7ae432f5
#define RPC_REQ_RESULT 0x63aeda4e
#define RPC_SEND_SESSION_MSG 0x1ed5a3cc
#define RPC_READY 0x6a34cac7
#define RPC_STOP_READY 0x59d86654
#define RPC_PHP_IMMEDIATE_STATS 0x3d27a21b
#define RPC_PHP_FULL_STATS 0x1f8ae120

#define TL_KPHP_START_LEASE 0x61344739
#define TL_KPHP_LEASE_STATS 0x3013ebf4
#define TL_KPHP_STOP_LEASE 0x183bf49d

#define SPOLL_SEND_STATS 0x32d20000
#define SPOLL_SEND_IMMEDIATE_STATS 1
#define SPOLL_SEND_FULL_STATS 2

#define SIGTERM_MAX_TIMEOUT 10
#define SIGTERM_WAIT_TIMEOUT 0.1

#define SIGSTAT (SIGRTMIN)
#define MAX_WORKERS 1000


#ifdef __cplusplus
extern "C" {
#endif


void reopen_logs (void);

#ifdef __cplusplus
}
#endif
