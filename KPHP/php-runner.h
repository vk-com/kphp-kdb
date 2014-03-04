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

#include <assert.h>
#include <ucontext.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include <signal.h>
#include <stdlib.h>

#include "dl-utils-lite.h"
#include "php_script.h"

typedef enum {rst_finished, rst_uncleared, rst_query, rst_error, rst_ready, rst_before_init, rst_running, rst_empty, rst_query_running} run_state_t;
typedef enum {q_memcached, q_rpc, q_int} query_type;

#define SEGV_STACK_SIZE (MINSIGSTKSZ + 65536)
#define MAX_SCRIPT_TIMEOUT (60 * 6)
#pragma pack(push, 4)

typedef struct {
  query_type type;
} query_base;

typedef struct {
  query_type type; // MUST BE HERE
  int val, res;
} query_int;

//typedef void (*func_run) (void);

#pragma pack(pop)

//C interface
#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  long long q_id;
  const char *desc;
  int port;
  const char *query;
  double timeout;
} query_stats_t;

extern query_stats_t query_stats;
extern long long query_stats_id;

void dump_query_stats (void);

int msq (int x);

void init_handlers (void);
void php_script_finish (void *ptr);
void php_script_free (void *ptr);
void php_script_clear (void *ptr);
void php_script_init (void *ptr, script_t *f_run, php_query_data *data);
run_state_t php_script_iterate (void *ptr);
query_base *php_script_get_query (void *ptr);
script_result *php_script_get_res (void *ptr);
void php_script_query_readed (void *ptr);
void php_script_query_answered (void *ptr);
run_state_t php_script_get_state (void *ptr);
void *php_script_create (size_t mem_size, size_t stack_size);
void php_script_terminate (void *ptr, const char *error_message);
void php_script_set_timeout (double t);
const char *php_script_get_error (void *ptr);

#ifdef __cplusplus
}
#endif

//C++ part
#ifdef __cplusplus

class PHPScriptBase;
class PHPScriptBase {
public:

  static PHPScriptBase * volatile current_script;
  static ucontext_t exit_context;
  volatile static bool is_running;
  volatile static bool tl_flag;

  run_state_t state;
  const char *error_message;
  void *query;
  char *run_stack, *protected_end, *run_stack_end, *run_mem;
  size_t mem_size, stack_size;
  ucontext_t run_context;

  double cur_timestamp, net_time, script_time;
  int queries_cnt;

  static void cur_run();
  static void error (const char *s);

  void check_tl();

  bool is_protected (char *x);
  bool check_stack_overflow (char *x);

  PHPScriptBase (size_t mem_size, size_t stack_size);
  virtual ~PHPScriptBase();

  void init (script_t *f_run, php_query_data *data_to_set);

  //in php script
  void pause();
  void ask_query (void *q);
  void set_script_result (script_result *res_to_set);

  void resume();
  run_state_t iterate();

  void finish();
  void clear();
  void query_readed (void);
  void query_answered (void);

  void run (void);

  script_t *run_main;
  php_query_data *data;
  script_result *res;
};

//TODO: sometimes I need to call old handlers
void sigalrm_handler (int signal);
void sigsegv_handler (int signum, siginfo_t *info, void *data);

#endif
