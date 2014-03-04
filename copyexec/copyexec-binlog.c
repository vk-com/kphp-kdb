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

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/wait.h>

#include "net-connections.h"
#include "kfs.h"
#include "server-functions.h"
#include "kdb-copyexec-binlog.h"
#include "copyexec-err.h"

int skip_timestamps = 0;
double binlog_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos = 0;
long long binlog_loaded_size;
FILE *out;

int start_time = 0, end_time = 0x7fffffff;
static int dump_log_pos = 0, dump_timestamp = 0;
#define mytime() get_utime (CLOCK_MONOTONIC)

static int copyexec_main_le_start (struct lev_start *E) {
  if (E->schema_id != COPYEXEC_MAIN_SCHEMA_V1 && E->schema_id != COPYEXEC_RESULT_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

inline int dump_line_header (const char *const tp) {
  if (start_time > now) { return -1; }
  if (dump_log_pos) { fprintf (out, "%lld\t",  log_cur_pos()); }
  if (dump_timestamp) { fprintf (out, "%d\t", now); }
  fprintf (out, "%s\t", tp);
  return 0;
}

void dump_wait_status (int status) {
  if (WIFEXITED(status)) {
    fprintf (out, "exit:%d", WEXITSTATUS(status));
  } else if (WIFSIGNALED(status)) {
    fprintf (out, "sig:%d", WTERMSIG(status));
  } else {
    fprintf (out, "%d", status);
  }
}

static void dump_ts (int tp) {
  switch (tp & 255) {
    case ts_running:
      fprintf (out, "running");
      break;
    case ts_ignored:
      fprintf (out, "ignored");
      break;
    case ts_interrupted:
      fprintf (out, "interrupted");
      break;
    case ts_cancelled:
      fprintf (out, "cancelled");
      break;
    case ts_terminated:
      fprintf (out, "terminated");
      break;
    case ts_failed:
      fprintf (out, "failed");
      break;
    case ts_decryption_failed:
      fprintf (out, "decryption_failed");
      break;
    case ts_io_failed:
      fprintf (out, "io_failed");
      break;
    default:
      fprintf (out, "%d", tp);
  }
}

void swap_exit_signal_bytes (unsigned *x) {
  unsigned a = *x, b = a >> 16, c = a & 0xffff, d = c >> 8, e = c & 0xff;
  *x = (b << 16) | (e << 8) | d;
}

void dump_result (int status, unsigned result) {
  if (status == ts_io_failed || status == ts_decryption_failed) {
    fprintf (out, "%s", copyexec_strerror (-((int) result)));
  } else {
    fprintf (out, "0x%04x", result);
  }
}

void dump_status (struct lev_copyexec_main_transaction_status *E) {
  int status = E->type & 255;
  if (dump_line_header ("LEV_COPYEXEC_MAIN_TRANSACTION_STATUS")) {
    return;
  }
  dump_ts (E->type);
  fprintf (out, "\t%d\t%lld\t0x%x\t%d\t%d\t", E->transaction_id, E->binlog_pos, E->mask, E->pid, E->creation_time);
  dump_result (status, E->result);
  fprintf (out, "\t%lld\t%lld\n", E->st_dev, E->st_ino);
}

void dump_command_begin (struct lev_copyexec_main_command_begin *E) {
  if (dump_line_header ("LEV_COPYEXEC_MAIN_COMMAND_BEGIN")) {
    return;
  }
  fprintf (out, "%d\t%d\t%d\t%.*s\n", E->transaction_id, E->command_id, E->pid, E->command_size, E->data);
}

void dump_command_end (struct lev_copyexec_main_command_end *E) {
  if (dump_line_header ("LEV_COPYEXEC_MAIN_COMMAND_END")) {
    return;
  }
  fprintf (out, "%d\t%d\t%d\t", E->transaction_id, E->command_id, E->pid);
  dump_wait_status (E->status);
  fprintf (out, "\t%d\t%d\n", E->stdout_size, E->stderr_size);
  fprintf (out, "%.*s\n%.*s\n", E->saved_stdout_size, E->data, E->saved_stderr_size, E->data + E->saved_stdout_size);
}

void dump_transaction_err (struct lev_copyexec_main_transaction_err *E) {
  if (dump_line_header ("LEV_COPYEXEC_MAIN_TRANSACTION_ERR")) {
    return;
  }
  fprintf (out, "%d\t%.*s\n", E->transaction_id, E->error_msg_size, E->data);
}

static int dumping_crc32;

void dump_crc32 (struct lev_crc32 *E) {
  if (!dumping_crc32) { return; }
  if (dump_line_header ("LEV_CRC32")) {
    return;
  }
  fprintf (out, "%lld\t0x%x\n", E->pos, E->crc32);
}

int dump_copyexec_main_replay_logevent (struct lev_generic *E, int size);
int dump_copyexec_results_replay_logevent (struct lev_generic *E, int size);

int init_copyexec_main_data (int schema) {
  vkprintf (1, "init_copyexe_main_data\n");
  replay_logevent = dump_copyexec_main_replay_logevent;
  return 0;
}

int init_copyexec_result_data (int schema) {
  replay_logevent = dump_copyexec_results_replay_logevent;
  return 0;
}

int dump_copyexec_main_replay_logevent (struct lev_generic *E, int size) {
  int s;
  if (now > end_time) {
    fflush (out);
    fclose (out);
    exit (0);
  }
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return copyexec_main_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
      return default_replay_logevent (E, size);
    case LEV_CRC32:
      if (size < sizeof (struct lev_crc32)) { return -2; }
      dump_crc32 ((struct lev_crc32 *) E);
      return default_replay_logevent (E, size);
    case LEV_COPYEXEC_MAIN_TRANSACTION_STATUS ... LEV_COPYEXEC_MAIN_TRANSACTION_STATUS + ts_io_failed:
      s = sizeof (struct lev_copyexec_main_transaction_status);
      if (size < s) {
        return -2;
      }
      dump_status ((struct lev_copyexec_main_transaction_status *) E);
      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_BEGIN:
      s = sizeof (struct lev_copyexec_main_command_begin);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_command_begin *) E)->command_size;
      if (size < s) {
        return -2;
      }
      dump_command_begin ((struct lev_copyexec_main_command_begin *) E);
      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_END:
      s = sizeof (struct lev_copyexec_main_command_end);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_command_end *) E)->saved_stdout_size;
      s += ((struct lev_copyexec_main_command_end *) E)->saved_stderr_size;
      if (size < s) {
        return -2;
      }
      dump_command_end ((struct lev_copyexec_main_command_end *) E);
      return s;
    case LEV_COPYEXEC_MAIN_TRANSACTION_ERROR:
      s = sizeof (struct lev_copyexec_main_transaction_err);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_copyexec_main_transaction_err *) E)->error_msg_size;
      if (size < s) {
        return -2;
      }
      dump_transaction_err ((struct lev_copyexec_main_transaction_err *) E);
      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_WAIT:
      s = sizeof (struct lev_copyexec_main_command_wait);
      if (size < s) {
        return -2;
      }
      if (!dump_line_header ("LEV_COPYEXEC_MAIN_COMMAND_WAIT")) {
        fprintf (out, "%d\t%d\n", E->a, E->b);
      }
      return s;
    case LEV_COPYEXEC_MAIN_COMMAND_KILL:
      s = sizeof (struct lev_copyexec_main_command_kill);
      if (size < s) {
        return -2;
      }
      if (!dump_line_header ("LEV_COPYEXEC_MAIN_COMMAND_KILL")) {
        fprintf (out, "%d\t%d\t%d\n", E->a, E->b, E->c);
      }
      return s;
    case LEV_COPYEXEC_MAIN_TRANSACTION_SKIP:
      s = sizeof (struct lev_copyexec_main_transaction_skip);
      if (size < s) {
        return -2;
      }
      if (!dump_line_header ("LEV_COPYEXEC_MAIN_TRANSACTION_SKIP")) {
        fprintf (out, "%d\n", E->a);
      }
      return s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());

  return -3;

}

static void dump_connect (struct lev_copyexec_result_connect *E) {
  if (dump_line_header ("LEV_COPYEXEC_RESULT_CONNECT")) {
    return;
  }
  fprintf (out, "0x%llx\t0x%llx\t%s\t%.*s\t%d\n", E->random_tag, E->volume_id, show_ip (E->ip), E->hostname_length, E->hostname, E->pid);
}

static void dump_data (struct lev_copyexec_result_data *E) {
  if (dump_line_header ("LEV_COPYEXEC_RESULT_DATA")) {
    return;
  }
  fprintf (out, "0x%llx\t%d\t", E->random_tag, E->transaction_id);
  int status = E->result >> 28, result = E->result & 0x0fffffff;
  dump_ts (status);
  fprintf (out, "\t");
  dump_result (status, result);
  fprintf (out, "\t%lld\n", E->binlog_pos);
}

static void dump_enable (struct lev_copyexec_result_enable *E) {
  if (dump_line_header (E->type == LEV_COPYEXEC_RESULT_ENABLE ? "LEV_COPYEXEC_RESULT_ENABLE": "LEV_COPYEXEC_RESULT_DISABLE") ) {
    return;
  }
  fprintf (out, "0x%llx\n", E->random_tag);
}

int dump_copyexec_results_replay_logevent (struct lev_generic *E, int size) {
  int s;
  if (now > end_time) {
    fflush (out);
    fclose (out);
    exit (0);
  }
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return copyexec_main_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
      return default_replay_logevent (E, size);
    case LEV_COPYEXEC_RESULT_CONNECT:
      s = sizeof (struct lev_copyexec_result_connect);
      if (size < s) { return -2; }
      s += ((struct lev_copyexec_result_connect *) E)->hostname_length;
      if (size < s) { return -2; }
      dump_connect ((struct lev_copyexec_result_connect *) E);
      return s;
    case LEV_COPYEXEC_RESULT_DATA:
      s = sizeof (struct lev_copyexec_result_data);
      if (size < s) { return -2; }
      dump_data ( (struct lev_copyexec_result_data *) E);
      return s;
    case LEV_COPYEXEC_RESULT_DISABLE:
    case LEV_COPYEXEC_RESULT_ENABLE:
      s = sizeof (struct lev_copyexec_result_enable);
      if (size < s) { return -2; }
      dump_enable ((struct lev_copyexec_result_enable *) E);
      return s;
  }

  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());

  return -3;

}

void usage() {
  fprintf (stderr, "copyexec-binlog [-p] [-t] [-v] [-h] [-S<start-time>] [-T<end-time] <binlog>\n"
                   "\tConverts copyexec main binlog or copyexec results binlog into text format.\n"
                   "\t-p\tdump log pos\n"
                   "\t-t\tdump timestamp\n"
                   "\t-C\tdump CRC32\n"
                   "\t-S<start-time>\tsets start-time\n"
                   "\t-T<end-time>\tsets end-time\n");
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  long long jump_log_pos = 0;
  out = stdout;
  while ((i = getopt (argc, argv, "tphvCS:T:")) != -1) {
    switch (i) {
    case 'C':
      dumping_crc32 = 1;
      break;
    case 'S':
      start_time = atoi (optarg);
      break;
    case 'T':
      end_time = atoi (optarg);
      break;
    case 'p':
      dump_log_pos = 1;
      break;
    case 't':
      dump_timestamp = 1;
      break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage ();
    }
  }

  if (optind >= argc) {
    usage ();
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  vkprintf (3, "engine_preload_filelist done\n");

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);

  binlog_load_time = -mytime();
  clear_log();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);
  vkprintf (1, "replay log events started\n");

  i = replay_log (0, 1);

  fflush (out);
  fclose (out);

  vkprintf (1, "replay log events finished\n");

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  return 0;
}
