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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Anton  Timofeev    (atimofeev@vkontakte.ru)
 *          Converts tab-separated table dump into KDB binlog (for antispam-engine).
 * Created: 01.04.2012
 */

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>

// #include "net-connections.h"

#include "server-functions.h"

#include "antispam-common.h"
#include "kdb-antispam-binlog.h"
#include "antispam-engine.h"
#include "antispam-data.h"
#include "antispam-db.h"

/**
 * Stats variables
 */
static int records_read = 0, records_processed = 0;

/**
 * Parse file in format (each line):
 * "${id}\t${time_stamp}\t${string}\t${ip}\t${uahash}"
 */

static inline bool delimiter (char c) {
  return c == 0xA || c == 0xD || c == '\t';
}

// at most five parts because of input format
static int split_string (char *s, char **parts) {
  int pn = 0;
  while (TRUE) {
    int end = 0;
    while (!end && *s && delimiter (*s)) {
      if (*s == '\t') {
        end = 1;
      }
      *s++ = 0;
    }
    if (!*s || pn == 5) {
      break;
    }
    parts[pn++] = s;
    while (*s && !delimiter (*s)) {
      s++;
    }
  }
  return (pn == 5) && (*s == 0);
}

static void import_patterns_dump (char const* file_name) {
  FILE *f = fopen (file_name, "rt");
  if (f == NULL) {
    fprintf (stderr, "Fatal: failed to open '%s' patterns source\n", file_name);
    exit (1);
  }

  static char str[MAX_PATTERN_LEN + 1];
  str[MAX_PATTERN_LEN] = 0;
  // '\n' from fgets will be cleaned in split function
  while (fgets (str, MAX_PATTERN_LEN, f)) {
    ++records_read;

    char *parts[5];
    // {id, ip, ua_hash, flags, pattern}
    bool result = split_string (str, parts);
    antispam_pattern_t pattern;
    if (result) {
      result &= (sscanf (parts[0], "%d", &pattern.id) == 1);
      result &= (sscanf (parts[1], "%u", &pattern.ip) == 1);
      result &= (sscanf (parts[2], "%u", &pattern.uahash) == 1);
      result &= (sscanf (parts[3], "%hu", &pattern.flags) == 1);
    }
    if (!result || !do_add_pattern (pattern, strlen (parts[4]), parts[4], TRUE)) {
      if (verbosity > 0) {
        fprintf (stderr, "rejected line: '%s' from '%s'\n", str, file_name);
      }
    } else {
      ++records_processed;
    }
  }
  fclose (f);
}

static void clean_binlog_file () {
  FILE *f = fopen (binlogname, "wb");
  if (f == 0) {
    fprintf (stderr, "fatal: can't open binlogname=%s, to cleanup (-c option)\n", binlogname);
    exit (1);
  }
  struct lev_start record = {LEV_START, ANTISPAM_SCHEMA_V1, 0, 1, 0, 1};
  fwrite (&record, sizeof (record) - 4, 1, f);
  fclose (f);
}

/**
 * Signal handlers
 */

static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\nOnly part of dump imported\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  flush_binlog_last ();
  sync_binlog (2);
  finish_all ();
  exit (1);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\nOnly part of dump imported\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  flush_binlog_last ();
  sync_binlog (2);
  finish_all ();
  exit (1);
}

//static void sigpoll_handler (const int sig) {
//  sigpoll_cnt++;
//  signal (SIGPOLL, sigpoll_handler);
//}

void usage (void) {
  fprintf (stderr, "usage:\t%s [-h] [-v] [-u<username>] [-B<max-binlog-size>] [-p<patterns-dump-name>] [-c] <binlog-name>\n"
           "\tConverts tab-separated table dump into KDB binlog (for antispam-engine).\n"
           "\tRecommented mySQL query: \"SELECT * FROM recent_patterns WHERE state >= 0 AND conditions = ''\"\n"
           "\t-h\tthis help screen\n"
           "\t-v\tverbose mode on\n"
           "\t-c\tclears <binlog-name> before dump appending (setup empty binlog)\n"
           "\t-u<username>\tassume identity of given user\n"
           "\t-B<max-binlog-size>\tdefines maximum size of each binlog file\n"
           "\t-p<patterns-dump-name>\tif specified will be appended to the <binlog-name> tail\n"
           "\t                      \tdump line format: id<tab>ip<tab>ua_hash<tab>flags<tab>pattern\n"
           "\t                      \tflags & 0|32|16 (means simplify-type=none|partial|full)\n"
           "\t<binlog-name>\tspecify binlog to replay and append patterns dump\n",
           progname);
}

int main (int argc, char **argv) {
  progname = argv[0];

  int i;
  long long x = 0;
  char c = 0;
  bool binlog_file_need_cleaning = FALSE;
  char const *patterns_name = 0;
  while ((i = getopt (argc, argv, "hvu:B:p:c")) != -1) {
    switch (i) {
    case 'h':
      usage();
      return 2;
    case 'v':
      verbosity++;
      break;
    case 'u':
      username = optarg;
      break;
    case 'B':
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
      }
      if (x >= 1024 && x < (1LL << 60)) {
        max_binlog_size = x;
      }
      break;
    case 'p':
      patterns_name = optarg;
      break;
    case 'c':
      binlog_file_need_cleaning = TRUE;
      break;
    }
  }

  if (argc != optind + 1) {
    usage();
    return 2;
  }

  antispam_change_user ();

  binlogname = argv[optind];
  if (binlog_file_need_cleaning) {
    clean_binlog_file ();
  }
  antispam_engine_common_init_part (""/* index_fname, empty */);

  assert (append_to_binlog (Binlog) == log_readto_pos);

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
//  signal (SIGPOLL, sigpoll_handler);

  if (patterns_name) {
    import_patterns_dump (patterns_name);
  }

  flush_binlog_forced (1);
//  flush_binlog_last ();
//  sync_binlog (2);

  if (verbosity > 0) {
    fprintf (stderr, "read: %d records read, %d processed\nwritten: log_pos=%lld, used_z_memory=%ld\n",
             records_read, records_processed, (long long)log_pos, dyn_used_memory ());
  }

  finish_all ();

  if (verbosity > 2) {
    st_printf ("Memory lost: z_malloc = $3%ld$^, dl_malloc = $3%lld$^\n", dyn_used_memory (), dl_get_memory_used ());
  }

  mt_test();
  return 0;
}
