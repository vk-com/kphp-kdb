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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "utils.h"
#include "server-functions.h"

char *out_name = "stat_result.txt", *username;
char *stat_name = NULL;

int engineF = 0, engineN = 0;
int ansI;

int left_files = 0;

FILE *f[MAX_FN];

void init_data (void) {
  char fname[100];
  int i;

  for (i = engineF; i < engineN; i++) {
    if (snprintf (fname, 100, "%s%03d", stat_name, i) >= 100) {
      fprintf (stderr, "Filename is too long.\n");
      exit (1);
    }
    f[i] = fopen (fname, "r");
    if (f[i] == NULL) {
      fprintf (stderr, "File '%s' not found.\n", fname);
      exit (1);
    }
    left_files++;
  }
}

void run (void) {
  while (left_files) {
    long long sum = 0;

    int i;
    for (i = engineF; i < engineN; i++) {
      if (f[i] != NULL) {
        long long cur;
        if (fscanf (f[i], "%lld", &cur) <= 0) {
          left_files--;
        } else {
          sum += cur;
        }
      }
    }

    assert (left_files == 0 || left_files == engineN - engineF);
    if (left_files != 0) {
      fprintf (f[ansI], "%lld\n", sum);
    }
  }
}

void close_data (void) {
  int i;
  for (i = 0; i < MAX_FN; i++) {
    if (f[i] != NULL) {
      fclose (f[i]);
    }
  }
}

void usage (void) {
  printf ("usage: %s [-u] [-o<output_file>] [-F<first_engine_num>][-N<engines_cnt>] <stat_name>\n"
    "Merge engineN binary files \"<stat_name><server_num>\"\n"
    "  server_num is 3 digit number with leading zeros\n"
	  "\t-o\tname of output file\n"
	  "\t-F<first_engine_num>\tnumber of first engine, default 0\n"
	  "\t-N<engines_cnt>\tnumber of last engine plus 1\n",
	  progname);
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;

  dl_set_debug_handlers ();
  progname = argv[0];

  if (argc == 1) {
    usage();
    return 2;
  }

  while ((i = getopt (argc, argv, "hF:N:o:u:")) != -1) {
    switch (i) {
    case 'h':
      usage ();
      return 2;
    case 'F':
      engineF = atoi (optarg);
      break;
    case 'N':
      engineN = atoi (optarg);
      break;
    case 'o':
      out_name = optarg;
      break;
    case 'u':
      username = optarg;
      break;
    }
  }

  if (argc != optind + 1) {
    usage();
    return 2;
  }
  stat_name = argv[optind];

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  assert (engineN > engineF && engineF >= 0);
  ansI = engineN + 1;

  assert (engineN + 1 < MAX_FN);

  f[ansI] = fopen (out_name, "w");
  assert (f[ansI] != NULL);

  init_data();
  run();
  close_data();

  return 0;
}
