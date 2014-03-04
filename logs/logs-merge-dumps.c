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

char *out_name = "", *username;
char *dump_name = "dump";

int engineF = 0, engineN = 0;
int ansI;

#define BUFF_SIZE 655360
int *f_buff_size, *f_buff_r;
char **f_buff;

int left_files = 0;

void init_data (void) {
  char fname[100];
  int i;

  f_buff = dl_malloc (sizeof (char *) * engineN);
  f_buff_size = dl_malloc0 (sizeof (int) * engineN);
  f_buff_r = dl_malloc0 (sizeof (int) * engineN);

  for (i = engineF; i < engineN; i++) {
//    if (snprintf (fname, 100, "%s%03d", dump_name, i) >= 100) {
    if (snprintf (fname, 100, "%s%d.dump", dump_name, i) >= 100) {
      fprintf (stderr, "Filename is too long.\n");
      exit (1);
    }
    if (dl_open_file (i, fname, -1) < 0) {
      fprintf (stderr, "File '%s' not found.\n", fname);
      fd[i] = -1;
      continue;
    } else {
//      fprintf (stderr, "Opened file '%s' by pid %d.\n", fname, fd[i]);
    }
    left_files++;
    f_buff[i] = dl_malloc (sizeof (char) * BUFF_SIZE);
  }
}

int load (int en) {
  if (fsize[en] <= 0) {
    assert (fsize[en] == 0);
    return -1;
  }
  if (f_buff_r[en] < f_buff_size[en]) {
//    fprintf (stderr, "%d %d\n", f_buff_r[en], f_buff_size[en]);
    assert (f_buff_r[en] > f_buff_size[en] - f_buff_r[en]);
    memcpy (f_buff[en], f_buff[en] + f_buff_r[en], f_buff_size[en] - f_buff_r[en]);
  }
  f_buff_size[en] -= f_buff_r[en];
  f_buff_r[en] = 0;
  assert (0 <= f_buff_size[en]);

  int need = BUFF_SIZE - f_buff_size[en];
  if (fsize[en] < need) {
    need = fsize[en];
  }

//  fprintf (stderr, "Loading from %d dump %d bytes, already readed %llu, %lld bytes to read\n", en, need, lseek (fd[en], 0, SEEK_CUR), fsize[en]);
  assert (read (fd[en], f_buff[en] + f_buff_size[en], need) == need);
  f_buff_size[en] += need;
  fsize[en] -= need;

  return 1;
}

char w_buff[BUFF_SIZE];
int w_buff_n;

void flush_w_buff() {
  write (fd[ansI], w_buff, w_buff_n);
  w_buff_n = 0;
}

void my_write (char *s, int n) {
  int i = 0;
  while (i < n) {
    int x = n - i;
    if (x > BUFF_SIZE - w_buff_n) {
      x = BUFF_SIZE - w_buff_n;
    }
    memcpy (w_buff + w_buff_n, s + i, x);
    i += x;
    w_buff_n += x;
    if (w_buff_n == BUFF_SIZE) {
      flush_w_buff();
    }
  }
}

void run (void) {
  int i;
  while (left_files) {
    int min_time = 2147483637, now, size;
    for (i = engineF; i < engineN; i++) {
      if (fd[i] >= 0 && f_buff_size[i] - f_buff_r[i] < 2 * (int)sizeof (int) && load (i) == -1) {
        fd[i] = -1;
        left_files--;
        assert (f_buff_size[i] == f_buff_r[i]);
      }
    }

    for (i = engineF; i < engineN; i++) {
      if (fd[i] >= 0) {
        char *buff = f_buff[i] + f_buff_r[i];
        now = ((int *)buff)[0];
        size = ((int *)buff)[1];
//        fprintf (stderr, "%d %d %d\n", i, now, size);
        if (f_buff_size[i] - f_buff_r[i] < 2 * (int)sizeof (int) + size) {
          if (load (i) == -1 || f_buff_size[i] - f_buff_r[i] < 2 * (int)sizeof (int) + size) {
            fprintf (stderr, "Dump %d is broken. It contains event of size %d. f_buff_size = %d, f_buff_r = %d.\n", i, size, f_buff_size[i], f_buff_r[i]);
            assert (0);
          }
        }
        if (now < min_time) {
          min_time = now;
        }
      }
    }

    for (i = engineF; i < engineN; i++) {
      if (fd[i] >= 0) {
        char *buff = f_buff[i] + f_buff_r[i];
        now = ((int *)buff)[0];
        if (now == min_time) {
          size = ((int *)buff)[1];
          my_write (buff, 2 * sizeof (int) + size);
          f_buff_r[i] += 2 * sizeof (int) + size;
        }
      }
    }
  }
}

void usage (void) {
  printf ("usage: %s [-u] [-o<output_file>] [-N<engineN>] [-d<dump_name>]\n"
    "Merge engineN binary files \"<dump_name><server_num>.dump\"\n"
	  "\t-d\tprefix of dump files, delauit is \"dump\"\n"
	  "\t-o\tname of output binary file\n"
	  "\t-F\tnumber of first engine, default 0\n"
	  "\t-N\tnumber of last engine plus 1\n",
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

  while ((i = getopt (argc, argv, "d:hF:N:o:u:")) != -1) {
    switch (i) {
    case 'd':
      dump_name = optarg;
      break;
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

  if (argc != optind) {
    usage();
    return 2;
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  assert (engineN > engineF && engineF >= 0);
  ansI = engineN + 1;

  assert (engineN + 1 < MAX_FN);

  dl_open_file (ansI, out_name, 2);

  init_data();
  run();

  flush_w_buff();
  return 0;
}
