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

    Copyright 2010-2013	Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/
#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>

#include "hash_table.h"
#include "maccub.h"
#include "server-functions.h"

char *username;

char **fnames;
int *fd;
long long *fsize, *fcurr;

#define MY_BUFF_SIZE 655360

int verbosity = 0;

int A1 = -1, B1 = -1, A2 = -1, B2 = -1, B1a, B1b, B2a, B2b;

const int entry_size = sizeof (int) * 2;

int my_read1 (void) {
  static char buff[MY_BUFF_SIZE * sizeof (int) * 2 /* entry_size */];
  static int i = 0, n = 0;

  if (i == n) {
    n = (fsize[0] - fcurr[0]) / entry_size;
    if (n > MY_BUFF_SIZE) {
      n = MY_BUFF_SIZE;
    }

    if (n == 0) {
      return 0;
    }

    read (fd[0], buff, n * entry_size);
    fcurr[0] += n * entry_size;
    i = 0;
  }
  A1 = ((int *)buff)[2 * i];
  B1 = ((int *)buff)[2 * i++ + 1];
  B1a = B1 > 0 ? 0 : 1;
  B1b = abs (B1) & ((1 << 30) - 1);

  return 1;
}

int my_read2 (void) {
  static char buff[MY_BUFF_SIZE * sizeof (int) * 2 /* entry_size */];
  static int i = 0, n = 0;

  if (i == n) {
    n = (fsize[1] - fcurr[1]) / entry_size;
    if (n > MY_BUFF_SIZE) {
      n = MY_BUFF_SIZE;
    }

    if (n == 0) {
      return 0;
    }

    read (fd[1], buff, n * entry_size);
    fcurr[1] += n * entry_size;
    i = 0;
  }
  A2 = ((int *)buff)[2 * i];
  B2 = ((int *)buff)[2 * i++ + 1];
  B2a = B2 > 0 ? 0 : 1;
  B2b = abs (B2) & ((1 << 30) - 1);

  return 1;
}

#define W_BUFF_SIZE 1000000
char w_buff[W_BUFF_SIZE];
unsigned char list_buff[W_BUFF_SIZE];
int w_buff_n;

void flush_w_buff() {
  write (fd[2], w_buff, w_buff_n);
//  fsz += w_buff_n;
  w_buff_n = 0;
}


void my_write (void *_s, int n) {
  char *s = (char *)_s;
  int i = 0;
  while (i < n) {
    int x = n - i;
    if (x > W_BUFF_SIZE - w_buff_n) {
      x = W_BUFF_SIZE - w_buff_n;
    }
    memcpy (w_buff + w_buff_n, s + i, x);
    i += x;
    w_buff_n += x;
    if (w_buff_n == W_BUFF_SIZE) {
      flush_w_buff();
    }
  }
}


void run (void) {
  int f1 = 1, f2 = 1;
  while (f1 || f2) {
    if (f1 && f2 && A1 == A2 && B1 == B2) {
      f1 = my_read1();
      f2 = my_read2();
    } else if (!f2 || (f1 && (A1 < A2 || (A1 == A2 && (B1a < B2a || (B1a == B2a && B1b < B2b)))))) {
      my_write (&A1, sizeof (int));
      my_write (&B1, sizeof (int));
      f1 = my_read1();
    } else {
      my_write (&A2, sizeof (int));
      my_write (&B2, sizeof (int));
      f2 = my_read2();
    }
  }
}


char *progname;

void usage (void) {
  printf ("usage: %s [-u] first.dump second.dump\n"
    "Generates XOR of two dumps. Writes result into stdout.\n",
	  progname);
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;

  set_debug_handlers ();
  progname = argv[0];

  if (argc == 1) {
    usage();
    return 2;
  }

  while ((i = getopt (argc, argv, "hu:")) != -1) {
    switch (i) {
    case 'h':
      usage ();
      return 2;
    case 'u':
      username = optarg;
      break;
    }
  }
  if (argc != optind + 2) {
    usage();
    return 2;
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_files (3);

  open_file (0, argv[optind], 0);
  open_file (1, argv[optind + 1], 0);
  fd[2] = 1;

  run();

  flush_w_buff();
  fsync (fd[2]);
//  assert (fsync (fd[2]) >= 0);  // fails when stdout is a pipe

  return 0;
}
