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
#include <time.h>

#include "utils.h"
#include "server-functions.h"

#define VERSION "0.13"
#define VERSION_STR "mf-process-file "VERSION

int userN = 1000000, engineN = 100, max_user_events = 2000, stdout_flag = 0;
int uf = -1;

vector *v;
int *l_len;

char **fnames;
int *fd;
long long *fsize, *fcurr;

#define W_BUFF_SIZE 1000000

char w_buff[W_BUFF_SIZE];
unsigned char list_buff[W_BUFF_SIZE];
int w_buff_n;

void flush_w_buff() {
  write (fd[1], w_buff, w_buff_n);
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

//short *events_left;

int verbosity = 0;
char *input_name = "friends.dump", *output_name = "", *username;


void init_data (void) {
  open_file (0, input_name, 0);
  open_file (1, output_name, 2);

/*  events_left = qmalloc (sizeof (short) * (userN + 2));
  int i;
  for (i = 0; i <= userN; i++) {
    events_left[i] = max_user_events;
  }*/
}

void gen_events (int *a, int n) {
  int i, j;
  int mask = (1 << 30) - 1;
  for (i = 0; i < n; i++) {
    int can = 1, ida = a[i], idb;
    if (ida < 0) {
      ida = -ida;
    }

    if (ida > (1 << 30)) {
      can = 0;
      ida &= mask;
    }

    if (ida % engineN != uf) {
      continue;
    }
    assert (ida < userN);

    for (j = 0; j < n/* && events_left[ida] > 0*/; j++) {
      if (i != j) {
        //fprintf (stderr, "%d %d\n", a[i], a[j]);
        idb = a[j];
        if (idb < 0) {
          continue;
        }
        assert (idb < userN);
        //events_left[ida]--;

        idb &= mask;

        vct_add_lim (&v[ida / engineN], 2 * idb + can, max_user_events);
      }
    }
  }
}

char *progname;

void usage (void) {
  printf ("%s\nusage: %s [-i<input_file>] [-o<output_file>] [-U<userN>] [-E<engineN>] [-T<to_engine_number>] [-u<username>] [-m<max_size_of_result>]\n"
    "Generates prepared binary file with friends of friends\n"
    "\tserver_num is 3 digit number maybe with leading zeros\n"
	  "\t-i\tname of input file with dump from friends engine\n"
	  "\t-o\tname of output binary file\n"
	  "\t-m\tmaximal number friends of friends, generated for one user on one server in mebibytes\n"
	  "\t-T\tnumber of engine for which generate friends of friends\n"
	  "\t-U\tmaximal expected total number of users\n"
	  "\t-E\tnumber of friend engines\n",
	  VERSION_STR, progname);
  exit (2);
}

int friends_list[1000000], friends_list_n;

int A, B;

const int entry_size = sizeof (int) * 2;

#define MY_BUFF_SIZE 1048576
int my_read (void) {
  static char buff[MY_BUFF_SIZE * sizeof (int) * 2 /* entry_size */];
  static int i = 0, n = 0;

  while (1) {
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
    A = ((int *)buff)[2 * i];
    B = ((int *)buff)[2 * i++ + 1];

    if (A < (int)2e9 && B < (int)2e9) {
      break;
    }
  }

  return 1;
}

int main (int argc, char *argv[]) {
  int i;

  set_debug_handlers();
  progname = argv[0];
  srand (time (NULL));

  if (argc == 1) {
    usage();
    return 2;
  }

  while ((i = getopt (argc, argv, "i:o:hvU:E:u:T:m:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'i':
      input_name = optarg;
      break;
    case 'o':
      output_name = optarg;
      break;
    case 'U':
      userN = atoi (optarg);
      break;
    case 'E':
      engineN = atoi (optarg);
      break;
    case 'T':
      assert (uf == -1);
      uf = atoi (optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'm':
      max_user_events = atoi (optarg);
      assert (1 <= max_user_events && max_user_events <= 10000000);
      break;
    }
  }

  assert (uf >= 0);

  if (argc != optind) {
    usage();
    return 2;
  }

  if (verbosity) {
    fprintf (stderr, "userN = %d, engineN = %d\n", userN, engineN);
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  assert (0 < engineN);

  init_files (2);

  init_data();

  int cnt = userN / engineN;
  v = qmalloc (sizeof (vector) * (cnt + 2));

  for (i = 0; i <= cnt; i++) {
    vct_init (&v[i]);
  }

  assert (fsize[0] % 8 == 0);

  long long total = fsize[0] / 8;

  assert (total > 0);

  int prev = -1;
  while (total--) {
    my_read();
    if (A != prev) {
      prev = A;
      gen_events (friends_list, friends_list_n);
      friends_list_n = 0;
    }

    friends_list[friends_list_n++] = B;
  }
  gen_events (friends_list, friends_list_n);

  long long fsz = -1;
  write (fd[1], &fsz, sizeof (long long));


  int header_size = sizeof (int) * (cnt + 2) + sizeof (long long);
  fsz = header_size;
  l_len = qmalloc0 (header_size);
  l_len[0] = cnt + 1;

  assert (lseek (fd[1], header_size, SEEK_SET) == header_size);

  for (i = 0; i <= cnt; i++) {
    l_len[i + 1] = v[i].n * sizeof (int);
    fsz += v[i].n * sizeof (int);
    my_write (v[i].mem, v[i].n * sizeof (int));
  }
  flush_w_buff();

  assert (lseek (fd[1], 0, SEEK_SET) == 0);
  write (fd[1], &fsz, sizeof (long long));
  write (fd[1], l_len, header_size - sizeof (long long));

  return 0;
}
