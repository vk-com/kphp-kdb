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

#include "utils.h"
#include "maccub.h"
#include "server-functions.h"

char **fnames;
int *fd;
long long *fsize, *fcurr;

int userN = -1, engineN = -1;

char *in_name, *out_name, *username;

int A, B;

#define MY_BUFF_SIZE 1048576
const int entry_size = sizeof (int) * 2;

int *l_len;


int my_read (void) {
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
  A = ((int *)buff)[2 * i];
  B = ((int *)buff)[2 * i++ + 1];

  return 1;
}

vector *v;

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

int verbosity = 0;

char *progname;

void usage (void) {
  printf ("usage: %s [-u] [-i<input_file>] [-o<output_file>] [-U<userN>] [-E<engineN>]\n"
    "Prepares binary files (default \"to<server_num>.tmp\") with friends of friends\n"
    "  server_num is 3 digit number maybe with leading zeros\n"
	  "\t-i\tname of input binary file from\n"
	  "\t-o\tname of output binary file\n"
	  "\t-U\tmaximal expected total number of users\n"
	  "\t-E\tnumber of friend engines\n",
	  progname);
  exit (2);
}

int cmp_int_inv (const void * _a, const void * _b)
{
  int a = *(int *)_a, b = *(int *)_b;
  if (a < b) {
    return +1;
  } else if (a > b) {
    return -1;
  }
  return 0;
}



int main (int argc, char *argv[]) {
  int i;

  set_debug_handlers ();
  progname = argv[0];

  if (argc == 1) {
    usage();
    return 2;
  }

  while ((i = getopt (argc, argv, "hi:o:U:E:u:")) != -1) {
    switch (i) {
    case 'h':
      usage ();
      return 2;
    case 'i':
      in_name = optarg;
      break;
    case 'o':
      out_name = optarg;
      break;
    case 'U':
      userN = atoi (optarg);
      break;
    case 'E':
      engineN = atoi (optarg);
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

  assert (userN >= 0);
  assert (engineN >= 0);

  init_files (engineN + 1);

  int cnt = userN / engineN;
  v = qmalloc (sizeof (vector) * (cnt + 2));
  assert (v != NULL);

  for (i = 0; i <= cnt; i++) {
    vct_init (&v[i]);
  }

  if (in_name != NULL) {
    open_file (0, in_name, 0);
  } else {
    fd[0] = 0;
  }
  fcurr[0] = 0;

  open_file (1, out_name, 2);

  long long fsz = -1;
  write (fd[1], &fsz, sizeof (long long));

  while (my_read()) {
//    fprintf (stderr, "%d (%d;%d)\n", A, B / 2 , B & 1);
    int x = A / engineN;
    vct_add (&v[x], B);
  }


  int header_size = sizeof (int) * (cnt + 2) + sizeof (long long);
  fsz = header_size;
  l_len = qmalloc0 (header_size);
  l_len[0] = cnt + 1;

  assert (lseek (fd[1], header_size, SEEK_SET) == header_size);

  for (i = 0; i <= cnt; i++) {
  //  fprintf (stderr, "%d/%d\n", i, cnt);
    //qsort (v[i].mem, v[i].n, sizeof (int), cmp_int_inv);


    //l_len[i + 1] = LIST_ (encode) (v[i].mem, v[i].n, 2 * userN + 1, list_buff);

    //my_write (list_buff, l_len[i + 1]);
    //fsz += l_len[i + 1];

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
