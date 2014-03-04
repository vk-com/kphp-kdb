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

#include "hash_table.h"
#include "maccub.h"
#include "server-functions.h"

#define MAX_FRIENDS 200000


#define MAXH (1 << 24)

int h_key[MAXH], h_val[MAXH];
int h_add (int key, int val) {
  int i = key & (MAXH - 1);
  while (h_key[i] && h_key[i] != key) {
    if (++i == MAXH) {
      i = 0;
    }
  }
  int res = -1;
  if (h_key[i] == 0) {
    h_key[i] = key;
    res = i;
  }

  h_val[i] += val;

  return res;
}

void h_rem (int key) {
  int i = key & (MAXH - 1);
  while (h_key[i] && h_key[i] != key) {
    if (++i == MAXH) {
      i = 0;
    }
  }

  h_val[i] = 0;
}


char *friends_name = "", *exceptions_name = "";
char *out_name = "", *username;

char **fnames;
int *fd;
long long *fsize, *fcurr;

int userN = -1, engineN = -1, this_mod = -1, un;

long long fsz;
int friendsI, exceptionsI, ansI;


#define MAX_ANS 1000

#define MY_BUFF_SIZE 655360
int *f_buff_size, **f_header, *f_buff_r, *f_buff_i;
unsigned char **f_buff;

int verbosity = 0, test_mode = 0;

void init_data (void) {
  char fname[50];
  int i;

  f_buff = qmalloc (sizeof (unsigned char *) * engineN);
  f_header = qmalloc (sizeof (int *) * engineN);

  f_buff_size = qmalloc0 (sizeof (int) * engineN);
  f_buff_r = qmalloc0 (sizeof (int) * engineN);
  f_buff_i = qmalloc0 (sizeof (int) * engineN);

  for (i = 0; i < engineN; i++) {
    sprintf (fname, "from%03d", i);
    if (open_file (i, fname, -1) < 0) {
      fd[i] = -1;
      continue;
    }
    f_buff[i] = qmalloc (sizeof (unsigned char) * MY_BUFF_SIZE);

    long long fsz;
    read (fd[i], &fsz, sizeof (long long));
    if (fsz != fsize[i]) {
      fprintf (stderr, "something wrong with file <%s> : wrong size %lld (%lld expected)\n", fname, fsize[i], fsz);
    }
    assert (fsz == fsize[i]);


    int cnt;
    read (fd[i], &cnt, sizeof (int));

    assert (cnt == un + 1);
    f_header[i] = qmalloc (sizeof (int) * cnt);
    read (fd[i], f_header[i], sizeof (int) * cnt);
  }
  if (strlen (friends_name) > 0) {
    open_file (friendsI, friends_name, 0);
  }
  if (strlen (exceptions_name) > 0) {
    open_file (exceptionsI, exceptions_name, 0);
  }
}

int load (int en, int id, int *a) {
  int r = f_buff_r[en];

  if (id >= r) {
    assert (id == r);
    int sz = 0;
    while (r <= un && sz + f_header[en][r] <= MY_BUFF_SIZE) {
      sz += f_header[en][r++];
    }
    if (f_header[en][r] > MY_BUFF_SIZE) {
      fprintf (stderr, "BIG USER DETECTED %d, r = %d, en = %d, id= %d\n", f_header[en][r], r, en, r * engineN + en);
      exit (0);    	
    }
    read (fd[en], f_buff[en], sz * sizeof (unsigned char));
    f_buff_i[en] = 0;
    f_buff_r[en] = r;
    f_buff_size[en] = sz;
  }


  assert (f_buff_i[en] + f_header[en][id] <= MY_BUFF_SIZE);
  memcpy (a, f_buff[en] + f_buff_i[en], f_header[en][id]);

  int res = f_header[en][id] / sizeof (int);

  f_buff_i[en] += f_header[en][id];
  assert (f_buff_i[en] <= f_buff_size[en]);
  return res;
}


int A1 = -1, B1 = -1, A2 = -1, B2 = -1;

const int entry_size = sizeof (int) * 2;

int my_read1 (void) {
  static char buff[MY_BUFF_SIZE * sizeof (int) * 2 /* entry_size */];
  static int i = 0, n = 0;

  if (i == n) {
    n = (fsize[friendsI] - fcurr[friendsI]) / entry_size;
    if (n > MY_BUFF_SIZE) {
      n = MY_BUFF_SIZE;
    }

    if (n == 0) {
      return 0;
    }

    read (fd[friendsI], buff, n * entry_size);
    fcurr[friendsI] += n * entry_size;
    i = 0;
  }
  A1 = ((int *)buff)[2 * i];
  B1 = ((int *)buff)[2 * i++ + 1];

  return 1;
}

int my_read2 (void) {
  static char buff[MY_BUFF_SIZE * sizeof (int) * 2 /* entry_size */];
  static int i = 0, n = 0;

  if (i == n) {
    n = (fsize[exceptionsI] - fcurr[exceptionsI]) / entry_size;
    if (n > MY_BUFF_SIZE) {
      n = MY_BUFF_SIZE;
    }

    if (n == 0) {
      return 0;
    }

    read (fd[exceptionsI], buff, n * entry_size);
    fcurr[exceptionsI] += n * entry_size;
    i = 0;
  }
  A2 = ((int *)buff)[2 * i];
  B2 = ((int *)buff)[2 * i++ + 1];

  return 1;
}

#define W_BUFF_SIZE 1000000
char w_buff[W_BUFF_SIZE];
unsigned char list_buff[W_BUFF_SIZE];
int w_buff_n;

void flush_w_buff() {
  write (fd[ansI], w_buff, w_buff_n);
  fsz += w_buff_n;
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



#define maxn 33000000
//must be at least MY_BUFF_SIZE * engineN
int a[maxn], an, b[maxn], cnt[maxn], c[maxn], d[maxn], dn;

void run (void) {
  int i, j;
  for (j = 0; j <= un && j * engineN + this_mod <= userN; j++) {
    an = 0;
    for (i = 0; i < engineN; i++) {
      if (fd[i] >= 0) {
        an += load (i, j, a + an);
      }
    }
    assert (an < maxn);
    int add;

    dn = 0;

    for (i = 0; i < an; i++) {
      add = (a[i] & 1) * 3 + 1;
      a[i] /= 2;
      d[dn] = h_add (a[i], add);
      if (d[dn] != -1) {
        dn++;
      }
    }

    int curr_f = j * engineN + this_mod;

    while (A1 <= curr_f) {
      if (A1 == curr_f) {
        if (B1 >= 0) {
          h_rem (B1 & ((1 << 30) - 1));
        }
      }
      if (!my_read1()) {
        break;    	
      }
    }

    while (A2 <= curr_f) {
      if (A2 == curr_f) {
        if (B2 >= 0) {
          h_rem (B2 & ((1 << 30) - 1));
        }
      }
      if (!my_read2()) {
        break;
      }
    }

    int n = 0, mx = 0;

    for (i = 0; i < dn; i++) {
      int j = d[i];
      if (h_val[j]) {
        a[n] = h_key[j];
        b[n] = h_val[j];
        if (mx < b[n]) {
          mx = b[n];
        }
        n++;
      }
      h_key[j] = 0;
      h_val[j] = 0;
    }


    memset (cnt, 0, sizeof (int) * (mx + 1));
    for (i = 0; i < n; i++) {
      cnt[b[i]]++;
    }
    int t = 0;
    for (i = 1; i <= mx; i++) {
      int tt = cnt[i];
      cnt[i] = cnt[i - 1] + t;
      t = tt;
    }

    for (i = 0; i < n; i++) {
      c[cnt[b[i]]++] = i;
    }

    int wn = n;
    if (wn > MAX_ANS) {
      wn = MAX_ANS;
    }

    my_write (&curr_f, sizeof (int));
    my_write (&wn, sizeof (int));
    for (i = 1; i <= wn; i++) {
      my_write (&b[c[n - i]], sizeof (int));
      my_write (&a[c[n - i]], sizeof (int));
    }

    if (test_mode) {
      printf ("%d %d", curr_f, wn);

      for (i = 1; i <= wn; i++) {
        printf (" (%d;%d)", b[c[n - i]], a[c[n - i]]);
      }
      printf ("\n");
    }
  }
}

char *progname;

void usage (void) {
  printf ("usage: %s [-u] [-o<output_file>] [-U<userN>] [-E<engineN>] [-T<this_engine_number>] [-F<friends.dump>] [-B<exceptions.dump>] [-t]\n"
    "Merge engineN binary files \"from<server_num>\" on target server with friends of friends\n"
    "  server_num is 3 digit number with leading zeros\n"
	  "\t-o\tname of output binary file\n"
	  "\t-U\tmaximal expected total number of users\n"
	  "\t-E\tnumber of friend engines\n"
	  "\t-T\tnumber of this engine\n"
	  "\t-F\tname of file with dump from friends engine\n"
	  "\t-B\tname of file with dump from mutual-friends engine\n"
	  "\t-t\ttest mode on (answer will be written to stdout in readable format)\n",
	  progname);
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;

  assert ((MAXH & (MAXH - 1)) == 0);

  set_debug_handlers();
  progname = argv[0];
//  fprintf (stderr, "%s\n", progname);

  if (argc == 1) {
    usage();
    return 2;
  }

  while ((i = getopt (argc, argv, "hU:E:T:F:B:to:u:")) != -1) {
    switch (i) {
    case 'h':
      usage ();
      return 2;
    case 'U':
      userN = atoi (optarg);
      break;
    case 'E':
      engineN = atoi (optarg);
      break;
    case 'T':
      this_mod = atoi (optarg);
      break;
    case 'F':
      friends_name = optarg;
      break;
    case 'B':
      exceptions_name = optarg;
      break;
    case 't':
      test_mode = 1;
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

  assert (userN >= 0);
  assert (engineN >= 0);
  assert (this_mod >= 0);
  friendsI = engineN;
  exceptionsI = engineN + 1;
  ansI = engineN + 2;

  init_files (engineN + 3);

  open_file (ansI, out_name, 2);

  fsz = -1;
  write (fd[ansI], &fsz, sizeof (long long));
  fsz = sizeof (long long);

  un = userN / engineN;
  init_data();
  run();

  flush_w_buff();

  assert (lseek (fd[ansI], 0, SEEK_SET) == 0);
  write (fd[ansI], &fsz, sizeof (long long));

  return 0;
}
