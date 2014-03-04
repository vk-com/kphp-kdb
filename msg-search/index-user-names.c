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

    Copyright 2008 Nikolai Durov
              2008 Andrei Lopatin
*/

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include "md5.c"

#define USERNAME_HASH_INDEX_MAGIC	0x11ef3ae6

#define BUFFSIZE (1L << 24)

#define MAX_USERS (1L << 27)
#define MAX_PAIRS (1L << 27)

int verbose = 0;

int Uc, Pc, Dc;

int fd;

typedef unsigned long long hash_t;
typedef struct userpair userpair_t;

#pragma	pack(4)

struct userpair {
  int user_id;
  hash_t hash;
};

struct username_index {
  int magic;
  int users;
  int offset[MAX_USERS];
} *UU;

#define	U	(*UU)

userpair_t *P;   // P[MAX_PAIRS];

void my_psort (int a, int b) {
  userpair_t t;
  int h, i, j;
  if (a >= b) return;
  i = a;  j = b;
  h = P[(a+b)>>1].user_id;
  do {
    while (P[i].user_id < h) i++;
    while (P[j].user_id > h) j--;
    if (i <= j) {
      t = P[i];  P[i++] = P[j];  P[j--] = t;
    }
  } while (i <= j);
  my_psort (a, j);
  my_psort (i, b);
}


int percent (long long a, long long b) {
  if (b <= 0 || a <= 0) return 0;
  if (a >= b) return 100;
  return (a*100 / b);
}

void output_stats (void) {
  fprintf (stderr, "%d hashes read (max %ld), %d hashes written, describing users up to %d (max %ld)\n", 
	   Pc, MAX_PAIRS, Dc, U.users, MAX_USERS);
}

char *progname = "index-user-names";

void usage (void) {
  printf ("usage: %s [-v] <members-hash-file-1> [<members-hash-file-2> ...]\n"
	  "\tCreates a user name index from a (concatenation of) user name hash files\n"
	  "\tResulting index file is written to stdout\n"
	  "\t-v\toutput statistical information into stderr\n",
	  progname);
  exit(2);
}


int main (int argc, char *argv[]) {
  int i, r;
  hash_t p = 0;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hv")) != -1) {
    switch (i) {
    case 'v':
      verbose = 1;
      break;
    case 'h':
      usage();
      return 2;
    }
  }
  if (argc <= optind) {
    usage();
    return 2;
  }

  P = malloc (MAX_PAIRS * sizeof (userpair_t));
  UU = malloc (sizeof (struct username_index));
  assert (P && UU);

  while (optind < argc) {
    fd = open (argv[optind], O_RDONLY);
    if (fd < 0) {
      fprintf (stderr, "%s: cannot open() %s: %m\n", progname, argv[optind]);
      optind++;
      continue;
    }
    r = (MAX_PAIRS - Pc) * sizeof(userpair_t);
    i = read (fd, P + Pc, r);
    if (i < 0) {
      fprintf (stderr, "%s: error reading %s: %m\n", progname, argv[optind]);
      return 1;
    }
    if (i == r) {
      fprintf (stderr, "%s: error reading %s: hash space exhausted (%ld entries)\n", progname, argv[optind], MAX_PAIRS);
      return 1;
    }
    Pc += i / sizeof(userpair_t);    
    close (fd);
    optind++;
  }

  my_psort(0, Pc-1);

  for (i = 0; i < Pc; i++) {
    int j = P[i].user_id;
    hash_t h = P[i].hash;
    assert (j < MAX_USERS-3);
    while (U.users <= j) {
      U.offset[U.users++] = Dc;
      p = 0;
    }
    if (h != p) {
      ((hash_t *) P)[Dc++] = h;
      p = h;
    }
  }
  U.offset[U.users++] = Dc;
  if (U.users & 1) { U.offset[U.users++] = Dc; }

  U.magic = USERNAME_HASH_INDEX_MAGIC;
  r = 8 + 4 * U.users;
  assert (write (1, &U, r) == r);
  r = Dc * 8;
  assert (write (1, P, r) == r);

  if (verbose) {
    output_stats();
  }
                                      
  return 0;
}
