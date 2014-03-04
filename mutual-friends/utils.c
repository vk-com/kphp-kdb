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

#include "utils.h"

size_t memory_used = 0;

void *qmalloc (size_t x) {
  memory_used += x;
  void *res = malloc (x);
  assert (x == 0 || res != NULL);
  return res;
}

void *qmalloc0 (size_t x) {
  memory_used += x;
  void *res = calloc (x, 1);
  assert (x == 0 || res != NULL);
  return res;
}

void *qrealloc (void *p, size_t x, size_t old) {
  memory_used += x - old;
  void *res = realloc (p, x);
  assert (x == 0 || res != NULL);
  return res;
}

long get_memory_used (void) {
  return memory_used;
}


void qfree (void *p, size_t x) {
  memory_used -= x;
  return free (p);
}

void vct_init (vector *v) {
  v->mem = qmalloc (sizeof (int));
  v->mx = 1, v->n = 0;
  v->rn = 0;
}

void vct_add (vector *v, int x) {
  if (v->mx == v->n) {
    v->mem = qrealloc (v->mem, sizeof (int) * v->mx * 2, sizeof (int) * v->mx);
    v->mx *= 2;
  }
  v->mem[v->n++] = x;
}

void vct_add_lim (vector *v, int x, int lim)  {
  if (v->mx == v->n && v->mx != lim) {
    int new_len = v->mx * 2;
    if (new_len > lim) {
      new_len = lim;
    }
    v->mem = qrealloc (v->mem, sizeof (int) * new_len, sizeof (int) * v->mx);
    v->mx = new_len;
  }

  v->rn++;
  if (v->n < lim) {
    v->mem[v->n++] = x;
  } else {
    int i = rand() % v->rn;
    if (i < v->n) {
      v->mem[i] = x;
    }
  }
}

void vct_free (vector *v) {
  qfree (v->mem, sizeof (int) * v->mx);
  v->mx = 0, v->n = 0;
}

int vct_back (vector *v) {
  if (v->n) {
    return v->mem[v->n - 1];
  }
  return 0;
}

void vct_set_add (vector *v, int val) {
  if (vct_back (v) != val) {
    vct_add (v, val);
  }
}
/* file utils */

char **fnames;
int *fd;
long long *fsize, *fcurr;
extern char *progname;
extern int verbosity;

int f_inited = 0;

void init_files (int fn) {
  assert (!f_inited);
  fnames = qmalloc0 (sizeof (char *) * fn);
  fd = qmalloc0 (sizeof (int) * fn);
  fsize = qmalloc0 (sizeof (long long) * fn);
  fcurr = qmalloc0 (sizeof (long long) * fn);
  f_inited = fn;
}

int open_file (int x, char *fname, int creat) {
  assert (f_inited);
  assert (0 <= x && x < f_inited);

  //fprintf (stderr, "open %d %s %d\n", x, fname, creat);

  fnames[x] = fname;
  int options;
  if (creat > 0) {
    options = O_RDWR | O_CREAT;
    if (creat == 2) {
      options |= O_TRUNC;
    }
  } else {
    options = O_RDONLY;
  }

  fd[x] = open (fname, options, 0600);
  if (creat < 0 && fd[x] < 0) {
    if (fd[x] < 0) {
      fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    }
    return -1;
  }
  if (fd[x] < 0) {
    fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    exit (1);
  }
  fsize[x] = lseek (fd[x], 0, SEEK_END);
  if (fsize[x] < 0) {
    fprintf (stderr, "%s: cannot seek %s: %m\n", progname, fname);
    exit (2);
  }
  lseek (fd[x], 0, SEEK_SET);
  if (verbosity) {
    fprintf (stderr, "opened file %s, fd=%d, size=%lld\n", fname, fd[x], fsize[x]);
  }
  return fd[x];
}
