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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "icpl-data.h"
#include "kdb-data-common.h"
#include "server-functions.h"

#define	VERSION_STR	"icplc-0.01"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

void usage (void) {
  printf ("%s\n",  FullVersionStr);
  printf ("usage: icpl [-v] [-h] [program-file]\n"
      "\tICPL compiler\n"
      "\t-v\toutput statistical and debug information into stderr\n");
  exit (2);
}

char *icpl_readfile (const char *const filename) {
  int fd = open (filename, O_RDONLY);
  if (fd < 0) {
    kprintf ("open (\"%s\") fail. %m", filename);
    return NULL;
  }
  struct stat b;
  if (fstat (fd, &b) < 0) {
    kprintf ("fstat (\"%s\") fail. %m", filename);
    close (fd);
    return NULL;
  }
  if (b.st_size > 0x1000000) {
    kprintf ("\"%s\" file too big", filename);
    close (fd);
    return NULL;
  }
  char *a = malloc (b.st_size + 1);
  assert (a);
  assert (read (fd, a, b.st_size) == b.st_size);
  a[b.st_size] = 0;
  return a;
}

int main (int argc, char *argv[]) {
  int i;
  set_debug_handlers ();
  while ((i = getopt (argc, argv, "hv")) != -1) {
    switch (i) {
    case 'h':
      usage ();
      return 2;
    case 'v':
      verbosity++;
      break;
    }
  }

  if (argc != optind + 1) {
    usage ();
  }
  
  char *prog_filename = argv[optind];

  init_dyn_data ();
  icpl_init ();
  char *a = icpl_readfile (prog_filename);
  if (a == NULL) {
    kprintf ("icpl_readfile (\"%s\") fail.\n", prog_filename);
    exit (1);
  }

  if (icpl_parse (a) < 0) {
    free (a);
    exit (1);
  }

  free (a);
  return 0;
}
