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
              2012-2013 Vitaliy Valtman
*/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "tl-parser-new.h"
#include "server-functions.h"
#include "kdb-data-common.h"

#include "unistd.h"
#include "sys/types.h"
#include "sys/stat.h"
#include "fcntl.h"

#define	VERSION_STR	"tlc-0.01"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;


int verbosity;
int output_expressions;
int schema_version = 2;
void usage (void) {
  printf ("%s\n",  FullVersionStr);
  printf ("usage: tlc-new [-v] [-h] <TL-schema-file>\n"
      "\tTL compiler\n"
      "\t-v\toutput statistical and debug information into stderr\n"
      "\t-E\twhenever is possible output to stdout expressions\n"
      "\t-e <file>\texport serialized schema to file\n"
      "\t-w\t custom version of serialized schema (0 - very old, 1 - old, 2 - current (default))\n"
       );
  exit (2);
}

int vkext_write (const char *filename) {
  int f = open (filename, O_CREAT | O_WRONLY | O_TRUNC, 0640);
  assert (f >= 0);
  write_types (f);
  close (f);
  return 0;
}

int main (int argc, char **argv) {
  int i;
  char *expr_filename = NULL;
  char *vkext_file = 0;
  set_debug_handlers ();
  while ((i = getopt (argc, argv, "Eho:ve:w:")) != -1) {
    switch (i) {
    case 'E':
      output_expressions++;
      break;
    case 'o':
      expr_filename = optarg;
      break;
    case 'h':
      usage ();
      return 2;
    case 'e':
      vkext_file = optarg;
      break;
    case 'w':
      schema_version = atoi (optarg);
      break;
    case 'v':
      verbosity++;
      break;
    }
  }

  dynamic_data_buffer_size = (1 << 30);

  if (argc != optind + 1) {
    usage ();
  }
  init_dyn_data ();
 
  struct parse *P = tl_init_parse_file (argv[optind]);
  if (!P) {
    return 0;
  }
  struct tree *T;
  if (!(T = tl_parse_lex (P))) {
    fprintf (stderr, "Error in parse:\n");
    tl_print_parse_error ();
    return 0;
  } else {
    if (verbosity) {
      fprintf (stderr, "Parse ok\n");
    }
    if (!tl_parse (T)) {
      if (verbosity) {
        fprintf (stderr, "Fail\n");
      }
      return 1;
    } else {
      if (verbosity) {
        fprintf (stderr, "Ok\n");
      }
    }
  }
  if (vkext_file) {
    vkext_write (vkext_file);
  }
  return 0;
}
