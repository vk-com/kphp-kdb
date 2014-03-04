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
#include "tl-parser.h"
#include "tl-scheme.h"
#include "tl-serialize.h"
#include "kdb-data-common.h"
#include "server-functions.h"

#define	VERSION_STR	"tlc-0.01"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

void usage (void) {
  printf ("%s\n",  FullVersionStr);
  printf ("usage: tlc [-v] [-h] <TL-schema-file> [program-file]\n"
      "\tTL compiler\n"
      "\t-v\toutput statistical and debug information into stderr\n"
      "\t-o<filename>\toutput schema's expressions with magic-numbers to given file\n"
      "\t-E\twhenever is possible output to stdout expressions\n"
      "\t-t\ttest mode - trying to unserialize TLC output\n");
  exit (2);
}

#define BUFFSIZE 0x1000000
int buff[BUFFSIZE];
static int output_expressions = 0;
static int test_mode = 0;

int main (int argc, char *argv[]) {
  int i;
  char *expr_filename = NULL;
  set_debug_handlers ();
  while ((i = getopt (argc, argv, "Eho:tv")) != -1) {
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
    case 't':
      test_mode = 1;
      break;
    case 'v':
      verbosity++;
      break;
    }
  }

  if (argc != optind + 1 && argc != optind + 2) {
    usage ();
  }
  char *prog_filename = (argc == optind + 2) ? argv[optind+1] : NULL;

  init_dyn_data ();

  struct tl_compiler c;
  if (tl_schema_read_from_file (&c, argv[optind]) < 0) {
    kprintf ("Error in \"%s\"\n", argv[optind]);
    tl_compiler_print_errors (&c, stderr);
    exit (1);
  }

  if (expr_filename) {
    if (tl_write_expressions_to_file (&c, expr_filename) < 0) {
      tl_compiler_print_errors (&c, stderr);
      exit (1);
    }
  }

  if (prog_filename) {
    struct tl_scheme_object *O = tl_scheme_parse_file (&c, prog_filename);
    if (O == NULL) {
      kprintf ("Error in \"%s\"\n", prog_filename);
      tl_compiler_print_errors (&c, stderr);
      exit (1);
    }

    if (verbosity) {
      tl_scheme_object_dump (stderr, O);
      fprintf (stderr, "\n");
    }

    int r = tl_serialize (&c, O, buff, BUFFSIZE);
    if (r < 0) {
      tl_compiler_print_errors (&c, stderr);
      exit (1);
    }
    for (i = 0; i < r; i++) {
      fprintf (stdout, "0x%08x", buff[i]);
      if (output_expressions) {
        struct tl_expression *E = tl_expression_find_by_magic (&c, buff[i]);
        if (E) {
          fprintf (stdout, "\t%s", E->text);
        }
      }
      fprintf (stdout, "\n");
    }
    tl_scheme_object_free (O);

    if (test_mode) {
      struct tl_buffer b;
      tl_string_buffer_init (&b);
      i = 0;
      while (i < r) {
        int o = tl_unserialize (&c, &b, buff + i, r - i, 2);
        if (o < 0) {
          tl_compiler_print_errors (&c, stderr);
          break;
        }
        i += o;
        tl_string_buffer_append_char (&b, '\n');
      }
      tl_string_buffer_append_char (&b, 0);
      fprintf (stdout, "%s", b.buff);
      tl_string_buffer_free (&b);
    }
  }

  tl_compiler_free (&c);
  return 0;
}

