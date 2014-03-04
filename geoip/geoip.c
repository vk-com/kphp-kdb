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

    Copyright 2012 Vkontakte Ltd
              2012 Vitaliy Valtman
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <assert.h>
#include <signal.h>
#include <errno.h>

#include "server-functions.h"

#define VERSION "1.00"
#define VERSION_STR "geoip "VERSION

#ifndef COMMIT
#  define COMMIT ""
#endif


const char *input_file, *output_file;
int verbosity;
int mode;

char buff[1000000];
int parse_pos;

void init_parse (void) {
  parse_pos = 0;
}

unsigned parse_ip (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  unsigned a,b,c,d;
  int x;
  if (!mode) {
    assert (sscanf (buff + parse_pos, "\"%u.%u.%u.%u\"%n", &a, &b, &c, &d, &x) == 4);
  } else if (mode == 2) {
    assert (sscanf (buff + parse_pos, "%u.%u.%u.%u%n", &a, &b, &c, &d, &x) == 4);
  } else {
    assert (0);
  }
  assert (0 <= a && a < 256);
  assert (0 <= b && b < 256);
  assert (0 <= c && c < 256);
  assert (0 <= d && d < 256);
  parse_pos += x;
  assert (!buff[parse_pos] || buff[parse_pos] == ',');
  return (a << 24) + (b << 16) + (c << 8) + d;
}

void parse_ip_mask (unsigned *s, unsigned *f) {
  int x;
  unsigned a, b, c, d, m;
  assert (sscanf (buff + parse_pos, "%u.%u.%u.%u/%u%n", &a, &b, &c, &d, &m, &x) == 5);
  assert (0 <= a && a < 256);
  assert (0 <= b && b < 256);
  assert (0 <= c && c < 256);
  assert (0 <= d && d < 256);
  assert (0 < m && m <= 32);
  m = 32 - m;
  parse_pos += x;
  assert (!buff[parse_pos] || buff[parse_pos] == ',');
  *s = (a << 24) + (b << 16) + (c << 8) + d;
  m = (1 << m) - 1;
  //fprintf (stderr, "%x %x\n", *s, m);
  assert (!(*s & m));
  *f = *s + m;
}

unsigned parse_int (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  int a, x;
  if (!mode) {
    assert (sscanf (buff + parse_pos, "\"%u\"%n", &a, &x) == 1);
  } else {
    assert (sscanf (buff + parse_pos, "%u%n", &a, &x) == 1);
  }
  parse_pos += x;
  assert (!buff[parse_pos] || buff[parse_pos] == ',' || buff[parse_pos] == 10 || buff[parse_pos] == 13);
  return a;
}

unsigned parse_country (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  unsigned r = 0;
  assert (buff[parse_pos ++] == '"');
  if (buff[parse_pos] != '"') {
    r = buff[parse_pos ++];
    r = r * 256 + buff[parse_pos ++];
  }
  assert (buff[parse_pos ++] == '"');
  assert (!buff[parse_pos] || buff[parse_pos] == ',' || buff[parse_pos] == 10 || buff[parse_pos] == 13);
  return r;
}

int parse_string (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  assert (buff[parse_pos ++] == '"');
  int l = 0;
  while (buff[parse_pos ++] != '"') {
    assert (buff[parse_pos]);
    l++;
  }
  assert (!buff[parse_pos] || buff[parse_pos] == ',' || buff[parse_pos] == 10 || buff[parse_pos] == 13);
  return l;
}

int end_parse (void) {
  return !buff[parse_pos] || (buff[parse_pos] == 10 ) || (buff[parse_pos] == 13);
}

int skip_parse (void) {
  int cc = 0;
  while (buff[parse_pos] && buff[parse_pos] != 10 && buff[parse_pos] != 13) {
    if (buff[parse_pos] == ',') {
      cc ++;
    }
    parse_pos ++;
  }
  return cc;
}


struct tree {
  unsigned num;
  int level;
  int text;
  unsigned extra;
  unsigned mask;
  struct tree *left;
  struct tree *right;
};

struct tree root = {.num = 0, .level = 0, .text = -1, .left = 0, .right = 0, .mask = 0xffffffff};
int total = 1;
int rules;

void tdup (struct tree* tree) {
  assert (!tree->left);
  assert (!tree->right);
  assert (tree->level < 32);
  if (verbosity >= 3) {
    fprintf (stderr, "%u.%u.%u.%u\n", tree->num >> 24, (tree->num >> 16) & 0xff, (tree->num >> 8) & 0xff, tree->num & 0xff);
  }
  //assert (tree->text == -1);
  tree->left = malloc (sizeof (struct tree));
  tree->left->num = tree->num;
  tree->left->level = tree->level + 1;
  tree->left->left = 0;
  tree->left->right = 0;
  tree->left->extra = 0;
  tree->left->text = tree->text;
  tree->left->extra = (1 << (31 - tree->level));
  tree->left->mask = tree->mask;
  tree->right = malloc (sizeof (struct tree));
  tree->right->num = tree->num + (1 << (31 - tree->level));
  tree->right->level = tree->level + 1;
  tree->right->left = 0;
  tree->right->right = 0;
  tree->right->extra = 0;
  tree->right->text = tree->text;
  tree->right->extra = (1 << (31 - tree->level));
  tree->right->mask = tree->mask;
  tree->text = 0;
  total += 2;
}

void add (struct tree* tree, unsigned start, unsigned end, unsigned text, unsigned d) {
  unsigned s = tree->num;
  unsigned f = tree->num + (tree->level == 0 ? 0 : (1 << (32 - tree->level))) - 1;
  if (verbosity >= 4) {
    fprintf (stderr, "%u %u %u %u\n", s, f, start, end);
  }
  assert (f >= s);
  assert (start <= end);
  if (s > end || f < start) {
    return;
  }
  if (start <= s && f <= end) {
    if (!tree->left && !tree->right) {
      if (tree->mask >= d) {
        tree->text = text;
        tree->mask = d;
      }
    } else {
      assert (tree->left);
      assert (tree->right);
      add (tree->left, start, end, text, d);
      add (tree->right, start, end, text, d);
    }
/*    assert (!tree->left);
    assert (!tree->right);
    if (tree->text != -1) {
      fprintf (stderr, "tree->text = %d\n", tree->text);
    }
    assert (tree->text == -1);
    tree->text = text;
    tree->extra = (1 << (32 - tree->level));*/
    return;
  }
  if (!tree->left) {
    assert (!tree->right);
    tdup (tree);
  }
  assert (!tree->text);
  add (tree->left, start, end, text, d);
  add (tree->right, start, end, text, d);
}

unsigned count (struct tree *tree, int text) {
  if (!tree) {
    return 0;
  }
  if (tree->text == text) {
    return tree->extra;
  }
  if (tree->text != 0) {
    return 0;
  }
  return count (tree->left, text) + count (tree->right, text);
}

void pack (struct tree *tree) {
  if (!tree) {
    return;
  }
  if (tree->text) {
    assert (!tree->left);
    assert (!tree->right);
    return;
  }
  assert (tree->left);
  assert (tree->right);
  pack (tree->left);
  pack (tree->right);
  if (tree->left->text == tree->right->text) {
    tree->text = tree->left->text;
    tree->extra = tree->left->extra + tree->right->extra;
    assert (tree->text);
    return;
  }
  unsigned c1 = count (tree, tree->left->text);
  unsigned c2 = count (tree, tree->right->text);
  if (c1 < c2) {
    tree->text = tree->right->text;
    tree->extra = c2;
  } else {
    tree->text = tree->left->text;
    tree->extra = c1;
  }
  //fprintf (stderr, "%d %u\n", tree->text, tree->extra);
  assert (tree->text);
  return;
}

void run (void) {
  while (1) {
    if (!fgets (buff, 1000000, stdin)) {
      break;
    }
    init_parse ();
    if (*buff == 10 || *buff == 13 || *buff == '#') {
      continue;
    }
    unsigned start_ip, end_ip, country, region;
    if (!mode) {
      start_ip = parse_ip ();
      assert (!end_parse ());
      end_ip = parse_ip ();
      assert (!end_parse ());
      assert (start_ip == parse_int ());
      assert (!end_parse ());
      assert (end_ip == parse_int ());
      assert (!end_parse ());
      country = parse_country ();
      assert (country);
      assert (!end_parse ());
      assert (parse_string ());
      assert (end_parse ());
    } else if (mode == 1) {
      parse_ip_mask (&start_ip, &end_ip);
      assert (!end_parse ());
      country = parse_int ();
      assert (end_parse ());
    } else if (mode == 2) {
      start_ip = parse_ip ();
      assert (!end_parse ());
      end_ip = parse_ip ();
      assert (!end_parse ());
      country = parse_country ();
      assert (country);
      assert (!end_parse ());
      region = parse_country ();
      assert (!end_parse ());
      assert (skip_parse () == 6);
      assert (end_parse ());
      country = (country << 16) + region;
    } else {
      assert (0);
    }
    if (verbosity >= 3) {
      #define IP(a) (a >> 24) & 0xff, (a >> 16) & 0xff, (a >> 8) & 0xff, (a >> 0) & 0xff
      fprintf (stderr, "add: start_ip = %u.%u.%u.%u, end_ip = %u.%u.%u.%u, color = %d\n", IP(start_ip), IP(end_ip), country);
    }
    add (&root, start_ip, end_ip, country, end_ip - start_ip);
    rules ++;
  }
  if (verbosity) {
    fprintf (stderr, "total %d rules\n", rules);
    fprintf (stderr, "total %d vertices\n", total);
  }
  pack (&root);
}

void init () {}
int total_out;

void dump (struct tree *tree, int color) {
  if (!tree) {
    return;
  }
  assert (color);
  assert (tree->text);
  if (tree->text != color) {
    unsigned c = count (tree, color);
    if (c >= tree->extra) {
      tree->text = color;
      tree->extra = c;
    }
  }
  if (color != tree->text) {
    if (tree->text != -1) {
      if (!mode) {
        printf ("%u %d %c%c\n", tree->num, tree->level, (char)(tree->text >> 8), (char)(tree->text & 255));
      } else if (mode == 1) {
        printf ("%u %d %02d\n", tree->num, tree->level, tree->text);
      } else if (mode == 2) {
        if (tree->text & 255) {
          printf ("%u %d %c%c/%c%c\n", tree->num, tree->level, (char)(tree->text >> 24), (char)(tree->text >> 16), (char )(tree->text >> 8), (char)(tree->text));
        } else {
          printf ("%u %d %c%c\n", tree->num, tree->level, (char)(tree->text >> 24), (char)(tree->text >> 16));
        }
      } else {
        assert (0);
      }
    } else {
      if (!mode || mode == 2) {
        printf ("%u %d %c%c\n", tree->num, tree->level, '?', '?');
      } else if (mode == 1) {
        printf ("%u %d 00\n", tree->num, tree->level);        
      } else {
        assert (0);
      }
    }
    total_out ++;
  }
  dump (tree->left, tree->text);
  dump (tree->right, tree->text);
}
void help (void) {
  printf (VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " (%d-bit) after commit " COMMIT "\n"
          "[-i input_file] file with database in csv-format\n"
          "[-o output_file] file to write result. Default - stdout\n"
          "[-v] increase verbosity\n"
          "[-s] number of lines to skip\n"
          "[-m] increase mode (0 - maxmind data format, 1 - cache-engine format, 2- maxmind region data format)\n",
          (int) sizeof(void *) * 8
  );
}

int main (int argc, char **argv) {
  int i;
  int skip_lines = 0;
  set_debug_handlers ();

  while ((i = getopt (argc, argv, "i:o:vhs:m")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      help();
      return 2;
    case 'i':
      input_file = optarg;
      break;
    case 'o':
      output_file = optarg;
      break;
    case 's':
      skip_lines = atoi (optarg);
      break;
    case 'm':
      mode ++;
      break;
    default:
      help ();
      return 2;
    }
  }

  if (argc != optind || !input_file) {
    help();
    return 2;
  }

  if (output_file) {
    assert (freopen (output_file, "wt", stdout));
  }

  printf ("%d\n", mode);
  assert (freopen (input_file, "rt", stdin));
  for (i = 0; i < skip_lines; i++) {
    assert (fgets (buff, 1000000, stdin));
  }
  
  init ();
  run ();
  dump (&root, -1);
  if (verbosity) {
    fprintf (stderr, "Total %d rules\n", total_out);
  }

  return 0;
}
