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

char buff[1000000];
int parse_pos;

void init_parse (void) {
  parse_pos = 0;
}

struct ipv6 {
  unsigned short data[8];
};

struct ipv6 parse_ip (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  while (buff[parse_pos] == ' ') {
    parse_pos ++;
  }
  assert (buff[parse_pos ++] == '"');
  int p = 0;
  struct ipv6 a;
  a.data[0] = 0;

  while (buff[parse_pos] != '"' ) {
    if (buff[parse_pos] == ':') {
      assert (p <= 6);
      a.data[++p] = 0;
    } else {
      char c = buff[parse_pos];
      assert (('0' <= c && c <= '9') || (c >= 'a' && c <= 'f'));
      if (c <= '9' && c >= '0') {
        a.data[p] = a.data[p] * 16 + c - '0';
      } else {
        a.data[p] = a.data[p] * 16 + c + 10 - 'a';
      }
    }
    parse_pos ++;
  }
  //assert (p == 7);
  assert (buff[parse_pos ++] == '"');
  assert (!buff[parse_pos] || buff[parse_pos] == ',' || buff[parse_pos] == 10 || buff[parse_pos] == 13);
  return a;
}

void skip_int (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  while (buff[parse_pos] == ' ') {
    parse_pos ++;
  }
  assert (buff[parse_pos ++] == '"');
  while (buff[parse_pos] != '"') {
    char c = buff[parse_pos ++];
    assert (c >= '0' && c <= '9');
  }
  assert (buff[parse_pos ++] == '"');
  assert (!buff[parse_pos] || buff[parse_pos] == ',' || buff[parse_pos] == 10 || buff[parse_pos] == 13);
}

unsigned parse_int (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  int a, x;
  assert (sscanf (buff + parse_pos, "\"%u\"%n", &a, &x) == 1);
  parse_pos += x;
  assert (!buff[parse_pos] || buff[parse_pos] == ',' || buff[parse_pos] == 10 || buff[parse_pos] == 13);
  return a;
}

unsigned parse_country (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  while (buff[parse_pos] == ' ') {
    parse_pos ++;
  }
  unsigned r = 0;
  assert (buff[parse_pos ++] == '"');
  r = buff[parse_pos ++];
  r = r * 256 + buff[parse_pos ++];
  assert (buff[parse_pos ++] == '"');
  assert (!buff[parse_pos] || buff[parse_pos] == ',' || buff[parse_pos] == 10 || buff[parse_pos] == 13);
  return r;
}

int parse_string (void) {
  if (buff[parse_pos] == ',') {
    parse_pos ++;
  }
  while (buff[parse_pos] == ' ') {
    parse_pos ++;
  }
  assert (buff[parse_pos ++] == '"');
  int l = 0;
  while (buff[parse_pos ++] != '"') {
    assert (buff[parse_pos]);
    l++;
  }
  assert (!buff[parse_pos] || buff[parse_pos] == ',' || buff[parse_pos] == 10 || buff[parse_pos] == 13);
  return l + 1;
}

int end_parse (void) {
  return !buff[parse_pos] || (buff[parse_pos] == 10 ) || (buff[parse_pos] == 13);
}


struct tree {
  struct ipv6 num;
  int level;
  int text;
  double extra;
  struct tree *left;
  struct tree *right;
};

struct tree root = {.num = {.data = {0,0,0,0,0,0,0,0}}, .level = 0, .text = -1, .left = 0, .right = 0};
int total = 1;
int rules;

void set_bit (struct ipv6 *a, int level) {
  assert (level < 128);
  int q = (7 - (level / 16));
  a->data[q] += (1 << (level & 15));
  if (!a->data[q]) {
    q --;
    while (q >= 0) {
      a->data[q] ++;
      if (a->data[q]) {
        return;
      }
      q --;
    }
  }
}

int greater_ip (struct ipv6 a, struct ipv6 b) {
  int i;
  for (i = 0; i < 8; i++) {
    if (a.data[i] > b.data[i]) { 
      return 1;
    }
    if (a.data[i] < b.data[i]) {
      return 0;
    }
  }
  return 0;
}

void dec_ip (struct ipv6 *a) {
  int i = 7;
  while (i >= 0) {
    if (a->data[i] > 0) {
      a->data[i] --;
      return;
    } else {
      a->data[i] = 0xffff;
      i --;
    }
  }
}

double deg2 (int n) {
  double e = 1;
  int i;
  for (i = 0; i < n; i++) {
    e *= 2;
  }
  return e;
}

void tdup (struct tree* tree) {
  assert (!tree->left);
  assert (!tree->right);
  assert (tree->level < 128);
  //assert (tree->text == -1);
  tree->left = malloc (sizeof (struct tree));
  tree->left->num = tree->num;
  tree->left->level = tree->level + 1;
  tree->left->left = 0;
  tree->left->right = 0;
  tree->left->extra = 0;
  tree->left->text = tree->text;
  tree->left->extra = deg2 (127 - tree->level);
  tree->right = malloc (sizeof (struct tree));
  tree->right->num = tree->num; 
  set_bit (&tree->right->num, 127 - tree->level);
  //+ (1 << (31 - tree->level));
  tree->right->level = tree->level + 1;
  tree->right->left = 0;
  tree->right->right = 0;
  tree->right->extra = 0;
  tree->right->text = tree->text;
  tree->right->extra = tree->left->extra;
  tree->text = 0;
  total += 2;
}
#define IPV6_PRINT_STR "%04x:%04x:%04x:%04x:%04x:%04x:%04x:%04x"
#define IPV6_PRINT_STR2 "%04x%04x%04x%04x%04x%04x%04x%04x"
#define IPV6_TO_PRINT(x) (int)x.data[0], (int)x.data[1], (int)x.data[2], (int)x.data[3], (int)x.data[4], (int)x.data[5], (int)x.data[6], (int)x.data[7]

void add (struct tree* tree, struct ipv6 start, struct ipv6 end, unsigned text) {
  struct ipv6 s = tree->num;
  struct ipv6 f = s;
  if (tree->level > 0) {
    set_bit (&f, 128 - tree->level);
  }
  dec_ip (&f);
  vkprintf (5, "??: level = %d, start = " IPV6_PRINT_STR " end = " IPV6_PRINT_STR " s = " IPV6_PRINT_STR " f = " IPV6_PRINT_STR "\n", tree->level, IPV6_TO_PRINT (start), IPV6_TO_PRINT (end), IPV6_TO_PRINT (s), IPV6_TO_PRINT (f));
  //unsigned f = tree->num + (tree->level == 0 ? 0 : (1 << (32 - tree->level))) - 1;
  //fprintf (stderr, "%u %u %u %u\n", s, f, start, end);
  assert (!greater_ip (s, f));
  assert (!greater_ip (start, end));
  //assert (f >= s);
  //assert (start <= end);
  if (greater_ip (s, end) || greater_ip (start, f)) {
    //fprintf (stderr, "ERR: start = " IPV6_PRINT_STR " end = " IPV6_PRINT_STR " s = " IPV6_PRINT_STR " f = " IPV6_PRINT_STR "\n", IPV6_TO_PRINT (start), IPV6_TO_PRINT (end), IPV6_TO_PRINT (s), IPV6_TO_PRINT (f));
    return;
  }
  //if (s > end || f < start) {
  //  return;
  //}
  //if (start <= s && f <= end) {
  if (!greater_ip (start, s) && !greater_ip (f, end)) {
    //fprintf (stderr, "OK: start = " IPV6_PRINT_STR " end = " IPV6_PRINT_STR " s = " IPV6_PRINT_STR " f = " IPV6_PRINT_STR "\n", IPV6_TO_PRINT (start), IPV6_TO_PRINT (end), IPV6_TO_PRINT (s), IPV6_TO_PRINT (f));
    assert (!tree->left);
    assert (!tree->right);
    //assert (tree->text == -1);
    assert (tree->level >= 1);
    tree->text = text;
    tree->extra = deg2 (128 - tree->level);
    return;
  }
  if (!tree->left) {
    assert (!tree->right);
    tdup (tree);
  }
  //fprintf (stderr, "BAD: start = " IPV6_PRINT_STR " end = " IPV6_PRINT_STR " s = " IPV6_PRINT_STR " f = " IPV6_PRINT_STR "\n", IPV6_TO_PRINT (start), IPV6_TO_PRINT (end), IPV6_TO_PRINT (s), IPV6_TO_PRINT (f));
  assert (!tree->text);
  add (tree->left, start, end, text);
  add (tree->right, start, end, text);
}

double count (struct tree *tree, int text) {
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
  double c1 = count (tree, tree->left->text);
  double c2 = count (tree, tree->right->text);
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
    struct ipv6 start_ip = parse_ip ();
    assert (!end_parse ());
    struct ipv6 end_ip = parse_ip ();
    assert (!end_parse ());
    skip_int ();
    assert (!end_parse ());
    skip_int ();
    assert (!end_parse ());
    //assert (start_ip == parse_int ());
    //assert (!end_parse ());
    //assert (end_ip == parse_int ());
    //assert (!end_parse ());
    unsigned country = parse_country ();
    assert (country);
    assert (!end_parse ());
    assert (parse_string ());
    assert (end_parse ());
    vkprintf (3, "start = " IPV6_PRINT_STR " end = " IPV6_PRINT_STR "\n", IPV6_TO_PRINT (start_ip), IPV6_TO_PRINT (end_ip));
    add (&root, start_ip, end_ip, country);
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

#define IPV6_PRINT_STR "%04x:%04x:%04x:%04x:%04x:%04x:%04x:%04x"
#define IPV6_TO_PRINT(x) (int)x.data[0], (int)x.data[1], (int)x.data[2], (int)x.data[3], (int)x.data[4], (int)x.data[5], (int)x.data[6], (int)x.data[7]

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
      printf (IPV6_PRINT_STR2 " %d %c%c\n", IPV6_TO_PRINT(tree->num), tree->level, (char)(tree->text >> 8), (char)(tree->text & 255));
    } else {
      printf (IPV6_PRINT_STR2 " %d %c%c\n", IPV6_TO_PRINT(tree->num), tree->level, '?', '?');
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
          "[-s] number of lines to skip\n",
          (int) sizeof(void *) * 8
  );
}

int main (int argc, char **argv) {
  int i;
  int skip_lines = 0;
  set_debug_handlers ();

  while ((i = getopt (argc, argv, "i:o:vhs:")) != -1) {
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
