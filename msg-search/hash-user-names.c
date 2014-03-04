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

#define MAIL_INDEX_BLOCK_MAGIC	0x11ef55aa

#define BUFFSIZE (1L << 24)
#define WATERMARK (1L << 16)
#define TEXT_SIZE (1L << 14)
#define MAX_PAIRS (1L << 23)
#define MAX_DATA  (1L << 23)

#define PRIME	1000003

#define HEAP_SIZE (1666L << 20)

typedef struct entry entry_t;

unsigned char is_letter[256];

int verbose = 0;
long long tot_Pc;

int tot_users;

char Buff[3][BUFFSIZE+4];

int fd[2];
char *rptr[3], *wptr[3];
entry_t *cur[2];

struct entry {
  long user_id;
  long len;
  char text[TEXT_SIZE]; 
};

#define	RQ_SIZE	4
entry_t RQueue[2][RQ_SIZE];
int RQueue_pos[2];

typedef unsigned long long hash_t;
typedef struct userpair userpair_t;

struct userpair {
  long user_id;
  hash_t hash;
};

entry_t *load_entry (int x) {
  char *ptr = rptr[x], *eptr = wptr[x], i, *to;
  int r, c, state;
  entry_t *E = RQueue[x] + RQueue_pos[x];
  RQueue_pos[x] = (RQueue_pos[x] + 1) & (RQ_SIZE - 1);
  if (ptr >= Buff[x]+BUFFSIZE-WATERMARK) {
    memcpy (Buff[x], ptr, eptr - ptr);
    wptr[x] = eptr -= ptr - Buff[x];
    ptr = rptr[x] = Buff[x];
    r = read (fd[x], eptr, BUFFSIZE - (eptr - ptr));
    fprintf (stderr, "%d bytes read\n", r);
    assert (r >= 0);
    wptr[x] = eptr += r;
    *eptr = 0;
  }
  if (ptr == eptr) { return cur[x] = 0; }
  E->user_id = strtol (ptr, &ptr, 10);
  assert (*ptr++ == '\t');
  to = E->text;
  state = 0;
  for (i = 0; i < 3; i++) {
    while (1) {
      assert (ptr < eptr && to < E->text + TEXT_SIZE - 4);
      c = *ptr++;
      if (c == '\n' || c == '\t') {
        if (state == '\\') to--;
        else break;
      }
      if (c == '\\' && state == '\\') {
        state = 0;
        to--;
      } else state = c;
      *to++ = c;
    }
    assert (c == i["\t\t\n"]);
    *to++ = c;
  }
  rptr[x] = ptr;
  *--to = 0;
  E->len = to - E->text;
  return cur[x] = E;
}

void flushout (void) {
  int w, s;
  if (rptr[2] < wptr[2]) {
    s = wptr[2] - rptr[2];
    w = write (1, rptr[2], s);
    assert (w == s);
  }
  rptr[2] = wptr[2] = Buff[2];
}

void writeout (const void *D, size_t len) {
  const char *d = D;
  while (len > 0) {
    int r = Buff[2] + BUFFSIZE - wptr[2];
    if (r > len) r = len;
    memcpy (wptr[2], d, r);
    d += r;
    wptr[2] += r;
    len -= r;
    if (len > 0) flushout();
  }                                
}


void process_text (char *text, long user_id) {
  //fprintf (stderr, "%ld %s\n", user_id, text);
  while (is_letter[(unsigned char)*text] == 32) text++;

  while (*text) {
    char *sav = text;
    union {
      unsigned char data[16];
      hash_t hash;
    } md5_h;
    static userpair_t Tmp;

    for (;is_letter[(unsigned char)*text] > 32;++text) *text = is_letter[(unsigned char)*text];

    md5 ((unsigned char *) sav, text - sav, md5_h.data);

    Tmp.hash = md5_h.hash;
    Tmp.user_id = user_id;

    //fprintf (stderr, "%llu %d %d\n", P[Pc].hash, P[Pc].order, P[Pc].message_id);

    writeout (&Tmp, sizeof(Tmp));
    tot_Pc++;

    for (;is_letter[(unsigned char)*text] == 32; ++text);
  }
}

void init_is_letter (void) {
  int i;

  memset (is_letter, 32, sizeof (is_letter));
  is_letter[0] = 0;

  for (i = 'A'; i <= 'Z'; i++) is_letter[i] = i + 32;
  for (i = 'a'; i <= 'z'; i++) is_letter[i] = i;
  is_letter[0xa8] = is_letter[0xb8] = 0xe5;
  for (i = 0xc0; i <= 0xdf; i++) is_letter[i] = i + 32;
  for (i = 0xe0; i <= 0xff; i++) is_letter[i] = i;
}

int percent (long long a, long long b) {
  if (b <= 0 || a <= 0) return 0;
  if (a >= b) return 100;
  return (a*100 / b);
}

void output_stats (void) {
  fprintf (stderr, "%lld words hashed describing %d users\n", 
	   tot_Pc, tot_users);
}

char *progname = "hash-user-names";

void usage (void) {
  printf ("usage: %s [-v] <members-fname-1> [<members-fname-2> ...]\n"
	  "\tResulting hash list file is written to stdout\n"
	  "\t-v\toutput statistical information into stderr\n",
	  progname);
  exit(2);
}


int main (int argc, char *argv[]) {
  int i;
  entry_t *E;
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

  init_is_letter ();

  rptr[2] = wptr[2] = Buff[2];

  while (optind < argc) {
    fd[0] = open (argv[optind], O_RDONLY);
    if (fd[0] < 0) {
      fprintf (stderr, "%s: cannot open() %s: %m\n", progname, argv[optind]);
      optind++;
      continue;
    }
    assert (fd[0] >= 0);
    rptr[0] = wptr[0] = Buff[0] + BUFFSIZE;

    while ((E = load_entry(0)) != 0) {
      tot_users++;
      process_text (E->text, E->user_id);
    }

    close (fd[0]);
    optind++;
  }

  flushout();

  if (verbose) {
    output_stats();
  }
                                      
  return 0;
}
