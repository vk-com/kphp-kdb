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
#define USERNAME_HASH_INDEX_MAGIC	0x11ef3ae6

#define BUFFSIZE (1L << 24)
#define WATERMARK (1L << 16)
#define TEXT_SIZE (1L << 14)
#define MAX_PAIRS (1L << 23)
#define MAX_DATA  (1L << 23)
#define MAX_USERINDEX_SIZE (588L << 20)

#define PRIME	1000003

#define HEAP_SIZE (1333L << 20)

typedef struct entry entry_t;

unsigned char is_letter[256];

int mod = 1, rem = 0, verbose = 0;
int msgs_read, msgs_analyzed, date_adj;
long long tot_Pc, tot_Ps, tot_Dc, tot_bad_senders, tot_known_senders, tot_sender_hashes;
int max_Pc, max_Ps, max_Dc, max_Pc_user, max_Ps_user, max_Dc_user;
char user_index_fname[256];

int tot_users, User[PRIME], UserList[PRIME];
entry_t *LastMsg[PRIME];

char Buff[3][BUFFSIZE+4];

int fd[2];
char *rptr[3], *wptr[3];
entry_t *cur[2];

struct entry {
  long user_id;
  long peer_id;
  long message_id;
  long date;
  long len;
  entry_t *prev;
  char text[TEXT_SIZE]; 
};

entry_t Infty, Tmp;

#define	RQ_SIZE	4
entry_t RQueue[2][RQ_SIZE];
int RQueue_pos[2];

typedef unsigned long long hash_t;
typedef struct pair pair_t;

struct pair {
  hash_t hash;
  long order;
  long message_id;
};

static int Pc = 0, Ps = 0;
pair_t P[MAX_PAIRS];

static int Dc = 0;
long D[MAX_DATA];

struct username_index {
  long magic;
  long users;
  long offset[MAX_USERINDEX_SIZE >> 2];
} UserIndex;
int ui_size = -1, ui_bytes = 0;
hash_t *ui_start;

struct user_header {
  long magic;
  long user_id;
  long hash_cnt;
  long list_cnt;
} UH;


char *Heap, *heap_a;

void *mymalloc (size_t s) {
  void *res = heap_a;
  if (s > (1L << 25) || heap_a + s > Heap + HEAP_SIZE) return 0;
  heap_a += (s + 3) & -4;
  return res;
}
              
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
    assert (r >= 0);
    wptr[x] = eptr += r;
    *eptr = 0;
  }
  if (ptr == eptr) { return cur[x] = &Infty; }
  if (x) {
    E->message_id = -strtol (ptr, &ptr, 10);
    assert (*ptr++ == '\t');
    E->user_id = strtol (ptr, &ptr, 10);
    assert (*ptr++ == '\t');
    E->peer_id = strtol (ptr, &ptr, 10);
    assert (*ptr++ == '\t');
  } else {
    E->message_id = strtol (ptr, &ptr, 10);
    assert (*ptr++ == '\t');
    E->peer_id = strtol (ptr, &ptr, 10);
    assert (*ptr++ == '\t');
    E->user_id = strtol (ptr, &ptr, 10);
    assert (*ptr++ == '\t');
  }               
  strtol (ptr, &ptr, 10);  // to_reply
  assert (*ptr++ == '\t');
  E->date = strtol (ptr, &ptr, 10); // date
  assert (*ptr++ == '\t');
  for (i = 0; i < 8; i++) {
    while (*ptr > ' ') ptr++;
    assert (*ptr++ == '\t');
  }
  to = E->text;
  state = 0;
  for (i = 0; i < 2; i++) {
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
    assert (c == i["\t\n"]);
    *to++ = c;
  }
  rptr[x] = ptr;
  *--to = 0;
  E->len = to - E->text;
  return cur[x] = E;
}

entry_t *store_entry (entry_t *E) {
  int r = sizeof(entry_t) - TEXT_SIZE + E->len + 1;
  entry_t *R = mymalloc (r);
  assert (R);
  memcpy (R, E, r);
  return R;
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


int get_hash (int x) {
  assert (x > 0);
  int h1 = x % PRIME, h2 = x % (PRIME - 1) + 1;
  while (User[h1] && User[h1] != x) {
    h1 += h2;
    if (h1 >= PRIME) h1 -= PRIME;
  }
  if (!User[h1]) {
    User[h1] = x;
    assert (tot_users < PRIME / 2);
    UserList[tot_users++] = x;
  };
  
  return h1;
};

void process (entry_t *E) {
  int mid = E->user_id;
  int h = get_hash (mid);
  msgs_read++;
  if (mid % mod == rem) {
    E->prev = LastMsg [h];
    LastMsg[h] = store_entry (E);
    msgs_analyzed++;
  }
//  printf ("%ld %ld %ld %ld %s\n", E->user_id, E->peer_id, E->message_id, E->date, E->text);
}


int icmp (const void *a, const void *b) {
  return *(int *)a - *(int *)b;
}

void process_text_sender (int user_id, int msg_id, int order) {
  int a, b;
  if (user_id <= 0 || user_id >= UserIndex.users-1) {
    tot_bad_senders++;
    return;
  }

  a = UserIndex.offset[user_id];
  b = UserIndex.offset[user_id+1];
  if (!(a >= 0 && a <= b && b <= ui_size)) {
    fprintf (stderr, "%d %d %d %d\n", user_id, a, b, ui_size);
  }
  assert (a >= 0 && a <= b && b <= ui_size);

  if (a == b) {
    tot_bad_senders++;
    return;
  }

  tot_known_senders++;
  tot_sender_hashes += b-a;

  while (a < b) {
    P[Pc].hash = ui_start[a++];
    // fprintf (stderr, "import hash %llu for user %d\n", P[Pc].hash, user_id);
    P[Pc].order = order;
    P[Pc].message_id = msg_id;
    Pc++;
  }
}

void process_text (char *text, int msg_id, int order) {
  //fprintf (stderr, "%d %d %s\n", msg_id, order, text);

  if (text[0] == 'R' && text[1] == 'e' && text[2] < 'A') { text += 2; }

  while (is_letter[(unsigned char)*text] == 32) text++;

  while (*text) {
    char *sav = text;
    union {
      unsigned char data[16];
      hash_t hash;
    } md5_h;

    for (;is_letter[(unsigned char)*text] > 32;++text) *text = is_letter[(unsigned char)*text];

    md5 ((unsigned char *) sav, text - sav, md5_h.data);

    P[Pc].hash = md5_h.hash;
    P[Pc].order = order;
    P[Pc].message_id = msg_id;

    //fprintf (stderr, "%llu %d %d\n", P[Pc].hash, P[Pc].order, P[Pc].message_id);

    ++Pc;

    assert (Pc <= MAX_PAIRS);

    for (;is_letter[(unsigned char)*text] == 32; ++text);
  }
}


int pcmp (const void *a, const void *b) {
  const pair_t *aa = a, *bb = b;
  if (aa->hash < bb->hash) return -1;
  if (aa->hash > bb->hash) return  1;
  return aa->order - bb->order;
}

void my_psort (int a, int b) {
  pair_t h, t;
  int i, j;
  if (a >= b) return;
  i = a;  j = b;
  h = P[(a+b)>>1];
  do {
    while (P[i].hash < h.hash || (P[i].hash == h.hash && P[i].order < h.order)) i++;
    while (P[j].hash > h.hash || (P[j].hash == h.hash && P[j].order > h.order)) j--;
    if (i <= j) {
      t = P[i];  P[i++] = P[j];  P[j--] = t;
    }
  } while (i <= j);
  my_psort (a, j);
  my_psort (i, b);
}


void process_user (int x) {
  int h = get_hash (x);
  entry_t *E;
  int cnt = 0;
  Pc = 0;

  //fprintf (stderr, "user %d\n", x);

  for (E = LastMsg[h]; E; E = E->prev, cnt++) {
    process_text_sender (E->peer_id, E->message_id, cnt);
    process_text (E->text, E->message_id, cnt);
  }

  if (!Pc) return;
  if (Pc > max_Pc) { max_Pc = Pc; max_Pc_user = x; }
  tot_Pc += Pc;

  // qsort (P, Pc, sizeof (pair_t), pcmp);
  my_psort (0, Pc-1);
  Dc = Ps = 0;

  // fprintf (stderr, "%d %llu\n", Pc, P[0].hash);


  for (cnt = 0; cnt < Pc;) {
    hash_t word = P[cnt].hash;
    int i = Dc, prev_id = 0;
    do {
      int cur_id = P[cnt++].message_id;
      if (cur_id != prev_id) {
        D[Dc++] = prev_id = cur_id;
      } 
    } while (cnt < Pc && P[cnt].hash == word);
    P[Ps].hash = word;
    P[Ps].order = Dc - i;
    if (Dc - i == 1) {
      P[Ps].message_id = D[--Dc];
    } else {
      P[Ps].message_id = i;
    }
    Ps++;
  }

  tot_Ps += Ps;
  if (Ps > max_Ps) { max_Ps = Ps;  max_Ps_user = x; }
  tot_Dc += Dc;
  if (Dc > max_Dc) { max_Dc = Dc;  max_Dc_user = x; }

  UH.magic = MAIL_INDEX_BLOCK_MAGIC;
  UH.user_id = x;
  UH.hash_cnt = Ps;
  UH.list_cnt = Dc;
  writeout (&UH, sizeof(UH));
  writeout (P, Ps*sizeof(P[0]));
  writeout (D, Dc*sizeof(D[0]));
}

static inline void fix_date (long a, entry_t *B, entry_t *C) {
  long min = a, max = C->date;
  if (min > max) { min = max; max = a; }
  if (B->date < min) { B->date = min; date_adj++; }
  else if (B->date > max) { B->date = max; date_adj++; }
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
  fprintf (stderr, "%d messages read, %d analyzed belonging to %d users\n", 
	   msgs_read, msgs_analyzed, tot_users);
  fprintf (stderr, "date adjusted %d times\n", date_adj);
  fprintf (stderr, "username data bytes: %d used out of %ld (%d%%)\n",
	   ui_bytes, (long)sizeof(UserIndex), percent (ui_bytes, sizeof(UserIndex)));
  fprintf (stderr, "messages with unknown correspondents: %lld out of %lld (%d%%)\n",
	   tot_bad_senders, tot_bad_senders+tot_known_senders, 
	   percent (tot_bad_senders, tot_bad_senders+tot_known_senders));
  fprintf (stderr, "imported username hashes: %lld out of %lld (%d%%)\n",
	   tot_sender_hashes, tot_Pc, percent (tot_sender_hashes, tot_Pc));
  fprintf (stderr, "messages buffer memory bytes: total %ld, used %ld (%d%%)\n",
	   HEAP_SIZE, (long)(heap_a - Heap), percent (heap_a - Heap, HEAP_SIZE));
  fprintf (stderr, "pairs:\ttotal %lld, maximal usage %d of %ld (%d%%) for user %d\n",
	   tot_Pc, max_Pc, MAX_PAIRS, percent (max_Pc, MAX_PAIRS), max_Pc_user);
  fprintf (stderr, "hashes:\ttotal %lld, maximal usage %d of %ld (%d%%) for user %d\n",
	   tot_Ps, max_Ps, MAX_PAIRS, percent (max_Ps, MAX_PAIRS), max_Ps_user);
  fprintf (stderr, "data:\ttotal %lld, maximal usage %d of %ld (%d%%) for user %d\n",
	   tot_Dc, max_Dc, MAX_DATA, percent (max_Dc, MAX_DATA), max_Dc_user);
}

char *progname = "index-messages";

void usage (void) {
  printf ("usage: %s [-v] [-w<modulus>:<remainder>] [-u<users-index-file>] <inbox-fname> <outbox-fname>\n"
	  "\tResulting index file is written to stdout\n"
	  "\t-v\toutput statistical information into stderr\n"
	  "\t-w\tprocess only users with user_id MOD modulus == remainder\n"
	  "\t-u\tload specified users index file for indexing message senders\n",
	  progname);
  exit(2);
}


int main (int argc, char *argv[]) {
  int i;
  entry_t *E0, *E1, *F0, *F1;
  static long date0 = 0, date1 = 0;
  progname = argv[0];
  while ((i = getopt (argc, argv, "hu:vw:")) != -1) {
    switch (i) {
    case 'v':
      verbose = 1;
      break;
    case 'w':
      assert (sscanf (optarg, "%d,%d\n", &mod, &rem) == 2);
      assert (rem >= 0 && rem < mod);
      break;
    case 'u':
      strncpy (user_index_fname, optarg, sizeof(user_index_fname)-1);
      break;
    case 'h':
      usage();
      return 2;
    }
  }
  if (argc != optind + 2) {
    usage();
    return 2;
  }

  heap_a = Heap = malloc (HEAP_SIZE);
  assert (Heap);

  init_is_letter ();

  if (*user_index_fname) {
    fd[0] = open (user_index_fname, O_RDONLY);
    if (fd[0] < 0) {
      fprintf (stderr, "%s: cannot open %s: %m\n", progname, user_index_fname);
      return 1;
    }
    i = ui_bytes = read (fd[0], &UserIndex, sizeof(UserIndex));
    close (fd[0]);
    assert (i >= 8 && i < sizeof(UserIndex));
    assert (UserIndex.magic == USERNAME_HASH_INDEX_MAGIC);
    assert (UserIndex.users < (MAX_USERINDEX_SIZE >> 2) - 20);
    ui_size = (i - 8 - UserIndex.users*4) >> 3;
    assert (ui_size > 0);
    ui_start = (hash_t *) (UserIndex.offset + UserIndex.users);
    if (verbose) {
      fprintf (stderr, "user index file %s loaded, %ld users, %d hashes\n",
	       user_index_fname, UserIndex.users, ui_size);
    }
  }

  for (i = 0; i < 2; i++) {
    fd[i] = open (argv[optind+i], O_RDONLY);
    assert (fd[i] >= 0);
    rptr[i] = wptr[i] = Buff[i] + BUFFSIZE;
  }
  rptr[2] = wptr[2] = Buff[2];

  Infty.date = 0x7fffffff;
  E0 = load_entry(0);
  F0 = load_entry(0);
  E1 = load_entry(1);        
  F1 = load_entry(1);
  fix_date (date0, E0, F0);
  fix_date (date1, E1, F1);

  while (E0 != &Infty || E1 != &Infty) {
    if (E0->date < E1->date) {
      process(E0);
      date0 = E0->date;
      E0 = F0;
      F0 = load_entry(0);
      fix_date (date0, E0, F0);
    } else {
      process(E1);
      date1 = E1->date;
      E1 = F1;
      F1 = load_entry(1);
      fix_date (date0, E0, F0);
    }
  }

  qsort (UserList, tot_users, sizeof (int), icmp);

  for (i = 0; i < tot_users; i++) {
    process_user (UserList[i]);
  }

  flushout();

  if (verbose) {
    output_stats();
  }
                                      
  return 0;
}
