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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>

#ifndef	O_BINARY
#define O_BINARY	0
#endif

#define MAIL_INDEX_BLOCK_MAGIC	0x11ef55aa

#define READ_AHEAD	(1L << 16)
#define MAX_USERS	(1L << 24)

typedef struct user_header {
  long magic;
  long user_id;
  long hash_cnt;
  long list_cnt;
} user_header_t;

typedef struct userlist_entry {
  long user_id;
  long hash_cnt;
  long list_cnt;
  long file_no;
  long long offset;
} userlist_entry_t;
  

int ifd, ofd;

long long cur_offs, load_offs;
int r, offs, N;

char buff[READ_AHEAD];
userlist_entry_t *P[MAX_USERS], A[MAX_USERS], B[MAX_USERS];

void my_sort (int a, int b) {
  userlist_entry_t *t;
  int h, i, j;
  if (a >= b) return;
  i = a;  j = b;
  h = P[(a+b)>>1]->user_id;
  do {
    while (P[i]->user_id < h) i++;
    while (P[j]->user_id > h) j--;
    if (i <= j) {
      t = P[i];  P[i++] = P[j];  P[j--] = t;
    }
  } while (i <= j);
  my_sort (a, j);
  my_sort (i, b);
}

int main (int argc, const char *argv[]) {
  user_header_t *UH;
  int i;
  assert (argc == 2);
  ifd = open (argv[1], O_RDONLY);
  assert (ifd >= 0);
  N = 0;
  do {
    if (cur_offs + sizeof(user_header_t) > load_offs + r) {
      assert (lseek (ifd, cur_offs, SEEK_SET) == cur_offs);
      r = read (ifd, buff, READ_AHEAD);
      assert (r >= 0);
      load_offs = cur_offs;
      // fprintf (stderr, "%d bytes read at position %lld, [0]=%02x\n", r, cur_offs, buff[0]);
      if (!r) break;
      assert (r >= sizeof(user_header_t));
    }
    //fprintf (stderr, "cur_offs = %lld\n", cur_offs);
    UH = (user_header_t *) &buff[cur_offs - load_offs];
    //fprintf (stderr, "magic = %08lx\n", UH->magic);
    assert (UH->magic == MAIL_INDEX_BLOCK_MAGIC);
    //fprintf (stderr, "user %ld, hash_cnt=%ld, list_cnt=%ld, pos=%lld\n", UH->user_id, UH->hash_cnt, UH->list_cnt, cur_offs);
    assert ((unsigned long) UH->user_id < (1L << 31) && (unsigned long) UH->hash_cnt < (1L << 24) && (unsigned long) UH->list_cnt < (1L << 26));
    cur_offs += sizeof (user_header_t);
    if (UH->user_id && (unsigned long) UH->user_id < (1L << 26)) {
      A[N].user_id = UH->user_id;
      A[N].hash_cnt = UH->hash_cnt;
      A[N].list_cnt = UH->list_cnt;
      A[N].file_no = 0;
      A[N].offset = cur_offs;
      P[N] = &A[N];
      N++;
    }
    cur_offs += UH->hash_cnt * 16 + UH->list_cnt * 4;
    assert (N <= MAX_USERS);
  } while (1);
  my_sort (0, N-1);
  for (i = 0; i < N; i++) B[i] = *P[i];
  i = write (1, B, N * sizeof(userlist_entry_t));
  assert (i == N * sizeof(userlist_entry_t));
  return 0;
}
