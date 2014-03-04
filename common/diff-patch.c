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

    Copyright 2011 Vkontakte Ltd
              2011 Anton Maydell
*/

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/resource.h>
#include <zlib.h>

#include "kdb-data-common.h"
#include "listcomp.h"
#include "diff-patch.h"

//#define DIFF_DEBUG

static inline int bsr (int i) {
  int r, t;
  asm("bsr %1,%0\n\t"
    : "=&q" (r), "=&q" (t)
    : "1" (i)
    : "cc");
  return r;
}

typedef struct {
  int offset;
  int length;
} diff_string_t;

typedef struct {
  unsigned char *a;
  diff_string_t *H;
  int *e, *next;
  int size;
} diff_hashtable_t;

/* returns prime number which greater than 1.5n and not greater than 1.1 * 1.5 * n */
static int get_hashtable_size (int n) {
  static const int p[] = {1103,1217,1361,1499,1657,1823,2011,2213,2437,2683,2953,3251,3581,3943,4339,
  4783,5273,5801,6389,7039,7753,8537,9391,10331,11369,12511,13763,15149,16673,18341,20177,22229,
  24469,26921,29629,32603,35869,39461,43411,47777,52561,57829,63617,69991,76991,84691,93169,102497,
  112757,124067,136481,150131,165161,181693,199873,219871,241861,266051,292661,321947,354143, 389561, 428531,
  471389,518533,570389,627433,690187,759223,835207,918733,1010617,1111687,1222889,1345207,
  1479733,1627723,1790501,1969567,2166529,2383219,2621551,2883733,3172123,3489347,3838283,4222117,
  4644329,5108767,5619667,6181639,6799811,7479803,8227787,9050599,9955697,10951273,12046403,13251047,
  14576161,16033799,17637203,19400929,21341053,23475161,25822679,28404989,31245491,34370053,37807061,
  41587807,45746593,50321261,55353391,60888739,66977621,73675391,81042947,89147249,98061979,107868203,
  118655027,130520531,143572609,157929907,173722907,191095213,210204763,231225257,254347801,279782593,
  307760897,338536987,372390691,409629809,450592801,495652109,545217341,599739083,659713007,725684317,
  798252779,878078057,965885863,1062474559};
  const int lp = sizeof (p) / sizeof (p[0]);
  int a = -1;
  int b = lp;
  n += n >> 1;
  while (b - a > 1) {
    int c = ((a + b) >> 1);
    if (p[c] <= n) { a = c; } else { b = c; }
  }
  if (a < 0) { a++; }
  assert (a < lp-1);
  return p[a];
}

static void diff_hashtable_init (diff_hashtable_t *T, unsigned char *a, int N) {
  T->size = get_hashtable_size (N);
  T->a = a;
  unsigned sz = T->size * sizeof (T->H[0]);
  T->H = zmalloc (sz);
  memset (T->H, 0xff, sz);
  sz = 4 * T->size;
  T->e = zmalloc (sz);
  memset (T->e, 0xff, sz);
  sz = 4 * N;
  T->next = zmalloc (sz);
  memset (T->next, 0xff, sz);
}

static int diff_hashtable_get_f (diff_hashtable_t *T, const unsigned char *const a, const diff_string_t *const S, int id) {
  unsigned hc1 = 0, hc2 = 0;
  int i;
  const unsigned char *b = a + S->offset;
  for (i = 0; i < S->length; i++) {
    hc1 = (239 * hc1) + (*b);
    hc2 = (3 * hc2) + (*b);
    b++;
  }
  hc1 %= T->size;
  hc2 = 1 + (hc2 % (T->size - 1));
  b = a + S->offset;
  while (T->H[hc1].length >= 0) {
    if (T->H[hc1].length == S->length && !memcmp (T->a + T->H[hc1].offset, b, S->length)) {
      if (id >= 0) {
        T->next[id] = T->e[hc1];
        T->e[hc1]= id;
      }
      return T->e[hc1];
    }
    if ( (hc1 += hc2) >= T->size) {
      hc1 -= T->size;
    }
  }
  if (id < 0) {
    return -1;
  }
  T->H[hc1].offset = S->offset;
  T->H[hc1].length = S->length;
  T->next[id] = T->e[hc1];
  T->e[hc1]= id;
  return T->e[hc1];
}

/* returns number of lines in buffer a */
static int line_split (unsigned char *a, int n, diff_string_t **R) {
  int i, k = 0;
  for (i = 0; i < n; i++) {
    if (a[i] == '\n') {
      k++;
    }
  }
  diff_string_t *A = zmalloc ((k + 1) * sizeof (diff_string_t));
  int first = 0, l = 0;
  for (i = 0; i < n - 1; i++) {
    if (a[i] == '\n') {
      A[l].offset = first;
      A[l].length = i - first + 1;
      l++;
      first = i + 1;
    }
  }
  A[l].offset = first;
  A[l].length = n - first;
  l++;
  assert (l <= k + 1);
  *R = A;
#ifdef DIFF_DEBUG
  for (k = 0; k < l; k++) {
    assert (!k || A[k-1].offset + A[k-1].length == A[k].offset);
    n -= A[k].length;
  }
  assert (!n);
#endif
  return l;
}

static inline double get_rusage_time (void) {
  struct rusage r;
  if (getrusage (RUSAGE_SELF, &r)) { return 0.0; }
  return r.ru_utime.tv_sec + r.ru_stime.tv_sec + 1e-6 * (r.ru_utime.tv_usec + r.ru_stime.tv_usec);
}

struct diff_node {
  struct diff_node *prev;
  int x;
};

#pragma	pack(push,4)
struct diff_header {
  int compressed_text_size;
  int text_size;
  int ops;
  unsigned char lbuckets;
  unsigned char compress_level;
};
#pragma pack(pop)

static struct diff_node *diff_node_alloc (int x, struct diff_node *prev) {
  struct diff_node *p = dyn_alloc (sizeof (struct diff_node), PTRSIZE);
  if (p == NULL) { return p; }
  p->x = x;
  p->prev = prev;
  return p;
}

static int find_diff (unsigned char *a, int n, unsigned char *b, int m, unsigned char *c, int buff_size, int compress_level, double timeout) {
  struct diff_header header;
  if (buff_size < sizeof (struct diff_header)) { return DIFF_ERR_OUTPUT_BUFFER_OVERFLOW; }
  diff_string_t *A, *B;
  const int N = line_split (a, n, &A), M = line_split (b, m, &B), MAX_LCS_SIZE = (N > M) ? N : M;
  int *x = zmalloc (4 * (MAX_LCS_SIZE + 1));
  dyn_mark_t mrk;
  dyn_mark (mrk);
  int i, j, k, o;
  diff_hashtable_t T;
  diff_hashtable_init (&T, a, N);
  for (i = 0; i < N; i++) {
    diff_hashtable_get_f (&T, a, A + i, i);
  }
  struct diff_node **nodes = zmalloc (sizeof (struct diff_node *) * (MAX_LCS_SIZE + 1));
  nodes[0] = diff_node_alloc (0, NULL);
  if (!nodes[0]) { return DIFF_ERR_MEMORY; }
  k = 1;
  int pairs = 0;
  double start_time = get_rusage_time ();
  for (j = 0; j < M; j++) {
    int r = diff_hashtable_get_f (&T, b, B + j, -1);
    int max_d = k - 1;
    while (r >= 0) {
#ifdef DIFF_DEBUG
      assert (A[r].length == B[j].length && !memcmp (a + A[r].offset, b + B[j].offset, A[r].length));
#endif
      if (!(++pairs & 16383) && get_rusage_time () - start_time > timeout) {
        return DIFF_ERR_TIMEOUT;
      }
      int pos = r + 1, c = 0, d = max_d, e;
      while (d > c) {
        e = (c + d + 1) >> 1;
        if (nodes[e]->x <= pos) { c = e; } else { d = e - 1; }
      }
      if (nodes[c]->x < pos) {
        if (c == k - 1) {
          assert (k < MAX_LCS_SIZE);
          nodes[k] = diff_node_alloc (pos, nodes[c]);
          if (!nodes[k]) { return DIFF_ERR_MEMORY; }
          k++;
        } else {
          nodes[c+1] = diff_node_alloc (pos, nodes[c]);
          if (!nodes[c+1]) { return DIFF_ERR_MEMORY; }
        }
        max_d = c;
      } else {
        max_d = c - 1;
      }
      assert (T.next[r] < r);
      r = T.next[r];
    }
  }
  struct diff_node *q = nodes[k - 1];
  for (o = k - 1; o > 0; o--) {
    x[o] = q->x;
    q = q->prev;
  }
  dyn_release (mrk); /* free hashtable and nodes */
  int *y = zmalloc (4 * k);
  j = 0;
  for (o = 1; o < k; o++) {
    i = x[o] - 1;
    while (j < M && (B[j].length != A[i].length || memcmp (a + A[i].offset, b + B[j].offset, B[j].length))) {
      j++;
    }
    assert (j < M);
    y[o] = j++;
  }
  int max_ops = N + M - (k - 1);
  unsigned *d = zmalloc (4 * max_ops);
  int ops = 0;
  i = 1;
  j = 0;
  int p = sizeof (struct diff_header);
  for (o = 1; o < k; o++) {
    while (i < x[o]) {
      assert (ops < max_ops);
      d[ops++] = 0; /* erase */
      i++;
    }
    while (j < y[o]) {
      int l = B[j].length;
      if (l + p > buff_size) { return DIFF_ERR_OUTPUT_BUFFER_OVERFLOW; }
      memcpy (c + p, b + B[j].offset, l);
      p += l;
      assert (ops < max_ops);
      d[ops++] = 1; /* insert */
      j++;
    }
    assert (ops < max_ops);
    d[ops++] = 2; /* copy */
    i++;
    j++;
  }
  while (i <= N) {
    assert (ops < max_ops);
    d[ops++] = 0;
    i++;
  }
  while (j < M) {
    int l = B[j].length;
    if (l + p > buff_size) { return DIFF_ERR_OUTPUT_BUFFER_OVERFLOW; }
    memcpy (c + p, b + B[j].offset, l);
    p += l;
    assert (ops < max_ops);
    d[ops++] = 1;
    j++;
  }
  assert (ops == max_ops);
  header.text_size = p - sizeof (struct diff_header);
  uLongf dest = compressBound (header.text_size);
  dyn_mark (mrk);
  void *gzipped_text = zmalloc (dest);
  if (!compress_level || compress2 (gzipped_text, &dest, c + sizeof (struct diff_header), header.text_size, compress_level) != Z_OK || dest >= header.text_size) {
    header.compressed_text_size = header.text_size;
    header.compress_level = 0;
  } else {
    header.compress_level = compress_level;
    header.compressed_text_size = dest;
    memcpy (c + sizeof (struct diff_header), gzipped_text, dest);
  }
  dyn_release (mrk);
  p = header.compressed_text_size + sizeof (struct diff_header);

  i = o = 0;
  int max_lgt = 0, lgt;
  while (i < ops) {
    j = i + 1;
    while (j < ops && d[i] == d[j]) { j++; }
    lgt = j - i;
    if (max_lgt < lgt) {
      max_lgt = lgt;
    }
    d[o++] = (d[i] << 30) + lgt;
    i = j;
  }
  header.ops = ops = o;
  const int lbuckets = (bsr (max_lgt) + 1), buckets = 3 * lbuckets;
  header.lbuckets = lbuckets;
  int *freq = zmalloc0 (4 * buckets);
  for (i = 0; i < ops; i++) {
    int tp = d[i] >> 30;
    lgt = d[i] & 0x3fffffff;
    freq[tp * lbuckets + bsr (lgt)]++;
  }
  int alphabet_size, *l = get_huffman_codes_lengths (freq, buckets, HUFFMAN_MAX_CODE_LENGTH, &alphabet_size);
  int bits = 4 * buckets;
  if (alphabet_size >= 2) {
    for (i = 0; i < buckets; i++) {
      bits += freq[i] * (l[i] + (i % lbuckets));
    }
  } else {
    for (i = 0; i < buckets; i++) {
      bits += freq[i] * (i % lbuckets);
    }
  }
  const int bytes = (bits + 7) >> 3;
  if (bytes + p > buff_size) { return DIFF_ERR_OUTPUT_BUFFER_OVERFLOW; }
  struct bitwriter bw;
  bwrite_init (&bw, c + p, c + buff_size, 0);
  bwrite_huffman_codes (&bw, l, buckets);
  const int single_symbol_in_alphabet = alphabet_size < 2;
  int firstcode[HUFFMAN_MAX_CODE_LENGTH+1];
  int *codeword = zmalloc (4 * buckets), *symbols = zmalloc (4 * 32 * (HUFFMAN_MAX_CODE_LENGTH + 1));
  canonical_huffman (l, buckets, HUFFMAN_MAX_CODE_LENGTH, firstcode, codeword, symbols);
  for (i = 0; i < ops; i++) {
    int tp = d[i] >> 30;
    lgt = d[i] & 0x3fffffff;
    k = bsr (lgt);
    o = tp * lbuckets + k;
    assert (o >= 0 && o < buckets);
    if (!single_symbol_in_alphabet) {
      bwrite_nbits (&bw, codeword[o], l[o]);
    }
    bwrite_nbits (&bw, lgt ^ (1 << k), k);
  }
  assert (bits == bwrite_get_bits_written (&bw));
  p += bytes;
  assert (p <= buff_size);
  memcpy (c, &header, sizeof (struct diff_header));
  return p;
}

static int apply_patch (unsigned char *a, int n, unsigned char *b, int m, unsigned char *c, int buff_size) {
  struct diff_header header;
  int i, j, k;
  if (m < sizeof (struct diff_header)) { return PATCH_ERR_HEADER_NOT_FOUND; }
  memcpy (&header, b, sizeof (struct diff_header));
  if (header.compressed_text_size + sizeof (struct diff_header) >= m) {
    return PATCH_ERR_TEXTBUFF_TOO_BIG;
  }
  int ops = header.ops, lbuckets = header.lbuckets, buckets = 3 * lbuckets;
  unsigned char *text;
  if (!header.compress_level) {
    text = b + sizeof (struct diff_header);
  } else {
    text = zmalloc (header.text_size);
    uLongf destLen = header.text_size;
    assert (Z_OK == uncompress (text, &destLen, b + sizeof (struct diff_header), header.compressed_text_size));
    assert (destLen == header.text_size);
  }
  b += sizeof (struct diff_header) + header.compressed_text_size;
  m -= sizeof (struct diff_header) + header.compressed_text_size;
  diff_string_t *A, *B;
  const int N = line_split (a, n, &A), M = line_split (text, header.text_size, &B);
  struct bitreader br;
  bread_init (&br, b, 0);
  int *l = zmalloc (4 * buckets), alphabet_size;
  bread_huffman_codes (&br, l, buckets, &alphabet_size);
  int firstcode[HUFFMAN_MAX_CODE_LENGTH+1];
  int *symbols = NULL, single_bucket = -1;
  if (alphabet_size > 1) {
    symbols = zmalloc (4 * buckets * (HUFFMAN_MAX_CODE_LENGTH + 1));
    canonical_huffman (l, buckets, HUFFMAN_MAX_CODE_LENGTH, firstcode, NULL, symbols);
  } else {
    for (i = 0; i < buckets; i++) {
      if (l[i]) {
        single_bucket = i;
        break;
      }
    }
  }
  i = j = k = 0;
  while (--ops >= 0) {
    const int code = single_bucket >= 0 ? single_bucket : bread_huffman_decode_int (&br, firstcode, symbols);
    assert (code >= 0 && code < buckets);
    const int tp = code / lbuckets, lgt = code - tp * lbuckets;
    int l = (1 << lgt) + bread_nbits (&br, lgt);
    switch (tp) {
      case 0: /* erase */
        i += l;
        if (i > N) { return PATCH_ERR_OLDPTR_OUT_OF_RANGE; }
        break;
      case 1: /* insert */
        while (--l >= 0) {
          if (j >= M) { return PATCH_ERR_PATCHPTR_OUT_OF_RANGE; }
          if (k + B[j].length > buff_size) { return PATCH_ERR_OUTPUT_BUFFER_OVERFLOW; }
          memcpy (c + k, text + B[j].offset, B[j].length);
          k += B[j].length;
          j++;
        }
        break;
      case 2: /* copy */
        while (--l >= 0) {
          if (i >= N) { return PATCH_ERR_OLDPTR_OUT_OF_RANGE; }
          if (k + A[i].length > buff_size) { return PATCH_ERR_OUTPUT_BUFFER_OVERFLOW; }
          memcpy (c + k, a + A[i].offset, A[i].length);
          k += A[i].length;
          i++;
        }
        break;
      default:
        assert (0);
    }
  }
  return k;
}

int vk_diff (unsigned char *old_buff, int old_buff_size, unsigned char *new_buff, int new_buff_size, unsigned char *patch_buff, int patch_buff_size, int compress_level, double timeout) {
  dyn_mark_t mrk;
  dyn_mark (mrk);
  int r = find_diff (old_buff, old_buff_size, new_buff, new_buff_size, patch_buff, patch_buff_size, compress_level, timeout);
  dyn_release (mrk);
  return r;
}

int vk_patch (unsigned char *old_buff, int old_buff_size, unsigned char *patch_buff, int patch_buff_size, unsigned char *new_buff, int new_buff_size) {
  dyn_mark_t mrk;
  dyn_mark (mrk);
  int r = apply_patch (old_buff, old_buff_size, patch_buff, patch_buff_size, new_buff, new_buff_size);
  dyn_release (mrk);
  return r;
}
