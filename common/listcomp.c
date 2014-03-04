/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Anton Maydell
*/

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "kdb-data-common.h"
#include "listcomp.h"

/* hiword: major version, loword: minor version */
/* major version: different compression format */
/* minor version: different compression algorithm implementation (optimization, etc.) */
const int listcomp_version = 0x0000000c;

#define unlikely(x) __builtin_expect((x),0)
#define	decode_cur_bit (m < 0)
#define	decode_load_bit()	{ m <<= 1; if (unlikely(m == (-1 << 31))) { m = ((int) *br->ptr++ << 24) + (1 << 23); } }

static inline int bsr (int i) {
  int r, t;
  asm("bsr %1,%0\n\t"
    : "=&q" (r), "=&q" (t)
    : "1" (i)
    : "cc");
  return r;
}

/*********************** bitwriter *****************************/
void bwrite_init (struct bitwriter *bw, unsigned char *start_ptr, unsigned char *end_ptr, unsigned int prefix_bit_offset) {
  bw->prefix_bit_offset = prefix_bit_offset;
  bw->start_ptr = start_ptr;
  bw->end_ptr = end_ptr;
  bw->ptr = start_ptr + (prefix_bit_offset >> 3);
  assert (bw->ptr < bw->end_ptr);
  prefix_bit_offset &= 7;
  bw->m = 0x80 >> prefix_bit_offset;
  *bw->ptr &= 0xffffff00U >> prefix_bit_offset;
}

static inline void bwrite_append (struct bitwriter *bw, unsigned char value) {
  bw->ptr++;
  assert (bw->ptr < bw->end_ptr);
  *bw->ptr = value;
}

void bwrite_nbits (struct bitwriter *bw, unsigned int d, int n) {
  assert (!(n & -32));
  if (!n) { return; }
  unsigned int i;
  for (i = 1U << (n-1); i != 0; i >>= 1) {
    if (!bw->m) {
      bwrite_append (bw, 0);
      bw->m = 0x80;
    }
    if (d & i) {
      *bw->ptr += bw->m;
    }
    bw->m >>= 1;
  }
}

void bwrite_nbitsull (struct bitwriter *bw, unsigned long long d, int n) {
  assert (!(n & -64));
  if (!n) { return; }
  unsigned long long i;
  for (i = 1ULL << (n-1); i != 0; i >>= 1) {
    if (!bw->m) {
      bwrite_append (bw, 0);
      bw->m = 0x80;
    }
    if (d & i) {
      *bw->ptr += bw->m;
    }
    bw->m >>= 1;
  }
}

void bwrite_interpolative_encode_value (struct bitwriter *bw, int a, int r) {
  int b[32], k = 0, x = r >> 1;
  if (a >= x) {
    a = (a - x) << 1;
  } else {
    a = ((x - a) << 1) - 1;
  }
  a += r;
  while (a > 1) {
    b[k++] = a & 1;
    a >>= 1;
  }
  for (k--; k >= 0; k--) {
    if (!bw->m) {
      bwrite_append (bw, 0);
      bw->m = 0x80;
    }
    if (b[k]) {
      *bw->ptr += bw->m;
    }
    bw->m >>= 1;
  }
}

void bwrite_interpolative_sublist (struct bitwriter *bw, int *L, int u, int v) {
  if (v - u <= 1) { return; }
  const int  m = (u + v) >> 1,
            hi = L[v] - (v - m),
            lo = L[u] + (m - u),
             a = L[m] - lo,
             r = hi - lo + 1;
  bwrite_interpolative_encode_value (bw, a, r);
  bwrite_interpolative_sublist (bw, L, u, m);
  bwrite_interpolative_sublist (bw, L, m, v);
}


static void bwrite_interpolative_ext_sublist_first_pass (struct bitwriter *bw, int *L, int u, int v, int left_subtree_size_threshold, struct left_subtree_bits_array *p) {
  const int sz = v - u;
  if (sz <= 1) { return; }
  const int  m = (u + v) >> 1,
            hi = L[v] - (v - m),
            lo = L[u] + (m - u),
             a = L[m] - lo,
             r = hi - lo + 1;
  bwrite_interpolative_encode_value (bw, a, r);
  if (sz >= left_subtree_size_threshold) {
    assert (p->idx < p->n);
    int *q = &p->a[p->idx];
    p->idx++;
    int tree_bits = -bwrite_get_bits_written (bw);
    bwrite_interpolative_ext_sublist_first_pass (bw, L, u, m, left_subtree_size_threshold, p);
    tree_bits += bwrite_get_bits_written (bw);
    *q = tree_bits;
    bwrite_gamma_code (bw, tree_bits + 1);
  } else {
    bwrite_interpolative_ext_sublist_first_pass (bw, L, u, m, left_subtree_size_threshold, p);
  }
  bwrite_interpolative_ext_sublist_first_pass (bw, L, m, v, left_subtree_size_threshold, p);
}

static void bwrite_interpolative_ext_sublist_second_pass (struct bitwriter *bw, int *L, int u, int v, int left_subtree_size_threshold, struct left_subtree_bits_array *p, int *redundant_bits) {
  const int sz = v - u;
  if (sz <= 1) { return; }
  const int  m = (u + v) >> 1,
            hi = L[v] - (v - m),
            lo = L[u] + (m - u),
             a = L[m] - lo,
             r = hi - lo + 1;
  bwrite_interpolative_encode_value (bw, a, r);
  if (sz >= left_subtree_size_threshold) {
    assert (p->idx < p->n);
    int lsb = p->a[p->idx];
    p->idx++;
    if (redundant_bits != NULL) {
      (*redundant_bits) += get_gamma_code_length (lsb + 1);
    }
    bwrite_gamma_code (bw, lsb + 1);
    int tree_bits = -bwrite_get_bits_written (bw);
    bwrite_interpolative_ext_sublist_second_pass (bw, L, u, m, left_subtree_size_threshold, p, redundant_bits);
    tree_bits += bwrite_get_bits_written (bw);
    assert (lsb == tree_bits);
  } else {
    bwrite_interpolative_ext_sublist_second_pass (bw, L, u, m, left_subtree_size_threshold, p, redundant_bits);
  }
  bwrite_interpolative_ext_sublist_second_pass (bw, L, m, v, left_subtree_size_threshold, p, redundant_bits);
}

int get_gamma_code_length (unsigned int d) {
  assert (d > 0);
  int k = bsr (d);
  return (2 * k + 1);
}

void bwrite_gamma_code (struct bitwriter *bw, unsigned int d) {
  assert (d > 0);
  int i, k = bsr (d);
  d ^= 1U << k;
  for (i = 0; i < k; i++) {
    if (!bw->m) {
      bwrite_append (bw, 0);
      bw->m = 0x80;
    }
    *bw->ptr += bw->m;
    bw->m >>= 1;
  }
  if (!bw->m) {
    bwrite_append (bw, 0);
    bw->m = 0x80;
  }
  bw->m >>= 1;
  bwrite_nbits (bw, d, k);
}

int get_subtree_array_size (int u, int v, int left_subtree_size_threshold) {
  if (v - u < left_subtree_size_threshold) {
    return 0;
  }
  int m = (u + v) >> 1;
  return 1 + get_subtree_array_size (u, m, left_subtree_size_threshold) + get_subtree_array_size (m, v, left_subtree_size_threshold);
}

void bwrite_interpolative_ext_sublist (struct bitwriter *bw, int *L, int u, int v, int left_subtree_size_threshold, int *redundant_bits) {
  struct bitwriter tmp;
  memcpy (&tmp, bw, sizeof (struct bitwriter));
  unsigned char c = *(bw->ptr);
  struct left_subtree_bits_array p;
  p.n = get_subtree_array_size (u, v, left_subtree_size_threshold);
  dyn_mark_t mrk;
  dyn_mark (mrk);
  p.a = zmalloc (p.n * sizeof (int));
  p.idx = 0;
  bwrite_interpolative_ext_sublist_first_pass (bw, L, u, v, left_subtree_size_threshold, &p);
  memcpy (bw, &tmp, sizeof (struct bitwriter));
  *(bw->ptr) = c;
  p.idx = 0;
  if (redundant_bits != NULL) {
    *redundant_bits = 0;
  }
  bwrite_interpolative_ext_sublist_second_pass (bw, L, u, v, left_subtree_size_threshold, &p, redundant_bits);
  dyn_release (mrk);
}

unsigned int bwrite_get_bits_written (const struct bitwriter *bw) {
  return ((bw->ptr - bw->start_ptr) << 3) + (8 - ffs (bw->m)) - bw->prefix_bit_offset;
}

/*********************** bitreader *****************************/
void bread_init (struct bitreader *br, const unsigned char *start_ptr, int prefix_bit_offset) {
  br->prefix_bit_offset = prefix_bit_offset;
  br->start_ptr = start_ptr;
  br->ptr = start_ptr + (prefix_bit_offset >> 3);
  br->m = (((int) *br->ptr++ << 24) + (1 << 23)) << (prefix_bit_offset & 7);
}

inline void bread_seek (struct bitreader *br, unsigned int bit_offset) {
  br->ptr = br->start_ptr + (bit_offset >> 3);
  br->m = (((int) *br->ptr++ << 24) + (1 << 23)) << (bit_offset & 7);
}

unsigned int bread_nbits (struct bitreader *br, int n) {
  assert (!(n & -32));
  unsigned int d = 0;
  int m = br->m;
  while (n--) {
    d <<= 1;
    if (decode_cur_bit) {
      d++;
    }
    decode_load_bit();
  }
  br->m = m;
  return d;
}

unsigned long long bread_nbitsull (struct bitreader *br, int n) {
  assert (!(n & -64));
  unsigned long long d = 0;
  int m = br->m;
  while (n--) {
    d <<= 1;
    if (decode_cur_bit) {
      d++;
    }
    decode_load_bit();
  }
  br->m = m;
  return d;
}

unsigned int bread_gamma_code (struct bitreader *br) {
  int k = 0, m = br->m;
  while (decode_cur_bit) {
    k++;
    decode_load_bit();
  }
  decode_load_bit();
  br->m = m;
  return (1U << k) | bread_nbits (br, k);
}



const char* list_get_compression_method_description (int compression_method) {
  switch (compression_method) {
  case le_degenerate:
    return "N == K";
  case le_golomb:
    return "Golomb";
  case le_interpolative:
    return "Interpolative";
  case le_interpolative_ext:
    return "Interpolative_ext";
  case le_llrun:
    return "LLRUN";
  default:
    return "Unknown";
  }
}

static void store_int (struct list_encoder *enc, int d) {
  /* just store in internal array for further compression */
  enc->L[enc->k++] = d;
}

/******************** Trivial case N == K (no coding needed) ********************/

static void degenerate_encode_int (struct list_encoder *enc, int d) {
  assert (d == enc->k);
  enc->k++;
}

static int degenerate_decode_int (struct list_decoder *dec) {
  if (dec->k >= dec->K) {  /* by K.O.T. */
    return 0x7fffffff;
  }
  return dec->k++;
}
/******************** Raw int32 array ********************/
static int raw_int32_decode_int (struct list_decoder *dec) {
  int r;
  memcpy (&r, dec->br.ptr, sizeof (int));
  dec->br.ptr += 4;
  dec->k++;
  return r;
}
static void raw_int32_decoder_init (struct list_decoder *dec) {
  assert (!(dec->br.prefix_bit_offset & 7));
  dec->k = 0;
  dec->br.ptr--;
  dec->decode_int = &raw_int32_decode_int;
}
/******************** LLRUN ********************/

static void llrun_encoder_init (struct list_encoder *enc) {
  enc->L = malloc (enc->K * sizeof (enc->L[0]));
  enc->k = 0;
  enc->encode_int = &store_int;
}

/* coin collection problem list entry */
struct ccp_list_entry {
  long long freq;
  struct ccp_list_entry *next, *left, *right;
  int leaf_value;
};

static struct ccp_list_entry *new_ccp_list_entry (long long freq) {
  struct ccp_list_entry *x = zmalloc (sizeof (struct ccp_list_entry));
  x->freq = freq;
  x->left = x->right = x->next = NULL;
  return x;
}

/* http://en.wikipedia.org/wiki/Package-merge_algorithm */
static struct ccp_list_entry *package_merge (struct ccp_list_entry *x, struct ccp_list_entry *y) {
  struct ccp_list_entry *head = NULL, *tail = NULL;
  while (x != NULL) {
    if (x->next == NULL) {
      break;
    }
    struct ccp_list_entry *w = x->next, *p = new_ccp_list_entry (x->freq + w->freq);
    p->leaf_value = -1;
    p->left = x;
    p->right = w;
    x = w->next;

    if (head == NULL) {
      head = tail = p;
    } else {
      tail = tail->next = p;
    }
  }
  x = head;
  head = tail = NULL;
  while (x != NULL && y != NULL) {
    if (x->freq <= y->freq) {
      if (head == NULL) {
        head = tail = x;
      } else {
        tail = tail->next = x;
      }
      x = x->next;
    } else {
      if (head == NULL) {
        head = tail = y;
      } else {
        tail = tail->next = y;
      }
      y = y->next;
    }
  }
  if (y != NULL) {
    x = y;
  }
  if (head == NULL) {
    return x;
  }
  tail->next = x;
  return head;
}


static struct ccp_list_entry *zmalloc_ccp_list (struct ccp_list_entry *a, int M) {
  int i;
  struct ccp_list_entry *head = NULL, *tail = NULL;
  for (i = 0; i < M; i++) {
    struct ccp_list_entry *p = new_ccp_list_entry (a[i].freq);
    p->leaf_value = a[i].leaf_value;
    if (head == NULL) {
      head = tail = p;
    } else {
      tail = tail->next = p;
    }
  }
  return head;
}

static int llhuf_check_codes_lengths (int *l, int N) {
  int i, nz = 0;
  unsigned long long u = 0;
  for (i = 0; i < N; i++) {
    if (l[i]) {
      if (l[i] < 0 || l[i] >= 32) {
        return -1;
      }
      u += 1U << (32 - l[i]);
    } else {
      nz++;
    }
  }

  if (nz == N - 1) {
    return 0;
  }

  if (u > 0x100000000ULL) {
    return -2;
  }
  return 0;
}

static int cmp_ccp_list_entries (const void *a, const void *b) {
  const struct ccp_list_entry *A = (const struct ccp_list_entry *) a;
  const struct ccp_list_entry *B = (const struct ccp_list_entry *) b;
  if (A->freq < B->freq) { return -1; }
  if (A->freq > B->freq) { return 1; }
  if (A->leaf_value < B->leaf_value) { return -1; }
  if (A->leaf_value > B->leaf_value) { return 1; }
  return 0;
}

static void llhuf_incr_code_lengths (int *l, struct ccp_list_entry *x) {
  if (x->leaf_value >= 0) {
    l[x->leaf_value]++;
  } else {
    llhuf_incr_code_lengths (l, x->left);
    llhuf_incr_code_lengths (l, x->right);
  }
}

/* llhuf - means limited length huffman */
int* get_huffman_codes_lengths (int *freq, int N, int L, int *alphabet_size) {
  int k;
  assert (L >= 2);
  int *l = zmalloc0 (N * sizeof (int));
  dyn_mark_t E_mark;
  dyn_mark (E_mark);
  struct ccp_list_entry *E = zmalloc (N * sizeof (struct ccp_list_entry));
  int M = 0;
  for (k = 0; k < N; k++) {
    if (freq[k]) {
      E[M].freq = freq[k];
      E[M].leaf_value = k;
      M++;
    }
  }
  *alphabet_size = M;
  assert (M > 0);
  qsort (E, M, sizeof (E[0]), cmp_ccp_list_entries);
  if (M == 1) {
    /* only one symbol in alphabet */
    l[E[0].leaf_value] = L; /* mark it */
    dyn_release (E_mark);
    return l;
  }
  struct ccp_list_entry *x = zmalloc_ccp_list (E, M);
  for (k = L-1; k >= 1; k--) {
    x = package_merge (x, zmalloc_ccp_list (E, M));
  }
  x = package_merge (x, NULL);
  for (k = 1; x != NULL; k++) {
    if (k < M) {
      llhuf_incr_code_lengths (l, x);
    }
    x = x->next;
  }
  dyn_release (E_mark);
  return l;
}

static inline int get_max_possible_gap (int N, int K) {
  return N - K + 1;
}

static inline int llrun_get_buckets_quantity (int max_gap) {
  return bsr (max_gap) + 1;
}

void canonical_huffman (int *l, int N, int L, int* firstcode, int *codeword, int *symbols) {
  int i;
  int numl[HUFFMAN_MAX_CODE_LENGTH+1], nextcode[HUFFMAN_MAX_CODE_LENGTH+1];
  memset (&numl[1], 0, L * sizeof(int));
  for (i = 0; i < N; i++) {
    numl[l[i]]++;
  }
  firstcode[L] = 0;
  for (i = L - 1; i >= 1; i--) {
    firstcode[i] = (firstcode[i+1] + numl[i+1]) >> 1;
  }
  memcpy (&nextcode[1], &firstcode[1], L * sizeof (int));
  /* codeword array only needed for encoding */
  if (unlikely (codeword != NULL)) {
    for (i = 0; i < N; i++) {
      int li = l[i];
      if (!li) {
        continue;
      }
      codeword[i] = nextcode[li];
      symbols[(HUFFMAN_MAX_CODE_LENGTH+1) * ((nextcode[li]++) - firstcode[li]) + li] = i;
    }
  } else {
    for (i = 0; i < N; i++) {
      int li = l[i];
      if (!li) {
        continue;
      }
      symbols[(HUFFMAN_MAX_CODE_LENGTH+1) * ((nextcode[li]++) - firstcode[li]) + li] = i;
    }
  }
}

inline void list_encode_nbits (struct list_encoder *enc, int d, int n) {
  bwrite_nbits (&enc->bw, d, n);
}

/* encode "canonical huffman tree" (store only code lengths) */
void bwrite_huffman_codes (struct bitwriter *bw, int *l, int N) {
  int i;
  for (i = 0; i < N; i++) {
    assert (l[i] >= 0 && l[i] <= HUFFMAN_MAX_CODE_LENGTH);
    bwrite_nbits (bw, l[i], 4);
  }
}

static void llrun_encoder_finish (struct list_encoder *enc) {
  dyn_mark_t mrk;
  dyn_mark (mrk);
  assert (enc->k == enc->K);
  int max_gap = get_max_possible_gap (enc->N, enc->K);
  int nbuckets = llrun_get_buckets_quantity (max_gap);
  int *freq = zmalloc0 (nbuckets * sizeof (int));
  int last = -1, i;
  for (i = 0; i < enc->K; i++) {
    int d = enc->L[i] - last;
    assert (d > 0 && d <= max_gap);
    freq[bsr (d)]++;
    last = enc->L[i];
  }
  int alphabet_size, *l = get_huffman_codes_lengths (freq, nbuckets, HUFFMAN_MAX_CODE_LENGTH, &alphabet_size);
  assert (llhuf_check_codes_lengths (l, nbuckets) == 0);
  bwrite_huffman_codes (&enc->bw, l, nbuckets);
  const int single_symbol_in_alphabet = alphabet_size < 2;
  int firstcode[HUFFMAN_MAX_CODE_LENGTH+1], codeword[32], symbols[32 * (HUFFMAN_MAX_CODE_LENGTH + 1)];
  canonical_huffman (l, nbuckets, HUFFMAN_MAX_CODE_LENGTH, firstcode, codeword, symbols);
  last = -1;
  for (i = 0; i < enc->K; i++) {
    int d = enc->L[i] - last;
    assert (d > 0 && d <= max_gap);
    int o = bsr (d);
    assert (o >= 0 && o < nbuckets);
    if (!single_symbol_in_alphabet) {
      list_encode_nbits (enc, codeword[o], l[o]);
    }
    bwrite_nbits (&enc->bw, d ^ (1 << o), o);
    last = enc->L[i];
  }

  free (enc->L);
  dyn_release (mrk);
}

static int llrun_decode_int_single_bucket (struct list_decoder *dec) {
  const int o = dec->M;
  return dec->last += bread_nbits (&dec->br, o) + (1 << o);
}

int bread_huffman_decode_int (struct bitreader *br, int *firstcode ,int *symbols) {
  int m = br->m, l, v = 0;
  if (decode_cur_bit) {
    v++;
  }
  decode_load_bit();
  l = 1;
  while (v < firstcode[l]) {
    v <<= 1;
    if (decode_cur_bit) {
      v++;
    }
    decode_load_bit();
    l++;
  }
  br->m = m;
  return symbols[(HUFFMAN_MAX_CODE_LENGTH + 1) * (v - firstcode[l]) + l];
}

static int llrun_decode_int (struct list_decoder *dec) {
  struct bitreader *br = &dec->br;
  int *data = dec->data, o = bread_huffman_decode_int (br, data, data + (HUFFMAN_MAX_CODE_LENGTH + 1));
  return dec->last += bread_nbits (br, o) + (1 << o);
}

void bread_huffman_codes (struct bitreader *br, int *l, int N, int *alphabet_size) {
  int i, m = br->m;
  *alphabet_size = 0;
  for (i = 0; i < N; i++) {
    int d = 0;
    if (decode_cur_bit) { d++; }
    decode_load_bit();
    d <<= 1;
    if (decode_cur_bit) { d++; }
    decode_load_bit();
    d <<= 1;
    if (decode_cur_bit) { d++; }
    decode_load_bit();
    d <<= 1;
    if (decode_cur_bit) { d++; }
    decode_load_bit();
    if (d) {
      (*alphabet_size)++;
    }
    l[i] = d;
  }
  br->m = m;
}

static void llrun_decoder_init (struct list_decoder *dec) {
  struct bitreader *br = &dec->br;
  int l[32], i, alphabet_size;
  bread_huffman_codes (br, l, dec->p, &alphabet_size);
  if (alphabet_size <= 1) {
    /* only one symbol in alphabet */
    for (i = 0; i < dec->p; i++) {
      if (l[i]) {
        dec->M = i;
        break;
      }
    }
    dec->decode_int = &llrun_decode_int_single_bucket;
  } else {
    dec->M = -1;
    canonical_huffman (l, dec->p, HUFFMAN_MAX_CODE_LENGTH, dec->data, NULL, dec->data + (HUFFMAN_MAX_CODE_LENGTH + 1));
    dec->decode_int = &llrun_decode_int;
  }
  dec->last = -1;
}

/******************** Golomb codes ********************/

static int golomb_crit[9] = {
  27639569, 43069824, 58383690, 73660334, 88920289, 
  104171314, 119417000, 134659239, 149899122 
};
// log(2) = 5278688/7615537
// golomb_crit[k] = 2*5278688/(1-solve(X=0,1,X^(k+1)*(X+1)-1))

static int compute_golomb_parameter (int N, int K) {
  assert (K > 0 && K <= N);
  long long t = (long long) 2*5278688*N / K;
  if (t <= 165137325) {
    int i = 0;
    do {
      if ((int) t <= golomb_crit[i]) {
        return i + 1;
      }
    } while (++i <= 8);
    return 10;
  }
  return (int) ((t + (7615537-5278688)) / (2*7615537));
}

static void golomb_encode_int (struct list_encoder *enc, int d) {
  struct bitwriter *bw = &enc->bw;
  int td = d;
  d -= enc->last;
  enc->last = td;
  assert (d > 0);
  d--;
// d = qM + r
// output: q ones, 1 zero
// if r < p:=2^k-M, output: r using k-1 digits
// if r >= 2^k-M, output: r + 2^k - M using k digits
  while (d >= enc->M) {
    if (!bw->m) {
      bwrite_append (bw, 0x80);
      bw->m = 0x40;
    } else {
      *bw->ptr += bw->m;
      bw->m >>= 1;
    }
    d -= enc->M;
  }
  if (!bw->m) {
    bwrite_append (bw, 0);
    bw->m = 0x40;
  } else {
    bw->m >>= 1;
  }
  if (d < enc->p) {
    d = ((4*d + 2) << enc->k);
  } else {
    d = ((2*(d + enc->p) + 1) << enc->k);
  }
  while (d != (-1 << 31)) {
    if (!bw->m) {
      bwrite_append (bw, 0);
      bw->m = 0x80;
    }
    if (d < 0) { *bw->ptr += bw->m; }
    d <<= 1;
    bw->m >>= 1;
  }
}

static void golomb_encoder_init (struct list_encoder *enc) {
  int M = compute_golomb_parameter (enc->N, enc->K);
  enc->k = 31;
  enc->p = 1;
  while (enc->p <= M) {
    enc->p *= 2;
    enc->k--;
  }
  enc->p -= M;
  enc->M = M;
  enc->last = -1;
  enc->encode_int = &golomb_encode_int;
}

static int golomb_decode_int_small_k (struct list_decoder *dec) {
  struct bitreader *br = &dec->br;
  register int m = br->m;
  int a = dec->last;
  while (decode_cur_bit) {
    a += dec->M;
    decode_load_bit();
  }
  decode_load_bit();
  br->m = m;
  return dec->last = a + 1;
}

static int golomb_decode_int_big_k (struct list_decoder *dec) {
  struct bitreader *br = &dec->br;
  register int m = br->m;
  int d = 0, a = dec->last;
  while (decode_cur_bit) {
    a += dec->M;
    decode_load_bit();
  }
  decode_load_bit();
  int i = dec->k;
  do {
    d <<= 1;
    if (decode_cur_bit) {
      d++;
    }
    decode_load_bit();
  } while (--i > 1);
  if (d >= dec->p) {
    d <<= 1;
    if (decode_cur_bit) {
      d++;
    }
    decode_load_bit();
    d -= dec->p;
  }
  br->m = m;
  return dec->last = a + d + 1;
}

static void golomb_decoder_init (struct list_decoder *dec) {
  const int M = compute_golomb_parameter (dec->N, dec->K);
  dec->M = M;
  dec->p = (1 << (dec->k = bsr (M) + 1)) - M;
  dec->last = -1;
  dec->decode_int = dec->k > 1 ? &golomb_decode_int_big_k : &golomb_decode_int_small_k;
}

/******************** Interpolative codes ********************/

struct interpolative_decoder_stack_entry {
  int left_idx;
  int left_value;
  int middle_value;
  int right_idx;
  int right_value;
};

static void interpolative_encoder_init (struct list_encoder *enc) {
  enc->L = malloc ( (enc->K + 2) * sizeof (enc->L[0]));
  enc->L[0] = -1;
  enc->L[enc->K+1] = enc->N;
  enc->k = 1;
  enc->encode_int = &store_int;
}

#define INTERPOLATIVE_DECODER_NOT_EVALUATED (-2)
static int interpolative_decode_int (struct list_decoder *dec) {
  dec->k++;
  if (dec->k > dec->K) {  /* by K.O.T. */
    return 0x7fffffff;
  }
  struct interpolative_decoder_stack_entry *data = (struct interpolative_decoder_stack_entry *) dec->data + dec->p;
  for (;;) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (data->middle_value == INTERPOLATIVE_DECODER_NOT_EVALUATED) {
      const int hi = data->right_value - (data->right_idx - middle);
      int lo = data->left_value + (middle - data->left_idx), r = hi - lo;
      if (r) {
        r++;
        struct bitreader *br = &dec->br;
        int m = br->m;
        int i = 1;
        while (i < r) {
          i <<= 1;
          if (decode_cur_bit) {
            i++;
          }
          decode_load_bit();
        }
        br->m = m;
        i -= r;
        if (i & 1) {
          lo += (r >> 1) - (i >> 1) - 1;
        } else {
          lo += (r >> 1) + (i >> 1);
        }
      }
      data->middle_value = lo;
    }
    for (;;) {
      if (dec->k == middle) {
        return data->middle_value;
      }
      if (dec->k < data->right_idx) { break; }
      dec->p--;
      data--;
      middle = (data->left_idx + data->right_idx) >> 1;
    }
    dec->p++;
    struct interpolative_decoder_stack_entry *next = data + 1;
    if (dec->k < middle) {
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->middle_value = INTERPOLATIVE_DECODER_NOT_EVALUATED;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      data = next;
    } else {
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->middle_value = INTERPOLATIVE_DECODER_NOT_EVALUATED;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      data = next;
    }
  }
}

static void interpolative_decoder_init (struct list_decoder *dec) {
  dec->p = 0;
  struct interpolative_decoder_stack_entry *data = (struct interpolative_decoder_stack_entry *) dec->data;
  data->left_idx = 0;
  data->left_value = -1;
  data->middle_value = INTERPOLATIVE_DECODER_NOT_EVALUATED;
  data->right_idx = dec->K + 1;
  data->right_value = dec->N;
  dec->k = 0;
  dec->decode_int = &interpolative_decode_int;
}

static void interpolative_encode_sublist (struct list_encoder *enc, int u, int v) {
  bwrite_interpolative_sublist (&enc->bw, enc->L, u, v);
}

static void interpolative_encoder_finish (struct list_encoder *enc) {
  assert (enc->k == enc->K + 1);
  interpolative_encode_sublist (enc, 0, enc->K + 1);
  free (enc->L);
}

/******************** Redundant interpolative code ********************/
void interpolative_ext_decode_node (struct list_decoder *dec, struct interpolative_ext_decoder_stack_entry *data) {
  int middle = (data->left_idx + data->right_idx) >> 1;
  const int hi = data->right_value - (data->right_idx - middle);
  int lo = data->left_value + (middle - data->left_idx), r = hi - lo;
  if (r) {
    r++;
    struct bitreader *br = &dec->br;
    int m = br->m;
    int i = 1;
    while (i < r) {
      i <<= 1;
      if (decode_cur_bit) {
        i++;
      }
      decode_load_bit();
    }
    br->m = m;
    i -= r;
    if (i & 1) {
      lo += (r >> 1) - (i >> 1) - 1;
    } else {
      lo += (r >> 1) + (i >> 1);
    }
  }
  data->middle_value = lo;
  if (data->right_idx - data->left_idx >= dec->M) {
    data->right_subtree_offset = bread_gamma_code (&dec->br) - 1;
    data->right_subtree_offset += bread_get_bitoffset (&dec->br);
  } else {
    data->right_subtree_offset = -1;
  }
}

static int interpolative_ext_decode_int (struct list_decoder *dec) {
  if (dec->k >= dec->K) {  /* by K.O.T. */
    return 0x7fffffff;
  }
  dec->k++;
  struct interpolative_ext_decoder_stack_entry *data = (struct interpolative_ext_decoder_stack_entry *) dec->data + dec->p;
  for (;;) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    for (;;) {
      if (dec->k == middle) {
        return data->middle_value;
      }
      if (dec->k < data->right_idx) { break; }
      dec->p--;
      data--;
      middle = (data->left_idx + data->right_idx) >> 1;
    }
    dec->p++;
    struct interpolative_ext_decoder_stack_entry *next = data + 1;
    if (dec->k < middle) {
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      interpolative_ext_decode_node (dec, next);
      data = next;
    } else {
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      interpolative_ext_decode_node (dec, next);
      data = next;
    }
  }
}

int list_interpolative_ext_forward_decode_idx (struct list_decoder *dec, int doc_id_lowerbound) {
  if (doc_id_lowerbound >= dec->N) {
    return 0x7fffffff;
  }
  struct interpolative_ext_decoder_stack_entry *data = (struct interpolative_ext_decoder_stack_entry *) dec->data;

  int p = dec->p;
  data += dec->p;
  while (data->right_value <= doc_id_lowerbound) {
    data--;
    p--;
  }

  //fprintf (stderr, "p = %d, dec->p = %d\n", p, dec->p);
  if (p < dec->p) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (data->right_subtree_offset < 0) {
      while (dec->k < middle) {
        interpolative_ext_decode_int (dec);
      }
    } else {
      bread_seek (&dec->br, data->right_subtree_offset);
      dec->k = middle;
    }
    dec->p = p;
  }

  for ( ; ; dec->p++, data++) {
    const int middle = (data->left_idx + data->right_idx) >> 1;
    //fprintf (stderr, "(x[%d] = %d, x[%d] = %d, x[%d] = %d\n", data->left_idx, data->left_value, middle, data->middle_value,  data->right_idx, data->right_value);
    //fprintf (stderr, "dec->k = %d, dec->p = %d\n", dec->k, dec->p);
    if (data->middle_value == doc_id_lowerbound) {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          interpolative_ext_decode_int (dec);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      return data->middle_value;
    }
    const int l = data->right_idx - data->left_idx;
    assert (l >= 2);
    if (l == 2) {
      assert (data->right_value >= doc_id_lowerbound);
      if (data->middle_value < doc_id_lowerbound) {
        if (data->right_idx == dec->K + 1) {
          return 0x7fffffff;
        }
        dec->k = data->right_idx;
        return data->right_value;
      }
      if (data->left_value < doc_id_lowerbound) {
        dec->k = middle;
        return data->middle_value;
      }
      assert (data->left_value >= doc_id_lowerbound);
      dec->k = data->left_idx;
      return data->left_value;
    }
    struct interpolative_ext_decoder_stack_entry *next = data + 1;
    if (data->middle_value > doc_id_lowerbound) {
      //fprintf (stderr, "left\n");
      // left subtree
      if (data->left_idx == middle - 1) {
        dec->k = middle;
        return data->middle_value;
      }
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      interpolative_ext_decode_node (dec, next);
    } else {
      //fprintf (stderr, "right\n");
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          interpolative_ext_decode_int (dec);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      interpolative_ext_decode_node (dec, next);
    }
  }
  return -1;
}

/*
int list_interpolative_ext_forward_decode_idx (struct list_decoder *dec, int doc_id_lowerbound) {
  if (doc_id_lowerbound >= dec->N) {
    return 0x7fffffff;
  }
  struct interpolative_ext_decoder_stack_entry *data = (struct interpolative_ext_decoder_stack_entry *) dec->data;
  int k;
  for (k = 0; k <= dec->p; k++, data++) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (data->middle_value == doc_id_lowerbound) {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          interpolative_ext_decode_int (dec);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      dec->p = k;
      return data->middle_value;
    }
    const int l = data->right_idx - data->left_idx;
    assert (l >= 2);
    if (l == 2) {
      dec->p = k;
      assert (data->right_value >= doc_id_lowerbound);
      if (data->middle_value < doc_id_lowerbound) {
        if (data->right_idx == dec->K + 1) {
          return 0x7fffffff;
        }
        dec->k = data->right_idx;
        return data->right_value;
      }
      if (data->left_value < doc_id_lowerbound) {
        dec->k = middle;
        return data->middle_value;
      }
      assert (data->left_value >= doc_id_lowerbound);
      dec->k = data->left_idx;
      return data->left_value;
    }
    int desired_move = 0, old_tree_move = 0;
    if (data->middle_value < doc_id_lowerbound) {
      // want to go to the right subtree
      desired_move++;
    }
    if (k == dec->p) {
      old_tree_move--;
    } else if (data[1].left_idx == middle) {
      old_tree_move++;
    }
    if (desired_move == old_tree_move) {
      continue;
    }
    struct interpolative_ext_decoder_stack_entry *next = data + 1;
    if (desired_move == 0) {
      // left subtree
      if (data->left_idx == middle - 1) {
        dec->k = middle;
        return data->middle_value;
      }
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      interpolative_ext_decode_node (dec, next);
      dec->p = k + 1;
    } else {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          interpolative_ext_decode_int (dec);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      interpolative_ext_decode_node (dec, next);
      dec->p = k + 1;
    }
  }
  assert (0);
  return -1;
}
*/
/******************** List encoder/decoder ********************/
int inline list_too_short_for_llrun (int N, int K) {
  return (K <= 80); /* huffman tree is more than 5% list data  */
}

void list_encoder_init (struct list_encoder *enc, int N, int K, unsigned char *ptr, unsigned char *end_ptr, enum list_coding_type tp, int prefix_bit_offset) {
  bwrite_init (&enc->bw, ptr, end_ptr, prefix_bit_offset);
  enc->N = N;
  enc->K = K;
  if (N == K) {
    enc->tp = le_degenerate;
    enc->k = 0;
    enc->encode_int = &degenerate_encode_int;
    return;
  }
  enc->tp = tp;
  switch (tp) {
  case le_golomb:
    golomb_encoder_init (enc);
    return;
  case le_interpolative:
    interpolative_encoder_init (enc);
    return;
  case le_llrun:
    if (list_too_short_for_llrun (N, K)) {
      enc->tp = le_golomb;
      golomb_encoder_init (enc);
    } else {
      llrun_encoder_init (enc);
    }
    return;
  default:
    assert (0);
  }
}

static void list_decoder_init (struct list_decoder *dec, int N, int K, const unsigned char *start_ptr, enum list_coding_type tp, int prefix_bit_offset) {
  bread_init (&dec->br, start_ptr, prefix_bit_offset);
  dec->N = N;
  dec->K = K;
  dec->tp = tp;
  switch (tp) {
  case le_golomb:
    golomb_decoder_init (dec);
    break;
  case le_interpolative:
    interpolative_decoder_init (dec);
    break;
  case le_interpolative_ext:
    assert (0);
    break;
  case le_llrun:
    if (list_too_short_for_llrun (N, K)) {
      dec->tp = le_golomb;
      golomb_decoder_init (dec);
    } else {
      llrun_decoder_init (dec);
    }
    break;
  case le_degenerate:
    dec->k = 0;
    dec->decode_int = &degenerate_decode_int;
    break;
  case le_raw_int32:
    raw_int32_decoder_init (dec);
    break;
  default:
    assert (0);
  }
}

struct list_decoder *zmalloc_list_decoder (int N, int K, const unsigned char *start_ptr, enum list_coding_type tp, int prefix_bit_offset) {
  int p = 0, sz = sizeof (struct list_decoder);
  if (N == K) {
    tp = le_degenerate;
  }
  switch (tp) {
  case le_golomb:
  case le_degenerate:
    break;
  case le_interpolative:
    sz += sizeof (struct interpolative_decoder_stack_entry) * (bsr (K+1) + 1);
    break;
  case le_interpolative_ext:
    assert (0);
    break;
  case le_llrun:
    p = llrun_get_buckets_quantity (get_max_possible_gap (N, K));
    sz += ((HUFFMAN_MAX_CODE_LENGTH + 1) + (p + 1) * (HUFFMAN_MAX_CODE_LENGTH + 1)) * sizeof (int);
    break;
  case le_raw_int32:
    break;
  }
  struct list_decoder *dec = zmalloc (sz);
  dec->p = p;
  dec->size = sz;
  list_decoder_init (dec, N, K, start_ptr, tp, prefix_bit_offset);
  return dec;
}

void golomb_list_decoder_init (struct list_decoder *dec, int N, int K, const unsigned char *start_ptr, int prefix_bit_offset) {
  bread_init (&dec->br, start_ptr, prefix_bit_offset);
  dec->N = N;
  dec->K = K;
  dec->size = sizeof (struct list_decoder);
  if (K == N) {
    dec->tp = le_degenerate;
    dec->k = 0;
    dec->decode_int = &degenerate_decode_int;
  } else {
    dec->tp = le_golomb;
    golomb_decoder_init (dec);
  }
}

struct list_decoder *zmalloc_list_decoder_ext (int N, int K, const unsigned char *start_ptr, enum list_coding_type tp, int prefix_bit_offset, int extra) {
  if (tp != le_interpolative_ext) {
    return zmalloc_list_decoder (N, K, start_ptr, tp, prefix_bit_offset);
  }
  int sz = sizeof (struct list_decoder);
  sz += sizeof (struct interpolative_ext_decoder_stack_entry) * (bsr (K+1) + 1);
  struct list_decoder *dec = zmalloc (sz);
  dec->size = sz;
  dec->tp = tp;
  bread_init (&dec->br, start_ptr, prefix_bit_offset);
  dec->N = N;
  dec->K = K;
  dec->p = 0;
  dec->M = extra;
  struct interpolative_ext_decoder_stack_entry *data = (struct interpolative_ext_decoder_stack_entry *) dec->data;
  data->left_idx = 0;
  data->left_value = -1;
  data->right_idx = dec->K + 1;
  data->right_value = dec->N;
  interpolative_ext_decode_node (dec, data);
  dec->k = 0;
  dec->decode_int = &interpolative_ext_decode_int;
  return dec;
}

void zfree_list_decoder (struct list_decoder *dec) {
  zfree (dec, dec->size);
}

void list_encoder_finish (struct list_encoder *enc) {
  switch (enc->tp) {
  case le_golomb:
  case le_degenerate:
    break;
  case le_interpolative:
    interpolative_encoder_finish (enc);
    break;
  case le_llrun:
    llrun_encoder_finish (enc);
    break;
  default:
    assert (0);
  }
}

/* Interpolative code with jumps and multiplicities (mlist) functions */

static void bwrite_mlist_sublist_first_pass (struct bitwriter *bw, int *L, int *M, int u, int v, int left_subtree_size_threshold, struct left_subtree_bits_array *p, int all_ones) {
  const int sz = v - u;
  if (sz <= 1) { return; }
  const int  m = (u + v) >> 1,
            hi = L[v] - (v - m),
            lo = L[u] + (m - u),
             a = L[m] - lo,
             r = hi - lo + 1;
  bwrite_interpolative_encode_value (bw, a, r);
  if (!all_ones) {
    bwrite_gamma_code (bw, M[m]);
  }
  if (sz >= left_subtree_size_threshold) {
    assert (p->idx < p->n);
    int *q = &p->a[p->idx];
    p->idx++;
    int tree_bits = -bwrite_get_bits_written (bw);
    bwrite_mlist_sublist_first_pass (bw, L, M, u, m, left_subtree_size_threshold, p, all_ones);
    tree_bits += bwrite_get_bits_written (bw);
    *q = tree_bits;
    bwrite_gamma_code (bw, tree_bits + 1);
  } else {
    bwrite_mlist_sublist_first_pass (bw, L, M, u, m, left_subtree_size_threshold, p, all_ones);
  }
  bwrite_mlist_sublist_first_pass (bw, L, M, m, v, left_subtree_size_threshold, p, all_ones);
}

static void bwrite_mlist_sublist_second_pass (struct bitwriter *bw, int *L, int *M, int u, int v, int left_subtree_size_threshold, struct left_subtree_bits_array *p, int *redundant_bits, int all_ones) {
  const int sz = v - u;
  if (sz <= 1) { return; }
  const int  m = (u + v) >> 1,
            hi = L[v] - (v - m),
            lo = L[u] + (m - u),
             a = L[m] - lo,
             r = hi - lo + 1;
  bwrite_interpolative_encode_value (bw, a, r);
  if (!all_ones) {
    bwrite_gamma_code (bw, M[m]);
  }
  if (sz >= left_subtree_size_threshold) {
    assert (p->idx < p->n);
    int lsb = p->a[p->idx];
    p->idx++;
    if (redundant_bits != NULL) {
      (*redundant_bits) += get_gamma_code_length (lsb + 1);
    }
    bwrite_gamma_code (bw, lsb + 1);
    int tree_bits = -bwrite_get_bits_written (bw);
    bwrite_mlist_sublist_second_pass (bw, L, M, u, m, left_subtree_size_threshold, p, redundant_bits, all_ones);
    tree_bits += bwrite_get_bits_written (bw);
    assert (lsb == tree_bits);
  } else {
    bwrite_mlist_sublist_second_pass (bw, L, M, u, m, left_subtree_size_threshold, p, redundant_bits, all_ones);
  }
  bwrite_mlist_sublist_second_pass (bw, L, M, m, v, left_subtree_size_threshold, p, redundant_bits, all_ones);
}

static void mlist_decode_node (struct mlist_decoder *dec, struct mlist_decoder_stack_entry *data) {
  int middle = (data->left_idx + data->right_idx) >> 1;
  const int hi = data->right_value - (data->right_idx - middle);
  int lo = data->left_value + (middle - data->left_idx), r = hi - lo;
  if (r) {
    r++;
    struct bitreader *br = &dec->br;
    int m = br->m;
    int i = 1;
    while (i < r) {
      i <<= 1;
      if (decode_cur_bit) {
        i++;
      }
      decode_load_bit();
    }
    br->m = m;
    i -= r;
    if (i & 1) {
      lo += (r >> 1) - (i >> 1) - 1;
    } else {
      lo += (r >> 1) + (i >> 1);
    }
  }
  data->middle_value = lo;
  if (dec->all_ones) {
    data->multiplicity = 1;
  } else {
    data->multiplicity = bread_gamma_code (&dec->br);
  }
  if (data->right_idx - data->left_idx >= dec->left_subtree_size_threshold) {
    data->right_subtree_offset = bread_gamma_code (&dec->br) - 1;
    data->right_subtree_offset += bread_get_bitoffset (&dec->br);
  } else {
    data->right_subtree_offset = -1;
  }
}

void bwrite_mlist (struct bitwriter *bw, int *L, int *M, int K, int left_subtree_size_threshold, int *redundant_bits) {
  assert (L[0] == -1);
  const int v = K + 1;
  int i, all_ones = 1;
  for (i = 1; i <= K; i++) {
    assert (M[i] >= 1);
    if (M[i] > 1) {
      all_ones = 0;
      break;
    }
  }
  struct bitwriter tmp;
  memcpy (&tmp, bw, sizeof (struct bitwriter));
  unsigned char c = *(bw->ptr);
  struct left_subtree_bits_array p;
  p.n = get_subtree_array_size (0, v, left_subtree_size_threshold);
  dyn_mark_t mrk;
  dyn_mark (mrk);
  p.a = zmalloc (p.n * sizeof (int));
  p.idx = 0;
  bwrite_nbits (bw, all_ones, 1);
  bwrite_mlist_sublist_first_pass (bw, L, M, 0, v, left_subtree_size_threshold, &p, all_ones);
  memcpy (bw, &tmp, sizeof (struct bitwriter));
  *(bw->ptr) = c;
  p.idx = 0;
  if (redundant_bits != NULL) {
    *redundant_bits = 0;
  }
  bwrite_nbits (bw, all_ones, 1);
  bwrite_mlist_sublist_second_pass (bw, L, M, 0, v, left_subtree_size_threshold, &p, redundant_bits, all_ones);
  dyn_release (mrk);
}

struct mlist_decoder *zmalloc_mlist_decoder (int N, int K, const unsigned char *start_ptr, int prefix_bit_offset, int left_subtree_size_threshold) {
  int stack_sz = (K >= 0 ? bsr (K + 1) : bsr (N + 1));
  int sz = sizeof (struct mlist_decoder) + sizeof (struct mlist_decoder_stack_entry) * (stack_sz + 1);
  struct mlist_decoder *dec = zmalloc (sz);

  dec->size = sz;
  bread_init (&dec->br, start_ptr, prefix_bit_offset);

  if (K == -1) {
    K = bread_gamma_code (&dec->br);
  }

  dec->N = N;
  dec->K = K;
  dec->p = 0;
  dec->left_subtree_size_threshold = left_subtree_size_threshold;

  struct mlist_decoder_stack_entry *data = dec->stack;
  data->left_idx = 0;
  data->left_value = -1;
  data->right_idx = dec->K + 1;
  data->right_value = dec->N;
  dec->all_ones = bread_nbits (&dec->br, 1);
  mlist_decode_node (dec, data);
  dec->k = 0;

  return dec;
}

void zfree_mlist_decoder (struct mlist_decoder *dec) {
  zfree (dec, dec->size);
}

int mlist_decode_pair (struct mlist_decoder *dec, int *multiplicity) {
  if (dec->k >= dec->K) {
    *multiplicity = 0;
    return 0x7fffffff;
  }
  dec->k++;
  struct mlist_decoder_stack_entry *data = dec->stack + dec->p;
  for (;;) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    for (;;) {
      if (dec->k == middle) {
        *multiplicity = data->multiplicity;
        return data->middle_value;
      }
      if (dec->k < data->right_idx) { break; }
      dec->p--;
      data--;
      middle = (data->left_idx + data->right_idx) >> 1;
    }
    dec->p++;
    struct mlist_decoder_stack_entry *next = data + 1;
    if (dec->k < middle) {
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      mlist_decode_node (dec, next);
      data = next;
    } else {
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      mlist_decode_node (dec, next);
      data = next;
    }
  }
}

static void mlist_uptree (struct mlist_decoder *dec, struct mlist_decoder_stack_entry *data, int idx, int *multiplicity) {
  dec->k = idx;
  for (;;) {
    data--;
    (dec->p)--;
    assert (dec->p >= 0);
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (middle == idx) {
      *multiplicity = data->multiplicity;
      return;
    }
  }
}

int mlist_forward_decode_idx (struct mlist_decoder *dec, int doc_id_lowerbound, int *multiplicity) {
  if (doc_id_lowerbound >= dec->N) {
    *multiplicity = 0;
    return 0x7fffffff;
  }
  struct mlist_decoder_stack_entry *data = dec->stack;

  int p = dec->p;
  data += dec->p;
  while (data->right_value <= doc_id_lowerbound) {
    data--;
    p--;
  }

  if (p < dec->p) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (data->right_subtree_offset < 0) {
      while (dec->k < middle) {
        mlist_decode_pair (dec, multiplicity);
      }
    } else {
      bread_seek (&dec->br, data->right_subtree_offset);
      dec->k = middle;
    }
    dec->p = p;
  }

  for ( ; ; dec->p++, data++) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (data->middle_value == doc_id_lowerbound) {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          mlist_decode_pair (dec, multiplicity);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      *multiplicity = data->multiplicity;
      return data->middle_value;
    }
    const int l = data->right_idx - data->left_idx;
    assert (l >= 2);
    if (l == 2) {
      assert (data->right_value >= doc_id_lowerbound);
      if (data->middle_value < doc_id_lowerbound) {
        if (data->right_idx == dec->K + 1) {
          *multiplicity = 0;
          return 0x7fffffff;
        }
        mlist_uptree (dec, data, data->right_idx, multiplicity);
        return data->right_value;
      }
      if (data->left_value < doc_id_lowerbound) {
        dec->k = middle;
        *multiplicity = data->multiplicity;
        return data->middle_value;
      }
      assert (data->left_value >= doc_id_lowerbound);
      mlist_uptree (dec, data, data->left_idx, multiplicity);
      return data->left_value;
    }
    struct mlist_decoder_stack_entry *next = data + 1;
    if (data->middle_value > doc_id_lowerbound) {
      // left subtree
      if (data->left_idx == middle - 1) {
        dec->k = middle;
        *multiplicity = data->multiplicity;
        return data->middle_value;
      }
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      mlist_decode_node (dec, next);
    } else {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          mlist_decode_pair (dec, multiplicity);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      mlist_decode_node (dec, next);
    }
  }
  assert (0);
  return -1;
}

int mlist_forward_decode_item (struct mlist_decoder *dec, long long item_id_lowerbound, long long (*docid_to_itemid) (int), int *multiplicity) {
  struct mlist_decoder_stack_entry *data = dec->stack;

  int p = dec->p;
  data += dec->p;
  while (docid_to_itemid (data->right_value) <= item_id_lowerbound) {
    data--;
    p--;
  }

  if (p < dec->p) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (data->right_subtree_offset < 0) {
      while (dec->k < middle) {
        mlist_decode_pair (dec, multiplicity);
      }
    } else {
      bread_seek (&dec->br, data->right_subtree_offset);
      dec->k = middle;
    }
    dec->p = p;
  }

  for ( ; ; dec->p++, data++) {
    int middle = (data->left_idx + data->right_idx) >> 1;
    if (docid_to_itemid (data->middle_value) == item_id_lowerbound) {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          mlist_decode_pair (dec, multiplicity);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      *multiplicity = data->multiplicity;
      return data->middle_value;
    }
    const int l = data->right_idx - data->left_idx;
    assert (l >= 2);
    if (l == 2) {
      assert (docid_to_itemid (data->right_value) >= item_id_lowerbound);
      if (docid_to_itemid (data->middle_value) < item_id_lowerbound) {
        if (data->right_idx == dec->K + 1) {
          return -1;
        }
        mlist_uptree (dec, data, data->right_idx, multiplicity);
        return data->right_value;
      }
      if (docid_to_itemid (data->left_value) < item_id_lowerbound) {
        dec->k = middle;
        *multiplicity = data->multiplicity;
        return data->middle_value;
      }
      //assert (data->left_value >= doc_id_lowerbound);
      mlist_uptree (dec, data, data->left_idx, multiplicity);
      return data->left_value;
    }
    struct mlist_decoder_stack_entry *next = data + 1;
    if (docid_to_itemid (data->middle_value) > item_id_lowerbound) {
      // left subtree
      if (data->left_idx == middle - 1) {
        dec->k = middle;
        *multiplicity = data->multiplicity;
        return data->middle_value;
      }
      next->left_idx = data->left_idx;
      next->left_value = data->left_value;
      next->right_idx = middle;
      next->right_value = data->middle_value;
      mlist_decode_node (dec, next);
    } else {
      if (data->right_subtree_offset < 0) {
        while (dec->k < middle) {
          mlist_decode_pair (dec, multiplicity);
        }
      } else {
        bread_seek (&dec->br, data->right_subtree_offset);
        dec->k = middle;
      }
      next->left_idx = middle;
      next->left_value = data->middle_value;
      next->right_idx = data->right_idx;
      next->right_value = data->right_value;
      mlist_decode_node (dec, next);
    }
  }
  assert (0);
  return -1;
}

/*********************** CHECK VERSION *****************************/
int check_listcomp_version (int version) {
  if ( (version ^ listcomp_version) & 0xffff0000) {
    return 0;
  }
  if ( (version & 0xffff) > (listcomp_version & 0xffff)) {
    return 0;
  }
  return 1;
}
