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

#ifndef __LIST_COMPRESSION_H__
#define __LIST_COMPRESSION_H__

#include <strings.h>
extern const int listcomp_version;

enum list_coding_type {
  le_degenerate = -1, /* not a compression method (internal usage only for trivial case) */
  le_golomb = 0,
  le_llrun = 1,
  le_interpolative = 2,
  le_interpolative_ext = 3,
  le_raw_int32 = 4
};

struct bitwriter {
  unsigned char *ptr;
  unsigned char *start_ptr;
  unsigned char *end_ptr;
  unsigned int prefix_bit_offset;
  int m;
};

int get_gamma_code_length (unsigned int d);
void bwrite_init (struct bitwriter *bw, unsigned char *start_ptr, unsigned char *end_ptr, unsigned int prefix_bits_offset);
inline void bwrite_nbits (struct bitwriter *bw, unsigned int d, int n);
inline void bwrite_nbitsull (struct bitwriter *bw, unsigned long long d, int n);
inline void bwrite_gamma_code (struct bitwriter *bw, unsigned int d);
void bwrite_interpolative_sublist (struct bitwriter *bw, int *L, int u, int v);

/* exteded interpolative : supports fast skip, stores left subtree encoded bits */
void bwrite_interpolative_ext_sublist (struct bitwriter *bw, int *L, int u, int v, int left_subtree_size_threshold, int *redundant_bits);
unsigned int bwrite_get_bits_written (const struct bitwriter *bw);

struct bitreader {
  const unsigned char *start_ptr;
  const unsigned char *ptr;
  unsigned int prefix_bit_offset;
  int m;
};

static inline unsigned int bread_get_bitoffset (const struct bitreader *br) {
  return ((br->ptr - br->start_ptr) << 3) + (ffs (br->m) - 32);
}

static inline unsigned int bread_get_bits_read (const struct bitreader *br) {
  return ((br->ptr - br->start_ptr) << 3) + (ffs (br->m) - 32) - br->prefix_bit_offset;
}

void bread_init (struct bitreader *br, const unsigned char *start_ptr, int prefix_bit_offset);
inline void bread_seek (struct bitreader *br, unsigned int bit_offset);
inline unsigned int bread_nbits (struct bitreader *br, int n);
inline unsigned long long bread_nbitsull (struct bitreader *br, int n);
unsigned int bread_gamma_code (struct bitreader *br);

struct list_encoder {
  void (*encode_int) (struct list_encoder *enc, int d);
  struct bitwriter bw;
  int *L; /* input data (used in interpolative and llrun encoders) */
  int k, p, M, last;
  int N, K;
  enum list_coding_type tp;
};

struct list_decoder {
  int (*decode_int) (struct list_decoder *dec);
  struct bitreader br;
  int size;
  int k, p, M, last;
  int N, K;
  enum list_coding_type tp;
  int data[0];
};

void list_encoder_init (struct list_encoder *enc, int N, int K, unsigned char *ptr, unsigned char *end_ptr, enum list_coding_type tp, int prefix_bit_offset);

void list_encoder_finish (struct list_encoder *enc);

/* memory used by list_decoder depends on coding type */
struct list_decoder *zmalloc_list_decoder (int N, int K, const unsigned char *start_ptr, enum list_coding_type tp, int prefix_bit_offset);

/* le_interpolative_ext compression method need additional parameter (left_subtree_size_threshold) which passed to decoder in extra */
/* for other compression method extra is ignored */
struct list_decoder *zmalloc_list_decoder_ext (int N, int K, const unsigned char *start_ptr, enum list_coding_type tp, int prefix_bit_offset, int extra);
void zfree_list_decoder (struct list_decoder *dec);

/* Golomb list decoder init function (reduce number zmalloc/zfree calls in searchy) */
void golomb_list_decoder_init (struct list_decoder *dec, int N, int K, const unsigned char *start_ptr, int prefix_bit_offset);

struct interpolative_ext_decoder_stack_entry {
  int left_idx;
  int left_value;
  int middle_value;
  int right_idx;
  int right_value;
  int right_subtree_offset;
};
void interpolative_ext_decode_node (struct list_decoder *dec, struct interpolative_ext_decoder_stack_entry *data);
int list_interpolative_ext_forward_decode_idx (struct list_decoder *dec, int doc_id_lowerbound);

const char* list_get_compression_method_description (int compression_method);

#define HUFFMAN_MAX_CODE_LENGTH 15
// N - alphabet size, L - max code length
int* get_huffman_codes_lengths (int *freq, int N, int L, int *alphabet_size);
void bread_huffman_codes (struct bitreader *br, int *l, int N, int *alphabet_size);
void bwrite_huffman_codes (struct bitwriter *bw, int *l, int N);
void canonical_huffman (int *l, int N, int L, int* firstcode, int *codeword, int *symbols);
/* alpabets should contains more than one symbol */
int bread_huffman_decode_int (struct bitreader *br, int *firstcode ,int *symbols);

/* mlist : compression increasing sequence of integers with multiplicities using interpolative encoding with jumps */

struct mlist_decoder_stack_entry {
  int left_idx;
  int left_value;
  int middle_value;
  int right_idx;
  int right_value;
  int right_subtree_offset;
  int multiplicity;
};

struct mlist_decoder {
  struct bitreader br;
  int size;
  int k, p, last;
  int left_subtree_size_threshold;
  int N, K;
  int all_ones;
  struct mlist_decoder_stack_entry stack[0];
};

/* bwrite_mlist encodes list with multiplicities:
   caller should fill bound of L array with L[0] = -1 and L[K+1] = N
   L[0] < L[1] < ... < L[K+1]
   M[i] contains multiplicity of L[i] 
*/
void bwrite_mlist (struct bitwriter *bw, int *L, int *M, int K, int left_subtree_size_threshold, int *redundant_bits);
struct mlist_decoder *zmalloc_mlist_decoder (int N, int K, const unsigned char *start_ptr, int prefix_bit_offset, int left_subtree_size_threshold);
void zfree_mlist_decoder (struct mlist_decoder *dec);
int mlist_decode_pair (struct mlist_decoder *dec, int *multiplicity);
int mlist_forward_decode_idx (struct mlist_decoder *dec, int doc_id_lowerbound, int *multiplicity);

/* returns docid: docid_to_itemid (docid) >= item_id_lowerbound */
/* returns -1 if no such docid exists */
/* in case doc_id >= items docid_to_itemid should return MAX_ITEM_ID */
int mlist_forward_decode_item (struct mlist_decoder *dec, long long item_id_lowerbound, long long (*docid_to_itemid) (int), int *multiplicity);
int check_listcomp_version (int version);


/******************** low level structures and functions ********************/
struct left_subtree_bits_array {
  int *a;
  int idx;
  int n;
};
int get_subtree_array_size (int u, int v, int left_subtree_size_threshold);
void bwrite_interpolative_encode_value (struct bitwriter *bw, int a, int r);

#endif

