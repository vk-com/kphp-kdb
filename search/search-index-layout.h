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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
              2010-2013 Vitaliy Valtman
              2010-2013 Anton Maydell
*/

#ifndef __SEARCH_INDEX_LAYOUT_H__
#define	__SEARCH_INDEX_LAYOUT_H__

#include "kdb-data-common.h"

#pragma	pack(push,4)

#define	SEARCH_INDEX_MAGIC	0x11efaa61
#define	SEARCH_INDEX_WITH_CRC32_MAGIC	(SEARCH_INDEX_MAGIC + 0x100)
#define	SEARCHX_INDEX_MAGIC	0x8acb81ee
#define	SEARCHY_INDEX_MAGIC	0x7f0b857e

#define FLAG_CLS_ENABLE_TAGS 1
#define FLAGS_CLS_USE_STEMMER 2
#define FLAGS_CLS_ENABLE_UNIVERSE 4
#define FLAGS_CLS_ENABLE_ITEM_WORDS_FREQS 8
#define FLAGS_CLS_CREATION_DATE 16
#define FLAGS_CLS_WORD_SPLIT_UTF8 32
#define FLAGS_CLS_TAG_OWNER 64

struct search_index_header {
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  unsigned long long slice_hash;
  long long compression_bytes[8];
  long long item_texts_size;
  long long index_items_size;
  int items;
  int words;
  int hapax_legomena;
  int frequent_words;
  int log_timestamp;
  int log_split_min, log_split_max, log_split_mod;
  unsigned log_pos1_crc32;	// 0 in old files
  int stemmer_version;
  int word_split_version;
  int listcomp_version;
  int command_line_switches_flags;
  int word_list_compression_methods[2];
  int left_subtree_size_threshold; // used in interpolative ext compression method
};

struct search_index_crc32_header {
  unsigned crc32_header;
  unsigned crc32_items;
  unsigned crc32_text;
  unsigned crc32_words;
  unsigned crc32_hapax_legomena;
  unsigned crc32_data;
};

/* header->items of these, 4-byte aligned */
struct search_item_text {
  long long item_id;
  int len;
  char text[1];	// len+1 bytes + 4-byte alignment
};

/* header->words+1 of these, then
   header->frequent_words+1 more */
struct search_index_word {
  hash_t word;
  long long file_offset;	// if len=1 or 2, item_id here
#ifndef SEARCHX
  long long file_offset_subseq; // if len_subseq=1 or 2, numbers are here
#endif
  int len;		// len=-len indicates that list is <= 8 bytes, kept in file_offset
#ifndef SEARCHX
  int len_subseq; // len_subseq=-len_subseq indicates that list is <= 8 bytes, kept in file_offset_subseq
#endif
  //unsigned short requests;	// sort of priority
  unsigned short bytes;		// bytes used for this entry, 0xffff = large
#ifndef SEARCHX
  unsigned short bytes_subseq;		// bytes used for this entry, 0xffff = large
#endif
};

struct search_index_hapax_legomena {
  hash_t word;
  unsigned int doc_id_and_priority; //high bit contains priority flag
};

/* then header->words+header->frequent_words lists follow, 1-byte aligned, in Golomb code */


struct searchy_index_word {
  hash_t word;
  long long file_offset;
  int len; // len=-len indicates that list is <= 8 bytes, kept in file_offset
};

extern int universal, hashtags_enabled, use_stemmer, wordfreqs_enabled, creation_date, tag_owner;
int get_cls_flags (void);
int check_header (struct search_index_header *H);

#pragma	pack(pop)

#endif
