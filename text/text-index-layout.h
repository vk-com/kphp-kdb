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
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
*/

#ifndef __TEXT_INDEX_LAYOUT_H__
#define	__TEXT_INDEX_LAYOUT_H__

#define	TEXT_INDEX_MAGIC	0x11efbeef
#define	TEXT_INDEX_SEARCH_MAGIC	0x11effeba
#define	TEXT_INDEX_CRC_MAGIC	0xef11beef
#define	TEXT_INDEX_CRC_SEARCH_MAGIC	0xef11feba
#define	TEXT_INDEX_CRC_SEARCH_HISTORY_MAGIC	0xef11bae3
#define	MAX_SUBLISTS		16

#define	MATCHES_SUBLIST(__flags,__subdescr) (!(((__flags) ^ (__subdescr).xor_mask) & (__subdescr).and_mask))

union packed_sublist_descr {
  struct { unsigned short xor_mask, and_mask; };
  int combined_xor_and;
};

struct text_index_header {
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  unsigned long long slice_hash;
  int log_timestamp;
  int log_split_min, log_split_max, log_split_mod;
  long long sublists_descr_offset;
  long long word_char_dictionary_offset;
  long long notword_char_dictionary_offset;
  long long word_dictionary_offset;
  long long notword_dictionary_offset;
  long long user_list_offset;
  long long user_data_offset;
  long long extra_data_offset;
  long long data_end_offset;
  int tot_users;
  union packed_sublist_descr peer_list_mask;
  int sublists_num;
  int last_global_id;
  int max_legacy_id;
  int min_legacy_id;
  long long dictionary_log_pos;
  unsigned log_pos0_crc32;
  unsigned log_pos1_crc32;
  int extra_fields_mask;
  int search_coding_used;	// 0 = none; bit 0 (+1) = use stemmer; bit 1 (+2) = search v1
  int reserved[19];
  unsigned header_crc32;
};

struct file_user_list_entry {
  long long user_id;
  long long user_data_offset;
  int user_data_size;
  int user_last_local_id;
  int user_sublists_size[0];
};

struct file_user_list_entry_search {
  long long user_id;
  long long user_data_offset;
  int user_data_size;
  int user_last_local_id;
  int user_search_size;		// present only for TEXT_INDEX_SEARCH_MAGIC
  int user_sublists_size[0];
};

struct file_user_list_entry_search_history {
  long long user_id;
  long long user_data_offset;
  int user_data_size;
  int user_last_local_id;
  int user_search_size;		// present only for TEXT_INDEX_SEARCH{,_HISTORY}_MAGIC
  int user_history_min_ts;	// present only for TEXT_INDEX_SEARCH_HISTORY_MAGIC
  int user_history_max_ts;	// present only for TEXT_INDEX_SEARCH_HISTORY_MAGIC
  int user_sublists_size[0];
};

#define FILE_USER_MAGIC 0x11efc0ba
#define FILE_USER_SEARCH_MAGIC 0x11ef0bec
#define FILE_USER_HISTORY_MAGIC 0x11ef6c0d

struct file_user_header {
  int magic;
  int user_last_local_id;
  int user_first_local_id;
  int sublists_num;
  long long user_id;
  int peers_num;
  int peers_offset;
  int sublists_offset;
  int legacy_list_offset;
  int directory_offset;
  int data_offset;
  int extra_offset;
  int total_bytes;
};

struct file_search_header {
  int magic;
  int words_num;
  int word_start[0];   // words_num+1 byte offsets w.r. to start of this sub-metafile
  // word sub-metafiles follow, each padded for an integer number of bytes
};

// word sub-metafiles:
// 1. (packed) word (using word dictionary and word-char dictionary)
// 2. length of list L in alpha-encoding (i.e. k*1, 1*0, k digits of L-2^k, 2^k <= L < 2^(k+1)))
//    L=1 -> "0", L=2 -> "100", L=3 -> "101", L=4 -> "11000"
// 3. packed list of local_id's (user_first_local_id-based); 
//    use interpolative encoding
// 4. align to byte boundary with "1" + up to 7 "0".

#define	PERSISTENT_HISTORY_TS_START	0

struct file_history_header {
  int magic;
  int history_min_ts;
  int history_max_ts;
  int history[0];
// (history_max_ts - history_min_ts) 8-byte history entries follow
};

struct history_entry {
  int local_id;
  short flags;
  char reserved;
  unsigned char type;
};


struct file_message {
  int flags;
  int peer_id;
  int date;
// int legacy_id IF (flags & TXF_HAS_LEGACY_ID) | long long legacy_id IF (flags & TXF_HAS_LONG_LEGACY_ID)
// int peer_msg_id IF (flags & TXF_HAS_PEER_MSG_ID)
// int extra[__builtin_popcount(flags & TXF_HAS_EXTRAS)] IF (flags & TXF_HAS_EXTRAS)
  int data[0];
};

struct file_message_extras {
  int global_id;
  unsigned ip;
  int port;
  unsigned front;
  unsigned long long ua_hash;
};

struct file_char_dictionary {
  int dict_size;
  unsigned char char_code_len[256];
  int char_freq[256];
};

struct file_word_dictionary {
  int dict_size;	// = N
  int offset[1];	// = N+1 offsets from the beginning of the structure
  			// N struct word_dictionary_entry follow
};

struct file_word_dictionary_entry {
  unsigned char code_len;
  unsigned char str_len;
  char str[1];
};

#endif
