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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <byteswap.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kdb-text-binlog.h"
#include "text-index-layout.h"
#include "text-data.h"
#include "server-functions.h"
#include "word-split.h"
#include "stemmer.h"
#include "listcomp.h"
#include "aho-kmp.h"
#include "vv-tl-aio.h"

#define unlikely(x) __builtin_expect((x),0)

#define FITS_AN_INT(a) ( (a) >= -0x80000000LL && (a) <= 0x7fffffffLL )

extern int now;
extern int verbosity;

extern long long active_aio_queries;

//struct aio_connection *WaitAio, *WaitAio2, *WaitAio3;

struct file_message *user_metafile_message_lookup (char *metafile, int metafile_size, int local_id, user_t *U);
static void process_delayed_ops (user_t *U, tree_t *T);
static void process_delayed_values (user_t *U, tree_t *T);

static inline int feed_byte (int c, int *state);

#define	UserHdr	((struct file_user_header *) metafile)

/* --------- text data ------------- */

/* write all events into binlog, even if they don't change anything */
int write_all_events = /* 1 */ 0;
/* history enabled */
int history_enabled = 0;
/* online status preserved for hold_online_time seconds */
int hold_online_time = DEFAULT_HOLD_ONLINE_TIME;

int index_long_legacy_id = 0;

struct sublist_descr Messages_Sublists[] = {
{ 0, TXF_OUTBOX | TXF_SPAM | TXF_DELETED },
{ TXF_OUTBOX, TXF_OUTBOX | TXF_SPAM | TXF_DELETED },
{ TXF_UNREAD, TXF_OUTBOX | TXF_UNREAD | TXF_SPAM | TXF_DELETED },
{ TXF_IMPORTANT, TXF_IMPORTANT | TXF_SPAM | TXF_DELETED },
{ TXF_FRIENDS, TXF_OUTBOX | TXF_FRIENDS | TXF_SPAM | TXF_DELETED },
{ TXF_OUTBOX | TXF_FRIENDS, TXF_OUTBOX | TXF_FRIENDS | TXF_SPAM | TXF_DELETED },
{ TXF_UNREAD | TXF_CHAT, TXF_OUTBOX | TXF_UNREAD | TXF_CHAT | TXF_SPAM | TXF_DELETED },
{ 0, TXF_OUTBOX | TXF_CHAT | TXF_SPAM | TXF_DELETED },
{ TXF_OUTBOX, TXF_OUTBOX | TXF_CHAT | TXF_SPAM | TXF_DELETED },
{ TXF_UNREAD, TXF_OUTBOX | TXF_UNREAD | TXF_CHAT | TXF_SPAM | TXF_DELETED },
{ TXF_SPAM, TXF_SPAM | TXF_DELETED },
{ TXF_UNREAD | TXF_SPAM, TXF_UNREAD | TXF_SPAM | TXF_DELETED },
{ 0, 0 }
};

struct sublist_descr Statuses_Sublists[] = {
{ 0, TXFS_WALL | TXFS_REPLY | TXFS_COPY | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_WALL | TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ TXFS_COPY, TXFS_WALL | TXFS_REPLY | TXFS_COPY | TXFS_SPAM | TXFS_DELETED },
{ TXFS_COPIED, TXFS_WALL | TXFS_REPLY | TXFS_COPY | TXFS_COPIED | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ TXFS_WALL, TXFS_WALL | TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ TXFS_WALL, TXFS_WALL | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_SPAM | TXFS_DELETED },
{ TXFS_LOCATION, TXFS_WALL | TXFS_REPLY | TXFS_COPY | TXFS_LOCATION | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_FRIENDSONLY | TXFS_WALL | TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ 0, TXFS_FRIENDSONLY | TXFS_REPLY | TXFS_SPAM | TXFS_DELETED },
{ 0, 0 }
};

struct sublist_descr Forum_Sublists[] = {
{ TXFP_TOPIC, TXFP_REPLY | TXFP_TOPIC | TXFP_FIXED | TXFP_CLOSED | TXFP_SPAM | TXFP_DELETED },
{ TXFP_TOPIC, TXFP_REPLY | TXFP_TOPIC | TXFP_FIXED | TXFP_SPAM | TXFP_DELETED },
{ TXFP_TOPIC | TXFP_FIXED, TXFP_REPLY | TXFP_TOPIC | TXFP_FIXED | TXFP_SPAM | TXFP_DELETED },
{ TXFP_TOPIC | TXFP_CLOSED, TXFP_REPLY | TXFP_TOPIC | TXFP_CLOSED | TXFP_SPAM | TXFP_DELETED },
{ 0, 0 }
};

struct sublist_descr Comments_Sublists[] = {
{ 0, TXF_SPAM | TXF_DELETED },
{ TXF_SPAM, TXF_SPAM | TXF_DELETED },
{ 0, 0 }
};

int onload_user_metafile (struct connection *c, int read_bytes);
int onload_search_metafile (struct connection *c, int read_bytes);
int onload_history_metafile (struct connection *c, int read_bytes);


conn_type_t ct_metafile_user_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_user_metafile
};

conn_type_t ct_metafile_search_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_search_metafile
};

conn_type_t ct_metafile_history_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_history_metafile
};

conn_query_type_t aio_metafile_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "text-data-aio-metafile-query",
.wakeup = aio_query_timeout,
.close = delete_aio_query,
.complete = delete_aio_query
};


struct sublist_descr *Sublists = Messages_Sublists;
struct sublist_descr PeerFlagFilter = { 0, TXF_DELETED | TXF_SPAM }, 
	    Statuses_PeerFlagFilter = { 0, TXFS_DELETED | TXFS_SPAM | TXFS_COPY },
	       Forum_PeerFlagFilter = { 0, TXFP_TOPIC | TXFP_DELETED | TXFP_SPAM },
            Comments_PeerFlagFilter = { 0, TXF_DELETED | TXF_SPAM };
struct sublist_descr NewMsgHistoryFilter = { 0, TXF_DELETED | TXF_SPAM };
union packed_sublist_descr Sublists_packed[MAX_SUBLISTS+1];
int sublists_num = 0;
int last_global_id;
int index_extra_mask = 0, read_extra_mask = 0, write_extra_mask = 0, unpacked_extra_mask;
int extra_ints, text_shift;

int search_enabled, use_stemmer, hashtags_enabled, searchtags_enabled;
int persistent_history_enabled;

int incore_persistent_history_lists, incore_persistent_history_events;
long long incore_persistent_history_bytes;

int text_replay_logevent (struct lev_generic *E, int size);



int count_sublists (void) {
  int i = 0;
  while (Sublists[i].xor_mask || Sublists[i].and_mask) {
    Sublists_packed[i].and_mask = Sublists[i].and_mask;
    Sublists_packed[i].xor_mask = Sublists[i].xor_mask;
    i++;
  }
  sublists_num = i;
  assert (sublists_num <= MAX_SUBLISTS);
  return i;
}


int init_text_data (int schema) {

  count_sublists();

  replay_logevent = text_replay_logevent;

  return 0;
}

/* --------- LOAD INDEX -------- */

#define	MAX_FILE_DICTIONARY_BYTES	(1 << 27)
#define	MAX_USERLIST_BYTES		(1 << 27)

struct char_dictionary {
  int dict_size;
  unsigned char code_len[256];
  int used_codes;
  int max_bits;
  int reserved;
  unsigned first_codes[32];
  int *code_ptr[32];
  int chars[256];
};

struct word_dictionary {
  char *raw_data;
  int raw_data_len;
  int word_num;
  int max_bits;
  int reserved;
  struct file_word_dictionary_entry **words;
  unsigned first_codes[32];
  struct file_word_dictionary_entry **code_ptr[32];
};

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;

struct text_index_header Header;

int tree_nodes, online_tree_nodes, incore_messages;
int msg_search_nodes, edit_text_nodes;
long msg_search_nodes_bytes, edit_text_bytes;
int idx_fd, idx_users, idx_loaded_bytes, idx_last_global_id;
int idx_search_enabled, idx_crc_enabled, idx_persistent_history_enabled, idx_sublists_offset;
long long idx_fsize;

long metafile_crc_check_size_threshold = (1L << 26);

int userlist_entry_size;

union packed_sublist_descr idx_Sublists_packed[MAX_SUBLISTS+1];
struct sublist_descr idx_Sublists[MAX_SUBLISTS+1];

struct char_dictionary WordCharDict, NotWordCharDict;
struct word_dictionary WordDict, NotWordDict;

char *user_list_metafile;
struct file_user_list_entry **FileUsers;

int use_aio = 1;
long long aio_read_errors, aio_crc_errors;

void *load_index_part (void *data, long long offset, long long size, int max_size) {
  int r;
  static struct iovec io_req[2];
  static unsigned index_part_crc32;
  assert (size >= 0 && size <= (long long) (dyn_top - dyn_cur));
  assert (size <= max_size || !max_size);
  assert (offset >= 0 && offset <= idx_fsize && offset + size <= idx_fsize);
  if (!data) {
    data = zmalloc (size);
  }
  assert (lseek (idx_fd, offset, SEEK_SET) == offset);
  io_req[0].iov_base = data;
  io_req[0].iov_len = size;
  io_req[1].iov_base = &index_part_crc32;
  io_req[1].iov_len = 4;
  r = readv (idx_fd, io_req, 1 + idx_crc_enabled);
  size += idx_crc_enabled * 4;
  if (r != size) {
    fprintf (stderr, "error reading data from index file: read %d bytes instead of %lld at position %lld: %m\n", r, size, offset);
    assert (r == size);
    return 0;
  }
  if (idx_crc_enabled) {
    unsigned data_crc32 = compute_crc32 (data, size - 4);
    if (data_crc32 != index_part_crc32) {
      fprintf (stderr, "error reading %lld bytes from index file at position %lld: crc32 mismatch: expected %08x, actual %08x\n", size, offset, index_part_crc32, data_crc32);
      assert (data_crc32 == index_part_crc32);
      return 0;
    }
  }
  idx_loaded_bytes += r;
  return data;
}

struct char_dictionary *load_char_dictionary (struct char_dictionary *D, long long offset) {
  int i, j, k;
  unsigned long long x;
  D = load_index_part (D, offset, 4+256, 0);
  if (!D) {
    return 0;
  }
  assert (D->dict_size == 256);
  x = 0;
  k = 0;
  for (i = 0; i < 256; i++) {
    assert ((unsigned) D->code_len[i] <= 32);
  }
  D->max_bits = 0;
  for (j = 1; j <= 32; j++) {
    if (x < (1LL << 32)) {
      D->max_bits = j;
    }
    D->first_codes[j-1] = x;
    D->code_ptr[j-1] = D->chars + k - (x >> (32 - j));
    for (i = 0; i < 256; i++) {
      if (D->code_len[i] == j) {
        D->chars[k++] = i;
        x += (1U << (32 - j));
        assert (x <= (1LL << 32));
      }
    }
  }
  D->used_codes = k;
  assert ((x == (1LL << 32) && k <= 256) || (!x && !k));
  return D;        
}

struct word_dictionary *load_dictionary (struct word_dictionary *D, long long offset, long long size) {
  int N, i, j, k;
  struct file_word_dictionary *tmp;
  long long x;
  D->raw_data = load_index_part (0, offset, size, MAX_FILE_DICTIONARY_BYTES);
  assert (D->raw_data);
  D->raw_data_len = size;
  assert (size >= 4);
  tmp = (struct file_word_dictionary *) D->raw_data;
  
  N = tmp->dict_size;
  assert (N >= 0 && N <= (size >> 2) - 2);
  D->word_num = N;

  assert (tmp->offset[0] >= (N+2)*4 && tmp->offset[0] <= size);
  assert (tmp->offset[N] <= size);

  D->words = zmalloc (N*sizeof(void *));

  for (i = 0; i < N; i++) {
    struct file_word_dictionary_entry *E = (struct file_word_dictionary_entry *) (D->raw_data + tmp->offset[i]);
    assert (tmp->offset[i] < tmp->offset[i+1]);
    assert (tmp->offset[i+1] <= size);
    assert (tmp->offset[i] + E->str_len + 2 <= tmp->offset[i+1]);
    assert (E->code_len <= 32 && E->code_len >= 1);
  }

  D->max_bits = 32;
  
  x = 0;
  k = 0;
  for (j = 1; j <= 32; j++) {
    if (x < (1LL << 32)) {
      D->max_bits = j;
    }
    D->first_codes[j-1] = x;
    D->code_ptr[j-1] = D->words + k - (x >> (32 - j));
    for (i = 0; i < N; i++) {
      struct file_word_dictionary_entry *E = (struct file_word_dictionary_entry *) (D->raw_data + tmp->offset[i]);
      if (E->code_len == j) {
        D->words[k++] = E;
        x += (1U << (32 - j));
        assert (x <= (1LL << 32));
      }
    }
  }
  assert (k == N && (x == (1LL << 32) || (!k && !x)));
  return D;
}

#define	LN	255

static int LX[LN+3], LY[LN+3], LZ[LN+3];

void lm_heap_insert (int data_size, int user_id, int last_local_id) {
  int i, j;
  if (data_size <= LX[1]) {
    return;
  }
  i = 1;
  while (1) {
    j = i*2;
    if (j > LN) {
      break;
    }
    if (LX[j] > LX[j+1]) {
      j++;
    }
    if (data_size <= LX[j]) {
      break;
    }
    LX[i] = LX[j];
    LY[i] = LY[j];
    LZ[i] = LZ[j];
    i = j;
  }
  LX[i] = data_size;
  LY[i] = user_id;
  LZ[i] = last_local_id;
}

#define	BB0(x)	x,
#define BB1(x)	BB0(x) BB0(x+1) BB0(x+1) BB0(x+2)
#define BB2(x)	BB1(x) BB1(x+1) BB1(x+1) BB1(x+2)
#define BB3(x)	BB2(x) BB2(x+1) BB2(x+1) BB2(x+2)
#define BB4(x)	BB3(x) BB3(x+1) BB3(x+1) BB3(x+2)
#define BB5(x)	BB4(x) BB4(x+2) BB4(x+2) BB4(x+4)
#define BB6(x)	BB5(x) BB5(x+2) BB5(x+2) BB5(x+4)

char prec_mask_intcount[MAX_EXTRA_MASK+2] = { BB6(0) 0 };

static inline int extra_mask_intcount (int mask) {
  return prec_mask_intcount[mask & MAX_EXTRA_MASK];
}

static void init_extra_mask (int value) {
  assert (!index_extra_mask && !tree_nodes && !incore_messages);
  index_extra_mask = read_extra_mask = write_extra_mask = value;
  assert (!(index_extra_mask & ~MAX_EXTRA_MASK));
  extra_ints = extra_mask_intcount (index_extra_mask);
  text_shift = extra_ints * 4;
}

static void change_extra_mask (int value) {
  assert (!(value & ~MAX_EXTRA_MASK));
  read_extra_mask &= write_extra_mask = value;
}

int load_index (kfs_file_handle_t Index) {
  int r, i;
  int fd;
  long long fsize;

  assert (Index);
  fd = Index->fd;
  fsize = Index->info->file_size - Index->offset;

  r = read (fd, &Header, sizeof (struct text_index_header));
  if (r < 0) {
    fprintf (stderr, "error reading index file header: %m\n");
    return -3;
  }
  if (r < sizeof (struct text_index_header)) {
    fprintf (stderr, "index file too short: only %d bytes read\n", r);
    return -2;
  }

  if (Header.magic != TEXT_INDEX_MAGIC && Header.magic != TEXT_INDEX_SEARCH_MAGIC && Header.magic != TEXT_INDEX_CRC_MAGIC && Header.magic != TEXT_INDEX_CRC_SEARCH_MAGIC && Header.magic != TEXT_INDEX_CRC_SEARCH_HISTORY_MAGIC) {
    fprintf (stderr, "bad text index file header\n");
    return -4;
  }

  assert ((unsigned) Header.sublists_num <= MAX_SUBLISTS);
  assert ((unsigned) Header.tot_users <= MAX_USERS_NUM);
  assert (Header.last_global_id >= 0);
  if (Header.magic != TEXT_INDEX_CRC_SEARCH_HISTORY_MAGIC) {
    assert (((Header.magic ^ TEXT_INDEX_SEARCH_MAGIC) & 0xffff) || (Header.search_coding_used & ~29) == 2);
    assert (((Header.magic ^ TEXT_INDEX_MAGIC) & 0xffff) || !Header.search_coding_used);
  }
  assert (!Header.search_coding_used || (Header.search_coding_used & ~29) == 2);

  if (search_enabled && !Header.search_coding_used) {
    fprintf (stderr, "search support requested but index does not contain search data\n");
    return -4;
  }

  if (search_enabled && use_stemmer != (Header.search_coding_used & 1)) {
    fprintf (stderr, "stemmer support requested but index was generated without stemmer, or conversely\n");
    return -4;
  }

  if (search_enabled && hashtags_enabled * 4 != (Header.search_coding_used & 4)) {
    fprintf (stderr, "hashtag support requested but index was generated without hashtags, or conversely\n");
    return -4;
  }

  if (search_enabled && searchtags_enabled * 16 != (Header.search_coding_used & 16)) {
    fprintf (stderr, "search tag support requested but index was generated without search tags, or conversely\n");
    return -4;
  }

  if (search_enabled && word_split_utf8 * 8 != (Header.search_coding_used & 8)) {
    fprintf (stderr, "native utf8 search support requested but index was generated not in utf8 mode, or conversely\n");
    return -4;
  }

  if (persistent_history_enabled && Header.magic != TEXT_INDEX_CRC_SEARCH_HISTORY_MAGIC) {
    fprintf (stderr, "persistent history support requested but index does not contain history data\n");
    return -4;
  }

  idx_persistent_history_enabled = (Header.magic == TEXT_INDEX_CRC_SEARCH_HISTORY_MAGIC);
  idx_search_enabled = (Header.search_coding_used != 0);
  idx_crc_enabled = !((Header.magic ^ TEXT_INDEX_CRC_MAGIC) & -0x10000);
  idx_sublists_offset = (idx_persistent_history_enabled ? 3 : idx_search_enabled);

  last_global_id = idx_last_global_id = Header.last_global_id;
  first_extra_global_id = last_global_id + 1;

  if (idx_crc_enabled) {
    unsigned hdr_crc32 = compute_crc32 (&Header, offsetof (struct text_index_header, header_crc32));
    if (hdr_crc32 != Header.header_crc32) {
      fprintf (stderr, "index header crc32 mismatch: %08x expected, %08x actual\n", Header.header_crc32, hdr_crc32);
      return -4;
    }
  } else {
    assert (!Header.header_crc32);
  }

  count_sublists();

  if (Header.sublists_num != sublists_num) {
    memcpy (&PeerFlagFilter, &Statuses_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Statuses_Sublists;
    count_sublists ();

    if (Header.sublists_num != sublists_num) {
      memcpy (&PeerFlagFilter, &Forum_PeerFlagFilter, sizeof (PeerFlagFilter));
      Sublists = Forum_Sublists;
      count_sublists ();

      if (Header.sublists_num != sublists_num) {
	memcpy (&PeerFlagFilter, &Comments_PeerFlagFilter, sizeof (PeerFlagFilter));
	Sublists = Comments_Sublists;
	count_sublists ();
      }
    }
  }

  assert (Header.sublists_num == sublists_num);

  userlist_entry_size = sizeof (struct file_user_list_entry) + 4 * (Header.sublists_num + (idx_persistent_history_enabled ? 3 : idx_search_enabled));

  assert (Header.sublists_descr_offset >= sizeof (struct text_index_header));
  assert (Header.sublists_descr_offset + Header.sublists_num * 4 + idx_crc_enabled * 4 <= Header.word_char_dictionary_offset);
  assert (Header.word_char_dictionary_offset + sizeof (struct file_char_dictionary) + idx_crc_enabled * 4 <= Header.notword_char_dictionary_offset);
  assert (Header.notword_char_dictionary_offset + sizeof (struct file_char_dictionary) + idx_crc_enabled * 4 <= Header.word_dictionary_offset);
  assert (Header.notword_dictionary_offset >= Header.word_dictionary_offset + 4 + idx_crc_enabled * 4);
  assert (Header.notword_dictionary_offset <= Header.word_dictionary_offset + MAX_FILE_DICTIONARY_BYTES);
  assert (Header.user_list_offset >= Header.notword_dictionary_offset + 4 + idx_crc_enabled * 4);
  assert (Header.user_list_offset <= Header.notword_dictionary_offset + MAX_FILE_DICTIONARY_BYTES);
//  fprintf (stderr, "user_list_offset=%lld, tot_users=%d, userlist_entry_size=%d, user_data_offset=%lld\n");
  assert (Header.user_list_offset + Header.tot_users * userlist_entry_size + 16 + idx_crc_enabled * 4 <= Header.user_data_offset);
  assert (Header.user_data_offset <= Header.extra_data_offset);
  assert (Header.extra_data_offset <= Header.data_end_offset);
  assert (Header.data_end_offset == fsize);

  if (Header.min_legacy_id > 0) {
    assert (Header.min_legacy_id == 0x7fffffff);
    index_long_legacy_id = 1;
  }

  if (Header.max_legacy_id < 0) {
    assert (Header.min_legacy_id == -0x80000000);
    index_long_legacy_id = 1;
  }


  idx_fsize = fsize;
  idx_fd = fd;
  idx_loaded_bytes = 0;
  idx_users = Header.tot_users;

  load_index_part (idx_Sublists_packed, Header.sublists_descr_offset, Header.sublists_num * 4, 0);

  for (i = 0; i < Header.sublists_num; i++) {
    idx_Sublists[i].xor_mask = idx_Sublists_packed[i].xor_mask;
    idx_Sublists[i].and_mask = idx_Sublists_packed[i].and_mask;
    assert (idx_Sublists[i].xor_mask == Sublists[i].xor_mask);
    assert (idx_Sublists[i].and_mask == Sublists[i].and_mask);
  }

  assert (Header.peer_list_mask.xor_mask == PeerFlagFilter.xor_mask && Header.peer_list_mask.and_mask == PeerFlagFilter.and_mask);

  load_char_dictionary (&WordCharDict, Header.word_char_dictionary_offset);
  load_char_dictionary (&NotWordCharDict, Header.notword_char_dictionary_offset);

  load_dictionary (&WordDict, Header.word_dictionary_offset, Header.notword_dictionary_offset - Header.word_dictionary_offset - idx_crc_enabled * 4);
  load_dictionary (&NotWordDict, Header.notword_dictionary_offset, Header.user_list_offset - Header.notword_dictionary_offset - idx_crc_enabled * 4);

  user_list_metafile = load_index_part (0, Header.user_list_offset, Header.tot_users * userlist_entry_size + 16, MAX_USERLIST_BYTES);
  FileUsers = zmalloc (sizeof (void *) * (Header.tot_users + 1));

  for (i = 0; i <= Header.tot_users; i++) {
    FileUsers[i] = (struct file_user_list_entry *) (user_list_metafile + i * userlist_entry_size);
  }

  assert (FileUsers[0]->user_data_offset >= Header.user_data_offset);
  for (i = 0; i < Header.tot_users; i++) {
    int min_ts = 1, max_ts = 0;
    if (idx_persistent_history_enabled) {
      min_ts = ((struct file_user_list_entry_search_history *) FileUsers[i])->user_history_min_ts;
      max_ts = ((struct file_user_list_entry_search_history *) FileUsers[i])->user_history_max_ts;
      assert (min_ts > 0 && max_ts >= min_ts - 1 && max_ts <= 0xffffff0);
    }
    assert (FileUsers[i]->user_data_size >= sizeof (struct file_user_header));
    assert (FileUsers[i]->user_data_offset + FileUsers[i]->user_data_size + (idx_search_enabled ? ((struct file_user_list_entry_search *) FileUsers[i])->user_search_size : 0) + (max_ts >= min_ts ? (max_ts - min_ts + 1) * 8 + 16 : 0) <= FileUsers[i+1]->user_data_offset);
    assert (FileUsers[i]->user_id < FileUsers[i+1]->user_id);
    lm_heap_insert (FileUsers[i]->user_data_size, FileUsers[i]->user_id, FileUsers[i]->user_last_local_id);
  }
  assert (FileUsers[i]->user_data_offset <= Header.extra_data_offset);

  if (verbosity > 0) {
    fprintf (stderr, "finished loading index: %lld index bytes, %d preloaded bytes\n", idx_fsize, idx_loaded_bytes);
  }

  log_split_min = Header.log_split_min;
  log_split_max = Header.log_split_max;
  log_split_mod = Header.log_split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  init_extra_mask (Header.extra_fields_mask);

  log_schema = TEXT_SCHEMA_V1;
  init_text_data (log_schema);

  replay_logevent = text_replay_logevent;

  jump_log_ts = Header.log_timestamp;
  jump_log_pos = Header.log_pos1;
  jump_log_crc32 = Header.log_pos1_crc32;

  return 0;
}

void output_large_metafiles (void) {
  int i;
  for (i = 1; i <= LN; i++) {
    if (LX[i]) {
      printf ("%d\t%d\t%d\n", LX[i], LY[i], LZ[i]);
    }
  }
}

struct file_user_list_entry *lookup_user_directory (long long user_id) {
  int i = -1, j = Header.tot_users, k;
  if (j <= 0) {
    return 0;
  }
  while (j - i > 1) {
    k = ((i + j) >> 1);
    if (user_id < FileUsers[k]->user_id) { j = k; } else { i = k; }
  }
  if (i >= 0 && FileUsers[i]->user_id == user_id) {
    return FileUsers[i];
  }
  return 0;
}

int cur_user_metafiles, tot_user_metafiles;
int cur_search_metafiles, tot_search_metafiles;
int cur_history_metafiles, tot_history_metafiles;
long long cur_user_metafile_bytes, tot_user_metafile_bytes, allocated_metafile_bytes;
long long cur_search_metafile_bytes, tot_search_metafile_bytes, allocated_search_metafile_bytes;
long long cur_history_metafile_bytes, tot_history_metafile_bytes, allocated_history_metafile_bytes;
long long metafile_alloc_threshold = METAFILE_ALLOC_THRESHOLD;

void bind_user_metafile (user_t *U);
void unbind_user_metafile (user_t *U);

struct metafile_queue MQ = {.first = (core_mf_t *) &MQ, .last = (core_mf_t *) &MQ};
struct metafile_queue MRQ = {.first = (core_mf_t *) &MRQ, .last = (core_mf_t *) &MRQ};

inline core_mf_t *touch_metafile (core_mf_t *M) {
  if (!M || M->aio) {
    return 0;
  }
  assert (M->next);

  M->prev->next = M->next;
  M->next->prev = M->prev;
  MQ.last->next = M;
  M->prev = MQ.last;
  M->next = (core_mf_t *) &MQ;
  MQ.last = M;

  return M;
}


inline char *get_search_metafile (user_t *U) {
  core_mf_t *M = touch_metafile (U->search_mf);
  return M ? M->data : 0;
}

inline char *get_history_metafile (user_t *U) {
  core_mf_t *M = touch_metafile (U->history_mf);
  return M ? M->data : 0;
}

inline char *get_user_metafile (user_t *U) {
  core_mf_t *M = touch_metafile (U->mf);
  return M ? M->data : 0;
}

core_mf_t *alloc_metafile (long metafile_len) {
  long long keep_allocated_metafile_bytes = allocated_metafile_bytes;
  long long keep_allocated_search_metafile_bytes = allocated_search_metafile_bytes;
  long long keep_allocated_history_metafile_bytes = allocated_history_metafile_bytes;
  core_mf_t *M;
  int unloaded_metafiles = 0;

  if (metafile_len > metafile_alloc_threshold * 4 / 5) {
    fprintf (stderr, "metafile too large - cannot allocate %ld bytes (%lld currently used)\n", metafile_len, allocated_metafile_bytes);
    return 0;
  }

  while (1) {

    if (allocated_metafile_bytes + allocated_search_metafile_bytes + allocated_history_metafile_bytes + metafile_len <= metafile_alloc_threshold) {
      M = malloc (metafile_len + offsetof (struct core_metafile, data) + 8);
      if (M) {
        break;
      }
    }

    if (MQ.first == MQ.last) {
      fprintf (stderr, "no space to load metafile - cannot allocate %ld bytes (%lld+%lld+%lld currently used)\n", metafile_len, allocated_metafile_bytes, allocated_search_metafile_bytes, allocated_history_metafile_bytes);
      return 0;
    }
    unloaded_metafiles++;
    assert (unload_generic_metafile (MQ.first) == 1);
  }

  memset (M, 0, offsetof (struct core_metafile, data));

  M->len = metafile_len;

  if (verbosity > 0 && unloaded_metafiles) {
    fprintf (stderr, "!NOTICE! had to unload %d metafiles to allocate %ld bytes, was %lld+%lld+%lld, now %lld+%lld+%lld\n", unloaded_metafiles, metafile_len, keep_allocated_metafile_bytes, keep_allocated_search_metafile_bytes, keep_allocated_history_metafile_bytes, allocated_metafile_bytes, allocated_search_metafile_bytes, allocated_history_metafile_bytes);
  }

  return M;

}

core_mf_t *load_search_metafile (long long user_id) {
  user_t *U = get_user_f (user_id);
  struct file_user_list_entry *D;

  assert (U);
  assert (U->dir_entry);
  assert (idx_search_enabled);

  //WaitAio2 = 0;

  if (U->search_mf && !U->search_mf->aio) {
    return touch_metafile (U->search_mf);
  }

  if (U->search_mf && U->search_mf->aio) {
    check_aio_completion (U->search_mf->aio);
    if (U->search_mf && U->search_mf->aio) {
      WaitAioArrAdd (U->search_mf->aio);
      return 0;
    }
    if (U->search_mf) {
      return touch_metafile (U->search_mf);
    } else {
      fprintf (stderr, "Previous AIO query failed for search metafile of user %d, scheduling a new one\n", U->user_id);
    }
  }

  D = U->dir_entry;

  long long pos = D->user_data_offset + D->user_data_size + idx_crc_enabled * 4;
  int search_metafile_len = ((struct file_user_list_entry_search *) D)->user_search_size;

  assert (search_metafile_len >= sizeof (struct file_search_header));
  search_metafile_len += idx_crc_enabled * 4;

  assert (!U->search_mf);

  core_mf_t *M = U->search_mf = alloc_metafile (search_metafile_len);
  if (!M) {
    return 0;
  }

  allocated_search_metafile_bytes += search_metafile_len;

  M->user = U;
  M->mf_type = MF_SEARCH;

  if (verbosity > 0) {
    fprintf (stderr, "*** Scheduled reading user %d search data from index at position %lld, %d bytes\n", U->user_id, pos, search_metafile_len);
  }

  if (!use_aio) {
    static struct aio_connection empty_aio_conn;

    assert (lseek (idx_fd, pos, SEEK_SET) == pos);
    int size = search_metafile_len;
    int r = read (idx_fd, M->data, size);
    if (r < size) {
      fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %lld: %m\n", r, size, pos);
      assert (r == size);
      free (M);
      U->search_mf = 0;
      return 0;
    }

    empty_aio_conn.extra = M;
    empty_aio_conn.basic_type = ct_aio;
    M->aio = &empty_aio_conn;

    onload_search_metafile ((struct connection *)(&empty_aio_conn), search_metafile_len);

    return U->search_mf;
  } else {
    M->aio = create_aio_read_connection (idx_fd, M->data, pos, search_metafile_len, &ct_metafile_search_aio, M);
    assert (M->aio);
    //WaitAio2 = M->aio;
    WaitAioArrAdd (M->aio);
  }

  assert (!M->next);

  MRQ.last->next = M;
  M->prev = MRQ.last;
  M->next = (core_mf_t *) &MRQ;
  MRQ.last = M;

  return 0;
}

core_mf_t *load_history_metafile (long long user_id) {
  user_t *U = get_user_f (user_id);
  struct file_user_list_entry_search_history *D;

  assert (U);
  assert (U->dir_entry);
  assert (idx_persistent_history_enabled);

  //WaitAio3 = 0;

  if (U->history_mf && !U->history_mf->aio) {
    return touch_metafile (U->history_mf);
  }

  if (U->history_mf && U->history_mf->aio) {
    check_aio_completion (U->history_mf->aio);
    if (U->history_mf && U->history_mf->aio) {
      WaitAioArrAdd (U->history_mf->aio);
      return 0;
    }
    if (U->history_mf) {
      return touch_metafile (U->history_mf);
    } else {
      fprintf (stderr, "Previous AIO query failed for history metafile of user %d, scheduling a new one\n", U->user_id);
    }
  }

  D = (struct file_user_list_entry_search_history *) U->dir_entry;

  long long pos = D->user_data_offset + D->user_data_size + 4 + (D->user_search_size ? D->user_search_size + 4 : 0);
  int history_metafile_len = (D->user_history_max_ts - D->user_history_min_ts + 1) * 8 + 16;

  assert (history_metafile_len >= sizeof (struct file_history_header));

  assert (!U->history_mf);

  core_mf_t *M = U->history_mf = alloc_metafile (history_metafile_len);
  if (!M) {
    return 0;
  }

  allocated_history_metafile_bytes += history_metafile_len;

  M->user = U;
  M->mf_type = MF_HISTORY;

  if (verbosity > 0) {
    fprintf (stderr, "*** Scheduled reading user %d history data from index at position %lld, %d bytes\n", U->user_id, pos, history_metafile_len);
  }

  if (!use_aio) {
    static struct aio_connection empty_aio_conn;

    assert (lseek (idx_fd, pos, SEEK_SET) == pos);
    int size = history_metafile_len;
    int r = read (idx_fd, M->data, size);
    if (r < size) {
      fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %lld: %m\n", r, size, pos);
      assert (r == size);
      free (M);
      U->history_mf = 0;
      return 0;
    }

    empty_aio_conn.extra = M;
    empty_aio_conn.basic_type = ct_aio;
    M->aio = &empty_aio_conn;

    onload_history_metafile ((struct connection *)(&empty_aio_conn), history_metafile_len);

    return U->history_mf;
  } else {
    M->aio = create_aio_read_connection (idx_fd, M->data, pos, history_metafile_len, &ct_metafile_history_aio, M);
    assert (M->aio);
    //WaitAio3 = M->aio;
    WaitAioArrAdd (M->aio);
  }

  assert (!M->next);

  MRQ.last->next = M;
  M->prev = MRQ.last;
  M->next = (core_mf_t *) &MRQ;
  MRQ.last = M;

  return 0;
}


core_mf_t *load_user_metafile (long long user_id) {
  user_t *U = get_user_f (user_id);
  struct file_user_list_entry *D;

  assert (U);
  assert (U->dir_entry);

  //WaitAio = 0;

  if (U->mf && !U->mf->aio) {
    if (verbosity > 0) {
      fprintf (stderr, "*** Reading user %d data from index not needed: metafile already loaded\n", U->user_id);
    }
    return touch_metafile (U->mf);
  }


  if (U->mf && U->mf->aio) {
    check_aio_completion (U->mf->aio);
    if (U->mf && U->mf->aio) {
      //WaitAio = U->mf->aio;
      WaitAioArrAdd (U->mf->aio);
      if (verbosity > 0) {
        fprintf (stderr, "*** Reading user %d data from index aalready scheduled\n", U->user_id);
      }
      return 0;
    }
    if (U->mf) {
      if (verbosity > 0) {
        fprintf (stderr, "*** Luck. aio for user %d just completed\n", U->user_id);
      }
      return touch_metafile (U->mf);
    } else {
      fprintf (stderr, "Previous AIO query failed for user %d, scheduling a new one\n", U->user_id);
    }
  }

  D = U->dir_entry;

  int metafile_len = D->user_data_size;
  assert (metafile_len >= sizeof (struct file_user_header));
  metafile_len += idx_crc_enabled * 4;

  assert (!U->mf);

  core_mf_t *M = U->mf = alloc_metafile (metafile_len);
  if (!M) {
    return 0;
  }

  allocated_metafile_bytes += metafile_len;

  M->user = U;
  M->mf_type = MF_USER;

  if (verbosity > 0) {
    fprintf (stderr, "*** Scheduled reading user %d data from index at position %lld, %d bytes\n", U->user_id, D->user_data_offset, metafile_len);
  }

  if (!use_aio) {
    static struct aio_connection empty_aio_conn;

    assert (lseek (idx_fd, D->user_data_offset, SEEK_SET) == D->user_data_offset);
    int size = metafile_len;
    int r = read (idx_fd, M->data, size);
    if (r < size) {
      fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %lld: %m\n", r, size, D->user_data_offset);
      assert (r == size);
      free (M);
      U->mf = 0;
      return 0;
    }

    empty_aio_conn.extra = M;
    empty_aio_conn.basic_type = ct_aio;
    M->aio = &empty_aio_conn;

    onload_user_metafile ((struct connection *)(&empty_aio_conn), metafile_len);

    return U->mf;
  } else {
    M->aio = create_aio_read_connection (idx_fd, M->data, D->user_data_offset, metafile_len, &ct_metafile_user_aio, M);
    assert (M->aio);
    //WaitAio = M->aio;
    WaitAioArrAdd (M->aio);
  }

  assert (!M->next);

  MRQ.last->next = M;
  M->prev = MRQ.last;
  M->next = (core_mf_t *) &MRQ;
  MRQ.last = M;

  return 0;
}

int onload_search_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 0) {
    fprintf (stderr, "onload_search_metafile(%p,%d)\n", c, read_bytes);
  }
  assert (idx_search_enabled);

  struct aio_connection *a = (struct aio_connection *)c;
  core_mf_t *M = (core_mf_t *) a->extra;
  user_t *U = M->user;

  assert (a->basic_type == ct_aio);
  assert (M->mf_type == MF_SEARCH);
  assert (U->search_mf == M);
  assert (M->aio == a);

  struct file_user_list_entry_search *D = (struct file_user_list_entry_search *) U->dir_entry;

  unsigned data_crc32, disk_crc32;
  if (idx_crc_enabled && read_bytes == M->len && read_bytes < metafile_crc_check_size_threshold) {
    assert (read_bytes >= 4);
    disk_crc32 = *((unsigned *) (M->data + read_bytes - 4));
    data_crc32 = compute_crc32 (M->data, read_bytes - 4);
  } else {
    disk_crc32 = data_crc32 = 0;
  }

  assert (M->len == D->user_search_size + idx_crc_enabled * 4);
  if (read_bytes != M->len || disk_crc32 != data_crc32) {
    aio_read_errors++;
    if (verbosity >= 0) {
      fprintf (stderr, "ERROR reading user %d search data from index at position %lld [pending aio queries: %lld]: read %d bytes out of %d: %m\n", U->user_id, D->user_data_offset + D->user_data_size, active_aio_queries, read_bytes, M->len);
    }
    if (disk_crc32 != data_crc32) {
      aio_crc_errors++;
      if (verbosity >= 0) {
	fprintf (stderr, "CRC mismatch: expected %08x, found %08x\n", disk_crc32, data_crc32);
      }
      assert (disk_crc32 == data_crc32);
    }

    allocated_search_metafile_bytes -= M->len;
    M->next->prev = M->prev;
    M->prev->next = M->next;
    M->prev = M->next = 0;
    M->aio = 0;

    free (M);
    U->search_mf = 0;

    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "*** Read user %d search data from index at position %lld: read %d bytes\n", U->user_id, D->user_data_offset + D->user_data_size, read_bytes);
  }

  M->aio = 0;

  struct file_search_header *H = (struct file_search_header *) get_search_metafile (U);

  assert (H->magic == FILE_USER_SEARCH_MAGIC);
  assert (H->words_num > 0);
  assert (H->words_num <= (read_bytes >> 2) - 2);

  cur_search_metafile_bytes += read_bytes;
  tot_search_metafile_bytes += read_bytes;
  cur_search_metafiles++;
  tot_search_metafiles++;

  if (U->user_id == 92226304) {
    // write (1, M->data, M->len);
    // exit (0);
  }

  return 1;
}

int onload_history_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 0) {
    fprintf (stderr, "onload_history_metafile(%p,%d)\n", c, read_bytes);
  }
  assert (idx_persistent_history_enabled);

  struct aio_connection *a = (struct aio_connection *)c;
  core_mf_t *M = (core_mf_t *) a->extra;
  user_t *U = M->user;

  assert (a->basic_type == ct_aio);
  assert (M->mf_type == MF_HISTORY);
  assert (U->history_mf == M);
  assert (M->aio == a);

  struct file_user_list_entry_search_history *D = (struct file_user_list_entry_search_history *) U->dir_entry;

  unsigned data_crc32, disk_crc32;
  if (read_bytes == M->len && read_bytes < metafile_crc_check_size_threshold) {
    assert (read_bytes >= 4);
    disk_crc32 = *((unsigned *) (M->data + read_bytes - 4));
    data_crc32 = compute_crc32 (M->data, read_bytes - 4);
  } else {
    disk_crc32 = data_crc32 = 0;
  }

  int history_metafile_len = (D->user_history_max_ts - D->user_history_min_ts + 1) * 8 + 12;

  assert (M->len == history_metafile_len + 4);
  if (read_bytes != M->len || disk_crc32 != data_crc32) {
    aio_read_errors++;
    if (verbosity >= 0) {
      fprintf (stderr, "ERROR reading user %d history data from index at position %lld [pending aio queries: %lld]: read %d bytes out of %d: %m\n", U->user_id, D->user_data_offset + D->user_data_size, active_aio_queries, read_bytes, M->len);
    }
    if (disk_crc32 != data_crc32) {
      aio_crc_errors++;
      if (verbosity >= 0) {
	fprintf (stderr, "CRC mismatch: expected %08x, found %08x\n", disk_crc32, data_crc32);
      }
      assert (disk_crc32 == data_crc32);
    }

    allocated_history_metafile_bytes -= M->len;
    M->next->prev = M->prev;
    M->prev->next = M->next;
    M->prev = M->next = 0;
    M->aio = 0;

    free (M);
    U->history_mf = 0;

    return 0;
  }

  if (verbosity > 0) {
    fprintf (stderr, "*** Read user %d history data from index at position %lld: read %d bytes\n", U->user_id, D->user_data_offset + D->user_data_size, read_bytes);
  }

  M->aio = 0;

  struct file_history_header *H = (struct file_history_header *) get_history_metafile (U);

  assert (H->magic == FILE_USER_HISTORY_MAGIC);
  assert (H->history_min_ts == D->user_history_min_ts);
  assert (H->history_max_ts == D->user_history_max_ts);
  assert (H->history_min_ts > 0 && H->history_max_ts >= H->history_min_ts - 1);

  cur_history_metafile_bytes += read_bytes;
  tot_history_metafile_bytes += read_bytes;
  cur_history_metafiles++;
  tot_history_metafiles++;

  if (U->user_id == 92226304) {
    // write (1, M->data, M->len);
    // exit (0);
  }

  return 1;
}



int onload_user_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 0) {
    fprintf (stderr, "onload_user_metafile(%p,%d)\n", c, read_bytes);
  }
  struct aio_connection *a = (struct aio_connection *)c;
  core_mf_t *M = (core_mf_t *) a->extra;
  user_t *U = M->user;

  assert (a->basic_type == ct_aio);
  assert (M->mf_type == MF_USER);
  assert (U->mf == M);
  assert (M->aio == a);

  struct file_user_list_entry *D = U->dir_entry;
  struct file_user_header *H;

  unsigned data_crc32, disk_crc32;
  if (idx_crc_enabled && read_bytes == M->len && read_bytes < metafile_crc_check_size_threshold) {
    assert (read_bytes >= 4);
    disk_crc32 = *((unsigned *) (M->data + read_bytes - 4));
    data_crc32 = compute_crc32 (M->data, read_bytes - 4);
  } else {
    disk_crc32 = data_crc32 = 0;
  }

  if (read_bytes < M->len || disk_crc32 != data_crc32) {
    aio_read_errors++;
    if (verbosity >= 0) {
      fprintf (stderr, "ERROR reading user %d data from index at position %lld [pending aio queries: %lld]: read %d bytes out of %d: %m\n", U->user_id, D->user_data_offset, active_aio_queries, read_bytes, M->len);
    }
    if (disk_crc32 != data_crc32) {
      aio_crc_errors++;
      if (verbosity >= 0) {
	fprintf (stderr, "CRC mismatch: expected %08x, found %08x\n", disk_crc32, data_crc32);
      }
      assert (disk_crc32 == data_crc32);
    }

    allocated_metafile_bytes -= M->len;
    M->next->prev = M->prev;
    M->prev->next = M->next;
    M->prev = M->next = 0;
    M->aio = 0;

    free (M);
    U->mf = 0;

    return 0;
  }

  M->aio = 0;

  H = (struct file_user_header *) (touch_metafile (M)->data);

  if (verbosity > 0 || H->magic != FILE_USER_MAGIC) {
    fprintf (stderr, "*** Read user %d data from index at position %lld: read %d bytes at address %p, magic %08x\n", U->user_id, D->user_data_offset, read_bytes, H, H->magic);
  }

  assert (H->magic == FILE_USER_MAGIC);
  assert (H->user_first_local_id >= 1 && H->user_last_local_id >= H->user_first_local_id - 1);
  assert (H->user_last_local_id == D->user_last_local_id);
  int r = H->user_last_local_id - H->user_first_local_id + 1;
  assert (H->user_id == U->user_id);
  assert (H->sublists_num == Header.sublists_num);
  assert (H->peers_offset == sizeof (struct file_user_header));
  assert ((unsigned) H->peers_num <= (1 << 24));
  assert (H->sublists_offset >= H->peers_offset + (H->peers_num ? H->peers_num * 8 + 4 : 0));
  assert (H->legacy_list_offset >= H->sublists_offset + (H->sublists_num ? H->sublists_num * 8 + 4 : 0));
  assert (H->directory_offset >= H->legacy_list_offset);
  assert (H->data_offset == H->directory_offset + 4*(r+1));
  assert (H->extra_offset >= H->data_offset);

  int search_bytes = (idx_search_enabled || idx_persistent_history_enabled) ? ((struct file_user_list_entry_search *) D)->user_search_size : 0;
  if (search_bytes && idx_crc_enabled) {
    search_bytes += 4;
  }

  int min_ts = 1, max_ts = 0, history_bytes = 0;
  if (idx_persistent_history_enabled) {
    min_ts = ((struct file_user_list_entry_search_history *) D)->user_history_min_ts;
    max_ts = ((struct file_user_list_entry_search_history *) D)->user_history_max_ts;
    if (max_ts >= min_ts) {
      history_bytes = (max_ts - min_ts + 1) * 8 + 16;
    }
  }

  assert (H->total_bytes == H->extra_offset + search_bytes + history_bytes + r * sizeof (struct file_message_extras) + 4 * idx_crc_enabled);
  assert (H->extra_offset == M->len);

  cur_user_metafile_bytes += M->len;
  tot_user_metafile_bytes += M->len;
  cur_user_metafiles++;
  tot_user_metafiles++;

  bind_user_metafile (U);

  return 1;
}

int unload_search_metafile (long long user_id) {
  user_t *U = get_user (user_id);

  if (verbosity > 1) {
    fprintf (stderr, "unload_search_metafile(%lld)\n", user_id);
  }

  if (!U || !U->search_mf || U->search_mf->aio) {
    if (verbosity > 1) {
      fprintf (stderr, "cannot unload search metafile (%lld)\n", user_id);
    }
    return 0;
  }

  core_mf_t *M = U->search_mf;

  assert (M->next);
  M->next->prev = M->prev;
  M->prev->next = M->next;
  M->next = M->prev = 0;

  allocated_search_metafile_bytes -= M->len;
  cur_search_metafile_bytes -= M->len;
  cur_search_metafiles--;

  free (M);
  U->search_mf = 0;

  if (verbosity > 1) {
    fprintf (stderr, "unload_search_metafile(%lld) END\n", user_id);
  }

  return 1;
}

int unload_history_metafile (long long user_id) {
  user_t *U = get_user (user_id);

  if (verbosity > 1) {
    fprintf (stderr, "unload_history_metafile(%lld)\n", user_id);
  }

  if (!U || !U->history_mf || U->history_mf->aio) {
    if (verbosity > 1) {
      fprintf (stderr, "cannot unload history metafile (%lld)\n", user_id);
    }
    return 0;
  }

  core_mf_t *M = U->history_mf;

  assert (M->next);
  M->next->prev = M->prev;
  M->prev->next = M->next;
  M->next = M->prev = 0;

  allocated_history_metafile_bytes -= M->len;
  cur_history_metafile_bytes -= M->len;
  cur_history_metafiles--;

  free (M);
  U->history_mf = 0;

  if (verbosity > 1) {
    fprintf (stderr, "unload_history_metafile(%lld) END\n", user_id);
  }

  return 1;
}

int unload_user_metafile (long long user_id) {
  user_t *U = get_user (user_id);

  if (verbosity > 1) {
    fprintf (stderr, "unload_user_metafile(%lld)\n", user_id);
  }

  if (!U || !U->mf || U->mf->aio) {
    if (verbosity > 1) {
      fprintf (stderr, "cannot unload user metafile (%lld)\n", user_id);
    }
    return 0;
  }


  unbind_user_metafile (U);

  if (verbosity > 1) {
    fprintf (stderr, "after unbind_user_metafile (%p)\n", U);
  }

  core_mf_t *M = U->mf;

  assert (M->next);
  M->next->prev = M->prev;
  M->prev->next = M->next;
  M->next = M->prev = 0;

  allocated_metafile_bytes -= M->len;
  cur_user_metafile_bytes -= M->len;
  cur_user_metafiles--;

  free (M);
  U->mf = 0;

  if (verbosity > 1) {
    fprintf (stderr, "unload_user_metafile(%lld) END\n", user_id);
  }

  return 1;
}

int unload_generic_metafile (core_mf_t *M) {
  assert (M);
  switch (M->mf_type) {
  case MF_USER:
    assert (M->user->mf == M);
    return unload_user_metafile (M->user->user_id);
  case MF_SEARCH:
    assert (M->user->search_mf == M);
    return unload_search_metafile (M->user->user_id);
  case MF_HISTORY:
    assert (M->user->history_mf == M);
    return unload_history_metafile (M->user->user_id);
  default:
    assert (M->mf_type == MF_USER || M->mf_type == MF_SEARCH || M->mf_type == MF_HISTORY);
  }
  return -1;
}


int check_user_metafile (long long user_id, int *R) {
  user_t *U = get_user (user_id);

  if (!U || !U->mf || U->mf->aio) {
    return 0;
  }

  if (R) {
    R[0] = U->mf->len;
  }

  return 1;
}

int *fetch_file_peer_list (char *metafile, int peer_id, int *N) {
  struct file_user_header *H = (struct file_user_header *) metafile;
  assert (metafile);
  int *L = (int *) (metafile + H->peers_offset);
  int a, b, c;
   if (N) {
    *N = 0;
  }
  if (!H->peers_num) {
    return 0;
  }
  a = -1;
  b = H->peers_num;
  //fprintf (stderr, "%d peers: %d %d %d %d %d %d ...\n", b, L[1], L[3], L[5], L[7], L[9], L[11]);
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (L[c*2+1] <= peer_id) { a = c; } else { b = c; }
  }
  if (a < 0 || L[a*2+1] != peer_id) {
    return 0;
  }
  if (N) {
    *N = ((L[a*2+2] - L[a*2]) >> 2);
  }
  assert (L[a*2] >= 0 && L[a*2] <= L[a*2+2] && L[a*2+2] <= H->extra_offset);
  return (int *) (metafile + L[a*2]);
}

/* ---------- tree functions ------------ */

static inline tree_t *new_tree_node (int x, int y, void *data) {
  tree_t *P;
  P = zmalloc (sizeof (tree_t));
  assert (P);
  P->left = P->right = 0;
  P->x = x;
  P->y = y;
  P->data = data;
  ++tree_nodes;
  return P;
}

static inline ltree_t *new_ltree_node (long long x, int y, int z) {
  ltree_t *P;
  P = zmalloc (sizeof (ltree_t));
  assert (P);
  P->left = P->right = 0;
  P->x = x;
  P->y = y;
  P->z = z;
  ++tree_nodes;
  return P;
}

int tree_depth (tree_t *T, int d) {
  if (!T) { return d; }
  int u = tree_depth (T->left, d+1);
  int v = tree_depth (T->right, d+1);
  return (u > v ? u : v);
}

static void free_tree_node (tree_t *T) {
  zfree (T, sizeof (tree_t));
  --tree_nodes;
}

static void free_ltree_node (ltree_t *T) {
  zfree (T, sizeof (ltree_t));
  --tree_nodes;
}

static tree_t *tree_lookup (tree_t *T, int x) {
  while (T && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

static ltree_t *ltree_lookup_legacy (ltree_t *T, long long x) {
  while (T && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

static inline int legacy_tree_equal (ltree_t *T, long long x, int z) {
  return x == T->x && z == T->z; 
}


static inline int legacy_tree_less (long long x, int z, ltree_t *T) {
  return x < T->x || (x == T->x && z < T->z); 
}


static ltree_t *legacy_tree_lookup (ltree_t *T, long long x, int z) {
  while (T && !legacy_tree_equal (T, x, z)) {
    T = legacy_tree_less (x, z, T) ? T->left : T->right;
  }
  return T;
}

tree_t *NewestNode;

/* T[x' < x] -> L,  T[x' >= x] -> R */
/*
static void tree_split (tree_t **L, tree_t **R, tree_t *T, int x) {
  while (T) {
    if (x < T->x) {
      *R = T;
      R = &T->left;
      T = *R;
    } else {
      *L = T;
      L = &T->right;
      T = *L;
    }
  }
  *L = *R = 0;
}*/

static tree_t *tree_insert (tree_t *T, int x, int y, void *data) {
  tree_t *Root = T, **U = &Root, **L, **R;

  while (T && T->y >= y) {
    U = (x < T->x) ? &T->left : &T->right;
    T = *U;
  }

  *U = NewestNode = new_tree_node (x, y, data);


  L = &(*U)->left;
  R = &(*U)->right;

  while (T) {
    if (x < T->x) {
      *R = T;
      R = &T->left;
      T = *R;
    } else {
      *L = T;
      L = &T->right;
      T = *L;
    }
  }

  *L = *R = 0;

  return Root;
}


static ltree_t *legacy_tree_insert (ltree_t *T, long long x, int y, int z) {
  ltree_t *Root = T, **U = &Root, **L, **R;

  while (T && T->y >= y) {
    U = legacy_tree_less (x, z, T) ? &T->left : &T->right;
    T = *U;
  }

  *U = new_ltree_node (x, y, z);


  L = &(*U)->left;
  R = &(*U)->right;

  while (T) {
    if (legacy_tree_less (x, z, T)) {
      *R = T;
      R = &T->left;
      T = *R;
    } else {
      *L = T;
      L = &T->right;
      T = *L;
    }
  }

  *L = *R = 0;

  return Root;
}


static tree_t *tree_delete (tree_t *T, int x) {
  tree_t *Root = T, **U = &Root, *L, *R;

  while (T && x != T->x) {
    U = (x < T->x) ? &T->left : &T->right;
    T = *U;
  }

  if (!T) {
    return Root;
  }

  L = T->left;
  R = T->right;
  free_tree_node (T);

  while (L && R) {
    if (L->y > R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }

  *U = L ? L : R;

  return Root;
}

static ltree_t *legacy_tree_delete (ltree_t *T, long long x, int z) {
  ltree_t *Root = T, **U = &Root, *L, *R;

  while (T && !legacy_tree_equal (T, x, z)) {
    U = legacy_tree_less (x, z, T) ? &T->left : &T->right;
    T = *U;
  }

  if (!T) {
    return Root;
  }

  L = T->left;
  R = T->right;
  free_ltree_node (T);

  while (L && R) {
    if (L->y > R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }

  *U = L ? L : R;

  return Root;
}

/*
static tree_t *tree_merge (tree_t *L, tree_t *R) {
  tree_t *Root, **U = &Root;

  while (L && R) {
    if (L->y > R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }

  *U = L ? L : R;

  return Root;
}

static void free_tree (tree_t *T) {
  if (T) {
    free_tree (T->left);
    free_tree (T->right);
    free_tree_node (T);
  }
}
*/

/* stree = trees without extra data */

static inline stree_t *new_stree_node (int x, int y) {
  stree_t *P;
  P = zmalloc (sizeof (stree_t));
  assert (P);
  P->left = P->right = 0;
  P->x = x;
  P->y = y;
  ++online_tree_nodes;
  return P;
}

static inline void free_stree_node (stree_t *T) {
  --online_tree_nodes;
  zfree (T, sizeof (stree_t));
}

static inline stree_t *stree_lookup (stree_t *T, int x) {
  while (T && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

static stree_t *stree_insert (stree_t *T, int x, int y) {
  stree_t *Root = T, **U = &Root, **L, **R;

  while (T && T->y <= y) {
    U = (x < T->x) ? &T->left : &T->right;
    T = *U;
  }

  *U = new_stree_node (x, y);


  L = &(*U)->left;
  R = &(*U)->right;

  while (T) {
    if (x < T->x) {
      *R = T;
      R = &T->left;
      T = *R;
    } else {
      *L = T;
      L = &T->right;
      T = *L;
    }
  }

  *L = *R = 0;

  return Root;
}

static int minsert_flag;

static stree_t *stree_delete (stree_t *T, int x) {
  stree_t *Root = T, **U = &Root, *L, *R;

  while (T && x != T->x) {
    U = (x < T->x) ? &T->left : &T->right;
    T = *U;
  }

  if (!T) {
    minsert_flag = 0;
    return Root;
  }

  L = T->left;
  R = T->right;
  free_stree_node (T);

  while (L && R) {
    if (L->y < R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }

  *U = L ? L : R;

  minsert_flag = -1;
  return Root;
}

static stree_t *stree_merge (stree_t *L, stree_t *R) {
  stree_t *Root, **U = &Root;

  while (L && R) {
    if (L->y < R->y) {
      *U = L;
      U = &L->right;
      L = *U;
    } else {
      *U = R;
      U = &R->left;
      R = *U;
    }
  }

  *U = L ? L : R;

  return Root;
}

stree_t *FreedNodes;

/* kills nodes with y < min_y */
static stree_t *stree_prune (stree_t *T, int min_y) {
  if (!T || T->y >= min_y) {
    return T;
  }
  stree_t *R = stree_merge (stree_prune (T->left, min_y), stree_prune (T->right, min_y));
  T->left = FreedNodes;
  FreedNodes = T;
  return R;
}

/*
static void free_stree (stree_t *T) {
  if (T) {
    free_stree (T->left);
    free_stree (T->right);
    free_stree_node (T);
  }
}
*/


/* tree_num = trees with counters */

#define	NIL	((tree_num_t *) &NIL_NUM_NODE)
#define	NIL_N	((tree_num_t *) &NIL_NUM_NODE)
const static tree_num_t NIL_NUM_NODE = {.left = NIL, .right = NIL, .y = (-1 << 31) };

static inline tree_num_t *new_tree_num_node (int x, int y, int z) {
  tree_num_t *P;
  P = zmalloc (sizeof (tree_num_t));
  assert (P);
  P->left = P->right = NIL;
  P->x = x;
  P->y = y;
  P->z = z;
  return P;
}


static void free_tree_num_node (tree_num_t *T) {
  assert (T != NIL);
  zfree (T, sizeof (tree_num_t));
}


static inline tree_num_t *tree_num_lookup (tree_num_t *T, int x) {
  while (T != NIL && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

static inline void tree_num_relax (tree_num_t *T) {
  T->N = T->left->N + T->right->N + 1;
}

/* T[x' < x] -> L,  T[x' >= x] -> R */
static void tree_num_split (tree_num_t **L, tree_num_t **R, tree_num_t *T, int x) {
  if (T == NIL) { *L = *R = NIL; return; }
  if (T->x >= x) {
    *R = T;
    tree_num_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_num_split (&T->right, R, T->right, x);
  }
  tree_num_relax (T);
}

static tree_num_t *tree_num_insert (tree_num_t *T, int x, int y, int z) {
  tree_num_t *P;
  if (T->y > y) {
    if (x < T->x) {
      T->left = tree_num_insert (T->left, x, y, z);
    } else {
      T->right = tree_num_insert (T->right, x, y, z);
    }
    tree_num_relax (T);
    return T;
  }
  P = new_tree_num_node (x, y, z);
  tree_num_split (&P->left, &P->right, T, x);
  tree_num_relax (P);
  return P;
}

static tree_num_t *tree_num_merge (tree_num_t *L, tree_num_t *R) {
  if (L == NIL) { return R; }
  if (R == NIL) { return L; }
  if (L->y > R->y) {
    L->right = tree_num_merge (L->right, R);
    tree_num_relax (L);
    return L;
  } else {
    R->left = tree_num_merge (L, R->left);
    tree_num_relax (R);
    return R;
  }
}




static tree_num_t *tree_num_delete (tree_num_t *T, int x) {
  assert (T != NIL);
  if (T->x == x) {
    tree_num_t *N = tree_num_merge (T->left, T->right);
    free_tree_num_node (T);
    return N;
  }
  if (x < T->x) {
    T->left = tree_num_delete (T->left, x);
  } else {
    T->right = tree_num_delete (T->right, x);
  }
  tree_num_relax (T);
  return T;
}

static void free_tree_num (tree_num_t *T) {
  if (T != NIL) {
    free_tree_num (T->left);
    free_tree_num (T->right);
    free_tree_num_node (T);
  }
}

/* writes range [from..to], 1-based */
int *tree_num_get_range (tree_num_t *T, int *R, int from, int to) {
  if (T == NIL) {
    return R;
  }
  if (from <= T->left->N) {
    R = tree_num_get_range (T->left, R, from, to);
  }
  from -= T->left->N + 1;
  to -= T->left->N + 1;
  if (to >= 0 && from <= 0) {
    *R++ = T->x;
    *R++ = T->z;
  }
  if (to > 0) {
    R = tree_num_get_range (T->right, R, from, to);
  }
  return R;
}


int *tree_num_get_range_rev (tree_num_t *T, int *R, int from, int to) {
  if (T == NIL) {
    return R;
  }
  if (from <= T->right->N) {
    R = tree_num_get_range_rev (T->right, R, from, to);
  }
  from -= T->right->N + 1;
  to -= T->right->N + 1;
  if (to >= 0 && from <= 0) {
    *R++ = T->x;
    *R++ = T->z;
  }
  if (to > 0) {
    R = tree_num_get_range_rev (T->left, R, from, to);
  }
  return R;
}

#undef NIL

/* tree_ext = trees with deltas and counters */

#define	NIL	((tree_ext_t *) &NIL_NODE)
const static tree_ext_t NIL_NODE = {.left = NIL, .right = NIL, .y = (-1 << 31) };

static int y_to_delta[4] = {0, 1, 0, -1};	// sin (k*pi/2)

static inline tree_ext_t *new_tree_ext_node (int x, int y, int rpos) {
  tree_ext_t *P;
  P = zmalloc (sizeof (tree_ext_t));
  assert (P);
  P->left = P->right = NIL;
  P->x = x;
  P->y = y;
  P->rpos = rpos;
//  P->delta = y_to_delta[y & 3];
  return P;
}

static void free_tree_ext_node (tree_ext_t *T) {
  assert (T != NIL);
  zfree (T, sizeof (tree_ext_t));
}

static inline tree_ext_t *tree_ext_lookup (tree_ext_t *T, int x) {
  while (T != NIL && x != T->x) {
    T = (x < T->x) ? T->left : T->right;
  }
  return T;
}

static inline void tree_ext_relax (tree_ext_t *T) {
  T->delta = T->left->delta + T->right->delta + y_to_delta[T->y & 3];
}

static void tree_ext_split (tree_ext_t **L, tree_ext_t **R, tree_ext_t *T, int x) {
  if (T == NIL) { *L = *R = NIL; return; }
  if (x < T->x) {
    *R = T;
    tree_ext_split (L, &T->left, T->left, x);
  } else {
    *L = T;
    tree_ext_split (&T->right, R, T->right, x);
  }
  tree_ext_relax (T);
}

static tree_ext_t *tree_ext_insert (tree_ext_t *T, int x, int y, int rpos) {
  tree_ext_t *P;
  if (T->y > y) {
    if (x < T->x) {
      T->left = tree_ext_insert (T->left, x, y, rpos);
    } else {
      T->right = tree_ext_insert (T->right, x, y, rpos);
    }
    tree_ext_relax (T);
    return T;
  }
  P = new_tree_ext_node (x, y, rpos);
  tree_ext_split (&P->left, &P->right, T, x);
  tree_ext_relax (P);
  return P;
}

static tree_ext_t *tree_ext_merge (tree_ext_t *L, tree_ext_t *R) {
  if (L == NIL) { return R; }
  if (R == NIL) { return L; }
  if (L->y > R->y) {
    L->right = tree_ext_merge (L->right, R);
    tree_ext_relax (L);
    return L;
  } else {
    R->left = tree_ext_merge (L, R->left);
    tree_ext_relax (R);
    return R;
  }
}




static tree_ext_t *tree_ext_delete (tree_ext_t *T, int x) {
  assert (T != NIL);
  if (T->x == x) {
    tree_ext_t *N = tree_ext_merge (T->left, T->right);
    free_tree_ext_node (T);
    return N;
  }
  if (x < T->x) {
    T->left = tree_ext_delete (T->left, x);
  } else {
    T->right = tree_ext_delete (T->right, x);
  }
  tree_ext_relax (T);
  return T;
}

/*
static void free_tree_ext (tree_ext_t *T) {
  if (T) {
    free_tree_ext (T->left);
    free_tree_ext (T->right);
    free_tree_ext_node (T);
  }
}
*/

/* tree_and_list = (dynamic) tree + (static) list */

inline int listree_get_size (listree_t *U) {
  return U->N + U->root->delta;
}

/* returns kth value in (tree+list) in ascending order, k>=0
   if k >= (total size), returns -1
   if list is not present and is needed, returns -2
*/
int listree_get_kth (listree_t *U, int k) {
  tree_ext_t *T = U->root;
  if (k < 0 || k >= U->N + T->delta) {
    return -1;
  }
  int l = k;
  while (T != NIL) {
    /* T->lpos + T->left->delta = # of el. of joined list < T->x */
    if (l < U->N - T->rpos + T->left->delta) {
      T = T->left;
    } else if (l == U->N - T->rpos + T->left->delta && (T->y & 3) != TF_MINUS) {
      return T->x;
    } else {
      l -= T->left->delta + y_to_delta[T->y & 3];
      T = T->right;
    }      
    /* T->lpos + T->left->delta + (...) = # of el. of joined list <= T->x */
    /* ((T->y & 3) != TF_MINUS) == (T->x is in list)
       ((T->y & 3) != TF_MINUS) + y_to_delta[T->y & 3] == (T->x in joined list)
    */
    /* old version, somehow equivalent
    if (l >= T->lpos + T->left->delta + ((T->y & 3) == TF_MINUS ? 0 : 1)) {
//    if (l > T->lpos + T->left->delta + y_to_delta[T->y & 3]) {
//      l -= T->left->delta + ((T->y & 3) == TF_MINUS ? 0 : 1);
      l -= T->left->delta + y_to_delta[T->y & 3];
      T = T->right;
    } else {
      return T->x;
    }
    */
  }
  assert (l >= 0 && l < U->N);
  if (!U->A) {
    return -2;
  } else {
    return U->A[l];
  }
}

int listree_get_kth_last (listree_t *U, int k) {
  tree_ext_t *T = U->root;
//  if (k < 0) { return -1; }
  return listree_get_kth (U, U->N + T->delta - 1 - k);
}

int *RA, *RB;

/* returns elements a .. b (included) of merged list+tree */
int listree_get_range_rec (int *A, tree_ext_t *T, int N, int a, int b) {
  if (a > b) {
    return 1;
  }
  if (T == NIL) {
    assert (a <= b);
    if (!A) {
      return -2;
    }
    while (a <= b) {
      *RA++ = A[a++];
    }
    return 1;
  }
  int c = N - T->rpos + T->left->delta;			/* # of el. of joined list < T->x */
  int c1 = c + ((T->y & 3) == TF_MINUS ? 0 : 1);        /* # of el. of joined list <= T->x */
  int s = T->left->delta + y_to_delta[T->y & 3];
  if (b < c) {
    return listree_get_range_rec (A, T->left, N, a, b);
  }
  if (a >= c1) {
    return listree_get_range_rec (A, T->right, N, a - s, b - s);
  }
  if (listree_get_range_rec (A, T->left, N, a, c-1) < 0) {
    return -2;
  }
  /* now a < c1, b >= c, c <= c1 <= c+1 => a <= c, c1-1 <= b */
  if (c < c1) {
    *RA++ = T->x;
  }
  return listree_get_range_rec (A, T->right, N, c1 - s, b - s);
}

/* same as above, but write pointer is advanced downwards */
int listree_get_range_rec_rev (int *A, tree_ext_t *T, int N, int a, int b) {
  if (a > b) {
    return 1;
  }
  if (T == NIL) {
    assert (a <= b);
    if (!A) {
      return -2;
    }
    while (a <= b) {
      *--RA = A[a++];
    }
    return 1;
  }
  int c = N - T->rpos + T->left->delta;			/* # of el. of joined list < T->x */
  int c1 = c + ((T->y & 3) == TF_MINUS ? 0 : 1);        /* # of el. of joined list <= T->x */
  int s = T->left->delta + y_to_delta[T->y & 3];
  if (b < c) {
    return listree_get_range_rec_rev (A, T->left, N, a, b);
  }
  if (a >= c1) {
    return listree_get_range_rec_rev (A, T->right, N, a - s, b - s);
  }
  if (listree_get_range_rec_rev (A, T->left, N, a, c-1) < 0) {
    return -2;
  }
  /* now a < c1, b >= c, c <= c1 <= c+1 => a <= c, c1-1 <= b */
  if (c < c1) {
    *--RA = T->x;
  }
  return listree_get_range_rec_rev (A, T->right, N, c1 - s, b - s);
}



/* elements k ... k+n-1 of joined list, returns # of written elements */
int listree_get_range (listree_t *U, int k, int n) {
  tree_ext_t *T = U->root;
  int M = U->N + T->delta;
  n += k;
  if (k < 0) { 
    k = 0; 
  }
  if (n > M) {
    n = M;
  }
  if (n <= k) {
    return 0;
  }
  if (listree_get_range_rec (U->A, T, U->N, k, n - 1) < 0) {
    return -2;
  } else {
    return n - k;
  }
}

/* elements k ... k+n-1 of reversed joined list, returns # of written elements */
int listree_get_range_rev (listree_t *U, int k, int n) {
  tree_ext_t *T = U->root;
  int M = U->N + T->delta;
  n += k;
  if (k < 0) { 
    k = 0; 
  }
  if (n > M) {
    n = M;
  }
  if (n <= k) {
    return 0;
  }
  RA += n - k;
  if (listree_get_range_rec_rev (U->A, T, U->N, M - n, M - k - 1) < 0) {
    return -2;
  } else {
    RA += n - k;
    return n - k;
  }
}

/* returns position = (number of entries <= x) - 1 in given listree */
static int listree_get_pos (listree_t *U, int x, int inexact) {
  tree_ext_t *T = U->root;
  int G_N = U->N;
  int sd = 0, a = 0, b = G_N - 1;
  while (T != NIL) {
    int c = G_N - T->rpos;		// # of array elements < T->x, i.e. A[0]...A[c-1]
    /* T->lpos + T->left->delta = # of el. of joined list < T->x */
    int node_type = T->y & 3;
    if (x < T->x) {
      T = T->left;
      b = c - 1;  // left subtree corresponds to [a .. c-1]
    } else if (x == T->x) {
      assert (inexact || node_type != TF_MINUS);
      return c + sd + T->left->delta - (node_type == TF_MINUS);
    } else {
      a = c + (node_type != TF_PLUS);	// right subtree corresponds to [rl .. b]
      sd += T->left->delta + y_to_delta[node_type];
      T = T->right;
    }      
  }
  assert (a >= 0 && a <= b + inexact && b < G_N);
  b++;
  a--;
  if (!U->A && b - a > 1) {
    return -2;
  }
  while (b - a > 1) {
    int c = (a + b) >> 1;
    //vkprintf (, "(a = %d, b = %d, c = %d, U->A = %p\n");
    if (x < U->A[c]) {
      b = c;
    } else if (x > U->A[c]) {
      a = c;
    } else {
      c += sd;
      assert (c >= 0 && c < G_N + U->root->delta);
      return c;
    }
  }
  assert (inexact);
  return a + sd;
}


int find_rpos (listree_t *U, int local_id) {
  int l = -1, r = U->N, x;

  if (!U->A && local_id > U->last_A) {
    return 0;
  }
 
  if (!U->A && U->N) {
    return -2;
  }

  /* A[N-i-1]<x<=A[N-i] means A[l] < x <= A[r]; i = N - r */
  while (r - l > 1) {
    x = (l + r) / 2;
    if (local_id <= U->A[x]) {
      r = x;
    } else {
      l = x;
    }
  }
  assert (r >= 0 && r <= U->N);
  return U->N - r;
}


// deletes an existing node
void listree_delete (listree_t *U, int x) {
  int tp, rpos;
  tree_ext_t *T = tree_ext_lookup (U->root, x);

  if (T == NIL) {
    rpos = find_rpos (U, x);
    if (rpos <= 0 || U->A[U->N - rpos] != x) {
      fprintf (stderr, "listree_delete: T=%p rpos=%d U->A=%p U->N=%d U->last_A=%d U->root=%p U->A[N-rpos]=%d x=%d\n", T, rpos, U->A, U->N, U->last_A, U->root, rpos > 0 ? U->A[U->N - rpos] : -1, x);
      assert (rpos > 0 && U->A[U->N - rpos] == x);
    }
    U->root = tree_ext_insert (U->root, x, (lrand48 () << 2) + TF_MINUS, rpos);
  } else {
    tp = T->y & 3;
    rpos = T->rpos;
    assert (tp == TF_PLUS);
    U->root = tree_ext_delete (U->root, x);
//    if (tp != 1) {
//      U->root = tree_ext_insert (U->root, x, (lrand48 () << 2) + 3, rpos);
//    }
  }
}

// inserts a non-existing node
void listree_insert (listree_t *U, int x) {
  int tp, rpos;
  tree_ext_t *T = tree_ext_lookup (U->root, x);
  if (T == NIL) {
    rpos = find_rpos (U, x);
    assert (rpos >= 0);
    assert (!rpos || U->A[U->N - rpos] != x);
    U->root = tree_ext_insert (U->root, x, (lrand48 () << 2) + TF_PLUS, rpos);
  } else {
    tp = T->y & 3;
    rpos = T->rpos;
    assert (tp == TF_MINUS);
    U->root = tree_ext_delete (U->root, x);
//    if (tp != 3) {
//      U->root = tree_ext_insert (U->root, x, (lrand48 () << 2) + 1, rpos);
//    }
  }
}

/*
 *
 *		BIND/UNBIND METAFILE
 *
 */

int *rebuild_topmsg_tree (tree_t *T, int *L, int *LE, int max_peer_id, user_t *U, char *metafile) {
  int peer_id;
  listree_t X;
  if (T) {
    L = rebuild_topmsg_tree (T->left, L, LE, T->x - 1, U, metafile);
    peer_id = T->x;
    X.root = T->data;
    if (L < LE && L[1] == T->x) {
      X.N = ((L[2] - L[0]) >> 2);
      assert (L[0] >= 0 && L[0] <= L[2] && L[2] <= UserHdr->extra_offset);
      X.A = (int *) (metafile + L[0]);
      X.last_A = (X.N ? X.A[X.N-1] : 0);
      L += 2;
    } else {
      X.N = 0;
      X.A = 0;
      X.last_A = 0;
    }
    int local_id = listree_get_kth_last (&X, 0);
    if (local_id != -1) {
      assert (local_id > 0);
      U->topmsg_tree = tree_num_insert (U->topmsg_tree, local_id, lrand48(), peer_id);
    }
    L = rebuild_topmsg_tree (T->right, L, LE, max_peer_id, U, metafile);
  }
  while (L < LE && L[1] <= max_peer_id) {
    peer_id = L[1];
    X.root = NIL;
    X.N = ((L[2] - L[0]) >> 2);
    assert (L[0] >= 0 && L[0] <= L[2] && L[2] <= UserHdr->extra_offset);
    X.A = (int *) (metafile + L[0]);
    X.last_A = (X.N ? X.A[X.N-1] : 0);
    L += 2;
    int local_id = listree_get_kth_last (&X, 0);
    if (local_id != -1) {
      assert (local_id > 0);
      U->topmsg_tree = tree_num_insert (U->topmsg_tree, local_id, lrand48(), peer_id);
    }
  }
  return L;
}

void bind_user_metafile (user_t *U) {
  core_mf_t *M = U->mf;
  int i, *x;
  assert (M && !M->aio);
  char *metafile = M->data;
  assert (U->dir_entry);
  assert (UserHdr->sublists_num == sublists_num);

  if (verbosity > 2) {
    fprintf (stderr, "bind_user_metafile(%p) : user_id=%d mf=%p\n", U, U->user_id, metafile);
  }

  x = (int *) (metafile + UserHdr->sublists_offset);
  assert (!x[0]);
  for (i = 0; i < sublists_num; i++) {
    int *A = (int *) (metafile + UserHdr->sublists_offset + sublists_num * 8 + 4) + x[2*i];
    int N = x[2*i+2] - x[2*i];
    assert (x[2*i+1] == idx_Sublists_packed[i].combined_xor_and);
    assert (N >= 0);
    assert (N == U->Sublists[i].N);
    assert (!U->Sublists[i].A);
    U->Sublists[i].A = A;
    assert (!N || A[N-1] <= U->Sublists[i].last_A);
    U->Sublists[i].last_A = N ? A[N-1] : 0;
  }
  assert (UserHdr->sublists_offset + sublists_num * 8 + 4 + x[2*i] * 4 <= UserHdr->legacy_list_offset && UserHdr->legacy_list_offset <= M->len);

  assert (U->topmsg_tree == NIL_N);

  int *L = (int *) (metafile + UserHdr->peers_offset);
  int *LE = L + 2 * UserHdr->peers_num;

  assert (rebuild_topmsg_tree (U->peer_tree, L, LE, MAX_PEER_ID, U, metafile) == LE);

  if (U->delayed_tree) {
    /* perform delayed operations */
    process_delayed_ops (U, U->delayed_tree);
    U->delayed_tree = 0;
  }

  if (U->delayed_value_tree) {
    process_delayed_values (U, U->delayed_value_tree);
    U->delayed_value_tree = 0;
  }

}

void unbind_user_metafile (user_t *U) {
  int i;
  core_mf_t *M = U->mf;

  assert (M && !M->aio);
  char *metafile = M->data;

  if (verbosity > 2) {
    fprintf (stderr, "unbind_user_metafile(%p) : user_id=%d mf=%p\n", U, U->user_id, metafile);
  }

  for (i = 0; i < sublists_num; i++) {
    U->Sublists[i].A = 0;
  }

  free_tree_num (U->topmsg_tree);
  U->topmsg_tree = NIL_N;
}




/*
 *
 *		REPLAY LOG
 *
 */

struct message_extras LastMsgExtra[CYCLIC_IP_BUFFER_SIZE];
int first_extra_global_id = 1;

user_t *User[MAX_USERS + 1];
int min_uid = MAX_USERS, max_uid, tot_users;

static int text_le_start (struct lev_start *E) {
  int q = 0;
  if (E->schema_id != TEXT_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 == log_split_max && log_split_max <= log_split_mod);

  if (E->extra_bytes >= 3 && E->str[0] == 1) {
    init_extra_mask (*(unsigned short *) (E->str + 1));
    q = 3;
  }
  if (E->extra_bytes >= q + 6 && !memcmp (E->str + q, "status", 6)) {
    memcpy (&PeerFlagFilter, &Statuses_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Statuses_Sublists;
    count_sublists ();
  }
  if (E->extra_bytes >= q + 5 && !memcmp (E->str + q, "forum", 5)) {
    memcpy (&PeerFlagFilter, &Forum_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Forum_Sublists;
    count_sublists ();
  }
  if (E->extra_bytes >= q + 8 && !memcmp (E->str + q, "comments", 8)) {
    memcpy (&PeerFlagFilter, &Comments_PeerFlagFilter, sizeof (PeerFlagFilter));
    Sublists = Comments_Sublists;
    count_sublists ();
  }

  return 0;
}

int conv_uid (int user_id) {
#if 0
  int t = user_id;
  t %= log_split_mod;
  if (t != log_split_min && t != -log_split_min) {
    return -1;
  }
  int h1 = user_id % USERS_PRIME;
  int h2 = (user_id * 17 + 239) % (USERS_PRIME - 1);
  if (h1 < 0) {
    h1 += USERS_PRIME;
  }
  if (h2 < 0) {
    h2 += USERS_PRIME - 1;
  }
  ++h2;
  while (User[h1]) {
    if (User[h1]->user_id == user_id) {
      return h1;
    }
    h1 += h2;
    if (h1 >= USERS_PRIME) {
      h1 -= USERS_PRIME;
    }
  }
  assert (tot_users < MAX_USERS_NUM);
  return h1;
#else
  int t = user_id % log_split_mod;
  if (!user_id || (t != log_split_min && t != -log_split_min)) {
    return -1;
  }
  user_id /= log_split_mod;
  if (user_id < 0 || t < 0) {
    user_id--;
  }
  user_id += NEGATIVE_USER_OFFSET;
  return (unsigned) user_id < MAX_USERS ? user_id : -1;
#endif
}

/*
static int unconv_uid (int uid) {
  uid -= NEGATIVE_USER_OFFSET;
  uid *= log_split_mod;
  if (uid < 0) {
    uid += log_split_mod - log_split_min;
  } else {
    uid += log_split_min;
  }
  return uid;
}
*/

user_t *get_user (int user_id) {
  int i = conv_uid (user_id);
  return i >= 0 ? User[i] : 0;
}

user_t *get_user_f (int user_id) {
  int i = conv_uid (user_id);
  user_t *U;
  if (i < 0) { return 0; }
  U = User[i];
  if (U) { return U; }

  int sz = sizeof (user_t) + sublists_num * sizeof (listree_t);
  U = zmalloc (sz);
  memset (U, 0, sz);
  U->user_id = user_id;
  User[i] = U;
  if (i > max_uid) { max_uid = i; }
  if (i < min_uid) { min_uid = i; }
  tot_users++;

  U->first_q = U->last_q = USER_CONN (U);
  U->first_pq = U->last_pq = USER_PCONN (U);

  struct file_user_list_entry *UF = lookup_user_directory (user_id);
  U->dir_entry = UF;
  if (UF) {
    U->last_local_id = UF->user_last_local_id;
    if (persistent_history_enabled) {
      U->persistent_ts = ((struct file_user_list_entry_search_history *) UF)->user_history_max_ts;
    }
    for (i = 0; i < sublists_num; i++) {
      U->Sublists[i].root = NIL;
      U->Sublists[i].N = UF->user_sublists_size[idx_sublists_offset+i];
      U->Sublists[i].last_A = UF->user_last_local_id;
    }
  } else {
    for (i = 0; i < sublists_num; i++) {
      U->Sublists[i].root = NIL;
    }
  }

  U->topmsg_tree = NIL_N;
  
  return U;
}

inline void free_message (message_t *M) {
  assert (M);
  zfree (M, M->len + sizeof (message_t) + text_shift);
  incore_messages--;
}

struct value_data *alloc_value_data (int fields_mask) {
  struct value_data *V = zmalloc (4 + 4 * extra_mask_intcount (fields_mask));
  V->fields_mask = fields_mask;
  V->zero_mask = 0;
  return V;
}

void free_value_data (struct value_data *V) {
  zfree (V, 4 + 4 * extra_mask_intcount (V->fields_mask));
}

extern int pending_http_queries;

int process_user_query_queue (user_t *U) {
  struct conn_query *tmp, *tnext;

  for (tmp = U->first_q; tmp != USER_CONN (U); tmp = tnext) {
    tnext = tmp->next;
    //fprintf (stderr, "scanning user history queue %p,next = %p\n", tmp, tnext);
    pending_http_queries--;
    delete_conn_query (tmp);
    zfree (tmp, sizeof (*tmp));
  }

  return 0;
}

int process_user_query_queue2 (user_t *U) {
  struct conn_query *Q;
  double wtime = get_utime (CLOCK_MONOTONIC) + 0.1;

  for (Q = U->first_q; Q != USER_CONN (U); Q = Q->next) {
    if (Q->timer.wakeup_time > wtime || Q->timer.wakeup_time == 0) {
      Q->timer.wakeup_time = wtime;
      insert_event_timer (&Q->timer);
    }
  }

  return 0;
}

int process_user_persistent_query_queue2 (user_t *U) {
  struct conn_query *Q;
  double wtime = get_utime (CLOCK_MONOTONIC) + 0.1;

  for (Q = U->first_pq; Q != USER_PCONN (U); Q = Q->next) {
    if (Q->timer.wakeup_time > wtime || Q->timer.wakeup_time == 0) {
      Q->timer.wakeup_time = wtime;
      insert_event_timer (&Q->timer);
    }
  }

  return 0;
}

int alloc_history_strings;
long long alloc_history_strings_size;

static inline char *alloc_aux_history_data (const char *string, int length) {
  assert ((unsigned) length <= 65535);
  char *ptr = malloc (length + 3) + 2;
  *((short *)(ptr - 2)) = length;
  memcpy (ptr, string, length);
  ptr[length] = 0;
  alloc_history_strings++;
  alloc_history_strings_size += length + 3;
  return ptr;
}

static inline void free_aux_history_data (long aux_addr) {
#ifdef __LP64__
  aux_addr &= (1LL << 56) - 1;
#endif
  alloc_history_strings--;
  alloc_history_strings_size -= *(unsigned short *)(aux_addr - 2) + 3;
  free ((char *) aux_addr - 2);
}

static inline int new_history_ts (void) {
  return (lrand48() & 0xfffffff) + 0x60000000;
}

static void update_history (user_t *U, int local_id, int flags, int op) {
  assert (U);
  assert (local_id > 0 || op > 5);
  assert ((unsigned char) op < 100);
  if (!history_enabled) {
    return;
  }
  flags = ((flags & 0xffff) | (op << 24));
  if (!U->history_ts) {
    U->history_ts = new_history_ts ();
  }
  if (!U->history) {
    U->history = zmalloc0 (HISTORY_EVENTS * 8);
  }
  int t = ++U->history_ts & (HISTORY_EVENTS - 1);
  if ((unsigned) U->history[t*2+1] >= (100U << 24)) {
    free_aux_history_data (*(long *) (U->history + t*2));
  }
  U->history[t*2+1] = flags;
  U->history[t*2] = local_id;

  if (U->first_q != USER_CONN (U)) {
    process_user_query_queue2 (U);
  }
}

static void update_history_persistent (user_t *U, int local_id, int flags, int op) {
  assert (U && local_id > 0 && (unsigned) op <= 5);
  if (!persistent_history_enabled) {
    return;
  }
  struct incore_persistent_history *H = U->persistent_history;
  if (!H) {
    H = U->persistent_history = malloc (sizeof (struct incore_persistent_history) + 8 * MIN_PERSISTENT_HISTORY_EVENTS);
    H->alloc_events = MIN_PERSISTENT_HISTORY_EVENTS;
    H->cur_events = 0;
    incore_persistent_history_bytes += sizeof (struct incore_persistent_history) + H->alloc_events * 8;
    incore_persistent_history_lists++;
  } else if (H->cur_events == H->alloc_events) {
    H = U->persistent_history = realloc (H, sizeof (struct incore_persistent_history) + 16 * H->alloc_events);
    incore_persistent_history_bytes += H->alloc_events * 8;
    H->alloc_events *= 2;
  }
  int *p = H->history + H->cur_events++ * 2;
  p[0] = local_id;
  p[1] = (flags & 0xffff) | (op << 24);
  incore_persistent_history_events++;

  if (U->first_pq != USER_PCONN (U)) {
    process_user_persistent_query_queue2 (U);
  }
}

static void update_history_both (user_t *U, int local_id, int flags, int op) {
  if ((unsigned) op <= 5) {
    update_history_persistent (U, local_id, flags, op);
  }
  update_history (U, local_id, flags, op);
}

static void update_history_extended (user_t *U, const char *string, int length, int op) {
  assert (U);
  assert ((unsigned char) op >= 100);
  if (!history_enabled) {
    return;
  }
  if (!U->history_ts) {
    U->history_ts = new_history_ts ();
  }
  if (!U->history) {
    U->history = zmalloc0 (HISTORY_EVENTS * 8);
  }
  int t = ++U->history_ts & (HISTORY_EVENTS - 1);
  if ((unsigned) U->history[t*2+1] >= (100U << 24)) {
    free_aux_history_data (*(long *) (U->history + t*2));
  }
  *(void **)(U->history + t*2) = alloc_aux_history_data (string, length);
#ifdef __LP64__
  U->history[t*2+1] |= (op << 24);
#else
  U->history[t*2+1] = (op << 24);
#endif

  if (U->first_q != (struct conn_query *) U) {
    process_user_query_queue2 (U);
  }
}

/* invariant of topmsg_tree : 
   if node (local_id, peer_id) is present in topmsgtree, 
   then local_id MUST be the last element in peermsglistree corresponding to peer_id;
     in particular, all peer_id are distinct.
   When metafile is present in memory, topmsg_tree contains EXACTLY one entry 
        corresponding to each non-empty peermsglist.
   When metafile is absent, topmsg_tree MUST be empty.
*/

/* op = -1 - delete, run AFTER changing peer_tree
   op = 1  - insert, run BEFORE changing peer_tree
*/
int adjust_topmsg_tree (user_t *U, int peer_id, int local_id, int op) {
  char *metafile = get_user_metafile (U);
  struct file_user_list_entry *D = U->dir_entry;
  listree_t X;
  int prev_id = 0;
  tree_num_t *TT;

  assert (local_id > 0 && (metafile || !D || local_id > D->user_last_local_id));

  if (!metafile && D) {
    assert (U->topmsg_tree == NIL_N);
    return 0;
  }

  tree_t *T = tree_lookup (U->peer_tree, peer_id);

  X.root = (T ? T->data : NIL);

  if (metafile) {
    X.A = fetch_file_peer_list (metafile, peer_id, &X.N);
    X.last_A = (X.N ? X.A[X.N-1] : 0);
    //fprintf (stderr, "peer list size %d: %d %d %d...", X.N, X.A?X.A[0]:-1, X.A?X.A[1]:-1, X.A?X.A[2]:-1);
  } else {
    X.A = 0;
    X.N = 0;
    X.last_A = 0;
  }

  prev_id = listree_get_kth_last (&X, 0);

  vkprintf (4, "adjust_topmsg_tree (%p %d, %d, %d, %d) : prev_id=%d metafile=%p D=%p\n", U, U->user_id, peer_id, local_id, op, prev_id, metafile, D);

  assert (prev_id == -1 || prev_id > 0);

  if (local_id < prev_id) {
    // we are inserting or deleting a message under current top of this peermsglistree
    return prev_id;
  }

  if (op > 0) {
    if (prev_id > 0) {
      TT = tree_num_lookup (U->topmsg_tree, prev_id);
      if (TT->z != peer_id) {
        fprintf (stderr, "ERROR in adjust_topmsg_tree: uid=%d, U=%p, mf=%p, D=%p, mf_last_local_id=%d, peer=%d, local_id=%d, prev_id=%d, op=%d, TT.x=%d, TT.z=%d\n", U->user_id, U, metafile, D, D ? D->user_last_local_id : 0, peer_id, local_id, prev_id, op, TT->x, TT->z);
	// print_backtrace ();
	// return 0;
      }
      assert (TT->z == peer_id);
      U->topmsg_tree = tree_num_delete (U->topmsg_tree, prev_id);
    }
    U->topmsg_tree = tree_num_insert (U->topmsg_tree, local_id, lrand48(), peer_id);
  } else {
    TT = tree_num_lookup (U->topmsg_tree, local_id);
    if (TT->z != peer_id) {
      fprintf (stderr, "ERROR in adjust_topmsg_tree: uid=%d, U=%p, mf=%p, D=%p, mf_last_local_id=%d, peer=%d, local_id=%d, prev_id=%d, op=%d, TT.x=%d, TT.z=%d\n", U->user_id, U, metafile, D, D ? D->user_last_local_id : 0, peer_id, local_id, prev_id, op, TT->x, TT->z);
      // print_backtrace ();
      // return 0;
    }
    assert (TT->z == peer_id);
    U->topmsg_tree = tree_num_delete (U->topmsg_tree, local_id);
    if (prev_id > 0) {
      U->topmsg_tree = tree_num_insert (U->topmsg_tree, prev_id, lrand48(), peer_id);
    }
  }

  return prev_id;
}

struct msg_search_node *add_new_msg_search_node (struct msg_search_node *last, int local_id, char *text, int len);
void free_msg_search_node (struct msg_search_node *search_node);

static inline char *skip_kludges (char *text, int len) {
  char *ptr = text, *text_end = text + len, *kptr = text;
  int state = 5;

  while (ptr < text_end) {
    if (feed_byte (*ptr, &state)) {
      return ptr;
    }
    if (!*ptr || (state == 2 && (unsigned char) *ptr < 32 && *ptr != 9)) {
      return 0;
    }
    ++ptr;
  }

  if (state != 2) {
    kptr = ptr;
  }

  return kptr;
}

char *check_kludges (char *text, int len) {
  return skip_kludges (text, len);
}

int store_new_message (struct lev_add_message *E, int random_tag) {
  user_t *U;
  tree_t *T;
  int local_id, len = E->text_len, i, local_extra = 0;
  char *text = E->text;
  message_t *M;

  if (conv_uid (E->user_id) < 0 || (unsigned) len >= MAX_TEXT_LEN) {
    return 0;
  }

  if ((E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT || (E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_ZF || 
      (E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_LL) {
    local_extra = extra_mask_intcount (E->type & MAX_EXTRA_MASK);
    text += local_extra * 4;
  }

  char *kptr = skip_kludges (text, len);

  if (!kptr) {
    fprintf (stderr, "error: bad kludges in message, log pos=%lld, text:len = %p:%d =", log_cur_pos(), text, len);
    for (i = 0; i < len; i++) {
      fprintf (stderr, " %c%02x", (unsigned char) text[i] < 0x20 ? '.' : text[i], (unsigned char) text[i]);
    }
    fprintf (stderr, "\n");

    assert (kptr);
  }

  U = get_user (E->user_id);

  if (U) {
    if (verbosity > 0) {
      if (E->legacy_id && ltree_lookup_legacy (U->legacy_tree, E->legacy_id)) {
        fprintf (stderr, "Message with duplicate legacy id found: %d for user %d, continuing\n", E->legacy_id, E->user_id);
      }
    }
    /*if (E->legacy_id && legacy_tree_lookup (U->legacy_tree, E->legacy_id)) {
      return 0;
    }*/
  } else {
    U = get_user_f (E->user_id);
  }

  local_id = ++U->last_local_id;

  M = zmalloc (sizeof (message_t) + len + text_shift);
  memset (M->extra, 0, text_shift);
  ++incore_messages;

  M->legacy_id = E->legacy_id;

  if ((E->type & -0x100) == LEV_TX_ADD_MESSAGE) {
    M->flags = E->type & 0xff;
    M->date = E->date ? E->date : now;
  } else if ((E->type & -0x1000) == LEV_TX_ADD_MESSAGE_MF) {
    M->flags = E->type & 0xfff;
    M->date = E->date ? E->date : now;
  } else if (E->type == LEV_TX_ADD_MESSAGE_LF || (E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT) {
    M->flags = E->date & 0xffff;
    M->date = now;
  } else if ((E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_LL) {
    M->flags = E->date & 0xffff;
    M->date = now;
    M->legacy_id = (M->legacy_id & 0xffffffffLL) | ( E->ua_hash & 0xffffffff00000000LL);
  } else if ((E->type & ~MAX_EXTRA_MASK) == LEV_TX_ADD_MESSAGE_EXT_ZF) {
    M->flags = 0;
    M->date = E->date ? E->date : now;
  } else {
    assert (0 && "invalid LEV_TX_ADD_MESSAGE");
  }

  M->user_id = E->user_id;
  M->peer_id = E->peer_id;
  M->peer_msg_id = E->peer_msg_id;
  M->global_id = ++last_global_id;
  M->len = len;
  M->kludges_size = kptr - text;

  if (E->peer_id == (-1 << 31)) {
    fprintf (stderr, "warning: new message with peer_id=(-1 << 31), user_id=%d, local_id=%d\n", E->user_id, local_id);
  }

  if (local_extra) {
    assert (E->type & MAX_EXTRA_MASK);
    assert (!(E->type & MAX_EXTRA_MASK & ~write_extra_mask));
    int *M_extra = M->extra, *E_extra = E->extra;
    for (i = 1; i < MAX_EXTRA_FIELDS; i <<= 1) {
      if (E->type & read_extra_mask & i) {
        if (i < 256) {
          *M_extra = *E_extra;
        } else {
          *(long long *) M_extra = *(long long *) E_extra;
        }
      }
      if (index_extra_mask & i) {
        M_extra += (i < 256 ? 1 : 2);
      }
      if (E->type & i) {
        E_extra += (i < 256 ? 1 : 2);
      }
    }
  }

  memcpy (M->text + text_shift, text, len);
  M->text[text_shift + len] = 0;

  if (MATCHES_SUBLIST (M->flags, NewMsgHistoryFilter)) {
    update_history (U, local_id, M->flags & 0xffff, 4);
  }
  update_history_persistent (U, local_id, M->flags & 0xffff, 4);

  struct message_extras *EX = &LastMsgExtra[last_global_id & (CYCLIC_IP_BUFFER_SIZE-1)];
  EX->ip = E->ip;
  EX->port = E->port;
  EX->front = E->front;
  EX->ua_hash = E->ua_hash;
  EX->user_id = E->user_id;
  EX->local_id = local_id;
  EX->date = M->date;

  U->msg_tree = tree_insert (U->msg_tree, local_id, (lrand48 () << 3) + TF_PLUS, M);
  for (i = 0; i < sublists_num; i++) {
    if (MATCHES_SUBLIST (M->flags, Sublists[i])) {
      U->Sublists[i].root =
        tree_ext_insert (U->Sublists[i].root, local_id, (lrand48 () << 2) + TF_PLUS, 0);
    }
  }

  if (M->peer_id && MATCHES_SUBLIST (M->flags, PeerFlagFilter)) {
    adjust_topmsg_tree (U, M->peer_id, local_id, 1);
    T = tree_lookup (U->peer_tree, M->peer_id);
    if (!T) {
      tree_ext_t *N = new_tree_ext_node (local_id, (lrand48() << 2) + TF_PLUS, 0);
      N->delta = 1;
      U->peer_tree = tree_insert (U->peer_tree, M->peer_id, lrand48 (), N);
    } else {
      T->data = tree_ext_insert ((tree_ext_t *) T->data, local_id, (lrand48 () << 2) + TF_PLUS, 0);
    }
  }

  if (M->legacy_id) {
    assert (!legacy_tree_lookup (U->legacy_tree, M->legacy_id, local_id));
    U->legacy_tree = legacy_tree_insert (U->legacy_tree, M->legacy_id, lrand48 (), local_id);
  }

  if (random_tag > 0) {
    U->insert_tags[U->cur_insert_tags][0] = random_tag;
    U->insert_tags[U->cur_insert_tags][1] = local_id;
    if (++U->cur_insert_tags == MAX_INS_TAGS) {
      U->cur_insert_tags = 0;
    }
  }

  if (search_enabled && U->last_local_id <= (U->dir_entry ? U->dir_entry->user_last_local_id : 0) + MAX_USER_INCORE_MESSAGES) {
    U->last = add_new_msg_search_node (U->last, local_id, text, len);
  }

  return local_id;
}

static void free_edit_text (edit_text_t *edit_text) {
  assert (edit_text);
  if (edit_text->search_node) {
    assert (search_enabled);
    free_msg_search_node (edit_text->search_node);
    edit_text->search_node = 0;
  }
  assert ((unsigned) edit_text->len < MAX_TEXT_LEN);
  edit_text->kludges_size = -1;
  edit_text_nodes--;
  edit_text_bytes -= edit_text->len;
  zfree (edit_text, offsetof (edit_text_t, text) + edit_text->len + 1);
}

static edit_text_t *make_edit_text (int local_id, char *text, int text_len, int kludges_len) {
  assert (kludges_len >= 0 && kludges_len <= text_len && text_len < MAX_TEXT_LEN);
  assert (!text[text_len]);
  edit_text_t *X = zmalloc (offsetof (edit_text_t, text) + text_len + 1);
  X->len = text_len;
  X->kludges_size = kludges_len;
  memcpy (X->text, text, text_len + 1);
  X->search_node = search_enabled ? add_new_msg_search_node (0, local_id, text, text_len) : 0;
  edit_text_nodes++;
  edit_text_bytes += text_len;
  return X;
}  

/* changes text of previously existing message */
int replace_message_text (struct lev_replace_text_long *E) {
  int text_len;
  char *text;
  int local_id = E->local_id;

  if (E->type == LEV_TX_REPLACE_TEXT_LONG) {
    text = E->text;
    text_len = E->text_len;
  } else {
    assert ((E->type & -0x1000) == LEV_TX_REPLACE_TEXT);
    text = ((struct lev_replace_text *) E)->text;
    text_len = E->type & 0xfff;
  }

  assert ((unsigned) text_len < MAX_TEXT_LEN && !text[text_len]);
  assert (local_id > 0);

  char *kptr = skip_kludges (text, text_len);
  assert (kptr);

  if (conv_uid (E->user_id) < 0) {
    return 0;
  }

  user_t *U = get_user_f (E->user_id);
  struct file_user_list_entry *D = U->dir_entry;
  tree_t *T = tree_lookup (U->msg_tree, local_id);
  message_t *old_msg, *new_msg;

  update_history_both (U, local_id, 0, 5);

  if (T) {
    switch (T->y & 7) {
    case TF_ZERO:
    case TF_ZERO_PRIME:
      assert (D && local_id <= D->user_last_local_id);
      break;
    case TF_REPLACED:
      assert (D && local_id <= D->user_last_local_id);
    case TF_PLUS:
      old_msg = T->msg;
      new_msg = zmalloc (sizeof (message_t) + text_shift + text_len);
      memcpy (new_msg, old_msg, offsetof (message_t, text) + text_shift);
      memcpy (new_msg->text + text_shift, text, text_len + 1);
      new_msg->len = text_len;
      new_msg->kludges_size = kptr - text;
      zfree (old_msg, sizeof (message_t) + text_shift + old_msg->len);
      T->msg = new_msg;
      if (!search_enabled) {
	return local_id;
      }
      break;
    default:
      assert ((T->y & 7) == TF_PLUS);
    }
  }

  tree_t *X = tree_lookup (U->edit_text_tree, local_id);

  if (X) {
    free_edit_text (X->edit_text);
    X->edit_text = make_edit_text (local_id, text, text_len, kptr - text);
  } else {
    U->edit_text_tree = tree_insert (U->edit_text_tree, local_id, lrand48(), make_edit_text (local_id, text, text_len, kptr - text));
  }

  return local_id;
}

/* 0: object doesn't (and didn't) exist; 1: object existed, changed; 2: object not changed; 3: object may have changed;
   -1: error 
*/
static int adjust_message_internal (user_t *U, tree_t *T, int local_id, int clear_mask, int set_mask) {
  struct file_user_list_entry *D = U->dir_entry;
  struct file_message *M;
  char *metafile = get_user_metafile (U);
  int old_flags = -1, new_flags = -1, peer_id = (-1 << 31);
  long long legacy_id = (-1LL << 63);

  assert ((clear_mask == -1 && set_mask == -1) || (!(clear_mask & set_mask) && (clear_mask | set_mask | 0xffff) == 0xffff));
  assert (local_id > 0 && local_id <= U->last_local_id);

  if (!T) {
    T = tree_lookup (U->msg_tree, local_id);
  }

  if (!T) {
    if (!D || local_id > D->user_last_local_id) {
      return 0;
    }
    assert (metafile);
    M = user_metafile_message_lookup (metafile, U->mf->len, local_id, U);
    if (!M) {
      return 0;
    }
    peer_id = M->peer_id;
    if (M->flags & TXF_HAS_LEGACY_ID) {
      legacy_id = M->data[0];
      assert (!(M->flags & TXF_HAS_LONG_LEGACY_ID));
    } else if (M->flags & TXF_HAS_LONG_LEGACY_ID) {
      legacy_id = *(long long *)(M->data);
    } else {
      legacy_id = 0;
    }
    old_flags = M->flags & 0xffff;
    if (clear_mask >= 0) {
      new_flags = (old_flags & ~clear_mask) | set_mask;
      if (new_flags == old_flags) {
        return 2;
      }
      U->msg_tree = tree_insert (U->msg_tree, local_id, (lrand48() << 3) + TF_ZERO, (void *) (long) new_flags);
    } else {
      U->msg_tree = tree_insert (U->msg_tree, local_id, (lrand48() << 3) + TF_MINUS, 0);
    }
  } else {
    if (clear_mask >= 0) {
      switch (T->y & 7) {
      case TF_MINUS:
        return 0;
      case TF_ZERO:
        old_flags = T->flags;
        T->flags = new_flags = (old_flags & ~clear_mask) | set_mask;
        break;
      case TF_ZERO_PRIME:
        old_flags = T->value->flags;
        T->value->flags = new_flags = (old_flags & ~clear_mask) | set_mask;
        break;
      case TF_PLUS:
      case TF_REPLACED:
        old_flags = T->msg->flags;
        T->msg->flags = new_flags = (old_flags & ~clear_mask) | set_mask;
        peer_id = T->msg->peer_id;
	legacy_id = T->msg->legacy_id;
	break;
      default:
        assert (0);	
      }
      if (new_flags == old_flags) {
        return 2;
      }
    } else {
      switch (T->y & 7) {
      case TF_MINUS:
        return 0;
      case TF_ZERO:
        old_flags = T->flags;
        T->y |= TF_MINUS;
        T->flags = -1;
        break;
      case TF_ZERO_PRIME:
        old_flags = T->value->flags;
        free_value_data (T->value);
        T->value = 0;
        T->y += TF_MINUS - TF_ZERO_PRIME;
        break;
      case TF_PLUS:
        old_flags = T->msg->flags;
        peer_id = T->msg->peer_id;
	legacy_id = T->msg->legacy_id;
        free_message (T->msg);
        U->msg_tree = tree_delete (U->msg_tree, local_id);
        break;
      case TF_REPLACED:
        old_flags = T->msg->flags;
        peer_id = T->msg->peer_id;
	legacy_id = T->msg->legacy_id;
        free_message (T->msg);
        T->msg = 0;
        T->y += TF_MINUS - TF_REPLACED;
        break;
      default:
        assert (0);
      }
    }
  }

  if (clear_mask < 0) {
    /* delete node from edit_text_tree as well */
    tree_t *X = tree_lookup (U->edit_text_tree, local_id);
    if (X) {
      free_edit_text (X->edit_text);
      X->edit_text = 0;
      U->edit_text_tree = tree_delete (U->edit_text_tree, local_id);
    }
  }

/* now have to process one peer list and some sublists based on (old_flags => new_flags) */
  if (peer_id == (-1 << 31)) {
    assert (metafile);
    M = user_metafile_message_lookup (metafile, U->mf->len, local_id, U);
    assert (M);
    peer_id = M->peer_id;
    if (M->flags & TXF_HAS_LEGACY_ID) {
      legacy_id = M->data[0];
      assert (!(M->flags & TXF_HAS_LONG_LEGACY_ID));
    } else if (M->flags & TXF_HAS_LONG_LEGACY_ID) {
      legacy_id = *(long long *)(M->data);
    } else {
      legacy_id = 0;
    }
  }

  assert (legacy_id != (-1LL << 63));

  if (peer_id) {
    listree_t X;
    if (new_flags == -1 || !MATCHES_SUBLIST (new_flags, PeerFlagFilter)) {
      if (MATCHES_SUBLIST (old_flags, PeerFlagFilter)) {
        /* remove from peer list */
        T = tree_lookup (U->peer_tree, peer_id);
        if (!T) { 
          U->peer_tree = tree_insert (U->peer_tree, peer_id, lrand48(), NIL);
          T = NewestNode;
        }
        if (metafile) {
          X.A = fetch_file_peer_list (metafile, peer_id, &X.N);
          X.last_A = (X.N ? X.A[X.N-1] : 0);
        } else {
          X.A = 0;
          X.N = 0;
          X.last_A = D ? D->user_last_local_id : 0;
        }
        X.root = T->data;
        listree_delete (&X, local_id);
        if (X.root != T->data) {
          T->data = X.root;
        }
        adjust_topmsg_tree (U, peer_id, local_id, -1);
      }
    } else if (!MATCHES_SUBLIST (old_flags, PeerFlagFilter)) {
      /* add into peer list */
      adjust_topmsg_tree (U, peer_id, local_id, 1);
      T = tree_lookup (U->peer_tree, peer_id);
      if (!T) { 
        U->peer_tree = tree_insert (U->peer_tree, peer_id, lrand48(), NIL);
        T = NewestNode;
      }
      if (metafile) {
        X.A = fetch_file_peer_list (metafile, peer_id, &X.N);
        X.last_A = (X.N ? X.A[X.N-1] : 0);
      } else {
        X.A = 0;
        X.N = 0;
        X.last_A = D ? D->user_last_local_id : 0;
      }
      X.root = T->data;
      listree_insert (&X, local_id);
      if (X.root != T->data) {
        T->data = X.root;
      }
    }
  }

  int i;
  for (i = 0; i < sublists_num; i++) {
    if (new_flags == -1 || !MATCHES_SUBLIST (new_flags, Sublists[i])) {
      if (MATCHES_SUBLIST (old_flags, Sublists[i])) {
        /* remove from sublist */
        listree_delete (&U->Sublists[i], local_id);
      }
    } else if (!MATCHES_SUBLIST (old_flags, Sublists[i])) {
      /* add into sublist */
      listree_insert (&U->Sublists[i], local_id);
    }
  }

  /* delete from legacy list */
  if (new_flags == -1 && legacy_id && (!D || local_id > D->user_last_local_id)) {
    U->legacy_tree = legacy_tree_delete (U->legacy_tree, legacy_id, local_id);
  }
  return 1;
}

/* 0: object doesn't (and didn't) exist; 1: object existed, changed; 2: object not changed; 3: object may have changed;
   -1: error 
   V is freed
*/
static int adjust_message_values_internal (user_t *U, int local_id, struct value_data *V) {
  struct file_user_list_entry *D = U->dir_entry;
  tree_t *T;
  struct file_message *M;
  char *metafile = get_user_metafile (U);
  int *T_extra, *M_extra, *V_extra, *W_extra;

  assert (local_id > 0 && local_id <= U->last_local_id);
  assert (!(V->zero_mask & ~V->fields_mask) && !(V->fields_mask & ~index_extra_mask));

  T = tree_lookup (U->msg_tree, local_id);

  if (!T || (T->y & 7) == TF_ZERO) {
    if (!D || local_id > D->user_last_local_id) {
      assert (!T);
      free_value_data (V);
      return 0;
    }
    assert (metafile);
    M = user_metafile_message_lookup (metafile, U->mf->len, local_id, U);
    if (!M) {
      assert (!T);
      free_value_data (V);
      return 0;
    }

    M_extra = M->data;
    V_extra = V->data;

    if (M->flags & TXF_HAS_LEGACY_ID) {
      M_extra++;
    }
    if (M->flags & TXF_HAS_LONG_LEGACY_ID) {
      M_extra += 2;
      assert (!(M->flags & TXF_HAS_LEGACY_ID));
    }
    if (M->flags & TXF_HAS_PEER_MSGID) {
      M_extra++;
    }

    int i, q = (M->flags >> 16) & MAX_EXTRA_MASK, r = q & ~V->zero_mask & V->fields_mask;

    assert (!(q & ~index_extra_mask));

    for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
      if (r & i) {
        if (i < 256) {
          *V_extra += *M_extra;
        } else {
          *(long long *) V_extra += *(long long *) M_extra;
        }
      }
      if (q & i) {
        M_extra += (i < 256 ? 1 : 2);
      }
      if (V->fields_mask & i) {
        V_extra += (i < 256 ? 1 : 2);
      }
    }

    if (!T) {
      V->flags = M->flags & 0xffff;
      U->msg_tree = tree_insert (U->msg_tree, local_id, (lrand48() << 3) + TF_ZERO_PRIME, V);
    } else {
      V->flags = T->flags & 0xffff;
      T->y += TF_ZERO_PRIME - TF_ZERO;
      T->value = V;
    }

    return 1;

  } else {
    int i, q; 
    V_extra = V->data;

    switch (T->y & 7) {
    case TF_MINUS:
      free_value_data (V);
      return 0;
    case TF_PLUS:
    case TF_REPLACED:
      T_extra = T->msg->extra;
      for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
        if (V->fields_mask & i) {
          if (i < 256) {
            if (V->zero_mask & i) {
              *T_extra = *V_extra;
            } else {
              *T_extra += *V_extra;
            }
            V_extra++;
          } else {
            if (V->zero_mask & i) {
              *(long long *) T_extra = *(long long *) V_extra;
            } else {
              *(long long *) T_extra += *(long long *) V_extra;
            }
            V_extra += 2;
          }
        }
        if (index_extra_mask & i) {
          T_extra += (i < 256 ? 1 : 2);
        }
      }
      free_value_data (V);
      return 1;
    case TF_ZERO_PRIME:
      //assert (!V->zero_mask);
      //assert (!(V->fields_mask & T->value->fields_mask));
      assert (metafile);
      M = user_metafile_message_lookup (metafile, U->mf->len, local_id, U);
      assert (M);

      struct value_data *W = alloc_value_data (V->fields_mask | T->value->fields_mask);
      W->flags = T->value->flags;

      W_extra = W->data;
      V_extra = V->data;
      T_extra = T->value->data;

      M_extra = M->data;
      if (M->flags & TXF_HAS_LEGACY_ID) {
        M_extra++;
      }
      if (M->flags & TXF_HAS_LONG_LEGACY_ID) {
        M_extra += 2;
	assert (!(M->flags & TXF_HAS_LEGACY_ID));
      }
      if (M->flags & TXF_HAS_PEER_MSGID) {
        M_extra++;
      }
      q = (M->flags >> 16) & MAX_EXTRA_MASK;

      for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
        if (V->zero_mask & i) {
          if (i < 256) {
            *W_extra++ = *V_extra;
          } else {
            *(long long *) W_extra = *(long long *) V_extra;
            W_extra += 2;
          }
        } else if (V->fields_mask & i) {
          if (T->value->fields_mask & i) {
            if (i < 256) {
              *W_extra++ = *V_extra + *T_extra;
            } else {
              *(long long *) W_extra = *(long long *) V_extra + *(long long *) T_extra;
              W_extra += 2;
            }
          } else if (q & i) {
            if (i < 256) {
              *W_extra++ = *V_extra + *M_extra;
            } else {
              *(long long *) W_extra = *(long long *) V_extra + *(long long *) M_extra;
              W_extra += 2;
            }
          } else {
            if (i < 256) {
              *W_extra++ = *V_extra;
            } else {
              *(long long *) W_extra = *(long long *) V_extra;
              W_extra += 2;
            }
          }
        } else if (T->value->fields_mask & i) {
          if (i < 256) {
            *W_extra++ = *T_extra;
          } else {
            *(long long *) W_extra = *(long long *) T_extra;
            W_extra += 2;
          }
        }
        if (q & i) {
          M_extra += (i < 256 ? 1 : 2);
        }
        if (T->value->fields_mask & i) {
          T_extra += (i < 256 ? 1 : 2);
        }
        if (V->fields_mask & i) {
          V_extra += (i < 256 ? 1 : 2);
        }
      }
      free_value_data (V);
      free_value_data (T->value);
      T->value = W;

      return 1;
    }
  }
  assert (0);
  return -1;
}

static void process_delayed_ops (user_t *U, tree_t *T) {
  if (!T) {
    return;
  }
  process_delayed_ops (U, T->left);
  if (T->flags == -1) {
    assert (adjust_message_internal (U, 0, T->x, -1, -1) >= 0);
  } else {
    assert (adjust_message_internal (U, 0, T->x, T->clear_mask, T->set_mask) >= 0);
  }
  process_delayed_ops (U, T->right);
  zfree (T, sizeof (tree_t));
}


static void process_delayed_values (user_t *U, tree_t *T) {
  if (!T) {
    return;
  }

  process_delayed_values (U, T->left);

  assert (adjust_message_values_internal (U, T->x, T->value) >= 0);

  process_delayed_values (U, T->right);

  zfree (T, sizeof (tree_t));
}

/* 0: object doesn't exist; 1: object existed, changed; 2: object not changed; 3: object may have changed;
   -1: error 
*/
int adjust_message_intermediate (int user_id, int local_id, int clear_mask, int set_mask) {
  user_t *U;
  struct file_user_list_entry *D;
  tree_t *T;
  int t;

  assert (local_id > 0);

  U = get_user (user_id);

  if (!U) {
    D = lookup_user_directory (user_id);
    if (!D) {
      return 0;
    }
    if (local_id > D->user_last_local_id) {
      return 0;
    }
    U = get_user_f (user_id);
  } else {
    D = U->dir_entry;
    if (local_id > U->last_local_id) {
      return 0;
    }
  }

  if (clear_mask < 0) {
    update_history (U, local_id, 0, 0);

    U->delayed_value_tree = tree_delete (U->delayed_value_tree, local_id);
  } else if ((clear_mask | set_mask) == 0xffff) {
    update_history (U, local_id, set_mask, 1);
  } else {
    if (clear_mask) {
      update_history (U, local_id, clear_mask, 3);
    }
    if (set_mask) {
      update_history (U, local_id, set_mask, 2);
    }
  }

  T = tree_lookup (U->msg_tree, local_id);

  if (T || !D || local_id > D->user_last_local_id || get_user_metafile (U)) {
    if (T && ((T->y & 7) == TF_ZERO || (T->y & 7) == TF_ZERO_PRIME) && !get_user_metafile (U)) {
      if (verbosity > 1) {
        fprintf (stderr, "warning: interesting situation for message %d:%d : have ZERO node (type=%d) in memory, no metafile loaded; creating delayed flags operation node.\n", user_id, local_id, T->y & 7);
      }
    } else {
      return adjust_message_internal (U, T, local_id, clear_mask, set_mask);
    }
  }

  if (!clear_mask && !set_mask) {
    return 2;
  }

  T = tree_lookup (U->delayed_tree, local_id);
  if (!T) {
    t = (clear_mask < 0 ? -1 : COMBINE_CLEAR_SET (clear_mask, set_mask));
    U->delayed_tree = tree_insert (U->delayed_tree, local_id, lrand48 (), (void *) (long) t);
//    if (U->max_delayed_local_id < local_id) {
//      U->max_delayed_local_id = local_id;
//    }
    return 3;
  }

  if (T->flags == -1) {
    return 0;
  }

  if (clear_mask < 0) {
    T->flags = -1;
    return 3;
  }

  t = COMBINE_CLEAR_SET ((T->clear_mask | clear_mask) & ~set_mask, (T->set_mask | set_mask) & ~clear_mask);

  if (T->flags == t) {
    return 2;
  }

  T->flags = t;
  return 3;
}

int adjust_message (int user_id, int local_id, int clear_mask, int set_mask, int from_binlog) {
  if (clear_mask != -1 || set_mask != -1) {
    clear_mask &= 0xffff;
    set_mask &= 0xffff;
    assert (!(clear_mask & set_mask));
  }

  if (conv_uid (user_id) < 0 || local_id <= 0) {
    return -1;
  }

  int res = adjust_message_intermediate (user_id, local_id, clear_mask, set_mask);

  if (from_binlog || (res & 1) || (res >= 0 && write_all_events)) {
    user_t *U = get_user_f (user_id);
    if (clear_mask < 0) {
      update_history_persistent (U, local_id, 0, 0);
    } else if ((clear_mask | set_mask) == 0xffff) {
      update_history_persistent (U, local_id, set_mask, 1);
    } else {
      if (clear_mask) {
	assert (!set_mask);
	update_history_persistent (U, local_id, clear_mask, 3);
      } else {
	assert (!clear_mask);
	update_history_persistent (U, local_id, set_mask, 2);
      }
    }
  }

  return res;
}

/* S is a delayed_values_tree node, V contains a description of the action to perform
  V is applied to S, according to do_mask
  S is reallocated if necessary and returned */
static struct value_data *merge_delayed_values (struct value_data *S, struct value_data *V, int do_mask, int has_zero_mask) {
  int i;
  struct value_data *W;
  int *S_extra, *V_extra, *W_extra;

  do_mask &= V->fields_mask;
  if ((do_mask | S->fields_mask) != S->fields_mask) {
    W = alloc_value_data (do_mask | S->fields_mask);
  } else {
    W = S;
  }
  if (has_zero_mask) {
    W->zero_mask = (do_mask & V->zero_mask) | S->zero_mask;
  } else {
    W->zero_mask = S->zero_mask;
  }

  W_extra = W->data;
  S_extra = S->data;
  V_extra = V->data;

  for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
    if (do_mask & i) {
      if ((V->zero_mask & i) || !(S->fields_mask & i)) {
        if (i < 256) {
          *W_extra++ = *V_extra;
        } else {
          *(long long *) W_extra = *(long long *) V_extra;
          W_extra += 2;
        }
      } else {
        if (i < 256) {
          *W_extra++ = *V_extra + *S_extra;
        } else {
          *(long long *) W_extra = *(long long *) V_extra + *(long long *) S_extra;
          W_extra += 2;
        }
      }
    } else if (S->fields_mask & i) {
      if (i < 256) {
        *W_extra++ = *S_extra;
      } else {
        *(long long *) W_extra = *(long long *) S_extra;
        W_extra += 2;
      }
    }

    if (S->fields_mask & i) {
      S_extra += i < 256 ? 1 : 2;
    }

    if (V->fields_mask & i) {
      V_extra += i < 256 ? 1 : 2;
    }
  }
  if (W != S) {
    free_value_data (S);
  }

  return W;
}

/* removes from value_data fields absent in read_extra_mask */
struct value_data *convert_value_data (struct value_data *V, int mode, int flags) {
  struct value_data *W = alloc_value_data (V->fields_mask & flags);
  int i, *V_extra = V->data, *W_extra = W->data;
  W->zero_mask = V->zero_mask & flags;
  for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
    if (W->fields_mask & i) {
      if (i < 256) {
        *W_extra++ = *V_extra;
      } else {
        *(long long *) W_extra = *(long long *) V_extra;
        W_extra += 2;
      }
    }
    if (V->fields_mask & i) {
      V_extra += i < 256 ? 1 : 2;
    }
  }
  if (!mode) {
    free_value_data (V);
  }
  return W;
}

/* 0: object doesn't exist; 1: object existed, changed; 2: object not changed; 3: object may have changed;
   -1: error 
   V is freed
*/
int adjust_message_values (int user_id, int local_id, struct value_data *V) {
  user_t *U;
  struct file_user_list_entry *D;
  tree_t *T, *S;
  struct value_data *W;
  int remaining_mask;

  if (conv_uid (user_id) < 0 || local_id <= 0) {
    free_value_data (V);
    return -1;
  }

  assert (!(V->zero_mask & ~V->fields_mask) && !(V->fields_mask & ~write_extra_mask));

  if (!(V->fields_mask & read_extra_mask)) {
    /* all changes are to fields which will be always read as zero, ignore */
    free_value_data (V);
    return 3;
  }

  U = get_user (user_id);

  if (!U) {
    D = lookup_user_directory (user_id);
    if (!D || local_id > D->user_last_local_id) {
      free_value_data (V);
      return 0;
    }
    U = get_user_f (user_id);
  } else {
    D = U->dir_entry;
    if (local_id > U->last_local_id) {
      free_value_data (V);
      return 0;
    }
  }

  if (V->fields_mask & ~read_extra_mask) {
    V = convert_value_data (V, 0, read_extra_mask);
  }

  if (!D || local_id > D->user_last_local_id || get_user_metafile (U)) {
    return adjust_message_values_internal (U, local_id, V);
  }

  T = tree_lookup (U->msg_tree, local_id);

  if (!T) {
    S = tree_lookup (U->delayed_value_tree, local_id);
    if (!S) {
      U->delayed_value_tree = tree_insert (U->delayed_value_tree, local_id, lrand48 (), V);
      return 3;
    }
    /* merge S->value with V into new S->value */
    S->value = merge_delayed_values (S->value, V, -1, 1);
    free_value_data (V);
    return 3;
  }

  /* if T is present, S may contain only increment actions */

  switch (T->y & 7) {
  case TF_MINUS:
    free_value_data (V);
    return 0;
  case TF_REPLACED:
    /* apply V to T->extra */
    return adjust_message_values_internal (U, local_id, V);
  case TF_ZERO:
    if (!V->zero_mask) {
      S = tree_lookup (U->delayed_value_tree, local_id);
      /* V<set> is empty, so don't need to convert ZERO into ZERO_PRIME */
      /* merge S->value with V into new S->value */
      if (!S) {
        U->delayed_value_tree = tree_insert (U->delayed_value_tree, local_id, lrand48 (), V);
        return 3;
      }
      S->value = merge_delayed_values (S->value, V, -1, 1);
      free_value_data (V);
      return 3;
    }
    /* convert into TF_ZERO_PRIME, use V<set> as new T->value */
    /* merge V<incr> with S */
    W = alloc_value_data (V->zero_mask);
    W->flags = T->flags & 0xffff;
    T->y += TF_ZERO_PRIME - TF_ZERO;
    T->value = W;
  case TF_ZERO_PRIME:
    S = tree_lookup (U->delayed_value_tree, local_id);
    /* apply V to T->value as much as possible (fields of V present in T->value + V<set>) */
    T->value = merge_delayed_values (T->value, V, V->zero_mask | T->value->fields_mask, 0);
    /* merge remaining part of V with S */
    remaining_mask = V->fields_mask & ~(V->zero_mask | T->value->fields_mask);
    if (remaining_mask) {
      if (!S) {
        if (remaining_mask != V->fields_mask) {
          V = convert_value_data (V, 0, remaining_mask);
        }
        U->delayed_value_tree = tree_insert (U->delayed_value_tree, local_id, lrand48 (), V);
        return 3;
      }
      S->value = merge_delayed_values (S->value, V, ~T->value->fields_mask, 1);
    }
    free_value_data (V);
    return 3;
  }

  assert (0);
  return -1;
}

int delete_first_messages (int user_id, int first_local_id) {
  if (conv_uid (user_id) < 0 || first_local_id <= 0) {
    return -1;
  }
  user_t *U = get_user (user_id);
  if (U) {
    if (first_local_id > U->last_local_id + 1) {
      first_local_id = U->last_local_id + 1;
    }
  } else {
    struct file_user_list_entry *D = lookup_user_directory (user_id);
    if (!D) {
      return -1;
    }
    if (first_local_id > D->user_last_local_id + 1) {
      first_local_id = D->user_last_local_id + 1;
    }
  }

  return first_local_id;
}


int text_replay_logevent (struct lev_generic *E, int size) {
  int s, t;
  struct lev_add_message *EM;
  struct value_data *V;

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return text_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);
  case LEV_TX_ADD_MESSAGE ... LEV_TX_ADD_MESSAGE + 0xff:
  case LEV_TX_ADD_MESSAGE_MF ... LEV_TX_ADD_MESSAGE_MF + 0xfff:
  case LEV_TX_ADD_MESSAGE_LF:
    if (size < sizeof (struct lev_add_message)) { return -2; }
    EM = (void *) E;
    s = sizeof (struct lev_add_message) + EM->text_len + 1;
    if (size < s) { return -2; }
    if ((unsigned) EM->text_len >= MAX_TEXT_LEN || EM->text[EM->text_len]) { 
      return -4; 
    }
    store_new_message (EM, 0);
    return s;
  case LEV_TX_ADD_MESSAGE_EXT ... LEV_TX_ADD_MESSAGE_EXT + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_EXT_ZF ... LEV_TX_ADD_MESSAGE_EXT_ZF + MAX_EXTRA_MASK:
  case LEV_TX_ADD_MESSAGE_EXT_LL ... LEV_TX_ADD_MESSAGE_EXT_LL + MAX_EXTRA_MASK:
    if (size < sizeof (struct lev_add_message)) { return -2; }
    EM = (void *) E;
    t = extra_mask_intcount (E->type & MAX_EXTRA_MASK) * 4;
    s = sizeof (struct lev_add_message) + t + EM->text_len + 1;
    if (size < s) { return -2; }
    if ((unsigned) EM->text_len >= MAX_TEXT_LEN || EM->text[t + EM->text_len]) { 
      return -4; 
    }
    store_new_message (EM, 0);
    return s;
  case LEV_TX_REPLACE_TEXT ... LEV_TX_REPLACE_TEXT + 0xfff:
    if (size < sizeof (struct lev_replace_text)) { return -2; }
    t = E->type & 0xfff;
    s = offsetof (struct lev_replace_text, text) + t + 1; 
    if (size < s) { return -2; }
    if ((unsigned) t >= MAX_TEXT_LEN || ((struct lev_replace_text *) E)->text[t]) {
      return -4;
    }
    replace_message_text ((struct lev_replace_text_long *) E);
    return s;
  case LEV_TX_REPLACE_TEXT_LONG:
    if (size < sizeof (struct lev_replace_text_long)) { return -2; }
    t = ((struct lev_replace_text_long *) E)->text_len;
    s = offsetof (struct lev_replace_text_long, text) + t + 1; 
    if (size < s) { return -2; }
    if ((unsigned) t >= MAX_TEXT_LEN || ((struct lev_replace_text_long *) E)->text[t]) {
      return -4;
    }
    replace_message_text ((struct lev_replace_text_long *) E);
    return s;
  case LEV_TX_DEL_MESSAGE:
    if (size < 12) { return -2; }
    adjust_message (E->a, E->b, -1, -1, 1);
    return 12;
  case LEV_TX_DEL_FIRST_MESSAGES:
    if (size < 12) { return -2; }
    delete_first_messages (E->a, E->b);
    return 12;
  case LEV_TX_SET_MESSAGE_FLAGS ... LEV_TX_SET_MESSAGE_FLAGS+0xff:
    if (size < 12) { return -2; }
    adjust_message (E->a, E->b, ~(E->type & 0xff), E->type & 0xff, 1);
    return 12;
  case LEV_TX_SET_MESSAGE_FLAGS_LONG:
    if (size < 16) { return -2; }
    adjust_message (E->a, E->b, ~(E->c & 0xffff), (E->c & 0xffff), 1);
    return 16;
  case LEV_TX_INCR_MESSAGE_FLAGS ... LEV_TX_INCR_MESSAGE_FLAGS+0xff:
    if (size < 12) { return -2; }
    adjust_message (E->a, E->b, 0, E->type & 0xff, 1);
    return 12;
  case LEV_TX_INCR_MESSAGE_FLAGS_LONG:
    if (size < 16) { return -2; }
    adjust_message (E->a, E->b, 0, (E->c & 0xffff), 1);
    return 16;
  case LEV_TX_DECR_MESSAGE_FLAGS ... LEV_TX_DECR_MESSAGE_FLAGS+0xff:
    if (size < 12) { return -2; }
    adjust_message (E->a, E->b, E->type & 0xff, 0, 1);
    return 12;
  case LEV_TX_DECR_MESSAGE_FLAGS_LONG:
    if (size < 16) { return -2; }
    adjust_message (E->a, E->b, E->c & 0xffff, 0, 1);
    return 16;
  case LEV_TX_SET_EXTRA_FIELDS ... LEV_TX_SET_EXTRA_FIELDS + MAX_EXTRA_MASK:
    s = 12 + 4 * extra_mask_intcount (E->type & MAX_EXTRA_MASK);
    if (size < s) { return -2; }
    V = alloc_value_data (E->type & MAX_EXTRA_MASK);
    V->zero_mask = V->fields_mask;
    memcpy (V->data, ((struct lev_set_extra_fields *) E)->extra, s - 12);
    adjust_message_values (E->a, E->b, V);
    return s;
  case LEV_TX_INCR_FIELD ... LEV_TX_INCR_FIELD + 7:
    if (size < 16) { return -2; }
    V = alloc_value_data (1 << (E->type & 7));
    V->zero_mask = 0;
    V->data[0] = E->c;
    adjust_message_values (E->a, E->b, V);
    return 16;
  case LEV_TX_INCR_FIELD_LONG + 8 ... LEV_TX_INCR_FIELD_LONG + 11:
    if (size < 20) { return -2; }
    V = alloc_value_data (0x100 << (E->type & 3));
    V->zero_mask = 0;
    V->data[0] = E->c;
    V->data[1] = E->d;
    adjust_message_values (E->a, E->b, V);
    return 20;
  case LEV_CHANGE_FIELDMASK_DELAYED:
    if (size < 8) { return -2; }
    change_extra_mask (E->a);
    return 8;
  }
   
  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());

  return -3;

}

static inline void copy_adjust_text (char *ptr, char *text, int text_len) {
  int state = 5;

  char *text_end = text + text_len;

  while (text < text_end) {
    feed_byte (*text, &state);
    if (!*text || (state == 2 && (unsigned char) *text < ' ' && *text != 9)) {
      *ptr++ = ' ';
    } else {
      *ptr++ = *text;
    }
    text++;
  }

  *ptr = 0;
}

/* adding new log entries */

int do_store_new_message (struct lev_add_message *M, int random_tag, char *text, long long legacy_id) {
  struct lev_add_message *E;
  char *ptr;
  int i;
  //user_t *U;

  if (conv_uid (M->user_id) < 0 || (unsigned) M->text_len >= MAX_TEXT_LEN) {
    return -1;
  }

  //U = get_user (M->user_id);

  int fmask = M->type >> 16, wmask = fmask & write_extra_mask;

  M->legacy_id = legacy_id;
  if (!FITS_AN_INT (legacy_id)) {
    M->date = M->type & 0xffff;
    M->type = LEV_TX_ADD_MESSAGE_EXT_LL | wmask;
    M->ua_hash = (M->ua_hash & 0xffffffffLL) | (legacy_id & 0xffffffff00000000LL);
  } else if (wmask & MAX_EXTRA_MASK) {
    M->date = M->type & 0xffff;
    M->type = LEV_TX_ADD_MESSAGE_EXT | wmask;
  } else if (M->type & -0x1000) {
    M->date = M->type & 0xffff;
    M->type = LEV_TX_ADD_MESSAGE_LF;
  } else {
    M->type |= LEV_TX_ADD_MESSAGE_MF;
  }

  int extra_len = 4 * extra_mask_intcount (fmask);
  int w_extra_len = 4 * extra_mask_intcount (wmask);

  E = alloc_log_event (M->type, sizeof (struct lev_add_message) + w_extra_len + M->text_len + 1, M->user_id);
  memcpy (&E->legacy_id, &M->legacy_id, (char *) &M->text - (char *) &M->legacy_id);

  ptr = E->text + w_extra_len;
  if (!text) {
    text = M->text + extra_len;
  }

  int *E_extra = E->extra, *M_extra = M->extra;

  for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
    if (wmask & i) {
      if (i < 256) {
        *E_extra++ = *M_extra++;
      } else {
        *(long long *) E_extra = *(long long *) M_extra;
        E_extra += 2;
        M_extra += 2;
      }
    } else if (fmask & i) {
      M_extra += (i < 256 ? 1 : 2);
    }
  }

  assert ((char *) E_extra == ptr);

  copy_adjust_text (ptr, text, M->text_len);

  return store_new_message (E, random_tag);
}

/* replaces text of an EXISTING message, returns local_id on success, 0 if bad user_id, negative on error */
int do_replace_message_text (int user_id, int local_id, char *text, int text_len) {
  if (local_id <= 0 || conv_uid (user_id) < 0 || text_len < 0 || text_len >= MAX_TEXT_LEN) { 
    return -1; 
  }

  if (text_len < 4096) {
    struct lev_replace_text *E = alloc_log_event (LEV_TX_REPLACE_TEXT + text_len, offsetof (struct lev_replace_text, text) + text_len + 1, user_id);
    E->local_id = local_id;
    copy_adjust_text (E->text, text, text_len);
    return replace_message_text ((struct lev_replace_text_long *) E);
  } else {
    struct lev_replace_text_long *E = alloc_log_event (LEV_TX_REPLACE_TEXT_LONG, offsetof (struct lev_replace_text_long, text) + text_len + 1, user_id);
    E->local_id = local_id;
    E->text_len = text_len;
    copy_adjust_text (E->text, text, text_len);
    return replace_message_text (E);
  }
}


int do_delete_message (int user_id, int local_id) {
  if (verbosity > 1) {
    fprintf (stderr, "do_delete_message(%d,%d):\n", user_id, local_id);
  }
  int res = adjust_message (user_id, local_id, -1, -1, 0);
  if (verbosity > 1) {
    fprintf (stderr, "adjust_message returned %d\n", res);
  }
  if (res < 0) {
    return res;
  }
  if ((res & 1) || write_all_events) {
    struct lev_del_message *E = alloc_log_event (LEV_TX_DEL_MESSAGE, sizeof (struct lev_del_message), user_id);
    E->local_id = local_id;
  }
  return res & 1;
}

int do_delete_first_messages (int user_id, int first_local_id) {
  if (verbosity > 1) {
    fprintf (stderr, "do_delete_first_messages(%d,%d):\n", user_id, first_local_id);
  }
  int res = delete_first_messages (user_id, first_local_id);
  if (verbosity > 1) {
    fprintf (stderr, "delete_first_messages returned %d\n", res);
  }
  if (res <= 0) {
    return res;
  }
  if (res > 0) {
    struct lev_del_first_messages *E = alloc_log_event (LEV_TX_DEL_FIRST_MESSAGES, sizeof (struct lev_del_first_messages), user_id);
    E->first_local_id = res;
  }
  return 1;
}

int do_set_flags (int user_id, int local_id, int flags) {
  if (verbosity > 1) {
    fprintf (stderr, "do_set_flags(%d,%d,%d):\n", user_id, local_id, flags);
  }
  if (flags & -0x10000) {
    return -1;
  }
  int res = adjust_message (user_id, local_id, ~flags, flags, 0);
  if (verbosity > 1) {
    fprintf (stderr, "adjust_message returned %d\n", res);
  }
  if (res < 0) {
    return res;
  }
  if ((res & 1) || write_all_events) {
    if (flags & -0x100) {
      struct lev_set_flags_long *E = alloc_log_event (LEV_TX_SET_MESSAGE_FLAGS_LONG, sizeof (struct lev_set_flags_long), user_id);
      E->local_id = local_id;
      E->flags = flags;
    } else {
      struct lev_set_flags *E = alloc_log_event (LEV_TX_SET_MESSAGE_FLAGS + flags, sizeof (struct lev_set_flags), user_id);
      E->local_id = local_id;
    }
  }
  return res > 0;
}

int do_set_values (int user_id, int local_id, struct value_data *V) {
  if (verbosity > 1) {
    fprintf (stderr, "do_set_values(%d,%d,%d:%d,%d,...):\n", user_id, local_id, V->fields_mask, V->zero_mask, V->fields_mask ? V->data[0] : 0);
  }
  if (V->fields_mask != V->zero_mask || (V->fields_mask & ~MAX_EXTRA_MASK)) {
    return -1;
  }
  if (!(V->fields_mask & write_extra_mask)) {
    return 0;
  }
  if (conv_uid (user_id) < 0 || local_id <= 0) {
    return -1;
  }
  V = convert_value_data (V, 1, write_extra_mask);
  int s = 4 * extra_mask_intcount (V->fields_mask);
  struct lev_set_extra_fields *E = alloc_log_event (LEV_TX_SET_EXTRA_FIELDS + V->fields_mask, 12 + s, user_id);
  E->local_id = local_id;
  memcpy (E->extra, V->data, s);
  return adjust_message_values (user_id, local_id, V) > 0;
}

int do_incr_value (int user_id, int local_id, int value_id, int incr) {
  if (verbosity > 1) {
    fprintf (stderr, "do_incr_value(%d,%d,%d,%d):\n", user_id, local_id, value_id, incr);
  }
  if (conv_uid (user_id) < 0 || local_id <= 0 || (unsigned) value_id >= 8) {
    return -1;
  }
  if (!((write_extra_mask >> value_id) & 1)) {
    return 0;
  }
  struct lev_incr_field *E = alloc_log_event (LEV_TX_INCR_FIELD + value_id, 16, user_id);
  E->local_id = local_id;
  E->value = incr;

  if (!(read_extra_mask & (1 << value_id))) {
    return 1;
  }

  struct value_data *V = alloc_value_data (1 << value_id);
  V->zero_mask = 0;
  V->data[0] = incr;

  return adjust_message_values (user_id, local_id, V) > 0;
}


int do_incr_value_long (int user_id, int local_id, int value_id, long long incr) {
  if (verbosity > 1) {
    fprintf (stderr, "do_incr_value_long(%d,%d,%d,%lld):\n", user_id, local_id, value_id, incr);
  }
  if (conv_uid (user_id) < 0 || local_id <= 0 || value_id < 8 || value_id >= 12) {
    return -1;
  }
  if (!((write_extra_mask >> value_id) & 1)) {
    return 0;
  }
  struct lev_incr_field_long *E = alloc_log_event (LEV_TX_INCR_FIELD_LONG + value_id, 20, user_id);
  E->local_id = local_id;
  E->value = incr;

  if (!(read_extra_mask & (1 << value_id))) {
    return 1;
  }

  struct value_data *V = alloc_value_data (1 << value_id);
  V->zero_mask = 0;
  memcpy (&V->data, &incr, 8);

  return adjust_message_values (user_id, local_id, V) > 0;
}

int do_change_mask (int new_mask) {
  if (verbosity > 1) {
    fprintf (stderr, "do_change_mask(%d)\n", new_mask);
  }
  if (new_mask & ~MAX_EXTRA_MASK) {
    return -1;
  }
  if (new_mask == write_extra_mask) {
    return 0;
  }
  alloc_log_event (LEV_CHANGE_FIELDMASK_DELAYED, 8, new_mask);
  change_extra_mask (new_mask);

  return 1;
}

/* force=-1 : possibly returns flags of nonexistent messages;
   force=0  : don't read disk if flags not available (returns -2 instead)
   force=1  : read disk, return precise result
 */
int get_message_flags (int user_id, int local_id, int force) {
  user_t *U = get_user (user_id);
  tree_t *T, *O;
  int flags, res;
  struct imessage M;

  if (local_id <= 0 || conv_uid (user_id) < 0) { 
    return -1; 
  }
  if (U) {
    T = tree_lookup (U->msg_tree, local_id);
    O = tree_lookup (U->delayed_tree, local_id);
    if (O && O->flags == -1) {
      return -1;
    }
    if (O && force < 0 && ((O->clear_mask | O->set_mask | -0x10000) == -1)) {
      return O->set_mask;
    }
    if (T) {
      switch (T->y & 7) {
      case TF_MINUS:
        return -1;
      case TF_PLUS:
      case TF_REPLACED:
        assert (!O);
        flags = T->msg->flags;
        break;
      case TF_ZERO:
        flags = T->flags;
        break;
      case TF_ZERO_PRIME:
        flags = T->value->flags;
        break;
      default:
        assert (0);
      }
      if (O) {
        flags = (flags & ~O->clear_mask) | O->set_mask;
      }
      return flags;
    }
  }

  res = load_message (&M, user_id, local_id, force > 0);

  if (res < 0) {
    return res;
  }
  if (!res) {
    return -1;
  }

  return M.flags;
}

int do_incr_flags (int user_id, int local_id, int flags) {
  if (verbosity > 1) {
    fprintf (stderr, "do_incr_flags(%d,%d,%d):\n", user_id, local_id, flags);
  }
  if (flags & -0x10000) {
    return -1;
  }
  int res = adjust_message (user_id, local_id, 0, flags, 0);
  if (verbosity > 1) {
    fprintf (stderr, "adjust_message returned %d\n", res);
  }
  if (res < 0) {
    return res;
  }
  if ((res & 1) || write_all_events) {
    if (flags & -0x100) {
      struct lev_set_flags_long *E = alloc_log_event (LEV_TX_INCR_MESSAGE_FLAGS_LONG, sizeof (struct lev_set_flags_long), user_id);
      E->local_id = local_id;
      E->flags = flags;
    } else {
      struct lev_set_flags *E = alloc_log_event (LEV_TX_INCR_MESSAGE_FLAGS + flags, sizeof (struct lev_set_flags), user_id);
      E->local_id = local_id;
    }
  }
  return get_message_flags (user_id, local_id, -1);
}

int do_decr_flags (int user_id, int local_id, int flags) {
  if (verbosity > 1) {
    fprintf (stderr, "do_decr_flags(%d,%d,%d):\n", user_id, local_id, flags);
  }
  if (flags & -0x10000) {
    return -1;
  }
  int res = adjust_message (user_id, local_id, flags, 0, 0);
  if (verbosity > 1) {
    fprintf (stderr, "adjust_message returned %d\n", res);
  }
  if (res < 0) {
    return res;
  }
  if ((res & 1) || write_all_events) {
    if (flags & -0x100) {
      struct lev_set_flags_long *E = alloc_log_event (LEV_TX_DECR_MESSAGE_FLAGS_LONG, sizeof (struct lev_set_flags_long), user_id);
      E->local_id = local_id;
      E->flags = flags;
    } else {
      struct lev_set_flags *E = alloc_log_event (LEV_TX_DECR_MESSAGE_FLAGS + flags, sizeof (struct lev_set_flags), user_id);
      E->local_id = local_id;
    }
  }
  return get_message_flags (user_id, local_id, -1);
}

/* force=-1 : possibly returns values of nonexistent messages;
   force=0  : don't read disk if value not available (returns -2 instead)
   force=1  : read disk, return precise result
 */
int get_message_value (int user_id, int local_id, int value_id, int force, long long *result) {
  user_t *U = get_user (user_id);
  tree_t *T, *O;
  int res;
  struct imessage M;

  if (local_id <= 0 || conv_uid (user_id) < 0 || (unsigned) value_id >= 12) { 
    return -1; 
  }

  if (U) {
    T = tree_lookup (U->msg_tree, local_id);
    if (T) {
      switch (T->y & 7) {
      case TF_MINUS:
        return -1;
      case TF_PLUS:
      case TF_REPLACED:
        if (!((read_extra_mask >> value_id) & 1)) {
          *result = 0;
          return 1;
        }
        int offset = extra_mask_intcount (index_extra_mask & ((1 << value_id) - 1));
        *result = value_id < 8 ? T->msg->extra[offset] : *(long long *)(T->msg->extra + offset);
        return 1;
      case TF_ZERO:
        if (!((read_extra_mask >> value_id) & 1)) {
          *result = 0;
          return 1;
        }
        break;
      case TF_ZERO_PRIME:
        if (!((read_extra_mask >> value_id) & 1)) {
          *result = 0;
          return 1;
        }
        if ((T->value->fields_mask >> value_id) & 1) {
          int *source = T->value->data + extra_mask_intcount (T->value->fields_mask & ((1 << value_id) - 1));
          *result = value_id < 8 ? *source : *(long long *) source;
          return 1;
        }
        break;
      default:
        assert (0);
      }
    } else {
      O = tree_lookup (U->delayed_tree, local_id);
      if (O && O->flags == -1) {
        return -1;
      }
      if (force < 0) {
        if (!((read_extra_mask >> value_id) & 1)) {
          *result = 0;
          return 1;
        }
        O = tree_lookup (U->delayed_value_tree, local_id);
        if (O && ((O->value->zero_mask >> value_id) & 1)) {
          int *source = O->value->data + extra_mask_intcount (O->value->fields_mask & ((1 << value_id) - 1));
          *result = value_id < 8 ? *source : *(long long *) source;
          return 1;
        }
      }
    }
  }

  res = load_message (&M, user_id, local_id, force > 0);

  if (res < 0) {
    return res;
  }
  if (!res) {
    return -1;
  }

  assert (!M.msg && (!M.value_actions || !(M.value_actions->fields_mask & (1 << value_id))));
  if (!M.fmsg) {
    return -1;
  }

  int *M_extra = M.fmsg->data;

  int mflags = M.fmsg->flags;

  if (mflags & TXF_HAS_LEGACY_ID) {
    M_extra++;
  }
  if (mflags & TXF_HAS_LONG_LEGACY_ID) {
    M_extra += 2;
    assert (!(mflags & TXF_HAS_LEGACY_ID));
  }
  if (mflags & TXF_HAS_PEER_MSGID) {
    M_extra++;
  }

  mflags >>= 16;
  mflags &= MAX_EXTRA_MASK;
  assert (!(mflags & ~index_extra_mask));

  if (!(read_extra_mask & mflags & (1 << value_id))) {
    *result = 0;
    return 1;
  }

  M_extra += extra_mask_intcount (mflags & ((1 << value_id) - 1));
  *result = (value_id < 8) ? *M_extra : *(long long *) M_extra;
  return 1;
}


/* data access */

struct sublist_descr *get_peer_sublist_type (void) {
  return &PeerFlagFilter;
}

int get_sublist_types (struct sublist_descr *A) {
  assert (sublists_num >= 0 && sublists_num <= 16);
  memcpy (A, Sublists, sizeof (struct sublist_descr) * sublists_num);
  return sublists_num;
}

int get_local_id_by_random_tag (int user_id, int random_tag) {
  int i;
  user_t *U = get_user (user_id);

  if (!U || random_tag <= 0) {
    return 0;
  }
  for (i = 0; i < MAX_INS_TAGS; i++) {
    if (U->insert_tags[i][0] == random_tag) {
      return U->insert_tags[i][1];
    }
  }
  return 0;
}

int get_local_id_by_legacy_id (int user_id, long long legacy_id) {
  user_t *U = get_user (user_id);
  ltree_t *T;
  struct file_user_list_entry *D;
  char *metafile;

  if (!legacy_id) {
    return 0;
  }

  if (U) {
    T = ltree_lookup_legacy (U->legacy_tree, legacy_id); // may be unstable
    if (T) {
      return T->z;
    }
    D = U->dir_entry;
    metafile = get_user_metafile (U);
  } else {
    D = lookup_user_directory (user_id);
    metafile = 0;
  }

  if (!D) {
    return 0;
  }

  if (!metafile) {
    core_mf_t *M = load_user_metafile (user_id);
    if (!M) {
      return -2;
    }
    metafile = M->data;
  }

  if (!index_long_legacy_id) {
    int a = -1, b, c, *X;
    X = (int *) (metafile + UserHdr->legacy_list_offset);
    b = ((UserHdr->directory_offset - UserHdr->legacy_list_offset) >> 3);
    while (b - a > 1) {
      c = ((a + b) >> 1);
      if (X[2*c] <= legacy_id) {
	a = c;
      } else {
	b = c;
      }
    }
    
    if (a >= 0 && X[2*a] == legacy_id) {
      return X[2*a+1];
    }
  } else {
    int a = -1, b, c;
    int *X;
    X = (int *) (metafile + UserHdr->legacy_list_offset);
    b = ((UserHdr->directory_offset - UserHdr->legacy_list_offset) / 12);
    while (b - a > 1) {
      c = ((a + b) >> 1);
      if (*(long long *)(X + 3*c) <= legacy_id) {
	a = c;
      } else {
	b = c;
      }
    }
    
    if (a >= 0 && *(long long *)(X + 3*a) == legacy_id) {
      return X[3*a+2];
    }
  }
  
  return 0;
}

/* single message access */

int check_message_size (struct file_message *M, int size, char **text_ptr) {
  int k = 0;
  unsigned char *ptr;
  int len, extra_fields;
  if (M->flags & TXF_HAS_LEGACY_ID) { k++; }
  if (M->flags & TXF_HAS_LONG_LEGACY_ID) { k += 2; assert (!(M->flags & TXF_HAS_LEGACY_ID)); }
  if (M->flags & TXF_HAS_PEER_MSGID) { k++; }
  extra_fields = (M->flags >> 16) & MAX_EXTRA_MASK;
  assert (!(extra_fields & ~index_extra_mask));
  k += extra_mask_intcount (extra_fields);
  assert (size >= sizeof (struct file_message) + k * 4);
  ptr = (unsigned char *) (M->data + k);
  len = *ptr++;
  if (len == 254) {
    len = *((unsigned short *) ptr);
    ptr += 2;
  } else if (len == 255) {
    len = *((int *) ptr);
    ptr += 4;
    assert (len >= 0 && len < size);
  }
  if (text_ptr) {
    *text_ptr = (char *) ptr;
  }
  assert (sizeof (struct file_message) + k*4 + len <= size);
  return len;
}

struct file_message *user_metafile_message_lookup (char *metafile, int metafile_size, int local_id, user_t *U) {
  struct file_user_header *H = (struct file_user_header *) metafile;
  struct file_message *M;
  if (!metafile || metafile_size <= 0) {
    fprintf (stderr, "user_metafile_message_lookup (%p, %d, %d, %p [%d])\n", metafile, metafile_size, local_id, U, U->user_id);
  }
  assert (metafile && metafile_size > 0);
  if (local_id < H->user_first_local_id || local_id > H->user_last_local_id) {
    return 0;
  }
  int *p = (int *) (metafile + H->directory_offset) + (local_id - H->user_first_local_id);
  if (p[0] == p[1]) {
    return 0;
  }
  assert (p[0] >= H->data_offset && p[0] < p[1] && p[1] <= H->extra_offset);
  M = (struct file_message *) (metafile + p[0]);
  check_message_size (M, p[1] - p[0], 0);
  return M;
}

/* 0 = none, 1 = ok, -1 = error, -2 = need load */
int load_message_internal (struct imessage *M, user_t *U, struct file_user_list_entry *D, int user_id, int local_id, int force) {
  tree_t *T;
  char *metafile;

//  fprintf (stderr, "load_message(%d,%d):\n", user_id, local_id);

  /* U = get_user (user_id); */
  T = 0;
  M->msg = 0;
  M->fmsg = 0;
  M->m_extra = 0;
  M->flags = -1;
  M->value_actions = 0;
  M->edit_text = 0;

  if (U) {
    /* D = U->dir_entry; */
    T = tree_lookup (U->msg_tree, local_id);
    if (T) {
      switch (T->y & 7) {
      case TF_PLUS:
      case TF_REPLACED:
        M->msg = T->msg;
        M->flags = T->msg->flags;
        return 1;
      case TF_MINUS:
        return 0;
      case TF_ZERO:
        M->flags = T->flags;
        break;
      case TF_ZERO_PRIME:
        M->flags = T->value->flags;
        M->value_actions = T->value;
        break;
      default:
        assert (0);
      }
    }
    tree_t *X = tree_lookup (U->edit_text_tree, local_id);
    if (X) {
      M->edit_text = X->edit_text;
    }
  } else {
    /* D = lookup_user_directory (user_id); */
    T = 0;
  }
  if (!D || local_id > D->user_last_local_id) {
    assert (!T);
    return 0;
  }
  if (!U || !get_user_metafile (U)) {
    if (!force) {
      return -2;
    }
    core_mf_t *M = load_user_metafile (user_id);
    if (!M) {
      return -2;
    }
    metafile = M->data;
  } else {
    metafile = get_user_metafile (U);
    assert (U->mf && U->mf->len >= D->user_data_size);
  }
  /* now metafile points to user metafile, and we have to get M->fmsg from there */
  M->fmsg = user_metafile_message_lookup (metafile, D->user_data_size, local_id, U);
  assert (!T || M->fmsg);
  if (M->flags < 0 && M->fmsg) {
    M->flags = M->fmsg->flags & 0xffff;
  }
  return M->fmsg ? 1 : 0;
}

/* 0 = none, 1 = ok, -1 = error, -2 = need load */
int load_message (struct imessage *M, int user_id, int local_id, int force) {
  user_t *U;
  struct file_user_list_entry *D;

  if (conv_uid (user_id) < 0 || local_id <= 0) {
    return -1;
  }
  
  U = get_user (user_id);
  if (U) {
    D = U->dir_entry;
  } else {
    D = lookup_user_directory (user_id);
  }

  return load_message_internal (M, U, D, user_id, local_id, force);
}


static inline struct file_word_dictionary_entry *word_code_lookup (struct word_dictionary *D, unsigned code, int *l) {
  int a = -1, b = D->max_bits, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (D->first_codes[c] <= code) { a = c; } else { b = c; }
  }
  assert (a >= 0);
  *l = a + 1;
  return D->code_ptr[a][code >> (31 - a)];
}

static inline int char_code_lookup (struct char_dictionary *D, unsigned code, int *l) {
  int a = -1, b = D->max_bits, c;
  while (b - a > 1) {
    c = ((a + b) >> 1);
    if (D->first_codes[c] <= code) { a = c; } else { b = c; }
  }
  assert (a >= 0);
  *l = a + 1;
  return D->code_ptr[a][code >> (31 - a)];
}

static inline int feed_byte (int c, int *state) {
  if (*state == 2) {
    return 0;
  }
  if (*state == 5) {
    if (c != 1 && c != 2) {
      *state = 2;
      return 1;
    }
    *state = 1;
    return 0;
  }
  if (c == 9) {
    *state = 5;
  }
  return 0;
}

int unpack_message_text (struct file_message *FM, char *to, int max_text_len, int fetch_filter, int *msg_len_ptr, int *kludges_len_ptr) {
  int k, bits, state;
  char *packed_ptr, *rptr, *rptr_e, *wptr, *wptr_e, *kptr;
  int packed_len = check_message_size (FM, 0x7fffffff, &packed_ptr);

  rptr = packed_ptr;
  rptr_e = packed_ptr + packed_len;
  wptr = to;
  wptr_e = wptr + max_text_len;
  *msg_len_ptr = *kludges_len_ptr = max_text_len;

  fetch_filter &= 3;

  if (max_text_len <= 0 || !fetch_filter) {
    return 1;
  }

  bits = packed_len * 8 - 1;
  k = rptr_e[-1];
  assert (k);
  while (!(k & 1)) { k >>= 1; bits--; }

  unsigned t = bswap_32 (*((unsigned *) rptr));
  unsigned z = (((unsigned char) rptr[4]) << 1) + 1;
  rptr += 5;

  kptr = to;
  state = 5;

#define	LOAD_BITS(__n)	{ int n=(__n); bits-=n; do {if (!(z&0xff)) {z=((*rptr++)<<1)+1;} t<<=1; if (z&0x100) t++; z<<=1;} while (--n);}

  while (bits > 0) {
    struct file_word_dictionary_entry *W;
    int c, N, l;

    if (state == 2 && fetch_filter <= 1) {
      bits = 0;
      break;
    }

    /* decode one word */
    W = word_code_lookup (&WordDict, t, &l);
    LOAD_BITS (l);
    if (W->str_len != 2 || W->str[0]) {
      char *p = W->str, *q = p + W->str_len;
      while (p < q) {
        c = *p++;
        if (feed_byte (c, &state)) {
          *kludges_len_ptr = wptr - kptr;
        }
        if (state & fetch_filter) {
          *wptr++ = c;
        }
        if (wptr == wptr_e) {
          assert (bits >= 0);
          return 1;
        }
      }
    } else {
      /* special case: word not in dictionary, decode N word characters */
      N = (unsigned char) W->str[1];
      assert (N > 0);
      do {
        c = char_code_lookup (&WordCharDict, t, &l);
        LOAD_BITS (l);
        if (feed_byte (c, &state)) {
          *kludges_len_ptr = wptr - kptr;
        }
        if (state & fetch_filter) {
          *wptr++ = c;
        }
        if (wptr == wptr_e) {
          assert (bits >= 0);
          return 1;
        }
      } while (--N);
    }

    if (bits <= 0) {
      break;
    }

    /* decode one not-word */
    W = word_code_lookup (&NotWordDict, t, &l);
    LOAD_BITS (l);
    if (W->str_len != 2 || W->str[0]) {
      char *p = W->str, *q = p + W->str_len;
      while (p < q) {
        c = *p++;
        if (feed_byte (c, &state)) {
          *kludges_len_ptr = wptr - kptr;
        }
        if (state & fetch_filter) {
          *wptr++ = c;
        }
        if (wptr == wptr_e) {
          assert (bits >= 0);
          return 1;
        }
      }
    } else {
      /* special case: not-word not in dictionary, decode N not-word characters */
      N = (unsigned char) W->str[1];
      assert (N > 0);
      do {
        c = char_code_lookup (&NotWordCharDict, t, &l);
        LOAD_BITS (l);
        if (feed_byte (c, &state)) {
          *kludges_len_ptr = wptr - kptr;
        }
        if (state & fetch_filter) {
          *wptr++ = c;
        }
        if (wptr == wptr_e) {
          assert (bits >= 0);
          return 1;
        }
      } while (--N);
    }
  }
  assert (!bits && wptr <= wptr_e);

#undef LOAD_BITS

  *msg_len_ptr = wptr - kptr;
  if (state != 2) {
    *kludges_len_ptr = *msg_len_ptr;
  }

  if (verbosity > 2) {
    fprintf (stderr, "decoded text: %d bytes from %d bytes\n%.*s\n", *msg_len_ptr, packed_len, *msg_len_ptr, to);
  }

  return 1;
}

int unpack_message_long (struct imessage_long *M, int max_text_len, int fetch_filter) {
  int k, bits;

  M->m_extra = 0;

  if (M->msg && M->msg->global_id >= first_extra_global_id && M->msg->global_id > last_global_id - CYCLIC_IP_BUFFER_SIZE) {
    int x = M->msg->global_id & (CYCLIC_IP_BUFFER_SIZE - 1);
    M->m_extra = &LastMsgExtra[x];
  }

  if (M->msg) {
    M->builtin_msg.flags = M->msg->flags;
    return 1;
  }
  assert (M->fmsg);
  M->msg = &M->builtin_msg;
  if (M->builtin_msg.flags == -1) {
    M->builtin_msg.flags = M->fmsg->flags & 0xffff;
  }
  M->msg->peer_id = M->fmsg->peer_id;
  M->msg->date = M->fmsg->date;
  memset (M->msg->extra, 0, text_shift);

  k = 0;
  if (M->fmsg->flags & TXF_HAS_LEGACY_ID) {
    M->msg->legacy_id = M->fmsg->data[k++];
    assert (!(M->fmsg->flags & TXF_HAS_LONG_LEGACY_ID));
  } else if (M->fmsg->flags & TXF_HAS_LONG_LEGACY_ID) {
    M->msg->legacy_id = *(long long *)(M->fmsg->data + k);
    k += 2;
  } else {
    M->msg->legacy_id = 0;
  }
  if (M->fmsg->flags & TXF_HAS_PEER_MSGID) {
    M->msg->peer_msg_id = M->fmsg->data[k++];
  } else {
    M->msg->peer_msg_id = 0;
  }
  bits = (M->fmsg->flags >> 16) & MAX_EXTRA_MASK;

  if (bits || M->value_actions) {

    int *M_extra = M->fmsg->data + k;
    int *V_extra = 0;
    int *W_extra = M->msg->extra;
    int V_fields = 0;

    if (M->value_actions) {
      V_extra = M->value_actions->data;
      V_fields = M->value_actions->fields_mask;
    }

    int i;

    assert (!(bits & ~index_extra_mask));

    for (i = 1; i < MAX_EXTRA_MASK; i <<= 1) {
      if (read_extra_mask & i) {
        if (V_fields & i) {
          if (i < 256) {
            *W_extra = *V_extra;
          } else {
            *(long long *) W_extra = *(long long *) V_extra;
          }
        } else if (bits & i) {
          if (i < 256) {
            *W_extra = *M_extra;
          } else {
            *(long long *) W_extra = *(long long *) M_extra;
          }
        }
      }
      if (bits & i) {
        M_extra += (i < 256 ? 1 : 2);
      }
      if (V_fields & i) {
        V_extra += (i < 256 ? 1 : 2);
      }
      if (index_extra_mask & i) {
        W_extra += (i < 256 ? 1 : 2);
      }
    }
  }

  M->msg->global_id = 0;

  if (M->edit_text) {
    M->builtin_msg.text[text_shift] = 0;
    M->builtin_msg.len = 0;
    M->builtin_msg.kludges_size = 0;
    return 1;
  }

  unpack_message_text (M->fmsg, M->builtin_msg.text + text_shift, max_text_len, fetch_filter, &M->builtin_msg.len, &M->builtin_msg.kludges_size);

  return 1;
}

int load_message_long (struct imessage_long *M, int user_id, int local_id, int max_text_len, int fetch_filter) {
  int r = load_message ((struct imessage *) M, user_id, local_id, 1);
  if (r <= 0) {
    return r;
  }
  M->builtin_msg.user_id = user_id;
  unpack_message_long (M, max_text_len, fetch_filter);

  return r;
}

/* other data access functions */

int R[MAX_TMP_RES], R_cnt;

int fetch_msg_data_aux (int *A, struct imessage *M, int mode) {
  int i = 0;
  if (mode & 32) {
    A[i++] = M->flags;
  }
  if (M->msg) {
    if (mode & 64) {
      A[i++] = M->msg->date;
    }
    if (mode & 128) {
      A[i++] = M->msg->peer_id;
    }
  } else {
    assert (M->fmsg);
    if (mode & 64) {
      A[i++] = M->fmsg->date;
    }
    if (mode & 128) {
      A[i++] = M->fmsg->peer_id;
    }
  }
  return i;
}

int get_msg_sublist (int user_id, int and_mask, int xor_mask, int from, int to) {
  return get_msg_sublist_ext (user_id, and_mask, xor_mask, 0, from, to);
}

int prepare_msg_sublist (int user_id, int and_mask, int xor_mask, listree_t **X, int from, int to) {
  user_t *U;
  struct file_user_list_entry *D;
  int S;
  int k;

  //fprintf (stderr, "get_msg_sublist_ext(%d,%d,%d:%d,%d,%d)\n", user_id, mode, and_mask, xor_mask, from, to);

  if (conv_uid (user_id) < 0 || !and_mask) {
    return -1;
  }

  for (k = 0; k < sublists_num; k++) {
    if (Sublists[k].and_mask == and_mask && Sublists[k].xor_mask == xor_mask) {
      break;
    }
  }

  if (k == sublists_num) {
    return -3;
  }

  U = get_user (user_id);
  *X = 0;
  S = 0;

  if (U) {
    D = U->dir_entry;
    *X = U->Sublists + k;
    if (U->delayed_tree) {
      /* need to load user metafile to perform delayed operations */
      if (!load_user_metafile (user_id)) {
        return -2;
      }
    }
  } else {
    D = lookup_user_directory (user_id);
  }

  if (!*X && !D) {
    return 0;
  }

  if (!*X) {
    S = (D ? D->user_sublists_size[idx_sublists_offset+k] : 0);
    if (!from || !to || !S) {
      return S;
    }
    if ((from < -S || from > S) && (to < -S || to > S)) {
      return S;
    }
    /* need to load metafile here */
    if (!load_user_metafile (user_id)) {
      return -2;
    }

    U = get_user (user_id);
    assert (U && get_user_metafile (U));
    *X = U->Sublists + k;
  }

  assert (!U || !U->delayed_tree);
  return -4;
}

int get_msg_sublist_ext (int user_id, int and_mask, int xor_mask, int mode, int from, int to) {
  static listree_t *X;
  int extras = __builtin_popcount (mode) + 1;

  R_cnt = 0;

  if ((mode & ~0xe0) != 0) {
    return -1;
  }

  //fprintf (stderr, "get_msg_sublist_ext(%d,%d,%d:%d,%d,%d)\n", user_id, mode, and_mask, xor_mask, from, to);
  int res = prepare_msg_sublist (user_id, and_mask, xor_mask, &X, from, to);

  if (res != -4) {
    return res;
  }

  int S = listree_get_size (X);

  assert (S >= 0);
  if (!S) {
    return 0;
  }

  if (!from || !to) {
    return S;
  }
  if (from < 0) {
    from += S;
  } else {
    from--;
  }
  if (to < 0) {
    to += S;
  } else {
    to--;
  }

  if (from <= to) {
    to = to - from + 1;
    if ((unsigned) to > MAX_RES / extras) {
      to = MAX_RES / extras;
    }
    RA = R;
    R_cnt = listree_get_range (X, from, to);
    if (R_cnt == -2) {
      /* user metafile needed */
      if (!load_user_metafile (user_id)) {
        return -2;
      }
      RA = R;
      R_cnt = listree_get_range (X, from, to);
      assert (R_cnt >= 0);
    }
  } else {
    to = from - to + 1;
    if ((unsigned) to > MAX_RES / extras) {
      to = MAX_RES / extras;
    }
    RA = R;
    R_cnt = listree_get_range_rev (X, S - 1 - from, to);
    if (R_cnt == -2) {
      /* user metafile needed */
      if (!load_user_metafile (user_id)) {
        return -2;
      }
      RA = R;
      R_cnt = listree_get_range_rev (X, S - 1 - from, to);
      assert (R_cnt >= 0);
    }
  }

  if (!(R_cnt >= 0 && R_cnt <= to)) {
    fprintf (stderr, "get_msg_sublist_ext(%d,%d:%d): R_cnt=%d, from=%d, to=%d, S=%d\n", user_id, and_mask, xor_mask, R_cnt, from, to, S);

  }
  assert (R_cnt >= 0 && R_cnt <= to);

  if (mode) {
    user_t *U = get_user (user_id);
    struct file_user_list_entry *D = U ? U->dir_entry : lookup_user_directory (user_id);
    assert (R_cnt <= MAX_RES / extras);
    int i = 0;
    struct imessage M;
    for (i = R_cnt - 1; i >= 0; i--) {
      int local_id = R[i];
      assert (load_message_internal (&M, U, D, user_id, local_id, 0) == 1);
      R[i * extras] = local_id;
      assert (fetch_msg_data_aux (R + i * extras + 1, &M, mode) == extras - 1);
    }
    R_cnt *= extras;
  }

  return S;
}

/* returns position = (number of entries <= local_id) in given sublist OR a negative number on error */
int get_msg_sublist_pos (int user_id, int and_mask, int xor_mask, int local_id) { // ???
  static listree_t *X;
  int S;
  static int rec_cnt = 0;

  R_cnt = 0;

  int res = prepare_msg_sublist (user_id, and_mask, xor_mask, &X, 1, 1);
  if (res != -4) {
    return res;
  }

  S = listree_get_size (X);
  assert (S >= 0);
  if (!S) {
    return 0;
  }

  res = listree_get_pos (X, local_id, 1);

  if (res == -2) {
    if (!load_user_metafile (user_id)) {
      return -2;
    }
    assert (++rec_cnt == 1);
    res = get_msg_sublist_pos (user_id, and_mask, xor_mask, local_id);
    --rec_cnt;
    return res;
  }

  res++;

  assert (res >= 0 && res <= S);
  return res;
}


int get_top_msglist (int user_id, int from, int to) {
  user_t *U;
  tree_num_t *T;
  struct file_user_list_entry *D;
  int S;

  if (verbosity > 1) {
    fprintf (stderr, "get_top_peers_list(%d,%d,%d)\n", user_id, from, to);
  }

  if (conv_uid (user_id) < 0) {
    return -1;
  }

  U = get_user (user_id);
  R_cnt = 0;

  if (U) {
    D = U->dir_entry;
    if (U->delayed_tree || D) {
      /* need to load user metafile to perform delayed operations */
      if (!load_user_metafile (user_id)) {
        return -2;
      }
    }
  } else {
    D = lookup_user_directory (user_id);
    if (D) {
      U = get_user_f (user_id);
      if (!load_user_metafile (user_id)) {
        return -2;
      }
    }
  }


  if (!U || U->topmsg_tree == NIL_N) {
    return 0;
  }

  T = U->topmsg_tree;

  S = T->N;
  assert (S >= 0);
  if (!S) {
    return 0;
  }

  if (!from || !to) {
    return S;
  }
  if (from < 0) {
    from += S;
  } else {
    from--;
  }
  if (to < 0) {
    to += S;
  } else {
    to--;
  }

  if (from <= to) {
    to = to - from + 1;
    if ((unsigned) to > MAX_RES / 2) {
      to = MAX_RES / 2;
    }
    R_cnt = tree_num_get_range (T, R, from + 1, from + to) - R;
  } else {
    to = from - to + 1;
    if ((unsigned) to > MAX_RES / 2) {
      to = MAX_RES / 2;
    }
    R_cnt = tree_num_get_range_rev (T, R, S - from, S - from + to - 1) - R;
  }

  assert (R_cnt >= 0 && R_cnt <= to * 2);

  return S;
}

int prepare_peer_msglist (int user_id, int peer_id, listree_t *X) {
  user_t *U;
  tree_t *T;
  struct file_user_list_entry *D;
  char *metafile;

  //fprintf (stderr, "get_peer_msglist(%d,%d,%d,%d)\n", user_id, peer_id, from, to);

  if (conv_uid (user_id) < 0 || !peer_id) {
    return -1;
  }

  U = get_user (user_id);
  T = 0;

  if (U) {
    D = U->dir_entry;
    if (U->delayed_tree) {
      /* need to load user metafile to perform delayed operations */
      if (!load_user_metafile (user_id)) {
        return -2;
      }
    }
    T = tree_lookup (U->peer_tree, peer_id);
  } else {
    D = lookup_user_directory (user_id);
  }

  if (!T && !D) {
    return 0;
  }

  X->root = (T ? T->data : NIL);

  if (D) {
    if (!U || !get_user_metafile (U)) {
      core_mf_t *M = load_user_metafile (user_id);
      if (!M) {
        return -2;
      }
      metafile = M->data;
    } else {
      metafile = get_user_metafile (U);
    }
    X->A = fetch_file_peer_list (metafile, peer_id, &X->N);
    X->last_A = (X->N ? X->A[X->N-1] : 0);
    //fprintf (stderr, "peer list size %d: %d %d %d...", X->N, X->A?X->A[0]:-1, X->A?X->A[1]:-1, X->A?X->A[2]:-1);
  } else {
    X->A = 0;
    X->N = 0;
    X->last_A = 0;
  }

  assert (!U || !U->delayed_tree);
  return 1;
}

int get_peer_msglist (int user_id, int peer_id, int from, int to) {
  static listree_t X;
  int S;

  R_cnt = 0;

  int res = prepare_peer_msglist (user_id, peer_id, &X);
  if (res <= 0) {
    return res;
  }

  S = listree_get_size (&X);
  assert (S >= 0);
  if (!S) {
    return 0;
  }

  if (!from || !to) {
    return S;
  }
  if (from < 0) {
    from += S;
  } else {
    from--;
  }
  if (to < 0) {
    to += S;
  } else {
    to--;
  }

  if (from <= to) {
    to = to - from + 1;
    if ((unsigned) to > MAX_RES) {
      to = MAX_RES;
    }
    RA = R;
    R_cnt = listree_get_range (&X, from, to);
  } else {
    to = from - to + 1;
    if ((unsigned) to > MAX_RES) {
      to = MAX_RES;
    }
    RA = R;
    R_cnt = listree_get_range_rev (&X, S - 1 - from, to);
  }

  assert (R_cnt >= 0 && R_cnt <= to);

  return S;
}

/* returns position = (number of entries <= local_id) in given peer_msg_list OR a negative number on error */
int get_peer_msglist_pos (int user_id, int peer_id, int local_id) {
  static listree_t X;
  int S;

  R_cnt = 0;

  int res = prepare_peer_msglist (user_id, peer_id, &X);
  if (res <= 0) {
    return res;
  }

  S = listree_get_size (&X);
  assert (S >= 0);
  if (!S) {
    return 0;
  }

  res = listree_get_pos (&X, local_id, 1) + 1;

  assert (res >= 0 && res <= S);
  return res;
}

/* join peermsg lists */

#define	MAX_JOIN_PEERS	65536

static void int_sort (int *A, int b) {
  int i = 0, j = b;
  int h, t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (A[i] < h) { i++; }
    while (A[j] > h) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  int_sort (A+i, b-i);
  int_sort (A, j);
}

struct iterator_stack_node {
  struct iterator_stack_node *prev;
  tree_ext_t *node;
};

struct iterator {
  int *array_ptr;
  int array_cnt, array_x, tree_x;
  int x;
  struct iterator_stack_node *top;
  tree_ext_t *node;
};

struct iterator_stack_node *isn_alloc, *isn_top, *isn_free;

#define	MAX_IS_NODES	(1L << 20)

static inline void it_relax_x (struct iterator *cur) {
  if (cur->array_x > cur->tree_x) {
    cur->x = cur->array_x;
  } else {
    cur->x = cur->tree_x;
  }
}

static inline void it_advance_array (struct iterator *cur) {
  if (cur->array_cnt) {
    cur->array_x = cur->array_ptr[--cur->array_cnt];
    it_relax_x (cur);
  } else {
    cur->array_x = 0;
    cur->x = cur->tree_x;
  }
}

static inline struct iterator_stack_node *alloc_stack_node (void) {
  if (isn_free) {
    struct iterator_stack_node *S = isn_free;
    isn_free = S->prev;
    return S;
  } else if (isn_alloc < isn_top) {
    return isn_alloc++;
  } else {
    return 0;
  }
}

static inline void free_stack_node (struct iterator_stack_node *S) {
  S->prev = isn_free;
  isn_free = S;
}

static inline void it_push_stack (struct iterator *cur, tree_ext_t *T) {
  struct iterator_stack_node *S = alloc_stack_node ();
  if (S) {
    S->prev = cur->top;
    S->node = T;
    cur->top = S;
  }
}

static inline tree_ext_t *it_pop_stack (struct iterator *cur) {
  struct iterator_stack_node *S = cur->top;
  cur->top = S->prev;
  free_stack_node (S);
  return S->node;
}

static inline void it_go_right (struct iterator *cur, tree_ext_t *T) {
  while (T->right != NIL) {
    it_push_stack (cur, T);
    T = T->right;
  }
  cur->node = T;
  cur->tree_x = T->x;
}

static inline void it_advance_tree (struct iterator *cur) {
  assert (cur->node);
  tree_ext_t *T = cur->node->left;
  if (T != NIL) {
    it_go_right (cur, T);
    it_relax_x (cur);
  } else if (cur->top) {
    T = it_pop_stack (cur);
    cur->node = T;
    cur->tree_x = T->x;
    it_relax_x (cur);
  } else {
    cur->node = 0;
    cur->tree_x = 0;
    cur->x = cur->array_x;
  }
}

static inline void it_tree_remove_minus (struct iterator *cur) {
  while (cur->tree_x == cur->array_x && cur->tree_x > 0) {
    assert ((cur->node->y & 3) == TF_MINUS);
    it_advance_tree (cur);
    it_advance_array (cur);
  }
}

static inline void it_heap_insert (struct iterator **H, int HN, struct iterator *cur) {
  int i = ++HN, j, x = cur->x;
  while (i > 1) {
    j = (i >> 1);
    if (H[j]->x >= x) {
      break;
    }
    H[i] = H[j];
    i = j;
  }
  H[i] = cur;
}

static inline void it_heap_relax (struct iterator **H, int HN, struct iterator *cur) {
  int i = 1, j, x = cur->x;
  while (1) {
    j = (i << 1);
    if (j > HN) {
      break;
    }
    if (j < HN && H[j+1]->x > H[j]->x) {
      j++;
    }
    if (x >= H[j]->x) {
      break;
    }
    H[i] = H[j];
    i = j;
  }
  H[i] = cur;
}

int get_join_peer_msglist (int user_id, int peer_list[], int peers, int limit) {
  R_cnt = 0;

  if (conv_uid (user_id) < 0 || limit <= 0 || limit > MAX_RES || peers < 0 || peers > MAX_JOIN_PEERS) {
    return -1;
  }

  if (!peers) {
    return 0;
  }

  user_t *U = get_user (user_id);
  struct file_user_list_entry *D;
  char *metafile = 0;

  if (U) {
    D = U->dir_entry;
  } else {
    D = lookup_user_directory (user_id);
  }

  if (!D && (!U || !U->peer_tree)) {
    return 0;
  }

  if (D) {
    if (!U || !get_user_metafile (U)) {
      core_mf_t *M = load_user_metafile (user_id);
      if (!M) {
        return -2;
      }
      metafile = M->data;
    } else {
      metafile = get_user_metafile (U);
    }
  }

  assert (!U || !U->delayed_tree);

  int i, tot_size = 0;

  if (verbosity >= 3) {
    fprintf (stderr, "invoked join_peer_msglist (user_id: %d, limit: %d, peers: %d, peers_list:", user_id, limit, peers);
    for (i = 0; i < peers; i++) {
      fprintf (stderr, " %d", peer_list[i]);
    }
    fprintf (stderr, ")\n");
  }

  for (i = 1; i < peers; i++) {
    if (peer_list[i] <= peer_list[i-1]) {
      break;
    }
  }

  if (i < peers) {
    int_sort (peer_list, peers - 1);
    int j = 1;
    for (i = 1; i < peers; i++) {
      if (peer_list[i] > peer_list[i - 1]) {
	peer_list[j++] = peer_list[i];
      }
    }
    peers = j;
  }

  dyn_mark (0);

  assert (dyn_top >= dyn_cur + 1024);
  dyn_cur += 15 & -(long)dyn_cur;

  assert (!((long)dyn_cur & (PTRSIZE - 1)));

  struct iterator *iterators = (struct iterator *)dyn_cur, *cur = iterators;

  for (i = 0; i < peers; i++) {
    tree_t *T = tree_lookup (U->peer_tree, peer_list[i]);
    int N = 0;
    int *A = metafile ? fetch_file_peer_list (metafile, peer_list[i], &N) : 0;
    if (!T && !A) {
      continue;
    }
    assert ((char *)(cur + 1) <= (char *)dyn_top);
    cur->array_ptr = A;
    if (A) {
      cur->array_cnt = N - 1;
      cur->array_x = A[N - 1];
      assert (cur->array_x > 0);
    } else {
      cur->array_cnt = 0;
      cur->array_x = 0;
    }
    cur->top = 0;
    cur->tree_x = 0;
    cur->node = T ? T->data : NIL;
    vkprintf (3, "peer %d, size %d\n", peer_list[i], N + cur->node->delta);
    tot_size += N + cur->node->delta;
    cur++;
  }

  int itn = cur - iterators;
  int HN = 0;
  struct iterator **H = (struct iterator **) cur;

  isn_alloc = (struct iterator_stack_node *) (H + itn + 1);
  isn_top = -1 + (struct iterator_stack_node *) dyn_top;

  assert (isn_alloc <= isn_top);

  if (isn_top - isn_alloc > MAX_IS_NODES) {
    isn_top = isn_alloc + MAX_IS_NODES;
  }
  isn_free = 0;

  for (i = 0, cur = iterators; i < itn; i++, cur++) {
    if (cur->node != NIL) {
      it_go_right (cur, cur->node);
      it_relax_x (cur);
      it_tree_remove_minus (cur);
      if (cur->x > 0) {
	it_heap_insert (H, HN, cur);
	HN++;
      }
    } else {
      cur->tree_x = 0;
      cur->x = cur->array_x;
      it_heap_insert (H, HN, cur);
      HN++;
    }
  }

  while (HN > 0 && R_cnt < limit) {
    cur = H[1];
    if (cur->tree_x > cur->array_x) {
      assert ((cur->node->y & 3) == TF_PLUS);
      R[R_cnt++] = cur->tree_x;
      it_advance_tree (cur);
    } else {
      R[R_cnt++] = cur->array_x;
      it_advance_array (cur);
    }
    it_tree_remove_minus (cur);
    if (cur->x <= 0) {
      if (!--HN) {
	break;
      }
      cur = H[HN + 1];
    }
    it_heap_relax (H, HN, cur);
  }

  dyn_release (0);
  return tot_size;
}

/* persistent history functions */

int get_persistent_timestamp (int user_id) {
  if (!persistent_history_enabled || conv_uid (user_id) < 0) {
    return -1;
  }

  user_t *U = get_user (user_id);

  if (!U) {
    struct file_user_list_entry_search_history *D = (struct file_user_list_entry_search_history *) lookup_user_directory (user_id);
    return D ? D->user_history_max_ts + 1 : 1;
  }
  return U->persistent_ts + 1 + (U->persistent_history ? U->persistent_history->cur_events : 0);
}

/* returns # of new events, or -1 if incorrect user_id/timestamp, or -2 if aio needed */
int get_persistent_history (int user_id, int timestamp, int limit, int *R) {
  if (!persistent_history_enabled || conv_uid (user_id) < 0) {
    return -1;
  }

  user_t *U = get_user (user_id);
  struct file_user_list_entry_search_history *D;
  int cur_ts;

  if (!U) {
    D = (struct file_user_list_entry_search_history *) lookup_user_directory (user_id);
    if (!D) {
      return timestamp == 1 ? 0 : -1;
    }
    cur_ts = D->user_history_max_ts + 1;
  } else {
    D = (struct file_user_list_entry_search_history *) U->dir_entry;
    cur_ts = U->persistent_ts + 1 + (U->persistent_history ? U->persistent_history->cur_events : 0);
  }
  
  if (timestamp < (D ? D->user_history_min_ts : 1) || timestamp > cur_ts) {
    return -1;
  }

  int N = cur_ts - timestamp;
  
  if (!N) {
    return 0;
  }
  if (N > MAX_PERSISTENT_HISTORY_EVENTS) {
    N = MAX_PERSISTENT_HISTORY_EVENTS;
  }
  
  if (limit > 0 && N > limit) {
    N = limit;
  }

  int L = N;

  if (D && timestamp <= D->user_history_max_ts) {
    if (!U) {
      U = get_user_f (user_id);
      assert (U);
    }
    if (!load_user_metafile (user_id) | !load_history_metafile (user_id)) {
      return -2;
    }
    struct file_history_header *H = (struct file_history_header *) get_history_metafile (U);
    int *ptr = H->history + (timestamp - D->user_history_min_ts) * 2;
    int M = D->user_history_max_ts - timestamp + 1;
    if (M > N) {
      M = N;
    }
    L = N - M;
    timestamp += M;
    while (M --> 0) {
      int b = *ptr++;
      int a = *ptr++;
      *R++ = ((unsigned) a >> 24);
      *R++ = b;
      *R++ = a & 0xffffff;
    }
  }

  if (!L) {
    return N;
  }

  assert (U && U->persistent_history);
  timestamp -= U->persistent_ts + 1;
  assert (timestamp >= 0 && timestamp + L <= U->persistent_history->cur_events);

  int *ptr = U->persistent_history->history + timestamp * 2;
  while (L --> 0) {
    int b = *ptr++;
    int a = *ptr++;
    *R++ = ((unsigned) a >> 24);
    *R++ = b;
    *R++ = a & 0xffffff;
  }
  
  return N;
}

/* history functions */

int get_timestamp (int user_id, int force) {
  user_t *U;

  if (conv_uid (user_id) < 0) {
    return -1;
  }

  if (force) {
    U = get_user_f (user_id);
    if (!U->history_ts) {
      U->history_ts = new_history_ts ();
    }
  } else {
    U = get_user (user_id);
    if (!U || !U->history_ts) {
      return 0;
    }
  }

  return U->history_ts;
}

/* returns # of new events, or -1 */
int get_history (int user_id, int timestamp, int limit, int *R) {
  user_t *U;
  int N;

  U = get_user (user_id);
  if (!U || !U->history_ts) {
    return -1;
  }

  N = U->history_ts - timestamp;
  if (!N) {
    return 0;
  }
  if ((unsigned) N > HISTORY_EVENTS) {
    return -1;
  }
  if (!U->history) {
    return -1;
  }
  if (limit > 0 && N > limit) {
    N = limit;
  }

  int count = N;
  while (count --> 0) {
    int i = ++timestamp & (HISTORY_EVENTS - 1);
    int a = U->history[i*2+1], b = U->history[i*2];
    if (!a && !b) {
      return -1;
    }
    *R++ = ((unsigned) a >> 24);
    *R++ = b;
    *R++ = a & 0xffffff;
  }
    
  return N;
}


char *get_user_secret (int user_id) {
  user_t *U = get_user (user_id);
  int i;

  if (!U) {
    return 0;
  }

  for (i = 0; i < 8; i++) {
    if (U->secret[i]) {
      break;
    }
  }

  return i == 8 ? 0 : U->secret;
}


int set_user_secret (int user_id, const char *secret) {
  user_t *U = get_user_f (user_id);
  if (!U) {
    return 0;
  }

  if (!secret) {
    memset (U->secret, 0, 8);
    return 1;
  }

  if (strlen (secret) != 8) {
    return 0;
  }

  memcpy (U->secret, secret, 8);
  return 8;
}

/* online friends lists */

#define	FRIEND_MULT	659203823
#define	FRIEND_MULT_INV	790841359

static void adjust_online_tree (user_t *U) {
  stree_t *T;
  FreedNodes = 0;
  U->online_tree = stree_prune (U->online_tree, now - hold_online_time);
  while (FreedNodes) {
    T = FreedNodes;
    FreedNodes = T->left;
    /* send offline notification to U->user_id about T->x */
    update_history (U, -FRIEND_MULT_INV * T->x, 1, 9);
    free_stree_node (T);
  }
}

int user_friends_online (int user_id, int N, int *A) {
  user_t *U;
  int i, x = user_id * FRIEND_MULT, c = 0;

  if (N < 0 || N > MAX_USER_FRIENDS || user_id <= 0) {
    return -1;
  }
  if (!N) {
    return 0;
  }

  for (i = 0; i < N; i++) {
    U = get_user_f (A[i]);
    if (!U) {
      continue;
    }
    c++;
    U->online_tree = stree_insert (stree_delete (U->online_tree, x), x, now);
    adjust_online_tree (U);
    if (!minsert_flag) {
      /* send online notification to U->user_id about user_id */
      update_history (U, -user_id, 0, 8);
    }
  }

  return c;
}

int user_friends_offline (int user_id, int N, int *A) {
  user_t *U;
  int i, x = user_id * FRIEND_MULT, c = 0;

  if (N < 0 || N > MAX_USER_FRIENDS || user_id <= 0) {
    return -1;
  }
  if (!N) {
    return 0;
  }

  for (i = 0; i < N; i++) {
    U = get_user (A[i]);
    if (!U) {
      continue;
    }
    c++;
    U->online_tree = stree_delete (U->online_tree, x);
    if (minsert_flag) {
      /* send offline notification to U->user_id about user_id */
      update_history (U, -user_id, 0, 9);
    }
    adjust_online_tree (U);
  }

  return c;
}

static void fetch_online_tree (stree_t *T, int mode) {
  if (!T) {
    return;
  }
  fetch_online_tree (T->left, mode);
  if (RA >= R + MAX_RES - 1) {
    return;
  }
  *RA++ = T->x * FRIEND_MULT_INV;
  if (mode) {
    *RA++ = T->y;
  }
  fetch_online_tree (T->right, mode);
}

static void sort_res (int a, int b) {
  int i, j, h, t;
  if (a >= b) {
    return;
  }
  h = R[(a+b)>>1];
  i = a;
  j = b;
  do {
    while (R[i] < h) { i++; }
    while (R[j] > h) { j--; }
    if (i <= j) {
      t = R[i];  R[i++] = R[j];  R[j--] = t;
    }
  } while (i <= j);
  sort_res (a, j);
  sort_res (i, b);
}

static void sort_res2 (int a, int b) {
  int i, j, h;
  long long t;
  if (a >= b) {
    return;
  }
  h = R[(a+b)&-2];
  i = a;
  j = b;
  do {
    while (R[2*i] < h) { i++; }
    while (R[2*j] > h) { j--; }
    if (i <= j) {
      t = ((long long *)R)[i]; ((long long *)R)[i++] = ((long long *)R)[j];  ((long long *)R)[j--] = t;
    }
  } while (i <= j);
  sort_res2 (a, j);
  sort_res2 (i, b);
}

int get_online_friends (int user_id, int mode) {
  user_t *U = get_user (user_id);
  R_cnt = 0;
  if (!U) {
    return conv_uid (user_id) < 0 ? -1 : 0;
  }
  RA = R;
  adjust_online_tree (U);
  fetch_online_tree (U->online_tree, mode);
  R_cnt = RA - R;
  if (mode) {
    sort_res2 (0, (R_cnt >> 1) - 1);
    return R_cnt >> 1;
  } else {
    sort_res (0, R_cnt - 1);
    return R_cnt;
  }
}

static int scan_uid = MAX_USERS;

void adjust_some_users (void) {
  user_t *U;
  int i = scan_uid, j = 2000, min_y = now - hold_online_time;
  if (j > max_uid) { j = max_uid; }
  while (j --> 0) {
    U = User[i++];
    if (i > max_uid) {
      i = min_uid;
    }
    if (U && U->online_tree && U->online_tree->y < min_y) {
      adjust_online_tree (U);
    }
  }
  scan_uid = i;
}

int do_update_history (int user_id, int local_id, int flags, int op) {
  if (op < 50 || op >= 100 || flags & ~0xffff) {
    return -1;
  }
  user_t *U = get_user_f (user_id);
  if (!U) {
    return -1;
  }
  update_history (U, local_id, flags, op);
  return 1;
}

int do_update_history_extended (int user_id, const char *string, long len, int op) {
  if (op < 100 || op >= 200 || !string) {
    return -1;
  }
  if (len < 0) {
    len = strlen (string);
  }
  if (len & (-1 << 16)) {
    return -1;
  }
  user_t *U = get_user_f (user_id);
  if (!U) {
    return -1;
  }
  update_history_extended (U, string, len, op);
  return 1;
}


/*
 *
 *    SEARCH
 *
 */

/* next two functions taken from text-index.c */

static void ull_sort (unsigned long long *A, int b) {
  int i = 0, j = b;
  unsigned long long h, t;
  if (b <= 0) { return; }
  h = A[b >> 1];
  do {
    while (A[i] < h) { i++; }
    while (A[j] > h) { j--; }
    if (i <= j) {
      t = A[i];  A[i++] = A[j];  A[j--] = t;
    }
  } while (i <= j);
  ull_sort (A+i, b-i);
  ull_sort (A, j);
}


static unsigned long long WordCRC[MAX_TEXT_LEN / 2];

int compute_message_distinct_words (char *text, int text_len) {
  char *ptr = text;
  int count = 0, len;
  static char buff[1024];

  assert ((unsigned) text_len < MAX_TEXT_LEN);

  while (*ptr == 1 || *ptr == 2) {
    char *tptr = ptr;
    while (*tptr && *tptr != 9) {
      ++tptr;
    }

    if (*ptr == 2) {
      char tptr_peek = *tptr;
      
      if (*tptr == 9) {
        *tptr = 0;
      }

      while (*ptr && *ptr != 32) {
        ++ptr;
      }

      while (*ptr) {
        len = get_notword (ptr);
        if (len < 0) {
          break;
        }
        ptr += len;

        len = get_word (ptr);
        assert (len >= 0);

        if (len > 0) {
          assert (count < MAX_TEXT_LEN / 2);
          WordCRC[count++] = crc64 (buff, my_lc_str (buff, ptr, len)) & -64;
        }
        ptr += len;
      }

      *tptr = tptr_peek;

      if (ptr != tptr && verbosity > 0) {
        fprintf (stderr, "warning: error while parsing kludge in '%.*s': text=%p ptr=%p tptr=%p\n", text_len, text, text, ptr, tptr);
      }
      assert (ptr <= tptr);

    }

    ptr = tptr;

    if (*ptr) {
      ++ptr;
    }
  }

  while (*ptr) {
    len = get_word(ptr);
    assert (len >= 0);
 
    if (len > 0) {
      assert (count < MAX_TEXT_LEN / 2);
      WordCRC[count++] = crc64 (buff, my_lc_str (buff, ptr, len)) & -64;
    }
    ptr += len;

    len = get_notword (ptr);
    if (len < 0) {
      break;
    }
    ptr += len;
  }

  ull_sort (WordCRC, count - 1);

  if (count > 0) {
    int newcount = 1, i;
    for (i = 1; i < count; i++) {
      if (WordCRC[i] != WordCRC[i - 1]) {
        WordCRC[newcount++] = WordCRC[i];
      }
    }
    return newcount;
  }

  return 0;
}

struct msg_search_node *add_new_msg_search_node (struct msg_search_node *last, int local_id, char *text, int len) {
  int wn = compute_message_distinct_words (text, len);
  int sz = offsetof (struct msg_search_node, words) + 8 * wn;
  struct msg_search_node *X = zmalloc (sz);
  X->prev = last;
  X->local_id = local_id;
  X->words_num = wn;
  memcpy (X->words, WordCRC, 8 * wn);
  ++msg_search_nodes;
  msg_search_nodes_bytes += sz;
  return X;
}

void free_msg_search_node (struct msg_search_node *X) {
  int sz = offsetof (struct msg_search_node, words) + 8 * X->words_num;
  assert (X->words_num >= 0);
  assert (!X->prev);
  --msg_search_nodes;
  msg_search_nodes_bytes -= sz;
  zfree (X, sz);
}

int list_contained (hash_t *A, int An, hash_t *B, int Bn) {
  int i;
  for (i = 0; i < An; i++) {
    int l = -1, r = Bn;
    hash_t cur = A[i];
    while (r - l > 1) {
      int m = (l + r) >> 1;
      if (cur < B[m]) {
        r = m;
      } else {
        l = m;
      }
    }
    if (l < 0 || B[l] != cur) {
      return 0;
    }
  }

  return 1;
}


#define aho_feed_char(c) {\
  while ((c) != KS[q]) {\
    q = KA[q];\
    if (!q) {\
      break;\
    }\
  }\
  m |= KB[++q];\
}

#define aho_feed_str(s) {\
  char t, *str = (s);\
  while ((t = *str) != 0) {\
    aho_feed_char (t);\
    ++str;\
  }\
}

int aho_check_message (char *text, int text_len) {
  char *ptr = text;
  int len, last_space = 1, tab_seen = 0;
  int q = 1, m = 0;
  static char buff[1024];

  assert ((unsigned) text_len < MAX_TEXT_LEN);

  aho_feed_char (' ');

  while (*ptr == 1 || *ptr == 2) {
    char *tptr = ptr;
    while (*tptr && *tptr != 9) {
      ++tptr;
    }

    if (*ptr == 2) {
      char tptr_peek = *tptr;
      
      if (*tptr == 9) {
        *tptr = 0;
      }

      while (*ptr && *ptr != 32) {
        ++ptr;
      }

      while (*ptr) {
        len = get_notword (ptr);
        if (len < 0) {
          break;
        }
        ptr += len;

        if (!last_space) {
          aho_feed_char (' ');
          last_space = 1;
        }

        len = get_word (ptr);
        assert (len >= 0);

        if (len > 0) {
          lc_str (buff, ptr, len);
          aho_feed_str (buff);
          last_space = 0;
        }
        ptr += len;
      }

      *tptr = tptr_peek;

      if (ptr != tptr && verbosity > 0) {
        fprintf (stderr, "warning: error while parsing kludge in '%.*s': text=%p ptr=%p tptr=%p\n", text_len, text, text, ptr, tptr);
      }
      assert (ptr <= tptr);

      aho_feed_char (' ');
      if (!last_space) {
        aho_feed_char (' ');
        last_space = 1;
      }
    }

    ptr = tptr;

    if (*ptr) {
      ++ptr;
    }
  }


  while (*ptr) {
    len = get_word(ptr);
    assert (len >= 0);
 
    if (len > 0) {
      lc_str (buff, ptr, len);
      aho_feed_str (buff);
      last_space = 0;
    }
    ptr += len;

    len = get_notword (ptr);
    if (len < 0) {
      break;
    }

    if (!tab_seen && len > 0 && memchr (ptr, 9, len)) {
      tab_seen = 1;
      aho_feed_char (' ');
    }

    ptr += len;

    if (!last_space) {
      aho_feed_char (' ');
      last_space = 1;
    }
  }
  
  if (!last_space) {
    aho_feed_char (' ');
  }

  return m == (1 << KN) - 1;
}

/* ----- tree iterators ----- */

#define MAX_SP	128

typedef struct tree_iterator tree_iterator_t;

struct tree_iterator {
  int Sp;
  tree_t *St[MAX_SP];
};

static inline tree_t *ti_current (tree_iterator_t *I) {
  return I->Sp ? I->St[I->Sp - 1] : 0;
}

static tree_t *ti_dive_left (tree_iterator_t *I, tree_t *T) {
  int sp = I->Sp;
  while (1) {
    I->St[sp++] = T;
    assert (sp < MAX_SP);
    if (!T->left) {
      I->Sp = sp;
      return T;
    }
    T = T->left;
  }
}

static tree_t *ti_init_left (tree_iterator_t *I, tree_t *T) {
  I->Sp = 0;
  if (T) {
    return ti_dive_left (I, T);
  } else {
    return 0;
  }
}

static tree_t *ti_next_left (tree_iterator_t *I) {
  int sp = I->Sp;
  if (!sp) {
    return 0;
  }
  tree_t *T = I->St[--sp];
  I->Sp = sp;
  if (T->right) {
    return ti_dive_left (I, T->right);
  } else if (sp) {
    return I->St[sp - 1];
  } else {
    return 0;
  }
}

/* right-to-left iterator */

static tree_t *ti_dive_right (tree_iterator_t *I, tree_t *T) {
  int sp = I->Sp;
  while (1) {
    I->St[sp++] = T;
    assert (sp < MAX_SP);
    if (!T->right) {
      I->Sp = sp;
      return T;
    }
    T = T->right;
  }
}

static tree_t *ti_init_right (tree_iterator_t *I, tree_t *T) {
  I->Sp = 0;
  if (T) {
    return ti_dive_right (I, T);
  } else {
    return 0;
  }
}

static tree_t *ti_next_right (tree_iterator_t *I) {
  int sp = I->Sp;
  if (!sp) {
    return 0;
  }
  tree_t *T = I->St[--sp];
  I->Sp = sp;
  if (T->left) {
    return ti_dive_right (I, T->left);
  } else if (sp) {
    return I->St[sp - 1];
  } else {
    return 0;
  }
}

#define	MAX_QUERY_WORDS	256
#define	MAX_QUERY_QUOTES	16

static hash_t QWords[MAX_QUERY_WORDS];
static unsigned char *QW_mfile[MAX_QUERY_WORDS];
static int QL[MAX_QUERY_WORDS], QW_msize[MAX_QUERY_WORDS];
static int V[MAX_QUERY_WORDS];
struct list_decoder *Decoder[MAX_QUERY_WORDS];
static int Qn;  // distinct words in query

static int Qq;  // quoted strings in query
static char *QStr[MAX_QUERY_QUOTES];  // quoted strings, in form " abc de f "

static unsigned char tmp_data[MAX_QUERY_WORDS*4 + 4], *tmp_data_w;

unsigned char *lookup_word_metafile (struct file_search_header *H, int size, hash_t word, int *meta_len) {
  assert (H->magic == FILE_USER_SEARCH_MAGIC);
  assert (H->words_num <= size / 4 - 2);
  int a = -1, b = H->words_num;
  hash_t aw = 0, bw = -1LL;

  if (verbosity >= 2) {
    fprintf (stderr, "looking up word hash %016llx\n", word);
  }

  while (b - a > 1) {
    int c = (a + b) >> 1;
    int skip_bits = __builtin_clzll (aw ^ bw);
    int x = H->word_start[c];
    hash_t y;
    if (x < 0) {
      y = (x ^ (1 << 31));
      y <<= 32;
    } else {
      assert (x >= H->words_num * 4 + 2 && x < size);
      y = bswap_64 (* (unsigned long long *) ((char *) H + x));
    }
    int s = (y >> 58);
    y <<= 6;
    assert (s >= 0 && s <= 58 - skip_bits);
    hash_t cw = skip_bits ? aw & (-1LL << (64 - skip_bits)) : 0;
    if (s > 0) {
      y &= (-1LL << (64 - s));
      cw |= (y >> skip_bits);
    }
    if (verbosity >= 3) {
      fprintf (stderr, "a=%d b=%d c=%d; aw=%016llx bw=%016llx cw=%016llx bits=%d+%d\n", a, b, c, aw, bw, cw, skip_bits, s);
    }
    if (skip_bits + s == 0 || !((word ^ cw) & (-1LL << (64 - skip_bits - s)))) {
      /* found */
      if (verbosity >= 2) {
        fprintf (stderr, "found at position %d (offset %08x); cw=%016llx bits=%d+%d\n", c, x, cw, skip_bits, s);
      }
      if (x < 0) {
        *((int *) tmp_data_w) = bswap_32 (x ^ 0x80000000);
        *meta_len = 4;
        tmp_data_w += 4;
        return tmp_data_w - 4;
      } else {
        if (c < H->words_num - 1 && H->word_start[c+1] >= 0) {
//          fprintf (stderr, "c=%d num=%d ws[c]=%d ws[c+1]=%d size=%d\n", c, H->words_num, x, H->word_start[c+1], size);
          assert (H->word_start[c+1] > x && H->word_start[c+1] <= size);
          size = H->word_start[c+1];
        }
        *meta_len = size - x;
        return (unsigned char *) H + x;
      }
    }
    if (word < cw) {
      b = c;
      bw = cw;
    } else {
      a = c;
      aw = cw;
    }
  }

  if (verbosity >= 2) {
    fprintf (stderr, "not found!\n");
  }
  return 0;
}

int get_word_metafile_len (unsigned char *wm, int ws, user_t *U, int word_idx) {
 int s = (*wm >> 2) + 6;
 unsigned char *ptr = wm + (s >> 3);
 int m = ((((int) *ptr++) << 24) | (1 << 23)) << (s & 7);
#define	cur_bit (m < 0)
#define	load_bit()	{ m <<= 1; if (unlikely(m == (-1 << 31))) { m = ((int) *ptr++ << 24) + (1 << 23); } }
 int a = 0;
 while (cur_bit) {
   a++;
   load_bit();
   assert (a <= 30);
 }
 load_bit();
 int r = 1;
 while (a > 0) {
   r <<= 1;
   r += cur_bit;
   load_bit();
   a--;
 }
 if (m & (1 << 23)) {
   ptr--;
 }
 if (ptr > wm + ws) {
   fprintf (stderr, "ptr=%p wm=%p ws=%d r=%d user_id=%d word_idx=%d word_crc32=%016llx search_mf_data=%p search_mf_len=%d\n", ptr, wm, ws, r, U->user_id, word_idx, QWords[word_idx], U->search_mf ? U->search_mf->data : 0, U->search_mf ? U->search_mf->len : -1);
   int i;
   for (i = 0; i < 32 && i < ptr - wm; i++) {
     fprintf (stderr, "%02x ", wm[i]);
   }
   fprintf (stderr, "\n");
 }
 assert (ptr <= wm + ws);
 return r;
}

static void sort_wmeta (int a, int b) {
  if (a >= b) {
    return;
  }
  int i = a, j = b, h = QL[(a+b)>>1];
  do {
    while (QL[i] < h) { i++; }
    while (QL[j] > h) { j--; }
    if (i <= j) {
      int t = QL[i];
      QL[i] = QL[j];
      QL[j] = t;
      t = QW_msize[i];
      QW_msize[i] = QW_msize[j];
      QW_msize[j] = t;
      unsigned char *tp = QW_mfile[i];
      QW_mfile[i] = QW_mfile[j];
      QW_mfile[j] = tp;
      hash_t tt;
      tt = QWords[i];
      QWords[i] = QWords[j];
      QWords[j] = tt;
      i++;
      j--;
    }
  } while (i <= j);
  sort_wmeta (a, j);
  sort_wmeta (i, b);
}

int s_first_local_id, s_last_local_id, s_peer_id, s_and_mask, s_xor_mask, s_min_time, s_max_time;
int s_messages_checked, s_max_unpack_messages;

/* 1 = ok, 0 = no, -1 = exit */
int check_one_message (user_t *U, int local_id, tree_t *Z) {
  int flags = -1;
  struct message *M = 0;
  struct file_message *FM = 0;

  tree_t *T = tree_lookup (U->msg_tree, local_id);
  if (T) {
    switch (T->y & 7) {
    case TF_PLUS:
    case TF_REPLACED:
      M = T->msg;
      flags = M->flags;
      break;
    case TF_MINUS:
      break;
    case TF_ZERO:
      flags = T->flags;
      break;
    case TF_ZERO_PRIME:
      flags = T->value->flags;
      break;
    default:
      assert (0);
    }
    if (flags < 0 || ((flags ^ s_xor_mask) & s_and_mask) != 0) {
      return 0;
    }
  }

  if (M) {
    if ((s_peer_id && M->peer_id != s_peer_id) || M->date < s_min_time || M->date >= s_max_time) {
      return 0;
    }
  } else {
    FM = user_metafile_message_lookup (U->mf->data, U->dir_entry->user_data_size, local_id, U);
    if (!FM) {
      fprintf (stderr, "while searching for user %d: found local_id=%d, first_local_id=%d, last_local_id=%d; absent from metafile\n", U->user_id, local_id, s_first_local_id, s_last_local_id);
    }
    assert (FM);
          
    if (!T && ((FM->flags ^ s_xor_mask) & s_and_mask) != 0) {
      return 0;
    }
    if ((s_peer_id && FM->peer_id != s_peer_id) || FM->date < s_min_time || FM->date >= s_max_time) {
      return 0;
    }
  }

  if (Z) {
    assert (Z->x == local_id);
    struct msg_search_node *cur = Z->edit_text->search_node;
    if (!cur || !list_contained (QWords, Qn, cur->words, cur->words_num)) {
      return 0;
    }
  }

  if (Qq || unlikely (!s_messages_checked && !Z)) {
    /* unpack message and check */
    assert (dyn_top >= dyn_cur + MAX_TEXT_LEN + 1024);

    char *text;
    int text_len = 0, kludges_len;

    if (M) {
      text = M->text + text_shift;
      text_len = M->len;
    } else if (Z) {
      text = Z->edit_text->text;
      text_len = Z->edit_text->len;
    } else {
      text = dyn_cur;
      assert (unpack_message_text (FM, text, MAX_TEXT_LEN, 3, &text_len, &kludges_len) == 1);
      assert (text_len >= 0 && text_len <= MAX_TEXT_LEN);
      text[text_len] = 0;
    }

    if (!Z && unlikely (!s_messages_checked++)) {
      int wn = compute_message_distinct_words (text, text_len);
      if (!list_contained (QWords, Qn, WordCRC, wn)) {
	if (verbosity >= 2) {
	  fprintf (stderr, "some of search words not found in test message #%d, ignoring metafile search results\n", local_id);
	}
	return -1;
      }
    }

    if (Qq) {
      if (!--s_max_unpack_messages) {
	return -1;
      }
      if (!aho_check_message (text, text_len)) {
	return 0;
      }
    }
  }
  return 1;
}

int metafile_search (user_t *U, int max_res, int *Res) {
  int i, w = 0, found_in_replaced = 0;
  static tree_iterator_t I;

  struct file_user_list_entry *D = U->dir_entry;
  if (!D) {
    return 0;
  }

  char *metafile = get_user_metafile (U);
  assert (U->mf);
  assert (U->mf->data == metafile);
  assert (U->mf->len >= D->user_data_size);

  s_max_unpack_messages = MAX_SEARCH_UNPACKED_MESSAGES;

  tree_t *Z = ti_init_left (&I, U->edit_text_tree);

  int search_metafile_len = ((struct file_user_list_entry_search *)D)->user_search_size;
  if (!search_metafile_len) {
    goto finish_tree_search;
  }

  assert (U->search_mf);
  assert (search_metafile_len > 0);
  assert (U->search_mf->len == search_metafile_len + idx_crc_enabled * 4);

  tmp_data_w = tmp_data;

  for (i = 0; i < Qn; i++) {
    QW_mfile[i] = lookup_word_metafile ((struct file_search_header *) U->search_mf->data, search_metafile_len, QWords[i], &QW_msize[i]);
    if (!QW_mfile[i]) {
      goto finish_tree_search;
    }
    QL[i] = get_word_metafile_len ((unsigned char *) QW_mfile[i], QW_msize[i], U, i);
  }

  sort_wmeta (0, Qn-1);

  if (verbosity > 1) {
    fprintf (stderr, "intersecting %d word metafiles:\n", Qn);
    for (i = 0; i < Qn; i++) {
      fprintf (stderr, "word #%d: hash=%016llx len=%d metafile=%p msize=%d\n", i+1, QWords[i], QL[i], QW_mfile[i], QW_msize[i]);
    }
  }

  for (i = 0; i < Qn; i++) {
    int s = 6 + ((* (unsigned char *) QW_mfile[i]) >> 2) + 1 + 2 * (31 - __builtin_clz (QL[i]));
    Decoder[i] = zmalloc_list_decoder (s_last_local_id - s_first_local_id + 1, QL[i], QW_mfile[i], le_interpolative, s);
    V[i] = Decoder[i]->decode_int (Decoder[i]);
  }

  s_messages_checked = 0;

  int x = V[0], y = -1, j;

  while (x < 0x7fffffff) {
    for (j = 1; j < Qn; j++) {
      y = V[j];
      while (y < x) {
        y = Decoder[j]->decode_int (Decoder[j]);
      }
      V[j] = y;
      if (y > x) {
        break;
      }
    }

    if (j == Qn) {
      int local_id = x + s_first_local_id;
      int match = 0;
      x = Decoder[0]->decode_int (Decoder[0]);

      while (Z && Z->x < local_id) {
	match = check_one_message (U, Z->x, Z);
	assert (match >= 0);
	if (match > 0) {
	  found_in_replaced++;
	  Res[w++] = Z->x;
	  if (w >= max_res) {
	    goto finish_metafile_search;
	  }
	}
	Z = ti_next_left (&I);
      }

      if (Z && Z->x == local_id) {
	match = check_one_message (U, local_id, Z);
	Z = ti_next_left (&I);
      } else {
	match = check_one_message (U, local_id, 0);
      }

      if (!match) {
	continue;
      }

      if (match < 0) {
	break;
      }
//      if (verbosity >= 2) {
//        fprintf (stderr, "found local_id=%d, flags=%d\n", local_id, flags);
//      }
      Res[w++] = local_id;
      if (w >= max_res) {
        break;
      }

    } else {
      do {
        x = Decoder[0]->decode_int (Decoder[0]);
      } while (x < y);
    }
  }

 finish_metafile_search:

  for (i = 0; i < Qn; i++) {
    if (Decoder[i]->br.m & (1 << 23)) {
      Decoder[i]->br.m = (-1 << 31);
      Decoder[i]->br.ptr--;
    }
    if (Decoder[i]->br.ptr > QW_mfile[i] + QW_msize[i]) {
      fprintf (stderr, "ERROR while decoding search list for user %d (hash %016llx, len=%d): start=%p size=%d end=%p ptr=%p m=%08x\n",
		U->user_id, QWords[i], QL[i], QW_mfile[i], QW_msize[i], QW_mfile[i]+QW_msize[i], Decoder[i]->br.ptr, Decoder[i]->br.m);
      w = 0;
    }
    assert (Decoder[i]->br.ptr <= QW_mfile[i] + QW_msize[i]);
    zfree_list_decoder (Decoder[i]);
  }

 finish_tree_search:

  while (Z && Z->x <= s_last_local_id) {
    int match = check_one_message (U, Z->x, Z);
    assert (match >= 0);
    if (match > 0) {
      found_in_replaced++;
      Res[w++] = Z->x;
      if (w >= max_res) {
	break;
      }
    }
    Z = ti_next_left (&I);
  }
     
  if (verbosity >= 2) {
    fprintf (stderr, "found %d messages, %d of them accepted by flags, %d of them in edited texts\n", s_messages_checked, w, found_in_replaced);
  }

  return w;
}

static void reverse_list (int *Res, int sz) {
  int i;
  for (i = 0; i < (sz >> 1); i++) {
    int t = Res[i];
    Res[i] = Res[sz - i - 1];
    Res[sz - i - 1] = t;
  }
}

int incore_search (user_t *U, int max_res, int *Res) {
  struct msg_search_node *cur;
  int max_scanned_messages = MAX_SEARCH_SCANNED_INCORE_MESSAGES;
  int i = 0;
  static tree_iterator_t I;
  tree_t *Z = ti_init_right (&I, U->edit_text_tree);

  for (cur = U->last; cur; cur = cur->prev) {

    struct msg_search_node *cur2 = cur;

    while (Z && Z->x > cur->local_id) {
      Z = ti_next_right (&I);
    }

    if (Z && Z->x == cur->local_id) {
      cur2 = Z->edit_text->search_node;
      Z = ti_next_right (&I);
    }
    
    if (list_contained (QWords, Qn, cur2->words, cur2->words_num)) {
      tree_t *T = tree_lookup (U->msg_tree, cur->local_id);
      if (T) {
        assert ((T->y & 7) == TF_PLUS);
        int flags = T->msg->flags;
        if (!((flags ^ s_xor_mask) & s_and_mask) && (!s_peer_id || T->msg->peer_id == s_peer_id) && T->msg->date >= s_min_time && T->msg->date < s_max_time) {
          if (Qq) {
            if (!--max_scanned_messages) {
              break;
            }
            if (!aho_check_message (T->msg->text + text_shift, T->msg->len)) {
              continue;
            }
          }
          Res[i++] = cur->local_id;
          if (i >= max_res) {
            return i;
          }
        }
      }
    }
  }

  return i;
}

void prepare_quoted_query (const char *query) {
  char *ptr = (char *) query, *to = dyn_cur;
  int qc, qm = 0, len;

  Qq = 0;
  *to++ = 0;

  while (*ptr) {
    len = get_notword (ptr);
    if (len < 0) {
      break;
    }
    qc = 0;
    while (len > 0) {
      if (*ptr++ == '"') {
        qc++;
      }
      len--;
    }
    if (qc) {
      if (qm) {
        if (to[-1] != ' ') {
          *to++ = ' ';
        }
        if (!to[-2]) {
          --Qq;
        }
        *to++ = 0;
        qm = 0;
        qc--;
      }
      if ((qc & 1) && Qq < MAX_QUERY_QUOTES - 1) {
        QStr[Qq++] = to;
        *to++ = ' ';
        qm = 1;
      }
    } else if (qm) {
      if (to[-1] != ' ') {
        *to++ = ' ';
      }
    }

    len = get_word (ptr);
    assert (len >= 0);

    if (len > 0) {
      lc_str (to, ptr, len);
      to += len;
    }
    ptr += len;
  }

  if (qm) {
    if (to[-1] != ' ') {
      *to++ = ' ';
    }
    if (!to[-2]) {
      --Qq;
    }
    *to++ = 0;
  }

  assert (to + 8 < dyn_top);
  assert (Qq >= 0 && Qq < MAX_QUERY_QUOTES);

  if (Qq) {
    dyn_cur = to + (- (long) to & 7);
  }
}

static char query_buffer[1024];

/* max_res > 0 -- return first messages, max_res < 0 -- return last messages */
int get_search_results (int user_id, int peer_id, int and_mask, int xor_mask, int min_time, int max_time, int max_res, const char *query) {
  R_cnt = 0;
  if (!search_enabled) {
    return -1;
  }
  if (conv_uid (user_id) < 0) {
    return -1;
  }

  if (xor_mask == -1) {
    and_mask = TXF_SPAM | TXF_DELETED;
    xor_mask = 0;
  }

  if (!strncmp (query, "min_time_", 9)) {
    char *tmp;
    query += 9;
    min_time = strtol ((char *)query, &tmp, 10);
    if (query == tmp) {
      query -= 9;
    } else {
      query = tmp;
      if (*query) {
	++query;
      }
    }
  }

  if (!strncmp (query, "max_time_", 9)) {
    char *tmp;
    query += 9;
    max_time = strtol ((char *)query, &tmp, 10);
    if (query == tmp) {
      query -= 9;
    } else {
      query = tmp;
      if (*query) {
	++query;
      }
    }
  }

  if (!strncmp (query, "peer_id_", 8)) {
    char *tmp;
    query += 8;
    peer_id = strtol ((char *)query, &tmp, 10);
    if (query == tmp) {
      query -= 8;
    } else {
      query = tmp;
      if (*query) {
	++query;
      }
    }
  }

  if (!max_time) {
    max_time = 0x7fffffff;
  }

  if (min_time >= max_time) {
    return R_cnt = 0;
  }

  if (searchtags_enabled) {
    int i;
    for (i = 0; query[i]; i++) {
      if (i > 1020) {
	return R_cnt = 0;
      }
      if (query[i] == '?' && (query[i+1] & 0xc0) != 0) {
	query_buffer[i] = 0x1f;
      } else {
	query_buffer[i] = query[i];
      }
    }
    query_buffer[i] = 0;
    query = query_buffer;
  }

  int wn = compute_message_distinct_words ((char *) query, strlen (query));
  if (wn <= 0 || wn > MAX_QUERY_WORDS) {
    return -1;
  }

  memcpy (QWords, WordCRC, wn * 8);
  Qn = wn;
  Qq = 0;

  struct file_user_list_entry *D;
  user_t *U = get_user (user_id);

  if (U) {
    D = U->dir_entry;
  } else {
    D = lookup_user_directory (user_id);
  }

  if (!D && !U) {
    return -3;
  }

  int search_metafile_len = D ? ((struct file_user_list_entry_search *) D)->user_search_size : 0;

  if (search_metafile_len) {
    if (!load_user_metafile (user_id) | !load_search_metafile (user_id)) { // arithmetic OR!
      return -2;
    }
    U = get_user (user_id);
    assert (U && U->dir_entry == D && !U->delayed_tree);

    assert (U->mf);
    struct file_user_header *H = (struct file_user_header *) U->mf->data;
    assert (H->magic == FILE_USER_MAGIC);
    s_first_local_id = H->user_first_local_id;
    s_last_local_id = H->user_last_local_id;
  } else if (!U) {
    R_cnt = 0;
    return 0;
  } else {
    s_first_local_id = 1;
    s_last_local_id = 0;
  }

  char *keep_dyn_cur = dyn_cur;

  if (strchr (query, '"')) {
    prepare_quoted_query (query);
  }

  if (Qq) {
    if (aho_prepare (Qq, QStr) <= 0) {
      Qq = 0;
    } else {
      if (verbosity >= 3) {
        aho_dump ();
      }

    }
  }

  dyn_cur = keep_dyn_cur;

  int ires, mres, tres;

  s_peer_id = peer_id;
  s_and_mask = and_mask;
  s_xor_mask = xor_mask;
  s_min_time = min_time;
  s_max_time = max_time;

  if (max_res > 0) {
    mres = (search_metafile_len ? metafile_search (U, MAX_TMP_RES, R) : 0);
    ires = incore_search (U, MAX_TMP_RES - mres, R + mres);

    reverse_list (R + mres, ires);

    R_cnt = tres = ires + mres;
    if (R_cnt > max_res) {
      R_cnt = max_res;
    }
  } else {
    ires = incore_search (U, MAX_TMP_RES, R);
    mres = (search_metafile_len ? metafile_search (U, MAX_TMP_RES - ires, R + ires) : 0);

    reverse_list (R + ires, mres);

    R_cnt = tres = ires + mres;
    if (R_cnt > -max_res) {
      R_cnt = -max_res;
    }
  }
  
  return tres;
}

