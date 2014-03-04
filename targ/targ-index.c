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
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <aio.h>
#include <byteswap.h>

#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "net-connections.h"
#include "net-aio.h"
#include "kdb-targ-binlog.h"
#include "targ-data.h"
#include "targ-index.h"
#include "targ-search.h"
#include "targ-index-layout.h"
#include "word-split.h"
#include "translit.h"
#include "server-functions.h"
#include "stemmer.h"
#include "crc32.h"
#include "listcomp.h"
#include "vv-tl-aio.h"

long long jump_log_pos;
int jump_log_ts;
unsigned jump_log_crc32;

char *newidxname;
char *idx_filename;

long long idx_bytes, idx_loaded_bytes;
int idx_fd;

struct targ_index_header Header;
struct targ_index_word_directory_entry *idx_worddir;

unsigned char *idx_word_data;
int idx_word_data_bytes;

int idx_stale_ads, idx_fresh_ads, idx_words, idx_ads, idx_users, idx_max_uid, idx_periodic_ads, idx_lru_ads, idx_recent_views;
int idx_max_stale_ad_id = -1;

struct targ_index_ads_directory_entry *idx_stale_ad_dir, *idx_fresh_ad_dir;

int ancient_ads_pending, ancient_ads_loading, ancient_ads_loaded, ancient_ads_aio_loaded;
long long ancient_ads_loaded_bytes, ancient_ads_aio_loaded_bytes;
int allocated_metafiles, aio_read_errors;
long long allocated_metafile_bytes;

/*
 *
 *		LOAD/SAVE GLOBAL STATS FILE
 *
 */

int load_stats_file (char *stats_filename) {
  if (verbosity > 0) {
    fprintf (stderr, "loading global click/view statistics from %s\n", stats_filename);
  }
  int fd = open (stats_filename, O_RDONLY);
  if (fd < 0) {
    fprintf (stderr, "cannot open statistics file %s: %m\n", stats_filename);
    return -1;
  }
  int r = read (fd, AdStats.g, sizeof (AdStats.g));
  assert (r == sizeof (AdStats.g));
  long i;
  long long c = AdStats.g[0].clicks, v = AdStats.g[0].views;
  assert (c >= 0 && v >= 0);
  for (i = 1; i < MAX_AD_VIEWS; i++) {
    assert (AdStats.g[i].clicks >= 0 && AdStats.g[i].views >= 0);
    assert (AdStats.g[i].clicks <= c && AdStats.g[i].views <= v);
    c -= AdStats.g[i].clicks;
    v -= AdStats.g[i].views;
  }
  assert (!c && !v);
  close (fd);
  vkprintf (1, "loaded global click/view statistics from file %s, %lld/%lld clicks/views\n", stats_filename, AdStats.g[0].clicks, AdStats.g[0].views);
  return 1;
}

int save_stats_file (char *stats_filename) {
  if (verbosity > 0) {
    fprintf (stderr, "saving global click/view statistics to %s\n", stats_filename);
  }
  int fd = open (stats_filename, O_WRONLY | O_CREAT | O_EXCL, 0644);
  if (fd < 0) {
    fprintf (stderr, "cannot open statistics file %s for writing: %m\n", stats_filename);
    return -1;
  }
  int w = write (fd, AdStats.g, sizeof (AdStats.g));
  assert (w == sizeof (AdStats.g));
  assert (close (fd) >= 0);
  return 1;
}

/*
 *
 *        word directory/data lookup 
 *
 */

unsigned tmp_word_data[TMP_WDATA_INTS], *tmp_word_data_cur;

unsigned char *idx_word_lookup (hash_t word, int *max_bytes) {
  int a = -1, b = idx_words, c;
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (idx_worddir[c].word <= word) {
      a = c;
    } else {
      b = c;
    }
  }
  if (a < 0 || idx_worddir[a].word != word) {
    *max_bytes = 0;
    return 0;
  }
  int offs = idx_worddir[a].data_offset;
  if (offs < 0) {
    assert (tmp_word_data_cur < tmp_word_data + TMP_WDATA_INTS);
    *max_bytes = 4;
    *tmp_word_data_cur++ = bswap_32 (offs << 1);
    return (unsigned char *) (tmp_word_data_cur - 1);
  }
  assert (offs < idx_word_data_bytes);
  if (idx_worddir[b].data_offset >= 0) {
    *max_bytes = idx_worddir[b].data_offset - offs;
  } else {
    *max_bytes = idx_word_data_bytes - offs;
  }
  return idx_word_data + offs;
}

int get_idx_word_list_len (hash_t word) {
  int len;
  unsigned char *ptr = idx_word_lookup (word, &len);
  if (!ptr) {
    return 0;
  }
  struct bitreader br;
  bread_init (&br, ptr, 0);
  int x = bread_gamma_code (&br);
  assert (bread_get_bits_read (&br) <= len * 8);
  return x;
}

/*
 *
 *		AIO (ANCIENT ADS LOAD)
 *
 */

int onload_ad_metafile (struct connection *c, int read_bytes);

conn_type_t ct_metafile_ad_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_ad_metafile
};

conn_query_type_t aio_metafile_query_type = {
.magic = CQUERY_FUNC_MAGIC,
.title = "targ-data-aio-metafile-query",
.wakeup = aio_query_timeout,
.close = delete_aio_query,
.complete = delete_aio_query
};



struct targ_index_ads_directory_entry *lookup_ancient_ad (int ad_id) {
  int a = -1, b = idx_stale_ads, c;
  while (b - a > 1) {
    c = (a + b) >> 1;
    if (idx_stale_ad_dir[c].ad_id <= ad_id) {
      a = c;
    } else {
      b = c;
    }
  }
  if (a >= 0 && idx_stale_ad_dir[a].ad_id == ad_id) {
    return &idx_stale_ad_dir[a];
  } else {
    return 0;
  }
}

int schedule_load_ancient_ad (struct advert *A, long long pos, long len) {
  assert (A->flags & ADF_ANCIENT);

  WaitAioArrClear ();

  if (A->mf) {
    assert (A->mf->aio);
    check_aio_completion (A->mf->aio);
    if (A->mf && A->mf->aio) {
      WaitAioArrAdd (A->mf->aio);
      return -2;
    }
    assert (!A->mf);
    if (!(A->flags & ADF_ANCIENT)) {
      return 0;
    }
    fprintf (stderr, "Previous AIO query failed for ad %d, scheduling a new one\n", A->ad_id);
  }

  core_mf_t *M = A->mf = malloc (len + offsetof (struct core_metafile, data));
  assert (M);

  allocated_metafile_bytes += len;
  allocated_metafiles++;
  
  M->ad = A;
  M->aio = 0;
  M->pos = pos;
  M->len = len;

  if (verbosity > 0) {
    fprintf (stderr, "*** Scheduled reading ad %d data from index at position %lld, %ld bytes\n", A->ad_id, pos, len);
  }

  assert (use_aio > 0);

  M->aio = create_aio_read_connection (idx_fd, M->data, pos, len, &ct_metafile_ad_aio, M);
  assert (M->aio);
  WaitAioArrAdd (M->aio);
  
  return -2;
}

/*
 *
 * GENERIC BUFFERED WRITE
 *
 */

#define	BUFFSIZE	16777216

char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff, *wptr0 = Buff, *wptr_crc = Buff;
long long write_pos;
long long metafile_pos;
unsigned crc32_acc = -1;

int newidx_fd;

static inline long long get_write_pos (void) {
  int s = wptr - wptr0;
  wptr0 = wptr;
  metafile_pos += s;
  return write_pos += s;
}

static inline long long get_metafile_pos (void) {
  int s = wptr - wptr0;
  wptr0 = wptr;
  write_pos += s;
  return metafile_pos += s;
}

static inline void reset_metafile_pos (void) {
  get_metafile_pos ();
  metafile_pos = 0;
}

void flushout (void) {
  int w, s;
  if (wptr > wptr0) {
    s = wptr - wptr0;
    write_pos += s;
    metafile_pos += s;
  }
  if (rptr < wptr) {
    s = wptr - rptr;
    w = write (newidx_fd, rptr, s);
    assert (w == s);
    assert (rptr <= wptr_crc && wptr_crc <= wptr);
    crc32_acc = crc32_partial (wptr_crc, wptr - wptr_crc, crc32_acc);
  }
  rptr = wptr = wptr0 = wptr_crc = Buff;
}

void clearin (void) {
  rptr = wptr = wptr0 = Buff + BUFFSIZE;
}

static inline void writeout (const void *D, size_t len) {
  const char *d = D;
  while (len > 0) {
    int r = Buff + BUFFSIZE - wptr;
    if (r > len) { 
      memcpy (wptr, d, len);
      wptr += len;
      return;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout();
    }
  }                                
}

static inline void writeout_long (long long value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 8)) {
    flushout();
  }
  *((long long *) wptr) = value;
  wptr += 8;
}

static inline void writeout_int (int value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 4)) {
    flushout();
  }
  *((int *) wptr) = value;
  wptr += 4;
}

static inline void writeout_short (int value) {
  if (unlikely (wptr > Buff + BUFFSIZE - 2)) {
    flushout();
  }
  *((short *) wptr) = value;
  wptr += 2;
}

static inline void writeout_ushort_check (int value) {
  assert (likely ((unsigned) value < 0x10000));
  writeout_short (value);
}

static inline void writeout_char (char value) {
  if (unlikely (wptr == Buff + BUFFSIZE)) {
    flushout();
  }
  *wptr++ = value;
}

static inline void writeout_align (int align) {
  int k = -get_write_pos() & (align - 1);
  while (k > 0) {
    writeout_char (0);
    k--;
  }
}

static inline void initcrc (void) {
  crc32_acc = (unsigned) -1;
  wptr_crc = wptr;
}

static inline void relaxcrc (void) {
  crc32_acc = crc32_partial (wptr_crc, wptr - wptr_crc, crc32_acc);
  wptr_crc = wptr;
}

static inline void writecrc (void) {
  relaxcrc ();
  unsigned crc32 = ~crc32_acc;
  writeout (&crc32, 4);
  initcrc ();
}

void write_seek (long long new_pos) {
  flushout();
  assert (lseek (newidx_fd, new_pos, SEEK_SET) == new_pos);
  write_pos = new_pos;
}

/*
 *
 *	INDEX LOAD
 *
 */

char *RBuff, *RBuffEnd, *idx_rptr, *idx_wptr, *idx_ptr_crc;
long long idx_read_pos;		// corresponds to idx_wptr
long long idx_metafile_pos;	// corresponds to idx_wptr
unsigned read_crc32_acc = -1;

void idx_set_readpos (long long pos) {
  assert (pos <= idx_bytes && pos >= 0);
  assert (lseek (idx_fd, pos, SEEK_SET) == pos);
  idx_read_pos = pos;
  idx_metafile_pos = 0;
  read_crc32_acc = -1;
  idx_rptr = idx_wptr = idx_ptr_crc = RBuff;
}

void idx_fake_readpos (long long pos) {
  long len = RBuffEnd - RBuff;
  assert (len > 0 && pos + len <= idx_bytes && pos >= 0);
  idx_read_pos = pos + len;
  idx_metafile_pos = len;
  read_crc32_acc = -1;
  idx_rptr = idx_ptr_crc = RBuff;
  idx_wptr = RBuffEnd;
}

static inline unsigned idx_relax_crc32 (void) {
  if (idx_ptr_crc < idx_rptr) {
    idx_loaded_bytes += idx_rptr - idx_ptr_crc;
    read_crc32_acc = crc32_partial (idx_ptr_crc, idx_rptr - idx_ptr_crc, read_crc32_acc);
    idx_ptr_crc = idx_rptr;
  }
  return ~read_crc32_acc;
}

void idx_slide_read_buffer (void) {
  assert (idx_ptr_crc >= RBuff && idx_ptr_crc <= idx_rptr && idx_rptr <= idx_wptr && idx_wptr <= RBuffEnd);
  idx_relax_crc32 ();
  if (idx_rptr == RBuff) {
    return;
  }
  memmove (RBuff, idx_rptr, idx_wptr - idx_rptr);
  idx_wptr = RBuff + (idx_wptr - idx_rptr);
  idx_rptr = idx_ptr_crc = RBuff;
}

static inline long long idx_cur_read_pos (void) {
  return idx_read_pos - (idx_wptr - idx_rptr);
}

static inline long long idx_cur_metafile_pos (void) {
  return idx_metafile_pos - (idx_wptr - idx_rptr);
}

// after execution, next need_bytes will be contiguous at idx_rptr
int idx_load_next (int need_bytes) {
  assert (need_bytes > 0 && need_bytes <= RBuffEnd - RBuff);
  long long idx_cur_pos = idx_cur_read_pos ();
  if (need_bytes + idx_cur_pos > idx_bytes) {
    need_bytes = idx_bytes - idx_cur_pos;
    assert (need_bytes >= 0);
    if (!need_bytes) {
      vkprintf (4, "failed at pos %lld, ends at %lld bytes, need %d\n", idx_cur_pos, idx_bytes, need_bytes);
      return 0;
    }
  }
  assert (idx_rptr >= RBuff && idx_rptr <= idx_wptr && idx_wptr <= RBuffEnd);
  if (idx_wptr >= idx_rptr + need_bytes) {
    return idx_wptr - idx_rptr;
  }
  if (idx_rptr + need_bytes > RBuffEnd) {
    idx_slide_read_buffer ();
    assert (idx_rptr + need_bytes <= RBuffEnd);
  }
  int to_load = RBuffEnd - idx_wptr;
  long long idx_wptr_pos = idx_cur_pos + (idx_wptr - idx_rptr);
  if (idx_wptr_pos + to_load > idx_bytes) {
    to_load = idx_bytes - idx_wptr_pos;
  }
  assert (to_load > 0);
  int r = read (idx_fd, idx_wptr, to_load);
  if (r != to_load) {
    fprintf (stderr, "error reading %d bytes from snapshot %s at position %lld: only %d bytes read: %m\n", to_load, idx_filename, idx_read_pos, r);
    assert (r == to_load);
  } else if (verbosity > 3) {
    fprintf (stderr, "preloaded %d bytes from snapshot at position %lld\n", r, idx_read_pos);
  }

  idx_wptr += r;
  idx_read_pos += r;
  idx_metafile_pos += r;
  return idx_wptr - idx_rptr;
}

void idx_read_align (int alignment) {
  int to_skip = -idx_cur_read_pos() & (alignment - 1);
  if (to_skip > 0) {
    assert (idx_load_next (to_skip) >= to_skip);
    idx_rptr += to_skip;
  }
} 

int readin_int (void) {
  if (unlikely (idx_rptr + 4 > idx_wptr)) {
    assert (idx_load_next (4) >= 4);
  }
  int x = *((int *) idx_rptr);
  idx_rptr += 4;
  return x;
}

static inline void idx_check_crc (void) {
  unsigned crc32 = idx_relax_crc32 ();
  if ((unsigned) readin_int() != crc32) {
    fprintf (stderr, "crc32 mismatch while reading snapshot %s at position %lld\n", idx_filename, idx_cur_read_pos ());
    assert (0);
  }
}

hash_list_t *load_hashlist (void) {
  int n = readin_int ();
  //  fprintf (stderr, "loading hashlist, size = %d at %p\n", n, idx_rptr);
  assert (n >= 0 && n <= BUFFSIZE / 8);
  if (!n) {
    return 0;
  }
  assert (idx_load_next (n * 8) >= n * 8);
  hash_list_t *H = dyn_alloc (8 + n * 8, 8);
  assert (H);
  H->size = H->len = n;
  memcpy (H->A, idx_rptr, n * 8);
  idx_rptr += n * 8;
  return H;
}

int preload_string (void) {
  int have_bytes = idx_load_next (RBuffEnd - RBuff < 65536 ? RBuffEnd - RBuff : 65536);
  char *string_end = memchr (idx_rptr, 0, have_bytes <= 65536 ? have_bytes : 65536);
  assert (string_end);
  return string_end - idx_rptr;
}

char *load_string (void) {
  int len = preload_string ();
  if (!len) {
    ++idx_rptr;
    return 0;
  }
  char *str = idx_rptr;
  idx_rptr += len + 1;
  //  fprintf (stderr, "loaded string %s at %p\n", str, str);
  return exact_strdup (str, len);
}

treeref_t read_ad_tree (int nodes) {
  assert (nodes >= 0);
  if (!nodes) {
    return 0;
  }
  assert (idx_load_next (nodes * 8) >= nodes * 8);
  int *A = (int *) idx_rptr;
  idx_rptr += nodes * 8;
  if (!targeting_disabled) {
    return intree_build_from_list (AdSpace, A, nodes);
  }
  return 0;
}

void *load_index_part (void *data, long long offset, long long size, int max_size) {
  int r;
  static struct iovec io_req[2];
  static unsigned index_part_crc32;
  assert (size >= 0 && size <= (long long) (dyn_top - dyn_cur));
  assert (size <= max_size || !max_size);
  assert (offset >= 0 && offset <= idx_bytes && offset + size <= idx_bytes);
  if (!data) {
    data = zmalloc (size);
  } else if (data == (void *) -1) {
    data = malloc (size);
  }
  assert (lseek (idx_fd, offset, SEEK_SET) == offset);
  io_req[0].iov_base = data;
  io_req[0].iov_len = size;
  io_req[1].iov_base = &index_part_crc32;
  io_req[1].iov_len = 4;
  r = readv (idx_fd, io_req, 2);
  if (r != size + 4) {
    fprintf (stderr, "error reading data from index file: read %d bytes instead of %lld at position %lld: %m\n", r, size + 4, offset);
    assert (r == size + 4);
    return 0;
  }
  if (verbosity > 3) {
    fprintf (stderr, "loaded %lld bytes from index at position %lld\n", size + 4, offset);
  }
  unsigned data_crc32 = compute_crc32 (data, size);
  if (data_crc32 != index_part_crc32) {
    fprintf (stderr, "error reading %lld bytes from index file at position %lld: crc32 mismatch: expected %08x, actual %08x\n", size, offset, index_part_crc32, data_crc32);
    assert (data_crc32 == index_part_crc32);
    return 0;
  }
  idx_loaded_bytes += r;
  return data;
}

void idx_read_user (void) {
  assert (idx_load_next (sizeof (struct targ_index_user_v1)) >= sizeof (struct targ_index_user_v1));
  struct targ_index_user_v1 *T = (struct targ_index_user_v1 *) idx_rptr;

  assert (T->user_struct_magic == TARG_INDEX_USER_STRUCT_V1_MAGIC);
  int user_id = T->user_id;
  user_t *U = get_user_f (user_id);
  assert (U);

  U->prev_user_creations = 0;

  int user_active_ads_num = T->user_active_ads_num;
  int user_inactive_ads_num = T->user_inactive_ads_num;
  int user_clicked_ads_num = T->user_clicked_ads_num;

  rate_change (U, T->rate);
  U->last_visited = T->last_visited;

#define CPYP(field,Min)		assert (T->field >= (Min));	\
				U->field = T->field;
#define CPYB(field,Min,Max)	assert (T->field >= (Min) && T->field <= (Max)); \
				U->field = T->field;

  CPYP (uni_city, 0);
  CPYB (region, 0, MAX_REGION);
  memcpy (U->user_group_types, T->user_group_types, 16);
  CPYB (bday_day, 0, 31);
  CPYB (bday_month, 0, 12);
  if (T->bday_year) {
    CPYB (bday_year, 1900, 2017);
  }
  CPYB (political, 0, MAX_POLITICAL);
  CPYB (sex, 0, MAX_SEX);
  CPYB (mstatus, 0, MAX_MSTATUS);
  CPYB (uni_country, 0, 255);
  U->cute = T->cute;
  CPYB (privacy, 0, MAX_PRIVACY);
  CPYB (has_photo, 0, 255);
  CPYB (browser, 0, MAX_BROWSER);
  CPYB (operator, 0, MAX_OPERATOR);
  CPYB (timezone, -MAX_TIMEZONE, MAX_TIMEZONE);
  CPYB (height, 0, MAX_HEIGHT);
  CPYB (smoking, 0, MAX_SMOKING);
  CPYB (alcohol, 0, MAX_ALCOHOL);
  CPYB (ppriority, 0, MAX_PPRIORITY);
  CPYB (iiothers, 0, MAX_IIOTHERS);
  CPYB (cvisited, 0, MAX_CVISITED);
  CPYB (hidden, 0, MAX_HIDDEN);
  CPYB (gcountry, 0, MAX_GCOUNTRY);
  memcpy (U->custom_fields, T->custom_fields, 15);

#undef CPYB
#undef CPYP

#define CPYF(field,Min,Max)	assert (T->field >= (Min) && T->field <= (Max)); \
				int field = T->field;
  CPYF (edu_num, 0, 65536);
  CPYF (sch_num, 0, 65536);
  CPYF (work_num, 0, 65536);
  CPYF (addr_num, 0, 65536);
  CPYF (inter_num, 0, 65536);
  CPYF (mil_num, 0, 65536);
  CPYF (grp_num, 0, 1 << 21);
  CPYF (lang_num, 0, 65536);
#undef CPYF

  idx_rptr += offsetof (struct targ_index_user_v1, var_fields);

  U->name_hashes = load_hashlist ();
  U->inter_hashes = load_hashlist ();
  U->religion_hashes = load_hashlist ();
  U->hometown_hashes = load_hashlist ();
  U->proposal_hashes = load_hashlist ();

  U->name = load_string ();
  U->religion = load_string ();
  U->hometown = load_string ();
  U->proposal = load_string ();

  if (edu_num) {
    assert (idx_load_next (24 * edu_num) >= 24 * edu_num);
  }

  struct education **EE = &U->edu;
  while (edu_num-- > 0) {
    //    fprintf (stderr, "loading education at %p\n", idx_rptr);
    struct education *E = zmalloc (sizeof (struct education));
    *EE = E;
    EE = &E->next;
    memcpy (&E->grad_year, idx_rptr, 24);
    idx_rptr += 24;
  }
  *EE = 0;

  struct school **SS = &U->sch;
  while (sch_num-- > 0) {
    //    fprintf (stderr, "loading school at %p\n", idx_rptr);
    struct school *S = zmalloc (sizeof (struct school));
    *SS = S;
    SS = &S->next;
    assert (idx_load_next (sizeof (struct targ_index_school)) >= sizeof (struct targ_index_school));
    struct targ_index_school *T = (struct targ_index_school *) idx_rptr;
    S->city = T->city;
    S->school = T->school;
    S->start = T->start;
    S->finish = T->finish;
    S->grad = T->grad;
    S->country = T->country;
    S->sch_class = T->sch_class;
    S->sch_type = T->sch_type;
    idx_rptr += sizeof (struct targ_index_school);
    S->spec_hashes = load_hashlist ();
    S->spec = load_string ();
  }
  *SS = 0;

  struct company **CC = &U->work;
  while (work_num-- > 0) {
    //    fprintf (stderr, "loading company at %p\n", idx_rptr);
    struct company *C = zmalloc (sizeof (struct company));
    *CC = C;
    CC = &C->next;
    assert (idx_load_next (sizeof (struct targ_index_company)) >= sizeof (struct targ_index_company));
    struct targ_index_company *T = (struct targ_index_company *) idx_rptr;
    C->city = T->city;
    C->company = T->company;
    C->start = T->start;
    C->finish = T->finish;
    C->country = T->country;
    idx_rptr += sizeof (struct targ_index_company);
    C->name_hashes = load_hashlist ();
    C->job_hashes = load_hashlist ();
    C->company_name = load_string ();
    C->job = load_string ();
  }
  *CC = 0;

  struct address **AA = &U->addr;
  while (addr_num-- > 0) {
    //    fprintf (stderr, "loading address at %p\n", idx_rptr);
    struct address *A = zmalloc (sizeof (struct address));
    *AA = A;
    AA = &A->next;
    assert (idx_load_next (sizeof (struct targ_index_address)) >= sizeof (struct targ_index_address));
    struct targ_index_address *T = (struct targ_index_address *) idx_rptr;
    A->city = T->city;
    A->district = T->district;
    A->station = T->station;
    A->street = T->street;
    A->country = T->country;
    A->atype = T->atype;
    idx_rptr += sizeof (struct targ_index_address);
    A->house_hashes = load_hashlist ();
    A->name_hashes = load_hashlist ();
    A->house = load_string ();
    A->name = load_string ();
  }
  *AA = 0;

  struct interest **II = &U->inter;
  while (inter_num-- > 0) {
    assert (idx_load_next (2) >= 2);
    struct targ_index_interest *T = (struct targ_index_interest *) idx_rptr;
    assert (T->type <= MAX_INTERESTS && T->type > 0);
    idx_rptr += 2;

    int i_type = T->type, i_flags = T->flags, i_len = preload_string ();

    struct interest *I = zmalloc (offsetof (struct interest, text) + i_len + 1);
    *II = I;
    II = &I->next;

    I->type = i_type;
    I->flags = i_flags;

    memcpy (I->text, idx_rptr, i_len + 1);
    idx_rptr += i_len + 1;
  }
  *II = 0;

  idx_read_align (4);

  if (mil_num) {
    assert (idx_load_next (8 * mil_num) >= 8 * mil_num);
  }

  struct military **MM = &U->mil;
  while (mil_num-- > 0) {
    struct military *M = zmalloc (sizeof (struct military));
    *MM = M;
    MM = &M->next;
    memcpy (&M->unit_id, idx_rptr, 8);
    idx_rptr += 8;
  }
  *MM = 0;

  if (grp_num) {
    assert (idx_load_next (4 * grp_num) >= 4 * grp_num);
    struct user_groups *G = U->grp = zmalloc (sizeof (struct user_groups) + 4 * (grp_num + 2));
    G->cur_groups = grp_num;
    G->tot_groups = grp_num + 2;

    memcpy (G->G, idx_rptr, 4 * grp_num);
    idx_rptr += 4 * grp_num;
  }

  if (lang_num) {
    assert (idx_load_next (2 * lang_num) >= 2 * lang_num);
    struct user_langs *L = U->langs = zmalloc (sizeof (struct user_langs) + 2 * lang_num);
    L->cur_langs = lang_num;
    L->tot_langs = lang_num;

    memcpy (L->L, idx_rptr, 2 * lang_num);
    idx_rptr += 2 * lang_num;
  }

  idx_read_align (4);

  if (!targeting_disabled) {
    active_ad_nodes += user_active_ads_num;
    inactive_ad_nodes += user_inactive_ads_num;
    clicked_ad_nodes += user_clicked_ads_num;
  }

  U->active_ads = read_ad_tree (user_active_ads_num);
  U->inactive_ads = read_ad_tree (user_inactive_ads_num);
  U->clicked_ads = read_ad_tree (user_clicked_ads_num);

  /*
  intree_check_tree (AdSpace, U->active_ads);  
  intree_check_tree (AdSpace, U->inactive_ads);
  intree_check_tree (AdSpace, U->clicked_ads);
  */

  user_online_tree_insert (U);
}

static int *RetargetAdList, *LRUAdList;

void pa_sort (int *A, int b) {
  if (b <= 0) {
    return;
  }
  int h = get_ad (A[b >> 1])->retarget_time, i = 0, j = b;
  do {
    while (get_ad (A[i])->retarget_time < h) {
      i++;
    }
    while (get_ad (A[j])->retarget_time > h) {
      j--;
    }
    if (i <= j) {
      int t = A[i];
      A[i++] = A[j];
      A[j--] = t;
    }
  } while (i <= j);
  pa_sort (A, j);
  pa_sort (A + i, b - i);
}

void pal_sort (int *A, int b) {
  if (b <= 0) {
    return;
  }
  int h = get_ad (A[b >> 1])->disabled_since, i = 0, j = b;
  do {
    while (get_ad (A[i])->disabled_since < h) {
      i++;
    }
    while (get_ad (A[j])->disabled_since > h) {
      j--;
    }
    if (i <= j) {
      int t = A[i];
      A[i++] = A[j];
      A[j--] = t;
    }
  } while (i <= j);
  pal_sort (A, j);
  pal_sort (A + i, b - i);
}


void idx_read_ad (int ad_id, int is_ancient, int ad_descr_bytes) {
  int i;
  int magic = TARG_INDEX_AD_STRUCT_MAGIC_V1, ad_struct_size = sizeof (struct targ_index_advert_v1);
  if (Header.magic == TARG_INDEX_MAGIC_V2) {
    magic = TARG_INDEX_AD_STRUCT_MAGIC_V2;
    ad_struct_size = sizeof (struct targ_index_advert_v2);
  }
  assert (!targeting_disabled);
  assert (!is_ancient || !idx_cur_metafile_pos ());
  assert (idx_load_next (4) >= 4);

  if (*((int *) idx_rptr) == TARG_INDEX_AD_STRUCT_MAGIC_V3) {
    assert (magic == TARG_INDEX_AD_STRUCT_MAGIC_V2);
    magic = TARG_INDEX_AD_STRUCT_MAGIC_V3;
    ad_struct_size = sizeof (struct targ_index_advert_v3);
  } else if (*((int *) idx_rptr) == TARG_INDEX_AD_STRUCT_MAGIC_V4) {
    assert (magic == TARG_INDEX_AD_STRUCT_MAGIC_V2);
    magic = TARG_INDEX_AD_STRUCT_MAGIC_V4;
    ad_struct_size = sizeof (struct targ_index_advert_v4);
  }

    
  assert (idx_load_next (ad_struct_size) >= ad_struct_size);
  struct targ_index_advert_v4 *T = (struct targ_index_advert_v4 *) idx_rptr;
  idx_rptr += ad_struct_size;

  assert (T->ad_struct_magic == magic);
  assert (T->ad_id == ad_id && ad_id > 0 && ad_id < MAX_ADS);
  assert (!(T->flags & ~ADF_ALLOWED_INDEX_FLAGS));
  assert ((T->flags & ADF_ANCIENT) == is_ancient * ADF_ANCIENT);

  struct advert *A = get_ad_f (ad_id, 1);
  assert (A);

  A->flags = T->flags;
  if (A->flags & ADF_ON) {
    ++active_ads;
  }
  A->price = T->price;
  assert (A->price);
  A->retarget_time = T->retarget_time;
  A->disabled_since = T->disabled_since;
  A->userlist_computed_at = T->userlist_computed_at;
  A->users = T->users;
  A->ext_users = T->ext_users;
  A->l_clicked = T->l_clicked;
  A->click_money = T->click_money;
  A->views = T->views;

  /*tot_clicks += A->clicked;
  tot_click_money += A->click_money;
  tot_views += A->views;*/

  A->l_clicked_old = T->l_clicked_old;
  A->l_views = T->l_views;
  A->g_views = T->g_views;

  A->g_clicked_old = T->g_clicked_old;
  A->g_clicked = T->g_clicked;

  if (magic >= TARG_INDEX_AD_STRUCT_MAGIC_V2) {
    A->l_sump0 = T->l_sump0 * (1.0 / (1LL << 32));
    A->l_sump1 = T->l_sump1 * (1.0 / (1LL << 32));
    A->l_sump2 = T->l_sump2 * (1.0 / (1LL << 32));
    A->g_sump0 = T->g_sump0 * (1.0 / (1LL << 32));
    A->g_sump1 = T->g_sump1 * (1.0 / (1LL << 32));
    A->g_sump2 = T->g_sump2 * (1.0 / (1LL << 32));
    //fprintf (stderr, "Ad %d, l_sump0=%.6lf, l_sump1=%.6lf, l_sump2=%.6lf, g_sump0=%.6lf, g_sump1=%.6lf, g_sump2=%.6lf\n", A->ad_id, A->l_sump0, A->l_sump1, A->l_sump2, A->g_sump0, A->g_sump1, A->g_sump2);
  } else {
    assert (A->g_clicked == (A->g_clicked_old >> 31));
    A->g_clicked = 0;
  }

  if (magic >= TARG_INDEX_AD_STRUCT_MAGIC_V3) {
    assert (T->factor >= 1e5 && T->factor <= 1e6);
    assert (T->recent_views_limit >= 0);
    A->factor = T->factor * 1e-6;
    A->recent_views_limit = T->recent_views_limit;
  }

  if (magic >= TARG_INDEX_AD_STRUCT_MAGIC_V4) {
    A->domain = T->domain;
    A->category = T->category;
    A->subcategory = T->subcategory;
    A->group = T->group;
  }

  A->query = load_string ();
  idx_read_align (4);

  assert ((unsigned) A->users <= MAX_USERS);

  if (is_ancient) {
    int remaining = ad_descr_bytes - idx_cur_metafile_pos ();
    assert (remaining >= 4 && (remaining & 7) == 4);
    remaining >>= 3;
    while (remaining--) {
      int uid = readin_int ();
      int view_count = readin_int ();
      assert (uid >= 0 && uid <= idx_max_uid);
      if (User[uid]) {
	user_t *U = User[uid];
	assert (!intree_lookup (AdSpace, U->inactive_ads, ad_id));
	assert (!intree_lookup (AdSpace, U->clicked_ads, ad_id));
	treeref_t N = new_intree_node (AdSpace);
	TNODE (AdSpace, N)->x = ad_id;
	TNODE (AdSpace, N)->z = view_count;
	U->inactive_ads = intree_insert (AdSpace, U->inactive_ads, N);
	++inactive_ad_nodes;
      }
    }
    return;
  }

  assert (!A->users || idx_load_next (A->users * 4) >= A->users * 4);
  A->user_list = malloc (A->users * 4 + 4);
  memcpy (A->user_list, idx_rptr, A->users * 4);
  idx_rptr += A->users * 4;
  A->user_list[A->users] = 0x7fffffff;

  tot_userlists++;
  tot_userlists_size += A->users;

  for (i = 0; i < A->users; i++) {
    assert (A->user_list[i] < A->user_list[i+1]);
  }
  assert (A->user_list[0] >= 0);
  assert (!A->users || A->user_list[A->users - 1] < MAX_USERS);

  if ((A->flags & (ADF_PERIODIC | ADF_ON)) == (ADF_PERIODIC | ADF_ON)) {
    assert (A->retarget_time > 0);
    RetargetAdList[idx_periodic_ads++] = ad_id;
  }

  if (!(A->flags & ADF_ON)) {
    LRUAdList[idx_lru_ads++] = ad_id;
  }

  compute_estimated_gain (A);
}

int load_index (kfs_file_handle_t Index) {
  int r, fd; 
  long i;
  long long fsize;

  assert (Index);
  fd = Index->fd;
  fsize = Index->info->file_size - Index->offset;

  r = read (fd, &Header, sizeof (struct targ_index_header));
  if (r < 0) {
    fprintf (stderr, "error reading index file header: %m\n");
    return -3;
  }
  if (r < sizeof (struct targ_index_header)) {
    fprintf (stderr, "index file too short: only %d bytes read\n", r);
    return -2;
  }

  if (Header.magic != TARG_INDEX_MAGIC && Header.magic != TARG_INDEX_MAGIC_V2) {
    fprintf (stderr, "bad targ index file header\n");
    return -4;
  }

  unsigned hdr_crc32 = compute_crc32 (&Header, offsetof (struct targ_index_header, header_crc32));
  if (hdr_crc32 != Header.header_crc32) {
    fprintf (stderr, "index header crc32 mismatch: %08x expected, %08x actual\n", Header.header_crc32, hdr_crc32);
    return -4;
  }

  assert (Header.stemmer_version == stemmer_version);
  assert (Header.word_split_version == word_split_version);
  assert (check_listcomp_version (Header.listcomp_version));

  idx_filename = strdup (Index->info->filename);
  idx_bytes = fsize;
  idx_fd = fd;
  idx_loaded_bytes = sizeof (Header);

  assert ((unsigned)Header.tot_users <= (unsigned)Header.max_uid);
  assert ((unsigned)Header.max_uid < MAX_USERS);
  assert ((unsigned)Header.words < HASH_BUCKETS * 16);
  assert ((unsigned)Header.ads < MAX_ADS);

  assert (Header.log_split_min >= 0 && Header.log_split_max == Header.log_split_min + 1 && Header.log_split_max <= Header.log_split_mod);
  log_split_min = Header.log_split_min;
  log_split_max = Header.log_split_max;
  log_split_mod = Header.log_split_mod;
  log_last_ts = Header.log_timestamp;

  assert (Header.stats_data_offset >= sizeof (struct targ_index_header));
  if (!Header.recent_views_data_offset) {
    assert (Header.word_directory_offset == Header.stats_data_offset + sizeof (AdStats) + 4);
  } else {
    assert (Header.recent_views_data_offset == Header.stats_data_offset + sizeof (AdStats) + 4);
    assert (Header.word_directory_offset >= Header.recent_views_data_offset + 4 && Header.word_directory_offset <= Header.recent_views_data_offset + sizeof (struct cyclic_views_entry) * (CYCLIC_VIEWS_BUFFER_SIZE - 1) + 4);
    idx_recent_views = (Header.word_directory_offset - Header.recent_views_data_offset) / sizeof (struct cyclic_views_entry);
    assert (Header.word_directory_offset == Header.recent_views_data_offset + idx_recent_views * sizeof (struct cyclic_views_entry) + 4);
  }
  assert (Header.user_data_offset == Header.word_directory_offset + (Header.words + 1) * sizeof (struct targ_index_word_directory_entry) + 4);
  assert (Header.word_data_offset >= Header.user_data_offset + Header.tot_users * sizeof (struct targ_index_user_v1) + 4);
  assert (Header.fresh_ads_directory_offset >= Header.word_data_offset + 4);
  assert (Header.stale_ads_directory_offset >= Header.fresh_ads_directory_offset + 4);
  assert (Header.fresh_ads_data_offset >= Header.stale_ads_directory_offset + 4);
  assert (Header.fresh_ads_data_offset == Header.fresh_ads_directory_offset + sizeof (struct targ_index_ads_directory_entry) * (Header.ads + 2) + 8);

  idx_fresh_ads = (Header.stale_ads_directory_offset - Header.fresh_ads_directory_offset - 4) / sizeof (struct targ_index_ads_directory_entry) - 1;
  idx_stale_ads = (Header.fresh_ads_data_offset - Header.stale_ads_directory_offset - 4) / sizeof (struct targ_index_ads_directory_entry) - 1;
  assert (idx_fresh_ads + idx_stale_ads == Header.ads);

  assert (Header.stale_ads_directory_offset == Header.fresh_ads_directory_offset + (idx_fresh_ads + 1) * sizeof (struct targ_index_ads_directory_entry) + 4);
  assert (Header.fresh_ads_data_offset == Header.stale_ads_directory_offset + (idx_stale_ads + 1) * sizeof (struct targ_index_ads_directory_entry) + 4);

  assert (Header.stale_ads_data_offset >= Header.fresh_ads_data_offset + idx_fresh_ads * sizeof (struct targ_index_advert_v1) + 4);
  assert (Header.data_end >= Header.stale_ads_data_offset + idx_stale_ads * sizeof (struct targ_index_advert_v1) + 4);
  assert (Header.data_end == idx_bytes);

  tot_clicks = Header.tot_clicks;
  tot_views = Header.tot_views;
  tot_click_money = Header.tot_click_money;

  idx_rptr = idx_wptr = idx_ptr_crc = RBuff = malloc (BUFFSIZE);
  RBuffEnd = RBuff + BUFFSIZE;

  init_targ_data (0);

  load_index_part (&AdStats, Header.stats_data_offset, sizeof (AdStats), 0);

  idx_words = Header.words;
  idx_users = Header.tot_users;
  idx_ads = Header.ads;
  idx_max_uid = Header.max_uid;

  idx_worddir = load_index_part (0, Header.word_directory_offset, (idx_words + 1) * sizeof (struct targ_index_word_directory_entry), 1 << 30);

  int plast = -4;
  for (i = 0; i < idx_words; i++) {
    assert (idx_worddir[i + 1].word > idx_worddir[i].word);
    if (idx_worddir[i].data_offset >= 0) {
      assert (idx_worddir[i].data_offset >= plast + 4);
      plast = idx_worddir[i].data_offset;
    }
  }
  assert (idx_worddir[i].data_offset >= plast + 4);
  assert (((Header.word_data_offset + idx_worddir[i].data_offset + 7) & -4) == Header.fresh_ads_directory_offset);

  ocur_now = Header.log_timestamp;

  idx_set_readpos (Header.user_data_offset);
  while (idx_cur_read_pos () < Header.word_data_offset - 8) {
    idx_read_user ();
  }
  assert (readin_int () == TARG_INDEX_USER_DATA_END);
  idx_check_crc ();

  user_creations = 0;

  relax_online_tree ();

  assert (idx_cur_read_pos () == Header.word_data_offset);

  idx_word_data_bytes = idx_worddir[idx_words].data_offset;
  idx_word_data = load_index_part (0, Header.word_data_offset, idx_word_data_bytes, 1 << 30);

  if (!targeting_disabled) {
    idx_fresh_ad_dir = load_index_part ((void *) -1, Header.fresh_ads_directory_offset, (idx_fresh_ads + 1) * sizeof (struct targ_index_ads_directory_entry), 1 << 28);

    idx_stale_ad_dir = load_index_part (0, Header.stale_ads_directory_offset, (idx_stale_ads + 1) * sizeof (struct targ_index_ads_directory_entry), 1 << 28);

    RetargetAdList = malloc (idx_fresh_ads * 4 + 4);
    LRUAdList = malloc (idx_fresh_ads * 4 + 4);
    idx_periodic_ads = 0;

    idx_set_readpos (Header.fresh_ads_data_offset);

    for (i = 0; i < idx_fresh_ads; i++) {
      assert (idx_fresh_ad_dir[i+1].ad_id > idx_fresh_ad_dir[i].ad_id);
      assert (idx_fresh_ad_dir[i+1].ad_info_offset > idx_fresh_ad_dir[i].ad_info_offset + sizeof (struct targ_index_advert_v1));
      assert (idx_fresh_ad_dir[i].ad_info_offset == idx_cur_metafile_pos ());
      idx_read_ad (idx_fresh_ad_dir[i].ad_id, 0, idx_fresh_ad_dir[i+1].ad_info_offset - idx_fresh_ad_dir[i].ad_info_offset);
    }
    assert (idx_fresh_ad_dir[i].ad_info_offset == idx_cur_metafile_pos ());
    idx_check_crc ();
    assert (idx_cur_read_pos () == Header.stale_ads_data_offset);

    assert (idx_periodic_ads <= idx_fresh_ads);
    assert (idx_lru_ads <= idx_fresh_ads);
    pa_sort (RetargetAdList, idx_periodic_ads - 1);
    pal_sort (LRUAdList, idx_lru_ads - 1);

    for (i = 0; i < idx_periodic_ads; i++) {
      reinsert_retarget_ad_last (get_ad (RetargetAdList[i]));
    }

    for (i = 0; i < idx_lru_ads; i++) {
      reinsert_lru_ad_last (get_ad (LRUAdList[i]));
    }

    free (RetargetAdList);
    free (LRUAdList);

    assert (idx_stale_ad_dir[0].ad_id > 0 && idx_stale_ad_dir[0].ad_info_offset >= 0);
    for (i = 0; i < idx_stale_ads; i++) {
      assert (idx_stale_ad_dir[i+1].ad_id > idx_stale_ad_dir[i].ad_id);
      assert (idx_stale_ad_dir[i+1].ad_info_offset > idx_stale_ad_dir[i].ad_info_offset + sizeof (struct targ_index_advert_v1));
    }
    assert (!i || idx_stale_ad_dir[i-1].ad_id < MAX_ADS);
    assert (idx_stale_ad_dir[i].ad_info_offset == Header.data_end - Header.stale_ads_data_offset);

    if (i) {
      idx_max_stale_ad_id = idx_stale_ad_dir[i-1].ad_id;
      for (i = 0; i < idx_stale_ads; i++) {
	int ad_id = idx_stale_ad_dir[i].ad_id;
	AncientAdBitmap[ad_id >> 3] |= (1 << (ad_id & 7));
      }
    }

    free (idx_fresh_ad_dir);
    idx_fresh_ad_dir = 0;
  }

  if (Header.recent_views_data_offset && !targeting_disabled) {
    load_index_part (CViews, Header.recent_views_data_offset, idx_recent_views * sizeof (struct cyclic_views_entry), sizeof (CViews));
    CV_r = CViews;
    CV_w = CViews + idx_recent_views;
    for (i = 0; i < idx_recent_views; i++) {
      int ad_id = CViews[i].ad_id;
      struct advert *A = get_ad_f (ad_id, 0);
      if (A) {
	A->recent_views++;
      }
    }
  }

  free (RBuff);
  RBuffEnd = RBuff = 0;

  jump_log_pos = Header.log_pos;
  jump_log_ts = Header.log_timestamp;
  jump_log_crc32 = Header.log_pos_crc32;

  return 0;
}

int load_ancient_ad (struct advert *A) {
  assert (A);
  if (!(A->flags & ADF_ANCIENT)) {
    return 0;
  }
  if (verbosity > 1) {
    fprintf (stderr, "load_ancient_ad(%d) invoked\n", A->ad_id);
  }
  struct targ_index_ads_directory_entry *p = lookup_ancient_ad (A->ad_id);
  assert (p);
  long long pos = p[0].ad_info_offset;
  long long len = p[1].ad_info_offset - pos;
  pos += Header.stale_ads_data_offset;

  assert (pos > 0 && len > 0 && len <= (1 << 28) && pos + len <= idx_bytes);

  if (use_aio > 0) {
    return schedule_load_ancient_ad (A, pos, len);
  }

  RBuff = malloc (len);
  assert (RBuff);
  RBuffEnd = RBuff + len;
  long long keep_idx_bytes = idx_bytes;
  idx_bytes = pos + len;

  idx_set_readpos (pos);
  idx_read_ad (A->ad_id, 1, len);
  idx_check_crc ();

  idx_bytes = keep_idx_bytes;
  free (RBuff);
  RBuffEnd = RBuff = 0;

  A->flags &= ~ADF_ANCIENT;

  ancient_ads_pending--;
  ancient_ads_loaded++;
  ancient_ads_loaded_bytes += len;

  return 1;
}

int onload_ad_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 0) {
    fprintf (stderr, "onload_ad_metafile(%p,%d)\n", c, read_bytes);
  }
  struct aio_connection *a = (struct aio_connection *)c;
  core_mf_t *M = (core_mf_t *) a->extra;
  struct advert *A = M->ad;

  assert (a->basic_type == ct_aio);
  assert (A->mf == M);
  assert (M->aio == a);

  if (read_bytes < M->len) {
    aio_read_errors++;
    if (verbosity >= 0) {
      fprintf (stderr, "ERROR reading ad %d data from index at position %lld [pending aio queries: %lld]: read %d bytes out of %d: %m\n", A->ad_id, M->pos, active_aio_queries, read_bytes, M->len);
    }

    allocated_metafile_bytes -= M->len;
    allocated_metafiles--;
    M->aio = 0;

    free (M);
    A->mf = 0;

    return 0;
  }

  M->aio = 0;

  if (verbosity > 0) {
    fprintf (stderr, "*** Read ad %d data from index at position %lld: read %d bytes at address %p, magic %08x\n", A->ad_id, M->pos, read_bytes, M->data, * (int *) M->data);
  }

  RBuff = M->data;
  RBuffEnd = RBuff + M->len;
  long long keep_idx_bytes = idx_bytes;
  idx_bytes = M->pos + M->len;

  idx_fake_readpos (M->pos);
  idx_read_ad (A->ad_id, 1, M->len);
  idx_check_crc ();
  
  assert (idx_metafile_pos == M->len);

  idx_bytes = keep_idx_bytes;
  RBuffEnd = RBuff = 0;

  A->flags &= ~ADF_ANCIENT;

  ancient_ads_pending--;
  ancient_ads_aio_loaded++;
  ancient_ads_aio_loaded_bytes += M->len;

  allocated_metafile_bytes -= M->len;
  allocated_metafiles--;
  M->aio = 0;

  free (M);
  A->mf = 0;

  return 1;
}


/*
 *
 *	INDEX SAVE
 *
 */

struct targ_index_header NewHeader;
double idx_start_time, idx_end_time;

void init_new_header0 (void) {
  NewHeader.magic = -1;
  NewHeader.log_pos = (long long) log_cur_pos();
  NewHeader.log_timestamp = log_read_until;
  write_pos = 0;
  writeout (&NewHeader, sizeof(NewHeader));
}

int new_idx_users;
int new_stale_ads, new_fresh_ads;

#pragma pack(push, 4)
struct ad_user_view_triple {
  int ad_id, uid, view_count;
};
#pragma pack(pop)

struct targ_index_word_directory_entry *NewWordDir;
struct targ_index_ads_directory_entry *NewFreshAdsDir, *NewStaleAdsDir;
int new_idx_words, new_idx_words_short, new_worddir_size;
long long ancient_ad_users;
long long word_user_pairs;

static struct ad_user_view_triple *all_stale_ads_userlist, *all_stale_ads_userlist_ptr;
static int stale_ads_current_user;
static int new_fresh_addir_size, new_stale_addir_size;

static void sort_word_dictionary (struct targ_index_word_directory_entry *A, int b) {
  int i, j;
  hash_t h;
  struct targ_index_word_directory_entry t;
  if (b <= 0) {
    return;
  }
  i = 0;
  j = b;
  h = A[b >> 1].word;
  do {
    while (A[i].word < h) { i++; }
    while (A[j].word > h) { j--; }
    if (i <= j) {
      t = A[i];
      A[i++] = A[j];
      A[j--] = t;
    }
  } while (i <= j);
  sort_word_dictionary (A, j);
  sort_word_dictionary (A + i, b - i);
}

void init_new_word_directory (void) {
  int i, j = 0;
  struct hash_word *p;
  for (i = 0; i < HASH_BUCKETS; i++) {
    for (p = Hash[i]; p; p = p->next) {
      if (p->word_tree) {
	++j;
	assert ((unsigned) j < 0x7fffffff / sizeof (struct targ_index_word_directory_entry) - 1);
      }
    }
  }
  new_idx_words = j;
  new_worddir_size = (new_idx_words + idx_words + 1) * sizeof (struct targ_index_word_directory_entry);
  NewWordDir = malloc (new_worddir_size);
  for (i = 0, j = 0; i < HASH_BUCKETS; i++) {
    for (p = Hash[i]; p; p = p->next) {
      if (p->word_tree) {
	clear_tmp_word_data ();
	int ilen = get_idx_word_list_len (p->word);
	if (ilen == p->num) {
	  dyn_mark (0);
	  iterator_t I = build_word_iterator (p->word);
	  int res = I->pos;
	  dyn_release (0);
	  if (res == INFTY) {
	    continue;
	  }
	}
	NewWordDir[j].word = p->word;
	NewWordDir[j].data_offset = p->word_tree;
	++j;
      }
    }
  }
  assert (j <= new_idx_words);

  for (i = 0; i < idx_words; i++) {
    hash_t word = idx_worddir[i].word;
    p = get_hash_node (word, 0);
    if (!p || !p->word_tree) {
      NewWordDir[j].word = word;
      NewWordDir[j].data_offset = 0;
      j++;
    }
  }

  assert (j <= new_idx_words + idx_words);
  new_idx_words = j;

  NewWordDir[j].word = -1;
  NewWordDir[j].data_offset = -1;
  new_worddir_size = (new_idx_words + 1) * sizeof (struct targ_index_word_directory_entry);

  sort_word_dictionary (NewWordDir, j - 1);
}

static void sort_axz (struct ad_user_view_triple *A, long b) {
  long i, j;
  int ha, hu;
  struct ad_user_view_triple t;
  if (b <= 0) {
    return;
  }
  i = 0;
  j = b;
  ha = A[b >> 1].ad_id;
  hu = A[b >> 1].uid;
  do {
    while (A[i].ad_id < ha || (A[i].ad_id == ha && A[i].uid < hu)) { 
      i++; 
    }
    while (A[j].ad_id > ha || (A[j].ad_id == ha && A[j].uid > hu)) { 
      j--; 
    }
    if (i <= j) {
      t = A[i];
      A[i++] = A[j];
      A[j--] = t;
    }
  } while (i <= j);
  sort_axz (A, j);
  sort_axz (A + i, b - i);
}

void init_new_fresh_ads_directory (void) {
  int i;
  new_fresh_addir_size = sizeof (struct targ_index_ads_directory_entry) * (new_fresh_ads + 1);
  struct targ_index_ads_directory_entry *p = NewFreshAdsDir = malloc (new_fresh_addir_size);
  for (i = 0; i < MAX_ADS; i++) {
    struct advert *A = get_ad (i);
    if (A && !(A->flags & (ADF_ANCIENT | ADF_NEWANCIENT))) {
      assert (p - NewFreshAdsDir < new_fresh_ads);
      p->ad_id = A->ad_id;
      p->ad_info_offset = -1;
      ++p;
    }
  }
  assert (p - NewFreshAdsDir == new_fresh_ads);
  p->ad_id = 0x7fffffff;
  p->ad_info_offset = -1;
}

void init_new_stale_ads_directory (void) {
  int i;
  new_stale_addir_size = sizeof (struct targ_index_ads_directory_entry) * (new_stale_ads + 1);
  struct targ_index_ads_directory_entry *p = NewStaleAdsDir = malloc (new_stale_addir_size);
  for (i = 0; i < MAX_ADS; i++) {
    struct advert *A = get_ad (i);
    if (A && (A->flags & (ADF_ANCIENT | ADF_NEWANCIENT))) {
      assert (p - NewStaleAdsDir < new_stale_ads);
      p->ad_id = A->ad_id;
      p->ad_info_offset = -1;
      ++p;
    } else if (ad_is_ancient (i)) {
      assert (!A);
      p->ad_id = i;
      p->ad_info_offset = -1;
      ++p;
    }
  }
  assert (p - NewStaleAdsDir == new_stale_ads);
  p->ad_id = 0x7fffffff;
  p->ad_info_offset = -1;
}

static int WN, WU[MAX_USERS+2], WM[MAX_USERS+2];
static unsigned char WPacked[MAX_USERS*16+128];

/*static int keep_int (intree_t TC) {
  assert (TC->z > 0);
  if (WN <= MAX_USERS) {
    WU[WN] = TC->x;
    WM[WN++] = TC->z;
  }
  return 1;
  }*/


int write_word (hash_t word, treeref_t tree) {
  WN = 1;
  WU[0] = -1;

  //int num = intree_traverse (WordSpace, tree, keep_int);
  int num = 0;
  clear_tmp_word_data ();
  dyn_mark (0);
  struct mult_iterator *I = (struct mult_iterator *) build_word_iterator (word);
  while (I->pos < INFTY) {
    WU[++num] = I->pos;
    WM[num] = I->mult;
    I->jump_to ((iterator_t)I, I->pos + 1);
  }
  I = 0;
  dyn_release (0);
  assert (num > 0 && num <= MAX_USERS);
  WU[num+1] = max_uid + 1;

  word_user_pairs += num;

  struct bitwriter bw;
  int extra_bits;
  memset (WPacked, 0, 4);
  bwrite_init (&bw, WPacked, WPacked + sizeof (WPacked) - 128, 0);
  bwrite_gamma_code (&bw, num);
  bwrite_mlist (&bw, WU, WM, num, INTERPOLATIVE_CODE_JUMP_SIZE, &extra_bits);
  int bits_written = bwrite_get_bits_written (&bw);

  if (bits_written <= 31) {
    new_idx_words_short++;
    return (bswap_32 (*((unsigned *) WPacked)) >> 1) | (-1 << 31);
  }

  int res = get_metafile_pos ();

  assert (res >= 0);

  writeout (WPacked, (bits_written + 7) >> 3);
  return res;
}

static inline int get_list_len (void *ptr) {
  int l = 0;
  while (ptr) {
    ptr = *(void **) ptr;
    l++;
  }
  return l;
}

static void writeout_hashlist (hash_list_t *H) {
  if (!H) {
    writeout_int (0);
    return;
  }
  writeout_int (H->len);
  int i;
  for (i = 0; i < H->len; i++) {
    writeout_long (H->A[i]);
  }
}

static void writeout_string (char *str) {
  if (!str) {
    writeout_char (0);
  } else {
    writeout (str, strlen (str) + 1);
  }
}

static int return_one (intree_t TC) {
  return 1;
}

static int return_not_ancient (intree_t TC) {
  return !(get_ad (TC->x)->flags & ADF_NEWANCIENT);
}

static int writeout_xz (intree_t TC) {
  writeout_int (TC->x);
  writeout_int (TC->z);
  return 1;
}

static int writeout_xz_not_ancient (intree_t TC) {
  if (get_ad (TC->x)->flags & ADF_NEWANCIENT) {
    assert (TC->z);
    ancient_ad_users++;
    return 0;
  } else {
    writeout_int (TC->x);
    writeout_int (TC->z);
    return 1;
  }
}

static int store_axz_ancient (intree_t TC) {
  if (get_ad (TC->x)->flags & ADF_NEWANCIENT) {
    all_stale_ads_userlist_ptr->ad_id = TC->x;
    all_stale_ads_userlist_ptr->uid = stale_ads_current_user;
    all_stale_ads_userlist_ptr->view_count = TC->z;
    ++all_stale_ads_userlist_ptr;
    return 1;
  } else {
    return 0;
  }
}

static void idx_write_user (user_t *U) {
  assert (U);

  writeout_int (TARG_INDEX_USER_STRUCT_V1_MAGIC);
  writeout_int (U->user_id);
  writeout_int (intree_traverse (AdSpace, U->active_ads, return_one));
  writeout_int (intree_traverse (AdSpace, U->inactive_ads, return_not_ancient));
  writeout_int (intree_traverse (AdSpace, U->clicked_ads, return_one));
  writeout_int (U->rate >> 8);
  writeout_int (U->last_visited);
  writeout_int (U->uni_city);
  writeout_int (U->region);
  writeout (U->user_group_types, 16);
  writeout_char (U->bday_day);
  writeout_char (U->bday_month);
  writeout_short (U->bday_year);
  writeout_char (U->political);
  writeout_char (U->sex);
  writeout_char (U->mstatus);
  writeout_char (U->uni_country);
  writeout_char (U->cute);
  writeout_char (U->privacy);
  writeout_char (U->has_photo);
  writeout_char (U->browser);
  writeout_short (U->operator);
  writeout_short (U->timezone);
  writeout_char (U->height);
  writeout_char (U->smoking);
  writeout_char (U->alcohol);
  writeout_char (U->ppriority);
  writeout_char (U->iiothers);
  writeout_char (U->cvisited);
  writeout_char (U->hidden);
  writeout_char (U->gcountry);
  writeout (U->custom_fields, 15);

  writeout_ushort_check (get_list_len (U->edu));
  writeout_ushort_check (get_list_len (U->sch));
  writeout_ushort_check (get_list_len (U->work));
  writeout_ushort_check (get_list_len (U->addr));
  writeout_ushort_check (get_list_len (U->inter));
  writeout_ushort_check (get_list_len (U->mil));
  writeout_ushort_check (U->grp ? U->grp->cur_groups : 0);
  writeout_ushort_check (U->langs ? U->langs->cur_langs : 0);

  writeout_hashlist (U->name_hashes);
  writeout_hashlist (U->inter_hashes);
  writeout_hashlist (U->religion_hashes);
  writeout_hashlist (U->hometown_hashes);
  writeout_hashlist (U->proposal_hashes);

  writeout_string (U->name);
  writeout_string (U->religion);
  writeout_string (U->hometown);
  writeout_string (U->proposal);

  struct education *E;
  for (E = U->edu; E; E = E->next) {
    writeout_int (E->grad_year);
    writeout_int (E->chair);
    writeout_int (E->faculty);
    writeout_int (E->university);
    writeout_int (E->city);
    writeout_char (E->country);
    writeout_char (E->edu_form);
    writeout_char (E->edu_status);
    writeout_char (E->primary);
  }

  struct school *S;
  for (S = U->sch; S; S = S->next) {
    writeout_int (S->city);
    writeout_int (S->school);
    writeout_short (S->start);
    writeout_short (S->finish);
    writeout_short (S->grad);
    writeout_char (S->country);
    writeout_char (S->sch_class);
    writeout_char (S->sch_type);
    writeout_hashlist (S->spec_hashes);
    writeout_string (S->spec);
  }

  struct company *C;
  for (C = U->work; C; C = C->next) {
    writeout_int (C->city);
    writeout_int (C->company);
    writeout_short (C->start);
    writeout_short (C->finish);
    writeout_char (C->country);
    writeout_hashlist (C->name_hashes);
    writeout_hashlist (C->job_hashes);
    writeout_string (C->company_name);
    writeout_string (C->job);
  }

  struct address *A;
  for (A = U->addr; A; A = A->next) {
    writeout_int (A->city);
    writeout_int (A->district);
    writeout_int (A->station);
    writeout_int (A->street);
    writeout_char (A->country);
    writeout_char (A->atype);
    writeout_hashlist (A->house_hashes);
    writeout_hashlist (A->name_hashes);
    writeout_string (A->house);
    writeout_string (A->name);
  }

  struct interest *I;
  for (I = U->inter; I; I = I->next) {
    writeout_char (I->type);
    writeout_char (I->flags);
    writeout_string (I->text);
  }

  writeout_align (4);
  
  struct military *M;
  for (M = U->mil; M; M = M->next) {
    writeout_int (M->unit_id);
    writeout_short (M->start);
    writeout_short (M->finish);
  }

  if (U->grp) {
    writeout (U->grp->G, U->grp->cur_groups * 4);
  }

  if (U->langs) {
    writeout (U->langs->L, U->langs->cur_langs * 2);
  }

  writeout_align (4);

  intree_traverse (AdSpace, U->active_ads, writeout_xz);
  intree_traverse (AdSpace, U->inactive_ads, writeout_xz_not_ancient);
  intree_traverse (AdSpace, U->clicked_ads, writeout_xz);
}

static void write_ad (struct advert *A, int is_ancient) {
  assert (A);
  int new_flags = (A->flags & ~ADF_NEWANCIENT) | (A->flags & ADF_NEWANCIENT ? ADF_ANCIENT : 0); 
  assert (!(new_flags & ~ADF_ALLOWED_INDEX_FLAGS));
  writeout_int (TARG_INDEX_AD_STRUCT_MAGIC_V4);
  writeout_int (A->ad_id);
  writeout_int (new_flags);
  writeout_int (A->price);
  writeout_int (A->retarget_time);
  writeout_int (A->disabled_since);
  writeout_int (is_ancient ? 0 : A->userlist_computed_at);
  writeout_int (A->users);
  writeout_int (A->ext_users);
  writeout_int (A->l_clicked);
  writeout_long (A->click_money);
  writeout_int (A->views);
  writeout_int (A->l_clicked_old);
  writeout_int (A->l_views);
  writeout_int (A->g_clicked_old);
  writeout_int (A->g_clicked);
  writeout_long (A->g_views);
  writeout_long (A->l_sump0 * (1LL << 32) + 0.5);
  writeout_long (A->l_sump1 * (1LL << 32) + 0.5);
  writeout_long (A->l_sump2 * (1LL << 32) + 0.5);
  writeout_long (A->g_sump0 * (1LL << 32) + 0.5);
  writeout_long (A->g_sump1 * (1LL << 32) + 0.5);
  writeout_long (A->g_sump2 * (1LL << 32) + 0.5);
  writeout_int (A->factor * 1e6 + 0.499);
  writeout_int (A->recent_views_limit);
  writeout_int (A->domain);
  writeout_short (A->category);
  writeout_short (A->subcategory);
  writeout_int (A->group);

  assert (A->query);

  int len = strlen (A->query);
  assert (len <= MAX_QUERY_STRING_LEN);
  writeout (A->query, len + 1);
  writeout_align (4);
  
  if (!is_ancient) {
    assert (!(new_flags & ADF_ANCIENT));
  } else {
    assert ((new_flags & ADF_ANCIENT));
  }
}

void mark_ancient_ads (void) {
  int i, j = 0;
  for (i = 0; i < MAX_ADS; i++) {
    struct advert *A = get_ad (i);
    if (A) {
      if (ad_became_ancient (A)) {
	A->flags |= ADF_NEWANCIENT;
	j++;
      } else {
	if (!(A->flags & ADF_ANCIENT)) {
	  ++new_fresh_ads;
	} else {
	  j++;
	}
      }
    } else if (ad_is_ancient (i)) {
      j++;
    }
  }
  new_stale_ads = j;
}

long long fresh_ads_descr_bytes, tot_fresh_ads_userlist_bytes, tot_stale_ads_userlist_bytes;
extern double binlog_load_time;

static void output_index_stats (void) {
  fprintf (stderr, "binlog loaded in %.3f seconds, binlog position %lld, timestamp %d\n", binlog_load_time, log_cur_pos (), log_last_ts);
  fprintf (stderr, "word directory: %d words, %lld bytes, %d short words\n", new_idx_words, NewHeader.user_data_offset - NewHeader.word_directory_offset, new_idx_words_short);
  fprintf (stderr, "user data: %d users, max_uid=%d, %lld bytes\n", tot_users, max_uid, NewHeader.word_data_offset - NewHeader.user_data_offset);
  fprintf (stderr, "word data: %d words, %lld bytes, %lld word-user pairs\n", new_idx_words - new_idx_words_short, NewHeader.fresh_ads_directory_offset - NewHeader.word_data_offset, word_user_pairs);
  fprintf (stderr, "fresh ads: %d ads, %lld bytes in directory, %lld ad info bytes (%lld of them in userlists)\n", new_fresh_ads, NewHeader.stale_ads_directory_offset - NewHeader.fresh_ads_directory_offset, NewHeader.stale_ads_data_offset - NewHeader.fresh_ads_data_offset, tot_fresh_ads_userlist_bytes);
  fprintf (stderr, "stale ads: %d ads, %lld bytes in directory, %lld ad info bytes (%lld of them in userlists)\n", new_stale_ads, NewHeader.fresh_ads_data_offset - NewHeader.stale_ads_directory_offset, NewHeader.data_end - NewHeader.stale_ads_data_offset, tot_stale_ads_userlist_bytes);
  fprintf (stderr, "loaded %d ancient ads, %lld bytes\n", ancient_ads_loaded, ancient_ads_loaded_bytes);
  fprintf (stderr, "total index size %lld bytes\n", NewHeader.data_end);
  fprintf (stderr, "index generated in %.3f seconds, used %ld dyn_heap bytes, %lld heap bytes for %d userlists, %d+%d treespace ints\n", idx_end_time - idx_start_time, (long) (dyn_cur - dyn_first + dyn_last - dyn_top), tot_userlists_size << 2, tot_userlists, ((struct treespace_header *)AdSpace)->used_ints, ((struct treespace_header *)WordSpace)->used_ints);
}


void idx_copy_part (long len) {
  while (len > 0) {
    int to_load = len < BUFFSIZE ? len : BUFFSIZE;
    int bytes = idx_load_next (to_load);
    assert (bytes > 0);
    if (bytes > to_load) {
      assert (bytes <= to_load + 4);
      bytes = to_load;
    }
    writeout (idx_rptr, bytes);
    idx_rptr += bytes;
    len -= bytes;
  }
  assert (!len);
}

int write_index (int writing_binlog) {
  int i;

  if (targeting_disabled) {
    kprintf ("not allowed to generate snapshot in search-only mode\n");
    return 0;
  } 

  use_aio = 0;

  if (delay_targeting) {
    perform_delayed_ad_activation ();
  }

  if (log_cur_pos() == jump_log_pos) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot %s for this position already exists\n",
       newidxname, jump_log_pos, idx_filename ? idx_filename : "(empty)");
    return 0;
  } 

  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  idx_start_time = get_utime(CLOCK_MONOTONIC);

  if (verbosity > 0) {
    fprintf (stderr, "creating index %s at log position %lld\n", newidxname, log_cur_pos());
  }

  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }

  newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);
  if (newidx_fd < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  process_lru_ads ();
  mark_ancient_ads ();

  init_new_header0();

  NewHeader.stats_data_offset = get_write_pos ();

  initcrc ();
  writeout (&AdStats, sizeof (AdStats));
  writecrc ();

  NewHeader.recent_views_data_offset = get_write_pos ();

  forget_old_views();
  initcrc ();
  if (CV_r <= CV_w) {
    writeout (CV_r, (char *) CV_w - (char *) CV_r);
  } else {
    writeout (CV_r, (char *) (CViews + CYCLIC_VIEWS_BUFFER_SIZE) - (char *) CV_r);
    writeout (CViews, (char *) CV_w - (char *) CViews);
  }
  writecrc ();
    

  NewHeader.tot_users = new_idx_users = tot_users;
  NewHeader.max_uid = max_uid;

  NewHeader.tot_clicks = tot_clicks;
  NewHeader.tot_click_money = tot_click_money;
  NewHeader.tot_views = tot_views;

  NewHeader.word_directory_offset = get_write_pos ();

  init_new_word_directory ();

  NewHeader.words = new_idx_words;
  writeout (NewWordDir, new_worddir_size);
  writeout_int (-1);

  NewHeader.user_data_offset = get_write_pos ();
  reset_metafile_pos ();
  initcrc ();

  for (i = 0; i < MAX_USERS; i++) {
    if (User[i]) {
      idx_write_user (User[i]);
    }
  }

  writeout_int (TARG_INDEX_USER_DATA_END);
  writecrc ();

  initcrc ();
  reset_metafile_pos ();
  NewHeader.word_data_offset = get_write_pos ();

  for (i = 0; i < new_idx_words; i++) {
    NewWordDir[i].data_offset = write_word (NewWordDir[i].word, NewWordDir[i].data_offset);
  }
  NewWordDir[i].data_offset = get_metafile_pos ();
  writecrc ();

  writeout_align (4);

  NewHeader.fresh_ads_directory_offset = get_write_pos ();
  init_new_fresh_ads_directory ();
  writeout (NewFreshAdsDir, new_fresh_addir_size);
  writeout_int (-1);
  
  NewHeader.stale_ads_directory_offset = get_write_pos ();
  init_new_stale_ads_directory ();
  writeout (NewStaleAdsDir, new_stale_addir_size);
  writeout_int (-1);

  initcrc ();
  reset_metafile_pos ();
  NewHeader.fresh_ads_data_offset = get_write_pos ();

  for (i = 0; i < new_fresh_ads; i++) {
    struct advert *A = get_ad_f (NewFreshAdsDir[i].ad_id, 0);
    assert (A && !(A->flags & (ADF_ANCIENT | ADF_NEWANCIENT)));
    NewFreshAdsDir[i].ad_info_offset = get_metafile_pos ();
    write_ad (A, 0);
    assert (A->user_list);
    writeout (A->user_list, A->users * 4);
    tot_fresh_ads_userlist_bytes += A->users * 4;
  }
  NewFreshAdsDir[i].ad_info_offset = get_metafile_pos ();
  writecrc ();

  all_stale_ads_userlist_ptr = all_stale_ads_userlist = malloc ((ancient_ad_users + 1) * sizeof (struct ad_user_view_triple));
  int tot = 0;
  for (i = 0; i < MAX_USERS; i++) {
    if (User[i]) {
      stale_ads_current_user = i;
      tot += intree_traverse (AdSpace, User[i]->inactive_ads, store_axz_ancient);
    }
  }
  assert (tot == ancient_ad_users && all_stale_ads_userlist_ptr == all_stale_ads_userlist + ancient_ad_users);
  all_stale_ads_userlist_ptr->ad_id = 0x7fffffff;

  sort_axz (all_stale_ads_userlist, tot - 1);
  
  reset_metafile_pos ();
  NewHeader.stale_ads_data_offset = get_write_pos ();

  idx_rptr = idx_wptr = idx_ptr_crc = RBuff = malloc (BUFFSIZE);
  RBuffEnd = RBuff + BUFFSIZE;
  
  struct ad_user_view_triple *ptr = all_stale_ads_userlist;
  for (i = 0; i < new_stale_ads; i++) {
    NewStaleAdsDir[i].ad_info_offset = get_metafile_pos ();
    struct advert *A = get_ad (NewStaleAdsDir[i].ad_id);
    if (!A || (A->flags & ADF_ANCIENT)) {
      struct targ_index_ads_directory_entry *p = lookup_ancient_ad (NewStaleAdsDir[i].ad_id);
      assert (p);
      long long pos = p[0].ad_info_offset;
      long long len = p[1].ad_info_offset - pos;
      pos += Header.stale_ads_data_offset;
      
      assert (pos > 0 && len > 0 && len <= (1 << 28) && pos + len <= idx_bytes);

      long long keep_idx_bytes = idx_bytes;
      idx_bytes = pos + len;

      initcrc ();
      idx_set_readpos (pos);
      idx_copy_part (len - 4);
      idx_check_crc ();

      idx_bytes = keep_idx_bytes;
    } else {
      assert (A && (A->flags & (ADF_ANCIENT | ADF_NEWANCIENT)) == ADF_NEWANCIENT);
      initcrc ();
      write_ad (A, 1);
      assert (ptr->ad_id >= A->ad_id);
      while (ptr->ad_id == A->ad_id) {
	writeout_int (ptr->uid);
	writeout_int (ptr->view_count);
	ptr++;
      }
    }
    writecrc ();
    assert (ptr->ad_id > NewStaleAdsDir[i].ad_id);
  }
  tot_stale_ads_userlist_bytes = ancient_ad_users * 8LL;

  NewStaleAdsDir[i].ad_info_offset = get_metafile_pos ();
  assert (ptr == all_stale_ads_userlist_ptr && ptr->ad_id == 0x7fffffff);

  NewHeader.data_end = get_write_pos ();

  write_seek (NewHeader.word_directory_offset);
  initcrc ();
  writeout (NewWordDir, new_worddir_size);
  writecrc ();

  write_seek (NewHeader.fresh_ads_directory_offset);
  initcrc ();
  writeout (NewFreshAdsDir, new_fresh_addir_size);
  writecrc ();

  assert (get_write_pos() == NewHeader.stale_ads_directory_offset);
  initcrc ();
  writeout (NewStaleAdsDir, new_stale_addir_size);
  writecrc ();

  write_seek (0);

  NewHeader.magic = TARG_INDEX_MAGIC_V2;
  NewHeader.created_at = time(0);
  NewHeader.log_pos = log_cur_pos();
  NewHeader.log_pos_crc32 = ~log_crc32_complement;
  NewHeader.log_timestamp = log_last_ts;
  NewHeader.log_split_min = log_split_min;
  NewHeader.log_split_max = log_split_max;
  NewHeader.log_split_mod = log_split_mod;
  NewHeader.ads = new_fresh_ads + new_stale_ads;
  NewHeader.stemmer_version = stemmer_version;
  NewHeader.word_split_version = word_split_version;
  NewHeader.listcomp_version = listcomp_version;

  NewHeader.header_crc32 = compute_crc32 (&NewHeader, offsetof (struct targ_index_header, header_crc32));
  writeout (&NewHeader, sizeof (NewHeader));

  flushout ();
  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  idx_end_time = get_utime(CLOCK_MONOTONIC);

  if (verbosity > 0) {
    output_index_stats ();
  }

  print_snapshot_name (newidxname);

  return 0;
}
