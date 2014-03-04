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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Vitaliy Valtman
                   2011 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include "pmemcached-index-disk.h"
#include "pmemcached-data.h"
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include "crc32.h"

#include "vv-tl-aio.h"

struct metafile metafiles[MAX_METAFILES];
int next_use[MAX_METAFILES+1];
int prev_use[MAX_METAFILES+1];
int idx_fd, newidx_fd;

extern long long index_size;
int metafile_number;
extern long long snapshot_size;
extern int verbosity;
extern int hash_st[];

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;

long long global_offset;
extern long long init_memory;

char buffer_meta [MAX_METAFILE_SIZE];
int buffer_meta_pos, buffer_meta_number;
long long buffer_shifts [MAX_METAFILE_ELEMENTS];
char buffer_meta_key [MAX_METAFILE_SIZE];
int buffer_meta_key_len;

int iterator_metafile_number, iterator_metafile_position, iterator_metafile;
extern long long allocated_metafile_bytes;
long long metafiles_load_errors;
long long metafiles_load_success;
long long metafiles_unload_LRU;
long long tot_aio_loaded_bytes;

struct index_entry *index_iterator;

struct index_entry index_entry_tmp;
struct metafile_header metafile_header_tmp;

extern struct index_entry index_entry_not_found;
extern int index_type;
extern long long memory_for_metafiles;
extern int metafile_size;

int metafiles_loaded = 0;
int use_metafile_crc32;


void load_metafile (int metafile);


/*
 *
 * GENERIC BUFFERED WRITE
 *
 */

#define	BUFFSIZE	16777216

char Buff[BUFFSIZE], *rptr = Buff, *wptr = Buff, *wptr0 = Buff;
long long write_pos;
int metafile_pos;
int newidx_fd;

static inline long long get_write_pos (void) {
  int s = wptr - wptr0;
  wptr0 = wptr;
  metafile_pos += s;
  return write_pos += s;
}

static inline int get_metafile_pos (void) {
  int s = wptr - wptr0;
  wptr0 = wptr;
  write_pos += s;
  return metafile_pos += s;
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
  }
  rptr = wptr = wptr0 = Buff;
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

#define likely(x) __builtin_expect((x),1) 
#define unlikely(x) __builtin_expect((x),0)

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

static inline void writeout_char (char value) {
  if (unlikely (wptr == Buff + BUFFSIZE)) {
    flushout();
  }
  *wptr++ = value;
}

void write_seek (long long new_pos) {
  flushout();
  assert (lseek (newidx_fd, new_pos, SEEK_SET) == new_pos);
  write_pos = new_pos;
}



inline struct index_entry* metafile_get_entry ( int metafile, int idx ) {
  return (struct index_entry *)&metafiles[metafile].data[metafiles[metafile].local_offsets[idx]];
}

void del_use (int metafile_number) {
  assert (0 <= metafile_number && metafile_number < MAX_METAFILES);
  prev_use[next_use[metafile_number]] = prev_use[metafile_number];
  next_use[prev_use[metafile_number]] = next_use[metafile_number];  
}

void add_use (int metafile_number) {
  assert (0 <= metafile_number && metafile_number < MAX_METAFILES);
  prev_use[metafile_number] = MAX_METAFILES;
  next_use[metafile_number] = next_use[MAX_METAFILES];
  next_use[MAX_METAFILES] = metafile_number;
  prev_use[next_use[metafile_number]] = metafile_number;
}

void renew_use (int metafile_number) {
  assert (0 <= metafile_number && metafile_number < MAX_METAFILES);
  del_use (metafile_number);
  add_use (metafile_number);
}


int metafile_unload (int metafile_number) {
  assert (0 <= metafile_number && metafile_number < MAX_METAFILES);
  if (verbosity >= 3) {
    fprintf (stderr, "unloading metafile %d\n", metafile_number);
  }
  struct metafile *meta = &metafiles[metafile_number];
  if (meta->data == 0) {
    return -1;
  }
  if (meta->aio) {
    return -1;
  }
  zzfree (meta->local_offsets, meta->header->metafile_size);
  meta->data = 0;
  meta->local_offsets = 0;
  metafiles_loaded--;
  allocated_metafile_bytes -= meta->header->metafile_size;
  del_use (metafile_number);
  return 0;
}


int use_query_fails = 0;


int metafile_unload_LRU() {
  if (prev_use[MAX_METAFILES] == MAX_METAFILES) {
    return 0;
  }
  use_query_fails = 0;
  int cur = prev_use[MAX_METAFILES];
  while (cur != MAX_METAFILES) {
    if (metafile_unload (cur) == 0) {
      metafiles_unload_LRU++;
      return 1;
    } else {
      use_query_fails ++;
      cur = prev_use [cur];
    }
  }
  return 0;
}

void free_metafiles () {
  while (allocated_metafile_bytes > 0.9 * memory_for_metafiles) {
    if (!metafile_unload_LRU ()) {
      break;
    }
  }
}


int metafile_load (int metafile_number) {
  if (verbosity >= 3) {
    fprintf (stderr, "loading metafile %d\n", metafile_number);
  }
  assert (0 <= metafile_number && metafile_number < MAX_METAFILES);
  struct metafile *meta = &metafiles[metafile_number];
  if (meta->aio) {
    fprintf (stderr, "meta->aio != 0. Dropping data\n");
    meta->aio = 0;
    meta->data = 0;
    meta->local_offsets = 0;
  }
  if (meta->data != 0) {
    return 1;
  }

  //if (metafiles_loaded == MAX_METAFILES_LOADED) {
  //  metafile_unload_LRU();
  //}

  free_metafiles ();

  assert (lseek (idx_fd, meta->header->global_offset + meta->header->local_offset, SEEK_SET) == meta->header->global_offset + meta->header->local_offset);
  assert (meta->local_offsets == 0);

  meta->local_offsets = zzmalloc (meta->header->metafile_size);
  while  (!meta->local_offsets) {
    if (!metafile_unload_LRU()) {
      fprintf (stderr, "No memory\n");
      return 0;
    }
    meta->local_offsets = zzmalloc (meta->header->metafile_size);
  }

  meta->data = (char *) (meta->local_offsets + meta->header->nrecords);
  assert (read (idx_fd, meta->local_offsets, meta->header->metafile_size) == meta->header->metafile_size);
  if (use_metafile_crc32) {
    crc32_check_and_repair (meta->local_offsets, meta->header->metafile_size, &meta->header->crc32, 1);
  }
  if (verbosity >= 4 && meta->data) {
    int i;
    for (i = 0; i < meta->header->nrecords; i++) {
      fprintf (stderr, "key/data - %d/%d - %s\n", metafile_get_entry (metafile_number, i)->key_len, metafile_get_entry (metafile_number, i)->data_len, metafile_get_entry (metafile_number, i)->data);
    }
  }
  metafiles_loaded++;
  allocated_metafile_bytes += meta->header->metafile_size;
  add_use (metafile_number);
  return 0;
}





long long metafiles_cache_miss;
long long metafiles_cache_ok;

struct index_entry* index_get (const char *key, int key_len) {
  int l = -1;
  int r = metafile_number;
  while (r-l > 1) {
    int x = (r+l)>>1;
    if (mystrcmp(key, key_len, metafiles[x].header->key, metafiles[x].header->key_len) < 0) {
      r = x;
    } else {
      l = x;
    }
  }
  if (l < 0) {
    if (verbosity>=4) { fprintf (stderr, "not found[1]\n"); }
    return &index_entry_not_found;
  }
  if (verbosity>=4) {
    fprintf (stderr, "(l,r) = (%d,%d)\n", l, r);
  }
  if (metafiles[l].data == 0 || metafiles[l].aio) {
    load_metafile (l);
	  if (metafiles[l].data == 0 || metafiles[l].aio) {
      metafiles_cache_miss ++;
  	  return 0;
  	}
  }
  metafiles_cache_ok ++;
  renew_use (l);
  int metafile = l;
  struct metafile* meta = &metafiles[l];

  l = -1;
  r = meta->header->nrecords;
  while (r-l > 1) {
    int x = (r+l)>>1;
    if (mystrcmp(key, key_len, metafile_get_entry (metafile, x)->data, metafile_get_entry (metafile, x)->key_len) < 0) {
      r = x;
    } else {
      l = x;
    }
  }
  if (verbosity>=4) {
    fprintf (stderr, "(l,r) = (%d,%d)\n", l, r);
  }
  
  if (l < 0) {
    if (verbosity>=4) { fprintf (stderr, "not found[2]\n"); }
    return &index_entry_not_found;
  }
  struct index_entry *E = metafile_get_entry (metafile, l);
  if (verbosity >= 4) {
    fprintf (stderr, "metafile_get_entry (%d, %d)->key = ", metafile, l);
    debug_dump_key (E->data, E->key_len);
  }
  if (mystrcmp(key, key_len, E->data, E->key_len) == 0) {
    if (verbosity>=4) {
      fprintf (stderr, "Found. l=%d\n", l);
    }
    return metafile_get_entry (metafile, l);
  }
  
  if (verbosity>=4) { fprintf (stderr, "not found[3]\n"); }
  return &index_entry_not_found;
}

struct index_entry* index_get_num (int n, int use_aio) {
  int l = 0;  
  int sum = 0;
  while (l < metafile_number && n >= sum + metafiles[l].header->nrecords) {
    sum += metafiles[l].header->nrecords;
    l++;
  }
  if (l == metafile_number) {
    return &index_entry_not_found;
  }
  metafile_load (l);
  struct metafile* meta = &metafiles[l];
  assert (meta->data);
  assert (n - sum >= 0);
  assert (n - sum < meta->header->nrecords);
  
  return metafile_get_entry (l, n - sum);
}

#define min(x,y) ((x) < (y) ? (x) : (y))
struct index_entry* index_get_next (const char *key, int key_len) {
  int l = -1;
  int r = metafile_number;
  int lc = 1;
  int rc = 1;
  while (r - l > 1) {
    int x = (r + l) >> 1;
    int c = mystrcmp2 (key, key_len, metafiles[x].header->key, metafiles[x].header->key_len, min (lc, rc) - 1);
    if (c < 0) {
      r = x;
      rc = -c;
    } else if (c > 0) {
      l = x;
      lc = c;
    } else {
      l = x;
      break;
    }
  }
  if (l < 0) { l = 0; }
  if (l >= metafile_number) {
    if (verbosity >= 4) { fprintf (stderr, "not found[1]\n"); }
    return &index_entry_not_found;
  }
  if (verbosity >= 4) {
    fprintf (stderr, "(l,r) = (%d,%d)\n", l, r);
  }
  int ll = l;
  int ll_start = ll;
  while (1) {
    assert (ll_start - ll < 2);
    if (ll == metafile_number) {
      return &index_entry_not_found;
    }
    if (metafiles[ll].data == 0 || metafiles[ll].aio) {
      load_metafile (ll);
	    if (metafiles[ll].data == 0 || metafiles[ll].aio) {
        metafiles_cache_miss ++;
    	  return 0;
    	}
    }
    metafiles_cache_ok ++;
    renew_use (ll);
    int metafile = ll;
    struct metafile* meta = &metafiles[ll];

    l = -1;
    r = meta->header->nrecords;
    lc = 1;
    rc = 1;
    while (r-l > 1) {
      int x = (r+l)>>1;
      int c = mystrcmp2 (key, key_len, metafile_get_entry (metafile, x)->data, metafile_get_entry (metafile, x)->key_len, min (lc, rc) - 1); 
      if (c < 0) {
        r = x;
        rc = -c;
      } else if (c > 0) {
        l = x;
        lc = c;
      } else {
        l = x; 
        break;
      }
    }
    if (verbosity>=4) {
      fprintf (stderr, "(l,r) = (%d,%d)\n", l, r);
    }
    l ++;

    if (l == meta->header->nrecords) {
      ll ++;
      continue;
    }
  
    struct index_entry *E = metafile_get_entry (metafile, l);
    if (verbosity >= 4) {
      fprintf (stderr, "metafile_get_entry (%d, %d)->key = ", metafile, l);
      debug_dump_key (E->data, E->key_len);
    }
    return metafile_get_entry (metafile, l);
  }
}

int write_buffer_number = -1;

void buffer_meta_init () {
  buffer_meta_pos = 0;
  buffer_meta_number = 0;
  buffer_shifts[0] = 0;
  write_buffer_number++;
}


void buffer_meta_init_key (const char *key, int key_len) { 
  memcpy (buffer_meta_key, key, key_len);
  buffer_meta_key_len = key_len;
}

void buffer_meta_hash (hash_entry_t* hash_entry) {
  if (verbosity >= 3) {
    fprintf (stderr, "Data from hash_entry\n");
    fprintf (stderr, "key_len = %d\n", hash_entry->key_len);
    fprintf (stderr, "key = %p\n", hash_entry->key);
//    fprintf (stderr, "key = %.1000s\n", hash_entry->key);
    fprintf (stderr, "data_len = %d\n", hash_entry->data_len);
  }
  if (buffer_meta_pos == 0) {
    buffer_meta_init_key (hash_entry->key, hash_entry->key_len);
  }
  index_entry_tmp.key_len = hash_entry->key_len;
  index_entry_tmp.data_len = hash_entry->data_len;
  index_entry_tmp.flags = hash_entry->flags;
  index_entry_tmp.delay = hash_entry->exp_time;
  assert (buffer_meta_pos + sizeof (struct index_entry) + hash_entry->key_len + hash_entry->data_len + 1 < MAX_METAFILE_SIZE);
  memcpy (buffer_meta + buffer_meta_pos, &index_entry_tmp, sizeof (struct index_entry));
  buffer_meta_pos += sizeof (struct index_entry);
  memcpy (buffer_meta + buffer_meta_pos, hash_entry->key, hash_entry->key_len);
  buffer_meta_pos += hash_entry->key_len;
  memcpy (buffer_meta + buffer_meta_pos, hash_entry->data, hash_entry->data_len);
  buffer_meta_pos += hash_entry->data_len;
  buffer_meta[buffer_meta_pos++] = 0;
  assert (buffer_meta_number < MAX_METAFILES - 1);
  buffer_shifts[++buffer_meta_number] = buffer_meta_pos;
}

void buffer_meta_index (struct index_entry* index_entry) {
  if (verbosity >= 3) {
    fprintf (stderr, "Data from index_entry\n");
  }
  if (buffer_meta_pos == 0) {
    buffer_meta_init_key (index_entry->data, index_entry->key_len);
  }
  assert (buffer_meta_pos + sizeof (struct index_entry) + index_entry->key_len + index_entry->data_len + 1 < MAX_METAFILE_SIZE);
  memcpy (buffer_meta + buffer_meta_pos, index_entry, sizeof(struct index_entry) + index_entry->key_len + index_entry->data_len + 1);
  buffer_meta_pos += sizeof(struct index_entry) + index_entry->key_len + index_entry->data_len + 1;
  assert (buffer_meta_number < MAX_METAFILES - 1);
  buffer_shifts[++buffer_meta_number] = buffer_meta_pos;
}

void buffer_meta_flush () {
  metafile_header_tmp.global_offset = global_offset;
  metafile_header_tmp.local_offset = sizeof (struct metafile_header) + buffer_meta_key_len;
  metafile_header_tmp.key_len = buffer_meta_key_len;
  metafile_header_tmp.nrecords = buffer_meta_number;
  metafile_header_tmp.metafile_size = buffer_meta_pos + buffer_meta_number * sizeof (long long);
  unsigned crc32_l = compute_crc32 (buffer_shifts, buffer_meta_number * sizeof (long long));
  unsigned crc32_r = compute_crc32 (buffer_meta, buffer_meta_pos);
  metafile_header_tmp.crc32 = compute_crc32_combine (crc32_l, crc32_r, buffer_meta_pos);
  writeout (&metafile_header_tmp, sizeof (struct metafile_header));
  writeout (buffer_meta_key, buffer_meta_key_len);
  writeout (buffer_shifts, buffer_meta_number * sizeof (long long));
  writeout (buffer_meta, buffer_meta_pos);
  if (verbosity >= 3) {
    fprintf (stderr, "writing metafile %d\n", write_buffer_number);
    fprintf (stderr, "offset = %lld\n", global_offset);
    fprintf (stderr, "number of records = %d\n", buffer_meta_number);
  }
  global_offset+=sizeof (struct metafile_header) + buffer_meta_key_len + buffer_meta_number * sizeof (long long) + buffer_meta_pos;
  buffer_meta_init();
}







void index_iterator_next_meta () { 
  iterator_metafile_number++;
  iterator_metafile_position = 0;
  if (iterator_metafile_number < metafile_number) {
    metafile_load (iterator_metafile_number);
  } 
}


void index_iterator_next () {
  while (iterator_metafile_number != metafile_number && iterator_metafile_position == metafiles[iterator_metafile_number].header->nrecords) {
    index_iterator_next_meta ();
  }
  if (iterator_metafile_number != metafile_number && iterator_metafile_position < metafiles[iterator_metafile_number].header->nrecords) {
    index_iterator =  metafile_get_entry ( iterator_metafile_number, iterator_metafile_position++);
  } else {
    index_iterator = 0;
  }
}

void index_iterator_init () {
  iterator_metafile_number = -1;
  index_iterator_next_meta ();
  index_iterator_next ();
}



/****
   Index i/o functions
                   ****/

int hash_count;

int key_cmp (struct hash_entry *a, struct index_entry *b) {
  if (a == 0) return 1;
  if (b == 0) return -1;
  return mystrcmp(a->key, a->key_len, b->data, b->key_len);
}



long long get_index_header_size (index_header *header) {
  return sizeof (index_header);
}

long long tot_records;
int use_metafile_crc32;

void upgrade_header (struct metafile_header *x) {
  x->key_len = ((struct metafile_header_old *)x)->key_len;
  x->crc32 = 0;
}

extern int metafile_mode;
int load_index (kfs_file_handle_t Index) {
  index_type = PMEMCACHED_TYPE_INDEX_DISK;
  prev_use[MAX_METAFILES] = MAX_METAFILES;
  next_use[MAX_METAFILES] = MAX_METAFILES;
  if (Index == NULL) {
    index_size = 0;
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    snapshot_size = 0;
    return 0;
  }
  metafile_mode = 1;
  idx_fd = Index->fd;
  index_header header;
  read (idx_fd, &header, sizeof (index_header));
  if (header.magic !=  PMEMCACHED_INDEX_MAGIC && header.magic != PMEMCACHED_INDEX_MAGIC_OLD) {
    fprintf (stderr, "index file is not for pmemcached\n");
    return -1;
  }
  int old_metafiles = (header.magic == PMEMCACHED_INDEX_MAGIC_OLD);
  use_metafile_crc32 = !old_metafiles;
  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;
  
  metafile_number = header.nrecords;
  if (verbosity>=2){
    fprintf (stderr, "%d metafiles readed\n", header.nrecords);
  }
  if (metafile_number > MAX_METAFILES) {
    fprintf (stderr, "Fatal: too many metafiles\n");
    return -1;
  }

  int i;
  tot_records = 0;
  for (i = 0; i < metafile_number; i++) {
    if (old_metafiles) {
      read (idx_fd, &metafile_header_tmp, sizeof (struct metafile_header_old));      
      upgrade_header (&metafile_header_tmp);
    } else {
      read (idx_fd, &metafile_header_tmp, sizeof (struct metafile_header));
    }
    metafiles[i].header = zzmalloc (sizeof (struct metafile_header) + metafile_header_tmp.key_len);
    init_memory += sizeof (struct metafile_header) + metafile_header_tmp.key_len;
    memcpy (metafiles[i].header, &metafile_header_tmp, sizeof (struct metafile_header));
    metafiles[i].data = 0;
    metafiles[i].local_offsets = 0;
    metafiles[i].aio = NULL;
    read (idx_fd, metafiles[i].header->key, metafile_header_tmp.key_len);
    lseek (idx_fd, metafiles[i].header->metafile_size, SEEK_CUR);
    if (verbosity >= 3) {
      fprintf (stderr, "read metafile %d\n", i);
      fprintf (stderr, "number of records = %d\n", metafiles[i].header->nrecords);
    }
    tot_records += metafiles[i].header->nrecords;
  }

  if (verbosity) {
    fprintf (stderr, "Total %lld records in index\n", tot_records);
  }
  index_size = Index->info->file_size;
  snapshot_size = init_memory;

  pmemcached_register_replay_logevent();
  return 0;
}


extern struct  entry *current_cache;
int save_index (int writing_binlog) {
  hash_count = get_entry_cnt (); 
  hash_entry_t **p = zzmalloc (hash_count * sizeof (hash_entry_t *)); 
  char *newidxname = NULL;
  memory_for_metafiles = 0;
  if (engine_snapshot_replica) {
    newidxname = get_new_snapshot_name (engine_snapshot_replica, log_cur_pos(), engine_snapshot_replica->replica_prefix);
  }

  if (!newidxname || newidxname[0] == '-') {
    fprintf (stderr, "cannot write index: cannot compute its name\n");
    exit (1);
  }

  if (log_cur_pos() == jump_log_pos) {
    fprintf (stderr, "skipping generation of new snapshot %s for position %lld: snapshot for this position already exists\n",
       newidxname, jump_log_pos);
    return 0;
  } 

  if (verbosity > 0) {
    fprintf (stderr, "creating index %s at log position %lld\n", newidxname, log_cur_pos());
  }

  newidx_fd = open (newidxname, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0660);

  if (newidx_fd < 0) {
    fprintf (stderr, "cannot create new index file %s: %m\n", newidxname);
    exit (1);
  }

  index_header header;
  memset (&header, 0, sizeof (header));

  header.magic = PMEMCACHED_INDEX_MAGIC;
  header.created_at = time (NULL);
  header.log_pos1 = log_cur_pos ();
  header.log_timestamp = log_read_until;
  if (writing_binlog) {
    relax_write_log_crc32 ();
  } else {
    relax_log_crc32 (0);
  }
  header.log_pos1_crc32 = ~log_crc32_complement;

  //hash_count = 0;

  if (verbosity >= 3) {
    fprintf (stderr, "preparing index header\n");
  }
  
  int x = (dump_pointers (p, 0, hash_count));
  if (x != hash_count) {
    vkprintf (0, "dump_pointers = %d, hash_count = %d\n", x, hash_count);
    assert (0);
  }


  long long fCurr = get_index_header_size (&header);
  assert (lseek (newidx_fd, fCurr, SEEK_SET) == fCurr);

  index_iterator_init();
  buffer_meta_init();
  global_offset = sizeof (index_header);
  int p2 = 0;

  while (index_iterator || p2 < hash_count) {
    int t = key_cmp (p2 < hash_count ? p[p2] : 0, index_iterator);
    if (t == 0) {
      //fprintf (stderr, "p2 = %d, p[p2]->key_len = %d, p[p2]->data_len = %d\n", p2, p[p2]->key_len, p[p2]->data_len);
      do_pmemcached_merge (p[p2]->key, p[p2]->key_len);
      if (current_cache->data.data_len >= 0 && current_cache->hash_entry && current_cache->hash_entry->data_len >= 0) {
        assert (current_cache->hash_entry);
        buffer_meta_hash (current_cache->hash_entry);
      }
      p2 ++;
      index_iterator_next();
//      fprintf (stderr, "p2 = %d, p[p2]->key_len = %d\n", p2, p[p2]->key_len);
    }
    if (t < 0) {
      if (p[p2]->data_len >= 0) {
        buffer_meta_hash (p[p2]);
      }
      p2++;
    }
    if (t > 0) {
      if (t > 0) {
        buffer_meta_index (index_iterator);
      }
      index_iterator_next();
    }
    if (buffer_meta_pos >= metafile_size) {
      buffer_meta_flush();
    }
  }
  if (buffer_meta_pos > 0) {
    buffer_meta_flush();
  }
  flushout ();
  header.nrecords = write_buffer_number;

  assert (lseek (newidx_fd, 0, SEEK_SET) == 0);
  writeout (&header, get_index_header_size (&header));


  if (verbosity >= 3) {
    fprintf (stderr, "writing binary data done\n");
  }


  flushout ();

  assert (fsync (newidx_fd) >= 0);
  assert (close (newidx_fd) >= 0);

  if (verbosity >= 3) {
    fprintf (stderr, "writing index done\n");
  }

  if (rename_temporary_snapshot (newidxname)) {
    fprintf (stderr, "cannot rename new index file from %s: %m\n", newidxname);
    unlink (newidxname);
    exit (1);
  }

  print_snapshot_name (newidxname);

  return 0;
}



/*
 *
 *         AIO
 *
 */
//struct aio_connection *WaitAio;
int onload_metafile (struct connection *c, int read_bytes);

conn_type_t ct_metafile_aio = {
  .magic = CONN_FUNC_MAGIC,
  .wakeup_aio = onload_metafile
};

conn_query_type_t aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "pmemcached-index-disk-aio-metafile-query",
  .wakeup = aio_query_timeout,
  .close = delete_aio_query,
  .complete = delete_aio_query
};


void load_metafile (int metafile) {
  //WaitAio = NULL;
  WaitAioArrClear ();

  assert (metafile < metafile_number);

  struct metafile *meta = &metafiles[metafile];

  if (verbosity >= 3) {
    fprintf (stderr, "loading metafile %d in aio mode\n", metafile);
  }


  if (meta->aio != NULL) {
    check_aio_completion (meta->aio);
    if (meta->aio != NULL) {
      //WaitAio = meta->aio;
      WaitAioArrAdd (meta->aio);
      return;
    }

    if (meta->data) {
      return;
    } else {
      fprintf (stderr, "Previous AIO query failed at %p, scheduling a new one\n", meta);
    }
  } else {
    if (verbosity >= 4) {
      fprintf (stderr, "No previous aio found for this metafile\n");
    }
  }

  if (meta->data) {
    fprintf (stderr, "Error: memory leak at load_metafile.\n");
    return;
  }                             

  free_metafiles ();

  //add_use (metafile);

  while (1) {
    meta->local_offsets = (long long *)zzmalloc (meta->header->metafile_size);
    if (meta->local_offsets != NULL) {
      meta->data = (char *)(meta->local_offsets + meta->header->nrecords);
      break;
    }
    fprintf (stderr, "no space to load metafile - cannot allocate %d bytes\n", meta->header->metafile_size);
  }

  allocated_metafile_bytes += meta->header->metafile_size;

  if (verbosity >= 4) {
    fprintf (stderr, "AIO query creating...\n");
  }
  meta->aio = create_aio_read_connection (idx_fd, meta->local_offsets, meta->header->global_offset + meta->header->local_offset, meta->header->metafile_size, &ct_metafile_aio, meta);
  if (verbosity >= 4) {
    fprintf (stderr, "AIO query created\n");
  }
  assert (meta->aio != NULL);
  //WaitAio = meta->aio;
  WaitAioArrAdd (meta->aio);

  return;
}

int onload_metafile (struct connection *c, int read_bytes) {
  if (verbosity > 2) {
    fprintf (stderr, "onload_metafile(%p,%d)\n", c, read_bytes);
  }

  struct aio_connection *a = (struct aio_connection *)c;
  struct metafile *meta = (struct metafile *) a->extra;

  assert (a->basic_type == ct_aio);
  assert (meta != NULL);          

  if (meta->aio != a) {
    fprintf (stderr, "assertion (meta->aio == a) will fail\n");
    fprintf (stderr, "%p != %p\n", meta->aio, a);
  }

  assert (meta->aio == a);

  if (read_bytes != meta->header->metafile_size) {
    if (verbosity > 0) {
      fprintf (stderr, "ERROR reading metafile: read %d bytes out of %d: %m\n", read_bytes, meta->header->metafile_size);
    }
  }
  if (verbosity > 2) {
    fprintf (stderr, "*** Read metafile: read %d bytes\n", read_bytes);
  }

  if (read_bytes != meta->header->metafile_size) {
    meta->aio = NULL;
    meta->data = 0;
    zzfree (meta->local_offsets, meta->header->metafile_size);  
    meta->local_offsets = 0;
    allocated_metafile_bytes -= meta->header->metafile_size;
    metafiles_load_errors ++;
  } else {
    meta->aio = NULL;
    metafiles_loaded ++;
    add_use (meta - metafiles);
    metafiles_load_success ++;
    if (use_metafile_crc32) {
      crc32_check_and_repair (meta->local_offsets, meta->header->metafile_size, &meta->header->crc32, 1);
    }
    tot_aio_loaded_bytes += read_bytes;
  }
  return 1;
}

extern long long expired_aio_queries;
      
void custom_prepare_stats (stats_buffer_t *sb) {
  sb_printf (sb,
    "total_index_entries\t%lld\n"
    "total_metafiles\t%d\n"
    "metafiles_loaded\t%d\n"
    "metafiles_allocated_bytes\t%lld\n"
    "metafiles_unloaded_LRU\t%lld\n"
    "metafiles_load_errors\t%lld\n"
    "metafiles_load_success\t%lld\n"
    "metafiles_load_timeout\t%lld\n"
    "metafiles_total_loaded_bytes\t%lld\n"
    "metafiles_LRU_fails\t%d\n"
    "metafiles_cache_miss\t%lld\n"
    "metafiles_cache_ok\t%lld\n",
    tot_records,
    metafile_number,
    metafiles_loaded,
    allocated_metafile_bytes,
    metafiles_unload_LRU,
    metafiles_load_errors,
    metafiles_load_success,
    expired_aio_queries,
    tot_aio_loaded_bytes,
    use_query_fails,
    metafiles_cache_miss,
    metafiles_cache_ok);
}
