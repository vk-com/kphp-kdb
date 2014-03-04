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

#include "pmemcached-index-ram.h"
#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>

//long long index_hash[MAX_INDEX_ITEMS];
//long long index_offset[MAX_INDEX_ITEMS];
//char index_binary_data[MAX_INDEX_BINARY_DATA];
long long *index_offset;
char *index_binary_data;


extern int index_size;
extern long long snapshot_size;
extern int verbosity;
extern int hash_st[];

extern long long jump_log_pos;
extern int jump_log_ts;
extern unsigned jump_log_crc32;
extern int index_type;

struct index_entry empty_index_entry = {
  .data_len = -1
};


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





struct index_entry* index_get (const char *key, int key_len) {
  int l = -1;
  int r = index_size;
  while (r-l > 1) {
    int x = (r+l)>>1;
    struct index_entry *entry = (struct index_entry *)&index_binary_data[index_offset[x]];
    if (mystrcmp (entry->data, entry->key_len, key, key_len) < 0) {
      l = x;
    } else {
      r = x;
    }
  }
  if (verbosity>=4) {
    fprintf (stderr, "(l,r) = (%d,%d)\n", l, r);
    fprintf (stderr, "index_size = %d\n", index_size);
  }
  l++;
  struct index_entry *entry;
  if (l < index_size) {
    entry = (struct index_entry *)&index_binary_data[index_offset[l]];
    vkprintf (4, "entry->key_len = %d, key_len = %d\n", entry->key_len, key_len);
    if (verbosity >= 6) {
      int i;
      for (i = 0; i < entry->key_len; i++)
        fprintf (stderr, "%c", entry->data[i]);
      fprintf (stderr, "\n");
    }
  }
  if (l < index_size && !mystrcmp (entry->data, entry->key_len, key, key_len)) { 
    vkprintf (4, "Item found in index.\n");
    return entry;
  }
  return &empty_index_entry;
}

struct index_entry* index_get_num (int n, int use_aio) {
  if (n >= index_size) {
    return &empty_index_entry;
  }
  assert (n >= 0);
  return (struct index_entry *)&index_binary_data[index_offset[n]];
}

#define min(x,y) ((x) < (y) ? (x) : (y))
struct index_entry* index_get_next (const char *key, int key_len) {
  int l = -1;
  int r = index_size;
  int lc = 1;
  int rc = 1;
  while (r-l > 1) {
    int x = (r+l)>>1;
    struct index_entry *entry = (struct index_entry *)&index_binary_data[index_offset[x]];
    int c = (mystrcmp2 (entry->data, entry->key_len, key, key_len, min (lc, rc) - 1));
    if (c < 0) {
      l = x;
      lc = -c;
    } else if (c > 0) {
      r = x;
      rc = c;
    } else {
      l = x;
      break;
    }
  }
  if (verbosity>=4) {
    fprintf (stderr, "(l,r) = (%d,%d)\n", l, r);
    fprintf (stderr, "index_size = %d\n", index_size);
  }
  l++;
  if (l < index_size) {
    struct index_entry *entry = (struct index_entry *)&index_binary_data[index_offset[l]];
    return entry;
  }
  return &empty_index_entry;
}

inline struct index_entry* index_get_by_idx ( int idx ) {
  return (struct index_entry *)&index_binary_data[index_offset[idx]];
} 

/*
 *
 * Index i/o functions
 *
 */

int hash_count;
hash_entry_t **p;

int key_cmp (int a, int b) {
  if (a == hash_count) return 1;
  if (b == index_size) return -1;
  //if (hash_array[a] < index_hash[b]) return -1;
  //if (hash_array[a] > index_hash[b]) return 1;
  //printf ("p = %p, a = %d, b = %d\n", p, a, b);
  struct hash_entry *e1 = p[a];
  assert (e1);
  struct index_entry *e2 = index_get_by_idx (b);
  assert (e2);
  //fprintf (stderr, ".");
  return mystrcmp (e1->key, e1->key_len, e2->data, e2->key_len);
}



long long get_index_header_size (index_header *header) {
  return sizeof (index_header);
}

extern int disable_cache;
int load_index (kfs_file_handle_t Index) {
  index_type = PMEMCACHED_TYPE_INDEX_RAM;
  if (Index == NULL) {
    index_size = 0;
    jump_log_ts = 0;
    jump_log_pos = 0;
    jump_log_crc32 = 0;
    snapshot_size = 0;
    return 0;
  }
  int fd = Index->fd;
  index_header header;
  assert (read (fd, &header, sizeof (index_header)) == sizeof (index_header));
  if (header.magic !=  PMEMCACHED_INDEX_MAGIC) {
    fprintf (stderr, "index file is not for pmemcached\n");
    return -1;
  }
  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;
  
  int nrecords = header.nrecords;
  index_size = nrecords;
  if (verbosity>=2){
    fprintf(stderr, "%d records readed\n", nrecords);
  }
  //assert (read (fd, index_hash, sizeof (long long)*(nrecords+1)) == sizeof (long long)*(nrecords+1));
  index_offset = malloc (sizeof (long long) * (nrecords + 1));
  assert (index_offset);
  assert (read (fd, index_offset, sizeof (long long)*(nrecords+1)) == sizeof (long long)*(nrecords+1));
  /*if (nrecords + 1 > MAX_INDEX_ITEMS) {
    fprintf(stderr, "Number of entries limit exceeded in index.\n");
    return -1;
  }*/
  /*if (index_offset[nrecords] > MAX_INDEX_BINARY_DATA) {
    fprintf(stderr, "Memory limit exceeded in index.\n");
    return -1;
  }*/
  vkprintf (1, "index_offset[%d]=%lld\n", nrecords, index_offset[nrecords]);
  index_binary_data = malloc (index_offset[nrecords]);
  assert (index_binary_data);
  long long x = read (fd, index_binary_data, index_offset[nrecords]);
  if (x != index_offset[nrecords]) {
    vkprintf (0, "x = %lld\n", x);
    assert (x == index_offset[nrecords]);
  }
  //close (fd);
  if (verbosity>=4){
    int i;
    for(i = 0; i < index_size; i++) {
      struct index_entry* entry = index_get_by_idx (i);
      fprintf (stderr, "key_len=%d, data_len=%d\t\t", entry->key_len, entry->data_len);
      fprintf (stderr, "key/data=%s\n",entry->data);
    }
  }
  snapshot_size = index_offset[nrecords]+16*(nrecords+1)+sizeof(index_header);
  pmemcached_register_replay_logevent();
  return 0;
}


int save_index (int writing_binlog) {
  hash_count = get_entry_cnt (); 
//  hash_entry_t **p = zzmalloc (hash_count * sizeof (hash_entry_t *)); 
  char *newidxname = NULL;

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
  p = zzmalloc (hash_count * sizeof (hash_entry_t *)); 
  assert (p);
  int x = (dump_pointers (p, 0, hash_count));
  if (x != hash_count) {
    vkprintf (0, "dump_pointers = %d, hash_count = %d\n", x, hash_count);
    assert (0);
  }

  int p1 = 0;
  int p2 = 0;
  int total_elem = 0;
  while (p1 < hash_count || p2 < index_size) {
    //fprintf (stderr, "<");
    int c = key_cmp(p1, p2);
    //fprintf (stderr, ">");
    if (c == 0) {
      do_pmemcached_merge (p[p1]->key, p[p1]->key_len);
    }
    if (c <= 0) {
      assert (p1 < hash_count);
      assert (p[p1]);
      if (p[p1]->data_len < 0) {
        total_elem--;
      }      
      p1++; 
    } 
    if (c >= 0) {
      p2++; 
    } 
    total_elem++;
  }

  header.nrecords = total_elem;
  writeout (&header, get_index_header_size (&header) );;

  long long shift = 0;//16*total_elem;

  p1 = 0;
  p2 = 0;
  while (p1 < hash_count || p2 < index_size) {
    int c = key_cmp(p1, p2);
    if (c <= 0) {
      if (p[p1]->data_len >= 0){
        writeout_long (shift);
        shift += 13 + p[p1]->data_len + p[p1]->key_len;
      }
      p1++; 
    } 
    if (c >= 0) {
      if (c > 0) {
        writeout_long (shift);
        shift += 13 + index_get_by_idx (p2)->data_len + index_get_by_idx (p2)->key_len;
      }
      p2++;
    } 
  }
  writeout_long (shift);

  if (verbosity >= 3) {
    fprintf (stderr, "writing offsets done\n");
  }

  if (verbosity >= 3) {
    fprintf (stderr, "writing binary data\n");
  }
  p1 = 0;
  p2 = 0;
  while (p1 < hash_count || p2 < index_size) {
    int c = key_cmp(p1, p2);
    if (c <= 0) {
      if (p[p1]->data_len >= 0){
        struct hash_entry* entry = p[p1];
        vkprintf (4, "Writing to index: key_len = %d, data_len = %d, p1 = %d, p[p1] = %p\n", (int)entry->key_len, entry->data_len, p1, p[p1]);
        writeout_short (entry->key_len);
        writeout_short (entry->flags);
        writeout_int (entry->data_len);
        writeout_int (entry->exp_time);
        writeout (entry->key, entry->key_len);
        writeout (entry->data, entry->data_len);
        writeout_char (0);
      }
      p1++; 
    } 
    if (c >= 0) {
      if (c > 0) {
        struct index_entry *entry =  index_get_by_idx (p2);
        writeout (entry, sizeof(struct index_entry) + entry->key_len + entry->data_len);
        writeout_char (0);
      }
      p2++;
    } 
  }
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
                           

struct aio_connection *WaitAio;
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


int onload_metafile (struct connection *c, int read_bytes) {
  return 0;
};

void custom_prepare_stats (stats_buffer_t *sb) {
}

void free_metafiles () {
}
