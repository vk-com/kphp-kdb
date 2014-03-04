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
              2011-2013 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include "server-functions.h"
#include "kdb-lists-binlog.h"
#include "lists-data.h"

#define MAXINT  (0x7fffffff)

char *progname = "lists-binlog", *username = NULL;
//char *src_fname = NULL;
int verbosity = 0, skip_timestamps = 0;
int  cutoff_ago = 18000;
//int src_fd;
double binlog_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos = 0;
long long binlog_loaded_size;
FILE *out;

int object_id_ints = 0, list_id_ints = 0;
int cyclic_buffer_entry_size = -1, cyclic_buffer_entry_ints = -1;

#define mytime() get_utime (CLOCK_MONOTONIC)

#ifdef LISTS_Z
int object_list_ints = 0, ltree_node_size = -1, object_id_bytes = 0, list_id_bytes = 0, object_list_bytes = 0;
#define list_object_bytes	object_list_bytes
#define list_object_ints	object_list_ints
#define COPY_LIST_ID(dest,src)	memcpy ((dest), (src), list_id_bytes)
int payload_offset = -1, tree_ext_small_node_size = -1, tree_ext_large_node_size = -1;
int tree_ext_global_node_size = -1;
int list_struct_size = -1;
//int cyclic_buffer_entry_size = -1, cyclic_buffer_entry_ints = -1;
int file_list_index_entry_size = -1;
int max_object_id[MAX_OBJECT_ID_INTS];
int max_list_id[MAX_LIST_ID_INTS];
int max_list_object_pair[MAX_OBJECT_ID_INTS + MAX_LIST_ID_INTS];
#define PAYLOAD(T) ((struct tree_payload *)(((char *)(T)) + payload_offset))
#define OARR_ENTRY(A,i) ((A) + object_id_ints * (i))
#define check_debug_list(list_id) (0)
#define check_ignore_list(list_id) (0)
#define	FLI_ADJUST(p)	((struct file_list_index_entry *) ((char *) (p) + list_id_bytes))
#define	FLI_ENTRY(i)	((char *) FileLists + (i) * file_list_index_entry_size)
#define	FLI_ENTRY_ADJUSTED(i)	((struct file_list_index_entry *) (FileLists_adjusted + (i) * file_list_index_entry_size))
#define	FLI_LIST_ID(p)	((list_id_t) (p))
#define	NEW_FLI_ENTRY(i)	((char *) NewFileLists + (i) * file_list_index_entry_size)
#define	NEW_FLI_ENTRY_ADJUSTED(i)	((struct file_list_index_entry *) (NewFileLists_adjusted + (i) * file_list_index_entry_size))
#define	MF_ADJUST(p)	((metafile_t *) ((char *) (p) + list_id_bytes))
#define	MF_READJUST(p)	((metafile_t *) ((char *) (p) - list_id_bytes))
#define	MF_MAGIC(p)	(MF_READJUST(p)->magic)
#define	MF_LIST_ID(p)	(MF_READJUST(p)->list_id)
#define	REVLIST_PAIR(i)	((int *) ((char *) RData + (i) * object_list_bytes))
#define	REVLIST_OBJECT_ID(i)	((int *) ((char *) REVLIST_PAIR(i) + list_id_bytes))
#define	REVLIST_LIST_ID(i)	REVLIST_PAIR(i)
#define	lev_list_id_bytes	list_id_bytes
#define	lev_list_object_bytes	object_list_bytes
#define	lev_object_id_bytes	object_id_bytes
#define	LEV_OBJECT_ID(E)	((int *) ((char *) (E) + list_id_bytes + 4))
#define	LEV_ADJUST_LO(E)	((void *) ((char *) (E) + list_object_bytes))
#define	LEV_ADJUST_L(E)		((void *) ((char *) (E) + list_id_bytes))
#else
#define COPY_LIST_ID(dest,src)	(dest) = (src)
#define ltree_node_size (sizeof (ltree_t))
#define tree_ext_small_node_size (sizeof (tree_ext_small_t))
#define tree_ext_global_node_size (sizeof (tree_ext_global_t))
#define tree_ext_large_node_size (sizeof (tree_ext_large_t))
#define list_struct_size (sizeof (list_t))
#define PAYLOAD(T) (&((T)->payload))
#define OARR_ENTRY(A,i) ((A)[(i)])
#define cyclic_buffer_entry_size (sizeof (struct cyclic_buffer_entry))
#define cyclic_buffer_entry_ints (sizeof (struct cyclic_buffer_entry) / 4)
#define	file_list_index_entry_size (sizeof (struct file_list_index_entry))
#define check_debug_list(list_id) ((list_id) == debug_list_id)
#define check_ignore_list(list_id) ((list_id) == ignored_list2)
#define	FLI_ADJUST(p)	(p)
#define	FLI_ENTRY(i)	(FileLists + (i))
#define	FLI_ENTRY_ADJUSTED(i)	(FLI_ADJUST(FLI_ENTRY(i)))
#define	FLI_LIST_ID(p)	((p)->list_id)
#define	NEW_FLI_ENTRY(i)	(NewFileLists + (i))
#define	NEW_FLI_ENTRY_ADJUSTED(i)	(FLI_ADJUST(NEW_FLI_ENTRY(i)))
#define MF_ADJUST(p)	(p)
#define MF_READJUST(p)	(p)
#define	MF_MAGIC(p)	((p)->magic)
#define	MF_LIST_ID(p)	((p)->list_id)
#define	REVLIST_PAIR(i)	RData[i].pair
#define	REVLIST_OBJECT_ID(i)	RData[i].object_id
#define	REVLIST_LIST_ID(i)	RData[i].list_id
#define	lev_list_id_bytes	0
#define	lev_list_object_bytes	0
#define	lev_object_id_bytes	0
#define	LEV_OBJECT_ID(E)	((E)->object_id)
#define	LEV_ADJUST_LO(E)	(E)
#define	LEV_ADJUST_L(E)		(E)
#endif

#define LPAYLOAD(T) (PAYLOAD(LARGE_NODE(T)))
#define	FLI_ENTRY_LIST_ID(i)	(FLI_LIST_ID(FLI_ENTRY(i)))
#define	NEW_FLI_ENTRY_LIST_ID(i)	(FLI_LIST_ID(NEW_FLI_ENTRY(i)))

/*
[-v] [-u<username>] [-t<seconds-ago>] [-U<min-utime>..<max-utime>] <old-binlog-file> [<output-binlog-file>]\n"
     "If <output-binlog-file> is specified, resulting binlog is appended to it.\n"
     "\t-h\tthis help screen\n"
     "\t-v\tverbose mode on\n"
     "\t-t\tcutoff time relative to present moment\n"
     "\t-U\tcopies all binlog except delete entries with timestamps in given range\n"
     "\t-i\tdo not import timestamps\n"
     "\t-u<username>\tassume identity of given user\n",
*/

void usage (void) {
  fprintf (stderr, "usage:\t%s <binlog-file>\n"
                   "\tConverts lists binlog into text format.\n"
                   "\tflags:\n"
                   "\t-t<min_utime,max_utime>\tset dumping range, unix times should separated by single comma\n"
                   "\t-r<seconds-ago>\n"
                   "\t-a\tdump timestamp in human readable format\n"
   ,progname);
}

int lists_replay_logevent (struct lev_generic *E, int size);

/* ------ dumping funcions --------- */
void dump_int_list(FILE* f, int *s, int len) {
  int i;
  //fputc('{', f);  
  for(i=0;i<len;++i) {
    if (i > 0) fputc(':', f);
    //fprintf(f, "%d",(int) ((unsigned char) s[i]));
    fprintf(f, "%d", s[i]);
  } 
  //fputc('}', f);
}
void dump_unsigned_char_list(FILE* f, unsigned char *s, int len) {
  int i;
  //fputc('{', f);  
  for(i=0;i<len;++i) {
    if (i > 0) fputc(':', f);
    fprintf(f, "%d",(int) s[i]);
  } 
  //fputc('}', f);
}
void dump_char_list(FILE* f, char *s, int len) {
  int i;
  //fputc('{', f);  
  for(i=0;i<len;++i) {
    if (i > 0) fputc(':', f);
    fprintf(f, "%d",(int) s[i]);
  } 
  //fputc('}', f);
}
void dump_str(FILE* f, char *s, int len) {
  int i;
  for(i=0;i<len;++i) fputc(s[i], f);
}

#ifdef	LISTS_Z 
static inline char *out_int_vector (const int *A, int len) {
  static char buff[65536];
  static char *wptr = buff;
  int s = len * 12, i;
  if (wptr + s > buff + 65536) {
    wptr = buff;
  }
  char *res = wptr;
  assert (len > 0 && s < 65536);
  for (i = 0; i < len; i++) {
    wptr += sprintf (wptr, "%d:", A[i]);
  }
  wptr[-1] = 0;
  return res;
}  

char *out_list_id (list_id_t list_id) {
  return out_int_vector (list_id, list_id_ints);
}

char *out_object_id (object_id_t object_id) {
  return out_int_vector (object_id, object_id_ints);
}

#endif
void dump_object_id(FILE *f, object_id_t object_id) {
#ifdef	LISTS_Z 
  fprintf(f, "%s", out_object_id(object_id));  
#elif defined(LISTS64)
  fprintf(f, "%lld", object_id)  
#else
  fprintf(f, "%d", object_id);
#endif  
}

void dump_list_id(FILE *f, list_id_t list_id) {
#ifdef	LISTS_Z 
  fprintf(f, "%s", out_list_id(list_id));  
#elif defined(LISTS64)
  fprintf(f, "%lld", list_id)  
#else
  fprintf(f, "%d", list_id);
#endif  
}



/* ------ compute struct sizes -------- */
//int *CB=0;

static void compute_struct_sizes (void) {
  assert (list_id_ints > 0 && list_id_ints <= MAX_LIST_ID_INTS);
  assert (object_id_ints > 0 && object_id_ints <= MAX_OBJECT_ID_INTS);
  // compute binlog record sizes
  
  // compute memory structure sizes

  #ifdef LISTS_Z
  int i;
  object_list_ints = object_id_ints + list_id_ints;
  ltree_node_size = sizeof (ltree_t) + object_list_ints * 4;
  object_id_bytes = object_id_ints * 4;
  list_id_bytes = list_id_ints * 4;
  object_list_bytes = object_id_bytes + list_id_bytes;
  list_struct_size = sizeof (list_t) + list_id_bytes;
  payload_offset = tree_ext_small_node_size = __builtin_offsetof (tree_ext_small_t, x) + object_id_bytes;

  for (i = 0; i < object_id_ints; i++) {
    max_object_id[i] = MAXINT;
  }
  for (i = 0; i < list_id_ints; i++) {
    max_list_id[i] = MAXINT;
  }
  for (i = 0; i < object_id_ints + list_id_ints; i++) {
    max_list_object_pair[i] = MAXINT;
  }
  #ifdef _LP64 
  if (payload_offset & 4) {
    payload_offset += 4;
  }
  #endif
  tree_ext_global_node_size = __builtin_offsetof (tree_ext_global_t, z) + object_id_bytes;
  tree_ext_large_node_size = payload_offset + sizeof (struct tree_payload);


  file_list_index_entry_size = sizeof (struct file_list_index_entry) + list_id_bytes;
  #endif

  // compute offsets for data access
  /*
  cyclic_buffer_entry_size = sizeof (struct cyclic_buffer_entry) + list_id_bytes;
  cyclic_buffer_entry_ints = (cyclic_buffer_entry_size >> 2);
  
  assert (!CB);
  CB = malloc (CYCLIC_BUFFER_SIZE * cyclic_buffer_entry_size);
  assert (CB);
  */

}
static int lists_le_start (struct lev_start *E) {
  if (E->schema_id != LISTS_SCHEMA_CUR
#ifdef LISTS_Z
      && E->schema_id != LISTS_SCHEMA_V1
#endif
     ) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  assert (!list_id_ints && !object_id_ints);

  if (E->extra_bytes >= 6 && E->str[0] == 1) {
    struct lev_lists_start_ext *EX = (struct lev_lists_start_ext *) E;
    assert (EX->kludge_magic == 1 && EX->schema_id == LISTS_SCHEMA_V3);
    list_id_ints = EX->list_id_ints;
    object_id_ints = EX->object_id_ints;
    assert (EX->value_ints == sizeof (value_t) / 4);
    assert (!EX->extra_mask);
  } else {
#ifdef LISTS_Z
    if (E->schema_id != LISTS_SCHEMA_V1) {
      fprintf (stderr, "incorrect binlog for lists-x-engine");
      exit (1);
    } else {
      list_id_ints = object_id_ints = 1;
    }
#else
    list_id_ints = LIST_ID_INTS;
    object_id_ints = OBJECT_ID_INTS;
#endif
  }

#ifndef LISTS_Z
  assert (list_id_ints == 1 && object_id_ints == 1);
#endif

  compute_struct_sizes ();

  return 0;
}

unsigned long long lists_replay_logevent_calls = 0;

void debug_dump_type(FILE *f, struct lev_new_entry_ext *E) {
  switch(E->type) {
    case LEV_START:
      fprintf(f, "LEV_START");
      break;
    case LEV_NOOP:
      fprintf(f, "LEV_NOOP");
      break;
    case LEV_TIMESTAMP:
      fprintf(f, "LEV_TIMESTAMP");
      break;
    case LEV_CRC32:
      fprintf(f, "LEV_CRC32");
      break;
    case LEV_ROTATE_FROM:
      fprintf(f, "LEV_ROTATE_FROM");
      break;
    case LEV_ROTATE_TO:
      fprintf(f, "LEV_ROTATE_TO");
      break;
    case LEV_LI_SET_ENTRY ... LEV_LI_SET_ENTRY+0xff:
      fprintf(f, "LEV_LI_SET_ENTRY");
      break;
    case LEV_LI_ADD_ENTRY ... LEV_LI_ADD_ENTRY+0xff:
      fprintf(f, "LEV_LI_ADD_ENTRY");
      break;
    case LEV_LI_REPLACE_ENTRY ... LEV_LI_REPLACE_ENTRY+0xff:
      fprintf(f, "LEV_LI_REPLACE_ENTRY");
      break;
    case LEV_LI_SET_ENTRY_EXT ... LEV_LI_SET_ENTRY_EXT+0xff:
      fprintf(f, "LEV_LI_SET_ENTRY_EXT");
      break;
    case LEV_LI_ADD_ENTRY_EXT ... LEV_LI_ADD_ENTRY_EXT+0xff:
      fprintf(f, "LEV_LI_ADD_ENTRY_EXT");
      break;
    case LEV_LI_SET_ENTRY_TEXT ... LEV_LI_SET_ENTRY_TEXT+0xff:
      fprintf(f, "LEV_LI_SET_ENTRY_TEXT");
      break;
    case LEV_LI_SET_FLAGS ... LEV_LI_SET_FLAGS+0xff:
      fprintf(f, "LEV_LI_SET_FLAGS");
      break;
    case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
      fprintf(f, "LEV_LI_INCR_FLAGS");
      break;
    case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
      fprintf(f, "LEV_LI_DECR_FLAGS");
      break;
    case LEV_LI_SET_FLAGS_LONG:
      fprintf(f, "LEV_LI_SET_FLAGS_LONG");
      break;
    case LEV_LI_CHANGE_FLAGS_LONG:
      fprintf(f, "LEV_LI_CHANGE_FLAGS_LONG");
      break;
    case LEV_LI_SET_VALUE:
      fprintf(f, "LEV_LI_SET_VALUE");
      break;
    case LEV_LI_INCR_VALUE:
      fprintf(f, "LEV_LI_INCR_VALUE");
      break;
    case LEV_LI_INCR_VALUE_TINY ... LEV_LI_INCR_VALUE_TINY + 0xff:
      fprintf(f, "LEV_LI_INCR_VALUE_TINY");
      break;
    case LEV_LI_DEL_LIST:
      fprintf(f, "LEV_LI_DEL_LIST");
      break;
    case LEV_LI_DEL_OBJ:
      fprintf(f, "LEV_LI_DEL_OBJ");
      break;
    case LEV_LI_DEL_ENTRY:
      fprintf(f, "LEV_LI_DEL_ENTRY");
      break;
    case LEV_LI_SUBLIST_FLAGS:
      fprintf(f, "LEV_LI_SUBLIST_FLAGS");
      break;
    case LEV_LI_DEL_SUBLIST:
      fprintf(f, "LEV_LI_DEL_SUBLIST");
      break;
  }
}
void dump_new_entry(FILE *f, const char* szAction, int offset, struct lev_new_entry* E) {
  assert(0 <= offset && offset <= 0xff);
  fprintf(f, "%s+%d\t", szAction, offset);
  dump_list_id(out,E->list_id);
  fputc('\t', out);
  dump_object_id(out,E->object_id);
  fputc('\t', out);
  fprintf(out, "%d\n", E->value);
}
void dump_new_entry_ext(FILE *f, const char* szAction, int offset, struct lev_new_entry_ext* E) {
  assert(0 <= offset && offset <= 0xff);
  fprintf(f, "%s+%d\t", szAction, offset);
  dump_list_id(out,E->list_id);
  fputc('\t', out);
  dump_object_id(out,E->object_id);
  fputc('\t', out);
  fprintf(out, "%d\t", E->value);
  dump_int_list(out, E->extra, 4);
  fprintf(out, "\n");
}
void dump_set_flags (FILE *f, const char* szAction, int offset, struct lev_set_flags* E) {
  assert(0 <= offset && offset <= 0xff);
  fprintf(f, "%s+%d\t", szAction, E->type-LEV_LI_SET_FLAGS);
  dump_list_id(f,E->list_id);
  fputc('\t', f);
  dump_object_id(f,E->object_id);
  fputc('\n', f);
}    

void dump_set_value (FILE *f, const char* szAction, struct lev_set_value *E) {
  fprintf(f, "%s\t", szAction);
  dump_list_id(out,E->list_id);
  fputc('\t', out);
  dump_object_id(f,E->object_id);
  fprintf(f, "\t%d\n", E->value /* %d:int */);
}
int cur_timestamp = 0;
int min_timestamp = 0x7fffffff, max_timestamp = 0;
int min_utime = 0, max_utime = 0x7fffffff;
long long timeskipping_events = 0;
int human_readable_timestamp=0;

void update_timestamp (int timestamp) {
  cur_timestamp = timestamp;
  //avoid overlow (max_utime + 360 is illegal due int overlow)
  if (timestamp - 360 > max_utime) {
    if (verbosity >= 3) fprintf(stderr, "skiping binlog's tail\n");
    exit(0); /* skip binlog's tail (it doen't contains timestamp in given time range) */
  }

  if (min_timestamp > cur_timestamp) min_timestamp = cur_timestamp;
  if (max_timestamp < cur_timestamp) max_timestamp = cur_timestamp;
}

char szTempTimeBuf[256];
void kill_newline(char *s) {
  char *p = strchr(s, '\n');
  if (p != 0) *p = 0;
}
/** return code:
   -1: event dumping is unsupported
    0: logevent has been dumped
    1: logevent hasn't been dumped (ex. TIMESTAMP, CRC32)
*/
int dump_logevent (FILE *out, struct lev_generic *EE) {
  struct lev_new_entry_ext *E = (struct lev_new_entry_ext *)EE;
  if (E->type == LEV_TIMESTAMP) {
    update_timestamp(((struct lev_timestamp*) EE)->timestamp);
    return 1;
  }
  if (E->type == LEV_CRC32) {
    update_timestamp(((struct lev_crc32*) EE)->timestamp);
    return 1;
  }
  if (! (min_utime <= cur_timestamp && cur_timestamp <= max_utime)) {
    timeskipping_events++;
    return 1;
  }
  if (human_readable_timestamp) {
    time_t x = cur_timestamp;
    strncpy(szTempTimeBuf,asctime(localtime(&x)),255);
    szTempTimeBuf[255] = 0;
    kill_newline(szTempTimeBuf);
    fprintf(out, "%s\t", szTempTimeBuf);
  }
  else {
    fprintf(out, "%d\t", cur_timestamp);
  }
  switch (E->type) {
  case LEV_START:
    fprintf(out, "lev_start\t%d\t%d\t%d\t%d\t%d\t",
      ((struct lev_start*) EE)->schema_id /* %d:int */,
      ((struct lev_start*) EE)->extra_bytes /* %d:int */,
      ((struct lev_start*) EE)->split_mod /* %d:int */,
      ((struct lev_start*) EE)->split_min /* %d:int */,
      ((struct lev_start*) EE)->split_max /* %d:int */);
    dump_char_list(out, ((struct lev_start*) EE)->str, 4);
    fputc('\n', out);
    return 0;
  case LEV_NOOP:
    fprintf(out, "lev_noop\n");
    return 0;
  case LEV_TIMESTAMP:
    return 1;
  case LEV_CRC32:
    return 1;
  case LEV_ROTATE_FROM:
    fprintf(out, "lev_rotate_from\t%d\t%lld\t%llu\t%llu\n",
      ((struct lev_rotate_from*) EE)->timestamp /* %d:int */,
      ((struct lev_rotate_from*) EE)->cur_log_pos /* %lld:long long */,
      ((struct lev_rotate_from*) EE)->prev_log_hash /* %llu:hash_t */,
      ((struct lev_rotate_from*) EE)->cur_log_hash /* %llu:hash_t */);
    return 0;  
  case LEV_ROTATE_TO:
    fprintf(out, "lev_rotate_to\t%d\t%lld\t%llu\t%llu\n",
      ((struct lev_rotate_to*) EE)->timestamp /* %d:int */,
      ((struct lev_rotate_to*) EE)->next_log_pos /* %lld:long long */,
      ((struct lev_rotate_to*) EE)->cur_log_hash /* %llu:hash_t */,
      ((struct lev_rotate_to*) EE)->next_log_hash /* %llu:hash_t */);
    return 0;
  case LEV_LI_SET_ENTRY ... LEV_LI_SET_ENTRY+0xff:
    dump_new_entry(out, "lev_li_set_entry", E->type-LEV_LI_SET_ENTRY, ((struct lev_new_entry*) EE));
    return 0;
  case LEV_LI_ADD_ENTRY ... LEV_LI_ADD_ENTRY+0xff:
    dump_new_entry(out, "lev_li_add_entry", E->type-LEV_LI_ADD_ENTRY, ((struct lev_new_entry*) EE));
    return 0;
  case LEV_LI_REPLACE_ENTRY ... LEV_LI_REPLACE_ENTRY+0xff:
    dump_new_entry(out, "lev_li_replace_entry",E->type-LEV_LI_REPLACE_ENTRY, ((struct lev_new_entry*) EE));
    return 0;
  case LEV_LI_SET_ENTRY_EXT ... LEV_LI_SET_ENTRY_EXT+0xff:
    dump_new_entry_ext(out, "lev_li_set_entry_ext", E->type-LEV_LI_SET_ENTRY_EXT,((struct lev_new_entry_ext*) EE));
    return 0;
  case LEV_LI_ADD_ENTRY_EXT ... LEV_LI_ADD_ENTRY_EXT+0xff:
    dump_new_entry_ext(out, "lev_li_add_entry_ext", E->type-LEV_LI_ADD_ENTRY_EXT, ((struct lev_new_entry_ext*) EE));
    return 0;
  case LEV_LI_SET_ENTRY_TEXT ... LEV_LI_SET_ENTRY_TEXT+0xff:
    fprintf(out, "lev_set_entry_text\t");
    dump_list_id(out, ((struct lev_set_entry_text*) EE)->list_id);
    fputc('\t', out);
    dump_object_id(out, ((struct lev_set_entry_text*) EE)->object_id);
    fputc('\t', out);
    dump_str(out, ((struct lev_set_entry_text*) EE)->text, E->type & 0xff);
    fputc('\n', out);
    return 0;  
  case LEV_LI_SET_FLAGS ... LEV_LI_SET_FLAGS+0xff:
    dump_set_flags(out, "lev_li_set_flags", E->type - LEV_LI_SET_FLAGS, ((struct lev_set_flags*) EE)); 
    return 0;
  case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
    dump_set_flags(out, "lev_li_incr_flags", E->type - LEV_LI_INCR_FLAGS, ((struct lev_set_flags*) EE)); 
    return 0;
  case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
    dump_set_flags(out, "lev_li_decr_flags", E->type - LEV_LI_DECR_FLAGS, ((struct lev_set_flags*) EE)); 
    return 0;
  case LEV_LI_SET_FLAGS_LONG:
    fprintf(out, "lev_set_flags_long\t");
    dump_list_id(out,((struct lev_set_flags_long*) EE)->list_id);
    fputc('\t', out);
    dump_object_id(out,((struct lev_set_flags_long*) EE)->object_id);
    fprintf(out,"\t%d\n", ((struct lev_set_flags_long*) EE)->flags);
    return 0;
  case LEV_LI_CHANGE_FLAGS_LONG:
    fprintf(out, "lev_change_flags_long\t");
    dump_list_id(out,((struct lev_change_flags_long*) EE)->list_id);
    fputc('\t', out);
    dump_object_id(out,((struct lev_change_flags_long*) EE)->object_id);
    fprintf(out, "\t%d\t%d\n", 
      ((struct lev_change_flags_long*) EE)->and_mask /* %d:int */,
      ((struct lev_change_flags_long*) EE)->xor_mask /* %d:int */);
    return 0;
  case LEV_LI_SET_VALUE:
    dump_set_value(out, "lev_set_value", ((struct lev_set_value*) EE));
    return 0;
  case LEV_LI_INCR_VALUE:
    dump_set_value(out, "lev_incr_value", ((struct lev_set_value*) EE));
    return 0;
  case LEV_LI_INCR_VALUE_TINY ... LEV_LI_INCR_VALUE_TINY + 0xff:
    fprintf(out, "lev_li_incr_value_tiny+%d\t", E->type - LEV_LI_INCR_VALUE_TINY);
    dump_list_id(out,((struct lev_del_entry*) EE)->list_id);
    fputc('\t', out);
    dump_object_id(out,((struct lev_del_entry*) EE)->object_id);
    fputc('\n', out);
    return 0;
  case LEV_LI_DEL_LIST:
    fprintf(out, "lev_del_list\t");
    dump_list_id(out,((struct lev_del_list*) EE)->list_id);
    fputc('\n', out);
    return 0;
  case LEV_LI_DEL_OBJ:
    fprintf(out, "lev_li_del_obj\t");
    dump_object_id(out,((struct lev_del_obj*) EE)->object_id);
    fputc('\n', out);
    return 0;
  case LEV_LI_DEL_ENTRY:
    fprintf(out, "lev_del_entry\t");
    dump_list_id(out,((struct lev_del_entry*) EE)->list_id);
    fputc('\t', out);
    dump_object_id(out,((struct lev_del_entry*) EE)->object_id);
    fputc('\n', out);
    return 0;
  case LEV_LI_SUBLIST_FLAGS:
    fprintf(out, "lev_sublist_flags\t");
    dump_list_id(out,((struct lev_sublist_flags*) EE)->list_id);
    fprintf(out, "\t%d\t%d\t%d\t%d\n",
      (int)((struct lev_sublist_flags*) EE)->xor_cond /* %d:unsigned char */,
      (int)((struct lev_sublist_flags*) EE)->and_cond /* %d:unsigned char */,
      (int)((struct lev_sublist_flags*) EE)->and_set /* %d:unsigned char */,
      (int)((struct lev_sublist_flags*) EE)->xor_set /* %d:unsigned char */);
    return 0;
  case LEV_LI_DEL_SUBLIST:
    fprintf(out, "lev_del_sublist\t");
    dump_list_id(out, ((struct lev_del_sublist*) EE)->list_id);
    fprintf(out,"\t%d\t%d\t",
      (int)((struct lev_del_sublist*) EE)->xor_cond /* %d:unsigned char */,
      (int)((struct lev_del_sublist*) EE)->and_cond /* %d:unsigned char */);
    dump_unsigned_char_list(out, ((struct lev_del_sublist*) EE)->reserved, 2);
    fputc('\n', out);  
    return 0;
  default:
    return -1;
  }
}

int lists_replay_logevent (struct lev_generic *EE, int size) {
  struct lev_new_entry_ext *E = (struct lev_new_entry_ext *)EE;
  int s;  
  lists_replay_logevent_calls++;
  if (verbosity >= 4) {
    fprintf(stderr, "%llu call of lists_replay_logevent\n", lists_replay_logevent_calls);
    debug_dump_type(stderr, E);
    fprintf(stderr, "\n");
  }
  switch (E->type) {
  case LEV_START:
    if (size < 24 || EE->b < 0 || EE->b > 4096) { return -2; }
    s = 24 + ((EE->b + 3) & -4);
    if (size < s) { return -2; }
    dump_logevent(out, EE);
    return lists_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
    dump_logevent(out, EE);
    return default_replay_logevent (EE, size);
  case LEV_TIMESTAMP:
    dump_logevent(out, EE);
    return default_replay_logevent (EE, size);
  case LEV_CRC32:
    dump_logevent(out, EE);
    return default_replay_logevent (EE, size);
  case LEV_ROTATE_FROM:
    dump_logevent(out, EE);
    return default_replay_logevent (EE, size);
  case LEV_ROTATE_TO:
    dump_logevent(out, EE);
    return default_replay_logevent (EE, size);  
  case LEV_LI_SET_ENTRY ... LEV_LI_SET_ENTRY+0xff:
    if (size < sizeof (struct lev_new_entry) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_new_entry) + lev_list_object_bytes;
  case LEV_LI_ADD_ENTRY ... LEV_LI_ADD_ENTRY+0xff:
    if (size < sizeof (struct lev_new_entry) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_new_entry) + lev_list_object_bytes;
  case LEV_LI_REPLACE_ENTRY ... LEV_LI_REPLACE_ENTRY+0xff:
    if (size < sizeof (struct lev_new_entry) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_new_entry) + lev_list_object_bytes;
  case LEV_LI_SET_ENTRY_EXT ... LEV_LI_SET_ENTRY_EXT+0xff:
    if (size < sizeof (struct lev_new_entry_ext) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_new_entry_ext) + lev_list_object_bytes;
  case LEV_LI_ADD_ENTRY_EXT ... LEV_LI_ADD_ENTRY_EXT+0xff:
    if (size < sizeof (struct lev_new_entry_ext) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_new_entry_ext) + lev_list_object_bytes;
  case LEV_LI_SET_ENTRY_TEXT ... LEV_LI_SET_ENTRY_TEXT+0xff:
    s = E->type & 0xff;
    if (size < sizeof (struct lev_set_entry_text) + lev_list_object_bytes + s) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_set_entry_text) + lev_list_object_bytes + s;
  case LEV_LI_SET_FLAGS ... LEV_LI_SET_FLAGS+0xff:
    if (size < sizeof (struct lev_set_flags) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_set_flags) + lev_list_object_bytes;
  case LEV_LI_INCR_FLAGS ... LEV_LI_INCR_FLAGS+0xff:
    if (size < sizeof (struct lev_set_flags) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_set_flags) + lev_list_object_bytes;
  case LEV_LI_DECR_FLAGS ... LEV_LI_DECR_FLAGS+0xff:
    if (size < sizeof (struct lev_set_flags) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_set_flags) + lev_list_object_bytes;
  case LEV_LI_SET_FLAGS_LONG:
    if (size < sizeof (struct lev_set_flags_long) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_set_flags_long) + lev_list_object_bytes;
  case LEV_LI_CHANGE_FLAGS_LONG:
    if (size < sizeof (struct lev_change_flags_long) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_change_flags_long) + lev_list_object_bytes;
  case LEV_LI_SET_VALUE:
    if (size < sizeof (struct lev_set_value) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_set_value) + lev_list_object_bytes;
  case LEV_LI_INCR_VALUE:
    if (size < sizeof (struct lev_set_value) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_set_value) + lev_list_object_bytes;
  case LEV_LI_INCR_VALUE_TINY ... LEV_LI_INCR_VALUE_TINY + 0xff:
    if (size < sizeof (struct lev_del_entry) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_del_entry) + lev_list_object_bytes;
  case LEV_LI_DEL_LIST:
    if (size < sizeof (struct lev_del_list) + lev_list_id_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_del_list) + lev_list_id_bytes;
  case LEV_LI_DEL_OBJ:
    if (size < sizeof (struct lev_del_obj) + lev_object_id_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_del_obj) + lev_object_id_bytes;
  case LEV_LI_DEL_ENTRY:
    if (size < sizeof (struct lev_del_entry) + lev_list_object_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_del_entry) + lev_list_object_bytes;
  case LEV_LI_SUBLIST_FLAGS:
    if (size < sizeof (struct lev_sublist_flags) + lev_list_id_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_sublist_flags) + lev_list_id_bytes;
  case LEV_LI_DEL_SUBLIST:
    if (size < sizeof (struct lev_del_sublist) + lev_list_id_bytes) { return -2; }
    dump_logevent(out, EE);
    return sizeof (struct lev_del_sublist) + lev_list_id_bytes;
  default:
    debug_dump_type(stderr, E);
    fprintf(stderr," dumping is unimplemented!\n");
    return -1;
  }
}

/*
TODO: LEV_LI_SET_ENTRY_TEXT
*/

int main (int argc, char *argv[]) {
  int i,x,y;
//  long long jump_log_pos = 0;
  out = stdout;
  while ((i = getopt (argc, argv, "ahvr:t:u:J:")) != -1) {
    switch (i) {
    case 'a':
      human_readable_timestamp = 1;
      break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'u':
      username = optarg;
      break;
    case 'r':
      if (1 == sscanf(optarg, "%d", &x)) {
        min_utime = time(0) - x;
      }
      break;
    case 't':
      if (2 == sscanf(optarg, "%d,%d", &x, &y)) {
        min_utime = x;
        max_utime = y;
      }
      break;
    case 'J':
      assert (sscanf (optarg, "%lld:%u:%d", &jump_log_pos, &jump_log_crc32, &jump_log_ts) == 3);
      break;
    }
  }

  if (optind >= argc) {
    usage();
    return 2;
  }
  if (verbosity >= 3) {
  #ifdef	LISTS_Z 
    fprintf(stderr, "#defined LISTS_Z\n");
  #elif defined(LISTS64)
    fprintf(stderr, "#defined LISTS64\n");
  #else
    fprintf(stderr, "not defined LISTS64 and not define LISTS_Z\n");
  #endif  
  }
  if (verbosity) {
    fprintf(stderr, "dumping time range [%d, %d]\n", min_utime, max_utime);  
  }
  if (min_utime > max_utime) {
    fprintf(stderr, "min_utime > max_utime\n");
    return 1;
  }

  //src_fname = argv[optind];

  if (username && change_user(username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    return 1;
  }
  

  replay_logevent = lists_replay_logevent;
  
  if (engine_preload_filelist ( argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  if (verbosity>=3){
    fprintf (stderr, "engine_preload_filelist done\n");
  }

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = -mytime();
  clear_log();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);
  if (verbosity) {
    fprintf (stderr, "replay log events started\n");
  }

  i = replay_log (0, 1);
 
  if (verbosity) {
    fprintf (stderr, "replay log events finished\n");
  }

  binlog_load_time += mytime();
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (verbosity) {
    fprintf(stderr, "Processed %llu events.\n", lists_replay_logevent_calls);
    fprintf(stderr, "min timestamp = %d, max timestamp = %d\n", min_timestamp, max_timestamp);
  }

  /*
  src_fd = open (src_fname, O_RDONLY);
  if (src_fd < 0) {
    fprintf (stderr, "cannot open %s: %m\n", src_fname);
    return 1;
  }

  while (process_record() >= 0) { }

  flush_out();

  if (targ_fd != 1) {
    if (fdatasync(targ_fd) < 0) {
      fprintf (stderr, "error syncing %s: %m", targ_fname);
      exit (1);
    }
    close (targ_fd);
  }
  if (verbosity > 0) {
    output_stats();
  }
  */

 // return rend > rptr ? 1 : 0;
  return 0;

}
