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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Arseny Smirnov
              2010-2013 Aliaksei Levin
*/

#ifndef _ISEARCH_DATA_H_
#define _ISEARCH_DATA_H_

#include "server-functions.h"
#include "net-events.h"
#include "kfs.h"
#include "kdb-isearch-binlog.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"

#include "isearch-tl.h"
#include "isearch-interface-structures.h"

#include "stemmer-new.h"
#include "utils.h"

#define	ISEARCH_INDEX_MAGIC 0x15fab3ec

#ifndef NOSLOW
#  define SLOW
#endif

#ifndef NOTYPES
#  define TYPES
#endif

#ifndef NOFADING
#  define FADING
#endif

#ifdef NOFADING
  typedef int rating;
#  define FD "%d"
#  define RATING_MIN 100
#else
  typedef float rating;
#  define FD "%f"
#  define RATING_NORM (7*24*60*60)
#  define RATING_MIN 19.99999f
#endif

#ifndef NOISE_PERCENT
#  ifdef SLOW
#    define NOISE_PERCENT 950
#  else
#    define NOISE_PERCENT 985
#  endif
#endif

#if NOISE_PERCENT < 0 || NOISE_PERCENT >= 1000
#  error Wrong NOISE_PERCENT specified
#endif

#define MAX_BEST 20000


extern int index_mode;
extern long max_memory;
extern int header_size;
extern rating lowest_rate;
extern int binlog_readed;
extern int find_bad_requests;

extern int jump_log_ts;
extern long long jump_log_pos;
extern unsigned int jump_log_crc32;



int init_isearch_data (int schema);

int init_all (kfs_file_handle_t Index);
void free_all (void);

#pragma pack(push,4)

typedef struct {
/* strange numbers */
  int magic;
  int created_at;
  long long log_pos0;
  long long log_pos1;
  unsigned long long file_hash;
  unsigned long long slice_hash;
  int log_timestamp;
  int log_split_min, log_split_max, log_split_mod;
  unsigned int log_pos0_crc32;
  unsigned int log_pos1_crc32;
  int black_list_size;

  int prefn;
  int idn;
  int namesn;
  int wordn;
  int h_id_size, h_pref_size, h_word_size;
  rating lowest_rate;
  int use_stemmer;
  int bestn;
  int maxq;
  int maxt;
  int nofading;

  int reserved[31];
} index_header;

typedef struct {
  int prev_bucket, next_bucket,
      prev_time, next_time,
      prev_used, next_used, val;
} q_info;

#pragma pack(pop)

int do_isearch_set_stat (int uid, int stype, int cnt, char *text, int text_len);
void get_hints (char *buf, int mode, int buf_len);
void get_suggestion (char *buf, int buf_len);
void get_top (char *buf, int cnt, int buf_len);
void get_best (char *buf, int cnt, int buf_len);

int do_black_list_add (const char *s, int len);
int do_black_list_delete (const char *s, int len);
char *black_list_get (void);
void black_list_force (void);


int tl_do_isearch_get_hints (struct tl_act_extra *extra);
#ifdef TYPES
int tl_do_isearch_get_types (struct tl_act_extra *extra);
#endif
int tl_do_isearch_get_top (struct tl_act_extra *extra);
int tl_do_isearch_get_best (struct tl_act_extra *extra);


int save_index (void);

#endif
