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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Anton  Timofeev    (atimofeev@vkontakte.ru)
 *          Log events support and memcached exterface.
 * Created: 22.03.2012
 */

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include "server-functions.h"

#include "antispam-common.h"
#include "kdb-antispam-binlog.h"
#include "antispam-index-layout.h"
#include "antispam-engine.h"
#include "antispam-data.h"
#include "antispam-db.h"

#define MAX_DISPLAYED_LEN 30

static inline bool add_pattern (antispam_pattern_t p, int str_len, char* str, bool replace, char *version) {
  assert (str[str_len] == 0);
  if (antispam_add (p, str, replace)) {
    if (verbosity >= 3) {
      st_printf ("$2%s pattern_%s[%10d]$3 %10u,%10u,%5u,$1|$3%.*s%s$1| = %d$^\n",
        (replace ? "set" : "add"), version, p.id, p.ip, p.uahash, (unsigned)p.flags,
        (str_len <= MAX_DISPLAYED_LEN ? str_len : MAX_DISPLAYED_LEN - 3),
        str, (str_len <= MAX_DISPLAYED_LEN ? "" : "..."), str_len);
    }
    return TRUE;
  }
  if (verbosity >= 3) {
    st_printf ("$5%s pattern_%s[%10d] %10u,%10u,%5u,|%.*s%s| = %d (rejected: possibly wrong id)$^\n",
      (replace ? "set" : "add"), version, p.id, p.ip, p.uahash, (unsigned)p.flags,
      (str_len <= MAX_DISPLAYED_LEN ? str_len : MAX_DISPLAYED_LEN - 3),
      str, (str_len <= MAX_DISPLAYED_LEN ? "" : "..."), str_len);
  }
  return FALSE;
}
static bool add_pattern_v1 (lev_antispam_add_pattern_v1_t* E, bool replace) {
  flags_t flags = 0; /* SIMPLIFY_TYPE_NONE */
  E->p.flags &= 3;
  if (E->p.flags == SIMPLIFY_TYPE_PARTIAL) {
    flags |= FLAGS_SIMPLIFY_PARTIAL;
  } else if (E->p.flags == SIMPLIFY_TYPE_FULL) {
    flags |= FLAGS_SIMPLIFY_FULL;
  }
  antispam_pattern_t pattern = {E->p.id, E->p.ip, E->p.uahash, flags};
  return add_pattern (pattern, (E->type & 0xFFFF), E->str, replace, "v1");
}
static bool add_pattern_v2 (lev_antispam_add_pattern_v2_t* E, bool replace) {
  return add_pattern (E->pattern, (E->type & 0xFFFF), E->str, replace, "v2");
}

static bool del_pattern (lev_antispam_del_pattern_t* E) {
  if (antispam_del (E->id)) {
    if (verbosity >= 3) {
      st_printf ("$1del pattern[%10d]$^\n", E->id);
    }
    return TRUE;
  }
  if (verbosity >= 3) {
    st_printf ("$5del pattern[%10d] (rejected: possibly wrong id)$^\n", E->id);
  }
  return FALSE;
}

/**
 * Binlog
 */
int antispam_replay_logevent (struct lev_generic *E, int size);

int init_antispam_data (int schema) {
  replay_logevent = antispam_replay_logevent;
  return 0;
}

// str - is not zero ended string!
bool do_add_pattern (antispam_pattern_t p, int str_len, char *str, bool replace) {
  if (str_len < 0 || str_len >= MAX_PATTERN_LEN) {
    return FALSE;
  }

  int size = offsetof (struct lev_antispam_add_pattern_v2, str) + str_len + 1;
  lev_type_t lev_type = replace ? LEV_ANTISPAM_SET_PATTERN_V2 : LEV_ANTISPAM_ADD_PATTERN_V2;
  lev_antispam_add_pattern_v2_t* E = alloc_log_event ((lev_type | str_len), size, 0);

  E->pattern = p;
  memcpy (E->str, str, str_len);
  E->str[str_len] = 0;

  return add_pattern_v2 (E, replace);
}

// Binlog stores deletion even there is no such 'id' in DB
bool do_del_pattern (int id) {
  lev_antispam_del_pattern_t *E =
    alloc_log_event (LEV_ANTISPAM_DEL_PATTERN, sizeof (struct lev_antispam_del_pattern), id);
  return del_pattern (E);
}

static int antispam_le_start (struct lev_start *E) {
  if (E->schema_id != ANTISPAM_SCHEMA_V1) {
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

int antispam_replay_logevent (struct lev_generic *E, int size) {
  int s;

  switch (E->type) {
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    return antispam_le_start ((struct lev_start *) E) >= 0 ? s : -1;
  case LEV_NOOP:
  case LEV_TIMESTAMP:
  case LEV_CRC32:
    if (verbosity >= 3 && E->type == LEV_CRC32) {
      st_printf ("$1>>> LEV_CRC32 <<<$^\n");
    }
  case LEV_ROTATE_FROM:
  case LEV_ROTATE_TO:
  case LEV_TAG:
    return default_replay_logevent (E, size);

#define ADD_LOG_EVENT_HANDLER(ver, replace)                                             \
    if (size < (int)sizeof (struct lev_antispam_add_pattern_##ver)) {                   \
      return -2;                                                                        \
    }                                                                                   \
    s = (E->type & 0xFFFF) + 1 + offsetof (struct lev_antispam_add_pattern_##ver, str); \
    if (size < s) {                                                                     \
      return -2;                                                                        \
    }                                                                                   \
    add_pattern_##ver ((lev_antispam_add_pattern_##ver##_t*)E, replace);                \
    return s;

  case LEV_ANTISPAM_SET_PATTERN_V1 ... LEV_ANTISPAM_SET_PATTERN_V1 + MAX_PATTERN_LEN - 1:
    ADD_LOG_EVENT_HANDLER (v1, TRUE);
  case LEV_ANTISPAM_SET_PATTERN_V2 ... LEV_ANTISPAM_SET_PATTERN_V2 + MAX_PATTERN_LEN - 1:
    ADD_LOG_EVENT_HANDLER (v2, TRUE);
  // LEV_ANTISPAM_ADD_PATTERN doesn't replace existing one as SET version
  case LEV_ANTISPAM_ADD_PATTERN_V1 ... LEV_ANTISPAM_ADD_PATTERN_V1 + MAX_PATTERN_LEN - 1:
    ADD_LOG_EVENT_HANDLER (v1, FALSE);
  case LEV_ANTISPAM_ADD_PATTERN_V2 ... LEV_ANTISPAM_ADD_PATTERN_V2 + MAX_PATTERN_LEN - 1:
    ADD_LOG_EVENT_HANDLER (v2, FALSE);

  case LEV_ANTISPAM_DEL_PATTERN:
    STANDARD_LOG_EVENT_HANDLER (lev_antispam, del_pattern);
//  Macro substitution follows:
//    if (size < (int)sizeof (struct lev_antispam_del_pattern)) {
//      return -2;
//    }
//    del_pattern ((struct lev_antispam_del_pattern *)E);
//    return sizeof (struct lev_antispam_del_pattern);
  }

  fprintf (stderr, "unknown log event type %08x at position %lld\n", E->type, log_cur_pos());
  return -1;
}

/**
 * Index
 */

static bool load_header (kfs_file_handle_t Index, index_header* header) {
  int fd = -1;
  if (Index == NULL) {
    memset (header, 0, sizeof (index_header));
    header->created_at = time (NULL);
    header->log_split_max = 1;
    header->log_split_mod = 1;
    return TRUE;
  }

  fd = Index->fd;
  int offset = Index->offset;

  // reading header
  if (lseek (fd, offset, SEEK_SET) != offset) {
    fprintf (stderr, "error reading header from index file: incorrect Index->offset = %d: %m\n", offset);
    return FALSE;
  }

  int size = sizeof (index_header);
  int r = read (fd, header, size);
  if (r != size) {
    fprintf (stderr, "error reading header from index file: read %d bytes instead of %d at position %d: %m\n", r, size, offset);
    return FALSE;
  }

  if (header->magic != ANTISPAM_INDEX_MAGIC) {
    fprintf (stderr, "bad antispam index file header\n");
    fprintf (stderr, "magic = 0x%08x instead of 0x%08x // offset = %d\n", header->magic, ANTISPAM_INDEX_MAGIC, offset);
    return FALSE;
  }

  assert (header->log_split_max);
  log_split_min = header->log_split_min;
  log_split_max = header->log_split_max;
  log_split_mod = header->log_split_mod;

  if (verbosity > 1) {
    fprintf (stderr, "header loaded %d %d %d %d\n", fd, log_split_min, log_split_max, log_split_mod);
    fprintf (stderr, "ok\n");
  }
  return TRUE;
}

static bool load_index (kfs_file_handle_t Index) {
  if (verbosity >= 2) {
    st_printf ("$1load_index: $2started (%s)...$^\n", (Index == NULL ? "Index == 0" : "Index != 0"));
  }

  index_header header;
  if (!load_header (Index, &header)) {
    return FALSE;
  }

  jump_log_ts = header.log_timestamp;
  jump_log_pos = header.log_pos1;
  jump_log_crc32 = header.log_pos1_crc32;

  if (verbosity >= 2) {
    st_printf ("$1load_index: $2%sloaded!$^\n", (Index == NULL ? "empty index " : ""));
  }

  return TRUE;
}

void init_all (kfs_file_handle_t Index) {
  index_load_time = -get_utime (CLOCK_MONOTONIC);
  bool index_loaded = load_index (Index);
  index_load_time += get_utime (CLOCK_MONOTONIC);

  if (!index_loaded) {
    fprintf (stderr, "fatal: error while loading index file %s\n", engine_snapshot_name);
    exit (1);
  }

  if (verbosity > 0) {
    fprintf (stderr, "load index: done, jump_log_pos=%lld, time %.06lfs\n", jump_log_pos, index_load_time);
  }

  // initialize zmalloc and etc..
  init_dyn_data();

  antispam_init();
}

void finish_all (void) {
  antispam_finish();
}
