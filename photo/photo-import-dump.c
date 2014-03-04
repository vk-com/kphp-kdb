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
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>

#include "kdb-data-common.h"
#include "kdb-photo-binlog.h"

#include "dl.h"
#include "dl-utils.h"
#include "photo-data.h"

#define VERSION "0.01"
#define VERSION_STR "photo-import-dump "VERSION

volatile int sigpoll_cnt;

int split_rem, split_mod;

char *binlogname, *albums_dump, *photos_dump;

#define PHOTO_FD 3
#define ALBUM_FD 4

#define BUFF_LEN 1000000

char buff[BUFF_LEN + 1];

#define ADD_CHANGE(val, tp)                               \
  field_changes[field_changes_n].type = DL_CAT (t_, tp);  \
  field_changes[field_changes_n].v_fid = field_changes_n; \
  field_changes[field_changes_n].DL_CAT (v_, tp) = val;   \
  field_changes_n++;

#define ADD_CHANGE_STR(val)                                                            \
  field_changes[field_changes_n].DL_ADD_SUFF (v_string, len) = DL_ADD_SUFF (val, len); \
  ADD_CHANGE(val, string);

int do_change_data (int uid, int did, int type);

#define READ(name) {                   \
  if (*s == '\\') {                    \
    name = 0;                          \
    ++s;                               \
    assert (*s == 'N');                \
    ++s;                               \
    assert (*s == '\t' ||              \
            *s == '\n');               \
    ++s;                               \
  } else {                             \
    mul = 1;                           \
    name = 0;                          \
    if (*s == '-') {                   \
      mul = -1;                        \
      s++;                             \
    }                                  \
    while ( s < t &&                   \
           *s != '\t') {               \
      if (!('0' <= *s && *s <= '9')) {fprintf (stderr, "%s: %c\n", #name, *s); assert (0);}\
      assert ('0' <= *s && *s <= '9'); \
      name = name * 10 + *s++ - '0';   \
    }                                  \
    name *= mul;                       \
    s++;                               \
  }                                    \
}

#define READ_DBL(name) {                 \
  if (*s == '\\') {                      \
    name = 0.0;                          \
    ++s;                                 \
    assert (*s == 'N');                  \
    ++s;                                 \
    assert (*s == '\t' ||                \
            *s == '\n');                 \
    ++s;                                 \
  } else {                               \
    mul = 1;                             \
    name = 0;                            \
    if (*s == '-') {                     \
      mul = -1;                          \
      s++;                               \
    }                                    \
    double cur = 1.0;                    \
    while ( s < t &&                     \
           *s != '\t') {                 \
      if (!(('0' <= *s && *s <= '9') || *s == '.')) {fprintf (stderr, "%s: %c\n", #name, *s); assert (0);}\
      if (*s == '.') {                   \
        cur *= 0.1;                      \
      } else {                           \
        assert ('0' <= *s && *s <= '9'); \
        if (cur > 0.5) {                 \
          name = name * 10 + *s++ - '0'; \
        } else {                         \
          name += cur * (*s++ - '0');    \
          cur *= 0.1;                    \
        }                                \
      }                                  \
    }                                    \
    name *= mul;                         \
    s++;                                 \
  }                                      \
}

#define READ_STR(name) {                 \
  name = s;                              \
  name ## _len = 0;                      \
  if (s[0] == '\\' &&                    \
      s[1] == 'N') {                     \
    name[0] = 0;                         \
    assert (s[2] == '\t' ||              \
            s[2] == '\n');               \
    s += 3;                              \
  } else {                               \
    while ( s < t &&                     \
           *s != '\t') {                 \
      if (s[0] == '\\') {                \
        if (s[1] == '\t' ||              \
            s[1] == '\n' ||              \
            s[1] == '\\' ||              \
            s[1] == '0') {               \
          s++;                           \
          if (*s == '0') {               \
            *s = 0;                      \
          }                              \
        } else {                         \
          fprintf (stderr, "!!%c%c!!",   \
                           s[0], s[1]);  \
          assert (0);                    \
        }                                \
      }                                  \
      if (0 <= *s && *s <= 31 &&         \
          *s != '\t') {                  \
        name[name ## _len++] = ' ';      \
      } else {                           \
        name[name ## _len++] = *s;       \
      }                                  \
      s++;                               \
    }                                    \
    s++;                                 \
  }                                      \
}

void import_photo_dump (void) {
  assert (import_dump_mode);

  long long static_memory = dl_get_memory_used();

  dl_open_file (PHOTO_FD, photos_dump, 0);
  dl_open_file (ALBUM_FD, albums_dump, 0);

  set_ll albums;
  set_ll_init (&albums);

  int pos = 0, r, end_pos = 0, i, mul;
  map_int_vector_pii album_order;
  vector_int owners;

  map_int_vector_pii_init (&album_order);
  vector_int_init (&owners);

  int good_albums = 0;
  int last_album_id = 0;

  do {
    for (i = pos; i < end_pos; i++) {
      buff[i - pos] = buff[i];
    }
    end_pos -= pos;
    pos = 0;

    r = read (fd[ALBUM_FD], buff + end_pos, BUFF_LEN - end_pos);
    fsize[ALBUM_FD] -= r;
    end_pos += r;

    int id, owner_id, ordering;

    int title_len, description_len;
    char *title, *description;

    do {
      char *s = buff + pos, *t = buff + end_pos;
      while (s < t && *s != '\n') {
        s += *s == '\\';
        s++;
      }

      if (s >= t) {
        assert (end_pos == 0 || pos != 0);
        break;
      }

      t = s;
      s = buff + pos;

      READ(id);
      READ(owner_id);
      READ(ordering);
      READ_STR(title);
      READ_STR(description);

      assert (s == t + 1);
      pos = s - buff;

      if (dl_abs (owner_id) % split_mod != split_rem) {
        continue;
      }
      if (owner_id == 0) {
        fprintf (stderr, "Album %d has no owner.\n", id);
        continue;
      }

      good_albums++;
      last_album_id = id;

      field_changes_n = 0;

      ADD_CHANGE(id, int);
      ADD_CHANGE(owner_id, int);
      ADD_CHANGE_STR(title);
      ADD_CHANGE_STR(description);

      assert (do_create_album_force (owner_id, id));
      assert (do_change_data (owner_id, id, LEV_PHOTO_CHANGE_ALBUM));

      pair_int_int p = {
        .x = ordering,
        .y = -id,
      };

      int old_used = map_int_vector_pii_used (&album_order);
      vector_pii *v = map_int_vector_pii_add (&album_order, owner_id);
      if (old_used != map_int_vector_pii_used (&album_order)) {
        vector_pii_init (v);
        vector_int_pb (&owners, owner_id);
      }
      vector_pii_pb (v, p);

      set_ll_add (&albums, ((long long)owner_id << 32) | (id & 0xFFFFFFFFll));

      if (good_albums % 1000 == 0) {
        flush_binlog_forced (0);
      }
    } while (1);
  } while (r > 0);

  assert (end_pos == 0 && pos == end_pos && fsize[ALBUM_FD] == 0);

  int album_swap = 0;
  for (i = 0; i < vector_int_size (&owners); i++) {
    vector_pii *v = map_int_vector_pii_get (&album_order, owners.v[i]);
    assert (v);

    int j, k, n = vector_pii_size (v);

    if (n >= MAX_ALBUMS - 256) {
      fprintf (stderr, "Owner_id %d has %d albums, some will be deleted.\n", owners.v[i], n);
    }

    for (j = 1; j < n; j++) {
      if (dl_cmp_pair_int_int (v->v[j], v->v[j - 1]) < 0) {
        for (k = j - 1; k >= 0 && dl_cmp_pair_int_int (v->v[k + 1], v->v[k]) < 0; k--) {
          pair_int_int p = v->v[k];
          v->v[k] = v->v[k + 1];
          v->v[k + 1] = p;
        }

        assert (do_change_album_order (owners.v[i], -(v->v[k + 1].y), k == -1 ? 0 : -(v->v[k].y), 0) == 1);

        album_swap++;
        if (album_swap % 100000 == 0) {
          flush_binlog_forced (0);
        }
      }

      assert (dl_cmp_pair_int_int (v->v[j], v->v[j - 1]) > 0);
    }

    vector_pii_free (v);
  }

  if (verbosity > 0) {
    fprintf (stderr, "Album reordering has finished. Numder of swaps = %d.\n", album_swap);
  }

  map_int_vector_pii_free (&album_order);
  vector_int_free (&owners);

  int good_photos = 0, bad_photos = 0;

  map_ll_vector_pii photo_order;
  vector_ll owner_albums;

  map_ll_vector_pii_init (&photo_order);
  vector_ll_init (&owner_albums);

  do {
    for (i = pos; i < end_pos; i++) {
      buff[i - pos] = buff[i];
    }
    end_pos -= pos;
    pos = 0;

    r = read (fd[PHOTO_FD], buff + end_pos, BUFF_LEN - end_pos);
    fsize[PHOTO_FD] -= r;
    end_pos += r;

    int id, user_id, source_user_id, album_id, orig_album, owner_id, server, server2, ordering, height = 0, width = 0;

    int photo_len;
    char *photo;

    do {
      char *s = buff + pos, *t = buff + end_pos;
      while (s < t && *s != '\n') {
        s += *s == '\\';
        s++;
      }

      if (s >= t) {
        assert (end_pos == 0 || pos != 0);
        break;
      }

      t = s;
      s = buff + pos;

      READ(id);
      READ(user_id);
      READ(source_user_id);
      READ(album_id);
      READ(orig_album);
      READ(owner_id);
      READ(server);
      READ(server2);
      READ(ordering);
      READ_STR(photo);

      assert (s == t + 1);
      pos = s - buff;

      if (dl_abs (owner_id) % split_mod != split_rem) {
        continue;
      }

      if (owner_id == 0) {
        fprintf (stderr, "Photo %d has no owner.\n", id);
        continue;
      }

      if (!check_photo (photo, photo_len)) {
        continue;
      }

      long long owner_album = ((long long)owner_id << 32) | (album_id & 0xFFFFFFFFll);
      if (set_ll_get (&albums, owner_album) == NULL && album_id >= 0) {
        if (album_id <= last_album_id) {
          bad_photos++;
          continue;
        }
        set_ll_add (&albums, owner_album);
        assert (do_create_album_force (owner_id, album_id));
        fprintf (stderr, "Creating album %d %d\n", owner_id, album_id);
      }
      assert (album_id);

      good_photos++;

      field_changes_n = 0;

      ADD_CHANGE(id, int);
      ADD_CHANGE(album_id, int);
      ADD_CHANGE(owner_id, int);
      ADD_CHANGE(user_id, int);
      ADD_CHANGE(height, int);
      ADD_CHANGE(width, int);

      assert (do_create_photo_force (owner_id, album_id, 1, id));
      assert (do_change_data (owner_id, id, LEV_PHOTO_CHANGE_PHOTO));
      assert (do_add_photo_location (owner_id, id, 0, server, server2, source_user_id, orig_album, photo, photo_len));

      pair_int_int p = {
        .x = -ordering,
        .y = id,
      };

      int old_used = map_ll_vector_pii_used (&photo_order);
      vector_pii *v = map_ll_vector_pii_add (&photo_order, owner_album);
      if (old_used != map_ll_vector_pii_used (&photo_order)) {
        vector_pii_init (v);
        vector_ll_pb (&owner_albums, owner_album);
      }
      vector_pii_pb (v, p);

      if (good_photos % 1000 == 0) {
//        fprintf (stderr, "Good_photos = %d\r", good_photos);
        flush_binlog_forced (0);
      }
    } while (1);
  } while (r > 0);

  assert (end_pos == 0 && pos == end_pos && fsize[PHOTO_FD] == 0);

  if (verbosity > 0) {
    fprintf (stderr, "Good photos = %d. Bad photos = %d.\n", good_photos, bad_photos);
  }

  int photo_swap = 0;
  for (i = 0; i < vector_ll_size (&owner_albums); i++) {
    vector_pii *v = map_ll_vector_pii_get (&photo_order, owner_albums.v[i]);
    assert (v);

    int j, k, n = vector_pii_size (v);

    if (n >= MAX_PHOTOS - 256) {
      fprintf (stderr, "Album %d of owner_id %d has %d photos, some will be deleted.\n", (int)owner_albums.v[i], (int)(owner_albums.v[i] >> 32), n);
    }

    for (j = 1; j < n; j++) {
      if (dl_cmp_pair_int_int (v->v[j], v->v[j - 1]) < 0) {
        for (k = j - 1; k >= 0 && dl_cmp_pair_int_int (v->v[k + 1], v->v[k]) < 0; k--) {
          pair_int_int p = v->v[k];
          v->v[k] = v->v[k + 1];
          v->v[k + 1] = p;
        }

        if (do_change_photo_order ((int)(owner_albums.v[i] >> 32), v->v[k + 1].y, k == -1 ? 0 : v->v[k].y, 0) != 1) {
          fprintf (stderr, "Can't change order of photos. Owner_id = %d, photo_id = %d, prev_photo_id = %d, album_id = %d, j = %d, k = %d, n = %d.\n", (int)(owner_albums.v[i] >> 32),
                           v->v[k + 1].y, k == -1 ? 0 : v->v[k].y, (int)owner_albums.v[i], j, k, n);
          assert (0);
        }

        photo_swap++;
        if (photo_swap % 100000 == 0) {
          flush_binlog_forced (0);
        }
      }

      assert (dl_cmp_pair_int_int (v->v[j], v->v[j - 1]) > 0);
    }

    vector_pii_free (v);
  }
  flush_binlog_forced (1);

  if (verbosity > 0) {
    fprintf (stderr, "Photo reordering has finished. Numder of swaps = %d.\n", photo_swap);
  }

  map_ll_vector_pii_free (&photo_order);
  vector_ll_free (&owner_albums);

  set_ll_free (&albums);

  if (dl_get_memory_used() != static_memory) {
    if (verbosity > 0) {
      fprintf (stderr, "%lld memory left.\n", dl_get_memory_used() - static_memory);
    }
  }

  dl_close_file (PHOTO_FD);
  dl_close_file (ALBUM_FD);
}


static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  flush_binlog_last();
  sync_binlog (2);
  free_all();
  exit (EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  flush_binlog_last();
  sync_binlog (2);
  free_all();
  exit (EXIT_SUCCESS);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

void cron (void) {
}

/***
  MAIN
 ***/
void usage (void) {
  printf ("usage: %s [-v] [-u<username>] [-b<backlog>] [-l<log-name>] <index-file>\n"
    "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n"
    "\tGenerates new binlog for %s-engine using dumps of databases %s_albums and %s\n"
    "\t-B<max-binlog-size>\tdefines maximum size of each binlog file\n"
    "\t-v\toutput statistical and debug information into stderr\n"
    "\t-p<%s-dump-name>\tname of text file with dump from %ss\n"
    "\t-a<album-dump-name>\tname of text file with dump from albums\n"
    "\t-m<rem>,<mod>\tslice parameters: consider only objects with id %% mod == rem\n",
    progname, mode_names[mode], mode_names[mode], mode_names[mode], mode_names[mode], mode_names[mode]);
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;

  char c;
  long long x;

  dl_set_debug_handlers();
  progname = argv[0];

  import_dump_mode = 1;

  for (i = 0; i < MODE_MAX; i++) {
    if (strstr (progname, mode_names[i]) != NULL) {
      mode = i;
    }
  }

  while ((i = getopt (argc, argv, "a:b:B:l:m:p:u:hv")) != -1) {
    switch (i) {
    case 'a':
      albums_dump = optarg;
      break;
    case 'b':
      backlog = atoi (optarg);
      if (backlog <= 0) {
        backlog = BACKLOG;
      }
      break;
    case 'B':
      c = 0;
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
      }
      if (x >= 1024 && x < (1LL << 60)) {
        max_binlog_size = x;
      }
      break;
    case 'l':
      logname = optarg;
      break;
    case 'm':
      assert (sscanf (optarg, "%d,%d", &split_rem, &split_mod) == 2);
      break;
    case 'p':
      photos_dump = optarg;
      break;
    case 'u':
      username = optarg;
      break;
    case 'h':
      usage();
      return 2;
    case 'v':
      verbosity++;
      break;
    }
  }

  if (!(split_rem >= 0 && split_rem < split_mod) || argc != optind + 1) {
    usage();
    return 2;
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  init_all (Snapshot);

  //Binlog reading
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity > 0) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }

  clear_log();
  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  if (verbosity > 0) {
    fprintf (stderr, "replay log events started\n");
  }

  i = replay_log (0, 1);

  if (verbosity > 0) {
    fprintf (stderr, "replay log events finished\n");
  }

  clear_read_log();

  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  if (verbosity > 0) {
    fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%ld\n",
             (long long) log_pos, (long)dl_get_memory_used());
  }

  clear_write_log();

  assert (append_to_binlog (Binlog) == log_readto_pos);

  start_time = time (NULL);

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGPOLL, sigpoll_handler);

  switch (mode) {
    case MODE_PHOTO:
      import_photo_dump();
      break;
    default:
      assert ("Unsupported mode" && 0);
  }

  free_all();
  return 0;
}
