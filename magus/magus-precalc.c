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
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "server-functions.h"
#include "dl-utils.h"

#include "magus-precalc.h"

int now = 0;

int engine_n = -1;

char *database_dump_prefix = NULL;
int fids_limit = 1000;
int hints_limit = -1;
char dump_name[100];
int dump_type = 41;
const int dump_fd = 3;
const size_t fbuf_size = 4194304;

map_string_int fid_name_id;

map_int_int fid_id;
vector (int, fids);

vector (score, scores);
vector (user_dump, users);

int set_cur_dump (int dump_id) {
  assert (0 <= dump_id && dump_id < engine_n);

  if (fd[dump_fd] != -1 && dump_id) {
    dl_close_file (dump_fd);
  }

  int cur_dump = dump_id;

  if (verbosity > 0) {
    fprintf (stderr, "\nSwitching to dump %d\n", cur_dump);
  }

  snprintf (dump_name, 100, "dump%03d.%d", cur_dump, dump_type);
  return dl_open_file (dump_fd, dump_name, -1) >= 0;
}

int set_cur_database_dump (int dump_id) {
  assert (0 <= dump_id && dump_id < engine_n && database_dump_prefix != NULL);

  if (fd[dump_fd] != -1) {
    dl_close_file (dump_fd);
  }

  int cur_dump = dump_id;

  if (verbosity > 0) {
    fprintf (stderr, "\nSwitching to database dump %d, memory_used = %lld\n", cur_dump, dl_get_memory_used());
  }

  snprintf (dump_name, 100, "%s%03d.dump", database_dump_prefix, cur_dump);
  return dl_open_file (dump_fd, dump_name, -1) >= 0;
}

#define BUFF_LEN 1000000

char buff[BUFF_LEN + 1];

#define READ(name) {                   \
  if (*s == '\\') {                    \
    name = -1;                         \
    ++s;                               \
    assert (*s == 'N');                \
    ++s;                               \
    assert (*s == '\t' ||              \
            *s == '\n');               \
    ++s;                               \
  } else {                             \
    int mul = 1;                       \
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
      if (0 <= *s && *s <= 31) {         \
        name[name ## _len++] = ' ';      \
      } else {                           \
        name[name ## _len++] = *s;       \
      }                                  \
      s++;                               \
    }                                    \
    s++;                                 \
  }                                      \
}

int static_memory = 0;
#include "../hints/utils.h"
#include "string-processing.h"

int name_buff[MAX_NAME_SIZE];
char prepare_res[MAX_NAME_SIZE];

static int win_to_utf8_convert[256] = {0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0xa, 0xb, 0xc, 0xd, 0xe, 0xf, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48, 0x49, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f, 0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f, 0x402, 0x403, 0x201a, 0x453, 0x201e, 0x2026, 0x2020, 0x2021, 0x20ac, 0x2030, 0x409, 0x2039, 0x40a, 0x40c, 0x40b, 0x40f, 0x452, 0x2018, 0x2019, 0x201c, 0x201d, 0x2022, 0x2013, 0x2014, 0x0, 0x2122, 0x459, 0x203a, 0x45a, 0x45c, 0x45b, 0x45f, 0xa0, 0x40e, 0x45e, 0x408, 0xa4, 0x490, 0xa6, 0xa7, 0x401, 0xa9, 0x404, 0xab, 0xac, 0xad, 0xae, 0x407, 0xb0, 0xb1, 0x406, 0x456, 0x491, 0xb5, 0xb6, 0xb7, 0x451, 0x2116, 0x454, 0xbb, 0x458, 0x405, 0x455, 0x457, 0x410, 0x411, 0x412, 0x413, 0x414, 0x415, 0x416, 0x417, 0x418, 0x419, 0x41a, 0x41b, 0x41c, 0x41d, 0x41e, 0x41f, 0x420, 0x421, 0x422, 0x423, 0x424, 0x425, 0x426, 0x427, 0x428, 0x429, 0x42a, 0x42b, 0x42c, 0x42d, 0x42e, 0x42f, 0x430, 0x431, 0x432, 0x433, 0x434, 0x435, 0x436, 0x437, 0x438, 0x439, 0x43a, 0x43b, 0x43c, 0x43d, 0x43e, 0x43f, 0x440, 0x441, 0x442, 0x443, 0x444, 0x445, 0x446, 0x447, 0x448, 0x449, 0x44a, 0x44b, 0x44c, 0x44d, 0x44e, 0x44f};

char *prepare_str_cp1251 (char *s, int len) {
  if (len >= MAX_NAME_SIZE / 4 - 1) {
    return NULL;
  }

  sp_init();
  s = sp_to_lower (s);

  int i;
  int state = 0;
  int save_pos = -1;
  int cur_num = 0;
  int name_buff_len = 0;

  for (i = 0; i < len; i++) {
    if (state == 0 && s[i] == '&') {
      save_pos = name_buff_len;
      cur_num = 0;
      state++;
    } else if (state == 1 && s[i] == '#') {
      state++;
    } else if (state == 2 && s[i] >= '0' && s[i] <= '9') {
      if (cur_num < 0x20000) {
        cur_num = s[i] - '0' + cur_num * 10;
      }
    } else if (state == 2 && s[i] == ';') {
      state++;
    } else {
      state = 0;
    }
    if (state == 3 && (cur_num >= 32 && cur_num != 8232 && cur_num != 8233 && cur_num < 0x20000)) {
      name_buff_len = save_pos;
      name_buff[name_buff_len++] = cur_num;
    } else if (state == 3 && cur_num >= 0x10000) {
      name_buff[name_buff_len++] = win_to_utf8_convert[(unsigned char)s[i]];
      name_buff[save_pos] = '$';
    } else {
      name_buff[name_buff_len++] = win_to_utf8_convert[(unsigned char)s[i]];
    }
    if (state == 3) {
      state = 0;
    }
  }
  name_buff[name_buff_len] = 0;

  int *v = prepare_str_UTF8 (name_buff);
  char *t = prepare_res;

  while (*v != 0) {
    t += put_char_utf8 (*v++, t);
  }
  *t++ = 0;
  assert (t - prepare_res < MAX_NAME_SIZE);

  return clean_str (prepare_res);
}


int conv_fid (int x) {
  int *y = map_int_int_add (&fid_id, x);
  if (*y == 0) {
    *y = fids_size + 1;
    vector_pb (fids, x);
  }
  return (*y) - 1;
}

int conv_fid_name (dl_string x, int x_num) {
  int *y = map_string_int_add (&fid_name_id, x);
  if (*y == 0) {
    *y = x_num;
  }
  return *y;
}

float conv_val (float val) {
  if (val < 0) {
    val = 0.0;
  }
  if (val > 1e3) {
    val = 1e3;
  }
  return log (1.0 + 3 * val);
}

void data_init (void) {
  map_string_int_init (&fid_name_id);

  map_int_int_init (&fid_id);
  vector_init (fids);

  vector_init (scores);
  vector_init (users);
}

void data_free (void) {
  map_string_int_free (&fid_name_id);

  map_int_int_free (&fid_id);
  vector_free (fids);

  vector_free (scores);
  vector_free (users);
}

float *f_div;
int *p, *f_size;

int cmp_fid (const void *a, const void *b) {
  return f_size[*(int *)b] == f_size[*(int *)a] ? *(int *)a - *(int *)b : f_size[*(int *)b] - f_size[*(int *)a];
}

int cmp_score_fid (const void *a, const void *b) {
  return ((score *)a)->fid - ((score *)b)->fid;
}

int cmp_score_val (const void *a, const void *b) {
  float diff = ((score *)a)->val - ((score *)b)->val;
  return diff > 1e-9 ? -1 : diff < -1e-9;
}

void partial_score_val_sort (score *sc, int limit, int size) {
  score *begin_stack[32];
  score *end_stack[32];

  begin_stack[0] = sc;
  end_stack[0] = sc + size - 1;

  int depth;
  for (depth = 0; depth >= 0; --depth) {
    score *begin = begin_stack[depth];
    score *end = end_stack[depth];

    while (begin < end) {
      int offset = (end - begin) >> 1;
      dl_swap (*begin, begin[offset]);

      score *i = begin + 1, *j = end;

      while (1) {
        for ( ; i < j && begin->val < i->val; i++) {
        }

        for ( ; j >= i && j->val < begin->val; j--) {
        }

        if (i >= j) {
          break;
        }

        dl_swap (*i, *j);
        ++i;
        --j;
      }

      dl_swap (*begin, *j);

      if (j - begin <= end - j) {
        if (j + 1 < end && j + 1 < sc + limit) {
          begin_stack[depth] = j + 1;
          end_stack[depth++] = end;
        }
        end = j - 1;
      } else {
        if (j - 1 > begin) {
          begin_stack[depth] = begin;
          end_stack[depth++] = j - 1;
        }
        begin = j + 1;
      }
    }
  }
}

score types[257];
int types_size;

void add_type (int *x, int *y) {
  assert (types_size < 256);
  types[types_size].fid = *x;
  types[types_size].val = *y;
  types_size++;
}

inline float cross_product (score *a, score *b) {
//  return 1.0;
  float res = 0.2;
  while (a->fid >= 0 && b->fid >= 0) {
    if (a->fid == b->fid) {
      res += a->val * b->val;
      a++;
      b++;
    } else {
      if (a->val < b->val) {
        a++;
      } else {
        b++;
      }
    }
  }
  return res;
}


similarity_index_header header;

void usage (void) {
  printf ("usage: %s [-v] [-u<username>] -n<engineN> [-t<obj_type>] [-f<objs_limit>] [-h<hints_limit>] [-p<passes>]\n"
      "Preprocesses engineN binary files \"dump<server_num>.<obj_type>\" with information about user objects preferences\n"
      "  server_num is 3 digit number with leading zeros\n"
      "  write result to file similarity.<obj_type>\n"
      "\t-v\toutput statistical and debug information into stderr\n"
      "\t-t\ttype of objects, default is 41\n"
      "\t-a<dump_prefix>\tprefix of dumps from database\n"
      "\t-f\tnumber of significant objects, default is 1000, maximal is 1000000\n"
      "\t-h\tnumber of similar objects for each object, stored in similarity file, default equal to <objs_limit> and can't exceed it\n"
      "\t-p\tnumber of passes. More passes - less memory. Default is 1000\n"
      "\t-n\tnumber of hints engines, required\n",
      progname);
  exit (2);
}

int scores_cnt[FIDS_LIMIT];
char sim_name[50] = {"similarity"};

//#define DEBUG

#ifdef DEBUG
int my_num = -1;
//char *debug_str = "avantasia+crestfallen+";
//char *debug_str = "avantasia+in+paradise+serpents+";
//char *debug_str = "ayreon+castle+hall+the+";
//char *debug_str = "christ+jesus+laibach+superstar+";
//char *debug_str = "бони+вздумай+все+глаза+говорил+дарить+дрожать+им+как+клайд+кончено+любимая+люблю+не+никогда+них+ночные+окружены+плакать+при+радость+смей+снайперы+твои+я+";
char *debug_str = "frozen+madonna+";
//char *debug_str = "britney+spears+toxic+";
//char *debug_str = "beatles+the+yesterday+";
//char *debug_str = "could+fly+helloween+if+";
#endif

int main (int argc, char *argv[]) {
  int i, j, k, t;
  int passes = 1000;

  dl_set_debug_handlers();
  progname = argv[0];

  while ((i = getopt (argc, argv, "a:f:h:n:p:t:u:v")) != -1) {
    switch (i) {
      case 'a':
        database_dump_prefix = optarg;
        break;
      case 'f':
        fids_limit = atoi (optarg);
        assert (1 <= fids_limit && fids_limit <= FIDS_LIMIT);
        break;
      case 'h':
        hints_limit = atoi (optarg);
        assert (1 <= hints_limit && hints_limit <= FIDS_LIMIT);
        break;
      case 'n':
        engine_n = atoi (optarg);
        assert (1 <= engine_n && engine_n <= 1000);
        break;
      case 'p':
        passes = atoi (optarg);
        assert (1 <= passes && passes <= 100000);
        break;
      case 't':
        dump_type = atoi (optarg);
        assert (1 <= dump_type && dump_type <= 255);
        break;
      case 'u':
        username = optarg;
        break;
      case 'v':
        verbosity++;
        break;
    }
  }

  if (hints_limit == -1) {
    hints_limit = fids_limit;
  }

  if (argc != optind || engine_n == -1 || hints_limit > fids_limit) {
    usage();
    return 2;
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  data_init();
  char *fid_names = NULL, **fid_names_begins = NULL;
  long long fid_names_len = 0, max_fid_names_len = 0;
  int total_fids = 0, max_total_fids = 0;
  int first_engine = 1;
  const int MULTIPLIER = engine_n > 1 ? engine_n : 1;
  for (t = 0; t < engine_n; t++) {
    long long cur_pos = 0;
    dl_log_add (LOG_DEF, 3, "process dump[%d]\n", t);
    if (!set_cur_dump (t)) {
      continue;
    }

    char c = '0';
    int usn = 0;
    do {
      usn = usn * 10 + c - '0';
      assert (read (fd[dump_fd], &c, sizeof (char)) == sizeof (char));
      cur_pos++;
    } while ('0' <= c && c <= '9');

    if (verbosity > 0) {
      fprintf (stderr, "Users in current dump : %d\n", usn);
    }

    int lu = users_size, ls = scores_size;
    int old_size = users_size;

    size_t len = sizeof (user_dump) * usn, read_len;
    read_len = read (fd[dump_fd], vector_zpb (users, usn), len);
    assert (read_len == len);
    cur_pos += len;

    int scn = 0;
    for (i = 0; i < usn; i++) {
      scn += users[i + lu].len;
    }

    if (verbosity > 0) {
      fprintf (stderr, "Marks total in current dump : %d\n", scn);
    }

    len = sizeof (score) * scn;
    read_len = read (fd[dump_fd], vector_zpb (scores, scn), len);
    assert (read_len == len);
    cur_pos += len;

    if (cur_pos != fsize[dump_fd]) {
      char *cur_fid_names, **cur_fid_names_begins;
      int cur_fid_names_len, cur_total_fids;
      int multiplier = first_engine ? MULTIPLIER : 1;

      assert (read (fd[dump_fd], &cur_total_fids, sizeof (int)) == sizeof (int));
      if (verbosity > 0) {
        fprintf (stderr, "Total objects in dump %03d: %d\n", t, cur_total_fids);
      }
//      assert (cur_total_fids >= fids_size);

      long long len = fsize[dump_fd] - cur_pos - sizeof (int);
      assert (len < 2100000000);
      cur_fid_names = dl_malloc (len * multiplier);
      assert (read (fd[dump_fd], cur_fid_names, len) == len);
      cur_fid_names_len = len;

      cur_fid_names_begins = dl_malloc (sizeof (char *) * cur_total_fids * multiplier);

      char *t = cur_fid_names - 1;
      for (i = 0; i < cur_total_fids; i++) {
        t++;
        cur_fid_names_begins[i] = t;
        while (*t) {
          t++;
        }
        t += 2 * sizeof (int);
      }
      assert (t - cur_fid_names + 1 == len);

      if (first_engine) {
        header.has_names = 1;

        fid_names = cur_fid_names;
        fid_names_len = cur_fid_names_len;
        total_fids = cur_total_fids;
        fid_names_begins = cur_fid_names_begins;

        for (i = 0; i < total_fids; i++) {
          conv_fid_name (fid_names_begins[i], i + 1);
        }
        assert (map_string_int_used (&fid_name_id) == total_fids);

        max_fid_names_len = fid_names_len * MULTIPLIER;
        max_total_fids = total_fids * MULTIPLIER;
      } else {
        assert (header.has_names == 1);
        assert (cur_fid_names_len + fid_names_len < max_fid_names_len);
        assert (cur_total_fids + total_fids < max_total_fids);

        int ns = ls;
        for (i = 0; i < usn; i++) {
          int sn = users[lu + i].len;

          for (j = 0; j < sn; j++, ns++) {
            char *name = cur_fid_names_begins[scores[ns].fid - 1];
            int len = strlen (name) + 1 + 2 * sizeof (int);
            name = memcpy (fid_names + fid_names_len, name, len);

            scores[ns].fid = conv_fid_name (name, total_fids + 1);

            if (scores[ns].fid == total_fids + 1) {
              fid_names_begins[total_fids] = name;
              fid_names_len += len;
              total_fids++;
              assert (map_string_int_used (&fid_name_id) == total_fids);
            }
          }
        }
        assert (ns == scores_size);

        dl_free (cur_fid_names, cur_fid_names_len);
        dl_free (cur_fid_names_begins, sizeof (char *) * cur_total_fids);
      }
    }
    if (verbosity > 0) {
      fprintf (stderr, "Total_fids = %d\n", total_fids);
    }

    int ns = ls;
    scn = 0;
    for (i = 0; i < usn; i++) {
      int sn = users[old_size + i].len, os = ns;

      for (j = 0; j < sn; j++, ls++) {
        if (3 * scores[ls].val > 1) {
          scores[ns].fid = conv_fid (scores[ls].fid);
          scores[ns].val = conv_val (scores[ls].val);
          ns++;
        }
      }

      if (ns - os > 0) {
        qsort (scores + os, ns - os, sizeof (score), cmp_score_fid);
        j = os;
        for (k = os; k < ns; k++) {
          if (k == os || scores[k].fid != scores[k - 1].fid) {
            scores[j++] = scores[k];
          } else {
            if (scores[k].val > scores[j - 1].val) {
              scores[j - 1].val = scores[k].val;
            }
          }
        }
        ns = j;

        users[lu].len = ns - os;
        users[lu].id = users[old_size + i].id;
        lu++;
      }

      scn += ns - os;
    }
    scores_size = ns;
    users_size = lu;

    if (t == (engine_n - 1) / 2 && t + 1 != engine_n) {
      assert (scores_size < 1000000000);
      int nsn = (int)(scores_size * 2.1);
      vector_resize (scores, nsn);
    }
    first_engine = 0;
  }

  dl_realloc (fid_names, fid_names_len, max_fid_names_len);
  max_fid_names_len = fid_names_len;
  dl_realloc (fid_names_begins, sizeof (char *) * total_fids, sizeof (char *) * max_total_fids);
  max_total_fids = total_fids;
  vector_pack (users);
  vector_pack (scores);

  if (verbosity > 0) {
    fprintf (stderr, "\nLoading dumps finished for %lfs\n", clock() * 1.0 / CLOCKS_PER_SEC);
    fprintf (stderr, "Significant users in dumps : %d\n", users_size);
    fprintf (stderr, "Significant scores in dumps : %d\n", scores_size);
    fprintf (stderr, "Number of known objects : %d, memory_used = %lld\n", fids_size, dl_get_memory_used());
  }

  f_div = dl_malloc0 (sizeof (float) * fids_size);
  f_size = dl_malloc0 (sizeof (int) * fids_size);
  p = dl_malloc0 (sizeof (int) * fids_size);

  for (i = 0; i < scores_size; i++) {
    float x = scores[i].val;
    assert (scores[i].fid < fids_size);
    f_div[scores[i].fid] += x * x;
    f_size[scores[i].fid]++;
  }
  for (i = 0; i < fids_size; i++) {
    f_div[i] = 1.0f / sqrt (f_div[i]);
    assert (!isnan (f_div[i]) && !isinf (f_div[i]));
    p[i] = i;
  }

  if (fids_size > fids_limit) {
    double fchsum = 0;
    qsort (p, fids_size, sizeof (int), cmp_fid);

#ifdef DEBUG
    fprintf (stderr, "Bound:   f_size = %d, num = %d\n", f_size[fids_limit], p[fids_limit]);
    for (i = 0; i < fids_size; i++) {
      if (!strcmp (fid_names_begins[fids[i] - 1], debug_str)) {
        fprintf (stderr, "Current: f_size = %d, num = %d, i = %d", f_size[i], p[i], i);
        for (j = 0; j < fids_size; j++) {
          if (p[j] == i) {
            fprintf (stderr, ", pos = %d\n", j);
          }
        }
      }
    }
#endif
    for (i = fids_limit; i < fids_size; i++) {
      f_size[p[i]] = -1;
    }

    if (verbosity > 0) {
      for (i = 0; i < 10 && i < fids_limit; i++) {
        char *s = fid_names_begins[fids[p[i]] - 1];
        int *a = (int *)(s + strlen (s) + 1);
        fprintf (stderr, "Top object (%d of %d) : %s[%d;%d]\n", f_size[p[i]], users_size, s, a[0], a[1]);
      }
      for (i = max (fids_limit - 10, 0); i < fids_limit; i++) {
        char *s = fid_names_begins[fids[p[i]] - 1];
        int *a = (int *)(s + strlen (s) + 1);
        fprintf (stderr, "Tail object (%d of %d) : %s[%d;%d]\n", f_size[p[i]], users_size, s, a[0], a[1]);
      }
    }

    int k = 0;
    for (i = 0; i < fids_size; i++) {
      if (f_size[i] >= 0) {
        f_size[i] = k;
        fids[k] = fids[i];
        f_div[k++] = f_div[i];
      }
    }

    k = 0;
    score *s = scores;
    for (i = 0; i < users_size; i++) {
      int n = users[i].len;
      int prev_k = k;
      for (j = 0; j < n; j++) {
        if (f_size[s[j].fid] >= 0) {
          scores[k].fid = f_size[s[j].fid];
          scores[k].val = s[j].val * f_div[scores[k].fid];
          fchsum += s[j].val * (double)f_div[scores[k].fid];
//          fprintf (stderr, "%lf += %f * %f\n", fchsum, s[j].val, f_div[scores[k].fid]);
          k++;
        }
      }

      s += n;
      users[i].len = k - prev_k;
    }

    dl_realloc (f_div, sizeof (float) * fids_limit, sizeof (float) * fids_size);
    dl_free (f_size, sizeof (int) * fids_size);
    dl_free (p, sizeof (int) * fids_size);
    vector_resize (scores, k);
    vector_resize (fids, fids_limit);

    if (verbosity > 0) {
      fprintf (stderr, "\nNormalizing scores finished after %lfs\n", clock() * 1.0 / CLOCKS_PER_SEC);
      fprintf (stderr, "fchsum = %.20lf\n", fchsum);
      fprintf (stderr, "Significant scores in dumps : %d\n", k);
      fprintf (stderr, "Number of known objects : %d, memory_used = %lld\n\n", fids_limit, dl_get_memory_used());
    }
  } else {
    for (i = 0; i < scores_size; i++) {
      scores[i].val *= f_div[scores[i].fid];
    }

    dl_free (f_size, sizeof (int) * fids_size);
    dl_free (p, sizeof (int) * fids_size);
  }
  vector_pack (fids);

  char *fids_language = dl_malloc0 (sizeof (char) * fids_size);

  if (fid_names != NULL) {
    int new_len = 0;
    for (i = 0; i < fids_size; i++) {
      new_len += strlen (fid_names_begins[fids[i] - 1]) + 1 + 2 * sizeof (int);
    }

    char *fid_names_new = dl_malloc0 (new_len);
    char *fi = fid_names_new;
    char **fid_names_begins_new = dl_malloc0 (sizeof (char *) * fids_size);

    for (i = 0; i < fids_size; i++) {
#ifdef DEBUG
      if (!strcmp (fid_names_begins[fids[i] - 1], debug_str)) {
        fprintf (stderr, "old_num = %d, new_num = %d\n", fids[i], i);
        my_num = i;
      }
#endif
      int len = strlen (fid_names_begins[fids[i] - 1]) + 1 + 2 * sizeof (int);
      memcpy (fi, fid_names_begins[fids[i] - 1], len);
      for (j = 0; !fids_language[i] && fi[j]; j++) {
        fids_language[i] |= fi[j] < 0;
      }
      fid_names_begins_new[i] = fi;
      fi += len;
    }

    dl_free (fid_names, max_fid_names_len);
    dl_free (fid_names_begins, sizeof (char *) * max_total_fids);
    fid_names = fid_names_new;
    fid_names_len = new_len;
    fid_names_begins = fid_names_begins_new;
  }

  vector_int fids_types_begins;
  vector_score fids_types;
  vector_int_init (&fids_types_begins);
  vector_int_resize (&fids_types_begins, fids_size);
//  vector_score_resize (&fids_types, fids_size);
  vector_score_init (&fids_types);

  if (database_dump_prefix != NULL) {
    assert (header.has_names);

    map_string_int name_to_fid;
    map_string_int_init (&name_to_fid);
    for (i = 0; i < fids_size; i++) {
      *map_string_int_add (&name_to_fid, fid_names_begins[i]) = i;
    }

    vector (map_int_int, fid_to_type);
    vector_init (fid_to_type);
    vector_resize (fid_to_type, fids_size);

    for (t = 0; t < fids_size; t++) {
      map_int_int_init (fid_to_type + t);
    }

    int empty_name = 0, bad_name = 0, good_name = 0;
    for (t = 0; t < engine_n; t++) {
      dl_log_add (LOG_DEF, 3, "process database dump[%d]\n", t);
      if (!set_cur_database_dump (t)) {
        continue;
      }

      int pos = 0, r, end_pos = 0;
      do {
        for (i = pos; i < end_pos; i++) {
          buff[i - pos] = buff[i];
        }
        end_pos -= pos;
        pos = 0;

        r = read (fd[dump_fd], buff + end_pos, BUFF_LEN - end_pos);
        fsize[dump_fd] -= r;
        end_pos += r;

        int owner_id, type;

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

          READ(owner_id);
          READ(type);//unused id
          READ_STR(title);
          READ_STR(description);
          READ(type);

          assert (s == t + 1);
          pos = s - buff;

          if (owner_id < 0) {
            continue;
          }
          if (type < 0 || type >= 148 || type == 0 || type == 12) {
            empty_name++;
            continue;
          }

          title[title_len++] = ' ';
          memmove (title + title_len, description, description_len);
          title_len += description_len;
          title[title_len] = 0;

          description = title;
          title = prepare_str_cp1251 (title, title_len);
          if (title == NULL) {
//            fprintf (stderr, "Bad name %d: %s|||%s\n", owner_id, description, title);
            bad_name++;
            continue;
          }

          int *y = map_string_int_get (&name_to_fid, title);
          if (y == NULL) {
            bad_name++;
          } else {
            good_name++;
            ++*map_int_int_add (&fid_to_type[*y], type);
          }
        } while (1);
      } while (r > 0);

      assert (end_pos == 0 && pos == end_pos && fsize[dump_fd] == 0);
    }

    if (verbosity > 0) {
      fprintf (stderr, "\nLoading database dumps finished after %lfs\n", clock() * 1.0 / CLOCKS_PER_SEC);
      fprintf (stderr, "Number of empty objects = %d, number of unknown objects : %d, number of known objects : %d, memory_used = %lld\n\n", empty_name, bad_name, good_name, dl_get_memory_used());
    }

    int total = fids_size;
    for (t = 0; t < fids_size; t++) {
      total += map_int_int_used (fid_to_type + t);
      assert (total < 1000000000);
    }
    vector_score_resize (&fids_types, total);

    int cur = 0;
    for (t = 0; t < fids_size; t++) {
      fids_types_begins.v[t] = cur;

      types_size = 0;
      map_int_int_foreach (fid_to_type + t, add_type);
      assert (types_size == map_int_int_used (fid_to_type + t));

      if (types_size) {
        float total_val = 0;
        for (i = 0; i < types_size; i++) {
          total_val += (float)types[i].val * (float)types[i].val;
        }
        total_val = sqrt (total_val);
        float total_val_inv = 1.0f / total_val;
        for (i = 0; i < types_size; i++) {
          assert (types[i].fid >= 0);
          types[i].val *= total_val_inv;
        }

        qsort (types, types_size, sizeof (score), cmp_score_fid);
      }

      types[types_size].fid = -1;

      memcpy (&fids_types.v[cur], types, sizeof (score) * (types_size + 1));
      cur += types_size + 1;

      map_int_int_free (fid_to_type + t);
    }
    assert (cur == total);
    vector_free (fid_to_type);

    map_string_int_free (&name_to_fid);
  } else {
    memset (fids_types_begins.v, 0, sizeof (int) * fids_size);
    vector_score_resize (&fids_types, 1);
    fids_types.v[0].fid = -1;
  }

  header.magic = SIMILARITY_MAGIC;
  header.created_at = time (NULL);
  header.objs_type = dump_type;
  header.objs_limit = fids_size;
  header.hints_limit = hints_limit;
  header.has_names = fid_names_len;

  sprintf (sim_name + 10, ".%d", dump_type);
  assert (dl_open_file (4, sim_name, 2) != -1);

  if (verbosity > 0) {
    fprintf (stderr, "calc scores\n");
  }
  if (fids_size <= hints_limit) {
    header.file_type = 0;
    header.total_scores = fids_size * fids_size;
    float *C = dl_malloc0 (sizeof (float) * fids_size * fids_size);

    score *s = scores;
    for (i = 0; i < users_size; i++) {
      int n = users[i].len;
      for (j = 0; j + 1 < n; j++) {
        float cur_sc = s[j].val / conv_val (n),
              *cur_c = C + fids_size * s[j].fid;
        score *cur_type = fids_types.v + fids_types_begins.v[s[j].fid];
        char  cur_l = fids_language[s[j].fid];
        for (k = j + 1; k < n; k++) {
          cur_c[s[k].fid] += s[k].val * cur_sc * ((fids_language[s[k].fid] == cur_l) + 1) * cross_product (cur_type, fids_types.v + fids_types_begins.v[s[k].fid]);
        }
      }

      s += n;
    }

    int checksum = 0;
    for (i = 0; i < fids_size; i++) {
      float *a = C + fids_size * i + i,
            *b = a;
      for (j = i + 1; j < fids_size; j++, a++, b += fids_size) {
        *a = *b += *a;
        if (*a > 0.2) {
          checksum++;
        }
      }
    }
    if (verbosity > 0) {
      fprintf (stderr, "checksum = %d\n", checksum);
    }

    assert (write (fd[4], &header, sizeof (int) * 32) == (ssize_t)(sizeof (int) * 32));
    assert (write (fd[4], fids, sizeof (int) * fids_size) == (ssize_t)(sizeof (int) * fids_size));
    assert (write (fd[4], f_div, sizeof (float) * fids_size) == (ssize_t)(sizeof (float) * fids_size));
    assert (write (fd[4], C, sizeof (float) * fids_size * fids_size) == (ssize_t)(sizeof (float) * fids_size * fids_size));

    dl_free (C, sizeof (float) * fids_size * fids_size);
  } else {
    header.file_type = 1;

    int *count_sort_was = dl_malloc0 (fids_size * sizeof (int)),
        *count_sort_num = dl_malloc0 (fids_size * sizeof (int)),
        *count_sort_st = dl_malloc0 ((fids_size + 1) * sizeof (int)),
        count_sort_vn = 0, count_sort_id = 0;

//    long long ott = 0;
    score *s = scores;
    for (i = 0; i < users_size; i++) {
      int n = users[i].len;
      for (j = 0; j < n; j++) {
        scores_cnt[s[j].fid] += n;
      }

      s += n;
//      ott += n * n;
    }
//    fprintf (stderr, "3 %d %d %lld %lld\n", users_size, (int)(s - scores), ott, dl_get_memory_used());

    int max_score_size = 0;
    for (i = 0; i < fids_size; i++) {
      if (scores_cnt[i] > max_score_size) {
        max_score_size = scores_cnt[i];
      }
    }
    score *count_sort_res = dl_malloc0 (max_score_size * sizeof (score));

    vector_score *v = dl_malloc (sizeof (vector_score) * fids_size);
    int total_scores = 0;
    int pass;
    for (pass = 0; pass < passes; pass++) {
      for (i = pass; i < fids_size; i += passes) {
        vector_score_init (&v[i]);
        vector_score_resize (&v[i], scores_cnt[i]);
      }
//      fprintf (stderr, "4 %lld\n", dl_get_memory_used());

      s = scores;
      score cur;
      for (i = 0; i < users_size; i++) {
        int n = users[i].len;
        for (j = 0; j < n; j++) {
          if (s[j].fid % passes != pass) {
            continue;
          }

          float cur_sc = s[j].val / conv_val (n);
          char cur_l = fids_language[s[j].fid];
          score *cur_type = fids_types.v + fids_types_begins.v[s[j].fid];
          vector_score *vv = &v[s[j].fid];
          for (k = 0; k < n; k++) {
            cur.fid = s[k].fid;
            cur.val = s[k].val * cur_sc * ((fids_language[s[k].fid] == cur_l) + 1) * cross_product (cur_type, fids_types.v + fids_types_begins.v[s[k].fid]);
            vector_score_pb (vv, cur);
          }
#ifdef DEBUG
          if (s[j].fid == my_num && 0) {
            fprintf (stderr, "User %d\n", users[i].id);
            for (k = 0; k < n; k++) {
              fprintf (stderr, "%s : %f %f\n", fid_names_begins[s[k].fid], s[k].val, f_div[s[k].fid]);
            }
            fprintf (stderr, "\n");
          }
#endif
        }

        s += n;
      }

      if (verbosity > 0 && pass % 100 == 99) {
        fprintf (stderr, "\nSquarring matrix at pass %d finished after %lfs\n", pass, clock() * 1.0 / CLOCKS_PER_SEC);
        fprintf (stderr, "Memory_used = %lld\n", dl_get_memory_used());
      }

      for (i = pass; i < fids_size; i += passes) {
        vector_score *vv = &v[i];
        score *sc = vv->v;

//        qsort (sc, vv->v_size, sizeof (score), cmp_score_fid_stable);
        count_sort_id++;

        int count_sort_i;
        count_sort_vn = 0;
        count_sort_st[0] = 0;
        for (count_sort_i = 0; count_sort_i < vv->v_size; count_sort_i++) {
          int x = sc[count_sort_i].fid;
          if (count_sort_was[x] != count_sort_id) {
            count_sort_num[x] = count_sort_vn;
            count_sort_was[x] = count_sort_id;
            count_sort_vn++;
            count_sort_st[count_sort_vn] = 0;
          }

          count_sort_st[count_sort_num[x] + 1]++;
        }
        for (count_sort_i = 0; count_sort_i < count_sort_vn; count_sort_i++) {
          count_sort_st[count_sort_i + 1] += count_sort_st[count_sort_i];
        }
        for (count_sort_i = 0; count_sort_i < vv->v_size; count_sort_i++) {
          int x = sc[count_sort_i].fid;
          count_sort_res[count_sort_st[count_sort_num[x]]++] = sc[count_sort_i];
        }

        memcpy (sc, count_sort_res, vv->v_size * sizeof (score));


        j = 0;
        int cur = 0;
        for (k = 0; k < vv->v_size; k++) {
          if (sc[k].fid != i) {
            if (!k || sc[k].fid != sc[k - 1].fid) {
              if (cur) {
                sc[j - 1].val *= logf (cur * 10);
              }
              sc[j++] = sc[k];
              cur = 1;
            } else {
              sc[j - 1].val += sc[k].val;
              cur++;
            }
          }
        }

//        qsort (sc, j, sizeof (score), cmp_score_val);
        partial_score_val_sort (sc, hints_limit, j);
        if (j > hints_limit) {
          j = hints_limit;
        }
        vector_score_resize (vv, j);

        total_scores += vv->v_size;
        assert (total_scores < 250000000);
#ifdef DEBUG
        if (i == my_num) {
          for (k = 0; k < vv->v_size; k++) {
            fprintf (stderr, "%s : %f\n", fid_names_begins[vv->v[k].fid], vv->v[k].val);
          }
        }
#endif
      }

      if (verbosity > 0 && pass % 100 == 99) {
        fprintf (stderr, "\nSimplifying matrix at pass %d finished after %lfs\n", pass, clock() * 1.0 / CLOCKS_PER_SEC);
        fprintf (stderr, "Total_scores = %d, memory_used = %lld\n", total_scores, dl_get_memory_used());
      }
    }
    header.total_scores = total_scores;

    score *C = dl_malloc (sizeof (score) * total_scores);
    int *C_index = dl_malloc (sizeof (int) * (fids_size + 1));
    j = 0;
    for (i = 0; i < fids_size; i++) {
      vector_score *vv = &v[i];
      memcpy (C + j, vv->v, sizeof (score) * vv->v_size);
      C_index[i] = j;
      j += vv->v_size;
    }
    C_index[i] = j;
    assert (j == total_scores);

    assert (write (fd[4], &header, sizeof (int) * 32) == (ssize_t)(sizeof (int) * 32));
    assert (write (fd[4], fids, sizeof (int) * fids_size) == (ssize_t)(sizeof (int) * fids_size));
    assert (write (fd[4], f_div, sizeof (float) * fids_size) == (ssize_t)(sizeof (float) * fids_size));
    assert (write (fd[4], C_index, sizeof (int) * (fids_size + 1)) == (ssize_t)(sizeof (int) * (fids_size + 1)));
    assert (write (fd[4], C, sizeof (score) * total_scores) == (ssize_t)(sizeof (score) * total_scores));

    dl_free (C_index, sizeof (int) * (fids_size + 1));
    dl_free (C, sizeof (score) * total_scores);

    for (i = 0; i < fids_size; i++) {
      vector_score_free (&v[i]);
    }

    dl_free (v, sizeof (vector_score) * fids_size);

    dl_free (count_sort_was, fids_size * sizeof (int)),
    dl_free (count_sort_num, fids_size * sizeof (int)),
    dl_free (count_sort_st, (fids_size + 1) * sizeof (int)),
    dl_free (count_sort_res, max_score_size * sizeof (score));
  }

  if (fid_names != NULL) {
    assert (write (fd[4], fid_names, fid_names_len) == fid_names_len);

    dl_free (fid_names, fid_names_len);
    dl_free (fid_names_begins, sizeof (char *) * fids_size);
  }
  dl_close_file (4);
  data_free();
  vector_int_free (&fids_types_begins);
  vector_score_free (&fids_types);
  dl_free (fids_language, sizeof (char) * fids_size);
  dl_free (f_div, sizeof (float) * fids_size);
  if (dl_get_memory_used() != 0) {
    fprintf (stderr, "Memory left = %lld\n", dl_get_memory_used());
    assert (dl_get_memory_used() == 0);
  }
  return 0;
}
