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
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>


int import_mode;
char *import_file;
int import_fd;

#define	COMMENT_TYPES_MASK	0x01f00000
#define LEV_BOOKMARK_INSERT 0x247faa00

int verbosity;

#define WRITE_BUFF_SIZE 100000
#define MAX_SPLIT 1000
char *wptr[MAX_SPLIT];
char write_buff[MAX_SPLIT][WRITE_BUFF_SIZE];
int write_fd[MAX_SPLIT];

#define IMPORT_BUFF_SIZE 1000000
char import_buff[IMPORT_BUFF_SIZE + 1];
char *rptr = import_buff + IMPORT_BUFF_SIZE;
char *rend = import_buff + IMPORT_BUFF_SIZE;

char cur_write_name[1000];
char *name_pattern;

int log_split_mod;
unsigned allowed_types_mask = COMMENT_TYPES_MASK;

#define SPLIT_PARAM_OWNER 0
#define SPLIT_PARAM_PLACE 1
#define SPLIT_PARAM_OWNERXPLACE 2
int split_param = SPLIT_PARAM_PLACE;



void flush_write (int x) {
  //fprintf (stderr, "%d - %d\n", x, write_fd[x]);
  if (verbosity) {
    fprintf (stderr, "writing %ld bytes to file %d\n", (long)(wptr[x] - write_buff[x]), x);
  }
  if (wptr[x] != write_buff[x]) {
    int r = write (write_fd[x], write_buff[x], wptr[x] - write_buff[x]);
    if (r <= 0) {
      fprintf (stderr, "error_code %d (%s)\n", r, strerror (errno));
      exit (2);
    }
    if (r != wptr[x] - write_buff[x]) {
      fprintf (stderr, "written only part...\n");
      exit (2);
    }
  }
  wptr[x] = write_buff[x];
}

void write_int (int x, int d) {
  if (wptr[x] + 4 > write_buff[x] + WRITE_BUFF_SIZE)  {
    flush_write (x);
  }
  //fprintf (stderr, ".");
  //assert (*(int *)(wptr[x]) == 0);
  *(int *)(wptr[x]) = d;
  wptr[x] += 4;
}


#define USERMOD 1000
#define MAXUSERS 1000000
int a[MAXUSERS];
int b[MAXUSERS];
int c[MAXUSERS];
int u[MAXUSERS];
int z[32];

void write_stat (void) {
  int i;
  int max = 0;
  for (i = 0; i < MAXUSERS; i++)  {
    b[a[i]] ++;
    if (a[i] > max) max = a[i];
  }
  double mean = 0;
  int cou = 0;
  for (i = 1; i <= max; i++) {
    mean += i * b[i];
    cou += b[i];
  }
  mean /= cou;
  fprintf (stderr, "mean bookmarks number is %lf\n", mean);
  fprintf (stderr, "maximal bookmarks number is %d\n", max);
  fprintf (stderr, "total users number is %d\n", cou);

  int e = 0;
  for (i = 1; i <= 100; i++) {
    e += b[i];
    fprintf (stderr, "number of users with %d bookmarks is %d (with more %d)\n", i, b[i], cou - e);
  }

  int min = 1000000000;
  max = 0;
  mean = 0;
  int mai = -1;
  int mii = -1;
  for (i = 0; i < log_split_mod; i++) {
    if (max < c[i]) {
      max = c[i];
      mai = i;
    }
    if (min > c[i]) {
      min = c[i];
      mii = i;
    }
    mean += c[i];
  }
  mean /= log_split_mod;
  fprintf (stderr, "max items in file %d (file %d)\n", max, mai);
  fprintf (stderr, "min items in file %d (file %d)\n", min, mii);
  fprintf (stderr, "mean items in file %d\n", (int)mean);

  /*fprintf (stderr, "number of items with owner = 0, place = 1 is %d (%lf%%)\n", z[0], z[0]/(double)z[1]*100);
  fprintf (stderr, "(%d,%d,%d,%d,%d)\n", z[20], z[21], z[22], z[23], z[24]);

  max = 0;
  int ii = 0;
  for (i = 0; i < MAXUSERS; i++) {
    if (max < u[i]) {
      max = u[i];
      ii = i;
    }
  }
  fprintf (stderr, "most rich owner is %d (%d items (%lf%%))\n", ii, max, max / (double)z[1] * 100);*/



}

inline int gen_split_param (int type, int owner, int place) {
  if (split_param == SPLIT_PARAM_PLACE) {
    return place;
  } else if (split_param == SPLIT_PARAM_OWNER) {
    return owner;
  } else if (split_param == SPLIT_PARAM_OWNERXPLACE) {
    return owner ^ place ^ type;
  } else {
    assert (0);
    return 0;
  }
}

void write_event (int user_id, int type, int owner, int place) {
  int x;
  int t = gen_split_param (type, owner, place);
  if (t > 0) {
    x = t % log_split_mod;
  } else {
    x = (-t) % log_split_mod;
  }
  if (x == 422) {
    z[type] ++;
    z[0] ++;
    if (owner > 0) {
      u[owner / log_split_mod] ++;
    } else {
      u[(-owner) / log_split_mod] ++;
    }
  }
  z[1] ++;

  if (x == 1) {
    assert (user_id / USERMOD + MAXUSERS/2 < MAXUSERS && user_id / USERMOD + MAXUSERS/2 >= 0);
    a[MAXUSERS/2 + user_id / USERMOD] ++;
  }
  c[x]++;
  write_int (x, LEV_BOOKMARK_INSERT + type);
  write_int (x, user_id);
  write_int (x, owner);
  write_int (x, place);
}

void generate_write_name (int i) {
  int z = 2;
  if (log_split_mod > 100) {
    z = 3;
  }
  if (log_split_mod > 1000) {
    z = 4;
  }
  if (z == 2) {
    sprintf (cur_write_name, "%s%02d.bin", name_pattern, i);
  } else if (z == 3) {
    sprintf (cur_write_name, "%s%03d.bin", name_pattern, i);
  } else if (z == 4) {
    sprintf (cur_write_name, "%s%04d.bin", name_pattern, i);
  }
}

void advance_buff (void) {
  //assert (rptr != import_buff);
  memmove (import_buff, rptr, rend - rptr);
  rend -= (rptr - import_buff);
  rptr = import_buff;
  rend += read (import_fd, rend, import_buff + IMPORT_BUFF_SIZE - rend);
}

void eat_whitespaces (void) {
  while (1) {
    if (rptr == rend) {
      advance_buff ();
      if (rptr == rend) {
        return;
      }
    }
    if (*rptr > 32) {
      break;
    }
    rptr++;
  }
}

int next_str (char *s, int max_len) {
  int i = 0;
  eat_whitespaces ();
  if (rptr == rend) {
    return -1;
  }
  while (i < max_len && (*rptr > 32) ) {
    s[i++] = *(rptr++);
    if (rptr == rend) {
      advance_buff ();
      if (rptr == rend) {
        break;
      }
    }
  }
  s[i++] = 0;
  return 0;
}

int search_char (char c) {
  while (1) {
    if (rptr < rend && *(rptr++) == c) {
      break;
    }
    if (rptr == rend) {
      advance_buff ();
      if (rptr == rend) {
        return -1;
      }
    }
  }
  return 0;
}

char read_char (void) {
  if (rptr == rend) {
    advance_buff ();
  }
  if (rptr == rend) {
    return 0;
  } else {
    return *(rptr++);
  }
}

int read_int (void) {
  if (rptr == rend) {
    advance_buff ();
    assert (rptr != rend);
  }
  while ((*rptr < '0' || *rptr > '9') && *rptr != '-') {
    rptr ++;
    if (rptr == rend) {
      advance_buff ();
      assert (rptr != rend);
    }
  }
  if (rptr + 20 > rend) {
    advance_buff ();
  }
  int sign = 1;
  while (*rptr == '-') {
    sign = -sign;
    rptr ++;
  }
  int res = 0;
  while (rptr < rend && *rptr >= '0' && *rptr <= '9') {
    res = res * 10 + *rptr - '0';
    rptr ++;
  }
  if (sign < 0) {
    res = -res;
  }
  return res;
}

static inline int valid_type (unsigned t) {
  return t < 32 && ((allowed_types_mask >> t) & 1);
}


int new_type (int x) {
  switch (x) {
    case 1: return 21;
    case 2: return 22;
    case 3: return 20;
    case 4: return 23;
    case 5: return 24;
    default:
      break;
  }
  return 33;

}


int import_dump (void) {

  //int prev_time = 0;
  import_fd = open (import_file, O_RDONLY);
  if (import_fd < 0) {
    fprintf (stderr, "Can not open dump (file %s)\n", import_file);
    return 1;
  }
  int i;
  for (i = 0; i < log_split_mod; i++) {
    generate_write_name (i);
    //write_fd[i] = open (cur_write_name, O_CREAT  | O_APPEND, 0644);
    write_fd[i] = open (cur_write_name,  O_APPEND | O_WRONLY | O_CREAT, 0644);
    //fprintf (stderr, "(%d - %d) ", i, write_fd[i]);
    if (write_fd[i] < 0) {
      fprintf (stderr, "can not open file %s for write\n", cur_write_name);
      return 1;
    }
    wptr[i] = write_buff[i];
  }

  int xx = 0;
  char s[1001];
  while (1) {
    int ok = 0;
    while (1) {
      int x = next_str (s, 1000);
      if (x < 0) {
        break;
      }
      if (!strncmp (s, "VALUES", 6)) {
        //scanf ("%s\n", s);
        ok = 1;
        break;
      }
    }
    if (!ok) {
      break;
    }
    while (1) {
      search_char ('(');
      int a,b,c,d,e,f;
      a = read_int (); b = read_int (); c = read_int ();
      d = read_int (); e = read_int (); f = read_int ();
      d = new_type (d);
      if (valid_type (d)) {
        write_event (b, d, f, c);
        xx ++;
      } else {
        fprintf (stderr, "assert: d = %d\n", d);
        for (i = -100; i < 100; i++) {
          fprintf (stderr, "%c", *(rptr + i));
        }
        fprintf (stderr, "\n");
      }
      if (verbosity && !(xx & 0xfffff)) {
        fprintf (stderr, "written %d items\n", xx);
      }

      assert (read_char () == ')');
      char c1 = read_char ();
      if (c1 == ';') {
        break;
      }
      if (c1 != ',') {
        fprintf (stderr, "assert: c = %c\n", c1);
        for (i = -100; i < 100; i++) {
          fprintf (stderr, "%c", *(rptr + i));
        }
        fprintf (stderr, "\n");

      }
      assert (c1 == ',' || c1 == '-');
    }
    /*if (prev_time != now) {
      prev_time = now;
      cron ();
    } */
  }
  for (i = 0; i < log_split_mod; i++) {
    flush_write (i);
  }
  if (verbosity) {
    fprintf (stderr, "Total %d bookmarks.\n", xx);
    if (verbosity >= 2) {
      write_stat ();
    }
  }
  return 0;

}

int import_dump2 (void) {

  //int prev_time = 0;
  import_fd = open (import_file, O_RDONLY);
  if (import_fd < 0) {
    fprintf (stderr, "Can not open dump (file %s)\n", import_file);
    return 1;
  }
  int i;
  for (i = 0; i < log_split_mod; i++) {
    generate_write_name (i);
    //write_fd[i] = open (cur_write_name, O_CREAT  | O_APPEND, 0644);
    write_fd[i] = open (cur_write_name,  O_APPEND | O_WRONLY | O_CREAT, 0644);
    //fprintf (stderr, "(%d - %d) ", i, write_fd[i]);
    if (write_fd[i] < 0) {
      fprintf (stderr, "can not open file %s for write\n", cur_write_name);
      return 1;
    }
    //fprintf (stderr, "%d - %s\n", i, cur_write_name);
    wptr[i] = write_buff[i];
  }

  int xx = 0;
  while (1) {
    int a,b,c,d,e,f;
    a = read_int (); b = read_int (); c = read_int ();
    d = read_int (); e = read_int (); f = read_int ();
    d = new_type (d);
    if (valid_type (d)) {
      write_event (b, d, f, c);
      xx ++;
    }
    if (verbosity && !(xx & 0xfffff)) {
      fprintf (stderr, "written %d items\n", xx);
    }
    eat_whitespaces ();
    if (rptr == rend) {
      break;
    }

  }
  for (i = 0; i < log_split_mod; i++) {
    flush_write (i);
  }
  if (verbosity) {
    fprintf (stderr, "Total %d bookmarks.\n", xx);
    if (verbosity >= 2) {
      write_stat ();
    }
  }
  return 0;

}

char *progname;

void usage (void) {
  printf ("usage: %s [-v] -m <log-split-mod> -n <import-file> [-o] [-x] <answer-file-prefix>\n"
	  "\tConverts dump of bookmarks database into news-engine binlog\n"
    "\t-o: Split by owner (default split by place)\n"
    "\t-x: Split by (owner xor place) (default by place)\n"
    "\t-z: Use txt file for import\n", progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  progname = argv[0];
  int i;
  int mode = 0;
  while ((i = getopt (argc, argv, "m:vn:oxz")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'm':
      log_split_mod = atoi (optarg);
      break;
    case 'n':
      import_file = optarg;
      break;
    case 'o':
      split_param = SPLIT_PARAM_OWNER;
      break;
    case 'x':
      split_param = SPLIT_PARAM_OWNERXPLACE;
      break;
    case 'z':
      mode = 1;
      break;
    case 'h':
    default:
      usage ();
      return 2;
    }
  }
  if (argc != optind + 1 || !import_file || !log_split_mod) {
    usage();
    return 2;
  }
  name_pattern = argv[optind];
  if (mode == 0) {
    import_dump ();
  } else {
    import_dump2 ();
  }
  return 0;
}
