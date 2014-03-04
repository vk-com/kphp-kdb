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

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>

#include "base64.h"
#include "crc32.h"
#include "md5.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "storage-content.h"
#include "kdb-storage-binlog.h"
#include "net-crypto-aes.h"

#define mytime() (get_utime(CLOCK_MONOTONIC))

int verbosity = 0;
int split_rem, split_modulo = 0;

char *row_prefix = "<row>";
char *progname, *username;
long long volume_id = -1;
FILE *pmemcache_dump = NULL;
int use_old_secret = 0;

/*************************** Hash (string) **************************************/
#define MAX_DIRS 4

typedef struct {
  char *key;
  char *prefix[MAX_DIRS];
  int n;
  time_t mtime;
} hash_entry_t;


#define MAX_STR        4800000
#define HASH_STR_PRIME 5108767
static hash_entry_t *HS[HASH_STR_PRIME];
int tot_str;

static void hash_str_init (void) {
  memset (HS, 0, sizeof (HS));
  tot_str = 0;
}


static hash_entry_t *get_str_f (const char *s, int force) {
  unsigned int h1 = 0, h2 = 0;
  const char *p = s;
  while (*p) {
    unsigned char c = *p++;
    h1 *= 131;
    h1 += c;
    h2 *= 239;
    h2 += c;
  }
  h1 %= HASH_STR_PRIME;
  h2 = 1 + (h2 % (HASH_STR_PRIME - 1));
  while (HS[h1]) {
    if (!strcmp (HS[h1]->key, s)) {
      return HS[h1];
    }
    if ( (h1 += h2) >= HASH_STR_PRIME) {
      h1 -= HASH_STR_PRIME;
    }
  }
  if (force) {
    tot_str++;
    assert (tot_str <= MAX_STR);
    HS[h1] = zmalloc0 (sizeof (hash_entry_t));
    HS[h1]->key = zstrdup (s);
    return HS[h1];
  }
  return NULL;
}

static int dump_rows = 0;
static int local_id = 1;

#define PATH_BUFFSIZE 16384

unsigned long long make_secret (void) {
  unsigned long long r = 0;
  int i = 0;
  while (1) {
    r |= (lrand48 () >> 15) & 0xffff;
    if (++i == 4) {
      break;
    }
    r <<= 16;
  }
  return r;
}

static int extract_secret (const char* filename, unsigned long long *secret) {
  int i;
  unsigned long long x;
  char c;
  char s[4];
  memset (s, 0, 4);
  *secret = 0;
  if (sscanf (filename, "%c_%llx.%3s", &c, &x, s) == 3) {
    *secret = x << 32;
    *secret |= ((unsigned int) ((unsigned char) c)) << 24;
  } else if (sscanf (filename, "%llx.%3s", &x, s) == 2) {
    *secret = x << 24;
  } else {
    *secret = make_secret ();
    return 1;
  }
  for (i = 0; i < 3; i++) {
    *secret |= ((unsigned int) ((unsigned char) s[i])) << (8 * (2 - i));
  }
  return 0;
}

static int get_fsize (int fd, time_t *mtime) {
  struct stat buf;
  if (fstat (fd, &buf)) {
    return -1;
  }
  if (mtime) {
    *mtime = buf.st_mtime;
  }
  return buf.st_size;
}

static unsigned char *read_whole_file (const char *path, int *l, time_t *mtime) {
  int fd = open (path, O_RDONLY);
  if (fd < 0) {
    return NULL;
  }
  *l = get_fsize (fd, mtime);
  if (*l < 0) {
    close (fd);
    return NULL;
  }
  unsigned char *p = ztmalloc ((*l + 3) & -4);
  int bytes_read = read (fd, p, *l);
  close (fd);
  if (bytes_read != *l) {
    return NULL;
  }
  return p;
}

static int compare_buff (const unsigned char *a, const unsigned char *b, int l) {
  int i, res = 3;
  for (i = 0; i < l; i++) {
    if (a[i] != b[i]) {
      if (a[i] && !b[i]) {
        res &= ~2;
      } else if (b[i] && !a[i]) {
        res &= ~1;
      } else if (a[i] && b[i]) {
        return 0;
      }
      if (!res) {
        return 0;
      }
    }
  }
  return res;
}

//static int opened_base_doc, opened_aux_doc;
static int successfully_read[MAX_DIRS];

static inline int fits (int user_id) {
  if (user_id < 0) {
    user_id = -user_id;
  }
  return (user_id % split_modulo == split_rem);
}

static int check_depth0 (int i) {
  int j, m = 100 % split_modulo;
  i %= split_modulo;
  if (m == 0) {
    return (i == split_rem);
  }
  for (j = 0; j < 100; j++) {
    if (i == split_rem) {
      return 1;
    }
    if ( (i += m) >= split_modulo) {
      i -= split_modulo;
    }
  }
  return 0;
}

static int check_depth1 (int i) {
  return (i % split_modulo) == split_rem;
}


static unsigned log_crc32 = 0;

static int binlog_write (const void *buff, int sz) {
  if (sz != write (1, buff, sz)) {
    return -1;
  }
  log_crc32 = ~crc32_partial (buff, sz, ~log_crc32);
  log_pos += sz;
  return 0;
}

static void import_lev_start (void) {
  int md5_mode = 0;
  struct lev_start *E = malloc (36);
  E->type = LEV_START;
  E->schema_id = STORAGE_SCHEMA_V1;
  E->extra_bytes = 12;
  E->split_mod = split_modulo;
  E->split_min = split_rem;
  E->split_max = split_rem + 1;
  memcpy (E->str, &volume_id, 8);
  memcpy (&E->str[8], &md5_mode, 4);
  assert (!binlog_write (E, 36));
  free (E);
}

static int import_doc (int user_id, int album_id, int local_id, unsigned long long *secret, hash_entry_t *E) {
  vkprintf (3, "import_doc (user_id = %d, album_id = %d, local_id = %d, secret = %llx, E->key = %s\n",
    user_id, album_id, local_id, *secret, E->key);
  if (!fits (user_id)) {
    kprintf ("user_id %% split_modulo != split_rem, user_id = %d, album_id = %d, local_id = %d, E->key = %s\n",
      user_id, album_id, local_id, E->key);
    exit (1);
  }
  int len[MAX_DIRS];
  time_t mtime[MAX_DIRS];
  unsigned char *io_buff[MAX_DIRS];
  int maxl = -1, i;
  for (i = 0; i < E->n; i++) {
    io_buff[i] = NULL;
    char path[PATH_MAX];
    if (snprintf (path, PATH_MAX, "%s%s", E->prefix[i], E->key) >= PATH_MAX) {
      kprintf ("path too long\n");
      exit (1);
    }
    io_buff[i] = read_whole_file (path, len + i, mtime + i);
    if (io_buff[i]) {
      successfully_read[i]++;
      if (maxl < len[i]) {
        maxl = len[i];
      }
    } else {
      vkprintf (3, "read_whole_file (%s) fails\n", path);
    }
  }
  int k = 0;
  int z[MAX_DIRS];
  for (i = 0; i < E->n; i++) {
    if (io_buff[i] && len[i] == maxl) {
      z[k] = i;
      io_buff[k] = io_buff[i];
      mtime[k] = mtime[i];
      k++;
    }
  }
  if (!k) {
    kprintf ("Fail to read %s\n", E->key);
    return -1;
  }
  unsigned char *buff = io_buff[0];
  int l = len[0];
  time_t t = mtime[0];
  int id = z[0];
  if (k >= 2) {
    int r = compare_buff (io_buff[0], io_buff[1], maxl);
    switch (r) {
      case 0:
        kprintf ("%s%s and %s%s are differ\n", E->prefix[z[0]], E->key, E->prefix[z[1]], E->key);
        break;
      case 1:
        buff = io_buff[1];
        t = mtime[1];
        id = z[1];
        break;
      case 2:
      case 3:
        break;
    }
  }

  if (id) {
    kprintf ("use aux image: %s%s\n" , E->prefix[id], E->key);
  }

  struct lev_storage_file EE;
  EE.type = LEV_STORAGE_FILE;
  EE.secret = *secret;
  //E.user_id = user_id;
  //E.album_id = album_id;
  EE.local_id = local_id;
  EE.content_type = detect_content_type (buff, l);
  if (EE.content_type < 0) {
    kprintf ("unknown content_type: %s%s\n", E->prefix[id], E->key);
  }
  EE.size = l;
  EE.mtime = t;
  int sz = sizeof (struct lev_storage_file);
  md5_context ctx;
  md5_starts (&ctx);
  md5_update (&ctx, buff, l);
  md5_finish (&ctx, EE.md5);
  EE.crc32 = compute_crc32 (buff, l);

  if (!use_old_secret) {
    unsigned long long t[2];
    memcpy (&t[0], &EE.md5[0], 16);
    *secret = EE.secret ^= t[0] ^ t[1];
  }

  if (binlog_write (&EE, sz)) {
    kprintf ("write LEV_STORAGE_FILE header to stdout fail");
    exit (1);
  }

  int old_l = l;
  l = (l + 3) & -4;
  if (l != old_l) {
    memset (&buff[old_l], 0, l - old_l);
  }

  if (binlog_write (buff, l)) {
    kprintf ("write LEV_STORAGE_FILE body to stdout fail");
    exit (1);
  }

  struct lev_crc32 C;
  C.type = LEV_CRC32;
  C.timestamp = time (0);
  C.pos = log_pos;
  C.crc32 = log_crc32;
  if (binlog_write (&C, sizeof (struct lev_crc32))) {
    kprintf ("write LEV_CRC32 to stdout fail");
    exit (1);
  }

  return 0;
}

static char walk_path[PATH_BUFFSIZE];
static int walk_prefix_len;

static int walk_rec (DIR *D[5], int D_ID[5], int depth, char *base_path) {
  const int l = strlen (walk_path);
  D[depth] = opendir (walk_path);
  if (D[depth] == NULL) {
    kprintf ("opendir (%s) fail (depth = %d). %m\n", walk_path, depth);
    return -1;
  }
  struct dirent *entry;
  while ( (entry = readdir (D[depth])) != NULL) {
    if (!strcmp (entry->d_name, ".") || !strcmp (entry->d_name, "..")) {
      continue;
    }
    sprintf (walk_path + l, "/%s", entry->d_name);
    struct stat st;
    if (stat (walk_path, &st)) {
      kprintf ("stat (%s) fail. %m\n", walk_path);
      continue;
    }
    if (S_ISDIR(st.st_mode) && depth < 4) {
      errno = 0;
      D_ID[depth + 1] = strtol (entry->d_name, NULL, 10);
      if (errno) {
        continue;
      }
      int skip = 0;
      switch (depth) {
        case 0:
          if (!check_depth0 (D_ID[1])) {
            skip = 1;
          }
          break;
        case 1:
          if (!check_depth1 (D_ID[1] + D_ID[2] * 100)) {
            skip = 1;
          }
          break;
        case 2:
          if (D_ID[3] % split_modulo != split_rem) {
            skip = 1;
          }
          break;
      }
      if (skip) {
        continue;
      }
      walk_rec (D, D_ID, depth + 1, base_path);
    } else if (S_ISREG(st.st_mode) && depth >= 3 && depth <= 4) {
      hash_entry_t *E = get_str_f (walk_path + walk_prefix_len, 1);
      assert (E->n < MAX_DIRS);
      if (E->mtime < st.st_mtime) {
        E->mtime = st.st_mtime;
      }
      E->prefix[E->n++] = base_path;
    }
  }
  walk_path[l] = 0;
  closedir (D[depth]);
  return 0;
}
/*
static int cmp_str (const void *x, const void *y) {
  const char *s1 = *(const char **) x;
  const char *s2 = *(const char **) y;
  return strcmp (s1, s2);
}
*/

static int cmp_he (const void *x, const void *y) {
  const hash_entry_t *h1 = *(const hash_entry_t **) x;
  const hash_entry_t *h2 = *(const hash_entry_t **) y;
  const char *s1 = h1->key;
  const char *s2 = h2->key;
  if (s1 == s2) { return 0; }
  const char *r1 = strrchr (s1, '/');
  assert (r1 != NULL);
  const char *r2 = strrchr (s2, '/');
  assert (r2 != NULL);
  int l1 = r1 - s1, l2 = r2 - s2;
  if (l1 != l2) {
    return strcmp (s1, s2);
  }
  int c = strncmp (s1, s2, l1);
  if (c) {
    return c;
  }
  if (h1->mtime < h2->mtime) {
    return -1;
  }
  if (h1->mtime > h2->mtime) {
    return  1;
  }
  return strcmp (r1 + 1, r2 + 1);
}


static void secret_to_base64url (unsigned long long secret, char output[12]) {
  int r = base64url_encode ((unsigned char *) &secret, 8, output, 12);
  assert (!r);
}

static int import_images (char *P[MAX_DIRS], int n, int ugmode) {
  hash_str_init ();
  DIR *D[5];
  int D_ID[5];
  int i;
  for (i = 0; i < n; i++) {
    strcpy (walk_path, P[i]);
    walk_prefix_len = strlen (walk_path);
    walk_rec (D, D_ID, 0, P[i]);
  }
  int m = 0;
  for (i = 0; i < HASH_STR_PRIME; i++) {
    if (HS[i] != NULL) {
      HS[m++] = HS[i];
    }
  }
  double t = -mytime ();
  qsort (HS, m, sizeof (HS[0]), cmp_he);
  t += mytime ();
  vkprintf (1, "Sorting time: %.6lf\n", t);
  for (i = 0; i < m; i++) {
    int part0, part1, user_id = 0, album_id = 0, ok = 1;
    char doc_name[64];
    if (sscanf (HS[i]->key, "/%d/%d/%d/%d/%63s", &part0, &part1, &user_id, &album_id, doc_name) == 5) {
    } else if (sscanf (HS[i]->key, "/%d/%d/%d/%63s", &part0, &part1, &user_id, doc_name) == 4) {
      album_id = 0;
    } else {
      kprintf ("parse error: %s\n", HS[i]->key);
      ok = 0;
    }
    if (part0 + part1 * 100 != user_id % 10000) {
      kprintf ("part0 + part1 * 100 != user_id %% 10000: %s\n", HS[i]->key);
      ok = 0;
    }

    if (ok) {
      user_id *= ugmode;
      unsigned long long secret;
      if (extract_secret (doc_name, &secret)) {
        kprintf ("extract_secret fail %s, use random secret.\n", HS[i]->key);
        secret = make_secret ();
      }
      //int local_id = get_next_local_id (user_id, album_id);
      dyn_mark (NULL);
      int r = import_doc (user_id, album_id, local_id, &secret, HS[i]);
      dyn_release (NULL);
      if (r) {
        kprintf ("import_doc (%d, %d, %d, %llx, %s) returns error code %d\n", user_id, album_id, local_id, secret, doc_name, r);
        continue;
      } else {
        if (dump_rows) {
          fprintf (stderr, "%s%d\t%d\t%d\t%llx\t%s\n", row_prefix, user_id, album_id, local_id, secret, HS[i]->key);
        }
        if (pmemcache_dump) {
          char *p = strchr (HS[i]->key + 1, '/');
          p = strchr (p + 1, '/') + 1;
          assert (p != 0);
          char *q = strrchr (HS[i]->key, '.');
          assert (q != 0);
          char base64url_secret[12];
          secret_to_base64url (secret, base64url_secret);
          fprintf (pmemcache_dump, "/%c%s\t/v%lld/%x/%s%s\n", (ugmode > 0) ? 'u' : 'g', p, volume_id, local_id, base64url_secret, q);
        }
        local_id++;
      }
    }
    zfree (HS[i]->key, strlen (HS[i]->key) + 1);
    zfree (HS[i], sizeof (hash_entry_t));
  }
  return 0;
}

/*
 *
 *		MAIN
 *
 */


void usage (void) {
  printf ("usage: %s [-v] [-V<volume_id>] [-m<split_rem,split_modulo> [-u<username>] [-S] [-p<prefix>] [-g<group_base_path:group_aux_path>] [-L] [-P<dump-file>] user_base_path1:user_aux_path1 ... user_base_pathK:user_aux_pathK\n"
	  "\t\t\tit is possible to give up to 3 aux paths\n"
    "\t-v\toutput statistical and debug information into stderr\n"
	  "\t-p<prefix>\toutput <prefix> before image dumping info\n"
    "\t[-g<group_base_path:group_aux_path>]\timport group images\n"
    "\t-L\tdump import log to stderr\n"
    "\t-T\tdon't write lev_start to the stdout\n"
    "\t-S\ttry to use old secret (by default secrets xored with md5)\n"
    "\t-P<dump-file>\tdumps import log into tab separated format file which could be tranformed with pmemcached-import-dump into pmemcache binlog\n"
    "\t\t\tfirst column old image url, second column new image url\n",
	  progname);
  exit(2);
}

static void test_dir_exist (const char *const path) {
  if (strlen (path) > (PATH_BUFFSIZE - 256)) {
    kprintf ("%s too long\n", path);
    exit (1);
  }
  struct stat buf;
  if (stat (path, &buf) || !S_ISDIR (buf.st_mode)) {
    kprintf ("Directory %s doesn't exist\n", path);
    exit (1);
  }
}

static int get_paths (const char *paths, char *P[MAX_DIRS]) {
  char *s = strdup (paths), *p;
  int n = 0;
  for (p = strtok (s, ":"); p != NULL; p = strtok (NULL, ":")) {
    if (n >= MAX_DIRS) {
      free (s);
      return -1;
    }
    P[n++] = p;
  }
  return n;
}

static void test_paths (char *paths) {
  char *P[MAX_DIRS];
  int  n = get_paths (paths, P);
  if (n >= 0) {
    int i;
    for (i = 0; i < n; i++) {
      test_dir_exist (P[i]);
    }
    free (P[0]);
  }
}

static void import_paths (char *paths, int ugmode) {
  char *P[MAX_DIRS];
  int n = get_paths (paths, P);
  if (n >= 0) {
    import_images (P, n, ugmode);
    free (P[0]);
  }
}

#define MAX_GROUP_PATHS 1024
static char *GP[MAX_GROUP_PATHS];

int main (int argc, char *argv[]) {
  int i;
  int dump_lev_start = 1;
  set_debug_handlers();
  progname = strrchr (argv[0], '/');
  progname = (progname == NULL) ? argv[0] : progname + 1;
  int group_paths = 0;
  while ((i = getopt (argc, argv, "m:vu:Ip:g:V:TP:S")) != -1) {
    switch (i) {
    case 'S':
      use_old_secret = 1;
      break;
    case 'P':
      if (pmemcache_dump) {
        fclose (pmemcache_dump);
      }
      pmemcache_dump = fopen (optarg, "a");
      break;
    case 'T':
      dump_lev_start = 0;
      break;
    case 'V':
      sscanf (optarg, "%lld", &volume_id);
      break;
    case 'm':
      assert (sscanf (optarg, "%d,%d", &split_rem, &split_modulo) == 2);
      assert (split_modulo > 0 && split_modulo <= 1000 && split_rem >= 0 && split_rem < split_modulo);
      break;
    case 'v':
      verbosity++;
      break;
    case 'u':
      username = optarg;
      break;
    case 'I':
      dump_rows = 1;
      break;
    case 'p':
      row_prefix = optarg;
      break;
    case 'g':
      assert (group_paths < MAX_GROUP_PATHS);
      GP[group_paths++] = optarg;
    }
  }

  if (volume_id < 0) {
    usage ();
    kprintf ("volume_id is not given\n");
    exit (1);
  }

  if (!split_modulo) {
    usage ();
    kprintf ("-m option is not given\n");
    exit (1);
  }

  if (10000 % split_modulo) {
    kprintf ("split_modulo should divides 10000\n");
    exit (1);
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  for (i = optind; i < argc; i++) {
    test_paths (argv[i]);
  }
  for (i = 0; i < group_paths; i++) {
    test_paths (GP[i]);
  }

  init_dyn_data ();
  aes_load_pwd_file (NULL);
  if (dump_lev_start) {
    import_lev_start ();
  }

  for (i = optind; i < argc; i++) {
    import_paths (argv[i], 1);
  }
  for (i = 0; i < group_paths; i++) {
    import_paths (GP[i], -1);
  }
/*
  kprintf ("opened_base_doc = %d, opened_aux_doc = %d\n", opened_base_doc, opened_aux_doc);
  kprintf ("successfully_read_base_doc = %d, successfully_read_aux_doc = %d\n", successfully_read_base_doc, successfully_read_aux_doc);
*/
  if (pmemcache_dump) {
    fclose (pmemcache_dump);
  }

  return 0;
}

