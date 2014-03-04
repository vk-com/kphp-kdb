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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stddef.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <limits.h>
#include <signal.h>
#include <ctype.h>
#include <zlib.h>
#include <openssl/sha.h>

#include "kfs.h"
#include "crc32.h"
#include "server-functions.h"
#include "kdb-data-common.h"
#include "kdb-copyexec-binlog.h"
#include "copyexec-data.h"
#include "net-crypto-rsa.h"

#define	VERSION_STR	"copyexec-commit-0.01-r1"
#define mytime() (get_utime(CLOCK_MONOTONIC))

#ifndef COMMIT
#define COMMIT "unknown"
#endif

const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

static int compress_level = 9;
static int transaction_buffer_size = 256 << 20;
static int transaction_key = -1;
static char *private_key_prefix = NULL;
static int check_rsa_encryption = 1;
static unsigned committed_transaction_size;
static char *binlog_name;
#define MAX_LINE_LENGTH 16384

/*************************************************** Transaction Buffer code ***********************************************************/

struct transaction_buffer {
  unsigned char *buff;
  long long size;
  long long p;
};
static struct transaction_buffer T;

static void tb_init (struct transaction_buffer *B, long long size) {
  B->buff = malloc (size);
  if (!B->buff) {
    kprintf ("Not enough memory to allocate transaction buffer (%lld bytes size).\n", size);
    exit (1);
  }
  B->size = size;
  B->p = 0;
}

static void *tb_alloc_logevent (struct transaction_buffer *B, lev_type_t type, long long size) {
  size = (size + 3) & -4;
  if (B->p + size > B->size) {
    vkprintf (3, "B->p + size = %lld, B->size = %lld\n", B->p + size, B->size);
    kprintf ("Transaction buffer too small (%lld), try increase it with -M command line switch.\n", B->size);
    exit (1);
  }
  void *ptr = B->buff + B->p;
  B->p += size;
  memset (ptr, 0, size);
  struct lev_generic *E = ptr;
  E->type = type;
  return ptr;
}

/* computes transaction's crc64 */
static void tb_close (struct transaction_buffer *B) {
  unsigned char transaction_sha1[20];
  SHA1 (B->buff, B->p, transaction_sha1);
  struct lev_copyexec_aux_transaction_footer *E = tb_alloc_logevent (B, LEV_COPYEXEC_AUX_TRANSACTION_FOOTER, sizeof (struct lev_copyexec_aux_transaction_footer));
  memcpy (E->sha1, transaction_sha1, 20);
}

static void tb_free (struct transaction_buffer *B) {
  if (B->buff) {
    free (B->buff);
    B->buff = NULL;
  }
}

/******************** string hash ********************/

typedef struct {
  char *key;
} hash_entry_t;

#define HASH_STR_PRIME 10007
#define MAX_STR ((int) (0.8 * HASH_STR_PRIME))
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
    HS[h1] = calloc (1, sizeof (hash_entry_t));
    assert (HS[h1]);
    HS[h1]->key = strdup (s);
    return HS[h1];
  }
  return NULL;
}

/************************************ Actions parsing code *************************************************************/

char *parse_int (char *p, int line, const char *const actions_filename, int *r) {
  errno = 0;
  long long l;
  if (!strncmp (p, "0x", 2)) {
    l = strtoll (p, &p, 16);
  } else if (!strncmp (p, "0", 1)) {
    l = strtoll (p, &p, 8);
  } else {
    l = strtoll (p, &p, 10);
  }
  if (errno != 0) {
    kprintf ("strtoll failed during parsing int at line %d in %s.\n", line, actions_filename);
    exit (1);
  }
  if (l < INT_MIN || l > INT_MAX) {
    kprintf ("signed 32 bit integer is out of range at line %d in %s.\n", line, actions_filename);
    exit (1);
  }
  *r = l;
  return p;
}

static inline int skip_blanks (char *b, int i) {
  while (b[i] && isspace (b[i])) {
    i++;
  }
  return i;
}

static void gzip_buff (unsigned char *a, int n, unsigned char **b, int *m) {
  *b = NULL;
  if (compress_level <= 0) {
    kprintf ("compress_level should be strictly positive.\n");
    exit (1);
  }
  uLongf dest = compressBound (n);
  *b = malloc (dest);
  int r = compress2 (*b, &dest, a, n, compress_level);
  if (r != Z_OK) {
    kprintf ("compress2 returns error code %d.\n", r);
    exit (1);
  }
  if (dest > INT_MAX) {
    kprintf ("compressed data is greater than 2G\n");
    exit (1);
  }
  *m = dest;
}

static char *transaction_dirname = NULL;

static int create_temporary_file (char *filename, const char *const actions_filename, int line) {
  char *z = strdup (filename), *p, *argv[2];
  if (z[0] == '"') {
    p = strchr (z + 1, '"');
    if (p == NULL) {
      kprintf ("Closing quote isn't found in %s\n", filename);
      free (z);
      return -1;
    }
    argv[0] = z + 1;
    *p++ = 0;
  } else {
    argv[0] = z;
    p = z;
    while (*p && !isspace (*p)) {
      p++;
    }
    if (*p) {
      *p++ = 0;
    }
  }
  while (*p && isspace (*p)) {
    p++;
  }
  if (*p) {
    argv[1] = p;
  } else {
    p = strrchr (argv[0], '/');
    if (p == NULL) {
      argv[1] = argv[0];
    } else {
      argv[1] = p + 1;
    }
  }

  char a[PATH_MAX];
  if (strchr (argv[0], '/') == NULL) {
    assert (snprintf (a, PATH_MAX, "%s/%s", transaction_dirname, argv[0]) < PATH_MAX);
  } else {
    assert (snprintf (a, PATH_MAX, "%s", argv[0]) < PATH_MAX);
  }

  vkprintf (3, "src: %s\n", a);
  vkprintf (3, "dst: %s\n", argv[1]);

  if (strchr (argv[1], '/')) {
    kprintf ("dst_filename contains slash\n");
    kprintf ("src_filename: %s\n", a);
    kprintf ("dst_filename: %s\n", argv[1]);
    free (z);
    return -2;
  }

  int fd = open (a, O_RDONLY);
  if (fd < 0) {
    kprintf ("open (%s, O_RDONLY) failed. %m\n", a);
    free (z);
    exit (1);
  }
  struct stat st;
  if (fstat (fd, &st) < 0) {
    kprintf ("fstat for %s failed. %m\n", a);
    free (z);
    close (fd);
    exit (1);
  }

  long long s = st.st_size;
  unsigned char *b = malloc (s > 0 ? s : 1);
  assert (b);
  if (s > 0 && read (fd, b, s) != s) {
    kprintf ("reading %s failed. %m\n", a);
    free (z);
    close (fd);
    exit (1);
  }
  unsigned b_crc32 = compute_crc32 (b, s);
  assert (close (fd) >= 0);
  int n;
  unsigned char *c;
  gzip_buff (b, s, &c, &n);
  assert (c != NULL);
  free (b);
  char *local_filename = argv[1];
  if (get_str_f (local_filename, 0)) {
    kprintf ("%s, line %d: illegal cp command, duplicate local filename (%s).\n", actions_filename, line, local_filename);
    exit (1);
  }
  get_str_f (local_filename, 1);

  s = sizeof (struct lev_copyexec_aux_transaction_cmd_file) + strlen (local_filename) + n;
  struct lev_copyexec_aux_transaction_cmd_file *E = tb_alloc_logevent (&T, LEV_COPYEXEC_AUX_TRANSACTION_CMD_FILE, s);
  E->mode = st.st_mode;
  E->uid = st.st_uid;
  E->gid = st.st_gid;
  E->actime = st.st_atime;
  E->modtime = st.st_mtime;
  E->compressed_size = n;
  E->size = st.st_size;
  E->compressed_crc32 = compute_crc32 (c, n);
  E->crc32 = b_crc32;
  E->filename_size = strlen (local_filename);
  memcpy (E->data, local_filename, E->filename_size);
  if (n > 0) {
    memcpy (E->data + E->filename_size, c, n);
  }

  free (c);
  free (z);
  return 0;
}

static int kill_transaction (int signal, int transaction_id) {
  if (transaction_id < 1) {
    kprintf ("kill_transaction: X < 1\n");
    return -1;
  }
  if (transaction_id >= transactions + 1) {
    kprintf ("kill_transaction: X >= current transaction id (X = %d, transaction_id = %d)\n", transaction_id, transactions + 1);
    return -2;
  }
  if (signal != SIGINT && signal != SIGTERM && signal != SIGKILL) {
    kprintf ("kill_transaction: Unknown signal: %d\n", signal);
    return -3;
  }
  struct lev_copyexec_aux_transaction_cmd_kill *E = tb_alloc_logevent (&T, LEV_COPYEXEC_AUX_TRANSACTION_CMD_KILL, sizeof (struct lev_copyexec_aux_transaction_cmd_kill));
  E->signal = signal;
  E->transaction_id = transaction_id;
  return 0;
}

static int wait_transaction (int transaction_id) {
  if (transaction_id < 1) {
    kprintf ("wait_transaction: X < 1\n");
    return -1;
  }
  if (transaction_id >= transactions + 1) {
    kprintf ("wait_transaction: X >= current transaction id (X = %d, transaction_id = %d)\n", transaction_id, transactions + 1);
    return -2;
  }
  struct lev_copyexec_aux_transaction_cmd_wait *E = tb_alloc_logevent (&T, LEV_COPYEXEC_AUX_TRANSACTION_CMD_WAIT, sizeof (struct lev_copyexec_aux_transaction_cmd_wait));
  E->transaction_id = transaction_id;
  return 0;
}

static int cancel_transaction (int transaction_id) {
  if (transaction_id < 1) {
    kprintf ("cancel_transaction: X < 1\n");
    return -1;
  }
  if (transaction_id >= transactions + 1) {
    kprintf ("cancel_transaction: X >= current transaction id (X = %d, transaction_id = %d)\n", transaction_id, transactions + 1);
    return -2;
  }
  struct lev_copyexec_aux_transaction_cmd_wait *E = tb_alloc_logevent (&T, LEV_COPYEXEC_AUX_TRANSACTION_CMD_CANCEL, sizeof (struct lev_copyexec_aux_transaction_cmd_wait));
  E->transaction_id = transaction_id;
  return 0;
}

static int result_command (int x) {
  struct lev_copyexec_aux_transaction_cmd_result *E = tb_alloc_logevent (&T, LEV_COPYEXEC_AUX_TRANSACTION_CMD_RESULT, sizeof (struct lev_copyexec_aux_transaction_cmd_result));
  E->result = x;
  return 0;
}

static int exec_command (const char *cmd, lev_type_t type) {
  char *suffix = "";
  if (type == LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC_CHECK) {
    suffix = "_check";
  } else if (type == LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC_RESULT) {
    suffix = "_result";
  }
  vkprintf (2, "exec%s %s\n", suffix, cmd);
  if (!(*cmd)) {
    kprintf ("exec_command: empty filename\n");
    return -1;
  }
  struct lev_copyexec_aux_transaction_cmd_exec *E = tb_alloc_logevent (&T, type, sizeof (struct lev_copyexec_aux_transaction_cmd_exec) + strlen (cmd));
  E->command_size = strlen (cmd);
  memcpy (E->data, cmd, E->command_size);
  return 0;
}

static void parse_actions_file (const char *const actions_filename) {
  int flags = 0;
  char b[MAX_LINE_LENGTH];
  hash_str_init ();

  tb_init (&T, transaction_buffer_size);
  struct lev_copyexec_aux_transaction_header *E = tb_alloc_logevent (&T, LEV_COPYEXEC_AUX_TRANSACTION_HEADER, sizeof (*E));
  FILE *f = fopen (actions_filename, "r");
  if (f == NULL) {
    kprintf ("fopen (%s, \"r\") failed.\n", actions_filename);
    exit (1);
  }
  int line = 0;
  while (1) {
    line++;
    if (fgets (b, MAX_LINE_LENGTH, f) == 0) {
      break;
    }
    int l = strlen (b);
    if (l >= MAX_LINE_LENGTH - 1) {
      kprintf ("Line %d in %s is too long.\n", line, actions_filename);
      exit (1);
    }
    /* trim comments */
    char *p = strchr (b, '#');
    if (p != 0) {
      *p = 0;
    }
    p = strstr (b, "//");
    if (p != 0) {
      *p = 0;
    }
    l = strlen (b);
    /* trim trailing blanks */
    while (l > 0 && isspace (b[l-1])) {
      l--;
      b[l] = 0;
    }
    /* skip leading blanks */
    int i = skip_blanks (b, 0);
    if (i >= l) {
      /* ignore blank lines */
      continue;
    }
    p = b + i;
    i++;
    while (i < l && !isspace (b[i])) {
      i++;
    }
    b[i] = 0;
    if (i < l) {
      i = skip_blanks (b, i + 1);
    }
    /* p contains now a command */
    if (!strcmp (p, "mask")) {
      p = parse_int (b + i, line, actions_filename, &E->mask);
      if (!check_mask (E->mask)) {
        kprintf ("%s, line %d: illegal mask\n", actions_filename, line);
        exit (1);
      }
      if (*p) {
        kprintf ("Expected eoln at the line %d, column %d in %s.\n", line, (int) ((p - b) + 1), actions_filename);
        exit (1);
      }
      flags |= 1;
    } else if (!strcmp (p, "key")) {
      p = parse_int (b + i, line, actions_filename, &transaction_key);
      if (*p) {
        kprintf ("Expected eoln at the line %d, column %d in %s.\n", line, (int) ((p - b) + 1), actions_filename);
        exit (1);
      }
      flags |= 2;
    } else if (!strcmp (p, "cp")) {
      int r = create_temporary_file (b + i, actions_filename, line);
      if (r < 0) {
        kprintf ("Line %d in %s: create_temporary_file (\"%s\") returns error code %d.\n", line, actions_filename, b + i, r);
        exit (1);
      }
    } else if (!strcmp (p, "exec")) {
      int r = exec_command (b + i, LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC);
      if (r < 0) {
        kprintf ("%s, line %d: exec (\"%s\") returns error code %d.\n", actions_filename, line, b + i, r);
        exit (1);
      }
    } else if (!strcmp (p, "exec_check")) {
      int r = exec_command (b + i, LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC_CHECK);
      if (r < 0) {
        kprintf ("%s, line %d: exec_check (\"%s\") returns error code %d.\n", actions_filename, line, b + i, r);
        exit (1);
      }
    } else if (!strcmp (p, "exec_result")) {
      int r = exec_command (b + i, LEV_COPYEXEC_AUX_TRANSACTION_CMD_EXEC_RESULT);
      if (r < 0) {
        kprintf ("%s, line %d: exec_result (\"%s\") returns error code %d.\n", actions_filename, line, b + i, r);
        exit (1);
      }
    } else if (!strcmp (p, "result")) {
      int r, X;
      p = parse_int (b + i, line, actions_filename, &X);
      if (*p) {
        kprintf ("%s, line %d: expected eoln (column:%d).\n", actions_filename, line, (int) ((p - b) + 1));
        exit (1);
      }
      r = result_command (X);
      if (r < 0) {
        kprintf ("result_command (%d) returns error code %d.\n", X, r);
        exit (1);
      }
    } else if (!strcmp (p, "wait")) {
      int r, X;
      p = parse_int (b + i, line, actions_filename, &X);
      if (*p) {
        kprintf ("%s, line %d: expected eoln (column:%d).\n", actions_filename, line, (int) ((p - b) + 1));
        exit (1);
      }
      r = wait_transaction (X);
      if (r < 0) {
        kprintf ("wait_transaction (%d) returns error code %d.\n", X, r);
        exit (1);
      }
    } else if (!strcmp (p, "cancel")) {
      int r, X;
      p = parse_int (b + i, line, actions_filename, &X);
      if (*p) {
        kprintf ("%s, line %d: expected eoln (column:%d).\n", actions_filename, line, (int) ((p - b) + 1));
        exit (1);
      }
      r = cancel_transaction (X);
      if (r < 0) {
        kprintf ("cancel_transaction (%d) returns error code %d.\n", X, r);
        exit (1);
      }
    } else if (!strcmp (p, "kill")) {
      int r, signal,X;
      p = parse_int (b + i, line, actions_filename, &signal);
      p = parse_int (p, line, actions_filename, &X);
      if (*p) {
        kprintf ("%s, line %d: expected eoln (column:%d).\n", actions_filename, line, (int) ((p - b) + 1));
        exit (1);
      }
      r = kill_transaction (signal, X);
      if (r < 0) {
        kprintf ("kill_transaction (signal = %d, X = %d) returns error code %d.\n", signal, X, r);
        exit (1);
      }
    } else if (!strcmp (p, "important")) {
      flags |= MASK_IMPORTANT_TRANSACTION;
      if (b[i]) {
        kprintf ("%s, line %d: expected eoln (column:%d).\n", actions_filename, line, i);
        exit (1);
      }
    } else if (!strcmp (p, "waiting")) {
      flags |= MASK_WAITING_TRANSACTION;
      if (b[i]) {
        kprintf ("%s, line %d: expected eoln (column:%d).\n", actions_filename, line, i);
        exit (1);
      }
    } else if (!strcmp (p, "rerun")) {
      flags |= MASK_RERUN_TRANSACTION;
      if (b[i]) {
        kprintf ("%s, line %d: expected eoln (column:%d).\n", actions_filename, line, i);
        exit (1);
      }
    } else if (!strcmp (p, "synchronize")) {
      flags |= 4;
      if (b[i]) {
        kprintf ("%s, line %d: expected eoln (column:%d).\n", actions_filename, line, i);
        exit (1);
      }
      tb_alloc_logevent (&T, LEV_COPYEXEC_AUX_TRANSACTION_SYNCHRONIZE, 4);
    } else {
      kprintf ("%s, line %d: unknown command %s.\n", actions_filename, line, p);
      exit (1);
    }
  }

  if (ferror (f)) {
    kprintf ("after reading %s, ferror returns not zero exit code.\n", actions_filename);
    exit (1);
  }

  if (!(flags & 1)) {
    kprintf ("Transaction mask is not set in %s.\n", actions_filename);
    exit (1);
  }

  if (!(flags & 2)) {
    kprintf ("Transaction public key id is not set in %s.\n", actions_filename);
    exit (1);
  }

  if (flags & 4) {
    kprintf ("synchronization point inserted\n");
    if (T.p != 4 + sizeof (*E)) {
      kprintf ("synchronization point transaction couldn't contain any other command\n");
      exit (1);
    }
  }

  E->mask |= flags & (MASK_RERUN_TRANSACTION | MASK_IMPORTANT_TRANSACTION | MASK_WAITING_TRANSACTION);

  E->transaction_id = transactions + 1;
  E->binlog_pos = aux_log_readto_pos;
  E->time = time (NULL);
  E->size = T.p + 20;

  tb_close (&T);
  const int plain_transaction_len = T.p;
  vkprintf (1, "Transaction size before encryption %d.\n", plain_transaction_len);
  assert (E->size == plain_transaction_len);
  char private_key_name[PATH_MAX];
  assert (snprintf (private_key_name, PATH_MAX, "%s%d", private_key_prefix, transaction_key) < PATH_MAX);
  void *encrypted_transaction;
  int encrypted_transaction_len;
  int r = rsa_encrypt (private_key_name, T.buff, plain_transaction_len, &encrypted_transaction, &encrypted_transaction_len);
  if (r < 0) {
    kprintf ("rsa_encrypt returns error code %d.\n", r);
    exit (1);
  }
  vkprintf (1, "Encrypted transaction size = %d.\n", encrypted_transaction_len);
  void *plain_transaction_buff = NULL;
  if (check_rsa_encryption) {
    plain_transaction_buff = malloc (T.p);
    assert (plain_transaction_buff);
    memcpy (plain_transaction_buff, T.buff, T.p);
  }
  tb_free (&T);
  committed_transaction_size = sizeof (struct lev_copyexec_aux_transaction) + ((encrypted_transaction_len + 3) & -4);
  tb_init (&T, committed_transaction_size);
  struct lev_copyexec_aux_transaction *F = tb_alloc_logevent (&T, LEV_COPYEXEC_AUX_TRANSACTION, sizeof (struct lev_copyexec_aux_transaction));
  F->key_id = transaction_key;
  F->size = encrypted_transaction_len;
  F->crypted_body_crc64 = crc64 (encrypted_transaction, encrypted_transaction_len);
  F->crc32 = compute_crc32 (F, offsetof (struct lev_copyexec_aux_transaction, crc32));
  void *G = tb_alloc_logevent (&T, 0, encrypted_transaction_len);
  assert (T.p == committed_transaction_size);
  memcpy (G, encrypted_transaction, encrypted_transaction_len);

  /* rsa_decrypt modifies input buffer */
  if (check_rsa_encryption) {
    void *c;
    int clen;
    r = rsa_decrypt (private_key_name, 0, encrypted_transaction, encrypted_transaction_len, &c, &clen, 0);
    assert (r >= 0);
    if (clen != plain_transaction_len) {
      vkprintf (1, "clen = %d, plain_transaction_len = %d\n", clen, plain_transaction_len);
      assert (clen == plain_transaction_len);
    }
    assert (!memcmp (plain_transaction_buff, c, plain_transaction_len));
    free (c);
    free (plain_transaction_buff);
  }
  free (encrypted_transaction);

  vkprintf (3, "before writing: aux_log_readto_pos = %lld\n", aux_log_readto_pos);
  vkprintf (3, "committed_transaction_size = %d, encrypted_transaction_size = %d\n", committed_transaction_size, encrypted_transaction_len);
  if (lseek (binlog_fd, 0, SEEK_END) != aux_log_readto_pos) {
    kprintf ("lseek to binlog's end failed. %m\n");
    exit (1);
  }

  if (write (binlog_fd, T.buff, committed_transaction_size) != committed_transaction_size) {
    kprintf ("write %u bytes to binlog failed. %m\n", committed_transaction_size);
    exit (1);
  }

  if (fsync (binlog_fd) < 0) {
    kprintf ("fsync binlog failed. %m\n");
    exit (1);
  }
  tb_free (&T);
  aux_log_readto_pos += committed_transaction_size;
  transactions++;
}

#define COMMIT_LOG_BUFF_SIZE 16384

static void write_commit_log (void) {
  char a[PATH_MAX], b[COMMIT_LOG_BUFF_SIZE];
  assert (snprintf (a, PATH_MAX, "%s/.commit.log", transaction_dirname) < PATH_MAX);
  int fd = open (a, O_CREAT | O_TRUNC | O_WRONLY | O_EXCL, 0640);
  int n = snprintf (b, COMMIT_LOG_BUFF_SIZE,
    "transaction_id\t%d\n"
    "committed_transaction_size\t%u\n"
    "aux_binlog_path\t%s\n",
    transactions,
    committed_transaction_size,
    binlog_name);
  if (n > COMMIT_LOG_BUFF_SIZE - 1) {
    n = COMMIT_LOG_BUFF_SIZE - 1;
  }
  write (fd, b, n);
  close (fd);
}

static void usage_actions (void) {
  fprintf (stderr,
    "Mandatory actions commands:\n"
    "\tkey <key-id>\tset private key-id for transaction encryption.\n"
    "\tmask <mask>\tset transaction mask.\n"
    "Other actions commands:\n"
    "\twaiting\tset waiting flag to the transaction (transaction will wait till previously running transactions ends).\n"
    "\trerun\tset rerun flag (transaction will be rerun in the case of interrupted status).\n"
    "\timportant\tset important flag to the transaction.\n"
    "\tcp <src-filename> [dst-filename]\tcopy filename to temporary transaction directory, if dst-filename isn't given then it will equal to the <src-filename> basename.\n"
    "\t\tin case <src-filename> doesn't contain any slash copyexec-commit copies file from transaction-dir,\n"
    "\t\totherwise copyexec-commit considers <src-filename> as absolute path\n"
    "\t\tin the case when <src-filename> contains blanks, it is possible to enclose it into double quotes.\n"
    "\texec <cmd>\texecute \"/bin/sh -c cmd\" in child process and wait till this command terminates.\n"
    "\texec_check <cmd>\texecute <cmd> and terminate transaction if command returns not zero exit code or was killed by signal.\n"
    "\texec_result <cmd>\texecute <cmd> and set transaction's result by status obtained from waitpid call\n"
    "\twait <transaction_id>\twait till given transaction terminates\n"
    "\tkill <signal> <transaction_id>\tsend signal (2 or 15) to given transaction and wait till it terminates.\n"
    "\tcancel <transaction_id>\tsend signal SIGUSR2 to <transaction_id>, if no command in <transaction_id> hadn't executed, it will be cancelled.\n"
    "\tresult <int>\tset transaction's result by given <int>\n"
    "Inserting synchronization point (example: new apache server)\n"
    "\tsynchronize \"actions\" file should contain only 3 lines with key, mask and synchronize\n"
    );
}
void usage (void) {
  fprintf (stderr, "%s\n", FullVersionStr);
  fprintf (stderr, "Commits transaction to the auxiliary binlog.\n");
  fprintf (stderr,
           "./copyexec-commit -P<private-key-prefix> -T<transaction-dir> [-u<username>] [-v] [-l<log-name>] <binlog>\n"
                   "\t-P<private-key-prefix>\t(mandatory option), full private key name is concatenation of <private-key-prefix> and key_id found in action list file.\n"
                   "\t-T<transaction-dir>\t(mandatory option), set transaction dir, which should contain \"actions\" file and other tmp files\n"
                   "\t-1\tcompress snapshot faster\n"
                   "\t-9\tcompress snapshot better (default)\n"
                   "\t-M<max-transaction-size>\n"
                   "\t-E<volume-id>\tcreates new empty binlog, volume-id is a string.\n"
                   "\t-u<username>\tused only in create empty binlog mode\n"
                   "\t-v\tincrease verbosity level\n\n"
                   );
  usage_actions ();
  exit (2);
}

/************************************** PID-lock file routines **************************************************/
static int pid_fd;
static char *pid_filename;

static int lock_pid_file (const char *const dir) {
  char a[PATH_MAX];
  assert (snprintf (a, PATH_MAX, "%s/%s", dir, ".copyexec-commit.pid") < PATH_MAX);
  pid_fd = open (a, O_CREAT | O_WRONLY | O_EXCL, 0660);
  if (pid_fd < 0) {
    kprintf ("creating %s failed. %m\n", a);
    return -1;
  }
  if (lock_whole_file (pid_fd, F_WRLCK) < 0) {
    kprintf ("lock %s failed. %m\n", a);
    return -2;
  }
  pid_filename = strdup (a);
  int l = snprintf (a, PATH_MAX, "%lld\n", (long long) getpid ());
  assert (l < PATH_MAX);
  assert (write (pid_fd, a, l) == l);
  return 1;
}

static void unlock_pid_file (void) {
  if (pid_fd >= 0) {
    unlink (pid_filename);
    close (pid_fd);
  }
}

static void set_transaction_dirname (char *p) {
  int i = strlen (p);
  if (i == 0) {
    p = ".";
  } else if (p[i-1] == '/') {
    p[i - 1] = 0;
  }
  transaction_dirname = strdup (p);
}

int main (int argc, char *argv[]) {
  check_superuser ();
  max_binlog_size = 1LL << 62;
  int i;
  char *empty_binlog_volume_name = 0;
  set_debug_handlers ();
  while ((i = getopt (argc, argv, "l:hu:v123456789M:E:O:P:T:")) != -1) {
    switch (i) {
    case 'P':
      private_key_prefix = strdup (optarg);
      break;
    case 'T':
      set_transaction_dirname (optarg);
      break;
    case 'E':
      empty_binlog_volume_name = optarg;
      break;
    case '1'...'9':
      compress_level = i - '0';
      break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage ();
      return 2;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    default:
      kprintf ("Unimplemented option %c\n", i);
      exit (1);
      break;
    }
  }

  if (optind + 1 > argc) {
    usage ();
  }

  dynamic_data_buffer_size = (8 << 20);
  init_dyn_data ();
  //aes_load_pwd_file (NULL);

  if (empty_binlog_volume_name) {
    if (change_user (username) < 0) {
      kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
      exit (1);
    }
    make_empty_binlog (argv[optind], empty_binlog_volume_name, COPYEXEC_AUX_SCHEMA_V1, NULL, 0, NULL, 0);
    exit (0);
  }

  if (private_key_prefix == NULL) {
    kprintf ("private key prefix didn't set (use -P option).\n");
    usage ();
  }

  if (transaction_dirname == NULL) {
    kprintf ("transaction's dirname didn't set (use -T option).\n");
    usage ();
  }

  char actions[PATH_MAX];
  assert (snprintf (actions, PATH_MAX, "%s/%s", transaction_dirname, "actions") < PATH_MAX);

  if (lock_pid_file (transaction_dirname) < 0) {
    exit (1);
  }
  atexit (unlock_pid_file);

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;
    vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }
  i = aux_load_index (Snapshot);
  if (i < 0) {
    kprintf ("aux_load_index (%s) returns error code %d.\n", engine_snapshot_name, i);
    exit (1);
  }
  const long long jump_log_pos = aux_log_readto_pos;

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }
  binlog_name = strdup (Binlog->info->filename);

  clear_log ();
  init_log_data (jump_log_pos, 0, 0);

  vkprintf (1, "replaying binlog file %s (size %lld) from position %lld\n", binlogname, Binlog->info->file_size, jump_log_pos);

  fd_aux_binlog = binlog_fd;
  copyexec_aux_replay_binlog (jump_log_pos, NULL);
  assert (append_to_binlog (Binlog) == aux_log_readto_pos);

  signal (SIGINT, SIG_IGN);
  signal (SIGTERM, SIG_IGN);
  signal (SIGHUP, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);

  parse_actions_file (actions);
  aux_save_index (0);
  printf ("%d\n", transactions);
  write_commit_log ();

  return 0;
}

