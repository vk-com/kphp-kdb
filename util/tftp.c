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

    Copyright 2013 Vkontakte Ltd
              2013 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64
#define _XOPEN_SOURCE 500

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <math.h>
#include <alloca.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stddef.h>
#include <signal.h>
#include <time.h>
#include <sys/wait.h>
#include <netdb.h>
#include <sys/stat.h>

#include "kdb-data-common.h"
#include "net-events.h"
#include "net-msg.h"
#include "server-functions.h"
#include "net-udp.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "kfs.h"
#include "am-stats.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define TFTP_MAX_FILE_SIZE 0x7fffffff
#define TFTP_PORT 69
/*
#define TFTP_ACK_TIMEOUT 3.0
#define TFTP_RRQ_TIMEOUT 10.0
*/
#define TFTP_RESEND_TIMEOUT 3.0
#define TFTP_MAX_RESEND_DATA_PACKET_ATTEMPTS 10
#define TFTP_MAX_RESEND_ACK_PACKET_ATTEMPTS 10
#define TFTP_MAX_RESEND_OACK_PACKET_ATTEMPTS 10
#define TFTP_MAX_RESEND_RRQ_PACKET_ATTEMPTS 10


#define	VERSION_STR	"tftp-1.00"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() (get_utime(CLOCK_MONOTONIC))

static char *hostname, *filename, *mode = "octet", *input_filename, *output_filename, *tmp_filename;
#define	MAX_MSG_BYTES 65536
static char msg_in[MAX_MSG_BYTES+4], msg_out[MAX_MSG_BYTES+4];
static int tftp_process_msg (struct udp_socket *u, struct udp_message *msg);
struct udp_functions ut_tftp_server = {
  .magic = UDP_FUNC_MAGIC,
  .title = "tftp udp server",
  .process_msg = tftp_process_msg
};
static int client_port, server_sfd = -1;
static int blksize, timeout, min_port = 16384, max_port = 65535;
static char *source_dirname;

/********************* MEMCACHED *********************/
conn_type_t ct_tftp_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "tftp_engine_server",
  .accept = accept_new_connections,
  .init_accepted = mcs_init_accepted,
  .run = server_read_write,
  .reader = server_reader,
  .writer = server_writer,
  .parse_execute = mcs_parse_execute,
  .close = server_close_connection,
  .free_buffers = free_connection_buffers,
  .init_outbound = server_failed,
  .connected = server_failed,
  .wakeup = 0,
  .alarm = 0,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = mcs_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

static int stats_port;
static char stats_buff[16384];
static long long tftp_udp_recv_bytes, tftp_udp_sent_bytes;
static long long tftp_udp_recv_packets, tftp_udp_sent_packets;
static long long sent_netascii_files, sent_binary_files;
static long long resent_oack_packets, resent_ack_packets, resent_data_packets, resent_rrq_packets;
static long long recv_rrq_packets, recv_wrq_packets, recv_oack_packets, recv_data_packets, recv_ack_packets, recv_error_packets, recv_bad_format_packets;
static long long ignored_ack_packets;
static long long sent_rrq_packets, sent_ack_packets, sent_data_packets, sent_oack_packets, sent_error_packets;

static int tftp_udp_connections;

int tftp_prepare_stats (struct connection *c);

int memcache_stats (struct connection *c) {
  int len = tftp_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;
  if (key_len >= 5 && !memcmp (key, "stats", 5)) {
    int len = tftp_prepare_stats (c);
    return return_one_key (c, key - dog_len, stats_buff, len);
  }
  return 0;
}

/* RFC 1350: THE TFTP PROTOCOL (REVISION 2) */

enum tftp_opcode {
  tftp_rrq = 1,
  tftp_wrq = 2,
  tftp_data = 3,
  tftp_ack = 4,
  tftp_error = 5,
  tftp_oack = 6,
};

enum tftp_error {
  tftp_undef = 0,
  tftp_file_not_found = 1,
  tftp_access_violation = 2,
  tftp_disk_full = 3,
  tftp_illegal_operation = 4,
  tftp_unknown_transfer_id = 5,
  tftp_file_already_exists = 6,
  tftp_no_such_user = 7,
  tftp_option_negotiation = 8,
};

char *show_opcode (enum tftp_opcode o) {
  switch (o) {
    case tftp_rrq: return "Read request";
    case tftp_wrq: return "Write request";
    case tftp_data: return "Data";
    case tftp_ack: return "Acknowledgement";
    case tftp_error: return "Error";
    case tftp_oack: return "Option Acknowledgement";
  }
  return NULL;
}

char *show_error (enum tftp_error e) {
  switch (e) {
    case tftp_undef: return "Not defined";
    case tftp_file_not_found: return "File not found";
    case tftp_access_violation: return "Access violation";
    case tftp_disk_full: return "Disk full or allocation exceeded";
    case tftp_illegal_operation: return "Illegal TFTP operation";
    case tftp_unknown_transfer_id: return "Unknown transfer ID";
    case tftp_file_already_exists: return "File already exists";
    case tftp_no_such_user: return "No such user";
    case tftp_option_negotiation: return "Option negotiation error";
  }
  return "Bad error code";
}

/******************** Read files for memory functions (unused) ********************/
int tot_files;
#define TFTP_FILE_HASH_PRIME 10007
typedef struct tftp_file {
  long long size;
  char *alias;
  char *filename;
  char *data;
  struct tftp_file *hnext;
} tftp_file_t;
static tftp_file_t *HF[TFTP_FILE_HASH_PRIME];

tftp_file_t *get_file_f (const char *alias, int force) {
  unsigned int h = 0;
  const char *s;
  for (s = alias; *s; s++) {
    h = 239 * h + *s;
  }
  h %= TFTP_FILE_HASH_PRIME;
  assert (h >= 0 && h < TFTP_FILE_HASH_PRIME);
  tftp_file_t **p = HF + h, *V;
  while (*p) {
    V = *p;
    if (!strcmp (V->alias, alias)) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = HF[h];
        HF[h] = V;
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force > 0) {
    tot_files++;
    const int sz = sizeof (tftp_file_t);
    V = ztmalloc0 (sz);
    V->alias = zstrdup (alias);
    V->hnext = HF[h];
    return HF[h] = V;
  }
  return NULL;
}

ssize_t tftp_pread (tftp_file_t *F, void *buf, size_t count, off_t offset) {
  if (offset < 0 || offset > F->size) {
    return -1;
  }
  if (count > F->size - offset) {
    count = F->size - offset;
  }
  memcpy (buf, F->data + offset, count);
  return count;
}

int tftp_config_load (const char *config_filename) {
  char s[16384];
  FILE *f = fopen (config_filename, "r");
  if (f == NULL) {
    kprintf ("fail to open config file '%s'. %m\n", config_filename);
    return -1;
  }
  int line = 0, res = 0, fd = -1;
  while (fgets (s, sizeof (s), f) != NULL) {
    line++;
    int l = strlen (s);
    if (l >= sizeof (s) - 1) {
      kprintf ("config line %d is too long\n", line);
      res--;
      break;
    }
    char *ptr;
    char *alias = strtok_r (s, "\t\n ", &ptr);
    if (alias == NULL) {
      continue;
    }
    char *filename = strtok_r (NULL, "\t\n ", &ptr);
    if (filename == NULL) {
      kprintf ("at the %d line of config filename for alias '%s' wasn't given.\n", line, alias);
      res--;
      break;
    }
    int old_tot_files = tot_files;
    tftp_file_t *F = get_file_f (alias, 1);
    if (old_tot_files == tot_files) {
      kprintf ("%d line of config file contains duplicate record for alias '%s'.\n", line, alias);
      res--;
      break;
    }
    int fd = open (filename, O_RDONLY);
    if (fd < 0) {
      kprintf ("fail to open file '%s' for alias '%s' (config line: %d). %m\n", filename, alias, line);
      res--;
      break;
    }
    struct stat st;
    if (fstat (fd, &st) < 0) {
      kprintf ("fstat for file '%s' for alias '%s' failed (config line: %d). %m\n", filename, alias, line);
      res--;
      break;
    }
    F->size = st.st_size;
    if (F->size > TFTP_MAX_FILE_SIZE) {
      kprintf ("file '%s' for alias '%s' is too big (more than %lld bytes) (config line: %d). %m\n", filename, alias, F->size,  line);
      res--;
      break;
    }
    F->data = zmalloc (F->size);
    F->filename = zstrdup (filename);
    ssize_t bytes_read = read (fd, F->data, F->size);
    if (bytes_read != F->size) {
      if (bytes_read < 0) {
        kprintf ("read from file '%s' for alias '%s' failed. %m\n", filename, alias);
        res--;
        break;
      } else {
        kprintf ("read only %lld bytes of expected %lld bytes from file '%s' for alias '%s'.\n", (long long) bytes_read, F->size, filename, alias);
        res--;
        break;
      }
    }
    assert (!close (fd));
    fd = -1;
  }

  if (fd >= 0) {
    close (fd);
    fd = -1;
  }

  if (f) {
    fclose (f);
  }
  return res;
}

#define SERVER 0
#define CLIENT 1

static int working_mode, work_complete = 0;
static int fd_out = 1;

typedef struct tftp_connection {
  unsigned char source_ipv6[16];
  struct udp_socket u;
  struct {
    long long pos;
    long long size;
    char *name;
    int fd;
  } file;
  struct {
    int rptr;
    int size;
    unsigned char unflushed_char; // 255 - undef
    char iobuff[0];
  } *netascii;
  struct tftp_connection *next, *prev; /* completed connection list */
  unsigned char *iobuff;
  int block_size;
  int iobuff_blocks;
  int iobuff_first_block;
  int iobuff_bytes;
  int timeout;
  int last_block;
  int last_block_len;
  unsigned short tid[2];
  struct {
    event_timer_t timer;
    int our_ip_idx;
    int attempts;
  } resend;
  int complete:1;
  int oack_size:1;
  int netascii_mode:1;
} tftp_connection_t;

typedef struct {
  unsigned char *ipv6;
  char *error_msg;
  enum tftp_opcode opcode;
  enum tftp_error error_code;
  unsigned short source_port;
  unsigned short dest_port;
  union {
    struct {
      char *filename;
      char *mode;
      char *options;
      int blksize;
      int timeout;
      int tsize;
    } rrq;
    struct {
      unsigned short block;
      unsigned short len;
      char *d;
    } data;
    struct {
      unsigned short block;
    } ack;
    struct {
      enum tftp_error code;
      char *msg;
    } error;
  } u;
} tftp_packet_t;

int tftp_fetch_string (char *in, int avail_in) {
  int i = 0;
  while (i < avail_in && in[i]) {
    i++;
  }
  if (i >= avail_in) { return -1; }
  return i + 1;
}

inline int get_ushort (const char *in) {
  const unsigned char *s = (unsigned char *) in;
  return (((unsigned int) s[0]) << 8) | ((unsigned int) s[1]);
}

int tftp_get_response_packet_type (void) {
  return get_ushort (msg_out);
}

int tftp_parse_options (tftp_packet_t *p, char *in, int avail_in) {
  p->u.rrq.blksize = p->u.rrq.timeout = 0;
  p->u.rrq.tsize = -1;
  if (!avail_in) {
    p->u.rrq.options = "";
    return 0;
  }
  p->u.rrq.options = in;
  while (avail_in > 0) {
    if (p->u.rrq.options != in) {
      in[-1] = ',';
    }
    int i;
    char *z[2];
    for (i = 0; i < 2; i++) {
      const int l = tftp_fetch_string (in, avail_in);
      if (l < 0) {
        p->error_code = tftp_option_negotiation;
        p->error_msg = "Format error: option isn't NUL-terminated.";
        return -1;
      }
      z[i] = in;
      in += l;
      avail_in -= l;
    }
    if (!strcmp (z[0], "blksize")) {
      i = atoi (z[1]);
      if (i >= 8 && i <= 65464) {
        p->u.rrq.blksize = i;
      }
    }
    if (!strcmp (z[0], "timeout")) {
      i = atoi (z[1]);
      if (i >= 1 && i <= 255) {
        p->u.rrq.timeout = i;
      }
    }
    if (!strcmp (z[0], "tsize")) {
      i = atoi (z[1]);
      if (i >= 0) {
        p->u.rrq.tsize = i;
      }
    }
    z[1][-1] = '=';
  }
  return 0;
}

int tftp_packet_parse (tftp_packet_t *p, char *in, int avail_in) {
  memset (p, 0, sizeof (*p));
  if (avail_in < 2) {
    p->error_code = tftp_undef;
    p->error_msg = "Format error: opcode expected, but packet is too short";
    return -1;
  }
  p->opcode = get_ushort (in);
  in += 2;
  avail_in -= 2;
  int i, l;
  switch (p->opcode) {
    case tftp_rrq:
    case tftp_wrq:
      l = tftp_fetch_string (in, avail_in);
      if (l < 0) {
        p->error_code = tftp_undef;
        p->error_msg = "Format error: filename expected, but end of packet found.";
        return -1;
      }
      p->u.rrq.filename = in;
      in += l;
      avail_in -= l;
      l = tftp_fetch_string (in, avail_in);
      if (l < 0) {
        p->error_code = tftp_undef;
        p->error_msg = "Format error: mode expected, but end of packet found.";
        return -1;
      }
      p->u.rrq.mode = in;
      for (i = 0; i < l; i++) {
        in[i] = tolower (in[i]);
      }
      in += l;
      avail_in -= l;
      return tftp_parse_options (p, in, avail_in);
    case tftp_data:
      if (avail_in < 2) {
        p->error_code = tftp_undef;
        p->error_msg = "Format error: block expected, but packet is too short";
        return -1;
      }
      p->u.data.block = get_ushort (in);
      in += 2;
      avail_in -= 2;
      p->u.data.d = in;
      p->u.data.len = avail_in;
      return 0;
    case tftp_ack:
      if (avail_in < 2) {
        p->error_code = tftp_undef;
        p->error_msg = "Format error: block expected, but packet is too short";
        return -1;
      }
      p->u.ack.block = get_ushort (in);
      in += 2;
      avail_in -= 2;
      break;
    case tftp_error:
      if (avail_in < 2) {
        p->error_code = tftp_undef;
        p->error_msg = "Format error: error code expected, but packet is too short";
        return -1;
      }
      p->u.error.code = get_ushort (in);
      in += 2;
      avail_in -= 2;
      l = tftp_fetch_string (in, avail_in);
      if (l < 0) {
        p->error_code = tftp_undef;
        p->error_msg = "Format error: error message expected, but end of packet found.";
        return -1;
      }
      p->u.error.msg = in;
      in += l;
      avail_in -= l;
      break;
   case tftp_oack:
     return tftp_parse_options (p, in, avail_in);
   default:
      p->error_code = tftp_illegal_operation;
      return -1;
  }
  if (avail_in) {
    p->error_code = tftp_undef;
    p->error_msg = "Format error: packet contains extra data";
    return -1;
  }
  return 0;
}

static char *store_options (char *wptr, int block_size, int timeout, long long tsize) {
  if (block_size > 0) {
    strcpy (wptr, "blksize");
    wptr += 8;
    wptr += sprintf (wptr, "%d", block_size) + 1;
  }
  if (timeout > 0) {
    strcpy (wptr, "timeout");
    wptr += 8;
    wptr += sprintf (wptr, "%d", timeout) + 1;
  }
  if (tsize >= 0) {
    strcpy (wptr, "tsize");
    wptr += 6;
    wptr += sprintf (wptr, "%lld", tsize) + 1;
  }
  return wptr;
}

static int tftp_store_rrq (void) {
  char *wptr = msg_out;
  int l = 2 + strlen (filename) + strlen (mode) + 1 + 2 * (8 + 11) + 8;
  if (l > sizeof (msg_out)) {
    return -1;
  }
  *wptr++ = (tftp_rrq >> 8) & 0xff;
  *wptr++ = tftp_rrq & 0xff;
  strcpy (wptr, filename);
  wptr += strlen (filename) + 1;
  strcpy (wptr, mode);
  wptr += strlen (mode) + 1;
  wptr = store_options (wptr, blksize, timeout, 0);
  assert (wptr <= msg_out + sizeof (msg_out));
  return wptr - msg_out;
}

static int tftp_store_error (enum tftp_error code, char *msg) {
  vkprintf (2, "%s: %s(%s)\n", __func__, show_error (code), msg);
  if (msg == NULL) {
    msg = show_error (code);
  }
  int l = 2 + 2 + strlen (msg) + 1;
  assert (l <= sizeof (msg_out));
  char *wptr = msg_out;
  *wptr++ = (tftp_error >> 8) & 0xff;
  *wptr++ = tftp_error & 0xff;
  *wptr++ = (code >> 8) & 0xff;
  *wptr++ = code & 0xff;
  strcpy (wptr, msg);
  return l;
}

static tftp_connection_t *C[65536];

static void fd_close (int *fd) {
  if (fd >= 0) {
    epoll_close (*fd);
    close (*fd);
    *fd = -1;
  }
}

static void tftp_conn_close_handles (tftp_connection_t *c) {
  if (c->file.fd >= 0) {
    assert (!close (c->file.fd));
    c->file.fd = -1;
  }
  if (c->file.name) {
    zfree (c->file.name, strlen (c->file.name) + 1);
    c->file.name = NULL;
  }
  if (c->iobuff != NULL) {
    free (c->iobuff);
    c->iobuff = NULL;
  }
  if (c->netascii) {
    free (c->netascii);
    c->netascii = NULL;
  }
  if (c->resend.timer.h_idx) {
    remove_event_timer (&c->resend.timer);
  }
}

tftp_connection_t *get_connection_f (int tid, int force) {
  if (tid < 0 || tid > 0xffff) { return NULL; }
  tftp_connection_t *c = C[tid];
  if (!force) { return c; }
  if (force > 0) {
    tftp_udp_connections++;
    c = zmalloc0 (sizeof (tftp_connection_t));
    c->block_size = 512;
    return C[tid] = c;
  }
  assert (force < 0);
  if (c != NULL) {
    tftp_udp_connections--;
    fd_close (&c->u.fd);
    tftp_conn_close_handles (c);
    zfree (c, sizeof (tftp_connection_t));
    C[tid] = NULL;
  }
  return c;
}

int choose_random_tid (void) {
  const int n = max_port - min_port + 1;
  int tid = lrand48 () % n, steps = n;
  while (--steps >= 0) {
    if (C[min_port + tid] == NULL) {
      return min_port + tid;
    }
    tid = (tid + 1) % n;
  }
  return -1;
}

int tftp_store_ack (int block) {
/*
  if (block > 0xffff) {
    return tftp_store_error (tftp_undef, "Too many blocks");
  }
*/
  assert (sizeof (msg_out) >= 4);
  char *wptr = msg_out;
  *wptr++ = (tftp_ack >> 8) & 0xff;
  *wptr++ = tftp_ack & 0xff;
  *wptr++ = (block >> 8) & 0xff;
  *wptr++ = block & 0xff;
  return 4;
}

static int tftp_init_udp_port (tftp_connection_t *c, int sfd, int port, udp_type_t *type, void *extra, int mode) {
  if (check_udp_functions (type) < 0) {
    return -1;
  }
  assert (sfd > 2 && sfd < MAX_EVENTS);
  int opt = 1;
  if (setsockopt (sfd, IPPROTO_IP, IP_PKTINFO, &opt, sizeof (opt)) < 0) {
    kprintf ("setsockopt for %d failed: %m\n", sfd);
  }
  if ((mode & SM_IPV6) && setsockopt (sfd, IPPROTO_IPV6, IPV6_RECVPKTINFO, &opt, sizeof (opt)) < 0) {
    kprintf ("setsockopt for %d failed: %m\n", sfd);
  }
  c->u.fd = sfd;
  c->u.flags = mode & SM_IPV6 ? U_IPV6 : 0;
  c->u.type = type;
  c->u.extra = extra;
  c->u.ev = Events + sfd;
  c->u.our_port = port;
  epoll_sethandler (sfd, -5, server_receive_udp, &c->u);
  epoll_insert (sfd, EVT_LEVEL | EVT_READ | EVT_SPEC);
  return 0;
}

static int open_udp_server_socket (int port) {
  static struct in_addr settings_addr;
  return server_socket (port, settings_addr, backlog, SM_UDP + enable_ipv6);
}

int tftp_init_listening_connection (int port, int udp_sfd) {
  tftp_connection_t *c = get_connection_f (port, 1);
  c->tid[working_mode] = port;
  if (udp_sfd < 0) {
    udp_sfd = open_udp_server_socket (port);
    if (udp_sfd < 0) {
      return -1;
    }
  }
  assert (!tftp_init_udp_port (c, udp_sfd, port, &ut_tftp_server, 0, enable_ipv6));
  return 0;
}

/******************** Completed connections ********************/
static tftp_connection_t completed_connections = {
  .next = &completed_connections,
  .prev = &completed_connections
};

static void list_insert (tftp_connection_t *u, tftp_connection_t *V, tftp_connection_t *v) {
  u->next = V; V->prev = u;
  v->prev = V; V->next = v;
}
static void tftp_conn_complete (tftp_connection_t *c) {
  if (c->complete) {
    return;
  }
  if (working_mode == SERVER && c->tid[working_mode] == port) {
    return;
  }
  c->complete = 1;
  tftp_conn_close_handles (c);
  list_insert (completed_connections.prev, c, &completed_connections);
}

static void tftp_clean_completed_connections (void) {
  tftp_connection_t *c, *w;
  for (c = completed_connections.next; c != &completed_connections; c = w) {
    w = c->next;
    if (!c->u.send_queue_len) {
      c->prev->next = c->next;
      c->next->prev = c->prev;
      vkprintf (1, "Remove listening connection for port %d.\n", c->tid[working_mode]);
      get_connection_f (c->tid[working_mode], -1);
    }
  }
}

/******************** Resend ********************/
static void tftp_update_resend_timer (tftp_connection_t *c, int (*handler) (event_timer_t *), int attempts, int out_ip_idx) {
  double timeout = c->timeout > 0 ? c->timeout : TFTP_RESEND_TIMEOUT;
  if (c->resend.timer.h_idx) {
    remove_event_timer (&c->resend.timer);
  }
  c->resend.timer.h_idx = 0;
  c->resend.timer.wakeup = handler;
  c->resend.timer.wakeup_time = precise_now + timeout;// * pow (1.5, attempts);
  c->resend.attempts = attempts;
  insert_event_timer (&c->resend.timer);
  c->resend.our_ip_idx = out_ip_idx;
}

/******************** SERVER ********************/
#define TFTP_IOBUFF_SIZE 65536
static int tftp_netascii_getc (tftp_connection_t *c) {
  unsigned char d;
  if (c->netascii->unflushed_char != 255) {
    d = c->netascii->unflushed_char;
    c->netascii->unflushed_char = 255;
    return d;
  }
  if (c->netascii->rptr >= c->netascii->size) {
    ssize_t r = read (c->file.fd, c->netascii->iobuff, TFTP_IOBUFF_SIZE);
    if (r < 0) {
      vkprintf (1, "Fail to read file '%s' from offset %lld. %m\n", c->file.name, c->file.pos);
      return -2;
    }
    c->file.pos += r;
    c->netascii->size = r;
    c->netascii->rptr = 0;
    if (!r) {
      return -1; /* EOF */
    }
  }
  assert (c->netascii->rptr < c->netascii->size);
  d = c->netascii->iobuff[c->netascii->rptr++];
  if (d == '\n') {
    c->netascii->unflushed_char = '\n';
    return '\r';
  }
  if (d == '\r') {
    c->netascii->unflushed_char = 0;
    return '\r';
  }
  return d;
}

static int tftp_block_read (tftp_connection_t *c, int block, void *dest) {
  if (c->iobuff == NULL) {
    c->iobuff_blocks = TFTP_IOBUFF_SIZE / c->block_size;
    c->iobuff_first_block = INT_MIN;
    vkprintf (4, "%s: block_size = %d, io_buff_blocks = %d\n", __func__, c->block_size, c->iobuff_blocks);
    c->iobuff = malloc (c->iobuff_blocks * c->block_size);
    c->file.pos = 0;
    assert (c->iobuff);
    if (c->netascii_mode) {
      c->netascii = malloc (sizeof (*c->netascii) + TFTP_IOBUFF_SIZE);
      assert (c->netascii);
      c->netascii->size = c->netascii->rptr = 0;
      c->netascii->unflushed_char = 255;
    }
  }
  if (!(c->iobuff_first_block <= block && block < c->iobuff_first_block + c->iobuff_blocks)) {
    assert (block == 1 || block == c->iobuff_first_block + c->iobuff_blocks);
    c->iobuff_first_block = block;
    long long offset = ((long long) (block - 1)) * c->block_size;
    int count = c->iobuff_blocks * c->block_size;
    ssize_t r = 0;
    if (c->netascii_mode) {
      int i;
      c->iobuff_bytes = count;
      for (i = 0; i < count; i++) {
        int d = tftp_netascii_getc (c);
        if (d == -1) {
          c->iobuff_bytes = i;
          break;
        }
        if (d < 0) {
          return -1;
        }
        c->iobuff[i] = d;
      }
    } else {
      if (count > c->file.size - offset) {
        count = c->file.size - offset;
      }
      assert (c->file.pos == offset);
      r = read (c->file.fd, c->iobuff, count);
      if (r < 0) {
        vkprintf (1, "Fail to read file '%s' from offset %lld. %m\n", c->file.name, offset);
        return -1;
      }
      c->file.pos += r;
      c->iobuff_bytes = r;
    }
  }
  block -= c->iobuff_first_block;
  assert (block >= 0 && block < c->iobuff_blocks);
  int o = block * c->block_size;
  int l = c->iobuff_bytes - o;
  if (l > c->block_size) {
    l = c->block_size;
  }
  if (l <= 0) {
    return l;
  }
  memcpy (dest, c->iobuff + o, l);
  return l;
}

static int tftp_sent_data_packet (tftp_connection_t *c, int block) {
  //int r = tftp_pread (c->F, msg_out + 4, c->block_size, (block - 1) * c->block_size);
  int r = tftp_block_read (c, block, msg_out + 4);
  if (r < 0) {
    return tftp_store_error (tftp_undef, "IO error: Read failed");
  }
  msg_out[0] = (tftp_data >> 8) & 0xff;
  msg_out[1] = tftp_data & 0xff;
  msg_out[2] = (block >> 8) & 0xff;
  msg_out[3] = block & 0xff;
  c->last_block = block;
  c->last_block_len = r;
  return r + 4;
}

static int tftp_sent_oack_packet (tftp_connection_t *c) {
  msg_out[0] = (tftp_oack >> 8) & 0xff;
  msg_out[1] = tftp_oack & 0xff;
  char *wptr = msg_out + 2;
  wptr = store_options (msg_out + 2, c->block_size, c->timeout, c->oack_size ? c->file.size : -1);
  return wptr - msg_out;
}

static struct udp_message *tftp_udp_message_create (int len) {
  switch (tftp_get_response_packet_type ()) {
  case tftp_rrq:
    sent_rrq_packets++;
    break;
  case tftp_ack:
    sent_ack_packets++;
    break;
  case tftp_data:
    sent_data_packets++;
    break;
  case tftp_oack:
    sent_oack_packets++;
    break;
  case tftp_error:
    sent_error_packets++;
    break;
  }
  tftp_udp_sent_packets++;
  tftp_udp_sent_bytes += len;
  if (verbosity >= 3) {
    hexdump (msg_out, msg_out + len);
  }
  struct udp_message *a = malloc (sizeof (struct udp_message));
  assert (rwm_create (&a->raw, msg_out, len) == len);
  a->next = 0;
  return a;
}

static struct udp_message *tftp_send_message (tftp_connection_t *c, int len) {
  struct udp_message *a = tftp_udp_message_create (len);
  memcpy (a->ipv6, c->source_ipv6, 16);
  a->port = c->tid[1-working_mode];
  if (!a->port) {
    assert (tftp_get_response_packet_type () == tftp_rrq && working_mode == CLIENT);
    a->port = port;
  }
  a->our_ip_idx = c->resend.our_ip_idx;
  udp_queue_message (&c->u, a);
  return a;
}

static int resend_oack_packet (event_timer_t *et) {
  resent_oack_packets++;
  tftp_connection_t *c = (tftp_connection_t *) (((char *) et) - offsetof(tftp_connection_t, resend.timer));
  if (c->resend.attempts >= TFTP_MAX_RESEND_OACK_PACKET_ATTEMPTS) {
    vkprintf (1, "Too many unanswered oack packets from connection [%s]:%d. Close connection.", show_ipv6 (c->source_ipv6), c->tid[1-working_mode]);
    tftp_conn_complete (c);
    return 0;
  }
  tftp_send_message (c, tftp_sent_oack_packet (c));
  vkprintf (1, "Resend oack packet to [%s]:%d.\n", show_ipv6 (c->source_ipv6), c->tid[1-working_mode]);
  return 0;
}

static int resend_data_packet (event_timer_t *et) {
  resent_data_packets++;
  tftp_connection_t *c = (tftp_connection_t *) (((char *) et) - offsetof(tftp_connection_t, resend.timer));
  if (c->resend.attempts >= TFTP_MAX_RESEND_DATA_PACKET_ATTEMPTS) {
    vkprintf (1, "Too many unanswered data packets for block %d from connection [%s]:%d. Close connection.", c->last_block, show_ipv6 (c->source_ipv6), c->tid[1-working_mode]);
    tftp_conn_complete (c);
    return 0;
  }
  tftp_send_message (c, tftp_sent_data_packet (c, c->last_block));
  tftp_update_resend_timer (c, resend_data_packet, c->resend.attempts + 1, c->resend.our_ip_idx);
  vkprintf (1, "Resend data packet for block %d to [%s]:%d.\n", c->last_block, show_ipv6 (c->source_ipv6), c->tid[1-working_mode]);
  return 0;
}

static int process_rrq (tftp_packet_t *p) {
  recv_rrq_packets++;
  vkprintf (1, "Receive RRQ packet from connection [%s]:%d, file:\"%s\", mode:\"%s\", options:\"%s\"\n",
    show_ipv6 (p->ipv6), p->source_port, p->u.rrq.filename, p->u.rrq.mode, p->u.rrq.options);
  if (p->dest_port != port) {
    return tftp_store_error (tftp_undef, "Read request packet wasn't sent to the TFTP port");
  }
  if (working_mode == CLIENT) {
    return tftp_store_error (tftp_undef, "Read request packet was sent to client");
  }
  int tid = choose_random_tid ();
  vkprintf (3, "%s: choose_random_tid returns %d.\n", __func__, tid);
  if (tid < 0) {
    return tftp_store_error (tftp_undef, "Too many connections");
  }
  int mode = -1;
  if (!strcmp (p->u.rrq.mode, "octet")) {
    mode = 1;
  } else if (!strcmp (p->u.rrq.mode, "netascii")) {
    mode = 0;
  } else {
    return tftp_store_error (tftp_undef, "Unimplemented read request mode. Only 'netascii' and 'octet' modes are supported.");
  }
  if (strstr (p->u.rrq.filename, "../")) {
    return tftp_store_error (tftp_undef, "Filename contains forbidden '../' substring.");
  }
  assert (source_dirname);
  int abspath_len = strlen (source_dirname) + 1 + strlen (p->u.rrq.filename) + 1;
  char *abspath = alloca (abspath_len);
  assert (sprintf (abspath, "%s/%s", source_dirname, p->u.rrq.filename) == abspath_len - 1);
  int fd = open (abspath, O_RDONLY);
  if (fd < 0) {
    vkprintf (1, "Fail to open file '%s'. %m\n", abspath);
    return tftp_store_error (tftp_file_not_found, NULL);
  }
  long long file_size = lseek (fd, 0, SEEK_END);
  if (file_size < 0) {
    vkprintf (1, "Seek to the end of the file '%s' failed. %m\n", abspath);
    close (fd);
    return tftp_store_error (tftp_undef, "IO error: lseek failed.");
  }
  if (lseek (fd, 0, SEEK_SET) != 0) {
    vkprintf (1, "Seek to the start of the file '%s' failed. %m\n", abspath);
    close (fd);
    return tftp_store_error (tftp_undef, "IO error: lseek failed.");
  }
  if (tftp_init_listening_connection (tid, -1) < 0) {
    return tftp_store_error (tftp_undef, "cannot open UDP server socket");
  }
  tftp_connection_t *c = get_connection_f (tid, 0);
  c->netascii_mode = (mode == 0) ? 1 : 0;
  c->block_size = p->u.rrq.blksize;
  c->timeout = p->u.rrq.timeout;
  memcpy (c->source_ipv6, p->ipv6, 16);
  p->dest_port = tid;
  c->tid[1-working_mode] = p->source_port;
  c->file.fd = fd;
  c->file.name = zstrdup (p->u.rrq.filename);
  c->file.size = file_size;
  c->last_block = 0;
  c->oack_size = !p->u.rrq.tsize && !c->netascii_mode;
  if (c->block_size || c->timeout || c->oack_size) {
    int res = tftp_sent_oack_packet (c);
    if (!c->block_size) {
      c->block_size = 512;
    }
    c->last_block_len = c->block_size;
    return res;
  }
  if (!c->block_size) {
    c->block_size = 512;
  }
  c->last_block_len = c->block_size;
  return tftp_sent_data_packet (c, 1);
}

static int process_wrq (tftp_packet_t *p) {
  recv_wrq_packets++;
  if (p->dest_port != port) {
    return tftp_store_error (tftp_undef, "Write request packet wasn't sent to the TFTP port");
  }
  if (working_mode == CLIENT) {
    return tftp_store_error (tftp_undef, "Write request packet was sent to client");
  }
  return tftp_store_error (tftp_undef, "Not implemented");
}

static int process_ack (tftp_packet_t *p) {
/*
  if (lrand48 () & 1) {
    vkprintf (3, "Drop ack packet for block %d.\n", p->u.ack.block);
    return 0;
  }
*/
  recv_ack_packets++;
  if (p->dest_port == port) {
    return tftp_store_error (tftp_undef, "Acknowledgement request packet was sent to TFTP port");
  }
  if (working_mode == CLIENT) {
    return tftp_store_error (tftp_undef, "Acknowledgement request packet was sent to client");
  }
  tftp_connection_t *c = get_connection_f (p->dest_port, 0);
  if (c == NULL) {
    vkprintf (3, "%s: get_connection_f (%d, 0) returns NULL.\n", __func__, p->dest_port);
    return tftp_store_error (tftp_unknown_transfer_id, NULL);
  }
  if (c->tid[1-working_mode] != p->source_port) {
    vkprintf (3, "%s: c->tid[%s] = %d, but p->source_port = %d.\n", __func__, ((1-working_mode) == SERVER) ? "server" : "client", c->tid[1-working_mode], p->dest_port);
    return tftp_store_error (tftp_unknown_transfer_id, NULL);
  }
  if (memcmp (c->source_ipv6, p->ipv6, 16)) {
    return tftp_store_error (tftp_undef, "Connection IP isn't matched.");
  }
  if (p->u.ack.block != (unsigned short) c->last_block) {
    vkprintf (3, "%s: Acknowledgement packet block number is %d, but last sent block number is %d.\n",
     __func__, p->u.ack.block, c->last_block);
    ignored_ack_packets++;
    /* skip ack block */
    return -1;
  }
  if (c->last_block_len < c->block_size) {
    vkprintf (1, "%s: Receive acknowledgement packet for last block. Mark connection [%s]:%d as completed.\n",
      __func__, show_ipv6 (p->ipv6), p->source_port);
    if (c->netascii_mode) {
      sent_netascii_files++;
    } else {
      sent_binary_files++;
    }
    tftp_conn_complete (c);
    return 0;
  }
  return tftp_sent_data_packet (c, c->last_block + 1);
}

/******************** CLIENT ********************/
static int resend_rrq_packet (event_timer_t *et) {
  resent_rrq_packets++;
  tftp_connection_t *c = (tftp_connection_t *) (((char *) et) - offsetof(tftp_connection_t, resend.timer));
  if (c->resend.attempts >= TFTP_MAX_RESEND_RRQ_PACKET_ATTEMPTS) {
    kprintf ("Too many unanswered rrq packets.\n");
    work_complete = 2;
  }
  vkprintf (2, "%s: %d attempt.\n", __func__, c->resend.attempts);
  tftp_send_message (c, tftp_store_rrq ());
  tftp_update_resend_timer (c, resend_rrq_packet, c->resend.attempts + 1, c->resend.our_ip_idx);
  return 0;
}

static int process_oack (tftp_packet_t *p) {
/*
  static int drop = 0;
  if (!drop++) {
    vkprintf (3, "Drop oack packet\n");
    return 0;
  }
*/
  recv_oack_packets++;
  vkprintf (2, "Receive OACK packet from connection [%s]:%d, options:\"%s\"\n",
    show_ipv6 (p->ipv6), p->source_port, p->u.rrq.options);
  recv_oack_packets++;
  if (working_mode == SERVER) {
    return tftp_store_error (tftp_undef, "Option acknowledgement packet was sent to server");
  }
  tftp_connection_t *c = get_connection_f (p->dest_port, 0);
  if (c == NULL) {
    vkprintf (3, "%s: get_connection_f (%d, 0) returns NULL.\n", __func__, p->dest_port);
    return tftp_store_error (tftp_unknown_transfer_id, NULL);
  }
  if (!c->tid[1-working_mode]) {
    c->tid[1-working_mode] = p->source_port;
  }
  if (c->tid[1-working_mode] != p->source_port) {
    vkprintf (3, "%s: c->tid[%s] = %d, but p->source_port = %d.\n", __func__, ((1-working_mode) == SERVER) ? "server" : "client", c->tid[1-working_mode], p->dest_port);
    return tftp_store_error (tftp_unknown_transfer_id, NULL);
  }
  if (memcmp (c->source_ipv6, p->ipv6, 16)) {
    return tftp_store_error (tftp_undef, "Connection IP isn't matched.");
  }
  if (c->last_block > 0) {
    return -1;
  }
  if (p->u.rrq.blksize) {
    c->block_size = p->u.rrq.blksize;
  }
  if (p->u.rrq.timeout) {
    c->timeout = p->u.rrq.timeout;
  }
  c->last_block_len = c->block_size;
  return tftp_store_ack (0);
}

static int resend_ack_packet (event_timer_t *et) {
  resent_ack_packets++;
  tftp_connection_t *c = (tftp_connection_t *) (((char *) et) - offsetof(tftp_connection_t, resend.timer));
  if (c->last_block_len < c->block_size) {
    work_complete = 1; /* successful termination */
    return 0;
  }
  if (c->resend.attempts >= TFTP_MAX_RESEND_ACK_PACKET_ATTEMPTS) {
    kprintf ("Too many unanswered acknowledgement packets.\n");
    work_complete = 2;
    return 0;
  }
  tftp_send_message (c, tftp_store_ack (c->last_block));
  tftp_update_resend_timer (c, resend_ack_packet, c->resend.attempts + 1, c->resend.our_ip_idx);
  vkprintf (1, "Resend ack packet for block %d to [%s]:%d.\n", c->last_block, show_ipv6 (c->source_ipv6), c->tid[1-working_mode]);
  return 0;
}

static int process_data (tftp_packet_t *p) {
/*
  static int drop = 0;
  if (!(drop++ & 1)) {
    vkprintf (3, "Drop data packet\n");
    return 0;
  }
*/
  recv_data_packets++;
  if (working_mode == SERVER) {
    return tftp_store_error (tftp_undef, "Data packet was sent to server");
  }
  tftp_connection_t *c = get_connection_f (p->dest_port, 0);
  if (c == NULL) {
    return tftp_store_error (tftp_unknown_transfer_id, NULL);
  }
  if (p->u.data.block == 1 && !c->tid[1-working_mode]) {
    c->tid[1-working_mode] = p->source_port;
  }
  if (c->tid[1-working_mode] != p->source_port) {
    return tftp_store_error (tftp_unknown_transfer_id, NULL);
  }
  if (memcmp (c->source_ipv6, p->ipv6, 16)) {
    return tftp_store_error (tftp_undef, "Connection IP isn't matched.");
  }
  if (p->u.data.block == c->last_block) {
    vkprintf (3, "Receive duplicate data packet for block number %d. Resend acknowledgement packet.\n", p->u.data.block);
    return tftp_store_ack (p->u.data.block);
  }
  if (p->u.data.block != (unsigned short) (c->last_block + 1)) {
    /* skip data block */
    return -1;
  }
  if (p->u.data.len > 0) {
    if (pwrite (fd_out, p->u.data.d, p->u.data.len, (long long) c->block_size * c->last_block) != p->u.data.len) {
      return tftp_store_error (tftp_disk_full, NULL);
    }
  }
  c->last_block++;
  c->last_block_len = p->u.data.len;
  return tftp_store_ack (p->u.data.block);
}

static int process_error (tftp_packet_t *p) {
  recv_error_packets++;
  vkprintf (1, "Receive error packet from [%s]:%d. %s (%s).\n", show_ipv6 (p->ipv6), p->source_port, show_error (p->u.error.code), p->u.error.msg);
  if (working_mode == SERVER) {
    tftp_connection_t *c = get_connection_f (p->dest_port, 0);
    if (c && c->tid[1-working_mode] == p->source_port) {
      vkprintf (1, "Clean listening connection for [%s]:%d.\n", show_ipv6 (p->ipv6), p->source_port);
      tftp_conn_complete (c);
    }
  } else {
    assert (working_mode == CLIENT);
    work_complete = 2; /* termination on failure */
  }
  return 0;
}

/******************** UDP ********************/
static int tftp_process_msg (struct udp_socket *u, struct udp_message *msg) {
  vkprintf (2, "%s: processing udp message from [%s]:%d (%d bytes)\n", __func__, show_ipv6 (msg->ipv6), msg->port, msg->raw.total_bytes);
  tftp_udp_recv_packets++;
  int r = rwm_fetch_data (&msg->raw, msg_in, MAX_MSG_BYTES+4);
  if (verbosity >= 3) {
    kprintf ("%d bytes read (UDP)\n", r);
    hexdump (msg_in, msg_in + r);
  }
  tftp_udp_recv_bytes += r;
  if (r > MAX_MSG_BYTES) {
    vkprintf (1, "message too long, skipping\n");
    return 0;
  }
  tftp_packet_t p;
  int res = -1;
  if (tftp_packet_parse (&p, msg_in, r) < 0) {
    recv_bad_format_packets++;
    p.dest_port = u->our_port;
    res = tftp_store_error (p.error_code, p.error_msg);
    vkprintf (1, "fail to parse %d bytes packet\n", r);
    if (verbosity >= 1 && verbosity < 3) {
      hexdump (msg_in, msg_in + r);
    }
  } else {
    p.source_port = msg->port;
    p.dest_port = u->our_port;
    p.ipv6 = msg->ipv6;
    vkprintf (3, "%s: source port = %d, destination port = %d\n", __func__, p.source_port, p.dest_port);
    switch (p.opcode) {
      case tftp_rrq: res = process_rrq (&p); break;
      case tftp_wrq: res = process_wrq (&p); break;
      case tftp_data: res = process_data (&p); break;
      case tftp_ack: res = process_ack (&p); break;
      case tftp_error: res = process_error (&p); break;
      case tftp_oack: res = process_oack (&p); break;
      default:
        res = tftp_store_error (tftp_illegal_operation, NULL);
    }
  }
  if (res > 0) {
    struct udp_message *a = tftp_udp_message_create (res);
    memcpy (a->ipv6, msg->ipv6, 16);
    a->port = msg->port;
    a->our_ip_idx = msg->our_ip_idx;
    tftp_connection_t *c = get_connection_f (p.dest_port, 0);
    assert (c);
    udp_queue_message (&c->u, a);
    switch (tftp_get_response_packet_type ()) {
      case tftp_ack:
        if (working_mode == CLIENT) {
          tftp_update_resend_timer (c, resend_ack_packet, 0, a->our_ip_idx);
        }
        break;
      case tftp_data:
        if (working_mode == SERVER) {
          tftp_update_resend_timer (c, resend_data_packet, 0, a->our_ip_idx);
        }
        break;
      case tftp_error:
        if (working_mode == SERVER && c->tid[working_mode] != port) {
          tftp_conn_complete (c);
        } else if (working_mode == CLIENT) {
          work_complete = 2;
        }
        break;
      case tftp_oack:
        if (working_mode == SERVER) {
          tftp_update_resend_timer (c, resend_oack_packet, 0, a->our_ip_idx);
        }
        break;
    }
  }
  return 0;
}

void compute_tmp_filename (void) {
  char a[PATH_MAX];
  assert (snprintf (a, PATH_MAX, "%s.tmp.%ld", output_filename, time (NULL)) < PATH_MAX);
  tmp_filename = strdup (a);
}

int tftp_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, sizeof (stats_buff));
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);

  SB_PRINT_I32(tftp_udp_connections);
  SB_PRINT_I64(tftp_udp_recv_packets);
  SB_PRINT_I64(tftp_udp_recv_bytes);
  SB_PRINT_I64(tftp_udp_sent_packets);
  SB_PRINT_I64(tftp_udp_sent_bytes);

  SB_PRINT_I64(sent_netascii_files);
  SB_PRINT_I64(sent_binary_files);

  SB_PRINT_I64 (recv_bad_format_packets);
  SB_PRINT_I64 (recv_rrq_packets);
  SB_PRINT_I64 (recv_wrq_packets);
  SB_PRINT_I64 (recv_data_packets);
  SB_PRINT_I64 (recv_ack_packets);
  SB_PRINT_I64 (ignored_ack_packets);
  SB_PRINT_I64 (recv_error_packets);
  SB_PRINT_I64 (recv_oack_packets);

  if (working_mode == SERVER) {
    SB_PRINT_I64(resent_oack_packets);
    SB_PRINT_I64(resent_data_packets);
  } else {
    SB_PRINT_I64(resent_rrq_packets);
    SB_PRINT_I64(resent_ack_packets);
  }

  SB_PRINT_I64 (sent_rrq_packets);
  SB_PRINT_I64 (sent_data_packets);
  SB_PRINT_I64 (sent_ack_packets);
  SB_PRINT_I64 (sent_error_packets);
  SB_PRINT_I64 (sent_oack_packets);

  sb_printf (&sb, "mode\t%s\n", working_mode == SERVER ? "server" : "client");
  sb_printf (&sb, "version\t%s\n", FullVersionStr);
  return sb.pos;
}

/******************** SIGNALS ********************/
static void reopen_logs (void) {
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2 (fd, 0);
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }
  vkprintf (1, "logs reopened.\n");
}

static void sigint_immediate_handler (const int sig) {
  static const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  static const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  exit (1);
}

static void sigint_handler (const int sig) {
  static const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  pending_signals |= 1 << SIGINT;
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  static const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  pending_signals |= 1 << SIGTERM;
  signal (sig, sigterm_immediate_handler);
}

volatile int sigusr1_cnt = 0;
static void sigusr1_handler (const int sig) {
  static const char message[] = "got SIGUSR1.\n";
  kwrite (2, message, sizeof (message) - (size_t) 1);
  sigusr1_cnt++;
}

static void signals_init (void) {
  set_debug_handlers ();
  struct sigaction sa;
  memset (&sa, 0, sizeof (sa));
  sa.sa_handler = sigint_handler;
  sigemptyset (&sa.sa_mask);
  sigaddset (&sa.sa_mask, SIGTERM);
  sigaction (SIGINT, &sa, NULL);

  sa.sa_handler = sigterm_handler;
  sigemptyset (&sa.sa_mask);
  sigaddset (&sa.sa_mask, SIGINT);
  sigaction (SIGTERM, &sa, NULL);

  sa.sa_handler = SIG_IGN;
  sigaction (SIGPIPE, &sa, NULL);
  sigaction (SIGPOLL, &sa, NULL);
  sigaction (SIGUSR1, &sa, NULL);
  sigaction (SIGUSR2, &sa, NULL);

  sa.sa_handler = sigusr1_handler;
  sigemptyset (&sa.sa_mask);
  sigaction (SIGUSR1, &sa, NULL);

}

static void cron (void) {
  if (working_mode == SERVER) {
    tftp_clean_completed_connections ();
  }
}

static int check_source_dirname (void) {
  if (source_dirname == NULL) {
    return -1;
  }
  struct stat st;
  if (lstat (source_dirname, &st) < 0) {
    kprintf ("lstat for path '%s' failed. %m\n", source_dirname);
    return -1;
  }
  if (!S_ISDIR (st.st_mode)) {
    kprintf ("'%s' isn't directory.\n", source_dirname);
    return -1;
  }
  return 0;
}

static int parse_port_range (char *s) {
  int x, y;
  if (sscanf (s, "%d-%d", &x, &y) != 2) {
    return -1;
  }
  if (x > y) {
    return -1;
  }
  if (x <= PRIVILEGED_TCP_PORTS) {
    fprintf (stderr, "min port number should be greater than PRIVILEGED_TCP_PORTS(%d)\n", PRIVILEGED_TCP_PORTS);
    return -1;
  }
  if (y >= 65536) {
    fprintf (stderr, "max port number should be less than 65536\n");
    return -1;
  }
  min_port = x;
  max_port = y;
  return 0;
}

void usage (void) {
  fprintf (stderr, "usage: %s [-u<username>] [-v] [-d] [-6] [-l<log-name>] (<dir> | <input> <output>)\n%s\n"
    "Trivial file transfer protocol tool.\n"
    "If <dir> is given then tool works as server.\n"
    "Otherwise tool works as client.\n"
    , progname, FullVersionStr);
  parse_usage ();
  exit (2);
}

void start_server (void) {
  int last_cron_time = 0;

  if (daemonize) {
    setsid ();
    reopen_logs ();
  }

  if (working_mode == SERVER) {
    if (tftp_init_listening_connection (port, server_sfd) < 0) {
      kprintf ("Fail to open listening connection on the port %d.\n", port);
      exit (1);
    }
  } else {
    assert (working_mode == CLIENT);
    if (!access (output_filename, 0)) {
      kprintf ("fatal: file %s already exists\n", output_filename);
      exit (1);
    }
    fd_out = open (tmp_filename, O_CREAT | O_WRONLY | O_EXCL, 0640);
    if (fd_out < 0) {
      kprintf ("open(\"%s\", O_CREAT | O_WRONLY | O_EXCL, 0640) fail. %m\n", tmp_filename);
      exit (1);
    }
    client_port = lrand48 () % (65536 - 16384) + 16384;
    char *p = strchr (input_filename, ':');
    if (p == NULL) {
      kprintf ("<input> doesn't contain colon\n");
      usage ();
    }
    int l = p - input_filename;
    hostname = calloc (l + 1, 1);
    memcpy (hostname, input_filename, l);
    filename = strdup (p + 1);
    tftp_init_listening_connection (client_port, -1);
    tftp_connection_t *c = get_connection_f (client_port, 0);
    if (c == NULL) {
      kprintf ("fatal: fail to init listening connection at port %d\n", client_port);
      exit (1);
    }
    struct hostent *h;
    if (!(h = gethostbyname (hostname)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr_list[0]) {
      kprintf ("%s: cannot resolve %s\n", __func__, hostname);
      exit (1);
    }
    int res = tftp_store_rrq ();
    if (res < 0) {
      kprintf ("Filename is too long (read request packet is too big)");
      exit (1);
    }
    c->timeout = timeout;
    struct udp_message *a = tftp_udp_message_create (res);
    unsigned ip = (unsigned) (*((struct in_addr *) h->h_addr_list[0])).s_addr;
    set_4in6 (a->ipv6, ip);
    memcpy (c->source_ipv6, a->ipv6, 16);
    vkprintf (3, "%s: ip = %s\n", __func__, show_ipv6 (a->ipv6));
    a->port = port;
    a->our_ip_idx = lookup_our_ip (C[client_port]->u.our_ipv4); //???
    udp_queue_message (&c->u, a);
    precise_now = get_utime_monotonic ();
    tftp_update_resend_timer (c, resend_rrq_packet, 0, a->our_ip_idx);
  }

  int stats_sfd = -1;
  if (stats_port) {
    static struct in_addr settings_addr;
    stats_sfd = server_socket (stats_port, settings_addr, backlog, 0);
    if (stats_sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", stats_port);
      exit (1);
    }
    init_listening_connection (stats_sfd, &ct_tftp_engine_server, &memcache_methods);
  }

  int i;
  for (i = 0; !work_complete; i++) {
    if (pending_signals & ((1 << SIGINT) | (1 << SIGTERM))) {
      break;
    }
    if (!(i & 1023)) {
      vkprintf (1, "epoll_work()\n");
    }

    if (__sync_fetch_and_and (&sigusr1_cnt, 0)) {
      reopen_logs ();
    }

    epoll_work (37);
   if (last_cron_time != now) {
      last_cron_time = now;
      cron ();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (pending_signals & (1 << SIGTERM)) {
    kprintf ("Terminated by SIGTERM.\n");
  } else if (pending_signals & (1 << SIGINT)) {
    kprintf ("Terminated by SIGINT.\n");
  }

  if (working_mode == CLIENT) {
    if (fsync (fd_out) < 0) {
      kprintf ("fsync for the output file (\"%s\") fail. %m\n", output_filename);
      working_mode = 2;
    }
    close (fd_out);
    if (working_mode == 2) {
      exit (1);
    }
    if (!access (output_filename, 0)) {
      kprintf ("fatal: file %s already exists\n", output_filename);
      exit (1);
    }
    if (rename (tmp_filename, output_filename)) {
      kprintf ("renaming from temporary file '%s' to output file '%s' failed. %m\n", tmp_filename, output_filename);
      exit (1);
    }
  }
}

int f_parse_option (int val) {
  switch (val) {
  case 'B':
    blksize = atoi (optarg);
    assert (blksize >= 8 && blksize <= 65464);
    break;
  case 'P':
    stats_port = atoi (optarg);
    break;
  case 'R':
    if (parse_port_range (optarg) < 0) {
      fprintf (stderr, "Illegal -R %s option\n", optarg);
      usage ();
      break;
    }
  case 'T':
    timeout = atoi (optarg);
    assert (timeout >= 1 && timeout <= 255);
    break;
  default:
    fprintf (stderr, "Unimplemented option '%c' (%d)\n", (char) val, val);
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  signals_init ();
  progname = argv[0];

  remove_parse_option ('r'); //disable replica (no binlogs)
  remove_parse_option ('a');
  remove_parse_option ('B');

  parse_option ("server-port-range", required_argument, 0, 'R', "sets server port's range. Default range is '%d-%d'. Every UDP connection use one port.", min_port, max_port);
  parse_option ("client-blksize", required_argument, 0, 'B', "sets packet block size");
  parse_option ("client-timeout", required_argument, 0, 'T', "sets timeout in seconds");
  parse_option ("stats-port", required_argument, 0, 'P', "sets port for getting stats using memcache queries");
  parse_engine_options_long (argc, argv, f_parse_option);

  if (argc == optind + 1) {
    working_mode = SERVER;
    source_dirname = argv[optind];
  } else if (argc == optind + 2) {
    input_filename = argv[optind];
    output_filename = argv[optind+1];
    compute_tmp_filename ();
    working_mode = CLIENT;
  } else {
    usage ();
  }

  init_epoll ();

  if (!port) {
    port = TFTP_PORT;
  }

  if (working_mode == SERVER) {
    server_sfd = open_udp_server_socket (port);
    if (server_sfd < 0) {
      kprintf ("Fail to open UDP server socket on the port %d.\n", port);
      exit (1);
    }
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  aes_load_pwd_file (0); //srand48

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_dyn_data ();
  if (working_mode == SERVER) {
    if (check_source_dirname () < 0) {
      exit (1);
    }
  }
  init_msg_buffers (0);
  start_time = time (NULL);
  start_server ();
  return 0;
}
