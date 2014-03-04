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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <signal.h>
#include <netdb.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <zlib.h>

#include "md5.h"
#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "kdb-data-common.h"
#include "net-connections.h"
#include "net-rpc-client.h"
#include "net-crypto-aes.h"
#include "tl-serialize.h"

#include "rpc-const.h"

#define VERSION_STR	"tlclient-0.03-r19"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

/*
 *
 * TL RPC CLIENT
 *
 */

const int GZIP_PACKED = 0x3072cfa1;

static char hostname[256] = "127.0.0.1";
static int expect_input = 0, connected = 0;
static struct tl_compiler compiler;
static struct conn_target *targ;
static char *typename;
static long long req_id = 1;
static struct tl_buffer unserialize_buff;
static FILE *log_file;

static struct {
  int tabsize;
  int indentation;
} params = {
  .tabsize = 2,
  .indentation = 1,
};

static FILE *scheme_filename = NULL;

int rpcc_ready (struct connection *c) {
  connected = expect_input = 1;
  return 0;
}

int rpcc_execute (struct connection *c, int op, int len);

struct rpc_client_functions tl_rpc_client = {
  .execute = rpcc_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcc_flush_packet,
  .rpc_check_perm = rpcc_default_check_perm,
  .rpc_init_crypto = rpcc_init_crypto,
  .rpc_start_crypto = rpcc_start_crypto,
  .rpc_ready = rpcc_ready
};

static struct conn_target default_ct = {
  .min_connections = 1,
  .max_connections = 1,
  .type = &ct_rpc_client,
  .extra = (void *)&tl_rpc_client,
  .port = 11210,
  .reconnect_timeout = 17
};

#define MAX_PACKET_LEN 0x1000000
#define PACKET_BUFFER_SIZE (MAX_PACKET_LEN / 4)
int P[PACKET_BUFFER_SIZE];

static int debug_parse_error_packet_from_file (char *);
static void cleanup (void);
static int expand_utf8 (char *);
static int help (char *);
static int indent (char *);
static int logfile (char *);
static int magic (char *);
static int retrieve_current_scheme (char *);
static int tabsize (char *);
static int quit (char *);
static int unused (char *);

struct command {
  const char *name;
  int (*f) (char *);
  const char *args;
  const char *description;
} CMD[11] = {
  //{ "auth_id", auth_id, "<id>", "set authentication key id"},
  //{ "session_id", session_id, "<id>", "set session id"},
  { "debug_parse_error_packet_from_file", debug_parse_error_packet_from_file, "<filename>", ""} ,
  { "expand_utf8", expand_utf8, "<on/off>", "replace UTF-8 chars by \0x?? in strings" },
  { "help", help, "<function combinator>", "print to stdout types used in given RPC-function" },
  { "indent", indent, "on/off", "enable of disable indentation"},
  { "logfile", logfile, "<filename>", "redirect stdout to the given file"},
  { "magic", magic, "on/off", "enable or disable magics output" },
  { "quit", quit, NULL, "exit from tlclient" },
  { "retrieve_current_scheme", retrieve_current_scheme, "<filename>", "send help.getScheme request and write obtained scheme to given file" },
  { "tabsize", tabsize, "<value>", "set tab size in spaces"},
  { "unused", unused, NULL, "print unused schema types" },
  { NULL, NULL, NULL, NULL }
};

/******************** readline completion ********************/

static char *client_command_generator (const char *text, int state) {
  static int len;
  static int idx;
  if (!state) {
    idx = 0;
    len = strlen (text);
    if (!strncmp ("(", text, len)) {
      return strdup ("(");
    }
  }
  while (1) {
    const char *name = CMD[idx++].name;
    if (name == NULL) {
      break;
    }
    if (!strncmp (name, text, len)) {
      return strdup (name);
    }
  }
  return NULL;
}

static char *tl_command_generator (const char *text, int state) {
  static int len;
  static struct tl_expression *E;
  if (!state) {
    E = compiler.expr[TL_SECTION_FUNCTIONS].next;
    len = strlen (text);
  }
  while (E != &compiler.expr[TL_SECTION_FUNCTIONS]) {
    char *name = E->left->text;
    E = E->next;
    if (!strncmp (name, text, len)) {
      return strdup (name);
    }
  }
  return NULL;
}

static char *expected_type = NULL;

static char *tl_type_generator (const char *text, int state) {
  //kprintf ("%s: text: %s, state:%d\n", __func__, text, state);
  static int len;
  static struct tl_expression *E;
  if (!state) {
    E = tl_expression_find_first_by_composite_typename (&compiler, expected_type);
    len = strlen (text);
  }
  while (E != NULL) {
    char *name = E->left->text;
    E = E->rnext;
    if (!strncmp (name, text, len)) {
      return strdup (name);
    }
  }
  return NULL;
}

static struct tl_expression *expected_expr = NULL;
static char *tl_arg_generator (const char *text, int state) {
  //kprintf ("%s: text: %s, state:%d\n", __func__, text, state);
  static int len;
  static struct tl_token *T = NULL;
  if (!state) {
    T = NULL;
    if (expected_expr) {
      T = expected_expr->left;
      if (T) {
        T = T->next;
      }
    }
    len = strlen (text);
  }
  while (T != NULL) {
    char *name = T->text;
    T = T->next;
    if (!strncmp (name, text, len)) {
      return strdup (name);
    }
  }
  return NULL;
}

struct tl_expression *find_expression (int start) {
  int brackets_cnt = 0, o = start;
  while (o >= 0 && (rl_line_buffer[o] != '(' || brackets_cnt > 0)) {
    if (rl_line_buffer[o] == '(') {
      brackets_cnt--;
    }
    if (rl_line_buffer[o] == ')') {
      brackets_cnt++;
    }
    o--;
  }

  if (o >= 0 && rl_line_buffer[o] == '(') {
    int m = o + 1;
    while (!isspace (rl_line_buffer[m])) {
      m++;
    }
    char *combinator_name = cstr_substr (rl_line_buffer, o + 1, m);
    struct tl_expression *E = tl_list_expressions_find_by_combinator_name (&compiler, o ? TL_SECTION_TYPES : TL_SECTION_FUNCTIONS, combinator_name, NULL);
    cstr_free (&combinator_name);
    return E;
  }
  return NULL;
}

static char **tl_completion (const char *text, int start, int end) {
  if (start == 0) {
    return rl_completion_matches (text, client_command_generator);
  }

  if ( (start == 1 && rl_line_buffer[0] == '(') || (start == 5 && !memcmp (rl_line_buffer, "help ", 5))) {
    return rl_completion_matches (text, tl_command_generator);
  }

  int j = start - 1;
  while (j > 0 && rl_line_buffer[j] != '(' && !isspace (rl_line_buffer[j])) {
    if (rl_line_buffer[j] == ')') {
      j = 0;
    }
    j--;
  }
  if (j > 0 && rl_line_buffer[j] == '(' && rl_line_buffer[j-1] == ':') {
    int i = j - 2;
    while (i > 0 && !isspace (rl_line_buffer[i])) {
      i--;
    }
    struct tl_expression *E = find_expression (i);
    if (E != NULL) {
      char *field_name = cstr_substr (rl_line_buffer, i + 1, j - 1);
      //kprintf ("field_name: %s\n", field_name);
      expected_type = tl_expression_get_argument_type (E, field_name);
      //kprintf ("expected_type: %s\n", expected_type);
      cstr_free (&field_name);
      if (expected_type) {
        return rl_completion_matches (text, tl_type_generator);
      }
    }
  }

  struct tl_expression *E = find_expression (start);
  if (E) {
    expected_expr = E;
    return rl_completion_matches (text, tl_arg_generator);
  }
  return NULL;
}

/* replacing inflateInit by inflateInit2 in uncompress zlib function */
static int gzdecode (Bytef *dest, uLongf *destLen, const Bytef *source, uLong sourceLen) {
  z_stream stream;
  int err;
  stream.next_in = (Bytef*) source;
  stream.avail_in = (uInt) sourceLen;
  /* Check for source > 64K on 16-bit machine: */
  if ((uLong)stream.avail_in != sourceLen) return Z_BUF_ERROR;
  stream.next_out = dest;
  stream.avail_out = (uInt) *destLen;
  if ((uLong)stream.avail_out != *destLen) return Z_BUF_ERROR;
  stream.zalloc = (alloc_func) 0;
  stream.zfree = (free_func) 0;

  err = inflateInit2 (&stream, 15 + 32);
  if (err != Z_OK) {
    return err;
  }

  err = inflate (&stream, Z_FINISH);
  if (err != Z_STREAM_END) {
    inflateEnd (&stream);
    if (err == Z_NEED_DICT || (err == Z_BUF_ERROR && stream.avail_in == 0)) {
      return Z_DATA_ERROR;
    }
    return err;
  }
  *destLen = stream.total_out;

  err = inflateEnd (&stream);
  return err;
}

static int unpack_packet (struct tl_compiler *C, int len, int **Q, int *qlen) {
  *Q = NULL;
  *qlen = 0;
  const int ilen = (len / 4) - 6;
  if (ilen <= 0) {
    return -1;
  }
  if (P[5] != GZIP_PACKED) {
    *Q = &P[5];
    *qlen = ilen;
    return 0;
  }
  char *s;
  int slen;
  int r = tl_fetch_string (&P[6], ilen - 1, &s, &slen, 0);
  vkprintf (4, "ilen: %d, slen: %d\n", ilen, slen);
  if (r < 0) {
    return tl_failf (C, "tl_fetch_string fail");
  }

  if (r != ilen -1) {
    return tl_failf (C, "packet contains extra %d ints",  ilen - 1 - r);
  }

  assert ((void *) (s + slen) <= (void *) &P[len/4]);

  const int dlen = PACKET_BUFFER_SIZE - len;
  uLongf destLen = dlen;
  r = gzdecode ((Bytef *) &P[len / 4], &destLen, (Bytef *) s, slen);
  if (Z_OK != r) {
    return tl_failf (C, "uncompress (%p, %d, %p, %d) returns %s",
      &P[len / 4],
      dlen,
      s,
      slen,
      (r == Z_MEM_ERROR) ? "Z_MEM_ERROR" :
      (r == Z_BUF_ERROR) ? "Z_BUF_ERROR" :
      (r == Z_DATA_ERROR) ? "Z_DATA_ERROR" : "???");
    return -1;
  }

  if (destLen % 4) {
    return tl_failf (C, "destLen(%d) isn't multiple of 4", (int) destLen);
  }

  *Q = &P[len / 4];
  *qlen = destLen / 4;
  return 1;
}

static char *get_raw_scheme (struct tl_scheme_object *O) {
  if (O->type != tlso_list) {
    return NULL;
  }
  struct tl_scheme_object *A = O->u.p.cdr;
  if (A == &obj_empty_list) {
    return NULL;
  }
  assert (A->type == tlso_list);
  struct tl_scheme_object *B = A->u.p.car;
  //tl_scheme_object_dump (stdout, B);
  if (B->type == tlso_str && strcmp (B->u.s, "raw_scheme:")) {
    return NULL;
  }
  struct tl_scheme_object *C = A->u.p.cdr;
  if (C->type != tlso_list) {
    return NULL;
  }
  struct tl_scheme_object *D = C->u.p.car;
  if (D->type != tlso_str) {
    return NULL;
  }
  return D->u.s;
}

static void log_printf (const char *format, ...) __attribute__ ((format (printf, 1, 2)));
static void log_printf (const char *format, ...) {
  va_list ap;
  va_start (ap, format);
  vfprintf (stdout, format, ap);
  va_end (ap);
  if (log_file != NULL) {
    va_start (ap, format);
    vfprintf (log_file, format, ap);
    va_end (ap);
  }
}

static void log_char (char ch) {
  fputc (ch, stdout);
  if (log_file != NULL) {
    fputc (ch, log_file);
  }
}

static void log_str (char *s) {
  while (*s) {
    log_char (*s);
    s++;
  }
}

static void log_errors (void) {
  tl_compiler_print_errors (&compiler, stdout);
  if (log_file) {
    tl_compiler_print_errors (&compiler, log_file);
  }
}

int rpcc_log_error (int len) {
  assert (!(len % 4));
  int code = P[5], ilen = (len / 4) - 7;
  int l;
  char *text;
  assert (ilen > 0);
  int r = tl_fetch_string (&P[6], ilen, &text, &l, 0);
  assert (r == ilen);
  log_printf ("Receive RPC_REQ_ERROR (code: %d, text: \"%.*s\")\n", code, l, text);
  return 0;
}

int rpcc_execute (struct connection *c, int op, int len) {
  if (len > PACKET_BUFFER_SIZE) {
    return SKIP_ALL_BYTES;
  }
  if (op == RPC_REQ_RESULT) {
    long long r;
    assert (read_in (&c->In, P, len) == len);
    memcpy (&r, &P[3], 8);
    if (r != req_id) {
      kprintf ("RPC_REQ_RESULT: req_id = %lld, expected req_id = %lld\n", r, req_id);
      return 0;
    } else {
      int *Q;
      int qlen;
      tl_string_buffer_clear (&unserialize_buff);
      tl_compiler_clear_errors (&compiler);
      if (unpack_packet (&compiler, len, &Q, &qlen) < 0) {
        log_errors ();
        log_str ("unpack packet fail\n");
      } else {
        if (scheme_filename) {
          struct tl_scheme_object *O = NULL;
          tl_compiler_clear_errors (&compiler);
          int r = tl_expression_unserialize (&compiler, Q, qlen, 1 << TL_SECTION_TYPES, typename, &O);
          if (r >= 0 && O != NULL) {
            //tl_scheme_object_sbdump (b, O);
            char *s = get_raw_scheme (O);
            if (s) {
              fprintf (scheme_filename, "%s", s);
            }
            tl_scheme_object_free (O);
          }
          fclose (scheme_filename);
          scheme_filename = NULL;
        } else {
          if (qlen && Q[0] == RPC_REQ_ERROR) {
            int o = 4 * (qlen - 1);
            memcpy (P+5, Q+1, o);
            rpcc_log_error (o + 24);
            expect_input = 1;
            return 0;
          }
          int r = tl_unserialize_rpc_function_result (&compiler, &unserialize_buff, Q, qlen, typename, params.indentation);
          if (r < 0) {
            log_errors ();
          } else {
            if (r != qlen) {
              kprintf ("Warning: unserialize %d ints of result, but result contains %d ints.\n", r, qlen);
            }
            tl_string_buffer_append_char (&unserialize_buff, 0);
            char *s = unserialize_buff.buff;
            while (*s) {
              if (*s == '\t') {
                int k;
                for (k = 0; k < params.tabsize; k++) {
                  log_char (' ');
                }
              } else {
                log_char (*s);
              }
              s++;
            }
            log_char ('\n');
          }
        }
      }
      cstr_free (&typename);
      expect_input = 1;
    }
    return 0;
  } if (op == RPC_REQ_ERROR) {
    long long received_req_id;
    assert (read_in (&c->In, P, len) == len);
    memcpy (&received_req_id, &P[3], 8);
    if (received_req_id != req_id) {
      kprintf ("RPC_REQ_ERROR: req_id = %lld, expected req_id = %lld\n", received_req_id, req_id);
      return 0;
    } else {
      rpcc_log_error (len);
    }
    cstr_free (&typename);
    expect_input = 1;
    return 0;
  } else {
    return SKIP_ALL_BYTES;
  }
}

static struct connection *get_target_connection (struct conn_target *targ) {
  struct connection *c;
  if (!targ->outbound_connections) {
    return 0;
  }
  c = targ->first_conn;
  while (1) {
    if (server_check_ready (c) == cr_ok) {
      return c;
    }
    if (c == targ->last_conn) { break;}
    c = c->next;
  }
  return 0;
}

static unsigned long long auth_key_id = 0x0123456789abcdefULL;
static unsigned long long session_id =  0xfedcba9876543210ULL;
static unsigned long long msg_id = 0;
static int additional = 6;

int rpcc_send_query (struct connection *c, int r) {
  req_id++;
  P[0] = 4 * (r + 6 + additional);
  P[1] = RPCC_DATA(c)->out_packet_num++;
  P[2] = RPC_INVOKE_REQ;
  memcpy (&P[3], &req_id, 8);
  if (additional > 0) {
    memcpy (&P[5], &auth_key_id, 8);
    memcpy (&P[7], &session_id, 8);
    msg_id++;
    memcpy (&P[9], &msg_id, 8);
  }
  P[(P[0]/4)-1] = compute_crc32 (P, P[0] - 4);
  write_out (&c->Out, P, P[0]);
  RPCC_FUNC(c)->flush_packet(c);
  if (verbosity >= 4) {
    fprintf (stderr, "Sending:\n");
    dump_rpc_packet (P);
  }
  expect_input = 0;
  return 0;
}

static void open_log (void) {
  int fd;
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2 (fd, 2);
    if (fd > 2) {
      close (fd);
    }
  }
}

int send_rpc_command (char *cmd) {
  tl_compiler_clear_errors (&compiler);
  int r = tl_serialize_rpc_function_call (&compiler, cmd, &P[5 + additional], PACKET_BUFFER_SIZE - 6 - additional, &typename);
  if (r < 0) {
    log_str ("tl_serialize_rpc_function_call fail. Errors:\n");
    log_errors ();
    return r;
  } else {
    struct connection *c = get_target_connection (targ);
    if (c == NULL) {
      kprintf ("get_target_connection returns NULL.\n");
      exit (1);
    }
    rpcc_send_query (c, r);
  }
  return 0;
}

/******************** commands ********************/

int retrieve_current_scheme (char *s) {
  scheme_filename = fopen (s, "w");
  if (scheme_filename == NULL) {
    log_printf ("Couln't open \"%s\" for writing. %m\n", s);
    return -1;
  }

  if (send_rpc_command ("(help.getScheme 1)") < 0) {
    return -1;
  }
  return 0;
}

int quit (char *s) {
  cleanup ();
  exit (0);
}

int help (char *s) {
  return tl_function_help (&compiler, s, stdout);
}

static int parse_bool (char *s) {
  if (!strcmp (s, "on")) {
    return 1;
  }
  if (!strcmp (s, "off")) {
    return 0;
  }
  return -1;
}

static int rpc_read_packet_from_file (const char *filename) {
  FILE *f = fopen (filename, "r");
  if (f == NULL) {
    kprintf ("fopen (\"%s\", \"r\") failed. %m\n", filename);
    return -1;
  }
  int r = 0;
  while (r < BUFF_SIZE && fscanf (f, "%x", P+r) == 1) {
    r++;
  }
  fclose (f);
  if (r == BUFF_SIZE) {
    kprintf ("RPC packet is too big.\n");
    return -1;
  }
  return r;
}

int debug_parse_error_packet_from_file (char *s) {
  int r = rpc_read_packet_from_file (s);
  if (r < 0) {
    return r;
  }
  return rpcc_log_error (4 * r);
}

int expand_utf8 (char *s) {
  int b = parse_bool (s);
  if (b == 1) {
    log_str ("Enable expanding UTF-8 characters in strings.\n");
    tl_set_expand_utf8 (1);
  } else if (b == 0) {
    log_str ("Disable expanding UTF-8 characters in strings.\n");
    tl_set_expand_utf8 (0);
  }
  return 0;
}

int magic (char *s) {
  int b = parse_bool (s);
  if (b == 1) {
    log_str ("Output magics is enabled.\n");
    compiler.flag_output_magic = 1;
  } else if (b == 0) {
    log_str ("Output magics is disabled.\n");
    compiler.flag_output_magic = 0;
  }
  return 0;
}

int unused (char *s) {
  int r = tl_schema_print_unused_types (&compiler, stdout);
  if (log_file) {
    tl_schema_print_unused_types (&compiler, log_file);
  }
  return r;
}

int tabsize (char *s) {
  int i;
  if (sscanf (s, "%d", &i) == 1) {
    if (i > 8) {
      i = 8;
    }
    if (i < 2) {
      i = 2;
    }
    log_printf ("Set tabsize to %d\n", i);
    params.tabsize = i;
  }
  return 0;
}

int indent (char *s) {
  int b = parse_bool (s);
  if (b == 1) {
    log_str ("Indentation is enabled.\n");
    params.indentation = 1;
  } else if (b == 0) {
    log_str ("Indentation is disabled.\n");
    params.indentation = 0;
  }
  return 0;
}

int logfile (char *s) {
  if (log_file) {
    fclose (log_file);
    log_file = NULL;
  }
  log_file = fopen (s, "w");
  if (log_file == NULL) {
    kprintf ("fopen (\"%s\", \"w\") fail. %m\n", s);
  }
  return 0;
}

int exec_command (char *input) {
  char *p = strchr (input, ' ');
  if (p == NULL) {
    p = input + strlen (input);
  }
  int l = p - input;
  while (*p && isspace (*p)) {
    p++;
  }
  int k;
  for (k = 0; CMD[k].name; k++) {
    if (!strncmp (CMD[k].name, input, l)) {
      return CMD[k].f (p);
    }
  }
  return -1;
}

static char history_filename[PATH_MAX];

void read_rc_file (void) {
  memset (history_filename, 0, sizeof (history_filename));
  char *home = getenv ("HOME");
  if (home == NULL) {
    kprintf ("HOME environment variable isn't set, fail to find ~/tlclientrc file.\n");
    return;
  }
  char path[PATH_MAX];
  assert (snprintf (path, PATH_MAX, "%s/.tlclientrc", home) < PATH_MAX);
  assert (snprintf (history_filename, PATH_MAX, "%s/.tlclient_history", home) < PATH_MAX);
  FILE *f = fopen (path, "r");
  if (f == NULL) {
    f = fopen (path, "w");
    if (f == NULL) {
      kprintf ("fail to create default %s\n", path);
      return;
    }
    kprintf ("%s doesn't exist, create default file\n", path);
    fprintf (f, "tabsize %d\n"
                "indent %s\n"
                "magic on\n"
                "expand_utf8 off\n",
      params.tabsize, params.indentation ? "on" : "off");
    fclose (f);
    return;
  }
  char input[4096];
  int l = sizeof (input), line = 0;
  while (fgets (input, l, f) != NULL) {
    int i;
    line++;
    for (i = 0; i < l && input[i] && input[i] != '\n'; i++) {}
    if (i == l) {
      kprintf ("%s: line %d is too long\n", path, line);
      exit (1);
    }
    input[i] = 0;
    exec_command (input);
  }
  fclose (f);
  read_history (history_filename);
}

/******************** signal handlers ********************/

static void sigint_immediate_handler (const int sig) {
  static const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  static const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  exit (1);
}

static void sigint_handler (const int sig) {
  static const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  pending_signals |= 1 << sig;
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  static const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  pending_signals |= 1 << sig;
  signal (sig, sigterm_immediate_handler);
}

static void cron (void) {
  create_all_outbound_connections ();
}

static int sfd;

void cleanup (void) {
  if (history_filename[0]) {
    write_history (history_filename);
  }
  epoll_close (sfd);
  close (sfd);

  if (log_file) {
    fclose (log_file);
    log_file = NULL;
  }
}

void start_server (void) {
  int i;
  int prev_time;

  init_epoll ();
  init_netbuffers ();

  prev_time = 0;

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  create_all_outbound_connections ();

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);

  for (i = 0; !pending_signals; i++) {
    if (!(i & 255)) {
      vkprintf (1, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }

    epoll_work (97);

    if (now != prev_time) {
      prev_time = now;
      cron ();
      if (!connected && now - start_time > 10) {
        kprintf ("Didn't connect to server after 10 seconds.\n");
        exit (1);
      }
    }

    if (expect_input) {
      vkprintf (4, "Before readline call.\n");
      char *input = readline ("TL>");
      if (input[0]) {
        int l;
        if (log_file) {
          fprintf (log_file, "TL>%s\n", input);
        }
        add_history (input);
        for (l = strlen (input) - 1; l > 0 && input[l] == ' '; l--) {
          input[l] = 0;
        }

        if (input[0] == '(') {
          send_rpc_command (input);
        } else {
          exec_command (input);
        }
      }
      if (input != NULL) {
        free (input);
      }
    }
    if (quit_steps && !--quit_steps) break;
  }

  cleanup ();

}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  int k;
  printf ("%s\n", FullVersionStr);
  printf ("usage: tlclient [-v] [-r] [-a<hostname>] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-l<log-name>] <scheme.tl>\n"
	  "\tParsing SCHEME expressions, sending RPC requests to Kitten PHP, unserialize obtained result.\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
    "\t-A<auth_key_id>\tauth_key_id as unsigned long long hex number\n"
    "\t\tif auth_key_id is zero (auth_key_id, session_id, message_id will not sent after req_id)\n"
    "\t-S<session_id>\tsession_id as unsigned long long hex number\n"
  );

  printf ("\ntlclient commands:\n");
  for (k = 0; CMD[k].name; k++) {
    printf ("%s %s\t%s\n", CMD[k].name, CMD[k].args ? CMD[k].args : "", CMD[k].description);
  }
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  set_debug_handlers ();
  progname = argv[0];
  daemonize = 0;
  errno = 0;
  while ((i = getopt (argc, argv, "A:S:a:b:c:l:p:dhu:v")) != -1) {
    switch (i) {
    case 'A':
      sscanf (optarg, "%16llx", &auth_key_id);
      break;
    case 'S':
      sscanf (optarg, "%16llx", &session_id);
      break;
    case 'a':
      strncpy (hostname, optarg, 255);
      break;
    case 'b':
      backlog = atoi(optarg);
      if (backlog <= 0) backlog = BACKLOG;
      break;
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
        maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'h':
      usage();
      return 2;
    case 'l':
      logname = optarg;
      break;
    case 'p':
      default_ct.port = port = atoi(optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    }
  }

  if (argc != optind + 1) {
    usage ();
  }

  if (auth_key_id == 0) {
    additional = 0;
  }

  vkprintf (4, "auth_key_id: 0x%llx, session_id: 0x%llx\n", auth_key_id, session_id);

  init_dyn_data ();

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  aes_load_pwd_file (0);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  open_log ();

  start_time = time (0);

  struct hostent *h;
  if (!(h = gethostbyname (hostname)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    kprintf ("cannot resolve %s\n", hostname);
    exit (2);
  }
  default_ct.target = *((struct in_addr *) h->h_addr);

  targ = create_target (&default_ct, 0);

  if (tl_schema_read_from_file (&compiler, argv[optind]) < 0) {
    kprintf ("Error in \"%s\"\n", argv[optind]);
    tl_compiler_print_errors (&compiler, stderr);
    exit (1);
  }

  rl_attempted_completion_function = tl_completion;

  tl_string_buffer_init (&unserialize_buff);

  read_rc_file ();

  start_server ();

  return 0;
}
