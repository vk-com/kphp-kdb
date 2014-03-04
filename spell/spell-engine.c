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

    Copyright 2011 Vkontakte Ltd
              2011 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>

#include "net-events.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "spell-data.h"
#include "kdb-data-common.h"
#include "am-stats.h"

#define	MAX_KEY_LEN	1000
#define	MAX_VALUE_LEN	(1 << 20)

#define	VERSION "1.02"
#define	VERSION_STR "spell-engine "VERSION

#define TCP_PORT 11211
#define UDP_PORT 11211

#define MAX_NET_RES	(1L << 16)

#define mytime() get_utime (CLOCK_MONOTONIC)

#define MAX_CHILD_PROCESS 32

/*
 *
 *		MEMCACHED PORT
 *
 */


int mcp_get (struct connection *c, const char *key, int key_len);
int mcp_wakeup (struct connection *c);
int mcp_alarm (struct connection *c);
int mcp_stats (struct connection *c);
int mcp_check_ready (struct connection *c);

conn_type_t ct_spell_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "spell_engine_server",
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


int backlog = BACKLOG, port = TCP_PORT, udp_port = UDP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 0;

struct in_addr settings_addr;
int verbosity = 0;

int test_mode = 0;
int max_misspell_words = 0;
int max_msg_size = 0;
char *worst_misspell_msg = NULL;

int quit_steps;
int interactive;
int nthreads = 1;
char *progname = NULL, *username, *logname, *binlogname;

int start_time;

long long cmd_get, cmd_set, get_hits, get_missed, cmd_version, cmd_stats, curr_items, total_items;

#define VALUE_BUFFSIZE (1 << 20)
char value_buff[VALUE_BUFFSIZE];
#define STATS_BUFF_SIZE	(16 << 10)
char stats_buff[STATS_BUFF_SIZE];

char key_buff[MAX_KEY_LEN+1];

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = mcs_delete,
  .mc_version = memcache_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

static inline void init_tmp_buffers (struct connection *c) {
  free_tmp_buffers (c);
  c->Tmp = alloc_head_buffer ();
  assert (c->Tmp);
}

struct keep_mc_store {
  int text_id;
  int res[3];
};

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  cmd_set++;
  if (op != mct_set || size >= VALUE_BUFFSIZE - 1) {
    free_tmp_buffers (c);
    return -2;
  }
  struct keep_mc_store s;
  if (sscanf (key, "text%d", &s.text_id) == 1) {

    if (max_msg_size < size) {
      max_msg_size = size;
    }
    assert (read_in (&c->In, value_buff, size) == size);
    value_buff[size] = 0;
    int r = spell_check (value_buff, s.res, test_mode);
    if (!r) {
      if (max_misspell_words < s.res[1]) {
        max_misspell_words = s.res[1];
        if (test_mode) {
          if (worst_misspell_msg) {
            free (worst_misspell_msg);
          }
          worst_misspell_msg = strdup (value_buff);
        }
      }
      init_tmp_buffers (c);
      write_out (c->Tmp, &s, sizeof (struct keep_mc_store));
      return 1;
    }
    free_tmp_buffers (c);
    return 0;
  }
/*
  if (sscanf (key, "TEXT%d", &s.text_id) == 1) {
    assert (read_in (&c->In, value_buff, size) == size);
    value_buff[size] = 0;
    int r = spell_check2 (value_buff, s.res);
    if (!r) {
      init_tmp_buffers (c);
      write_out (c->Tmp, &s, sizeof (struct keep_mc_store));
      return 1;
    }
    free_tmp_buffers (c);
    return 0;
  }
*/
  free_tmp_buffers (c);
  return -2;
}

int spell_prepare_stats (struct connection *c);

int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof(VERSION));
  return 0;
}

int memcache_get (struct connection *c, const char *key, int key_len) {
  cmd_get++;
  int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;
  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int stats_len = spell_prepare_stats (c);
    return_one_key (c, key - dog_len, stats_buff, stats_len);
    return 0;
  }
  struct keep_mc_store *Data = 0;
  if (c->Tmp) {
    Data = (struct keep_mc_store *) c->Tmp->start;
  }
  int text_id;
  if (Data && sscanf (key, "request%d", &text_id) == 1 && Data->text_id == text_id) {
    get_hits++;
    return return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%d,%d,%d", Data->res[0], Data->res[1], Data->res[2]));
  }
  if (key_len == 5 && !memcmp (key, "dicts", 5)) {
  int l = spell_get_dicts (stats_buff, STATS_BUFF_SIZE);
    get_hits++;
    return return_one_key (c, key - dog_len, stats_buff, l);
  }
  if (test_mode && worst_misspell_msg && key_len == 18 && !memcmp (key, "worst_misspell_msg", 18)) {
    get_hits++;
    return return_one_key (c, key - dog_len, worst_misspell_msg,strlen (worst_misspell_msg));
  }
  free_tmp_buffers (c);
  get_missed++;
  return 0;
}

int spell_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  sb_printf (&sb,
      "cmd_get\t%lld\n"
      "cmd_set\t%lld\n"
      "get_hits\t%lld\n"
      "get_misses\t%lld\n"
      "cmd_version\t%lld\n"
      "cmd_stats\t%lld\n"
      "spellers\t%d\n"
      "use_aspell_suggestion\t%d\n"
      "yo_hack\t%d\n"
      "yo_hack_hits\t%lld\nyo_hack_calls\t%lld\n"
      "check_word_hits\t%lld\ncheck_word_calls\t%lld\n"
      "max_misspell_words\t%d\n"
      "max_msg_size\t%d\n"
      "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
      "64-bit"
#else
      "32-bit"
#endif
      "\n",
      cmd_get,
      cmd_set,
      get_hits,
      get_missed,
      cmd_version,
      cmd_stats,
      spellers,
      use_aspell_suggestion,
      yo_hack,
      yo_hack_stat[0], yo_hack_stat[1],
      check_word_stat[0], check_word_stat[1],
      max_misspell_words,
      max_msg_size
      );
  return sb.pos;
}

int memcache_stats (struct connection *c) {
  cmd_stats++;
  int stats_len = spell_prepare_stats (c);
  write_out (&c->Out, stats_buff, stats_len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


/*
 *
 *      END (PROXY MEMCACHE SERVER)
 *
 */

void reopen_logs (void) {
  int fd;
  fflush(stdout);
  fflush(stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2) {
      close(fd);
    }
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  exit (EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  exit (EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs ();
  signal (SIGUSR1, sigusr1_handler);
}

void cron (void) {
  create_all_outbound_connections ();
  spell_reoder_spellers ();
}

int sfd;

void start_server (void) {
  int i;
  int prev_time;

  init_epoll ();
  init_netbuffers ();

  prev_time = 0;

  if (daemonize) {
    setsid();
  }

  if (change_user(username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_listening_connection (sfd, &ct_spell_engine_server, &memcache_methods);

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGIO, SIG_IGN);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
  }

  int quit_steps = 0;

  for (i = 0; ; i++) {
    if (!(i & 255)) {
      vkprintf (1, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);

    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close (sfd);

}

/*
 *
 *		MAIN
 *
 */

void help (void) {
  printf (VERSION_STR"\n"
          "[-p <port>]\tTCP port number to listen on (default: %d)\n"
          "[-y]\t\tuse yo_hack (in case when russian dictionaries without ¸ doesn't installed, Debian aspell-ru package is outdated)\n"
          "[-S]\t\tuse aspell suggestion\n"
          "[-t]\t\ttest mode\n"
          "[-d]\t\trun as a daemon\n"
          "[-u <username>]\tassume identity of <username> (only when run as root)\n"
          "[-c <max_conn>]\tmax simultaneous connections, default is %d\n"
          "[-v]\t\tverbose\n"
          "[-h]\t\tprint this help and exit\n"
          "[-b <backlog>]\n"
          "[-l <log_name>]\tlog... about something\n",
          TCP_PORT,
          MAX_CONNECTIONS
         );
  exit (2);
}

int main (int argc, char *argv[]) {
  int i;
  //int k;
  //long long x;
  //char c;

  set_debug_handlers ();

  progname = argv[0];
  while ((i = getopt (argc, argv, "b:c:l:p:U:dhu:vSty")) != -1) {
    switch (i) {
    case 'y':
      yo_hack = 1;
    break;
    case 't':
      test_mode = 1;
    break;
    case 'S':
      use_aspell_suggestion = 1;
    break;
    case 'v':
      verbosity++;
      break;
    case 'h':
      help ();
      return 2;
    case 'b':
      backlog = atoi (optarg);
      if (backlog <= 0) {
        backlog = BACKLOG;
      }
      break;
    case 'c':
      maxconn = atoi (optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
        maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'U':
      udp_port = atoi (optarg);
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'd':
      daemonize ^= 1;
      break;
    }
  }

  //dynamic_data_buffer_size = 1 << 22;

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }
  
  sfd = server_socket (port, settings_addr, backlog, 0);
  if (sfd < 0) {
    kprintf ("cannot open server socket at port %d: %m\n", port);
    exit (1);
  }

  aes_load_pwd_file (0);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  init_dyn_data ();

  start_time = time (0);
  spell_init ();
  atexit (spell_done);

  start_server ();
  return 0;
}

