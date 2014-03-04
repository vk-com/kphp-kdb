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
              2011 Nikolai Durov
              2011 Andrei Lopatin
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <netdb.h>
#include <signal.h>

#include "kdb-data-common.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-client.h"
#include "../targ/kdb-targ-binlog.h"

#define	MAX_KEY_LEN	1000
#define MAX_KEY_RETRIES	3
#define	MAX_VALUE_LEN	(1 << 24)

#define	VERSION_STR "engine-tester-0.1"

#define MAX_NET_RES	(1L << 16)

#define	PING_INTERVAL	10
#define	RESPONSE_FAIL_TIMEOUT	5

char VB[MAX_VALUE_LEN + 16];

int proxy_client_execute (struct connection *c, int op);

int mcp_check_ready (struct connection *c);



struct memcache_client_functions tester_outbound = {
  .execute = proxy_client_execute,
  .check_ready = mcp_check_ready,
  .flush_query = mcc_flush_query,
};


struct conn_target default_ct = {
.min_connections = 1,
.max_connections = 1,
.type = &ct_memcache_client,
.extra = &tester_outbound,
.reconnect_timeout = 10,
}, *conn;

int port = 2391;

int disable_log, query_log, stat_log;

int mcpo_init_outbound (struct connection *c);



int backlog = BACKLOG, maxconn = MAX_CONNECTIONS, daemonize = 0;
struct in_addr settings_addr;
int verbosity = 0, interactive = 0, test_mode = 0, max_ad_id = 666;
int quit_steps;

char *fnames[3];
int fd[3];
long long fsize[3];

// unsigned char is_letter[256];
char *progname = "targ-recover", *username, *hostname = "localhost", *logname;
char *suffix;

int start_time;
long long netw_queries, minor_update_queries, search_queries;
long long tot_response_words, tot_response_bytes, tot_ok_gathers, tot_bad_gathers;
double tot_ok_gathers_time, tot_bad_gathers_time;

int active_queries;

long long tot_forwarded_queries, expired_forwarded_queries;

int reconnect_retry_interval = 20;
int connection_query_timeout = 1;
int connection_disable_interval = 20;


#define STATS_BUFF_SIZE	(16 << 10)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];

/* file utils */

int open_file (int x, char *fname, int creat) {
  fnames[x] = fname;
  fd[x] = open (fname, creat > 0 ? O_RDWR | O_CREAT : O_RDONLY, 0600);
  if (creat < 0 && fd[x] < 0) {
    if (fd[x] < 0) {
      fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    }
    return -1;
  }
  if (fd[x] < 0) {
    fprintf (stderr, "%s: cannot open %s: %m\n", progname, fname);
    exit(1);
  }
  fsize[x] = lseek (fd[x], 0, SEEK_END);
  if (fsize[x] < 0) {
    fprintf (stderr, "%s: cannot seek %s: %m\n", progname, fname);
    exit(2);
  }
  lseek (fd[x], 0, SEEK_SET);
  if (verbosity) {
    fprintf (stderr, "opened file %s, fd=%d, size=%lld\n", fname, fd[x], fsize[x]);
  }
  return fd[x];
}




void reopen_logs(void) {
  int fd;
  fflush(stdout);
  fflush(stderr);
  if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
    dup2(fd, 0);
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (logname && (fd = open(logname, O_WRONLY|O_APPEND|O_CREAT, 0640)) != -1) {
    dup2(fd, 1);
    dup2(fd, 2);
    if (fd > 2)
      close(fd);
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}



static void sigint_handler (const int sig) {
  fprintf (stderr, "SIGINT handled.\n");
  exit(EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  exit(EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf(stderr, "got SIGHUP.\n");
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf(stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs();
  signal(SIGUSR1, sigusr1_handler);
}

int cfd, next_ad_id = 1, state = 0;
int last_success;

void cron (void) {
  if (now > last_success + 5 && now > start_time + 5) {
    fprintf (stderr, "fatal: no response from %s:%d in 5 seconds, exiting; %d ads processed\n", hostname, port, next_ad_id-1);
    exit (2);
  }
}


void new_request1 (struct connection *c) {
  if (next_ad_id > max_ad_id) {
    if (verbosity > 0) {
      fprintf (stderr, "%d ads complete, exiting\n", max_ad_id);
    }
    exit (0);
  }
  static char buff[256];
  int len = sprintf (buff, "get ad_info%d\r\n", next_ad_id);
  write_out (&c->Out, buff, len);
  flush_connection_output (c);
  if (verbosity > 1) {
    fprintf (stderr, "Requested information about ad %d\n", next_ad_id);
  }
}

void new_request2 (struct connection *c) {
  static char buff[256];
  int len = sprintf (buff, "get ad_query%d\r\n", next_ad_id);
  write_out (&c->Out, buff, len);
  flush_connection_output (c);
  if (verbosity > 1) {
    fprintf (stderr, "Requested query for ad %d\n", next_ad_id);
  }
}



int temp_id = -1, ad_flags = -1, temp_users = -1;
struct lev_targ_target target_entry = {.type = LEV_TARG_TARGET};
struct lev_targ_ad_off off_entry = {.type = LEV_TARG_AD_OFF};
struct lev_targ_stat_load stat_entry = {.type = LEV_TARG_STAT_LOAD};

int nulll = 0;
/*
struct lev_targ_ad_off {
  lev_type_t type;
  int ad_id;
};
struct lev_targ_stat_load {
  lev_type_t type;
  int ad_id;
  int clicked;
  int click_money;
  int views;
  int l_clicked;
  int l_views;
};
*/



int proxy_client_execute (struct connection *c, int op) {
  int len, ret = SKIP_ALL_BYTES;
  char *ptr;
  struct mcc_data *D = MCC_DATA(c);

  if (op == mcrt_VALUE) {
    if (D->key_len > 0 && D->key_len <= MAX_KEY_LEN && D->arg_num == 2 && (unsigned) D->args[1] <= MAX_VALUE_LEN) {
      int needed_bytes = D->args[1] + D->response_len + 2 - c->In.total_bytes;
      if (needed_bytes > 0) {
        return needed_bytes;
      }
      nbit_advance (&c->Q, D->args[1]);
      len = nbit_ready_bytes (&c->Q);
      assert (len > 0);
      ptr = nbit_get_ptr (&c->Q);
    } else {
      fprintf (stderr, "error at VALUE: op=%d, key_len=%d, arg_num=%d, value_len=%lld\n", op, D->key_len, D->arg_num, D->args[1]);
     
      if (verbosity > -2) {
        dump_connection_buffers (c);
        if (c->first_query != (struct conn_query *) c && c->first_query->req_generation == c->first_query->requester->generation) {
          dump_connection_buffers (c->first_query->requester);
        }
      }

      D->response_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    if (len == 1) {
      nbit_advance (&c->Q, 1);
    }
    if (ptr[0] != '\r' || (len > 1 ? ptr[1] : *((char *) nbit_get_ptr(&c->Q))) != '\n') {
      fprintf (stderr, "missing cr/lf at VALUE: op=%d, key_len=%d, arg_num=%d, value_len=%lld\n", op, D->key_len, D->arg_num, D->args[1]);

      if (verbosity > -2) {
        dump_connection_buffers (c);
        if (c->first_query != (struct conn_query *) c && c->first_query->req_generation == c->first_query->requester->generation) {
          dump_connection_buffers (c->first_query->requester);
        }
      }

      D->response_flags |= 16;
      return SKIP_ALL_BYTES;
    }
    len = 2;

    if (verbosity > 0) {
      fprintf (stderr, "mcc_value: op=%d, key_len=%d, flags=%lld, time=%lld, value_len=%lld\n", op, D->key_len, D->args[0], D->args[1], D->args[2]);
    }

    ret = 0;


    assert (advance_skip_read_ptr (&c->In, D->response_len) == D->response_len);


    assert (read_in (&c->In, VB, D->args[1] + 2) == D->args[1] + 2);
    VB[D->args[1]] = 0;
  }

  if (op == mcrt_VERSION) {
    new_request1 (c);
    state = 1;
  } else if (op == mcrt_END) {
    ++state;
    last_success = now;
    if (state == 3) {
      ++next_ad_id;
      new_request1 (c);
      state = 1;
    } else {
      new_request2 (c);
      state = 2;
    }
  } else if (op == mcrt_VALUE) {
    if (state == 1) {
      if (verbosity > 1) {
        fprintf (stderr, "Info for ad %d: %s\n", next_ad_id, VB);
      }
      assert (sscanf (VB, "%d,%d,%d,%d,%d,%d,%d,%d,%d,", &temp_id, &ad_flags, &target_entry.ad_price, &temp_users, 
                                                        &stat_entry.views, &stat_entry.clicked, &stat_entry.click_money,
                                                        &stat_entry.l_clicked, &stat_entry.l_views) == 9);
      assert (temp_id == next_ad_id);
    } else {
      assert (D->args[1] <= 4096);
      if (verbosity > 1) {
        fprintf (stderr, "Query for ad %d (temp id = %d): %s\n", next_ad_id, temp_id, VB);
      }
      if (temp_id != next_ad_id) {
        fprintf (stderr, "WARNING: ad %d with query but without info\n", next_ad_id);
      } else {
        target_entry.ad_id = next_ad_id;
        //target_entry.ad_price is read
        assert (target_entry.ad_price != 0);
        target_entry.ad_query_len = D->args[1];
        assert (write (query_log, &target_entry, 14) == 14);
        assert (write (query_log, VB, D->args[1] + 1) == D->args[1] + 1);
        assert (write (query_log, &nulll, (-(D->args[1] + 15) & 3)) == ((-(D->args[1] + 15)) & 3));

        if (!(ad_flags & 1)) {
          off_entry.ad_id = next_ad_id;
          assert (write (disable_log, &off_entry, sizeof (off_entry)) == sizeof (off_entry));
        }

        stat_entry.ad_id = next_ad_id;
        assert (write (stat_log, &stat_entry, sizeof (stat_entry)) == sizeof (stat_entry));
      }
    }
  }

  return ret;
}

int mcp_check_ready (struct connection *c) {
  return c->ready = cr_ok;
}



void start_server (void) { 
  int i;
  int prev_time;
  struct hostent *h;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (daemonize) {
    setsid();
  }

  if (change_user(username) < 0 && !interactive && !test_mode) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  sprintf (VB, "disable%s.bin", suffix);
  disable_log = open (VB, O_CREAT | O_EXCL | O_WRONLY, 0640);
  sprintf (VB, "query%s.bin", suffix);
  query_log = open (VB, O_CREAT | O_EXCL | O_WRONLY, 0640);
  sprintf (VB, "stat%s.bin", suffix);
  stat_log = open (VB, O_CREAT | O_EXCL | O_WRONLY, 0640);

  assert (disable_log >= 0 && query_log >= 0 && stat_log >= 0);

  if (!(h = gethostbyname (hostname)) || h->h_addrtype != AF_INET || h->h_length != 4 || !h->h_addr_list || !h->h_addr) {
    fprintf (stderr, "fatal: cannot resolve hostname %s: %m\n", hostname);
    exit (1);                                       
  }

  default_ct.target = *(struct in_addr *) h->h_addr;
  default_ct.port = port;
  conn = create_target (&default_ct, 0);

  create_all_outbound_connections ();

  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigterm_handler);
  signal(SIGUSR1, sigusr1_handler);
  if (daemonize) {
    signal(SIGHUP, sighup_handler);
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (57);
    if (now != prev_time) {
      prev_time = now;
      cron();
    }
    if (quit_steps && !--quit_steps) break;
  }

}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-u<username>] [-l<log-name>] [-t<target-host>] [-p<target-port>] [-m<max-ad-id>]\n"
	  "\t-v\toutput statistical and debug information into stderr\n",
	  progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i;

  srand48 (time (0));

  progname = argv[0];                            
  while ((i = getopt (argc, argv, "a:b:c:l:m:n:p:t:u:Tdhv")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'h':
      usage();
      return 2;
    case 'b':
      backlog = atoi(optarg);
      if (backlog <= 0) {
        backlog = BACKLOG;
      }
      break;
    case 'c':
      maxconn = atoi(optarg);
      if (maxconn <= 0 || maxconn > MAX_CONNECTIONS) {
	maxconn = MAX_CONNECTIONS;
      }
      break;
    case 'n':
      errno = 0;
      nice (atoi (optarg));
      if (errno) {
        perror ("nice");
      }
      break;
    case 'u':
      username = optarg;
      break;
    case 'l':
      logname = optarg;
      break;
    case 'T':
      test_mode = 1;
      break;
    case 't':
      hostname = optarg;
      break;
    case 'p':
      port = atoi (optarg);
      break;
    case 'm':
      max_ad_id = atoi (optarg);
      assert (max_ad_id > 0);
      break;
    case 'a':
      suffix = optarg;
      break;
    case 'd':
      daemonize ^= 1;
    }
  }
  if (argc != optind) {
    usage();
    return 2;
  }

  if (!suffix) {
    fprintf (stderr, "fatal: supply a suffix with -a switch\n");
    exit (1);
  }


  init_dyn_data ();

  if (raise_file_rlimit (maxconn + 16) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  start_time = time(0);

  start_server();

  return 0;
}

