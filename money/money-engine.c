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

    Copyright 2010-2012 Vkontakte Ltd
              2010-2012 Nikolai Durov
              2010-2012 Andrei Lopatin
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
#include <signal.h>

#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "kdb-money-binlog.h"
#include "money-data.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "kfs.h"

#define	VERSION_STR	"money-engine-0.2"

#define TCP_PORT 11211

#define MAX_NET_RES	(1L << 16)



/*
 *
 *		MONEY ENGINE
 *
 */

int money_engine_wakeup (struct connection *c);
int money_engine_alarm (struct connection *c);

conn_type_t ct_money_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "money_engine_server",
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
  .wakeup = money_engine_wakeup,
  .alarm = money_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};



int verbosity = 0, interactive = 0;
volatile int quit_at;

char *fnames[3];
int fd[3];
long long fsize[3];

// unsigned char is_letter[256];
char *progname = "money-engine", *username, *logname;

char master_host[256];
int master_port;

/* stats counters */
int start_time, jump_log_ts;
long long binlog_loaded_size, jump_log_pos;
long long netw_queries, delete_queries, get_queries, update_queries, increment_queries;
long long create_account_queries, declare_transaction_queries;
long long tot_response_words, tot_response_bytes;
double binlog_load_time, index_load_time, total_get_time;
unsigned jump_log_crc32;

volatile int sigpoll_cnt;

#define STATS_BUFF_SIZE	(1 << 20)
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

/*
 *
 *		SERVER
 *
 */


int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 1, is_server = 0;
struct in_addr settings_addr;
int active_connections;

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_get_start (struct connection *c);
int memcache_get_end (struct connection *c, int key_count);


struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = memcache_get_start,
  .mc_get = memcache_get,
  .mc_get_end = memcache_get_end,
  .mc_incr = memcache_incr,
  .mc_delete = memcache_delete,
  .mc_version = mcs_version,
  .mc_stats = memcache_stats,
  .mc_check_perm = mcs_default_check_perm,
  .mc_init_crypto = mcs_init_crypto
};

int quit_steps;

static double safe_div (double x, double y) { return y > 0 ? x/y : 0; }

int money_prepare_stats (void) {
  int uptime = now - start_time;
  int log_uncommitted = compute_uncommitted_log_bytes();

  return stats_buff_len = snprintf (stats_buff, STATS_BUFF_SIZE,
		  "heap_used\t%ld\n"
		  "heap_max\t%ld\n"
		  "binlog_original_size\t%lld\n"
		  "binlog_loaded_bytes\t%lld\n"
		  "binlog_load_time\t%.6fs\n"
		  "current_binlog_size\t%lld\n"
		  "binlog_uncommitted_bytes\t%d\n"
		  "binlog_path\t%s\n"
		  "binlog_first_timestamp\t%d\n"
		  "binlog_read_timestamp\t%d\n"
		  "binlog_last_timestamp\t%d\n"
		  "account_types\t%d\n"
		  "accounts\t%d\n"
		  "frozen_accounts\t%d\n"
		  "special_accounts\t%d\n"
		  "transactions\t%d\n"
		  "temp_transactions\t%d\n"
		  "committed_transactions\t%d\n"
		  "cancelled_transactions\t%d\n"
		  "cancelled_committed_transactions\t%d\n"
		  "pending_long_transactions\t%d\n"
		  "committed_operations\t%d\n"
		  "locks\t%d\n"
		  "queries_get\t%lld\n"
		  "qps_get\t%.3f\n"
		  "avg_get_time\t%.6f\n"
		  "queries_delete\t%lld\n"
		  "qps_delete\t%.3f\n"
		  "queries_increment\t%lld\n"
		  "qps_increment\t%.3f\n"
		  "queries_create_account\t%lld\n"
		  "queries_declare_transaction\t%lld\n"
		  "qps_declare_transaction\t%.3f\n"
		  "version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
		  "64-bit"
#else
     		  "32-bit"
#endif
		  " after commit " COMMIT "\n",
		  (long) (dyn_cur - dyn_first),
		  (long) (dyn_last - dyn_first),
		  binlog_loaded_size,
		  log_readto_pos - jump_log_pos,
		  binlog_load_time,
		  log_pos,
		  log_uncommitted,
		  binlogname ? (strlen(binlogname) < 250 ? binlogname : "(too long)") : "(none)",
		  log_first_ts,
		  log_read_until,
		  log_last_ts,
                  tot_account_types, 
                  tot_accounts, 
                  frozen_accounts, 
                  special_accounts,
                  tot_transactions, 
                  temp_transactions, 
                  committed_transactions, 
                  cancelled_transactions, 
                  cancelled_committed_transactions, 
                  locked_long_transactions,
                  committed_operations, 
                  tot_locks,
		  get_queries,
		  safe_div(get_queries, uptime),
		  get_queries > 0 ? total_get_time / get_queries : 0,
		  delete_queries,
		  safe_div(delete_queries, uptime),
		  increment_queries,
		  safe_div(increment_queries, uptime),
		  create_account_queries,
		  declare_transaction_queries,
		  safe_div (declare_transaction_queries, uptime)
		  );
}


int parse_account_type (char *ptr, char **endptr) {
  int i, res = 0;

  for (i = 0; i < 3; i++) {
    if (*ptr < 'A' || *ptr > 'Z') {
      break;
    }
    res = res * 27 + (*ptr - 'A') + 1;
    ++ptr;
  }

  *endptr = ptr;
  return res;
}

int parse_auth_code (char *ptr) {
  int i;

  for (i = 0; i < 32; i++) {
    if ((ptr[i] < 'a' && ptr[i] > '9') || ptr[i] < '0' || ptr[i] > 'f') {
      return 0;
    }
  }

  return 32;
}

char *store_acc_id (char *to, int acc_type_id, long long acc_id) {
  int x;
  assert (acc_type_id >= 0 && acc_type_id <= MAX_ACCOUNT_TYPE);
  if (acc_type_id >= 27 * 27) {
    *to++ = 64 + (acc_type_id / (27 * 27));
  }
  if (acc_type_id >= 27) {
    x = acc_type_id / 27 % 27;
    *to++ = (x > 0 ? 64 + x : '_');
  }
  if (acc_type_id) {
    x = acc_type_id % 27;
    *to++ = (x > 0 ? 64 + x : '_');
  }
  return to + sprintf (to, "%lld", acc_id);
} 

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  int temp_id, tmp;
  char *ptr, *oldptr;
  if (verbosity > 1) {
    fprintf (stderr, "memcache_store: key='%s', key_len=%d, value_len=%d\n", key, key_len, size);
  }

  if (op != mct_add) {
    return -2;
  }

  if (global_engine_status != 1) {
    if (verbosity > 1) {
      fprintf (stderr, "operation forbidden by global_engine_status = %d\n", global_engine_status);
    }
  }

  if (sscanf (key, "transaction%d", &temp_id) >= 1 && temp_id > 0 && size < (1 << 14)) {
    struct lev_money_trans_declare *E = (void *) &stats_buff[1 << 14];
    E->temp_id = temp_id;

    assert (read_in (&c->In, stats_buff, size) == size);
    stats_buff[size] = 0;

    int i, N = strtoul (stats_buff, &ptr, 10);

//    fprintf (stderr, "N=%d\n", N);

    if (N < 2 || N > 10 || ptr == stats_buff || (*ptr != 13 && *ptr != 10)) {
      return 0;
    }

    if (*ptr == 13) {
      ptr++;
    }
    if (*ptr++ != 10) {
      return 0;
    }

    E->parties = N;
    E->transaction_id = 0;

    for (i = 0; i < N; i++) {
      E->T[i].acc_type_id = parse_account_type (ptr, &ptr);
//      fprintf (stderr, "party #%d: acc_type=%d\n", i, E->T[i].acc_type_id);
      if (E->T[i].acc_type_id == 0) {
        return 0;
      }
      E->T[i].acc_id = strtoull (ptr, &ptr, 10);
//      fprintf (stderr, "acc_id=%lld\n", E->T[i].acc_id);
      if (E->T[i].acc_id <= 0) {
        return 0;
      }
      if (*ptr++ != ':') {
        return 0;
      }
      tmp = strtol (oldptr = ptr, &ptr, 10);
      if (oldptr == ptr || tmp < MIN_CURRENCY_ID || tmp > MAX_CURRENCY_ID) {
        return 0;
      }
      E->T[i].currency = tmp;
//      fprintf (stderr, "currency=%d\n", tmp);
      if (*ptr++ != ':') {
        return 0;
      }
      E->T[i].acc_incr = strtoll (oldptr = ptr, &ptr, 10);
//      fprintf (stderr, "acc_incr=%lld\n", E->T[i].acc_incr);
      if (oldptr == ptr || E->T[i].acc_incr < -MAX_ACC_INCR || E->T[i].acc_incr >= MAX_ACC_INCR) {
        return 0;
      }
      if (*ptr == ':') {
        ++ptr;
	if (!parse_auth_code (ptr)) {
          return 0;
	}
        E->T[i].auth_code = (long long) (long) ptr;
	ptr += 32;
      } else {
        E->T[i].auth_code = 0;
      }

      if (verbosity > 1) {
        fprintf (stderr, "party #%d: acc_type_id=%d, acc_id=%lld, currency=%d, acc_incr=%lld, auth_code=%llx\n",
                   i, E->T[i].acc_type_id, E->T[i].acc_id, E->T[i].currency,
                   E->T[i].acc_incr, E->T[i].auth_code);
      }

      if (*ptr == '\r') {
        ++ptr;
      }

      if (*ptr++ != '\n') {
        return 0;
      }
    }

    tmp = -1;
    if (sscanf (ptr, "%u:%d%n", &E->ip, &E->declared_date, &tmp) < 2 || tmp < 0) {
      return 0;
    }

    ptr += tmp;

    if (*ptr == '\r') {
      ++ptr;
    }

    if (*ptr++ != '\n') {
      return 0;
    }

    assert (ptr - stats_buff <= size);

    E->comm_len = strlen (ptr);

    if (ptr + E->comm_len != stats_buff + size) {
      return 0;
    }

    memcpy ((char *)(&E->T[N]), ptr, E->comm_len + 1);

    for (i = 0; i < N; i++) {
      if (E->T[i].auth_code) {
        static char tmp_buff[256];
        char *t;
        t = store_acc_id (tmp_buff, E->T[i].acc_type_id, E->T[i].acc_id);
        t += sprintf (t, ":%d:%lld:%d:%d", E->T[i].currency, E->T[i].acc_incr, E->temp_id, E->declared_date);
        E->T[i].auth_code = check_auth_code ((char *) (long) E->T[i].auth_code, tmp_buff, E->T[i].acc_type_id, E->T[i].acc_id);
        if (verbosity > 1) {
          fprintf (stderr, "resulting auth_code=%016llx\n", E->T[i].auth_code);
        }
      }
    }

    if (verbosity > 1) {
      fprintf (stderr, "before create_temp_transaction(): temp_id=%d, parties=%d\n", E->temp_id, N);
    }
    int res = create_temp_transaction (E);
    if (verbosity > 1) {
      fprintf (stderr, "create_temp_transaction() = %d\n", res);
    }

    ++declare_transaction_queries;

    return res > 0;
  }


  tmp = -1;
  if (!memcmp (key, "account", 7) && size < (1 << 14)) {
    int acc_type_id, owner, comm_len = 0, L;
    unsigned ip;
    long long acc_id, auth_code = 0, access_code = 0, withdraw_code = 0;
    char *comment = 0, *auth_code_ptr = 0;

    ptr = (char *)(key + 7);
    acc_type_id = parse_account_type (ptr, &ptr);
    if (!acc_type_id) {
      return -2;
    }
   
    acc_id = strtoull (ptr, &ptr, 10);
    if (acc_id <= 0) {
      return -2;
    }

    L = ptr - (key + 7);
    memcpy (stats_buff, key + 7, L);
    stats_buff[L++] = ':';
    
    if (*ptr == '#') {
      ptr++;
      if (!parse_auth_code (ptr)) {
        return -2;
      }
      auth_code_ptr = ptr;
      ptr += 32;
    }

    if (ptr != key + key_len) {
      return -2;
    }

    ptr = stats_buff + L;
    assert (read_in (&c->In, ptr, size) == size);
    ptr[size] = 0;

    owner = strtol (oldptr = ptr, &ptr, 10);
    if (ptr == oldptr) {
      return 0;
    }

    if (*ptr++ != ',') {
      return 0;
    }

    ip = strtoul (oldptr = ptr, &ptr, 10);
    if (ptr == oldptr) {
      return 0;
    }
    
    if (*ptr == ':') {
      ++ptr;
      access_code = strtoull (oldptr = ptr, &ptr, 16);
      if (ptr != oldptr + 16) {
        return 0;
      }
      if (*ptr == ':') {
        ++ptr;
        withdraw_code = strtoull (oldptr = ptr, &ptr, 16);
        if (ptr != oldptr + 16) {
          return 0;
        }
      } else {
        withdraw_code = access_code;
      }
    }

    if (*ptr == '\t') {
      ++ptr;
      assert (ptr - stats_buff - L <= size);

      comm_len = strlen (ptr);

      if (ptr + comm_len != stats_buff + L + size) {
        return 0;
      }

      comment = ptr;
    } else {
      if (ptr != stats_buff + L + size) {
        return 0;
      }
    }

    if (auth_code_ptr) {
      auth_code = check_auth_code (auth_code_ptr, stats_buff, acc_type_id, acc_id);
    }

    if (verbosity > 1) {
      fprintf (stderr, "before do_create_account(): acc_type_id=%d, acc_id=%lld, owner=%d, auth_code=%016llx, comment=%.*s\n", acc_type_id, acc_id, owner, auth_code, (comm_len < 32) ? comm_len : 32, comment);
    }
    int res = do_create_account (acc_type_id, acc_id, auth_code, owner, ip, access_code, withdraw_code, comment, comm_len);
    if (verbosity > 1) {
      fprintf (stderr, "do_create_account() = %d\n", res);
    }

    ++create_account_queries;

    return res > 0;
  }

  return -2;
}

int memcache_stats (struct connection *c) {
  int len = money_prepare_stats ();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


#define RR_SIZE 65536
static long long RR[RR_SIZE + 16];

int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }

  if (key_len >= 7 && !strncmp (key, "balance", 7)) {
    char *ptr;
    int acc_type_id, currency;
    long long acc_id, auth_code = 0, locked;

    ptr = (char *)(key + 7);
    acc_type_id = parse_account_type (ptr, &ptr);
    if (!acc_type_id) {
      return 0;
    }
   
    acc_id = strtoull (ptr, &ptr, 10);
    if (acc_id <= 0) {
      return 0;
    }
    
    if (*ptr == '#') {
      ptr++;
      if (!parse_auth_code (ptr)) {
        return 0;
      }
      ptr[-1] = 0;
      auth_code = check_auth_code (ptr, (char *) key + 7, acc_type_id, acc_id);
      ptr[-1] = '#';
      ptr += 32;
    }

    if (ptr != key + key_len) {
      return 0;
    }

    long long balance = get_balance (acc_type_id, acc_id, auth_code, &currency, &locked);
    if (balance == BALANCE_NO_ACCOUNT) {
      return_one_key (c, key, "NO_ACCOUNT", 10);
      return 0;
    }
    if (balance == ERROR_BALANCE) {
      return_one_key (c, key, "FORBIDDEN", 9);
      return 0;
    }

    return_one_key (c, key, stats_buff, sprintf (stats_buff, "%lld:%d:%lld", balance, currency, locked));

    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "check", 5)) {
    int temp_id = 0;
    if (sscanf (key, "check%d", &temp_id) >= 1) {
      int res = do_check_transaction (temp_id);
      if (res < -16) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", res >> 4, 1 + (res & 15)));
      } else if (res) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }

  if (key_len >= 4 && !strncmp (key, "lock", 4)) {
    int temp_id = 0;
    if (sscanf (key, "lock%d", &temp_id) >= 1) {
      int res = do_lock_transaction (temp_id);
      if (res < -16) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", res >> 4, 1 + (res & 15)));
      } else if (res) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }

  if (key_len >= 6 && !strncmp (key, "commit", 6)) {
    int temp_id = 0;
    long long transaction_id = 0;
    if (sscanf (key, "commit%d", &temp_id) >= 1) {
      int res = do_commit_transaction (temp_id, &transaction_id, stats_buff + 1024);
      if (res < -16) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", res >> 4, 1 + (res & 15)));
      } else if (res != 1) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%lld:%s", res, transaction_id, stats_buff + 1024));
      }
    }
    return 0;
  }

  if (key_len >= 9 && !strncmp (key, "long_lock", 9)) {
    long long transaction_id;
    int seconds, temp_id;
    if (sscanf (key, "long_lock%d:%d", &temp_id, &seconds) >= 2) {
      money_auth_code_t codes[2];
      int res = do_long_lock_transaction (temp_id, seconds, &transaction_id, codes);
      if (res < -16) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", res >> 4, 1 + (res & 15)));
      } else if (res != 2) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%lld:%016llx:%016llx", res, transaction_id, codes[0], codes[1]));
      }
    }
    return 0;
  }

  if (key_len >= 10 && !strncmp (key, "long_check", 10)) {
    long long transaction_id;
    money_auth_code_t auth_code;
    if (sscanf (key, "long_check%lld#%llx", &transaction_id, &auth_code) >= 2) {
      int res = long_check_transaction (transaction_id, auth_code);
      if (res < -16) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", res >> 4, 1 + (res & 15)));
      } else {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }

  if (key_len >= 11 && !strncmp (key, "long_cancel", 11)) {
    long long transaction_id;
    money_auth_code_t auth_code;
    if (sscanf (key, "long_cancel%lld#%llx", &transaction_id, &auth_code) >= 2) {
      int res = do_long_cancel_transaction (transaction_id, auth_code);
      if (res < -16) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", res >> 4, 1 + (res & 15)));
      } else {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      }
    }
    return 0;
  }

  if (key_len >= 11 && !strncmp (key, "long_commit", 11)) {
    long long transaction_id;
    money_auth_code_t auth_code;
    if (sscanf (key, "long_commit%lld#%llx", &transaction_id, &auth_code) >= 2) {
      int res = do_long_commit_transaction (transaction_id, auth_code, stats_buff + 1024);
      if (res < -16) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", res >> 4, 1 + (res & 15)));
      } else if (res != 1) {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%lld:%s", res, transaction_id, stats_buff + 1024));
      }
    }
    return 0;
  }

  if (key_len >= 20 && !strncmp (key, "account_transactions", 20)) {
    char *ptr, *acc_id_end;
    int acc_type_id, flags, from, to, sign_time = 0, R_cnt = 0;
    long long acc_id;
    money_auth_code_t auth_code = 0;

    ptr = (char *)(key + 20);
    acc_type_id = parse_account_type (ptr, &ptr);
    if (!acc_type_id) {
      return 0;
    }
   
    acc_id = strtoull (ptr, &ptr, 10);
    if (acc_id <= 0) {
      return 0;
    }

    acc_id_end = ptr;

    if (*ptr++ != ',') {
      return 0;
    }

    flags = strtoul (ptr, &ptr, 10);
    if (flags < 0) {
      return 0;
    }

    if (*ptr++ != ',') {
      return 0;
    }

    from = strtol (ptr, &ptr, 10);

    if (*ptr++ != ':') {
      return 0;
    }

    to = strtol (ptr, &ptr, 10);

    if (*ptr == ',') {
      char tmp [256];
      ++ptr;
      sign_time = strtoul (ptr, &ptr, 10);
      if (sign_time <= 0) {
        return 0;
      }

      if (sign_time < now - 25 || sign_time > now + 5) {
        return 0;
      }

      if (*ptr++ != '#') {
        return 0;
      }

      if (!parse_auth_code (ptr)) {
        return 0;
      }

      sprintf (tmp, "%.*s,%d", (int)(acc_id_end - (key + 20)), key + 20, sign_time);

      auth_code = check_auth_code (ptr, tmp, acc_type_id, acc_id);

      ptr += 32;
    }

    if (ptr != key + key_len) {
      return 0;
    }

    int res = get_account_transactions (acc_type_id, acc_id, flags, from, to, auth_code, RR, &R_cnt, RR_SIZE);

    if (res < 0) {
      return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
    }

    return return_one_key_list_long (c, key, key_len, res, 0, RR, R_cnt);
  }

  if (key_len >= 11 && !strncmp (key, "transaction", 11)) {
    char *ptr = (char *)(key + 11), *auth_signature = 0, *comment;
    long long transaction_id, *RRR;
    int sign_time = 0, R_cnt = 0, i;

    transaction_id = strtoull (ptr, &ptr, 10);
    if (transaction_id <= 0) {
      return 0;
    }
    if (*ptr == ',') {
      ++ptr;
      sign_time = strtoul (ptr, &ptr, 10);
      if (sign_time <= 0) {
        return 0;
      }

      if (sign_time < now - 25 || sign_time > now + 5) {
        return 0;
      }

      if (*ptr++ != '#') {
        return 0;
      }

      if (!parse_auth_code (ptr)) {
        return 0;
      }
      auth_signature = ptr;
      ptr += 32;
    }

    if (ptr != key + key_len) {
      return 0;
    }

    int res = get_transaction_data (transaction_id, sign_time, auth_signature, RR, &R_cnt, &comment);

    if (res <= 0) {
      return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
    }

    ptr = stats_buff + sprintf (stats_buff, "%d,%lld,%lld,%lld,%lld,%lld,%lld\n", res, RR[0], RR[1], RR[2], RR[3], RR[4], RR[5]);
    RRR = RR + 8;

    assert (res >= 2 && res <= MAX_TRANSACTION_PARTIES);

    for (i = 0; i < res; i++) {
      ptr = store_acc_id (ptr, (int)RRR[0], RRR[1]);
      ptr += sprintf (ptr, ",%lld,%lld", RRR[2], RRR[3]);
      RRR += 4;
      *ptr++ = (i < res - 1) ? ',' : '\n';
    }

    if (comment) {
      int L = strlen (comment);
      memcpy (ptr, comment, L);
      ptr += L;
    }

    assert (ptr - stats_buff <= STATS_BUFF_SIZE);

    return return_one_key (c, key, stats_buff, ptr - stats_buff);
  }

  if (key_len == 12 && !strncmp (key, "system_ready", 12)) {
    return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", global_engine_status));
  }

  if (key_len >= 14 && !strncmp (key, "account_ready:", 14)) {
    char *ptr;
    int acc_type_id, currency, x = global_engine_status;
    long long acc_id, locked;

    ptr = (char *)(key + 14);
    acc_type_id = parse_account_type (ptr, &ptr);
    if (!acc_type_id) {
      return 0;
    }
   
    acc_id = strtoull (ptr, &ptr, 10);
    if (acc_id <= 0) {
      return 0;
    }

    if (ptr != key + key_len) {
      return 0;
    }

    long long balance = get_balance (acc_type_id, acc_id, 0, &currency, &locked);
    if (balance == BALANCE_NO_ACCOUNT) {
      if (x >= 0) {
        x += 4;
      }
    }

    return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", x));
  }

  if (key_len >= 16 && !strncmp (key, "cancel_committed", 16)) {
    char *ptr = (char *)(key + 16), *auth_signature = 0, *oldptr, *tmp;
    long long transaction_id, new_transaction_id;
    int sign_time = 0;
    unsigned ip;

    transaction_id = strtoull (ptr, &ptr, 10);
    if (transaction_id <= 0) {
      return 0;
    }
    if (*ptr++ != ',') {
      return 0;
    }
    sign_time = strtoul (ptr, &ptr, 10);
    if (sign_time <= 0) {
      return 0;
    }

    if (sign_time < now - 25 || sign_time > now + 5) {
      return 0;
    }

    if (*ptr++ != ',') {
      return 0;
    }

    ip = strtoul (oldptr = ptr, &ptr, 10);
    if (ptr == oldptr) {
      return 0;
    }

    if (*ptr++ != '#') {
      return 0;
    }

    if (!parse_auth_code (ptr)) {
      return 0;
    }
    auth_signature = ptr;
    ptr += 32;

    if (*ptr++ != ';') {
      return 0;
    }

    if (ptr > key + key_len || key + key_len > ptr + 250) {
      return 0;
    }

    static char comment[256];

    tmp = ptr;
    ptr = comment;

    for (; tmp < key + key_len; tmp++) {
      if ((unsigned char)*tmp < 32 || *tmp == '&') {
        *ptr++ = 32;
      } else {
        *ptr++ = *tmp;
      }
    }
    *ptr = 0;

    assert (!key[key_len]);

    int res = do_cancel_committed (transaction_id, sign_time, auth_signature, ip, comment, &new_transaction_id);

    if (res < -16) {
      return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d:%d", res >> 4, 1 + (res & 15)));
    } else if (res != 1) {
      return return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", res));
    }

    return return_one_key (c, key, stats_buff, sprintf (stats_buff, "1:%lld", new_transaction_id));
  }

  if (key_len == 16 && !strncmp (key, "free_block_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, FreeCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (key_len == 16 && !strncmp (key, "used_block_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, UsedCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (key_len == 16 && !strncmp (key, "allocation_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, NewAllocations[0], MAX_RECORD_WORDS * 4);
    return 0;
  }

  if (key_len == 17 && !strncmp (key, "split_block_stats", 17)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, SplitBlocks, MAX_RECORD_WORDS);
    return 0;
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = money_prepare_stats ();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, key, stats_buff, len + len2);
    return 0;
  }

  return 0;
}


int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  c->query_start_time = get_utime (CLOCK_MONOTONIC);
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  c->last_query_time = get_utime (CLOCK_MONOTONIC) - c->query_start_time;
  total_get_time += c->last_query_time;
  get_queries++;
  write_out (&c->Out, "END\r\n", 5);
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get end: query time %.3fms\n", c->last_query_time * 1000);
  }
  return 0;
}



int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_incr: op=%d, key='%s', val=%lld\n", op, key, arg);
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  int temp_id;
  if (verbosity > 1) {
    fprintf (stderr, "memcache_delete: key='%s'\n", key);
  }

  if (sscanf (key, "transaction%d", &temp_id) >= 1 && temp_id > 0) {
    if (delete_temp_transaction (temp_id) > 0) {
      write_out (&c->Out, "DELETED\r\n", 9);
      return 0;
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
      return 0;
    }
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}


int money_engine_wakeup (struct connection *c) {
//  struct mcs_data *D = MCS_DATA(c);
  return 0;
}


int money_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "money_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return money_engine_wakeup (c);
}

/*
 *
 *	PARSE ARGS & INITIALIZATION
 *
 */


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
  if (global_engine_status != 1) {
    pending_signals |= (1 << SIGINT);
    return;
  }
  global_engine_status = 2;
  if (!quit_at) {
    quit_at = now + 3;
  }
  signal(SIGINT, sigint_handler);
}

static void sigterm_handler (const int sig) {
  if (global_engine_status != 1) {
    pending_signals |= (1 << SIGTERM);
    return;
  }
  global_engine_status = 2;
  if (!quit_at) {
    quit_at = now + 3;
  }
  signal(SIGTERM, sigterm_handler);
}

static void sighup_handler (const int sig) {
  fprintf (stderr, "got SIGHUP.\n");
  sync_binlog (2);
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf (stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs();
  sync_binlog (2);
  signal(SIGUSR1, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal(SIGPOLL, sigpoll_handler);
}

void cron (void) {
  flush_binlog_forced (0);
  scan_delay_queues ();
  flush_binlog_forced (0);
}

int sfd;

void start_server (void) { 
  char buf[64];
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigterm_handler);
  signal(SIGUSR1, sigusr1_handler);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGPOLL, sigpoll_handler);

  if (daemonize) {
    signal(SIGHUP, sighup_handler);
    reopen_logs();
  }

  if (daemonize) {
    setsid();
  }

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    kprintf ("cannot open server socket at port %d: %m\n", port);
    exit (3);
  }

  vkprintf (1, "created listening socket at %s:%d, fd=%d\n", conv_addr(settings_addr.s_addr, buf), port, sfd);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  init_listening_connection (sfd, &ct_money_engine_server, &memcache_methods);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  if (!global_engine_status) {
    global_engine_status = 1;
  }

  for (i = 0; !pending_signals; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }

    epoll_work (10);

    if (sigpoll_cnt > 0) {
      vkprintf (2, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      sigpoll_cnt = 0;
    }

    /* AIO */
    // check_all_aio_completions ();


    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (pending_signals) {
      break;
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (now > quit_at && quit_at) break;
  }

  fprintf (stderr, "Quitting.\n");

  epoll_close (sfd);
  close (sfd);

  flush_binlog_last ();
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-r] [-M] [-S<rem>,<mod>,<master-host>:<port>] [-p<port>] [-i<acc-id>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-t<cutoff-time>] [-l<log-name>] <index-file>\n"
  	  "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
	  "64-bit"
#else
	  "32-bit"
#endif
	  " after commit " COMMIT "\n"
	  "\tPerforms money transfer queries using given index\n"
	  "\t-v\toutput statistical and debug information into stderr\n"
//	  "\t-m<metafile-memory>\tmaximal size of metafile cache\n"
	  "\t-M\tenables master mode\n"
	  "\t-S<rem>,<mod>,<master-host>:<port>\tenables slave mode; connect to master at <master-host>:<port>\n"
	  "\t-H<heap-size>\tdefines maximum heap size\n"
	  "\t-B<max-binlog-size>\tdefines maximum size of each binlog file\n"
	  "\t-r\tread-only binlog (don't log new events)\n",
	  progname);
  exit(2);
}

int main (int argc, char *argv[]) {
  int i, cutoff_time = 0;
  long long x;
  char c;

  set_debug_handlers ();

  progname = argv[0];
  max_binlog_size = (1LL << 62);

  while ((i = getopt (argc, argv, "a:b:c:dhi:l:p:rt:u:vB:H:MS:")) != -1) {
    switch (i) {
    case 'v':
      verbosity++;
      break;
    case 'r':
      binlog_disabled = 1;  /* DEBUG only */
      break;
    case 'h':
      usage ();
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
    case 'u':
      username = optarg;
      break;
    case 'i':
      special_acc_id = atoll (optarg);
      break;
    case 'a':
      binlogname = optarg;
      break;
    case 'M':
      assert (!master_slave_mode);
      master_slave_mode = 1;
      break;
    case 'S':
      assert (!master_slave_mode);
      master_slave_mode = -1;
      i = -1;
      assert (sscanf (optarg, "%d,%d,%127[^:]:%d%n", &split_min, &split_mod, master_host, &master_port, &i) >= 4);
      assert (i >= 0 && !optarg[i]);
      assert (split_min >= 0 && split_min < split_mod && split_mod <= 10000);
      split_max = split_min + 1;
      assert (master_port > 0 && master_port < 65535);
      break;
    case 'l':
      logname = optarg;
      break;
    case 't':
      cutoff_time = atoi (optarg);
      break;
    case 'B': case 'H':
      c = 0;
      assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
      switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
      }
      if (i == 'B' && x >= 1024 && x < (1LL << 60)) {
        max_binlog_size = x;
      } else if (i == 'H' && x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (200LL << 30))) {
	dynamic_data_buffer_size = x;
      }
      break;
    case 'd':
      daemonize ^= 1;
      break;
    }
  }
  assert (!master_slave_mode || max_binlog_size == (1LL << 62));
  if (argc != optind + 1 && argc != optind + 2) {
    usage ();
    return 2;
  }

  if (master_slave_mode < 0) {
    vkprintf (1, "slave mode enabled: slice %d out of %d, master is at %s port %d\n", split_min, split_mod, master_host, master_port);
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit (1);
  }

  if (port < PRIVILEGED_TCP_PORTS && !index_mode) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      kprintf ("cannot open server socket at port %d: %m\n", port);
      exit (1);
    }
  }

  aes_load_pwd_file (0);

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }
  
  init_dyn_data ();
  log_ts_interval = 0;

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    kprintf ("cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;

    vkprintf (1, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    index_load_time = -get_utime (CLOCK_MONOTONIC);

    i = load_index (Snapshot);

    index_load_time += get_utime (CLOCK_MONOTONIC);

    if (i < 0) {
      fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
      exit(1);
    }

    vkprintf (1, "load index: done, jump_log_pos=%lld, alloc_mem=%ld out of %ld, time %.06lfs\n", 
	      jump_log_pos, (long) (dyn_cur - dyn_first), (long) (dyn_last - dyn_first), index_load_time);

    //close_snapshot (Snapshot, 1);
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  // Reading Binlog
  vkprintf (2, "starting reading binlog\n");

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  vkprintf (1, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  binlog_load_time = -get_utime (CLOCK_MONOTONIC);

  clear_log ();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);
  use_aio = -use_aio;

  vkprintf (1, "replay log events started\n");
  i = replay_log (0, 1);
 
  use_aio = -use_aio;
  vkprintf (1, "replay log events finished\n");

  binlog_load_time += get_utime (CLOCK_MONOTONIC);
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (!binlog_disabled) {
    clear_read_log();
  }

  clear_write_log ();
  start_time = time (NULL);

  if (index_mode) {
    return write_index (0);
  }

  binlog_crc32_verbosity_level = 6;
  start_server();

  return 0;
}
