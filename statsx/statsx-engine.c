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

    Copyright 2009-2013 Vkontakte Ltd
              2009-2010 Nikolai Durov
              2009-2010 Andrei Lopatin
              2011-2013 Vitaliy Valtman
              2011-2013 Anton Maydell
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

#include "net-aio.h"
#include "md5.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "kdb-statsx-binlog.h"
#include "statsx-data.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "kdb-data-common.h"
#include "kfs.h"
#include "am-stats.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"
#include "statsx-tl.h"
#include "statsx-interface-structures.h"

#define	VERSION_STR	"statsx-engine-0.12-r1"
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
;

#define TCP_PORT 11211

/*
 *
 *		STATISTICS ENGINE
 *
 */

int verbosity = 0;
char *progname = "statsx-engine", *username, *binlogname, *logname;
char metaindex_fname_buff[256], binlog_fname_buff[256];
int default_timezone = 12;
extern int memcache_auto_answer_mode;
int auto_create_new_versions;
int custom_version_names;
int monthly_stat;
int index_fix_mode;
//char config_filename[512];

/* stats counters */
int start_time;
long long binlog_loaded_size;
double binlog_load_time, index_load_time;

#define STATS_BUFF_SIZE	(1 << 20)
char stats_buff[STATS_BUFF_SIZE];

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 1, test_mode = 0;
struct in_addr settings_addr;
int active_connections;
volatile int sigpoll_cnt;


long long memory_to_index = MEMORY_TO_INDEX;

int idx_fd;
long long jump_log_pos;
int jump_log_ts;
int jump_log_crc32;
long long snapshot_size;
int index_mode = 0;
int reverse_index_mode;
long long reverse_index_pos;
int ignore_set_timezone;
int create_day_start;
int udp_enabled;

int statsx_engine_wakeup (struct connection *c);
int statsx_engine_alarm (struct connection *c);


conn_type_t ct_stats_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "statsx_engine_server",
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
  .wakeup = statsx_engine_wakeup,
  .alarm = statsx_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_get_start (struct connection *c);
int memcache_get_end (struct connection *c, int key_count);
int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

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

struct rpc_server_functions rpc_methods = {
  .execute = default_tl_rpcs_execute,
  .check_ready = server_check_ready,
  .flush_packet = rpcs_flush_packet,
  .rpc_check_perm = rpcs_default_check_perm,
  .rpc_init_crypto = rpcs_init_crypto,
  .rpc_close = default_tl_close_conn,
  .memcache_fallback_type = &ct_stats_engine_server,
  .memcache_fallback_extra = &memcache_methods
};

int quit_steps;

static double percent (double x, double y) { return safe_div (100.0 * x, y); }

extern int index_size;
int stats_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF + AM_GET_MEMORY_USAGE_OVERALL);  
  SB_BINLOG;
  SB_INDEX;

  sb_printf (&sb,
      "snapshot_loading_average_blocking_read_bytes\t%.6f\n"
      "snapshot_loading_blocking_read_calls\t%d\n"
      "tot_user_metafiles\t%d\n"
      "tot_user_metafile_bytes\t%lld\n"
      "counters\t%d\n"
      "counters_percent\t%.6f\n"
      "counters_prime\t%d\n"
      "total_views\t%lld\n"
      "tree_nodes_allocated\t%d\n"
      "counter_instances\t%d\n"
      "counter_instances_percent\t%.6f\n"
      "allocated_counter_instances\t%d\n"
      "deleted_by_LRU\t%lld\n"
      "allocated_memory\t%lld\n"
      "tot_aio_queries\t%lld\n"
      "active_aio_queries\t%lld\n"
      "expired_aio_queries\t%lld\n"
      "avg_aio_query_time\t%.6f\n"
      "aio_bytes_loaded\t%lld\n"
      "tot_aio_queries\t%lld\n"
      "tot_aio_fails\t%lld\n"
      "memory_to_index\t%lld\n"
      "version\t%s\n",
    snapshot_loading_average_blocking_read_bytes,
    snapshot_loading_blocking_read_calls,
    tot_user_metafiles,
    tot_user_metafile_bytes,
    tot_counters,
    percent (tot_counters, max_counters),
    counters_prime,
    tot_views,
    alloc_tree_nodes,
    tot_counter_instances,
    percent (tot_counter_instances, index_size),
    tot_counters_allocated,
    deleted_by_lru,
    tot_memory_allocated,
    tot_aio_queries,
    active_aio_queries,
    expired_aio_queries,
    tot_aio_queries > 0 ? total_aio_time / tot_aio_queries : 0,
    tot_aio_loaded_bytes,
    tot_aio_queries,
    tot_aio_fails,
    memory_to_index,
    FullVersionStr
    );
  return sb.pos;
}

inline int not_found (struct connection *c) {
  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}


int memcache_delete (struct connection *c, const char *key, int key_len) {
  const int dog_len = get_at_prefix_length (key, key_len);
  key += dog_len;
  key_len -= dog_len;
  int counter_id;
  if (sscanf (key, "counter%d", &counter_id) >= 1) {
    delete_counter (counter_id, 0);
    write_out (&c->Out, "DELETED\r\n", 9);
    return 0;
  }
  return not_found (c);
}

inline int char3_to_int (char *a) {
  return (((int) a[0]) << 16) + (((int) a[1]) << 8) + a[2];
}

void debug_key (const char *key, int len) {
  int i;
  for (i = 0; i < len; i++) {
    fputc (key[i], stderr);
  }
}

void debug_error (const char *szAction, const char *szMsg, const char *key, int len) {
  if (!verbosity) { return; }
  fprintf (stderr, "Bad %s (key=\"", szAction);
  debug_key (key, len);
  fprintf (stderr, "\"); %s\n", szMsg);
}

double aio_t = 0.5;

int memcache_wait (struct connection *c) {
  if (verbosity > 2) {
    fprintf (stderr, "wait for aio..\n");
  }
  if (c->flags & C_INTIMEOUT) {
    if (verbosity > 1) {
      fprintf (stderr, "memcache_get: IN TIMEOUT (%p)\n", c);
    }
    WaitAioArrClear ();
    return 0;
  }
  if (c->Out.total_bytes > 8192) {
    c->flags |= C_WANTWR;
    c->type->writer (c);
  }
  
  if (!WaitAioArrPos) {
    fprintf (stderr, "WaitAio=0 - no memory to load user metafile, query dropped.\n");
    return 0;
  }
  
  int i;
  for (i = 0; i < WaitAioArrPos; i++) {
    conn_schedule_aio (WaitAioArr[i], c, 1.1 * aio_t, &aio_metafile_query_type);
  }

  set_connection_timeout (c, aio_t);
  WaitAioArrClear ();
  return 0;
}


int incr_version = -1;
long long incr_counter_id;
int incr_version_read;

int memcache_incr (struct connection *c, int op, const char *key, int len, long long arg) {
  if (verbosity >= 4) {
    fprintf (stderr, "memcache_incr (op = %d, key = \"", op);
    debug_key (key, len);
    fprintf (stderr, "\")\n");
  }

  const int dog_len = get_at_prefix_length (key, len);
  key += dog_len;
  len -= dog_len;

  if (len >= 7 && !memcmp (key, "counter", 7)) {
    long long cnt_id, tmp;
    int  subcnt_id=-1,  uid, city = 0, res;
    char sex = 0, age = 0, status = 0, polit = 0, section = 0, region[4], country[4], source = 0, *p;
    char optional_params_is_given = 1;
    int version = -1;
    memset (region, 0, sizeof (region));
    memset (country, 0, sizeof (country));
    errno = 0; tmp = strtoll (key + 7, &p, 10);
    if (errno) {
      debug_error ("incr", "fail to parse counter_id", key, len);
      return not_found (c);
    }
    cnt_id = tmp;
    if (*p == '@') {
      errno = 0; tmp = strtol(p + 1, &p, 10);
      if (errno || tmp < 0) {
        debug_error ("incr", "fail to parse version",  key, len);
        return not_found (c);
      }
      if (verbosity >= 3) {
        fprintf (stderr, "version = %d\n", version);
      }
      version = (int) tmp;
    }
    if (*p == ':') {
      errno = 0; tmp = strtol(p+1, &p, 10);
      if (errno) {
        debug_error ("incr", "fail to parse subcnt_id",  key, len);
        return not_found (c);
      }
      subcnt_id = (int) tmp;
    }
    if (*p != '#') {
      debug_error ("incr", "missed uid (expected '#')", key, len);
      return not_found (c);
    }
    errno = 0; tmp = strtol (p+1, &p, 10);
    if (errno) {
      debug_error ("incr", "fail to parse uid", key, len);
      return not_found (c);
    }
    uid = (int) tmp;
    if (*p && *p != '#') {
      debug_error ("incr", "expected '#' after uid", key, len);
      return not_found (c);
    }
    if (verbosity >= 4) {
      fprintf (stderr, "incr (cnt_id = %lld, subcnt_id = %d, uid = %d)\n", cnt_id, subcnt_id, uid);
    }
    if (!(*p)) optional_params_is_given = 0;
    if (*p) p++;
    if (*p) sex = *p++;
    if (*p) age = *p++;
    if (*p) status = *p++;
    if (*p) polit = *p++;
    if (*p) section = *p++;
    if (*p == ';') {
      sscanf (p+1,"%d;%3[^;];%3[^;];%c", &city, region, country, &source);
    }
    if (sex > '0' && sex <= '2') { sex -= '0'; } else { sex = 0; }
    if (age > '0' && age <= '8') { age -= '0'; } else { age = 0; }
    if (status > '0' && status <= '8') { status -= '0'; } else { status = 0; }
    if (polit > '0' && polit <= '8') { polit -= '0'; } else { polit = 0; }
    if (section >= 'A' && section <= 'P') { section -= 'A' - 1; } else { section = 0; }
    if (source >= 'A' && source <= 'P') { source -= 'A' - 1; } else { source = 0; }
    if (verbosity >= 4) {
      fprintf (stderr, "optional_params_is_given = %d\n", optional_params_is_given);
      fprintf (stderr, "sex = %d, age = %d, status = %d, polit = %d, section = %d, city = %d, region = %d, country = %d, source = %d\n",
             sex, age, status, polit, section, city, char3_to_int(region), char3_to_int(country),(int) source);
    }
    //int counter_incr_ext (int counter_id, int user_id, int replaying, int op, int subcnt, int sex, int age, int m_status, int polit_views, int section, int city, int country, int geoip_country, int source);
    incr_version = version;
    incr_counter_id = cnt_id;
    incr_version_read = 0;
    if ((version >= 0 && !custom_version_names) || (version < 0 && custom_version_names)) {
      debug_error ("incr", "fail due to version",  key, len);
      return not_found (c);
    }
    res = (optional_params_is_given && subcnt_id == -1) ?
           counter_incr_ext (cnt_id, uid, 0, op, subcnt_id, sex, age, status, polit, section, city, char3_to_int(region), char3_to_int(country), source) :
           counter_incr (cnt_id, uid, 0, op, subcnt_id);
    //int counter_incr (int counter_id, int user_id, int replaying, int op, int subcnt);
    if (res < 0) return not_found (c);
    write_out (&c->Out, stats_buff, sprintf (stats_buff, "%d\r\n", res));
    return 0;
  }
  return not_found (c);
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  return -2;
}

int memcache_stats (struct connection *c) {
  int len = stats_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int parse_countid_with_version(const char *s, long long *pcount_id, int *pver) {
  *pver = -1;
  if (strchr(s, '@') != 0) {
    return sscanf(s, "%lld@%d", pcount_id, pver) == 2;
  }
  return sscanf(s, "%lld", pcount_id) == 1;
}

int Q_raw;

int memcache_get (struct connection *c, const char *key, int len) {
  char *ptr;
  //char timezone[32];
  long long cnt_id;
  if (verbosity >= 4) {
    fprintf (stderr, "memcache_get (key = \"");
    debug_key (key, len);
    fprintf (stderr, "\")\n");
  }
  int dog_len = get_at_prefix_length (key, len);
  key += dog_len;
  len -= dog_len;

  Q_raw = 0;
  if (len > 0 && *key == '%') {
    dog_len ++;
    key ++;
    len --;
    Q_raw = 1;
  }


  if (len > 5 && !strncmp (key, "views", 5)) {
    int ver;
    if (!parse_countid_with_version (key + 5, &cnt_id, &ver)) {
      debug_error ("get", "couldn't parse count_id&version", key, len);
      return not_found (c);
    }
    //int res = get_counter_views(cnt_d, ver); TODO!!!
    int res = (ver == -1) ? get_counter_views (cnt_id) : get_counter_views_given_version (cnt_id,ver);
    if (res == -2) {
      return memcache_wait (c);
    }
    if (res >= 0) {
      //int return_one_key (struct connection *c, const char *key, char *val, int vlen) {
      if (!Q_raw) {
        return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        *(int *)stats_buff = res;
        return_one_key (c, key - dog_len, stats_buff, sizeof (int));
      }
    }
    return 0;
  }

  if (len > 8 && !strncmp (key, "visitors", 8)) {
    int ver;
    if (!parse_countid_with_version (key + 8, &cnt_id, &ver)) {
      debug_error ("get","couldn't parse count_id&version",key, len);
      return not_found(c);
    }
    //int res = get_counter_visitors(cnt_id, ver); TODO !!!
    int res = (ver == -1) ? get_counter_visitors (cnt_id) : get_counter_visitors_given_version (cnt_id, ver);
    if (res == -2) {
      return memcache_wait (c);
    }
    if (res >= 0) {
      //int return_one_key (struct connection *c, const char *key, char *val, int vlen) {
      if (!Q_raw) {
        return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        *(int *)stats_buff = res;
        return_one_key (c, key - dog_len, stats_buff, sizeof (int));
      }
    }
    return 0;
  }

  if (len > 14 && !strncmp (key, "enable_counter", 14)) {
    cnt_id = strtoll (key + 14, &ptr, 10);
    if (ptr > key + 14 && !*ptr) {
      int res = enable_counter (cnt_id, 0);
      if (res >= 0) {
      if (!Q_raw) {
        return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        *(int *)stats_buff = res;
        return_one_key (c, key - dog_len, stats_buff, sizeof (int));
      }
      }
      return 0;
    }
  }

  if (len > 15 && !strncmp (key, "disable_counter", 15)) {
    cnt_id = strtoll (key + 15, &ptr, 10);
    if (ptr > key + 15 && !*ptr) {
      int res = disable_counter (cnt_id, 0);
      if (!Q_raw) {
        return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        *(int *)stats_buff = res;
        return_one_key (c, key - dog_len, stats_buff, sizeof (int));
      }
      return 0;
    }
  }

  if (len > 12 && !strncmp(key, "set_timezone", 12)) {
    int tz = 0;
    //if (2 == sscanf(key+12,"%d#%31s",&cnt_id,timezone)) {
    if (2 == sscanf(key + 12,"%lld#%d", &cnt_id, &tz)) {
      tz = tz + 12 + 4;
      if (tz < 0) {
        return 0;
      }
      int res = set_timezone (cnt_id, tz, 0);
      if (!Q_raw) {
        return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        *(int *)stats_buff = res;
        return_one_key (c, key - dog_len, stats_buff, sizeof (int));
      }
      return 0;
    }
  }

  if (len > 8 && !strncmp(key, "timezone", 8)) {
    if (1 == sscanf(key + 8, "%lld", &cnt_id)) {
      int res = get_timezone (cnt_id);
      if (!Q_raw) {
        return_one_key (c, key - dog_len, stats_buff, sprintf (stats_buff, "%d", res));
      } else {
        *(int *)stats_buff = res;
        return_one_key (c, key - dog_len, stats_buff, sizeof (int));
      }
      return 0;
    }
  }


  if (len > 7 && !strncmp (key, "counter", 7)) {
    int ver = 0;
    if (sscanf (key, "counter%lld@%d", &cnt_id, &ver) >= 1) {
      int to_serialize = key[strlen(key) - 1] != '?';
      if (verbosity >= 4) {
        fprintf(stderr, "cnt_id = %lld, ver = %d\n", cnt_id, ver);
      }
      int res = get_counter_serialized (stats_buff, cnt_id, ver);
      if (res == -2) {
        return memcache_wait (c);
      }
      if (res > 0) {
        if (to_serialize && !Q_raw) {
          write_out (&c->Out, stats_buff+res, sprintf (stats_buff+res, "VALUE %s 1 %d\r\n", key - dog_len, res));
        } else {
          write_out (&c->Out, stats_buff+res, sprintf (stats_buff+res, "VALUE %s 0 %d\r\n", key - dog_len, res));
        }
        write_out (&c->Out, stats_buff, res);
        write_out (&c->Out, "\r\n", 2);
      }
      return 0;
    }
  }

  if (len > 16 && !strncmp (key, "monthly_visitors", 16)) {
    cnt_id = 0;
    if (sscanf (key, "monthly_visitors%lld", &cnt_id) >= 1) {
      int res = get_monthly_visitors_serialized (stats_buff, cnt_id);
      if (res == -2) {
        return memcache_wait (c);
      }
      if (res >= 0) {
        return_one_key (c, key - dog_len, stats_buff, res);
      }
      return 0;
    }
  }

  if (len > 13 && !strncmp (key, "monthly_views", 13)) {
    cnt_id = 0;
    if (sscanf (key, "monthly_views%lld", &cnt_id) >= 1) {
      int res = get_monthly_views_serialized (stats_buff, cnt_id);
      if (res == -2) {
        return memcache_wait (c);
      }
      if (res >= 0) {
        return_one_key (c, key - dog_len, stats_buff, res);
      }
      return 0;
    }
  }

  if (len > 12 && !strncmp (key, "counters_sum", 12)) {
    int start_id = 0, finish_id = 0, id = 0;
    int to_serialize = key[strlen(key) - 1] != '?';
    if (sscanf (key, "counters_sum%d_%d_%d", &id, &start_id, &finish_id)) {
      struct counter *C = get_counters_sum (id, start_id, finish_id);
      if (C == (void *)-2l) { 
        return memcache_wait (c);
      }
      if (C) {
        int res = counter_serialize (C, stats_buff);
        assert (res >= 0);
        if (to_serialize && !Q_raw) {
          write_out (&c->Out, stats_buff+res, sprintf (stats_buff+res, "VALUE %s 1 %d\r\n", key - dog_len, res));
        } else {
          write_out (&c->Out, stats_buff+res, sprintf (stats_buff+res, "VALUE %s 0 %d\r\n", key - dog_len, res));
        }
        write_out (&c->Out, stats_buff, res);
        write_out (&c->Out, "\r\n", 2);
      }
    }
  }
  /*
  if (len > 7 && !strncmp (key, "counter", 7)) {
    int cnt_id, subcnt_id = -1, ver = 0;
    if (sscanf (key, "counter%d:%d@%d", &cnt_id, &subcnt_id, &ver) >= 2) {
      //int res = get_counter_serialized (stats_buff, cnt_id, subcnt_id, ver); TODO !!!
      int res = get_counter_serialized (stats_buff, cnt_id, ver);
      if (res > 0) {
        write_out (&c->Out, stats_buff+res, sprintf (stats_buff+res, "VALUE %s 1 %d\r\n", key, res));
        write_out (&c->Out, stats_buff, res);
        write_out (&c->Out, "\r\n", 2);
      }
      return 0;
    }
    if (sscanf (key, "counter%d@%d", &cnt_id, &ver) == 2) {
      //int res = get_counter_serialized (stats_buff, cnt_id, -1, ver); TODO !!!
      int res = get_counter_serialized (stats_buff, cnt_id, ver);
      if (res > 0) {
        write_out (&c->Out, stats_buff+res, sprintf (stats_buff+res, "VALUE %s 1 %d\r\n", key, res));
        write_out (&c->Out, stats_buff, res);
        write_out (&c->Out, "\r\n", 2);
      }
      return 0;
    }
  }
  */

  if (len > 8 && !strncmp (key, "versions", 8)) {
    cnt_id = strtoll (key + 8, &ptr, 10);
    if (ptr > key+8 && !*ptr) {
      int res = get_counter_versions (stats_buff, cnt_id);
      if (res == -2) {
        return memcache_wait (c);
      }
      if (res > 0) {
        write_out (&c->Out, stats_buff + res, sprintf (stats_buff + res, "VALUE %s 0 %d\r\n", key - dog_len, res));
        write_out (&c->Out, stats_buff, res);
        write_out (&c->Out, "\r\n", 2);
      }
      return 0;
    }
  }

  if (len >= 16 && !strncmp (key, "free_block_stats", 16)) {
    return_one_key_list (c, key - dog_len, len + dog_len, MAX_RECORD_WORDS, 0, FreeCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (len >= 16 && !strncmp (key, "used_block_stats", 16)) {
    return_one_key_list (c, key - dog_len, len + dog_len, MAX_RECORD_WORDS, 0, UsedCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (len >= 16 && !strncmp (key, "allocation_stats", 16)) {
    return_one_key_list (c, key - dog_len, len + dog_len, MAX_RECORD_WORDS, 0, NewAllocations[0], MAX_RECORD_WORDS * 4);
    return 0;
  }

  if (len >= 17 && !strncmp (key, "split_block_stats", 17)) {
    return_one_key_list (c, key - dog_len, len + dog_len, MAX_RECORD_WORDS, 0, SplitBlocks, MAX_RECORD_WORDS);
    return 0;
  }

  if (len >= 5 && !strncmp (key, "stats", 5)) {
    return_one_key (c, key - dog_len, stats_buff, stats_prepare_stats (c));
    return 0;
  }

  return 0;
}


int statsx_engine_wakeup (struct connection *c) {
  struct mcs_data *D = MCS_DATA(c);

  if (verbosity > 1) {
    fprintf (stderr, "statsx_engine_wakeup (%p)\n", c);
  }

  c->generation = ++conn_generation;
  c->pending_queries = 0;
  c->status = conn_reading_query;
  assert (D->query_type == mct_get_resume);
  clear_connection_timeout (c);

  if (c->Out.total_bytes > 0) {
    c->flags |= C_WANTWR;
  }

  return 0;
}


int statsx_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "statsx_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return statsx_engine_wakeup (c);
}

TL_DO_FUN(incr)
  if (custom_version_names) {
    incr_version = e->custom_version;
    incr_counter_id = e->counter_id;
    incr_version_read = 0;
  }
  int res = (e->mode) ? 
           counter_incr_ext (e->counter_id, e->user_id, 0, e->op, -1, e->sex, e->age, e->mstatus, e->polit, e->section, e->city, e->geoip_country, e->country, e->source) :
           counter_incr (e->counter_id, e->user_id, 0, e->op, -1);
  if (res == -2) { return -2; }
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(incr,int op,int custom_version)
  e->op = op;
  e->counter_id = tl_fetch_long ();
  if (custom_version) {
    if (!custom_version_names) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Can not incr version. Maybe [-x] key missed?");
      return 0;
    }
    e->custom_version = tl_fetch_int ();
  } else {
    if (custom_version_names) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Can not incr without version. (custom version mode enabled)");
      return 0;
    }
  }
  e->user_id = tl_fetch_int ();
  e->mode = tl_fetch_int ();
  e->sex = (e->mode & TL_STATSX_SEX) ? tl_fetch_int () : 0;
  e->age = (e->mode & TL_STATSX_AGE) ? tl_fetch_int () : 0;
  e->mstatus = (e->mode & TL_STATSX_MSTATUS) ? tl_fetch_int () : 0;
  e->polit = (e->mode & TL_STATSX_POLIT) ? tl_fetch_int () : 0;
  e->section = (e->mode & TL_STATSX_SECTION) ? tl_fetch_int () : 0;
  e->city = (e->mode & TL_STATSX_CITY) ? tl_fetch_int () : 0;
  static char buf[6];
  if (e->mode & TL_STATSX_GEOIP_COUNTRY) {
    int l = tl_fetch_string0 (buf, 3);    
    if (l < 0) { return 0; }
    if (l > 3) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "geoip_country should have at most 3 characters (presented %d)", l);
      return 0;
    }
    while (l < 3) {
      buf[++l] = 0;
    }
    e->geoip_country = char3_to_int (buf);
  } else {
    e->geoip_country = 0;
  }
  if (e->mode & TL_STATSX_COUNTRY) {
    int l = tl_fetch_string0 (buf, 3);
    if (l < 0) { return 0; }
    if (l > 3) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "country should have at most 3 characters (presented %d)", l);
      return 0;
    }
    while (l < 3) {
      buf[++l] = 0;
    }
    e->country = char3_to_int (buf);
  } else {
    e->country = 0;
  }
  e->source = (e->mode & TL_STATSX_SOURCE) ? tl_fetch_int () : 0;
TL_PARSE_FUN_END

TL_DO_FUN(incr_subcnt)
  if (custom_version_names) {
    incr_version = e->custom_version;
    incr_counter_id = e->counter_id;
    incr_version_read = 0;
  }
  int res = counter_incr (e->counter_id, 0, 0, e->op, e->subcnt_id);
  if (res == -2) { return -2; }        
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(incr_subcnt,int op,int custom_version)
  e->op = op;
  e->counter_id = tl_fetch_long ();
  if (custom_version) {
    if (!custom_version_names) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Can not incr version. Maybe [-x] key missed?");
      return 0;
    }
    e->custom_version = tl_fetch_int ();
  } else {
    if (custom_version_names) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Can not incr without version. (custom version mode enabled)");
      return 0;
    }
  }
  e->subcnt_id = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(get_views)
  int res = (e->version == -1) ? get_counter_views (e->counter_id) : get_counter_views_given_version (e->counter_id, e->version);
  if (res == -2) { return -2; }
  if (res < 0) { 
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(get_views,int version)
  e->counter_id = tl_fetch_long ();
  e->version = version ? tl_fetch_int () : -1;
TL_PARSE_FUN_END

TL_DO_FUN(get_visitors)
  int res = (e->version == -1) ? get_counter_visitors (e->counter_id) : get_counter_visitors_given_version (e->counter_id, e->version);
  if (res == -2) { return -2; }
  if (res < 0) { 
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_PARSE_FUN(get_visitors,int version)
  e->counter_id = tl_fetch_long ();
  e->version = version ? tl_fetch_int () : -1;
TL_PARSE_FUN_END

TL_DO_FUN(enable_counter)
  int res = (e->disable ? disable_counter : enable_counter) (e->counter_id, 0);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(enable_counter,int disable)
  e->counter_id = tl_fetch_long ();
  e->disable = disable;
TL_PARSE_FUN_END

TL_DO_FUN(set_timezone)
  int res = set_timezone (e->counter_id, e->offset, 0);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_PARSE_FUN(set_timezone,void)
  e->counter_id = tl_fetch_long ();
  e->offset = tl_fetch_int ();
TL_PARSE_FUN_END

TL_DO_FUN(get_counter)
  int res = tl_get_counter_serialized (e->counter_id, e->version, e->mode);
  if (res == -2) { return -2; }
TL_DO_FUN_END

TL_PARSE_FUN(get_counter,int mask, int version)
  e->counter_id = tl_fetch_long ();
  e->mode = mask ? tl_fetch_int () : 262143;
  e->version = version ? tl_fetch_int () : 0;
TL_PARSE_FUN_END

TL_DO_FUN(get_versions)
  int res = tl_get_counter_versions (e->counter_id);
  if (res == -2) { return -2; }
TL_DO_FUN_END

TL_PARSE_FUN(get_versions,void)
  e->counter_id = tl_fetch_long ();
TL_PARSE_FUN_END

TL_DO_FUN(get_monthly_visitors)
  int res = tl_get_monthly_visitors_serialized (e->counter_id);
  if (res == -2) { return -2; }
TL_DO_FUN_END

TL_PARSE_FUN(get_monthly_visitors,void)
  e->counter_id = tl_fetch_long ();
TL_PARSE_FUN_END

TL_DO_FUN(get_monthly_views)
  int res = tl_get_monthly_views_serialized (e->counter_id);
  if (res == -2) { return -2; }
TL_DO_FUN_END

TL_PARSE_FUN(get_monthly_views,void)
  e->counter_id = tl_fetch_long ();
TL_PARSE_FUN_END

TL_DO_FUN(get_counters_sum)
  struct counter *C = get_counters_sum (e->counter_id, e->from, e->to);
  if (C == (void *)-2l) { 
    return -2;
  }
  if (!C) {
    tl_store_int (TL_MAYBE_FALSE);    
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_serialize_counter (C, 31 + 256 + 512 + 2048 + 16384);
  }
TL_DO_FUN_END

TL_PARSE_FUN(get_counters_sum,void)
  e->counter_id = tl_fetch_long ();
  e->from = tl_fetch_int ();
  e->to = tl_fetch_int ();
TL_PARSE_FUN_END

struct tl_act_extra *statsx_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Statsx only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_STATSX_INCR:
    return tl_incr (0, 0);
  case TL_STATSX_INCR_VERSION:
    return tl_incr (0, 1);
  case TL_STATSX_DECR:
    return tl_incr (1, 0);
  case TL_STATSX_DECR_VERSION:
    return tl_incr (1, 1);
  case TL_STATSX_INCR_SUBCNT:
    return tl_incr_subcnt (0, 0);
  case TL_STATSX_INCR_SUBCNT_VERSION:
    return tl_incr_subcnt (0, 1);
  case TL_STATSX_DECR_SUBCNT:
    return tl_incr_subcnt (1, 0);
  case TL_STATSX_DECR_SUBCNT_VERSION:
    return tl_incr_subcnt (1, 1);
  case TL_STATSX_GET_VIEWS:
    return tl_get_views (0);
  case TL_STATSX_GET_VIEWS_VERSION:
    return tl_get_views (1);
  case TL_STATSX_GET_VISITORS:
    return tl_get_visitors (0);
  case TL_STATSX_GET_VISITORS_VERSION:
    return tl_get_visitors (1);
  case TL_STATSX_ENABLE_COUNTER:
    return tl_enable_counter (0);
  case TL_STATSX_DISABLE_COUNTER:
    return tl_enable_counter (1);
  case TL_STATSX_SET_TIMEZONE:
    return tl_set_timezone ();
  case TL_STATSX_GET_COUNTER:
    return tl_get_counter (0, 0);
  case TL_STATSX_GET_COUNTER_VERSION:
    return tl_get_counter (0, 1);
  case TL_STATSX_GET_COUNTER_MASK:
    return tl_get_counter (1, 0);
  case TL_STATSX_GET_COUNTER_MASK_VERSION:
    return tl_get_counter (1, 1);
  case TL_STATSX_GET_VERSIONS:
    return tl_get_versions ();
  case TL_STATSX_GET_MONTHLY_VISITORS:
    return tl_get_monthly_visitors ();
  case TL_STATSX_GET_MONTHLY_VIEWS:
    return tl_get_monthly_views ();
  case TL_STATSX_GET_COUNTERS_SUM:
    return tl_get_counters_sum ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
}

void statsx_stats (void) {
  stats_prepare_stats (0);
  tl_store_stats (stats_buff, 0);
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

volatile int sighup_cnt = 0, sigusr1_cnt = 0;

static void sighup_handler (const int sig) {
  static const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  sighup_cnt++;
  signal (SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  static const char message[] = "got SIGUSR1.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);
  sigusr1_cnt++;
  signal (SIGUSR1, sigusr1_handler);
}

static void sigpoll_handler(const int sig) {
  sigpoll_cnt++;
  signal(SIGPOLL, sigpoll_handler);
}

void cron (void) {
  static int last_viewcnt_flush = 0;
  if (now >= last_viewcnt_flush + 60) {
    flush_view_counters();
    last_viewcnt_flush = now - now % 60;
  }
  while (tot_counters_allocated >= MAX_COUNTERS_ALLOCATED && free_LRU()) {
  }
  //  check_all_aio_completions ();

  //flush_binlog();
}

int sfd;

void start_server (void) {
  char buf[64];
  int i;
  int prev_time;
  int old_sigusr1_cnt = 0, old_sighup_cnt = 0;

  init_epoll();
  init_netbuffers();
  if (udp_enabled) {
    init_msg_buffers (0);
  }

  if (verbosity >= 4) {
    fprintf (stderr, "init netbuffers done\n");
  }

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
    exit(3);
  }

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr(settings_addr.s_addr, buf), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user(username) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  if (verbosity >= 4) {
    fprintf (stderr, "User changer successfully\n");
  }


  if (binlogname && !binlog_disabled) {
    if (!binlog_cyclic_mode) {
      assert (append_to_binlog (Binlog) == log_readto_pos);
    } else {
      assert (append_to_binlog (Binlog) >= log_readto_pos);
    }
  }
  
  tl_parse_function = statsx_parse_function;
  tl_stat_function = statsx_stats;
  tl_aio_timeout = 0.5;

  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  if (verbosity >= 4) {
    fprintf (stderr, "Setting signals handlers...\n");
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);

  if (verbosity >= 4) {
    fprintf (stderr, "Signals handlers set\n");
  }

  if (daemonize) {
    signal(SIGHUP, sighup_handler);
    reopen_logs();
  }

  sigpoll_cnt = 0;

  for (i = 0; !pending_signals ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    epoll_work (53);

    tl_restart_all_ready ();
    
    if (!binlog_cyclic_mode) {
      flush_binlog ();
    } else {
      flush_cbinlog (0);
    }

    if (old_sighup_cnt != sighup_cnt) {
      old_sighup_cnt = sighup_cnt;
      vkprintf (1, "start_server: sighup_cnt = %d.\n", old_sighup_cnt);
      sync_binlog (2);
    }

    if (old_sigusr1_cnt != sigusr1_cnt) {
      old_sigusr1_cnt = sigusr1_cnt;
      vkprintf (1, "start_server: sigusr1_cnt = %d.\n", old_sigusr1_cnt);
      sync_binlog (2);
      reopen_logs ();
    }

    if (sigpoll_cnt != 0) {
      if (verbosity > 1) {
        fprintf (stderr, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      }
      sigpoll_cnt = 0;
    }

    check_all_aio_completions ();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close(sfd);

  flush_view_counters ();
  if (!binlog_cyclic_mode) {
    flush_binlog_last ();
    sync_binlog (2);
  } else {
    flush_cbinlog (2);
  }
  kprintf ("Terminated (pending_signals = 0x%llx).\n", pending_signals);
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  printf ("usage: %s [-v] [-r] [-i] [-T] [-y] [-x] [-d] [-D] [-E] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] [-M<memory-to-index>] [-t] <binlog>\n"
	  "\t%s\n"
    "\tPerforms statistical queries using given indexes\n",
	  progname,
    FullVersionStr);
  parse_usage ();
  exit (2);
}

int f_parse_option (int val) {
  switch (val) {
  case 'f':
    memcache_auto_answer_mode ++;
    break;
  case 'T':
    test_mode ++;
    break;
  case 'm':
    mode_ignore_user_id = 1;
    break;
  case 'i':
    index_mode = 1;
    break;
  case 'M':
    memory_to_index = atoi(optarg) * (long long) 1024 * 1024;
    break;
  case 'y':
    auto_create_new_versions = 0;
    break;
  case 'x':
    custom_version_names = 1;
    auto_create_new_versions = 0;
    break;
  case 'E':
    monthly_stat ++;
    break;
  case 't':
    ignore_set_timezone ++;
    break;
  case 'D':
    create_day_start ++;
    break;
  case 'P':
    if ((sscanf (optarg, "%lf", &max_counters_growth_percent) != 1) || max_counters_growth_percent < 0.1) {
      kprintf ("Illegal -P option: %s\n", optarg);
      exit (1);
    }
    break;
  case 'S':
    default_timezone = atoi (optarg);
    break;
  case 1000:
    binlog_cyclic_mode = 1;
    break;
  default:
    return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;
  /*
  strcpy(config_filename, "startx-engine.rc");
  */

  set_debug_handlers ();
  progname = argv[0];

  index_mode = 0;
  if (strstr (progname, "statsx-index") != NULL) {
    index_mode = 1;
  }

  auto_create_new_versions = 1;

  parse_option (0, no_argument, 0, 'f', "=memcache_auto_answer_mode");
  parse_option ("test", no_argument, 0, 'T', "test mode");
  parse_option ("no-user-id", no_argument, 0, 'm', "ignore user id");
  parse_option ("index", no_argument, 0, 'i', "reindex");
  parse_option ("high-memory", no_argument, 0, 'M', "memory usage to reindex");
  parse_option ("no-version", no_argument, 0, 'y', "counters are valid until disabled by hands");
  parse_option ("custom-version", no_argument, 0, 'x', "use yyyymmdd as counter version. Includes [--no-version]");
  parse_option ("monthly", no_argument, 0, 'E', "enable monthly stat");
  parse_option ("no-set-timezone", no_argument, 0, 't', "ignore all set timezone events");
  parse_option ("day-start-version", no_argument, 0, 'D', "use unixtime at 0:00 as counter version");
  parse_option ("counter-growth", required_argument, 0, 'P', "counter hash table growth in percents (default %lf)", max_counters_growth_percent);
  parse_option ("default-timezone", required_argument, 0, 'S', "default timezone (hours offset from GMT)");
  parse_option ("cyclic-binlog", required_argument, 0, 1000, "use binlog in cyclic mode");
  
  parse_engine_options_long (argc, argv, f_parse_option);
  if (argc != optind + 1 && argc != optind + 2) {
    usage ();
    return 2;
  }

  if (verbosity >= 3) {
    if (index_mode) {
      fprintf (stderr, "Starting in index mode...\n");
    }
  }
/*
  types_count = 0;
*/
//  max_binlog_size = 170;

  if (raise_file_rlimit(maxconn + 16) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit(1);
  }

  if (port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
      exit(1);
    }
  }

  aes_load_pwd_file (0);

  if (change_user(username) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  if (MAX_ZALLOC > 0) {
  } else {
    dynamic_data_buffer_size = (1 << 23);
  }
  init_dyn_data();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }
/*
  reload_config();
*/

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }


  log_ts_interval = 10;
  int tt;
  today_start = tt = time(0);
  today_start -= tt % 86400;

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "load index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  index_load_time = -get_utime(CLOCK_MONOTONIC);

  i = load_index (Snapshot);

  index_load_time += get_utime(CLOCK_MONOTONIC);

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
    exit (1);
  }


  if (verbosity) {
    fprintf (stderr, "Reading binlog from position %lld\n", jump_log_pos);
  }

  if (reverse_index_mode) {
    assert (init_stats_data (STATS_SCHEMA_V1) >= 0);
    if (reverse_index_pos) {
      jump_log_pos = reverse_index_pos;
    } else {
      jump_log_pos = get_binlog_start_pos (engine_replica, 0, 0) + 36;
    }
  }

  log_ts_interval = 10;
  today_start = now = time(0);
  today_start -= now % 86400;
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }

  binlogname = Binlog->info->filename;

  if (verbosity) {
    fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
  }
  binlog_load_time = get_utime(CLOCK_MONOTONIC);

  clear_log();

  init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

  i = replay_log (0, 1);

  if (i < 0) {
    fprintf (stderr, "fatal: error reading binlog\n");
    exit (1);
  }

  binlog_load_time = get_utime(CLOCK_MONOTONIC) - binlog_load_time;
  binlog_loaded_size = log_readto_pos - jump_log_pos;

  if (!binlog_disabled) {
    clear_read_log();
  }


  if (index_mode) {
    if (verbosity >= 1) {
      fprintf (stderr, "Saving index...\n");
    }
    if (!reverse_index_mode) {
      save_index ();
    } else {
      assert (0);
    }
  } else {
    clear_write_log();
    start_time = time(0);

    start_server();
  }

  return 0;
}
