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
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
              2011-2013 Vitaliy Valtman
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
#include <sys/wait.h>

#include "md5.h"
#include "crc32.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "kdb-friends-binlog.h"
#include "friend-data.h"
#include "kdb-binlog-common.h"
#include "kfs.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"
#include "net-parse.h"
#include "am-stats.h"

#include "net-rpc-server.h"
#include "net-rpc-client.h"
#include "net-rpc-common.h"
#include "rpc-const.h"
#include "vv-tl-parse.h"
#include "vv-tl-aio.h"
#include "friends-tl.h"
#include "friends-interface-structures.h"
#include "common-data.h"

#define	VERSION_STR	"friend-engine-0.2"

#define TCP_PORT 11211

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

/*
 *
 *		FRIENDS ENGINE
 *
 */

int verbosity = 0;
char *progname = "friend-engine", *username, *binlogname, *logname;


/* stats counters */
int start_time;
long long binlog_loaded_size;
double binlog_load_time;
double index_load_time;
int jump_log_ts;
unsigned jump_log_crc32;
long long jump_log_pos;
volatile int force_write_index;
int udp_enabled;

int w_split_rem, w_split_mod;

#define STATS_BUFF_SIZE	(1 << 20)
char stats_buff[STATS_BUFF_SIZE];

#define MAX_USERLIST_NUM 10000
int userlist[MAX_USERLIST_NUM];
int resultlist[MAX_USERLIST_NUM];

int start_write_binlog (void);
int stop_write_binlog (void);

int backlog = BACKLOG, port = TCP_PORT, maxconn = MAX_CONNECTIONS, daemonize = 1, test_mode = 0;
struct in_addr settings_addr;
int active_connections;

int reverse_friends_mode;

conn_type_t ct_friends_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "friend_engine_server",
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
  .wakeup = server_failed,
  .alarm = server_failed,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_incr (struct connection *c, int op, const char *key, int key_len, long long arg);
int memcache_delete (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = mcs_get_start,
  .mc_get = memcache_get,
  .mc_get_end = mcs_get_end,
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
  .memcache_fallback_type = &ct_friends_engine_server,
  .memcache_fallback_extra = &memcache_methods
};

int quit_steps;

int friend_prepare_stats (struct connection *c) {
  stats_buffer_t sb;
  sb_prepare (&sb, c, stats_buff, STATS_BUFF_SIZE);
  sb_memory (&sb, AM_GET_MEMORY_USAGE_SELF);
  SB_BINLOG;
  SB_INDEX;

  sb_printf (&sb,
		  "tree_nodes\t%d\n"
		  "privacy_nodes\t%d\n"
      "reverse_friends_nodes\t%d\n"
		  "total_privacy_len\t%d\n"
		  "memory_users\t%d\n",
		  alloc_tree_nodes,
		  privacy_nodes,
      alloc_rev_friends_nodes,
		  tot_privacy_len,
		  tot_users);

  sb_printf (&sb, "version\t%s\n", FullVersionStr);
  return sb.pos;
}

static int parse_list (int *Res, int max_size, netbuffer_t *In, int bytes) {
  char *ptr = 0, *ptr_e = 0;
  int r = 0, s = 0, x;
  if (!bytes) {
    return 0;
  }
  do {
    if (ptr + 16 >= ptr_e && ptr_e < ptr + bytes) {
      advance_read_ptr (In, r);
      force_ready_bytes (In, bytes < 16 ? bytes : 16);
      ptr = get_read_ptr (In);
      r = get_ready_bytes (In);
      if (r > bytes) {
        r = bytes;
      }
      ptr_e = ptr + r;
      r = 0;
    }
    assert (ptr < ptr_e);
    x = 0;
    while (ptr < ptr_e && *ptr >= '0' && *ptr <= '9') {
      if (x >= 0x7fffffff / 10) {
        return -1;
      }
      x = x*10 + (*ptr++ - '0');
      r++;
      bytes--;
    }
    if (s >= max_size || (bytes > 0 && (ptr == ptr_e || *ptr != ','))) {
      advance_skip_read_ptr (In, r + bytes);
      return -1;
    }
    Res[s++] = x;
    if (!bytes) {
      advance_read_ptr (In, r);
      return s;
    }
    assert (*ptr == ',');
    ptr++;
    r++;
  } while (--bytes > 0);
  assert (!bytes);
  advance_read_ptr (In, r);
  return s;
}

int parse_privacy_key (const char *text, privacy_key_t *res, int reqeol) {
  int i = 0, j;
  unsigned x, y;
  if (verbosity > 1) {
    fprintf (stderr, "parsing privacy key '%s', reqeol=%d\n", text, reqeol);
  }
  while ((text[i] | 0x20) >= 'a' && (text[i] | 0x20) <= 'z' && i <= 32) {
    i++;
  }
  if (i <= 0 || i == 32) {
    return -1;
  }
  j = i;
  if (text[j] >= '0' && text[j] <= '9') {
    y = 0;
    while (text[j] >= '0' && text[j] <= '9') {
      if (j >= 48 || y > 0x7fffffff / 10) {
        return -1;
      }
      y = y * 10 + (text[j] - '0');
      j++;
    }
  } else if (text[j] == '*') {
    y = ~0;
    j++;
  } else {
    if (text[j] != '_' && text[j] != ' ' && text[j] != ',' && text[j]) {
      return -1;
    }
    while ((signed char) text[j] >= '0') {
      j++;
      if (j >= 64) {
        return -1;
      }
    }
    y = compute_crc32 (text + i, j - i);
  }
  if (text[j]) {
    if (reqeol > 0) {
      return -1;
    }
    if (text[j] != ' ' && (text[j] != ',' || reqeol != -1)) {
      return -1;
    }
  }
  x = compute_crc32 (text, i);
  *res = (((unsigned long long) x) << 32) + y;
  if (verbosity > 1) {
    fprintf (stderr, "privacy key = %016llx\n", *res);
  }
  return j;
}

struct keep_mc_header {
  int list_id;
  int num;
};


static inline void init_tmp_buffers (struct connection *c) {
  free_tmp_buffers (c);
  c->Tmp = alloc_head_buffer ();
  assert (c->Tmp);
}

int exec_store_userlist (struct connection *c, const char *key, int key_len, int size) {
  int pos = 0;
  int list_id;
  sscanf (key, "userlist%d%n", &list_id, &pos);
  if (pos != key_len || list_id >= 0) {
    advance_skip_read_ptr (&c->In, size);
    return 0;
  }
  int res = np_news_parse_list (userlist, MAX_USERLIST_NUM, 1, &c->In, size);
  //int res = parse_list (userlist, MAX_USERLIST_NUM, &c->In, size);
  if (res <= 0) {
    return 0;
  }
  struct keep_mc_header D;
  D.list_id = list_id;
  D.num = res;
  init_tmp_buffers (c);
  write_out (c->Tmp, &D, sizeof(D));
  write_out (c->Tmp, userlist, res * 4);
  return 1;
}

int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size) {
  int user_id, cat = 0, friend_id = 0, i;
  privacy_key_t privacy_key = 0;

  if (verbosity > 0) {
    fprintf (stderr, "mc_store: op=%d, key=\"%s\", flags=%d, delay=%d, bytes=%d\n", op, key, flags, delay, size);
  }

  if (binlog_disabled == 2) {
    return -2;
  }

  if (reverse_friends_mode) {
    if (op == mct_set && key_len >= 8 && !strncmp (key, "userlist", 8)) {
      return exec_store_userlist (c, key, key_len, size);
    }
    return 0;
  }

  if (op == mct_set && sscanf (key, "friends%d_%d", &user_id, &cat) == 2 && user_id > 0 && cat > 0 && cat < 32) {
    int s = parse_list (R, MAX_RES, &c->In, size);
    int res = 0;
    if (s >= 0) {
      res = do_set_category_friend_list (user_id, cat, R, s);
    }
    if (verbosity > 0) {
      fprintf (stderr, "set friend cat list: size = %d, res = %d\n", s, res);
    }
    return res;
  }

  if (size > 1024) {
    return -2;
  }

  assert (read_in (&c->In, stats_buff, size) == size);
  stats_buff[size] = 0;

  if (sscanf (key, "friendreq%d_%d", &user_id, &friend_id) == 2 && user_id > 0 && friend_id > 0) {
    return do_add_friend_request (user_id, friend_id, atol(stats_buff), (op == mct_add) * 2 + (op == mct_set)) >= 0;
  }

  if (op != mct_add && sscanf (key, "friend%d_%d", &user_id, &friend_id) == 2 && user_id > 0 && friend_id > 0) {
    return do_add_friend (user_id, friend_id, atol(stats_buff), 0, op == mct_set);
  }

  if (op != mct_add && sscanf (key, "privacy%d_%n", &user_id, &i) >= 1 && user_id > 0 && parse_privacy_key (key+i, &privacy_key, 1) > 0) {
    return do_set_privacy (user_id, privacy_key, stats_buff, size, op == mct_set);
  }

  return 0;
}

int memcache_delete (struct connection *c, const char *key, int key_len) {
  int user_id, arg = 0;
  privacy_key_t privacy_key;

  if (verbosity > 0) {
    fprintf (stderr, "delete \"%s\"\n", key);
  }

  int res = -1;

  if (binlog_disabled == 2 || reverse_friends_mode) {
    write_out (&c->Out, "NOT_FOUND\r\n", 11);
    return 0;
  }

  switch (*key) {
  case 'u':
    if (sscanf (key, "user%d ", &user_id) == 1) {
      res = do_delete_user (user_id);
    }
    break;
  case 'f':
    if (sscanf (key, "friend_cat%d_%d ", &user_id, &arg) == 2) {
      res = do_delete_friend_category (user_id, arg);
    }
    if (sscanf (key, "friendreq%d_%d ", &user_id, &arg) == 2) {
      res = do_delete_friend_request (user_id, arg);
    }
    if (sscanf (key, "friend%d_%d ", &user_id, &arg) == 2) {
      res = do_delete_friend (user_id, arg);
    }
    break;
  case 'p':
    if (sscanf (key, "privacy%d_%n", &user_id, &arg) >= 1 && parse_privacy_key (key+arg, &privacy_key, 1) > 0) {
      res = do_delete_privacy (user_id, privacy_key);
    }
    break;
  case 'r':
    if (sscanf (key, "requests%d ", &user_id) == 1) {
      res = do_delete_all_friend_requests (user_id);
    }
    break;
  }

  if (res > 0) {
    write_out (&c->Out, "DELETED\r\n", 9);
  } else {
    write_out (&c->Out, "NOT_FOUND\r\n", 11);
  }

  return 0;
}

void exec_get_friends (struct connection *c, const char *str, int len, int offs) {
  int user_id, cat_mask = -1, mode = 0;
  if (sscanf (str+offs, "%d_%d#%d", &user_id, &cat_mask, &mode) >= 2 ||
      sscanf (str+offs, "%d#%d", &user_id, &mode) >= 1) {
    if (offs != 7) {
      mode = 0;
    }
    int res = prepare_friends (user_id, cat_mask, mode);
    if (verbosity > 1) {
      fprintf (stderr, "prepare_friends(%d,%d,%d) = %d\n", user_id, cat_mask, mode, res);
    }
    if (res >= 0) {
      if (offs == 7) {
        return_one_key_list (c, str, len, res, 0, R, R_end - R);
      } else {
        return_one_key (c, str, stats_buff, sprintf(stats_buff, "%d", res));
      }
      return;
    }
  }
  return;
}

void exec_get_recent_friends (struct connection *c, const char *str, int len) {
  int user_id, num;
  if (sscanf (str+14, "%d#%d", &user_id, &num) >= 2) {
    int res = prepare_recent_friends (user_id, num);
    if (verbosity > 1) {
      fprintf (stderr, "prepare_recent_friends(%d,%d) = %d\n", user_id, num, res);
    }
    if (res >= 0) {
      return_one_key_list (c, str, len, res, 0, R, R_end - R);
      return;
    }
  }
  return;
}

void exec_get_one_friend (struct connection *c, const char *str, int len) {
  int user_id, friend_id;
  if (sscanf (str, "friend%d_%d", &user_id, &friend_id) >= 2) {
    int res = get_friend_cat (user_id, friend_id);
    if (verbosity > 1) {
      fprintf (stderr, "get_friend(%d,%d) = %d\n", user_id, friend_id, res);
    }
    if (res >= 0) {
      return_one_key (c, str, stats_buff, sprintf(stats_buff, "%d", res));
      return;
    }
  }
  return;
}

void exec_get_one_request (struct connection *c, const char *str, int len) {
  int user_id, friend_id;
  if (sscanf (str, "friendreq%d_%d", &user_id, &friend_id) >= 2) {
    int res = get_friend_request_cat (user_id, friend_id);
    if (verbosity > 1) {
      fprintf (stderr, "get_friend_request_cat(%d,%d) = %d\n", user_id, friend_id, res);
    }
    if (res >= 0) {
      return_one_key (c, str, stats_buff, sprintf(stats_buff, "%d", res));
      return;
    }
  }
  return;
}

void exec_get_requests (struct connection *c, const char *str, int len) {
  int user_id, num = -1;
  if (sscanf (str+8, "%d#%d", &user_id, &num) >= 1 && user_id > 0 && num >= -1) {
    int res = prepare_friend_requests (user_id, num);
    if (verbosity > 1) {
      fprintf (stderr, "prepare_friend_requests(%d,%d) = %d\n", user_id, num, res);
    }
    if (res >= 0) {
      return_one_key_list (c, str, len, res, 0, R, R_end - R);
      return;
    }
  }
  return;
}

void exec_get_check_privacy (struct connection *c, const char *str, int len) {
  int checker_id, user_id, i, j, k;
  privacy_key_t privacy_key;

  if (sscanf (str, "%d~%d:%n", &checker_id, &user_id, &i) >= 2 && (j = parse_privacy_key (str+i, &privacy_key, -1)) > 0) {
    j += i;
    if (!str[j]) {
      i = check_privacy (checker_id, user_id, privacy_key);
      if (verbosity > 1) {
        fprintf (stderr, "check_privacy(%d,%d,%016llx) = %d\n", checker_id, user_id, privacy_key, i);
      }
      stats_buff[0] = (i & -4 ? '?' : '0' + i);
      stats_buff[1] = 0;

      return_one_key (c, str, stats_buff, 1);
      return;
    } else if (str[j] == ',') {
      k = 0;
      while (k < 256) {
        i = check_privacy (checker_id, user_id, privacy_key);
        stats_buff[k++] = (i & -4 ? '?' : '0' + i);
        if (verbosity > 1) {
          fprintf (stderr, "check_privacy(%d,%d,%016llx) = %d\n", checker_id, user_id, privacy_key, i);
        }
        if (str[j] != ',') {
          break;
        }
        i = parse_privacy_key (str+j+1, &privacy_key, -1);
        if (i <= 0) {
          break;
        }
        j = j+i+1;
      }
      if (!str[j]) {
        stats_buff[k] = 0;
        return_one_key (c, str, stats_buff, k);
        return;
      }
    }
  }
  return;
}

void exec_get_privacy (struct connection *c, const char *str, int len) {
  int user_id, i;
  privacy_key_t privacy_key;

  if (sscanf (str, "privacy%d_%n", &user_id, &i) >= 1 && parse_privacy_key (str+i, &privacy_key, 1) > 0) {
    i = prepare_privacy_str (stats_buff, user_id, privacy_key);
    if (i >= 0) {
      return_one_key (c, str, stats_buff, i);
    } else {
      return_one_key (c, str, "?", 1);
    }
    return;
  }
  return;
}

int get_saved_userlist (struct connection *c, int list_id) {
  if (!c->Tmp) {
    return -1;
  }
  struct keep_mc_header *D = (struct keep_mc_header *) c->Tmp->start;
  advance_read_ptr (c->Tmp, sizeof (struct keep_mc_header));
  int res = D->num;
  assert (read_in (c->Tmp, userlist, res * 4) == 4 * res);
  if (D->list_id != list_id) {
    return -1;
  }
  return res;
}

void exec_get_common_friends_num (struct connection *c, const char *str, int len) {
  int user_id = 0;
  int raw = *str == '%';
  int pos = 0;
  const char *str_orig = str;
  int len_orig = len;
  if ((sscanf (str, "common_friends_num%d:%n", &user_id, &pos) >= 1 && pos > 0) ||
      (sscanf (str, "%%common_friends_num%d:%n", &user_id, &pos) >= 1 && pos > 0)) {
    str += pos;
    len -= pos;
    int user_num = 0;
    if (*str == '-') {
      int t;
      if (sscanf (str, "%d%n", &t, &pos) < 1) {
        return;
      }
      if (pos != len) {
        return;
      }
      user_num = get_saved_userlist (c, t);
    } else {
      while (1) {
        if (sscanf (str, "%d%n", &userlist[user_num++], &pos) < 1) {
          return;
        }
        str += pos;
        len -= pos;
        if (!len) {
          break;
        }
        if (user_num == MAX_USERLIST_NUM || *str != ',') {
          return;
        }
        str ++;
        len --;
      }
    }
    assert (user_num <= MAX_USERLIST_NUM);
    get_common_friends_num (user_id, user_num, userlist, resultlist);
    return_one_key_list (c, str_orig, len_orig, 1, -raw, resultlist, user_num);
  }
}


void exec_get_common_friends (struct connection *c, const char *str, int len) {
	free_tmp_buffers (c);
  int user_id = 0;
  int raw = *str == '%';
  int pos = 0;
  const char *str_orig = str;
  int len_orig = len;
  if ((sscanf (str, "common_friends%d,%d%n", &user_id, &userlist[0], &pos) >= 2 && pos > 0) ||
      (sscanf (str, "%%common_friends%d,%d%n", &user_id, &userlist[0], &pos) >= 2 && pos > 0)) {
    if (len != pos) {
    	return;
    }
  	int res = get_common_friends (user_id, 1, userlist, resultlist, MAX_USERLIST_NUM);
	  return_one_key_list (c, str_orig, len_orig, res, -raw, resultlist, res);
  }
}


int memcache_get (struct connection *c, const char *key, int key_len) {
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get: key='%s'\n", key);
  }

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    return_one_key (c, key, stats_buff, friend_prepare_stats (c));
    return 0;
  }

  if (key_len == 18 && !strncmp (key, "start_write_binlog", 18)) {
    return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", start_write_binlog()));
    return 0;
  }

  if (key_len == 17 && !strncmp (key, "stop_write_binlog", 17)) {
    return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", stop_write_binlog()));
    return 0;
  }

  if (key_len == 15 && !strncmp (key, "binlog_disabled", 15)) {
    return_one_key (c, key, stats_buff, sprintf (stats_buff, "%d", binlog_disabled));
    return 0;
  }

  if (key_len >= 16 && !strncmp (key, "free_block_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, FreeCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (key_len >= 16 && !strncmp (key, "used_block_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, UsedCnt, MAX_RECORD_WORDS);
    return 0;
  }

  if (key_len >= 16 && !strncmp (key, "allocation_stats", 16)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, NewAllocations[0], MAX_RECORD_WORDS * 4);
    return 0;
  }

  if (key_len >= 17 && !strncmp (key, "split_block_stats", 17)) {
    return_one_key_list (c, key, key_len, MAX_RECORD_WORDS, 0, SplitBlocks, MAX_RECORD_WORDS);
    return 0;
  }

  if (reverse_friends_mode) {
    if (key_len >= 19 && (!strncmp (key, "common_friends_num", 18) || !strncmp (key, "%common_friends_num", 19))) {
      exec_get_common_friends_num (c, key, key_len);
      free_tmp_buffers (c);
      return 0;
    }
    if (key_len >= 15 && (!strncmp (key, "common_friends", 14) || !strncmp (key, "%common_friends", 15))) {
      exec_get_common_friends (c, key, key_len);
      free_tmp_buffers (c);
      return 0;
    }
    return 0;
  }

  if (key_len >= 7 && !strncmp (key, "friends", 7)) {
    exec_get_friends (c, key, key_len, 7);
    return 0;
  }

  if (key_len >= 9 && !strncmp (key, "friendcnt", 9)) {
    exec_get_friends (c, key, key_len, 9);
    return 0;
  }

  if (key_len >= 9 && !strncmp (key, "friendreq", 9)) {
    exec_get_one_request (c, key, key_len);
    return 0;
  }

  if (key_len >= 9 && !strncmp (key, "requests", 8)) {
    exec_get_requests (c, key, key_len);
    return 0;
  }

  if (key_len >= 7 && !strncmp (key, "friend", 6)) {
    exec_get_one_friend (c, key, key_len);
    return 0;
  }

  if (key_len >= 14 && !strncmp (key, "recent_friends", 14)) {
    exec_get_recent_friends (c, key, key_len);
    return 0;
  }

  if (key_len >= 1 && *key >= '1' && *key <= '9') {
    exec_get_check_privacy (c, key, key_len);
    return 0;
  }

  if (key_len >= 7 && !strncmp (key, "privacy", 7)) {
    exec_get_privacy (c, key, key_len);
    return 0;
  }

  return 0;
}


int memcache_incr (struct connection *c, int op, const char *key, int len, long long arg) {
  int user_id, friend_id;

  if (len >= 7 && !memcmp (key, "friend", 6) && !reverse_friends_mode) {
    int res = -1;
    if (binlog_disabled != 2 && sscanf (key, "friend%d_%d", &user_id, &friend_id) >= 2) {
      res = do_add_friend (user_id, friend_id, op ? 0 : arg, ~arg, 0);
    }
    if (res > 0) {
      write_out (&c->Out, stats_buff, sprintf(stats_buff, "%d\r\n", res));
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }
    return 0;
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  return 0;
}

int memcache_stats (struct connection *c) {
  int len = friend_prepare_stats (c);
  write_out (&c->Out, stats_buff, len);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

TL_DO_FUN(delete_user)
  int res = do_delete_user (e->uid);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(get_friends)
  int mode = e->mode & 3;
  if (mode == 3) { mode = 2; }
  int res = prepare_friends (e->uid, e->mask, mode);
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
    assert (R_end - R == (mode + 1) * res);
    tl_store_string_data ((char *)R, 4 * (mode + 1) * res);
  }
TL_DO_FUN_END

TL_DO_FUN(get_friends_cnt)
  int res = prepare_friends (e->uid, e->mask, 0);
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_DO_FUN(get_recent_friends)
  int res = prepare_recent_friends (e->uid, e->num);
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
    res = (R_end - R) / 3;
    tl_store_int (res);
    tl_store_string_data ((char *)R, res * 12);
  }
TL_DO_FUN_END

TL_DO_FUN(set_cat_list)
  int res = do_set_category_friend_list (e->uid, e->cat, e->list, e->num);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE); 
TL_DO_FUN_END

TL_DO_FUN(delete_cat)
  int res = do_delete_friend_category (e->uid, e->cat);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE); 
TL_DO_FUN_END

TL_DO_FUN(get_friend)
  int res = get_friend_cat (e->uid, e->fid);
  tl_store_int (res < 0 ? TL_MAYBE_FALSE : TL_MAYBE_TRUE); 
  if (res >= 0) { tl_store_int (res); }
TL_DO_FUN_END

TL_DO_FUN(set_friend)
  int res = do_add_friend (e->uid, e->fid, e->xor_mask, e->and_mask, e->is_set);
  if (!e->is_incr) {
    tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE); 
  } else {
    tl_store_int (res <= 0 ? TL_MAYBE_FALSE : TL_MAYBE_TRUE);
    if (res >= 0) { tl_store_int (res); }
  }
TL_DO_FUN_END

TL_DO_FUN(delete_friend)
  int res = do_delete_friend (e->uid, e->fid);
  tl_store_int (res < 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE); 
TL_DO_FUN_END

TL_DO_FUN(get_friend_req)
  int res = get_friend_request_cat (e->uid, e->fid);
  tl_store_int (res < 0 ? TL_MAYBE_FALSE : TL_MAYBE_TRUE); 
  if (res >= 0) { tl_store_int (res); }
TL_DO_FUN_END

TL_DO_FUN(set_friend_req)
  int res = do_add_friend_request (e->uid, e->fid, e->cat, e->force);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE); 
TL_DO_FUN_END

TL_DO_FUN(delete_friend_req)
  int res = do_delete_friend_request (e->uid, e->fid);
  tl_store_int (res < 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE); 
TL_DO_FUN_END

TL_DO_FUN(delete_requests)
  int res = do_delete_all_friend_requests (e->uid);
  tl_store_int (res < 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE); 
TL_DO_FUN_END

TL_DO_FUN(get_requests)
  int res = prepare_friend_requests (e->uid, e->num);
  if (res < 0) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
    res = (R_end - R) / 3;
    tl_store_int (res);
    tl_store_string_data ((char *)R, res * 12);
  }
TL_DO_FUN_END

TL_DO_FUN(set_privacy)
  int res = do_set_privacy (e->uid, e->privacy_key, e->text, e->len, e->force);
  tl_store_int (res <= 0 ? TL_BOOL_FALSE : TL_BOOL_TRUE);
TL_DO_FUN_END

TL_DO_FUN(get_privacy)
  static char buf[(1 << 16)];
  int res = prepare_privacy_str (buf, e->uid, e->privacy_key);
  tl_store_int (res >= 0 ? TL_MAYBE_TRUE : TL_MAYBE_FALSE);
  if (res >= 0) {
    tl_store_string (buf, res);
  } 
TL_DO_FUN_END

TL_DO_FUN(delete_privacy)
  int res = do_delete_privacy (e->uid, e->privacy_key);
  tl_store_int (res >= 0 ? TL_BOOL_TRUE : TL_BOOL_FALSE);
TL_DO_FUN_END

TL_DO_FUN(check_privacy)
  int res = check_privacy (e->reqid, e->uid, e->privacy_key);
  if (res & -4) {
    tl_store_int (TL_MAYBE_FALSE);
  } else {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  }
TL_DO_FUN_END

TL_DO_FUN(check_privacy_list)
  tl_store_int (TL_VECTOR);
  int i;
  tl_store_int (e->num);
  for (i = 0; i < e->num; i++) {
    int res = check_privacy (e->reqid, e->uid, e->privacy_key[i]);
    if (res & -4) {
      tl_store_int (TL_MAYBE_FALSE);
    } else {
      tl_store_int (TL_MAYBE_TRUE);
      tl_store_int (res);
    }
  }
TL_DO_FUN_END

TL_DO_FUN(common_friends)
  tl_store_int (TL_VECTOR);
  int res = get_common_friends (e->uid, 1, &e->uid2, resultlist, MAX_USERLIST_NUM);
  assert (0 <= res && res < MAX_USERLIST_NUM);
  tl_store_int (res);
  tl_store_string_data ((char *)resultlist, res * 4);
TL_DO_FUN_END

TL_DO_FUN(common_friends_num)
  tl_store_int (TL_VECTOR);
  assert (e->num <= MAX_USERLIST_NUM);
  get_common_friends_num (e->uid, e->num, e->uid_list, resultlist);
  tl_store_int (e->num);
  tl_store_string_data ((char *)resultlist, e->num * sizeof (int));
TL_DO_FUN_END

int tl_fetch_uid (void) {
  if (tl_fetch_error ()) {
    return -1;
  }
  int uid = tl_fetch_int ();
  if (!uid || conv_uid (uid) < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong server: user_id = %d, log_split_mod = %d, log_split_min = %d, log_split_max = %d", uid, log_split_mod, log_split_min, log_split_max);
    return -1;
  }
  return uid;
}

int tl_fetch_cat (int set) {
  if (tl_fetch_error ()) {
    return -1;
  }
  int cat = tl_fetch_int ();
  if (set) {
    if (cat <= 0 || cat >= 31) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Set can be performed on cat in range 1..30. Cat = %d", cat);
      return -1;
    }
  } else {
    if (cat < 0 || cat >= 31) {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Get can be performed on cat in range 0..30. Cat = %d", cat);
      return -1;
    }
  }
  return cat;
}

TL_PARSE_FUN(delete_user)
  e->uid = tl_fetch_uid ();
TL_PARSE_FUN_END

TL_PARSE_FUN(get_friends,int full)
  e->uid = tl_fetch_uid ();
  e->mask = tl_fetch_int ();
  e->mode = full ? tl_fetch_int () : 0;
  if ((e->mode & 3) == 2) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Only mode 0, 1 and 3 are suppored (mode = 0x%08x)", e->mode);
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(get_friends_cnt,void)
  e->uid = tl_fetch_uid ();
  e->mask = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(get_recent_friends,void)
  e->uid = tl_fetch_uid ();  
  e->num = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(set_cat_list,void)
  e->uid = tl_fetch_uid ();  
  e->cat = tl_fetch_cat (1);
  e->num = tl_fetch_int ();
  if (e->num > 100000 || e->num < 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "List size is too big: num = %d", e->num);
    return 0;
  }
  tl_fetch_string_data ((char *)e->list, e->num * 4);
  extra->size += 4 * e->num;
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_cat,void)
  e->uid = tl_fetch_uid ();  
  e->cat = tl_fetch_cat (1);
TL_PARSE_FUN_END

TL_PARSE_FUN(get_friend,void)
  e->uid = tl_fetch_uid ();  
  e->fid = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(set_friend,int is_set,int mode)
  e->uid = tl_fetch_uid ();  
  e->fid = tl_fetch_int ();
  int a = tl_fetch_int ();
  if (!mode) {
    e->xor_mask = a;
    e->and_mask = 0;
  } else if (mode == 1) {
    e->xor_mask = a;
    e->and_mask = ~a;
  } else {
    e->xor_mask = 0;
    e->and_mask = ~a;
  }
  e->is_set = is_set;
  e->is_incr = mode >= 1;
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_friend,void)
  e->uid = tl_fetch_uid ();  
  e->fid = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(get_friend_req,void)
  e->uid = tl_fetch_uid ();  
  e->fid = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(set_friend_req,int force)
  e->uid = tl_fetch_uid ();  
  e->fid = tl_fetch_int ();
  e->cat = tl_fetch_int ();
  e->force = force;
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_friend_req,void)
  e->uid = tl_fetch_uid ();  
  e->fid = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_requests,void)
  e->uid = tl_fetch_uid ();  
TL_PARSE_FUN_END

TL_PARSE_FUN(get_requests,void)
  e->uid = tl_fetch_uid ();  
  e->num = tl_fetch_int ();
  if (e->num < -1) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "num should be -1 or non-negative integer (num = %d)", e->num);
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(set_privacy,int force)
  e->uid = tl_fetch_uid ();
  tl_fetch_string0 (e->text, 1024);
  if (tl_fetch_error ()) { return 0; }
  if (parse_privacy_key (e->text, &e->privacy_key, 1) <= 0)  {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "can not parse privacy");
    return 0;
  }
  e->len = tl_fetch_string0 (e->text, 1024);
  extra->size += e->len + 1;
  e->force = force;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_privacy,void)
  e->uid = tl_fetch_uid ();
  static char buf[1025];
  int num = tl_fetch_string (buf, 1024);
  if (tl_fetch_error ()) { return 0; }
  buf[num] = 0;
  if (parse_privacy_key (buf, &e->privacy_key, 1) <= 0)  {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "can not parse privacy");
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_privacy,void)
  e->uid = tl_fetch_uid ();
  static char buf[1025];
  int num = tl_fetch_string (buf, 1024);
  if (tl_fetch_error ()) { return 0; }
  buf[num] = 0;
  if (parse_privacy_key (buf, &e->privacy_key, 1) <= 0)  {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "can not parse privacy");
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(check_privacy,void)
  e->uid = tl_fetch_uid ();
  static char buf[1025];
  e->reqid = tl_fetch_int ();
  int num = tl_fetch_string (buf, 1024);
  if (tl_fetch_error ()) { return 0; }
  buf[num] = 0;
  if (parse_privacy_key (buf, &e->privacy_key, 1) <= 0)  {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "can not parse privacy");
    return 0;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(check_privacy_list,void)
  e->uid = tl_fetch_uid ();
  e->reqid = tl_fetch_int ();
  e->num = tl_fetch_int ();
  if (e->num < 0 || e->num > 256) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "number of privacy requests should be in range 0..256");
    return 0;
  }
  int i;
  for (i = 0; i < e->num; i++) {
    static char buf[1025];
    int num = tl_fetch_string (buf, 1024);
    if (tl_fetch_error ()) { return 0; }
    buf[num] = 0;
    if (parse_privacy_key (buf, &e->privacy_key[i], 1) <= 0)  {
      tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "can not parse privacy #%d", i);
      return 0;
    }
  }
  extra->size += e->num * sizeof (privacy_key_t);
TL_PARSE_FUN_END

TL_PARSE_FUN(common_friends,void)
  e->uid = tl_fetch_int ();  
  e->uid2 = tl_fetch_int ();
TL_PARSE_FUN_END

TL_PARSE_FUN(common_friends_num,void)
  e->uid = tl_fetch_int ();  
  e->num = tl_fetch_int ();
  if (e->num < 0 || e->num > 256) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "number of requests should be in range 0..256");
    return 0;
  }
  int i;
  for (i = 0; i < e->num; i++) {
    e->uid_list[i] = tl_fetch_int ();
  }
  extra->size += sizeof (int) * e->num;
TL_PARSE_FUN_END

struct tl_act_extra *friends_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Friends only support actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return 0;
  }
  int op = tl_fetch_int ();
  if (tl_fetch_error ()) {
    return 0;
  }
  switch (op) {
  case TL_FRIEND_DELETE_USER:
    return tl_delete_user ();
  case TL_FRIEND_GET_FRIENDS_ID:
    return tl_get_friends (0);
  case TL_FRIEND_GET_FRIENDS:
    return tl_get_friends (1);
  case TL_FRIEND_GET_FRIENDS_CNT:
    return tl_get_friends_cnt ();
  case TL_FRIEND_GET_RECENT_FRIENDS:
    return tl_get_recent_friends ();
  case TL_FRIEND_SET_CAT_LIST:
    return tl_set_cat_list ();
  case TL_FRIEND_DELETE_CAT:
    return tl_delete_cat ();
  case TL_FRIEND_GET_FRIEND:
    return tl_get_friend ();
  case TL_FRIEND_SET_FRIEND:
    return tl_set_friend (1, 0);
  case TL_FRIEND_REPLACE_FRIEND:
    return tl_set_friend (0, 0);
  case TL_FRIEND_INCR_FRIEND:
    return tl_set_friend (0, 1);
  case TL_FRIEND_DECR_FRIEND:
    return tl_set_friend (0, 2);
  case TL_FRIEND_DELETE_FRIEND:
    return tl_delete_friend ();
  case TL_FRIEND_GET_FRIEND_REQ:
    return tl_get_friend_req ();
  case TL_FRIEND_SET_FRIEND_REQ:
    return tl_set_friend_req (1);
  case TL_FRIEND_REPLACE_FRIEND_REQ:
    return tl_set_friend_req (0);
  case TL_FRIEND_ADD_FRIEND_REQ:
    return tl_set_friend_req (2);
  case TL_FRIEND_DELETE_FRIEND_REQ:
    return tl_delete_friend_req ();
  case TL_FRIEND_DELETE_REQS:
    return tl_delete_requests ();
  case TL_FRIEND_GET_REQS:
    return tl_get_requests ();
  case TL_FRIEND_SET_PRIVACY:
    return tl_set_privacy (1);
  case TL_FRIEND_REPLACE_PRIVACY:
    return tl_set_privacy (0);
  case TL_FRIEND_GET_PRIVACY:
    return tl_get_privacy ();
  case TL_FRIEND_DELETE_PRIVACY:
    return tl_delete_privacy ();
  case TL_FRIEND_CHECK_PRIVACY:
    return tl_check_privacy ();
  case TL_FRIEND_CHECK_PRIVACY_LIST:
    return tl_check_privacy_list ();
  case TL_FRIEND_COMMON_FRIENDS:
    return tl_common_friends ();
  case TL_FRIEND_COMMON_FRIENDS_NUM:
    return tl_common_friends_num ();
  default:
    tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown op %08x", op);
    return 0;
  }
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
  flush_binlog_last();
  sync_binlog (2);
  exit(EXIT_SUCCESS);
}

static void sigterm_handler (const int sig) {
  fprintf (stderr, "SIGTERM handled.\n");
  flush_binlog_last();
  sync_binlog (2);
  exit(EXIT_SUCCESS);
}

static void sighup_handler (const int sig) {
  fprintf(stderr, "got SIGHUP.\n");
  sync_binlog (2);
  signal(SIGHUP, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  fprintf(stderr, "got SIGUSR1, rotate logs.\n");
  reopen_logs();
  sync_binlog (2);
  signal(SIGUSR1, sigusr1_handler);
}

static void sigrtmax_handler (const int sig) {
  fprintf(stderr, "got SIGUSR3, write index.\n");
  force_write_index = 1;
}


int child_pid;

void check_child_status (void) {
  if (!child_pid) {
    return;
  }
  int status = 0;
  int res = waitpid (child_pid, &status, WNOHANG);
  if (res == child_pid) {
    if (WIFEXITED (status) || WIFSIGNALED (status)) {
      if (verbosity > 0) {
        fprintf (stderr, "child process %d terminated: exited = %d, signaled = %d, exit code = %d\n",
          child_pid, WIFEXITED (status) ? 1 : 0, WIFSIGNALED (status) ? 1 : 0, WEXITSTATUS (status));
      }
      child_pid = 0;
    }
  } else if (res == -1) {
    if (errno != EINTR) {
      fprintf (stderr, "waitpid (%d): %m\n", child_pid);
      child_pid = 0;
    }
  } else if (res) {
    fprintf (stderr, "waitpid (%d) returned %d???\n", child_pid, res);
  }
}

void fork_write_index (void) {
  if (child_pid) {
    if (verbosity > 0) {
      fprintf (stderr, "process with pid %d already generates index, skipping\n", child_pid);
    }
    return;
  }

  flush_binlog_ts ();

  int res = fork ();

  if (res < 0) {
    fprintf (stderr, "fork: %m\n");
  } else if (!res) {
    binlogname = 0;
    res = save_index (!binlog_disabled);
    exit (res);
  } else {
    if (verbosity > 0) {
      fprintf (stderr, "created child process pid = %d\n", res);
    }
    child_pid = res;
  }

  force_write_index = 0;
}

void cron (void) {
  flush_binlog();
  if (force_write_index) {
    fork_write_index ();
  }
  check_child_status();
}


int sfd;

void start_server (void) {
  char buf[64];
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();
  if (udp_enabled) {
    init_msg_buffers (0);
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

  if (binlogname && binlog_disabled != 1) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  tl_parse_function = friends_parse_function;
  tl_aio_timeout = 2.0;
  //init_listening_connection (sfd, &ct_friends_engine_server, &memcache_methods);
  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);
  if (udp_enabled) {
    add_udp_socket (port, 0);
  }

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal(SIGINT, sigint_handler);
  signal(SIGTERM, sigterm_handler);
  signal(SIGUSR1, sigusr1_handler);
  signal(SIGPIPE, SIG_IGN);
  signal (SIGRTMAX, sigrtmax_handler);
  if (daemonize) {
    signal(SIGHUP, sighup_handler);
    reopen_logs();
  }

  for (i = 0; ; i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
	       active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    fetch_binlog_data (binlog_disabled ? log_cur_pos() : log_write_pos ());
    epoll_work (17);

    tl_restart_all_ready ();

    flush_binlog ();
    cstatus_binlog_pos (binlog_disabled ? log_cur_pos() : log_write_pos (), binlog_disabled);

    if (now != prev_time) {
      prev_time = now;
      cron ();
    }

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  epoll_close (sfd);
  close(sfd);

  flush_binlog_ts();
}


/* switches from binlog_disabled = 2 to binlog_disabled = 0 */
int start_write_binlog (void) {
  return -1; // temporary disabled

  /*if (binlog_disabled != 2 || !binlogname) {
    return -1;
  }
  if (!is_write_log_empty ()) {
    return -3;
  }
  if (lock_whole_file (fd[2], F_WRLCK) <= 0) {
    return -2;
  }
  if (verbosity > 0) {
    fprintf (stderr, "obtained write lock for binlog, start writing...\n");
  }
  read_new_events();

  clear_read_log();

  //set_log_data (fd[2], fsize[2]);

  epoll_pre_event = 0;

  binlog_disabled = 0;
  clear_write_log();
  return 1;*/
}

/* switches from binlog_disabled = 0 to binlog_disabled = 2 */
int stop_write_binlog (void) {
  return -1; //temporary disabled

  /*if (!Binlog || binlog_disabled != 0 || !binlogname) {
    return -1;
  }
  flush_binlog_last();
  fsync (Binlog->fd);
  if (lock_whole_file (Binlog->fd, F_UNLCK) <= 0) {
    return -2;
  }
  if (verbosity > 0) {
    fprintf (stderr, "released write lock for binlog, start reading...\n");
  }

  clear_read_log();

  //set_log_data (fd[2], fsize[2]);

  epoll_pre_event = read_new_events;

  binlog_disabled = 2;
  clear_write_log();

  return 1;*/
}


void dump_lists (int mode) {
  int max_l = (max_uid + 1) * log_split_mod;
  int x, c, i, res;

  for (x = w_split_rem; x < max_l; x += w_split_mod) {
    res = prepare_friends (x, -1, 0);
    c = R_end - R;
    if (c > MAX_RES/2) {
      c = MAX_RES/2;
    }
    if (res > 0 && c > 0) {
      for (i = c - 1; i >= 0; i--) {
        R[2*i+1] = R[i];
        R[2*i] = x;
      }
      assert (write (1, R, c * 8) == c * 8);
    }
    if (!mode) {
      continue;
    }
    res = prepare_friend_requests (x, -2);
    c = R_end - R;
    if (c > MAX_RES/2) {
      c = MAX_RES/2;
    }
    if (res > 0 && c > 0) {
      for (i = c - 1; i >= 0; i--) {
        R[2*i+1] = -R[i];
        R[2*i] = x;
      }
      assert (write (1, R, c * 8) == c * 8);
    }
  }
}

/*
 *
 *		MAIN
 *
 */

int index_mode;

void usage (void) {
  printf ("%s\n", FullVersionStr);
  printf ("usage: %s [-v] [-r] [-i] [-p<port>] [-u<username>] [-b<backlog>] [-c<max-conn>] [-a<binlog-name>] [-l<log-name>] [-A] <huge-index-file> [<metaindex-file>]\n"
	  "\tPerforms friends and privacy queries using given indexes\n",
	  progname);
  parse_usage ();
  exit (2);
}

int f_parse_option (int val) {
  long long x;
  char c;
  switch (val) {
  case 'I':
    ignored_delete_user_id = atoi(optarg);
    break;
  case 'W':
    assert (sscanf(optarg, "%d,%d", &w_split_rem, &w_split_mod) == 2);
    assert (w_split_mod > 0 && w_split_mod <= 10000 && w_split_rem >= 0 && w_split_rem < w_split_mod);
    break;
  case 'A':
    reverse_friends_mode = 1;
    binlog_disabled ++;
    break;
  case 'H':
    c = 0;
    assert (sscanf (optarg, "%lld%c", &x, &c) >= 1);
    switch (c | 0x20) {
      case 'k':  x <<= 10; break;
      case 'm':  x <<= 20; break;
      case 'g':  x <<= 30; break;
      case 't':  x <<= 40; break;
      default: assert (c == 0x20);
    }
    if (val == 'H' && x >= (1LL << 20) && x <= (sizeof(long) == 4 ? (3LL << 30) : (100LL << 30))) {
      dynamic_data_buffer_size = x;
    }
    break;
  case 'i':
    index_mode ++;
    break;
  default:
    return -1;
  }
  return 0;
}

char *aes_pwd_file;
int main (int argc, char *argv[]) {
  int i;

  signal (SIGRTMAX, sigrtmax_handler);
  set_debug_handlers ();

  parse_option ("test-mode", no_argument, 0, 'T', "test mode");
  parse_option ("ignore-delete-user", required_argument, 0, 'I', 0);
  parse_option ("dump-lists", required_argument, 0, 'W', "argument rem,mod. Dumps lists with specified rem mod mod");
  parse_option ("reverse", no_argument, 0, 'A', "reverse friends mode");
  parse_option ("index", no_argument, &index_mode, 'i', "index mode");
  parse_option (0, required_argument, 0, 'H', "heap size");

  progname = argv[0];
  parse_engine_options_long (argc, argv, f_parse_option);
  if (argc != optind + 1 && argc != optind + 2) {
    usage();
    return 2;
  }

  if (binlog_disabled > 1 || reverse_friends_mode) {
    binlog_disabled = 1;
  }

  if (strlen (argv[0]) >= 5 && memcmp ( argv[0] + strlen (argv[0]) - 5, "index" , 5) == 0) {
    index_mode = 1;
  }

  if (!w_split_mod && raise_file_rlimit(maxconn + 16) < 0 && !test_mode) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn+16);
    exit(1);
  }

  if (!w_split_mod && port < PRIVILEGED_TCP_PORTS) {
    sfd = server_socket (port, settings_addr, backlog, 0);
    if (sfd < 0) {
      fprintf(stderr, "cannot open server socket at port %d: %m\n", port);
      exit(1);
    }
  }

  aes_load_pwd_file (aes_pwd_file);

  if (w_split_mod) {
    binlog_disabled = 1;
  }

  if (change_user(username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit(1);
  }

  init_dyn_data();
  if (udp_enabled) {
    init_server_PID (get_my_ipv4 (), port);
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }
  
  init_common_data (0, index_mode ? CD_INDEXER : CD_ENGINE);
  cstatus_binlog_name (engine_replica->replica_prefix);

  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = strdup (Snapshot->info->filename);
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
    fprintf (stderr, "load index: done, jump_log_pos=%lld, time %.06lfs\n",
       jump_log_pos, index_load_time);
  }

  close_snapshot (Snapshot, 1);

  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, 0LL);
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
  binlog_loaded_size = log_readto_pos;

  /*if (binlog_disabled != 1) {
    jump_log_pos = log_readto_pos;
    jump_log_ts = log_last_ts;
    jump_log_crc32 = ~log_crc32_complement;
    assert (jump_log_pos == log_crc32_pos);
    clear_read_log();
    close_binlog (Binlog);
    Binlog = 0;
  }*/

  if (binlog_disabled != 1) {
    clear_read_log();
  }

  clear_write_log();
  start_time = time(0);

  if (w_split_mod) {
    dump_lists (test_mode);
    return 0;
  }

  if (index_mode) {
    save_index (0);
  } else {
    start_server();
  }
  return 0;
}
