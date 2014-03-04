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
              2011-2013 Arseny Smirnov
              2011-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <signal.h>

#include "crc32.h"
#include "base64.h"
#include "kdb-data-common.h"
#include "net-events.h"
#include "net-buffers.h"
#include "server-functions.h"
#include "net-connections.h"
#include "net-memcache-server.h"
#include "net-crypto-aes.h"

#include "net-rpc-server.h"
#include "net-rpc-common.h"

#include "photo-data.h"
#include "dl-utils.h"
#include "dl-set-get.h"

#include "common-data.h"

#define VERSION "0.91"
#define VERSION_STR "photo "VERSION

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define TCP_PORT 11211
#define UDP_PORT 11211

/** stats vars **/
#define STATS_BUFF_SIZE (1 << 16)
int stats_buff_len;
char stats_buff[STATS_BUFF_SIZE];
int photo_prepare_stats (void);

/** server vars **/
int port = TCP_PORT, udp_port = UDP_PORT;
struct in_addr settings_addr;
int interactive = 0;

volatile int sigpoll_cnt;

/** index vars **/
double index_load_time;

/** binlog vars **/
long long binlog_loaded_size;
double binlog_load_time;

/***
  MEMCACHED interface
 ***/
#define MAX_VALUE_LEN (1 << 20)
char buf[MAX_VALUE_LEN];

/** stats **/
long long cmd_get, cmd_set, cmd_delete, cmd_sync, cmd_stats, cmd_version;
double cmd_get_time = 0.0, cmd_set_time = 0.0, cmd_delete_time = 0.0, cmd_sync_time = 0.0;
double max_cmd_get_time = 0.0, max_cmd_set_time = 0.0, max_cmd_delete_time = 0.0, max_cmd_sync_time = 0.0;

int photo_engine_wakeup (struct connection *c);
int photo_engine_alarm (struct connection *c);

conn_type_t ct_photo_engine_server = {
  .magic = CONN_FUNC_MAGIC,
  .title = "photo_engine_server",
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
  .wakeup = photo_engine_wakeup,
  .alarm = photo_engine_alarm,
#ifndef NOAES
  .crypto_init = aes_crypto_init,
  .crypto_free = aes_crypto_free,
  .crypto_encrypt_output = aes_crypto_encrypt_output,
  .crypto_decrypt_input = aes_crypto_decrypt_input,
  .crypto_needed_output_bytes = aes_crypto_needed_output_bytes,
#endif
};

int memcache_delete (struct connection *c, const char *old_key, int old_key_len);
int memcache_store (struct connection *c, int op, const char *key, int key_len, int flags, int delay, int size);
int memcache_get (struct connection *c, const char *key, int key_len);
int memcache_stats (struct connection *c);
int memcache_version (struct connection *c);
int memcache_get_start (struct connection *c);
int memcache_get_end (struct connection *c, int key_count);

struct memcache_server_functions memcache_methods = {
  .execute = mcs_execute,
  .mc_store = memcache_store,
  .mc_get_start = memcache_get_start,
  .mc_get = memcache_get,
  .mc_get_end = memcache_get_end,
  .mc_incr = mcs_incr,
  .mc_delete = memcache_delete,
  .mc_version = memcache_version,
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
  .memcache_fallback_type = &ct_photo_engine_server,
  .memcache_fallback_extra = &memcache_methods
};


int return_one_key_flags (struct connection *c, const char *key, char *val, int vlen, int flags) {
  int l = snprintf (stats_buff, STATS_BUFF_SIZE, "VALUE %s %d %d\r\n", key, flags, vlen);
  assert (l <= STATS_BUFF_SIZE);
  write_out (&c->Out, stats_buff, l);
  write_out (&c->Out, val, vlen);
  write_out (&c->Out, "\r\n", 2);
  return 0;
}

static inline void eat_at (const char *key, int key_len, char **new_key, int *new_len) {
  if (*key == '^' || *key == '!') {
    key++;
    key_len--;
  }

  *new_key = (char *)key;
  *new_len = key_len;

  if ((*key >= '0' && *key <= '9') || (*key == '-' && key[1] >= '0' && key[1] <= '9')) {
    key++;
    while (*key >= '0' && *key <= '9') {
      key++;
    }

    if (*key++ == '@') {
      if (*key == '^' || *key == '!') {
        key++;
      }

      *new_len -= (key - *new_key);
      *new_key = (char *)key;
    }
  }
}

// result of read_in will always be a string with length of len
// data must have size at least (len + 1)
static inline void safe_read_in (netbuffer_t *H, char *data, int len) {
  assert (read_in (H, data, len) == len);
  int i;
  for (i = 0; i < len; i++) {
    if (data[i] == 0) {
      data[i] = ' ';
    }
  }
  data[len] = 0;
}


// -2 -- NOT_STORED and not readed
//  0 -- NOT_STORED
//  1 -- STORED
int memcache_store (struct connection *c, int op, const char *old_key, int old_key_len, int flags, int delay, int size) {
  INIT;

  hst ("memcache_store: key='%s', key_len=%d, value_len=%d\n", old_key, old_key_len, size);

  if (unlikely (size >= MAX_VALUE_LEN)) {
    RETURN (set, -2);
  }

  char *key;
  int key_len;
  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 12 && !strncmp (key, "change_", 7) && !strncmp (key + 7, mode_names[mode], 5)) {
    int user_id, photo_id, cur;
    safe_read_in (&c->In, buf, size);

    if (sscanf (key + 12, "%d,%d%n", &user_id, &photo_id, &cur) < 2 || key[12 + cur]) {
      RETURN (set, 0);
    }

    int res = do_change_photo (user_id, photo_id, buf);
    RETURN (set, res);
  }

  if (key_len >= 12 && !strncmp (key, "change_album", 12)) {
    int user_id, album_id, cur;
    safe_read_in (&c->In, buf, size);

    if (sscanf (key + 12, "%d,%d%n", &user_id, &album_id, &cur) < 2 || key[12 + cur]) {
      RETURN (set, 0);
    }

    int res = do_change_album (user_id, album_id, buf);
    RETURN (set, res);
  }

  if (key_len >= 22 && !strncmp (key, mode_names[mode], 5) && !strncmp (key + 5, "s_overview_albums", 17)) {
    int random_tag, cur = -1;
    if (sscanf (key + 22, "%*d,%d%n", &random_tag, &cur) != 1 || key[22 + cur]) {
      RETURN (get, -2);
    }

    if (msg_reinit (MESSAGE (c), size, random_tag) < 0) {
      RETURN (get, -2);
    }
    safe_read_in (&c->In, msg_get_buf (MESSAGE (c)), size);

    RETURN (get, 1);
  }

  RETURN (set, -2);
}

#define OK "OK"
#define NOK "NOK"

// -2 -- wait for AIO
//  0 -- all other cases
int memcache_try_get (struct connection *c, const char *old_key, int old_key_len) {
  hst ("memcache_get: key='%s', key_len=%d\n", old_key, old_key_len);

  char *key;
  int key_len;
  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 5 && !strncmp (key, "stats", 5)) {
    int len = photo_prepare_stats();
    int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
    return_one_key (c, old_key, stats_buff, len + len2 - 1);

    return 0;
  }

  if (key_len >= 11 && !strncmp (key, "synchronize", 11)) {
    INIT;

    if (write_only && !key[11]) {
      flush_binlog();

      char *ret = dl_pstr ("%lld", log_pos + compute_uncommitted_log_bytes());
      return_one_key (c, old_key, ret, strlen (ret));
    }

    long long binlog_pos = -1;
    int cur;
    if (binlog_disabled && sscanf (key + 11, "%lld%n", &binlog_pos, &cur) == 1 && !key[cur + 11]) {
      int iter = 0;
      while (log_pos < binlog_pos) {
        if (++iter % 1000 == 0 && mytime() + cmd_time > 0.5) {
          break;
        }
        read_new_events();
      }
      if (log_pos < binlog_pos) {
        return_one_key (c, old_key, NOK, strlen (NOK));
      } else {
        return_one_key (c, old_key, OK, strlen (OK));
      }
    }

    RETURN (sync, 0);
  }

  GET_LOG;

  SET_LOG_VERBOSITY;

  INIT;

  if (key_len >= 10 && !strncmp (key, "set_volume", 10)) {
    int volume_id, server_id, cur;
    if (sscanf (key + 10, "%d,%d%n", &volume_id, &server_id, &cur) < 2 || key[cur + 10]) {
      RETURN (set, 0);
    }

    int res = do_set_volume (volume_id, server_id);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));
    }

    RETURN (set, 0);
  }

  if (key_len >= 25 && !strncmp (key, "add_", 4) && !strncmp (key + 4, mode_names[mode], 5) && (!strncmp (key + 9, "_location_engine", 16) || !strncmp (key + 9, "_original_location_engine", 25))) {
    int user_id, photo_id, original = (key[10] == 'o'), volume_id, local_id, extra, cur, add = 25 + 9 * original;
    char size;
    unsigned long long secret;
    char base64url_secret[12];
    int rotate = 0;
    if ('0' <= key[add] && key[add] <= '3') {
      rotate = key[add++] - '0';
    }
    if (sscanf (key + add, "_%c%d,%d,%d,%x,%d,%11[0-9A-Za-z_-]%n", &size, &user_id, &photo_id, &volume_id, &local_id, &extra, base64url_secret, &cur) < 7 ||
                key[cur + add] || base64url_to_secret (base64url_secret, &secret)) {
      RETURN (set, 0);
    }

    int res = do_add_photo_location_engine (user_id, photo_id, original, size, rotate, volume_id, local_id, extra, secret);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 25 && !strncmp (key, "del_", 4) && !strncmp (key + 4, mode_names[mode], 5) && (!strncmp (key + 9, "_location_engine", 16) || !strncmp (key + 9, "_original_location_engine", 25))) {
    int user_id, photo_id, original = (key[10] == 'o'), rotate = -1, cur, add = 25 + 9 * original;
    char size = -1;
    if ('0' <= key[add] && key[add] <= '3') {
      rotate = key[add++] - '0';
    }
    if (key[add++] != '_') {
      RETURN (delete, 0);
    }
    if ('a' <= key[add] && key[add] <= 'z') {
      size = key[add++];
    }
    if (sscanf (key + add, "%d,%d%n", &user_id, &photo_id, &cur) < 2 || key[cur + add]) {
      RETURN (delete, 0);
    }

    int res = do_del_photo_location_engine (user_id, photo_id, original, size, rotate);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (delete, 0);
    }

    RETURN (delete, res);
  }

  if (key_len >= 18 && !strncmp (key, "add_", 4) && !strncmp (key + 4, mode_names[mode], 5) && (!strncmp (key + 9, "_location", 9) || !strncmp (key + 9, "_original_location", 18))) {
    int user_id, photo_id, original = (key[10] == 'o'), server_id, server_id2, orig_owner_id, orig_album_id, cur, add = 18 + 9 * original;
    if (sscanf (key + add, "%d,%d,%d,%d,%d,%d%n", &user_id, &photo_id, &server_id, &server_id2, &orig_owner_id, &orig_album_id, &cur) < 6 || key[cur + add] != '(' || key[key_len - 1] != ')') {
      RETURN (set, 0);
    }
    dbg ("do_add_photo_location (user_id = %d)\n", user_id);
    int res = do_add_photo_location (user_id, photo_id, original, server_id, server_id2, orig_owner_id, orig_album_id, key + add + cur + 1, key_len - 1 - cur - add - 1);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 28 && !strncmp (key, "change_", 7) && !strncmp (key + 7, mode_names[mode], 5) && (!strncmp (key + 12, "_location_server", 16) || !strncmp (key + 12, "_original_location_server", 25))) {
    int user_id, photo_id, original = (key[13] == 'o'), server_num = 0, server_id = -1, cur, add = 28 + 9 * original;
    if (sscanf (key + add, "%d,%d,%d%n,%d%n", &user_id, &photo_id, &server_id, &cur, &server_num, &cur) < 3 || key[cur + add]) {
      RETURN (set, 0);
    }
    if (server_num >= 1) {
      server_num--;
    }

    int res = do_change_photo_location_server (user_id, photo_id, original, server_num, server_id);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 18 && !strncmp (key, "del_", 4) && !strncmp (key + 4, mode_names[mode], 5) && (!strncmp (key + 9, "_location", 9) || !strncmp (key + 9, "_original_location", 18))) {
    int user_id, photo_id, original = (key[10] == 'o'), cur, add = 18 + 9 * original;
    if (sscanf (key + add, "%d,%d%n", &user_id, &photo_id, &cur) < 2 || key[cur + add]) {
      RETURN (delete, 0);
    }

    int res = do_del_photo_location (user_id, photo_id, original);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (delete, 0);
    }

    RETURN (delete, res);
  }

  if (key_len >= 19 && !strncmp (key, "save_", 5) && !strncmp (key + 5, mode_names[mode], 5) && !strncmp (key + 10, "_location", 9)) {
    int user_id, photo_id, cur;
    if (sscanf (key + 19, "%d,%d%n", &user_id, &photo_id, &cur) < 2 || key[cur + 19]) {
      RETURN (set, 0);
    }

    int res = do_save_photo_location (user_id, photo_id);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 22 && !strncmp (key, "restore_", 8) && !strncmp (key + 8, mode_names[mode], 5) && !strncmp (key + 13, "_location", 9)) {
    int user_id, photo_id, cur;
    if (sscanf (key + 22, "%d,%d%n", &user_id, &photo_id, &cur) < 2 || key[cur + 22]) {
      RETURN (set, 0);
    }

    int res = do_restore_photo_location (user_id, photo_id);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 12 && !strncmp (key, "rotate_", 7) && !strncmp (key + 7, mode_names[mode], 5)) {
    int dir, user_id, photo_id, cur;
    if (sscanf (key + 12, "%d,%d,%d%n", &user_id, &photo_id, &dir, &cur) < 3 || key[cur + 12]) {
      RETURN (set, 0);
    }

    int res = do_rotate_photo (user_id, photo_id, dir);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 18 && !strncmp (key, "change_", 7) && !strncmp (key + 7, mode_names[mode], 5) && !strncmp (key + 12, "_order", 6)) {
    int add = 18, is_next = 0;
    if (!strncmp (key + add, "_next", 5)) {
      is_next = 1;
      add += 5;
    }

    int user_id, photo_id, photo_id_near, cur;
    if (sscanf (key + add, "%d,%d,%d%n", &user_id, &photo_id, &photo_id_near, &cur) != 3 || key[cur + add]) {
      RETURN (set, 0);
    }

    int res = do_change_photo_order (user_id, photo_id, photo_id_near, is_next);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 18 && !strncmp (key, "change_album_order", 18)) {
    int add = 18, is_next = 0;
    if (!strncmp (key + add, "_next", 5)) {
      is_next = 1;
      add += 5;
    }

    int user_id, album_id, album_id_near, cur;
    if (sscanf (key + add, "%d,%d,%d%n", &user_id, &album_id, &album_id_near, &cur) != 3 || key[cur + add]) {
      RETURN (set, 0);
    }

    int res = do_change_album_order (user_id, album_id, album_id_near, is_next);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 15 && !strncmp (key, "new_", 4) && !strncmp (key + 4, mode_names[mode], 5) && !strncmp (key + 9, "_force", 6)) {
    int user_id, album_id, photo_id, cur, cnt = 1;
    if (sscanf (key + 15, "%d,%d,%d%n,%d%n", &user_id, &album_id, &photo_id, &cur, &cnt, &cur) < 3 || cnt <= 0 || cnt > MAX_PHOTOS || key[cur + 15]) {
      RETURN (set, 0);
    }

    int res = do_create_photo_force (user_id, album_id, cnt, photo_id);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 15 && !strncmp (key, "new_album_force", 15)) {
    int user_id, album_id, cur;
    if (sscanf (key + 15, "%d,%d%n", &user_id, &album_id, &cur) != 2 || key[cur + 15]) {
      RETURN (set, 0);
    }

    int res = do_create_album_force (user_id, album_id);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 9 && !strncmp (key, "new_", 4) && !strncmp (key + 4, mode_names[mode], 5)) {
    int user_id, album_id, cur, cnt = 1;
    if (sscanf (key + 9, "%d,%d%n,%d%n", &user_id, &album_id, &cur, &cnt, &cur) < 2 || cnt <= 0 || cnt > MAX_PHOTOS || key[cur + 9]) {
      RETURN (set, 0);
    }

    int res = do_create_photo (user_id, album_id, cnt);

    if (res >= 0) {
      char *ret = dl_pstr ("%d", res);
      return_one_key (c, old_key, ret, strlen (ret));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 9 && !strncmp (key, "new_album", 9)) {
    int user_id, cur;
    if (sscanf (key + 9, "%d%n", &user_id, &cur) != 1 || key[cur + 9]) {
      RETURN (set, 0);
    }

    int res = do_create_album (user_id);

    if (res >= 0) {
      char *ret = dl_pstr ("%d", res);
      return_one_key (c, old_key, ret, strlen (ret));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  if (key_len >= 22 && !strncmp (key, mode_names[mode], 5) && !strncmp (key + 5, "s_overview_albums", 17)) {
    int add = 22, need_reverse = 0, need_count = 0;
    if (!strncmp (key + add, "_reverse", 8)) {
      need_reverse = 1;
      add += 8;
    }
    if (!strncmp (key + add, "_cnt", 4)) {
      need_count = 1;
      add += 4;
    }

    int user_id, offset = MAX_PHOTOS_DYN, limit = -7, cur;
    if (sscanf (key + add, "%d%n#%d%n,%d%n", &user_id, &cur, &offset, &cur, &limit, &cur) < 1 || key[cur + add] != '(' || key[key_len - 1] != ')') {
      RETURN (get, 0);
    }

    if (limit == -7) {
      limit = offset;
      offset = 0;
    }

    key[key_len - 1] = 0;
    char *result = NULL;
    int res = get_photos_overview (user_id, key + add + cur + 1, offset, limit, "", need_reverse, need_count, &result);
    key[key_len - 1] = ')';

    if (res > 0) {
      return_one_key_flags (c, old_key, result, strlen (result), 1);

      RETURN (get, 0);
    }

    RETURN (get, res);
  }

  if (key_len >= 15 && !strncmp (key, mode_names[mode], 5) && !strncmp (key + 5, "s_overview", 10)) {
    int add = 15, need_reverse = 0, need_count = 0;
    if (!strncmp (key + add, "_reverse", 8)) {
      need_reverse = 1;
      add += 8;
    }
    if (!strncmp (key + add, "_cnt", 4)) {
      need_count = 1;
      add += 4;
    }

    int user_id, offset = MAX_PHOTOS_DYN, limit = -7, random_tag, cur;
    if (sscanf (key + add, "%d,%d%n#%d%n,%d%n", &user_id, &random_tag, &cur, &offset, &cur, &limit, &cur) < 2 || key[cur + add] != '(' || key[key_len - 1] != ')') {
      RETURN (get, 0);
    }

    if (limit == -7) {
      limit = offset;
      offset = 0;
    }

    message *msg = MESSAGE (c);

    if (msg_verify (msg, random_tag) < 0) {
      RETURN (get, 0);
    }
    if (limit <= 0) {
      if (need_count) {
        limit = 0;
      } else {
        msg_free (msg);
        RETURN (get, 0);
      }
    }

    key[key_len - 1] = 0;
    char *result = NULL;
    int res = get_photos_overview (user_id, msg->text, offset, limit, key + add + cur + 1, need_reverse, need_count, &result);
    key[key_len - 1] = ')';

    if (res != -2) {
      msg_free (msg);
    }
    if (res > 0) {
      return_one_key_flags (c, old_key, result, strlen (result), 1);

      RETURN (get, 0);
    }

    RETURN (get, res);
  }

  if (key_len >= 21 && (!strncmp (key, "in", 2) || !strncmp (key, "de", 2)) && !strncmp (key + 2, "crement_", 8) && !strncmp (key + 10, mode_names[mode], 5) && !strncmp (key + 15, "_field", 6)) {
    int user_id, photo_id, cnt = 1, cur;
    if (sscanf (key + 21, "%d,%d%n,%d%n", &user_id, &photo_id, &cur, &cnt, &cur) < 2 || key[cur + 21] != '(' || key[key_len - 1] != ')') {
      RETURN (set, 0);
    }
    if (key[0] == 'd') {
      cnt = -cnt;
    }

    key[key_len - 1] = 0;
    int res = do_increment_photo_field (user_id, photo_id, key + cur + 22, cnt);
    key[key_len - 1] = ')';

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res == -2 ? -2 : 0);
  }

  if (key_len >= 21 && (!strncmp (key, "in", 2) || !strncmp (key, "de", 2)) && !strncmp (key + 2, "crement_album_field", 19)) {
    int user_id, album_id, cnt = 1, cur;
    if (sscanf (key + 21, "%d,%d%n,%d%n", &user_id, &album_id, &cur, &cnt, &cur) < 2 || key[cur + 21] != '(' || key[key_len - 1] != ')') {
      RETURN (set, 0);
    }
    if (key[0] == 'd') {
      cnt = -cnt;
    }

    key[key_len - 1] = 0;
    int res = do_increment_album_field (user_id, album_id, key + cur + 22, cnt);
    key[key_len - 1] = ')';

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res == -2 ? -2 : 0);
  }

  if (!write_only && key_len >= 12 && !strncmp (key, mode_names[mode], 5) && !strncmp (key + 5, "s_count", 7)) {
    int user_id, album_id, cur;
    if (sscanf (key + 12, "%d,%d%n", &user_id, &album_id, &cur) < 2 || (key[cur + 12] && (key[cur + 12] != '(' || key[key_len - 1] != ')'))) {
      RETURN (get, 0);
    }

    char *condition = NULL;
    if (key[cur + 12] == '(') {
      key[key_len - 1] = 0;
      condition = key + cur + 13;
    }

    int res = get_photos_count (user_id, album_id, condition);
    if (condition != NULL) {
      key[key_len - 1] = ')';
    }

    if (res >= 0) {
      char *ret = dl_pstr ("%d", res);
      return_one_key (c, old_key, ret, strlen (ret));

      RETURN (get, 0);
    }

    RETURN (get, res == -2 ? -2 : 0);
  }

  if (!write_only && key_len >= 12 && !strncmp (key, "albums_count", 12)) {
    int user_id, cur;
    if (sscanf (key + 12, "%d%n", &user_id, &cur) < 1 || (key[cur + 12] && (key[cur + 12] != '(' || key[key_len - 1] != ')'))) {
      RETURN (get, 0);
    }

    char *condition = NULL;
    if (key[cur + 12] == '(') {
      key[key_len - 1] = 0;
      condition = key + cur + 13;
    }

    int res = get_albums_count (user_id, condition);
    if (condition != NULL) {
      key[key_len - 1] = ')';
    }

    if (res >= 0) {
      char *ret = dl_pstr ("%d", res);
      return_one_key (c, old_key, ret, strlen (ret));

      RETURN (get, 0);
    }

    RETURN (get, res == -2 ? -2 : 0);
  }

  if (!write_only && key_len >= 6 && !strncmp (key, mode_names[mode], 5) && !strncmp (key + 5, "s", 1)) {
    int add = 6, need_reverse = 0, need_count = 0;
    if (!strncmp (key + add, "_reverse", 8)) {
      need_reverse = 1;
      add += 8;
    }
    if (!strncmp (key + add, "_cnt", 4)) {
      need_count = 1;
      add += 4;
    }

    int user_id, album_id, offset = MAX_PHOTOS_DYN, limit = -7, cur;
    if (sscanf (key + add, "%d,%d%n#%d%n,%d%n", &user_id, &album_id, &cur, &offset, &cur, &limit, &cur) < 2 || key[cur + add] != '(' || key[key_len - 1] != ')') {
      RETURN (get, 0);
    }

    if (limit == -7) {
      limit = offset;
      offset = 0;
    }

    key[key_len - 1] = 0;
    char *fields = key + add + cur + 1;
    char *condition = strchr (fields, '|');
    if (condition != NULL) {
      *condition++ = 0;
    }

    char *result = NULL;
    int res = get_photos (user_id, album_id, offset, limit, fields, need_reverse, need_count, condition, &result);

    if (condition != NULL) {
      condition[-1] = '|';
    }
    key[key_len - 1] = ')';

    if (res > 0) {
      return_one_key_flags (c, old_key, result, strlen (result), 1);

      RETURN (get, 0);
    }

    RETURN (get, res);
  }

  if (!write_only && key_len >= 6 && !strncmp (key, "albums", 6)) {
    int add = 6, need_reverse = 0, need_count = 0;
    if (!strncmp (key + add, "_reverse", 8)) {
      need_reverse = 1;
      add += 8;
    }
    if (!strncmp (key + add, "_cnt", 4)) {
      need_count = 1;
      add += 4;
    }

    int user_id, offset = MAX_ALBUMS, limit = -7, cur;
    if (sscanf (key + add, "%d%n#%d%n,%d%n", &user_id, &cur, &offset, &cur, &limit, &cur) < 1 || key[cur + add] != '(' || key[key_len - 1] != ')') {
      RETURN (get, 0);
    }

    if (limit == -7) {
      limit = offset;
      offset = 0;
    }

    key[key_len - 1] = 0;
    char *fields = key + add + cur + 1;
    char *condition = strchr (fields, '|');
    if (condition != NULL) {
      *condition++ = 0;
    }

    char *result = NULL;
    int res = get_albums (user_id, offset, limit, fields, need_reverse, need_count, condition, &result);

    if (condition != NULL) {
      condition[-1] = '|';
    }
    key[key_len - 1] = ')';

    if (res > 0) {
      return_one_key_flags (c, old_key, result, strlen (result), 1);

      RETURN (get, 0);
    }

    RETURN (get, res);
  }

  if (!write_only && key_len >= 5 && !strncmp (key, mode_names[mode], 5)) {
    int add = 5, need_force = 0;
    if (!strncmp (key + add, "_force", 6)) {
      need_force = 1;
      add += 6;
    }

    int user_id, photo_id, cur;
    if (sscanf (key + add, "%d,%d%n", &user_id, &photo_id, &cur) < 2 || key[cur + add] != '(' || key[key_len - 1] != ')') {
      RETURN (get, 0);
    }

    key[key_len - 1] = 0;
    char *result = NULL;
    int res = get_photo (user_id, photo_id, need_force, key + add + cur + 1, &result);
    key[key_len - 1] = ')';

    if (res > 0) {
      return_one_key_flags (c, old_key, result, strlen (result), 1);

      RETURN (get, 0);
    }

    RETURN (get, res);
  }

  if (!write_only && key_len >= 5 && !strncmp (key, "album", 5)) {
    int add = 5;

    int user_id, album_id, cur;
    if (sscanf (key + add, "%d,%d%n", &user_id, &album_id, &cur) < 2 || key[cur + add] != '(' || key[key_len - 1] != ')') {
      RETURN (get, 0);
    }

    key[key_len - 1] = 0;
    char *result = NULL;
    int res = get_album (user_id, album_id, 0, key + add + cur + 1, &result);
    key[key_len - 1] = ')';

    if (res > 0) {
      return_one_key_flags (c, old_key, result, strlen (result), 1);

      RETURN (get, 0);
    }

    RETURN (get, res);
  }


  if (key_len >= 13 && !strncmp (key, "restore_", 8) && !strncmp (key + 8, mode_names[mode], 5)) {
    int user_id, photo_id, cur;
    if (sscanf (key + 13, "%d,%d%n", &user_id, &photo_id, &cur) < 2 || key[cur + 13]) {
      RETURN (set, 0);
    }

    int res = do_restore_photo (user_id, photo_id);

    if (res > 0) {
      return_one_key (c, old_key, OK, strlen (OK));

      RETURN (set, 0);
    }

    RETURN (set, res);
  }

  RETURN (get, 0);
}

conn_query_type_t aio_metafile_query_type = {
  .magic = CQUERY_FUNC_MAGIC,
  .title = "photo-data-aio-metafile-query",
  .wakeup = aio_query_timeout,
  .close = delete_aio_query,
  .complete = delete_aio_query
};

int memcache_get (struct connection *c, const char *key, int key_len) {
  WaitAioArrClear();
  int res = memcache_try_get (c, key, key_len);

  if (res == -2) {
    if (c->flags & C_INTIMEOUT) {
      if (verbosity > 1) {
        fprintf (stderr, "memcache_get: IN TIMEOUT (%p)\n", c);
      }
      return 0;
    }

    if (c->Out.total_bytes > 8192) {
      c->flags |= C_WANTWR;
      c->type->writer (c);
    }

    assert (WaitAioArrPos);

    int i;
    for (i = 0; i < WaitAioArrPos; i++) {
      assert (WaitAioArr[i]);
      conn_schedule_aio (WaitAioArr[i], c, 0.7, &aio_metafile_query_type);
    }
    set_connection_timeout (c, 0.5);
    return 0;
  }

  assert (res == 0);
  return 0;
}

int memcache_get_start (struct connection *c) {
  c->flags &= ~C_INTIMEOUT;
  c->query_start_time = mytime();
  return 0;
}

int memcache_get_end (struct connection *c, int key_count) {
  c->flags &= ~C_INTIMEOUT;
  c->last_query_time = mytime() - c->query_start_time;
  write_out (&c->Out, "END\r\n", 5);
  if (verbosity > 1) {
    fprintf (stderr, "memcache_get end: query time %.3fms\n", c->last_query_time * 1000);
  }
  return 0;
}


int memcache_delete (struct connection *c, const char *old_key, int old_key_len) {
  INIT;

  hst ("memcache_delete: key='%s'\n", old_key);

  char *key;
  int key_len;

  eat_at (old_key, old_key_len, &key, &key_len);

  if (key_len >= 5 && !strncmp (key, mode_names[mode], 5)) {
    int user_id, photo_id, cur;
    if (sscanf (key + 5, "%d,%d%n", &user_id, &photo_id, &cur) == 2 && !key[cur + 5]) {
      if (do_delete_photo (user_id, photo_id)) {
        write_out (&c->Out, "DELETED\r\n", 9);
      } else {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
      }
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }

    RETURN(delete, 0);
  }

  if (key_len >= 5 && !strncmp (key, "album", 5)) {
    int user_id, album_id, cur;
    if (sscanf (key + 5, "%d,%d%n", &user_id, &album_id, &cur) == 2 && !key[cur + 5]) {
      if (do_delete_album (user_id, album_id)) {
        write_out (&c->Out, "DELETED\r\n", 9);
      } else {
        write_out (&c->Out, "NOT_FOUND\r\n", 11);
      }
    } else {
      write_out (&c->Out, "NOT_FOUND\r\n", 11);
    }

    RETURN(delete, 0);
  }

  write_out (&c->Out, "NOT_FOUND\r\n", 11);
  RETURN(delete, 0);
}



int memcache_version (struct connection *c) {
  cmd_version++;
  write_out (&c->Out, "VERSION "VERSION"\r\n", 9 + sizeof (VERSION));
  return 0;
}


int memcache_stats (struct connection *c) {
  int len = photo_prepare_stats();
  int len2 = prepare_stats (c, stats_buff + len, STATS_BUFF_SIZE - len);
  write_out (&c->Out, stats_buff, len + len2);
  write_out (&c->Out, "END\r\n", 5);
  return 0;
}

int photo_engine_wakeup (struct connection *c) {
  struct mcs_data *D = MCS_DATA (c);

  if (verbosity > 1) {
    fprintf (stderr, "photo_engine_wakeup (%p)\n", c);
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


int photo_engine_alarm (struct connection *c) {
  if (verbosity > 1) {
    fprintf (stderr, "photo_engine connection %d timeout alarm, %d queries pending, status=%d\n", c->fd, c->pending_queries, c->status);
  }
  assert (c->status == conn_wait_aio);
  c->flags |= C_INTIMEOUT;
  return photo_engine_wakeup (c);
}

//TODO buffer overflow in stats?

/***
  STATS
 ***/
int photo_prepare_stats (void) {
  cmd_stats++;
  int log_uncommitted = compute_uncommitted_log_bytes();

  char *s = stats_buff;
#define W(args...) WRITE_ALL (s, ## args)
  W ("version\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " after commit " COMMIT "\n");
  W ("mode\t%s\n",                       mode_names[mode]);

  /* z memory stats */
  W ("heap_used\t%ld\n",                 (long) (dyn_cur - dyn_first));
  W ("heap_max\t%ld\n",                  (long) (dyn_last - dyn_first));

  /* binlog stats */
  W ("binlog_original_size\t%lld\n",     log_readto_pos);
  W ("binlog_loaded_bytes\t%lld\n",      log_readto_pos - jump_log_pos);
  W ("binlog_load_time\t%.6lfs\n",       binlog_load_time);
  W ("current_binlog_size\t%lld\n",      log_pos);
  W ("binlog_uncommitted_bytes\t%d\n",   log_uncommitted);
  W ("binlog_path\t%s\n",                binlogname ? (strlen (binlogname) < 250 ? binlogname : "(too long)") : "(none)");
  W ("binlog_first_timestamp\t%d\n",     log_first_ts);
  W ("binlog_read_timestamp\t%d\n",      log_read_until);
  W ("binlog_last_timestamp\t%d\n",      log_last_ts);
  W ("max_binlog_size\t%lld\n",          max_binlog_size);

  /* index stats */
  W ("index_loaded_bytes\t%d\n",         header_size);
  W ("index_size\t%lld\n",               engine_snapshot_size);
  W ("index_path\t%s\n",                 engine_snapshot_name);
  W ("index_load_time\t%.6lfs\n",        index_load_time);

  /* users stats */
  W ("new_users\t%d\n",                  cur_local_id - index_users);
  W ("total_users\t%d\n",                cur_local_id);
  W ("max_users\t%d\n",                  user_cnt);
  W ("memory_users\t%d\n",               cur_users);
  W ("unloaded_users\t%d\n",             del_by_LRU);
  W ("load_users_cnt\t%lld\n",           cmd_load_user);
  W ("load_users_time\t%.7lf\n",         cmd_load_user_time);
  W ("max_load_user_time\t%.7lf\n",      max_cmd_load_user_time);
  W ("unload_users_cnt\t%lld\n",         cmd_unload_user);
  W ("unload_users_time\t%.7lf\n",       cmd_unload_user_time);
  W ("max_unload_user_time\t%.7lf\n",    max_cmd_unload_user_time);
  W ("total_photos\t%lld\n",             total_photos);

  /* misc stats */
  W ("pid\t%d\n",                        getpid());
  W ("version\t%s\n",                    VERSION);
  W ("pointer_size\t%d\n",               (int)(sizeof (void *) * 8));
  W ("cmd_get\t%lld\n",                  cmd_get);
  W ("cmd_get_time\t%.7lf\n",            cmd_get_time);
  W ("max_cmd_get_time\t%.7lf\n",        max_cmd_get_time);
  W ("cmd_set\t%lld\n",                  cmd_set);
  W ("cmd_set_time\t%.7lf\n",            cmd_set_time);
  W ("max_cmd_set_time\t%.7lf\n",        max_cmd_set_time);
  W ("cmd_delete\t%lld\n",               cmd_delete);
  W ("cmd_delete_time\t%.7lf\n",         cmd_delete_time);
  W ("max_cmd_delete_time\t%.7lf\n",     max_cmd_delete_time);
  W ("cmd_sync\t%lld\n",                 cmd_sync);
  W ("cmd_sync_time\t%.7lf\n",           cmd_sync_time);
  W ("max_cmd_sync_time\t%.7lf\n",       max_cmd_sync_time);
  W ("cmd_stats\t%lld\n",                cmd_stats);
  W ("cmd_version\t%lld\n",              cmd_version);
  W ("tot_aio_queries\t%lld\n",          tot_aio_queries);
  W ("active_aio_queries\t%lld\n",       active_aio_queries);
  W ("expired_aio_queries\t%lld\n",      expired_aio_queries);
  W ("avg_aio_query_time\t%.6f\n",       tot_aio_queries > 0 ? total_aio_time / tot_aio_queries : 0);

  /* memory stats */
  W ("allocated_metafile_bytes\t%lld\n", allocated_metafile_bytes);
  W ("other_memory\t%lld\n",             dl_get_memory_used() - allocated_metafile_bytes);
  W ("current_memory_used\t%lld\n",      dl_get_memory_used());
  W ("limit_max_dynamic_memory\t%ld\n",  max_memory);

  if (index_mode) {
    W ("uptime\t%d\n",                   (int)(time (NULL) - start_time));
  }

#undef W

  int len = s - stats_buff;
  assert (len < STATS_BUFF_SIZE);
  return len;
}


/*
 *
 *    RPC interface
 *
 */

void tl_check_get (void) {
  assert (!index_mode);
  if (write_only) {
    tl_fetch_set_error_format (-4001, "Get operations are unsupported in write_only mode");
  }
}

int tl_fetch_bool (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int bool = tl_fetch_int();
  if (bool == TL_BOOL_TRUE) {
    return 1;
  }
  if (bool != TL_BOOL_FALSE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Bool excpected, %x found", bool);
  }
  return 0;
}

int tl_fetch_user_id (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int uid = tl_fetch_int();
  if (!check_user_id (uid)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong server: user_id = %d, log_split_mod = %d, log_split_min = %d, log_split_max = %d", uid, log_split_mod, log_split_min, log_split_max);
  }

  return uid;
}

int tl_fetch_photo_id (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int pid = tl_fetch_int();
  if (!check_photo_id (pid)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong photo_id %d", pid);
  }
  return pid;
}

int tl_fetch_album_id (void) {
  if (tl_fetch_error()) {
    return 0;
  }
  int aid = tl_fetch_int();
  if (!check_album_id (aid)) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong album_id %d", aid);
  }
  return aid;
}

long long tl_fetch_mask (int *force) {
  if (tl_fetch_error()) {
    return 0;
  }
  long long mask = tl_fetch_int();
  if (mask >= 0) {
    *force = (mask >> 29) & 1;
    mask -= (*force << 29);

    if (mask & (1 << 30)) {
      long long mask2 = tl_fetch_int();
      if (mask2 >= 0) {
        return (mask - (1 << 30)) + (mask2 << 29);
      }
    } else {
      return mask;
    }
  }
  tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Wrong mask %lld", mask);
  return 0;
}


TL_DO_FUN(increment_photo_field)
  int res = do_increment_photo_field (e->user_id, e->photo_id, e->field_name, e->cnt);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(increment_album_field)
  int res = do_increment_album_field (e->user_id, e->album_id, e->field_name, e->cnt);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(set_volume_server)
  int res = do_set_volume (e->volume_id, e->server_id);
  tl_store_int (TL_BOOL_STAT);
  tl_store_int (res > 0);
  tl_store_int (res <= 0);
  tl_store_int (0);
TL_DO_FUN_END

TL_DO_FUN(delete_location_engine)
  int res = do_del_photo_location_engine (e->user_id, e->photo_id, e->original, e->size, e->rotate);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(delete_location)
  int res = do_del_photo_location (e->user_id, e->photo_id, e->original);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(change_location_server)
  int res = do_change_photo_location_server (e->user_id, e->photo_id, e->original, e->server_num, e->server_id);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(save_photo_location)
  int res = do_save_photo_location (e->user_id, e->photo_id);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(restore_photo_location)
  int res = do_restore_photo_location (e->user_id, e->photo_id);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(rotate_photo)
  int res = do_rotate_photo (e->user_id, e->photo_id, e->dir);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(change_photo_order)
  int res = do_change_photo_order (e->user_id, e->photo_id, e->id_near, e->is_next);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(change_album_order)
  int res = do_change_album_order (e->user_id, e->album_id, e->id_near, e->is_next);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(new_photo_force)
  int res = do_create_photo_force (e->user_id, e->album_id, 1, e->photo_id);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(new_photo)
  int res = do_create_photo (e->user_id, e->album_id, 1);
  if (res < 0) {
    return res;
  }

  if (res > 0) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(new_album_force)
  int res = do_create_album_force (e->user_id, e->album_id);
  if (res < 0) {
    return res;
  }

  if (res > 0) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(new_album)
  int res = do_create_album (e->user_id);
  if (res < 0) {
    return res;
  }

  if (res > 0) {
    tl_store_int (TL_MAYBE_TRUE);
    tl_store_int (res);
  } else {
    tl_store_int (TL_MAYBE_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(get_photos_count)
  int res = get_photos_count (e->user_id, e->album_id, e->condition);
  if (res < 0) {
    return res;
  }

  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
TL_DO_FUN_END

TL_DO_FUN(get_albums_count)
  int res = get_albums_count (e->user_id, e->condition);
  if (res < 0) {
    return res;
  }

  tl_store_int (TL_MAYBE_TRUE);
  tl_store_int (res);
TL_DO_FUN_END

TL_DO_FUN(restore_photo)
  int res = do_restore_photo (e->user_id, e->photo_id);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(delete_photo)
  int res = do_delete_photo (e->user_id, e->photo_id);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END

TL_DO_FUN(delete_album)
  int res = do_delete_album (e->user_id, e->album_id);
  if (res < 0) {
    return res;
  }

  if (res == 1) {
    tl_store_int (TL_BOOL_TRUE);
  } else {
    tl_store_int (TL_BOOL_FALSE);
  }
TL_DO_FUN_END


TL_PARSE_FUN(change_photo, void)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
  int force;
  e->mask = tl_fetch_mask (&force);
  e->changes_len = tl_fetch_unread();
  extra->size += e->changes_len;

  assert (e->changes_len % sizeof (int) == 0);

  if (e->changes_len == 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Changes are empty");
    return NULL;
  }
  if (e->changes_len >= MAX_EVENT_SIZE || extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Changes size is too large");
    return NULL;
  }

  tl_fetch_raw_data (e->changes, e->changes_len);
TL_PARSE_FUN_END

TL_PARSE_FUN(change_album, void)
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_photo_id();
  int force;
  e->mask = tl_fetch_mask (&force);
  e->changes_len = tl_fetch_unread();
  extra->size += e->changes_len;

  assert (e->changes_len % sizeof (int) == 0);

  if (e->changes_len == 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Changes are empty");
    return NULL;
  }
  if (e->changes_len >= MAX_EVENT_SIZE || extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Changes size is too large");
    return NULL;
  }

  tl_fetch_raw_data (e->changes, e->changes_len);
TL_PARSE_FUN_END

TL_PARSE_FUN(increment_photo_field, void)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
  extra->size += tl_fetch_string0 (e->field_name, STATS_BUFF_SIZE - extra->size - 1) + 1;
  e->cnt = tl_fetch_int();

  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Field name is too long");
    return NULL;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(increment_album_field, void)
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
  extra->size += tl_fetch_string0 (e->field_name, STATS_BUFF_SIZE - extra->size - 1) + 1;
  e->cnt = tl_fetch_int();

  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Field name is too long");
    return NULL;
  }
TL_PARSE_FUN_END

TL_PARSE_FUN(set_volume_server, void)
  e->volume_id = tl_fetch_int();
  e->server_id = tl_fetch_int();
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_location_engine, int original)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
  e->rotate = tl_fetch_int();

  char size[2];
  int size_len = tl_fetch_string (size, 2);
  if (size_len != 1) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Size must contain exactly 1 symbol");
    return NULL;
  }
  e->size = size[0];

  e->original = original;
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_location, int original)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
  e->original = original;
TL_PARSE_FUN_END

TL_PARSE_FUN(change_location_server, int original, int server_num)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
  e->server_id = tl_fetch_int();
  e->original = original;
  e->server_num = server_num;
TL_PARSE_FUN_END

TL_PARSE_FUN(save_photo_location, void)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(restore_photo_location, void)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(rotate_photo, void)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
  e->dir = tl_fetch_int();
TL_PARSE_FUN_END

TL_PARSE_FUN(change_photo_order, void)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
  e->id_near = tl_fetch_photo_id();
  e->is_next = tl_fetch_bool();
TL_PARSE_FUN_END

TL_PARSE_FUN(change_album_order, void)
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
  e->id_near = tl_fetch_album_id();
  e->is_next = tl_fetch_bool();
TL_PARSE_FUN_END

TL_PARSE_FUN(new_photo_force, void)
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
  e->photo_id = tl_fetch_photo_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(new_photo, void)
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(new_album_force, void)
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(new_album, void)
  e->user_id = tl_fetch_user_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(get_photos_overview, int need_count)
  tl_check_get();
  e->user_id = tl_fetch_user_id();

  e->albums_cnt = tl_fetch_int();
  extra->size += e->albums_cnt * sizeof (int);

  if (e->albums_cnt == 0) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "List of albums is empty");
    return NULL;
  }
  if (extra->size >= STATS_BUFF_SIZE) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Number of albums is too large");
    return NULL;
  }
  int i;
  for (i = 0; i < e->albums_cnt; i++) {
    e->albums[i] = tl_fetch_album_id();
  }

  int force;
  e->mask = tl_fetch_mask (&force);

  e->offset = tl_fetch_int();
  e->limit = tl_fetch_int();
  e->is_reverse = tl_fetch_bool();
  e->need_count = need_count;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_photos_count, void)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
  extra->size += tl_fetch_string0 (e->condition, STATS_BUFF_SIZE - extra->size - 1) + 1;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_albums_count, void)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  extra->size += tl_fetch_string0 (e->condition, STATS_BUFF_SIZE - extra->size - 1) + 1;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_photos, int need_count)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
  int force;
  e->mask = tl_fetch_mask (&force);
  e->offset = tl_fetch_int();
  e->limit = tl_fetch_int();
  e->is_reverse = tl_fetch_bool();
  e->need_count = need_count;
  extra->size += tl_fetch_string0 (e->condition, STATS_BUFF_SIZE - extra->size - 1) + 1;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_albums, int need_count)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  int force;
  e->mask = tl_fetch_mask (&force);
  e->offset = tl_fetch_int();
  e->limit = tl_fetch_int();
  e->is_reverse = tl_fetch_bool();
  e->need_count = need_count;
  extra->size += tl_fetch_string0 (e->condition, STATS_BUFF_SIZE - extra->size - 1) + 1;
TL_PARSE_FUN_END

TL_PARSE_FUN(get_photo, void)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
  e->mask = tl_fetch_mask (&e->force);
TL_PARSE_FUN_END

TL_PARSE_FUN(get_album, void)
  tl_check_get();
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
  e->mask = tl_fetch_mask (&e->force);
TL_PARSE_FUN_END

TL_PARSE_FUN(restore_photo, void)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_photo, void)
  e->user_id = tl_fetch_user_id();
  e->photo_id = tl_fetch_photo_id();
TL_PARSE_FUN_END

TL_PARSE_FUN(delete_album, void)
  e->user_id = tl_fetch_user_id();
  e->album_id = tl_fetch_album_id();
TL_PARSE_FUN_END


struct tl_act_extra *photo_parse_function (long long actor_id) {
  if (actor_id != 0) {
    tl_fetch_set_error ("Photo only supports actor_id = 0", TL_ERROR_WRONG_ACTOR_ID);
    return NULL;
  }

  int op = tl_fetch_int();
  if (tl_fetch_error()) {
    tl_fetch_set_error_format (TL_ERROR_BAD_VALUE, "Request is empty");
    return NULL;
  }

  switch (op) {
    case TL_PHOTO_CHANGE_PHOTO:
    case TL_PHOTO_CHANGE_AUDIO:
    case TL_PHOTO_CHANGE_VIDEO:
      return tl_change_photo();
    case TL_PHOTO_CHANGE_PHOTO_ALBUM:
    case TL_PHOTO_CHANGE_AUDIO_ALBUM:
    case TL_PHOTO_CHANGE_VIDEO_ALBUM:
      return tl_change_album();
    case TL_PHOTO_INCREMENT_PHOTO_FIELD:
    case TL_PHOTO_INCREMENT_AUDIO_FIELD:
    case TL_PHOTO_INCREMENT_VIDEO_FIELD:
      return tl_increment_photo_field();
    case TL_PHOTO_INCREMENT_ALBUM_FIELD:
      return tl_increment_album_field();
    case TL_PHOTO_SET_VOLUME_SERVER:
      return tl_set_volume_server();
    case TL_PHOTO_DELETE_LOCATION_STORAGE:
      return tl_delete_location_engine (0);
    case TL_PHOTO_DELETE_LOCATION:
      return tl_delete_location (0);
    case TL_PHOTO_DELETE_ORIGINAL_LOCATION_STORAGE:
      return tl_delete_location_engine (1);
    case TL_PHOTO_DELETE_ORIGINAL_LOCATION:
      return tl_delete_location (1);
    case TL_PHOTO_CHANGE_LOCATION_SERVER:
      return tl_change_location_server (0, 0);
    case TL_PHOTO_CHANGE_ORIGINAL_LOCATION_SERVER:
      return tl_change_location_server (1, 0);
    case TL_PHOTO_CHANGE_LOCATION_SERVER2:
      return tl_change_location_server (0, 1);
    case TL_PHOTO_CHANGE_ORIGINAL_LOCATION_SERVER2:
      return tl_change_location_server (1, 1);
    case TL_PHOTO_SAVE_PHOTO_LOCATION:
      return tl_save_photo_location();
    case TL_PHOTO_RESTORE_PHOTO_LOCATION:
      return tl_restore_photo_location();
    case TL_PHOTO_ROTATE_PHOTO:
      return tl_rotate_photo();
    case TL_PHOTO_CHANGE_PHOTO_ORDER:
    case TL_PHOTO_CHANGE_AUDIO_ORDER:
    case TL_PHOTO_CHANGE_VIDEO_ORDER:
      return tl_change_photo_order();
    case TL_PHOTO_CHANGE_ALBUM_ORDER:
      return tl_change_album_order();
    case TL_PHOTO_NEW_PHOTO_FORCE:
    case TL_PHOTO_NEW_AUDIO_FORCE:
    case TL_PHOTO_NEW_VIDEO_FORCE:
      return tl_new_photo_force();
    case TL_PHOTO_NEW_PHOTO:
    case TL_PHOTO_NEW_AUDIO:
    case TL_PHOTO_NEW_VIDEO:
      return tl_new_photo();
    case TL_PHOTO_NEW_ALBUM_FORCE:
      return tl_new_album_force();
    case TL_PHOTO_NEW_ALBUM:
      return tl_new_album();
    case TL_PHOTO_GET_PHOTOS_OVERVIEW:
      return tl_get_photos_overview (0);
    case TL_PHOTO_GET_PHOTOS_OVERVIEW_COUNT:
      return tl_get_photos_overview (1);
    case TL_PHOTO_GET_PHOTOS_COUNT:
    case TL_PHOTO_GET_AUDIOS_COUNT:
    case TL_PHOTO_GET_VIDEOS_COUNT:
      return tl_get_photos_count();
    case TL_PHOTO_GET_ALBUMS_COUNT:
      return tl_get_albums_count();
    case TL_PHOTO_GET_PHOTOS:
    case TL_PHOTO_GET_AUDIOS:
    case TL_PHOTO_GET_VIDEOS:
      return tl_get_photos (0);
    case TL_PHOTO_GET_PHOTO_ALBUMS:
    case TL_PHOTO_GET_AUDIO_ALBUMS:
    case TL_PHOTO_GET_VIDEO_ALBUMS:
      return tl_get_albums (0);
    case TL_PHOTO_GET_PHOTOS_WITH_COUNT:
    case TL_PHOTO_GET_AUDIOS_WITH_COUNT:
    case TL_PHOTO_GET_VIDEOS_WITH_COUNT:
      return tl_get_photos (1);
    case TL_PHOTO_GET_PHOTO_ALBUMS_WITH_COUNT:
    case TL_PHOTO_GET_AUDIO_ALBUMS_WITH_COUNT:
    case TL_PHOTO_GET_VIDEO_ALBUMS_WITH_COUNT:
      return tl_get_albums (1);
    case TL_PHOTO_GET_PHOTO:
    case TL_PHOTO_GET_AUDIO:
    case TL_PHOTO_GET_VIDEO:
      return tl_get_photo();
    case TL_PHOTO_GET_PHOTO_ALBUM:
    case TL_PHOTO_GET_AUDIO_ALBUM:
    case TL_PHOTO_GET_VIDEO_ALBUM:
      return tl_get_album();
    case TL_PHOTO_RESTORE_PHOTO:
    case TL_PHOTO_RESTORE_AUDIO:
    case TL_PHOTO_RESTORE_VIDEO:
      return tl_restore_photo();
    case TL_PHOTO_DELETE_PHOTO:
    case TL_PHOTO_DELETE_AUDIO:
    case TL_PHOTO_DELETE_VIDEO:
      return tl_delete_photo();
    case TL_PHOTO_DELETE_ALBUM:
      return tl_delete_album();
  }

  tl_fetch_set_error_format (TL_ERROR_UNKNOWN_FUNCTION_ID, "Unknown function %08x", op);
  return NULL;
}

/***
  SERVER
 ***/

void reopen_logs (void) {
  int fd;
  fflush (stdout);
  fflush (stderr);
  if ((fd = open ("/dev/null", O_RDWR, 0)) != -1) {
    dup2 (fd, 0);
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      assert (close (fd) >= 0);
    }
  }
  if (logname && (fd = open (logname, O_WRONLY | O_APPEND | O_CREAT, 0640)) != -1) {
    dup2 (fd, 1);
    dup2 (fd, 2);
    if (fd > 2) {
      assert (close (fd) >= 0);
    }
  }
  if (verbosity > 0) {
    fprintf (stderr, "logs reopened.\n");
  }
}

static void sigint_immediate_handler (const int sig) {
  const char message[] = "SIGINT handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (1);
}

static void sigterm_immediate_handler (const int sig) {
  const char message[] = "SIGTERM handled immediately.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  exit (1);
}

static void sigint_handler (const int sig) {
  const char message[] = "SIGINT handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << sig);
  signal (sig, sigint_immediate_handler);
}

static void sigterm_handler (const int sig) {
  const char message[] = "SIGTERM handled.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1 << sig);
  signal (sig, sigterm_immediate_handler);
}

static void sighup_handler (const int sig) {
  const char message[] = "got SIGHUP.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sighup_handler);
}

static void sigusr1_handler (const int sig) {
  const char message[] = "got SIGUSR1, rotate logs.\n";
  kwrite (2, message, sizeof (message) - (size_t)1);

  pending_signals |= (1ll << sig);
  signal (sig, sigusr1_handler);
}

static void sigpoll_handler (const int sig) {
  sigpoll_cnt++;
  signal (SIGPOLL, sigpoll_handler);
}

void cron (void) {
  flush_binlog();
}

int sfd;

void start_server (void) {
  int i;
  int prev_time;

  init_epoll();
  init_netbuffers();

  prev_time = 0;

  if (!sfd) {
    sfd = server_socket (port, settings_addr, backlog, 0);
  }

  if (sfd < 0) {
    fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
    exit (3);
  }

  if (verbosity) {
    fprintf (stderr, "created listening socket at %s:%d, fd=%d\n", conv_addr (settings_addr.s_addr, stats_buff), port, sfd);
  }

  if (daemonize) {
    setsid();
  }

  if (change_user (username) < 0) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (binlogname && !binlog_disabled) {
    assert (append_to_binlog (Binlog) == log_readto_pos);
  }

  tl_parse_function = photo_parse_function;
  tl_aio_timeout = 0.7;
  memcache_auto_answer_mode = 1;
  init_listening_connection (sfd, &ct_rpc_server, &rpc_methods);

  if (binlog_disabled && binlog_fd >= 0) {
    epoll_pre_event = read_new_events;
  }

  signal (SIGINT, sigint_handler);
  signal (SIGTERM, sigterm_handler);
  signal (SIGUSR1, sigusr1_handler);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGPOLL, sigpoll_handler);
  if (daemonize) {
    signal (SIGHUP, sighup_handler);
    reopen_logs();
  }

  if (verbosity) {
    fprintf (stderr, "Server started\n");
  }

  for (i = 0; !(pending_signals & ~((1ll << SIGUSR1) | (1ll << SIGHUP))); i++) {
    if (verbosity > 0 && !(i & 255)) {
      fprintf (stderr, "epoll_work(): %d out of %d connections, network buffers: %d used, %d out of %d allocated\n",
         active_connections, maxconn, NB_used, NB_alloc, NB_max);
    }
    fetch_binlog_data (binlog_disabled ? log_cur_pos() : log_write_pos());
    epoll_work (17);

    if (sigpoll_cnt > 0) {
      if (verbosity > 1) {
        fprintf (stderr, "after epoll_work(), sigpoll_cnt=%d\n", sigpoll_cnt);
      }
      sigpoll_cnt = 0;
    }

    if (pending_signals & (1ll << SIGHUP)) {
      pending_signals &= ~(1ll << SIGHUP);

      sync_binlog (2);
    }

    if (pending_signals & (1ll << SIGUSR1)) {
      pending_signals &= ~(1ll << SIGUSR1);

      reopen_logs();
      sync_binlog (2);
    }

    if (!NOAIO) {
      check_all_aio_completions();
    }
    tl_restart_all_ready();

    if (now != prev_time) {
      prev_time = now;
      cron();
    }

    if (write_only) {
      flush_binlog();
    }
    cstatus_binlog_pos (binlog_disabled ? log_cur_pos() : log_write_pos(), binlog_disabled);

    if (epoll_pre_event) {
      epoll_pre_event();
    }

    if (quit_steps && !--quit_steps) break;
  }

  if (verbosity > 0 && pending_signals) {
    fprintf (stderr, "Quitting because of pending signals = %llx\n", pending_signals);
  }

  epoll_close (sfd);
  assert (close (sfd) >= 0);

  flush_binlog_last();
  sync_binlog (2);
}


/***
  MAIN
 ***/

void usage (void) {
  printf ("usage: %s [options] <index-file>\n"
    "\t" VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
    "64-bit"
#else
    "32-bit"
#endif
    " after commit " COMMIT "\n",
    progname);
  if (index_mode) {
    printf ("\tGenerates new index of %s\n", mode_names[mode]);
  } else {
    printf ("\tManages %ss and %s albums\n", mode_names[mode], mode_names[mode]);
  }

  parse_usage();
  exit (2);
}

static char *dump_fields_str = NULL;

int photo_parse_option (int val) {
  switch (val) {
    case 'D':
      disable_crc32 = atoi (optarg);
      assert (0 <= disable_crc32 && disable_crc32 <= 3);
      break;
    case 'm':
      max_memory = atoi (optarg);
      if (max_memory < 1) {
        max_memory = 1;
      }
      max_memory *= 1048576;
      break;
    case 'M':
      dump_fields_str = optarg;
      index_mode = 1;
      break;
    case 'i':
      index_mode = 1;
      break;
    case 'I':
      dump_unimported_mode++;
      index_mode = 1;
      break;
    case 'k':
      if (mlockall (MCL_CURRENT | MCL_FUTURE) != 0) {
        fprintf (stderr, "error: fail to lock paged memory\n");
      }
      break;
    case 'R':
      repair_binlog_mode = 1;
      index_mode = 1;
      break;
    case 'w':
      write_only = 1;
      break;
    default:
      return -1;
  }
  return 0;
}

int main (int argc, char *argv[]) {
  int i;

  dl_set_debug_handlers();
  progname = argv[0];
  now = time (NULL);

  index_mode = 0;
  if (strstr (progname, "-index") != NULL) {
    index_mode = 1;
  }

  for (i = 0; i < MODE_MAX; i++) {
    if (strstr (progname, mode_names[i]) != NULL) {
      mode = i;
    }
  }

  remove_parse_option (204);
  parse_option ("disable-crc32", required_argument, NULL, 'D', "<disable-crc32> sets disable_crc32 to specified level");
  parse_option ("memory-limit", required_argument, NULL, 'm', "<memory-limit> sets maximal size of used memory not including zmemory for AIO in mebibytes");
  parse_option ("dump-fields", required_argument, NULL, 'M', "<field-names> dump for each object specified comma-separated list of fields into file <index-file>.dump using format of MySQL dumps, forces index mode");
  if (!index_mode) {
    parse_option ("index-mode", no_argument, NULL, 'i', "run in index mode");
  }
  parse_option ("dump-unimported", no_argument, NULL, 'I', "dump unimported to storage-engine photos to stdout in format\n\t\t\t\t\t\"server\\towner_id\\t%s_id\\tsource_user_id\\torig_album\\t%s\\n\",\n\t\t\t\t\tif specified twice, dumps only photos with condition (user_id != owner_id || album_id != orig_album), forces index_mode", mode_names[mode], mode_names[mode]);
  parse_option ("lock-memory", no_argument, NULL, 'k', "lock paged memory");
  parse_option ("repair-binlog", no_argument, NULL, 'R', "generate binlog using index, forces index mode, currently unsupported");
  if (!index_mode) {
    parse_option ("write-only", no_argument, NULL, 'w', "don't save changes in memory and don't answer queries");
  }

  parse_engine_options_long (argc, argv, photo_parse_option);
  if (argc != optind + 1) {
    usage();
    return 2;
  }

  if (verbosity > 0) {
    fprintf (stderr, "index_mode = %d\n", index_mode);
  }

  index_name = strdup (argv[optind]);

  assert (!(disable_crc32 && index_mode));

  if (index_mode) {
    binlog_disabled = 1;
  }

  dynamic_data_buffer_size = (1 << 16);//16 for AIO

  init_dyn_data();

  if (!username && maxconn == MAX_CONNECTIONS && geteuid()) {
    maxconn = 1000; //not for root
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    fprintf (stderr, "fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (!index_mode) {
    if (port < PRIVILEGED_TCP_PORTS) {
      sfd = server_socket (port, settings_addr, backlog, 0);
      if (sfd < 0) {
        fprintf (stderr, "cannot open server socket at port %d: %m\n", port);
        exit (1);
      }
    }
  }

  aes_load_pwd_file (NULL);

  if (change_user (username) < 0 && !interactive) {
    fprintf (stderr, "fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (engine_preload_filelist (argv[optind], binlogname) < 0) {
    fprintf (stderr, "cannot open binlog files for %s\n", binlogname ? binlogname : argv[optind]);
    exit (1);
  }

  init_common_data (0, index_mode ? CD_INDEXER : CD_ENGINE);
  cstatus_binlog_name (engine_replica->replica_prefix);

  //Snapshot reading
  Snapshot = open_recent_snapshot (engine_snapshot_replica);

  if (Snapshot) {
    engine_snapshot_name = Snapshot->info->filename;
    engine_snapshot_size = Snapshot->info->file_size;

    if (verbosity) {
      fprintf (stderr, "loading index file %s (size %lld)\n", engine_snapshot_name, engine_snapshot_size);
    }
  } else {
    engine_snapshot_name = NULL;
    engine_snapshot_size = 0;
  }

  index_load_time = -mytime();

  i = init_all (Snapshot);

  index_load_time += mytime();

  if (i < 0) {
    fprintf (stderr, "fatal: error %d while loading index file %s\n", i, engine_snapshot_name);
    exit (1);
  }

  if (verbosity) {
    fprintf (stderr, "load index: done, jump_log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
       jump_log_pos, (long)dl_get_memory_used(), index_load_time);
  }
  dbg ("load index: done, jump_log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
    jump_log_pos, (long)dl_get_memory_used(), index_load_time);

//  close_snapshot (Snapshot, 1);

  if (!repair_binlog_mode) {
    //Binlog reading
    Binlog = open_binlog (engine_replica, jump_log_pos);
    if (!Binlog) {
      fprintf (stderr, "fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
      exit (1);
    }

    binlogname = Binlog->info->filename;

    if (verbosity) {
      fprintf (stderr, "replaying binlog file %s (size %lld)\n", binlogname, Binlog->info->file_size);
    }
    binlog_load_time = -mytime();

    clear_log();
    init_log_data (jump_log_pos, jump_log_ts, jump_log_crc32);

    dbg ("jump_log_pos = %lld\n", jump_log_pos);

    if (verbosity) {
      fprintf (stderr, "replay log events started\n");
    }

    i = replay_log (0, 1);

    if (verbosity) {
      fprintf (stderr, "replay log events finished\n");
    }

    binlog_load_time += mytime();
    binlog_loaded_size = log_readto_pos - jump_log_pos;

    if (!binlog_disabled) {
      clear_read_log();
    }

    if (i == -2) {
      long long true_readto_pos = log_readto_pos - Binlog->info->log_pos + Binlog->offset;
      fprintf (stderr, "REPAIR: truncating %s at log position %lld (file position %lld)\n", Binlog->info->filename, log_readto_pos, true_readto_pos);
      if (truncate (Binlog->info->filename, true_readto_pos) < 0) {
        perror ("truncate()");
        exit (2);
      }
    } else if (i < 0) {
      fprintf (stderr, "fatal: error reading binlog\n");
      exit (1);
    }

    if (verbosity) {
      fprintf (stderr, "replay binlog file: done, log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
               (long long) log_pos, (long)dl_get_memory_used(), binlog_load_time);
    }
    dbg ("replay binlog file: done, log_pos=%lld, alloc_mem=%ld, time %.06lfs\n",
               (long long) log_pos, (long)dl_get_memory_used(), binlog_load_time);

    clear_write_log();
  }

  binlog_readed = 1;

  start_time = time (NULL);

  if (index_mode) {
    int result;
    if (dump_unimported_mode) {
      result = dump_unimported();
    } else if (repair_binlog_mode) {
      result = repair_binlog();
    } else {
      result = save_index (dump_fields_str);
    }

    if (verbosity > 0) {
      int len = photo_prepare_stats();
      stats_buff[len] = 0;
      fprintf (stderr, "%s\n", stats_buff);
    }

    free_all();
    return result;
  }

  start_server();

  free_all();
  return 0;
}
