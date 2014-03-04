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

#pragma once

#include <aio.h>
#include <stdarg.h>
#include <unistd.h>

#include "net-aio.h"
#include "kfs.h"

#include "vv-tl-parse.h"
#include "vv-tl-aio.h"

#include "photo-tl.h"
#include "photo-interface-structures.h"

#define NOAIO 0

#define MAX_NAME_LEN 256
#define MAX_TYPE 3
#define MAX_FIELDS 32


#ifdef DL_DEBUG
#  define MAX_ALBUMS 10
#  define MAX_PHOTOS 5
#  define MAX_PHOTOS_DYN (MAX_PHOTOS * 3)
#  define MAX_HIDE 10
#else
#  define MAX_ALBUMS 32767
#  define MAX_PHOTOS 32767
#  define MAX_PHOTOS_DYN (MAX_PHOTOS * 32)
#  define MAX_HIDE 32767
#endif

#define MAX_EVENT_SIZE 32768

#define HIDE_ITERS 2
#define GET_MAX_PHOTOS(aid) (aid >= 0 ? MAX_PHOTOS : MAX_PHOTOS_DYN)

#define MIN_NEW_ALBUM_ID 456239017
#define MIN_NEW_PHOTO_ID 456239017

#define PHOTO_TYPE 1
#define ALBUM_TYPE 2

#define MAX_MEMORY 2000000000
#define MEMORY_USER_PERCENT 0.75
#define	PHOTO_INDEX_MAGIC 0x12d1f8a5

#if MAX_FIELDS >= 33 || MAX_FIELDS <= 0
#  error Wrong MAX_FIELDS
#endif

#if MAX_FIELDS >= 29 + 31 || MAX_FIELDS <= 0
#  error Wrong MAX_FIELDS
#endif

typedef enum { t_int = 0, t_long = 1, t_double = 2, t_string = 3, t_raw = 4} field_type;

typedef enum {MODE_PHOTO, MODE_VIDEO, MODE_AUDIO, MODE_MAX} mode_type;
extern const char *mode_names[MODE_MAX];
extern mode_type mode;


typedef struct {
  field_type type;
  int v_fid;
  union {
    int v_string_len;
    int v_raw_len;
  };
  union {
    int v_int;
    long long v_long;
    double v_double;
    char *v_string;
    char *v_raw;
  };
} field;

extern int field_changes_n;
extern field field_changes[MAX_FIELDS + 1];

extern int import_dump_mode, index_mode, dump_unimported_mode, repair_binlog_mode;
extern long max_memory;
extern int index_users, header_size;
extern int binlog_readed;
extern int write_only;

extern int jump_log_ts;
extern long long jump_log_pos;
extern unsigned int jump_log_crc32;
extern char *index_name;

extern int cur_local_id, user_cnt, cur_users, del_by_LRU;
extern long long total_photos;
extern long long allocated_metafile_bytes;

extern long long cmd_load_user, cmd_unload_user;
extern double cmd_load_user_time, cmd_unload_user_time;
extern double max_cmd_load_user_time, max_cmd_unload_user_time;


int base64url_to_secret (const char *input, unsigned long long *secret);


int check_user_id (int user_id);
int check_photo_id (int photo_id);
int check_album_id (int album_id);
int check_photo (char *photo, int photo_len);


int do_create_photo_force (int user_id, int album_id, int cnt, int photo_id);
int do_create_album_force (int user_id, int album_id);
int do_create_photo (int user_id, int album_id, int cnt);
int do_create_album (int user_id);
int do_change_photo (int user_id, int photo_id, char *changes);
int do_change_album (int user_id, int album_id, char *changes);
int do_increment_photo_field (int user_id, int photo_id, char *field_name, int cnt);
int do_increment_album_field (int user_id, int album_id, char *field_name, int cnt);
int do_change_photo_order (int user_id, int photo_id, int photo_id_near, int is_next);
int do_change_album_order (int user_id, int album_id, int album_id_near, int is_next);
int do_delete_photo (int user_id, int photo_id);
int do_delete_album (int user_id, int album_id);
int do_restore_photo (int user_id, int photo_id);


int get_photos_overview (int user_id, char *albums_id, int offset, int limit, char *fields, const int reverse, const int count, char **result);
int get_photos_count    (int user_id, int album_id, char *condition);
int get_albums_count    (int user_id, char *condition);
int get_photos          (int user_id, int album_id, int offset, int limit, char *fields, const int reverse, const int count, char *condition, char **result);
int get_albums          (int user_id, int offset, int limit, char *fields, const int reverse, const int count, char *condition, char **result);
int get_photo           (int user_id, int photo_id, const int force, char *fields, char **result);
int get_album           (int user_id, int album_id, const int force, char *fields, char **result);

int do_set_volume (int volume_id, int server_id);
int do_add_photo_location (int user_id, int photo_id, int original, int server_id, int server_id2, int orig_owner_id, int orig_album_id, char *photo, int photo_len);
int do_add_photo_location_engine (int user_id, int photo_id, int original, char size, int rotate, int volume_id, int local_id, int extra, unsigned long long secret);
int do_change_photo_location_server (int user_id, int photo_id, int original, int server_num, int server_id);
int do_del_photo_location (int user_id, int photo_id, int original);
int do_del_photo_location_engine (int user_id, int photo_id, int original, char size, int rotate);
int do_save_photo_location (int user_id, int photo_id);
int do_restore_photo_location (int user_id, int photo_id);
int do_rotate_photo (int user_id, int photo_id, int dir);


int tl_do_change_photo (struct tl_act_extra *extra);
int tl_do_change_album (struct tl_act_extra *extra);

int tl_do_get_photos_overview (struct tl_act_extra *extra);
int tl_do_get_photos (struct tl_act_extra *extra);
int tl_do_get_albums (struct tl_act_extra *extra);
int tl_do_get_photo (struct tl_act_extra *extra);
int tl_do_get_album (struct tl_act_extra *extra);


int init_all (kfs_file_handle_t Index);
int save_index (char *dump_fields_str);
int dump_unimported (void);
int repair_binlog (void);
void free_all (void);

void test_events (void);
