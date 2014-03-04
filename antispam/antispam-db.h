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

    Copyright 2012-2013	Vkontakte Ltd
              2012      Sergey Kopeliovich <Burunduk30@gmail.com>
              2012      Anton Timofeev <atimofeev@vkontakte.ru>
*/
/**
 * Author:  Sergey Kopeliovich (Burunduk30@gmail.com)
 *          Anton  Timofeev    (atimofeev@vkontakte.ru)
 * Created: 16.03.2012
 */

#pragma once

#include "kdb-antispam-binlog.h"

#include "st-vec.h"
#include "st-list.h"

// Flags bits descriptions
enum
{
  SIMPLIFY_TYPE_NONE     = 0,
  SIMPLIFY_TYPE_PARTIAL  = 1,
  SIMPLIFY_TYPE_FULL     = 2,
  SIMPLIFY_TYPE_COUNT    = 3,

  FLAGS_SIMPLIFY_PARTIAL = 0x0020,
  FLAGS_SIMPLIFY_FULL    = 0x0010
};

enum {
  ANTISPAM_DB_FIELDS_IDS   = 1,
  ANTISPAM_DB_FIELDS_FLAGS = 2,
};

// Answer to 'antispam_get_id_list' request stored here.
DECLARE_VEC (int, vec_int)
extern vec_int_t antispam_db_request;

// Initialization and finalizing.
void antispam_init (void);
void antispam_finish (void);

// Add/Delete a pattern from DB.
// id, s, p: pattern characteristics
// replace:  replace or skip new pattern if there is one with the same id
// returns:  operation succeeded or not
bool antispam_add (antispam_pattern_t p, const char *s, bool replace);
bool antispam_del (int id);

// Search all DB-patterns matches in specified text.
// ip, uahash: search pattern
// text:       where to find matches
// fields:     (fields & ANTISPAM_DB_FIELDS_IDS)   == need to output ids,
//             (fields & ANTISPAM_DB_FIELDS_FLAGS) == need to output flags.
// limit:      max number of matchings to store into 'antispam_db_request'
// returns:    total number of matches
int antispam_get_matches (ip_t ip, uahash_t uahash, const char *text, byte fields, int limit);

// Find pattern in DB and serialize it to string.
// id:      pattern id to search
// output:  string to store result of serialization
// returns: search result
bool antispam_serialize_pattern (int id, char *output);

// Write db-statistics to a buffer [first, last) in the "key value" form.
void antispam_write_engine_stats (char** first, char* last);
