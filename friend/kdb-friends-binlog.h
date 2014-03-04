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

    Copyright 2009 Vkontakte Ltd
              2009 Nikolai Durov
              2009 Andrei Lopatin
*/

#ifndef __KDB_FRIENDS_BINLOG_H__
#define __KDB_FRIENDS_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	FRIENDS_SCHEMA_BASE
#define	FRIENDS_SCHEMA_BASE	0x2bec0000
#endif

#define	FRIENDS_SCHEMA_V1	0x2bec0101

/* sets list of friends of given user, len<256 or arbitrary */
#define	LEV_FR_SETLIST_CAT	0x713a2100
#define	LEV_FR_SETLIST_CAT_LONG	0x365cd7a5
/* same with all categories=1 */
#define	LEV_FR_SETLIST		0x19aa5e00
#define	LEV_FR_SETLIST_LONG	0x7692abd2
/* delete user with all her friend and privacy lists */
#define	LEV_FR_USERDEL		0x47d292d3
/* adds one friend to a friendlist, setting his categories;
   if the friend is already there, changes his categories */
#define	LEV_FR_ADD_FRIEND	0x1f98a11d
/* same as above, but only if friend is already in the friendlist */
#define	LEV_FR_EDIT_FRIEND	0x2abc4237
#define	LEV_FR_EDIT_FRIEND_OR	0x40124cc2
#define	LEV_FR_EDIT_FRIEND_AND	0x6ba319a5
/* removes friend from friendlist */
#define	LEV_FR_DEL_FRIEND	0x066ca22d
/* adds a friend to a specific category */
#define	LEV_FR_ADDTO_CAT	0x52342300
/* removes a friend from a specific category */
#define	LEV_FR_REMFROM_CAT	0x452ef600
/* deletes a friend category and invalidates all references to it in privacy lists */
#define	LEV_FR_DEL_CAT		0x1bccd300
/* empties a friend category */
#define	LEV_FR_CLEAR_CAT	0x66ca0300
/* sets list of friends belonging to given category */
#define	LEV_FR_CAT_SETLIST	0x3dfea400
/* sets extended privacy */
#define	LEV_FR_SET_PRIVACY		0x151ae400
#define	LEV_FR_SET_PRIVACY_FORCE	0x151ae500
/* sets extended privacy (long) */
#define	LEV_FR_SET_PRIVACY_LONG		0x421a34bc
#define	LEV_FR_SET_PRIVACY_LONG_FORCE	0x421a34bd
/* delete privacy */
#define	LEV_FR_DEL_PRIVACY	0x643aa123
/* friend requests */
#define	LEV_FR_ADD_FRIENDREQ	0x3a12b069
#define	LEV_FR_NEW_FRIENDREQ	0x5865c931
#define	LEV_FR_REPLACE_FRIENDREQ	0x423678fe
#define	LEV_FR_DEL_FRIENDREQ	0x61f7aa91
#define	LEV_FR_DELALL_REQ	0x184a238f


typedef struct id_cat_pair {
  int id;
  int cat;
} id_cat_pair_t;

struct lev_setlist_cat {
  lev_type_t type;
  int user_id;
  id_cat_pair_t L[0];
};

struct lev_setlist_cat_long {
  lev_type_t type;
  int user_id;
  int num;
  id_cat_pair_t L[0];
};

struct lev_setlist {
  lev_type_t type;
  int user_id;
  int L[0];
};

struct lev_setlist_long {
  lev_type_t type;
  int user_id;
  int num;
  int L[0];
};

struct lev_userdel {
  lev_type_t type;
  int user_id;
};

struct lev_add_friend {
  lev_type_t type;
  int user_id;
  int friend_id;
  int cat;
};

struct lev_del_friend {
  lev_type_t type;
  int user_id;
  int friend_id;
};

typedef unsigned long long privacy_key_t;

struct lev_del_privacy {
  lev_type_t type;
  int user_id;
  privacy_key_t key;
};

#define	PL_M_DENY	(1 << 31)
#define	PL_M_ALLOW	0
#define	PL_M_CAT	(1 << 30)
#define	PL_M_USER	0
#define	PL_M_MASK	((1 << 30) - 1)

#define	CAT_FR_FR	((1 << 30) - 3)
#define	CAT_FR_ALL	((1 << 30) - 1)
#define	CAT_FR_PACKED	((1 << 30) - 7)

struct lev_set_privacy {
  lev_type_t type;
  int user_id;
  privacy_key_t key;
  int List[0];
};

struct lev_set_privacy_long {
  lev_type_t type;
  int user_id;
  privacy_key_t key;
  int len;
  int List[0];
};

struct lev_user_generic {
  lev_type_t type;
  int user_id;
  int a, b, c, d, e;
};

#pragma	pack(pop)

#endif
