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
*/

#ifndef __KDB_TEXT_BINLOG_H__
#define __KDB_TEXT_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	TEXT_SCHEMA_BASE
#define	TEXT_SCHEMA_BASE	0x2cb30000
#endif

#define	TEXT_SCHEMA_V1		0x2cb30101

/* sets list of friends of given user, len<256 or arbitrary */
#define	LEV_TX_ADD_MESSAGE		0x37bd3200
#define	LEV_TX_ADD_MESSAGE_MF		0x358e7000
#define	LEV_TX_ADD_MESSAGE_LF		0x31aac5f8
#define	LEV_TX_ADD_MESSAGE_EXT		0x339f5000
#define	LEV_TX_ADD_MESSAGE_EXT_LL	0x3b424000
#define	LEV_TX_ADD_MESSAGE_EXT_ZF      	0x39eab000
#define	LEV_TX_SET_MESSAGE_FLAGS	0x5ecda600
#define	LEV_TX_SET_MESSAGE_FLAGS_LONG	0x52a05fc2
#define	LEV_TX_SET_EXTRA_FIELDS		0x54b5f000
#define	LEV_TX_INCR_MESSAGE_FLAGS	0x217bbe00
#define	LEV_TX_INCR_MESSAGE_FLAGS_LONG	0x2d3df2ad
#define	LEV_TX_DECR_MESSAGE_FLAGS	0x18443a00
#define	LEV_TX_DECR_MESSAGE_FLAGS_LONG	0x10248dbd
#define	LEV_TX_INCR_FIELD		0x25889fc0
#define	LEV_TX_INCR_FIELD_LONG		0x271db510
// #define	LEV_TX_SET_READ_DATE_FLAGS	0x26ccda00
#define	LEV_TX_DEL_MESSAGE		0x41a1cc4e
// #define	LEV_TX_DEL_MESSAGES		0x165a212f
// #define	LEV_TX_DEL_MESSAGE_LIST		0x64211ac3
#define	LEV_TX_DEL_FIRST_MESSAGES	0x69a43ae6
#define	LEV_TX_INCREASE_GLOBAL_ID_SMALL	0x2f5e0000
#define	LEV_TX_INCREASE_GLOBAL_ID_LARGE	0x2f5fbeda

#define	LEV_TX_REPLACE_TEXT		0x4edad000
#define	LEV_TX_REPLACE_TEXT_LONG	0x7b02edab

#define	TXF_UNREAD	1
#define	TXF_OUTBOX	2
#define	TXF_REPLIED	4
#define	TXF_IMPORTANT	8
#define	TXF_CHAT	0x10
#define	TXF_FRIENDS	0x20
#define	TXF_SPAM	0x40
#define	TXF_DELETED	0x80
#define	TXF_FIXED	0x100
#define	TXF_MEDIA	0x200
#define	TXF_ATTACH	0x400
#define	TXF_EMAIL	0x800

#define	TXFS_WALL	1
#define	TXFS_REPLY	2
#define	TXFS_COPY	4
#define	TXFS_REPLIED	8
#define	TXFS_COPIED	0x10
#define	TXFS_LOCATION	0x20
#define	TXFS_SPAM	0x40
#define	TXFS_DELETED	0x80
#define	TXFS_SMS	0x100
#define	TXFS_MEDIA	0x200
#define	TXFS_ATTACH	0x400
#define	TXFS_API	0x800
#define	TXFS_UNREAD	0x1000
#define	TXFS_SHARE	0x2000
#define	TXFS_FRIENDSONLY	0x4000

#define TXFP_REPLY	1
#define TXFP_TOPIC	2
#define TXFP_FIXED	4
#define TXFP_CLOSED	8
#define	TXFP_SPAM	0x40
#define	TXFP_DELETED	0x80
#define	TXFP_POLL	0x100
#define	TXFP_MEDIA	0x200
#define	TXFP_ATTACH	0x400
#define	TXFP_API	0x800
#define	TXFP_SHARE	0x2000

#define	MAX_EXTRA_FIELDS	12
#define	MAX_EXTRA_INTS		16
#define	MAX_EXTRA_MASK		((1 << MAX_EXTRA_FIELDS) - 1)

#define	TXF_HAS_LEGACY_ID	0x10000000
#define	TXF_HAS_PEER_MSGID	0x20000000
#define TXF_HAS_LONG_LEGACY_ID	0x40000000
#define	TXF_HAS_EXTRAS		(MAX_EXTRA_MASK << 16)


#define MSG_ADJ_DEL 0
#define MSG_ADJ_SET 1
#define MSG_ADJ_INC 2
#define MSG_ADJ_DEC 3
#define TEXT_REPLACE_FLAGS	0x11ef55aa

#define	MAX_TEXT_LEN	(1 << 20)

struct lev_add_message {
  lev_type_t type;
  int user_id;		// e.g. user id owning this in/outbox
  int legacy_id;	// legacy message id
  int peer_id;		// e.g. user id who sent or received this message
  int peer_msg_id;	// related peer message id (e.g. in sender's outbox); >0 = local_id, <0 = legacy_id
  int date;             // contains "long flags" if type == LEV_TX_ADD_MESSAGE_LF
  unsigned ip;
  int port;
  unsigned front;
  unsigned long long ua_hash;
  int text_len;
  int extra[0];		// up to 8 ints depending on lower 8 bits of TXF_LEV_ADD_MESSAGE_EXT
  char text[0];
};

struct lev_set_flags {
  lev_type_t type;
  int user_id;		// e.g. user id owning this in/outbox
  int local_id;
};

struct lev_set_flags_long {
  lev_type_t type;
  int user_id;		// e.g. user id owning this in/outbox
  int local_id;
  int flags;
};

struct lev_set_extra_fields {
  lev_type_t type;
  int user_id;
  int local_id;
  int extra[0];
};

struct lev_incr_field {
  lev_type_t type;
  int user_id;
  int local_id;
  int value;
};

struct lev_incr_field_long {
  lev_type_t type;
  int user_id;
  int local_id;
  long long value;
};

struct lev_del_message {
  lev_type_t type;
  int user_id;
  int local_id;
};

/*
struct lev_set_read_date_flags {
  lev_type_t type;
  int user_id;		// e.g. user id owning this in/outbox
  int message_id;
  int read_date;
};
*/

struct lev_del_first_messages {
  lev_type_t type;
  int user_id;
  int first_local_id;
};

struct lev_replace_text {
  lev_type_t type;
  int user_id;
  int local_id;
  char text[0];
};

struct lev_replace_text_long {
  lev_type_t type;
  int user_id;
  int local_id;
  int text_len;
  char text[0];
};



#pragma	pack(pop)

#endif
