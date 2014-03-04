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

    Copyright 2013 Vkontakte Ltd
              2013 Vitaliy Valtman
*/

#ifndef __TEXT_TL_H__
#define __TEXT_TL_H__

#define CONCAT(a,b) a ## b

#include "TL/constants.h"


#define TL_TEXT_GET_MESSAGE_e_EXTRA TL_TEXT_GET_MESSAGEEXTRA
#define TL_TEXT_SET_MESSAGE_e_EXTRA TL_TEXT_SET_MESSAGEEXTRA
#define TL_TEXT_INCR_MESSAGE_e_EXTRA TL_TEXT_INCR_MESSAGEEXTRA
#define TL_TEXT_DECR_MESSAGE_e_EXTRA TL_TEXT_DECR_MESSAGEEXTRA

#define TL_TEXT_GET_MESSAGE_E_EXTRA TL_TEXT_GET_MESSAGE_EXTRA
#define TL_TEXT_SET_MESSAGE_E_EXTRA TL_TEXT_SET_MESSAGE_EXTRA
#define TL_TEXT_INCR_MESSAGE_E_EXTRA TL_TEXT_INCR_MESSAGE_EXTRA
#define TL_TEXT_DECR_MESSAGE_E_EXTRA TL_TEXT_DECR_MESSAGE_EXTRA

/*

#define TL_TEXT_MESSAGE 0x559eb78a
#define TL_TEXT_MESSAGE_FULL_ID 0x8e98031c
#define TL_TEXT_SUBLIST_TYPE 0x3bd33f3b
#define TL_TEXT_EVENT_OLD 0x25f09fef
#define TL_TEXT_EVENT_BASE 0x418394ff
#define TL_TEXT_EVENT_EX 0xd7bd0ef7
#define TL_TEXT_EVENT_LIST_OLD 0x211ccf03
#define TL_TEXT_EVENT_LIST 0x1b139bfd
#define TL_TEXT_ONLINE_FRIEND 0xb984ee0a

#define TL_TEXT_GET_MESSAGE_SHORT 0xb2ce085b
#define TL_TEXT_GET_MESSAGE 0x248171bc
#define TL_TEXT_CONVERT_LEGACY_ID 0x0e9bd317
#define TL_TEXT_PEER_MSG_LIST 0xb176e192
#define TL_TEXT_PEER_MSG_LIST_POS 0x6eea623e
#define TL_TEXT_TOP_MSG_LIST 0x3951c842

#define TL_TEXT_SUBLIST 0x34802e81
#define TL_TEXT_SUBLIST_SHORT 0xfac62f29
#define TL_TEXT_SUBLIST_POS 0x2666e082
#define TL_TEXT_SUBLIST_TYPES 0xa1cefc9c

#define TL_TEXT_PEERMSG_TYPE 0x7cbfc43c

#define TL_TEXT_SEND_MESSAGE 0x1a8d47de

#define TL_TEXT_DELETE_MESSAGE 0x99b39198
#define TL_TEXT_DELETE_FIRST_MESSAGES 0x3d8bc243

#define TL_TEXT_GET_MESSAGE_FLAGS 0x4d0aef0d
#define TL_TEXT_SET_MESSAGE_FLAGS 0xe887ffb0
#define TL_TEXT_INCR_MESSAGE_FLAGS 0x6ea30103
#define TL_TEXT_DECR_MESSAGE_FLAGS 0x61ce5093

#define TL_TEXT_GET_MESSAGE_e_EXTRA 0xa2a2d031
#define TL_TEXT_SET_MESSAGE_e_EXTRA 0x4c564dfe
#define TL_TEXT_INCR_MESSAGE_e_EXTRA 0x33111fb8
#define TL_TEXT_DECR_MESSAGE_e_EXTRA 0xe9148763

#define TL_TEXT_GET_MESSAGE_E_EXTRA 0xa5009fde
#define TL_TEXT_SET_MESSAGE_E_EXTRA 0x37f30733
#define TL_TEXT_INCR_MESSAGE_E_EXTRA 0x7d25cade
#define TL_TEXT_DECR_MESSAGE_E_EXTRA 0xe13d1779

#define TL_TEXT_GET_USERDATA 0xa32e8142
#define TL_TEXT_DELETE_USERDATA 0x9a8566bb
#define TL_TEXT_LOAD_USERDATA 0x1251b4d1

#define TL_TEXT_REPLACE_MESSAGE_TEXT 0x84a7e012

#define TL_TEXT_GET_EXTRA_MASK 0xce45b8b5
#define TL_TEXT_SET_EXTRA_MASK 0x1035a059

#define TL_TEXT_SEARCH 0xdc6a4fd0
#define TL_TEXT_SEARCH_EX 0x6e78a36a

#define TL_TEXT_GET_TIMESTAMP 0x4b4656b0
#define TL_TEXT_GET_FORCE_TIMESTAMP 0x93e3e6a5

#define TL_TEXT_EVENT_BASE 0x418394ff
#define TL_TEXT_EVENT_EX 0xd7bd0ef7

#define TL_TEXT_HISTORY 0xeb0864fb

#define TL_TEXT_HISTORY_ACTION 0x75c1f71e

#define TL_TEXT_GET_PTIMESTAMP 0xc825a0fa

#define TL_TEXT_PHISTORY 0xc960e9c0

#define TL_TEXT_ONLINE 0x23a22073
#define TL_TEXT_OFFLINE 0x87327f49
#define TL_TEXT_ONLINE_FRIENDS_ID 0x22b0c97f
#define TL_TEXT_ONLINE_FRIENDS 0x0fdfa2f0

#define TL_TEXT_SET_SECRET 0xe6164a53
#define TL_TEXT_GET_SECRET 0x0b3a1c69
#define TL_TEXT_DELETE_SECRET 0xd645798e
*/
#endif
