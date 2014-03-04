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
              2011-2013 Nikolai Durov
              2011-2013 Andrei Lopatin
                   2013 Anton Maydell
*/

#ifndef __REPLICATOR_PROTOCOL_H__
#define __REPLICATOR_PROTOCOL_H__

#pragma pack(push,4)

// generic packet layout
struct rpc_packet_generic {
  int size;		// in bytes, including crc32
  int packet_num;
  int data[0];
  int crc32;		// crc32 of all previous fields
};

#define	P_REPL_HANDSHAKE	0x3afe4acc
#define	P_REPL_HANDSHAKE_OK	0x5219ace7
#define	P_REPL_ERROR		0x6edabeda
#define P_REPL_DATA		0x426312fc
#define P_REPL_DATA_ACK		0x7accda7a
#define	P_REPL_ROTATE		0x20cdfebe
#define P_REPL_POS_INFO		0x39847ea0
#define P_REPL_PING		0x112358e5
#define P_REPL_PONG		0x5d853211

typedef unsigned char md5_hash_t[16];

struct repl_handshake {
  int type;
  int handshake_id;
  int flags;
  long long binlog_slice_start_pos;
  long long binlog_slice_end_pos;
  long long binlog_file_size;
  long long binlog_first_file_size;	// -1 = non-existent
  md5_hash_t binlog_file_start_hash;	// md5 of first min(128k,binlog_file_size) bytes
  md5_hash_t binlog_file_end_hash;	// md5 of last min(128k,binlog_file_size) bytes
  md5_hash_t binlog_first_file_start_hash;	// md5 of first min(1m,binlog_first_file_size) bytes
  int binlog_tag_len;
  int binlog_slice_name_len;
  char binlog_tag[0];			// binlog_tag_len+1 bytes, with null terminator
  char binlog_slice_name[0];		// binlog_slice_name_len+1 bytes, with null terminator
  char padding[0];			// size must be a multiple of 4
};

struct repl_handshake_ok {
  int type;
  int handshake_id;			// same as in repl_handshake
  int session_id;
  long long binlog_slice_start_pos;
  long long binlog_slice_end_pos;
  long long binlog_last_start_pos;
  long long binlog_last_end_pos;
};

struct repl_error {
  int type;
  int handshake_id;
  int session_id;		// 0 if it is after repl_handshake
  int error;			// negative
  char error_message[0];	// zero-terminated and zero-padded
};

struct repl_data_descr {
  int headers_size;			// either 0k, 4k or 8k
  int data_size;
  long long binlog_slice_start_pos;
  long long binlog_slice_cur_pos;	// file position is binlog_slice_cur_pos - binlog_slice_start_pos + headers_size
};

#define RDF_SENTALL	1

struct repl_data {
  int type;
  int handshake_id;
  int session_id;
  int flags;		// only RDF_SENTALL defined now (meaning that master has sent all it had)
  struct repl_data_descr A;
  char data[0];			
};

struct repl_data_ack {
  int type;
  int handshake_id;
  int session_id;
  long long binlog_written_pos;
  long long binlog_received_pos;
};

struct repl_rotate {
  int type;
  int handshake_id;
  int session_id;
  int flags;
  struct repl_data_descr A1;
  struct repl_data_descr A2;
  int binlog_slice2_name_len;
  char binlog_slice2_name[0];		// 0-terminated and 0-padded to 4-byte boundary
  char data1[0];
  char data2[0];
};

struct repl_pos_info {
  int type;
  int handshake_id;
  int session_id;
  long long binlog_pos;
  long long binlog_time;
};

struct repl_ping {
  int type;
  int ping_id;
};

struct repl_pong {
  int type;
  int ping_id;
};

#define R_ERROR_QUIT	0		// stop replication (no error), for example, server or client is quitting
#define R_ERROR_ENOSYS	-1		// unknown packet type
#define R_ERROR_EINVAL	-2		// invalid packet: packet size or syntax check failed
#define R_ERROR_ENOENT	-3		// no such binlog tag (server does not provide this)
#define R_ERROR_EBADSLICE	-4	// no such binlog slice, or binlog slice exists with different hash/start_pos
#define	R_ERROR_EBADFD	-5		// incorrect session_id or handshake_id
#define R_ERROR_EAGAIN	-6		// resource temporarily unavailable (e.g. disk error), stop replication, try again later (not immediately)
#define R_ERROR_EUNLINKSLICE -7 // client should unlink slice

#pragma pack(pop)

#endif

