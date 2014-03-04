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
              2013 Anton Maydell
*/

#ifndef __KDB_DNS_BINLOG_H__
#define __KDB_DNS_BINLOG_H__

#include "kdb-binlog-common.h"

#pragma	pack(push,4)

#ifndef	DNS_SCHEMA_BASE
#define	DNS_SCHEMA_BASE	0x144a0000
#endif

#define	DNS_SCHEMA_V1	0x144a0101

#define LEV_DNS_RECORD_A       0x5d0b0f00
#define LEV_DNS_RECORD_AAAA    0x3d4e9a00
#define LEV_DNS_RECORD_PTR     0x22639300
#define LEV_DNS_RECORD_NS      0x7ea64b00
#define LEV_DNS_RECORD_SOA     0x3dada700
#define LEV_DNS_RECORD_TXT     0x60bcd100
#define LEV_DNS_RECORD_MX      0x2259c100
#define LEV_DNS_RECORD_CNAME   0x2bce9d00
#define LEV_DNS_RECORD_SRV     0x51124e00
#define LEV_DNS_CHANGE_ZONE    0x3ec2e300
#define LEV_DNS_DELETE_RECORDS 0x28c1b600

struct lev_dns_record_a {
  lev_type_t type; /* lobyte name len */
  unsigned int ttl;
  unsigned int ipv4;
  char name[0];
};

struct lev_dns_record_aaaa {
  lev_type_t type; /* lobyte name len */
  unsigned int ttl;
  char ipv6[16];
  char name[0];
};

struct lev_dns_record_ns {
  lev_type_t type; /* lobyte nsdname len */
  char nsdname[0];
};

struct lev_dns_record_ptr {
  lev_type_t type;
  int data_len;
  char name[0];
  char data[0];
};

struct lev_dns_record_soa {
  lev_type_t type; /* lobyte name len */
  int serial;
  int refresh;
  int retry;
  int expire;
  int negative_cache_ttl;
  short mname_len;
  short rname_len;
  char name[0];
  char mname[0];
  char rname[0];
};

struct lev_dns_record_txt {
  lev_type_t type; /* lobyte name len */
  unsigned int ttl;
  int text_len;
  char name[0];
  char text[0];
};

struct lev_dns_record_mx {
  lev_type_t type; /* lobyte name len */
  unsigned short preference;
  unsigned short exchange_len;
  char name[0];
  char exchange[0];
};

struct lev_dns_record_cname {
  lev_type_t type; /* lobyte name len */
  int alias_len;
  char name[0];
  char alias[0];
};

struct lev_dns_record_srv {
  lev_type_t type; /* lobyte name len */
  unsigned int ttl;
  unsigned short priority;
  unsigned short weight;
  unsigned short port;
  unsigned short target_len;
  char name[0];
  char target[0];
};

struct lev_dns_change_zone {
  lev_type_t type;
  char origin[0];
};

struct lev_dns_delete_records {
  lev_type_t type;
  int qtype;
  char name[0];
};


/* TODO: special logevent for changing origin */

#pragma	pack(pop)

#endif


