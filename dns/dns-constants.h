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

#ifndef __KDB_DNS_CONSTANTS_H__
#define __KDB_DNS_CONSTANTS_H__

/******************** DNS protocol ********************/

enum {
  dns_type_a  = 1,     /* a host address */
  dns_type_ns = 2,     /* an authoritative name server */
  dns_type_cname = 5,  /* the canonical name for an alias */
  dns_type_soa = 6,    /* marks the start of a zone of authority */
  dns_type_ptr = 12,   /* a domain name pointer */
  dns_type_hinfo = 13, /* host information */
  dns_type_minfo = 14, /* mailbox or mail list information */
  dns_type_mx = 15,    /* mail exchange */
  dns_type_txt = 16,   /* text strings */
  dns_type_aaaa = 28,  /* see RFC 3596 (DNS Extensions to Support IPv6) */
  dns_type_srv = 33,   /* see RFC 2782 (A DNS RR for specifying the location of services (DNS SRV)) */
  dns_type_opt = 41,   /* RFC 2671 (Extension Mechanisms for DNS (EDNS0) */
};

enum {
  dns_qtype_axfr = 252,  /* A request for a transfer of an entire zone */
  dns_qtype_mailb = 253, /* request for mailbox-related records (MB, MG or MR) */
  dns_qtype_any = 255    /* A request for all records */
};

enum {
  dns_class_in = 1, /* the Internet */
  dns_class_ch = 3, /* the CHAOS class */
  dns_class_hs = 4, /* Hesiod */
  dns_class_any = 255
};

enum {
  dns_header_flag_response = 0x8000,
  dns_header_mask_opcode = 0x7800,
  dns_header_flag_opcode_query = 0,
  dns_header_flag_opcode_status = 0x1000,
  dns_header_flag_opcode_iquery = 0x800,
/* Authoritative Answer - this bit is valid in responses,
   and specifies that the responding name server is an
   authority for the domain name in question section.
   Note that the contents of the answer section may have
   multiple owner names because of aliases. The AA bit
   corresponds to the name which matches the query name, or
   the first owner name in the answer section. */
  dns_header_flag_aa = 0x400,
/* TrunCation - specifies that this message was truncated
   due to length greater than that permitted on the
   transmission channel. */
  dns_header_flag_tc = 0x200,
/* Recursion Desired - this bit may be set in a query and
   is copied into the response. If RD is set, it directs
   the name server to pursue the query recursively.
   Recursive query support is optional. */
  dns_header_flag_rd = 0x100,
/* Recursion Available - this be is set or cleared in a
   response, and denotes whether recursive query support is
   available in the name server. */
  dns_header_flag_ra = 0x80,
};

enum dns_rcode {
  dns_rcode_no_error = 0,
/* Format error - The name server was
   unable to interpret the query. */
  dns_rcode_format_error = 1,
/* Server failure - The name server was
   unable to process this query due to a
   problem with the name server. */
  dns_rcode_server_failure = 2,
/* Name Error - Meaningful only for
   responses from an authoritative name
   server, this code signifies that the
   domain name referenced in the query does
   not exist. */
  dns_rcode_name_error = 3,
/* Not Implemented - The name server does
   not support the requested kind of query. */
  dns_rcode_not_implemented = 4,
/* Refused - The name server refuses to
   perform the specified operation for
   policy reasons. For example, a name
   server may not wish to provide the
   information to the particular requester,
   or a name server may not wish to perform
   a particular operation (e.g., zone
   transfer) for particular data. */
  dns_rcode_refused = 5,
};

#endif

