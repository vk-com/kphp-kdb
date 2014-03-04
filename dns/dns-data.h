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

#ifndef __KDB_DNS_DATA_H__
#define __KDB_DNS_DATA_H__

#include "dns-constants.h"

#define DNS_NAME_HASH_SIZE 100000
#define DNS_DEFAULT_TTL 900
#define DNS_MAX_RESPONSE_RECORDS 32

#define DNS_LABELS_BUFFSIZE  0x1000000
#define DNS_RECORDS_BUFFSIZE 0x4000000
#define DNS_MAX_RECORDS      0x400000
#define DNS_MAX_TRIE_NODES   0x200000
#define DNS_MAX_TRIE_EDGES   0x200000
/* DNS_EDGE_HASH_SIZE should be power of 2 */
#define DNS_EDGE_HASH_SIZE   0x200000
#define DNS_MAX_ZONES 1024
#define DNS_MAX_BINLOG_ALLOW_QUERY_NETWORKS 16
/******************** Iterators ********************/

typedef struct {
  unsigned char *start;
  unsigned char *rptr;
  int ilen;
  int avail_in;
} dns_read_iterator_t;

#define DNS_MAX_WRITE_ITERATOR_NAMES 256
#define DNS_WRITE_ITERATOR_DOMAIN_BUFFSIZE 16384

typedef struct {
  unsigned char *start;
  unsigned char *wptr;
  unsigned char *record_start; /* for truncaction */
  int olen;
  int avail_out;

  int domains;
  struct {
    char *s;
    int off;
    int len;
  } domain[DNS_MAX_WRITE_ITERATOR_NAMES];
  char *domain_wptr;
  char domain_buff[DNS_WRITE_ITERATOR_DOMAIN_BUFFSIZE];
} dns_write_iterator_t;

typedef struct {
  unsigned short mname_len;
  unsigned short rname_len;
  int data[5];
  char mname[0];
  char rname[0];
} dns_soa_t;

typedef struct {
  unsigned short priority;
  unsigned short weight;
  unsigned short port;
  unsigned short target_len;
  char target[0];
} dns_srv_t;

typedef struct {
  int parent_node_id; /* hashtable of tuple (parent_node_id, label_ptr, label_len) */
  int label_start;
  int child_node_id;
  int hnext;
  int label_len;//label < 64 characters
} dns_name_trie_edge_t;

typedef struct {
  int first_record_id;
  int zone_id;
} dns_name_trie_node;

typedef struct {
  unsigned short data_len;
  char flag_has_ttl;
  char data_type;
  char data[0];
} dns_trie_record_t;

typedef struct {
  char *name;
  dns_trie_record_t *R;
  int name_len;
} dns_record_t;

typedef struct {
  unsigned int ipv4;
  char ipv6[16];
  int a_ttl;
  int aaaa_ttl;
} dns_adresses_t;

typedef struct {
  dns_adresses_t addrs_exchange;
  unsigned short preference;
  unsigned short exchange_len;
  char exchange[0];
} dns_mx_t;

typedef struct dns_nameserver_en {
  dns_adresses_t addrs_nsd;
  int nsdname_len;
  struct dns_nameserver_en *next;
  char nsdname[0];
} dns_nameserver_en_t;

typedef struct {
  char *origin;
  dns_nameserver_en_t *servers;
  int origin_len;
  int ns_servers;
  unsigned int ttl;
  int records;
  char soa_record;
} dns_zone_t;

typedef struct {
  int ipv4;
  int mask;
  int prefix_bits;
} dns_network_t;

typedef struct {
  unsigned char ipv6[16];
  unsigned char mask[16];
  int prefix_bits;
} dns_network6_t;

typedef struct {
  unsigned short id;
  unsigned short flags;
  /* an unsigned 16 bit integer specifying the number of entries in the question section. */
  unsigned short qdcount;
 /* an unsigned 16 bit integer specifying the number of resource records in the answer section. */
  unsigned short ancount;
  /* an unsigned 16 bit integer specifying the number of name server resource records in the authority records section. */
  unsigned short nscount;
  /* an unsigned 16 bit integer specifying the number of resource records in the additional records section. */
  unsigned short arcount;
} dns_header_t;

typedef struct {
  char name[256];
  int name_len;
  unsigned short qtype;
  unsigned short qclass;
} dns_question_section_t;

/*
typedef struct {
  unsigned char name[256];
  unsigned short type;
  unsigned short class;
  unsigned int ttl;
  unsigned short rdlength;
  unsigned char rddata[0];
} dns_resource_record_t;
*/

#define DNS_QUERY_FLAG_IPV6       1
#define DNS_QUERY_FLAG_UDP        2
#define DNS_QUERY_FLAG_BAD_FORMAT 4
#define DNS_QUERY_FLAG_EDNS       8

typedef struct {
  dns_header_t header;
  dns_question_section_t QS;
  unsigned char ipv6[16];
  int ipv4;
  int max_udp_buffsize;
  int flags;
} dns_query_t;

typedef struct {
  dns_header_t header;
  dns_write_iterator_t Out;
  dns_query_t *q;
  int additional_records;
  int truncated;
  int answers;
  enum dns_rcode rcode;
  dns_record_t RA[DNS_MAX_RESPONSE_RECORDS];
} dns_response_t;

int dns_query_parse (dns_query_t *q, unsigned char *in, int ilen, int udp);
int dns_query_act (dns_query_t *q, dns_response_t *r, unsigned char *out, int olen);
void dns_query_set_ip (dns_query_t *q, int af, void *ip);

int dns_config_load (const char *filename, int exit_after_first_error, const char *output_binlog_name);
void dns_reset (void);

typedef struct {
  double percent_label_buff;
  double percent_label_reused;
  double percent_record_buff;
  double percent_nodes;
  double percent_edges;
} dns_stat_t;

typedef struct {
   long long dns_udp_queries, dns_udp_bad_act_queries, dns_udp_bad_parse_queries, dns_udp_skipped_long_queries;
   long long dns_udp_query_bytes, dns_udp_response_bytes;
   long long dns_tcp_queries, dns_tcp_bad_act_queries, dns_tcp_bad_parse_queries, dns_tcp_skipped_long_queries;
   long long dns_tcp_query_bytes, dns_tcp_response_bytes;
   long long dns_truncated_responses;
   long long rcode_no_error_queries;
   long long rcode_format_queries;
   long long rcode_server_failure_queries;
   long long rcode_name_error_queries;
   long long rcode_not_implemented_queries;
   long long rcode_refused_queries;
   long long refused_by_remote_ip_queries;
   double workers_average_idle_percent, workers_max_idle_percent;
   double workers_recent_idle_percent, workers_max_recent_idle_percent;
   int dns_udp_max_response_bytes, dns_tcp_max_response_bytes;
   int dns_tcp_connections;
} worker_stats_t;

void dns_stats (dns_stat_t *S);
int dns_binlog_allow_query_networks_dump (char *output, int avail_out);
extern int labels_wptr, trie_nodes, trie_edges, records_wptr, tot_records, labels_saved_bytes, records_saved_bytes, reload_time;
extern int dns_max_response_records, config_zones, zones, edns_response_bufsize;
extern worker_stats_t wstat;
extern char *include_binlog_name;

#endif
