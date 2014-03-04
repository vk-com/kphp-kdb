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

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <errno.h>
#include <arpa/inet.h>
#include "kfs.h"
#include "net-events.h"
#include "server-functions.h"
#include "kdb-data-common.h"
#include "dns-data.h"
#include "am-hash.h"
#include "kdb-dns-binlog.h"

/******************** DNS names trie (DNS names hash replacement) ********************/
int labels_wptr, trie_nodes, trie_edges, records_wptr, tot_records, free_records, labels_saved_bytes, records_saved_bytes, reload_time;
int dns_max_response_records = DNS_MAX_RESPONSE_RECORDS, config_zones, zones;
int edns_response_bufsize = 1024;
worker_stats_t wstat;
char *include_binlog_name;
static int dns_convert_config_to_binlog;
static int cur_zone_id;
static char labels_buff[DNS_LABELS_BUFFSIZE];
static dns_name_trie_node names[DNS_MAX_TRIE_NODES];
static struct {
  int record_off;
  int next;
} RB[DNS_MAX_RECORDS];
static int free_rb;
static char records_buff[DNS_RECORDS_BUFFSIZE];
static int HE[DNS_EDGE_HASH_SIZE];
static dns_name_trie_edge_t edge_buff[DNS_MAX_TRIE_EDGES];
static dns_zone_t Z[DNS_MAX_ZONES];
static int dec_number[256];
static int lo_alpha[26]; /* optimize IPv6 PTR */
static dns_network_t BAQN[DNS_MAX_BINLOG_ALLOW_QUERY_NETWORKS];
static dns_network6_t BAQN6[DNS_MAX_BINLOG_ALLOW_QUERY_NETWORKS];
static int binlog_allow_query_networks, binlog_allow_query_networks6;

static int get_label_start (const char *s, int l) {
  int i, x = -1, y = -1;
  if (l == 1 && s[0] >= 'a' && s[0] <= 'z') {
    y = s[0] - 'a';
    if (lo_alpha[y] >= 0) {
      labels_saved_bytes += l;
      return lo_alpha[y];
    }
  }
  if (l <= 3) {
    x = 0;
    for (i = 0; i < l; i++) {
      if (s[i] >= '0' && s[i] <= '9') {
        x = x * 10 + (s[i] - '0');
      } else {
        x = -1;
        break;
      }
    }
    if (x >= 256 || (l > 1 && s[0] == '0')) {
      x = -1;
    }
    if (x >= 0 && dec_number[x] >= 0) {
      labels_saved_bytes += l;
      return dec_number[x];
    }
  }

  int m;
  for (m = l < labels_wptr ? l : labels_wptr; m >= 1; m--) {
    for (i = 0; i < m; i++) {
      if (s[i] != labels_buff[labels_wptr - m + i]) {
        break;
      }
    }
    if (i >= m) {
      break;
    }
  }
  labels_saved_bytes += m;
  if (labels_wptr + l - m > DNS_LABELS_BUFFSIZE) {
    kprintf ("%s: Label buffer overflow, try to increase DNS_LABELS_BUFFSIZE(%d) define.\n", __func__, DNS_LABELS_BUFFSIZE);
    return -1;
  }
  int r = labels_wptr - m;
  if (x >= 0) {
    dec_number[x] = r;
  }
  if (y >= 0) {
    lo_alpha[y] = r;
  }
  memcpy (labels_buff + r, s, l);
  labels_wptr = r + l;
  return r;
}

static int create_new_node (void) {
  if (trie_nodes >= DNS_MAX_TRIE_NODES) {
    kprintf ("%s: Nodes buffer overflow, try to increase DNS_MAX_TRIE_NODES(%d) define.\n", __func__, DNS_MAX_TRIE_NODES);
    exit (1);
  }
  names[trie_nodes].first_record_id = -1;
  names[trie_nodes].zone_id = cur_zone_id;
  return trie_nodes++;
}

static int get_node_f (int parent_node_id, const char *label, int label_len, int force) {
  vkprintf (4, "%s: parent_node_id = %d, label = '%.*s', force = %d\n", __func__, parent_node_id, label_len, label, force);
  unsigned int h = parent_node_id;
  int i;
  char *s = alloca (label_len);
  for (i = 0; i < label_len; i++) {
    h = h * 239 + (s[i] = tolower (label[i]));
  }
  h %= DNS_EDGE_HASH_SIZE;
  int *p = HE + h;
  dns_name_trie_edge_t *V;
  while (*p >= 0) {
    V = &edge_buff[*p];
    if (parent_node_id == V->parent_node_id && label_len == V->label_len && !memcmp (&labels_buff[V->label_start], (const char *) s, label_len)) {
      int v = *p;
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = HE[h];
        HE[h] = v;
      }
      return v;
    }
    p = &V->hnext;
  }
  if (force > 0) {
    if (trie_edges >= DNS_MAX_TRIE_EDGES) {
      kprintf ("%s: Trie edges buffer overflow. Try to increase DNS_MAX_TRIE_EDGES(%d) define.\n", __func__, DNS_MAX_TRIE_EDGES);
      exit (1);
    }
    V = &edge_buff[trie_edges];
    V->parent_node_id = parent_node_id;
    V->label_start = get_label_start ((const char *) s, label_len);
    V->label_len = label_len;
    V->child_node_id = create_new_node ();
    V->hnext = HE[h];
    return HE[h] = trie_edges++;
  }
  return -1;
}

static int get_name_f (const char *name, const int name_len, int force) {
  vkprintf (4, "%s: name='%.*s', force=%d\n", __func__, name_len, name, force);
  int parent_node_id = -1;
  int l = name_len;
  while (l > 0) {
    int i;
    for (i = l - 1; i >= 0 && name[i] != '.'; i--) { }
    //label = name[i+1:l]
    const int node_id = get_node_f (parent_node_id, name + i + 1, (l - (i + 1)), force);
    if (node_id < 0) {
      return -1;
    }
    l = i;
    parent_node_id = edge_buff[node_id].child_node_id;
  }
  return parent_node_id;
}

static int find_name (const char *name, int l, int *name_id, int *zone_id) {
  *name_id = get_name_f (name, l, 0);
  if (*name_id < 0) {
    /* try to find *.<domain> */
    int parent_node_id = -1;
    while (l > 0) {
      int i;
      for (i = l - 1; i >= 0 && name[i] != '.'; i--) { }
      //label = name[i+1:l]
      const int star_name_id = get_node_f (parent_node_id, "*", 1, 0);
      if (star_name_id >= 0) {
        *name_id = star_name_id;
      }
      const int node_id = get_node_f (parent_node_id, name + i + 1, (l - (i + 1)), 0);
      if (node_id < 0) {
        break;
      }
      l = i;
      parent_node_id = edge_buff[node_id].child_node_id;
    }
    if (*name_id < 0) {
      *zone_id = parent_node_id >= 0 ? names[parent_node_id].zone_id : -1;
      return -1;
    }
  }
  *zone_id = names[*name_id].zone_id;
  return 0;
}

#define DNS_ERR_UNKNOWN_NAME (-2)
#define DNS_ERR_BUFFER_OVERFLOW (-3)

static int dns_get_records (char *name, int name_len, int data_qtype, int *name_id, int *zone_id, dns_record_t *R, int avail_out) {
  vkprintf (4, "%s(\"%.*s\", data_qtype: %d)\n", __func__, name_len, name, data_qtype);
  if (find_name (name, name_len, name_id, zone_id) < 0) {
    return DNS_ERR_UNKNOWN_NAME;
  }
  int n = 0;
  if (data_qtype == dns_qtype_any) {
    int *w = &names[*name_id].first_record_id;
    int tp = -1;
    while (*w >= 0) {
      dns_trie_record_t *p = (dns_trie_record_t *) (&records_buff[RB[*w].record_off]);
      assert (tp != p->data_type);
      tp = p->data_type;
      int *t = &RB[*w].next, m = 1;
      while (*t >= 0) {
        p = (dns_trie_record_t *) (&records_buff[RB[*t].record_off]);
        if (p->data_type != tp) {
          break;
        }
        m++;
        t = &RB[*t].next;
      }
      vkprintf (4, "%s: tp = %d, m = %d\n", __func__, tp, m);
      int *last = t;
      if (m > 1) {
        int *u = &RB[*w].next;
        /* (*w, *t, *u) := (*u, *w, *t) */
        int tmp = *u;
        *u = *t;
        *t = *w;
        *w = tmp;
        last = u;
      }
      if (m > dns_max_response_records) {
        m = dns_max_response_records;
      }
      int o = *w, u;
      for (u = 0; u < m; u++) {
        if (--avail_out < 0) {
          return DNS_ERR_BUFFER_OVERFLOW;
        }
        R->name = name;
        R->name_len = name_len;
        assert (o >= 0);
        R->R = (dns_trie_record_t *) (&records_buff[RB[o].record_off]);
        R++;
        o = RB[o].next;
      }
      n += m;
      w = last;
    }
  } else {
    int *w = &names[*name_id].first_record_id;
    if (*w < 0) {
      return 0;
    }
    dns_trie_record_t *p = (dns_trie_record_t *) (&records_buff[RB[*w].record_off]);
    if (p->data_type == dns_type_cname) {
      if (--avail_out < 0) {
        return DNS_ERR_BUFFER_OVERFLOW;
      }
      R->name = name;
      R->name_len = name_len;
      R->R = p;
      R++;
      n++;
      if (data_qtype == dns_type_cname) {
        return n;
      }
      int cname_id, cname_zone_id;
      int res = dns_get_records (p->data, p->data_len, data_qtype, &cname_id, &cname_zone_id, R, avail_out);
      if (res == DNS_ERR_BUFFER_OVERFLOW) {
        return DNS_ERR_BUFFER_OVERFLOW;
      }
      if (res > 0) {
        n += res;
      }
      return n;
    }
    for (;;) {
      if (p->data_type == data_qtype) {
        int *t = &RB[*w].next;
        while (*t >= 0) {
          p = (dns_trie_record_t *) (&records_buff[RB[*t].record_off]);
          if (p->data_type != data_qtype) {
            break;
          }
          t = &RB[*t].next;
        }
        int h = *w;
        *w = *t;
        *t = -1;
        if (RB[h].next < 0) {
          /* one record */
        } else {
          /* move first record to the end */
          *t = h;
          t = &RB[h].next;
          h = *t;
          *t = -1;
        }
        int o = h;
        while (o >= 0 && n < dns_max_response_records) {
          if (--avail_out < 0) {
            return DNS_ERR_BUFFER_OVERFLOW;
          }
          R->name = name;
          R->name_len = name_len;
          R->R = (dns_trie_record_t *) (&records_buff[RB[o].record_off]);
          R++;
          o = RB[o].next;
          n++;
        }
        *t = names[*name_id].first_record_id;
        names[*name_id].first_record_id = h;
        return n;
      }
      w = &RB[*w].next;
      if (*w < 0) {
        break;
      }
      p = (dns_trie_record_t *) (&records_buff[RB[*w].record_off]);
    }
  }
  return n;
}

static unsigned int dns_get_ttl (dns_trie_record_t *R, dns_zone_t *Z) {
  if (!R->flag_has_ttl) {
    return Z->ttl;
  }
  unsigned int x;
  memcpy (&x, R->data + R->data_len, 4);
  return x;
}

/********************* Records hash  *********************/
int tot_hashed_records = 0, max_hashed_records, records_hash_prime;
int *RH;
static void dns_record_hash_init (void) {
  if (RH) {
    return;
  }
  records_hash_prime = am_choose_hash_prime (tot_hashed_records ? tot_hashed_records * 2 : 1000000);
  max_hashed_records = records_hash_prime * 0.75;
  RH = malloc (records_hash_prime * 4);
  memset (RH, 0xff, records_hash_prime * 4);
  tot_hashed_records = 0;
}

static void dns_record_hash_free (void) {
  if (RH) {
    free (RH);
    RH = NULL;
  }
}

static int dns_record_hash_lookup (int record_off) {
  if (RH == NULL) {
    return -1;
  }
  dns_trie_record_t *R = (dns_trie_record_t *) (&records_buff[record_off]);
  switch (R->data_type) {
    case dns_type_a:
    case dns_type_aaaa:
    case dns_type_ptr:
    case dns_type_cname:
      break;
    default:
      return -1;
  }
  unsigned int h1 = R->data_type, h2 = R->data_type;
  int sz = R->data_len;
  if (R->flag_has_ttl) {
    sz += 4;
  }
  int i;
  for (i = 0; i < sz; i++) {
    h1 = (257 * h1 + R->data[i]) % records_hash_prime;
    h2 = (3 * h2 + R->data[i]) % (records_hash_prime - 1);
  }
  h2++;
  while (RH[h1] >= 0) {
    dns_trie_record_t *Q = (dns_trie_record_t *) (&records_buff[RH[h1]]);
    if (Q->data_type == R->data_type && Q->data_len == R->data_len && Q->flag_has_ttl == R->flag_has_ttl && !memcmp (Q->data, R->data, sz)) {
      return RH[h1];
    }
    h1 += h2;
    if (h1 >= records_hash_prime) {
      h1 -= records_hash_prime;
    }
  }
  if (tot_hashed_records >= max_hashed_records) {
    return -1;
  }
  tot_hashed_records++;
  RH[h1] = record_off;
  return -1;
}

static int dns_name_add_record (int name_id, int data_type, unsigned int record_ttl, void *data, int data_len) __attribute__ ((warn_unused_result));
static int dns_name_add_record (int name_id, int data_type, unsigned int record_ttl, void *data, int data_len) {
  vkprintf (4, "%s: name_id = %d, data_type = %d, data_len = %d\n", __func__, name_id, data_type, data_len);
  assert (name_id >= 0);
  assert (data_len >= 0 && data_len <= 0xffff);
  dns_zone_t *z = &Z[names[name_id].zone_id];
  switch (data_type) {
    case dns_type_a: assert (data_len == 4); break;
    case dns_type_aaaa: assert (data_len == 16); break;
    case dns_type_ns: assert (data_len == sizeof (void *)); break;
    case dns_type_soa:
      z->soa_record = 1;
      break;
    case dns_type_ptr:
    case dns_type_txt:
    case dns_type_mx:
    case dns_type_cname:
    case dns_type_srv:
      break;
    default: assert (0);
  }
  int has_ttl = (z->ttl != record_ttl) ? 1 : 0;
  int sz = sizeof (dns_trie_record_t) + data_len + 4 * has_ttl;
  if (tot_records >= DNS_MAX_RECORDS) {
    kprintf ("Too many records. Try to increase DNS_MAX_RECORDS(%d) define.\n", DNS_MAX_RECORDS);
    exit (1);
  }
  if (records_wptr + sz > DNS_RECORDS_BUFFSIZE) {
    kprintf ("Records buffer overflow. Try to increase DNS_RECORDS_BUFFSIZE(%d) define.\n", DNS_RECORDS_BUFFSIZE);
    exit (1);
  }
  dns_trie_record_t *R = (dns_trie_record_t *) (&records_buff[records_wptr]);
  Z[cur_zone_id].records++;
  R->flag_has_ttl = has_ttl;
  R->data_type = data_type;
  R->data_len = data_len;
  memcpy (R->data, data, data_len);
  if (has_ttl) {
    memcpy (R->data + data_len, &record_ttl, 4);
  }
  int o = dns_record_hash_lookup (records_wptr);
  if (o < 0) {
    o = records_wptr;
    records_wptr += sz;
  } else {
    records_saved_bytes += sz;
  }
  /* records with same type are consecutive */
  int *w = &names[name_id].first_record_id;

  if (data_type == dns_type_cname && (*w) >= 0) {
    kprintf ("An alias defined in a CNAME record must have no other resource records of other types.\n");
    return -1;
  }

  while (*w >= 0) {
    dns_trie_record_t *p = (dns_trie_record_t *) (&records_buff[RB[*w].record_off]);
    if (p->data_type == dns_type_cname) {
      kprintf ("An alias defined in a CNAME record must have no other resource records of other types.\n");
      return -1;
    }
    if (p->data_type == data_type) {
      do {
        w = &RB[*w].next;
      } while (*w >= 0 && ((dns_trie_record_t *) (&records_buff[RB[*w].record_off]))->data_type == data_type);
      break;
    }
    w = &RB[*w].next;
  }
  int x = -1;
  if (free_rb >= 0) {
    x = free_rb;
    free_rb = RB[x].next;
    free_records--;
    vkprintf (4, "%s: Use free record slot. %d free records slots are remaining.\n", __func__, free_records);
  } else {
    x = tot_records++;
  }
  assert (x >= 0);
  RB[x].record_off = o;
  RB[x].next = *w;
  *w = x;
  return 0;
}

static int dns_name_delete_records (int name_id, int data_type) {
  assert (name_id >= 0);
  /* records with same type are consecutive */
  int *w = &names[name_id].first_record_id;
  while (*w >= 0) {
    dns_trie_record_t *p = (dns_trie_record_t *) (&records_buff[RB[*w].record_off]);
    if (p->data_type == data_type) {
      int *y = w, t = 0;
      do {
        w = &RB[*w].next;
        t++;
      } while (*w >= 0 && ((dns_trie_record_t *) (&records_buff[RB[*w].record_off]))->data_type == data_type);
      int first_delete = *y;
      *y = *w;
      *w = free_rb;
      free_rb = first_delete;
      free_records += t;
      break;
    }
    w = &RB[*w].next;
  }
  return 0;
}

/******************** Fetch functions  ********************/
static int dns_read_iterator_fetch_uchar (dns_read_iterator_t *B, unsigned char *res) __attribute__ ((warn_unused_result));
static int dns_read_iterator_fetch_uchars (dns_read_iterator_t *B, int len, unsigned char *res) __attribute__ ((warn_unused_result));
static int dns_read_iterator_fetch_ushort (dns_read_iterator_t *B, unsigned short *res) __attribute__ ((warn_unused_result));
static int dns_read_iterator_fetch_header (dns_read_iterator_t *B, dns_header_t *H) __attribute__ ((warn_unused_result));
static int dns_read_iterator_fetch_name (dns_read_iterator_t *B, char *output, char *wptr, int avail_out) __attribute__ ((warn_unused_result));
static int dns_read_iterator_fetch_question_section (dns_read_iterator_t *B, dns_question_section_t *QS) __attribute__ ((warn_unused_result));

static void dns_read_iterator_init (dns_read_iterator_t *B, unsigned char *in, int ilen) {
  B->start = B->rptr = in;
  B->ilen = B->avail_in = ilen;
}

static int dns_read_iterator_fetch_uchar (dns_read_iterator_t *B, unsigned char *res) {
  if (B->avail_in < 1) {
    return -1;
  }
  *res = B->rptr[0];
  B->avail_in -= 1;
  B->rptr += 1;
  return 0;
}

static int dns_read_iterator_fetch_uchars (dns_read_iterator_t *B, int len, unsigned char *res) {
  if (B->avail_in < len) {
    return -1;
  }
  memcpy (res, B->rptr, len);
  B->avail_in -= len;
  B->rptr += len;
  return 0;
}

static int dns_read_iterator_fetch_ushort (dns_read_iterator_t *B, unsigned short *res) {
  if (B->avail_in < 2) {
    return -1;
  }
  *res = (((int) B->rptr[0]) << 8) + (int) B->rptr[1];
  B->avail_in -= 2;
  B->rptr += 2;
  return 0;
}

static int dns_read_iterator_fetch_header (dns_read_iterator_t *B, dns_header_t *H) {
  if (dns_read_iterator_fetch_ushort (B, &H->id) < 0) {
    return -1;
  }
  if (dns_read_iterator_fetch_ushort (B, &H->flags) < 0) {
    return -1;
  }
  if (dns_read_iterator_fetch_ushort (B, &H->qdcount) < 0) {
    return -1;
  }
  if (dns_read_iterator_fetch_ushort (B, &H->ancount) < 0) {
    return -1;
  }
  if (dns_read_iterator_fetch_ushort (B, &H->nscount) < 0) {
    return -1;
  }
  if (dns_read_iterator_fetch_ushort (B, &H->arcount) < 0) {
    return -1;
  }
  vkprintf (3, "%s: id = %d, flags = %d, qdcount = %d, ancount = %d, nscount = %d, arcount = %d\n",
    __func__, (int) H->id, (int) H->flags, (int) H->qdcount, (int) H->ancount, (int) H->nscount, (int) H->arcount);
  return 0;
}

static int dns_read_iterator_fetch_name (dns_read_iterator_t *B, char *output, char *wptr, int avail_out) {
  for (;;) {
    unsigned char c;
    if (dns_read_iterator_fetch_uchar (B, &c) < 0) {
      return -1;
    }
    if (!c) {
      break;
    }
    if ((c & 0xc0) == 0xc0) {
      /* 4.1.4. Message compression */
      unsigned char d;
      c &= 63;
      if (dns_read_iterator_fetch_uchar (B, &d) < 0) {
        return -1;
      }
      unsigned short off = (c & 63) * 256 + d;
      if (off >= B->ilen) {
        return -1;
      }
      dns_read_iterator_t B2;
      dns_read_iterator_init (&B2, B->start + off, B->ilen - off);
      if (dns_read_iterator_fetch_name (&B2, output, wptr, avail_out) < 0) {
        return -1;
      }
      return 0;
    }
    if (c & 0xc0) {
      return -1;
    }
    int l = c;
    if (!l) {
      break;
    }
    if (output != wptr) {
      if (avail_out <= 0) {
        return -1;
      }
      avail_out--;
      *wptr++ = '.';
    }
    if (avail_out < l) {
      return -1;
    }
    if (dns_read_iterator_fetch_uchars (B, l, (unsigned char *) wptr) < 0) {
      return -1;
    }
    avail_out -= l;
    wptr += l;
  }
  if (avail_out <= 0) {
    return -1;
  }
  avail_out--;
  *wptr = 0;
  return wptr - output;
}

static int dns_read_iterator_fetch_question_section (dns_read_iterator_t *B, dns_question_section_t *QS) {
  QS->name_len = dns_read_iterator_fetch_name (B, QS->name, QS->name, sizeof (QS->name));
  if (QS->name_len < 0) {
    return -1;
  }
  if (dns_read_iterator_fetch_ushort (B, &QS->qtype) < 0) {
    return -1;
  }
  if (dns_read_iterator_fetch_ushort (B, &QS->qclass) < 0) {
    return -1;
  }
  vkprintf (3, "%s: name = '%s', qtype = %d, qclass = %d\n",
    __func__, QS->name, (int) QS->qtype, (int) QS->qclass);
  return 0;
}

static int dns_read_iterator_fetch_max_udp_buffsize (dns_read_iterator_t *B, int *max_udp_buffsize) {
  unsigned char ch;
  if (dns_read_iterator_fetch_uchar (B, &ch) < 0) {
    return -1;
  }
  if (ch) {
    return -1;
  }
  unsigned short s, payload, rdlen;
  if (dns_read_iterator_fetch_ushort (B, &s) < 0 || s != dns_type_opt) {
    return -1;
  }
  if (dns_read_iterator_fetch_ushort (B, &payload) < 0) {
    return -1;
  }
  unsigned int ttl;
  if (dns_read_iterator_fetch_uchars (B, 4, (unsigned char *) &ttl) < 0) {
    return -1;
  }
  /* NOTICE: ttl in network order */
  if (dns_read_iterator_fetch_ushort (B, &rdlen) < 0) {
    return -1;
  }
  if (rdlen) {
    return -1;
  }
  if (payload < 512) {
    return -1;
  }
  *max_udp_buffsize = payload;
  vkprintf (3, "%s: max_udp_buffsize is %d bytes.\n", __func__, *max_udp_buffsize);
  return 0;
}

int dns_query_parse (dns_query_t *q, unsigned char *in, int ilen, int udp) {
  int i;
  q->flags = udp ? DNS_QUERY_FLAG_UDP : 0;
  q->max_udp_buffsize = 512;
  dns_read_iterator_t it;
  dns_read_iterator_init (&it, in, ilen);
  if (dns_read_iterator_fetch_header (&it, &q->header) < 0) {
    return -1;
  }
  if (q->header.flags & dns_header_flag_response) {
    q->flags |= DNS_QUERY_FLAG_BAD_FORMAT;
    return 0;
  }
  if (q->header.ancount) {
    vkprintf (2, "%s: header ancount isn't 0.\n", __func__);
    q->flags |= DNS_QUERY_FLAG_BAD_FORMAT;
    return 0;
  }
  for (i = 0; i < q->header.qdcount; i++) {
    if (dns_read_iterator_fetch_question_section (&it, &q->QS) < 0) {
      q->flags |= DNS_QUERY_FLAG_BAD_FORMAT;
      return 0;
    }
    break;
  }
  if (udp && !q->header.nscount && q->header.arcount == 1 && it.avail_in == 11) {
    dns_read_iterator_fetch_max_udp_buffsize (&it, &q->max_udp_buffsize);
    q->flags |= DNS_QUERY_FLAG_EDNS;
  }
  return 0;
}

/******************** Store functions  ********************/
static int dns_write_iterator_store_uchar (dns_write_iterator_t *B, unsigned char x) __attribute__ ((warn_unused_result));
static int dns_write_iterator_store_ushort (dns_write_iterator_t *B, unsigned short x) __attribute__ ((warn_unused_result));
static int dns_write_iterator_store_uint (dns_write_iterator_t *B, unsigned int x) __attribute__ ((warn_unused_result));
static int dns_write_iterator_store_label (dns_write_iterator_t *B, const char *s, int l) __attribute__ ((warn_unused_result));
static int dns_write_iterator_store_raw_data (dns_write_iterator_t *B, unsigned char *data, int data_len) __attribute__ ((warn_unused_result));
static int dns_write_iterator_store_name (dns_write_iterator_t *B, char *name, int l) __attribute__ ((warn_unused_result));
static int dns_write_iterator_store_rr (dns_write_iterator_t *B, char *name, int l, unsigned short rtype, unsigned int ttl) __attribute__ ((warn_unused_result));

static void dns_write_iterator_init (dns_write_iterator_t *B, unsigned char *out, int olen) {
  B->start = B->wptr = out;
  B->olen = B->avail_out = olen;
  B->domains = 0;
  B->domain_wptr = B->domain_buff;
}

static int dns_write_iterator_advance (dns_write_iterator_t *B, int len) {
  if (B->avail_out < len) {
    return -1;
  }
  B->wptr += len;
  B->avail_out -= len;
  return 0;
}

static int dns_write_iterator_store_uchar (dns_write_iterator_t *B, unsigned char x) {
  if (B->avail_out < 1) {
    return -1;
  }
  *B->wptr = x;
  B->avail_out -= 1;
  B->wptr += 1;
  return 0;
}

static int dns_write_iterator_store_ushort (dns_write_iterator_t *B, unsigned short x) {
  if (B->avail_out < 2) {
    return -1;
  }
  B->wptr[0] = (x >> 8);
  B->wptr[1] = (x & 0xff);
  B->avail_out -= 2;
  B->wptr += 2;
  return 0;
}

static int dns_write_iterator_store_uint (dns_write_iterator_t *B, unsigned int x) {
  if (B->avail_out < 4) {
    return -1;
  }
  B->avail_out -= 4;
  x = __builtin_bswap32 (x);
  memcpy (B->wptr, &x, 4);
  B->wptr += 4;
  return 0;
}

static int dns_write_iterator_store_label (dns_write_iterator_t *B, const char *s, int l) {
  if (l > 63 || B->avail_out < l + 1) {
    return -1;
  }
  *B->wptr++ = l;
  memcpy (B->wptr, s, l);
  B->wptr += l;
  B->avail_out -= l + 1;
  return 0;
}

/* converts from host (little-endian) to network (big-endian) */
static int dns_write_iterator_store_raw_data (dns_write_iterator_t *B, unsigned char *data, int data_len) {
  if (B->avail_out < data_len) {
    return -1;
  }
  int i;
  for (i = data_len - 1; i >= 0; i--) {
    *B->wptr++ = data[i];
  }
  B->avail_out -= data_len;
  return 0;
}

static int dns_write_iterator_name_lookup (dns_write_iterator_t *B, char *name, int l) {
  int i;
  for (i = 0; i < B->domains; i++) {
    if (l == B->domain[i].len && !memcmp (name, B->domain[i].s, l)) {
      return B->domain[i].off;
    }
  }
  return -1;
}

static int dns_write_iterator_store_name (dns_write_iterator_t *B, char *name, int l) {
  char *domain_rptr = NULL;
  char *p = name;
  while (l > 0) {
    int i = dns_write_iterator_name_lookup (B, p, l);
    if (i >= 0) {
      if (i >= 0x4000) {
        return -1;
      }
      if (dns_write_iterator_store_ushort (B, 0xc000 + i) < 0) {
        return -1;
      }
      return 0;
    }
    if (domain_rptr == NULL) {
      if (B->domain_wptr + l >= B->domain_buff + DNS_WRITE_ITERATOR_DOMAIN_BUFFSIZE) {
        vkprintf (3, "%s: write iterator domain buffer overflow.\n", __func__);
        return -1;
      }
      domain_rptr = B->domain_wptr;
      memcpy (B->domain_wptr, name, l);
      B->domain_wptr += l;
    }
    if (B->domains >= DNS_MAX_WRITE_ITERATOR_NAMES) {
      return -1;
    }
    B->domain[B->domains].s = domain_rptr;
    B->domain[B->domains].len = l;
    B->domain[B->domains].off = B->wptr - B->start;
    B->domains++;
    char *q = memchr (p, '.', l);
    const int o = q ? q - p : l;
    if (dns_write_iterator_store_label (B, p, o) < 0) {
      return -1;
    }
    if (q == NULL) {
      break;
    }
    l -= o + 1;
    domain_rptr += o + 1;
    p = q + 1;
  }
  if (dns_write_iterator_store_uchar (B, 0) < 0) {
    return -1;
  }
  return 0;
}

static int dns_write_iterator_store_rr (dns_write_iterator_t *B, char *name, int name_len, unsigned short rtype, unsigned int ttl) {
  B->record_start = B->wptr;
  if (dns_write_iterator_store_name (B, name, name_len) < 0) {
    return -1;
  }
  if (B->avail_out < 8) {
    return -1;
  }
  B->avail_out -= 8;
  *(B->wptr)++ = rtype >> 8;
  *(B->wptr)++ = rtype & 0xff;
  *(B->wptr)++ = dns_class_in >> 8;
  *(B->wptr)++ = dns_class_in & 0xff;
  ttl = __builtin_bswap32 (ttl);
  memcpy (B->wptr, &ttl, 4);
  B->wptr += 4;
  return 0;
}

static int dns_store_header (dns_response_t *r) {
  unsigned short *p = (unsigned short *) r->Out.start;
  dns_header_t *H = &r->header;
  switch (r->rcode) {
    case dns_rcode_no_error:
      wstat.rcode_no_error_queries++;
      break;
    case dns_rcode_format_error:
      wstat.rcode_format_queries++;
      break;
    case dns_rcode_server_failure:
      wstat.rcode_server_failure_queries++;
      break;
    case dns_rcode_name_error:
      wstat.rcode_name_error_queries++;
      break;
    case dns_rcode_not_implemented:
      wstat.rcode_not_implemented_queries++;
      break;
    case dns_rcode_refused:
      wstat.rcode_refused_queries++;
      break;
    default:
      assert (0);
  }
  p[0] = htons (H->id);
  H->flags |= r->rcode;
  p[1] = htons (H->flags);
  p[2] = htons (H->qdcount);
  p[3] = htons (H->ancount);
  p[4] = htons (H->nscount);
  p[5] = htons (H->arcount);
  return r->Out.wptr - r->Out.start;
}

static int dns_write_iterator_soa_rdata (dns_response_t *r, dns_soa_t *S, unsigned short *rlen) {
  int i;
  dns_write_iterator_t *out = &r->Out;
  if (dns_write_iterator_store_name (out, S->mname, S->mname_len) < 0) { return -1; }
  if (dns_write_iterator_store_name (out, S->rname + S->mname_len, S->rname_len) < 0) { return -1; }
  for (i = 0; i < 5; i++) {
    if (dns_write_iterator_store_uint (out, S->data[i]) < 0) { return -1; }
  }
  *rlen = htons ((out->wptr - (unsigned char *) rlen) - 2);
  return 0;
}

static int dns_write_iterator_srv_rdata (dns_response_t *r, dns_srv_t *S, unsigned short *rlen) {
  dns_write_iterator_t *out = &r->Out;
  if (dns_write_iterator_store_ushort (out, S->priority) < 0) { return -1; }
  if (dns_write_iterator_store_ushort (out, S->weight) < 0) { return -1; }
  if (dns_write_iterator_store_ushort (out, S->port) < 0) { return -1; }
  if (dns_write_iterator_store_name (out, S->target, S->target_len) < 0) { return -1; }
  *rlen = htons ((out->wptr - (unsigned char *) rlen) - 2);
  return 0;
}

static void dns_addresses_reset (dns_adresses_t *A) {
  A->a_ttl = A->aaaa_ttl = -2;
}

static void dns_adresses_find (dns_response_t *r, char *name, int name_len, dns_adresses_t *A) {
  if (A->a_ttl == -2) {
    int name_id, zone_id;
    if (find_name (name, name_len, &name_id, &zone_id) >= 0) {
      int o = names[name_id].first_record_id;
      //TODO: cname
      while (o >= 0) {
        dns_trie_record_t *p = (dns_trie_record_t *) (&records_buff[RB[o].record_off]);
        if (p->data_type == dns_type_a) {
          memcpy (&A->ipv4, p->data, 4);
          A->a_ttl = dns_get_ttl (p, &Z[names[name_id].zone_id]);
          if (A->a_ttl < 0) {
            A->a_ttl = -1;
          }
        } else if (p->data_type == dns_type_aaaa) {
          memcpy (&A->ipv6, p->data, 16);
          A->aaaa_ttl = dns_get_ttl (p, &Z[names[name_id].zone_id]);
          if (A->aaaa_ttl < 0) {
            A->aaaa_ttl = -1;
          }
        }
        o = RB[o].next;
      }
    } else {
      A->a_ttl = A->aaaa_ttl = -1;
    }
  }
  if (A->a_ttl >= 0) {
    vkprintf (4, "%s: incr additional_records (A) for name '%.*s'\n", __func__, name_len, name);
    r->additional_records++;
  }
  if (A->aaaa_ttl >= 0) {
    vkprintf (4, "%s: incr additional_records (AAAA) for name '%.*s'\n", __func__, name_len, name);
    r->additional_records++;
  }
}

static int dns_write_iterator_mx_rdata (dns_response_t *r, dns_mx_t *M, unsigned short *rlen) {
  vkprintf (3, "%s: preference = %d, exchanged = '%s'\n", __func__, M->preference, M->exchange);
  dns_write_iterator_t *out = &r->Out;
  if (dns_write_iterator_store_ushort (out, M->preference) < 0) { return -1; }
  if (dns_write_iterator_store_name (out, M->exchange, M->exchange_len) < 0) { return -1; }
  *rlen = htons ((out->wptr - (unsigned char *) rlen) - 2);
  dns_adresses_find (r, M->exchange, M->exchange_len, &M->addrs_exchange);
  return 0;
}

static int dns_write_iterator_ns_rdata (dns_response_t *r, dns_nameserver_en_t *N, unsigned short *rlen) {
  //vkprintf (3, "%s: preference = %d, exchanged = '%s'\n", __func__, M->preference, M->exchange);
  dns_write_iterator_t *out = &r->Out;
  if (dns_write_iterator_store_name (out, N->nsdname, N->nsdname_len) < 0) { return -1; }
  *rlen = htons ((out->wptr - (unsigned char *) rlen) - 2);
  dns_adresses_find (r, N->nsdname, N->nsdname_len, &N->addrs_nsd);
  return 0;
}

static int dns_store_error (dns_response_t *r, enum dns_rcode status) {
  vkprintf (3, "%s: status = %d\n", __func__, status);
  r->rcode = status;
  assert (r->Out.start == r->Out.wptr);
  if (dns_write_iterator_advance (&r->Out, 12) < 0) {
    return -1;
  }
  dns_query_t *q = r->q;
  if (q->header.qdcount == 1 && status != dns_rcode_format_error) {
    /* question */
    r->Out.record_start = r->Out.wptr;
    if (dns_write_iterator_store_name (&r->Out, q->QS.name, q->QS.name_len) < 0) { return -2; }
    if (dns_write_iterator_store_ushort (&r->Out, q->QS.qtype) < 0) { return -2; }
    if (dns_write_iterator_store_ushort (&r->Out, q->QS.qclass) < 0) { return -2; }
    r->header.qdcount++;
  }
  return dns_store_header (r);
}

static int dns_write_iterator_store_address (dns_response_t *r, char *name, int name_len, dns_adresses_t *A) {
  int k;
  dns_write_iterator_t *out = &r->Out;
  if (A->a_ttl >= 0) {
    for (k = 0; k < r->answers; k++) {
      if (r->RA[k].R->data_type == dns_type_a && r->RA[k].name_len == name_len && !memcmp (name, r->RA[k].name, name_len) && !memcmp (&A->ipv4, r->RA[k].R->data, 4)) {
        break;
      }
    }
    if (k >= r->answers) {
      vkprintf (4, "%s: store A record for name '%.*s'\n", __func__, name_len, name);
      if (dns_write_iterator_store_rr (out, name, name_len, dns_type_a, A->a_ttl) < 0) {
        return -1;
      }
      if (dns_write_iterator_store_ushort (out, 4) < 0) { return -1; }
      if (dns_write_iterator_store_uint (out, A->ipv4) < 0) { return -1; }
      r->header.arcount++;
    }
  }
  if (A->aaaa_ttl >= 0) {
    for (k = 0; k < r->answers; k++) {
      if (r->RA[k].R->data_type == dns_type_aaaa && r->RA[k].name_len == name_len && !memcmp (name, r->RA[k].name, name_len) && !memcmp (A->ipv6, r->RA[k].R->data, 16)) {
        break;
      }
    }
    if (k >= r->answers) {
      vkprintf (4, "%s: store AAAA record for name '%.*s'\n", __func__, name_len, name);
      if (dns_write_iterator_store_rr (out, name, name_len, dns_type_aaaa, A->aaaa_ttl) < 0) {
        return -1;
      }
      if (dns_write_iterator_store_ushort (out, 16) < 0) { return -1; }
      if (dns_write_iterator_store_raw_data (out, (unsigned char *) A->ipv6, 16) < 0) { return -1; }
      r->header.arcount++;
    }
  }
  return 0;
}

static int dns_check_ip6 (dns_network6_t *N, unsigned char *ipv6) {
  unsigned long long *m = (unsigned long long *) N->mask;
  unsigned long long *a = (unsigned long long *) N->ipv6;
  unsigned long long *b = (unsigned long long *) ipv6;
  if (((a[0] ^ b[0]) & m[0]) | ((a[1] ^ b[1]) & m[1])) {
    return -1;
  }
  return 0;
}

static int dns_query_check_ip (dns_query_t *q) {
  int i;
  if (!(q->flags & DNS_QUERY_FLAG_IPV6)) {
    for (i = 0; i < binlog_allow_query_networks; i++) {
      vkprintf (4, "%s: %s and %s (mask = %s)\n", __func__, show_ip (q->ipv4), show_ip (BAQN[i].ipv4), show_ip (BAQN[i].mask));
      if (!((BAQN[i].ipv4 ^ q->ipv4) & BAQN[i].mask)) {
        return 2 * i;
      }
    }
  }
  for (i = 0; i < binlog_allow_query_networks6; i++) {
    vkprintf (4, "%s: %s and %s (mask = %s)\n", __func__, show_ipv6 (q->ipv6), show_ipv6 (BAQN6[i].ipv6), show_ipv6 (BAQN6[i].mask));
    if (!dns_check_ip6 (BAQN6 + i, q->ipv6)) {
      return 2 * i + 1;
    }
  }
  return -1;
}

static int ignore_case_equal_strings (char *x, char *y, int len) {
  while (--len >= 0) {
    if (tolower (*x) != tolower (*y)) { return 0; }
    x++;
    y++;
  }
  return 1;
}

static int dns_gen_response (dns_response_t *r, unsigned char *out, int olen) {
  int k;
  dns_query_t *q = r->q;
  if ((q->flags & DNS_QUERY_FLAG_UDP) && olen > q->max_udp_buffsize) {
    olen = q->max_udp_buffsize;
  }
  r->rcode = dns_rcode_no_error;
  r->additional_records = 0;
  dns_write_iterator_init (&r->Out, out, olen);
  r->header.id = q->header.id;
  r->header.flags = q->header.flags & dns_header_flag_rd;
  r->header.flags |= dns_header_flag_response;
  r->header.qdcount = r->header.ancount = r->header.nscount = r->header.arcount = 0;
  if (q->flags & DNS_QUERY_FLAG_BAD_FORMAT) {
    return dns_store_error (r, dns_rcode_format_error);
  }
  const int opcode = q->header.flags & dns_header_mask_opcode;
  if (opcode != dns_header_flag_opcode_query) {
    vkprintf (2, "%s: Unimplemented opcode %d.\n", __func__, opcode);
    return dns_store_error (r, dns_rcode_not_implemented);
  }
  if (q->header.qdcount > 1) {
    vkprintf (2, "%s: Too many queries (qdcount = %d).\n", __func__, (int) q->header.qdcount);
    return dns_store_error (r, dns_rcode_not_implemented);
  }
  if (q->header.qdcount < 1) {
    vkprintf (2, "%s: No queries (qdcount = %d).\n", __func__, (int) q->header.qdcount);
    return dns_store_error (r, dns_rcode_format_error);
  }
  if (q->QS.qclass != dns_class_in) {
    vkprintf (2, "%s: Unimplemented class (%d).\n", __func__, (int) q->QS.qclass);
    return dns_store_error (r, dns_rcode_not_implemented);
  }
  //dns_record_t RA[DNS_MAX_RESPONSE_RECORDS];
  int name_id, zone_id;
  r->answers = dns_get_records (q->QS.name, q->QS.name_len, q->QS.qtype, &name_id, &zone_id, r->RA, DNS_MAX_RESPONSE_RECORDS);
  if (r->answers == DNS_ERR_BUFFER_OVERFLOW) {
    return dns_store_error (r, dns_rcode_server_failure);
  }

  if (zone_id < 0) {
    return dns_store_error (r, dns_rcode_refused);
  }

  if (r->answers == DNS_ERR_UNKNOWN_NAME) {
    r->rcode = dns_rcode_name_error;
  }

  vkprintf (3, "%s: Find %d answers.\n", __func__, r->answers);
  assert (zone_id >= 0);
  dns_zone_t *z = &Z[zone_id];
  unsigned short *count = &r->header.ancount;
  if (r->answers <= 0) {
    int soa_name_id, soa_zone_id;
    if (z->soa_record) {
      r->answers = dns_get_records (z->origin, z->origin_len, dns_type_soa, &soa_name_id, &soa_zone_id, r->RA, DNS_MAX_RESPONSE_RECORDS);
      if (r->answers < 0) {
        vkprintf (2, "%s: SOA record for zone '%.*s' wasn't found.\n", __func__, z->origin_len, z->origin);
        return dns_store_error (r, dns_rcode_server_failure);
      }
      for (k = 0; k < r->answers; k++) {
        if (r->RA[k].name_len <= q->QS.name_len && ignore_case_equal_strings (r->RA[k].name, q->QS.name + q->QS.name_len - r->RA[k].name_len, r->RA[k].name_len)) {
          r->RA[k].name = q->QS.name + q->QS.name_len - r->RA[k].name_len;
        }
      }
      count = &r->header.nscount;
    }
  }

  if (z->soa_record) {
    r->header.flags |= dns_header_flag_aa;
  }

  if (zone_id >= config_zones && dns_query_check_ip (q) < 0) {
    vkprintf (2, "Refuse query from %s\n", (q->flags & DNS_QUERY_FLAG_IPV6) ? show_ipv6 (q->ipv6) : show_ip (q->ipv4));
    wstat.refused_by_remote_ip_queries++;
    return dns_store_error (r, dns_rcode_refused);
  }

  if (dns_write_iterator_advance (&r->Out, 12) < 0) {
    return -1;
  }
  /* question */
  r->Out.record_start = r->Out.wptr;
  if (dns_write_iterator_store_name (&r->Out, q->QS.name, q->QS.name_len) < 0) { return -2; }
  if (dns_write_iterator_store_ushort (&r->Out, q->QS.qtype) < 0) { return -2; }
  if (dns_write_iterator_store_ushort (&r->Out, q->QS.qclass) < 0) { return -2; }
  r->header.qdcount++;
  /* answers */
  int ns_records = 0;
  for (k = 0; k < r->answers; k++) {
    dns_trie_record_t *R = r->RA[k].R;
    vkprintf (4, "%s: k = %d (cur packet size is %d bytes), R->data_type: %d, R->data_len: %d\n", __func__, k, (int) (r->Out.wptr - r->Out.start), (int) R->data_type, R->data_len);
    if (dns_write_iterator_store_rr (&r->Out, r->RA[k].name, r->RA[k].name_len, R->data_type, dns_get_ttl (R, z)) < 0) {
      return -2;
    }
    unsigned short *w = (unsigned short *) r->Out.wptr;
    if (dns_write_iterator_store_ushort (&r->Out, R->data_len) < 0) { return -2; }
    switch (R->data_type) {
      case dns_type_soa:
        if (dns_write_iterator_soa_rdata (r, (dns_soa_t *) R->data, w) < 0) { return -2; }
        break;
      case dns_type_mx:
        if (dns_write_iterator_mx_rdata (r, (dns_mx_t *) R->data, w) < 0) { return -2; }
        break;
      case dns_type_srv:
        if (dns_write_iterator_srv_rdata (r, (dns_srv_t *) R->data, w) < 0) { return -2; }
        break;
      case dns_type_ns:
        ns_records++;
        if (dns_write_iterator_ns_rdata (r, *((dns_nameserver_en_t **) R->data), w) < 0) { return -2; }
        break;
      case dns_type_ptr:
      case dns_type_cname:
        if (dns_write_iterator_store_name (&r->Out, R->data, R->data_len) < 0) { return -2; }
        *w = htons ((r->Out.wptr - (unsigned char *) w) - 2);
        break;
      default:
        if (dns_write_iterator_store_raw_data (&r->Out, (unsigned char *) R->data, R->data_len) < 0) {
          return -2;
        }
        break;
    }
    (*count)++;
  }
  dns_nameserver_en_t *p, *last = NULL;
  const int authority_section = !ns_records && !r->header.nscount;
  if (authority_section) {
    for (p = z->servers; p != NULL; p = p->next) {
      if (z->origin_len > q->QS.name_len || !ignore_case_equal_strings (z->origin, q->QS.name + q->QS.name_len - z->origin_len, z->origin_len)) {
        continue;
      }
      if (dns_write_iterator_store_rr (&r->Out, q->QS.name + q->QS.name_len - z->origin_len, z->origin_len, dns_type_ns, z->ttl) < 0) {
        return -2;
      }
      unsigned short *w = (unsigned short *) r->Out.wptr;
      if (dns_write_iterator_advance (&r->Out, 2) < 0) { return -2; }
      if (dns_write_iterator_store_name (&r->Out, p->nsdname, p->nsdname_len) < 0) { return -2; }
      *w = htons ((r->Out.wptr - (unsigned char *) w) - 2);
      dns_adresses_find (r, p->nsdname, p->nsdname_len, &p->addrs_nsd);
      r->header.nscount++;
      last = p;
    }
  }
  if (r->additional_records) {
    for (k = 0; k < r->answers; k++) {
      dns_trie_record_t *R = r->RA[k].R;
      if (R->data_type == dns_type_mx) {
        dns_mx_t *M = (dns_mx_t *) R->data;
        if (dns_write_iterator_store_address (r, M->exchange, M->exchange_len, &M->addrs_exchange) < 0) {
          return -2;
        }
      } else if (R->data_type == dns_type_ns) {
        dns_nameserver_en_t *N = *((dns_nameserver_en_t **) R->data);
        if (dns_write_iterator_store_address (r, N->nsdname, N->nsdname_len, &N->addrs_nsd) < 0) { return -2; }
      }
    }
    if (authority_section) {
      for (p = z->servers; p != NULL; p = p->next) {
        if (dns_write_iterator_store_address (r, p->nsdname, p->nsdname_len, &p->addrs_nsd) < 0) {
          return -2;
        }
      }
    }
  }
  //assert (r->additional_records == r->header.arcount);
  if (last && z->servers != last) {
    /* cyclic rotate */
    last->next = z->servers;
    z->servers = z->servers->next;
    last->next->next = NULL;
  }
  if ((q->flags & DNS_QUERY_FLAG_EDNS) && r->Out.avail_out >= 11) {
    unsigned char *p = r->Out.wptr;
    *p++ = 0;
    *p++ = dns_type_opt >> 8;
    *p++ = dns_type_opt & 0xff;
    *p++ = (edns_response_bufsize >> 8) & 0xff;
    *p++ = edns_response_bufsize & 0xff;
    memset (p, 0, 6); //ttl (int), rdlen (short)
    r->Out.wptr += 11;
    r->Out.avail_out -= 11;
    r->header.arcount++;
  }
  return dns_store_header (r);
}

void dns_query_set_ip (dns_query_t *q, int af, void *ip) {
  if (af == AF_INET) {
    memcpy (&q->ipv4, ip, 4);
    set_4in6 (q->ipv6, q->ipv4);
    vkprintf (3, "%s: %s\n", __func__, show_ip (q->ipv4));
  } else {
    assert (af == AF_INET6);
    memcpy (q->ipv6, ip, 16);
    q->flags |= DNS_QUERY_FLAG_IPV6;
    vkprintf (3, "%s: %s\n", __func__, show_ipv6 (q->ipv6));
  }
}

int dns_query_act (dns_query_t *q, dns_response_t *r, unsigned char *out, int olen) {
  r->truncated = 0;
  r->q = q;
  int res = dns_gen_response (r, out, olen);
  if (res == -2) {
    r->Out.wptr = r->Out.record_start;
    r->header.flags |= dns_header_flag_tc;
    r->truncated = 1;
    res = dns_store_header (r);
  }
  return res;
}

static int append_origin_to_name (char *name, int name_len) {
  char *origin = Z[cur_zone_id].origin;
  assert (origin);
  int l = Z[cur_zone_id].origin_len;
  char *s = alloca (name_len + l + 1);
  memcpy (s, name, name_len);
  s[name_len] = '.';
  memcpy (s + name_len + 1, origin, l);
  l += name_len + 1;
  if (name[0] == '@' && name_len == 1) {
    s += 2;
    l -= 2;
  }
  return get_name_f (s, l, 1);
}

static int dns_change_zone (struct lev_dns_change_zone *E) {
  vkprintf (4, "%s: origin = '%.*s'\n", __func__, E->type & 0xff, E->origin);
  const int l = E->type & 0xff;
  for (cur_zone_id = 0; cur_zone_id < zones; cur_zone_id++) {
    dns_zone_t *z = Z + cur_zone_id;
    if (z->origin_len == l && !memcmp (z->origin, E->origin, l)) {
      return 0;
    }
  }
  if (cur_zone_id == zones) {
    dns_zone_t *z = Z + cur_zone_id;
    if (zones >= DNS_MAX_ZONES) {
      kprintf ("Too many zones. Try to increase DNS_MAX_ZONES(%d) constant.\n", DNS_MAX_ZONES);
      return -1;
    }
    z->origin = zmalloc (l);
    memcpy (z->origin, E->origin, l);
    z->origin_len = l;
    z->ns_servers = 0;
    z->ttl = DNS_DEFAULT_TTL;
    z->records = 0;
    z->soa_record = 0;
    /* insert into the trie edges with undefined zone_id */
    char *p = strchr (z->origin, '.');
    if (p) {
      cur_zone_id = -1;
      get_name_f (p + 1, strlen (p + 1), 1);
      cur_zone_id = zones;
    }
    zones++;
  }
  return 0;
}

static int dns_delete_records (struct lev_dns_delete_records *E) {
  //int l = E->type & 0xff;
  vkprintf (4, "%s: name = '%.*s', type = %d\n", __func__, E->type & 0xff, E->name, E->qtype);
  int name_id = append_origin_to_name (E->name, E->type & 0xff);
  return dns_name_delete_records (name_id, E->qtype);
}

static int dns_record_a (struct lev_dns_record_a *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->name);
  int name_id = append_origin_to_name (E->name, E->type & 0xff);
  return dns_name_add_record (name_id, dns_type_a, E->ttl, &E->ipv4, 4);
}

static int dns_record_aaaa (struct lev_dns_record_aaaa *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->name);
  int name_id = append_origin_to_name (E->name, E->type & 0xff);
  return dns_name_add_record (name_id, dns_type_aaaa, E->ttl, E->ipv6, 16);
}

static int dns_record_ptr (struct lev_dns_record_ptr *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->name);
  int name_len = E->type & 0xff;
  int name_id = append_origin_to_name (E->name, name_len);
  return dns_name_add_record (name_id, dns_type_ptr, Z[cur_zone_id].ttl, E->data + name_len, E->data_len);
}

static int dns_record_cname (struct lev_dns_record_cname *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->name);
  int name_len = E->type & 0xff;
  int name_id = append_origin_to_name (E->name, name_len);
  return dns_name_add_record (name_id, dns_type_cname, Z[cur_zone_id].ttl, E->alias + name_len, E->alias_len);
}

static int dns_record_txt (struct lev_dns_record_txt *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->name);
  int name_len = E->type & 0xff;
  int name_id = append_origin_to_name (E->name, name_len);
  return dns_name_add_record (name_id, dns_type_txt, E->ttl, E->text + name_len, E->text_len);
};

static int dns_record_ns (struct lev_dns_record_ns *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->nsdname);
  const int l = E->type & 0xff;
  dns_zone_t *z = &Z[cur_zone_id];
  dns_nameserver_en_t *p = zmalloc (sizeof (dns_nameserver_en_t) + l);
  dns_addresses_reset (&p->addrs_nsd);
  memcpy (p->nsdname, E->nsdname, l);
  p->nsdname_len = l;
  p->next = NULL;
  if (z->servers) {
    dns_nameserver_en_t *last = z->servers;
    while (1) {
      if (last->next == NULL) {
        break;
      }
      last = last->next;
    }
    last->next = p;
  } else {
    z->servers = p;
  }

  z->ns_servers++;

  int name_id = append_origin_to_name ("@", 1);
  return dns_name_add_record (name_id, dns_type_ns, z->ttl, &p, sizeof (void *));
}

static int dns_record_soa (struct lev_dns_record_soa *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->name);
  int name_len = E->type & 0xff;
  int name_id = append_origin_to_name (E->name, name_len);
  int data_len = sizeof (dns_soa_t) + E->mname_len + E->rname_len;
  dns_soa_t *S = alloca (data_len);
  S->mname_len = E->mname_len;
  S->rname_len = E->rname_len;
  memcpy (S->mname, E->mname + name_len, E->mname_len);
  memcpy (S->rname + E->mname_len, E->rname + name_len + E->mname_len, E->rname_len);
  S->data[0] = E->serial;
  S->data[1] = E->refresh;
  S->data[2] = E->retry;
  S->data[3] = E->expire;
  S->data[4] = E->negative_cache_ttl;
  dns_zone_t *z = &Z[cur_zone_id];
  return dns_name_add_record (name_id, dns_type_soa, z->ttl, S, data_len);
}

static int dns_record_srv (struct lev_dns_record_srv *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->name);
  int name_len = E->type & 0xff;
  int name_id = append_origin_to_name (E->name, name_len);
  int data_len = sizeof (dns_srv_t) + E->target_len;
  dns_srv_t *S = alloca (data_len);
  S->target_len = E->target_len;
  S->priority = E->priority;
  S->weight = E->weight;
  S->port = E->port;
  memcpy (S->target, E->target + name_len, E->target_len);
  return dns_name_add_record (name_id, dns_type_srv, E->ttl, S, data_len);
}

static int dns_record_mx (struct lev_dns_record_mx *E) {
  vkprintf (4, "%s: name = '%.*s'\n", __func__, E->type & 0xff, E->name);
  int name_len = E->type & 0xff;
  int name_id = append_origin_to_name (E->name, name_len);
  int sz = sizeof (dns_mx_t) + E->exchange_len;
  dns_mx_t *S = alloca (sz);
  S->preference = E->preference;
  S->exchange_len = E->exchange_len;
  dns_addresses_reset (&S->addrs_exchange);
  memcpy (S->exchange, E->exchange + name_len, E->exchange_len);
  dns_zone_t *z = &Z[cur_zone_id];
  return dns_name_add_record (name_id, dns_type_mx, z->ttl, S, sz);
}

/* for converting from network byte order to host byte order */
static void array_reverse (unsigned char *s, int l) {
  int i = 0, j = l - 1;
  while (i < j) {
    unsigned char w = s[i];
    s[i] = s[j];
    s[j] = w;
    i++;
    j--;
  }
}

static int parse_time (const char *s, int *t) {
  unsigned value, multiplier = 1;
  char c;
  const int n = sscanf (s, "%u%c", &value, &c);
  if (n < 1) {
    return -1;
  }
  if (n == 2) {
    switch (tolower (c)) {
      case 'd': multiplier = 24 * 3600; break;
      case 'h': multiplier = 3600; break;
      case 'm': multiplier = 60; break;
      case 'w': multiplier = 7 * 24 * 3600; break;
      default:
        kprintf ("%s(\"%s\"): unknown suffix\n", __func__, s);
        return -1;
    }
  }
  *t = multiplier * value;
  return 0;
}

#define DNS_STREAM_BUFFSIZE 4096

typedef struct {
  char buff[DNS_STREAM_BUFFSIZE];
  char tokens_buff[DNS_STREAM_BUFFSIZE];
  char generate_buff[DNS_STREAM_BUFFSIZE];
  FILE *f;
  const char *filename;
  int errors;
  int line;
  int cur_generate_idx;
  int last_generate_idx;
} dns_config_stream_t;

static int dns_config_stream_init (dns_config_stream_t *self, const char *filename) {
  self->filename = filename;
  self->f = fopen (filename, "r");
  if (self->f == NULL) {
    kprintf ("%s: fopen (\"%s\", \"r\") fail. %m\n", __func__, self->filename);
    return -1;
  }
  self->last_generate_idx = INT_MIN;
  self->line = self->cur_generate_idx = 0;
  self->errors = 0;
  return 0;
}

static void dns_config_stream_close (dns_config_stream_t *self) {
  if (self->f) {
    fclose (self->f);
    self->f = NULL;
  }
}

static void dns_config_stream_error (dns_config_stream_t *self, const char *funcname) {
  fprintf (stderr, "File: %s, Line: %d, Function: %s\n", self->filename, self->line, funcname);
  fprintf (stderr, "%s\n", self->buff);
  self->errors++;
}

static int dns_config_stream_tokenize (dns_config_stream_t *self, char **tokens, int max_tokens, int *n) {
  char *p;
  do {
    if (self->cur_generate_idx <= self->last_generate_idx) {
      char s[16];
      int ls = snprintf (s, sizeof (s), "%d", self->cur_generate_idx);
      assert (ls < sizeof (s));
      int i = 0;
      for (p = self->generate_buff; *p; p++) {
        int l = ls;
        char *z = s;
        char t[16];
        if (*p == '$') {
          if (p[1] == '{') {
            errno = 0;
            int x = strtol (p + 2, &p, 10);
            if (errno) {
              return -1;
            }
            if (*p != '}') {
              return -1;
            }
            l = snprintf (t, sizeof (t), "%d", self->cur_generate_idx + x);
            assert (l < sizeof (t));
            z = t;
          }
          if (i + l >= DNS_STREAM_BUFFSIZE) {
            return -1;
          }
          strcpy (self->buff + i, z);
          i += l;
        } else {
          if (i >= DNS_STREAM_BUFFSIZE) {
            return -1;
          }
          self->buff[i++] = *p;
        }
      }
      if (i >= DNS_STREAM_BUFFSIZE) {
        return -1;
      }
      self->buff[i] = 0;
      self->cur_generate_idx++;
    } else {
      if (fgets (self->buff, sizeof (self->buff), self->f) == NULL) {
        *n = -1;
        return 0;
      }
      self->buff[sizeof (self->buff) - 1] = 0;
      self->line++;
    }
    vkprintf (4, "%s\n", self->buff);
    int l = strlen (self->buff);
    if (l >= sizeof (self->buff) - 1) {
      strcpy (self->buff + 80, "...");
      dns_config_stream_error (self, __func__);
      kprintf ("Line is too long.\n");
      return -1;
    }
    if (l > 0 && self->buff[l-1] == '\n') {
      self->buff[--l] = 0;
    }
    /* remove comments */
    p = self->buff;
    int quoted = 0;
    for (p = self->buff; *p; p++) {
      if (quoted) {
        if (*p == '"') {
          quoted = 0;
        }
      } else {
        if (*p == '"') {
          quoted = 1;
        } else if (*p == ';') {
          *p = 0;
          break;
        }
      }
    }
    strcpy (self->tokens_buff, self->buff);
    *n = 0;
    p = self->tokens_buff;
    while (*p) {
      while (isspace (*p)) {
        p++;
      }
      if (!(*p)) {
        break;
      }
      if (*n >= max_tokens) {
        dns_config_stream_error (self, __func__);
        kprintf ("Too many tokens.\n");
        return -1;
      }
      tokens[(*n)++] = p;
      if (*p == '"') {
        tokens[(*n)-1]++;
        char *q = strchr (p + 1, '"');
        if (q == NULL) {
          dns_config_stream_error (self, __func__);
          kprintf ("Unclosed double quote.\n");
          return -1;
        }
        *q = 0;
        p = q + 1;
      } else {
        char *q = p + 1;
        while (*q && !isspace (*q)) {
          q++;
        }
        if (!(*q)) {
          break;
        }
        *q = 0;
        p = q + 1;
      }
    }

    if ((*n) >= 2 && !strcmp (tokens[0], "$GENERATE")) {
      int i;
      if (sscanf (tokens[1], "%d-%d", &self->cur_generate_idx, &self->last_generate_idx) != 2) {
        dns_config_stream_error (self, __func__);
        kprintf ("Couldn't parse generate range.\n");
        return -1;
      }
      p = self->generate_buff;
      for (i = 2; i < (*n); i++) {
        if (i > 2) {
          *p++ = '\t';
        }
        strcpy (p, tokens[i]);
        p += strlen (tokens[i]);
      }
      vkprintf (4, "%s: generate_buff = '%s'\n", __func__, self->generate_buff);
      return dns_config_stream_tokenize (self, tokens, max_tokens, n);
    }
  } while (!(*n));
  return 0;
}

#define MAX_TOKENS 16
#define EXPAND_BUFF_SIZE 65536

static char *expand_name (char *s) {
  static char *buf = NULL, *wptr = NULL;
  if (s == NULL) {
    if (buf) {
      free (buf);
      buf = wptr = NULL;
    }
    return NULL;
  }
  if (buf == NULL) {
    wptr = buf = malloc (EXPAND_BUFF_SIZE);
    assert (buf);
  }
  int m = strlen (s);
  if (m > 0 && s[m-1] == '.') {
    s[--m] = 0;
    return s;
  }
  char *origin = Z[cur_zone_id].origin;
  assert (origin);
  int l = m + 1 + Z[cur_zone_id].origin_len + 1;
  assert (l < EXPAND_BUFF_SIZE);
  if (wptr + l > buf + EXPAND_BUFF_SIZE) {
    wptr = buf;
  }
  char *z = wptr;
  memcpy (wptr, s, m);
  wptr += m;
  *wptr++ = '.';
  memcpy (wptr, origin, Z[cur_zone_id].origin_len);
  wptr += Z[cur_zone_id].origin_len;
  *wptr++ = 0;
  vkprintf (4, "%s: '%s' - name after expanding.\n", __func__, z);
  return z;
}

static int rstrip (char *s, char ch) {
  int l = strlen (s), r = 0;
  while (l > 0 && s[l-1] == ch) {
    s[--l] = 0;
    r++;
  }
  return r;
}

/******************** DNS binlog ********************/
static int dns_replay_logevent (struct lev_generic *E, int size);

int init_dns_data (int schema) {
  replay_logevent = dns_replay_logevent;
  return 0;
}

int dns_le_start (struct lev_start *E) {
  if (E->schema_id != DNS_SCHEMA_V1) {
    kprintf ("LEV_START schema_id isn't to DNS_SCHEMA_V1.\n");
    return -1;
  }
  if (E->extra_bytes) {
    kprintf ("LEV_START extra_bytes isn't equal to 0.\n");
    return -1;
  }
  log_split_min = E->split_min;
  log_split_max = E->split_max;
  log_split_mod = E->split_mod;
  assert (log_split_mod > 0 && log_split_min >= 0 && log_split_min + 1 ==  log_split_max && log_split_max <= log_split_mod);

  return 0;
}

static int dns_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
    case LEV_START:
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      return dns_le_start ((struct lev_start *) E) >= 0 ? s : -1;
    case LEV_NOOP:
    case LEV_TIMESTAMP:
    case LEV_CRC32:
    case LEV_ROTATE_FROM:
    case LEV_ROTATE_TO:
    case LEV_TAG:
      return default_replay_logevent (E, size);
    case LEV_DNS_RECORD_A ... LEV_DNS_RECORD_A + 0xff:
      s = sizeof (struct lev_dns_record_a) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      if (dns_record_a ((struct lev_dns_record_a *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_AAAA ... LEV_DNS_RECORD_AAAA + 0xff:
      s = sizeof (struct lev_dns_record_aaaa) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      if (dns_record_aaaa ((struct lev_dns_record_aaaa *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_PTR ... LEV_DNS_RECORD_PTR + 0xff:
      s = sizeof (struct lev_dns_record_ptr) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_dns_record_ptr *) E)->data_len;
      if (size < s) {
        return -2;
      }
      if (dns_record_ptr ((struct lev_dns_record_ptr *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_NS ... LEV_DNS_RECORD_NS + 0xff:
      s = sizeof (struct lev_dns_record_ns) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      if (dns_record_ns ((struct lev_dns_record_ns *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_SOA ... LEV_DNS_RECORD_SOA + 0xff:
      s = sizeof (struct lev_dns_record_soa) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_dns_record_soa *) E)->mname_len + ((struct lev_dns_record_soa *) E)->rname_len;
      if (size < s) {
        return -2;
      }
      if (dns_record_soa ((struct lev_dns_record_soa *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_TXT ... LEV_DNS_RECORD_TXT + 0xff:
      s = sizeof (struct lev_dns_record_txt) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_dns_record_txt *) E)->text_len;
      if (size < s) {
        return -2;
      }
      if (dns_record_txt ((struct lev_dns_record_txt *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_MX ... LEV_DNS_RECORD_MX + 0xff:
      s = sizeof (struct lev_dns_record_mx) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_dns_record_mx *) E)->exchange_len;
      if (size < s) {
        return -2;
      }
      if (dns_record_mx ((struct lev_dns_record_mx *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_CNAME ... LEV_DNS_RECORD_CNAME + 0xff:
      s = sizeof (struct lev_dns_record_cname) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_dns_record_cname *) E)->alias_len;
      if (size < s) {
        return -2;
      }
      if (dns_record_cname ((struct lev_dns_record_cname *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_SRV ... LEV_DNS_RECORD_SRV + 0xff:
      s = sizeof (struct lev_dns_record_srv) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      s += ((struct lev_dns_record_srv *) E)->target_len;
      if (size < s) {
        return -2;
      }
      if (dns_record_srv ((struct lev_dns_record_srv *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_CHANGE_ZONE ... LEV_DNS_CHANGE_ZONE + 0xff:
      s = sizeof (struct lev_dns_change_zone) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      if (dns_change_zone ((struct lev_dns_change_zone *) E) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_DELETE_RECORDS ... LEV_DNS_DELETE_RECORDS + 0xff:
      s = sizeof (struct lev_dns_delete_records) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      if (dns_delete_records ((struct lev_dns_delete_records *) E) < 0) {
        return -1;
      }
      return s;
  }
  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());
  return -3;
}

void *dns_alloc_log_event (int type, int bytes, int arg1) {
  struct lev_generic *E;
  if (dns_convert_config_to_binlog) {
    E = alloc_log_event (type, bytes, arg1);
    assert (E);
  } else {
    static char buf[65536];
    assert (bytes <= sizeof (buf));
    E = (struct lev_generic *) buf;
    E->type = type;
    E->a = arg1;
  }
  return E;
}

static int config_load (const char *filename, int exit_after_first_error, int depth) {
  int err = -1;
  dns_config_stream_t S;
  if (dns_config_stream_init (&S, filename) < 0) {
    return -1;
  }
  char *tokens[MAX_TOKENS];
  while (1) {
    int n = 0;
    if (dns_config_stream_tokenize (&S, tokens, MAX_TOKENS, &n) < 0) {
      goto exit;
    }
    if (n < 0) { /* end of config file */
      break;
    }

    if (n == 2 && !strcmp (tokens[0], "$BINLOG")) {
      if (include_binlog_name) {
        dns_config_stream_error (&S, __func__);
        kprintf ("More than one $BINLOG macro.\n");
        goto exit;
      }
      include_binlog_name = zstrdup (tokens[1]);
      continue;
    }

    if (n == 2 && !strcmp (tokens[0], "$BINLOG_ALLOW_QUERY")) {
      char *p = strchr (tokens[1], '/');
      if (p == NULL) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Expected '/' in network address '%s'.\n", tokens[1]);
        if (exit_after_first_error) { goto exit; }
        continue;
      }
      if (binlog_allow_query_networks >= DNS_MAX_BINLOG_ALLOW_QUERY_NETWORKS || binlog_allow_query_networks6 >= DNS_MAX_BINLOG_ALLOW_QUERY_NETWORKS) {
        kprintf ("%s: Binlog allow queries network array overflow. Try to increase DNS_MAX_BINLOG_ALLOW_QUERY_NETWORKS(%d) define.\n", __func__, DNS_MAX_BINLOG_ALLOW_QUERY_NETWORKS);
      }
      *p = 0;
      int prefix_bits = -1;
      if (sscanf (p + 1, "%d", &prefix_bits) != 1 || prefix_bits <= 0) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Illegal mask length (%s)\n", p);
        if (exit_after_first_error) { goto exit; }
        continue;
      }
      if (prefix_bits <= 32 && 1 == inet_pton (AF_INET, tokens[1], &BAQN[binlog_allow_query_networks].ipv4)) {
        BAQN[binlog_allow_query_networks].ipv4 = ntohl (BAQN[binlog_allow_query_networks].ipv4);
        BAQN[binlog_allow_query_networks].mask = ~((1 << (32 - prefix_bits)) - 1);
        BAQN[binlog_allow_query_networks].prefix_bits = prefix_bits;
        binlog_allow_query_networks++;
      } else if (prefix_bits <= 128 && 1 == inet_pton (AF_INET6, tokens[1], BAQN6[binlog_allow_query_networks6].ipv6)) {
        int k;
        BAQN6[binlog_allow_query_networks6].prefix_bits = prefix_bits;
        memset (BAQN6[binlog_allow_query_networks6].mask, 0, 16);
        for (k = 0; k < prefix_bits; k++) {
          BAQN6[binlog_allow_query_networks6].mask[k / 8] |= 1 << (7 - (k % 8));
        }
        fprintf (stderr, "%s: %s\n", __func__, show_ipv6 (BAQN6[binlog_allow_query_networks6].mask));
        binlog_allow_query_networks6++;
      } else {
        dns_config_stream_error (&S, __func__);
        kprintf ("Fail to parse network '%s/%d'.\n", tokens[1], prefix_bits);
        if (exit_after_first_error) { goto exit; }
      }
      continue;
    }

    if (n == 3 && !strcmp (tokens[0], "$INCLUDE")) {
      if (depth == 1) {
        dns_config_stream_error (&S, __func__);
        kprintf ("$INCLUDE macro could be only in main config file.\n");
        goto exit;
      }
      char *zone = tokens[1], *zone_filename = tokens[2];
      rstrip (zone, '.');
      const int zone_len = strlen (zone);
      struct lev_dns_change_zone *E = dns_alloc_log_event (LEV_DNS_CHANGE_ZONE + zone_len, sizeof (struct lev_dns_change_zone) + zone_len, 0);
      memcpy (E->origin, zone, zone_len);
      if (dns_change_zone (E) < 0) {
        dns_config_stream_error (&S, __func__);
        goto exit;
      }
      if (config_load (zone_filename, exit_after_first_error, depth + 1) < 0) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Loading config for the file '%s' with origin '%s' failed.\n", zone_filename, zone);
        if (exit_after_first_error) { goto exit; }
      }
      dns_zone_t *z = Z + cur_zone_id;
      vkprintf (2, "%s: load %d records for zone '%s'.\n", __func__, z->records, z->origin);
      continue;
    }

    if (!zones) {
      dns_config_stream_error (&S, __func__);
      kprintf ("Expected $INCLUDE <zone> <filename>\n");
      goto exit;
    }

    if (n == 2 && !strcmp (tokens[0], "$TTL")) {
      dns_zone_t *z = &Z[cur_zone_id];
      if (parse_time (tokens[1], (int *) &z->ttl) < 0) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Fail to parse TTL value ('%s').\n", tokens[1]);
        goto exit;
      }
      if (z->records > 0) {
        dns_config_stream_error (&S, __func__);
        kprintf ("$TTL define must be declare before zone's first record.\n");
        goto exit;
      }
      continue;
    }

/*
    if (n == 2 && !strcmp (tokens[0], "$ORIGIN")) {
      change_origin (tokens[1]);
      continue;
    }
*/

    if (n == 6 && !strcmp (tokens[1], "IN") && !strcmp (tokens[2], "SOA") && !strcmp (tokens[5], "(")) {
      if (strcmp (tokens[0], "@")) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Expected SOA record for origin(@), but name '%s' found.\n", tokens[0]);
        goto exit;
      }
      tokens[3] = expand_name (tokens[3]);
      tokens[4] = expand_name (tokens[4]);
      const int name_len = strlen (tokens[0]), mname_len = strlen (tokens[3]), rname_len = strlen (tokens[4]);
      if (name_len > 255) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Name too long more 255 characters.\n");
        goto exit;
      }
      struct lev_dns_record_soa *E = dns_alloc_log_event (LEV_DNS_RECORD_SOA + name_len, sizeof (struct lev_dns_record_soa) + name_len + mname_len + rname_len, 0);
      E->mname_len = mname_len;
      E->rname_len = rname_len;
      memcpy (E->name, tokens[0], name_len);
      memcpy (E->mname + name_len, tokens[3], mname_len);
      memcpy (E->rname + name_len + mname_len , tokens[4], rname_len);
      if (dns_config_stream_tokenize (&S, tokens, MAX_TOKENS, &n) < 0 || n != 1 || sscanf (tokens[0], "%d", &E->serial) != 1) {
        kprintf ("Fail to parse Serial in SOA record.\n");
        goto exit;
      }
      if (dns_config_stream_tokenize (&S, tokens, MAX_TOKENS, &n) < 0 || n != 1 || parse_time (tokens[0], &E->refresh) < 0) {
        kprintf ("Fail to parse Refresh in SOA record.\n");
        goto exit;
      }
      if (dns_config_stream_tokenize (&S, tokens, MAX_TOKENS, &n) < 0 || n != 1 || parse_time (tokens[0], &E->retry) < 0) {
        kprintf ("Fail to parse Retry in SOA record.\n");
        goto exit;
      }
      if (dns_config_stream_tokenize (&S, tokens, MAX_TOKENS, &n) < 0 || n != 1 || parse_time (tokens[0], &E->expire) < 0) {
        kprintf ("Fail to parse Expire in SOA record.\n");
        goto exit;
      }
      if (dns_config_stream_tokenize (&S, tokens, MAX_TOKENS, &n) < 0 || n != 1 || parse_time (tokens[0], &E->negative_cache_ttl) < 0) {
        kprintf ("Fail to parse Negative Cache TTL in SOA record.\n");
        goto exit;
      }
      if (dns_config_stream_tokenize (&S, tokens, MAX_TOKENS, &n) < 0 || n != 1 || strcmp (tokens[0], ")")) {
        kprintf ("Fail to parse ')' in SOA record.\n");
        goto exit;
      }
      if (dns_record_soa (E) < 0) {
        dns_config_stream_error (&S, __func__);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      continue;
    }

    if (n == 3 && !strcmp (tokens[0], "IN") && !strcmp (tokens[1], "NS")) {
      tokens[2] = expand_name (tokens[2]);
      struct lev_dns_record_ns *E = dns_alloc_log_event (LEV_DNS_RECORD_NS + strlen (tokens[2]),  sizeof (struct lev_dns_record_ns) + strlen (tokens[2]), 0);
      memcpy (E->nsdname, tokens[2], strlen (tokens[2]));
      if (dns_record_ns (E) < 0) {
        dns_config_stream_error (&S, __func__);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      continue;
    }

    int o = -1;
    if (n == 4 && !strcmp (tokens[0], "IN") && !strcmp (tokens[1], "MX")) {
      o = 0;
    }
    if (n == 5 && !strcmp (tokens[1], "IN") && !strcmp (tokens[2], "MX")) {
      o = 1;
    }

    if (o >= 0) {
      char *exchange = expand_name (tokens[3 + o]);
      char *name = o ? tokens[0] : "@";
      int name_len = strlen (name);
      if (name_len > 255) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Name too long more 255 characters.\n");
        goto exit;
      }
      int exchange_len = strlen (exchange);
      if (exchange_len > 255) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Exchange too long more 255 characters.\n");
        goto exit;
      }
      int preference;
      if (sscanf (tokens[2+o], "%d", &preference) != 1 || preference < 0 || preference > 0xffff) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Fail to parse preference '%s' in MX record.\n", tokens[2+o]);
        goto exit;
      }
      struct lev_dns_record_mx *E = dns_alloc_log_event (LEV_DNS_RECORD_MX + name_len, sizeof (struct lev_dns_record_mx) + name_len + exchange_len, 0);
      E->preference = preference;
      E->exchange_len = exchange_len;
      memcpy (E->name, name, name_len);
      memcpy (E->exchange + name_len, exchange, exchange_len);
      if (dns_record_mx (E) < 0) {
        dns_config_stream_error (&S, __func__);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      continue;
    }

    if (n == 4 || n == 5) {
      int record_ttl, offset_after_ttl = 0;
      if (n == 5) {
        offset_after_ttl = 1;
        if (parse_time (tokens[1], &record_ttl) < 0) {
          dns_config_stream_error (&S, __func__);
          kprintf ("Fail to parse TTL value ('%s').\n", tokens[1]);
          goto exit;
        }
      } else {
        record_ttl = Z[cur_zone_id].ttl;
      }
      char *name = tokens[0], *class = tokens[offset_after_ttl+1], *type = tokens[offset_after_ttl + 2], *data = tokens[offset_after_ttl + 3];
      rstrip (name, '.');
      const int name_len = strlen (name);
      if (name_len > 255) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Name too long more 255 characters.\n");
        goto exit;
      }
      if (strcmp (class, "IN")) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Class isn't 'IN'.\n");
        goto exit;
      }
      unsigned char ip[16];
      int data_len = -1, data_type = -1;
      if (!strcmp (type, "A")) {
        if (1 != inet_pton (AF_INET, data, ip)) {
          dns_config_stream_error (&S, __func__);
          kprintf ("inet_pton (AF_INET, \"%s\") fail.\n", data);
          goto exit;
        }
        data_type = dns_type_a;
        data_len = 4;
        array_reverse (ip, data_len);
      } else if (!strcmp (type, "AAAA")) {
        if (1 != inet_pton (AF_INET6, data, ip)) {
          dns_config_stream_error (&S, __func__);
          kprintf ("inet_pton (AF_INET6, \"%s\") fail.\n", data);
          goto exit;
        }
        data_type = dns_type_aaaa;
        data_len = 16;
        array_reverse (ip, data_len);
      } else if (!strcmp (type, "PTR")) {
        data = expand_name (data);
        data_type = dns_type_ptr;
        data_len = strlen (data);
      } else if (!strcmp (type, "TXT")) {
        data_type = dns_type_txt;
        data_len = strlen (data);
        if (data_len > 255) {
          dns_config_stream_error (&S, __func__);
          kprintf ("Too long TXT record (more than 255 characters).\n");
          goto exit;
        }
        char *txt = alloca (data_len + 1);
        txt[0] = data_len;
        memcpy (txt + 1, data, data_len);
        data_len++;
        array_reverse ((unsigned char *) txt, data_len); /* hack */
        data = txt;
      } else if (!strcmp (type, "CNAME")) {
        data = expand_name (data);
        data_len = strlen (data);
        data_type = dns_type_cname;
        if (data_len > 255) {
          dns_config_stream_error (&S, __func__);
          kprintf ("Too long CNAME record (more than 255 characters).\n");
          goto exit;
        }
      }
      if (data_len < 0) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Unknown type. (name: '%s', class: '%s', type: '%s')\n", name, class, type);
        goto exit;
      }
      int res = -1;
      if (data_type == dns_type_a) {
        struct lev_dns_record_a *E = dns_alloc_log_event (LEV_DNS_RECORD_A + name_len, sizeof (struct lev_dns_record_a) + name_len, record_ttl);
        memcpy (&E->ipv4, ip, 4);
        memcpy (E->name, name, strlen (name));
        res = dns_record_a (E);
      } else if (data_type == dns_type_aaaa) {
        struct lev_dns_record_aaaa *E = dns_alloc_log_event (LEV_DNS_RECORD_AAAA + name_len, sizeof (struct lev_dns_record_aaaa) + name_len, record_ttl);
        memcpy (&E->ipv6, ip, 16);
        memcpy (E->name, name, name_len);
        res = dns_record_aaaa (E);
      } else if (data_type == dns_type_ptr) {
        struct lev_dns_record_ptr *E = dns_alloc_log_event (LEV_DNS_RECORD_PTR + name_len, sizeof (struct lev_dns_record_ptr) + name_len + data_len, data_len);
        memcpy (E->name, name, name_len);
        memcpy (E->data + name_len, data, data_len);
        res = dns_record_ptr (E);
      } else if (data_type == dns_type_cname) {
        struct lev_dns_record_cname *E = dns_alloc_log_event (LEV_DNS_RECORD_CNAME + name_len, sizeof (struct lev_dns_record_cname) + name_len + data_len, data_len);
        memcpy (E->name, name, name_len);
        memcpy (E->alias + name_len, data, data_len);
        res = dns_record_cname (E);
      } else {
        assert (data_type == dns_type_txt);
        struct lev_dns_record_txt *E = dns_alloc_log_event (LEV_DNS_RECORD_TXT + name_len, sizeof (struct lev_dns_record_txt) + name_len + data_len, record_ttl);
        E->text_len = data_len;
        memcpy (E->name, name, name_len);
        memcpy (E->text + name_len, data, data_len);
        res = dns_record_txt (E);
      }
      if (res < 0) {
        dns_config_stream_error (&S, __func__);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      continue;
    }

    if (n == 8 && !strcmp (tokens[2], "IN") && !strcmp (tokens[3], "SRV")) {
      char *name = tokens[0], *target = tokens[7];
      const int name_len = strlen (name);
      if (name_len > 255) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Name too long more 255 characters.\n");
        goto exit;
      }
      int record_ttl = DNS_DEFAULT_TTL;
      if (parse_time (tokens[1], &record_ttl) < 0) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Fail to parse TTL value ('%s').\n", tokens[1]);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      int priority, weight, port;
      if (sscanf (tokens[4], "%d", &priority) != 1 || priority < 0 || priority > 0xffff) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Fail to parse PRIORITY value ('%s').\n", tokens[4]);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      if (sscanf (tokens[5], "%d", &weight) != 1 || weight < 0 || weight > 0xffff) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Fail to parse WEIGHT value ('%s').\n", tokens[5]);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      if (sscanf (tokens[6], "%d", &port) != 1 || port < 0 || port > 0xffff) {
        dns_config_stream_error (&S, __func__);
        kprintf ("Fail to parse WEIGHT value ('%s').\n", tokens[6]);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      int target_len = strlen (target);
      struct lev_dns_record_srv *E = dns_alloc_log_event (LEV_DNS_RECORD_SRV + name_len, sizeof (struct lev_dns_record_srv) + name_len + target_len, record_ttl);
      E->priority = priority;
      E->weight = weight;
      E->port = port;
      E->target_len = target_len;
      memcpy (E->name, name, name_len);
      memcpy (E->target + name_len, target, target_len);
      if (dns_record_srv (E) < 0) {
        dns_config_stream_error (&S, __func__);
        if (exit_after_first_error) {
          goto exit;
        }
      }
      continue;
    }

    dns_config_stream_error (&S, __func__);
    kprintf ("Parsing error (unimplemented config pattern(?)).\n");
    if (exit_after_first_error) {
      goto exit;
    }
  }

  err = S.errors ? -1 : 0;

  exit:

  dns_config_stream_close (&S);
  return err;
}

int dns_config_load (const char *filename, int exit_after_first_error, const char *output_binlog_name) {
  if (output_binlog_name) {
    char a[PATH_MAX];
    assert (snprintf (a, PATH_MAX, "%s.bin", output_binlog_name) < PATH_MAX);
    int fd = open (a, O_CREAT | O_WRONLY | O_EXCL, 0660);
    if (fd < 0) {
      kprintf ("open ('%s', O_CREAT | O_WRONLY | O_EXCL, 0660) failed. %m\n", a);
      exit (1);
    }
    struct lev_start *E = alloca (24);
    E->type = LEV_START;
    E->schema_id = DNS_SCHEMA_V1;
    E->extra_bytes = 0;
    E->split_mod = 1;
    E->split_min = 0;
    E->split_max = 1;
    assert (write (fd, E, 24) == 24);
    assert (fsync (fd) >= 0);
    assert (close (fd) >= 0);
    if (engine_preload_filelist (output_binlog_name, NULL) < 0) {
      kprintf ("cannot open binlog files for %s\n", output_binlog_name);
      exit (1);
    }
    Binlog = open_binlog (engine_replica, 0);
    if (!Binlog) {
      kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, 0ll);
      exit (1);
    }
    binlogname = Binlog->info->filename;
    clear_log ();
    init_log_data (0, 0, 0);
    vkprintf (1, "replay log events started\n");
    assert (replay_log (0, 1) >= 0);
    vkprintf (1, "replay log events finished\n");
    clear_read_log();
    clear_write_log ();
    assert (append_to_binlog (Binlog) == log_readto_pos);
    dns_convert_config_to_binlog = 1;
  }
  include_binlog_name = NULL;
  dns_record_hash_init ();
  int res = config_load (filename, exit_after_first_error, 0);
  dns_record_hash_free ();
  expand_name (0); //free cyclic buffer
  config_zones = zones;
  if (output_binlog_name) {
    if (include_binlog_name) {
      kprintf ("ERROR: Config contains $BINLOG macro. This feature is forbidden in writing binlog mode.\n");
      return -1;
    }
    flush_binlog_last ();
    sync_binlog (2);
    close_binlog (Binlog, 1);
    Binlog = NULL;
    binlogname = NULL;
  }

  if (include_binlog_name && !binlog_allow_query_networks && !binlog_allow_query_networks6) {
    kprintf ("Ignore '$BINLOG %s' macro since there isn't any '$BINLOG_ALLOW_QUERY <network addr>' macro in config.\n", include_binlog_name);
    include_binlog_name = NULL;
  }
  return res;
}

void dns_reset (void) {
  reload_time = time (NULL);
  dyn_clear_low ();
  dyn_clear_high ();
  dyn_clear_free_blocks ();
  tot_records = 0;
  labels_wptr = trie_nodes = trie_edges = records_wptr = labels_saved_bytes = 0;
  config_zones = zones = 0;
  binlog_allow_query_networks = binlog_allow_query_networks6 = 0;
  free_rb = -1;
  labels_buff[0] = 0;
  memset (HE, 0xff, sizeof (HE));
  memset (Z, 0, sizeof (Z));
  memset (dec_number, 0xff, sizeof (dec_number));
  memset (lo_alpha, 0xff, sizeof (lo_alpha));
}

void dns_stats (dns_stat_t *S) {
  S->percent_label_buff = (labels_wptr * 100.0) / DNS_LABELS_BUFFSIZE;
  S->percent_label_buff = (labels_saved_bytes * 100.0) / DNS_LABELS_BUFFSIZE;
  S->percent_record_buff = (records_wptr * 100.0) / DNS_RECORDS_BUFFSIZE;
  S->percent_nodes = (trie_nodes * 100.0) / DNS_MAX_TRIE_NODES;
  S->percent_edges = (trie_edges * 100.0) / DNS_MAX_TRIE_EDGES;
}


int dns_binlog_allow_query_networks_dump (char *output, int avail_out) {
  int i, k = 0;
  for (i = 0; i < binlog_allow_query_networks; i++) {
    unsigned int ip = BAQN[i].ipv4;
    if (k) {
      if (avail_out <= 2) {
        return -1;
      }
      *output++ = ',';
      *output++ = ' ';
      avail_out -= 2;
    }
    int l = snprintf (output, avail_out, "%d.%d.%d.%d/%d", ip >> 24, (ip >> 16) & 0xff, (ip >> 8) & 0xff, ip & 0xff, BAQN[i].prefix_bits);
    if (l >= avail_out) {
      return -1;
    }
    output += l;
    avail_out -= l;
    k++;
  }
  for (i = 0; i < binlog_allow_query_networks6; i++) {
    if (k) {
      if (avail_out <= 2) {
        return -1;
      }
      *output++ = ',';
      *output++ = ' ';
      avail_out -= 2;
    }
    int l = snprintf (output, avail_out, "%s/%d", show_ipv6 (BAQN6[i].ipv6), BAQN6[i].prefix_bits);
    if (l >= avail_out) {
      return -1;
    }
    output += l;
    avail_out -= l;
    k++;
  }
  if (avail_out <= 0) {
    return -1;
  }
  *output = 0;
  return 0;
}
