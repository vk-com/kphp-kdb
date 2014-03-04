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

#include <assert.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <semaphore.h>

#include "dns-constants.h"
#include "kdb-dns-binlog.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "kfs.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"dns-binlog-diff-1.00"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

#define mytime() (get_utime(CLOCK_MONOTONIC))

static const long long jump_log_pos = 0;
static long long add_logevents, delete_logevents;

typedef struct {
  char *name;
} name_t;

typedef struct {
  char *name;
  void *data;
  int age;
  short data_len; /* negative - delete record */
  unsigned char qtype;
  unsigned char name_len;
} record_t;

typedef struct zone {
  struct zone *next;
  char *origin;
  int origin_len;
  record_t *R;
  int records;
  int capacity;
} zone_t;

zone_t *zones;
zone_t *cur_zone;
static int cur_record_age;

static int cmp_str (char *a, int la, char *b, int lb) {
  if (la < lb) {
    if (!la) { return -1; }
    return (memcmp (a, b, la) <= 0) ? -1 : 1;
  }
  if (la > lb) {
    if (!lb) { return 1; }
    return (memcmp (a, b, lb) >= 0) ? 1 : -1;
  }
  if (!la) {
    return 0;
  }
  return memcmp (a, b, la);
}

static inline int cmp_record0 (record_t *a, record_t *b) {
  const int c = cmp_str (a->name, a->name_len, b->name, b->name_len);
  if (c) { return c; }
  if (a->qtype < b->qtype) { return -1; }
  if (a->qtype > b->qtype) { return  1; }
  return 0;
}

/* sort for delete records step */
static int cmp_record1 (const void *x, const void *y) {
  record_t *a = (record_t *) x;
  record_t *b = (record_t *) y;
  const int c = cmp_record0 (a, b);
  if (c) { return c; }
  if (a->age < b->age) { return -1; }
  if (a->age > b->age) { return  1; }
  return 0;
}

/* sort for merge records step */
static int cmp_record2 (const void *x, const void *y) {
  record_t *a = (record_t *) x;
  record_t *b = (record_t *) y;
  const int c = cmp_record0 (a, b);
  if (c) { return c; }
  return cmp_str (a->data, a->data_len, b->data, b->data_len);
}

static int cmp_ptr_zone (const void *x, const void *y) {
  zone_t *a = *((zone_t **) x);
  zone_t *b = *((zone_t **) y);
  return cmp_str (a->origin, a->origin_len, b->origin, b->origin_len);
}

static void zone_records_sort (zone_t *z) {
  record_t *R = z->R;
  qsort (R, z->records, sizeof (R[0]), cmp_record1);
  /* remove deleted records */
  int i = 0;
  while (i < z->records) {
    int j = i + 1, k;
    while (j < z->records && !cmp_str (R[i].name, R[i].name_len, R[j].name, R[j].name_len) && R[i].qtype == R[j].qtype) {
      j++;
    }
    for (k = j - 1; k >= i; k--) {
      if (R[k].data_len < 0) {
        int o;
        for (o = k - 1; o >= i; o--) {
          if (R[o].data_len > 0) {
            free (R[o].data);
            R[o].data = NULL;
            R[o].data_len = -1;
          }
        }
        break;
      }
    }
    i = j;
  }
  int n = 0;
  for (i = 0; i < z->records; i++) {
    if (R[i].data_len >= 0) {
      if (i > n) {
        memcpy (R + n, R + i, sizeof (R[i]));
      }
      n++;
    }
  }
  z->records = n;
  if (verbosity >= 4) {
    kprintf ("%s: zone %.*s\n", __func__, z->origin_len, z->origin);
    for (i = 0; i < n; i++) {
      assert (R[i].data_len >= 0);
      fprintf (stderr, "%d: name='%.*s', qtype = %d, data_len = %d\n", i, R[i].name_len, R[i].name, R[i].qtype, R[i].data_len);
    }
  }
}

static int zones_count (zone_t *z) {
  int r = 0;
  while (z != NULL) {
    r++;
    z = z->next;
  }
  return r;
}

static void zones_sort (zone_t **z, int n) {
  qsort (z, n, sizeof (z[0]), cmp_ptr_zone);
}

static void try_change_zone (void) {
  if (cur_zone == NULL) { return; }
  vkprintf (3, "%s: %.*s\n", __func__, cur_zone->origin_len, cur_zone->origin);
  struct lev_dns_change_zone *E = alloc_log_event (LEV_DNS_CHANGE_ZONE + cur_zone->origin_len, sizeof (struct lev_dns_change_zone) + cur_zone->origin_len, 0);
  memcpy (E->origin, cur_zone->origin, cur_zone->origin_len);
  cur_zone = NULL;
}

static void record_add (record_t *r) {
  try_change_zone ();
  assert (r->data_len >= 4);
  void *E = alloc_log_event (0, r->data_len, 0);
  memcpy (E, r->data, r->data_len);
  if (compute_uncommitted_log_bytes () > (1 << 23)) {
    flush_binlog_forced (0);
  }
  add_logevents++;
}

static record_t *last_deleted_record;

static void record_delete (record_t *r) {
  if (last_deleted_record && !cmp_str (last_deleted_record->name, last_deleted_record->name_len, r->name, r->name_len) && last_deleted_record->qtype == r->qtype) {
    return;
  }

  try_change_zone ();
  vkprintf (3, "Delete records for name '%.*s' of type %d.\n", r->name_len, r->name, r->qtype);
  struct lev_dns_delete_records *E = alloc_log_event (LEV_DNS_DELETE_RECORDS + r->name_len, sizeof (struct lev_dns_delete_records) + r->name_len, r->qtype);
  memcpy (E->name, r->name, r->name_len);
  last_deleted_record = r;
  if (compute_uncommitted_log_bytes () > (1 << 23)) {
    flush_binlog_forced (0);
  }
  delete_logevents++;
}

static void zone_merge (zone_t *a, zone_t *b, void (*add) (record_t *), void (*delete) (record_t *)) {
  int i = 0, j = 0;
  while (i < a->records && j < b->records) {
    int c = cmp_record0 (a->R+i, b->R+j);
    if (c < 0) {
      add (&a->R[i++]);
    } else if (!c) {
      int u = i + 1, v = j + 1;
      while (u < a->records && !cmp_record0 (a->R+i, a->R+u)) { u++; }
      while (v < b->records && !cmp_record0 (b->R+j, b->R+v)) { v++; }
      int eq = 1;
      const int n = u - i;
      if (n == v - j) {
        int k;
        for (k = 0; k < n; k++) {
          if (cmp_record2 (a->R + i + k, b->R + j + k)) {
            eq = 0;
            break;
          }
        }
      } else {
        eq = 0;
      }
      if (!eq) {
        while (j < v) {
          delete (&b->R[j++]);
        }
        while (i < u) {
          add (&a->R[i++]);
        }
      }
      i = u;
      j = v;
    } else {
      delete (&b->R[j++]);
    }
  }
  while (i < a->records) {
    add (&a->R[i++]);
  }
  while (j < b->records) {
    delete (&b->R[j++]);
  }
}

static void zone_diff (zone_t *a, zone_t *b) {
  cur_zone = a;
  qsort (a->R, a->records, sizeof (a->R[0]), cmp_record2);
  qsort (b->R, b->records, sizeof (b->R[0]), cmp_record2);
  zone_merge (a, b, record_add, record_delete);
}

static void zone_add (zone_t *a) {
  int i;
  vkprintf (2, "%s: %.*s\n", __func__, a->origin_len, a->origin);
  cur_zone = a;
  for (i = 0; i < a->records; i++) {
    record_add (&a->R[i]);
  }
}

static void zone_delete (zone_t *a) {
  int i;
  vkprintf (2, "%s: %.*s\n", __func__, a->origin_len, a->origin);
  cur_zone = a;
  for (i = 0; i < a->records; i++) {
    record_delete (&a->R[i]);
  }
}

static void zones_merge (zone_t *new_zones, zone_t *old_zones) {
  last_deleted_record = NULL;
  int na = zones_count (new_zones);
  int nb = zones_count (old_zones);
  int i = 0, j = 0;
  zone_t *z;
  zone_t **A = alloca (sizeof (A[0]) * na);
  for (z = new_zones; z != NULL; z = z->next) {
    A[i++] = z;
  }
  zone_t **B = alloca (sizeof (B[0]) * nb);
  for (z = old_zones; z != NULL; z = z->next) {
    B[j++] = z;
  }
  assert (i == na && j == nb);
  zones_sort (A, na);
  zones_sort (B, nb);
  i = j = 0;
  while (i < na && j < nb) {
    int c = cmp_str (A[i]->origin, A[i]->origin_len, B[j]->origin, B[j]->origin_len);
    if (c < 0) {
      zone_add (A[i++]);
    } else if (!c) {
      zone_diff (A[i], B[j]);
      i++;
      j++;
    } else {
      zone_delete (B[j++]);
    }
  }
  while (i < na) {
    zone_add (A[i++]);
  }
  while (j < nb) {
    zone_delete (B[j++]);
  }
}

static int dns_diff_replay_logevent (struct lev_generic *E, int size);

static int add_record (void *a, int sz, int qtype, char *name) {
  cur_record_age++;
  void *b = malloc (sz);
  assert (b);
  memcpy (b, a, sz);
  assert (cur_record_age > 0);
  zone_t *z = cur_zone;
  assert (z);
  if (z->capacity == z->records) {
    z->capacity *= 2;
    z->R = realloc (z->R, z->capacity * sizeof (z->R[0]));
    assert (z->R);
  }
  assert (z->capacity > z->records);
  record_t *R = &z->R[z->records++];
  R->data = b;
  R->age = cur_record_age;
  assert (qtype >= 0 && qtype <= 0xff);
  R->qtype = qtype;
  R->data_len = sz;
  if (name) {
    R->name_len = ((struct lev_generic *) a)->type & 0xff;
    R->name = b + (name - (char *) a);
  } else {
    R->name_len = 0;
    R->name = NULL;
  }
  return 0;
}

static int dns_record_a (struct lev_dns_record_a *E, int s) {
  return add_record (E, s, dns_type_a, E->name);
}

static int dns_record_aaaa (struct lev_dns_record_aaaa *E, int s) {
  return add_record (E, s, dns_type_aaaa, E->name);
}

static int dns_record_ptr (struct lev_dns_record_ptr *E, int s) {
  return add_record (E, s, dns_type_ptr, E->name);
}

static int dns_record_ns (struct lev_dns_record_ns *E, int s) {
  return add_record (E, s, dns_type_ns, NULL);
}

static int dns_record_soa (struct lev_dns_record_soa *E, int s) {
  return add_record (E, s, dns_type_soa, E->name);
}

static int dns_record_txt (struct lev_dns_record_txt *E, int s) {
  return add_record (E, s, dns_type_txt, E->name);
}

static int dns_record_mx (struct lev_dns_record_mx *E, int s) {
  return add_record (E, s, dns_type_mx, E->name);
}

static int dns_record_cname (struct lev_dns_record_cname *E, int s) {
  vkprintf (3, "%s: %.*s\n", __func__, E->type & 0xff, E->name);
  return add_record (E, s, dns_type_cname, E->name);
}

static int dns_record_srv (struct lev_dns_record_srv *E, int s) {
  return add_record (E, s, dns_type_srv, E->name);
}

static int dns_change_zone (struct lev_dns_change_zone *E) {
  vkprintf (3, "%s: origin = '%.*s'\n", __func__, E->type & 0xff, E->origin);
  const int l = E->type & 0xff;
  zone_t *z, *last = NULL;
  for (z = zones; z != NULL; z = z->next) {
    if (z->origin_len == l && !memcmp (z->origin, E->origin, l)) {
      cur_zone = z;
      return 0;
    }
    last = z;
  }
  cur_zone = z = malloc (sizeof (zone_t));
  assert (z);
  z->next = NULL;
  z->capacity = 32;
  z->R = malloc (z->capacity * sizeof (z->R[0]));
  assert (z->R);
  z->records = 0;
  if (last) {
    last->next = z;
  } else {
    assert (zones == NULL);
    zones = z;
  }
  z->origin = malloc (l);
  assert (z->origin);
  memcpy (z->origin, E->origin, l);
  z->origin_len = l;
  return 0;
}

int init_dns_data (int schema) {
  vkprintf (2, "%s: schema = 0x%x\n", __func__, schema);
  zones = NULL;
  cur_zone = NULL;
  cur_record_age = 0;
  replay_logevent = dns_diff_replay_logevent;
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

static int dns_diff_replay_logevent (struct lev_generic *E, int size) {
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
      if (dns_record_a ((struct lev_dns_record_a *) E, s) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_AAAA ... LEV_DNS_RECORD_AAAA + 0xff:
      s = sizeof (struct lev_dns_record_aaaa) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      if (dns_record_aaaa ((struct lev_dns_record_aaaa *) E, s) < 0) {
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
      if (dns_record_ptr ((struct lev_dns_record_ptr *) E, s) < 0) {
        return -1;
      }
      return s;
    case LEV_DNS_RECORD_NS ... LEV_DNS_RECORD_NS + 0xff:
      s = sizeof (struct lev_dns_record_ns) + (E->type & 0xff);
      if (size < s) {
        return -2;
      }
      if (dns_record_ns ((struct lev_dns_record_ns *) E, s) < 0) {
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
      if (dns_record_soa ((struct lev_dns_record_soa *) E, s) < 0) {
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
      if (dns_record_txt ((struct lev_dns_record_txt *) E, s) < 0) {
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
      if (dns_record_mx ((struct lev_dns_record_mx *) E, s) < 0) {
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
      if (dns_record_cname ((struct lev_dns_record_cname *) E, s) < 0) {
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
      if (dns_record_srv ((struct lev_dns_record_srv *) E, s) < 0) {
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
    //TODO: remove record logevents
  }
  kprintf ("unknown log event type %08x at position %lld\n", E->type, log_cur_pos ());
  return -3;
}

zone_t *binlog_load (const char *dns_binlog_name, int readonly) {
  vkprintf (2, "%s: dns_binlog_name = '%s'\n", __func__, dns_binlog_name);
  replay_logevent = default_replay_logevent;
  double binlog_load_time = -mytime ();
  binlog_disabled = readonly;
  if (engine_preload_filelist (dns_binlog_name, NULL) < 0) {
    kprintf ("cannot open binlog files for %s\n", dns_binlog_name);
    exit (1);
  }
  Binlog = open_binlog (engine_replica, jump_log_pos);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, jump_log_pos);
    exit (1);
  }
  binlogname = Binlog->info->filename;
  clear_log ();
  init_log_data (jump_log_pos, 0, 0);
  vkprintf (1, "replay log events started\n");
  if (replay_log (0, 1) < 0) {
    exit (1);
  }
  vkprintf (1, "replay log events finished\n");
  binlog_load_time += mytime ();
  if (!binlog_disabled) {
    clear_read_log ();
  }
  clear_write_log ();
  if (readonly) {
    close_binlog (Binlog, 1);
    Binlog = NULL;
    close_replica (engine_replica);
    engine_replica = NULL;
  }
  zone_t *z;
  for (z = zones; z != NULL; z = z->next) {
    zone_records_sort (z);
  }

  return zones;
}

/*
 *
 *		MAIN
 *
 */

void usage (void) {
  fprintf (stderr, "usage: %s [-u<username>] [-v] [-l<log-name>] <new-binlog> <old-binlog> %s\n"
    "Appends <old-binlog> so it will be identical to <new-binlog>.\n"
    "<new-binlog> should be generated with dns-engine via -E option.\n"
    , progname, FullVersionStr);
  exit (2);
}

int main (int argc, char *argv[]) {
  int c;

  progname = argv[0];

  while ((c = getopt (argc, argv, "c:l:u:v")) != -1) {
    switch (c) {
    case 'c':
      maxconn = atoi (optarg);
      break;
    case 'l':
      logname = optarg;
      break;
    case 'u':
      username = optarg;
      break;
    case 'v':
      verbosity++;
      break;
    default:
      fprintf (stderr, "Unimplemented option %c\n", c);
      usage ();
    }
  }
  if (argc != optind + 2) {
    usage ();
    return 2;
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  //dynamic_data_buffer_size = 64 << 20;
  //init_dyn_data ();
  zone_t *new_zones = binlog_load (argv[optind], 1);
  assert (new_zones);
  zone_t *old_zones = binlog_load (argv[optind+1], 0);
  assert (old_zones);

  assert (binlogname);
  assert (!binlog_disabled);
  assert (append_to_binlog (Binlog) == log_readto_pos);

  zones_merge (new_zones, old_zones);
  flush_binlog_last ();
  sync_binlog (2);
  if (verbosity) {
    fprintf (stdout, "%lld add record logevents.\n", add_logevents);
    fprintf (stdout, "%lld delete record logevents.\n", delete_logevents);
  }

  return 0;
}
