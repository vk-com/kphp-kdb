/*
    This file is part of VK/KittenPHP-DB-Engine Library.

    VK/KittenPHP-DB-Engine Library is free software: you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation, either version 2 of the License, or
    (at your option) any later version.

    VK/KittenPHP-DB-Engine Library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with VK/KittenPHP-DB-Engine Library.  If not, see <http://www.gnu.org/licenses/>.

    Copyright 2009-2013 Vkontakte Ltd
              2009-2013 Nikolai Durov
              2009-2013 Andrei Lopatin
                   2013 Vitaliy Valtman
              2012-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#define _XOPEN_SOURCE 500

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <aio.h>
#include <errno.h>

#include "crc32.h"
#include "md5.h"
#include "kdb-binlog-common.h"
#include "kdb-data-common.h"
#include "kfs-layout.h"
#include "kfs.h"
#include "server-functions.h"

#define MAX_LOG_TS_INTERVAL	100


extern int verbosity, now;

char *binlogname;
int binlog_fd = -1;
int binlog_disabled;
int binlog_repairing;
int binlog_zipped;
int binlog_cyclic_mode;
int binlog_end_found;
unsigned char binlog_tag[16];

int binlog_check_mode = 0, binlog_check_errors;

long long max_binlog_size = DEFAULT_MAX_BINLOG_SIZE;
long long next_binlog_rotate_pos = DEFAULT_MAX_BINLOG_SIZE;

/*
 *	following functions WILL be moved to kfs-read.c
 */

unsigned tot_crc32;
long long tot_crc32_pos;

int binlog_existed = 0;
int binlog_headers;
int failed_rotate_to = 0;

long long log_headers_size;
long long log_start_pos;

struct kfs_file_header kfs_Hdr[3], *KHDR;

struct lev_start *ST;
struct lev_rotate_from *CONT;

static struct aiocb binlog_sync_aiocb, binlog_write_aiocb;
static int binlog_sync_active, binlog_sync_last, binlog_write_active, binlog_write_last;

static char *aio_write_start;

kfs_hash_t cur_binlog_file_hash, cur_replica_hash, prev_binlog_file_hash;
int need_switch_binlog;


int check_kfs_header_basic (struct kfs_file_header *H) {
  assert (H->magic == KFS_MAGIC);
  if (compute_crc32 (H, 4092) != H->header_crc32) {
    return -1;
  }
  if (H->kfs_version != KFS_V01) {
    return -1;
  }
  return 0;
}

void create_kfs_header_basic (struct kfs_file_header *H) {
  memset (H, 0, 4096);
  H->magic = KFS_MAGIC;
  H->kfs_version = KFS_V01;
  H->finished = -1;
}

void fix_kfs_header_crc32 (struct kfs_file_header *H) {
  H->header_crc32 = compute_crc32 (H, 4092);
}

static int load_binlog_headers (int fd) {
  int r;
  struct lev_start *E;
  assert (sizeof (struct kfs_file_header) == 4096);

  binlog_headers = 0;
  binlog_existed = 0;
  ST = 0;
  CONT = 0;
  KHDR = 0;
  cur_binlog_file_hash = 0;
  log_headers_size = 0;
  tot_crc32 = 0;
  tot_crc32_pos = 0;

  if (lseek (fd, 0, SEEK_SET) != 0) {
    fprintf (stderr, "cannot lseek binlog file %d: %m\n", fd);
    return -1;
  }

  r = read (fd, kfs_Hdr, 4096 * 3);
  if (!r) {
    return 0;
  }

  if (Binlog && Binlog->fd == fd) {
    kfs_buffer_crypt (Binlog, kfs_Hdr, r, 0LL);
  }

  if (r >= 4096 && kfs_Hdr[0].magic == KFS_MAGIC) {
    if (check_kfs_header_basic (kfs_Hdr) < 0 || kfs_Hdr[0].kfs_file_type != kfs_binlog) {
      fprintf (stderr, "bad kfs header #0\n");
      return -1;
    }
    binlog_headers++;
    if (r >= 8192 && kfs_Hdr[1].magic == KFS_MAGIC) {
      if (check_kfs_header_basic (kfs_Hdr + 1) < 0 || kfs_Hdr[1].kfs_file_type != kfs_binlog) {
        fprintf (stderr, "bad kfs header #1\n");
        return -1;
      }
      binlog_headers++;
      if (kfs_Hdr[1].header_seq_num == kfs_Hdr[0].header_seq_num) {
        assert (!memcmp (kfs_Hdr + 1, kfs_Hdr, 4096));
      }
    }
  }
  r -= binlog_headers * 4096;
  if (r < 4) {
    fprintf (stderr, "no first entry in binlog\n");
    return -1;
  }
  E = (struct lev_start *) (kfs_Hdr + binlog_headers);

  loop:
  switch (E->type) {
  case LEV_START:
    assert (r >= sizeof (struct lev_start) - 4);
    ST = E;
    log_start_pos = 0;
    break;
  case LEV_ROTATE_FROM:
    assert (r >= sizeof (struct lev_rotate_from));
    CONT = (struct lev_rotate_from *) E;
    log_start_pos = CONT->cur_log_pos;
    cur_binlog_file_hash = CONT->cur_log_hash;
    break;
  case KFS_BINLOG_ZIP_MAGIC:
    assert (!binlog_headers && binlog_zipped);
    assert (r >= sizeof (kfs_binlog_zip_header_t));
    /* infinite loop guard */
    assert (E == (struct lev_start *) (kfs_Hdr + binlog_headers));
    E = (struct lev_start *) ((kfs_binlog_zip_header_t *) E)->first36_bytes;
    r = 36;
    goto loop;
  default:
    fprintf (stderr, "fatal: binlog file begins with wrong entry type %08x\n", E->type);
    return -1;
  }

  binlog_existed = 1;

  if (!binlog_headers) {
    log_headers_size = 0;
    assert (lseek (fd, log_headers_size, SEEK_SET) == log_headers_size);
    return 0;
  }

  KHDR = kfs_Hdr;
  if (binlog_headers > 1 && kfs_Hdr[1].header_seq_num > kfs_Hdr[0].header_seq_num) {
    KHDR++;
  }

  assert (KHDR->data_size + binlog_headers * 4096 == KHDR->raw_size);
//  assert (lseek (fd, 0, SEEK_END) == KHDR->raw_size);

  tot_crc32 = KHDR->data_crc32;
  tot_crc32_pos = KHDR->log_pos + KHDR->data_size;

  if (KHDR->finished == -1) {
    fprintf (stderr, "fatal: incomplete kfs file\n");
    return -1;
  }

  if (ST) {
    if (ST->schema_id != KHDR->schema_id) {
      fprintf (stderr, "fatal: binlog schema id mismatch.\n");
      return -1;
    }
    if (ST->split_min != KHDR->split_min || ST->split_max != KHDR->split_max || ST->split_mod != KHDR->split_mod) {
      fprintf (stderr, "fatal: binlog slice parameters mismatch.\n");
      return -1;
    }
    if (KHDR->log_pos) {
      fprintf (stderr, "fatal: first binlog file has non-zero log_pos %lld\n", KHDR->log_pos);
      return -1;
    }
  }

  if (CONT) {
    if (KHDR->log_pos != CONT->cur_log_pos) {
      fprintf (stderr, "fatal: continuation binlog file log_pos mismatch: %lld != %lld\n", KHDR->log_pos, CONT->cur_log_pos);
      return -1;
    }
    if (KHDR->prev_log_hash != CONT->prev_log_hash) {
      fprintf (stderr, "fatal: binlog file prev_log_hash mismatch: %016llx != %016llx\n", KHDR->prev_log_hash, CONT->prev_log_hash);
      return -1;
    }
    if (KHDR->file_id_hash != CONT->cur_log_hash) {
      fprintf (stderr, "fatal: binlog file file_id_hash mismatch: %016llx != %016llx\n", KHDR->file_id_hash, CONT->cur_log_hash);
      return -1;
    }
    if (KHDR->log_pos_crc32 != CONT->crc32) {
      fprintf (stderr, "fatal: binlog file crc32 mismatch: %08x != %08x\n", KHDR->log_pos_crc32, CONT->crc32);
      return -1;
    }
    if (KHDR->prev_log_time != CONT->timestamp) {
      fprintf (stderr, "fatal: binlog file file_id_hash mismatch: %d != %d\n", KHDR->prev_log_time, CONT->timestamp);
      return -1;
    }
  }

  log_headers_size = binlog_headers * 4096;
  assert (lseek (fd, log_headers_size, SEEK_SET) == log_headers_size);

  if (KHDR) {
    cur_binlog_file_hash = KHDR->file_id_hash;
    if (!cur_replica_hash) {
      cur_replica_hash = KHDR->replica_id_hash;
    } else if (cur_replica_hash != KHDR->replica_id_hash) {
      fprintf (stderr, "fatal: binlog file replica_hash mismatch: %016llx != %016llx\n", KHDR->replica_id_hash, cur_replica_hash);
      return -1;
    }
  }

  return binlog_headers;
}


hash_t calc_binlog_hash (int rhandle, long long expected_size, const char *logbuf, int bufsize, int overlap) {
  if (binlog_zipped) {
    if (cur_binlog_file_hash) {
      return cur_binlog_file_hash;
    }
    kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) Binlog->info->start;
    assert (*(int *) H->first36_bytes == LEV_START);
    return cur_binlog_file_hash = H->file_hash;
  }

  long long cpos = lseek (rhandle, 0, SEEK_CUR);  
  long long fsize = (!binlog_cyclic_mode || !binlog_end_found) ? lseek (rhandle, 0, SEEK_END) : lseek (rhandle, -4, SEEK_END);

  assert (overlap >= 0 && overlap <= bufsize && overlap <= fsize);
  assert (fsize >= 0 && (cpos == fsize || (binlog_cyclic_mode && binlog_end_found)));

  fsize -= overlap;

  assert (expected_size >= 24 + 36);

  assert (fsize + bufsize == expected_size);

  if (cur_binlog_file_hash) {
    return cur_binlog_file_hash;
  }

  static unsigned char buffer[32768];

  assert (lseek (rhandle, 0, SEEK_SET) == 0);


  if (fsize + bufsize <= 32768) {
    int r = 0;
    if (fsize > 0) {
      r = read (rhandle, buffer, fsize);
      assert (r == fsize);
      kfs_buffer_crypt (Binlog, buffer, r, 0LL);
    }
    memcpy (buffer + r, logbuf, bufsize);
  } else if (fsize >= 16384) {
    int r1 = read (rhandle, buffer, 16384);
    assert (r1 == 16384);
    kfs_buffer_crypt (Binlog, buffer, r1, 0LL);
    int toread = 16384 - bufsize;
    if (toread > 0) {
      if (!binlog_cyclic_mode) {
        assert (lseek (rhandle, - toread - overlap, SEEK_END) == fsize - toread);
      } else {
        assert (lseek (rhandle, - toread - overlap - 4, SEEK_END) == fsize - toread);
      }
      int r2 = read (rhandle, buffer + 16384, toread);
      assert (r2 == toread);
      kfs_buffer_crypt (Binlog, buffer + 16384, r2, fsize - toread);
      memcpy (buffer + 16384 + r2, logbuf, bufsize);
    } else {
      memcpy (buffer + 16384, logbuf + bufsize - 16384, 16384);
    }
  } else {
    int r1 = 0;
    if (fsize > 0) {
      r1 = read (rhandle, buffer, fsize);
      assert (r1 == fsize);
      kfs_buffer_crypt (Binlog, buffer, r1, 0LL);
    }
    memcpy (buffer + r1, logbuf, 16384 - fsize);
    memcpy (buffer + 16384, logbuf + bufsize - 16384, 16384);
  }

  assert (*(int *)buffer == LEV_START);

  int totsize = expected_size >= 32768 ? 32768 : (int)expected_size;

  *(hash_t *)(buffer + totsize - 16) = 0;
  *(hash_t *)(buffer + totsize - 8) = 0;
  
  static unsigned char md[16];
  md5 (buffer, totsize, md);

  if (!binlog_cyclic_mode || !binlog_end_found) {
    assert (lseek (rhandle, 0, SEEK_END) == fsize + overlap);
  } else {
    assert (lseek (rhandle, -4, SEEK_END) == fsize + overlap);
  }

  return cur_binlog_file_hash = *(hash_t *)md;
}


/*
 *
 *  LOG REPLAY
 *
 */


static struct log_buffer {
  long long log_start_pos;
  char *log_start, *log_rptr, *log_wptr, *log_end, *log_endw, *log_last_endw, *log_wcrypt_ptr;
} R, W;

long long binlog_size, log_pos, log_readto_pos, log_cutoff_pos, log_limit_pos = -1;
int log_split_min, log_split_max, log_split_mod;
int log_first_ts, log_read_until, log_last_ts; // first log timestamp, last read timestamp, last read or written timestamp
int log_ts_interval = MAX_LOG_TS_INTERVAL;
int log_ts_exact_interval = 60;
int log_next_exact_ts;
int log_time_cutoff, log_scan_mode;
int log_true_now, log_set_now;

unsigned log_crc32_complement = -1;
long long log_crc32_pos;

int log_crc32_interval = 16384;
/* if bytes_after_crc32 >= the above number, 
   write crc32 before */
int log_crc32_interval2 = 4096;	
/* if bytes_after_crc32 >= the above number, 
   and we want to write a timestamp, write crc32 instead */
int bytes_after_crc32 = ULOG_BUFFER_SIZE*2;

int disable_crc32;  /* 1 = disable write, 2 = disable check, 3 = disable both, +4 = disable eval */
int disable_ts; /* 1 = disable write */

char LogBuffer[ULOG_BUFFER_SIZE+ZLOG_BUFFER_SIZE+16];
char *LogCopyBuffer = NULL;
char LogSlaveBuffer[SLOG_BUFFER_SIZE+16];

extern int init_targ_data (int schema);
extern int init_stats_data (int schema);

// NB: while reading binlog, this is position of the log event being interpreted;
//     while writing binlog, this is the last position to have been written to disk, uncommitted log bytes are not included.
long long log_cur_pos (void) {
  return log_pos - (R.log_wptr - R.log_rptr);
} 

long long log_write_pos (void) {
  if (W.log_rptr <= W.log_wptr) {
    return log_pos + (W.log_wptr - W.log_rptr);
  } else {
    assert (W.log_endw);
    return log_pos + (W.log_wptr - W.log_start) + (W.log_endw - W.log_rptr);
  }
} 

long long log_last_pos (void) {
  return binlog_disabled ? log_cur_pos () : log_write_pos ();
}

int init_targ_data (int schema) __attribute ((weak));
int init_stats_data (int schema) __attribute ((weak));
int init_search_data (int schema) __attribute ((weak));
int init_friends_data (int schema) __attribute ((weak));
int init_news_data (int schema) __attribute ((weak));
int init_lists_data (int schema) __attribute ((weak));
int init_text_data (int schema) __attribute ((weak));
int init_money_data (int schema) __attribute ((weak));
int init_hints_data (int schema) __attribute ((weak));
int init_bayes_data (int schema) __attribute ((weak));
int init_magus_data (int schema) __attribute ((weak));
int init_photo_data (int schema) __attribute ((weak));
int init_mf_data (int schema) __attribute ((weak));
int init_isearch_data (int schema) __attribute ((weak));
int init_logs_data (int schema) __attribute ((weak));
int init_pmemcached_data (int schema) __attribute ((weak));
int init_sql_data (int schema) __attribute ((weak));
int init_password_data (int schema) __attribute ((weak));
int init_filesys_data (int schema) __attribute ((weak));
int init_cache_data (int schema) __attribute ((weak));
int init_dns_data (int schema) __attribute ((weak));
int init_weights_data (int schema) __attribute ((weak));
int init_storage_data (int schema) __attribute ((weak));
int init_support_data (int schema) __attribute ((weak));
int init_antispam_data (int schema) __attribute ((weak));
int init_copyexec_aux_data (int schema) __attribute ((weak));
int init_copyexec_main_data (int schema) __attribute ((weak));
int init_copyexec_result_data (int schema) __attribute ((weak));
int init_gms_data (int schema) __attribute ((weak));
int init_gms_money_data (int schema) __attribute ((weak));
int init_seqmap_data (int schema) __attribute ((weak));
int init_rpc_proxy_data (int schema) __attribute ((weak));

int init_targ_data (int schema) {
  fprintf (stderr, "sorry, TARGET_SCHEMA module is not loaded.\n");
  return -1;
}

int init_stats_data (int schema) {
  fprintf (stderr, "sorry, STATS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_search_data (int schema) {
  fprintf (stderr, "sorry, SEARCH_SCHEMA module is not loaded.\n");
  return -1;
}

int init_friends_data (int schema) {
  fprintf (stderr, "sorry, FRIENDS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_news_data (int schema) {
  fprintf (stderr, "sorry, NEWS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_lists_data (int schema) {
  fprintf (stderr, "sorry, LISTS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_text_data (int schema) {
  fprintf (stderr, "sorry, TEXT_SCHEMA module is not loaded.\n");
  return -1;
}

int init_money_data (int schema) {
  fprintf (stderr, "sorry, MONEY_SCHEMA module is not loaded.\n");
  return -1;
}

int init_hints_data (int schema) {
  fprintf (stderr, "sorry, HINTS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_bayes_data (int schema) {
  fprintf (stderr, "sorry, BAYES_SCHEMA module is not loaded.\n");
  return -1;
}

int init_magus_data (int schema) {
  fprintf (stderr, "sorry, MAGUS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_photo_data (int schema) {
  fprintf (stderr, "sorry, PHOTO_SCHEMA module is not loaded.\n");
  return -1;
}

int init_mf_data (int schema) {
  fprintf (stderr, "sorry, MF_SCHEMA module is not loaded.\n");
  return -1;
}

int init_isearch_data (int schema) {
  fprintf (stderr, "sorry, ISEARCH_SCHEMA module is not loaded.\n");
  return -1;
}

int init_logs_data (int schema) {
  fprintf (stderr, "sorry, LOGS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_pmemcached_data (int schema) {
  fprintf (stderr, "sorry, PMEMCACHED_SCHEMA module is not loaded.\n");
  return -1;
}

int init_sql_data (int schema) {
  fprintf (stderr, "sorry, SQL_SCHEMA module is not loaded.\n");
  return -1;
}

int init_password_data (int schema) {
  fprintf (stderr, "sorry, PASSWORD_SCHEMA module is not loaded.\n");
  return -1;
}

int init_filesys_data (int schema) {
  fprintf (stderr, "sorry, FILESYS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_cache_data (int schema) {
  fprintf (stderr, "sorry, CACHE_SCHEMA module is not loaded.\n");
  return -1;
}

int init_weights_data (int schema) {
  fprintf (stderr, "sorry, WEIGHTS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_dns_data (int schema) {
  fprintf (stderr, "sorry, DNS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_storage_data (int schema) {
  fprintf (stderr, "sorry, STORAGE_SCHEMA module is not loaded.\n");
  return -1;
}

int init_support_data (int schema) {
  fprintf (stderr, "sorry, SUPPORT_SCHEMA module is not loaded.\n");
  return -1;
}

int init_antispam_data (int schema) {
  fprintf (stderr, "sorry, ANTISPAM_SCHEMA module is not loaded.\n");
  return -1;
}

int init_copyexec_aux_data (int schema) {
  fprintf (stderr, "sorry, COPYEXEC_AUX_SCHEMA module is not loaded.\n");
  return -1;
}

int init_copyexec_main_data (int schema) {
  fprintf (stderr, "sorry, COPYEXEC_MAIN_SCHEMA module is not loaded.\n");
  return -1;
}

int init_copyexec_result_data (int schema) {
  fprintf (stderr, "sorry, COPYEXEC_RESULT_SCHEMA module is not loaded.\n");
  return -1;
}

int init_gms_data (int schema) {
  fprintf (stderr, "sorry, GMS_SCHEMA module is not loaded.\n");
  return -1;
}

int init_gms_money_data (int schema) {
  fprintf (stderr, "sorry, GMS_MONEY_SCHEMA module is not loaded.\n");
  return -1;
}

int init_seqmap_data (int schema) {
  fprintf (stderr, "sorry, SEQMAP_SCHEMA module is not loaded.\n");
  return -1;
}

int init_rpc_proxy_data (int schema) {
  fprintf (stderr, "sorry, RPC_PROXY_SCHEMA module is not loaded.\n");
  return -1;
}

int set_schema (int schema) {
  int res = -1;
  switch (schema & (-1 << 16)) {
  case 0x6ba30000:
    res = init_targ_data (schema);
    break;
  case 0x3ae60000:
    res = init_stats_data (schema);
    break;
  case 0xbeef0000:
    res = init_search_data (schema);
    break;  
  case 0x2bec0000:
    res = init_friends_data (schema);
    break;
  case 0x53c40000:
    res = init_news_data (schema);
    break;
  case 0x6ef20000:
    res = init_lists_data (schema);
    break;
  case 0x2cb30000:
    res = init_text_data (schema);
    break;
  case 0xf00d0000:
    res = init_money_data (schema);
    break;
  case 0x4fad0000:
    res = init_hints_data (schema);
    break;
  case 0x3ad50000:
    res = init_bayes_data (schema);
    break;
  case 0x7a9c0000:
    res = init_magus_data (schema);
    break;
  case 0x5f180000:
    res = init_photo_data (schema);
    break;
  case 0xafe60000:
    res = init_mf_data (schema);
    break;
  case 0x5fad0000:
    res = init_isearch_data (schema);
    break;
  case 0x21090000:
    res = init_logs_data (schema);
    break;
  case 0x37450000:
    res = init_pmemcached_data (schema);
    break;
  case 0xfa730000:
    res = init_sql_data (schema);
    break;
  case 0x65510000:
    res = init_password_data (schema);
    break;
  case 0x723d0000:
    res = init_filesys_data (schema);
    break;
  case 0x3ded0000:
    res = init_cache_data (schema);
    break;
  case 0x521e0000:
    res = init_weights_data (schema);
    break;
  case 0x144a0000:
    res = init_dns_data (schema);
    break;
  case 0x805a0000:
    res = init_storage_data (schema);
    break;
  case 0x1fec0000:
    res = init_support_data (schema);
    break;
  case 0x91a70000:
    res = init_antispam_data (schema);
    break;
  case 0x29790000:
    res = init_copyexec_aux_data (schema);
    break;
  case 0xeda90000:
    res = init_copyexec_main_data (schema);
    break;
  case 0xedaa0000:
    res = init_copyexec_result_data (schema);
    break;
  case 0x8f6a0000:
    res = init_gms_data (schema);
    break;
  case 0xfa8f0000:
    res = init_gms_money_data (schema);
    break;
  case 0x8f0e0000:
    res = init_seqmap_data (schema);
    break;
  case 0xf9a90000:
    res = init_rpc_proxy_data (schema);
    break;
  }
  if (res >= 0) {
    log_schema = schema;
  } else {
    fprintf (stderr, "unknown schema: %08x\n", schema);
  }
  return res;
}

/* computes log_crc32_complement up to new log_crc32_pos, equal to log_cur_pos() + s
   in most cases, s = 0
*/
void relax_log_crc32 (int s) {
  assert (s >= 0);
  s = (s + 3) & -4;
  if (disable_crc32 & 2) {
    return;
  }
  assert (log_crc32_pos >= log_pos - (R.log_wptr - R.log_start)); // log_pos corresponds to R.log_wptr
  assert (s <= R.log_wptr - R.log_rptr);
  long long new_log_crc32_pos = log_cur_pos() + s;
  if (log_crc32_pos < tot_crc32_pos && new_log_crc32_pos >= tot_crc32_pos) {
    log_crc32_complement = crc32_partial (R.log_wptr - (log_pos - log_crc32_pos), tot_crc32_pos - log_crc32_pos, log_crc32_complement);
    log_crc32_pos = tot_crc32_pos;
    if (tot_crc32 != ~log_crc32_complement) {
      fprintf (stderr, "fatal: crc32 mismatch in binlog at position %lld: header expects %08x, actual %08x\n",
      tot_crc32_pos, tot_crc32, ~log_crc32_complement);
    }
    assert (tot_crc32 == ~log_crc32_complement);
  }
  log_crc32_complement = crc32_partial (R.log_wptr - (log_pos - log_crc32_pos), new_log_crc32_pos - log_crc32_pos, log_crc32_complement);
  log_crc32_pos = new_log_crc32_pos;
}


/* updates log_crc32_complement while WRITING binlog
*/
unsigned relax_write_log_crc32 (void) {
  if (binlog_disabled || (disable_crc32 & 4)) {
    return 0;
  }
  long long log_start_pos = W.log_endw ? log_pos + (W.log_endw - W.log_rptr) : log_pos - (W.log_rptr - W.log_start);
  if (!(log_crc32_pos >= log_start_pos) || W.log_wptr < W.log_start) {
    fprintf(stderr, "W.log_endw = %p\n", W.log_endw);
    fprintf(stderr, "log_pos = %lld\n", log_pos);
    fprintf(stderr, "W.log_rptr = %p\n", W.log_rptr);
    fprintf(stderr, "W.log_start = %p\n", W.log_start);
    fprintf(stderr, "W.log_wptr = %p\n", W.log_wptr);
    fprintf(stderr, "log_crc32_pos = %lld\n", log_crc32_pos);
    fprintf(stderr, "log_start_pos = %lld\n", log_start_pos);
  }
  assert (log_crc32_pos >= log_start_pos); // log_pos corresponds to W.log_rptr
  long long new_log_crc32_pos = log_start_pos + (W.log_wptr - W.log_start);
  assert (log_crc32_pos <= new_log_crc32_pos);
  log_crc32_complement = crc32_partial (W.log_start + (log_crc32_pos - log_start_pos), new_log_crc32_pos - log_crc32_pos, log_crc32_complement);
  log_crc32_pos = new_log_crc32_pos;
  return ~log_crc32_complement;
}


static struct lev_rotate_to new_rotate_to;
static struct lev_rotate_from new_rotate_from;
static struct {
  hash_t prev_hash;
  long long stpos;
  unsigned crc32;
} cbuff;
static unsigned char MDBUF[16];


int force_new_binlog (void) {
  if (verbosity > 1) {
    fprintf (stderr, "required next binlog creation at log position %lld\n", log_cur_pos());
  }
  if (failed_rotate_to) {
    next_binlog_rotate_pos = (1LL << 62);
    return 0;
  }

  if (!Binlog || Binlog->lock >= 0) {
    return 0;
  }
  assert (Binlog->lock == -1);

/*
  int cnt = 0;
  while (binlog_write_active) {
    flush_binlog ();
    usleep (1000);
    if (++cnt > 3000) {
      fprintf (stderr, "cannot write binlog for 3 seconds!");
      exit (3);
    }
  }

  if (cnt > 0) {
    fprintf (stderr, "had to wait %d milliseconds for aio_write termination\n", cnt);
  }
*/

  if (!binlog_cyclic_mode) {
    flush_binlog_forced (0);
  } else {
    flush_cbinlog (1);
  }

  if (W.log_rptr != W.log_wptr || W.log_endw) {
    fprintf (stderr, "failed to flush binlog file %s at position %lld\n", Binlog->info->filename, log_pos);
    failed_rotate_to = 1;
    return -1;
  }

  unsigned crc32 = relax_write_log_crc32();
  long long pos = log_crc32_pos;

  assert (pos == log_cur_pos() && pos == log_pos);

  new_rotate_to.type = LEV_ROTATE_TO;
  new_rotate_to.timestamp = (now ? now : time(0));
  new_rotate_to.next_log_pos = pos + 36;
  new_rotate_to.crc32 = crc32;
  new_rotate_to.cur_log_hash = 0;
  new_rotate_to.next_log_hash = 0;

  if (!cur_binlog_file_hash) {
    assert (!log_start_pos && !binlog_headers);
    int h = open (Binlog->info->filename, O_RDONLY);
    if (h < 0) {
      fprintf (stderr, "failed to reopen first binlog file %s to compute its file hash\n", Binlog->info->filename);
      failed_rotate_to = 1;
      return -1;
    }
    if (!binlog_cyclic_mode) {
      assert (lseek (h, 0, SEEK_END) >= 0);
    } else {
      assert (lseek (h, -4, SEEK_END) >= 0);
    }
    assert (!binlog_zipped);
    new_rotate_to.cur_log_hash = calc_binlog_hash (h, log_pos + 36, (char *)&new_rotate_to, 36, 0);
    close (h);
  } else { 
    new_rotate_to.cur_log_hash = cur_binlog_file_hash;
  }

  cbuff.prev_hash = cur_binlog_file_hash;
  cbuff.stpos = pos + 36;
  cbuff.crc32 = crc32;
  md5 ((unsigned char *)&cbuff, 20, MDBUF);
  new_rotate_to.next_log_hash = *(hash_t *)MDBUF;

  new_rotate_from.type = LEV_ROTATE_FROM;
  new_rotate_from.timestamp = new_rotate_to.timestamp;
  new_rotate_from.crc32 = ~crc32_partial (&new_rotate_to, 36, log_crc32_complement);
  new_rotate_from.cur_log_pos = pos + 36;
  new_rotate_from.prev_log_hash = new_rotate_to.cur_log_hash;
  new_rotate_from.cur_log_hash = new_rotate_to.next_log_hash;

  kfs_file_handle_t NextBinlog = create_next_binlog (Binlog, new_rotate_from.cur_log_pos, new_rotate_from.cur_log_hash);
  if (!NextBinlog) {
    fprintf (stderr, "failed to create next binlog file after %s at position %lld\n", Binlog->info->filename, log_pos);
    failed_rotate_to = 1;
    return -1;
  }

  kfs_buffer_crypt (NextBinlog, &new_rotate_from, 36, 0);
  int w = write (NextBinlog->fd, &new_rotate_from, 36);
  kfs_buffer_crypt (NextBinlog, &new_rotate_from, 36, 0); /* restore new_rotate_from */
  if (w != 36) {
    fprintf (stderr, "unable to write 36 bytes into next binlog file %s (log position %lld): %m\n", NextBinlog->info->filename, log_pos);
    unlink (NextBinlog->info->filename);
    close_binlog (NextBinlog, 1);
    failed_rotate_to = 1;
    return -1;
  }

  if (binlog_cyclic_mode) {
    int t = LEV_CBINLOG_END;
    kfs_buffer_crypt (NextBinlog, &t, 4, 40);
    assert (write (NextBinlog->fd, &t, 4) == 4);
    w = ftruncate (NextBinlog->fd, max_binlog_size);
    if (w < 0) {
      fprintf (stderr, "Error truncating file to size %lld: %m\n", max_binlog_size);
      exit (3);
    }
  }

  long long log_file_pos = log_pos - log_start_pos + log_headers_size;
  if (!binlog_cyclic_mode) {
    assert (lseek (Binlog->fd, 0, SEEK_END) == log_file_pos);
  } else {
    assert (lseek (Binlog->fd, -4, SEEK_END) == log_file_pos);
  }
  
  kfs_buffer_crypt (Binlog, &new_rotate_to, 36, log_file_pos);
  assert (write (Binlog->fd, &new_rotate_to, 36) == 36);
  kfs_buffer_crypt (Binlog, &new_rotate_to, 36, log_file_pos); /* restore new_rotato_to */
  
  if (verbosity > 0) {
    fprintf (stderr, "switching to new binlog file %s from %s (size=%lld), log position %lld; new fd=%d\n",
             NextBinlog->info->filename, Binlog->info->filename, pos + 36 - log_start_pos + binlog_headers * 4096,
             pos + 36, NextBinlog->fd);
  }

  close_binlog (Binlog, !binlog_sync_active && !binlog_write_active);

  Binlog = NextBinlog;

  binlog_headers = 0;
  binlog_existed = 0;
  ST = 0;
  CONT = &new_rotate_from;
  KHDR = 0;
  prev_binlog_file_hash = new_rotate_from.prev_log_hash;
  cur_binlog_file_hash = new_rotate_from.cur_log_hash;

  log_crc32_complement = crc32_partial (&new_rotate_from, 36, ~new_rotate_from.crc32);
  log_start_pos = CONT->cur_log_pos;
  log_pos = log_crc32_pos = log_start_pos + 36;

  binlog_existed = 1;

  log_headers_size = 0;
  binlogname = Binlog->info->filename;

  binlog_fd = Binlog->fd;
  binlog_size = 36;

  int keep_now = now;
  bytes_after_crc32 = 0;
  log_last_ts = now = new_rotate_to.timestamp;
  log_next_exact_ts = now - now % log_ts_exact_interval + log_ts_exact_interval;
  next_binlog_rotate_pos = log_pos + max_binlog_size;
  
//  clear_write_log ();
//  W.log_start_pos = log_pos;
  assert (W.log_rptr == W.log_wptr && !W.log_endw);
  memcpy (alloc_log_event (LEV_ROTATE_TO, 36, now), &new_rotate_to, 36);
  memcpy (alloc_log_event (LEV_ROTATE_FROM, 36, now), &new_rotate_from, 36);
  if (W.log_endw) {
    assert (W.log_endw >= W.log_rptr && W.log_wptr >= W.log_start && (W.log_endw - W.log_rptr) + (W.log_wptr - W.log_start) == 72);
  } else {
    assert (W.log_wptr == W.log_rptr + 72);
  }
  if (!binlog_cyclic_mode) {
    W.log_rptr = W.log_wptr;
    W.log_endw = 0;
  } else {
    W.log_rptr = W.log_wptr = W.log_start;
    W.log_endw = 0;
  }

  W.log_wcrypt_ptr = W.log_rptr;

  now = keep_now;

  return 1;
}


static void process_timestamp_event (struct lev_generic *E) {
  if (!log_first_ts) {
    log_first_ts = E->a;
  }
  if (log_read_until > E->a) {
    fprintf (stderr, "time goes back from %d to %d in log file\n", log_read_until, E->a);
  }
  log_last_ts = log_read_until = E->a;
  if (log_set_now) {
    now = log_read_until;
  }
  if (E->a >= log_time_cutoff && !log_scan_mode) {
    log_cutoff_pos = log_cur_pos ();
    log_scan_mode = 1;
    if (verbosity) {
      fprintf (stderr, "reached timestamp %d above cutoff %d at binlog position %lld, entering scan mode 1\n",
        E->a, log_time_cutoff, log_cutoff_pos);
    }
  }
}


static void process_crc_event (struct lev_generic *E, int offset) {
  relax_log_crc32 (0);
  if (!(disable_crc32 & 2)) {
    long long cur_pos = log_cur_pos() + offset;
    if (~log_crc32_complement != ((struct lev_crc32 *) E)->crc32) {
      fprintf (stderr, "crc mismatch at binlog position %lld, file %s offset %lld\n", cur_pos, binlogname, cur_pos - log_start_pos + log_headers_size);
      assert (~log_crc32_complement == ((struct lev_crc32 *) E)->crc32);
    }
    if (cur_pos != ((struct lev_crc32 *) E)->pos) {
      fprintf (stderr, "position at binlog position %lld, file %s offset %lld: expected position %lld\n", cur_pos, binlogname, cur_pos - log_start_pos + log_headers_size, ((struct lev_crc32 *) E)->pos);
      assert (cur_pos == ((struct lev_crc32 *) E)->pos);
    }
  }
  process_timestamp_event (E);
}


static struct lev_rotate_to last_rotate_to;

int default_replay_logevent (struct lev_generic *E, int size) {
  int t, s;
  switch (E->type) {
/* TODO:
  case LEV_ROTATED_FROM:
  case LEV_ROTATE_TO:
*/
  case LEV_NOOP:
    if (size < 4) { return -2; }
    return 4;
  case LEV_CRC32:
    if (size < sizeof (struct lev_crc32)) { return -2; }
    process_crc_event (E, 0);
    return sizeof (struct lev_crc32);
  case LEV_TIMESTAMP:
    if (size < 8) { return -2; }
    process_timestamp_event (E);
    return 8;
  case LEV_START:
    if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
    s = 24 + ((E->b + 3) & -4);
    if (size < s) { return -2; }
    t = set_schema (E->a);
    if (t < 0) { return t; }
    else if (replay_logevent != default_replay_logevent) {
      return replay_logevent (E, s);
    }
    return s;
  case LEV_ROTATE_FROM:
    if (size < 36) { return -2; }
    process_crc_event (E, 0);
    assert (need_switch_binlog == 2);
    need_switch_binlog = 0;
    if (!cur_binlog_file_hash) {
      cbuff.prev_hash = prev_binlog_file_hash;
      cbuff.stpos = log_cur_pos ();
      cbuff.crc32 = last_rotate_to.crc32;
      md5 ((unsigned char *)&cbuff, 20, MDBUF);
      cur_binlog_file_hash = *(hash_t *)MDBUF;
    }

    assert (log_cur_pos () == ((struct lev_rotate_from *)E)->cur_log_pos);
    assert (((struct lev_rotate_from *)E)->cur_log_pos == last_rotate_to.next_log_pos);
    assert (((struct lev_rotate_from *)E)->timestamp == last_rotate_to.timestamp);
    assert (((struct lev_rotate_from *)E)->prev_log_hash == last_rotate_to.cur_log_hash);
    assert (((struct lev_rotate_from *)E)->cur_log_hash == last_rotate_to.next_log_hash);
    assert (prev_binlog_file_hash == last_rotate_to.cur_log_hash);
    assert (cur_binlog_file_hash == last_rotate_to.next_log_hash);
    assert (prev_binlog_file_hash && cur_binlog_file_hash);

    return 36;
  case LEV_ROTATE_TO:
    if (size < 36) { return -2; }
    assert (size == 36);
    process_crc_event (E, 36);
    memcpy (&last_rotate_to, E, 36);
    assert (R.log_rptr + 36 == R.log_wptr);
    assert (log_cur_pos() + 36 == last_rotate_to.next_log_pos);
    prev_binlog_file_hash = calc_binlog_hash (binlog_fd, log_cur_pos () + 36 - log_start_pos + binlog_headers * 4096, R.log_start, R.log_wptr - R.log_start, R.log_wptr - R.log_start);
    assert (((struct lev_rotate_to *)E)->cur_log_hash);
    assert (((struct lev_rotate_to *)E)->next_log_hash);
    assert (((struct lev_rotate_to *)E)->cur_log_hash == cur_binlog_file_hash);
    need_switch_binlog = 1;
    return 36;
  case LEV_TAG:
    for (t = 0; t < 16; t++) {
      if (binlog_tag[t]) {
        fprintf (stderr, "%s: handle LEV_TAG error at position %lld, binlog tag is already set.\n", __func__, log_cur_pos ());
        return -1;
      }
    }
    memcpy (binlog_tag, ((struct lev_tag *) E)->tag, 16);
    for (t = 0; t < 16; t++) {
      if (binlog_tag[t]) {
        break;
      }
    }
    if (t >= 16) {
      fprintf (stderr, "%s: handle LEV_TAG error at position %lld, zero tag.\n", __func__, log_cur_pos ());
      return -1;
    }
    return 20;
  case LEV_CBINLOG_END:
    return -13;
  }
  fprintf (stderr, "unknown logevent type %08x read at position %lld before START log event\n", E->type, log_cur_pos());
  return -1;
}

#define	LOG_READ_THRESHOLD	1024

int binlog_crc32_verbosity_level = 3;

replay_logevent_vector_t replay_logevent = default_replay_logevent;
  
int replay_log (int cutoff_time, int set_now) {
  int r, s = 0, size_err = 0, iteration = 0;
  if (binlog_fd < 0) { return 0; }

  log_time_cutoff = cutoff_time ? cutoff_time : 0x7fffffff;
  log_scan_mode = 0;
  log_cutoff_pos = -1;
  if (!log_pos) {
    log_last_ts = 0;
  }
  log_set_now = set_now;

  if (!log_schema && !replay_logevent) {
    replay_logevent = default_replay_logevent;
  }

  if (log_set_now) {
    log_true_now = now;
    now = log_last_ts;
  }

  while (1) {
    if (R.log_rptr >= R.log_wptr) {
      if (R.log_rptr > R.log_wptr) {
        fprintf (stderr, "log read pointer advanced after end of file for %ld bytes\n", (long) (R.log_rptr - R.log_wptr));
      }
      relax_log_crc32 (0);
      r = s = R.log_end - R.log_start;
      if (log_limit_pos >= 0 && log_pos + s > log_limit_pos) {
        s = log_limit_pos - log_pos;
        assert (s >= 0);
      }

      if (binlog_zipped) {
        if (kfs_bz_decode (Binlog, log_pos - log_start_pos, R.log_start, &r, NULL) < 0) {
          exit (1);
        }
        if (r > s) {
          r = s;
        }
      } else {
        r = read (binlog_fd, R.log_start, s);
        if (verbosity > 0 && (r || iteration)) {
          fprintf (stderr, "read %d bytes from binlog %s\n", r, binlogname);
        }
        assert (r >= 0);
        kfs_buffer_crypt (Binlog, R.log_start, r, log_pos - log_start_pos + log_headers_size);
      }

      if (binlog_check_mode) {
        memcpy (LogCopyBuffer, R.log_start, r);
      }

      R.log_rptr = R.log_start;
      R.log_wptr = R.log_start + r;
      log_pos += r;

      if (need_switch_binlog == 1) {
        assert (r == 0);
        assert (Binlog);
        kfs_file_handle_t NextBinlog = next_binlog (Binlog);

        if (!NextBinlog) {
          update_replica (Binlog->info->replica, 0);
          NextBinlog = next_binlog (Binlog);
        }
        if (!NextBinlog) {
          fprintf (stderr, "fatal: cannot find next binlog file after %s, log position %lld\n", binlogname, log_pos);
          exit (1);
        }

        close_binlog (Binlog, 1);
        Binlog = NextBinlog;

        assert (Binlog);

        if (verbosity > 0) {
          fprintf (stderr, "switched from binlog file %s to %s at position %lld\n", binlogname, Binlog->info->filename, log_pos);
        }

        binlogname = Binlog->info->filename;

        assert (log_last_ts == last_rotate_to.timestamp);
        assert (last_rotate_to.next_log_pos == log_pos);

        init_log_data (log_pos, log_last_ts, ~log_crc32_complement);

        need_switch_binlog = 2;

        continue;
      }

      if (!r) { break; }
    }
    if (R.log_rptr >= R.log_end - LOG_READ_THRESHOLD || size_err) {
      relax_log_crc32 (0);
      if (R.log_start != R.log_rptr) {
        memcpy (R.log_start, R.log_rptr, R.log_end - R.log_rptr);
        if (binlog_check_mode) {
          memcpy (LogCopyBuffer, R.log_start, R.log_end - R.log_rptr);
        }
      }
      R.log_wptr -= (R.log_rptr - R.log_start);
      R.log_rptr = R.log_start;
      r = s = R.log_end - R.log_wptr;
      if (log_limit_pos >= 0 && log_pos + s > log_limit_pos) {
        s = log_limit_pos - log_pos;
        assert (s >= 0);
      }

      if (binlog_zipped) {
        assert (R.log_wptr <= R.log_start + ZLOG_BUFFER_SIZE);
        if (kfs_bz_decode (Binlog, log_pos - log_start_pos, R.log_wptr, &r, NULL) < 0) {
          exit (1);
        }
        if (r > s) {
          r = s;
        }
      } else {
        r = read (binlog_fd, R.log_wptr, s);
        if (verbosity > 0 && (r || iteration)) {
          fprintf (stderr, "read %d bytes from binlog %s\n", r, binlogname);
        }
        if (r < 0) {
          fprintf (stderr, "Error reading binlog: %m\n");
        }
        assert (r >= 0);
        kfs_buffer_crypt (Binlog, R.log_wptr, r, log_pos - log_start_pos + log_headers_size);
      }

      if (binlog_check_mode) {
        memcpy (LogCopyBuffer + (R.log_wptr - R.log_start), R.log_wptr, r);
      }
      R.log_wptr += r;
      log_pos += r;
      //      if (!r) { break; }
    }

    iteration++;

    r = (R.log_wptr - R.log_rptr) & -4;
    if (r < 4) { break; }

    s = replay_logevent ((struct lev_generic *) R.log_rptr, r);
    if (s == -2 && size_err < 10) {
      size_err++;
      continue;
    }
    if (s == -13 && binlog_cyclic_mode) {
      binlog_end_found ++;
      break;
    }
    size_err = 0;
    if (s < 0) { 
      if (verbosity > 0 && (s != -2 || !binlog_disabled)) {
        fprintf (stderr, "replay_logevent(%p,%d) returned error %d\n", R.log_rptr, r, s);
      }
      break;
    }

    if (binlog_check_mode) {
      if (memcmp (LogCopyBuffer + (R.log_rptr - R.log_start), R.log_rptr, (s + 3) & -4)) {
        binlog_check_errors++;
        fprintf (stderr, "WARNING: record %08x changed binlog buffer contents at position %lld (record size %d)\n", 
            *(unsigned *)(LogCopyBuffer + (R.log_rptr - R.log_start)), log_cur_pos (), s);
        assert (binlog_check_mode <= 2 || !binlog_check_errors);
      }
    }

    R.log_rptr += (s + 3) & -4;
  }

  if (binlog_cyclic_mode && !binlog_end_found) {
    fprintf (stderr, "LEV_CBINLOG_END not found\n");
    return -1;
  }

  if (binlog_cyclic_mode) {
    R.log_wptr -= 4;
    log_pos -= 4;
  }

  relax_log_crc32 (0);

  if (verbosity >= binlog_crc32_verbosity_level && !(disable_crc32 & 2)) {
    fprintf (stderr, "computed binlog crc32 %08x up to position %lld\n", ~log_crc32_complement, log_crc32_pos);
  }

  if (s == -2 && binlog_disabled) {
    log_readto_pos = log_pos;
    if (log_set_now) {
      now = log_true_now;
    }
    return 0;
  }

  if (R.log_rptr < R.log_wptr && verbosity >= 0 && !binlog_end_found) {
    r = R.log_wptr - R.log_rptr;
    log_pos -= r;
    fprintf (stderr, "replay binlog: %d bytes left unread at position %lld (timestamp %d)\n", r, log_pos, log_read_until);
    if (r) {
      fprintf (stderr, "bytes at %lld (0x%08llx): ", log_pos, log_pos);
      for (r = 0; r < 16 && r < R.log_wptr - R.log_rptr; r++) {
        fprintf (stderr, "%02x ", (unsigned char) R.log_rptr[r]);
      }
      fprintf (stderr, "\n");
    }
  }
  log_readto_pos = log_pos;
  if (log_set_now) {
    now = log_true_now;
  }

  if (R.log_rptr < R.log_wptr && binlog_repairing > 0 && log_pos >= binlog_size - binlog_repairing && !binlog_end_found) {
    binlog_repairing = log_pos - binlog_size;
    fprintf (stderr, "REPAIRING binlog at position %lld (%d bytes to cut off)\n", log_pos, -binlog_repairing);
    binlog_size = log_pos;
    return -2;
  } else {
    binlog_repairing = 0;
  }

  if ((R.log_rptr == R.log_wptr || binlog_end_found) && Binlog && log_limit_pos < 0 && !binlog_is_last (Binlog)) {
    fprintf (stderr, "replay binlog: last read file %s is not last in chain, possibly truncated\n", Binlog->info->filename);
    return -1;
  }

  if (binlog_cyclic_mode) {
    r = R.log_wptr - R.log_rptr;
    log_pos -= r;
  }

  return (R.log_rptr < R.log_wptr && !binlog_end_found) ? -1 : 0;
}

static void clear_one_log (struct log_buffer *B, char *buffer, int size) {
  if (!buffer || !size) {
    memset (B, 0, sizeof (struct log_buffer));
    return;
  }
  B->log_rptr = B->log_wptr = B->log_start = buffer + ((16 - (long) buffer) & 15);
  B->log_end = B->log_start + size;
  B->log_endw = 0;
  B->log_last_endw = 0;
  B->log_start_pos = 0;
  B->log_wcrypt_ptr = B->log_start;
} 

void clear_read_log (void) {
  int readlog_size = ULOG_BUFFER_SIZE;
  if (binlog_zipped) {
    readlog_size += ZLOG_BUFFER_SIZE;
  }
  clear_one_log (&R, LogBuffer, readlog_size);
}

void clear_write_log (void) {
  if (binlog_disabled) {
    clear_one_log (&W, LogSlaveBuffer, SLOG_BUFFER_SIZE);
  } else {
    /* wait for pending aio_write termination */
    int cnt = 0;
    while (binlog_write_active) {
      flush_binlog ();
      usleep (10000);
      if (++cnt > 300) {
        fprintf (stderr, "cannot write binlog for 3 seconds!");
        exit (3);
      }
    }
    if (cnt > 0) {
      fprintf (stderr, "clear_write_log: had to wait %d milliseconds for aio_write termination\n", cnt);
    }

    assert (!aio_write_start);

    clear_one_log (&W, LogBuffer, ULOG_BUFFER_SIZE);
  }
}

int is_write_log_empty (void) {
  return W.log_rptr == W.log_start && W.log_wptr == W.log_start;
}

void clear_log (void) {
  clear_read_log ();
  clear_write_log ();
//  log_last_ts = 0;
//  log_pos = 0;
  binlog_fd = -1;
  binlog_size = 0;
}

void set_log_data (int logfd, long long logsize) {
  binlog_fd = logfd;
  binlog_size = logsize;
  if (load_binlog_headers (logfd) < 0) {
    fprintf (stderr, "bad binlog headers (fd=%d)\n", logfd);
    exit (1);
  }
  if (binlog_check_mode && !LogCopyBuffer) {
    LogCopyBuffer = malloc (ULOG_BUFFER_SIZE + 16);
  }
  W.log_start_pos = log_pos;
}

void init_log_data (long long new_log_pos, int log_timestamp, unsigned log_crc32) {
  assert (Binlog);
  binlog_zipped = Binlog->info->flags & 16;
  binlogname = Binlog->info->filename;
  set_log_data (Binlog->fd, binlog_zipped ? ((kfs_binlog_zip_header_t *) Binlog->info->start)->orig_file_size : Binlog->info->file_size);
  log_seek (new_log_pos, log_timestamp, log_crc32);
  next_binlog_rotate_pos = log_start_pos + max_binlog_size;
}


void log_seek (long long new_log_pos, int log_timestamp, unsigned log_crc32) {
  long long log_file_pos = new_log_pos - log_start_pos + log_headers_size;
  if (log_file_pos < 0 || (!binlog_zipped && lseek (binlog_fd, log_file_pos, SEEK_SET) < 0)) {
    fprintf (stderr, "cannot seek file position %lld in binlog file %d: %m\n", new_log_pos, binlog_fd);
    exit (1);
  }
  clear_read_log ();
  log_first_ts = log_read_until = log_last_ts = log_timestamp;
  log_pos = new_log_pos;
  R.log_start_pos = log_pos;
  if (log_crc32 || !new_log_pos) {
    log_crc32_pos = new_log_pos;
    log_crc32_complement = ~log_crc32;
  } else {
    disable_crc32 = 7;
  }
}

void sync_binlog (int mode) {
  int res, err;
  if (!binlogname || binlog_disabled) {
    return;
  }
  if (binlog_sync_active) {
    res = err = aio_error (&binlog_sync_aiocb);
    if (res == EINPROGRESS && now > binlog_sync_active + 120) {
      fprintf (stderr, "%d binlog aio_sync didn't complete for 120 seconds, invoking fsync()!\n", now);
      aio_cancel (binlog_sync_aiocb.aio_fildes, &binlog_sync_aiocb);
      fsync (binlog_sync_aiocb.aio_fildes);
      exit (3);
    }
    if (res != EINPROGRESS) {
      binlog_sync_active = 0;
      res = aio_return (&binlog_sync_aiocb);
      if (res >= 0) {
        binlog_sync_last = now;
        if (binlog_sync_aiocb.aio_fildes != binlog_fd && binlog_write_aiocb.aio_fildes != binlog_sync_aiocb.aio_fildes) {
          close (binlog_sync_aiocb.aio_fildes);
        }
        binlog_sync_aiocb.aio_fildes = -1;
        if (verbosity > 1) {
          fprintf (stderr, "%d aio_fsync() for binlog completed successfully\n", now);
        }
      } else {
        errno = err;
        fprintf (stderr, "%d error syncing binlog %s: %m\n", now, binlogname);
        fsync (binlog_sync_aiocb.aio_fildes);
        exit (3);
      }
    }
  }
  if (!binlog_sync_active && mode == 1) {
    memset (&binlog_sync_aiocb, 0, sizeof (struct aiocb));
    binlog_sync_aiocb.aio_fildes = binlog_fd;
    binlog_sync_aiocb.aio_sigevent.sigev_notify = SIGEV_NONE;
    if (aio_fsync (O_DSYNC, &binlog_sync_aiocb) < 0) {
      fprintf (stderr, "%d aio_fsync() for binlog failed: %m\n", now);
      fsync (binlog_fd);
      exit (3);
    }
    binlog_sync_active = now;
    if (verbosity > 1) {
      fprintf (stderr, "%d queued aio_fsync() for binlog\n", now);
    }
  }
  if (mode >= 2) {
    if (fsync (binlog_fd) < 0) {
      fprintf (stderr, "%d error syncing binlog: %m", now);
    } else {
      binlog_sync_last = now;
      if (verbosity > 1) {
        fprintf (stderr, "%d binlog sync ok\n", now);
      }
    }
  }
}

void flush_binlog_forced (int force_sync) {
  int w, s, t;
  long long log_file_pos;
  if (!binlogname || binlog_disabled) {
    return;
  }
  if (W.log_rptr == W.log_wptr) {
    sync_binlog (force_sync * 2);
    return;
  }
  if (verbosity > 0) {
    fprintf (stderr, "%d flush_binlog()\n", now);
  }
  if (W.log_endw) {
    assert (W.log_wptr < W.log_rptr && W.log_rptr <= W.log_endw); 
    s = W.log_endw - W.log_rptr;
    if (s > 0) {
      log_file_pos = log_pos - log_start_pos + log_headers_size;
      assert (lseek (binlog_fd, log_file_pos, SEEK_SET) == log_file_pos);
      if (W.log_rptr <= W.log_wcrypt_ptr && W.log_wcrypt_ptr < W.log_endw) {
        t = W.log_endw - W.log_wcrypt_ptr;
        relax_write_log_crc32 (); /* relax crc32 before encryption */
        kfs_buffer_crypt (Binlog, W.log_wcrypt_ptr, t, log_file_pos + (W.log_wcrypt_ptr - W.log_rptr));
        W.log_wcrypt_ptr = W.log_start;
      }
      w = write (binlog_fd, W.log_rptr, s);
      if (w < 0) {
        fprintf (stderr, "error writing %d bytes at %lld (file position %lld) to %s: %m\n", s, log_pos, log_file_pos, binlogname);
        return;
      }
      W.log_rptr += w;
      if (Binlog && !binlog_cyclic_mode) {
        //Binlog->info->log_pos += w;
        Binlog->info->file_size += w;
      }
      log_pos += w;
      if (w < s) {
        return;
      }
    }
    W.log_rptr = W.log_start;
    W.log_endw = 0;
  }
  assert (W.log_rptr <= W.log_wptr);
  s = W.log_wptr - W.log_rptr;
  if (s > 0) {
    log_file_pos = log_pos - log_start_pos + log_headers_size;
    assert (lseek (binlog_fd, log_file_pos, SEEK_SET) == log_file_pos);
    if (W.log_rptr <= W.log_wcrypt_ptr && W.log_wcrypt_ptr < W.log_wptr) {
      t = W.log_wptr - W.log_wcrypt_ptr;
      relax_write_log_crc32 (); /* relax crc32 before encryption */
      kfs_buffer_crypt (Binlog, W.log_wcrypt_ptr, t, log_file_pos + (W.log_wcrypt_ptr - W.log_rptr));
      W.log_wcrypt_ptr = W.log_wptr;
    }
    w = write (binlog_fd, W.log_rptr, s);
    if (w < 0) {
      int binlog_write_errno = errno;
      kprintf ("error writing %d bytes at %lld (file position %lld) to %s: %m\n", s, log_pos, log_file_pos, binlogname);
      assert (binlog_write_errno == EINTR || binlog_write_errno == EAGAIN);
      return;
    }
    W.log_rptr += w;
    if (Binlog && !binlog_cyclic_mode) {
      //Binlog->info->log_pos += w;
      Binlog->info->file_size += w;
    }
    log_pos += w;
  }
  sync_binlog (1 + force_sync);
}

void flush_binlog (void) {
  int res, err;
  int force_sync = 0;
  if (!binlogname || binlog_disabled) {
    return;
  }
  if (binlog_write_active) {
    res = err = aio_error (&binlog_write_aiocb);
    if (res == EINPROGRESS && now > binlog_write_active + 120) {
      fprintf (stderr, "%d binlog aio_write didn't complete for 120 seconds, invoking write()!\n", now);
      aio_cancel (binlog_write_aiocb.aio_fildes, &binlog_write_aiocb);
      flush_binlog_forced (1);
      exit (3);
    }
    if (res != EINPROGRESS) {
      binlog_write_active = 0;
      res = aio_return (&binlog_write_aiocb);
      if (res >= 0) {
        assert (res <= binlog_write_aiocb.aio_nbytes);
        binlog_write_last = now;
        if (binlog_write_aiocb.aio_fildes != binlog_fd && binlog_write_aiocb.aio_fildes != binlog_sync_aiocb.aio_fildes) {
          close (binlog_write_aiocb.aio_fildes);
        }
        binlog_write_aiocb.aio_fildes = -1;
        assert (aio_write_start);

        if (W.log_rptr == aio_write_start) {
          log_pos += res;
          W.log_rptr += res;
          if (Binlog && !binlog_cyclic_mode) {
            //Binlog->info->log_pos += res;
            Binlog->info->file_size += res;
          }
          assert ((!W.log_endw && W.log_rptr <= W.log_wptr) || W.log_rptr <= W.log_endw);
          if (W.log_rptr == W.log_endw) {
            W.log_rptr = W.log_start;
            W.log_endw = 0;
          }
        }

        aio_write_start = 0;
        force_sync = 1;

        if (verbosity > 1) {
          fprintf (stderr, "%d aio_write() for binlog (position=%lld, size=%ld) completed successfully, %d bytes written\n", now, (long long) binlog_write_aiocb.aio_offset, (long) binlog_write_aiocb.aio_nbytes, res);
        }
      } else {
        errno = err;
        fprintf (stderr, "%d error writing binlog %s (position=%lld, size=%ld): %m\n", now, binlogname, (long long) binlog_write_aiocb.aio_offset, (long) binlog_write_aiocb.aio_nbytes);
        flush_binlog_forced (1);
        exit (3);
      }
    }
  }
  if (!binlog_write_active && W.log_rptr != W.log_wptr) {
    long long log_file_pos = log_pos - log_start_pos + log_headers_size;
    int w, t;
    // assert (lseek (binlog_fd, log_file_pos, SEEK_SET) == log_file_pos);

    if (W.log_endw) {
      if (W.log_rptr <= W.log_wcrypt_ptr && W.log_wcrypt_ptr < W.log_endw) {
        t = W.log_endw - W.log_wcrypt_ptr;
        relax_write_log_crc32 (); /* relax crc32 before encryption */
        kfs_buffer_crypt (Binlog, W.log_wcrypt_ptr, t, log_file_pos + (W.log_wcrypt_ptr - W.log_rptr));
        W.log_wcrypt_ptr = W.log_start;
      }
      w = W.log_endw - W.log_rptr;
      if (!w) {
        W.log_rptr = W.log_start;
        W.log_endw = 0;
      }
    }
    if (!W.log_endw) {
      if (W.log_rptr <= W.log_wcrypt_ptr && W.log_wcrypt_ptr < W.log_wptr) {
        t = W.log_wptr - W.log_wcrypt_ptr;
        relax_write_log_crc32 (); /* relax crc32 before encryption */
        kfs_buffer_crypt (Binlog, W.log_wcrypt_ptr, t, log_file_pos + (W.log_wcrypt_ptr - W.log_rptr));
        W.log_wcrypt_ptr = W.log_wptr;
      }
      w = W.log_wptr - W.log_rptr;
    }

    assert (w >= 0);

    if (w > 0) {
      memset (&binlog_write_aiocb, 0, sizeof (struct aiocb));
      binlog_write_aiocb.aio_fildes = binlog_fd;
      binlog_write_aiocb.aio_offset = log_file_pos;
      binlog_write_aiocb.aio_nbytes = w;
      binlog_write_aiocb.aio_buf = W.log_rptr;
      binlog_write_aiocb.aio_sigevent.sigev_notify = SIGEV_NONE;

      if (aio_write (&binlog_write_aiocb) < 0) {
        fprintf (stderr, "%d aio_write() for binlog (position=%lld, size=%d) failed: %m\n", now, log_file_pos, w);
        flush_binlog_forced (1);
        exit (3);
      }

      binlog_write_active = now;
      aio_write_start = W.log_rptr;

      if (verbosity > 1) {
        fprintf (stderr, "%d queued aio_write() for binlog (position=%lld, size=%d)\n", now, log_file_pos, w);
      }
    }
  }

  sync_binlog (force_sync);

}

void flush_cbinlog (int force_sync) {
  assert (binlog_cyclic_mode);
  assert (W.log_rptr == W.log_start);
  assert (W.log_wptr + 4 < W.log_end);
  if (W.log_wptr != W.log_rptr) {
    relax_write_log_crc32 ();
    *(int *)W.log_wptr = LEV_CBINLOG_END;
    long long pos = log_pos - log_start_pos + log_headers_size;
    int w = W.log_wptr - W.log_rptr;
    assert (w >= 0);
    assert (binlog_fd >= 0);
    //fprintf (stderr, "pos = %lld, w = %d\n", pos, w);
    //fprintf (stderr, "fsize = %lld, binlog_fd = %d, Binlog->fd = %d, %m, %d, %s, name = %s\n", (long long)lseek (binlog_fd, 0, SEEK_END), binlog_fd, Binlog->fd, errno, strerror (errno), binlogname);    
    if (W.log_rptr <= W.log_wcrypt_ptr && W.log_wcrypt_ptr < W.log_wptr) {
      int t = W.log_wptr - W.log_wcrypt_ptr;
      kfs_buffer_crypt (Binlog, W.log_wcrypt_ptr, t, pos + (W.log_wcrypt_ptr - W.log_rptr));
      W.log_wcrypt_ptr = W.log_wptr;
    }
    kfs_buffer_crypt (Binlog, W.log_wptr, 4, pos + w);
    long long r = pwrite (binlog_fd, W.log_rptr, w + 4, pos);
    if (r != w + 4) {
      fprintf (stderr, "%s: Error %m\n", __func__);
    }
    log_pos += w;
    W.log_wptr = W.log_rptr = W.log_wcrypt_ptr = W.log_start;
  }
  sync_binlog (force_sync);
}

/* new log events */

static inline void alloc_log_crc32_event (void) {
  unsigned crc32 = relax_write_log_crc32();
  long long pos = log_crc32_pos;

  if (!binlog_cyclic_mode) {
    if (!binlog_disabled && pos >= next_binlog_rotate_pos && force_new_binlog() > 0) {
      return;
    }
  } else {
    if (!binlog_disabled && log_pos >= next_binlog_rotate_pos && force_new_binlog() > 0) {
      return;
    }
  }

  struct lev_crc32 *E = alloc_log_event (LEV_CRC32, sizeof (struct lev_crc32), 0);
  E->pos = pos;
  E->crc32 = crc32;

  if (verbosity > 2) {
    fprintf (stderr, "written log crc32 event: timestamp=%d, pos=%lld, crc32=%08x\n", E->timestamp, pos, crc32);
  }

  bytes_after_crc32 = 0;
}

static inline void alloc_log_timestamp_event (void) {
  if (bytes_after_crc32 >= log_crc32_interval2 && !(disable_crc32 & 1)) {
    alloc_log_crc32_event ();
  } else if (!(disable_ts & 1)) {
    alloc_log_event (LEV_TIMESTAMP, 0, 0);
  }
}

void *alloc_log_event (int type, int bytes, int arg1) {
  struct lev_generic *EV;
  if (!binlogname) {
    return 0;
  }
  int adj_bytes = -bytes & 3;  
  bytes = (bytes + 3) & -4;
  if (type == LEV_TIMESTAMP) {
    bytes = 8;
    arg1 = now ? now : time(0);
    if (log_last_ts < arg1) {
      log_last_ts = arg1;
    }
  } else if (type == LEV_CRC32) {
    arg1 = now ? now : time(0);
    if (log_last_ts < arg1) {
      log_last_ts = arg1;
    }
  } else if (now > log_last_ts + log_ts_interval || now >= log_next_exact_ts) {
    log_next_exact_ts = now - now % log_ts_exact_interval + log_ts_exact_interval;
    alloc_log_timestamp_event ();
  } else if (bytes_after_crc32 >= log_crc32_interval && !(disable_crc32 & 1)) {
    alloc_log_crc32_event ();
  }
  //fprintf (stderr, "type = 0x%08x, bytes = %d, W->log_wptr = %p, W->log_start = %p, W->log_end = %p\n", type, bytes, W.log_wptr, W.log_start, W.log_end);
  if (bytes > W.log_end - W.log_wptr) {
    relax_write_log_crc32();
    W.log_endw = W.log_wptr;
    W.log_last_endw = W.log_endw;
    W.log_wptr = W.log_start;
  }
  if ((W.log_endw && W.log_wptr + bytes >= W.log_rptr) || (W.log_wptr < aio_write_start && W.log_wptr + bytes >= aio_write_start)) {
    if (!binlog_disabled) {
      fprintf (stderr, "fatal: binlog buffer overflow!\n");
      exit (3);
    } else {
      fprintf (stderr, "binlog overflow, buffer cleaned (binlog commit disabled)\n");
      W.log_rptr = W.log_wptr = W.log_start;
      W.log_endw = 0;
    }
  }

  EV = (struct lev_generic *) W.log_wptr;

  W.log_wptr += bytes;
  bytes_after_crc32 += bytes;

  EV->type = type;
  if (bytes >= 8) {
    EV->a = arg1;
  }

  if (adj_bytes) {
    memset ((char *) EV + bytes - adj_bytes, adj_bytes, adj_bytes);
  }

  return EV;
}

void unalloc_log_event (int bytes) {
  bytes = (bytes + 3) & -4;
  W.log_wptr -= bytes;
  bytes_after_crc32 -= bytes;
}

void flush_binlog_ts (void) {
  if (!binlogname || binlog_disabled) {
    return;
  }
  if (log_last_ts) {
    if (!(disable_crc32 & 1)) {
      alloc_log_crc32_event ();
    } else if (!(disable_ts & 1)) {
      alloc_log_timestamp_event ();
    }
  }
  flush_binlog_forced (1);
}

void flush_binlog_last (void) {
  if (log_cur_pos () != log_readto_pos) {
    flush_binlog_ts ();
  } else {
    flush_binlog_forced (1);
  }
}

int compute_uncommitted_log_bytes (void) {
  int log_uncommitted = W.log_wptr - W.log_rptr;
  if (log_uncommitted < 0) {
    log_uncommitted += W.log_endw - W.log_start;
  }
  return log_uncommitted;
}

/* used for implementing -r key : use epoll_pre_event = read_new_events */
void read_new_events (void) {
  int res = replay_log (0, 1);
  if (res < 0) {
    fprintf (stderr, "fatal: ceased reading binlog updates from %d.\n", binlog_fd);
    binlog_fd = -1;
  }
}

int read_binlog_data (char *buf, int len) {
  int xlen = W.log_last_endw <= W.log_wptr ? W.log_wptr - W.log_start : W.log_wptr - W.log_start + W.log_last_endw - W.log_wptr;
  if (xlen > len) {
    xlen = len;
  }
  len = xlen;
  if (W.log_wptr - W.log_start >= len) {
    memcpy (buf, W.log_wptr - len, len);
    return xlen;
  } else {
    len -= W.log_wptr - W.log_start;
    memcpy (buf + len, W.log_start, W.log_wptr - W.log_start);
  }
  if (W.log_last_endw) {
    if (W.log_last_endw > W.log_wptr) {
      if (W.log_last_endw - W.log_wptr >= len) {
        memcpy (buf, W.log_last_endw - len, len);
        return xlen;
      } else {
        len -= W.log_last_endw - W.log_wptr;
        memcpy (buf, W.log_wptr, W.log_last_endw - W.log_wptr);
      }
    }
  }
  assert (!len);
  return xlen;
}

int match_rotate_logevents (struct lev_rotate_to *RT, struct lev_rotate_from *RF) {
  if (RT->type != LEV_ROTATE_TO) {
    fprintf (stderr, "%s: invalid RT->type (0x%08x), expected LEV_ROTATE_TO.\n", __func__, RT->type);
    return 0;
  }
  if (RF->type != LEV_ROTATE_FROM) {
    fprintf (stderr, "%s: invalid RF->type (0x%08x), expected LEV_ROTATE_FROM.\n", __func__, RT->type);
    return 0;
  }

  if (!RT->cur_log_hash || !RT->next_log_hash) {
    fprintf (stderr, "%s: invalid (zero) hash.\n", __func__);
    return 0;
  }

  if (RT->timestamp != RF->timestamp) {
    fprintf (stderr, "%s: timestamp is mismatched.\n", __func__);
    return 0;
  }

  if (RT->next_log_pos != RF->cur_log_pos) {
    fprintf (stderr, "%s: log position is mismatched.\n", __func__);
    return 0;
  }

  if (~crc32_partial (RT, 36, ~RT->crc32) != RF->crc32) {
    fprintf (stderr, "%s: crc32 is mismatched.\n", __func__);
    return 0;
  }

  if (RT->cur_log_hash != RF->prev_log_hash || RT->next_log_hash != RF->cur_log_hash) {
    fprintf (stderr, "%s: hash is mismatched.\n", __func__);
    return 0;
  }

  struct {
    hash_t prev_hash;
    long long stpos;
    unsigned crc32;
  } cbuff;
  unsigned char MDBUF[16];
  cbuff.prev_hash = RF->prev_log_hash;
  cbuff.stpos = RF->cur_log_pos;
  cbuff.crc32 = RT->crc32;
  md5 ((unsigned char *) &cbuff, 20, MDBUF);
  if ((*(hash_t *) MDBUF) != RF->cur_log_hash) {
    fprintf (stderr, "%s: computed hash isn't equal to RF->cur_log_hash.\n", __func__);
    return 0;
  }

  return 1;
}
