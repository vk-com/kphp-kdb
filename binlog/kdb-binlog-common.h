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

#ifndef __KDB_BINLOG_COMMON_H__
#define __KDB_BINLOG_COMMON_H__

#pragma	pack(push,4)
/* if a KFS header is present, first four bytes will contain KFS_FILE_MAGIC; then 4Kbytes have to be skipped */

#ifndef KFS_FILE_MAGIC
#define	KFS_FILE_MAGIC	0x0473664b
#endif

#define DEFAULT_MAX_BINLOG_SIZE	((1LL << 30) - (1LL << 20))

#ifndef	hash_t
typedef unsigned long long hash_t;
#define	hash_t	hash_t
#endif

typedef unsigned int lev_type_t;
#define	LEV_TYPE(x)	(*((lev_type_t *) (x)))
#define	LEV_DATA(x)	(*((struct lev_generic *) (x)))
#define	LEV_IS_GENERIC(t)	(((t) & 0xff000000) == 0x04000000)

// first entry in any binlog file: sets schema, but doesn't clear data if occurs twice
#define	LEV_START	0x044c644b
// last entry in a binlog file, no further entries possible (only after LEV_ROTATE_TO)
#define	LEV_END		0x04b6ac2d
// for alignment
#define LEV_NOOP	0x04ba3de4
// timestamp (unix time) for all following records
#define	LEV_TIMESTAMP	0x04d931a8
// drops all data ('truncate table'), useful in the very first binlog file to declare initial state
#define LEV_EMPTY	0x0455004e
// must be immediately after LEV_START; defines a 16-byte `random tag` for this virtual binlog
#define LEV_TAG		0x04476154
// declares the slice to contain unknown data at this point; a snapshot must be available for some later binlog position, otherwise the slice will be unworkable
#define	LEV_UNKNOWN_INIT	0x049a53cc
// declares that a snapshot up to this point was once required (e.g. for backup or replication)
// reindexing software must create a snapshot every time it hits this record 
#define	LEV_SNAPSHOT	0x04d26850
// usually the second entry in a binlog file, unless this binlog file is the very first
#define	LEV_ROTATE_FROM	0x04724cd2
// almost last entry, output before switching to a new binlog file; LEV_CRC32 and LEV_END might follow
#define	LEV_ROTATE_TO	0x04464c72
// stores crc32 up to this point
#define	LEV_CRC32	0x04435243
// sets slice status/mode (e.g. "normal", "init", "frozen")
#define	LEV_SLICE_STATUS	0x04747300
// states that reindexing was required at this point (useful in replication streams to trigger lite replica reindexing; reindexing software is not required to make a snapshot for this point)
#define	LEV_REINDEX	0x04586469
// delayed alteration of extra fields bitmask
#define	LEV_CHANGE_FIELDMASK_DELAYED	0x04546c41
// end of data in cbinlog
#define LEV_CBINLOG_END 0x04644e65

#define	LEV_ALIGN_FILL	0xfc

struct lev_generic {
  lev_type_t type;
  int a;
  int b;
  int c;
  int d;
  int e;
  int f;
};

struct lev_start {
  lev_type_t type;
  int schema_id;
  int extra_bytes;
  int split_mod;
  int split_min;
  int split_max;
  char str[4];		// extra_bytes, contains: [\x01 char field_bitmask] [<table_name> [\t <extra_schema_args>]] \0
};

struct lev_timestamp {
  lev_type_t type;
  int timestamp;
};

struct lev_snapshot {
  lev_type_t type;
  int urgency;		// max seconds before snapshot generation is actually launched, -1 = not urgent
  int checkpoint_id;	// goes to snapshot unchanged, useful for creating a distributed db snapshot
  int checkpoint_date;	// when snapshot was required, could be different from current timestamp
};

struct lev_crc32 {
  lev_type_t type;
  int timestamp;	// timestamp (serves as a LEV_TIMESTAMP)
  long long pos;	// position of the beginning of this record in the log
  unsigned crc32;		// crc32 of all data in file up to the beginning of this record
};

struct lev_rotate_from {
  lev_type_t type;
  int timestamp;
  long long cur_log_pos;
  unsigned crc32;
  hash_t prev_log_hash;
  hash_t cur_log_hash;
};

struct lev_rotate_to {
  lev_type_t type;
  int timestamp;
  long long next_log_pos;
  unsigned crc32;
  hash_t cur_log_hash;
  hash_t next_log_hash;
};

struct lev_tag {
  lev_type_t type;
  unsigned char tag[16];
};

struct lev_change_fieldmask_delayed {
  lev_type_t type;
  int new_fieldmask;
};

struct lev_slice_status {
  lev_type_t type;	// LSB same as slice_status in kfs header
};


#define	KFS_BERR_ANY	-1
#define	KFS_BERR_SHORT	-2
#define	KFS_BERR_INVCODE	-3
#define	KFS_BERR_BADFMT	-4

/* for kdb-binlog-common.c */

#define ULOG_BUFFER_SIZE	(1 << 24)
#define SLOG_BUFFER_SIZE	(1 << 20)
/* additional space for moving unprocessed logevent befor decompression zipped binlog chunk */
#define ZLOG_BUFFER_SIZE	(1 << 20)
	
/* binlogs */

extern char *binlogname;
extern long long binlog_size;
extern int binlog_fd;
extern int binlog_repairing;
extern int binlog_disabled;
extern unsigned char binlog_tag[16];
extern int binlog_cyclic_mode;

extern long long log_pos, log_readto_pos, log_cutoff_pos, log_limit_pos;
extern int log_first_ts, log_read_until, log_last_ts;
extern int log_ts_interval, log_ts_exact_interval;
extern int log_true_now, log_set_now;
extern int log_split_min, log_split_max, log_split_mod;

extern long long max_binlog_size;

extern int disable_crc32;
extern int disable_ts;

extern unsigned log_crc32_complement;
extern long long log_crc32_pos;

extern int binlog_crc32_verbosity_level;

void clear_read_log (void);
void clear_write_log (void);
void clear_log (void);
void init_log_data (long long new_log_pos, int log_timestamp, unsigned log_crc32);
void set_log_data (int logfd, long long logsize);
void log_seek (long long new_log_pos, int log_timestamp, unsigned log_crc32);

int default_replay_logevent (struct lev_generic *E, int size);
typedef int (*replay_logevent_vector_t)(struct lev_generic *E, int size);

extern replay_logevent_vector_t replay_logevent;
int log_schema;

int replay_log (int cutoff_time, int set_now);
void flush_binlog (void);
void flush_binlog_forced (int force_sync);
void flush_binlog_ts (void);
void flush_binlog_last (void);
void sync_binlog (int mode);
void flush_cbinlog (int force_sync);

int compute_uncommitted_log_bytes (void);
long long log_cur_pos (void);
// NB: while reading binlog, this is position of the log event being interpreted;
//     while writing binlog, this is the last position to have been written to disk, uncommitted log bytes are not included.

void *alloc_log_event (int type, int bytes, int arg1);
void unalloc_log_event (int bytes);

void read_new_events (void);	// set epoll_pre_event = read_new_events for -r key

/* file locks (will eventually move into kfs.h) */

int lock_whole_file (int fd, int mode);
int is_write_log_empty (void);

extern int binlog_check_mode, binlog_check_errors;

void relax_log_crc32 (int s);
unsigned relax_write_log_crc32 (void);

int read_binlog_data (char *buf, int len);
long long log_write_pos (void);
long long log_last_pos (void);

int match_rotate_logevents (struct lev_rotate_to *RT, struct lev_rotate_from *RF);

#pragma	pack(pop)

#endif
