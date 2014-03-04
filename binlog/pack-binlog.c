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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#define _FILE_OFFSET_BITS 64

#include <assert.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <zlib.h>
#include <lzma.h>

#include "kdb-data-common.h"
#include "server-functions.h"
#include "kfs.h"
#include "md5.h"
#include "crc32.h"
#include "xz_dec.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"pack-binlog-1.13"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

static int allow_cutting_kfs_headers = 0;

static int xz_init_encoder (lzma_stream *strm, int level) {
	// Initialize the encoder using a preset. Set the integrity to check
	// to CRC64, which is the default in the xz command line tool. If
	// the .xz file needs to be decompressed with XZ Embedded, use
	// LZMA_CHECK_CRC32 instead.
	lzma_ret ret = lzma_easy_encoder (strm, level, LZMA_CHECK_CRC32);
	if (ret == LZMA_OK) {
		return 0;
  }
	// Something went wrong. The possible errors are documented in
	// lzma/container.h (src/liblzma/api/lzma/container.h in the source
	// package or e.g. /usr/include/lzma/container.h depending on the
	// install prefix).
	const char *msg;
	switch (ret) {
	case LZMA_MEM_ERROR:
		msg = "Memory allocation failed";
		break;
	case LZMA_OPTIONS_ERROR:
		msg = "Specified preset is not supported";
		break;
	case LZMA_UNSUPPORTED_CHECK:
		msg = "Specified integrity check is not supported";
		break;
	default:
		// This is most likely LZMA_PROG_ERROR indicating a bug in
		// this program or in liblzma. It is inconvenient to have a
		// separate error message for errors that should be impossible
		// to occur, but knowing the error code is important for
		// debugging. That's why it is good to print the error code
		// at least when there is no good error message to show.
		msg = "Unknown error, possibly a bug";
		break;
	}
  kprintf ("%s: Error initializing the encoder: %s (error code %u)\n", __func__, msg, ret);
	return -1;
}

static int xz_compress2 (void *dst, int *dest_len, void *src, int src_len, int level) {
	lzma_stream strm = LZMA_STREAM_INIT;
	if (xz_init_encoder (&strm, level) < 0) {
	  lzma_end (&strm);
    return -1;
  }

	strm.next_in = src;
	strm.avail_in = src_len;
	strm.next_out = dst;
	strm.avail_out = *dest_len;

  lzma_ret ret = lzma_code (&strm, LZMA_FINISH);
  if (ret == LZMA_STREAM_END) {
		*dest_len = *dest_len - strm.avail_out;
	  lzma_end (&strm);

    static int calls = 0;
    if (0 == calls++) {
      vkprintf (2, "Check unpacking after 1st call of the '%s' function.\n", __func__);
      int len = src_len;
      char *a = malloc (src_len);
      assert (a);
      const unsigned int c = compute_crc32 (dst, *dest_len);
      if (xz_uncompress2 (a, &len, dst, *dest_len) < 0) {
        kprintf ("%s: xz_uncompress2 failed (dynamic liblzma library packs different than static xz_dec.c unpacks)\n", __func__);
        free (a);
        return -1;
      }
      assert (c == compute_crc32 (dst, *dest_len));
      if (len != src_len || memcmp (a, src, len)) {
        kprintf ("%s: dynamic liblzma library packs different than static xz_dec.c unpacks\n", __func__);
        free (a);
        return -1;
      }
      free (a);
    }

    return 0;
  }

  if (ret == LZMA_OK) {
    kprintf ("%s: lzma_code returns LZMA_OK, but expected LZMA_STREAM_END. Output buffer overflow. strm.avail_out = %d.\n",
      __func__, (int) strm.avail_out);
	  lzma_end (&strm);
    return -1;
  }

  const char *msg;
  switch (ret) {
    case LZMA_MEM_ERROR:
      msg = "Memory allocation failed";
      break;
    case LZMA_DATA_ERROR:
      // This error is returned if the compressed
      // or uncompressed size get near 8 EiB
      // (2^63 bytes) because that's where the .xz
      // file format size limits currently are.
      // That is, the possibility of this error
      // is mostly theoretical unless you are doing
      // something very unusual.
      //
      // Note that strm->total_in and strm->total_out
      // have nothing to do with this error. Changing
      // those variables won't increase or decrease
      // the chance of getting this error.
      msg = "File size limits exceeded";
      break;
    default:
      // This is most likely LZMA_PROG_ERROR, but
      // if this program is buggy (or liblzma has
      // a bug), it may be e.g. LZMA_BUF_ERROR or
      // LZMA_OPTIONS_ERROR too.
      //
      // It is inconvenient to have a separate
      // error message for errors that should be
      // impossible to occur, but knowing the error
      // code is important for debugging. That's why
      // it is good to print the error code at least
      // when there is no good error message to show.
      msg = "Unknown error, possibly a bug";
      break;
  }

  kprintf ("%s: Encoder error: %s (error code %u)\n", __func__, msg, ret);
  // Free the memory allocated for the encoder. If we were encoding
	// multiple files, this would only need to be done after the last
	// file. See 02_decompress.c for handling of multiple files.
	//
	// It is OK to call lzma_end() multiple times or when it hasn't been
	// actually used except initialized with LZMA_STREAM_INIT.
	lzma_end (&strm);

  return -1;
}

int bz_compress (void *dst, int *dst_len, void *src, int src_len, kfs_bz_format_t format, int level) {
  uLongf destLen;
  int r;
  switch (format) {
    case kfs_bzf_zlib:
      destLen = *dst_len;
      r = compress2 (dst, &destLen, src, src_len, level);
      if (r != Z_OK) {
        kprintf ("%s: compress2 returns error code %d.\n", __func__, r);
        return -1;
      }
      *dst_len = (int) destLen;
    break;
    case kfs_bzf_xz:
      r = xz_compress2 (dst, dst_len, src, src_len, level);
      if (r < 0) {
        return r;
      }
    break;
#ifdef BINLOG_ZIP_BZ2
    case kfs_bzf_bz2:
      r = BZ2_bzBuffToBuffCompress (dst, (unsigned int *) dst_len, src, src_len, level, 0, 0);
      if (r != BZ_OK) {
        kprintf ("%s: bzBuffToBuffCompress returns error code %d.\n", __func__, r);
        return -1;
      }
    break;
#endif
    default: return -1;
  }
  return 0;
}

/******************** buffered write  ********************/
#define BUFFSIZE 0x1000000
static char Buff[BUFFSIZE], *rptr = NULL, *wptr = NULL, *write_filename = NULL;
static int write_fd = -1, buffered_write;
static long long write_off;

static void flushout (void) {
  if (!buffered_write) {
    return;
  }
  int w, s;
  if (rptr < wptr) {
    s = wptr - rptr;
    assert (write_fd >= 0);
    w = write (write_fd, rptr, s);
    if (w != s) {
      if (w < 0) {
        kprintf ("%s: fail to write %d bytes to the file '%s', offset: %lld. %m\n", __func__, s, write_filename, write_off);
      } else {
        kprintf ("%s: write %d of expected %d bytes to the file '%s', offset %lld. %m\n", __func__, w, s, write_filename, write_off);
      }
      exit (1);
    }
    write_off += s;
  }
  rptr = wptr = Buff;
}

static int open_write_fd (char *filename, int bwrite, int ofd) {
  buffered_write = bwrite;
  write_filename = filename;
  rptr = wptr = Buff + BUFFSIZE;
  write_off = 0;
  if (ofd < 0) {
    ofd = open (filename, O_CREAT | O_WRONLY | O_EXCL, 0640);
    if (ofd < 0) {
      kprintf ("open (%s, O_CREAT | O_WRONLY | O_EXCL, 0640) failed. %m\n", filename);
      return -1;
    }
    assert (ofd > 1);
    if (lock_whole_file (ofd, F_WRLCK) <= 0) {
      kprintf ("cannot lock file '%s' for writing\n", filename);
      assert (!close (ofd));
      return -1;
    }
  } else {
    assert (ofd == 1);
  }
  return write_fd = ofd;
}

static int writeout (const void *D, int len) {
  assert (len >= 0);

  if (!buffered_write) {
    /* unpack shouldn't use buffered write, since CHUNK_SIZE is equal to 16Mb */
    int w = write (write_fd, D, len);
    if (w != len) {
      if (w < 0) {
        kprintf ("%s: fail to write %d bytes to the file '%s', offset: %lld. %m\n", __func__, len, write_filename, write_off);
      } else {
        kprintf ("%s: write %d of expected %d bytes to the file '%s', offset %lld. %m\n", __func__, w, len, write_filename, write_off);
      }
      exit (1);
    }
    return len;
  }

  const int res = len;
  const char *d = D;
  while (len > 0) {
    int r = (Buff + BUFFSIZE) - wptr;
    if (r > len) {
      r = len;
    }
    memcpy (wptr, d, r);
    d += r;
    wptr += r;
    len -= r;
    if (len > 0) {
      flushout ();
    }
  }
  return res;
}

static unsigned char read_buff [KFS_BINLOG_ZIP_CHUNK_SIZE];
static unsigned char write_buff[KFS_BINLOG_ZIP_MAX_ENCODED_CHUNK_SIZE];
static const int MIN_BINLOG_SIZE = 1 << 20;

static long long compute_offset (long long off, int headers) {
  if (headers < 0) {
    return off;
  }
  return off + 4096 * headers;
}

static struct kfs_file_header kfs_Hdr[3];

typedef struct {
  char *input_short_filename;
  char *output_short_filename;
  char *backup_suffix;
  char *output_tmp_filename;
  char *output_filename;
} filenames_t;

static int endswith (const char *s, const char *suffix, int suffix_len) {
  int l = strlen (s);
  return l >= suffix_len && !strcmp (s + (l - suffix_len), suffix);
}

/*
static char *basename (char *filename) {
  char *p = strrchr (filename, '/');
  return p ? (p + 1) : filename;
}

static void filename_change_dir (char **filename, const char *dst_dir) {
  if (dst_dir != NULL) {
    char *s = basename (*filename);
    int sz = strlen (dst_dir) + strlen (s) + 2;
    char *r = malloc (sz);
    assert (r);
    assert (sprintf (r, "%s/%s", dst_dir, s) == sz - 1);
    free (*filename);
    *filename = r;
  }
}
*/

int filenames_init (filenames_t *F, const char *filename, int pack, int output_stdout) {
  F->input_short_filename = strdup (filename);
  assert (F->input_short_filename);
  int l = strlen (F->input_short_filename);
  F->backup_suffix = "";
  if (l >= 11 && F->input_short_filename[l - 11] == '.') {
    int i;
    for (i = 1; i <= 10; i++) {
      if (!isdigit (F->input_short_filename[l - i])) {
        break;
      }
    }
    if (i > 10) {
      F->backup_suffix = malloc (12);
      strcpy (F->backup_suffix, F->input_short_filename + (l - 11));
      F->input_short_filename[l-11] = 0;
      l -= 11;
      vkprintf (1, "backup suffix is '%s'\n", F->backup_suffix);
    }
  }

  if (pack) {
    if (!endswith (F->input_short_filename, ".bin", 4)) {
      kprintf ("'%s' doesn't end by .bin\n", F->input_short_filename);
      return -1;
    }
  } else {
    if (!endswith (F->input_short_filename, ".bin.bz", 7)) {
      kprintf ("'%s' doesn't end by '.bin.bz'\n", F->input_short_filename);
      return -1;
    }
  }

  int len = l + 1;
  if (pack) {
    len += 3;
  }
  F->output_short_filename = malloc (len);
  assert (F->output_short_filename);
  strcpy (F->output_short_filename, F->input_short_filename);
  if (pack) {
    strcpy (F->output_short_filename + l, ".bz");
  } else {
    F->output_short_filename[l-3] = 0;
  }

  l = strlen (F->output_short_filename);

  len = l + 5 + strlen (F->backup_suffix);
  if (output_stdout) {
    F->output_tmp_filename = strdup ("/dev/stdout");
  } else {
    F->output_tmp_filename = malloc (len);
    assert (F->output_tmp_filename);
    assert (sprintf (F->output_tmp_filename, "%s.tmp%s", F->output_short_filename, F->backup_suffix) == len - 1);
  }

  len -= 4;
  F->output_filename = malloc (len);
  assert (F->output_filename);
  assert (sprintf (F->output_filename, "%s%s", F->output_short_filename, F->backup_suffix) == len - 1);

  return 0;
}

int pack_binlog_file (const char *orig_binlog_filename, kfs_bz_format_t format, int level) {
  filenames_t F;
  if (filenames_init (&F, orig_binlog_filename, 1, 0) < 0) {
    return -1;
  }

  if (!access (F.output_filename, 0)) {
    kprintf ("destination file '%s' already exists.\n", F.output_filename);
    return -1;
  }

  struct kfs_file_header *KHDR = NULL;
  unsigned int log_crc32_complement = -1U;
  int magic = 0;
  static unsigned char hash_buff[0x8000];

  struct stat st;
  int fd = open (orig_binlog_filename, O_RDONLY);
  if (fd < 0) {
    kprintf ("%s: open (\"%s\", O_RDONLY) failed. %m\n", __func__, orig_binlog_filename);
    return -1;
  }
  if (fstat (fd, &st) < 0) {
    kprintf ("%s: fstat for the file '%s' failed. %m\n", __func__, orig_binlog_filename);
    assert (!close (fd));
    return -1;
  }

  if (st.st_size <= MIN_BINLOG_SIZE) {
    kprintf ("%s: binlog file size is too small (%lld bytes).\n", __func__, (long long) st.st_size);
    assert (!close (fd));
    return -1;
  }

  if (st.st_size > KFS_BINLOG_ZIP_MAX_FILESIZE) {
    kprintf ("%s: binlog file size is too big (%lld bytes.\n", __func__, (long long) st.st_size);
    assert (!close (fd));
    return -1;
  }

  int header_size =  kfs_bz_compute_header_size (st.st_size);
  kfs_binlog_zip_header_t *H = zmalloc0 (header_size);
  H->orig_file_size = st.st_size;

  int n = H->orig_file_size < 0x20000 ? H->orig_file_size : 0x20000;

  if (lseek (fd, H->orig_file_size - n, SEEK_SET) == (off_t) -1) {
    kprintf ("%s: lseek to offset %lld of file '%s' failed. %m\n", __func__, H->orig_file_size - n, orig_binlog_filename);
    assert (!close (fd));
    return -1;
  }

  ssize_t r = read (fd, read_buff, n);
  if (r != n) {
    if (r < 0) {
      kprintf ("%s: fail to read last %d bytes from file '%s'. %m\n", __func__, n, orig_binlog_filename);
    } else {
      kprintf ("%s: read %lld of expected %d bytes from file '%s'. %m\n", __func__, (long long) r, n, orig_binlog_filename);
    }
    assert (!close (fd));
    return -1;
  }

  memcpy (hash_buff + 0x4000, read_buff + n - 0x4000, 0x4000);
  memcpy (&H->last36_bytes, read_buff + n - 36, 36);
  if (*((int *) H->last36_bytes) != LEV_ROTATE_TO) {
    kprintf ("%s: binlog file '%s' doesn't end by LEV_ROTATE_TO\n", __func__, orig_binlog_filename);
    assert (!close (fd));
    return -1;
  }
  md5 (read_buff, n, H->last_128k_md5);

  int ofd = open_write_fd (F.output_tmp_filename, 1, -1);
  if (ofd < 0) {
    assert (!close (fd));
    return -1;
  }

  long long off = 0;
  if (lseek (fd, off, SEEK_SET) == (off_t) -1) {
    kprintf ("%s: lseek to offset %lld of file '%s' failed. %m\n", __func__, 0LL, orig_binlog_filename);
    unlink (F.output_tmp_filename);
    assert (!close (fd));
    assert (!close (ofd));
    return -1;
  }

  int chunks = kfs_bz_get_chunks_no (H->orig_file_size), i = 0;
  long long cur_writing_off = header_size;

  writeout (H, header_size);

  md5_context ctx;
  md5_starts (&ctx);
  int m, headers = -1;

  int has_tag = 0;
  unsigned char binlog_tag[16];

  while (off < H->orig_file_size) {
    assert (i < chunks);
    H->chunk_offset[i] = cur_writing_off;
    const long long remaining_bytes = H->orig_file_size - off;
    assert (remaining_bytes > 0);
    n = remaining_bytes < KFS_BINLOG_ZIP_CHUNK_SIZE ? remaining_bytes : KFS_BINLOG_ZIP_CHUNK_SIZE;
    assert (n <= sizeof (read_buff));
    r = read (fd, read_buff, n);
    if (r != n) {
      if (r < 0) {
        kprintf ("%s: fail to read %d bytes from file '%s' from offset %lld. %m\n", __func__, n, orig_binlog_filename, compute_offset (off, headers));
      } else {
        kprintf ("%s: read %lld of expected %d bytes from file '%s' from offset %lld. %m\n", __func__, (long long) r, n, orig_binlog_filename, compute_offset (off, headers));
      }
      assert (!close (fd));
      assert (!close (ofd));
      return -1;
    }

    vkprintf (2, "%s: read %lld bytes from file '%s' from offset %lld.\n", __func__, (long long) r, orig_binlog_filename, compute_offset (off, headers));
    if (!off) {
      if (allow_cutting_kfs_headers) {
        for (headers = 0; headers <= 3; headers++) {
          magic = *((int *) (read_buff + headers * 4096));
          if (magic != KFS_MAGIC) {
            break;
          }
        }
        assert (headers * 4096 <= n);

        if (headers >= 3) {
          kprintf ("%s: error, binlog file '%s' contains at least %d KFS headers.\n", __func__, orig_binlog_filename, headers);
          exit (1);
        }

        if (headers > 0) {
          memcpy (kfs_Hdr, read_buff, headers * 4096);
          KHDR = kfs_Hdr;
          kprintf ("%s: warning, skip %d KFS headers in the binlog file '%s'.\n", __func__, headers, orig_binlog_filename);
          H->orig_file_size -= 4096 * headers;
          if (lseek (fd, 4096 * headers, SEEK_SET) == (off_t) -1) {
            kprintf ("%s: lseek to offset %d of file '%s' failed. %m\n", __func__, 4096 * headers, orig_binlog_filename);
            unlink (F.output_tmp_filename);
            assert (!close (fd));
            assert (!close (ofd));
            return -1;
          }
          continue;
        }
      }

      memcpy (hash_buff, read_buff, 0x4000);
      memcpy (H->first36_bytes, read_buff, 36);
      magic = *((int *) H->first36_bytes);
      if (magic != LEV_START && magic != LEV_ROTATE_FROM) {
        if (!allow_cutting_kfs_headers && magic == KFS_MAGIC) {
          kprintf ("binlog '%s' contains KFS-headers. You could cut them using not recommended to use option [-K].\n", orig_binlog_filename);
          unlink (F.output_tmp_filename);
          assert (!close (fd));
          assert (!close (ofd));
          return -1;
        }
        kprintf ("binlog '%s' starts by 0x%08x, but expected LEV_START or LEV_ROTATE_FROM\n", orig_binlog_filename, magic);
        unlink (F.output_tmp_filename);
        assert (!close (fd));
        assert (!close (ofd));
        return -1;
      }
      if (magic == LEV_ROTATE_FROM) {
        log_crc32_complement = ~(((struct lev_rotate_from *) H->first36_bytes)->crc32);
      } else {
        if (!kfs_get_tag (read_buff, n, binlog_tag)) {
          has_tag = 1;
        }
      }

      if (!has_tag) {
        md5 (read_buff, n < 0x20000 ? n : 0x20000, H->first_128k_md5);
      }

      md5 (read_buff, n < 0x100000 ? n : 0x100000, H->first_1m_md5);
    }

    md5_update (&ctx, read_buff, n);
    const long long remaining_crc32_bytes = remaining_bytes - 36;
    const int k = remaining_crc32_bytes < n ? remaining_crc32_bytes : n;
    if (k > 0) {
      log_crc32_complement = crc32_partial (read_buff, k, log_crc32_complement);
    }

    m = sizeof (write_buff);
    int res = bz_compress (write_buff, &m, read_buff, n, format, level);
    if (res < 0) {
      kprintf ("bz_compress failed\n");
      assert (!close (fd));
      assert (!close (ofd));
      return -1;
    }
    writeout (write_buff, m);
    vkprintf (2, "%s: writeout %d bytes to the file '%s', offset %lld.\n", __func__, m, F.output_tmp_filename, cur_writing_off);
    cur_writing_off += m;
    off += n;
    i++;
  }
  flushout ();
  assert (i == chunks);
  md5_finish (&ctx, H->orig_file_md5);
  unsigned char tmp[16];
  H->magic = KFS_BINLOG_ZIP_MAGIC;
  H->format = format | (level << 4);
  if (has_tag) {
    H->format |= KFS_BINLOG_ZIP_FORMAT_FLAG_HAS_TAG;
    memcpy (H->first_128k_md5, binlog_tag, 16);
  }
  if (KHDR) {
    H->file_hash = KHDR->file_id_hash;
  } else {
    memset (hash_buff + 0x8000 - 16, 0, 16);
    md5 (hash_buff, 0x8000, tmp);
    memcpy (&H->file_hash, tmp, 8);
  }
  if (magic == LEV_START && H->file_hash != ((struct lev_rotate_to *) H->last36_bytes)->cur_log_hash) {
    kprintf ("%s: computed file_hash(0x%016llx) isn't equal to LEV_ROTATE_TO cur_log_hash(0x%016llx) in the file '%s'.\n", __func__, H->file_hash, ((struct lev_rotate_to *) H->last36_bytes)->cur_log_hash, orig_binlog_filename);
    assert (!close (fd));
    assert (!close (ofd));
    return -1;
  }

  unsigned header_crc32 = compute_crc32 (H, header_size - 4);
  unsigned char *p = (unsigned char *) H;
  memcpy (p + (header_size - 4), &header_crc32, 4);
  if (lseek (ofd, 0, SEEK_SET) == (off_t) -1) {
    kprintf ("%s: lseek to offset %lld of file '%s' failed. %m\n", __func__, 0LL, F.output_tmp_filename);
    assert (!close (fd));
    assert (!close (ofd));
    return -1;
  }
  m = header_size;
  r = write (ofd, H, m);
  if (r != m) {
    if (r < 0) {
      kprintf ("%s: fail to write %d bytes to the file '%s', offset: %lld. %m\n", __func__, m, F.output_tmp_filename, 0LL);
    } else {
      kprintf ("%s: write %lld of expected %d bytes to the file '%s', offset %lld. %m\n", __func__, (long long) r, m, F.output_tmp_filename, 0LL);
    }
    assert (!close (fd));
    assert (!close (ofd));
    return -1;
  }

  assert (!close (fd));
  assert (!fsync (ofd));
  assert (!close (ofd));

  if (chmod (F.output_tmp_filename, 0440) < 0) {
    kprintf ("chmod ('%s', 0440) failed. %m\n", F.output_tmp_filename);
    return -1;
  }

  struct lev_rotate_to *RTO = (struct lev_rotate_to *) H->last36_bytes;
  assert (RTO->type == LEV_ROTATE_TO);
  if (~log_crc32_complement != RTO->crc32) {
    kprintf ("LEV_ROTATE_TO crc32 (0x%08x) doesn't match computed crc32 (0x%08x), file: %s.\n",
      RTO->crc32, ~log_crc32_complement, orig_binlog_filename);
    return -1;
  }

  if (!access (F.output_filename, 0)) {
    kprintf ("file '%s' already exists. Renaming temporary file '%s' failed.\n", F.output_filename, F.output_tmp_filename);
    return -1;
  }
  if (rename (F.output_tmp_filename, F.output_filename) < 0) {
    kprintf ("rename temporary file '%s' to file '%s' failed. %m\n", F.output_tmp_filename, F.output_filename);
    return -1;
  }

  struct timeval times[2];
  memset (times, 0, sizeof (times));
  times[0].tv_sec = times[1].tv_sec = st.st_mtime;
  if (utimes (F.output_filename, times) < 0) {
    kprintf ("warning: utimes for file '%s' failed. %m\n", F.output_filename);
  }

  printf ("%s\n", F.output_filename);

  vkprintf (1, "original file size: %lld, compressed file size: %lld, ratio: %.3lf%%\n",
    (long long) st.st_size,
    cur_writing_off,
    (100.0 * cur_writing_off) / st.st_size);
  return 0;
}

typedef struct {
  int write_buff_off;
  int size;
  int off;
} unpack_interval_t;

static int unpack_get_last_n_bytes (unsigned long long orig_file_size, int m, int chunk_no, int chunks, int n, unpack_interval_t *R) {
  unsigned u = orig_file_size;
  u &= (KFS_BINLOG_ZIP_CHUNK_SIZE - 1);
  if (u >= n) {
    if (chunk_no == chunks - 1) {
      R->write_buff_off = m - n;
      R->size = n;
      R->off = 0;
      return 0;
    }
    return -1;
  } else {
    if (chunk_no == chunks - 1) {
      R->write_buff_off = 0;
      R->size = u;
      R->off = n - u;
      return 0;
    } else {
      assert (chunk_no == chunks - 2);
      u = n - u;
      R->write_buff_off = m - u;
      R->size = u;
      R->off = 0;
      return 0;
    }
  }
}

const char *get_format (int format) {
  switch (format) {
    case kfs_bzf_zlib: return "gz";
    case kfs_bzf_bz2: return "bzip";
    case kfs_bzf_xz: return "xz";
  }
  static char buf[32];
  sprintf (buf, "unknown (%d)", format);
  return buf;
}

static char hcyf[16] = "0123456789abcdef";

const char *get_md5 (unsigned char a[16]) {
  static char output[33];
  int i;
  for (i = 0; i < 16; i++) {
    output[2*i] = hcyf[(a[i] >> 4) & 15];
    output[2*i+1] = hcyf[a[i] & 15];
  }
  output[32] = 0;
  return output;
}

const char *get_bytes (unsigned char *a, int len) {
  static char buf[16384];
  char *p = buf, *e = buf + sizeof (buf);
  int i;
  for (i = 0; i < len; i++) {
    assert (p + 3 <= e);
    *p++ = ' ';
    *p++ = hcyf[(a[i] >> 4) & 15];
    *p++ = hcyf[a[i] & 15];
  }
  assert (p < e);
  *p = 0;
  return buf + 1;
}

void row (const char *key) {
  int i = strlen (key) + 1;
  printf ("%s:", key);
  while (i < 17) {
    putchar (' ');
    i++;
  }
}

int show_info_zipped_binlog (const char *filename) {
  kfs_file_handle_t B = kfs_open_file (filename, 1);
  if (!B) {
    kprintf ("%s: kfs_open_file returns NULL, filename: '%s'\n", __func__, filename);
    return -1;
  }
  if (!(B->info->flags & 16)) {
    kprintf ("%s: '%s' isn't zipped filename\n", __func__, filename);
    return -1;
  }
  struct kfs_file_info *FI = B->info;
  kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) FI->start;
  row ("filename");
  printf ("%s\n", filename);
  row ("format");
  printf ("%s\n", get_format (H->format & 15));
  int level = (H->format & 0xf0) >> 4;
  if (level) {
    row ("level");
    printf ("%d\n", level);
  }
  row ("orig_file_size");
  printf ("%lld\n", H->orig_file_size);
  row ("head");
  printf ("%s\n", get_bytes ((unsigned char *) H->first36_bytes, 36));
  row ("tail");
  printf ("%s\n", get_bytes ((unsigned char *) H->last36_bytes, 36));
  row ("md5");
  printf ("%s\n", get_md5 (H->orig_file_md5));
  row ("md5(head 1Mi)");
  printf ("%s\n", get_md5 (H->first_1m_md5));
  if (H->format & KFS_BINLOG_ZIP_FORMAT_FLAG_HAS_TAG) {
    row ("tag");
    printf ("%s\n", get_md5 (H->first_128k_md5));
  } else {
    row ("md5(head 128Ki)");
    printf ("%s\n", get_md5 (H->first_128k_md5));
  }
  row ("md5(tail 128Ki)");
  printf ("%s\n", get_md5 (H->last_128k_md5));
  row ("hash");
  printf ("0x%016llx\n", H->file_hash);
  kfs_close_file (B, 1);
  return 0;
}

int unpack_binlog_file (const char *filename, int decompress) {
  const int test_mode = (decompress & 4) ? 1 : 0;
  const int integrity_check = 1;
  filenames_t F;
  if (filenames_init (&F, filename, 0, (decompress & 2) ? 1 : 0) < 0) {
    return -1;
  }

  if (!test_mode && !access (F.output_filename, 0)) {
    kprintf ("destination file '%s' already exists.\n", F.output_filename);
    return -1;
  }

  unpack_interval_t I;
  unsigned char last16k[0x4000];

  kfs_file_handle_t B = kfs_open_file (filename, 1);
  if (!B) {
    kprintf ("%s: kfs_open_file returns NULL, filename: '%s'\n", __func__, filename);
    return -1;
  }
  if (!(B->info->flags & 16)) {
    kprintf ("%s: '%s' isn't zipped filename\n", __func__, filename);
    return -1;
  }
  struct kfs_file_info *FI = B->info;
  kfs_binlog_zip_header_t *H = (kfs_binlog_zip_header_t *) FI->start;

  int ofd = -1;

  if (!test_mode) {
    ofd = open_write_fd (F.output_tmp_filename, 0, (decompress & 2) ? 1 : -1);
    if (ofd < 0) {
      kfs_close_file (B, 1);
      return -1;
    }
  }

  long long cur_writing_off = 0;
  int i;

  md5_context ctx, ctx_last128k, ctx_file_hash;
  unsigned char tmp[16];
  if (integrity_check) {
    md5_starts (&ctx);
    md5_starts (&ctx_last128k);
  }
  md5_starts (&ctx_file_hash);

  const int chunks = kfs_bz_get_chunks_no (H->orig_file_size);

  for (i = 0; i < chunks; i++) {
    int m = KFS_BINLOG_ZIP_CHUNK_SIZE;
    if (kfs_bz_decode (B, cur_writing_off, write_buff, &m, NULL) < 0) {
      kprintf ("%s: kfs_bz_decode (off: %lld) failed.\n", __func__, cur_writing_off);
      kfs_close_file (B, 1);
      if (ofd >= 0) {
        assert (!close (ofd));
      }
      return -1;
    }

    if (ofd >= 0) {
      writeout (write_buff, m);
      vkprintf (2, "%s: writeout %d bytes to the file '%s', offset %lld.\n", __func__, m, F.output_tmp_filename, cur_writing_off);
    }
    if (!i) {
      if (memcmp (H->first36_bytes, write_buff, 36)) {
        kprintf ("%s: first36_bytes isn't matched, file: '%s'.\n", __func__, filename);
        return -1;
      }
      md5_update (&ctx_file_hash, write_buff, 0x4000);
    }

    if (integrity_check) {
      md5_update (&ctx, write_buff, m);
      if (!i) {
        if (H->format & KFS_BINLOG_ZIP_FORMAT_FLAG_HAS_TAG) {
          if (kfs_get_tag (write_buff, m, tmp) < 0) {
            kprintf ("%s: tag extraction failed, file: '%s'.\n", __func__, filename);
            return -1;
          }
          if (memcmp (tmp, H->first_128k_md5, 16)) {
            kprintf ("%s: tag isn't matched, file: '%s'.\n", __func__, filename);
            return -1;
          }
        } else {
          md5 (write_buff, m < 0x20000 ? m : 0x20000, tmp);
          if (memcmp (tmp, H->first_128k_md5, 16)) {
            kprintf ("%s: first_128_md5 isn't matched, file: '%s'.\n", __func__, filename);
            return -1;
          }
        }
        md5 (write_buff, m < 0x100000 ? m : 0x100000, tmp);
        if (memcmp (tmp, H->first_1m_md5, 16)) {
          kprintf ("%s: first_1m_md5 isn't matched, file: '%s'.\n", __func__, filename);
          return -1;
        }
      }
      if (i >= chunks - 2) {
        if (!unpack_get_last_n_bytes (H->orig_file_size, m, i, chunks, 0x20000, &I)) {
          md5_update (&ctx_last128k, write_buff + I.write_buff_off, I.size);
        }
      }
    }

    if (i >= chunks - 2) {
      if (!unpack_get_last_n_bytes (H->orig_file_size, m, i, chunks, 36, &I)) {
        if (memcmp (H->last36_bytes + I.off, write_buff + I.write_buff_off, I.size)) {
          kprintf ("%s: last36_bytes isn't matched, file: '%s'.\n", __func__, filename);
          return -1;
        }
      }
      if (!unpack_get_last_n_bytes (H->orig_file_size, m, i, chunks, 0x4000, &I)) {
        memcpy (last16k + I.off, write_buff + I.write_buff_off, I.size);
      }
    }
    cur_writing_off += m;
  }

  if (integrity_check) {
    md5_finish (&ctx, tmp);
    if (memcmp (tmp, H->orig_file_md5, 16)) {
      kprintf ("%s: orig_file_md5 isn't matched, file: '%s'.\n", __func__, filename);
      return -1;
    }
    md5_finish (&ctx_last128k, tmp);
    if (memcmp (tmp, H->last_128k_md5, 16)) {
      kprintf ("%s: last_128k_md5 isn't matched, file: '%s'.\n", __func__, filename);
      return -1;
    }
  }

  memset (last16k + 0x4000 - 16, 0, 16);
  md5_update (&ctx_file_hash, last16k, 0x4000);
  md5_finish (&ctx_file_hash, tmp);
  if (!allow_cutting_kfs_headers && memcmp (tmp, &H->file_hash, 8)) {
    kprintf ("%s: file_hash isn't matched, file: '%s'.\n", __func__, filename);
    return -1;
  }

  kfs_close_file (B, 1);

  if (ofd > 1) {
    assert (!fsync (ofd));
    assert (!close (ofd));
    if (!access (F.output_filename, 0)) {
      kprintf ("file '%s' already exists. Renaming temporary file '%s' failed.\n", F.output_filename, F.output_tmp_filename);
      return -1;
    }
    if (rename (F.output_tmp_filename, F.output_filename) < 0) {
      kprintf ("rename temporary file '%s' to file '%s' failed. %m\n", F.output_tmp_filename, F.output_filename);
      return -1;
    }

    struct timeval times[2];
    memset (times, 0, sizeof (times));
    times[0].tv_sec = times[1].tv_sec = FI->mtime;
    if (utimes (F.output_filename, times) < 0) {
      kprintf ("warning: utimes for file '%s' failed. %m\n", F.output_filename);
    }
  }
  return 0;
}

void usage (void) {
  printf ("%s\n", FullVersionStr);
  printf (
    "pack-binlog [-123456789] [-u<username>] [-v] <binlog>\n"
    "\tBinlog packing tool.\n"
    "\t[-v]\t\toutput statistical and debug information into stderr\n"
    "\t[-d]\t\tdecompress\n"
    "\t[-c]\t\tdecompress to stdout\n"
    "\t[-t]\t\ttest, check the compressed file integrity\n"
    "\t[-i]\t\tshow zipped binlog info\n"
    "\t[-x]\t\txz compression\n"
    "\t[-z]\t\tzlib compression\n"
    "\t[-1]\t\tcompress faster\n"
    "\t[-6]\t\tdefault for xz\n"
    "\t[-9]\t\tcompress better (default for zlib)\n"
    "\t[-K]\t\tallows to cut KFS headers (not recommend to use).\n"
    "\t\t\tKFS headers cuttings leads to broken replication and failure during binlog replaying after unpacking.\n"
#ifdef BINLOG_ZIP_BZ2
    "\t[-j]\t\tbz2 compression\n"
#endif
  );
/*
  int i;
  for (i = 0; i < 10; i++) {
    long long x = lzma_easy_decoder_memusage (i);
    printf ("lzma_easy_decoder_memusage (%d) = %.3lfMi\n", i, x / (double) (1<<20));
  }
*/
  exit (2);
}

int main (int argc, char *argv[]) {
  int i, level = -1, format = kfs_bzf_xz, decompress = 0, info = 0;
  maxconn = 10;
  set_debug_handlers ();
  while ((i = getopt (argc, argv, "0123456789Kcdhijtu:vxz")) != -1) {
    switch (i) {
    case '0'...'9':
      level = i - '0';
    break;
    case 'K':
      allow_cutting_kfs_headers = 1;
    break;
    case 'c':
      decompress |= 2;
    break;
    case 'd':
      decompress |= 1;
    break;
    case 'h':
      usage ();
    break;
    case 'i':
      info = 1;
    break;
    case 'j':
      format = kfs_bzf_bz2;
    break;
    case 't':
      decompress |= 4;
    break;
    case 'u':
      username = optarg;
    break;
    case 'v':
      verbosity++;
    break;
    case 'x':
      format = kfs_bzf_xz;
    break;
    case 'z':
      format = kfs_bzf_zlib;
    break;
    default:
      fprintf (stderr, "Unimplemented option %c\n", i);
      exit (2);
    break;
    }
  }

  if (level < 0) {
    level = format == kfs_bzf_xz ? 6 : 9;
  }

  if (!strncmp (argv[0], "unpack", 6)) {
    decompress = 1;
  }

  if (optind + 1 != argc) {
    usage ();
  }

  if (raise_file_rlimit (maxconn + 16) < 0) {
    kprintf ("fatal: cannot raise open file limit to %d\n", maxconn + 16);
    exit (1);
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  dynamic_data_buffer_size = 16 << 20;
  init_dyn_data ();

  if (info) {
    if (show_info_zipped_binlog (argv[optind]) < 0) {
      return 1;
    }
    return 0;
  }

  if (!decompress) {
    vkprintf (1, "format: %d, level: %d\n", (int) format, level);
    if (pack_binlog_file (argv[optind], format, level) < 0) {
      vkprintf (2, "pack_binlog_file ('%s', %d, %d) failed.\n", argv[optind], format, level);
      exit (1);
    }
  } else {
    if (unpack_binlog_file (argv[optind], decompress) < 0) {
      vkprintf (2, "unpack_binlog_file ('%s') failed.\n", argv[optind]);
      exit (1);
    }
  }

  return 0;
}
