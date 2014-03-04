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
#include <stddef.h>
#include <string.h>
#include <ctype.h>
#include <limits.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>

#include "net-crypto-rsa.h"
#include "kdb-data-common.h"
#include "server-functions.h"
#include "kfs.h"
#include "crc32.h"

#ifndef COMMIT
#define COMMIT "unknown"
#endif

#define	VERSION_STR	"tag-binlog-1.01"
const char FullVersionStr[] = VERSION_STR " compiled at " __DATE__ " " __TIME__ " by gcc " __VERSION__ " "
#ifdef __LP64__
  "64-bit"
#else
  "32-bit"
#endif
" after commit " COMMIT;

extern int now;

static int start = 0, act = 0;

static char hcyf[16] = "0123456789abcdef";

static void convert_md5_to_hex (char *output, unsigned char *hash) {
  int i;
  for (i = 0; i < 16; i++) {
    output[2*i] = hcyf[(hash[i] >> 4) & 15];
    output[2*i+1] = hcyf[hash[i] & 15];
  }
  output[32] = 0;
}

static unsigned ord (char x) {
  if (!isxdigit (x)) {
    kprintf ("'%c' isn't hex digit\n", x);
    exit (1);
  }
  if (isdigit (x)) {
    return x - 48;
  }
  x = tolower (x);
  return x - 87;
}

static void convert_hex_to_md5 (unsigned char *output, char *hex) {
  int i;
  const int l = strlen (hex);
  if (l != 32) {
    kprintf ("Tag should contain 32 hexdigits. Given tag length is %d.\n", l);
    exit (1);
  }
  for (i = 0; i < 16; i++) {
    output[i] = (ord (hex[2*i]) << 4) + ord (hex[2*i+1]);
  }
}

int tag_replay_logevent (struct lev_generic *E, int size) {
  int s;
  switch (E->type) {
    case LEV_START:
      assert (!start && !log_cur_pos ());
      if (size < 24 || E->b < 0 || E->b > 4096) { return -2; }
      s = 24 + ((E->b + 3) & -4);
      if (size < s) { return -2; }
      start = 1;
      return s;
    case LEV_TAG:
      s = default_replay_logevent (E, size);
      if (act == 'i') {
        char output[33];
        convert_md5_to_hex (output, binlog_tag);
        printf ("%s\n", output);
        exit (0);
      }
      kprintf ("ERROR: binlog has already a tag.\n");
      return -1;
  }
  kprintf ("unexpected log event type %08x at position %lld\n", E->type, log_cur_pos ());
  return -1;
}


void usage (void) {
  printf ("%s\n", FullVersionStr);
  printf (
    "tag-binlog [-u<username>] [-v] <binlog>\n"
    "\tAppending LEV_TAG tool.\n"
    "\t[-v]\t\toutput statistical and debug information into stderr\n"
    "\t[-a]\t\tappend random tag\n"
    "\t[-t<tag>]\tappend given tag\n"
    "\t[-i]\t\tinformation mode - print tag and exit\n"
  );
  exit (2);
}

int empty_tag (void) {
  int i;
  for (i = 0; i < 16; i++) {
    if (binlog_tag[i]) {
      return 0;
    }
  }
  return 1;
}

void add_action (int *act, int i) {
  if (*act) {
    kprintf ("You give two different actions: '%c' and '%c'.\n", *act, i);
    usage ();
  }
  *act = i;
}

int main (int argc, char *argv[]) {
  int i;
  char *hex_tag = NULL;
  set_debug_handlers ();
  while ((i = getopt (argc, argv, "aiht:u:vx")) != -1) {
    switch (i) {
    case 'a':
    case 'i':
      add_action (&act, i);
    break;
    case 't':
      hex_tag = optarg;
      add_action (&act, i);
    break;
    case 'h':
      usage ();
    break;
    case 'u':
      username = optarg;
    break;
    case 'v':
      verbosity++;
    break;
    default:
      fprintf (stderr, "Unimplemented option %c\n", i);
      exit (2);
    break;
    }
  }

  if (!act) {
    kprintf ("You didn't give action.\n");
    usage ();
  }

  binlog_disabled = (act == 'i');

  if (optind + 1 != argc) {
    usage ();
  }

  if (change_user (username) < 0) {
    kprintf ("fatal: cannot change user to %s\n", username ? username : "(none)");
    exit (1);
  }

  if (engine_preload_filelist (argv[optind], 0) < 0) {
    kprintf ("cannot open binlog files for %s\n", argv[optind]);
    exit (1);
  }

  Binlog = open_binlog (engine_replica, 0);
  if (!Binlog) {
    kprintf ("fatal: cannot find binlog for %s, log position %lld\n", engine_replica->replica_prefix, 0LL);
    exit (1);
  }

  clear_log ();
  init_log_data (0, 0, 0);

  replay_logevent = tag_replay_logevent;
  vkprintf (1, "replay log events started\n");
  i = replay_log (0, 1);
  vkprintf (1, "replay log events finished\n");
  if (!binlog_disabled) {
    clear_read_log();
  }
  if (i < 0) {
    kprintf ("fatal: error reading binlog\n");
    exit (1);
  }

  if (act == 'i') {
    kprintf ("Binlog doesn't contain tag.\n");
    return 1;
  }

  clear_write_log ();

  if (!start) {
    kprintf ("ERROR: LEV_START wasn't found.\n");
    return 1;
  }
  if (act == 'a') {
    int r = get_random_bytes (binlog_tag, 16);
    if (r != 16) {
      kprintf ("Not enough random bytes.\n");
      return 1;
    }
  } else {
    assert (act == 't');
    assert (hex_tag);
    convert_hex_to_md5 (binlog_tag, hex_tag);
  }
  if (empty_tag ()) {
    kprintf ("FATAL: binlog tag contains only zeroes.\n");
    return 1;
  }
  disable_crc32 |= 1;
  now = INT_MIN;
  assert (append_to_binlog (Binlog) == log_readto_pos);
  struct lev_tag *T = alloc_log_event (LEV_TAG, 20, 0);
  now = time (NULL);
  memcpy (T->tag, binlog_tag, 16);
  flush_binlog_last ();
  sync_binlog (2);
  
  char output[33];
  convert_md5_to_hex (output, binlog_tag);
  printf ("%s\n", output);
  return 0;
}
