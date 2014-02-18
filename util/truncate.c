/*
 This file is distributed as is. Do whatever you want with this source.
*/

#define	_FILE_OFFSET_BITS	64

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

int lock_whole_file (int fd, int mode) {
  static struct flock L;
  L.l_type = mode;
  L.l_whence = SEEK_SET;
  L.l_start = 0;
  L.l_len = 0;
  if (fcntl (fd, F_SETLK, &L) < 0) {
    fprintf (stderr, "cannot lock file %d: %m\n", fd);
    return -1;
  }
  return 1;
}

void usage (void) {
  fprintf (stderr, "usage: truncate <filename> <truncate-position>\n");
  exit (2);
}

int main (int argc, const char *argv[]) {
  char *end;
  struct stat st;
  int fd;
     
  if (argc != 3) {
    usage();
    return 2;
  }
  long long pos = strtoll (argv[2], &end, 0);
  if (!end || *end || !*argv[2]) {
    usage();
    return 2;
  }
  fd = open (argv[1], O_WRONLY);
  if (fd < 0) {
    fprintf (stderr, "cannot open file %s: %m\n", argv[1]);
    return 1;
  }
  if (fstat (fd, &st) < 0) {
    fprintf (stderr, "cannot stat file %s: %m\n", argv[1]);
    return 1;
  }
  if (!S_ISREG(st.st_mode)) {
    fprintf (stderr, "%s is not a regular file\n", argv[1]);
    return 1;
  }
  if (pos >= st.st_size) {
    fprintf (stderr, "truncate position %lld after end of file\n", pos);
    return 2;
  }
  if (pos < -st.st_size) {
    fprintf (stderr, "truncate position %lld before start of file\n", pos);
    return 2;
  }
  if (pos < 0) {
    pos += st.st_size;
  }
  if (lock_whole_file (fd, F_WRLCK) <= 0) {
    return 1;
  }
  if (ftruncate (fd, pos) < 0) {
    fprintf (stderr, "cannot truncate file %s at position %lld: %m\n", argv[1], pos);
  }
  close (fd);
  return 0;
}
