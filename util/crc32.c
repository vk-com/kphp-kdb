/*
 This file is distributed as is. Do whatever you want with this source.
*/

#define _FILE_OFFSET_BITS       64

#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include "crc32.h"

#define RBUFF_SIZE	(1 << 24)

int main (int argc, const char *argv[]) {
  if (argc == 2) {
    close (0);
    int fd = open (argv[1], O_RDONLY);
    if (fd) {
      fprintf (stderr, "cannot read file %s: %m\n", argv[1]);
      return 1;
    }
  } else if (argc > 2) {
    return 2;
  }
  unsigned crc32_complement = -1;
  static char RBuff[RBUFF_SIZE];
  int r;
  do {
    r = read (0, RBuff, RBUFF_SIZE);
    if (r < 0) {
      fprintf (stderr, "error reading input: %m\n");
      return 1;
    }
    crc32_complement = crc32_partial (RBuff, r, crc32_complement);
  } while (r);
  printf ("%08x\n", ~crc32_complement);
  return 0;
}
