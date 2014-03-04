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

#define	_FILE_OFFSET_BITS	64
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <openssl/bn.h>
#include <openssl/rand.h>

#include "random-data.h"
#include "server-functions.h"
#include "kdb-data-common.h"

static int get_random_bytes (void *buf, int n) {
  int r = 0, h = open ("/dev/random", O_RDONLY | O_NONBLOCK);
  if (h >= 0) {
    r = read (h, buf, n);
    if (r > 0) {
      vkprintf (3, "added %d bytes of real entropy to the security key\n", r);
    }
    close (h);
  }

  if (r < n) {
    h = open ("/dev/urandom", O_RDONLY);
    if (h < 0) {
      return r;
    }
    int s = read (h, buf + r, n - r);
    close (h);
    if (s < 0) {
      return r;
    }
    r += s;
  }
  return r;
}

static void prng_seed (const char *password_filename, int password_length) {
  unsigned char *a = calloc (64 + password_length, 1);
  assert (a != NULL);
  long long r = rdtsc ();
  struct timespec T;
  assert (clock_gettime(CLOCK_REALTIME, &T) >= 0);
  memcpy (a, &T.tv_sec, 4);
  memcpy (a+4, &T.tv_nsec, 4);
  memcpy (a+8, &r, 8);
  unsigned short p = getpid ();
  memcpy (a + 16, &p, 2);
  int s = get_random_bytes (a + 18, 32) + 18;
  int fd = open (password_filename, O_RDONLY);
  if (fd < 0) {
    kprintf ("Warning: fail to open password file - \"%s\", %m.\n", password_filename);
  } else {
    int l = read (fd, a + s, password_length);
    if (l < 0) {
      kprintf ("Warning: fail to read password file - \"%s\", %m.\n", password_filename);
    } else {
      vkprintf (1, "read %d bytes from password file.\n", l);
      s += l;
    }
    close (fd);
  }
  RAND_seed (a, s);
  memset (a, 0, s);
  free (a);
}

/* Blum Blum Shub : random generator */
typedef struct {
  BIGNUM *m;
  BN_RECP_CTX *r;
  BIGNUM *x;
  BN_CTX *ctx;
  unsigned char *tmp;
  int mlen;
  long long i;
  int bits;
} bbs_t;

int bbs_init (bbs_t *self, int bits, const char *const password_filename, int password_length) {
  if (bits < 256) {
    return -1;
  }
  self->bits = bits;
  self->ctx = BN_CTX_new ();
  assert (self->ctx);
  BIGNUM *three = BN_new ();
  assert (three);
  assert (1 == BN_set_word (three, 3));
  BIGNUM *four = BN_new ();
  assert (four);
  assert (1 == BN_set_word (four, 4));

  prng_seed (password_filename, password_length);
  vkprintf (2, "PRNG initialized.\n");
  self->bits = bits;
  vkprintf (2, "p was generated.\n");
  BIGNUM *t = BN_new (), *p = NULL, *q = NULL;
  while (1) {
    BIGNUM *p1 = BN_new (), *q1 = BN_new ();
    assert (p1 && q1);
    p = BN_generate_prime (NULL, bits / 2, 0, four, three, NULL, NULL);
    assert (p);
    BN_sub (p1, p, BN_value_one ());
    q = BN_generate_prime (NULL, bits / 2, 0, four, three, NULL, NULL);
    assert (q);
    BN_sub (q1, q, BN_value_one ());

    BN_rshift1 (p1, p1);
    BN_rshift1 (q1, q1);
    BN_gcd (t, p1, q1, self->ctx);
    BN_free (p1);
    BN_free (q1);

    if (BN_is_one (t)) {
      break;
    }
    vkprintf (2, "gcd ((p-1)/2, (q-1)/2) isn't 1.\n");
    BN_free (p);
    p = NULL;
    BN_free (q);
    q = NULL;
  }

  BN_free (three);
  BN_free (four);

  self->m = BN_new ();
  BN_mul (self->m, p, q, self->ctx);
  BN_clear_free (p);
  BN_clear_free (q);
  self->r = BN_RECP_CTX_new ();
  assert (1 == BN_RECP_CTX_set (self->r, self->m, self->ctx));

  self->mlen = BN_num_bytes (self->m);
  self->tmp = calloc ((self->mlen + 3) & -4, 1);
  assert (self->tmp);
  self->x = BN_new ();
  BN_set_word (self->x, 239);
  BN_gcd (t, self->x, self->m, self->ctx);
  assert (BN_is_one (t));
  BN_free (t);
  self->i = 0;

  return 0;
}

int bbs_next_random_byte (bbs_t *self) {
  BIGNUM *z = BN_new ();
  BN_mod_mul_reciprocal (z, self->x, self->x, self->r, self->ctx);
  BN_mask_bits (self->x, 8);
  int r = BN_get_word (self->x);
  assert (r >= 0 && r < 256);
  BN_clear_free (self->x);
  self->x = z;
  ++(self->i);
  return r;
}

void bbs_free (bbs_t *self) {
  BN_free (self->m);
  BN_RECP_CTX_free (self->r);
  BN_free (self->x);
  BN_CTX_free (self->ctx);
  free (self->tmp);
  memset (self, 0, sizeof (*self));
}

static unsigned char *buffer;
static int qleft, qright, qsize, qtotal;
static bbs_t B;


void random_init (int key_size, int buffer_size, const char *const password_filename, int password_length) {
  int i;
  if (buffer_size <= 0) {
    buffer_size = 256 << 20;
  }
  buffer = zmalloc (buffer_size);
  qsize = buffer_size;
  qleft = 0;
  qright = 0;
  qtotal = 0;

  if (bbs_init (&B, key_size, password_filename, password_length) < 0) {
    kprintf ("Random number generator initialization failed.\n");
    exit (1);
  }

  for (i = 1; i <= 64; i++) {
    bbs_next_random_byte (&B);
  }
}

void random_work (int bytes) {
  while (qtotal < qsize - 1 && bytes > 0) {
    buffer[qright] = bbs_next_random_byte (&B);
    if (++qright == qsize) {
      qright = 0;
    }
    qtotal++;
    bytes--;
  }
}

int random_get_bytes (char *s, int size) {
  int r = 0;
  while (qtotal > 0 && r < size) {
    *s++ = buffer[qleft];
    buffer[qleft] = 0;
    if (++qleft == qsize) {
      qleft = 0;
    }
    r++;
    qtotal--;
  }
  int t = 0;
  while (r < size && t < 65536) {
    *s++ = bbs_next_random_byte (&B);
    r++;
    t++;
  }
  return r;
}

void random_free (const char *const password_filename, int password_length) {
  int fd = open (password_filename, O_CREAT | O_TRUNC | O_WRONLY, 0600);
  if (fd >= 0) {
    char *a = malloc (password_length);
    assert (a);
    int r = random_get_bytes (a, password_length);
    assert (r == write (fd, a, r));
    assert (!close (fd));
    memset (a, 0, password_length);
    free (a);
  }
  bbs_free (&B);
}

int random_get_hex_bytes (char *s, int size) {
  static char hcyf[16] = "0123456789abcdef";
  int r = 0;
  while (qtotal > 0 && r < size) {
    unsigned c = buffer[qleft];
    buffer[qleft] = 0;
    *s++ = hcyf[c >> 4];
    *s++ = hcyf[c & 15];
    if (++qleft == qsize) {
      qleft = 0;
    }
    r += 2;
    qtotal--;
  }
  int t = 0;
  while (r < size && t < 65536) {
    unsigned c = bbs_next_random_byte (&B);
    *s++ = hcyf[c >> 4];
    *s++ = hcyf[c & 15];
    r += 2;
    t++;
  }

  if (r > size) {
    r = size;
  }

  return r;
}
