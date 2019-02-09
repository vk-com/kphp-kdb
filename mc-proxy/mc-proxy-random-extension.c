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

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <openssl/aes.h>
#include <openssl/evp.h>

#include "net-events.h"
#include "net-buffers.h"
#include "net-connections.h"
#include "net-memcache-server.h"

#include "mc-proxy-random-extension.h"
#include "net-memcache-client.h"
#include "mc-proxy.h"

#define	PHP_MAX_RES	65536

unsigned char R[PHP_MAX_RES+64];
unsigned char A[PHP_MAX_RES];

int random_merge_end_query (struct connection *c, const char *key, int key_len, void *E, struct gather_entry *data, int tot_num) {
  struct random_gather_extra *extra = E;
  int i, j, bi = 0;
  for (i = 1; i < tot_num; i++) {
    if (data[bi].res_bytes < data[i].res_bytes) {
      bi = i;
    }
  }
  int max_bytes = data[bi].res_bytes, *r, *w;
  if (max_bytes <= 64) {
    return 0;
  }
  memset (R, 0, (max_bytes + 3) & -4);

  for (i = 0; i < tot_num; i++) {
    r = data[i].data;
    w = (int *) R;
    for (j = 0; j < data[i].res_bytes; j += 4) {
      *w++ ^= *r++;
    }
  }

  int c_len, f_len;

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  // for openssl 1.0.2
  EVP_CIPHER_CTX e;
  EVP_CIPHER_CTX_init (&e);

  EVP_EncryptInit_ex (&e, EVP_aes_256_cbc(), NULL, R, R + 32);

  if (!EVP_EncryptUpdate (&e, A, &c_len, R + 64, max_bytes - 64)) {
    vkprintf (1, "EVP_EncryptUpdate fail.\n");
    EVP_CIPHER_CTX_cleanup (&e);
    return 0;
  }

  if (!EVP_EncryptFinal_ex (&e, A + c_len, &f_len)) {
    vkprintf (1, "EVP_EncryptFinal_ex fail.\n");
    EVP_CIPHER_CTX_cleanup (&e);
    return 0;
  }

  EVP_CIPHER_CTX_cleanup (&e);
#else
  // for openssl 1.1.0
  EVP_CIPHER_CTX *e;
  e = EVP_CIPHER_CTX_new ();

  EVP_EncryptInit_ex (e, EVP_aes_256_cbc(), NULL, R, R + 32);

   if (!EVP_EncryptUpdate (e, A, &c_len, R + 64, max_bytes - 64)) {
    vkprintf (1, "EVP_EncryptUpdate fail.\n");
    EVP_CIPHER_CTX_free (e);
    return 0;
  }

  if (!EVP_EncryptFinal_ex (e, A + c_len, &f_len)) {
    vkprintf (1, "EVP_EncryptFinal_ex fail.\n");
    EVP_CIPHER_CTX_free (e);
    return 0;
  }

  EVP_CIPHER_CTX_free (e);
#endif

  f_len += c_len;

  int res = f_len < data[0].res_bytes ? f_len : data[0].res_bytes;
  if (res > extra->bytes) {
    res = extra->bytes;
  }

  w = (int *) A;
  r = data[0].data;
  for (i = 0; i < res; i += 4) {
    *w++ ^= *r++;
  }

  if (extra->hex) {
    static const char hcyf[16] = "0123456789abcdef";
    for (i = 0; i < res; i++) {
      R[2*i] = hcyf[(A[i] >> 4) & 15];
      R[2*i+1] = hcyf[A[i] & 15];
    }
    res *= 2;
    if (res > extra->limit) {
      res = extra->limit;
    }
    return return_one_key (c, key, (char *) R, res);
  }
  return return_one_key (c, key, (char *) A, res);
}

int random_generate_new_key (char *buff, char *key, int len, void *E) {
  struct random_gather_extra *extra = E;
  return sprintf (buff, "random%d", extra->bytes + 64);
}

void *random_store_gather_extra (const char *key, int key_len) {
  int limit = 0, hex = -1, bytes;
  if (sscanf (key, "random%d", &limit) == 1) {
    hex = 0;
  } else if (sscanf (key, "hex_random%d", &limit) == 1) {
    hex = 1;
  } else {
    vkprintf (4, "Error in parse: no correct query\n");
    return 0;
  }

  if (limit <= 0 || limit > PHP_MAX_RES) {
    limit = PHP_MAX_RES;
  }

  if (hex) {
    bytes = (limit >> 1) + (limit & 1);
  } else {
    bytes = limit;
  }

  struct random_gather_extra *extra = zzmalloc (sizeof (struct random_gather_extra));
  extra->limit = limit;
  extra->hex = hex;
  extra->bytes = bytes;
  return extra;
}

int random_check_query (int type, const char *key, int key_len) {
  vkprintf (2, "random_check: type = %d, key = %s, key_len = %d\n", type, key, key_len);
  int l = eat_at (key, key_len);
  key += l;
  key_len -= l;
  if (type == mct_get) {
    return (key_len >= 6 && !memcmp (key, "random", 6)) ||
           (key_len >= 10 && !memcmp (key, "hex_random", 10));
  } else {
    return 1;
  }
}

int random_free_gather_extra (void *E) {
  zzfree (E, sizeof (struct random_gather_extra));
  return 0;
}

struct mc_proxy_merge_functions random_merge_extension_functions = {
  .free_gather_extra = random_free_gather_extra,
  .merge_end_query = random_merge_end_query,
  .generate_preget_query = default_generate_preget_query,
  .generate_new_key = random_generate_new_key,
  .store_gather_extra = random_store_gather_extra,
  .check_query = random_check_query,
  .merge_store = default_merge_store,
  .load_saved_data = default_load_saved_data,
  .use_preget_query = default_use_preget_query
};

struct mc_proxy_merge_conf random_merge_extension_conf = {
  .use_at = 1,
  .use_preget_query = 0
};
