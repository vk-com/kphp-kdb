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

    Copyright 2011-2013 Vkontakte Ltd
              2011-2013 Anton Maydell
*/

#define	_FILE_OFFSET_BITS	64

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <openssl/pem.h>
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <arpa/inet.h>

#include "net-crypto-rsa.h"
#include "server-functions.h"

int get_random_bytes (void *buf, int n) {
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

static int generate_aes_key (unsigned char key[32], unsigned char iv[32]) {
  unsigned char a[64];
  long long r = rdtsc ();
  struct timespec T;
  assert (clock_gettime(CLOCK_REALTIME, &T) >= 0);
  memcpy (a, &T.tv_sec, 4);
  memcpy (a+4, &T.tv_nsec, 4);
  memcpy (a+8, &r, 8);
  unsigned short p = getpid ();
  memcpy (a + 16, &p, 2);
  int s = get_random_bytes (a + 18, 32);
  RAND_seed (a, s + 18);
  assert (RAND_bytes (key, 32));
  assert (RAND_pseudo_bytes (iv, 32));
  /*
  memcpy (a + 18, "ejudge was hacked", 17);
  int m = 18 + 17;
  unsigned salt[2] = {0x44af03bf, 0x2b515cb7};
  int i = EVP_BytesToKey (EVP_aes_256_cbc(), EVP_sha1(), (unsigned char *) salt, a, m, 128, key, iv);
  assert (i == 32);
  */
  return 0;
}

int rsa_encrypt (const char *const private_key_name, void *input, int ilen, void **output, int *olen) {
  vkprintf (3, "rsa_encrypt (private_key_name = %s, ilen = %d)\n", private_key_name, ilen);
  int err = 0;
  RSA *privKey = NULL;
  *output = NULL;
  *olen = -1;
  FILE *f = fopen (private_key_name, "rb");
  if (f == NULL) {
    kprintf ("Couldn't open private key file: %s\n", private_key_name);
    return -1;
  }
  privKey = PEM_read_RSAPrivateKey (f, NULL, NULL, NULL);
  if (privKey == NULL) {
    kprintf ("PEM_read_RSAPrivateKey returns NULL.\n");
    err = -2;
    goto clean;
  }
  fclose (f);
  f = NULL;
  unsigned char key[32], iv[32];
  generate_aes_key (key, iv);
  const int rsa_size = RSA_size (privKey);
  *olen = 4 + rsa_size + 32 + ilen + AES_BLOCK_SIZE;
  unsigned char *b = *output = malloc (*olen);

  memcpy (b, &rsa_size, 4);
  if (!RSA_private_encrypt (32, key, b + 4, privKey, RSA_PKCS1_PADDING)) {
    kprintf ("RSA_private_encrypt fail.\n");
    err = -3;
    goto clean;
  }
  memcpy (b + 4 + rsa_size, iv, 32);


  int c_len, f_len;

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  // for openssl 1.0.2
  EVP_CIPHER_CTX e;
  EVP_CIPHER_CTX_init (&e);

  EVP_EncryptInit_ex (&e, EVP_aes_256_cbc(), NULL, key, iv);
  EVP_EncryptUpdate (&e, b + 4 + rsa_size + 32, &c_len, input, ilen);
  EVP_EncryptFinal_ex (&e, b + 4 + rsa_size + 32 + c_len, &f_len);
  EVP_CIPHER_CTX_cleanup (&e);
#else
  // for openssl 1.1.0
  EVP_CIPHER_CTX *e;
  e = EVP_CIPHER_CTX_new ();

  EVP_EncryptInit_ex (e, EVP_aes_256_cbc(), NULL, key, iv);
  EVP_EncryptUpdate (e, b + 4 + rsa_size + 32, &c_len, input, ilen);
  EVP_EncryptFinal_ex (e, b + 4 + rsa_size + 32 + c_len, &f_len);
  EVP_CIPHER_CTX_free (e);
#endif
  
  int r = 4 + rsa_size + 32 + c_len + f_len;
  vkprintf (3, "c_len = %d, f_len = %d\n", c_len, f_len);
  assert (r <= *olen);
  *olen = r;
  clean:
  if (f != NULL) {
    fclose (f);
  }
  if (privKey) {
    RSA_free (privKey);
  }

  return err;
}

int rsa_decrypt (const char *const key_name, int public_key, void *input, int ilen, void **output, int *olen, int log_error_level) {
  vkprintf (3, "rsa_decrypt (key_name = %s, ilen = %d)\n", key_name, ilen);
  int err = 0, rsa_size = -1;
  void *b = NULL;
  *output = NULL;
  if (ilen < 4) {
    vkprintf (log_error_level, "Input too short (ilen = %d).\n", ilen);
    return -3;
  }
  memcpy (&rsa_size, input, 4);
  if (ilen < 4 + rsa_size + 32) {
    vkprintf (log_error_level, "Input too short (ilen = %d).\n", ilen);
    return -3;
  }
  FILE *f = fopen (key_name, "rb");
  if (f == NULL) {
    vkprintf (log_error_level, "Couldn't open key file: %s\n", key_name);
    return -1;
  }
  RSA *pubKey = NULL;
  if (public_key) {
    if (!PEM_read_RSA_PUBKEY (f, &pubKey, NULL, NULL)) {
      vkprintf (log_error_level, "PEM_read_RSA_PUBKEY: failed.\n");
      err = -2;
      goto clean;
    }
  } else {
    RSA *privKey = PEM_read_RSAPrivateKey (f, NULL, NULL, NULL);
    if (privKey == NULL) {
      vkprintf (log_error_level, "PEM_read_RSA_PUBKEY: failed.\n");
      err = -7;
      goto clean;
    }
    pubKey = RSAPublicKey_dup (privKey);
    assert (pubKey != NULL);
    RSA_free (privKey);
  }
  assert (pubKey != NULL);
  fclose (f);
  f = NULL;
  vkprintf (3, "rsa_decrypt: read key - ok!\n");
  if (rsa_size != RSA_size (pubKey)) {
    vkprintf (log_error_level, "Illegal key size = %d.\n", RSA_size (pubKey));
    err = -4;
    goto clean;
  }
  b = malloc (rsa_size);
  if (b == NULL) {
    vkprintf (log_error_level, "malloc (%d) fail. %m\n", rsa_size);
    err = -8;
    goto clean;
  }
  if (!RSA_public_decrypt (rsa_size, input + 4, b, pubKey, RSA_PKCS1_PADDING)) {
    vkprintf (log_error_level, "RSA_public_decrypt failed.\n");
    err = -5;
    goto clean;
  }
  unsigned char key[32], iv[32];
  memcpy (key, b, 32);
  free (b);
  b = NULL;
  memcpy (iv, input + 4 + rsa_size, 32);
  int aes_cipher_len = ilen - (4 + rsa_size + 32);
  void *a = malloc (aes_cipher_len);
  if (a == NULL) {
    vkprintf (log_error_level, "malloc (%d) fail. %m\n", aes_cipher_len);
    err = -9;
    goto clean;
  }
  *output = a;
  int p_len = 0;

  int f_len = 0;

#if OPENSSL_VERSION_NUMBER < 0x10100000L
  // for openssl 1.0.2
  EVP_CIPHER_CTX e;
  EVP_CIPHER_CTX_init (&e);

  EVP_DecryptInit_ex (&e, EVP_aes_256_cbc(), NULL, key, iv);
  EVP_DecryptUpdate (&e, a, &p_len, input + 4 + rsa_size + 32, aes_cipher_len);
  EVP_DecryptFinal_ex (&e, input + 4 + rsa_size + 32 + p_len, &f_len);
  EVP_CIPHER_CTX_cleanup (&e);
#else
  // for openssl 1.1.0
  EVP_CIPHER_CTX *e;
  e = EVP_CIPHER_CTX_new ();

  EVP_DecryptInit_ex (e, EVP_aes_256_cbc(), NULL, key, iv);
  EVP_DecryptUpdate (e, a, &p_len, input + 4 + rsa_size + 32, aes_cipher_len);
  EVP_DecryptFinal_ex (e, input + 4 + rsa_size + 32 + p_len, &f_len);
  EVP_CIPHER_CTX_free (e);
#endif

  vkprintf (3, "p_len = %d, f_len = %d\n", p_len, f_len);
  *olen = p_len + f_len;

  clean:
  if (f != NULL) {
    fclose (f);
  }
  if (pubKey) {
    RSA_free (pubKey);
  }
  if (b) {
    free (b);
  }

  return err;
}

