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

    Copyright 2010-2013 Vkontakte Ltd
              2010-2013 Nikolai Durov
              2010-2013 Andrei Lopatin
                   2013 Vitaliy Valtman
*/

#define	_FILE_OFFSET_BITS	64

#include <stdio.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <openssl/sha.h>
// #include <openssl/aes.h>
#include "net-crypto-aes.h"

#include "net-connections.h"
#include "md5.h"

#define	DEFAULT_PWD_FILE	"/etc/engine/aes_password"
#define	MIN_PWD_LEN 32
#define MAX_PWD_LEN 256

extern int verbosity;

int allocated_aes_crypto;

char pwd_buf[MAX_PWD_LEN + 128];
char rand_buf[64];
int pwd_len, rad_len;


int aes_crypto_init (struct connection *c, void *key_data, int key_data_len) {
  assert (key_data_len == sizeof (struct aes_key_data));
  struct aes_crypto *T = malloc (sizeof (struct aes_crypto));
  struct aes_key_data *D = key_data;
  assert (T);
  ++allocated_aes_crypto;
  /* AES_set_decrypt_key (D->read_key, 256, &T->read_aeskey); */
  vk_aes_set_decrypt_key (&T->read_aeskey, D->read_key, 256);
  memcpy (T->read_iv, D->read_iv, 16);
  /* AES_set_encrypt_key (D->write_key, 256, &T->write_aeskey); */
  vk_aes_set_encrypt_key (&T->write_aeskey, D->write_key, 256);
  memcpy (T->write_iv, D->write_iv, 16);
  c->crypto = T;
  return 0;
}

int aes_crypto_free (struct connection *c) {
  if (c->crypto) {
    free (c->crypto);
    c->crypto = 0;
    --allocated_aes_crypto;
  }
  return 0;
}


/* 0 = all ok, >0 = so much more bytes needed to encrypt last block */
int aes_crypto_encrypt_output (struct connection *c) {
  static nb_processor_t P;
  struct aes_crypto *T = c->crypto;
  assert (c->crypto);

  //dump_buffers (&c->Out);
  
  nb_start_process (&P, &c->Out);
  while (P.len0 + P.len1 >= 16) {
    assert (P.len0 >= 0 && P.len1 >= 0);
    if (P.len0 >= 16) {
      // AES_cbc_encrypt ((unsigned char *)P.ptr0, (unsigned char *)P.ptr0, P.len0 & -16, &T->write_aeskey, T->write_iv, AES_ENCRYPT);
      T->write_aeskey.cbc_crypt (&T->write_aeskey, (unsigned char *)P.ptr0, (unsigned char *)P.ptr0, P.len0 & -16, T->write_iv);
      nb_advance_process (&P, P.len0 & -16);
    } else {
      static unsigned char tmpbuf[16];
      memcpy (tmpbuf, P.ptr0, P.len0);
      memcpy (tmpbuf + P.len0, P.ptr1, 16 - P.len0);
      // AES_cbc_encrypt (tmpbuf, tmpbuf, 16, &T->write_aeskey, T->write_iv, AES_ENCRYPT);
      T->write_aeskey.cbc_crypt (&T->write_aeskey, tmpbuf, tmpbuf, 16, T->write_iv);
      memcpy (P.ptr0, tmpbuf, P.len0);
      memcpy (P.ptr1, tmpbuf + P.len0, 16 - P.len0);
      nb_advance_process (&P, 16);
    }
  }

  //dump_buffers (&c->Out);
  
  assert (P.len0 + P.len1 == c->Out.unprocessed_bytes);

  return c->Out.unprocessed_bytes ? 16 - c->Out.unprocessed_bytes : 0;
}


/* 0 = all ok, >0 = so much more bytes needed to decrypt last block */
int aes_crypto_decrypt_input (struct connection *c) {
  static nb_processor_t P;
  struct aes_crypto *T = c->crypto;
  assert (c->crypto);

  //dump_buffers (&c->In);

  nb_start_process (&P, &c->In);
  while (P.len0 + P.len1 >= 16) {
    assert (P.len0 >= 0 && P.len1 >= 0);
    if (P.len0 >= 16) {
      // AES_cbc_encrypt ((unsigned char *)P.ptr0, (unsigned char *)P.ptr0, P.len0 & -16, &T->read_aeskey, T->read_iv, AES_DECRYPT);
      T->read_aeskey.cbc_crypt (&T->read_aeskey, (unsigned char *)P.ptr0, (unsigned char *)P.ptr0, P.len0 & -16, T->read_iv);
      nb_advance_process (&P, P.len0 & -16);
    } else {
      static unsigned char tmpbuf[16];
      memcpy (tmpbuf, P.ptr0, P.len0);
      memcpy (tmpbuf + P.len0, P.ptr1, 16 - P.len0);
      // AES_cbc_encrypt (tmpbuf, tmpbuf, 16, &T->read_aeskey, T->read_iv, AES_DECRYPT);
      T->read_aeskey.cbc_crypt (&T->read_aeskey, tmpbuf, tmpbuf, 16, T->read_iv);
      memcpy (P.ptr0, tmpbuf, P.len0);
      memcpy (P.ptr1, tmpbuf + P.len0, 16 - P.len0);
      nb_advance_process (&P, 16);
    }
  }

  assert (P.len0 + P.len1 == c->In.unprocessed_bytes);

  //dump_buffers (&c->In);

  return c->In.unprocessed_bytes ? 16 - c->In.unprocessed_bytes : 0;
}

/* returns # of bytes needed to complete last output block */
int aes_crypto_needed_output_bytes (struct connection *c) {
  assert (c->crypto);
  return -c->Out.unprocessed_bytes & 15;
}

int aes_initialized;

// filename = 0 -- use DEFAULT_PWD_FILE
// 1 = init ok, else < 0
int aes_load_pwd_file (const char *filename) {
  int h = open ("/dev/random", O_RDONLY | O_NONBLOCK);
  int r = 0;

  if (h >= 0) {
    r = read (h, rand_buf, 16);
    if (r > 0 && verbosity > 1) {
      fprintf (stderr, "added %d bytes of real entropy to the AES security key\n", r);
    }
    close (h);
  }

  if (r < 16) {
    h = open ("/dev/urandom", O_RDONLY);
    if (h < 0) {
      pwd_len = 0;
      return -1;
    }
    int s = read (h, rand_buf + r, 16 - r);
    if (r + s != 16) {
      pwd_len = 0;
      return -1;
    }
    close (h);
  }

  *(long *) rand_buf ^= lrand48();

  srand48 (*(long *)rand_buf);

  h = open (filename ? filename : DEFAULT_PWD_FILE, O_RDONLY);
  if (h < 0) {
    return -1;
  }

  r = read (h, pwd_buf, MAX_PWD_LEN + 1);

  close (h);

  if (r < MIN_PWD_LEN || r > MAX_PWD_LEN) {
    return -1;
  }

  pwd_len = r;

  if (verbosity > 0) {
    fprintf (stderr, "loaded password file %s\n", filename ? filename : DEFAULT_PWD_FILE);
  }

  aes_initialized = 1;

  return 1;
}


int aes_generate_nonce (char res[16]) {
  *(int *)(rand_buf + 16) = lrand48 ();
  *(int *)(rand_buf + 20) = lrand48 ();
  *(long long *)(rand_buf + 24) = rdtsc ();
  struct timespec T;
  assert (clock_gettime(CLOCK_REALTIME, &T) >= 0);
  *(int *)(rand_buf + 32) = T.tv_sec;
  *(int *)(rand_buf + 36) = T.tv_nsec;
  (*(int *)(rand_buf + 40))++;

  md5 ((unsigned char *)rand_buf, 44, (unsigned char *)res);
  return 0;
} 


// str := nonce_server.nonce_client.client_timestamp.server_ip.client_port.("Server"/"Client").client_ip.server_port.master_key.nonce_server.[client_ipv6.server_ipv6].nonce_client
// key := SUBSTR(MD5(str+4),0,12).SHA1(str)
// iv  := MD5(str+2)

int aes_create_keys (struct aes_key_data *R, int am_client, char nonce_server[16], char nonce_client[16], int client_timestamp,
		     unsigned server_ip, unsigned short server_port, unsigned char server_ipv6[16], unsigned client_ip, unsigned short client_port, unsigned char client_ipv6[16]) {
  unsigned char str[16+16+4+4+2+6+4+2+MAX_PWD_LEN+16+16+4+16*2];
  int str_len;

  if (!pwd_len) {
    return -1;
  }

  assert (pwd_len >= MIN_PWD_LEN && pwd_len <= MAX_PWD_LEN);

  memcpy (str, nonce_server, 16);
  memcpy (str + 16, nonce_client, 16);
  *((int *) (str + 32)) = client_timestamp;
  *((unsigned *) (str + 36)) = server_ip;
  *((unsigned short *) (str + 40)) = client_port;
  memcpy (str + 42, am_client ? "Client" : "Server", 6);
  *((unsigned *) (str + 48)) = client_ip;
  *((unsigned short *) (str + 52)) = server_port;
  memcpy (str + 54, pwd_buf, pwd_len);
  memcpy (str + 54 + pwd_len, nonce_server, 16);
  str_len = 70 + pwd_len;

  if (!server_ip) {
    assert (!client_ip);
    memcpy (str + str_len, client_ipv6, 16);
    memcpy (str + str_len + 16, server_ipv6, 16);
    str_len += 32;
  } else {
    assert (client_ip);
  }

  memcpy (str + str_len, nonce_client, 16);
  str_len += 16;

  md5 (str + 4, str_len - 4, R->write_key);
  SHA1 (str, str_len, R->write_key + 12);
  md5 (str + 2, str_len - 2, R->write_iv);

  memcpy (str + 42, !am_client ? "Client" : "Server", 6);

  md5 (str + 4, str_len - 4, R->read_key);
  SHA1 (str, str_len, R->read_key + 12);
  md5 (str + 2, str_len - 2, R->read_iv);

  memset (str, 0, str_len);

  return 1;
}

// str := server_pid . key . client_pid
// key := SUBSTR(MD5(str+3),0,12).SHA1(str)
// iv  := MD5(str+5)

int aes_create_udp_keys (struct aes_key_data *R, struct process_id *local_pid, struct process_id *remote_pid, int generation) {
  unsigned char str[16+16+4+4+2+6+4+2+MAX_PWD_LEN+16+16+4+16*2];
  int str_len;

  if (!pwd_len) {
    return -1;
  }

  assert (pwd_len >= MIN_PWD_LEN && pwd_len <= MAX_PWD_LEN);

  memcpy (str, local_pid, 12);
  memcpy (str + 12, pwd_buf, pwd_len);  
  memcpy (str + 12 + pwd_len, remote_pid, 12);
  memcpy (str + 24 + pwd_len, &generation, 4);
  str_len = 28 + pwd_len;

  md5 (str + 3, str_len - 3, R->write_key);
  SHA1 (str, str_len, R->write_key + 12);
  md5 (str + 5, str_len - 5, R->write_iv);

  
  memcpy (str, remote_pid, 12);
  memcpy (str + 12 + pwd_len, local_pid, 12);

  md5 (str + 3, str_len - 3, R->read_key);
  SHA1 (str, str_len, R->read_key + 12);
  md5 (str + 5, str_len - 5, R->read_iv);

  memset (str, 0, str_len);

  return 1;
}


int get_crypto_key_id (void) {
  if (pwd_len >= 4) {
    return *(int *)pwd_buf;
  } else {
    return 0;
  }
}
