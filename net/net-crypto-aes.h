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

#ifndef __VK_NET_CRYPTO_AES_H__
#define	__VK_NET_CRYPTO_AES_H__

#include <openssl/aes.h>

#include "net-connections.h"
#include "crypto/aesni256.h"
#include "pid.h"

int aes_crypto_init (struct connection *c, void *key_data, int key_data_len);  /* < 0 = error */
int aes_crypto_free (struct connection *c);
int aes_crypto_encrypt_output (struct connection *c);  /* 0 = all ok, >0 = so much more bytes needed to encrypt last block */
int aes_crypto_decrypt_input (struct connection *c);   /* 0 = all ok, >0 = so much more bytes needed to decrypt last block */
int aes_crypto_needed_output_bytes (struct connection *c);	/* returns # of bytes needed to complete last output block */

/* for aes_crypto_init */
struct aes_key_data {
  unsigned char read_key[32];
  unsigned char read_iv[16];
  unsigned char write_key[32];
  unsigned char write_iv[16];
};

#define	AES_KEY_DATA_LEN	sizeof (struct aes_key_data)

/* for c->crypto */
struct aes_crypto {
  unsigned char read_iv[16], write_iv[16];
  /*  AES_KEY read_aeskey;
      AES_KEY write_aeskey; */
  vk_aes_ctx_t read_aeskey;
  vk_aes_ctx_t write_aeskey;
};

extern int allocated_aes_crypto;
extern int aes_initialized;

int aes_load_pwd_file (const char *filename);
int aes_generate_nonce (char res[16]);

int aes_create_keys (struct aes_key_data *R, int am_client, char nonce_server[16], char nonce_client[16], int client_timestamp,
		     unsigned server_ip, unsigned short server_port, unsigned char server_ipv6[16],
		     unsigned client_ip, unsigned short client_port, unsigned char client_ipv6[16]);

int aes_create_udp_keys (struct aes_key_data *R, struct process_id *local_pid, struct process_id *remote_pid, int generation);

int get_crypto_key_id (void);

#endif
