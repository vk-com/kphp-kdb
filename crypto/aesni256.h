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

    Copyright 2012-2013 Vkontakte Ltd
              2012-2013 Anton Maydell
*/

#ifndef __NET_CRYPTO_AESNI256_H
#define __NET_CRYPTO_AESNI256_H

#include <openssl/aes.h>

struct aesni256_ctx {
  unsigned char a[256+16];
};

typedef struct vk_aes_ctx {
  void (*cbc_crypt) (struct vk_aes_ctx *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16]);
  void (*ige_crypt) (struct vk_aes_ctx *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[32]);
  void (*ctr_crypt) (struct vk_aes_ctx *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16], unsigned long long offset);
  union {
    AES_KEY key;
    struct aesni256_ctx ctx;
  } u;
} vk_aes_ctx_t;

void vk_aes_set_encrypt_key (vk_aes_ctx_t *ctx, unsigned char *key, int bits);
void vk_aes_set_decrypt_key (vk_aes_ctx_t *ctx, unsigned char *key, int bits);

/******************** Test only declarations  ********************/
void vk_ssl_aes_ctr_crypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16], unsigned long long offset);

#endif
