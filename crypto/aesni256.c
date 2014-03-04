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

#include "crypto/aesni256.h"
#include <stdint.h>
#include "server-functions.h"

static void *align16 (void *ptr) {
  return (void *) (((uintptr_t) ptr + 15) & -16L);
}

static int aesni256_is_supported (void) {
  vk_cpuid_t *p = vk_cpuid ();
  return (p->ecx & (1 << 25)) && ((p->edx & 0x06000000) == 0x06000000);
}

static void aesni256_set_encrypt_key (struct aesni256_ctx *ctx, unsigned char key[32]) {
  int a, b;
  unsigned int *c, *d;
  asm volatile (
    "movdqu (%4), %%xmm1\n\t"
    "movdqa %%xmm1, 0x0(%5)\n\t"
    "movdqu 0x10(%4), %%xmm3\n\t"
    "movdqa %%xmm3, 0x10(%5)\n\t"
#ifdef __LP64__
    "addq $0x20, %5\n\t"
#else
    "addl $0x20, %5\n\t"
#endif
    "aeskeygenassist $0x01, %%xmm3, %%xmm2\n\t"
    "call 1f\n\t"
    "aeskeygenassist $0x02, %%xmm3, %%xmm2\n\t"
    "call 1f\n\t"
    "aeskeygenassist $0x04, %%xmm3, %%xmm2\n\t"
    "call 1f\n\t"
    "aeskeygenassist $0x08, %%xmm3, %%xmm2\n\t"
    "call 1f\n\t"
    "aeskeygenassist $0x10, %%xmm3, %%xmm2\n\t"
    "call 1f\n\t"
    "aeskeygenassist $0x20, %%xmm3, %%xmm2\n\t"
    "call 1f\n\t"
    "aeskeygenassist $0x40, %%xmm3, %%xmm2\n\t"
    "call 1f\n\t"
    "jmp 2f\n\t"
    "1:\n\t"
#ifdef __LP64__
    "movq %5, %3\n\t"
#else
    "movl %5, %3\n\t"
#endif
    "pshufd $0xff, %%xmm2, %%xmm2\n\t"
    "pxor %%xmm1, %%xmm2\n\t"
    "movd %%xmm2, %0\n\t"
    "movl %0, (%5)\n\t"
#ifdef __LP64__
    "addq $4, %5\n\t"
#else
    "addl $4, %5\n\t"
#endif
    "pshufd $0xe5, %%xmm1, %%xmm1\n\t"
    "movd %%xmm1, %1\n\t"
    "xor %1, %0\n\t"
    "movl %0, (%5)\n\t"
#ifdef __LP64__
    "addq $4, %5\n\t"
#else
    "addl $4, %5\n\t"
#endif
    "pshufd $0xe6, %%xmm1, %%xmm1\n\t"
    "movd %%xmm1, %1\n\t"
    "xor %1, %0\n\t"
    "movl %0, (%5)\n\t"
#ifdef __LP64__
    "addq $4, %5\n\t"
#else
    "addl $4, %5\n\t"
#endif
    "pshufd $0xe7, %%xmm1, %%xmm1\n\t"
    "movd %%xmm1, %1\n\t"
    "xor %1, %0\n\t"
    "movl %0, (%5)\n\t"
#ifdef __LP64__
    "addq $4, %5\n\t"
#else
    "addl $4, %5\n\t"
#endif
    "movdqa (%3), %%xmm4\n\t"
    "aeskeygenassist $0, %%xmm4, %%xmm4\n\t"
    "pshufd $0xe6, %%xmm4, %%xmm4\n\t"
    "pxor %%xmm3, %%xmm4\n\t"
    "movd %%xmm4, %0\n\t"
    "movl %0, (%5)\n\t"
#ifdef __LP64__
    "addq $4, %5\n\t"
#else
    "addl $4, %5\n\t"
#endif
    "pshufd $0xe5, %%xmm3, %%xmm3\n\t"
    "movd %%xmm3, %1\n\t"
    "xor %1, %0\n\t"
    "movl %0, (%5)\n\t"
#ifdef __LP64__
    "addq $4, %5\n\t"
#else
    "addl $4, %5\n\t"
#endif
    "pshufd $0xe6, %%xmm3, %%xmm3\n\t"
    "movd %%xmm3, %1\n\t"
    "xor %1, %0\n\t"
    "movl %0, (%5)\n\t"
#ifdef __LP64__
    "addq $4, %5\n\t"
#else
    "addl $4, %5\n\t"
#endif
    "pshufd $0xe7, %%xmm3, %%xmm3\n\t"
    "movd %%xmm3, %1\n\t"
    "xor %1, %0\n\t"
    "movl %0, (%5)\n\t"
#ifdef __LP64__
    "addq $4, %5\n\t"
#else
    "addl $4, %5\n\t"
#endif
    "movdqa (%3), %%xmm1\n\t"
#ifdef __LP64__
    "addq $0x10, %3\n\t"
#else
    "addl $0x10, %3\n\t"
#endif
    "movdqa (%3), %%xmm3\n\t"
    "ret\n\t"
    "2:\n\t"
   : "=&r" (a), "=&r" (b), "=&r"(c), "=&r" (d)
   : "r" (key), "2" (align16 (&ctx->a[0]))
   : "%xmm1", "%xmm2", "%xmm3", "%xmm4", "memory"
  );
}

static void aesni256_set_decrypt_key (struct aesni256_ctx *ctx, unsigned char key[32]) {
  int i;
  aesni256_set_encrypt_key (ctx, key);
  unsigned char *a = align16 (&ctx->a[0]);
  for (i = 1; i <= 13; i++) {
    asm volatile (
      "aesimc (%0), %%xmm1\n\t"
      "movdqa %%xmm1, (%0)\n\t"
    :
    : "r" (&a[i * 16])
    : "%xmm1", "memory"
    );
  }
}

static void aesni256_cbc_encrypt (struct aesni256_ctx *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16]) {
  void *p1, *p2;
  if (size < 16) {
    return;
  }
  asm volatile (
      "movdqu (%5), %%xmm1\n\t"
      "1:\n\t"
      "subl $0x10, %3\n\t"
      "movdqu (%4), %%xmm2\n\t"
#ifdef __LP64__
      "addq $0x10, %4\n\t"
#else
      "addl $0x10, %4\n\t"
#endif
      "pxor %%xmm1, %%xmm2\n\t"
      "pxor (%6), %%xmm2\n\t"
      "aesenc 0x10(%6), %%xmm2\n\t"
      "aesenc 0x20(%6), %%xmm2\n\t"
      "aesenc 0x30(%6), %%xmm2\n\t"
      "aesenc 0x40(%6), %%xmm2\n\t"
      "aesenc 0x50(%6), %%xmm2\n\t"
      "aesenc 0x60(%6), %%xmm2\n\t"
      "aesenc 0x70(%6), %%xmm2\n\t"
      "aesenc 0x80(%6), %%xmm2\n\t"
      "aesenc 0x90(%6), %%xmm2\n\t"
      "aesenc 0xa0(%6), %%xmm2\n\t"
      "aesenc 0xb0(%6), %%xmm2\n\t"
      "aesenc 0xc0(%6), %%xmm2\n\t"
      "aesenc 0xd0(%6), %%xmm2\n\t"
      "aesenclast 0xe0(%6), %%xmm2\n\t"
      "movaps %%xmm2, %%xmm1\n\t"
      "movdqu %%xmm2, (%7)\n\t"
#ifdef __LP64__
      "addq $0x10, %7\n\t"
#else
      "addl $0x10, %7\n\t"
#endif
      "cmpl $0x0f, %3\n\t"
      "jg 1b\n\t"
      "movdqu %%xmm1, (%5)\n\t"
    : "=r" (size), "=r" (p1), "=r" (p2)
    : "0" (size), "1r" (in), "r" (iv), "r" (align16 (ctx)), "2r" (out)
    : "%xmm1", "%xmm2", "memory"
  );
}

static void aesni256_cbc_decrypt (struct aesni256_ctx *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16]) {
  void *p1, *p2;
  if (size < 16) {
    return;
  }
  asm volatile (
      "movdqu (%5), %%xmm1\n\t"
      "1:\n\t"
      "subl $0x10, %3\n\t"
      "movdqu (%4), %%xmm2\n\t"
      "pxor 0xe0(%6), %%xmm2\n\t"
      "aesdec 0xd0(%6), %%xmm2\n\t"
      "aesdec 0xc0(%6), %%xmm2\n\t"
      "aesdec 0xb0(%6), %%xmm2\n\t"
      "aesdec 0xa0(%6), %%xmm2\n\t"
      "aesdec 0x90(%6), %%xmm2\n\t"
      "aesdec 0x80(%6), %%xmm2\n\t"
      "aesdec 0x70(%6), %%xmm2\n\t"
      "aesdec 0x60(%6), %%xmm2\n\t"
      "aesdec 0x50(%6), %%xmm2\n\t"
      "aesdec 0x40(%6), %%xmm2\n\t"
      "aesdec 0x30(%6), %%xmm2\n\t"
      "aesdec 0x20(%6), %%xmm2\n\t"
      "aesdec 0x10(%6), %%xmm2\n\t"
      "aesdeclast 0x00(%6), %%xmm2\n\t"
      "pxor %%xmm1, %%xmm2\n\t"
      "movdqu (%4), %%xmm1\n\t"
      "movdqu %%xmm2, (%7)\n\t"
#ifdef __LP64__
      "addq $0x10, %4\n\t"
      "addq $0x10, %7\n\t"
#else
      "addl $0x10, %4\n\t"
      "addl $0x10, %7\n\t"
#endif
      "cmpl $0x0f, %3\n\t"
      "jg 1b\n\t"
      "movdqu %%xmm1, (%5)\n\t"
    : "=r" (size), "=r" (p1), "=r" (p2)
    : "0" (size), "1r" (in), "r" (iv), "r" (align16 (ctx)), "2r" (out)
    : "%xmm1", "%xmm2", "memory"
  );
}

static void vk_aesni_cbc_encrypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16]) {
  aesni256_cbc_encrypt (&ctx->u.ctx, in, out, size, iv);
}

static void vk_aesni_cbc_decrypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16]) {
  aesni256_cbc_decrypt (&ctx->u.ctx, in, out, size, iv);
}

static void vk_ssl_aes_cbc_encrypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16]){
  AES_cbc_encrypt (in, out, size, &ctx->u.key, iv, AES_ENCRYPT);
}

static void vk_ssl_aes_cbc_decrypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16]){
  AES_cbc_encrypt (in, out, size, &ctx->u.key, iv, AES_DECRYPT);
}

static void aesni256_ige_encrypt (struct aesni256_ctx *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[32]) {
  void *p1, *p2;
  if (size < 16) {
    return;
  }
  asm volatile (
      "movdqu 0x0(%5), %%xmm3\n\t"  //xmm3 := y[i-1]
      "movdqu 0x10(%5), %%xmm1\n\t"      //xmm1 := x[i-1]
      "1:\n\t"
      "subl $0x10, %3\n\t"
      "movdqu (%4), %%xmm2\n\t"
#ifdef __LP64__
      "addq $0x10, %4\n\t"
#else
      "addl $0x10, %4\n\t"
#endif
      "movaps %%xmm2, %%xmm4\n\t"   //xmm4 := x[i]

      "pxor %%xmm3, %%xmm2\n\t"
      "pxor (%6), %%xmm2\n\t"
      "aesenc 0x10(%6), %%xmm2\n\t"
      "aesenc 0x20(%6), %%xmm2\n\t"
      "aesenc 0x30(%6), %%xmm2\n\t"
      "aesenc 0x40(%6), %%xmm2\n\t"
      "aesenc 0x50(%6), %%xmm2\n\t"
      "aesenc 0x60(%6), %%xmm2\n\t"
      "aesenc 0x70(%6), %%xmm2\n\t"
      "aesenc 0x80(%6), %%xmm2\n\t"
      "aesenc 0x90(%6), %%xmm2\n\t"
      "aesenc 0xa0(%6), %%xmm2\n\t"
      "aesenc 0xb0(%6), %%xmm2\n\t"
      "aesenc 0xc0(%6), %%xmm2\n\t"
      "aesenc 0xd0(%6), %%xmm2\n\t"
      "aesenclast 0xe0(%6), %%xmm2\n\t"
      "pxor %%xmm1, %%xmm2\n\t"
      "movaps %%xmm4, %%xmm1\n\t"
      "movaps %%xmm2, %%xmm3\n\t"
      "movdqu %%xmm2, (%7)\n\t"
#ifdef __LP64__
      "addq $0x10, %7\n\t"
#else
      "addl $0x10, %7\n\t"
#endif
      "cmpl $0x0f, %3\n\t"
      "jg 1b\n\t"
      "movdqu %%xmm3, 0x0(%5)\n\t"
      "movdqu %%xmm1, 0x10(%5)\n\t"
    : "=r" (size), "=r" (p1), "=r" (p2)
    : "0" (size), "1r" (in), "r" (iv), "r" (align16 (ctx)), "2r" (out)
    : "%xmm1", "%xmm2", "%xmm3", "%xmm4", "memory"
  );
}

static void aesni256_ige_decrypt (struct aesni256_ctx *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[32]) {
  void *p1, *p2;
  if (size < 16) {
    return;
  }
  asm volatile (
      "movdqu 0x10(%5), %%xmm1\n\t" //xmm1 := x[i-1]
      "movdqu 0x0(%5), %%xmm3\n\t"  //xmm3 := y[i-1]
      "1:\n\t"
      "subl $0x10, %3\n\t"
      "movdqu (%4), %%xmm2\n\t"
#ifdef __LP64__
      "addq $0x10, %4\n\t"
#else
      "addl $0x10, %4\n\t"
#endif
      "movaps %%xmm2, %%xmm4\n\t"   //xmm4 := y[i]
      "pxor %%xmm1, %%xmm2\n\t"
      "pxor 0xe0(%6), %%xmm2\n\t"
      "aesdec 0xd0(%6), %%xmm2\n\t"
      "aesdec 0xc0(%6), %%xmm2\n\t"
      "aesdec 0xb0(%6), %%xmm2\n\t"
      "aesdec 0xa0(%6), %%xmm2\n\t"
      "aesdec 0x90(%6), %%xmm2\n\t"
      "aesdec 0x80(%6), %%xmm2\n\t"
      "aesdec 0x70(%6), %%xmm2\n\t"
      "aesdec 0x60(%6), %%xmm2\n\t"
      "aesdec 0x50(%6), %%xmm2\n\t"
      "aesdec 0x40(%6), %%xmm2\n\t"
      "aesdec 0x30(%6), %%xmm2\n\t"
      "aesdec 0x20(%6), %%xmm2\n\t"
      "aesdec 0x10(%6), %%xmm2\n\t"
      "aesdeclast 0x00(%6), %%xmm2\n\t"
      "pxor %%xmm3, %%xmm2\n\t"
      "movaps %%xmm2, %%xmm1\n\t"
      "movaps %%xmm4, %%xmm3\n\t"
      "movdqu %%xmm2, (%7)\n\t"
#ifdef __LP64__
      "addq $0x10, %7\n\t"
#else
      "addl $0x10, %7\n\t"
#endif
      "cmpl $0x0f, %3\n\t"
      "jg 1b\n\t"
      "movdqu %%xmm1, 0x10(%5)\n\t"
      "movdqu %%xmm3, 0x0(%5)\n\t"
    : "=r" (size), "=r" (p1), "=r" (p2)
    : "0" (size), "1r" (in), "r" (iv), "r" (align16 (ctx)), "2r" (out)
    : "%xmm1", "%xmm2", "%xmm3", "%xmm4", "memory"
  );
}

static void vk_aesni_ige_encrypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[32]) {
  aesni256_ige_encrypt (&ctx->u.ctx, in, out, size, iv);
}

static void vk_aesni_ige_decrypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[32]) {
  aesni256_ige_decrypt (&ctx->u.ctx, in, out, size, iv);
}

static void vk_ssl_aes_ige_encrypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[32]){
  AES_ige_encrypt (in, out, size, &ctx->u.key, iv, AES_ENCRYPT);
}

static void vk_ssl_aes_ige_decrypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[32]){
  AES_ige_encrypt (in, out, size, &ctx->u.key, iv, AES_DECRYPT);
}

static void aesni256_encrypt (unsigned char *ctx, const unsigned char *in, unsigned char *out) {
  void *p1, *p2;
  asm volatile (
      "movdqu (%2), %%xmm1\n\t"
      "pxor (%3), %%xmm1\n\t"
      "aesenc 0x10(%3), %%xmm1\n\t"
      "aesenc 0x20(%3), %%xmm1\n\t"
      "aesenc 0x30(%3), %%xmm1\n\t"
      "aesenc 0x40(%3), %%xmm1\n\t"
      "aesenc 0x50(%3), %%xmm1\n\t"
      "aesenc 0x60(%3), %%xmm1\n\t"
      "aesenc 0x70(%3), %%xmm1\n\t"
      "aesenc 0x80(%3), %%xmm1\n\t"
      "aesenc 0x90(%3), %%xmm1\n\t"
      "aesenc 0xa0(%3), %%xmm1\n\t"
      "aesenc 0xb0(%3), %%xmm1\n\t"
      "aesenc 0xc0(%3), %%xmm1\n\t"
      "aesenc 0xd0(%3), %%xmm1\n\t"
      "aesenclast 0xe0(%3), %%xmm1\n\t"
      "movdqu %%xmm1, (%4)\n\t"
    : "=r" (p1), "=r" (p2)
    : "0r" (in), "r" (ctx), "1r" (out)
    : "%xmm1", "memory"
  );
}

static void vk_aesni_ctr_crypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16], unsigned long long offset) {
  unsigned char *a = align16 (&ctx->u.ctx.a[0]);
  unsigned long long *p = (unsigned long long *) (iv + 8);
  const unsigned long long old_ctr_value = *p;
  (*p) += offset >> 4;
  union {
    unsigned char c[16];
    unsigned long long d[2];
  } u;
  int i = offset & 15, l;
  if (i) {
    aesni256_encrypt (a, iv, u.c);
    (*p)++;
    l = i + size;
    if (l > 16) {
      l = 16;
    }
    size -= l - i;
    do {
      *out++ = (*in++) ^ u.c[i++];
    } while (i < l);
  }
  const unsigned long long *I = (const unsigned long long *) in;
  unsigned long long *O = (unsigned long long *) out;
  int n = size >> 4;
  while (--n >= 0) {
    aesni256_encrypt (a, iv, (unsigned char *) u.d);
    (*p)++;
    *O++ = (*I++) ^ u.d[0];
    *O++ = (*I++) ^ u.d[1];
  }
  in = (const unsigned char *) I;
  out = (unsigned char *) O;
  l = size & 15;
  if (l) {
    aesni256_encrypt (a, iv, u.c);
    i = 0;
    do {
      *out++ = (*in++) ^ u.c[i++];
    } while (i < l);
  }
  *p = old_ctr_value;
}

void vk_ssl_aes_ctr_crypt (vk_aes_ctx_t *ctx, const unsigned char *in, unsigned char *out, int size, unsigned char iv[16], unsigned long long offset) {
  unsigned long long *p = (unsigned long long *) (iv + 8);
  const unsigned long long old_ctr_value = *p;
  (*p) += offset >> 4;
  union {
    unsigned char c[16];
    unsigned long long d[2];
  } u;
  int i = offset & 15, l;
  if (i) {
    AES_encrypt (iv, u.c, &ctx->u.key);
    (*p)++;
    l = i + size;
    if (l > 16) {
      l = 16;
    }
    size -= l - i;
    do {
      *out++ = (*in++) ^ u.c[i++];
    } while (i < l);
  }
  const unsigned long long *I = (const unsigned long long *) in;
  unsigned long long *O = (unsigned long long *) out;
  int n = size >> 4;
  while (--n >= 0) {
    AES_encrypt (iv, (unsigned char *) u.d, &ctx->u.key);
    (*p)++;
    *O++ = (*I++) ^ u.d[0];
    *O++ = (*I++) ^ u.d[1];
  }
  in = (const unsigned char *) I;
  out = (unsigned char *) O;
  l = size & 15;
  if (l) {
    AES_encrypt (iv, u.c, &ctx->u.key);
    i = 0;
    do {
      *out++ = (*in++) ^ u.c[i++];
    } while (i < l);
  }
  *p = old_ctr_value;
}

void vk_aes_set_encrypt_key (vk_aes_ctx_t *ctx, unsigned char *key, int bits) {
  if (aesni256_is_supported () && bits == 256) {
    aesni256_set_encrypt_key (&ctx->u.ctx, key);
    ctx->cbc_crypt = vk_aesni_cbc_encrypt;
    ctx->ige_crypt = vk_aesni_ige_encrypt;
    ctx->ctr_crypt = vk_aesni_ctr_crypt;
  } else {
    AES_set_encrypt_key (key, bits, &ctx->u.key);
    ctx->cbc_crypt = vk_ssl_aes_cbc_encrypt;
    ctx->ige_crypt = vk_ssl_aes_ige_encrypt;
    ctx->ctr_crypt = vk_ssl_aes_ctr_crypt;
  }
}

void vk_aes_set_decrypt_key (vk_aes_ctx_t *ctx, unsigned char *key, int bits) {
  if (aesni256_is_supported () && bits == 256) {
    aesni256_set_decrypt_key (&ctx->u.ctx, key);
    ctx->cbc_crypt = vk_aesni_cbc_decrypt;
    ctx->ige_crypt = vk_aesni_ige_decrypt;
  } else {
    AES_set_decrypt_key (key, bits, &ctx->u.key);
    ctx->cbc_crypt = vk_ssl_aes_cbc_decrypt;
    ctx->ige_crypt = vk_ssl_aes_ige_decrypt;
  }
  ctx->ctr_crypt = NULL; /* NOTICE: vk_aes_set_encrypt_key should be used for CTR decryption */
}
