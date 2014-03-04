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
              2009-2012 Nikolai Durov (original mc-proxy code)
              2009-2012 Andrei Lopatin (original mc-proxy code)
              2012-2013 Vitaliy Valtman
*/

#define _FILE_OFFSET_BITS 64
#include "rpc-proxy.h"
#include "net-rpc-targets.h"
#include "vv-tree.h"
#include "net-rpc-server.h"
#include "net-rpc-client.h"

#include <assert.h>

#define MAX_RETRIES 10

int extension_firstint_num;

unsigned hash_key (const char *key, int key_len) {
//  unsigned hash = (crc32 (key, key_len) >> 16) & 0x7fff;
  unsigned hash = (compute_crc32 (key, key_len) >> 8) & 0x7fffff;
  return hash ? hash : 1;
}

static unsigned long long extract_num (const char *key, int key_len, char **eptr) {
  const char *ptr = key, *ptr_e = key + key_len, *num_st;
  unsigned long long x;
  while (ptr < ptr_e && (*ptr < '0' || *ptr > '9')) {
    ptr++;
  }
  if (ptr == ptr_e) {
    if (eptr) {
      *eptr = (char *)ptr;
    }
    return (unsigned long long) -1;
  }
  do {
    num_st = ptr;
    x = 0;
    while (ptr < ptr_e && *ptr >= '0' && *ptr <= '9') {
      if (x >= 0x7fffffffffffffffLL / 10) {
        if (eptr) {
          *eptr = (char *)ptr;
        }
        return (unsigned long long) -1;
      }
      x = x*10 + (*ptr - '0');
      ptr++;
    }
    if (ptr == num_st) {
      if (eptr) {
        *eptr = (char *)ptr;
      }
      return (unsigned long long) -1;
    }
  } while (num_st == key && ptr < ptr_e && *ptr++ == '~');
  if (eptr) {
    *eptr = (char *)ptr;
  }
  return x;
}


int rpc_fun_kint_forward (void **IP, void **Data) {
  const char *key = *Data;
  int key_len = (long)*(Data + 1);
  int k = (long)CC->extensions_extra[extension_firstint_num];
  char *p1 = (char *) key, *p2;
  int clen = key_len;
  unsigned long long longhash = 0;
  int i;
  for (i = 0; i < k; i++) {
    longhash = extract_num (p1, clen, &p2);
    if ((long long) longhash == -1) {
      tl_fetch_set_error_format (TL_ERROR_PROXY_NO_TARGET, "Can not extract %d ints", k);
      return -1;
    }
    assert (p2 >= p1 && p2 <= p1 + clen);
    clen -= p2 - p1;
    p1 = p2;
  }
  if (CC->step > 0) {
    longhash /= CC->step;
  }
  *(Data + 2) = &CC->buckets[longhash % CC->tot_buckets];
  return 0;
}

int rpc_dot_extension (void **IP, void **Data) {
  const char *key = *Data;
  int key_len = (long)*(Data + 1);
  char *dot_pos = memchr (key, '.', key_len);
  if (dot_pos) {
    *(Data + 1) = (void *)(long)(dot_pos - key);
  }
  RPC_FUN_NEXT;
}

int rpc_persistent_forward (void **IP, void **Data) {
  const char *key = *Data;
  int key_len = (long)*(Data + 1);
  int z = 0;
  if (key_len >= 4 && *key == '#' && *(key + 1) == '#')  {
    z = 2;
    while (z < key_len && key[z] != '#') {
      z ++;
    }
    if (z < key_len - 1 && key[z] == '#' && key[z + 1] == '#') {
      z += 2;
    } else {
      z = 0;
    }
    if (z >= key_len) {
      z = 0;
    }
  }
  long long hash = hash_key (key + z, key_len - z);
  if (CC->step > 0) {
    hash /= CC->step;
  }
  *(Data + 2) = &CC->buckets[hash % CC->tot_buckets];
  return 0;
}

int rpc_memcached_forward (void **IP, void **Data) {
  const char *key = *Data;
  int key_len = (long)*(Data + 1);
  int z = 0;
  if (key_len >= 4 && *key == '#' && *(key + 1) == '#')  {
    z = 2;
    while (z < key_len && key[z] != '#') {
      z ++;
    }
    if (z < key_len - 1 && key[z] == '#' && key[z + 1] == '#') {
      z += 2;
    } else {
      z = 0;
    }
    if (z >= key_len) {
      z = 0;
    }
  }
  long long hash = hash_key (key + z, key_len - z);
  if (CC->step > 0) {
    hash /= CC->step;
  }
  struct rpc_cluster_bucket *B = &CC->buckets[hash % CC->tot_buckets];
  int i = 0;
  char key_buffer[key_len + 2];
  while (B->methods->get_state (B) < 0) {
    if (!i) {
      memcpy (key_buffer+2, key + z, key_len - z);
      key_buffer[1] = '0';
      key_buffer[0] = '0';
    }
    if (++i > MAX_RETRIES) {
      *(Data + 2) = 0;
      return 0;
    }
    key_buffer[1]++;
    if (i < 10) {
      hash += hash_key (key_buffer+1, key_len + 1 - z);
    } else {
      if (key_buffer[1] == ':') {
        key_buffer[1] = '0';
        key_buffer[0]++;
      }
      hash += hash_key (key_buffer, key_len + 2 - z);
    }
    B = &CC->buckets[hash % CC->tot_buckets];
  }
  if (B) {
    *(Data + 2) = B;
    return 0;
  } else {
    *(Data + 2) = 0;
    return -1;
  }
}

int rpc_kint_extension_add (struct rpc_cluster *C, struct rpc_cluster_create *Z, int flags, int k) {
  if (Z->lock & (1 << RPC_FUN_STRING_FORWARD)) {
    return -1;
  }
  Z->lock |= (1 << RPC_FUN_STRING_FORWARD);
   
  assert (Z->funs_last[RPC_FUN_STRING_FORWARD] > 0);
  Z->funs[RPC_FUN_STRING_FORWARD][--Z->funs_last[RPC_FUN_STRING_FORWARD]] = rpc_fun_kint_forward;
  C->extensions_extra[extension_firstint_num] = (void *)(long)k;
  C->cluster_mode = k + 1;
  return 0;
}


EXTENSION_ADD(firstint) {
  return rpc_kint_extension_add (C, Z, flags, 0);
}
EXTENSION_REGISTER_NUM(firstint, 0, extension_firstint_num)

EXTENSION_ADD(secondint) {
  return rpc_kint_extension_add (C, Z, flags, 1);
}
EXTENSION_REGISTER(secondint, 0)

EXTENSION_ADD(thirdint) {
  return rpc_kint_extension_add (C, Z, flags, 2);
}
EXTENSION_REGISTER(thirdint, 0)

EXTENSION_ADD(fourthint) {
  return rpc_kint_extension_add (C, Z, flags, 3);
}
EXTENSION_REGISTER(fourthint, 0)

EXTENSION_ADD(fifthint) {
  return rpc_kint_extension_add (C, Z, flags, 4);
}
EXTENSION_REGISTER(fifthint, 0)

EXTENSION_ADD(dot) {
  Z->lock |= (1 << RPC_FUN_STRING_FORWARD);
  assert (Z->funs_last[RPC_FUN_STRING_FORWARD] > 0);
  Z->funs[RPC_FUN_STRING_FORWARD][--Z->funs_last[RPC_FUN_STRING_FORWARD]] = rpc_dot_extension;
  return 0;
}
EXTENSION_REGISTER(dot, 1)

EXTENSION_ADD(persistent) {
  if (Z->lock & (1 << RPC_FUN_STRING_FORWARD)) {
    return -1;
  }
  Z->lock |= (1 << RPC_FUN_STRING_FORWARD);
 
  assert (Z->funs_last[RPC_FUN_STRING_FORWARD] > 0);
  Z->funs[RPC_FUN_STRING_FORWARD][--Z->funs_last[RPC_FUN_STRING_FORWARD]] = rpc_persistent_forward;
  return 0;
}
EXTENSION_REGISTER(persistent, 0)

EXTENSION_ADD(memcached) {
  if (Z->lock & (1 << RPC_FUN_STRING_FORWARD)) {
    return -1;
  }
  Z->lock |= (1 << RPC_FUN_STRING_FORWARD);
 
  assert (Z->funs_last[RPC_FUN_STRING_FORWARD] > 0);
  Z->funs[RPC_FUN_STRING_FORWARD][--Z->funs_last[RPC_FUN_STRING_FORWARD]] = rpc_memcached_forward;
  return 0;
}

EXTENSION_REGISTER(memcached, 0)
