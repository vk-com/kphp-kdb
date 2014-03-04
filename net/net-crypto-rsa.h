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

    Copyright 2011-2012 Vkontakte Ltd
              2011-2012 Anton Maydell
*/

#ifndef __VK_NET_CRYPTO_RSA_H__
#define	__VK_NET_CRYPTO_RSA_H__

int get_random_bytes (void *buf, int n);
int rsa_encrypt (const char *const private_key_name, void *input, int ilen, void **output, int *olen);
/* Warning: rsa_decrypt changes input buffer */
int rsa_decrypt (const char *const key_name, int public_key, void *input, int ilen, void **output, int *olen, int log_error_level);

#endif

