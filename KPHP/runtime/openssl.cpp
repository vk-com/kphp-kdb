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
              2012-2013 Arseny Smirnov
              2012-2013 Aliaksei Levin
*/

#define _FILE_OFFSET_BITS 64

#include <openssl/err.h>
#include <openssl/hmac.h>
#include <openssl/md5.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/sha.h>

#include <unistd.h>

#include "common/crc32.h"

#include "openssl.h"

#include "files.h"
#include "string_functions.h"

#include "gost_hash.h"

array <string> f$hash_algos (void) {
  return array <string> (
      string ("sha1", 4),
      string ("sha256", 6),
      string ("md5", 3),
      string ("gost", 4));
}

string f$hash (const string &algo, const string &s, bool raw_output) {
  if (!strcmp (algo.c_str(), "sha256")) {
    string res;
    if (raw_output) {
      res.assign ((dl::size_type)32, false);
    } else {
      res.assign ((dl::size_type)64, false);
    }

    SHA256 (reinterpret_cast <const unsigned char *> (s.c_str()), (unsigned long)s.size(), reinterpret_cast <unsigned char *> (res.buffer()));

    if (!raw_output) {
      for (int i = 31; i >= 0; i--) {
        res[2 * i + 1] = lhex_digits[res[i] & 15];
        res[2 * i] = lhex_digits[(res[i] >> 4) & 15];
      }
    }
    return res;
  } else if (!strcmp (algo.c_str(), "md5")) {
    return f$md5 (s, raw_output);
  } else if (!strcmp (algo.c_str(), "gost")) {
    return f$gost (s, raw_output);
  } else if (!strcmp (algo.c_str(), "sha1")) {
    return f$sha1 (s, raw_output);
  }

  php_critical_error ("algo %s not supported in function hash", algo.c_str());
}

string f$hash_hmac (const string &algo, const string &data, const string &key, bool raw_output) {
  if (!strcmp (algo.c_str(), "sha1")) {
    string res;
    if (raw_output) {
      res.assign ((dl::size_type)20, false);
    } else {
      res.assign ((dl::size_type)40, false);
    }

    unsigned int md_len;
    HMAC (EVP_sha1(), static_cast <const void *> (key.c_str()), (int)key.size(),
                      reinterpret_cast <const unsigned char *> (data.c_str()), (int)data.size(),
                      reinterpret_cast <unsigned char *> (res.buffer()), &md_len);
    php_assert (md_len == 20);

    if (!raw_output) {
      for (int i = 19; i >= 0; i--) {
        res[2 * i + 1] = lhex_digits[res[i] & 15];
        res[2 * i] = lhex_digits[(res[i] >> 4) & 15];
      }
    }
    return res;
  }

  php_critical_error ("unsupported algo = \"%s\" in function hash_hmac", algo.c_str());
  return string();
}

string f$sha1 (const string &s, bool raw_output) {
  string res;
  if (raw_output) {
    res.assign ((dl::size_type)20, false);
  } else {
    res.assign ((dl::size_type)40, false);
  }

  SHA1 (reinterpret_cast <const unsigned char *> (s.c_str()), (unsigned long)s.size(), reinterpret_cast <unsigned char *> (res.buffer()));

  if (!raw_output) {
    for (int i = 19; i >= 0; i--) {
      res[2 * i + 1] = lhex_digits[res[i] & 15];
      res[2 * i] = lhex_digits[(res[i] >> 4) & 15];
    }
  }
  return res;
}


string f$md5 (const string &s, bool raw_output) {
  string res;
  if (raw_output) {
    res.assign ((dl::size_type)16, false);
  } else {
    res.assign ((dl::size_type)32, false);
  }

  MD5 (reinterpret_cast <const unsigned char *> (s.c_str()), (unsigned long)s.size(), reinterpret_cast <unsigned char *> (res.buffer()));

  if (!raw_output) {
    for (int i = 15; i >= 0; i--) {
      res[2 * i + 1] = lhex_digits[res[i] & 15];
      res[2 * i] = lhex_digits[(res[i] >> 4) & 15];
    }
  }
  return res;
}

string f$gost (const string &s, bool raw_output) {
  string res;
  if (raw_output) {
    res.assign ((dl::size_type)32, false);
  } else {
    res.assign ((dl::size_type)64, false);
  }

  Hash::Gost_ctx* hash_ctx = new Hash::Gost_ctx;
  if (hash_ctx) {
    hash_ctx->init(NULL, NULL);
    hash_ctx->update(reinterpret_cast <const unsigned char *> (s.c_str()), (unsigned long)s.size());
    hash_ctx->finish(reinterpret_cast <unsigned char *>(res.buffer()));
    delete hash_ctx;
  }

  if (!raw_output) {
    for (int i = 31; i >= 0; i--) {
      res[2 * i + 1] = lhex_digits[res[i] & 15];
      res[2 * i] = lhex_digits[(res[i] >> 4) & 15];
    }
  }
  return res;
}

string f$md5_file (const string &file_name, bool raw_output) {
  dl::enter_critical_section();//OK
  struct stat stat_buf;
  int read_fd = open (file_name.c_str(), O_RDONLY);
  if (read_fd < 0) {
    dl::leave_critical_section();
    return string();
  }
  if (fstat (read_fd, &stat_buf) < 0) {
    close (read_fd);
    dl::leave_critical_section();
    return string();
  }

  if (!S_ISREG (stat_buf.st_mode)) {
    close (read_fd);
    dl::leave_critical_section();
    php_warning ("Regular file expected in function md5_file, \"%s\" is given", file_name.c_str());
    return string();
  }

  MD5_CTX c;
  php_assert (MD5_Init (&c) == 1);

  size_t size = stat_buf.st_size;
  while (size > 0) {
    size_t len = min (size, (size_t)PHP_BUF_LEN);
    if (read_safe (read_fd, php_buf, len) < (ssize_t)len) {
      break;
    }
    php_assert (MD5_Update (&c, static_cast <const void *> (php_buf), (unsigned long)len) == 1);
    size -= len;
  }
  close (read_fd);
  php_assert (MD5_Final (reinterpret_cast <unsigned char *> (php_buf), &c) == 1);
  dl::leave_critical_section();

  if (size > 0) {
    php_warning ("Error while reading file \"%s\"", file_name.c_str());
    return string();
  }

  if (!raw_output) {
    string res (32, false);
    for (int i = 15; i >= 0; i--) {
      res[2 * i + 1] = lhex_digits[php_buf[i] & 15];
      res[2 * i] = lhex_digits[(php_buf[i] >> 4) & 15];
    }
    return res;
  } else {
    return string (php_buf, 16);
  }
}

int f$crc32 (const string &s) {
  return compute_crc32 ((const void *)s.c_str(), s.size());
}

int f$crc32_file (const string &file_name) {
  dl::enter_critical_section();//OK
  struct stat stat_buf;
  int read_fd = open (file_name.c_str(), O_RDONLY);
  if (read_fd < 0) {
    dl::leave_critical_section();
    return -1;
  }
  if (fstat (read_fd, &stat_buf) < 0) {
    close (read_fd);
    dl::leave_critical_section();
    return -1;
  }

  if (!S_ISREG (stat_buf.st_mode)) {
    close (read_fd);
    dl::leave_critical_section();
    php_warning ("Regular file expected in function crc32_file, \"%s\" is given", file_name.c_str());
    return -1;
  }

  int res = -1;
  size_t size = stat_buf.st_size;
  while (size > 0) {
    size_t len = min (size, (size_t)PHP_BUF_LEN);
    if (read_safe (read_fd, php_buf, len) < (ssize_t)len) {
      break;
    }
    res = crc32_partial (php_buf, (int)len, res);
    size -= len;
  }
  close (read_fd);
  dl::leave_critical_section();

  if (size > 0) {
    return -1;
  }

  return res ^ -1;
}


static char openssl_pkey_storage[sizeof (array <EVP_PKEY *>)];
static array <EVP_PKEY *> *openssl_pkey = (array <EVP_PKEY *> *)openssl_pkey_storage;
static long long openssl_pkey_last_query_num = -1;

static EVP_PKEY *openssl_get_evp (const string &key, const string &passphrase, bool is_public, bool *from_cache) {
  int num = 0;
  if (openssl_pkey_last_query_num == dl::query_num && key[0] == ':' && key[1] == ':' && sscanf (key.c_str() + 2, "%d", &num) == 1 && (unsigned int)num < (unsigned int)openssl_pkey->count()) {
    *from_cache = true;
    return openssl_pkey->get_value (num);
  }

  dl::enter_critical_section();//OK
  BIO *in = BIO_new_mem_buf ((void *)key.c_str(), key.size());
  if (in == NULL) {
    dl::leave_critical_section();
    return NULL;
  }

  EVP_PKEY *evp_pkey = is_public ? PEM_read_bio_PUBKEY (in, NULL, NULL, NULL) : PEM_read_bio_PrivateKey (in, NULL, NULL, (void *)passphrase.c_str());
/*
  ERR_load_crypto_strings();
  if (evp_pkey == NULL) {
    unsigned long val;
    while ((val = ERR_get_error())) {
      fprintf (stderr, "%s\n", ERR_error_string (val, NULL));
    }
  }
*/
  BIO_free (in);
  dl::leave_critical_section();

  *from_cache = false;
  return evp_pkey;
}

bool f$openssl_public_encrypt (const string &data, string &result, const string &key) {
  dl::enter_critical_section();//OK
  bool from_cache;
  EVP_PKEY *pkey = openssl_get_evp (key, string(), true, &from_cache);
  if (pkey == NULL) {
    dl::leave_critical_section();

    php_warning ("Parameter key is not a valid public key");
    result = string();
    return false;
  }
  if (pkey->type != EVP_PKEY_RSA && pkey->type != EVP_PKEY_RSA2) {
    if (!from_cache) {
      EVP_PKEY_free (pkey);
    }
    dl::leave_critical_section();

    php_warning ("Key type is neither RSA nor RSA2");
    result = string();
    return false;
  }

  int key_size = EVP_PKEY_size (pkey);
  php_assert (PHP_BUF_LEN >= key_size);

  if (RSA_public_encrypt ((int)data.size(), reinterpret_cast <const unsigned char *> (data.c_str()), 
                          reinterpret_cast <unsigned char *> (php_buf), pkey->pkey.rsa, RSA_PKCS1_PADDING) != key_size) {
    if (!from_cache) {
      EVP_PKEY_free (pkey);
    }
    dl::leave_critical_section();

    php_warning ("RSA public encrypt failed");
    result = string();
    return false;
  }

  if (!from_cache) {
    EVP_PKEY_free (pkey);
  }
  dl::leave_critical_section();

  result = string (php_buf, key_size);
  return true;
}

bool f$openssl_public_encrypt (const string &data, var &result, const string &key) {
  string result_string;
  if (f$openssl_public_encrypt (data, result_string, key)) {
    result = result_string;
    return true;
  }
  result = var();
  return false;
}

bool f$openssl_private_decrypt (const string &data, string &result, const string &key) {
  dl::enter_critical_section();//OK
  bool from_cache;
  EVP_PKEY *pkey = openssl_get_evp (key, string(), false, &from_cache);
  if (pkey == NULL) {
    dl::leave_critical_section();
    php_warning ("Parameter key is not a valid private key");
    return false;
  }
  if (pkey->type != EVP_PKEY_RSA && pkey->type != EVP_PKEY_RSA2) {
    if (!from_cache) {
      EVP_PKEY_free (pkey);
    }
    dl::leave_critical_section();
    php_warning ("Key type is not an RSA nor RSA2");
    return false;
  }

  int key_size = EVP_PKEY_size (pkey);
  php_assert (PHP_BUF_LEN >= key_size);

  int len = RSA_private_decrypt ((int)data.size(), reinterpret_cast <const unsigned char *> (data.c_str()),
                                 reinterpret_cast <unsigned char *> (php_buf), pkey->pkey.rsa, RSA_PKCS1_PADDING);
  if (!from_cache) {
    EVP_PKEY_free (pkey);
  }
  dl::leave_critical_section();
  if (len == -1) {
    php_warning ("RSA private decrypt failed");
    result = string();
    return false;
  }

  result.assign (php_buf, len);
  return true;
}

bool f$openssl_private_decrypt (const string &data, var &result, const string &key) {
  string result_string;
  if (f$openssl_private_decrypt (data, result_string, key)) {
    result = result_string;
    return true;
  }
  result = var();
  return false;
}

OrFalse <string> f$openssl_pkey_get_private (const string &key, const string &passphrase) {
  dl::enter_critical_section();//NOT OK: openssl_pkey

  bool from_cache;
  EVP_PKEY *pkey = openssl_get_evp (key, passphrase, false, &from_cache);

  if (pkey == NULL) {
    dl::leave_critical_section();

    php_warning ("Parameter key is not a valid key or passphrase is not a valid password");
    return false;
  }

  if (from_cache) {
    dl::leave_critical_section();
    return key;
  }

  if (dl::query_num != openssl_pkey_last_query_num) {
    new (openssl_pkey_storage) array <EVP_PKEY *>();
    openssl_pkey_last_query_num = dl::query_num;
  }

  string result (2, ':');
  result.append (openssl_pkey->count());
  openssl_pkey->push_back (pkey);
  dl::leave_critical_section();

  return result;
}


void openssl_init_static (void) {
  dl::enter_critical_section();//OK
//  OpenSSL_add_all_ciphers();
//  OpenSSL_add_all_digests();
  OpenSSL_add_all_algorithms();
  dl::leave_critical_section();
}

void openssl_free_static (void) {
  dl::enter_critical_section();//OK
  if (dl::query_num == openssl_pkey_last_query_num) {
    const array <EVP_PKEY *> *const_openssl_pkey = openssl_pkey;
    for (array <EVP_PKEY *>::const_iterator p = const_openssl_pkey->begin(); p != const_openssl_pkey->end(); ++p) {
      EVP_PKEY_free (p.get_value());
    }
    openssl_pkey_last_query_num--;
  }

  EVP_cleanup();
  dl::leave_critical_section();
}

