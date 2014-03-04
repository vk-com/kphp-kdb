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
                   2011 Anton Maydell
*/


#ifdef __cplusplus
extern "C" {
#endif

/* C version numberToBase62 function from global.php */
int number_to_base62 (long long number, char *output, int olen);
/* C version php base64_encode function */
/* returns 0: on success */
/* output contains zero terminated string base64 encoded string */
int base64_encode (const unsigned char* const input, int ilen, char *output, int olen);

/* returns negative value on error */
/* return os success number decoded bytes */
int base64_decode (const char *const input, unsigned char *output, int olen);

int base64url_encode (const unsigned char* const input, int ilen, char *output, int olen);

/* returns negative value on error */
/* return os success number decoded bytes */
int base64url_decode (const char *const input, unsigned char *output, int olen);

int base64_to_base64url (const char *const input, char *output, int olen);
int base64url_to_base64 (const char *const input, char *output, int olen);

#ifdef __cplusplus
}
#endif

