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

    Copyright 2013 Vkontakte Ltd
              2013 Anton Maydell
*/

#ifndef __DHCP_DATA_H__
#define __DHCP_DATA_H__

#include "dhcp-proto.h"
#define DHCP_MAX_OPTIONS 512

typedef struct {
  unsigned char op;    /* +0 */
  unsigned char htype; /* +1 */
  unsigned char hlen;  /* +2 */
  unsigned char hops;  /* +3 */
  unsigned int xid;    /* +4 */
  unsigned short secs; /* +8 */
  unsigned short flags;/* +10 */
  unsigned int ciaddr; /* +12 */
  unsigned int yiaddr; /* +16 */
  unsigned int siaddr; /* +20 */
  unsigned int giaddr; /* +24 */
  unsigned char chaddr[16]; /* +28 */
  unsigned char sname[64]; /* +44 */
  unsigned char file[128]; /* +108 */
  int magic;
  enum dhcp_message_type type;
  int options;
  struct {
    enum dhcp_option o;
    int len;
    unsigned char *data;
  } O[DHCP_MAX_OPTIONS];
} dhcp_message_t;

typedef struct dhcp_map_macaddr_ip_en {
  long long macaddr;
  struct dhcp_map_macaddr_ip_en *hnext;
  int ip;
} dhcp_map_macaddr_ip_en_t;

int dhcp_config_load (const char *config_name);
void dhcp_message_print (dhcp_message_t *M, FILE *f);
int dhcp_message_parse (dhcp_message_t *M, unsigned char *in, int ilen);

#endif
