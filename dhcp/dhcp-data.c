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

#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <arpa/inet.h>
#include "server-functions.h"
#include "kdb-data-common.h"
#include "net-events.h"
#include "dhcp-data.h"

int server_ip = 0x0100007f;

void dhcp_message_print (dhcp_message_t *M, FILE *f) {
  int i;
  fprintf (f, "op: %d\n", (int) M->op);
  fprintf (f, "htype: %d\n", (int) M->htype);
  fprintf (f, "hlen: %d\n", (int) M->hlen);
  fprintf (f, "hops: %d\n", (int) M->hops);
  fprintf (f, "xid: %u\n", M->xid);
  fprintf (f, "secs: %d\n", (int) M->secs);
  fprintf (f, "flags: %d\n", (int) M->flags);
  fprintf (f, "ciaddr: %s\n", show_ip (M->ciaddr));
  fprintf (f, "yiaddr: %s\n", show_ip (M->yiaddr));
  fprintf (f, "siaddr: %s\n", show_ip (M->siaddr));
  fprintf (f, "giaddr: %s\n", show_ip (M->giaddr));
  fprintf (f, "chaddr:");
  for (i = 0; i < 16; i++) {
    fprintf (f, " %02x", (int) M->chaddr[i]);
  }
  fprintf (f, "\nsname: %.64s\n", M->sname);
  fprintf (f, "file: %.128s\n", M->file);
  fprintf (f, "%d options\n", M->options);
  for (i = 0; i < M->options; i++) {
    dhcp_option_print (f, M->O[i].o, M->O[i].data, M->O[i].len);
  }
}

int dhcp_message_parse (dhcp_message_t *M, unsigned char *in, int ilen) {
  if (ilen < 240) {
    vkprintf (1, "%s: message is too short (%d bytes).\n", __func__, ilen);
    return -1;
  }
  M->op = in[0];
  M->htype = in[1];
  M->hlen = in[2];
  M->hops = in[3];
  M->xid = ntohl (*((int *) (in + 4)));
  M->secs = ntohs (*((short *) (in + 8)));
  M->flags = ntohs (*((short *) (in + 10)));
  M->ciaddr = ntohl (*((int *) (in + 12)));
  M->yiaddr = ntohl (*((int *) (in + 16)));
  M->siaddr = ntohl (*((int *) (in + 20)));
  M->giaddr = ntohl (*((int *) (in + 24)));
  memcpy (M->chaddr, in + 28, 16);
  M->sname[63] = 0;
  strncpy ((char *) M->sname, (char *) in + 44, 64);
  M->file[127] = 0;
  strncpy ((char *) M->file, (char *) in + 108, 128);
  M->magic = ntohl (*((int *) (in + 236)));
  if (M->magic != DHCP_MAGIC) {
    vkprintf (1, "%s: expected DHCP magic(0x%08x), but 0x%08x found.\n", __func__, DHCP_MAGIC, M->magic);
    return -1;
  }
  M->options = 0;
  M->type = 0;
  int o = 240;
  while (o < ilen) {
    int opcode = in[o++];
    if (opcode == dhcpo_pad) {
      continue;
    }
    if (opcode == dhcpo_end) {
      break;
    }
    if (o >= ilen) {
      return -1;
    }
    int len = in[o++];
    if (len + o > ilen) {
      return -1;
    }
    if (M->options >= DHCP_MAX_OPTIONS) {
      vkprintf (2, "%s: too many options\n", __func__);
      return -1;
    }
    M->O[M->options].o = opcode;
    M->O[M->options].len = len;
    M->O[M->options].data = in + o;
    if (dhcp_option_check (opcode, in + o, len) < 0) {
      return -1;
    }
    if (opcode == dhcpo_dhcp_message_type) {
      if (M->type) {
        vkprintf (2, "%s: duplicate DHCP Message Type option\n", __func__);
        return -1;
      }
      M->type = in[0];
    }
    M->options++;
    o += len;
  }
  if (!M->type) {
    vkprintf (2, "%s: DHCP Message Type option wasn't found.\n", __func__);
    return -1;
  }
  return 0;
}

static int parse_macaddr (char *s, long long *macaddr) {
  int i;
  char *z = alloca (strlen (s)) + 1;
  strcpy (z, s);
  *macaddr = 0;
  for (i = 0; i < 6; i++) {
    char *p = strchr (z, ':');
    if (i < 5 && p == NULL) {
      kprintf ("%s(\"%s\"): ERROR. MAC-addr contains <5 colons.\n", __func__, s);
      return -1;
    }
    if (i == 5 && p != NULL) {
      kprintf ("%s(\"%s\"): ERROR. MAC-addr contains >5 colons.\n", __func__, s);
      return -1;
    }
    *p = 0;
    int x;
    if (sscanf (z, "%x", &x) != 1 || x < 0 || x > 255) {
      kprintf ("%s(\"%s\"): ERROR. Couldn't parse token '%s'.\n", __func__, s, z);
      return -1;
    }
    *macaddr <<= 8;
    *macaddr |= x;
    z = p + 1;
  }
  return 0;
}

static long long dhcp_macaddr_load (unsigned char *s) {
  unsigned long r = 0;
  int i = 0;
  do {
    r <<= 8;
    r |= s[i];
  } while (++i < 6);
  return r;
}

static char *show_macaddr (long long macaddr) {
  int i;
  static char z[6 * 3];
  for (i = 5; i >= 0; i--) {
    sprintf (z + i * 3, "%02x", (unsigned) (macaddr & 0xff));
    z[i * 3 + 2] = (i < 5) ? ':' : 0;
  }
  return z;
}

#define DHCP_HASH_PRIME 10007
dhcp_map_macaddr_ip_en_t *HMI[DHCP_HASH_PRIME];
int tot_macaddrs;
static dhcp_map_macaddr_ip_en_t *get_macaddr_f (long long macaddr, int force) {
  int h = ((unsigned long long) macaddr) % DHCP_HASH_PRIME;
  assert (h >= 0 && h < DHCP_HASH_PRIME);
  dhcp_map_macaddr_ip_en_t **p = HMI + h, *V;
  while (*p) {
    V = *p;
    if (macaddr == V->macaddr) {
      *p = V->hnext;
      if (force >= 0) {
        V->hnext = HMI[h];
        HMI[h] = V;
      }
      return V;
    }
    p = &V->hnext;
  }
  if (force > 0) {
    tot_macaddrs++;
    V = zmalloc0 (sizeof (dhcp_map_macaddr_ip_en_t));
    V->macaddr = macaddr;
    V->hnext = HMI[h];
    return HMI[h] = V;
  }
  return NULL;
}

int dhcp_config_load (const char *config_name) {
  FILE *f = fopen (config_name, "r");
  if (f == NULL) {
    kprintf ("%s: fopen (\"%s\", \"r\") fail. %m\n", __func__, config_name);
    return -1;
  }
  char buff[4096];
  int line = 0, res = 0;
  while (1) {
    line++;
    buff[sizeof (buff) - 1] = 0;
    if (fgets (buff, sizeof (buff), f) == NULL) {
      break;
    }
    if (buff[sizeof (buff) - 1]) {
      kprintf ("%s: line %d is too long\n", __func__, line);
      res--;
      break;
    }
    char *ptr;
    char *p[2];
    p[0] = strtok_r (buff, "\t\n ", &ptr);
    if (p[0] == NULL) {
      continue;
    }
    p[1] = strtok_r (NULL, "\t\n ", &ptr);
    if (p[1] == NULL) {
      kprintf ("%s: IP isn't found for MAC-addr '%s'. Config's line %d.\n", __func__, p[0], line);
      res--;
      break;
    }
    long long macaddr;
    if (parse_macaddr (p[0], &macaddr) < 0) {
      res--;
      break;
    }
    int ip;
    if (1 != inet_pton (AF_INET, p[1], &ip)) {
      kprintf ("%s: inet_pton (AF_INET, \"%s\") failed. Config's line %d.\n", __func__, p[1], line);
      res--;
      break;
    }
    int i = tot_macaddrs;
    dhcp_map_macaddr_ip_en_t *M = get_macaddr_f (macaddr, 1);
    if (i == tot_macaddrs) {
      kprintf ("%s: found duplicate MAC-addr '%s' at the config's line %d.\n", __func__, p[0], line);
      res--;
      break;
    }
    assert (tot_macaddrs == i + 1);
    M->ip = ntohl (ip);
  }
  fclose (f);
  return res;
}

static int process_discover (dhcp_message_t *M, unsigned char *out, int olen) {
  if (M->hlen != 6) {
    vkprintf (2, "%s: hlen(%d) isn't 6.\n", __func__, (int) M->hlen);
    return -1;
  }
  long long macaddr = dhcp_macaddr_load (M->chaddr);
  dhcp_map_macaddr_ip_en_t *m = get_macaddr_f (macaddr, 0);
  if (m == NULL) {
    vkprintf (2, "%s: unknown MAC-addr '%s'.\n", __func__, show_macaddr (macaddr));
    return -1;
  }

  memset (out, 0, 240);
  out[0] = 2; /* BOOTREPLY */
  out[1] = M->htype;
  out[2] = M->hlen;
  out[3] = M->hops;
  *((int *) (out + 4)) = htonl (M->xid);
  //M->secs = ntohs (*((short *) (in + 8)));
  //M->flags = ntohs (*((short *) (in + 10)));
  //M->ciaddr = ntohl (*((int *) (in + 12)));

  *((int *) (out + 16)) = htonl (m->ip); //yiaddr
  *((int *) (out + 20)) = htonl (server_ip); //siaddr

  //M->giaddr = ntohl (*((int *) (in + 24)));
  memcpy (out + 28, M->chaddr, 16);
  *((int *) (out + 236)) = htonl (DHCP_MAGIC);
  int wptr = 240;

  int i, requested_ip = 0;
  for (i = 0; i < M->options; i++) {
    if (M->O[i].o == dhcpo_requested_ip_address) {
      requested_ip = M->xid = ntohl (*((int *) M->O[i].data));
      break;
    }
  }

  //DHCP option 1: 255.255.255.0 subnet mask
  //DHCP option 3: 192.168.1.1 router
  //DHCP option 51: 86400s (1 day) IP lease time
  //DHCP option 54: 192.168.1.1 DHCP server
  //DHCP option 6: DNS servers 9.7.10.15, 9.7.10.16, 9.7.10.18

  return wptr;
}


int dhcp_query_act (dhcp_message_t *M, unsigned char *out, int olen) {
  if (M->op != 1) {
    vkprintf (2, "Op code isn't BOOTREQUEST.\n");
    return -1;
  }

  switch (M->type) {
    case dhcpmt_discover: return process_discover (M, out, olen);
    default: return -1;
  }
  return -1;
}
