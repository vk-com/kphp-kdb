import os

#RFC3397: Dynamic Host Configuration Protocol (DHCP) Domain Search Option

msg_types = [ (1, 'DHCPDISCOVER'), (2, 'DHCPOFFER'), (3, 'DHCPREQUEST'), (4, 'DHCPDECLINE'), (5, 'DHCPACK'), (6, 'DHCPNAK'), (7, 'DHCPRELEASE'), (8, 'DHCPINFORM')]

options = [\
(0, 'Pad', 'null'), \
(255, 'End', 'null'), \
(1, 'Subnet Mask', 'addr'), \
(2, 'Time Offset', 'int'), \
(3, 'Router', 'addrs'), \
(4, 'Time Server', 'addrs'), \
(5, 'Name Server', 'addrs'), \
(6, 'Domain Name Server', 'addrs'), \
(7, 'Log Server', 'addrs'), \
(8, 'Cookie Server', 'addrs'), \
(9, 'LPR Server', 'addrs'), \
(10, 'Impress Server', 'addrs'), \
(11, 'Resource Location Server', 'addrs'), \
(12, 'Host Name', 'string'), \
(13, 'Boot File Size', 'short'), \
(14, 'Merit Dump File', 'string'), \
(15, 'Domain Name', 'string'), \
(16, 'Swap Server', 'addrs'), \
(17, 'Root Path', 'string'), \
(18, 'Extensions Path', 'string'), \
(19, 'IP Forwarding', 'bool'), \
(20, 'Non-Local Source Routing', 'bool'), \
(21, 'Policy Filter', 'addr_pairs'), \
(22, 'Maximum Datagram Reassembly Size', 'short'), \
(23, 'Default IP Time-to-live', 'char'), \
(24, 'Path MTU Aging Timeout', 'int'), \
(25, 'Path MTU Plateau Table', 'shorts'), \
(26, 'Interface MTU Option', 'short'), \
(27, 'All Subnets are Local', 'bool'), \
(28, 'Broadcast Address', 'addr'), \
(29, 'Perform Mask Discovery', 'bool'), \
(30, 'Mask Supplier', 'bool'), \
(31, 'Perform Router Discovery', 'bool'), \
(32, 'Router Solicitation Address', 'addr'), \
(33, 'Static Route', 'addr_pairs'), \
(34, 'Trailer Encapsulation', 'bool'), \
(35, 'ARP Cache Timeout', 'int'), \
(36, 'Ethernet Encapsulation', 'bool'), \
(37, 'TCP Default TTL', 'char'), \
(38, 'TCP Keepalive Interval', 'int'), \
(39, 'TCP Keepalive Garbage', 'bool'), \
(40, 'Network Information Service Domain', 'string'), \
(41, 'Network Information Servers', 'addrs'), \
(42, 'Network Time Protocol Servers', 'addrs'), \
(43, 'Vendor Specific', 'chars'), \
(44, 'NetBIOS over TCP/IP Name Server', 'addrs'), \
(45, 'NetBIOS over TCP/IP Datagram Distribution Server', 'addrs'), \
(46, 'NetBIOS over TCP/IP Node Type', 'char'), \
(47, 'NetBIOS over TCP/IP Scope', 'chars'), \
(48, 'X Window System Font Server', 'addrs'), \
(49, 'X Window System Display Manager', 'addrs'), \
(64, 'Network Information Service+ Domain', 'string'), \
(65, 'Network Information Service+ Servers', 'addrs'), \
(68, 'Mobile IP Home Agent', 'maybe_addrs'), \
(69, 'Simple Mail Transport Protocol Server', 'addrs'), \
(70, 'Post Office Protocol Server', 'addrs'), \
(71, 'Network News Transport Protocol Server', 'addrs'), \
(72, 'Default World Wide Web Server', 'addrs'), \
(73, 'Default Finger Server', 'addrs'), \
(74, 'Default Internet Relay Chat Server', 'addrs'), \
(75, 'StreetTalk Server', 'addrs'), \
(76, 'StreetTalk Directory Assistance Server', 'addrs'), \
(50, 'Requested IP Address', 'addr'), \
(51, 'IP Address Lease Time', 'int'), \
(52, 'Overload', 'char'), \
(66, 'TFTP server name', 'string'), \
(67, 'Bootfile name', 'string'), \
(53, 'DHCP Message Type', 'message_type'), \
(54, 'Server Identifier', 'addr'), \
(55, 'Parameter Request List', 'opcodes'), \
(56, 'Message', 'string'), \
(57, 'Maximum DHCP Message Size', 'short'), \
(58, 'Renewal Time Value', 'int'), \
(59, 'Rebinding Time Value', 'int'), \
(60, 'Vendor class identifier', 'chars'), \
(61, 'Client-identifier', 'chars'), \
(119, 'Domain Search', 'string'), \
]

def case_type(f, options, t, action, indent = '  '):
  k = 0
  for opcode, name, tp in options:
    if (isinstance(t, list) and (tp in t)) or (tp == t):
      k += 1
      f.write(indent + 'case ' + coption (name) + ':\n')
  assert(k > 0)
  f.write(indent + '  ' + action + '\n')

def coption(s):
  return 'dhcpo_' + s.replace('-', '_').replace('+',' Plus').replace('/',' ').replace(' ','_').lower()

def ctype(s):
  return 'dhcpmt_' + s[4:].lower()

def br(f): f.write('    break;\n')

#DST_DIR = os.path.expanduser('~/tmp/.dhcp')
DST_DIR = os.path.expanduser('~/engine/src/dhcp')
if not os.path.lexists(DST_DIR): os.mkdir(DST_DIR, 0700)

h = open(os.path.join(DST_DIR, 'dhcp-proto.h'), 'w')
c = open(os.path.join(DST_DIR, 'dhcp-proto.c'), 'w')
h.write('''#ifndef __DHCP_PROTO_H__
#define __DHCP_PROTO_H__

#define DHCP_MAGIC 0x63825363

enum dhcp_option {
''')
d = {}
i = 0
c.write('''
#include <stdio.h>
#include <arpa/inet.h>
#include "net-events.h"
#include "dhcp-proto.h"
#include "server-functions.h"

static char *show_option (enum dhcp_option o) {
  switch (o) {
''')
for opcode, name, tp in options:
  assert(0 <= opcode < 256)
  assert(d.get(opcode) == None)
  d[opcode] = (name, tp)
  if i > 0: h.write (',\n')
  h.write('  ' + coption(name) + ' = ' + str(opcode))
  c.write('  case ' + coption(name) + ': return "' + name + '";\n')
  i += 1
h.write('\n};\n')
c.write('''  }
  static char s[128];
  snprintf (s, sizeof (s), "Unknown opcode %d", o);
  return s;
}
''')
i = 0
c.write('''
static char *show_message_type (enum dhcp_message_type t) {
  switch (t) {
''')

h.write('''
enum dhcp_message_type {
''')

for t, name in msg_types:
  if i > 0: h.write (',\n')
  h.write('  ' + ctype(name) + ' = ' + str(t))
  c.write('  case ' + ctype(name) + ': return "' + name + '";\n')
  i += 1
h.write('\n};\n')
c.write('''  }
  return NULL;
}

static void print_options (FILE *f, unsigned char *data, int n) {
  int i;
  for (i = 0; i < n; i++) {
    fprintf (f, "%s '%s'", (i > 0) ? "," : "", show_option (data[i]));
  }
}

static void print_shorts (FILE *f, short *data, int n) {
  int i;
  for (i = 0; i < n; i++) {
    fprintf (f, " %d", (int) ntohs (data[i]));
  }
}

static void print_addrs (FILE *f, int *data, int n) {
  int i;
  for (i = 0; i < n; i++) {
    fprintf (f, " %s", show_ip (ntohl (data[i])));
  }
}

static void print_addr_pairs (FILE *f, int *data, int n) {
  int i;
  for (i = 0; i < n; i++) {
    fprintf (f, " (%s, %s)", show_ip (ntohl (data[2*i])), show_ip (ntohl (data[2*i+1])));
  }
}

static void print_chars (FILE *f, unsigned char *data, int n) {
  int i;
  for (i = 0; i < n; i++) {
    fprintf (f, " %02x", (int) data[i]);
  }
}

''')

h.write('''
int dhcp_option_check (enum dhcp_option o, unsigned char *data, int len);
int dhcp_option_print (FILE *f, enum dhcp_option o, unsigned char *data, int len);
#endif
''')

c.write('''
static int dhcp_option_check_len (enum dhcp_option o, int len) {
  switch (o) {
''')
case_type(c, options, 'null', 'return len == 0;')
case_type(c, options, ['char', 'bool', 'message_type'], 'return len == 1;')
case_type(c, options, 'short', 'return len == 2;')
case_type(c, options, ['int', 'addr'], 'return len == 4;')
case_type(c, options, ['string', 'opcodes'], 'return len >= 1;')
case_type(c, options, 'shorts', 'return len >= 2 && !(len & 1);')
case_type(c, options, 'maybe_addrs', 'return len >= 0 && !(len & 3);')
case_type(c, options, 'addrs', 'return len >= 4 && !(len & 3);')
case_type(c, options, 'addr_pairs', 'return len >= 8 && !(len & 7);')
case_type(c, options, 'chars', 'return len >= 0;')
c.write('''  }
  return 0;
}

int dhcp_option_check (enum dhcp_option o, unsigned char *data, int len) {
  if (!dhcp_option_check_len (o, len)) {
    vkprintf (2, "%s: illegal data length (%d) for option '%s'.\\n", __func__, len, show_option (o));
    return -1;
  }
  switch (o) {
''')
case_type(c, options, 'bool', 'return (data[0] == 0 || data[0] == 1) ? 0 : -1;')
case_type(c, options, 'message_type', 'return (data[0] >= 1 && data[0] <= 8) ? 0 : -1;')
c.write('  case dhcpo_overload: return (data[0] >= 1 && data[0] <= 3) ? 0 : -1;\n')
c.write('  case dhcpo_netbios_over_tcp_ip_node_type: return (data[0] == 1 || data[0] == 2 || data[0] == 4 || data[0] == 8) ? 0 : -1;\n')
c.write('  default: break;\n')
c.write('''  }
  return 0;
}

int dhcp_option_print (FILE *f, enum dhcp_option o, unsigned char *data, int len) {
  fprintf (f, "Option '%s': ", show_option (o));
  switch (o) {
''')
case_type(c, options, 'null', 'break;')
case_type(c, options, 'char', 'fprintf (f, "%d", (int) (data[0]));')
br(c)
case_type(c, options, 'bool', 'fprintf (f, "%s", (data[0]) ? "true" : "false");')
br(c)
case_type(c, options, 'message_type', 'fprintf (f, "%s", show_message_type (data[0]));')
br(c)
case_type(c, options, 'short', 'fprintf (f, "%d", ntohs (*((short *) data)));')
br(c)
case_type(c, options, 'int', 'fprintf (f, "%u", ntohl (*((int *) data)));')
br(c)
case_type(c, options, 'opcodes', 'print_options (f, data, len);')
br(c)
case_type(c, options, 'string', 'fprintf (f, "%.*s", len, data);')
br(c)
case_type(c, options, 'shorts', 'print_shorts (f, (short *) data, len / 2);')
br(c)
case_type(c, options, ['addr', 'maybe_addrs', 'addrs'], 'print_addrs (f, (int *) data, len / 4);')
br(c)
case_type(c, options, 'addr_pairs', 'print_addr_pairs (f, (int *) data, len / 8);')
br(c)
case_type(c, options, 'chars', 'print_chars (f, data, len);')
br(c)
c.write('''  }
  fprintf (f, "\\n");
  return 0;
}
''')

h.close()
c.close()

