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


#include <stdio.h>
#include <arpa/inet.h>
#include "net-events.h"
#include "dhcp-proto.h"
#include "server-functions.h"

static char *show_option (enum dhcp_option o) {
  switch (o) {
  case dhcpo_pad: return "Pad";
  case dhcpo_end: return "End";
  case dhcpo_subnet_mask: return "Subnet Mask";
  case dhcpo_time_offset: return "Time Offset";
  case dhcpo_router: return "Router";
  case dhcpo_time_server: return "Time Server";
  case dhcpo_name_server: return "Name Server";
  case dhcpo_domain_name_server: return "Domain Name Server";
  case dhcpo_log_server: return "Log Server";
  case dhcpo_cookie_server: return "Cookie Server";
  case dhcpo_lpr_server: return "LPR Server";
  case dhcpo_impress_server: return "Impress Server";
  case dhcpo_resource_location_server: return "Resource Location Server";
  case dhcpo_host_name: return "Host Name";
  case dhcpo_boot_file_size: return "Boot File Size";
  case dhcpo_merit_dump_file: return "Merit Dump File";
  case dhcpo_domain_name: return "Domain Name";
  case dhcpo_swap_server: return "Swap Server";
  case dhcpo_root_path: return "Root Path";
  case dhcpo_extensions_path: return "Extensions Path";
  case dhcpo_ip_forwarding: return "IP Forwarding";
  case dhcpo_non_local_source_routing: return "Non-Local Source Routing";
  case dhcpo_policy_filter: return "Policy Filter";
  case dhcpo_maximum_datagram_reassembly_size: return "Maximum Datagram Reassembly Size";
  case dhcpo_default_ip_time_to_live: return "Default IP Time-to-live";
  case dhcpo_path_mtu_aging_timeout: return "Path MTU Aging Timeout";
  case dhcpo_path_mtu_plateau_table: return "Path MTU Plateau Table";
  case dhcpo_interface_mtu_option: return "Interface MTU Option";
  case dhcpo_all_subnets_are_local: return "All Subnets are Local";
  case dhcpo_broadcast_address: return "Broadcast Address";
  case dhcpo_perform_mask_discovery: return "Perform Mask Discovery";
  case dhcpo_mask_supplier: return "Mask Supplier";
  case dhcpo_perform_router_discovery: return "Perform Router Discovery";
  case dhcpo_router_solicitation_address: return "Router Solicitation Address";
  case dhcpo_static_route: return "Static Route";
  case dhcpo_trailer_encapsulation: return "Trailer Encapsulation";
  case dhcpo_arp_cache_timeout: return "ARP Cache Timeout";
  case dhcpo_ethernet_encapsulation: return "Ethernet Encapsulation";
  case dhcpo_tcp_default_ttl: return "TCP Default TTL";
  case dhcpo_tcp_keepalive_interval: return "TCP Keepalive Interval";
  case dhcpo_tcp_keepalive_garbage: return "TCP Keepalive Garbage";
  case dhcpo_network_information_service_domain: return "Network Information Service Domain";
  case dhcpo_network_information_servers: return "Network Information Servers";
  case dhcpo_network_time_protocol_servers: return "Network Time Protocol Servers";
  case dhcpo_vendor_specific: return "Vendor Specific";
  case dhcpo_netbios_over_tcp_ip_name_server: return "NetBIOS over TCP/IP Name Server";
  case dhcpo_netbios_over_tcp_ip_datagram_distribution_server: return "NetBIOS over TCP/IP Datagram Distribution Server";
  case dhcpo_netbios_over_tcp_ip_node_type: return "NetBIOS over TCP/IP Node Type";
  case dhcpo_netbios_over_tcp_ip_scope: return "NetBIOS over TCP/IP Scope";
  case dhcpo_x_window_system_font_server: return "X Window System Font Server";
  case dhcpo_x_window_system_display_manager: return "X Window System Display Manager";
  case dhcpo_network_information_service_plus_domain: return "Network Information Service+ Domain";
  case dhcpo_network_information_service_plus_servers: return "Network Information Service+ Servers";
  case dhcpo_mobile_ip_home_agent: return "Mobile IP Home Agent";
  case dhcpo_simple_mail_transport_protocol_server: return "Simple Mail Transport Protocol Server";
  case dhcpo_post_office_protocol_server: return "Post Office Protocol Server";
  case dhcpo_network_news_transport_protocol_server: return "Network News Transport Protocol Server";
  case dhcpo_default_world_wide_web_server: return "Default World Wide Web Server";
  case dhcpo_default_finger_server: return "Default Finger Server";
  case dhcpo_default_internet_relay_chat_server: return "Default Internet Relay Chat Server";
  case dhcpo_streettalk_server: return "StreetTalk Server";
  case dhcpo_streettalk_directory_assistance_server: return "StreetTalk Directory Assistance Server";
  case dhcpo_requested_ip_address: return "Requested IP Address";
  case dhcpo_ip_address_lease_time: return "IP Address Lease Time";
  case dhcpo_overload: return "Overload";
  case dhcpo_tftp_server_name: return "TFTP server name";
  case dhcpo_bootfile_name: return "Bootfile name";
  case dhcpo_dhcp_message_type: return "DHCP Message Type";
  case dhcpo_server_identifier: return "Server Identifier";
  case dhcpo_parameter_request_list: return "Parameter Request List";
  case dhcpo_message: return "Message";
  case dhcpo_maximum_dhcp_message_size: return "Maximum DHCP Message Size";
  case dhcpo_renewal_time_value: return "Renewal Time Value";
  case dhcpo_rebinding_time_value: return "Rebinding Time Value";
  case dhcpo_vendor_class_identifier: return "Vendor class identifier";
  case dhcpo_client_identifier: return "Client-identifier";
  case dhcpo_domain_search: return "Domain Search";
  }
  static char s[128];
  snprintf (s, sizeof (s), "Unknown opcode %d", o);
  return s;
}

static char *show_message_type (enum dhcp_message_type t) {
  switch (t) {
  case dhcpmt_discover: return "DHCPDISCOVER";
  case dhcpmt_offer: return "DHCPOFFER";
  case dhcpmt_request: return "DHCPREQUEST";
  case dhcpmt_decline: return "DHCPDECLINE";
  case dhcpmt_ack: return "DHCPACK";
  case dhcpmt_nak: return "DHCPNAK";
  case dhcpmt_release: return "DHCPRELEASE";
  case dhcpmt_inform: return "DHCPINFORM";
  }
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


static int dhcp_option_check_len (enum dhcp_option o, int len) {
  switch (o) {
  case dhcpo_pad:
  case dhcpo_end:
    return len == 0;
  case dhcpo_ip_forwarding:
  case dhcpo_non_local_source_routing:
  case dhcpo_default_ip_time_to_live:
  case dhcpo_all_subnets_are_local:
  case dhcpo_perform_mask_discovery:
  case dhcpo_mask_supplier:
  case dhcpo_perform_router_discovery:
  case dhcpo_trailer_encapsulation:
  case dhcpo_ethernet_encapsulation:
  case dhcpo_tcp_default_ttl:
  case dhcpo_tcp_keepalive_garbage:
  case dhcpo_netbios_over_tcp_ip_node_type:
  case dhcpo_overload:
  case dhcpo_dhcp_message_type:
    return len == 1;
  case dhcpo_boot_file_size:
  case dhcpo_maximum_datagram_reassembly_size:
  case dhcpo_interface_mtu_option:
  case dhcpo_maximum_dhcp_message_size:
    return len == 2;
  case dhcpo_subnet_mask:
  case dhcpo_time_offset:
  case dhcpo_path_mtu_aging_timeout:
  case dhcpo_broadcast_address:
  case dhcpo_router_solicitation_address:
  case dhcpo_arp_cache_timeout:
  case dhcpo_tcp_keepalive_interval:
  case dhcpo_requested_ip_address:
  case dhcpo_ip_address_lease_time:
  case dhcpo_server_identifier:
  case dhcpo_renewal_time_value:
  case dhcpo_rebinding_time_value:
    return len == 4;
  case dhcpo_host_name:
  case dhcpo_merit_dump_file:
  case dhcpo_domain_name:
  case dhcpo_root_path:
  case dhcpo_extensions_path:
  case dhcpo_network_information_service_domain:
  case dhcpo_network_information_service_plus_domain:
  case dhcpo_tftp_server_name:
  case dhcpo_bootfile_name:
  case dhcpo_parameter_request_list:
  case dhcpo_message:
  case dhcpo_domain_search:
    return len >= 1;
  case dhcpo_path_mtu_plateau_table:
    return len >= 2 && !(len & 1);
  case dhcpo_mobile_ip_home_agent:
    return len >= 0 && !(len & 3);
  case dhcpo_router:
  case dhcpo_time_server:
  case dhcpo_name_server:
  case dhcpo_domain_name_server:
  case dhcpo_log_server:
  case dhcpo_cookie_server:
  case dhcpo_lpr_server:
  case dhcpo_impress_server:
  case dhcpo_resource_location_server:
  case dhcpo_swap_server:
  case dhcpo_network_information_servers:
  case dhcpo_network_time_protocol_servers:
  case dhcpo_netbios_over_tcp_ip_name_server:
  case dhcpo_netbios_over_tcp_ip_datagram_distribution_server:
  case dhcpo_x_window_system_font_server:
  case dhcpo_x_window_system_display_manager:
  case dhcpo_network_information_service_plus_servers:
  case dhcpo_simple_mail_transport_protocol_server:
  case dhcpo_post_office_protocol_server:
  case dhcpo_network_news_transport_protocol_server:
  case dhcpo_default_world_wide_web_server:
  case dhcpo_default_finger_server:
  case dhcpo_default_internet_relay_chat_server:
  case dhcpo_streettalk_server:
  case dhcpo_streettalk_directory_assistance_server:
    return len >= 4 && !(len & 3);
  case dhcpo_policy_filter:
  case dhcpo_static_route:
    return len >= 8 && !(len & 7);
  case dhcpo_vendor_specific:
  case dhcpo_netbios_over_tcp_ip_scope:
  case dhcpo_vendor_class_identifier:
  case dhcpo_client_identifier:
    return len >= 0;
  }
  return 0;
}

int dhcp_option_check (enum dhcp_option o, unsigned char *data, int len) {
  if (!dhcp_option_check_len (o, len)) {
    vkprintf (2, "%s: illegal data length (%d) for option '%s'.\n", __func__, len, show_option (o));
    return -1;
  }
  switch (o) {
  case dhcpo_ip_forwarding:
  case dhcpo_non_local_source_routing:
  case dhcpo_all_subnets_are_local:
  case dhcpo_perform_mask_discovery:
  case dhcpo_mask_supplier:
  case dhcpo_perform_router_discovery:
  case dhcpo_trailer_encapsulation:
  case dhcpo_ethernet_encapsulation:
  case dhcpo_tcp_keepalive_garbage:
    return (data[0] == 0 || data[0] == 1) ? 0 : -1;
  case dhcpo_dhcp_message_type:
    return (data[0] >= 1 && data[0] <= 8) ? 0 : -1;
  case dhcpo_overload: return (data[0] >= 1 && data[0] <= 3) ? 0 : -1;
  case dhcpo_netbios_over_tcp_ip_node_type: return (data[0] == 1 || data[0] == 2 || data[0] == 4 || data[0] == 8) ? 0 : -1;
  default: break;
  }
  return 0;
}

int dhcp_option_print (FILE *f, enum dhcp_option o, unsigned char *data, int len) {
  fprintf (f, "Option '%s': ", show_option (o));
  switch (o) {
  case dhcpo_pad:
  case dhcpo_end:
    break;
  case dhcpo_default_ip_time_to_live:
  case dhcpo_tcp_default_ttl:
  case dhcpo_netbios_over_tcp_ip_node_type:
  case dhcpo_overload:
    fprintf (f, "%d", (int) (data[0]));
    break;
  case dhcpo_ip_forwarding:
  case dhcpo_non_local_source_routing:
  case dhcpo_all_subnets_are_local:
  case dhcpo_perform_mask_discovery:
  case dhcpo_mask_supplier:
  case dhcpo_perform_router_discovery:
  case dhcpo_trailer_encapsulation:
  case dhcpo_ethernet_encapsulation:
  case dhcpo_tcp_keepalive_garbage:
    fprintf (f, "%s", (data[0]) ? "true" : "false");
    break;
  case dhcpo_dhcp_message_type:
    fprintf (f, "%s", show_message_type (data[0]));
    break;
  case dhcpo_boot_file_size:
  case dhcpo_maximum_datagram_reassembly_size:
  case dhcpo_interface_mtu_option:
  case dhcpo_maximum_dhcp_message_size:
    fprintf (f, "%d", ntohs (*((short *) data)));
    break;
  case dhcpo_time_offset:
  case dhcpo_path_mtu_aging_timeout:
  case dhcpo_arp_cache_timeout:
  case dhcpo_tcp_keepalive_interval:
  case dhcpo_ip_address_lease_time:
  case dhcpo_renewal_time_value:
  case dhcpo_rebinding_time_value:
    fprintf (f, "%u", ntohl (*((int *) data)));
    break;
  case dhcpo_parameter_request_list:
    print_options (f, data, len);
    break;
  case dhcpo_host_name:
  case dhcpo_merit_dump_file:
  case dhcpo_domain_name:
  case dhcpo_root_path:
  case dhcpo_extensions_path:
  case dhcpo_network_information_service_domain:
  case dhcpo_network_information_service_plus_domain:
  case dhcpo_tftp_server_name:
  case dhcpo_bootfile_name:
  case dhcpo_message:
  case dhcpo_domain_search:
    fprintf (f, "%.*s", len, data);
    break;
  case dhcpo_path_mtu_plateau_table:
    print_shorts (f, (short *) data, len / 2);
    break;
  case dhcpo_subnet_mask:
  case dhcpo_router:
  case dhcpo_time_server:
  case dhcpo_name_server:
  case dhcpo_domain_name_server:
  case dhcpo_log_server:
  case dhcpo_cookie_server:
  case dhcpo_lpr_server:
  case dhcpo_impress_server:
  case dhcpo_resource_location_server:
  case dhcpo_swap_server:
  case dhcpo_broadcast_address:
  case dhcpo_router_solicitation_address:
  case dhcpo_network_information_servers:
  case dhcpo_network_time_protocol_servers:
  case dhcpo_netbios_over_tcp_ip_name_server:
  case dhcpo_netbios_over_tcp_ip_datagram_distribution_server:
  case dhcpo_x_window_system_font_server:
  case dhcpo_x_window_system_display_manager:
  case dhcpo_network_information_service_plus_servers:
  case dhcpo_mobile_ip_home_agent:
  case dhcpo_simple_mail_transport_protocol_server:
  case dhcpo_post_office_protocol_server:
  case dhcpo_network_news_transport_protocol_server:
  case dhcpo_default_world_wide_web_server:
  case dhcpo_default_finger_server:
  case dhcpo_default_internet_relay_chat_server:
  case dhcpo_streettalk_server:
  case dhcpo_streettalk_directory_assistance_server:
  case dhcpo_requested_ip_address:
  case dhcpo_server_identifier:
    print_addrs (f, (int *) data, len / 4);
    break;
  case dhcpo_policy_filter:
  case dhcpo_static_route:
    print_addr_pairs (f, (int *) data, len / 8);
    break;
  case dhcpo_vendor_specific:
  case dhcpo_netbios_over_tcp_ip_scope:
  case dhcpo_vendor_class_identifier:
  case dhcpo_client_identifier:
    print_chars (f, data, len);
    break;
  }
  fprintf (f, "\n");
  return 0;
}
