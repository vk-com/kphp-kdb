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

#ifndef __DHCP_PROTO_H__
#define __DHCP_PROTO_H__

#define DHCP_MAGIC 0x63825363

enum dhcp_option {
  dhcpo_pad = 0,
  dhcpo_end = 255,
  dhcpo_subnet_mask = 1,
  dhcpo_time_offset = 2,
  dhcpo_router = 3,
  dhcpo_time_server = 4,
  dhcpo_name_server = 5,
  dhcpo_domain_name_server = 6,
  dhcpo_log_server = 7,
  dhcpo_cookie_server = 8,
  dhcpo_lpr_server = 9,
  dhcpo_impress_server = 10,
  dhcpo_resource_location_server = 11,
  dhcpo_host_name = 12,
  dhcpo_boot_file_size = 13,
  dhcpo_merit_dump_file = 14,
  dhcpo_domain_name = 15,
  dhcpo_swap_server = 16,
  dhcpo_root_path = 17,
  dhcpo_extensions_path = 18,
  dhcpo_ip_forwarding = 19,
  dhcpo_non_local_source_routing = 20,
  dhcpo_policy_filter = 21,
  dhcpo_maximum_datagram_reassembly_size = 22,
  dhcpo_default_ip_time_to_live = 23,
  dhcpo_path_mtu_aging_timeout = 24,
  dhcpo_path_mtu_plateau_table = 25,
  dhcpo_interface_mtu_option = 26,
  dhcpo_all_subnets_are_local = 27,
  dhcpo_broadcast_address = 28,
  dhcpo_perform_mask_discovery = 29,
  dhcpo_mask_supplier = 30,
  dhcpo_perform_router_discovery = 31,
  dhcpo_router_solicitation_address = 32,
  dhcpo_static_route = 33,
  dhcpo_trailer_encapsulation = 34,
  dhcpo_arp_cache_timeout = 35,
  dhcpo_ethernet_encapsulation = 36,
  dhcpo_tcp_default_ttl = 37,
  dhcpo_tcp_keepalive_interval = 38,
  dhcpo_tcp_keepalive_garbage = 39,
  dhcpo_network_information_service_domain = 40,
  dhcpo_network_information_servers = 41,
  dhcpo_network_time_protocol_servers = 42,
  dhcpo_vendor_specific = 43,
  dhcpo_netbios_over_tcp_ip_name_server = 44,
  dhcpo_netbios_over_tcp_ip_datagram_distribution_server = 45,
  dhcpo_netbios_over_tcp_ip_node_type = 46,
  dhcpo_netbios_over_tcp_ip_scope = 47,
  dhcpo_x_window_system_font_server = 48,
  dhcpo_x_window_system_display_manager = 49,
  dhcpo_network_information_service_plus_domain = 64,
  dhcpo_network_information_service_plus_servers = 65,
  dhcpo_mobile_ip_home_agent = 68,
  dhcpo_simple_mail_transport_protocol_server = 69,
  dhcpo_post_office_protocol_server = 70,
  dhcpo_network_news_transport_protocol_server = 71,
  dhcpo_default_world_wide_web_server = 72,
  dhcpo_default_finger_server = 73,
  dhcpo_default_internet_relay_chat_server = 74,
  dhcpo_streettalk_server = 75,
  dhcpo_streettalk_directory_assistance_server = 76,
  dhcpo_requested_ip_address = 50,
  dhcpo_ip_address_lease_time = 51,
  dhcpo_overload = 52,
  dhcpo_tftp_server_name = 66,
  dhcpo_bootfile_name = 67,
  dhcpo_dhcp_message_type = 53,
  dhcpo_server_identifier = 54,
  dhcpo_parameter_request_list = 55,
  dhcpo_message = 56,
  dhcpo_maximum_dhcp_message_size = 57,
  dhcpo_renewal_time_value = 58,
  dhcpo_rebinding_time_value = 59,
  dhcpo_vendor_class_identifier = 60,
  dhcpo_client_identifier = 61,
  dhcpo_domain_search = 119
};

enum dhcp_message_type {
  dhcpmt_discover = 1,
  dhcpmt_offer = 2,
  dhcpmt_request = 3,
  dhcpmt_decline = 4,
  dhcpmt_ack = 5,
  dhcpmt_nak = 6,
  dhcpmt_release = 7,
  dhcpmt_inform = 8
};

int dhcp_option_check (enum dhcp_option o, unsigned char *data, int len);
int dhcp_option_print (FILE *f, enum dhcp_option o, unsigned char *data, int len);
#endif
