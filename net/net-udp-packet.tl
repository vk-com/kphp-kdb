---types---
/////
//
// Text
//
/////

//  Flag bits:

net.pid ip:int port_pid:int utime:int = net.Pid;

netUdpPacket.unencHeader flags:# remote_pid:flags.0?%Pid local_pid:flags.0?%Pid short_pids:flags.1?%(Tuple int 3) pid_hash:flags.2?long crypto_init:flags.3?int sha1:flags.4?%(Tuple int 16) random_key:flags.5?%(Tuple int 16) = netUdpPacket.UnencHeader;
netUdpPacket.encHeader flags:# random_block:flags.8?%(Tuple int 16) packet_ack:flags.12?int packet_ack_prefix:flags.13?int packet_ack_from:flags.14?int packet_ack_to:flags.14?int packet_ack_set:flags.15?%(Vector int) 
  packet_num:flags.20?int packets_from:flags.21?int packets_to:flags.21?int prev_parts:flags.22?int next_parts:flags.23?int packed_num_parts:flags.24?%(Tuple int 2) = netUdpPacket.EncHeader;

udp.range from:int to:int = udp.Range;
udp.resendRequest from:int to:int = udp.ResendRequest;
udp.resendRequestExt msg_ids:%(Vector %udp.Range) = udp.ResendRequest;
udp.nop = True;

udp.obsoletePid pid:%net.Pid real_pid:%net.Pid generation:int = udp.ObsoletePid;

udp.obsoleteHash hash:long real_pid:%net.Pid = udp.ObsoleteHash;

udp.obsoleteGeneration pid:%net.Pid generation:int = udp.ObsoleteGeneration;

udp.disableEncryption = udp.DisableEncryption;
---functions---
