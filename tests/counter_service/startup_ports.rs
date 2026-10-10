// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(target_os = "linux")]
use super::*;

#[test]
fn late_listener_reservations_avoid_kernel_client_ports() {
    // Prevent a concurrent child spawn from inheriting these parent handles
    // across the exact release/rebind check; child execution stays parallel.
    let _spawn = fixture_gate();
    // The recorded failure was allocation180, peer33426, during cold recovery.
    // Model used blocks locally, retaining actual global reservations separately.
    let mut global = PORT_BLOCKS.lock().unwrap_or_else(|e| e.into_inner());
    let mut used = global.clone();
    used.extend((PORT_BASE_START..PORT_BASE_END).step_by(128).take(180));
    let (base, tcp, udp) = reserve_ports(&mut used);
    global.insert(base);
    drop(global);
    let range = fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").unwrap();
    let range = range
        .split_whitespace()
        .map(|n| n.parse::<u16>().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(range.len(), 2);
    for listener in tcp.values() {
        let address = listener.local_addr().unwrap();
        assert!(
            address.port() < range[0] || address.port() > range[1],
            "late fixture listener {address} overlaps kernel client range {range:?}"
        );
        assert_eq!(
            TcpListener::bind(address).unwrap_err().kind(),
            std::io::ErrorKind::AddrInUse
        );
    }
    let tcp_address = tcp[&2].local_addr().unwrap();
    let udp_address = udp[1].local_addr().unwrap();
    assert_eq!(
        UdpSocket::bind(udp_address).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    drop(tcp);
    drop(udp);
    // No accepted connections here: owned placeholder release frees the ports.
    let _tcp = TcpListener::bind(tcp_address).unwrap();
    let _udp = UdpSocket::bind(udp_address).unwrap();
}
