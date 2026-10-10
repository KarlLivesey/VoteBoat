// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::native::connect::NativePeerProtocol;

#[test]
fn native_shutdown_retains_elapsed_host_epoch_and_reopens_owned_resources() {
    let protocols = [
        NativePeerProtocol::TcpTls,
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic,
    ];
    for protocol in protocols {
        let directory = root();
        let (tcp, udp) = (0..32)
            .find_map(|_| {
                let tcp = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
                let udp = std::net::UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                Some((tcp, udp))
            })
            .unwrap();
        let address = tcp.local_addr().unwrap();
        drop((tcp, udp));
        let clock = Instant::now().checked_sub(Duration::from_secs(20)).unwrap();
        let mut selected = config(directory.clone(), NativeOpenMode::Create);
        selected.listen = address;
        let mut node = selected
            .open_with_protocol_and_timers(
                protocol,
                NativeTimingProfile::Throughput.timers(),
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(20000),
            )
            .unwrap();
        let now = MonoTime(clock.elapsed().as_millis() as u64);
        assert!(now.0 >= 20000);
        node.poll(now, NodePollBudget::default()).unwrap();
        let session = node.local().owner.identity().store.session;
        drain_wire_nodes(vec![node], clock);
        let tcp = std::net::TcpListener::bind(address).unwrap();
        let udp = std::net::UdpSocket::bind(address).unwrap();
        drop((tcp, udp));
        let mut selected = config(directory.clone(), NativeOpenMode::Recover);
        selected.listen = address;
        let recovered = selected
            .open_with_protocol_and_timers(
                protocol,
                NativeTimingProfile::Throughput.timers(),
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .unwrap();
        assert!(recovered.local().owner.identity().store.session > session);
        drain_wire_nodes(vec![recovered], Instant::now());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
