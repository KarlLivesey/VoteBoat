// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual native providers retain the selected application and original stores.
use super::*;
pub(super) fn prepare(
    root: &std::path::Path,
    clock: Instant,
    id: u64,
    protocol: NativePeerProtocol,
    mode: NativeOpenMode,
) -> NativeLocalParts<LogicalApplication> {
    let directory = root.join(id.to_string());
    if mode == NativeOpenMode::Create {
        std::fs::create_dir_all(&directory).unwrap();
    }
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = held.local_addr().unwrap();
    drop(held);
    let startup = NativeStartup {
        directory,
        mode,
        node: support::node(id),
        store: support::identity(u128::from(id)),
        bootstrap: setup::child(),
        listen,
        peers: (1..=3)
            .filter(|&n| n != id)
            .map(|n| {
                (
                    support::node(n),
                    NativeStartupPeer {
                        address: format!("127.0.0.1:{}", 41000 + n).parse().unwrap(),
                        certificate: channels::certificate(n).to_vec(),
                        server_name: format!("node{n}.voteboat.test"),
                    },
                )
            })
            .collect(),
        tls: channels::config(id).with_wire_version(8).unwrap(),
        entropy_seed: id * 17,
        limits: NodeLimits::default(),
    };
    let parts = startup
        .prepare_for_discovery(
            protocol,
            NativeTimingProfile::Throughput.timers(),
            setup::app(),
            Arc::new(ThreadWake::current()),
            MonoTime(clock.elapsed().as_millis() as u64),
        )
        .unwrap_or_else(|e| panic!("original relay child startup {:?}", e.reason));
    let mut peers = parts.peers.unwrap();
    peers.connector.close();
    assert!(peers
        .connector
        .poll(
            MonoTime(clock.elapsed().as_millis() as u64),
            Default::default()
        )
        .unwrap()
        .is_empty());
    let mut dialer = peers
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("unused original direct connector drained"));
    let end = Instant::now() + Duration::from_secs(10);
    while dialer.as_mut().is_some_and(|d| !d.try_finish().unwrap()) {
        assert!(Instant::now() < end);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    parts.local
}
pub(super) fn join(mut boat: Boat) {
    assert_eq!(
        boat.cancel_activation_readiness(setup::child().group),
        Err(LogicalBackingPollError::Node(Box::new(NodeError::Closed)))
    );
    let mut parts = boat
        .into_parts()
        .unwrap_or_else(|_| panic!("actual selected relay child drained"));
    assert!(parts.peers.take().unwrap().hop.is_drained());
    let mut snapshots = parts.local.snapshots.take().unwrap();
    let end = Instant::now() + Duration::from_secs(10);
    let mut wal = false;
    let mut snap = false;
    while !wal || !snap {
        if !wal {
            wal = parts.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snap {
            snap = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        assert!(
            Instant::now() < end,
            "same original native WAL/snapshot workers join"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
