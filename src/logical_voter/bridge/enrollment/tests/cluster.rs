// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real three-physical-replica target, its original workers and selected TLS sockets.
use super::*;
use crate::native::{
    connect::{NativePeerProtocol, NativeServiceConnector},
    node::NativeNode,
    startup::*,
    tls::{NativeTlsConfig, TlsCredentials},
    worker::ThreadWake,
};
use std::{
    net::{SocketAddr, TcpListener, UdpSocket},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
pub(super) type Child = NativeNode<LogicalApplication, NativeServiceConnector>;
static NEXT: AtomicU64 = AtomicU64::new(1);
pub(super) struct Cluster {
    pub nodes: Vec<Child>,
    pub clock: Instant,
    root: PathBuf,
    addresses: [SocketAddr; 3],
}
impl Cluster {
    pub fn new(protocol: NativePeerProtocol, app: LogicalApplication) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-enrollment347-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let held: Vec<_> = (0..3)
            .map(|_| {
                let tcp = TcpListener::bind("127.0.0.1:0").unwrap();
                let udp = UdpSocket::bind(tcp.local_addr().unwrap()).unwrap();
                (tcp, udp)
            })
            .collect();
        let addresses: [SocketAddr; 3] = held
            .iter()
            .map(|(t, _)| t.local_addr().unwrap())
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        drop(held);
        let mut host = Self {
            nodes: Vec::new(),
            clock: Instant::now(),
            root,
            addresses,
        };
        host.nodes = (1..=3)
            .map(|id| {
                host.config(id)
                    .open_with_protocol_and_timers(
                        protocol,
                        NativeTimingProfile::Throughput.timers(),
                        app.clone(),
                        Arc::new(ThreadWake::current()),
                        host.now(),
                    )
                    .unwrap_or_else(|e| panic!("actual target startup: {:?}", e.reason))
            })
            .collect();
        host.nodes[0]
            .control(fixture::mapping().child, NodeControl::Campaign)
            .unwrap();
        host.wait(|h| {
            h.nodes.iter().any(|n| {
                n.local()
                    .owner
                    .core(fixture::mapping().child)
                    .unwrap()
                    .role()
                    == Role::Leader
            })
        });
        host
    }
    fn config(&self, id: u64) -> NativeStartup {
        NativeStartup {
            directory: self.root.join(id.to_string()),
            mode: NativeOpenMode::Create,
            node: fixture::node(id),
            store: fixture::store(id.into()),
            bootstrap: fixture::child(),
            listen: self.addresses[id as usize - 1],
            peers: (1..=3)
                .filter(|p| *p != id)
                .map(|p| {
                    (
                        fixture::node(p),
                        NativeStartupPeer {
                            address: self.addresses[p as usize - 1],
                            certificate: certificate(p).to_vec(),
                            server_name: format!("node{p}.voteboat.test"),
                        },
                    )
                })
                .collect(),
            tls: tls(id).with_wire_version(8).unwrap(),
            entropy_seed: id * 17,
            limits: NodeLimits::default(),
            ready_visit_items: 16,
            append_window: None,
        }
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.clock.elapsed().as_millis() as u64)
    }
    pub fn poll(&mut self) {
        let now = self.now();
        for node in &mut self.nodes {
            node.poll(now, NodePollBudget::default()).unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    pub fn wait(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let end = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            self.poll();
            assert!(Instant::now() < end, "actual target history timeout");
        }
    }
    pub fn finish(mut self) {
        for node in &mut self.nodes {
            node.begin_shutdown();
        }
        self.wait(|h| h.nodes.iter().all(Child::is_drained));
        for node in self.nodes.drain(..) {
            join(node);
        }
        for address in self.addresses {
            drop((
                TcpListener::bind(address).unwrap(),
                UdpSocket::bind(address).unwrap(),
            ));
        }
        std::fs::remove_dir_all(self.root).unwrap();
    }
}
fn certificate(id: u64) -> &'static [u8] {
    match id {
        1 => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tls/node1.der"
        )),
        2 => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tls/node2.der"
        )),
        3 => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tls/node3.der"
        )),
        _ => unreachable!(),
    }
}
fn tls(id: u64) -> NativeTlsConfig {
    let key: &[u8] = match id {
        1 => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tls/node1-key.der"
        )),
        2 => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tls/node2-key.der"
        )),
        3 => include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tls/node3-key.der"
        )),
        _ => unreachable!(),
    };
    NativeTlsConfig::new(TlsCredentials {
        roots: vec![include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/tls/ca.der"
        ))
        .to_vec()],
        certificate_chain: vec![certificate(id).to_vec()],
        private_key: key.to_vec(),
    })
    .unwrap()
}
fn join(node: Child) {
    let mut parts = node
        .into_parts()
        .unwrap_or_else(|_| panic!("actual target owners must drain"));
    let mut dialer = parts
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("original target dialer must drain"));
    let mut snapshots = parts.local.snapshots.take().unwrap();
    let (mut wal, mut snapshot) = (false, false);
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        let dial = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if !wal {
            wal = parts.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snapshot {
            snapshot = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        if dial && wal && snapshot {
            return;
        }
        assert!(Instant::now() < end, "actual target worker joins");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
