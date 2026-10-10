// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
use std::{cell::Cell, net::SocketAddr, rc::Rc};
use voteboat::{
    connect::{DiscoveryConnector, PeerConnector},
    discovery::*,
    native::connect::{NativePeerProtocol, NativeServiceConnector},
    secure::PeerIdentity,
};
type Boat = NativeNode<HostApplication, DiscoveryConnector<NativeServiceConnector, Source>>;
type Hints = Rc<BTreeMap<NodeId, (PeerIdentity, SocketAddr)>>;
struct Source {
    hints: Hints,
    calls: Rc<Cell<usize>>,
    closed: Rc<Cell<bool>>,
}
impl PeerDiscovery for Source {
    fn resolve(
        &mut self,
        peer: PeerIdentity,
        now: MonoTime,
    ) -> Result<PeerEndpointHint, DiscoveryError> {
        if self.closed.get() {
            return Err(DiscoveryError::Closed);
        }
        self.calls.set(self.calls.get() + 1);
        let (expected, endpoint) = self.hints.get(&peer.node).ok_or(DiscoveryError::Missing)?;
        if *expected != peer {
            return Err(DiscoveryError::WrongBinding);
        }
        Ok(PeerEndpointHint {
            peer,
            generation: HintGeneration::new(7).unwrap(),
            endpoint: *endpoint,
            expires_at: MonoTime(now.0 + 30_000),
        })
    }
    fn invalidate(&mut self, _: PeerIdentity, _: HintGeneration) -> bool {
        false
    }
    fn close(&mut self) {
        self.closed.set(true);
    }
}
struct Cluster {
    directory: PathBuf,
    addresses: Vec<SocketAddr>,
    bootstrap: Bootstrap,
    protocol: NativePeerProtocol,
    hints: Hints,
    nodes: Vec<Boat>,
    closed: Vec<Rc<Cell<bool>>>,
    calls: Vec<Rc<Cell<usize>>>,
    clock: Instant,
}
impl Cluster {
    fn new(protocol: NativePeerProtocol) -> Self {
        let reservations = (0..3)
            .map(|_| {
                (0..32)
                    .find_map(|_| {
                        let tcp = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
                        let udp = std::net::UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                        Some((tcp, udp))
                    })
                    .expect("TCP/UDP reservation")
            })
            .collect::<Vec<_>>();
        let addresses = reservations
            .iter()
            .map(|(tcp, _)| tcp.local_addr().unwrap())
            .collect::<Vec<_>>();
        let stores = (1..=3)
            .map(|n| {
                (
                    NodeId::new(n).unwrap(),
                    StoreIdentity {
                        id: StoreId::new(n as u128).unwrap(),
                        incarnation: StoreIncarnation::new(1).unwrap(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let bootstrap = Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(9).unwrap(),
            policy: Policy::new(
                Tree::Majority(stores.keys().copied().map(Tree::Voter).collect()),
                Limits::default(),
            )
            .unwrap(),
            voter_stores: stores,
        };
        let hints = Rc::new(
            bootstrap
                .voter_stores
                .iter()
                .enumerate()
                .map(|(i, (&node, &store))| (node, (PeerIdentity { node, store }, addresses[i])))
                .collect(),
        );
        drop(reservations);
        let directory = root();
        std::fs::create_dir(&directory).unwrap();
        Self {
            directory,
            addresses,
            bootstrap,
            protocol,
            hints,
            nodes: Vec::new(),
            closed: Vec::new(),
            calls: Vec::new(),
            clock: Instant::now(),
        }
    }
    fn open(&mut self, mode: NativeOpenMode) -> Vec<StoreSession> {
        self.clock = Instant::now();
        let mut sessions = Vec::new();
        for id in 1..=3 {
            let mut config = selected_wire_config(
                &self.directory,
                &self.addresses,
                &self.bootstrap,
                id,
                mode,
                1,
            );
            // Accept addresses remain real; only outgoing defaults are stale.
            for (&peer, route) in &mut config.peers {
                if config.node < peer {
                    route.address = format!("127.0.0.1:{}", peer.get()).parse().unwrap();
                }
            }
            let limits = config.limits;
            let parts = config
                .prepare_for_discovery(
                    self.protocol,
                    TimerConfig {
                        election_min_ms: 10_000,
                        election_spread_ms: 1_000,
                        ..Default::default()
                    },
                    app(),
                    Arc::new(ThreadWake::current()),
                    MonoTime(0),
                )
                .unwrap();
            sessions.push(parts.local.owner.identity().store.session);
            let closed = Rc::new(Cell::new(false));
            let calls = Rc::new(Cell::new(0));
            self.nodes.push(assemble(
                parts,
                limits,
                Source {
                    hints: self.hints.clone(),
                    calls: calls.clone(),
                    closed: closed.clone(),
                },
            ));
            self.closed.push(closed);
            self.calls.push(calls);
        }
        self.nodes[0]
            .control(group(), NodeControl::Campaign)
            .unwrap();
        self.until(|c| {
            c.nodes[0].local().owner.core(group()).unwrap().role() == voteboat::raft::Role::Leader
        });
        sessions
    }
    fn until(&mut self, mut done: impl FnMut(&mut Self) -> bool) {
        let start = Instant::now();
        loop {
            let now = MonoTime(self.clock.elapsed().as_millis() as u64);
            for node in &mut self.nodes {
                node.poll(now, NodePollBudget::default()).unwrap();
            }
            if done(self) {
                return;
            }
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "discovered startup stalled"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn write(&mut self, operation: u128, delta: i64, expected: i64) -> CounterReceipt {
        let ticket = self.nodes[0]
            .propose(ClientRequest {
                group: group(),
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            })
            .unwrap();
        let mut result = None;
        self.until(|c| {
            if let Some(reply) = c.nodes[0].poll_client() {
                assert_eq!(reply.ticket(), ticket);
                let ClientOutcome::Applied { receipt, .. } =
                    c.nodes[0].complete_client(reply).unwrap()
                else {
                    panic!("not applied")
                };
                result = Some(receipt);
            }
            result.is_some()
                && c.nodes.iter().all(|n| {
                    let a = &n.local().applications[&group()];
                    a.0.read_applied(a.applied_index()) == Ok(expected)
                })
        });
        result.unwrap()
    }
    fn close(&mut self) {
        for n in &mut self.nodes {
            n.begin_shutdown();
        }
        self.until(|c| c.nodes.iter().all(|n| n.is_drained()));
        for n in self.nodes.drain(..) {
            finish(n.into_parts().unwrap_or_else(|_| panic!("not drained")));
        }
        assert!(self.closed.iter().all(|closed| closed.get()));
        let peer = self.hints.values().next().unwrap().0;
        let mut independent = Source {
            hints: self.hints.clone(),
            calls: Rc::new(Cell::new(0)),
            closed: Rc::new(Cell::new(false)),
        };
        assert!(independent.resolve(peer, MonoTime(0)).is_ok());
        self.closed.clear();
        self.calls.clear();
    }
}
fn assemble(
    parts: NativeNodeParts<HostApplication, NativeServiceConnector>,
    limits: NodeLimits,
    source: Source,
) -> Boat {
    NativeNode::from_parts(discovered_parts(parts, source), limits, MonoTime(0))
        .unwrap_or_else(|e| panic!("{:?}", e.reason))
}
fn discovered_parts(
    parts: NativeNodeParts<HostApplication, NativeServiceConnector>,
    source: Source,
) -> NativeNodeParts<HostApplication, DiscoveryConnector<NativeServiceConnector, Source>> {
    let NativeNodeParts { local, peers } = parts;
    let PeerParts {
        connector,
        roster,
        factory,
        ingress,
        routes,
        admission_routes,
    } = peers.unwrap();
    let connector = DiscoveryConnector::new(connector, source, MonoTime(0))
        .ok()
        .unwrap();
    NativeNodeParts {
        local,
        peers: Some(PeerParts {
            connector,
            roster,
            factory,
            ingress,
            routes,
            admission_routes,
        }),
    }
}
fn finish(
    mut parts: NativeNodeParts<HostApplication, DiscoveryConnector<NativeServiceConnector, Source>>,
) {
    let (connector, source) = parts
        .peers
        .take()
        .unwrap()
        .connector
        .into_parts()
        .ok()
        .unwrap();
    assert!(source.closed.get());
    let mut dialer = connector.into_dialer().ok().unwrap();
    let mut snapshots = parts.local.snapshots.take().unwrap();
    let mut log_done = false;
    let mut snapshot_done = false;
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let dial_done = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
        if !log_done {
            log_done = parts.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snapshot_done {
            snapshot_done = snapshots.worker.try_reclaim().unwrap().is_some();
        }
        if log_done && snapshot_done && dial_done {
            return;
        }
        assert!(Instant::now() < deadline, "prepared workers did not join");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn history(protocol: NativePeerProtocol) {
    let mut cluster = Cluster::new(protocol);
    let before = cluster.open(NativeOpenMode::Create);
    assert_eq!(cluster.write(1, 7, 7).outcome, CounterOutcome::Value(7));
    assert_eq!(cluster.write(2, 3, 10).outcome, CounterOutcome::Value(10));
    assert!(cluster.calls[0].get() >= 2);
    cluster.close();
    let after = cluster.open(NativeOpenMode::Recover);
    assert!(before.iter().zip(after).all(|(a, b)| *a != b));
    let receipt = cluster.write(1, 7, 10);
    assert!(receipt.duplicate);
    assert_eq!(receipt.outcome, CounterOutcome::Value(7));
    assert!(cluster.write(2, 3, 10).duplicate);
    assert_eq!(cluster.write(3, 5, 15).outcome, CounterOutcome::Value(15));
    cluster.close();
    std::fs::remove_dir_all(cluster.directory).unwrap();
}
#[test]
fn tcp_prepared_startup_accepts_host_discovery_and_recovers_original_receipts() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_prepared_startup_accepts_host_discovery_and_recovers_original_receipts() {
    history(NativePeerProtocol::Quic);
}

#[test]
fn prepared_startup_invalid_timing_and_missing_recovery_return_original_application() {
    for mode in [NativeOpenMode::Create, NativeOpenMode::Recover] {
        let directory = root();
        let mut timers = TimerConfig::default();
        if mode == NativeOpenMode::Create {
            timers.heartbeat_ms = 0;
        }
        let mut rejected = config(directory.clone(), mode)
            .prepare_for_discovery(
                NativePeerProtocol::TcpTls,
                timers,
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .err()
            .unwrap();
        assert_eq!(rejected.application.as_ref().unwrap().applied_index(), 0);
        assert!(rejected.try_cleanup().unwrap());
        assert!(!directory.exists());
    }
}

#[test]
fn rejected_prepared_composition_keeps_host_source_open_and_returns_native_parts() {
    let directory = root();
    let parts = config(directory.clone(), NativeOpenMode::Create)
        .prepare_for_discovery(
            NativePeerProtocol::TcpTls,
            TimerConfig::default(),
            app(),
            Arc::new(ThreadWake::current()),
            MonoTime(0),
        )
        .unwrap();
    let owner = parts.local.owner.identity();
    let closed = Rc::new(Cell::new(false));
    let parts = discovered_parts(
        parts,
        Source {
            hints: Rc::new(BTreeMap::new()),
            calls: Rc::new(Cell::new(0)),
            closed: closed.clone(),
        },
    );
    let mut limits = NodeLimits::default();
    limits.configuration.requests = 0;
    let rejected = NativeNode::from_parts(parts, limits, MonoTime(0))
        .err()
        .unwrap();
    assert!(!closed.get());
    assert_eq!(rejected.parts.local.owner.identity(), owner);
    assert_eq!(
        rejected.parts.local.applications[&group()].applied_index(),
        0
    );
    let mut node = NativeNode::from_parts(*rejected.parts, NodeLimits::default(), MonoTime(0))
        .unwrap_or_else(|e| panic!("{:?}", e.reason));
    node.begin_shutdown();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !node.is_drained() {
        node.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    finish(node.into_parts().unwrap_or_else(|_| panic!("not drained")));
    assert!(closed.get());
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn abandoned_prepared_startup_closes_workers_and_releases_listener() {
    use voteboat::{snapshot_worker::SnapshotWorker, worker::PersistenceWorker};
    for protocol in [
        NativePeerProtocol::TcpTls,
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic,
    ] {
        let directory = root();
        let mut config = config(directory.clone(), NativeOpenMode::Create);
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);
        config.listen = address;
        let mut parts = config
            .prepare_for_discovery(
                protocol,
                TimerConfig::default(),
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .unwrap();
        parts.local.owner.close_admission().unwrap();
        parts.local.persistence.close();
        let mut snapshots = parts.local.snapshots.take().unwrap();
        snapshots.worker.close();
        let mut peers = parts.peers.take().unwrap();
        peers.connector.close();
        let mut dialer = peers.connector.into_dialer().ok().unwrap();
        let mut log_done = false;
        let mut snapshots_done = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let dial_done = dialer.as_mut().is_none_or(|d| d.try_finish().unwrap());
            if !log_done {
                log_done = parts.local.persistence.try_reclaim().unwrap().is_some();
            }
            if !snapshots_done {
                snapshots_done = snapshots.worker.try_reclaim().unwrap().is_some();
            }
            if dial_done && log_done && snapshots_done {
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        drop(parts);
        drop(snapshots);
        let tcp = std::net::TcpListener::bind(address).unwrap();
        let udp = std::net::UdpSocket::bind(address).unwrap();
        drop((tcp, udp));
        std::fs::remove_dir_all(directory).unwrap();
    }
}
