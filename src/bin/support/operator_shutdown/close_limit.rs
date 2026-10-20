// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    connect::ConnectUsage,
    outbound::OutboundQueue,
    quorum::{Limits, Policy, Tree},
    secure::{SessionBinding, SessionState},
    snapshot_worker::SnapshotWorker,
    transport::{PeerDrainStatus, TransportState, TransportUsage},
    worker::PersistenceWorker,
};

struct DelayedPeers {
    root: std::path::PathBuf,
    addresses: [SocketAddr; 2],
    relay: UdpSocket,
    nodes: Vec<Service>,
}
impl DelayedPeers {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-close-limit-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let sockets = [
            UdpSocket::bind("127.0.0.1:0").unwrap(),
            UdpSocket::bind("127.0.0.1:0").unwrap(),
        ];
        let addresses = sockets.each_ref().map(|s| s.local_addr().unwrap());
        let relay = UdpSocket::bind("127.0.0.1:0").unwrap();
        relay.set_nonblocking(true).unwrap();
        drop(sockets);
        let tls = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let nodes = (1..=2)
            .map(|id| {
                let mut config = setup::configuration(
                    &root.join(id.to_string()),
                    id,
                    1,
                    &tls,
                    true,
                    setup::PeerInput::Legacy(None),
                )
                .unwrap()
                .startup;
                config.listen = addresses[id as usize - 1];
                config.bootstrap.voter_stores.retain(|n, _| n.get() <= 2);
                config.bootstrap.policy = Policy::new(
                    Tree::Majority(
                        config
                            .bootstrap
                            .voter_stores
                            .keys()
                            .copied()
                            .map(Tree::Voter)
                            .collect(),
                    ),
                    Limits::default(),
                )
                .unwrap();
                config.peers.retain(|n, _| n.get() <= 2);
                for peer in config.peers.values_mut() {
                    peer.address = relay.local_addr().unwrap();
                }
                config.tls = config.tls.with_wire_version(8).unwrap();
                let app =
                    counter_application::Application::new(setup::application().unwrap(), true)
                        .unwrap();
                // Isolate actual transport ownership from election traffic;
                // all provider handshake, close and poll budgets stay original.
                config
                    .open_with_protocol_and_timers(
                        NativePeerProtocol::Quic,
                        TimerConfig {
                            heartbeat_ms: 50,
                            election_min_ms: 100_000,
                            election_spread_ms: 1000,
                            expirations_per_poll: 32,
                        },
                        app,
                        std::sync::Arc::new(voteboat::native::worker::ThreadWake::current()),
                        MonoTime(0),
                    )
                    .unwrap()
            })
            .collect();
        Self {
            root,
            addresses,
            relay,
            nodes,
        }
    }
    fn forward(&self) {
        let mut bytes = [0; 65536];
        for _ in 0..256 {
            let (size, from) = match self.relay.recv_from(&mut bytes) {
                Ok(packet) => packet,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return,
                Err(e) => panic!("actual UDP relay: {e}"),
            };
            let target = if from == self.addresses[0] {
                self.addresses[1]
            } else {
                assert_eq!(from, self.addresses[1]);
                self.addresses[0]
            };
            assert_eq!(self.relay.send_to(&bytes[..size], target).unwrap(), size);
        }
        panic!("bounded UDP relay visit exhausted");
    }
    fn poll(&mut self, now: MonoTime) {
        self.forward();
        for node in &mut self.nodes {
            let progress = node.poll(now, NodePollBudget::default()).unwrap();
            if let Some(replica) = progress.replica {
                assert!(
                    replica.steps.iter().all(|s| s.error.is_none()),
                    "{:?}",
                    replica.steps
                );
            }
        }
        self.forward();
        std::thread::park_timeout(Duration::from_micros(50));
    }
    fn delayed_handshake(&mut self) -> u64 {
        let end = Instant::now() + Duration::from_secs(2);
        let mut bytes = [0; 65536];
        let size = loop {
            self.nodes[0]
                .poll(MonoTime(0), NodePollBudget::default())
                .unwrap();
            match self.relay.recv_from(&mut bytes) {
                Ok((size, from)) => {
                    assert_eq!(from, self.addresses[0]);
                    assert!(size > 0);
                    break size;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < end,
                        "actual client initial packet must arrive"
                    );
                    std::thread::park_timeout(Duration::from_millis(1));
                }
                Err(e) => panic!("actual first packet: {e}"),
            }
        };
        // The original encrypted packet is physically held here until the
        // actual accept-side node first processes it at supplied time 3500.
        self.relay
            .send_to(&bytes[..size], self.addresses[1])
            .unwrap();
        self.nodes[1]
            .poll(MonoTime(3500), NodePollBudget::default())
            .unwrap();
        for now in 3501..5000 {
            self.poll(MonoTime(now));
            if self.nodes.iter_mut().enumerate().all(|(index, node)| {
                node.peer_drain_status(NodeId::new(2 - index as u64).unwrap())
                    .and_then(|p| p.transport)
                    .and_then(|t| t.session)
                    .is_some_and(|s| s.state == SessionState::Ready && s.flushed)
            }) {
                return now;
            }
        }
        panic!("actual two-node delayed handshake must authenticate");
    }
    fn finish(
        mut self,
        original: &BTreeMap<(usize, NodeId), Close>,
        controller: &mut shutdown::Shutdown,
        target: usize,
    ) {
        for deadline in original
            .values()
            .map(|p| p.deadline)
            .collect::<std::collections::BTreeSet<_>>()
        {
            let before = MonoTime(deadline.0 - 1);
            self.poll(before);
            let pending = retained(&mut self.nodes, before);
            assert!(pending
                .iter()
                .all(|(key, close)| original.get(key) == Some(close)));
            controller
                .advance(&mut self.nodes[target], true, before)
                .unwrap();
            assert_pin(controller, original, target);
            for _ in 0..1000 {
                self.poll(deadline);
                if self.nodes.iter().all(Service::is_drained) {
                    break;
                }
            }
        }
        assert!(
            self.nodes.iter().all(Service::is_drained),
            "actual provider close must drain"
        );
        let end = original.values().map(|close| close.deadline).max().unwrap();
        controller
            .advance(&mut self.nodes[target], true, end)
            .unwrap();
        for node in self.nodes {
            setup::join(node).unwrap();
        }
        for address in self.addresses {
            drop(UdpSocket::bind(address).unwrap());
        }
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

fn assert_pin(
    controller: &shutdown::Shutdown,
    original: &BTreeMap<(usize, NodeId), Close>,
    target: usize,
) {
    let pin = controller.protocol.as_ref().unwrap();
    let expected = original.iter().filter(|((index, _), _)| *index == target);
    assert_eq!(pin.original.len(), expected.clone().count());
    let end = expected
        .clone()
        .map(|(_, close)| close.deadline)
        .max()
        .unwrap();
    for ((_, peer), close) in expected {
        let actual = pin.original.get(peer).unwrap();
        assert_eq!(actual.binding, close.binding);
        assert_eq!(actual.deadline, close.deadline);
    }
    assert_eq!(pin.end, end);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Close {
    binding: SessionBinding,
    deadline: MonoTime,
}
fn close(status: PeerDrainStatus) -> Option<Close> {
    if !status.closed
        || !status.authorized
        || status.fenced
        || status.retiring
        || status.attempt.is_some()
        || status.accepted.is_some()
    {
        return None;
    }
    let transport = status.transport?;
    let session = transport.session?;
    if status.binding != Some(transport.binding)
        || status.peer.node != transport.binding.peer.node
        || status.peer.store != transport.binding.peer.store.identity
        || transport.state != TransportState::Draining
        || transport.usage != TransportUsage::default()
        || transport.received.is_some()
        || transport.close_started != Some(true)
        || session.state != SessionState::Closing
        || !session.flushed
        || session.close_started != Some(true)
        || session.protocol_drained != Some(false)
    {
        return None;
    }
    Some(Close {
        binding: transport.binding,
        deadline: session.next_deadline?,
    })
}
fn settled(node: &Service) {
    let local = node.local();
    assert_eq!(node.state(), NodeState::Draining);
    assert!(local.clients.is_drained());
    assert!(local.reads.is_drained());
    assert!(local.results.is_drained());
    assert!(local.owner.is_drained());
    assert!(local.persistence.is_drained());
    assert_eq!(local.persistence.usage(), Default::default());
    assert!(local.outbound.is_drained());
    assert_eq!(node.replica_usage(), ReplicaDriverUsage::default());
    assert!(node.checkpoints().pending.is_empty());
    let snapshots = local.snapshots.as_ref().unwrap();
    assert!(snapshots.router.is_drained());
    assert!(snapshots.worker.is_drained());
    assert_eq!(snapshots.router.usage(), Default::default());
    assert_eq!(snapshots.worker.usage(), Default::default());
    let peers = node.peers().unwrap();
    assert_eq!(peers.usage(), PeerDriverUsage::default());
    assert_eq!(peers.connector_usage(), ConnectUsage::default());
    assert!(peers.connector_is_drained());
    assert!(peers.ingress().is_drained());
    assert_eq!(peers.roster().usage().connecting, 0);
}
fn retained(nodes: &mut [Service], now: MonoTime) -> BTreeMap<(usize, NodeId), Close> {
    let mut remaining = BTreeMap::new();
    for (index, node) in nodes.iter_mut().enumerate() {
        if node.is_drained() {
            continue;
        }
        settled(node);
        let peers = node
            .peers()
            .unwrap()
            .roster()
            .tracked_peers()
            .collect::<Vec<_>>();
        for peer in peers {
            let status = node.peer_drain_status(peer).unwrap();
            eprintln!(
                "actual Service node {} cutoff {now:?}: {status:?}",
                index + 1
            );
            let original =
                close(status).expect("only original authenticated protocol close may remain");
            assert!(original.deadline > now);
            remaining.insert((index, peer), original);
        }
        assert_eq!(
            node.peers().unwrap().roster().usage().connections,
            remaining.keys().filter(|(n, _)| *n == index).count()
        );
    }
    assert!(!remaining.is_empty());
    remaining
}
#[test]
fn executable_retains_actual_settled_future_quic_close() {
    let mut h = DelayedPeers::new();
    let start = h.delayed_handshake();
    for node in &mut h.nodes {
        node.begin_shutdown();
    }
    for _ in 0..1000 {
        h.poll(MonoTime(start));
        if h.nodes.iter().all(|n| n.state() != NodeState::Quiescing) {
            break;
        }
    }
    for _ in 0..100 {
        h.poll(MonoTime(start));
    }
    let at_close = retained(&mut h.nodes, MonoTime(start));
    let cutoff = MonoTime(start + 10001);
    for _ in 0..100 {
        h.poll(cutoff);
    }
    let original = retained(&mut h.nodes, cutoff);
    assert!(original
        .iter()
        .all(|(key, close)| at_close.get(key) == Some(close)));
    let target = original.keys().next().unwrap().0;
    // Supplied provider time and the executable's explicit wall-start input
    // both represent the same intended ten-second-and-one-ms close interval.
    let mut controller = shutdown::Shutdown {
        started: Some(
            Instant::now()
                .checked_sub(Duration::from_millis(10001))
                .unwrap(),
        ),
        ..shutdown::Shutdown::default()
    };
    controller
        .advance(&mut h.nodes[target], true, cutoff)
        .unwrap();
    assert!(!h.nodes[target].is_drained());
    assert_eq!(retained(&mut h.nodes, cutoff), original);
    assert_pin(&controller, &original, target);
    h.finish(&original, &mut controller, target);
}
