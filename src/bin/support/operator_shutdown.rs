// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{
    collections::BTreeMap,
    net::UdpSocket,
    sync::atomic::{AtomicU64, Ordering},
};
use voteboat::{raft::*, secure::PeerIdentity};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Harness {
    root: std::path::PathBuf,
    addresses: BTreeMap<u64, std::net::SocketAddr>,
    nodes: Vec<Service>,
    clock: Instant,
}
impl Harness {
    fn new(protocol: NativePeerProtocol) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-operator-shutdown-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let sockets = (0..3)
            .map(|_| {
                (0..32)
                    .find_map(|_| {
                        let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
                        let udp = UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                        Some((tcp, udp))
                    })
                    .expect("reserved TCP/UDP endpoint")
            })
            .collect::<Vec<_>>();
        let addresses = (1..=3)
            .zip(sockets.iter().map(|(t, _)| t.local_addr().unwrap()))
            .collect::<BTreeMap<_, _>>();
        drop(sockets);
        let tls = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
        let nodes = (1..=3)
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
                config.listen = addresses[&id];
                for (peer, target) in &mut config.peers {
                    target.address = addresses[&peer.get()];
                }
                config.tls = config.tls.with_wire_version(8).unwrap();
                let app =
                    counter_application::Application::new(setup::application().unwrap(), true)
                        .unwrap();
                // Explicit fixture timers isolate the held owner step from elections;
                // executable production/default timers remain unchanged.
                config
                    .open_with_protocol_and_timers(
                        protocol,
                        TimerConfig {
                            heartbeat_ms: 50,
                            election_min_ms: 1000,
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
            nodes,
            clock: Instant::now(),
        }
    }
    fn drive(&mut self, mut done: impl FnMut(&[Service]) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for node in &mut self.nodes {
                let now = MonoTime(self.clock.elapsed().as_millis() as u64);
                let progress = node.poll(now, NodePollBudget::default()).unwrap();
                if let Some(replica) = progress.replica {
                    for step in replica.steps {
                        assert!(step.error.is_none(), "{:?}", step.error);
                    }
                }
            }
            if done(&self.nodes) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "native operator fixture deadline"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn close(mut self) {
        for node in &mut self.nodes {
            node.begin_shutdown();
        }
        self.drive(|nodes| nodes.iter().all(Service::is_drained));
        for node in self.nodes {
            setup::join(node).unwrap();
        }
        for address in self.addresses.values() {
            let tcp = TcpListener::bind(address).unwrap();
            let udp = UdpSocket::bind(address).unwrap();
            drop((tcp, udp));
        }
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

fn history(protocol: NativePeerProtocol) {
    let mut h = Harness::new(protocol);
    h.nodes[0]
        .control(setup::group(), NodeControl::Campaign)
        .unwrap();
    h.drive(|nodes| {
        nodes[0]
            .local()
            .owner
            .core(setup::group())
            .is_some_and(|c| c.role() == Role::Leader && c.state().commit_index > 0)
    });
    let target = PeerIdentity {
        node: NodeId::new(2).unwrap(),
        store: h.nodes[1].local().owner.identity().store.identity,
    };
    h.nodes[0]
        .control(
            setup::group(),
            NodeControl::TransferLeadership(LeadershipTransferRequest {
                operation: OperationId::new(93001).unwrap(),
                target,
                configuration: ConfigurationId::new(1).unwrap(),
            }),
        )
        .unwrap();
    // Do not poll the target; the original source must retain this transfer.
    let deadline = Instant::now() + Duration::from_secs(10);
    while h.nodes[0]
        .local()
        .owner
        .core(setup::group())
        .unwrap()
        .leadership_transfer()
        .is_none()
    {
        h.nodes[0]
            .poll(
                MonoTime(h.clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        assert!(Instant::now() < deadline);
    }
    let mut leaders = leadership_set::Leaders::new(&h.nodes[0]);
    let mut connection = None;
    let mut drain = None;
    let source = &h.nodes[0].local().owner;
    let term = source.core(setup::group()).unwrap().state().hard_state.term;
    advance_leadership(
        &mut h.nodes[0],
        &mut connection,
        &mut leaders,
        &mut drain,
        true,
    )
    .unwrap();
    // Hold the source owner step after cancellation admission, exactly while
    // shutdown closes admission and the old volatile transfer remains visible.
    h.nodes[0].begin_shutdown();
    assert_eq!(h.nodes[0].state(), NodeState::Quiescing);
    assert_eq!(
        h.nodes[0].control(setup::group(), NodeControl::Campaign),
        Err(NodeError::Closed)
    );
    assert!(h.nodes[0]
        .local()
        .owner
        .core(setup::group())
        .unwrap()
        .leadership_transfer()
        .is_some());
    let result = advance_leadership(
        &mut h.nodes[0],
        &mut connection,
        &mut leaders,
        &mut drain,
        true,
    )
    .map_err(|e| e.to_string());
    h.drive(|nodes| {
        nodes[0]
            .local()
            .owner
            .core(setup::group())
            .unwrap()
            .leadership_transfer()
            .is_none()
    });
    let source = &h.nodes[0].local().owner;
    assert_eq!(
        source.core(setup::group()).unwrap().state().hard_state.term,
        term,
        "queued cancellation must complete without relying on a new term"
    );
    h.close(); // Join/release actual native resources even for the old failure.
    assert_eq!(result, Ok(()));
}
#[test]
fn queued_operator_cancellation_drains_after_admission_closes_tcp() {
    history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn queued_operator_cancellation_drains_after_admission_closes_quic() {
    history(NativePeerProtocol::Quic);
}
