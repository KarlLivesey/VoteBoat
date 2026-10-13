// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Three original native WAL/application owners on the selected relay network.
use super::{channels::*, observed_wire::*, support::*};
use std::{
    cell::{Cell, RefCell},
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    rc::Rc,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    application::*,
    connect::*,
    identity::*,
    log::*,
    native::{connect::NativePeerProtocol, node::*, relay::*, startup::*, worker::ThreadWake},
    runtime::*,
    secure::*,
    transport::*,
    wire::{WireCodec, WireScope},
};
pub type Network = NativeRelayNetwork<Box<dyn SecureSession>, ObservedWire>;
pub type Boat = NativeNetworkNode<Counter, Network>;
#[path = "cluster/setup.rs"]
mod setup;
pub struct Cluster {
    pub root: PathBuf,
    pub clock: Instant,
    pub protocol: NativePeerProtocol,
    pub boats: Vec<Boat>,
    pub helper: RelayForwarder<Gated, ObservedWire>,
    pub branches: Vec<RelayForwarder<Gated, ObservedWire>>,
    pub controls: Vec<(RelayHopTicket, RelayFrame)>,
    pub tree: RelayTreePlan,
    pub gates: Vec<Gate>,
    pub helper_identity: LocalIdentity,
    pub discarded: usize,
    pub observations: Rc<RefCell<Vec<Observation>>>,
    pub scope_fault: Rc<Cell<bool>>,
    pub forwarded_acks: Vec<(
        NodeId,
        voteboat::raft::RequestContext,
        Option<voteboat::outbound::SendTicket>,
    )>,
    pub held_read_acks: BTreeSet<NodeId>,
    pub held_inputs: BTreeMap<NodeId, (RelayReceiveTicket, voteboat::outbound::SendTicket)>,
    pub phase: &'static str,
    pub retiring: bool,
    pub forwarded_probes: Vec<(
        voteboat::outbound::SendTicket,
        voteboat::raft::RequestContext,
    )>,
}
impl Cluster {
    pub fn new(protocol: NativePeerProtocol) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "voteboat-relay352-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        Self::open(root, protocol, NativeOpenMode::Create)
    }
    pub fn open(root: PathBuf, protocol: NativePeerProtocol, mode: NativeOpenMode) -> Self {
        setup::open(root, protocol, mode, false)
    }
    pub fn new_tree(protocol: NativePeerProtocol) -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let root = std::env::temp_dir().join(format!(
            "voteboat-relay364-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        setup::open(root, protocol, NativeOpenMode::Create, true)
    }
    pub fn now(&self) -> MonoTime {
        MonoTime(self.clock.elapsed().as_millis() as u64)
    }
    pub fn poll(&mut self) {
        let now = self.now();
        let hold = &self.held_read_acks;
        let inputs = &mut self.held_inputs;
        let mut helper = RelayForwarderProgress::default();
        for forwarder in self
            .branches
            .iter_mut()
            .chain(std::iter::once(&mut self.helper))
        {
            let progress = forwarder
                .poll_with_admission(now, 3, TransportPollBudget::default(), |node, received| {
                    if !hold.contains(&node) || received.frame().kind() != RelayFrameKind::Output {
                        return true;
                    }
                    let original = received.frame().original_ticket().unwrap();
                    let source = received.connection().peer;
                    let scope = WireScope {
                        from: source.node,
                        sender: source.store,
                        to: original.peer,
                    };
                    let codec = voteboat::native::wire::NativeWireCodec::with_leadership_transfer(
                        Default::default(),
                    )
                    .unwrap();
                    let messages = codec
                        .decode_batch(scope, received.frame().wire_bytes().unwrap())
                        .unwrap();
                    if !messages
                        .iter()
                        .any(|m| matches!(m.rpc, voteboat::raft::Rpc::ReadAck))
                    {
                        return true;
                    }
                    let actual = (received.ticket(), original);
                    if let Some(held) = inputs.get(&node) {
                        assert_eq!(*held, actual, "held original ticket changed");
                    } else {
                        inputs.insert(node, actual);
                    }
                    false
                })
                .unwrap();
            helper.completions.extend(progress.completions);
            helper.source_failures.extend(progress.source_failures);
        }
        self.record_frames(helper.completions);
        for (_, error) in &helper.source_failures {
            assert!(
                (self.retiring || self.boats.iter().all(|b| b.state() != NodeState::Running))
                    && matches!(
                        error,
                        RelayNetworkError::Truncated
                            | RelayNetworkError::Session(
                                SessionError::Closed | SessionError::Truncated
                            )
                            | RelayNetworkError::Session(SessionError::Io(
                                std::io::ErrorKind::ConnectionReset
                            ))
                    ),
                "helper failure {:?}, nodes {:?}",
                error,
                self.boats.iter().map(|b| b.state()).collect::<Vec<_>>()
            );
        }
        for boat in &mut self.boats {
            let p = boat
                .poll(now, NodePollBudget::default())
                .unwrap_or_else(|e| panic!("relay poll {:?}", e));
            if let Some(p) = p.peers {
                self.discarded += p.ingress.discarded;
            }
            if let Some(r) = p.replica {
                for step in r.steps {
                    assert!(step.error.is_none(), "core {:?}", step.error);
                }
            }
        }
    }
    fn record_frames(&mut self, completions: Vec<RelayForwardingCompletion>) {
        for completed in completions {
            if matches!(
                completed.local.frame.kind(),
                RelayFrameKind::SourceOpened | RelayFrameKind::SourceClosed
            ) && completed.local.result == voteboat::outbound::LocalSendResult::Sent
            {
                assert!(self.controls.len() < 1024);
                self.controls
                    .push((completed.local.ticket, completed.local.frame));
                continue;
            }
            let frame = &completed.local.frame;
            if frame.kind() != RelayFrameKind::Forwarded {
                continue;
            }
            let source = frame.source_capture().unwrap().source();
            let scope = WireScope {
                from: source.node,
                sender: source.store,
                to: frame.original_ticket().unwrap().peer,
            };
            let codec = voteboat::native::wire::NativeWireCodec::with_leadership_transfer(
                Default::default(),
            )
            .unwrap();
            let messages = codec
                .decode_batch(scope, frame.wire_bytes().unwrap())
                .unwrap();
            for (m, original) in messages.iter().zip(frame.returned_requests().unwrap()) {
                if matches!(m.rpc, voteboat::raft::Rpc::ReadProbe) {
                    assert!(self.forwarded_probes.len() < 1024);
                    self.forwarded_probes
                        .push((frame.original_ticket().unwrap(), m.context));
                }
                if matches!(m.rpc, voteboat::raft::Rpc::ReadAck) {
                    assert!(self.forwarded_acks.len() < 1024);
                    self.forwarded_acks.push((m.from, m.context, *original));
                }
            }
        }
    }
    pub fn until(&mut self, done: impl Fn(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            self.poll();
            assert!(
                Instant::now() < deadline,
                "native relay wait timed out phase {}: nodes {:?}; helper {:?}; observations {:?}; forwarded {:?}",self.phase,
                self.boats
                    .iter()
                    .map(|b| (b.state(), b.peers()))
                    .collect::<Vec<_>>(),
                self.helper,self.observations.borrow(),self.forwarded_acks
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    pub fn campaign(&mut self) {
        self.boats[0]
            .control(group(352), NodeControl::Campaign)
            .unwrap();
        self.until(|c| {
            c.boats[0].local().owner.core(group(352)).unwrap().role()
                == voteboat::raft::Role::Leader
        });
    }
    pub fn value(&self, slot: usize) -> i64 {
        let a = &self.boats[slot].local().applications[&group(352)];
        a.read_applied(a.applied_index()).unwrap()
    }
}
fn prepare(
    root: &std::path::Path,
    clock: Instant,
    id: u64,
    bootstrap: &Bootstrap,
    protocol: NativePeerProtocol,
    mode: NativeOpenMode,
) -> NativeLocalParts<Counter> {
    if mode == NativeOpenMode::Create {
        std::fs::create_dir_all(root.join(id.to_string())).unwrap();
    }
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen = reserve.local_addr().unwrap();
    drop(reserve);
    let startup = NativeStartup {
        directory: root.join(id.to_string()),
        mode,
        node: node(id),
        store: identity(id as u128),
        bootstrap: bootstrap.clone(),
        listen,
        peers: (1..=3)
            .filter(|n| *n != id)
            .map(|n| {
                (
                    node(n),
                    NativeStartupPeer {
                        address: format!("127.0.0.1:{}", 41000 + n).parse().unwrap(),
                        certificate: certificate(n).to_vec(),
                        server_name: format!("node{n}.voteboat.test"),
                    },
                )
            })
            .collect(),
        tls: config(id).with_wire_version(8).unwrap(),
        entropy_seed: id * 17,
        limits: NodeLimits::default(),
        ready_visit_items: 16,
    };
    let parts = startup
        .prepare_for_discovery(
            protocol,
            TimerConfig {
                election_min_ms: 60_000,
                election_spread_ms: 1_000,
                ..Default::default()
            },
            Counter::new(100).unwrap(),
            Arc::new(ThreadWake::current()),
            MonoTime(clock.elapsed().as_millis() as u64),
        )
        .unwrap();
    let mut peers = parts.peers.unwrap();
    peers.connector.close();
    let now = MonoTime(clock.elapsed().as_millis() as u64);
    assert!(peers
        .connector
        .poll(now, Default::default())
        .unwrap()
        .is_empty());
    let mut dialer = peers
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("unused direct connector not drained"));
    if let Some(d) = &mut dialer {
        let end = Instant::now() + Duration::from_secs(5);
        while !d.try_finish().unwrap() {
            assert!(Instant::now() < end);
            std::thread::yield_now();
        }
    }
    parts.local
}
