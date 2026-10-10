// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{maintenance::*, native::connect::*, raft::*, secure::PeerIdentity};
#[path = "maintenance/binding.rs"]
mod binding;
#[path = "maintenance/drain.rs"]
mod drain;
#[path = "maintenance/drain_recovery.rs"]
mod drain_recovery;
#[path = "maintenance/membership_drain.rs"]
mod membership_drain;
type Managed = NativeNode<Maintenance<HostApplication>, NativeServiceConnector>;
static DIRECTORY: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
struct History {
    directory: PathBuf,
    addresses: Vec<std::net::SocketAddr>,
    bootstrap: Bootstrap,
    protocol: NativePeerProtocol,
    nodes: Vec<Managed>,
    clock: Instant,
    member: bool,
    administration: Option<voteboat::native::administration::NativeAdministrationPlan>,
}
impl History {
    fn new(protocol: NativePeerProtocol, version: u16) -> Self {
        Self::new_mode(protocol, version, false)
    }
    fn new_mode(protocol: NativePeerProtocol, version: u16, member: bool) -> Self {
        let directory = root().with_extension(format!(
            "maintenance-{}",
            DIRECTORY.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        let sockets = (0..3)
            .map(|_| {
                let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let udp = std::net::UdpSocket::bind(tcp.local_addr().unwrap()).unwrap();
                (tcp, udp)
            })
            .collect::<Vec<_>>();
        let addresses = sockets
            .iter()
            .map(|(s, _)| s.local_addr().unwrap())
            .collect();
        let b = support_bootstrap();
        drop(sockets);
        let mut h = Self {
            directory,
            addresses,
            bootstrap: b,
            protocol,
            nodes: vec![],
            clock: Instant::now(),
            member,
            administration: None,
        };
        h.open(NativeOpenMode::Create, version);
        if member {
            // Membership recovery cannot invent a new voter from empty files.
            drain_wire_nodes(std::mem::take(&mut h.nodes), h.clock);
            h.open(NativeOpenMode::Recover, version);
        }
        h
    }
    fn open(&mut self, mode: NativeOpenMode, version: u16) {
        self.clock = Instant::now();
        self.nodes = (1..=3)
            .map(|id| {
                let c = selected_wire_config(
                    &self.directory,
                    &self.addresses,
                    &self.bootstrap,
                    id,
                    mode,
                    version,
                );
                let app = Maintenance::new(group(), 9001, 4, app()).unwrap();
                let wake = Arc::new(ThreadWake::current());
                if self.member && mode != NativeOpenMode::Create {
                    NativeMemberStartup {
                        startup: c,
                        provisioned_stores: self.bootstrap.voter_stores.clone(),
                    }
                    .open_with_protocol_and_timers(
                        self.protocol,
                        NativeTimingProfile::Throughput.timers(),
                        app,
                        wake,
                        MonoTime(0),
                    )
                    .unwrap()
                } else {
                    c.open_with_protocol_and_timers(
                        self.protocol,
                        NativeTimingProfile::Throughput.timers(),
                        app,
                        wake,
                        MonoTime(0),
                    )
                    .unwrap()
                }
            })
            .collect();
    }
    fn poll(&mut self) {
        let now = MonoTime(self.clock.elapsed().as_millis() as u64);
        for (id, n) in self.nodes.iter_mut().enumerate() {
            let progress = if let Some(administration) = &self.administration {
                n.poll_with_configuration_authorization(
                    now,
                    NodePollBudget::default(),
                    |core, proposal| administration.authorize(group(), core.membership(), proposal),
                )
            } else {
                n.poll(now, NodePollBudget::default())
            }
            .unwrap();
            if let Some(replica) = progress.replica {
                for step in replica.steps.iter().filter(|s| s.error.is_some()) {
                    eprintln!("node {} maintenance step {step:?}", id + 1);
                }
            }
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    fn wait(&mut self, phase: &str, predicate: impl Fn(&Self) -> bool) {
        let start = Instant::now();
        while !predicate(self) {
            self.poll();
            assert!(
                start.elapsed() < Duration::from_secs(10),
                "native maintenance timeout at {phase}: {:?}",
                (0..self.nodes.len())
                    .map(|i| (
                        self.core(i).role(),
                        self.core(i).state().hard_state.term,
                        self.core(i).state().commit_index,
                        self.core(i).state().base_index(),
                        self.app(i).applied_index(),
                        self.core(i).leadership_transfer()
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }
    fn app(&self, id: usize) -> &Maintenance<HostApplication> {
        &self.nodes[id].local().applications[&group()]
    }
    fn core(&self, id: usize) -> &Raft {
        self.nodes[id].local().owner.core(group()).unwrap()
    }
    fn elect(&mut self, id: usize) {
        self.wait("authenticated peers", |h| {
            h.nodes.iter().enumerate().all(|(local, n)| {
                (0..3).filter(|remote| *remote != local).all(|remote| {
                    n.peers()
                        .unwrap()
                        .roster()
                        .binding(NodeId::new(remote as u64 + 1).unwrap())
                        .is_some()
                })
            })
        });
        // Reopening can already have queued automatic campaigns on every
        // replica. Let that election settle and catch up the requested voter
        // before issuing a deliberate higher-term campaign for this fixture.
        self.wait("settled startup election", |h| {
            h.nodes.iter().enumerate().any(|(leader, _)| {
                let core = h.core(leader);
                core.role() == Role::Leader
                    && core.state().term_at(core.state().commit_index)
                        == Some(core.state().hard_state.term)
                    && h.core(id).state().commit_index >= core.state().commit_index
            })
        });
        if self.core(id).role() == Role::Leader {
            return;
        }
        self.nodes[id]
            .control(group(), NodeControl::Campaign)
            .unwrap();
        self.wait("election", |h| {
            h.core(id).role() == Role::Leader
                && h.core(id).state().term_at(h.core(id).state().commit_index)
                    == Some(h.core(id).state().hard_state.term)
                && h.nodes
                    .iter()
                    .all(|n| n.local().applications[&group()].applied_index() > 0)
        });
    }
    fn submit(
        &mut self,
        id: usize,
        operation: OperationId,
        bytes: Vec<u8>,
    ) -> ClientOutcome<MaintenanceReceipt<CounterReceipt>> {
        self.submit_with(id, operation, bytes, false)
    }
    fn submit_with(
        &mut self,
        id: usize,
        operation: OperationId,
        bytes: Vec<u8>,
        maintenance: bool,
    ) -> ClientOutcome<MaintenanceReceipt<CounterReceipt>> {
        let request = ClientRequest {
            group: group(),
            operation,
            bytes,
        };
        let ticket = if maintenance {
            self.nodes[id].propose_maintenance(request)
        } else {
            self.nodes[id].propose(request)
        }
        .unwrap();
        let start = Instant::now();
        loop {
            self.poll();
            if let Some(output) = self.nodes[id].poll_client() {
                assert_eq!(output.ticket(), ticket);
                let outcome = self.nodes[id].complete_client(output).unwrap();
                if let ClientOutcome::Applied { position, .. } = &outcome {
                    let index = position.index;
                    self.wait("all replicas applied", |h| {
                        h.nodes
                            .iter()
                            .all(|n| n.local().applications[&group()].applied_index() >= index)
                    });
                }
                return outcome;
            }
            assert!(start.elapsed() < Duration::from_secs(10));
        }
    }
    fn control(&mut self, id: usize, command: LeadershipCommand) -> LeadershipRecord {
        match self.submit_with(
            id,
            command.intent().request.operation,
            command.encode().unwrap(),
            true,
        ) {
            ClientOutcome::Applied {
                receipt:
                    MaintenanceReceipt::Administration {
                        outcome: MaintenanceOutcome::Recorded(r),
                        ..
                    },
                ..
            } => r,
            other => panic!("unexpected maintenance outcome: {other:?}"),
        }
    }
    fn read_record(&mut self, id: usize, operation: OperationId) -> LeadershipRecord {
        let ticket = self.nodes[id]
            .read(group(), MaintenanceQuery::Leadership(operation))
            .unwrap();
        let start = Instant::now();
        loop {
            self.poll();
            if let Some(output) = self.nodes[id].poll_read() {
                assert_eq!(output.ticket(), ticket);
                let ReadOutcome::Read {
                    result: Ok(MaintenanceRead::Leadership(Some(record))),
                    ..
                } = self.nodes[id].complete_read(output).unwrap()
                else {
                    panic!("expected quorum-backed maintenance status");
                };
                return record;
            }
            assert!(start.elapsed() < Duration::from_secs(10));
        }
    }
    fn reopen(&mut self, checkpoint: bool) {
        if checkpoint {
            let index = self
                .nodes
                .iter()
                .map(|n| n.local().applications[&group()].applied_index())
                .min()
                .unwrap();
            for n in &mut self.nodes {
                n.control(group(), NodeControl::Checkpoint).unwrap();
            }
            self.wait("checkpoint", |h| {
                (0..3).all(|id| h.core(id).state().base_index() >= index)
            });
        }
        drain_wire_nodes(std::mem::take(&mut self.nodes), self.clock);
        self.open(NativeOpenMode::Recover, 8);
    }
    fn close(mut self) {
        drain_wire_nodes(std::mem::take(&mut self.nodes), self.clock);
        std::fs::remove_dir_all(&self.directory).unwrap();
    }
}
fn support_bootstrap() -> Bootstrap {
    let voter_stores = (1..=3)
        .map(|n| {
            (
                NodeId::new(n).unwrap(),
                StoreIdentity {
                    id: StoreId::new(n.into()).unwrap(),
                    incarnation: StoreIncarnation::new(1).unwrap(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(9).unwrap(),
        policy: Policy::new(
            Tree::Majority(voter_stores.keys().copied().map(Tree::Voter).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores,
    }
}
fn handoff(protocol: NativePeerProtocol, checkpoint: bool) {
    let mut h = History::new(protocol, 8);
    h.elect(0);
    let intent = LeadershipIntent {
        request: LeadershipTransferRequest {
            operation: OperationId::new(u128::MAX).unwrap(),
            configuration: h.core(0).membership().id(),
            target: PeerIdentity {
                node: h.core(1).local_node(),
                store: h.core(1).storage_binding().identity,
            },
        },
        source: PeerIdentity {
            node: h.core(0).local_node(),
            store: h.core(0).storage_binding().identity,
        },
    };
    let first = h.control(0, LeadershipCommand::Begin(intent));
    assert_eq!(first.phase, LeadershipPhase::Pending);
    h.reopen(checkpoint);
    assert_eq!(h.app(0).pending(), Some(first));
    h.elect(0);
    let before = h.app(0).checkpoint(100000).unwrap();
    let illegal = LeadershipCommand::Complete {
        intent,
        index: first.index,
        term: h.core(0).state().hard_state.term,
    };
    assert!(h.nodes[0]
        .propose(ClientRequest {
            group: group(),
            operation: intent.request.operation,
            bytes: illegal.encode().unwrap()
        })
        .is_err());
    assert_eq!(h.app(0).checkpoint(100000).unwrap(), before);
    let LeadershipAction::Transfer(request) = h.app(0).leadership_action(h.core(0)).unwrap() else {
        panic!("expected handoff");
    };
    h.nodes[0]
        .control(group(), NodeControl::TransferLeadership(request))
        .unwrap();
    h.wait("target leadership", |h| {
        h.core(1).role() == Role::Leader
            && matches!(
                h.app(1).leadership_action(h.core(1)),
                Ok(LeadershipAction::Complete(_))
            )
    });
    let LeadershipAction::Complete(complete) = h.app(1).leadership_action(h.core(1)).unwrap()
    else {
        unreachable!()
    };
    let stale = LeadershipCommand::Complete {
        intent,
        index: first.index,
        term: h.core(1).state().hard_state.term - 1,
    };
    assert!(h.nodes[1]
        .propose(ClientRequest {
            group: group(),
            operation: intent.request.operation,
            bytes: stale.encode().unwrap(),
        })
        .is_err());
    let done = h.control(1, complete);
    assert!(matches!(done.phase, LeadershipPhase::Completed { term, .. } if term > first.term));
    assert_eq!(h.control(1, complete), done);
    h.reopen(checkpoint);
    for id in 0..3 {
        assert_eq!(h.app(id).record(intent.request.operation), Some(done));
    }
    h.elect(1);
    assert_eq!(h.read_record(1, intent.request.operation), done);
    assert!(matches!(
        h.submit(
            1,
            OperationId::new(20).unwrap(),
            Maintenance::<HostApplication>::data(&7i64.to_le_bytes(), 100).unwrap()
        ),
        ClientOutcome::Applied { .. }
    ));
    for id in 0..3 {
        assert_eq!(
            h.app(id).inner().0.read_applied(h.app(id).applied_index()),
            Ok(7)
        );
    }
    h.close();
}
#[test]
fn durable_leadership_handoff_reopens_pending_and_completed_intents_tcp() {
    handoff(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn durable_leadership_handoff_reopens_pending_and_completed_checkpoints_quic() {
    handoff(NativePeerProtocol::Quic, true);
}
#[test]
fn leadership_node_control_refuses_old_wire_before_admitting_work() {
    let mut h = History::new(NativePeerProtocol::TcpTls, 7);
    let request = LeadershipTransferRequest {
        operation: OperationId::new(1).unwrap(),
        configuration: h.bootstrap.configuration,
        target: PeerIdentity {
            node: NodeId::new(2).unwrap(),
            store: h.bootstrap.voter_stores[&NodeId::new(2).unwrap()],
        },
    };
    let before = h.core(0).state().clone();
    assert_eq!(
        h.nodes[0].control(group(), NodeControl::TransferLeadership(request)),
        Err(NodeError::IncompatiblePeerProtocol)
    );
    assert!(h.nodes[0].local().owner.is_drained());
    assert_eq!(h.core(0).state(), &before);
    h.close();
}
