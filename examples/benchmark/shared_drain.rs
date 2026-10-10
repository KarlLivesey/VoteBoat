// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real shared-WAL Nodes exercising the public bounded drain coordinator.
use super::*;
use voteboat::{
    drain::*,
    membership::*,
    native::{administration::*, drain_journal::*, placement::*},
    placement::*,
    raft::*,
};

struct Harness {
    root: std::path::PathBuf,
    clock: Instant,
    protocol: NativePeerProtocol,
    bootstraps: Vec<Bootstrap>,
    addresses: BTreeMap<u64, std::net::SocketAddr>,
    plan: MembershipDrainPlan,
    journal: NativeDrainJournal,
    authorization: BTreeMap<GroupIdentity, NativeAdministrationPlan>,
    nodes: Vec<Replica<SharedLog>>,
}
fn plan(bootstraps: &[Bootstrap]) -> MembershipDrainPlan {
    let entries = bootstraps
        .iter()
        .map(|b| {
            let original = Configuration::new(
                b.configuration,
                b.policy.clone(),
                b.voter_stores.clone(),
                Default::default(),
            )
            .unwrap();
            let target = Configuration::new(
                ConfigurationId::new(3).unwrap(),
                Policy::new(
                    Tree::Majority(vec![Tree::Voter(node(2)), Tree::Voter(node(3))]),
                    Limits::default(),
                )
                .unwrap(),
                [(node(2), store(2)), (node(3), store(3))].into(),
                [(node(1), store(1))].into(),
            )
            .unwrap();
            let operation = OperationId::new(200 + b.group.id.get()).unwrap();
            DrainMembershipGroup {
                group: b.group,
                original,
                handoff: PeerIdentity {
                    node: node(if b.group.id.get() == 2 { 3 } else { 2 }),
                    store: store(if b.group.id.get() == 2 { 3 } else { 2 }),
                },
                change: PlannedVoterChange {
                    joint: ConfigurationRecord {
                        operation,
                        expected: ConfigurationId::new(1).unwrap(),
                        change: ConfigurationChange::Joint {
                            id: ConfigurationId::new(2).unwrap(),
                            next: target,
                        },
                    },
                    finalize: ConfigurationRecord {
                        operation,
                        expected: ConfigurationId::new(2).unwrap(),
                        change: ConfigurationChange::Final {
                            id: ConfigurationId::new(3).unwrap(),
                        },
                    },
                },
            }
        })
        .collect();
    MembershipDrainPlan::new(
        PeerIdentity {
            node: node(1),
            store: store(1),
        },
        OperationId::new(9001).unwrap(),
        entries,
    )
    .unwrap()
}
impl Harness {
    fn new(protocol: NativePeerProtocol, mixed: bool) -> Self {
        let root = std::env::temp_dir().join(format!(
            "voteboat-multi-drain-{}-{protocol:?}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let sockets = (1..=3)
            .map(|n| {
                let socket = TcpListener::bind("127.0.0.1:0").unwrap();
                let udp = UdpSocket::bind(socket.local_addr().unwrap()).unwrap();
                (n, socket, udp)
            })
            .collect::<Vec<_>>();
        let addresses = sockets
            .iter()
            .map(|(n, s, _)| (*n, s.local_addr().unwrap()))
            .collect();
        let policy = Policy::new(
            Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
            Limits::default(),
        )
        .unwrap();
        let bootstraps = (1..=if mixed { 4 } else { 3 })
            .map(|g| Bootstrap {
                group: group_id(g),
                configuration: ConfigurationId::new(1).unwrap(),
                policy: policy.clone(),
                voter_stores: (1..=3).map(|n| (node(n), store(n))).collect(),
            })
            .collect::<Vec<_>>();
        let plan = plan(&bootstraps);
        let journal = NativeDrainJournal::initialize(
            FileDrainRecord::new(root.join("drain.record")),
            plan.record(1).unwrap().owner,
        )
        .ok()
        .unwrap();
        let mut h = Self {
            root,
            clock: Instant::now(),
            protocol,
            bootstraps,
            addresses,
            plan,
            journal,
            authorization: BTreeMap::new(),
            nodes: vec![],
        };
        drop(sockets);
        h.nodes = h.open(NativeOpenMode::Create);
        h.configure_authorization();
        for g in 1..=h.bootstraps.len() {
            h.nodes[0]
                .control(group_id(g), NodeControl::Campaign)
                .unwrap();
        }
        h.wait(|h| (1..=h.bootstraps.len()).all(|g| h.leader(group_id(g)) == Some(0)));
        for g in 1..=h.bootstraps.len() {
            h.write(group_id(g), 100 + g as u128, 7, false, 7);
        }
        if mixed {
            h.prepare_retained();
        }
        h.journal.publish(h.plan.record(1).unwrap()).unwrap();
        h.nodes[0].restore_drain(&h.journal).unwrap();
        h
    }
    fn configure_authorization(&mut self) {
        for entry in self.plan.groups() {
            let placement = NativePlacementAuthorizer::new(
                entry.group,
                entry
                    .original
                    .voter_stores()
                    .iter()
                    .map(|(n, s)| {
                        (
                            *n,
                            ReplicaPlacement {
                                store: *s,
                                domain: FailureDomainId::new(n.get()).unwrap(),
                            },
                        )
                    })
                    .collect(),
                PlacementRequirements {
                    minimum_voting_domains: 2,
                    survive_any_single_domain_loss: false,
                },
            )
            .unwrap();
            let requirements = self.nodes[0].local().applications[&entry.group]
                .deployment_requirements()
                .unwrap();
            self.authorization.insert(
                entry.group,
                NativeAdministrationPlan::new(
                    entry.group,
                    placement,
                    requirements,
                    vec![entry.change.joint.clone(), entry.change.finalize.clone()],
                )
                .unwrap(),
            );
        }
    }
    fn open(&self, mode: NativeOpenMode) -> Vec<Replica<SharedLog>> {
        (1..=3)
            .map(|n| {
                ClusterSetup {
                    root: &self.root,
                    mode,
                    protocol: self.protocol,
                    capacity: 128,
                    clock: &self.clock,
                    bootstraps: &self.bootstraps,
                    addresses: &self.addresses,
                    lane: 1,
                }
                .open_profile(n, None, true)
                .unwrap()
                .0
            })
            .collect()
    }
    fn poll(&mut self) {
        let now = MonoTime(self.clock.elapsed().as_millis() as u64);
        for n in &mut self.nodes {
            n.poll_with_configuration_authorization(
                now,
                NodePollBudget::default(),
                |core, proposal| {
                    let g = core.state().bootstrap.group;
                    self.authorization[&g].authorize(g, core.membership(), proposal)
                },
            )
            .unwrap();
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    fn core(&self, index: usize, g: GroupIdentity) -> &Raft {
        self.nodes[index].local().owner.core(g).unwrap()
    }
    fn leader(&self, g: GroupIdentity) -> Option<usize> {
        self.nodes.iter().position(|n| {
            let c = n.local().owner.core(g).unwrap();
            c.role() == Role::Leader
                && c.state().term_at(c.state().commit_index) == Some(c.state().hard_state.term)
        })
    }
    fn wait(&mut self, done: impl Fn(&Self) -> bool) {
        let until = Instant::now() + Duration::from_secs(20);
        while !done(self) {
            self.poll();
            assert!(
                Instant::now() < until,
                "multi-group wait: {}",
                self.root.display()
            );
        }
    }
    fn progress(&self) -> Vec<String> {
        self.nodes
            .iter()
            .enumerate()
            .map(|(index, n)| {
                let groups = (1..=self.bootstraps.len())
                    .map(|g| {
                        let c = n.local().owner.core(group_id(g)).unwrap();
                        format!(
                            "group={g} role={:?} configuration={} at={} commit={}",
                            c.role(),
                            c.membership().id().get(),
                            c.membership().last_configuration_index(),
                            c.state().commit_index
                        )
                    })
                    .collect::<Vec<_>>();
                format!("node={index} {groups:?}")
            })
            .collect()
    }
    fn write(
        &mut self,
        g: GroupIdentity,
        operation: u128,
        delta: i64,
        duplicate: bool,
        expected: i64,
    ) {
        self.wait(|h| h.leader(g).is_some());
        let leader = self.leader(g).unwrap();
        let ticket = self.nodes[leader]
            .propose(ClientRequest {
                group: g,
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            })
            .unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            self.poll();
            if let Some(output) = self.nodes[leader].poll_client() {
                assert_eq!(output.ticket(), ticket);
                let result = self.nodes[leader].complete_client(output).unwrap();
                assert!(
                    matches!(result,ClientOutcome::Applied{receipt:CounterReceipt{duplicate:found,operation:op,outcome:CounterOutcome::Value(value),..},..} if found==duplicate && op.get()==operation && value==expected),
                    "{result:?}"
                );
                return;
            }
            assert!(Instant::now() < until, "write timeout");
        }
    }
    #[cfg(feature = "quic")]
    fn checkpoint(&mut self) {
        for n in &mut self.nodes {
            for g in 1..=self.bootstraps.len() {
                n.control(group_id(g), NodeControl::Checkpoint).unwrap();
            }
        }
        self.wait(|h| {
            h.nodes.iter().all(|n| {
                (1..=h.bootstraps.len()).all(|g| {
                    n.local()
                        .owner
                        .core(group_id(g))
                        .unwrap()
                        .state()
                        .base_index()
                        > 0
                }) && n.local().snapshots.as_ref().unwrap().router.is_drained()
            })
        });
    }
    fn reopen(&mut self) {
        close(std::mem::take(&mut self.nodes), &self.clock).unwrap();
        self.journal = NativeDrainJournal::recover(
            FileDrainRecord::new(self.root.join("drain.record")),
            self.plan.record(1).unwrap().owner,
        )
        .ok()
        .unwrap();
        self.nodes = self.open(NativeOpenMode::Recover);
        self.nodes[0].restore_drain(&self.journal).unwrap();
    }
    fn prepare_retained(&mut self) {
        let entry = self.plan.groups()[3].clone();
        self.configure_before_drain(entry.change.joint);
        self.configure_before_drain(entry.change.finalize);
        self.plan = MembershipDrainPlan::with_retained_learners(
            self.plan.record(1).unwrap().owner,
            self.plan.record(1).unwrap().request.operation,
            self.plan.groups()[..3].to_vec(),
            vec![DrainRetainedGroup {
                group: group_id(4),
                original: self.core(0, group_id(4)).membership().stable().clone(),
            }],
        )
        .unwrap();
    }
    fn configure_before_drain(&mut self, record: ConfigurationRecord) {
        self.wait(|h| h.leader(group_id(4)).is_some());
        let owner = self.leader(group_id(4)).unwrap();
        let configuration = match &record.change {
            ConfigurationChange::Joint { id, .. } | ConfigurationChange::Final { id } => id.get(),
            _ => unreachable!(),
        };
        let requirements = self.nodes[owner].local().applications[&group_id(4)]
            .deployment_requirements()
            .unwrap();
        let ticket = self.nodes[owner]
            .configure(ConfigurationRequest {
                group: group_id(4),
                proposal: ConfigurationProposal {
                    record,
                    requirements,
                    readiness: vec![],
                },
            })
            .unwrap();
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            self.poll();
            if let Some(result) = self.nodes[owner].poll_configuration() {
                assert_eq!(result.ticket, ticket);
                assert!(matches!(
                    result.outcome,
                    ConfigurationOutcome::Committed(_)
                        | ConfigurationOutcome::Unknown(ConfigurationUnknown::LeadershipChanged)
                ));
                break;
            }
            assert!(Instant::now() < until, "retained group preparation timeout");
        }
        self.wait(|h| committed(h, 4, configuration));
    }
}
enum WaitKind {
    Transfer(NodeId),
    Configuration(ConfigurationTicket),
}
struct Pending {
    ticket: DrainDispatchTicket,
    owner: usize,
    kind: WaitKind,
}
struct Driver {
    coordinator: MembershipDrainCoordinator,
    pending: Vec<Pending>,
    lost: bool,
}
impl Driver {
    fn new(plan: MembershipDrainPlan) -> Self {
        Self {
            coordinator: MembershipDrainCoordinator::new(plan, 2).unwrap(),
            pending: vec![],
            lost: false,
        }
    }
    fn complete(&mut self, h: &mut Harness) {
        for i in 0..h.nodes.len() {
            while let Some(result) = h.nodes[i].poll_configuration() {
                let at = self
                    .pending
                    .iter()
                    .position(|p| matches!(p.kind,WaitKind::Configuration(t) if t==result.ticket))
                    .unwrap();
                assert_eq!(self.pending[at].owner, i);
                assert!(
                    matches!(
                        result.outcome,
                        ConfigurationOutcome::Committed(_)
                            | ConfigurationOutcome::Unknown(
                                ConfigurationUnknown::CancelledWait
                                    | ConfigurationUnknown::LeadershipChanged
                            )
                    ),
                    "{result:?}"
                );
                self.coordinator
                    .finish(&self.pending.remove(at).ticket)
                    .unwrap();
            }
        }
        let mut index = 0;
        while index < self.pending.len() {
            let p = &self.pending[index];
            let ready = match p.kind {
                WaitKind::Transfer(target) => h
                    .leader(p.ticket.group())
                    .is_some_and(|i| h.core(i, p.ticket.group()).local_node() == target),
                WaitKind::Configuration(_) => false,
            };
            if ready {
                self.coordinator
                    .finish(&self.pending.remove(index).ticket)
                    .unwrap();
            } else {
                index += 1;
            }
        }
    }
    fn tick(&mut self, h: &mut Harness, partial: bool) {
        h.poll();
        self.complete(h);
        let batch = self
            .coordinator
            .poll(&h.journal, 1, |g| {
                if partial
                    && (g == group_id(2)
                        || g == group_id(3)
                            && h.nodes.iter().any(|n| {
                                n.local()
                                    .owner
                                    .core(g)
                                    .unwrap()
                                    .membership()
                                    .joint()
                                    .is_some()
                            }))
                {
                    return None;
                }
                h.leader(g).map(|i| h.core(i, g))
            })
            .unwrap();
        assert!(batch.errors.is_empty(), "{:?}", batch.errors);
        assert!(batch.requests.len() <= 1 && self.coordinator.in_flight() <= 2);
        for dispatch in batch.requests {
            let g = dispatch.ticket.group();
            assert!(!self
                .coordinator
                .plan()
                .retained_learners()
                .iter()
                .any(|entry| entry.group == g));
            let owner = h.leader(g).unwrap();
            let kind = match dispatch.action {
                MembershipDrainAction::Transfer(request) => {
                    h.nodes[owner]
                        .control(g, NodeControl::TransferLeadership(request))
                        .unwrap();
                    WaitKind::Transfer(request.target.node)
                }
                MembershipDrainAction::Configure(record) => {
                    let requirements = h.nodes[owner].local().applications[&g]
                        .deployment_requirements()
                        .unwrap();
                    let ticket = h.nodes[owner]
                        .configure(ConfigurationRequest {
                            group: g,
                            proposal: ConfigurationProposal {
                                record,
                                requirements,
                                readiness: vec![],
                            },
                        })
                        .unwrap();
                    if !self.lost {
                        h.nodes[owner].cancel_configuration(ticket).unwrap();
                        self.lost = true;
                    }
                    WaitKind::Configuration(ticket)
                }
                _ => panic!("non-dispatch action"),
            };
            self.pending.push(Pending {
                ticket: dispatch.ticket,
                owner,
                kind,
            });
        }
    }
}
fn committed(h: &Harness, g: usize, configuration: u64) -> bool {
    h.nodes.iter().all(|n| {
        let core = n.local().owner.core(group_id(g)).unwrap();
        core.membership().id().get() == configuration
            && core.membership().last_configuration_index() <= core.state().commit_index
    })
}
fn history(protocol: NativePeerProtocol, mixed: bool) {
    let mut h = Harness::new(protocol, mixed);
    let mut driver = Driver::new(h.plan.clone());
    let deadline = Instant::now() + Duration::from_secs(20);
    while !(committed(&h, 1, 3)
        && committed(&h, 2, 1)
        && committed(&h, 3, 2)
        && driver.pending.is_empty())
    {
        driver.tick(&mut h, true);
        assert!(
            Instant::now() < deadline,
            "mixed drain state timeout: {}, progress: {:?}, pending: {}",
            h.root.display(),
            h.progress(),
            driver.pending.len()
        );
    }
    assert!(!h.nodes[0]
        .membership_drain_ready(&h.plan, &h.journal)
        .unwrap());
    h.write(group_id(1), 501, 2, false, 9);
    let batch = driver
        .coordinator
        .poll(&h.journal, h.plan.assignments().len(), |g| {
            if g == group_id(2) {
                h.leader(g).map(|i| h.core(i, g))
            } else {
                None
            }
        })
        .unwrap();
    let old = batch.requests.into_iter().next().unwrap().ticket;
    #[cfg(feature = "quic")]
    if protocol == NativePeerProtocol::Quic {
        h.checkpoint();
    }
    h.reopen();
    let mut driver = Driver::new(h.plan.clone());
    assert_eq!(
        driver.coordinator.finish(&old),
        Err(DrainCoordinatorError::StaleTicket)
    );
    let deadline = Instant::now() + Duration::from_secs(25);
    while !h.nodes[0]
        .membership_drain_ready(&h.plan, &h.journal)
        .unwrap()
        || !driver.pending.is_empty()
    {
        driver.tick(&mut h, false);
        assert!(
            Instant::now() < deadline,
            "resumed drain timeout: {}",
            h.root.display()
        );
    }
    for g in 1..=h.bootstraps.len() {
        assert!(committed(&h, g, 3));
    }
    let source = h.nodes.remove(0);
    close(vec![source], &h.clock).unwrap();
    for g in 1..=h.bootstraps.len() {
        h.write(group_id(g), 100 + g as u128, 7, true, 7);
        h.write(
            group_id(g),
            600 + g as u128,
            1,
            false,
            if g == 1 { 10 } else { 8 },
        );
    }
    close(std::mem::take(&mut h.nodes), &h.clock).unwrap();
    std::fs::remove_dir_all(&h.root).unwrap();
}
#[test]
fn tcp_shared_wal_drain_resumes_mixed_group_progress() {
    history(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_shared_wal_drain_resumes_mixed_checkpoint_progress() {
    history(NativePeerProtocol::Quic, false);
}
#[test]
fn tcp_mixed_voter_and_learner_assignments_resume_together() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_mixed_voter_and_learner_assignments_resume_together() {
    history(NativePeerProtocol::Quic, true);
}
