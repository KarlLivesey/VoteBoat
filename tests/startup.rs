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
#![cfg(feature = "tls")]
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    native::{node::*, startup::*, tls::*, worker::*},
    quorum::*,
    runtime::*,
};
#[derive(Clone)]
struct HostApplication(Counter);
impl StateMachine for HostApplication {
    type Receipt = CounterReceipt;
    fn applied_index(&self) -> u64 {
        self.0.applied_index()
    }
    fn apply_batch(&mut self, e: &[LogEntry]) -> Result<Vec<CounterReceipt>, ApplicationError> {
        self.0.apply_batch(e)
    }
}
impl BoundedStateMachine for HostApplication {
    fn receipt_bytes_bound(&self, e: &[LogEntry]) -> Result<usize, ApplicationError> {
        self.0.receipt_bytes_bound(e)
    }
}
impl ProposalAdmission for HostApplication {
    fn validate_proposal<'a>(
        &self,
        o: OperationId,
        b: &[u8],
        p: impl Iterator<Item = (OperationId, &'a [u8])>,
    ) -> Result<usize, ApplicationError> {
        self.0.validate_proposal(o, b, p)
    }
}
impl ReadableStateMachine for HostApplication {
    type Query = ();
    type ReadResult = i64;
    fn read_at(&self, i: u64, q: ()) -> Result<i64, ApplicationError> {
        self.0.read_at(i, q)
    }
}
impl BoundedReadableStateMachine for HostApplication {
    fn query_bytes(&self, q: &(), l: usize) -> Result<usize, ApplicationError> {
        self.0.query_bytes(q, l)
    }
    fn read_result_bound(&self, q: &()) -> Result<usize, ApplicationError> {
        self.0.read_result_bound(q)
    }
    fn read_result_bytes(&self, r: &i64, l: usize) -> Result<usize, ApplicationError> {
        self.0.read_result_bytes(r, l)
    }
}
impl CheckpointStateMachine for HostApplication {
    fn schema_version(&self) -> u64 {
        self.0.schema_version()
    }
    fn checkpoint(&self, l: usize) -> Result<Vec<u8>, ApplicationError> {
        self.0.checkpoint(l)
    }
    fn restore_checkpoint(&mut self, s: u64, i: u64, b: &[u8]) -> Result<(), ApplicationError> {
        self.0.restore_checkpoint(s, i, b)
    }
}
fn app() -> HostApplication {
    HostApplication(Counter::new(100).unwrap())
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(77).unwrap(),
        incarnation: GroupIncarnation::new(4).unwrap(),
    }
}
fn root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "voteboat-startup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn config(directory: PathBuf, mode: NativeOpenMode) -> NativeStartup {
    let node = NodeId::new(44).unwrap();
    let store = StoreIdentity {
        id: StoreId::new(1234).unwrap(),
        incarnation: StoreIncarnation::new(3).unwrap(),
    };
    NativeStartup {
        directory,
        mode,
        node,
        store,
        bootstrap: Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(9).unwrap(),
            policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
            voter_stores: [(node, store)].into(),
        },
        listen: "127.0.0.1:0".parse().unwrap(),
        peers: BTreeMap::new(),
        entropy_seed: 17,
        limits: NodeLimits::default(),
        tls: NativeTlsConfig::new(TlsCredentials {
            roots: vec![include_bytes!("fixtures/tls/ca.der").to_vec()],
            certificate_chain: vec![include_bytes!("fixtures/tls/node1.der").to_vec()],
            private_key: include_bytes!("fixtures/tls/node1-key.der").to_vec(),
        })
        .unwrap(),
    }
}
fn drive(n: &mut NativeNode<HostApplication>, done: impl Fn(&NativeNode<HostApplication>) -> bool) {
    let start = Instant::now();
    loop {
        n.poll(
            MonoTime(start.elapsed().as_millis() as u64),
            NodePollBudget::default(),
        )
        .unwrap();
        if done(n) {
            return;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn close(mut n: NativeNode<HostApplication>) {
    // Each helper invocation starts a new local clock domain only after a complete open.
    // Use a forward time for shutdown following the earlier drive's elapsed milliseconds.
    n.begin_shutdown();
    for tick in 10000..20000 {
        n.poll(MonoTime(tick), NodePollBudget::default()).unwrap();
        if n.is_drained() {
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut p = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
    let mut d = p
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("connector"));
    let mut s = p.local.snapshots.take().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let (mut log, mut snap) = (false, false);
    loop {
        let dial = d.try_finish().unwrap();
        if !log {
            log = p.local.persistence.try_reclaim().unwrap().is_some();
        }
        if !snap {
            snap = s.worker.try_reclaim().unwrap().is_some();
        }
        if dial && log && snap {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
#[test]
fn native_administration_receipts_commit_joint_and_final_and_reopen_exact_history() {
    use voteboat::{
        membership::*,
        native::log_store::{FileLogIo, NativeLogStore},
        raft::*,
    };
    use voteboat::{native::placement::NativePlacementAuthorizer, placement::*};
    let root = root();
    let mut startup = config(root.clone(), NativeOpenMode::Create);
    startup.tls = startup.tls.with_wire_version(4).unwrap();
    let bootstrap = startup.bootstrap.clone();
    let identity = startup.store;
    let local = startup.node;
    let placement = NativePlacementAuthorizer::new(
        group(),
        [(
            local,
            ReplicaPlacement {
                store: identity,
                domain: FailureDomainId::new(1).unwrap(),
            },
        )]
        .into(),
        PlacementRequirements {
            minimum_voting_domains: 1,
            survive_any_single_domain_loss: false,
        },
    )
    .unwrap();
    let mut n = startup
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .unwrap();
    n.control(group(), NodeControl::Campaign).unwrap();
    drive(&mut n, |n| {
        n.local().applications[&group()].applied_index() > 0
    });
    let operation = OperationId::new(700).unwrap();
    for (expected_index, finalizing) in (2..).zip([false, true]) {
        let ticket = if finalizing {
            let ConfigurationResumption::Submitted(ticket) = n
                .resume_configuration(
                    group(),
                    operation,
                    ReadinessRequirements {
                        application_schema: 1,
                        command_bytes: 8,
                        snapshot_bytes: 4096,
                    },
                )
                .unwrap()
            else {
                panic!("joint must be resumable")
            };
            ticket
        } else {
            let request = ConfigurationRequest {
                group: group(),
                proposal: ConfigurationProposal {
                    record: ConfigurationRecord {
                        operation,
                        expected: ConfigurationId::new(9).unwrap(),
                        change: ConfigurationChange::Joint {
                            id: ConfigurationId::new(10).unwrap(),
                            next: Configuration::new(
                                ConfigurationId::new(11).unwrap(),
                                bootstrap.policy.clone(),
                                bootstrap.voter_stores.clone(),
                                BTreeMap::new(),
                            )
                            .unwrap(),
                        },
                    },
                    readiness: vec![],
                    requirements: ReadinessRequirements {
                        application_schema: 1,
                        command_bytes: 8,
                        snapshot_bytes: 4096,
                    },
                },
            };
            let before = n.local().owner.core(group()).unwrap().state().clone();
            let mut oversized = ConfigurationRequest {
                group: request.group,
                proposal: request.proposal.clone(),
            };
            oversized.proposal.requirements.snapshot_bytes = usize::MAX;
            let rejected = n.configure(oversized).unwrap();
            n.poll_with_placement_authorizer(MonoTime(5000), NodePollBudget::default(), &placement)
                .unwrap();
            let outcome = n.poll_configuration().unwrap();
            assert_eq!(outcome.ticket, rejected);
            assert_eq!(
                outcome.outcome,
                ConfigurationOutcome::NotProposed(
                    ConfigurationProposalError::TransportCapacity(
                        voteboat::transport::TransportError::Wire(
                            voteboat::wire::WireError::TooLarge
                        )
                    )
                    .into()
                )
            );
            assert_eq!(n.local().owner.core(group()).unwrap().state(), &before);
            n.configure(request).unwrap()
        };
        let start = Instant::now();
        let result = loop {
            assert_eq!(n.local().owner.core(group()).unwrap().local_node(), local);
            assert_eq!(ticket.operation(), operation);
            n.poll_with_placement_authorizer(MonoTime(5000), NodePollBudget::default(), &placement)
                .unwrap();
            if let Some(c) = n.poll_configuration() {
                break c;
            }
            assert!(start.elapsed() < Duration::from_secs(5));
            std::thread::park_timeout(Duration::from_millis(1));
        };
        assert_eq!(result.ticket, ticket);
        assert_eq!(
            result.outcome,
            ConfigurationOutcome::Committed(ProposalPosition {
                index: expected_index,
                term: 1
            })
        );
    }
    close(n);
    let store = NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        identity,
        LogLimits::default(),
    )
    .unwrap();
    let state = store.state(group()).unwrap();
    assert_eq!(state.commit_index, 3);
    assert_eq!(
        state.membership().unwrap().id(),
        ConfigurationId::new(11).unwrap()
    );
    let recovered = Raft::recover_member(local, store.binding(), state, store.limits()).unwrap();
    assert_eq!(
        recovered
            .configuration_status(operation)
            .unwrap()
            .resume_action(),
        ConfigurationResumeAction::Completed
    );
    assert_eq!(
        recovered.membership().id(),
        ConfigurationId::new(11).unwrap()
    );
    assert!(recovered.membership().operations().contains(&operation));
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn host_application_and_non_demo_identities_write_and_recover_through_startup() {
    let root = root();
    let mut n = config(root.clone(), NativeOpenMode::Create)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .unwrap();
    n.control(group(), NodeControl::Campaign).unwrap();
    drive(&mut n, |n| {
        n.local().applications[&group()].applied_index() > 0
    });
    n.propose(ClientRequest {
        group: group(),
        operation: OperationId::new(1).unwrap(),
        bytes: 7i64.to_le_bytes().to_vec(),
    })
    .unwrap();
    // Keep a single monotonic domain across the two drive phases.
    let mut reply = None;
    for tick in 5000..10000 {
        n.poll(MonoTime(tick), NodePollBudget::default()).unwrap();
        if let Some(r) = n.poll_client() {
            reply = Some(n.complete_client(r).unwrap());
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(matches!(
        reply,
        Some(ClientOutcome::Applied {
            receipt: CounterReceipt {
                outcome: CounterOutcome::Value(7),
                ..
            },
            ..
        })
    ));
    close(n);
    let n = config(root.clone(), NativeOpenMode::Recover)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .unwrap();
    assert_eq!(
        n.local().applications[&group()]
            .0
            .read_applied(n.local().applications[&group()].applied_index()),
        Ok(7)
    );
    close(n);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn late_constructor_failure_returns_application_and_joins_every_started_worker() {
    let root = root();
    let mut c = config(root.clone(), NativeOpenMode::Create);
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    c.listen = reserve.local_addr().unwrap();
    let address = c.listen;
    drop(reserve);
    c.limits.replica.leases = 0;
    let mut rejected = c
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .err()
        .unwrap();
    assert_eq!(rejected.reason.stage, "node assembly");
    assert!(rejected.application.is_some());
    assert!(std::net::TcpListener::bind(address).is_err());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !rejected.try_cleanup().unwrap() {
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(rejected.try_cleanup().unwrap());
    drop(std::net::TcpListener::bind(address).unwrap());
    let n = config(root.clone(), NativeOpenMode::Recover)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .unwrap();
    close(n);
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn invalid_startup_is_side_effect_free_and_returns_the_host_application() {
    let root = root();
    let mut c = config(root.clone(), NativeOpenMode::Create);
    c.store.incarnation = StoreIncarnation::new(4).unwrap();
    let mut rejected = c
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(0))
        .err()
        .unwrap();
    assert_eq!(rejected.reason.stage, "configuration");
    assert!(rejected.application.is_some());
    assert!(rejected.try_cleanup().unwrap());
    assert!(!root.exists());
    let mut c = config(root.clone(), NativeOpenMode::Create);
    c.peers.insert(
        NodeId::new(8).unwrap(),
        NativeStartupPeer {
            address: "0.0.0.0:1".parse().unwrap(),
            certificate: vec![0],
            server_name: "bad name".into(),
        },
    );
    assert!(c.validate().is_err());
    assert!(!root.exists());
}

#[test]
fn stopping_one_startup_does_not_stop_another_or_the_shared_host_wake() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct HostWake(AtomicUsize);
    impl voteboat::worker::WorkerWake for HostWake {
        fn wake(&self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (a_root, b_root) = (root(), root());
    let wake = Arc::new(HostWake::default());
    let mut a = config(a_root.clone(), NativeOpenMode::Create)
        .open(app(), wake.clone(), MonoTime(0))
        .unwrap();
    let mut b_config = config(b_root.clone(), NativeOpenMode::Create);
    b_config.bootstrap.group.id = GroupId::new(78).unwrap();
    let b_group = b_config.bootstrap.group;
    b_config.store.id = StoreId::new(5678).unwrap();
    b_config
        .bootstrap
        .voter_stores
        .insert(b_config.node, b_config.store);
    let mut b = b_config.open(app(), wake.clone(), MonoTime(0)).unwrap();
    a.control(group(), NodeControl::Campaign).unwrap();
    b.control(b_group, NodeControl::Campaign).unwrap();
    drive(&mut a, |n| {
        n.local().applications[&group()].applied_index() > 0
    });
    drive(&mut b, |n| {
        n.local().applications[&b_group].applied_index() > 0
    });
    close(a);
    let before = wake.0.load(Ordering::Relaxed);
    b.propose(ClientRequest {
        group: b_group,
        operation: OperationId::new(1).unwrap(),
        bytes: 9i64.to_le_bytes().to_vec(),
    })
    .unwrap();
    let mut reply = None;
    for tick in 5000..10000 {
        b.poll(MonoTime(tick), NodePollBudget::default()).unwrap();
        if let Some(output) = b.poll_client() {
            reply = Some(b.complete_client(output).unwrap());
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(matches!(
        reply,
        Some(ClientOutcome::Applied {
            receipt: CounterReceipt {
                outcome: CounterOutcome::Value(9),
                ..
            },
            ..
        })
    ));
    assert!(wake.0.load(Ordering::Relaxed) > before);
    close(b);
    std::fs::remove_dir_all(a_root).unwrap();
    std::fs::remove_dir_all(b_root).unwrap();
}

#[test]
fn startup_uses_the_hosts_initial_monotonic_time_for_owner_and_deadlines() {
    let root = root();
    let mut n = config(root.clone(), NativeOpenMode::Create)
        .open(app(), Arc::new(ThreadWake::current()), MonoTime(1000))
        .unwrap();
    assert_eq!(
        n.poll(MonoTime(999), NodePollBudget::default()).err(),
        Some(NodeError::TimeWentBack)
    );
    assert_eq!(n.state(), NodeState::Running);
    n.poll(MonoTime(1000), NodePollBudget::default()).unwrap();
    let core = n.local().owner.core(group()).unwrap();
    assert_eq!(core.role(), voteboat::raft::Role::Follower);
    assert_eq!(core.state().hard_state.term, 0);
    assert!(!core.has_pending_dependency());
    close(n);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "quic")]
#[test]
fn quic_startup_rejection_releases_udp_socket_and_joins_started_storage_workers() {
    use voteboat::native::{
        connect::NativePeerProtocol,
        log_store::{FileLogIo, NativeLogStore},
    };
    let root = root();
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let mut selected = config(root.clone(), NativeOpenMode::Create);
    selected.listen = address;
    selected.limits.peers.staged_batches = 0; // Reject after native worker construction.
    let mut rejected = match selected.open_with_protocol(
        NativePeerProtocol::Quic,
        app(),
        Arc::new(ThreadWake::current()),
        MonoTime(0),
    ) {
        Ok(_) => panic!("invalid late node limit accepted"),
        Err(r) => r,
    };
    assert!(rejected.application.is_some());
    assert_eq!(rejected.reason.stage, "node assembly");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !rejected.try_cleanup().unwrap() {
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let _socket = std::net::UdpSocket::bind(address).unwrap();
    let recovered = NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        config(root.clone(), NativeOpenMode::Recover).store,
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(recovered.state(group()).unwrap().commit_index, 0);
    drop(recovered);
    std::fs::remove_dir_all(root).unwrap();
}

fn selected_wire_cluster(protocol: voteboat::native::connect::NativePeerProtocol, version: u16) {
    use voteboat::{native::connect::NativeServiceConnector, raft::Role};
    let directory = root();
    std::fs::create_dir(&directory).unwrap();
    let reservations = (0..3)
        .map(|_| {
            (0..32)
                .find_map(|_| {
                    let tcp = std::net::TcpListener::bind("127.0.0.1:0").ok()?;
                    let udp = std::net::UdpSocket::bind(tcp.local_addr().ok()?).ok()?;
                    Some((tcp, udp))
                })
                .expect("available TCP/UDP endpoint")
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
        voter_stores: stores.clone(),
    };
    let certificate = |n| match n {
        1 => include_bytes!("fixtures/tls/node1.der").as_slice(),
        2 => include_bytes!("fixtures/tls/node2.der").as_slice(),
        _ => include_bytes!("fixtures/tls/node3.der").as_slice(),
    };
    let key = |n| match n {
        1 => include_bytes!("fixtures/tls/node1-key.der").as_slice(),
        2 => include_bytes!("fixtures/tls/node2-key.der").as_slice(),
        _ => include_bytes!("fixtures/tls/node3-key.der").as_slice(),
    };
    let make_config = |n: usize, mode| {
        let mut config = config(directory.join(n.to_string()), mode);
        config.node = NodeId::new(n as u64).unwrap();
        config.store = stores[&config.node];
        config.bootstrap = bootstrap.clone();
        config.listen = addresses[n - 1];
        config.peers = (1..=3)
            .filter(|p| *p != n)
            .map(|p| {
                (
                    NodeId::new(p as u64).unwrap(),
                    NativeStartupPeer {
                        address: addresses[p - 1],
                        certificate: certificate(p).to_vec(),
                        server_name: format!("node{p}.voteboat.test"),
                    },
                )
            })
            .collect();
        config.entropy_seed += n as u64;
        config.tls = NativeTlsConfig::new(TlsCredentials {
            roots: vec![include_bytes!("fixtures/tls/ca.der").to_vec()],
            certificate_chain: vec![certificate(n).to_vec()],
            private_key: key(n).to_vec(),
        })
        .unwrap()
        .with_wire_version(version)
        .unwrap();
        config
    };
    // Release every placeholder before any real peer can connect to it.
    drop(reservations);
    let open = |mode| {
        (1..=3)
            .map(|n| {
                make_config(n, mode)
                    .open_with_protocol(
                        protocol,
                        app(),
                        Arc::new(ThreadWake::current()),
                        MonoTime(0),
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>()
    };
    let mut nodes = open(NativeOpenMode::Create);
    for node in &nodes {
        assert_eq!(node.peers().unwrap().roster().wire_version(), version);
        assert_eq!(node.peers().unwrap().admission_routes().unwrap().len(), 2);
        assert_eq!(
            node.local()
                .owner
                .connection_budget()
                .unwrap()
                .provisioned_peers()
                .unwrap()
                .count(),
            2
        );
    }
    nodes[0].control(group(), NodeControl::Campaign).unwrap();
    let clock = Instant::now();
    loop {
        for node in &mut nodes {
            node.poll(
                MonoTime(clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        }
        if nodes
            .iter()
            .all(|n| n.local().applications[&group()].applied_index() > 0)
        {
            break;
        }
        assert!(clock.elapsed() < Duration::from_secs(10));
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(
        nodes[0].local().owner.core(group()).unwrap().role(),
        Role::Leader
    );
    // Both native connectors attest exact provisioned stores. Reconciliation
    // of an unchanged shared roster must preserve its active bindings.
    for (index, node) in nodes.iter_mut().enumerate() {
        use voteboat::connect::ConnectDirection;
        let before = stores
            .keys()
            .filter_map(|peer| {
                node.peers()
                    .unwrap()
                    .roster()
                    .binding(*peer)
                    .map(|binding| (*peer, binding))
            })
            .collect::<BTreeMap<_, _>>();
        let routes = (0..3)
            .filter(|peer| *peer != index)
            .map(|peer| {
                (
                    NodeId::new(peer as u64 + 1).unwrap(),
                    if index < peer {
                        ConnectDirection::Dial(addresses[peer])
                    } else {
                        ConnectDirection::Accept
                    },
                )
            })
            .collect();
        node.reconcile_membership(routes, MonoTime(clock.elapsed().as_millis() as u64))
            .unwrap_or_else(|r| panic!("{:?}", r.reason));
        let mut retained = node.peers().unwrap().admission_routes().unwrap().clone();
        let peer = *retained.keys().next().unwrap();
        let store = retained[&peer].store;
        retained.get_mut(&peer).unwrap().store.id = StoreId::new(999).unwrap();
        let rejected = node
            .set_admission_routes(retained, MonoTime(clock.elapsed().as_millis() as u64))
            .err()
            .unwrap();
        assert_eq!(
            rejected.reason,
            voteboat::runtime::PeerDriverError::WrongBinding
        );
        let mut retained = rejected.routes;
        retained.get_mut(&peer).unwrap().store = store;
        node.set_admission_routes(retained, MonoTime(clock.elapsed().as_millis() as u64))
            .unwrap_or_else(|r| panic!("{:?}", r.reason));
        for (peer, binding) in before {
            assert_eq!(node.peers().unwrap().roster().binding(peer), Some(binding));
        }
    }
    nodes[0]
        .propose(ClientRequest {
            group: group(),
            operation: OperationId::new(88).unwrap(),
            bytes: 9i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    let mut completed = false;
    loop {
        for node in &mut nodes {
            node.poll(
                MonoTime(clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        }
        if let Some(reply) = nodes[0].poll_client() {
            assert!(matches!(
                nodes[0].complete_client(reply).unwrap(),
                ClientOutcome::Applied {
                    receipt: CounterReceipt {
                        outcome: CounterOutcome::Value(9),
                        ..
                    },
                    ..
                }
            ));
            completed = true;
        }
        if completed
            && nodes
                .iter()
                .all(|n| n.local().applications[&group()].applied_index() >= 2)
        {
            break;
        }
        assert!(clock.elapsed() < Duration::from_secs(10));
        std::thread::park_timeout(Duration::from_millis(1));
    }
    fn drain(mut nodes: Vec<NativeNode<HostApplication, NativeServiceConnector>>) {
        for node in &mut nodes {
            node.begin_shutdown();
        }
        let clock = Instant::now();
        loop {
            for node in &mut nodes {
                node.poll(
                    MonoTime(10000 + clock.elapsed().as_millis() as u64),
                    NodePollBudget::default(),
                )
                .unwrap();
            }
            if nodes.iter().all(|n| n.is_drained()) {
                break;
            }
            assert!(clock.elapsed() < Duration::from_secs(10));
            std::thread::park_timeout(Duration::from_millis(1));
        }
        for node in nodes {
            let mut parts = node.into_parts().unwrap_or_else(|_| panic!("not drained"));
            let mut dialer = parts
                .peers
                .take()
                .unwrap()
                .connector
                .into_dialer()
                .unwrap_or_else(|_| panic!("connector not drained"));
            let mut snapshots = parts.local.snapshots.take().unwrap();
            let (mut log_done, mut snapshot_done) = (false, false);
            loop {
                let dial_done = dialer
                    .as_mut()
                    .is_none_or(|dialer| dialer.try_finish().unwrap());
                if !log_done {
                    log_done = parts.local.persistence.try_reclaim().unwrap().is_some();
                }
                if !snapshot_done {
                    snapshot_done = snapshots.worker.try_reclaim().unwrap().is_some();
                }
                if dial_done && log_done && snapshot_done {
                    break;
                }
                assert!(clock.elapsed() < Duration::from_secs(10));
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
    drain(nodes);
    let nodes = open(NativeOpenMode::Recover);
    for node in &nodes {
        assert_eq!(node.peers().unwrap().roster().wire_version(), version);
        let application = &node.local().applications[&group()];
        assert_eq!(
            application.0.read_applied(application.applied_index()),
            Ok(9)
        );
    }
    drain(nodes);
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn tcp_startup_selects_matching_wire_codec_roster_and_sessions_for_versions_two_three_and_four() {
    for version in [2, 3, 4] {
        selected_wire_cluster(
            voteboat::native::connect::NativePeerProtocol::TcpTls,
            version,
        );
    }
}
#[cfg(feature = "quic")]
#[test]
fn quic_startup_selects_matching_wire_codec_roster_and_sessions_for_versions_two_three_and_four() {
    for version in [2, 3, 4] {
        selected_wire_cluster(voteboat::native::connect::NativePeerProtocol::Quic, version);
    }
}

#[test]
fn explicit_startup_timers_validate_before_files_and_use_host_deadlines() {
    use voteboat::native::connect::NativePeerProtocol;
    let protocols = [
        NativePeerProtocol::TcpTls,
        #[cfg(feature = "quic")]
        NativePeerProtocol::Quic,
    ];
    for protocol in protocols {
        let timers = TimerConfig {
            heartbeat_ms: 50,
            election_min_ms: 1000,
            election_spread_ms: 1000,
            expirations_per_poll: 32,
        };
        let mut invalid = Vec::new();
        for variant in 0..5 {
            let mut t = timers;
            match variant {
                0 => t.heartbeat_ms = 0,
                1 => t.election_min_ms = 50,
                2 => t.election_spread_ms = 0,
                3 => t.expirations_per_poll = 0,
                _ => t.expirations_per_poll = 65537,
            }
            invalid.push((t, MonoTime(1000)));
        }
        invalid.push((timers, MonoTime(u64::MAX - 100)));
        for (t, now) in invalid {
            let root = root();
            let mut rejected = config(root.clone(), NativeOpenMode::Create)
                .open_with_protocol_and_timers(
                    protocol,
                    t,
                    app(),
                    Arc::new(ThreadWake::current()),
                    now,
                )
                .err()
                .unwrap();
            assert!(rejected.application.is_some());
            assert!(rejected.try_cleanup().unwrap());
            assert!(!root.exists());
        }
        let root = root();
        let n = config(root.clone(), NativeOpenMode::Create)
            .open_with_protocol_and_timers(
                protocol,
                timers,
                app(),
                Arc::new(ThreadWake::current()),
                MonoTime(1000),
            )
            .unwrap();
        let token = n.local().owner.deadline(group()).unwrap();
        assert_eq!(token.kind, TimerKind::Election);
        assert!((2000..3000).contains(&token.deadline.0));
        // Consume the common Node shutdown path for either connector type.
        let mut n = n;
        n.begin_shutdown();
        let deadline = Instant::now() + Duration::from_secs(10);
        while !n.is_drained() {
            n.poll(MonoTime(1000), NodePollBudget::default()).unwrap();
            assert!(Instant::now() < deadline);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let mut parts = n.into_parts().unwrap_or_else(|_| panic!("not drained"));
        let mut dialer = parts
            .peers
            .take()
            .unwrap()
            .connector
            .into_dialer()
            .unwrap_or_else(|_| panic!("connector"));
        let mut snapshots = parts.local.snapshots.take().unwrap();
        let (mut log, mut snap) = (false, false);
        loop {
            let dial = match &mut dialer {
                Some(d) => d.try_finish().unwrap(),
                None => true,
            };
            if !log {
                log = parts.local.persistence.try_reclaim().unwrap().is_some();
            }
            if !snap {
                snap = snapshots.worker.try_reclaim().unwrap().is_some();
            }
            if log && snap && dial {
                break;
            }
            assert!(Instant::now() < deadline);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
