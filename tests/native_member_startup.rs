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
    collections::{BTreeMap, BTreeSet},
    net::{SocketAddr, TcpListener, UdpSocket},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use voteboat::{
    application::*,
    contracts::HardState,
    identity::*,
    log::*,
    membership::*,
    native::{connect::*, log_store::*, node::*, snapshot_store::*, startup::*, tls::*, worker::*},
    quorum::*,
    raft::*,
    runtime::*,
    snapshot::*,
};
use voteboat::{
    native::{administration::*, placement::*},
    placement::*,
};

#[path = "native_member_startup/recursive.rs"]
mod recursive;

fn administrative_promotion(protocol: NativePeerProtocol) {
    use voteboat::outbound::OutboundQueue;
    type Service = NativeNode<Counter, NativeServiceConnector>;
    fn drive(
        nodes: &mut [Service],
        plan: &NativeAdministrationPlan,
        clock: Instant,
        mut done: impl FnMut(&mut [Service]) -> bool,
    ) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for n in &mut *nodes {
                let progress = n
                    .poll_with_configuration_authorization(
                        MonoTime(clock.elapsed().as_millis() as u64),
                        NodePollBudget::default(),
                        |core, proposal| {
                            plan.authorize(
                                core.state().bootstrap.group,
                                core.membership(),
                                proposal,
                            )
                        },
                    )
                    .unwrap();
                if let Some(replica) = progress.replica {
                    for step in replica.steps {
                        let transitioned = matches!(
                            n.local()
                                .owner
                                .core(group())
                                .unwrap()
                                .membership()
                                .id()
                                .get(),
                            11 | 12
                        );
                        // Scope changes can reject already queued ingress. No
                        // administrative/application admission or other error is
                        // excused; exact committed outcomes remain mandatory.
                        assert!(
                            step.error.is_none()
                                || (transitioned
                                    && step.error == Some(RaftError::WrongIdentity)
                                    && step.admission.is_none()
                                    && step.operation.is_none()
                                    && step.proposed.is_none()
                                    && step.read.is_none()),
                            "{step:?}"
                        );
                    }
                }
            }
            if done(nodes) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "administrative progress timed out"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn plan(
        intents: Vec<ConfigurationRecord>,
        requirements: ReadinessRequirements,
    ) -> NativeAdministrationPlan {
        NativeAdministrationPlan::new(
            group(),
            NativePlacementAuthorizer::new(
                group(),
                (1..=3)
                    .map(|n| {
                        (
                            node(n),
                            ReplicaPlacement {
                                store: identity(n),
                                domain: FailureDomainId::new(n).unwrap(),
                            },
                        )
                    })
                    .collect(),
                PlacementRequirements {
                    minimum_voting_domains: 2,
                    survive_any_single_domain_loss: false,
                },
            )
            .unwrap(),
            requirements,
            intents,
        )
        .unwrap()
    }
    let directory = root().join(format!("administration-{protocol:?}"));
    std::fs::create_dir_all(&directory).unwrap();
    for local in 1..=3 {
        seed(&directory.join(local.to_string()), local, 1, 1, false);
    }
    let endpoints = [reservation(), reservation(), reservation()];
    let configs = (1..=3)
        .map(|local| {
            let (mut config, _unused) =
                startup(&directory.join(local.to_string()), local, &[1, 2, 3]);
            config.startup.tls = config.startup.tls.with_wire_version(6).unwrap();
            config.startup.listen = endpoints[local as usize - 1].0;
            for (peer, route) in &mut config.startup.peers {
                route.address = endpoints[peer.get() as usize - 1].0;
            }
            config
        })
        .collect::<Vec<_>>();
    drop(endpoints);
    let mut nodes = configs
        .into_iter()
        .map(|c| open(c, protocol).unwrap())
        .collect::<Vec<_>>();
    let requirements = Counter::new(100).unwrap().readiness_requirements();
    let joint = ConfigurationRecord {
        operation: OperationId::new(101).unwrap(),
        expected: cid(10),
        change: ConfigurationChange::Joint {
            id: cid(11),
            next: configuration(12, &[1, 2], &[3]),
        },
    };
    let final_record = ConfigurationRecord {
        operation: joint.operation,
        expected: cid(11),
        change: ConfigurationChange::Final { id: cid(12) },
    };
    let allowed = plan(vec![joint.clone(), final_record.clone()], requirements);
    nodes[0].control(group(), NodeControl::Campaign).unwrap();
    let clock = Instant::now();
    drive(&mut nodes, &allowed, clock, |nodes| {
        nodes[0].local().owner.core(group()).unwrap().role() == Role::Leader
    });
    let first = nodes[0]
        .propose(ClientRequest {
            group: group(),
            operation: OperationId::new(899).unwrap(),
            bytes: 1i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    let mut first_index = None;
    drive(&mut nodes, &allowed, clock, |nodes| {
        while let Some(output) = nodes[0].poll_client() {
            assert_eq!(output.ticket(), first);
            let ClientOutcome::Applied { position, .. } = nodes[0].complete_client(output).unwrap()
            else {
                panic!("initial commit");
            };
            first_index = Some(position.index);
        }
        first_index.is_some_and(|index| {
            nodes
                .iter()
                .all(|n| n.local().applications[&group()].read_applied(index) == Ok(1))
        })
    });
    nodes[0]
        .request_learner_readiness(group(), node(2), requirements)
        .unwrap();
    // Hold application ingress while allowing transport ACKs/flow control. Then
    // let the real learner snapshot worker verify readiness and hold its reply
    // in leader ingress. This works for both TCP and QUIC without starving I/O.
    let deadline = Instant::now() + Duration::from_secs(5);
    let held_ingress = NodePollBudget {
        peers: PeerDriverBudget {
            ingress: 0,
            ..Default::default()
        },
        ..Default::default()
    };
    loop {
        let progress = nodes[0]
            .poll(
                MonoTime(clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        assert!(progress
            .replica
            .unwrap()
            .steps
            .iter()
            .all(|s| s.error.is_none()));
        for index in [1, 2] {
            nodes[index]
                .poll(MonoTime(clock.elapsed().as_millis() as u64), held_ingress)
                .unwrap();
        }
        if nodes[0].local().owner.is_drained() && nodes[0].local().outbound.is_drained() {
            break;
        }
        assert!(Instant::now() < deadline, "readiness request did not send");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut verified = false;
    loop {
        let progress = nodes[1]
            .poll(
                MonoTime(clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        let replica = progress.replica.unwrap();
        assert!(replica.steps.iter().all(|s| s.error.is_none()));
        verified |= replica.snapshot_events > 0;
        for index in [0, 2] {
            nodes[index]
                .poll(MonoTime(clock.elapsed().as_millis() as u64), held_ingress)
                .unwrap();
        }
        if verified && nodes[1].local().owner.is_drained() && nodes[1].local().outbound.is_drained()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "readiness reply did not verify/send"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .ready_learner()
        .is_none());
    assert!(!nodes[0].peers().unwrap().ingress().is_drained());
    let before_cancel = nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .state()
        .clone();
    // Cancellation is queued before the held reply can enter the leader owner.
    // It clears only volatile checks/results, never membership or application data.
    nodes[0].cancel_learner_readiness(group()).unwrap();
    drive(&mut nodes, &allowed, clock, |nodes| {
        nodes[0]
            .local()
            .owner
            .core(group())
            .unwrap()
            .ready_learner()
            .is_none()
    });
    assert_eq!(
        nodes[0].local().owner.core(group()).unwrap().state(),
        &before_cancel
    );
    nodes[0]
        .request_learner_readiness(group(), node(2), requirements)
        .unwrap();
    drive(&mut nodes, &allowed, clock, |nodes| {
        nodes[0]
            .local()
            .owner
            .core(group())
            .unwrap()
            .ready_learner()
            .is_some()
    });
    let ready = nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .ready_learner()
        .unwrap()
        .clone();
    let authenticated = nodes[0]
        .peers()
        .unwrap()
        .roster()
        .binding(node(2))
        .unwrap()
        .peer
        .store;
    let proposal = ConfigurationProposal {
        record: joint,
        requirements,
        readiness: vec![PromotionReadiness {
            ready,
            authenticated,
        }],
    };
    // Admission does not cache authority: replace the selected plan while the
    // request is queued, and require refusal without any durable transition.
    let revoked = plan(vec![final_record], requirements);
    let before = nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .state()
        .clone();
    let denied = nodes[0]
        .configure(ConfigurationRequest {
            group: group(),
            proposal: proposal.clone(),
        })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        nodes[0]
            .poll_with_configuration_authorization(
                MonoTime(clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
                |core, p| revoked.authorize(group(), core.membership(), p),
            )
            .unwrap();
        if let Some(result) = nodes[0].poll_configuration() {
            assert_eq!(result.ticket, denied);
            assert_eq!(
                result.outcome,
                ConfigurationOutcome::NotProposed(
                    ConfigurationProposalError::AuthenticationRequired.into()
                )
            );
            break;
        }
        assert!(Instant::now() < deadline);
    }
    assert_eq!(
        nodes[0].local().owner.core(group()).unwrap().state(),
        &before
    );
    let lost = nodes[0]
        .configure(ConfigurationRequest {
            group: group(),
            proposal,
        })
        .unwrap();
    nodes[0].cancel_configuration(lost).unwrap();
    let cancelled = nodes[0].poll_configuration().unwrap();
    assert_eq!(cancelled.ticket, lost);
    assert_eq!(
        cancelled.outcome,
        ConfigurationOutcome::Unknown(ConfigurationUnknown::CancelledWait)
    );
    // Lost observation is not rollback. Joint must actually commit over the old
    // and new native voter sets before ordinary durable resumption can finalize.
    drive(&mut nodes, &allowed, clock, |nodes| {
        nodes.iter().all(|n| {
            matches!(
                n.configuration_status(group(), OperationId::new(101).unwrap())
                    .unwrap()
                    .resume_action(),
                ConfigurationResumeAction::Finalize(_)
            )
        })
    });
    let ConfigurationResumption::Submitted(final_ticket) = nodes[0]
        .resume_configuration(group(), OperationId::new(101).unwrap(), requirements)
        .unwrap()
    else {
        panic!("resumable committed joint");
    };
    let mut final_receipt = false;
    drive(&mut nodes, &allowed, clock, |nodes| {
        while let Some(result) = nodes[0].poll_configuration() {
            assert_eq!(result.ticket, final_ticket);
            assert!(matches!(result.outcome, ConfigurationOutcome::Committed(_)));
            final_receipt = true;
        }
        final_receipt
            && nodes.iter().all(|n| {
                matches!(
                    n.configuration_status(group(), OperationId::new(101).unwrap())
                        .unwrap()
                        .resume_action(),
                    ConfigurationResumeAction::Completed
                )
            })
    });
    assert!(!nodes[2].local().owner.core(group()).unwrap().local_voter());
    let write = nodes[0]
        .propose(ClientRequest {
            group: group(),
            operation: OperationId::new(900).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    let mut applied = None;
    drive(&mut nodes, &allowed, clock, |nodes| {
        while let Some(output) = nodes[0].poll_client() {
            assert_eq!(output.ticket(), write);
            let ClientOutcome::Applied { position, .. } = nodes[0].complete_client(output).unwrap()
            else {
                panic!("final-view write");
            };
            applied = Some(position.index);
        }
        applied.is_some_and(|index| {
            nodes
                .iter()
                .all(|n| n.local().applications[&group()].read_applied(index) == Ok(8))
        })
    });
    nodes[0].control(group(), NodeControl::Checkpoint).unwrap();
    drive(&mut nodes, &allowed, clock, |nodes| {
        nodes[0]
            .local()
            .owner
            .core(group())
            .unwrap()
            .state()
            .base_index()
            > 0
    });
    for n in &mut nodes {
        n.begin_shutdown();
        assert_eq!(n.cancel_learner_readiness(group()), Err(NodeError::Closed));
    }
    drive(&mut nodes, &allowed, clock, |nodes| {
        nodes.iter().all(|n| n.is_drained())
    });
    for n in nodes {
        close(n);
    }
    for local in 1..=3 {
        let path = directory.join(local.to_string());
        let log = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(local),
            LogLimits::default(),
        )
        .unwrap();
        let mut snapshots = NativeSnapshotStore::recover(
            FileSnapshotIo::open(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: identity(local),
                group: group(),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut app = Counter::new(100).unwrap();
        let (core, _) =
            recover_member_replica(node(local), group(), &log, &mut snapshots, &mut app).unwrap();
        assert_eq!(core.membership().id(), cid(12));
        assert_eq!(core.local_voter(), local != 3);
        assert_eq!(app.read_applied(core.state().commit_index), Ok(8));
        assert!(core.ready_learner().is_none());
        assert!(matches!(
            core.configuration_status(OperationId::new(101).unwrap())
                .unwrap()
                .resume_action(),
            ConfigurationResumeAction::Completed
        ));
    }
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn authorized_native_promotion_lost_receipt_resumption_and_file_recovery_tcp() {
    administrative_promotion(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn authorized_native_promotion_lost_receipt_resumption_and_file_recovery_quic() {
    administrative_promotion(NativePeerProtocol::Quic);
}
fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn identity(n: u64) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(n as u128).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(60).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn cid(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn root() -> PathBuf {
    std::env::temp_dir().join(format!(
        "voteboat-member-startup-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn configuration(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    Configuration::new(
        cid(id),
        Policy::new(
            Tree::Majority(voters.iter().map(|n| Tree::Voter(node(*n))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voters.iter().map(|n| (node(*n), identity(*n))).collect(),
        learners.iter().map(|n| (node(*n), identity(*n))).collect(),
    )
    .unwrap()
}
fn bootstrap() -> Bootstrap {
    let c = configuration(9, &[1, 3], &[]);
    Bootstrap {
        group: group(),
        configuration: c.id(),
        policy: c.policy().clone(),
        voter_stores: c.voter_stores().clone(),
    }
}
fn entries(phase: u64) -> Vec<LogEntry> {
    let records = [
        ConfigurationRecord {
            operation: OperationId::new(100).unwrap(),
            expected: cid(9),
            change: ConfigurationChange::Learners(configuration(10, &[1, 3], &[2])),
        },
        ConfigurationRecord {
            operation: OperationId::new(101).unwrap(),
            expected: cid(10),
            change: ConfigurationChange::Joint {
                id: cid(11),
                next: configuration(12, &[1, 2], &[]),
            },
        },
        ConfigurationRecord {
            operation: OperationId::new(101).unwrap(),
            expected: cid(11),
            change: ConfigurationChange::Final { id: cid(12) },
        },
    ];
    records
        .into_iter()
        .take(phase as usize)
        .enumerate()
        .map(|(i, r)| LogEntry {
            index: i as u64 + 1,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(r)),
        })
        .collect()
}
fn seed(path: &Path, local: u64, phase: u64, commit: u64, compact: bool) {
    let mut log = NativeLogStore::create(
        FileLogIo::create(path).unwrap(),
        identity(local),
        LogLimits::default(),
    )
    .unwrap();
    let tickets = log
        .append_batch(vec![LogMutation::Create(bootstrap())])
        .unwrap();
    log.barrier(&tickets).unwrap();
    let state = log.state(group()).unwrap();
    let mut history = entries(phase);
    if compact {
        history.push(LogEntry {
            index: 4,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(900).unwrap(),
                bytes: 7i64.to_le_bytes().to_vec(),
            },
        });
    }
    let tickets = log
        .append_batch(vec![LogMutation::Update(LogUpdate {
            group: group(),
            expected_revision: state.revision,
            hard_state: HardState {
                term: 1,
                voted_for: None,
            },
            commit_index: if compact { 4 } else { commit },
            suffix: Some(Suffix {
                from: 1,
                entries: history,
            }),
            snapshot: None,
            snapshot_membership: None,
        })])
        .unwrap();
    log.barrier(&tickets).unwrap();
    let mut snapshots = NativeSnapshotStore::create(
        FileSnapshotIo::create(path.join("snapshots")).unwrap(),
        SnapshotIdentity {
            store: identity(local),
            group: group(),
        },
        SnapshotLimits::default(),
    )
    .unwrap();
    if compact {
        let mut app = Counter::new(100).unwrap();
        let (mut core, _) =
            recover_member_replica(node(local), group(), &log, &mut snapshots, &mut app).unwrap();
        let receipt = checkpoint_application(&core, &app, &mut snapshots).unwrap();
        compact_replica(
            &mut core,
            &mut log,
            &mut snapshots,
            &app,
            receipt.reference(),
        )
        .unwrap();
        assert!(log.state(group()).unwrap().entries.is_empty());
    }
}
static ENDPOINT_PORTS: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
fn reservation() -> (SocketAddr, TcpListener, UdpSocket) {
    // Opening native providers releases these probes before binding. Never let
    // another fixture choose a released address during that handoff or reuse it
    // while accepted sockets from an earlier fixture are still closing.
    let mut ports = ENDPOINT_PORTS.lock().unwrap_or_else(|e| e.into_inner());
    (0..32)
        .find_map(|_| {
            let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
            let address = tcp.local_addr().ok()?;
            if ports.contains(&address.port()) {
                return None;
            }
            let udp = UdpSocket::bind(address).ok()?;
            ports.insert(address.port());
            Some((address, tcp, udp))
        })
        .expect("TCP/UDP endpoint")
}
fn certificate(n: u64) -> Vec<u8> {
    match n {
        1 => include_bytes!("fixtures/tls/node1.der").to_vec(),
        2 => include_bytes!("fixtures/tls/node2.der").to_vec(),
        _ => include_bytes!("fixtures/tls/node3.der").to_vec(),
    }
}
fn startup(
    path: &Path,
    local: u64,
    provisioned: &[u64],
) -> (
    NativeMemberStartup,
    Vec<(SocketAddr, TcpListener, UdpSocket)>,
) {
    let (listen, tcp, udp) = reservation();
    drop(tcp);
    drop(udp);
    let peers = provisioned
        .iter()
        .filter(|n| **n != local)
        .map(|n| (*n, reservation()))
        .collect::<BTreeMap<_, _>>();
    let config = NativeStartup {
        directory: path.into(),
        mode: NativeOpenMode::Recover,
        node: node(local),
        store: identity(local),
        bootstrap: bootstrap(),
        listen,
        peers: peers
            .iter()
            .map(|(n, (address, _, _))| {
                (
                    node(*n),
                    NativeStartupPeer {
                        address: *address,
                        certificate: certificate(*n),
                        server_name: format!("node{n}.voteboat.test"),
                    },
                )
            })
            .collect(),
        tls: NativeTlsConfig::new(TlsCredentials {
            roots: vec![include_bytes!("fixtures/tls/ca.der").to_vec()],
            certificate_chain: vec![certificate(local)],
            private_key: match local {
                1 => include_bytes!("fixtures/tls/node1-key.der").to_vec(),
                2 => include_bytes!("fixtures/tls/node2-key.der").to_vec(),
                _ => include_bytes!("fixtures/tls/node3-key.der").to_vec(),
            },
        })
        .unwrap()
        .with_wire_version(4)
        .unwrap(),
        entropy_seed: 60 + local,
        limits: NodeLimits::default(),
    };
    (
        NativeMemberStartup {
            startup: config,
            provisioned_stores: provisioned
                .iter()
                .map(|n| (node(*n), identity(*n)))
                .collect(),
        },
        peers.into_values().collect(),
    )
}
fn open(
    config: NativeMemberStartup,
    protocol: NativePeerProtocol,
) -> Result<NativeNode<Counter, NativeServiceConnector>, Box<NativeStartupRejected<Counter>>> {
    let opened = config.open_with_protocol_and_timers(
        protocol,
        TimerConfig {
            heartbeat_ms: 50,
            election_min_ms: 1000,
            election_spread_ms: 1000,
            expirations_per_poll: 32,
        },
        Counter::new(100).unwrap(),
        Arc::new(ThreadWake::current()),
        MonoTime(0),
    );
    if let Ok(n) = &opened {
        if let Some(token) = n.local().owner.deadline(group()) {
            assert_eq!(token.kind, TimerKind::Election);
            assert!((1000..2000).contains(&token.deadline.0));
        }
    }
    opened
}
fn cleanup(mut rejected: Box<NativeStartupRejected<Counter>>) {
    assert!(rejected.application.is_some());
    let deadline = Instant::now() + Duration::from_secs(5);
    while !rejected.try_cleanup().unwrap() {
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn close(mut n: NativeNode<Counter, NativeServiceConnector>) {
    n.begin_shutdown();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !n.is_drained() {
        n.poll(MonoTime(10000), NodePollBudget::default()).unwrap();
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut parts = n.into_parts().unwrap_or_else(|_| panic!("drained"));
    let mut dialer = parts
        .peers
        .take()
        .unwrap()
        .connector
        .into_dialer()
        .unwrap_or_else(|_| panic!("connector drained"));
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
        if dial && log && snap {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn member_histories(protocol: NativePeerProtocol) {
    for (phase, commit, local, compact) in [
        (1, 1, 2, false),
        (2, 2, 2, false),
        (3, 2, 2, false),
        (3, 3, 2, false),
        (3, 3, 2, true),
    ] {
        let directory = root();
        seed(&directory, local, phase, commit, compact);
        let (selected, _peers) = startup(&directory, local, &[1, 2, 3]);
        assert!(selected.startup.validate().is_err()); // Local 2 was not a bootstrap voter.
        let mut n = open(selected, protocol).unwrap();
        let core = n.local().owner.core(group()).unwrap();
        assert_eq!(core.state().commit_index, if compact { 4 } else { commit });
        assert_eq!(core.local_voter(), phase >= 2);
        let active = n
            .peers()
            .unwrap()
            .roster()
            .authorized_peers()
            .collect::<Vec<_>>();
        assert_eq!(
            active,
            if commit < 3 {
                vec![node(1), node(3)]
            } else {
                vec![node(1)]
            }
        );
        assert_eq!(n.peers().unwrap().admission_routes().unwrap().len(), 2);
        if phase == 2 {
            assert!(
                matches!(n.configuration_status(group(), OperationId::new(101).unwrap()).unwrap().resume_action(),
                ConfigurationResumeAction::Finalize(ConfigurationRecord { expected, .. }) if expected == cid(11))
            );
        }
        if phase == 3 && commit == 2 {
            assert_eq!(
                n.resume_configuration(
                    group(),
                    OperationId::new(101).unwrap(),
                    ReadinessRequirements {
                        application_schema: 1,
                        command_bytes: 8,
                        snapshot_bytes: 4096,
                    }
                )
                .unwrap(),
                ConfigurationResumption::WaitForCommit
            );
        }
        if compact {
            assert_eq!(n.local().applications[&group()].read_applied(4).unwrap(), 7);
            let mut retry = n.local().applications[&group()].clone();
            retry
                .apply_batch(&[LogEntry {
                    index: 5,
                    term: 1,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(900).unwrap(),
                        bytes: 7i64.to_le_bytes().to_vec(),
                    },
                }])
                .unwrap();
            assert_eq!(retry.read_applied(5).unwrap(), 7);
            assert!(matches!(
                n.configuration_status(group(), OperationId::new(101).unwrap())
                    .unwrap()
                    .committed,
                ConfigurationProgress::CompactedCompleted { through: 4 }
            ));
        }
        if phase == 1 {
            n.control(group(), NodeControl::Campaign).unwrap();
            let progress = n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
            assert!(progress
                .replica
                .unwrap()
                .steps
                .iter()
                .any(|s| s.error == Some(RaftError::NotVoter)));
            assert_eq!(
                n.local().owner.core(group()).unwrap().role(),
                Role::Follower
            );
            assert_eq!(
                n.local()
                    .owner
                    .core(group())
                    .unwrap()
                    .state()
                    .hard_state
                    .term,
                1
            );
        }
        close(n);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
fn enrollment_history(protocol: NativePeerProtocol) {
    let directory = root();
    let (mut selected, _peers) = startup(&directory, 2, &[1, 2, 3]);
    selected.startup.mode = NativeOpenMode::Create;
    let mut history = entries(1);
    history.push(LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(900).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec(),
        },
    });
    let mut source = Counter::new(100).unwrap();
    source.apply_batch(&history).unwrap();
    let image = Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: bootstrap(),
            membership: Some(Box::new(
                Membership::replay(&bootstrap(), &history, 2).unwrap(),
            )),
            index: 2,
            term: 1,
            application_schema: 1,
        },
        application: source.checkpoint(4096).unwrap(),
    };
    let mut bad = image.clone();
    bad.metadata.application_schema = 2;
    assert!(selected
        .enroll_snapshot(&bad, &mut Counter::new(100).unwrap())
        .is_err());
    assert!(!directory.exists());
    let mut app = Counter::new(100).unwrap();
    selected.enroll_snapshot(&image, &mut app).unwrap();
    assert_eq!(app.read_applied(2).unwrap(), 7);
    selected.startup.mode = NativeOpenMode::Recover;
    selected
        .enroll_snapshot(&image, &mut Counter::new(100).unwrap())
        .unwrap();
    let n = open(selected, protocol).unwrap();
    assert!(!n.local().owner.core(group()).unwrap().local_voter());
    assert_eq!(n.local().applications[&group()].read_applied(2).unwrap(), 7);
    close(n);
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn native_snapshot_enrollment_retries_and_reopens_tcp_learner() {
    enrollment_history(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn native_snapshot_enrollment_retries_and_reopens_quic_learner() {
    enrollment_history(NativePeerProtocol::Quic);
}
#[test]
fn tcp_member_startup_recovers_learner_joint_final_and_checkpoint() {
    member_histories(NativePeerProtocol::TcpTls);
}

fn remote_joint_repair(
    protocol: NativePeerProtocol,
    missing_entries: u64,
    checkpoint_mode: Option<bool>,
    recursive: bool,
) {
    remote_joint_repair_with_tail(protocol, missing_entries, checkpoint_mode, recursive, false);
}
fn remote_joint_repair_with_tail(
    protocol: NativePeerProtocol,
    missing_entries: u64,
    checkpoint_mode: Option<bool>,
    recursive: bool,
    divergent: bool,
) {
    remote_joint_repair_schedule(
        protocol,
        missing_entries,
        checkpoint_mode,
        recursive,
        divergent,
        false,
    );
}
fn remote_joint_repair_schedule(
    protocol: NativePeerProtocol,
    missing_entries: u64,
    checkpoint_mode: Option<bool>,
    recursive: bool,
    divergent: bool,
    restart_repaired: bool,
) {
    assert!(!restart_repaired || (recursive && checkpoint_mode.is_none()));
    let root = root();
    std::fs::create_dir(&root).unwrap();
    let policy = |required| {
        Policy::new(
            Tree::Weighted(
                [1, 3]
                    .into_iter()
                    .map(|id| WeightedChild {
                        weight: if id == required { 3 } else { 1 },
                        node: Tree::Voter(node(id)),
                    })
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap()
    };
    let mut initial = bootstrap();
    initial.policy = if recursive {
        Policy::new(
            Tree::Majority(vec![
                Tree::Voter(node(1)),
                Tree::Majority(vec![Tree::Voter(node(3))]),
            ]),
            Limits::default(),
        )
        .unwrap()
    } else {
        policy(3)
    };
    let learners = Configuration::new(
        cid(10),
        initial.policy.clone(),
        initial.voter_stores.clone(),
        [(node(2), identity(2))].into(),
    )
    .unwrap();
    let target_policy = if recursive {
        Policy::new(
            Tree::Majority(vec![
                Tree::Voter(node(2)),
                Tree::Majority(vec![Tree::Voter(node(3))]),
            ]),
            Limits::default(),
        )
        .unwrap()
    } else {
        Policy::new(
            Tree::Weighted(
                [2, 3]
                    .into_iter()
                    .map(|id| WeightedChild {
                        weight: if id == 2 { 3 } else { 1 },
                        node: Tree::Voter(node(id)),
                    })
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap()
    };
    let target = Configuration::new(
        cid(12),
        target_policy,
        [(node(2), identity(2)), (node(3), identity(3))].into(),
        BTreeMap::new(),
    )
    .unwrap();
    let mut history = [
        ConfigurationRecord {
            operation: OperationId::new(100).unwrap(),
            expected: cid(9),
            change: ConfigurationChange::Learners(learners),
        },
        ConfigurationRecord {
            operation: OperationId::new(101).unwrap(),
            expected: cid(10),
            change: ConfigurationChange::Joint {
                id: cid(11),
                next: target,
            },
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(index, record)| LogEntry {
        index: index as u64 + 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(record)),
    })
    .collect::<Vec<_>>();
    let mut joint = history.pop().unwrap();
    for index in 2..2 + missing_entries {
        history.push(LogEntry {
            index,
            term: 1,
            payload: EntryPayload::Noop,
        });
    }
    joint.index += missing_entries;
    history.push(joint);
    if recursive {
        history.push(LogEntry {
            index: history.len() as u64 + 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(901).unwrap(),
                bytes: 11i64.to_le_bytes().to_vec(),
            },
        });
    }
    if divergent {
        assert!(checkpoint_mode.is_none() && missing_entries > 63);
        for entry in history.iter_mut().skip(1) {
            entry.term = 3;
        }
    }
    let expected_value = if recursive { 18 } else { 7 };
    let locals: &[u64] = if recursive { &[1, 2, 3] } else { &[2, 3] };
    let election_commit = history.len() as u64 + 1;
    for &local in locals {
        let count = if local == 3 { history.len() } else { 1 };
        let path = root.join(local.to_string());
        let mut log = NativeLogStore::create(
            FileLogIo::create(&path).unwrap(),
            identity(local),
            LogLimits::default(),
        )
        .unwrap();
        let tickets = log
            .append_batch(vec![LogMutation::Create(initial.clone())])
            .unwrap();
        log.barrier(&tickets).unwrap();
        let state = log.state(group()).unwrap();
        let mut retained = history[..count].to_vec();
        if divergent && local == 2 {
            // An abandoned term-2 command suffix is durable but uncommitted,
            // and extends beyond the candidate's term-3 joint record.
            for index in 2..=missing_entries + 40 {
                retained.push(LogEntry {
                    index,
                    term: 2,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(6000 + u128::from(index)).unwrap(),
                        bytes: 99i64.to_le_bytes().to_vec(),
                    },
                });
            }
        }
        let tickets = log
            .append_batch(vec![LogMutation::Update(LogUpdate {
                group: group(),
                expected_revision: state.revision,
                hard_state: HardState {
                    term: if divergent { 3 } else { 1 },
                    voted_for: None,
                },
                commit_index: if local == 3 && checkpoint_mode.is_some() {
                    history.len() as u64 - u64::from(checkpoint_mode == Some(false))
                } else {
                    1
                },
                suffix: Some(Suffix {
                    from: 1,
                    entries: retained,
                }),
                snapshot: None,
                snapshot_membership: None,
            })])
            .unwrap();
        log.barrier(&tickets).unwrap();
        let mut snapshots = NativeSnapshotStore::create(
            FileSnapshotIo::create(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: identity(local),
                group: group(),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
        if local == 3 && checkpoint_mode.is_some() {
            let mut app = Counter::new(100).unwrap();
            let (mut core, _) =
                recover_member_replica(node(local), group(), &log, &mut snapshots, &mut app)
                    .unwrap();
            let receipt = checkpoint_application(&core, &app, &mut snapshots).unwrap();
            compact_replica(
                &mut core,
                &mut log,
                &mut snapshots,
                &app,
                receipt.reference(),
            )
            .unwrap();
        }
    }
    let endpoint1 = recursive.then(reservation);
    let endpoint2 = reservation();
    let endpoint3 = reservation();
    let (mut learner, _hints2) = startup(&root.join("2"), 2, &[1, 2, 3]);
    let (mut candidate, _hints3) = startup(&root.join("3"), 3, &[1, 2, 3]);
    learner.startup.bootstrap = initial.clone();
    candidate.startup.bootstrap = initial.clone();
    if checkpoint_mode.is_some() || missing_entries > 63 {
        let version = if checkpoint_mode.is_some() { 6 } else { 5 };
        learner.startup.tls = learner.startup.tls.with_wire_version(version).unwrap();
        candidate.startup.tls = candidate.startup.tls.with_wire_version(version).unwrap();
    }
    learner.startup.listen = endpoint2.0;
    candidate.startup.listen = endpoint3.0;
    learner.startup.peers.get_mut(&node(3)).unwrap().address = endpoint3.0;
    candidate.startup.peers.get_mut(&node(2)).unwrap().address = endpoint2.0;
    let mut old_config = endpoint1.as_ref().map(|endpoint| {
        let (mut c, _hints) = startup(&root.join("1"), 1, &[1, 2, 3]);
        c.startup.bootstrap = initial;
        c.startup.tls = c.startup.tls.with_wire_version(6).unwrap();
        c.startup.listen = endpoint.0;
        c.startup.peers.get_mut(&node(2)).unwrap().address = endpoint2.0;
        c.startup.peers.get_mut(&node(3)).unwrap().address = endpoint3.0;
        learner.startup.peers.get_mut(&node(1)).unwrap().address = endpoint.0;
        candidate.startup.peers.get_mut(&node(1)).unwrap().address = endpoint.0;
        c
    });
    if recursive {
        learner.startup.tls = learner.startup.tls.with_wire_version(6).unwrap();
        candidate.startup.tls = candidate.startup.tls.with_wire_version(6).unwrap();
    }
    let restart_addresses = endpoint1.as_ref().map(|e| [e.0, endpoint2.0, endpoint3.0]);
    let restart_bootstrap = learner.startup.bootstrap.clone();
    drop(endpoint1);
    drop(endpoint2);
    drop(endpoint3);
    let mut old_voter = if restart_repaired {
        None
    } else {
        old_config.take().map(|c| open(c, protocol).unwrap())
    };
    let mut learner = open(learner, protocol).unwrap();
    let mut candidate = open(candidate, protocol).unwrap();
    assert!(!learner.local().owner.core(group()).unwrap().local_voter());
    if restart_repaired {
        // Without the old required voter, repairing accepted membership cannot
        // supply the missing old quorum or commit any speculative application.
        let mut survivors = vec![learner, candidate];
        let clock = Instant::now();
        survivors[1]
            .control(group(), NodeControl::Campaign)
            .unwrap();
        recursive::drive(&mut survivors, &clock, |ns| {
            let core = ns[0].local().owner.core(group()).unwrap();
            core.membership().id() == cid(11)
                && core.state().last_index() >= missing_entries + 2
                && !core.has_pending_dependency()
        });
        for n in &survivors {
            let core = n.local().owner.core(group()).unwrap();
            assert_eq!(core.state().commit_index, 1);
            assert_ne!(core.role(), Role::Leader);
            assert_eq!(n.local().applications[&group()].read_applied(1), Ok(0));
        }
        recursive::shutdown(survivors, &clock);
        let reopen = |local: u64| {
            let (mut selected, _hints) = startup(&root.join(local.to_string()), local, &[1, 2, 3]);
            let addresses = restart_addresses.unwrap();
            selected.startup.bootstrap = restart_bootstrap.clone();
            selected.startup.listen = addresses[local as usize - 1];
            selected.startup.tls = selected.startup.tls.with_wire_version(6).unwrap();
            for (peer, hint) in &mut selected.startup.peers {
                hint.address = addresses[peer.get() as usize - 1];
            }
            open(selected, protocol).unwrap()
        };
        learner = reopen(2);
        candidate = reopen(3);
        for n in [&learner, &candidate] {
            assert_eq!(
                n.local().owner.core(group()).unwrap().membership().id(),
                cid(11)
            );
            assert_eq!(
                n.local().owner.core(group()).unwrap().state().commit_index,
                1
            );
            assert_eq!(n.local().applications[&group()].read_applied(1), Ok(0));
        }
        old_voter = old_config.take().map(|c| open(c, protocol).unwrap());
    }
    if recursive {
        candidate.control(group(), NodeControl::Campaign).unwrap();
    }
    let start = Instant::now();
    let mut ticket = None;
    let mut applied_index = None;
    loop {
        let now = MonoTime(start.elapsed().as_millis() as u64);
        for n in old_voter.iter_mut().chain([&mut learner, &mut candidate]) {
            let p = n.poll(now, NodePollBudget::default()).unwrap();
            if let Some(replica) = p.replica {
                for step in replica.steps {
                    // Competing campaigns repeat repair after promotion and
                    // request ballots from older views that cannot authorize
                    // the promoted candidate. These ingress refusals are
                    // expected; the history still requires eventual election,
                    // exact commitment/application and native-file recovery.
                    let ingress_refusal = recursive
                        && matches!(
                            step.error,
                            Some(RaftError::InvalidMessage | RaftError::WrongIdentity)
                        );
                    assert!(
                        step.error.is_none() || ingress_refusal,
                        "protocol={protocol:?} step={step:?}"
                    );
                }
            }
        }
        let core = candidate.local().owner.core(group()).unwrap();
        if core.role() == Role::Leader
            && core.state().commit_index >= election_commit
            && ticket.is_none()
        {
            ticket = Some(
                candidate
                    .propose(ClientRequest {
                        group: group(),
                        operation: OperationId::new(900).unwrap(),
                        bytes: 7i64.to_le_bytes().to_vec(),
                    })
                    .unwrap_or_else(|r| panic!("{:?}", r.reason)),
            );
        }
        while let Some(output) = candidate.poll_client() {
            assert_eq!(Some(output.ticket()), ticket);
            let outcome = candidate
                .complete_client(output)
                .unwrap_or_else(|r| panic!("{:?}", r.reason));
            match outcome {
                ClientOutcome::Applied { position, .. } => applied_index = Some(position.index),
                other => panic!("{other:?}"),
            }
        }
        if applied_index.is_some_and(|index| {
            learner.local().applications[&group()].read_applied(index) == Ok(expected_value)
                && old_voter.as_ref().is_none_or(|v| {
                    v.local().applications[&group()].read_applied(index) == Ok(expected_value)
                })
        }) {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(15),
            "protocol={protocol:?} source={:?} learner={:?} source_failure={:?} learner_failure={:?}",
            candidate.local().owner.core(group()).unwrap().state(), learner.local().owner.core(group()).unwrap().state(),
            candidate.failure(), learner.failure());
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(learner.local().owner.core(group()).unwrap().local_voter());
    assert_eq!(
        candidate
            .local()
            .owner
            .core(group())
            .unwrap()
            .membership()
            .id(),
        cid(11)
    );
    // Quiesce the cluster together so one joined node cannot close a peer's
    // connection while that peer still owns admitted reply work.
    for n in old_voter.iter_mut().chain([&mut learner, &mut candidate]) {
        n.begin_shutdown();
    }
    let shutdown = Instant::now();
    while !learner.is_drained()
        || !candidate.is_drained()
        || old_voter.as_ref().is_some_and(|n| !n.is_drained())
    {
        let now = MonoTime(10000 + start.elapsed().as_millis() as u64);
        for n in old_voter.iter_mut().chain([&mut learner, &mut candidate]) {
            n.poll(now, NodePollBudget::default()).unwrap();
        }
        assert!(shutdown.elapsed() < Duration::from_secs(5));
        std::thread::park_timeout(Duration::from_millis(1));
    }
    close(candidate);
    close(learner);
    if let Some(voter) = old_voter {
        close(voter);
    }
    for &local in locals {
        let path = root.join(local.to_string());
        let log = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(local),
            LogLimits::default(),
        )
        .unwrap();
        let mut snapshots = NativeSnapshotStore::recover(
            FileSnapshotIo::open(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: identity(local),
                group: group(),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut app = Counter::new(100).unwrap();
        let (core, _) =
            recover_member_replica(node(local), group(), &log, &mut snapshots, &mut app).unwrap();
        assert!(core.local_voter());
        assert_eq!(core.membership().id(), cid(11));
        assert_eq!(app.read_applied(applied_index.unwrap()), Ok(expected_value));
        if divergent {
            assert!(!log.state(group()).unwrap().entries.iter().any(|entry|
                matches!(&entry.payload, EntryPayload::Command { operation, .. } if operation.get() >= 6000)), "abandoned learner commands must not survive/apply");
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_native_joint_repair_elects_and_commits_after_old_leader_loss() {
    remote_joint_repair(NativePeerProtocol::TcpTls, 0, None, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_native_joint_repair_elects_and_commits_after_old_leader_loss() {
    remote_joint_repair(NativePeerProtocol::Quic, 0, None, false);
}
#[test]
fn tcp_native_joint_repair_catches_up_retained_learner_prefix() {
    remote_joint_repair(NativePeerProtocol::TcpTls, 32, None, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_native_joint_repair_catches_up_retained_learner_prefix() {
    remote_joint_repair(NativePeerProtocol::Quic, 32, None, false);
}
#[test]
fn tcp_native_joint_repair_catches_up_multiple_batches() {
    remote_joint_repair(NativePeerProtocol::TcpTls, 160, None, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_native_joint_repair_catches_up_multiple_batches() {
    remote_joint_repair(NativePeerProtocol::Quic, 160, None, false);
}
#[test]
fn tcp_retained_repair_replaces_divergent_uncommitted_learner_tail() {
    remote_joint_repair_with_tail(NativePeerProtocol::TcpTls, 160, None, false, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_retained_repair_replaces_divergent_uncommitted_learner_tail() {
    remote_joint_repair_with_tail(NativePeerProtocol::Quic, 160, None, false, true);
}
#[test]
fn tcp_recursive_retained_repair_replaces_divergent_learner_tail() {
    remote_joint_repair_with_tail(NativePeerProtocol::TcpTls, 160, None, true, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_recursive_retained_repair_replaces_divergent_learner_tail() {
    remote_joint_repair_with_tail(NativePeerProtocol::Quic, 160, None, true, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_member_startup_recovers_learner_joint_final_and_checkpoint() {
    member_histories(NativePeerProtocol::Quic);
}
#[test]
fn recovery_requires_committed_local_assignment_and_all_rollback_peer_credentials() {
    for (local, phase, commit, provisioned, wrong) in [
        (2, 1, 0, vec![1, 2, 3], false),
        (2, 3, 2, vec![1, 2], false),
        (2, 3, 2, vec![1, 2, 3], true),
        (3, 3, 3, vec![1, 2, 3], false),
    ] {
        let directory = root();
        seed(&directory, local, phase, commit, false);
        let (mut selected, _peers) = startup(&directory, local, &provisioned);
        if wrong {
            selected
                .provisioned_stores
                .get_mut(&node(3))
                .unwrap()
                .incarnation = StoreIncarnation::new(2).unwrap();
        }
        let rejected = open(selected, NativePeerProtocol::TcpTls)
            .err()
            .expect("invalid assignment/provisioning");
        cleanup(rejected);
        // Failed startup released store/snapshot locks; it did not rewrite membership.
        let log = NativeLogStore::recover(
            FileLogIo::open(&directory).unwrap(),
            identity(local),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group()).unwrap().commit_index, commit);
        drop(log);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
#[test]
fn create_and_static_wire_are_rejected_before_filesystem_changes() {
    for create in [true, false] {
        let directory = root();
        let (mut selected, _peers) = startup(&directory, 2, &[1, 2, 3]);
        if create {
            selected.startup.mode = NativeOpenMode::Create;
        } else {
            selected.startup.tls = selected.startup.tls.with_wire_version(1).unwrap();
        }
        assert!(selected.validate().is_err());
        cleanup(
            open(selected, NativePeerProtocol::TcpTls)
                .err()
                .expect("explicit recovery only"),
        );
        assert!(!directory.exists());
    }
}

#[test]
fn missing_checkpoint_data_prevents_member_exposure() {
    let directory = root();
    seed(&directory, 2, 3, 3, true);
    let snapshots = directory.join("snapshots");
    for slot in ["snapshot-0", "snapshot-1"] {
        let path = snapshots.join(slot);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    let (selected, _peers) = startup(&directory, 2, &[1, 2, 3]);
    cleanup(
        open(selected, NativePeerProtocol::TcpTls)
            .err()
            .expect("missing pinned data"),
    );
    std::fs::remove_dir_all(directory).unwrap();
}
fn late_failure(protocol: NativePeerProtocol) {
    let directory = root();
    seed(&directory, 2, 3, 3, false);
    let (mut selected, _peers) = startup(&directory, 2, &[1, 2, 3]);
    let address = selected.startup.listen;
    selected.startup.limits.replica.leases = 0;
    let rejected = open(selected, protocol)
        .err()
        .expect("invalid late node limits");
    assert_eq!(rejected.reason.stage, "node assembly");
    cleanup(rejected);
    let tcp = TcpListener::bind(address).unwrap();
    let udp = UdpSocket::bind(address).unwrap();
    drop(tcp);
    drop(udp);
    let log = NativeLogStore::recover(
        FileLogIo::open(&directory).unwrap(),
        identity(2),
        LogLimits::default(),
    )
    .unwrap();
    assert_eq!(
        log.state(group()).unwrap().membership().unwrap().id(),
        cid(12)
    );
    drop(log);
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn tcp_member_late_failure_joins_workers_and_releases_resources() {
    late_failure(NativePeerProtocol::TcpTls);
}
#[cfg(feature = "quic")]
#[test]
fn quic_member_late_failure_joins_workers_and_releases_resources() {
    late_failure(NativePeerProtocol::Quic);
}

#[test]
fn committed_retirement_releases_old_peer_requirements_without_weakening_static_startup() {
    let directory = root();
    seed(&directory, 1, 3, 3, false);
    let (selected, _peers) = startup(&directory, 1, &[1, 3]);
    assert!(selected.startup.validate().is_ok()); // Exactly the original bootstrap peers.
    cleanup(
        selected
            .startup
            .open_with_protocol(
                NativePeerProtocol::TcpTls,
                Counter::new(100).unwrap(),
                Arc::new(ThreadWake::current()),
                MonoTime(0),
            )
            .err()
            .expect("static startup still rejects dynamic journal"),
    );
    let (selected, _peers) = startup(&directory, 1, &[1, 2]);
    let n = open(selected, NativePeerProtocol::TcpTls).unwrap();
    assert_eq!(
        n.peers()
            .unwrap()
            .roster()
            .authorized_peers()
            .collect::<Vec<_>>(),
        vec![node(2)]
    );
    close(n);
    std::fs::remove_dir_all(directory).unwrap();
}

#[test]
fn tcp_native_snapshot_repair_catches_up_then_installs_joint() {
    remote_joint_repair(NativePeerProtocol::TcpTls, 80, Some(false), false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_native_snapshot_repair_catches_up_then_installs_joint() {
    remote_joint_repair(NativePeerProtocol::Quic, 80, Some(false), false);
}
#[test]
fn tcp_native_snapshot_repair_recovers_compacted_joint() {
    remote_joint_repair(NativePeerProtocol::TcpTls, 80, Some(true), false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_native_snapshot_repair_recovers_compacted_joint() {
    remote_joint_repair(NativePeerProtocol::Quic, 80, Some(true), false);
}

#[test]
fn tcp_recursive_repair_catches_up_old_voter_and_candidate_tail() {
    remote_joint_repair(NativePeerProtocol::TcpTls, 80, None, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_recursive_repair_catches_up_old_voter_and_candidate_tail() {
    remote_joint_repair(NativePeerProtocol::Quic, 80, None, true);
}

fn seed_promoted_witness(
    root: &Path,
    compacted_leader: bool,
    compacted_witness: bool,
) -> Bootstrap {
    let weighted = |voters: &[u64], required| {
        Policy::new(
            Tree::Weighted(
                voters
                    .iter()
                    .map(|id| WeightedChild {
                        weight: if *id == required { 3 } else { 1 },
                        node: Tree::Voter(node(*id)),
                    })
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap()
    };
    let mut initial = bootstrap();
    initial.policy = weighted(&[1, 3], 3);
    let stable = Configuration::new(
        cid(10),
        initial.policy.clone(),
        initial.voter_stores.clone(),
        [(node(2), identity(2))].into(),
    )
    .unwrap();
    let next = Configuration::new(
        cid(12),
        weighted(&[2, 3], 2),
        [(node(2), identity(2)), (node(3), identity(3))].into(),
        [(node(1), identity(1))].into(),
    )
    .unwrap();
    let history: Vec<_> = [
        ConfigurationRecord {
            operation: OperationId::new(100).unwrap(),
            expected: cid(9),
            change: ConfigurationChange::Learners(stable),
        },
        ConfigurationRecord {
            operation: OperationId::new(101).unwrap(),
            expected: cid(10),
            change: ConfigurationChange::Joint { id: cid(11), next },
        },
        ConfigurationRecord {
            operation: OperationId::new(101).unwrap(),
            expected: cid(11),
            change: ConfigurationChange::Final { id: cid(12) },
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(i, record)| LogEntry {
        index: i as u64 + 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(record)),
    })
    .collect();
    for local in 1..=3 {
        let path = root.join(local.to_string());
        let mut log = NativeLogStore::create(
            FileLogIo::create(&path).unwrap(),
            identity(local),
            LogLimits::default(),
        )
        .unwrap();
        let tickets = log
            .append_batch(vec![LogMutation::Create(initial.clone())])
            .unwrap();
        log.barrier(&tickets).unwrap();
        let state = log.state(group()).unwrap();
        let count = if local == 1 { 1 } else { 3 };
        let tickets = log
            .append_batch(vec![LogMutation::Update(LogUpdate {
                group: group(),
                expected_revision: state.revision,
                hard_state: HardState {
                    term: 1,
                    voted_for: None,
                },
                commit_index: count as u64,
                suffix: Some(Suffix {
                    from: 1,
                    entries: history[..count].to_vec(),
                }),
                snapshot: None,
                snapshot_membership: None,
            })])
            .unwrap();
        log.barrier(&tickets).unwrap();
        let mut snapshots = NativeSnapshotStore::create(
            FileSnapshotIo::create(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: identity(local),
                group: group(),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
        // Compact either source independently: a promoted leader can use a
        // separate retained-history witness, but a compacted witness cannot
        // reconstruct the old base needed to authenticate its requester.
        if (local == 2 && compacted_leader) || (local == 3 && compacted_witness) {
            let mut app = Counter::new(100).unwrap();
            let (mut core, _) =
                recover_member_replica(node(local), group(), &log, &mut snapshots, &mut app)
                    .unwrap();
            let reference = checkpoint_application(&core, &app, &mut snapshots)
                .unwrap()
                .reference();
            compact_replica(&mut core, &mut log, &mut snapshots, &app, reference).unwrap();
        }
    }
    initial
}

fn promoted_leader_witness_catchup(protocol: NativePeerProtocol, compacted: bool) {
    use voteboat::secure::PeerIdentity;
    let root = root();
    std::fs::create_dir(&root).unwrap();
    let initial = seed_promoted_witness(&root, compacted, false);
    let endpoints: Vec<_> = (0..3).map(|_| reservation()).collect();
    let configs: Vec<_> = (1..=3)
        .map(|local| {
            let (mut config, _hints) = startup(&root.join(local.to_string()), local, &[1, 2, 3]);
            config.startup.bootstrap = initial.clone();
            config.startup.tls = config.startup.tls.with_wire_version(6).unwrap();
            config.startup.listen = endpoints[local as usize - 1].0;
            for (peer, entry) in &mut config.startup.peers {
                entry.address = endpoints[peer.get() as usize - 1].0;
            }
            config
        })
        .collect();
    drop(endpoints);
    let mut nodes: Vec<_> = configs
        .into_iter()
        .map(|c| open(c, protocol).unwrap())
        .collect();
    let status = |n: &NativeNode<Counter, NativeServiceConnector>| {
        n.local()
            .owner
            .core(group())
            .unwrap()
            .replication_authorization_status()
    };
    let before = nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .state()
        .clone();
    let reset = nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .election_reset_sequence();
    let candidate = PeerIdentity {
        node: node(2),
        store: identity(2),
    };
    let query = NodeControl::AuthorizeReplication {
        witness: PeerIdentity {
            node: node(3),
            store: identity(3),
        },
        candidate,
        configuration: cid(12),
    };
    nodes[0].control(group(), query).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    // Keep virtual time fixed during the control exchange: a query/grant does
    // not establish leadership or cause election-timer resets.
    while status(&nodes[0]) == ReplicationAuthorizationStatus::None {
        nodes[0]
            .poll(MonoTime(0), NodePollBudget::default())
            .unwrap();
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let ReplicationAuthorizationStatus::Pending { context: first, .. } = status(&nodes[0]) else {
        panic!("pending query")
    };
    nodes[0]
        .control(group(), NodeControl::CancelReplicationAuthorization)
        .unwrap();
    while status(&nodes[0]) != ReplicationAuthorizationStatus::None {
        nodes[0]
            .poll(MonoTime(0), NodePollBudget::default())
            .unwrap();
        assert!(Instant::now() < deadline);
    }
    let mut refused_late_grant = false;
    while !refused_late_grant {
        for index in [2, 0] {
            let progress = nodes[index]
                .poll(MonoTime(0), NodePollBudget::default())
                .unwrap();
            if let Some(replica) = progress.replica {
                for step in replica.steps {
                    if index == 0 && step.error == Some(RaftError::WrongIdentity) {
                        refused_late_grant = true;
                    } else {
                        assert!(step.error.is_none(), "{step:?}");
                    }
                }
            }
        }
        assert_eq!(status(&nodes[0]), ReplicationAuthorizationStatus::None);
        assert!(Instant::now() < deadline, "canceled reply was not observed");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    nodes[0].control(group(), query).unwrap();
    while !matches!(
        status(&nodes[0]),
        ReplicationAuthorizationStatus::Pending { .. }
    ) {
        nodes[0]
            .poll(MonoTime(0), NodePollBudget::default())
            .unwrap();
        assert!(Instant::now() < deadline);
    }
    let ReplicationAuthorizationStatus::Pending {
        context: second, ..
    } = status(&nodes[0])
    else {
        unreachable!()
    };
    assert_ne!(first, second);
    while !matches!(
        status(&nodes[0]),
        ReplicationAuthorizationStatus::Granted { .. }
    ) {
        for index in [2, 0] {
            nodes[index]
                .poll(MonoTime(0), NodePollBudget::default())
                .unwrap();
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(
        status(&nodes[0]),
        ReplicationAuthorizationStatus::Granted {
            candidate,
            configuration: cid(12),
            base: cid(10)
        }
    );
    assert_eq!(
        nodes[0].local().owner.core(group()).unwrap().state(),
        &before
    );
    assert_eq!(
        nodes[0]
            .local()
            .owner
            .core(group())
            .unwrap()
            .election_reset_sequence(),
        reset
    );
    nodes[1].control(group(), NodeControl::Campaign).unwrap();
    let clock = Instant::now();
    let mut ticket = None;
    let mut applied = None;
    loop {
        let now = MonoTime(clock.elapsed().as_millis() as u64);
        for n in &mut nodes {
            let progress = n.poll(now, NodePollBudget::default()).unwrap();
            if let Some(replica) = progress.replica {
                for step in replica.steps {
                    assert!(step.error.is_none(), "{step:?}");
                }
            }
        }
        let core = nodes[1].local().owner.core(group()).unwrap();
        if core.role() == Role::Leader && core.state().commit_index >= 4 && ticket.is_none() {
            ticket = Some(
                nodes[1]
                    .propose(ClientRequest {
                        group: group(),
                        operation: OperationId::new(900).unwrap(),
                        bytes: 7i64.to_le_bytes().to_vec(),
                    })
                    .unwrap(),
            );
        }
        while let Some(output) = nodes[1].poll_client() {
            assert_eq!(Some(output.ticket()), ticket);
            let ClientOutcome::Applied { position, .. } = nodes[1].complete_client(output).unwrap()
            else {
                panic!("application outcome")
            };
            applied = Some(position.index);
        }
        if applied.is_some_and(|index| {
            nodes
                .iter()
                .all(|n| n.local().applications[&group()].read_applied(index) == Ok(7))
        }) {
            break;
        }
        assert!(
            clock.elapsed() < Duration::from_secs(10),
            "protocol={protocol:?} compacted={compacted}"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let receiver = nodes[0].local().owner.core(group()).unwrap();
    assert_eq!(receiver.membership().id(), cid(12));
    assert!(!receiver.local_voter());
    assert_eq!(status(&nodes[0]), ReplicationAuthorizationStatus::None);
    assert_eq!(receiver.state().snapshot.is_some(), compacted);
    for n in &mut nodes {
        n.begin_shutdown();
    }
    let shutdown = Instant::now();
    while nodes.iter().any(|n| !n.is_drained()) {
        for n in &mut nodes {
            n.poll(
                MonoTime(10000 + clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        }
        assert!(shutdown.elapsed() < Duration::from_secs(5));
        std::thread::park_timeout(Duration::from_millis(1));
    }
    for n in nodes {
        close(n);
    }
    for local in 1..=3 {
        let path = root.join(local.to_string());
        let log = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(local),
            LogLimits::default(),
        )
        .unwrap();
        let mut snapshots = NativeSnapshotStore::recover(
            FileSnapshotIo::open(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: identity(local),
                group: group(),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut app = Counter::new(100).unwrap();
        let (core, _) =
            recover_member_replica(node(local), group(), &log, &mut snapshots, &mut app).unwrap();
        assert_eq!(core.membership().id(), cid(12));
        assert_eq!(core.local_voter(), local != 1);
        assert_eq!(
            core.replication_authorization_status(),
            ReplicationAuthorizationStatus::None
        );
        assert_eq!(app.read_applied(applied.unwrap()), Ok(7));
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn tcp_promoted_leader_witness_repairs_old_view_after_canceled_grant() {
    promoted_leader_witness_catchup(NativePeerProtocol::TcpTls, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_promoted_leader_witness_repairs_old_view_after_canceled_grant() {
    promoted_leader_witness_catchup(NativePeerProtocol::Quic, false);
}
#[test]
fn tcp_promoted_leader_witness_installs_compacted_final_snapshot() {
    promoted_leader_witness_catchup(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_promoted_leader_witness_installs_compacted_final_snapshot() {
    promoted_leader_witness_catchup(NativePeerProtocol::Quic, true);
}

fn promoted_leader_loss_and_witness_outage(protocol: NativePeerProtocol, compacted_witness: bool) {
    use voteboat::secure::PeerIdentity;
    type Service = NativeNode<Counter, NativeServiceConnector>;
    fn assert_old_view(n: &Service, expected: &GroupLog) {
        let state = n.local().owner.core(group()).unwrap().state();
        // Election retries may durably advance a ballot. They cannot invent
        // membership records, application entries, a snapshot or commitment.
        assert_eq!(state.bootstrap, expected.bootstrap);
        assert_eq!(state.entries, expected.entries);
        assert_eq!(state.commit_index, expected.commit_index);
        assert_eq!(state.snapshot, expected.snapshot);
        assert_eq!(state.snapshot_membership, expected.snapshot_membership);
    }
    fn poll(nodes: &mut [Service], now: u64) -> Vec<usize> {
        let mut refusals = Vec::with_capacity(nodes.len());
        for n in nodes {
            let mut refused = 0;
            let progress = n.poll(MonoTime(now), NodePollBudget::default()).unwrap();
            if let Some(replica) = progress.replica {
                for step in replica.steps {
                    // Old-view ingress and superseded authority replies can be
                    // refused. No application/admin/provider error is excused.
                    assert!(
                        step.error.is_none()
                            || (step.error == Some(RaftError::WrongIdentity)
                                && step.admission.is_none()
                                && step.operation.is_none()
                                && step.proposed.is_none()),
                        "{step:?}"
                    );
                    refused += usize::from(step.error == Some(RaftError::WrongIdentity));
                }
            }
            refusals.push(refused);
        }
        refusals
    }
    fn drive(nodes: &mut [Service], clock: &Instant, mut done: impl FnMut(&mut [Service]) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            poll(nodes, clock.elapsed().as_millis() as u64);
            if done(nodes) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "promoted leader/witness fault timed out"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    fn write(
        nodes: &mut [Service],
        clock: &Instant,
        leader: usize,
        operation: u128,
        delta: i64,
        expected: i64,
    ) -> u64 {
        let ticket = nodes[leader]
            .propose(ClientRequest {
                group: group(),
                operation: OperationId::new(operation).unwrap(),
                bytes: delta.to_le_bytes().to_vec(),
            })
            .unwrap();
        let mut index = None;
        drive(nodes, clock, |ns| {
            while let Some(reply) = ns[leader].poll_client() {
                assert_eq!(reply.ticket(), ticket);
                let ClientOutcome::Applied { position, receipt } =
                    ns[leader].complete_client(reply).unwrap()
                else {
                    panic!("acknowledged write failed")
                };
                assert_eq!(receipt.outcome, CounterOutcome::Value(expected));
                index = Some(position.index);
            }
            index.is_some()
        });
        index.unwrap()
    }
    let clock = Instant::now();
    let root = root();
    std::fs::create_dir(&root).unwrap();
    let initial = seed_promoted_witness(&root, true, compacted_witness);
    let reservations: Vec<_> = (0..3).map(|_| reservation()).collect();
    let addresses = reservations.iter().map(|r| r.0).collect::<Vec<_>>();
    let config = |local: u64| {
        let (mut c, _hints) = startup(&root.join(local.to_string()), local, &[1, 2, 3]);
        c.startup.bootstrap = initial.clone();
        c.startup.tls = c.startup.tls.with_wire_version(6).unwrap();
        c.startup.listen = addresses[local as usize - 1];
        for (peer, hint) in &mut c.startup.peers {
            hint.address = addresses[peer.get() as usize - 1];
        }
        c
    };
    drop(reservations);
    let open_node = |local| {
        config(local)
            .open_with_protocol(
                protocol,
                Counter::new(100).unwrap(),
                Arc::new(ThreadWake::current()),
                MonoTime(clock.elapsed().as_millis() as u64),
            )
            .unwrap()
    };
    // The committed new policy gives node 2 sufficient weight to commit alone.
    // First acknowledge real data, then lose that promoted leader before the
    // old-view replica has learned either the promotion or the data.
    let mut promoted = vec![open_node(2)];
    promoted[0].control(group(), NodeControl::Campaign).unwrap();
    drive(&mut promoted, &clock, |ns| {
        ns[0].local().owner.core(group()).unwrap().role() == Role::Leader
    });
    let acknowledged = write(&mut promoted, &clock, 0, 900, 7, 7);
    close(promoted.remove(0));
    let mut older = vec![open_node(1)];
    older[0].control(group(), NodeControl::Campaign).unwrap();
    drive(&mut older, &clock, |ns| {
        let core = ns[0].local().owner.core(group()).unwrap();
        core.role() == Role::Candidate
            && core.state().hard_state.term == 2
            && core.state().hard_state.voted_for == Some(node(1))
    });
    let request = ClientRequest {
        group: group(),
        operation: OperationId::new(901).unwrap(),
        bytes: 100i64.to_le_bytes().to_vec(),
    };
    let allocation = request.bytes.as_ptr();
    let rejected = older[0].propose(request).unwrap_err();
    assert_eq!(
        rejected.reason,
        ClientError::Consensus(RaftError::NotLeader)
    );
    assert_eq!(rejected.request.group, group());
    assert_eq!(rejected.request.operation, OperationId::new(901).unwrap());
    assert_eq!(rejected.request.bytes, 100i64.to_le_bytes());
    assert_eq!(rejected.request.bytes.as_ptr(), allocation);
    let baseline = older[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .state()
        .clone();
    let candidate = PeerIdentity {
        node: node(2),
        store: identity(2),
    };
    let query = NodeControl::AuthorizeReplication {
        witness: PeerIdentity {
            node: node(3),
            store: identity(3),
        },
        candidate,
        configuration: cid(12),
    };
    let status = |n: &Service| {
        n.local()
            .owner
            .core(group())
            .unwrap()
            .replication_authorization_status()
    };
    older[0].control(group(), query).unwrap();
    drive(&mut older, &clock, |ns| {
        matches!(
            status(&ns[0]),
            ReplicationAuthorizationStatus::Pending { .. }
        )
    });
    let ReplicationAuthorizationStatus::Pending {
        context: unavailable,
        ..
    } = status(&older[0])
    else {
        unreachable!()
    };
    // Witness and promoted leader are both unavailable. Repeated bounded polls
    // cannot turn a routing hint or pending query into authority or a commit.
    for _ in 0..50 {
        poll(&mut older, clock.elapsed().as_millis() as u64);
        assert!(!matches!(
            status(&older[0]),
            ReplicationAuthorizationStatus::Granted { .. }
        ));
        assert_old_view(&older[0], &baseline);
        assert_eq!(
            older[0].local().applications[&group()].read_applied(1),
            Ok(0)
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    older[0]
        .control(group(), NodeControl::CancelReplicationAuthorization)
        .unwrap();
    drive(&mut older, &clock, |ns| {
        status(&ns[0]) == ReplicationAuthorizationStatus::None
    });
    older[0].control(group(), query).unwrap();
    drive(&mut older, &clock, |ns| {
        matches!(
            status(&ns[0]),
            ReplicationAuthorizationStatus::Pending { .. }
        )
    });
    let ReplicationAuthorizationStatus::Pending { context: fresh, .. } = status(&older[0]) else {
        unreachable!()
    };
    assert_ne!(unavailable, fresh);
    // Restore the actual witness store, never a reset/empty replacement. The
    // promoted leader remains down while the witness answers the fresh query.
    older.push(open_node(3));
    if compacted_witness {
        // Missing historical membership prevents authenticating the request;
        // it yields WrongIdentity, not an authenticated negative reply. Poll
        // both actual nodes until the restored witness refuses ingress, then
        // continue transport progress without treating silence as authority.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let refusals = poll(&mut older, clock.elapsed().as_millis() as u64);
            assert!(!matches!(
                status(&older[0]),
                ReplicationAuthorizationStatus::Granted { .. }
            ));
            assert_old_view(&older[0], &baseline);
            if refusals[1] > 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "compacted witness received no refused ingress"
            );
            std::thread::park_timeout(Duration::from_millis(1));
        }
        for _ in 0..50 {
            poll(&mut older, clock.elapsed().as_millis() as u64);
            assert!(!matches!(
                status(&older[0]),
                ReplicationAuthorizationStatus::Granted { .. }
            ));
            assert_old_view(&older[0], &baseline);
            std::thread::park_timeout(Duration::from_millis(1));
        }
        older[0]
            .control(group(), NodeControl::CancelReplicationAuthorization)
            .unwrap();
        drive(&mut older, &clock, |ns| {
            status(&ns[0]) == ReplicationAuthorizationStatus::None
        });
    } else {
        drive(&mut older, &clock, |ns| {
            matches!(
                status(&ns[0]),
                ReplicationAuthorizationStatus::Granted { .. }
            )
        });
        assert_eq!(
            status(&older[0]),
            ReplicationAuthorizationStatus::Granted {
                candidate,
                configuration: cid(12),
                base: cid(10)
            }
        );
    }
    assert_old_view(&older[0], &baseline);
    // Reopen the failed promoted store. Its acknowledged write survives, while
    // only a retained-history witness can authorize the old view's catch-up.
    older.push(open_node(2));
    assert_eq!(
        older[2].local().applications[&group()].read_applied(acknowledged),
        Ok(7)
    );
    older[2].control(group(), NodeControl::Campaign).unwrap();
    drive(&mut older, &clock, |ns| {
        ns[2].local().owner.core(group()).unwrap().role() == Role::Leader
    });
    // Wait for recovery/elections to settle before admitting the new client write.
    drive(&mut older, &clock, |ns| {
        ns[2].local().owner.core(group()).unwrap().role() == Role::Leader
            && ns[1].local().applications[&group()].read_applied(acknowledged) == Ok(7)
            && (compacted_witness
                || ns[0].local().applications[&group()].read_applied(acknowledged) == Ok(7))
    });
    let final_index = write(&mut older, &clock, 2, 902, 2, 9);
    drive(&mut older, &clock, |ns| {
        ns[1..]
            .iter()
            .all(|n| n.local().applications[&group()].read_applied(final_index) == Ok(9))
            && (compacted_witness
                || ns[0].local().applications[&group()].read_applied(final_index) == Ok(9))
    });
    assert_eq!(status(&older[0]), ReplicationAuthorizationStatus::None);
    if compacted_witness {
        assert_old_view(&older[0], &baseline);
        assert_eq!(
            older[0].local().applications[&group()].read_applied(1),
            Ok(0)
        );
        assert_eq!(
            older[0]
                .local()
                .owner
                .core(group())
                .unwrap()
                .membership()
                .id(),
            cid(10)
        );
    } else {
        assert_eq!(
            older[0]
                .local()
                .owner
                .core(group())
                .unwrap()
                .membership()
                .id(),
            cid(12)
        );
        assert!(!older[0].local().owner.core(group()).unwrap().local_voter());
        assert!(older[0]
            .local()
            .owner
            .core(group())
            .unwrap()
            .state()
            .snapshot
            .is_some());
    }
    for n in &mut older {
        n.begin_shutdown();
    }
    let shutdown = Instant::now();
    while older.iter().any(|n| !n.is_drained()) {
        poll(&mut older, 10000 + shutdown.elapsed().as_millis() as u64);
        assert!(shutdown.elapsed() < Duration::from_secs(5));
        std::thread::park_timeout(Duration::from_millis(1));
    }
    for n in older {
        close(n);
    }
    for local in 1..=3 {
        let path = root.join(local.to_string());
        let log = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(local),
            LogLimits::default(),
        )
        .unwrap();
        let mut snapshots = NativeSnapshotStore::recover(
            FileSnapshotIo::open(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: identity(local),
                group: group(),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
        let mut app = Counter::new(100).unwrap();
        let (core, _) =
            recover_member_replica(node(local), group(), &log, &mut snapshots, &mut app).unwrap();
        if local == 1 && compacted_witness {
            assert_eq!(core.membership().id(), cid(10));
            assert_eq!(core.state().commit_index, 1);
            assert_eq!(app.read_applied(1), Ok(0));
        } else {
            assert_eq!(core.membership().id(), cid(12));
            assert_eq!(app.read_applied(final_index), Ok(9));
        }
        assert_eq!(
            core.replication_authorization_status(),
            ReplicationAuthorizationStatus::None
        );
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_promoted_leader_loss_requires_retained_restored_witness() {
    promoted_leader_loss_and_witness_outage(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_promoted_leader_loss_refuses_compacted_witness() {
    promoted_leader_loss_and_witness_outage(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_promoted_leader_loss_requires_retained_restored_witness() {
    promoted_leader_loss_and_witness_outage(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_promoted_leader_loss_refuses_compacted_witness() {
    promoted_leader_loss_and_witness_outage(NativePeerProtocol::Quic, true);
}
