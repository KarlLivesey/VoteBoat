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
    net::{SocketAddr, TcpListener, UdpSocket},
    path::{Path, PathBuf},
    sync::Arc,
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
fn reservation() -> (SocketAddr, TcpListener, UdpSocket) {
    (0..32)
        .find_map(|_| {
            let tcp = TcpListener::bind("127.0.0.1:0").ok()?;
            let address = tcp.local_addr().ok()?;
            let udp = UdpSocket::bind(address).ok()?;
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
        entropy_seed: 60,
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
    config.open_with_protocol(
        protocol,
        Counter::new(100).unwrap(),
        Arc::new(ThreadWake::current()),
        MonoTime(0),
    )
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

fn remote_joint_repair(protocol: NativePeerProtocol, missing_entries: u64) {
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
    initial.policy = policy(3);
    let learners = Configuration::new(
        cid(10),
        initial.policy.clone(),
        initial.voter_stores.clone(),
        [(node(2), identity(2))].into(),
    )
    .unwrap();
    let target = Configuration::new(
        cid(12),
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
        .unwrap(),
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
    let election_commit = history.len() as u64 + 1;
    for (local, count) in [(2, 1), (3, history.len())] {
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
        let tickets = log
            .append_batch(vec![LogMutation::Update(LogUpdate {
                group: group(),
                expected_revision: state.revision,
                hard_state: HardState {
                    term: 1,
                    voted_for: None,
                },
                commit_index: 1,
                suffix: Some(Suffix {
                    from: 1,
                    entries: history[..count].to_vec(),
                }),
                snapshot: None,
                snapshot_membership: None,
            })])
            .unwrap();
        log.barrier(&tickets).unwrap();
        NativeSnapshotStore::create(
            FileSnapshotIo::create(path.join("snapshots")).unwrap(),
            SnapshotIdentity {
                store: identity(local),
                group: group(),
            },
            SnapshotLimits::default(),
        )
        .unwrap();
    }
    let endpoint2 = reservation();
    let endpoint3 = reservation();
    let (mut learner, _hints2) = startup(&root.join("2"), 2, &[1, 2, 3]);
    let (mut candidate, _hints3) = startup(&root.join("3"), 3, &[1, 2, 3]);
    learner.startup.bootstrap = initial.clone();
    candidate.startup.bootstrap = initial;
    learner.startup.listen = endpoint2.0;
    candidate.startup.listen = endpoint3.0;
    learner.startup.peers.get_mut(&node(3)).unwrap().address = endpoint3.0;
    candidate.startup.peers.get_mut(&node(2)).unwrap().address = endpoint2.0;
    drop(endpoint2);
    drop(endpoint3);
    let mut learner = open(learner, protocol).unwrap();
    let mut candidate = open(candidate, protocol).unwrap();
    assert!(!learner.local().owner.core(group()).unwrap().local_voter());
    let start = Instant::now();
    let mut ticket = None;
    let mut applied_index = None;
    loop {
        let now = MonoTime(start.elapsed().as_millis() as u64);
        for n in [&mut learner, &mut candidate] {
            let p = n.poll(now, NodePollBudget::default()).unwrap();
            if let Some(replica) = p.replica {
                for step in replica.steps {
                    assert!(step.error.is_none(), "protocol={protocol:?} step={step:?}");
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
            learner.local().applications[&group()].read_applied(index) == Ok(7)
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
    close(candidate);
    close(learner);
    for local in [2, 3] {
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
        assert_eq!(app.read_applied(applied_index.unwrap()), Ok(7));
    }
    std::fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_native_joint_repair_elects_and_commits_after_old_leader_loss() {
    remote_joint_repair(NativePeerProtocol::TcpTls, 0);
}
#[cfg(feature = "quic")]
#[test]
fn quic_native_joint_repair_elects_and_commits_after_old_leader_loss() {
    remote_joint_repair(NativePeerProtocol::Quic, 0);
}
#[test]
fn tcp_native_joint_repair_catches_up_retained_learner_prefix() {
    remote_joint_repair(NativePeerProtocol::TcpTls, 32);
}
#[cfg(feature = "quic")]
#[test]
fn quic_native_joint_repair_catches_up_retained_learner_prefix() {
    remote_joint_repair(NativePeerProtocol::Quic, 32);
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
