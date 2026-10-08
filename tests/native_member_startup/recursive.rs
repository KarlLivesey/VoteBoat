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

type Service = NativeNode<Counter, NativeServiceConnector>;

fn poll(nodes: &mut [Service], clock: &Instant) {
    for n in nodes {
        let progress = n
            .poll(
                MonoTime(clock.elapsed().as_millis() as u64),
                NodePollBudget::default(),
            )
            .unwrap();
        if let Some(replica) = progress.replica {
            for step in replica.steps {
                // An older candidate's queued vote can outlive final catch-up.
                // Only unaffiliated ingress refusals are admissible here.
                assert!(
                    step.error.is_none()
                        || (matches!(
                            step.error,
                            Some(RaftError::WrongIdentity | RaftError::InvalidMessage)
                        ) && step.admission.is_none()
                            && step.proposed.is_none()
                            && step.operation.is_none()
                            && step.read.is_none()),
                    "{step:?}"
                );
            }
        }
    }
}

pub(super) fn drive(nodes: &mut [Service], clock: &Instant, done: impl Fn(&[Service]) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        poll(nodes, clock);
        if done(nodes) {
            return;
        }
        assert!(Instant::now() < deadline, "recursive partial final stalled");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

pub(super) fn shutdown(mut nodes: Vec<Service>, clock: &Instant) {
    for n in &mut nodes {
        n.begin_shutdown();
    }
    drive(&mut nodes, clock, |ns| ns.iter().all(|n| n.is_drained()));
    for n in nodes {
        close(n);
    }
}

fn partial_final_restart(protocol: NativePeerProtocol, compacted_joint: bool) {
    let root = root();
    std::fs::create_dir(&root).unwrap();
    // Old requires node 1; new requires the entire nested {2,3} child. A flat
    // two-of-three count would incorrectly permit {1,2} or {1,3} in the new view.
    let policy = |new| {
        Policy::new(
            Tree::Weighted(vec![
                WeightedChild {
                    weight: if new { 1 } else { 3 },
                    node: Tree::Voter(node(1)),
                },
                WeightedChild {
                    weight: if new { 3 } else { 1 },
                    node: Tree::Majority(vec![Tree::Voter(node(2)), Tree::Voter(node(3))]),
                },
            ]),
            Limits::default(),
        )
        .unwrap()
    };
    let initial = Bootstrap {
        policy: policy(false),
        voter_stores: [1, 2, 3].map(|id| (node(id), identity(id))).into(),
        ..bootstrap()
    };
    let configuration = |id, policy| {
        Configuration::new(
            cid(id),
            policy,
            initial.voter_stores.clone(),
            BTreeMap::new(),
        )
        .unwrap()
    };
    let history: Vec<_> = [
        ConfigurationRecord {
            operation: OperationId::new(100).unwrap(),
            expected: cid(9),
            change: ConfigurationChange::Learners(configuration(10, policy(false))),
        },
        ConfigurationRecord {
            operation: OperationId::new(101).unwrap(),
            expected: cid(10),
            change: ConfigurationChange::Joint {
                id: cid(11),
                next: configuration(12, policy(true)),
            },
        },
        ConfigurationRecord {
            operation: OperationId::new(101).unwrap(),
            expected: cid(11),
            change: ConfigurationChange::Final { id: cid(12) },
        },
    ]
    .into_iter()
    .enumerate()
    .map(|(index, record)| LogEntry {
        index: index as u64 + 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(record)),
    })
    .collect();
    // Seed a legally committed joint everywhere, final delivered only to 3.
    // Node 1 then remains offline for the whole native history.
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
        let tickets = log
            .append_batch(vec![LogMutation::Update(LogUpdate {
                group: group(),
                expected_revision: log.state(group()).unwrap().revision,
                hard_state: HardState {
                    term: 1,
                    voted_for: None,
                },
                commit_index: 2,
                suffix: Some(Suffix {
                    from: 1,
                    entries: history[..if local == 3 { 3 } else { 2 }].to_vec(),
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
        if local == 2 && compacted_joint {
            let mut app = Counter::new(100).unwrap();
            let (mut core, _) =
                recover_member_replica(node(local), group(), &log, &mut snapshots, &mut app)
                    .unwrap();
            let checkpoint = checkpoint_application(&core, &app, &mut snapshots).unwrap();
            compact_replica(
                &mut core,
                &mut log,
                &mut snapshots,
                &app,
                checkpoint.reference(),
            )
            .unwrap();
        }
    }
    let reservations: Vec<_> = (0..3).map(|_| reservation()).collect();
    let addresses: Vec<_> = reservations.iter().map(|r| r.0).collect();
    let clock = Instant::now();
    let open_node = |local: u64| {
        let (mut selected, _hints) = startup(&root.join(local.to_string()), local, &[1, 2, 3]);
        selected.startup.bootstrap = initial.clone();
        selected.startup.listen = addresses[local as usize - 1];
        selected.startup.tls = selected.startup.tls.with_wire_version(6).unwrap();
        for (peer, hint) in &mut selected.startup.peers {
            hint.address = addresses[peer.get() as usize - 1];
        }
        selected
            .open_with_protocol(
                protocol,
                Counter::new(100).unwrap(),
                Arc::new(ThreadWake::current()),
                MonoTime(clock.elapsed().as_millis() as u64),
            )
            .unwrap()
    };
    drop(reservations);
    let mut nodes = vec![open_node(3)];
    nodes[0].control(group(), NodeControl::Campaign).unwrap();
    drive(&mut nodes, &clock, |ns| {
        let core = ns[0].local().owner.core(group()).unwrap();
        core.state().hard_state.term == 2 && core.state().hard_state.voted_for == Some(node(3))
    });
    let promise = nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .state()
        .clone();
    assert_eq!(promise.ballot_origin.unwrap().configuration, cid(12));
    assert_eq!(promise.commit_index, 2);
    assert_eq!(
        nodes[0].local().owner.core(group()).unwrap().role(),
        Role::Candidate
    );
    let operation = OperationId::new(902).unwrap();
    let rejected = nodes[0]
        .propose(ClientRequest {
            group: group(),
            operation,
            bytes: 7i64.to_le_bytes().to_vec(),
        })
        .unwrap_err();
    assert_eq!(
        rejected.reason,
        ClientError::Consensus(RaftError::NotLeader)
    );
    shutdown(nodes, &clock);
    // Reopen the candidate after its exact durable promise, before any vote
    // reply. The older voter comes back from WAL or a verified joint snapshot.
    let mut nodes = vec![open_node(2), open_node(3)];
    assert_eq!(
        nodes[1]
            .local()
            .owner
            .core(group())
            .unwrap()
            .state()
            .hard_state,
        promise.hard_state
    );
    assert_eq!(
        nodes[1]
            .local()
            .owner
            .core(group())
            .unwrap()
            .state()
            .ballot_origin,
        promise.ballot_origin
    );
    assert_eq!(
        nodes[0]
            .local()
            .owner
            .core(group())
            .unwrap()
            .membership()
            .id(),
        cid(11)
    );
    assert_eq!(
        nodes[1]
            .local()
            .owner
            .core(group())
            .unwrap()
            .membership()
            .id(),
        cid(12)
    );
    nodes[1].control(group(), NodeControl::Campaign).unwrap();
    drive(&mut nodes, &clock, |ns| {
        ns[1].local().owner.core(group()).unwrap().role() == Role::Leader
            && ns
                .iter()
                .all(|n| n.local().owner.core(group()).unwrap().state().commit_index >= 4)
    });
    // Its reply echoed final scope, but the durable promise was made while
    // node 2 still accepted joint. Catch-up must not rewrite that origin.
    let voter_promise = nodes[0]
        .local()
        .owner
        .core(group())
        .unwrap()
        .state()
        .ballot_origin
        .unwrap();
    assert_eq!(voter_promise.configuration, cid(11));
    assert_eq!(voter_promise.candidate_store, identity(3));
    let ticket = nodes[1]
        .propose(ClientRequest {
            group: group(),
            operation,
            bytes: 7i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    let mut index = None;
    let deadline = Instant::now() + Duration::from_secs(10);
    while index.is_none()
        || !nodes
            .iter()
            .all(|n| n.local().applications[&group()].read_applied(index.unwrap()) == Ok(7))
    {
        poll(&mut nodes, &clock);
        while let Some(reply) = nodes[1].poll_client() {
            assert_eq!(reply.ticket(), ticket);
            let ClientOutcome::Applied { position, receipt } =
                nodes[1].complete_client(reply).unwrap()
            else {
                panic!("write not acknowledged")
            };
            assert_eq!(receipt.outcome, CounterOutcome::Value(7));
            index = Some(position.index);
        }
        assert!(Instant::now() < deadline);
        std::thread::park_timeout(Duration::from_millis(1));
    }
    shutdown(nodes, &clock);
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
        if local == 1 {
            assert_eq!(core.membership().id(), cid(11));
            assert_eq!(core.state().commit_index, 2);
            assert_eq!(app.read_applied(2), Ok(0));
        } else {
            assert_eq!(core.membership().id(), cid(12));
            assert_eq!(app.read_applied(index.unwrap()), Ok(7));
            if local == 2 {
                assert_eq!(core.state().ballot_origin, Some(voter_promise));
            }
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn tcp_recursive_partial_final_restarts_candidate_and_joint_voter() {
    partial_final_restart(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_recursive_partial_final_recovers_compacted_joint_voter() {
    partial_final_restart(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_recursive_partial_final_restarts_candidate_and_joint_voter() {
    partial_final_restart(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_recursive_partial_final_recovers_compacted_joint_voter() {
    partial_final_restart(NativePeerProtocol::Quic, true);
}

#[test]
fn tcp_recursive_partial_joint_restarts_after_native_learner_repair() {
    remote_joint_repair_schedule(NativePeerProtocol::TcpTls, 80, None, true, false, true);
}
#[test]
fn tcp_recursive_partial_joint_restarts_after_divergent_tail_repair() {
    remote_joint_repair_schedule(NativePeerProtocol::TcpTls, 160, None, true, true, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_recursive_partial_joint_restarts_after_native_learner_repair() {
    remote_joint_repair_schedule(NativePeerProtocol::Quic, 80, None, true, false, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_recursive_partial_joint_restarts_after_divergent_tail_repair() {
    remote_joint_repair_schedule(NativePeerProtocol::Quic, 160, None, true, true, true);
}
