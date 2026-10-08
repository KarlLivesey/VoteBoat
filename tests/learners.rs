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
//! Explicit host-authorized learner enrollment through the public storage,
//! core, application and snapshot contracts. No network bootstrap is inferred.
mod support;
use support::*;
use voteboat::{application::*, identity::*, log::*, membership::*, raft::*, snapshot::*};
fn cid(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn assignment(state: &GroupLog, id: u64, learners: &[(u64, u128)]) -> LogEntry {
    LogEntry {
        index: state.last_index() + 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(id as u128).unwrap(),
            expected: state.membership().unwrap().id(),
            change: ConfigurationChange::Learners(
                Configuration::new(
                    cid(id),
                    state.bootstrap.policy.clone(),
                    state.bootstrap.voter_stores.clone(),
                    learners
                        .iter()
                        .map(|(n, s)| (node(*n), identity(*s)))
                        .collect(),
                )
                .unwrap(),
            ),
        })),
    }
}
fn enroll<S: LogStore>(store: &mut S) {
    // The host is explicitly authorized to import this committed assignment
    // and its bootstrap into the newly assigned learner's selected store.
    append(store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let s = store.state(group(1)).unwrap();
    let entry = assignment(&s, 2, &[(4, 4), (5, 5)]);
    append(
        store,
        vec![update(
            &s,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry],
            }),
        )],
    );
}
fn message(rpc: Rpc, term: u64) -> Message {
    let sender = HostLogStore::new(1).binding();
    Message {
        group: group(1),
        configuration: cid(2),
        from: node(1),
        sender,
        to: node(4),
        term,
        context: RequestContext {
            origin: sender,
            sequence: term,
        },
        rpc,
    }
}
fn command_message() -> Message {
    message(
        Rpc::Append {
            previous_index: 1,
            previous_term: 1,
            entries: vec![LogEntry {
                index: 2,
                term: 2,
                payload: EntryPayload::Command {
                    operation: OperationId::new(7).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec(),
                },
            }],
            leader_commit: 2,
        },
        2,
    )
}
fn persist<S: LogStore>(core: &mut Raft, store: &mut S, effects: Vec<Effect>) -> Vec<Effect> {
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("expected one persistence dependency")
    };
    persist_effect(core, store, update.clone()).unwrap()
}
fn core<S: LogStore>(store: &S) -> Raft {
    Raft::recover_learner(
        node(4),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap()
}
fn conformance<S: LogStore>(store: &mut S) -> Raft {
    enroll(store);
    let mut core = core(store);
    assert!(!core.local_voter());
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.step(Event::Campaign), Err(RaftError::NotVoter));
    assert_eq!(
        core.step(Event::Propose {
            operation: OperationId::new(1).unwrap(),
            bytes: vec![1]
        }),
        Err(RaftError::NotLeader)
    );
    assert_eq!(
        core.step(Event::Read {
            request: ReadRequestId::new(1).unwrap()
        }),
        Err(RaftError::NotLeader)
    );
    let before = core.state().clone();
    assert_eq!(
        core.step(Event::Receive(message(Rpc::ReadProbe, 99))),
        Err(RaftError::WrongIdentity)
    );
    assert_eq!(core.state(), &before);
    let vote = message(
        Rpc::Vote {
            last_index: 1,
            last_term: 1,
        },
        2,
    );
    let effects = core.step(Event::Receive(vote)).unwrap();
    assert_eq!(core.state().hard_state.term, 1);
    let effects = persist(&mut core, store, effects);
    assert!(matches!(
        &effects[..],
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: false },
            ..
        })]
    ));
    assert_eq!(core.state().hard_state.voted_for, None);
    assert_eq!(core.state().ballot_origin, None);
    let request = command_message();
    let effects = core.step(Event::Receive(request.clone())).unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!()
    };
    let tickets = store
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    core.admitted(tickets[0]).unwrap();
    assert_eq!(core.state().last_index(), 1);
    assert_eq!(store.state(group(1)).unwrap().last_index(), 1);
    assert_eq!(
        core.complete(&DurableLog { tickets: vec![] }),
        Err(RaftError::WrongCompletion)
    );
    let completion = store.barrier(&tickets).unwrap();
    let effects = core.complete(&completion).unwrap();
    assert!(
        matches!(&effects[..], [Effect::Committed(entries), Effect::Send(Message { rpc: Rpc::Appended { success: true, matching_index: 2 }, .. })] if entries.len() == 1)
    );
    assert!(!core.local_voter());
    assert_eq!(core.state().commit_index, 2);
    assert!(matches!(
        &core.step(Event::Receive(request)).unwrap()[..],
        [Effect::Send(Message {
            rpc: Rpc::Appended {
                success: true,
                matching_index: 2
            },
            ..
        })]
    ));
    let mut app = Counter::new(16).unwrap();
    app.apply_batch(core.replay_committed()).unwrap();
    assert_eq!(app.read_applied(2).unwrap(), 7);
    core
}
fn snapshots() -> support::snapshot::HostSnapshots {
    let mut snapshots = support::snapshot::HostSnapshots::new();
    snapshots.identity.store = identity(4);
    snapshots.binding.identity = identity(4);
    snapshots
}
fn checkpoint_conformance<L: LogStore, S: SnapshotRetention>(log: &mut L, snapshots: &mut S) {
    let mut core = conformance(log);
    let mut app = Counter::new(16).unwrap();
    app.apply_batch(core.replay_committed()).unwrap();
    let receipt = checkpoint_application(&core, &app, snapshots).unwrap();
    assert_eq!(receipt.metadata.configuration(), cid(2));
    compact_replica(&mut core, log, snapshots, &app, receipt.reference()).unwrap();
    assert_eq!(core.state().base_index(), 2);
    assert!(core.state().entries.is_empty());
    assert!(matches!(
        Raft::recover_learner(
            node(4),
            log.binding(),
            log.state(group(1)).unwrap(),
            log.limits()
        ),
        Err(RaftError::InvalidRecovery)
    ));
    let mut restored = Counter::new(16).unwrap();
    let (mut core, replay) =
        recover_learner_replica(node(4), group(1), log, snapshots, &mut restored).unwrap();
    assert_eq!(replay.checkpoint_index, 2);
    assert!(replay.replay_receipts.is_empty());
    assert_eq!(restored.read_applied(2).unwrap(), 7);
    assert!(!core.local_voter());
    assert_eq!(core.step(Event::Campaign), Err(RaftError::NotVoter));
}
#[test]
fn host_learner_enrollment_replication_and_checkpoint_recovery() {
    checkpoint_conformance(&mut HostLogStore::new(4), &mut snapshots());
}
#[test]
fn enrollment_requires_committed_and_accepted_exact_store_assignment() {
    let mut store = HostLogStore::new(4);
    enroll(&mut store);
    let state = store.state(group(1)).unwrap();
    assert!(matches!(
        Raft::recover(node(4), store.binding(), state.clone(), store.limits()),
        Err(RaftError::InvalidRecovery)
    ));
    for kind in 0..5 {
        let mut invalid = state.clone();
        let mut binding = store.binding();
        let mut local = node(4);
        match kind {
            0 => invalid.commit_index = 0,
            1 => binding.identity = identity(99),
            2 => local = node(1),
            3 => {
                invalid.hard_state.voted_for = Some(node(1));
                invalid.ballot_origin = Some(BallotOrigin {
                    configuration: cid(1),
                    candidate_store: identity(1),
                });
            }
            _ => invalid.entries.clear(),
        }
        assert!(matches!(
            Raft::recover_learner(local, binding, invalid, store.limits()),
            Err(RaftError::InvalidRecovery)
        ));
    }
    // An uncommitted unrelated learner change does not revoke a committed
    // exact local assignment. Removing/replacing this learner does.
    for learners in [vec![(4, 4), (6, 6)], vec![(5, 5)], vec![(4, 44)]] {
        let entry = assignment(&state, 3, &learners);
        let mut next = [(group(1), state.clone())].into();
        apply_batch(
            &mut next,
            &[update(
                &state,
                1,
                1,
                Some(Suffix {
                    from: 2,
                    entries: vec![entry],
                }),
            )],
            store.limits(),
        )
        .unwrap();
        let valid = Raft::recover_learner(
            node(4),
            store.binding(),
            next.remove(&group(1)).unwrap(),
            store.limits(),
        )
        .is_ok();
        assert_eq!(valid, learners[0] == (4, 4));
    }
}
#[test]
fn missing_pinned_learner_data_prevents_recovery_and_application_exposure() {
    let mut log = HostLogStore::new(4);
    let mut snapshots = snapshots();
    checkpoint_conformance(&mut log, &mut snapshots);
    snapshots.pins.clear();
    snapshots.current = None;
    let mut application = Counter::new(16).unwrap();
    assert!(
        recover_learner_replica(node(4), group(1), &log, &mut snapshots, &mut application).is_err()
    );
    assert_eq!(application.applied_index(), 0);
    assert_eq!(application.read_applied(0).unwrap(), 0);
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::native::{log_store::*, snapshot_store::*};
    #[test]
    fn native_log_and_host_snapshot_use_the_same_learner_contract() {
        let mut log =
            NativeLogStore::create(ModelIo::default(), identity(4), LogLimits::default()).unwrap();
        checkpoint_conformance(&mut log, &mut snapshots());
    }
    #[test]
    fn native_files_restart_compacted_learner_with_verified_application_and_exact_assignment() {
        let root = std::env::temp_dir().join(format!("voteboat-learner37-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let mut log = NativeLogStore::create(
            FileLogIo::create(root.join("log")).unwrap(),
            identity(4),
            LogLimits::default(),
        )
        .unwrap();
        let si = SnapshotIdentity {
            store: identity(4),
            group: group(1),
        };
        let mut snapshots = NativeSnapshotStore::create(
            FileSnapshotIo::create(root.join("snapshot")).unwrap(),
            si,
            SnapshotLimits::default(),
        )
        .unwrap();
        checkpoint_conformance(&mut log, &mut snapshots);
        let expected = log.state(group(1)).unwrap();
        log.reclaim(log.limits().max_wal_bytes).unwrap();
        drop(log);
        drop(snapshots);
        let log = NativeLogStore::recover(
            FileLogIo::open(root.join("log")).unwrap(),
            identity(4),
            LogLimits::default(),
        )
        .unwrap();
        let mut snapshots = NativeSnapshotStore::recover(
            FileSnapshotIo::open(root.join("snapshot")).unwrap(),
            si,
            SnapshotLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group(1)).unwrap(), expected);
        let mut app = Counter::new(16).unwrap();
        let (mut core, restored) =
            recover_learner_replica(node(4), group(1), &log, &mut snapshots, &mut app).unwrap();
        assert_eq!(restored.checkpoint_index, 2);
        assert_eq!(app.read_applied(2).unwrap(), 7);
        assert!(!core.local_voter());
        assert_eq!(core.step(Event::Campaign), Err(RaftError::NotVoter));
        drop(snapshots);
        drop(log);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn learner_assignment_written_is_not_enrollment_and_every_torn_frame_recovers_whole_state() {
        let io = ModelIo::default();
        let mut log =
            NativeLogStore::create(io.clone(), identity(4), LogLimits::default()).unwrap();
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let old = log.state(group(1)).unwrap();
        let before = io.0.borrow().log.len();
        let manifest = io.0.borrow().manifest.clone();
        let record = assignment(&old, 2, &[(4, 4)]);
        let tickets = log
            .append_batch(vec![update(
                &old,
                1,
                1,
                Some(Suffix {
                    from: 1,
                    entries: vec![record],
                }),
            )])
            .unwrap();
        assert!(Raft::recover_learner(
            node(4),
            log.binding(),
            log.state(group(1)).unwrap(),
            log.limits()
        )
        .is_err());
        let full = io.0.borrow().log.clone();
        log.barrier(&tickets).unwrap();
        let expected = log.state(group(1)).unwrap();
        assert!(
            Raft::recover_learner(node(4), log.binding(), expected.clone(), log.limits()).is_ok()
        );
        drop(log);
        for cut in before..=full.len() {
            let image = ModelIo::default();
            image.0.borrow_mut().log = full[..cut].to_vec();
            image.0.borrow_mut().manifest = manifest.clone();
            let log = NativeLogStore::recover(image, identity(4), LogLimits::default()).unwrap();
            let state = log.state(group(1)).unwrap();
            assert_eq!(
                state,
                if cut == full.len() {
                    expected.clone()
                } else {
                    old.clone()
                },
                "cut {cut}"
            );
            assert_eq!(
                Raft::recover_learner(node(4), log.binding(), state, log.limits()).is_ok(),
                cut == full.len()
            );
        }
    }
    #[test]
    fn failed_assignment_barriers_never_create_a_partially_enrolled_replica() {
        for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
            let io = ModelIo::default();
            let mut log =
                NativeLogStore::create(io.clone(), identity(4), LogLimits::default()).unwrap();
            append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
            let old = log.state(group(1)).unwrap();
            let record = assignment(&old, 2, &[(4, 4)]);
            let tickets = log
                .append_batch(vec![update(
                    &old,
                    1,
                    1,
                    Some(Suffix {
                        from: 1,
                        entries: vec![record],
                    }),
                )])
                .unwrap();
            io.0.borrow_mut().fault = fault;
            assert!(log.barrier(&tickets).is_err());
            drop(log);
            io.0.borrow_mut().power_loss();
            let log = NativeLogStore::recover(io, identity(4), LogLimits::default()).unwrap();
            let state = log.state(group(1)).unwrap();
            let enrolled = state.commit_index == 1;
            assert_eq!(
                state.membership().unwrap().replica_store(node(4)).is_some(),
                enrolled
            );
            assert_eq!(
                Raft::recover_learner(node(4), log.binding(), state, log.limits()).is_ok(),
                enrolled
            );
        }
    }
}

#[test]
fn joint_or_policy_transition_cannot_use_learner_recovery_to_open_dynamic_voting() {
    let mut log = HostLogStore::new(4);
    enroll(&mut log);
    let s = log.state(group(1)).unwrap();
    for voters in [vec![1, 2, 3], vec![1, 2, 4]] {
        let learners = if voters.contains(&4) {
            vec![3, 5]
        } else {
            vec![4, 5]
        };
        let target = Configuration::new(
            cid(4),
            voteboat::quorum::Policy::new(
                voteboat::quorum::Tree::Majority(
                    voters
                        .iter()
                        .map(|n| voteboat::quorum::Tree::Voter(node(*n)))
                        .collect(),
                ),
                voteboat::quorum::Limits::default(),
            )
            .unwrap(),
            voters
                .iter()
                .map(|n| (node(*n), identity(*n as u128)))
                .collect(),
            learners
                .iter()
                .map(|n| (node(*n), identity(*n as u128)))
                .collect(),
        )
        .unwrap();
        let entry = LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(3).unwrap(),
                expected: cid(2),
                change: ConfigurationChange::Joint {
                    id: cid(3),
                    next: target,
                },
            })),
        };
        let mut state = [(group(1), s.clone())].into();
        apply_batch(
            &mut state,
            &[update(
                &s,
                1,
                1,
                Some(Suffix {
                    from: 2,
                    entries: vec![entry],
                }),
            )],
            log.limits(),
        )
        .unwrap();
        assert!(matches!(
            Raft::recover_learner(
                node(4),
                log.binding(),
                state.remove(&group(1)).unwrap(),
                log.limits()
            ),
            Err(RaftError::InvalidRecovery)
        ));
    }
}

fn readiness_requirements() -> ReadinessRequirements {
    ReadinessRequirements {
        application_schema: 1,
        command_bytes: 8,
        snapshot_bytes: 4096,
    }
}
fn response(request: &Message, from: u64, rpc: Rpc) -> Message {
    Message {
        from: node(from),
        sender: HostLogStore::new(from as u128).binding(),
        to: request.from,
        rpc,
        ..request.clone()
    }
}
fn sent(effects: &[Effect], to: u64) -> Message {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Send(message) if message.to == node(to) => Some(message.clone()),
            _ => None,
        })
        .expect("request for replica")
}
/// Actual core election/replication; only initial enrollment is host-imported.
fn readiness_cluster<L: LogStore>(log: &mut L) -> (Raft, Raft, Counter, HostLogStore) {
    let mut leader_log = HostLogStore::new(1);
    let (leader, learner, application) = readiness_cluster_with_stores(log, &mut leader_log);
    (leader, learner, application, leader_log)
}
fn readiness_cluster_with_stores<L: LogStore, M: LogStore>(
    log: &mut L,
    leader_log: &mut M,
) -> (Raft, Raft, Counter) {
    enroll(leader_log);
    enroll(log);
    let mut leader = Raft::recover_member(
        node(1),
        leader_log.binding(),
        leader_log.state(group(1)).unwrap(),
        leader_log.limits(),
    )
    .unwrap();
    let effects = leader.step(Event::Campaign).unwrap();
    let effects = persist(&mut leader, leader_log, effects);
    let vote = sent(&effects, 2);
    let effects = leader
        .step(Event::Receive(response(
            &vote,
            2,
            Rpc::Voted { granted: true },
        )))
        .unwrap();
    let effects = persist(&mut leader, leader_log, effects);
    let append_two = sent(&effects, 2);
    let append_learner = sent(&effects, 4);
    let mut learner = core(log);
    let effects = learner.step(Event::Receive(append_learner)).unwrap();
    persist(&mut learner, log, effects);
    let effects = leader
        .step(Event::Receive(response(
            &append_two,
            2,
            Rpc::Appended {
                success: true,
                matching_index: 2,
            },
        )))
        .unwrap();
    persist(&mut leader, leader_log, effects);
    let effects = leader.step(Event::Heartbeat).unwrap();
    let effects = learner.step(Event::Receive(sent(&effects, 4))).unwrap();
    persist(&mut learner, log, effects);
    assert_eq!(leader.state().commit_index, 2);
    assert_eq!(learner.state().commit_index, 2);
    let mut application = Counter::new(16).unwrap();
    application.apply_batch(learner.replay_committed()).unwrap();
    (leader, learner, application)
}
fn learner_peer() -> voteboat::secure::PeerIdentity {
    voteboat::secure::PeerIdentity {
        node: node(4),
        store: identity(4),
    }
}
#[test]
fn readiness_uses_fresh_context_exact_sessions_and_applied_durable_prefix() {
    let mut log = HostLogStore::new(4);
    let (mut leader, learner, app, _leader_log) = readiness_cluster(&mut log);
    let mut snapshots = snapshots();
    let request = leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    assert_eq!(
        leader.begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements()
        ),
        Err(ReadinessError::Consensus(RaftError::Busy))
    );
    let unapplied = Counter::new(16).unwrap();
    assert_eq!(
        verify_learner_readiness(
            &learner,
            &unapplied,
            &log,
            &mut snapshots,
            request,
            leader.storage_binding()
        ),
        Err(ReadinessError::NotCaughtUp)
    );
    let receipt = verify_learner_readiness(
        &learner,
        &app,
        &log,
        &mut snapshots,
        request,
        leader.storage_binding(),
    )
    .unwrap();
    for changed in [
        LearnerReadinessRequest {
            index: request.index + 1,
            ..request
        },
        LearnerReadinessRequest {
            term: request.term + 1,
            ..request
        },
        LearnerReadinessRequest {
            configuration: cid(99),
            ..request
        },
        LearnerReadinessRequest {
            requirements: ReadinessRequirements {
                application_schema: 99,
                ..request.requirements
            },
            ..request
        },
    ] {
        assert_eq!(
            leader.accept_learner_readiness(
                LearnerReadinessReceipt {
                    request: changed,
                    binding: log.binding(),
                },
                log.binding()
            ),
            Err(ReadinessError::Stale)
        );
    }
    let mut wrong_session = log.binding();
    wrong_session.session = StoreSession::new(99).unwrap();
    assert_eq!(
        leader.accept_learner_readiness(receipt.clone(), wrong_session),
        Err(ReadinessError::WrongBinding)
    );
    let ready = leader
        .accept_learner_readiness(receipt.clone(), log.binding())
        .unwrap();
    leader
        .check_learner_readiness(&ready, log.binding())
        .unwrap();
    assert_eq!(
        leader.check_learner_readiness(&ready, wrong_session),
        Err(ReadinessError::WrongBinding)
    );
    assert_eq!(
        leader.accept_learner_readiness(receipt.clone(), log.binding()),
        Err(ReadinessError::Stale)
    );
    let fresh = leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    assert_ne!(fresh.context, request.context);
    assert_eq!(
        leader.accept_learner_readiness(receipt, log.binding()),
        Err(ReadinessError::Stale)
    );
    let receipt = verify_learner_readiness(
        &learner,
        &app,
        &log,
        &mut snapshots,
        fresh,
        leader.storage_binding(),
    )
    .unwrap();
    leader
        .accept_learner_readiness(receipt, log.binding())
        .unwrap();
    leader.storage_failed();
    assert_eq!(
        leader.check_learner_readiness(&ready, log.binding()),
        Err(ReadinessError::Consensus(RaftError::Fenced))
    );
}
#[test]
fn readiness_rejects_changed_capabilities_provider_bindings_and_pending_writes() {
    let mut log = HostLogStore::new(4);
    let (mut leader, mut learner, app, _leader_log) = readiness_cluster(&mut log);
    let mut snapshots = snapshots();
    let request = leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    for requirements in [
        ReadinessRequirements {
            application_schema: 99,
            ..request.requirements
        },
        ReadinessRequirements {
            command_bytes: usize::MAX,
            ..request.requirements
        },
        ReadinessRequirements {
            snapshot_bytes: usize::MAX,
            ..request.requirements
        },
    ] {
        assert_eq!(
            verify_learner_readiness(
                &learner,
                &app,
                &log,
                &mut snapshots,
                LearnerReadinessRequest {
                    requirements,
                    ..request
                },
                leader.storage_binding()
            ),
            Err(ReadinessError::Capability)
        );
    }
    let mut wrong = leader.storage_binding();
    wrong.session = StoreSession::new(99).unwrap();
    assert_eq!(
        verify_learner_readiness(&learner, &app, &log, &mut snapshots, request, wrong),
        Err(ReadinessError::WrongBinding)
    );
    snapshots.identity.store = identity(99);
    assert_eq!(
        verify_learner_readiness(
            &learner,
            &app,
            &log,
            &mut snapshots,
            request,
            leader.storage_binding()
        ),
        Err(ReadinessError::WrongBinding)
    );
    snapshots.identity.store = identity(4);
    let mut append = message(
        Rpc::Append {
            previous_index: 2,
            previous_term: 2,
            entries: vec![LogEntry {
                index: 3,
                term: 2,
                payload: EntryPayload::Noop,
            }],
            leader_commit: 2,
        },
        2,
    );
    append.context.sequence = 999;
    let effects = learner.step(Event::Receive(append)).unwrap();
    assert_eq!(
        verify_learner_readiness(
            &learner,
            &app,
            &log,
            &mut snapshots,
            request,
            leader.storage_binding()
        ),
        Err(ReadinessError::Consensus(RaftError::Busy))
    );
    // Written is not a completion: the dependency and readiness rejection remain.
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("pending append")
    };
    let tickets = log
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    learner.admitted(tickets[0]).unwrap();
    assert_eq!(
        verify_learner_readiness(
            &learner,
            &app,
            &log,
            &mut snapshots,
            request,
            leader.storage_binding()
        ),
        Err(ReadinessError::Consensus(RaftError::Busy))
    );
    let durable = log.barrier(&tickets).unwrap();
    learner.complete(&durable).unwrap();
    verify_learner_readiness(
        &learner,
        &app,
        &log,
        &mut snapshots,
        request,
        leader.storage_binding(),
    )
    .unwrap();
}

#[test]
fn readiness_requires_pinned_compacted_data_and_rechecks_configuration_and_commit() {
    let mut log = HostLogStore::new(4);
    let (mut leader, mut learner, app, _leader_log) = readiness_cluster(&mut log);
    let mut snapshots = snapshots();
    let request = leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    let receipt = checkpoint_application(&learner, &app, &mut snapshots).unwrap();
    compact_replica(
        &mut learner,
        &mut log,
        &mut snapshots,
        &app,
        receipt.reference(),
    )
    .unwrap();
    let ready_receipt = verify_learner_readiness(
        &learner,
        &app,
        &log,
        &mut snapshots,
        request,
        leader.storage_binding(),
    )
    .unwrap();
    let ready = leader
        .accept_learner_readiness(ready_receipt, log.binding())
        .unwrap();
    snapshots.pins.clear();
    assert!(matches!(
        verify_learner_readiness(
            &learner,
            &app,
            &log,
            &mut snapshots,
            request,
            leader.storage_binding()
        ),
        Err(ReadinessError::Storage(_))
    ));
    // A new accepted configuration is not proof of a committed learner assignment.
    let mut state = leader.state().clone();
    let mut changed = assignment(&state, 3, &[(4, 4), (5, 5)]);
    changed.term = state.hard_state.term;
    let mut leader_log = HostLogStore::new(1);
    leader_log.durable.insert(group(1), state.clone());
    leader_log.accepted.insert(group(1), state.clone());
    let mut mutation = update(
        &state,
        2,
        2,
        Some(Suffix {
            from: 3,
            entries: vec![changed],
        }),
    );
    if let LogMutation::Update(update) = &mut mutation {
        update.hard_state = state.hard_state;
    }
    append(&mut leader_log, vec![mutation]);
    state = leader_log.state(group(1)).unwrap();
    let mut restored = Raft::recover_member(
        node(1),
        leader.storage_binding(),
        state,
        leader_log.limits(),
    )
    .unwrap();
    assert_eq!(
        restored.begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements()
        ),
        Err(ReadinessError::Stale)
    );
    assert_eq!(
        restored.check_learner_readiness(&ready, log.binding()),
        Err(ReadinessError::Stale)
    );
    // Compaction below a requested boundary cannot establish a missing match.
    let mut old = request;
    old.index = 1;
    old.index_term = 1;
    assert_eq!(
        verify_learner_readiness(
            &learner,
            &app,
            &log,
            &mut snapshots,
            old,
            leader.storage_binding()
        ),
        Err(ReadinessError::NotCaughtUp)
    );
}

#[cfg(feature = "native")]
#[test]
fn native_readiness_reopens_compacted_files_and_requires_fresh_peer_session() {
    use voteboat::native::{log_store::*, snapshot_store::*};
    let root = std::env::temp_dir().join(format!("voteboat-readiness52-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let mut log = NativeLogStore::create(
        FileLogIo::create(root.join("log")).unwrap(),
        identity(4),
        LogLimits::default(),
    )
    .unwrap();
    let si = SnapshotIdentity {
        store: identity(4),
        group: group(1),
    };
    let mut snapshots = NativeSnapshotStore::create(
        FileSnapshotIo::create(root.join("snapshot")).unwrap(),
        si,
        SnapshotLimits::default(),
    )
    .unwrap();
    let (mut leader, mut learner, app, _leader_log) = readiness_cluster(&mut log);
    let old_binding = log.binding();
    let request = leader
        .begin_learner_readiness(
            learner_peer(),
            old_binding.session,
            readiness_requirements(),
        )
        .unwrap();
    let checkpoint = checkpoint_application(&learner, &app, &mut snapshots).unwrap();
    compact_replica(
        &mut learner,
        &mut log,
        &mut snapshots,
        &app,
        checkpoint.reference(),
    )
    .unwrap();
    let receipt = verify_learner_readiness(
        &learner,
        &app,
        &log,
        &mut snapshots,
        request,
        leader.storage_binding(),
    )
    .unwrap();
    let ready = leader
        .accept_learner_readiness(receipt, old_binding)
        .unwrap();
    drop(learner);
    drop(log);
    drop(snapshots);
    let log = NativeLogStore::recover(
        FileLogIo::open(root.join("log")).unwrap(),
        identity(4),
        LogLimits::default(),
    )
    .unwrap();
    let mut snapshots = NativeSnapshotStore::recover(
        FileSnapshotIo::open(root.join("snapshot")).unwrap(),
        si,
        SnapshotLimits::default(),
    )
    .unwrap();
    let mut app = Counter::new(16).unwrap();
    let (learner, restored) =
        recover_learner_replica(node(4), group(1), &log, &mut snapshots, &mut app).unwrap();
    assert_eq!(restored.checkpoint_index, 2);
    assert_ne!(log.binding().session, old_binding.session);
    assert_eq!(
        leader.check_learner_readiness(&ready, log.binding()),
        Err(ReadinessError::WrongBinding)
    );
    assert_eq!(
        verify_learner_readiness(
            &learner,
            &app,
            &log,
            &mut snapshots,
            request,
            leader.storage_binding()
        ),
        Err(ReadinessError::WrongBinding)
    );
    let fresh = leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    let receipt = verify_learner_readiness(
        &learner,
        &app,
        &log,
        &mut snapshots,
        fresh,
        leader.storage_binding(),
    )
    .unwrap();
    leader
        .accept_learner_readiness(receipt, log.binding())
        .unwrap();
    drop(log);
    drop(snapshots);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn readiness_expires_when_commit_advances_or_leader_changes_term() {
    let mut log = HostLogStore::new(4);
    let (mut leader, learner, app, mut leader_log) = readiness_cluster(&mut log);
    let mut snapshots = snapshots();
    let request = leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    let receipt = verify_learner_readiness(
        &learner,
        &app,
        &log,
        &mut snapshots,
        request,
        leader.storage_binding(),
    )
    .unwrap();
    let ready = leader
        .accept_learner_readiness(receipt, log.binding())
        .unwrap();
    leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    let effects = leader
        .step(Event::Propose {
            operation: OperationId::new(99).unwrap(),
            bytes: 7i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    assert_eq!(
        leader.check_learner_readiness(&ready, log.binding()),
        Err(ReadinessError::Consensus(RaftError::Busy))
    );
    let effects = persist(&mut leader, &mut leader_log, effects);
    let append_two = sent(&effects, 2);
    // Complete the heartbeat already in flight before acknowledging the new
    // command; a reply cannot exceed the original request's matching end.
    let effects = leader
        .step(Event::Receive(response(
            &append_two,
            2,
            Rpc::Appended {
                success: true,
                matching_index: 2,
            },
        )))
        .unwrap();
    let append_two = sent(&effects, 2);
    let effects = leader
        .step(Event::Receive(response(
            &append_two,
            2,
            Rpc::Appended {
                success: true,
                matching_index: 3,
            },
        )))
        .unwrap();
    persist(&mut leader, &mut leader_log, effects);
    assert_eq!(
        leader.check_learner_readiness(&ready, log.binding()),
        Err(ReadinessError::Stale)
    );
    // The stale pending request was canceled, rather than blocking a fresh round.
    let next = leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    assert_eq!(next.index, 3);
    assert_eq!(
        verify_learner_readiness(
            &learner,
            &app,
            &log,
            &mut snapshots,
            next,
            leader.storage_binding()
        ),
        Err(ReadinessError::NotCaughtUp)
    );
    let effects = leader.step(Event::Campaign).unwrap();
    let effects = persist(&mut leader, &mut leader_log, effects);
    let vote = sent(&effects, 2);
    let effects = leader
        .step(Event::Receive(response(
            &vote,
            2,
            Rpc::Voted { granted: true },
        )))
        .unwrap();
    persist(&mut leader, &mut leader_log, effects);
    assert_eq!(leader.role(), Role::Leader);
    assert_eq!(
        leader.check_learner_readiness(&ready, log.binding()),
        Err(ReadinessError::Stale)
    );
    assert_eq!(
        leader.begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements()
        ),
        Err(ReadinessError::NotCaughtUp)
    );
}

fn proposal(voters: &[u64], readiness: Vec<PromotionReadiness>) -> ConfigurationProposal {
    let target = Configuration::new(
        cid(4),
        voteboat::quorum::Policy::new(
            voteboat::quorum::Tree::Majority(
                voters
                    .iter()
                    .map(|n| voteboat::quorum::Tree::Voter(node(*n)))
                    .collect(),
            ),
            voteboat::quorum::Limits::default(),
        )
        .unwrap(),
        voters
            .iter()
            .map(|n| (node(*n), identity(*n as u128)))
            .collect(),
        [(node(5), identity(5))].into(),
    )
    .unwrap();
    ConfigurationProposal {
        record: ConfigurationRecord {
            operation: OperationId::new(53).unwrap(),
            expected: cid(2),
            change: ConfigurationChange::Joint {
                id: cid(3),
                next: target,
            },
        },
        readiness,
        requirements: readiness_requirements(),
    }
}
fn ready_proof<L: LogStore>(
    leader: &mut Raft,
    learner: &Raft,
    log: &L,
    app: &Counter,
) -> PromotionReadiness {
    let request = leader
        .begin_learner_readiness(
            learner_peer(),
            log.binding().session,
            readiness_requirements(),
        )
        .unwrap();
    let receipt = verify_learner_readiness(
        learner,
        app,
        log,
        &mut snapshots(),
        request,
        leader.storage_binding(),
    )
    .unwrap();
    PromotionReadiness {
        ready: leader
            .accept_learner_readiness(receipt, log.binding())
            .unwrap(),
        authenticated: log.binding(),
    }
}
fn configuration_error(error: ConfigurationProposalError) -> RaftError {
    RaftError::Configuration(Box::new(error))
}
#[test]
fn local_configuration_promotion_checks_all_proofs_before_mutating_journal() {
    let mut log = HostLogStore::new(4);
    let (mut leader, learner, app, _leader_log) = readiness_cluster(&mut log);
    let proof = ready_proof(&mut leader, &learner, &log, &app);
    let before = leader.state().clone();
    let cases = [
        (
            proposal(&[1, 2, 4], vec![]),
            ConfigurationProposalError::MissingReadiness(node(4)),
        ),
        (
            proposal(&[1, 2, 4], vec![proof.clone(), proof.clone()]),
            ConfigurationProposalError::UnexpectedReadiness(node(4)),
        ),
        (
            proposal(&[1, 2, 3], vec![proof.clone()]),
            ConfigurationProposalError::UnexpectedReadiness(node(4)),
        ),
    ];
    for (p, error) in cases {
        assert_eq!(
            leader.step(Event::Configure(Box::new(p))),
            Err(configuration_error(error))
        );
        assert_eq!(leader.state(), &before);
        assert_eq!(leader.membership().id(), cid(2));
        assert!(!leader.has_pending_dependency());
    }
    let mut wrong = proposal(&[1, 2, 4], vec![proof.clone()]);
    wrong.requirements.application_schema = 99;
    assert_eq!(
        leader.step(Event::Configure(Box::new(wrong))),
        Err(configuration_error(
            ConfigurationProposalError::WrongRequirements(node(4))
        ))
    );
    let mut wrong = proposal(&[1, 2, 4], vec![proof.clone()]);
    wrong.readiness[0].authenticated.session = StoreSession::new(99).unwrap();
    assert_eq!(
        leader.step(Event::Configure(Box::new(wrong))),
        Err(configuration_error(ConfigurationProposalError::Readiness(
            ReadinessError::WrongBinding
        )))
    );
    let mut wrong = proposal(&[1, 2, 4], vec![proof.clone()]);
    wrong.record.expected = cid(1);
    assert_eq!(
        leader.step(Event::Configure(Box::new(wrong))),
        Err(configuration_error(ConfigurationProposalError::Membership(
            MembershipError::StaleConfiguration
        )))
    );
    let effects = leader
        .step(Event::Configure(Box::new(proposal(
            &[1, 2, 4],
            vec![proof],
        ))))
        .unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert_eq!(leader.state(), &before);
    assert_eq!(leader.membership().id(), cid(3));
    assert!(leader.has_pending_dependency());
}

fn configuration_conformance<L: LogStore>(leader_log: &mut L) -> (Raft, u64) {
    let mut learner_log = HostLogStore::new(4);
    let (mut leader, learner, app) = readiness_cluster_with_stores(&mut learner_log, leader_log);
    let proof = ready_proof(&mut leader, &learner, &learner_log, &app);
    let effects = leader
        .step(Event::Configure(Box::new(proposal(
            &[1, 2, 4],
            vec![proof.clone()],
        ))))
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("configuration persistence")
    };
    let tickets = leader_log
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    leader.admitted(tickets[0]).unwrap();
    assert_eq!(leader.state().commit_index, 2);
    assert_eq!(leader.membership().id(), cid(3));
    assert_eq!(leader.step(Event::Heartbeat), Err(RaftError::Busy));
    let durable = leader_log.barrier(&tickets).unwrap();
    let effects = leader.complete(&durable).unwrap();
    let joint_index = leader.state().last_index();
    assert_eq!(joint_index, 3);
    let final_event = || {
        Event::Configure(Box::new(ConfigurationProposal {
            record: ConfigurationRecord {
                operation: OperationId::new(53).unwrap(),
                expected: cid(3),
                change: ConfigurationChange::Final { id: cid(4) },
            },
            readiness: vec![],
            requirements: readiness_requirements(),
        }))
    };
    assert_eq!(
        leader.step(final_event()),
        Err(configuration_error(ConfigurationProposalError::Membership(
            MembershipError::JointNotCommitted
        )))
    );
    let request = sent(&effects, 2);
    let effects = leader
        .step(Event::Receive(response(
            &request,
            2,
            Rpc::Appended {
                success: true,
                matching_index: joint_index,
            },
        )))
        .unwrap();
    persist(&mut leader, leader_log, effects);
    assert_eq!(leader.state().commit_index, joint_index);
    let effects = leader.step(final_event()).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert_eq!(leader.membership().id(), cid(4));
    assert_eq!(leader.state().membership().unwrap().id(), cid(3));
    let effects = persist(&mut leader, leader_log, effects);
    let request = sent(&effects, 2);
    let effects = leader
        .step(Event::Receive(response(
            &request,
            2,
            Rpc::Appended {
                success: true,
                matching_index: 4,
            },
        )))
        .unwrap();
    persist(&mut leader, leader_log, effects);
    assert_eq!(leader.state().commit_index, 4);
    assert_eq!(leader.membership().id(), cid(4));
    assert!(leader.membership().joint().is_none());
    assert!(leader.membership().is_voter(node(4)));
    assert!(!leader.membership().is_voter(node(3)));
    let mut application = Counter::new(16).unwrap();
    let receipts = application.apply_batch(leader.replay_committed()).unwrap();
    assert!(receipts.is_empty());
    assert_eq!(application.applied_index(), 4);
    assert_eq!(
        leader.check_learner_readiness(&proof.ready, learner_log.binding()),
        Err(ReadinessError::Stale)
    );
    // Deliberately only local proposal/quorum-reply checks: peer 2 acknowledgements
    // are host assertions, not faulted remote configuration delivery evidence.
    (leader, joint_index)
}
#[test]
fn host_configuration_proposal_requires_joint_commit_before_final_and_recovery_replays_it() {
    let mut log = HostLogStore::new(1);
    let (leader, _) = configuration_conformance(&mut log);
    let recovered = Raft::recover_member(
        node(1),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    assert_eq!(recovered.membership(), leader.membership());
    assert_eq!(recovered.state(), leader.state());
}
#[cfg(feature = "native")]
#[test]
fn native_configuration_proposals_reopen_committed_joint_and_final_journal() {
    use voteboat::native::log_store::*;
    let root = std::env::temp_dir().join(format!("voteboat-config53-{}", std::process::id()));
    std::fs::create_dir(&root).unwrap();
    let mut log = NativeLogStore::create(
        FileLogIo::create(&root).unwrap(),
        identity(1),
        LogLimits::default(),
    )
    .unwrap();
    let (leader, _) = configuration_conformance(&mut log);
    let expected = leader.state().clone();
    let old = log.binding();
    drop(log);
    let log = NativeLogStore::recover(
        FileLogIo::open(&root).unwrap(),
        identity(1),
        LogLimits::default(),
    )
    .unwrap();
    assert_ne!(log.binding().session, old.session);
    let recovered = Raft::recover_member(
        node(1),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    assert_eq!(recovered.state(), &expected);
    assert_eq!(recovered.membership().id(), cid(4));
    drop(log);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "native")]
#[test]
fn queued_promotion_rechecks_live_bindings_and_defaults_to_rejection() {
    use voteboat::{native::runtime::*, runtime::*, worker::*};
    let mut log = HostLogStore::new(4);
    let (mut leader, learner, app, _leader_log) = readiness_cluster(&mut log);
    let proof = ready_proof(&mut leader, &learner, &log, &app);
    let proposed = proposal(&[1, 2, 4], vec![proof.clone()]);
    let identity = RuntimeOwner {
        store: leader.storage_binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        identity,
        ShardLimits {
            max_groups: 1,
            max_event_bytes: 32 * 1024,
            ..ShardLimits::default()
        },
        FairScheduler::new(1).unwrap(),
    )
    .unwrap();
    shard.register(leader).unwrap();
    let timed = TimedShard::new(
        shard,
        DeadlineQueue::new(identity, 1).unwrap(),
        JitterEntropy::new(53),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    let mut owner = EffectOwner::new(
        timed,
        WorkerBinding {
            store: identity.store,
            generation: StorageWorkerGeneration::new(1).unwrap(),
        },
        EffectOwnerLimits::default(),
    )
    .unwrap();
    owner
        .set_connection_budget(
            ConnectionBudget::new(
                voteboat::secure::LocalIdentity {
                    node: node(1),
                    store: identity.store,
                },
                4,
                (2..=5)
                    .map(|n| (node(n), support::identity(n as u128)))
                    .collect(),
            )
            .unwrap(),
        )
        .unwrap();
    let before = owner.core(group(1)).unwrap().state().clone();
    let mut oversized = proposed.clone();
    oversized.readiness.reserve_exact(50000);
    let retained_capacity = oversized.readiness.capacity();
    let oversized = Event::Configure(Box::new(oversized));
    let rejected = owner.admit(group(1), oversized).unwrap_err();
    assert_eq!(rejected.reason, RuntimeError::EventTooLarge);
    let Event::Configure(returned) = *rejected.event else {
        panic!("original event")
    };
    assert_eq!(returned.readiness.capacity(), retained_capacity);
    assert_eq!(*returned, proposed);
    owner
        .admit(group(1), Event::Configure(Box::new(proposed.clone())))
        .unwrap();
    let steps = owner.advance(MonoTime(0), 1).unwrap();
    assert_eq!(
        steps[0].error,
        Some(configuration_error(
            ConfigurationProposalError::AuthenticationRequired
        ))
    );
    assert_eq!(owner.core(group(1)).unwrap().state(), &before);
    owner
        .admit(group(1), Event::Configure(Box::new(proposed.clone())))
        .unwrap();
    let steps = owner
        .advance_with_configuration_bindings(MonoTime(0), 1, |_| None)
        .unwrap();
    assert_eq!(
        steps[0].error,
        Some(configuration_error(
            ConfigurationProposalError::AuthenticationRequired
        ))
    );
    owner
        .admit(group(1), Event::Configure(Box::new(proposed.clone())))
        .unwrap();
    let mut restarted = proof.authenticated;
    restarted.session = StoreSession::new(99).unwrap();
    let steps = owner
        .advance_with_configuration_bindings(MonoTime(0), 1, |_| Some(restarted))
        .unwrap();
    assert_eq!(
        steps[0].error,
        Some(configuration_error(
            ConfigurationProposalError::AuthenticationRequired
        ))
    );
    assert_eq!(owner.core(group(1)).unwrap().state(), &before);
    owner
        .admit(group(1), Event::Configure(Box::new(proposed)))
        .unwrap();
    let mut checked = Vec::new();
    let steps = owner
        .advance_with_configuration_bindings(MonoTime(0), 1, |node| {
            checked.push(node);
            Some(proof.authenticated)
        })
        .unwrap();
    assert_eq!(checked, vec![node(4)]);
    assert_eq!(steps[0].error, None);
    assert_eq!(steps[0].operation, None);
    assert_eq!(steps[0].proposed, None);
    let lease = owner.take_effect().unwrap().unwrap();
    assert!(matches!(lease.effect, Effect::Persist(_)));
    assert_eq!(owner.core(group(1)).unwrap().state(), &before);
    assert_eq!(owner.core(group(1)).unwrap().membership().id(), cid(3));
    assert!(!owner.is_failed());
}

#[test]
fn local_policy_change_commits_only_with_old_and_new_recursive_predicates() {
    use voteboat::quorum::*;
    let mut log = HostLogStore::new(4);
    let (mut leader, _learner, _app, mut leader_log) = readiness_cluster(&mut log);
    let mut proposed = proposal(&[1, 2, 3], vec![]);
    let ConfigurationChange::Joint { next, .. } = &mut proposed.record.change else {
        unreachable!()
    };
    *next = Configuration::new(
        next.id(),
        Policy::new(
            Tree::Weighted(vec![
                WeightedChild {
                    weight: 1,
                    node: Tree::Majority(vec![Tree::Voter(node(1)), Tree::Voter(node(2))]),
                },
                WeightedChild {
                    weight: 5,
                    node: Tree::Voter(node(3)),
                },
            ]),
            Limits::default(),
        )
        .unwrap(),
        next.voter_stores().clone(),
        next.learners().clone(),
    )
    .unwrap();
    let effects = leader.step(Event::Configure(Box::new(proposed))).unwrap();
    let effects = persist(&mut leader, &mut leader_log, effects);
    let request_two = sent(&effects, 2);
    let request_three = sent(&effects, 3);
    let effects = leader
        .step(Event::Receive(response(
            &request_two,
            2,
            Rpc::Appended {
                success: true,
                matching_index: 3,
            },
        )))
        .unwrap();
    assert!(!effects.iter().any(|e| matches!(e, Effect::Persist(_))));
    assert_eq!(leader.state().commit_index, 2);
    let effects = leader
        .step(Event::Receive(response(
            &request_three,
            3,
            Rpc::Appended {
                success: true,
                matching_index: 3,
            },
        )))
        .unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    persist(&mut leader, &mut leader_log, effects);
    assert_eq!(leader.state().commit_index, 3);
    assert!(leader.membership().joint().is_some());
}

#[cfg(feature = "native")]
#[test]
fn failed_native_joint_barrier_fences_and_power_loss_recovers_prior_configuration() {
    use voteboat::native::log_store::*;
    let io = ModelIo::default();
    let disk = io.0.clone();
    let mut leader_log = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
    let mut learner_log = HostLogStore::new(4);
    let (mut leader, learner, app) =
        readiness_cluster_with_stores(&mut learner_log, &mut leader_log);
    let proof = ready_proof(&mut leader, &learner, &learner_log, &app);
    let effects = leader
        .step(Event::Configure(Box::new(proposal(
            &[1, 2, 4],
            vec![proof],
        ))))
        .unwrap();
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("only persistence")
    };
    let tickets = leader_log
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    leader.admitted(tickets[0]).unwrap();
    assert_eq!(leader.state().membership().unwrap().id(), cid(2));
    assert_eq!(leader.membership().id(), cid(3));
    disk.borrow_mut().fault = Fault::Sync;
    assert!(leader_log.barrier(&tickets).is_err());
    leader.storage_failed();
    assert!(leader.is_fenced());
    assert_eq!(leader.step(Event::Heartbeat), Err(RaftError::Fenced));
    drop(leader_log);
    // Simulated power-loss model discards bytes not covered by the last sync;
    // this is not a claim about physical hardware power-loss behavior.
    {
        let mut d = disk.borrow_mut();
        d.log = d.synced.clone();
        d.fault = Fault::None;
    }
    let recovered_log =
        NativeLogStore::recover(ModelIo(disk), identity(1), LogLimits::default()).unwrap();
    let recovered = Raft::recover_member(
        node(1),
        recovered_log.binding(),
        recovered_log.state(group(1)).unwrap(),
        recovered_log.limits(),
    )
    .unwrap();
    assert_eq!(recovered.membership().id(), cid(2));
    assert_eq!(recovered.state().last_index(), 2);
    assert_eq!(recovered.state().commit_index, 2);
}
