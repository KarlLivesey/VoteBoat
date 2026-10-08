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
//! Host-authorized dynamic recovery uses the same public core/storage seams.
//! Imported committed assignments are fixture premises, not network certificates.
mod support;
use support::*;
use voteboat::{
    application::*, identity::*, log::*, membership::*, quorum::*, raft::*, snapshot::*,
};
fn cid(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn configuration(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    Configuration::new(
        cid(id),
        Policy::new(
            Tree::Majority(voters.iter().map(|n| Tree::Voter(node(*n))).collect()),
            Limits::default(),
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
    .unwrap()
}
fn record<S: LogStore>(log: &mut S, operation: u128, change: ConfigurationChange, commit: u64) {
    let state = log.state(group(1)).unwrap();
    let entry = LogEntry {
        index: state.last_index() + 1,
        term: state.hard_state.term.max(1),
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(operation).unwrap(),
            expected: state.membership().unwrap().id(),
            change,
        })),
    };
    let mut mutation = update(
        &state,
        entry.term,
        commit,
        Some(Suffix {
            from: entry.index,
            entries: vec![entry],
        }),
    );
    if let LogMutation::Update(u) = &mut mutation {
        u.hard_state.voted_for = state.hard_state.voted_for;
    }
    append(log, vec![mutation]);
}
fn commit<S: LogStore>(log: &mut S, index: u64) {
    let state = log.state(group(1)).unwrap();
    let mut mutation = update(&state, state.hard_state.term, index, None);
    if let LogMutation::Update(u) = &mut mutation {
        u.hard_state = state.hard_state;
    }
    append(log, vec![mutation]);
}
fn prepare<S: LogStore>(log: &mut S, final_entry: bool, final_commit: bool) {
    append(log, vec![LogMutation::Create(bootstrap(1, 3))]);
    record(
        log,
        2,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
        1,
    );
    record(
        log,
        3,
        ConfigurationChange::Joint {
            id: cid(3),
            next: configuration(4, &[1, 2, 4], &[3, 5]),
        },
        1,
    );
    if final_entry {
        commit(log, 2);
        record(
            log,
            3,
            ConfigurationChange::Final { id: cid(4) },
            if final_commit { 3 } else { 2 },
        );
    }
}
fn recover<S: LogStore>(log: &S) -> Raft {
    Raft::recover_member(
        node(4),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap()
}
fn persist<S: LogStore>(core: &mut Raft, log: &mut S, effects: Vec<Effect>) -> Vec<Effect> {
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("expected exact persistence dependency")
    };
    persist_effect(core, log, update.clone()).unwrap()
}
fn vote_reply(request: &Message) -> Event {
    Event::Receive(Message {
        group: request.group,
        configuration: request.configuration,
        from: request.to,
        to: request.from,
        sender: StoreBinding {
            identity: identity(request.to.get() as u128),
            session: StoreSession::new(1).unwrap(),
        },
        term: request.term,
        context: request.context,
        rpc: Rpc::Voted { granted: true },
    })
}
fn election<S: LogStore>(log: &mut S, final_entry: bool) {
    prepare(log, final_entry, false);
    let state = log.state(group(1)).unwrap();
    assert!(Raft::recover(node(4), log.binding(), state.clone(), log.limits()).is_err());
    assert!(Raft::recover_learner(node(4), log.binding(), state, log.limits()).is_err());
    let mut core = recover(log);
    assert!(core.local_voter());
    assert_eq!(core.role(), Role::Follower);
    let effects = core.step(Event::Campaign).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    assert_eq!(core.state().hard_state.voted_for, None);
    let effects = persist(&mut core, log, effects);
    assert_eq!(core.role(), Role::Candidate);
    assert_eq!(
        core.state().ballot_origin.unwrap().configuration,
        if final_entry { cid(4) } else { cid(3) }
    );
    let request = |peer| {
        effects
            .iter()
            .find_map(|e| match e {
                Effect::Send(m) if m.to == node(peer) && matches!(m.rpc, Rpc::Vote { .. }) => {
                    Some(m.clone())
                }
                _ => None,
            })
            .unwrap()
    };
    // Self 4 + peer 1 satisfies new majority but not old majority.
    let next = core.step(vote_reply(&request(1))).unwrap();
    if final_entry {
        assert!(matches!(next.as_slice(), [Effect::Persist(_)]));
    } else {
        assert!(next.is_empty());
        assert_eq!(core.role(), Role::Candidate);
        let next = core.step(vote_reply(&request(2))).unwrap();
        assert!(matches!(next.as_slice(), [Effect::Persist(_)]));
    }
}
#[test]
fn host_joint_and_uncommitted_final_restart_reconstruct_distinct_election_predicates() {
    election(&mut HostLogStore::new(4), false);
    election(&mut HostLogStore::new(4), true);
}
#[test]
fn dynamic_recovery_requires_exact_assignment_in_both_views_and_valid_history() {
    let mut log = HostLogStore::new(4);
    prepare(&mut log, false, false);
    let state = log.state(group(1)).unwrap();
    for invalid in 0..6 {
        let mut state = state.clone();
        let mut binding = log.binding();
        let mut local = node(4);
        match invalid {
            0 => state.commit_index = 0,
            1 => binding.identity = identity(44),
            2 => local = node(99),
            3 => state.entries[1].index = 8,
            4 => state.entries[1].term = 9,
            _ => {
                state.ballot_origin = Some(BallotOrigin {
                    configuration: cid(3),
                    candidate_store: identity(4),
                })
            }
        }
        assert!(
            matches!(
                Raft::recover_member(local, binding, state, log.limits()),
                Err(RaftError::InvalidRecovery)
            ),
            "case {invalid}"
        );
    }
    let mut log = HostLogStore::new(4);
    prepare(&mut log, true, true);
    record(
        &mut log,
        5,
        ConfigurationChange::Learners(configuration(5, &[1, 2, 4], &[3, 5, 6])),
        3,
    );
    let state = log.state(group(1)).unwrap();
    assert!(
        Raft::recover_member(
            node(6),
            StoreBinding {
                identity: identity(6),
                ..log.binding()
            },
            state,
            log.limits()
        )
        .is_err(),
        "uncommitted first assignment"
    );
    let mut log = HostLogStore::new(5);
    prepare(&mut log, true, true);
    record(
        &mut log,
        5,
        ConfigurationChange::Learners(configuration(5, &[1, 2, 4], &[3])),
        3,
    );
    assert!(
        Raft::recover_member(
            node(5),
            log.binding(),
            log.state(group(1)).unwrap(),
            log.limits()
        )
        .is_err(),
        "accepted removal"
    );
}
#[test]
fn recovered_dynamic_learner_cannot_vote_campaign_or_serve_reads() {
    let mut log = HostLogStore::new(5);
    prepare(&mut log, true, true);
    let mut core = Raft::recover_member(
        node(5),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    assert!(!core.local_voter());
    assert_eq!(core.step(Event::Campaign), Err(RaftError::NotVoter));
    assert_eq!(
        core.step(Event::Read {
            request: ReadRequestId::new(1).unwrap()
        }),
        Err(RaftError::NotLeader)
    );
    let sender = StoreBinding {
        identity: identity(1),
        session: StoreSession::new(1).unwrap(),
    };
    let effects = core
        .step(Event::Receive(Message {
            group: group(1),
            configuration: cid(4),
            from: node(1),
            to: node(5),
            sender,
            term: 2,
            context: RequestContext {
                origin: sender,
                sequence: 1,
            },
            rpc: Rpc::Vote {
                last_index: 3,
                last_term: 1,
            },
        }))
        .unwrap();
    let effects = persist(&mut core, &mut log, effects);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: false },
            ..
        })]
    ));
    assert_eq!(core.state().hard_state.voted_for, None);
}
fn snapshots() -> support::snapshot::HostSnapshots {
    let mut snapshots = support::snapshot::HostSnapshots::new();
    snapshots.identity.store = identity(4);
    snapshots.binding.identity = identity(4);
    snapshots
}
fn checkpoint<L: LogStore, S: SnapshotRetention>(log: &mut L, snapshots: &mut S) {
    prepare(log, true, true);
    let state = log.state(group(1)).unwrap();
    append(
        log,
        vec![update(
            &state,
            2,
            4,
            Some(Suffix {
                from: 4,
                entries: vec![LogEntry {
                    index: 4,
                    term: 2,
                    payload: EntryPayload::Command {
                        operation: OperationId::new(900).unwrap(),
                        bytes: 7i64.to_le_bytes().to_vec(),
                    },
                }],
            }),
        )],
    );
    let mut core = recover(log);
    let mut app = Counter::new(16).unwrap();
    app.apply_batch(core.replay_committed()).unwrap();
    let receipt = checkpoint_application(&core, &app, snapshots).unwrap();
    compact_replica(&mut core, log, snapshots, &app, receipt.reference()).unwrap();
    assert!(Raft::recover_member(
        node(4),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits()
    )
    .is_err());
    let mut restored = Counter::new(16).unwrap();
    let (core, replay) =
        recover_member_replica(node(4), group(1), log, snapshots, &mut restored).unwrap();
    assert!(core.local_voter());
    assert_eq!(core.membership().id(), cid(4));
    assert_eq!(replay.checkpoint_index, 4);
    assert_eq!(restored.read_applied(4).unwrap(), 7);
}
#[test]
fn host_checkpoint_restores_dynamic_membership_and_application_before_exposure() {
    let mut log = HostLogStore::new(4);
    let mut snapshots = snapshots();
    checkpoint(&mut log, &mut snapshots);
    snapshots.current = None;
    snapshots.pins.clear();
    let mut app = Counter::new(16).unwrap();
    assert!(recover_member_replica(node(4), group(1), &log, &mut snapshots, &mut app).is_err());
    assert_eq!(app.applied_index(), 0);
}

#[test]
fn rollback_restores_joint_rules_and_historical_ballot_survives_final_restart() {
    let mut log = HostLogStore::new(4);
    prepare(&mut log, false, false);
    commit(&mut log, 2);
    let mut core = recover(&log);
    let sender = StoreBinding {
        identity: identity(1),
        session: StoreSession::new(1).unwrap(),
    };
    let effects = core
        .step(Event::Receive(Message {
            group: group(1),
            configuration: cid(3),
            from: node(1),
            to: node(4),
            sender,
            term: 2,
            context: RequestContext {
                origin: sender,
                sequence: 1,
            },
            rpc: Rpc::Vote {
                last_index: 2,
                last_term: 1,
            },
        }))
        .unwrap();
    persist(&mut core, &mut log, effects);
    let promise = core.state().ballot_origin;
    record(&mut log, 3, ConfigurationChange::Final { id: cid(4) }, 2);
    let mut core = recover(&log);
    assert_eq!(core.membership().id(), cid(4));
    assert_eq!(core.state().ballot_origin, promise);
    let sender = StoreBinding {
        identity: identity(2),
        session: StoreSession::new(1).unwrap(),
    };
    let effects = core
        .step(Event::Receive(Message {
            group: group(1),
            configuration: cid(4),
            from: node(2),
            to: node(4),
            sender,
            term: 2,
            context: RequestContext {
                origin: sender,
                sequence: 2,
            },
            rpc: Rpc::Vote {
                last_index: 3,
                last_term: 2,
            },
        }))
        .unwrap();
    assert!(matches!(
        effects.as_slice(),
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: false },
            ..
        })]
    ));
    let state = log.state(group(1)).unwrap();
    let mut rollback = update(
        &state,
        2,
        2,
        Some(Suffix {
            from: 3,
            entries: vec![],
        }),
    );
    if let LogMutation::Update(u) = &mut rollback {
        u.hard_state = state.hard_state;
    }
    append(&mut log, vec![rollback]);
    let core = recover(&log);
    assert!(core.membership().joint().is_some());
    assert_eq!(core.membership().id(), cid(3));
    assert_eq!(core.state().ballot_origin, promise);
    assert_eq!(core.state().hard_state.voted_for, Some(node(1)));
}

#[test]
fn recovered_final_weighted_policy_is_used_instead_of_bootstrap_majority() {
    let mut log = HostLogStore::new(4);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    record(
        &mut log,
        2,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
        1,
    );
    let next = Configuration::new(
        cid(4),
        Policy::new(
            Tree::Weighted(vec![
                WeightedChild {
                    weight: 1,
                    node: Tree::Voter(node(1)),
                },
                WeightedChild {
                    weight: 1,
                    node: Tree::Voter(node(2)),
                },
                WeightedChild {
                    weight: 3,
                    node: Tree::Voter(node(4)),
                },
            ]),
            Limits::default(),
        )
        .unwrap(),
        configuration(4, &[1, 2, 4], &[3, 5]).voter_stores().clone(),
        configuration(4, &[1, 2, 4], &[3, 5]).learners().clone(),
    )
    .unwrap();
    record(
        &mut log,
        3,
        ConfigurationChange::Joint { id: cid(3), next },
        1,
    );
    commit(&mut log, 2);
    record(&mut log, 3, ConfigurationChange::Final { id: cid(4) }, 3);
    let mut core = recover(&log);
    let effects = core.step(Event::Campaign).unwrap();
    let effects = persist(&mut core, &mut log, effects);
    assert_eq!(
        core.role(),
        Role::Leader,
        "durable self vote satisfies new weighted quorum"
    );
    assert!(
        matches!(effects.as_slice(), [Effect::Persist(_)]),
        "leader no-op still requires its own durability"
    );
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::native::{log_store::*, snapshot_store::*};
    #[test]
    fn native_store_uses_the_same_dynamic_election_contract() {
        for final_entry in [false, true] {
            let mut log =
                NativeLogStore::create(ModelIo::default(), identity(4), LogLimits::default())
                    .unwrap();
            election(&mut log, final_entry);
        }
    }
    #[test]
    fn native_files_reclaim_and_restart_dynamic_member_with_verified_application() {
        let root = std::env::temp_dir().join(format!("voteboat-member42-{}", std::process::id()));
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
            FileSnapshotIo::create(root.join("snapshots")).unwrap(),
            si,
            SnapshotLimits::default(),
        )
        .unwrap();
        checkpoint(&mut log, &mut snapshots);
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
            FileSnapshotIo::open(root.join("snapshots")).unwrap(),
            si,
            SnapshotLimits::default(),
        )
        .unwrap();
        assert_eq!(log.state(group(1)).unwrap(), expected);
        let mut app = Counter::new(16).unwrap();
        let (core, restored) =
            recover_member_replica(node(4), group(1), &log, &mut snapshots, &mut app).unwrap();
        assert!(core.local_voter());
        assert_eq!(core.membership().id(), cid(4));
        assert_eq!(restored.checkpoint_index, 4);
        assert_eq!(app.read_applied(4).unwrap(), 7);
        let receipt = app
            .apply_batch(&[LogEntry {
                index: 5,
                term: 3,
                payload: EntryPayload::Command {
                    operation: OperationId::new(900).unwrap(),
                    bytes: 7i64.to_le_bytes().to_vec(),
                },
            }])
            .unwrap();
        assert!(receipt[0].duplicate);
        assert_eq!(receipt[0].outcome, CounterOutcome::Value(7));
        drop(snapshots);
        drop(log);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn written_joint_and_each_torn_frame_never_create_partial_voting_membership() {
        let io = ModelIo::default();
        let mut log =
            NativeLogStore::create(io.clone(), identity(4), LogLimits::default()).unwrap();
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        record(
            &mut log,
            2,
            ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
            1,
        );
        let old = log.state(group(1)).unwrap();
        let before = io.0.borrow().log.len();
        let manifest = io.0.borrow().manifest.clone();
        let mutation = update(
            &old,
            1,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![LogEntry {
                    index: 2,
                    term: 1,
                    payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                        operation: OperationId::new(3).unwrap(),
                        expected: cid(2),
                        change: ConfigurationChange::Joint {
                            id: cid(3),
                            next: configuration(4, &[1, 2, 4], &[3, 5]),
                        },
                    })),
                }],
            }),
        );
        let tickets = log.append_batch(vec![mutation]).unwrap();
        assert!(
            !recover(&log).local_voter(),
            "Written is not a recovered voting assignment"
        );
        let full = io.0.borrow().log.clone();
        log.barrier(&tickets).unwrap();
        let expected = log.state(group(1)).unwrap();
        assert!(recover(&log).local_voter());
        drop(log);
        for cut in before..=full.len() {
            let image = ModelIo::default();
            image.0.borrow_mut().log = full[..cut].to_vec();
            image.0.borrow_mut().manifest = manifest.clone();
            let log = NativeLogStore::recover(image, identity(4), LogLimits::default()).unwrap();
            assert_eq!(
                log.state(group(1)).unwrap(),
                if cut == full.len() {
                    expected.clone()
                } else {
                    old.clone()
                },
                "cut {cut}"
            );
            assert_eq!(recover(&log).local_voter(), cut == full.len(), "cut {cut}");
        }
    }
}

#[test]
fn committed_replacement_assignment_authorizes_only_the_new_exact_store() {
    let mut log = HostLogStore::new(44);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    let replacement = Configuration::new(
        cid(2),
        bootstrap(1, 3).policy,
        bootstrap(1, 3).voter_stores,
        [(node(4), identity(44))].into_iter().collect(),
    )
    .unwrap();
    record(&mut log, 2, ConfigurationChange::Learners(replacement), 1);
    let mut stores = configuration(4, &[1, 2, 4], &[3]).voter_stores().clone();
    stores.insert(node(4), identity(44));
    let target = Configuration::new(
        cid(4),
        configuration(4, &[1, 2, 4], &[3]).policy().clone(),
        stores,
        [(node(3), identity(3))].into_iter().collect(),
    )
    .unwrap();
    record(
        &mut log,
        3,
        ConfigurationChange::Joint {
            id: cid(3),
            next: target,
        },
        1,
    );
    let state = log.state(group(1)).unwrap();
    assert!(recover(&log).local_voter());
    assert!(Raft::recover_member(
        node(4),
        StoreBinding {
            identity: identity(4),
            ..log.binding()
        },
        state,
        log.limits()
    )
    .is_err());
}

#[cfg(feature = "native")]
#[test]
fn failed_joint_barriers_recover_only_a_complete_old_or_new_membership() {
    use voteboat::native::log_store::*;
    for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
        let io = ModelIo::default();
        let mut log =
            NativeLogStore::create(io.clone(), identity(4), LogLimits::default()).unwrap();
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        record(
            &mut log,
            2,
            ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
            1,
        );
        let old = log.state(group(1)).unwrap();
        let mutation = update(
            &old,
            1,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![LogEntry {
                    index: 2,
                    term: 1,
                    payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                        operation: OperationId::new(3).unwrap(),
                        expected: cid(2),
                        change: ConfigurationChange::Joint {
                            id: cid(3),
                            next: configuration(4, &[1, 2, 4], &[3, 5]),
                        },
                    })),
                }],
            }),
        );
        let mut expected = [(group(1), old.clone())].into();
        apply_batch(&mut expected, std::slice::from_ref(&mutation), log.limits()).unwrap();
        let tickets = log.append_batch(vec![mutation]).unwrap();
        io.0.borrow_mut().fault = fault;
        assert!(log.barrier(&tickets).is_err());
        drop(log);
        io.0.borrow_mut().power_loss();
        let log = NativeLogStore::recover(io, identity(4), LogLimits::default()).unwrap();
        let state = log.state(group(1)).unwrap();
        assert!(state == old || state == expected[&group(1)]);
        let core = recover(&log);
        assert_eq!(
            core.local_voter(),
            state.membership().unwrap().joint().is_some()
        );
        assert_eq!(core.role(), Role::Follower);
    }
}
