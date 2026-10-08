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

fn retired_notice() -> Message {
    let sender = StoreBinding {
        identity: identity(3),
        session: StoreSession::new(1).unwrap(),
    };
    Message {
        group: group(1),
        configuration: cid(4),
        from: node(3),
        sender,
        to: node(4),
        term: 1,
        context: RequestContext {
            origin: sender,
            sequence: 21,
        },
        rpc: Rpc::Append {
            previous_index: 3,
            previous_term: 1,
            entries: vec![],
            leader_commit: 3,
        },
    }
}
fn retirement_receipt<S: LogStore>(log: &mut S) {
    prepare(log, true, false);
    let mut core = recover(log);
    let membership = core.membership().clone();
    let hard = core.state().hard_state;
    let reset = core.election_reset_sequence();
    let effects = core.step(Event::Receive(retired_notice())).unwrap();
    assert!(matches!(&effects[..],[Effect::Persist(u)] if u.commit_index==3 && u.suffix.is_none()));
    assert_eq!(core.state().commit_index, 2);
    assert!(core.complete(&DurableLog { tickets: vec![] }).is_err());
    let effects = persist(&mut core, log, effects);
    assert!(effects.iter().any(
        |e| matches!(e,Effect::Committed(entries) if entries.len()==1 && entries[0].index==3)
    ));
    assert!(effects.iter().any(
        |e| matches!(e,Effect::Send(m) if m.to==node(3) && m.context==retired_notice().context)
    ));
    assert_eq!(core.state().commit_index, 3);
    assert_eq!(core.state().hard_state, hard);
    assert_eq!(core.membership(), &membership);
    assert_eq!(core.election_reset_sequence(), reset);
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(recover(log).state().commit_index, 3);
    assert!(matches!(
        &core.step(Event::Receive(retired_notice())).unwrap()[..],
        [Effect::Send(_)]
    ));
}
#[test]
fn host_member_records_an_exact_retired_voters_final_commit_after_its_barrier() {
    retirement_receipt(&mut HostLogStore::new(4));
}
#[cfg(feature = "native")]
#[test]
fn native_member_records_an_exact_retired_voters_final_commit_after_its_barrier() {
    use voteboat::native::log_store::*;
    retirement_receipt(
        &mut NativeLogStore::create(ModelIo::default(), identity(4), LogLimits::default()).unwrap(),
    );
}
#[cfg(feature = "native")]
#[test]
fn failed_retirement_commit_barriers_release_no_reply_and_recover_whole_boundaries() {
    use voteboat::native::log_store::*;
    for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
        let io = ModelIo::default();
        let mut log =
            NativeLogStore::create(io.clone(), identity(4), LogLimits::default()).unwrap();
        prepare(&mut log, true, false);
        let mut core = recover(&log);
        let effects = core.step(Event::Receive(retired_notice())).unwrap();
        let [Effect::Persist(update)] = effects.as_slice() else {
            panic!("persistence must precede reply")
        };
        let tickets = log
            .append_batch(vec![LogMutation::Update(update.clone())])
            .unwrap();
        core.admitted(tickets[0]).unwrap();
        assert_eq!(core.state().commit_index, 2);
        io.0.borrow_mut().fault = fault;
        assert!(log.barrier(&tickets).is_err());
        core.storage_failed();
        assert!(core.step(Event::Receive(retired_notice())).is_err());
        drop(log);
        io.0.borrow_mut().power_loss();
        let log = NativeLogStore::recover(io, identity(4), LogLimits::default()).unwrap();
        let mut core = recover(&log);
        assert!([2, 3].contains(&core.state().commit_index));
        assert_eq!(core.membership().id(), cid(4));
        let effects = core.step(Event::Receive(retired_notice())).unwrap();
        if core.state().commit_index == 2 {
            assert!(matches!(&effects[..], [Effect::Persist(_)]));
        } else {
            assert!(matches!(&effects[..], [Effect::Send(_)]));
        }
    }
}

fn authority_transfer(message: &Message) -> Message {
    #[cfg(feature = "native")]
    {
        use voteboat::{native::wire::NativeWireCodec, wire::*};
        let codec = NativeWireCodec::with_authority(WireLimits::default()).unwrap();
        let scope = WireScope {
            from: message.from,
            sender: message.sender,
            to: message.to,
        };
        let bytes = codec
            .encode_batch(scope, std::slice::from_ref(message))
            .unwrap();
        codec.decode_batch(scope, &bytes).unwrap().remove(0)
    }
    #[cfg(not(feature = "native"))]
    message.clone()
}
fn witnessed_replication<S: LogStore>(witness_log: &mut S) {
    witnessed_replication_with(witness_log, authority_transfer);
}
fn witnessed_replication_with<S: LogStore>(
    witness_log: &mut S,
    mut transfer: impl FnMut(&Message) -> Message,
) {
    use voteboat::secure::PeerIdentity;
    prepare(witness_log, false, false);
    commit(witness_log, 2);
    let mut witness = Raft::recover_member(
        node(1),
        witness_log.binding(),
        witness_log.state(group(1)).unwrap(),
        witness_log.limits(),
    )
    .unwrap();
    let mut receiver_log = HostLogStore::new(3);
    append(
        &mut receiver_log,
        vec![LogMutation::Create(bootstrap(1, 3))],
    );
    record(
        &mut receiver_log,
        2,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
        1,
    );
    let mut receiver = Raft::recover_member(
        node(3),
        receiver_log.binding(),
        receiver_log.state(group(1)).unwrap(),
        receiver_log.limits(),
    )
    .unwrap();
    let before = receiver.state().clone();
    let reset = receiver.election_reset_sequence();
    let query = receiver
        .step(Event::AuthorizeReplication {
            witness: PeerIdentity {
                node: node(1),
                store: witness_log.binding().identity,
            },
            candidate: PeerIdentity {
                node: node(4),
                store: identity(4),
            },
            configuration: cid(3),
        })
        .unwrap();
    let [Effect::Send(query)] = query.as_slice() else {
        panic!()
    };
    let reply = witness.step(Event::Receive(transfer(query))).unwrap();
    let [Effect::Send(reply)] = reply.as_slice() else {
        panic!()
    };
    assert!(matches!(
        reply.rpc,
        Rpc::AuthorityReply {
            granted: true,
            committed_index: 2,
            ..
        }
    ));
    receiver.step(Event::Receive(transfer(reply))).unwrap();
    assert_eq!(receiver.state(), &before);
    assert_eq!(receiver.election_reset_sequence(), reset);
    let sender = StoreBinding {
        identity: identity(4),
        session: StoreSession::new(1).unwrap(),
    };
    let probe = Message {
        group: group(1),
        configuration: cid(3),
        from: node(4),
        sender,
        to: node(3),
        term: 1,
        context: RequestContext {
            origin: sender,
            sequence: 10,
        },
        rpc: Rpc::Append {
            previous_index: 2,
            previous_term: 1,
            entries: vec![],
            leader_commit: 2,
        },
    };
    let result = receiver.step(Event::Receive(probe.clone())).unwrap();
    assert!(matches!(
        result.as_slice(),
        [Effect::Send(Message {
            rpc: Rpc::Appended { success: false, .. },
            ..
        })]
    ));
    receiver.storage_failed();
    assert_eq!(
        receiver.step(Event::Receive(probe.clone())),
        Err(RaftError::Fenced)
    );
    let mut receiver = Raft::recover_member(
        node(3),
        receiver_log.binding(),
        before,
        receiver_log.limits(),
    )
    .unwrap();
    assert_eq!(
        receiver.step(Event::Receive(probe)),
        Err(RaftError::WrongIdentity)
    );
}
#[test]
fn host_witness_authorizes_only_volatile_replication_through_public_contracts() {
    witnessed_replication(&mut HostLogStore::new(1));
}
#[cfg(feature = "native")]
#[test]
fn native_witness_authorizes_only_volatile_replication_through_public_contracts() {
    use voteboat::native::log_store::*;
    witnessed_replication(
        &mut NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap(),
    );
}
#[test]
fn term_zero_replica_can_query_without_advancing_hard_state() {
    use voteboat::secure::PeerIdentity;
    let mut log = HostLogStore::new(2);
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    let state = log.state(group(1)).unwrap();
    let mut receiver = Raft::recover(node(2), log.binding(), state.clone(), log.limits()).unwrap();
    let result = receiver
        .step(Event::AuthorizeReplication {
            witness: PeerIdentity {
                node: node(1),
                store: identity(1),
            },
            candidate: PeerIdentity {
                node: node(4),
                store: identity(4),
            },
            configuration: cid(2),
        })
        .unwrap();
    assert!(matches!(
        result.as_slice(),
        [Effect::Send(Message { term: 1, .. })]
    ));
    assert_eq!(receiver.state(), &state);
    receiver
        .step(Event::CancelReplicationAuthorization)
        .unwrap();
}

#[cfg(feature = "tls")]
mod authenticated_witness {
    use super::*;
    use std::{
        net::{TcpListener, TcpStream},
        time::{Duration, Instant},
    };
    use voteboat::{
        native::{
            log_store::*, outbound::NativeOutbound, tls::*, transport::NativePeerTransport,
            wire::NativeWireCodec,
        },
        outbound::*,
        runtime::MonoTime,
        secure::*,
        transport::*,
        wire::WireLimits,
    };
    fn identities(log: &impl LogStore) -> (LocalIdentity, LocalIdentity) {
        (
            LocalIdentity {
                node: node(3),
                store: HostLogStore::new(3).binding(),
            },
            LocalIdentity {
                node: node(1),
                store: log.binding(),
            },
        )
    }
    fn run<S: SecureSession>(log: &mut impl LogStore, mut a: S, mut b: S) {
        let clock = Instant::now();
        while a.state() != SessionState::Ready || b.state() != SessionState::Ready {
            let now = MonoTime(clock.elapsed().as_millis() as u64);
            a.poll(now, SessionPollBudget::default()).unwrap();
            b.poll(now, SessionPollBudget::default()).unwrap();
            assert!(clock.elapsed() < Duration::from_secs(5));
            std::thread::park_timeout(Duration::from_millis(1));
        }
        assert_eq!(require_authenticated(&a).unwrap().wire_version, 3);
        assert_eq!(require_authenticated(&b).unwrap().wire_version, 3);
        let queue = |local: LocalIdentity| {
            NativeOutbound::new(
                OutboundBinding {
                    node: local.node,
                    store: local.store,
                    generation: OutboundGeneration::new(1).unwrap(),
                },
                OutboundLimits::default(),
            )
            .unwrap()
        };
        let mut qa = queue(a.binding().unwrap().local);
        let mut qb = queue(b.binding().unwrap().local);
        let codec = || NativeWireCodec::with_authority(WireLimits::default()).unwrap();
        let mut a = NativePeerTransport::new(a, codec(), &qa, TransportLimits::default()).unwrap();
        let mut b = NativePeerTransport::new(b, codec(), &qb, TransportLimits::default()).unwrap();
        witnessed_replication_with(log, |message| {
            let forward = message.from == node(3);
            if forward {
                qa.submit(vec![message.clone()]).unwrap();
                a.submit(qa.poll(1).pop().unwrap()).unwrap();
            } else {
                qb.submit(vec![message.clone()]).unwrap();
                b.submit(qb.poll(1).pop().unwrap()).unwrap();
            }
            loop {
                let now = MonoTime(clock.elapsed().as_millis() as u64);
                a.poll(now, TransportPollBudget::default()).unwrap();
                b.poll(now, TransportPollBudget::default()).unwrap();
                let done = if forward {
                    a.usage().completion && b.received_info().is_some()
                } else {
                    b.usage().completion && a.received_info().is_some()
                };
                if done {
                    break;
                }
                assert!(clock.elapsed() < Duration::from_secs(10));
                std::thread::park_timeout(Duration::from_millis(1));
            }
            let received = if forward {
                b.take_received().unwrap()
            } else {
                a.take_received().unwrap()
            };
            assert_eq!(received.messages.as_slice(), std::slice::from_ref(message));
            let completion = if forward {
                a.take_send().unwrap()
            } else {
                b.take_send().unwrap()
            };
            assert_eq!(completion.result, LocalSendResult::Sent);
            if forward {
                qa.complete(completion.batch, completion.result).unwrap();
            } else {
                qb.complete(completion.batch, completion.result).unwrap();
            }
            assert_eq!(qa.usage().batches + qb.usage().batches, 0);
            received.messages.into_iter().next().unwrap()
        });
    }
    #[test]
    fn native_durable_witness_exchange_uses_real_tcp_tls_wire_three() {
        let mut log =
            NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap();
        let (receiver, witness) = identities(&log);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        client.set_nodelay(true).unwrap();
        server.set_nodelay(true).unwrap();
        let config = |n| support::tls::configuration(n).with_wire_version(3).unwrap();
        let a = NativeTlsSession::client_tcp(
            client,
            &config(3),
            receiver,
            support::tls::peer(witness),
            SecureSessionGeneration::new(1).unwrap(),
            SessionLimits::default(),
            MonoTime(0),
        )
        .unwrap();
        let b = NativeTlsSession::server_tcp(
            server,
            &config(1),
            witness,
            support::tls::peer(receiver),
            SecureSessionGeneration::new(1).unwrap(),
            SessionLimits::default(),
            MonoTime(0),
        )
        .unwrap();
        run(&mut log, a, b);
    }
    #[cfg(feature = "quic")]
    #[test]
    fn native_durable_witness_exchange_uses_real_quic_wire_three() {
        use std::net::UdpSocket;
        use voteboat::native::quic::*;
        let mut log =
            NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap();
        let (receiver, witness) = identities(&log);
        let a = UdpSocket::bind("127.0.0.1:0").unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").unwrap();
        let aa = a.local_addr().unwrap();
        let ba = b.local_addr().unwrap();
        let config = |n| support::tls::configuration(n).with_wire_version(3).unwrap();
        let options = |local, remote, address| QuicSessionOptions {
            local,
            peer: support::tls::peer(remote),
            remote: address,
            generation: SecureSessionGeneration::new(1).unwrap(),
            limits: SessionLimits::default(),
        };
        let a =
            NativeQuicSession::client(a, &config(3), options(receiver, witness, ba), MonoTime(0))
                .unwrap();
        let b =
            NativeQuicSession::server(b, &config(1), options(witness, receiver, aa), MonoTime(0))
                .unwrap();
        run(&mut log, a, b);
    }
}
