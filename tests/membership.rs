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
mod support;
use std::collections::{BTreeMap, BTreeSet};
use support::*;
use voteboat::{identity::*, log::*, membership::*, quorum::*, raft::*};

fn cid(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn config(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
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
fn record(index: u64, operation: u128, expected: u64, change: ConfigurationChange) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(operation).unwrap(),
            expected: cid(expected),
            change,
        })),
    }
}
fn learners() -> LogEntry {
    record(
        1,
        100,
        1,
        ConfigurationChange::Learners(config(2, &[1, 2, 3], &[4, 5])),
    )
}
fn joint() -> LogEntry {
    record(
        2,
        101,
        2,
        ConfigurationChange::Joint {
            id: cid(3),
            next: config(4, &[3, 4, 5], &[1]),
        },
    )
}
fn final_record() -> LogEntry {
    record(3, 101, 3, ConfigurationChange::Final { id: cid(4) })
}
fn replay(entries: &[LogEntry], commit: u64) -> Result<Membership, MembershipError> {
    Membership::replay(&bootstrap(1, 3), entries, commit)
}
fn set(nodes: &[u64]) -> BTreeSet<NodeId> {
    nodes.iter().map(|n| node(*n)).collect()
}

#[test]
fn accepted_log_activation_uses_both_policies_before_joint_commit() {
    let stable = replay(&[learners()], 0).unwrap();
    assert_eq!(stable.id(), cid(2));
    assert!(!stable.is_voter(node(4)));
    assert!(!stable.is_satisfied(&set(&[4, 5])));
    let joint = replay(&[learners(), joint()], 1).unwrap();
    assert_eq!(joint.id(), cid(3));
    assert_eq!(joint.joint().unwrap().index, 2);
    assert!(!joint.is_satisfied(&set(&[1, 2])));
    assert!(!joint.is_satisfied(&set(&[4, 5])));
    assert!(joint.is_satisfied(&set(&[1, 3, 4])));
    let final_state = replay(&[learners(), self::joint(), final_record()], 2).unwrap();
    assert_eq!(final_state.id(), cid(4));
    assert!(final_state.joint().is_none());
    assert!(!final_state.is_voter(node(1)));
    assert!(final_state.is_satisfied(&set(&[4, 5])));
    assert!(!final_state.is_satisfied(&set(&[1, 2])));
}
#[test]
fn transition_grammar_rejects_overlap_early_final_and_operation_reuse() {
    assert_eq!(
        replay(&[learners(), joint()], 0),
        Err(MembershipError::TransitionInProgress)
    );
    assert_eq!(
        replay(&[learners(), joint(), final_record()], 1),
        Err(MembershipError::JointNotCommitted)
    );
    assert_eq!(
        replay(
            &[record(1, 101, 1, ConfigurationChange::Final { id: cid(4) })],
            0
        ),
        Err(MembershipError::InvalidFinal)
    );
    for (operation, id, expected, error) in [
        (102, 4, 3, MembershipError::InvalidFinal),
        (101, 5, 3, MembershipError::InvalidFinal),
        (101, 4, 2, MembershipError::StaleConfiguration),
    ] {
        assert_eq!(
            replay(
                &[
                    learners(),
                    joint(),
                    record(
                        3,
                        operation,
                        expected,
                        ConfigurationChange::Final { id: cid(id) }
                    )
                ],
                2
            ),
            Err(error)
        );
    }
    let second = record(
        4,
        102,
        4,
        ConfigurationChange::Joint {
            id: cid(5),
            next: config(6, &[3, 4, 5], &[]),
        },
    );
    assert_eq!(
        replay(&[learners(), joint(), final_record(), second.clone()], 2),
        Err(MembershipError::TransitionInProgress)
    );
    assert!(replay(&[learners(), joint(), final_record(), second], 3).is_ok());
    let reused = record(
        4,
        101,
        4,
        ConfigurationChange::Learners(config(5, &[3, 4, 5], &[])),
    );
    assert_eq!(
        replay(&[learners(), joint(), final_record(), reused], 3),
        Err(MembershipError::ReusedOperation)
    );
    let overlap = record(
        3,
        102,
        3,
        ConfigurationChange::Learners(config(5, &[1, 2, 3], &[])),
    );
    assert_eq!(
        replay(&[learners(), joint(), overlap], 2),
        Err(MembershipError::TransitionInProgress)
    );
}
#[test]
fn promotion_requires_committed_matching_learner_identity_and_no_hot_voter_edit() {
    let direct = record(
        1,
        101,
        1,
        ConfigurationChange::Joint {
            id: cid(3),
            next: config(4, &[3, 4, 5], &[]),
        },
    );
    assert_eq!(replay(&[direct], 0), Err(MembershipError::UnpreparedVoter));
    let mut stores = config(4, &[3, 4, 5], &[]).voter_stores().clone();
    stores.insert(node(4), identity(44));
    let wrong_store = Configuration::new(
        cid(4),
        config(4, &[3, 4, 5], &[]).policy().clone(),
        stores,
        BTreeMap::new(),
    )
    .unwrap();
    let wrong = record(
        2,
        101,
        2,
        ConfigurationChange::Joint {
            id: cid(3),
            next: wrong_store,
        },
    );
    assert_eq!(
        replay(&[learners(), wrong], 1),
        Err(MembershipError::UnpreparedVoter)
    );
    let mut stores = config(4, &[1, 2, 3], &[]).voter_stores().clone();
    stores.insert(node(1), identity(11));
    let changed = Configuration::new(
        cid(4),
        config(4, &[1, 2, 3], &[]).policy().clone(),
        stores,
        BTreeMap::new(),
    )
    .unwrap();
    assert_eq!(
        replay(
            &[record(
                1,
                101,
                1,
                ConfigurationChange::Joint {
                    id: cid(3),
                    next: changed
                }
            )],
            0
        ),
        Err(MembershipError::ChangedVoterStore)
    );
    assert_eq!(
        replay(
            &[record(
                1,
                100,
                1,
                ConfigurationChange::Learners(config(2, &[1, 2], &[3]))
            )],
            0
        ),
        Err(MembershipError::LearnerChangesVoters)
    );
    assert!(Configuration::new(
        cid(2),
        config(2, &[1, 2, 3], &[]).policy().clone(),
        config(2, &[1, 2, 3], &[]).voter_stores().clone(),
        [(node(1), identity(1))].into()
    )
    .is_err());
    assert!(Configuration::new(
        cid(2),
        config(2, &[1, 2, 3], &[]).policy().clone(),
        [(node(1), identity(1))].into(),
        BTreeMap::new()
    )
    .is_err());
    assert_eq!(
        replay(
            &[record(
                1,
                100,
                1,
                ConfigurationChange::Learners(config(1, &[1, 2, 3], &[]))
            )],
            0
        ),
        Err(MembershipError::StaleConfiguration)
    );
}
#[test]
fn arbitrary_weighted_policy_changes_require_joint_rules_even_with_same_voters() {
    let base = bootstrap(1, 3);
    let weighted = Policy::new(
        Tree::Weighted(vec![
            WeightedChild {
                weight: 5,
                node: Tree::Voter(node(1)),
            },
            WeightedChild {
                weight: 1,
                node: Tree::Majority(vec![Tree::Voter(node(2)), Tree::Voter(node(3))]),
            },
        ]),
        Limits::default(),
    )
    .unwrap();
    let target = Configuration::new(
        cid(3),
        weighted.clone(),
        base.voter_stores.clone(),
        BTreeMap::new(),
    )
    .unwrap();
    let entry = record(
        1,
        101,
        1,
        ConfigurationChange::Joint {
            id: cid(2),
            next: target,
        },
    );
    let effective = replay(&[entry], 0).unwrap();
    for mask in 0..8 {
        let acks = (1..=3)
            .filter(|n| mask & (1 << (n - 1)) != 0)
            .map(node)
            .collect();
        assert_eq!(
            effective.is_satisfied(&acks),
            base.policy.is_satisfied(&acks) && weighted.is_satisfied(&acks)
        );
    }
    assert!(!effective.is_satisfied(&set(&[1])));
    assert!(!effective.is_satisfied(&set(&[2, 3])));
    assert!(effective.is_satisfied(&set(&[1, 2])));
}
#[test]
fn joint_frontier_matches_exhaustive_prefix_evaluation() {
    let effective = replay(&[learners(), joint()], 1).unwrap();
    for code in 0..3125u64 {
        let mut digits = code;
        let mut prefixes = BTreeMap::new();
        for n in 1..=5 {
            prefixes.insert(node(n), digits % 5);
            digits /= 5;
        }
        let slow = (0..=4)
            .filter(|index| {
                let acks = prefixes
                    .iter()
                    .filter(|(_, prefix)| **prefix >= *index)
                    .map(|(n, _)| *n)
                    .collect();
                effective.is_satisfied(&acks)
            })
            .max()
            .unwrap();
        assert_eq!(effective.frontier(&prefixes), slow);
    }
    assert_eq!(
        effective.frontier(&[(node(1), 99), (node(2), 99), (node(4), 3), (node(5), 3)].into()),
        3
    );
}
fn journal_conformance<S: LogStore>(mut store: S) {
    append(
        &mut store,
        vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ],
    );
    let initial = store.state(group(1)).unwrap();
    let first = store
        .append_batch(vec![update(
            &initial,
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![learners()],
            }),
        )])
        .unwrap();
    assert_eq!(store.state(group(1)).unwrap(), initial); // Written is not Durable.
    store.barrier(&first).unwrap();
    let staged = store.state(group(1)).unwrap();
    assert_eq!(staged.membership().unwrap().id(), cid(2));
    let tickets = store
        .append_batch(vec![update(
            &staged,
            1,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![joint()],
            }),
        )])
        .unwrap();
    store.barrier(&tickets).unwrap();
    let joint_state = store.state(group(1)).unwrap();
    assert_eq!(joint_state.membership().unwrap().id(), cid(3));
    let second = store.state(group(2)).unwrap();
    assert!(store
        .append_batch(vec![
            update(&second, 2, 0, None),
            update(
                &joint_state,
                1,
                1,
                Some(Suffix {
                    from: 3,
                    entries: vec![final_record()]
                })
            )
        ])
        .is_err());
    assert_eq!(store.state(group(2)).unwrap(), second);
    assert_eq!(store.state(group(1)).unwrap(), joint_state);
    append(
        &mut store,
        vec![update(
            &joint_state,
            1,
            2,
            Some(Suffix {
                from: 3,
                entries: vec![final_record()],
            }),
        )],
    );
    let final_state = store.state(group(1)).unwrap();
    assert_eq!(final_state.membership().unwrap().id(), cid(4));
    // Roll back an uncommitted final record to the surviving joint configuration.
    append(
        &mut store,
        vec![update(
            &final_state,
            2,
            2,
            Some(Suffix {
                from: 3,
                entries: vec![entry(3, 2, 3)],
            }),
        )],
    );
    let rolled_back = store.state(group(1)).unwrap();
    assert_eq!(rolled_back.membership().unwrap().id(), cid(3));
    assert!(rolled_back.generation > final_state.generation);
    assert!(store
        .fetch_range(group(1), final_state.generation, 3, 1, 1024)
        .is_err());
    // A committed joint cannot be removed to regain old-only rules.
    assert!(store
        .append_batch(vec![update(
            &rolled_back,
            3,
            2,
            Some(Suffix {
                from: 2,
                entries: vec![]
            })
        )])
        .is_err());
}
#[test]
fn host_store_configuration_journal_conformance() {
    journal_conformance(HostLogStore::new(1));
}
#[cfg(feature = "native")]
#[test]
fn native_store_configuration_journal_conformance() {
    use voteboat::native::log_store::NativeLogStore;
    journal_conformance(
        NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap(),
    );
}
#[test]
fn replacement_removes_uncommitted_joint_and_reconstructs_previous_learners() {
    let mut store = HostLogStore::new(1);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let initial = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &initial,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![learners(), joint()],
            }),
        )],
    );
    let old = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &old,
            2,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![entry(2, 2, 7)],
            }),
        )],
    );
    let surviving = store.state(group(1)).unwrap();
    assert_eq!(surviving.membership().unwrap().id(), cid(2));
    assert!(!surviving.membership().unwrap().is_voter(node(4)));
}
#[test]
fn static_core_refuses_journal_recovery_and_host_injected_configuration_rpc() {
    let mut store = HostLogStore::new(2);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let initial = store.state(group(1)).unwrap();
    let mut core =
        Raft::recover(node(2), store.binding(), initial.clone(), store.limits()).unwrap();
    let sender = HostLogStore::new(1).binding();
    let message = Message {
        group: group(1),
        configuration: cid(1),
        from: node(1),
        sender,
        to: node(2),
        term: 2,
        context: RequestContext {
            origin: sender,
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![learners()],
            leader_commit: 0,
        },
    };
    assert!(core.step(Event::Receive(message)).is_err());
    assert_eq!(core.state(), &initial);
    append(
        &mut store,
        vec![update(
            &initial,
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![learners()],
            }),
        )],
    );
    assert!(matches!(
        Raft::recover(
            node(2),
            store.binding(),
            store.state(group(1)).unwrap(),
            store.limits()
        ),
        Err(RaftError::InvalidRecovery)
    ));
}
#[cfg(feature = "native")]
#[test]
fn native_wire_refuses_configuration_records_until_online_activation_is_integrated() {
    use voteboat::{native::wire::NativeWireCodec, wire::*};
    let sender = HostLogStore::new(1).binding();
    let scope = WireScope {
        from: node(1),
        sender,
        to: node(2),
    };
    let message = Message {
        group: group(1),
        configuration: cid(1),
        from: scope.from,
        sender,
        to: scope.to,
        term: 1,
        context: RequestContext {
            origin: sender,
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![learners()],
            leader_commit: 0,
        },
    };
    assert!(NativeWireCodec::new(WireLimits::default())
        .unwrap()
        .encode_batch(scope, &[message])
        .is_err());
}
#[test]
fn replay_rejects_index_overflow_without_panicking() {
    let entries = vec![
        LogEntry {
            index: u64::MAX,
            term: 1,
            payload: EntryPayload::Noop,
        },
        LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Noop,
        },
    ];
    assert_eq!(replay(&entries, 0), Err(MembershipError::InvalidHistory));
}

#[test]
fn snapshots_cannot_erase_the_only_recoverable_configuration() {
    use voteboat::snapshot::SnapshotRef;
    let mut store = HostLogStore::new(1);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let initial = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &initial,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![learners(), joint()],
            }),
        )],
    );
    let state = store.state(group(1)).unwrap();
    let reference = SnapshotRef {
        store: identity(1),
        group: group(1),
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: cid(1),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 0,
    };
    let compact = LogMutation::Update(LogUpdate {
        group: group(1),
        expected_revision: state.revision,
        hard_state: state.hard_state,
        commit_index: 1,
        suffix: None,
        snapshot: Some(reference),
    });
    assert_eq!(
        store.append_batch(vec![compact]),
        Err(voteboat::contracts::StorageError::Rejected(
            "snapshot would discard configuration journal"
        ))
    );
    assert_eq!(store.state(group(1)).unwrap(), state);
}

#[cfg(feature = "native")]
mod crash {
    use super::*;
    use voteboat::{contracts::StorageError, native::log_store::*};
    fn before_stage(stage: usize, io: ModelIo) -> NativeLogStore<ModelIo> {
        let mut store = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
        append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
        if stage >= 1 {
            let s = store.state(group(1)).unwrap();
            append(
                &mut store,
                vec![update(
                    &s,
                    1,
                    1,
                    Some(Suffix {
                        from: 1,
                        entries: vec![learners()],
                    }),
                )],
            );
        }
        if stage >= 2 {
            let s = store.state(group(1)).unwrap();
            append(
                &mut store,
                vec![update(
                    &s,
                    1,
                    2,
                    Some(Suffix {
                        from: 2,
                        entries: vec![joint()],
                    }),
                )],
            );
        }
        if stage >= 3 {
            let s = store.state(group(1)).unwrap();
            append(
                &mut store,
                vec![update(
                    &s,
                    1,
                    2,
                    Some(Suffix {
                        from: 3,
                        entries: vec![final_record()],
                    }),
                )],
            );
        }
        store
    }
    fn next_stage(stage: usize, s: &GroupLog) -> LogMutation {
        let (from, entries, commit, term) = match stage {
            0 => (1, vec![learners()], 0, 1),
            1 => (2, vec![joint()], 1, 1),
            2 => (3, vec![final_record()], 2, 1),
            3 => (3, vec![entry(3, 2, 9)], 2, 2),
            _ => unreachable!(),
        };
        update(s, term, commit, Some(Suffix { from, entries }))
    }
    #[test]
    fn every_torn_configuration_and_replacement_frame_recovers_one_complete_state() {
        for stage in 0..4 {
            let io = ModelIo::default();
            let mut store = before_stage(stage, io.clone());
            let old = store.state(group(1)).unwrap();
            let old_len = io.0.borrow().log.len();
            let manifest = io.0.borrow().manifest.clone();
            let mutation = next_stage(stage, &old);
            let mut expected = [(group(1), old.clone())].into();
            apply_batch(
                &mut expected,
                std::slice::from_ref(&mutation),
                store.limits(),
            )
            .unwrap();
            store.append_batch(vec![mutation]).unwrap();
            let full = io.0.borrow().log.clone();
            drop(store);
            for cut in old_len..=full.len() {
                let image = ModelIo::default();
                image.0.borrow_mut().log = full[..cut].to_vec();
                image.0.borrow_mut().manifest = manifest.clone();
                let recovered = NativeLogStore::recover(image, identity(1), LogLimits::default())
                    .unwrap()
                    .state(group(1))
                    .unwrap();
                let wanted = if cut == full.len() {
                    &expected[&group(1)]
                } else {
                    &old
                };
                assert_eq!(&recovered, wanted, "stage {stage}, cut {cut}");
                assert_eq!(
                    recovered.membership().unwrap(),
                    wanted.membership().unwrap()
                );
            }
        }
    }
    #[test]
    fn failed_configuration_barriers_fence_and_recover_without_fabricated_receipts() {
        for stage in 0..4 {
            for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
                let io = ModelIo::default();
                let mut store = before_stage(stage, io.clone());
                let old = store.state(group(1)).unwrap();
                let mutation = next_stage(stage, &old);
                let mut expected = [(group(1), old.clone())].into();
                apply_batch(
                    &mut expected,
                    std::slice::from_ref(&mutation),
                    store.limits(),
                )
                .unwrap();
                let tickets = store.append_batch(vec![mutation]).unwrap();
                io.0.borrow_mut().fault = fault;
                assert!(matches!(
                    store.barrier(&tickets),
                    Err(StorageError::Uncertain(_))
                ));
                assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
                drop(store);
                io.0.borrow_mut().power_loss();
                io.0.borrow_mut().fault = Fault::None;
                let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default())
                    .unwrap()
                    .state(group(1))
                    .unwrap();
                let wanted = if matches!(fault, Fault::Sync) {
                    old
                } else {
                    expected.remove(&group(1)).unwrap()
                };
                assert_eq!(recovered, wanted);
                assert_eq!(
                    recovered.membership().unwrap(),
                    wanted.membership().unwrap()
                );
            }
        }
    }
    #[test]
    fn physical_reclamation_preserves_joint_final_and_operation_reuse_guards() {
        for stage in [2, 3] {
            let io = ModelIo::default();
            let mut store = before_stage(stage, io.clone());
            for term in 2..=20 {
                let s = store.state(group(1)).unwrap();
                append(&mut store, vec![update(&s, term, s.commit_index, None)]);
            }
            let before = store.state(group(1)).unwrap();
            let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
            assert!(report.after_bytes < report.before_bytes);
            assert_eq!(store.state(group(1)).unwrap(), before);
            drop(store);
            io.0.borrow_mut().power_loss();
            let mut store = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
            assert_eq!(store.state(group(1)).unwrap(), before);
            assert_eq!(
                store.state(group(1)).unwrap().membership().unwrap(),
                before.membership().unwrap()
            );
            if stage == 3 {
                let mut reused = record(
                    4,
                    101,
                    4,
                    ConfigurationChange::Learners(config(5, &[3, 4, 5], &[])),
                );
                reused.term = 20;
                assert!(store
                    .append_batch(vec![update(
                        &before,
                        20,
                        3,
                        Some(Suffix {
                            from: 4,
                            entries: vec![reused]
                        })
                    )])
                    .is_err());
            }
        }
    }
    #[test]
    fn configuration_range_and_admission_charge_owned_metadata() {
        let io = ModelIo::default();
        let store = before_stage(1, io);
        let s = store.state(group(1)).unwrap();
        assert!(s.entries[0].retained_payload_bytes() > 500);
        assert!(store
            .fetch_range(group(1), s.generation, 1, 1, 100)
            .is_err());
        let fetched = store
            .fetch_range(group(1), s.generation, 1, 1, 16384)
            .unwrap();
        assert_eq!(fetched, vec![learners()]);
        let limits = LogLimits {
            max_command_bytes: 100,
            ..LogLimits::default()
        };
        let mut state = BTreeMap::new();
        apply_batch(&mut state, &[LogMutation::Create(bootstrap(1, 3))], limits).unwrap();
        let original = state.clone();
        assert!(apply_batch(
            &mut state,
            &[update(
                &original[&group(1)],
                1,
                0,
                Some(Suffix {
                    from: 1,
                    entries: vec![learners()]
                })
            )],
            limits
        )
        .is_err());
        assert_eq!(state, original);
    }
}

#[test]
fn configuration_ingress_charges_owned_metadata_to_data_budget() {
    use voteboat::outbound::{message_cost, MessageClass};
    let sender = HostLogStore::new(1).binding();
    let mut message = Message {
        group: group(1),
        configuration: cid(1),
        from: node(1),
        sender,
        to: node(2),
        term: 1,
        context: RequestContext {
            origin: sender,
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![learners()],
            leader_commit: 0,
        },
    };
    let (class, cost) = message_cost(&message, 65536).unwrap();
    assert_eq!(class, MessageClass::Data);
    assert!(message_cost(&message, cost - 1).is_err());
    if let Rpc::Append { entries, .. } = &mut message.rpc {
        entries[0].payload = EntryPayload::Noop;
    }
    let (noop_class, noop_cost) = message_cost(&message, 65536).unwrap();
    assert_eq!(noop_class, MessageClass::Control);
    assert_eq!(cost - noop_cost, learners().retained_payload_bytes());
}
