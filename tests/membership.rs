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
fn status_conformance<S: LogStore>(mut store: S) {
    let operation = OperationId::new(101).unwrap();
    append(
        &mut store,
        vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ],
    );
    let status = configuration_status::<S>;
    assert_eq!(
        status(&store).resume_action(),
        ConfigurationResumeAction::NotFoundLocally
    );
    let pending = commit_status_joint(&mut store);
    let other = store
        .state(group(2))
        .unwrap()
        .configuration_status(operation)
        .unwrap();
    assert_eq!(other.group, group(2));
    assert_eq!(
        other.resume_action(),
        ConfigurationResumeAction::NotFoundLocally
    );
    let state = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &state,
            1,
            2,
            Some(Suffix {
                from: 3,
                entries: vec![self::final_record()],
            }),
        )],
    );
    let pending_final = status(&store);
    assert_eq!(pending_final.committed, pending.accepted);
    assert_eq!(
        pending_final.accepted,
        ConfigurationProgress::Final {
            configuration: cid(4),
            index: 3,
            term: 1
        }
    );
    assert_eq!(
        pending_final.resume_action(),
        ConfigurationResumeAction::WaitForCommit
    );
    // Rollback restores exactly the resumable committed joint phase.
    let state = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &state,
            2,
            2,
            Some(Suffix {
                from: 3,
                entries: vec![],
            }),
        )],
    );
    assert!(matches!(
        status(&store).resume_action(),
        ConfigurationResumeAction::Finalize(_)
    ));
    let mut final_entry = self::final_record();
    final_entry.term = 2;
    let state = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &state,
            2,
            3,
            Some(Suffix {
                from: 3,
                entries: vec![final_entry],
            }),
        )],
    );
    let finished = status(&store);
    assert_eq!(
        finished.committed,
        ConfigurationProgress::Final {
            configuration: cid(4),
            index: 3,
            term: 2
        }
    );
    assert_eq!(
        finished.resume_action(),
        ConfigurationResumeAction::Completed
    );
    check_enrolled_status(&store);
}
#[test]
fn host_configuration_status_distinguishes_durable_and_committed_phases() {
    status_conformance(HostLogStore::new(1));
}
#[cfg(feature = "native")]
#[test]
fn native_configuration_status_distinguishes_durable_and_committed_phases() {
    use voteboat::native::log_store::*;
    status_conformance(
        NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap(),
    );
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
fn wire_capability_does_not_bypass_live_core_configuration_refusal() {
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
        .encode_batch(scope, std::slice::from_ref(&message))
        .is_err());
    let codec = NativeWireCodec::with_membership(WireLimits::default()).unwrap();
    let frame = codec.encode_batch(scope, &[message]).unwrap();
    let decoded = codec.decode_batch(scope, &frame).unwrap().pop().unwrap();
    let mut store = HostLogStore::new(2);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let initial = store.state(group(1)).unwrap();
    let mut core =
        Raft::recover(node(2), store.binding(), initial.clone(), store.limits()).unwrap();
    assert_eq!(
        core.step(Event::Receive(decoded)),
        Err(RaftError::InvalidMessage)
    );
    assert_eq!(core.state(), &initial);
    assert!(!core.has_pending_dependency());
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
        snapshot_membership: None,
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
            "snapshot membership differs from matching prefix"
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
    fn final_commit_status_requires_recovered_barrier_not_written_or_uncertain_reply() {
        let operation = OperationId::new(101).unwrap();
        for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
            let io = ModelIo::default();
            let mut store = before_stage(3, io.clone());
            let state = store.state(group(1)).unwrap();
            let tickets = store
                .append_batch(vec![update(&state, 1, 3, None)])
                .unwrap();
            assert_eq!(
                store
                    .state(group(1))
                    .unwrap()
                    .configuration_status(operation)
                    .unwrap()
                    .resume_action(),
                ConfigurationResumeAction::WaitForCommit
            );
            io.0.borrow_mut().fault = fault;
            assert!(matches!(
                store.barrier(&tickets),
                Err(StorageError::Uncertain(_))
            ));
            assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
            drop(store);
            io.0.borrow_mut().power_loss();
            let store = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
            let status = store
                .state(group(1))
                .unwrap()
                .configuration_status(operation)
                .unwrap();
            assert_eq!(
                status.accepted,
                ConfigurationProgress::Final {
                    configuration: cid(4),
                    index: 3,
                    term: 1
                }
            );
            if matches!(fault, Fault::Sync) {
                assert_eq!(
                    status.resume_action(),
                    ConfigurationResumeAction::WaitForCommit
                );
            } else {
                assert_eq!(status.committed, status.accepted);
                assert_eq!(status.resume_action(), ConfigurationResumeAction::Completed);
            }
        }
    }
    #[test]
    fn actual_files_reopen_resumable_joint_then_committed_final_status() {
        let path = std::env::temp_dir().join(format!(
            "voteboat-configuration-status-{}",
            std::process::id()
        ));
        let mut store = NativeLogStore::create(
            FileLogIo::create(&path).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
        let state = store.state(group(1)).unwrap();
        append(
            &mut store,
            vec![update(
                &state,
                1,
                2,
                Some(Suffix {
                    from: 1,
                    entries: vec![learners(), joint()],
                }),
            )],
        );
        drop(store);
        let operation = OperationId::new(101).unwrap();
        let mut store = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let state = store.state(group(1)).unwrap();
        let core =
            Raft::recover_member(node(1), store.binding(), state.clone(), store.limits()).unwrap();
        assert_eq!(
            core.configuration_status(operation)
                .unwrap()
                .resume_action(),
            ConfigurationResumeAction::Finalize(ConfigurationRecord {
                operation,
                expected: cid(3),
                change: ConfigurationChange::Final { id: cid(4) }
            })
        );
        append(
            &mut store,
            vec![update(
                &state,
                1,
                3,
                Some(Suffix {
                    from: 3,
                    entries: vec![final_record()],
                }),
            )],
        );
        drop(store);
        let store = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        let state = store.state(group(1)).unwrap();
        assert_eq!(
            state
                .configuration_status(operation)
                .unwrap()
                .resume_action(),
            ConfigurationResumeAction::Completed
        );
        let mut core =
            Raft::recover_member(node(1), store.binding(), state, store.limits()).unwrap();
        core.storage_failed();
        assert_eq!(core.configuration_status(operation), Err(RaftError::Fenced));
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
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

mod configuration_snapshots {
    use super::*;
    use voteboat::{application::*, runtime::*, snapshot::*, worker::*};
    fn command(index: u64, term: u64) -> LogEntry {
        LogEntry {
            index,
            term,
            payload: EntryPayload::Command {
                operation: OperationId::new(1000).unwrap(),
                bytes: 7i64.to_le_bytes().to_vec(),
            },
        }
    }
    fn history() -> Vec<LogEntry> {
        let mut learner = learners();
        learner.index = 2;
        let mut joint = super::joint();
        joint.index = 3;
        let mut final_entry = final_record();
        final_entry.index = 4;
        vec![command(1, 1), learner, joint, final_entry]
    }
    fn metadata(s: &GroupLog, index: u64) -> SnapshotMetadata {
        SnapshotMetadata {
            bootstrap: s.bootstrap.clone(),
            membership: s.checkpoint_membership(index).unwrap(),
            index,
            term: s.term_at(index).unwrap(),
            application_schema: 1,
        }
    }
    fn publish<S: SnapshotStore>(
        snapshots: &mut S,
        metadata: SnapshotMetadata,
        bytes: &[u8],
    ) -> SnapshotReceipt {
        let ticket = snapshots.begin(metadata, bytes.len()).unwrap();
        for (i, chunk) in bytes.chunks(snapshots.limits().max_chunk_bytes).enumerate() {
            snapshots
                .write_chunk(ticket, i * snapshots.limits().max_chunk_bytes, chunk)
                .unwrap();
        }
        let sealed = snapshots.seal(ticket).unwrap();
        snapshots.publish(sealed).unwrap()
    }
    fn compact<L: LogStore, S: SnapshotRetention>(
        log: &mut L,
        snapshots: &mut S,
        receipt: &SnapshotReceipt,
    ) {
        let reference = receipt.reference();
        snapshots.pin_for_log(reference).unwrap();
        assert_eq!(
            snapshots.load_pinned(reference).unwrap().metadata,
            receipt.metadata
        );
        let s = log.state(group(1)).unwrap();
        let mutation = LogMutation::Update(LogUpdate {
            group: group(1),
            expected_revision: s.revision,
            hard_state: s.hard_state,
            commit_index: s.commit_index,
            suffix: None,
            snapshot: Some(reference),
            snapshot_membership: receipt.metadata.membership.clone(),
        });
        let tickets = log.append_batch(vec![mutation]).unwrap();
        assert_eq!(log.state(group(1)).unwrap(), s);
        log.barrier(&tickets).unwrap();
        snapshots.reconcile_log(Some(reference)).unwrap();
    }
    fn conformance<L: LogStore, S: SnapshotRetention>(mut log: L, mut snapshots: S) -> (L, S) {
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let s = log.state(group(1)).unwrap();
        append(
            &mut log,
            vec![update(
                &s,
                1,
                3,
                Some(Suffix {
                    from: 1,
                    entries: history(),
                }),
            )],
        );
        let original = log.state(group(1)).unwrap();
        let mut application = Counter::new(10).unwrap();
        application.apply_batch(&original.entries[..3]).unwrap();
        let bytes = application.checkpoint(16384).unwrap();
        let receipt = publish(&mut snapshots, metadata(&original, 3), &bytes);
        check_joint_snapshot(&receipt);
        compact(&mut log, &mut snapshots, &receipt);
        let compacted = log.state(group(1)).unwrap();
        let operation = check_compacted_joint(&compacted, &original);
        // The final record is uncommitted. Rollback must expose the joint base.
        append(
            &mut log,
            vec![update(
                &compacted,
                2,
                3,
                Some(Suffix {
                    from: 4,
                    entries: vec![LogEntry {
                        index: 4,
                        term: 2,
                        payload: EntryPayload::Noop,
                    }],
                }),
            )],
        );
        let rolled = log.state(group(1)).unwrap();
        assert!(matches!(
            rolled
                .configuration_status(operation)
                .unwrap()
                .resume_action(),
            ConfigurationResumeAction::Finalize(_)
        ));
        assert_eq!(rolled.membership().unwrap().id(), cid(3));
        assert!(!rolled.membership().unwrap().is_satisfied(&set(&[4, 5])));
        let mut final_entry = final_record();
        final_entry.index = 4;
        final_entry.term = 3;
        append(
            &mut log,
            vec![update(
                &rolled,
                3,
                5,
                Some(Suffix {
                    from: 4,
                    entries: vec![final_entry, command(5, 3)],
                }),
            )],
        );
        let finished = log.state(group(1)).unwrap();
        let saved = snapshots.load_pinned(receipt.reference()).unwrap();
        let mut restored = Counter::new(10).unwrap();
        restored
            .restore_checkpoint(1, 3, &saved.application)
            .unwrap();
        let receipts = restored.apply_batch(&finished.entries).unwrap();
        assert_eq!(receipts.len(), 1);
        assert!(receipts[0].duplicate);
        assert_eq!(receipts[0].outcome, CounterOutcome::Value(7));
        let final_bytes = restored.checkpoint(16384).unwrap();
        let final_receipt = publish(&mut snapshots, metadata(&finished, 5), &final_bytes);
        assert_eq!(final_receipt.reference().configuration, cid(4));
        compact(&mut log, &mut snapshots, &final_receipt);
        for operation in [100, 101] {
            let status = log
                .state(group(1))
                .unwrap()
                .configuration_status(OperationId::new(operation).unwrap())
                .unwrap();
            assert_eq!(
                status.committed,
                ConfigurationProgress::CompactedCompleted { through: 5 }
            );
            assert_eq!(status.resume_action(), ConfigurationResumeAction::Completed);
        }
        check_compacted_reuse(
            &mut log,
            &mut snapshots,
            &receipt,
            &final_receipt,
            &final_bytes,
        );
        (log, snapshots)
    }

    fn check_joint_snapshot(receipt: &SnapshotReceipt) {
        assert_eq!(receipt.reference().configuration, cid(3));
        assert_eq!(
            receipt
                .metadata
                .membership
                .as_ref()
                .unwrap()
                .joint()
                .unwrap()
                .index,
            3
        );
    }
    fn check_compacted_joint(compacted: &GroupLog, original: &GroupLog) -> OperationId {
        let operation = OperationId::new(101).unwrap();
        let status = compacted.configuration_status(operation).unwrap();
        assert_eq!(
            status.committed,
            ConfigurationProgress::Joint {
                configuration: cid(3),
                target: cid(4),
                index: 3,
                term: Some(1)
            }
        );
        assert_eq!(
            status.accepted,
            ConfigurationProgress::Final {
                configuration: cid(4),
                index: 4,
                term: 1
            }
        );
        assert_eq!(
            status.resume_action(),
            ConfigurationResumeAction::WaitForCommit
        );
        assert_eq!(
            compacted.membership().unwrap(),
            original.membership().unwrap()
        );
        assert_eq!(compacted.entries.len(), 1);
        assert_eq!(compacted.snapshot_membership.as_ref().unwrap().id(), cid(3));
        assert_eq!(compacted.membership().unwrap().id(), cid(4));
        operation
    }

    fn check_compacted_reuse<L: LogStore, S: SnapshotRetention>(
        log: &mut L,
        snapshots: &mut S,
        receipt: &SnapshotReceipt,
        final_receipt: &SnapshotReceipt,
        final_bytes: &[u8],
    ) {
        let mut regression = receipt.metadata.clone();
        regression.index = 6;
        regression.term = 3;
        assert!(snapshots.begin(regression, final_bytes.len()).is_err());
        let mut rewritten = final_receipt.metadata.clone();
        rewritten.index = 6;
        rewritten.membership = Some(Box::new(
            Membership::from_checkpoint(
                config(4, &[1, 2, 3], &[]),
                None,
                4,
                final_receipt
                    .metadata
                    .membership
                    .as_ref()
                    .unwrap()
                    .operations()
                    .clone(),
                6,
            )
            .unwrap(),
        ));
        assert!(snapshots.begin(rewritten, final_bytes.len()).is_err());
        let base = log.state(group(1)).unwrap();
        assert!(base.entries.is_empty());
        assert_eq!(base.membership().unwrap().id(), cid(4));
        assert_eq!(base.membership().unwrap().operations().len(), 2);
        assert!(!base.membership().unwrap().is_voter(node(1)));
        let mut reused = record(
            6,
            101,
            4,
            ConfigurationChange::Learners(config(5, &[3, 4, 5], &[])),
        );
        reused.term = 3;
        assert!(log
            .append_batch(vec![update(
                &base,
                3,
                5,
                Some(Suffix {
                    from: 6,
                    entries: vec![reused]
                })
            )])
            .is_err());
        let mut next = record(
            6,
            102,
            4,
            ConfigurationChange::Learners(config(5, &[3, 4, 5], &[])),
        );
        next.term = 3;
        append(
            log,
            vec![update(
                &base,
                3,
                5,
                Some(Suffix {
                    from: 6,
                    entries: vec![next],
                }),
            )],
        );
        assert_eq!(
            log.state(group(1)).unwrap().membership().unwrap().id(),
            cid(5)
        );
    }

    fn snapshot_cost_state() -> GroupLog {
        let mut state = BTreeMap::new();
        apply_batch(
            &mut state,
            &[LogMutation::Create(bootstrap(1, 3))],
            LogLimits::default(),
        )
        .unwrap();
        let original = state[&group(1)].clone();
        apply_batch(
            &mut state,
            &[update(
                &original,
                1,
                2,
                Some(Suffix {
                    from: 1,
                    entries: vec![learners(), joint()],
                }),
            )],
            LogLimits::default(),
        )
        .unwrap();
        state.remove(&group(1)).unwrap()
    }
    #[test]
    fn host_snapshot_base_preserves_joint_rollback_final_and_application_dedup() {
        conformance(
            HostLogStore::new(1),
            support::snapshot::HostSnapshots::new(),
        );
    }
    #[test]
    fn compacted_joint_status_does_not_invent_the_discarded_entry_term() {
        let mut log = HostLogStore::new(1);
        let mut snapshots = support::snapshot::HostSnapshots::new();
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let state = log.state(group(1)).unwrap();
        append(
            &mut log,
            vec![update(
                &state,
                2,
                3,
                Some(Suffix {
                    from: 1,
                    entries: vec![
                        learners(),
                        joint(),
                        LogEntry {
                            index: 3,
                            term: 2,
                            payload: EntryPayload::Noop,
                        },
                    ],
                }),
            )],
        );
        let state = log.state(group(1)).unwrap();
        let mut app = Counter::new(10).unwrap();
        app.apply_batch(&state.entries).unwrap();
        let receipt = publish(
            &mut snapshots,
            metadata(&state, 3),
            &app.checkpoint(16384).unwrap(),
        );
        compact(&mut log, &mut snapshots, &receipt);
        let state = log.state(group(1)).unwrap();
        let status = state
            .configuration_status(OperationId::new(101).unwrap())
            .unwrap();
        assert_eq!(
            status.committed,
            ConfigurationProgress::Joint {
                configuration: cid(3),
                target: cid(4),
                index: 2,
                term: None
            }
        );
        assert!(matches!(
            status.resume_action(),
            ConfigurationResumeAction::Finalize(_)
        ));
        assert_eq!(
            state
                .configuration_status(OperationId::new(100).unwrap())
                .unwrap()
                .committed,
            ConfigurationProgress::CompactedCompleted { through: 3 }
        );
    }
    #[test]
    fn checkpoint_validation_rejects_bad_boundaries_operations_and_joint_identity() {
        let mut log = HostLogStore::new(1);
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let mut malformed = log.state(group(1)).unwrap();
        malformed.entries = vec![entry(77, 1, 1)];
        assert_eq!(
            malformed.membership_at(2),
            Err(MembershipError::InvalidHistory)
        );
        let state = replay(&[learners(), joint()], 1).unwrap();
        let ops = state.operations().clone();
        assert!(Membership::from_checkpoint(
            state.stable().clone(),
            state.joint().cloned(),
            2,
            ops.clone(),
            1
        )
        .is_err());
        assert!(Membership::from_checkpoint(
            state.stable().clone(),
            state.joint().cloned(),
            2,
            BTreeSet::new(),
            2
        )
        .is_err());
        let mut wrong = state.joint().cloned().unwrap();
        wrong.index = 1;
        assert!(Membership::from_checkpoint(
            state.stable().clone(),
            Some(wrong),
            2,
            ops.clone(),
            2
        )
        .is_err());
        let mut wrong = state.joint().cloned().unwrap();
        wrong.id = cid(2);
        assert!(
            Membership::from_checkpoint(state.stable().clone(), Some(wrong), 2, ops, 2).is_err()
        );
        let empty =
            Membership::from_checkpoint(config(2, &[1, 2, 3], &[]), None, 0, BTreeSet::new(), 0)
                .unwrap();
        assert!(empty.validate_checkpoint(&bootstrap(1, 3), 0).is_err());
        let operations = (1..=MAX_CONFIGURATION_OPERATIONS as u128)
            .map(|n| OperationId::new(n).unwrap())
            .collect();
        let full = Membership::from_checkpoint(
            config(2, &[1, 2, 3], &[]),
            None,
            MAX_CONFIGURATION_OPERATIONS as u64,
            operations,
            MAX_CONFIGURATION_OPERATIONS as u64,
        )
        .unwrap();
        let entry = record(
            MAX_CONFIGURATION_OPERATIONS as u64 + 1,
            99999,
            2,
            ConfigurationChange::Learners(config(3, &[1, 2, 3], &[])),
        );
        assert_eq!(
            Membership::replay_from(
                &bootstrap(1, 3),
                Some(&full),
                MAX_CONFIGURATION_OPERATIONS as u64,
                &[entry],
                MAX_CONFIGURATION_OPERATIONS as u64
            ),
            Err(MembershipError::HistoryFull)
        );
    }
    #[test]
    fn matching_snapshot_cannot_forge_configuration_or_drop_operation_history() {
        let mut log = HostLogStore::new(1);
        append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
        let s = log.state(group(1)).unwrap();
        append(
            &mut log,
            vec![update(
                &s,
                1,
                3,
                Some(Suffix {
                    from: 1,
                    entries: vec![learners(), joint(), final_record()],
                }),
            )],
        );
        let s = log.state(group(1)).unwrap();
        let real = s.checkpoint_membership(3).unwrap().unwrap();
        let forged = Membership::from_checkpoint(
            config(4, &[1, 2, 3], &[]),
            None,
            3,
            real.operations().clone(),
            3,
        )
        .unwrap();
        let reference = SnapshotRef {
            store: identity(1),
            group: group(1),
            generation: SnapshotGeneration::new(1).unwrap(),
            configuration: cid(4),
            index: 3,
            term: 1,
            application_schema: 1,
            file_bytes: 100,
            checksum: 0,
        };
        let mut mutation = LogUpdate {
            group: group(1),
            expected_revision: s.revision,
            hard_state: s.hard_state,
            commit_index: 3,
            suffix: None,
            snapshot: Some(reference),
            snapshot_membership: Some(Box::new(forged)),
        };
        assert!(log
            .append_batch(vec![LogMutation::Update(mutation.clone())])
            .is_err());
        mutation.snapshot_membership = Some(real);
        mutation.snapshot.as_mut().unwrap().configuration = cid(3);
        assert!(log
            .append_batch(vec![LogMutation::Update(mutation.clone())])
            .is_err());
        mutation.snapshot = None;
        assert!(log
            .append_batch(vec![LogMutation::Update(mutation)])
            .is_err());
        assert_eq!(log.state(group(1)).unwrap(), s);
        // A nonmatching snapshot beyond the log cannot roll back committed config.
        let mut bad = reference;
        bad.index = 9;
        bad.configuration = cid(1);
        let mutation = LogUpdate {
            group: group(1),
            expected_revision: s.revision,
            hard_state: s.hard_state,
            commit_index: 9,
            suffix: None,
            snapshot: Some(bad),
            snapshot_membership: None,
        };
        assert!(log
            .append_batch(vec![LogMutation::Update(mutation)])
            .is_err());
    }
    #[test]
    fn snapshot_membership_cost_is_retained_by_worker_and_ingress_reservations() {
        let state = snapshot_cost_state();
        let m = metadata(&state, 2);
        let snapshot = Snapshot {
            metadata: m.clone(),
            application: vec![7; 64],
        };
        let cost = voteboat::snapshot_worker::snapshot_image_bytes(&snapshot).unwrap();
        let mut plain = snapshot.clone();
        plain.metadata.membership = None;
        assert_eq!(
            cost - voteboat::snapshot_worker::snapshot_image_bytes(&plain).unwrap(),
            m.membership.as_ref().unwrap().retained_bytes()
        );
        let sender = HostLogStore::new(1).binding();
        let message = Message {
            group: group(1),
            configuration: cid(3),
            from: node(1),
            sender,
            to: node(2),
            term: 1,
            context: RequestContext {
                origin: sender,
                sequence: 1,
            },
            rpc: Rpc::Snapshot {
                snapshot: Box::new(snapshot),
            },
        };
        let (_, cost) = voteboat::outbound::message_cost(&message, 65536).unwrap();
        assert!(voteboat::outbound::message_cost(&message, cost - 1).is_err());
        let unit = PersistUnit {
            visit: VisitTicket {
                owner: RuntimeOwner {
                    store: sender,
                    lane: ExecutionLaneId::new(1).unwrap(),
                    generation: RuntimeGeneration::new(1).unwrap(),
                },
                group: group(1),
                sequence: 1,
            },
            update: LogUpdate {
                group: group(1),
                expected_revision: state.revision,
                hard_state: state.hard_state,
                commit_index: 2,
                suffix: None,
                snapshot: Some(SnapshotRef {
                    store: identity(1),
                    group: group(1),
                    generation: SnapshotGeneration::new(1).unwrap(),
                    configuration: cid(3),
                    index: 2,
                    term: 1,
                    application_schema: 1,
                    file_bytes: 100,
                    checksum: 0,
                }),
                snapshot_membership: m.membership,
            },
        };
        let (cost, control) =
            batch_cost(std::slice::from_ref(&unit), 1, WorkerLimits::default()).unwrap();
        assert!(!control);
        let mut plain = PersistUnit {
            visit: unit.visit,
            update: unit.update.clone(),
        };
        plain.update.snapshot_membership = None;
        let (plain_cost, plain_control) = batch_cost(&[plain], 1, WorkerLimits::default()).unwrap();
        assert!(plain_control);
        assert_eq!(
            cost - plain_cost,
            unit.update
                .snapshot_membership
                .as_ref()
                .unwrap()
                .retained_bytes()
        );
        let mut journal = PersistUnit {
            visit: unit.visit,
            update: unit.update.clone(),
        };
        journal.update.snapshot = None;
        journal.update.snapshot_membership = None;
        journal.update.suffix = Some(Suffix {
            from: 1,
            entries: vec![learners()],
        });
        let (cost, control) =
            batch_cost(std::slice::from_ref(&journal), 1, WorkerLimits::default()).unwrap();
        assert!(!control);
        journal.update.suffix.as_mut().unwrap().entries[0].payload = EntryPayload::Noop;
        let (noop_cost, control) = batch_cost(&[journal], 1, WorkerLimits::default()).unwrap();
        assert!(control);
        assert_eq!(cost - noop_cost, learners().retained_payload_bytes());
    }

    #[cfg(feature = "native")]
    mod native {
        use super::*;
        use std::{cell::RefCell, io, rc::Rc};
        use voteboat::{
            contracts::StorageError,
            native::{log_store::*, snapshot_store::*},
        };
        #[derive(Clone, Copy, Default)]
        enum Failure {
            #[default]
            None,
            Begin(usize),
            Append(usize),
            SyncBefore,
            SyncAfter,
            PublishBefore,
            PublishAfter,
        }
        #[derive(Clone, Default)]
        struct Device {
            manifest: Option<Vec<u8>>,
            slots: [Option<Vec<u8>>; 2],
            synced: [Option<Vec<u8>>; 2],
            failure: Failure,
        }
        impl Device {
            fn power_loss(&mut self) {
                self.slots = self.synced.clone();
                self.failure = Failure::None;
            }
        }
        #[derive(Clone, Default)]
        struct Memory(Rc<RefCell<Device>>);
        impl SnapshotIo for Memory {
            fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
                self.0
                    .borrow()
                    .manifest
                    .clone()
                    .ok_or(io::ErrorKind::NotFound.into())
            }
            fn read_slot(&mut self, slot: u8, limit: usize) -> io::Result<Vec<u8>> {
                let bytes = self.0.borrow().slots[slot as usize]
                    .clone()
                    .ok_or(io::ErrorKind::NotFound)?;
                if bytes.len() > limit {
                    return Err(io::Error::other("budget"));
                }
                Ok(bytes)
            }
            fn begin_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
                let mut d = self.0.borrow_mut();
                if let Failure::Begin(cut) = d.failure {
                    d.slots[slot as usize] = Some(bytes[..cut.min(bytes.len())].to_vec());
                    return Err(io::Error::other("begin cut"));
                }
                d.slots[slot as usize] = Some(bytes.to_vec());
                Ok(())
            }
            fn append_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
                let mut d = self.0.borrow_mut();
                if let Failure::Append(cut) = d.failure {
                    d.slots[slot as usize]
                        .as_mut()
                        .unwrap()
                        .extend(&bytes[..cut.min(bytes.len())]);
                    return Err(io::Error::other("append cut"));
                }
                d.slots[slot as usize].as_mut().unwrap().extend(bytes);
                Ok(())
            }
            fn sync_slot(&mut self, slot: u8) -> io::Result<()> {
                let mut d = self.0.borrow_mut();
                if matches!(d.failure, Failure::SyncBefore) {
                    return Err(io::Error::other("sync"));
                }
                d.synced[slot as usize] = d.slots[slot as usize].clone();
                if matches!(d.failure, Failure::SyncAfter) {
                    return Err(io::Error::other("sync receipt lost"));
                }
                Ok(())
            }
            fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
                let mut d = self.0.borrow_mut();
                if matches!(d.failure, Failure::PublishBefore) {
                    return Err(io::Error::other("publish"));
                }
                d.manifest = Some(bytes.to_vec());
                if matches!(d.failure, Failure::PublishAfter) {
                    return Err(io::Error::other("publish receipt lost"));
                }
                Ok(())
            }
        }
        fn sid() -> SnapshotIdentity {
            SnapshotIdentity {
                store: identity(1),
                group: group(1),
            }
        }
        fn create(io: Memory) -> NativeSnapshotStore<Memory> {
            NativeSnapshotStore::create(io, sid(), SnapshotLimits::default()).unwrap()
        }
        fn recover(io: Memory) -> NativeSnapshotStore<Memory> {
            NativeSnapshotStore::recover(io, sid(), SnapshotLimits::default()).unwrap()
        }
        fn populated(io: ModelIo) -> NativeLogStore<ModelIo> {
            let mut log = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
            append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
            let s = log.state(group(1)).unwrap();
            append(
                &mut log,
                vec![update(
                    &s,
                    1,
                    3,
                    Some(Suffix {
                        from: 1,
                        entries: history(),
                    }),
                )],
            );
            log
        }
        #[test]
        fn native_configuration_snapshot_recovery_and_physical_reclaim_conformance() {
            let io = ModelIo::default();
            let snapshots_io = Memory::default();
            let log =
                NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
            let (mut log, snapshots) = conformance(log, create(snapshots_io.clone()));
            for term in 4..=30 {
                let s = log.state(group(1)).unwrap();
                append(&mut log, vec![update(&s, term, s.commit_index, None)]);
            }
            let expected = log.state(group(1)).unwrap();
            let report = log.reclaim(log.limits().max_wal_bytes).unwrap();
            assert!(report.after_bytes < report.before_bytes);
            drop(log);
            drop(snapshots);
            io.0.borrow_mut().power_loss();
            snapshots_io.0.borrow_mut().power_loss();
            let log = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
            assert_eq!(log.state(group(1)).unwrap(), expected);
            let mut snapshots = recover(snapshots_io);
            let reference = expected.snapshot.unwrap();
            assert_eq!(
                log.state(group(1))
                    .unwrap()
                    .configuration_status(OperationId::new(101).unwrap())
                    .unwrap()
                    .committed,
                ConfigurationProgress::CompactedCompleted {
                    through: reference.index
                }
            );
            let snapshot = snapshots.load_pinned(reference).unwrap();
            assert!(reference.matches(&snapshot));
            assert_eq!(snapshot.metadata.membership, expected.snapshot_membership);
            let mut app = Counter::new(10).unwrap();
            app.restore_checkpoint(1, reference.index, &snapshot.application)
                .unwrap();
            app.apply_batch(
                &expected.entries[..(expected.commit_index - reference.index) as usize],
            )
            .unwrap();
            assert_eq!(app.applied_index(), expected.commit_index);
            assert!(matches!(
                recover_replica(
                    node(1),
                    group(1),
                    &log,
                    &mut snapshots,
                    &mut Counter::new(10).unwrap()
                ),
                Err(CheckpointError::Consensus(RaftError::InvalidRecovery))
            ));
        }
        #[test]
        fn actual_files_preserve_joint_and_final_configuration_bases() {
            let directory = std::env::temp_dir().join(format!(
                "voteboat-membership-snapshot-{}",
                std::process::id()
            ));
            let log = NativeLogStore::create(
                FileLogIo::create(&directory).unwrap(),
                identity(1),
                LogLimits::default(),
            )
            .unwrap();
            let snapshots = NativeSnapshotStore::create(
                FileSnapshotIo::create(directory.join("snapshots")).unwrap(),
                sid(),
                SnapshotLimits::default(),
            )
            .unwrap();
            let (log, snapshots) = conformance(log, snapshots);
            let expected = log.state(group(1)).unwrap();
            drop(log);
            drop(snapshots);
            let log = NativeLogStore::recover(
                FileLogIo::open(&directory).unwrap(),
                identity(1),
                LogLimits::default(),
            )
            .unwrap();
            let mut snapshots = NativeSnapshotStore::recover(
                FileSnapshotIo::open(directory.join("snapshots")).unwrap(),
                sid(),
                SnapshotLimits::default(),
            )
            .unwrap();
            assert_eq!(
                log.state(group(1))
                    .unwrap()
                    .configuration_status(OperationId::new(101).unwrap())
                    .unwrap()
                    .committed,
                ConfigurationProgress::CompactedCompleted { through: 5 }
            );
            assert_eq!(log.state(group(1)).unwrap(), expected);
            assert_eq!(
                snapshots
                    .load_pinned(expected.snapshot.unwrap())
                    .unwrap()
                    .metadata
                    .membership,
                expected.snapshot_membership
            );
            drop(log);
            drop(snapshots);
            std::fs::remove_dir_all(directory).unwrap();
        }
        #[test]
        fn interrupted_snapshot_publication_never_exposes_a_partial_membership_base() {
            let log = populated(ModelIo::default());
            let state = log.state(group(1)).unwrap();
            let io = Memory::default();
            let mut snapshots = create(io.clone());
            let old = publish(&mut snapshots, metadata(&state, 1), &[7; 64]);
            snapshots.pin_for_log(old.reference()).unwrap();
            drop(snapshots);
            let baseline = io.0.borrow().clone();
            let m = metadata(&state, 3);
            let prefix = NativeSnapshotCodec
                .prefix(&m, 64, SnapshotLimits::default())
                .unwrap();
            for cut in 0..=prefix.len() {
                let io = Memory(Rc::new(RefCell::new(baseline.clone())));
                let mut snapshots = recover(io.clone());
                io.0.borrow_mut().failure = Failure::Begin(cut);
                assert!(snapshots.begin(m.clone(), 64).is_err());
                drop(snapshots);
                io.0.borrow_mut().power_loss();
                let mut snapshots = recover(io);
                assert_eq!(snapshots.load().unwrap().unwrap().metadata, old.metadata);
            }
            let faults = (0..=64)
                .map(|cut| (0, Failure::Append(cut)))
                .chain((0..=4).map(|cut| (1, Failure::Append(cut))))
                .chain([
                    (1, Failure::SyncBefore),
                    (1, Failure::SyncAfter),
                    (2, Failure::PublishBefore),
                    (2, Failure::PublishAfter),
                ]);
            for (phase, failure) in faults {
                let io = Memory(Rc::new(RefCell::new(baseline.clone())));
                let mut snapshots = recover(io.clone());
                let ticket = snapshots.begin(m.clone(), 64).unwrap();
                if phase == 0 {
                    io.0.borrow_mut().failure = failure;
                    assert!(snapshots.write_chunk(ticket, 0, &[9; 64]).is_err());
                } else {
                    snapshots.write_chunk(ticket, 0, &[9; 64]).unwrap();
                    if phase == 1 {
                        io.0.borrow_mut().failure = failure;
                        assert!(snapshots.seal(ticket).is_err());
                    } else {
                        let sealed = snapshots.seal(ticket).unwrap();
                        io.0.borrow_mut().failure = failure;
                        assert!(snapshots.publish(sealed).is_err());
                    }
                }
                drop(snapshots);
                io.0.borrow_mut().power_loss();
                let mut snapshots = recover(io);
                let loaded = snapshots.load().unwrap().unwrap();
                assert_eq!(
                    loaded.metadata,
                    if matches!(failure, Failure::PublishAfter) {
                        m.clone()
                    } else {
                        old.metadata.clone()
                    }
                );
                assert_eq!(
                    snapshots.load_pinned(old.reference()).unwrap().metadata,
                    old.metadata
                );
            }
        }
        #[test]
        fn every_torn_wal_snapshot_switch_retains_the_selected_pinned_membership() {
            let io = ModelIo::default();
            let mut log = populated(io.clone());
            let state = log.state(group(1)).unwrap();
            let snapshots_io = Memory::default();
            let mut snapshots = create(snapshots_io.clone());
            let old = publish(&mut snapshots, metadata(&state, 1), &[7; 64]);
            compact(&mut log, &mut snapshots, &old);
            let prior = log.state(group(1)).unwrap();
            let next = publish(&mut snapshots, metadata(&prior, 3), &[9; 64]);
            snapshots.pin_for_log(next.reference()).unwrap();
            let snapshot_image = snapshots_io.0.borrow().clone();
            let old_len = io.0.borrow().log.len();
            let old_manifest = io.0.borrow().manifest.clone();
            let mutation = LogMutation::Update(LogUpdate {
                group: group(1),
                expected_revision: prior.revision,
                hard_state: prior.hard_state,
                commit_index: prior.commit_index,
                suffix: None,
                snapshot: Some(next.reference()),
                snapshot_membership: next.metadata.membership.clone(),
            });
            let mut expected = [(group(1), prior.clone())].into();
            apply_batch(&mut expected, std::slice::from_ref(&mutation), log.limits()).unwrap();
            log.append_batch(vec![mutation]).unwrap();
            let full = io.0.borrow().log.clone();
            drop(log);
            drop(snapshots);
            for cut in old_len..=full.len() {
                let io = ModelIo::default();
                io.0.borrow_mut().log = full[..cut].to_vec();
                io.0.borrow_mut().manifest = old_manifest.clone();
                let log = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
                let state = log.state(group(1)).unwrap();
                assert_eq!(
                    state,
                    if cut == full.len() {
                        expected[&group(1)].clone()
                    } else {
                        prior.clone()
                    }
                );
                let mut snapshots = recover(Memory(Rc::new(RefCell::new(snapshot_image.clone()))));
                let reference = state.snapshot.unwrap();
                let loaded = snapshots.load_pinned(reference).unwrap();
                assert_eq!(loaded.metadata.membership, state.snapshot_membership);
                assert!(reference.matches(&loaded));
                snapshots.reconcile_log(Some(reference)).unwrap();
                assert_eq!(snapshots.latest_reference().unwrap(), Some(reference));
                assert_eq!(state.membership().unwrap().id(), cid(4));
            }
        }
        #[test]
        fn legacy_codec_capability_cannot_drop_membership_metadata() {
            struct Legacy;
            impl SnapshotCodec for Legacy {
                fn format_version(&self) -> u32 {
                    1
                }
                fn prefix(
                    &self,
                    m: &SnapshotMetadata,
                    n: usize,
                    l: SnapshotLimits,
                ) -> Result<Vec<u8>, StorageError> {
                    NativeSnapshotCodec.prefix(m, n, l)
                }
                fn finish(&self, b: &[u8], l: SnapshotLimits) -> Result<[u8; 4], StorageError> {
                    NativeSnapshotCodec.finish(b, l)
                }
                fn decode(&self, b: &[u8], l: SnapshotLimits) -> Result<Snapshot, StorageError> {
                    NativeSnapshotCodec.decode(b, l)
                }
            }
            let log = populated(ModelIo::default());
            let state = log.state(group(1)).unwrap();
            let io = Memory::default();
            let mut snapshots = NativeSnapshotStore::create_with_codec(
                io.clone(),
                sid(),
                SnapshotLimits::default(),
                Legacy,
            )
            .unwrap();
            let before = io.0.borrow().clone();
            assert!(snapshots.begin(metadata(&state, 3), 64).is_err());
            assert_eq!(io.0.borrow().manifest, before.manifest);
            assert_eq!(io.0.borrow().slots, before.slots);
            drop(snapshots);
            let mut snapshots = recover(io.clone());
            publish(&mut snapshots, metadata(&state, 3), &[7; 64]);
            drop(snapshots);
            let before = io.0.borrow().manifest.clone();
            assert!(NativeSnapshotStore::recover_with_codec(
                io.clone(),
                sid(),
                SnapshotLimits::default(),
                Legacy
            )
            .is_err());
            assert_eq!(io.0.borrow().manifest, before);
        }
    }
}

#[test]
fn replica_and_voter_identity_views_follow_activation_rollback_and_compaction() {
    let staged = replay(&[learners()], 0).unwrap();
    let joint = replay(&[learners(), joint()], 1).unwrap();
    let finalized = replay(&[learners(), self::joint(), final_record()], 2).unwrap();
    for (membership, replicas, voters) in [
        (&staged, vec![1, 2, 3, 4, 5], vec![1, 2, 3]),
        (&joint, vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4, 5]),
        (&finalized, vec![1, 3, 4, 5], vec![3, 4, 5]),
    ] {
        let identities = membership.replicas().collect::<BTreeMap<_, _>>();
        assert_eq!(identities.len(), membership.replicas().count());
        assert_eq!(
            identities.keys().copied().collect::<BTreeSet<_>>(),
            set(&replicas)
        );
        for n in 1..=6 {
            let expected_store = identity(n as u128);
            assert_eq!(
                membership.replica_store(node(n)),
                replicas.contains(&n).then_some(expected_store)
            );
            assert_eq!(
                membership.voter_store(node(n)),
                voters.contains(&n).then_some(expected_store)
            );
        }
    }
    let compacted =
        Membership::replay_from(&bootstrap(1, 3), Some(&joint), 2, &[final_record()], 2).unwrap();
    assert_eq!(
        compacted.replicas().collect::<BTreeMap<_, _>>(),
        finalized.replicas().collect()
    );
    let rollback = Membership::replay_from(&bootstrap(1, 3), Some(&joint), 2, &[], 2).unwrap();
    assert_eq!(rollback.voter_store(node(2)), Some(identity(2)));
    assert_eq!(finalized.replica_store(node(2)), None);
    assert_eq!(finalized.voter_store(node(1)), None);
    assert_eq!(finalized.replica_store(node(1)), Some(identity(1)));
}

fn configuration_status<S: LogStore>(store: &S) -> ConfigurationOperationStatus {
    let operation = OperationId::new(101).unwrap();

    store
        .state(group(1))
        .unwrap()
        .configuration_status(operation)
        .unwrap()
}

fn commit_status_joint<S: LogStore>(store: &mut S) -> ConfigurationOperationStatus {
    let status = configuration_status::<S>;
    let initial = store.state(group(1)).unwrap();
    append(
        store,
        vec![update(
            &initial,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![learners()],
            }),
        )],
    );
    let state = store.state(group(1)).unwrap();
    let tickets = store
        .append_batch(vec![update(
            &state,
            1,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![joint()],
            }),
        )])
        .unwrap();
    // A submitted/Written joint is absent from the authoritative durable state.
    assert_eq!(
        status(store).accepted,
        ConfigurationProgress::NotFoundLocally
    );
    store.barrier(&tickets).unwrap();
    let pending = status(store);
    assert_eq!(pending.committed, ConfigurationProgress::NotFoundLocally);
    assert_eq!(
        pending.accepted,
        ConfigurationProgress::Joint {
            configuration: cid(3),
            target: cid(4),
            index: 2,
            term: Some(1)
        }
    );
    assert_eq!(
        pending.resume_action(),
        ConfigurationResumeAction::WaitForCommit
    );
    let state = store.state(group(1)).unwrap();
    append(store, vec![update(&state, 1, 2, None)]);
    assert_eq!(status(store).committed, pending.accepted);
    let EntryPayload::Configuration(final_record) = final_record().payload else {
        panic!()
    };
    assert_eq!(
        status(store).resume_action(),
        ConfigurationResumeAction::Finalize(*final_record)
    );
    pending
}

fn check_enrolled_status<S: LogStore>(store: &S) {
    let enrolled = store
        .state(group(1))
        .unwrap()
        .configuration_status(OperationId::new(100).unwrap())
        .unwrap();
    assert_eq!(
        enrolled.committed,
        ConfigurationProgress::Learners {
            configuration: cid(2),
            index: 1,
            term: 1
        }
    );
    assert_eq!(
        enrolled.resume_action(),
        ConfigurationResumeAction::Completed
    );
}
