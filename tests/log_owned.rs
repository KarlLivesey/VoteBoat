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
//! Consensus log transitions and the host-replaceable storage contract.
mod support;
use std::collections::BTreeMap;
use support::*;
use voteboat::{contracts::StorageError, identity::*, log::*, membership::*};

fn same(owned: &OwnedLogState, oracle: &BTreeMap<GroupIdentity, GroupLog>) {
    for (&id, state) in oracle {
        assert_eq!(owned.get(id), Some(state));
    }
}
fn compare(
    owned: &mut OwnedLogState,
    oracle: &mut BTreeMap<GroupIdentity, GroupLog>,
    batch: Vec<LogMutation>,
) {
    let before = oracle.clone();
    let expected = apply_batch(oracle, &batch, owned.limits());
    assert_eq!(owned.apply_batch(batch), expected);
    if expected.is_err() {
        assert_eq!(*oracle, before);
    }
    same(owned, oracle);
}
fn setup() -> (OwnedLogState, BTreeMap<GroupIdentity, GroupLog>) {
    let mut owned = OwnedLogState::new(LogLimits::default()).unwrap();
    let mut oracle = BTreeMap::new();
    compare(
        &mut owned,
        &mut oracle,
        (1..=3)
            .map(|id| LogMutation::Create(bootstrap(id, 3)))
            .collect(),
    );
    (owned, oracle)
}

#[test]
fn cross_group_failed_later_unit_and_reused_identity_keep_every_original_state() {
    let (mut owned, mut oracle) = setup();
    let valid = update(
        &oracle[&group(1)],
        1,
        0,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1, 1)],
        }),
    );
    let invalid = update(&oracle[&group(2)], 1, 1, None);
    compare(&mut owned, &mut oracle, vec![valid.clone(), invalid]);
    compare(&mut owned, &mut oracle, vec![valid.clone(), valid.clone()]);
    let mut reused = bootstrap(4, 3);
    reused.group.incarnation = GroupIncarnation::new(2).unwrap();
    compare(
        &mut owned,
        &mut oracle,
        vec![
            LogMutation::Create(bootstrap(4, 3)),
            LogMutation::Create(reused),
        ],
    );
    compare(&mut owned, &mut oracle, vec![valid]);
    assert_eq!(owned.into_states(), oracle);
}

#[test]
fn finite_ballot_commit_suffix_rows_match_materialized_map_validator() {
    for term in 0..=3 {
        for vote in [None, Some(node(1)), Some(node(4))] {
            for commit in 0..=3 {
                for from in 0..=3 {
                    let (mut owned, mut oracle) = setup();
                    let mut mutation = update(
                        &oracle[&group(1)],
                        term,
                        commit,
                        Some(Suffix {
                            from,
                            entries: vec![entry(from, term, 7)],
                        }),
                    );
                    let LogMutation::Update(proposal) = &mut mutation else {
                        unreachable!()
                    };
                    proposal.hard_state.voted_for = vote;
                    compare(&mut owned, &mut oracle, vec![mutation]);
                    assert_eq!(owned.into_states(), oracle);
                }
            }
        }
    }
}

#[test]
fn plain_append_and_metadata_keep_prior_payload_allocations_and_cold_groups() {
    let (mut owned, mut oracle) = setup();
    for id in 1..=3 {
        let mutation = update(
            &oracle[&group(id)],
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, id as u8), entry(2, 1, 1)],
            }),
        );
        compare(&mut owned, &mut oracle, vec![mutation]);
    }
    let pointers: Vec<_> = (1..=3)
        .map(
            |id| match &owned.get(group(id)).unwrap().entries[0].payload {
                EntryPayload::Command { bytes, .. } => bytes.as_ptr(),
                _ => unreachable!(),
            },
        )
        .collect();
    let append = update(
        &oracle[&group(1)],
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1, 3)],
        }),
    );
    compare(&mut owned, &mut oracle, vec![append]);
    let commit = update(&oracle[&group(1)], 1, 3, None);
    compare(&mut owned, &mut oracle, vec![commit]);
    for (id, pointer) in (1..=3).zip(pointers) {
        let EntryPayload::Command { bytes, .. } = &owned.get(group(id)).unwrap().entries[0].payload
        else {
            unreachable!()
        };
        assert_eq!(bytes.as_ptr(), pointer);
    }
    assert_eq!(owned.into_states(), oracle);
}

#[test]
fn configuration_snapshot_and_generation_replacement_keep_the_exact_fallback() {
    let (mut owned, mut oracle) = setup();
    let seed = update(
        &oracle[&group(1)],
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1, 1), entry(2, 1, 2)],
        }),
    );
    compare(&mut owned, &mut oracle, vec![seed]);
    let replacement = update(
        &oracle[&group(1)],
        2,
        1,
        Some(Suffix {
            from: 2,
            entries: vec![entry(2, 2, 8)],
        }),
    );
    let generation = oracle[&group(1)].generation;
    compare(&mut owned, &mut oracle, vec![replacement]);
    assert!(owned.get(group(1)).unwrap().generation > generation);
    let state = &oracle[&group(1)];
    let learners = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        state.bootstrap.policy.clone(),
        state.bootstrap.voter_stores.clone(),
        BTreeMap::new(),
    )
    .unwrap();
    let configuration = LogEntry {
        index: 3,
        term: 2,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(500).unwrap(),
            expected: state.bootstrap.configuration,
            change: ConfigurationChange::Learners(learners),
        })),
    };
    let accept = update(
        state,
        2,
        3,
        Some(Suffix {
            from: 3,
            entries: vec![configuration],
        }),
    );
    compare(&mut owned, &mut oracle, vec![accept]);
    let state = &oracle[&group(1)];
    let checkpoint = LogMutation::Update(LogUpdate {
        group: group(1),
        expected_revision: state.revision,
        hard_state: state.hard_state,
        commit_index: state.commit_index,
        suffix: None,
        snapshot_membership: state.checkpoint_membership(3).unwrap(),
        snapshot: Some(voteboat::snapshot::SnapshotRef {
            store: identity(1),
            group: group(1),
            configuration: ConfigurationId::new(2).unwrap(),
            generation: SnapshotGeneration::new(1).unwrap(),
            index: 3,
            term: 2,
            application_schema: 1,
            file_bytes: 100,
            checksum: 0,
        }),
    });
    compare(&mut owned, &mut oracle, vec![checkpoint]);
    let append = update(
        &oracle[&group(1)],
        2,
        4,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 2, 4)],
        }),
    );
    compare(&mut owned, &mut oracle, vec![append]);
    assert_eq!(owned.into_states(), oracle);
}

#[test]
fn immutable_limits_bound_new_groups_and_missing_lookup_has_no_side_effect() {
    assert!(OwnedLogState::new(LogLimits {
        max_groups: 0,
        ..LogLimits::default()
    })
    .is_err());
    let limits = LogLimits {
        max_groups: 1,
        max_entries_per_group: 1,
        ..LogLimits::default()
    };
    let mut owned = OwnedLogState::new(limits).unwrap();
    let mut oracle = BTreeMap::new();
    assert!(owned.get(group(1)).is_none());
    compare(
        &mut owned,
        &mut oracle,
        vec![LogMutation::Create(bootstrap(1, 3))],
    );
    compare(
        &mut owned,
        &mut oracle,
        vec![LogMutation::Create(bootstrap(2, 3))],
    );
    let overflow = update(
        &oracle[&group(1)],
        1,
        0,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1, 1), entry(2, 1, 2)],
        }),
    );
    compare(&mut owned, &mut oracle, vec![overflow]);
    assert_eq!(owned.limits().max_groups, 1);
    assert_eq!(
        owned.apply_batch(Vec::new()),
        Err(StorageError::Rejected("batch unit limit"))
    );
    assert_eq!(owned.into_states(), oracle);
}
