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
use voteboat::{contracts::StorageError, identity::GroupIncarnation, log::*};

fn populated() -> BTreeMap<voteboat::identity::GroupIdentity, GroupLog> {
    let mut state = BTreeMap::new();
    apply_batch(
        &mut state,
        &(1..=3)
            .map(|g| LogMutation::Create(bootstrap(g, 3)))
            .collect::<Vec<_>>(),
        LogLimits::default(),
    )
    .unwrap();
    for g in 1..=3 {
        let mutation = update(
            &state[&group(g)],
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, g as u8)].into(),
            }),
        );
        apply_batch(&mut state, &[mutation], LogLimits::default()).unwrap();
    }
    state
}

#[test]
fn later_invalid_transition_leaves_the_whole_batch_uninstalled() {
    let mut state = populated();
    let before = state.clone();
    let valid = update(
        &state[&group(1)],
        2,
        0,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 2, 7)].into(),
        }),
    );
    let invalid = update(&state[&group(2)], 1, 2, None);
    assert!(apply_batch(&mut state, &[valid.clone(), invalid], LogLimits::default()).is_err());
    assert_eq!(state, before);
    let cold_allocations: Vec<_> = state[&group(3)]
        .entries
        .iter()
        .map(std::ptr::from_ref)
        .collect();
    apply_batch(&mut state, &[valid], LogLimits::default()).unwrap();
    assert_eq!(
        state[&group(3)]
            .entries
            .iter()
            .map(std::ptr::from_ref)
            .collect::<Vec<_>>(),
        cold_allocations
    );
    assert_eq!(state[&group(2)], before[&group(2)]);
    assert!(state[&group(1)].generation > before[&group(1)].generation);
}

#[test]
fn creates_count_new_groups_and_reject_reused_ids_and_duplicate_units_atomically() {
    let limits = LogLimits {
        max_groups: 4,
        ..LogLimits::default()
    };
    let mut state = populated();
    let mutation = update(&state[&group(1)], 2, 0, None);
    apply_batch(
        &mut state,
        &[mutation, LogMutation::Create(bootstrap(4, 3))],
        limits,
    )
    .unwrap();
    let before = state.clone();
    assert!(apply_batch(&mut state, &[LogMutation::Create(bootstrap(5, 3))], limits).is_err());
    let mut reused = bootstrap(6, 3);
    reused.group.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(apply_batch(
        &mut state,
        &[
            LogMutation::Create(bootstrap(6, 3)),
            LogMutation::Create(reused),
        ],
        LogLimits::default()
    )
    .is_err());
    let mut existing = bootstrap(1, 3);
    existing.group.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(apply_batch(
        &mut state,
        &[LogMutation::Create(existing)],
        LogLimits::default()
    )
    .is_err());
    let mutation = update(&state[&group(1)], 3, 0, None);
    assert_eq!(
        apply_batch(&mut state, &[mutation.clone(), mutation], limits),
        Err(StorageError::Rejected(
            "one atomic unit per group per batch"
        ))
    );
    assert_eq!(state, before);
}
