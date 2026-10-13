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
use super::*;
use crate::quorum::{Limits, Tree};

mod compact;
mod configured;
mod incoming;
mod oracle;
mod overlay;

fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn group() -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn entry(index: u64, term: u64) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Command {
            operation: OperationId::new(index as u128).unwrap(),
            bytes: vec![7; 8],
        },
    }
}
fn current() -> GroupLog {
    let bootstrap = Bootstrap {
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((1..=3).map(|id| Tree::Voter(node(id))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (1..=3).map(|id| (node(id), store(id as u128))).collect(),
    };
    let mut states = BTreeMap::new();
    apply_batch(
        &mut states,
        &[LogMutation::Create(bootstrap)],
        LogLimits::default(),
    )
    .unwrap();
    let state = states.remove(&group()).unwrap();
    stage_update(
        &state,
        &update(
            &state,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1), entry(2, 1)],
            }),
        ),
        LogLimits::default(),
    )
    .unwrap()
}
fn update(current: &GroupLog, term: u64, commit: u64, suffix: Option<Suffix>) -> LogUpdate {
    LogUpdate {
        group: current.bootstrap.group,
        expected_revision: current.revision,
        hard_state: HardState {
            term,
            voted_for: None,
        },
        commit_index: commit,
        suffix,
        snapshot: None,
        snapshot_membership: None,
    }
}
fn compare(current: &GroupLog, update: LogUpdate, limits: LogLimits) {
    let expected = stage_update(current, &update, limits);
    let plan = NextLog::plan(
        LogView::new(current),
        &LogSummary::from_validated(current),
        &update,
        limits,
    );
    match (expected, plan) {
        (Ok(expected), Ok(plan)) => {
            assert_eq!(plan.metadata(), LogMetadata::from_log(&expected));
            for index in 0..=expected.last_index().min(32) + 1 {
                assert_eq!(
                    plan.term_at(current, &update, index),
                    expected.term_at(index)
                );
            }
            let mut installed = current.clone();
            let summary = plan.install(&mut installed, update).unwrap();
            assert_eq!(installed, expected);
            assert_eq!(summary, LogSummary::from_validated(&installed));
        }
        (Err(expected), Err(actual)) => assert_eq!(actual, expected),
        (expected, _) => panic!("plan diverged from materialized validator: {expected:?}"),
    }
}

#[test]
fn plain_install_keeps_old_payloads_and_normalizes_only_new_allocations() {
    let mut state = current();
    let pointers: Vec<_> = state
        .entries
        .iter()
        .map(|entry| match &entry.payload {
            EntryPayload::Command { bytes, .. } => bytes.as_ptr(),
            _ => unreachable!(),
        })
        .collect();
    let mut new = entry(3, 1);
    if let EntryPayload::Command { bytes, .. } = &mut new.payload {
        bytes.reserve(10000);
    }
    let append = update(
        &state,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![new],
        }),
    );
    let plan = NextLog::plan(
        LogView::new(&state),
        &LogSummary::from_validated(&state),
        &append,
        LogLimits::default(),
    )
    .unwrap();
    assert!(plan.full().is_none());
    let summary = plan.install(&mut state, append).unwrap();
    for (entry, pointer) in state.entries.iter().zip(pointers) {
        let EntryPayload::Command { bytes, .. } = &entry.payload else {
            unreachable!()
        };
        assert_eq!(bytes.as_ptr(), pointer);
    }
    let EntryPayload::Command { bytes, .. } = &state.entries[2].payload else {
        unreachable!()
    };
    assert_eq!(bytes.capacity(), bytes.len());
    assert_eq!(summary, LogSummary::from_validated(&state));
    let commit = update(&state, 1, 3, None);
    let before = state.entries.as_ptr();
    let plan = NextLog::plan(
        LogView::new(&state),
        &summary,
        &commit,
        LogLimits::default(),
    )
    .unwrap();
    plan.install(&mut state, commit).unwrap();
    assert_eq!(state.entries.as_ptr(), before);
}

#[test]
fn stale_summary_or_wrong_install_payload_and_predecessor_cannot_mutate_log() {
    let state = current();
    let append = update(
        &state,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)],
        }),
    );
    let summary = LogSummary::from_validated(&state);
    let mut other = state.clone();
    other.revision = LogRevision::new(other.revision.get() + 1).unwrap();
    assert!(NextLog::plan(
        LogView::new(&state),
        &LogSummary::from_validated(&other),
        &append,
        LogLimits::default()
    )
    .is_err());
    for changed in [false, true] {
        let plan = NextLog::plan(
            LogView::new(&state),
            &summary,
            &append,
            LogLimits::default(),
        )
        .unwrap();
        let mut installed = state.clone();
        let mut wrong = append.clone();
        if changed {
            installed.revision = other.revision;
        } else {
            wrong.suffix.as_mut().unwrap().entries[0] = entry(3, 2);
        }
        let before = installed.clone();
        assert!(plan.install(&mut installed, wrong).is_err());
        assert_eq!(installed, before);
    }
}
