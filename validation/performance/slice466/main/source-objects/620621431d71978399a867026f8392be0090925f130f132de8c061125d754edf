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
//! Configured deltas retain the validated electorate and historical promise.
use super::*;
use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};

fn configuration(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    Configuration::new(
        ConfigurationId::new(id).unwrap(),
        Policy::new(
            Tree::Majority(voters.iter().map(|&id| Tree::Voter(node(id))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voters
            .iter()
            .map(|&id| (node(id), store(u128::from(id))))
            .collect(),
        learners
            .iter()
            .map(|&id| (node(id), store(u128::from(id))))
            .collect(),
    )
    .unwrap()
}
fn accept(state: &GroupLog, operation: u128, change: ConfigurationChange) -> GroupLog {
    let index = state.last_index() + 1;
    let mut record = entry(index, state.hard_state.term);
    record.payload = EntryPayload::Configuration(Box::new(ConfigurationRecord {
        operation: OperationId::new(operation).unwrap(),
        expected: state.membership().unwrap().id(),
        change,
    }));
    stage_update(
        state,
        &proposal(
            state,
            state.commit_index,
            Some(Suffix {
                from: index,
                entries: vec![record].into(),
            }),
        ),
        LogLimits::default(),
    )
    .unwrap()
}
fn commit(state: &GroupLog, index: u64) -> GroupLog {
    stage_update(state, &proposal(state, index, None), LogLimits::default()).unwrap()
}
fn histories() -> Vec<GroupLog> {
    let mut initial = current();
    let mut promise = update(&initial, 1, 2, None);
    promise.hard_state.voted_for = Some(node(1));
    initial = stage_update(&initial, &promise, LogLimits::default()).unwrap();
    let learners = accept(
        &initial,
        100,
        ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4])),
    );
    let stable = commit(&learners, 3);
    let joint = accept(
        &stable,
        101,
        ConfigurationChange::Joint {
            id: ConfigurationId::new(3).unwrap(),
            next: configuration(4, &[2, 3, 4], &[]),
        },
    );
    let committed_joint = commit(&joint, 4);
    let final_state = accept(
        &committed_joint,
        101,
        ConfigurationChange::Final {
            id: ConfigurationId::new(4).unwrap(),
        },
    );
    let committed_final = commit(&final_state, 5);
    vec![
        learners,
        stable,
        joint,
        committed_joint,
        final_state,
        committed_final,
    ]
}
fn proposal(state: &GroupLog, commit: u64, suffix: Option<Suffix>) -> LogUpdate {
    let mut proposal = update(state, state.hard_state.term, commit, suffix);
    proposal.hard_state = state.hard_state;
    proposal
}
fn checkpoint(state: &GroupLog, index: u64) -> GroupLog {
    let mut proposal = proposal(state, state.commit_index, None);
    let membership = state.checkpoint_membership(index).unwrap();
    proposal.snapshot = Some(SnapshotRef {
        store: store(1),
        group: group(),
        configuration: membership.as_ref().unwrap().id(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index,
        term: state.term_at(index).unwrap(),
        application_schema: 1,
        file_bytes: 100,
        checksum: 0,
    });
    proposal.snapshot_membership = membership;
    stage_update(state, &proposal, LogLimits::default()).unwrap()
}
fn planned(state: &GroupLog, proposal: &LogUpdate) -> NextLog {
    NextLog::plan(
        &LogView::new(state),
        &LogSummary::from_validated(state),
        proposal,
        LogLimits::default(),
    )
    .unwrap()
}
fn append(state: &GroupLog) -> LogUpdate {
    let index = state.last_index() + 1;
    proposal(
        state,
        state.commit_index,
        Some(Suffix {
            from: index,
            entries: vec![entry(index, state.hard_state.term)].into(),
        }),
    )
}

#[test]
fn stable_joint_final_and_compacted_histories_match_original_validator() {
    let histories = histories();
    let mut states = histories.clone();
    states.push(checkpoint(&histories[1], 3));
    states.push(checkpoint(&histories[3], 4));
    states.push(checkpoint(&histories[5], 5));
    for state in states {
        for commit in state.commit_index.saturating_sub(1)..=state.last_index() + 2 {
            for suffix in [
                None,
                Some(Suffix {
                    from: state.last_index() + 1,
                    entries: vec![entry(state.last_index() + 1, 1)].into(),
                }),
                Some(Suffix {
                    from: state.last_index() + 1,
                    entries: vec![LogEntry {
                        index: state.last_index() + 1,
                        term: 1,
                        payload: EntryPayload::Noop,
                    }]
                    .into(),
                }),
            ] {
                compare(
                    &state,
                    proposal(&state, commit, suffix),
                    LogLimits::default(),
                );
            }
        }
        let plan = planned(&state, &append(&state));
        assert!(plan.full().is_none());
        let membership = state.membership().unwrap();
        let origin = state.ballot_origin;
        let mut installed = state.clone();
        let summary = plan.install(&mut installed, append(&state)).unwrap();
        assert!(!summary.is_plain());
        assert_eq!(installed.membership().unwrap(), membership);
        assert_eq!(installed.ballot_origin, origin);
        assert_eq!(installed.snapshot_membership, state.snapshot_membership);
    }
}

#[test]
fn configured_delta_preserves_old_payload_and_checkpoint_allocations() {
    let state = histories().remove(3);
    let mut state = checkpoint(&state, 3);
    // Add one retained command after the joint record and its compacted base.
    let incoming = append(&state);
    planned(&state, &incoming)
        .install(&mut state, incoming)
        .unwrap();
    let EntryPayload::Command { bytes, .. } = &state.entries.last().unwrap().payload else {
        unreachable!()
    };
    let pointer = bytes.as_ptr();
    let base_pointer = &**state.snapshot_membership.as_ref().unwrap() as *const _;
    let incoming = append(&state);
    let summary = planned(&state, &incoming)
        .install(&mut state, incoming)
        .unwrap();
    let EntryPayload::Command { bytes, .. } = &state.entries[state.entries.len() - 2].payload
    else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), pointer);
    assert_eq!(
        &**state.snapshot_membership.as_ref().unwrap() as *const _,
        base_pointer
    );
    assert!(!summary.is_plain());
    assert_eq!(summary, LogSummary::from_validated(&state));
    let pointers: Vec<_> = state.entries.iter().map(std::ptr::from_ref).collect();
    let incoming = proposal(&state, state.last_index(), None);
    planned(&state, &incoming)
        .install(&mut state, incoming)
        .unwrap();
    assert_eq!(
        state
            .entries
            .iter()
            .map(std::ptr::from_ref)
            .collect::<Vec<_>>(),
        pointers
    );
}

#[test]
fn changed_ballots_use_accepted_electorate_and_full_fallback() {
    let state = histories().remove(5);
    assert!(!state.membership().unwrap().is_voter(node(1)));
    assert_eq!(
        state.ballot_origin.unwrap().configuration,
        ConfigurationId::new(1).unwrap()
    );
    compare(&state, append(&state), LogLimits::default());
    for candidate in [None, Some(node(1)), Some(node(4))] {
        let mut incoming = proposal(&state, state.commit_index, None);
        incoming.hard_state.term = 2;
        incoming.hard_state.voted_for = candidate;
        compare(&state, incoming.clone(), LogLimits::default());
        if let Ok(plan) = NextLog::plan(
            &LogView::new(&state),
            &LogSummary::from_validated(&state),
            &incoming,
            LogLimits::default(),
        ) {
            assert!(plan.full().is_some());
            if candidate == Some(node(4)) {
                assert_eq!(
                    plan.metadata().ballot_origin.unwrap().configuration,
                    ConfigurationId::new(4).unwrap()
                );
            }
        }
    }
}

#[test]
fn configured_replacement_configuration_and_snapshot_keep_full_fallback() {
    let state = histories().remove(0);
    let replacement = proposal(
        &state,
        state.commit_index,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)].into(),
        }),
    );
    assert!(planned(&state, &replacement).full().is_some());
    compare(&state, replacement, LogLimits::default());
    let stable = commit(&state, 3);
    let next = accept(
        &stable,
        101,
        ConfigurationChange::Joint {
            id: ConfigurationId::new(3).unwrap(),
            next: configuration(4, &[2, 3, 4], &[]),
        },
    );
    let incoming = proposal(
        &stable,
        3,
        Some(Suffix {
            from: 4,
            entries: vec![next.entries.last().unwrap().clone()].into(),
        }),
    );
    assert!(planned(&stable, &incoming).full().is_some());
    compare(&stable, incoming, LogLimits::default());
    let compacted = checkpoint(&stable, 3);
    let mut incoming = proposal(&stable, 3, None);
    incoming.snapshot = compacted.snapshot;
    incoming.snapshot_membership = compacted.snapshot_membership;
    assert!(planned(&stable, &incoming).full().is_some());
    compare(&stable, incoming, LogLimits::default());
}

#[test]
fn configured_limits_stale_versions_and_exhaustion_keep_original_refusals() {
    let mut state = histories().remove(0);
    let incoming = append(&state);
    for limits in [
        LogLimits {
            max_entries_per_group: state.entries.len(),
            ..LogLimits::default()
        },
        LogLimits {
            max_command_bytes: 7,
            ..LogLimits::default()
        },
    ] {
        compare(&state, incoming.clone(), limits);
    }
    let summary = LogSummary::from_validated(&state);
    state.revision = LogRevision::new(u64::MAX).unwrap();
    assert!(NextLog::plan(
        &LogView::new(&state),
        &summary,
        &append(&state),
        LogLimits::default()
    )
    .is_err());
    compare(&state, append(&state), LogLimits::default());
}

#[test]
fn configured_overlay_tail_supports_unpublished_deltas_and_full_ballot_fallback() {
    let base = checkpoint(&histories().remove(3), 4);
    let first = append(&base);
    let NextLog::Delta(delta) = planned(&base, &first) else {
        panic!("configured delta required")
    };
    let metadata = delta.metadata();
    let tail = EntryHistory::from(delta.append.unwrap().entries);
    let accepted = stage_update(&base, &first, LogLimits::default()).unwrap();
    let mut second = append(&accepted);
    second.commit_index = second.suffix.as_ref().unwrap().from;
    let view = LogView::overlay(&base, &tail, metadata);
    let plan = NextLog::plan(&view, &delta.summary, &second, LogLimits::default()).unwrap();
    assert!(plan.full().is_none());
    let mut installed = accepted;
    let summary = plan.install(&mut installed, second).unwrap();
    assert!(!summary.is_plain());
    let mut ballot = proposal(&installed, installed.commit_index, None);
    ballot.hard_state = HardState {
        term: 2,
        voted_for: Some(node(4)),
    };
    assert!(planned(&installed, &ballot).full().is_some());
    compare(&installed, ballot, LogLimits::default());
}

#[test]
fn configured_matching_checkpoint_retains_full_membership_validator() {
    for state in histories() {
        let index = state.commit_index;
        let mut change = proposal(&state, index, None);
        change.snapshot_membership = state.checkpoint_membership(index).unwrap();
        change.snapshot = Some(SnapshotRef {
            store: store(1),
            group: group(),
            configuration: change
                .snapshot_membership
                .as_ref()
                .map_or(state.bootstrap.configuration, |base| base.id()),
            generation: SnapshotGeneration::new(1).unwrap(),
            index,
            term: state.term_at(index).unwrap(),
            application_schema: 1,
            file_bytes: 100,
            checksum: 7,
        });
        assert!(planned(&state, &change).full().is_some());
        compare(&state, change, LogLimits::default());
    }
}
