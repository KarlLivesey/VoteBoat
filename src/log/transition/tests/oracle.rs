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

#[test]
fn finite_plain_ballot_commit_suffix_and_budget_cases_match_original_validator() {
    let state = current();
    let suffixes = [
        None,
        Some(Suffix {
            from: 3,
            entries: vec![],
        }),
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1), entry(4, 2)],
        }),
        Some(Suffix {
            from: 3,
            entries: vec![entry(4, 1)],
        }),
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 0)],
        }),
        Some(Suffix {
            from: 2,
            entries: vec![entry(2, 2)],
        }),
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 1)],
        }),
    ];
    for term in 0..=3 {
        for voted_for in [None, Some(node(1)), Some(node(4))] {
            for commit in 0..=5 {
                for suffix in &suffixes {
                    let mut proposal = update(&state, term, commit, suffix.clone());
                    proposal.hard_state.voted_for = voted_for;
                    compare(&state, proposal, LogLimits::default());
                }
            }
        }
    }
    let append = update(
        &state,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)],
        }),
    );
    compare(
        &state,
        append.clone(),
        LogLimits {
            max_entries_per_group: 2,
            ..LogLimits::default()
        },
    );
    compare(
        &state,
        append,
        LogLimits {
            max_command_bytes: 7,
            ..LogLimits::default()
        },
    );
}

#[test]
fn retained_historical_ballot_origin_and_new_votes_match_original_validator() {
    let mut state = current();
    state.hard_state.voted_for = Some(node(1));
    state.ballot_origin = Some(BallotOrigin {
        configuration: ConfigurationId::new(9).unwrap(),
        candidate_store: store(7),
    });
    for (term, voted_for) in [
        (1, Some(node(1))),
        (2, Some(node(2))),
        (2, None),
        (1, Some(node(2))),
    ] {
        let mut proposal = update(&state, term, 2, None);
        proposal.hard_state.voted_for = voted_for;
        compare(&state, proposal, LogLimits::default());
    }
}

#[test]
fn checked_index_and_revision_exhaustion_matches_fallback_without_arithmetic_panic() {
    let mut state = current();
    state.revision = LogRevision::new(u64::MAX).unwrap();
    compare(&state, update(&state, 1, 2, None), LogLimits::default());
    state.revision = LogRevision::new(2).unwrap();
    state.snapshot = Some(SnapshotRef {
        store: store(1),
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index: u64::MAX - 2,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 0,
    });
    state.entries.clear();
    state.commit_index = u64::MAX - 2;
    compare(
        &state,
        update(
            &state,
            1,
            state.commit_index,
            Some(Suffix {
                from: u64::MAX - 1,
                entries: vec![entry(u64::MAX - 1, 1), entry(u64::MAX, 1), entry(1, 1)],
            }),
        ),
        LogLimits::default(),
    );
}

#[test]
fn configuration_replacement_fall_back_and_plain_matching_snapshot_uses_compact() {
    use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
    let state = current();
    let configuration = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        state.bootstrap.policy.clone(),
        state.bootstrap.voter_stores.clone(),
        BTreeMap::new(),
    )
    .unwrap();
    let mut record = entry(3, 1);
    record.payload = EntryPayload::Configuration(Box::new(ConfigurationRecord {
        operation: OperationId::new(55).unwrap(),
        expected: state.bootstrap.configuration,
        change: ConfigurationChange::Learners(configuration),
    }));
    let mut snapshot = update(&state, 1, 1, None);
    snapshot.snapshot = Some(SnapshotRef {
        store: store(1),
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 0,
    });
    for proposal in [
        update(
            &state,
            1,
            1,
            Some(Suffix {
                from: 3,
                entries: vec![record],
            }),
        ),
        snapshot,
        update(
            &state,
            2,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![entry(2, 2)],
            }),
        ),
    ] {
        let plan = NextLog::plan(
            LogView::new(&state),
            &LogSummary::from_validated(&state),
            &proposal,
            LogLimits::default(),
        )
        .unwrap();
        if proposal.snapshot.is_some() {
            assert!(matches!(plan, NextLog::Compact(_)));
        } else {
            assert!(plan.full().is_some());
        }
        compare(&state, proposal, LogLimits::default());
    }
}
