// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    contracts::HardState,
    quorum::{Limits, Policy, Tree},
    snapshot::SnapshotRef,
};

fn state() -> GroupLog {
    let owned = crate::log::owned::tests::setup();
    let mut state = owned.into_states().into_values().next().unwrap();
    state.hard_state.term = 1;
    state.entries = (1..=3).map(entry).collect();
    state
}
fn entry(index: u64) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new((1 << 127) | u128::from(index)).unwrap(),
            bytes: vec![7; usize::try_from(index % 3).unwrap()].into(),
        },
    }
}
fn snapshot(state: &GroupLog, index: u64) -> SnapshotRef {
    SnapshotRef {
        store: *state.bootstrap.voter_stores.values().next().unwrap(),
        group: state.bootstrap.group,
        configuration: state.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    }
}
fn work(cache: &CheckpointComparison, full: bool, compared: usize, reused: usize) {
    assert_eq!(
        cache.work(),
        CheckpointComparisonWork {
            full,
            compared,
            reused
        }
    );
}

#[test]
fn certified_append_and_compaction_compare_only_unverified_survivors() {
    let mut state = state();
    let lineage = CheckpointLineage::new();
    let mut cache = CheckpointComparison::default();
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, true, 3, 0);
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, false, 0, 3);
    state.entries.push(entry(4));
    state.revision = LogRevision::new(2).unwrap();
    cache.preserve(&state);
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, false, 1, 3);
    state.snapshot = Some(snapshot(&state, 2));
    state.entries.discard_prefix(2);
    state.revision = LogRevision::new(3).unwrap();
    state.generation = LogGeneration::new(2).unwrap();
    cache.preserve(&state);
    assert_eq!(cache.verified_interval(), Some((3, 4)));
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, false, 0, 2);
    state.entries.push(entry(5));
    state.revision = LogRevision::new(4).unwrap();
    cache.preserve(&state);
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, false, 1, 2);
    cache.clear();
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, true, 3, 0);
}

#[test]
fn fresh_and_alternating_owners_never_reuse_another_mutation_domain() {
    let state = state();
    let first = CheckpointLineage::new();
    let second = CheckpointLineage::new();
    assert_ne!(first, second);
    assert_eq!(first, first.clone());
    let mut cache = CheckpointComparison::default();
    for lineage in [&first, &second, &first] {
        assert!(cache.compare(&state, &CheckpointLog::new(&state, lineage)));
        work(&cache, true, 3, 0);
    }
    let mut replacement = CheckpointComparison::default();
    assert!(replacement.compare(&state, &CheckpointLog::new(&state, &first)));
    work(&replacement, true, 3, 0);
}

#[test]
fn false_tail_and_uncertified_versions_clear_or_refuse_cached_evidence() {
    let mut state = state();
    let lineage = CheckpointLineage::new();
    let mut cache = CheckpointComparison::default();
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    state.entries.push(entry(4));
    state.revision = LogRevision::new(2).unwrap();
    cache.preserve(&state);
    let mut different = state.clone();
    let mut replacement = different.entries[3].clone();
    replacement.payload = EntryPayload::Noop;
    different.entries.replace(3, replacement).unwrap();
    assert!(!cache.compare(&state, &CheckpointLog::new(&different, &lineage)));
    work(&cache, false, 1, 3);
    assert_eq!(cache.verified_interval(), None);
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, true, 4, 0);
    state.revision = LogRevision::new(3).unwrap();
    // No certified installation was reported to this provider record.
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, true, 4, 0);
}

#[test]
fn every_header_field_stays_exact_and_false_clears_the_previous_interval() {
    let state = state();
    let lineage = CheckpointLineage::new();
    let changes: &[fn(&mut GroupLog)] = &[
        |s| s.bootstrap.group.incarnation = GroupIncarnation::new(99).unwrap(),
        |s| s.bootstrap.configuration = ConfigurationId::new(99).unwrap(),
        |s| {
            s.bootstrap.policy =
                Policy::new(Tree::Voter(NodeId::new(2).unwrap()), Limits::default()).unwrap()
        },
        |s| s.bootstrap.voter_stores.clear(),
        |s| s.revision = LogRevision::new(99).unwrap(),
        |s| s.generation = LogGeneration::new(99).unwrap(),
        |s| {
            s.hard_state = HardState {
                term: 2,
                voted_for: Some(NodeId::new(1).unwrap()),
            }
        },
        |s| {
            s.ballot_origin = Some(BallotOrigin {
                configuration: ConfigurationId::new(1).unwrap(),
                candidate_store: *s.bootstrap.voter_stores.values().next().unwrap(),
            })
        },
        |s| s.commit_index = 1,
        |s| s.snapshot = Some(snapshot(s, 1)),
        |s| {
            s.snapshot_membership = Some(Box::new(
                crate::membership::Membership::replay(&s.bootstrap, &[], 0).unwrap(),
            ))
        },
        |s| {
            s.entries.truncate(s.entries.len() - 1);
        },
    ];
    for change in changes {
        let mut cache = CheckpointComparison::default();
        assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
        let mut different = state.clone();
        change(&mut different);
        assert_ne!(different, state);
        assert!(!cache.compare(&state, &CheckpointLog::new(&different, &lineage)));
        assert_eq!(cache.verified_interval(), None);
    }
}

#[test]
fn empty_nonoverlap_and_overflowed_bounds_never_grant_skipped_entries() {
    let mut state = state();
    let lineage = CheckpointLineage::new();
    let mut cache = CheckpointComparison::default();
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    state.snapshot = Some(snapshot(&state, 3));
    state.entries.clear();
    state.revision = LogRevision::new(2).unwrap();
    state.generation = LogGeneration::new(2).unwrap();
    cache.preserve(&state);
    assert_eq!(cache.verified_interval(), None);
    state.entries.push(entry(4));
    state.revision = LogRevision::new(3).unwrap();
    cache.preserve(&state);
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, false, 1, 0);
    state.snapshot.as_mut().unwrap().index = u64::MAX;
    cache.preserve(&state);
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    work(&cache, true, 1, 0);
    assert_eq!(cache.verified_interval(), None);
}

#[test]
#[should_panic(expected = "checkpoint comparison oracle")]
fn reference_oracle_detects_test_only_violation_of_private_mutation_ownership() {
    let state = state();
    let lineage = CheckpointLineage::new();
    let mut cache = CheckpointComparison::default();
    assert!(cache.compare(&state, &CheckpointLog::new(&state, &lineage)));
    let mut rewritten = state.clone();
    let mut replacement = rewritten.entries[0].clone();
    replacement.payload = EntryPayload::Noop;
    rewritten.entries.replace(0, replacement).unwrap();
    // No public API can attach this existing lineage to rewritten public data.
    cache.compare(&state, &CheckpointLog::new(&rewritten, &lineage));
}
