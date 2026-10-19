// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Test-only oracle: the restore algorithm used before checkpoint v3
//! (replay every retained record and configuration entry in log order through
//! `apply_entries`) and the generation derivation it relied on.
use super::*;
/// Rebuild state the pre-v3 way, from the retained history alone.
pub(in crate::logical_voter::application::tests) fn legacy_replay(
    a: &LogicalApplication,
) -> application::State {
    let mut next = a.clone();
    next.state = application::State::new(&a.child_bootstrap).unwrap();
    let mut entries: BTreeMap<u64, LogEntry> = a
        .state
        .records
        .values()
        .map(|r| {
            (
                r.first_index,
                LogEntry {
                    index: r.first_index,
                    term: r.first_term,
                    payload: EntryPayload::Command {
                        operation: r.operation,
                        bytes: r.command.clone().into(),
                    },
                },
            )
        })
        .collect();
    for e in &a.state.configurations {
        assert!(entries.insert(e.index, e.clone()).is_none());
    }
    for entry in entries.into_values() {
        next.state.applied = entry.index - 1;
        next.apply_entries(&[entry]).unwrap();
    }
    next.state.applied = a.state.applied;
    next.state.applied_term = a.state.applied_term;
    next.state
}
/// The generation derivation the high-water mark replaced.
pub(in crate::logical_voter::application::tests) fn legacy_generation(
    s: &application::State,
) -> u64 {
    s.records
        .values()
        .filter_map(|r| match r.receipt.outcome {
            LogicalOutcome::SnapshotPublished(r) => Some(r.generation.get()),
            _ => None,
        })
        .chain(
            s.enrollment
                .iter()
                .map(|r| r.target_reference.generation.get()),
        )
        .max()
        .unwrap_or(0)
}
/// Bound on retained records for the per-apply equivalence check, so the
/// shared test helper stays cheap in tests that fill the action table.
const CHECKED_RECORDS: usize = 64;
/// Asserted after every helper-driven apply in the application tests: the
/// high-water mark equals the former derivation, and the direct-decode
/// restore and the former replay restore both rebuild the exact state.
pub(in crate::logical_voter::application::tests) fn assert_restore_equivalent(
    a: &LogicalApplication,
) {
    assert_eq!(a.state.snapshot_generation, legacy_generation(&a.state));
    assert_eq!(a.state.retired_below, 0);
    if a.state.records.len() > CHECKED_RECORDS {
        return;
    }
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut direct = a.clone();
    direct.state = application::State::new(&a.child_bootstrap).unwrap();
    let (applies, _) = application::work();
    direct
        .restore_checkpoint(a.schema_version(), a.applied_index(), &bytes)
        .unwrap();
    assert_eq!(application::work().0, applies);
    assert_eq!(direct.state, a.state);
    assert_eq!(legacy_replay(a), a.state);
}
