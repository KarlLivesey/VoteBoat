// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Test-only oracles: replay of the full applied history (the `cfg(test)`
//! journal), replay of one entry from the checkpoint taken before it, and the
//! generation derivation the high-water mark replaced. Replaying only the
//! retained records (LV1's oracle) stopped being meaningful once LV2 prunes.
use super::*;
/// Rebuild state by replaying every entry applied since construction into a
/// fresh application; `None` after a restore (the history is unknown).
pub(in crate::logical_voter::application::tests) fn genesis_replay(
    a: &LogicalApplication,
) -> Option<application::State> {
    let journal = a.journal.clone()?;
    let mut next = a.clone();
    next.state = application::State::new(&a.child_bootstrap).unwrap();
    next.journal = Some(Vec::new());
    for entry in journal {
        next.apply_batch(&[entry]).unwrap();
    }
    Some(next.state)
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
/// Bound on retained records and journal length for the per-apply checks, so
/// the shared test helper stays cheap in tests that fill the action table.
const CHECKED_RECORDS: usize = 64;
const CHECKED_JOURNAL: usize = 256;
/// Asserted after every helper-driven apply in the application tests: the
/// watermark invariants hold, the high-water mark covers the former
/// derivation, the direct-decode restore rebuilds the exact state without
/// replay, and replaying the whole history rebuilds it too.
pub(in crate::logical_voter::application::tests) fn assert_restore_equivalent(
    a: &LogicalApplication,
) {
    assert!(a.state.snapshot_generation >= legacy_generation(&a.state));
    assert_watermark(a);
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
    if a.journal
        .as_ref()
        .is_some_and(|j| j.len() <= CHECKED_JOURNAL)
    {
        assert_eq!(genesis_replay(a).unwrap(), a.state);
    }
}
/// Restoring the checkpoint taken just before `entry` and replaying it gives
/// the same state and receipts (a checkpoint between every apply).
pub(in crate::logical_voter::application::tests) fn assert_step_replays(
    before: &[u8],
    entry: &LogEntry,
    receipts: &[LogicalReceipt],
    a: &LogicalApplication,
) {
    if a.state.records.len() > CHECKED_RECORDS {
        return;
    }
    let mut next = a.clone();
    next.state = application::State::new(&a.child_bootstrap).unwrap();
    next.restore_checkpoint(a.schema_version(), entry.index - 1, before)
        .unwrap();
    assert_eq!(
        next.apply_batch(std::slice::from_ref(entry)).unwrap(),
        receipts
    );
    assert_eq!(next.state, a.state);
}
/// Reachable-state watermark rules (architect §2.6): live session keys are at
/// or above it; it is 0 before the first claim and inside the current
/// representative's session afterwards.
pub(in crate::logical_voter::application::tests) fn assert_watermark(a: &LogicalApplication) {
    let w = a.state.retired_below;
    for op in a.state.records.keys() {
        assert!(
            op.get() < command::SESSION_SPACE || op.get() >= w,
            "live session key below the watermark"
        );
    }
    match a.state.representative {
        None => assert_eq!(w, 0),
        Some(r) => {
            assert_eq!(w >> 64, u128::from(r.logical_session.get()));
            assert!(w as u64 >= 1);
        }
    }
}
