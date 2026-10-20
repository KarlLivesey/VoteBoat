// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact comparison evidence belongs to protected canonical mutation owners.
use super::*;
use std::{fmt, sync::Arc};

/// Opaque volatile identity of one core's certified entry-preserving history.
/// Equality identifies a mutation domain, never data equality or durable work.
/// Recovery and rewriting installations create a fresh identity.
#[derive(Clone)]
pub struct CheckpointLineage(Arc<()>);
impl CheckpointLineage {
    pub(crate) fn new() -> Self {
        Self(Arc::new(()))
    }
}
impl PartialEq for CheckpointLineage {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for CheckpointLineage {}
impl fmt::Debug for CheckpointLineage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CheckpointLineage { opaque }")
    }
}

/// Immutable canonical state issued only by `Raft::checkpoint_log`.
/// The borrow cannot outlive its core and exposes no way to relabel other data.
/// Dev/debug profiles check optimized comparisons against full equality.
/// Release profiles preserve normal assertions and use the exact tracked path;
/// unit tests always retain the independent full-equality reference assertion.
pub struct CheckpointLog<'a> {
    state: &'a GroupLog,
    lineage: &'a CheckpointLineage,
}
impl<'a> CheckpointLog<'a> {
    pub(crate) fn new(state: &'a GroupLog, lineage: &'a CheckpointLineage) -> Self {
        Self { state, lineage }
    }
    pub fn state(&self) -> &'a GroupLog {
        self.state
    }
    pub fn group(&self) -> GroupIdentity {
        self.state.bootstrap.group
    }
    pub fn lineage(&self) -> &CheckpointLineage {
        self.lineage
    }
}

struct Verified {
    lineage: CheckpointLineage,
    revision: LogRevision,
    generation: LogGeneration,
    interval: Option<(u64, u64)>,
}
#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CheckpointComparisonWork {
    pub full: bool,
    /// Actual per-entry Eq invocations by the algorithm, excluding debug oracle.
    pub compared: usize,
    pub reused: usize,
}

/// One bounded record per actual provider/group owner; never clone this cache
/// into a fresh owner. Only certified append/metadata/compact installs preserve it.
#[derive(Default)]
pub(crate) struct CheckpointComparison {
    verified: Option<Verified>,
    #[cfg(test)]
    work: CheckpointComparisonWork,
}
impl CheckpointComparison {
    pub(crate) fn clear(&mut self) {
        self.verified = None;
    }
    /// Called only after a validated entry-preserving canonical installation.
    /// Old-generation evidence transfers only to its exact surviving interval.
    pub(crate) fn preserve(&mut self, state: &GroupLog) {
        let Some(interval) = interval(state) else {
            self.clear();
            return;
        };
        if let Some(verified) = &mut self.verified {
            verified.interval = overlap(verified.interval, interval);
            verified.revision = state.revision;
            verified.generation = state.generation;
        }
    }
    pub(crate) fn compare(&mut self, state: &GroupLog, expected: &CheckpointLog<'_>) -> bool {
        #[cfg(test)]
        {
            self.work = CheckpointComparisonWork::default();
        }
        let equal = self.compare_inner(state, expected);
        #[cfg(any(test, voteboat_checkpoint_debug_oracle))]
        assert_eq!(
            equal,
            state == expected.state(),
            "checkpoint comparison oracle"
        );
        equal
    }
    fn compare_inner(&mut self, state: &GroupLog, expected: &CheckpointLog<'_>) -> bool {
        if !headers_equal(state, expected.state()) {
            self.clear();
            return false;
        }
        let retained = interval(state);
        let expected_retained = interval(expected.state());
        let reusable = self.verified.as_ref().filter(|verified| {
            verified.lineage == *expected.lineage()
                && verified.revision == state.revision
                && verified.generation == state.generation
                && retained.is_some()
                && retained == expected_retained
        });
        let skipped = reusable
            .and_then(|verified| overlap(verified.interval, retained.flatten()))
            .and_then(|(start, end)| {
                let base = state.base_index();
                let begin = usize::try_from(start.checked_sub(base.checked_add(1)?)?).ok()?;
                let end = usize::try_from(end.checked_sub(base)?).ok()?;
                (begin <= end && end <= state.entries.len()).then_some((begin, end))
            });
        #[cfg(test)]
        {
            self.work.full = reusable.is_none();
            self.work.reused = skipped.map_or(0, |(begin, end)| end - begin);
        }
        let length = state.entries.len();
        let (begin, end) = skipped.unwrap_or((length, length));
        for range in [0..begin, end..length] {
            for (left, right) in state
                .entries
                .range(range.clone())
                .into_iter()
                .flatten()
                .zip(expected.state().entries.range(range).into_iter().flatten())
            {
                #[cfg(test)]
                {
                    self.work.compared += 1;
                }
                if left != right {
                    self.clear();
                    return false;
                }
            }
        }
        self.verified = retained
            .filter(|_| retained == expected_retained)
            .map(|interval| Verified {
                lineage: expected.lineage().clone(),
                revision: state.revision,
                generation: state.generation,
                interval,
            });
        true
    }
    #[cfg(test)]
    pub(crate) fn work(&self) -> CheckpointComparisonWork {
        self.work
    }
    #[cfg(test)]
    pub(crate) fn verified_interval(&self) -> Option<(u64, u64)> {
        self.verified
            .as_ref()
            .and_then(|verified| verified.interval)
    }
}

// None means inconsistent/overflowed bounds; Some(None) is a valid empty log.
fn interval(state: &GroupLog) -> Option<Option<(u64, u64)>> {
    if state.entries.is_empty() {
        return Some(None);
    }
    let start = state.base_index().checked_add(1)?;
    let count = u64::try_from(state.entries.len()).ok()?;
    let end = state.base_index().checked_add(count)?;
    (state.entries.first()?.index == start && state.entries.last()?.index == end)
        .then_some(Some((start, end)))
}
fn overlap(left: Option<(u64, u64)>, right: Option<(u64, u64)>) -> Option<(u64, u64)> {
    let (left_start, left_end) = left?;
    let (right_start, right_end) = right?;
    let start = left_start.max(right_start);
    let end = left_end.min(right_end);
    (start <= end).then_some((start, end))
}
fn headers_equal(left: &GroupLog, right: &GroupLog) -> bool {
    let GroupLog {
        bootstrap,
        revision,
        generation,
        hard_state,
        ballot_origin,
        commit_index,
        snapshot,
        snapshot_membership,
        entries,
    } = left;
    let GroupLog {
        bootstrap: other_bootstrap,
        revision: other_revision,
        generation: other_generation,
        hard_state: other_hard_state,
        ballot_origin: other_ballot_origin,
        commit_index: other_commit_index,
        snapshot: other_snapshot,
        snapshot_membership: other_snapshot_membership,
        entries: other_entries,
    } = right;
    bootstrap == other_bootstrap
        && revision == other_revision
        && generation == other_generation
        && hard_state == other_hard_state
        && ballot_origin == other_ballot_origin
        && commit_index == other_commit_index
        && snapshot == other_snapshot
        && snapshot_membership == other_snapshot_membership
        && entries.len() == other_entries.len()
}

#[cfg(test)]
mod tests;
