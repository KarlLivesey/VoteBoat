// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Plain matching-prefix compaction, prepared without materializing history.
use super::*;

pub(crate) struct CompactDelta {
    predecessor: LogMetadata,
    pub(crate) removed: usize,
    pub(crate) summary: LogSummary,
}
impl CompactDelta {
    pub(crate) fn metadata(&self) -> LogMetadata {
        self.summary.metadata
    }
    pub(crate) fn check_install(
        &self,
        current: LogMetadata,
        update: &LogUpdate,
    ) -> Result<(), StorageError> {
        // Every field of the original snapshot-only update is represented by
        // predecessor/next metadata or the required absent payload variants.
        let LogUpdate {
            group,
            expected_revision,
            hard_state,
            commit_index,
            snapshot,
            suffix,
            snapshot_membership,
        } = update;
        let next = self.metadata();
        if current != self.predecessor
            || *group != current.group
            || *expected_revision != current.revision
            || *hard_state != next.hard_state
            || *commit_index != next.commit_index
            || *snapshot != next.snapshot
            || suffix.is_some()
            || snapshot_membership.is_some()
        {
            return Err(StorageError::Rejected(
                "transition plan predecessor or update mismatch",
            ));
        }
        Ok(())
    }
    pub(crate) fn install(self, current: &mut GroupLog) -> LogSummary {
        current.entries.drain(..self.removed);
        self.metadata().install_header(current);
        self.summary
    }
}
pub(super) fn eligible(view: LogView<'_>, summary: &LogSummary, update: &LogUpdate) -> bool {
    summary.plain
        && summary.clone_stable
        && update.hard_state == view.metadata().hard_state
        && update.snapshot_membership.is_none()
        && update.suffix.is_none()
        && update.snapshot.is_some_and(|reference| {
            reference.index <= view.metadata().commit_index
                && view.term_at(reference.index) == Some(reference.term)
        })
}
pub(super) fn plan(
    view: LogView<'_>,
    summary: &LogSummary,
    update: &LogUpdate,
    limits: LogLimits,
) -> Result<CompactDelta, StorageError> {
    let current = view.metadata();
    if update.expected_revision != current.revision
        || !update.hard_state.follows(current.hard_state)
    {
        return Err(StorageError::Rejected("stale revision or invalid ballot"));
    }
    validate_ballot_fields(view.bootstrap(), current.hard_state, current.ballot_origin)?;
    // Complete HardState equality retains the exact validated historical origin.
    let reference = update.snapshot.expect("eligible compact snapshot");
    if reference.group != update.group
        || reference.configuration != view.bootstrap().configuration
        || reference.index <= current.base_index
        || reference.index == u64::MAX
        || reference.term == 0
        || reference.term > update.hard_state.term
        || reference.application_schema == 0
        || reference.file_bytes < 48
        || reference.file_bytes > limits.max_snapshot_bytes as u64 + 4 * 1024 * 1024 + 48
        || update.commit_index < reference.index
    {
        return Err(StorageError::Rejected("invalid snapshot boundary/scope"));
    }
    // Exact validated plain history has no configuration checkpoint to replay.
    let removed = usize::try_from(reference.index - current.base_index)
        .map_err(|_| StorageError::Rejected("snapshot index overflow"))?;
    let mut next = current;
    next.generation = current
        .generation
        .get()
        .checked_add(1)
        .and_then(LogGeneration::new)
        .ok_or(StorageError::Rejected("generation exhausted"))?;
    if update.commit_index < current.commit_index
        || update.commit_index > current.last_index
        || current.last_term > update.hard_state.term
    {
        return Err(StorageError::Rejected(
            "commit regression/gap or hard term behind log",
        ));
    }
    next.revision = current
        .revision
        .get()
        .checked_add(1)
        .and_then(LogRevision::new)
        .ok_or(StorageError::Rejected("revision exhausted"))?;
    next.snapshot = Some(reference);
    next.base_index = reference.index;
    next.base_term = reference.term;
    next.entries_len -= removed;
    next.commit_index = update.commit_index;
    let retained = retained_after(view, summary.retained, removed, next.entries_len);
    Ok(CompactDelta {
        predecessor: current,
        removed,
        summary: LogSummary {
            metadata: next,
            retained,
            ..*summary
        },
    })
}
fn retained_after(
    view: LogView<'_>,
    retained: Option<usize>,
    removed: usize,
    left: usize,
) -> Option<usize> {
    let removed_bytes = removed
        .checked_mul(size_of::<LogEntry>())
        .and_then(|bytes| {
            view.entries().take(removed).try_fold(bytes, |sum, entry| {
                sum.checked_add(entry.retained_payload_bytes())
            })
        });
    if let Some(bytes) = retained.and_then(|bytes| bytes.checked_sub(removed_bytes?)) {
        return Some(bytes);
    }
    // Trimming can turn a previous accounting overflow into a representable charge.
    view.entries()
        .skip(removed)
        .try_fold(left.checked_mul(size_of::<LogEntry>())?, |sum, entry| {
            sum.checked_add(entry.retained_payload_bytes())
        })
}
