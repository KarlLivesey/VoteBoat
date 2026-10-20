// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Sealed owned incoming updates; scalar admission precedes copying/normalization.
use super::*;

/// An admitted plain append. Every other field of its update is the
/// predecessor's or the next summary's, so only the suffix is retained.
pub(crate) struct OwnedAppend {
    predecessor: LogMetadata,
    summary: LogSummary,
    suffix: Option<Suffix>,
}
/// The core's own commit-only round: an empty append planned without an
/// update. Providers never receive one.
pub(crate) struct CommitAdvance {
    predecessor: LogMetadata,
    summary: LogSummary,
}
pub(crate) enum OwnedChange {
    Append(OwnedAppend),
    Commit(CommitAdvance),
    // Ballot, configuration, snapshot and compaction plans are cold.
    Other {
        next: Box<NextLog>,
        update: LogUpdate,
    },
}
pub(crate) struct OwnedNextLog(OwnedChange);
pub(crate) enum PreparedOwnedLog {
    /// Checked against its predecessor; a commit advance has no suffix.
    Append(OwnedAppend),
    Other(PreparedLog),
}
impl OwnedNextLog {
    pub(crate) fn preserves_checkpoint_prefix(&self) -> bool {
        match &self.0 {
            OwnedChange::Append(_) | OwnedChange::Commit(_) => true,
            OwnedChange::Other { next, .. } => {
                matches!(**next, NextLog::Delta(_) | NextLog::Compact(_))
            }
        }
    }
    /// Core needs one independent normalized copy while its original crosses
    /// the public owned provider boundary. Inspect the raw input first.
    pub(crate) fn plan_copy(
        view: &LogView<'_>,
        summary: &LogSummary,
        update: &LogUpdate,
        limits: LogLimits,
    ) -> Result<Self, StorageError> {
        let limits = validate_base(view, summary, update, limits)?;
        if append_eligible(view, summary, update) {
            let metadata = validate_append_metadata(view, update, limits)?;
            Ok(Self(OwnedChange::Append(OwnedAppend::new(
                view,
                summary,
                metadata,
                update.suffix.clone(),
            ))))
        } else {
            Ok(Self(OwnedChange::Other {
                next: Box::new(NextLog::plan(view, summary, update, limits)?),
                update: update.clone(),
            }))
        }
    }
    /// A provider already owns this exact input. Its shared entries move into
    /// the plan; no retained canonical history is copied.
    pub(crate) fn plan_owned(
        view: &LogView<'_>,
        summary: &LogSummary,
        update: LogUpdate,
        limits: LogLimits,
    ) -> Result<Self, StorageError> {
        let limits = validate_base(view, summary, &update, limits)?;
        if append_eligible(view, summary, &update) {
            let metadata = validate_append_metadata(view, &update, limits)?;
            Ok(Self(OwnedChange::Append(OwnedAppend::new(
                view,
                summary,
                metadata,
                update.suffix,
            ))))
        } else {
            let next = Box::new(NextLog::plan(view, summary, &update, limits)?);
            Ok(Self(OwnedChange::Other { next, update }))
        }
    }
    /// The core's own commit-only round: the update `plan_owned` would get has
    /// this log's group, revision and hard state, no suffix and no snapshot.
    /// Runs the checks `validate_base` and `validate_append_metadata` make for
    /// it, in the same order with the same errors, and keeps no update.
    pub(crate) fn plan_commit(
        view: &LogView<'_>,
        summary: &LogSummary,
        commit_index: u64,
        limits: LogLimits,
    ) -> Result<Self, StorageError> {
        limits.validate()?;
        if !view.summarized_by(summary) {
            return Err(StorageError::Rejected("stale log summary"));
        }
        let current = view.metadata();
        if !current.hard_state.follows(current.hard_state) {
            return Err(StorageError::Rejected("stale revision or invalid ballot"));
        }
        // An unchanged ballot retains its validated origin.
        validate_ballot_fields(view.bootstrap(), current.hard_state, current.ballot_origin)?;
        if commit_index < current.commit_index
            || commit_index > current.last_index
            || current.last_term > current.hard_state.term
        {
            return Err(StorageError::Rejected(
                "commit regression/gap or hard term behind log",
            ));
        }
        let revision = current
            .revision
            .get()
            .checked_add(1)
            .and_then(LogRevision::new)
            .ok_or(StorageError::Rejected("revision exhausted"))?;
        let next = LogMetadata {
            revision,
            commit_index,
            ..*current
        };
        Ok(Self(OwnedChange::Commit(CommitAdvance {
            predecessor: *current,
            summary: append_summary(view, summary, next, None),
        })))
    }
    /// The planned suffix, borrowed; a commit advance has none.
    pub(crate) fn suffix(&self) -> Option<&Suffix> {
        match &self.0 {
            OwnedChange::Append(append) => append.suffix.as_ref(),
            OwnedChange::Commit(_) => None,
            OwnedChange::Other { update, .. } => update.suffix.as_ref(),
        }
    }
    /// The exact update this plan was admitted from (or stands for).
    #[cfg(test)]
    pub(crate) fn update(&self) -> LogUpdate {
        match &self.0 {
            OwnedChange::Append(append) => plain_update(
                &append.predecessor,
                &append.summary.metadata,
                append.suffix.clone(),
            ),
            OwnedChange::Commit(commit) => {
                plain_update(&commit.predecessor, &commit.summary.metadata, None)
            }
            OwnedChange::Other { update, .. } => update.clone(),
        }
    }
    #[cfg(test)]
    pub(crate) fn is_commit(&self) -> bool {
        matches!(self.0, OwnedChange::Commit(_))
    }
    pub(crate) fn matches_update(&self, original: &LogUpdate) -> bool {
        let owned = match &self.0 {
            OwnedChange::Append(append) => {
                return matches_plain(
                    original,
                    &append.predecessor,
                    &append.summary.metadata,
                    append.suffix.as_ref(),
                );
            }
            OwnedChange::Commit(commit) => {
                return matches_plain(
                    original,
                    &commit.predecessor,
                    &commit.summary.metadata,
                    None,
                );
            }
            OwnedChange::Other { update, .. } => update,
        };
        let LogUpdate {
            group,
            expected_revision,
            hard_state,
            commit_index,
            suffix,
            snapshot,
            snapshot_membership,
        } = original;
        *group == owned.group
            && *expected_revision == owned.expected_revision
            && *hard_state == owned.hard_state
            && *commit_index == owned.commit_index
            && *suffix == owned.suffix
            && *snapshot == owned.snapshot
            && *snapshot_membership == owned.snapshot_membership
    }
    pub(crate) fn metadata(&self) -> LogMetadata {
        match &self.0 {
            OwnedChange::Append(append) => append.metadata(),
            OwnedChange::Commit(commit) => commit.summary.metadata,
            OwnedChange::Other { next, .. } => next.metadata(),
        }
    }
    pub(crate) fn full(&self) -> Option<&GroupLog> {
        match &self.0 {
            OwnedChange::Append(_) | OwnedChange::Commit(_) => None,
            OwnedChange::Other { next, .. } => next.full(),
        }
    }
    pub(crate) fn is_append(&self) -> bool {
        matches!(self.0, OwnedChange::Append(_))
    }
    pub(crate) fn term_at(&self, current: &GroupLog, index: u64) -> Option<u64> {
        match &self.0 {
            OwnedChange::Append(_) => self
                .entry_at(index)
                .map(|entry| entry.term)
                .or_else(|| current.term_at(index)),
            OwnedChange::Commit(_) => current.term_at(index),
            OwnedChange::Other { next, update } => next.term_at(current, update, index),
        }
    }
    pub(crate) fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        let OwnedChange::Append(append) = &self.0 else {
            return None;
        };
        let suffix = append.suffix.as_ref()?;
        suffix
            .entries
            .get(usize::try_from(index.checked_sub(suffix.from)?).ok()?)
    }
    pub(crate) fn prepare_install(
        self,
        current: &GroupLog,
    ) -> Result<PreparedOwnedLog, StorageError> {
        match self.0 {
            OwnedChange::Append(append) => {
                append.check_install(LogMetadata::from_log(current))?;
                Ok(PreparedOwnedLog::Append(append))
            }
            OwnedChange::Commit(CommitAdvance {
                predecessor,
                summary,
            }) => {
                let append = OwnedAppend {
                    predecessor,
                    summary,
                    suffix: None,
                };
                append.check_install(LogMetadata::from_log(current))?;
                Ok(PreparedOwnedLog::Append(append))
            }
            OwnedChange::Other { next, update } => (*next)
                .prepare_install(current, &update)
                .map(PreparedOwnedLog::Other),
        }
    }
    pub(crate) fn install(self, current: &mut GroupLog) -> Result<LogSummary, StorageError> {
        Ok(self.prepare_install(current)?.install(current))
    }
    /// [`Self::install`] for a plan made from a [`SummarizedLog`] view of
    /// `current`, with no change to `current` since (its version is
    /// unchanged): the predecessor is `current`'s metadata, so an append or
    /// commit advance installs without comparing it. Other plans keep every
    /// check. The installed log's summary is written to `summary`, which is
    /// left unchanged on error.
    pub(super) fn install_unchanged(
        self,
        current: &mut GroupLog,
        summary: &mut LogSummary,
    ) -> Result<(), StorageError> {
        match self.0 {
            OwnedChange::Append(append) => {
                debug_assert!(append.check_install(LogMetadata::from_log(current)).is_ok());
                *summary = install_append(current, append.suffix, append.summary);
            }
            OwnedChange::Commit(commit) => {
                debug_assert_eq!(commit.predecessor, LogMetadata::from_log(current));
                *summary = install_append(current, None, commit.summary);
            }
            other @ OwnedChange::Other { .. } => *summary = Self(other).install(current)?,
        }
        Ok(())
    }
    #[cfg(feature = "native")]
    pub(crate) fn into_change(self) -> OwnedChange {
        self.0
    }
    #[cfg(test)]
    pub(crate) fn full_oracle(
        view: LogView<'_>,
        update: &LogUpdate,
        limits: LogLimits,
    ) -> Result<Self, StorageError> {
        let mut next = view.materialize();
        apply_update(&mut next, update, limits)?;
        Ok(Self(OwnedChange::Other {
            next: Box::new(NextLog::Full {
                predecessor: *view.metadata(),
                next: Box::new(next),
                update: update.clone(),
            }),
            update: update.clone(),
        }))
    }
}
impl OwnedAppend {
    fn new(
        view: &LogView<'_>,
        summary: &LogSummary,
        metadata: LogMetadata,
        suffix: Option<Suffix>,
    ) -> Self {
        Self {
            predecessor: *view.metadata(),
            summary: append_summary(view, summary, metadata, suffix.as_ref()),
            suffix,
        }
    }
    pub(crate) fn metadata(&self) -> LogMetadata {
        self.summary.metadata
    }
    #[cfg(feature = "native")]
    pub(crate) fn summary(&self) -> LogSummary {
        self.summary
    }
    pub(crate) fn check_install(&self, current: LogMetadata) -> Result<(), StorageError> {
        if current != self.predecessor {
            return Err(StorageError::Rejected(
                "transition plan predecessor or update mismatch",
            ));
        }
        // The original is immutable and bound to this plan before sealing.
        Ok(())
    }
    #[cfg(feature = "native")]
    pub(crate) fn predecessor(&self) -> LogMetadata {
        self.predecessor
    }
    #[cfg(feature = "native")]
    pub(crate) fn into_suffix(self) -> Option<Suffix> {
        self.suffix
    }
    #[cfg(feature = "native")]
    pub(crate) fn install(self, current: &mut GroupLog) -> LogSummary {
        install_append(current, self.suffix, self.summary)
    }
}
impl PreparedOwnedLog {
    pub(crate) fn preserves_checkpoint_prefix(&self) -> bool {
        match self {
            Self::Append(_) => true,
            Self::Other(prepared) => prepared.preserves_checkpoint_prefix(),
        }
    }
    pub(crate) fn install(self, current: &mut GroupLog) -> LogSummary {
        match self {
            Self::Append(append) => install_append(current, append.suffix, append.summary),
            Self::Other(prepared) => prepared.install(current),
        }
    }
}
/// A plain plan's update had the predecessor's group and revision, the next
/// ballot and commit index, the planned suffix and no snapshot; admission
/// rejects any other update before planning. Comparing these fields therefore
/// gives the same answer as comparing with a retained copy of that update.
fn matches_plain(
    original: &LogUpdate,
    predecessor: &LogMetadata,
    next: &LogMetadata,
    planned: Option<&Suffix>,
) -> bool {
    let LogUpdate {
        group,
        expected_revision,
        hard_state,
        commit_index,
        suffix,
        snapshot,
        snapshot_membership,
    } = original;
    *group == predecessor.group
        && *expected_revision == predecessor.revision
        && *hard_state == next.hard_state
        && *commit_index == next.commit_index
        && suffix.as_ref() == planned
        && snapshot.is_none()
        && snapshot_membership.is_none()
}
#[cfg(test)]
fn plain_update(
    predecessor: &LogMetadata,
    next: &LogMetadata,
    suffix: Option<Suffix>,
) -> LogUpdate {
    LogUpdate {
        group: predecessor.group,
        expected_revision: predecessor.revision,
        hard_state: next.hard_state,
        commit_index: next.commit_index,
        suffix,
        snapshot: None,
        snapshot_membership: None,
    }
}
fn install_append(
    current: &mut GroupLog,
    suffix: Option<Suffix>,
    summary: LogSummary,
) -> LogSummary {
    if let Some(suffix) = suffix {
        current.entries.append(suffix.entries);
    }
    summary.metadata.install_header(current);
    summary
}
fn validate_base(
    view: &LogView<'_>,
    summary: &LogSummary,
    update: &LogUpdate,
    limits: LogLimits,
) -> Result<LogLimits, StorageError> {
    let limits = limits.validate()?;
    if view.metadata().group != update.group {
        return Err(StorageError::Rejected("group not bootstrapped"));
    }
    if !view.summarized_by(summary) {
        return Err(StorageError::Rejected("stale log summary"));
    }
    Ok(limits)
}
