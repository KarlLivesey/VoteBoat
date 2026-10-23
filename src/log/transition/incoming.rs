// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Sealed owned incoming updates; scalar admission precedes copying/normalization.
use super::*;

/// An admitted plain append. Every other field of its update is the
/// predecessor's or its checked outcomes; no full next summary is staged.
pub(crate) struct OwnedAppend {
    predecessor: LogMetadata,
    delta: AppendDelta,
    facts: SummaryFacts,
    suffix: Option<Suffix>,
}
/// The core's own commit-only round: an empty append planned without an
/// update. Providers never receive one.
pub(crate) struct CommitAdvance {
    predecessor: LogMetadata,
    revision: LogRevision,
    commit_index: u64,
    facts: SummaryFacts,
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
/// Borrowed scalar access to a checked plan. Hot reads never rebuild metadata.
pub(crate) enum PlannedMetadata<'a> {
    Append(&'a OwnedAppend),
    Commit(&'a CommitAdvance),
    Other(&'a NextLog),
}
impl PlannedMetadata<'_> {
    pub(crate) fn group(&self) -> GroupIdentity {
        match self {
            Self::Append(append) => append.predecessor.group,
            Self::Commit(commit) => commit.predecessor.group,
            Self::Other(next) => next.metadata().group,
        }
    }
    pub(crate) fn revision(&self) -> LogRevision {
        match self {
            Self::Append(append) => append.delta.revision,
            Self::Commit(commit) => commit.revision,
            Self::Other(next) => next.metadata().revision,
        }
    }
    pub(crate) fn generation(&self) -> LogGeneration {
        match self {
            Self::Append(append) => append.predecessor.generation,
            Self::Commit(commit) => commit.predecessor.generation,
            Self::Other(next) => next.metadata().generation,
        }
    }
    pub(crate) fn hard_state(&self) -> HardState {
        match self {
            Self::Append(append) => append.delta.hard_state,
            Self::Commit(commit) => commit.predecessor.hard_state,
            Self::Other(next) => next.metadata().hard_state,
        }
    }
    pub(crate) fn ballot_origin(&self) -> Option<BallotOrigin> {
        match self {
            Self::Append(append) => append.delta.ballot_origin,
            Self::Commit(commit) => commit.predecessor.ballot_origin,
            Self::Other(next) => next.metadata().ballot_origin,
        }
    }
    pub(crate) fn commit_index(&self) -> u64 {
        match self {
            Self::Append(append) => append.delta.commit_index,
            Self::Commit(commit) => commit.commit_index,
            Self::Other(next) => next.metadata().commit_index,
        }
    }
    pub(crate) fn last_index(&self) -> u64 {
        match self {
            Self::Append(append) => append.delta.last_index,
            Self::Commit(commit) => commit.predecessor.last_index,
            Self::Other(next) => next.metadata().last_index,
        }
    }
}
impl CommitAdvance {
    #[cfg(any(test, feature = "native"))]
    fn metadata(&self) -> LogMetadata {
        LogMetadata {
            revision: self.revision,
            commit_index: self.commit_index,
            ..self.predecessor
        }
    }
    fn into_append(self) -> OwnedAppend {
        OwnedAppend {
            delta: AppendDelta {
                revision: self.revision,
                hard_state: self.predecessor.hard_state,
                ballot_origin: self.predecessor.ballot_origin,
                commit_index: self.commit_index,
                last_index: self.predecessor.last_index,
                last_term: self.predecessor.last_term,
                entries_len: self.predecessor.entries_len,
            },
            predecessor: self.predecessor,
            facts: self.facts,
            suffix: None,
        }
    }
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
            let delta = validate_append_delta(view, update, limits)?;
            Ok(Self(OwnedChange::Append(OwnedAppend::new(
                view,
                summary,
                delta,
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
            let delta = validate_append_delta(view, &update, limits)?;
            Ok(Self(OwnedChange::Append(OwnedAppend::new(
                view,
                summary,
                delta,
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
        Ok(Self(OwnedChange::Commit(CommitAdvance {
            predecessor: *current,
            revision,
            commit_index,
            facts: SummaryFacts::appended(view, summary, None),
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
                &append.metadata(),
                append.suffix.clone(),
            ),
            OwnedChange::Commit(commit) => {
                plain_update(&commit.predecessor, &commit.metadata(), None)
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
                    append.delta.hard_state,
                    append.delta.commit_index,
                    append.suffix.as_ref(),
                );
            }
            OwnedChange::Commit(commit) => {
                return matches_plain(
                    original,
                    &commit.predecessor,
                    commit.predecessor.hard_state,
                    commit.commit_index,
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
    /// Materialize only for consumers that require a full owned boundary value.
    #[cfg(any(test, feature = "native"))]
    pub(crate) fn metadata(&self) -> LogMetadata {
        match &self.0 {
            OwnedChange::Append(append) => append.metadata(),
            OwnedChange::Commit(commit) => commit.metadata(),
            OwnedChange::Other { next, .. } => next.metadata(),
        }
    }
    pub(crate) fn planned_metadata(&self) -> PlannedMetadata<'_> {
        match &self.0 {
            OwnedChange::Append(append) => PlannedMetadata::Append(append),
            OwnedChange::Commit(commit) => PlannedMetadata::Commit(commit),
            OwnedChange::Other { next, .. } => PlannedMetadata::Other(next),
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
            OwnedChange::Commit(commit) => {
                let append = commit.into_append();
                append.check_install(LogMetadata::from_log(current))?;
                Ok(PreparedOwnedLog::Append(append))
            }
            OwnedChange::Other { next, update } => (*next)
                .prepare_install(current, &update)
                .map(PreparedOwnedLog::Other),
        }
    }
    #[cfg(test)]
    pub(crate) fn install(self, current: &mut GroupLog) -> Result<LogSummary, StorageError> {
        Ok(self.prepare_install(current)?.install(current))
    }
    /// Install this plan into `current` where it lies and write the
    /// installed log's summary to `summary`. `unchanged` means the plan was
    /// made from a [`SummarizedLog`] view of `current` with no change since
    /// (its stamp is unchanged): the predecessor is then `current`'s
    /// metadata, so an append or commit advance installs without comparing
    /// it. Every other plan, and every plan when `unchanged` is false, is
    /// checked as [`Self::install`] checks it.
    ///
    /// Every check precedes the first change: on error `current`, `summary`
    /// and this plan are as they were. On success the plan is spent (its
    /// suffix belongs to `current`; a full plan holds the replaced log) and
    /// must be dropped without further use.
    pub(super) fn install_in_place(
        &mut self,
        current: &mut GroupLog,
        summary: &mut LogSummary,
        unchanged: bool,
    ) -> Result<(), StorageError> {
        match &mut self.0 {
            OwnedChange::Append(append) => {
                if unchanged {
                    debug_assert!(append.check_install(LogMetadata::from_log(current)).is_ok());
                } else {
                    append.check_install(LogMetadata::from_log(current))?;
                }
                if let Some(suffix) = append.suffix.take() {
                    current.entries.append(suffix.entries);
                }
                append.delta.install(current);
                append.delta.write_metadata(&mut summary.metadata);
                append.facts.write(summary);
            }
            OwnedChange::Commit(commit) => {
                if unchanged {
                    debug_assert_eq!(commit.predecessor, LogMetadata::from_log(current));
                } else {
                    check_predecessor(&commit.predecessor, LogMetadata::from_log(current))?;
                }
                current.revision = commit.revision;
                current.commit_index = commit.commit_index;
                summary.metadata.revision = commit.revision;
                summary.metadata.commit_index = commit.commit_index;
                commit.facts.write(summary);
            }
            OwnedChange::Other { next, update } => {
                *summary = next.install_in_place(current, update)?;
            }
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
        delta: AppendDelta,
        suffix: Option<Suffix>,
    ) -> Self {
        Self {
            predecessor: *view.metadata(),
            delta,
            facts: SummaryFacts::appended(view, summary, suffix.as_ref()),
            suffix,
        }
    }
    pub(crate) fn metadata(&self) -> LogMetadata {
        self.delta.materialize(&self.predecessor)
    }
    pub(crate) fn summary(&self) -> LogSummary {
        self.facts.materialize(self.metadata())
    }
    pub(crate) fn check_install(&self, current: LogMetadata) -> Result<(), StorageError> {
        // The original is immutable and bound to this plan before sealing.
        check_predecessor(&self.predecessor, current)
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
        let summary = self.summary();
        install_append(current, self.suffix, summary)
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
            Self::Append(append) => {
                let summary = append.summary();
                install_append(current, append.suffix, summary)
            }
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
    next_hard_state: HardState,
    next_commit_index: u64,
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
        && *hard_state == next_hard_state
        && *commit_index == next_commit_index
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
fn check_predecessor(predecessor: &LogMetadata, current: LogMetadata) -> Result<(), StorageError> {
    if current != *predecessor {
        return Err(StorageError::Rejected(
            "transition plan predecessor or update mismatch",
        ));
    }
    Ok(())
}
fn install_append(
    current: &mut GroupLog,
    suffix: Option<Suffix>,
    summary: LogSummary,
) -> LogSummary {
    install_append_ref(current, suffix, &summary);
    summary
}
/// Append `suffix` and install `next`'s header; the caller stores `next`.
fn install_append_ref(current: &mut GroupLog, suffix: Option<Suffix>, next: &LogSummary) {
    if let Some(suffix) = suffix {
        current.entries.append(suffix.entries);
    }
    next.metadata.install_header(current);
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
