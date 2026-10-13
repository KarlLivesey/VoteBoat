// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Sealed owned incoming updates; scalar admission precedes copying/normalization.
use super::*;

pub(crate) struct OwnedAppend {
    predecessor: LogMetadata,
    update: LogUpdate,
    summary: LogSummary,
}
pub(crate) enum OwnedChange {
    Append(OwnedAppend),
    Other { next: NextLog, update: LogUpdate },
}
pub(crate) struct OwnedNextLog(OwnedChange);
pub(crate) enum PreparedOwnedLog {
    Append {
        update: LogUpdate,
        summary: LogSummary,
    },
    Other(PreparedLog),
}
impl OwnedNextLog {
    /// Core needs one independent normalized copy while its original crosses
    /// the public owned provider boundary. Inspect the raw input first.
    pub(crate) fn plan_copy(
        view: LogView<'_>,
        summary: &LogSummary,
        update: &LogUpdate,
        limits: LogLimits,
    ) -> Result<Self, StorageError> {
        let limits = validate_base(view, summary, update, limits)?;
        if append_eligible(view, summary, update) {
            let metadata = validate_append_metadata(view, update, limits)?;
            Ok(Self(OwnedChange::Append(OwnedAppend::new(
                view.metadata(),
                summary,
                metadata,
                update.clone(),
            ))))
        } else {
            Ok(Self(OwnedChange::Other {
                next: NextLog::plan(view, summary, update, limits)?,
                update: update.clone(),
            }))
        }
    }
    /// A provider already owns this exact input. Only admitted new append
    /// allocations are normalized; no retained canonical history is copied.
    pub(crate) fn plan_owned(
        view: LogView<'_>,
        summary: &LogSummary,
        mut update: LogUpdate,
        limits: LogLimits,
    ) -> Result<Self, StorageError> {
        let limits = validate_base(view, summary, &update, limits)?;
        if append_eligible(view, summary, &update) {
            let metadata = validate_append_metadata(view, &update, limits)?;
            normalize_append(&mut update);
            Ok(Self(OwnedChange::Append(OwnedAppend::new(
                view.metadata(),
                summary,
                metadata,
                update,
            ))))
        } else {
            let next = NextLog::plan(view, summary, &update, limits)?;
            Ok(Self(OwnedChange::Other { next, update }))
        }
    }
    pub(crate) fn update(&self) -> &LogUpdate {
        match &self.0 {
            OwnedChange::Append(append) => &append.update,
            OwnedChange::Other { update, .. } => update,
        }
    }
    pub(crate) fn matches_update(&self, original: &LogUpdate) -> bool {
        let LogUpdate {
            group,
            expected_revision,
            hard_state,
            commit_index,
            suffix,
            snapshot,
            snapshot_membership,
        } = original;
        let owned = self.update();
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
            OwnedChange::Other { next, .. } => next.metadata(),
        }
    }
    pub(crate) fn full(&self) -> Option<&GroupLog> {
        match &self.0 {
            OwnedChange::Append(_) => None,
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
            OwnedChange::Other { next, update } => next.term_at(current, update, index),
        }
    }
    pub(crate) fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if !self.is_append() {
            return None;
        }
        let suffix = self.update().suffix.as_ref()?;
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
                Ok(PreparedOwnedLog::Append {
                    update: append.update,
                    summary: append.summary,
                })
            }
            OwnedChange::Other { next, update } => next
                .prepare_install(current, &update)
                .map(PreparedOwnedLog::Other),
        }
    }
    pub(crate) fn install(self, current: &mut GroupLog) -> Result<LogSummary, StorageError> {
        Ok(self.prepare_install(current)?.install(current))
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
            next: NextLog::Full {
                predecessor: view.metadata(),
                next: Box::new(next),
                update: update.clone(),
            },
            update: update.clone(),
        }))
    }
}
impl OwnedAppend {
    fn new(
        predecessor: LogMetadata,
        summary: &LogSummary,
        metadata: LogMetadata,
        update: LogUpdate,
    ) -> Self {
        Self {
            predecessor,
            summary: append_summary(summary, metadata, update.suffix.as_ref()),
            update,
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
        self.update.suffix
    }
    #[cfg(feature = "native")]
    pub(crate) fn install(self, current: &mut GroupLog) -> LogSummary {
        install_append(current, self.update, self.summary)
    }
}
impl PreparedOwnedLog {
    pub(crate) fn install(self, current: &mut GroupLog) -> LogSummary {
        match self {
            Self::Append { update, summary } => install_append(current, update, summary),
            Self::Other(prepared) => prepared.install(current),
        }
    }
}
fn install_append(current: &mut GroupLog, update: LogUpdate, summary: LogSummary) -> LogSummary {
    if let Some(suffix) = update.suffix {
        current.entries.extend(suffix.entries);
    }
    summary.metadata.install_header(current);
    summary
}
fn validate_base(
    view: LogView<'_>,
    summary: &LogSummary,
    update: &LogUpdate,
    limits: LogLimits,
) -> Result<LogLimits, StorageError> {
    let limits = limits.validate()?;
    if view.metadata().group != update.group {
        return Err(StorageError::Rejected("group not bootstrapped"));
    }
    if !summary.matches_metadata(&view.metadata()) {
        return Err(StorageError::Rejected("stale log summary"));
    }
    Ok(limits)
}
fn normalize_append(update: &mut LogUpdate) {
    if let Some(suffix) = &mut update.suffix {
        if suffix.entries.capacity() != suffix.entries.len() {
            suffix.entries = std::mem::take(&mut suffix.entries)
                .into_boxed_slice()
                .into_vec();
        }
        for entry in &mut suffix.entries {
            if let EntryPayload::Command { bytes, .. } = &mut entry.payload {
                if bytes.capacity() != bytes.len() {
                    *bytes = std::mem::take(bytes).into_boxed_slice().into_vec();
                }
            }
        }
    }
}
