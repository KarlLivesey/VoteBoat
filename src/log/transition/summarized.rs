// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The core's canonical log and the summary of its current version, changed
//! only together.
use super::*;
use std::ops::Deref;

/// A canonical log owned by one core, with the summary of its exact current
/// version. The fields are private to this module and there is no `DerefMut`:
/// every change goes through [`Self::install`], which replaces the summary in
/// the same call, so the summary never describes an older log.
pub(crate) struct SummarizedLog {
    log: GroupLog,
    summary: LogSummary,
    // Changes (wrapping) with every change of `log`; never a durability or
    // protocol value, only "nothing changed since this plan".
    version: u64,
}
impl Deref for SummarizedLog {
    type Target = GroupLog;
    fn deref(&self) -> &GroupLog {
        &self.log
    }
}
impl SummarizedLog {
    /// Caller has validated this canonical state, as for
    /// [`LogSummary::from_validated`].
    pub(crate) fn new(log: GroupLog) -> Self {
        Self {
            summary: LogSummary::from_validated(&log),
            log,
            version: 0,
        }
    }
    #[inline]
    pub(crate) fn summary(&self) -> &LogSummary {
        self.debug_check();
        &self.summary
    }
    /// Pass to [`Self::install`] with a plan made from [`Self::view`].
    #[inline]
    pub(crate) fn version(&self) -> u64 {
        self.version
    }
    /// A view of the log whose metadata is the summary's (no rebuild) and
    /// which planning accepts with this summary without comparing fields.
    #[inline]
    pub(crate) fn view(&self) -> LogView<'_> {
        self.debug_check();
        LogView::summarized(&self.log, &self.summary)
    }
    /// Install `next`, planned from [`Self::view`] when this log had version
    /// `planned`. While the version is unchanged the log is the one the plan
    /// was made from, so its predecessor comparison is skipped; otherwise the
    /// full checked install runs. On error nothing changes.
    pub(crate) fn install(&mut self, next: OwnedNextLog, planned: u64) -> Result<(), StorageError> {
        self.summary = if planned == self.version {
            next.install_unchanged(&mut self.log)?
        } else {
            next.install(&mut self.log)?
        };
        self.version = self.version.wrapping_add(1);
        self.debug_check();
        Ok(())
    }
    /// Test-only direct edit: the summary is rebuilt and the version moves,
    /// as for any other change, so a plan staged before the edit takes the
    /// full checked install.
    #[cfg(test)]
    pub(crate) fn edit<R>(&mut self, edit: impl FnOnce(&mut GroupLog) -> R) -> R {
        let result = edit(&mut self.log);
        self.summary = LogSummary::from_validated(&self.log);
        self.version = self.version.wrapping_add(1);
        result
    }
    #[inline]
    fn debug_check(&self) {
        debug_assert!(
            self.summary.matches(&self.log),
            "cached log summary describes another log version"
        );
    }
}
