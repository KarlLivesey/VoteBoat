// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The core's canonical log and the summary of its current version, changed
//! only together.
use super::*;
use std::num::NonZeroU32;
use std::ops::Deref;
use std::sync::atomic::{AtomicU32, Ordering};

/// Which summarized log, at which version, a view was taken from. The core
/// stages each plan with the stamp of the view it planned from, and the
/// install skips the predecessor comparison only when that stamp is the
/// installing log's current one: the plan's predecessor is then that log's
/// unchanged metadata. Never a durability or protocol value; 8 bytes, so
/// `Pending` keeps its size.
///
/// Only equality is used, and no wrap can make a wrong plan match. A staged
/// plan is installed or dropped before its log's next install, so its
/// version can only be the current one or one earlier change behind (test
/// `edit`s aside); the 32-bit version wraps after 2^32 installs, never
/// within that window. Log identities repeat after 2^32 constructions in one
/// process; a repeat matters only for a plan moved between cores, which the
/// core cannot do (its plan is private and consumed by its own install), so
/// it is a second guard, not the first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LogStamp {
    log: NonZeroU32,
    version: u32,
}
static NEXT_LOG: AtomicU32 = AtomicU32::new(1);
// One relaxed increment per constructed log, never per write.
fn next_log() -> NonZeroU32 {
    let id = NEXT_LOG.fetch_add(1, Ordering::Relaxed);
    NonZeroU32::new(id).unwrap_or(NonZeroU32::MIN)
}
/// A canonical log owned by one core, with the summary of its exact current
/// version. The fields are private to this module and there is no `DerefMut`:
/// every change goes through [`Self::install`], which replaces the summary in
/// the same call, so the summary never describes an older log.
pub(crate) struct SummarizedLog {
    log: GroupLog,
    summary: LogSummary,
    // This log's identity and its version, which changes (wrapping) with
    // every change of `log`: only "nothing changed since this plan".
    stamp: LogStamp,
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
            stamp: LogStamp {
                log: next_log(),
                version: 0,
            },
        }
    }
    #[inline]
    pub(crate) fn summary(&self) -> &LogSummary {
        self.debug_check();
        &self.summary
    }
    #[cfg(test)]
    pub(crate) fn version(&self) -> u32 {
        self.stamp.version
    }
    /// A view of the log whose metadata is the summary's (no rebuild), which
    /// planning accepts with this summary without comparing fields, and which
    /// carries this log's current stamp ([`LogView::stamp`]) for staging.
    #[inline]
    pub(crate) fn view(&self) -> LogView<'_> {
        self.debug_check();
        LogView::summarized(&self.log, &self.summary, self.stamp)
    }
    /// Install `next`, planned from a view whose stamp was `planned`. When
    /// that is this log's current stamp the log is the one the plan was made
    /// from, so an append or commit advance skips its predecessor comparison;
    /// otherwise (another log, a changed log, or a view not taken from a
    /// summarized log) the full checked install runs. On error nothing
    /// changes.
    pub(crate) fn install(
        &mut self,
        next: OwnedNextLog,
        planned: Option<LogStamp>,
    ) -> Result<(), StorageError> {
        if planned == Some(self.stamp) {
            next.install_unchanged(&mut self.log, &mut self.summary)?;
        } else {
            #[cfg(test)]
            checked_installs::note();
            self.summary = next.install(&mut self.log)?;
        }
        self.stamp.version = self.stamp.version.wrapping_add(1);
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
        self.stamp.version = self.stamp.version.wrapping_add(1);
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
/// Test-only count of the installs on this thread that took the full checked
/// path because their stamp was not the log's current one.
#[cfg(test)]
pub(crate) mod checked_installs {
    use std::cell::Cell;
    thread_local! {
        static CHECKED: Cell<u64> = const { Cell::new(0) };
    }
    pub(super) fn note() {
        CHECKED.with(|checked| checked.set(checked.get() + 1));
    }
    /// Checked installs on this thread so far.
    pub(crate) fn count() -> u64 {
        CHECKED.with(Cell::get)
    }
}
