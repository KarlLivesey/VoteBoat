// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The core's canonical log and the summary of its current version, changed
//! only together.
use super::*;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};

/// Which summarized log, at which version, a view was taken from. The core
/// stages each plan with the stamp of the view it planned from, and the
/// install skips the predecessor comparison only when that stamp is the
/// installing log's current one: the plan's predecessor is then that log's
/// unchanged metadata. Never a durability or protocol value.
///
/// A stamp is never repeated in one process, so the guard holds for any
/// plan, including one moved between cores. Each identity is issued at most
/// once (2^64 − 2 of them: about 584 years at 10^9 constructions per second)
/// and the version moves on every install without wrapping (2^64 − 1
/// installs: about 584 years at 10^9 per second on one log; past that the
/// log takes a fresh identity). A log constructed after identities run out
/// has no stamp and installs every plan through the checked path
/// ([`unidentified_logs`] counts them). Stamps are not persisted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LogStamp {
    log: NonZeroU64,
    version: u64,
}
#[cfg(test)]
impl LogStamp {
    pub(crate) fn log(self) -> u64 {
        self.log.get()
    }
    pub(crate) fn version(self) -> u64 {
        self.version
    }
}

/// Issues log identities, each at most once. `next` only grows and stops at
/// `u64::MAX`, which is never issued: from then on every request is refused.
struct LogIdentities {
    next: AtomicU64,
    refused: AtomicU64,
}
impl LogIdentities {
    const fn starting_at(next: u64) -> Self {
        Self {
            next: AtomicU64::new(next),
            refused: AtomicU64::new(0),
        }
    }
    // One relaxed update per constructed log, never per write. Uniqueness
    // needs only the atomic's single modification order, not ordering with
    // other memory.
    fn issue(&self) -> Option<NonZeroU64> {
        match self
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
        {
            Ok(id) => NonZeroU64::new(id),
            Err(_) => {
                self.note_refused();
                None
            }
        }
    }
    #[cold]
    fn note_refused(&self) {
        let _ = self
            .refused
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_add(1))
            });
    }
    fn refused(&self) -> u64 {
        self.refused.load(Ordering::Relaxed)
    }
}
static LOG_IDENTITIES: LogIdentities = LogIdentities::starting_at(1);

fn issue_identity() -> Option<NonZeroU64> {
    #[cfg(test)]
    if let Some(id) = test_identities::issue() {
        return id;
    }
    LOG_IDENTITIES.issue()
}
fn fresh_stamp() -> Option<LogStamp> {
    issue_identity().map(|log| LogStamp { log, version: 0 })
}
/// Logs in this process that got no identity because all identities were
/// issued (saturating). Each installs every plan through the checked path.
pub(crate) fn unidentified_logs() -> u64 {
    LOG_IDENTITIES.refused()
}

/// A canonical log owned by one core, with the summary of its exact current
/// version. The fields are private to this module and there is no `DerefMut`:
/// every change goes through [`Self::install`], which replaces the summary in
/// the same call, so the summary never describes an older log.
pub(crate) struct SummarizedLog {
    log: GroupLog,
    summary: LogSummary,
    // This log's identity and version, which moves with every change of
    // `log`; `None` only if no identity was left (checked installs only).
    stamp: Option<LogStamp>,
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
            stamp: fresh_stamp(),
        }
    }
    #[inline]
    pub(crate) fn summary(&self) -> &LogSummary {
        self.debug_check();
        &self.summary
    }
    #[cfg(test)]
    pub(crate) fn stamp(&self) -> Option<LogStamp> {
        self.stamp
    }
    /// Test-only: place this log's version, to reach the version bound.
    #[cfg(test)]
    pub(crate) fn set_version(&mut self, version: u64) {
        if let Some(stamp) = &mut self.stamp {
            stamp.version = version;
        }
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
    /// otherwise (another log, a changed log, a log or view without a stamp)
    /// the full checked install runs. On error nothing changes.
    pub(crate) fn install(
        &mut self,
        next: OwnedNextLog,
        planned: Option<LogStamp>,
    ) -> Result<(), StorageError> {
        if planned.is_some_and(|planned| self.stamp == Some(planned)) {
            next.install_unchanged(&mut self.log, &mut self.summary)?;
        } else {
            #[cfg(test)]
            checked_installs::note();
            self.summary = next.install(&mut self.log)?;
        }
        self.advance();
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
        self.advance();
        result
    }
    /// Move the version after a change of `log`. It never wraps: past
    /// `u64::MAX` the log takes a never-issued identity at version 0, or no
    /// stamp if none is left.
    #[inline]
    fn advance(&mut self) {
        if let Some(stamp) = &mut self.stamp {
            match stamp.version.checked_add(1) {
                Some(version) => stamp.version = version,
                None => self.renew(),
            }
        }
    }
    #[cold]
    fn renew(&mut self) {
        self.stamp = fresh_stamp();
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
/// Test-only allocator override: constructions on this thread draw from a
/// private allocator, so a test can start at the identity bound without
/// constructing 2^64 logs or touching other tests' threads.
#[cfg(test)]
pub(crate) mod test_identities {
    use super::LogIdentities;
    use std::cell::RefCell;
    use std::num::NonZeroU64;
    thread_local! {
        static OVERRIDE: RefCell<Option<LogIdentities>> = const { RefCell::new(None) };
    }
    /// `None` when no override is active; otherwise the override's answer.
    pub(super) fn issue() -> Option<Option<NonZeroU64>> {
        OVERRIDE.with_borrow(|ids| ids.as_ref().map(LogIdentities::issue))
    }
    /// Run `f` with this thread's log identities issued from `next` on (the
    /// production `issue`), and return its result and the number of
    /// constructions refused.
    pub(crate) fn with_log_identities<R>(next: u64, f: impl FnOnce() -> R) -> (R, u64) {
        assert!(next >= 1, "identity 0 is never issued");
        let previous = OVERRIDE.replace(Some(LogIdentities::starting_at(next)));
        let result = f();
        let ids = OVERRIDE.replace(previous).expect("override still set");
        (result, ids.refused())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_are_issued_once_in_order_from_one() {
        let ids = LogIdentities::starting_at(1);
        let issued: Vec<_> = (0..3).map(|_| ids.issue().map(NonZeroU64::get)).collect();
        assert_eq!(issued, [Some(1), Some(2), Some(3)]);
        assert_eq!(ids.refused(), 0);
    }

    /// At the bound the last identity is issued once, then every request is
    /// refused and counted; the allocator never wraps back to a used value.
    #[test]
    fn identities_stop_at_the_bound_and_never_wrap() {
        let ids = LogIdentities::starting_at(u64::MAX - 2);
        let issued: Vec<_> = (0..5).map(|_| ids.issue().map(NonZeroU64::get)).collect();
        assert_eq!(
            issued,
            [Some(u64::MAX - 2), Some(u64::MAX - 1), None, None, None]
        );
        assert_eq!(ids.refused(), 3);
        assert_eq!(ids.next.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn the_refused_count_saturates() {
        let ids = LogIdentities::starting_at(u64::MAX);
        ids.refused.store(u64::MAX - 1, Ordering::Relaxed);
        assert_eq!((ids.issue(), ids.issue()), (None, None));
        assert_eq!(ids.refused(), u64::MAX);
    }
}
