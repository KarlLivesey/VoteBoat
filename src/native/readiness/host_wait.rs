// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The host loop's wait policy, shared by the service binaries and the
//! benchmark's cluster mode. Safe code only: the audited `poll(2)` stays in
//! the parent module.
#![forbid(unsafe_code)]

use super::{HostWake, Readiness};
use crate::runtime::MonoTime;
use std::sync::Arc;

/// Longest a host loop waits with nothing announced; bounds host-side timers
/// such as credential reloads and administrative deadlines.
pub const HOST_MAX_WAIT: std::time::Duration = std::time::Duration::from_millis(10);
/// Gather window before a round that already has queued work.
pub const HOST_GATHER: std::time::Duration = std::time::Duration::from_micros(200);
/// How one [`HostWait::wait`] ended. Diagnostic only: the host polls the same
/// way after every outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostWaitOutcome {
    /// Queued work needed no kernel wait; the host gathered briefly instead.
    Immediate,
    /// A descriptor or the wake reported readiness before the limit.
    Ready,
    /// The limit passed with nothing reported (or the wait was interrupted).
    Timeout,
}
/// Host-loop readiness state: the wake every worker signals, the reusable
/// descriptor buffer and the wait itself.
pub struct HostWait {
    wake: Arc<HostWake>,
    readiness: Readiness,
    fds: Vec<crate::worker::ReadinessFd>,
    gather: std::time::Duration,
    max_wait: std::time::Duration,
}
impl HostWait {
    /// A host loop that waits on `wake` (the wake its workers signal) with
    /// the default gather window and wait cap.
    pub fn new(wake: Arc<HostWake>) -> Self {
        Self::with(wake, HOST_GATHER, HOST_MAX_WAIT)
    }
    /// A host loop with its own gather window (the CPU/latency knob: longer
    /// gathers mean fewer, fuller rounds) and wait cap (the bound on
    /// host-side timers such as credential reloads).
    pub fn with(
        wake: Arc<HostWake>,
        gather: std::time::Duration,
        max_wait: std::time::Duration,
    ) -> Self {
        Self {
            wake,
            readiness: Readiness::default(),
            fds: Vec::new(),
            gather,
            max_wait,
        }
    }
    /// Wait for a socket named by `collect`, a worker wake or `deadline`
    /// instead of polling at a fixed cadence; polling remains the only source
    /// of progress, so a missed or spurious report costs latency only.
    /// Returns how the wait ended, for host-loop counters.
    pub fn wait(
        &mut self,
        collect: impl FnOnce(&mut Vec<crate::worker::ReadinessFd>),
        deadline: Option<MonoTime>,
        time: MonoTime,
    ) -> std::io::Result<HostWaitOutcome> {
        self.fds.clear();
        collect(&mut self.fds);
        self.fds.push(self.wake.fd());
        let limit = deadline
            .map(|d| std::time::Duration::from_millis(d.0.saturating_sub(time.0)))
            .map_or(self.max_wait, |d| d.min(self.max_wait));
        let outcome = if self.fds.iter().any(|f| f.immediate) {
            std::thread::park_timeout(self.gather);
            HostWaitOutcome::Immediate
        } else if self.readiness.wait(&self.fds, limit)? {
            HostWaitOutcome::Ready
        } else {
            HostWaitOutcome::Timeout
        };
        self.wake.drain();
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::{ReadinessFd, WorkerWake};

    #[test]
    fn wait_reports_how_it_ended() {
        let wake = Arc::new(HostWake::new().unwrap());
        let mut host = HostWait::new(wake.clone());
        let now = MonoTime(7);
        // Queued work: no kernel wait.
        let ended = host.wait(|fds| fds.push(ReadinessFd::immediate()), None, now);
        assert_eq!(ended.unwrap(), HostWaitOutcome::Immediate);
        // A pending wake is reported, then drained.
        wake.wake();
        assert_eq!(
            host.wait(|_| (), None, now).unwrap(),
            HostWaitOutcome::Ready
        );
        // A deadline already reached with nothing announced: a zero wait.
        assert_eq!(
            host.wait(|_| (), Some(now), now).unwrap(),
            HostWaitOutcome::Timeout
        );
    }
}
