// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Optional fixed-size volatile file-call timings. Never durability evidence.
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JournalCallTiming {
    pub calls: u64,
    pub errors: u64,
    pub elapsed_ns: u64,
    pub max_ns: u64,
}
/// Concurrent fields may be sampled between updates. Read after worker join for
/// stable totals. Durations overlap across workers and are not critical-path time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JournalTimingSnapshot {
    pub append: JournalCallTiming,
    pub log_sync: JournalCallTiming,
    pub manifest: JournalCallTiming,
}
#[derive(Default)]
struct Call {
    calls: AtomicU64,
    errors: AtomicU64,
    elapsed_ns: AtomicU64,
    max_ns: AtomicU64,
}
impl Call {
    fn add(field: &AtomicU64, n: u64) {
        let _ = field.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_add(n))
        });
    }
    fn record(&self, elapsed: Duration, success: bool) {
        let ns = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        Self::add(&self.calls, 1);
        Self::add(&self.errors, u64::from(!success));
        Self::add(&self.elapsed_ns, ns);
        self.max_ns.fetch_max(ns, Ordering::Relaxed);
    }
    fn snapshot(&self) -> JournalCallTiming {
        JournalCallTiming {
            calls: self.calls.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            elapsed_ns: self.elapsed_ns.load(Ordering::Relaxed),
            max_ns: self.max_ns.load(Ordering::Relaxed),
        }
    }
}
/// Host-owned reader for explicitly selected native file diagnostics. Cloning or
/// dropping a reader never closes a file or worker. No callbacks, queues or I/O.
#[derive(Clone, Default)]
pub struct JournalTimings(Arc<[Call; 3]>);
impl JournalTimings {
    pub fn snapshot(&self) -> JournalTimingSnapshot {
        JournalTimingSnapshot {
            append: self.0[0].snapshot(),
            log_sync: self.0[1].snapshot(),
            manifest: self.0[2].snapshot(),
        }
    }
    pub(super) fn record(&self, operation: usize, elapsed: Duration, success: bool) {
        self.0[operation].record(elapsed, success);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn totals_saturate_and_reader_clone_has_no_close_effect() {
        let timing = JournalTimings::default();
        let reader = timing.clone();
        timing.record(1, Duration::MAX, false);
        timing.record(1, Duration::from_nanos(2), true);
        drop(timing);
        assert_eq!(
            reader.snapshot().log_sync,
            JournalCallTiming {
                calls: 2,
                errors: 1,
                elapsed_ns: u64::MAX,
                max_ns: u64::MAX
            }
        );
        assert_eq!(reader.snapshot().append, JournalCallTiming::default());
    }
}
