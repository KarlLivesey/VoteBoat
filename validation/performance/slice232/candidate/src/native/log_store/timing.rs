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
/// Invoked steps of the existing atomic manifest publication, in order.
/// Directory synchronization includes opening the containing directory.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ManifestPublicationTiming {
    pub open: JournalCallTiming,
    pub write: JournalCallTiming,
    pub file_sync: JournalCallTiming,
    pub rename: JournalCallTiming,
    pub directory_sync: JournalCallTiming,
}
/// Concurrent fields may be sampled between updates. Read after worker join for
/// stable totals. Durations overlap across workers and are not critical-path time.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct JournalTimingSnapshot {
    pub append: JournalCallTiming,
    pub log_sync: JournalCallTiming,
    pub manifest: JournalCallTiming,
    pub publication: ManifestPublicationTiming,
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
pub struct JournalTimings(Arc<[Call; 8]>);
impl JournalTimings {
    pub fn snapshot(&self) -> JournalTimingSnapshot {
        JournalTimingSnapshot {
            append: self.0[0].snapshot(),
            log_sync: self.0[1].snapshot(),
            manifest: self.0[2].snapshot(),
            publication: ManifestPublicationTiming {
                open: self.0[3].snapshot(),
                write: self.0[4].snapshot(),
                file_sync: self.0[5].snapshot(),
                rename: self.0[6].snapshot(),
                directory_sync: self.0[7].snapshot(),
            },
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
    #[test]
    fn publication_step_totals_saturate_independently() {
        let timing = JournalTimings::default();
        for operation in 3..8 {
            timing.record(operation, Duration::MAX, false);
            timing.record(operation, Duration::from_nanos(1), true);
        }
        let p = timing.snapshot().publication;
        for step in [p.open, p.write, p.file_sync, p.rename, p.directory_sync] {
            assert_eq!(step.calls, 2);
            assert_eq!(step.errors, 1);
            assert_eq!(step.elapsed_ns, u64::MAX);
            assert_eq!(step.max_ns, u64::MAX);
        }
        assert_eq!(timing.snapshot().manifest, JournalCallTiming::default());
    }
}
