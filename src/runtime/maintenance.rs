// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Store maintenance on by default: physical WAL reclaim scheduling driven by
//! the host clock and the store's published occupancy.
use super::{MonoTime, NodeError, ReplicaError};
use crate::{
    contracts::StorageError,
    log::{LogReclaimed, WalUsage},
    worker::*,
};

/// How a node keeps retained storage bounded for unbounded uptime.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetentionMode {
    /// Automatic checkpoints for every group and, when the persistence
    /// provider supports it, physical WAL reclaim, with policies derived from
    /// the store limits. Construction is refused when a group cannot
    /// checkpoint. Explicit policies may replace the defaults; they cannot be
    /// removed.
    #[default]
    Maintained,
    /// Explicit opt-out: no automatic maintenance. Each group stops admitting
    /// writes once its retained entries or WAL bytes reach the store limits;
    /// a leader elected at the entry limit stays leader and refuses typed.
    LifetimeBounded,
}

/// Typed maintenance refusals, raised at construction or configuration so
/// they cannot appear while the node is serving.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaintenanceError {
    /// `Maintained` needs a checkpoint-capable snapshot worker for every group.
    CheckpointsUnavailable,
    /// Removing a schedule requires constructing with `LifetimeBounded`.
    DisableRequiresLifetimeBounded,
    /// The threshold leaves less than half the retained-entry limit for writes
    /// that arrive while a checkpoint runs, so the cap could be met first.
    CheckpointThresholdUnreachable {
        min_entries: u64,
        max_entries_per_group: usize,
    },
}

/// Physical replacement only: logical retention and checkpoint authorization
/// remain with the existing core/snapshot contracts. No latency guarantee.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalMaintenancePolicy {
    /// Delay from configuration or the last successful completion.
    pub interval_ms: u64,
    /// Delay after any refusal or failed completion, and the minimum spacing
    /// of occupancy-triggered attempts.
    pub retry_ms: u64,
    /// Replacement image budget. Must equal the provider's `reclaim_limit`, so
    /// a live image can never outgrow it.
    pub max_bytes: usize,
    /// An interval deadline rewrites only when at least this many bytes are
    /// reclaimable (published occupancy minus the live retained log); with
    /// less, the deadline moves on one interval, since rewriting the live image
    /// would buy nothing. Zero rewrites at every deadline. Providers that
    /// publish no occupancy are rewritten at every deadline.
    pub min_reclaim_bytes: usize,
}
impl WalMaintenancePolicy {
    /// The default schedule for a provider whose reclaim ceiling is `ceiling`.
    pub fn for_ceiling(ceiling: usize) -> Self {
        Self {
            interval_ms: 1000,
            retry_ms: 100,
            max_bytes: ceiling,
            // About 4 MiB at the default 64 MiB WAL.
            min_reclaim_bytes: ceiling / 16,
        }
    }
}

/// Latest-only local diagnostics. A new completion replaces the previous one;
/// manual reclaim receipts remain separately owned by the caller.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalMaintenanceStatus {
    pub policy: Option<WalMaintenancePolicy>,
    /// Next interval deadline. None while a request is in flight, with no
    /// policy, or after `u64` millisecond clock exhaustion (584 million years).
    /// Every refusal and failed completion reschedules after `retry_ms`.
    pub next_deadline: Option<MonoTime>,
    pub pending: Option<ReclaimTicket>,
    pub last_completion: Option<ReclaimEvent>,
    pub last_admission_error: Option<WorkerError>,
    /// Last successful rewrite and when it completed.
    pub last_reclaimed: Option<(MonoTime, LogReclaimed)>,
    /// Refusals and failed completions since the last success (saturating).
    pub consecutive_failures: u32,
    /// Last submission attempt; spaces occupancy-triggered attempts.
    pub last_attempt: Option<MonoTime>,
    /// Interval deadlines passed without a rewrite because too little was
    /// reclaimable (saturating; diagnostic only).
    pub skipped: u64,
}
/// WAL occupancy at or above half the usable capacity: checkpoints stop
/// waiting for their entry threshold and reclaim may run early.
pub(super) fn pressure(usage: Option<WalUsage>) -> bool {
    usage.is_some_and(|u| u.capacity > 0 && u.bytes >= u.capacity / 2)
}
impl WalMaintenanceStatus {
    pub(super) fn configure(
        &mut self,
        policy: Option<WalMaintenancePolicy>,
        now: MonoTime,
        ceiling: Option<usize>,
    ) -> Result<(), NodeError> {
        if self.pending.is_some() {
            return Err(NodeError::Replica(ReplicaError::Worker(
                WorkerError::Overloaded,
            )));
        }
        let next = if let Some(p) = policy {
            if p.interval_ms == 0 || p.retry_ms == 0 || p.max_bytes == 0 {
                return Err(NodeError::InvalidLimits);
            }
            let ceiling = ceiling.ok_or(NodeError::Replica(ReplicaError::Worker(
                WorkerError::Unsupported,
            )))?;
            // A smaller image budget is refused for ever once the live state
            // outgrows it, which would switch reclaim off without a signal.
            if p.max_bytes != ceiling || p.max_bytes > u32::MAX as usize {
                return Err(NodeError::InvalidLimits);
            }
            Some(MonoTime(
                now.0
                    .checked_add(p.interval_ms)
                    .ok_or(NodeError::InvalidLimits)?,
            ))
        } else {
            None
        };
        self.policy = policy;
        self.next_deadline = next;
        self.last_admission_error = None;
        self.consecutive_failures = 0;
        Ok(())
    }
    /// True when the interval deadline has passed and no rewrite is in flight.
    pub(super) fn interval_due(&self, now: MonoTime) -> bool {
        self.policy.is_some()
            && self.pending.is_none()
            && self.next_deadline.is_some_and(|d| now >= d)
    }
    /// Due at the interval deadline when at least `min_reclaim_bytes` are
    /// reclaimable (occupancy minus the `live` retained-log estimate); a
    /// deadline with less just moves on one interval. Due early when the
    /// occupancy passes half the capacity and at least an eighth of it is
    /// reclaimable, at most once per `retry_ms`. A provider that publishes no
    /// occupancy is rewritten at every deadline.
    pub(super) fn due(
        &mut self,
        now: MonoTime,
        usage: Option<WalUsage>,
        live: usize,
    ) -> Option<WalMaintenancePolicy> {
        let policy = self.policy?;
        if self.pending.is_some() {
            return None;
        }
        if self.interval_due(now) {
            let reclaimable = usage
                .filter(|u| u.capacity > 0)
                .map(|u| u.bytes.saturating_sub(live) >= policy.min_reclaim_bytes);
            if reclaimable != Some(false) {
                return Some(policy);
            }
            self.next_deadline = self.after(now, false);
            self.skipped = self.skipped.saturating_add(1);
            return None;
        }
        let usage = usage.filter(|u| pressure(Some(*u)))?;
        let spaced = self
            .last_attempt
            .is_none_or(|t| now.0 >= t.0.saturating_add(policy.retry_ms));
        (spaced && usage.bytes.saturating_sub(live) >= usage.capacity / 8).then_some(policy)
    }
    pub(super) fn submitted(&mut self, ticket: ReclaimTicket, now: MonoTime) {
        self.pending = Some(ticket);
        self.next_deadline = None;
        self.last_admission_error = None;
        self.last_attempt = Some(now);
    }
    /// Every admission refusal is retried. Configuration-class kinds are
    /// refused by `configure`; a fenced or closed provider fails the replica,
    /// which the node turns into `RecoveryRequired`.
    pub(super) fn rejected(&mut self, error: WorkerError, now: MonoTime) {
        self.next_deadline = self.after(now, true);
        self.last_admission_error = Some(error);
        self.last_attempt = Some(now);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
    }
    /// Success waits an interval; any other completion retries, except a
    /// fatal storage error, which fails the replica through the driver.
    pub(super) fn completed(&mut self, event: ReclaimEvent, now: MonoTime) {
        self.pending = None;
        match &event.result {
            Ok(reclaimed) => {
                self.next_deadline = self.after(now, false);
                self.last_reclaimed = Some((now, *reclaimed));
                self.consecutive_failures = 0;
            }
            Err(error) => {
                // A fatal storage error has already failed the replica, so the
                // node reports `RecoveryRequired` instead of a retry.
                self.next_deadline = if matches!(
                    error,
                    StorageError::Uncertain(_) | StorageError::Corrupt(_) | StorageError::Fenced
                ) {
                    None
                } else {
                    self.after(now, true)
                };
                self.consecutive_failures = self.consecutive_failures.saturating_add(1);
            }
        }
        self.last_completion = Some(event);
    }
    fn after(&self, now: MonoTime, retry: bool) -> Option<MonoTime> {
        let p = self.policy?;
        now.0
            .checked_add(if retry { p.retry_ms } else { p.interval_ms })
            .map(MonoTime)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deadline_exhaustion_stops_instead_of_wrapping_or_spinning() {
        let mut state = WalMaintenanceStatus::default();
        let policy = WalMaintenancePolicy {
            interval_ms: 10,
            retry_ms: 2,
            max_bytes: 100,
            min_reclaim_bytes: 0,
        };
        state
            .configure(Some(policy), MonoTime(u64::MAX - 10), Some(100))
            .unwrap();
        assert_eq!(state.next_deadline, Some(MonoTime(u64::MAX)));
        state.rejected(WorkerError::Overloaded, MonoTime(u64::MAX));
        assert_eq!(state.next_deadline, None);
        assert_eq!(state.due(MonoTime(u64::MAX), None, 0), None);
        assert!(state
            .configure(Some(policy), MonoTime(u64::MAX), Some(100))
            .is_err());
    }
    #[test]
    fn occupancy_triggers_only_when_half_full_reclaimable_and_spaced() {
        let mut state = WalMaintenanceStatus::default();
        let policy = WalMaintenancePolicy::for_ceiling(100);
        state
            .configure(Some(policy), MonoTime(0), Some(100))
            .unwrap();
        let usage = |bytes| {
            Some(WalUsage {
                bytes,
                capacity: 800,
            })
        };
        assert_eq!(state.due(MonoTime(10), usage(399), 0), None);
        assert_eq!(state.due(MonoTime(10), usage(400), 0), Some(policy));
        // Mostly live: a rewrite would free less than an eighth.
        assert_eq!(state.due(MonoTime(10), usage(400), 301), None);
        assert_eq!(state.due(MonoTime(10), usage(400), 300), Some(policy));
        assert_eq!(state.due(MonoTime(10), None, 0), None);
        let ticket = ReclaimTicket {
            binding: WorkerBinding {
                store: crate::identity::StoreBinding {
                    identity: crate::identity::StoreIdentity {
                        id: crate::identity::StoreId::new(1).unwrap(),
                        incarnation: crate::identity::StoreIncarnation::new(1).unwrap(),
                    },
                    session: crate::identity::StoreSession::new(1).unwrap(),
                },
                generation: crate::identity::StorageWorkerGeneration::new(1).unwrap(),
            },
            sequence: 1,
        };
        state.submitted(ticket, MonoTime(10));
        assert_eq!(state.due(MonoTime(20), usage(800), 0), None);
        state.completed(
            ReclaimEvent {
                request: ticket,
                result: Ok(LogReclaimed {
                    before_bytes: 800,
                    after_bytes: 500,
                }),
            },
            MonoTime(20),
        );
        assert_eq!(state.next_deadline, Some(MonoTime(1020)));
        // Spaced by `retry_ms` from the last attempt, not from completion.
        assert_eq!(state.due(MonoTime(109), usage(800), 0), None);
        assert_eq!(state.due(MonoTime(110), usage(800), 0), Some(policy));
        // At the interval deadline a rewrite needs `min_reclaim_bytes` (here
        // 100 / 16 = 6) reclaimable; otherwise the deadline moves on one
        // interval, never stops.
        assert_eq!(policy.min_reclaim_bytes, 6);
        assert_eq!(state.due(MonoTime(1020), usage(105), 100), None);
        assert_eq!(state.next_deadline, Some(MonoTime(2020)));
        assert_eq!(state.skipped, 1);
        assert_eq!(state.due(MonoTime(2020), usage(106), 100), Some(policy));
        // The interval still runs with no occupancy signal at all.
        assert_eq!(state.due(MonoTime(2020), None, 0), Some(policy));
    }
}
