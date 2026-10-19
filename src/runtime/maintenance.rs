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
    /// WAL high-water mark, in percent of the usable capacity: at or above it
    /// (or while the WAL thread holds a batch that does not fit) new client
    /// proposals are refused with the retryable `ClientError::StoragePressure`
    /// and a rewrite is due as soon as `min_reclaim_bytes` is reclaimable.
    pub high_water_percent: u8,
    /// Proposals resume once occupancy is below this percentage and no batch
    /// is held. `0 < low_water_percent < high_water_percent <= 100`.
    pub low_water_percent: u8,
}
/// Water marks that apply when no WAL policy is installed (explicit opt-out).
pub const DEFAULT_HIGH_WATER_PERCENT: u8 = 75;
pub const DEFAULT_LOW_WATER_PERCENT: u8 = 50;
impl WalMaintenancePolicy {
    /// The default schedule for a provider whose reclaim ceiling is `ceiling`.
    pub fn for_ceiling(ceiling: usize) -> Self {
        Self {
            interval_ms: 1000,
            retry_ms: 100,
            max_bytes: ceiling,
            // About 4 MiB at the default 64 MiB WAL.
            min_reclaim_bytes: ceiling / 16,
            high_water_percent: DEFAULT_HIGH_WATER_PERCENT,
            low_water_percent: DEFAULT_LOW_WATER_PERCENT,
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
    /// WAL high-water state (ST2b): new client proposals are refused with
    /// `ClientError::StoragePressure` until occupancy falls below the low-water
    /// mark and no batch is held. Heartbeats, votes, commit records and
    /// checkpoints are never refused by it.
    pub high_water: bool,
    /// Times the high-water state was entered (saturating).
    pub high_water_entries: u64,
    /// Client proposals refused by the high-water state (saturating).
    pub refused_proposals: u64,
}
/// WAL occupancy at or above half the usable capacity: checkpoints stop
/// waiting for their entry threshold and reclaim may run early.
pub(super) fn pressure(usage: Option<WalUsage>) -> bool {
    usage.is_some_and(|u| u.capacity > 0 && u.bytes >= u.capacity / 2)
}
fn percent_of(capacity: usize, percent: u8) -> usize {
    (capacity as u128 * u128::from(percent) / 100) as usize
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
            if p.interval_ms == 0
                || p.retry_ms == 0
                || p.max_bytes == 0
                || p.low_water_percent == 0
                || p.low_water_percent >= p.high_water_percent
                || p.high_water_percent > 100
            {
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
    /// Re-evaluate the high-water state from the latest published occupancy:
    /// enter at the high-water mark or while a batch is held, leave below the
    /// low-water mark with nothing held. Without occupancy it is never set.
    pub(super) fn update_high_water(&mut self, usage: Option<WalUsage>) -> bool {
        let Some(u) = usage.filter(|u| u.capacity > 0) else {
            self.high_water = false;
            return false;
        };
        let (high, low) = self.policy.map_or(
            (DEFAULT_HIGH_WATER_PERCENT, DEFAULT_LOW_WATER_PERCENT),
            |p| (p.high_water_percent, p.low_water_percent),
        );
        if u.parked > 0 || u.bytes >= percent_of(u.capacity, high) {
            if !self.high_water {
                self.high_water_entries = self.high_water_entries.saturating_add(1);
            }
            self.high_water = true;
        } else if u.bytes < percent_of(u.capacity, low) {
            self.high_water = false;
        }
        self.high_water
    }
    pub(super) fn refused_proposal(&mut self) {
        self.refused_proposals = self.refused_proposals.saturating_add(1);
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
    /// reclaimable, at most once per `retry_ms`. In the high-water state the
    /// early rewrite needs only `min_reclaim_bytes`; with a held batch, any
    /// growth since the last rewrite (compaction records mean checkpoints
    /// freed space the live estimate may not show). A provider that
    /// publishes no occupancy is rewritten at every deadline.
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
        let usage = usage.filter(|u| self.high_water || pressure(Some(*u)))?;
        let spaced = self
            .last_attempt
            .is_none_or(|t| now.0 >= t.0.saturating_add(policy.retry_ms));
        let reclaimable = usage.bytes.saturating_sub(live);
        let due = if self.high_water {
            let grown = self
                .last_reclaimed
                .is_none_or(|(_, r)| usage.bytes > r.after_bytes);
            reclaimable >= policy.min_reclaim_bytes.max(1) || (usage.parked > 0 && grown)
        } else {
            reclaimable >= usage.capacity / 8
        };
        (spaced && due).then_some(policy)
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
            high_water_percent: 75,
            low_water_percent: 50,
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
    fn usage(bytes: usize, parked: usize) -> Option<WalUsage> {
        Some(WalUsage {
            bytes,
            capacity: 1000,
            parked,
        })
    }
    #[test]
    fn high_water_enters_at_the_mark_or_a_held_batch_and_leaves_below_low_water() {
        let mut state = WalMaintenanceStatus::default();
        // No policy (explicit opt-out): the default marks, 75% and 50%.
        assert!(!state.update_high_water(usage(749, 0)));
        assert!(state.update_high_water(usage(750, 0)));
        assert_eq!(state.high_water_entries, 1);
        // Hysteresis: stays set until below the low-water mark.
        assert!(state.update_high_water(usage(500, 0)));
        assert!(!state.update_high_water(usage(499, 0)));
        // A held batch forces the state at any occupancy, and holds it.
        assert!(state.update_high_water(usage(10, 1)));
        assert_eq!(state.high_water_entries, 2);
        assert!(state.update_high_water(usage(10, 1)));
        assert_eq!(state.high_water_entries, 2);
        assert!(!state.update_high_water(usage(10, 0)));
        // No published occupancy: never set.
        assert!(state.update_high_water(usage(900, 0)));
        assert!(!state.update_high_water(None));
        // Policy marks apply when configured.
        let policy = WalMaintenancePolicy {
            high_water_percent: 90,
            low_water_percent: 20,
            ..WalMaintenancePolicy::for_ceiling(100)
        };
        state
            .configure(Some(policy), MonoTime(0), Some(100))
            .unwrap();
        assert!(!state.update_high_water(usage(899, 0)));
        assert!(state.update_high_water(usage(900, 0)));
        assert!(state.update_high_water(usage(200, 0)));
        assert!(!state.update_high_water(usage(199, 0)));
        for (high, low) in [(75, 0), (50, 50), (50, 75), (101, 50)] {
            let bad = WalMaintenancePolicy {
                high_water_percent: high,
                low_water_percent: low,
                ..policy
            };
            assert_eq!(
                state.configure(Some(bad), MonoTime(0), Some(100)),
                Err(NodeError::InvalidLimits)
            );
        }
    }
    #[test]
    fn high_water_rewrites_once_a_sixteenth_is_reclaimable_or_a_held_batch_may_fit() {
        let mut state = WalMaintenanceStatus::default();
        let policy = WalMaintenancePolicy::for_ceiling(1600);
        state
            .configure(Some(policy), MonoTime(0), Some(1600))
            .unwrap();
        assert_eq!(policy.min_reclaim_bytes, 100);
        // Below the high-water mark the pressure rule needs an eighth (125).
        assert!(!state.update_high_water(usage(700, 0)));
        assert_eq!(state.due(MonoTime(10), usage(700, 0), 590), None);
        // In high water a sixteenth of the ceiling (100) is enough.
        assert!(state.update_high_water(usage(800, 0)));
        assert_eq!(state.due(MonoTime(10), usage(800, 0), 701), None);
        assert_eq!(state.due(MonoTime(10), usage(800, 0), 700), Some(policy));
        // With a held batch: any growth since the last rewrite, even when
        // the live estimate says nothing is reclaimable.
        assert!(state.update_high_water(usage(300, 7)));
        assert_eq!(state.due(MonoTime(10), usage(300, 7), 400), Some(policy));
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
        state.completed(
            ReclaimEvent {
                request: ticket,
                result: Ok(LogReclaimed {
                    before_bytes: 990,
                    after_bytes: 300,
                }),
            },
            MonoTime(20),
        );
        // Unchanged since that rewrite: another would free nothing.
        assert_eq!(state.due(MonoTime(200), usage(300, 7), 400), None);
        assert_eq!(state.due(MonoTime(200), usage(301, 7), 400), Some(policy));
        // Spaced by `retry_ms` from the last attempt.
        assert_eq!(state.due(MonoTime(109), usage(301, 7), 400), None);
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
                parked: 0,
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
