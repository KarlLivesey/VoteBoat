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
use crate::identity::GroupIdentity;
use crate::{
    contracts::StorageError,
    log::{LogReclaimed, WalUsage},
    worker::*,
};
use std::collections::BTreeSet;

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
    /// The WAL space above the high-water mark (`headroom` bytes) cannot hold
    /// the largest batch the store may encode plus the control reserve
    /// (`required`), so a batch could wait for ever for a rewrite to make
    /// room (ST2 round 3, I5).
    WalHeadroomTooSmall { headroom: usize, required: usize },
}
/// Longest accepted maintenance or scan interval (24 hours): a larger value
/// could overflow the next deadline and stop the schedule without a signal
/// (ST2 round 3, I6). This bounds the interval, not the clock: within 24 hours
/// of `u64` millisecond exhaustion the next deadline is `None` and the
/// schedule stops, under the recorded protocol-clock horizon.
pub const MAX_SCHEDULE_MS: u64 = 24 * 60 * 60 * 1000;
/// The space above the WAL high-water mark must hold the largest batch the
/// store may encode plus its control reserve: data never exceeds the mark
/// (the shard's admission account), so after a rewrite a held batch fits
/// above the live data and control records (I5).
pub(super) fn check_wal_headroom(
    usage: Option<WalUsage>,
    high_water_percent: u8,
    max_batch_bytes: usize,
    control_reserve_bytes: usize,
) -> Result<(), NodeError> {
    let Some(u) = usage.filter(|u| u.capacity > 0) else {
        return Ok(());
    };
    let headroom = u
        .capacity
        .saturating_sub(percent_of(u.capacity, high_water_percent));
    let required = max_batch_bytes.saturating_add(control_reserve_bytes);
    if headroom < required {
        return Err(NodeError::Maintenance(
            MaintenanceError::WalHeadroomTooSmall { headroom, required },
        ));
    }
    Ok(())
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
    /// Client proposals refused by the high-water state or the entry gate
    /// (saturating).
    pub refused_proposals: u64,
    /// Groups whose retained entries are in the entry high-water state (ST2
    /// round 3, I2): entered at `high_water_percent` of
    /// `max_entries_per_group`, left below `low_water_percent`; their new
    /// proposals are refused with `ClientError::StoragePressure`.
    pub entry_high_water: BTreeSet<GroupIdentity>,
    /// Proposals refused by the entry gate (saturating; included in
    /// `refused_proposals`).
    pub refused_entry_proposals: u64,
    /// Since when the WAL thread has held at least one batch that does not
    /// fit yet (protocol time of the poll that first saw it); None when
    /// nothing is held.
    pub held_since: Option<MonoTime>,
    // Data refusals by the shard's byte account seen so far, and whether a
    // new one arrived since the state was last evaluated (I5).
    budget_refusals: u64,
    budget_exhausted: bool,
    /// Noop transitions refused by the shard's account (R4-1) since the last
    /// successful rewrite: like a held batch, a rewrite is then due whenever
    /// the WAL grew since the last one, so dead records alone can never keep
    /// a group without a leader.
    pub noop_starved: bool,
    noop_refusals: u64,
}
/// WAL occupancy at or above half the usable capacity: checkpoints stop
/// waiting for their entry threshold and reclaim may run early.
pub(super) fn pressure(usage: Option<WalUsage>) -> bool {
    usage.is_some_and(|u| u.capacity > 0 && u.bytes >= u.capacity / 2)
}
pub(super) fn percent_of(capacity: usize, percent: u8) -> usize {
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
                || p.interval_ms > MAX_SCHEDULE_MS
                || p.retry_ms > MAX_SCHEDULE_MS
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
        let exhausted = std::mem::take(&mut self.budget_exhausted);
        if u.parked > 0 || exhausted || self.noop_starved || u.bytes >= percent_of(u.capacity, high)
        {
            if !self.high_water {
                self.high_water_entries = self.high_water_entries.saturating_add(1);
            }
            self.high_water = true;
        } else if u.bytes < percent_of(u.capacity, low) {
            self.high_water = false;
        }
        self.high_water
    }
    /// The shard's running total of data refused or declined for the byte
    /// account (I5). A new refusal enters the high-water state like a held
    /// batch: admission closes before more data reaches a core.
    pub(super) fn note_budget_refusals(&mut self, total: u64) {
        if total > self.budget_refusals {
            self.budget_refusals = total;
            self.budget_exhausted = true;
        }
    }
    /// The shard's running total of refused noop transitions (R4-1). A new
    /// one enters the high-water state and makes a rewrite due once the WAL
    /// has grown since the last rewrite, until a rewrite succeeds.
    pub(super) fn note_noop_refusals(&mut self, total: u64) {
        if total > self.noop_refusals {
            self.noop_refusals = total;
            self.noop_starved = true;
        }
    }
    /// The high-water mark in bytes of a WAL with `capacity` usable bytes.
    pub fn high_water_bytes(&self, capacity: usize) -> usize {
        percent_of(capacity, self.high_water_percent())
    }
    pub(super) fn high_water_percent(&self) -> u8 {
        self.policy
            .map_or(DEFAULT_HIGH_WATER_PERCENT, |p| p.high_water_percent)
    }
    pub(super) fn low_water_percent(&self) -> u8 {
        self.policy
            .map_or(DEFAULT_LOW_WATER_PERCENT, |p| p.low_water_percent)
    }
    /// The entry gate for `group` holding `retained` entries of its data
    /// budget `max_entries`, with the same hysteresis as the WAL gate.
    pub(super) fn entry_high_water(
        &mut self,
        group: GroupIdentity,
        retained: usize,
        max_entries: usize,
    ) -> bool {
        if retained < percent_of(max_entries, self.low_water_percent()) {
            if !self.entry_high_water.is_empty() {
                self.entry_high_water.remove(&group);
            }
            return false;
        }
        if retained >= percent_of(max_entries, self.high_water_percent()) {
            self.entry_high_water.insert(group);
            return true;
        }
        self.entry_high_water.contains(&group)
    }
    pub(super) fn refused_entry_proposal(&mut self) {
        self.refused_entry_proposals = self.refused_entry_proposals.saturating_add(1);
        self.refused_proposal();
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
    /// deadline with less moves on one interval and is then checked like any
    /// other poll (R5-2). Due early when the occupancy passes half the
    /// capacity and at least an eighth of it is reclaimable, at most once per
    /// `retry_ms`. In the high-water state the early rewrite needs only
    /// `min_reclaim_bytes`; with a held batch, any growth since the last
    /// rewrite (compaction records mean checkpoints freed space the live
    /// estimate may not show), and the same after a refused noop (R4-1). A
    /// provider that publishes no occupancy is rewritten at every deadline.
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
            // R5-2: a poll that lands on every deadline must still reach the
            // early rules below (held batch, refused noop), or they never run.
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
            reclaimable >= policy.min_reclaim_bytes.max(1)
                || ((usage.parked > 0 || self.noop_starved) && grown)
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
                self.noop_starved = false;
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
    /// ST2 round 4 (R4-1): a refused noop enters high water and makes a
    /// rewrite due whenever the WAL grew since the last rewrite (dead vote
    /// and commit records), never when it did not (an irreducible live
    /// image: no rewrite storm). A successful rewrite clears the state.
    #[test]
    fn a_refused_noop_rewrites_only_while_the_wal_grows() {
        let mut state = WalMaintenanceStatus::default();
        let policy = WalMaintenancePolicy::for_ceiling(1600);
        state
            .configure(Some(policy), MonoTime(0), Some(1600))
            .unwrap();
        // Below the mark, live estimate equal to occupancy: nothing due.
        assert!(!state.update_high_water(usage(600, 0)));
        assert_eq!(state.due(MonoTime(10), usage(600, 0), 600), None);
        state.note_noop_refusals(1);
        assert!(state.noop_starved);
        assert!(state.update_high_water(usage(600, 0)));
        assert_eq!(state.due(MonoTime(10), usage(600, 0), 600), Some(policy));
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
                    before_bytes: 600,
                    after_bytes: 590,
                }),
            },
            MonoTime(20),
        );
        assert!(!state.noop_starved);
        // Refused again with nothing grown: no rewrite.
        state.note_noop_refusals(2);
        assert_eq!(state.due(MonoTime(200), usage(590, 0), 590), None);
        // An unchanged total is not a new refusal.
        state.noop_starved = false;
        state.note_noop_refusals(2);
        assert!(!state.noop_starved);
        state.note_noop_refusals(3);
        // Grown since the last rewrite: due again, spaced by `retry_ms`.
        assert_eq!(state.due(MonoTime(200), usage(591, 0), 591), Some(policy));
    }
    fn ticket(sequence: u64) -> ReclaimTicket {
        ReclaimTicket {
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
            sequence,
        }
    }
    /// Submit and complete one successful rewrite at `now` to `after_bytes`.
    fn rewrite(state: &mut WalMaintenanceStatus, sequence: u64, now: MonoTime, after_bytes: usize) {
        state.submitted(ticket(sequence), now);
        state.completed(
            ReclaimEvent {
                request: ticket(sequence),
                result: Ok(LogReclaimed {
                    before_bytes: after_bytes,
                    after_bytes,
                }),
            },
            now,
        );
    }
    /// ST2 round 5 (R5-2), Sol's round-4 reproduction: every host poll lands
    /// on the interval deadline, where too little is reclaimable for the
    /// interval rule. The refused-noop rule must still run there: the WAL
    /// grew since the last rewrite, so a rewrite is due. On cd90fb7a the
    /// deadline branch returned first: 0 rewrites in 100 polls, `skipped`
    /// 100, `noop_starved` still set.
    #[test]
    fn refused_noop_gets_a_rewrite_when_every_poll_is_a_deadline() {
        let ceiling = 4608 * 1024;
        let capacity = ceiling - 64 * 1024;
        assert_eq!(capacity, 4_653_056);
        check_wal_headroom(
            Some(WalUsage {
                bytes: 0,
                capacity,
                parked: 0,
            }),
            DEFAULT_HIGH_WATER_PERCENT,
            1024 * 1024,
            64 * 1024,
        )
        .unwrap();
        let mut state = WalMaintenanceStatus::default();
        let policy = WalMaintenancePolicy::for_ceiling(ceiling);
        assert_eq!(policy.min_reclaim_bytes, 294_912);
        state
            .configure(Some(policy), MonoTime(0), Some(ceiling))
            .unwrap();
        state.note_noop_refusals(1);
        let live = 3_554_000;
        let mut rewrites = 0;
        for n in 1..=100u64 {
            let now = MonoTime(1000 * n);
            let usage = Some(WalUsage {
                bytes: 3_555_000 + n as usize / 2,
                capacity,
                parked: 0,
            });
            assert!(state.update_high_water(usage));
            if state.due(now, usage, live).is_some() {
                rewrites += 1;
                break;
            }
        }
        assert_eq!(rewrites, 1, "skipped {}", state.skipped);
        assert_eq!(state.skipped, 1, "the deadline still moved on");
    }
    /// R5-2: at deadlines the starvation rule keeps its `retry_ms` spacing
    /// and its "grown since the last rewrite" condition. Here the interval
    /// (10 ms) is shorter than the spacing (100 ms) and every poll is a
    /// deadline: rewrites come at most once per 100 ms while the WAL grows
    /// and refusals continue, and never once it stops growing.
    #[test]
    fn starvation_rewrites_at_deadlines_keep_their_spacing_and_need_growth() {
        let ceiling = 1600;
        let mut state = WalMaintenanceStatus::default();
        let policy = WalMaintenancePolicy {
            interval_ms: 10,
            retry_ms: 100,
            ..WalMaintenancePolicy::for_ceiling(ceiling)
        };
        state
            .configure(Some(policy), MonoTime(0), Some(ceiling))
            .unwrap();
        let usage = |bytes| {
            Some(WalUsage {
                bytes,
                capacity: ceiling,
                parked: 0,
            })
        };
        let mut attempts = Vec::new();
        let mut bytes = 600;
        let mut refusals = 0;
        for n in 1..=100u64 {
            let now = MonoTime(10 * n);
            assert_eq!(state.next_deadline, Some(now), "every poll a deadline");
            bytes += 1;
            refusals += 1;
            state.note_noop_refusals(refusals);
            state.update_high_water(usage(bytes));
            if state.due(now, usage(bytes), bytes).is_some() {
                attempts.push(now.0);
                rewrite(&mut state, n, now, bytes);
            }
        }
        assert!(attempts.len() >= 2, "{attempts:?}");
        assert!(
            attempts.windows(2).all(|w| w[1] - w[0] >= policy.retry_ms),
            "{attempts:?}"
        );
        // A rewrite at the current size, then no growth: no rewrite at any
        // later deadline, however many refusals follow.
        rewrite(&mut state, 101, MonoTime(1000), bytes);
        let last = attempts.len();
        for n in 101..=200u64 {
            let now = MonoTime(10 * n);
            refusals += 1;
            state.note_noop_refusals(refusals);
            state.update_high_water(usage(bytes));
            assert_eq!(state.due(now, usage(bytes), bytes), None, "at {}", now.0);
        }
        assert_eq!(attempts.len(), last);
    }
    /// ST2 round 3 (I2): the entry gate has the WAL gate's hysteresis.
    #[test]
    fn entry_gate_enters_at_the_high_mark_and_leaves_below_the_low_mark() {
        let mut state = WalMaintenanceStatus::default();
        let group = crate::identity::GroupIdentity {
            id: crate::identity::GroupId::new(1).unwrap(),
            incarnation: crate::identity::GroupIncarnation::new(1).unwrap(),
        };
        assert!(!state.entry_high_water(group, 12_287, 16_384));
        assert!(state.entry_high_water(group, 12_288, 16_384));
        assert!(state.entry_high_water(group, 8_192, 16_384));
        assert_eq!(state.entry_high_water.len(), 1);
        assert!(!state.entry_high_water(group, 8_191, 16_384));
        assert!(state.entry_high_water.is_empty());
        state.refused_entry_proposal();
        assert_eq!(
            (state.refused_entry_proposals, state.refused_proposals),
            (1, 1)
        );
    }
    /// ST2 round 3 (I5, U5): the space above the high-water mark must hold the
    /// largest batch plus the control reserve; no bounded WAL, no check.
    #[test]
    fn wal_headroom_must_hold_the_largest_batch_and_the_reserve() {
        let wal = |capacity| {
            Some(WalUsage {
                bytes: 0,
                capacity,
                parked: 0,
            })
        };
        // Defaults: 63 MiB usable, 75% mark, 1 MiB batches and reserve.
        let usable = 63 * 1024 * 1024;
        assert_eq!(
            check_wal_headroom(wal(usable), 75, 1 << 20, 1 << 20),
            Ok(())
        );
        // The round-2 Codex case: 4 KiB usable, 8 KiB batches, 4 KiB reserve.
        assert_eq!(
            check_wal_headroom(wal(4096), 75, 8192, 4096),
            Err(NodeError::Maintenance(
                MaintenanceError::WalHeadroomTooSmall {
                    headroom: 1024,
                    required: 12_288,
                }
            ))
        );
        assert_eq!(check_wal_headroom(None, 75, usize::MAX, 0), Ok(()));
        // An out-of-range mark cannot underflow the check.
        assert!(check_wal_headroom(wal(4096), 200, 1, 0).is_err());
    }
    /// ST2 round 3 (I6): intervals that could overflow a deadline are refused.
    #[test]
    fn schedules_longer_than_a_day_are_refused_at_configuration() {
        let mut state = WalMaintenanceStatus::default();
        let policy = WalMaintenancePolicy::for_ceiling(100);
        for bad in [
            WalMaintenancePolicy {
                interval_ms: MAX_SCHEDULE_MS + 1,
                ..policy
            },
            WalMaintenancePolicy {
                retry_ms: u64::MAX,
                ..policy
            },
        ] {
            assert_eq!(
                state.configure(Some(bad), MonoTime(0), Some(100)),
                Err(NodeError::InvalidLimits)
            );
        }
        let longest = WalMaintenancePolicy {
            interval_ms: MAX_SCHEDULE_MS,
            ..policy
        };
        state
            .configure(Some(longest), MonoTime(0), Some(100))
            .unwrap();
        assert_eq!(state.next_deadline, Some(MonoTime(MAX_SCHEDULE_MS)));
    }
}
