// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded elapsed intervals; inclusive and nested, never phase CPU attribution.
mod gaps;
use crate::Failure;
use gaps::{Boundaries, GapReport, Gaps};
use serde::Serialize;
use std::{cell::Cell, time::Instant};

#[derive(Clone, Copy)]
pub(super) enum Phase {
    HostAdmission,
    Core,
    PersistHelper,
    StoreAppend,
    StoreBarrier,
    Application,
    Results,
    Send,
    Maintenance,
    SnapshotMaintenance,
    HostWait,
    ActorLifetime,
    CheckpointReserve,
    CheckpointPublish,
    CheckpointCompact,
    CheckpointEffects,
    StoreState,
    SnapshotBegin,
    SnapshotWrite,
    SnapshotSeal,
    SnapshotPublish,
    SnapshotAbort,
    SnapshotLoad,
    SnapshotLatest,
    SnapshotPin,
    SnapshotLoadPinned,
    SnapshotReconcile,
    StoreMatchesState,
    StoreCheckpointState,
}
const PHASES: usize = 29;
const SCOPES: [Phase; 4] = [
    Phase::CheckpointReserve,
    Phase::CheckpointPublish,
    Phase::CheckpointCompact,
    Phase::CheckpointEffects,
];
const NAMES: [&str; PHASES] = [
    "host_admission",
    "core_direct",
    "persist_helper_inclusive",
    "store_append_nested",
    "store_barrier_nested",
    "application",
    "results",
    "send",
    "maintenance_tick_inclusive",
    "snapshot_helpers_inclusive",
    "host_wait",
    "actor_lifetime_inclusive",
    "checkpoint_reserve_inclusive",
    "checkpoint_publish_inclusive",
    "checkpoint_compact_inclusive",
    "checkpoint_effects_inclusive",
    "store_state_clone_nested",
    "snapshot_begin_nested",
    "snapshot_write_chunk_nested",
    "snapshot_seal_nested",
    "snapshot_publish_nested",
    "snapshot_abort_nested",
    "snapshot_load_nested",
    "snapshot_latest_reference_nested",
    "snapshot_pin_for_log_nested",
    "snapshot_load_pinned_nested",
    "snapshot_reconcile_log_inclusive_nested",
    "store_matches_state_exact_nested",
    "store_checkpoint_state_protected_inclusive_nested",
];
fn provider_phase(index: usize) -> bool {
    index == Phase::StoreAppend as usize
        || index == Phase::StoreBarrier as usize
        || index >= Phase::StoreState as usize
}
#[derive(Clone, Copy, Default, Debug, Serialize, PartialEq, Eq)]
pub(super) struct Interval {
    calls: u64,
    failures: u64,
    total_elapsed_ns: u64,
    maximum_elapsed_ns: u64,
    items: u64,
}
impl Interval {
    fn add(&mut self, elapsed: u64, failed: bool, items: u64) -> Result<(), Failure> {
        let next = Self {
            calls: self
                .calls
                .checked_add(1)
                .ok_or("attribution calls exhausted")?,
            failures: self
                .failures
                .checked_add(u64::from(failed))
                .ok_or("attribution failures exhausted")?,
            total_elapsed_ns: self
                .total_elapsed_ns
                .checked_add(elapsed)
                .ok_or("attribution elapsed exhausted")?,
            maximum_elapsed_ns: self.maximum_elapsed_ns.max(elapsed),
            items: self
                .items
                .checked_add(items)
                .ok_or("attribution items exhausted")?,
        };
        *self = next;
        Ok(())
    }
}
/// Additive scoped counters deliberately omit maxima: maxima cannot be differenced.
#[derive(Clone, Copy, Default, Debug, Serialize, PartialEq, Eq)]
struct ScopedInterval {
    calls: u64,
    failures: u64,
    total_elapsed_ns: u64,
    items: u64,
}
impl ScopedInterval {
    fn advance(self, before: Interval, after: Interval) -> Option<Self> {
        Some(Self {
            calls: self
                .calls
                .checked_add(after.calls.checked_sub(before.calls)?)?,
            failures: self
                .failures
                .checked_add(after.failures.checked_sub(before.failures)?)?,
            total_elapsed_ns: self.total_elapsed_ns.checked_add(
                after
                    .total_elapsed_ns
                    .checked_sub(before.total_elapsed_ns)?,
            )?,
            items: self
                .items
                .checked_add(after.items.checked_sub(before.items)?)?,
        })
    }
}
pub(super) struct ProviderSnapshot([Interval; PHASES]);
#[derive(Clone, Copy, Default, Debug, Eq, PartialEq, Serialize)]
struct EntryAllocations {
    observations: u64,
    last_bytes: usize,
    maximum_observed_bytes: usize,
}
#[derive(Clone)]
pub(super) struct Recorder<const ENABLED: bool> {
    intervals: [Cell<Interval>; PHASES],
    scopes: [Cell<[ScopedInterval; PHASES]>; 4],
    invalid: Cell<bool>,
    boundaries: Boundaries,
    compact_gaps: Gaps,
    entry_allocations: Cell<EntryAllocations>,
}
impl<const ENABLED: bool> Default for Recorder<ENABLED> {
    fn default() -> Self {
        Self {
            intervals: std::array::from_fn(|_| Cell::new(Interval::default())),
            scopes: std::array::from_fn(|_| Cell::new([ScopedInterval::default(); PHASES])),
            invalid: Cell::new(false),
            boundaries: Boundaries::default(),
            compact_gaps: Gaps::default(),
            entry_allocations: Cell::new(EntryAllocations::default()),
        }
    }
}
impl<const ENABLED: bool> Recorder<ENABLED> {
    /// Lazy diagnostic observation: disabled recording never inspects history.
    /// Failure poisons only the final report, preserving provider outcomes.
    pub fn observe_entry_allocations(&self, charge: impl FnOnce() -> Option<usize>) {
        if ENABLED {
            let before = self.entry_allocations.get();
            if let Some((bytes, observations)) = charge().zip(before.observations.checked_add(1)) {
                self.entry_allocations.set(EntryAllocations {
                    observations,
                    last_bytes: bytes,
                    maximum_observed_bytes: before.maximum_observed_bytes.max(bytes),
                });
            } else {
                self.invalid.set(true);
            }
        }
    }
    pub fn start() -> Option<Instant> {
        if ENABLED {
            Some(Instant::now())
        } else {
            None
        }
    }
    /// Observation failure never substitutes for a provider/protocol outcome.
    pub fn finish(&self, phase: Phase, started: Option<Instant>, failed: bool, items: usize) {
        if ENABLED {
            if let Some(start) = started {
                // Reuse the same one end-clock observation as the old elapsed() call.
                self.observe(phase, start, Instant::now(), failed, items);
            } else {
                self.invalid.set(true);
            }
        }
    }
    fn observe(&self, phase: Phase, start: Instant, end: Instant, failed: bool, items: usize) {
        if ENABLED {
            let measured = end
                .checked_duration_since(start)
                .and_then(|duration| u64::try_from(duration.as_nanos()).ok())
                .zip(u64::try_from(items).ok());
            if let Some((elapsed, items)) = measured {
                if let Some(call) = self.record(phase, elapsed, failed, items) {
                    self.boundaries.record(phase, call, start, end);
                }
            } else {
                self.invalid.set(true);
            }
        }
    }
    fn record(&self, phase: Phase, elapsed: u64, failed: bool, items: u64) -> Option<u64> {
        if ENABLED {
            let cell = &self.intervals[phase as usize];
            let mut interval = cell.get();
            if interval.add(elapsed, failed, items).is_ok() {
                cell.set(interval);
                return Some(interval.calls);
            }
            self.invalid.set(true);
        }
        None
    }
    pub fn provider_snapshot(store: &Self, snapshots: &Self) -> Option<ProviderSnapshot> {
        if ENABLED {
            Some(ProviderSnapshot(std::array::from_fn(|i| {
                if i == Phase::StoreAppend as usize
                    || i == Phase::StoreBarrier as usize
                    || i == Phase::StoreState as usize
                    || i == Phase::StoreMatchesState as usize
                    || i == Phase::StoreCheckpointState as usize
                {
                    store.intervals[i].get()
                } else {
                    snapshots.intervals[i].get()
                }
            })))
        } else {
            None
        }
    }
    pub fn finish_scope(
        &self,
        phase: Phase,
        before: Option<ProviderSnapshot>,
        store: &Self,
        snapshots: &Self,
    ) {
        if ENABLED {
            let scoped = SCOPES
                .iter()
                .position(|scope| *scope as usize == phase as usize);
            if let (Some(slot), Some(before), Some(after)) =
                (scoped, before, Self::provider_snapshot(store, snapshots))
            {
                let mut next = self.scopes[slot].get();
                for (i, value) in next
                    .iter_mut()
                    .enumerate()
                    .filter(|(i, _)| provider_phase(*i))
                {
                    let Some(delta) = value.advance(before.0[i], after.0[i]) else {
                        self.invalid.set(true);
                        return;
                    };
                    *value = delta;
                }
                self.scopes[slot].set(next);
            } else {
                self.invalid.set(true);
            }
        }
    }
    pub fn report(
        &self,
        slot: usize,
        store: &Self,
        snapshots: &Self,
    ) -> Result<Option<Attribution>, Failure> {
        if !ENABLED {
            return Ok(None);
        }
        if self.invalid.get() || store.invalid.get() || snapshots.invalid.get() {
            return Err("invalid attribution counters; provider outcomes were preserved".into());
        }
        let provider = Self::provider_snapshot(store, snapshots).unwrap();
        Ok(Some(Attribution {
            replica_slot: slot,
            scope: "whole_actor_lifetime_including_campaign_warmup_follower_drain_and_shutdown;provider_maxima_whole_provider_lifetime_including_setup",
            clock: "monotonic_elapsed_wall_intervals_not_cpu_or_critical_path",
            nesting: "checkpoint_intervals_nested_in_maintenance;provider_intervals_nested_in_helpers;reconcile_contains_load_pinned;effects_contain_persist_and_snapshot_helpers;actor_lifetime_includes_all;replicas_overlap;do_not_sum",
            residual: "helper_elapsed_outside_known_provider_methods_and_reported_compact_gaps_remains_unattributed;gap_labels_are_inclusive_not_pure_Core_or_Eq;scoped_provider_sums_are_not_additive_when_nested",
            item_units: "host_admission_core_proposals_application_results=original_commands;send=messages;store_state=cloned_retained_entries;store_matches_state=retained_entries_in_successfully_equal_state_false_or_error_zero_not_physical_traversal;store_checkpoint_state=retained_entries_in_successfully_equal_state_false_or_error_zero_not_actual_compared_or_skipped_entries_full_fallback_inside_this_phase;snapshot_write=attempted_application_bytes;other=zero",
            phases: NAMES.iter().enumerate().map(|(i, &phase)| PhaseReport {
                phase, interval: if provider_phase(i) { provider.0[i] } else { self.intervals[i].get() },
            }).collect(),
            checkpoint_compact_gap_scope: "whole_successful_checkpoint_compact_observations_including_warmup_and_drain;state_to_pin_starts_after_protected_checkpoint_verification_including_any_full_fallback_and_includes_guards_excludes_prior_verification;pin_to_append_includes_nested_pinned_load_verification_restore_and_planning;barrier_to_reconcile_includes_Core_completion_install_broadcast_and_return_cleanup;elapsed_not_CPU_or_pure_Core;do_not_add_nested_provider_intervals",
            checkpoint_compact_gaps: self.compact_gaps.report(),
            entry_allocation_observation_scope: "TRACE_only_whole_lifetime_per_owner_EntryHistory_backing_payloads_hidden_entries_and_descriptor_capacity;Core_after_successful_persistence_and_maintenance;store_after_successful_volatile_apply_including_pending;not_continuous_peak_not_RSS_excludes_other_Core_pending_effects_application_snapshot_queue_and_transient_coalescing_memory;shared_owners_charged_in_full_do_not_sum_as_physical_RSS",
            core_entry_allocations: self.entry_allocations.get(),
            store_entry_allocations: store.entry_allocations.get(),
            checkpoint_provider_scopes: SCOPES.iter().enumerate().map(|(slot, &phase)| {
                let values = self.scopes[slot].get();
                ScopeReport { scope: NAMES[phase as usize], methods: NAMES.iter().enumerate()
                    .filter(|(i, _)| provider_phase(*i)).map(|(i, &method)| MethodReport { method, interval: values[i] }).collect() }
            }).collect(),
        }))
    }
}
#[derive(Clone, Debug, Serialize)]
struct PhaseReport {
    phase: &'static str,
    #[serde(flatten)]
    interval: Interval,
}
#[derive(Clone, Debug, Serialize)]
struct MethodReport {
    method: &'static str,
    #[serde(flatten)]
    interval: ScopedInterval,
}
#[derive(Clone, Debug, Serialize)]
struct ScopeReport {
    scope: &'static str,
    methods: Vec<MethodReport>,
}
#[derive(Clone, Debug, Serialize)]
pub(super) struct Attribution {
    replica_slot: usize,
    scope: &'static str,
    clock: &'static str,
    nesting: &'static str,
    residual: &'static str,
    item_units: &'static str,
    phases: Vec<PhaseReport>,
    checkpoint_provider_scopes: Vec<ScopeReport>,
    checkpoint_compact_gap_scope: &'static str,
    checkpoint_compact_gaps: [GapReport; 3],
    entry_allocation_observation_scope: &'static str,
    core_entry_allocations: EntryAllocations,
    store_entry_allocations: EntryAllocations,
}
#[cfg(test)]
mod tests;
