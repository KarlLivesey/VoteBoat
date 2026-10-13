// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded elapsed intervals; inclusive and nested, never phase CPU attribution.
use crate::Failure;
use serde::Serialize;
use std::time::Instant;

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
}
const PHASES: usize = 12;
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
];
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
pub(super) struct Recorder<const ENABLED: bool> {
    intervals: [Interval; PHASES],
}
impl<const ENABLED: bool> Default for Recorder<ENABLED> {
    fn default() -> Self {
        Self {
            intervals: [Interval::default(); PHASES],
        }
    }
}
impl<const ENABLED: bool> Recorder<ENABLED> {
    pub fn start() -> Option<Instant> {
        if ENABLED {
            Some(Instant::now())
        } else {
            None
        }
    }
    pub fn finish(
        &mut self,
        phase: Phase,
        started: Option<Instant>,
        failed: bool,
        items: usize,
    ) -> Result<(), Failure> {
        if ENABLED {
            let elapsed = u64::try_from(
                started
                    .ok_or("missing attribution start")?
                    .elapsed()
                    .as_nanos(),
            )?;
            self.intervals[phase as usize].add(elapsed, failed, u64::try_from(items)?)?;
        }
        Ok(())
    }
    pub fn report(&self, slot: usize, store: &Self) -> Option<Attribution> {
        ENABLED.then(|| Attribution {
            replica_slot: slot,
            scope: "whole_actor_lifetime_including_campaign_warmup_follower_drain_and_shutdown",
            clock: "monotonic_elapsed_wall_intervals_not_cpu_or_critical_path",
            nesting: "store_append_and_barrier_nested_in_persist_or_maintenance;maintenance_tick_includes_effects_and_nested_snapshot_helpers;actor_lifetime_includes_all;replicas_overlap;do_not_sum",
            item_units: "host_admission_core_proposals_application_results=original_commands;send=messages;other=zero",
            phases: NAMES.iter().enumerate().map(|(i, &phase)| PhaseReport {
                phase,
                interval: if i == Phase::StoreAppend as usize || i == Phase::StoreBarrier as usize { store.intervals[i] } else { self.intervals[i] },
            }).collect(),
        })
    }
}
#[derive(Clone, Debug, Serialize)]
pub(super) struct PhaseReport {
    phase: &'static str,
    #[serde(flatten)]
    interval: Interval,
}
#[derive(Clone, Debug, Serialize)]
pub(super) struct Attribution {
    replica_slot: usize,
    scope: &'static str,
    clock: &'static str,
    nesting: &'static str,
    item_units: &'static str,
    phases: Vec<PhaseReport>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn disabled_recorder_reads_no_clock_and_emits_no_record() {
        let mut recorder = Recorder::<false>::default();
        assert!(Recorder::<false>::start().is_none());
        recorder
            .finish(Phase::Core, None, true, usize::MAX)
            .unwrap();
        assert_eq!(recorder.intervals, [Interval::default(); PHASES]);
        assert!(recorder.report(0, &recorder).is_none());
    }
    #[test]
    fn interval_accounting_is_checked_and_overflow_is_atomic() {
        let mut interval = Interval::default();
        interval.add(7, false, 4).unwrap();
        interval.add(3, true, 1).unwrap();
        assert_eq!(
            interval,
            Interval {
                calls: 2,
                failures: 1,
                total_elapsed_ns: 10,
                maximum_elapsed_ns: 7,
                items: 5
            }
        );
        interval.calls = u64::MAX;
        let before = interval;
        assert!(interval.add(1, false, 1).is_err());
        assert_eq!(interval, before);
    }
}
