// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fixed last-call boundaries, correlated to one unchanged compact helper invocation.
use super::*;

const BOUNDARIES: usize = 6;
const METHODS: [Phase; BOUNDARIES] = [
    Phase::StoreCheckpointState,
    Phase::SnapshotPin,
    Phase::SnapshotPin,
    Phase::StoreAppend,
    Phase::StoreBarrier,
    Phase::SnapshotReconcile,
];
const STORE_METHODS: [Phase; 3] = [
    Phase::StoreCheckpointState,
    Phase::StoreAppend,
    Phase::StoreBarrier,
];
const GAP_NAMES: [&str; 3] = ["state_to_pin", "pin_to_append", "barrier_to_reconcile"];
#[derive(Clone, Copy)]
struct Stamp {
    call: u64,
    at: Instant,
}
#[derive(Clone, Default)]
pub(super) struct Boundaries([Cell<Option<Stamp>>; BOUNDARIES]);
impl Boundaries {
    pub(super) fn record(&self, phase: Phase, call: u64, start: Instant, end: Instant) {
        match phase {
            Phase::StoreCheckpointState => self.0[0].set(Some(Stamp { call, at: end })),
            Phase::SnapshotPin => {
                self.0[1].set(Some(Stamp { call, at: start }));
                self.0[2].set(Some(Stamp { call, at: end }));
            }
            Phase::StoreAppend => self.0[3].set(Some(Stamp { call, at: start })),
            Phase::StoreBarrier => self.0[4].set(Some(Stamp { call, at: end })),
            Phase::SnapshotReconcile => self.0[5].set(Some(Stamp { call, at: start })),
            _ => (),
        }
    }
}
#[derive(Clone, Default)]
pub(super) struct Gaps([Cell<Interval>; 3]);
#[derive(Clone, Debug, Serialize)]
pub(super) struct GapReport {
    gap: &'static str,
    #[serde(flatten)]
    interval: Interval,
}
impl Gaps {
    /// All three counters advance together, only for a checked complete observation.
    fn advance(&self, stamps: [Stamp; BOUNDARIES]) -> Option<()> {
        let mut next = std::array::from_fn::<_, 3, _>(|i| self.0[i].get());
        for (i, pair) in stamps.as_chunks::<2>().0.iter().enumerate() {
            let elapsed =
                u64::try_from(pair[1].at.checked_duration_since(pair[0].at)?.as_nanos()).ok()?;
            next[i].add(elapsed, false, 0).ok()?;
        }
        for (cell, interval) in self.0.iter().zip(next) {
            cell.set(interval);
        }
        Some(())
    }
    pub(super) fn report(&self) -> [GapReport; 3] {
        std::array::from_fn(|i| GapReport {
            gap: GAP_NAMES[i],
            interval: self.0[i].get(),
        })
    }
}
impl<const ENABLED: bool> Recorder<ENABLED> {
    pub(crate) fn finish_compact_gaps(
        &self,
        before: Option<&ProviderSnapshot>,
        store: &Self,
        snapshots: &Self,
        failed: bool,
    ) {
        if !ENABLED || failed {
            return;
        }
        let Some(before) = before else {
            self.invalid.set(true);
            return;
        };
        let Some(after) = Self::provider_snapshot(store, snapshots) else {
            self.invalid.set(true);
            return;
        };
        // A volatile log has no store provider: none of its boundaries ran, so
        // there is no complete observation to record and none is fabricated.
        // The snapshot side must still have run exactly once each, unfailed.
        if STORE_METHODS
            .iter()
            .all(|phase| before.0[*phase as usize] == after.0[*phase as usize])
        {
            let once = |phase: Phase| {
                let (before, after) = (before.0[phase as usize], after.0[phase as usize]);
                before.calls.checked_add(1) == Some(after.calls)
                    && before.failures == after.failures
            };
            if !once(Phase::SnapshotPin) || !once(Phase::SnapshotReconcile) {
                self.invalid.set(true);
            }
            return;
        }
        let mut stamps = [None; BOUNDARIES];
        for (i, phase) in METHODS.iter().enumerate() {
            let index = *phase as usize;
            let next = after.0[index];
            if before.0[index].calls.checked_add(1) != Some(next.calls)
                || before.0[index].failures != next.failures
            {
                self.invalid.set(true);
                return;
            }
            let provider = if STORE_METHODS.iter().any(|m| *m as usize == *phase as usize) {
                store
            } else {
                snapshots
            };
            let Some(stamp) = provider.boundaries.0[i]
                .get()
                .filter(|stamp| stamp.call == next.calls)
            else {
                self.invalid.set(true);
                return;
            };
            stamps[i] = Some(stamp);
        }
        let stamps = stamps.map(Option::unwrap);
        if stamps.windows(2).any(|pair| pair[0].at > pair[1].at)
            || self.compact_gaps.advance(stamps).is_none()
        {
            self.invalid.set(true);
        }
    }
}
#[cfg(test)]
mod tests;
