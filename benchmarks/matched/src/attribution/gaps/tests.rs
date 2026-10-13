// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::time::Duration;

struct Fixture {
    actor: Recorder<true>,
    store: Recorder<true>,
    snapshots: Recorder<true>,
    base: Instant,
}
impl Fixture {
    fn new() -> Self {
        Self {
            actor: Recorder::default(),
            store: Recorder::default(),
            snapshots: Recorder::default(),
            base: Instant::now(),
        }
    }
    fn before(&self) -> Option<ProviderSnapshot> {
        Recorder::provider_snapshot(&self.store, &self.snapshots)
    }
    fn methods(&self, offset: u64, state_to_pin: u64) {
        let at = |n| self.base + Duration::from_nanos(offset + n);
        self.store
            .observe(Phase::StoreMatchesState, at(0), at(1), false, 0);
        self.snapshots.observe(
            Phase::SnapshotPin,
            at(1 + state_to_pin),
            at(3 + state_to_pin),
            false,
            0,
        );
        self.store.observe(
            Phase::StoreAppend,
            at(6 + state_to_pin),
            at(7 + state_to_pin),
            false,
            0,
        );
        self.store.observe(
            Phase::StoreBarrier,
            at(9 + state_to_pin),
            at(11 + state_to_pin),
            false,
            0,
        );
        self.snapshots.observe(
            Phase::SnapshotReconcile,
            at(14 + state_to_pin),
            at(17 + state_to_pin),
            false,
            0,
        );
    }
    fn finish(&self, before: Option<&ProviderSnapshot>, failed: bool) {
        self.actor
            .finish_compact_gaps(before, &self.store, &self.snapshots, failed);
    }
    fn counts(&self) -> [Interval; 3] {
        std::array::from_fn(|i| self.actor.compact_gaps.0[i].get())
    }
}
#[test]
fn complete_ordinals_produce_exact_gaps_and_actual_whole_gap_maxima() {
    let fixture = Fixture::new();
    for (offset, gap) in [(0, 7), (100, 2)] {
        let before = fixture.before();
        fixture.methods(offset, gap);
        fixture.finish(before.as_ref(), false);
    }
    let intervals = fixture.counts();
    assert_eq!(
        intervals[0],
        Interval {
            calls: 2,
            failures: 0,
            total_elapsed_ns: 9,
            maximum_elapsed_ns: 7,
            items: 0
        }
    );
    for interval in &intervals[1..] {
        assert_eq!(
            *interval,
            Interval {
                calls: 2,
                failures: 0,
                total_elapsed_ns: 6,
                maximum_elapsed_ns: 3,
                items: 0
            }
        );
    }
    let value = serde_json::to_value(
        fixture
            .actor
            .report(0, &fixture.store, &fixture.snapshots)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        value["checkpoint_compact_gaps"].as_array().unwrap().len(),
        3
    );
    assert!(value["checkpoint_compact_gap_scope"]
        .as_str()
        .unwrap()
        .contains("nested_pinned_load"));
}
#[test]
fn missing_stale_and_backward_marks_invalidate_only_the_final_diagnostics() {
    for fault in 0..3 {
        let fixture = Fixture::new();
        let before = fixture.before();
        fixture.methods(0, 7);
        let mark = fixture.snapshots.boundaries.0[1].get().unwrap();
        fixture.snapshots.boundaries.0[1].set(match fault {
            0 => None,
            1 => Some(Stamp {
                call: mark.call - 1,
                ..mark
            }),
            _ => Some(Stamp {
                at: fixture.base,
                ..mark
            }),
        });
        fixture.finish(before.as_ref(), false);
        assert_eq!(fixture.counts(), [Interval::default(); 3]);
        assert!(fixture
            .actor
            .report(0, &fixture.store, &fixture.snapshots)
            .is_err());
    }
}
#[test]
fn missing_prefix_extra_calls_or_failed_methods_cannot_certify_a_successful_helper() {
    for fault in 0..3 {
        let fixture = Fixture::new();
        let before = fixture.before();
        fixture.methods(0, 7);
        if fault == 1 {
            let _ = fixture.store.record(Phase::StoreMatchesState, 1, false, 0);
        } else if fault == 2 {
            let cell = &fixture.store.intervals[Phase::StoreMatchesState as usize];
            cell.set(Interval {
                failures: 1,
                ..cell.get()
            });
        }
        fixture.finish(if fault == 0 { None } else { before.as_ref() }, false);
        assert_eq!(fixture.counts(), [Interval::default(); 3]);
        assert!(fixture.actor.invalid.get());
    }
}
#[test]
fn overflow_is_checked_before_any_gap_counter_advances() {
    let fixture = Fixture::new();
    let before = fixture.before();
    fixture.methods(0, 7);
    fixture.actor.compact_gaps.0[2].set(Interval {
        calls: u64::MAX,
        ..Interval::default()
    });
    let old = fixture.counts();
    fixture.finish(before.as_ref(), false);
    assert_eq!(fixture.counts(), old);
    assert!(fixture
        .actor
        .report(0, &fixture.store, &fixture.snapshots)
        .is_err());
}
#[test]
fn partial_helper_error_does_not_read_or_fabricate_a_completed_gap() {
    let fixture = Fixture::new();
    fixture.finish(None, true);
    assert!(!fixture.actor.invalid.get());
    assert_eq!(fixture.counts(), [Interval::default(); 3]);
}
#[test]
fn disabled_mode_ignores_poisoned_marks_counters_and_missing_clock_inputs() {
    let actor = Recorder::<false>::default();
    let store = Recorder::<false>::default();
    let snapshots = Recorder::<false>::default();
    store.invalid.set(true);
    snapshots.invalid.set(true);
    actor.invalid.set(true);
    let poisoned = Stamp {
        call: u64::MAX,
        at: Instant::now(),
    };
    for cell in store.boundaries.0.iter().chain(&snapshots.boundaries.0) {
        cell.set(Some(poisoned));
    }
    for slot in &actor.compact_gaps.0 {
        slot.set(Interval {
            calls: u64::MAX,
            ..Interval::default()
        });
    }
    store.intervals[Phase::StoreMatchesState as usize].set(Interval {
        calls: u64::MAX,
        ..Interval::default()
    });
    actor.finish_compact_gaps(None, &store, &snapshots, false);
    store.finish(Phase::StoreMatchesState, None, true, usize::MAX);
    assert!(Recorder::<false>::start().is_none());
    assert!(actor.report(0, &store, &snapshots).unwrap().is_none());
    assert!(actor
        .compact_gaps
        .0
        .iter()
        .all(|slot| slot.get().calls == u64::MAX));
}
