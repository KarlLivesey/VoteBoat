// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn disabled_recorder_reads_no_clock_cells_or_snapshots() {
    let recorder = Recorder::<false>::default();
    recorder.invalid.set(true); // Poisoned cells cannot affect the disabled path.
    assert!(Recorder::<false>::start().is_none());
    recorder.finish(Phase::Core, None, true, usize::MAX);
    assert!(Recorder::<false>::provider_snapshot(&recorder, &recorder).is_none());
    recorder.finish_scope(Phase::CheckpointCompact, None, &recorder, &recorder);
    assert!(recorder.report(0, &recorder, &recorder).unwrap().is_none());
    assert!(recorder
        .intervals
        .iter()
        .all(|cell| cell.get() == Interval::default()));
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
#[test]
fn scoped_provider_deltas_exclude_setup_and_do_not_invent_maxima() {
    let actor = Recorder::<true>::default();
    let store = Recorder::<true>::default();
    let snapshots = Recorder::<true>::default();
    store.record(Phase::StoreState, 99, false, 12);
    let before = Recorder::provider_snapshot(&store, &snapshots);
    store.record(Phase::StoreState, 7, false, 6);
    snapshots.record(Phase::SnapshotLoadPinned, 3, false, 0);
    snapshots.record(Phase::SnapshotReconcile, 5, false, 0);
    actor.finish_scope(Phase::CheckpointCompact, before, &store, &snapshots);
    let report =
        serde_json::to_value(actor.report(0, &store, &snapshots).unwrap().unwrap()).unwrap();
    let scope = &report["checkpoint_provider_scopes"][2]["methods"];
    let state = scope
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["method"] == "store_state_clone_nested")
        .unwrap();
    assert_eq!(state["calls"], 1);
    assert_eq!(state["total_elapsed_ns"], 7);
    assert_eq!(state["items"], 6);
    assert!(state.get("maximum_elapsed_ns").is_none());
    assert!(report["nesting"]
        .as_str()
        .unwrap()
        .contains("reconcile_contains_load_pinned"));
    assert!(report["residual"]
        .as_str()
        .unwrap()
        .contains("not_pure_Core"));
}
#[test]
fn invalid_observation_does_not_interrupt_a_provider_and_is_rejected_at_report() {
    let recorder = Recorder::<true>::default();
    recorder.intervals[Phase::StoreAppend as usize].set(Interval {
        calls: u64::MAX,
        ..Interval::default()
    });
    recorder.finish(Phase::StoreAppend, Recorder::<true>::start(), false, 0);
    assert!(recorder.invalid.get());
    assert!(recorder.report(0, &recorder, &recorder).is_err());
    let before = ScopedInterval::default();
    assert!(before
        .advance(
            Interval {
                calls: 1,
                ..Interval::default()
            },
            Interval::default()
        )
        .is_none());
}

#[test]
fn exhausted_real_snapshot_observer_preserves_successful_provider_outcome() {
    use voteboat::{identity::*, snapshot::SnapshotRetention};
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let binding = StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    };
    let snapshots = crate::snapshots::MemorySnapshots::<true>::new(binding, group);
    snapshots.attribution.intervals[Phase::SnapshotLatest as usize].set(Interval {
        calls: u64::MAX,
        ..Interval::default()
    });
    assert_eq!(snapshots.latest_reference(), Ok(None));
    assert!(snapshots
        .attribution
        .report(0, &Recorder::default(), &snapshots.attribution)
        .is_err());
}
