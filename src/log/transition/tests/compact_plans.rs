// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn compact_layout_does_not_increase_the_existing_plan_budget() {
    use crate::log::transition::incoming::{CommitAdvance, OwnedAppend, OwnedChange};
    let sizes = [
        ("OwnedAppend", size_of::<OwnedAppend>(), 672),
        ("CommitAdvance", size_of::<CommitAdvance>(), 640),
        ("OwnedChange", size_of::<OwnedChange>(), 672),
        ("OwnedNextLog", size_of::<OwnedNextLog>(), 672),
        // Native's by-value prepared boundary is a separate, larger type in
        // the original flow; it is not the staged core-plan budget.
        ("PreparedOwnedLog", size_of::<PreparedOwnedLog>(), 688),
    ];
    for (name, size, baseline) in sizes {
        println!("{name}={size}");
        assert!(size <= baseline, "{name} exceeds its original type size");
    }
}

#[test]
fn compact_narrow_provider_input_and_plan_have_independent_entry_owners() {
    let state = current();
    let mut raw = Vec::with_capacity(2);
    raw.push(entry(3, 1));
    let change = update(
        &state,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: raw.into(),
        }),
    );
    let input = change.suffix.as_ref().unwrap();
    let pointer = input.entries.first_backing_ptr();
    let capacity = input.entries.allocated_entries();
    let plan = OwnedNextLog::plan_copy(
        &LogView::new(&state),
        &LogSummary::from_validated(&state),
        &change,
        LogLimits::default(),
    )
    .unwrap();
    assert_ne!(plan.suffix().unwrap().entries.first_backing_ptr(), pointer);
    assert_eq!(
        change.suffix.as_ref().unwrap().entries.first_backing_ptr(),
        pointer
    );
    assert_eq!(
        change.suffix.as_ref().unwrap().entries.allocated_entries(),
        capacity
    );
    drop(plan);
    assert_eq!(change.suffix.as_ref().unwrap().entries[0], entry(3, 1));
}

fn append(state: &GroupLog, width: usize) -> LogUpdate {
    let from = state.last_index() + 1;
    let entries = (from..from + width as u64)
        .map(|index| LogEntry {
            index,
            term: state.hard_state.term,
            payload: EntryPayload::Command {
                operation: OperationId::new((1u128 << 100) | u128::from(index)).unwrap(),
                bytes: index.to_le_bytes().to_vec().into(),
            },
        })
        .collect::<Vec<_>>();
    LogUpdate {
        hard_state: state.hard_state,
        ..update(
            state,
            state.hard_state.term,
            state.commit_index,
            Some(Suffix {
                from,
                entries: entries.into(),
            }),
        )
    }
}

fn paired_round(state: &GroupLog, width: usize, stamp_kind: usize) {
    let mut canonical = SummarizedLog::new(state.clone());
    let change = append(state, width);
    let expected = stage_update(state, &change, LogLimits::default()).unwrap();
    let view = canonical.view();
    let mut plan =
        OwnedNextLog::plan_owned(&view, canonical.summary(), change, LogLimits::default()).unwrap();
    assert!(plan.is_append());
    let planned = match stamp_kind {
        0 => view.stamp(),
        1 => None,
        _ => SummarizedLog::new(state.clone()).stamp(),
    };
    let before = canonical.stamp().unwrap();
    canonical.install(&mut plan, planned).unwrap();
    assert_eq!(*canonical, expected);
    assert_eq!(*canonical.summary(), LogSummary::from_validated(&expected));
    assert_eq!(canonical.stamp().unwrap().version(), before.version() + 1);
    let commit = expected.last_index();
    let expected_commit = stage_update(
        &expected,
        &LogUpdate {
            hard_state: expected.hard_state,
            ..update(&expected, expected.hard_state.term, commit, None)
        },
        LogLimits::default(),
    )
    .unwrap();
    let view = canonical.view();
    let mut plan =
        OwnedNextLog::plan_commit(&view, canonical.summary(), commit, LogLimits::default())
            .unwrap();
    let stamp = if stamp_kind == 0 { view.stamp() } else { None };
    canonical.install(&mut plan, stamp).unwrap();
    assert_eq!(*canonical, expected_commit);
    assert_eq!(
        *canonical.summary(),
        LogSummary::from_validated(&expected_commit)
    );
    assert_eq!(canonical.stamp().unwrap().version(), before.version() + 2);
}

#[test]
fn compact_paired_install_matches_full_oracle_at_all_widths_and_stamp_paths() {
    let histories = configured::histories();
    let states = [
        current(),
        histories[3].clone(),
        configured::checkpoint(&histories[5], 5),
    ];
    for state in &states {
        for width in [1, 231, 515] {
            for stamp_kind in 0..3 {
                paired_round(state, width, stamp_kind);
            }
        }
    }
}

fn different_predecessors(state: &GroupLog) -> Vec<GroupLog> {
    let mut wrong = vec![state.clone(); 10];
    wrong[0].bootstrap.group.incarnation = GroupIncarnation::new(9).unwrap();
    wrong[1].bootstrap.configuration = ConfigurationId::new(9).unwrap();
    wrong[2].generation = LogGeneration::new(9).unwrap();
    wrong[5].hard_state.term += 1;
    wrong[6].commit_index += 1;
    wrong[7].entries.edit(|entries| {
        entries.pop();
    });
    wrong[8].snapshot = Some(SnapshotRef {
        store: store(1),
        group: group(),
        configuration: ConfigurationId::new(1).unwrap(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 1,
        checksum: 1,
    });
    wrong[3].snapshot = wrong[8].snapshot.map(|snapshot| SnapshotRef {
        index: 2,
        ..snapshot
    });
    wrong[4].snapshot = wrong[8].snapshot.map(|snapshot| SnapshotRef {
        term: 2,
        ..snapshot
    });
    wrong[9].ballot_origin = Some(BallotOrigin {
        configuration: ConfigurationId::new(1).unwrap(),
        candidate_store: store(1),
    });
    wrong
}

#[test]
fn compact_raw_refusal_preserves_plan_log_summary_for_equal_revision_changes() {
    let state = current();
    for mut wrong in different_predecessors(&state) {
        assert_eq!(wrong.revision, state.revision);
        for commit in [false, true] {
            let change = append(&state, 231);
            let view = LogView::new(&state);
            let summary = LogSummary::from_validated(&state);
            let mut plan = if commit {
                OwnedNextLog::plan_commit(&view, &summary, 2, LogLimits::default()).unwrap()
            } else {
                OwnedNextLog::plan_owned(&view, &summary, change, LogLimits::default()).unwrap()
            };
            let original = plan.update();
            let metadata = plan.metadata();
            let before = wrong.clone();
            let mut summary = LogSummary::from_validated(&wrong);
            let before_summary = summary;
            assert!(plan
                .install_in_place(&mut wrong, &mut summary, false)
                .is_err());
            assert_eq!(wrong, before);
            assert_eq!(summary, before_summary);
            assert_eq!(plan.update(), original);
            assert_eq!(plan.metadata(), metadata);
        }
    }
}
