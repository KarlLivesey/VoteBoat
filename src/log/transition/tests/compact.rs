// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

mod smaller;

pub(super) fn compact(state: &GroupLog, index: u64) -> LogUpdate {
    let mut change = update(state, state.hard_state.term, state.commit_index, None);
    change.hard_state = state.hard_state;
    change.snapshot = Some(SnapshotRef {
        store: store(1),
        group: state.bootstrap.group,
        configuration: state.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index,
        term: state.term_at(index).unwrap_or(1),
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    });
    change
}
fn committed() -> GroupLog {
    let state = current();
    stage_update(&state, &update(&state, 1, 2, None), LogLimits::default()).unwrap()
}
fn pointers(state: &GroupLog) -> Vec<*const u8> {
    state
        .entries
        .iter()
        .map(|entry| {
            let EntryPayload::Command { bytes, .. } = &entry.payload else {
                unreachable!()
            };
            bytes.as_ptr()
        })
        .collect()
}
#[test]
fn snapshot_boundary_ballot_commit_and_overflow_errors_match_full_oracle() {
    let first = committed();
    let second = stage_update(&first, &compact(&first, 1), LogLimits::default()).unwrap();
    for state in [first, second] {
        for index in 0..=4 {
            for term in 0..=2 {
                for commit in 0..=4 {
                    let mut change = compact(&state, index);
                    change.snapshot.as_mut().unwrap().term = term;
                    change.commit_index = commit;
                    compare(&state, change, LogLimits::default());
                }
            }
        }
        let valid = compact(&state, 2);
        for mutation in 0..10 {
            let mut change = valid.clone();
            let reference = change.snapshot.as_mut().unwrap();
            match mutation {
                0 => {
                    reference.group = GroupIdentity {
                        id: GroupId::new(8).unwrap(),
                        ..reference.group
                    }
                }
                1 => reference.configuration = ConfigurationId::new(8).unwrap(),
                2 => reference.index = u64::MAX,
                3 => reference.application_schema = 0,
                4 => reference.file_bytes = 47,
                5 => reference.file_bytes = u64::MAX,
                6 => change.expected_revision = LogRevision::new(100).unwrap(),
                7 => change.hard_state.term += 1,
                8 => {
                    change.suffix = Some(Suffix {
                        from: 3,
                        entries: vec![].into(),
                    })
                }
                _ => change.hard_state.voted_for = Some(node(4)),
            }
            compare(&state, change, LogLimits::default());
        }
        for (generation, revision) in [(u64::MAX, 1), (1, u64::MAX), (u64::MAX, u64::MAX)] {
            let mut exhausted = state.clone();
            exhausted.generation = LogGeneration::new(generation).unwrap();
            exhausted.revision = LogRevision::new(revision).unwrap();
            for commit in 0..=3 {
                let mut change = compact(&exhausted, 2);
                change.commit_index = commit;
                compare(&exhausted, change, LogLimits::default());
            }
        }
    }
}
#[test]
fn compaction_preserves_surviving_payloads_origin_and_exact_charge() {
    let mut state = committed();
    state.hard_state.voted_for = Some(node(4));
    state.ballot_origin = Some(BallotOrigin {
        configuration: ConfigurationId::new(2).unwrap(),
        candidate_store: store(4),
    });
    let original = pointers(&state);
    for index in [1, 2] {
        let change = compact(&state, index);
        let expected = stage_update(&state, &change, LogLimits::default()).unwrap();
        let plan = NextLog::plan(
            &LogView::new(&state),
            &LogSummary::from_validated(&state),
            &change,
            LogLimits::default(),
        )
        .unwrap();
        assert!(matches!(plan, NextLog::Compact(_)));
        for point in 0..=3 {
            assert_eq!(
                plan.term_at(&state, &change, point),
                expected.term_at(point)
            );
        }
        let summary = plan.install(&mut state, change).unwrap();
        assert_eq!(state, expected);
        assert_eq!(summary, LogSummary::from_validated(&state));
        assert_eq!(pointers(&state), original[index as usize..]);
    }
}
#[test]
fn immutable_command_shape_compacts_and_overflow_charge_recomputes_remaining_slice() {
    let mut state = committed();
    state.entries.edit(|entries| {
        let EntryPayload::Command { bytes, .. } = &mut entries[1].payload else {
            unreachable!()
        };
        let mut raw = bytes.to_vec();
        raw.reserve(10000);
        *bytes = raw.into();
    });
    let change = compact(&state, 1);
    let plan = NextLog::plan(
        &LogView::new(&state),
        &LogSummary::from_validated(&state),
        &change,
        LogLimits::default(),
    )
    .unwrap();
    assert!(plan.full().is_none());
    let expected = stage_update(&state, &change, LogLimits::default()).unwrap();
    let summary = plan.install(&mut state, change).unwrap();
    assert_eq!(state, expected);
    assert_eq!(summary, LogSummary::from_validated(&expected));
    let mut overflow = LogSummary::from_validated(&state);
    overflow.retained = None; // Exercise the otherwise impractically large cached-overflow branch.
    let change = compact(&state, 2);
    let plan = NextLog::plan(
        &LogView::new(&state),
        &overflow,
        &change,
        LogLimits::default(),
    )
    .unwrap();
    let summary = plan.install(&mut state, change).unwrap();
    assert_eq!(summary.retained_bytes(), Some(0));
    assert_eq!(summary, LogSummary::from_validated(&state));
}
#[test]
fn compact_plan_rejects_stale_summary_predecessor_and_altered_reference() {
    let state = committed();
    let change = compact(&state, 1);
    let mut stale = state.clone();
    stale.revision = LogRevision::new(99).unwrap();
    assert!(NextLog::plan(
        &LogView::new(&state),
        &LogSummary::from_validated(&stale),
        &change,
        LogLimits::default()
    )
    .is_err());
    for alteration in 0..11 {
        let plan = NextLog::plan(
            &LogView::new(&state),
            &LogSummary::from_validated(&state),
            &change,
            LogLimits::default(),
        )
        .unwrap();
        let mut canonical = state.clone();
        let mut wrong = change.clone();
        match alteration {
            0 => canonical.generation = LogGeneration::new(99).unwrap(),
            1 => wrong.snapshot.as_mut().unwrap().checksum += 1,
            2 => wrong.snapshot.as_mut().unwrap().store = store(99),
            3 => wrong.snapshot.as_mut().unwrap().application_schema += 1,
            4 => wrong.commit_index = 0,
            5 => wrong.expected_revision = LogRevision::new(99).unwrap(),
            6 => wrong.hard_state.term += 1,
            7 => {
                wrong.suffix = Some(Suffix {
                    from: 3,
                    entries: vec![].into(),
                })
            }
            8 => wrong.snapshot_membership = Some(Box::new(state.membership().unwrap())),
            9 => wrong.group.id = GroupId::new(99).unwrap(),
            _ => wrong.snapshot = None,
        }
        let before = canonical.clone();
        assert!(plan.install(&mut canonical, wrong).is_err());
        assert_eq!(canonical, before);
    }
}
#[test]
fn trimmed_overlay_terms_materialization_and_configured_fallback_are_exact() {
    let base = committed();
    let change = compact(&base, 1);
    let plan = NextLog::plan(
        &LogView::new(&base),
        &LogSummary::from_validated(&base),
        &change,
        LogLimits::default(),
    )
    .unwrap();
    let NextLog::Compact(delta) = plan else {
        panic!("compact required")
    };
    let tail = EntryLog::default();
    let view = LogView::trimmed_overlay(&base, &tail, delta.metadata(), delta.removed);
    let expected = stage_update(&base, &change, LogLimits::default()).unwrap();
    assert_eq!(view.materialize(), expected);
    for index in 0..=3 {
        assert_eq!(view.term_at(index), expected.term_at(index));
    }
    let mut next = update(
        &expected,
        2,
        2,
        Some(Suffix {
            from: 2,
            entries: vec![entry(2, 2)].into(),
        }),
    );
    next.commit_index = 1; // Replacement crossing commit remains rejected by the full oracle.
    assert_eq!(
        NextLog::plan(&view, &delta.summary, &next, LogLimits::default()).err(),
        stage_update(&expected, &next, LogLimits::default()).err()
    );
}
