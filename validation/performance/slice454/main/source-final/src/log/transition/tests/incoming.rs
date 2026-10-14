// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn pointer(entry: &LogEntry) -> *const u8 {
    let EntryPayload::Command { bytes, .. } = &entry.payload else {
        unreachable!()
    };
    bytes.as_ptr()
}
#[test]
fn owned_append_moves_new_input_and_retains_canonical_prefix_and_exact_charge() {
    let mut state = current();
    let prefix = pointer(&state.entries[1]);
    let change = update(
        &state,
        1,
        2,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)],
        }),
    );
    let incoming = pointer(&change.suffix.as_ref().unwrap().entries[0]);
    let expected = stage_update(&state, &change, LogLimits::default()).unwrap();
    let original = change.clone();
    let plan = OwnedNextLog::plan_owned(
        &LogView::new(&state),
        &LogSummary::from_validated(&state),
        change,
        LogLimits::default(),
    )
    .unwrap();
    assert!(plan.is_append());
    assert!(plan.matches_update(&original));
    assert_eq!(pointer(plan.entry_at(3).unwrap()), incoming);
    assert_eq!(
        pointer(&plan.update().suffix.as_ref().unwrap().entries[0]),
        incoming
    );
    let summary = plan.install(&mut state).unwrap();
    assert_eq!(state, expected);
    assert_eq!(pointer(&state.entries[1]), prefix);
    assert_eq!(pointer(&state.entries[2]), incoming);
    assert_eq!(summary, LogSummary::from_validated(&state));
}
#[test]
fn copy_keeps_raw_external_capacity_and_seals_one_normalized_allocation() {
    let state = current();
    let mut change = update(
        &state,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)],
        }),
    );
    let suffix = change.suffix.as_mut().unwrap();
    suffix.entries.reserve(100);
    let EntryPayload::Command { bytes, .. } = &mut suffix.entries[0].payload else {
        unreachable!()
    };
    bytes.reserve(10000);
    let raw_capacity = bytes.capacity();
    let raw_pointer = bytes.as_ptr();
    let plan = OwnedNextLog::plan_copy(
        &LogView::new(&state),
        &LogSummary::from_validated(&state),
        &change,
        LogLimits::default(),
    )
    .unwrap();
    let owned = plan.update().suffix.as_ref().unwrap();
    let EntryPayload::Command { bytes, .. } = &owned.entries[0].payload else {
        unreachable!()
    };
    assert_eq!(bytes.capacity(), bytes.len());
    assert_eq!(owned.entries.capacity(), owned.entries.len());
    assert_ne!(bytes.as_ptr(), raw_pointer);
    assert_eq!(pointer(plan.entry_at(3).unwrap()), bytes.as_ptr());
    let EntryPayload::Command { bytes, .. } = &change.suffix.as_ref().unwrap().entries[0].payload
    else {
        unreachable!()
    };
    assert_eq!(bytes.capacity(), raw_capacity);
    assert_eq!(bytes.as_ptr(), raw_pointer);
    assert!(plan.matches_update(&change));
    let expected = stage_update(&state, &change, LogLimits::default()).unwrap();
    let mut canonical = state;
    let summary = plan.install(&mut canonical).unwrap();
    assert_eq!(canonical, expected);
    assert_eq!(summary, LogSummary::from_validated(&expected));
}
#[test]
fn owned_normalization_matches_old_clone_and_identical_headers_do_not_hide_payload_changes() {
    let state = current();
    let mut change = update(
        &state,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)],
        }),
    );
    let EntryPayload::Command { bytes, .. } =
        &mut change.suffix.as_mut().unwrap().entries[0].payload
    else {
        unreachable!()
    };
    bytes.reserve(10000);
    let original = change.clone();
    let expected = stage_update(&state, &change, LogLimits::default()).unwrap();
    let plan = OwnedNextLog::plan_owned(
        &LogView::new(&state),
        &LogSummary::from_validated(&state),
        change,
        LogLimits::default(),
    )
    .unwrap();
    for operation in [false, true] {
        let mut wrong = original.clone();
        let EntryPayload::Command {
            operation: id,
            bytes,
        } = &mut wrong.suffix.as_mut().unwrap().entries[0].payload
        else {
            unreachable!()
        };
        if operation {
            *id = OperationId::new(99).unwrap();
        } else {
            bytes[0] ^= 1;
        }
        assert!(!plan.matches_update(&wrong));
    }
    let mut canonical = state;
    let summary = plan.install(&mut canonical).unwrap();
    assert_eq!(canonical, expected);
    assert_eq!(summary, LogSummary::from_validated(&expected));
}
#[test]
fn owned_validation_and_terms_match_borrowed_oracle_for_valid_invalid_and_compact_updates() {
    let initial = current();
    let compact = compact::compact(&initial, 1);
    let checkpointed = stage_update(&initial, &compact, LogLimits::default()).unwrap();
    for state in [initial, checkpointed] {
        for term in 0..=2 {
            for commit in 0..=4 {
                for from in 1..=4 {
                    let change = update(
                        &state,
                        term,
                        commit,
                        Some(Suffix {
                            from,
                            entries: vec![entry(from, term)],
                        }),
                    );
                    compare_owned(&state, change);
                }
            }
        }
        compare_owned(&state, compact::compact(&state, 2));
    }
}
fn compare_owned(state: &GroupLog, change: LogUpdate) {
    let expected = stage_update(state, &change, LogLimits::default());
    let plan = OwnedNextLog::plan_owned(
        &LogView::new(state),
        &LogSummary::from_validated(state),
        change,
        LogLimits::default(),
    );
    match (expected, plan) {
        (Ok(expected), Ok(plan)) => {
            assert_eq!(plan.metadata(), LogMetadata::from_log(&expected));
            for index in 0..=4 {
                assert_eq!(plan.term_at(state, index), expected.term_at(index));
            }
            let mut canonical = state.clone();
            let summary = plan.install(&mut canonical).unwrap();
            assert_eq!(canonical, expected);
            assert_eq!(summary, LogSummary::from_validated(&canonical));
        }
        (Err(expected), Err(actual)) => assert_eq!(actual, expected),
        (expected, _) => panic!("owned transition diverged: {expected:?}"),
    }
}
#[test]
fn owned_preparation_rejects_stale_predecessor_without_installation() {
    let state = current();
    let change = update(
        &state,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)],
        }),
    );
    let plan = OwnedNextLog::plan_owned(
        &LogView::new(&state),
        &LogSummary::from_validated(&state),
        change,
        LogLimits::default(),
    )
    .unwrap();
    let mut wrong = state;
    wrong.revision = LogRevision::new(99).unwrap();
    let before = wrong.clone();
    assert!(plan.install(&mut wrong).is_err());
    assert_eq!(wrong, before);
}
