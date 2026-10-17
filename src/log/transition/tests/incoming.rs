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
    assert_eq!(pointer(&plan.suffix().unwrap().entries[0]), incoming);
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
    let raw_capacity = suffix.entries.capacity();
    let EntryPayload::Command { bytes, .. } = &mut suffix.entries[0].payload else {
        unreachable!()
    };
    let raw_charge = bytes.retained_bytes();
    let raw_pointer = bytes.as_ptr();
    let plan = OwnedNextLog::plan_copy(
        &LogView::new(&state),
        &LogSummary::from_validated(&state),
        &change,
        LogLimits::default(),
    )
    .unwrap();
    let owned = plan.suffix().unwrap();
    let EntryPayload::Command { bytes, .. } = &owned.entries[0].payload else {
        unreachable!()
    };
    assert_eq!(bytes.retained_bytes(), raw_charge);
    assert_eq!(owned.entries.capacity(), owned.entries.len());
    assert_eq!(bytes.as_ptr(), raw_pointer);
    assert_eq!(pointer(plan.entry_at(3).unwrap()), bytes.as_ptr());
    let EntryPayload::Command { bytes, .. } = &change.suffix.as_ref().unwrap().entries[0].payload
    else {
        unreachable!()
    };
    assert_eq!(
        change.suffix.as_ref().unwrap().entries.capacity(),
        raw_capacity
    );
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
    let mut raw = bytes.to_vec();
    raw.reserve(10000);
    *bytes = raw.into();
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
            let mut raw = bytes.to_vec();
            raw[0] ^= 1;
            *bytes = raw.into();
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
/// Every answer of the scalar plan equals the empty append `plan_owned` builds
/// for the same commit-only update, including its errors.
fn assert_commit_equivalent(
    state: &GroupLog,
    summary: &LogSummary,
    commit: u64,
    limits: LogLimits,
) {
    let view = LogView::new(state);
    let change = LogUpdate {
        hard_state: state.hard_state,
        ..update(state, state.hard_state.term, commit, None)
    };
    let owned = OwnedNextLog::plan_owned(&view, summary, change.clone(), limits);
    let scalar = OwnedNextLog::plan_commit(&view, summary, commit, limits);
    assert_eq!(
        scalar.as_ref().err(),
        owned.as_ref().err(),
        "commit {commit}"
    );
    let (Ok(owned), Ok(scalar)) = (owned, scalar) else {
        return;
    };
    assert!(owned.is_append() && !scalar.is_append());
    assert!(scalar.is_commit() && scalar.suffix().is_none());
    assert_eq!(scalar.update(), change);
    assert!(scalar.full().is_none());
    assert_eq!(scalar.metadata(), owned.metadata());
    assert!(scalar.preserves_checkpoint_prefix());
    for index in 0..=state.last_index() + 1 {
        assert_eq!(scalar.term_at(state, index), owned.term_at(state, index));
        assert!(scalar.entry_at(index).is_none());
    }
    let mut wrong = vec![change.clone(); 7];
    wrong[0].group.incarnation = GroupIncarnation::new(2).unwrap();
    wrong[1].expected_revision = LogRevision::new(state.revision.get() + 1).unwrap();
    wrong[2].hard_state.term += 1;
    wrong[3].commit_index += 1;
    wrong[4].suffix = Some(Suffix {
        from: state.last_index() + 1,
        entries: Vec::new(),
    });
    wrong[5].snapshot = Some(SnapshotRef {
        store: store(1),
        group: state.bootstrap.group,
        configuration: state.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 1,
        checksum: 1,
    });
    wrong[6].snapshot_membership = state.membership().ok().map(Box::new);
    // Both plans answer exactly as a comparison with the original would.
    assert_eq!(owned.update(), change);
    for candidate in std::iter::once(&change).chain(&wrong) {
        assert_eq!(scalar.matches_update(candidate), *candidate == change);
        assert_eq!(owned.matches_update(candidate), *candidate == change);
    }
    assert!(scalar.matches_update(&change));
    // The predecessor check precedes any mutation.
    let mut moved = state.clone();
    moved.revision = LogRevision::new(state.revision.get() + 5).unwrap();
    let refused = scalar.prepare_install(&moved).err();
    assert!(refused.is_some());
    assert_eq!(
        refused,
        OwnedNextLog::plan_owned(&view, summary, change.clone(), limits)
            .unwrap()
            .prepare_install(&moved)
            .err()
    );
    let scalar = OwnedNextLog::plan_commit(&view, summary, commit, limits).unwrap();
    let (mut left, mut right) = (state.clone(), state.clone());
    let installed = scalar.install(&mut left).unwrap();
    assert_eq!(installed, owned.install(&mut right).unwrap());
    assert_eq!(left, right);
    assert_eq!(installed, LogSummary::from_validated(&left));
    let mut expected = state.clone();
    expected.revision = LogRevision::new(state.revision.get() + 1).unwrap();
    expected.commit_index = commit;
    assert_eq!(left, expected);
}
#[test]
fn scalar_commit_plan_matches_the_owned_empty_append() {
    let limits = LogLimits::default();
    let plain = current();
    let mut voted = plain.clone();
    voted.hard_state.voted_for = Some(node(1));
    voted.ballot_origin = Some(BallotOrigin {
        configuration: voted.bootstrap.configuration,
        candidate_store: store(1),
    });
    let mut unvalidated_vote = plain.clone();
    unvalidated_vote.hard_state.voted_for = Some(node(1));
    let mut behind = plain.clone();
    behind.hard_state.term = 0;
    let mut exhausted = plain.clone();
    exhausted.revision = LogRevision::new(u64::MAX).unwrap();
    for state in [&plain, &voted, &unvalidated_vote, &behind, &exhausted] {
        let summary = LogSummary::from_validated(state);
        for commit in 0..=state.last_index() + 1 {
            let mut regressed = state.clone();
            regressed.commit_index = state.last_index();
            assert_commit_equivalent(state, &summary, commit, limits);
            assert_commit_equivalent(
                &regressed,
                &LogSummary::from_validated(&regressed),
                commit,
                limits,
            );
        }
    }
    // A stale summary and invalid limits fail the same way.
    let stale = LogSummary::from_validated(&voted);
    assert_commit_equivalent(&plain, &stale, 2, limits);
    let invalid = LogLimits {
        max_groups: 0,
        ..limits
    };
    assert_commit_equivalent(&plain, &LogSummary::from_validated(&plain), 2, invalid);
}
