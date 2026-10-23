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
use super::*;

#[test]
fn summarized_metadata_borrows_the_exact_summary_including_copied_views() {
    let owner = SummarizedLog::new(current());
    let view = owner.view();
    let copied = view;
    for view in [view, copied] {
        assert!(std::ptr::eq(view.metadata(), &owner.summary().metadata));
        assert_eq!(*view.metadata(), LogMetadata::from_log(&owner));
        assert_eq!(view.stamp(), owner.stamp());
        assert!(view.summarized_by(owner.summary()));
        assert_eq!(view.materialize(), *owner);
    }
}

#[test]
fn new_accepted_and_trimmed_overlay_metadata_are_independent_exact_snapshots() {
    let base = current();
    let plain = LogView::new(&base);
    let mut snapshot = LogMetadata::from_log(&base);
    assert_eq!(*plain.metadata(), snapshot);
    assert!(!std::ptr::eq(plain.metadata(), &snapshot));
    let append = update(
        &base,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)].into(),
        }),
    );
    let accepted = stage_update(&base, &append, LogLimits::default()).unwrap();
    snapshot = LogMetadata::from_log(&accepted);
    let tail = EntryLog::from(append.suffix.unwrap().entries);
    let overlay = LogView::overlay(&base, &tail, snapshot);
    snapshot.commit_index = 0;
    assert_ne!(*overlay.metadata(), snapshot);
    assert_eq!(*overlay.metadata(), LogMetadata::from_log(&accepted));
    assert_eq!(overlay.materialize(), accepted);
    assert!(overlay.stamp().is_none());

    let compact = compact::compact(&accepted, 1);
    let trimmed = stage_update(&accepted, &compact, LogLimits::default()).unwrap();
    let summary = LogSummary::from_validated(&trimmed);
    let view = LogView::trimmed_overlay(&base, &tail, summary.metadata, 1);
    assert!(!std::ptr::eq(view.metadata(), &summary.metadata));
    assert_eq!(*view.metadata(), summary.metadata);
    assert_eq!(view.materialize(), trimmed);
    for index in 0..=4 {
        assert_eq!(view.term_at(index), trimmed.term_at(index));
    }
    assert_eq!(view.trimmed_charge(0), trimmed.entries.retained_bytes());
    assert!(view.summarized_by(&summary));
    assert!(view.stamp().is_none());
}

#[test]
fn equal_value_foreign_and_stale_summaries_keep_metadata_validation() {
    let owner = SummarizedLog::new(current());
    let equal = *owner.summary();
    assert!(!std::ptr::eq(owner.summary(), &equal));
    let change = update(&owner, 1, 2, None);
    for view in [owner.view(), LogView::new(&owner)] {
        assert!(view.summarized_by(&equal));
        assert!(
            OwnedNextLog::plan_owned(&view, &equal, change.clone(), LogLimits::default()).is_ok()
        );
        for metadata in [
            LogMetadata {
                revision: LogRevision::new(99).unwrap(),
                ..equal.metadata
            },
            LogMetadata {
                configuration: ConfigurationId::new(99).unwrap(),
                ..equal.metadata
            },
            LogMetadata {
                group: GroupIdentity {
                    id: GroupId::new(99).unwrap(),
                    ..equal.metadata.group
                },
                ..equal.metadata
            },
        ] {
            let foreign = LogSummary { metadata, ..equal };
            assert!(!view.summarized_by(&foreign));
            assert_eq!(
                OwnedNextLog::plan_owned(&view, &foreign, change.clone(), LogLimits::default())
                    .err(),
                Some(StorageError::Rejected("stale log summary"))
            );
        }
    }
}

#[test]
fn borrowed_and_owned_views_match_full_oracle_for_valid_and_refused_updates() {
    let initial = current();
    let checkpoint = stage_update(
        &initial,
        &compact::compact(&initial, 1),
        LogLimits::default(),
    )
    .unwrap();
    for state in [initial, checkpoint] {
        let owner = SummarizedLog::new(state);
        for term in 0..=2 {
            for commit in 0..=4 {
                for from in 1..=4 {
                    let change = update(
                        &owner,
                        term,
                        commit,
                        Some(Suffix {
                            from,
                            entries: vec![entry(from, term)].into(),
                        }),
                    );
                    let expected = stage_update(&owner, &change, LogLimits::default());
                    for view in [owner.view(), LogView::new(&owner)] {
                        for plan in [
                            OwnedNextLog::plan_owned(
                                &view,
                                owner.summary(),
                                change.clone(),
                                LogLimits::default(),
                            ),
                            OwnedNextLog::plan_copy(
                                &view,
                                owner.summary(),
                                &change,
                                LogLimits::default(),
                            ),
                            OwnedNextLog::full_oracle(view, &change, LogLimits::default()),
                        ] {
                            match (&expected, plan) {
                                (Ok(expected), Ok(plan)) => {
                                    assert_eq!(plan.metadata(), LogMetadata::from_log(expected));
                                    let mut installed = (*owner).clone();
                                    let summary = plan.install(&mut installed).unwrap();
                                    assert_eq!(&installed, expected);
                                    assert_eq!(summary, LogSummary::from_validated(expected));
                                }
                                (Err(expected), Err(actual)) => assert_eq!(&actual, expected),
                                (expected, _) => panic!("view diverged from oracle: {expected:?}"),
                            }
                        }
                    }
                }
            }
        }
    }
}

#[test]
fn borrowed_planning_keeps_owned_predecessor_and_foreign_stamp_checks() {
    let (mut plan, stamp, original, expected) = {
        let owner = SummarizedLog::new(current());
        let change = update(&owner, 1, 2, None);
        let expected = stage_update(&owner, &change, LogLimits::default()).unwrap();
        let view = owner.view();
        let plan =
            OwnedNextLog::plan_owned(&view, owner.summary(), change, LogLimits::default()).unwrap();
        (plan, view.stamp(), (*owner).clone(), expected)
    }; // No summary or view borrow escapes into the plan.
    let mut foreign = SummarizedLog::new(original);
    assert_ne!(stamp, foreign.stamp());
    let checked = checked_installs::count();
    foreign.install(&mut plan, stamp).unwrap();
    assert_eq!(checked_installs::count(), checked + 1);
    assert_eq!(*foreign, expected);
}
