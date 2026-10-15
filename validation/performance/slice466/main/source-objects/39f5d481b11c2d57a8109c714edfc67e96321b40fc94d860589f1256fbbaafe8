// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn every_trim_boundary_preserves_exact_charge_with_known_or_overflowed_total() {
    let mut state = committed();
    let entries = (3..=17)
        .map(|index| {
            let mut next = entry(index, 1);
            match index % 3 {
                0 => next.payload = EntryPayload::Noop,
                1 => {
                    if let EntryPayload::Command { bytes, .. } = &mut next.payload {
                        *bytes = Vec::new().into();
                    }
                }
                _ => {
                    if let EntryPayload::Command { bytes, .. } = &mut next.payload {
                        *bytes = vec![7; index as usize].into();
                    }
                }
            }
            next
        })
        .collect();
    state = stage_update(
        &state,
        &update(&state, 1, 17, Some(Suffix { from: 3, entries })),
        LogLimits::default(),
    )
    .unwrap();
    for index in 1..=17 {
        let change = compact(&state, index);
        let expected = stage_update(&state, &change, LogLimits::default()).unwrap();
        for known in [true, false] {
            let mut summary = LogSummary::from_validated(&state);
            if !known {
                summary.retained = None;
            }
            let plan = NextLog::plan(
                &LogView::new(&state),
                &summary,
                &change,
                LogLimits::default(),
            )
            .unwrap();
            assert!(matches!(plan, NextLog::Compact(_)));
            let mut installed = state.clone();
            let actual = plan.install(&mut installed, change.clone()).unwrap();
            assert_eq!(installed, expected);
            assert_eq!(actual, LogSummary::from_validated(&installed));
        }
    }
}

#[test]
fn repeated_trim_of_base_and_unpublished_tail_matches_materialized_oracle() {
    let base = committed();
    let mut accepted = stage_update(&base, &compact(&base, 1), LogLimits::default()).unwrap();
    let tail = (3..=8).map(|index| entry(index, 1)).collect::<Vec<_>>();
    accepted = stage_update(
        &accepted,
        &update(
            &accepted,
            1,
            8,
            Some(Suffix {
                from: 3,
                entries: tail.clone().into(),
            }),
        ),
        LogLimits::default(),
    )
    .unwrap();
    let tail = EntryHistory::from(tail);
    let view = LogView::trimmed_overlay(&base, &tail, LogMetadata::from_log(&accepted), 1);
    assert_eq!(view.materialize(), accepted);
    for index in 2..=8 {
        let change = compact(&accepted, index);
        let expected = stage_update(&accepted, &change, LogLimits::default()).unwrap();
        for known in [true, false] {
            let mut summary = LogSummary::from_validated(&accepted);
            if !known {
                summary.retained = None;
            }
            let plan = NextLog::plan(&view, &summary, &change, LogLimits::default()).unwrap();
            let mut installed = accepted.clone();
            let actual = plan.install(&mut installed, change.clone()).unwrap();
            assert_eq!(installed, expected);
            assert_eq!(actual, LogSummary::from_validated(&installed));
        }
    }
}
