// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Every refused in-place install, for every plan kind, leaves the log, its
//! summary and the plan byte-identical (reviewer-supplied regression).
use super::*;

#[derive(Debug, PartialEq)]
struct PlanView {
    update: LogUpdate,
    metadata: LogMetadata,
    full: Option<GroupLog>,
    suffix: Option<Suffix>,
    append: bool,
    commit: bool,
    prefix: bool,
}
fn view(plan: &OwnedNextLog) -> PlanView {
    PlanView {
        update: plan.update(),
        metadata: plan.metadata(),
        full: plan.full().cloned(),
        suffix: plan.suffix().cloned(),
        append: plan.is_append(),
        commit: plan.is_commit(),
        prefix: plan.preserves_checkpoint_prefix(),
    }
}
fn wrongs(state: &GroupLog) -> Vec<GroupLog> {
    let mut out = Vec::new();
    let mut w = state.clone();
    w.generation = LogGeneration::new(99).unwrap();
    out.push(w);
    let mut w = state.clone();
    w.revision = LogRevision::new(99).unwrap();
    out.push(w);
    let mut w = state.clone();
    w.hard_state.voted_for = Some(node(3));
    out.push(w);
    let mut w = state.clone();
    w.commit_index = 0;
    out.push(w);
    out
}
fn owned(state: &GroupLog, change: LogUpdate) -> OwnedNextLog {
    OwnedNextLog::plan_owned(
        &LogView::new(state),
        &LogSummary::from_validated(state),
        change,
        LogLimits::default(),
    )
    .unwrap()
}
type Case = (
    &'static str,
    GroupLog,
    Box<dyn Fn(&GroupLog) -> OwnedNextLog>,
);

fn cases() -> Vec<Case> {
    let state = current();
    let committed =
        stage_update(&state, &update(&state, 1, 2, None), LogLimits::default()).unwrap();
    vec![
        (
            "append",
            state.clone(),
            Box::new(|s: &GroupLog| {
                owned(
                    s,
                    update(
                        s,
                        1,
                        1,
                        Some(Suffix {
                            from: 3,
                            entries: vec![entry(3, 1)].into(),
                        }),
                    ),
                )
            }),
        ),
        (
            "commit",
            state.clone(),
            Box::new(|s: &GroupLog| {
                OwnedNextLog::plan_commit(
                    &LogView::new(s),
                    &LogSummary::from_validated(s),
                    2,
                    LogLimits::default(),
                )
                .unwrap()
            }),
        ),
        (
            "full",
            state.clone(),
            Box::new(|s: &GroupLog| {
                let change = update(
                    s,
                    2,
                    1,
                    Some(Suffix {
                        from: 2,
                        entries: vec![entry(2, 2)].into(),
                    }),
                );
                let plan = owned(s, change);
                assert!(
                    plan.full().is_some(),
                    "not full: append={} commit={}",
                    plan.is_append(),
                    plan.is_commit()
                );
                plan
            }),
        ),
        (
            "compact",
            committed,
            Box::new(|s: &GroupLog| {
                let plan = owned(s, compact::compact(s, 1));
                assert!(plan.full().is_none() && !plan.is_append());
                plan
            }),
        ),
    ]
}

#[test]
fn review_every_refused_in_place_install_changes_nothing() {
    for (name, state, make) in cases() {
        for unchanged in [false] {
            for (i, wrong) in wrongs(&state).into_iter().enumerate() {
                let mut plan = make(&state);
                let before = view(&plan);
                let mut target = wrong.clone();
                let mut summary = LogSummary::from_validated(&wrong);
                let summary_before = summary;
                let result = plan.install_in_place(&mut target, &mut summary, unchanged);
                if name == "full" || name == "compact" || i != 3 || true {
                    assert!(result.is_err(), "{name} wrong {i} must be refused");
                }
                assert_eq!(target, wrong, "{name}/{i}: log changed");
                assert_eq!(summary, summary_before, "{name}/{i}: summary changed");
                assert_eq!(view(&plan), before, "{name}/{i}: plan changed");
                // The intact plan still installs exactly as a fresh one.
                let mut ours = state.clone();
                let mut s = LogSummary::from_validated(&ours);
                plan.install_in_place(&mut ours, &mut s, false).unwrap();
                let mut oracle = state.clone();
                let expect = make(&state).install(&mut oracle).unwrap();
                assert_eq!(ours, oracle, "{name}/{i}: retried install differs");
                assert_eq!(s, expect);
                assert_eq!(s, LogSummary::from_validated(&ours));
                // A spent plan never installs again (fails closed).
                let mut again = ours.clone();
                let mut s2 = s;
                assert!(
                    plan.install_in_place(&mut again, &mut s2, false).is_err(),
                    "{name}: spent plan reinstalled"
                );
                assert_eq!(again, ours);
                assert_eq!(s2, s);
            }
        }
    }
}

#[test]
fn review_delta_next_log_refusals_change_nothing() {
    let state = current();
    let change = update(
        &state,
        1,
        2,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)].into(),
        }),
    );
    let mk = || {
        let p = NextLog::plan(
            &LogView::new(&state),
            &LogSummary::from_validated(&state),
            &change,
            LogLimits::default(),
        )
        .unwrap();
        assert!(matches!(p, NextLog::Delta(_)));
        p
    };
    for wrong in wrongs(&state) {
        let mut plan = mk();
        let meta = plan.metadata();
        let mut target = wrong.clone();
        assert!(plan.install_in_place(&mut target, &change).is_err());
        assert_eq!(target, wrong);
        assert_eq!(plan.metadata(), meta);
        let NextLog::Delta(d) = &plan else {
            unreachable!()
        };
        assert_eq!(d.append, change.suffix);
    }
    let mut bad = change.clone();
    bad.commit_index = 1;
    let mut plan = mk();
    let mut target = state.clone();
    assert!(plan.install_in_place(&mut target, &bad).is_err());
    assert_eq!(target, state);
    let mut ours = state.clone();
    plan.install_in_place(&mut ours, &change).unwrap();
    assert_eq!(
        ours,
        stage_update(&state, &change, LogLimits::default()).unwrap()
    );
    // compact via NextLog with update alterations
    let committed =
        stage_update(&state, &update(&state, 1, 2, None), LogLimits::default()).unwrap();
    let cchange = compact::compact(&committed, 1);
    let mut cplan = NextLog::plan(
        &LogView::new(&committed),
        &LogSummary::from_validated(&committed),
        &cchange,
        LogLimits::default(),
    )
    .unwrap();
    assert!(matches!(cplan, NextLog::Compact(_)));
    let mut wrong = cchange.clone();
    wrong.snapshot.as_mut().unwrap().checksum += 1;
    let mut t = committed.clone();
    assert!(cplan.install_in_place(&mut t, &wrong).is_err());
    assert_eq!(t, committed);
    cplan.install_in_place(&mut t, &cchange).unwrap();
    assert_eq!(
        t,
        stage_update(&committed, &cchange, LogLimits::default()).unwrap()
    );
    assert!(cplan.install_in_place(&mut t.clone(), &cchange).is_err());
}
