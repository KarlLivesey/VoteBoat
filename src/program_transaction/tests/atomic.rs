// SPDX-License-Identifier: RPL-1.5
use super::*;
#[test]
fn all_key_commit_versions_and_read_dependencies_hold_until_applied_all() {
    let p = program(1, 0);
    let mut f = Fixture::new(&p);
    f.start(&p);
    let facts = f.prepared(&p);
    for app in &f.p {
        for id in [1, 2] {
            assert_eq!(app.key(key(id)), KeyRead::Locked(p.id()));
        }
    }
    let d = f.decide(&p, Decision::Commit, facts);
    let r = apply(
        &mut f.p[0],
        command(
            &p,
            ProgramStep::ApplyDecision,
            Some(Decision::Commit),
            vec![d],
        ),
    );
    assert_eq!(r.outcome, ProgramOutcome::DecisionApplied(Decision::Commit));
    assert_eq!(f.p[0].key(key(1)), KeyRead::Locked(p.id()));
    assert_eq!(f.p[0].keys[&key(1)].version, KeyVersion::new(1));
    f.finish(&p, Decision::Commit, d);
    for app in &mut f.p {
        assert_eq!(
            app.key(key(1)),
            KeyRead::Value {
                value: 11,
                version: KeyVersion::new(1)
            }
        );
        assert_eq!(
            app.key(key(2)),
            KeyRead::Value {
                value: 20,
                version: KeyVersion::default()
            }
        );
        let duplicate = prepare(app, &p);
        assert!(duplicate.duplicate);
        assert_eq!(duplicate.first_index, 1);
        assert_eq!(app.keys[&key(1)].version, KeyVersion::new(1));
    }
}
#[test]
fn failed_second_key_never_installs_partial_locks_and_refusal_is_retained() {
    let mut p = program(1, 0);
    p.participants[0].keys[1].expected_version = KeyVersion::new(1);
    let mut app = participant(3, 4);
    let before = app.keys.clone();
    let r = prepare(&mut app, &p);
    assert_eq!(
        r.outcome,
        ProgramOutcome::Prepare(PrepareOutcome::Refused(PrepareRefusal::Version {
            key: key(2),
            actual: KeyVersion::default()
        }))
    );
    assert_eq!(app.keys, before);
    let duplicate = prepare(&mut app, &p);
    assert!(duplicate.duplicate);
    assert_eq!(r.outcome, duplicate.outcome);
}
#[test]
fn no_wait_refusal_aborts_prepared_and_uncontacted_then_closes_late_prepare() {
    let p = program(1, 0);
    let mut f = Fixture::new(&p);
    f.start(&p);
    let lock = program(2, 0);
    assert_eq!(
        prepare(&mut f.p[1], &lock).outcome,
        ProgramOutcome::Prepare(PrepareOutcome::Prepared)
    );
    assert_eq!(
        prepare(&mut f.p[0], &p).outcome,
        ProgramOutcome::Prepare(PrepareOutcome::Prepared)
    );
    let refused = prepare(&mut f.p[1], &p);
    let ProgramOutcome::Prepare(PrepareOutcome::Refused(reason)) = refused.outcome else {
        panic!("expected actual semantic refusal")
    };
    let mut decide = command(
        &p,
        ProgramStep::Decide,
        Some(Decision::Abort),
        vec![fact(&f.p[1], refused)],
    );
    decide.refusal = Some((1, reason));
    let r = apply(&mut f.c, decide);
    let d = fact(&f.c, r);
    f.finish(&p, Decision::Abort, d);
    assert_eq!(
        prepare(&mut f.p[2], &p).outcome,
        ProgramOutcome::Prepare(PrepareOutcome::Closed(Decision::Abort))
    );
    assert_eq!(
        f.p[0].key(key(1)),
        KeyRead::Value {
            value: 10,
            version: KeyVersion::default()
        }
    );
    assert_eq!(f.p[1].key(key(1)), KeyRead::Locked(lock.id()));
}
#[test]
fn committed_same_value_advances_version_and_rejects_stale_program() {
    let mut p = program(1, 0);
    for part in &mut p.participants {
        part.keys[0].write = Some(10);
    }
    let mut f = Fixture::new(&p);
    f.start(&p);
    let facts = f.prepared(&p);
    let d = f.decide(&p, Decision::Commit, facts);
    f.finish(&p, Decision::Commit, d);
    let stale = program(3, 0);
    let r = prepare(&mut f.p[0], &stale);
    assert_eq!(
        r.outcome,
        ProgramOutcome::Prepare(PrepareOutcome::Refused(PrepareRefusal::Version {
            key: key(1),
            actual: KeyVersion::new(1)
        }))
    );
    assert_eq!(
        f.p[0].key(key(1)),
        KeyRead::Value {
            value: 10,
            version: KeyVersion::new(1)
        }
    );
}
