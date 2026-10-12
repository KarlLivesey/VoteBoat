// SPDX-License-Identifier: RPL-1.5
use super::*;
#[test]
fn retained_and_pending_content_capacity_and_deterministic_coordinators() {
    let p = program(1, 0);
    let other = program(3, 0);
    assert_eq!(p.coordinator(), other.coordinator());
    assert_ne!(p.coordinator(), program(2, 0).coordinator());
    let app = coordinator(&p, 1);
    let b = ProgramApplication::start(p.clone()).unwrap();
    let c = ProgramApplication::start(other.clone()).unwrap();
    assert_eq!(
        app.validate_proposal(
            other.operation(0).unwrap(),
            &c,
            [(p.operation(0).unwrap(), b.as_slice())].into_iter()
        ),
        Err(ApplicationError::DedupCapacity)
    );
    let changed = program(1, 1);
    let c = ProgramApplication::start(changed.clone()).unwrap();
    assert_eq!(
        app.validate_proposal(
            p.operation(0).unwrap(),
            &c,
            [(p.operation(0).unwrap(), b.as_slice())].into_iter()
        ),
        Err(ApplicationError::OperationConflict)
    );
    let mut app = app;
    apply(&mut app, Command::start(p.clone()));
    assert_eq!(
        app.validate_proposal(p.operation(0).unwrap(), &c, std::iter::empty()),
        Err(ApplicationError::OperationConflict)
    );
    assert!(app
        .validate_proposal(p.operation(0).unwrap(), &b, std::iter::empty())
        .is_ok());
}
#[test]
fn aggregate_catalog_capacity_retains_absent_abort_and_control_phases_at_bound() {
    let mut app = participant(3, 1);
    assert_eq!(app.transaction_capacity(), 2);
    for id in [1, 2] {
        let p = program(id, 0);
        let decision = PhaseFact {
            group: p.coordinator(),
            operation: p.operation(1).unwrap(),
            index: 2,
        };
        let r = apply(
            &mut app,
            command(
                &p,
                ProgramStep::ApplyDecision,
                Some(Decision::Abort),
                vec![decision],
            ),
        );
        assert_eq!(r.outcome, ProgramOutcome::DecisionApplied(Decision::Abort));
    }
    assert_eq!(app.remaining_transactions(), 0);
    let p = program(1, 0);
    let all = PhaseFact {
        group: p.coordinator(),
        operation: p.operation(2).unwrap(),
        index: 3,
    };
    let r = apply(
        &mut app,
        command(&p, ProgramStep::Release, Some(Decision::Abort), vec![all]),
    );
    assert_eq!(r.outcome, ProgramOutcome::Released(Decision::Abort));
    assert_eq!(
        prepare(&mut app, &p).outcome,
        ProgramOutcome::Prepare(PrepareOutcome::Closed(Decision::Abort))
    );
    let q = program(3, 0);
    let c = command(&q, ProgramStep::Prepare, None, vec![]);
    let bytes = codec::encode(&c).unwrap();
    assert_eq!(
        app.validate_proposal(q.operation(0).unwrap(), &bytes, std::iter::empty()),
        Err(ApplicationError::DedupCapacity)
    );
}
#[test]
fn internal_fact_shape_and_original_retry_content_are_exact() {
    let p = program(1, 0);
    let mut f = Fixture::new(&p);
    f.start(&p);
    let facts = f.prepared(&p);
    let mut missing = command(
        &p,
        ProgramStep::Decide,
        Some(Decision::Commit),
        facts[..2].to_vec(),
    );
    assert!(codec::encode(&missing).is_err());
    missing.facts = facts.clone().into_boxed_slice();
    missing.facts[0].group = group(99);
    assert!(codec::encode(&missing).is_err());
    let c = command(
        &p,
        ProgramStep::Decide,
        Some(Decision::Commit),
        facts.clone(),
    );
    apply(&mut f.c, c.clone());
    let original = f.c.status(p.id()).unwrap();
    let mut changed = c.clone();
    changed.facts[0].index += 1;
    let r = apply(&mut f.c, changed);
    assert_eq!(
        r.outcome,
        ProgramOutcome::Conflict(ProgramConflict::Operation)
    );
    assert!(r.duplicate);
    assert_eq!(r.first_index, 2);
    let r = apply(&mut f.c, c);
    assert_eq!(r.outcome, ProgramOutcome::Decided(Decision::Commit));
    assert_eq!(r.first_index, 2);
    assert!(r.duplicate);
    assert_eq!(f.c.status(p.id()), Some(original));
}
