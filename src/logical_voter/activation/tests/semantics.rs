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
fn one_replicated_slot_retains_busy_terminal_and_first_positions() {
    let mut a = app(3, 2);
    let i = intent(10);
    let competing = intent(20);
    let business = a.business(&5i64.to_le_bytes()).unwrap();
    let r = a.apply_batch(&[entry(1, op(100), business)]).unwrap();
    assert!(
        matches!(&r[0], ActivationReceipt::Business(CounterReceipt { outcome: CounterOutcome::Value(5), operation, .. }) if *operation == op(100))
    );
    let first = begin(&mut a, &i);
    assert_eq!(first.first, ActivationPosition { index: 2, term: 4 });
    let busy = begin(&mut a, &competing);
    assert_eq!(busy.outcome, ChildActivationOutcome::Busy(op(10)));
    assert_eq!(a.active(), Some(op(10)));
    assert_eq!(a.inner.read_at(3, ()).unwrap(), 5);
    finish(&mut a, &i);
    assert_eq!(a.active(), None);
    let retry = begin(&mut a, &i);
    assert!(retry.duplicate);
    assert_eq!(retry.first, first.first);
    assert_eq!(retry.outcome, ChildActivationOutcome::Begun);
    let late_busy = begin(&mut a, &competing);
    assert!(late_busy.duplicate);
    assert_eq!(late_busy.first, busy.first);
    assert_eq!(late_busy.outcome, busy.outcome);
    assert_eq!(a.active(), None);
    begin(&mut a, &intent(30));
    assert_eq!(a.active(), Some(op(30)));
    assert_eq!(a.inner.applied_index(), a.applied_index());
}
#[test]
fn reserved_terminal_capacity_survives_full_business_and_intent_lifetimes() {
    let mut a = app(1, 1);
    let i = intent(10);
    let bytes = a.business(&7i64.to_le_bytes()).unwrap();
    a.apply_batch(&[entry(1, op(100), bytes)]).unwrap();
    begin(&mut a, &i);
    let fresh = a.begin(&intent(20)).unwrap();
    assert_eq!(
        a.validate_proposal(op(20), &fresh, std::iter::empty()),
        Err(ApplicationError::DedupCapacity)
    );
    let outcome = codec::command(&final_command(&i), CONTROL_BYTES).unwrap();
    assert!(a
        .validate_proposal(i.operations.outcome, &outcome, std::iter::empty())
        .is_ok());
    let r = apply(&mut a, final_command(&i));
    let release = ControlCommand::Release {
        begin: i.operations.begin,
        outcome: r.first,
    };
    let bytes = codec::command(&release, CONTROL_BYTES).unwrap();
    assert!(a
        .validate_proposal(i.operations.release, &bytes, std::iter::empty())
        .is_ok());
    apply(&mut a, release);
    assert_eq!(a.active(), None);
    assert_eq!(a.control.records.len(), 1);
}
#[test]
fn malformed_late_and_unprepared_phases_do_not_partially_apply() {
    let mut a = app(2, 2);
    let i = intent(10);
    begin(&mut a, &i);
    let absent = ControlCommand::Release {
        begin: op(10),
        outcome: ActivationPosition { index: 1, term: 4 },
    };
    let checkpoint = a.checkpoint(usize::MAX).unwrap();
    let business = a.business(&9i64.to_le_bytes()).unwrap();
    assert!(a
        .apply_batch(&[
            entry(2, op(100), business),
            entry(3, op(14), codec::command(&absent, CONTROL_BYTES).unwrap())
        ])
        .is_err());
    assert_eq!(checkpoint, a.checkpoint(usize::MAX).unwrap());
    let mut wrong = final_command(&i);
    if let ControlCommand::Outcome { fact, .. } = &mut wrong {
        fact.configuration = ConfigurationId::new(5).unwrap();
    }
    assert!(a
        .validate_proposal(
            op(13),
            &codec::command(&wrong, CONTROL_BYTES).unwrap(),
            std::iter::empty()
        )
        .is_err());
    let busy = intent(20);
    begin(&mut a, &busy);
    assert!(a
        .validate_proposal(
            op(23),
            &codec::command(&final_command(&busy), CONTROL_BYTES).unwrap(),
            std::iter::empty()
        )
        .is_err());
    let outcome = apply(&mut a, final_command(&i));
    assert!(a
        .validate_proposal(
            op(14),
            &codec::command(&absent, CONTROL_BYTES).unwrap(),
            std::iter::empty()
        )
        .is_err());
    apply(
        &mut a,
        ControlCommand::Release {
            begin: op(10),
            outcome: outcome.first,
        },
    );
    let conflict = apply(&mut a, wrong);
    assert_eq!(conflict.outcome, ChildActivationOutcome::OperationConflict);
    assert_eq!(conflict.first, outcome.first);
    assert_eq!(
        a.status(op(10)).unwrap().outcome.unwrap().outcome,
        ChildActivationOutcome::Completed
    );
}
#[test]
fn all_pending_and_retained_phase_aliases_refuse() {
    let mut a = app(2, 2);
    let i = intent(10);
    let begin_bytes = a.begin(&i).unwrap();
    let business = a.business(&1i64.to_le_bytes()).unwrap();
    for id in i.operations.ids() {
        assert_eq!(
            a.validate_proposal(
                id,
                &business,
                std::iter::once((op(10), begin_bytes.as_slice()))
            ),
            Err(ApplicationError::OperationConflict)
        );
        assert_eq!(
            a.validate_proposal(
                op(10),
                &begin_bytes,
                std::iter::once((id, business.as_slice()))
            ),
            Err(ApplicationError::OperationConflict)
        );
    }
    let colliding = intent(14);
    let other = a.begin(&colliding).unwrap();
    assert_eq!(
        a.validate_proposal(
            op(14),
            &other,
            std::iter::once((op(10), begin_bytes.as_slice()))
        ),
        Err(ApplicationError::OperationConflict)
    );
    a.apply_batch(&[entry(1, op(12), business)]).unwrap();
    assert_eq!(
        a.validate_proposal(op(10), &begin_bytes, std::iter::empty()),
        Err(ApplicationError::OperationConflict)
    );
}
#[test]
fn malformed_configuration_or_cross_incarnation_cannot_begin() {
    let mut i = intent(10);
    i.target[2] = ActivationVoter {
        node: NodeId::new(5).unwrap(),
        store: i.old[0].store,
    };
    assert!(i.validate().is_err());
    i = intent(10);
    i.target[0].store.incarnation = StoreIncarnation::new(2).unwrap();
    assert!(i.validate().is_err());
    i = intent(10);
    i.operations.release = i.operations.outcome;
    assert!(i.validate().is_err());
    i = intent(10);
    i.mapping.parent.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(app(1, 1).begin(&i).is_err());
    assert!(ChildActivationIntent::new(
        mapping(),
        intent(10).operations,
        &config([1, 2, 3], 1),
        ConfigurationId::new(2).unwrap(),
        ConfigurationId::new(3).unwrap(),
        &config([1, 4, 5], 4)
    )
    .is_err());
}
