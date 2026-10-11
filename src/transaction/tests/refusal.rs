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
fn refusal_abort_absent_tombstone_and_late_prepare_preserve_other_lock() {
    let mut f = Fixture::new(2);
    let first = spec(1);
    let second = spec(2);
    f.start(first);
    f.start(second);
    apply(
        &mut f.p[0],
        command(first, TransferStep::Prepare, None, [None; 2]),
    );
    let refused = apply(
        &mut f.p[0],
        command(second, TransferStep::Prepare, None, [None; 2]),
    );
    let reason = PrepareRefusal::Locked(first.id());
    assert_eq!(
        refused.outcome,
        TransferOutcome::Prepare(PrepareOutcome::Refused(reason))
    );
    let mut decide = command(
        second,
        TransferStep::Decide,
        Some(TransferDecision::Abort),
        [Some(fact(&f.p[0], refused)), None],
    );
    decide.refusal = Some((0, reason));
    let decided = apply(&mut f.c, decide);
    f.finish(second, TransferDecision::Abort, fact(&f.c, decided));
    assert_eq!(f.p[0].account(aid(1)), AccountRead::Locked(first.id()));
    let late = apply(
        &mut f.p[1],
        command(second, TransferStep::Prepare, None, [None; 2]),
    );
    assert_eq!(
        late.outcome,
        TransferOutcome::Prepare(PrepareOutcome::Closed(TransferDecision::Abort))
    );
    assert_eq!(f.p[1].account(aid(2)), AccountRead::Balance(0));
    assert_eq!(
        f.p[1].status(second.id()).unwrap().phase,
        TransferPhase::Released(TransferDecision::Abort)
    );
    let prepared = apply(
        &mut f.p[0],
        command(first, TransferStep::Prepare, None, [None; 2]),
    );
    assert!(prepared.duplicate);
    f.commit(first);
    assert_eq!(f.p[0].account(aid(1)), AccountRead::Balance(15));
}
#[test]
fn insufficient_and_overflow_are_committed_business_outcomes_without_mutation() {
    let s = spec(1);
    let limits = TransferLimits::new(1, 1).unwrap();
    for (group, account, value, refusal) in [
        (gid(20), aid(1), 4, PrepareRefusal::InsufficientFunds),
        (gid(30), aid(2), i64::MAX, PrepareRefusal::CreditOverflow),
    ] {
        let mut app =
            TransferApplication::participant(group, s.profile(), &[(account, value)], limits)
                .unwrap();
        let r = apply(&mut app, command(s, TransferStep::Prepare, None, [None; 2]));
        assert_eq!(
            r.outcome,
            TransferOutcome::Prepare(PrepareOutcome::Refused(refusal))
        );
        assert_eq!(app.account(account), AccountRead::Balance(value));
        assert_eq!(app.remaining_transactions(), 0);
        assert_eq!(
            apply(&mut app, command(s, TransferStep::Prepare, None, [None; 2])).outcome,
            r.outcome
        );
    }
}
#[test]
fn changed_phase_assertion_or_transaction_content_never_reapplies() {
    let mut f = Fixture::new(2);
    let s = spec(1);
    f.start(s);
    f.commit(s);
    let before = f.p[0].status(s.id()).unwrap();
    let mut changed = f.p[0].records[&s.id()].phases[1].unwrap().command;
    changed.facts[0].as_mut().unwrap().index += 1;
    assert_eq!(
        f.p[0].validate_proposal(
            changed.operation(gid(20)).unwrap(),
            &codec::encode(changed).unwrap(),
            std::iter::empty()
        ),
        Err(ApplicationError::OperationConflict)
    );
    let r = apply(&mut f.p[0], changed);
    assert_eq!(
        r.outcome,
        TransferOutcome::Conflict(TransferConflict::Operation)
    );
    assert!(r.duplicate);
    assert_eq!(f.p[0].status(s.id()), Some(before));
    assert_eq!(f.p[0].account(aid(1)), AccountRead::Balance(15));
    let changed =
        TransferSpec::new(s.id(), s.profile(), s.debit(), s.credit(), 6, s.phases()).unwrap();
    assert_eq!(
        apply(&mut f.c, Command::start(changed)).outcome,
        TransferOutcome::Conflict(TransferConflict::Transaction)
    );
}
