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
use super::{oracle, protocol::*, search};
fn deliver(state: &mut State, message: Message) {
    *state = state.advance(Step::Send(message), Weakening::None);
    oracle::check(state).unwrap();
    *state = state.advance(Step::Deliver(message.transaction()), Weakening::None);
    oracle::check(state).unwrap();
}
fn prepare(state: &mut State, t: usize) {
    for p in 0..2 {
        deliver(state, Message::Prepare(t, p));
        deliver(state, Message::Prepared(t, p));
    }
}
fn finish(state: &mut State, t: usize, d: Decision) {
    deliver(state, Message::Decide(t, d));
    for p in 0..2 {
        deliver(state, Message::Apply(t, p, d));
        deliver(state, Message::Applied(t, p, d));
    }
    deliver(state, Message::All(t, d));
    for p in 0..2 {
        deliver(state, Message::Release(t, p, d));
        deliver(state, Message::Released(t, p, d));
    }
    deliver(state, Message::Finish(t, d));
}
#[test]
fn real_phase_sequence_finishes_both_transfers_and_original_retries_do_not_reapply() {
    let mut state = State::new(20);
    for t in 0..2 {
        prepare(&mut state, t);
        finish(&mut state, t, Decision::Commit);
        assert_eq!(
            state.coordinator[t].phase,
            CoordinatorPhase::Finished(Decision::Commit)
        );
        let original = state.balances;
        deliver(&mut state, Message::Apply(t, 0, Decision::Commit));
        assert_eq!(state.balances, original);
    }
    assert_eq!(state.balances, [8, 12]);
}
#[test]
fn conflicting_and_insufficient_prepares_record_abort_without_partial_effects() {
    let mut state = State::new(20);
    deliver(&mut state, Message::Prepare(0, 0));
    deliver(&mut state, Message::Prepare(1, 0));
    assert!(state.participant[1][0].refused);
    deliver(&mut state, Message::Refused(1, 0));
    finish(&mut state, 1, Decision::Abort);
    assert_eq!(state.locks, [Some(0), None]);
    deliver(&mut state, Message::Prepared(0, 0));
    deliver(&mut state, Message::Prepare(0, 1));
    deliver(&mut state, Message::Prepared(0, 1));
    finish(&mut state, 0, Decision::Commit);
    assert_eq!(state.balances, [15, 5]);
    assert_eq!(
        state.coordinator.map(|c| c.phase),
        [
            CoordinatorPhase::Finished(Decision::Commit),
            CoordinatorPhase::Finished(Decision::Abort)
        ]
    );
    let mut insufficient = State::new(4);
    deliver(&mut insufficient, Message::Prepare(0, 0));
    deliver(&mut insufficient, Message::Refused(0, 0));
    finish(&mut insufficient, 0, Decision::Abort);
    assert_eq!(insufficient.balances, [4, 0]);
}
#[test]
fn receipt_loss_restart_and_timeouts_keep_prepared_reads_blocked_until_exact_release() {
    let mut state = State::new(20);
    deliver(&mut state, Message::Prepare(0, 0));
    state = state.advance(Step::Send(Message::Prepared(0, 0)), Weakening::None);
    let durable = (
        state.coordinator,
        state.participant,
        state.balances,
        state.locks,
    );
    state = state.advance(Step::Restart, Weakening::None);
    assert_eq!(
        (
            state.coordinator,
            state.participant,
            state.balances,
            state.locks
        ),
        durable
    );
    assert_eq!(state.wire, [None; 2]);
    state = state.advance(Step::Timeout(0, 0), Weakening::None);
    assert_eq!(state.read(0), None);
    assert_eq!(state.coordinator[0].phase, CoordinatorPhase::Started);
    deliver(&mut state, Message::Prepared(0, 0));
    deliver(&mut state, Message::Prepare(0, 1));
    deliver(&mut state, Message::Prepared(0, 1));
    assert_eq!(state.observed[0].prepared, 3);
    state = state.advance(Step::Restart, Weakening::None);
    assert_eq!(state.observed, [Observations::default(); 2]);
    assert_eq!(state.coordinator[0].phase, CoordinatorPhase::Started);
    assert_eq!(state.locks, [Some(0); 2]);
    for p in 0..2 {
        deliver(&mut state, Message::Prepared(0, p));
    }
    deliver(&mut state, Message::Decide(0, Decision::Commit));
    for p in 0..2 {
        deliver(&mut state, Message::Apply(0, p, Decision::Commit));
        deliver(&mut state, Message::Applied(0, p, Decision::Commit));
    }
    assert_eq!([state.read(0), state.read(1)], [None; 2]);
    assert!(!state
        .steps(Weakening::None)
        .contains(&Step::Send(Message::Release(0, 0, Decision::Commit))));
    deliver(&mut state, Message::All(0, Decision::Commit));
    deliver(&mut state, Message::Release(0, 0, Decision::Commit));
    assert_eq!([state.read(0), state.read(1)], [Some(15), None]);
}
#[test]
fn lost_application_and_release_receipts_reconstruct_from_durable_outcomes() {
    let mut state = State::new(20);
    prepare(&mut state, 0);
    deliver(&mut state, Message::Decide(0, Decision::Commit));
    for p in 0..2 {
        deliver(&mut state, Message::Apply(0, p, Decision::Commit));
        state = state.advance(
            Step::Send(Message::Applied(0, p, Decision::Commit)),
            Weakening::None,
        );
        state = state.advance(Step::Lose(0), Weakening::None);
        oracle::check(&state).unwrap();
    }
    assert_eq!(state.coordinator[0].applied, 0);
    assert_eq!([state.read(0), state.read(1)], [None; 2]);
    deliver(&mut state, Message::Applied(0, 0, Decision::Commit));
    state = state.advance(Step::Restart, Weakening::None);
    assert_eq!(state.observed[0].applied, 0);
    assert_eq!(state.coordinator[0].prepared, 3);
    assert_eq!(
        state.coordinator[0].phase,
        CoordinatorPhase::Decided(Decision::Commit)
    );
    for p in 0..2 {
        deliver(&mut state, Message::Applied(0, p, Decision::Commit));
    }
    deliver(&mut state, Message::All(0, Decision::Commit));
    for p in 0..2 {
        deliver(&mut state, Message::Release(0, p, Decision::Commit));
        state = state.advance(
            Step::Send(Message::Released(0, p, Decision::Commit)),
            Weakening::None,
        );
        state = state.advance(Step::Restart, Weakening::None);
        oracle::check(&state).unwrap();
    }
    assert_eq!(state.coordinator[0].released, 0);
    assert_eq!([state.read(0), state.read(1)], [Some(15), Some(5)]);
    deliver(&mut state, Message::Released(0, 0, Decision::Commit));
    state = state.advance(Step::Restart, Weakening::None);
    assert_eq!(state.observed[0].released, 0);
    assert_eq!(state.coordinator[0].applied, 3);
    assert_eq!(
        state.coordinator[0].phase,
        CoordinatorPhase::AppliedAll(Decision::Commit)
    );
    for p in 0..2 {
        deliver(&mut state, Message::Released(0, p, Decision::Commit));
    }
    deliver(&mut state, Message::Finish(0, Decision::Commit));
    assert_eq!(
        state.coordinator[0].phase,
        CoordinatorPhase::Finished(Decision::Commit)
    );
}
fn broken(prefix: State, weak: Weakening, expected: &str) {
    oracle::check(&prefix).unwrap();
    let (history, violation) = search::counterexample(prefix, weak);
    println!("{weak:?}: {history:?}: {violation}");
    assert!(!history.is_empty());
    assert_eq!(violation, expected);
}
#[test]
fn checker_detects_timeout_abort_of_a_durable_prepared_participant() {
    let mut state = State::new(20);
    deliver(&mut state, Message::Prepare(0, 0));
    broken(
        state,
        Weakening::TimeoutAbort,
        "participant outcome has no matching coordinator decision",
    );
}
#[test]
fn checker_detects_release_before_exact_applied_all() {
    let mut state = State::new(20);
    prepare(&mut state, 0);
    deliver(&mut state, Message::Decide(0, Decision::Commit));
    deliver(&mut state, Message::Apply(0, 0, Decision::Commit));
    broken(
        state,
        Weakening::PrematureRelease,
        "participant released before exact AppliedAll",
    );
}
#[test]
fn checker_detects_reversal_of_the_original_replicated_decision() {
    let mut state = State::new(20);
    prepare(&mut state, 0);
    deliver(&mut state, Message::Decide(0, Decision::Commit));
    broken(
        state,
        Weakening::UnstableDecision,
        "durable coordinator decision changed",
    );
}
