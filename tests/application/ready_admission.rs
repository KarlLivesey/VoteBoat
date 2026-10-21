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
//! Ready-batch admission equals the full validator and never reads pending.
use super::*;
use std::cell::Cell;

/// Candidate bytes: valid acknowledgements in both directions, malformed
/// lengths and an acknowledgement that is not below the sequence.
fn candidates() -> Vec<(OperationId, Vec<u8>)> {
    let mut all = Vec::new();
    for session in 1..=3 {
        for sequence in 1..=6 {
            for ack in [0, 1, sequence - 1, sequence] {
                all.push((op(session, sequence), Counter::command(ack, 1)));
            }
            all.push((op(session, sequence), vec![0; 15]));
        }
    }
    for nonce in [1, 2] {
        all.push((OperationId::new(nonce).unwrap(), Vec::new()));
        all.push((OperationId::new(nonce).unwrap(), vec![0; 16]));
    }
    all
}

/// Small states: registrations, executed sequences and acknowledgements.
fn states() -> Vec<Counter> {
    let mut states = Vec::new();
    for window in 1..=3 {
        let limits = SessionLimits::new(2, window, 1).unwrap();
        for history in 0..6u64 {
            let mut counter = Counter::with_limits(limits).unwrap();
            let mut entries = vec![register(1, 1)];
            for sequence in 1..=history {
                entries.push(add(
                    sequence + 1,
                    1 + sequence % 2,
                    sequence,
                    sequence / 3,
                    2,
                ));
            }
            counter.apply_batch(&entries).unwrap();
            states.push(counter);
        }
    }
    states
}

#[test]
fn prepared_admission_equals_the_validator_for_every_small_state() {
    let mut comparisons = 0;
    for counter in states() {
        let before = counter.checkpoint(1 << 16).unwrap();
        let mut prepared = counter.prepare_ready_admission(std::iter::empty()).unwrap();
        for (operation, bytes) in candidates() {
            let expected = counter.validate_proposal(operation, &bytes, std::iter::empty());
            assert_eq!(prepared(operation, &bytes), expected);
            assert_eq!(
                counter.validate_proposal_bounded(operation, &bytes, usize::MAX),
                Some(expected)
            );
            comparisons += 1;
        }
        drop(prepared);
        assert_eq!(counter.checkpoint(1 << 16).unwrap(), before);
    }
    assert_eq!(comparisons, 18 * 94);
}

#[test]
fn admission_never_reads_pending_commands() {
    let counter = Counter::new(2).unwrap();
    let observed = Cell::new(0);
    let pending = || {
        (1..=1000).map(|sequence| {
            observed.set(observed.get() + 1);
            (op(1, sequence), &[][..])
        })
    };
    let mut prepared = counter.prepare_ready_admission(pending()).unwrap();
    let bound = std::mem::size_of::<CounterReceipt>();
    // Every candidate is decided by the window over applied state alone.
    assert_eq!(prepared(op(1, 1), &Counter::command(0, 1)), Ok(bound));
    assert_eq!(prepared(op(1, 2), &Counter::command(0, 1)), Ok(bound));
    assert_eq!(
        prepared(op(1, 3), &Counter::command(0, 1)),
        Err(ApplicationError::SessionWindow)
    );
    assert_eq!(prepared(op(1, 3), &Counter::command(1, 1)), Ok(bound));
    drop(prepared);
    assert_eq!(
        counter.validate_proposal(op(1, 3), &Counter::command(0, 1), pending()),
        Err(ApplicationError::SessionWindow)
    );
    assert_eq!(
        counter.validate_proposal_bounded(op(1, 2), &Counter::command(0, 1), 1_000_000),
        Some(Ok(bound))
    );
    assert_eq!(observed.get(), 0);
}

#[test]
fn applied_conflicts_overflow_and_expired_ids_admit_for_terminal_truth() {
    let mut counter = Counter::new(2).unwrap();
    counter
        .apply_batch(&[command(1, 1, i64::MAX), command(2, 2, 1)])
        .unwrap();
    let bound = std::mem::size_of::<CounterReceipt>();
    // Retained duplicates and conflicts admit; apply returns the original.
    for (sequence, delta) in [(1, 0), (2, 1)] {
        assert_eq!(
            counter.validate_proposal(
                op(1, sequence),
                &Counter::command(0, delta),
                std::iter::empty()
            ),
            Ok(bound)
        );
    }
    let receipts = counter
        .apply_batch(&[command(3, 1, 0), command(4, 2, 1)])
        .unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::OperationConflict);
    assert_eq!(receipts[1].outcome, CounterOutcome::Overflow);
    assert!(receipts.iter().all(|receipt| receipt.duplicate));
    // Acknowledge both, then a late copy of sequence 1 admits and reads Expired.
    counter.apply_batch(&[add(5, 1, 3, 2, 0)]).unwrap();
    assert_eq!(
        counter.validate_proposal(op(1, 1), &Counter::command(0, i64::MAX), std::iter::empty()),
        Ok(bound)
    );
    let late = counter.apply_batch(&[command(6, 1, i64::MAX)]).unwrap();
    assert_eq!(
        late[0].outcome,
        CounterOutcome::Refused(SessionRefusal::Expired)
    );
    assert_eq!(counter.read_applied(6), Ok(i64::MAX));
    assert_eq!(counter.retained_receipts(), 1);
}
