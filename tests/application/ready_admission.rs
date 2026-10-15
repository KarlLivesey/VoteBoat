// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Differential controls against the unchanged per-command Counter validator.
use super::*;
use std::cell::Cell;

fn operation(id: u128) -> OperationId {
    OperationId::new(id).unwrap()
}

fn differential(
    counter: &Counter,
    pending: &[(u128, Vec<u8>)],
    candidates: &[(u128, Vec<u8>)],
) -> Vec<Result<usize, ApplicationError>> {
    let before = counter.checkpoint(4096).unwrap();
    let pending_items = || {
        pending
            .iter()
            .map(|(id, bytes)| (operation(*id), bytes.as_slice()))
    };
    let mut prepared = counter.prepare_ready_admission(pending_items()).unwrap();
    let mut successful = Vec::new();
    let mut results = Vec::new();
    for (id, bytes) in candidates {
        let original = counter.validate_proposal(
            operation(*id),
            bytes,
            pending_items().chain(
                successful
                    .iter()
                    .map(|(id, bytes): &(u128, &[u8])| (operation(*id), *bytes)),
            ),
        );
        let result = prepared(operation(*id), bytes);
        assert_eq!(result, original);
        results.push(result);
        if result.is_err() {
            break;
        }
        successful.push((*id, bytes.as_slice()));
    }
    drop(prepared);
    assert_eq!(counter.checkpoint(4096).unwrap(), before);
    results
}

#[test]
fn exact_ready_reservations_match_growing_prefix_oracle_across_small_states() {
    let choices = [
        (1, 1i64.to_le_bytes().to_vec()),
        (1, 2i64.to_le_bytes().to_vec()),
        (2, (-1i64).to_le_bytes().to_vec()),
        (4, i64::MAX.to_le_bytes().to_vec()),
        (3, vec![0; 7]),
    ];
    let mut comparisons = 0;
    for capacity in 1..=4 {
        for applied in 0..=capacity {
            let mut counter = Counter::new(capacity).unwrap();
            let entries: Vec<_> = (1..=applied)
                .map(|id| command(id as u64, id as u128, id as i64))
                .collect();
            counter.apply_batch(&entries).unwrap();
            for pending_len in 0..=3 {
                for mut pending_code in 0..4usize.pow(pending_len) {
                    let pending: Vec<_> = (0..pending_len)
                        .map(|_| {
                            let id = (pending_code % 4 + 1) as u128;
                            pending_code /= 4;
                            // Pending syntax is not validated by this seam.
                            (id, vec![id as u8; 3])
                        })
                        .collect();
                    for mut candidate_code in 0..choices.len().pow(3) {
                        let candidates: Vec<_> = (0..3)
                            .map(|_| {
                                let candidate = choices[candidate_code % choices.len()].clone();
                                candidate_code /= choices.len();
                                candidate
                            })
                            .collect();
                        differential(&counter, &pending, &candidates);
                        comparisons += 1;
                    }
                }
            }
        }
    }
    assert_eq!(comparisons, 148_750);
}

#[test]
fn pending_overflow_is_deferred_behind_candidate_syntax_and_stops_consumption() {
    let counter = Counter::new(1).unwrap();
    let observed = Cell::new(0);
    let pending = [1, 1, 2, 3].into_iter().map(|id| {
        observed.set(observed.get() + 1);
        (operation(id), &[][..])
    });
    let mut prepared = counter.prepare_ready_admission(pending).unwrap();
    assert_eq!(observed.get(), 3);
    assert_eq!(
        prepared(operation(1), &[0; 7]),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(observed.get(), 3);
    drop(prepared);
    let overflow = [(1, vec![]), (1, vec![7]), (2, vec![]), (3, vec![])];
    assert_eq!(
        differential(&counter, &overflow, &[(1, vec![0; 8])]),
        vec![Err(ApplicationError::DedupCapacity)]
    );
    assert_eq!(
        differential(&counter, &overflow, &[(1, vec![0; 9])]),
        vec![Err(ApplicationError::InvalidCommand)]
    );
}

#[test]
fn context_owns_pending_ids_without_borrowing_temporary_payloads() {
    let counter = Counter::new(2).unwrap();
    let mut prepared = {
        let temporary = [(operation(1), vec![3; 128])];
        counter
            .prepare_ready_admission(temporary.iter().map(|(id, bytes)| (*id, bytes.as_slice())))
            .unwrap()
    };
    let bound = std::mem::size_of::<CounterReceipt>();
    assert_eq!(prepared(operation(1), &[0; 8]), Ok(bound));
    assert_eq!(prepared(operation(2), &[0; 8]), Ok(bound));
    assert_eq!(
        prepared(operation(3), &[0; 8]),
        Err(ApplicationError::DedupCapacity)
    );
}

#[test]
fn applied_conflicts_and_overflow_stay_original_application_outcomes() {
    let mut counter = Counter::new(2).unwrap();
    counter
        .apply_batch(&[command(1, 1, i64::MAX), command(2, 2, 1)])
        .unwrap();
    let bound = std::mem::size_of::<CounterReceipt>();
    assert_eq!(
        differential(
            &counter,
            &[(1, vec![0; 8]), (2, vec![1; 8])],
            &[
                (1, 0i64.to_le_bytes().to_vec()),
                (2, 1i64.to_le_bytes().to_vec()),
            ],
        ),
        vec![Ok(bound), Ok(bound)]
    );
    let receipts = counter
        .apply_batch(&[command(3, 1, 0), command(4, 2, 1)])
        .unwrap();
    assert_eq!(receipts[0].outcome, CounterOutcome::OperationConflict);
    assert_eq!(receipts[1].outcome, CounterOutcome::Overflow);
    assert!(receipts.iter().all(|receipt| receipt.duplicate));
    assert_eq!(counter.remaining_operations(), 0);
}

#[test]
fn dropping_and_repreparing_after_replay_and_restore_has_no_reservation_history() {
    let mut counter = Counter::new(3).unwrap();
    differential(
        &counter,
        &[(1, vec![0; 8])],
        &[(2, vec![0; 8]), (2, vec![1; 8])],
    );
    assert_eq!(counter.remaining_operations(), 3);
    counter.apply_batch(&[command(1, 1, 4)]).unwrap();
    let image = counter.checkpoint(4096).unwrap();
    let mut restored = Counter::new(3).unwrap();
    restored.restore_checkpoint(1, 1, &image).unwrap();
    let pending = [(1, vec![9; 8]), (2, vec![0; 8])];
    let candidates = [(1, vec![7; 8]), (3, vec![1; 8]), (4, vec![0; 8])];
    assert_eq!(
        differential(&counter, &pending, &candidates),
        differential(&restored, &pending, &candidates)
    );
    restored.apply_batch(&[command(2, 2, 5)]).unwrap();
    assert_eq!(restored.remaining_operations(), 1);
    differential(&restored, &[], &[(3, vec![0; 8]), (4, vec![0; 8])]);
}
