// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent legacy Counter admission oracle and iterator-observability controls.
use super::*;
use std::{
    cell::Cell,
    collections::BTreeSet,
    panic::{catch_unwind, AssertUnwindSafe},
};

const VALID: [u8; 8] = 7i64.to_le_bytes();

fn operation(id: u128) -> OperationId {
    OperationId::new(id).unwrap()
}

// Original480 algorithm, with committed membership supplied by public apply.
// Keep this independent of the optimized validator and prepared admission.
fn original<'a>(
    applied: &BTreeSet<OperationId>,
    remaining: usize,
    candidate: OperationId,
    bytes: &[u8],
    pending: impl Iterator<Item = (OperationId, &'a [u8])>,
) -> Result<usize, ApplicationError> {
    if bytes.len() != 8 {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut reserved = BTreeSet::new();
    for (id, _) in pending {
        if !applied.contains(&id) {
            reserved.insert(id);
        }
        if reserved.len() > remaining {
            return Err(ApplicationError::DedupCapacity);
        }
    }
    if !applied.contains(&candidate) {
        reserved.insert(candidate);
    }
    if reserved.len() > remaining {
        return Err(ApplicationError::DedupCapacity);
    }
    Ok(std::mem::size_of::<CounterReceipt>())
}

fn populated(capacity: usize, ids: &[u128]) -> (Counter, BTreeSet<OperationId>) {
    let mut counter = Counter::new(capacity).unwrap();
    let entries: Vec<_> = ids
        .iter()
        .enumerate()
        .map(|(index, id)| command(index as u64 + 1, *id, *id as i64))
        .collect();
    counter.apply_batch(&entries).unwrap();
    (counter, ids.iter().copied().map(operation).collect())
}

fn compare_state(counter: &Counter, applied: &BTreeSet<OperationId>) -> usize {
    let mut comparisons = 0;
    for len in 0..=4 {
        for mut code in 0..5usize.pow(len) {
            let pending: Vec<_> = (0..len)
                .map(|_| {
                    let id = code % 5 + 1;
                    code /= 5;
                    // The original ignores pending syntax, including malformed bytes.
                    (operation(id as u128), vec![id as u8; id])
                })
                .collect();
            for id in 1..=6 {
                for bytes in [&[][..], &[0; 7][..], &VALID[..], &[0; 9][..]] {
                    let items = || pending.iter().map(|(id, bytes)| (*id, bytes.as_slice()));
                    assert_eq!(
                        counter.validate_proposal(operation(id), bytes, items()),
                        original(
                            applied,
                            counter.remaining_operations(),
                            operation(id),
                            bytes,
                            items()
                        ),
                        "applied={applied:?}, pending={pending:?}, candidate={id}, bytes={bytes:?}"
                    );
                    comparisons += 1;
                }
            }
        }
    }
    comparisons
}

#[test]
fn bounded_scan_matches_original_across_exhaustive_small_states_without_mutation() {
    let mut comparisons = 0;
    for capacity in 1..=4 {
        for mask in 0u8..16 {
            let ids: Vec<_> = (1..=4)
                .filter(|id| mask & (1 << (id - 1)) != 0)
                .map(|id| id as u128)
                .collect();
            if ids.len() > capacity {
                continue;
            }
            let (counter, applied) = populated(capacity, &ids);
            let before = counter.checkpoint(4096).unwrap();
            comparisons += compare_state(&counter, &applied);
            assert_eq!(counter.checkpoint(4096).unwrap(), before);
            assert_eq!(counter.applied_index(), ids.len() as u64);
            assert_eq!(counter.remaining_operations(), capacity - ids.len());
        }
    }
    assert_eq!(comparisons, 880_968);
}

#[test]
fn larger_spare_exact_and_exhausted_boundaries_match_original_and_checkpoint() {
    for capacity in [17, 64, 65, 1024] {
        for applied_len in [0, capacity - 2, capacity] {
            let ids: Vec<_> = (1..=applied_len as u128).collect();
            let (counter, applied) = populated(capacity, &ids);
            let before = counter.checkpoint(100_000).unwrap();
            let remaining = counter.remaining_operations();
            for len in [remaining.saturating_sub(1), remaining, remaining + 1] {
                let novel: Vec<_> = (1..=len as u128)
                    .map(|id| (operation(10_000 + id), &[][..]))
                    .collect();
                let mixed: Vec<_> = novel
                    .iter()
                    .copied()
                    .flat_map(|item| [item, item])
                    .collect();
                for pending in [&novel, &mixed] {
                    for candidate in [1, 10_001, 100_000] {
                        let candidate = operation(candidate);
                        assert_eq!(
                            counter.validate_proposal(candidate, &VALID, pending.iter().copied()),
                            original(
                                &applied,
                                remaining,
                                candidate,
                                &VALID,
                                pending.iter().copied()
                            )
                        );
                    }
                }
            }
            assert_eq!(counter.checkpoint(100_000).unwrap(), before);
        }
    }
}

struct Hostile<'a> {
    ids: &'a [u128],
    position: usize,
    calls: &'a Cell<usize>,
    panic_at: Option<usize>,
    dropped: &'a Cell<bool>,
}

impl Iterator for Hostile<'_> {
    type Item = (OperationId, &'static [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.calls.set(self.calls.get() + 1);
        assert_ne!(self.panic_at, Some(self.position), "injected next panic");
        let id = self.ids.get(self.position).copied()?;
        self.position += 1;
        Some((operation(id), &VALID))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        panic!("size_hint must not be consulted")
    }

    fn count(self) -> usize {
        panic!("count must not replace direct next")
    }

    fn nth(&mut self, _: usize) -> Option<Self::Item> {
        panic!("nth must not replace direct next")
    }

    fn last(self) -> Option<Self::Item> {
        panic!("last must not replace direct next")
    }

    fn collect<B: FromIterator<Self::Item>>(self) -> B {
        panic!("collect must not replace direct next")
    }

    fn fold<B, F>(self, _: B, _: F) -> B
    where
        F: FnMut(B, Self::Item) -> B,
    {
        panic!("fold must not replace direct next")
    }
}

impl Drop for Hostile<'_> {
    fn drop(&mut self) {
        self.dropped.set(true);
    }
}

fn hostile<'a>(
    ids: &'a [u128],
    calls: &'a Cell<usize>,
    dropped: &'a Cell<bool>,
    panic_at: Option<usize>,
) -> Hostile<'a> {
    Hostile {
        ids,
        position: 0,
        calls,
        panic_at,
        dropped,
    }
}

#[test]
fn hostile_iterator_uses_only_next_and_preserves_exact_boundary_consumption() {
    let bound = std::mem::size_of::<CounterReceipt>();
    let cases = [
        (3, &[][..], &[1, 2][..], 3, Ok(bound)),
        (2, &[][..], &[1, 2][..], 3, Ok(bound)),
        (
            2,
            &[][..],
            &[1, 1, 2, 3, 4][..],
            4,
            Err(ApplicationError::DedupCapacity),
        ),
        (1, &[1][..], &[][..], 1, Ok(bound)),
        (1, &[1][..], &[1, 1][..], 3, Ok(bound)),
        (
            1,
            &[1][..],
            &[2, 3][..],
            1,
            Err(ApplicationError::DedupCapacity),
        ),
    ];
    for (capacity, applied, pending, expected_calls, expected) in cases {
        let (counter, _) = populated(capacity, applied);
        let before = counter.checkpoint(4096).unwrap();
        let calls = Cell::new(0);
        let dropped = Cell::new(false);
        let result = counter.validate_proposal(
            operation(1),
            &VALID,
            hostile(pending, &calls, &dropped, None),
        );
        assert_eq!(result, expected);
        assert_eq!(calls.get(), expected_calls);
        assert!(dropped.get());
        assert_eq!(counter.checkpoint(4096).unwrap(), before);
    }
}

#[test]
fn malformed_candidate_precedes_any_iterator_access_even_at_zero_remaining() {
    for ids in [&[][..], &[1][..]] {
        let (counter, _) = populated(1, ids);
        let before = counter.checkpoint(4096).unwrap();
        let calls = Cell::new(0);
        let dropped = Cell::new(false);
        assert_eq!(
            counter.validate_proposal(
                operation(2),
                &[0; 7],
                hostile(&[3], &calls, &dropped, Some(0))
            ),
            Err(ApplicationError::InvalidCommand)
        );
        assert_eq!(calls.get(), 0);
        assert!(dropped.get());
        assert_eq!(counter.checkpoint(4096).unwrap(), before);
    }
}

#[test]
fn first_none_terminates_nonfused_pending_without_resurrection() {
    for capacity in [1, 2, 3] {
        let counter = Counter::new(capacity).unwrap();
        let calls = Cell::new(0);
        let mut pending = std::iter::from_fn(|| {
            calls.set(calls.get() + 1);
            match calls.get() {
                1 => Some((operation(1), &VALID[..])),
                2 => None,
                3 => Some((operation(2), &VALID[..])),
                _ => panic!("validator read beyond first None"),
            }
        });
        assert_eq!(
            counter.validate_proposal(operation(1), &VALID, &mut pending),
            Ok(std::mem::size_of::<CounterReceipt>())
        );
        assert_eq!(calls.get(), 2);
        assert_eq!(pending.next(), Some((operation(2), &VALID[..])));
    }
}

#[test]
fn iterator_panics_drop_temporary_state_without_changing_counter_or_retry() {
    for (capacity, panic_at) in [(4, 1), (2, 2), (1, 0)] {
        let (counter, _) = populated(capacity, &[1]);
        let before = counter.checkpoint(4096).unwrap();
        let calls = Cell::new(0);
        let dropped = Cell::new(false);
        let result = catch_unwind(AssertUnwindSafe(|| {
            counter.validate_proposal(
                operation(1),
                &VALID,
                hostile(&[1, 2, 3], &calls, &dropped, Some(panic_at)),
            )
        }));
        assert!(result.is_err());
        assert!(dropped.get());
        assert_eq!(calls.get(), panic_at + 1);
        assert_eq!(counter.checkpoint(4096).unwrap(), before);
        assert_eq!(
            counter.validate_proposal(operation(1), &VALID, std::iter::empty()),
            Ok(std::mem::size_of::<CounterReceipt>())
        );
    }
}
