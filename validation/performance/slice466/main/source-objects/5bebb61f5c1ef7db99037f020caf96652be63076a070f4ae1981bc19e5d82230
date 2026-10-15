// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn received_append_payload_moves_into_accepted_tail_and_canonical_after_subset_barrier() {
    let mut store = open(2);
    let change = mutation(
        &store,
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1)].into(),
        }),
    );
    let LogMutation::Update(update) = &change else {
        unreachable!()
    };
    let pointer = payload_pointer(&update.suffix.as_ref().unwrap().entries[0]);
    let first = store.append_batch(vec![change]).unwrap();
    assert_eq!(payload_pointer(&tail(&store)[0]), pointer);
    assert!(store.durable[&group(1)].entries.is_empty());
    let other = mutation(&store, 2, 0, None);
    let other = store.append_batch(vec![other]).unwrap();
    assert_eq!(store.barrier(&other).unwrap().tickets, other);
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[0]),
        pointer
    );
    assert_eq!(store.barrier(&first).unwrap().tickets, first);
}
#[test]
fn owned_preparation_retains_original_mutation_ticket_order() {
    let mut store = open(2);
    let batch = vec![mutation(&store, 2, 0, None), mutation(&store, 1, 0, None)];
    let tickets = store.append_batch(batch).unwrap();
    assert_eq!(
        tickets
            .iter()
            .map(|ticket| ticket.group)
            .collect::<Vec<_>>(),
        vec![group(2), group(1)]
    );
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
}
