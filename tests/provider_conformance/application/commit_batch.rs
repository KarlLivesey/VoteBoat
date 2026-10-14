// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Independent admission reference retained after rejecting fusion461.
use super::*;

fn pending(entries: &[LogEntry]) -> impl Iterator<Item = (OperationId, &[u8])> {
    entries.iter().filter_map(|entry| match &entry.payload {
        EntryPayload::Command { operation, bytes } => Some((*operation, bytes.as_slice())),
        _ => None,
    })
}

#[test]
fn counter_admission_is_invariant_under_moving_pending_prefix_to_applied() {
    for capacity in 1..=5 {
        // Includes duplicate/conflicting operations and overflow outcomes.
        let entries: Vec<_> = (0..8)
            .map(|offset| {
                CounterCase::command(
                    offset + 1,
                    (offset as usize % capacity + 1) as u128,
                    if offset % 2 == 0 { i64::MAX } else { 1 },
                )
            })
            .collect();
        for applied in 0..=entries.len() {
            let mut before = Counter::new(capacity).unwrap();
            before.apply_batch(&entries[..applied]).unwrap();
            for committed in applied..=entries.len() {
                let mut after = before.clone();
                after.apply_batch(&entries[applied..committed]).unwrap();
                for queued_id in [1, capacity + 1] {
                    let queued = [CounterCase::command(9, queued_id as u128, -1)];
                    for operation in 1..=capacity + 2 {
                        for bytes in [
                            &1i64.to_le_bytes()[..],
                            &i64::MAX.to_le_bytes()[..],
                            &[0][..],
                        ] {
                            let operation = OperationId::new(operation as u128).unwrap();
                            assert_eq!(
                                before.validate_proposal(
                                    operation,
                                    bytes,
                                    pending(&entries[applied..]).chain(pending(&queued)),
                                ),
                                after.validate_proposal(
                                    operation,
                                    bytes,
                                    pending(&entries[committed..]).chain(pending(&queued)),
                                ),
                                "capacity={capacity} applied={applied} committed={committed} queued={queued_id}",
                            );
                        }
                    }
                }
            }
        }
    }
}
