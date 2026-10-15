// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    identity::{ConfigurationId, OperationId},
    membership::{ConfigurationChange, ConfigurationRecord},
};
use std::mem::size_of;

fn entries() -> Vec<LogEntry> {
    vec![
        LogEntry {
            index: 37,
            term: 9,
            payload: EntryPayload::Noop,
        },
        LogEntry {
            index: 38,
            term: 10,
            payload: EntryPayload::Command {
                operation: OperationId::new(u128::MAX).unwrap(),
                bytes: vec![7, 8, 9].into(),
            },
        },
        LogEntry {
            index: 39,
            term: 10,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(1 << 127).unwrap(),
                expected: ConfigurationId::new(3).unwrap(),
                change: ConfigurationChange::Final {
                    id: ConfigurationId::new(4).unwrap(),
                },
            })),
        },
    ]
}

#[test]
fn capture_sharing_full_equality_and_explicit_mutable_ownership() {
    let original = entries();
    let pointer = original.as_ptr();
    let batch = EntryBatch::from(original);
    assert_eq!(batch.as_ptr(), pointer);
    let shared = batch.clone();
    assert_eq!(batch.as_ptr(), shared.as_ptr());
    let independent = EntryBatch::from(entries());
    assert_ne!(batch.as_ptr(), independent.as_ptr());
    assert_eq!(batch, independent);
    assert_eq!(batch, entries());
    assert_eq!(batch.as_ref(), entries());
    assert_eq!(format!("{batch:?}"), format!("{:?}", entries()));
    let weak = Arc::downgrade(batch.0.as_ref().unwrap());
    let mut mutable = batch.into_vec();
    assert_ne!(mutable.as_ptr(), pointer);
    mutable[0].term += 1;
    let EntryPayload::Command { operation, bytes } = &mut mutable[1].payload else {
        unreachable!()
    };
    *operation = OperationId::new(u128::MAX ^ (1 << 96)).unwrap();
    *bytes = vec![7, 8, 0].into();
    let EntryPayload::Configuration(record) = &mut mutable[2].payload else {
        unreachable!()
    };
    record.expected = ConfigurationId::new(9).unwrap();
    assert_ne!(shared, mutable);
    assert_eq!(shared, entries());
    assert!(weak.upgrade().is_some());
    drop(shared);
    assert!(weak.upgrade().is_none());
    let unique = EntryBatch::from(entries());
    let unique_pointer = unique.as_ptr();
    let recovered = unique.into_vec();
    assert_eq!(recovered.as_ptr(), unique_pointer);
}

#[test]
fn normalization_preserves_raw_capacity_validation_and_configuration_fallback() {
    let mut spare = Vec::with_capacity(37);
    spare.extend(entries().into_iter().take(2));
    let capacity = spare.capacity();
    let batch = EntryBatch::from(spare);
    assert_eq!(batch.capacity(), capacity);
    let normalized = batch.normalized_clone();
    assert_eq!(batch, normalized);
    assert_eq!(normalized.capacity(), normalized.len());
    assert_ne!(batch.as_ptr(), normalized.as_ptr());
    let plain: EntryBatch = entries().into_iter().take(2).collect::<Vec<_>>().into();
    let plain = plain.normalized_clone();
    assert!(plain.is_plain());
    assert_eq!(plain.as_ptr(), plain.normalized_clone().as_ptr());
    let configured = EntryBatch::from(entries());
    let clone = configured.normalized_clone();
    assert_eq!(configured, clone);
    assert_ne!(configured.as_ptr(), clone.as_ptr());
    let EntryPayload::Configuration(first) = &configured[2].payload else {
        unreachable!()
    };
    let EntryPayload::Configuration(second) = &clone[2].payload else {
        unreachable!()
    };
    assert!(!std::ptr::eq(first.as_ref(), second.as_ref()));
    let empty = EntryBatch::from(Vec::with_capacity(100));
    assert!(empty.0.is_some());
    assert!(empty.is_empty());
    assert!(empty.capacity() >= 100);
    assert!(empty.allocation_bytes().unwrap() >= empty.capacity() * size_of::<LogEntry>());
    let normalized_empty = empty.normalized_clone();
    assert!(normalized_empty.0.is_none());
    assert_eq!(normalized_empty.capacity(), 0);
    assert_eq!(normalized_empty.allocation_bytes(), Some(0));
    assert!(EntryBatch::from(Vec::new()).0.is_none());
}

#[test]
fn accounting_charges_raw_backing_header_spare_slots_and_each_owner_payloads() {
    let mut raw = Vec::with_capacity(37);
    raw.extend(entries());
    let payload = raw
        .iter()
        .map(LogEntry::retained_payload_bytes)
        .sum::<usize>();
    let capacity = raw.capacity();
    let batch = EntryBatch::from(raw);
    // Independent integer layout arithmetic: Arc's two atomic counters precede
    // a properly aligned Backing, then its complete allocation is padded.
    let alignment = std::mem::align_of::<Backing>()
        .max(std::mem::align_of::<AtomicUsize>())
        .max(2);
    let counters = 2 * size_of::<AtomicUsize>();
    let offset = counters.next_multiple_of(std::mem::align_of::<Backing>());
    let arc = (offset + size_of::<Backing>()).next_multiple_of(alignment);
    let expected = arc + capacity * size_of::<LogEntry>() + payload;
    assert_eq!(batch.allocation_bytes(), Some(expected));
    assert_eq!(
        EntryBatch::allocation_bytes_for_shape(capacity, payload),
        Some(expected)
    );
    assert_eq!(EntryBatch::allocation_bytes_for_shape(usize::MAX, 0), None);
    assert_eq!(
        EntryBatch::allocation_bytes_for_shape(capacity, usize::MAX),
        None
    );
    assert_eq!(EntryBatch::allocation_bytes_for_shape(0, 0), Some(0));
    assert_eq!(batch.clone().allocation_bytes(), Some(expected));
    assert_eq!(batch.clone().into_iter().collect::<Vec<_>>(), entries());
    assert_eq!((&batch).into_iter().cloned().collect::<Vec<_>>(), entries());
    let array = [entries().remove(0)];
    assert_eq!(EntryBatch::from(array.to_vec()), array);
    assert!(Layout::array::<LogEntry>(usize::MAX).is_err());
}

#[test]
fn independent_changes_to_every_header_and_payload_component_are_detected() {
    let batch = EntryBatch::from(entries());
    for field in 0..7 {
        let mut altered = entries();
        match field {
            0 => altered[0].index += 1,
            1 => altered[0].term += 1,
            2 => {
                altered[0].payload = EntryPayload::Command {
                    operation: OperationId::new(1).unwrap(),
                    bytes: Default::default(),
                }
            }
            3 | 4 => {
                let EntryPayload::Command { operation, bytes } = &mut altered[1].payload else {
                    unreachable!()
                };
                if field == 3 {
                    *operation = OperationId::new(u128::MAX ^ (1 << 96)).unwrap();
                } else {
                    *bytes = vec![7, 8, 0].into();
                }
            }
            _ => {
                let EntryPayload::Configuration(record) = &mut altered[2].payload else {
                    unreachable!()
                };
                if field == 5 {
                    record.operation = OperationId::new(1).unwrap();
                } else {
                    record.change = ConfigurationChange::Final {
                        id: ConfigurationId::new(5).unwrap(),
                    };
                }
            }
        }
        assert_ne!(batch, EntryBatch::from(altered), "field {field}");
    }
}
