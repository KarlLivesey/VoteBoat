// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Public immutable ownership, ranges and per-owner physical accounting.
use super::*;
use std::{borrow::Cow, mem::size_of};

#[test]
fn batch_capture_clone_export_and_consumption_preserve_independent_values() {
    fn traits<T: Clone + Eq + std::fmt::Debug + Send + Sync>() {}
    traits::<EntryBatch>();
    traits::<EntryHistory>();
    let mut raw = Vec::with_capacity(19);
    raw.extend([entry(1, 7, 1), entry(2, 7, 2)]);
    let original_pointer = raw.as_ptr();
    let original_capacity = raw.capacity();
    let batch = EntryBatch::from(raw);
    let shared = batch.clone();
    assert_eq!(batch.as_ptr(), original_pointer);
    assert_eq!(shared.as_ptr(), original_pointer);
    assert_eq!(batch.capacity(), original_capacity);
    assert_eq!(batch.as_slice(), AsRef::<[LogEntry]>::as_ref(&batch));
    assert_eq!(format!("{batch:?}"), format!("{:?}", batch.as_slice()));
    let mut changed = shared.to_vec();
    changed[0].term += 1;
    assert_ne!(batch, changed);
    let exported = shared.into_vec();
    assert_ne!(exported.as_ptr(), original_pointer);
    assert_eq!(batch, exported);
    let recovered = batch.into_vec();
    assert_eq!(recovered.as_ptr(), original_pointer);
    let collected: EntryBatch = exported.into_iter().collect();
    assert_eq!(collected.into_iter().collect::<Vec<_>>().len(), 2);
    assert_eq!(EntryBatch::default().allocation_bytes(), Some(0));
}

#[test]
fn history_ranges_and_edits_match_vec_without_mutating_shared_owners() {
    let reference = (1..=9)
        .map(|index| entry(index, 1, index as u8))
        .collect::<Vec<_>>();
    let mut history = EntryHistory::default();
    for entries in reference.chunks(3) {
        history.append_batch(entries.to_vec().into());
    }
    let original = history.clone();
    for start in 0..=reference.len() {
        for end in start..=reference.len() {
            let range = history.range(start..end).unwrap();
            assert_eq!(range.len(), end - start);
            assert_eq!(range.is_empty(), start == end);
            assert_eq!(range.to_vec(), reference[start..end]);
            assert_eq!(
                range.iter().rev().cloned().collect::<Vec<_>>(),
                reference[start..end]
                    .iter()
                    .rev()
                    .cloned()
                    .collect::<Vec<_>>()
            );
        }
    }
    assert!(history.range(10..).is_none());
    assert!(history.range(..=usize::MAX).is_none());
    assert!(history.get(9).is_none());
    history.discard_prefix(2);
    history.truncate(4);
    let changed = entry(99, 9, 255);
    assert_eq!(
        history.replace(1, changed.clone()),
        Some(reference[3].clone())
    );
    let mut expected = reference[2..6].to_vec();
    expected[1] = changed;
    history.append_history(vec![entry(100, 9, 1)].into());
    expected.push(entry(100, 9, 1));
    history.push(entry(101, 9, 2));
    expected.push(entry(101, 9, 2));
    history.extend_from_slice(&reference[..1]);
    expected.extend_from_slice(&reference[..1]);
    history.extend(reference[1..2].iter().cloned());
    expected.extend_from_slice(&reference[1..2]);
    assert_eq!(history, expected);
    assert_eq!(original, reference);
    assert_eq!(original.first(), reference.first());
    assert_eq!(original.last(), reference.last());
    assert_eq!(history.into_vec(), expected);
    let mut collected: EntryHistory = original.into_iter().collect();
    assert_eq!(collected, reference);
    collected.clear();
    assert!(collected.is_empty());
}

#[test]
fn cross_chunk_coalescing_preserves_one_ordered_complete_interval() {
    let first: EntryBatch = vec![entry(1, 1, 1), entry(2, 1, 2)].into();
    let mut history = EntryHistory::from(first.clone());
    history.append_batch(vec![entry(3, 1, 3)].into());
    let range = history.view(..2);
    assert_eq!(range.as_contiguous().unwrap().as_ptr(), first.as_ptr());
    assert!(matches!(range.to_cow(), Cow::Borrowed(_)));
    let complete = history.view(..);
    assert!(complete.as_contiguous().is_none());
    let Cow::Owned(mut entries) = complete.to_cow() else {
        panic!("cross-chunk interval requires one contiguous export")
    };
    assert_eq!(
        entries,
        vec![entry(1, 1, 1), entry(2, 1, 2), entry(3, 1, 3)]
    );
    entries[1].term += 1;
    assert_eq!(history[1].term, 1);
    let mut iterator = complete.iter();
    assert_eq!(iterator.len(), 3);
    assert_eq!(iterator.next(), Some(&history[0]));
    assert_eq!(iterator.next_back(), Some(&history[2]));
    assert_eq!(iterator.next(), Some(&history[1]));
    assert_eq!(iterator.next_back(), None);
    assert!(history.view(2..2).is_empty());
}

#[test]
fn every_owner_charges_full_backing_including_hidden_entries_and_spare_capacity() {
    let mut raw = Vec::with_capacity(37);
    raw.extend([entry(1, 1, 1), entry(2, 1, 2), entry(3, 1, 3)]);
    let slots = raw.capacity();
    let batch: EntryBatch = raw.into();
    let charge = batch.allocation_bytes().unwrap();
    assert!(charge > slots * size_of::<LogEntry>());
    let mut history = EntryHistory::from(batch.clone());
    let full = history.allocation_bytes().unwrap();
    assert!(full > charge);
    let original = history.clone();
    assert_eq!(original.allocation_bytes(), Some(full));
    history.discard_prefix(1);
    history.truncate(1);
    assert_eq!(history.capacity(), slots);
    assert_eq!(history.allocation_bytes(), Some(full));
    assert!(std::ptr::eq(&history[0], &batch[1]));
    drop(batch);
    assert_eq!(history[0], original[1]);
    history.clear();
    let descriptors = history.allocation_bytes().unwrap();
    assert!(descriptors > 0 && descriptors < full);
    assert_eq!(original.len(), 3);
    let reserved: EntryBatch = Vec::with_capacity(37).into();
    assert!(reserved.is_empty());
    assert!(reserved.allocation_bytes().unwrap() >= 37 * size_of::<LogEntry>());
    assert_eq!(EntryHistory::from(reserved).allocation_bytes(), Some(0));
}
