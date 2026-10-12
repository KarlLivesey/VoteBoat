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
fn exact_original_entries_and_additive_query_encoding_roundtrip() {
    let mut a = app(LogicalLimits::default());
    for operation in [10, 11, 999] {
        let q = LogicalQuery::ConfigurationOperation(op(operation));
        assert_eq!(a.query_encoding_bound(&q).unwrap(), 26);
        let mut query = [0; 26];
        assert_eq!(a.encode_query(&q, &mut query).unwrap(), 26);
        assert_eq!(&query[..10], b"VBLOGR01\x01\x04");
        assert_eq!(&query[10..], &operation.to_le_bytes());
    }
    let mut old = [0; 10];
    a.encode_query(&LogicalQuery::Metadata, &mut old).unwrap();
    assert_eq!(&old, b"VBLOGR01\x01\x03");
    for entries in [prefix(), suffix()] {
        a.apply_batch(&entries).unwrap();
        for operation in [10, 11, 999] {
            let h = history(&a, operation);
            let bytes = encoded(&a, h.clone());
            assert_eq!(a.decode_configuration_operation(&bytes).unwrap(), h);
        }
    }
}
#[test]
fn truncated_trailing_overcount_and_wrong_scope_bytes_refuse() {
    let mut a = app(LogicalLimits::default());
    a.apply_batch(&prefix()).unwrap();
    a.apply_batch(&suffix()).unwrap();
    let bytes = encoded(&a, history(&a, 11));
    for cut in 0..bytes.len() {
        assert!(a.decode_configuration_operation(&bytes[..cut]).is_err());
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(a.decode_configuration_operation(&trailing).is_err());
    // Original header10 + group24 + operation16 + applied8 + count8 + codec8.
    let changes: [(usize, &[u8]); 7] = [
        (0, b"BADMAGIC"),
        (9, &[4]),
        (10, &99u128.to_le_bytes()),
        (34, &12u128.to_le_bytes()),
        (50, &3u64.to_le_bytes()),
        (58, &u64::MAX.to_le_bytes()),
        (66, &99u64.to_le_bytes()),
    ];
    for (offset, value) in changes {
        let mut changed = bytes.clone();
        changed[offset..offset + value.len()].copy_from_slice(value);
        assert!(a.decode_configuration_operation(&changed).is_err());
    }
    let mut oversized = bytes;
    oversized.resize(a.limits().max_parent_checkpoint_bytes + 1, 0);
    assert_eq!(
        a.decode_configuration_operation(&oversized),
        Err(ApplicationError::ReceiptBudget)
    );
}
#[test]
fn invalid_original_history_shapes_cannot_encode_and_short_buffers_are_atomic() {
    let mut a = app(LogicalLimits::default());
    a.apply_batch(&prefix()).unwrap();
    a.apply_batch(&suffix()).unwrap();
    let original = history(&a, 11);
    let mut variants = Vec::new();
    for entries in [
        vec![final_entry()],
        vec![final_entry(), joint()],
        vec![joint(), final_entry(), final_entry()],
        vec![learners(), final_entry()],
    ] {
        let mut h = original.clone();
        h.entries = entries;
        variants.push(h);
    }
    let mut h = original.clone();
    h.entries[1].term = 2;
    variants.push(h);
    let mut h = original.clone();
    h.entries[0].payload = EntryPayload::Noop;
    variants.push(h);
    let mut h = original.clone();
    h.child_applied = 6;
    variants.push(h);
    let bytes = encoded(&a, original.clone());
    for len in [0, 9, bytes.len() - 1] {
        let mut short = vec![0xa5; len];
        assert!(a
            .encode_read(
                &LogicalRead::ConfigurationOperation(Box::new(original.clone())),
                &mut short
            )
            .is_err());
        assert!(short.iter().all(|b| *b == 0xa5));
    }
    for h in variants {
        let mut out = vec![0xa5; a.limits().max_parent_checkpoint_bytes];
        assert!(a
            .encode_read(&LogicalRead::ConfigurationOperation(Box::new(h)), &mut out)
            .is_err());
        assert!(out.iter().all(|b| *b == 0xa5));
    }
}
#[test]
fn retained_allocations_are_charged_and_config_apply_refuses_small_envelope_atomically() {
    let mut a = app(LogicalLimits::default());
    a.apply_batch(&prefix()).unwrap();
    a.apply_batch(&suffix()).unwrap();
    let mut h = history(&a, 11);
    h.entries.reserve_exact(13);
    let result = LogicalRead::ConfigurationOperation(Box::new(h));
    let charge = a.read_result_bytes(&result, usize::MAX).unwrap();
    assert_eq!(
        a.read_result_bytes(&result, charge - 1),
        Err(ApplicationError::ReceiptBudget)
    );
    assert_eq!(a.read_result_bytes(&result, charge).unwrap(), charge);
    let mut small = app(LogicalLimits {
        max_parent_checkpoint_bytes: 8192,
        ..LogicalLimits::default()
    });
    let before = small.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        small.apply_batch(&many_learners()),
        Err(ApplicationError::ReceiptBudget)
    );
    assert_eq!(small.checkpoint(usize::MAX).unwrap(), before);
    assert_eq!(small.applied_index(), 0);
}
