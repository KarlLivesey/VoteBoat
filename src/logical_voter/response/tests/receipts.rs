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
fn encoded_receipt(a: &LogicalApplication, r: &LogicalReceipt) -> Vec<u8> {
    let mut out = vec![0; RECEIPT_BOUND];
    let n = a.encode_receipt(r, &mut out).unwrap();
    out.truncate(n);
    out
}
#[test]
fn receipts_preserve_all_outcomes_and_original_fields() {
    let a = app();
    let original = receipt();
    let bytes = encoded_receipt(&a, &original);
    assert_eq!(bytes.len(), RECEIPT_BOUND);
    assert_eq!(decode::receipt(&bytes), original);
    assert_eq!(a.logical_response_schema(), LOGICAL_RESPONSE_SCHEMA);
    let changes: [fn(&mut LogicalReceipt); 7] = [
        |r| r.index += 1,
        |r| r.operation = OperationId::new(31).unwrap(),
        |r| r.first_index += 1,
        |r| r.parent_revision = None,
        |r| r.parent_generation = None,
        |r| r.epoch = None,
        |r| r.duplicate = false,
    ];
    for change in changes {
        let mut other = original;
        change(&mut other);
        let b = encoded_receipt(&a, &other);
        assert_ne!(b, bytes);
        assert_eq!(decode::receipt(&b), other);
    }
    let outcomes = [
        LogicalOutcome::Claimed,
        LogicalOutcome::Mutated,
        LogicalOutcome::SnapshotReconciled,
        LogicalOutcome::CapsuleAuthorized,
    ];
    for outcome in outcomes {
        let r = LogicalReceipt {
            outcome,
            ..original
        };
        let b = encoded_receipt(&a, &r);
        assert_ne!(b, bytes);
        assert_eq!(decode::receipt(&b), r);
    }
    for refusal in [
        LogicalRefusal::StaleRepresentative,
        LogicalRefusal::StaleImage,
        LogicalRefusal::Ineligible,
        LogicalRefusal::MissingSnapshot,
        LogicalRefusal::SnapshotConflict,
        LogicalRefusal::InvalidResponse,
        LogicalRefusal::OperationConflict,
        LogicalRefusal::Expired,
    ] {
        let r = LogicalReceipt {
            outcome: LogicalOutcome::Refused(refusal),
            ..original
        };
        assert_eq!(decode::receipt(&encoded_receipt(&a, &r)), r);
    }
}
fn references() -> Vec<SnapshotRef> {
    let r = reference();
    vec![
        SnapshotRef {
            store: store(3),
            ..r
        },
        SnapshotRef {
            group: group(3),
            ..r
        },
        SnapshotRef {
            generation: SnapshotGeneration::new(2).unwrap(),
            ..r
        },
        SnapshotRef {
            configuration: ConfigurationId::new(2).unwrap(),
            ..r
        },
        SnapshotRef { index: 2, ..r },
        SnapshotRef { term: 2, ..r },
        SnapshotRef {
            application_schema: 2,
            ..r
        },
        SnapshotRef {
            file_bytes: 124,
            ..r
        },
        SnapshotRef {
            checksum: 12346,
            ..r
        },
        SnapshotRef {
            store: StoreIdentity {
                incarnation: StoreIncarnation::new(2).unwrap(),
                ..r.store
            },
            ..r
        },
        SnapshotRef {
            group: GroupIdentity {
                incarnation: GroupIncarnation::new(2).unwrap(),
                ..r.group
            },
            ..r
        },
    ]
}
#[test]
fn exact_query_variants_and_every_snapshot_reference_field_are_distinct() {
    let a = app();
    for q in [
        LogicalQuery::Image,
        LogicalQuery::Outcome(OperationId::new(4).unwrap()),
        LogicalQuery::Snapshot(reference()),
        LogicalQuery::Metadata,
    ] {
        let bound = a.query_encoding_bound(&q).unwrap();
        let mut bytes = vec![0; bound];
        assert_eq!(a.encode_query(&q, &mut bytes).unwrap(), bound);
        assert_eq!(decode::query(&bytes), q);
    }
    let mut golden = [0u8; 10];
    a.encode_query(&LogicalQuery::Metadata, &mut golden)
        .unwrap();
    assert_eq!(&golden, b"VBLOGR03\x01\x03");
    let original = encoded_receipt(&a, &receipt());
    for r in references() {
        let receipt = LogicalReceipt {
            outcome: LogicalOutcome::SnapshotPublished(r),
            ..receipt()
        };
        let bytes = encoded_receipt(&a, &receipt);
        assert_ne!(bytes, original);
        assert_eq!(decode::receipt(&bytes), receipt);
        let q = LogicalQuery::Snapshot(r);
        let mut bytes = vec![0; 114];
        a.encode_query(&q, &mut bytes).unwrap();
        assert_eq!(decode::query(&bytes), q);
    }
}
#[test]
fn short_receipt_and_query_destinations_remain_unchanged() {
    let a = app();
    let r = receipt();
    for len in 0..RECEIPT_BOUND {
        let mut out = vec![0xa5; len];
        assert!(a.encode_receipt(&r, &mut out).is_err());
        assert!(out.iter().all(|b| *b == 0xa5));
    }
    let q = LogicalQuery::Snapshot(reference());
    for len in 0..114 {
        let mut out = vec![0xa5; len];
        assert!(a.encode_query(&q, &mut out).is_err());
        assert!(out.iter().all(|b| *b == 0xa5));
    }
}
