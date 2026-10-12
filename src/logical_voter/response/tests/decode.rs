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
use codec::Decoder;
fn header(d: &mut Decoder<'_>, kind: u8) {
    assert_eq!(d.take(8).unwrap(), MAGIC);
    assert_eq!(d.u8().unwrap(), kind);
}
pub(super) fn receipt(bytes: &[u8]) -> LogicalReceipt {
    let mut d = Decoder::new(bytes);
    header(&mut d, 0);
    let r = receipt_body(&mut d);
    d.finish().unwrap();
    r
}
fn receipt_body(d: &mut Decoder<'_>) -> LogicalReceipt {
    LogicalReceipt {
        index: d.u64().unwrap(),
        operation: d.id128(OperationId::new).unwrap(),
        first_index: d.u64().unwrap(),
        outcome: outcome(d),
        parent_revision: LogRevision::new(d.u64().unwrap()),
        parent_generation: LogGeneration::new(d.u64().unwrap()),
        epoch: RepresentativeEpoch::new(d.u64().unwrap()),
        duplicate: d.flag().unwrap(),
    }
}
fn outcome(d: &mut Decoder<'_>) -> LogicalOutcome {
    match d.u8().unwrap() {
        0 => LogicalOutcome::Claimed,
        1 => LogicalOutcome::Mutated,
        2 => LogicalOutcome::SnapshotPublished(d.reference().unwrap()),
        3 => LogicalOutcome::SnapshotReconciled,
        4 => LogicalOutcome::CapsuleAuthorized,
        5 => LogicalOutcome::Refused(match d.u8().unwrap() {
            0 => LogicalRefusal::StaleRepresentative,
            1 => LogicalRefusal::StaleImage,
            2 => LogicalRefusal::Ineligible,
            3 => LogicalRefusal::MissingSnapshot,
            4 => LogicalRefusal::SnapshotConflict,
            5 => LogicalRefusal::InvalidResponse,
            6 => LogicalRefusal::OperationConflict,
            _ => panic!("refusal tag"),
        }),
        _ => panic!("outcome tag"),
    }
}
pub(super) fn query(bytes: &[u8]) -> LogicalQuery {
    let mut d = Decoder::new(bytes);
    header(&mut d, 1);
    let q = match d.u8().unwrap() {
        0 => LogicalQuery::Image,
        1 => LogicalQuery::Outcome(d.id128(OperationId::new).unwrap()),
        2 => LogicalQuery::Snapshot(d.reference().unwrap()),
        3 => LogicalQuery::Metadata,
        4 => LogicalQuery::ConfigurationOperation(d.id128(OperationId::new).unwrap()),
        _ => panic!("query tag"),
    };
    d.finish().unwrap();
    q
}
pub(super) fn read(a: &LogicalApplication, bytes: &[u8]) -> LogicalRead {
    if bytes.get(9) == Some(&5) {
        return LogicalRead::ConfigurationOperation(Box::new(
            a.decode_configuration_operation(bytes).unwrap(),
        ));
    }
    let mut d = Decoder::new(bytes);
    header(&mut d, 2);
    let r = match d.u8().unwrap() {
        0 => LogicalRead::Image(Box::new(image(a, &mut d))),
        1 => LogicalRead::Outcome(if d.flag().unwrap() {
            Some(Box::new(status(&mut d)))
        } else {
            None
        }),
        2 => {
            assert_eq!(
                d.u64().unwrap(),
                u64::from(a.snapshot_codec.format_version())
            );
            LogicalRead::Snapshot(Box::new(
                a.snapshot_codec
                    .decode(
                        d.blob(a.limits.max_parent_checkpoint_bytes).unwrap(),
                        a.limits.parent_snapshots,
                    )
                    .unwrap(),
            ))
        }
        3 => LogicalRead::Metadata(Box::new(metadata(&mut d))),
        4 => LogicalRead::Uninitialized,
        _ => panic!("read tag"),
    };
    d.finish().unwrap();
    r
}
fn image(a: &LogicalApplication, d: &mut Decoder<'_>) -> LogicalImage {
    let mapping = d.mapping().unwrap();
    let representative = d.representative().unwrap();
    assert_eq!(d.u64().unwrap(), u64::from(a.log_codec.format_version()));
    let b = d.blob(a.limits.max_parent_checkpoint_bytes).unwrap();
    let (sequence, mut groups, consumed) = a
        .log_codec
        .decode_checkpoint(b, a.limits.parent_log)
        .unwrap();
    let parent = groups.remove(&mapping.parent).unwrap();
    assert!(groups.is_empty());
    assert_eq!(consumed, b.len());
    assert_eq!(sequence, parent.revision.get());
    assert_eq!(d.u64().unwrap(), u64::from(NativeLogCodec.format_version()));
    let membership = d.blob(a.limits.max_parent_checkpoint_bytes).unwrap();
    let child_applied = d.u64().unwrap();
    let child_membership = NativeLogCodec::decode_membership(membership, child_applied).unwrap();
    let published = d.optional_reference().unwrap();
    let pins = (0..d.u64().unwrap())
        .map(|_| d.reference().unwrap())
        .collect();
    LogicalImage {
        mapping,
        representative,
        parent,
        child_membership,
        child_applied,
        published,
        pins,
    }
}
fn status(d: &mut Decoder<'_>) -> LogicalActionStatus {
    LogicalActionStatus {
        operation: d.id128(OperationId::new).unwrap(),
        first_index: d.u64().unwrap(),
        first_term: d.u64().unwrap(),
        preceding_configuration: d.id(ConfigurationId::new).unwrap(),
        command: d.blob(16 * 1024 * 1024).unwrap().to_vec(),
        receipt: receipt_body(d),
    }
}
fn metadata(d: &mut Decoder<'_>) -> LogicalMetadata {
    LogicalMetadata {
        mapping: d.mapping().unwrap(),
        representative: if d.flag().unwrap() {
            Some(d.representative().unwrap())
        } else {
            None
        },
        child_applied: d.u64().unwrap(),
        retained_actions: usize::try_from(d.u64().unwrap()).unwrap(),
        retained_action_bytes: usize::try_from(d.u64().unwrap()).unwrap(),
        parent_revision: LogRevision::new(d.u64().unwrap()),
        parent_generation: LogGeneration::new(d.u64().unwrap()),
    }
}
