// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Pure ownership accounting controls grant no logical release authority.
use super::*;
use crate::{
    log::{CommandBytes, EntryBatch, EntryPayload},
    raft::{RequestContext, Rpc},
};
use std::mem::size_of;

#[test]
fn protocol_provenance_charges_the_whole_box_payload_and_encoded_capacity() {
    for width in [1, 16, 231, 515] {
        let mut commands = crate::log::CommandEntries::with_capacity(width);
        let mut payload_charge = 0;
        for offset in 0..width {
            let bytes: CommandBytes = vec![offset as u8; 9].into();
            payload_charge += bytes.retained_bytes();
            commands.push(
                OperationId::new((1u128 << 127) + offset as u128 + 1).unwrap(),
                bytes,
            );
        }
        let entries = commands.bind(7, 3).unwrap();
        let message = Message {
            group: group(1),
            configuration: ConfigurationId::new(1).unwrap(),
            from: NodeId::new(1).unwrap(),
            sender: owner().store,
            to: NodeId::new(2).unwrap(),
            term: 3,
            context: RequestContext {
                origin: owner().store,
                sequence: 9,
            },
            rpc: Rpc::Append {
                previous_index: 6,
                previous_term: 2,
                entries,
                leader_commit: 6,
            },
        };
        // The exact-capacity builder keeps the original wide Vec, has no
        // mixed-prefix allocation, and a singleton has no entry allocation.
        let entry_storage = if width == 1 {
            0
        } else {
            width * size_of::<LogEntry>()
        };
        let encoded_capacity = 997;
        let expected = size_of::<LogicalSendProvenance>()
            + size_of::<Command>()
            + encoded_capacity
            + size_of::<Message>()
            + entry_storage
            + payload_charge;
        assert_eq!(
            LogicalSendProvenance::protocol_charge(&message, encoded_capacity, expected),
            Ok(expected),
            "boxed Message must be counted in full"
        );
        assert_eq!(
            LogicalSendProvenance::protocol_charge(&message, encoded_capacity, expected - 1),
            Err(OutboundError::BatchTooLarge)
        );
        assert_eq!(
            LogicalSendProvenance::protocol_charge(&message, usize::MAX, usize::MAX),
            Err(OutboundError::BatchTooLarge)
        );
        let Rpc::Append { entries, .. } = &message.rpc else {
            unreachable!()
        };
        assert!(matches!(entries[0].payload, EntryPayload::Command { .. }));
        assert_eq!(EntryBatch::from_entry(entries[0].clone()).len(), 1);
    }
}

#[test]
fn capture_reservation_keeps_each_boxed_message_and_typed_command_owner() {
    let r = accounting();
    for count in [1, 3, 17] {
        let metadata = count * (size_of::<ExpectedCapsule>() + size_of::<LogicalReleaseRequest>())
            + size_of::<Pending<CounterReceipt, i64>>();
        let boxes = count * (3 * size_of::<Message>() + size_of::<Command>());
        let payload_and_encoding = count * 4 * r.limits.capsule_bytes;
        assert_eq!(
            r.capture_charge(count).unwrap(),
            metadata + boxes + payload_and_encoding
        );
    }
    assert_eq!(
        r.capture_charge(usize::MAX),
        Err(LogicalReleaseError::Overloaded)
    );
}

#[test]
fn cpu6_release_wrapper_normal_sizes() {
    println!("CPU6_SIZE ReleasePermit {} ExpectedCapsule {} LogicalReleaseRequest {} ReleasePending {} EffectLease {}", size_of::<ReleasePermit>(), size_of::<ExpectedCapsule>(), size_of::<LogicalReleaseRequest>(), size_of::<Pending<CounterReceipt, i64>>(), size_of::<EffectLease>());
}
