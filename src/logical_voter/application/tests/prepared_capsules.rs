// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Capsule owners and logical bytes are independent of entry-container shape.
use super::*;
use crate::{
    log::EntryBatch,
    raft::{Message, RequestContext, Rpc},
};

fn protocol(width: usize, mixed: bool) -> Message {
    let mut commands = crate::log::CommandEntries::with_capacity(width);
    for offset in 0..width {
        commands.push(
            OperationId::new((1u128 << 127) + offset as u128 * 17 + 1).unwrap(),
            vec![offset as u8; if mixed { offset % 19 + 1 } else { 9 }].into(),
        );
    }
    Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: NodeId::new(1).unwrap(),
        sender: owner().store,
        to: NodeId::new(3).unwrap(),
        term: 3,
        context: RequestContext {
            origin: owner().store,
            sequence: 7,
        },
        rpc: Rpc::Append {
            previous_index: 6,
            previous_term: 2,
            entries: commands.bind(7, 3).unwrap(),
            leader_commit: 6,
        },
    }
}

fn capsule(a: &LogicalApplication, message: Message) -> LogicalCapsule {
    let parent = a.state.parent.as_ref().unwrap();
    LogicalCapsule {
        expected_revision: parent.revision,
        expected_generation: parent.generation,
        local_configuration: ConfigurationId::new(1).unwrap(),
        origin: LogicalCapsuleOrigin::Protocol(EffectTicket {
            visit: visit(),
            sequence: 1,
        }),
        body: LogicalCapsuleBody::Protocol(Box::new(message)),
    }
}

#[test]
fn prepared_capsule_clone_borrow_move_drop_and_codec_keep_full_contents() {
    for width in [1, 16, 231, 515] {
        for mixed in [false, true] {
            let mut a = app();
            claim(&mut a);
            let original = protocol(width, mixed);
            let Rpc::Append { entries, .. } = &original.rpc else {
                unreachable!()
            };
            let EntryPayload::Command { bytes, .. } = &entries[0].payload else {
                unreachable!()
            };
            let weak = bytes.weak().unwrap();
            let canonical = crate::log::EntryLog::from(entries.clone());
            let raw = entries.to_vec();
            let owned = capsule(&a, original.clone());
            let clone = owned.clone();
            let (LogicalCapsuleBody::Protocol(left), LogicalCapsuleBody::Protocol(right)) =
                (&owned.body, &clone.body)
            else {
                unreachable!()
            };
            assert!(!std::ptr::eq(left.as_ref(), right.as_ref()));
            assert_eq!(left.as_ref(), &original);
            assert_eq!(right.as_ref(), &original);
            let c = command(2, 1, CommandBody::Authorize(owned.clone()));
            let encoded = a.encode_command(&c, usize::MAX).unwrap();
            let mut generic = c.clone();
            let CommandBody::Authorize(generic_capsule) = &mut generic.body else {
                unreachable!()
            };
            let LogicalCapsuleBody::Protocol(generic_message) = &mut generic_capsule.body else {
                unreachable!()
            };
            let Rpc::Append { entries, .. } = &mut generic_message.rpc else {
                unreachable!()
            };
            *entries = EntryBatch::from(raw.clone());
            assert_eq!(a.encode_command(&generic, usize::MAX).unwrap(), encoded);
            assert_eq!(a.decode_command(&encoded).unwrap(), c);
            drop(generic);
            drop(c);
            drop(raw);
            drop(canonical);
            drop(original);
            drop(owned);
            assert!(
                weak.upgrade().is_some(),
                "independent capsule retains payload"
            );
            let LogicalCapsuleBody::Protocol(boxed) = clone.body else {
                unreachable!()
            };
            let moved = *boxed;
            let Rpc::Append { entries, .. } = &moved.rpc else {
                unreachable!()
            };
            assert_eq!(entries.len(), width);
            assert_eq!(entries[0].index, 7);
            assert_eq!(entries.last().unwrap().index, 6 + width as u64);
            assert!(
                weak.upgrade().is_some(),
                "moving out retains the actual payload"
            );
            drop(moved);
            assert!(
                weak.upgrade().is_none(),
                "last capsule owner releases payload"
            );
        }
    }
}

#[test]
fn cpu6_capsule_normal_sizes() {
    println!("CPU6_SIZE LogicalCapsuleBody {} LogicalCapsule {} CommandBody {} Command {} LogicalSendProvenance {} AuthorizedLogicalSend {} CommittedLogicalAction {}", std::mem::size_of::<LogicalCapsuleBody>(), std::mem::size_of::<LogicalCapsule>(), std::mem::size_of::<CommandBody>(), std::mem::size_of::<Command>(), std::mem::size_of::<LogicalSendProvenance>(), std::mem::size_of::<AuthorizedLogicalSend>(), std::mem::size_of::<crate::logical_voter::CommittedLogicalAction>());
}

#[test]
fn prepared_capsule_decode_charges_message_owner_at_exact_boundary() {
    for width in [1, 16, 231, 515] {
        for mixed in [false, true] {
            let mut a = app();
            claim(&mut a);
            let message = protocol(width, mixed);
            let Rpc::Append { entries, .. } = &message.rpc else {
                unreachable!()
            };
            let payloads = entries
                .iter()
                .map(LogEntry::retained_payload_bytes)
                .sum::<usize>();
            let prefixes = if mixed && width >= 16 {
                (width + 1) * std::mem::size_of::<[usize; 3]>()
            } else {
                0
            };
            // Decode uses a generic Vec of real entries, then transfers its
            // charged Message slot into the capsule's Box after dropping Vec.
            let expected = std::mem::size_of::<Message>()
                + width * std::mem::size_of::<LogEntry>()
                + payloads
                + prefixes;
            let c = command(2, 1, CommandBody::Authorize(capsule(&a, message)));
            let bytes = a.encode_command(&c, usize::MAX).unwrap();
            a.wire_codec = NativeWireCodec::with_membership(WireLimits {
                max_decoded_bytes: expected,
                ..WireLimits::default()
            })
            .unwrap();
            assert_eq!(a.decode_command(&bytes).unwrap(), c);
            a.wire_codec = NativeWireCodec::with_membership(WireLimits {
                max_decoded_bytes: expected - 1,
                ..WireLimits::default()
            })
            .unwrap();
            assert_eq!(
                a.decode_command(&bytes),
                Err(ApplicationError::InvalidCommand),
                "capsule decode must reject an omitted Message owner"
            );
        }
    }
}
