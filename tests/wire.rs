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
mod support;
use support::*;
use voteboat::{identity::*, raft::*, wire::*};
fn scope() -> WireScope {
    WireScope {
        from: node(1),
        sender: HostLogStore::new(1).binding,
        to: node(2),
    }
}
fn message(rpc: Rpc) -> Message {
    Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: scope().from,
        sender: scope().sender,
        to: scope().to,
        term: 3,
        // Responses can retain a context originating at a different store.
        context: RequestContext {
            origin: HostLogStore::new(2).binding,
            sequence: 7,
        },
        rpc,
    }
}
/// An independent single-fixture host implementation proves the seam can be
/// selected without native features. It is not a production full-RPC codec.
struct FixtureCodec {
    expected: Message,
}
impl WireCodec for FixtureCodec {
    fn format_version(&self) -> u16 {
        37
    }
    fn header_bytes(&self) -> usize {
        4
    }
    fn limits(&self) -> WireLimits {
        WireLimits {
            max_frame_bytes: 4,
            max_command_bytes: 1,
            max_snapshot_bytes: 1,
            ..WireLimits::default()
        }
    }
    fn frame_length(&self, header: &[u8]) -> Result<usize, WireError> {
        if header == b"HOST" {
            Ok(4)
        } else {
            Err(WireError::Corrupt("host fixture"))
        }
    }
    fn encode_batch(&self, scope: WireScope, messages: &[Message]) -> Result<Vec<u8>, WireError> {
        if messages != [self.expected.clone()] || !scope.matches(&self.expected) {
            return Err(WireError::WrongPeer);
        }
        Ok(b"HOST".to_vec())
    }
    fn decode_batch(&self, scope: WireScope, frame: &[u8]) -> Result<Vec<Message>, WireError> {
        self.frame_length(frame)?;
        if !scope.matches(&self.expected) {
            return Err(WireError::WrongPeer);
        }
        Ok(vec![self.expected.clone()])
    }
}
fn fixture_roundtrip(codec: impl WireCodec, message: Message) {
    let encoded = codec
        .encode_batch(scope(), std::slice::from_ref(&message))
        .unwrap();
    assert_eq!(
        codec.frame_length(&encoded[..codec.header_bytes()]),
        Ok(encoded.len())
    );
    assert_eq!(codec.decode_batch(scope(), &encoded).unwrap(), [message]);
    let mut wrong = scope();
    wrong.sender.session = StoreSession::new(2).unwrap();
    assert_eq!(
        codec.decode_batch(wrong, &encoded),
        Err(WireError::WrongPeer)
    );
}
#[test]
fn host_fixture_uses_only_public_wire_contract_without_native_features() {
    let expected = message(Rpc::ReadProbe);
    assert!(FixtureCodec {
        expected: expected.clone()
    }
    .limits()
    .validate()
    .is_ok());
    fixture_roundtrip(
        FixtureCodec {
            expected: expected.clone(),
        },
        expected,
    );
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::{log::*, native::wire::NativeWireCodec, quorum::*, snapshot::*};
    fn codec() -> NativeWireCodec {
        NativeWireCodec::new(WireLimits::default()).unwrap()
    }
    fn snapshot() -> Message {
        message(Rpc::Snapshot {
            snapshot: Box::new(Snapshot {
                metadata: SnapshotMetadata {
                    bootstrap: bootstrap(1, 3),
                    index: 2,
                    term: 2,
                    application_schema: 1,
                },
                application: vec![7; 24],
            }),
        })
    }
    fn append_message() -> Message {
        message(Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![entry(1, 1, 7)],
            leader_commit: 0,
        })
    }
    // Independent reference checksum allows valid-integrity hostile frames.
    // A recomputed CRC is not authentication and must not bypass shape budgets.
    fn crc(bytes: &[u8]) -> u32 {
        let mut value = !0u32;
        for byte in bytes {
            value ^= u32::from(*byte);
            for _ in 0..8 {
                value = if value & 1 == 0 {
                    value >> 1
                } else {
                    (value >> 1) ^ 0x82f63b78
                };
            }
        }
        !value
    }
    fn reseal(frame: &mut [u8]) {
        let h = crc(&frame[..20]);
        frame[20..24].copy_from_slice(&h.to_le_bytes());
        let end = frame.len() - 4;
        let b = crc(&frame[..end]);
        frame[end..].copy_from_slice(&b.to_le_bytes());
    }
    fn change(frame: &[u8], offset: usize, bytes: &[u8]) -> Vec<u8> {
        let mut result = frame.to_vec();
        result[offset..offset + bytes.len()].copy_from_slice(bytes);
        reseal(&mut result);
        result
    }
    fn frame(message: Message) -> Vec<u8> {
        codec().encode_batch(scope(), &[message]).unwrap()
    }
    #[test]
    fn native_all_rpc_multiplexing_snapshot_policy_and_canonical_frame_roundtrip() {
        fixture_roundtrip(codec(), message(Rpc::ReadProbe));
        let mut snap = snapshot();
        let Rpc::Snapshot { snapshot } = &mut snap.rpc else {
            panic!()
        };
        snapshot.metadata.bootstrap.policy = Policy::new(
            Tree::Majority(vec![
                Tree::Voter(node(1)),
                Tree::Weighted(vec![
                    WeightedChild {
                        weight: 1,
                        node: Tree::Voter(node(2)),
                    },
                    WeightedChild {
                        weight: 2,
                        node: Tree::Voter(node(3)),
                    },
                ]),
            ]),
            Limits::default(),
        )
        .unwrap();
        let mut messages = vec![
            message(Rpc::Vote {
                last_index: 2,
                last_term: 2,
            }),
            message(Rpc::Voted { granted: true }),
            message(Rpc::Voted { granted: false }),
            append_message(),
            message(Rpc::Append {
                previous_index: 0,
                previous_term: 0,
                entries: vec![
                    LogEntry {
                        index: 1,
                        term: 1,
                        payload: EntryPayload::Noop,
                    },
                    entry(2, 3, 9),
                ],
                leader_commit: 2,
            }),
            message(Rpc::Appended {
                success: true,
                matching_index: 2,
            }),
            message(Rpc::Appended {
                success: false,
                matching_index: 0,
            }),
            message(Rpc::ReadProbe),
            message(Rpc::ReadAck),
            snap,
            message(Rpc::SnapshotAck { index: 2 }),
            message(Rpc::Compacted { index: 2, term: 2 }),
        ];
        messages[1].group = group(2); // multiple groups in one peer frame
        let c = codec();
        let encoded = c.encode_batch(scope(), &messages).unwrap();
        assert_eq!(encoded.capacity(), encoded.len());
        let decoded = c.decode_batch(scope(), &encoded).unwrap();
        assert_eq!(decoded, messages);
        assert_eq!(c.encode_batch(scope(), &decoded).unwrap(), encoded);
        let probe = frame(message(Rpc::ReadProbe));
        assert_eq!(probe.len(), 161);
        assert_eq!(
            &probe[..20],
            &[
                b'V', b'B', b'W', b'I', b'R', b'E', b'0', b'1', 1, 0, 0, 0, 161, 0, 0, 0, 1, 0, 0,
                0
            ]
        );
        assert_eq!(&probe[24..28], &129u32.to_le_bytes());
        assert_eq!(probe[156], 4);
        assert_eq!(crc(b"123456789"), 0xe3069283);
    }
    #[test]
    fn every_single_bit_truncation_trailing_data_and_unknown_version_fail_closed() {
        let original = frame(message(Rpc::ReadProbe));
        let c = codec();
        for bit in 0..original.len() * 8 {
            let mut corrupt = original.clone();
            corrupt[bit / 8] ^= 1 << (bit % 8);
            assert!(c.decode_batch(scope(), &corrupt).is_err(), "bit {bit}");
        }
        for cut in 0..original.len() {
            assert!(
                c.decode_batch(scope(), &original[..cut]).is_err(),
                "cut {cut}"
            );
        }
        let mut trailing = original.clone();
        trailing.push(0);
        assert!(c.decode_batch(scope(), &trailing).is_err());
        assert_eq!(
            c.decode_batch(scope(), &change(&original, 8, &2u16.to_le_bytes())),
            Err(WireError::UnsupportedVersion(2))
        );
        assert_eq!(
            c.decode_batch(scope(), &change(&original, 10, &1u16.to_le_bytes())),
            Err(WireError::UnsupportedFlags)
        );
        assert_eq!(
            c.decode_batch(scope(), &change(&original, 156, &[255])),
            Err(WireError::InvalidMessage("RPC kind"))
        );
        let voted = frame(message(Rpc::Voted { granted: false }));
        assert_eq!(
            c.decode_batch(scope(), &change(&voted, 157, &[2])),
            Err(WireError::InvalidMessage("boolean"))
        );
    }
    #[test]
    fn valid_checksums_cannot_hide_unbounded_lengths_or_invalid_identity_and_entries() {
        let original = frame(append_message());
        let c = codec();
        // Fixed v1 offsets: frame 24, record length 4, message envelope 128,
        // RPC tag 1, append prefix 24, entry count 4, entry index/term/tag 17.
        for offset in [12, 16, 181, 218] {
            let corrupt = change(&original, offset, &u32::MAX.to_le_bytes());
            assert!(
                c.decode_batch(scope(), &corrupt).is_err(),
                "offset {offset}"
            );
        }
        let huge = change(&original, 12, &u32::MAX.to_le_bytes());
        assert_eq!(c.frame_length(&huge[..24]), Err(WireError::TooLarge));
        for (offset, length) in [
            (28, 16),
            (44, 8),
            (52, 8),
            (60, 8),
            (68, 16),
            (84, 8),
            (92, 8),
            (100, 8),
            (108, 8),
            (116, 16),
            (132, 8),
            (140, 8),
            (148, 8),
            (193, 8),
            (202, 16),
        ] {
            assert!(
                c.decode_batch(scope(), &change(&original, offset, &vec![0; length]))
                    .is_err(),
                "zero field {offset}"
            );
        }
        assert!(c
            .decode_batch(scope(), &change(&original, 185, &2u64.to_le_bytes()))
            .is_err());
        assert!(c
            .decode_batch(scope(), &change(&original, 193, &4u64.to_le_bytes()))
            .is_err());
        assert!(c
            .decode_batch(scope(), &change(&original, 201, &[9]))
            .is_err());
        assert!(c
            .decode_batch(scope(), &change(&original, 24, &u32::MAX.to_le_bytes()))
            .is_err());
        // A sender cannot switch recipient, node, store session or context by
        // merely recalculating integrity checks.
        assert_eq!(
            c.decode_batch(scope(), &change(&original, 92, &2u64.to_le_bytes())),
            Err(WireError::WrongPeer)
        );
        let mut wrong = scope();
        wrong.to = node(3);
        assert_eq!(c.decode_batch(wrong, &original), Err(WireError::WrongPeer));
        let mixed = [
            append_message(),
            Message {
                to: node(3),
                ..message(Rpc::ReadProbe)
            },
        ];
        assert_eq!(c.encode_batch(scope(), &mixed), Err(WireError::WrongPeer));
    }
    #[test]
    fn snapshot_policy_shape_weights_membership_and_payload_budgets_are_checked() {
        let original = frame(snapshot());
        let c = codec();
        for offset in [182, 213, 313] {
            assert!(
                c.decode_batch(scope(), &change(&original, offset, &u32::MAX.to_le_bytes()))
                    .is_err(),
                "snapshot count {offset}"
            );
        }
        // Duplicate leaves, duplicate store identities by voter key, empty
        // branches, and a map differing from policy membership are invalid.
        assert!(c
            .decode_batch(scope(), &change(&original, 196, &1u64.to_le_bytes()))
            .is_err());
        assert!(c
            .decode_batch(scope(), &change(&original, 249, &1u64.to_le_bytes()))
            .is_err());
        assert!(c
            .decode_batch(scope(), &change(&original, 182, &0u32.to_le_bytes()))
            .is_err());
        assert!(c
            .decode_batch(scope(), &change(&original, 217, &99u64.to_le_bytes()))
            .is_err());
        let mut weighted = snapshot();
        let Rpc::Snapshot { snapshot } = &mut weighted.rpc else {
            panic!()
        };
        snapshot.metadata.bootstrap.policy = Policy::new(
            Tree::Weighted(
                (1..=3)
                    .map(|id| WeightedChild {
                        weight: 1,
                        node: Tree::Voter(node(id)),
                    })
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap();
        let weighted = frame(weighted);
        assert!(c
            .decode_batch(scope(), &change(&weighted, 186, &0u64.to_le_bytes()))
            .is_err());
        assert!(c
            .decode_batch(scope(), &change(&weighted, 186, &u64::MAX.to_le_bytes()))
            .is_err());
        let narrow = NativeWireCodec::new(WireLimits {
            max_snapshot_bytes: 1,
            ..WireLimits::default()
        })
        .unwrap();
        assert_eq!(
            narrow.decode_batch(scope(), &original),
            Err(WireError::TooLarge)
        );
        // Recomputed checksums cannot hide an over-deep tree. Wrap the valid
        // tree in 34 single-child majority nodes, then update record/frame size.
        let mut deep = original.clone();
        deep.splice(181..181, [1u8, 1, 0, 0, 0].repeat(34));
        let len = deep.len() as u32;
        deep[12..16].copy_from_slice(&len.to_le_bytes());
        let record = len - 32;
        deep[24..28].copy_from_slice(&record.to_le_bytes());
        reseal(&mut deep);
        assert_eq!(c.decode_batch(scope(), &deep), Err(WireError::TooLarge));
        assert_eq!(
            narrow.encode_batch(scope(), &[super::native::snapshot()]),
            Err(WireError::TooLarge)
        );
        let narrow = NativeWireCodec::new(WireLimits {
            policy: Limits {
                max_depth: 0,
                max_voters: 1,
                max_tree_nodes: 1,
            },
            ..WireLimits::default()
        })
        .unwrap();
        assert_eq!(
            narrow.decode_batch(scope(), &original),
            Err(WireError::TooLarge)
        );
    }
    #[test]
    fn frame_command_entry_count_and_decoded_memory_limits_are_symmetric() {
        let c = codec();
        let input = append_message();
        let original = frame(input.clone());
        let limits = WireLimits {
            max_frame_bytes: 161,
            max_command_bytes: 8,
            max_snapshot_bytes: 8,
            ..WireLimits::default()
        };
        let tiny = NativeWireCodec::new(limits).unwrap();
        assert_eq!(
            tiny.encode_batch(scope(), std::slice::from_ref(&input)),
            Err(WireError::TooLarge)
        );
        assert_eq!(
            tiny.decode_batch(scope(), &original),
            Err(WireError::TooLarge)
        );
        let memory = NativeWireCodec::new(WireLimits {
            max_decoded_bytes: std::mem::size_of::<Message>(),
            ..WireLimits::default()
        })
        .unwrap();
        assert_eq!(
            memory.encode_batch(scope(), std::slice::from_ref(&input)),
            Err(WireError::TooLarge)
        );
        assert_eq!(
            memory.decode_batch(scope(), &original),
            Err(WireError::TooLarge)
        );
        assert!(memory
            .decode_batch(scope(), &frame(message(Rpc::ReadProbe)))
            .is_ok());
        let one = NativeWireCodec::new(WireLimits {
            max_messages: 1,
            ..WireLimits::default()
        })
        .unwrap();
        let messages = [input.clone(), input.clone()];
        let multiple = c.encode_batch(scope(), &messages).unwrap();
        assert_eq!(
            one.encode_batch(scope(), &messages),
            Err(WireError::TooLarge)
        );
        assert_eq!(
            one.decode_batch(scope(), &multiple),
            Err(WireError::TooLarge)
        );
        let entries = message(Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: vec![entry(1, 1, 1), entry(2, 2, 2)],
            leader_commit: 0,
        });
        let one_entry = NativeWireCodec::new(WireLimits {
            max_entries_per_message: 1,
            ..WireLimits::default()
        })
        .unwrap();
        assert_eq!(
            one_entry.encode_batch(scope(), std::slice::from_ref(&entries)),
            Err(WireError::TooLarge)
        );
        assert_eq!(
            one_entry.decode_batch(scope(), &c.encode_batch(scope(), &[entries]).unwrap()),
            Err(WireError::TooLarge)
        );
        let short_command = NativeWireCodec::new(WireLimits {
            max_command_bytes: 1,
            ..WireLimits::default()
        })
        .unwrap();
        let mut larger = input.clone();
        let Rpc::Append { entries, .. } = &mut larger.rpc else {
            panic!()
        };
        let EntryPayload::Command { bytes, .. } = &mut entries[0].payload else {
            panic!()
        };
        bytes.extend([1, 2, 3]);
        assert_eq!(
            short_command.encode_batch(scope(), &[larger.clone()]),
            Err(WireError::TooLarge)
        );
        assert_eq!(
            short_command.decode_batch(scope(), &c.encode_batch(scope(), &[larger]).unwrap()),
            Err(WireError::TooLarge)
        );
        let mut invalid = input.clone();
        invalid.context.sequence = 0;
        assert!(c.encode_batch(scope(), &[invalid]).is_err());
        assert!(NativeWireCodec::new(WireLimits {
            max_messages: 0,
            ..WireLimits::default()
        })
        .is_err());
        assert!(c.encode_batch(scope(), &[]).is_err());
        assert!(tiny
            .encode_batch(scope(), &[message(Rpc::ReadProbe)])
            .is_ok());
    }
}
