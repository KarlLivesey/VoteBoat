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
                    membership: None,
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

    fn membership_codec() -> NativeWireCodec {
        NativeWireCodec::with_membership(WireLimits::default()).unwrap()
    }
    fn configuration(
        id: u64,
        voters: &[u64],
        learners: &[u64],
    ) -> voteboat::membership::Configuration {
        voteboat::membership::Configuration::new(
            ConfigurationId::new(id).unwrap(),
            Policy::new(
                Tree::Majority(voters.iter().map(|n| Tree::Voter(node(*n))).collect()),
                Limits::default(),
            )
            .unwrap(),
            voters
                .iter()
                .map(|n| (node(*n), identity(*n as u128)))
                .collect(),
            learners
                .iter()
                .map(|n| (node(*n), identity(*n as u128)))
                .collect(),
        )
        .unwrap()
    }
    fn configuration_entries() -> Vec<LogEntry> {
        use voteboat::membership::*;
        [
            (
                100,
                1,
                ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4])),
            ),
            (
                101,
                2,
                ConfigurationChange::Joint {
                    id: ConfigurationId::new(3).unwrap(),
                    next: configuration(4, &[2, 3, 4], &[1]),
                },
            ),
            (
                101,
                3,
                ConfigurationChange::Final {
                    id: ConfigurationId::new(4).unwrap(),
                },
            ),
        ]
        .into_iter()
        .enumerate()
        .map(|(i, (operation, expected, change))| LogEntry {
            index: i as u64 + 1,
            term: 2,
            payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                operation: OperationId::new(operation).unwrap(),
                expected: ConfigurationId::new(expected).unwrap(),
                change,
            })),
        })
        .collect()
    }
    fn membership_append() -> Message {
        message(Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: configuration_entries(),
            leader_commit: 2,
        })
    }
    fn membership_snapshot(finalized: bool) -> Message {
        let entries = configuration_entries();
        let index = if finalized { 3 } else { 2 };
        let membership =
            voteboat::membership::Membership::replay(&bootstrap(1, 3), &entries[..index], 2)
                .unwrap();
        let mut result = message(Rpc::Snapshot {
            snapshot: Box::new(Snapshot {
                metadata: SnapshotMetadata {
                    bootstrap: bootstrap(1, 3),
                    membership: Some(Box::new(membership)),
                    index: index as u64,
                    term: 2,
                    application_schema: 1,
                },
                application: vec![9; 32],
            }),
        });
        // The sender's accepted head can be newer than the checkpoint base.
        result.configuration = ConfigurationId::new(4).unwrap();
        result
    }
    #[test]
    fn explicit_wire_two_roundtrips_all_configuration_phases_and_joint_final_snapshots() {
        let c = membership_codec();
        assert_eq!(c.format_version(), 2);
        let mut messages = vec![
            membership_append(),
            membership_snapshot(false),
            membership_snapshot(true),
            snapshot(),
            message(Rpc::ReadProbe),
            Message {
                configuration: ConfigurationId::new(4).unwrap(),
                ..snapshot()
            },
        ];
        messages[4].group = group(2);
        let bytes = c.encode_batch(scope(), &messages).unwrap();
        assert_eq!(bytes.capacity(), bytes.len());
        assert_eq!(c.decode_batch(scope(), &bytes).unwrap(), messages);
        assert_eq!(
            c.encode_batch(scope(), &c.decode_batch(scope(), &bytes).unwrap())
                .unwrap(),
            bytes
        );
        assert_eq!(
            codec().frame_length(&bytes[..24]),
            Err(WireError::UnsupportedVersion(2))
        );
        assert_eq!(
            c.frame_length(&frame(message(Rpc::ReadProbe))[..24]),
            Err(WireError::UnsupportedVersion(1))
        );
        for input in [
            membership_append(),
            membership_snapshot(false),
            membership_snapshot(true),
        ] {
            assert!(codec()
                .encode_batch(scope(), std::slice::from_ref(&input))
                .is_err());
            let bytes = c.encode_batch(scope(), &[input]).unwrap();
            // Re-labelling a v2 frame does not make mandatory extensions valid v1.
            let old = change(&bytes, 8, &1u16.to_le_bytes());
            assert!(codec().decode_batch(scope(), &old).is_err());
        }
    }
    #[test]
    fn wire_two_recursive_weighted_policy_survives_without_changing_bootstrap() {
        use voteboat::membership::*;
        let target = Configuration::new(
            ConfigurationId::new(4).unwrap(),
            Policy::new(
                Tree::Majority(vec![
                    Tree::Voter(node(2)),
                    Tree::Weighted(vec![
                        WeightedChild {
                            weight: 2,
                            node: Tree::Voter(node(3)),
                        },
                        WeightedChild {
                            weight: 1,
                            node: Tree::Voter(node(4)),
                        },
                    ]),
                ]),
                Limits::default(),
            )
            .unwrap(),
            configuration(4, &[2, 3, 4], &[]).voter_stores().clone(),
            Default::default(),
        )
        .unwrap();
        let mut entries = configuration_entries();
        let EntryPayload::Configuration(record) = &mut entries[1].payload else {
            panic!()
        };
        record.change = ConfigurationChange::Joint {
            id: ConfigurationId::new(3).unwrap(),
            next: target,
        };
        let base = Membership::replay(&bootstrap(1, 3), &entries[..2], 1).unwrap();
        let mut input = membership_snapshot(false);
        let Rpc::Snapshot { snapshot } = &mut input.rpc else {
            panic!()
        };
        snapshot.metadata.membership = Some(Box::new(base));
        fixture_roundtrip(membership_codec(), input);
    }
    #[test]
    fn wire_two_integrity_truncation_and_valid_crc_hostile_configuration_fields_fail_closed() {
        let c = membership_codec();
        for input in [
            membership_append(),
            membership_snapshot(false),
            membership_snapshot(true),
        ] {
            let bytes = c.encode_batch(scope(), &[input]).unwrap();
            for cut in 0..bytes.len() {
                assert!(c.decode_batch(scope(), &bytes[..cut]).is_err(), "cut {cut}");
            }
            for bit in 0..bytes.len() * 8 {
                let mut corrupt = bytes.clone();
                corrupt[bit / 8] ^= 1 << (bit % 8);
                assert!(c.decode_batch(scope(), &corrupt).is_err(), "bit {bit}");
            }
        }
        let bytes = c.encode_batch(scope(), &[membership_append()]).unwrap();
        // First entry's subformat, operation, expected ID, change kind, next ID,
        // majority child count, voter-store count, learner-store count.
        for (offset, field) in [
            (202, vec![2]),
            (203, vec![0; 16]),
            (219, vec![0; 8]),
            (227, vec![255]),
            (228, vec![0; 8]),
            (237, u32::MAX.to_le_bytes().to_vec()),
            (268, u32::MAX.to_le_bytes().to_vec()),
            (368, u32::MAX.to_le_bytes().to_vec()),
        ] {
            assert!(
                c.decode_batch(scope(), &change(&bytes, offset, &field))
                    .is_err(),
                "field {offset}"
            );
        }
        // Duplicate voter leaf, voter-map key, and overlap learner/voter maps.
        for offset in [250, 304, 372] {
            assert!(
                c.decode_batch(scope(), &change(&bytes, offset, &1u64.to_le_bytes()))
                    .is_err(),
                "duplicate/overlap {offset}"
            );
        }
    }
    #[test]
    fn wire_two_membership_base_rejects_invalid_history_and_operation_sets() {
        let c = membership_codec();
        let bytes = c
            .encode_batch(scope(), &[membership_snapshot(false)])
            .unwrap();
        // Snapshot tag9, original bootstrap config, index/term/schema,
        // bootstrap tree+map, then membership subformat at322. Stable config
        // ends499; last config index at499, joint flag507. Joint target ends716;
        // operation count716, operation IDs720 and736.
        assert_eq!(bytes[156], 9);
        assert_eq!(bytes[322], 1);
        assert_eq!(bytes[507], 1);
        assert_eq!(u32::from_le_bytes(bytes[716..720].try_into().unwrap()), 2);
        for (offset, field) in [
            (157, vec![0; 8]),
            (321, vec![2]),
            (322, vec![2]),
            (499, 3u64.to_le_bytes().to_vec()),
            (507, vec![2]),
            (508, vec![0; 16]),
            (524, 2u64.to_le_bytes().to_vec()),
            (532, 1u64.to_le_bytes().to_vec()),
            (716, u32::MAX.to_le_bytes().to_vec()),
            (736, 100u128.to_le_bytes().to_vec()),
            (736, 102u128.to_le_bytes().to_vec()),
        ] {
            assert!(
                c.decode_batch(scope(), &change(&bytes, offset, &field))
                    .is_err(),
                "checkpoint field {offset}"
            );
        }
        // A coherent checkpoint still cannot rewrite the immutable bootstrap.
        assert!(c
            .decode_batch(scope(), &change(&bytes, 157, &5u64.to_le_bytes()))
            .is_err());
    }
    #[test]
    fn wire_two_resource_limits_include_configurations_and_checkpoint_operation_history() {
        let c = membership_codec();
        let inputs = [
            membership_append(),
            membership_snapshot(false),
            membership_snapshot(true),
        ];
        for input in inputs {
            let bytes = c
                .encode_batch(scope(), std::slice::from_ref(&input))
                .unwrap();
            for limits in [
                WireLimits {
                    max_decoded_bytes: std::mem::size_of::<Message>() + 128,
                    ..WireLimits::default()
                },
                WireLimits {
                    policy: Limits {
                        max_voters: 3,
                        ..Limits::default()
                    },
                    ..WireLimits::default()
                },
                WireLimits {
                    max_frame_bytes: 512,
                    max_snapshot_bytes: 128,
                    max_command_bytes: 128,
                    ..WireLimits::default()
                },
            ] {
                let narrow = NativeWireCodec::with_membership(limits).unwrap();
                assert!(narrow
                    .encode_batch(scope(), std::slice::from_ref(&input))
                    .is_err());
                assert!(narrow.decode_batch(scope(), &bytes).is_err());
            }
        }
        let bytes = c.encode_batch(scope(), &[membership_append()]).unwrap();
        let narrow = NativeWireCodec::with_membership(WireLimits {
            max_command_bytes: 32,
            ..WireLimits::default()
        })
        .unwrap();
        assert_eq!(
            narrow.decode_batch(scope(), &bytes),
            Err(WireError::TooLarge)
        );
        assert_eq!(
            narrow.encode_batch(scope(), &[membership_append()]),
            Err(WireError::TooLarge)
        );
        // Search the actual symmetric retained-object bound, rather than assume
        // that a compact wire representation accounts for decoded B-tree state.
        let input = membership_snapshot(false);
        let bytes = c
            .encode_batch(scope(), std::slice::from_ref(&input))
            .unwrap();
        let mut low = std::mem::size_of::<Message>();
        let mut high = WireLimits::default().max_decoded_bytes;
        while low < high {
            let mid = low + (high - low) / 2;
            let narrow = NativeWireCodec::with_membership(WireLimits {
                max_decoded_bytes: mid,
                ..WireLimits::default()
            })
            .unwrap();
            if narrow.decode_batch(scope(), &bytes).is_ok() {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        assert!(low > bytes.len());
        for (max_decoded_bytes, expected) in [(low - 1, false), (low, true)] {
            let narrow = NativeWireCodec::with_membership(WireLimits {
                max_decoded_bytes,
                ..WireLimits::default()
            })
            .unwrap();
            assert_eq!(narrow.decode_batch(scope(), &bytes).is_ok(), expected);
            assert_eq!(
                narrow
                    .encode_batch(scope(), std::slice::from_ref(&input))
                    .is_ok(),
                expected
            );
        }
    }

    #[test]
    fn wire_two_maximum_checkpoint_operation_set_remains_bounded_and_lossless() {
        use voteboat::membership::*;
        let boundary = MAX_CONFIGURATION_OPERATIONS as u64;
        let operations = (1..=MAX_CONFIGURATION_OPERATIONS)
            .map(|n| OperationId::new(n as u128).unwrap())
            .collect();
        let base = Membership::from_checkpoint(
            configuration(2, &[1, 2, 3], &[4]),
            None,
            boundary,
            operations,
            boundary,
        )
        .unwrap();
        let mut input = membership_snapshot(true);
        let Rpc::Snapshot { snapshot } = &mut input.rpc else {
            panic!()
        };
        snapshot.metadata.index = boundary;
        snapshot.metadata.membership = Some(Box::new(base));
        fixture_roundtrip(membership_codec(), input);
    }

    #[test]
    fn authority_format_is_explicit_bounded_and_preserves_membership_payloads() {
        use voteboat::secure::PeerIdentity;
        let codec = NativeWireCodec::with_authority(WireLimits::default()).unwrap();
        assert_eq!(codec.format_version(), 3);
        let candidate = PeerIdentity {
            node: node(4),
            store: identity(4),
        };
        let head = ConfigurationId::new(3).unwrap();
        let mut query = message(Rpc::AuthorityRequest {
            candidate,
            configuration: head,
        });
        query.context.origin = query.sender;
        let grant = message(Rpc::AuthorityReply {
            candidate,
            configuration: head,
            committed_index: 2,
            committed_term: 2,
            granted: true,
        });
        let denial = message(Rpc::AuthorityReply {
            candidate,
            configuration: head,
            committed_index: 0,
            committed_term: 0,
            granted: false,
        });
        for input in [query.clone(), grant.clone(), denial] {
            fixture_roundtrip(codec, input.clone());
            for old in [super::native::codec(), membership_codec()] {
                assert!(old
                    .encode_batch(scope(), std::slice::from_ref(&input))
                    .is_err());
                let frame = codec
                    .encode_batch(scope(), std::slice::from_ref(&input))
                    .unwrap();
                assert!(old.decode_batch(scope(), &frame).is_err());
                // Even correctly resealed old-version frames reject the RPC kind.
                let frame = change(&frame, 8, &old.format_version().to_le_bytes());
                assert!(old.decode_batch(scope(), &frame).is_err());
            }
        }
        fixture_roundtrip(codec, membership_snapshot(true));
        fixture_roundtrip(codec, membership_snapshot(false));
        let frame = codec
            .encode_batch(scope(), std::slice::from_ref(&grant))
            .unwrap();
        let end = frame.len() - 5;
        assert!(codec
            .decode_batch(scope(), &change(&frame, end, &[2]))
            .is_err());
        for cut in 0..frame.len() {
            assert!(codec.decode_batch(scope(), &frame[..cut]).is_err());
        }
        let mut invalid = Vec::new();
        let mut wrong = query.clone();
        wrong.context.origin.session = StoreSession::new(9).unwrap();
        invalid.push(wrong);
        let mut wrong = query.clone();
        if let Rpc::AuthorityRequest { candidate, .. } = &mut wrong.rpc {
            candidate.node = wrong.from;
        }
        invalid.push(wrong);
        let mut wrong = grant.clone();
        if let Rpc::AuthorityReply {
            committed_index, ..
        } = &mut wrong.rpc
        {
            *committed_index = 0;
        }
        invalid.push(wrong);
        let mut wrong = grant.clone();
        if let Rpc::AuthorityReply { committed_term, .. } = &mut wrong.rpc {
            *committed_term = 4;
        }
        invalid.push(wrong);
        let mut wrong = grant;
        if let Rpc::AuthorityReply { granted, .. } = &mut wrong.rpc {
            *granted = false;
        }
        invalid.push(wrong);
        for input in invalid {
            assert!(codec.encode_batch(scope(), &[input]).is_err());
        }
        let narrow = NativeWireCodec::with_authority(WireLimits {
            max_messages: 1,
            ..WireLimits::default()
        })
        .unwrap();
        assert!(narrow
            .encode_batch(scope(), &[query.clone(), query])
            .is_err());
    }
}
