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
use voteboat::{contracts::*, identity::*, log::*, membership::*, quorum::*, raft::*};
fn cid(n: u64) -> ConfigurationId {
    ConfigurationId::new(n).unwrap()
}
fn configuration(id: u64, voters: &[u64], learners: &[u64]) -> Configuration {
    Configuration::new(
        cid(id),
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
fn configuration_entry(
    index: u64,
    term: u64,
    op: u128,
    expected: u64,
    change: ConfigurationChange,
) -> LogEntry {
    LogEntry {
        index,
        term,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(op).unwrap(),
            expected: cid(expected),
            change,
        })),
    }
}
fn mutation(
    s: &GroupLog,
    term: u64,
    vote: Option<u64>,
    commit: u64,
    suffix: Option<Suffix>,
) -> LogMutation {
    let LogMutation::Update(mut u) = update(s, term, commit, suffix) else {
        unreachable!()
    };
    u.hard_state.voted_for = vote.map(node);
    LogMutation::Update(u)
}
fn staged<S: LogStore>(store: &mut S) {
    append(store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let s = store.state(group(1)).unwrap();
    append(
        store,
        vec![mutation(
            &s,
            1,
            None,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![configuration_entry(
                    1,
                    1,
                    100,
                    1,
                    ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4, 5])),
                )],
            }),
        )],
    );
}
fn joint<S: LogStore>(store: &mut S) {
    staged(store);
    let s = store.state(group(1)).unwrap();
    append(
        store,
        vec![mutation(
            &s,
            1,
            None,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![configuration_entry(
                    2,
                    1,
                    101,
                    2,
                    ConfigurationChange::Joint {
                        id: cid(3),
                        next: configuration(4, &[3, 4, 5], &[1, 2]),
                    },
                )],
            }),
        )],
    );
}
fn ballot<S: LogStore>(store: &mut S) {
    let s = store.state(group(1)).unwrap();
    append(store, vec![mutation(&s, 2, Some(4), s.commit_index, None)]);
}
fn check_promise(s: &GroupLog) {
    assert_eq!(
        s.hard_state,
        HardState {
            term: 2,
            voted_for: Some(node(4))
        }
    );
    assert_eq!(
        s.ballot_origin,
        Some(BallotOrigin {
            configuration: cid(3),
            candidate_store: identity(4)
        })
    );
    s.validate_ballot().unwrap();
}
fn rejected<S: LogStore>(store: &mut S, m: LogMutation) {
    let old = store.state(group(1)).unwrap();
    assert!(store.append_batch(vec![m]).is_err());
    assert_eq!(store.state(group(1)).unwrap(), old);
}
fn rollback_conformance<S: LogStore>(store: &mut S) {
    staged(store);
    let s = store.state(group(1)).unwrap();
    rejected(store, mutation(&s, 2, Some(4), 1, None)); // staged learner is not a voter
                                                        // A new joint suffix cannot authorize its own ballot in that same unit.
    rejected(
        store,
        mutation(
            &s,
            2,
            Some(4),
            1,
            Some(Suffix {
                from: 2,
                entries: vec![configuration_entry(
                    2,
                    1,
                    101,
                    2,
                    ConfigurationChange::Joint {
                        id: cid(3),
                        next: configuration(4, &[3, 4, 5], &[1, 2]),
                    },
                )],
            }),
        ),
    );
    append(
        store,
        vec![mutation(
            &s,
            1,
            None,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![configuration_entry(
                    2,
                    1,
                    101,
                    2,
                    ConfigurationChange::Joint {
                        id: cid(3),
                        next: configuration(4, &[3, 4, 5], &[1, 2]),
                    },
                )],
            }),
        )],
    );
    ballot(store);
    let s = store.state(group(1)).unwrap();
    check_promise(&s);
    append(
        store,
        vec![mutation(
            &s,
            2,
            Some(4),
            1,
            Some(Suffix {
                from: 2,
                entries: vec![LogEntry {
                    index: 2,
                    term: 2,
                    payload: EntryPayload::Noop,
                }],
            }),
        )],
    );
    let s = store.state(group(1)).unwrap();
    check_promise(&s);
    assert_eq!(s.membership().unwrap().id(), cid(2));
    assert!(!s.membership().unwrap().is_voter(node(4)));
    for candidate in [None, Some(2)] {
        rejected(store, mutation(&s, 2, candidate, 1, None));
    }
    rejected(store, mutation(&s, 3, Some(4), 1, None)); // still only a learner
    append(store, vec![mutation(&s, 3, Some(2), 1, None)]);
    let s = store.state(group(1)).unwrap();
    assert_eq!(
        s.ballot_origin,
        Some(BallotOrigin {
            configuration: cid(2),
            candidate_store: identity(2)
        })
    );
}
fn removal<S: LogStore>(store: &mut S) {
    joint(store);
    ballot(store);
    let s = store.state(group(1)).unwrap();
    append(
        store,
        vec![mutation(
            &s,
            2,
            Some(4),
            2,
            Some(Suffix {
                from: 3,
                entries: vec![configuration_entry(
                    3,
                    2,
                    101,
                    3,
                    ConfigurationChange::Final { id: cid(4) },
                )],
            }),
        )],
    );
    let s = store.state(group(1)).unwrap();
    append(
        store,
        vec![mutation(
            &s,
            2,
            Some(4),
            3,
            Some(Suffix {
                from: 4,
                entries: vec![configuration_entry(
                    4,
                    2,
                    102,
                    4,
                    ConfigurationChange::Joint {
                        id: cid(5),
                        next: configuration(6, &[1, 2, 3], &[]),
                    },
                )],
            }),
        )],
    );
    let s = store.state(group(1)).unwrap();
    append(
        store,
        vec![mutation(
            &s,
            2,
            Some(4),
            4,
            Some(Suffix {
                from: 5,
                entries: vec![configuration_entry(
                    5,
                    2,
                    102,
                    5,
                    ConfigurationChange::Final { id: cid(6) },
                )],
            }),
        )],
    );
    let s = store.state(group(1)).unwrap();
    append(store, vec![mutation(&s, 2, Some(4), 5, None)]);
    let s = store.state(group(1)).unwrap();
    check_promise(&s);
    assert_eq!(s.membership().unwrap().replica_store(node(4)), None);
}
fn compact<S: LogStore>(store: &mut S) {
    use voteboat::snapshot::SnapshotRef;
    let s = store.state(group(1)).unwrap();
    // Logical-store conformance assumes the documented external snapshot pin.
    // This test does not assert application snapshot data has been published.
    append(
        store,
        vec![LogMutation::Update(LogUpdate {
            group: group(1),
            expected_revision: s.revision,
            hard_state: s.hard_state,
            commit_index: 5,
            suffix: None,
            snapshot: Some(SnapshotRef {
                store: store.binding().identity,
                group: group(1),
                configuration: cid(6),
                index: 5,
                term: 2,
                application_schema: 1,
                generation: SnapshotGeneration::new(1).unwrap(),
                file_bytes: 128,
                checksum: 7,
            }),
            snapshot_membership: s.checkpoint_membership(5).unwrap(),
        })],
    );
    let s = store.state(group(1)).unwrap();
    check_promise(&s);
    assert!(s.entries.is_empty());
}
#[test]
fn host_new_ballot_eligibility_and_rollback_promise_conformance() {
    rollback_conformance(&mut HostLogStore::new(1));
}
#[test]
fn host_removal_and_compaction_keep_the_historical_promise() {
    let mut store = HostLogStore::new(1);
    removal(&mut store);
    compact(&mut store);
    let s = store.state(group(1)).unwrap();
    for candidate in [None, Some(2)] {
        rejected(&mut store, mutation(&s, 2, candidate, 5, None));
    }
}
#[test]
fn ballot_origin_is_checked_on_recovery_without_granting_current_authority() {
    let mut historical = HostLogStore::new(1);
    removal(&mut historical);
    let removed = historical.state(group(1)).unwrap();
    check_promise(&removed);
    assert_eq!(removed.membership().unwrap().voter_store(node(4)), None);
    assert!(matches!(
        Raft::recover(node(1), historical.binding(), removed, historical.limits()),
        Err(RaftError::InvalidRecovery)
    )); // online recovery gate remains
    let mut store = HostLogStore::new(1);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let initial = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![mutation(
            &initial,
            2,
            Some(2),
            0,
            Some(Suffix {
                from: 1,
                entries: vec![LogEntry {
                    index: 1,
                    term: 2,
                    payload: EntryPayload::Noop,
                }],
            }),
        )],
    );
    let static_state = store.state(group(1)).unwrap();
    let mut core = Raft::recover(
        node(1),
        store.binding(),
        static_state.clone(),
        store.limits(),
    )
    .unwrap();
    let candidate = HostLogStore::new(3).binding();
    let message = Message {
        group: group(1),
        configuration: cid(1),
        from: node(3),
        sender: candidate,
        to: node(1),
        term: 2,
        context: RequestContext {
            origin: candidate,
            sequence: 1,
        },
        rpc: Rpc::Vote {
            last_index: 1,
            last_term: 2,
        },
    };
    assert!(matches!(
        &core.step(Event::Receive(message)).unwrap()[..],
        [Effect::Send(Message {
            rpc: Rpc::Voted { granted: false },
            ..
        })]
    ));
    for corrupt in 0..3 {
        let mut invalid = static_state.clone();
        match corrupt {
            0 => invalid.ballot_origin = None,
            1 => invalid.hard_state.voted_for = None,
            _ => invalid.ballot_origin.as_mut().unwrap().candidate_store = identity(99),
        }
        assert!(invalid.validate_ballot().is_err());
        assert!(matches!(
            Raft::recover(node(1), store.binding(), invalid, store.limits()),
            Err(RaftError::InvalidRecovery)
        ));
    }
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use voteboat::native::log_store::*;
    fn crc(bytes: &[u8]) -> u32 {
        let mut value = !0u32;
        for byte in bytes {
            value ^= u32::from(*byte);
            for _ in 0..8 {
                value = (value >> 1) ^ (0x82f63b78 & 0u32.wrapping_sub(value & 1));
            }
        }
        !value
    }
    fn reseal(image: &mut [u8]) {
        let header = crc(&image[..24]);
        image[24..28].copy_from_slice(&header.to_le_bytes());
        let end = image.len() - 16;
        let checksum = crc(&image[..end]);
        image[end + 8..end + 12].copy_from_slice(&checksum.to_le_bytes());
    }
    #[test]
    fn historical_ballot_image_rejects_torn_corrupt_and_invalid_origins() {
        let mut store =
            NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap();
        removal(&mut store);
        compact(&mut store);
        let codec = NativeLogCodec;
        let limits = store.limits();
        let state = store.state(group(1)).unwrap();
        let image = codec
            .encode_checkpoint(
                10,
                &[(group(1), state)].into(),
                limits,
                limits.max_wal_bytes,
            )
            .unwrap();
        for cut in 0..image.len() {
            assert!(
                codec.decode_checkpoint(&image[..cut], limits).is_err(),
                "cut {cut}"
            );
        }
        for offset in 0..image.len() {
            for bit in 0..8 {
                let mut bad = image.clone();
                bad[offset] ^= 1 << bit;
                assert!(
                    codec.decode_checkpoint(&bad, limits).is_err(),
                    "byte {offset} bit {bit}"
                );
            }
        }
        let mut origin = vec![1];
        origin.extend(3u64.to_le_bytes());
        origin.extend(4u128.to_le_bytes());
        origin.extend(1u64.to_le_bytes());
        let offsets: Vec<_> = image
            .windows(origin.len())
            .enumerate()
            .filter_map(|(i, bytes)| (bytes == origin).then_some(i))
            .collect();
        assert_eq!(offsets.len(), 1);
        let offset = offsets[0];
        for (start, length) in [(0, 1), (1, 8), (9, 16), (25, 8)] {
            let mut bad = image.clone();
            bad[offset + start..offset + start + length].fill(0);
            reseal(&mut bad);
            assert!(codec.decode_checkpoint(&bad, limits).is_err());
        }
        let mut bad = image.clone();
        bad[offset] = 2;
        reseal(&mut bad);
        assert!(codec.decode_checkpoint(&bad, limits).is_err());
        // A bootstrap-scoped origin must identify that bootstrap candidate's store.
        let mut bad = image.clone();
        bad[offset + 1..offset + 9].copy_from_slice(&1u64.to_le_bytes());
        reseal(&mut bad);
        assert!(codec.decode_checkpoint(&bad, limits).is_err());
        // Both canonical records must describe one exact hard state.
        let mut hard = Vec::from(2u64.to_le_bytes());
        hard.extend(4u64.to_le_bytes());
        let matches: Vec<_> = image
            .windows(hard.len())
            .enumerate()
            .filter_map(|(i, bytes)| (bytes == hard).then_some(i))
            .collect();
        assert_eq!(matches.len(), 2);
        let mut bad = image.clone();
        bad[matches[1] + 8..matches[1] + 16].copy_from_slice(&2u64.to_le_bytes());
        reseal(&mut bad);
        assert!(codec.decode_checkpoint(&bad, limits).is_err());
    }
    #[test]
    fn host_decoder_cannot_publish_structurally_invalid_ballot_recovery() {
        struct WrongOrigin(u8);
        impl LogCodec for WrongOrigin {
            fn format_version(&self) -> u32 {
                2
            }
            fn encode_batch(
                &self,
                sequence: u64,
                mutations: &[LogMutation],
                limits: LogLimits,
            ) -> Result<Vec<u8>, StorageError> {
                NativeLogCodec.encode_batch(sequence, mutations, limits)
            }
            fn frame_length(
                &self,
                header: &[u8],
                limits: LogLimits,
            ) -> Result<usize, StorageError> {
                NativeLogCodec.frame_length(header, limits)
            }
            fn decode_batch(
                &self,
                bytes: &[u8],
                limits: LogLimits,
            ) -> Result<(u64, Vec<LogMutation>), StorageError> {
                NativeLogCodec.decode_batch(bytes, limits)
            }
            fn decode_checkpoint(
                &self,
                bytes: &[u8],
                limits: LogLimits,
            ) -> Result<
                (
                    u64,
                    std::collections::BTreeMap<GroupIdentity, GroupLog>,
                    usize,
                ),
                StorageError,
            > {
                let (sequence, mut state, length) =
                    NativeLogCodec.decode_checkpoint(bytes, limits)?;
                let group = state.get_mut(&group(1)).unwrap();
                match self.0 {
                    0 => group.ballot_origin = None,
                    1 => group.hard_state.voted_for = None,
                    _ => group.ballot_origin.as_mut().unwrap().configuration = cid(1),
                }
                Ok((sequence, state, length))
            }
        }
        let io = ModelIo::default();
        let mut store =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        removal(&mut store);
        compact(&mut store);
        store.reclaim(store.limits().max_wal_bytes).unwrap();
        drop(store);
        let manifest = io.0.borrow().manifest.clone();
        for mode in 0..3 {
            assert!(matches!(
                NativeLogStore::recover_with_codec(
                    io.clone(),
                    identity(1),
                    LogLimits::default(),
                    WrongOrigin(mode)
                ),
                Err(StorageError::Corrupt(_))
            ));
            assert_eq!(io.0.borrow().manifest, manifest);
        }
    }
    #[test]
    fn native_new_ballot_eligibility_and_rollback_promise_conformance() {
        rollback_conformance(
            &mut NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default())
                .unwrap(),
        );
    }
    #[test]
    fn native_image_and_restart_preserve_removed_candidate_after_compaction() {
        let io = ModelIo::default();
        let mut store =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        removal(&mut store);
        compact(&mut store);
        let expected = store.state(group(1)).unwrap();
        let codec = NativeLogCodec;
        let image = codec
            .encode_checkpoint(
                10,
                &[(group(1), expected.clone())].into(),
                store.limits(),
                store.limits().max_wal_bytes,
            )
            .unwrap();
        assert_eq!(&image[..8], b"VBLCPT02");
        let (_, decoded, _) = codec.decode_checkpoint(&image, store.limits()).unwrap();
        assert_eq!(decoded[&group(1)], expected);
        store.reclaim(store.limits().max_wal_bytes).unwrap();
        assert_eq!(&io.0.borrow().log[..8], b"VBLCPT02");
        drop(store);
        io.0.borrow_mut().power_loss();
        let mut store = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        assert_eq!(store.state(group(1)).unwrap(), expected);
        rejected(&mut store, mutation(&expected, 2, Some(2), 5, None));
    }
    #[test]
    fn written_ballot_is_not_durable_and_every_torn_vote_frame_recovers_one_promise() {
        let io = ModelIo::default();
        let mut store =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        joint(&mut store);
        let old = store.state(group(1)).unwrap();
        let before = io.0.borrow().log.len();
        let manifest = io.0.borrow().manifest.clone();
        let tickets = store
            .append_batch(vec![mutation(&old, 2, Some(4), 1, None)])
            .unwrap();
        assert_eq!(store.state(group(1)).unwrap(), old);
        let full = io.0.borrow().log.clone();
        store.barrier(&tickets).unwrap();
        let expected = store.state(group(1)).unwrap();
        check_promise(&expected);
        drop(store);
        for cut in before..=full.len() {
            let image = ModelIo::default();
            image.0.borrow_mut().log = full[..cut].to_vec();
            image.0.borrow_mut().manifest = manifest.clone();
            let recovered = NativeLogStore::recover(image, identity(1), LogLimits::default())
                .unwrap()
                .state(group(1))
                .unwrap();
            assert_eq!(
                recovered,
                if cut == full.len() {
                    expected.clone()
                } else {
                    old.clone()
                },
                "cut {cut}"
            );
        }
    }
    #[test]
    fn failed_ballot_barriers_and_image_replacement_never_erase_a_durable_promise() {
        for fault in [Fault::Sync, Fault::PublishBefore, Fault::PublishAfter] {
            let io = ModelIo::default();
            let mut store =
                NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
            joint(&mut store);
            let old = store.state(group(1)).unwrap();
            let tickets = store
                .append_batch(vec![mutation(&old, 2, Some(4), 1, None)])
                .unwrap();
            io.0.borrow_mut().fault = fault;
            assert!(matches!(
                store.barrier(&tickets),
                Err(StorageError::Uncertain(_))
            ));
            drop(store);
            io.0.borrow_mut().power_loss();
            io.0.borrow_mut().fault = Fault::None;
            let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default())
                .unwrap()
                .state(group(1))
                .unwrap();
            if matches!(fault, Fault::Sync) {
                assert_eq!(recovered, old);
            } else {
                check_promise(&recovered);
            }
        }
        for fault in [Fault::ReplaceBefore, Fault::ReplaceAfter] {
            let io = ModelIo::default();
            let mut store =
                NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
            removal(&mut store);
            compact(&mut store);
            let expected = store.state(group(1)).unwrap();
            io.0.borrow_mut().fault = fault;
            assert!(matches!(
                store.reclaim(store.limits().max_wal_bytes),
                Err(StorageError::Uncertain(_))
            ));
            drop(store);
            io.0.borrow_mut().power_loss();
            io.0.borrow_mut().fault = Fault::None;
            assert_eq!(
                NativeLogStore::recover(io, identity(1), LogLimits::default())
                    .unwrap()
                    .state(group(1))
                    .unwrap(),
                expected
            );
        }
    }
    #[test]
    fn actual_native_file_restart_keeps_ballot_origin_after_full_image_reclaim() {
        let path =
            std::env::temp_dir().join(format!("voteboat-ballot-files-{}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        let mut store = NativeLogStore::create(
            FileLogIo::create(&path).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        removal(&mut store);
        compact(&mut store);
        let expected = store.state(group(1)).unwrap();
        store.reclaim(store.limits().max_wal_bytes).unwrap();
        drop(store);
        let store = NativeLogStore::recover(
            FileLogIo::open(&path).unwrap(),
            identity(1),
            LogLimits::default(),
        )
        .unwrap();
        assert_eq!(store.state(group(1)).unwrap(), expected);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}
