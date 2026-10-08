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
#![cfg(feature = "native")]
mod support;
use support::*;
use voteboat::{contracts::*, log::*, native::log_store::*};

fn compacted_log_conformance<S: LogStore>(mut store: S) {
    use voteboat::{identity::SnapshotGeneration, snapshot::SnapshotRef};
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let state = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &state,
            1,
            3,
            Some(Suffix {
                from: 1,
                entries: (1..=4).map(|i| entry(i, 1, i as u8)).collect(),
            }),
        )],
    );
    let old = store.state(group(1)).unwrap();
    let reference = SnapshotRef {
        store: store.binding().identity,
        group: group(1),
        configuration: old.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 2,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    };
    let compact = LogUpdate {
        group: group(1),
        expected_revision: old.revision,
        hard_state: old.hard_state,
        commit_index: 3,
        suffix: None,
        snapshot: Some(reference),
    };
    // Storage validates transitions; the common driver separately supplies the
    // durable snapshot pin before submitting this raw log mutation.
    append(&mut store, vec![LogMutation::Update(compact)]);
    let base = store.state(group(1)).unwrap();
    assert_eq!(base.entries, vec![entry(3, 1, 3), entry(4, 1, 4)]);
    assert_eq!(base.term_at(2), Some(1));
    assert_eq!(base.term_at(1), None);
    assert_eq!(
        store.fetch_range(group(1), old.generation, 3, 2, 1024),
        Err(StorageError::StaleTicket)
    );
    assert_eq!(
        store.fetch_range(group(1), base.generation, 2, 2, 1024),
        Err(StorageError::Compacted { first_index: 3 })
    );
    for (index, term, owner) in [(1, 1, 1), (3, 2, 1), (3, 1, 2)] {
        let mut bad = reference;
        bad.index = index;
        bad.term = term;
        bad.store = identity(owner);
        let mutation = LogUpdate {
            group: group(1),
            expected_revision: base.revision,
            hard_state: HardState {
                term: 2,
                voted_for: None,
            },
            commit_index: 3,
            suffix: None,
            snapshot: Some(bad),
        };
        assert!(store
            .append_batch(vec![LogMutation::Update(mutation)])
            .is_err());
        assert_eq!(store.state(group(1)).unwrap(), base);
    }
    assert!(store
        .append_batch(vec![update(
            &base,
            2,
            3,
            Some(Suffix {
                from: 3,
                entries: vec![]
            })
        )])
        .is_err());
    append(
        &mut store,
        vec![update(
            &base,
            2,
            3,
            Some(Suffix {
                from: 4,
                entries: vec![entry(4, 2, 9)],
            }),
        )],
    );
    let repaired = store.state(group(1)).unwrap();
    assert_eq!(repaired.snapshot, Some(reference));
    assert_eq!(
        store
            .fetch_range(group(1), repaired.generation, 3, 2, 1024)
            .unwrap(),
        vec![entry(3, 1, 3), entry(4, 2, 9)]
    );
}

#[test]
fn compacted_log_ranges_and_committed_prefix_protection_with_native_and_host() {
    compacted_log_conformance(HostLogStore::new(1));
    compacted_log_conformance(
        NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap(),
    );
}

fn conformance<S: LogStore>(mut store: S) {
    assert!(store.state(group(1)).is_err());
    let tickets = store
        .append_batch(vec![
            LogMutation::Create(bootstrap(1, 3)),
            LogMutation::Create(bootstrap(2, 3)),
        ])
        .unwrap();
    assert!(store.state(group(1)).is_err());
    store.barrier(&tickets).unwrap();
    let initial = store.state(group(1)).unwrap();
    assert_eq!(initial.bootstrap, bootstrap(1, 3));
    let tickets = append(
        &mut store,
        vec![update(
            &initial,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 10), entry(2, 1, 20)],
            }),
        )],
    );
    let old = store.state(group(1)).unwrap();
    assert_eq!(old.commit_index, 1);
    assert_eq!(old.last_index(), 2);
    assert_eq!(
        store
            .fetch_range(group(1), old.generation, 1, 1, 100)
            .unwrap(),
        vec![entry(1, 1, 10)]
    );
    assert!(store
        .fetch_range(group(1), old.generation, 1, 1, 1)
        .is_err());
    append(
        &mut store,
        vec![update(
            &old,
            2,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![entry(2, 2, 30), entry(3, 2, 40)],
            }),
        )],
    );
    let new = store.state(group(1)).unwrap();
    assert!(new.generation > old.generation);
    assert_eq!(
        new.entries,
        vec![entry(1, 1, 10), entry(2, 2, 30), entry(3, 2, 40)]
    );
    assert_eq!(store.barrier(&tickets), Err(StorageError::StaleTicket));
    assert_eq!(
        store.fetch_range(group(1), old.generation, 1, 1, 100),
        Err(StorageError::StaleTicket)
    );
    for invalid in [
        update(
            &new,
            2,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![],
            }),
        ),
        update(&new, 2, 0, None),
        update(&new, 1, 1, None),
        update(&new, 2, 4, None),
        update(
            &new,
            2,
            1,
            Some(Suffix {
                from: 5,
                entries: vec![],
            }),
        ),
        update(&old, 2, 1, None),
    ] {
        assert!(store.append_batch(vec![invalid]).is_err());
        assert_eq!(store.state(group(1)).unwrap(), new);
    }
    let other = store.state(group(2)).unwrap();
    assert!(store
        .append_batch(vec![update(&other, 1, 0, None), update(&new, 2, 0, None)])
        .is_err());
    assert_eq!(store.state(group(2)).unwrap(), other);
    assert!(store
        .append_batch(vec![LogMutation::Create(bootstrap(1, 3))])
        .is_err());
}

#[test]
fn public_store_conformance_runs_against_native_and_host() {
    conformance(
        NativeLogStore::create(ModelIo::default(), identity(1), LogLimits::default()).unwrap(),
    );
    conformance(HostLogStore::new(1));
}
#[test]
fn restart_recovers_policy_identity_hard_state_suffix_and_commit() {
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let s = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &s,
            2,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 1), entry(2, 2, 2)],
            }),
        )],
    );
    let s = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &s,
            3,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![entry(2, 3, 3)],
            }),
        )],
    );
    let expected = store.state(group(1)).unwrap();
    let binding = store.binding();
    drop(store);
    io.0.borrow_mut().power_loss();
    let store = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
    assert_eq!(store.state(group(1)).unwrap(), expected);
    assert!(store.binding().session > binding.session);
}
#[test]
fn suffix_fence_invalidates_uncompleted_old_generation() {
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let s = store.state(group(1)).unwrap();
    let old = store
        .append_batch(vec![update(
            &s,
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 1)],
            }),
        )])
        .unwrap();
    // The accepted revision is carried by the ticket. A host driving more than
    // one unit supplies that revision; the durable range remains unchanged.
    let replacement = LogMutation::Update(LogUpdate {
        snapshot: None,
        group: group(1),
        expected_revision: old[0].revision,
        hard_state: HardState {
            term: 2,
            voted_for: None,
        },
        commit_index: 0,
        suffix: Some(Suffix {
            from: 1,
            entries: vec![entry(1, 2, 2)],
        }),
    });
    let new = store.append_batch(vec![replacement]).unwrap();
    assert_eq!(store.barrier(&old), Err(StorageError::StaleTicket));
    store.barrier(&new).unwrap();
    assert_eq!(store.state(group(1)).unwrap().entries, vec![entry(1, 2, 2)]);
}
#[test]
fn every_interrupted_replacement_is_atomic_and_never_loses_committed_prefix() {
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let s = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &s,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 1), entry(2, 1, 2)],
            }),
        )],
    );
    let old = store.state(group(1)).unwrap();
    let old_len = io.0.borrow().log.len();
    let old_manifest = io.0.borrow().manifest.clone();
    store
        .append_batch(vec![update(
            &old,
            2,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![entry(2, 2, 3), entry(3, 2, 4)],
            }),
        )])
        .unwrap();
    let full = io.0.borrow().log.clone();
    drop(store);
    for length in old_len..=full.len() {
        let image = ModelIo::default();
        image.0.borrow_mut().log = full[..length].to_vec();
        image.0.borrow_mut().manifest = old_manifest.clone();
        let recovered = NativeLogStore::recover(image, identity(1), LogLimits::default())
            .unwrap()
            .state(group(1))
            .unwrap();
        assert_eq!(recovered.entries[0], entry(1, 1, 1));
        assert_eq!(recovered.commit_index, 1);
        if length < full.len() {
            assert_eq!(recovered, old);
        } else {
            assert_eq!(recovered.entries[1], entry(2, 2, 3));
        }
    }
    for length in 0..old_len {
        let image = ModelIo::default();
        image.0.borrow_mut().log = full[..length].to_vec();
        image.0.borrow_mut().manifest = old_manifest.clone();
        assert!(NativeLogStore::recover(image, identity(1), LogLimits::default()).is_err());
    }
}
#[test]
fn failed_sync_or_publication_fences_without_acknowledging_the_transition() {
    for fault in [
        Fault::Append(10),
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ] {
        let io = ModelIo::default();
        let mut store =
            NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
        append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
        let s = store.state(group(1)).unwrap();
        io.0.borrow_mut().fault = fault;
        match store.append_batch(vec![update(
            &s,
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 1)],
            }),
        )]) {
            Err(StorageError::Uncertain(_)) => (),
            Ok(tickets) => assert!(matches!(
                store.barrier(&tickets),
                Err(StorageError::Uncertain(_))
            )),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
        drop(store);
        io.0.borrow_mut().power_loss();
        let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        let s = recovered.state(group(1)).unwrap();
        assert!(s.entries.is_empty() || s.entries == vec![entry(1, 1, 1)]);
    }
}
#[test]
fn codec_roundtrips_rejects_every_single_bit_mutation_and_unbounded_lengths() {
    let limits = LogLimits::default();
    let b = bootstrap(1, 3);
    let encoded = NativeLogCodec
        .encode_batch(1, &[LogMutation::Create(b.clone())], limits)
        .unwrap();
    assert_eq!(
        NativeLogCodec.decode_batch(&encoded, limits).unwrap(),
        (1, vec![LogMutation::Create(b)])
    );
    for bit in 0..encoded.len() * 8 {
        let mut bytes = encoded.clone();
        bytes[bit / 8] ^= 1 << (bit % 8);
        assert!(NativeLogCodec.decode_batch(&bytes, limits).is_err());
    }
    for len in 0..encoded.len() {
        assert!(NativeLogCodec
            .decode_batch(&encoded[..len], limits)
            .is_err());
    }
}

#[test]
fn valid_checksums_cannot_hide_unbounded_policy_or_command_lengths() {
    fn crc(b: &[u8]) -> u32 {
        let mut crc = !0u32;
        for byte in b {
            crc ^= *byte as u32;
            for _ in 0..8 {
                crc = (crc >> 1) ^ (0x82f63b78 & 0u32.wrapping_sub(crc & 1));
            }
        }
        !crc
    }
    fn reseal(b: &mut [u8]) {
        let end = b.len() - 16;
        let header = crc(&b[..24]);
        b[24..28].copy_from_slice(&header.to_le_bytes());
        let checksum = crc(&b[..end]);
        b[end + 8..end + 12].copy_from_slice(&checksum.to_le_bytes());
    }
    let limits = LogLimits::default();
    let mut bytes = NativeLogCodec
        .encode_batch(1, &[LogMutation::Create(bootstrap(1, 3))], limits)
        .unwrap();
    // Header, tag, group identity, configuration, majority tag, child count.
    let children = 32 + 1 + 24 + 8 + 1;
    bytes[children..children + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    reseal(&mut bytes);
    assert!(matches!(
        NativeLogCodec.decode_batch(&bytes, limits),
        Err(StorageError::Corrupt("policy child limit"))
    ));
    let mutation = LogMutation::Update(LogUpdate {
        snapshot: None,
        group: group(1),
        expected_revision: voteboat::identity::LogRevision::new(1).unwrap(),
        hard_state: HardState {
            term: 1,
            voted_for: None,
        },
        commit_index: 0,
        suffix: Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1, 1)],
        }),
    });
    let mut bytes = NativeLogCodec.encode_batch(2, &[mutation], limits).unwrap();
    let command_len = 32 + 1 + 24 + 8 + 8 + 8 + 8 + 1 + 8 + 4 + 8 + 8 + 1 + 16;
    bytes[command_len..command_len + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    reseal(&mut bytes);
    assert!(matches!(
        NativeLogCodec.decode_batch(&bytes, limits),
        Err(StorageError::Corrupt("command budget"))
    ));
}
#[test]
fn user_commands_leave_control_reserve_and_admission_is_bounded() {
    let limits = LogLimits {
        max_wal_bytes: 1200,
        control_reserve_bytes: 600,
        max_batch_bytes: 512,
        max_command_bytes: 128,
        max_batch_units: 1,
        max_pending_units: 1,
        ..LogLimits::default()
    };
    let mut store = NativeLogStore::create(ModelIo::default(), identity(1), limits).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let s = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &s,
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 1)],
            }),
        )],
    );
    loop {
        let s = store.state(group(1)).unwrap();
        let from = s.last_index() + 1;
        assert!(from < 20, "application writes never reached their limit");
        match store.append_batch(vec![update(
            &s,
            1,
            0,
            Some(Suffix {
                from,
                entries: vec![entry(from, 1, 2)],
            }),
        )]) {
            Ok(tickets) => {
                store.barrier(&tickets).unwrap();
            }
            Err(StorageError::Rejected(_)) => break,
            other => panic!("unexpected admission result {other:?}"),
        }
    }
    let s = store.state(group(1)).unwrap();
    append(&mut store, vec![update(&s, 2, 0, None)]);
    assert_eq!(store.state(group(1)).unwrap().hard_state.term, 2);
}
