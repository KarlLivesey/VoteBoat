// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{generator::*, support::*};
use voteboat::{log::*, native::log_store::*};
fn limits() -> LogLimits {
    LogLimits {
        max_groups: 4,
        max_entries_per_group: 8,
        max_command_bytes: 64,
        max_snapshot_bytes: 512,
        max_batch_bytes: 4096,
        max_batch_units: 8,
        max_pending_units: 16,
        max_wal_bytes: 16384,
        control_reserve_bytes: 4096,
    }
}
fn reseal(bytes: &mut [u8]) {
    let header = crc(&bytes[..24]).to_le_bytes();
    bytes[24..28].copy_from_slice(&header);
    let end = bytes.len() - 16;
    let footer = crc(&bytes[..end]).to_le_bytes();
    bytes[end + 8..end + 12].copy_from_slice(&footer);
}
fn batch_corpus(mutations: Vec<LogMutation>, name: &str) {
    let original = NativeLogCodec
        .encode_batch(1, &mutations, limits())
        .unwrap();
    campaign(
        name,
        &original,
        &[(8, 8), (16, 4), (20, 4), (32, 1)],
        |case| {
            let mut bytes = case.bytes.clone();
            if case.repair() {
                reseal(&mut bytes);
            }
            let result = NativeLogCodec.decode_batch(&bytes, limits());
            if case.original() {
                assert_eq!(result, Ok((1, mutations.clone())));
            }
            if case.integrity_damage() {
                assert!(result.is_err());
            }
            match result {
                Ok((sequence, mutations)) => {
                    assert!(!mutations.is_empty() && mutations.len() <= limits().max_batch_units);
                    let encoded = NativeLogCodec
                        .encode_batch(sequence, &mutations, limits())
                        .unwrap();
                    assert!(encoded.len() <= limits().max_batch_bytes);
                    assert_eq!(
                        NativeLogCodec.decode_batch(&encoded, limits()),
                        Ok((sequence, mutations))
                    );
                    true
                }
                Err(_) => false,
            }
        },
    );
}
fn populated() -> (ModelIo, GroupLog) {
    let image = ModelIo::default();
    let mut store = NativeLogStore::create(image.clone(), identity(1), limits()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let initial = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &initial,
            1,
            2,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 7), entry(2, 1, 3)],
            }),
        )],
    );
    let expected = store.state(group(1)).unwrap();
    drop(store);
    (image, expected)
}
fn recover(log: Vec<u8>, manifest: Vec<u8>) -> Result<GroupLog, voteboat::contracts::StorageError> {
    let image = ModelIo::default();
    {
        let mut device = image.0.borrow_mut();
        device.log = log;
        device.manifest = Some(manifest);
    }
    let store = NativeLogStore::recover(image, identity(1), limits())?;
    assert_eq!(store.binding().session.get(), 2);
    store.state(group(1))
}
#[test]
fn generated_native_batch_mutations_reach_bounded_inner_decoders() {
    batch_corpus(
        vec![LogMutation::Create(bootstrap(1, 3))],
        "wal-create-batch",
    );
    let (_, state) = populated();
    let mut command = entry(3, 2, 7);
    let EntryPayload::Command { bytes, .. } = &mut command.payload else {
        unreachable!()
    };
    *bytes = vec![0x91; 16].into();
    batch_corpus(
        vec![update(
            &state,
            2,
            3,
            Some(Suffix {
                from: 3,
                entries: vec![command],
            }),
        )],
        "wal-update-batch",
    );
}
#[test]
fn generated_acknowledged_wal_and_manifest_corruption_never_recovers_a_partial_prefix() {
    let (image, expected) = populated();
    let log = image.0.borrow().log.clone();
    let manifest = image.0.borrow().manifest.clone().unwrap();
    campaign(
        "wal-acknowledged-recovery",
        &log,
        &[(16, 4), (20, 4)],
        |case| {
            let result = recover(case.bytes.clone(), manifest.clone());
            if case.integrity_damage() {
                assert!(result.is_err());
            }
            if case.original() {
                assert_eq!(result, Ok(expected.clone()));
            }
            if let Ok(state) = &result {
                assert_eq!(*state, expected);
            }
            result.is_ok()
        },
    );
    campaign(
        "wal-manifest-recovery",
        &manifest,
        &[(8, 8), (24, 8), (32, 8), (40, 8)],
        |case| {
            let result = recover(log.clone(), case.bytes.clone());
            if case.integrity_damage() {
                assert!(result.is_err());
            }
            if case.original() {
                assert_eq!(result, Ok(expected.clone()));
            }
            if let Ok(state) = &result {
                assert_eq!(*state, expected);
            }
            result.is_ok()
        },
    );
}

#[test]
fn generated_interrupted_unacknowledged_suffixes_recover_only_the_original_prefix() {
    let (image, expected) = populated();
    let log = image.0.borrow().log.clone();
    let manifest = image.0.borrow().manifest.clone().unwrap();
    let next = NativeLogCodec
        .encode_batch(3, &[update(&expected, 2, 2, None)], limits())
        .unwrap();
    // Nothing past the recovery boundary was acknowledged: every interrupted
    // or damaged suffix must recover exactly the acknowledged prefix.
    campaign_all_accepted(
        "wal-interrupted-suffix",
        &next,
        &[(16, 4), (20, 4)],
        |case| {
            let cut = usize::try_from(case.seed % u64::try_from(next.len()).unwrap())
                .unwrap()
                .wrapping_add(case.index)
                % next.len();
            let tail = &case.bytes[..cut.min(case.bytes.len())];
            let mut bytes = log.clone();
            bytes.extend(tail);
            let result = recover(bytes, manifest.clone());
            if case.original() {
                assert_eq!(result, Ok(expected.clone()));
            }
            if let Ok(state) = &result {
                assert_eq!(*state, expected);
            }
            result.is_ok()
        },
    );
}
