// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::native::{log_store::FileLogIo, vote_store::crc32c};

#[test]
fn fixed_format_rejects_checksum_valid_invalid_metadata_and_contradictions() {
    let path = Directory::new();
    let store = populated(&path.0);
    let original = store.io.primary.unwrap();
    let encoded = original.encode(1);
    assert_eq!(format::decode(&encoded, 1, 0).unwrap(), Some(original));
    for byte in [0, 8, 12, 16, 24, 32, 48, 56, 72] {
        let mut damaged = encoded;
        match byte {
            24 | 32 | 48 | 56 => {
                let width = if byte == 32 { 16 } else { 8 };
                damaged[byte..byte + width].fill(0);
            }
            _ => damaged[byte] ^= 1,
        }
        let crc = crc32c(&damaged[..124]);
        damaged[124..].copy_from_slice(&crc.to_le_bytes());
        assert!(format::decode(&damaged, 1, 0).is_err(), "field {byte}");
    }
    let mut bad = encoded;
    bad[124] ^= 1;
    assert_eq!(format::decode(&bad, 1, 0).unwrap(), None);
    let mut ahead = original;
    ahead.counter += 20;
    ahead.binding.session = StoreSession::new(original.binding.session.get() + 1).unwrap();
    assert_eq!(
        format::mandatory(Some(ahead), Some(original)).unwrap(),
        original
    );
    assert!(format::mandatory(Some(original), Some(ahead)).is_err());
    ahead.counter = original.counter;
    assert!(format::mandatory(Some(ahead), Some(original)).is_err());
    ahead = original;
    ahead.counter += 1;
    ahead.length -= 1;
    assert!(format::mandatory(Some(ahead), Some(original)).is_err());
    ahead = original;
    ahead.counter += 1;
    ahead.binding.session = StoreSession::new(original.binding.session.get() + 2).unwrap();
    assert!(format::mandatory(Some(ahead), Some(original)).is_err());
}
#[test]
fn selected_missing_corrupt_or_legacy_files_never_fall_back() {
    let path = Directory::new();
    drop(populated(&path.0));
    assert!(FileLogIo::create(&path.0).is_err());
    assert!(NativeLogStore::recover(
        FileLogIo::open(&path.0).unwrap(),
        identity(),
        LogLimits::default(),
    )
    .is_err());
    assert!(WitnessLogIo::create(&path.0).is_err());
    let selector = fs::read(path.0.join("WITNESS-CURRENT")).unwrap();
    for cut in 0..selector.len() {
        fs::write(path.0.join("WITNESS-CURRENT"), &selector[..cut]).unwrap();
        assert!(WitnessLogIo::open(&path.0).is_err());
    }
    fs::remove_file(path.0.join("WITNESS-CURRENT")).unwrap();
    assert!(WitnessLogIo::open(&path.0).is_err());
    fs::write(path.0.join("WITNESS-CURRENT"), selector).unwrap();
    fs::write(path.0.join("CURRENT"), b"legacy").unwrap();
    assert!(WitnessLogIo::open(&path.0).is_err());
    fs::remove_file(path.0.join("CURRENT")).unwrap();
    fs::remove_file(path.0.join("MANIFEST")).unwrap();
    assert!(WitnessLogIo::open(&path.0).is_err());
    let legacy = Directory::new();
    drop(FileLogIo::create(&legacy.0).unwrap());
    assert!(WitnessLogIo::create(&legacy.0).is_err());
    assert!(WitnessLogIo::open(&legacy.0).is_err());
}
#[test]
fn checked_counters_bounds_and_lock_survive_errors() {
    let path = Directory::new();
    let mut store = populated(&path.0);
    assert!(WitnessLogIo::open(&path.0).is_err());
    assert!(store.io.truncate_log(u64::MAX).is_err());
    assert!(store.io.read_log(0).is_err());
    let mut record = store.io.primary.unwrap();
    record.counter = u64::MAX;
    store.io.primary = Some(record);
    store.io.secondary = Some(record);
    let metadata = crate::native::log_store::manifest(store.binding(), store.length as u64);
    assert!(store.io.publish_barrier(&metadata).is_err());
    assert!(WitnessLogIo::open(&path.0).is_err());
    record.counter = 1;
    store.io.primary = Some(record);
    store.io.secondary = Some(record);
    store.io.generation = u64::MAX;
    assert!(store
        .io
        .replace_log(b"", &crate::native::log_store::manifest(store.binding(), 0))
        .is_err());
}
