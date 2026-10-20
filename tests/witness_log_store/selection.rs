// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::fs;

#[test]
fn exclusive_lifetime_and_explicit_legacy_provider_incompatibility() {
    let root = Directory::new("lock");
    let store = populated(&root);
    assert!(WitnessLogIo::open(&root.0).is_err());
    assert!(WitnessLogIo::create(&root.0).is_err());
    assert!(FileLogIo::open(&root.0).is_err());
    drop(store);
    assert!(FileLogIo::create(&root.0).is_err());
    // The format-aware FileLogIo selects the witness reader; it never reads
    // witness files as the legacy layout.
    let expected = recover(&root).unwrap().state(group(1)).unwrap();
    let selected = FileLogIo::open(&root.0).unwrap();
    assert!(selected.is_witness());
    let store = NativeLogStore::recover(selected, identity(1), LogLimits::default()).unwrap();
    assert_eq!(store.state(group(1)).unwrap().entries, expected.entries);
    drop(store);
    let legacy = Directory::new("legacy");
    let store = NativeLogStore::create(
        FileLogIo::create(&legacy.0).unwrap(),
        identity(1),
        LogLimits::default(),
    )
    .unwrap();
    drop(store);
    assert!(WitnessLogIo::create(&legacy.0).is_err());
    assert!(WitnessLogIo::open(&legacy.0).is_err());
}

#[test]
fn selected_missing_files_and_invalid_current_never_fall_back_to_old_pairs() {
    let root = Directory::new("selection");
    let mut store = populated(&root);
    let old_wal = fs::read(root.0.join("log.wal")).unwrap();
    let old_metadata = fs::read(root.0.join("MANIFEST")).unwrap();
    for _ in 0..4 {
        let s = store.state(group(1)).unwrap();
        append(&mut store, vec![update(&s, 2, 1, None)]);
    }
    store.reclaim(store.limits().max_wal_bytes).unwrap();
    drop(store);
    let (wal_path, metadata_path) = selected(&root);
    let wal = fs::read(&wal_path).unwrap();
    let metadata = fs::read(&metadata_path).unwrap();
    fs::write(root.0.join("log.wal"), old_wal).unwrap();
    fs::write(root.0.join("MANIFEST"), old_metadata).unwrap();
    for path in [&wal_path, &metadata_path] {
        fs::remove_file(path).unwrap();
        assert!(recover(&root).is_err());
        fs::write(path, if path == &wal_path { &wal } else { &metadata }).unwrap();
    }
    let current_path = root.0.join("WITNESS-CURRENT");
    let current = fs::read(&current_path).unwrap();
    fs::remove_file(&current_path).unwrap();
    assert!(recover(&root).is_err());
    for bytes in [b"bad selection".to_vec(), current[..19].to_vec()] {
        fs::write(&current_path, bytes).unwrap();
        assert!(recover(&root).is_err());
    }
    let mut wrong = current.clone();
    wrong[..8].copy_from_slice(b"VBLCUR01");
    rechecksum(&mut wrong);
    fs::write(&current_path, wrong).unwrap();
    assert!(recover(&root).is_err());
    fs::write(&current_path, current).unwrap();
    assert!(recover(&root).is_ok());
}

#[test]
fn complete_unacknowledged_suffix_is_recovered_and_only_incomplete_tail_is_discarded() {
    let root = Directory::new("tail");
    let mut store = populated(&root);
    let before = store.state(group(1)).unwrap();
    let pending = store
        .append_batch(vec![update(
            &before,
            2,
            2,
            Some(Suffix {
                from: 2,
                entries: vec![entry(2, 2, 73)].into(),
            }),
        )])
        .unwrap();
    assert_eq!(store.state(group(1)).unwrap(), before);
    drop(store);
    let mut reopened = recover(&root).unwrap();
    let expected = reopened.state(group(1)).unwrap();
    assert_eq!(expected.entries, vec![entry(1, 1, 17), entry(2, 2, 73)]);
    assert_eq!(reopened.barrier(&pending), Err(StorageError::StaleTicket));
    drop(reopened);
    let (wal_path, _) = selected(&root);
    use std::io::Write;
    fs::OpenOptions::new()
        .append(true)
        .open(wal_path)
        .unwrap()
        .write_all(&[1, 2, 3])
        .unwrap();
    let reopened = recover(&root).unwrap();
    assert_eq!(reopened.state(group(1)).unwrap(), expected);
}

#[test]
fn failed_or_partial_initialization_is_never_opened_as_an_empty_voter() {
    let root = Directory::new("initialization");
    let io = WitnessLogIo::create(&root.0).unwrap();
    drop(io);
    assert!(WitnessLogIo::open(&root.0).is_err());
    assert!(WitnessLogIo::create(&root.0).is_err());
}
