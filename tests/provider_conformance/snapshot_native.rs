// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{snapshot_cases as cases, support::*};
use std::path::PathBuf;
use voteboat::{native::snapshot_store::*, snapshot::*};

fn identity_for(id: u128) -> SnapshotIdentity {
    SnapshotIdentity {
        store: identity(id),
        group: group(1),
    }
}

fn limits() -> SnapshotLimits {
    SnapshotLimits {
        max_application_bytes: 32,
        max_chunk_bytes: 4,
        ..SnapshotLimits::default()
    }
}

fn root(name: &str) -> PathBuf {
    let path =
        std::env::temp_dir().join(format!("voteboat-snapshot-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

#[test]
fn file_snapshots_preserve_same_contract_and_both_anchors_after_reopen() {
    let path = root("anchors");
    let mut store = NativeSnapshotStore::create(
        FileSnapshotIo::create(&path).unwrap(),
        identity_for(1),
        limits(),
    )
    .unwrap();
    cases::stage_identity_and_limits(&mut store);
    cases::abort_preserves_root(&mut store);
    let pins = cases::two_pins(&mut store);
    let binding = store.binding();
    drop(store);
    let mut store = NativeSnapshotStore::recover(
        FileSnapshotIo::open(&path).unwrap(),
        identity_for(1),
        limits(),
    )
    .unwrap();
    assert_eq!(store.binding().identity, binding.identity);
    assert!(store.binding().session > binding.session);
    cases::assert_two_pins(&mut store, pins);
    cases::reconcile(&mut store, pins);
    drop(store);
    let mut store = NativeSnapshotStore::recover(
        FileSnapshotIo::open(&path).unwrap(),
        identity_for(1),
        limits(),
    )
    .unwrap();
    cases::assert_image(&mut store, 4);
    for pin in pins {
        assert!(store.load_pinned(pin).is_err());
    }
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn file_snapshot_bindings_cannot_publish_or_pin_each_others_images() {
    let path = root("bindings");
    let mut first = NativeSnapshotStore::create(
        FileSnapshotIo::create(path.join("first")).unwrap(),
        identity_for(1),
        limits(),
    )
    .unwrap();
    let mut second = NativeSnapshotStore::create(
        FileSnapshotIo::create(path.join("second")).unwrap(),
        identity_for(2),
        limits(),
    )
    .unwrap();
    cases::cross_store(&mut first, &mut second);
    drop(first);
    cases::assert_image(&mut second, 1);
    drop(second);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn file_reopen_discards_each_unpublished_stage_and_rejects_old_admissions() {
    for phase in 0..3 {
        let path = root(&format!("unpublished-{phase}"));
        let mut store = NativeSnapshotStore::create(
            FileSnapshotIo::create(&path).unwrap(),
            identity_for(1),
            limits(),
        )
        .unwrap();
        let original = cases::publish(&mut store, 1).reference();
        store.pin_for_log(original).unwrap();
        let ticket = store.begin(cases::metadata(2), 7).unwrap();
        let mut sealed = None;
        if phase > 0 {
            store.write_chunk(ticket, 0, &[2; 4]).unwrap();
        }
        if phase == 2 {
            store.write_chunk(ticket, 4, &[2; 3]).unwrap();
            sealed = Some(store.seal(ticket).unwrap());
        }
        drop(store);
        let mut store = NativeSnapshotStore::recover(
            FileSnapshotIo::open(&path).unwrap(),
            identity_for(1),
            limits(),
        )
        .unwrap();
        assert!(store.binding().session > ticket.binding.session);
        assert_eq!(store.latest_reference().unwrap(), Some(original));
        assert_eq!(store.load_pinned(original).unwrap().application, [1; 7]);
        cases::assert_image(&mut store, 1);
        let current = store.begin(cases::metadata(2), 7).unwrap();
        assert!(store.write_chunk(ticket, 0, &[2]).is_err());
        assert!(store.seal(ticket).is_err());
        assert!(store.abort(ticket).is_err());
        if let Some(sealed) = sealed {
            assert!(store.publish(sealed).is_err());
        }
        store.write_chunk(current, 0, &[2; 4]).unwrap();
        store.write_chunk(current, 4, &[2; 3]).unwrap();
        let receipt = store.seal(current).unwrap();
        store.publish(receipt).unwrap();
        cases::assert_image(&mut store, 2);
        assert_eq!(store.load_pinned(original).unwrap().application, [1; 7]);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}
