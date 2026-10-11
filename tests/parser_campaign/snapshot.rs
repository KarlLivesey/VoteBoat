// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{generator::*, support::*};
use std::{cell::RefCell, io, rc::Rc};
use voteboat::{native::snapshot_store::*, snapshot::*};
fn limits() -> SnapshotLimits {
    SnapshotLimits {
        max_application_bytes: 128,
        max_metadata_bytes: 1024,
        max_chunk_bytes: 32,
    }
}
fn metadata() -> SnapshotMetadata {
    SnapshotMetadata {
        bootstrap: bootstrap(1, 3),
        membership: None,
        index: 2,
        term: 1,
        application_schema: 1,
    }
}
fn identity_of() -> SnapshotIdentity {
    SnapshotIdentity {
        store: identity(1),
        group: group(1),
    }
}
#[derive(Clone, Default)]
struct Memory(Rc<RefCell<Device>>);
#[derive(Clone, Default)]
struct Device {
    root: Vec<u8>,
    slots: [Option<Vec<u8>>; 2],
}
impl SnapshotIo for Memory {
    fn read_manifest(&mut self) -> io::Result<Vec<u8>> {
        let root = self.0.borrow().root.clone();
        if root.is_empty() {
            Err(io::ErrorKind::NotFound.into())
        } else {
            Ok(root)
        }
    }
    fn read_slot(&mut self, slot: u8, limit: usize) -> io::Result<Vec<u8>> {
        assert!(limit <= limits().max_file_bytes());
        let device = self.0.borrow();
        let bytes = device
            .slots
            .get(usize::from(slot))
            .and_then(Option::as_ref)
            .ok_or(io::ErrorKind::NotFound)?;
        if bytes.len() > limit {
            return Err(io::Error::other("slot exceeds requested read bound"));
        }
        Ok(bytes.clone())
    }
    fn begin_slot(&mut self, slot: u8, prefix: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().slots[usize::from(slot)] = Some(prefix.to_vec());
        Ok(())
    }
    fn append_slot(&mut self, slot: u8, bytes: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().slots[usize::from(slot)]
            .as_mut()
            .unwrap()
            .extend(bytes);
        Ok(())
    }
    fn sync_slot(&mut self, _: u8) -> io::Result<()> {
        Ok(())
    }
    fn publish_manifest(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.0.borrow_mut().root = bytes.to_vec();
        Ok(())
    }
}
fn image() -> (Memory, Snapshot) {
    let image = Memory::default();
    let mut store = NativeSnapshotStore::create(image.clone(), identity_of(), limits()).unwrap();
    let expected = Snapshot {
        metadata: metadata(),
        application: vec![7; 32],
    };
    let ticket = store
        .begin(expected.metadata.clone(), expected.application.len())
        .unwrap();
    store.write_chunk(ticket, 0, &expected.application).unwrap();
    let sealed = store.seal(ticket).unwrap();
    store.publish(sealed).unwrap();
    drop(store);
    (image, expected)
}
fn encoded(snapshot: &Snapshot) -> Vec<u8> {
    let mut bytes = NativeSnapshotCodec
        .prefix(&snapshot.metadata, snapshot.application.len(), limits())
        .unwrap();
    bytes.extend(&snapshot.application);
    bytes.extend(NativeSnapshotCodec.finish(&bytes, limits()).unwrap());
    bytes
}
#[test]
fn generated_snapshot_image_mutations_validate_metadata_application_bounds_and_integrity() {
    let (_, expected) = image();
    let original = encoded(&expected);
    campaign(
        "snapshot-image",
        &original,
        &[(8, 8), (16, 8), (24, 8), (32, 4), (36, 8)],
        |case| {
            let mut bytes = case.bytes.clone();
            if case.repair() {
                seal_tail(&mut bytes);
            }
            let result = NativeSnapshotCodec.decode(&bytes, limits());
            if case.original() {
                assert_eq!(result, Ok(expected.clone()));
            }
            if case.integrity_damage() {
                assert!(result.is_err());
            }
            match result {
                Ok(snapshot) => {
                    snapshot.metadata.validate().unwrap();
                    assert!(
                        snapshot.application.len() <= limits().max_application_bytes
                            && snapshot.application.capacity() <= limits().max_application_bytes
                    );
                    assert_eq!(
                        NativeSnapshotCodec.decode(&encoded(&snapshot), limits()),
                        Ok(snapshot)
                    );
                    true
                }
                Err(_) => false,
            }
        },
    );
}
#[test]
fn generated_published_snapshot_manifest_mutations_cannot_select_wrong_or_partial_data() {
    let (image, expected) = image();
    let original = image.0.borrow().clone();
    campaign(
        "snapshot-published-manifest",
        &original.root,
        &[
            (8, 8),
            (24, 8),
            (32, 8),
            (40, 8),
            (64, 1),
            (65, 1),
            (66, 8),
            (74, 8),
        ],
        |case| {
            let mut device = original.clone();
            device.root = case.bytes.clone();
            if case.repair() {
                seal_tail(&mut device.root);
            }
            let recovered = NativeSnapshotStore::recover(
                Memory(Rc::new(RefCell::new(device))),
                identity_of(),
                limits(),
            );
            if case.original() {
                assert!(recovered.is_ok());
            }
            if case.integrity_damage() {
                assert!(recovered.is_err());
            }
            match recovered {
                Ok(mut store) => {
                    assert_eq!(store.identity(), identity_of());
                    assert_eq!(store.load().unwrap(), Some(expected.clone()));
                    true
                }
                Err(_) => false,
            }
        },
    );
}
