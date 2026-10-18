// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

pub(super) struct Directory(pub PathBuf);
impl Directory {
    pub fn new(label: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "voteboat-witness-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        Self(root)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
pub(super) type Store = NativeLogStore<WitnessLogIo>;
pub(super) fn create(root: &Directory) -> Store {
    NativeLogStore::create(
        WitnessLogIo::create(&root.0).unwrap(),
        identity(1),
        LogLimits::default(),
    )
    .unwrap()
}
pub(super) fn recover(root: &Directory) -> Result<Store, StorageError> {
    let io = WitnessLogIo::open(&root.0).map_err(|e| StorageError::Uncertain(e.to_string()))?;
    NativeLogStore::recover(io, identity(1), LogLimits::default())
}
pub(super) fn populated(root: &Directory) -> Store {
    let mut store = create(root);
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 1))]);
    let old = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &old,
            1,
            1,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1, 17), entry(2, 1, 29)].into(),
            }),
        )],
    );
    store
}
pub(super) fn selected(root: &Directory) -> (PathBuf, PathBuf) {
    let current = fs::read(root.0.join("WITNESS-CURRENT")).unwrap();
    assert_eq!(current.len(), 20);
    let generation = u64::from_le_bytes(current[8..16].try_into().unwrap());
    if generation == 0 {
        (root.0.join("log.wal"), root.0.join("MANIFEST"))
    } else {
        (
            root.0.join(format!("log.{}.wal", generation % 2)),
            root.0.join(format!("MANIFEST.{}", generation % 2)),
        )
    }
}
pub(super) fn crc(bytes: &[u8]) -> u32 {
    let mut value = !0_u32;
    for b in bytes {
        value ^= u32::from(*b);
        for _ in 0..8 {
            value = (value >> 1) ^ (0x82f6_3b78 & 0_u32.wrapping_sub(value & 1));
        }
    }
    !value
}
pub(super) fn rechecksum(bytes: &mut [u8]) {
    let end = bytes.len() - 4;
    let checksum = crc(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum.to_le_bytes());
}
pub(super) fn pair(root: &Directory) -> (Vec<u8>, Vec<u8>) {
    let (wal, metadata) = selected(root);
    let wal = fs::read(wal).unwrap();
    let metadata = fs::read(metadata).unwrap();
    assert!(wal.len() > 128);
    assert_eq!(metadata.len(), 128);
    assert_eq!(
        crc(&wal[..124]),
        u32::from_le_bytes(wal[124..128].try_into().unwrap())
    );
    assert_eq!(
        crc(&metadata[..124]),
        u32::from_le_bytes(metadata[124..128].try_into().unwrap())
    );
    (wal, metadata)
}
pub(super) fn write_pair(root: &Directory, wal: &[u8], metadata: &[u8]) {
    let (w, m) = selected(root);
    std::fs::write(w, wal).unwrap();
    std::fs::write(m, metadata).unwrap();
}
