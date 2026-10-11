// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

struct Substituted;
impl SnapshotCodec for Substituted {
    fn format_version(&self) -> u32 {
        2
    }
    fn prefix(
        &self,
        m: &SnapshotMetadata,
        n: usize,
        l: SnapshotLimits,
    ) -> Result<Vec<u8>, StorageError> {
        NativeSnapshotCodec.prefix(m, n, l)
    }
    fn finish(&self, b: &[u8], l: SnapshotLimits) -> Result<[u8; 4], StorageError> {
        NativeSnapshotCodec.finish(b, l)
    }
    fn decode(&self, b: &[u8], l: SnapshotLimits) -> Result<Snapshot, StorageError> {
        let mut snapshot = NativeSnapshotCodec.decode(b, l)?;
        snapshot.application[0] ^= 1;
        Ok(snapshot)
    }
}
#[test]
fn before_fix_substituted_application_gets_a_sealed_published_receipt() {
    let io = MemoryIo::default();
    let mut store = NativeSnapshotStore::create_with_codec(
        io,
        snapshot_identity(),
        SnapshotLimits::default(),
        Substituted,
    )
    .unwrap();
    let receipt = publish(&mut store, metadata(1), &[7, 8, 9]);
    assert_eq!(store.latest_reference().unwrap(), Some(receipt.reference()));
    let application = store.load().unwrap().unwrap().application;
    assert_eq!(application, [6, 8, 9]);
    eprintln!("sealed/published original [7, 8, 9], loaded substituted {application:?}");
}
