// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::BTreeMap;

struct Marker;
impl LogCodec for Marker {
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
    fn frame_length(&self, header: &[u8], limits: LogLimits) -> Result<usize, StorageError> {
        NativeLogCodec.frame_length(header, limits)
    }
    fn decode_batch(
        &self,
        bytes: &[u8],
        limits: LogLimits,
    ) -> Result<(u64, Vec<LogMutation>), StorageError> {
        NativeLogCodec.decode_batch(bytes, limits)
    }
    fn supports_checkpoint(&self) -> bool {
        true
    }
    fn encode_checkpoint(
        &self,
        sequence: u64,
        state: &BTreeMap<GroupIdentity, GroupLog>,
        limits: LogLimits,
        max_bytes: usize,
    ) -> Result<Vec<u8>, StorageError> {
        let mut encoded = NativeLogCodec.encode_checkpoint(sequence, state, limits, max_bytes)?;
        encoded[0] ^= 0x80; // Test companion marker, reversed before native validation.
        Ok(encoded)
    }
    fn decode_checkpoint(
        &self,
        bytes: &[u8],
        limits: LogLimits,
    ) -> Result<(u64, BTreeMap<GroupIdentity, GroupLog>, usize), StorageError> {
        if !marked(bytes) {
            return Err(StorageError::Corrupt("host checkpoint marker"));
        }
        let mut original = bytes.to_vec();
        original[0] ^= 0x80;
        NativeLogCodec.decode_checkpoint(&original, limits)
    }
}
fn marked(bytes: &[u8]) -> bool {
    bytes.get(..8).is_some_and(|prefix| {
        prefix[0] == (b'V' ^ 0x80) && (&prefix[1..] == b"BLCPT01" || &prefix[1..] == b"BLCPT02")
    })
}
#[test]
fn before_fix_reclamation_accepts_checkpoint_that_same_codec_cannot_reopen() {
    let io = ModelIo::default();
    let mut store = populated_with(io.clone(), Marker);
    let expected = [
        store.state(group(1)).unwrap(),
        store.state(group(2)).unwrap(),
    ];
    let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
    assert!(report.after_bytes < report.before_bytes);
    assert!(marked(&io.0.borrow().log));
    assert_eq!(store.state(group(1)).unwrap(), expected[0]);
    assert_eq!(store.state(group(2)).unwrap(), expected[1]);
    drop(store);
    io.0.borrow_mut().power_loss();
    match NativeLogStore::recover_with_codec(io, identity(1), LogLimits::default(), Marker) {
        Err(StorageError::Corrupt(reason)) => {
            eprintln!("accepted custom checkpoint cannot reopen: {reason}")
        }
        Err(error) => panic!("unexpected failure: {error:?}"),
        Ok(_) => panic!("prior hard-coded recognition unexpectedly accepted host marker"),
    }
}
