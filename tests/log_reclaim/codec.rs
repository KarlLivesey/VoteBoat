// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::BTreeMap;

struct Marker(bool);
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
    fn is_checkpoint(&self, header: &[u8]) -> bool {
        assert!(
            header.len() <= 32,
            "recognition must receive a bounded prefix"
        );
        self.0 && marked(header)
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
fn supplied_marker_recovers_exact_state_sessions_and_tickets_after_power_loss() {
    let io = ModelIo::default();
    let mut store = populated_with(io.clone(), Marker(true));
    let expected = states(&store);
    let binding = store.binding();
    let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
    assert!(report.after_bytes < report.before_bytes);
    assert!(marked(&io.0.borrow().log));
    assert_eq!(store.state(group(1)).unwrap(), expected[0]);
    assert_eq!(store.state(group(2)).unwrap(), expected[1]);
    drop(store);
    io.0.borrow_mut().power_loss();
    wrong_codec_preserves_journal(&io);
    let mut recovered = reopen(io.clone());
    assert_eq!(states(&recovered), expected);
    assert_ne!(recovered.binding().session, binding.session);
    let first = append(&mut recovered, vec![update(&expected[0], 21, 1, None)])[0];
    assert_eq!(first.batch, 23);
    let expected = states(&recovered);
    drop(recovered);
    io.0.borrow_mut().power_loss();
    let mut recovered = reopen(io.clone());
    assert_eq!(
        states(&recovered),
        expected,
        "checkpoint plus trailing batch"
    );
    assert_eq!(recovered.barrier(&[first]), Err(StorageError::StaleTicket));
    recovered.reclaim(recovered.limits().max_wal_bytes).unwrap();
    drop(recovered);
    io.0.borrow_mut().power_loss();
    let mut recovered = reopen(io);
    assert_eq!(states(&recovered), expected, "second reclamation");
    let next = append(&mut recovered, vec![update(&expected[0], 22, 1, None)])[0];
    assert_eq!(next.batch, first.batch + 1);
}

fn states<I: JournalIo>(store: &NativeLogStore<I, Marker>) -> [GroupLog; 2] {
    [
        store.state(group(1)).unwrap(),
        store.state(group(2)).unwrap(),
    ]
}
fn reopen<I: JournalIo>(io: I) -> NativeLogStore<I, Marker> {
    NativeLogStore::recover_with_codec(io, identity(1), LogLimits::default(), Marker(true)).unwrap()
}
fn wrong_codec_preserves_journal(io: &ModelIo) {
    let before = journal(io);
    assert!(matches!(
        NativeLogStore::recover(io.clone(), identity(1), LogLimits::default()),
        Err(StorageError::Corrupt(_))
    ));
    let after = io.0.borrow();
    assert_eq!(after.log, before.0);
    assert_eq!(after.synced, before.1);
    assert_eq!(after.manifest, before.2);
}
fn journal(io: &ModelIo) -> (Vec<u8>, Vec<u8>, Option<Vec<u8>>) {
    let device = io.0.borrow();
    (
        device.log.clone(),
        device.synced.clone(),
        device.manifest.clone(),
    )
}

#[test]
fn inconsistent_recognition_rejects_before_replacement_or_state_change() {
    let io = ModelIo::default();
    let mut store = populated_with(io.clone(), Marker(false));
    let before = journal(&io);
    let expected = states(&store);
    let binding = store.binding();
    assert_eq!(
        store.reclaim(store.limits().max_wal_bytes),
        Err(StorageError::Rejected("checkpoint codec prefix mismatch"))
    );
    assert_eq!(states(&store), expected);
    assert_eq!(store.binding(), binding);
    assert_eq!(journal(&io), before);
    let ticket = append(&mut store, vec![update(&expected[0], 21, 1, None)])[0];
    assert_eq!(ticket.batch, 23, "rejection consumes no sequence");
}

#[test]
fn supplied_marker_reopens_real_files_with_trailing_batches_and_reclamation() {
    let path = std::env::temp_dir().join(format!("voteboat-host-codec-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    let mut store = populated_with(FileLogIo::create(&path).unwrap(), Marker(true));
    for term in [21, 22, 23] {
        for _ in 0..10 {
            let state = store.state(group(1)).unwrap();
            append(&mut store, vec![update(&state, term, 1, None)]);
        }
        let expected = states(&store);
        let binding = store.binding();
        let report = store.reclaim(store.limits().max_wal_bytes).unwrap();
        assert!(report.after_bytes < report.before_bytes);
        let ticket = append(&mut store, vec![update(&expected[0], term, 1, None)])[0];
        let expected = states(&store);
        drop(store);
        let mut io = FileLogIo::open(&path).unwrap();
        let manifest = io.read_manifest().unwrap();
        drop(io);
        assert!(matches!(
            NativeLogStore::recover(
                FileLogIo::open(&path).unwrap(),
                identity(1),
                LogLimits::default()
            ),
            Err(StorageError::Corrupt(_))
        ));
        let mut io = FileLogIo::open(&path).unwrap();
        assert_eq!(io.read_manifest().unwrap(), manifest);
        store = reopen(io);
        assert_eq!(states(&store), expected);
        assert_ne!(store.binding().session, binding.session);
        assert_eq!(store.barrier(&[ticket]), Err(StorageError::StaleTicket));
    }
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}
