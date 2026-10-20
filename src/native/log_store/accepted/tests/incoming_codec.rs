// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{cell::Cell, rc::Rc};
struct RefusingCodec(Rc<Cell<usize>>);
impl LogCodec for RefusingCodec {
    fn format_version(&self) -> u32 {
        2
    }
    fn encode_batch(
        &self,
        sequence: u64,
        mutations: &[LogMutation],
        limits: LogLimits,
    ) -> Result<Vec<u8>, StorageError> {
        for mutation in mutations {
            if let LogMutation::Update(update) = mutation {
                if let Some(suffix) = &update.suffix {
                    self.0.set(
                        suffix
                            .entries
                            .iter()
                            .map(LogEntry::retained_payload_bytes)
                            .sum(),
                    );
                }
                return Err(StorageError::Rejected("selected codec refuses original"));
            }
        }
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
}
#[test]
fn codec_sees_shared_retained_charge_and_refuses_before_invalid_log_admission_or_any_io() {
    let io = fixture::Memory::default();
    let observed = Rc::new(Cell::new(0));
    let identity = bootstrap(1).voter_stores[&NodeId::new(1).unwrap()];
    let mut store = NativeLogStore::create_with_codec(
        io.clone(),
        identity,
        LogLimits::default(),
        RefusingCodec(observed.clone()),
    )
    .unwrap();
    let created = store
        .append_batch(vec![LogMutation::Create(bootstrap(1))])
        .unwrap();
    store.barrier(&created).unwrap();
    let before = store.state(group(1)).unwrap();
    let written = io.0.borrow().bytes.clone();
    let mut input = entry(1, 1);
    let EntryPayload::Command { bytes, .. } = &mut input.payload else {
        unreachable!()
    };
    let mut raw = bytes.to_vec();
    raw.reserve(10000);
    *bytes = raw.into();
    let charge = bytes.retained_bytes();
    let LogMutation::Update(mut change) = mutation_raw(
        &before,
        0,
        Some(Suffix {
            from: 1,
            entries: vec![input].into(),
        }),
    ) else {
        unreachable!()
    };
    change.expected_revision = LogRevision::new(99).unwrap();
    assert_eq!(
        store.append_batch(vec![LogMutation::Update(change)]),
        Err(StorageError::Rejected("selected codec refuses original"))
    );
    assert_eq!(observed.get(), charge);
    assert_eq!(store.state(group(1)).unwrap(), before);
    assert_eq!(io.0.borrow().bytes, written);
    assert!(store.pending.is_empty());
    assert!(store.accepted.is_clean());
}
