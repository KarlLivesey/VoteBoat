// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::voteboat_engine::Reply;
use std::{
    collections::{hash_map::RandomState, HashMap},
    hash::BuildHasher,
    mem::{align_of, size_of},
};

#[derive(Debug, Eq, PartialEq)]
struct Call {
    method: &'static str,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct RecordingHasher {
    calls: Vec<Call>,
}

macro_rules! record_integer {
    ($method:ident, $type:ty) => {
        fn $method(&mut self, value: $type) {
            self.calls.push(Call {
                method: stringify!($method),
                bytes: value.to_ne_bytes().to_vec(),
            });
        }
    };
}

impl Hasher for RecordingHasher {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.calls.push(Call {
            method: "write",
            bytes: bytes.to_vec(),
        });
    }

    record_integer!(write_u8, u8);
    record_integer!(write_u16, u16);
    record_integer!(write_u32, u32);
    record_integer!(write_u64, u64);
    record_integer!(write_u128, u128);
    record_integer!(write_usize, usize);
    record_integer!(write_i8, i8);
    record_integer!(write_i16, i16);
    record_integer!(write_i32, i32);
    record_integer!(write_i64, i64);
    record_integer!(write_i128, i128);
    record_integer!(write_isize, isize);
}

fn recorded(value: &impl Hash) -> Vec<Call> {
    let mut state = RecordingHasher::default();
    value.hash(&mut state);
    state.calls
}

fn identities() -> Vec<OperationId> {
    (0..128)
        .map(|bit| 1_u128 << bit)
        .chain([
            u128::MAX,
            u128::from(u64::MAX),
            u128::from(u64::MAX) << 64,
            0xfedc_ba98_7654_3210_0123_4567_89ab_cdef,
            7,
            (1 << 96) | 7,
            (1 << 127) | 7,
            7,
        ])
        .map(|value| OperationId::new(value).unwrap())
        .collect()
}

#[test]
fn all_bits_preserve_the_exact_hash_method_stream_and_random_state_output() {
    let random = RandomState::new();
    for operation in identities() {
        let key = ReplyKey::from(operation);
        let restored = u128::from(key.0[0]) | (u128::from(key.0[1]) << 64);
        assert_eq!(restored, operation.get());
        assert_eq!(recorded(&key), recorded(&operation), "{operation:?}");
        assert_eq!(random.hash_one(key), random.hash_one(operation));
    }
}

#[test]
fn exact_equality_and_map_replacement_keep_high_words_distinct() {
    let ids = identities();
    for &left in &ids {
        for &right in &ids {
            assert_eq!(left == right, ReplyKey::from(left) == ReplyKey::from(right));
        }
    }
    let mut pending = HashMap::new();
    let same_low = [7, (1 << 96) | 7, (1 << 127) | 7];
    for value in same_low {
        let operation = OperationId::new(value).unwrap();
        assert_eq!(pending.insert(ReplyKey::from(operation), value), None);
    }
    let operation = OperationId::new(same_low[1]).unwrap();
    assert_eq!(pending.insert(operation.into(), 99), Some(same_low[1]));
    assert_eq!(pending.len(), 3);
    assert_eq!(pending.remove(&ReplyKey::from(operation)), Some(99));
    for value in [same_low[2], same_low[0]] {
        assert_eq!(
            pending.remove(&ReplyKey::from(OperationId::new(value).unwrap())),
            Some(value)
        );
    }
    assert!(pending.is_empty());
}

#[test]
fn actual_target_layout_removes_pending_tuple_padding() {
    println!(
        "layout_scope=actual_standalone_dev_test target_arch={} legacy_key_bytes={} legacy_key_alignment={} compact_key_bytes={} compact_key_alignment={} reply_bytes={} legacy_tuple_bytes={} compact_tuple_bytes={} legacy_tuple_alignment={} compact_tuple_alignment={}",
        std::env::consts::ARCH,
        size_of::<OperationId>(),
        align_of::<OperationId>(),
        size_of::<ReplyKey>(),
        align_of::<ReplyKey>(),
        size_of::<Reply>(),
        size_of::<(OperationId, Reply)>(),
        size_of::<(ReplyKey, Reply)>(),
        align_of::<(OperationId, Reply)>(),
        align_of::<(ReplyKey, Reply)>(),
    );
    assert_eq!(size_of::<OperationId>(), size_of::<ReplyKey>());
    assert!(align_of::<ReplyKey>() < align_of::<OperationId>());
    assert!(size_of::<(ReplyKey, Reply)>() < size_of::<(OperationId, Reply)>());
}
