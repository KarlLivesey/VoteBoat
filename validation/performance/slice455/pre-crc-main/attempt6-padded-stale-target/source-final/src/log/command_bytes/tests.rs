// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{
    identity::OperationId,
    log::{EntryPayload, LogEntry},
};

#[test]
fn empty_values_normalize_without_backing_or_charge() {
    let mut spare = Vec::with_capacity(4096);
    spare.clear();
    for value in [CommandBytes::default(), spare.into(), (&[][..]).into()] {
        assert!(value.0.is_none());
        assert!(value.clone().0.is_none());
        assert_eq!(value.retained_bytes(), 0);
        assert_eq!(format!("{value:?}"), "[]");
        assert_eq!(value.as_ref(), &[]);
    }
}
#[test]
fn shared_content_survives_other_owners_and_mutable_copies_are_independent() {
    let value = CommandBytes::from(vec![1, 2, 3]);
    let shared = value.clone();
    assert_eq!(value.as_ptr(), shared.as_ptr());
    let weak = Arc::downgrade(value.0.as_ref().unwrap());
    let mut mutable = value.to_vec();
    mutable[0] = 9;
    let replacement = CommandBytes::from(mutable);
    assert_ne!(replacement, value);
    drop(value);
    assert_eq!(shared.as_slice(), &[1, 2, 3]);
    assert!(weak.upgrade().is_some());
    drop(shared);
    assert!(weak.upgrade().is_none());
    assert_eq!(replacement.as_slice(), &[9, 2, 3]);
}
#[test]
fn independent_buffers_compare_full_contents_and_all_original_fields() {
    let left = CommandBytes::from(vec![1, 2, 3]);
    let right = CommandBytes::from(&[1, 2, 3][..]);
    assert_ne!(left.as_ptr(), right.as_ptr());
    assert_eq!(left, right);
    assert_ne!(left, CommandBytes::from(vec![1, 2, 4]));
    assert_ne!(left, CommandBytes::from(vec![1, 2]));
    let entry = LogEntry {
        index: 37,
        term: 19,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::MAX).unwrap(),
            bytes: left,
        },
    };
    let mut altered = entry.clone();
    assert_eq!(entry, altered);
    let EntryPayload::Command {
        operation,
        bytes: _,
    } = &mut altered.payload
    else {
        unreachable!()
    };
    *operation = OperationId::new(u128::MAX ^ (1 << 96)).unwrap();
    assert_ne!(entry, altered);
    let EntryPayload::Command {
        operation,
        bytes: _,
    } = &mut altered.payload
    else {
        unreachable!()
    };
    *operation = OperationId::new(u128::MAX).unwrap();
    let EntryPayload::Command {
        operation: _,
        bytes,
    } = &mut altered.payload
    else {
        unreachable!()
    };
    *bytes = CommandBytes::from(vec![1, 2, 4]);
    assert_ne!(entry, altered);
}
#[test]
fn every_nonempty_retained_clone_is_charged_without_sharing_discount() {
    let value = CommandBytes::from(vec![7; 37]);
    let charge = (37 + 2 * std::mem::size_of::<AtomicUsize>())
        .next_multiple_of(std::mem::align_of::<AtomicUsize>().max(2));
    assert_eq!(value.retained_bytes(), charge);
    assert_eq!(value.clone().retained_bytes(), charge);
    assert_eq!(CommandBytes::retained_bytes_for_len(0), Some(0));
    assert_eq!(CommandBytes::retained_bytes_for_len(usize::MAX), None);
    assert_eq!(
        CommandBytes::retained_bytes_for_len(isize::MAX as usize),
        None
    );
    let alignment = std::mem::align_of::<AtomicUsize>().max(2);
    let largest_layout = (isize::MAX as usize) & !(alignment - 1);
    let largest_content = largest_layout - 2 * std::mem::size_of::<AtomicUsize>();
    assert_eq!(
        CommandBytes::retained_bytes_for_len(largest_content),
        Some(largest_layout)
    );
    assert_eq!(
        CommandBytes::retained_bytes_for_len(largest_content + 1),
        None
    );
}
#[test]
fn retained_charge_includes_requested_allocation_alignment_padding() {
    let alignment = std::mem::align_of::<AtomicUsize>().max(2);
    let header = 2 * std::mem::size_of::<AtomicUsize>();
    for len in 0..=16 {
        // Independent bounded arithmetic oracle for the repr(C) two-counter
        // header followed by bytes, including final allocation alignment.
        let expected = if len == 0 {
            0
        } else {
            (header + len).next_multiple_of(alignment)
        };
        let value = CommandBytes::from(vec![7; len]);
        assert_eq!(value.retained_bytes(), expected, "length {len}");
        assert_eq!(value.clone().retained_bytes(), expected, "clone {len}");
        assert_eq!(CommandBytes::retained_bytes_for_len(len), Some(expected));
    }
}
#[test]
fn current_and_legacy_representation_layouts_are_diagnostic_only() {
    enum LegacyPayload {
        Noop,
        Configuration(Box<crate::membership::ConfigurationRecord>),
        Command {
            operation: OperationId,
            bytes: Vec<u8>,
        },
    }
    struct LegacyEntry {
        index: u64,
        term: u64,
        payload: LegacyPayload,
    }
    let operation = OperationId::new(1).unwrap();
    let record = crate::membership::ConfigurationRecord {
        operation,
        expected: crate::identity::ConfigurationId::new(1).unwrap(),
        change: crate::membership::ConfigurationChange::Final {
            id: crate::identity::ConfigurationId::new(2).unwrap(),
        },
    };
    let replicas = [
        LegacyPayload::Noop,
        LegacyPayload::Configuration(Box::new(record)),
        LegacyPayload::Command {
            operation,
            bytes: vec![1],
        },
    ];
    let mut diagnostic_fields = 0u128;
    for payload in replicas {
        let entry = LegacyEntry {
            index: 1,
            term: 2,
            payload,
        };
        diagnostic_fields += u128::from(entry.index + entry.term)
            + match entry.payload {
                LegacyPayload::Noop => 0,
                LegacyPayload::Configuration(record) => record.operation.get(),
                LegacyPayload::Command { operation, bytes } => {
                    operation.get() + bytes.len() as u128
                }
            };
    }
    println!("scope=dev_current_vs_literal_legacy_representation_not_published_ELF CommandBytes={} OptionArcSlice={} EntryPayload={} LogEntry={} legacyVec={} legacyPayload={} legacyEntry={} sharedHeaderCharge={} legacyFixtureFieldRead={}", std::mem::size_of::<CommandBytes>(), std::mem::size_of::<Option<Arc<[u8]>>>(), std::mem::size_of::<EntryPayload>(), std::mem::size_of::<LogEntry>(), std::mem::size_of::<Vec<u8>>(), std::mem::size_of::<LegacyPayload>(), std::mem::size_of::<LegacyEntry>(), 2 * std::mem::size_of::<usize>(), diagnostic_fields);
}

#[test]
fn historical_checkpoint_command_at_batch_limit_decodes_and_recovers_unchanged() {
    use crate::{
        contracts::HardState,
        identity::*,
        log::*,
        log_format::{LogCodec, NativeLogCodec},
        quorum::{Limits, Policy, Tree},
        raft::Raft,
    };
    use std::collections::BTreeMap;
    let node = NodeId::new(1).unwrap();
    let store = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let group = GroupIdentity {
        id: GroupId::new(1).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    };
    let bootstrap = Bootstrap {
        group,
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Majority(vec![Tree::Voter(node)]), Limits::default()).unwrap(),
        voter_stores: BTreeMap::from([(node, store)]),
    };
    let limits = LogLimits {
        max_command_bytes: 256,
        max_batch_bytes: 256,
        ..LogLimits::default()
    };
    let mut states = BTreeMap::new();
    apply_batch(&mut states, &[LogMutation::Create(bootstrap)], limits).unwrap();
    let state = states.get_mut(&group).unwrap();
    state.hard_state = HardState {
        term: 1,
        voted_for: None,
    };
    state.revision = LogRevision::new(2).unwrap();
    state.commit_index = 1;
    state.entries.push(LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(u128::MAX).unwrap(),
            bytes: vec![37; 256].into(),
        },
    });
    let encoded = NativeLogCodec
        .encode_checkpoint(7, &states, limits, limits.max_wal_bytes)
        .unwrap();
    assert!(encoded.len() > limits.max_batch_bytes);
    let (sequence, decoded, used) = NativeLogCodec.decode_checkpoint(&encoded, limits).unwrap();
    assert_eq!(sequence, 7);
    assert_eq!(used, encoded.len());
    assert_eq!(decoded, states);
    assert_eq!(
        NativeLogCodec
            .encode_checkpoint(7, &decoded, limits, limits.max_wal_bytes)
            .unwrap(),
        encoded
    );
    let recovered = Raft::recover(
        node,
        StoreBinding {
            identity: store,
            session: StoreSession::new(1).unwrap(),
        },
        decoded[&group].clone(),
        limits,
    )
    .unwrap();
    assert_eq!(recovered.state(), &states[&group]);
}
