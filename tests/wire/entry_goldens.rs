// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Byte goldens for entry-carrying wire frames, WAL batches and checkpoints.
//! The hex was captured from the encoders at 2b1bf078, before shared entry
//! batches (V3b) changed the in-memory representation. However the entries
//! are partitioned or shared in memory, the encoded bytes must not change.
use super::*;
use voteboat::log_format::{LogCodec, NativeLogCodec};

const WIRE_COMMANDS: &[&str] = &[
    "5642574952453031010000001b01000001000000849794aafb000000010000000000000000000000",
    "00000000010000000000000001000000000000000100000000000000010000000000000000000000",
    "00000000010000000000000001000000000000000200000000000000030000000000000002000000",
    "00000000000000000000000001000000000000000100000000000000070000000000000002000000",
    "00000000000000000000000000010000000000000003000000010000000000000001000000000000",
    "00000200000000000000010000000000000001070000000000000000000000000000000000000003",
    "0000000000000002000000000000000108000000000000000000000000000000030000000102038a",
    "0c58f0",
];
const WIRE_CONFIGURATION: &[&str] = &[
    "564257495245303102000000ae020000010000008f9b55948e020000010000000000000000000000",
    "00000000010000000000000001000000000000000100000000000000010000000000000000000000",
    "00000000010000000000000001000000000000000200000000000000030000000000000002000000",
    "00000000000000000000000001000000000000000100000000000000070000000000000002000000",
    "00000000000000000000000000020000000000000003000000010000000000000002000000000000",
    "00020164000000000000000000000000000000010000000000000000020000000000000001030000",
    "00000100000000000000000200000000000000000300000000000000030000000100000000000000",
    "01000000000000000000000000000000010000000000000002000000000000000200000000000000",
    "00000000000000000100000000000000030000000000000003000000000000000000000000000000",
    "01000000000000000100000004000000000000000400000000000000000000000000000001000000",
    "00000000020000000000000002000000000000000201650000000000000000000000000000000200",
    "00000000000001030000000000000004000000000000000103000000000200000000000000000300",
    "00000000000000040000000000000003000000020000000000000002000000000000000000000000",
    "00000001000000000000000300000000000000030000000000000000000000000000000100000000",
    "00000004000000000000000400000000000000000000000000000001000000000000000100000001",
    "00000000000000010000000000000000000000000000000100000000000000030000000000000002",
    "00000000000000020165000000000000000000000000000000030000000000000002040000000000",
    "0000a6e12c29",
];
const WAL_COMMANDS: &[&str] = &[
    "56424c4f47303032050000000000000001000000a40000003b073200000000000101000000000000",
    "00000000000000000001000000000000000100000000000000020000000000000000000000000000",
    "00010000000000000001010000000000000003000000010000000000000001000000000000000002",
    "00000000000000010000000000000001070000000000000000000000000000000000000003000000",
    "00000000020000000000000001080000000000000000000000000000000300000001020356424c45",
    "4e4430321737a98e00000000",
];
const WAL_CONFIGURATION: &[&str] = &[
    "56424c4f47303032050000000000000001000000370200000da4bb96000000000101000000000000",
    "00000000000000000001000000000000000100000000000000020000000000000000000000000000",
    "00010000000000000001010000000000000003000000010000000000000002000000000000000201",
    "64000000000000000000000000000000010000000000000000020000000000000001030000000001",
    "00000000000000000200000000000000000300000000000000030000000100000000000000010000",
    "00000000000000000000000000010000000000000002000000000000000200000000000000000000",
    "00000000000100000000000000030000000000000003000000000000000000000000000000010000",
    "00000000000100000004000000000000000400000000000000000000000000000001000000000000",
    "00020000000000000002000000000000000201650000000000000000000000000000000200000000",
    "00000001030000000000000004000000000000000103000000000200000000000000000300000000",
    "00000000040000000000000003000000020000000000000002000000000000000000000000000000",
    "01000000000000000300000000000000030000000000000000000000000000000100000000000000",
    "04000000000000000400000000000000000000000000000001000000000000000100000001000000",
    "00000000010000000000000000000000000000000100000000000000030000000000000002000000",
    "00000000020165000000000000000000000000000000030000000000000002040000000000000056",
    "424c454e443032bb8ceb4400000000",
];
const CHECKPOINT_COMMANDS: &[&str] = &[
    "56424c43505430310900000000000000010000005a010000a9050a12000000000001000000000000",
    "00000000000000000001000000000000000100000000000000010300000000010000000000000000",
    "02000000000000000003000000000000000300000001000000000000000100000000000000000000",
    "00000000000100000000000000020000000000000002000000000000000000000000000000010000",
    "00000000000300000000000000030000000000000000000000000000000100000000000000020000",
    "00000000000100000000000000000101000000000000000000000000000000010000000000000001",
    "00000000000000020000000000000000000000000000000100000000000000010100000000000000",
    "03000000010000000000000001000000000000000002000000000000000100000000000000010700",
    "00000000000000000000000000000000000003000000000000000200000000000000010800000000",
    "00000000000000000000000300000001020356424c454e443032b327859c00000000",
];

fn golden(parts: &[&str]) -> String {
    parts.concat()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn command_entries() -> Vec<LogEntry> {
    vec![
        LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Noop,
        },
        LogEntry {
            index: 2,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(7).unwrap(),
                bytes: Vec::new().into(),
            },
        },
        LogEntry {
            index: 3,
            term: 2,
            payload: EntryPayload::Command {
                operation: OperationId::new(8).unwrap(),
                bytes: vec![1, 2, 3].into(),
            },
        },
    ]
}
fn pad(after: (u64, u64), count: u64) -> impl Iterator<Item = LogEntry> {
    (1..=count).map(move |n| LogEntry {
        index: after.0 + n,
        term: after.1,
        payload: EntryPayload::Noop,
    })
}
/// The same logical entries as one owned batch, as a subrange of one wide
/// shared backing, and as a range across two shared backings.
fn partitions(entries: Vec<LogEntry>) -> Vec<EntryBatch> {
    let whole: EntryBatch = entries.clone().into();
    let first = entries[0].index;
    let last = entries.last().map(|e| (e.index, e.term)).unwrap();
    let mut wide = entries.clone();
    wide.extend(pad(last, 20));
    let shared = EntryLog::from(wide).shared_range(0..entries.len()).unwrap();
    // Two backings of at least the sharing width, split after the first entry.
    let mut head: Vec<LogEntry> = pad((first.saturating_sub(21), 1), 20).collect();
    head.push(entries[0].clone());
    let mut tail = entries[1..].to_vec();
    tail.extend(pad(last, 20));
    let mut log = EntryLog::from(head);
    log.append(tail.into());
    let split = log.shared_range(20..20 + entries.len()).unwrap();
    assert_eq!(split.slices().count(), 2);
    vec![whole, shared, split]
}
fn command_append(entries: EntryBatch) -> Message {
    message(Rpc::Append {
        previous_index: 0,
        previous_term: 0,
        entries,
        leader_commit: 1,
    })
}
fn update(entries: EntryBatch) -> LogMutation {
    LogMutation::Update(LogUpdate {
        group: group(1),
        expected_revision: LogRevision::new(1).unwrap(),
        hard_state: voteboat::contracts::HardState {
            term: 2,
            voted_for: None,
        },
        commit_index: 1,
        suffix: Some(Suffix { from: 1, entries }),
        snapshot: None,
        snapshot_membership: None,
    })
}
fn checkpoint(entries: EntryBatch) -> Vec<u8> {
    let mut state = std::collections::BTreeMap::new();
    let limits = LogLimits::default();
    apply_batch(&mut state, &[LogMutation::Create(bootstrap(1, 3))], limits).unwrap();
    apply_batch(&mut state, &[update(entries)], limits).unwrap();
    NativeLogCodec
        .encode_checkpoint(9, &state, limits, 1 << 20)
        .unwrap()
}
fn wal(entries: EntryBatch) -> Vec<u8> {
    NativeLogCodec
        .encode_batch(5, &[update(entries)], LogLimits::default())
        .unwrap()
}

#[test]
fn entry_encodings_are_byte_identical_for_every_partition() {
    for entries in partitions(command_entries()) {
        let frame = frame(command_append(entries.clone()));
        assert_eq!(hex(&frame), golden(WIRE_COMMANDS));
        assert_eq!(
            codec().decode_batch(scope(), &frame).unwrap(),
            [command_append(entries.clone())]
        );
        assert_eq!(hex(&wal(entries.clone())), golden(WAL_COMMANDS));
        assert_eq!(hex(&checkpoint(entries)), golden(CHECKPOINT_COMMANDS));
    }
    for entries in partitions(configuration_entries()) {
        let Rpc::Append {
            previous_index,
            previous_term,
            leader_commit,
            ..
        } = membership_append().rpc
        else {
            unreachable!()
        };
        let mut message = membership_append();
        message.rpc = Rpc::Append {
            previous_index,
            previous_term,
            entries: entries.clone(),
            leader_commit,
        };
        let bytes = membership_codec()
            .encode_batch(scope(), &[message])
            .unwrap();
        assert_eq!(hex(&bytes), golden(WIRE_CONFIGURATION));
        assert_eq!(hex(&wal(entries)), golden(WAL_CONFIGURATION));
    }
}
