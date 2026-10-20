// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Failing inputs for every direct-restore check. Byte cases start from the
//! populated golden and edit one documented field, so each test names the
//! exact field it covers. Most edits leave every other check satisfied, so
//! removing the check under test lets the bytes restore. Three inputs are
//! also refused by a second layer: swapped records by the exact re-encode
//! (records are a sorted map), reordered configuration entries by the
//! membership fold, and a third snapshot by the metadata read-envelope bound.
//! State cases encode a reachable state with one invariant broken. Budget
//! cases fill the retained set to an admission limit and one past it.
use super::*;
use crate::logical_voter::codec::{Decoder, Encoder};
use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
use std::ops::Range;

/// Byte offsets of one encoded record.
struct Record {
    span: Range<usize>,
    /// The outcome tag; the receipt's revision, generation and epoch are the
    /// last 24 bytes of the span.
    outcome: usize,
}
impl Record {
    fn index(&self) -> usize {
        self.span.start + 16
    }
    fn term(&self) -> usize {
        self.span.start + 24
    }
    fn preceding(&self) -> usize {
        self.span.start + 32
    }
    fn epoch(&self) -> usize {
        self.span.end - 8
    }
}
/// Byte offsets of the fields the cases edit.
struct Layout {
    mark: usize,
    records: Vec<Record>,
    configurations: Vec<Range<usize>>,
    /// Flag byte, then the binding when set.
    representative: Range<usize>,
    /// Flag byte, then the image blob when set.
    parent: Range<usize>,
    snapshot_count: usize,
    snapshots: Vec<Range<usize>>,
}
fn layout(a: &LogicalApplication, bytes: &[u8]) -> Layout {
    let mut d = Decoder::new(bytes);
    d.take(8).unwrap();
    d.blob(usize::MAX).unwrap();
    d.take(8 + 8 + 16).unwrap();
    let mark = d.offset;
    d.u64().unwrap();
    let mut records = Vec::new();
    for _ in 0..d.u64().unwrap() {
        let start = d.offset;
        d.take(16 + 8 + 8 + 8).unwrap();
        d.blob(usize::MAX).unwrap();
        let outcome = d.offset;
        match d.u8().unwrap() {
            2 => drop(d.reference().unwrap()),
            5 => drop(d.u8().unwrap()),
            _ => {}
        }
        d.take(24).unwrap();
        records.push(Record {
            span: start..d.offset,
            outcome,
        });
    }
    let mut configurations = Vec::new();
    for _ in 0..d.u64().unwrap() {
        let start = d.offset;
        d.blob(usize::MAX).unwrap();
        configurations.push(start..d.offset);
    }
    if a.candidate() && d.flag().unwrap() {
        a.decode_enrollment(&mut d).unwrap();
    }
    let start = d.offset;
    if d.flag().unwrap() {
        d.representative().unwrap();
    }
    let representative = start..d.offset;
    let start = d.offset;
    if d.flag().unwrap() {
        d.blob(usize::MAX).unwrap();
    }
    let parent = start..d.offset;
    d.blob(usize::MAX).unwrap();
    d.optional_reference().unwrap();
    let snapshot_count = d.offset;
    let mut snapshots = Vec::new();
    for _ in 0..d.u64().unwrap() {
        let start = d.offset;
        d.reference().unwrap();
        d.blob(usize::MAX).unwrap();
        snapshots.push(start..d.offset);
    }
    d.u64().unwrap();
    d.finish().unwrap();
    Layout {
        mark,
        records,
        configurations,
        representative,
        parent,
        snapshot_count,
        snapshots,
    }
}
fn read(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}
fn write(bytes: &[u8], at: usize, n: u64) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    bytes[at..at + 8].copy_from_slice(&n.to_le_bytes());
    bytes
}
fn splice(bytes: &[u8], range: Range<usize>, with: &[u8]) -> Vec<u8> {
    [&bytes[..range.start], with, &bytes[range.end..]].concat()
}
/// `bytes` is refused with `InvalidCheckpoint` and leaves `target` unchanged.
fn assert_refused(target: &mut LogicalApplication, applied: u64, bytes: &[u8], name: &str) {
    let before = target.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        target.restore_checkpoint(target.schema_version(), applied, bytes),
        Err(ApplicationError::InvalidCheckpoint),
        "{name}"
    );
    assert_eq!(target.checkpoint(usize::MAX).unwrap(), before, "{name}");
}
#[test]
fn reordered_colliding_and_field_corruptions_are_refused() {
    let a = history::populated();
    let good = a.checkpoint(usize::MAX).unwrap();
    let l = layout(&a, &good);
    let (r0, r1) = (&l.records[0], &l.records[1]);
    // The populated history: the child configuration entry at index 1; after
    // retirement the reconcile refusal (sequence 7) at index 8, term 1, and
    // the final mutation (sequence 8) at index 10, term 2, are retained, both
    // after configuration 2.
    assert_eq!(l.records.len(), 2);
    assert_eq!(l.configurations.len(), 1);
    assert_eq!([read(&good, r0.index()), read(&good, r1.index())], [8, 10]);
    assert_eq!([read(&good, r0.term()), read(&good, r1.term())], [1, 2]);
    assert_eq!(read(&good, r0.preceding()), 2);
    assert_eq!(read(&good, r0.epoch()), 1);
    assert_eq!(read(&good, l.mark), 2);
    assert_eq!(l.snapshots.len(), 2);
    let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
    // Records must be in strictly ascending operation ID.
    let swapped = [
        &good[..r0.span.start],
        &good[r1.span.clone()],
        &good[r0.span.clone()],
        &good[r1.span.end..],
    ]
    .concat();
    cases.push(("records swapped", swapped));
    // A record at the configuration entry's index, with the configuration in
    // force before index 1, so only the position collision is wrong.
    let collide = write(&write(&good, r0.index(), 1), r0.preceding(), 1);
    cases.push(("record and configuration at one index", collide));
    // Two records at one index, otherwise consistent.
    cases.push(("duplicate record index", write(&good, r1.index(), 8)));
    // A configuration entry twice.
    let c = l.configurations[0].clone();
    let twice = [
        &good[..c.start - 8],
        &2u64.to_le_bytes(),
        &good[c.clone()],
        &good[c.clone()],
        &good[c.end..],
    ]
    .concat();
    cases.push(("duplicate configuration entry", twice));
    // Index 8 at term 2 before index 10 at term 1; both terms are within
    // `applied_term`, so only the order is wrong.
    let decrease = write(&write(&good, r0.term(), 2), r1.term(), 1);
    cases.push(("terms decrease", decrease));
    // The representative is present exactly when the parent image is.
    let no_parent = splice(&good, l.parent.clone(), &[0]);
    cases.push(("representative without parent image", no_parent));
    let no_representative = splice(&good, l.representative.clone(), &[0]);
    cases.push(("parent image without representative", no_representative));
    // A receipt epoch above the representative's (epoch 1).
    cases.push(("receipt epoch above current", write(&good, r0.epoch(), 2)));
    // A third, valid and distinct snapshot with a matching generation mark,
    // so only the retained-snapshot count is wrong.
    let third = history::later_snapshot(&a);
    let mut e = Encoder::new(usize::MAX);
    e.reference(
        a.snapshot_reference(&third, SnapshotGeneration::new(3).unwrap())
            .unwrap(),
    )
    .unwrap();
    e.blob(&a.snapshot_bytes(&third).unwrap()).unwrap();
    let end = l.snapshots[1].end;
    let three = splice(&write(&good, l.snapshot_count, 3), end..end, &e.bytes);
    cases.push(("three snapshots", write(&three, l.mark, 3)));
    // Outcome tags: 0..=5, recorded refusal reasons 0..=6. Reason 7
    // (`Expired`) is answered but never recorded.
    let mut outcome = good.clone();
    assert_eq!(outcome[r1.outcome], 1);
    outcome[r1.outcome] = 6;
    cases.push(("outcome tag", outcome));
    assert_eq!(good[r0.outcome], 5);
    for (name, tag) in [("recorded Expired", 7), ("refusal reason", 8)] {
        let mut reason = good.clone();
        reason[r0.outcome + 1] = tag;
        cases.push((name, reason));
    }
    let mut b = history::populated();
    for (name, bytes) in cases {
        assert_refused(&mut b, 10, &bytes, name);
    }
    b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 10, &good)
        .unwrap();
    assert_eq!(b.state, a.state);
}
fn transition(index: u64, expected: u64, change: ConfigurationChange) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(100 + u128::from(index > 1)).unwrap(),
            expected: ConfigurationId::new(expected).unwrap(),
            change,
        })),
    }
}
fn configuration(id: u64, voter: u64, learners: &[u64]) -> Configuration {
    Configuration::new(
        ConfigurationId::new(id).unwrap(),
        Policy::new(
            Tree::Voter(NodeId::new(voter).unwrap()),
            PolicyLimits::default(),
        )
        .unwrap(),
        BTreeMap::from([(NodeId::new(voter).unwrap(), store(u128::from(voter)))]),
        learners
            .iter()
            .map(|n| (NodeId::new(*n).unwrap(), store(u128::from(*n))))
            .collect(),
    )
    .unwrap()
}
#[test]
fn reordered_configuration_entries_are_refused() {
    let mut a = app();
    a.apply_batch(&[
        transition(
            1,
            1,
            ConfigurationChange::Learners(configuration(2, 2, &[3])),
        ),
        transition(
            2,
            2,
            ConfigurationChange::Joint {
                id: ConfigurationId::new(3).unwrap(),
                next: configuration(4, 3, &[]),
            },
        ),
    ])
    .unwrap();
    let good = a.checkpoint(usize::MAX).unwrap();
    let l = layout(&a, &good);
    let (c0, c1) = (l.configurations[0].clone(), l.configurations[1].clone());
    assert_eq!(c0.end, c1.start);
    let swapped = [
        &good[..c0.start],
        &good[c1.clone()],
        &good[c0],
        &good[c1.end..],
    ]
    .concat();
    let mut b = app();
    assert_refused(&mut b, 2, &swapped, "configurations swapped");
    b.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 2, &good)
        .unwrap();
    assert_eq!(b.state, a.state);
}
#[test]
fn unretained_published_or_durable_snapshot_is_refused() {
    let a = history::populated();
    // The parent's durable snapshot is the first publication; the second is
    // the published one. Dropping either keeps the generation floor at 2.
    let durable = a.state.parent.as_ref().unwrap().snapshot.unwrap();
    let published = a.state.published.unwrap();
    assert_ne!(durable, published);
    for (name, dropped) in [("published", published), ("durable", durable)] {
        let mut broken = a.clone();
        broken.state.snapshots.retain(|(r, _)| *r != dropped);
        assert_eq!(broken.state.snapshots.len(), 1);
        let bytes = broken.checkpoint(usize::MAX).unwrap();
        assert_refused(&mut app(), 10, &bytes, name);
    }
}
/// Appends `n` retained copies of the record `template` with fresh
/// operation IDs (in its partition) at the next indexes, as apply stores an
/// original whose admission succeeded. Nothing else in the state changes.
fn retain_copies(a: &mut LogicalApplication, template: OperationId, n: u128) {
    let t = a.state.records[&template].clone();
    let mut c = a.decode_command(&t.command).unwrap();
    let session = template.get() >= command::SESSION_SPACE;
    let next = a
        .state
        .records
        .keys()
        .map(|op| op.get())
        .filter(|op| (*op >= command::SESSION_SPACE) == session)
        .max()
        .unwrap()
        + 1;
    for id in next..next + n {
        c.operation = OperationId::new(id).unwrap();
        let command = a.encode_command(&c, usize::MAX).unwrap();
        let index = a.state.applied + 1;
        a.state.applied = index;
        a.state.bytes += command.len();
        let mut r = t.clone();
        r.operation = c.operation;
        r.first_index = index;
        r.receipt.operation = c.operation;
        r.receipt.index = index;
        r.receipt.first_index = index;
        r.command = command;
        a.state.records.insert(c.operation, r);
    }
}
/// A claim refused at a later epoch with no expected epoch; kept because no
/// session command of the current representative has run since.
fn stale_claim(a: &mut LogicalApplication) -> OperationId {
    let c = command(
        2,
        2,
        CommandBody::Claim {
            expected_epoch: None,
        },
    );
    assert_eq!(
        apply(a, &c).outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
    );
    c.operation
}
/// An ordinary (non-control) original: a parent mutation carrying a client
/// command, refused for a stale (future-session) representative, so it does
/// not move the watermark.
fn stale_ordinary(a: &mut LogicalApplication) -> OperationId {
    let suffix = Suffix {
        from: 1,
        entries: vec![LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(500).unwrap(),
                bytes: vec![1; 64].into(),
            },
        }]
        .into(),
    };
    let c = command(
        2,
        2,
        CommandBody::Mutate {
            expected_generation: a.state.parent.as_ref().unwrap().generation,
            update: update(a, 1, Some(suffix)),
            origin: origin(),
        },
    );
    assert!(!c.control(None));
    assert_eq!(
        apply(a, &c).outcome,
        LogicalOutcome::Refused(LogicalRefusal::StaleRepresentative)
    );
    c.operation
}
/// `a` restores exactly; one more copy of `template` is refused by restore
/// and by admission alike.
fn assert_boundary(mut a: LogicalApplication, template: OperationId, name: &str) {
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut b = a.clone();
    b.state = application::State::new(&a.child_bootstrap).unwrap();
    b.restore_checkpoint(a.schema_version(), a.applied_index(), &bytes)
        .unwrap();
    assert_eq!(b.state, a.state, "{name}");
    let mut c = a
        .decode_command(&a.state.records[&template].command)
        .unwrap();
    // A fresh ID in the template's partition.
    c.operation = OperationId::new(template.get() | (1 << 60)).unwrap();
    let e = entry(&a, &c, a.applied_index() + 1);
    assert_eq!(
        a.apply_batch(&[e]),
        Err(ApplicationError::DedupCapacity),
        "{name}"
    );
    retain_copies(&mut a, template, 1);
    let over = a.checkpoint(usize::MAX).unwrap();
    assert_refused(&mut b, a.applied_index(), &over, name);
}
#[test]
fn restore_enforces_the_retained_byte_budget() {
    let limits = LogicalLimits {
        max_retained_action_bytes: 256 * 1024,
        control_action_bytes: 1,
        ..LogicalLimits::default()
    };
    let mut a = app_with_limits(limits);
    claim(&mut a);
    let template = stale_claim(&mut a);
    let each = a.state.records[&template].command.len();
    let room = (limits.max_retained_action_bytes - a.state.bytes) / each;
    retain_copies(&mut a, template, room as u128);
    assert!(a.state.bytes <= limits.max_retained_action_bytes);
    assert!(a.state.bytes + each > limits.max_retained_action_bytes);
    assert!(a.state.records.len() < limits.max_actions);
    assert_boundary(a, template, "retained bytes");
}
#[test]
fn restore_enforces_the_ordinary_count_reserve() {
    let limits = LogicalLimits {
        max_actions: 64,
        control_actions: 32,
        ..LogicalLimits::default()
    };
    let mut a = app_with_limits(limits);
    claim(&mut a);
    let template = stale_ordinary(&mut a);
    retain_copies(&mut a, template, 31);
    // 32 ordinary originals plus the control claim, well under 64 actions.
    assert_eq!(a.state.records.len(), 33);
    assert_boundary(a, template, "ordinary count");
}
#[test]
fn restore_enforces_the_ordinary_byte_reserve() {
    let mut a = app();
    claim(&mut a);
    let template = stale_ordinary(&mut a);
    let each = a.state.records[&template].command.len();
    // Room for exactly three ordinary originals; the total budget and every
    // count stay far from their limits.
    let max = 256 * 1024;
    let limits = LogicalLimits {
        max_retained_action_bytes: max,
        control_action_bytes: max - 3 * each,
        ..LogicalLimits::default()
    };
    let mut b = app_with_limits(limits);
    claim(&mut b);
    let template = stale_ordinary(&mut b);
    retain_copies(&mut b, template, 2);
    assert!(b.state.bytes + each < max);
    assert_boundary(b, template, "ordinary bytes");
}
#[test]
fn restore_accepts_exactly_max_actions_records() {
    let mut a = app();
    claim(&mut a);
    let template = stale_claim(&mut a);
    let max = a.limits.max_actions;
    retain_copies(&mut a, template, (max - 2) as u128);
    assert_eq!(a.state.records.len(), max);
    assert_boundary(a, template, "max actions");
}
