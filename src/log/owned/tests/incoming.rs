// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn owned_input_moves_new_payload_after_whole_batch_admission() {
    let mut owned = setup();
    let group = *owned.groups.keys().next().unwrap();
    let LogMutation::Update(mut change) = update(owned.get(group).unwrap()) else {
        unreachable!()
    };
    let bytes: CommandBytes = vec![7; 8].into();
    let pointer = bytes.as_ptr();
    change.suffix = Some(Suffix {
        from: 1,
        entries: vec![LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(1).unwrap(),
                bytes,
            },
        }],
    });
    let raw = vec![LogMutation::Update(change)];
    let mut oracle = states(&owned);
    apply_batch(&mut oracle, &raw, owned.limits()).unwrap();
    owned.apply_batch(raw).unwrap();
    assert_eq!(states(&owned), oracle);
    let EntryPayload::Command { bytes, .. } = &owned.get(group).unwrap().entries[0].payload else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), pointer);
}

#[test]
fn invalid_later_owned_unit_never_installs_an_earlier_moved_append() {
    let mut owned = setup();
    let before = states(&owned);
    let mut batch: Vec<_> = before.values().map(update).collect();
    let LogMutation::Update(first) = &mut batch[0] else {
        unreachable!()
    };
    first.suffix = Some(Suffix {
        from: 1,
        entries: vec![LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Command {
                operation: OperationId::new(1).unwrap(),
                bytes: vec![7; 8].into(),
            },
        }],
    });
    let LogMutation::Update(second) = &mut batch[1] else {
        unreachable!()
    };
    second.expected_revision = LogRevision::new(99).unwrap();
    let mut oracle = before.clone();
    let expected = apply_batch(&mut oracle, &batch, owned.limits());
    assert!(expected.is_err());
    assert_eq!(owned.apply_batch(batch), expected);
    assert_eq!(states(&owned), before);
    assert_eq!(oracle, before);
}
