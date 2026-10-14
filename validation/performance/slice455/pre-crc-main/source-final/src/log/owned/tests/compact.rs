// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn compaction_batch_is_atomic_and_retains_surviving_payload_allocation() {
    let mut owned = setup();
    let first = *owned.groups.keys().next().unwrap();
    let LogMutation::Update(mut initial) = update(owned.get(first).unwrap()) else {
        unreachable!()
    };
    initial.commit_index = 2;
    initial.suffix = Some(Suffix {
        from: 1,
        entries: (1..=2)
            .map(|index| LogEntry {
                index,
                term: 1,
                payload: EntryPayload::Command {
                    operation: OperationId::new(u128::from(index)).unwrap(),
                    bytes: vec![7; 8].into(),
                },
            })
            .collect(),
    });
    owned
        .apply_batch(vec![LogMutation::Update(initial)])
        .unwrap();
    let before = states(&owned);
    let EntryPayload::Command { bytes, .. } = &owned.get(first).unwrap().entries[1].payload else {
        unreachable!()
    };
    let pointer = bytes.as_ptr();
    let LogMutation::Update(mut compact) = update(owned.get(first).unwrap()) else {
        unreachable!()
    };
    compact.commit_index = 2;
    compact.snapshot = Some(crate::snapshot::SnapshotRef {
        store: before[&first].bootstrap.voter_stores[&NodeId::new(1).unwrap()],
        group: first,
        configuration: before[&first].bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 1,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    });
    let LogMutation::Update(mut invalid) = update(
        owned
            .groups
            .values()
            .next_back()
            .map(|group| &group.state)
            .unwrap(),
    ) else {
        unreachable!()
    };
    invalid.expected_revision = LogRevision::new(99).unwrap();
    let batch = vec![
        LogMutation::Update(compact.clone()),
        LogMutation::Update(invalid),
    ];
    let mut oracle = before.clone();
    assert_eq!(
        owned.apply_batch(batch.clone()),
        apply_batch(&mut oracle, &batch, owned.limits())
    );
    assert_eq!(states(&owned), before);
    let valid = vec![LogMutation::Update(compact)];
    apply_batch(&mut oracle, &valid, owned.limits()).unwrap();
    owned.apply_batch(valid).unwrap();
    assert_eq!(states(&owned), oracle);
    let EntryPayload::Command { bytes, .. } = &owned.get(first).unwrap().entries[0].payload else {
        unreachable!()
    };
    assert_eq!(bytes.as_ptr(), pointer);
}
