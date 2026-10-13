// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn compact(store: &Store, index: u64) -> LogMutation {
    let state = store
        .accepted
        .view(&store.durable, group(1))
        .unwrap()
        .0
        .materialize();
    let LogMutation::Update(mut update) = mutation_raw(&state, state.commit_index, None) else {
        unreachable!()
    };
    update.snapshot = Some(crate::snapshot::SnapshotRef {
        store: store.binding.identity,
        group: group(1),
        configuration: state.bootstrap.configuration,
        generation: SnapshotGeneration::new(index).unwrap(),
        index,
        term: state.term_at(index).unwrap(),
        application_schema: 1,
        file_bytes: 100,
        checksum: index as u32,
    });
    LogMutation::Update(update)
}
fn accept(
    store: &mut Store,
    oracle: &mut BTreeMap<GroupIdentity, GroupLog>,
    mutation: LogMutation,
) -> Vec<LogTicket> {
    apply_batch(oracle, std::slice::from_ref(&mutation), store.limits).unwrap();
    let tickets = store.append_batch(vec![mutation]).unwrap();
    for (&group, state) in oracle.iter() {
        assert_eq!(
            store
                .accepted
                .view(&store.durable, group)
                .unwrap()
                .0
                .materialize(),
            *state
        );
    }
    tickets
}
#[test]
fn deferred_trim_composes_with_accepted_tail_and_subset_publication() {
    let mut store = open(3);
    let initial = mutation(
        &store,
        1,
        3,
        Some(Suffix {
            from: 1,
            entries: (1..=3).map(|i| entry(i, 1)).collect(),
        }),
    );
    commit(&mut store, vec![initial]);
    let untouched = store.durable[&group(3)].clone();
    let original = payload_pointer(&store.durable[&group(1)].entries[2]);
    let mut oracle = store.durable.clone();
    let update = mutation(
        &store,
        1,
        5,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 1), entry(5, 1)],
        }),
    );
    let old = accept(&mut store, &mut oracle, update);
    let tail_pointer = payload_pointer(&tail(&store)[1]);
    let update = compact(&store, 2);
    let first = accept(&mut store, &mut oracle, update);
    assert_eq!(store.durable[&group(1)].base_index(), 0);
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[2]),
        original
    );
    let update = mutation(
        &store,
        1,
        5,
        Some(Suffix {
            from: 6,
            entries: vec![entry(6, 1)],
        }),
    );
    let appended = accept(&mut store, &mut oracle, update);
    let update = compact(&store, 4); // Removes the rest of durable prefix and one accepted tail entry.
    let second = accept(&mut store, &mut oracle, update);
    assert_eq!(payload_pointer(&tail(&store)[0]), tail_pointer);
    let update = mutation(
        &store,
        1,
        5,
        Some(Suffix {
            from: 7,
            entries: vec![entry(7, 1)],
        }),
    );
    let later = accept(&mut store, &mut oracle, update);
    assert_eq!(store.barrier(&old), Err(StorageError::StaleTicket));
    assert_eq!(store.barrier(&first), Err(StorageError::StaleTicket));
    assert_eq!(store.barrier(&appended), Err(StorageError::StaleTicket));
    let update = mutation(&store, 2, 0, None);
    let other = accept(&mut store, &mut oracle, update);
    assert_eq!(store.barrier(&other).unwrap().tickets, other);
    assert!(store.accepted.is_clean());
    assert_eq!(store.durable, oracle);
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[0]),
        tail_pointer
    );
    assert_eq!(store.durable[&group(3)], untouched);
    assert_eq!(store.barrier(&second).unwrap().tickets, second);
    assert_eq!(store.barrier(&later).unwrap().tickets, later);
}
#[test]
fn invalid_later_unit_does_not_trim_or_write_the_earlier_compaction() {
    let mut store = open(2);
    let initial = mutation(
        &store,
        1,
        2,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1), entry(2, 1)],
        }),
    );
    commit(&mut store, vec![initial]);
    let before = store.durable.clone();
    let bytes = store.io.0.borrow().bytes.clone();
    let first = compact(&store, 1);
    let LogMutation::Update(mut invalid) = mutation(&store, 2, 0, None) else {
        unreachable!()
    };
    invalid.expected_revision = LogRevision::new(99).unwrap();
    assert!(store
        .append_batch(vec![first, LogMutation::Update(invalid)])
        .is_err());
    assert_eq!(store.durable, before);
    assert_eq!(store.io.0.borrow().bytes, bytes);
    assert!(store.accepted.is_clean());
}

#[test]
fn trimmed_accepted_view_materializes_exact_configuration_and_replacement_fallback() {
    use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
    for configuration in [false, true] {
        let mut store = open(1);
        let initial = mutation(
            &store,
            1,
            2,
            Some(Suffix {
                from: 1,
                entries: (1..=3).map(|i| entry(i, 1)).collect(),
            }),
        );
        commit(&mut store, vec![initial]);
        let before = store.durable[&group(1)].clone();
        let mut oracle = store.durable.clone();
        let change = compact(&store, 1);
        accept(&mut store, &mut oracle, change);
        let (from, payload) = if configuration {
            let state = &oracle[&group(1)];
            let record = ConfigurationRecord {
                operation: OperationId::new(100).unwrap(),
                expected: state.bootstrap.configuration,
                change: ConfigurationChange::Learners(
                    Configuration::new(
                        ConfigurationId::new(2).unwrap(),
                        state.bootstrap.policy.clone(),
                        state.bootstrap.voter_stores.clone(),
                        BTreeMap::new(),
                    )
                    .unwrap(),
                ),
            };
            let mut next = entry(4, 1);
            next.payload = EntryPayload::Configuration(Box::new(record));
            (4, next)
        } else {
            (3, entry(3, 2))
        };
        let update = mutation(
            &store,
            1,
            2,
            Some(Suffix {
                from,
                entries: vec![payload],
            }),
        );
        let ticket = accept(&mut store, &mut oracle, update);
        assert!(matches!(
            store.accepted.overlays[&group(1)],
            Overlay::Full(_)
        ));
        assert_eq!(store.durable[&group(1)], before);
        store.barrier(&ticket).unwrap();
        assert_eq!(store.durable, oracle);
    }
}
