// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
mod checkpoint;
mod compact;
mod configured;
mod fixture;
mod incoming;
mod incoming_codec;
mod torn_tail;
use fixture::*;

fn payload_pointer(entry: &LogEntry) -> *const u8 {
    let EntryPayload::Command { bytes, .. } = &entry.payload else {
        panic!("expected payload")
    };
    bytes.as_ptr()
}
fn tail(store: &Store) -> &Vec<LogEntry> {
    let Overlay::Plain(plain) = &store.accepted.overlays[&group(1)] else {
        panic!("plain overlay required")
    };
    &plain.tail
}

#[test]
fn original_prefix_and_accepted_tail_allocations_survive_appends_and_subset_barriers() {
    let mut store = open(1);
    let entries = (1..=128).map(|index| entry(index, 1)).collect();
    let initial = mutation(&store, 1, 128, Some(Suffix { from: 1, entries }));
    commit(&mut store, vec![initial]);
    let original = payload_pointer(&store.durable[&group(1)].entries[0]);
    let mut input = entry(129, 1);
    let EntryPayload::Command { bytes, .. } = &mut input.payload else {
        unreachable!()
    };
    let mut raw = bytes.to_vec();
    raw.reserve(4096);
    *bytes = raw.into();
    let first_update = mutation(
        &store,
        1,
        128,
        Some(Suffix {
            from: 129,
            entries: vec![input],
        }),
    );
    let first = store.append_batch(vec![first_update]).unwrap();
    let accepted_pointer = payload_pointer(&tail(&store)[0]);
    let EntryPayload::Command { bytes, .. } = &tail(&store)[0].payload else {
        unreachable!()
    };
    assert_eq!(
        bytes.retained_bytes(),
        bytes.len() + 2 * std::mem::size_of::<usize>()
    );
    let middle_update = mutation(&store, 1, 129, None);
    let middle = store.append_batch(vec![middle_update]).unwrap();
    let second_update = mutation(
        &store,
        1,
        129,
        Some(Suffix {
            from: 130,
            entries: vec![entry(130, 1)],
        }),
    );
    let second = store.append_batch(vec![second_update]).unwrap();
    assert_eq!(store.durable[&group(1)].last_index(), 128);
    assert_eq!(store.durable[&group(1)].commit_index, 128);
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[0]),
        original
    );
    assert_eq!(payload_pointer(&tail(&store)[0]), accepted_pointer);
    assert_eq!(store.barrier(&first).unwrap().tickets, first);
    assert!(store.accepted.is_clean());
    assert_eq!(store.durable[&group(1)].last_index(), 130);
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[0]),
        original
    );
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[128]),
        accepted_pointer
    );
    let newer_update = mutation(
        &store,
        1,
        130,
        Some(Suffix {
            from: 131,
            entries: vec![entry(131, 1)],
        }),
    );
    let newer = store.append_batch(vec![newer_update]).unwrap();
    assert_eq!(store.barrier(&middle).unwrap().tickets, middle);
    assert_eq!(store.durable[&group(1)].last_index(), 131);
    assert_eq!(store.barrier(&second).unwrap().tickets, second);
    assert_eq!(store.barrier(&newer).unwrap().tickets, newer);
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[0]),
        original
    );
    assert!(store.pending.is_empty());
}

#[test]
fn metadata_update_does_not_allocate_a_second_retained_log() {
    let mut store = open(1);
    let update = mutation(
        &store,
        1,
        0,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1)],
        }),
    );
    commit(&mut store, vec![update]);
    let pointer = payload_pointer(&store.durable[&group(1)].entries[0]);
    let update = mutation(&store, 1, 1, None);
    let tickets = store.append_batch(vec![update]).unwrap();
    assert!(tail(&store).is_empty());
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[0]),
        pointer
    );
    store.barrier(&tickets).unwrap();
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[0]),
        pointer
    );
}

#[test]
fn invalid_later_group_and_stale_summary_refuse_before_any_append() {
    let mut store = open(2);
    let before = store.io.0.borrow().bytes.len();
    let first = mutation(
        &store,
        1,
        0,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1)],
        }),
    );
    let LogMutation::Update(mut second) = mutation(&store, 2, 0, None) else {
        unreachable!()
    };
    second.expected_revision = LogRevision::new(100).unwrap();
    assert!(store
        .append_batch(vec![first, LogMutation::Update(second)])
        .is_err());
    assert_eq!(store.io.0.borrow().bytes.len(), before);
    assert!(store.accepted.is_clean());
    assert!(store.pending.is_empty());
    let mut stale = store.durable[&group(1)].clone();
    stale.revision = LogRevision::new(stale.revision.get() + 1).unwrap();
    store
        .accepted
        .summaries
        .insert(group(1), LogSummary::from_validated(&stale));
    let update = mutation_raw(&store.durable[&group(1)], 0, None);
    assert_eq!(
        store.append_batch(vec![update]),
        Err(StorageError::Rejected("stale log summary"))
    );
    assert_eq!(store.io.0.borrow().bytes.len(), before);
}

#[test]
fn uncertain_append_sync_or_publication_does_not_promote_an_overlay() {
    for fault in [Fault::Append, Fault::Sync, Fault::Publish] {
        let mut store = open(1);
        store.io.0.borrow_mut().fault = fault;
        let update = mutation(
            &store,
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1)],
            }),
        );
        match store.append_batch(vec![update]) {
            Err(StorageError::Uncertain(_)) => assert!(store.accepted.is_clean()),
            Ok(tickets) => {
                assert!(matches!(
                    store.barrier(&tickets),
                    Err(StorageError::Uncertain(_))
                ));
                assert!(!store.accepted.is_clean());
            }
            other => panic!("unexpected {other:?}"),
        }
        assert!(store.fenced);
        assert!(store.durable[&group(1)].entries.is_empty());
        assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
    }
}

#[test]
fn replacement_materializes_the_exact_accepted_tail_and_invalidates_old_generation() {
    let mut store = open(1);
    let first_update = mutation(
        &store,
        1,
        0,
        Some(Suffix {
            from: 1,
            entries: vec![entry(1, 1), entry(2, 1)],
        }),
    );
    let first = store.append_batch(vec![first_update]).unwrap();
    let second_update = mutation(&store, 1, 1, None);
    let second = store.append_batch(vec![second_update]).unwrap();
    let replacement = mutation(
        &store,
        1,
        1,
        Some(Suffix {
            from: 2,
            entries: vec![entry(2, 2)],
        }),
    );
    let replacement = store.append_batch(vec![replacement]).unwrap();
    assert!(matches!(
        store.accepted.overlays[&group(1)],
        Overlay::Full(_)
    ));
    assert_eq!(store.barrier(&first), Err(StorageError::StaleTicket));
    assert_eq!(store.barrier(&second), Err(StorageError::StaleTicket));
    store.barrier(&replacement).unwrap();
    assert_eq!(
        store.state(group(1)).unwrap().entries,
        vec![entry(1, 1), entry(2, 2)]
    );
    let update = mutation(&store, 1, 2, None);
    let tickets = store.append_batch(vec![update]).unwrap();
    assert!(matches!(
        store.accepted.overlays[&group(1)],
        Overlay::Plain(_)
    ));
    store.barrier(&tickets).unwrap();
}

#[test]
fn creations_obey_accepted_identity_and_batch_group_limits() {
    let mut store = open(1);
    let create = LogMutation::Create(bootstrap(2));
    let ticket = store.append_batch(vec![create.clone()]).unwrap();
    assert!(store.append_batch(vec![create]).is_err());
    let mut replacement_identity = bootstrap(2);
    replacement_identity.group.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(store
        .append_batch(vec![LogMutation::Create(replacement_identity)])
        .is_err());
    store.barrier(&ticket).unwrap();
    store.limits.max_groups = 3;
    assert!(store
        .append_batch(vec![
            LogMutation::Create(bootstrap(3)),
            LogMutation::Create(bootstrap(4))
        ])
        .is_err());
    assert!(store.accepted.is_clean());
}
