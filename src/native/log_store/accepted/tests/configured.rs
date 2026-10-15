// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Accepted configured deltas retain prefix ownership and original publication.
use super::*;
use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
use crate::snapshot::SnapshotRef;

fn configured() -> Store {
    let mut store = open(2);
    let state = store.state(group(1)).unwrap();
    let learners = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        state.bootstrap.policy.clone(),
        state.bootstrap.voter_stores.clone(),
        BTreeMap::from([(
            NodeId::new(2).unwrap(),
            StoreIdentity {
                id: StoreId::new(2).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
        )]),
    )
    .unwrap();
    let mut record = entry(1, 1);
    record.payload = EntryPayload::Configuration(Box::new(ConfigurationRecord {
        operation: OperationId::new(100).unwrap(),
        expected: state.bootstrap.configuration,
        change: ConfigurationChange::Learners(learners),
    }));
    let update = mutation(
        &store,
        1,
        1,
        Some(Suffix {
            from: 1,
            entries: vec![record],
        }),
    );
    commit(&mut store, vec![update]);
    store
}

#[test]
fn configured_multiple_accepted_deltas_subset_publication_and_full_fallback_recover() {
    let mut store = configured();
    let state = store.state(group(1)).unwrap();
    let update = mutation(
        &store,
        1,
        1,
        Some(Suffix {
            from: 2,
            entries: vec![entry(2, 1)],
        }),
    );
    let first = store.append_batch(vec![update]).unwrap();
    assert!(!store.accepted.summaries[&group(1)].is_plain());
    let pointer = payload_pointer(&tail(&store)[0]);
    let update = mutation(
        &store,
        1,
        2,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)],
        }),
    );
    let second = store.append_batch(vec![update]).unwrap();
    assert_eq!(payload_pointer(&tail(&store)[0]), pointer);
    assert_eq!(store.state(group(1)).unwrap(), state);
    let update = mutation(&store, 2, 0, None);
    let other = store.append_batch(vec![update]).unwrap();
    assert_eq!(store.barrier(&other).unwrap().tickets, other);
    assert_eq!(
        payload_pointer(&store.durable[&group(1)].entries[1]),
        pointer
    );
    assert!(!store.accepted.summaries[&group(1)].is_plain());
    let update = mutation(
        &store,
        1,
        2,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 2)],
        }),
    );
    let replaced = store.append_batch(vec![update]).unwrap();
    assert!(matches!(
        &store.accepted.overlays[&group(1)],
        Overlay::Full(_)
    ));
    assert_eq!(store.barrier(&first), Err(StorageError::StaleTicket));
    assert_eq!(store.barrier(&second), Err(StorageError::StaleTicket));
    store.barrier(&replaced).unwrap();
    let state = store.state(group(1)).unwrap();
    let mut snapshot = match mutation_raw(&state, 2, None) {
        LogMutation::Update(update) => update,
        LogMutation::Create(_) => unreachable!(),
    };
    snapshot.snapshot = Some(SnapshotRef {
        store: store.binding.identity,
        group: group(1),
        configuration: ConfigurationId::new(2).unwrap(),
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 2,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    });
    snapshot.snapshot_membership = state.checkpoint_membership(2).unwrap();
    commit(&mut store, vec![LogMutation::Update(snapshot)]);
    let update = mutation(
        &store,
        1,
        3,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 2)],
        }),
    );
    let tickets = store.append_batch(vec![update]).unwrap();
    assert!(matches!(
        &store.accepted.overlays[&group(1)],
        Overlay::Plain(_)
    ));
    store.barrier(&tickets).unwrap();
    let expected = store.state(group(1)).unwrap();
    let io = store.io.clone();
    let identity = store.binding.identity;
    drop(store);
    let recovered = NativeLogStore::recover(io, identity, LogLimits::default()).unwrap();
    assert_eq!(recovered.state(group(1)).unwrap(), expected);
    assert!(!recovered.accepted.summaries[&group(1)].is_plain());
}

#[test]
fn configured_delta_uncertainty_fences_and_recovers_complete_transition() {
    for fault in [Fault::Append, Fault::Sync, Fault::Publish] {
        let mut store = configured();
        let before = store.state(group(1)).unwrap();
        let io = store.io.clone();
        let identity = store.binding.identity;
        let update = mutation(
            &store,
            1,
            1,
            Some(Suffix {
                from: 2,
                entries: vec![entry(2, 1)],
            }),
        );
        let mut expected = BTreeMap::from([(group(1), before.clone())]);
        apply_batch(
            &mut expected,
            std::slice::from_ref(&update),
            LogLimits::default(),
        )
        .unwrap();
        io.0.borrow_mut().fault = fault;
        match store.append_batch(vec![update]) {
            Err(StorageError::Uncertain(_)) => (),
            Ok(tickets) => assert!(matches!(
                store.barrier(&tickets),
                Err(StorageError::Uncertain(_))
            )),
            other => panic!("unexpected admission {other:?}"),
        }
        assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
        drop(store);
        io.0.borrow_mut().fault = Fault::None;
        let recovered = NativeLogStore::recover(io, identity, LogLimits::default()).unwrap();
        // This I/O fixture retains completed writes across reopen even when
        // synchronization fails; recovery may retain that complete record.
        // Neither outcome permits a receipt to escape the fenced instance.
        let state = recovered.state(group(1)).unwrap();
        assert!(state == before || state == expected[&group(1)]);
        if matches!(fault, Fault::Append) {
            assert_eq!(state, before);
        }
    }
}
