// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{identity::SnapshotGeneration, snapshot::SnapshotRef};
fn fixture() -> (ModelIo, NativeLogStore<ModelIo>, GroupLog, LogMutation) {
    let io = ModelIo::default();
    let mut store = NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    let state = store.state(group(1)).unwrap();
    append(
        &mut store,
        vec![update(
            &state,
            1,
            3,
            Some(Suffix {
                from: 1,
                entries: (1..=4).map(|i| entry(i, 1, i as u8)).collect(),
            }),
        )],
    );
    let old = store.state(group(1)).unwrap();
    let LogMutation::Update(mut compact) = update(&old, 1, 3, None) else {
        unreachable!()
    };
    compact.snapshot = Some(SnapshotRef {
        store: identity(1),
        group: group(1),
        configuration: old.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 2,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    });
    (io, store, old, LogMutation::Update(compact))
}
#[test]
fn compaction_uncertainty_and_power_loss_preserve_exact_sync_frontier() {
    for fault in [
        Fault::Append(10),
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ] {
        let (io, mut store, old, compact) = fixture();
        let mut oracle = std::collections::BTreeMap::from([(group(1), old.clone())]);
        apply_batch(
            &mut oracle,
            std::slice::from_ref(&compact),
            LogLimits::default(),
        )
        .unwrap();
        io.0.borrow_mut().fault = fault;
        let attempted = store.append_batch(vec![compact]);
        match &attempted {
            Err(StorageError::Uncertain(_)) => assert!(matches!(fault, Fault::Append(_))),
            Ok(tickets) => assert!(matches!(
                store.barrier(tickets),
                Err(StorageError::Uncertain(_))
            )),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
        assert!(matches!(
            store.append_batch(vec![update(&old, 1, 3, None)]),
            Err(StorageError::Fenced)
        ));
        drop(store);
        io.0.borrow_mut().power_loss();
        let mut recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        let expected = if matches!(fault, Fault::Append(_) | Fault::Sync) {
            &old
        } else {
            &oracle[&group(1)]
        };
        assert_eq!(recovered.state(group(1)).unwrap(), *expected);
        if let Ok(tickets) = attempted {
            assert_eq!(recovered.barrier(&tickets), Err(StorageError::StaleTicket));
        }
    }
}
