// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real owner transitions; no comparison mark is minted from public metadata.
use super::*;
use crate::raft::{persist_effect, Effect, Event, Raft};
use std::collections::VecDeque;

fn core(store: &Store, id: u128) -> Raft {
    Raft::recover(
        NodeId::new(1).unwrap(),
        store.binding(),
        store.state(group(id)).unwrap(),
        store.limits(),
    )
    .unwrap()
}
fn drive(core: &mut Raft, store: &mut Store, effects: Vec<Effect>) {
    let mut queue: VecDeque<_> = effects.into();
    while let Some(effect) = queue.pop_front() {
        if let Effect::Persist(update) = effect {
            queue.extend(persist_effect(core, store, update).unwrap());
        }
    }
}
fn proposal(core: &mut Raft, store: &mut Store, operation: u128) {
    let effects = core
        .step(Event::Propose {
            operation: OperationId::new(operation).unwrap(),
            bytes: vec![7; 32],
        })
        .unwrap();
    drive(core, store, effects);
}
fn verified(store: &Store) -> Option<(u64, u64)> {
    store
        .checkpoint_comparisons
        .get(&group(1))
        .and_then(CheckpointComparison::verified_interval)
}

#[test]
fn protected_native_comparison_reuses_exact_prefix_and_transfers_compaction_survivors() {
    let mut store = open(1);
    let mut owner = core(&store, 1);
    let effects = owner.step(Event::Campaign).unwrap();
    drive(&mut owner, &mut store, effects);
    for operation in 1..=3 {
        proposal(&mut owner, &mut store, operation);
    }
    assert_eq!(owner.state().entries.len(), 4);
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    let first = store.checkpoint_comparisons[&group(1)].work();
    assert!(first.full);
    assert_eq!((first.compared, first.reused), (4, 0));
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    let again = store.checkpoint_comparisons[&group(1)].work();
    assert!(!again.full);
    assert_eq!((again.compared, again.reused), (0, 4));

    let effects = owner
        .step(Event::Propose {
            operation: OperationId::new(4).unwrap(),
            bytes: vec![8; 32],
        })
        .unwrap();
    let update = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Persist(update) => Some(update.clone()),
            _ => None,
        })
        .unwrap();
    let tickets = store
        .append_batch(vec![LogMutation::Update(update.clone())])
        .unwrap();
    owner.admit_effect(&update, tickets[0]).unwrap();
    // Admission keeps the old durable view and cannot extend verified progress.
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    assert_eq!(verified(&store), Some((1, 4)));
    let completed = store.barrier(&tickets).unwrap();
    assert_eq!(verified(&store), Some((1, 4)));
    let effects = owner.complete(&completed).unwrap();
    drive(&mut owner, &mut store, effects);
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    let append = store.checkpoint_comparisons[&group(1)].work();
    assert!(!append.full);
    assert_eq!((append.compared, append.reused), (1, 4));
    assert_eq!(store.state(group(1)).unwrap(), *owner.state());

    let old_generation = owner.state().generation;
    let reference = crate::snapshot::SnapshotRef {
        store: store.binding().identity,
        group: group(1),
        configuration: owner.state().bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 3,
        term: owner.state().term_at(3).unwrap(),
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    };
    // Exercise only the canonical compaction transition here; snapshot data
    // and pin dependencies are covered by the actual integration histories.
    let effects = owner.begin_compact(reference).unwrap().into_owned();
    drive(&mut owner, &mut store, effects);
    assert_ne!(owner.state().generation, old_generation);
    assert_eq!(verified(&store), Some((4, 5)));
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    let trim = store.checkpoint_comparisons[&group(1)].work();
    assert!(!trim.full);
    assert_eq!((trim.compared, trim.reused), (0, 2));
    proposal(&mut owner, &mut store, 5);
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    let next = store.checkpoint_comparisons[&group(1)].work();
    assert_eq!((next.compared, next.reused), (1, 2));
    assert_eq!(store.state(group(1)).unwrap(), *owner.state());
}

#[test]
fn combined_pending_append_trim_and_subset_barrier_only_preserve_verified_overlap() {
    let mut store = open(2);
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
    let owner = core(&store, 1);
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    let update = mutation(
        &store,
        1,
        5,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 1), entry(5, 1)].into(),
        }),
    );
    let old = store.append_batch(vec![update]).unwrap();
    let state = store
        .accepted
        .view(&store.durable, group(1))
        .unwrap()
        .0
        .materialize();
    let LogMutation::Update(mut update) = mutation_raw(&state, 5, None) else {
        unreachable!()
    };
    update.snapshot = Some(crate::snapshot::SnapshotRef {
        store: store.binding().identity,
        group: group(1),
        configuration: state.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: 2,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    });
    let compact = store
        .append_batch(vec![LogMutation::Update(update)])
        .unwrap();
    let update = mutation(
        &store,
        1,
        5,
        Some(Suffix {
            from: 6,
            entries: vec![entry(6, 1)].into(),
        }),
    );
    let later = store.append_batch(vec![update]).unwrap();
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    assert_eq!(verified(&store), Some((1, 3)));
    let update = mutation(&store, 2, 0, None);
    let other = store.append_batch(vec![update]).unwrap();
    assert_eq!(store.barrier(&old), Err(StorageError::StaleTicket));
    assert_eq!(store.barrier(&other).unwrap().tickets, other);
    assert_eq!(verified(&store), Some((3, 3)));
    assert_eq!(store.durable[&group(1)].entries.len(), 4);
    assert_eq!(store.barrier(&compact).unwrap().tickets, compact);
    assert_eq!(store.barrier(&later).unwrap().tickets, later);
    // The stale core has different complete metadata: a mark cannot override it.
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(false)
    );
    assert_eq!(verified(&store), None);
}

#[test]
fn rewrites_fresh_owners_and_reopen_cannot_reuse_old_prefix_proof() {
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
    let owner = core(&store, 1);
    let other_owner = core(&store, 1);
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    assert_eq!(
        store.matches_checkpoint_state(other_owner.checkpoint_log()),
        Ok(true)
    );
    assert!(store.checkpoint_comparisons[&group(1)].work().full);
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    assert!(store.checkpoint_comparisons[&group(1)].work().full);
    let replacement = mutation(
        &store,
        1,
        2,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 2)].into(),
        }),
    );
    let tickets = store.append_batch(vec![replacement]).unwrap();
    // Safe later plans cannot erase an earlier full/rewrite provenance.
    let append = mutation(
        &store,
        1,
        2,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 2)].into(),
        }),
    );
    let appended = store.append_batch(vec![append]).unwrap();
    let metadata = mutation(&store, 1, 3, None);
    let metadata = store.append_batch(vec![metadata]).unwrap();
    assert!(matches!(
        store.accepted.overlays[&group(1)],
        Overlay::Full(_)
    ));
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(true)
    );
    assert_eq!(verified(&store), Some((1, 3)));
    store.barrier(&tickets).unwrap();
    assert!(store.checkpoint_comparisons.is_empty());
    store.barrier(&appended).unwrap();
    store.barrier(&metadata).unwrap();
    assert_eq!(
        store.matches_checkpoint_state(owner.checkpoint_log()),
        Ok(false)
    );
    let new_owner = core(&store, 1);
    assert_eq!(
        store.matches_checkpoint_state(new_owner.checkpoint_log()),
        Ok(true)
    );
    assert!(store.checkpoint_comparisons[&group(1)].work().full);
    let io = store.io.clone();
    let identity = store.binding().identity;
    drop(store);
    let mut reopened = NativeLogStore::recover(io, identity, LogLimits::default()).unwrap();
    assert!(reopened.checkpoint_comparisons.is_empty());
    assert_eq!(
        reopened.matches_checkpoint_state(new_owner.checkpoint_log()),
        Ok(true)
    );
    assert!(reopened.checkpoint_comparisons[&group(1)].work().full);
}

#[test]
fn failed_publication_fences_before_missing_and_never_advances_verified_prefix() {
    for fault in [Fault::Append, Fault::Sync, Fault::Publish] {
        let mut store = open(1);
        let owner = core(&store, 1);
        assert_eq!(
            store.matches_checkpoint_state(owner.checkpoint_log()),
            Ok(true)
        );
        store.io.0.borrow_mut().fault = fault;
        let update = mutation(
            &store,
            1,
            0,
            Some(Suffix {
                from: 1,
                entries: vec![entry(1, 1)].into(),
            }),
        );
        match store.append_batch(vec![update]) {
            Err(StorageError::Uncertain(_)) => (),
            Ok(tickets) => assert!(matches!(
                store.barrier(&tickets),
                Err(StorageError::Uncertain(_))
            )),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(
            store.matches_checkpoint_state(owner.checkpoint_log()),
            Err(StorageError::Fenced)
        );
        let mut missing_state = owner.state().clone();
        missing_state.bootstrap.group = group(99);
        let missing_owner = Raft::recover(
            NodeId::new(1).unwrap(),
            owner.storage_binding(),
            missing_state,
            store.limits(),
        )
        .unwrap();
        assert_eq!(
            store.matches_checkpoint_state(missing_owner.checkpoint_log()),
            Err(StorageError::Fenced)
        );
        assert_eq!(verified(&store), None);
        assert!(store.durable[&group(1)].entries.is_empty());
    }
}
