// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::BTreeMap;
#[test]
fn every_ready_quantum_preserves_exact_power_loss_frontier_for_proposal_and_commit() {
    for items in [1, 16, 32, 64] {
        for commit_phase in [false, true] {
            for fault in [
                Fault::Append(10),
                Fault::Sync,
                Fault::PublishBefore,
                Fault::PublishAfter,
            ] {
                publication_cut(items, commit_phase, fault);
            }
        }
    }
}
fn publication_cut(items: usize, commit_phase: bool, fault: Fault) {
    let io = ModelIo::default();
    let (mut store, mut core) = setup_with_batch_limits(io.clone(), BatchLimits::default());
    let effects = core
        .propose_batch(
            (1..=items)
                .map(|id| Proposal {
                    operation: voteboat::identity::OperationId::new(id as u128).unwrap(),
                    bytes: 1i64.to_le_bytes().to_vec(),
                })
                .collect(),
        )
        .unwrap();
    let mut update = persist(effects);
    if commit_phase {
        update = persist(persist_effect(&mut core, &mut store, update).unwrap());
    }
    let before = store.state(group(1)).unwrap();
    let mut oracle = BTreeMap::from([(group(1), before.clone())]);
    apply_batch(
        &mut oracle,
        &[LogMutation::Update(update.clone())],
        store.limits(),
    )
    .unwrap();
    io.0.borrow_mut().fault = fault;
    assert!(persist_effect(&mut core, &mut store, update).is_err());
    assert!(core.is_fenced());
    assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
    drop(store);
    io.0.borrow_mut().power_loss();
    let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
    let expected = if matches!(fault, Fault::Append(_) | Fault::Sync) {
        &before
    } else {
        &oracle[&group(1)]
    };
    assert_eq!(recovered.state(group(1)).unwrap(), *expected);
}
fn persist(effects: Vec<Effect>) -> LogUpdate {
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("exact one persistence dependency")
    };
    update.clone()
}
