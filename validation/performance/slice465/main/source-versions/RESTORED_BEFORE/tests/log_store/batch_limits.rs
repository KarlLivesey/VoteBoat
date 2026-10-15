// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Actual WAL publication cuts for large Core originals and their commit marker.
use super::*;
use std::collections::VecDeque;
use voteboat::raft::{persist_effect, BatchLimits, Effect, Event, Proposal, Raft};
#[path = "batch_limits/aggregate.rs"]
mod aggregate;
#[path = "batch_limits/ready_items.rs"]
mod ready_items;

fn setup(io: ModelIo) -> (NativeLogStore<ModelIo>, Raft) {
    setup_with_batch_limits(
        io,
        BatchLimits {
            max_proposal_entries: 4096,
            max_replication_entries: 1024,
        },
    )
}
fn setup_with_batch_limits(io: ModelIo, limits: BatchLimits) -> (NativeLogStore<ModelIo>, Raft) {
    let mut store = NativeLogStore::create(io, identity(1), LogLimits::default()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 1))]);
    let mut core = Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    core.configure_batch_limits(limits).unwrap();
    let effects = core.step(Event::Campaign).unwrap();
    settle(&mut core, &mut store, effects);
    let effects = core
        .step(Event::Propose {
            operation: voteboat::identity::OperationId::new(9999).unwrap(),
            bytes: vec![9],
        })
        .unwrap();
    assert_eq!(settle(&mut core, &mut store, effects), vec![9999]);
    (store, core)
}

fn settle(core: &mut Raft, store: &mut NativeLogStore<ModelIo>, effects: Vec<Effect>) -> Vec<u128> {
    let mut effects = VecDeque::from(effects);
    let mut applied = Vec::new();
    while let Some(effect) = effects.pop_front() {
        match effect {
            Effect::Persist(update) => effects.extend(persist_effect(core, store, update).unwrap()),
            Effect::Committed(entries) => applied.extend(entries.into_iter().filter_map(|entry| {
                if let EntryPayload::Command { operation, .. } = entry.payload {
                    Some(operation.get())
                } else {
                    None
                }
            })),
            other => panic!("unexpected single-voter effect {other:?}"),
        }
    }
    applied
}

fn check_recovery(
    recovered: NativeLogStore<ModelIo>,
    before: &GroupLog,
    old_binding: voteboat::identity::StoreBinding,
    commit_phase: bool,
    fault: Fault,
) {
    let state = recovered.state(group(1)).unwrap();
    let publication_selected = matches!(fault, Fault::PublishBefore | Fault::PublishAfter);
    assert_eq!(
        state.last_index(),
        if commit_phase || publication_selected {
            4098
        } else {
            2
        }
    );
    assert_eq!(
        state.commit_index,
        if commit_phase && publication_selected {
            4098
        } else {
            2
        }
    );
    assert_eq!(&state.entries[..2], &before.entries);
    assert_ne!(recovered.binding().session, old_binding.session);
    let mut restarted =
        Raft::recover(node(1), recovered.binding(), state, recovered.limits()).unwrap();
    assert_eq!(restarted.batch_limits(), BatchLimits::default());
    restarted
        .configure_batch_limits(BatchLimits {
            max_proposal_entries: 4096,
            max_replication_entries: 1024,
        })
        .unwrap();
    let originals: Vec<_> = restarted
        .replay_committed()
        .iter()
        .filter_map(|entry| {
            if let EntryPayload::Command { operation, .. } = &entry.payload {
                Some(operation.get())
            } else {
                None
            }
        })
        .collect();
    let mut expected = vec![9999];
    if commit_phase && publication_selected {
        expected.extend(1..=4096);
    }
    assert_eq!(originals, expected);
}

#[test]
fn large_batch_and_commit_publication_cuts_recover_without_premature_original_results() {
    for commit_phase in [false, true] {
        for fault in [
            Fault::Append(10),
            Fault::Sync,
            Fault::PublishBefore,
            Fault::PublishAfter,
        ] {
            let io = ModelIo::default();
            let (mut store, mut core) = setup(io.clone());
            let before = core.state().clone();
            let old_binding = store.binding();
            let mut effects = core
                .propose_batch(
                    (1..=4096)
                        .map(|id| Proposal {
                            operation: voteboat::identity::OperationId::new(id).unwrap(),
                            bytes: vec![id as u8],
                        })
                        .collect(),
                )
                .unwrap();
            assert_eq!(core.state(), &before);
            assert_eq!(effects.len(), 1);
            if commit_phase {
                let Effect::Persist(update) = effects.remove(0) else {
                    panic!("append required");
                };
                effects = persist_effect(&mut core, &mut store, update).unwrap();
                assert_eq!(core.state().last_index(), 4098);
                assert_eq!(core.state().commit_index, 2);
                assert_eq!(effects.len(), 1);
                assert!(
                    matches!(&effects[0], Effect::Persist(update) if update.commit_index == 4098 && update.suffix.is_none())
                );
            }
            io.0.borrow_mut().fault = fault;
            let Effect::Persist(update) = effects.remove(0) else {
                panic!("original durable dependency required");
            };
            assert!(matches!(
                persist_effect(&mut core, &mut store, update),
                Err(voteboat::raft::RaftError::Storage(StorageError::Uncertain(
                    _
                )))
            ));
            assert!(core.is_fenced());
            assert_eq!(core.state().commit_index, 2);
            drop(store);
            io.0.borrow_mut().power_loss();
            let recovered = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
            check_recovery(recovered, &before, old_binding, commit_phase, fault);
        }
    }
}
