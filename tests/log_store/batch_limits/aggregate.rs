// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::application::{Counter, CounterOutcome, StateMachine};
const ORIGINALS: u128 = 16384;
fn limits() -> LogLimits {
    LogLimits {
        max_entries_per_group: 65536,
        max_batch_bytes: 4 * 1024 * 1024,
        ..LogLimits::default()
    }
}
fn batch_limits() -> BatchLimits {
    BatchLimits {
        max_proposal_entries: ORIGINALS as usize,
        max_replication_entries: 1024,
    }
}
fn setup_aggregate(io: ModelIo) -> (NativeLogStore<ModelIo>, Raft) {
    let mut store = NativeLogStore::create(io, identity(1), limits()).unwrap();
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 1))]);
    let mut core = Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    core.configure_batch_limits(batch_limits()).unwrap();
    let effects = core.step(Event::Campaign).unwrap();
    settle(&mut core, &mut store, effects);
    (store, core)
}
fn exact_persist(effects: Vec<Effect>) -> LogUpdate {
    let [Effect::Persist(update)] = effects.as_slice() else {
        panic!("one exact publication dependency")
    };
    update.clone()
}
fn replay_counter(core: &Raft) -> Counter {
    let mut app = Counter::new(ORIGINALS as usize + 1).unwrap();
    app.apply_batch(&core.replay_committed()).unwrap();
    app
}
fn retry_after_recovery(mut store: NativeLogStore<ModelIo>, retained: bool) {
    let mut core = Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    assert_eq!(core.batch_limits(), BatchLimits::default());
    core.configure_batch_limits(batch_limits()).unwrap();
    let mut app = replay_counter(&core);
    let mut applied = app.applied_index();
    let effects = core.step(Event::Campaign).unwrap();
    settle(&mut core, &mut store, effects);
    let replay = core.replay_committed();
    app.apply_batch(&replay[replay.partition_point(|entry| entry.index <= applied)..])
        .unwrap();
    applied = app.applied_index();
    let effects = core
        .step(Event::Propose {
            operation: voteboat::identity::OperationId::new(1).unwrap(),
            bytes: 1i64.to_le_bytes().to_vec(),
        })
        .unwrap();
    settle(&mut core, &mut store, effects);
    let replay = core.replay_committed();
    let receipts = app
        .apply_batch(&replay[replay.partition_point(|entry| entry.index <= applied)..])
        .unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].operation.get(), 1);
    assert_eq!(receipts[0].duplicate, retained);
    assert_eq!(receipts[0].outcome, CounterOutcome::Value(1));
    assert_eq!(
        app.read_applied(app.applied_index()),
        Ok(if retained { ORIGINALS as i64 } else { 1 })
    );
}
#[test]
fn aggregate16384_model_power_loss_keeps_exact_frontiers_and_original_retry_once() {
    for commit_phase in [false, true] {
        for fault in [
            Fault::Append(10),
            Fault::Sync,
            Fault::PublishBefore,
            Fault::PublishAfter,
        ] {
            publication_cut(commit_phase, fault);
        }
    }
}
fn publication_cut(commit_phase: bool, fault: Fault) {
    let io = ModelIo::default();
    let (mut store, mut core) = setup_aggregate(io.clone());
    let proposals = (1..=ORIGINALS)
        .map(|id| Proposal {
            operation: voteboat::identity::OperationId::new(id).unwrap(),
            bytes: 1i64.to_le_bytes().to_vec(),
        })
        .collect();
    let before_proposal = core.state().clone();
    let mut update = exact_persist(core.propose_batch(proposals).unwrap());
    assert_eq!(core.state(), &before_proposal);
    if commit_phase {
        update = exact_persist(persist_effect(&mut core, &mut store, update).unwrap());
    }
    let before = store.state(group(1)).unwrap();
    let old_binding = store.binding();
    let mut oracle = std::collections::BTreeMap::from([(group(1), before.clone())]);
    apply_batch(
        &mut oracle,
        &[LogMutation::Update(update.clone())],
        store.limits(),
    )
    .unwrap();
    io.0.borrow_mut().fault = fault;
    assert!(matches!(
        persist_effect(&mut core, &mut store, update),
        Err(voteboat::raft::RaftError::Storage(StorageError::Uncertain(
            _
        )))
    ));
    assert!(core.is_fenced());
    assert_eq!(store.state(group(1)), Err(StorageError::Fenced));
    drop(store);
    io.0.borrow_mut().power_loss();
    let store = NativeLogStore::recover(io, identity(1), limits()).unwrap();
    let published = matches!(fault, Fault::PublishBefore | Fault::PublishAfter);
    assert_eq!(
        store.state(group(1)).unwrap(),
        if published {
            oracle.remove(&group(1)).unwrap()
        } else {
            before
        }
    );
    assert_ne!(store.binding().session, old_binding.session);
    let recovered_core = Raft::recover(
        node(1),
        store.binding(),
        store.state(group(1)).unwrap(),
        store.limits(),
    )
    .unwrap();
    let app = replay_counter(&recovered_core);
    assert_eq!(
        app.read_applied(app.applied_index()),
        Ok(if commit_phase && published {
            ORIGINALS as i64
        } else {
            0
        })
    );
    retry_after_recovery(store, commit_phase || published);
}
