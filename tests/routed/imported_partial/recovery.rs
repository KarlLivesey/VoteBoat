// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::collections::VecDeque;
use voteboat::raft::*;

// Abort stops application polling, but accepted worker updates can still become
// durable. Compare with that exact committed tail, never a masked checkpoint.
pub(super) fn expected<A: CheckpointStateMachine>(before: &A, log: &GroupLog) -> A {
    let applied = before.applied_index();
    assert!(log.base_index() <= applied && applied <= log.commit_index);
    let tail: Vec<_> = log
        .entries
        .iter()
        .filter(|e| e.index > applied && e.index <= log.commit_index)
        .cloned()
        .collect();
    let mut next = before.clone();
    next.apply_batch(&tail).unwrap();
    assert_eq!(next.applied_index(), log.commit_index);
    next
}

struct RunCore {
    core: Raft,
    store: NativeLogStore<support::ModelIo>,
    io: support::ModelIo,
    app: Counter,
}
impl RunCore {
    fn new() -> Self {
        let io = support::ModelIo::default();
        let mut store =
            NativeLogStore::create(io.clone(), support::identity(1), LogLimits::default()).unwrap();
        support::append(
            &mut store,
            vec![LogMutation::Create(support::bootstrap(1, 1))],
        );
        let core = Raft::recover(
            support::node(1),
            store.binding(),
            store.state(group(1)).unwrap(),
            store.limits(),
        )
        .unwrap();
        let mut run = Self {
            core,
            store,
            io,
            app: Counter::new(8).unwrap(),
        };
        run.event(Event::Campaign);
        run
    }
    fn event(&mut self, event: Event) -> Vec<CounterReceipt> {
        let mut effects = VecDeque::from(self.core.step(event).unwrap());
        let mut receipts = Vec::new();
        while let Some(effect) = effects.pop_front() {
            match effect {
                Effect::Persist(update) => {
                    effects.extend(persist_effect(&mut self.core, &mut self.store, update).unwrap())
                }
                Effect::Committed(entries) => {
                    receipts.extend(self.app.apply_batch(&entries).unwrap())
                }
                _ => panic!("unexpected effect: {effect:?}"),
            }
        }
        receipts
    }
    fn write(&mut self, id: u128, delta: i64) -> CounterReceipt {
        self.event(Event::Propose {
            operation: OperationId::new(id).unwrap(),
            bytes: delta.to_le_bytes().to_vec(),
        })
        .pop()
        .unwrap()
    }
    fn reopened(self) -> Self {
        let Self {
            store,
            core,
            io,
            app: _,
        } = self;
        drop(core);
        drop(store);
        let store = NativeLogStore::recover(io.clone(), support::identity(1), LogLimits::default())
            .unwrap();
        let core = Raft::recover(
            support::node(1),
            store.binding(),
            store.state(group(1)).unwrap(),
            store.limits(),
        )
        .unwrap();
        let mut app = Counter::new(8).unwrap();
        app.apply_batch(core.replay_committed()).unwrap();
        Self {
            core,
            store,
            io,
            app,
        }
    }
}
#[test]
fn restart_expectation_includes_actual_campaign_noop_after_capture() {
    let mut run = RunCore::new();
    assert_eq!(run.write(9911, 7).outcome, CounterOutcome::Value(7));
    let captured = run.app.clone();
    run.event(Event::Campaign);
    assert!(run.app.applied_index() > captured.applied_index());
    assert_eq!(run.app.read_applied(run.app.applied_index()), Ok(7));
    let run = run.reopened();
    let expected = expected(&captured, run.core.state());
    assert_ne!(captured.checkpoint(4096), run.app.checkpoint(4096));
    assert_eq!(expected.checkpoint(4096), run.app.checkpoint(4096));
}
#[test]
fn restart_expectation_preserves_actual_committed_tail_and_original_retry() {
    let mut run = RunCore::new();
    assert_eq!(run.write(9911, 7).outcome, CounterOutcome::Value(7));
    let captured = run.app.clone();
    assert_eq!(run.write(9912, 3).outcome, CounterOutcome::Value(10));
    let mut run = run.reopened();
    let expected = expected(&captured, run.core.state());
    assert_eq!(expected.checkpoint(4096), run.app.checkpoint(4096));
    run.event(Event::Campaign);
    let receipt = run.write(9911, 7);
    assert!(receipt.duplicate);
    assert_eq!(receipt.outcome, CounterOutcome::Value(7));
    assert_eq!(run.app.read_applied(run.app.applied_index()), Ok(10));
    assert_eq!(run.app.remaining_operations(), 6);
}
