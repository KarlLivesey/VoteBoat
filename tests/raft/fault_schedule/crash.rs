// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::application::{Counter, CounterOutcome, CounterReceipt, StateMachine};

#[derive(Clone, Copy)]
pub(super) enum Mode {
    Process,
    PowerLoss,
}
fn cut(id: u64, power_loss: bool) -> Action {
    if power_loss {
        Action::PowerLoss(id)
    } else {
        Action::ProcessCrash(id)
    }
}
// Fixed three-node history: term, leader entry and commit can each own one
// local dependency. This drains actual returned completions, never force-releases.
fn finish_local(run: &mut Replay, id: u64) {
    for _ in 0..4 {
        run.action(&Action::Sync(id)).unwrap();
        run.action(&Action::Complete(id)).unwrap();
        run.action(&Action::Pump).unwrap();
        if !run.point().held.contains_key(&id) {
            return;
        }
    }
    panic!("fixed local history did not drain its actual dependencies");
}
fn counter(run: &Replay, id: u64) -> (Counter, Vec<CounterReceipt>) {
    let mut counter = Counter::new(4).unwrap();
    let receipts = counter.apply_batch(&run.cluster.applied[&id]).unwrap();
    (counter, receipts)
}
fn assert_original_counter(run: &Replay, id: u64) {
    let (application, receipts) = counter(run, id);
    assert_eq!(application.read_applied(application.applied_index()), Ok(9));
    assert_eq!(receipts.iter().filter(|r| !r.duplicate).count(), 1);
    assert!(receipts.iter().all(|r| {
        r.operation == OperationId::new(9911).unwrap() && r.outcome == CounterOutcome::Value(9)
    }));
}

#[test]
fn process_crash_and_power_loss_distinguish_complete_unsynced_vote_frames() {
    for power_loss in [false, true] {
        for synced in [false, true] {
            let mut run = Replay::new();
            run.action(&Action::Delay(1)).unwrap();
            run.action(&Action::Campaign(1)).unwrap();
            if synced {
                run.action(&Action::Sync(1)).unwrap();
            }
            let device = run.images[&1].device.0.borrow();
            assert_eq!(device.log == device.synced, synced);
            drop(device);
            let old = run.cluster.replicas[&1].store.binding();
            assert_eq!(run.point().queued, 0);
            assert_eq!(run.point().logs[&1].hard_state.term, 0);
            run.action(&cut(1, power_loss)).unwrap();
            let retained = synced || !power_loss;
            assert_eq!(run.point().logs[&1].hard_state.term, u64::from(retained));
            assert_eq!(
                run.point().logs[&1].hard_state.voted_for,
                retained.then_some(node(1))
            );
            let fresh = run.cluster.replicas[&1].store.binding();
            assert_eq!(fresh.identity, old.identity);
            assert_eq!(fresh.session.get(), old.session.get() + 1);
            assert!(run.point().held.is_empty());
            assert_eq!(run.point().queued, 0);
        }
    }
}

#[test]
fn failed_sync_fences_actual_store_and_core_without_escaping_vote_effects() {
    let mut run = Replay::new();
    run.action(&Action::Delay(1)).unwrap();
    run.action(&Action::Campaign(1)).unwrap();
    run.action(&Action::FailSync(1)).unwrap();
    assert_eq!(run.point().sync_failures, 1);
    assert_eq!(run.point().fenced, BTreeSet::from([1]));
    assert!(run.point().held.is_empty());
    assert!(run.point().pending.is_empty());
    assert_eq!(run.point().queued, 0);
    assert_eq!(run.point().logs[&1].hard_state.term, 0);
    run.action(&Action::Complete(1)).unwrap();
    run.action(&Action::Heartbeat(1)).unwrap();
    assert_eq!(run.point().queued, 0);
    assert_eq!(run.point().fenced_refused, 1);
    run.action(&Action::ProcessCrash(1)).unwrap();
    assert!(run.point().fenced.is_empty());
    assert_eq!(run.point().logs[&1].hard_state.term, 1);
}

#[test]
fn uncertain_counter_write_recovers_and_original_id_applies_once() {
    for power_loss in [false, true] {
        let mut run = Replay::new();
        run.action(&Action::Campaign(1)).unwrap();
        run.action(&Action::Pump).unwrap();
        run.action(&Action::Delay(1)).unwrap();
        run.action(&Action::TryCounter(1, 9911, 9)).unwrap();
        run.action(&Action::FailSync(1)).unwrap();
        assert_eq!(counter(&run, 1).0.read_applied(0), Ok(0));
        assert_eq!(run.point().queued, 0);
        run.action(&cut(1, power_loss)).unwrap();
        run.action(&Action::Campaign(1)).unwrap();
        finish_local(&mut run, 1);
        assert_eq!(run.point().roles[&1], Role::Leader);
        run.action(&Action::TryCounter(1, 9911, 9)).unwrap();
        finish_local(&mut run, 1);
        assert_original_counter(&run, 1);
        assert_eq!(counter(&run, 1).1.len(), if power_loss { 1 } else { 2 });
        for id in 1..=3 {
            run.action(&Action::PowerLoss(id)).unwrap();
            assert_original_counter(&run, id);
        }
        run.action(&Action::Campaign(1)).unwrap();
        finish_local(&mut run, 1);
        run.action(&Action::TryCounter(1, 9911, 9)).unwrap();
        finish_local(&mut run, 1);
        for id in 1..=3 {
            assert_original_counter(&run, id);
            assert!(counter(&run, id).1.last().unwrap().duplicate);
        }
    }
}

#[test]
fn composed_process_crashes_and_failed_syncs_replay_exactly() {
    let mut actions = vec![
        Action::Delay(1),
        Action::Campaign(1),
        Action::FailSync(1),
        Action::Heartbeat(1),
        Action::ProcessCrash(1),
    ];
    for round in 0..2 {
        actions.push(Action::Campaign(1));
        for _ in 0..3 {
            actions.extend([Action::Sync(1), Action::Complete(1), Action::Pump]);
        }
        actions.push(Action::TryCounter(1, 9911, 9));
        if round == 0 {
            actions.extend([Action::FailSync(1), Action::ProcessCrash(1)]);
        } else {
            for _ in 0..2 {
                actions.extend([Action::Sync(1), Action::Complete(1), Action::Pump]);
            }
        }
    }
    actions.extend([Action::PowerLoss(1), Action::Stale(1)]);
    let trace = Trace { seed: 265, actions };
    let saved = trace.encode();
    let decoded = Trace::decode(&saved).unwrap();
    let points = replay(&trace).unwrap();
    assert_eq!(points, replay(&decoded).unwrap(), "{saved}");
    assert_eq!(points.last().unwrap().sync_failures, 2);
    assert!(points.iter().any(|p| !p.fenced.is_empty()));
    assert_eq!(points.last().unwrap().stale_rejected, 1);
    eprintln!("composed crash/failure trace:\n{saved}");
}
