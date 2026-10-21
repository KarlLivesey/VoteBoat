// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::application::CounterOutcome;

fn op(id: u64, operation: Operation) -> Action {
    Action::Snapshot(id, operation)
}
fn initial() -> Vec<Action> {
    vec![
        Action::Campaign(1),
        Action::Pump,
        Action::TryCounter(1, 1, 9),
        Action::Pump,
    ]
}
fn phases(id: u64) -> [Action; 5] {
    [
        op(id, Operation::Begin),
        op(id, Operation::Write),
        op(id, Operation::Seal),
        op(id, Operation::Publish),
        op(id, Operation::Compact),
    ]
}
fn execute(trace: &Trace) -> Replay {
    let saved = trace.encode();
    let decoded = Trace::decode(&saved).unwrap();
    assert_eq!(decoded, *trace);
    assert_eq!(replay(trace).unwrap(), replay(&decoded).unwrap(), "{saved}");
    let mut run = Replay::new();
    for action in &trace.actions {
        run.action(action).unwrap();
    }
    run
}
fn assert_counter(run: &mut Replay, id: u64, value: i64) {
    let application = run.snapshot_counter(id).unwrap();
    assert_eq!(
        application.read_applied(application.applied_index()),
        Ok(value)
    );
    recovery::assert_application(&application, &run.cluster.applied[&id]).unwrap();
}
#[test]
fn every_snapshot_phase_survives_process_and_power_loss_with_original_retry() {
    for power in [false, true] {
        for prefix in 1..=5 {
            let mut actions = initial();
            actions.extend(phases(1)[..prefix].iter().cloned());
            actions.extend([
                crash::cut(1, power),
                op(1, Operation::Stale),
                Action::Campaign(1),
                Action::Pump,
                Action::TryCounter(1, 1, 9),
                Action::Pump,
                Action::Read(1, 100),
                Action::Pump,
                Action::ServeCounter(1, 100),
            ]);
            let mut run = execute(&Trace { seed: 294, actions });
            assert_counter(&mut run, 1, 9);
            assert_eq!(run.snapshots.counts.recoveries, 1);
            assert_eq!(run.snapshots.counts.stale, 1);
            assert!(run.snapshots.stages.is_empty());
            assert_eq!(run.point().logs[&1].base_index() > 0, prefix == 5);
            assert!(run
                .read_outcomes
                .iter()
                .any(|a| a.outcome == reads::Outcome::Value(9)));
            let replica = &run.cluster.replicas[&1];
            let mut fresh_application = fresh();
            let restored = restore_application(
                &replica.core,
                &mut fresh_application,
                run.snapshots.providers.get_mut(&1).unwrap(),
            )
            .unwrap();
            assert!(restored.replay_receipts.last().unwrap().duplicate);
            assert_eq!(
                restored.replay_receipts.last().unwrap().outcome,
                CounterOutcome::Value(9)
            );
        }
    }
}
#[test]
fn failed_seal_and_lost_root_completions_recover_actual_published_state() {
    for power in [false, true] {
        for operation in [
            Operation::FailSync,
            Operation::PublishBefore,
            Operation::PublishAfter,
        ] {
            let mut actions = initial();
            actions.extend(phases(1)[..2].iter().cloned());
            if operation != Operation::FailSync {
                actions.push(op(1, Operation::Seal));
            }
            actions.push(op(1, operation));
            actions.extend([
                crash::cut(1, power),
                op(1, Operation::Stale),
                op(1, Operation::Compact),
            ]);
            let mut run = execute(&Trace { seed: 295, actions });
            assert_counter(&mut run, 1, 9);
            assert_eq!(run.snapshots.counts.failures, 1);
            assert_eq!(run.snapshots.counts.stale, 1);
            assert_eq!(run.point().logs[&1].base_index(), 0);
            assert_eq!(
                run.snapshots.recovered_roots[&1].is_some(),
                operation == Operation::PublishAfter
            );
            assert_eq!(run.snapshots.providers[&1].latest_reference(), Ok(None));
        }
    }
}
#[test]
fn a_replaced_snapshot_retains_the_old_log_anchor_through_every_new_phase_cut() {
    for power in [false, true] {
        for prefix in 1..=5 {
            let mut actions = initial();
            actions.extend(phases(1));
            actions.extend([Action::TryCounter(1, 2, 4), Action::Pump]);
            let replacement = [
                op(1, Operation::Begin),
                op(1, Operation::Write),
                op(1, Operation::Write),
                op(1, Operation::Seal),
                op(1, Operation::Publish),
                op(1, Operation::Compact),
            ];
            actions.extend(replacement[..prefix].iter().cloned());
            actions.push(crash::cut(1, power));
            let mut run = execute(&Trace { seed: 296, actions });
            assert_counter(&mut run, 1, 13);
            assert_eq!(run.point().logs[&1].base_index(), 2);
            assert_eq!(
                run.snapshots.providers[&1]
                    .latest_reference()
                    .unwrap()
                    .unwrap()
                    .index,
                2
            );
        }
    }
}
#[test]
fn lagging_actual_follower_installs_pinned_snapshot_and_recovers_original_operations() {
    let mut actions = initial();
    actions.extend([
        Action::Isolate(3),
        Action::TryCounter(1, 2, 4),
        Action::Pump,
        op(1, Operation::Begin),
        op(1, Operation::Write),
        op(1, Operation::Write),
        op(1, Operation::Seal),
        op(1, Operation::Publish),
        op(1, Operation::Compact),
        Action::Heal,
        Action::Heartbeat(1),
        Action::DuplicateOldest,
        Action::DeliverNewest,
        Action::Pump,
        Action::PowerLoss(3),
        Action::Campaign(1),
        Action::Pump,
        Action::TryCounter(1, 1, 9),
        Action::Pump,
    ]);
    let mut run = execute(&Trace { seed: 297, actions });
    assert!(run.snapshots.counts.installs > 0);
    for id in 1..=3 {
        assert_counter(&mut run, id, 13);
    }
    assert!(run.point().logs[&3].base_index() > 0);
}
#[test]
fn false_snapshot_seal_sync_is_replayed_reduced_and_rejected_before_voting() {
    let mut actions = initial();
    actions.extend([
        op(1, Operation::Begin),
        op(1, Operation::Write),
        op(1, Operation::BadSync),
        op(1, Operation::Publish),
        op(1, Operation::Compact),
        Action::PowerLoss(1),
    ]);
    let trace = Trace { seed: 298, actions };
    let target = LostPrefix {
        node: 1,
        operation: Some(Counter::operation(1, 1).unwrap()),
        stage: FailureStage::SnapshotRecoveryLost,
    };
    assert_eq!(replay(&trace), Err(target.clone()));
    let reduced = reduce::minimize(&trace, &target, 512);
    assert!(reduced.one_removal_minimal);
    assert!(reduced.attempts <= 512);
    let saved = reduced.trace.encode();
    assert_eq!(replay(&Trace::decode(&saved).unwrap()), Err(target.clone()));
    for i in 0..reduced.trace.actions.len() {
        let mut smaller = reduced.trace.clone();
        smaller.actions.remove(i);
        assert_ne!(replay(&smaller).err(), Some(target.clone()));
    }
    let mut correct = trace;
    for action in &mut correct.actions {
        if *action == op(1, Operation::BadSync) {
            *action = op(1, Operation::Seal);
        }
    }
    let mut run = execute(&correct);
    assert_counter(&mut run, 1, 9);
    eprintln!("minimized false snapshot-seal-sync trace:\n{saved}");
}
#[test]
fn seeded_snapshot_network_and_restart_histories_replay_exactly_without_vacuous_phases() {
    for seed in 1..=16u64 {
        let mut actions = initial();
        let mut random = seed;
        for round in 0..6u64 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let id = random % 3 + 1;
            actions.extend([
                Action::Campaign(1),
                Action::DuplicateOldest,
                Action::DeliverNewest,
                Action::Pump,
                Action::TryCounter(1, 3 + u128::from(round), 1),
                Action::Pump,
                op(id, Operation::Begin),
            ]);
            actions.extend((0..4).map(|_| op(id, Operation::Write)));
            actions.extend([
                op(id, Operation::Seal),
                op(id, Operation::Publish),
                op(id, Operation::Compact),
                crash::cut(id, random & 1 == 0),
                op(id, Operation::Stale),
            ]);
        }
        let mut run = execute(&Trace { seed, actions });
        assert_eq!(run.snapshots.counts.compactions, 6);
        assert_eq!(run.snapshots.counts.recoveries, 6);
        assert_eq!(run.snapshots.counts.stale, 6);
        assert_eq!(run.snapshots.counts.failures, 0);
        for id in 1..=3 {
            assert_counter(&mut run, id, 15);
        }
    }
}
#[test]
fn snapshot_trace_version_and_malformed_phases_are_rejected_before_execution() {
    let trace = Trace {
        seed: 299,
        actions: Operation::ALL
            .into_iter()
            .map(|operation| op(1, operation))
            .collect(),
    };
    let saved = trace.encode();
    assert!(saved.starts_with("VBSIM 2 "));
    assert_eq!(Trace::decode(&saved).unwrap(), trace);
    for text in [
        "VBSIM 1 1\nsnapshot_begin 1",
        "VBSIM 2 1\nsnapshot_begin 0",
        "VBSIM 2 1\nsnapshot_publish 4",
        "VBSIM 2 1\nsnapshot_seal 1 extra",
        "VBSIM 2 1\nsnapshot_unknown 1",
        "VBSIM 3 1\nsnapshot_begin 1",
    ] {
        assert!(Trace::decode(text).is_err(), "{text}");
    }
}

#[test]
fn aborted_replacement_stage_preserves_actual_published_log_anchor() {
    let mut actions = initial();
    actions.extend(phases(1));
    actions.extend([
        Action::TryCounter(1, 2, 4),
        Action::Pump,
        op(1, Operation::Begin),
        op(1, Operation::Write),
        op(1, Operation::Abort),
        Action::PowerLoss(1),
        op(1, Operation::Stale),
    ]);
    let mut run = execute(&Trace { seed: 300, actions });
    assert_counter(&mut run, 1, 13);
    assert!(run.snapshots.stages.is_empty());
    assert_eq!(
        run.snapshots.providers[&1]
            .latest_reference()
            .unwrap()
            .unwrap()
            .index,
        2
    );
    assert_eq!(run.snapshots.counts.begins, 2);
    assert_eq!(run.snapshots.counts.publishes, 1);
    assert_eq!(run.snapshots.counts.stale, 1);
}
#[test]
fn compacted_snapshot_composes_with_delayed_wal_completion_and_uncertain_original_retry() {
    for power in [false, true] {
        for synced in [false, true] {
            let mut actions = initial();
            actions.extend(phases(1));
            actions.extend([
                Action::Delay(1),
                Action::TryCounter(1, 2, 4),
                op(1, Operation::Begin),
            ]);
            if synced {
                actions.push(Action::Sync(1));
            }
            actions.extend([
                crash::cut(1, power),
                op(1, Operation::Stale),
                Action::Campaign(1),
            ]);
            for _ in 0..4 {
                actions.extend([Action::Sync(1), Action::Complete(1), Action::Pump]);
            }
            actions.push(Action::TryCounter(1, 2, 4));
            for _ in 0..4 {
                actions.extend([Action::Sync(1), Action::Complete(1), Action::Pump]);
            }
            actions.extend([Action::PowerLoss(1), op(1, Operation::Stale)]);
            let mut run = execute(&Trace { seed: 301, actions });
            assert_counter(&mut run, 1, 13);
            assert!(run.point().held.is_empty());
            assert!(run.point().pending.is_empty());
            assert_eq!(run.snapshots.counts.compactions, 1);
            assert_eq!(run.snapshots.counts.recoveries, 2);
            assert_eq!(run.snapshots.counts.refused, 1);
        }
    }
}
