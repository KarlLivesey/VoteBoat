// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::application::{Counter, CounterOutcome, StateMachine};

#[test]
fn deferred_heartbeat_batch_preserves_duplicate_conflict_and_original_application_outcomes() {
    let mut shard = shard(ShardLimits::default(), LogLimits::default());
    shard.admit(group(), Event::Heartbeat).unwrap();
    let admissions: Vec<_> = [(1, 7_i64), (2, 2), (1, 9), (1, 7)]
        .into_iter()
        .map(|(id, delta)| {
            shard
                .admit_tracked(
                    group(),
                    Event::Propose {
                        operation: OperationId::new(id).unwrap(),
                        bytes: delta.to_le_bytes().to_vec(),
                    },
                )
                .unwrap()
        })
        .collect();
    let visit = data_turn(&mut shard);
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 4, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(step.admission, Some(admissions[0]));
    assert_eq!(step.proposed.unwrap().index, 2);
    assert_eq!(step.batch_members.len(), 3);
    for (offset, member) in step.batch_members.iter().enumerate() {
        assert_eq!(member.admission, Some(admissions[offset + 1]));
        assert_eq!(member.proposed.unwrap().index, offset as u64 + 3);
    }
    let mut counter = Counter::new(4).unwrap();
    counter
        .apply_batch(&shard.core(group()).unwrap().replay_committed().contiguous())
        .unwrap();
    assert_eq!(counter.read_applied(1), Ok(0));
    assert_eq!(shard.core(group()).unwrap().state().commit_index, 1);
    shard
        .with_core(visit, |core| finish(core, step.result.unwrap()))
        .unwrap();
    let receipts = counter
        .apply_batch(&shard.core(group()).unwrap().replay_committed().contiguous()[1..])
        .unwrap();
    assert_eq!(receipts.len(), 4);
    for (offset, outcome) in [
        CounterOutcome::Value(7),
        CounterOutcome::Value(9),
        CounterOutcome::OperationConflict,
        CounterOutcome::Value(7),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(receipts[offset].index, offset as u64 + 2);
        assert_eq!(receipts[offset].outcome, outcome);
        assert_eq!(receipts[offset].duplicate, offset >= 2);
        assert_eq!(receipts[offset].operation.get(), [1, 2, 1, 1][offset]);
    }
    assert_eq!(counter.read_applied(5), Ok(9));
    assert_eq!(shard.groups[&group()].queues[0].len(), 1);
    shard.finish(visit).unwrap();
    assert_eq!(shard.usage().items, 1);
}
