// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn routine_control_does_not_expand_per_core_visit_or_output_count_bounds() {
    for (core_limit, visit_limit, output_limit, expected) in [
        (1, 128, 128, 1),
        (3, 128, 128, 3),
        (80, 128, 128, 70),
        (80, 2, 128, 2),
        (80, 128, 2, 2),
    ] {
        let limits = ShardLimits {
            visit_items: visit_limit,
            ..ShardLimits::default()
        };
        let mut shard = shard_with_batch_limits(
            limits,
            LogLimits::default(),
            BatchLimits {
                max_proposal_entries: core_limit,
                ..BatchLimits::default()
            },
        );
        shard.admit(group(), Event::Heartbeat).unwrap();
        for id in 1..=70 {
            shard.admit_tracked(group(), command(id)).unwrap();
        }
        let visit = data_turn(&mut shard);
        let reserve = shard
            .next_ready_effect_reservation(visit, MonoTime(0), output_limit)
            .unwrap()
            .unwrap();
        assert_eq!(shard.groups[&group()].queues[1].len(), 70);
        let step = shard
            .step_ready_checked(visit, MonoTime(0), output_limit, |_, _, _| Ok(()))
            .unwrap()
            .unwrap();
        assert_eq!(step.batch_members.len() + 1, expected);
        assert!(reserve >= shard.core(group()).unwrap().effect_reservation(0).unwrap());
        assert_eq!(shard.groups[&group()].queues[1].len(), 70 - expected);
        assert_eq!(shard.groups[&group()].queues[0].len(), 1);
    }
}

#[test]
fn byte_budget_and_expired_visit_leave_routine_control_and_originals_bounded() {
    let cost = size_of::<Event>() + size_of::<AdmissionTicket>() + 128;
    let limits = ShardLimits {
        max_event_bytes: cost,
        visit_bytes: 2 * cost,
        ..ShardLimits::default()
    };
    let mut shard = shard(limits, LogLimits::default());
    shard.admit(group(), Event::Heartbeat).unwrap();
    for id in 1..=4 {
        shard
            .admit_tracked(
                group(),
                Event::Propose {
                    operation: OperationId::new(id).unwrap(),
                    bytes: vec![0; 128],
                },
            )
            .unwrap();
    }
    let visit = data_turn(&mut shard);
    let deadline = shard.groups[&group()].visit.as_ref().unwrap().deadline;
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 4, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(step.batch_members.len(), 1);
    assert_eq!(
        shard.groups[&group()].visit.as_ref().unwrap().used[1].bytes,
        2 * cost
    );
    shard
        .with_core(visit, |core| finish(core, step.result.unwrap()))
        .unwrap();
    assert!(shard
        .step_ready_checked(visit, deadline, 4, |_, _, _| Ok(()))
        .unwrap()
        .is_none());
    assert_eq!(shard.groups[&group()].queues[0].len(), 1);
    assert_eq!(shard.groups[&group()].queues[1].len(), 2);
}
