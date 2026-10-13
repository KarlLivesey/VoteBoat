// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[test]
fn every_native_quantum_keeps_control_data_background_and_group_fairness() {
    for items in [1, 16, 32, 64] {
        let mut store = HostLogStore::new(1);
        append(
            &mut store,
            (1..=2)
                .map(|id| LogMutation::Create(bootstrap(id, 3)))
                .collect(),
        );
        let mut shard = Shard::new(
            owner(store.binding()),
            ShardLimits {
                max_groups: 2,
                visit_items: items,
                ..ShardLimits::default()
            },
            HostReady::new(2),
        )
        .unwrap();
        for id in 1..=2 {
            shard
                .register(
                    Raft::recover(
                        node(1),
                        store.binding(),
                        store.state(group(id)).unwrap(),
                        store.limits(),
                    )
                    .unwrap(),
                )
                .unwrap();
            for operation in 1..=4 {
                shard
                    .admit(group(id), proposal(id * 100 + operation))
                    .unwrap();
            }
            shard.admit(group(id), Event::Checkpoint).unwrap();
        }
        let mut seen = [0usize; 2];
        let mut background = [false; 2];
        for _ in 0..128 {
            for id in 1..=2 {
                let _ = shard.admit(group(id), Event::Heartbeat);
            }
            let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
            let id = usize::try_from(visit.group.id.get() - 1).unwrap();
            if let Some(step) = shard.step_next(visit, MonoTime(0)).unwrap() {
                if step.operation.is_some() {
                    seen[id] += 1;
                } else if step.result == Err(RaftError::NotApplied) {
                    background[id] = true;
                }
            }
            shard.finish(visit).unwrap();
            if seen == [4, 4] && background == [true, true] {
                break;
            }
        }
        assert_eq!(seen, [4, 4]);
        assert_eq!(background, [true, true]);
    }
}
