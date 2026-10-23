// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Reachability in the actual consumer across unchanged default maintenance.
use super::*;

#[test]
fn all_three_selected_consumers_survive_two_default_checkpoints_single_and_wide() {
    for width in [1, 515] {
        let Replicas {
            mut actors,
            clients: _clients,
            stop: _stop,
        } = replicas_with_clients(4096);
        actors[0].step(Event::Campaign).unwrap();
        settle(&mut actors);
        let mut stages = [[false; 3]; 3];
        let mut next = 1;
        while next <= 12_000 {
            let before: Vec<_> = actors
                .iter()
                .enumerate()
                .map(|(slot, actor)| {
                    (
                        actor.parts.stats[slot].local_units.load(Ordering::Relaxed),
                        actor.parts.stats[slot]
                            .checkpoint_count
                            .load(Ordering::Relaxed),
                    )
                })
                .collect();
            let count = width.min(12_001 - next);
            let mut originals = Vec::new();
            let mut waits = Vec::new();
            for id in next..next + count {
                let operation = OperationId::new(id as u128).unwrap();
                let (reply, wait) = oneshot::channel();
                originals.push(Original {
                    operation,
                    bytes: Vec::new(),
                    reply,
                });
                waits.push((operation, wait));
            }
            actors[0].requests(Request { originals }).unwrap();
            settle(&mut actors);
            for (slot, actor) in actors.iter().enumerate() {
                assert_eq!(actor.parts.stats[slot].local_units.load(Ordering::Relaxed)-before[slot].0, 2,
                    "all three actual consumers select append and commit, width={width} slot={slot} next={next}");
                stages[slot][before[slot].1.min(2)] = true;
                assert_eq!(actor.parts.application.applied, next as u64 + count as u64);
                assert_eq!(
                    actor.parts.core.state().commit_index,
                    actor.parts.application.applied
                );
                assert!(!actor.parts.core.is_fenced());
            }
            for (expected, mut wait) in waits {
                assert_eq!(wait.try_recv().unwrap(), expected);
            }
            for (slot, actor) in actors.iter_mut().enumerate() {
                let previous = actor.parts.core.state().base_index();
                actor.maintain().unwrap();
                if actor.parts.core.state().base_index() != previous {
                    assert!(actor.parts.core.state().base_index() > previous);
                    assert_eq!(
                        actor.parts.core.state().base_index(),
                        actor.parts.application.applied - 1000
                    );
                    assert_eq!(actor.parts.core.state().entries.len(), 1000);
                    assert_eq!(actor.parts.snapshots.pins.len(), 1);
                    assert!(actor.parts.snapshots.current.is_some());
                    assert_eq!(
                        actor.parts.stats[slot]
                            .checkpoint_count
                            .load(Ordering::Relaxed),
                        before[slot].1 + 1
                    );
                    assert!(!actor.parts.core.has_pending_dependency());
                }
            }
            settle(&mut actors);
            next += count;
        }
        assert_eq!(
            stages, [[true; 3]; 3],
            "selection before and after both checkpoints"
        );
        for (slot, actor) in actors.iter().enumerate() {
            assert_eq!(
                actor.parts.stats[slot]
                    .checkpoint_count
                    .load(Ordering::Relaxed),
                2
            );
            assert!(actor.parts.core.state().base_index() >= 9000);
            assert!(actor.pending.is_empty());
        }
    }
}
