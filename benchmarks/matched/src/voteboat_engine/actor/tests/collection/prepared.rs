// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Normal Actor collection/maintenance/application, with bounded owned queues.
use super::*;
type OriginalWait = (OperationId, oneshot::Receiver<OperationId>);

fn settle_active(actors: &mut [Actor], lagging: bool) {
    for _ in 0..10_000 {
        let mut delivered = false;
        for (slot, actor) in actors.iter_mut().enumerate() {
            while let Ok(message) = actor.parts.peer_receiver.try_recv() {
                if slot != 2 || !lagging {
                    actor.step(Event::Receive(message)).unwrap();
                }
                delivered = true;
            }
        }
        if !delivered {
            return;
        }
    }
    panic!("bounded normal peer queues failed to settle");
}

fn originals(first: usize, width: usize) -> (Vec<Request>, Vec<OriginalWait>) {
    let mut requests = Vec::new();
    let mut waits = Vec::new();
    for chunk in (first..first + width).collect::<Vec<_>>().chunks(4) {
        let mut originals = Vec::new();
        for &ordinal in chunk {
            let operation = OperationId::new((1u128 << 127) + ordinal as u128 * 17).unwrap();
            let (reply, wait) = oneshot::channel();
            originals.push(Original {
                operation,
                bytes: Vec::new(),
                reply,
            });
            waits.push((operation, wait));
        }
        requests.push(Request { originals });
    }
    (requests, waits)
}

fn write(replicas: &mut Replicas, first: usize, width: usize, lagging: bool) {
    let (requests, waits) = originals(first, width);
    let mut requests = requests.into_iter();
    let initial = requests.next().unwrap();
    for request in requests {
        assert!(replicas.clients[0].try_send(request).is_ok());
    }
    replicas.actors[0].requests(initial).unwrap();
    settle_active(&mut replicas.actors, lagging);
    replicas.actors[0].step(Event::Heartbeat).unwrap();
    settle_active(&mut replicas.actors, lagging);
    for (expected, mut wait) in waits {
        assert_eq!(wait.try_recv().unwrap(), expected);
    }
    assert!(replicas.actors[0].pending.is_empty());
    for (slot, actor) in replicas.actors.iter_mut().enumerate() {
        if slot != 2 || !lagging {
            assert_eq!(actor.parts.application.applied, (first + width) as u64);
            assert_eq!(
                actor.parts.core.state().commit_index,
                (first + width) as u64
            );
            actor.maintain().unwrap();
        }
    }
    settle_active(&mut replicas.actors, lagging);
}

#[test]
fn prepared_all_three_normal_actors_b1_b231_b515_default_checkpoints_and_snapshot() {
    for width in [1usize, 231, 515] {
        let mut replicas = replicas_clients(4096);
        replicas.actors[0].step(Event::Campaign).unwrap();
        settle_active(&mut replicas.actors, false);
        assert_eq!(replicas.actors[0].parts.core.role(), Role::Leader);
        let rounds = 10_500usize.div_ceil(width);
        let total = rounds * width;
        for round in 0..rounds {
            write(&mut replicas, round * width + 1, width, false);
        }
        for (slot, actor) in replicas.actors.iter().enumerate() {
            assert!(actor.last_snapshot_at >= 10_000);
            assert!(actor.parts.core.state().base_index() >= 9_000);
            assert!(
                actor.parts.stats[slot]
                    .checkpoint_count
                    .load(Ordering::Relaxed)
                    >= 2
            );
        }
        for round in 0..rounds {
            write(&mut replicas, total + round * width + 1, width, true);
        }
        replicas.actors[0].step(Event::Heartbeat).unwrap();
        settle_active(&mut replicas.actors, false);
        assert!(
            replicas.actors[2].parts.stats[2]
                .snapshot_installs
                .load(Ordering::Relaxed)
                > 0
        );
        for round in 0..2 {
            write(&mut replicas, 2 * total + round * width + 1, width, false);
        }
        let diagnostics = serde_json::to_value(replicas.actors[0].parts.stats[0].read()).unwrap();
        assert_eq!(
            diagnostics["proposal_widths"],
            widths(&[(width, 2 * rounds + 2)])
        );
        assert_eq!(diagnostics["proposed_originals"], 2 * total + 2 * width);
        for actor in &replicas.actors {
            assert_eq!(
                actor.parts.application.applied,
                (2 * total + 2 * width + 1) as u64
            );
            assert!(!actor.parts.core.is_fenced());
            assert!(actor.parts.receiver.is_empty());
            assert!(actor.parts.peer_receiver.is_empty());
            assert!(actor.pending.is_empty());
        }
        println!("CPU6_MATCHED width={width} completed={} all3_applied={} default_checkpoints=2plus lagging_snapshot=true", 2 * total + 2 * width, 2 * total + 2 * width + 1);
    }
}
