// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub(super) struct Replica {
    pub owner: Owner,
    pub worker: HostWorker,
    pub app: Counter,
    held: Option<EffectLease>,
}
pub(super) fn cluster() -> Vec<Replica> {
    let mut nodes: Vec<_> = (1..=3)
        .map(|id| {
            let mut store = HostLogStore::new(id);
            let runtime = timed(id as u64, &mut store, 1, 3);
            let worker = HostWorker::new(store);
            Replica {
                owner: EffectOwner::new(runtime, worker.binding(), EffectOwnerLimits::default())
                    .unwrap(),
                worker,
                app: Counter::new(100).unwrap(),
                held: None,
            }
        })
        .collect();
    nodes[0].owner.admit(group(1), Event::Campaign).unwrap();
    drive(&mut nodes);
    assert_eq!(nodes[0].owner.core(group(1)).unwrap().role(), Role::Leader);
    assert_eq!(nodes[0].app.applied_index(), 1);
    nodes
}

pub(super) fn stage(replica: &mut Replica) -> WorkerEvent {
    stage_inner(replica, None)
}
pub(super) fn stage_with_remaining_space(replica: &mut Replica, remaining: usize) -> WorkerEvent {
    stage_inner(replica, Some(remaining))
}
fn stage_inner(replica: &mut Replica, remaining: Option<usize>) -> WorkerEvent {
    replica
        .owner
        .admit(
            group(1),
            Event::Propose {
                operation: Counter::operation(1, 71).unwrap(),
                bytes: Counter::command(0, 7),
            },
        )
        .unwrap();
    let steps = replica.owner.advance(MonoTime(0), 1).unwrap();
    assert_eq!(steps.len(), 1);
    assert!(steps[0].error.is_none());
    let lease = replica.owner.take_effect().unwrap().unwrap();
    assert!(matches!(lease.effect, Effect::Persist(_)));
    if let Some(remaining) = remaining {
        let limits = replica.owner.limits();
        let extra = (limits.reserved_bytes - limits.control_bytes)
            .saturating_sub(replica.owner.usage().reserved_bytes + remaining);
        replica
            .owner
            .extend_reservation(lease.ticket, extra)
            .unwrap();
    }
    replica
        .owner
        .submit_persists(&mut replica.worker, vec![lease], MonoTime(0))
        .unwrap();
    let written = replica.worker.poll(1).pop().unwrap();
    assert!(matches!(written, WorkerEvent::Written { .. }));
    written
}

pub(super) fn drive_one(nodes: &mut [Replica], index: usize) {
    for _ in 0..30 {
        let replica = &mut nodes[index];
        for event in replica.worker.poll(1) {
            replica.owner.deliver_worker(event, MonoTime(0)).unwrap();
        }
        let steps = replica.owner.advance(MonoTime(0), 4).unwrap();
        assert!(steps.iter().all(|step| step.error.is_none()));
        let mut outgoing = Vec::new();
        if replica.worker.is_drained() {
            if let Some(lease) = replica.held.take() {
                replica
                    .owner
                    .submit_persists(&mut replica.worker, vec![lease], MonoTime(0))
                    .unwrap();
            }
        }
        while let Some(lease) = replica.owner.take_effect().unwrap() {
            match &lease.effect {
                Effect::Persist(_) => {
                    assert!(replica.held.replace(lease).is_none());
                    break;
                }
                Effect::Send(message) => {
                    outgoing.push(message.clone());
                    replica
                        .owner
                        .release_transferred_send(lease.ticket)
                        .unwrap();
                }
                Effect::Committed(entries) => {
                    replica.app.apply_batch(entries).unwrap();
                    replica
                        .owner
                        .release(lease, replica.app.applied_index(), MonoTime(0))
                        .unwrap();
                }
                other => panic!("unexpected {other:?}"),
            }
        }
        for message in outgoing {
            let target = message.to.get() as usize - 1;
            nodes[target]
                .owner
                .admit(group(1), Event::Receive(message))
                .unwrap();
        }
    }
}

pub(super) fn drive(nodes: &mut [Replica]) {
    for _ in 0..100 {
        for index in 0..nodes.len() {
            drive_one(nodes, index);
        }
        if nodes.iter().all(|replica| {
            replica.held.is_none() && replica.worker.is_drained() && replica.owner.is_drained()
        }) {
            return;
        }
    }
    panic!("cluster did not drain");
}
