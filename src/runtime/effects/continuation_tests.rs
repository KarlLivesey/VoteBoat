// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! A completion's continuation takes the released effect's place in its visit.
use super::peer_ingress_tests::{group, node, owner_with, store, vote_in, Owner};
use super::*;
use crate::snapshot::SnapshotRef;

/// Accepts every batch and records nothing durable; the owner-side checks in
/// `submit_persists` run before it is reached.
struct AcceptingWorker {
    binding: WorkerBinding,
    sequence: u64,
    units: usize,
}
impl AcceptingWorker {
    fn new(owner: &Owner) -> Self {
        Self {
            binding: owner.worker_binding(),
            sequence: 0,
            units: 0,
        }
    }
}
impl PersistenceWorker for AcceptingWorker {
    fn binding(&self) -> WorkerBinding {
        self.binding
    }
    fn limits(&self) -> WorkerLimits {
        WorkerLimits::default()
    }
    fn usage(&self) -> WorkerUsage {
        WorkerUsage {
            requests: 0,
            units: 0,
            bytes: 0,
        }
    }
    fn submit(&mut self, units: Vec<PersistUnit>) -> Result<WorkerTicket, WorkerRejected> {
        self.sequence += 1;
        self.units += units.len();
        Ok(WorkerTicket {
            binding: self.binding,
            sequence: self.sequence,
        })
    }
    fn poll(&mut self, _limit: usize) -> Vec<WorkerEvent> {
        Vec::new()
    }
    fn close(&mut self) {}
}

#[derive(Debug, Eq, PartialEq)]
enum Shape {
    SnapshotRequired,
    /// A Send, named by its context sequence.
    Send(u64),
    Persist,
}
fn shape(effect: &Effect) -> Shape {
    match effect {
        Effect::SnapshotRequired { .. } => Shape::SnapshotRequired,
        Effect::Send(message) => Shape::Send(message.context.sequence),
        Effect::Persist(_) => Shape::Persist,
        other => panic!("unexpected effect {other:?}"),
    }
}
fn queue(owner: &Owner, g: u128) -> Vec<Shape> {
    owner.active[&group(g)].effects.iter().map(shape).collect()
}
fn snapshot_required(g: u128) -> Effect {
    Effect::SnapshotRequired {
        to: node(3),
        context: RequestContext {
            origin: store(1),
            sequence: 7,
        },
        reference: SnapshotRef {
            store: store(1).identity,
            group: group(g),
            generation: SnapshotGeneration::new(1).unwrap(),
            configuration: ConfigurationId::new(1).unwrap(),
            index: 1,
            term: 1,
            application_schema: 1,
            file_bytes: 1,
            checksum: 0,
        },
    }
}
/// A Send to node 3 of group `g`. The owner orders by effect kind and peer
/// only, so the payload stands in for the loaded image's `Rpc::Snapshot`.
fn send(g: u128, sequence: u64) -> Effect {
    Effect::Send(Message {
        group: group(g),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(1),
        sender: store(1),
        to: node(3),
        term: 2,
        context: RequestContext {
            origin: store(1),
            sequence,
        },
        rpc: Rpc::Voted { granted: false },
    })
}
/// Advance one vote per group (each step emits only its Persist), then put
/// `ahead` in front of that Persist, as one leader step emitting
/// `[SnapshotRequired, …, Persist]` would have queued it.
fn visits(groups: &[(u128, Vec<Effect>)]) -> Owner {
    let ids = groups.iter().map(|(g, _)| *g).collect::<Vec<_>>();
    let mut owner = owner_with(&ids);
    for &g in &ids {
        owner.admit(group(g), vote_in(g, 2, 2)).unwrap();
    }
    let steps = owner.advance(MonoTime(0), ids.len()).unwrap();
    assert_eq!(steps.len(), ids.len());
    assert!(steps.iter().all(|s| s.error.is_none()));
    for (g, ahead) in groups {
        let a = owner.active.get_mut(&group(*g)).unwrap();
        for effect in ahead.iter().rev() {
            a.effects.push_front(effect.clone());
        }
    }
    owner
}
/// Lease the visit's front SnapshotRequired with room for its loaded image.
fn take_snapshot_lease(owner: &mut Owner) -> EffectLease {
    let lease = owner.take_effect().unwrap().unwrap();
    assert_eq!(shape(&lease.effect), Shape::SnapshotRequired);
    owner.extend_reservation(lease.ticket, 64 * 1024).unwrap();
    lease
}
fn complete(owner: &mut Owner, lease: EffectLease, continuation: Vec<Effect>) {
    owner
        .complete_effect_with(lease, 0, MonoTime(0), |_, effect| {
            assert_eq!(shape(effect), Shape::SnapshotRequired);
            Ok((continuation, ()))
        })
        .unwrap();
}
/// Release every queued Send in lease order and return the Persists, which
/// must each be the last effect their visit leases.
fn drain(owner: &mut Owner, sent: &mut BTreeMap<GroupIdentity, Vec<u64>>) -> Vec<EffectLease> {
    let mut persists = Vec::new();
    while let Some(lease) = owner.take_effect().unwrap() {
        match shape(&lease.effect) {
            Shape::Send(sequence) => {
                sent.entry(lease.ticket.visit.group)
                    .or_default()
                    .push(sequence);
                owner.release_transferred_send(lease.ticket).unwrap();
            }
            Shape::Persist => {
                assert!(owner.active[&lease.ticket.visit.group].effects.is_empty());
                persists.push(lease);
            }
            Shape::SnapshotRequired => panic!("snapshot lease left queued"),
        }
    }
    persists
}

#[test]
fn loaded_snapshot_send_takes_its_place_ahead_of_the_steps_persist() {
    let mut owner = visits(&[(1, vec![snapshot_required(1)])]);
    assert_eq!(queue(&owner, 1), [Shape::SnapshotRequired, Shape::Persist]);
    let lease = take_snapshot_lease(&mut owner);
    complete(&mut owner, lease, vec![send(1, 11)]);
    assert_eq!(queue(&owner, 1), [Shape::Send(11), Shape::Persist]);
    let mut sent = BTreeMap::new();
    let persists = drain(&mut owner, &mut sent);
    assert_eq!(sent[&group(1)], [11]);
    assert_eq!(persists.len(), 1);
    let mut worker = AcceptingWorker::new(&owner);
    owner
        .submit_persists(&mut worker, persists, MonoTime(0))
        .unwrap();
    assert_eq!(worker.units, 1);
    assert!(!owner.is_failed());
}

#[test]
fn continuations_released_out_of_lease_order_keep_their_positions() {
    let mut owner = visits(&[
        (1, vec![snapshot_required(1), send(1, 19)]),
        (2, vec![snapshot_required(2)]),
    ]);
    assert_eq!(
        queue(&owner, 1),
        [Shape::SnapshotRequired, Shape::Send(19), Shape::Persist]
    );
    assert_eq!(queue(&owner, 2), [Shape::SnapshotRequired, Shape::Persist]);
    let first = take_snapshot_lease(&mut owner);
    let second = take_snapshot_lease(&mut owner);
    assert_eq!(first.ticket.visit.group, group(1));
    assert_eq!(second.ticket.visit.group, group(2));
    assert!(first.ticket.sequence < second.ticket.sequence);
    // The later lease completes first, with a two-effect continuation.
    complete(&mut owner, second, vec![send(2, 21), send(2, 22)]);
    assert_eq!(
        queue(&owner, 2),
        [Shape::Send(21), Shape::Send(22), Shape::Persist]
    );
    assert_eq!(queue(&owner, 1), [Shape::Send(19), Shape::Persist]);
    complete(&mut owner, first, vec![send(1, 11)]);
    assert_eq!(
        queue(&owner, 1),
        [Shape::Send(11), Shape::Send(19), Shape::Persist]
    );
    let mut sent = BTreeMap::new();
    let persists = drain(&mut owner, &mut sent);
    assert_eq!(sent[&group(1)], [11, 19]);
    assert_eq!(sent[&group(2)], [21, 22]);
    assert_eq!(persists.len(), 2);
    let mut worker = AcceptingWorker::new(&owner);
    owner
        .submit_persists(&mut worker, persists, MonoTime(0))
        .unwrap();
    assert_eq!(worker.units, 2);
    assert!(!owner.is_failed());
}
