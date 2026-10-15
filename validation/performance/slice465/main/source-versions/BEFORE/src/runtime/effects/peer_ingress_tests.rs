// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Original ingress ownership only; local handback is not remote durability.
use super::*;
use crate::{
    contracts::HardState,
    log::*,
    native::runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
    quorum::*,
};

type Owner = EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy>;
fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store(id: u128) -> StoreBinding {
    StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(id).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    }
}
fn owner() -> Owner {
    let bootstrap = Bootstrap {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (1..=3)
            .map(|n| (node(n), store(u128::from(n)).identity))
            .collect(),
    };
    let mut states = BTreeMap::new();
    let log = LogLimits::default();
    apply_batch(&mut states, &[LogMutation::Create(bootstrap)], log).unwrap();
    let revision = states[&group(1)].revision;
    apply_batch(
        &mut states,
        &[LogMutation::Update(LogUpdate {
            group: group(1),
            expected_revision: revision,
            hard_state: HardState {
                term: 1,
                voted_for: Some(node(1)),
            },
            commit_index: 0,
            suffix: None,
            snapshot: None,
            snapshot_membership: None,
        })],
        log,
    )
    .unwrap();
    let core = Raft::recover(node(1), store(1), states.remove(&group(1)).unwrap(), log).unwrap();
    let identity = RuntimeOwner {
        store: store(1),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        identity,
        ShardLimits {
            max_groups: 1,
            ..Default::default()
        },
        FairScheduler::new(1).unwrap(),
    )
    .unwrap();
    shard.register(core).unwrap();
    let timed = TimedShard::new(
        shard,
        DeadlineQueue::new(identity, 1).unwrap(),
        JitterEntropy::new(1),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    EffectOwner::new(
        timed,
        WorkerBinding {
            store: store(1),
            generation: StorageWorkerGeneration::new(1).unwrap(),
        },
        EffectOwnerLimits::default(),
    )
    .unwrap()
}
fn vote(from: u64, term: u64) -> Event {
    Event::Receive(Message {
        group: group(1),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(from),
        sender: store(u128::from(from)),
        to: node(1),
        term,
        context: RequestContext {
            origin: store(u128::from(from)),
            sequence: 1,
        },
        rpc: Rpc::Vote {
            last_index: 0,
            last_term: 0,
        },
    })
}

#[test]
fn queued_and_leased_original_peer_ingress_retires_only_with_its_visit() {
    let mut owner = owner();
    owner.admit(group(1), vote(2, 1)).unwrap();
    owner.admit(group(1), vote(3, 1)).unwrap();
    assert!(owner.has_pending_peer_ingress(group(1), node(2)));
    assert!(owner.has_pending_peer_ingress(group(1), node(3)));
    assert!(!owner.has_pending_peer_ingress(group(2), node(2)));
    let steps = owner.advance(MonoTime(0), 1).unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].error, None);
    let lease = owner.take_effect().unwrap().unwrap();
    assert!(
        matches!(&lease.effect, Effect::Send(m) if m.to == node(2) && matches!(m.rpc, Rpc::Voted {granted: false}))
    );
    assert!(owner.has_pending_peer_ingress(group(1), node(2)));
    assert!(owner.has_pending_send(node(2)));
    owner.release_transferred_send(lease.ticket).unwrap();
    assert!(!owner.has_pending_peer_ingress(group(1), node(2)));
    assert!(!owner.has_pending_send(node(2)));
    assert!(owner.has_pending_peer_ingress(group(1), node(3)));
}

#[test]
fn original_receive_source_survives_queued_input_consumption_and_persistence_lease() {
    let mut owner = owner();
    owner.admit(group(1), vote(2, 2)).unwrap();
    let steps = owner.advance(MonoTime(0), 1).unwrap();
    assert_eq!(steps[0].error, None);
    assert!(!owner.runtime.has_pending_peer_ingress(group(1), node(2)));
    let lease = owner.take_effect().unwrap().unwrap();
    assert!(matches!(lease.effect, Effect::Persist(_)));
    assert!(owner.has_pending_peer_ingress(group(1), node(2)));
    assert!(!owner.has_pending_peer_ingress(group(1), node(3)));
    assert!(!owner.has_pending_peer_ingress(group(2), node(2)));
    assert_eq!(
        owner.inspect_visit(group(1)).unwrap().unwrap().leased(),
        Some(lease.ticket)
    );
}

#[test]
fn deferred_payload_reservation_keeps_source_capacity_until_visit_retirement() {
    let mut owner = owner();
    owner.admit(group(1), vote(2, 1)).unwrap();
    let steps = owner.advance(MonoTime(0), 1).unwrap();
    let visit = steps[0].visit;
    let lease = owner.take_effect().unwrap().unwrap();
    assert!(matches!(lease.effect, Effect::Send(_)));
    // Isolate deferred-payload accounting with a real live visit/lease. The
    // joined transition/worker test separately proves actual source collection.
    let active = owner.active.get_mut(&group(1)).unwrap();
    active.other_received_from = Vec::with_capacity(7);
    active.other_received_from.push(node(3));
    let source_bytes = active.other_received_from.capacity() * size_of::<NodeId>();
    let capacity = active.effects.capacity();
    let payload = effect_bytes(active.effects.make_contiguous(), capacity).unwrap()
        + effect_bytes(std::slice::from_ref(&lease.effect), 1).unwrap();
    let original = owner.usage().reserved_bytes;
    owner.defer_snapshot(&lease).unwrap();
    assert_eq!(owner.usage().reserved_bytes, payload + source_bytes);
    assert!(owner.usage().reserved_bytes < original);
    assert!(owner.has_pending_peer_ingress(group(1), node(2)));
    assert!(owner.has_pending_peer_ingress(group(1), node(3)));
    assert_eq!(
        owner.inspect_visit(group(1)).unwrap().unwrap().visit(),
        visit
    );
    owner.defer_snapshot(&lease).unwrap();
    assert_eq!(owner.usage().reserved_bytes, payload + source_bytes);
    owner.release_transferred_send(lease.ticket).unwrap();
    assert_eq!(owner.usage().reserved_bytes, 0);
    assert!(!owner.has_pending_peer_ingress(group(1), node(2)));
    assert!(!owner.has_pending_peer_ingress(group(1), node(3)));
}
