// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Differential resource inspection; no membership or durability authority.
use super::*;
use crate::{contracts::HardState, log::*, membership::*, quorum::*, snapshot::*};

#[derive(Default)]
struct Ready(VecDeque<GroupIdentity>);
impl ReadyScheduler for Ready {
    fn capacity(&self) -> usize {
        8
    }
    fn enqueue(&mut self, group: GroupIdentity) -> Result<(), RuntimeError> {
        if !self.0.contains(&group) {
            self.0.push_back(group);
        }
        Ok(())
    }
    fn pop(&mut self) -> Option<GroupIdentity> {
        self.0.pop_front()
    }
    fn cancel(&mut self, group: GroupIdentity) {
        self.0.retain(|old| *old != group);
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}
fn node(n: u64) -> NodeId {
    NodeId::new(n).unwrap()
}
fn store(n: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(n).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn group(n: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(n).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn binding() -> StoreBinding {
    StoreBinding {
        identity: store(1),
        session: StoreSession::new(1).unwrap(),
    }
}
fn bootstrap(n: u128) -> Bootstrap {
    Bootstrap {
        group: group(n),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(vec![Tree::Voter(node(1)), Tree::Voter(node(2))]),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: BTreeMap::from([(node(1), store(1)), (node(2), store(2))]),
    }
}
fn core(n: u128) -> Raft {
    Raft::recover(
        node(1),
        binding(),
        GroupLog {
            bootstrap: bootstrap(n),
            revision: LogRevision::new(1).unwrap(),
            generation: LogGeneration::new(1).unwrap(),
            hard_state: HardState::default(),
            ballot_origin: None,
            commit_index: 0,
            snapshot: None,
            snapshot_membership: None,
            entries: vec![],
        },
        LogLimits::default(),
    )
    .unwrap()
}
fn shard(
    limit: usize,
    history: BTreeMap<NodeId, StoreIdentity>,
    provisioned: Option<BTreeMap<NodeId, StoreIdentity>>,
) -> Shard<Ready> {
    let owner = RuntimeOwner {
        store: binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        owner,
        ShardLimits {
            max_groups: 8,
            ..Default::default()
        },
        Ready::default(),
    )
    .unwrap();
    shard.register(core(1)).unwrap();
    shard.register(core(2)).unwrap();
    let mut budget = ConnectionBudget::new(
        LocalIdentity {
            node: node(1),
            store: binding(),
        },
        limit,
        history,
    )
    .unwrap();
    if let Some(peers) = provisioned {
        budget = budget.with_provisioned(peers).unwrap();
    }
    // Driver-owned route installation; the public capacity setter deliberately
    // preserves the previously installed route policy.
    let budget = shard.prepare_connection_budget(budget).unwrap();
    shard.install_connection_budget(budget);
    shard
}
fn receive(n: u128, rpc: Rpc) -> Event {
    Event::Receive(Message {
        group: group(n),
        configuration: ConfigurationId::new(1).unwrap(),
        from: node(2),
        sender: StoreBinding {
            identity: store(2),
            session: StoreSession::new(1).unwrap(),
        },
        to: node(1),
        term: 1,
        context: RequestContext {
            origin: binding(),
            sequence: 1,
        },
        rpc,
    })
}
fn snapshot(n: u128, membership: bool, kind: u8) -> Event {
    let initial = core(n);
    let snapshot = Box::new(Snapshot {
        metadata: SnapshotMetadata {
            bootstrap: bootstrap(n),
            membership: membership.then(|| Box::new(initial.state().membership_at(0).unwrap())),
            index: 1,
            term: 1,
            application_schema: 1,
        },
        application: vec![],
    });
    receive(
        n,
        match kind {
            0 => Rpc::Snapshot { snapshot },
            1 => Rpc::LearnerRepairSnapshot { snapshot },
            _ => Rpc::CommittedLearnerRepairSnapshot { snapshot },
        },
    )
}
fn learner(n: u128, peer: u64, identity: StoreIdentity) -> Event {
    let initial = bootstrap(n);
    let next = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        initial.policy,
        initial.voter_stores,
        BTreeMap::from([(node(peer), identity)]),
    )
    .unwrap();
    receive(
        n,
        Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            leader_commit: 0,
            entries: vec![LogEntry {
                index: 1,
                term: 1,
                payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
                    operation: OperationId::new(1).unwrap(),
                    expected: ConfigurationId::new(1).unwrap(),
                    change: ConfigurationChange::Learners(next),
                })),
            }],
        },
    )
}
fn compare(
    shard: &Shard<Ready>,
    n: u128,
    event: &Event,
    covered: bool,
) -> Result<(), RuntimeError> {
    let budget = shard.connection_budget().unwrap();
    assert_eq!(
        shard.covered_connection_event(budget, group(n), event),
        covered
    );
    let reference = shard
        .reserved_connections(budget, Some((group(n), event)), None)
        .map(|_| ());
    let actual = shard.check_connection_event(group(n), event);
    assert_eq!(actual, reference);
    actual
}
#[test]
fn repeated_known_snapshots_and_configurations_match_full_union_at_capacity() {
    let shard = shard(2, BTreeMap::from([(node(3), store(3))]), None);
    for membership in [false, true] {
        for kind in 0..3 {
            let event = snapshot(1, membership, kind);
            for _ in 0..4 {
                compare(&shard, 1, &event, true).unwrap();
            }
        }
    }
    compare(&shard, 1, &learner(1, 3, store(3)), true).unwrap();
    assert_eq!(shard.reserved_connection_peers().unwrap(), Some(2));
    assert_eq!(shard.usage().items, 0);
}
#[test]
fn historical_but_unprovisioned_peer_uses_reference_refusal() {
    let mut shard = shard(
        2,
        BTreeMap::from([(node(3), store(3))]),
        Some(BTreeMap::from([(node(2), store(2))])),
    );
    compare(&shard, 1, &snapshot(1, true, 0), true).unwrap();
    let event = learner(1, 3, store(3));
    assert_eq!(
        compare(&shard, 1, &event, false),
        Err(RuntimeError::PeerUnavailable)
    );
    let budget = shard
        .connection_budget()
        .unwrap()
        .clone()
        .with_provisioned(BTreeMap::from([(node(2), store(2)), (node(3), store(3))]))
        .unwrap();
    let budget = shard.prepare_connection_budget(budget).unwrap();
    shard.install_connection_budget(budget);
    compare(&shard, 1, &event, true).unwrap();
}
#[test]
fn queued_unknown_peers_keep_union_capacity_and_exact_store_conflicts() {
    let mut shard = shard(2, BTreeMap::new(), None);
    let future = learner(1, 3, store(3));
    compare(&shard, 1, &future, false).unwrap();
    shard.admit(group(1), future).unwrap();
    compare(&shard, 2, &snapshot(2, false, 0), true).unwrap();
    assert_eq!(
        compare(&shard, 2, &learner(2, 4, store(4)), false),
        Err(RuntimeError::PeerCapacity)
    );
    let mut changed = store(3);
    changed.incarnation = StoreIncarnation::new(2).unwrap();
    assert_eq!(
        compare(&shard, 2, &learner(2, 3, changed), false),
        Err(RuntimeError::PeerStoreConflict)
    );
    assert_eq!(shard.reserved_connection_peers().unwrap(), Some(2));
    assert_eq!(shard.usage().items, 1);
}
#[test]
fn another_live_fenced_core_preserves_global_failure_before_bad_target() {
    let mut shard = shard(1, BTreeMap::new(), None);
    shard.admit(group(2), Event::Heartbeat).unwrap();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    assert_eq!(visit.group, group(2));
    shard
        .with_core(visit, |core| core.storage_failed())
        .unwrap();
    assert!(!shard.groups[&group(2)].fenced);
    let mut event = snapshot(1, false, 0);
    assert_eq!(compare(&shard, 1, &event, false), Err(RuntimeError::Fenced));
    let Event::Receive(message) = &mut event else {
        unreachable!()
    };
    message.to = node(9);
    assert_eq!(compare(&shard, 1, &event, false), Err(RuntimeError::Fenced));
    shard.stop_group(group(2)).unwrap();
    assert_eq!(
        compare(&shard, 1, &event, false),
        Err(RuntimeError::WrongOwner)
    );
    compare(&shard, 1, &snapshot(1, false, 0), true).unwrap();
}
#[test]
fn local_identity_group_and_target_mismatches_never_skip_reference() {
    let shard = shard(1, BTreeMap::new(), None);
    for mutation in 0..3 {
        let mut event = snapshot(1, false, 0);
        let Event::Receive(message) = &mut event else {
            unreachable!()
        };
        match mutation {
            0 => message.to = node(9),
            1 => message.group = group(9),
            _ => {
                let Rpc::Snapshot { snapshot } = &mut message.rpc else {
                    unreachable!()
                };
                snapshot
                    .metadata
                    .bootstrap
                    .voter_stores
                    .insert(node(1), store(9));
            }
        }
        assert!(compare(&shard, 1, &event, false).is_err());
    }
}
#[test]
fn failed_tightening_stop_and_register_keep_exact_owned_union() {
    let mut shard = shard(2, BTreeMap::new(), None);
    shard.admit(group(1), learner(1, 3, store(3))).unwrap();
    let local = shard.connection_budget().unwrap().local();
    assert_eq!(
        shard.set_connection_budget(ConnectionBudget::new(local, 1, BTreeMap::new()).unwrap()),
        Err(RuntimeError::PeerCapacity)
    );
    assert_eq!(shard.connection_budget().unwrap().limit(), 2);
    compare(&shard, 2, &snapshot(2, true, 0), true).unwrap();
    shard.stop_group(group(1)).unwrap();
    shard
        .set_connection_budget(ConnectionBudget::new(local, 1, BTreeMap::new()).unwrap())
        .unwrap();
    shard.register(core(3)).unwrap();
    compare(&shard, 3, &snapshot(3, false, 0), true).unwrap();
}

#[test]
fn caught_mutable_callback_panic_falls_back_until_full_post_guard_succeeds() {
    let mut shard = shard(1, BTreeMap::new(), None);
    shard.admit(group(1), Event::Heartbeat).unwrap();
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    let before = shard.usage();
    let mut state = core(1).state().clone();
    state.bootstrap.policy = Policy::new(
        Tree::Majority((1..=3).map(|n| Tree::Voter(node(n))).collect()),
        Limits::default(),
    )
    .unwrap();
    state.bootstrap.voter_stores.insert(node(3), store(3));
    let replacement = Raft::recover(node(1), binding(), state, LogLimits::default()).unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        shard.with_core(visit, |current| {
            *current = replacement;
            panic!("host callback unwind after replacement");
        })
    }));
    assert!(panic.is_err());
    assert_eq!(shard.usage(), before);
    assert_eq!(
        compare(&shard, 2, &snapshot(2, false, 0), false),
        Err(RuntimeError::PeerCapacity)
    );
    let repaired = shard.with_core(visit, |current| {
        *current = core(1);
        Err::<(), _>(RaftError::Busy)
    });
    assert_eq!(repaired, Ok(Err(RaftError::Busy)));
    assert_eq!(shard.usage(), before);
    compare(&shard, 2, &snapshot(2, false, 0), true).unwrap();
    let stopped = shard.stop_group(group(1)).unwrap();
    assert_eq!(stopped.queued, vec![Event::Heartbeat]);
    assert_eq!(shard.usage(), Usage::default());
}
