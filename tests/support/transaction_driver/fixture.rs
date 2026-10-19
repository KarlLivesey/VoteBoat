// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use super::*;
use crate::peer_driver::{Connector, Factory};
use crate::snapshot_routes::Worker as SnapshotWorker;
use support::outbound::HostOutbound;
use voteboat::{
    outbound::*,
    quorum::{Limits, Policy, Tree},
};

pub(super) type HostNode = Node<
    Ready,
    Timers,
    Entropy,
    TransferApplication,
    HostWorker,
    HostOutbound,
    SnapshotWorker,
    Connector,
    Factory,
>;

pub(super) fn profile() -> TransferProfile {
    TransferProfile::new(group(10), [group(20), group(30)]).unwrap()
}
pub(super) fn account() -> AccountId {
    AccountId::new(1).unwrap()
}
pub(super) fn spec(id: u128, amount: i64, reverse: bool) -> TransferSpec {
    let groups = profile().participants();
    let base = id * 100;
    let op = |n| OperationId::new(base + n).unwrap();
    let phases = PhaseIds::new(
        [op(1), op(2), op(3), op(4)],
        [[op(5), op(6), op(7)], [op(8), op(9), op(10)]],
    )
    .unwrap();
    TransferSpec::new(
        TransactionId::new(id).unwrap(),
        profile(),
        AccountRef::new(groups[usize::from(reverse)], account()),
        AccountRef::new(groups[usize::from(!reverse)], account()),
        amount,
        phases,
    )
    .unwrap()
}
fn bootstrap_for(slot: usize) -> Bootstrap {
    let id = slot as u64 + 1;
    let groups = [
        profile().coordinator(),
        profile().participants()[0],
        profile().participants()[1],
    ];
    Bootstrap {
        group: groups[slot],
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(vec![Tree::Voter(node(id))]),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: [(node(id), identity(id as u128))].into(),
    }
}
fn application(slot: usize) -> TransferApplication {
    let limits = TransferLimits::new(8, 1).unwrap();
    if slot == 0 {
        TransferApplication::coordinator(profile().coordinator(), profile(), limits).unwrap()
    } else {
        TransferApplication::participant(
            profile().participants()[slot - 1],
            profile(),
            &[(account(), if slot == 1 { 20 } else { 0 })],
            limits,
        )
        .unwrap()
    }
}
fn owner(slot: usize, store: &mut HostLogStore) -> Owner {
    let bootstrap = bootstrap_for(slot);
    append(store, vec![LogMutation::Create(bootstrap.clone())]);
    let binding = RuntimeOwner {
        store: store.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        binding,
        ShardLimits {
            max_groups: 1,
            ..ShardLimits::default()
        },
        Ready(VecDeque::new()),
    )
    .unwrap();
    shard
        .register(
            Raft::recover(
                node(slot as u64 + 1),
                store.binding(),
                store.state(bootstrap.group).unwrap(),
                store.limits(),
            )
            .unwrap(),
        )
        .unwrap();
    let timed = TimedShard::new(
        shard,
        Timers {
            owner: binding,
            sequence: 0,
            entries: BTreeMap::new(),
        },
        Entropy(17),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    EffectOwner::new(
        timed,
        WorkerBinding {
            store: store.binding(),
            generation: StorageWorkerGeneration::new(1).unwrap(),
        },
        EffectOwnerLimits::default(),
    )
    .unwrap()
}
fn local_parts(
    slot: usize,
) -> NodeLocalParts<
    Ready,
    Timers,
    Entropy,
    TransferApplication,
    HostWorker,
    HostOutbound,
    SnapshotWorker,
> {
    let mut store = HostLogStore::new(slot as u128 + 1);
    let owner = owner(slot, &mut store);
    let binding = owner.identity();
    NodeLocalParts {
        owner,
        persistence: HostWorker::new(store),
        applications: [(bootstrap_for(slot).group, application(slot))].into(),
        results: ApplicationRouter::new(
            ApplicationRouterBinding::new(
                RuntimeAuthorization::physical(binding),
                ApplicationRouterGeneration::new(1).unwrap(),
            ),
            ApplicationRouterLimits::default(),
        )
        .unwrap(),
        clients: ClientRouter::new(
            ClientRouterBinding::new(
                RuntimeAuthorization::physical(binding),
                ClientRouterGeneration::new(1).unwrap(),
            ),
            ClientRouterLimits::default(),
        )
        .unwrap(),
        reads: ReadRequests::new(
            ReadInvocationBinding::new(
                RuntimeAuthorization::physical(binding),
                ReadInvocationGeneration::new(1).unwrap(),
            ),
            ReadInvocationLimits::default(),
            ReadRouter::new(
                ReadRouterBinding::new(
                    RuntimeAuthorization::physical(binding),
                    ReadRouterGeneration::new(1).unwrap(),
                ),
                ReadRouterLimits::default(),
            )
            .unwrap(),
        )
        .unwrap(),
        outbound: HostOutbound::new(
            OutboundBinding {
                node: node(slot as u64 + 1),
                store: binding.store,
                generation: OutboundGeneration::new(1).unwrap(),
            },
            OutboundLimits::default(),
        )
        .unwrap(),
        snapshots: None,
    }
}
pub(super) fn nodes() -> Vec<HostNode> {
    let mut nodes = (0..3)
        .map(|slot| {
            Node::from_parts(
                NodeParts {
                    local: local_parts(slot),
                    peers: None,
                },
                // Short conformance runs without a snapshot worker: the
                // explicit lifetime-bounded opt-out.
                NodeLimits {
                    retention: RetentionMode::LifetimeBounded,
                    ..Default::default()
                },
                MonoTime(0),
            )
            .unwrap_or_else(|e| panic!("{:?}", e.reason))
        })
        .collect::<Vec<_>>();
    for (slot, n) in nodes.iter_mut().enumerate() {
        n.control(bootstrap_for(slot).group, NodeControl::Campaign)
            .unwrap();
    }
    settle(&mut nodes);
    nodes
}
pub(super) fn settle(nodes: &mut [HostNode]) {
    for _ in 0..1000 {
        for n in &mut *nodes {
            n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        }
        if nodes.iter().all(|n| {
            n.local().owner.is_drained()
                && n.local().persistence.is_drained()
                && n.replica_usage().leases() == 0
        }) {
            return;
        }
    }
    panic!("host nodes did not settle");
}
pub(super) fn balances(nodes: &[HostNode]) -> [AccountRead; 2] {
    [
        nodes[1].local().applications[&profile().participants()[0]].account(account()),
        nodes[2].local().applications[&profile().participants()[1]].account(account()),
    ]
}
pub(super) fn close(nodes: &mut [HostNode]) {
    for n in &mut *nodes {
        n.begin_shutdown();
    }
    for _ in 0..1000 {
        for n in &mut *nodes {
            n.poll(MonoTime(0), NodePollBudget::default()).unwrap();
        }
        if nodes.iter().all(|n| n.state() == NodeState::Drained) {
            return;
        }
    }
    panic!("host Node ownership did not drain");
}
