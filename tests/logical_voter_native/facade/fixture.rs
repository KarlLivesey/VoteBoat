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
use std::collections::BTreeMap;
use voteboat::{
    identity::*,
    log::Bootstrap,
    log_format::NativeLogCodec,
    native::runtime::*,
    outbound::*,
    quorum::{Limits, Policy, Tree},
    raft::Raft,
    snapshot_format::NativeSnapshotCodec,
    wire_format::NativeWireCodec,
    worker::*,
};
pub type FacadeNode =
    LogicalNode<FairScheduler, DeadlineQueue, JitterEntropy, super::super::application::App>;
pub(super) type Parts =
    LogicalNodeParts<FairScheduler, DeadlineQueue, JitterEntropy, super::super::application::App>;
pub type FacadeRecovery = LogicalNodeRecovery<Parts, CounterReceipt, i64>;
pub fn child_application() -> LogicalApplication {
    let parent = Bootstrap {
        policy: Policy::new(
            Tree::Majority(vec![Tree::Voter(setup::mapping().voter)]),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: BTreeMap::from([(setup::mapping().voter, setup::mapping().store)]),
        ..setup::parent()
    };
    LogicalApplication::new(
        setup::mapping(),
        setup::child(),
        parent,
        setup::limits(),
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_leadership_transfer(voteboat::wire::WireLimits::default()).unwrap(),
    )
    .unwrap()
}
pub fn limits() -> LogicalNodeLimits {
    LogicalNodeLimits {
        configuration: ConfigurationRequestLimits::default(),
        replica: ReplicaDriverLimits {
            persistence_batch_units: 1,
            ..ReplicaDriverLimits::default()
        },
        releases: LogicalReleaseLimits {
            pending: 16,
            bytes: 1024 * 1024,
            capsule_bytes: 8192,
            control_pending: 4,
            control_bytes: 128 * 1024,
        },
    }
}
pub fn budget(completions: usize) -> LogicalBridgeBudget {
    LogicalBridgeBudget {
        node_polls: 3,
        completions,
        transitions: 1,
        node: NodePollBudget::default(),
    }
}
pub fn build(image: &VerifiedLogicalImage) -> Parts {
    let (worker, port) = logical_persistence(
        image,
        StorageWorkerGeneration::new(1).unwrap(),
        WorkerLimits {
            max_requests: 8,
            max_units: 8,
            max_bytes: 64 * 1024,
            control_requests: 2,
            control_units: 2,
            control_bytes: 8192,
            batch_units: 1,
            batch_bytes: 8192,
        },
    )
    .unwrap();
    let mut app = super::super::application::App::new();
    let (core, _) = image.recover_parent(&mut app).unwrap();
    let owner = owner(image, worker.binding(), core);
    let routers = image
        .routers(&owner, LogicalRouterLimits::default())
        .unwrap();
    let outbound = LogicalNetworkQueue::new(
        owner.authorization(),
        OutboundGeneration::new(1).unwrap(),
        OutboundLimits::default(),
        LogicalNetworkLimits::default(),
    )
    .unwrap();
    LogicalNodeParts {
        local: NodeLocalParts {
            owner,
            persistence: worker,
            applications: BTreeMap::from([(image.mapping().parent, app)]),
            results: routers.results,
            clients: routers.clients,
            reads: routers.reads,
            outbound,
            snapshots: None,
        },
        persistence: port,
        snapshots: None,
        sessions: BTreeMap::new(),
    }
}
pub fn attach_snapshots(mut parts: Parts, image: &VerifiedLogicalImage) -> Parts {
    let snapshots = super::super::parent_snapshots::ParentSnapshots::new(
        image,
        &parts.persistence,
        &parts.local.owner,
    );
    parts.local.snapshots = Some(NodeSnapshots {
        worker: snapshots.worker,
        router: snapshots.router,
    });
    parts.snapshots = Some(snapshots.port);
    parts
}
fn owner(
    image: &VerifiedLogicalImage,
    binding: WorkerBinding,
    core: Raft,
) -> EffectOwner<FairScheduler, DeadlineQueue, JitterEntropy> {
    let id = RuntimeOwner {
        store: image.binding(),
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let mut shard = Shard::new(
        id,
        ShardLimits {
            max_groups: 1,
            ..ShardLimits::default()
        },
        FairScheduler::new(1).unwrap(),
    )
    .unwrap();
    shard.register(core).unwrap();
    let timed = TimedShard::new(
        shard,
        DeadlineQueue::new(id, 2).unwrap(),
        JitterEntropy::new(17),
        TimerConfig::default(),
        MonoTime(0),
    )
    .unwrap();
    image
        .into_owner(timed, binding, EffectOwnerLimits::default())
        .unwrap_or_else(|e| panic!("actual child-backed owner {:?}", e.reason))
}
