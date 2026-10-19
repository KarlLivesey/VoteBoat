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
pub type FacadeNode = LogicalNode<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    LogicalActivationApplication<super::application::App>,
>;
pub type Parts = LogicalNodeParts<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    LogicalActivationApplication<super::application::App>,
>;

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
        storage_limits(),
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_leadership_transfer(voteboat::wire::WireLimits::default()).unwrap(),
    )
    .unwrap()
    .with_activation_control_profile(app().control_profile())
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
            bytes: 4 * 1024 * 1024,
            capsule_bytes: 8192,
            control_pending: 4,
            control_bytes: 3 * 1024 * 1024,
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
    build_at(image, MonoTime(0))
}
pub fn build_at(image: &VerifiedLogicalImage, now: MonoTime) -> Parts {
    build_with_timers(image, now, TimerConfig::default())
}
pub fn build_with_timers(
    image: &VerifiedLogicalImage,
    now: MonoTime,
    timers: TimerConfig,
) -> Parts {
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
    let mut app = app_with_mappings(
        image
            .activation_control_profile()
            .unwrap()
            .mappings()
            .to_vec(),
    );
    let (core, _) = image.recover_parent(&mut app).unwrap();
    let owner = owner(image, worker.binding(), core, now, timers);
    let routers = image.routers(&owner, router_limits()).unwrap();
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
pub fn router_limits() -> LogicalRouterLimits {
    let count = storage_limits().parent_log.max_entries_per_group;
    // The actual wrapper retains one ActivationReceipt per command and its
    // bounded Counter supplies the inner full-batch receipt bound.
    let batch_bytes = count
        * (std::mem::size_of::<ActivationReceipt<voteboat::application::CounterReceipt>>()
            + std::mem::size_of::<voteboat::application::CounterReceipt>());
    LogicalRouterLimits {
        applications: ApplicationRouterLimits {
            batch_receipts: count,
            batch_bytes,
            control_batches: 2,
            ..ApplicationRouterLimits::default()
        },
        ..LogicalRouterLimits::default()
    }
}
fn owner(
    image: &VerifiedLogicalImage,
    binding: WorkerBinding,
    core: Raft,
    now: MonoTime,
    timers: TimerConfig,
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
        JitterEntropy::new(image.mapping().voter.get() * 17),
        timers,
        now,
    )
    .unwrap();
    image
        .into_owner(timed, binding, EffectOwnerLimits::default())
        .unwrap_or_else(|e| panic!("actual child-backed owner {:?}", e.reason))
}

pub fn app() -> LogicalActivationApplication<super::application::App> {
    app_with_mappings(vec![setup::mapping()])
}
pub fn app_with_mappings(
    mappings: Vec<LogicalStoreMapping>,
) -> LogicalActivationApplication<super::application::App> {
    LogicalActivationApplication::new(
        setup::mapping().parent,
        mappings,
        LogicalActivationLimits {
            intents: 2,
            command_bytes: 1024,
            checkpoint_bytes: 64 * 1024,
        },
        super::application::App::new(),
    )
    .unwrap()
}
pub fn storage_limits() -> LogicalLimits {
    let mut limits = setup::limits();
    limits.control_action_bytes = 64 * 1024;
    limits.parent_snapshots.max_application_bytes = 64 * 1024;
    limits.parent_log.max_snapshot_bytes = 128 * 1024;
    limits.max_parent_checkpoint_bytes = 128 * 1024;
    limits.max_child_checkpoint_bytes = 512 * 1024;
    limits.validate().unwrap()
}
