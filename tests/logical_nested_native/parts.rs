// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::collections::BTreeMap;
use voteboat::{
    application::*, identity::*, logical_voter::*, native::runtime::*, outbound::*, raft::*,
    runtime::*, snapshot_worker::*, worker::*,
};
pub type Parts<A> = LogicalNodeParts<FairScheduler, DeadlineQueue, JitterEntropy, A>;
fn owner(
    image: &VerifiedLogicalImage,
    binding: WorkerBinding,
    core: Raft,
    now: MonoTime,
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
    let runtime = TimedShard::new(
        shard,
        DeadlineQueue::new(id, 2).unwrap(),
        JitterEntropy::new(image.mapping().voter.get() * 17),
        super::setup::timers(image.mapping().parent),
        now,
    )
    .unwrap();
    image
        .into_owner(runtime, binding, EffectOwnerLimits::default())
        .unwrap_or_else(|e| panic!("original nested owner: {:?}", e.reason))
}
pub fn build<A>(
    image: &VerifiedLogicalImage,
    mut app: A,
    limits: LogicalLimits,
    now: MonoTime,
) -> Parts<A>
where
    A: ProposalAdmission + LogicalResponseCodec + CheckpointStateMachine,
    A::Receipt: ApplicationReceipt,
{
    let (worker, port) = logical_persistence(
        image,
        StorageWorkerGeneration::new(1).unwrap(),
        super::setup::persistence_limits(limits),
    )
    .unwrap();
    let (core, _) = image.recover_parent(&mut app).unwrap();
    let owner = owner(image, worker.binding(), core, now);
    let (invocations, execution) = super::setup::read_limits(limits);
    let routers = image
        .routers(
            &owner,
            LogicalRouterLimits {
                clients: super::setup::client_limits(limits),
                invocations,
                execution,
                ..LogicalRouterLimits::default()
            },
        )
        .unwrap();
    let (snapshot_bytes, snapshot_control) = super::setup::snapshot_budget(limits);
    let (snapshots, snapshot_port) = logical_snapshots(
        image,
        &port,
        SnapshotWorkerGeneration::new(1).unwrap(),
        SnapshotWorkLimits {
            max_groups: 1,
            max_requests: 4,
            max_bytes: snapshot_bytes,
            control_requests: 1,
            control_bytes: snapshot_control,
        },
    )
    .unwrap();
    let router = SnapshotRouter::new(
        owner.identity(),
        snapshots.binding(),
        SnapshotRouterLimits {
            requests: 4,
            max_image_bytes: snapshot_load_reservation(limits.parent_snapshots).unwrap(),
        },
    )
    .unwrap();
    let outbound = LogicalNetworkQueue::new(
        owner.authorization(),
        OutboundGeneration::new(1).unwrap(),
        OutboundLimits::default(),
        super::setup::network_limits(),
    )
    .unwrap();
    Parts {
        local: NodeLocalParts {
            owner,
            persistence: worker,
            applications: BTreeMap::from([(image.mapping().parent, app)]),
            results: routers.results,
            clients: routers.clients,
            reads: routers.reads,
            outbound,
            snapshots: Some(NodeSnapshots {
                worker: snapshots,
                router,
            }),
        },
        persistence: port,
        snapshots: Some(snapshot_port),
        sessions: BTreeMap::new(),
    }
}
pub fn node_limits(limits: LogicalLimits) -> LogicalNodeLimits {
    let releases = LogicalReleaseLimits {
        pending: 32,
        bytes: 2 * 1024 * 1024,
        capsule_bytes: limits.max_capsule_bytes,
        control_pending: 8,
        control_bytes: 512 * 1024,
    };
    // Both selected application types can capture a whole declared parent
    // batch. Protect one complete control capture alongside ordinary work;
    // this static profile does not reserve every activation control window.
    let count = limits.parent_log.max_entries_per_group;
    let capture = releases
        .capture_reservation_bytes::<LogicalReceipt, LogicalRead>(count)
        .unwrap()
        .max(
            releases
                .capture_reservation_bytes::<CounterReceipt, i64>(count)
                .unwrap(),
        );
    LogicalNodeLimits {
        configuration: ConfigurationRequestLimits::default(),
        replica: ReplicaDriverLimits {
            persistence_batch_units: 1,
            ..ReplicaDriverLimits::default()
        },
        releases: LogicalReleaseLimits {
            bytes: releases.bytes.max(capture.checked_mul(2).unwrap()),
            control_bytes: releases.control_bytes.max(capture),
            ..releases
        },
    }
}
