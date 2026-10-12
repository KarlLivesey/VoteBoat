// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::collections::BTreeMap;
use voteboat::{
    application::*, identity::*, logical_voter::*, native::runtime::*, raft::*, runtime::*,
    snapshot_worker::*, worker::*,
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
    shard
        .register(core.with_configuration_replication())
        .unwrap();
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
        WorkerLimits {
            max_requests: 8,
            max_units: 8,
            max_bytes: 512 * 1024,
            control_requests: 2,
            control_units: 2,
            control_bytes: 128 * 1024,
            batch_units: 1,
            batch_bytes: limits.parent_log.max_batch_bytes,
        },
    )
    .unwrap();
    let (core, _) = image.recover_parent(&mut app).unwrap();
    let owner = owner(image, worker.binding(), core, now);
    let count = limits.parent_log.max_entries_per_group;
    let result_bytes = if image.activation_control_profile().is_some() {
        1024 * 1024
    } else {
        16 * 1024 * 1024
    };
    let routers = image
        .routers(
            &owner,
            LogicalRouterLimits {
                applications: ApplicationRouterLimits {
                    control_batches: 2,
                    batch_receipts: count,
                    batch_bytes: count
                        * (std::mem::size_of::<A::Receipt>()
                            + std::mem::size_of::<CounterReceipt>()),
                    ..Default::default()
                },
                invocations: ReadInvocationLimits {
                    result_bytes,
                    ..super::setup::read_limits()
                },
                execution: ReadRouterLimits {
                    result_bytes,
                    bytes: 64 * 1024 * 1024,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
    let (snapshots, snapshot_port) = logical_snapshots(
        image,
        &port,
        SnapshotWorkerGeneration::new(1).unwrap(),
        SnapshotWorkLimits {
            max_groups: 1,
            max_requests: 4,
            max_bytes: 8 * 1024 * 1024,
            control_requests: 1,
            control_bytes: 2 * 1024 * 1024,
        },
    )
    .unwrap();
    let allowance = snapshot_load_reservation(limits.parent_snapshots).unwrap();
    let router = SnapshotRouter::new_with_recovery_limits(
        owner.identity(),
        snapshots.binding(),
        SnapshotRouterLimits {
            requests: 4,
            max_image_bytes: allowance,
        },
        // Two original complete images plus bounded export-ticket/outcome metadata.
        SnapshotRecoveryLimits {
            requests: 4,
            image_bytes: allowance * 2 + 4096,
        },
    )
    .unwrap();
    let outbound = LogicalNetworkQueue::new(
        owner.authorization(),
        OutboundGeneration::new(1).unwrap(),
        super::setup::outbound_limits(),
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
pub fn node_limits<R: ApplicationReceipt, V>(limits: LogicalLimits) -> LogicalNodeLimits {
    let mut releases = LogicalReleaseLimits {
        pending: 32,
        bytes: 1,
        capsule_bytes: limits.max_capsule_bytes,
        control_pending: 2,
        control_bytes: 1,
    };
    // Every declared receipt batch fits as one original Committed capture.
    // The two separate control windows preserve the stronger activation profile.
    let batch = releases
        .capture_reservation_bytes::<R, V>(limits.parent_log.max_entries_per_group)
        .unwrap();
    releases.control_bytes = batch.checked_mul(releases.control_pending).unwrap();
    releases.bytes = batch.checked_add(releases.control_bytes).unwrap();
    assert!(releases.bytes <= 1024 * 1024 * 1024);
    LogicalNodeLimits {
        configuration: ConfigurationRequestLimits::default(),
        replica: ReplicaDriverLimits {
            persistence_batch_units: 1,
            ..ReplicaDriverLimits::default()
        },
        releases,
    }
}
