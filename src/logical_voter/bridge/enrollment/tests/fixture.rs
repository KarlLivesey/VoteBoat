// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Deterministic source bytes are a target-component fixture, not source authority.
use super::*;
use crate::{
    application::Counter,
    membership::{Configuration, ConfigurationChange, ConfigurationRecord, Membership},
    quorum::{Limits, Policy, Tree},
    secure::PeerIdentity,
    snapshot_worker::{SnapshotWorkTicket, SnapshotWorkerBinding},
};
use std::collections::BTreeMap;
pub(super) fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
pub(super) fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
pub(super) fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
pub(super) fn mapping() -> LogicalStoreMapping {
    LogicalStoreMapping::new(group(800), node(14), store(104), group(801)).unwrap()
}
pub(super) fn child() -> Bootstrap {
    Bootstrap {
        group: group(801),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((1..=3).map(|id| Tree::Voter(node(id))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (1..=3).map(|id| (node(id), store(id.into()))).collect(),
    }
}
pub(super) fn application() -> LogicalApplication {
    let parent = Bootstrap {
        group: group(800),
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((11..=13).map(|id| Tree::Voter(node(id))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (11..=13)
            .map(|id| (node(id), store(u128::from(id + 90))))
            .collect(),
    };
    let limits = LogicalLimits {
        parent_log: crate::log::LogLimits {
            max_groups: 1,
            max_entries_per_group: 16,
            max_command_bytes: 1024,
            max_batch_bytes: 8192,
            max_batch_units: 1,
            max_pending_units: 8,
            max_snapshot_bytes: 64 * 1024,
            ..crate::log::LogLimits::default()
        },
        parent_snapshots: crate::snapshot::SnapshotLimits {
            max_application_bytes: 4096,
            max_metadata_bytes: 4096,
            max_chunk_bytes: 1024,
        },
        max_command_bytes: 32 * 1024,
        max_actions: 32,
        max_retained_action_bytes: 128 * 1024,
        control_actions: 4,
        control_action_bytes: 64 * 1024,
        max_capsule_bytes: 8192,
        max_parent_checkpoint_bytes: 32 * 1024,
        max_child_checkpoint_bytes: 256 * 1024,
    };
    LogicalApplication::new_enrollment_candidate(
        mapping(),
        child(),
        parent,
        limits,
        (
            NativeLogCodec,
            NativeSnapshotCodec,
            NativeWireCodec::with_leadership_transfer(crate::wire::WireLimits::default()).unwrap(),
        ),
        LogicalEnrollmentPlan {
            parent: mapping().parent,
            target: PeerIdentity {
                node: mapping().voter,
                store: mapping().store,
            },
            operation: OperationId::new(10).unwrap(),
            expected: ConfigurationId::new(1).unwrap(),
            configuration: ConfigurationId::new(2).unwrap(),
            source_application_schema: 1,
        },
    )
    .unwrap()
}
pub(super) fn enrollment(app: &LogicalApplication) -> LogicalLearnerEnrollment {
    let LogicalInitialization::EnrollmentCandidate(plan) = app.initialization() else {
        unreachable!()
    };
    let config = Configuration::new(
        plan.configuration,
        app.parent_bootstrap.policy.clone(),
        app.parent_bootstrap.voter_stores.clone(),
        BTreeMap::from([(mapping().voter, mapping().store)]),
    )
    .unwrap();
    let learners = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: plan.operation,
            expected: plan.expected,
            change: ConfigurationChange::Learners(config),
        })),
    };
    let membership =
        Membership::replay(&app.parent_bootstrap, std::slice::from_ref(&learners), 1).unwrap();
    let mut counter = Counter::new(16).unwrap();
    counter
        .apply_batch(std::slice::from_ref(&learners))
        .unwrap();
    let snapshot = Snapshot {
        metadata: crate::snapshot::SnapshotMetadata {
            bootstrap: app.parent_bootstrap.clone(),
            membership: Some(Box::new(membership)),
            index: 1,
            term: 1,
            application_schema: 1,
        },
        application: counter.checkpoint(4096).unwrap(),
    };
    let source =
        LogicalStoreMapping::new(mapping().parent, node(11), store(101), group(802)).unwrap();
    let representative = RepresentativeBinding {
        epoch: RepresentativeEpoch::new(1).unwrap(),
        physical: PeerIdentity {
            node: node(1),
            store: store(1),
        },
        physical_session: StoreSession::new(1).unwrap(),
        logical_session: StoreSession::new(1).unwrap(),
    };
    let owner = RuntimeOwner {
        store: StoreBinding {
            identity: source.store,
            session: representative.logical_session,
        },
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let reference = SnapshotRef {
        store: source.store,
        ..app
            .snapshot_reference(&snapshot, SnapshotGeneration::new(9).unwrap())
            .unwrap()
    };
    LogicalLearnerEnrollment::mint(
        mapping(),
        LogicalLearnerImport {
            plan,
            learners,
            snapshot,
            origin: LogicalEnrollmentOrigin {
                mapping: source,
                representative,
                export: SnapshotExportTicket {
                    owner,
                    reference,
                    request: SnapshotWorkTicket {
                        binding: SnapshotWorkerBinding {
                            store: owner.store,
                            generation: SnapshotWorkerGeneration::new(1).unwrap(),
                        },
                        sequence: 1,
                    },
                    visit: VisitTicket {
                        owner,
                        group: mapping().parent,
                        sequence: 1,
                    },
                },
            },
        },
    )
}
