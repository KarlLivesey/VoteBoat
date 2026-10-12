// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
pub(super) fn plan() -> LogicalEnrollmentPlan {
    LogicalEnrollmentPlan {
        parent: group(1),
        target: PeerIdentity {
            node: NodeId::new(3).unwrap(),
            store: store(3),
        },
        operation: OperationId::new(10).unwrap(),
        expected: ConfigurationId::new(1).unwrap(),
        configuration: ConfigurationId::new(2).unwrap(),
        source_application_schema: 1,
    }
}
pub(super) fn candidate() -> LogicalApplication {
    candidate_with_limits(LogicalLimits::default()).unwrap()
}
pub(super) fn candidate_with_limits(
    limits: LogicalLimits,
) -> Result<LogicalApplication, ApplicationError> {
    LogicalApplication::new_enrollment_candidate(
        LogicalStoreMapping::new(group(1), NodeId::new(3).unwrap(), store(3), group(2)).unwrap(),
        bootstrap(group(2), 2, store(2)),
        bootstrap(group(1), 1, store(1)),
        limits,
        (
            NativeLogCodec,
            NativeSnapshotCodec,
            NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
        ),
        plan(),
    )
}
pub(super) fn import(a: &LogicalApplication) -> LogicalLearnerImport {
    let bootstrap = a.parent_bootstrap.clone();
    let configuration = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        bootstrap.policy.clone(),
        bootstrap.voter_stores.clone(),
        BTreeMap::from([(NodeId::new(3).unwrap(), store(3))]),
    )
    .unwrap();
    let learners = LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: plan().operation,
            expected: plan().expected,
            change: ConfigurationChange::Learners(configuration),
        })),
    };
    let membership = Membership::replay(&bootstrap, std::slice::from_ref(&learners), 1).unwrap();
    let mut counter = Counter::new(16).unwrap();
    counter
        .apply_batch(std::slice::from_ref(&learners))
        .unwrap();
    let snapshot = Snapshot {
        metadata: SnapshotMetadata {
            bootstrap,
            membership: Some(Box::new(membership)),
            index: 1,
            term: 1,
            application_schema: 1,
        },
        application: counter.checkpoint(4096).unwrap(),
    };
    let reference = SnapshotRef {
        store: store(1),
        ..a.snapshot_reference(&snapshot, SnapshotGeneration::new(9).unwrap())
            .unwrap()
    };
    LogicalLearnerImport {
        plan: plan(),
        learners,
        snapshot,
        origin: LogicalEnrollmentOrigin {
            mapping: LogicalStoreMapping::new(
                group(1),
                NodeId::new(1).unwrap(),
                store(1),
                group(4),
            )
            .unwrap(),
            representative: representative(1),
            export: SnapshotExportTicket {
                owner: owner(),
                visit: visit(),
                reference,
                request: SnapshotWorkTicket {
                    binding: SnapshotWorkerBinding {
                        store: owner().store,
                        generation: SnapshotWorkerGeneration::new(1).unwrap(),
                    },
                    sequence: 1,
                },
            },
        },
    }
}
pub(super) fn enrollment(input: LogicalLearnerImport, operation: u128) -> Command {
    command(operation, 1, CommandBody::EnrollmentClaim(Box::new(input)))
}
