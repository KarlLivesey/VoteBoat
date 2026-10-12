// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Provisioning and public diagnostics never initialize or authorize a replica.
use super::*;
use voteboat::{runtime::LogicalResponseCodec, secure::PeerIdentity};
fn plan() -> LogicalEnrollmentPlan {
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
fn construct(
    plan: LogicalEnrollmentPlan,
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
        plan,
    )
}
#[test]
fn candidate_public_configuration_is_nonserving_and_bound_to_explicit_schema() {
    let a = construct(plan(), LogicalLimits::default()).unwrap();
    assert_eq!(
        a.initialization(),
        LogicalInitialization::EnrollmentCandidate(plan())
    );
    assert_eq!(a.schema_version(), LOGICAL_ENROLLMENT_APPLICATION_SCHEMA);
    assert_eq!(a.deployment_requirements().unwrap().application_schema, 2);
    assert_eq!(
        a.logical_response_schema(),
        LOGICAL_ENROLLMENT_RESPONSE_SCHEMA
    );
    assert_eq!(
        a.read_at(0, LogicalQuery::Image).unwrap(),
        LogicalRead::Uninitialized
    );
    assert_eq!(a.metadata().representative, None);
    assert_eq!(a.metadata().parent_revision, None);
    let LogicalRead::Enrollment(view) = a.read_at(0, LogicalQuery::Enrollment).unwrap() else {
        panic!("explicit diagnostics");
    };
    assert_eq!(view.plan, plan());
    assert!(view.record.is_none());
    assert!(a
        .validate_proposal(
            OperationId::new(11).unwrap(),
            b"ordinary business",
            std::iter::empty()
        )
        .is_err());
    assert_eq!(
        app(LogicalLimits::default()).schema_version(),
        LOGICAL_APPLICATION_SCHEMA
    );
}
#[test]
fn candidate_constructor_refuses_aliases_schema_zero_and_short_control_reserve() {
    for change in 0..4 {
        let mut p = plan();
        match change {
            0 => p.target.node = NodeId::new(1).unwrap(),
            1 => p.target.store = store(1),
            2 => p.source_application_schema = 0,
            _ => p.configuration = p.expected,
        }
        assert!(construct(p, LogicalLimits::default()).is_err());
    }
    let limits = LogicalLimits::default();
    assert!(construct(
        plan(),
        LogicalLimits {
            control_action_bytes: 2 * limits.max_command_bytes - 1,
            ..limits
        }
    )
    .is_err());
}
#[test]
fn candidate_checkpoint_plan_and_legacy_schema_mismatch_refuse_atomically() {
    let a = construct(plan(), LogicalLimits::default()).unwrap();
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut p = plan();
    p.source_application_schema = 2;
    let mut other = construct(p, LogicalLimits::default()).unwrap();
    assert!(!a.same_configuration(&other));
    let before = other.checkpoint(usize::MAX).unwrap();
    assert!(other.restore_checkpoint(2, 0, &bytes).is_err());
    assert_eq!(other.checkpoint(usize::MAX).unwrap(), before);
    let legacy = app(LogicalLimits::default())
        .checkpoint(usize::MAX)
        .unwrap();
    assert_eq!(
        other.restore_checkpoint(1, 0, &legacy),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert_eq!(other.checkpoint(usize::MAX).unwrap(), before);
    let mut restored = construct(plan(), LogicalLimits::default()).unwrap();
    restored.restore_checkpoint(2, 0, &bytes).unwrap();
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), bytes);
}
